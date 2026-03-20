//! Fixed-size, arena-backed mimalloc heap.
//!
//! A [`Heap`] reserves an exclusive mimalloc arena and pins a heap to it.
//! Allocations are confined to that arena — when it's exhausted, allocations
//! return NULL (and the `Allocator` impl returns `AllocError`).
//!
//! Internally reference-counted with a striped refcount for near-zero
//! contention on clone/drop. The refcount metadata lives *inside* the arena
//! it manages (self-hosted). On last drop the arena is bulk-freed and unmapped.
//!
//! # Thread Safety
//!
//! mimalloc heaps are thread-local — `mi_heap_new_ex()` creates a heap on the
//! calling thread. `mi_free()` can free from any thread.

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use allocator_api2::alloc::{AllocError, Allocator};

use crate::os_mmap;
use crate::padded::CachePadded;
use bisque_mimalloc_sys as ffi;

// Re-export for use in async contexts.
pub use allocator_api2::alloc::AllocError as HeapAllocError;

/// Mimalloc segment alignment (32 MiB on x86_64).
const MI_SEGMENT_ALIGN: usize = 32 * 1024 * 1024;

// =========================================================================
// Striped refcount (inline, no allocation)
// =========================================================================

const NUM_STRIPES: usize = 16;
const STRIPE_MASK: usize = NUM_STRIPES - 1;
const CLOSED_MASK: i64 = 1 << 63;
const VALUE_MASK: i64 = !CLOSED_MASK;
const BIAS: i64 = 1 << 62;

#[repr(C)]
struct StripedRefCount {
    counters: [CachePadded<AtomicI64>; NUM_STRIPES],
    cleanup_claimed: AtomicU64,
}

impl StripedRefCount {
    fn new() -> Self {
        Self {
            counters: std::array::from_fn(|_| CachePadded::new(AtomicI64::new(BIAS))),
            cleanup_claimed: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    fn increment(&self) {
        self.counters[stripe_index() & STRIPE_MASK].fetch_add(1, Ordering::Release);
    }

    #[inline(always)]
    fn decrement_and_check_closed(&self) -> bool {
        let prev = self.counters[stripe_index() & STRIPE_MASK].fetch_sub(1, Ordering::AcqRel);
        (prev & CLOSED_MASK) != 0
    }

    fn close_all(&self) {
        for c in &self.counters {
            c.fetch_or(CLOSED_MASK, Ordering::Release);
        }
    }

    fn total(&self) -> i64 {
        self.counters
            .iter()
            .map(|c| (c.load(Ordering::Acquire) & VALUE_MASK).wrapping_sub(BIAS))
            .sum()
    }

    fn try_claim_cleanup(&self) -> bool {
        self.cleanup_claimed
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }
}

// =========================================================================
// HeapInner — self-hosted inside the arena it manages
// =========================================================================

/// Lives at a fixed address inside the arena's mmap region. The striped
/// refcount counters are inline (no sub-allocation). On last-ref drop
/// the entire arena is destroyed and unmapped.
#[repr(C)]
struct HeapInner {
    refcount: StripedRefCount,
    mi_heap: *mut ffi::mi_heap_t,
    mmap_ptr: *mut u8,
    mmap_len: usize,
    arena_id: ffi::mi_arena_id_t,
    /// True when at least one allocation has failed (OOM). Enables
    /// freed-byte tracking on dealloc so waiters can be notified.
    pressure: AtomicBool,
    /// Bytes freed since pressure mode was entered. Reset when
    /// pressure mode exits. Only written when `pressure == true`.
    freed_since_pressure: AtomicU64,
    /// Wait queue for tasks blocked on OOM. Slots pre-allocated from the arena.
    pressure_wq: crate::wait_queue::WaitQueue,
    /// Max concurrent async waiters (configurable at construction).
    max_waiters: usize,
}

unsafe impl Send for HeapInner {}
unsafe impl Sync for HeapInner {}

/// Destroy the arena. Called exactly once when refcount reaches zero.
///
/// Reads all fields to stack locals, then:
/// 1. `mi_heap_destroy` — bulk-frees all allocations (including HeapInner itself)
/// 2. `mi_arena_unload` — removes arena from mimalloc's table
/// 3. `free_pages` — releases virtual memory
unsafe fn destroy_arena(ptr: *const HeapInner) {
    unsafe {
        let mi_heap = (*ptr).mi_heap;
        let mmap_ptr = (*ptr).mmap_ptr;
        let mmap_len = (*ptr).mmap_len;
        let arena_id = (*ptr).arena_id;

        // After this call, `ptr` is dangling — the HeapInner was in the arena.
        ffi::mi_heap_destroy(mi_heap);

        ffi::mi_arena_unload(
            arena_id,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );

        os_mmap::free_pages(mmap_ptr, mmap_len);
    }
}

// =========================================================================
// Heap — the public handle
// =========================================================================

/// A fixed-size, arena-backed mimalloc heap.
///
/// Clone is a single striped atomic increment (near-zero contention).
/// The reference-counted metadata lives inside the arena it manages.
///
/// # Examples
///
/// ```rust,no_run
/// use bisque_alloc::{Heap, HeapVec};
///
/// let heap = Heap::new(64 * 1024 * 1024).unwrap(); // 64 MiB arena
/// let mut v = HeapVec::new(&heap);
/// v.extend_from_slice(&[1, 2, 3]).unwrap();
/// ```
pub struct Heap {
    ptr: *const HeapInner,
    master: bool,
}

unsafe impl Send for Heap {}
unsafe impl Sync for Heap {}

impl Heap {
    /// Default number of async waiter slots for `alloc_async`.
    const DEFAULT_MAX_WAITERS: usize = 64;

    /// Create a new heap backed by an exclusive mimalloc arena.
    ///
    /// `max_bytes` is rounded up to 32 MiB alignment. When the arena is
    /// exhausted, allocations return `AllocError`.
    pub fn new(max_bytes: usize) -> Result<Self, AllocError> {
        Self::with_max_waiters(max_bytes, Self::DEFAULT_MAX_WAITERS)
    }

    /// Create a new heap with a specific number of async waiter slots.
    ///
    /// `max_waiters` controls how many tasks can concurrently await
    /// in `alloc_async` when the arena is under memory pressure.
    /// If all slots are exhausted, `alloc_async` returns `AllocError`
    /// immediately.
    pub fn with_max_waiters(max_bytes: usize, max_waiters: usize) -> Result<Self, AllocError> {
        let aligned = (max_bytes + MI_SEGMENT_ALIGN - 1) & !(MI_SEGMENT_ALIGN - 1);
        let aligned = aligned.max(MI_SEGMENT_ALIGN);

        // 1. Allocate virtual memory (cross-platform).
        let mmap_ptr = os_mmap::alloc_pages(aligned);
        if mmap_ptr.is_null() {
            return Err(AllocError);
        }

        // 2. Register as exclusive mimalloc arena.
        let mut arena_id: ffi::mi_arena_id_t = std::ptr::null_mut();
        let ok = unsafe {
            ffi::mi_manage_os_memory_ex(
                mmap_ptr as *mut core::ffi::c_void,
                aligned,
                true,
                false,
                true,
                -1,
                true,
                &mut arena_id,
            )
        };
        if !ok {
            unsafe { os_mmap::free_pages(mmap_ptr, aligned) };
            return Err(AllocError);
        }

        // 3. Create mimalloc heap pinned to arena.
        let mi_heap = unsafe { ffi::mi_heap_new_ex(0, true, arena_id) };
        if mi_heap.is_null() {
            unsafe {
                ffi::mi_arena_unload(
                    arena_id,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                );
                os_mmap::free_pages(mmap_ptr, aligned);
            }
            return Err(AllocError);
        }

        // 4. Allocate HeapInner *inside* the arena.
        let layout = Layout::new::<HeapInner>();
        let raw = unsafe { ffi::mi_heap_malloc_aligned(mi_heap, layout.size(), layout.align()) };
        if raw.is_null() {
            unsafe {
                ffi::mi_heap_destroy(mi_heap);
                ffi::mi_arena_unload(
                    arena_id,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                );
                os_mmap::free_pages(mmap_ptr, aligned);
            }
            return Err(AllocError);
        }

        let inner = raw as *mut HeapInner;
        unsafe {
            let pressure_wq = crate::wait_queue::WaitQueue::new_in(max_waiters, mi_heap);
            std::ptr::write(
                inner,
                HeapInner {
                    refcount: StripedRefCount::new(),
                    mi_heap,
                    mmap_ptr,
                    mmap_len: aligned,
                    arena_id,
                    pressure: AtomicBool::new(false),
                    freed_since_pressure: AtomicU64::new(0),
                    pressure_wq,
                    max_waiters,
                },
            );
            // Start with refcount of 1.
            (*inner).refcount.increment();
        }

        Ok(Self {
            ptr: inner,
            master: true,
        })
    }

    #[inline]
    fn inner(&self) -> &HeapInner {
        unsafe { &*self.ptr }
    }

    /// Total arena capacity in bytes.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner().mmap_len
    }

    /// Allocate `size` bytes with the given alignment. Returns null on OOM.
    #[inline]
    pub fn alloc(&self, size: usize, align: usize) -> *mut u8 {
        if size == 0 {
            return align as *mut u8;
        }
        unsafe { ffi::mi_heap_malloc_aligned(self.inner().mi_heap, size, align) as *mut u8 }
    }

    /// Allocate `size` bytes zero-initialized. Returns null on OOM.
    #[inline]
    pub fn alloc_zeroed(&self, size: usize, align: usize) -> *mut u8 {
        if size == 0 {
            return align as *mut u8;
        }
        unsafe { ffi::mi_heap_zalloc_aligned(self.inner().mi_heap, size, align) as *mut u8 }
    }

    /// Reallocate. Returns null on OOM (original pointer is NOT freed).
    ///
    /// # Safety
    /// `ptr` must have been allocated from mimalloc.
    #[inline]
    pub unsafe fn realloc(&self, ptr: *mut u8, new_size: usize, align: usize) -> *mut u8 {
        unsafe {
            ffi::mi_heap_realloc_aligned(
                self.inner().mi_heap,
                ptr as *mut core::ffi::c_void,
                new_size,
                align,
            ) as *mut u8
        }
    }

    /// Free a pointer. Thread-safe — mimalloc auto-detects the owning heap.
    ///
    /// During memory pressure, tracks the freed size and wakes one
    /// waiting task when enough bytes have been released.
    ///
    /// # Safety
    /// `ptr` must have been allocated by mimalloc.
    #[inline]
    pub unsafe fn dealloc(&self, ptr: *mut u8) {
        Self::free_with_pressure(self.inner(), ptr);
    }

    /// Free a pointer with pressure notification. Used by all drop paths
    /// (Bytes, BytesMut, collections::Vec, Box, Arc) to ensure memory
    /// pressure waiters are notified regardless of which type frees.
    ///
    /// # Safety
    /// `ptr` must have been allocated by mimalloc.
    #[inline]
    pub(crate) unsafe fn free_with_pressure(inner: &HeapInner, ptr: *mut u8) {
        if inner.pressure.load(Ordering::Relaxed) {
            let freed = unsafe { ffi::mi_usable_size(ptr as *const core::ffi::c_void) };
            inner
                .freed_since_pressure
                .fetch_add(freed as u64, Ordering::Relaxed);
            inner.pressure_wq.notify_one();
        }
        unsafe { ffi::mi_free(ptr as *mut core::ffi::c_void) }
    }

    /*
    Normal mode:                        Pressure mode:
    ┌─────────┐                        ┌─────────────────────┐
    │ alloc() │                        │ alloc()             │
    │  └→ mi_heap_malloc               │  └→ mi_heap_malloc  │
    │  └→ return ptr (0 ns overhead)   │  └→ return ptr      │
    └─────────┘                        └─────────────────────┘

    ┌───────────┐                      ┌────────────────────────┐
    │ dealloc() │                      │ dealloc()              │
    │  └→ mi_free (0 ns overhead)      │  └→ mi_usable_size     │
    │                                  │  └→ freed += size      │
    └───────────┘                      │  └→ event.notify(1)    │
                                       │  └→ mi_free            │
                                       └────────────────────────┘
                ┌───────────────────┐
                │ alloc_async()     │
                │  └→ try alloc()   │ ← fast path: returns immediately
                │  └→ if null:      │
                │     └→ enter pressure mode
                │     └→ event.listen().await  ← suspends task
                │     └→ woken by dealloc's notify(1)
                │     └→ retry alloc()
                │     └→ if ok: try_exit_pressure()
                └───────────────────┘
     */

    /// Async allocation — tries immediately, and if OOM, enters pressure
    /// mode and awaits until freed memory makes retry worthwhile.
    ///
    /// Returns `Err(AllocError)` if:
    /// - The arena is full AND all waiter slots are exhausted (too many
    ///   concurrent tasks waiting for memory on this heap).
    ///
    /// Once a waiter slot is acquired, the task retries after each
    /// notification until allocation succeeds. The slot is held across
    /// retries — no risk of losing it between attempts.
    ///
    /// This is runtime-agnostic (works with tokio, async-std, or any executor).
    pub async fn alloc_async(&self, size: usize, align: usize) -> Result<NonNull<u8>, AllocError> {
        // Fast path: try immediately.
        let ptr = self.alloc(size, align);
        if !ptr.is_null() {
            return Ok(unsafe { NonNull::new_unchecked(ptr) });
        }
        if size == 0 {
            return Ok(NonNull::new(align as *mut u8).unwrap());
        }

        // Enter pressure mode — start tracking frees.
        let inner = self.inner();
        inner.pressure.store(true, Ordering::Release);
        inner.freed_since_pressure.store(0, Ordering::Relaxed);

        // Acquire a waiter slot. If all slots are full, return error
        // immediately — too many concurrent waiters on this heap.
        let mut guard = inner.pressure_wq.try_acquire().ok_or(AllocError)?;

        loop {
            // Wait for a dealloc to free some memory. The guard
            // holds our slot across retries — if allocation fails
            // after wakeup, we go back to waiting with the same slot.
            guard.wait().await;

            // Retry allocation.
            let ptr = self.alloc(size, align);
            if !ptr.is_null() {
                drop(guard); // release slot
                self.try_exit_pressure();
                return Ok(unsafe { NonNull::new_unchecked(ptr) });
            }
            // Allocation still failed — loop back and wait again.
            // Our slot is still held, no re-acquisition needed.
        }
    }

    /// Check if pressure mode should be exited (no more waiters).
    fn try_exit_pressure(&self) {
        let inner = self.inner();
        if !inner.pressure_wq.has_waiters() {
            inner.pressure.store(false, Ordering::Release);
            inner.freed_since_pressure.store(0, Ordering::Relaxed);
        }
    }

    /// Returns true if the heap is currently in memory pressure mode.
    #[inline]
    pub fn is_under_pressure(&self) -> bool {
        self.inner().pressure.load(Ordering::Relaxed)
    }

    /// Maximum number of concurrent async waiters for this heap.
    #[inline]
    pub fn max_waiters(&self) -> usize {
        self.inner().max_waiters
    }

    /// Release thread-local caches and deferred free blocks.
    pub fn collect(&self, force: bool) {
        unsafe { ffi::mi_heap_collect(self.inner().mi_heap, force) }
    }

    /// Walk live blocks. Returns `(committed_bytes, used_block_count)`.
    /// O(n) — use for periodic diagnostics.
    pub fn visit_stats(&self) -> (usize, usize) {
        struct Acc {
            committed: usize,
            used: usize,
        }

        unsafe extern "C" fn visitor(
            _heap: *const ffi::mi_heap_t,
            area: *const ffi::mi_heap_area_t,
            _block: *mut core::ffi::c_void,
            _block_size: usize,
            arg: *mut core::ffi::c_void,
        ) -> bool {
            unsafe {
                let acc = &mut *(arg as *mut Acc);
                let a = &*area;
                acc.committed += a.committed;
                acc.used += a.used;
            }
            true
        }

        let mut acc = Acc {
            committed: 0,
            used: 0,
        };
        unsafe {
            ffi::mi_heap_visit_blocks(
                self.inner().mi_heap,
                false,
                Some(visitor),
                &mut acc as *mut Acc as *mut core::ffi::c_void,
            );
        }
        (acc.committed, acc.used)
    }

    /// Raw `mi_heap_t` pointer. Do not destroy or delete.
    #[inline]
    pub unsafe fn raw(&self) -> *mut ffi::mi_heap_t {
        self.inner().mi_heap
    }

    fn try_cleanup(&self) {
        let inner = self.inner();
        std::sync::atomic::fence(Ordering::SeqCst);
        let total = inner.refcount.total();
        debug_assert!(total >= 0, "Heap refcount went negative: {total}");
        if total == 0 && inner.refcount.try_claim_cleanup() {
            unsafe { destroy_arena(self.ptr) };
        }
    }
}

impl Clone for Heap {
    #[inline]
    fn clone(&self) -> Self {
        self.inner().refcount.increment();
        Self {
            ptr: self.ptr,
            master: false,
        }
    }
}

impl Drop for Heap {
    fn drop(&mut self) {
        let inner = self.inner();
        if self.master {
            inner.refcount.close_all();
            inner.refcount.counters[stripe_index() & STRIPE_MASK].fetch_sub(1, Ordering::AcqRel);
            self.try_cleanup();
        } else if inner.refcount.decrement_and_check_closed() {
            inner.refcount.close_all();
            self.try_cleanup();
        }
    }
}

impl Default for Heap {
    fn default() -> Self {
        Self::new(64 * 1024 * 1024).expect("failed to create default 64 MiB heap")
    }
}

// Remove the old `WaitQueue::new()` const — it's no longer available.
// WaitQueue now requires `new_in(count, mi_heap)` for arena allocation.

impl std::fmt::Debug for Heap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (committed, used) = self.visit_stats();
        f.debug_struct("Heap")
            .field("capacity", &self.capacity())
            .field("committed_bytes", &committed)
            .field("used_blocks", &used)
            .finish()
    }
}

// =========================================================================
// Allocator trait (allocator-api2 — works on stable, used by hashbrown)
// =========================================================================

unsafe impl Allocator for Heap {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout.size() == 0 {
            let ptr = NonNull::new(layout.align() as *mut u8).unwrap();
            return Ok(NonNull::slice_from_raw_parts(ptr, 0));
        }
        let ptr = self.alloc(layout.size(), layout.align());
        if ptr.is_null() {
            return Err(AllocError);
        }
        Ok(NonNull::slice_from_raw_parts(
            unsafe { NonNull::new_unchecked(ptr) },
            layout.size(),
        ))
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if layout.size() == 0 {
            return;
        }
        unsafe { self.dealloc(ptr.as_ptr()) }
    }

    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        _old: Layout,
        new: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if new.size() == 0 {
            return <Self as Allocator>::allocate(self, new);
        }
        let p = unsafe {
            ffi::mi_heap_realloc_aligned(
                self.inner().mi_heap,
                ptr.as_ptr() as *mut _,
                new.size(),
                new.align(),
            )
        };
        if p.is_null() {
            return Err(AllocError);
        }
        Ok(NonNull::slice_from_raw_parts(
            unsafe { NonNull::new_unchecked(p as *mut u8) },
            new.size(),
        ))
    }

    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old: Layout,
        new: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if new.size() == 0 {
            unsafe { <Self as Allocator>::deallocate(self, ptr, old) };
            let d = NonNull::new(new.align() as *mut u8).unwrap();
            return Ok(NonNull::slice_from_raw_parts(d, 0));
        }
        let p = unsafe {
            ffi::mi_heap_realloc_aligned(
                self.inner().mi_heap,
                ptr.as_ptr() as *mut _,
                new.size(),
                new.align(),
            )
        };
        if p.is_null() {
            return Err(AllocError);
        }
        Ok(NonNull::slice_from_raw_parts(
            unsafe { NonNull::new_unchecked(p as *mut u8) },
            new.size(),
        ))
    }
}

// =========================================================================
// HeapVec (kept for backward compat — delegates to collections::Vec)
// =========================================================================

/// Legacy alias. Prefer [`crate::collections::Vec`].
pub type HeapVec = crate::collections::Vec;

// =========================================================================
// Process info
// =========================================================================

/// Returns `(rss_bytes, committed_bytes)` from mimalloc process stats.
pub fn process_memory_info() -> (usize, usize) {
    let (mut e, mut u, mut s, mut rss, mut pr, mut c, mut pc, mut pf) = (0, 0, 0, 0, 0, 0, 0, 0);
    unsafe {
        ffi::mi_process_info(
            &mut e, &mut u, &mut s, &mut rss, &mut pr, &mut c, &mut pc, &mut pf,
        )
    }
    (rss, c)
}

// =========================================================================
// Thread stripe index
// =========================================================================

#[inline]
fn stripe_index() -> usize {
    use std::hash::{Hash, Hasher};
    thread_local! {
        static STRIPE: usize = {
            let id = std::thread::current().id();
            let mut hasher = std::hash::DefaultHasher::new();
            id.hash(&mut hasher);
            hasher.finish() as usize
        };
    }
    STRIPE.with(|s| *s)
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_create_and_alloc() {
        let heap = Heap::new(64 * 1024 * 1024).unwrap();
        assert!(heap.capacity() >= 64 * 1024 * 1024);
        let ptr = heap.alloc(256, 8);
        assert!(!ptr.is_null());
        unsafe { heap.dealloc(ptr) };
    }

    #[test]
    fn heap_zeroed() {
        let heap = Heap::new(64 * 1024 * 1024).unwrap();
        let ptr = heap.alloc_zeroed(128, 8);
        assert!(!ptr.is_null());
        let s = unsafe { std::slice::from_raw_parts(ptr, 128) };
        assert!(s.iter().all(|&b| b == 0));
        unsafe { heap.dealloc(ptr) };
    }

    #[test]
    fn heap_allocator_trait() {
        let heap = Heap::new(64 * 1024 * 1024).unwrap();
        let layout = Layout::from_size_align(128, 8).unwrap();
        let ptr = Allocator::allocate(&heap, layout).unwrap();
        assert_eq!(ptr.len(), 128);
        unsafe { Allocator::deallocate(&heap, ptr.cast(), layout) };
    }

    #[test]
    fn heap_allocator_oom() {
        let heap = Heap::new(MI_SEGMENT_ALIGN).unwrap();
        let mut ptrs = Vec::new();
        let mut oom = false;
        for _ in 0..10_000 {
            let ptr = heap.alloc(65536, 8);
            if ptr.is_null() {
                oom = true;
                break;
            }
            ptrs.push(ptr);
        }
        for ptr in ptrs {
            unsafe { heap.dealloc(ptr) }
        }
        assert!(oom, "expected OOM on 32 MiB heap");
    }

    #[test]
    fn heap_visit_stats() {
        let heap = Heap::new(64 * 1024 * 1024).unwrap();
        let p1 = heap.alloc(1024, 8);
        let p2 = heap.alloc(4096, 8);
        let (committed, used) = heap.visit_stats();
        assert!(committed > 0);
        assert!(used >= 2, "expected >= 2 blocks, got {used}");
        unsafe {
            heap.dealloc(p1);
            heap.dealloc(p2);
        }
    }

    #[test]
    fn heap_destroy_bulk_frees() {
        let heap = Heap::new(64 * 1024 * 1024).unwrap();
        for _ in 0..100 {
            let _ = heap.alloc(512, 8);
        }
        drop(heap);
    }

    #[test]
    fn heap_clone_shares() {
        let h1 = Heap::new(64 * 1024 * 1024).unwrap();
        let h2 = h1.clone();
        let ptr = h1.alloc(256, 8);
        unsafe { h2.dealloc(ptr) };
    }

    #[test]
    fn heap_clone_drop_order() {
        let h1 = Heap::new(64 * 1024 * 1024).unwrap();
        let h2 = h1.clone();
        let h3 = h2.clone();
        drop(h1); // master drops first — closes stripes
        drop(h3);
        drop(h2); // last clone triggers cleanup
    }

    #[test]
    fn heap_threaded_clone_drop() {
        let heap = Heap::new(64 * 1024 * 1024).unwrap();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let h = heap.clone();
            handles.push(std::thread::spawn(move || {
                let ptr = h.alloc(128, 8);
                assert!(!ptr.is_null());
                unsafe { h.dealloc(ptr) };
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        drop(heap);
    }

    #[test]
    fn heap_vec_basic() {
        let heap = Heap::new(64 * 1024 * 1024).unwrap();
        let mut v = HeapVec::with_capacity(64, &heap).unwrap();
        v.extend_from_slice(b"hello").unwrap();
        v.push(b'!').unwrap();
        assert_eq!(&v[..], b"hello!");
    }

    #[test]
    fn heap_vec_grow() {
        let heap = Heap::new(64 * 1024 * 1024).unwrap();
        let mut v = HeapVec::new(&heap);
        for i in 0..1000u16 {
            v.extend_from_slice(&i.to_le_bytes()).unwrap();
        }
        assert_eq!(v.len(), 2000);
    }

    #[test]
    fn heap_vec_into_bytes() {
        let heap = Heap::new(64 * 1024 * 1024).unwrap();
        let mut v = HeapVec::with_capacity(64, &heap).unwrap();
        v.extend_from_slice(b"hello world").unwrap();
        let b = v.into_bytes();
        assert_eq!(&b[..], b"hello world");
        assert_eq!(&b.clone()[..], b"hello world");
    }

    #[test]
    fn heap_vec_oom() {
        let heap = Heap::new(MI_SEGMENT_ALIGN).unwrap();
        let mut v = HeapVec::new(&heap);
        let mut oom = false;
        for _ in 0..10_000 {
            if v.extend_from_slice(&[0u8; 8192]).is_err() {
                oom = true;
                break;
            }
        }
        assert!(oom, "expected OOM");
    }

    #[test]
    fn heap_collect() {
        let heap = Heap::new(64 * 1024 * 1024).unwrap();
        let p = heap.alloc(256, 8);
        unsafe { heap.dealloc(p) };
        heap.collect(true);
    }

    #[test]
    fn process_memory_info_works() {
        let (rss, c) = process_memory_info();
        assert!(rss > 0 || c > 0);
    }

    #[test]
    fn heap_pressure_mode_off_by_default() {
        let heap = Heap::new(64 * 1024 * 1024).unwrap();
        assert!(!heap.is_under_pressure());
        assert_eq!(heap.max_waiters(), Heap::DEFAULT_MAX_WAITERS);
    }

    #[test]
    fn heap_custom_max_waiters() {
        let heap = Heap::with_max_waiters(64 * 1024 * 1024, 128).unwrap();
        assert_eq!(heap.max_waiters(), 128);
    }

    #[tokio::test]
    async fn heap_alloc_async_fast_path() {
        let heap = Heap::new(64 * 1024 * 1024).unwrap();
        let ptr = heap.alloc_async(256, 8).await.unwrap();
        assert!(!heap.is_under_pressure());
        unsafe { heap.dealloc(ptr.as_ptr()) };
    }

    #[tokio::test]
    async fn heap_alloc_async_zero_size() {
        let heap = Heap::new(64 * 1024 * 1024).unwrap();
        let ptr = heap.alloc_async(0, 8).await.unwrap();
        // ZST returns dangling aligned pointer.
        assert!(!ptr.as_ptr().is_null());
    }

    #[tokio::test]
    async fn heap_alloc_async_under_pressure() {
        let heap = Heap::new(MI_SEGMENT_ALIGN).unwrap();

        // Fill the arena.
        let mut ptrs = Vec::new();
        loop {
            let ptr = heap.alloc(65536, 8);
            if ptr.is_null() {
                break;
            }
            ptrs.push(ptr);
        }
        assert!(!ptrs.is_empty());

        // Spawn a task that frees after yielding.
        let heap2 = heap.clone();
        let ptr_to_free = ptrs.pop().unwrap() as usize;
        let free_task = tokio::spawn(async move {
            tokio::task::yield_now().await;
            unsafe { heap2.dealloc(ptr_to_free as *mut u8) };
        });

        // alloc_async should wait, then succeed.
        let result = heap.alloc_async(65536, 8).await;
        assert!(result.is_ok());

        free_task.await.unwrap();
        let ptr = result.unwrap();
        unsafe { heap.dealloc(ptr.as_ptr()) };
        for p in ptrs {
            unsafe { heap.dealloc(p) };
        }
    }

    #[tokio::test]
    async fn heap_alloc_async_retry_on_contention() {
        // Two tasks competing for the last free block.
        let heap = Heap::new(MI_SEGMENT_ALIGN).unwrap();

        let mut ptrs = Vec::new();
        loop {
            let ptr = heap.alloc(65536, 8);
            if ptr.is_null() {
                break;
            }
            ptrs.push(ptr);
        }
        // Leave exactly 2 blocks to free.
        let p1 = ptrs.pop().unwrap();
        let p2 = ptrs.pop().unwrap();

        let heap2 = heap.clone();
        let heap3 = heap.clone();
        let p1_addr = p1 as usize;
        let p2_addr = p2 as usize;

        // Task A waits for memory.
        let task_a = tokio::spawn({
            let h = heap2;
            async move { h.alloc_async(65536, 8).await.unwrap().as_ptr() as usize }
        });

        // Task B waits for memory.
        let task_b = tokio::spawn({
            let h = heap3;
            async move { h.alloc_async(65536, 8).await.unwrap().as_ptr() as usize }
        });

        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        // Free first block — one task should wake and succeed.
        unsafe { heap.dealloc(p1_addr as *mut u8) };
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        // Free second block — the other task should wake and succeed.
        unsafe { heap.dealloc(p2_addr as *mut u8) };

        let a = task_a.await.unwrap();
        let b = task_b.await.unwrap();
        assert_ne!(a, b);

        unsafe {
            heap.dealloc(a as *mut u8);
            heap.dealloc(b as *mut u8);
        }
        for p in ptrs {
            unsafe { heap.dealloc(p) };
        }
    }

    #[tokio::test]
    async fn heap_alloc_async_waiter_slots_exhausted() {
        // Create heap with only 2 waiter slots.
        let heap = Heap::with_max_waiters(MI_SEGMENT_ALIGN, 2).unwrap();
        assert_eq!(heap.max_waiters(), 2);

        // Fill the arena completely.
        let mut ptrs = Vec::new();
        loop {
            let ptr = heap.alloc(65536, 8);
            if ptr.is_null() {
                break;
            }
            ptrs.push(ptr);
        }

        // Spawn 2 waiters — they should get slots.
        let heap2 = heap.clone();
        let heap3 = heap.clone();
        let _task1 = tokio::spawn(async move {
            let _ = heap2.alloc_async(65536, 8).await;
        });
        let _task2 = tokio::spawn(async move {
            let _ = heap3.alloc_async(65536, 8).await;
        });

        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        // Third waiter — should fail immediately (slots exhausted).
        let result = heap.alloc_async(65536, 8).await;
        assert!(
            result.is_err(),
            "expected AllocError when waiter slots exhausted"
        );

        // Cleanup: free everything so spawned tasks can complete.
        for p in ptrs {
            unsafe { heap.dealloc(p) };
        }
    }

    #[tokio::test]
    async fn heap_pressure_mode_exits_when_no_waiters() {
        let heap = Heap::new(MI_SEGMENT_ALIGN).unwrap();

        // Fill, alloc_async, free — pressure should exit after.
        let mut ptrs = Vec::new();
        loop {
            let ptr = heap.alloc(65536, 8);
            if ptr.is_null() {
                break;
            }
            ptrs.push(ptr);
        }
        let to_free = ptrs.pop().unwrap() as usize;

        let heap2 = heap.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            unsafe { heap2.dealloc(to_free as *mut u8) };
        });

        let ptr = heap.alloc_async(65536, 8).await.unwrap();
        unsafe { heap.dealloc(ptr.as_ptr()) };

        // After alloc_async succeeds and guard is dropped,
        // pressure should exit if no other waiters.
        assert!(!heap.is_under_pressure());

        for p in ptrs {
            unsafe { heap.dealloc(p) };
        }
    }

    #[tokio::test]
    async fn heap_alloc_async_multiple_frees_needed() {
        // Waiter needs a large block, but frees come in small chunks.
        let heap = Heap::new(MI_SEGMENT_ALIGN).unwrap();

        // Fill with small blocks.
        let mut ptrs = Vec::new();
        loop {
            let ptr = heap.alloc(4096, 8);
            if ptr.is_null() {
                break;
            }
            ptrs.push(ptr);
        }

        let heap2 = heap.clone();
        // Free blocks one by one on a background task.
        let ptrs_to_free: Vec<usize> = ptrs
            .drain(ptrs.len().saturating_sub(20)..)
            .map(|p| p as usize)
            .collect();

        tokio::spawn(async move {
            for addr in ptrs_to_free {
                tokio::task::yield_now().await;
                unsafe { heap2.dealloc(addr as *mut u8) };
            }
        });

        // Try to allocate a larger block — may need multiple frees to coalesce.
        let result = heap.alloc_async(4096, 8).await;
        assert!(result.is_ok());

        let ptr = result.unwrap();
        unsafe { heap.dealloc(ptr.as_ptr()) };
        for p in ptrs {
            unsafe { heap.dealloc(p) };
        }
    }
}

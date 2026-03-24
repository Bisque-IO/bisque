//! Memory-limited mimalloc heap.
//!
//! [`HeapMaster`] owns the heap and its memory limit via [`Tlrc`].
//! [`Heap`] is a cheap, cloneable handle via [`TlrcRef`].
//!
//! Reference counting uses thread-local counters ([`crate::tlrc`]) for
//! zero cross-thread contention on clone/drop. The `TlrcInner<HeapData>`
//! is self-hosted — allocated from the mimalloc heap it manages.
//! On last-ref drop the heap is bulk-freed via `mi_heap_destroy`.
//!
//! # Thread Safety
//!
//! mimalloc heaps are thread-local for allocation — `mi_heap_new_ex()` creates
//! a heap on the calling thread. `mi_free()` can free from any thread.

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use allocator_api2::alloc::{AllocError, Allocator};

use crate::ptr::{Tlrc, TlrcRef, tlrc::TlrcInner};
use bisque_mimalloc_sys as ffi;

// Re-export for use in async contexts.
pub use allocator_api2::alloc::AllocError as HeapAllocError;

/// Minimum arena size for tests with OOM scenarios.
/// Must be large enough for at least one segment (32 MiB on 64-bit).
#[cfg(test)]
const TEST_SMALL_ARENA: usize = 64 * 1024 * 1024; // 64 MiB

// =========================================================================
// HeapData — metadata for a memory-limited mimalloc heap
// =========================================================================

/// The actual heap data, stored inside a `TlrcInner<HeapData>`.
/// Allocated from the mimalloc heap itself (self-hosted).
pub(crate) struct HeapData {
    pub(crate) mi_heap: *mut ffi::mi_heap_t,
    memory_limit: usize,
    /// True when at least one allocation has failed (OOM). Enables
    /// freed-byte tracking on dealloc so waiters can be notified.
    pressure: AtomicBool,
    /// Bytes freed since pressure mode was entered. Reset when
    /// pressure mode exits. Only written when `pressure == true`.
    freed_since_pressure: AtomicU64,
    /// Wait queue for tasks blocked on OOM. Slots pre-allocated from the heap.
    pressure_wq: crate::wait_queue::WaitQueue,
    /// Max concurrent async waiters (configurable at construction).
    max_waiters: usize,
}

unsafe impl Send for HeapData {}
unsafe impl Sync for HeapData {}

/// Custom destroy for TlrcInner<HeapData>.
///
/// TlrcInner<HeapData> is Box-allocated (global allocator), but HeapData's
/// internal fields (WaitQueue slots) were allocated from the mi_heap.
/// Sequence:
///   1. Save mi_heap pointer
///   2. drop_in_place — drops HeapData fields (WaitQueue Drop accesses slots)
///   3. mi_heap_destroy — bulk-frees all mi_heap pages (including WaitQueue slots)
///   4. dealloc — frees the TlrcInner memory (global allocator)
unsafe fn destroy_heap(ptr: *mut TlrcInner<HeapData>) {
    unsafe {
        let data_ref = &*ptr;
        let mi_heap = data_ref.data.mi_heap;
        // Drop all fields first (WaitQueue needs its slots alive)
        std::ptr::drop_in_place(ptr);
        // Bulk-free all mi_heap allocations (WaitQueue slots, user data, etc.)
        ffi::mi_heap_destroy(mi_heap);
        // Free the TlrcInner itself (global allocator)
        std::alloc::dealloc(ptr as *mut u8, Layout::new::<TlrcInner<HeapData>>());
    }
}

// =========================================================================
// HeapMaster — owns the heap, not cloneable
// =========================================================================

/// Owns a memory-limited mimalloc heap. Not cloneable.
///
/// Create cheap [`Heap`] handles via [`heap()`](HeapMaster::heap). The master
/// should outlive all `Heap` clones. When it drops, the `Tlrc` signals closing;
/// the inner is freed when all threads have unregistered.
///
/// # Examples
///
/// ```rust,no_run
/// use bisque_alloc::{HeapMaster, Heap, HeapVec};
///
/// let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
/// let heap: Heap = master.heap();
/// let mut v = HeapVec::new(&heap);
/// v.extend_from_slice(&[1, 2, 3]).unwrap();
/// // master must outlive heap and v
/// ```
pub struct HeapMaster {
    /// Cached Heap handle for Deref. Created once at construction.
    /// Drops BEFORE inner (Rust drops fields in declaration order),
    /// so the TlrcRef's reference is released before Tlrc closes.
    handle: Heap,
    inner: Tlrc<HeapData>,
}

impl HeapMaster {
    /// Default number of async waiter slots for `alloc_async`.
    const DEFAULT_MAX_WAITERS: usize = 64;

    /// Create a new memory-limited heap.
    ///
    /// `max_bytes` sets the per-heap memory limit (0 = unlimited).
    /// Tracked at 64 KiB slice granularity.
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
        ffi::ensure_initialized();

        // 1. Create a standard mimalloc heap with allow_destroy=true.
        let mi_heap = unsafe { ffi::mi_heap_new_ex(0, true, ffi::mi_arena_id_t::default()) };
        if mi_heap.is_null() {
            return Err(AllocError);
        }

        // 2. Set the memory limit.
        if max_bytes > 0 {
            unsafe { ffi::mi_heap_set_memory_limit(mi_heap, max_bytes) };
        }

        // 3. Allocate TlrcInner<HeapData> from the global allocator (Box).
        //    This ensures TlrcInner survives mi_heap_destroy, so thread-local
        //    SlabEntries can safely reference it during TLS teardown.
        let pressure_wq = unsafe { crate::wait_queue::WaitQueue::new_in(max_waiters, mi_heap) };
        let inner_box = Box::new(TlrcInner::new(HeapData {
            mi_heap,
            memory_limit: max_bytes,
            pressure: AtomicBool::new(false),
            freed_since_pressure: AtomicU64::new(0),
            pressure_wq,
            max_waiters,
        }));
        let inner_ptr = Box::into_raw(inner_box);

        let inner = unsafe { Tlrc::from_raw(inner_ptr) };
        let handle = Heap {
            inner: inner.tlrc_ref(),
        };
        Ok(Self { inner, handle })
    }

    /// Create a cheap, cloneable [`Heap`] handle.
    #[inline]
    pub fn heap(&self) -> Heap {
        Heap {
            inner: self.inner.tlrc_ref(),
        }
    }

    #[inline(always)]
    fn data(&self) -> &HeapData {
        &self.inner
    }
}

impl std::ops::Deref for HeapMaster {
    type Target = Heap;

    #[inline(always)]
    fn deref(&self) -> &Heap {
        &self.handle
    }
}

impl std::fmt::Debug for HeapMaster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let heap: &Heap = self;
        f.debug_struct("HeapMaster")
            .field("capacity", &heap.capacity())
            .field("memory_usage", &heap.memory_usage())
            .finish()
    }
}

// =========================================================================
// Heap — cheap cloneable handle
// =========================================================================

/// Cheap, cloneable handle to a memory-limited mimalloc heap.
///
/// Created from a [`HeapMaster`] via [`heap()`](HeapMaster::heap) or by
/// cloning an existing `Heap`. Clone uses thread-local reference counting
/// with zero cross-thread contention.
pub struct Heap {
    inner: TlrcRef<HeapData>,
}

unsafe impl Send for Heap {}
unsafe impl Sync for Heap {}

impl Heap {
    #[inline(always)]
    pub(crate) fn data(&self) -> &HeapData {
        &self.inner
    }

    /// Memory limit in bytes (0 = unlimited).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data().memory_limit
    }

    /// Current memory usage in bytes (tracked at 64 KiB slice granularity).
    #[inline]
    pub fn memory_usage(&self) -> usize {
        unsafe { ffi::mi_heap_get_memory_usage(self.data().mi_heap) }
    }

    /// Allocate `size` bytes with the given alignment. Returns null on OOM.
    #[inline]
    pub fn alloc(&self, size: usize, align: usize) -> *mut u8 {
        if size == 0 {
            return align as *mut u8;
        }
        let heap = self.data().mi_heap;
        if align <= core::mem::size_of::<usize>() {
            unsafe { ffi::mi_heap_malloc(heap, size) as *mut u8 }
        } else {
            unsafe { ffi::mi_heap_malloc_aligned(heap, size, align) as *mut u8 }
        }
    }

    /// Allocate `size` bytes zero-initialized. Returns null on OOM.
    #[inline]
    pub fn alloc_zeroed(&self, size: usize, align: usize) -> *mut u8 {
        if size == 0 {
            return align as *mut u8;
        }
        unsafe { ffi::mi_heap_zalloc_aligned(self.data().mi_heap, size, align) as *mut u8 }
    }

    /// Reallocate. Returns null on OOM (original pointer is NOT freed).
    ///
    /// # Safety
    /// `ptr` must have been allocated from mimalloc.
    #[inline]
    pub unsafe fn realloc(&self, ptr: *mut u8, new_size: usize, align: usize) -> *mut u8 {
        unsafe {
            ffi::mi_heap_realloc_aligned(
                self.data().mi_heap,
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
        Self::free_with_pressure(self.data(), ptr);
    }

    /// Free a pointer with pressure notification. Used by all drop paths
    /// (Bytes, BytesMut, collections::Vec, Box, Arc) to ensure memory
    /// pressure waiters are notified regardless of which type frees.
    ///
    /// # Safety
    /// `ptr` must have been allocated by mimalloc.
    #[inline]
    pub(crate) unsafe fn free_with_pressure(inner: &HeapData, ptr: *mut u8) {
        if inner.pressure.load(Ordering::Relaxed) {
            let freed = unsafe { ffi::mi_usable_size(ptr as *const core::ffi::c_void) };
            inner
                .freed_since_pressure
                .fetch_add(freed as u64, Ordering::Relaxed);
            inner.pressure_wq.notify_one();
        }
        unsafe { ffi::mi_free(ptr as *mut core::ffi::c_void) }
    }

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
        let inner = self.data();
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
        let inner = self.data();
        if !inner.pressure_wq.has_waiters() {
            inner.pressure.store(false, Ordering::Release);
            inner.freed_since_pressure.store(0, Ordering::Relaxed);
        }
    }

    /// Returns true if the heap is currently in memory pressure mode.
    #[inline]
    pub fn is_under_pressure(&self) -> bool {
        self.data().pressure.load(Ordering::Relaxed)
    }

    /// Maximum number of concurrent async waiters for this heap.
    #[inline]
    pub fn max_waiters(&self) -> usize {
        self.data().max_waiters
    }

    /// Release thread-local caches and deferred free blocks.
    pub fn collect(&self, force: bool) {
        unsafe { ffi::mi_heap_collect(self.data().mi_heap, force) }
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
                self.data().mi_heap,
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
        self.data().mi_heap
    }
}

// Clone and Drop are handled by TlrcRef<HeapData> inside Heap.

impl Clone for Heap {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl std::fmt::Debug for Heap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Heap")
            .field("capacity", &self.capacity())
            .field("memory_usage", &self.memory_usage())
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
                self.data().mi_heap,
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
                self.data().mi_heap,
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
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_create_and_alloc() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        assert!(heap.capacity() >= 64 * 1024 * 1024);
        let ptr = heap.alloc(256, 8);
        assert!(!ptr.is_null());
        unsafe { heap.dealloc(ptr) };
    }

    #[test]
    fn heap_zeroed() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let ptr = heap.alloc_zeroed(128, 8);
        assert!(!ptr.is_null());
        let s = unsafe { std::slice::from_raw_parts(ptr, 128) };
        assert!(s.iter().all(|&b| b == 0));
        unsafe { heap.dealloc(ptr) };
    }

    #[test]
    fn heap_allocator_trait() {
        let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let heap: &Heap = &master;
        let layout = Layout::from_size_align(128, 8).unwrap();
        let ptr = Allocator::allocate(heap, layout).unwrap();
        assert_eq!(ptr.len(), 128);
        unsafe { Allocator::deallocate(heap, ptr.cast(), layout) };
    }

    #[test]
    fn heap_allocator_oom() {
        let heap = HeapMaster::new(TEST_SMALL_ARENA).unwrap();
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
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
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
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        for _ in 0..100 {
            let _ = heap.alloc(512, 8);
        }
        drop(heap);
    }

    /// Verify mimalloc can handle a large number of concurrent heaps.
    /// Each heap allocates, uses, and frees memory independently.
    #[test]
    fn heap_100k_heaps() {
        const NUM_HEAPS: usize = 100_000;
        const LIMIT: usize = 0; // unlimited — we only care about heap count, not memory

        // Create and destroy heaps in batches to avoid holding 100K heaps simultaneously
        // (which would consume too much memory). Batches of 1000 prove that mimalloc's
        // internal heap tracking can handle high churn.
        let mut total_created = 0usize;
        while total_created < NUM_HEAPS {
            let batch = (NUM_HEAPS - total_created).min(1000);
            let mut masters: Vec<HeapMaster> = Vec::with_capacity(batch);
            for _ in 0..batch {
                let m = HeapMaster::new(LIMIT).unwrap();
                // Allocate and free one block to exercise the full path.
                let p = m.alloc(128, 8);
                assert!(
                    !p.is_null(),
                    "alloc failed on heap #{}",
                    total_created + masters.len()
                );
                unsafe { m.dealloc(p) };
                masters.push(m);
            }
            total_created += batch;
            // All masters drop here, freeing the heaps.
        }
        assert_eq!(total_created, NUM_HEAPS);
    }

    #[test]
    fn heap_clone_shares() {
        let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let h1 = master.heap();
        let h2 = h1.clone();
        let ptr = h1.alloc(256, 8);
        unsafe { h2.dealloc(ptr) };
    }

    #[test]
    fn heap_clone_drop_order() {
        let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let h1 = master.heap();
        let h2 = h1.clone();
        drop(master); // master drops first — closes stripes
        drop(h2);
        drop(h1); // last clone triggers cleanup
    }

    #[test]
    fn heap_threaded_clone_drop() {
        let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let h = master.heap();
            handles.push(std::thread::spawn(move || {
                let ptr = h.alloc(128, 8);
                assert!(!ptr.is_null());
                unsafe { h.dealloc(ptr) };
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        drop(master);
    }

    #[test]
    fn heap_vec_basic() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut v = HeapVec::with_capacity(64, &heap).unwrap();
        v.extend_from_slice(b"hello").unwrap();
        v.push(b'!').unwrap();
        assert_eq!(&v[..], b"hello!");
    }

    #[test]
    fn heap_vec_grow() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut v = HeapVec::new(&heap);
        for i in 0..1000u16 {
            v.extend_from_slice(&i.to_le_bytes()).unwrap();
        }
        assert_eq!(v.len(), 2000);
    }

    #[test]
    fn heap_vec_into_bytes() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let mut v = HeapVec::with_capacity(64, &heap).unwrap();
        v.extend_from_slice(b"hello world").unwrap();
        let b = v.into_bytes();
        assert_eq!(&b[..], b"hello world");
        assert_eq!(&b.clone()[..], b"hello world");
    }

    #[test]
    fn heap_vec_oom() {
        let heap = HeapMaster::new(TEST_SMALL_ARENA).unwrap();
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
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
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
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        assert!(!heap.is_under_pressure());
        assert_eq!(heap.max_waiters(), HeapMaster::DEFAULT_MAX_WAITERS);
    }

    #[test]
    fn heap_custom_max_waiters() {
        let heap = HeapMaster::with_max_waiters(64 * 1024 * 1024, 128).unwrap();
        assert_eq!(heap.max_waiters(), 128);
    }

    #[tokio::test]
    async fn heap_alloc_async_fast_path() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let ptr = heap.alloc_async(256, 8).await.unwrap();
        assert!(!heap.is_under_pressure());
        unsafe { heap.dealloc(ptr.as_ptr()) };
    }

    #[tokio::test]
    async fn heap_alloc_async_zero_size() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let ptr = heap.alloc_async(0, 8).await.unwrap();
        // ZST returns dangling aligned pointer.
        assert!(!ptr.as_ptr().is_null());
    }

    #[tokio::test]
    async fn heap_alloc_async_under_pressure() {
        let heap = HeapMaster::new(TEST_SMALL_ARENA).unwrap();

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
        let heap2 = heap.heap();
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
        let heap = HeapMaster::new(TEST_SMALL_ARENA).unwrap();

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

        let heap2 = heap.heap();
        let heap3 = heap.heap();
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
        let heap = HeapMaster::with_max_waiters(TEST_SMALL_ARENA, 2).unwrap();
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
        let heap2 = heap.heap();
        let heap3 = heap.heap();
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
        let heap = HeapMaster::new(TEST_SMALL_ARENA).unwrap();

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

        let heap2 = heap.heap();
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
        let heap = HeapMaster::new(TEST_SMALL_ARENA).unwrap();

        // Fill with small blocks.
        let mut ptrs = Vec::new();
        loop {
            let ptr = heap.alloc(4096, 8);
            if ptr.is_null() {
                break;
            }
            ptrs.push(ptr);
        }

        let heap2 = heap.heap();
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

    // =====================================================================
    // Memlimit battle tests
    // =====================================================================

    /// Helper: allocate until OOM, return all pointers and the count.
    fn fill_heap(heap: &Heap, size: usize) -> Vec<*mut u8> {
        let mut ptrs = Vec::new();
        loop {
            let p = heap.alloc(size, 8);
            if p.is_null() {
                break;
            }
            ptrs.push(p);
        }
        ptrs
    }

    /// After freeing everything and collecting, memory_usage must return to zero.
    /// Note: mimalloc retains pages for reuse after blocks are freed.
    /// `collect(true)` forces page retirement.
    #[test]
    fn memlimit_usage_returns_to_zero_small() {
        let heap = HeapMaster::new(4 * 1024 * 1024).unwrap();
        let ptrs = fill_heap(&heap, 64);
        assert!(!ptrs.is_empty());
        assert!(heap.memory_usage() > 0);
        for p in ptrs {
            unsafe { heap.dealloc(p) };
        }
        heap.collect(true);
        assert!(
            heap.memory_usage() <= MEMLIMIT_COLLECT_TOLERANCE,
            "small: usage not zero after free+collect"
        );
    }

    #[test]
    fn memlimit_usage_returns_to_zero_medium() {
        let heap = HeapMaster::new(4 * 1024 * 1024).unwrap();
        let ptrs = fill_heap(&heap, 16 * 1024);
        assert!(!ptrs.is_empty());
        for p in ptrs {
            unsafe { heap.dealloc(p) };
        }
        heap.collect(true);
        assert!(
            heap.memory_usage() <= MEMLIMIT_COLLECT_TOLERANCE,
            "medium: usage not zero after free+collect"
        );
    }

    #[test]
    fn memlimit_usage_returns_to_zero_large() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let ptrs = fill_heap(&heap, 512 * 1024);
        assert!(!ptrs.is_empty());
        for p in ptrs {
            unsafe { heap.dealloc(p) };
        }
        heap.collect(true);
        assert!(
            heap.memory_usage() <= MEMLIMIT_COLLECT_TOLERANCE,
            "large: usage not zero after free+collect"
        );
    }

    #[test]
    fn memlimit_usage_returns_to_zero_mixed() {
        let heap = HeapMaster::new(32 * 1024 * 1024).unwrap();
        let sizes = [
            32,
            128,
            1024,
            4096,
            8192,
            32768,
            65536,
            256 * 1024,
            1024 * 1024,
        ];
        let mut ptrs = Vec::new();
        for &sz in sizes.iter().cycle().take(200) {
            let p = heap.alloc(sz, 8);
            if p.is_null() {
                break;
            }
            ptrs.push(p);
        }
        assert!(!ptrs.is_empty());
        for p in ptrs {
            unsafe { heap.dealloc(p) };
        }
        heap.collect(true);
        assert!(
            heap.memory_usage() <= MEMLIMIT_COLLECT_TOLERANCE,
            "mixed: usage not zero after free+collect"
        );
    }

    /// Usage must stay within limit + bounded overshoot.
    /// Overshoot is at most MI_MEDIUM_PAGE_SIZE (512 KiB) per concurrent allocating thread.
    // With CAS-based enforcement in page_queue_push, memory_usage never exceeds memory_limit.
    // Zero overshoot on the limit itself.
    const MEMLIMIT_MAX_OVERSHOOT: usize = 0;

    // After free+collect, mimalloc may retain pages for reuse. This is the tolerance
    // for "returns to zero" tests. Bounded by a few retained small pages.
    const MEMLIMIT_COLLECT_TOLERANCE: usize = 16 * 65536; // 16 slices (1 MiB) — accounts for page retention after collect

    #[test]
    fn memlimit_never_exceeds_limit() {
        let limit = 8 * 1024 * 1024; // 8 MiB
        let heap = HeapMaster::new(limit).unwrap();
        let sizes = [64, 256, 4096, 16384, 65536, 256 * 1024];
        let mut ptrs = Vec::new();
        for &sz in sizes.iter().cycle().take(5000) {
            let p = heap.alloc(sz, 8);
            if p.is_null() {
                break;
            }
            ptrs.push(p);
            assert!(
                heap.memory_usage() <= limit + MEMLIMIT_MAX_OVERSHOOT,
                "usage {} exceeded limit {} + overshoot {} after alloc of {} bytes",
                heap.memory_usage(),
                limit,
                MEMLIMIT_MAX_OVERSHOOT,
                sz
            );
        }
        for p in ptrs {
            unsafe { heap.dealloc(p) };
        }
    }

    /// Alloc-free-alloc cycles: usage must never exceed limit and must stay bounded.
    #[test]
    fn memlimit_alloc_free_cycles() {
        let limit = 4 * 1024 * 1024;
        let heap = HeapMaster::new(limit).unwrap();
        for _ in 0..1000 {
            let p = heap.alloc(4096, 8);
            assert!(!p.is_null());
            assert!(heap.memory_usage() <= limit + MEMLIMIT_MAX_OVERSHOOT);
            unsafe { heap.dealloc(p) };
        }
        // After cycles, usage may be non-zero (mimalloc retains pages for reuse).
        // But it must still be within the limit.
        assert!(heap.memory_usage() <= limit + MEMLIMIT_MAX_OVERSHOOT);
    }

    /// Fill to OOM, free half, allocate more, verify usage stays bounded.
    #[test]
    fn memlimit_partial_free_realloc() {
        let limit = 8 * 1024 * 1024;
        let heap = HeapMaster::new(limit).unwrap();
        let mut ptrs = fill_heap(&heap, 1024);
        let usage_at_full = heap.memory_usage();
        assert!(usage_at_full > 0);
        assert!(
            usage_at_full <= limit,
            "usage_at_full={} limit={}",
            usage_at_full,
            limit
        );

        // Free half
        let half = ptrs.len() / 2;
        for p in ptrs.drain(..half) {
            unsafe { heap.dealloc(p) };
        }
        let usage_after_free = heap.memory_usage();
        assert!(usage_after_free < usage_at_full);

        // Allocate more — should succeed since we freed space
        let mut more = Vec::new();
        for _ in 0..half {
            let p = heap.alloc(1024, 8);
            if p.is_null() {
                break;
            }
            more.push(p);
            assert!(heap.memory_usage() <= limit + MEMLIMIT_MAX_OVERSHOOT);
        }
        assert!(!more.is_empty());

        // Clean up
        for p in ptrs {
            unsafe { heap.dealloc(p) };
        }
        for p in more {
            unsafe { heap.dealloc(p) };
        }
        heap.collect(true);
        assert!(heap.memory_usage() <= MEMLIMIT_COLLECT_TOLERANCE);
    }

    /// Multi-threaded: each thread has its own heap, all respect their limits.
    #[test]
    fn memlimit_multithread_per_thread_heap() {
        let thread_count = 8;
        let limit = 4 * 1024 * 1024;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(thread_count));

        std::thread::scope(|s| {
            for _ in 0..thread_count {
                let barrier = barrier.clone();
                s.spawn(move || {
                    let heap = HeapMaster::new(limit).unwrap();
                    barrier.wait(); // synchronize start
                    let ptrs = fill_heap(&heap, 256);
                    assert!(!ptrs.is_empty());
                    assert!(heap.memory_usage() <= limit + MEMLIMIT_MAX_OVERSHOOT);
                    for p in ptrs {
                        unsafe { heap.dealloc(p) };
                    }
                    heap.collect(true);
                    assert!(heap.memory_usage() <= MEMLIMIT_COLLECT_TOLERANCE);
                });
            }
        });
    }

    /// Multi-threaded: main thread allocates, worker threads free.
    /// mimalloc heaps are thread-local for allocation; mi_free is safe from any thread.
    #[test]
    fn memlimit_multithread_cross_thread_free() {
        let limit = 16 * 1024 * 1024;
        let heap = HeapMaster::new(limit).unwrap();

        // Allocate on main thread
        let mut ptrs: Vec<*mut u8> = Vec::new();
        for _ in 0..1000 {
            let p = heap.alloc(256, 8);
            if p.is_null() {
                break;
            }
            ptrs.push(p);
        }
        assert!(!ptrs.is_empty());
        assert!(heap.memory_usage() <= limit + MEMLIMIT_MAX_OVERSHOOT);

        // Free from worker threads (wrap raw pointers for Send)
        let chunks: Vec<Vec<usize>> = ptrs
            .chunks(250)
            .map(|c| c.iter().map(|p| *p as usize).collect())
            .collect();
        std::thread::scope(|s| {
            for chunk in chunks {
                let heap = heap.heap();
                s.spawn(move || {
                    for p in chunk {
                        unsafe { heap.dealloc(p as *mut u8) };
                    }
                });
            }
        });
        // Process delayed frees and retire pages
        heap.collect(true);
        assert!(
            heap.memory_usage() <= MEMLIMIT_COLLECT_TOLERANCE,
            "cross-thread free: usage not zero"
        );
    }

    /// Multi-threaded: per-thread heaps, verify limit never exceeded under contention.
    #[test]
    fn memlimit_multithread_concurrent_heaps_limit_check() {
        let limit = 4 * 1024 * 1024;
        let violation = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));

        std::thread::scope(|s| {
            for _ in 0..8 {
                let violation = violation.clone();
                let barrier = barrier.clone();
                s.spawn(move || {
                    let heap = HeapMaster::new(limit).unwrap();
                    barrier.wait();
                    let mut held = Vec::new();
                    for _ in 0..500 {
                        let p = heap.alloc(4096, 8);
                        if !p.is_null() {
                            held.push(p);
                            if heap.memory_usage() > limit + MEMLIMIT_MAX_OVERSHOOT {
                                violation.store(true, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                        if held.len() > 10 {
                            for p in held.drain(..5) {
                                unsafe { heap.dealloc(p) };
                            }
                        }
                    }
                    for p in held {
                        unsafe { heap.dealloc(p) };
                    }
                });
            }
        });
        assert!(
            !violation.load(std::sync::atomic::Ordering::Relaxed),
            "limit was exceeded"
        );
    }

    /// Stress: rapid create/destroy of many heaps.
    #[test]
    fn memlimit_rapid_heap_lifecycle() {
        for _ in 0..100 {
            let heap = HeapMaster::new(2 * 1024 * 1024).unwrap();
            let mut ptrs = Vec::new();
            for _ in 0..50 {
                let p = heap.alloc(1024, 8);
                if p.is_null() {
                    break;
                }
                ptrs.push(p);
            }
            for p in ptrs {
                unsafe { heap.dealloc(p) };
            }
            heap.collect(true);
            assert!(heap.memory_usage() <= MEMLIMIT_COLLECT_TOLERANCE);
            drop(heap);
        }
    }

    /// Different size classes in same heap: verify accounting is exact.
    #[test]
    fn memlimit_size_class_accounting() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();

        // Small: <= 8 KiB → 64 KiB page (1 slice)
        let p1 = heap.alloc(64, 8);
        assert!(!p1.is_null());
        let u1 = heap.memory_usage();
        assert!(u1 > 0, "small alloc should register usage");

        // Medium: 8 KiB < size <= 64 KiB → 512 KiB page (8 slices)
        let p2 = heap.alloc(16 * 1024, 8);
        assert!(!p2.is_null());
        let u2 = heap.memory_usage();
        assert!(u2 > u1, "medium alloc should increase usage");

        // Large: 64 KiB < size <= 16 MiB
        let p3 = heap.alloc(256 * 1024, 8);
        assert!(!p3.is_null());
        let u3 = heap.memory_usage();
        assert!(u3 > u2, "large alloc should increase usage");

        // Free all — usage tracks pages, not blocks. Pages are freed/retired
        // asynchronously by mimalloc, so usage may not decrease immediately.
        unsafe { heap.dealloc(p3) };
        unsafe { heap.dealloc(p2) };
        unsafe { heap.dealloc(p1) };
        heap.collect(true);
        // After freeing all blocks + collect, usage should be bounded
        assert!(
            heap.memory_usage() <= MEMLIMIT_COLLECT_TOLERANCE,
            "all freed but usage not zero"
        );
    }

    /// Fuzz-style: random sizes, random alloc/free pattern, verify invariants.
    #[test]
    fn memlimit_fuzz_random_pattern() {
        use std::collections::VecDeque;
        let limit = 16 * 1024 * 1024;
        let heap = HeapMaster::new(limit).unwrap();
        let mut live: VecDeque<*mut u8> = VecDeque::new();
        let sizes = [
            8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072,
            262144, 524288, 1048576,
        ];

        for i in 0..10_000u64 {
            let sz = sizes[(i
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407)
                >> 48) as usize
                % sizes.len()];
            if i % 3 != 0 {
                // allocate
                let p = heap.alloc(sz, 8);
                if !p.is_null() {
                    live.push_back(p);
                }
            } else if let Some(p) = live.pop_front() {
                // free oldest
                unsafe { heap.dealloc(p) };
            }
            // invariant: usage never exceeds limit
            assert!(
                heap.memory_usage() <= limit + MEMLIMIT_MAX_OVERSHOOT,
                "fuzz: usage {} > limit {} + overshoot at iter {}",
                heap.memory_usage(),
                limit,
                i
            );
        }
        // free all remaining
        for p in live {
            unsafe { heap.dealloc(p) };
        }
        heap.collect(true);
        assert!(
            heap.memory_usage() <= MEMLIMIT_COLLECT_TOLERANCE,
            "fuzz: usage not zero after cleanup"
        );
    }

    /// Fuzz-style multi-threaded: each thread owns its heap, random alloc/free.
    #[test]
    fn memlimit_fuzz_multithread() {
        let limit = 16 * 1024 * 1024;
        let violation = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));

        std::thread::scope(|s| {
            for tid in 0..4u64 {
                let violation = violation.clone();
                let barrier = barrier.clone();
                s.spawn(move || {
                    let heap = HeapMaster::new(limit).unwrap();
                    barrier.wait();
                    let mut live = Vec::new();
                    let sizes = [64, 512, 4096, 32768, 131072, 524288];
                    for i in 0..5_000u64 {
                        let hash = i
                            .wrapping_mul(2862933555777941757)
                            .wrapping_add(tid.wrapping_mul(3037000493));
                        let sz = sizes[(hash >> 48) as usize % sizes.len()];
                        if hash % 3 != 0 {
                            let p = heap.alloc(sz, 8);
                            if !p.is_null() {
                                live.push(p);
                            }
                            if heap.memory_usage() > limit + MEMLIMIT_MAX_OVERSHOOT {
                                violation.store(true, std::sync::atomic::Ordering::Relaxed);
                            }
                        } else if let Some(p) = live.pop() {
                            unsafe { heap.dealloc(p) };
                        }
                    }
                    for p in live {
                        unsafe { heap.dealloc(p) };
                    }
                    heap.collect(true);
                    assert!(
                        heap.memory_usage() <= MEMLIMIT_COLLECT_TOLERANCE,
                        "mt fuzz tid {}: usage not zero",
                        tid
                    );
                });
            }
        });
        assert!(
            !violation.load(std::sync::atomic::Ordering::Relaxed),
            "mt fuzz: limit exceeded"
        );
    }
}

//! Memory-limited mimalloc heap.
//!
//! [`HeapMaster`] owns a slab slot containing the heap metadata.
//! [`Heap`] is a cheap, `Copy` handle (raw pointer to `HeapData`).
//!
//! Heap slots live in a global [`FixedSlab`]. When a `HeapMaster`
//! drops, it force-collects the heap. If `memory_committed` reaches
//! zero the slot is marked RECYCLE (immediately reusable). Otherwise
//! it goes onto a **dirty free list** with state DIRTY. On the next
//! `HeapMaster::new`, the dirty list is scanned under a mutex:
//! slots whose `memory_committed` has since drained to zero are
//! reclaimed first; if none are clean a fresh slot is used; as a
//! last resort the least-dirty slot is picked and its residual
//! committed bytes are recorded as `initial_committed` so the new
//! owner's `memory_usage()` subtracts them.
//!
//! # Thread Safety
//!
//! mimalloc 3.x heaps are thread-safe for allocation from any thread.
//! `mi_free()` can also free from any thread.

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

const FLAG_PRESSURE: u8 = 1;
const FLAG_DROPPED: u8 = 2;

/// Global generation counter for debug use-after-recycle detection.
static HEAP_GENERATION: AtomicU32 = AtomicU32::new(1);

use allocator_api2::alloc::{AllocError, Allocator};

use bisque_mimalloc_sys::{self as ffi};

use crate::slab::FixedSlab;

// Re-export for use in async contexts.
pub use allocator_api2::alloc::AllocError as HeapAllocError;

/// Minimum arena size for tests with OOM scenarios.
/// Must be large enough for at least one segment (32 MiB on 64-bit).
#[cfg(test)]
const TEST_SMALL_ARENA: usize = 64 * 1024 * 1024; // 64 MiB

// =========================================================================
// Global slab + dirty free list
// =========================================================================

static HEAPS: std::sync::LazyLock<FixedSlab<HeapData, 2048>> =
    std::sync::LazyLock::new(|| FixedSlab::<HeapData, 2048>::new());

/// Dirty free list: slot ids whose mi_heap has non-zero memory_committed
/// at drop time. Protected by a mutex — only touched during HeapMaster
/// construction and destruction, never on the hot alloc/free path.
static DIRTY_LIST: parking_lot::Mutex<Vec<usize>> = parking_lot::Mutex::new(Vec::new());

// =========================================================================
// HeapData — metadata for a memory-limited mimalloc heap
// =========================================================================

/// Per-heap metadata, stored in a global slab slot.
///
/// Slots are never destroyed — the `mi_heap_t` stays alive across
/// recycles. `HeapMaster::drop` collects the heap; if committed
/// memory drains to zero the slot is immediately reusable, otherwise
/// it enters the dirty free list for deferred reclamation.
pub struct HeapData {
    pub(crate) mi_heap: *mut ffi::mi_heap_t,
    memory_limit: usize,
    /// Committed bytes at construction time. Subtracted in
    /// `memory_usage()` so the new owner sees only its own pages.
    initial_committed: usize,
    /// Monotonic generation — incremented each time the slot is recycled.
    /// Debug builds snapshot this in `Heap` and assert on every access
    /// to catch use-after-recycle.
    generation: u32,
    /// Bit flags (single atomic load on the free path):
    ///   bit 0 (PRESSURE): OOM pressure — track freed bytes, wake waiters
    ///   bit 1 (DROPPED):  HeapMaster dropped — force-collect after each free
    flags: AtomicU8,
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

impl Default for HeapData {
    fn default() -> Self {
        Self {
            mi_heap: std::ptr::null_mut(),
            memory_limit: 0,
            initial_committed: 0,
            generation: 0,
            flags: AtomicU8::new(0),
            freed_since_pressure: AtomicU64::new(0),
            pressure_wq: crate::wait_queue::WaitQueue::empty(),
            max_waiters: 0,
        }
    }
}

// =========================================================================
// HeapMaster — owns the slab slot, not cloneable
// =========================================================================

/// Owns a memory-limited mimalloc heap via a slab slot. Not cloneable.
///
/// Create cheap [`Heap`] handles via [`heap()`](HeapMaster::heap). The master
/// should outlive all `Heap` copies. When it drops, the slab slot is marked
/// for recycling — the next `HeapMaster::new` that picks up the slot
/// handles cleanup (free WaitQueue, collect pages, set new memlimit).
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
    id: usize,
    handle: Heap,
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

        // Try to reclaim a clean slot from the dirty list first.
        if let Some(master) = Self::try_from_dirty(max_bytes, max_waiters) {
            return Ok(master);
        }

        // Normal path: fresh or RECYCLE slot from the slab.
        let id = HEAPS.alloc().ok_or(AllocError)?;
        let slot = HEAPS.get_mut_ptr(id);
        let old_mi_heap = unsafe { (*slot).mi_heap };

        let mi_heap = if !old_mi_heap.is_null() {
            // RECYCLE slot: don't change the limit yet — init_slot
            // will collect under the old limit then set the new one.
            old_mi_heap
        } else {
            // Virgin slot: create new mi_heap.
            let h = unsafe { ffi::mi_heap_new() };
            if h.is_null() {
                HEAPS.free(id);
                return Err(AllocError);
            }
            h
        };

        Ok(Self::init_slot(id, mi_heap, max_bytes, max_waiters))
    }

    /// Scan the dirty list under the mutex. Collect each dirty heap and
    /// check if memory_committed has reached zero. Return the first clean
    /// one; otherwise fall back to the least-dirty slot if no fresh slab
    /// slots are available.
    fn try_from_dirty(max_bytes: usize, max_waiters: usize) -> Option<Self> {
        let mut dirty = DIRTY_LIST.lock();
        if dirty.is_empty() {
            return None;
        }

        // Scan dirty slots. Only read memory_usage (atomic, lock-free) —
        // never call mi_heap_collect here, as the heap's theaps on other
        // threads may still be active.
        let mut best_idx = None;
        let mut best_committed = usize::MAX;
        let mut clean_idx = None;

        for (i, &id) in dirty.iter().enumerate() {
            let mi_heap = unsafe { (*HEAPS.get_mut_ptr(id)).mi_heap };
            if mi_heap.is_null() {
                continue;
            }
            let committed = unsafe { ffi::mi_heap_get_memory_usage(mi_heap) };
            if committed == 0 {
                clean_idx = Some(i);
                break;
            }
            if committed < best_committed {
                best_committed = committed;
                best_idx = Some(i);
            }
        }

        // Take a clean slot if found.
        if let Some(i) = clean_idx {
            let id = dirty.swap_remove(i);
            if !HEAPS.try_claim_dirty(id) {
                return None;
            }
            let mi_heap = unsafe { (*HEAPS.get_mut_ptr(id)).mi_heap };
            return Some(Self::init_slot(id, mi_heap, max_bytes, max_waiters));
        }

        // No clean slot. Prefer a fresh slab slot over a dirty one.
        if let Some(fresh_id) = HEAPS.alloc() {
            HEAPS.free(fresh_id);
            return None;
        }

        // No fresh slots available. Take the least-dirty slot.
        let i = best_idx?;
        let id = dirty.swap_remove(i);
        drop(dirty);

        if !HEAPS.try_claim_dirty(id) {
            return None;
        }
        let mi_heap = unsafe { (*HEAPS.get_mut_ptr(id)).mi_heap };
        Some(Self::init_slot(id, mi_heap, max_bytes, max_waiters))
    }

    /// Write HeapData into the claimed slab slot and return the HeapMaster.
    fn init_slot(
        id: usize,
        mi_heap: *mut ffi::mi_heap_t,
        max_bytes: usize,
        max_waiters: usize,
    ) -> Self {
        let slot = HEAPS.get_mut_ptr(id);

        // Set the new limit before snapshotting so the limit is active
        // for all subsequent page push/free tracking.
        if max_bytes > 0 {
            unsafe { ffi::mi_heap_set_memory_limit(mi_heap, max_bytes) };
        } else {
            unsafe { ffi::mi_heap_set_memory_limit(mi_heap, 0) };
        }

        // Snapshot the current committed memory as the baseline.
        // memory_committed is always accurate — never corrupted.
        let initial_committed = unsafe { ffi::mi_heap_get_memory_usage(mi_heap) };

        let pressure_wq = unsafe { crate::wait_queue::WaitQueue::new_in(max_waiters, mi_heap) };
        let generation = HEAP_GENERATION.fetch_add(1, Ordering::Relaxed);

        unsafe {
            std::ptr::write(
                slot,
                HeapData {
                    mi_heap,
                    memory_limit: max_bytes,
                    initial_committed,
                    generation,
                    flags: AtomicU8::new(0),
                    freed_since_pressure: AtomicU64::new(0),
                    pressure_wq,
                    max_waiters,
                },
            );
        }

        let handle = Heap {
            ptr: HEAPS.get(id) as *const HeapData,
        };

        Self { id, handle }
    }

    /// Create a cheap, copyable [`Heap`] handle.
    #[inline]
    pub fn heap(&self) -> Heap {
        self.handle
    }

    #[inline(always)]
    fn data(&self) -> &HeapData {
        self.handle.data()
    }
}

impl Drop for HeapMaster {
    fn drop(&mut self) {
        let mi_heap = self.data().mi_heap;

        // Set DROPPED flag. Subsequent frees via free_with_pressure will
        // force-collect the freeing thread's theap after each mi_free.
        self.data().flags.fetch_or(FLAG_DROPPED, Ordering::Release);

        // Force-collect this thread's theap now.
        unsafe { ffi::mi_heap_collect(mi_heap, true) };

        let committed = unsafe { ffi::mi_heap_get_memory_usage(mi_heap) };
        if committed == 0 {
            HEAPS.recycle(self.id);
        } else {
            // Ghost pages on other threads' theaps remain. Park in the
            // dirty list — try_from_dirty will collect(false) later to
            // drain them, or theaps will self-collect when alloc_count→0.
            HEAPS.mark_dirty(self.id);
            DIRTY_LIST.lock().push(self.id);
        }
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
// Heap — cheap copyable handle
// =========================================================================

/// Cheap, copyable handle to a memory-limited mimalloc heap.
///
/// Created from a [`HeapMaster`] via [`heap()`](HeapMaster::heap) or by
/// copying an existing `Heap`. `Copy` — no reference counting overhead.
#[repr(C)]
pub struct Heap {
    ptr: *const HeapData,
}

unsafe impl Send for Heap {}
unsafe impl Sync for Heap {}

impl Copy for Heap {}

impl Clone for Heap {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl Heap {
    /// Create a new memory-limited heap.
    ///
    /// `max_bytes` sets the per-heap memory limit (0 = unlimited).
    /// Tracked at 64 KiB slice granularity.
    pub fn new(max_bytes: usize) -> Result<HeapMaster, AllocError> {
        HeapMaster::new(max_bytes)
    }

    #[inline(always)]
    pub(crate) fn data(&self) -> &HeapData {
        unsafe { &*self.ptr }
    }

    /// Raw pointer to the HeapData. The caller must ensure the
    /// HeapData outlives any use of this pointer.
    #[inline(always)]
    pub(crate) fn data_ptr(&self) -> *const HeapData {
        self.ptr
    }

    /// Free a pointer using a raw `HeapData` pointer.
    ///
    /// # Safety
    /// - `heap_data` must point to a valid, live `HeapData`.
    /// - `ptr` must have been allocated by mimalloc.
    #[inline]
    pub(crate) unsafe fn dealloc_raw(heap_data: *const HeapData, ptr: *mut u8) {
        unsafe { Self::free_with_pressure(&*heap_data, ptr) };
    }

    /// Reconstruct a `Heap` handle from a raw `HeapData` pointer.
    ///
    /// This is free — `Heap` is `Copy` and just wraps the pointer.
    ///
    /// # Safety
    /// `heap_data` must point into a live slab slot.
    #[inline]
    pub(crate) unsafe fn from_data_ptr(heap_data: *const HeapData) -> Self {
        Self { ptr: heap_data }
    }

    /// Memory limit in bytes (0 = unlimited).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data().memory_limit
    }

    /// Current memory usage in bytes (tracked at 64 KiB slice granularity).
    /// Subtracts the baseline inherited from a recycled heap so the caller
    /// sees only this owner's committed pages.
    #[inline]
    pub fn memory_usage(&self) -> usize {
        let raw = unsafe { ffi::mi_heap_get_memory_usage(self.data().mi_heap) };
        raw.saturating_sub(self.data().initial_committed)
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
        unsafe { Self::free_with_pressure(self.data(), ptr) };
    }

    /// Free a pointer with pressure notification. Used by all drop paths
    /// (Bytes, BytesMut, collections::Vec, Box, Arc) to ensure memory
    /// pressure waiters are notified regardless of which type frees.
    ///
    /// # Safety
    /// `ptr` must have been allocated by mimalloc.
    #[inline]
    pub(crate) unsafe fn free_with_pressure(inner: &HeapData, ptr: *mut u8) {
        let flags = inner.flags.load(Ordering::Relaxed);
        if flags != 0 {
            Self::free_slow(inner, ptr, flags);
            return;
        }
        unsafe { ffi::mi_free(ptr as *mut core::ffi::c_void) }
    }

    #[cold]
    #[inline(never)]
    fn free_slow(inner: &HeapData, ptr: *mut u8, flags: u8) {
        if flags & FLAG_PRESSURE != 0 {
            Self::free_with_pressure_slow(inner, ptr);
        }
        unsafe { ffi::mi_free(ptr as *mut core::ffi::c_void) };
        if flags & FLAG_DROPPED != 0 {
            unsafe { ffi::mi_heap_collect(inner.mi_heap, true) };
        }
    }

    #[cold]
    #[inline(never)]
    fn free_with_pressure_slow(inner: &HeapData, ptr: *mut u8) {
        let freed = unsafe { ffi::mi_usable_size(ptr as *const core::ffi::c_void) };
        inner
            .freed_since_pressure
            .fetch_add(freed as u64, Ordering::Relaxed);
        inner.pressure_wq.notify_one();
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
        inner.flags.fetch_or(FLAG_PRESSURE, Ordering::Release);
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
            inner.flags.fetch_and(!FLAG_PRESSURE, Ordering::Release);
            inner.freed_since_pressure.store(0, Ordering::Relaxed);
        }
    }

    /// Returns true if the heap is currently in memory pressure mode.
    #[inline]
    pub fn is_under_pressure(&self) -> bool {
        self.data().flags.load(Ordering::Relaxed) & FLAG_PRESSURE != 0
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
pub type HeapVec = crate::Vec<u8>;

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
    #[ignore]
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
        let h2 = h1;
        let ptr = h1.alloc(256, 8);
        unsafe { h2.dealloc(ptr) };
    }

    #[test]
    fn heap_copy_semantics() {
        let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let h1 = master.heap();
        let h2 = h1; // Copy
        let h3 = h1; // Still valid — h1 is Copy
        drop(master);
        // h1, h2, h3 are all dangling now (master dropped), but no
        // double-free or refcount issues since Heap has no Drop.
        let _ = (h2, h3);
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

    /// Isolated reproducer: mi_heap_destroy after cross-thread theap usage.
    /// Pure mimalloc API, no Rust wrappers.
    #[test]
    fn heap_destroy_after_cross_thread_use() {
        for _ in 0..200 {
            let heap = unsafe { ffi::mi_heap_new() };
            assert!(!heap.is_null());
            let heap_addr = heap as usize;
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));

            let mut handles = Vec::new();
            for _ in 0..8 {
                let barrier = barrier.clone();
                handles.push(std::thread::spawn(move || {
                    let heap = heap_addr as *mut ffi::mi_heap_t;
                    for _ in 0..500 {
                        let p = unsafe { ffi::mi_heap_malloc(heap, 16) };
                        assert!(!p.is_null());
                        unsafe { ffi::mi_free(p) };
                    }
                    barrier.wait();
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            // All threads joined and barriers passed.
            unsafe { ffi::mi_heap_destroy(heap) };
        }
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

    /// Zero overshoot: memory_usage never exceeds memory_limit.
    /// The atomic fetch_add check in mi_heap_memlimit_page_over rejects any page
    /// that would push usage above the limit. For memlimit heaps, page abandonment
    /// is disabled so full pages stay tracked in the full queue.
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

    /// Fill to OOM, free half, collect to retire empty pages, allocate more.
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

        // Free half — pages stay in full queue until collected
        let half = ptrs.len() / 2;
        for p in ptrs.drain(..half) {
            unsafe { heap.dealloc(p) };
        }
        // Collect to retire now-empty pages and release their tracked usage
        heap.collect(true);
        let usage_after_free = heap.memory_usage();
        assert!(
            usage_after_free < usage_at_full,
            "usage_after_free={} should be < usage_at_full={}",
            usage_after_free,
            usage_at_full
        );

        // Allocate more — should succeed by reusing freed block slots in existing pages
        let mut more = Vec::new();
        for _ in 0..half {
            let p = heap.alloc(1024, 8);
            if p.is_null() {
                break;
            }
            more.push(p);
            assert!(
                heap.memory_usage() <= limit,
                "usage {} exceeded limit {} during re-alloc",
                heap.memory_usage(),
                limit
            );
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

    /// Different size classes: each alloc that requires a new page should
    /// increase memory_usage. Allocs that reuse a ghost page (from a
    /// recycled heap) won't increase usage — that's correct (the page
    /// is already committed). We verify monotonic non-decrease and that
    /// at least *some* usage is registered after all three allocs.
    #[test]
    fn memlimit_size_class_accounting() {
        let heap = HeapMaster::new(64 * 1024 * 1024).unwrap();

        // Small: <= 8 KiB → 64 KiB page (1 slice)
        let p1 = heap.alloc(64, 8);
        assert!(!p1.is_null());
        let u1 = heap.memory_usage();

        // Medium: 8 KiB < size <= 64 KiB → 512 KiB page (8 slices)
        let p2 = heap.alloc(16 * 1024, 8);
        assert!(!p2.is_null());
        let u2 = heap.memory_usage();
        assert!(u2 >= u1, "usage should not decrease after medium alloc");

        // Large: 64 KiB < size <= 16 MiB
        let p3 = heap.alloc(256 * 1024, 8);
        assert!(!p3.is_null());
        let u3 = heap.memory_usage();
        assert!(u3 >= u2, "usage should not decrease after large alloc");

        // Free all — usage tracks pages, not blocks.
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

    /// Shared heap: worker threads allocate, exit (theap deleted), main thread
    /// continues using the heap. Exercises the theap page-transfer path where
    /// a dying theap transfers its pages to a sibling instead of abandoning.
    #[test]
    fn memlimit_shared_heap_worker_exit() {
        let limit = 8 * 1024 * 1024;
        let master = HeapMaster::new(limit).unwrap();

        // Phase 1: workers allocate on the shared heap, then exit.
        // Their theaps are deleted — pages must transfer to a sibling theap.
        let ptrs: Vec<*mut u8> = std::thread::scope(|s| {
            let mut handles = Vec::new();
            for _ in 0..4 {
                let h = master.heap();
                handles.push(s.spawn(move || {
                    let mut local = Vec::new();
                    for _ in 0..200 {
                        let p = h.alloc(256, 8);
                        if !p.is_null() {
                            unsafe { std::ptr::write_bytes(p, 0xAB, 256) };
                            local.push(p as usize);
                        }
                    }
                    local
                }));
            }
            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .map(|addr| addr as *mut u8)
                .collect()
        });
        // All worker threads have exited. Their theaps are deleted.
        // Pages should have been transferred, not abandoned.
        assert!(!ptrs.is_empty(), "should have allocated blocks");
        assert!(
            master.memory_usage() <= limit,
            "usage {} > limit {} after worker exit",
            master.memory_usage(),
            limit
        );

        // Phase 2: verify data integrity (pages weren't corrupted by transfer).
        for &p in &ptrs {
            let slice = unsafe { std::slice::from_raw_parts(p, 256) };
            assert!(
                slice.iter().all(|&b| b == 0xAB),
                "data corrupted after theap transfer"
            );
        }

        // Phase 3: main thread can still allocate on the same heap.
        let p = master.alloc(1024, 8);
        assert!(!p.is_null(), "alloc failed after worker exit");
        unsafe { master.dealloc(p) };

        // Phase 4: free worker allocations from main thread (cross-theap free).
        for &p in &ptrs {
            unsafe { master.dealloc(p) };
        }
        master.collect(true);
        assert!(
            master.memory_usage() <= MEMLIMIT_COLLECT_TOLERANCE,
            "usage not zero after cleanup: {}",
            master.memory_usage()
        );
    }

    /// Shared heap: rapid worker spawn/exit cycles. Each cycle spawns N workers
    /// that allocate on the shared heap, then all exit. Tests repeated theap
    /// creation and page-transfer under churn.
    #[test]
    fn memlimit_shared_heap_worker_churn() {
        let limit = 8 * 1024 * 1024;
        let master = HeapMaster::new(limit).unwrap();

        for round in 0..20 {
            // Spawn workers, allocate, exit
            let ptrs: Vec<*mut u8> = std::thread::scope(|s| {
                let mut handles = Vec::new();
                for _ in 0..4 {
                    let h = master.heap();
                    handles.push(s.spawn(move || {
                        let mut local = Vec::new();
                        for _ in 0..100 {
                            let p = h.alloc(512, 8);
                            if !p.is_null() {
                                local.push(p as usize);
                            }
                        }
                        local
                    }));
                }
                handles
                    .into_iter()
                    .flat_map(|h| h.join().unwrap())
                    .map(|addr| addr as *mut u8)
                    .collect()
            });
            // Workers exited — free their allocations from main thread.
            for &p in &ptrs {
                unsafe { master.dealloc(p) };
            }
            // Usage must stay bounded across rounds.
            assert!(
                master.memory_usage() <= limit,
                "round {round}: usage {} > limit {}",
                master.memory_usage(),
                limit
            );
        }
        master.collect(true);
        assert!(
            master.memory_usage() <= MEMLIMIT_COLLECT_TOLERANCE,
            "usage not zero after churn: {}",
            master.memory_usage()
        );
    }
}

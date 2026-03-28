//! Epoch reclamation with per-epoch reader counters.
//!
//! Alternative to `heap.rs` watermark-scan approach. Instead of scanning all
//! thread slots for the minimum pinned epoch, each Epoch maintains a ring of
//! per-thread reader counters. Reclaim checks `is_clear(epoch)` — O(thread_count)
//! reads for one specific epoch, driven by the min-heap rather than a global scan.
//!
//! **Pin**: increment `counters[epoch % RING][thread_idx]` — own slot, zero contention.
//! **Unpin**: decrement same slot. If `needs_reclaim` set, trigger reclaim.
//! **Reclaim**: pop from min-heap while `is_clear(oldest_epoch)`.
//!
//! No nesting logic needed — each guard independently increments/decrements.
//! No watermark scan — per-epoch counters give exact reader presence.

use std::cell::UnsafeCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};

use parking_lot::Mutex;

use crate::CachePadded;
use crate::Heap;

// ═══════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════

const RING_BITS: usize = 6;
const RING_SIZE: usize = 1 << RING_BITS; // 64 epoch slots
const RING_MASK: usize = RING_SIZE - 1;

const MAX_COLLECTORS: usize = 256;
const MAX_THREADS: usize = 1024;
const UNPINNED: u64 = u64::MAX;
const CLAIMED: u64 = u64::MAX - 1;

// ═══════════════════════════════════════════════════════════════════════════
// RetiredBag (same as heap.rs)
// ═══════════════════════════════════════════════════════════════════════════

pub type DeallocFn = unsafe fn(&Heap, usize);

enum RetiredPtrs {
    One(usize),
    Many(crate::Vec<usize>),
}

impl RetiredPtrs {
    #[inline]
    fn len(&self) -> usize {
        match self {
            RetiredPtrs::One(_) => 1,
            RetiredPtrs::Many(v) => v.len(),
        }
    }

    #[inline]
    fn for_each(&self, heap: &Heap, dealloc: DeallocFn) {
        match self {
            RetiredPtrs::One(ptr) => unsafe { dealloc(heap, *ptr) },
            RetiredPtrs::Many(ptrs) => {
                for &ptr in ptrs.as_slice() {
                    unsafe { dealloc(heap, ptr) };
                }
            }
        }
    }
}

struct RetiredBag {
    epoch: u64,
    heap: Heap,
    dealloc: DeallocFn,
    ptrs: RetiredPtrs,
}

impl PartialEq for RetiredBag {
    fn eq(&self, other: &Self) -> bool { self.epoch == other.epoch }
}
impl Eq for RetiredBag {}
impl PartialOrd for RetiredBag {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Ord for RetiredBag {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.epoch.cmp(&other.epoch) }
}

// ═══════════════════════════════════════════════════════════════════════════
// Per-epoch reader counters — ring of per-thread counters
// ═══════════════════════════════════════════════════════════════════════════

/// Ring of per-epoch, per-thread reader counters.
///
/// `counters[epoch % RING_SIZE][thread_idx]` holds the reader count for that
/// thread at that epoch. Each thread only touches its own slot — zero contention.
///
/// The ring has RING_SIZE=64 slots. If a reader holds a pin across 64 publishes,
/// the ring wraps and the counter is stale. In practice pins are microseconds;
/// 64 publishes takes orders of magnitude longer.
/// Per-thread counter slot, cache-line padded to eliminate false sharing.
/// Only the owning thread writes; readers (reclaimer) do Acquire loads.
/// Uses AtomicI32 but writes are non-contended load+store (not fetch_add).
type CounterSlot = CachePadded<AtomicI32>;

struct EpochCounters {
    /// ring[epoch % RING_SIZE][thread_idx] — per-thread, per-epoch reader count.
    /// Each thread exclusively owns its slot — writes are plain load+store.
    /// The reclaimer reads all slots to check if an epoch is clear.
    ring: Box<[Box<[CounterSlot]>; RING_SIZE]>,
}

impl EpochCounters {
    fn new() -> Self {
        let ring: Vec<Box<[CounterSlot]>> = (0..RING_SIZE)
            .map(|_| {
                (0..MAX_THREADS)
                    .map(|_| CachePadded::new(AtomicI32::new(0)))
                    .collect::<Vec<_>>()
                    .into_boxed_slice()
            })
            .collect();
        Self {
            ring: ring.into_boxed_slice().try_into().unwrap(),
        }
    }

    /// Increment: only called by the owning thread. Plain load + store.
    #[inline]
    fn increment(&self, epoch: u64, thread_idx: usize) {
        let slot = &self.ring[epoch as usize & RING_MASK][thread_idx];
        let val = slot.load(Ordering::Relaxed);
        slot.store(val + 1, Ordering::Release);
    }

    /// Decrement: only called by the owning thread. Plain load + store.
    #[inline]
    fn decrement(&self, epoch: u64, thread_idx: usize) {
        let slot = &self.ring[epoch as usize & RING_MASK][thread_idx];
        let val = slot.load(Ordering::Relaxed);
        slot.store(val - 1, Ordering::Release);
    }

    /// Check if an epoch has zero readers across all threads.
    fn is_clear(&self, epoch: u64, thread_count: usize) -> bool {
        let slot = &self.ring[epoch as usize & RING_MASK];
        for i in 0..thread_count {
            if slot[i].load(Ordering::Acquire) != 0 {
                return false;
            }
        }
        true
    }

    #[cfg(test)]
    fn readers_at(&self, epoch: u64, thread_count: usize) -> u32 {
        let slot = &self.ring[epoch as usize & RING_MASK];
        let mut total = 0u32;
        for i in 0..thread_count {
            let v = slot[i].load(Ordering::Relaxed);
            if v > 0 { total += v as u32; }
        }
        total
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Global Collector registry (reuse same pattern as heap.rs)
// ═══════════════════════════════════════════════════════════════════════════

struct CollectorSlot {
    state: CachePadded<AtomicU64>,
    needs_reclaim: AtomicBool,
}

struct CollectorRegistry {
    slots: Box<[CollectorSlot]>,
    count: AtomicUsize,
}

static COLLECTORS: std::sync::LazyLock<CollectorRegistry> = std::sync::LazyLock::new(|| {
    let slots: Vec<CollectorSlot> = (0..MAX_COLLECTORS)
        .map(|_| CollectorSlot {
            state: CachePadded::new(AtomicU64::new(UNPINNED)),
            needs_reclaim: AtomicBool::new(false),
        })
        .collect();
    CollectorRegistry {
        slots: slots.into_boxed_slice(),
        count: AtomicUsize::new(0),
    }
});

// ═══════════════════════════════════════════════════════════════════════════
// Global Thread registry — only garbage storage, no epoch pins
// ═══════════════════════════════════════════════════════════════════════════

struct CollectorGarbage {
    garbage: Mutex<BinaryHeap<Reverse<RetiredBag>>>,
}

struct ThreadSlot {
    state: CachePadded<AtomicU64>,
    collectors: Box<[CollectorGarbage]>,
}

struct ThreadRegistry {
    slots: Box<[ThreadSlot]>,
    count: AtomicUsize,
}

static THREADS: std::sync::LazyLock<ThreadRegistry> = std::sync::LazyLock::new(|| {
    let slots: Vec<ThreadSlot> = (0..MAX_THREADS)
        .map(|_| {
            let collectors: Vec<CollectorGarbage> = (0..MAX_COLLECTORS)
                .map(|_| CollectorGarbage {
                    garbage: Mutex::new(BinaryHeap::new()),
                })
                .collect();
            ThreadSlot {
                state: CachePadded::new(AtomicU64::new(UNPINNED)),
                collectors: collectors.into_boxed_slice(),
            }
        })
        .collect();
    ThreadRegistry {
        slots: slots.into_boxed_slice(),
        count: AtomicUsize::new(0),
    }
});

// ═══════════════════════════════════════════════════════════════════════════
// Thread slot allocation (same as heap.rs)
// ═══════════════════════════════════════════════════════════════════════════

fn alloc_thread_slot() -> usize {
    let reg = &*THREADS;
    let count = reg.count.load(Ordering::Acquire);
    for i in 0..count {
        if reg.slots[i]
            .state
            .compare_exchange_weak(UNPINNED, CLAIMED, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return i;
        }
    }
    let idx = reg.count.fetch_add(1, Ordering::AcqRel);
    assert!(idx < MAX_THREADS, "too many threads (max {MAX_THREADS})");
    reg.slots[idx].state.store(CLAIMED, Ordering::Release);
    idx
}

struct ThreadHandle { idx: usize }

impl Drop for ThreadHandle {
    fn drop(&mut self) {
        THREADS.slots[self.idx].state.store(UNPINNED, Ordering::Release);
    }
}

const UNSET: usize = usize::MAX;

struct ThreadLocal {
    idx: UnsafeCell<usize>,
    handle: UnsafeCell<Option<ThreadHandle>>,
}

unsafe impl Sync for ThreadLocal {}

thread_local! {
    static TL: ThreadLocal = const {
        ThreadLocal {
            idx: UnsafeCell::new(UNSET),
            handle: UnsafeCell::new(None),
        }
    };
}

#[inline]
fn current_thread_idx() -> usize {
    TL.with(|tl| {
        let idx = unsafe { *tl.idx.get() };
        if idx != UNSET {
            return idx;
        }
        let idx = alloc_thread_slot();
        unsafe {
            *tl.idx.get() = idx;
            *tl.handle.get() = Some(ThreadHandle { idx });
        }
        idx
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// Collector
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
pub struct Collector {
    id: usize,
}

impl Collector {
    pub fn new() -> Self {
        let reg = &*COLLECTORS;
        let _ = &*THREADS;
        let count = reg.count.load(Ordering::Acquire);
        for i in 0..count {
            if reg.slots[i]
                .state
                .compare_exchange_weak(UNPINNED, CLAIMED, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Self { id: i };
            }
        }
        let id = reg.count.fetch_add(1, Ordering::AcqRel);
        assert!(id < MAX_COLLECTORS, "too many collectors (max {MAX_COLLECTORS})");
        reg.slots[id].state.store(CLAIMED, Ordering::Release);
        Self { id }
    }

    #[inline]
    pub fn id(&self) -> usize { self.id }

    /// Reclaim using per-epoch counters from the Epoch's ring.
    fn try_reclaim(&self, counters: &EpochCounters) {
        let creg = &COLLECTORS.slots[self.id];

        if !creg.needs_reclaim.load(Ordering::Relaxed) {
            return;
        }

        if creg
            .needs_reclaim
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let thread_count = THREADS.count.load(Ordering::Acquire);
        let mut any_remaining = false;

        for i in 0..thread_count {
            let cg = &THREADS.slots[i].collectors[self.id];
            if let Some(mut garbage) = cg.garbage.try_lock() {
                while let Some(Reverse(oldest)) = garbage.peek() {
                    if counters.is_clear(oldest.epoch, thread_count) {
                        let Reverse(bag) = garbage.pop().unwrap();
                        bag.ptrs.for_each(&bag.heap, bag.dealloc);
                    } else {
                        break;
                    }
                }
                if !garbage.is_empty() {
                    any_remaining = true;
                }
            } else {
                any_remaining = true;
            }
        }

        if any_remaining {
            creg.needs_reclaim.store(true, Ordering::Release);
        }
    }

    fn force_reclaim_all(&self) {
        let thread_count = THREADS.count.load(Ordering::Acquire);
        for i in 0..thread_count {
            let cg = &THREADS.slots[i].collectors[self.id];
            let mut garbage = cg.garbage.lock();
            while let Some(Reverse(bag)) = garbage.pop() {
                bag.ptrs.for_each(&bag.heap, bag.dealloc);
            }
        }
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.force_reclaim_all();
        COLLECTORS.slots[self.id].state.store(UNPINNED, Ordering::Release);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Epoch — per-structure epoch with reader counters
// ═══════════════════════════════════════════════════════════════════════════

pub struct Epoch {
    /// Writer's authoritative epoch counter.
    epoch: CachePadded<AtomicU64>,
    /// Per-thread broadcast of the current epoch. Writer stores to all;
    /// each reader loads only its own slot (always L1, never bounced).
    reader_epochs: Box<[CachePadded<AtomicU64>]>,
    collector_id: usize,
    heap: Heap,
    dealloc: DeallocFn,
    counters: EpochCounters,
}

/// Guard that increments/decrements the per-epoch reader counter.
/// No nesting logic needed — each guard is independent.
pub struct EpochGuard<'a> {
    epoch_handle: &'a Epoch,
    pinned_epoch: u64,
    thread_idx: usize,
    _not_send: std::marker::PhantomData<*const ()>,
}

impl Epoch {
    pub fn new(collector: &Collector, heap: &Heap, dealloc: DeallocFn) -> Self {
        let reader_epochs: Vec<CachePadded<AtomicU64>> = (0..MAX_THREADS)
            .map(|_| CachePadded::new(AtomicU64::new(1)))
            .collect();
        Self {
            epoch: CachePadded::new(AtomicU64::new(1)),
            reader_epochs: reader_epochs.into_boxed_slice(),
            collector_id: collector.id,
            heap: heap.clone(),
            dealloc,
            counters: EpochCounters::new(),
        }
    }

    #[inline]
    pub fn current(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Pin: read epoch from own thread-local slot (L1 hit), increment counter.
    #[inline]
    pub fn pin(&self) -> EpochGuard<'_> {
        let tidx = current_thread_idx();
        // Read from our own cacheline — never invalidated by other readers.
        // Writer stores here infrequently (on advance), so it's hot in L1
        // between advances.
        let epoch = self.reader_epochs[tidx].load(Ordering::Acquire);
        self.counters.increment(epoch, tidx);
        EpochGuard {
            epoch_handle: self,
            pinned_epoch: epoch,
            thread_idx: tidx,
            _not_send: std::marker::PhantomData,
        }
    }

    /// Advance epoch by 1. Broadcasts to all reader slots. Triggers reclaim.
    pub fn advance(&self) -> u64 {
        let new = self.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        // Broadcast to all active reader slots.
        let thread_count = THREADS.count.load(Ordering::Acquire);
        for i in 0..thread_count {
            self.reader_epochs[i].store(new, Ordering::Release);
        }
        let creg = &COLLECTORS.slots[self.collector_id];
        if creg.needs_reclaim.load(Ordering::Relaxed) {
            self.collector_ref().try_reclaim(&self.counters);
        }
        new
    }

    pub fn retire(&self, ptrs: crate::Vec<usize>) {
        if ptrs.is_empty() { return; }
        self.retire_inner(RetiredPtrs::Many(ptrs));
    }

    #[inline]
    pub fn retire_one(&self, ptr: usize) {
        if ptr == 0 { return; }
        self.retire_inner(RetiredPtrs::One(ptr));
    }

    pub fn retire_drain(&self, garbage: &mut crate::Vec<usize>) {
        if garbage.is_empty() { return; }
        let ptrs = std::mem::replace(garbage, crate::Vec::new(&self.heap));
        self.retire(ptrs);
    }

    #[inline]
    fn retire_inner(&self, ptrs: RetiredPtrs) {
        let epoch = self.epoch.load(Ordering::Relaxed);
        let tidx = current_thread_idx();
        THREADS.slots[tidx].collectors[self.collector_id]
            .garbage
            .lock()
            .push(Reverse(RetiredBag {
                epoch,
                heap: self.heap.clone(),
                dealloc: self.dealloc,
                ptrs,
            }));
        COLLECTORS.slots[self.collector_id]
            .needs_reclaim
            .store(true, Ordering::Release);
    }

    fn collector_ref(&self) -> std::mem::ManuallyDrop<Collector> {
        std::mem::ManuallyDrop::new(Collector { id: self.collector_id })
    }

    #[cfg(test)]
    pub fn garbage_count(&self) -> usize {
        let thread_count = THREADS.count.load(Ordering::Acquire);
        let mut total = 0;
        for i in 0..thread_count {
            let garbage = THREADS.slots[i].collectors[self.collector_id]
                .garbage.lock();
            for Reverse(bag) in garbage.iter() {
                total += bag.ptrs.len();
            }
        }
        total
    }

    #[cfg(test)]
    pub fn readers_at(&self, target_epoch: u64) -> u32 {
        let thread_count = THREADS.count.load(Ordering::Acquire);
        self.counters.readers_at(target_epoch, thread_count)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// EpochGuard
// ═══════════════════════════════════════════════════════════════════════════

impl<'a> EpochGuard<'a> {
    #[inline]
    pub fn epoch(&self) -> u64 {
        self.pinned_epoch
    }
}

impl Drop for EpochGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.epoch_handle.counters.decrement(self.pinned_epoch, self.thread_idx);
        let creg = &COLLECTORS.slots[self.epoch_handle.collector_id];
        if creg.needs_reclaim.load(Ordering::Relaxed) {
            self.epoch_handle.collector_ref().try_reclaim(&self.epoch_handle.counters);
        }
    }
}

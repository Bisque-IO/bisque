//! Heap-epoch reclamation with per-Collector thread-local pin slots.
//!
//! **Collector**: owns the slot registry, reclaim flag, and garbage queue.
//! **Pin/unpin**: plain store to a per-thread, per-Collector AtomicU64. Zero contention.
//! **Retire**: push to a single lock-free SegQueue per Collector.
//! **Reclaim**: single winner via CAS, pops from the SegQueue.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

use crate::CachePadded;
use crate::Heap;
use crate::collections::seg_queue::SegQueue;

// ═══════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════

const UNPINNED: u64 = u64::MAX;
const CLAIMED: u64 = u64::MAX - 1;
const MAX_COLLECTORS: usize = 4096;
const MAX_THREADS: usize = 1024;

// ═══════════════════════════════════════════════════════════════════════════
// SealedGarbage — a batch of pointers tagged with the epoch they were sealed at
// ═══════════════════════════════════════════════════════════════════════════

pub type DeallocFn = unsafe fn(&Heap, usize);

struct SealedGarbage {
    epoch: u64,
    ptrs: crate::Vec<usize>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Global Collector registry
// ═══════════════════════════════════════════════════════════════════════════

struct CollectorSlot {
    state: CachePadded<AtomicU64>,
    needs_reclaim: CachePadded<AtomicBool>,
    last_watermark: CachePadded<AtomicU64>,
    skip_count: CachePadded<AtomicU32>,
}

struct CollectorRegistry {
    slots: Box<[CollectorSlot]>,
    count: AtomicUsize,
}

static COLLECTORS: std::sync::LazyLock<CollectorRegistry> = std::sync::LazyLock::new(|| {
    let slots: Vec<CollectorSlot> = (0..MAX_COLLECTORS)
        .map(|_| CollectorSlot {
            state: CachePadded::new(AtomicU64::new(UNPINNED)),
            needs_reclaim: CachePadded::new(AtomicBool::new(false)),
            last_watermark: CachePadded::new(AtomicU64::new(0)),
            skip_count: CachePadded::new(AtomicU32::new(0)),
        })
        .collect();
    CollectorRegistry {
        slots: slots.into_boxed_slice(),
        count: AtomicUsize::new(0),
    }
});

// ═══════════════════════════════════════════════════════════════════════════
// Global Thread registry — pin slots only, no garbage storage
// ═══════════════════════════════════════════════════════════════════════════

struct CollectorPin {
    epoch: CachePadded<AtomicU64>,
    /// Per-thread lock-free garbage queue. Writer pushes, reclaimer pops.
    garbage: SegQueue<SealedGarbage>,
}

struct ThreadSlot {
    state: CachePadded<AtomicU64>,
    collectors: Box<[CollectorPin]>,
}

struct ThreadRegistry {
    slots: Box<[ThreadSlot]>,
    count: AtomicUsize,
}

static THREADS: std::sync::LazyLock<ThreadRegistry> = std::sync::LazyLock::new(|| {
    let slots: Vec<ThreadSlot> = (0..MAX_THREADS)
        .map(|_| {
            let collectors: Vec<CollectorPin> = (0..MAX_COLLECTORS)
                .map(|_| CollectorPin {
                    epoch: CachePadded::new(AtomicU64::new(UNPINNED)),
                    garbage: SegQueue::new(),
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
// Thread slot allocation
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

struct ThreadHandle {
    idx: usize,
}

impl Drop for ThreadHandle {
    fn drop(&mut self) {
        THREADS.slots[self.idx]
            .state
            .store(UNPINNED, Ordering::Release);
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
        assert!(
            id < MAX_COLLECTORS,
            "too many collectors (max {MAX_COLLECTORS})"
        );
        reg.slots[id].state.store(CLAIMED, Ordering::Release);
        Self { id }
    }

    #[inline]
    pub fn id(&self) -> usize {
        self.id
    }

    fn try_reclaim(&self, from_advance: bool, heap: &Heap, dealloc: DeallocFn) {
        let creg = &COLLECTORS.slots[self.id];

        if !creg.needs_reclaim.load(Ordering::Relaxed) {
            return;
        }

        if from_advance {
            let skip = creg.skip_count.load(Ordering::Relaxed);
            if skip > 0 {
                creg.skip_count.store(skip - 1, Ordering::Relaxed);
                return;
            }
        } else {
            creg.skip_count.store(0, Ordering::Relaxed);
        }

        if creg
            .needs_reclaim
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let thread_count = THREADS.count.load(Ordering::Acquire);
        let mut watermark = UNPINNED;
        for i in 0..thread_count {
            let val = THREADS.slots[i].collectors[self.id]
                .epoch
                .load(Ordering::Acquire);
            if val < watermark {
                watermark = val;
            }
        }

        let safe_below = if watermark == UNPINNED {
            u64::MAX
        } else if watermark == 0 {
            creg.needs_reclaim.store(true, Ordering::Release);
            return;
        } else {
            watermark
        };

        let prev = creg.last_watermark.load(Ordering::Relaxed);
        if safe_below <= prev && safe_below != u64::MAX {
            let prev_skip = creg.skip_count.load(Ordering::Relaxed);
            creg.skip_count
                .store((prev_skip + 1).min(32), Ordering::Relaxed);
            creg.needs_reclaim.store(true, Ordering::Release);
            return;
        }
        creg.last_watermark.store(safe_below, Ordering::Relaxed);
        creg.skip_count.store(0, Ordering::Relaxed);

        // Drain per-thread garbage queues.
        let mut any_remaining = false;
        for i in 0..thread_count {
            let cpin = &THREADS.slots[i].collectors[self.id];
            loop {
                match cpin.garbage.pop::<false>() {
                    Some(bag) if bag.epoch < safe_below => {
                        for &ptr in bag.ptrs.as_slice() {
                            unsafe { dealloc(heap, ptr) };
                        }
                    }
                    Some(bag) => {
                        // Not safe yet — push back. It'll be at the back of the
                        // queue, but since all remaining bags have epoch >= this
                        // one, ordering is preserved for this thread's queue.
                        cpin.garbage.push(bag);
                        any_remaining = true;
                        break;
                    }
                    None => break,
                }
            }
        }

        if any_remaining {
            creg.needs_reclaim.store(true, Ordering::Release);
        }
    }

    fn force_reclaim_all(&self, dealloc: Option<DeallocFn>, heap: Option<&Heap>) {
        let thread_count = THREADS.count.load(Ordering::Acquire);
        for i in 0..thread_count {
            let cpin = &THREADS.slots[i].collectors[self.id];
            if let (Some(dealloc), Some(heap)) = (dealloc, heap) {
                while let Some(bag) = cpin.garbage.pop::<true>() {
                    for &ptr in bag.ptrs.as_slice() {
                        unsafe { dealloc(heap, ptr) };
                    }
                }
            }
            cpin.epoch.store(UNPINNED, Ordering::Release);
        }
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.force_reclaim_all(None, None);
        COLLECTORS.slots[self.id]
            .state
            .store(UNPINNED, Ordering::Release);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Epoch
// ═══════════════════════════════════════════════════════════════════════════

pub struct Epoch {
    epoch: CachePadded<AtomicU64>,
    collector_id: usize,
    heap: Heap,
    dealloc: DeallocFn,
}

pub struct EpochGuard<'a> {
    epoch_handle: &'a Epoch,
    pinned_epoch: u64,
    slot_ptr: *const AtomicU64,
    prev_epoch: u64,
    _not_send: std::marker::PhantomData<*const ()>,
}

impl Epoch {
    pub fn new(collector: &Collector, heap: &Heap, dealloc: DeallocFn) -> Self {
        Self {
            epoch: CachePadded::new(AtomicU64::new(1)),
            collector_id: collector.id,
            heap: heap.clone(),
            dealloc,
        }
    }

    #[inline]
    pub fn current(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    #[inline]
    pub fn pin(&self) -> EpochGuard<'_> {
        let tidx = current_thread_idx();
        let slot_ptr: *const AtomicU64 = &*THREADS.slots[tidx].collectors[self.collector_id].epoch;
        let epoch = self.epoch.load(Ordering::Acquire);
        let prev = unsafe { (*slot_ptr).load(Ordering::Relaxed) };
        unsafe { (*slot_ptr).store(epoch.min(prev), Ordering::Release) };
        EpochGuard {
            epoch_handle: self,
            pinned_epoch: epoch,
            slot_ptr,
            prev_epoch: prev,
            _not_send: std::marker::PhantomData,
        }
    }

    pub fn advance(&self) -> u64 {
        let new = self.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let creg = &COLLECTORS.slots[self.collector_id];
        if creg.needs_reclaim.load(Ordering::Relaxed) {
            self.collector_ref()
                .try_reclaim(true, &self.heap, self.dealloc);
        }
        new
    }

    /// Seal a batch of garbage. Zero copy — pushes to the thread's own SegQueue.
    pub fn seal(&self, ptrs: crate::Vec<usize>) {
        if ptrs.is_empty() {
            return;
        }
        let epoch = self.epoch.load(Ordering::Relaxed);
        let tidx = current_thread_idx();
        THREADS.slots[tidx].collectors[self.collector_id]
            .garbage
            .push(SealedGarbage { epoch, ptrs });
        let creg = &COLLECTORS.slots[self.collector_id];
        if !creg.needs_reclaim.swap(true, Ordering::AcqRel) {
            self.epoch.fetch_add(1, Ordering::Release);
        }
    }

    /// Legacy retire (calls seal).
    pub fn retire(&self, ptrs: crate::Vec<usize>) {
        self.seal(ptrs);
    }

    fn collector_ref(&self) -> std::mem::ManuallyDrop<Collector> {
        std::mem::ManuallyDrop::new(Collector {
            id: self.collector_id,
        })
    }

    #[cfg(test)]
    pub fn garbage_count(&self) -> usize {
        let thread_count = THREADS.count.load(Ordering::Acquire);
        let mut total = 0;
        for i in 0..thread_count {
            total += THREADS.slots[i].collectors[self.collector_id].garbage.len();
        }
        total
    }

    #[cfg(test)]
    pub fn readers_at(&self, target_epoch: u64) -> u32 {
        let thread_count = THREADS.count.load(Ordering::Acquire);
        let mut total = 0u32;
        for i in 0..thread_count {
            if THREADS.slots[i].collectors[self.collector_id]
                .epoch
                .load(Ordering::Relaxed)
                == target_epoch
            {
                total += 1;
            }
        }
        total
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
        unsafe { (*self.slot_ptr).store(self.prev_epoch, Ordering::Release) };
        if self.prev_epoch == UNPINNED {
            let creg = &COLLECTORS.slots[self.epoch_handle.collector_id];
            if creg.needs_reclaim.load(Ordering::Relaxed) {
                self.epoch_handle.collector_ref().try_reclaim(
                    false,
                    &self.epoch_handle.heap,
                    self.epoch_handle.dealloc,
                );
            }
        }
    }
}

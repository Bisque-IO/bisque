//! Epoch-based reclamation with thread-local pin slots and per-slot garbage.
//!
//! **Pin/unpin**: plain store to a per-thread AtomicU64. Zero contention.
//! **Retire**: push to the current thread's min-heap (per-slot Mutex, no cross-thread contention).
//! **Reclaim**: single winner via global CAS, scans all slots, pops safe garbage.
//!
//! All garbage lives in global thread slots — TreeEpoch has no per-tree garbage storage.
//! RetiredBag carries the Heap for deallocation so any tree's garbage can be freed from any slot.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use parking_lot::Mutex;

use crate::CachePadded;
use crate::Heap;

use super::node::dealloc_node;

// ═══════════════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════════════

const UNPINNED: u64 = u64::MAX;
const CLAIMED: u64 = u64::MAX - 1;
const MAX_SLOTS: usize = 1024;

// ═══════════════════════════════════════════════════════════════════════════
// RetiredBag — carries its own Heap for cross-tree deallocation
// ═══════════════════════════════════════════════════════════════════════════

struct RetiredBag {
    epoch: u64,
    #[cfg(test)]
    owner_id: u64,
    heap: Heap,
    nodes: crate::Vec<usize>,
}

impl PartialEq for RetiredBag {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch
    }
}

impl Eq for RetiredBag {}

impl PartialOrd for RetiredBag {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RetiredBag {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.epoch.cmp(&other.epoch)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Global state
// ═══════════════════════════════════════════════════════════════════════════

/// Monotonic tree ID generator.
static NEXT_TREE_ID: AtomicU64 = AtomicU64::new(1);

// ═══════════════════════════════════════════════════════════════════════════
// Global thread slot registry
// ═══════════════════════════════════════════════════════════════════════════

struct ThreadSlot {
    /// Pinned epoch (UNPINNED if not pinned, CLAIMED if reserved).
    epoch: CachePadded<AtomicU64>,
    /// Min-heap of garbage from any tree, ordered by epoch.
    /// Pushed by this thread only; reclaimer locks briefly to pop safe entries.
    garbage: Mutex<BinaryHeap<Reverse<RetiredBag>>>,
}

struct SlotRegistry {
    slots: Box<[ThreadSlot]>,
    count: AtomicUsize,
    /// Combined dirty + reclaim flag. Set to `true` by retire (Release).
    /// Reclaimer CAS true→false to claim; readers do a relaxed load first.
    /// If garbage remains after reclaim, set back to true.
    /// This avoids the two-flag race where a final `store(false)` clobbers
    /// a concurrent retire's `store(true)`.
    needs_reclaim: AtomicBool,
}

// Can't use a const array of ThreadSlot because Mutex isn't const.
// Use a lazy static instead.
static REGISTRY: std::sync::LazyLock<SlotRegistry> = std::sync::LazyLock::new(|| {
    let slots: Vec<ThreadSlot> = (0..MAX_SLOTS)
        .map(|_| ThreadSlot {
            epoch: CachePadded::new(AtomicU64::new(UNPINNED)),
            garbage: Mutex::new(BinaryHeap::new()),
        })
        .collect();
    SlotRegistry {
        slots: slots.into_boxed_slice(),
        count: AtomicUsize::new(0),
        needs_reclaim: AtomicBool::new(false),
    }
});

/// Claim the lowest available slot.
fn alloc_slot() -> (usize, *const AtomicU64) {
    let reg = &*REGISTRY;
    let count = reg.count.load(Ordering::Acquire);
    // Scan for a freed slot.
    for i in 0..count {
        if reg.slots[i]
            .epoch
            .compare_exchange_weak(UNPINNED, CLAIMED, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            let ptr: *const AtomicU64 = &*reg.slots[i].epoch;
            return (i, ptr);
        }
    }
    // Extend high-water mark.
    let idx = reg.count.fetch_add(1, Ordering::AcqRel);
    assert!(
        idx < MAX_SLOTS,
        "too many threads registered (max {MAX_SLOTS})"
    );
    reg.slots[idx].epoch.store(CLAIMED, Ordering::Release);
    let ptr: *const AtomicU64 = &*reg.slots[idx].epoch;
    (idx, ptr)
}

fn free_slot(idx: usize) {
    REGISTRY.slots[idx].epoch.store(UNPINNED, Ordering::Release);
}

struct SlotHandle {
    idx: usize,
    ptr: *const AtomicU64,
}

unsafe impl Send for SlotHandle {}

impl Drop for SlotHandle {
    fn drop(&mut self) {
        unsafe { (*self.ptr).store(UNPINNED, Ordering::Release) };
        free_slot(self.idx);
    }
}

#[inline]
fn thread_slot() -> (usize, *const AtomicU64) {
    thread_local! {
        static SLOT: SlotHandle = {
            let (idx, ptr) = alloc_slot();
            SlotHandle { idx, ptr }
        };
    }
    SLOT.with(|s| (s.idx, s.ptr))
}

/// Lock-free scan for the minimum pinned epoch.
fn min_pinned_epoch() -> u64 {
    let reg = &*REGISTRY;
    let count = reg.count.load(Ordering::Acquire);
    let mut min = UNPINNED;
    for i in 0..count {
        let val = reg.slots[i].epoch.load(Ordering::Acquire);
        if val < CLAIMED && val < min {
            min = val;
        }
    }
    min
}

// ═══════════════════════════════════════════════════════════════════════════
// TreeEpoch — lightweight per-tree handle
// ═══════════════════════════════════════════════════════════════════════════

pub struct TreeEpoch {
    epoch: CachePadded<AtomicU64>,
    tree_id: u64,
    heap: Heap,
}

pub struct EpochGuard<'a> {
    tree_epoch: &'a TreeEpoch,
    pinned_epoch: u64,
    slot_idx: usize,
    slot_ptr: *const AtomicU64,
    prev_epoch: u64,
    _not_send: std::marker::PhantomData<*const ()>,
}

impl TreeEpoch {
    pub fn new(heap: &Heap) -> Self {
        // Force registry initialization.
        let _ = &*REGISTRY;
        Self {
            epoch: CachePadded::new(AtomicU64::new(1)),
            tree_id: NEXT_TREE_ID.fetch_add(1, Ordering::Relaxed),
            heap: heap.clone(),
        }
    }

    #[inline]
    pub fn current(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Pin: one plain store to thread-local slot. Supports nesting.
    #[inline]
    pub fn pin(&self) -> EpochGuard<'_> {
        let (slot_idx, slot_ptr) = thread_slot();
        let epoch = self.epoch.load(Ordering::Acquire);
        let prev = unsafe { (*slot_ptr).load(Ordering::Relaxed) };
        unsafe { (*slot_ptr).store(epoch.min(prev), Ordering::Release) };
        EpochGuard {
            tree_epoch: self,
            pinned_epoch: epoch,
            slot_idx,
            slot_ptr,
            prev_epoch: prev,
            _not_send: std::marker::PhantomData,
        }
    }

    /// Advance epoch by 1. Triggers reclaim attempt.
    pub fn advance(&self) -> u64 {
        let new = self.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        try_reclaim();
        new
    }

    /// Retire garbage into the current thread's slot.
    /// The bag carries this tree's heap for deallocation.
    pub fn retire(&self, nodes: crate::Vec<usize>) {
        if nodes.is_empty() {
            return;
        }
        let epoch = self.epoch.load(Ordering::Relaxed);
        let (slot_idx, _) = thread_slot();
        REGISTRY.slots[slot_idx]
            .garbage
            .lock()
            .push(Reverse(RetiredBag {
                epoch,
                #[cfg(test)]
                owner_id: self.tree_id,
                heap: self.heap.clone(),
                nodes,
            }));
        REGISTRY.needs_reclaim.store(true, Ordering::Release);
    }

    /// Retire from a mutable vec (takes ownership of contents, leaves vec empty).
    pub fn retire_drain(&self, garbage: &mut crate::Vec<usize>) {
        if garbage.is_empty() {
            return;
        }
        let nodes = std::mem::replace(garbage, crate::Vec::new(&self.heap));
        self.retire(nodes);
    }

    #[cfg(test)]
    pub(super) fn garbage_count(&self) -> usize {
        let reg = &*REGISTRY;
        let count = reg.count.load(Ordering::Acquire);
        let mut total = 0;
        for i in 0..count {
            let garbage = reg.slots[i].garbage.lock();
            for Reverse(bag) in garbage.iter() {
                if bag.owner_id == self.tree_id {
                    total += bag.nodes.len();
                }
            }
        }
        total
    }

    #[cfg(test)]
    pub(super) fn readers_at(&self, target_epoch: u64) -> u32 {
        let reg = &*REGISTRY;
        let count = reg.count.load(Ordering::Acquire);
        let mut total = 0u32;
        for i in 0..count {
            if reg.slots[i].epoch.load(Ordering::Relaxed) == target_epoch {
                total += 1;
            }
        }
        total
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Global reclaim — single winner
// ═══════════════════════════════════════════════════════════════════════════

fn try_reclaim() {
    let reg = &*REGISTRY;

    // Fast path: relaxed load — stays in L1 when flag is false (read-shared cacheline).
    if !reg.needs_reclaim.load(Ordering::Relaxed) {
        return;
    }

    // Single winner: CAS true→false claims reclaim rights AND clears the flag.
    // Concurrent retires will re-set it to true — no clobber window.
    if reg
        .needs_reclaim
        .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        return; // someone else won, or flag was cleared between load and CAS
    }

    let watermark = min_pinned_epoch();
    let safe_below = if watermark == UNPINNED {
        // No readers pinned — any epoch is safe.
        u64::MAX
    } else {
        watermark
    };

    let count = reg.count.load(Ordering::Acquire);
    let mut any_remaining = false;
    for i in 0..count {
        // try_lock: skip if the slot's owner is currently pushing garbage.
        if let Some(mut garbage) = reg.slots[i].garbage.try_lock() {
            // Pop from min-heap while the oldest bag is safe to free.
            while let Some(Reverse(oldest)) = garbage.peek() {
                if oldest.epoch < safe_below {
                    let Reverse(bag) = garbage.pop().unwrap();
                    for &node in &bag.nodes {
                        unsafe { dealloc_node(&bag.heap, node) };
                    }
                } else {
                    break;
                }
            }
            if !garbage.is_empty() {
                any_remaining = true;
            }
        } else {
            // Couldn't lock — slot may have garbage.
            any_remaining = true;
        }
    }

    // Re-set flag if we couldn't drain everything.
    if any_remaining {
        reg.needs_reclaim.store(true, Ordering::Release);
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
        // Restore slot to previous value (supports nesting).
        unsafe { (*self.slot_ptr).store(self.prev_epoch, Ordering::Release) };
        // Try reclaim if this was the outermost guard.
        if self.prev_epoch == UNPINNED || self.prev_epoch == CLAIMED {
            try_reclaim();
        }
    }
}

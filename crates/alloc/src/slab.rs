//! Lock-free slabs for concurrent slot allocation.
//!
//! - [`FixedSlab`]: Single segment, fixed capacity. No growth overhead.
//! - [`SegmentedSlab`]: Grows by appending segments. Stable pointers.
//!
//! Both use power-of-2 sizing and shift/mask indexing.
//! Reads are lock-free. Alloc/free use CAS.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use parking_lot::Mutex;

const EMPTY: u64 = u64::MAX;
const CLAIMED: u64 = u64::MAX - 1;
const MAX_SEGMENTS: usize = 64;

struct Entry<T> {
    state: AtomicU64,
    data: T,
}

/// A fixed-size heap-allocated slice of entries.
/// Stable — never moved or reallocated once created.
struct Segment<T> {
    entries: Box<[Entry<T>]>,
}

impl<T: Default> Segment<T> {
    fn new(cap: usize) -> Self {
        let entries: Vec<Entry<T>> = (0..cap)
            .map(|_| Entry {
                state: AtomicU64::new(EMPTY),
                data: T::default(),
            })
            .collect();
        Segment {
            entries: entries.into_boxed_slice(),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Shared alloc/free logic
// ═══════════════════════════════════════════════════════════════════════════

/// Scan for the lowest free slot below `count`, CAS to claim it.
#[inline]
fn scan_free<T>(entries: &[Entry<T>], count: usize) -> Option<usize> {
    for id in 0..count {
        if entries[id]
            .state
            .compare_exchange_weak(EMPTY, CLAIMED, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return Some(id);
        }
    }
    None
}

// ═══════════════════════════════════════════════════════════════════════════
// FixedSlab — inline fixed array, const-generic capacity
// ═══════════════════════════════════════════════════════════════════════════

/// Fixed-capacity lock-free slab with inline entries. No heap allocation.
pub struct FixedSlab<T, const N: usize> {
    entries: [Entry<T>; N],
    count: AtomicUsize,
}

unsafe impl<T: Send + Sync, const N: usize> Send for FixedSlab<T, N> {}
unsafe impl<T: Send + Sync, const N: usize> Sync for FixedSlab<T, N> {}

impl<T: Default, const N: usize> FixedSlab<T, N> {
    pub fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| Entry {
                state: AtomicU64::new(EMPTY),
                data: T::default(),
            }),
            count: AtomicUsize::new(0),
        }
    }

    pub fn alloc(&self) -> Option<usize> {
        let count = self.count.load(Ordering::Acquire);

        if let Some(id) = scan_free(&self.entries, count) {
            return Some(id);
        }

        let id = self.count.fetch_add(1, Ordering::AcqRel);
        if id >= N {
            self.count.fetch_sub(1, Ordering::Relaxed);
            return None;
        }

        if self.entries[id]
            .state
            .compare_exchange(EMPTY, CLAIMED, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return self.alloc();
        }

        Some(id)
    }

    #[inline]
    pub fn free(&self, id: usize) {
        self.entries[id].state.store(EMPTY, Ordering::Release);
    }
}

impl<T: Default, const N: usize> Default for FixedSlab<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const N: usize> FixedSlab<T, N> {
    #[inline]
    pub fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    #[inline]
    pub const fn capacity(&self) -> usize {
        N
    }

    #[inline]
    pub fn get(&self, id: usize) -> &T {
        &self.entries[id].data
    }

    #[inline]
    pub fn state(&self, id: usize) -> &AtomicU64 {
        &self.entries[id].state
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SegmentedSlab — grows by appending segments
// ═══════════════════════════════════════════════════════════════════════════

/// Growable lock-free slab. Appends segments as needed.
/// Reads are lock-free; growth serialized via Mutex.
pub struct SegmentedSlab<T> {
    shift: u32,
    mask: usize,
    cap: usize,
    /// Segment table. Written only under `grow_lock`.
    segments: UnsafeCell<[*const Segment<T>; MAX_SEGMENTS]>,
    segment_count: AtomicUsize,
    count: AtomicUsize,
    grow_lock: Mutex<()>,
}

unsafe impl<T: Send + Sync> Send for SegmentedSlab<T> {}
unsafe impl<T: Send + Sync> Sync for SegmentedSlab<T> {}

impl<T: Default> SegmentedSlab<T> {
    /// Create with `cap` entries per segment. Must be power of 2.
    pub fn new(mut cap: usize) -> Self {
        cap = cap.next_power_of_two();
        let mut segments: [*const Segment<T>; MAX_SEGMENTS] = [std::ptr::null(); MAX_SEGMENTS];
        segments[0] = Box::into_raw(Box::new(Segment::new(cap)));
        Self {
            shift: cap.trailing_zeros(),
            mask: cap - 1,
            cap,
            segments: UnsafeCell::new(segments),
            segment_count: AtomicUsize::new(1),
            count: AtomicUsize::new(0),
            grow_lock: Mutex::new(()),
        }
    }

    pub fn alloc(&self) -> Option<usize> {
        // Only scan slots in fully-initialized segments.
        let seg_count = self.segment_count.load(Ordering::Acquire);
        let scannable = seg_count << self.shift;
        let count = self.count.load(Ordering::Acquire).min(scannable);

        for id in 0..count {
            let entry = self.entry(id);
            if entry
                .state
                .compare_exchange_weak(EMPTY, CLAIMED, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(id);
            }
        }

        let id = self.count.fetch_add(1, Ordering::AcqRel);
        let seg = id >> self.shift;

        if seg >= self.segment_count.load(Ordering::Acquire) {
            let _lock = self.grow_lock.lock();
            while seg >= self.segment_count.load(Ordering::Relaxed) {
                let next = self.segment_count.load(Ordering::Relaxed);
                if next >= MAX_SEGMENTS {
                    self.count.fetch_sub(1, Ordering::Relaxed);
                    return None;
                }
                unsafe {
                    (*self.segments.get())[next] =
                        Box::into_raw(Box::new(Segment::new(self.cap)));
                }
                self.segment_count.store(next + 1, Ordering::Release);
            }
        }

        let entry = self.entry(id);
        if entry
            .state
            .compare_exchange(EMPTY, CLAIMED, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return self.alloc();
        }

        Some(id)
    }

    #[inline]
    pub fn free(&self, id: usize) {
        self.entry(id).state.store(EMPTY, Ordering::Release);
    }
}

impl<T> SegmentedSlab<T> {
    #[inline]
    pub fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    #[inline]
    pub fn get(&self, id: usize) -> &T {
        &self.entry(id).data
    }

    #[inline]
    pub fn state(&self, id: usize) -> &AtomicU64 {
        &self.entry(id).state
    }

    #[inline]
    fn entry(&self, id: usize) -> &Entry<T> {
        let seg = id >> self.shift;
        let off = id & self.mask;
        let seg_ptr = unsafe { (*self.segments.get())[seg] };
        unsafe { &(*seg_ptr).entries[off] }
    }
}

impl<T> Drop for SegmentedSlab<T> {
    fn drop(&mut self) {
        let n = *self.segment_count.get_mut();
        let segments = self.segments.get_mut();
        for i in 0..n {
            let ptr = segments[i];
            if !ptr.is_null() {
                unsafe {
                    drop(Box::from_raw(ptr as *mut Segment<T>));
                }
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Barrier;

    // ─── FixedSlab ───────────────────────────────────────────────────

    #[test]
    fn fixed_alloc_sequential() {
        let slab = FixedSlab::<AtomicU64, 64>::new();
        for i in 0..64 {
            assert_eq!(slab.alloc(), Some(i));
        }
        assert_eq!(slab.count(), 64);
    }

    #[test]
    fn fixed_get_and_state() {
        let slab = FixedSlab::<AtomicU64, 16>::new();
        let id = slab.alloc().unwrap();
        slab.get(id).store(0xDEAD, Ordering::Relaxed);
        assert_eq!(slab.get(id).load(Ordering::Relaxed), 0xDEAD);
        assert_eq!(slab.state(id).load(Ordering::Relaxed), CLAIMED);
    }

    #[test]
    fn fixed_free_reuse_lowest() {
        let slab = FixedSlab::<AtomicU64, 64>::new();
        let a = slab.alloc().unwrap();
        let b = slab.alloc().unwrap();
        let c = slab.alloc().unwrap();
        let d = slab.alloc().unwrap();
        assert_eq!((a, b, c, d), (0, 1, 2, 3));

        slab.free(1);
        slab.free(3);
        // Should reuse 1 first (lowest free).
        assert_eq!(slab.alloc(), Some(1));
        assert_eq!(slab.alloc(), Some(3));
        // High-water mark unchanged.
        assert_eq!(slab.count(), 4);
    }

    #[test]
    fn fixed_free_all_realloc() {
        let slab = FixedSlab::<AtomicU64, 16>::new();
        for _ in 0..16 {
            slab.alloc().unwrap();
        }
        for i in 0..16 {
            slab.free(i);
        }
        // All freed — should reuse from 0.
        for i in 0..16 {
            assert_eq!(slab.alloc(), Some(i));
        }
    }

    #[test]
    fn fixed_data_persists_across_free() {
        let slab = FixedSlab::<AtomicU64, 16>::new();
        let id = slab.alloc().unwrap();
        slab.get(id).store(42, Ordering::Relaxed);
        slab.free(id);
        // Data still readable (but slot is logically free — caller's responsibility).
        assert_eq!(slab.get(id).load(Ordering::Relaxed), 42);
    }

    #[test]
    fn fixed_capacity() {
        let slab = FixedSlab::<AtomicU64, 32>::new();
        assert_eq!(slab.capacity(), 32);
    }

    #[test]
    fn fixed_count_is_high_water() {
        let slab = FixedSlab::<AtomicU64, 64>::new();
        assert_eq!(slab.count(), 0);
        slab.alloc().unwrap();
        slab.alloc().unwrap();
        assert_eq!(slab.count(), 2);
        slab.free(0);
        // Count is high-water, not active count.
        assert_eq!(slab.count(), 2);
        slab.alloc().unwrap(); // reuses 0
        assert_eq!(slab.count(), 2); // unchanged
        slab.alloc().unwrap(); // extends to 2
        assert_eq!(slab.count(), 3);
    }

    #[test]
    fn fixed_various_sizes() {
        let s1 = FixedSlab::<AtomicU64, 1>::new();
        assert_eq!(s1.capacity(), 1);
        let s2 = FixedSlab::<AtomicU64, 16>::new();
        assert_eq!(s2.capacity(), 16);
        let s3 = FixedSlab::<AtomicU64, 256>::new();
        assert_eq!(s3.capacity(), 256);
    }

    #[test]
    fn fixed_returns_none_when_full() {
        let slab = FixedSlab::<AtomicU64, 4>::new();
        for _ in 0..4 {
            assert!(slab.alloc().is_some());
        }
        assert_eq!(slab.alloc(), None);
    }

    #[test]
    fn fixed_fill_free_fill() {
        let slab = FixedSlab::<AtomicU64, 8>::new();
        // Fill completely.
        for _ in 0..8 {
            slab.alloc().unwrap();
        }
        // Free all.
        for i in 0..8 {
            slab.free(i);
        }
        // Fill again — all reused.
        let mut ids: Vec<usize> = (0..8).map(|_| slab.alloc().unwrap()).collect();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn fixed_concurrent_unique_ids() {
        let slab = Arc::new(FixedSlab::<AtomicU64, 1024>::new());

        let ids: Vec<Vec<usize>> = std::thread::scope(|s| {
            (0..8)
                .map(|_| {
                    let slab = &slab;
                    s.spawn(move || (0..100).map(|_| slab.alloc().unwrap()).collect::<Vec<_>>())
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect()
        });

        let mut all: Vec<usize> = ids.into_iter().flatten().collect();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 800);
    }

    #[test]
    fn fixed_concurrent_alloc_free_cycle() {
        let slab = Arc::new(FixedSlab::<AtomicU64, 64>::new());

        std::thread::scope(|s| {
            for t in 0..8u64 {
                let slab = &slab;
                s.spawn(move || {
                    for i in 0..1000u64 {
                        let id = slab.alloc().unwrap();
                        let val = t * 10000 + i;
                        slab.get(id).store(val, Ordering::Relaxed);
                        assert_eq!(slab.get(id).load(Ordering::Relaxed), val);
                        slab.free(id);
                    }
                });
            }
        });

        // Slots are reused — high-water mark should be low.
        assert!(slab.count() <= 8, "count = {}", slab.count());
    }

    #[test]
    fn fixed_concurrent_readers_and_allocator() {
        let slab = Arc::new(FixedSlab::<AtomicU64, 256>::new());
        let barrier = Arc::new(Barrier::new(9));

        // Pre-allocate some slots.
        for i in 0..100 {
            let id = slab.alloc().unwrap();
            slab.get(id).store(i as u64, Ordering::Relaxed);
        }

        std::thread::scope(|s| {
            // 8 reader threads verify data concurrently.
            for _ in 0..8 {
                let slab = &slab;
                let barrier = &barrier;
                s.spawn(move || {
                    barrier.wait();
                    for id in 0..100 {
                        let val = slab.get(id).load(Ordering::Relaxed);
                        assert_eq!(val, id as u64);
                    }
                });
            }
            // 1 allocator thread adds more slots concurrently.
            let slab = &slab;
            let barrier = &barrier;
            s.spawn(move || {
                barrier.wait();
                for _ in 0..50 {
                    slab.alloc().unwrap();
                }
            });
        });
    }

    // ─── SegmentedSlab ───────────────────────────────────────────────

    #[test]
    fn segmented_alloc_sequential() {
        let slab: SegmentedSlab<AtomicU64> = SegmentedSlab::new(16);
        for i in 0..100 {
            assert_eq!(slab.alloc(), Some(i));
        }
        assert_eq!(slab.count(), 100);
    }

    #[test]
    fn segmented_get_and_state() {
        let slab: SegmentedSlab<AtomicU64> = SegmentedSlab::new(16);
        let id = slab.alloc().unwrap();
        slab.get(id).store(0xBEEF, Ordering::Relaxed);
        assert_eq!(slab.get(id).load(Ordering::Relaxed), 0xBEEF);
        assert_eq!(slab.state(id).load(Ordering::Relaxed), CLAIMED);
    }

    #[test]
    fn segmented_free_reuse_lowest() {
        let slab: SegmentedSlab<AtomicU64> = SegmentedSlab::new(64);
        let a = slab.alloc().unwrap();
        let b = slab.alloc().unwrap();
        let c = slab.alloc().unwrap();
        let d = slab.alloc().unwrap();
        assert_eq!((a, b, c, d), (0, 1, 2, 3));

        slab.free(1);
        slab.free(3);
        assert_eq!(slab.alloc(), Some(1));
        assert_eq!(slab.alloc(), Some(3));
        assert_eq!(slab.count(), 4);
    }

    #[test]
    fn segmented_grows_one_segment() {
        let slab: SegmentedSlab<AtomicU64> = SegmentedSlab::new(4);
        // First segment: 0..3
        for i in 0..4 {
            assert_eq!(slab.alloc(), Some(i));
        }
        // Triggers second segment: 4..7
        for i in 4..8 {
            assert_eq!(slab.alloc(), Some(i));
        }
    }

    #[test]
    fn segmented_grows_many_segments() {
        let slab: SegmentedSlab<AtomicU64> = SegmentedSlab::new(4);
        // 10 segments × 4 = 40 entries.
        for i in 0..40 {
            assert_eq!(slab.alloc(), Some(i));
        }
        // Verify data across all segments.
        for i in 0..40 {
            slab.get(i).store(i as u64 * 7, Ordering::Relaxed);
        }
        for i in 0..40 {
            assert_eq!(slab.get(i).load(Ordering::Relaxed), i as u64 * 7);
        }
    }

    #[test]
    fn segmented_free_across_segments() {
        let slab: SegmentedSlab<AtomicU64> = SegmentedSlab::new(4);
        for _ in 0..12 {
            slab.alloc().unwrap();
        }
        // Free one from each segment.
        slab.free(1);  // segment 0
        slab.free(5);  // segment 1
        slab.free(9);  // segment 2

        // Should reuse lowest first.
        assert_eq!(slab.alloc(), Some(1));
        assert_eq!(slab.alloc(), Some(5));
        assert_eq!(slab.alloc(), Some(9));
        assert_eq!(slab.count(), 12);
    }

    #[test]
    fn segmented_count_is_high_water() {
        let slab: SegmentedSlab<AtomicU64> = SegmentedSlab::new(8);
        assert_eq!(slab.count(), 0);
        slab.alloc().unwrap();
        slab.alloc().unwrap();
        assert_eq!(slab.count(), 2);
        slab.free(0);
        assert_eq!(slab.count(), 2);
        slab.alloc().unwrap(); // reuses 0
        assert_eq!(slab.count(), 2);
    }

    #[test]
    fn segmented_fill_free_fill() {
        let slab: SegmentedSlab<AtomicU64> = SegmentedSlab::new(4);
        // Fill 3 segments.
        for _ in 0..12 {
            slab.alloc().unwrap();
        }
        // Free all.
        for i in 0..12 {
            slab.free(i);
        }
        // Refill — all reused.
        let mut ids: Vec<usize> = (0..12).map(|_| slab.alloc().unwrap()).collect();
        ids.sort();
        assert_eq!(ids, (0..12).collect::<Vec<_>>());
    }

    #[test]
    fn segmented_rounds_up_to_power_of_2() {
        let slab: SegmentedSlab<AtomicU64> = SegmentedSlab::new(13);
        // 13 rounds up to 16. Alloc 16 should fit in one segment.
        for i in 0..16 {
            assert_eq!(slab.alloc(), Some(i));
        }
        // 17th triggers second segment.
        assert_eq!(slab.alloc(), Some(16));
    }

    #[test]
    fn segmented_concurrent_unique_ids() {
        let slab = Arc::new(SegmentedSlab::<AtomicU64>::new(64));

        let ids: Vec<Vec<usize>> = std::thread::scope(|s| {
            (0..8)
                .map(|_| {
                    let slab = &slab;
                    s.spawn(move || (0..100).map(|_| slab.alloc().unwrap()).collect::<Vec<_>>())
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect()
        });

        let mut all: Vec<usize> = ids.into_iter().flatten().collect();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 800);
    }

    #[test]
    fn segmented_concurrent_alloc_free_cycle() {
        let slab = Arc::new(SegmentedSlab::<AtomicU64>::new(16));

        std::thread::scope(|s| {
            for t in 0..8u64 {
                let slab = &slab;
                s.spawn(move || {
                    for i in 0..1000u64 {
                        let id = slab.alloc().unwrap();
                        let val = t * 10000 + i;
                        slab.get(id).store(val, Ordering::Relaxed);
                        assert_eq!(slab.get(id).load(Ordering::Relaxed), val);
                        slab.free(id);
                    }
                });
            }
        });

        assert!(slab.count() <= 8, "count = {}", slab.count());
    }

    #[test]
    fn segmented_concurrent_growth() {
        // Small cap forces frequent segment allocation under contention.
        // cap=8, 8 threads × 25 = 200 slots = 25 segments (within 64 max).
        let slab = Arc::new(SegmentedSlab::<AtomicU64>::new(8));

        let ids: Vec<Vec<usize>> = std::thread::scope(|s| {
            (0..8)
                .map(|_| {
                    let slab = &slab;
                    s.spawn(move || (0..25).map(|_| slab.alloc().unwrap()).collect::<Vec<_>>())
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect()
        });

        let mut all: Vec<usize> = ids.into_iter().flatten().collect();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 200);

        // Verify all entries are accessible across segments.
        for &id in &all {
            slab.get(id).store(id as u64, Ordering::Relaxed);
        }
        for &id in &all {
            assert_eq!(slab.get(id).load(Ordering::Relaxed), id as u64);
        }
    }

    #[test]
    fn segmented_concurrent_mixed_alloc_free_grow() {
        // Threads alloc, free, and force growth simultaneously.
        // cap=32 gives 64×32=2048 max — enough headroom for high-water overshoot.
        let slab = Arc::new(SegmentedSlab::<AtomicU64>::new(32));
        let barrier = Arc::new(Barrier::new(12));

        std::thread::scope(|s| {
            // 8 threads alloc/free cycling (reuse slots).
            for _ in 0..8 {
                let slab = &slab;
                let barrier = &barrier;
                s.spawn(move || {
                    barrier.wait();
                    for _ in 0..200 {
                        let id = slab.alloc().unwrap();
                        slab.get(id).store(id as u64, Ordering::Relaxed);
                        slab.free(id);
                    }
                });
            }
            // 4 threads alloc-only (force growth).
            for _ in 0..4 {
                let slab = &slab;
                let barrier = &barrier;
                s.spawn(move || {
                    barrier.wait();
                    let _ids: Vec<usize> = (0..25).map(|_| slab.alloc().unwrap()).collect();
                });
            }
        });

        // At least 100 unique slots were held (4 threads × 25).
        assert!(slab.count() >= 100, "count = {}", slab.count());
    }

    // ─── Custom data type ────────────────────────────────────────────

    #[derive(Default)]
    struct TestData {
        x: AtomicU64,
        y: AtomicU64,
    }

    #[test]
    fn fixed_custom_data() {
        let slab = FixedSlab::<TestData, 16>::new();
        let id = slab.alloc().unwrap();
        slab.get(id).x.store(1, Ordering::Relaxed);
        slab.get(id).y.store(2, Ordering::Relaxed);
        assert_eq!(slab.get(id).x.load(Ordering::Relaxed), 1);
        assert_eq!(slab.get(id).y.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn segmented_custom_data() {
        let slab: SegmentedSlab<TestData> = SegmentedSlab::new(4);
        // Alloc across segments.
        for _ in 0..12 {
            slab.alloc().unwrap();
        }
        for i in 0..12 {
            slab.get(i).x.store(i as u64, Ordering::Relaxed);
            slab.get(i).y.store(i as u64 * 10, Ordering::Relaxed);
        }
        for i in 0..12 {
            assert_eq!(slab.get(i).x.load(Ordering::Relaxed), i as u64);
            assert_eq!(slab.get(i).y.load(Ordering::Relaxed), i as u64 * 10);
        }
    }
}

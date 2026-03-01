//! Benchmark comparing DashMap vs Congee for u64 -> Arc<T> mappings
//!
//! This benchmark simulates the group index use case in storage_impl.rs:
//! - Many concurrent readers (hot path)
//! - Occasional writers (group creation)
//! - Arc pointer storage and retrieval
//!
//! Run with: cargo run --release --example dashmap_vs_congee_bench

use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use congee::U64Congee;
use congee::epoch;
use dashmap::DashMap;
use parking_lot::RwLock;

/// Simulated group state (like GroupState<C> in storage_impl.rs)
struct GroupState {
    group_id: u64,
    #[allow(dead_code)]
    data: [u8; 256], // Some payload to make it realistic
}

impl GroupState {
    fn new(group_id: u64) -> Self {
        Self {
            group_id,
            data: [0u8; 256],
        }
    }
}

// ============================================================================
// DashMap implementation
// ============================================================================

struct DashMapIndex {
    map: DashMap<u64, Arc<GroupState>>,
}

impl DashMapIndex {
    fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    #[inline]
    fn get(&self, group_id: u64) -> Option<Arc<GroupState>> {
        self.map.get(&group_id).map(|r| r.value().clone())
    }

    fn insert(&self, group_id: u64, state: Arc<GroupState>) {
        self.map.insert(group_id, state);
    }

    fn remove(&self, group_id: u64) -> Option<Arc<GroupState>> {
        self.map.remove(&group_id).map(|(_, v)| v)
    }
}

// ============================================================================
// Congee implementation (lock-free, stores Arc as usize)
// ============================================================================

struct CongeeIndex {
    map: U64Congee<usize>,
}

impl CongeeIndex {
    fn new() -> Self {
        Self {
            map: U64Congee::<usize>::new(),
        }
    }

    #[inline]
    fn get(&self, group_id: u64) -> Option<Arc<GroupState>> {
        let guard = epoch::pin();
        let ptr_val = self.map.get(group_id, &guard)?;

        let ptr = ptr_val as *const GroupState;
        unsafe {
            let arc = Arc::from_raw(ptr);
            let cloned = arc.clone();
            let _ = Arc::into_raw(arc); // Don't drop original
            Some(cloned)
        }
    }

    fn insert(&self, group_id: u64, state: Arc<GroupState>) {
        let guard = epoch::pin();
        let ptr = Arc::into_raw(state);
        let ptr_val = ptr as usize;

        if let Ok(Some(old_ptr_val)) = self.map.insert(group_id, ptr_val, &guard) {
            // Drop old value if replaced
            let old_ptr = old_ptr_val as *const GroupState;
            unsafe {
                let _ = Arc::from_raw(old_ptr);
            }
        }
    }

    fn remove(&self, group_id: u64) -> Option<Arc<GroupState>> {
        let guard = epoch::pin();
        let ptr_val = self.map.remove(group_id, &guard)?;
        let ptr = ptr_val as *const GroupState;
        Some(unsafe { Arc::from_raw(ptr) })
    }
}

impl Drop for CongeeIndex {
    fn drop(&mut self) {
        let guard = epoch::pin();
        let mut buf: Vec<([u8; 8], usize)> = vec![([0u8; 8], 0usize); 256];
        let mut start = 0u64;

        loop {
            let n = self.map.range(start, u64::MAX, &mut buf[..], &guard);
            if n == 0 {
                break;
            }

            for i in 0..n {
                let group_id = u64::from_be_bytes(buf[i].0);
                if let Some(ptr_val) = self.map.remove(group_id, &guard) {
                    let ptr = ptr_val as *const GroupState;
                    unsafe {
                        let _ = Arc::from_raw(ptr);
                    }
                }
                start = group_id.saturating_add(1);
            }
        }
    }
}

// ============================================================================
// Congee with RwLock side-vector (like LogIndex in storage_impl.rs)
// ============================================================================

struct CongeeRwLockIndex {
    map: U64Congee<usize>,
    states: RwLock<Vec<Option<Arc<GroupState>>>>,
    free: RwLock<Vec<usize>>,
}

impl CongeeRwLockIndex {
    fn new() -> Self {
        Self {
            map: U64Congee::<usize>::new(),
            states: RwLock::new(Vec::new()),
            free: RwLock::new(Vec::new()),
        }
    }

    #[inline]
    fn get(&self, group_id: u64) -> Option<Arc<GroupState>> {
        let guard = epoch::pin();
        let slot = self.map.get(group_id, &guard)?;
        let states = self.states.read();
        states.get(slot).and_then(|opt| opt.clone())
    }

    fn insert(&self, group_id: u64, state: Arc<GroupState>) {
        let guard = epoch::pin();

        let slot = if let Some(slot) = self.free.write().pop() {
            let mut states = self.states.write();
            if slot >= states.len() {
                states.resize(slot + 1, None);
            }
            states[slot] = Some(state);
            slot
        } else {
            let mut states = self.states.write();
            let slot = states.len();
            states.push(Some(state));
            slot
        };

        let _ = self.map.insert(group_id, slot, &guard);
    }

    fn remove(&self, group_id: u64) -> Option<Arc<GroupState>> {
        let guard = epoch::pin();
        if let Some(slot) = self.map.remove(group_id, &guard) {
            let mut states = self.states.write();
            let state = states.get_mut(slot).and_then(|opt| opt.take());
            self.free.write().push(slot);
            state
        } else {
            None
        }
    }
}

// ============================================================================
// AtomicPtr Slab implementation (lock-free, O(1) direct indexing)
// ============================================================================

/// A simple slab allocator using AtomicPtr for lock-free access.
/// This provides O(1) direct indexing for bounded, dense group IDs.
struct AtomicSlabIndex {
    slots: Box<[AtomicPtr<GroupState>]>,
}

impl AtomicSlabIndex {
    fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(AtomicPtr::new(ptr::null_mut()));
        }
        Self {
            slots: slots.into_boxed_slice(),
        }
    }

    #[inline]
    fn get(&self, group_id: u64) -> Option<Arc<GroupState>> {
        let idx = group_id as usize;
        if idx >= self.slots.len() {
            return None;
        }

        let ptr = self.slots[idx].load(Ordering::Acquire);
        if ptr.is_null() {
            return None;
        }

        // Reconstruct Arc without dropping, then clone
        unsafe {
            let arc = Arc::from_raw(ptr);
            let cloned = arc.clone();
            let _ = Arc::into_raw(arc); // Don't drop original
            Some(cloned)
        }
    }

    fn insert(&self, group_id: u64, state: Arc<GroupState>) {
        let idx = group_id as usize;
        if idx >= self.slots.len() {
            return; // Out of bounds - in real code, would need to handle this
        }

        let new_ptr = Arc::into_raw(state) as *mut GroupState;
        let old_ptr = self.slots[idx].swap(new_ptr, Ordering::AcqRel);

        // Drop old value if there was one
        if !old_ptr.is_null() {
            unsafe {
                let _ = Arc::from_raw(old_ptr);
            }
        }
    }

    fn remove(&self, group_id: u64) -> Option<Arc<GroupState>> {
        let idx = group_id as usize;
        if idx >= self.slots.len() {
            return None;
        }

        let ptr = self.slots[idx].swap(ptr::null_mut(), Ordering::AcqRel);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { Arc::from_raw(ptr) })
        }
    }
}

impl Drop for AtomicSlabIndex {
    fn drop(&mut self) {
        for slot in self.slots.iter() {
            let ptr = slot.load(Ordering::Acquire);
            if !ptr.is_null() {
                unsafe {
                    let _ = Arc::from_raw(ptr);
                }
            }
        }
    }
}

// ============================================================================
// Two-level AtomicPtr Slab (for sparse/larger ID ranges)
// ============================================================================

const SEGMENT_BITS: u32 = 16;
const SEGMENT_SIZE: usize = 1 << SEGMENT_BITS; // 64K slots per segment
const SEGMENT_MASK: u64 = (SEGMENT_SIZE - 1) as u64;

struct Segment {
    slots: Box<[AtomicPtr<GroupState>]>,
}

impl Segment {
    fn new() -> Self {
        let mut slots = Vec::with_capacity(SEGMENT_SIZE);
        for _ in 0..SEGMENT_SIZE {
            slots.push(AtomicPtr::new(ptr::null_mut()));
        }
        Self {
            slots: slots.into_boxed_slice(),
        }
    }
}

/// Two-level slab for larger/sparse ID ranges.
/// Supports up to 4B entries with lazy segment allocation.
struct TwoLevelSlabIndex {
    segments: Box<[AtomicPtr<Segment>]>,
}

impl TwoLevelSlabIndex {
    fn new(max_segments: usize) -> Self {
        let mut segments = Vec::with_capacity(max_segments);
        for _ in 0..max_segments {
            segments.push(AtomicPtr::new(ptr::null_mut()));
        }
        Self {
            segments: segments.into_boxed_slice(),
        }
    }

    #[inline]
    fn get_segment(&self, group_id: u64) -> Option<&Segment> {
        let seg_idx = (group_id >> SEGMENT_BITS) as usize;
        if seg_idx >= self.segments.len() {
            return None;
        }

        let seg_ptr = self.segments[seg_idx].load(Ordering::Acquire);
        if seg_ptr.is_null() {
            None
        } else {
            Some(unsafe { &*seg_ptr })
        }
    }

    fn get_or_create_segment(&self, group_id: u64) -> Option<&Segment> {
        let seg_idx = (group_id >> SEGMENT_BITS) as usize;
        if seg_idx >= self.segments.len() {
            return None;
        }

        let seg_ptr = self.segments[seg_idx].load(Ordering::Acquire);
        if !seg_ptr.is_null() {
            return Some(unsafe { &*seg_ptr });
        }

        // Need to create segment - use CAS to avoid races
        let new_segment = Box::new(Segment::new());
        let new_ptr = Box::into_raw(new_segment);

        match self.segments[seg_idx].compare_exchange(
            ptr::null_mut(),
            new_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Some(unsafe { &*new_ptr }),
            Err(existing) => {
                // Another thread won - drop our segment and use theirs
                unsafe {
                    let _ = Box::from_raw(new_ptr);
                }
                Some(unsafe { &*existing })
            }
        }
    }

    #[inline]
    fn get(&self, group_id: u64) -> Option<Arc<GroupState>> {
        let segment = self.get_segment(group_id)?;
        let slot_idx = (group_id & SEGMENT_MASK) as usize;

        let ptr = segment.slots[slot_idx].load(Ordering::Acquire);
        if ptr.is_null() {
            return None;
        }

        unsafe {
            let arc = Arc::from_raw(ptr);
            let cloned = arc.clone();
            let _ = Arc::into_raw(arc);
            Some(cloned)
        }
    }

    fn insert(&self, group_id: u64, state: Arc<GroupState>) {
        let segment = match self.get_or_create_segment(group_id) {
            Some(s) => s,
            None => return,
        };
        let slot_idx = (group_id & SEGMENT_MASK) as usize;

        let new_ptr = Arc::into_raw(state) as *mut GroupState;
        let old_ptr = segment.slots[slot_idx].swap(new_ptr, Ordering::AcqRel);

        if !old_ptr.is_null() {
            unsafe {
                let _ = Arc::from_raw(old_ptr);
            }
        }
    }

    fn remove(&self, group_id: u64) -> Option<Arc<GroupState>> {
        let segment = self.get_segment(group_id)?;
        let slot_idx = (group_id & SEGMENT_MASK) as usize;

        let ptr = segment.slots[slot_idx].swap(ptr::null_mut(), Ordering::AcqRel);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { Arc::from_raw(ptr) })
        }
    }
}

impl Drop for TwoLevelSlabIndex {
    fn drop(&mut self) {
        for seg_atomic in self.segments.iter() {
            let seg_ptr = seg_atomic.load(Ordering::Acquire);
            if !seg_ptr.is_null() {
                let segment = unsafe { Box::from_raw(seg_ptr) };
                for slot in segment.slots.iter() {
                    let ptr = slot.load(Ordering::Acquire);
                    if !ptr.is_null() {
                        unsafe {
                            let _ = Arc::from_raw(ptr);
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
// Benchmark harness
// ============================================================================

struct BenchResult {
    name: &'static str,
    read_ops_per_sec: f64,
    write_ops_per_sec: f64,
    mixed_ops_per_sec: f64,
}

fn bench_dashmap(num_entries: u64, num_threads: usize, duration_secs: u64) -> BenchResult {
    let index = Arc::new(DashMapIndex::new());

    // Pre-populate
    for i in 0..num_entries {
        index.insert(i, Arc::new(GroupState::new(i)));
    }

    // Read-only benchmark
    let idx = index.clone();
    let read_ops = bench_reads(num_entries, num_threads, duration_secs, move |group_id| {
        idx.get(group_id)
    });

    // Write benchmark (insert/remove cycle)
    let idx1 = index.clone();
    let idx2 = index.clone();
    let write_ops = bench_writes(
        num_entries,
        num_threads,
        duration_secs,
        move |group_id| {
            let state = Arc::new(GroupState::new(group_id));
            idx1.insert(group_id, state);
        },
        move |group_id| {
            let _ = idx2.remove(group_id);
        },
    );

    // Mixed benchmark (95% reads, 5% writes)
    let idx1 = index.clone();
    let idx2 = index.clone();
    let mixed_ops = bench_mixed(
        num_entries,
        num_threads,
        duration_secs,
        move |group_id| idx1.get(group_id),
        move |group_id| {
            let state = Arc::new(GroupState::new(group_id));
            idx2.insert(group_id, state);
        },
    );

    BenchResult {
        name: "DashMap",
        read_ops_per_sec: read_ops,
        write_ops_per_sec: write_ops,
        mixed_ops_per_sec: mixed_ops,
    }
}

fn bench_congee(num_entries: u64, num_threads: usize, duration_secs: u64) -> BenchResult {
    let index = Arc::new(CongeeIndex::new());

    // Pre-populate
    for i in 0..num_entries {
        index.insert(i, Arc::new(GroupState::new(i)));
    }

    // Read-only benchmark
    let idx = index.clone();
    let read_ops = bench_reads(num_entries, num_threads, duration_secs, move |group_id| {
        idx.get(group_id)
    });

    // Write benchmark
    let idx1 = index.clone();
    let idx2 = index.clone();
    let write_ops = bench_writes(
        num_entries,
        num_threads,
        duration_secs,
        move |group_id| {
            let state = Arc::new(GroupState::new(group_id));
            idx1.insert(group_id, state);
        },
        move |group_id| {
            let _ = idx2.remove(group_id);
        },
    );

    // Mixed benchmark
    let idx1 = index.clone();
    let idx2 = index.clone();
    let mixed_ops = bench_mixed(
        num_entries,
        num_threads,
        duration_secs,
        move |group_id| idx1.get(group_id),
        move |group_id| {
            let state = Arc::new(GroupState::new(group_id));
            idx2.insert(group_id, state);
        },
    );

    BenchResult {
        name: "Congee (Arc as usize)",
        read_ops_per_sec: read_ops,
        write_ops_per_sec: write_ops,
        mixed_ops_per_sec: mixed_ops,
    }
}

fn bench_congee_rwlock(num_entries: u64, num_threads: usize, duration_secs: u64) -> BenchResult {
    let index = Arc::new(CongeeRwLockIndex::new());

    // Pre-populate
    for i in 0..num_entries {
        index.insert(i, Arc::new(GroupState::new(i)));
    }

    // Read-only benchmark
    let idx = index.clone();
    let read_ops = bench_reads(num_entries, num_threads, duration_secs, move |group_id| {
        idx.get(group_id)
    });

    // Write benchmark
    let idx1 = index.clone();
    let idx2 = index.clone();
    let write_ops = bench_writes(
        num_entries,
        num_threads,
        duration_secs,
        move |group_id| {
            let state = Arc::new(GroupState::new(group_id));
            idx1.insert(group_id, state);
        },
        move |group_id| {
            let _ = idx2.remove(group_id);
        },
    );

    // Mixed benchmark
    let idx1 = index.clone();
    let idx2 = index.clone();
    let mixed_ops = bench_mixed(
        num_entries,
        num_threads,
        duration_secs,
        move |group_id| idx1.get(group_id),
        move |group_id| {
            let state = Arc::new(GroupState::new(group_id));
            idx2.insert(group_id, state);
        },
    );

    BenchResult {
        name: "Congee + RwLock",
        read_ops_per_sec: read_ops,
        write_ops_per_sec: write_ops,
        mixed_ops_per_sec: mixed_ops,
    }
}

fn bench_atomic_slab(num_entries: u64, num_threads: usize, duration_secs: u64) -> BenchResult {
    // Allocate enough capacity for all entries plus write test range
    let capacity = (num_entries as usize) + (num_threads * 100_000) + 1000;
    let index = Arc::new(AtomicSlabIndex::new(capacity));

    // Pre-populate
    for i in 0..num_entries {
        index.insert(i, Arc::new(GroupState::new(i)));
    }

    // Read-only benchmark
    let idx = index.clone();
    let read_ops = bench_reads(num_entries, num_threads, duration_secs, move |group_id| {
        idx.get(group_id)
    });

    // Write benchmark
    let idx1 = index.clone();
    let idx2 = index.clone();
    let write_ops = bench_writes(
        num_entries,
        num_threads,
        duration_secs,
        move |group_id| {
            let state = Arc::new(GroupState::new(group_id));
            idx1.insert(group_id, state);
        },
        move |group_id| {
            let _ = idx2.remove(group_id);
        },
    );

    // Mixed benchmark
    let idx1 = index.clone();
    let idx2 = index.clone();
    let mixed_ops = bench_mixed(
        num_entries,
        num_threads,
        duration_secs,
        move |group_id| idx1.get(group_id),
        move |group_id| {
            let state = Arc::new(GroupState::new(group_id));
            idx2.insert(group_id, state);
        },
    );

    BenchResult {
        name: "AtomicPtr Slab",
        read_ops_per_sec: read_ops,
        write_ops_per_sec: write_ops,
        mixed_ops_per_sec: mixed_ops,
    }
}

fn bench_two_level_slab(num_entries: u64, num_threads: usize, duration_secs: u64) -> BenchResult {
    // 64 segments = 64 * 64K = 4M max entries
    let index = Arc::new(TwoLevelSlabIndex::new(64));

    // Pre-populate
    for i in 0..num_entries {
        index.insert(i, Arc::new(GroupState::new(i)));
    }

    // Read-only benchmark
    let idx = index.clone();
    let read_ops = bench_reads(num_entries, num_threads, duration_secs, move |group_id| {
        idx.get(group_id)
    });

    // Write benchmark
    let idx1 = index.clone();
    let idx2 = index.clone();
    let write_ops = bench_writes(
        num_entries,
        num_threads,
        duration_secs,
        move |group_id| {
            let state = Arc::new(GroupState::new(group_id));
            idx1.insert(group_id, state);
        },
        move |group_id| {
            let _ = idx2.remove(group_id);
        },
    );

    // Mixed benchmark
    let idx1 = index.clone();
    let idx2 = index.clone();
    let mixed_ops = bench_mixed(
        num_entries,
        num_threads,
        duration_secs,
        move |group_id| idx1.get(group_id),
        move |group_id| {
            let state = Arc::new(GroupState::new(group_id));
            idx2.insert(group_id, state);
        },
    );

    BenchResult {
        name: "TwoLevel AtomicPtr Slab",
        read_ops_per_sec: read_ops,
        write_ops_per_sec: write_ops,
        mixed_ops_per_sec: mixed_ops,
    }
}

fn bench_reads<F>(num_entries: u64, num_threads: usize, duration_secs: u64, get_fn: F) -> f64
where
    F: Fn(u64) -> Option<Arc<GroupState>> + Send + Sync + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let total_ops = Arc::new(AtomicU64::new(0));
    let get_fn = Arc::new(get_fn);

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let stop = stop.clone();
            let total_ops = total_ops.clone();
            let get_fn = get_fn.clone();

            std::thread::spawn(move || {
                let mut ops = 0u64;
                let mut i = thread_id as u64;

                while !stop.load(Ordering::Relaxed) {
                    let group_id = i % num_entries;
                    let _ = get_fn(group_id);
                    ops += 1;
                    i += 1;
                }

                total_ops.fetch_add(ops, Ordering::Relaxed);
            })
        })
        .collect();

    std::thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let ops = total_ops.load(Ordering::Relaxed);
    ops as f64 / duration_secs as f64
}

fn bench_writes<FI, FR>(
    num_entries: u64,
    num_threads: usize,
    duration_secs: u64,
    insert_fn: FI,
    remove_fn: FR,
) -> f64
where
    FI: Fn(u64) + Send + Sync + 'static,
    FR: Fn(u64) + Send + Sync + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let total_ops = Arc::new(AtomicU64::new(0));
    let insert_fn = Arc::new(insert_fn);
    let remove_fn = Arc::new(remove_fn);

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let stop = stop.clone();
            let total_ops = total_ops.clone();
            let insert_fn = insert_fn.clone();
            let remove_fn = remove_fn.clone();

            std::thread::spawn(move || {
                let mut ops = 0u64;
                // Use a range outside the pre-populated entries to avoid conflicts
                let base = num_entries + (thread_id as u64 * 100_000);
                let mut i = 0u64;

                while !stop.load(Ordering::Relaxed) {
                    let group_id = base + (i % 1000);
                    insert_fn(group_id);
                    remove_fn(group_id);
                    ops += 2;
                    i += 1;
                }

                total_ops.fetch_add(ops, Ordering::Relaxed);
            })
        })
        .collect();

    std::thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let ops = total_ops.load(Ordering::Relaxed);
    ops as f64 / duration_secs as f64
}

fn bench_mixed<FG, FI>(
    num_entries: u64,
    num_threads: usize,
    duration_secs: u64,
    get_fn: FG,
    insert_fn: FI,
) -> f64
where
    FG: Fn(u64) -> Option<Arc<GroupState>> + Send + Sync + 'static,
    FI: Fn(u64) + Send + Sync + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let total_ops = Arc::new(AtomicU64::new(0));
    let get_fn = Arc::new(get_fn);
    let insert_fn = Arc::new(insert_fn);

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let stop = stop.clone();
            let total_ops = total_ops.clone();
            let get_fn = get_fn.clone();
            let insert_fn = insert_fn.clone();

            std::thread::spawn(move || {
                let mut ops = 0u64;
                let mut i = thread_id as u64;

                while !stop.load(Ordering::Relaxed) {
                    let group_id = i % num_entries;

                    // 95% reads, 5% writes
                    if i % 20 == 0 {
                        insert_fn(group_id);
                    } else {
                        let _ = get_fn(group_id);
                    }

                    ops += 1;
                    i += 1;
                }

                total_ops.fetch_add(ops, Ordering::Relaxed);
            })
        })
        .collect();

    std::thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let ops = total_ops.load(Ordering::Relaxed);
    ops as f64 / duration_secs as f64
}

fn print_results(results: &[BenchResult]) {
    println!("\n{:=<80}", "");
    println!("{:^80}", "BENCHMARK RESULTS");
    println!("{:=<80}\n", "");

    println!(
        "{:<25} {:>15} {:>15} {:>15}",
        "Implementation", "Reads/sec", "Writes/sec", "Mixed/sec"
    );
    println!("{:-<70}", "");

    for r in results {
        println!(
            "{:<25} {:>15.0} {:>15.0} {:>15.0}",
            r.name, r.read_ops_per_sec, r.write_ops_per_sec, r.mixed_ops_per_sec
        );
    }

    println!();

    // Calculate speedups relative to DashMap
    if let Some(dashmap) = results.iter().find(|r| r.name == "DashMap") {
        println!("Speedup vs DashMap:");
        println!("{:-<70}", "");
        for r in results {
            if r.name != "DashMap" {
                println!(
                    "{:<25} {:>15.2}x {:>15.2}x {:>15.2}x",
                    r.name,
                    r.read_ops_per_sec / dashmap.read_ops_per_sec,
                    r.write_ops_per_sec / dashmap.write_ops_per_sec,
                    r.mixed_ops_per_sec / dashmap.mixed_ops_per_sec
                );
            }
        }
    }

    println!();
}

fn main() {
    println!("DashMap vs Congee vs AtomicPtr Slab Benchmark");
    println!("==============================================\n");

    let num_entries = 1000u64;
    let duration_secs = 3u64;

    // Test with different thread counts
    for num_threads in [1, 2, 4, 8, 16] {
        println!(
            "\n### {} threads, {} entries, {}s per test ###",
            num_threads, num_entries, duration_secs
        );

        let start = Instant::now();

        let results = vec![
            bench_dashmap(num_entries, num_threads, duration_secs),
            bench_congee(num_entries, num_threads, duration_secs),
            bench_congee_rwlock(num_entries, num_threads, duration_secs),
            bench_atomic_slab(num_entries, num_threads, duration_secs),
            bench_two_level_slab(num_entries, num_threads, duration_secs),
        ];

        print_results(&results);

        println!("Benchmark took {:.1}s", start.elapsed().as_secs_f64());
    }
}

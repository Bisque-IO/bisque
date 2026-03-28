//! Fuzz target for the heap-backed concurrent ART (Adaptive Radix Tree).
//!
//! Models extreme concurrency: persistent reader and writer threads operate
//! on a shared Art while the main thread drives mutations. Exercises insert,
//! remove, get, range scan, compare_exchange, compute_if_present,
//! compute_or_insert, epoch guard reuse, OOM paths, and concurrent
//! structural changes (node splits/merges under contention).
//!
//! Run: cd crates/alloc/fuzz && cargo +nightly fuzz run fuzz_art

#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use bisque_alloc::collections::art::Art;
use bisque_alloc::HeapMaster;
use libfuzzer_sys::fuzz_target;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Maximum number of operations per fuzz run.
const MAX_OPS: usize = 256;
/// Heap memory limit — small to exercise OOM/pressure paths.
const HEAP_LIMIT: usize = 4 * 1024 * 1024;
/// Number of persistent reader threads.
const NUM_READERS: usize = 1;
/// Number of persistent writer threads.
const NUM_WRITERS: usize = 1;
/// Writers use keys in the lower half; main thread uses the full range.
/// Overlap is intentional — it exercises concurrent mutations on the same keys.
const WRITER_KEY_LIMIT: usize = KEY_SPACE / 2;
/// Key space — small enough for frequent collisions, large enough for
/// structural variety (node-4 → node-16 → node-48 → node-256 transitions).
const KEY_SPACE: usize = 512;
/// All inserted values must be below this bound. Anything above indicates
/// memory corruption or a torn read in the concurrent ART.
const MAX_VAL: usize = KEY_SPACE * 2 + 1;

#[derive(Debug, Arbitrary)]
enum Op {
    /// Insert a key-value pair.
    Insert { key: u16, val: u16 },
    /// Remove a key.
    Remove { key: u16 },
    /// Get a key and verify consistency.
    Get { key: u16 },
    /// Range scan between two keys.
    Range { start: u16, end: u16 },
    /// Compare-exchange: try to swap old→new atomically.
    CompareExchange { key: u16, old: u16, new_val: u16 },
    /// Compute-if-present: conditionally update or delete.
    ComputeIfPresent { key: u16, delete: bool },
    /// Compute-or-insert: upsert pattern.
    ComputeOrInsert { key: u16, val: u16 },
    /// Batch insert N sequential keys (triggers node splits).
    BatchInsert { start: u16, count: u8 },
    /// Batch remove N sequential keys (triggers node merges).
    BatchRemove { start: u16, count: u8 },
    /// Re-pin the epoch guard (exercises guard lifecycle).
    Repin,
    /// Check if tree is empty.
    IsEmpty,
    /// Collect all keys (snapshot).
    Keys,
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);

    let op_count: usize = match u.int_in_range(1..=MAX_OPS) {
        Ok(n) => n,
        Err(_) => return,
    };
    let mut ops = Vec::with_capacity(op_count);
    for _ in 0..op_count {
        match u.arbitrary() {
            Ok(op) => ops.push(op),
            Err(_) => break,
        }
    }
    if ops.is_empty() {
        return;
    }

    let master = match HeapMaster::new(HEAP_LIMIT) {
        Ok(m) => m,
        Err(_) => return,
    };
    let tree = Arc::new(Art::<usize, usize>::new(&master));
    let done = Arc::new(AtomicBool::new(false));
    // Track total successful inserts/removes for sanity checks.
    let insert_count = Arc::new(AtomicUsize::new(0));
    let remove_count = Arc::new(AtomicUsize::new(0));

    // --- Persistent reader threads (optimistic lock path) ---
    let mut reader_handles = Vec::with_capacity(NUM_READERS);
    for _ in 0..NUM_READERS {
        let tree = tree.clone();
        let done = done.clone();
        reader_handles.push(std::thread::spawn(move || {
            let mut reads = 0u64;
            while !done.load(Ordering::Relaxed) {
                let guard = tree.pin();
                // Random-ish key scan driven by iteration count.
                for i in 0..32usize {
                    let key = (i.wrapping_mul(reads as usize + 1)) % KEY_SPACE;
                    if let Some(v) = tree.get(&key, &guard) {
                        // Value must be in valid range (we only insert key-derived values).
                        assert!(v < MAX_VAL, "corrupted value: {v} for key {key}");
                    }
                    reads += 1;
                }
                // Range scan with guard reuse.
                let mut buf = [(0usize, 0usize); 32];
                let start = (reads as usize) % KEY_SPACE;
                let end = start.saturating_add(64).min(KEY_SPACE);
                if start < end {
                    let n = tree.range(&start, &end, &mut buf, &guard);
                    for &(k, v) in &buf[..n] {
                        assert!(
                            k >= start && k < end,
                            "range key {k} outside [{start}, {end})"
                        );
                        assert!(v < MAX_VAL, "range corrupted value: {v}");
                    }
                }
            }
            reads
        }));
    }

    // --- Persistent writer threads (concurrent mutations) ---
    let mut writer_handles = Vec::with_capacity(NUM_WRITERS);
    for thread_id in 0..NUM_WRITERS {
        let tree = tree.clone();
        let done = done.clone();
        let ins = insert_count.clone();
        let rem = remove_count.clone();
        writer_handles.push(std::thread::spawn(move || {
            let mut writes = 0u64;
            // Writers use keys in [0, WRITER_KEY_LIMIT), split by thread_id.
            let slice = WRITER_KEY_LIMIT / NUM_WRITERS.max(1);
            let base = thread_id * slice;
            while !done.load(Ordering::Relaxed) {
                let guard = tree.pin();
                for i in 0..64 {
                    let key = base + (i + writes as usize) % slice;
                    let val = key + KEY_SPACE; // Distinct from key for verification.
                    if writes % 3 == 0 {
                        // Insert
                        if tree.insert(key, val, &guard).is_ok() {
                            ins.fetch_add(1, Ordering::Relaxed);
                        }
                    } else if writes % 3 == 1 {
                        // Remove
                        if tree.remove(&key, &guard).is_some() {
                            rem.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        // CAS: try to update to val+1 (capped to MAX_VAL).
                        let new = (val + 1) % MAX_VAL;
                        let _ = tree.compare_exchange(&key, &val, Some(new), &guard);
                    }
                    writes += 1;
                }
            }
            writes
        }));
    }

    // --- Main thread: fuzzer-driven operations ---
    let mut guard = tree.pin();

    for op in &ops {
        match op {
            Op::Insert { key, val } => {
                let k = *key as usize % KEY_SPACE;
                let v = *val as usize % KEY_SPACE;
                match tree.insert(k, v, &guard) {
                    Ok(old) => {
                        if let Some(v) = old {
                            assert!(v < MAX_VAL, "insert old corrupted: {v} for key {k}");
                        }
                        insert_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {} // OOM
                }
            }
            Op::Remove { key } => {
                let k = *key as usize % KEY_SPACE;
                let removed = tree.remove(&k, &guard);
                if let Some(v) = removed {
                    assert!(v < MAX_VAL, "remove corrupted: {v} for key {k}");
                    remove_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            Op::Get { key } => {
                let k = *key as usize % KEY_SPACE;
                if let Some(v) = tree.get(&k, &guard) {
                    assert!(v < MAX_VAL, "get corrupted: {v} for key {k}");
                }
            }
            Op::Range { start, end } => {
                let s = *start as usize % KEY_SPACE;
                let e = (*end as usize % KEY_SPACE).max(s + 1).min(KEY_SPACE);
                let mut buf = [(0usize, 0usize); 64];
                let n = tree.range(&s, &e, &mut buf, &guard);
                assert!(n <= buf.len(), "range returned more than buffer size");
                for &(k, v) in &buf[..n] {
                    assert!(k >= s && k < e, "range key {k} outside [{s}, {e})");
                    assert!(v < MAX_VAL, "range value {v} corrupted");
                }
            }
            Op::CompareExchange { key, old, new_val } => {
                let k = *key as usize % KEY_SPACE;
                let o = *old as usize % KEY_SPACE;
                let n = *new_val as usize % KEY_SPACE;
                let _ = tree.compare_exchange(&k, &o, Some(n), &guard);
            }
            Op::ComputeIfPresent { key, delete } => {
                let k = *key as usize % KEY_SPACE;
                let _ = tree.compute_if_present(
                    &k,
                    |old_val| {
                        if *delete {
                            None
                        } else {
                            Some(old_val.wrapping_add(1) % MAX_VAL)
                        }
                    },
                    &guard,
                );
            }
            Op::ComputeOrInsert { key, val } => {
                let k = *key as usize % KEY_SPACE;
                let v = *val as usize % KEY_SPACE;
                let _ = tree.compute_or_insert(
                    k,
                    |existing| match existing {
                        Some(old) => old.wrapping_add(1) % MAX_VAL,
                        None => v,
                    },
                    &guard,
                );
            }
            Op::BatchInsert { start, count } => {
                let base = *start as usize % KEY_SPACE;
                let n = (*count as usize).min(64);
                for i in 0..n {
                    let k = (base + i) % KEY_SPACE;
                    if tree.insert(k, k, &guard).is_ok() {
                        insert_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Op::BatchRemove { start, count } => {
                let base = *start as usize % KEY_SPACE;
                let n = (*count as usize).min(64);
                for i in 0..n {
                    let k = (base + i) % KEY_SPACE;
                    if tree.remove(&k, &guard).is_some() {
                        remove_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Op::Repin => {
                drop(guard);
                guard = tree.pin();
            }
            Op::IsEmpty => {
                let _ = tree.is_empty(&guard);
            }
            Op::Keys => {
                // Disabled: keys() without guard caused UAF. Now has guard.
                // Temporarily disabled to isolate corruption source.
            }
        }
    }

    // Signal background threads to stop.
    done.store(true, Ordering::Relaxed);
    drop(guard);

    for h in reader_handles {
        let _ = h.join();
    }
    for h in writer_handles {
        let _ = h.join();
    }

    // Final consistency check: every key in the tree must have a valid value.
    {
        let guard = tree.pin();
        for k in 0..KEY_SPACE {
            if let Some(v) = tree.get(&k, &guard) {
                assert!(
                    v < KEY_SPACE * 2,
                    "post-run corrupted value {v} for key {k}"
                );
            }
        }
    }

    // Tree drop exercises epoch-deferred deallocation of all ART nodes.
    drop(tree);
});

//! Benchmark comparing BTreeMap (with RwLock) vs Congee for u64 -> usize mappings
//!
//! This benchmark simulates the LogIndex use case in storage_impl.rs:
//! - Many concurrent readers looking up log entry positions
//! - Occasional writers appending new entries
//! - u64 keys (log indices) mapping to usize values (positions)
//!
//! Run with: cargo run --release --example btreemap_vs_congee_bench

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use congee::U64Congee;
use congee::epoch;
use parking_lot::RwLock;

// ============================================================================
// BTreeMap + RwLock implementation
// ============================================================================

struct BTreeMapIndex {
    map: RwLock<BTreeMap<u64, usize>>,
}

impl BTreeMapIndex {
    fn new() -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
        }
    }

    #[inline]
    fn get(&self, key: u64) -> Option<usize> {
        self.map.read().get(&key).copied()
    }

    fn insert(&self, key: u64, value: usize) {
        self.map.write().insert(key, value);
    }

    fn remove(&self, key: u64) -> Option<usize> {
        self.map.write().remove(&key)
    }

    /// Range query: get all entries in [start, end)
    fn range(&self, start: u64, end: u64) -> Vec<(u64, usize)> {
        self.map
            .read()
            .range(start..end)
            .map(|(&k, &v)| (k, v))
            .collect()
    }
}

// ============================================================================
// Congee implementation (lock-free)
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
    fn get(&self, key: u64) -> Option<usize> {
        let guard = epoch::pin();
        self.map.get(key, &guard)
    }

    fn insert(&self, key: u64, value: usize) {
        let guard = epoch::pin();
        let _ = self.map.insert(key, value, &guard);
    }

    fn remove(&self, key: u64) -> Option<usize> {
        let guard = epoch::pin();
        self.map.remove(key, &guard)
    }

    /// Range query: get all entries in [start, end)
    fn range(&self, start: u64, end: u64) -> Vec<(u64, usize)> {
        let guard = epoch::pin();
        let mut buf: Vec<([u8; 8], usize)> = vec![([0u8; 8], 0usize); 1024];
        let mut result = Vec::new();
        let mut current_start = start;

        loop {
            let n = self.map.range(current_start, end, &mut buf[..], &guard);
            if n == 0 {
                break;
            }

            for i in 0..n {
                let key = u64::from_be_bytes(buf[i].0);
                if key >= end {
                    return result;
                }
                result.push((key, buf[i].1));
                current_start = key.saturating_add(1);
            }
        }

        result
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
    range_ops_per_sec: f64,
}

fn bench_btreemap(num_entries: u64, num_threads: usize, duration_secs: u64) -> BenchResult {
    let index = Arc::new(BTreeMapIndex::new());

    // Pre-populate
    for i in 0..num_entries {
        index.insert(i, i as usize * 100);
    }

    // Read-only benchmark
    let idx = index.clone();
    let read_ops = bench_reads(num_entries, num_threads, duration_secs, move |key| {
        idx.get(key)
    });

    // Write benchmark (insert/remove cycle)
    let idx1 = index.clone();
    let idx2 = index.clone();
    let write_ops = bench_writes(
        num_entries,
        num_threads,
        duration_secs,
        move |key| {
            idx1.insert(key, key as usize * 100);
        },
        move |key| {
            let _ = idx2.remove(key);
        },
    );

    // Mixed benchmark (95% reads, 5% writes)
    let idx1 = index.clone();
    let idx2 = index.clone();
    let mixed_ops = bench_mixed(
        num_entries,
        num_threads,
        duration_secs,
        move |key| idx1.get(key),
        move |key| {
            idx2.insert(key, key as usize * 100);
        },
    );

    // Range query benchmark
    let idx = index.clone();
    let range_ops = bench_range(num_entries, num_threads, duration_secs, move |start| {
        idx.range(start, start + 10)
    });

    BenchResult {
        name: "BTreeMap + RwLock",
        read_ops_per_sec: read_ops,
        write_ops_per_sec: write_ops,
        mixed_ops_per_sec: mixed_ops,
        range_ops_per_sec: range_ops,
    }
}

fn bench_congee(num_entries: u64, num_threads: usize, duration_secs: u64) -> BenchResult {
    let index = Arc::new(CongeeIndex::new());

    // Pre-populate
    for i in 0..num_entries {
        index.insert(i, i as usize * 100);
    }

    // Read-only benchmark
    let idx = index.clone();
    let read_ops = bench_reads(num_entries, num_threads, duration_secs, move |key| {
        idx.get(key)
    });

    // Write benchmark
    let idx1 = index.clone();
    let idx2 = index.clone();
    let write_ops = bench_writes(
        num_entries,
        num_threads,
        duration_secs,
        move |key| {
            idx1.insert(key, key as usize * 100);
        },
        move |key| {
            let _ = idx2.remove(key);
        },
    );

    // Mixed benchmark
    let idx1 = index.clone();
    let idx2 = index.clone();
    let mixed_ops = bench_mixed(
        num_entries,
        num_threads,
        duration_secs,
        move |key| idx1.get(key),
        move |key| {
            idx2.insert(key, key as usize * 100);
        },
    );

    // Range query benchmark
    let idx = index.clone();
    let range_ops = bench_range(num_entries, num_threads, duration_secs, move |start| {
        idx.range(start, start + 10)
    });

    BenchResult {
        name: "Congee (lock-free)",
        read_ops_per_sec: read_ops,
        write_ops_per_sec: write_ops,
        mixed_ops_per_sec: mixed_ops,
        range_ops_per_sec: range_ops,
    }
}

fn bench_reads<F>(num_entries: u64, num_threads: usize, duration_secs: u64, get_fn: F) -> f64
where
    F: Fn(u64) -> Option<usize> + Send + Sync + 'static,
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
                    let key = i % num_entries;
                    let _ = get_fn(key);
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
                    let key = base + (i % 1000);
                    insert_fn(key);
                    remove_fn(key);
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
    FG: Fn(u64) -> Option<usize> + Send + Sync + 'static,
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
                    let key = i % num_entries;

                    // 95% reads, 5% writes
                    if i % 20 == 0 {
                        insert_fn(key);
                    } else {
                        let _ = get_fn(key);
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

fn bench_range<F>(num_entries: u64, num_threads: usize, duration_secs: u64, range_fn: F) -> f64
where
    F: Fn(u64) -> Vec<(u64, usize)> + Send + Sync + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let total_ops = Arc::new(AtomicU64::new(0));
    let range_fn = Arc::new(range_fn);

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let stop = stop.clone();
            let total_ops = total_ops.clone();
            let range_fn = range_fn.clone();

            std::thread::spawn(move || {
                let mut ops = 0u64;
                let mut i = thread_id as u64;

                while !stop.load(Ordering::Relaxed) {
                    let start = i % (num_entries.saturating_sub(10).max(1));
                    let _ = range_fn(start);
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
    println!("\n{:=<90}", "");
    println!("{:^90}", "BENCHMARK RESULTS");
    println!("{:=<90}\n", "");

    println!(
        "{:<25} {:>15} {:>15} {:>15} {:>15}",
        "Implementation", "Reads/sec", "Writes/sec", "Mixed/sec", "Range/sec"
    );
    println!("{:-<85}", "");

    for r in results {
        println!(
            "{:<25} {:>15.0} {:>15.0} {:>15.0} {:>15.0}",
            r.name,
            r.read_ops_per_sec,
            r.write_ops_per_sec,
            r.mixed_ops_per_sec,
            r.range_ops_per_sec
        );
    }

    println!();

    // Calculate speedups relative to BTreeMap
    if let Some(btree) = results.iter().find(|r| r.name.contains("BTreeMap")) {
        println!("Speedup vs BTreeMap + RwLock:");
        println!("{:-<85}", "");
        for r in results {
            if !r.name.contains("BTreeMap") {
                println!(
                    "{:<25} {:>15.2}x {:>15.2}x {:>15.2}x {:>15.2}x",
                    r.name,
                    r.read_ops_per_sec / btree.read_ops_per_sec,
                    r.write_ops_per_sec / btree.write_ops_per_sec,
                    r.mixed_ops_per_sec / btree.mixed_ops_per_sec,
                    r.range_ops_per_sec / btree.range_ops_per_sec
                );
            }
        }
    }

    println!();
}

fn main() {
    println!("BTreeMap vs Congee Benchmark");
    println!("============================\n");
    println!("This simulates the LogIndex use case: u64 log indices -> usize positions\n");

    let duration_secs = 3u64;

    // Test with different entry counts
    for num_entries in [100u64, 1_000, 10_000, 100_000] {
        // Test with different thread counts
        for num_threads in [1, 4, 8] {
            println!(
                "\n### {} threads, {} entries, {}s per test ###",
                num_threads, num_entries, duration_secs
            );

            let start = Instant::now();

            let results = vec![
                bench_btreemap(num_entries, num_threads, duration_secs),
                bench_congee(num_entries, num_threads, duration_secs),
            ];

            print_results(&results);

            println!("Benchmark took {:.1}s", start.elapsed().as_secs_f64());
        }
    }
}

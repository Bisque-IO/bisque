//! Concurrent map benchmark: ShardedArt vs DashMap vs papaya::HashMap.
//!
//! Measures throughput (Mops/s) for point lookups and insertions under
//! various reader/writer thread configurations.
//!
//! Run: cargo run --release -p bisque-alloc --example sharded-art-bench
//!
//! Scenarios:
//!   - Read-heavy:  R readers, 1 writer
//!   - Write-heavy: 1 reader, W writers
//!   - Mixed:       N/2 readers, N/2 writers
//!
//! Each scenario runs for a fixed duration. Writers update existing keys
//! (no net growth). Readers do point lookups against pre-populated data.
//!
//! ShardedArt note: `publish()` is not concurrent-safe with writers, so
//! the benchmark does NOT call publish during the timed window. Readers
//! see the pre-published snapshot; writers measure raw shard throughput.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bisque_alloc::collections::art::{Art, Collector, ShardedArt};
use bisque_alloc::{HeapMaster, MiMalloc};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const HEAP_SIZE: usize = 4 * 1024 * 1024 * 1024; // 4 GiB
const BENCH_DURATION: Duration = Duration::from_secs(1);
const PRE_POPULATE: usize = 100_000;

// ─────────────────────────────────────────────────────────────────────────────
// RNG
// ─────────────────────────────────────────────────────────────────────────────

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Result formatting
// ─────────────────────────────────────────────────────────────────────────────

struct BenchResult {
    name: &'static str,
    reader_mops: f64,
    writer_mops: f64,
    extra: String,
}

fn print_header(scenario: &str, readers: usize, writers: usize) {
    println!(
        "\n─── {scenario}: {readers}R / {writers}W  ({:.0}s) ───",
        BENCH_DURATION.as_secs_f64()
    );
    println!(
        "  {:30} {:>12} {:>12}  {}",
        "Implementation", "Read Mops/s", "Write Mops/s", "Notes"
    );
    println!("  {:-<30} {:->12} {:->12}  {:-<20}", "", "", "", "");
}

fn print_result(r: &BenchResult) {
    println!(
        "  {:30} {:>12.2} {:>12.2}  {}",
        r.name, r.reader_mops, r.writer_mops, r.extra,
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Pre-populate keys (deterministic, uniform first-byte distribution)
// ─────────────────────────────────────────────────────────────────────────────

fn generate_keys(count: usize, seed: u64) -> Vec<u64> {
    let mut rng = Rng::new(seed);
    (0..count).map(|_| rng.next_u64()).collect()
}

/// Spawn reader threads that do point lookups in a loop.
fn spawn_readers(
    done: &Arc<AtomicBool>,
    total_reads: &Arc<AtomicU64>,
    lookup: &[u64],
    num_readers: usize,
    get_fn: Arc<dyn Fn(u64) -> Option<u64> + Send + Sync>,
) -> Vec<std::thread::JoinHandle<()>> {
    let mut handles = Vec::new();
    for _ in 0..num_readers {
        let get_fn = get_fn.clone();
        let keys = lookup.to_vec();
        let done = done.clone();
        let reads = total_reads.clone();
        handles.push(std::thread::spawn(move || {
            let mut count = 0u64;
            while !done.load(Ordering::Relaxed) {
                for &k in &keys {
                    black_box(get_fn(k));
                    count += 1;
                }
            }
            reads.fetch_add(count, Ordering::Relaxed);
        }));
    }
    handles
}

// ─────────────────────────────────────────────────────────────────────────────
// ShardedArt benchmark
// ─────────────────────────────────────────────────────────────────────────────

fn bench_sharded_art(
    existing: &[u64],
    lookup: &[u64],
    num_readers: usize,
    num_writers: usize,
) -> BenchResult {
    let master = HeapMaster::new(HEAP_SIZE).unwrap();
    let heap = master.heap();
    let tree = Arc::new(ShardedArt::<usize>::new(&heap));

    // Pre-populate and publish so readers see data.
    for &k in existing {
        tree.put(k as usize, k as usize, &heap).unwrap();
    }
    tree.publish().unwrap();

    let done = Arc::new(AtomicBool::new(false));
    let total_reads = Arc::new(AtomicU64::new(0));
    let total_writes = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    // Reader threads — read from the published snapshot.
    {
        let tree = tree.clone();
        let get_fn: Arc<dyn Fn(u64) -> Option<u64> + Send + Sync> =
            Arc::new(move |k| tree.get(k as usize).map(|v| v as u64));
        handles.extend(spawn_readers(
            &done,
            &total_reads,
            lookup,
            num_readers,
            get_fn,
        ));
    }

    // Writer threads — each gets its own per-thread Heap.
    // Writers update existing keys (COW replaces the leaf, no net growth).
    let write_keys = Arc::new(existing.to_vec());
    for tid in 0..num_writers {
        let tree = tree.clone();
        let done = done.clone();
        let writes = total_writes.clone();
        let wh = master.heap();
        let wkeys = write_keys.clone();
        handles.push(std::thread::spawn(move || {
            let mut count = 0u64;
            let offset = tid * wkeys.len() / num_writers.max(1);
            while !done.load(Ordering::Relaxed) {
                for i in 0..wkeys.len() {
                    if done.load(Ordering::Relaxed) {
                        break;
                    }
                    let idx = (offset + i) % wkeys.len();
                    let key = wkeys[idx] as usize;
                    match tree.put(key, key.wrapping_add(1), &wh) {
                        Ok(_) => count += 1,
                        Err(_) => std::thread::yield_now(),
                    }
                }
            }
            writes.fetch_add(count, Ordering::Relaxed);
        }));
    }

    // NOTE: publish() is not called during the timed window because COW
    // garbage collection has a known limitation — ensure_mutable() pushes
    // shared nodes to garbage that may still be referenced by older snapshots.
    // Concurrent publish + long-lived readers can cause use-after-free.
    // TODO: fix garbage collection to use epoch watermarks.

    std::thread::sleep(BENCH_DURATION);
    done.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    BenchResult {
        name: "ShardedArt (MVCC-COW)",
        reader_mops: total_reads.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        writer_mops: total_writes.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        extra: "256-shard, lock-free reads".into(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Art benchmark (single-writer MVCC baseline)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_art(
    existing: &[u64],
    lookup: &[u64],
    num_readers: usize,
    num_writers: usize,
) -> BenchResult {
    let master = HeapMaster::new(HEAP_SIZE).unwrap();
    let heap = master.heap();
    let c = Collector::new();
    let tree = Arc::new(Art::<usize, usize>::new(&c, &heap));

    // Pre-populate and publish.
    {
        let mut w = tree.write();
        for &k in existing {
            w.insert(k as usize, k as usize).unwrap();
        }
        w.publish().unwrap();
    }

    let done = Arc::new(AtomicBool::new(false));
    let total_reads = Arc::new(AtomicU64::new(0));
    let total_writes = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    // Reader threads — use read() guard to pin once per batch.
    for _ in 0..num_readers {
        let tree = tree.clone();
        let keys = lookup.to_vec();
        let done = done.clone();
        let reads = total_reads.clone();
        handles.push(std::thread::spawn(move || {
            let mut count = 0u64;
            while !done.load(Ordering::Relaxed) {
                let guard = tree.read();
                for &k in &keys {
                    black_box(guard.get(&(k as usize)));
                    count += 1;
                }
            }
            reads.fetch_add(count, Ordering::Relaxed);
        }));
    }

    // Writer threads — OLC fast path for leaf updates, structure_lock fallback.
    let write_keys = Arc::new(existing.to_vec());
    for tid in 0..num_writers {
        let tree = tree.clone();
        let done = done.clone();
        let writes = total_writes.clone();
        let wkeys = write_keys.clone();
        handles.push(std::thread::spawn(move || {
            let mut count = 0u64;
            let offset = tid * wkeys.len() / num_writers.max(1);
            while !done.load(Ordering::Relaxed) {
                let mut w = tree.write();
                for i in 0..10000.min(wkeys.len()) {
                    if done.load(Ordering::Relaxed) {
                        break;
                    }
                    let idx = (offset + count as usize + i) % wkeys.len();
                    let key = wkeys[idx] as usize;
                    match w.insert(key, key.wrapping_add(1)) {
                        Ok(_) => count += 1,
                        Err(_) => break,
                    }
                }
                let _ = w.publish();
            }
            writes.fetch_add(count, Ordering::Relaxed);
        }));
    }

    std::thread::sleep(BENCH_DURATION);
    done.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    BenchResult {
        name: "Art (single-Mutex COW)",
        reader_mops: total_reads.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        writer_mops: total_writes.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        extra: "OLC fast-path, epoch reads".into(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DashMap benchmark
// ─────────────────────────────────────────────────────────────────────────────

fn bench_dashmap(
    existing: &[u64],
    lookup: &[u64],
    num_readers: usize,
    num_writers: usize,
) -> BenchResult {
    let map = Arc::new(dashmap::DashMap::<u64, u64>::new());
    for &k in existing {
        map.insert(k, k);
    }

    let done = Arc::new(AtomicBool::new(false));
    let total_reads = Arc::new(AtomicU64::new(0));
    let total_writes = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    {
        let map = map.clone();
        let get_fn: Arc<dyn Fn(u64) -> Option<u64> + Send + Sync> =
            Arc::new(move |k| map.get(&k).map(|v| *v));
        handles.extend(spawn_readers(
            &done,
            &total_reads,
            lookup,
            num_readers,
            get_fn,
        ));
    }

    let write_keys = Arc::new(existing.to_vec());
    for tid in 0..num_writers {
        let map = map.clone();
        let done = done.clone();
        let writes = total_writes.clone();
        let wkeys = write_keys.clone();
        handles.push(std::thread::spawn(move || {
            let mut count = 0u64;
            let offset = tid * wkeys.len() / num_writers.max(1);
            while !done.load(Ordering::Relaxed) {
                for i in 0..wkeys.len() {
                    if done.load(Ordering::Relaxed) {
                        break;
                    }
                    let idx = (offset + i) % wkeys.len();
                    map.insert(wkeys[idx], wkeys[idx].wrapping_add(1));
                    count += 1;
                }
            }
            writes.fetch_add(count, Ordering::Relaxed);
        }));
    }

    std::thread::sleep(BENCH_DURATION);
    done.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    BenchResult {
        name: "DashMap",
        reader_mops: total_reads.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        writer_mops: total_writes.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        extra: "sharded RwLock<HashMap>".into(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// papaya::HashMap benchmark
// ─────────────────────────────────────────────────────────────────────────────

fn bench_papaya(
    existing: &[u64],
    lookup: &[u64],
    num_readers: usize,
    num_writers: usize,
) -> BenchResult {
    let map = Arc::new(papaya::HashMap::<u64, u64>::new());
    {
        let guard = map.guard();
        for &k in existing {
            map.insert(k, k, &guard);
        }
    }

    let done = Arc::new(AtomicBool::new(false));
    let total_reads = Arc::new(AtomicU64::new(0));
    let total_writes = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    {
        let map = map.clone();
        let get_fn: Arc<dyn Fn(u64) -> Option<u64> + Send + Sync> =
            Arc::new(move |k| map.get(&k, &map.guard()).copied());
        handles.extend(spawn_readers(
            &done,
            &total_reads,
            lookup,
            num_readers,
            get_fn,
        ));
    }

    let write_keys = Arc::new(existing.to_vec());
    for tid in 0..num_writers {
        let map = map.clone();
        let done = done.clone();
        let writes = total_writes.clone();
        let wkeys = write_keys.clone();
        handles.push(std::thread::spawn(move || {
            let mut count = 0u64;
            let offset = tid * wkeys.len() / num_writers.max(1);
            while !done.load(Ordering::Relaxed) {
                let guard = map.guard();
                for i in 0..wkeys.len() {
                    if done.load(Ordering::Relaxed) {
                        break;
                    }
                    let idx = (offset + i) % wkeys.len();
                    map.insert(wkeys[idx], wkeys[idx].wrapping_add(1), &guard);
                    count += 1;
                }
            }
            writes.fetch_add(count, Ordering::Relaxed);
        }));
    }

    std::thread::sleep(BENCH_DURATION);
    done.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    BenchResult {
        name: "papaya::HashMap",
        reader_mops: total_reads.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        writer_mops: total_writes.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        extra: "epoch-based, lock-free".into(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

fn main() {
    println!("=== Concurrent Map Benchmark: Art vs ShardedArt vs DashMap vs papaya ===");
    println!("  Pre-populated: {PRE_POPULATE} keys (random u64)");
    println!(
        "  Duration: {:.0}s per scenario",
        BENCH_DURATION.as_secs_f64()
    );
    println!("  Workload: update existing keys (bounded tree size)");
    println!("  ShardedArt: pre-published snapshot, no publish during bench");

    let existing = generate_keys(PRE_POPULATE, 0xBEEF);
    let lookup = {
        let mut keys = existing.clone();
        let mut rng = Rng::new(0xCAFE);
        for i in (1..keys.len()).rev() {
            let j = rng.next_u64() as usize % (i + 1);
            keys.swap(i, j);
        }
        keys
    };

    // Scenarios: (readers, writers)
    let scenarios: &[(&str, &[(usize, usize)])] = &[
        ("Read-heavy", &[(1, 1), (4, 1), (8, 1), (16, 1)]),
        ("Write-heavy", &[(1, 1), (1, 4), (1, 8), (1, 16)]),
        ("Balanced", &[(2, 2), (4, 4), (8, 8)]),
    ];

    for (scenario_name, configs) in scenarios {
        println!("\n{}", "═".repeat(60));
        println!("  {scenario_name}");
        println!("{}", "═".repeat(60));

        for &(readers, writers) in *configs {
            print_header(scenario_name, readers, writers);

            let results = [
                bench_art(&existing, &lookup, readers, writers),
                bench_sharded_art(&existing, &lookup, readers, writers),
                bench_dashmap(&existing, &lookup, readers, writers),
                bench_papaya(&existing, &lookup, readers, writers),
            ];

            for r in &results {
                print_result(r);
            }
        }
    }

    println!("\n=== Done ===");
}

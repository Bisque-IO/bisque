//! Deduplication system benchmark.
//!
//! Measures throughput of dedup check-and-insert, indexed insert, prune, and
//! DedupIndex range removal across various configurations:
//!
//! - Concurrency levels (1, 2, 4, 8, 16 threads)
//! - Topic counts (1, 10, 100)
//! - Hit ratios (0%, 50%, 100% duplicate)
//! - Window sizes (small: 1K, medium: 100K, large: 1M entries)
//!
//! Usage:
//!   cargo run --release --example dedup_bench
//!   cargo run --release --example dedup_bench -- --ops 1000000 --threads 1,4,8,16
//!   cargo run --release --example dedup_bench -- --quick

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bisque_mq::topic::{DedupIndex, DedupPreFilter, DedupWindow, bytes_to_dedup_key};
use bisque_mq::types::TopicDedupConfig;

// ─── Configuration ──────────────────────────────────────────────────────────

struct BenchConfig {
    ops_per_thread: usize,
    thread_counts: Vec<usize>,
    topic_counts: Vec<usize>,
    hit_ratios: Vec<f64>,
    window_sizes: Vec<usize>,
    quick: bool,
}

impl BenchConfig {
    fn from_args() -> Self {
        let mut ops_per_thread = 500_000;
        let mut thread_counts = vec![1, 2, 4, 8, 16];
        let mut topic_counts = vec![1, 10, 100];
        let mut hit_ratios = vec![0.0, 0.5, 1.0];
        let mut window_sizes = vec![1_000, 100_000, 1_000_000];
        let mut quick = false;

        let args: Vec<String> = std::env::args().collect();
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--ops" | "-n" => {
                    i += 1;
                    ops_per_thread = args[i].parse().expect("invalid --ops");
                }
                "--threads" | "-t" => {
                    i += 1;
                    thread_counts = args[i]
                        .split(',')
                        .map(|s| s.trim().parse().expect("invalid thread count"))
                        .collect();
                }
                "--topics" => {
                    i += 1;
                    topic_counts = args[i]
                        .split(',')
                        .map(|s| s.trim().parse().expect("invalid topic count"))
                        .collect();
                }
                "--quick" | "-q" => {
                    quick = true;
                    ops_per_thread = 100_000;
                    thread_counts = vec![1, 4, 8];
                    topic_counts = vec![1, 10];
                    hit_ratios = vec![0.0, 0.5];
                    window_sizes = vec![10_000, 100_000];
                }
                "--help" | "-h" => {
                    eprintln!(
                        "Usage: dedup_bench [--ops N] [--threads 1,4,8] [--topics 1,10] [--quick]"
                    );
                    std::process::exit(0);
                }
                _ => {
                    eprintln!("unknown arg: {}", args[i]);
                    std::process::exit(1);
                }
            }
            i += 1;
        }

        Self {
            ops_per_thread,
            thread_counts,
            topic_counts,
            hit_ratios,
            window_sizes,
            quick,
        }
    }
}

// ─── Result formatting ──────────────────────────────────────────────────────

struct BenchResult {
    name: String,
    threads: usize,
    total_ops: u64,
    elapsed: Duration,
}

impl BenchResult {
    fn ops_per_sec(&self) -> f64 {
        self.total_ops as f64 / self.elapsed.as_secs_f64()
    }

    fn ns_per_op(&self) -> f64 {
        self.elapsed.as_nanos() as f64 / self.total_ops as f64
    }

    fn print(&self) {
        println!(
            "  {:50} {:>3} threads │ {:>12.2} ops/s │ {:>8.1} ns/op │ {:>10} ops in {:.3}s",
            self.name,
            self.threads,
            self.ops_per_sec(),
            self.ns_per_op(),
            self.total_ops,
            self.elapsed.as_secs_f64(),
        );
    }
}

// ─── Benchmarks ─────────────────────────────────────────────────────────────

/// Benchmark: forward-only check_and_insert with configurable hit ratio.
fn bench_forward_check(ops_per_thread: usize, threads: usize, hit_ratio: f64) -> BenchResult {
    let config = TopicDedupConfig {
        window_secs: 3600,
        max_entries: 0, // no inline GC
    };
    let dw = Arc::new(DedupWindow::new(&config));

    // Pre-populate for hit ratio.
    let hit_keys = (ops_per_thread as f64 * hit_ratio) as usize;
    for i in 0..hit_keys {
        dw.check_and_insert(i as u128, 1_000_000);
    }

    let total_ops = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let dw = Arc::clone(&dw);
            let total_ops = Arc::clone(&total_ops);
            let offset = t as u128 * ops_per_thread as u128;
            std::thread::spawn(move || {
                let mut ops = 0u64;
                for i in 0..ops_per_thread {
                    // Mix hits and misses based on ratio.
                    let key = if hit_ratio > 0.0 && (i as f64 / ops_per_thread as f64) < hit_ratio {
                        i as u128 // will hit pre-populated keys
                    } else {
                        offset + i as u128 + 1_000_000_000 // unique miss
                    };
                    let _ = dw.check_and_insert(key, 1_000_000 + i as u64);
                    ops += 1;
                }
                total_ops.fetch_add(ops, Ordering::Relaxed);
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: format!("forward check_and_insert (hit={:.0}%)", hit_ratio * 100.0),
        threads,
        total_ops: total_ops.load(Ordering::Relaxed),
        elapsed,
    }
}

/// Benchmark: indexed check_and_insert (forward + reverse index).
fn bench_indexed_check(
    ops_per_thread: usize,
    threads: usize,
    num_topics: usize,
    hit_ratio: f64,
) -> BenchResult {
    let index = Arc::new(DedupIndex::new());
    let configs: Vec<Arc<DedupWindow>> = (0..num_topics)
        .map(|_| {
            let config = TopicDedupConfig {
                window_secs: 3600,
                max_entries: 0,
            };
            Arc::new(DedupWindow::new(&config))
        })
        .collect();

    // Pre-populate for hit ratio.
    let hit_keys = (ops_per_thread as f64 * hit_ratio) as usize;
    for i in 0..hit_keys {
        let topic_idx = i % num_topics;
        configs[topic_idx].check_and_insert_indexed(i as u128, 1_000_000, topic_idx as u64, &index);
    }

    let total_ops = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let index = Arc::clone(&index);
            let configs = configs.clone();
            let total_ops = Arc::clone(&total_ops);
            let offset = t as u128 * ops_per_thread as u128;
            std::thread::spawn(move || {
                let mut ops = 0u64;
                for i in 0..ops_per_thread {
                    let topic_idx = i % num_topics;
                    let key = if hit_ratio > 0.0 && (i as f64 / ops_per_thread as f64) < hit_ratio {
                        i as u128
                    } else {
                        offset + i as u128 + 1_000_000_000
                    };
                    let _ = configs[topic_idx].check_and_insert_indexed(
                        key,
                        1_000_000 + i as u64,
                        topic_idx as u64,
                        &index,
                    );
                    ops += 1;
                }
                total_ops.fetch_add(ops, Ordering::Relaxed);
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: format!(
            "indexed check ({}T, hit={:.0}%)",
            num_topics,
            hit_ratio * 100.0
        ),
        threads,
        total_ops: total_ops.load(Ordering::Relaxed),
        elapsed,
    }
}

/// Benchmark: DedupIndex prune_before with various pre-populated sizes.
fn bench_prune(window_size: usize, pct_expired: f64) -> BenchResult {
    let index = DedupIndex::new();
    let expired_count = (window_size as f64 * pct_expired) as u64;
    let cutoff_ts = 1_000_000u64;

    // Insert entries: expired ones at low timestamps, live ones at high.
    for i in 0..window_size as u64 {
        let ts = if i < expired_count {
            i // below cutoff
        } else {
            cutoff_ts + i // above cutoff
        };
        let topic_id = (i % 10) as u64;
        index.insert(ts, topic_id, i as u128);
    }

    let start = Instant::now();
    let expired = index.prune_before(cutoff_ts);
    let elapsed = start.elapsed();

    BenchResult {
        name: format!(
            "prune {:.0}K entries ({:.0}% expired)",
            window_size as f64 / 1000.0,
            pct_expired * 100.0
        ),
        threads: 1,
        total_ops: expired.len() as u64,
        elapsed,
    }
}

/// Benchmark: concurrent insert + prune contention.
fn bench_concurrent_insert_prune(
    ops_per_thread: usize,
    insert_threads: usize,
    prune_threads: usize,
    num_topics: usize,
) -> BenchResult {
    let index = Arc::new(DedupIndex::new());
    let configs: Vec<Arc<DedupWindow>> = (0..num_topics)
        .map(|_| {
            let config = TopicDedupConfig {
                window_secs: 3600,
                max_entries: 0,
            };
            Arc::new(DedupWindow::new(&config))
        })
        .collect();

    let total_ops = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = Vec::new();

    // Insert threads.
    for t in 0..insert_threads {
        let index = Arc::clone(&index);
        let configs = configs.clone();
        let total_ops = Arc::clone(&total_ops);
        let offset = t as u128 * ops_per_thread as u128;
        handles.push(std::thread::spawn(move || {
            let mut ops = 0u64;
            for i in 0..ops_per_thread {
                let topic_idx = i % num_topics;
                let key = offset + i as u128;
                let ts = 1_000 + (i as u64 % 10_000);
                configs[topic_idx].check_and_insert_indexed(key, ts, topic_idx as u64, &index);
                ops += 1;
            }
            total_ops.fetch_add(ops, Ordering::Relaxed);
        }));
    }

    // Prune threads.
    for _ in 0..prune_threads {
        let index = Arc::clone(&index);
        let total_ops = Arc::clone(&total_ops);
        handles.push(std::thread::spawn(move || {
            let mut ops = 0u64;
            for cutoff in (0..10_000).step_by(10) {
                let expired = index.prune_before(cutoff);
                ops += expired.len() as u64;
            }
            total_ops.fetch_add(ops, Ordering::Relaxed);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: format!(
            "concurrent insert+prune ({} ins + {} prune, {}T)",
            insert_threads, prune_threads, num_topics
        ),
        threads: insert_threads + prune_threads,
        total_ops: total_ops.load(Ordering::Relaxed),
        elapsed,
    }
}

/// Benchmark: bytes_to_dedup_key conversion throughput.
fn bench_key_conversion(ops: usize) -> BenchResult {
    let keys: Vec<Vec<u8>> = (0..1000)
        .map(|i| format!("msg-id-{:08}", i).into_bytes())
        .collect();

    let start = Instant::now();
    let mut sum = 0u128;
    for i in 0..ops {
        sum = sum.wrapping_add(bytes_to_dedup_key(&keys[i % keys.len()]));
    }
    let elapsed = start.elapsed();
    // Prevent dead-code elimination.
    std::hint::black_box(sum);

    BenchResult {
        name: "bytes_to_dedup_key conversion".to_string(),
        threads: 1,
        total_ops: ops as u64,
        elapsed,
    }
}

/// Benchmark: inline GC trigger throughput.
fn bench_inline_gc(ops_per_thread: usize, threads: usize) -> BenchResult {
    let config = TopicDedupConfig {
        window_secs: 5,
        max_entries: 1000,
    };
    let dw = Arc::new(DedupWindow::new(&config));

    let total_ops = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let dw = Arc::clone(&dw);
            let total_ops = Arc::clone(&total_ops);
            std::thread::spawn(move || {
                let mut ops = 0u64;
                let base = t as u128 * ops_per_thread as u128;
                for i in 0..ops_per_thread {
                    // Use increasing timestamps to trigger inline GC repeatedly.
                    let ts = (i as u64) * 100;
                    let key = base + i as u128;
                    let _ = dw.check_and_insert(key, ts);
                    ops += 1;
                }
                total_ops.fetch_add(ops, Ordering::Relaxed);
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: "inline GC (max_entries=1000, window=5s)".to_string(),
        threads,
        total_ops: total_ops.load(Ordering::Relaxed),
        elapsed,
    }
}

/// Benchmark: DedupPreFilter single-threaded throughput with configurable hit ratio.
fn bench_prefilter(ops: usize, hit_ratio: f64) -> BenchResult {
    let config = TopicDedupConfig {
        window_secs: 3600,
        max_entries: 100_000,
    };
    let mut pf = DedupPreFilter::from_config(&config);

    // Pre-populate for hit ratio.
    let hit_keys = (ops as f64 * hit_ratio) as usize;
    for i in 0..hit_keys {
        pf.check_and_insert(i as u128, 1_000_000);
    }

    let start = Instant::now();
    let mut dups = 0u64;
    for i in 0..ops {
        let key = if hit_ratio > 0.0 && (i as f64 / ops as f64) < hit_ratio {
            i as u128 // will hit pre-populated keys
        } else {
            i as u128 + 1_000_000_000 // unique miss
        };
        if pf.check_and_insert(key, 1_000_000 + i as u64) {
            dups += 1;
        }
    }
    let elapsed = start.elapsed();
    std::hint::black_box(dups);

    BenchResult {
        name: format!("prefilter check_and_insert (hit={:.0}%)", hit_ratio * 100.0),
        threads: 1,
        total_ops: ops as u64,
        elapsed,
    }
}

/// Benchmark: two-tier pattern (DedupPreFilter → DedupWindow).
fn bench_two_tier(ops: usize, hit_ratio: f64) -> BenchResult {
    let pf_config = TopicDedupConfig {
        window_secs: 3600,
        max_entries: 100_000,
    };
    let dw_config = TopicDedupConfig {
        window_secs: 3600,
        max_entries: 0, // no inline GC — measure lookup pattern only
    };
    let mut pf = DedupPreFilter::from_config(&pf_config);
    let dw = DedupWindow::new(&dw_config);

    // Pre-populate both tiers for hit ratio.
    let hit_keys = (ops as f64 * hit_ratio) as usize;
    for i in 0..hit_keys {
        pf.check_and_insert(i as u128, 1_000_000);
        dw.check_and_insert(i as u128, 1_000_000);
    }

    let start = Instant::now();
    let mut prefilter_hits = 0u64;
    let mut shared_hits = 0u64;
    for i in 0..ops {
        let key = if hit_ratio > 0.0 && (i as f64 / ops as f64) < hit_ratio {
            i as u128
        } else {
            i as u128 + 1_000_000_000
        };
        let ts = 1_000_000 + i as u64;
        if pf.check_and_insert(key, ts) {
            prefilter_hits += 1;
        } else if dw.check_and_insert(key, ts) {
            shared_hits += 1;
        }
    }
    let elapsed = start.elapsed();
    std::hint::black_box((prefilter_hits, shared_hits));

    BenchResult {
        name: format!("two-tier prefilter→shared (hit={:.0}%)", hit_ratio * 100.0),
        threads: 1,
        total_ops: ops as u64,
        elapsed,
    }
}

/// Benchmark: DedupPreFilter with window expiry under load.
fn bench_prefilter_expiry(ops: usize) -> BenchResult {
    let config = TopicDedupConfig {
        window_secs: 1, // 1-second window forces frequent eviction
        max_entries: 100_000,
    };
    let mut pf = DedupPreFilter::from_config(&config);

    let start = Instant::now();
    for i in 0..ops {
        // Timestamps advance rapidly to trigger expiry.
        let ts = i as u64; // 1ms per op, window=1000ms → constant eviction
        pf.check_and_insert(i as u128, ts);
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: "prefilter with window expiry (1s window)".to_string(),
        threads: 1,
        total_ops: ops as u64,
        elapsed,
    }
}

/// Benchmark: forward-map remove_keys batch throughput.
fn bench_remove_keys(window_size: usize, batch_size: usize) -> BenchResult {
    let config = TopicDedupConfig {
        window_secs: 3600,
        max_entries: 0,
    };
    let dw = DedupWindow::new(&config);

    // Populate.
    for i in 0..window_size {
        dw.check_and_insert(i as u128, 1_000_000);
    }

    // Build key batches.
    let keys: Vec<u128> = (0..window_size as u128).collect();
    let batches: Vec<&[u128]> = keys.chunks(batch_size).collect();

    let start = Instant::now();
    let mut total_removed = 0usize;
    for batch in &batches {
        total_removed += dw.remove_keys(batch);
    }
    let elapsed = start.elapsed();

    BenchResult {
        name: format!(
            "remove_keys ({:.0}K entries, batch={})",
            window_size as f64 / 1000.0,
            batch_size
        ),
        threads: 1,
        total_ops: total_removed as u64,
        elapsed,
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────

fn main() {
    let cfg = BenchConfig::from_args();

    println!(
        "═══════════════════════════════════════════════════════════════════════════════════════════════════════════════"
    );
    println!("  bisque-mq dedup benchmark");
    println!("  ops/thread: {}  quick: {}", cfg.ops_per_thread, cfg.quick);
    println!(
        "═══════════════════════════════════════════════════════════════════════════════════════════════════════════════"
    );

    // 1. Key conversion baseline.
    println!(
        "\n─── Key Conversion ────────────────────────────────────────────────────────────────────────────────────────────"
    );
    bench_key_conversion(cfg.ops_per_thread * 2).print();

    // 2. Forward-only check_and_insert.
    println!(
        "\n─── Forward-Only check_and_insert ─────────────────────────────────────────────────────────────────────────────"
    );
    for &threads in &cfg.thread_counts {
        for &hit_ratio in &cfg.hit_ratios {
            bench_forward_check(cfg.ops_per_thread, threads, hit_ratio).print();
        }
    }

    // 3. Indexed check (forward + reverse).
    println!(
        "\n─── Indexed check_and_insert (forward + DedupIndex) ────────────────────────────────────────────────────────────"
    );
    for &threads in &cfg.thread_counts {
        for &num_topics in &cfg.topic_counts {
            for &hit_ratio in &cfg.hit_ratios {
                bench_indexed_check(cfg.ops_per_thread, threads, num_topics, hit_ratio).print();
            }
        }
    }

    // 4. Prune throughput.
    println!(
        "\n─── DedupIndex prune_before ────────────────────────────────────────────────────────────────────────────────────"
    );
    for &window_size in &cfg.window_sizes {
        for &pct in &[0.1, 0.5, 1.0] {
            bench_prune(window_size, pct).print();
        }
    }

    // 5. Concurrent insert + prune.
    println!(
        "\n─── Concurrent Insert + Prune ──────────────────────────────────────────────────────────────────────────────────"
    );
    for &num_topics in &cfg.topic_counts {
        for &(ins, prn) in &[(4, 1), (8, 2), (4, 4)] {
            if ins + prn > 16 && cfg.quick {
                continue;
            }
            bench_concurrent_insert_prune(cfg.ops_per_thread / 2, ins, prn, num_topics).print();
        }
    }

    // 6. Inline GC.
    println!(
        "\n─── Inline GC (max_entries threshold) ─────────────────────────────────────────────────────────────────────────"
    );
    for &threads in &cfg.thread_counts {
        bench_inline_gc(cfg.ops_per_thread, threads).print();
    }

    // 7. Batch remove_keys.
    println!(
        "\n─── Forward-Map remove_keys (batch) ───────────────────────────────────────────────────────────────────────────"
    );
    for &window_size in &cfg.window_sizes {
        for &batch_size in &[100, 1000, 10_000] {
            if batch_size > window_size {
                continue;
            }
            bench_remove_keys(window_size, batch_size).print();
        }
    }

    // 8. DedupPreFilter (single-threaded).
    println!(
        "\n─── DedupPreFilter (single-threaded) ──────────────────────────────────────────────────────────────────────────"
    );
    for &hit_ratio in &cfg.hit_ratios {
        bench_prefilter(cfg.ops_per_thread * 2, hit_ratio).print();
    }
    bench_prefilter_expiry(cfg.ops_per_thread * 2).print();

    // 9. Two-tier: DedupPreFilter → DedupWindow.
    println!(
        "\n─── Two-Tier: PreFilter → DedupWindow ─────────────────────────────────────────────────────────────────────────"
    );
    for &hit_ratio in &cfg.hit_ratios {
        bench_two_tier(cfg.ops_per_thread * 2, hit_ratio).print();
    }

    println!(
        "\n═══════════════════════════════════════════════════════════════════════════════════════════════════════════════"
    );
    println!("  done");
    println!(
        "═══════════════════════════════════════════════════════════════════════════════════════════════════════════════"
    );
}

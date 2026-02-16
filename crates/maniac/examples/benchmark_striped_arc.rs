//! Comprehensive benchmark suite for StripedArc
//!
//! This benchmark compares StripedArc against std::Arc across various
//! multi-threaded scenarios to demonstrate the performance benefits of
//! striped reference counting under high contention.

/*

16 core CPU
==============================================================================
Benchmark: Clone/Drop
==============================================================================

┌─────────┬────────────┬───────────┬─────────┬────────────┐
│ Threads ┆ StripedArc ┆ std::Arc  ┆ Speedup ┆ Winner     │
│         ┆ (ops/sec)  ┆ (ops/sec) ┆         ┆            │
╞═════════╪════════════╪═══════════╪═════════╪════════════╡
│ 1       ┆ 211.88M    ┆ 195.72M   ┆ 1.08x   ┆ StripedArc │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2       ┆ 427.87M    ┆ 87.63M    ┆ 4.88x   ┆ StripedArc │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 4       ┆ 820.53M    ┆ 87.52M    ┆ 9.38x   ┆ StripedArc │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 8       ┆ 1.52B      ┆ 66.89M    ┆ 22.77x  ┆ StripedArc │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 10      ┆ 1.80B      ┆ 65.79M    ┆ 27.42x  ┆ StripedArc │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 12      ┆ 1.69B      ┆ 50.03M    ┆ 33.78x  ┆ StripedArc │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 14      ┆ 2.32B      ┆ 41.20M    ┆ 56.27x  ┆ StripedArc │
└─────────┴────────────┴───────────┴─────────┴────────────┘
*/

use comfy_table::{
    Attribute, CellAlignment, Color, ContentArrangement, Table,
    presets::{ASCII_BORDERS_ONLY, UTF8_FULL},
};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
fn set_cpu_affinity(cpu: usize) -> bool {
    use std::mem;

    unsafe {
        let mut cpuset: libc::cpu_set_t = mem::zeroed();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(cpu, &mut cpuset);
        libc::pthread_setaffinity_np(libc::pthread_self(), mem::size_of_val(&cpuset), &cpuset) == 0
    }
}

#[cfg(not(target_os = "linux"))]
fn set_cpu_affinity(_cpu: usize) -> bool {
    true
}

// Benchmark result structures
struct BenchmarkResult {
    benchmark_name: String,
    implementation: String,
    threads: usize,
    ops_per_sec: f64,
    duration_ms: f64,
    total_ops: f64,
}
fn humanize_number(num: f64) -> String {
    let units = ["", "K", "M", "B", "T"];
    let mut value = num;
    let mut unit_index = 0;

    while value >= 1000.0 && unit_index < units.len() - 1 {
        value /= 1000.0;
        unit_index += 1;
    }

    format!("{:.2}{}", value, units[unit_index])
}
fn humanize_ops_per_sec(ops_per_sec: f64) -> String {
    humanize_number(ops_per_sec) + " ops/sec"
}

/// Global result storage
static RESULTS: Mutex<Vec<BenchmarkResult>> = Mutex::new(Vec::new());

/// Record a benchmark result
fn record_result(result: BenchmarkResult) {
    RESULTS.lock().unwrap().push(result);
}

// Re-export StripedArc for benchmarking
use maniac::ptr::StripedArc;

// Test data structure
#[derive(Debug, Default)]
struct TestData {
    value: [u64; 16],
}

#[inline]
fn do_work(arc: &TestData) {
    // Simulate reading from the data
    // let mut sum: u64 = 0;
    // for v in arc.value.iter() {
    //     sum = sum.wrapping_add(*v);
    // }
    // Prevent compiler from optimizing away
    // std::hint::black_box(sum);
}

// ============================================================================
// BENCHMARK 1: Clone Drop Throughput
// ============================================================================
fn benchmark_clone_drop_striped_arc(num_threads: usize, iterations_per_thread: usize) {
    println!("\n=== StripedArc Clone/Drop ===");
    println!(
        "Threads: {}, Iterations per thread: {}",
        num_threads, iterations_per_thread
    );
    println!(
        "Total operations: {}",
        humanize_number((num_threads * iterations_per_thread) as f64)
    );

    let arc = StripedArc::new(TestData::default());

    // Warmup
    for _ in 0..1000 {
        let _ = arc.clone();
    }

    let mut handles = vec![];
    let start = Instant::now();

    for _i in 0..num_threads {
        let arc_clone = arc.clone();
        handles.push(thread::spawn(move || {
            set_cpu_affinity(_i as usize);
            for _ in 0..iterations_per_thread {
                let _arc = arc_clone.clone();
                // Force drop to happen
                drop(_arc);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    let total_ops = num_threads * iterations_per_thread * 2; // clone + drop
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();

    println!("Duration: {:?}", duration);
    println!("Throughput: {}", humanize_number(ops_per_sec) + " ops/sec");
    println!(
        "Per thread: {}",
        humanize_number((iterations_per_thread * 2) as f64 / duration.as_secs_f64())
            + " ops/thread/sec"
    );

    record_result(BenchmarkResult {
        benchmark_name: "Clone/Drop".to_string(),
        implementation: "StripedArc".to_string(),
        threads: num_threads,
        ops_per_sec,
        duration_ms: duration.as_secs_f64() * 1000.0,
        total_ops: total_ops as f64,
    });
}

fn benchmark_clone_drop_std_arc(num_threads: usize, iterations_per_thread: usize) {
    println!("\n=== std::Arc Clone/Drop ===");
    println!(
        "Threads: {}, Iterations per thread: {}",
        num_threads, iterations_per_thread
    );
    println!(
        "Total operations: {}",
        humanize_number((num_threads * iterations_per_thread) as f64)
    );

    let arc = Arc::new(TestData::default());

    // Warmup
    for _ in 0..1000 {
        let _ = arc.clone();
    }

    let mut handles = vec![];
    let start = Instant::now();

    for _i in 0..num_threads {
        let arc_clone = arc.clone();
        handles.push(thread::spawn(move || {
            set_cpu_affinity(_i as usize);
            for _ in 0..iterations_per_thread {
                let _arc = arc_clone.clone();
                // Force drop to happen
                drop(_arc);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    let total_ops = num_threads * iterations_per_thread * 2; // clone + drop
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();

    println!("Duration: {:?}", duration);
    println!("Throughput: {}", humanize_number(ops_per_sec) + " ops/sec");
    println!(
        "Per thread: {}",
        humanize_number((iterations_per_thread * 2) as f64 / duration.as_secs_f64())
            + " ops/thread/sec"
    );

    record_result(BenchmarkResult {
        benchmark_name: "Clone/Drop".to_string(),
        implementation: "std::Arc".to_string(),
        threads: num_threads,
        ops_per_sec,
        duration_ms: duration.as_secs_f64() * 1000.0,
        total_ops: total_ops as f64,
    });
}

// ============================================================================
// BENCHMARK 2: Read-Heavy Workload
// ============================================================================
fn benchmark_read_heavy_striped_arc(num_readers: usize, num_writers: usize, duration_secs: u64) {
    println!("\n=== StripedArc Read-Heavy ===");
    println!("Readers: {}, Writers: {}", num_readers, num_writers);
    println!("Duration: {}s", duration_secs);

    let arc = StripedArc::new(TestData::default());
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (tx, rx) = std::sync::mpsc::channel::<u64>();
    let mut handles = vec![];

    // Warmup
    for _ in 0..1000 {
        do_work(&arc.clone());
    }

    // Reader threads
    for _i in 0..num_readers {
        let arc_clone = arc.clone();
        let stop_clone = stop.clone();
        let tx_clone = tx.clone();
        handles.push(thread::spawn(move || {
            set_cpu_affinity(_i as usize);
            let mut local_ops = 0u64;
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                do_work(&arc_clone);
                // Sporadic clone to test refcount contention
                if local_ops % 100 == 0 {
                    let _clone = arc_clone.clone();
                    drop(_clone);
                }
                local_ops += 1;
            }
            tx_clone.send(local_ops).unwrap();
        }));
    }

    // Writer threads (cause more refcount operations)
    for _i in 0..num_writers {
        let arc_clone = arc.clone();
        let stop_clone = stop.clone();
        let tx_clone = tx.clone();
        handles.push(thread::spawn(move || {
            set_cpu_affinity(_i as usize);
            let mut local_ops = 0u64;
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                // Frequent cloning
                let clone1 = arc_clone.clone();
                let clone2 = arc_clone.clone();
                do_work(&clone1);
                drop(clone1);
                do_work(&clone2);
                drop(clone2);
                local_ops += 2;
            }
            tx_clone.send(local_ops).unwrap();
        }));
    }

    // Let it run
    thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    // Drop our sender so rx.recv() will finish when all threads are done
    drop(tx);

    let mut total_ops = 0u64;
    for (i, ops) in rx.iter().enumerate() {
        total_ops += ops;
        if i < num_readers {
            println!("Reader {}: {} ops", i, humanize_number(ops as f64));
        } else {
            println!(
                "Writer {}: {} ops",
                i - num_readers,
                humanize_number(ops as f64)
            );
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("Total operations: {}", humanize_number(total_ops as f64));
    let ops_per_sec = total_ops as f64 / duration_secs as f64;
    println!("Ops/sec: {}", humanize_number(ops_per_sec) + " ops/sec");

    record_result(BenchmarkResult {
        benchmark_name: "Read-Heavy".to_string(),
        implementation: "StripedArc".to_string(),
        threads: num_readers + num_writers,
        ops_per_sec,
        duration_ms: (duration_secs * 1000) as f64,
        total_ops: total_ops as f64,
    });
}

fn benchmark_read_heavy_std_arc(num_readers: usize, num_writers: usize, duration_secs: u64) {
    println!("\n=== std::Arc Read-Heavy ===");
    println!("Readers: {}, Writers: {}", num_readers, num_writers);
    println!("Duration: {}s", duration_secs);

    let arc = Arc::new(TestData::default());
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (tx, rx) = std::sync::mpsc::channel::<u64>();
    let mut handles = vec![];

    // Warmup
    for _ in 0..1000 {
        do_work(&arc.clone());
    }

    // Reader threads
    for _i in 0..num_readers {
        let arc_clone = arc.clone();
        let stop_clone = stop.clone();
        let tx_clone = tx.clone();
        handles.push(thread::spawn(move || {
            set_cpu_affinity(_i as usize);
            let mut local_ops = 0u64;
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                do_work(&arc_clone);
                // Sporadic clone to test refcount contention
                if local_ops % 100 == 0 {
                    let _clone = arc_clone.clone();
                    drop(_clone);
                }
                local_ops += 1;
            }
            tx_clone.send(local_ops).unwrap();
        }));
    }

    // Writer threads (cause more refcount operations)
    for _i in 0..num_writers {
        let arc_clone = arc.clone();
        let stop_clone = stop.clone();
        let tx_clone = tx.clone();
        handles.push(thread::spawn(move || {
            set_cpu_affinity(_i as usize);
            let mut local_ops = 0u64;
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                // Frequent cloning
                let clone1 = arc_clone.clone();
                let clone2 = arc_clone.clone();
                do_work(&clone1);
                drop(clone1);
                do_work(&clone2);
                drop(clone2);
                local_ops += 2;
            }
            tx_clone.send(local_ops).unwrap();
        }));
    }

    // Let it run
    thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    // Drop our sender so rx.recv() will finish when all threads are done
    drop(tx);

    let mut total_ops = 0u64;
    for (i, ops) in rx.iter().enumerate() {
        total_ops += ops;
        if i < num_readers {
            println!("Reader {}: {} ops", i, humanize_number(ops as f64));
        } else {
            println!(
                "Writer {}: {} ops",
                i - num_readers,
                humanize_number(ops as f64)
            );
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("Total operations: {}", humanize_number(total_ops as f64));
    let ops_per_sec = total_ops as f64 / duration_secs as f64;
    println!("Ops/sec: {}", humanize_number(ops_per_sec) + " ops/sec");

    record_result(BenchmarkResult {
        benchmark_name: "Read-Heavy".to_string(),
        implementation: "std::Arc".to_string(),
        threads: num_readers + num_writers,
        ops_per_sec,
        duration_ms: (duration_secs * 1000) as f64,
        total_ops: total_ops as f64,
    });
}

// ============================================================================
// BENCHMARK 3: Burst Contention
// ============================================================================
fn benchmark_burst_contention_striped_arc(
    num_threads: usize,
    num_bursts: usize,
    burst_size: usize,
) {
    println!("\n=== StripedArc Burst Contention ===");
    println!(
        "Threads: {}, Bursts: {}, Burst size: {}",
        num_threads, num_bursts, burst_size
    );

    let arc = StripedArc::new(TestData::default());
    let barrier = Arc::new(std::sync::Barrier::new(num_threads));
    let mut handles = vec![];

    for _i in 0..num_threads {
        let arc_clone = arc.clone();
        let barrier_clone = barrier.clone();
        handles.push(thread::spawn(move || {
            set_cpu_affinity(_i as usize);
            for _ in 0..num_bursts {
                // Synchronize for burst
                barrier_clone.wait();

                // Rapidly clone and drop
                for _ in 0..burst_size {
                    let clone = arc_clone.clone();
                    do_work(&clone);
                    drop(clone);
                }
            }
        }));
    }

    let start = Instant::now();

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    let total_ops = num_threads * num_bursts * burst_size;

    println!("Duration: {:?}", duration);
    let total_ops_f = total_ops as f64;
    let ops_per_sec = total_ops_f / duration.as_secs_f64();
    println!("Total operations: {}", humanize_number(total_ops_f));
    println!("Throughput: {}", humanize_number(ops_per_sec) + " ops/sec");
    println!(
        "Time per burst: {:.2?}ms",
        duration.as_millis() as f64 / num_bursts as f64
    );

    record_result(BenchmarkResult {
        benchmark_name: "Burst".to_string(),
        implementation: "StripedArc".to_string(),
        threads: num_threads,
        ops_per_sec,
        duration_ms: duration.as_secs_f64() * 1000.0,
        total_ops: total_ops_f,
    });
}

fn benchmark_burst_contention_std_arc(num_threads: usize, num_bursts: usize, burst_size: usize) {
    println!("\n=== std::Arc Burst Contention ===");
    println!(
        "Threads: {}, Bursts: {}, Burst size: {}",
        num_threads, num_bursts, burst_size
    );

    let arc = Arc::new(TestData::default());
    let barrier = Arc::new(std::sync::Barrier::new(num_threads));
    let mut handles = vec![];

    for _i in 0..num_threads {
        let arc_clone = arc.clone();
        let barrier_clone = barrier.clone();
        handles.push(thread::spawn(move || {
            set_cpu_affinity(_i as usize);
            for _ in 0..num_bursts {
                // Synchronize for burst
                barrier_clone.wait();

                // Rapidly clone and drop
                for _ in 0..burst_size {
                    let clone = arc_clone.clone();
                    do_work(&clone);
                    drop(clone);
                }
            }
        }));
    }

    let start = Instant::now();

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    let total_ops = num_threads * num_bursts * burst_size;

    println!("Duration: {:?}", duration);
    let total_ops_f = total_ops as f64;
    let ops_per_sec = total_ops_f / duration.as_secs_f64();
    println!("Total operations: {}", humanize_number(total_ops_f));
    println!("Throughput: {}", humanize_number(ops_per_sec) + " ops/sec");
    println!(
        "Time per burst: {:.2?}ms",
        duration.as_millis() as f64 / num_bursts as f64
    );

    record_result(BenchmarkResult {
        benchmark_name: "Burst".to_string(),
        implementation: "std::Arc".to_string(),
        threads: num_threads,
        ops_per_sec,
        duration_ms: duration.as_secs_f64() * 1000.0,
        total_ops: total_ops_f,
    });
}

// ============================================================================
// BENCHMARK 4: Cleanup Under Load
// ============================================================================
fn benchmark_cleanup_under_load_striped_arc(num_threads: usize, arc_depth: usize) {
    println!("\n=== StripedArc Cleanup Under Load ===");
    println!("Threads: {}, Arc depth: {}", num_threads, arc_depth);

    // Create a chain of arcs
    let mut arcs = Vec::new();
    arcs.push(StripedArc::new(TestData::default()));

    for _ in 1..arc_depth {
        arcs.push(arcs.last().unwrap().clone());
    }

    let start = Instant::now();
    let mut handles = vec![];

    // Threads will randomly clone/drop arcs
    for thread_id in 0..num_threads {
        let mut local_arcs = arcs.clone();
        handles.push(thread::spawn(move || {
            set_cpu_affinity(thread_id as usize);
            let mut rng = thread_id as u64;
            for iteration in 0..100000 {
                // Simple LCG for pseudo-randomness
                rng = rng.wrapping_mul(1103515245).wrapping_add(12345);
                let action = (rng & 0x1) as usize;

                if action == 0 && !local_arcs.is_empty() {
                    // Clone a random arc
                    let idx = (rng >> 1) as usize % local_arcs.len();
                    local_arcs.push(local_arcs[idx].clone());
                } else if !local_arcs.is_empty() {
                    // Drop a random arc
                    let idx = (rng >> 1) as usize % local_arcs.len();
                    local_arcs.swap_remove(idx);
                }

                if iteration % 1000 == 0 {
                    // Read from existing arcs
                    for arc in &local_arcs {
                        do_work(arc);
                    }
                }
            }
        }));
    }

    // Drop all original arcs
    drop(arcs);

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();

    println!("Duration: {:?}", duration);
    println!("Cleanup completed successfully without deadlock or crash!");

    record_result(BenchmarkResult {
        benchmark_name: "Cleanup".to_string(),
        implementation: "StripedArc".to_string(),
        threads: num_threads,
        ops_per_sec: 0.0, // Not measuring ops/sec for cleanup test
        duration_ms: duration.as_secs_f64() * 1000.0,
        total_ops: 0.0,
    });
}

fn benchmark_cleanup_under_load_std_arc(num_threads: usize, arc_depth: usize) {
    println!("\n=== std::Arc Cleanup Under Load ===");
    println!("Threads: {}, Arc depth: {}", num_threads, arc_depth);

    // Create a chain of arcs
    let mut arcs = Vec::new();
    arcs.push(Arc::new(TestData::default()));

    for _ in 1..arc_depth {
        arcs.push(arcs.last().unwrap().clone());
    }

    let start = Instant::now();
    let mut handles = vec![];

    // Threads will randomly clone/drop arcs
    for thread_id in 0..num_threads {
        let mut local_arcs = arcs.clone();
        handles.push(thread::spawn(move || {
            let mut rng = thread_id as u64;
            for iteration in 0..100000 {
                // Simple LCG for pseudo-randomness
                rng = rng.wrapping_mul(1103515245).wrapping_add(12345);
                let action = (rng & 0x1) as usize;

                if action == 0 && !local_arcs.is_empty() {
                    // Clone a random arc
                    let idx = (rng >> 1) as usize % local_arcs.len();
                    local_arcs.push(local_arcs[idx].clone());
                } else if !local_arcs.is_empty() {
                    // Drop a random arc
                    let idx = (rng >> 1) as usize % local_arcs.len();
                    local_arcs.swap_remove(idx);
                }

                if iteration % 1000 == 0 {
                    // Read from existing arcs
                    for arc in &local_arcs {
                        do_work(arc);
                    }
                }
            }
        }));
    }

    // Drop all original arcs
    drop(arcs);

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();

    println!("Duration: {:?}", duration);
    println!("Cleanup completed successfully without deadlock or crash!");

    record_result(BenchmarkResult {
        benchmark_name: "Cleanup".to_string(),
        implementation: "std::Arc".to_string(),
        threads: num_threads,
        ops_per_sec: 0.0, // Not measuring ops/sec for cleanup test
        duration_ms: duration.as_secs_f64() * 1000.0,
        total_ops: 0.0,
    });
}

// ============================================================================
// Main Benchmark Runner
// ============================================================================
fn main() {
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║         StripedArc vs std::Arc Multi-Threaded Benchmark      ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");

    let thread_counts: Vec<usize> = vec![1, 2, 4, 8, 10, 12, 14];
    // let thread_counts: Vec<usize> = vec![1, 2, 4, 8, 10, 12, 14, 16, 32];
    let available_parallelism = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    println!("\nSystem parallelism: {} threads", available_parallelism);

    // =========================================================================
    // BENCHMARK 1: Clone/Drop Throughput
    // =========================================================================
    println!("\n╔═══════════════════════════════════════════════════════════════╗");
    println!("║  BENCHMARK 1: Clone/Drop Throughput                          ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");

    for &num_threads in &thread_counts {
        let iters = 10000000 / num_threads as usize;

        println!("\n--- {} threads ---", num_threads);
        benchmark_clone_drop_striped_arc(num_threads, iters);
        benchmark_clone_drop_std_arc(num_threads, iters);
    }

    // =========================================================================
    // BENCHMARK 2: Read-Heavy Workload
    // =========================================================================
    // println!("\n╔═══════════════════════════════════════════════════════════════╗");
    // println!("║  BENCHMARK 2: Read-Heavy Workload                            ║");
    // println!("╚═══════════════════════════════════════════════════════════════╝");

    // benchmark_read_heavy_striped_arc(4, 4, 3);
    // benchmark_read_heavy_std_arc(4, 4, 3);

    // benchmark_read_heavy_striped_arc(16, 4, 3);
    // benchmark_read_heavy_std_arc(16, 4, 3);

    // =========================================================================
    // BENCHMARK 3: Burst Contention
    // =========================================================================
    // println!("\n╔═══════════════════════════════════════════════════════════════╗");
    // println!("║  BENCHMARK 3: Burst Contention                               ║");
    // println!("╚═══════════════════════════════════════════════════════════════╝");

    // for &num_threads in &[8, 16, 32] {
    //     println!("\n--- {} threads ---", num_threads);
    //     benchmark_burst_contention_striped_arc(num_threads, 100, 1000);
    //     benchmark_burst_contention_std_arc(num_threads, 100, 1000);
    // }

    // =========================================================================
    // BENCHMARK 4: Cleanup Under Load
    // =========================================================================
    // println!("\n╔═══════════════════════════════════════════════════════════════╗");
    // println!("║  BENCHMARK 4: Cleanup Under Load (Correctness Test)          ║");
    // println!("╚═══════════════════════════════════════════════════════════════╝");

    // for &num_threads in &[8, 16] {
    //     benchmark_cleanup_under_load_striped_arc(num_threads, 10);
    // }

    // benchmark_cleanup_under_load_std_arc(16, 10);

    // =========================================================================
    // PRINT SUMMARY TABLES
    // =========================================================================
    print_summary_tables();

    // =========================================================================
    // SUMMARY
    // =========================================================================
    println!("\n╔═══════════════════════════════════════════════════════════════╗");
    println!("║  BENCHMARK COMPLETE                                          ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!("\nKey observations to look for:");
    println!("  • StripedArc should outperform std::Arc under high contention");
    println!("  • Performance gap increases with thread count");
    println!("  • No deadlocks, crashes, or double-free in cleanup test");
    println!("    • StripedArc scales better with increased parallelism");
    println!("\nNote: Actual performance depends on hardware architecture.");
    println!("StripedArc benefits from multiple physical cores and cache lines.");
}

fn print_summary_tables() {
    let results = RESULTS.lock().unwrap();

    if results.is_empty() {
        return;
    }

    println!("\n╔══════════════════════════════════════════════════════════════════╗");
    println!("║                   BENCHMARK SUMMARY TABLES                       ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");

    // Group results by benchmark name
    let mut benchmark_groups: std::collections::HashMap<String, Vec<&BenchmarkResult>> =
        std::collections::HashMap::new();
    for result in results.iter() {
        benchmark_groups
            .entry(result.benchmark_name.clone())
            .or_insert_with(Vec::new)
            .push(result);
    }

    // Print tables for each benchmark
    for benchmark_name in ["Clone/Drop", "Read-Heavy", "Burst", "Cleanup"] {
        if let Some(group_results) = benchmark_groups.get(benchmark_name) {
            print_benchmark_table(benchmark_name, group_results);
        }
    }

    // Print overall comparison by thread count
    let result_refs: Vec<_> = results.iter().collect();
    print_comparison_table(&result_refs);
}

fn print_benchmark_table(benchmark_name: &str, results: &[&BenchmarkResult]) {
    if results.is_empty() {
        return;
    }

    println!("\n{}", "=".repeat(78));
    println!("Benchmark: {}", benchmark_name);
    println!("{}", "=".repeat(78));
    println!();

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            "Threads",
            "StripedArc\n(ops/sec)",
            "std::Arc\n(ops/sec)",
            "Speedup",
            "Winner",
        ]);

    // Group by thread count
    let thread_counts: std::collections::BTreeSet<_> = results.iter().map(|r| r.threads).collect();

    for thread_count in &thread_counts {
        let striped_result = results
            .iter()
            .find(|r| r.threads == *thread_count && r.implementation == "StripedArc");
        let std_result = results
            .iter()
            .find(|r| r.threads == *thread_count && r.implementation == "std::Arc");

        match (striped_result, std_result) {
            (Some(sa), Some(arc)) => {
                let sa_ops = humanize_number(sa.ops_per_sec);
                let arc_ops = humanize_number(arc.ops_per_sec);
                let speedup = if arc.ops_per_sec > 0.0 {
                    format!("{:.2}x", sa.ops_per_sec / arc.ops_per_sec)
                } else {
                    "-".to_string()
                };
                let winner = if sa.ops_per_sec > arc.ops_per_sec {
                    "StripedArc".to_string()
                } else if arc.ops_per_sec > sa.ops_per_sec {
                    "std::Arc".to_string()
                } else {
                    "Tie".to_string()
                };

                table.add_row(vec![
                    thread_count.to_string(),
                    sa_ops,
                    arc_ops,
                    speedup,
                    winner,
                ]);
            }
            (Some(sa), None) => {
                table.add_row(vec![
                    thread_count.to_string(),
                    humanize_number(sa.ops_per_sec),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                ]);
            }
            (None, Some(arc)) => {
                table.add_row(vec![
                    thread_count.to_string(),
                    "-".to_string(),
                    humanize_number(arc.ops_per_sec),
                    "-".to_string(),
                    "-".to_string(),
                ]);
            }
            _ => {}
        }
    }

    println!("{}", table);
}

fn print_comparison_table(results: &[&BenchmarkResult]) {
    println!("\n{}", "=".repeat(78));
    println!("Overall Performance Summary");
    println!("{}", "=".repeat(78));
    println!();

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec!["Benchmark", "StripedArc", "std::Arc", "Best\nThreads"]);

    let benchmarks = ["Clone/Drop", "Read-Heavy", "Burst"];

    for benchmark in benchmarks {
        let sa_results: Vec<_> = results
            .iter()
            .filter(|r| r.benchmark_name == benchmark && r.implementation == "StripedArc")
            .collect();
        let arc_results: Vec<_> = results
            .iter()
            .filter(|r| r.benchmark_name == benchmark && r.implementation == "std::Arc")
            .collect();

        let best_sa = sa_results
            .iter()
            .max_by(|a, b| a.ops_per_sec.partial_cmp(&b.ops_per_sec).unwrap());
        let best_arc = arc_results
            .iter()
            .max_by(|a, b| a.ops_per_sec.partial_cmp(&b.ops_per_sec).unwrap());

        match (best_sa, best_arc) {
            (Some(sa), Some(arc)) => {
                let winner = if sa.ops_per_sec > arc.ops_per_sec {
                    "StripedArc".to_string()
                } else {
                    "std::Arc".to_string()
                };

                table.add_row(vec![
                    benchmark.to_string(),
                    humanize_number(sa.ops_per_sec),
                    humanize_number(arc.ops_per_sec),
                    winner,
                ]);
            }
            _ => {}
        }
    }

    println!("{}", table);

    println!("\n📊 Key Insights:");
    println!("   • Speedup > 1.0x: StripedArc wins");
    println!("   • Speedup < 1.0x: std::Arc wins");
    println!("   • Look for trends as thread count increases");
    println!("   • StripedArc should improve relative performance with more threads");
}

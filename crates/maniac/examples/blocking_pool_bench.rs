//! BlockingPool Benchmark
//!
//! Comprehensive benchmarks for the BlockingPool including:
//! - Thundering herd tests (sparse signals, bursts)
//! - Raw context switching (schedule + poll cycles)
//! - Lifecycle tests (spawn overhead, thread scaling)
//! - Comparison with standard thread pool patterns

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use maniac::blocking::BlockingPool;
use maniac::scheduler::{BiasState, ContractGroup};
use maniac::util::CachePadded;

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

// ============================================================================
// Configuration
// ============================================================================

const TEST_DURATION: Duration = Duration::from_millis(500);
const MAX_THREADS: usize = 8;

// ============================================================================
// Utilities
// ============================================================================

/// Simple LCG random number generator
struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        maniac::util::random::rapidrng_fast(&mut self.state)
    }

    fn random_duration(&mut self, min_ns: u64, max_ns: u64) -> Duration {
        let range = max_ns - min_ns;
        let ns = min_ns + (self.next() % range.max(1));
        Duration::from_nanos(ns)
    }
}

/// Hash-based work that consumes CPU cycles
fn hash_work<const N: usize>() -> usize {
    const STR: &str = "this is a peculiar set of words";
    let mut n: usize = 0;
    for _ in 0..N {
        let mut hasher = DefaultHasher::new();
        STR.hash(&mut hasher);
        n = n.wrapping_mul(hasher.finish() as usize);
    }
    std::hint::black_box(n)
}

/// Spin for approximately the given number of nanoseconds
fn spin_ns(ns: u64) {
    let start = Instant::now();
    while start.elapsed().as_nanos() < ns as u128 {
        std::hint::spin_loop();
    }
}

/// Statistics collector for active thread counts
struct ActiveThreadStats {
    histogram: Vec<AtomicU64>,
    weighted_sum: AtomicU64,
    total_samples: AtomicU64,
    max_active: AtomicUsize,
    over_activation_count: AtomicU64,
    over_activation_sum: AtomicU64,
}

impl ActiveThreadStats {
    fn new(max_threads: usize) -> Self {
        let mut histogram = Vec::with_capacity(max_threads + 1);
        for _ in 0..=max_threads {
            histogram.push(AtomicU64::new(0));
        }
        Self {
            histogram,
            weighted_sum: AtomicU64::new(0),
            total_samples: AtomicU64::new(0),
            max_active: AtomicUsize::new(0),
            over_activation_count: AtomicU64::new(0),
            over_activation_sum: AtomicU64::new(0),
        }
    }

    fn record(&self, active_count: usize, expected_max: usize) {
        if active_count < self.histogram.len() {
            self.histogram[active_count].fetch_add(1, Ordering::Relaxed);
        }
        self.weighted_sum
            .fetch_add(active_count as u64, Ordering::Relaxed);
        self.total_samples.fetch_add(1, Ordering::Relaxed);

        let mut current_max = self.max_active.load(Ordering::Relaxed);
        while active_count > current_max {
            match self.max_active.compare_exchange_weak(
                current_max,
                active_count,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }

        if active_count > expected_max {
            self.over_activation_count.fetch_add(1, Ordering::Relaxed);
            self.over_activation_sum
                .fetch_add((active_count - expected_max) as u64, Ordering::Relaxed);
        }
    }

    fn print_report(&self, num_threads: usize) {
        let total = self.total_samples.load(Ordering::Relaxed);
        if total == 0 {
            println!("  No samples collected");
            return;
        }

        let weighted = self.weighted_sum.load(Ordering::Relaxed);
        let avg = weighted as f64 / total as f64;
        let max = self.max_active.load(Ordering::Relaxed);
        let over_count = self.over_activation_count.load(Ordering::Relaxed);
        let over_sum = self.over_activation_sum.load(Ordering::Relaxed);

        println!("\n  Active Thread Statistics:");
        println!("  -------------------------");
        println!("  Average active threads: {:.2}", avg);
        println!(
            "  Maximum active threads: {} (of {} total)",
            max, num_threads
        );
        println!(
            "  Over-activation events: {} ({:.2}% of samples)",
            over_count,
            100.0 * over_count as f64 / total as f64
        );
        if over_count > 0 {
            println!(
                "  Avg extra threads per over-activation: {:.2}",
                over_sum as f64 / over_count as f64
            );
        }

        println!("\n  Histogram (active threads -> % of time):");
        for (count, occurrences) in self.histogram.iter().enumerate() {
            let occ = occurrences.load(Ordering::Relaxed);
            if occ > 0 {
                let pct = 100.0 * occ as f64 / total as f64;
                let bar_len = (pct / 2.0) as usize;
                let bar: String = std::iter::repeat('#').take(bar_len).collect();
                println!("    {:2}: {:6.2}% {}", count, pct, bar);
            }
        }
    }
}

/// Gather statistics from counts
fn gather_stats(input: &[usize]) -> (usize, f64, f64, f64) {
    let total: usize = input.iter().sum();
    let mean = total as f64 / input.len().max(1) as f64;

    let variance: f64 = input
        .iter()
        .map(|&v| {
            let diff = v as f64 - mean;
            diff * diff
        })
        .sum::<f64>()
        / (input.len() - 1).max(1) as f64;

    let sd = variance.sqrt();
    let cv = if mean > 0.0 { sd / mean } else { 0.0 };

    (total, mean, sd, cv)
}

fn humanize(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        format!("{}", n)
    }
}

// ============================================================================
// Thundering Herd Tests
// ============================================================================

fn run_thundering_herd_test(
    name: &str,
    min_threads: usize,
    max_threads: usize,
    test_duration: Duration,
    signal_interval_min_ns: u64,
    signal_interval_max_ns: u64,
    work_duration_min_ns: u64,
    work_duration_max_ns: u64,
) {
    println!("\n{}", "=".repeat(70));
    println!("Test: {}", name);
    println!("{}", "=".repeat(70));
    println!("  Threads: {} - {}", min_threads, max_threads);
    println!(
        "  Signal interval: {}ns - {}ns",
        signal_interval_min_ns, signal_interval_max_ns
    );
    println!(
        "  Work duration: {}ns - {}ns",
        work_duration_min_ns, work_duration_max_ns
    );

    let pool = Arc::new(BlockingPool::with_threads(min_threads, max_threads));
    let active_threads = Arc::new(AtomicUsize::new(0));
    let tasks_completed = Arc::new(AtomicU64::new(0));
    let stats = Arc::new(ActiveThreadStats::new(max_threads));
    let pending_signals = Arc::new(AtomicUsize::new(0));

    let start_time = Instant::now();
    let mut rng = SimpleRng::new(12345);
    let mut task_id: u64 = 0;

    while start_time.elapsed() < test_duration {
        let delay = rng.random_duration(signal_interval_min_ns, signal_interval_max_ns);
        thread::sleep(delay);

        let active_threads = Arc::clone(&active_threads);
        let tasks_completed = Arc::clone(&tasks_completed);
        let pending_signals_clone = Arc::clone(&pending_signals);
        let stats = Arc::clone(&stats);
        let work_min = work_duration_min_ns;
        let work_max = work_duration_max_ns;
        let id = task_id;
        task_id += 1;

        pending_signals.fetch_add(1, Ordering::Relaxed);

        pool.spawn(move || {
            let pending = pending_signals_clone.fetch_sub(1, Ordering::Relaxed);
            let prev_active = active_threads.fetch_add(1, Ordering::Relaxed);

            let expected_max = pending.min(8).max(1);
            stats.record(prev_active + 1, expected_max);

            let work_ns = work_min + ((id * 7) % (work_max - work_min).max(1));
            spin_ns(work_ns);

            active_threads.fetch_sub(1, Ordering::Relaxed);
            tasks_completed.fetch_add(1, Ordering::Relaxed);
        });
    }

    // Wait for pending tasks
    thread::sleep(Duration::from_millis(100));

    let completed = tasks_completed.load(Ordering::Relaxed);
    let rate = completed as f64 / test_duration.as_secs_f64();
    println!("\n  Tasks completed: {} ({:.0}/sec)", completed, rate);
    println!(
        "  Final thread count: {} active, {} idle",
        pool.active_threads(),
        pool.idle_threads()
    );

    stats.print_report(max_threads);
}

fn run_burst_test(
    name: &str,
    min_threads: usize,
    max_threads: usize,
    test_duration: Duration,
    burst_size: usize,
    burst_interval_ms: u64,
    work_duration_ns: u64,
) {
    println!("\n{}", "=".repeat(70));
    println!("Burst Test: {}", name);
    println!("{}", "=".repeat(70));
    println!("  Threads: {} - {}", min_threads, max_threads);
    println!("  Burst size: {} tasks at once", burst_size);
    println!("  Burst interval: {}ms", burst_interval_ms);
    println!("  Work duration: {}ns", work_duration_ns);

    let pool = Arc::new(BlockingPool::with_threads(min_threads, max_threads));
    let active_threads = Arc::new(AtomicUsize::new(0));
    let tasks_completed = Arc::new(AtomicU64::new(0));
    let stats = Arc::new(ActiveThreadStats::new(max_threads));
    let pending_signals = Arc::new(AtomicUsize::new(0));
    let bursts_sent = Arc::new(AtomicU64::new(0));

    let start_time = Instant::now();

    while start_time.elapsed() < test_duration {
        for _ in 0..burst_size {
            let active_threads = Arc::clone(&active_threads);
            let tasks_completed = Arc::clone(&tasks_completed);
            let pending_signals_clone = Arc::clone(&pending_signals);
            let stats = Arc::clone(&stats);
            let work_ns = work_duration_ns;

            pending_signals.fetch_add(1, Ordering::Relaxed);

            pool.spawn(move || {
                let pending = pending_signals_clone.fetch_sub(1, Ordering::Relaxed);
                let prev_active = active_threads.fetch_add(1, Ordering::Relaxed);
                let expected_max = pending.min(8).max(1);
                stats.record(prev_active + 1, expected_max);

                spin_ns(work_ns);

                active_threads.fetch_sub(1, Ordering::Relaxed);
                tasks_completed.fetch_add(1, Ordering::Relaxed);
            });
        }
        bursts_sent.fetch_add(1, Ordering::Relaxed);

        thread::sleep(Duration::from_millis(burst_interval_ms));
    }

    thread::sleep(Duration::from_millis(100));

    let completed = tasks_completed.load(Ordering::Relaxed);
    let bursts = bursts_sent.load(Ordering::Relaxed);
    let rate = completed as f64 / test_duration.as_secs_f64();
    println!("\n  Bursts sent: {}", bursts);
    println!("  Tasks completed: {} ({:.0}/sec)", completed, rate);
    println!(
        "  Expected tasks: {} (burst_size * bursts)",
        burst_size as u64 * bursts
    );
    println!(
        "  Final thread count: {} active, {} idle",
        pool.active_threads(),
        pool.idle_threads()
    );

    stats.print_report(max_threads);
}

// ============================================================================
// Context Switching / Throughput Tests
// ============================================================================

fn bench_spawn_throughput(
    num_threads: usize,
    num_producers: usize,
    test_duration: Duration,
) -> (u64, f64) {
    let pool = Arc::new(BlockingPool::with_threads(8, 128));
    let stop = Arc::new(AtomicBool::new(false));

    // Per-producer counters to avoid contention
    let completed: Arc<Vec<CachePadded<AtomicU64>>> = Arc::new(
        (0..num_producers)
            .map(|_| CachePadded::new(AtomicU64::new(0)))
            .collect(),
    );
    let spawned: Arc<Vec<CachePadded<AtomicU64>>> = Arc::new(
        (0..num_producers)
            .map(|_| CachePadded::new(AtomicU64::new(0)))
            .collect(),
    );

    // Keep at most 2K tasks in flight per producer (8 producers * 2K = 16K, well within 64K)
    const MAX_IN_FLIGHT_PER_PRODUCER: u64 = 65536;

    let mut handles = Vec::new();

    // Spawn producer threads
    for i in 0..num_producers {
        let pool = Arc::clone(&pool);
        let completed = Arc::clone(&completed);
        let spawned = Arc::clone(&spawned);
        let stop = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                // Backpressure on this producer's counters only
                let my_spawned = spawned[i].load(Ordering::Relaxed);
                let my_done = completed[i].load(Ordering::Relaxed);
                if my_spawned.saturating_sub(my_done) >= MAX_IN_FLIGHT_PER_PRODUCER {
                    thread::yield_now();
                    continue;
                }

                let completed = Arc::clone(&completed);
                pool.spawn(move || {
                    completed[i].fetch_add(1, Ordering::Relaxed);
                });
                spawned[i].fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    let start = Instant::now();
    thread::sleep(test_duration);
    stop.store(true, Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }

    // Wait for remaining tasks to complete
    let total_spawned: u64 = spawned.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    loop {
        let total_completed: u64 = completed.iter().map(|c| c.load(Ordering::Relaxed)).sum();
        if total_completed >= total_spawned {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    pool.shutdown();

    let total: u64 = completed.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    let elapsed = start.elapsed().as_secs_f64();
    let rate = total as f64 / elapsed;

    (total, rate)
}

/// Benchmark standard ContractGroup with spin-polling workers
fn bench_contract_group_throughput(
    num_threads: usize,
    num_producers: usize,
    test_duration: Duration,
) -> (u64, f64) {
    let group = Arc::new(ContractGroup::new_blocking_with_capacity(65536 * 8));
    let stop = Arc::new(AtomicBool::new(false));

    // Per-producer counters to avoid contention
    let completed: Arc<Vec<CachePadded<AtomicU64>>> = Arc::new(
        (0..num_producers)
            .map(|_| CachePadded::new(AtomicU64::new(0)))
            .collect(),
    );
    let spawned: Arc<Vec<CachePadded<AtomicU64>>> = Arc::new(
        (0..num_producers)
            .map(|_| CachePadded::new(AtomicU64::new(0)))
            .collect(),
    );

    const MAX_IN_FLIGHT_PER_PRODUCER: u64 = 65536;

    let mut handles = Vec::new();

    // Spawn worker threads that spin-poll the contract group
    for _ in 0..num_threads {
        let group = Arc::clone(&group);
        let stop = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            let mut bias = BiasState::new();
            while !stop.load(Ordering::Relaxed) {
                if group.poll_next(&mut bias).is_none() {
                    std::hint::spin_loop();
                }
            }
            // Drain remaining work
            while group.poll_next(&mut bias).is_some() {}
        });
        handles.push(handle);
    }

    // Spawn producer threads
    for i in 0..num_producers {
        let group = Arc::clone(&group);
        let completed = Arc::clone(&completed);
        let spawned = Arc::clone(&spawned);
        let stop = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                // Backpressure
                let my_spawned = spawned[i].load(Ordering::Relaxed);
                let my_done = completed[i].load(Ordering::Relaxed);
                if my_spawned.saturating_sub(my_done) >= MAX_IN_FLIGHT_PER_PRODUCER {
                    // thread::yield_now();
                    // continue;
                }

                let completed = Arc::clone(&completed);
                let _ = group.spawn(
                    async move {
                        completed[i].fetch_add(1, Ordering::Relaxed);
                    },
                    true,
                );
                spawned[i].fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    let start = Instant::now();
    thread::sleep(test_duration);
    stop.store(true, Ordering::Relaxed);
    group.stop();

    for handle in handles {
        handle.join().unwrap();
    }

    let total: u64 = completed.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    let elapsed = start.elapsed().as_secs_f64();
    let rate = total as f64 / elapsed;

    (total, rate)
}

fn bench_spawn_blocking_roundtrip(num_threads: usize, test_duration: Duration) -> (u64, f64) {
    // Use 2x threads so there's always a worker available
    let pool = Arc::new(BlockingPool::with_threads(num_threads * 2, num_threads * 2));
    let completed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();
    for _ in 0..num_threads {
        let pool = Arc::clone(&pool);
        let completed = Arc::clone(&completed);
        let stop = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let result = pool.spawn_blocking(|| {
                    std::hint::black_box(hash_work::<1>());
                    42u64
                });
                std::hint::black_box(result.join());
                completed.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    let start = Instant::now();
    thread::sleep(test_duration);
    stop.store(true, Ordering::Relaxed);

    for handle in handles {
        handle.join().unwrap();
    }

    pool.shutdown();

    let total = completed.load(Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64();
    let rate = total as f64 / elapsed;

    (total, rate)
}

// ============================================================================
// Lifecycle Tests
// ============================================================================

fn bench_thread_scaling(test_duration: Duration) {
    println!("\n{}", "=".repeat(70));
    println!("Thread Scaling Benchmark");
    println!("{}", "=".repeat(70));
    println!("Tests dynamic thread pool scaling under load\n");

    let pool = Arc::new(BlockingPool::with_config(
        1,
        MAX_THREADS,
        Duration::from_millis(500),
    ));
    let completed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    // Track thread counts over time
    let thread_samples = Arc::new(std::sync::Mutex::new(Vec::new()));

    // Sampling thread
    let pool_sample = Arc::clone(&pool);
    let samples = Arc::clone(&thread_samples);
    let stop_sample = Arc::clone(&stop);
    let sampler = thread::spawn(move || {
        while !stop_sample.load(Ordering::Relaxed) {
            let active = pool_sample.active_threads();
            let idle = pool_sample.idle_threads();
            samples.lock().unwrap().push((active, idle));
            thread::sleep(Duration::from_millis(10));
        }
    });

    // Producer that ramps up load
    let pool_producer = Arc::clone(&pool);
    let completed_producer = Arc::clone(&completed);
    let stop_producer = Arc::clone(&stop);
    let producer = thread::spawn(move || {
        let start = Instant::now();
        let ramp_duration = test_duration.as_secs_f64() / 3.0;

        while !stop_producer.load(Ordering::Relaxed) {
            let elapsed = start.elapsed().as_secs_f64();

            // Ramp up load over time
            let target_rate = if elapsed < ramp_duration {
                (elapsed / ramp_duration * 10000.0) as u64
            } else if elapsed < ramp_duration * 2.0 {
                10000
            } else {
                // Ramp down
                let remaining = (test_duration.as_secs_f64() - elapsed) / ramp_duration;
                (remaining * 10000.0).max(100.0) as u64
            };

            let sleep_ns = 1_000_000_000 / target_rate.max(1);

            let completed = Arc::clone(&completed_producer);
            pool_producer.spawn(move || {
                spin_ns(1000); // 1us work
                completed.fetch_add(1, Ordering::Relaxed);
            });

            if sleep_ns > 1000 {
                thread::sleep(Duration::from_nanos(sleep_ns));
            }
        }
    });

    thread::sleep(test_duration);
    stop.store(true, Ordering::Relaxed);

    producer.join().unwrap();
    sampler.join().unwrap();

    let samples = thread_samples.lock().unwrap();
    let total = completed.load(Ordering::Relaxed);

    println!("  Tasks completed: {}", total);
    println!("  Samples collected: {}", samples.len());

    if !samples.is_empty() {
        let avg_active: f64 =
            samples.iter().map(|(a, _)| *a as f64).sum::<f64>() / samples.len() as f64;
        let max_active = samples.iter().map(|(a, _)| *a).max().unwrap_or(0);
        let avg_idle: f64 =
            samples.iter().map(|(_, i)| *i as f64).sum::<f64>() / samples.len() as f64;

        println!("  Average active threads: {:.2}", avg_active);
        println!("  Maximum active threads: {}", max_active);
        println!("  Average idle threads: {:.2}", avg_idle);
    }
}

fn bench_idle_timeout(idle_timeout: Duration) {
    println!("\n{}", "=".repeat(70));
    println!("Idle Timeout Benchmark");
    println!("{}", "=".repeat(70));
    println!("Tests thread cleanup after idle period\n");

    let pool = Arc::new(BlockingPool::with_config(1, 8, idle_timeout));

    // Spawn burst of work to scale up
    let barrier = Arc::new(std::sync::Barrier::new(9));
    for _ in 0..8 {
        let barrier = Arc::clone(&barrier);
        pool.spawn(move || {
            barrier.wait();
        });
    }

    // Release barrier
    thread::sleep(Duration::from_millis(50));
    barrier.wait();

    println!(
        "  After burst: {} active, {} idle",
        pool.active_threads(),
        pool.idle_threads()
    );

    // Wait for idle timeout
    thread::sleep(idle_timeout + Duration::from_millis(200));

    println!(
        "  After timeout: {} active, {} idle",
        pool.active_threads(),
        pool.idle_threads()
    );
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    set_cpu_affinity(0);

    println!("BlockingPool Comprehensive Benchmark");
    println!("=====================================\n");

    // ========================================================================
    // Thundering Herd Tests
    // ========================================================================
    // println!("\n\x1b[1;36m{}\x1b[0m", "=".repeat(70));
    // println!("\x1b[1;36mTHUNDERING HERD TESTS\x1b[0m");
    // println!("\x1b[1;36m{}\x1b[0m", "=".repeat(70));
    // println!("Tests how efficiently threads are woken when work arrives.");
    // println!("Ideal: active threads ~= pending work items");
    // println!("Thundering herd: many threads wake but only few find work\n");

    // // Fixed pool tests
    // run_thundering_herd_test(
    //     "Fixed Pool - Sparse Signals",
    //     4,
    //     4,
    //     TEST_DURATION,
    //     1_000_000,
    //     5_000_000, // 1-5ms between signals
    //     100_000,
    //     200_000, // 100-200us work
    // );

    // run_thundering_herd_test(
    //     "Fixed Pool - Moderate Load",
    //     4,
    //     4,
    //     TEST_DURATION,
    //     100_000,
    //     500_000, // 100-500us between signals
    //     50_000,
    //     100_000, // 50-100us work
    // );

    // // Dynamic pool tests
    // run_thundering_herd_test(
    //     "Dynamic Pool - Sparse Signals (1-8 threads)",
    //     1,
    //     8,
    //     TEST_DURATION,
    //     1_000_000,
    //     5_000_000,
    //     100_000,
    //     200_000,
    // );

    // run_thundering_herd_test(
    //     "Dynamic Pool - High Load (1-8 threads)",
    //     1,
    //     8,
    //     TEST_DURATION,
    //     1_000,
    //     10_000, // 1-10us between signals
    //     10_000,
    //     50_000, // 10-50us work
    // );

    // ========================================================================
    // Burst Tests
    // ========================================================================
    // println!("\n\n\x1b[1;36m{}\x1b[0m", "=".repeat(70));
    // println!("\x1b[1;36mBURST TESTS\x1b[0m");
    // println!("\x1b[1;36m{}\x1b[0m", "=".repeat(70));
    // println!("Tests chain wakeup mechanism with burst arrivals\n");

    // run_burst_test(
    //     "Small Burst (4 tasks, 4 fixed threads)",
    //     4,
    //     4,
    //     TEST_DURATION,
    //     4,       // burst size
    //     10,      // 10ms between bursts
    //     100_000, // 100us work
    // );

    // run_burst_test(
    //     "Large Burst (8 tasks, 4 fixed threads)",
    //     4,
    //     4,
    //     TEST_DURATION,
    //     8,
    //     10,
    //     100_000,
    // );

    // run_burst_test(
    //     "Dynamic Burst (8 tasks, 1-8 threads)",
    //     1,
    //     8,
    //     TEST_DURATION,
    //     8,
    //     10,
    //     100_000,
    // );

    // ========================================================================
    // Throughput Tests
    // ========================================================================
    println!("\n\n\x1b[1;36m{}\x1b[0m", "=".repeat(70));
    println!("\x1b[1;36mTHROUGHPUT TESTS\x1b[0m");
    println!("\x1b[1;36m{}\x1b[0m", "=".repeat(70));

    println!("\n{}", "=".repeat(70));
    println!("Spawn Throughput (fire-and-forget, 1 producer)");
    println!("{}", "=".repeat(70));
    println!("{:<15}{:<20}{:<20}", "Threads", "Total Tasks", "Tasks/sec");
    println!("{}", "-".repeat(55));

    for threads in [1, 2, 4, 8] {
        if threads > MAX_THREADS {
            continue;
        }
        let (total, rate) = bench_spawn_throughput(threads, 1, TEST_DURATION);
        println!(
            "{:<15}{:<20}{:<20}",
            threads,
            humanize(total),
            humanize(rate as u64)
        );
    }

    println!("\n{}", "=".repeat(70));
    println!("Spawn Throughput (fire-and-forget, N producers)");
    println!("{}", "=".repeat(70));
    println!(
        "{:<15}{:<15}{:<20}{:<20}",
        "Threads", "Producers", "Total Tasks", "Tasks/sec"
    );
    println!("{}", "-".repeat(70));

    for threads in [2, 4, 8] {
        if threads > MAX_THREADS {
            continue;
        }
        let (total, rate) = bench_spawn_throughput(threads, threads, TEST_DURATION);
        println!(
            "{:<15}{:<15}{:<20}{:<20}",
            threads,
            threads,
            humanize(total),
            humanize(rate as u64)
        );
    }

    println!("\n{}", "=".repeat(70));
    println!("Spawn Blocking Roundtrip (spawn + join)");
    println!("{}", "=".repeat(70));
    println!(
        "{:<15}{:<20}{:<20}",
        "Threads", "Roundtrips", "Roundtrips/sec"
    );
    println!("{}", "-".repeat(55));

    for threads in [1, 2, 4, 8] {
        if threads > MAX_THREADS {
            continue;
        }
        let (total, rate) = bench_spawn_blocking_roundtrip(threads, TEST_DURATION);
        println!(
            "{:<15}{:<20}{:<20}",
            threads,
            humanize(total),
            humanize(rate as u64)
        );
    }

    // ========================================================================
    // Comparison: BlockingPool vs ContractGroup (spin-polling)
    // ========================================================================
    println!("\n\n\x1b[1;36m{}\x1b[0m", "=".repeat(70));
    println!("\x1b[1;36mCOMPARISON: BlockingPool vs ContractGroup (spin-polling)\x1b[0m");
    println!("\x1b[1;36m{}\x1b[0m", "=".repeat(70));
    println!("BlockingPool uses blocking semaphore, ContractGroup uses spin-polling\n");

    println!("{}", "=".repeat(90));
    println!("Single Producer Comparison");
    println!("{}", "=".repeat(90));
    println!(
        "{:<10}{:<20}{:<20}{:<20}{:<20}",
        "Threads", "Blocking/sec", "Spin-poll/sec", "Ratio", "Winner"
    );
    println!("{}", "-".repeat(90));

    for threads in [1, 2, 4, 8] {
        if threads > MAX_THREADS {
            continue;
        }
        let (_, blocking_rate) = bench_spawn_throughput(threads, 1, TEST_DURATION);
        let (_, spin_rate) = bench_contract_group_throughput(threads, 1, TEST_DURATION);
        let ratio = spin_rate / blocking_rate;
        let winner = if ratio > 1.1 {
            "Spin-poll"
        } else if ratio < 0.9 {
            "Blocking"
        } else {
            "~Equal"
        };
        println!(
            "{:<10}{:<20}{:<20}{:<20.2}{:<20}",
            threads,
            humanize(blocking_rate as u64),
            humanize(spin_rate as u64),
            ratio,
            winner
        );
    }

    println!("\n{}", "=".repeat(90));
    println!("N Producers Comparison (producers = threads)");
    println!("{}", "=".repeat(90));
    println!(
        "{:<10}{:<20}{:<20}{:<20}{:<20}",
        "Threads", "Blocking/sec", "Spin-poll/sec", "Ratio", "Winner"
    );
    println!("{}", "-".repeat(90));

    for threads in [2, 4, 8] {
        if threads > MAX_THREADS {
            continue;
        }
        let (_, blocking_rate) = bench_spawn_throughput(threads, threads, TEST_DURATION);
        let (_, spin_rate) = bench_contract_group_throughput(threads, threads, TEST_DURATION);
        let ratio = spin_rate / blocking_rate;
        let winner = if ratio > 1.1 {
            "Spin-poll"
        } else if ratio < 0.9 {
            "Blocking"
        } else {
            "~Equal"
        };
        println!(
            "{:<10}{:<20}{:<20}{:<20.2}{:<20}",
            threads,
            humanize(blocking_rate as u64),
            humanize(spin_rate as u64),
            ratio,
            winner
        );
    }

    // ========================================================================
    // Lifecycle Tests
    // ========================================================================
    println!("\n\n\x1b[1;36m{}\x1b[0m", "=".repeat(70));
    println!("\x1b[1;36mLIFECYCLE TESTS\x1b[0m");
    println!("\x1b[1;36m{}\x1b[0m", "=".repeat(70));

    bench_thread_scaling(Duration::from_secs(1));
    bench_idle_timeout(Duration::from_millis(100));

    println!("\n\nBenchmark complete.");
}

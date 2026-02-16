//! Benchmark comparing AtomicWaker vs DiatomicWaker vs SimpleWaker vs GenWaker.
//!
//! This benchmark tests various scenarios:
//! 1. Single-threaded register/notify cycles
//! 2. Multi-threaded contention (N notifiers, 1 registrant)
//! 3. Rapid re-registration with same waker (common case)
//! 4. Rapid re-registration with different wakers
//! 5. Notify-heavy workloads (many notifiers, few registrations)
//!
//! Run with: cargo run --release --example waker_bench --features io-driver

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{RawWaker, RawWakerVTable, Waker};
use std::thread;
use std::time::{Duration, Instant};

use atomic_waker::AtomicWaker;
use maniac::future::waker::{DiatomicWaker, GenWaker, SimpleWaker};

// ============================================================================
// Test Wakers
// ============================================================================

static WAKE_COUNT: AtomicU64 = AtomicU64::new(0);

fn noop_clone(data: *const ()) -> RawWaker {
    RawWaker::new(data, &NOOP_VTABLE)
}

fn noop_wake(_data: *const ()) {
    WAKE_COUNT.fetch_add(1, Ordering::Relaxed);
}

fn noop_wake_by_ref(_data: *const ()) {
    WAKE_COUNT.fetch_add(1, Ordering::Relaxed);
}

fn noop_drop(_data: *const ()) {}

const NOOP_VTABLE: RawWakerVTable =
    RawWakerVTable::new(noop_clone, noop_wake, noop_wake_by_ref, noop_drop);

fn counting_waker() -> Waker {
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_VTABLE)) }
}

fn unique_waker(id: usize) -> Waker {
    unsafe { Waker::from_raw(RawWaker::new(id as *const (), &NOOP_VTABLE)) }
}

// ============================================================================
// Benchmark Configuration
// ============================================================================

const WARMUP_ITERATIONS: u64 = 10_000;
const BENCH_ITERATIONS: u64 = 10_000_000;
const CONTENTION_DURATION_SECS: u64 = 2;

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
// Benchmark Harness
// ============================================================================

struct BenchResult {
    name: &'static str,
    atomic_ns: f64,
    diatomic_ns: f64,
    simple_ns: f64,
    gen_ns: f64,
}

impl BenchResult {
    fn print(&self) {
        let best_ns = self
            .atomic_ns
            .min(self.diatomic_ns)
            .min(self.simple_ns)
            .min(self.gen_ns);
        let best = if best_ns == self.gen_ns {
            "Gen"
        } else if best_ns == self.simple_ns {
            "Simple"
        } else if best_ns == self.diatomic_ns {
            "Diatomic"
        } else {
            "Atomic"
        };

        println!(
            "  {:<35} Atomic:{:>6.1}  Diatomic:{:>6.1}  Simple:{:>6.1}  Gen:{:>6.1}  [{}]",
            self.name, self.atomic_ns, self.diatomic_ns, self.simple_ns, self.gen_ns, best
        );
    }
}

// ============================================================================
// Individual Benchmarks
// ============================================================================

fn bench_register_notify() -> BenchResult {
    let waker = counting_waker();

    // AtomicWaker
    let atomic = AtomicWaker::new();
    for _ in 0..WARMUP_ITERATIONS {
        atomic.register(&waker);
        atomic.wake();
    }
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        atomic.register(&waker);
        atomic.wake();
    }
    let atomic_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    // DiatomicWaker
    let diatomic = DiatomicWaker::new();
    for _ in 0..WARMUP_ITERATIONS {
        unsafe { diatomic.register(&waker) };
        diatomic.notify();
    }
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        unsafe { diatomic.register(&waker) };
        diatomic.notify();
    }
    let diatomic_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    // SimpleWaker
    let simple = SimpleWaker::new();
    for _ in 0..WARMUP_ITERATIONS {
        unsafe { simple.register(&waker) };
        simple.notify();
    }
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        unsafe { simple.register(&waker) };
        simple.notify();
    }
    let simple_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    // GenWaker
    let gw = GenWaker::new();
    for _ in 0..WARMUP_ITERATIONS {
        unsafe { gw.register(&waker) };
        gw.notify();
    }
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        unsafe { gw.register(&waker) };
        gw.notify();
    }
    let gen_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    BenchResult {
        name: "register + notify cycle",
        atomic_ns,
        diatomic_ns,
        simple_ns,
        gen_ns,
    }
}

fn bench_register_same_waker() -> BenchResult {
    let waker = counting_waker();

    // AtomicWaker
    let atomic = AtomicWaker::new();
    atomic.register(&waker);
    for _ in 0..WARMUP_ITERATIONS {
        atomic.register(&waker);
    }
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        atomic.register(&waker);
    }
    let atomic_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    // DiatomicWaker
    let diatomic = DiatomicWaker::new();
    unsafe { diatomic.register(&waker) };
    for _ in 0..WARMUP_ITERATIONS {
        unsafe { diatomic.register(&waker) };
    }
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        unsafe { diatomic.register(&waker) };
    }
    let diatomic_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    // SimpleWaker
    let simple = SimpleWaker::new();
    unsafe { simple.register(&waker) };
    for _ in 0..WARMUP_ITERATIONS {
        unsafe { simple.register(&waker) };
    }
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        unsafe { simple.register(&waker) };
    }
    let simple_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    // GenWaker
    let gw = GenWaker::new();
    unsafe { gw.register(&waker) };
    for _ in 0..WARMUP_ITERATIONS {
        unsafe { gw.register(&waker) };
    }
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        unsafe { gw.register(&waker) };
    }
    let gen_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    BenchResult {
        name: "re-register same waker (no notify)",
        atomic_ns,
        diatomic_ns,
        simple_ns,
        gen_ns,
    }
}

fn bench_register_different_wakers() -> BenchResult {
    // AtomicWaker
    let atomic = AtomicWaker::new();
    for i in 0..WARMUP_ITERATIONS {
        atomic.register(&unique_waker(i as usize));
    }
    let start = Instant::now();
    for i in 0..BENCH_ITERATIONS {
        atomic.register(&unique_waker(i as usize));
    }
    let atomic_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    // DiatomicWaker
    let diatomic = DiatomicWaker::new();
    for i in 0..WARMUP_ITERATIONS {
        unsafe { diatomic.register(&unique_waker(i as usize)) };
    }
    let start = Instant::now();
    for i in 0..BENCH_ITERATIONS {
        unsafe { diatomic.register(&unique_waker(i as usize)) };
    }
    let diatomic_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    // SimpleWaker
    let simple = SimpleWaker::new();
    for i in 0..WARMUP_ITERATIONS {
        unsafe { simple.register(&unique_waker(i as usize)) };
    }
    let start = Instant::now();
    for i in 0..BENCH_ITERATIONS {
        unsafe { simple.register(&unique_waker(i as usize)) };
    }
    let simple_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    // GenWaker
    let gw = GenWaker::new();
    for i in 0..WARMUP_ITERATIONS {
        unsafe { gw.register(&unique_waker(i as usize)) };
    }
    let start = Instant::now();
    for i in 0..BENCH_ITERATIONS {
        unsafe { gw.register(&unique_waker(i as usize)) };
    }
    let gen_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    BenchResult {
        name: "register different wakers",
        atomic_ns,
        diatomic_ns,
        simple_ns,
        gen_ns,
    }
}

fn bench_notify_no_waker() -> BenchResult {
    // AtomicWaker
    let atomic = AtomicWaker::new();
    for _ in 0..WARMUP_ITERATIONS {
        atomic.wake();
    }
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        black_box(atomic.wake());
    }
    let atomic_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    // DiatomicWaker
    let diatomic = DiatomicWaker::new();
    for _ in 0..WARMUP_ITERATIONS {
        diatomic.notify();
    }
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        black_box(diatomic.notify());
    }
    let diatomic_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    // SimpleWaker
    let simple = SimpleWaker::new();
    for _ in 0..WARMUP_ITERATIONS {
        simple.notify();
    }
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        black_box(simple.notify());
    }
    let simple_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    // GenWaker - always wakes if waker present, but here there's no waker
    let gw = GenWaker::new();
    for _ in 0..WARMUP_ITERATIONS {
        gw.notify();
    }
    let start = Instant::now();
    for _ in 0..BENCH_ITERATIONS {
        black_box(gw.notify());
    }
    let gen_ns = start.elapsed().as_nanos() as f64 / BENCH_ITERATIONS as f64;

    BenchResult {
        name: "notify (no waker)",
        atomic_ns,
        diatomic_ns,
        simple_ns,
        gen_ns,
    }
}

fn bench_burst_notify() -> BenchResult {
    const BURST_SIZE: u64 = 100;
    const NUM_BURSTS: u64 = 100_000;
    let waker = counting_waker();

    // AtomicWaker
    let atomic = AtomicWaker::new();
    atomic.register(&waker);
    let start = Instant::now();
    for _ in 0..NUM_BURSTS {
        for _ in 0..BURST_SIZE {
            atomic.wake();
        }
        atomic.register(&waker);
    }
    let atomic_ns = start.elapsed().as_nanos() as f64 / (NUM_BURSTS * (BURST_SIZE + 1)) as f64;

    // DiatomicWaker
    let diatomic = DiatomicWaker::new();
    unsafe { diatomic.register(&waker) };
    let start = Instant::now();
    for _ in 0..NUM_BURSTS {
        for _ in 0..BURST_SIZE {
            diatomic.notify();
        }
        unsafe { diatomic.register(&waker) };
    }
    let diatomic_ns = start.elapsed().as_nanos() as f64 / (NUM_BURSTS * (BURST_SIZE + 1)) as f64;

    // SimpleWaker
    let simple = SimpleWaker::new();
    unsafe { simple.register(&waker) };
    let start = Instant::now();
    for _ in 0..NUM_BURSTS {
        for _ in 0..BURST_SIZE {
            simple.notify();
        }
        unsafe { simple.register(&waker) };
    }
    let simple_ns = start.elapsed().as_nanos() as f64 / (NUM_BURSTS * (BURST_SIZE + 1)) as f64;

    // GenWaker - every notify wakes, so this will be slower
    let gw = GenWaker::new();
    unsafe { gw.register(&waker) };
    let start = Instant::now();
    for _ in 0..NUM_BURSTS {
        for _ in 0..BURST_SIZE {
            gw.notify();
        }
        unsafe { gw.register(&waker) };
    }
    let gen_ns = start.elapsed().as_nanos() as f64 / (NUM_BURSTS * (BURST_SIZE + 1)) as f64;

    BenchResult {
        name: "burst (100 notify, 1 register)",
        atomic_ns,
        diatomic_ns,
        simple_ns,
        gen_ns,
    }
}

fn bench_contention(num_notifiers: usize) -> BenchResult {
    let name = Box::leak(format!("contention: {} notifiers", num_notifiers).into_boxed_str());
    let waker = counting_waker();

    // AtomicWaker
    let atomic = Arc::new(AtomicWaker::new());
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for i in 0..num_notifiers {
        let atomic = Arc::clone(&atomic);
        let stop = Arc::clone(&stop);
        let ops = Arc::clone(&ops);
        handles.push(thread::spawn(move || {
            set_cpu_affinity(i + 1);
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                atomic.wake();
                count += 1;
            }
            ops.fetch_add(count, Ordering::Relaxed);
        }));
    }

    set_cpu_affinity(0);
    let start = Instant::now();
    let mut reg_count = 0u64;
    while start.elapsed() < Duration::from_secs(CONTENTION_DURATION_SECS) {
        atomic.register(&waker);
        reg_count += 1;
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let atomic_ns = Duration::from_secs(CONTENTION_DURATION_SECS).as_nanos() as f64
        / (ops.load(Ordering::Relaxed) + reg_count) as f64;

    // DiatomicWaker
    let diatomic = Arc::new(DiatomicWaker::new());
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for i in 0..num_notifiers {
        let diatomic = Arc::clone(&diatomic);
        let stop = Arc::clone(&stop);
        let ops = Arc::clone(&ops);
        handles.push(thread::spawn(move || {
            set_cpu_affinity(i + 1);
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                diatomic.notify();
                count += 1;
            }
            ops.fetch_add(count, Ordering::Relaxed);
        }));
    }

    set_cpu_affinity(0);
    let start = Instant::now();
    let mut reg_count = 0u64;
    while start.elapsed() < Duration::from_secs(CONTENTION_DURATION_SECS) {
        unsafe { diatomic.register(&waker) };
        reg_count += 1;
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let diatomic_ns = Duration::from_secs(CONTENTION_DURATION_SECS).as_nanos() as f64
        / (ops.load(Ordering::Relaxed) + reg_count) as f64;

    // SimpleWaker
    let simple = Arc::new(SimpleWaker::new());
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for i in 0..num_notifiers {
        let simple = Arc::clone(&simple);
        let stop = Arc::clone(&stop);
        let ops = Arc::clone(&ops);
        handles.push(thread::spawn(move || {
            set_cpu_affinity(i + 1);
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                simple.notify();
                count += 1;
            }
            ops.fetch_add(count, Ordering::Relaxed);
        }));
    }

    set_cpu_affinity(0);
    let start = Instant::now();
    let mut reg_count = 0u64;
    while start.elapsed() < Duration::from_secs(CONTENTION_DURATION_SECS) {
        unsafe { simple.register(&waker) };
        reg_count += 1;
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let simple_ns = Duration::from_secs(CONTENTION_DURATION_SECS).as_nanos() as f64
        / (ops.load(Ordering::Relaxed) + reg_count) as f64;

    // GenWaker
    let gw = Arc::new(GenWaker::new());
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for i in 0..num_notifiers {
        let gw = Arc::clone(&gw);
        let stop = Arc::clone(&stop);
        let ops = Arc::clone(&ops);
        handles.push(thread::spawn(move || {
            set_cpu_affinity(i + 1);
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                gw.notify();
                count += 1;
            }
            ops.fetch_add(count, Ordering::Relaxed);
        }));
    }

    set_cpu_affinity(0);
    let start = Instant::now();
    let mut reg_count = 0u64;
    while start.elapsed() < Duration::from_secs(CONTENTION_DURATION_SECS) {
        unsafe { gw.register(&waker) };
        reg_count += 1;
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let gen_ns = Duration::from_secs(CONTENTION_DURATION_SECS).as_nanos() as f64
        / (ops.load(Ordering::Relaxed) + reg_count) as f64;

    BenchResult {
        name,
        atomic_ns,
        diatomic_ns,
        simple_ns,
        gen_ns,
    }
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    println!("=== Waker Benchmark (all times in nanoseconds) ===\n");
    println!(
        "Configuration: {} iterations, {} sec contention\n",
        BENCH_ITERATIONS, CONTENTION_DURATION_SECS
    );

    println!("--- Single-Threaded ---");
    bench_register_notify().print();
    bench_register_same_waker().print();
    bench_register_different_wakers().print();
    bench_notify_no_waker().print();
    bench_burst_notify().print();

    println!("\n--- Multi-Threaded ---");
    bench_contention(1).print();
    bench_contention(2).print();
    bench_contention(4).print();
    bench_contention(8).print();

    println!("\n=== Summary ===");
    println!("  Atomic:   safe, moderate perf");
    println!("  Diatomic: unsafe register, good perf, supports wait_until");
    println!("  Simple:   unsafe register, best notify perf, single CAS");
    println!("  Gen:      unsafe register, best register perf (just load!), every notify wakes");
}

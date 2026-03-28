//! Benchmark: single atomic counter vs striped counter.
//!
//! Measures increment, decrement, and count (sum) throughput with
//! 1 to 16 threads to quantify the benefit of cache-line striping.
//!
//! Run: cargo run --release -p bisque-alloc --example counter-bench

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Cache line size for padding.
#[repr(align(64))]
struct Padded<T>(T);

// ─────────────────────────────────────────────────────────────────────────────
// Single counter
// ─────────────────────────────────────────────────────────────────────────────

struct SingleCounter {
    value: Padded<AtomicU64>,
}

impl SingleCounter {
    fn new() -> Self {
        Self {
            value: Padded(AtomicU64::new(0)),
        }
    }
    #[inline]
    fn increment(&self) {
        self.value.0.fetch_add(1, Ordering::AcqRel);
    }
    #[inline]
    fn decrement(&self) {
        self.value.0.fetch_sub(1, Ordering::AcqRel);
    }
    #[inline]
    fn count(&self) -> u64 {
        self.value.0.load(Ordering::Acquire)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Striped counter (2 stripes)
// ─────────────────────────────────────────────────────────────────────────────

struct StripedCounter2 {
    stripes: [Padded<AtomicU64>; 2],
}

impl StripedCounter2 {
    fn new() -> Self {
        Self {
            stripes: [Padded(AtomicU64::new(0)), Padded(AtomicU64::new(0))],
        }
    }
    #[inline]
    fn increment(&self, thread_id: usize) {
        self.stripes[thread_id % 2].0.fetch_add(1, Ordering::AcqRel);
    }
    #[inline]
    fn decrement(&self, thread_id: usize) {
        self.stripes[thread_id % 2].0.fetch_sub(1, Ordering::AcqRel);
    }
    #[inline]
    fn count(&self) -> u64 {
        self.stripes
            .iter()
            .map(|s| s.0.load(Ordering::Acquire))
            .sum()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Striped counter (4 stripes)
// ─────────────────────────────────────────────────────────────────────────────

struct StripedCounter4 {
    stripes: [Padded<AtomicU64>; 4],
}

impl StripedCounter4 {
    fn new() -> Self {
        Self {
            stripes: std::array::from_fn(|_| Padded(AtomicU64::new(0))),
        }
    }
    #[inline]
    fn increment(&self, thread_id: usize) {
        self.stripes[thread_id % 4].0.fetch_add(1, Ordering::AcqRel);
    }
    #[inline]
    fn decrement(&self, thread_id: usize) {
        self.stripes[thread_id % 4].0.fetch_sub(1, Ordering::AcqRel);
    }
    #[inline]
    fn count(&self) -> u64 {
        self.stripes
            .iter()
            .map(|s| s.0.load(Ordering::Acquire))
            .sum()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Striped counter (8 stripes)
// ─────────────────────────────────────────────────────────────────────────────

struct StripedCounter8 {
    stripes: [Padded<AtomicU64>; 8],
}

impl StripedCounter8 {
    fn new() -> Self {
        Self {
            stripes: std::array::from_fn(|_| Padded(AtomicU64::new(0))),
        }
    }
    #[inline]
    fn increment(&self, thread_id: usize) {
        self.stripes[thread_id % 8].0.fetch_add(1, Ordering::AcqRel);
    }
    #[inline]
    fn decrement(&self, thread_id: usize) {
        self.stripes[thread_id % 8].0.fetch_sub(1, Ordering::AcqRel);
    }
    #[inline]
    fn count(&self) -> u64 {
        self.stripes
            .iter()
            .map(|s| s.0.load(Ordering::Acquire))
            .sum()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark harness
// ─────────────────────────────────────────────────────────────────────────────

const OPS_PER_THREAD: u64 = 10_000_000;
const BENCH_THREADS: &[usize] = &[1, 2, 4, 8, 16];

fn bench_single_increment(num_threads: usize) -> Duration {
    let counter = Arc::new(SingleCounter::new());
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let c = &counter;
            s.spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    c.increment();
                }
            });
        }
    });
    black_box(counter.count());
    start.elapsed()
}

fn bench_single_inc_dec(num_threads: usize) -> Duration {
    let counter = Arc::new(SingleCounter::new());
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let c = &counter;
            s.spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    c.increment();
                    c.decrement();
                }
            });
        }
    });
    black_box(counter.count());
    start.elapsed()
}

fn bench_single_count(num_threads: usize) -> Duration {
    let counter = Arc::new(SingleCounter::new());
    counter.value.0.store(42, Ordering::Relaxed);
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let c = &counter;
            s.spawn(move || {
                let mut sum = 0u64;
                for _ in 0..OPS_PER_THREAD {
                    sum += c.count();
                }
                black_box(sum);
            });
        }
    });
    start.elapsed()
}

macro_rules! bench_striped {
    ($name_inc:ident, $name_inc_dec:ident, $name_count:ident, $ty:ty) => {
        fn $name_inc(num_threads: usize) -> Duration {
            let counter = Arc::new(<$ty>::new());
            let start = Instant::now();
            std::thread::scope(|s| {
                for tid in 0..num_threads {
                    let c = &counter;
                    s.spawn(move || {
                        for _ in 0..OPS_PER_THREAD {
                            c.increment(tid);
                        }
                    });
                }
            });
            black_box(counter.count());
            start.elapsed()
        }

        fn $name_inc_dec(num_threads: usize) -> Duration {
            let counter = Arc::new(<$ty>::new());
            let start = Instant::now();
            std::thread::scope(|s| {
                for tid in 0..num_threads {
                    let c = &counter;
                    s.spawn(move || {
                        for _ in 0..OPS_PER_THREAD {
                            c.increment(tid);
                            c.decrement(tid);
                        }
                    });
                }
            });
            black_box(counter.count());
            start.elapsed()
        }

        fn $name_count(num_threads: usize) -> Duration {
            let counter = Arc::new(<$ty>::new());
            let start = Instant::now();
            std::thread::scope(|s| {
                for _ in 0..num_threads {
                    let c = &counter;
                    s.spawn(move || {
                        let mut sum = 0u64;
                        for _ in 0..OPS_PER_THREAD {
                            sum += c.count();
                        }
                        black_box(sum);
                    });
                }
            });
            start.elapsed()
        }
    };
}

bench_striped!(
    bench_s2_inc,
    bench_s2_inc_dec,
    bench_s2_count,
    StripedCounter2
);
bench_striped!(
    bench_s4_inc,
    bench_s4_inc_dec,
    bench_s4_count,
    StripedCounter4
);
bench_striped!(
    bench_s8_inc,
    bench_s8_inc_dec,
    bench_s8_count,
    StripedCounter8
);

fn mops(threads: usize, elapsed: Duration) -> f64 {
    let total_ops = OPS_PER_THREAD as f64 * threads as f64;
    total_ops / elapsed.as_secs_f64() / 1e6
}

fn main() {
    println!("=== Counter Benchmark: Single vs Striped ===");
    println!(
        "  {} Mops per thread, varying thread count\n",
        OPS_PER_THREAD / 1_000_000
    );

    // ─── Increment ─────────────────────────────────────────────────────
    println!("─── Increment (fetch_add) ───");
    println!(
        "  {:>8}  {:>12}  {:>12}  {:>12}  {:>12}",
        "Threads", "Single", "Striped×2", "Striped×4", "Striped×8"
    );
    for &t in BENCH_THREADS {
        let s = bench_single_increment(t);
        let s2 = bench_s2_inc(t);
        let s4 = bench_s4_inc(t);
        let s8 = bench_s8_inc(t);
        println!(
            "  {:>8}  {:>10.2} M  {:>10.2} M  {:>10.2} M  {:>10.2} M",
            t,
            mops(t, s),
            mops(t, s2),
            mops(t, s4),
            mops(t, s8)
        );
    }
    println!();

    // ─── Increment + Decrement (pin/unpin simulation) ──────────────────
    println!("─── Inc+Dec cycle (pin/unpin simulation) ───");
    println!(
        "  {:>8}  {:>12}  {:>12}  {:>12}  {:>12}",
        "Threads", "Single", "Striped×2", "Striped×4", "Striped×8"
    );
    for &t in BENCH_THREADS {
        let s = bench_single_inc_dec(t);
        let s2 = bench_s2_inc_dec(t);
        let s4 = bench_s4_inc_dec(t);
        let s8 = bench_s8_inc_dec(t);
        println!(
            "  {:>8}  {:>10.2} M  {:>10.2} M  {:>10.2} M  {:>10.2} M",
            t,
            mops(t, s),
            mops(t, s2),
            mops(t, s4),
            mops(t, s8)
        );
    }
    println!();

    // ─── Count (sum all stripes — read only) ───────────────────────────
    println!("─── Count / read (sum stripes) ───");
    println!(
        "  {:>8}  {:>12}  {:>12}  {:>12}  {:>12}",
        "Threads", "Single", "Striped×2", "Striped×4", "Striped×8"
    );
    for &t in BENCH_THREADS {
        let s = bench_single_count(t);
        let s2 = bench_s2_count(t);
        let s4 = bench_s4_count(t);
        let s8 = bench_s8_count(t);
        println!(
            "  {:>8}  {:>10.2} M  {:>10.2} M  {:>10.2} M  {:>10.2} M",
            t,
            mops(t, s),
            mops(t, s2),
            mops(t, s4),
            mops(t, s8)
        );
    }

    println!("\n=== Done ===");
}

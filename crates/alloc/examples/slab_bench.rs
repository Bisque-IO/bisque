//! Slab allocator benchmark.
//!
//! Measures throughput (Mops/s) for FixedSlab and SegmentedSlab across:
//!   - Sequential alloc, alloc+free cycle, random get
//!   - Concurrent alloc (all hold), alloc/free cycle, random get
//!
//! Run: cargo run --release -p bisque-alloc --example slab-bench

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use bisque_alloc::MiMalloc;
use bisque_alloc::slab::{FixedSlab, SegmentedSlab};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const THREADS: &[usize] = &[1, 2, 4, 8, 16];
const OPS_PER_THREAD: u64 = 500_000;

// Fixed sizes for FixedSlab benchmarks (must be const).
const FIXED_CAP: usize = 65536;
const FIXED_ALLOC_HOLD: usize = 32768;
const FIXED_GET_N: usize = 16384;

// Segment size for SegmentedSlab.
const SEG_CAP: usize = 1024;

fn mops(ops: u64, elapsed: Duration) -> f64 {
    ops as f64 / elapsed.as_secs_f64() / 1e6
}

fn fmt_mops(ops: u64, elapsed: Duration) -> String {
    format!("{:.2} Mops/s", mops(ops, elapsed))
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

// ─── Sequential ──────────────────────────────────────────────────────────

fn seq_alloc_fixed(n: usize) -> Duration {
    let slab = FixedSlab::<AtomicU64, FIXED_CAP>::new();
    let start = Instant::now();
    for _ in 0..n {
        black_box(slab.alloc().unwrap());
    }
    start.elapsed()
}

fn seq_alloc_segmented(n: usize) -> Duration {
    let slab = SegmentedSlab::<AtomicU64>::new(SEG_CAP);
    let start = Instant::now();
    for _ in 0..n {
        black_box(slab.alloc().unwrap());
    }
    start.elapsed()
}

fn seq_alloc_free_fixed(iters: usize) -> Duration {
    let slab = FixedSlab::<AtomicU64, FIXED_CAP>::new();
    let id = slab.alloc().unwrap();
    slab.free(id);
    let start = Instant::now();
    for _ in 0..iters {
        let id = slab.alloc().unwrap();
        black_box(id);
        slab.free(id);
    }
    start.elapsed()
}

fn seq_alloc_free_segmented(iters: usize) -> Duration {
    let slab = SegmentedSlab::<AtomicU64>::new(SEG_CAP);
    let id = slab.alloc().unwrap();
    slab.free(id);
    let start = Instant::now();
    for _ in 0..iters {
        let id = slab.alloc().unwrap();
        black_box(id);
        slab.free(id);
    }
    start.elapsed()
}

fn seq_get_fixed(n: usize, iters: usize) -> Duration {
    let slab = FixedSlab::<AtomicU64, FIXED_CAP>::new();
    for _ in 0..n {
        let id = slab.alloc().unwrap();
        slab.get(id).store(id as u64, Ordering::Relaxed);
    }
    let mut rng = Rng::new(0xCAFE);
    let start = Instant::now();
    for _ in 0..iters {
        let id = (rng.next() as usize) % n;
        black_box(slab.get(id).load(Ordering::Relaxed));
    }
    start.elapsed()
}

fn seq_get_segmented(n: usize, iters: usize) -> Duration {
    let slab = SegmentedSlab::<AtomicU64>::new(SEG_CAP);
    for _ in 0..n {
        let id = slab.alloc().unwrap();
        slab.get(id).store(id as u64, Ordering::Relaxed);
    }
    let mut rng = Rng::new(0xCAFE);
    let start = Instant::now();
    for _ in 0..iters {
        let id = (rng.next() as usize) % n;
        black_box(slab.get(id).load(Ordering::Relaxed));
    }
    start.elapsed()
}

// ─── Concurrent ──────────────────────────────────────────────────────────

fn conc_alloc_fixed(num_threads: usize) -> Duration {
    let ops = FIXED_ALLOC_HOLD / num_threads;
    let slab = Arc::new(FixedSlab::<AtomicU64, FIXED_ALLOC_HOLD>::new());
    let barrier = Arc::new(Barrier::new(num_threads));
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let slab = &slab;
            let barrier = &barrier;
            s.spawn(move || {
                barrier.wait();
                for _ in 0..ops {
                    black_box(slab.alloc().unwrap());
                }
            });
        }
    });
    start.elapsed()
}

fn conc_alloc_segmented(num_threads: usize) -> Duration {
    let ops = FIXED_ALLOC_HOLD / num_threads;
    let slab = Arc::new(SegmentedSlab::<AtomicU64>::new(SEG_CAP));
    let barrier = Arc::new(Barrier::new(num_threads));
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let slab = &slab;
            let barrier = &barrier;
            s.spawn(move || {
                barrier.wait();
                for _ in 0..ops {
                    black_box(slab.alloc().unwrap());
                }
            });
        }
    });
    start.elapsed()
}

fn conc_alloc_free_fixed(num_threads: usize) -> Duration {
    let slab = Arc::new(FixedSlab::<AtomicU64, 1024>::new());
    let barrier = Arc::new(Barrier::new(num_threads));
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let slab = &slab;
            let barrier = &barrier;
            s.spawn(move || {
                barrier.wait();
                for _ in 0..OPS_PER_THREAD {
                    let id = slab.alloc().unwrap();
                    slab.get(id).store(id as u64, Ordering::Relaxed);
                    black_box(slab.get(id).load(Ordering::Relaxed));
                    slab.free(id);
                }
            });
        }
    });
    start.elapsed()
}

fn conc_alloc_free_segmented(num_threads: usize) -> Duration {
    let slab = Arc::new(SegmentedSlab::<AtomicU64>::new(SEG_CAP));
    let barrier = Arc::new(Barrier::new(num_threads));
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let slab = &slab;
            let barrier = &barrier;
            s.spawn(move || {
                barrier.wait();
                for _ in 0..OPS_PER_THREAD {
                    let id = slab.alloc().unwrap();
                    slab.get(id).store(id as u64, Ordering::Relaxed);
                    black_box(slab.get(id).load(Ordering::Relaxed));
                    slab.free(id);
                }
            });
        }
    });
    start.elapsed()
}

fn conc_get_fixed(num_threads: usize) -> Duration {
    let slab = Arc::new(FixedSlab::<AtomicU64, FIXED_GET_N>::new());
    for _ in 0..FIXED_GET_N {
        let id = slab.alloc().unwrap();
        slab.get(id).store(id as u64, Ordering::Relaxed);
    }
    let barrier = Arc::new(Barrier::new(num_threads));
    let start = Instant::now();
    std::thread::scope(|s| {
        for t in 0..num_threads {
            let slab = &slab;
            let barrier = &barrier;
            s.spawn(move || {
                barrier.wait();
                let mut rng = Rng::new(0xBEEF + t as u64);
                for _ in 0..OPS_PER_THREAD {
                    let id = (rng.next() as usize) % FIXED_GET_N;
                    black_box(slab.get(id).load(Ordering::Relaxed));
                }
            });
        }
    });
    start.elapsed()
}

fn conc_get_segmented(num_threads: usize) -> Duration {
    let slab = Arc::new(SegmentedSlab::<AtomicU64>::new(SEG_CAP));
    for _ in 0..FIXED_GET_N {
        let id = slab.alloc().unwrap();
        slab.get(id).store(id as u64, Ordering::Relaxed);
    }
    let barrier = Arc::new(Barrier::new(num_threads));
    let start = Instant::now();
    std::thread::scope(|s| {
        for t in 0..num_threads {
            let slab = &slab;
            let barrier = &barrier;
            s.spawn(move || {
                barrier.wait();
                let mut rng = Rng::new(0xBEEF + t as u64);
                for _ in 0..OPS_PER_THREAD {
                    let id = (rng.next() as usize) % FIXED_GET_N;
                    black_box(slab.get(id).load(Ordering::Relaxed));
                }
            });
        }
    });
    start.elapsed()
}

// ─── Main ────────────────────────────────────────────────────────────────

fn main() {
    println!("=== Slab Benchmark: FixedSlab vs SegmentedSlab ===\n");

    let n = 50_000;
    let iters = 1_000_000;

    println!("─── Sequential alloc ({n} entries) ───");
    let d1 = seq_alloc_fixed(n);
    let d2 = seq_alloc_segmented(n);
    println!("  Fixed      {}", fmt_mops(n as u64, d1));
    println!("  Segmented  {}", fmt_mops(n as u64, d2));
    println!();

    println!("─── Sequential alloc+free cycle ({iters} iters) ───");
    let d1 = seq_alloc_free_fixed(iters);
    let d2 = seq_alloc_free_segmented(iters);
    println!("  Fixed      {}", fmt_mops(iters as u64, d1));
    println!("  Segmented  {}", fmt_mops(iters as u64, d2));
    println!();

    println!("─── Sequential random get ({iters} lookups, {n} entries) ───");
    let d1 = seq_get_fixed(n, iters);
    let d2 = seq_get_segmented(n, iters);
    println!("  Fixed      {}", fmt_mops(iters as u64, d1));
    println!("  Segmented  {}", fmt_mops(iters as u64, d2));
    println!();

    println!("─── Concurrent alloc, all hold ({FIXED_ALLOC_HOLD} total) ───");
    println!("  {:>8}  {:>16}  {:>16}", "Threads", "Fixed", "Segmented");
    for &t in THREADS {
        let total = FIXED_ALLOC_HOLD / t * t;
        let d1 = conc_alloc_fixed(t);
        let d2 = conc_alloc_segmented(t);
        println!(
            "  {:>8}  {:>14.2} M  {:>14.2} M",
            t,
            mops(total as u64, d1),
            mops(total as u64, d2)
        );
    }
    println!();

    println!("─── Concurrent alloc+free cycle ({OPS_PER_THREAD} ops/thread) ───");
    println!("  {:>8}  {:>16}  {:>16}", "Threads", "Fixed", "Segmented");
    for &t in THREADS {
        let d1 = conc_alloc_free_fixed(t);
        let d2 = conc_alloc_free_segmented(t);
        let total = OPS_PER_THREAD * t as u64;
        println!(
            "  {:>8}  {:>14.2} M  {:>14.2} M",
            t,
            mops(total, d1),
            mops(total, d2)
        );
    }
    println!();

    println!("─── Concurrent random get ({OPS_PER_THREAD} ops/thread, {FIXED_GET_N} entries) ───");
    println!("  {:>8}  {:>16}  {:>16}", "Threads", "Fixed", "Segmented");
    for &t in THREADS {
        let d1 = conc_get_fixed(t);
        let d2 = conc_get_segmented(t);
        let total = OPS_PER_THREAD * t as u64;
        println!(
            "  {:>8}  {:>14.2} M  {:>14.2} M",
            t,
            mops(total, d1),
            mops(total, d2)
        );
    }

    println!("\n=== Done ===");
}

//! Benchmark: Heap Epoch vs Counted Epoch vs Seize vs Crossbeam-Epoch.
//!
//! Run: cargo run --release -p bisque-alloc --example epoch-bench

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bisque_alloc::epoch::counted::{
    Collector as CCollector, Epoch as CEpoch, DeallocFn as CDeallocFn,
};
use bisque_alloc::epoch::heap::{
    Collector as HCollector, Epoch as HEpoch, DeallocFn as HDeallocFn,
};
use bisque_alloc::epoch::{Collector as SeizeCollector, Guard};
use bisque_alloc::{Heap, HeapMaster, MiMalloc};

use crossbeam_epoch as ce;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const OPS_PER_THREAD: u64 = 5_000_000;
const THREADS: &[usize] = &[1, 2, 4, 8, 16];

fn mops(threads: usize, elapsed: Duration) -> f64 {
    (OPS_PER_THREAD as f64 * threads as f64) / elapsed.as_secs_f64() / 1e6
}

unsafe fn noop_dealloc(_: &Heap, _: usize) {}

// ─── Heap Epoch (watermark) ──────────────────────────────────────────────

fn heap_pin_unpin(n: usize, e: &Arc<HEpoch>) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..n {
            let e = e.clone();
            s.spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    let g = e.pin();
                    black_box(g.epoch());
                    drop(g);
                }
            });
        }
    });
    start.elapsed()
}

fn heap_pin_only(n: usize, e: &Arc<HEpoch>) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..n {
            let e = e.clone();
            s.spawn(move || {
                let g = e.pin();
                for _ in 0..OPS_PER_THREAD {
                    black_box(g.epoch());
                }
            });
        }
    });
    start.elapsed()
}

fn heap_mixed(n: usize, e: &Arc<HEpoch>) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..n {
            let e = e.clone();
            s.spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    let g = e.pin();
                    black_box(g.epoch());
                    drop(g);
                }
            });
        }
        let e = e.clone();
        s.spawn(move || {
            for i in 0..OPS_PER_THREAD {
                if i % 1000 == 0 { e.advance(); }
                std::hint::spin_loop();
            }
        });
    });
    start.elapsed()
}

// ─── Counted Epoch (per-epoch counters) ──────────────────────────────────

fn counted_pin_unpin(n: usize, e: &Arc<CEpoch>) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..n {
            let e = e.clone();
            s.spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    let g = e.pin();
                    black_box(g.epoch());
                    drop(g);
                }
            });
        }
    });
    start.elapsed()
}

fn counted_pin_only(n: usize, e: &Arc<CEpoch>) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..n {
            let e = e.clone();
            s.spawn(move || {
                let g = e.pin();
                for _ in 0..OPS_PER_THREAD {
                    black_box(g.epoch());
                }
            });
        }
    });
    start.elapsed()
}

fn counted_mixed(n: usize, e: &Arc<CEpoch>) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..n {
            let e = e.clone();
            s.spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    let g = e.pin();
                    black_box(g.epoch());
                    drop(g);
                }
            });
        }
        let e = e.clone();
        s.spawn(move || {
            for i in 0..OPS_PER_THREAD {
                if i % 1000 == 0 { e.advance(); }
                std::hint::spin_loop();
            }
        });
    });
    start.elapsed()
}

// ─── Seize Collector ─────────────────────────────────────────────────────

fn seize_pin_unpin(n: usize, c: &Arc<SeizeCollector>) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..n {
            let c = c.clone();
            s.spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    let g = c.enter();
                    black_box(g.thread_id());
                    drop(g);
                }
            });
        }
    });
    start.elapsed()
}

fn seize_pin_only(n: usize, c: &Arc<SeizeCollector>) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..n {
            let c = c.clone();
            s.spawn(move || {
                let g = c.enter();
                for _ in 0..OPS_PER_THREAD {
                    black_box(g.thread_id());
                }
            });
        }
    });
    start.elapsed()
}

fn seize_mixed(n: usize, c: &Arc<SeizeCollector>) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..n {
            let c = c.clone();
            s.spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    let g = c.enter();
                    black_box(g.thread_id());
                    drop(g);
                }
            });
        }
        // Writer: enter/exit guard to force epoch progression.
        let c = c.clone();
        s.spawn(move || {
            for i in 0..OPS_PER_THREAD {
                if i % 1000 == 0 {
                    let g = c.enter();
                    black_box(g.thread_id());
                    drop(g);
                }
                std::hint::spin_loop();
            }
        });
    });
    start.elapsed()
}

// ─── Multi-writer mixed ──────────────────────────────────────────────────

fn heap_multi_writer(num_readers: usize, num_writers: usize, e: &Arc<HEpoch>) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..num_readers {
            let e = e.clone();
            s.spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    let g = e.pin();
                    black_box(g.epoch());
                    drop(g);
                }
            });
        }
        for _ in 0..num_writers {
            let e = e.clone();
            s.spawn(move || {
                for i in 0..OPS_PER_THREAD {
                    if i % 1000 == 0 { e.advance(); }
                    std::hint::spin_loop();
                }
            });
        }
    });
    start.elapsed()
}

fn counted_multi_writer(num_readers: usize, num_writers: usize, e: &Arc<CEpoch>) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..num_readers {
            let e = e.clone();
            s.spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    let g = e.pin();
                    black_box(g.epoch());
                    drop(g);
                }
            });
        }
        for _ in 0..num_writers {
            let e = e.clone();
            s.spawn(move || {
                for i in 0..OPS_PER_THREAD {
                    if i % 1000 == 0 { e.advance(); }
                    std::hint::spin_loop();
                }
            });
        }
    });
    start.elapsed()
}

fn seize_multi_writer(num_readers: usize, num_writers: usize, c: &Arc<SeizeCollector>) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..num_readers {
            let c = c.clone();
            s.spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    let g = c.enter();
                    black_box(g.thread_id());
                    drop(g);
                }
            });
        }
        for _ in 0..num_writers {
            let c = c.clone();
            s.spawn(move || {
                for i in 0..OPS_PER_THREAD {
                    if i % 1000 == 0 {
                        let g = c.enter();
                        black_box(g.thread_id());
                        drop(g);
                    }
                    std::hint::spin_loop();
                }
            });
        }
    });
    start.elapsed()
}

// ─── Crossbeam-Epoch ─────────────────────────────────────────────────────

fn crossbeam_pin_unpin(n: usize) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..n {
            s.spawn(|| {
                for _ in 0..OPS_PER_THREAD {
                    let g = ce::pin();
                    black_box(&g);
                    drop(g);
                }
            });
        }
    });
    start.elapsed()
}

fn crossbeam_pin_only(n: usize) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..n {
            s.spawn(|| {
                let g = ce::pin();
                for _ in 0..OPS_PER_THREAD {
                    black_box(&g);
                }
            });
        }
    });
    start.elapsed()
}

fn crossbeam_mixed(n: usize) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..n {
            s.spawn(|| {
                for _ in 0..OPS_PER_THREAD {
                    let g = ce::pin();
                    black_box(&g);
                    drop(g);
                }
            });
        }
        s.spawn(|| {
            for i in 0..OPS_PER_THREAD {
                if i % 1000 == 0 {
                    let g = ce::pin();
                    black_box(&g);
                    drop(g);
                }
                std::hint::spin_loop();
            }
        });
    });
    start.elapsed()
}

fn crossbeam_multi_writer(num_readers: usize, num_writers: usize) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..num_readers {
            s.spawn(|| {
                for _ in 0..OPS_PER_THREAD {
                    let g = ce::pin();
                    black_box(&g);
                    drop(g);
                }
            });
        }
        for _ in 0..num_writers {
            s.spawn(|| {
                for i in 0..OPS_PER_THREAD {
                    if i % 1000 == 0 {
                        let g = ce::pin();
                        black_box(&g);
                        drop(g);
                    }
                    std::hint::spin_loop();
                }
            });
        }
    });
    start.elapsed()
}

fn mops_total(readers: usize, _writers: usize, elapsed: Duration) -> f64 {
    // Report reader throughput only (writers just spin).
    (OPS_PER_THREAD as f64 * readers as f64) / elapsed.as_secs_f64() / 1e6
}

// ─── Main ────────────────────────────────────────────────────────────────

fn main() {
    let master = HeapMaster::new(256 * 1024 * 1024).unwrap();
    let heap = master.heap();

    let hc = HCollector::new();
    let he = Arc::new(HEpoch::new(&hc, &heap, noop_dealloc as HDeallocFn));

    let cc = CCollector::new();
    let ce = Arc::new(CEpoch::new(&cc, &heap, noop_dealloc as CDeallocFn));

    let sc = Arc::new(SeizeCollector::new());

    let hdr = |label: &str| {
        println!("  {:>8}  {:>14}  {:>14}  {:>14}  {:>14}", label, "Heap", "Counted", "Seize", "Crossbeam");
    };

    println!("=== Epoch Benchmark: Heap vs Counted vs Seize vs Crossbeam ===");
    println!("  {} Mops per thread\n", OPS_PER_THREAD / 1_000_000);

    println!("─── Pin + Unpin cycle ───");
    hdr("Threads");
    for &t in THREADS {
        let h = heap_pin_unpin(t, &he);
        let c = counted_pin_unpin(t, &ce);
        let s = seize_pin_unpin(t, &sc);
        let x = crossbeam_pin_unpin(t);
        println!("  {:>8}  {:>12.2} M  {:>12.2} M  {:>12.2} M  {:>12.2} M",
            t, mops(t, h), mops(t, c), mops(t, s), mops(t, x));
    }
    println!();

    println!("─── Pin once, read many ───");
    hdr("Threads");
    for &t in THREADS {
        let h = heap_pin_only(t, &he);
        let c = counted_pin_only(t, &ce);
        let s = seize_pin_only(t, &sc);
        let x = crossbeam_pin_only(t);
        println!("  {:>8}  {:>12.2} M  {:>12.2} M  {:>12.2} M  {:>12.2} M",
            t, mops(t, h), mops(t, c), mops(t, s), mops(t, x));
    }
    println!();

    println!("─── Mixed: N readers + 1 writer ───");
    hdr("Readers");
    for &t in THREADS {
        let h = heap_mixed(t, &he);
        let c = counted_mixed(t, &ce);
        let s = seize_mixed(t, &sc);
        let x = crossbeam_mixed(t);
        println!("  {:>8}  {:>12.2} M  {:>12.2} M  {:>12.2} M  {:>12.2} M",
            t, mops(t, h), mops(t, c), mops(t, s), mops(t, x));
    }

    let writers = &[1, 2, 4, 8];
    let readers = 8;
    println!();
    println!("─── {readers} readers + N writers advancing ───");
    hdr("Writers");
    for &w in writers {
        let h = heap_multi_writer(readers, w, &he);
        let c = counted_multi_writer(readers, w, &ce);
        let s = seize_multi_writer(readers, w, &sc);
        let x = crossbeam_multi_writer(readers, w);
        println!("  {:>8}  {:>12.2} M  {:>12.2} M  {:>12.2} M  {:>12.2} M",
            w, mops_total(readers, w, h), mops_total(readers, w, c),
            mops_total(readers, w, s), mops_total(readers, w, x));
    }

    let readers = 16;
    println!();
    println!("─── {readers} readers + N writers advancing ───");
    hdr("Writers");
    for &w in writers {
        let h = heap_multi_writer(readers, w, &he);
        let c = counted_multi_writer(readers, w, &ce);
        let s = seize_multi_writer(readers, w, &sc);
        let x = crossbeam_multi_writer(readers, w);
        println!("  {:>8}  {:>12.2} M  {:>12.2} M  {:>12.2} M  {:>12.2} M",
            w, mops_total(readers, w, h), mops_total(readers, w, c),
            mops_total(readers, w, s), mops_total(readers, w, x));
    }

    println!("\n=== Done ===");
}

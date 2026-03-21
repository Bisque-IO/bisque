//! Comprehensive benchmark: mimalloc arena heap vs mimalloc global vs system allocator.
//!
//! Run with: cargo run --release -p bisque-alloc --example heap_bench

use std::alloc::{GlobalAlloc, Layout, System, alloc, dealloc};
use std::hint::black_box;
use std::time::{Duration, Instant};

use bisque_alloc::{Heap, HeapMaster, HeapVec, MiMalloc};
use bisque_mimalloc_sys as ffi;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const HEAP_SIZE: usize = 32 * 1024 * 1024; // 32 MiB arena per heap (minimum)
const DURATION: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct BenchResult {
    name: String,
    ops: u64,
    elapsed: Duration,
}

impl BenchResult {
    fn print(&self) {
        let ns = self.elapsed.as_nanos() as f64 / self.ops as f64;
        let ops_s = self.ops as f64 / self.elapsed.as_secs_f64();
        println!(
            "  {:<44} {:>12.1} ops/s   {:>8.1} ns/op",
            self.name, ops_s, ns
        );
    }
}

fn bench<F: FnMut()>(name: &str, dur: Duration, mut f: F) -> BenchResult {
    for _ in 0..10000 {
        f();
    }
    let t = Instant::now();
    let mut n = 0u64;
    while t.elapsed() < Duration::from_millis(10) {
        f();
        n += 1;
    }
    let rate = n as f64 / t.elapsed().as_nanos() as f64;
    let target = (rate * dur.as_nanos() as f64) as u64;
    let target = target.max(1000);
    let start = Instant::now();
    for _ in 0..target {
        f();
    }
    BenchResult {
        name: name.into(),
        ops: target,
        elapsed: start.elapsed(),
    }
}

fn bench_batched<F: FnMut()>(name: &str, dur: Duration, batch: u64, mut f: F) -> BenchResult {
    for _ in 0..10 {
        f();
    }
    let t = Instant::now();
    let mut n = 0u64;
    while t.elapsed() < Duration::from_millis(20) {
        f();
        n += 1;
    }
    let rate = n as f64 / t.elapsed().as_nanos() as f64;
    let target = (rate * dur.as_nanos() as f64) as u64;
    let target = target.max(100);
    let start = Instant::now();
    for _ in 0..target {
        f();
    }
    BenchResult {
        name: name.into(),
        ops: target * batch,
        elapsed: start.elapsed(),
    }
}

fn new_heap() -> HeapMaster {
    HeapMaster::new(HEAP_SIZE).unwrap()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_alloc_free(label: &str, size: usize) -> Vec<BenchResult> {
    let align = 8;
    let layout = Layout::from_size_align(size, align).unwrap();
    let heap = new_heap();
    let mut r = Vec::new();

    r.push(bench(&format!("heap {label}"), DURATION, || {
        let p = heap.alloc(size, align);
        black_box(p);
        unsafe { heap.dealloc(p) };
    }));
    r.push(bench(&format!("mimalloc global {label}"), DURATION, || {
        let p = unsafe { ffi::mi_malloc_aligned(layout.size(), layout.align()) };
        black_box(p);
        unsafe { ffi::mi_free(p as *mut core::ffi::c_void) };
    }));
    r.push(bench(&format!("system {label}"), DURATION, || {
        let p = unsafe { System.alloc(layout) };
        black_box(p);
        unsafe { System.dealloc(p, layout) };
    }));
    r
}

fn bench_mixed() -> Vec<BenchResult> {
    let sizes = [32, 64, 128, 256, 512, 1024, 4096, 16384, 65536];
    let batch = sizes.len() as u64;
    let heap = new_heap();
    let mut r = Vec::new();

    r.push(bench_batched("heap mixed", DURATION, batch, || {
        for &s in &sizes {
            let p = heap.alloc(s, 8);
            black_box(p);
            unsafe { heap.dealloc(p) };
        }
    }));
    r.push(bench_batched(
        "mimalloc global mixed",
        DURATION,
        batch,
        || {
            for &s in &sizes {
                let l = Layout::from_size_align(s, 8).unwrap();
                let p = unsafe { alloc(l) };
                black_box(p);
                unsafe { dealloc(p, l) };
            }
        },
    ));
    r.push(bench_batched("system mixed", DURATION, batch, || {
        for &s in &sizes {
            let l = Layout::from_size_align(s, 8).unwrap();
            let p = unsafe { System.alloc(l) };
            black_box(p);
            unsafe { System.dealloc(p, l) };
        }
    }));
    r
}

fn bench_churn() -> Vec<BenchResult> {
    let n = 1000u64;
    let size = 128;
    let align = 8;
    let layout = Layout::from_size_align(size, align).unwrap();
    let heap = new_heap();
    let mut r = Vec::new();

    r.push(bench_batched("heap churn", DURATION, n, || {
        let mut v = Vec::with_capacity(n as usize);
        for _ in 0..n {
            v.push(heap.alloc(size, align));
        }
        for p in v {
            unsafe { heap.dealloc(p) };
        }
    }));
    r.push(bench_batched("mimalloc global churn", DURATION, n, || {
        let mut v = Vec::with_capacity(n as usize);
        for _ in 0..n {
            v.push(unsafe { alloc(layout) });
        }
        for p in v {
            unsafe { dealloc(p, layout) };
        }
    }));
    r.push(bench_batched("system churn", DURATION, n, || {
        let mut v = Vec::with_capacity(n as usize);
        for _ in 0..n {
            v.push(unsafe { System.alloc(layout) });
        }
        for p in v {
            unsafe { System.dealloc(p, layout) };
        }
    }));
    r
}

fn bench_multithread() -> Vec<BenchResult> {
    let counts = [1, 2, 4, 8];
    let ops = 200_000u64;
    let size = 256;
    let align = 8;
    let layout = Layout::from_size_align(size, align).unwrap();
    let mut r = Vec::new();

    for &t in &counts {
        // Pre-create heaps outside the timed section
        let heaps: Vec<HeapMaster> = (0..t).map(|_| new_heap()).collect();
        let start = Instant::now();
        std::thread::scope(|s| {
            for h in &heaps {
                s.spawn(|| {
                    for _ in 0..ops {
                        let p = h.alloc(size, align);
                        black_box(p);
                        unsafe { h.dealloc(p) };
                    }
                });
            }
        });
        r.push(BenchResult {
            name: format!("heap {t}T per-thread"),
            ops: ops * t as u64,
            elapsed: start.elapsed(),
        });
        drop(heaps);

        let start = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..t {
                s.spawn(|| {
                    for _ in 0..ops {
                        let p = unsafe { alloc(layout) };
                        black_box(p);
                        unsafe { dealloc(p, layout) };
                    }
                });
            }
        });
        r.push(BenchResult {
            name: format!("mimalloc global {t}T"),
            ops: ops * t as u64,
            elapsed: start.elapsed(),
        });

        let start = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..t {
                s.spawn(|| {
                    for _ in 0..ops {
                        let p = unsafe { System.alloc(layout) };
                        black_box(p);
                        unsafe { System.dealloc(p, layout) };
                    }
                });
            }
        });
        r.push(BenchResult {
            name: format!("system {t}T"),
            ops: ops * t as u64,
            elapsed: start.elapsed(),
        });
    }
    r
}

fn bench_heap_vec_grow() -> Vec<BenchResult> {
    let heap = new_heap();
    let chunk = [0u8; 64];
    let iters = 100u64;
    let mut r = Vec::new();

    r.push(bench_batched(
        "HeapVec extend×100",
        DURATION,
        iters,
        || {
            let mut v = HeapVec::new(&heap);
            for _ in 0..iters {
                v.extend_from_slice(&chunk).unwrap();
            }
            black_box(&v[..]);
            drop(v);
        },
    ));
    r.push(bench_batched(
        "std Vec<u8> extend×100",
        DURATION,
        iters,
        || {
            let mut v = Vec::<u8>::new();
            for _ in 0..iters {
                v.extend_from_slice(&chunk);
            }
            black_box(&v[..]);
            drop(v);
        },
    ));
    r.push(bench_batched(
        "HeapVec pre-alloc extend×100",
        DURATION,
        iters,
        || {
            let mut v = HeapVec::with_capacity(6400, &heap).unwrap();
            for _ in 0..iters {
                v.extend_from_slice(&chunk).unwrap();
            }
            black_box(&v[..]);
            drop(v);
        },
    ));
    r.push(bench_batched(
        "std Vec<u8> pre-alloc extend×100",
        DURATION,
        iters,
        || {
            let mut v = Vec::<u8>::with_capacity(6400);
            for _ in 0..iters {
                v.extend_from_slice(&chunk);
            }
            black_box(&v[..]);
            drop(v);
        },
    ));
    r
}

fn bench_to_bytes() -> Vec<BenchResult> {
    let heap = new_heap();
    let mut r = Vec::new();

    r.push(bench("HeapVec→Bytes (1 KiB)", DURATION, || {
        let mut v = HeapVec::with_capacity(1024, &heap).unwrap();
        v.extend_from_slice(&[42u8; 1024]).unwrap();
        black_box(&v.into_bytes()[..]);
    }));
    r.push(bench("Vec<u8>→Bytes (1 KiB)", DURATION, || {
        let mut v = Vec::<u8>::with_capacity(1024);
        v.extend_from_slice(&[42u8; 1024]);
        black_box(&bytes::Bytes::from(v)[..]);
    }));
    r.push(bench("HeapVec→Bytes (64 KiB)", DURATION, || {
        let mut v = HeapVec::with_capacity(65536, &heap).unwrap();
        v.extend_from_slice(&[42u8; 65536]).unwrap();
        black_box(&v.into_bytes()[..]);
    }));
    r.push(bench("Vec<u8>→Bytes (64 KiB)", DURATION, || {
        let mut v = Vec::<u8>::with_capacity(65536);
        v.extend_from_slice(&[42u8; 65536]);
        black_box(&bytes::Bytes::from(v)[..]);
    }));
    r
}

fn bench_multi_heap() -> Vec<BenchResult> {
    let counts = [1, 4, 16];
    let ops_per = 50_000u64;
    let size = 256;
    let small_arena = 64 * 1024 * 1024; // 64 MiB per heap to avoid OOM with many heaps
    let mut r = Vec::new();

    for &n in &counts {
        let heaps: Vec<HeapMaster> = (0..n)
            .map(|_| HeapMaster::new(small_arena).unwrap())
            .collect();
        let start = Instant::now();
        for h in &heaps {
            for _ in 0..ops_per {
                let p = h.alloc(size, 8);
                black_box(p);
                unsafe { h.dealloc(p) };
            }
        }
        r.push(BenchResult {
            name: format!("{n} heaps sequential"),
            ops: ops_per * n as u64,
            elapsed: start.elapsed(),
        });

        let par_heaps: Vec<HeapMaster> = (0..n)
            .map(|_| HeapMaster::new(small_arena).unwrap())
            .collect();
        let start = Instant::now();
        std::thread::scope(|s| {
            for h in &par_heaps {
                s.spawn(|| {
                    for _ in 0..ops_per {
                        let p = h.alloc(size, 8);
                        black_box(p);
                        unsafe { h.dealloc(p) };
                    }
                });
            }
        });
        r.push(BenchResult {
            name: format!("{n} heaps parallel"),
            ops: ops_per * n as u64,
            elapsed: start.elapsed(),
        });
    }
    r
}

fn bench_realloc() -> Vec<BenchResult> {
    let heap = new_heap();
    let steps = 20u64;
    let mut r = Vec::new();

    r.push(bench_batched(
        "heap realloc ×20 grow",
        DURATION,
        steps,
        || {
            let mut size = 64usize;
            let mut ptr = heap.alloc(size, 8);
            for _ in 0..steps {
                let ns = size * 2;
                ptr = unsafe { heap.realloc(ptr, ns, 8) };
                size = ns;
            }
            unsafe { heap.dealloc(ptr) };
        },
    ));
    r.push(bench_batched(
        "mimalloc global realloc ×20 grow",
        DURATION,
        steps,
        || {
            let mut size = 64usize;
            let mut layout = Layout::from_size_align(size, 8).unwrap();
            let mut ptr = unsafe { alloc(layout) };
            for _ in 0..steps {
                let ns = size * 2;
                ptr = unsafe { std::alloc::realloc(ptr, layout, ns) };
                layout = Layout::from_size_align(ns, 8).unwrap();
                size = ns;
            }
            unsafe { dealloc(ptr, layout) };
        },
    ));
    r
}

fn bench_bulk_destroy() -> Vec<BenchResult> {
    let n = 10_000u64;
    let size = 256;
    let small_arena = 64 * 1024 * 1024;
    let mut r = Vec::new();

    // Individual frees: reuse one heap.
    let heap = HeapMaster::new(small_arena).unwrap();
    r.push(bench_batched("alloc 10K + free 10K", DURATION, n, || {
        let mut ptrs = Vec::with_capacity(n as usize);
        for _ in 0..n {
            ptrs.push(heap.alloc(size, 8));
        }
        for p in ptrs {
            unsafe { heap.dealloc(p) };
        }
    }));

    // Bulk destroy: manually timed (creating heaps in a loop exhausts
    // arena slots during calibration, so we run a fixed iteration count).
    let iters = 50u64;
    let start = Instant::now();
    for _ in 0..iters {
        let h = HeapMaster::new(small_arena).unwrap();
        for _ in 0..n {
            black_box(h.alloc(size, 8));
        }
        drop(h);
    }
    let elapsed = start.elapsed();
    r.push(BenchResult {
        name: "alloc 10K + heap_destroy".into(),
        ops: iters * n,
        elapsed,
    });

    r
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    println!();
    println!("=============================================================================");
    println!("  bisque-alloc benchmark: mimalloc arena heap vs global vs system");
    println!(
        "  Arena size: {} MiB. Each test ~500ms.",
        HEAP_SIZE / (1024 * 1024)
    );
    println!("=============================================================================");
    println!();

    let sections: Vec<(&str, Box<dyn Fn() -> Vec<BenchResult>>)> = vec![
        (
            "1. Small alloc+free (64 B)",
            Box::new(|| bench_alloc_free("64B", 64)),
        ),
        (
            "2. Medium alloc+free (4 KiB)",
            Box::new(|| bench_alloc_free("4K", 4096)),
        ),
        (
            "3. Large alloc+free (1 MiB)",
            Box::new(|| bench_alloc_free("1M", 1024 * 1024)),
        ),
        ("4. Mixed sizes", Box::new(bench_mixed)),
        ("5. Churn (1K alloc, 1K free)", Box::new(bench_churn)),
        ("6. Multi-threaded scaling", Box::new(bench_multithread)),
        (
            "7. HeapVec grow (extend×100)",
            Box::new(bench_heap_vec_grow),
        ),
        ("8. HeapVec → Bytes", Box::new(bench_to_bytes)),
        ("9. Multi-heap isolation", Box::new(bench_multi_heap)),
        ("10. Realloc (×20 doubling)", Box::new(bench_realloc)),
        ("11. Bulk destroy vs free", Box::new(bench_bulk_destroy)),
    ];

    for (title, f) in &sections {
        println!("--- {title} ---");
        for r in f() {
            r.print();
        }
        // Release mimalloc caches between sections to reduce RSS.
        unsafe {
            bisque_mimalloc_sys::mi_collect(true);
        }
        println!();
    }

    // Force immediate purge and reclaim abandoned segments.
    unsafe {
        bisque_mimalloc_sys::mi_option_set(
            bisque_mimalloc_sys::mi_option_e_mi_option_purge_delay as _,
            0,
        );
        // Collect multiple times — first reclaims abandoned segments, subsequent passes purge them.
        for _ in 0..5 {
            bisque_mimalloc_sys::mi_collect(true);
        }
    }
    let (rss, commit) = bisque_alloc::heap::process_memory_info();

    // Also read actual OS RSS from /proc
    let os_rss = std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse::<usize>().ok())
        })
        .unwrap_or(0);

    println!("--- Process Memory (after forced purge) ---");
    println!(
        "  mi_process_info RSS:  {:.2} MiB",
        rss as f64 / (1024.0 * 1024.0)
    );
    println!(
        "  mi_process_info committed: {:.2} MiB",
        commit as f64 / (1024.0 * 1024.0)
    );
    println!("  /proc/self VmRSS:     {:.2} MiB", os_rss as f64 / 1024.0);
    println!();
}

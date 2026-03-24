//! Comprehensive benchmark: bisque_alloc::bytes vs bytes crate.
//!
//! Run with: cargo run --release -p bisque-alloc --example bytes_bench

use std::hint::black_box;
use std::time::{Duration, Instant};

use bisque_alloc::{Heap, HeapMaster};

#[global_allocator]
static GLOBAL: bisque_alloc::MiMalloc = bisque_alloc::MiMalloc;

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
            "  {:<52} {:>12.1} ops/s  {:>8.1} ns/op",
            self.name, ops_s, ns
        );
    }
}

fn bench<F: FnMut()>(name: &str, dur: Duration, mut f: F) -> BenchResult {
    for _ in 0..5000 {
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

fn heap() -> HeapMaster {
    HeapMaster::new(128 * 1024 * 1024).unwrap()
}

// ---------------------------------------------------------------------------
// 1. Construction from slice (small: 16 bytes — inline territory)
// ---------------------------------------------------------------------------

fn bench_construct_small() -> Vec<BenchResult> {
    let data = [42u8; 16];
    let h = heap();
    let mut r = Vec::new();

    r.push(bench(
        "bisque Bytes::copy_from_slice (16B)",
        DURATION,
        || {
            let b = bisque_alloc::bytes::Bytes::copy_from_slice(&data, &h).unwrap();
            black_box(&b);
        },
    ));
    r.push(bench(
        "bytes::Bytes::copy_from_slice (16B)",
        DURATION,
        || {
            let b = bytes::Bytes::copy_from_slice(&data);
            black_box(&b);
        },
    ));
    r
}

// ---------------------------------------------------------------------------
// 2. Construction from slice (medium: 128 bytes — heap territory)
// ---------------------------------------------------------------------------

fn bench_construct_medium() -> Vec<BenchResult> {
    let data = [42u8; 128];
    let h = heap();
    let mut r = Vec::new();

    r.push(bench(
        "bisque Bytes::copy_from_slice (128B)",
        DURATION,
        || {
            let b = bisque_alloc::bytes::Bytes::copy_from_slice(&data, &h).unwrap();
            black_box(&b);
        },
    ));
    r.push(bench(
        "bytes::Bytes::copy_from_slice (128B)",
        DURATION,
        || {
            let b = bytes::Bytes::copy_from_slice(&data);
            black_box(&b);
        },
    ));
    r
}

// ---------------------------------------------------------------------------
// 3. Construction from slice (large: 4 KiB)
// ---------------------------------------------------------------------------

fn bench_construct_large() -> Vec<BenchResult> {
    let data = vec![42u8; 4096];
    let h = heap();
    let mut r = Vec::new();

    r.push(bench(
        "bisque Bytes::copy_from_slice (4K)",
        DURATION,
        || {
            let b = bisque_alloc::bytes::Bytes::copy_from_slice(&data, &h).unwrap();
            black_box(&b);
        },
    ));
    r.push(bench(
        "bytes::Bytes::copy_from_slice (4K)",
        DURATION,
        || {
            let b = bytes::Bytes::copy_from_slice(&data);
            black_box(&b);
        },
    ));
    r
}

// ---------------------------------------------------------------------------
// 4. Clone (inline — should be memcpy only, no atomics)
// ---------------------------------------------------------------------------

fn bench_clone_inline() -> Vec<BenchResult> {
    let h = heap();
    let bisque_b = bisque_alloc::bytes::Bytes::copy_from_slice(&[1u8; 16], &h).unwrap();
    let bytes_b = bytes::Bytes::copy_from_slice(&[1u8; 16]);
    let mut r = Vec::new();

    r.push(bench("bisque Bytes::clone (16B inline)", DURATION, || {
        let c = bisque_b.clone();
        black_box(&c);
    }));
    r.push(bench("bytes::Bytes::clone (16B)", DURATION, || {
        let c = bytes_b.clone();
        black_box(&c);
    }));
    r
}

// ---------------------------------------------------------------------------
// 5. Clone (heap — single fetch_add vs vtable dispatch)
// ---------------------------------------------------------------------------

fn bench_clone_heap() -> Vec<BenchResult> {
    let h = heap();
    let bisque_b = bisque_alloc::bytes::Bytes::copy_from_slice(&[1u8; 128], &h).unwrap();
    let bytes_b = bytes::Bytes::copy_from_slice(&[1u8; 128]);
    let mut r = Vec::new();

    r.push(bench("bisque Bytes::clone (128B heap)", DURATION, || {
        let c = bisque_b.clone();
        black_box(&c);
    }));
    r.push(bench("bytes::Bytes::clone (128B)", DURATION, || {
        let c = bytes_b.clone();
        black_box(&c);
    }));
    r
}

// ---------------------------------------------------------------------------
// 6. Clone + Drop cycle (the hot path in channel-based message passing)
// ---------------------------------------------------------------------------

fn bench_clone_drop() -> Vec<BenchResult> {
    let h = heap();
    let bisque_b = bisque_alloc::bytes::Bytes::copy_from_slice(&[1u8; 256], &h).unwrap();
    let bytes_b = bytes::Bytes::copy_from_slice(&[1u8; 256]);
    let mut r = Vec::new();

    r.push(bench("bisque clone+drop (256B heap)", DURATION, || {
        let c = bisque_b.clone();
        drop(black_box(c));
    }));
    r.push(bench("bytes  clone+drop (256B)", DURATION, || {
        let c = bytes_b.clone();
        drop(black_box(c));
    }));

    // Inline variant
    let bisque_s = bisque_alloc::bytes::Bytes::copy_from_slice(&[1u8; 16], &h).unwrap();
    let bytes_s = bytes::Bytes::copy_from_slice(&[1u8; 16]);

    r.push(bench("bisque clone+drop (16B inline)", DURATION, || {
        let c = bisque_s.clone();
        drop(black_box(c));
    }));
    r.push(bench("bytes  clone+drop (16B)", DURATION, || {
        let c = bytes_s.clone();
        drop(black_box(c));
    }));
    r
}

// ---------------------------------------------------------------------------
// 7. Slice (zero-copy sub-range)
// ---------------------------------------------------------------------------

fn bench_slice() -> Vec<BenchResult> {
    let h = heap();
    let bisque_b = bisque_alloc::bytes::Bytes::copy_from_slice(&[0u8; 1024], &h).unwrap();
    let bytes_b = bytes::Bytes::copy_from_slice(&[0u8; 1024]);
    let mut r = Vec::new();

    r.push(bench("bisque slice (1K → 256B)", DURATION, || {
        let s = bisque_b.slice(100..356);
        black_box(&s);
    }));
    r.push(bench("bytes  slice (1K → 256B)", DURATION, || {
        let s = bytes_b.slice(100..356);
        black_box(&s);
    }));
    r
}

// ---------------------------------------------------------------------------
// 8. Split (split_to — produces two halves)
// ---------------------------------------------------------------------------

fn bench_split() -> Vec<BenchResult> {
    let h = heap();
    let mut r = Vec::new();

    r.push(bench("bisque split_to (1K at 512)", DURATION, || {
        let mut b = bisque_alloc::bytes::Bytes::copy_from_slice(&[0u8; 1024], &h).unwrap();
        let head = b.split_to(512);
        black_box(&head);
        black_box(&b);
    }));
    r.push(bench("bytes  split_to (1K at 512)", DURATION, || {
        let mut b = bytes::Bytes::copy_from_slice(&[0u8; 1024]);
        let head = b.split_to(512);
        black_box(&head);
        black_box(&b);
    }));
    r
}

// ---------------------------------------------------------------------------
// 9. BytesMut build + freeze (the typical write pipeline path)
// ---------------------------------------------------------------------------

fn bench_build_freeze_small() -> Vec<BenchResult> {
    let h = heap();
    let mut r = Vec::new();

    // Small payload (fits inline after freeze)
    r.push(bench(
        "bisque BytesMut build+freeze (20B)",
        DURATION,
        || {
            let mut bm = bisque_alloc::bytes::BytesMut::with_capacity(64, &h).unwrap();
            bm.extend_from_slice(&[0xAA; 20]).unwrap();
            let b = bm.freeze();
            black_box(&b);
        },
    ));
    r.push(bench(
        "bytes  BytesMut build+freeze (20B)",
        DURATION,
        || {
            let mut bm = bytes::BytesMut::with_capacity(64);
            bm.extend_from_slice(&[0xAA; 20]);
            let b = bm.freeze();
            black_box(&b);
        },
    ));
    r
}

fn bench_build_freeze_large() -> Vec<BenchResult> {
    let h = heap();
    let mut r = Vec::new();

    // Larger payload (stays heap after freeze)
    r.push(bench(
        "bisque BytesMut build+freeze (256B)",
        DURATION,
        || {
            let mut bm = bisque_alloc::bytes::BytesMut::with_capacity(256, &h).unwrap();
            bm.extend_from_slice(&[0xBB; 256]).unwrap();
            let b = bm.freeze();
            black_box(&b);
        },
    ));
    r.push(bench(
        "bytes  BytesMut build+freeze (256B)",
        DURATION,
        || {
            let mut bm = bytes::BytesMut::with_capacity(256);
            bm.extend_from_slice(&[0xBB; 256]);
            let b = bm.freeze();
            black_box(&b);
        },
    ));
    r
}

// ---------------------------------------------------------------------------
// 10. BytesMut grow (repeated extend simulating message building)
// ---------------------------------------------------------------------------

fn bench_bytes_mut_grow() -> Vec<BenchResult> {
    let h = heap();
    let chunk = [0u8; 64];
    let iters = 50u64;
    let mut r = Vec::new();

    r.push(bench("bisque BytesMut extend×50 (3.2K)", DURATION, || {
        let mut bm = bisque_alloc::bytes::BytesMut::new(&h);
        for _ in 0..iters {
            bm.extend_from_slice(&chunk).unwrap();
        }
        black_box(&bm[..]);
    }));
    r.push(bench("bytes  BytesMut extend×50 (3.2K)", DURATION, || {
        let mut bm = bytes::BytesMut::new();
        for _ in 0..iters {
            bm.extend_from_slice(&chunk);
        }
        black_box(&bm[..]);
    }));
    r
}

// ---------------------------------------------------------------------------
// 11. Full pipeline: build → freeze → clone × N → drop all
//     (simulates MQ response fan-out)
// ---------------------------------------------------------------------------

fn bench_pipeline_fanout() -> Vec<BenchResult> {
    let h = heap();
    let fan_out = 8;
    let mut r = Vec::new();

    r.push(bench(
        "bisque build→freeze→clone×8→drop (256B)",
        DURATION,
        || {
            let mut bm = bisque_alloc::bytes::BytesMut::with_capacity(256, &h).unwrap();
            bm.extend_from_slice(&[0xCC; 256]).unwrap();
            let b = bm.freeze();
            let mut clones = Vec::with_capacity(fan_out);
            for _ in 0..fan_out {
                clones.push(b.clone());
            }
            drop(clones);
            drop(b);
        },
    ));
    r.push(bench(
        "bytes  build→freeze→clone×8→drop (256B)",
        DURATION,
        || {
            let mut bm = bytes::BytesMut::with_capacity(256);
            bm.extend_from_slice(&[0xCC; 256]);
            let b = bm.freeze();
            let mut clones = Vec::with_capacity(fan_out);
            for _ in 0..fan_out {
                clones.push(b.clone());
            }
            drop(clones);
            drop(b);
        },
    ));
    r
}

// ---------------------------------------------------------------------------
// 12. freeze → try_mut round-trip (unique owner reclaims mutability)
// ---------------------------------------------------------------------------

fn bench_freeze_try_mut() -> Vec<BenchResult> {
    let h = heap();
    let mut r = Vec::new();

    r.push(bench("bisque freeze→try_mut (256B)", DURATION, || {
        let mut bm = bisque_alloc::bytes::BytesMut::with_capacity(256, &h).unwrap();
        bm.extend_from_slice(&[0xDD; 256]).unwrap();
        let b = bm.freeze();
        let bm2 = b.try_mut().unwrap();
        black_box(&bm2[..]);
    }));
    // bytes crate: freeze → try_mut equivalent
    r.push(bench("bytes  freeze→try_mut (256B)", DURATION, || {
        let mut bm = bytes::BytesMut::with_capacity(256);
        bm.extend_from_slice(&[0xDD; 256]);
        let b = bm.freeze();
        let bm2 = b.try_into_mut().unwrap();
        black_box(&bm2[..]);
    }));
    r
}

// ---------------------------------------------------------------------------
// 13. Static bytes (no allocation path)
// ---------------------------------------------------------------------------

fn bench_static() -> Vec<BenchResult> {
    let mut r = Vec::new();

    r.push(bench("bisque Bytes::from_static", DURATION, || {
        let b = bisque_alloc::bytes::Bytes::from_static(b"hello world, this is static data");
        black_box(&b);
    }));
    r.push(bench("bytes  Bytes::from_static", DURATION, || {
        let b = bytes::Bytes::from_static(b"hello world, this is static data");
        black_box(&b);
    }));
    r
}

// ---------------------------------------------------------------------------
// 14. Multi-threaded clone+drop (contention on refcount)
// ---------------------------------------------------------------------------

fn bench_mt_clone_drop() -> Vec<BenchResult> {
    let h = heap();
    let ops = 200_000u64;
    let threads = 4;
    let mut r = Vec::new();

    let bisque_b = bisque_alloc::bytes::Bytes::copy_from_slice(&[0u8; 256], &h).unwrap();
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..threads {
            let b = &bisque_b;
            s.spawn(move || {
                for _ in 0..ops {
                    let c = b.clone();
                    drop(black_box(c));
                }
            });
        }
    });
    r.push(BenchResult {
        name: format!("bisque {threads}T clone+drop (256B)"),
        ops: ops * threads as u64,
        elapsed: start.elapsed(),
    });

    let bytes_b = bytes::Bytes::copy_from_slice(&[0u8; 256]);
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..threads {
            let b = &bytes_b;
            s.spawn(move || {
                for _ in 0..ops {
                    let c = b.clone();
                    drop(black_box(c));
                }
            });
        }
    });
    r.push(BenchResult {
        name: format!("bytes  {threads}T clone+drop (256B)"),
        ops: ops * threads as u64,
        elapsed: start.elapsed(),
    });

    r
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    println!();
    println!("=============================================================================");
    println!("  bisque_alloc::bytes vs bytes crate benchmark");
    println!("  Each test ~500ms. Sizes in bytes.");
    println!("=============================================================================");
    println!();

    let sections: Vec<(&str, Box<dyn Fn() -> Vec<BenchResult>>)> = vec![
        (
            "1.  Construct from slice (16B — inline)",
            Box::new(bench_construct_small),
        ),
        (
            "2.  Construct from slice (128B — heap)",
            Box::new(bench_construct_medium),
        ),
        (
            "3.  Construct from slice (4K — heap)",
            Box::new(bench_construct_large),
        ),
        ("4.  Clone inline (16B)", Box::new(bench_clone_inline)),
        ("5.  Clone heap (128B)", Box::new(bench_clone_heap)),
        ("6.  Clone + Drop cycle", Box::new(bench_clone_drop)),
        ("7.  Slice (zero-copy sub-range)", Box::new(bench_slice)),
        ("8.  Split (split_to)", Box::new(bench_split)),
        (
            "9a. BytesMut build+freeze (20B → inline)",
            Box::new(bench_build_freeze_small),
        ),
        (
            "9b. BytesMut build+freeze (256B → heap)",
            Box::new(bench_build_freeze_large),
        ),
        (
            "10. BytesMut grow (extend×50)",
            Box::new(bench_bytes_mut_grow),
        ),
        (
            "11. Full pipeline: build→freeze→clone×8→drop",
            Box::new(bench_pipeline_fanout),
        ),
        (
            "12. freeze → try_mut round-trip",
            Box::new(bench_freeze_try_mut),
        ),
        ("13. Static bytes (no allocation)", Box::new(bench_static)),
        (
            "14. Multi-threaded clone+drop (4T)",
            Box::new(bench_mt_clone_drop),
        ),
    ];

    for (title, f) in &sections {
        println!("--- {title} ---");
        for r in f() {
            r.print();
        }
        println!();
    }

    let (rss, commit) = bisque_alloc::heap::process_memory_info();
    println!("--- Process Memory ---");
    println!(
        "  RSS: {:.2} MiB   Committed: {:.2} MiB",
        rss as f64 / (1024.0 * 1024.0),
        commit as f64 / (1024.0 * 1024.0),
    );
    println!();
}

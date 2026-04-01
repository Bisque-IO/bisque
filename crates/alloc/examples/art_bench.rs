//! Art operation benchmark.
//!
//! Measures throughput for all Art operations: append vs insert,
//! get, delete, min/max, pop_min/pop_max, cursor seek, forward
//! and reverse iteration, compare_exchange.
//!
//! Run: cargo run --release -p bisque-alloc --example art-bench

use std::hint::black_box;
use std::time::{Duration, Instant};

use bisque_alloc::collections::art::{Art, Collector};
use bisque_alloc::collections::art::fixed::U64Art;
use bisque_alloc::{HeapMaster, MiMalloc};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const HEAP_SIZE: usize = 4 * 1024 * 1024 * 1024;

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed) }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0
    }
}

fn fmt_mops(ops: u64, elapsed: Duration) -> String {
    format!("{:.2} Mops/s", ops as f64 / elapsed.as_secs_f64() / 1e6)
}

fn bench<F: FnMut() -> u64>(name: &str, mut f: F) {
    // Warmup.
    black_box(f());
    // Timed run.
    let start = Instant::now();
    let ops = f();
    let elapsed = start.elapsed();
    println!("  {:40} {:>14}  ({} ops, {:.2}s)",
        name, fmt_mops(ops, elapsed), ops, elapsed.as_secs_f64());
}

fn main() {
    let master = HeapMaster::new(HEAP_SIZE).unwrap();
    let heap = master.heap();
    let c = Collector::new(&heap);

    println!("=== Art Operation Benchmark ===\n");

    // ─── Insert vs Append ──────────────────────────────────────────────
    println!("─── Sequential write: 100K keys ───");
    {
        bench("insert (sequential)", || {
            let t = Art::<usize, usize>::new(&c, &heap);
            let mut w = t.write();
            for i in 0..100_000usize { w.insert(i, i).unwrap(); }
            w.publish().unwrap();
            100_000
        });

        bench("append (sequential)", || {
            let t = Art::<usize, usize>::new(&c, &heap);
            let mut w = t.write();
            for i in 1..=100_000usize { w.append(i, i).unwrap(); }
            w.publish().unwrap();
            100_000
        });

        bench("put (sequential, batch + publish)", || {
            let t = Art::<usize, usize>::new(&c, &heap);
            let mut w = t.write();
            for i in 0..100_000usize { w.insert(i, i).unwrap(); }
            w.publish().unwrap();
            100_000
        });

        bench("insert (random 100K)", || {
            let t = Art::<usize, usize>::new(&c, &heap);
            let mut rng = Rng::new(0xBEEF);
            let mut w = t.write();
            for _ in 0..100_000 {
                let k = rng.next_u64() as usize;
                w.insert(k, k).unwrap();
            }
            w.publish().unwrap();
            100_000
        });
    }
    println!();

    // ─── Pre-populate ──────────────────────────────────────────────────
    let t = Art::<usize, usize>::new(&c, &heap);
    {
        let mut w = t.write();
        for i in 0..100_000usize { w.insert(i, i).unwrap(); }
        w.publish().unwrap();
    }
    let mut rng = Rng::new(0xCAFE);
    let lookup_keys: Vec<usize> = (0..100_000).map(|_| (rng.next_u64() as usize) % 100_000).collect();

    // ─── Point lookups ─────────────────────────────────────────────────
    println!("─── Point lookups (100K pre-populated) ───");
    {
        bench("get (pin per call)", || {
            let mut ops = 0u64;
            for &k in &lookup_keys { black_box(t.get(&k)); ops += 1; }
            ops
        });

        bench("ReadGuard.get (pin once)", || {
            let r = t.read();
            let mut ops = 0u64;
            for &k in &lookup_keys { black_box(r.get(&k)); ops += 1; }
            ops
        });
    }
    println!();

    // ─── Min / Max ─────────────────────────────────────────────────────
    println!("─── Min / Max (100K calls) ───");
    {
        bench("ReadGuard.min() (pin once)", || {
            let r = t.read();
            let mut ops = 0u64;
            for _ in 0..100_000 { black_box(r.min()); ops += 1; }
            ops
        });

        bench("min() (pin per call)", || {
            let mut ops = 0u64;
            for _ in 0..100_000 { black_box(t.read().min()); ops += 1; }
            ops
        });

        bench("ReadGuard.max() (pin once)", || {
            let r = t.read();
            let mut ops = 0u64;
            for _ in 0..100_000 { black_box(r.max()); ops += 1; }
            ops
        });

        bench("max() (pin per call)", || {
            let mut ops = 0u64;
            for _ in 0..100_000 { black_box(t.read().max()); ops += 1; }
            ops
        });
    }
    println!();

    // ─── Cursor seek ───────────────────────────────────────────────────
    println!("─── Cursor seek (100K ops) ───");
    {
        bench("seek_ge (pin once)", || {
            let r = t.read();
            let mut c = r.cursor();
            let mut ops = 0u64;
            for &k in &lookup_keys { c.seek_ge(k); black_box(c.key()); ops += 1; }
            ops
        });

        bench("seek_ge (pin per call)", || {
            let mut ops = 0u64;
            for &k in &lookup_keys {
                let r = t.read();
                let mut c = r.cursor();
                c.seek_ge(k);
                black_box(c.key());
                ops += 1;
            }
            ops
        });

        bench("seek_le (pin once)", || {
            let r = t.read();
            let mut c = r.cursor();
            let mut ops = 0u64;
            for &k in &lookup_keys { c.seek_le(k); black_box(c.key()); ops += 1; }
            ops
        });

        bench("seek_le (pin per call)", || {
            let mut ops = 0u64;
            for &k in &lookup_keys {
                let r = t.read();
                let mut c = r.cursor();
                c.seek_le(k);
                black_box(c.key());
                ops += 1;
            }
            ops
        });

        bench("seek_gt (pin once)", || {
            let r = t.read();
            let mut c = r.cursor();
            let mut ops = 0u64;
            for &k in &lookup_keys { c.seek_gt(k); black_box(c.key()); ops += 1; }
            ops
        });

        bench("seek_lt (pin once)", || {
            let r = t.read();
            let mut c = r.cursor();
            let mut ops = 0u64;
            for &k in &lookup_keys { c.seek_lt(k); black_box(c.key()); ops += 1; }
            ops
        });
    }
    println!();

    // ─── Iteration ─────────────────────────────────────────────────────
    println!("─── Full iteration (100K entries) ───");
    {
        bench("forward iter", || {
            let r = t.read();
            let mut ops = 0u64;
            for kv in r.iter() { black_box(kv); ops += 1; }
            ops
        });

        bench("reverse iter", || {
            let r = t.read();
            let mut ops = 0u64;
            for kv in r.rev_iter() { black_box(kv); ops += 1; }
            ops
        });

        bench("keys only", || {
            let r = t.read();
            let mut ops = 0u64;
            for k in r.keys() { black_box(k); ops += 1; }
            ops
        });

        bench("cursor_range 1000..2000", || {
            let r = t.read();
            let mut ops = 0u64;
            for kv in r.cursor_range(&1000, &2000) { black_box(kv); ops += 1; }
            ops
        });

        bench("cursor forward: seek_first + next", || {
            let r = t.read();
            let mut c = r.cursor();
            c.seek_first();
            let mut ops = 1u64;
            while c.next().is_some() { ops += 1; }
            ops
        });

        bench("cursor reverse: seek_last + prev", || {
            let r = t.read();
            let mut c = r.cursor();
            c.seek_last();
            let mut ops = 1u64;
            while c.prev().is_some() { ops += 1; }
            ops
        });
    }
    println!();

    // ─── Update existing ───────────────────────────────────────────────
    println!("─── Update existing keys (100K) ───");
    {
        bench("WriteGuard.insert (guard once)", || {
            let mut w = t.write();
            let mut ops = 0u64;
            for &k in &lookup_keys { w.insert(k, k + 1).unwrap(); ops += 1; }
            w.publish().unwrap();
            ops
        });

        bench("insert (pin per call, auto-publish)", || {
            let mut ops = 0u64;
            for &k in &lookup_keys { t.insert(k, k + 1).unwrap(); ops += 1; }
            ops
        });

        bench("put (batch + publish)", || {
            let mut w = t.write();
            let mut ops = 0u64;
            for &k in &lookup_keys { w.insert(k, k + 2).unwrap(); ops += 1; }
            w.publish().unwrap();
            ops
        });
    }
    println!();

    // ─── Delete + re-insert ────────────────────────────────────────────
    println!("─── Delete + re-insert (10K) ───");
    {
        bench("delete + re-insert cycle", || {
            let mut w = t.write();
            let mut ops = 0u64;
            for i in 0..10_000usize { w.remove(&i).unwrap(); ops += 1; }
            for i in 0..10_000usize { w.insert(i, i).unwrap(); ops += 1; }
            w.publish().unwrap();
            ops
        });
    }
    println!();

    // ─── Pop min / max ─────────────────────────────────────────────────
    println!("─── Pop min / max (1000 each from 10K tree) ───");
    {
        let t2 = Art::<usize, usize>::new(&c, &heap);
        {
            let mut w = t2.write();
            for i in 0..10_000usize { w.insert(i, i).unwrap(); }
            w.publish().unwrap();
        }

        bench("pop_min × 1000", || {
            let mut w = t2.write();
            let mut ops = 0u64;
            for _ in 0..1000 { black_box(w.pop_min().unwrap()); ops += 1; }
            w.publish().unwrap();
            ops
        });

        bench("pop_max × 1000", || {
            let mut w = t2.write();
            let mut ops = 0u64;
            for _ in 0..1000 { black_box(w.pop_max().unwrap()); ops += 1; }
            w.publish().unwrap();
            ops
        });
    }
    println!();

    // ─── Compare-exchange ──────────────────────────────────────────────
    println!("─── Compare-exchange (100K) ───");
    {
        // Reset values.
        { let mut w = t.write(); for i in 0..100_000usize { w.insert(i, i).unwrap(); } w.publish().unwrap(); }

        bench("compare_exchange (success)", || {
            let mut w = t.write();
            let mut ops = 0u64;
            for i in 0..100_000usize {
                let _ = w.compare_exchange(&i, i, Some(i + 1));
                ops += 1;
            }
            w.publish().unwrap();
            ops
        });

        bench("compare_exchange (fail)", || {
            let mut w = t.write();
            let mut ops = 0u64;
            for i in 0..100_000usize {
                let _ = w.compare_exchange(&i, i, Some(i + 2));
                ops += 1;
            }
            ops
        });
    }
    println!();

    // ─── Append scaling ────────────────────────────────────────────────
    println!("─── Append throughput scaling ───");
    for &count in &[10_000u64, 100_000, 1_000_000] {
        bench(&format!("append × {count}"), || {
            let t3 = Art::<usize, usize>::new(&c, &heap);
            let mut w = t3.write();
            for i in 1..=count as usize { w.append(i, i).unwrap(); }
            w.publish().unwrap();
            count
        });
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Art<usize> vs U64Art side-by-side
    // ═══════════════════════════════════════════════════════════════════════
    println!("\n═══ Art<usize> vs U64Art<usize> ═══\n");

    let ft = U64Art::<usize>::new(&c, &heap);
    {
        let mut w = ft.write();
        for i in 0..100_000u64 { w.insert(i, i as usize).unwrap(); }
        w.publish().unwrap();
    }

    println!("─── Sequential insert 100K ───");
    bench("Art<usize>  insert", || {
        let t = Art::<usize, usize>::new(&c, &heap);
        let mut w = t.write();
        for i in 0..100_000usize { w.insert(i, i).unwrap(); }
        w.publish().unwrap();
        100_000
    });
    bench("U64Art      put(u64)", || {
        let t = U64Art::<usize>::new(&c, &heap);
        let mut w = t.write();
        for i in 0..100_000u64 { w.insert(i, i as usize).unwrap(); }
        w.publish().unwrap();
        100_000
    });
    println!();

    println!("─── Random insert 100K ───");
    bench("Art<usize>  insert (random)", || {
        let t = Art::<usize, usize>::new(&c, &heap);
        let mut rng = Rng::new(0xBEEF);
        let mut w = t.write();
        for _ in 0..100_000 { let k = rng.next_u64() as usize; w.insert(k, k).unwrap(); }
        w.publish().unwrap();
        100_000
    });
    bench("U64Art      put(u64, random)", || {
        let t = U64Art::<usize>::new(&c, &heap);
        let mut rng = Rng::new(0xBEEF);
        let mut w = t.write();
        for _ in 0..100_000 { let k = rng.next_u64(); w.insert(k, k as usize).unwrap(); }
        w.publish().unwrap();
        100_000
    });
    println!();

    println!("─── Point lookup 100K (pin once) ───");
    bench("Art<usize>  ReadGuard.get", || {
        let r = t.read();
        let mut ops = 0u64;
        for &k in &lookup_keys { black_box(r.get(&k)); ops += 1; }
        ops
    });
    bench("U64Art      ReadGuard.get(&bytes)", || {
        let r = ft.read();
        let mut ops = 0u64;
        for &k in &lookup_keys { black_box(r.get(&(k as u64))); ops += 1; }
        ops
    });
    bench("U64Art      ReadGuard.get(&u64)", || {
        let r = ft.read();
        let mut ops = 0u64;
        for &k in &lookup_keys { black_box(r.get(&(k as u64))); ops += 1; }
        ops
    });
    println!();

    println!("─── Update existing 100K (guard once) ───");
    bench("Art<usize>  WriteGuard.insert", || {
        let mut w = t.write();
        let mut ops = 0u64;
        for &k in &lookup_keys { w.insert(k, k + 1).unwrap(); ops += 1; }
        w.publish().unwrap();
        ops
    });
    bench("U64Art      WriteGuard.put(u64)", || {
        let mut w = ft.write();
        let mut ops = 0u64;
        for &k in &lookup_keys { w.insert(k as u64, k + 1).unwrap(); ops += 1; }
        w.publish().unwrap();
        ops
    });
    println!();

    println!("─── Full forward iteration 100K ───");
    bench("Art<usize>  iter", || {
        let r = t.read();
        let mut ops = 0u64;
        for kv in r.iter() { black_box(kv); ops += 1; }
        ops
    });
    bench("U64Art      iter", || {
        let r = ft.read();
        let mut ops = 0u64;
        for kv in r.iter() { black_box(kv); ops += 1; }
        ops
    });
    println!();

    println!("─── Min / Max ───");
    bench("Art<usize>  min()", || {
        let r = t.read();
        let mut ops = 0u64;
        for _ in 0..100_000 { black_box(r.min()); ops += 1; }
        ops
    });
    bench("U64Art      min()", || {
        let r = ft.read();
        let mut ops = 0u64;
        for _ in 0..100_000 { black_box(r.min()); ops += 1; }
        ops
    });

    println!("\n=== Done ===");
}

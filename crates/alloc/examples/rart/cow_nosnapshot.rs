use bisque_alloc::collections::rart::cow_tree::CowTree;
use bisque_alloc::collections::rart::keys::u64_key::U64Key;
use bisque_alloc::{HeapMaster, MiMalloc};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Clone, Copy)]
#[repr(C)]
struct Meta {
    data: [u8; 96],
}
impl Meta {
    fn new(id: u64) -> Self {
        let mut m = Self { data: [0; 96] };
        m.data[..8].copy_from_slice(&id.to_le_bytes());
        m
    }
}

fn main() {
    let n = 100_000u64;
    let master = HeapMaster::new(512 * 1024 * 1024).unwrap();

    // --- With snapshot every 100 ops ---
    {
        let mut tree = CowTree::<U64Key, Arc<Meta>>::new(&master);
        for i in 0..n {
            tree.insert(i, Arc::new(Meta::new(i))).unwrap();
        }

        let start = Instant::now();
        let iters = 200u64;
        for batch in 0..iters {
            let base = n + batch * 100;
            for i in 0..100u64 {
                tree.insert(base + i, Arc::new(Meta::new(base + i)))
                    .unwrap();
            }
            let _snap = tree.snapshot(); // forces COW on next batch
            for i in 0..100u64 {
                tree.remove(base + i).unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total_ops = iters * 200;
        let mops = total_ops as f64 / elapsed.as_secs_f64() / 1e6;
        println!(
            "With snapshot/100:   {mops:.2} Mops/s  ({:.0} ns/op)",
            elapsed.as_nanos() as f64 / total_ops as f64
        );
    }

    // --- NO snapshot (pure insert+remove speed) ---
    {
        let mut tree = CowTree::<U64Key, Arc<Meta>>::new(&master);
        for i in 0..n {
            tree.insert(i, Arc::new(Meta::new(i))).unwrap();
        }

        let start = Instant::now();
        let iters = 200u64;
        for batch in 0..iters {
            let base = n + batch * 100;
            for i in 0..100u64 {
                tree.insert(base + i, Arc::new(Meta::new(base + i)))
                    .unwrap();
            }
            // NO snapshot
            for i in 0..100u64 {
                tree.remove(base + i).unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total_ops = iters * 200;
        let mops = total_ops as f64 / elapsed.as_secs_f64() / 1e6;
        println!(
            "No snapshot:         {mops:.2} Mops/s  ({:.0} ns/op)",
            elapsed.as_nanos() as f64 / total_ops as f64
        );
    }

    // --- Snapshot every 1000 ops ---
    {
        let mut tree = CowTree::<U64Key, Arc<Meta>>::new(&master);
        for i in 0..n {
            tree.insert(i, Arc::new(Meta::new(i))).unwrap();
        }

        let start = Instant::now();
        let iters = 20u64;
        for batch in 0..iters {
            let base = n + batch * 1000;
            for i in 0..1000u64 {
                tree.insert(base + i, Arc::new(Meta::new(base + i)))
                    .unwrap();
            }
            let _snap = tree.snapshot();
            for i in 0..1000u64 {
                tree.remove(base + i).unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total_ops = iters * 2000;
        let mops = total_ops as f64 / elapsed.as_secs_f64() / 1e6;
        println!(
            "With snapshot/1000:  {mops:.2} Mops/s  ({:.0} ns/op)",
            elapsed.as_nanos() as f64 / total_ops as f64
        );
    }
}

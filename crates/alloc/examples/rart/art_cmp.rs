use bisque_alloc::collections::rart::cow_tree::CowTree;
use bisque_alloc::collections::rart::keys::u64_key::U64Key;
use bisque_alloc::collections::rart::tree::AdaptiveRadixTree;
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

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
}

fn main() {
    let n = 100_000u64;
    let master = HeapMaster::new(512 * 1024 * 1024).unwrap();
    let mut rng = Rng::new(0xBEEF);
    let existing: Vec<u64> = (0..n).map(|_| rng.next()).collect();

    println!("100K random u64 keys pre-populated, then batch insert+remove:\n");

    // Plain ART
    {
        let mut tree = AdaptiveRadixTree::<U64Key, Meta>::new(&master);
        for &k in &existing {
            tree.insert(k, Meta::new(k)).unwrap();
        }

        let start = Instant::now();
        let iters = 200u64;
        for batch in 0..iters {
            let base = u64::MAX / 2 + batch * 100;
            for i in 0..100u64 {
                tree.insert(base + i, Meta::new(base + i)).unwrap();
            }
            for i in 0..100u64 {
                tree.remove(base + i).unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total = iters * 200;
        println!(
            "  Plain ART<U64Key, Meta>:    {:.2} Mops/s  ({:.0} ns/op)",
            total as f64 / elapsed.as_secs_f64() / 1e6,
            elapsed.as_nanos() as f64 / total as f64
        );
    }

    // CowTree inline Meta
    {
        let mut tree = CowTree::<U64Key, Meta>::new(&master);
        for &k in &existing {
            tree.insert(k, Meta::new(k)).unwrap();
        }

        let start = Instant::now();
        let iters = 200u64;
        for batch in 0..iters {
            let base = u64::MAX / 2 + batch * 100;
            for i in 0..100u64 {
                tree.insert(base + i, Meta::new(base + i)).unwrap();
            }
            for i in 0..100u64 {
                tree.remove(base + i).unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total = iters * 200;
        println!(
            "  CowTree<U64Key, Meta>:      {:.2} Mops/s  ({:.0} ns/op)",
            total as f64 / elapsed.as_secs_f64() / 1e6,
            elapsed.as_nanos() as f64 / total as f64
        );
    }

    // CowTree Arc<Meta>
    {
        let mut tree = CowTree::<U64Key, Arc<Meta>>::new(&master);
        for &k in &existing {
            tree.insert(k, Arc::new(Meta::new(k))).unwrap();
        }

        let start = Instant::now();
        let iters = 200u64;
        for batch in 0..iters {
            let base = u64::MAX / 2 + batch * 100;
            for i in 0..100u64 {
                tree.insert(base + i, Arc::new(Meta::new(base + i)))
                    .unwrap();
            }
            for i in 0..100u64 {
                tree.remove(base + i).unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total = iters * 200;
        println!(
            "  CowTree<U64Key, Arc<Meta>>: {:.2} Mops/s  ({:.0} ns/op)",
            total as f64 / elapsed.as_secs_f64() / 1e6,
            elapsed.as_nanos() as f64 / total as f64
        );
    }

    // CowTree ()
    {
        let mut tree = CowTree::<U64Key, ()>::new(&master);
        for &k in &existing {
            tree.insert(k, ()).unwrap();
        }

        let start = Instant::now();
        let iters = 200u64;
        for batch in 0..iters {
            let base = u64::MAX / 2 + batch * 100;
            for i in 0..100u64 {
                tree.insert(base + i, ()).unwrap();
            }
            for i in 0..100u64 {
                tree.remove(base + i).unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total = iters * 200;
        println!(
            "  CowTree<U64Key, ()>:        {:.2} Mops/s  ({:.0} ns/op)",
            total as f64 / elapsed.as_secs_f64() / 1e6,
            elapsed.as_nanos() as f64 / total as f64
        );
    }
}

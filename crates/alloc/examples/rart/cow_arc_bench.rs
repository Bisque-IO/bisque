//! Quick benchmark: CowTree<U64Key, Meta> vs CowTree<U64Key, Arc<Meta>>
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
    let keys: Vec<u64> = (0..n).collect();

    // --- CowTree<U64Key, Meta> (inline 96-byte values) ---
    {
        let mut tree = CowTree::<U64Key, Meta>::new(&master);
        for &k in &keys {
            tree.insert(k, Meta::new(k)).unwrap();
        }
        let _snap = tree.snapshot(); // root shared

        let start = Instant::now();
        let iters = 100u64;
        for batch in 0..iters {
            let base = n + batch * 100;
            for i in 0..100u64 {
                tree.insert(base + i, Meta::new(base + i)).unwrap();
            }
            let _s = tree.snapshot();
            for i in 0..100u64 {
                tree.remove(base + i).unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total_ops = iters * 200;
        let mops = total_ops as f64 / elapsed.as_secs_f64() / 1e6;
        let ns = elapsed.as_nanos() as f64 / total_ops as f64;
        println!("CowTree<U64Key, Meta>        (inline 96B):  {mops:.2} Mops/s  {ns:.1} ns/op");
    }

    // --- CowTree<U64Key, Arc<Meta>> (Arc-wrapped values) ---
    {
        let mut tree = CowTree::<U64Key, Arc<Meta>>::new(&master);
        for &k in &keys {
            tree.insert(k, Arc::new(Meta::new(k))).unwrap();
        }
        let _snap = tree.snapshot();

        let start = Instant::now();
        let iters = 100u64;
        for batch in 0..iters {
            let base = n + batch * 100;
            for i in 0..100u64 {
                tree.insert(base + i, Arc::new(Meta::new(base + i)))
                    .unwrap();
            }
            let _s = tree.snapshot();
            for i in 0..100u64 {
                tree.remove(base + i).unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total_ops = iters * 200;
        let mops = total_ops as f64 / elapsed.as_secs_f64() / 1e6;
        let ns = elapsed.as_nanos() as f64 / total_ops as f64;
        println!("CowTree<U64Key, Arc<Meta>>   (Arc 8B ref):  {mops:.2} Mops/s  {ns:.1} ns/op");
    }

    // --- CowTree<U64Key, ()> (zero-size value — best case) ---
    {
        let mut tree = CowTree::<U64Key, ()>::new(&master);
        for &k in &keys {
            tree.insert(k, ()).unwrap();
        }
        let _snap = tree.snapshot();

        let start = Instant::now();
        let iters = 100u64;
        for batch in 0..iters {
            let base = n + batch * 100;
            for i in 0..100u64 {
                tree.insert(base + i, ()).unwrap();
            }
            let _s = tree.snapshot();
            for i in 0..100u64 {
                tree.remove(base + i).unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total_ops = iters * 200;
        let mops = total_ops as f64 / elapsed.as_secs_f64() / 1e6;
        let ns = elapsed.as_nanos() as f64 / total_ops as f64;
        println!("CowTree<U64Key, ()>          (zero-size):   {mops:.2} Mops/s  {ns:.1} ns/op");
    }

    println!("\nEnum sizes:");
    println!(
        "  CowChild<U64Partial, Meta>:      {} bytes",
        std::mem::size_of::<
            bisque_alloc::collections::rart::cow_tree::CowChild::<
                bisque_alloc::collections::rart::keys::u64_key::U64Partial,
                Meta,
            >,
        >()
    );
    println!(
        "  CowChild<U64Partial, Arc<Meta>>:  {} bytes",
        std::mem::size_of::<
            bisque_alloc::collections::rart::cow_tree::CowChild::<
                bisque_alloc::collections::rart::keys::u64_key::U64Partial,
                Arc<Meta>,
            >,
        >()
    );
    println!(
        "  CowChild<U64Partial, ()>:         {} bytes",
        std::mem::size_of::<
            bisque_alloc::collections::rart::cow_tree::CowChild::<
                bisque_alloc::collections::rart::keys::u64_key::U64Partial,
                (),
            >,
        >()
    );
}

use bisque_alloc::collections::rart::keys::array_key::ArrayKey;
use bisque_alloc::collections::rart::versioned_tree::VersionedAdaptiveRadixTree;
use bisque_alloc::{HeapMaster, MiMalloc};
use std::hint::black_box;
use std::time::Instant;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    let n = 100_000u64;
    let master = HeapMaster::new(256 * 1024 * 1024).unwrap();

    // Scenario 1: No outstanding snapshot (all refcount=1)
    println!(
        "Memory usage before tree creation: {}",
        master.memory_usage()
    );
    {
        let mut tree = VersionedAdaptiveRadixTree::<ArrayKey<16>, u64>::new(&master);
        let start = Instant::now();
        for i in 0..n {
            tree.insert(&i, i).unwrap();
        }
        let elapsed = start.elapsed();
        let ns = elapsed.as_nanos() as f64 / n as f64;
        let mops = n as f64 / elapsed.as_secs_f64() / 1e6;
        println!("No snapshot:      {mops:>6.2} Mops/s  {ns:>6.1} ns/op");
        println!("Memory usage after tree: {}", master.memory_usage());
    }

    println!(
        "Memory usage before tree creation: {}",
        master.memory_usage()
    );
    // Scenario 2: One outstanding snapshot (root shared, first insert COWs root)
    {
        let mut tree = VersionedAdaptiveRadixTree::<ArrayKey<16>, u64>::new(&master);
        // Pre-populate so the tree has structure
        for i in 0..1000u64 {
            tree.insert(&i, i).unwrap();
        }
        let _snap = tree.snapshot(); // root refcount → 2
        let start = Instant::now();
        for i in 1000..1000 + n {
            tree.insert(&i, i).unwrap();
        }
        let elapsed = start.elapsed();
        let ns = elapsed.as_nanos() as f64 / n as f64;
        let mops = n as f64 / elapsed.as_secs_f64() / 1e6;
        println!("With snapshot:    {mops:>6.2} Mops/s  {ns:>6.1} ns/op");
    }

    println!(
        "Memory usage before tree creation: {}",
        master.memory_usage()
    );
    // Scenario 3: Publish every 100 inserts (simulating 2000 pub/s with batches)
    {
        let mut tree = VersionedAdaptiveRadixTree::<ArrayKey<16>, u64>::new(&master);
        let mut snaps = Vec::new();
        let start = Instant::now();
        for i in 0..n {
            tree.insert(&i, i).unwrap();
            if i % 100 == 99 {
                // Keep only the latest snapshot (drop old ones)
                snaps.clear();
                snaps.push(tree.snapshot());
            }
        }
        let elapsed = start.elapsed();
        let ns = elapsed.as_nanos() as f64 / n as f64;
        let mops = n as f64 / elapsed.as_secs_f64() / 1e6;
        println!("Pub every 100:    {mops:>6.2} Mops/s  {ns:>6.1} ns/op");
    }

    println!(
        "Memory usage before tree creation: {}",
        master.memory_usage()
    );
    // Scenario 4: Publish every 1000 inserts
    {
        let mut tree = VersionedAdaptiveRadixTree::<ArrayKey<16>, u64>::new(&master);
        let mut snaps = Vec::new();
        let start = Instant::now();
        for i in 0..n {
            tree.insert(&i, i).unwrap();
            if i % 1000 == 999 {
                snaps.clear();
                snaps.push(tree.snapshot());
            }
        }
        let elapsed = start.elapsed();
        let ns = elapsed.as_nanos() as f64 / n as f64;
        let mops = n as f64 / elapsed.as_secs_f64() / 1e6;
        println!("Pub every 1000:   {mops:>6.2} Mops/s  {ns:>6.1} ns/op");
    }
}

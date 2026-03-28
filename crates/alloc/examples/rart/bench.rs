//! Benchmark: ART vs hashbrown, BTreeMap, papaya, DashMap, scc::TreeIndex.
//!
//! Run with: cargo run --release -p bisque-alloc --example rart-bench
//!
//! Measures insert, get (sequential + random), range scan, and snapshot
//! overhead across seven data structure implementations.

use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

use bisque_alloc::collections::art::Art as ArtRaw;
use bisque_alloc::collections::congee::DefaultAllocator;
use bisque_alloc::collections::art::Art as MvccArt;
use bisque_alloc::collections::rart::cow_tree::CowTree;
use bisque_alloc::collections::rart::keys::array_key::ArrayKey;
use bisque_alloc::collections::rart::keys::u64_key::U64Key;
use bisque_alloc::collections::rart::published_tree::PublishedTree;
use bisque_alloc::collections::rart::tree::AdaptiveRadixTree;
use bisque_alloc::{HeapMaster, MiMalloc};
use comfy_table::{Attribute, Cell, ContentArrangement, Table};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const HEAP_SIZE: usize = 1024 * 1024 * 1024 * 8;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct BenchResult {
    name: String,
    total_ops: u64,
    elapsed: Duration,
}

impl BenchResult {
    fn ns_per_op(&self) -> f64 {
        self.elapsed.as_nanos() as f64 / self.total_ops as f64
    }
    fn mops_per_sec(&self) -> f64 {
        self.total_ops as f64 / self.elapsed.as_secs_f64() / 1_000_000.0
    }
}

fn bench<F: FnMut()>(name: &str, ops_per_call: u64, dur: Duration, mut f: F) -> BenchResult {
    for _ in 0..3 {
        f();
    }
    let t0 = Instant::now();
    f();
    let one_call = t0.elapsed().as_nanos().max(1) as f64;
    let target_iters = ((dur.as_nanos() as f64 / one_call) as u64).max(3);
    let start = Instant::now();
    for _ in 0..target_iters {
        f();
    }
    BenchResult {
        name: name.into(),
        total_ops: target_iters * ops_per_call,
        elapsed: start.elapsed(),
    }
}

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
}

fn shuffle(v: &mut [u64], seed: u64) {
    let mut rng = Rng::new(seed);
    for i in (1..v.len()).rev() {
        let j = rng.next_u64() as usize % (i + 1);
        v.swap(i, j);
    }
}

const N: usize = 100_000;
const DUR: Duration = Duration::from_millis(500);

fn make_sequential_keys() -> Vec<u64> {
    (0..N as u64).collect()
}

fn make_random_keys() -> Vec<u64> {
    let mut rng = Rng::new(0xDEAD_BEEF);
    (0..N).map(|_| rng.next_u64()).collect()
}

fn main() {
    let seq_keys = make_sequential_keys();
    let rand_keys = make_random_keys();
    let n = N as u64;

    println!(
        "Benchmark: N={N} keys, {}ms per measurement\n",
        DUR.as_millis()
    );

    // ===================================================================
    // 1. Sequential insert
    // ===================================================================
    let mut r = Vec::new();

    let master = HeapMaster::new(HEAP_SIZE).unwrap();

    println!(
        "Memory usage before tree creation: {}",
        master.memory_usage()
    );
    r.push(bench("ART<ArrayKey<16>>", n, DUR, || {
        let mut tree = AdaptiveRadixTree::<ArrayKey<16>, u64>::new(&master);
        for &k in &seq_keys {
            tree.insert(k, k).unwrap();
        }
        black_box(&tree);
    }));
    println!("Memory usage after: {}", master.memory_usage());

    r.push(bench("ART<U64Key>", n, DUR, || {
        let mut tree = AdaptiveRadixTree::<U64Key, u64>::new(&master);
        for &k in &seq_keys {
            tree.insert(k, k).unwrap();
        }
        black_box(&tree);
    }));

    r.push(bench("CowTree (inline leaf)", n, DUR, || {
        let mut tree = CowTree::<ArrayKey<16>, u64>::new(&master);
        for &k in &seq_keys {
            tree.insert(k, k).unwrap();
        }
        black_box(&tree);
    }));

    r.push(bench("hashbrown::HashMap", n, DUR, || {
        let mut map = hashbrown::HashMap::<u64, u64>::with_capacity(N);
        for &k in &seq_keys {
            map.insert(k, k);
        }
        black_box(&map);
    }));

    r.push(bench("std::BTreeMap", n, DUR, || {
        let mut map = BTreeMap::<u64, u64>::new();
        for &k in &seq_keys {
            map.insert(k, k);
        }
        black_box(&map);
    }));

    r.push(bench("papaya::HashMap", n, DUR, || {
        let map = papaya::HashMap::<u64, u64>::new();
        for &k in &seq_keys {
            map.pin().insert(k, k);
        }
        black_box(&map);
    }));

    r.push(bench("DashMap", n, DUR, || {
        let map = dashmap::DashMap::<u64, u64>::with_capacity(N);
        for &k in &seq_keys {
            map.insert(k, k);
        }
        black_box(&map);
    }));

    r.push(bench("scc::TreeIndex", n, DUR, || {
        let tree = scc::TreeIndex::<u64, u64>::new();
        for &k in &seq_keys {
            let _ = tree.insert_sync(k, k);
        }
        black_box(&tree);
    }));

    println!(
        "Memory usage before tree creation: {}",
        master.memory_usage()
    );
    {
        r.push(bench("Art (Heap+OLC)", n, DUR, || {
            let tree = ArtRaw::<usize, usize>::new(&master);
            let guard = tree.pin();
            for &k in &seq_keys {
                tree.insert(k as usize, k as usize, &guard).unwrap();
            }
            black_box(&tree);
        }));
        println!("Memory usage after: {}", master.memory_usage());
    }

    r.push(bench("MvccArt (COW)", n, DUR, || {
        let tree = MvccArt::<usize, usize>::new(&master);
        let mut txn = tree.write();
        for &k in &seq_keys {
            txn.insert(k as usize, k as usize).unwrap();
        }
        txn.commit().unwrap();
        black_box(&tree);
    }));

    r.push(bench("scc::HashMap", n, DUR, || {
        let map = scc::HashMap::<u64, u64>::new();
        for &k in &seq_keys {
            let _ = map.insert_sync(k, k);
        }
        black_box(&map);
    }));

    print_table("Sequential Insert (100K u64 keys)", &r);

    // ===================================================================
    // 2. Random insert
    // ===================================================================
    let mut r = Vec::new();

    r.push(bench("ART (Heap)", n, DUR, || {
        let mut tree = AdaptiveRadixTree::<ArrayKey<16>, u64>::new(&master);
        for &k in &rand_keys {
            tree.insert(k, k).unwrap();
        }
        black_box(&tree);
    }));

    r.push(bench("hashbrown::HashMap", n, DUR, || {
        let mut map = hashbrown::HashMap::<u64, u64>::with_capacity(N);
        for &k in &rand_keys {
            map.insert(k, k);
        }
        black_box(&map);
    }));

    r.push(bench("std::BTreeMap", n, DUR, || {
        let mut map = BTreeMap::<u64, u64>::new();
        for &k in &rand_keys {
            map.insert(k, k);
        }
        black_box(&map);
    }));

    r.push(bench("papaya::HashMap", n, DUR, || {
        let map = papaya::HashMap::<u64, u64>::new();
        for &k in &rand_keys {
            map.pin().insert(k, k);
        }
        black_box(&map);
    }));

    r.push(bench("DashMap", n, DUR, || {
        let map = dashmap::DashMap::<u64, u64>::with_capacity(N);
        for &k in &rand_keys {
            map.insert(k, k);
        }
        black_box(&map);
    }));

    r.push(bench("scc::TreeIndex", n, DUR, || {
        let tree = scc::TreeIndex::<u64, u64>::new();
        for &k in &rand_keys {
            let _ = tree.insert_sync(k, k);
        }
        black_box(&tree);
    }));

    // Art uses a shared HeapMaster to avoid use-after-free from
    // crossbeam-epoch deferred deallocations outliving the heap.
    {
        r.push(bench("Art (Heap+OLC)", n, DUR, || {
            // let tree = ArtRaw::<usize, usize, DefaultAllocator>::new(&master);
            let tree = ArtRaw::<usize, usize>::new(&master);
            let guard = tree.pin();
            for &k in &rand_keys {
                tree.insert(k as usize, k as usize, &guard).unwrap();
            }
            black_box(&tree);
        }));
    }

    r.push(bench("MvccArt (COW)", n, DUR, || {
        let tree = MvccArt::<usize, usize>::new(&master);
        let mut txn = tree.write();
        for &k in &rand_keys {
            txn.insert(k as usize, k as usize).unwrap();
        }
        txn.commit().unwrap();
        black_box(&tree);
    }));

    r.push(bench("scc::HashMap", n, DUR, || {
        let map = scc::HashMap::<u64, u64>::new();
        for &k in &rand_keys {
            let _ = map.insert_sync(k, k);
        }
        black_box(&map);
    }));

    print_table("Random Insert (100K u64 keys)", &r);

    // ===================================================================
    // 3. Sequential get (pre-populated)
    // ===================================================================
    let mut r = Vec::new();

    {
        let mut tree = AdaptiveRadixTree::<ArrayKey<16>, u64>::new(&master);
        for &k in &seq_keys {
            tree.insert(k, k).unwrap();
        }
        r.push(bench("ART<ArrayKey<16>>", n, DUR, || {
            for &k in &seq_keys {
                black_box(tree.get(k));
            }
        }));
    }

    {
        let mut tree = AdaptiveRadixTree::<U64Key, u64>::new(&master);
        for &k in &seq_keys {
            tree.insert(k, k).unwrap();
        }
        r.push(bench("ART<U64Key>", n, DUR, || {
            for &k in &seq_keys {
                black_box(tree.get(k));
            }
        }));
    }

    {
        let mut tree = CowTree::<ArrayKey<16>, u64>::new(&master);
        for &k in &seq_keys {
            tree.insert(k, k).unwrap();
        }
        r.push(bench("CowTree (inline leaf)", n, DUR, || {
            for &k in &seq_keys {
                black_box(tree.get(k));
            }
        }));
    }

    {
        let mut map = hashbrown::HashMap::<u64, u64>::with_capacity(N);
        for &k in &seq_keys {
            map.insert(k, k);
        }
        r.push(bench("hashbrown::HashMap", n, DUR, || {
            for &k in &seq_keys {
                black_box(map.get(&k));
            }
        }));
    }

    {
        let mut map = BTreeMap::<u64, u64>::new();
        for &k in &seq_keys {
            map.insert(k, k);
        }
        r.push(bench("std::BTreeMap", n, DUR, || {
            for &k in &seq_keys {
                black_box(map.get(&k));
            }
        }));
    }

    {
        let map = papaya::HashMap::<u64, u64>::new();
        for &k in &seq_keys {
            map.pin().insert(k, k);
        }
        r.push(bench("papaya::HashMap", n, DUR, || {
            let guard = map.guard();
            for &k in &seq_keys {
                black_box(map.get(&k, &guard));
            }
        }));
    }

    {
        let map = dashmap::DashMap::<u64, u64>::with_capacity(N);
        for &k in &seq_keys {
            map.insert(k, k);
        }
        r.push(bench("DashMap", n, DUR, || {
            for &k in &seq_keys {
                black_box(map.get(&k));
            }
        }));
    }

    {
        let tree = scc::TreeIndex::<u64, u64>::new();
        for &k in &seq_keys {
            let _ = tree.insert_sync(k, k);
        }
        r.push(bench("scc::TreeIndex", n, DUR, || {
            for &k in &seq_keys {
                black_box(tree.peek_with(&k, |_, v| *v));
            }
        }));
    }

    {
        let tree = ArtRaw::<usize, usize>::new(&master);
        {
            let g = tree.pin();
            for &k in &seq_keys {
                tree.insert(k as usize, k as usize, &g).unwrap();
            }
        }
        r.push(bench("Art (Heap+OLC)", n, DUR, || {
            let guard = tree.pin();
            for &k in &seq_keys {
                black_box(tree.get(&(k as usize), &guard));
            }
        }));
    }

    {
        let tree = MvccArt::<usize, usize>::new(&master);
        {
            let mut txn = tree.write();
            for &k in &seq_keys {
                txn.insert(k as usize, k as usize).unwrap();
            }
            txn.commit().unwrap();
        }
        r.push(bench("MvccArt (COW)", n, DUR, || {
            for &k in &seq_keys {
                black_box(tree.get(&(k as usize)));
            }
        }));
    }

    {
        let map = scc::HashMap::<u64, u64>::new();
        for &k in &seq_keys {
            let _ = map.insert_sync(k, k);
        }
        r.push(bench("scc::HashMap", n, DUR, || {
            for &k in &seq_keys {
                black_box(map.read_sync(&k, |_, v| *v));
            }
        }));
    }

    print_table("Sequential Get (100K keys, pre-populated)", &r);

    // ===================================================================
    // 4. Random get (pre-populated, shuffled lookup order)
    // ===================================================================
    let mut r = Vec::new();
    let mut lookup = rand_keys.clone();
    shuffle(&mut lookup, 0xCAFE);

    {
        let mut tree = AdaptiveRadixTree::<ArrayKey<16>, u64>::new(&master);
        for &k in &rand_keys {
            tree.insert(k, k).unwrap();
        }
        r.push(bench("ART (Heap)", n, DUR, || {
            for &k in &lookup {
                black_box(tree.get(k));
            }
        }));
    }

    {
        let mut map = hashbrown::HashMap::<u64, u64>::with_capacity(N);
        for &k in &rand_keys {
            map.insert(k, k);
        }
        r.push(bench("hashbrown::HashMap", n, DUR, || {
            for &k in &lookup {
                black_box(map.get(&k));
            }
        }));
    }

    {
        let mut map = BTreeMap::<u64, u64>::new();
        for &k in &rand_keys {
            map.insert(k, k);
        }
        r.push(bench("std::BTreeMap", n, DUR, || {
            for &k in &lookup {
                black_box(map.get(&k));
            }
        }));
    }

    {
        let map = papaya::HashMap::<u64, u64>::new();
        for &k in &rand_keys {
            map.pin().insert(k, k);
        }
        r.push(bench("papaya::HashMap", n, DUR, || {
            let guard = map.guard();
            for &k in &lookup {
                black_box(map.get(&k, &guard));
            }
        }));
    }

    {
        let map = dashmap::DashMap::<u64, u64>::with_capacity(N);
        for &k in &rand_keys {
            map.insert(k, k);
        }
        r.push(bench("DashMap", n, DUR, || {
            for &k in &lookup {
                black_box(map.get(&k));
            }
        }));
    }

    {
        let tree = scc::TreeIndex::<u64, u64>::new();
        for &k in &rand_keys {
            let _ = tree.insert_sync(k, k);
        }
        r.push(bench("scc::TreeIndex", n, DUR, || {
            for &k in &lookup {
                black_box(tree.peek_with(&k, |_, v| *v));
            }
        }));
    }

    {
        let tree = ArtRaw::<usize, usize>::new(&master);
        {
            let g = tree.pin();
            for &k in &rand_keys {
                tree.insert(k as usize, k as usize, &g).unwrap();
            }
        }
        r.push(bench("Art (Heap+OLC)", n, DUR, || {
            let guard = tree.pin();
            for &k in &lookup {
                black_box(tree.get(&(k as usize), &guard));
            }
        }));
    }

    {
        let tree = MvccArt::<usize, usize>::new(&master);
        {
            let mut txn = tree.write();
            for &k in &rand_keys {
                txn.insert(k as usize, k as usize).unwrap();
            }
            txn.commit().unwrap();
        }
        r.push(bench("MvccArt (COW)", n, DUR, || {
            for &k in &lookup {
                black_box(tree.get(&(k as usize)));
            }
        }));
    }

    {
        let map = scc::HashMap::<u64, u64>::new();
        for &k in &rand_keys {
            let _ = map.insert_sync(k, k);
        }
        r.push(bench("scc::HashMap", n, DUR, || {
            for &k in &lookup {
                black_box(map.read_sync(&k, |_, v| *v));
            }
        }));
    }

    print_table("Random Get (100K keys, shuffled lookup)", &r);

    // ===================================================================
    // 5. Range scan (100 elements from midpoint)
    // ===================================================================
    let mut r = Vec::new();
    let range_n = 100u64;

    {
        let mut tree = AdaptiveRadixTree::<ArrayKey<16>, u64>::new(&master);
        for &k in &seq_keys {
            tree.insert(k, k).unwrap();
        }
        let start_key: ArrayKey<16> = (N as u64 / 2).into();
        r.push(bench("ART range(start..)", range_n, DUR, || {
            let mut count = 0usize;
            for item in tree.range(start_key..) {
                black_box(item);
                count += 1;
                if count >= range_n as usize {
                    break;
                }
            }
        }));
    }

    {
        let mut map = BTreeMap::<u64, u64>::new();
        for &k in &seq_keys {
            map.insert(k, k);
        }
        let start_val = N as u64 / 2;
        r.push(bench("BTreeMap range(start..)", range_n, DUR, || {
            let mut count = 0usize;
            for item in map.range(start_val..) {
                black_box(item);
                count += 1;
                if count >= range_n as usize {
                    break;
                }
            }
        }));
    }

    {
        let tree = scc::TreeIndex::<u64, u64>::new();
        for &k in &seq_keys {
            let _ = tree.insert_sync(k, k);
        }
        let start_val = N as u64 / 2;
        r.push(bench("scc::TreeIndex range(start..)", range_n, DUR, || {
            let guard = scc::Guard::new();
            let mut count = 0usize;
            for item in tree.range(start_val.., &guard) {
                black_box(item);
                count += 1;
                if count >= range_n as usize {
                    break;
                }
            }
        }));
    }

    print_table("Range Scan (100 elements from midpoint)", &r);

    // ===================================================================
    // 7. Concurrent reads (1 writer + N readers)
    // ===================================================================
    println!("Concurrent Read (1 writer + 4 readers, 100K keys pre-populated)");
    println!("  Writer inserts 10K new keys while readers do 100K lookups each.\n");

    let num_readers = 4usize;
    let writer_keys: Vec<u64> = (N as u64..N as u64 + 10_000).collect();
    let reader_lookup = seq_keys.clone();

    // --- DashMap: concurrent read/write ---
    {
        let map = std::sync::Arc::new(dashmap::DashMap::<u64, u64>::with_capacity(N));
        for &k in &seq_keys {
            map.insert(k, k);
        }

        let iters = 20u64;
        let start = Instant::now();
        for _ in 0..iters {
            let mut handles = Vec::new();

            for _ in 0..num_readers {
                let map = map.clone();
                let lookup = reader_lookup.clone();
                handles.push(std::thread::spawn(move || {
                    let mut found = 0u64;
                    for &k in &lookup {
                        if map.get(&k).is_some() {
                            found += 1;
                        }
                    }
                    found
                }));
            }

            // Writer
            {
                for &k in &writer_keys {
                    map.insert(k, k);
                }
            }

            for h in handles {
                black_box(h.join().unwrap());
            }
            for &k in &writer_keys {
                map.remove(&k);
            }
        }
        let elapsed = start.elapsed();
        let total_reader_ops = iters * num_readers as u64 * reader_lookup.len() as u64;
        let total_writer_ops = iters * writer_keys.len() as u64;
        let reader_mops = total_reader_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        let writer_mops = total_writer_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        println!("  DashMap (shard-locked concurrent):");
        println!("    Reader throughput: {reader_mops:>8.2} Mops/s ({num_readers} threads)");
        println!("    Writer throughput: {writer_mops:>8.2} Mops/s (1 thread)");
        println!("    Wall time:         {:>8.1} ms\n", elapsed.as_millis());
    }

    // --- papaya::HashMap: concurrent read/write ---
    {
        let map = std::sync::Arc::new(papaya::HashMap::<u64, u64>::new());
        for &k in &seq_keys {
            map.pin().insert(k, k);
        }

        let iters = 20u64;
        let start = Instant::now();
        for _ in 0..iters {
            let mut handles = Vec::new();

            for _ in 0..num_readers {
                let map = map.clone();
                let lookup = reader_lookup.clone();
                handles.push(std::thread::spawn(move || {
                    let mut found = 0u64;
                    let guard = map.guard();
                    for &k in &lookup {
                        if map.get(&k, &guard).is_some() {
                            found += 1;
                        }
                    }
                    found
                }));
            }

            {
                for &k in &writer_keys {
                    map.pin().insert(k, k);
                }
            }

            for h in handles {
                black_box(h.join().unwrap());
            }
            for &k in &writer_keys {
                map.pin().remove(&k);
            }
        }
        let elapsed = start.elapsed();
        let total_reader_ops = iters * num_readers as u64 * reader_lookup.len() as u64;
        let total_writer_ops = iters * writer_keys.len() as u64;
        let reader_mops = total_reader_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        let writer_mops = total_writer_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        println!("  papaya::HashMap (epoch-based concurrent):");
        println!("    Reader throughput: {reader_mops:>8.02} Mops/s ({num_readers} threads)");
        println!("    Writer throughput: {writer_mops:>8.2} Mops/s (1 thread)");
        println!("    Wall time:         {:>8.1} ms\n", elapsed.as_millis());
    }

    // --- scc::TreeIndex: concurrent read/write ---
    {
        let tree = std::sync::Arc::new(scc::TreeIndex::<u64, u64>::new());
        for &k in &seq_keys {
            let _ = tree.insert_sync(k, k);
        }

        let iters = 20u64;
        let start = Instant::now();
        for _ in 0..iters {
            let mut handles = Vec::new();

            for _ in 0..num_readers {
                let tree = tree.clone();
                let lookup = reader_lookup.clone();
                handles.push(std::thread::spawn(move || {
                    let mut found = 0u64;
                    for &k in &lookup {
                        if tree.peek_with(&k, |_, v| *v).is_some() {
                            found += 1;
                        }
                    }
                    found
                }));
            }

            {
                for &k in &writer_keys {
                    let _ = tree.insert_sync(k, k);
                }
            }

            for h in handles {
                black_box(h.join().unwrap());
            }
            for &k in &writer_keys {
                tree.remove_sync(&k);
            }
        }
        let elapsed = start.elapsed();
        let total_reader_ops = iters * num_readers as u64 * reader_lookup.len() as u64;
        let total_writer_ops = iters * writer_keys.len() as u64;
        let reader_mops = total_reader_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        let writer_mops = total_writer_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        println!("  scc::TreeIndex (lock-free B-tree):");
        println!("    Reader throughput: {reader_mops:>8.2} Mops/s ({num_readers} threads)");
        println!("    Writer throughput: {writer_mops:>8.2} Mops/s (1 thread)");
        println!("    Wall time:         {:>8.1} ms\n", elapsed.as_millis());
    }

    // --- PublishedTree: snapshot readers, writer publishes periodically ---
    {
        let mut tree = PublishedTree::<ArrayKey<16>, u64>::new(&master);
        for &k in &seq_keys {
            tree.insert(&k, k).unwrap();
        }
        tree.publish();
        let reader = tree.reader();

        let iters = 20u64;
        let start = Instant::now();
        for _ in 0..iters {
            let mut handles = Vec::new();

            for _ in 0..num_readers {
                let r = reader.clone();
                let lookup = reader_lookup.clone();
                handles.push(std::thread::spawn(move || {
                    let mut found = 0u64;
                    let guard = r.load();
                    for &k in &lookup {
                        if guard.get(&k).is_some() {
                            found += 1;
                        }
                    }
                    found
                }));
            }

            // Writer
            for &k in &writer_keys {
                tree.insert(&k, k).unwrap();
            }
            tree.publish();

            for h in handles {
                black_box(h.join().unwrap());
            }
            for &k in &writer_keys {
                tree.remove(&k).unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total_reader_ops = iters * num_readers as u64 * reader_lookup.len() as u64;
        let total_writer_ops = iters * writer_keys.len() as u64;
        let reader_mops = total_reader_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        let writer_mops = total_writer_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        println!("  PublishedTree (snapshot + arc_swap):");
        println!("    Reader throughput: {reader_mops:>8.2} Mops/s ({num_readers} threads)");
        println!("    Writer throughput: {writer_mops:>8.2} Mops/s (1 thread)");
        println!("    Wall time:         {:>8.1} ms\n", elapsed.as_millis());
    }

    // --- Art (Heap+OLC): concurrent ART with epoch reclamation ---
    {
        let tree = std::sync::Arc::new(ArtRaw::<usize, usize>::new(&master));
        {
            let g = tree.pin();
            for &k in &seq_keys {
                tree.insert(k as usize, k as usize, &g).unwrap();
            }
        }

        let iters = 20u64;
        let start = Instant::now();
        for _ in 0..iters {
            let mut handles = Vec::new();

            for _ in 0..num_readers {
                let tree = tree.clone();
                let lookup = reader_lookup.clone();
                handles.push(std::thread::spawn(move || {
                    let mut found = 0u64;
                    let guard = tree.pin();
                    for &k in &lookup {
                        if tree.get(&(k as usize), &guard).is_some() {
                            found += 1;
                        }
                    }
                    found
                }));
            }

            {
                let guard = tree.pin();
                for &k in &writer_keys {
                    tree.insert(k as usize, k as usize, &guard).unwrap();
                }
            }

            for h in handles {
                black_box(h.join().unwrap());
            }
            {
                let guard = tree.pin();
                for &k in &writer_keys {
                    tree.remove(&(k as usize), &guard);
                }
            }
        }
        let elapsed = start.elapsed();
        let total_reader_ops = iters * num_readers as u64 * reader_lookup.len() as u64;
        let total_writer_ops = iters * writer_keys.len() as u64;
        let reader_mops = total_reader_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        let writer_mops = total_writer_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        println!("  Art (Heap+OLC, epoch-based):");
        println!("    Reader throughput: {reader_mops:>8.2} Mops/s ({num_readers} threads)");
        println!("    Writer throughput: {writer_mops:>8.2} Mops/s (1 thread)");
        println!("    Wall time:         {:>8.1} ms\n", elapsed.as_millis());
    }

    // --- MvccArt: MVCC snapshot readers, writer does batched COW ---
    {
        let tree = std::sync::Arc::new(MvccArt::<usize, usize>::new(&master));
        {
            let mut txn = tree.write();
            for &k in &seq_keys {
                txn.insert(k as usize, k as usize).unwrap();
            }
            txn.commit().unwrap();
        }

        let iters = 20u64;
        let start = Instant::now();
        for _ in 0..iters {
            let mut handles = Vec::new();

            for _ in 0..num_readers {
                let tree = tree.clone();
                let lookup = reader_lookup.clone();
                handles.push(std::thread::spawn(move || {
                    let mut found = 0u64;
                    let snap = tree.snapshot(); // one atomic sequence
                    for &k in &lookup {
                        if snap.get(&(k as usize)).is_some() {
                            found += 1; // zero atomics per read
                        }
                    }
                    found
                }));
            }

            {
                let mut txn = tree.write();
                for &k in &writer_keys {
                    txn.insert(k as usize, k as usize).unwrap();
                }
                txn.commit().unwrap();
            }

            for h in handles {
                black_box(h.join().unwrap());
            }
            {
                let mut txn = tree.write();
                for &k in &writer_keys {
                    txn.remove(&(k as usize)).unwrap();
                }
                txn.commit().unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total_reader_ops = iters * num_readers as u64 * reader_lookup.len() as u64;
        let total_writer_ops = iters * writer_keys.len() as u64;
        let reader_mops = total_reader_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        let writer_mops = total_writer_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        println!("  MvccArt (COW path-copy, MVCC snapshots):");
        println!("    Reader throughput: {reader_mops:>8.2} Mops/s ({num_readers} threads)");
        println!("    Writer throughput: {writer_mops:>8.2} Mops/s (1 thread)");
        println!("    Wall time:         {:>8.1} ms\n", elapsed.as_millis());
    }

    // --- scc::HashMap: concurrent ---
    {
        let map = std::sync::Arc::new(scc::HashMap::<u64, u64>::new());
        for &k in &seq_keys {
            let _ = map.insert_sync(k, k);
        }

        let iters = 20u64;
        let start = Instant::now();
        for _ in 0..iters {
            let mut handles = Vec::new();
            for _ in 0..num_readers {
                let map = map.clone();
                let lookup = reader_lookup.clone();
                handles.push(std::thread::spawn(move || {
                    let mut found = 0u64;
                    for &k in &lookup {
                        if map.read_sync(&k, |_, v| *v).is_some() {
                            found += 1;
                        }
                    }
                    found
                }));
            }
            {
                for &k in &writer_keys {
                    let _ = map.insert_sync(k, k);
                }
            }
            for h in handles {
                black_box(h.join().unwrap());
            }
            for &k in &writer_keys {
                map.remove_sync(&k);
            }
        }
        let elapsed = start.elapsed();
        let total_reader_ops = iters * num_readers as u64 * reader_lookup.len() as u64;
        let total_writer_ops = iters * writer_keys.len() as u64;
        let reader_mops = total_reader_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        let writer_mops = total_writer_ops as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        println!("  scc::HashMap:");
        println!("    Reader throughput: {reader_mops:>8.2} Mops/s ({num_readers} threads)");
        println!("    Writer throughput: {writer_mops:>8.2} Mops/s (1 thread)");
        println!("    Wall time:         {:>8.1} ms\n", elapsed.as_millis());
    }
    println!("Memory usage after: {}", master.memory_usage());
}

// ---------------------------------------------------------------------------
// Pretty printing
// ---------------------------------------------------------------------------

fn print_table(title: &str, results: &[BenchResult]) {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec![
        Cell::new("Structure").add_attribute(Attribute::Bold),
        Cell::new("Mops/s").add_attribute(Attribute::Bold),
        Cell::new("ns/op").add_attribute(Attribute::Bold),
    ]);
    for r in results {
        table.add_row(vec![
            Cell::new(&r.name),
            Cell::new(format!("{:>8.2}", r.mops_per_sec())),
            Cell::new(format!("{:>8.1}", r.ns_per_op())),
        ]);
    }
    println!("{title}");
    println!("{table}\n");
}

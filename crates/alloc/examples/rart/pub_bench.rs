//! Benchmark modelling the bisque-mq AckVariantState use case:
//!
//!   - 1 writer thread (raft async_apply) doing batched mutations
//!   - N reader threads doing point lookups
//!   - Writer publishes after each raft batch
//!
//! Compares: PublishedTree (CowTree) vs Mutex<BTreeMap> + papaya
//!           with both ArrayKey<16> and U64Key key types
//!
//! Run: cargo run --release -p bisque-alloc --example rart-pub-bench

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bisque_alloc::collections::rart::keys::array_key::ArrayKey;
use bisque_alloc::collections::rart::keys::u64_key::U64Key;
use bisque_alloc::collections::rart::published_tree::PublishedTree;
use bisque_alloc::{HeapMaster, MiMalloc};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const HEAP_SIZE: usize = 512 * 1024 * 1024;
const NUM_READERS: usize = 4;
const BENCH_DURATION: Duration = Duration::from_secs(3);

/// Simulated AckMessageMeta: ~96 bytes like the real struct.
#[derive(Clone, Copy)]
#[repr(C)]
struct Meta {
    message_id: u64,
    group_id: u64,
    state: u8,
    priority: u8,
    _pad: [u8; 6],
    deliver_after: u64,
    attempts: u32,
    _pad2: u32,
    last_delivered_at: u64,
    consumer_id: u64,
    visibility_deadline: u64,
    expires_at: u64,
    value_len: u32,
    publisher_id: u32,
    _extra: [u8; 16],
}

impl Meta {
    fn new(id: u64) -> Self {
        Self {
            message_id: id,
            group_id: 1,
            state: 0,
            priority: 0,
            _pad: [0; 6],
            deliver_after: 0,
            attempts: 0,
            _pad2: 0,
            last_delivered_at: 0,
            consumer_id: 0,
            visibility_deadline: 0,
            expires_at: 0,
            value_len: 256,
            publisher_id: 0,
            _extra: [0; 16],
        }
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

struct BenchResult {
    name: &'static str,
    reader_mops: f64,
    writer_mops: f64,
    extra: String,
}

fn main() {
    println!("=== bisque-mq AckVariantState model benchmark ===");
    println!(
        "  {} reader threads, 1 writer thread, {:.0}s per scenario",
        NUM_READERS,
        BENCH_DURATION.as_secs_f64()
    );
    println!("  Value type: Meta (~96 bytes)\n");

    let existing_count = 100_000u64;
    let existing_ids: Vec<u64> = {
        let mut rng = Rng::new(0xBEEF);
        (0..existing_count).map(|_| rng.next_u64()).collect()
    };

    let mut lookup_keys = existing_ids.clone();
    {
        let mut rng = Rng::new(0xCAFE);
        for i in (1..lookup_keys.len()).rev() {
            let j = rng.next_u64() as usize % (i + 1);
            lookup_keys.swap(i, j);
        }
    }

    let batch_sizes = [100, 1_000, 10_000, 100_000];

    for &batch_size in &batch_sizes {
        println!("─── Raft batch size: {batch_size} inserts per publish ───\n");

        let mut results = Vec::new();

        results.push(bench_published_tree_arraykey(
            &existing_ids,
            &lookup_keys,
            batch_size,
        ));
        results.push(bench_published_tree_u64key(
            &existing_ids,
            &lookup_keys,
            batch_size,
        ));
        results.push(bench_mutex_btree_papaya(
            &existing_ids,
            &lookup_keys,
            batch_size,
        ));
        results.push(bench_scc_treeindex(&existing_ids, &lookup_keys, batch_size));
        results.push(bench_congee(&existing_ids, &lookup_keys, batch_size));

        for r in &results {
            println!("  {}:", r.name);
            println!(
                "    Reader: {:>8.2} Mops/s  ({NUM_READERS} threads)",
                r.reader_mops
            );
            println!("    Writer: {:>8.2} Mops/s  {}", r.writer_mops, r.extra);
        }
        println!();
    }
}

fn bench_published_tree_arraykey(existing: &[u64], lookup: &[u64], batch_size: u64) -> BenchResult {
    let master = HeapMaster::new(HEAP_SIZE).unwrap();
    let mut tree = PublishedTree::<ArrayKey<16>, Meta>::new(&master);
    for &id in existing {
        tree.insert(id, Meta::new(id)).unwrap();
    }
    tree.publish();

    let reader = tree.reader();
    let done = Arc::new(AtomicBool::new(false));
    let total_reader_ops = Arc::new(AtomicU64::new(0));
    let total_writer_ops = Arc::new(AtomicU64::new(0));
    let total_publishes = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..NUM_READERS {
        let r = reader.clone();
        let keys = lookup.to_vec();
        let done = done.clone();
        let ops = total_reader_ops.clone();
        handles.push(std::thread::spawn(move || {
            let mut count = 0u64;
            while !done.load(Ordering::Relaxed) {
                let guard = r.load();
                for &k in &keys {
                    black_box(guard.get(k));
                    count += 1;
                }
            }
            ops.fetch_add(count, Ordering::Relaxed);
        }));
    }

    let wd = done.clone();
    let wops = total_writer_ops.clone();
    let wpub = total_publishes.clone();
    let writer = std::thread::spawn(move || {
        let mut next_id = u64::MAX / 2;
        let mut inserts = 0u64;
        while !wd.load(Ordering::Relaxed) {
            for i in 0..batch_size {
                tree.insert(next_id + i, Meta::new(next_id + i)).unwrap();
                inserts += 1;
            }
            tree.publish();
            for i in 0..batch_size {
                tree.remove(next_id + i).unwrap();
            }
            next_id += batch_size;
            wpub.fetch_add(1, Ordering::Relaxed);
        }
        wops.store(inserts, Ordering::Relaxed);
    });

    std::thread::sleep(BENCH_DURATION);
    done.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    writer.join().unwrap();

    let pubs = total_publishes.load(Ordering::Relaxed);
    BenchResult {
        name: "PublishedTree<ArrayKey<16>>",
        reader_mops: total_reader_ops.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        writer_mops: total_writer_ops.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        extra: format!("({:.0} pub/s)", pubs as f64 / BENCH_DURATION.as_secs_f64()),
    }
}

fn bench_published_tree_u64key(existing: &[u64], lookup: &[u64], batch_size: u64) -> BenchResult {
    let master = HeapMaster::new(HEAP_SIZE).unwrap();
    let mut tree = PublishedTree::<U64Key, Meta>::new(&master);
    for &id in existing {
        tree.insert(id, Meta::new(id)).unwrap();
    }
    tree.publish();

    let reader = tree.reader();
    let done = Arc::new(AtomicBool::new(false));
    let total_reader_ops = Arc::new(AtomicU64::new(0));
    let total_writer_ops = Arc::new(AtomicU64::new(0));
    let total_publishes = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..NUM_READERS {
        let r = reader.clone();
        let keys = lookup.to_vec();
        let done = done.clone();
        let ops = total_reader_ops.clone();
        handles.push(std::thread::spawn(move || {
            let mut count = 0u64;
            while !done.load(Ordering::Relaxed) {
                let guard = r.load();
                for &k in &keys {
                    black_box(guard.get(k));
                    count += 1;
                }
            }
            ops.fetch_add(count, Ordering::Relaxed);
        }));
    }

    let wd = done.clone();
    let wops = total_writer_ops.clone();
    let wpub = total_publishes.clone();
    let writer = std::thread::spawn(move || {
        let mut next_id = u64::MAX / 2;
        let mut inserts = 0u64;
        while !wd.load(Ordering::Relaxed) {
            for i in 0..batch_size {
                tree.insert(next_id + i, Meta::new(next_id + i)).unwrap();
                inserts += 1;
            }
            tree.publish();
            for i in 0..batch_size {
                tree.remove(next_id + i).unwrap();
            }
            next_id += batch_size;
            wpub.fetch_add(1, Ordering::Relaxed);
        }
        wops.store(inserts, Ordering::Relaxed);
    });

    std::thread::sleep(BENCH_DURATION);
    done.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    writer.join().unwrap();

    let pubs = total_publishes.load(Ordering::Relaxed);
    BenchResult {
        name: "PublishedTree<U64Key>",
        reader_mops: total_reader_ops.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        writer_mops: total_writer_ops.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        extra: format!("({:.0} pub/s)", pubs as f64 / BENCH_DURATION.as_secs_f64()),
    }
}

fn bench_mutex_btree_papaya(existing: &[u64], lookup: &[u64], batch_size: u64) -> BenchResult {
    let messages = Arc::new(papaya::HashMap::<u64, Meta>::new());
    let pending = Arc::new(Mutex::new(BTreeMap::<u64, ()>::new()));
    for &id in existing {
        messages.pin().insert(id, Meta::new(id));
        pending.lock().unwrap().insert(id, ());
    }

    let done = Arc::new(AtomicBool::new(false));
    let total_reader_ops = Arc::new(AtomicU64::new(0));
    let total_writer_ops = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..NUM_READERS {
        let msgs = messages.clone();
        let pend = pending.clone();
        let keys = lookup.to_vec();
        let done = done.clone();
        let ops = total_reader_ops.clone();
        handles.push(std::thread::spawn(move || {
            let mut count = 0u64;
            while !done.load(Ordering::Relaxed) {
                let guard = msgs.guard();
                for &k in &keys {
                    black_box(msgs.get(&k, &guard));
                    count += 1;
                }
                {
                    let lock = pend.lock().unwrap();
                    for e in lock.iter().take(10) {
                        black_box(e);
                        count += 1;
                    }
                }
            }
            ops.fetch_add(count, Ordering::Relaxed);
        }));
    }

    let wd = done.clone();
    let wops = total_writer_ops.clone();
    let wmsgs = messages.clone();
    let wpend = pending.clone();
    let writer = std::thread::spawn(move || {
        let mut next_id = u64::MAX / 2;
        let mut inserts = 0u64;
        while !wd.load(Ordering::Relaxed) {
            for i in 0..batch_size {
                let id = next_id + i;
                wmsgs.pin().insert(id, Meta::new(id));
                wpend.lock().unwrap().insert(id, ());
                inserts += 1;
            }
            for i in 0..batch_size {
                let id = next_id + i;
                wmsgs.pin().remove(&id);
                wpend.lock().unwrap().remove(&id);
            }
            next_id += batch_size;
        }
        wops.store(inserts, Ordering::Relaxed);
    });

    std::thread::sleep(BENCH_DURATION);
    done.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    writer.join().unwrap();

    BenchResult {
        name: "papaya + Mutex<BTreeMap>",
        reader_mops: total_reader_ops.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        writer_mops: total_writer_ops.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        extra: "(locks per op)".into(),
    }
}

fn bench_scc_treeindex(existing: &[u64], lookup: &[u64], batch_size: u64) -> BenchResult {
    let tree = Arc::new(scc::TreeIndex::<u64, Meta>::new());
    for &id in existing {
        let _ = tree.insert_sync(id, Meta::new(id));
    }

    let done = Arc::new(AtomicBool::new(false));
    let total_reader_ops = Arc::new(AtomicU64::new(0));
    let total_writer_ops = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..NUM_READERS {
        let tree = tree.clone();
        let keys = lookup.to_vec();
        let done = done.clone();
        let ops = total_reader_ops.clone();
        handles.push(std::thread::spawn(move || {
            let mut count = 0u64;
            while !done.load(Ordering::Relaxed) {
                for &k in &keys {
                    black_box(tree.peek_with(&k, |_, m| m.message_id));
                    count += 1;
                }
            }
            ops.fetch_add(count, Ordering::Relaxed);
        }));
    }

    let wd = done.clone();
    let wops = total_writer_ops.clone();
    let wtree = tree.clone();
    let writer = std::thread::spawn(move || {
        let mut next_id = u64::MAX / 2;
        let mut inserts = 0u64;
        while !wd.load(Ordering::Relaxed) {
            for i in 0..batch_size {
                let _ = wtree.insert_sync(next_id + i, Meta::new(next_id + i));
                inserts += 1;
            }
            for i in 0..batch_size {
                wtree.remove_sync(&(next_id + i));
            }
            next_id += batch_size;
        }
        wops.store(inserts, Ordering::Relaxed);
    });

    std::thread::sleep(BENCH_DURATION);
    done.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    writer.join().unwrap();

    BenchResult {
        name: "scc::TreeIndex",
        reader_mops: total_reader_ops.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        writer_mops: total_writer_ops.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        extra: "(lock-free B-tree)".into(),
    }
}

fn bench_congee(existing: &[u64], lookup: &[u64], batch_size: u64) -> BenchResult {
    use bisque_alloc::collections::art::Art;

    let master = HeapMaster::new(HEAP_SIZE).unwrap();
    let tree = Arc::new(Art::<usize, usize>::new(&master));
    {
        let g = tree.pin();
        for &id in existing {
            let _ = tree.insert(id as usize, id as usize, &g);
        }
    }

    let done = Arc::new(AtomicBool::new(false));
    let total_reader_ops = Arc::new(AtomicU64::new(0));
    let total_writer_ops = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..NUM_READERS {
        let tree = tree.clone();
        let keys = lookup.to_vec();
        let done = done.clone();
        let ops = total_reader_ops.clone();
        handles.push(std::thread::spawn(move || {
            let mut count = 0u64;
            while !done.load(Ordering::Relaxed) {
                let g = tree.pin();
                for &k in &keys {
                    black_box(tree.get(&(k as usize), &g));
                    count += 1;
                }
            }
            ops.fetch_add(count, Ordering::Relaxed);
        }));
    }

    let wd = done.clone();
    let wops = total_writer_ops.clone();
    let wtree = tree.clone();
    let writer = std::thread::spawn(move || {
        let mut next_id = u64::MAX / 2;
        let mut inserts = 0u64;
        while !wd.load(Ordering::Relaxed) {
            let g = wtree.pin();
            for i in 0..batch_size {
                let key = (next_id + i) as usize;
                let _ = wtree.insert(key, key, &g);
                inserts += 1;
            }
            for i in 0..batch_size {
                let key = (next_id + i) as usize;
                wtree.remove(&key, &g);
            }
            next_id += batch_size;
        }
        wops.store(inserts, Ordering::Relaxed);
    });

    std::thread::sleep(BENCH_DURATION);
    done.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    writer.join().unwrap();

    BenchResult {
        name: "Art (ART-OLC)",
        reader_mops: total_reader_ops.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        writer_mops: total_writer_ops.load(Ordering::Relaxed) as f64
            / BENCH_DURATION.as_secs_f64()
            / 1e6,
        extra: "(heap-backed, u64 key+val)".into(),
    }
}

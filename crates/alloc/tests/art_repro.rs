use bisque_alloc::HeapMaster;

/// Test 1: HeapMaster lifecycle only (no Art tree)
#[test]
fn heap_lifecycle_leak() {
    for round in 0..30_000 {
        let master = HeapMaster::new(4 * 1024 * 1024).unwrap();
        let heap = master.heap();
        // Allocate and free some memory
        let p = heap.alloc(1024, 8);
        if !p.is_null() {
            unsafe {
                heap.dealloc(p);
            }
        }
        drop(heap);
        drop(master);
        if round % 5000 == 0 {
            let rss = get_rss_mb();
            eprintln!("[heap_only] round {round}: RSS={rss}MB");
        }
    }
}

/// Test 2: Art tree without background threads
#[test]
fn art_single_thread_leak() {
    for round in 0..30_000 {
        let master = HeapMaster::new(4 * 1024 * 1024).unwrap();
        let tree = bisque_alloc::collections::art::Art::<usize, usize>::new(&master);
        let g = tree.pin();
        for k in 0..100usize {
            let _ = tree.insert(k, k, &g);
        }
        for k in 0..50usize {
            tree.remove(&k, &g);
        }
        drop(g);
        drop(tree);
        drop(master);
        if round % 5000 == 0 {
            let rss = get_rss_mb();
            eprintln!("[art_single] round {round}: RSS={rss}MB");
        }
    }
}

/// Test 3: Art tree WITH background writer thread
#[test]
fn art_concurrent_leak() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    for round in 0..30_000 {
        let master = HeapMaster::new(4 * 1024 * 1024).unwrap();
        let tree = Arc::new(bisque_alloc::collections::art::Art::<usize, usize>::new(
            &master,
        ));
        let done = Arc::new(AtomicBool::new(false));

        let tree2 = tree.clone();
        let done2 = done.clone();
        let writer = std::thread::spawn(move || {
            let mut i = 0usize;
            while !done2.load(Ordering::Relaxed) {
                let g = tree2.pin();
                for _ in 0..64 {
                    let k = i % 200;
                    if i % 2 == 0 {
                        let _ = tree2.insert(k, k, &g);
                    } else {
                        tree2.remove(&k, &g);
                    }
                    i += 1;
                }
            }
        });

        let g = tree.pin();
        for k in 0..200usize {
            let _ = tree.insert(k, k, &g);
        }
        for k in 0..100usize {
            tree.remove(&k, &g);
        }
        drop(g);

        done.store(true, Ordering::Relaxed);
        writer.join().unwrap();
        drop(tree);
        drop(master);

        if round % 5000 == 0 {
            let rss = get_rss_mb();
            eprintln!("[art_concurrent] round {round}: RSS={rss}MB");
        }
    }
}

fn get_rss_mb() -> usize {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse::<usize>().ok())
        })
        .unwrap_or(0)
        / 1024
}

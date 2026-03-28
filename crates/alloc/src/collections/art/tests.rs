#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::Heap;
    use crate::HeapMaster;
    use crate::collections::art::epoch::*;
    use crate::collections::art::node::*;
    use crate::collections::art::sharded::*;
    use crate::collections::art::*;

    fn make_heap() -> Heap {
        HeapMaster::new(256 * 1024 * 1024).unwrap().heap()
    }

    fn make_collector() -> Collector {
        Collector::new()
    }

    #[test]
    fn empty_tree() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        assert!(t.is_empty());
        assert_eq!(t.get(&42), None);
    }

    #[test]
    fn single_insert_get() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        assert_eq!(t.insert(42, 100).unwrap(), None);
        assert_eq!(t.get(&42), Some(100));
        assert_eq!(t.get(&99), None);
    }

    #[test]
    fn replace() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        assert_eq!(t.insert(1, 10).unwrap(), None);
        assert_eq!(t.insert(1, 20).unwrap(), Some(10));
        assert_eq!(t.get(&1), Some(20));
    }

    #[test]
    fn remove() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        t.insert(1, 10).unwrap();
        assert_eq!(t.remove(&1).unwrap(), Some(10));
        assert_eq!(t.get(&1), None);
    }

    #[test]
    fn many_keys() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in 0..10_000usize {
            t.insert(i, i).unwrap();
        }
        for i in 0..10_000usize {
            assert_eq!(t.get(&i), Some(i), "missing {i}");
        }
    }

    #[test]
    fn transaction_batch() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        {
            let mut txn = t.write();
            for i in 0..100usize {
                txn.insert(i, i * 10).unwrap();
            }
            txn.commit().unwrap(); // publishes
        }
        for i in 0..100usize {
            assert_eq!(t.get(&i), Some(i * 10), "missing {i}");
        }
    }

    #[test]
    fn snapshot_sees_writes() {
        // Writes are in-place — snapshot sees current state.
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        t.insert(1, 10).unwrap();

        let snap = t.read();
        assert_eq!(snap.get(&1), Some(10));

        t.insert(1, 20).unwrap();
        t.insert(2, 30).unwrap();

        // Direct reads see the latest.
        assert_eq!(t.get(&1), Some(20));
        assert_eq!(t.get(&2), Some(30));
    }

    #[test]
    fn concurrent_readers() {
        use std::sync::Arc as StdArc;

        let h = make_heap();
        let c = make_collector();
        let t = StdArc::new(Art::<usize, usize>::new(&c, &h));

        for i in 0..1000usize {
            t.insert(i, i).unwrap();
        }

        let r = &t;
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    let snap = r.read();
                    for i in 0..1000usize {
                        assert_eq!(snap.get(&i), Some(i));
                    }
                });
            }
            s.spawn(|| {
                let mut txn = r.write();
                for i in 1000..2000usize {
                    txn.insert(i, i).unwrap();
                }
                txn.commit().unwrap();
            });
        });

        for i in 0..2000usize {
            assert_eq!(t.get(&i), Some(i), "missing {i}");
        }
    }

    #[test]
    fn no_memory_leak() {
        let h = make_heap();
        let c = make_collector();
        let before = h.memory_usage();
        {
            let t = Art::<usize, usize>::new(&c, &h);
            for i in 0..10_000usize {
                t.insert(i, i).unwrap();
            }
        }
        h.collect(true);
        let after = h.memory_usage();
        assert!(
            after <= before + 4096,
            "memory leak: before={before}, after={after}"
        );
    }

    #[test]
    fn random_keys() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        let mut rng = 0xDEAD_BEEFu64;
        let mut keys = Vec::new();
        for _ in 0..10_000 {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let k = rng as usize;
            t.insert(k, k).unwrap();
            keys.push(k);
        }
        for &k in &keys {
            assert_eq!(t.get(&k), Some(k), "missing {k}");
        }
    }

    #[test]
    fn range_iter_basic() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in 0..100usize {
            t.insert(i, i * 10).unwrap();
        }
        let snap = t.read();
        let items: Vec<(usize, usize)> = snap.range_iter(&50, &59).collect();
        assert_eq!(items.len(), 10);
        for (i, &(k, v)) in items.iter().enumerate() {
            assert_eq!(k, 50 + i);
            assert_eq!(v, (50 + i) * 10);
        }
    }

    #[test]
    fn range_iter_empty() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in 0..100usize {
            t.insert(i, i).unwrap();
        }
        let snap = t.read();
        let items: Vec<(usize, usize)> = snap.range_iter(&200, &300).collect();
        assert_eq!(items.len(), 0);
    }

    #[test]
    fn range_iter_full() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in 0..1000usize {
            t.insert(i, i).unwrap();
        }
        let snap = t.read();
        let items: Vec<(usize, usize)> = snap.range_iter(&0, &999).collect();
        assert_eq!(items.len(), 1000);
        // Verify sorted order.
        for i in 1..items.len() {
            assert!(items[i].0 > items[i - 1].0, "not sorted at index {i}");
        }
    }

    #[test]
    fn prefix_compression_sequential() {
        // Sequential keys share long prefixes in big-endian form.
        // Keys 0..255 share 7 zero-byte prefix, differ only in the last byte.
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in 0..256usize {
            t.insert(i, i).unwrap();
        }
        for i in 0..256usize {
            assert_eq!(t.get(&i), Some(i), "missing {i}");
        }
        // Also verify range scan works with compressed prefixes.
        let snap = t.read();
        let items: Vec<(usize, usize)> = snap.range_iter(&100, &200).collect();
        assert_eq!(items.len(), 101);
        assert_eq!(items[0].0, 100);
        assert_eq!(items[100].0, 200);
    }

    // ── Concurrent API tests ─────────────────────────────────────────

    #[test]
    fn concurrent_put_publish() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);

        // Put entries — not visible until publish.
        for i in 0..100usize {
            t.insert(i, i * 10).unwrap();
        }
        // Publish makes them visible.
        for i in 0..100usize {
            assert_eq!(t.get(&i), Some(i * 10), "missing {i}");
        }
    }

    #[test]
    fn concurrent_delete() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        t.insert(1, 10).unwrap();
        t.insert(2, 20).unwrap();

        t.remove(&1).unwrap();

        assert_eq!(t.get(&1), None);
        assert_eq!(t.get(&2), Some(20));
    }

    #[test]
    fn compute_if_present() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        t.insert(1, 10).unwrap();

        let mut w = t.write();
        let result = w.compute_if_present(&1, |v| Some(v * 2)).unwrap();
        assert_eq!(result, Some((10, Some(20))));
        assert_eq!(w.get(&1), Some(20));

        let result = w.compute_if_present(&99, |v| Some(v * 2)).unwrap();
        assert_eq!(result, None);
        w.publish().unwrap();
    }

    #[test]
    fn compute_or_insert() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);

        let mut w = t.write();
        let v = w.compute_or_insert(1, || 42).unwrap();
        assert_eq!(v, 42);

        let v = w
            .compute_or_insert(1, || panic!("should not be called"))
            .unwrap();
        assert_eq!(v, 42);
        w.publish().unwrap();
    }

    #[test]
    fn compare_exchange() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        t.insert(1, 10).unwrap();

        let mut w = t.write();
        let r = w.compare_exchange(&1, 10, Some(20));
        assert_eq!(r, Ok(Some(10)));
        assert_eq!(w.get(&1), Some(20));

        let r = w.compare_exchange(&1, 10, Some(30));
        assert_eq!(r, Err(Some(20)));
        w.publish().unwrap();
    }

    #[test]
    fn concurrent_multi_thread_put() {
        use std::sync::Arc as StdArc;

        let h = make_heap();
        let c = make_collector();
        let t = StdArc::new(Art::<usize, usize>::new(&c, &h));

        // 4 threads each insert 1000 keys concurrently.
        std::thread::scope(|s| {
            for tid in 0..4u64 {
                let t = &t;
                s.spawn(move || {
                    let base = tid as usize * 1000;
                    for i in 0..1000usize {
                        t.insert(base + i, base + i).unwrap();
                    }
                });
            }
        });

        // All 4000 keys present.
        for i in 0..4000usize {
            assert_eq!(t.get(&i), Some(i), "missing {i}");
        }
    }

    #[test]
    fn concurrent_readers_with_publish() {
        use std::sync::Arc as StdArc;

        let h = make_heap();
        let c = make_collector();
        let t = StdArc::new(Art::<usize, usize>::new(&c, &h));

        // Pre-populate via concurrent API + publish.
        for i in 0..1000usize {
            t.insert(i, i).unwrap();
        }

        // Readers + writer concurrently.
        let r = &t;
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    let snap = r.read();
                    for i in 0..1000usize {
                        assert_eq!(snap.get(&i), Some(i));
                    }
                });
            }
            // Writer adds more via concurrent API.
            s.spawn(|| {
                for i in 1000..2000usize {
                    r.insert(i, i).unwrap();
                }
            });
        });
        for i in 0..2000usize {
            assert_eq!(t.get(&i), Some(i), "missing {i}");
        }
    }

    // ── ShardedArt tests ────────────────────────────────────────────

    fn make_sharded_master() -> crate::HeapMaster {
        HeapMaster::new(1024 * 1024 * 1024).unwrap()
    }

    #[test]
    fn sharded_basic() {
        let m = make_sharded_master();
        let h = m.heap();
        let t = ShardedArt::<usize>::new(&h);
        assert!(t.is_empty());
        t.put(42, 100, &h).unwrap();
        t.publish().unwrap();
        assert_eq!(t.get(42), Some(100));
        assert_eq!(t.get(99), None);
    }

    #[test]
    fn sharded_many_keys() {
        let m = make_sharded_master();
        let h = m.heap();
        let t = ShardedArt::<usize>::new(&h);
        for i in 0..10_000usize {
            t.put(i, i, &h).unwrap();
        t.publish().unwrap();
        }
        for i in 0..10_000usize {
            assert_eq!(t.get(i), Some(i), "missing {i}");
        }
    }

    #[test]
    fn sharded_random_keys() {
        let m = make_sharded_master();
        let h = m.heap();
        let t = ShardedArt::<usize>::new(&h);
        let mut rng = 0xDEAD_BEEFu64;
        let mut keys = Vec::new();
        for _ in 0..10_000 {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let k = rng as usize;
            t.put(k, k, &h).unwrap();
            keys.push(k);
        }
        t.publish().unwrap();
        for &k in &keys {
            assert_eq!(t.get(k), Some(k), "missing {k}");
        }
    }

    #[test]
    fn sharded_delete() {
        let m = make_sharded_master();
        let h = m.heap();
        let t = ShardedArt::<usize>::new(&h);
        t.put(1, 10, &h).unwrap();
        t.put(2, 20, &h).unwrap();
 t.publish().unwrap();

        t.delete(1, &h).unwrap();
        t.publish().unwrap();
        assert_eq!(t.get(1), None);
        assert_eq!(t.get(2), Some(20));
    }

    #[test]
    fn sharded_compute_or_insert_dedup() {
        let m = make_sharded_master();
        let h = m.heap();
        let t = ShardedArt::<usize>::new(&h);

        // First insert.
        let v = t.compute_or_insert(42, || 100, &h).unwrap();
        t.publish().unwrap();
        assert_eq!(v, 100);

        // Duplicate — returns existing, doesn't call f.
        let v = t
            .compute_or_insert(42, || panic!("should not call"), &h)
            .unwrap();
        assert_eq!(v, 100);
    }

    #[test]
    fn sharded_publish_makes_batch_visible() {
        let m = make_sharded_master();
        let h = m.heap();
        let t = ShardedArt::<usize>::new(&h);

        // Keys with different first bytes go to different shards.
        let ka = 1usize << 56; // first byte = 0x01
        let kb = 2usize << 56; // first byte = 0x02

        t.put(ka, 10, &h).unwrap();
        t.put(kb, 20, &h).unwrap();

        // Not visible yet (not published).
        assert_eq!(t.get(ka), None);
        assert_eq!(t.get(kb), None);

        t.publish().unwrap();
        assert_eq!(t.get(ka), Some(10));
        assert_eq!(t.get(kb), Some(20));
    }

    #[test]
    fn sharded_multi_thread_dedup() {
        use std::sync::Arc as StdArc;

        let m = make_sharded_master();
        let h = m.heap();
        let t = StdArc::new(ShardedArt::<usize>::new(&h));

        // 8 threads each insert 1000 keys with random distribution.
        // Each thread creates its own per-thread Heap for safe allocation.
        let num_threads = 8;
        let keys_per_thread = 1000;

        std::thread::scope(|s| {
            for tid in 0..num_threads {
                let t = &t;
                let th = m.heap(); // per-thread heap
                s.spawn(move || {
                    let mut rng = (tid as u64 + 1) * 0xDEAD_BEEF;
                    for _ in 0..keys_per_thread {
                        rng = rng
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        let key = rng as usize;
                        t.compute_or_insert(key, || key, &th).unwrap();
                    t.publish().unwrap();
                    }
                });
            }
        });
        eprintln!("[DEBUG] all threads joined, calling publish...");
        eprintln!("[DEBUG] publish done, verifying...");

        // Verify all keys are present.
        for tid in 0..num_threads {
            let mut rng = (tid as u64 + 1) * 0xDEAD_BEEF;
            for _ in 0..keys_per_thread {
                rng = rng
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let key = rng as usize;
                assert!(t.get(key).is_some(), "missing key from thread {tid}");
            }
        }
    }

    #[test]
    fn sharded_concurrent_readers_writers() {
        use std::sync::Arc as StdArc;

        let m = make_sharded_master();
        let h = m.heap();
        let t = StdArc::new(ShardedArt::<usize>::new(&h));

        // Pre-populate and publish.
        for i in 0..1000usize {
            t.put(i, i, &h).unwrap();
        t.publish().unwrap();
        }

        // 4 readers + 4 writers concurrently.
        std::thread::scope(|s| {
            for _ in 0..4 {
                let t = &t;
                s.spawn(move || {
                    for i in 0..1000usize {
                        assert_eq!(t.get(i), Some(i));
                    }
                });
            }
            for tid in 0..4u64 {
                let t = &t;
                let th = m.heap(); // per-thread heap
                s.spawn(move || {
                    let base = 1000 + tid as usize * 250;
                    for i in 0..250usize {
                        t.put(base + i, base + i, &th).unwrap();
                    t.publish().unwrap();
                    }
                });
            }
        });
        for i in 0..2000usize {
            assert_eq!(t.get(i), Some(i), "missing {i}");
        }
    }

    // ─── Epoch tests ───────────────────────────────────────────────

    /// Helper: retire a single node via a one-element Vec.
    fn retire_one(te: &Epoch, h: &Heap, node: usize) {
        let mut nodes = crate::Vec::new(h);
        nodes.push(node).unwrap();
        te.retire(nodes);
    }

    #[test]
    fn epoch_pin_unpin() {
        let h = make_heap();
        let c = make_collector();
        let te = new_art_epoch(&c, &h);
        assert_eq!(te.current(), 1);

        let guard = te.pin();
        assert_eq!(guard.epoch(), 1);
        assert_eq!(te.readers_at(1), 1);
        drop(guard);
    }

    #[test]
    fn epoch_advance() {
        let h = make_heap();
        let c = make_collector();
        let te = new_art_epoch(&c, &h);
        assert_eq!(te.advance(), 2);
        assert_eq!(te.advance(), 3);
        assert_eq!(te.current(), 3);
    }

    #[test]
    fn epoch_retire_reclaim() {
        let h = make_heap();
        let c = make_collector();
        let te = new_art_epoch(&c, &h);

        // Allocate a leaf and retire it.
        let leaf = unsafe { alloc_leaf(&h, 42, 100) };
        assert_ne!(leaf, NULL_CHILD);
        retire_one(&te, &h, leaf);
        assert_eq!(te.garbage_count(), 1);

        // With no pins, advance should reclaim it.
        te.advance();
        assert_eq!(te.garbage_count(), 0);
    }

    #[test]
    fn epoch_pinned_prevents_reclaim() {
        let h = make_heap();
        let c = make_collector();
        let te = new_art_epoch(&c, &h);

        // Pin at epoch 1.
        let guard = te.pin();

        // Retire a node at epoch 1.
        let leaf = unsafe { alloc_leaf(&h, 42, 100) };
        retire_one(&te, &h, leaf);

        // Advance to epoch 2 — should NOT reclaim (guard pins epoch 1).
        te.advance();
        assert_eq!(te.garbage_count(), 1);

        // Drop the guard — now epoch 1 garbage is reclaimable.
        // Guard drop triggers try_reclaim.
        drop(guard);
        assert_eq!(te.garbage_count(), 0);
    }

    #[test]
    fn epoch_multiple_generations() {
        let h = make_heap();
        let c = make_collector();
        let te = new_art_epoch(&c, &h);

        // Epoch 1: retire a node.
        let leaf1 = unsafe { alloc_leaf(&h, 1, 10) };
        retire_one(&te, &h, leaf1);

        // Advance to epoch 2, pin at 2.
        te.advance();
        let guard2 = te.pin();

        // Epoch 2: retire another node.
        let leaf2 = unsafe { alloc_leaf(&h, 2, 20) };
        retire_one(&te, &h, leaf2);

        // Advance to epoch 3 — epoch 1 garbage freed, epoch 2 retained (pinned).
        te.advance();
        // Only epoch 2's garbage should remain (1 node).
        assert_eq!(te.garbage_count(), 1);

        // Drop pin — epoch 2 garbage now reclaimable.
        // Guard drop triggers try_reclaim.
        drop(guard2);
        assert_eq!(te.garbage_count(), 0);
    }

    #[test]
    fn epoch_concurrent_pins() {
        use std::sync::Arc as StdArc;
        let h = make_heap();
        let c = make_collector();
        let te = StdArc::new(new_art_epoch(&c, &h));

        // Pins from different threads write to their thread-local slots.
        // Use a barrier to ensure both are pinned before checking.
        use std::sync::Barrier;
        let barrier = StdArc::new(Barrier::new(3));
        std::thread::scope(|s| {
            let te1 = te.clone();
            let b1 = barrier.clone();
            s.spawn(move || {
                let _g = te1.pin();
                b1.wait(); // signal: pinned
                b1.wait(); // wait: checker done
            });
            let te2 = te.clone();
            let b2 = barrier.clone();
            s.spawn(move || {
                let _g = te2.pin();
                b2.wait();
                b2.wait();
            });
            barrier.wait(); // both threads pinned
            assert_eq!(te.readers_at(1), 2);
            barrier.wait(); // release threads
        });
    }

    #[test]
    fn epoch_concurrent_threads() {
        use std::sync::Arc as StdArc;

        let h = make_heap();
        let c = make_collector();
        let te = StdArc::new(new_art_epoch(&c, &h));

        // Spawn threads that pin, retire, advance concurrently.
        std::thread::scope(|s| {
            for _ in 0..8 {
                let te = &te;
                let h = &h;
                s.spawn(move || {
                    for _ in 0..1000 {
                        let _guard = te.pin();
                        // Allocate and immediately retire a leaf.
                        let leaf = unsafe { alloc_leaf(h, 0xDEAD, 0xBEEF) };
                        if leaf != NULL_CHILD {
                            retire_one(te, h, leaf);
                        }
                    }
                });
            }
            // One thread advances the epoch periodically.
            let te = &te;
            s.spawn(move || {
                for _ in 0..100 {
                    te.advance();
                    std::thread::yield_now();
                }
            });
        });

        // Final advance with no pins — all garbage becomes reclaimable.
        te.advance();
        assert_eq!(te.garbage_count(), 0);
    }

    // ─── OLC guard tests ───────────────────────────────────────────────

    #[test]
    fn olc_read_guard() {
        let h = make_heap();
        let c = make_collector();
        let node = unsafe { alloc_node4(&h, 1) } as usize;

        let guard = NodeReadGuard::try_read(node).unwrap();
        assert_eq!(guard.header().kind, NodeKind::N4);
        assert!(guard.check().is_ok());

        unsafe { dealloc_node(&h, node) };
    }

    #[test]
    fn olc_write_guard_blocks_read() {
        let h = make_heap();
        let c = make_collector();
        let node = unsafe { alloc_node4(&h, 1) } as usize;

        let wg = NodeWriteGuard::try_lock(node).unwrap();
        // While write-locked, read should fail.
        assert!(NodeReadGuard::try_read(node).is_err());
        drop(wg);
        // After unlock, read succeeds but with bumped version.
        let rg = NodeReadGuard::try_read(node).unwrap();
        assert!(rg.check().is_ok());

        unsafe { dealloc_node(&h, node) };
    }

    #[test]
    fn olc_upgrade() {
        let h = make_heap();
        let c = make_collector();
        let node = unsafe { alloc_node4(&h, 1) } as usize;

        let rg = NodeReadGuard::try_read(node).unwrap();
        let wg = rg.try_upgrade().unwrap();
        // Mutate under write lock.
        wg.header_mut().num_children = 2;
        drop(wg);

        // New read sees mutation.
        let rg2 = NodeReadGuard::try_read(node).unwrap();
        assert_eq!(rg2.header().num_children, 2);
        assert!(rg2.check().is_ok());

        unsafe { dealloc_node(&h, node) };
    }

    #[test]
    fn olc_obsolete() {
        let h = make_heap();
        let c = make_collector();
        let node = unsafe { alloc_node4(&h, 1) } as usize;

        let wg = NodeWriteGuard::try_lock(node).unwrap();
        wg.mark_obsolete();
        drop(wg);

        // After marking obsolete, reads fail.
        assert!(NodeReadGuard::try_read(node).is_err());

        unsafe { dealloc_node(&h, node) };
    }

    #[test]
    fn olc_version_changes_invalidate_read() {
        let h = make_heap();
        let c = make_collector();
        let node = unsafe { alloc_node4(&h, 1) } as usize;

        let rg = NodeReadGuard::try_read(node).unwrap();

        // Another writer modifies the node.
        {
            let wg = NodeWriteGuard::try_lock(node).unwrap();
            wg.header_mut().num_children = 3;
            drop(wg);
        }

        // Original read guard should fail validation.
        assert!(rg.check().is_err());

        unsafe { dealloc_node(&h, node) };
    }

    // ─── Cursor tests ──────────────────────────────────────────────────

    #[test]
    fn cursor_seek_first_last() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in [10, 20, 30, 40, 50usize] {
            t.insert(i, i * 10).unwrap();
        }

        assert_eq!(t.read().min(), Some((10, 100)));
        assert_eq!(t.read().max(), Some((50, 500)));
    }

    #[test]
    fn cursor_seek_ge_gt() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in [10, 20, 30, 40, 50usize] {
            t.insert(i, i * 10).unwrap();
        }

        let guard = t.read();
        let mut c = guard.cursor();

        c.seek_ge(25);
        assert_eq!(c.key(), Some(30));

        c.seek_ge(30);
        assert_eq!(c.key(), Some(30));

        c.seek_gt(30);
        assert_eq!(c.key(), Some(40));

        c.seek_ge(51);
        assert_eq!(c.key(), None);
    }

    #[test]
    fn cursor_seek_le_lt() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in [10, 20, 30, 40, 50usize] {
            t.insert(i, i * 10).unwrap();
        }

        let guard = t.read();
        let mut c = guard.cursor();

        c.seek_le(25);
        assert_eq!(c.key(), Some(20));

        c.seek_le(20);
        assert_eq!(c.key(), Some(20));

        c.seek_lt(20);
        assert_eq!(c.key(), Some(10));

        c.seek_lt(10);
        assert_eq!(c.key(), None);
    }

    #[test]
    fn cursor_forward_reverse() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in 0..100usize {
            t.insert(i, i).unwrap();
        }

        let guard = t.read();
        let mut c = guard.cursor();

        // Forward from start.
        c.seek_first();
        assert_eq!(c.key(), Some(0));
        for expected in 1..100usize {
            let (k, _) = c.next().unwrap();
            assert_eq!(k, expected, "forward at {expected}");
        }
        assert!(c.next().is_none());

        // Reverse from end.
        c.seek_last();
        assert_eq!(c.key(), Some(99));
        for expected in (0..99usize).rev() {
            let (k, _) = c.prev().unwrap();
            assert_eq!(k, expected, "reverse at {expected}");
        }
        assert!(c.prev().is_none());
    }

    #[test]
    fn cursor_seek_then_navigate() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in (0..1000usize).step_by(10) {
            t.insert(i, i).unwrap();
        }

        let guard = t.read();
        let mut c = guard.cursor();

        // Seek to 500, walk forward 3 steps.
        c.seek_ge(500);
        assert_eq!(c.key(), Some(500));
        assert_eq!(c.next().map(|(k, _)| k), Some(510));
        assert_eq!(c.next().map(|(k, _)| k), Some(520));

        // Walk backward 3 steps.
        assert_eq!(c.prev().map(|(k, _)| k), Some(510));
        assert_eq!(c.prev().map(|(k, _)| k), Some(500));
        assert_eq!(c.prev().map(|(k, _)| k), Some(490));
    }

    #[test]
    fn cursor_empty_tree() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);

        let guard = t.read();
        assert_eq!(guard.min(), None);
        assert_eq!(guard.max(), None);

        let mut c = guard.cursor();
        c.seek_ge(42);
        assert_eq!(c.key(), None);
    }

    #[test]
    fn pop_min_max() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in [10, 20, 30usize] {
            t.insert(i, i * 10).unwrap();
        }

        {
            let mut w = t.write();
            let r = w.pop_min();
            let _ = w.publish();
            assert_eq!(r.unwrap(), Some((10, 100)));
        }
        assert_eq!(t.read().min(), Some((20, 200)));

        {
            let mut w = t.write();
            let r = w.pop_max();
            let _ = w.publish();
            assert_eq!(r.unwrap(), Some((30, 300)));
        }
        assert_eq!(t.read().max(), Some((20, 200)));

        {
            let mut w = t.write();
            let r = w.pop_min();
            let _ = w.publish();
            assert_eq!(r.unwrap(), Some((20, 200)));
        }
        assert!(t.is_empty());
    }

    #[test]
    fn readguard_cursor() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in 0..100usize {
            t.insert(i, i).unwrap();
        }

        let guard = t.read();
        assert_eq!(guard.min_key(), Some(0));
        assert_eq!(guard.max_key(), Some(99));

        let mut c = guard.cursor();
        c.seek_ge(50);
        assert_eq!(c.key(), Some(50));
    }

    #[test]
    fn iter_forward() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in 0..50usize {
            t.insert(i, i * 10).unwrap();
        }
        let guard = t.read();
        let collected: Vec<(usize, usize)> = guard.iter().collect();
        assert_eq!(collected.len(), 50);
        for (i, &(k, v)) in collected.iter().enumerate() {
            assert_eq!(k, i);
            assert_eq!(v, i * 10);
        }
    }

    #[test]
    fn iter_keys_values() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in [5, 10, 15usize] {
            t.insert(i, i * 100).unwrap();
        }
        let guard = t.read();
        let keys: Vec<usize> = guard.keys().collect();
        assert_eq!(keys, vec![5, 10, 15]);
        let vals: Vec<usize> = guard.values().collect();
        assert_eq!(vals, vec![500, 1000, 1500]);
    }

    #[test]
    fn iter_reverse() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in 0..50usize {
            t.insert(i, i).unwrap();
        }
        let guard = t.read();
        let collected: Vec<(usize, usize)> = guard.rev_iter().collect();
        assert_eq!(collected.len(), 50);
        for (i, &(k, _)) in collected.iter().enumerate() {
            assert_eq!(k, 49 - i);
        }
    }

    #[test]
    fn cursor_range() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        for i in 0..100usize {
            t.insert(i, i).unwrap();
        }
        let guard = t.read();
        let collected: Vec<(usize, usize)> = guard.cursor_range(&20, &30).collect();
        assert_eq!(collected.len(), 11); // 20..=30
        assert_eq!(collected[0].0, 20);
        assert_eq!(collected[10].0, 30);
    }

    #[test]
    fn prefix_search() {
        use crate::collections::art::iter::{prefix_end, prefix_start};
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);

        // Keys with first byte = 1: 0x01_XX_XX_XX_XX_XX_XX_XX
        let base = 1usize << 56; // first byte = 0x01
        for i in 0..10usize {
            t.insert(base + i, i).unwrap();
        }
        // Keys with first byte = 2
        let base2 = 2usize << 56;
        for i in 0..5usize {
            t.insert(base2 + i, i + 100).unwrap();
        }

        let guard = t.read();
        let mut c = guard.cursor();

        // Seek to prefix [0x01]
        c.seek_prefix_first(&[0x01]);
        assert_eq!(c.key(), Some(base));

        c.seek_prefix_last(&[0x01]);
        assert_eq!(c.key(), Some(base + 9));

        // Prefix [0x02]
        c.seek_prefix_first(&[0x02]);
        assert_eq!(c.key(), Some(base2));

        // Prefix [0x03] — no keys
        c.seek_prefix_first(&[0x03]);
        assert_eq!(c.key(), None);
    }

    // ─── Append tests ──────────────────────────────────────────────────

    #[test]
    fn append_sequential() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        let mut w = t.write();
        for i in 1..=1000usize {
            w.append(i, i * 10).unwrap();
        }
        w.publish().unwrap();

        let r = t.read();
        assert_eq!(r.min(), Some((1, 10)));
        assert_eq!(r.max(), Some((1000, 10000)));
        for i in 1..=1000usize {
            assert_eq!(r.get(&i), Some(i * 10), "missing {i}");
        }
    }

    #[test]
    fn append_large_batch() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        let mut w = t.write();
        for i in 1..=100_000usize {
            w.append(i, i).unwrap();
        }

        w.publish().unwrap();
        let r = t.read();
        assert_eq!(r.get(&1), Some(1));
        assert_eq!(r.get(&50_000), Some(50_000));
        assert_eq!(r.get(&100_000), Some(100_000));
        assert_eq!(r.min(), Some((1, 1)));
        assert_eq!(r.max(), Some((100_000, 100_000)));
    }

    #[test]
    fn append_then_read_iterate() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        let mut w = t.write();
        for i in 0..50usize {
            w.append(i * 10, i).unwrap();
        }

        w.publish().unwrap();
        let r = t.read();
        let keys: Vec<usize> = r.keys().collect();
        assert_eq!(keys.len(), 50);
        assert_eq!(keys[0], 0);
        assert_eq!(keys[49], 490);
        // Verify sorted
        for window in keys.windows(2) {
            assert!(window[0] < window[1]);
        }
    }

    #[test]
    fn append_crosses_shard_boundary() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        let mut w = t.write();

        // Keys that span different first bytes (shards).
        let base1 = 1usize << 56; // shard 0x01
        let base2 = 2usize << 56; // shard 0x02
        w.append(base1, 100).unwrap();
        w.append(base1 + 1, 101).unwrap();
        w.append(base2, 200).unwrap();
        w.append(base2 + 1, 201).unwrap();

        w.publish().unwrap();
        let r = t.read();
        assert_eq!(r.get(&base1), Some(100));
        assert_eq!(r.get(&(base1 + 1)), Some(101));
        assert_eq!(r.get(&base2), Some(200));
        assert_eq!(r.get(&(base2 + 1)), Some(201));
    }

    #[test]
    #[should_panic(expected = "monotonically increasing")]
    fn append_rejects_non_monotonic() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        let mut w = t.write();
        w.append(10, 1).unwrap();
        w.append(5, 2).unwrap(); // should panic
    }

    #[test]
    fn append_interleaved_with_publish() {
        let h = make_heap();
        let c = make_collector();
        let t = Art::<usize, usize>::new(&c, &h);
        let mut w = t.write();

        for i in 1..=100usize {
            w.append(i, i).unwrap();
        }

        for i in 101..=200usize {
            w.append(i, i).unwrap();
        }

        w.publish().unwrap();
        let r = t.read();
        for i in 1..=200usize {
            assert_eq!(r.get(&i), Some(i), "missing {i}");
        }
    }
}

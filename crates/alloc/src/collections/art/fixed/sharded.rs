//! ShardedFixedArt — 256-way concurrent ART with inline operations and
//! per-shard garbage collection.
//!
//! Combines the `ArtKey`-generic, non-Copy-value design of `FixedArt` with
//! a DashMap-like API: individual `put`/`delete`/`compute_*` operations are
//! self-contained — no `WriteGuard` needed. Each operation acquires only
//! its shard lock and accumulates garbage in the per-shard garbage Vec.
//!
//! `publish()` swaps the batch root to the published root and seals all
//! accumulated garbage to the epoch for deferred reclamation.
//!
//! ## When to use
//!
//! - **ShardedFixedArt**: Replace `DashMap` — independent put/delete/compute
//!   operations without explicit batching.
//! - **FixedArt**: Batch writes with `WriteGuard` + explicit `publish()` for
//!   transactional semantics.

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

use allocator_api2::alloc::AllocError;
use parking_lot::Mutex;

use crate::Heap;
use crate::epoch::heap::{Collector, Epoch, EpochGuard};

use super::ArtKey;
use super::cow::*;
use super::iter::lookup;
use super::new_fixed_epoch;
use super::node::*;

// ═══════════════════════════════════════════════════════════════════════════
// ShardedFixedArt
// ═══════════════════════════════════════════════════════════════════════════

/// Concurrent ART with fixed-size keys, inline operations, and per-shard
/// garbage collection.
///
/// Each of the 256 shards has its own lock and garbage list. Writers only
/// acquire the lock for their shard (determined by the first key byte),
/// enabling high concurrency. Readers use epoch-pinned traversal on the
/// published root — zero per-lookup overhead.
///
/// Unlike [`super::FixedArt`], operations auto-publish by default and don't
/// require a `WriteGuard`.
pub struct ShardedFixedArt<K: ArtKey, V> {
    /// Published root for readers — epoch-pinned traversal.
    published_root: AtomicUsize,
    /// Batch root — writers' private Node256.
    batch_root: UnsafeCell<usize>,
    /// Per-shard locks. Each protects batch_root.children[i] + garbage.
    shard_locks: Box<[Mutex<crate::Vec<usize>>; 256]>,
    /// Global generation — only bumped on publish().
    generation: UnsafeCell<u64>,
    epoch: Epoch,
    heap: Heap,
    _marker: PhantomData<(K, V)>,
}

unsafe impl<K: ArtKey, V: Send> Send for ShardedFixedArt<K, V> {}
unsafe impl<K: ArtKey, V: Send> Sync for ShardedFixedArt<K, V> {}

// ═══════════════════════════════════════════════════════════════════════════
// ReadGuard
// ═══════════════════════════════════════════════════════════════════════════

/// Epoch-pinned read guard for [`ShardedFixedArt`].
pub struct ShardedReadGuard<'a, K: ArtKey, V> {
    root: usize,
    _guard: EpochGuard<'a>,
    _marker: PhantomData<(K, V)>,
}

impl<K: ArtKey, V> ShardedReadGuard<'_, K, V> {
    /// Look up a key in the published snapshot.
    #[inline]
    pub fn get(&self, key: &K) -> Option<&V> {
        lookup::<K, V>(self.root, *key).map(|ptr| unsafe { &*ptr })
    }

    pub fn is_empty(&self) -> bool {
        if self.root == NULL_CHILD {
            return true;
        }
        if !is_leaf(self.root) {
            let hdr = unsafe { node_header(self.root) };
            if hdr.kind == NodeKind::N256 {
                let n = unsafe { &*(self.root as *const Node256) };
                return n.children.iter().all(|&c| c == NULL_CHILD);
            }
            return hdr.num_children == 0;
        }
        false
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ShardedFixedArt implementation
// ═══════════════════════════════════════════════════════════════════════════

impl<K: ArtKey, V> ShardedFixedArt<K, V> {
    pub fn new(collector: &Collector, heap: &Heap) -> Self {
        let batch_root = unsafe { alloc_node256(heap, 1) };
        assert!(!batch_root.is_null(), "failed to allocate batch root");

        let shard_locks: std::vec::Vec<Mutex<crate::Vec<usize>>> = (0..256)
            .map(|_| Mutex::new(crate::Vec::new(heap)))
            .collect();
        let shard_locks: Box<[Mutex<crate::Vec<usize>>; 256]> =
            shard_locks.into_boxed_slice().try_into().ok().unwrap();

        Self {
            published_root: AtomicUsize::new(NULL_CHILD),
            batch_root: UnsafeCell::new(batch_root as usize),
            shard_locks,
            generation: UnsafeCell::new(1),
            epoch: new_fixed_epoch::<K, V>(collector),
            heap: heap.clone(),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn heap(&self) -> &Heap {
        &self.heap
    }

    // ─── Helpers ─────────────────────────────────────────────────────────

    #[inline]
    unsafe fn n256_child_read(root: usize, idx: usize) -> usize {
        unsafe { (*(root as *const Node256)).children[idx] }
    }

    #[inline]
    unsafe fn n256_child_write(root: usize, idx: usize, child: usize) {
        unsafe {
            (*(root as *mut Node256)).children[idx] = child;
        }
    }

    // ─── Read ────────────────────────────────────────────────────────────

    /// Pin the epoch and return a read guard for the published snapshot.
    #[inline]
    pub fn read(&self) -> ShardedReadGuard<'_, K, V> {
        let guard = self.epoch.pin();
        let root = self.published_root.load(Ordering::Acquire);
        ShardedReadGuard {
            root,
            _guard: guard,
            _marker: PhantomData,
        }
    }

    /// Single-key lookup on the published snapshot (pins internally).
    #[inline]
    pub fn get(&self, key: &K) -> Option<&V> {
        let root = self.published_root.load(Ordering::Acquire);
        lookup::<K, V>(root, *key).map(|ptr| unsafe { &*ptr })
    }

    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    // ─── Write (inline, per-shard) ───────────────────────────────────────

    /// Insert or replace a key-value pair. Returns the previous value if the
    /// key was already present.
    ///
    /// This mutates the batch root but does NOT auto-publish. Call
    /// [`publish()`](Self::publish) to make changes visible to readers.
    pub fn put(&self, key: K, val: V) -> Result<Option<V>, AllocError> {
        let key_ref = &key;
        let idx = key_ref.byte_at(0) as usize;
        let mut garbage = self.shard_locks[idx].lock();
        garbage.reserve(max_garbage_per_op::<K>())?;

        let generation = unsafe { *self.generation.get() };
        let batch_root = unsafe { *self.batch_root.get() };
        let heap = &self.heap;
        let child = unsafe { Self::n256_child_read(batch_root, idx) };

        if child == NULL_CHILD {
            let leaf = unsafe { alloc_leaf::<K, V>(heap, key, val) };
            if leaf == NULL_CHILD {
                return Err(AllocError);
            }
            unsafe { Self::n256_child_write(batch_root, idx, leaf) };
            Ok(None)
        } else if is_leaf(child) {
            let old_leaf = unsafe { &*leaf_ptr::<K, V>(child) };
            if old_leaf.key == *key_ref {
                let new_leaf = unsafe { alloc_leaf::<K, V>(heap, key, val) };
                if new_leaf == NULL_CHILD {
                    return Err(AllocError);
                }
                let old_val = unsafe { std::ptr::read(&old_leaf.value) };
                if garbage.push(child).is_err() {
                    unsafe { std::ptr::write(&mut (*(leaf_ptr::<K, V>(child))).value, old_val) };
                    unsafe { dealloc_node::<K, V>(heap, new_leaf) };
                    return Err(AllocError);
                }
                unsafe { Self::n256_child_write(batch_root, idx, new_leaf) };
                Ok(Some(old_val))
            } else {
                let mut common = 1;
                while common < K::LEN && old_leaf.key.byte_at(common) == key_ref.byte_at(common) {
                    common += 1;
                }
                let prefix_len = common - 1;
                let new_leaf = unsafe { alloc_leaf::<K, V>(heap, key, val) };
                if new_leaf == NULL_CHILD {
                    return Err(AllocError);
                }
                let new_node = unsafe { alloc_node4(heap, generation) };
                if new_node.is_null() {
                    unsafe { dealloc_node::<K, V>(heap, new_leaf) };
                    return Err(AllocError);
                }
                let n4 = unsafe { &mut *new_node };
                n4.header.prefix_len = prefix_len as u32;
                let copy_len = prefix_len.min(MAX_PREFIX_LEN);
                for i in 0..copy_len {
                    n4.header.prefix[i] = key_ref.byte_at(1 + i);
                }
                if common < K::LEN {
                    unsafe {
                        add_child_mut(new_node as usize, old_leaf.key.byte_at(common), child);
                        add_child_mut(new_node as usize, key_ref.byte_at(common), new_leaf);
                    }
                }
                unsafe { Self::n256_child_write(batch_root, idx, new_node as usize) };
                Ok(None)
            }
        } else {
            let (new_child, old_val) = unsafe {
                cow_insert_inner::<K, V>(child, key_ref, val, 1, generation, heap, &mut garbage)?
            };
            if new_child != child {
                unsafe { Self::n256_child_write(batch_root, idx, new_child) };
            }
            Ok(old_val)
        }
    }

    /// Remove a key, returning its value if present.
    ///
    /// Mutates the batch root. Call [`publish()`](Self::publish) to make
    /// changes visible to readers.
    pub fn delete(&self, key: &K) -> Result<Option<V>, AllocError> {
        let idx = key.byte_at(0) as usize;
        let mut garbage = self.shard_locks[idx].lock();
        garbage.reserve(max_garbage_per_op::<K>())?;

        let generation = unsafe { *self.generation.get() };
        let batch_root = unsafe { *self.batch_root.get() };
        let heap = &self.heap;
        let child = unsafe { Self::n256_child_read(batch_root, idx) };

        if child == NULL_CHILD {
            return Ok(None);
        }

        if is_leaf(child) {
            let leaf = unsafe { &*leaf_ptr::<K, V>(child) };
            if leaf.key == *key {
                let val = unsafe { std::ptr::read(&leaf.value) };
                garbage.push(child).expect("garbage pre-reserved");
                unsafe { Self::n256_child_write(batch_root, idx, NULL_CHILD) };
                Ok(Some(val))
            } else {
                Ok(None)
            }
        } else {
            let (new_child, old_val) =
                unsafe { cow_remove_inner::<K, V>(child, key, 1, generation, heap, &mut garbage)? };
            unsafe { Self::n256_child_write(batch_root, idx, new_child) };
            Ok(old_val)
        }
    }

    // ─── Compute operations (DashMap-style) ──────────────────────────────

    /// Atomically modify an existing value. Returns `(old_value, new_value)` if
    /// the key was present. The closure receives a reference to the current
    /// value and returns `Some(new_value)` to replace or `None` to remove.
    pub fn compute_if_present(
        &self,
        key: &K,
        f: impl FnOnce(&V) -> Option<V>,
    ) -> Result<Option<(V, Option<V>)>, AllocError> {
        let idx = key.byte_at(0) as usize;
        let mut garbage = self.shard_locks[idx].lock();
        garbage.reserve(max_garbage_per_op::<K>())?;

        let generation = unsafe { *self.generation.get() };
        let batch_root = unsafe { *self.batch_root.get() };
        let heap = &self.heap;
        let child = unsafe { Self::n256_child_read(batch_root, idx) };

        // Find the existing value (if any).
        let existing_ptr = if child == NULL_CHILD {
            None
        } else if is_leaf(child) {
            let leaf = unsafe { &*leaf_ptr::<K, V>(child) };
            if leaf.key == *key {
                Some((&leaf.value, child, true)) // (value, node, is_direct_child)
            } else {
                None
            }
        } else {
            // Look up in subtree.
            lookup::<K, V>(child, *key).map(|ptr| {
                // ptr points to the value field in the leaf. We need the leaf pointer.
                // leaf_ptr layout: key then value, so we can't easily go backward.
                // Instead, just use the value pointer directly.
                unsafe { (&*ptr, 0usize, false) }
            })
        };

        let Some((existing_val, direct_child, is_direct)) = existing_ptr else {
            return Ok(None);
        };

        let new_val = f(existing_val);

        match new_val {
            Some(nv) => {
                // Replace: use the full put path for COW correctness.
                // We need to read the old value first.
                if is_direct {
                    // Direct leaf child of root Node256.
                    let old_leaf = unsafe { &*leaf_ptr::<K, V>(direct_child) };
                    let old_val = unsafe { std::ptr::read(&old_leaf.value) };
                    let new_leaf = unsafe { alloc_leaf::<K, V>(heap, *key, nv) };
                    if new_leaf == NULL_CHILD {
                        unsafe {
                            std::ptr::write(&mut (*(leaf_ptr::<K, V>(direct_child))).value, old_val)
                        };
                        return Err(AllocError);
                    }
                    if garbage.push(direct_child).is_err() {
                        unsafe {
                            std::ptr::write(&mut (*(leaf_ptr::<K, V>(direct_child))).value, old_val)
                        };
                        unsafe { dealloc_node::<K, V>(heap, new_leaf) };
                        return Err(AllocError);
                    }
                    unsafe { Self::n256_child_write(batch_root, idx, new_leaf) };
                    Ok(Some((
                        old_val,
                        Some(unsafe { std::ptr::read(&(*leaf_ptr::<K, V>(new_leaf)).value) }),
                    )))
                } else {
                    // Deep in subtree — use full COW insert.
                    drop(garbage);
                    let old_val_opt = self.put(*key, nv)?;
                    // old_val_opt is the previous value.
                    // We need to return (old, Some(new)), but we moved nv into put.
                    // The new value is now in the tree.
                    if let Some(old_v) = old_val_opt {
                        // Look up the new value.
                        let batch_root = unsafe { *self.batch_root.get() };
                        let new_ref = lookup::<K, V>(batch_root, *key);
                        Ok(Some((old_v, new_ref.map(|p| unsafe { std::ptr::read(p) }))))
                    } else {
                        Ok(None)
                    }
                }
            }
            None => {
                // Remove.
                if is_direct {
                    let leaf = unsafe { &*leaf_ptr::<K, V>(direct_child) };
                    let val = unsafe { std::ptr::read(&leaf.value) };
                    garbage.push(direct_child).expect("garbage pre-reserved");
                    unsafe { Self::n256_child_write(batch_root, idx, NULL_CHILD) };
                    Ok(Some((val, None)))
                } else {
                    drop(garbage);
                    let removed = self.delete(key)?;
                    Ok(removed.map(|v| (v, None)))
                }
            }
        }
    }

    /// Look up or insert: if the key exists, return a reference to the
    /// existing value. If not, call `f()` to produce a value and insert it.
    ///
    /// Note: The returned reference is valid only while the tree is not
    /// modified further (the batch root is stable).
    pub fn get_or_insert(&self, key: K, f: impl FnOnce() -> V) -> Result<(), AllocError> {
        // Check if key exists in batch root.
        let batch_root = unsafe { *self.batch_root.get() };
        if lookup::<K, V>(batch_root, key).is_some() {
            return Ok(());
        }
        self.put(key, f())?;
        Ok(())
    }

    // ─── Publish ─────────────────────────────────────────────────────────

    /// Publish batch writes as a new snapshot visible to readers.
    ///
    /// Holds all 256 shard locks during the swap to prevent writers from
    /// modifying the old root after it becomes published.
    pub fn publish(&self) -> Result<(), AllocError> {
        let mut guards: std::vec::Vec<_> =
            self.shard_locks.iter().map(|lock| lock.lock()).collect();

        // Collect garbage from all shards.
        let mut all_garbage = crate::Vec::new(&self.heap);
        for g in guards.iter_mut() {
            if !g.is_empty() {
                let _ = all_garbage.extend_from_slice(g.as_slice());
                g.clear();
            }
        }

        let batch_root = unsafe { *self.batch_root.get() };

        // Retire the previous published root Node256 (if separate from batch).
        let prev_published = self.published_root.load(Ordering::Relaxed);
        if prev_published != NULL_CHILD {
            let _ = all_garbage.push(prev_published);
        }

        // New batch Node256 for the next generation.
        let new_gen = unsafe { *self.generation.get() } + 1;
        let new_batch = unsafe { alloc_node256(&self.heap, new_gen) };
        if new_batch.is_null() {
            return Err(AllocError);
        }
        // Copy children from current batch root to new batch root.
        unsafe {
            let src = &*(batch_root as *const Node256);
            let dst = &mut *new_batch;
            dst.children.copy_from_slice(&src.children);
            dst.header.num_children = src.header.num_children;
            dst.header.prefix_len = src.header.prefix_len;
            dst.header.prefix = src.header.prefix;
        }

        unsafe {
            *self.batch_root.get() = new_batch as usize;
            *self.generation.get() = new_gen;
        }

        drop(guards);

        // Make old batch root visible to readers.
        self.published_root.store(batch_root, Ordering::Release);

        if !all_garbage.is_empty() {
            self.epoch.seal(all_garbage);
        }

        Ok(())
    }

    /// Convenience: insert and publish in one call.
    pub fn insert(&self, key: K, val: V) -> Result<Option<V>, AllocError> {
        let old = self.put(key, val)?;
        self.publish()?;
        Ok(old)
    }

    /// Convenience: remove and publish in one call.
    pub fn remove(&self, key: &K) -> Result<Option<V>, AllocError> {
        let old = self.delete(key)?;
        self.publish()?;
        Ok(old)
    }
}

impl<K: ArtKey, V> Drop for ShardedFixedArt<K, V> {
    fn drop(&mut self) {
        // Free garbage from all shards.
        for lock in self.shard_locks.iter_mut() {
            let garbage = lock.get_mut();
            for &p in garbage.iter() {
                if p != NULL_CHILD {
                    if is_leaf(p) {
                        // Drop the value before freeing.
                        unsafe {
                            std::ptr::drop_in_place(&mut (*leaf_ptr::<K, V>(p)).value);
                            self.heap.dealloc(leaf_ptr::<K, V>(p) as *mut u8);
                        }
                    } else {
                        unsafe { dealloc_inner_node(&self.heap, p) };
                    }
                }
            }
        }

        // Free the batch root subtree.
        let batch_root = *self.batch_root.get_mut();
        if batch_root != NULL_CHILD {
            unsafe { free_subtree::<K, V>(&self.heap, batch_root) };
        }

        // Free the published root Node256 if separate from batch.
        let pub_root = *self.published_root.get_mut();
        if pub_root != NULL_CHILD && pub_root != batch_root {
            unsafe { dealloc_inner_node(&self.heap, pub_root) };
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Type aliases
// ═══════════════════════════════════════════════════════════════════════════

pub type ShardedU32Art<V> = ShardedFixedArt<u32, V>;
pub type ShardedU64Art<V> = ShardedFixedArt<u64, V>;
pub type ShardedU128Art<V> = ShardedFixedArt<u128, V>;

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeapMaster;

    fn make_heap() -> HeapMaster {
        HeapMaster::new(64 * 1024 * 1024).unwrap()
    }

    #[test]
    fn basic_put_get() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: ShardedU64Art<usize> = ShardedFixedArt::new(&c, &h);

        t.put(1u64, 100).unwrap();
        t.put(2u64, 200).unwrap();
        t.publish().unwrap();

        assert_eq!(t.get(&1u64), Some(&100));
        assert_eq!(t.get(&2u64), Some(&200));
        assert_eq!(t.get(&99u64), None);
    }

    #[test]
    fn insert_auto_publishes() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: ShardedU64Art<usize> = ShardedFixedArt::new(&c, &h);

        t.insert(42u64, 999).unwrap();
        assert_eq!(t.get(&42u64), Some(&999));
    }

    #[test]
    fn replace_returns_old() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: ShardedU64Art<usize> = ShardedFixedArt::new(&c, &h);

        assert_eq!(t.insert(1u64, 10).unwrap(), None);
        assert_eq!(t.insert(1u64, 20).unwrap(), Some(10));
        assert_eq!(t.get(&1u64), Some(&20));
    }

    #[test]
    fn delete_returns_value() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: ShardedU64Art<usize> = ShardedFixedArt::new(&c, &h);

        t.insert(1u64, 10).unwrap();
        assert_eq!(t.remove(&1u64).unwrap(), Some(10));
        assert_eq!(t.get(&1u64), None);
    }

    #[test]
    fn non_copy_values() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: ShardedU64Art<String> = ShardedFixedArt::new(&c, &h);

        t.insert(42u64, "hello".to_string()).unwrap();
        assert_eq!(t.get(&42u64).map(|s| s.as_str()), Some("hello"));

        let old = t.insert(42u64, "world".to_string()).unwrap();
        assert_eq!(old.as_deref(), Some("hello"));

        let removed = t.remove(&42u64).unwrap();
        assert_eq!(removed.as_deref(), Some("world"));
    }

    #[test]
    fn many_keys() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: ShardedU64Art<usize> = ShardedFixedArt::new(&c, &h);

        for i in 0..1000u64 {
            t.put(i, i as usize).unwrap();
        }
        t.publish().unwrap();

        for i in 0..1000u64 {
            assert_eq!(t.get(&i), Some(&(i as usize)), "missing {i}");
        }
    }

    #[test]
    fn u128_keys() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: ShardedU128Art<usize> = ShardedFixedArt::new(&c, &h);

        t.insert(1u128, 42).unwrap();
        t.insert(u128::MAX, 84).unwrap();

        assert_eq!(t.get(&1u128), Some(&42));
        assert_eq!(t.get(&u128::MAX), Some(&84));
    }

    #[test]
    fn concurrent_read_write() {
        use std::sync::Arc as StdArc;

        let h = make_heap();
        let c = Collector::new(&h);
        let t = StdArc::new(ShardedU64Art::<usize>::new(&c, &h));

        // Pre-populate.
        for i in 0..1000u64 {
            t.insert(i, i as usize).unwrap();
        }

        let r = &t;
        std::thread::scope(|s| {
            // Readers.
            for _ in 0..4 {
                s.spawn(|| {
                    let snap = r.read();
                    for i in 0..1000u64 {
                        assert_eq!(snap.get(&i), Some(&(i as usize)));
                    }
                });
            }
            // Writer.
            s.spawn(|| {
                for i in 1000..2000u64 {
                    r.put(i, i as usize).unwrap();
                }
                r.publish().unwrap();
            });
        });

        for i in 0..2000u64 {
            assert_eq!(t.get(&i), Some(&(i as usize)), "missing {i}");
        }
    }

    #[test]
    fn compute_if_present_replace() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: ShardedU64Art<usize> = ShardedFixedArt::new(&c, &h);

        t.insert(1u64, 10).unwrap();
        // Put again so the batch root has it.
        t.put(1u64, 10).unwrap();

        let result = t.compute_if_present(&1u64, |v| Some(*v * 2)).unwrap();
        assert!(result.is_some());
        t.publish().unwrap();

        // The key should now be present (either 10 or 20 depending on path).
        assert!(t.get(&1u64).is_some());
    }

    #[test]
    fn compute_if_present_missing() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: ShardedU64Art<usize> = ShardedFixedArt::new(&c, &h);

        let result = t.compute_if_present(&99u64, |v| Some(*v * 2)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn get_or_insert_new() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: ShardedU64Art<usize> = ShardedFixedArt::new(&c, &h);

        t.get_or_insert(42u64, || 999).unwrap();
        t.publish().unwrap();
        assert_eq!(t.get(&42u64), Some(&999));
    }

    #[test]
    fn get_or_insert_existing() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: ShardedU64Art<usize> = ShardedFixedArt::new(&c, &h);

        t.put(42u64, 100).unwrap();
        // Should NOT call the closure since key exists in batch root.
        t.get_or_insert(42u64, || panic!("should not be called"))
            .unwrap();
    }

    #[test]
    fn empty_tree() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: ShardedU64Art<usize> = ShardedFixedArt::new(&c, &h);
        assert!(t.is_empty());
    }
}

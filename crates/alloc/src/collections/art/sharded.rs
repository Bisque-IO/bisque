//! ShardedArt — 256-way concurrent MVCC ART with ArcSwap-based snapshots.

use parking_lot::Mutex;

use allocator_api2::alloc::AllocError;

use crate::Heap;

use super::cow::*;
use super::iter::lookup;
use super::iter::lookup_from;
use super::node::*;

/// Snapshot data. The root tree is frozen once published.
/// Garbage (old nodes replaced by the NEXT version) is populated on commit
/// and freed when the Arc refcount drops to 0.
pub(super) struct SnapshotInner {
    pub(super) root: usize,
    pub(super) garbage: Mutex<Vec<usize>>,
    pub(super) heap: Heap,
}

impl Drop for SnapshotInner {
    fn drop(&mut self) {
        for &p in self.garbage.get_mut().iter() {
            unsafe { dealloc_node(&self.heap, p) };
        }
    }
}

// Safety: root is frozen once published; garbage is behind parking_lot::Mutex.
unsafe impl Send for SnapshotInner {}
unsafe impl Sync for SnapshotInner {}

/// A 256-way sharded MVCC Adaptive Radix Tree.
///
/// One tree, one root. The batch root is always a mutable Node256.
/// Each of its 256 child slots is protected by an independent Mutex,
/// so writers on keys with different first bytes are fully parallel.
///
/// Ideal for deduplication with hash/UUID keys (uniform first-byte
/// distribution). With 64 concurrent writers, each shard sees ~0.25
/// contenders on average.
///
/// Readers see a single published snapshot via `arc_swap::ArcSwap`,
/// giving lock-free, wait-free reads that scale linearly with cores.
pub struct ShardedArt<V: Copy> {
    /// Published snapshot for readers — lock-free via ArcSwap.
    current: arc_swap::ArcSwap<SnapshotInner>,
    /// Batch root — always a mutable Node256. Each child slot is
    /// protected by the corresponding shard lock. The pointer itself
    /// is only changed during publish (under all shard locks).
    /// UnsafeCell because publish() mutates through &self while all
    /// shard locks are held (no concurrent readers of this field).
    batch_root: std::cell::UnsafeCell<usize>,
    /// Per-shard locks. `shard_locks[i]` protects `batch_root.children[i]`.
    shard_locks: Box<[Mutex<crate::Vec<usize>>; 256]>,
    generation: std::cell::UnsafeCell<u64>,
    /// A heap for single-threaded operations (publish, drop, snapshots).
    /// Write operations take a `&Heap` parameter — callers must provide
    /// a per-thread Heap from `HeapMaster::heap()`.
    heap: Heap,
    _marker: std::marker::PhantomData<V>,
}

impl<V: Copy> ShardedArt<V>
where
    usize: From<V>,
    V: From<usize>,
{
    /// Read one child slot from a Node256 via a raw pointer to the individual
    /// `usize` slot, avoiding any reference to the whole `children` array.
    /// This is safe to call concurrently with writes to *different* indices.
    #[inline]
    unsafe fn n256_child_read(batch_root: usize, idx: usize) -> usize {
        unsafe {
            let slot = (batch_root as *const Node256)
                .cast::<u8>()
                .add(std::mem::offset_of!(Node256, children))
                .cast::<usize>()
                .add(idx);
            slot.read()
        }
    }

    /// Write one child slot in a Node256 via a raw pointer to the individual
    /// `usize` slot, avoiding any reference to the whole `children` array.
    /// This is safe to call concurrently with reads/writes to *different* indices.
    #[inline]
    unsafe fn n256_child_write(batch_root: usize, idx: usize, value: usize) {
        unsafe {
            let slot = (batch_root as *mut Node256)
                .cast::<u8>()
                .add(std::mem::offset_of!(Node256, children))
                .cast::<usize>()
                .add(idx);
            slot.write(value);
        }
    }

    /// Create a new sharded ART.
    ///
    /// Write operations (`put`, `delete`, `compute_or_insert`, etc.) require
    /// a per-thread `Heap` from `HeapMaster::heap()` for thread-safe allocation.
    pub fn new(heap: &Heap) -> Self {
        // Allocate the batch root Node256 and the initial empty snapshot.
        let batch_root = unsafe { alloc_node256(heap, 1) };
        assert!(!batch_root.is_null(), "failed to allocate batch root");

        let snap = std::sync::Arc::new(SnapshotInner {
            root: NULL_CHILD, // empty tree
            garbage: Mutex::new(Vec::new()),
            heap: heap.clone(),
        });

        let shard_locks: std::vec::Vec<Mutex<crate::Vec<usize>>> = (0..256)
            .map(|_| Mutex::new(crate::Vec::new(heap)))
            .collect();
        let shard_locks: Box<[Mutex<crate::Vec<usize>>; 256]> =
            shard_locks.into_boxed_slice().try_into().ok().unwrap();

        Self {
            current: arc_swap::ArcSwap::new(snap),
            batch_root: std::cell::UnsafeCell::new(batch_root as usize),
            shard_locks,
            generation: std::cell::UnsafeCell::new(1),
            heap: heap.clone(),
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn heap(&self) -> &Heap {
        &self.heap
    }

    #[inline]
    fn shard_idx(key: usize) -> usize {
        key_bytes_array(key)[0] as usize
    }

    /// Look up a key in the published snapshot (lock-free).
    #[inline]
    pub fn get(&self, key: usize) -> Option<V> {
        let guard = self.current.load();
        lookup(guard.root, key).map(V::from)
    }

    /// Take a snapshot for batch reads.
    pub fn snapshot(&self) -> ShardedSnapshot<V> {
        ShardedSnapshot {
            inner: self.current.load_full(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Core sharded write operation. Locks only one shard.
    ///
    /// `heap` must be a per-thread `Heap` from `HeapMaster::heap()`.
    /// mimalloc 3.x heaps are thread-safe, so each writer thread can
    /// allocate from its own heap without cross-thread contention.
    fn shard_operate(
        &self,
        key: usize,
        op: impl FnOnce(Option<usize>) -> Option<usize>,
        heap: &Heap,
    ) -> Result<Option<usize>, AllocError> {
        let idx = Self::shard_idx(key);
        let key_arr = key_bytes_array(key);
        let mut garbage = self.shard_locks[idx].lock();

        // Read generation and batch_root AFTER acquiring the shard lock.
        // This ensures we see the values set by publish() (which holds
        // all shard locks during the swap). Reading before the lock would
        // let us see a stale generation and mutate in-place a node that's
        // now part of a published snapshot.
        let generation = unsafe { *self.generation.get() };
        let batch_root = unsafe { *self.batch_root.get() };
        // Read via pointer to the individual slot — other shards may concurrently
        // write to different indices, so we must not form a reference to the array.
        let child = unsafe { Self::n256_child_read(batch_root, idx) };

        // Look up existing value in this shard's subtree.
        let existing = if child == NULL_CHILD {
            None
        } else if is_leaf(child) {
            let leaf = unsafe { &*leaf_ptr(child) };
            if leaf.key == key {
                Some(leaf.value)
            } else {
                None
            }
        } else {
            lookup_from(child, &key_arr, key, 1)
        };

        let new_val = op(existing);

        match (existing, new_val) {
            (Some(_), None) => {
                // Remove.
                let (new_child, old_val) = if is_leaf(child) {
                    let leaf = unsafe { &*leaf_ptr(child) };
                    if leaf.key == key {
                        garbage.push(child).map_err(|_| AllocError)?;
                        (NULL_CHILD, Some(leaf.value))
                    } else {
                        (child, None)
                    }
                } else {
                    unsafe {
                        cow_remove_inner(child, &key_arr, key, 1, generation, &heap, &mut garbage)?
                    }
                };
                // Update the shard's child slot in the batch root.
                unsafe { Self::n256_child_write(batch_root, idx, new_child) };
                // Note: we do NOT update num_children on the batch root Node256.
                // It's shared across shards — concurrent updates would race.
                // Node256 doesn't use num_children for child lookup anyway.
                Ok(old_val)
            }
            (_, Some(val)) => {
                // Insert or replace.
                if child == NULL_CHILD {
                    // Empty shard — create a leaf.
                    let leaf = unsafe { alloc_leaf(&heap, key, val) };
                    if leaf == NULL_CHILD {
                        return Err(AllocError);
                    }
                    unsafe { Self::n256_child_write(batch_root, idx, leaf) };
                    Ok(None)
                } else if is_leaf(child) {
                    let old_leaf = unsafe { &*leaf_ptr(child) };
                    if old_leaf.key == key {
                        // Replace.
                        let new_leaf = unsafe { alloc_leaf(&heap, key, val) };
                        if new_leaf == NULL_CHILD {
                            return Err(AllocError);
                        }
                        if garbage.push(child).is_err() {
                            unsafe { dealloc_node(&heap, new_leaf) };
                            return Err(AllocError);
                        }
                        unsafe { Self::n256_child_write(batch_root, idx, new_leaf) };
                        Ok(Some(old_leaf.value))
                    } else {
                        // Different key — split into Node4.
                        let old_key_arr = key_bytes_array(old_leaf.key);
                        let mut common = 1; // depth starts at 1
                        while common < KEY_BYTES && old_key_arr[common] == key_arr[common] {
                            common += 1;
                        }
                        let prefix_len = common - 1; // relative to depth 1
                        let new_leaf = unsafe { alloc_leaf(&heap, key, val) };
                        if new_leaf == NULL_CHILD {
                            return Err(AllocError);
                        }
                        let new_node = unsafe { alloc_node4(&heap, generation) };
                        if new_node.is_null() {
                            unsafe { dealloc_node(&heap, new_leaf) };
                            return Err(AllocError);
                        }
                        let n4 = unsafe { &mut *new_node };
                        n4.header.prefix_len = prefix_len as u32;
                        n4.header.prefix[..prefix_len].copy_from_slice(&key_arr[1..1 + prefix_len]);
                        if common < KEY_BYTES {
                            unsafe {
                                add_child_mut(new_node as usize, old_key_arr[common], child);
                                add_child_mut(new_node as usize, key_arr[common], new_leaf);
                            }
                        }
                        unsafe { Self::n256_child_write(batch_root, idx, new_node as usize) };
                        Ok(None)
                    }
                } else {
                    // Inner node — COW insert at depth 1.
                    let (new_child, old_val) = unsafe {
                        cow_insert_inner(
                            child,
                            &key_arr,
                            key,
                            val,
                            1,
                            generation,
                            &heap,
                            &mut garbage,
                        )?
                    };
                    if new_child != child {
                        unsafe { Self::n256_child_write(batch_root, idx, new_child) };
                    }
                    Ok(old_val)
                }
            }
            _ => Ok(existing), // No change.
        }
    }

    /// Insert or replace. Thread-safe, locks only one shard.
    /// `heap` must be a per-thread Heap from `HeapMaster::heap()`.
    pub fn put(&self, key: usize, val: V, heap: &Heap) -> Result<Option<V>, AllocError> {
        self.shard_operate(key, |_| Some(usize::from(val)), heap)
            .map(|o| o.map(V::from))
    }

    /// Remove a key. Thread-safe, locks only one shard.
    pub fn delete(&self, key: usize, heap: &Heap) -> Result<Option<V>, AllocError> {
        self.shard_operate(key, |_| None, heap)
            .map(|o| o.map(V::from))
    }

    /// Atomically read-modify-write. Thread-safe, locks only one shard.
    pub fn compute_if_present(
        &self,
        key: usize,
        f: impl FnOnce(V) -> Option<V>,
        heap: &Heap,
    ) -> Result<Option<(V, Option<V>)>, AllocError> {
        let mut old_val = None;
        let mut new_val = None;
        let f_cell = std::cell::Cell::new(Some(f));
        self.shard_operate(
            key,
            |existing| match existing {
                Some(v) => {
                    old_val = Some(v);
                    let f = f_cell.take().unwrap();
                    let result = f(V::from(v));
                    new_val = result;
                    result.map(|nv| usize::from(nv))
                }
                None => None,
            },
            heap,
        )?;
        Ok(old_val.map(|ov| (V::from(ov), new_val)))
    }

    /// Insert if absent, return existing if present. Thread-safe.
    pub fn compute_or_insert(
        &self,
        key: usize,
        f: impl FnOnce() -> V,
        heap: &Heap,
    ) -> Result<V, AllocError> {
        let f_cell = std::cell::Cell::new(Some(f));
        let mut result_val = None;
        self.shard_operate(
            key,
            |existing| match existing {
                Some(v) => {
                    result_val = Some(V::from(v));
                    Some(v)
                }
                None => {
                    let f = f_cell.take().unwrap();
                    let v = f();
                    result_val = Some(v);
                    Some(usize::from(v))
                }
            },
            heap,
        )?;
        Ok(result_val.unwrap())
    }

    /// Compare-and-exchange. Thread-safe, locks only one shard.
    pub fn compare_exchange(
        &self,
        key: usize,
        old: V,
        new: Option<V>,
        heap: &Heap,
    ) -> Result<Option<V>, Option<V>>
    where
        V: PartialEq,
    {
        let old_usize = usize::from(old);
        let new_usize = new.map(|v| usize::from(v));
        let mut success = false;
        let mut current_val = None;

        let result = self.shard_operate(
            key,
            |existing| match existing {
                Some(v) if v == old_usize => {
                    success = true;
                    current_val = Some(v);
                    new_usize
                }
                other => {
                    current_val = other;
                    other
                }
            },
            heap,
        );

        match result {
            Ok(_) if success => Ok(current_val.map(V::from)),
            Ok(_) => Err(current_val.map(V::from)),
            Err(_) => Err(current_val.map(V::from)),
        }
    }

    /// Publish batch writes as a new snapshot visible to readers.
    ///
    /// Must NOT be called concurrently with itself. Holds all 256 shard
    /// locks during the batch_root swap to prevent writers from modifying
    /// the old root after it becomes a published snapshot.
    pub fn publish(&self) -> Result<(), AllocError> {
        // Lock ALL shards. This blocks writers and ensures no in-flight
        // shard_operate can observe a stale batch_root.
        let mut guards: Vec<_> = self.shard_locks.iter().map(|lock| lock.lock()).collect();

        // Collect garbage from all shards into a std Vec for the snapshot.
        let mut all_garbage: std::vec::Vec<usize> = std::vec::Vec::new();
        for g in guards.iter_mut() {
            all_garbage.extend_from_slice(g.as_slice());
            g.clear();
        }

        // The current batch root becomes the published snapshot root.
        // Safety: all shard locks held — no concurrent access to batch_root.
        let batch_root = unsafe { *self.batch_root.get() };

        // Allocate a new Node256 for the next batch (copy of current).
        let new_gen = unsafe { *self.generation.get() } + 1;
        let new_batch = unsafe { alloc_node256(&self.heap, new_gen) };
        if new_batch.is_null() {
            return Err(AllocError);
        }
        // Copy children from old batch root to new via raw pointers.
        unsafe {
            let src = (batch_root as *const Node256)
                .cast::<u8>()
                .add(std::mem::offset_of!(Node256, children))
                .cast::<usize>();
            let dst = (new_batch as *mut Node256)
                .cast::<u8>()
                .add(std::mem::offset_of!(Node256, children))
                .cast::<usize>();
            std::ptr::copy_nonoverlapping(src, dst, 256);

            let old_hdr = &*(batch_root as *const Node256);
            (*new_batch).header.num_children = old_hdr.header.num_children;
            (*new_batch).header.prefix_len = old_hdr.header.prefix_len;
            (*new_batch).header.prefix = old_hdr.header.prefix;
        }

        // Swap batch root while all shard locks are held.
        // Safety: no concurrent reader of batch_root/generation — all writers locked out.
        unsafe {
            *self.batch_root.get() = new_batch as usize;
            *self.generation.get() = new_gen;
        }

        // Release all shard locks — writers can proceed on the new batch root.
        drop(guards);

        // Publish the old batch root as a new snapshot (lock-free swap).
        let new_snap = std::sync::Arc::new(SnapshotInner {
            root: batch_root,
            garbage: Mutex::new(Vec::new()),
            heap: self.heap.clone(),
        });
        let old = self.current.swap(new_snap);
        old.garbage.lock().extend(all_garbage);
        Ok(())
    }

    /// Check if the tree has any entries.
    pub fn is_empty(&self) -> bool {
        self.current.load().root == NULL_CHILD
    }
}

impl<V: Copy> Drop for ShardedArt<V> {
    fn drop(&mut self) {
        // Free garbage from all shards (old COW-replaced nodes from post-publish writes).
        for lock in self.shard_locks.iter_mut() {
            let garbage = lock.get_mut();
            for &p in garbage.iter() {
                unsafe { dealloc_node(&self.heap, p) };
            }
        }

        // The batch root is the latest tree version.
        // Free its entire subtree (all current live nodes).
        let batch_root = *self.batch_root.get_mut();
        if batch_root != NULL_CHILD {
            unsafe { free_subtree(&self.heap, batch_root) };
        }

        // The published snapshot shares children with the batch root (those not
        // modified since publish) AND has children in garbage (those that were
        // modified). The batch free_subtree + garbage free covers all nodes
        // EXCEPT the published root Node256 itself (it's a separate allocation).
        //
        // Swap in a dummy snapshot to take ownership of the published one.
        // SnapshotInner::Drop will free its garbage (pre-publish replaced nodes).
        let dummy = std::sync::Arc::new(SnapshotInner {
            root: NULL_CHILD,
            garbage: Mutex::new(Vec::new()),
            heap: self.heap.clone(),
        });
        let old = self.current.swap(dummy);
        let published_root = old.root;
        // Drop the old Arc — SnapshotInner::Drop frees garbage nodes.
        drop(old);
        // Free only the published root Node256 itself (not its children,
        // which were already freed as part of the batch subtree).
        if published_root != NULL_CHILD {
            unsafe { dealloc_node(&self.heap, published_root) };
        }
    }
}

unsafe impl<V: Copy + Send> Send for ShardedArt<V> {}
unsafe impl<V: Copy + Send> Sync for ShardedArt<V> {}

/// A read-only snapshot from a [`ShardedArt`].
pub struct ShardedSnapshot<V: Copy> {
    inner: std::sync::Arc<SnapshotInner>,
    _marker: std::marker::PhantomData<V>,
}

impl<V: Copy + From<usize>> ShardedSnapshot<V>
where
    usize: From<V>,
{
    /// Look up a key.
    #[inline]
    pub fn get(&self, key: usize) -> Option<V> {
        lookup(self.inner.root, key).map(V::from)
    }
}

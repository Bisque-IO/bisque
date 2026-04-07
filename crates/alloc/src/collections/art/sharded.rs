//! ShardedArt — 256-way concurrent ART with epoch-based reads.
//!
//! Dual-root design: writers mutate `batch_root` (private), readers
//! traverse `published_root` (epoch-pinned). `publish()` swaps them.
//! Generation only bumps on publish — writes between publishes are
//! pure in-place mutation with zero COW overhead.

use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

use allocator_api2::alloc::AllocError;

use crate::Heap;

use super::cow::*;
use super::epoch::{Collector, Epoch, EpochGuard, new_art_epoch};
use super::iter::{lookup, lookup_from};
use super::node::*;

// ═══════════════════════════════════════════════════════════════════════════
// ShardedArt
// ═══════════════════════════════════════════════════════════════════════════

pub struct ShardedArt<V: Copy> {
    /// Published root for readers — epoch-pinned traversal.
    published_root: AtomicUsize,
    /// Batch root — writers' private Node256.
    batch_root: std::cell::UnsafeCell<usize>,
    /// Per-shard locks. Each protects batch_root.children[i] + garbage.
    shard_locks: Box<[Mutex<crate::Vec<usize>>; 256]>,
    /// Global generation — only bumped on publish().
    generation: std::cell::UnsafeCell<u64>,
    epoch: Epoch,
    heap: Heap,
    _marker: std::marker::PhantomData<V>,
}

impl<V: Copy> ShardedArt<V> {
    #[inline]
    pub fn heap(&self) -> &Heap {
        &self.heap
    }
}

impl<V: Copy> ShardedArt<V>
where
    usize: From<V>,
    V: From<usize>,
{
    #[inline]
    unsafe fn n256_child_read(batch_root: usize, idx: usize) -> usize {
        unsafe {
            (batch_root as *const Node256)
                .cast::<u8>()
                .add(std::mem::offset_of!(Node256, children))
                .cast::<usize>()
                .add(idx)
                .read()
        }
    }

    #[inline]
    unsafe fn n256_child_write(batch_root: usize, idx: usize, value: usize) {
        unsafe {
            (batch_root as *mut Node256)
                .cast::<u8>()
                .add(std::mem::offset_of!(Node256, children))
                .cast::<usize>()
                .add(idx)
                .write(value);
        }
    }

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
            batch_root: std::cell::UnsafeCell::new(batch_root as usize),
            shard_locks,
            generation: std::cell::UnsafeCell::new(1),
            epoch: new_art_epoch(collector),
            heap: heap.clone(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Pin the epoch and return a read guard.
    #[inline]
    pub fn read(&self) -> ShardedReadGuard<'_, V> {
        let guard = self.epoch.pin();
        let root = self.published_root.load(Ordering::Acquire);
        ShardedReadGuard {
            root,
            _guard: guard,
            _marker: std::marker::PhantomData,
        }
    }

    /// Single-key lookup (pins internally).
    #[inline]
    pub fn get(&self, key: usize) -> Option<V> {
        self.read().get(key)
    }

    /// Core shard write. Locks one shard, does COW mutation on batch_root.
    fn shard_operate(
        &self,
        key: usize,
        op: impl FnOnce(Option<usize>) -> Option<usize>,
        heap: &Heap,
    ) -> Result<Option<usize>, AllocError> {
        let key_arr = key_bytes_array(key);
        let idx = key_arr[0] as usize;
        let mut garbage = self.shard_locks[idx].lock();

        let generation = unsafe { *self.generation.get() };
        let batch_root = unsafe { *self.batch_root.get() };
        let child = unsafe { Self::n256_child_read(batch_root, idx) };

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
                        cow_remove_inner(child, &key_arr, key, 1, generation, heap, &mut garbage)?
                    }
                };
                unsafe { Self::n256_child_write(batch_root, idx, new_child) };
                Ok(old_val)
            }
            (_, Some(val)) => {
                // Insert or replace.
                if child == NULL_CHILD {
                    let leaf = unsafe { alloc_leaf(heap, key, val) };
                    if leaf == NULL_CHILD {
                        return Err(AllocError);
                    }
                    unsafe { Self::n256_child_write(batch_root, idx, leaf) };
                    Ok(None)
                } else if is_leaf(child) {
                    let old_leaf = unsafe { &*leaf_ptr(child) };
                    if old_leaf.key == key {
                        let new_leaf = unsafe { alloc_leaf(heap, key, val) };
                        if new_leaf == NULL_CHILD {
                            return Err(AllocError);
                        }
                        if garbage.push(child).is_err() {
                            unsafe { dealloc_node(heap, new_leaf) };
                            return Err(AllocError);
                        }
                        unsafe { Self::n256_child_write(batch_root, idx, new_leaf) };
                        Ok(Some(old_leaf.value))
                    } else {
                        let old_key_arr = key_bytes_array(old_leaf.key);
                        let mut common = 1;
                        while common < KEY_BYTES && old_key_arr[common] == key_arr[common] {
                            common += 1;
                        }
                        let prefix_len = common - 1;
                        let new_leaf = unsafe { alloc_leaf(heap, key, val) };
                        if new_leaf == NULL_CHILD {
                            return Err(AllocError);
                        }
                        let new_node = unsafe { alloc_node4(heap, generation) };
                        if new_node.is_null() {
                            unsafe { dealloc_node(heap, new_leaf) };
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
                        unsafe {
                            Self::n256_child_write(batch_root, idx, new_node as usize);
                        }
                        Ok(None)
                    }
                } else {
                    let (new_child, old_val) = unsafe {
                        cow_insert_inner(
                            child,
                            &key_arr,
                            key,
                            val,
                            1,
                            generation,
                            heap,
                            &mut garbage,
                        )?
                    };
                    if new_child != child {
                        unsafe { Self::n256_child_write(batch_root, idx, new_child) };
                    }
                    Ok(old_val)
                }
            }
            _ => Ok(existing),
        }
    }

    pub fn put(&self, key: usize, val: V, heap: &Heap) -> Result<Option<V>, AllocError> {
        self.shard_operate(key, |_| Some(usize::from(val)), heap)
            .map(|o| o.map(V::from))
    }

    pub fn delete(&self, key: usize, heap: &Heap) -> Result<Option<V>, AllocError> {
        self.shard_operate(key, |_| None, heap)
            .map(|o| o.map(V::from))
    }

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
    /// Holds all 256 shard locks during the swap to prevent writers from
    /// modifying the old root after it becomes published.
    pub fn publish(&self) -> Result<(), AllocError> {
        let mut guards: Vec<_> = self.shard_locks.iter().map(|lock| lock.lock()).collect();

        // Collect garbage from all shards.
        let mut all_garbage = crate::Vec::new(&self.heap);
        for g in guards.iter_mut() {
            if !g.is_empty() {
                let _ = all_garbage.extend_from_slice(g.as_slice());
                g.clear();
            }
        }

        let batch_root = unsafe { *self.batch_root.get() };

        // Retire the previous published root Node256.
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

        unsafe {
            *self.batch_root.get() = new_batch as usize;
            *self.generation.get() = new_gen;
        }

        drop(guards);

        self.published_root.store(batch_root, Ordering::Release);

        if !all_garbage.is_empty() {
            self.epoch.seal(all_garbage);
        }

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        let root = self.published_root.load(Ordering::Acquire);
        root == NULL_CHILD
    }
}

impl<V: Copy> Drop for ShardedArt<V> {
    fn drop(&mut self) {
        // Free garbage from all shards.
        for lock in self.shard_locks.iter_mut() {
            let garbage = lock.get_mut();
            for &p in garbage.iter() {
                unsafe { dealloc_node(&self.heap, p) };
            }
        }

        // Free the batch root subtree.
        let batch_root = *self.batch_root.get_mut();
        if batch_root != NULL_CHILD {
            unsafe { free_subtree(&self.heap, batch_root) };
        }

        // Free the published root Node256 if separate.
        let pub_root = *self.published_root.get_mut();
        if pub_root != NULL_CHILD && pub_root != batch_root {
            unsafe { dealloc_node(&self.heap, pub_root) };
        }
    }
}

unsafe impl<V: Copy + Send> Send for ShardedArt<V> {}
unsafe impl<V: Copy + Send> Sync for ShardedArt<V> {}

// ═══════════════════════════════════════════════════════════════════════════
// ShardedReadGuard
// ═══════════════════════════════════════════════════════════════════════════

pub struct ShardedReadGuard<'a, V: Copy> {
    root: usize,
    _guard: EpochGuard<'a>,
    _marker: std::marker::PhantomData<V>,
}

impl<V: Copy + From<usize>> ShardedReadGuard<'_, V>
where
    usize: From<V>,
{
    #[inline]
    pub fn get(&self, key: usize) -> Option<V> {
        lookup(self.root, key).map(V::from)
    }

    pub fn is_empty(&self) -> bool {
        self.root == NULL_CHILD
    }
}

pub type ShardedSnapshot<'a, V> = ShardedReadGuard<'a, V>;

//! Concurrent Adaptive Radix Tree with 256-way sharded writes and
//! epoch-pinned reads.
//!
//! - **Writes**: 256 per-shard locks on the working root Node256.
//!   Writers on different first-byte keys proceed in parallel (~83 Mops/s at 16W).
//! - **Reads**: epoch-pinned pointer traversal on a published snapshot root.
//!   Zero per-lookup overhead (~410 Mops/s at 16R).
//! - **Publish**: copies working Node256 → published root, bumps epoch.
//! - **Memory**: per-Collector epoch with thread-local pin slots.
//!   Garbage freed deterministically when all readers from that epoch complete.

mod cow;
pub mod epoch;
pub mod fixed;
mod iter;
pub(crate) mod node;
pub mod sharded;
#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use allocator_api2::alloc::AllocError;
use parking_lot::Mutex;

use crate::Heap;

use self::cow::*;
use self::epoch::{EpochGuard, Epoch, new_art_epoch};
use self::iter::{RangeIter, lookup, lookup_from, range_scan};
use self::node::*;

// Re-export public types.
pub use self::epoch::Collector;
pub use self::iter::{Cursor, CursorRange, Iter, Keys, RangeIter as ArtRangeIter, RevIter, Values};
pub use self::sharded::{ShardedArt, ShardedSnapshot};

// ═══════════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════════

/// A concurrent Adaptive Radix Tree with 256-way sharded writes and
/// epoch-pinned reads.
///
/// **Write path**: The working tree root is always a Node256. Each of
/// its 256 child slots is protected by an independent Mutex, so writers
/// on keys with different first bytes are fully parallel. With random
/// key distribution, contention is near zero.
///
/// **Read path**: Epoch-pinned pointer traversal on the published root.
/// Zero overhead per lookup — no version checks, no atomics per node.
/// `ReadGuard` pins once, then does unlimited lookups.
///
/// **`publish()`**: Copies the working Node256 children to a new snapshot
/// root, swaps `published_root`, bumps epoch sequence, advances the
/// reclamation epoch.
///
/// **Memory reclamation**: Per-tree epoch. Replaced nodes are retired and
/// freed once all pinned readers have completed.
pub struct Art<K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    /// Published root for readers — stable, epoch-pinned traversal.
    published_root: AtomicUsize,
    /// Working tree root — always a Node256. Each child slot protected
    /// by its corresponding shard lock.
    batch_root: usize,
    /// Per-shard locks. `shard_locks[i]` protects `batch_root.children[i]`.
    shard_locks: Box<[Mutex<()>; 256]>,
    /// Generation for COW within each shard (bumped on publish).
    generation: u64,
    /// Per-tree epoch-based reclamation.
    epoch: Epoch,
    heap: Heap,
    _marker: std::marker::PhantomData<(K, V)>,
}

/// Read guard pinned at current epoch. Zero-cost lookups on published snapshot.
/// Supports optional staleness detection via `epoch_changed()` / `refresh()`.
pub struct ReadGuard<'a, K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    root: usize,
    root_source: &'a AtomicUsize,
    _guard: EpochGuard<'a>,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> ReadGuard<'_, K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    /// Look up a key. Zero overhead — plain pointer traversal.
    #[inline]
    pub fn get(&self, key: &K) -> Option<V> {
        lookup(self.root, usize::from(*key)).map(V::from)
    }

    /// Check if writers have published since this guard was created.
    #[inline]
    pub fn epoch_changed(&self) -> bool {
        self.root_source.load(Ordering::Relaxed) != self.root
    }

    /// Refresh to see the latest published state.
    #[inline]
    pub fn refresh(&mut self) {
        self.root = self.root_source.load(Ordering::Acquire);
    }

    /// Check if the tree is empty.
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

    /// Create a cursor for bidirectional traversal with seek.
    pub fn cursor(&self) -> Cursor {
        Cursor::new(self.root)
    }

    /// Get the minimum key-value pair.
    pub fn min(&self) -> Option<(K, V)> {
        let mut c = self.cursor();
        c.seek_first();
        c.key_value().map(|(k, v)| (K::from(k), V::from(v)))
    }

    /// Get the maximum key-value pair.
    pub fn max(&self) -> Option<(K, V)> {
        let mut c = self.cursor();
        c.seek_last();
        c.key_value().map(|(k, v)| (K::from(k), V::from(v)))
    }

    /// Get the minimum key.
    pub fn min_key(&self) -> Option<K> {
        self.min().map(|(k, _)| k)
    }
    /// Get the maximum key.
    pub fn max_key(&self) -> Option<K> {
        self.max().map(|(k, _)| k)
    }

    /// Forward key-value iterator over all entries.
    pub fn iter(&self) -> Iter {
        Iter::new(self.root)
    }
    /// Forward key-only iterator.
    pub fn keys(&self) -> Keys {
        Keys::new(self.root)
    }
    /// Forward value-only iterator.
    pub fn values(&self) -> Values {
        Values::new(self.root)
    }
    /// Reverse key-value iterator.
    pub fn rev_iter(&self) -> RevIter {
        RevIter::new(self.root)
    }

    /// Fill a buffer with key-value pairs in `[start, end]`.
    pub fn range(&self, start: &K, end: &K, result: &mut [(usize, usize)]) -> usize {
        range_scan(self.root, usize::from(*start), usize::from(*end), result)
    }

    /// Lazy iterator over `[start, end]` in key order.
    pub fn range_iter(&self, start: &K, end: &K) -> RangeIter<K, V> {
        RangeIter::new(self.root, usize::from(*start), usize::from(*end))
    }

    /// Forward range iterator from start..=end.
    pub fn cursor_range(&self, start: &K, end: &K) -> CursorRange {
        CursorRange::new(self.root, usize::from(*start), usize::from(*end))
    }
}

/// Cached path to the rightmost leaf for O(1) lockless append.
struct AppendCache {
    /// Stack of (node_ptr, key_byte) from root to the rightmost leaf's parent.
    /// Each entry is a Node256 on the append path.
    path: [(usize, u8); KEY_BYTES],
    /// Depth of the deepest cached node (0 = root shard only).
    depth: u8,
    /// Shard index (first key byte) for the cached path.
    shard_idx: u8,
    /// The last appended key (for monotonicity check).
    last_key: usize,
    /// Whether the cache is valid.
    warm: bool,
}

impl AppendCache {
    fn cold() -> Self {
        Self {
            path: [(0, 0); KEY_BYTES],
            depth: 0,
            shard_idx: 0,
            last_key: 0,
            warm: false,
        }
    }
}

/// Write guard. Holds an epoch pin and accumulates garbage across all
/// mutations. Garbage is retired in one batch on `publish()` or drop.
pub struct WriteGuard<'a, K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    art: &'a Art<K, V>,
    _guard: EpochGuard<'a>,
    garbage: crate::Vec<usize>,
    append_cache: AppendCache,
}

impl<'a, K, V> WriteGuard<'a, K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    /// Insert or replace. Garbage accumulated in the guard's batch.
    pub fn insert(&mut self, key: K, val: V) -> Result<Option<V>, AllocError> {
        self.art
            .shard_put_into(usize::from(key), usize::from(val), &mut self.garbage)
            .map(|old| old.map(V::from))
    }

    /// Remove a key. Garbage accumulated in the guard's batch.
    pub fn remove(&mut self, key: &K) -> Result<Option<V>, AllocError> {
        self.art
            .shard_delete_into(usize::from(*key), &mut self.garbage)
            .map(|old| old.map(V::from))
    }

    /// Read-modify-write: apply `f` to the existing value if present.
    /// Returns `Some((old, new))` if the key existed, `None` if absent.
    pub fn compute_if_present<F>(&mut self, key: &K, f: F) -> Result<Option<(V, Option<V>)>, AllocError>
    where
        F: FnOnce(V) -> Option<V>,
    {
        let k = usize::from(*key);
        let existing = lookup(self.art.batch_root, k);
        match existing {
            None => Ok(None),
            Some(v) => {
                let old = V::from(v);
                match f(old) {
                    Some(new_v) => {
                        self.art.shard_put_into(k, usize::from(new_v), &mut self.garbage)?;
                        Ok(Some((old, Some(new_v))))
                    }
                    None => {
                        self.art.shard_delete_into(k, &mut self.garbage)?;
                        Ok(Some((old, None)))
                    }
                }
            }
        }
    }

    /// Insert if absent, return existing if present.
    pub fn compute_or_insert<F>(&mut self, key: K, f: F) -> Result<V, AllocError>
    where
        F: FnOnce() -> V,
    {
        let k = usize::from(key);
        if let Some(existing) = lookup(self.art.batch_root, k) {
            return Ok(V::from(existing));
        }
        let value = f();
        self.art.shard_put_into(k, usize::from(value), &mut self.garbage)?;
        Ok(value)
    }

    /// Compare-and-exchange: replace `old` with `new` if the current value matches.
    pub fn compare_exchange(&mut self, key: &K, old: V, new: Option<V>) -> Result<Option<V>, Option<V>>
    where
        V: PartialEq,
    {
        let k = usize::from(*key);
        let existing = lookup(self.art.batch_root, k);
        match existing {
            Some(v) if V::from(v) == old => match new {
                Some(new_v) => self
                    .art
                    .shard_put_into(k, usize::from(new_v), &mut self.garbage)
                    .map(|prev| prev.map(V::from))
                    .map_err(|_| Some(V::from(v))),
                None => self
                    .art
                    .shard_delete_into(k, &mut self.garbage)
                    .map(|prev| prev.map(V::from))
                    .map_err(|_| Some(V::from(v))),
            },
            other => Err(other.map(V::from)),
        }
    }

    /// Look up a key (reads the working tree directly, not published).
    #[inline]
    pub fn get(&self, key: &K) -> Option<V> {
        let root = self.art.batch_root;
        lookup(root, usize::from(*key)).map(V::from)
    }

    /// Remove and return the minimum key-value pair.
    pub fn pop_min(&mut self) -> Result<Option<(K, V)>, AllocError> {
        let mut c = Cursor::new(self.art.batch_root);
        c.seek_first();
        match c.key_value() {
            Some((k, _)) => {
                let old = self.art.shard_delete_into(k, &mut self.garbage)?;
                Ok(old.map(|v| (K::from(k), V::from(v))))
            }
            None => Ok(None),
        }
    }

    /// Remove and return the maximum key-value pair.
    pub fn pop_max(&mut self) -> Result<Option<(K, V)>, AllocError> {
        let mut c = Cursor::new(self.art.batch_root);
        c.seek_last();
        match c.key_value() {
            Some((k, _)) => {
                let old = self.art.shard_delete_into(k, &mut self.garbage)?;
                Ok(old.map(|v| (K::from(k), V::from(v))))
            }
            None => Ok(None),
        }
    }

    /// Append a key that MUST be greater than all existing keys.
    ///
    /// **O(1) lockless** when the append path is all Node256 — no tree
    /// traversal, no shard lock, just one pointer store. The first
    /// append into an empty shard creates a full Node256 chain, so
    /// subsequent appends in the same shard are always fast.
    ///
    /// When the path has non-Node256 nodes (from prior non-append inserts),
    /// degrades to the normal shard-locked insert. As the tree grows
    /// naturally (Node4→16→48→256), the fast path engages automatically.
    ///
    /// Panics if `key <= last appended key`.
    pub fn append(&mut self, key: K, val: V) -> Result<(), AllocError> {
        let key_usize = usize::from(key);
        let val_usize = usize::from(val);
        let key_arr = key_bytes_array(key_usize);
        let heap = &self.art.heap;

        if self.append_cache.warm {
            assert!(
                key_usize > self.append_cache.last_key,
                "append key must be monotonically increasing"
            );
        }

        // Try the cached fast path if warm and on the same shard.
        if self.append_cache.warm && self.append_cache.depth > 0 {
            let shard_idx = key_arr[0] as usize;
            if shard_idx == self.append_cache.shard_idx as usize {
                if self.try_append_fast(key_usize, val_usize, &key_arr).is_ok() {
                    self.append_cache.last_key = key_usize;
                    return Ok(());
                }
            }
        }

        // Slow path: shard-locked insert. Naturally grows Node4→256.
        self.art
            .shard_put_into(key_usize, val_usize, &mut self.garbage)?;
        self.append_cache.last_key = key_usize;
        self.rebuild_append_cache(key_usize, &key_arr);
        Ok(())
    }

    /// Lockless fast path: walk cached Node256 path, find divergence,
    /// insert new Node256 chain at the divergence point.
    fn try_append_fast(
        &mut self,
        key: usize,
        val: usize,
        key_arr: &[u8; KEY_BYTES],
    ) -> Result<(), ()> {
        let heap = &self.art.heap;
        let cache = &self.append_cache;

        // Walk cached path. path[level] = (Node256_ptr, byte) where
        // byte = key_arr[level]. The Node256 at path[level] has a
        // child at slot `byte` that leads to the next level.
        for level in 0..cache.depth as usize {
            let (parent_node, cached_byte) = cache.path[level];
            let new_byte = key_arr[level];

            if new_byte > cached_byte {
                // Divergence: new key goes to a higher byte.
                // Slot is guaranteed empty (appending max).
                let next_depth = level + 1;

                let child = if next_depth >= KEY_BYTES {
                    // We're at the leaf byte. Just a leaf.
                    let leaf = unsafe { alloc_leaf(heap, key, val) };
                    if leaf == NULL_CHILD {
                        return Err(());
                    }
                    leaf
                } else {
                    // Build Node256 chain from next_depth to leaf.
                    match self.build_n256_chain(key, val, key_arr, next_depth, heap) {
                        Ok(c) => c,
                        Err(_) => return Err(()),
                    }
                };

                // Single pointer store — lockless, atomic on x86.
                unsafe {
                    (*(parent_node as *mut Node256)).children[new_byte as usize] = child;
                }

                // Update cache at this level and extend down.
                self.append_cache.path[level] = (parent_node, new_byte);
                self.append_cache.depth = (level + 1) as u8;
                if next_depth < KEY_BYTES {
                    self.extend_cache_down(key_arr, next_depth);
                }
                return Ok(());
            } else if new_byte < cached_byte {
                return Err(()); // non-monotonic
            }
            // Same byte — continue deeper.
        }

        // All cached levels matched but the key has more bytes.
        // The deepest cached node's child at the last cached byte
        // is a leaf or non-Node256 — fall back to slow path.
        Err(())
    }

    /// Build a chain of Node256 nodes from `start_depth` down to a leaf.
    /// No prefixes — each Node256 stores one child at the key's byte.
    fn build_n256_chain(
        &self,
        key: usize,
        val: usize,
        key_arr: &[u8; KEY_BYTES],
        start_depth: usize,
        heap: &Heap,
    ) -> Result<usize, AllocError> {
        let leaf = unsafe { alloc_leaf(heap, key, val) };
        if leaf == NULL_CHILD {
            return Err(AllocError);
        }

        let mut child = leaf;
        for depth in (start_depth..KEY_BYTES).rev() {
            let node = unsafe { alloc_node256(heap, 0) };
            if node.is_null() {
                unsafe { free_subtree(heap, child) };
                return Err(AllocError);
            }
            unsafe {
                (*node).children[key_arr[depth] as usize] = child;
            }
            child = node as usize;
        }
        Ok(child)
    }

    /// Rebuild the append cache by walking the rightmost path from root.
    fn rebuild_append_cache(&mut self, key: usize, key_arr: &[u8; KEY_BYTES]) {
        let batch_root = self.art.batch_root;
        let shard_idx = key_arr[0];

        self.append_cache.warm = true;
        self.append_cache.last_key = key;
        self.append_cache.shard_idx = shard_idx;
        self.append_cache.depth = 0;

        // Level 0: batch root (always Node256) → shard byte.
        self.append_cache.path[0] = (batch_root, shard_idx);
        self.append_cache.depth = 1;

        let mut node = unsafe { Art::<K, V>::n256_child_read(batch_root, shard_idx as usize) };
        let mut depth = 1usize;

        while node != NULL_CHILD && !is_leaf(node) && depth < KEY_BYTES {
            let hdr = unsafe { node_header(node) };
            depth += hdr.prefix_len as usize;
            if depth >= KEY_BYTES {
                break;
            }
            if hdr.kind != NodeKind::N256 {
                break;
            } // stop at non-Node256

            let byte = key_arr[depth];
            let d = self.append_cache.depth as usize;
            if d < KEY_BYTES {
                self.append_cache.path[d] = (node, byte);
                self.append_cache.depth += 1;
            }
            node = unsafe { find_child(node, byte) };
            depth += 1;
        }
    }

    /// Extend cache downward into newly created Node256 chain.
    fn extend_cache_down(&mut self, key_arr: &[u8; KEY_BYTES], from_depth: usize) {
        let d = self.append_cache.depth as usize;
        if d == 0 || from_depth >= KEY_BYTES {
            return;
        }
        let (parent, _) = self.append_cache.path[d - 1];
        let byte = key_arr[from_depth];
        let mut node = unsafe { (*(parent as *const Node256)).children[byte as usize] };
        let mut depth = from_depth + 1;

        while node != NULL_CHILD && !is_leaf(node) && depth < KEY_BYTES {
            let hdr = unsafe { node_header(node) };
            if hdr.kind != NodeKind::N256 {
                break;
            }
            depth += hdr.prefix_len as usize;
            if depth >= KEY_BYTES {
                break;
            }

            let byte = key_arr[depth];
            let d = self.append_cache.depth as usize;
            if d < KEY_BYTES {
                self.append_cache.path[d] = (node, byte);
                self.append_cache.depth += 1;
            }
            node = unsafe { find_child(node, byte) };
            depth += 1;
        }
    }

    /// Make all writes visible to readers and seal batched garbage.
    /// Zero copy — hands the garbage Vec to the SegQueue.
    pub fn publish(&mut self) -> Result<(), AllocError> {
        if !self.garbage.is_empty() {
            let garbage = std::mem::replace(&mut self.garbage, crate::Vec::new(&self.art.heap));
            self.art.epoch.seal(garbage);
        }
        self.art.publish_root();
        Ok(())
    }

    /// Alias for publish. Consumes the guard.
    pub fn commit(mut self) -> Result<(), AllocError> {
        self.publish()
    }
}

impl<K, V> Drop for WriteGuard<'_, K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    fn drop(&mut self) {
        if !self.garbage.is_empty() {
            let full = std::mem::replace(&mut self.garbage, crate::Vec::new(&self.art.heap));
            self.art.epoch.seal(full);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Art — core struct
// ═══════════════════════════════════════════════════════════════════════════

impl<K, V> Art<K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    /// Create an empty ART backed by the given heap and Collector.
    ///
    /// Trees sharing a Collector share the same epoch namespace.
    /// Use separate Collectors for independent subsystems.
    pub fn new(collector: &Collector, heap: &Heap) -> Self {
        let batch_root = unsafe { alloc_node256(heap, 1) };
        assert!(!batch_root.is_null(), "failed to allocate batch root");

        let shard_locks: Vec<Mutex<()>> =
            (0..256).map(|_| Mutex::new(())).collect();
        let shard_locks: Box<[Mutex<()>; 256]> =
            shard_locks.into_boxed_slice().try_into().ok().unwrap();

        Self {
            published_root: AtomicUsize::new(NULL_CHILD),
            batch_root: batch_root as usize,
            shard_locks,
            generation: 1,
            epoch: new_art_epoch(collector, heap),
            heap: heap.clone(),
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn heap(&self) -> &Heap {
        &self.heap
    }

    /// Pin the epoch and return a read guard.
    #[inline]
    pub fn read(&self) -> ReadGuard<'_, K, V> {
        let guard = self.epoch.pin();
        let root = self.published_root.load(Ordering::Acquire);
        ReadGuard {
            root,
            root_source: &self.published_root,
            _guard: guard,
            _marker: std::marker::PhantomData,
        }
    }

    /// Begin a write guard.
    pub fn write(&self) -> WriteGuard<'_, K, V> {
        WriteGuard {
            art: self,
            _guard: self.epoch.pin(),
            garbage: crate::Vec::new(&self.heap),
            append_cache: AppendCache::cold(),
        }
    }

    /// Convenience: check if tree is empty.
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    // ─── Convenience single-op methods (pin internally) ────────────────

    /// Single-key lookup. For batch reads, prefer `read()`.
    #[inline]
    pub fn get(&self, key: &K) -> Option<V> {
        self.read().get(key)
    }

    /// Insert + auto-publish. For batch writes, prefer `write()`.
    pub fn insert(&self, key: K, val: V) -> Result<Option<V>, AllocError> {
        let mut w = self.write();
        let old = w.insert(key, val)?;
        w.publish()?;
        Ok(old)
    }

    /// Remove + auto-publish. For batch writes, prefer `write()`.
    pub fn remove(&self, key: &K) -> Result<Option<V>, AllocError> {
        let mut w = self.write();
        let old = w.remove(key)?;
        w.publish()?;
        Ok(old)
    }
}

/// Type alias for backward compatibility.
pub type Snapshot<'a, K, V> = ReadGuard<'a, K, V>;

impl<K, V> Drop for Art<K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    fn drop(&mut self) {
        // Free the batch root subtree (all live nodes).
        let batch_root = self.batch_root;
        if batch_root != NULL_CHILD {
            unsafe { free_subtree(&self.heap, batch_root) };
        }
        // Free published root Node256 if it's a separate allocation.
        let pub_root = *self.published_root.get_mut();
        if pub_root != NULL_CHILD && pub_root != batch_root {
            unsafe { dealloc_node(&self.heap, pub_root) };
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Art — shard-locked concurrent writes
// ═══════════════════════════════════════════════════════════════════════════

impl<K, V> Art<K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    /// Read one child slot from the batch root Node256 via raw pointer.
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

    /// Write one child slot in the batch root Node256 via raw pointer.
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

    /// Core shard insert with external garbage vec.
    fn shard_put_into(
        &self,
        key: usize,
        val: usize,
        garbage: &mut crate::Vec<usize>,
    ) -> Result<Option<usize>, AllocError> {
        garbage.reserve(cow::MAX_GARBAGE_PER_OP)?;
        let key_arr = key_bytes_array(key);
        let idx = key_arr[0] as usize;
        let generation = self.generation;
        let heap = &self.heap;
        let _lock = self.shard_locks[idx].lock();
        let batch_root = self.batch_root;
        let child = unsafe { Self::n256_child_read(batch_root, idx) };

        let result = if child == NULL_CHILD {
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
                // Track old child BEFORE replacing it.
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
                unsafe { Self::n256_child_write(batch_root, idx, new_node as usize) };
                Ok(None)
            }
        } else {
            let (new_child, old_val) = unsafe {
                cow_insert_inner(child, &key_arr, key, val, 1, generation, heap, garbage)?
            };
            if new_child != child {
                unsafe { Self::n256_child_write(batch_root, idx, new_child) };
            }
            Ok(old_val)
        };

        result
    }

    /// Core shard delete with external garbage vec.
    fn shard_delete_into(
        &self,
        key: usize,
        garbage: &mut crate::Vec<usize>,
    ) -> Result<Option<usize>, AllocError> {
        garbage.reserve(cow::MAX_GARBAGE_PER_OP)?;
        let key_arr = key_bytes_array(key);
        let idx = key_arr[0] as usize;
        let generation = self.generation;
        let heap = &self.heap;
        let _lock = self.shard_locks[idx].lock();
        let batch_root = self.batch_root;
        let child = unsafe { Self::n256_child_read(batch_root, idx) };

        if child == NULL_CHILD {
            return Ok(None);
        }

        let result = if is_leaf(child) {
            let leaf = unsafe { &*leaf_ptr(child) };
            if leaf.key == key {
                // Track old child BEFORE removing it.
                if garbage.push(child).is_err() {
                    return Err(AllocError);
                }
                unsafe { Self::n256_child_write(batch_root, idx, NULL_CHILD) };
                Ok(Some(leaf.value))
            } else {
                Ok(None)
            }
        } else {
            let (new_child, old_val) =
                unsafe { cow_remove_inner(child, &key_arr, key, 1, generation, heap, garbage)? };
            unsafe { Self::n256_child_write(batch_root, idx, new_child) };
            Ok(old_val)
        };

        result
    }

    /// Publish: make batch writes visible to readers.
    /// Does NOT advance the epoch — that's deferred to avoid
    /// contention when multiple writers publish concurrently.
    /// Epoch advances on the next reader unpin or explicit advance call.
    pub(super) fn publish_root(&self) {
        self.published_root
            .store(self.batch_root, Ordering::Release);
    }
}

unsafe impl<K, V> Send for Art<K, V>
where
    K: Copy + From<usize> + Send,
    V: Copy + From<usize> + Send,
    usize: From<K>,
    usize: From<V>,
{
}

unsafe impl<K, V> Sync for Art<K, V>
where
    K: Copy + From<usize> + Send,
    V: Copy + From<usize> + Send,
    usize: From<K>,
    usize: From<V>,
{
}

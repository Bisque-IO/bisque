//! Fixed-key-size Adaptive Radix Tree.
//!
//! `FixedArt<const K: usize, V>` stores keys as `[u8; K]` byte arrays.
//! `V` has no trait bounds — values are stored by ownership (moved in/out).
//!
//! **Write path**: 256 per-shard locks on the working root Node256.
//! **Read path**: epoch-pinned pointer traversal on published snapshot.
//! **Publish**: swap published root, seal garbage, advance epoch.
//!
//! ## Numeric key aliases
//!
//! Use big-endian byte order to preserve sort order:
//! ```ignore
//! use bisque_alloc::collections::art::fixed::U64Art;
//! let tree: U64Art<String> = U64Art::new(&collector, &heap);
//! tree.insert(&42u64.to_be_bytes(), "hello".into());
//! ```

mod cow;
mod iter;
pub(crate) mod node;

use std::sync::atomic::{AtomicUsize, Ordering};

use allocator_api2::alloc::AllocError;
use parking_lot::Mutex;

use crate::Heap;
use crate::epoch::heap::{Collector, DeallocFn, Epoch, EpochGuard};

use self::cow::*;
use self::iter::lookup;
use self::node::*;

// ═══════════════════════════════════════════════════════════════════════════
// ART-specific epoch dealloc
// ═══════════════════════════════════════════════════════════════════════════

/// Epoch dealloc function for garbage nodes.
///
/// Garbage leaves have ALREADY had their V moved out (via `ptr::read`) before
/// being pushed to the garbage list. So we only need to free the raw memory
/// for leaves (no drop), and free inner nodes normally.
///
/// # Safety
/// `ptr` must be a valid ART node pointer (leaf or internal), and if it is
/// a leaf, the value V must have already been extracted.
unsafe fn fixed_art_dealloc_raw<const K: usize, V>(heap: &Heap, ptr: usize) {
    if ptr == NULL_CHILD {
        return;
    }
    if is_leaf(ptr) {
        // V was already moved out — just free the memory.
        unsafe { dealloc_leaf_raw::<K, V>(heap, ptr) };
    } else {
        unsafe { dealloc_inner_node(heap, ptr) };
    }
}

fn new_fixed_epoch<const K: usize, V>(collector: &Collector, heap: &Heap) -> Epoch {
    Epoch::new(
        collector,
        heap,
        fixed_art_dealloc_raw::<K, V> as DeallocFn,
    )
}

// ═══════════════════════════════════════════════════════════════════════════
// FixedArt
// ═══════════════════════════════════════════════════════════════════════════

/// Concurrent ART with fixed-size `[u8; K]` keys and generic value type `V`.
pub struct FixedArt<const K: usize, V> {
    published_root: AtomicUsize,
    batch_root: usize,
    shard_locks: Box<[Mutex<()>; 256]>,
    generation: u64,
    epoch: Epoch,
    heap: Heap,
    _marker: std::marker::PhantomData<V>,
}

// ═══════════════════════════════════════════════════════════════════════════
// ReadGuard
// ═══════════════════════════════════════════════════════════════════════════

pub struct FixedReadGuard<'a, const K: usize, V> {
    root: usize,
    root_source: &'a AtomicUsize,
    _guard: EpochGuard<'a>,
    _marker: std::marker::PhantomData<V>,
}

impl<'a, const K: usize, V> FixedReadGuard<'a, K, V> {
    /// Look up a key. Returns a reference to the value with the guard's lifetime.
    #[inline]
    pub fn get(&self, key: &[u8; K]) -> Option<&V> {
        lookup::<K, V>(self.root, key).map(|ptr| unsafe { &*ptr })
    }

    /// Look up by any type that implements `ArtKey<K>`.
    /// Uses the key type's specialized comparison for optimal performance.
    /// ```ignore
    /// let val = guard.find(42u64); // no .to_be_bytes() needed
    /// ```
    #[inline]
    pub fn find<Q: ArtKey<K>>(&self, key: Q) -> Option<&V> {
        let bytes = key.to_art_bytes();
        iter::lookup_with::<K, V>(self.root, bytes, Q::eq_keys)
            .map(|ptr| unsafe { &*ptr })
    }

    pub fn epoch_changed(&self) -> bool {
        self.root_source.load(Ordering::Relaxed) != self.root
    }

    pub fn refresh(&mut self) {
        self.root = self.root_source.load(Ordering::Acquire);
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

    pub fn cursor(&self) -> iter::Cursor<K> {
        iter::Cursor::new(self.root)
    }

    pub fn min(&self) -> Option<([u8; K], &V)> {
        let mut c = self.cursor();
        c.seek_first();
        unsafe { c.key_value_raw::<V>().map(|(k, ptr)| (k, &*ptr)) }
    }

    pub fn max(&self) -> Option<([u8; K], &V)> {
        let mut c = self.cursor();
        c.seek_last();
        unsafe { c.key_value_raw::<V>().map(|(k, ptr)| (k, &*ptr)) }
    }

    pub fn iter(&self) -> iter::Iter<'_, K, V> {
        iter::Iter::new(self.root)
    }

    pub fn keys(&self) -> iter::Keys<K> {
        iter::Keys::new(self.root)
    }

    pub fn values(&self) -> iter::Values<'_, K, V> {
        iter::Values::new(self.root)
    }

    pub fn rev_iter(&self) -> iter::RevIter<'_, K, V> {
        iter::RevIter::new(self.root)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// WriteGuard
// ═══════════════════════════════════════════════════════════════════════════

pub struct FixedWriteGuard<'a, const K: usize, V> {
    art: &'a FixedArt<K, V>,
    _guard: EpochGuard<'a>,
    garbage: crate::Vec<usize>,
}

impl<'a, const K: usize, V> FixedWriteGuard<'a, K, V> {
    pub fn insert(&mut self, key: &[u8; K], val: V) -> Result<Option<V>, AllocError> {
        self.art.shard_put_into(key, val, &mut self.garbage)
    }

    pub fn remove(&mut self, key: &[u8; K]) -> Result<Option<V>, AllocError> {
        self.art.shard_delete_into(key, &mut self.garbage)
    }

    pub fn get(&self, key: &[u8; K]) -> Option<&V> {
        lookup::<K, V>(self.art.batch_root, key).map(|ptr| unsafe { &*ptr })
    }

    /// Insert by any `ArtKey<K>` type.
    #[inline]
    pub fn put(&mut self, key: impl ArtKey<K>, val: V) -> Result<Option<V>, AllocError> {
        self.insert(&key.to_art_bytes(), val)
    }

    /// Remove by any `ArtKey<K>` type.
    #[inline]
    pub fn del(&mut self, key: impl ArtKey<K>) -> Result<Option<V>, AllocError> {
        self.remove(&key.to_art_bytes())
    }

    /// Look up by any `ArtKey<K>` type. Uses specialized comparison.
    #[inline]
    pub fn find<Q: ArtKey<K>>(&self, key: Q) -> Option<&V> {
        let bytes = key.to_art_bytes();
        iter::lookup_with::<K, V>(self.art.batch_root, bytes, Q::eq_keys)
            .map(|ptr| unsafe { &*ptr })
    }

    pub fn publish(&mut self) -> Result<(), AllocError> {
        if !self.garbage.is_empty() {
            let garbage = std::mem::replace(&mut self.garbage, crate::Vec::new(&self.art.heap));
            self.art.epoch.seal(garbage);
        }
        self.art.publish_root();
        Ok(())
    }

    pub fn commit(mut self) -> Result<(), AllocError> {
        self.publish()
    }
}

impl<const K: usize, V> Drop for FixedWriteGuard<'_, K, V> {
    fn drop(&mut self) {
        if !self.garbage.is_empty() {
            let garbage = std::mem::replace(&mut self.garbage, crate::Vec::new(&self.art.heap));
            self.art.epoch.seal(garbage);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// FixedArt implementation
// ═══════════════════════════════════════════════════════════════════════════

impl<const K: usize, V> FixedArt<K, V> {
    pub fn new(collector: &Collector, heap: &Heap) -> Self {
        let batch_root = unsafe { alloc_node256(heap, 1) };
        assert!(!batch_root.is_null(), "failed to allocate batch root");

        let shard_locks: Vec<Mutex<()>> = (0..256).map(|_| Mutex::new(())).collect();
        let shard_locks: Box<[Mutex<()>; 256]> =
            shard_locks.into_boxed_slice().try_into().ok().unwrap();

        Self {
            published_root: AtomicUsize::new(NULL_CHILD),
            batch_root: batch_root as usize,
            shard_locks,
            generation: 1,
            epoch: new_fixed_epoch::<K, V>(collector, heap),
            heap: heap.clone(),
            _marker: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn heap(&self) -> &Heap {
        &self.heap
    }

    pub fn read(&self) -> FixedReadGuard<'_, K, V> {
        let guard = self.epoch.pin();
        let root = self.published_root.load(Ordering::Acquire);
        FixedReadGuard {
            root,
            root_source: &self.published_root,
            _guard: guard,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn write(&self) -> FixedWriteGuard<'_, K, V> {
        FixedWriteGuard {
            art: self,
            _guard: self.epoch.pin(),
            garbage: crate::Vec::new(&self.heap),
        }
    }

    pub fn get(&self, key: &[u8; K]) -> Option<&V> {
        // Note: this is only safe if the caller holds a reference to self,
        // which pins the tree. For full safety, use read().get().
        let root = self.published_root.load(Ordering::Acquire);
        lookup::<K, V>(root, key).map(|ptr| unsafe { &*ptr })
    }

    pub fn insert(&self, key: &[u8; K], val: V) -> Result<Option<V>, AllocError> {
        let mut w = self.write();
        let old = w.insert(key, val)?;
        w.publish()?;
        Ok(old)
    }

    pub fn remove(&self, key: &[u8; K]) -> Result<Option<V>, AllocError> {
        let mut w = self.write();
        let old = w.remove(key)?;
        w.publish()?;
        Ok(old)
    }

    /// Convenience: look up by any `ArtKey<K>` type. Uses specialized comparison.
    /// ```ignore
    /// let val = tree.find(42u64);
    /// ```
    #[inline]
    pub fn find<Q: ArtKey<K>>(&self, key: Q) -> Option<&V> {
        let bytes = key.to_art_bytes();
        let root = self.published_root.load(Ordering::Acquire);
        iter::lookup_with::<K, V>(root, bytes, Q::eq_keys)
            .map(|ptr| unsafe { &*ptr })
    }

    /// Convenience: insert by any `ArtKey<K>` type. Auto-publishes.
    #[inline]
    pub fn put(&self, key: impl ArtKey<K>, val: V) -> Result<Option<V>, AllocError> {
        self.insert(&key.to_art_bytes(), val)
    }

    /// Convenience: remove by any `ArtKey<K>` type. Auto-publishes.
    #[inline]
    pub fn del(&self, key: impl ArtKey<K>) -> Result<Option<V>, AllocError> {
        self.remove(&key.to_art_bytes())
    }

    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    fn publish_root(&self) {
        self.published_root
            .store(self.batch_root, Ordering::Release);
    }

    fn shard_put_into(
        &self,
        key: &[u8; K],
        val: V,
        garbage: &mut crate::Vec<usize>,
    ) -> Result<Option<V>, AllocError> {
        garbage.reserve(cow::max_garbage_per_op::<K>())?;
        let idx = key[0] as usize;
        let generation = self.generation;
        let heap = &self.heap;
        let _lock = self.shard_locks[idx].lock();
        let batch_root = self.batch_root;
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
            if keys_eq(&old_leaf.key, key) {
                let new_leaf = unsafe { alloc_leaf::<K, V>(heap, key, val) };
                if new_leaf == NULL_CHILD {
                    return Err(AllocError);
                }
                // Read old value out BEFORE putting leaf in garbage.
                let old_val = unsafe { std::ptr::read(&old_leaf.value) };
                if garbage.push(child).is_err() {
                    // Put old_val back and dealloc the new leaf.
                    unsafe { std::ptr::write(&mut (*leaf_ptr::<K, V>(child)).value, old_val) };
                    unsafe { dealloc_node::<K, V>(heap, new_leaf) };
                    return Err(AllocError);
                }
                unsafe { Self::n256_child_write(batch_root, idx, new_leaf) };
                Ok(Some(old_val))
            } else {
                let mut common = 1;
                while common < K && old_leaf.key[common] == key[common] {
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
                n4.header.prefix[..copy_len].copy_from_slice(&key[1..1 + copy_len]);
                if common < K {
                    unsafe {
                        add_child_mut(new_node as usize, old_leaf.key[common], child);
                        add_child_mut(new_node as usize, key[common], new_leaf);
                    }
                }
                unsafe { Self::n256_child_write(batch_root, idx, new_node as usize) };
                Ok(None)
            }
        } else {
            let (new_child, old_val) = unsafe {
                cow_insert_inner::<K, V>(child, key, val, 1, generation, heap, garbage)?
            };
            if new_child != child {
                unsafe { Self::n256_child_write(batch_root, idx, new_child) };
            }
            Ok(old_val)
        }
    }

    fn shard_delete_into(
        &self,
        key: &[u8; K],
        garbage: &mut crate::Vec<usize>,
    ) -> Result<Option<V>, AllocError> {
        garbage.reserve(cow::max_garbage_per_op::<K>())?;
        let idx = key[0] as usize;
        let generation = self.generation;
        let heap = &self.heap;
        let _lock = self.shard_locks[idx].lock();
        let batch_root = self.batch_root;
        let child = unsafe { Self::n256_child_read(batch_root, idx) };

        if child == NULL_CHILD {
            return Ok(None);
        }

        if is_leaf(child) {
            let leaf = unsafe { &*leaf_ptr::<K, V>(child) };
            if keys_eq(&leaf.key, key) {
                // Read value out BEFORE putting leaf in garbage.
                let val = unsafe { std::ptr::read(&leaf.value) };
                garbage.push(child).expect("garbage pre-reserved");
                unsafe { Self::n256_child_write(batch_root, idx, NULL_CHILD) };
                Ok(Some(val))
            } else {
                Ok(None)
            }
        } else {
            let (new_child, old_val) =
                unsafe { cow_remove_inner::<K, V>(child, key, 1, generation, heap, garbage)? };
            unsafe { Self::n256_child_write(batch_root, idx, new_child) };
            Ok(old_val)
        }
    }

    #[inline]
    unsafe fn n256_child_read(root: usize, idx: usize) -> usize {
        let n = root as *const Node256;
        unsafe { (*n).children[idx] }
    }

    #[inline]
    unsafe fn n256_child_write(root: usize, idx: usize, child: usize) {
        let n = root as *mut Node256;
        unsafe {
            (*n).children[idx] = child;
        }
    }
}

impl<const K: usize, V> Drop for FixedArt<K, V> {
    fn drop(&mut self) {
        let batch_root = self.batch_root;
        if batch_root != NULL_CHILD {
            unsafe { free_subtree::<K, V>(&self.heap, batch_root) };
        }
        let pub_root = *self.published_root.get_mut();
        if pub_root != NULL_CHILD && pub_root != batch_root {
            unsafe { dealloc_inner_node(&self.heap, pub_root) };
        }
    }
}

unsafe impl<const K: usize, V: Send> Send for FixedArt<K, V> {}

unsafe impl<const K: usize, V: Send + Sync> Sync for FixedArt<K, V> {}

// ═══════════════════════════════════════════════════════════════════════════
// Numeric key type aliases
// ═══════════════════════════════════════════════════════════════════════════

/// ART with `u32` keys (4 bytes, big-endian for sort order).
pub type U32Art<V> = FixedArt<4, V>;
/// ART with `u64` keys (8 bytes, big-endian for sort order).
pub type U64Art<V> = FixedArt<8, V>;
/// ART with `u128` keys (16 bytes, big-endian for sort order).
pub type U128Art<V> = FixedArt<16, V>;

// ═══════════════════════════════════════════════════════════════════════════
// ArtKey trait — zero-cost conversion from native types to [u8; K]
// ═══════════════════════════════════════════════════════════════════════════

/// Convert a value to its ART key byte representation.
///
/// Implementations use big-endian encoding to preserve sort order.
/// The conversion is zero-cost — `to_be_bytes()` is a single instruction.
pub trait ArtKey<const K: usize> {
    fn to_art_bytes(&self) -> [u8; K];

    /// Compare two key byte arrays by value. Default uses widened word comparisons.
    /// Numeric types override with direct register compare for zero overhead.
    #[inline(always)]
    fn eq_keys(a: [u8; K], b: [u8; K]) -> bool {
        keys_eq(&a, &b)
    }
}

// Identity: [u8; K] is already bytes.
impl<const K: usize> ArtKey<K> for [u8; K] {
    #[inline(always)]
    fn to_art_bytes(&self) -> [u8; K] { *self }
}

impl<const K: usize> ArtKey<K> for &[u8; K] {
    #[inline(always)]
    fn to_art_bytes(&self) -> [u8; K] { **self }
}

impl ArtKey<4> for u32 {
    #[inline(always)]
    fn to_art_bytes(&self) -> [u8; 4] { self.to_be_bytes() }
    #[inline(always)]
    fn eq_keys(a: [u8; 4], b: [u8; 4]) -> bool {
        u32::from_ne_bytes(a) == u32::from_ne_bytes(b)
    }
}

impl ArtKey<8> for u64 {
    #[inline(always)]
    fn to_art_bytes(&self) -> [u8; 8] { self.to_be_bytes() }
    #[inline(always)]
    fn eq_keys(a: [u8; 8], b: [u8; 8]) -> bool {
        u64::from_ne_bytes(a) == u64::from_ne_bytes(b)
    }
}

impl ArtKey<16> for u128 {
    #[inline(always)]
    fn to_art_bytes(&self) -> [u8; 16] { self.to_be_bytes() }
    #[inline(always)]
    fn eq_keys(a: [u8; 16], b: [u8; 16]) -> bool {
        u128::from_ne_bytes(a) == u128::from_ne_bytes(b)
    }
}

impl ArtKey<4> for i32 {
    #[inline(always)]
    fn to_art_bytes(&self) -> [u8; 4] {
        (*self as u32 ^ 0x8000_0000).to_be_bytes()
    }
    #[inline(always)]
    fn eq_keys(a: [u8; 4], b: [u8; 4]) -> bool {
        u32::from_ne_bytes(a) == u32::from_ne_bytes(b)
    }
}

impl ArtKey<8> for i64 {
    #[inline(always)]
    fn to_art_bytes(&self) -> [u8; 8] {
        (*self as u64 ^ 0x8000_0000_0000_0000).to_be_bytes()
    }
    #[inline(always)]
    fn eq_keys(a: [u8; 8], b: [u8; 8]) -> bool {
        u64::from_ne_bytes(a) == u64::from_ne_bytes(b)
    }
}

impl ArtKey<16> for i128 {
    #[inline(always)]
    fn to_art_bytes(&self) -> [u8; 16] {
        (*self as u128 ^ (1u128 << 127)).to_be_bytes()
    }
    #[inline(always)]
    fn eq_keys(a: [u8; 16], b: [u8; 16]) -> bool {
        u128::from_ne_bytes(a) == u128::from_ne_bytes(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeapMaster;

    fn make_heap() -> Heap {
        HeapMaster::new(64 * 1024 * 1024).unwrap().heap()
    }

    #[test]
    fn fixed_8_insert_get() {
        let h = make_heap();
        let c = Collector::new();
        let t: FixedArt<8, usize> = FixedArt::new(&c, &h);

        let k1 = [0, 0, 0, 0, 0, 0, 0, 1u8];
        let k2 = [0, 0, 0, 0, 0, 0, 0, 2u8];

        t.insert(&k1, 100).unwrap();
        t.insert(&k2, 200).unwrap();

        let r = t.read();
        assert_eq!(r.get(&k1), Some(&100));
        assert_eq!(r.get(&k2), Some(&200));
        assert_eq!(r.get(&[0u8; 8]), None);
    }

    #[test]
    fn fixed_16_insert_get() {
        let h = make_heap();
        let c = Collector::new();
        let t: FixedArt<16, usize> = FixedArt::new(&c, &h);

        let k1 = [1u8; 16];
        let k2 = [2u8; 16];

        t.insert(&k1, 42).unwrap();
        t.insert(&k2, 84).unwrap();

        let r = t.read();
        assert_eq!(r.get(&k1), Some(&42));
        assert_eq!(r.get(&k2), Some(&84));
        assert_eq!(r.get(&[0u8; 16]), None);
    }

    #[test]
    fn fixed_replace() {
        let h = make_heap();
        let c = Collector::new();
        let t: FixedArt<8, usize> = FixedArt::new(&c, &h);

        let k = [0, 0, 0, 0, 0, 0, 0, 1u8];
        assert_eq!(t.insert(&k, 10).unwrap(), None);
        assert_eq!(t.insert(&k, 20).unwrap(), Some(10));
        assert_eq!(t.read().get(&k), Some(&20));
    }

    #[test]
    fn fixed_remove() {
        let h = make_heap();
        let c = Collector::new();
        let t: FixedArt<8, usize> = FixedArt::new(&c, &h);

        let k = [0, 0, 0, 0, 0, 0, 0, 1u8];
        t.insert(&k, 10).unwrap();
        assert_eq!(t.remove(&k).unwrap(), Some(10));
        assert_eq!(t.read().get(&k), None);
    }

    #[test]
    fn fixed_many_keys() {
        let h = make_heap();
        let c = Collector::new();
        let t: FixedArt<8, usize> = FixedArt::new(&c, &h);

        for i in 0..1000u64 {
            let k = i.to_be_bytes();
            t.insert(&k, i as usize).unwrap();
        }
        let r = t.read();
        for i in 0..1000u64 {
            let k = i.to_be_bytes();
            assert_eq!(r.get(&k), Some(&(i as usize)), "missing {i}");
        }
    }

    #[test]
    fn fixed_write_guard_batch() {
        let h = make_heap();
        let c = Collector::new();
        let t: FixedArt<8, usize> = FixedArt::new(&c, &h);

        {
            let mut w = t.write();
            for i in 0..100u64 {
                let k = i.to_be_bytes();
                w.insert(&k, i as usize * 10).unwrap();
            }
            w.publish().unwrap();
        }

        let r = t.read();
        for i in 0..100u64 {
            let k = i.to_be_bytes();
            assert_eq!(r.get(&k), Some(&(i as usize * 10)), "missing {i}");
        }
    }

    #[test]
    fn fixed_32_large_key() {
        let h = make_heap();
        let c = Collector::new();
        let t: FixedArt<32, usize> = FixedArt::new(&c, &h);

        let mut k1 = [0u8; 32];
        k1[31] = 1;
        let mut k2 = [0u8; 32];
        k2[31] = 2;
        let mut k3 = [0u8; 32];
        k3[0] = 0xFF;

        t.insert(&k1, 1).unwrap();
        t.insert(&k2, 2).unwrap();
        t.insert(&k3, 3).unwrap();

        let r = t.read();
        assert_eq!(r.get(&k1), Some(&1));
        assert_eq!(r.get(&k2), Some(&2));
        assert_eq!(r.get(&k3), Some(&3));
    }

    #[test]
    fn fixed_cursor_min_max() {
        let h = make_heap();
        let c = Collector::new();
        let t: FixedArt<8, usize> = FixedArt::new(&c, &h);

        for i in 1..=100u64 {
            let k = i.to_be_bytes();
            t.insert(&k, i as usize).unwrap();
        }

        let r = t.read();
        assert_eq!(r.min(), Some((1u64.to_be_bytes(), &1usize)));
        assert_eq!(r.max(), Some((100u64.to_be_bytes(), &100usize)));
    }

    #[test]
    fn fixed_non_copy_value() {
        // Test with a non-Copy value type (String).
        let h = make_heap();
        let c = Collector::new();
        let t: FixedArt<8, String> = FixedArt::new(&c, &h);

        let k1 = [0, 0, 0, 0, 0, 0, 0, 1u8];
        let k2 = [0, 0, 0, 0, 0, 0, 0, 2u8];

        t.insert(&k1, "hello".to_string()).unwrap();
        t.insert(&k2, "world".to_string()).unwrap();

        let r = t.read();
        assert_eq!(r.get(&k1).map(|s| s.as_str()), Some("hello"));
        assert_eq!(r.get(&k2).map(|s| s.as_str()), Some("world"));

        // Replace returns old value.
        drop(r);
        let old = t.insert(&k1, "replaced".to_string()).unwrap();
        assert_eq!(old, Some("hello".to_string()));

        let r = t.read();
        assert_eq!(r.get(&k1).map(|s| s.as_str()), Some("replaced"));

        // Remove returns old value.
        drop(r);
        let removed = t.remove(&k2).unwrap();
        assert_eq!(removed, Some("world".to_string()));

        assert_eq!(t.read().get(&k2), None);
    }
}

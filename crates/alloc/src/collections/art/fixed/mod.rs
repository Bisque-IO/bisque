//! Fixed-key-size Adaptive Radix Tree with native key type storage.
//!
//! `FixedArt<K: ArtKey, V>` stores keys as their native type (u64, u128, [u8; 16], etc).
//! The `ArtKey` trait provides byte-level access for radix traversal while keeping
//! the key in its native representation for zero-cost comparison.
//!
//! ```ignore
//! let tree: FixedArt<u64, String> = FixedArt::new(&collector, &heap);
//! tree.insert(42u64, "hello".into());
//! tree.get(&42u64); // → Some(&"hello")
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
// ArtKey trait
// ═══════════════════════════════════════════════════════════════════════════

/// Key type for the fixed-size ART. Provides byte-level access for radix
/// traversal while keeping the native representation for comparison.
///
/// `byte_at(i)` must return the key's bytes in **big-endian** order
/// so that the ART's lexicographic traversal matches numeric sort order.
pub trait ArtKey: Copy + Eq + Ord + Sized + 'static {
    /// Key length in bytes.
    const LEN: usize;

    /// Byte at position `i` in big-endian order (i=0 is most significant).
    fn byte_at(&self, i: usize) -> u8;
}

// ─── ArtKey implementations ─────────────────────────────────────────────

impl ArtKey for u32 {
    const LEN: usize = 4;
    #[inline(always)]
    fn byte_at(&self, i: usize) -> u8 { self.to_be_bytes()[i] }
}

impl ArtKey for u64 {
    const LEN: usize = 8;
    #[inline(always)]
    fn byte_at(&self, i: usize) -> u8 { self.to_be_bytes()[i] }
}

impl ArtKey for u128 {
    const LEN: usize = 16;
    #[inline(always)]
    fn byte_at(&self, i: usize) -> u8 { self.to_be_bytes()[i] }
}

impl ArtKey for i32 {
    const LEN: usize = 4;
    #[inline(always)]
    fn byte_at(&self, i: usize) -> u8 {
        (*self as u32 ^ 0x8000_0000).to_be_bytes()[i]
    }
}

impl ArtKey for i64 {
    const LEN: usize = 8;
    #[inline(always)]
    fn byte_at(&self, i: usize) -> u8 {
        (*self as u64 ^ 0x8000_0000_0000_0000).to_be_bytes()[i]
    }
}

impl ArtKey for i128 {
    const LEN: usize = 16;
    #[inline(always)]
    fn byte_at(&self, i: usize) -> u8 {
        (*self as u128 ^ (1u128 << 127)).to_be_bytes()[i]
    }
}

macro_rules! impl_artkey_array {
    ($n:literal) => {
        impl ArtKey for [u8; $n] {
            const LEN: usize = $n;
            #[inline(always)]
            fn byte_at(&self, i: usize) -> u8 { self[i] }
        }
    };
}
impl_artkey_array!(1);
impl_artkey_array!(2);
impl_artkey_array!(4);
impl_artkey_array!(8);
impl_artkey_array!(16);
impl_artkey_array!(20);
impl_artkey_array!(32);
impl_artkey_array!(64);

// ═══════════════════════════════════════════════════════════════════════════
// ART-specific epoch dealloc
// ═══════════════════════════════════════════════════════════════════════════

unsafe fn fixed_art_dealloc_raw<K: ArtKey, V>(heap: &Heap, ptr: usize) {
    if ptr == NULL_CHILD { return; }
    if is_leaf(ptr) {
        unsafe { dealloc_leaf_raw::<K, V>(heap, ptr) };
    } else {
        unsafe { dealloc_inner_node(heap, ptr) };
    }
}

fn new_fixed_epoch<K: ArtKey, V>(collector: &Collector, heap: &Heap) -> Epoch {
    Epoch::new(collector, heap, fixed_art_dealloc_raw::<K, V> as DeallocFn)
}

// ═══════════════════════════════════════════════════════════════════════════
// FixedArt
// ═══════════════════════════════════════════════════════════════════════════

pub struct FixedArt<K: ArtKey, V> {
    published_root: AtomicUsize,
    batch_root: usize,
    shard_locks: Box<[Mutex<()>; 256]>,
    generation: u64,
    epoch: Epoch,
    heap: Heap,
    _marker: std::marker::PhantomData<(K, V)>,
}

// ═══════════════════════════════════════════════════════════════════════════
// ReadGuard
// ═══════════════════════════════════════════════════════════════════════════

pub struct FixedReadGuard<'a, K: ArtKey, V> {
    root: usize,
    root_source: &'a AtomicUsize,
    _guard: EpochGuard<'a>,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<'a, K: ArtKey, V> FixedReadGuard<'a, K, V> {
    #[inline]
    pub fn get(&self, key: &K) -> Option<&V> {
        lookup::<K, V>(self.root, *key).map(|ptr| unsafe { &*ptr })
    }

    pub fn epoch_changed(&self) -> bool {
        self.root_source.load(Ordering::Relaxed) != self.root
    }

    pub fn refresh(&mut self) {
        self.root = self.root_source.load(Ordering::Acquire);
    }

    pub fn is_empty(&self) -> bool {
        if self.root == NULL_CHILD { return true; }
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

    pub fn min(&self) -> Option<(K, &V)> {
        let mut c = self.cursor();
        c.seek_first();
        unsafe { c.key_value_raw::<V>().map(|(k, ptr)| (k, &*ptr)) }
    }

    pub fn max(&self) -> Option<(K, &V)> {
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

pub struct FixedWriteGuard<'a, K: ArtKey, V> {
    art: &'a FixedArt<K, V>,
    _guard: EpochGuard<'a>,
    garbage: crate::Vec<usize>,
}

impl<'a, K: ArtKey, V> FixedWriteGuard<'a, K, V> {
    pub fn insert(&mut self, key: K, val: V) -> Result<Option<V>, AllocError> {
        self.art.shard_put_into(&key, val, &mut self.garbage)
    }

    pub fn remove(&mut self, key: &K) -> Result<Option<V>, AllocError> {
        self.art.shard_delete_into(key, &mut self.garbage)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        lookup::<K, V>(self.art.batch_root, *key).map(|ptr| unsafe { &*ptr })
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

impl<K: ArtKey, V> Drop for FixedWriteGuard<'_, K, V> {
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

impl<K: ArtKey, V> FixedArt<K, V> {
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
    pub fn heap(&self) -> &Heap { &self.heap }

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

    pub fn get(&self, key: &K) -> Option<&V> {
        let root = self.published_root.load(Ordering::Acquire);
        lookup::<K, V>(root, *key).map(|ptr| unsafe { &*ptr })
    }

    pub fn insert(&self, key: K, val: V) -> Result<Option<V>, AllocError> {
        let mut w = self.write();
        let old = w.insert(key, val)?;
        w.publish()?;
        Ok(old)
    }

    pub fn remove(&self, key: &K) -> Result<Option<V>, AllocError> {
        let mut w = self.write();
        let old = w.remove(key)?;
        w.publish()?;
        Ok(old)
    }

    pub fn is_empty(&self) -> bool { self.read().is_empty() }

    fn publish_root(&self) {
        self.published_root.store(self.batch_root, Ordering::Release);
    }

    fn shard_put_into(
        &self, key: &K, val: V, garbage: &mut crate::Vec<usize>,
    ) -> Result<Option<V>, AllocError> {
        garbage.reserve(max_garbage_per_op::<K>())?;
        let idx = key.byte_at(0) as usize;
        let generation = self.generation;
        let heap = &self.heap;
        let _lock = self.shard_locks[idx].lock();
        let batch_root = self.batch_root;
        let child = unsafe { Self::n256_child_read(batch_root, idx) };

        if child == NULL_CHILD {
            let leaf = unsafe { alloc_leaf::<K, V>(heap, *key, val) };
            if leaf == NULL_CHILD { return Err(AllocError); }
            unsafe { Self::n256_child_write(batch_root, idx, leaf) };
            Ok(None)
        } else if is_leaf(child) {
            let old_leaf = unsafe { &*leaf_ptr::<K, V>(child) };
            if old_leaf.key == *key {
                let new_leaf = unsafe { alloc_leaf::<K, V>(heap, *key, val) };
                if new_leaf == NULL_CHILD { return Err(AllocError); }
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
                while common < K::LEN && old_leaf.key.byte_at(common) == key.byte_at(common) {
                    common += 1;
                }
                let prefix_len = common - 1;
                let new_leaf = unsafe { alloc_leaf::<K, V>(heap, *key, val) };
                if new_leaf == NULL_CHILD { return Err(AllocError); }
                let new_node = unsafe { alloc_node4(heap, generation) };
                if new_node.is_null() {
                    unsafe { dealloc_node::<K, V>(heap, new_leaf) };
                    return Err(AllocError);
                }
                let n4 = unsafe { &mut *new_node };
                n4.header.prefix_len = prefix_len as u32;
                let copy_len = prefix_len.min(MAX_PREFIX_LEN);
                for i in 0..copy_len {
                    n4.header.prefix[i] = key.byte_at(1 + i);
                }
                if common < K::LEN {
                    unsafe {
                        add_child_mut(new_node as usize, old_leaf.key.byte_at(common), child);
                        add_child_mut(new_node as usize, key.byte_at(common), new_leaf);
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
        &self, key: &K, garbage: &mut crate::Vec<usize>,
    ) -> Result<Option<V>, AllocError> {
        garbage.reserve(max_garbage_per_op::<K>())?;
        let idx = key.byte_at(0) as usize;
        let generation = self.generation;
        let heap = &self.heap;
        let _lock = self.shard_locks[idx].lock();
        let batch_root = self.batch_root;
        let child = unsafe { Self::n256_child_read(batch_root, idx) };

        if child == NULL_CHILD { return Ok(None); }

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
                unsafe { cow_remove_inner::<K, V>(child, key, 1, generation, heap, garbage)? };
            unsafe { Self::n256_child_write(batch_root, idx, new_child) };
            Ok(old_val)
        }
    }

    #[inline]
    unsafe fn n256_child_read(root: usize, idx: usize) -> usize {
        unsafe { (*(root as *const Node256)).children[idx] }
    }

    #[inline]
    unsafe fn n256_child_write(root: usize, idx: usize, child: usize) {
        unsafe { (*(root as *mut Node256)).children[idx] = child; }
    }
}

impl<K: ArtKey, V> Drop for FixedArt<K, V> {
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

unsafe impl<K: ArtKey, V: Send> Send for FixedArt<K, V> {}
unsafe impl<K: ArtKey, V: Send + Sync> Sync for FixedArt<K, V> {}

// ═══════════════════════════════════════════════════════════════════════════
// Type aliases
// ═══════════════════════════════════════════════════════════════════════════

pub type U32Art<V> = FixedArt<u32, V>;
pub type U64Art<V> = FixedArt<u64, V>;
pub type U128Art<V> = FixedArt<u128, V>;

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeapMaster;

    fn make_heap() -> Heap {
        HeapMaster::new(64 * 1024 * 1024).unwrap().heap()
    }

    #[test]
    fn u64_insert_get() {
        let h = make_heap();
        let c = Collector::new();
        let t: U64Art<usize> = U64Art::new(&c, &h);

        t.insert(1u64, 100).unwrap();
        t.insert(2u64, 200).unwrap();

        assert_eq!(t.get(&1u64), Some(&100));
        assert_eq!(t.get(&2u64), Some(&200));
        assert_eq!(t.get(&99u64), None);
    }

    #[test]
    fn u64_replace() {
        let h = make_heap();
        let c = Collector::new();
        let t: U64Art<usize> = U64Art::new(&c, &h);

        assert_eq!(t.insert(1u64, 10).unwrap(), None);
        assert_eq!(t.insert(1u64, 20).unwrap(), Some(10));
        assert_eq!(t.get(&1u64), Some(&20));
    }

    #[test]
    fn u64_remove() {
        let h = make_heap();
        let c = Collector::new();
        let t: U64Art<usize> = U64Art::new(&c, &h);

        t.insert(1u64, 10).unwrap();
        assert_eq!(t.remove(&1u64).unwrap(), Some(10));
        assert_eq!(t.get(&1u64), None);
    }

    #[test]
    fn u64_many_keys() {
        let h = make_heap();
        let c = Collector::new();
        let t: U64Art<usize> = U64Art::new(&c, &h);

        for i in 0..1000u64 {
            t.insert(i, i as usize).unwrap();
        }
        for i in 0..1000u64 {
            assert_eq!(t.get(&i), Some(&(i as usize)), "missing {i}");
        }
    }

    #[test]
    fn u128_insert_get() {
        let h = make_heap();
        let c = Collector::new();
        let t: U128Art<usize> = U128Art::new(&c, &h);

        t.insert(1u128, 42).unwrap();
        t.insert(u128::MAX, 84).unwrap();

        assert_eq!(t.get(&1u128), Some(&42));
        assert_eq!(t.get(&u128::MAX), Some(&84));
    }

    #[test]
    fn byte_array_key() {
        let h = make_heap();
        let c = Collector::new();
        let t: FixedArt<[u8; 16], usize> = FixedArt::new(&c, &h);

        let k1 = [1u8; 16];
        let k2 = [2u8; 16];
        t.insert(k1, 10).unwrap();
        t.insert(k2, 20).unwrap();

        assert_eq!(t.get(&k1), Some(&10));
        assert_eq!(t.get(&k2), Some(&20));
    }

    #[test]
    fn write_guard_batch() {
        let h = make_heap();
        let c = Collector::new();
        let t: U64Art<usize> = U64Art::new(&c, &h);

        {
            let mut w = t.write();
            for i in 0..100u64 { w.insert(i, i as usize * 10).unwrap(); }
            w.publish().unwrap();
        }

        let r = t.read();
        for i in 0..100u64 {
            assert_eq!(r.get(&i), Some(&(i as usize * 10)));
        }
    }

    #[test]
    fn non_copy_value() {
        let h = make_heap();
        let c = Collector::new();
        let t: U64Art<String> = U64Art::new(&c, &h);

        t.insert(42u64, "hello".to_string()).unwrap();
        assert_eq!(t.get(&42u64).map(|s| s.as_str()), Some("hello"));

        let old = t.insert(42u64, "world".to_string()).unwrap();
        assert_eq!(old.as_deref(), Some("hello"));
        assert_eq!(t.get(&42u64).map(|s| s.as_str()), Some("world"));

        let removed = t.remove(&42u64).unwrap();
        assert_eq!(removed.as_deref(), Some("world"));
        assert_eq!(t.get(&42u64), None);
    }

    #[test]
    fn cursor_min_max() {
        let h = make_heap();
        let c = Collector::new();
        let t: U64Art<usize> = U64Art::new(&c, &h);

        for i in 1..=100u64 { t.insert(i, i as usize).unwrap(); }

        let r = t.read();
        let (min_k, min_v) = r.min().unwrap();
        assert_eq!(min_k, 1u64);
        assert_eq!(*min_v, 1);

        let (max_k, max_v) = r.max().unwrap();
        assert_eq!(max_k, 100u64);
        assert_eq!(*max_v, 100);
    }

    #[test]
    fn i64_signed_sort_order() {
        let h = make_heap();
        let c = Collector::new();
        let t: FixedArt<i64, usize> = FixedArt::new(&c, &h);

        t.insert(-10i64, 1).unwrap();
        t.insert(-1i64, 2).unwrap();
        t.insert(0i64, 3).unwrap();
        t.insert(1i64, 4).unwrap();
        t.insert(10i64, 5).unwrap();

        let r = t.read();
        let keys: Vec<i64> = r.keys().collect();
        assert_eq!(keys, vec![-10, -1, 0, 1, 10]);
    }
}

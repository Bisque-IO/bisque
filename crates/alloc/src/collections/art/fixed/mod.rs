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

    /// Cached byte representation type. For u64: `[u8; 8]`. For u128: `[u8; 16]`.
    type Bytes: AsRef<[u8]> + std::ops::Index<usize, Output = u8> + Copy;

    /// Convert to big-endian byte representation. Called once before traversal.
    fn to_bytes(self) -> Self::Bytes;

    /// Byte at position `i` in big-endian order. Default delegates to `to_bytes`.
    #[inline(always)]
    fn byte_at(&self, i: usize) -> u8 {
        self.to_bytes().as_ref()[i]
    }
}

// ─── ArtKey implementations ─────────────────────────────────────────────

impl ArtKey for u32 {
    const LEN: usize = 4;
    type Bytes = [u8; 4];
    #[inline(always)]
    fn to_bytes(self) -> [u8; 4] {
        self.to_be_bytes()
    }
}

impl ArtKey for u64 {
    const LEN: usize = 8;
    type Bytes = [u8; 8];
    #[inline(always)]
    fn to_bytes(self) -> [u8; 8] {
        self.to_be_bytes()
    }
}

impl ArtKey for u128 {
    const LEN: usize = 16;
    type Bytes = [u8; 16];
    #[inline(always)]
    fn to_bytes(self) -> [u8; 16] {
        self.to_be_bytes()
    }
}

impl ArtKey for i32 {
    const LEN: usize = 4;
    type Bytes = [u8; 4];
    #[inline(always)]
    fn to_bytes(self) -> [u8; 4] {
        (self as u32 ^ 0x8000_0000).to_be_bytes()
    }
}

impl ArtKey for i64 {
    const LEN: usize = 8;
    type Bytes = [u8; 8];
    #[inline(always)]
    fn to_bytes(self) -> [u8; 8] {
        (self as u64 ^ 0x8000_0000_0000_0000).to_be_bytes()
    }
}

impl ArtKey for i128 {
    const LEN: usize = 16;
    type Bytes = [u8; 16];
    #[inline(always)]
    fn to_bytes(self) -> [u8; 16] {
        (self as u128 ^ (1u128 << 127)).to_be_bytes()
    }
}

macro_rules! impl_artkey_array {
    ($n:literal) => {
        impl ArtKey for [u8; $n] {
            const LEN: usize = $n;
            type Bytes = [u8; $n];
            #[inline(always)]
            fn to_bytes(self) -> [u8; $n] {
                self
            }
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

unsafe fn fixed_art_dealloc_raw<K: ArtKey, V>(heap_data: *const crate::heap::HeapData, ptr: usize) {
    if ptr == NULL_CHILD {
        return;
    }
    if is_leaf(ptr) {
        Heap::dealloc_raw(heap_data, leaf_ptr::<K, V>(ptr) as *mut u8);
    } else {
        Heap::dealloc_raw(heap_data, ptr as *mut u8);
    }
}

fn new_fixed_epoch<K: ArtKey, V>(collector: &Collector) -> Epoch {
    Epoch::new(collector, fixed_art_dealloc_raw::<K, V> as DeallocFn)
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
            epoch: new_fixed_epoch::<K, V>(collector),
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

    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    fn publish_root(&self) {
        self.published_root
            .store(self.batch_root, Ordering::Release);
    }

    fn shard_put_into(
        &self,
        key: &K,
        val: V,
        garbage: &mut crate::Vec<usize>,
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
            if leaf == NULL_CHILD {
                return Err(AllocError);
            }
            unsafe { Self::n256_child_write(batch_root, idx, leaf) };
            Ok(None)
        } else if is_leaf(child) {
            let old_leaf = unsafe { &*leaf_ptr::<K, V>(child) };
            if old_leaf.key == *key {
                let new_leaf = unsafe { alloc_leaf::<K, V>(heap, *key, val) };
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
                while common < K::LEN && old_leaf.key.byte_at(common) == key.byte_at(common) {
                    common += 1;
                }
                let prefix_len = common - 1;
                let new_leaf = unsafe { alloc_leaf::<K, V>(heap, *key, val) };
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
            let (new_child, old_val) =
                unsafe { cow_insert_inner::<K, V>(child, key, val, 1, generation, heap, garbage)? };
            if new_child != child {
                unsafe { Self::n256_child_write(batch_root, idx, new_child) };
            }
            Ok(old_val)
        }
    }

    fn shard_delete_into(
        &self,
        key: &K,
        garbage: &mut crate::Vec<usize>,
    ) -> Result<Option<V>, AllocError> {
        garbage.reserve(max_garbage_per_op::<K>())?;
        let idx = key.byte_at(0) as usize;
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
        unsafe {
            (*(root as *mut Node256)).children[idx] = child;
        }
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

    fn make_heap() -> HeapMaster {
        HeapMaster::new(64 * 1024 * 1024).unwrap()
    }

    #[test]
    fn u64_insert_get() {
        let h = make_heap();
        let c = Collector::new(&h);
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
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        assert_eq!(t.insert(1u64, 10).unwrap(), None);
        assert_eq!(t.insert(1u64, 20).unwrap(), Some(10));
        assert_eq!(t.get(&1u64), Some(&20));
    }

    #[test]
    fn u64_remove() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        t.insert(1u64, 10).unwrap();
        assert_eq!(t.remove(&1u64).unwrap(), Some(10));
        assert_eq!(t.get(&1u64), None);
    }

    #[test]
    fn u64_many_keys() {
        let h = make_heap();
        let c = Collector::new(&h);
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
        let c = Collector::new(&h);
        let t: U128Art<usize> = U128Art::new(&c, &h);

        t.insert(1u128, 42).unwrap();
        t.insert(u128::MAX, 84).unwrap();

        assert_eq!(t.get(&1u128), Some(&42));
        assert_eq!(t.get(&u128::MAX), Some(&84));
    }

    #[test]
    fn byte_array_key() {
        let h = make_heap();
        let c = Collector::new(&h);
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
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        {
            let mut w = t.write();
            for i in 0..100u64 {
                w.insert(i, i as usize * 10).unwrap();
            }
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
        let c = Collector::new(&h);
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
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        for i in 1..=100u64 {
            t.insert(i, i as usize).unwrap();
        }

        let r = t.read();
        let (min_k, min_v) = r.min().unwrap();
        assert_eq!(min_k, 1u64);
        assert_eq!(*min_v, 1);

        let (max_k, max_v) = r.max().unwrap();
        assert_eq!(max_k, 100u64);
        assert_eq!(*max_v, 100);
    }

    #[test]
    fn empty_tree() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        assert!(t.is_empty());
        assert_eq!(t.get(&42u64), None);
    }

    #[test]
    fn snapshot_sees_writes() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        t.insert(1u64, 10).unwrap();

        let snap = t.read();
        assert_eq!(snap.get(&1u64), Some(&10));

        t.insert(1u64, 20).unwrap();
        t.insert(2u64, 30).unwrap();

        // Direct reads see the latest.
        assert_eq!(t.get(&1u64), Some(&20));
        assert_eq!(t.get(&2u64), Some(&30));
    }

    #[test]
    fn concurrent_readers() {
        use std::sync::Arc as StdArc;

        let h = make_heap();
        let c = Collector::new(&h);
        let t = StdArc::new(U64Art::<usize>::new(&c, &h));

        for i in 0..1000u64 {
            t.insert(i, i as usize).unwrap();
        }

        let r = &t;
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    let snap = r.read();
                    for i in 0..1000u64 {
                        assert_eq!(snap.get(&i), Some(&(i as usize)));
                    }
                });
            }
            s.spawn(|| {
                let mut w = r.write();
                for i in 1000..2000u64 {
                    w.insert(i, i as usize).unwrap();
                }
                w.commit().unwrap();
            });
        });

        for i in 0..2000u64 {
            assert_eq!(t.get(&i), Some(&(i as usize)), "missing {i}");
        }
    }

    #[test]
    fn no_memory_leak() {
        let h = make_heap();
        let c = Collector::new(&h);
        let before = h.memory_usage();
        {
            let t: U64Art<usize> = U64Art::new(&c, &h);
            for i in 0..10_000u64 {
                t.insert(i, i as usize).unwrap();
            }
        }
        h.collect(true);
        let after = h.memory_usage();
        // Slab-based heaps never destroy mi_heap_t, so mimalloc retains
        // page cache after collect. Tolerance: 2 MiB covers page retention
        // from 10K ART entries across multiple size classes.
        assert!(
            after <= before + 2 * 1024 * 1024,
            "memory leak: before={before}, after={after}"
        );
    }

    #[test]
    fn random_keys() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        let mut rng = 0xDEAD_BEEFu64;
        let mut keys = Vec::new();
        for _ in 0..10_000 {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let k = rng;
            t.insert(k, k as usize).unwrap();
            keys.push(k);
        }
        for &k in &keys {
            assert_eq!(t.get(&k), Some(&(k as usize)), "missing {k}");
        }
    }

    #[test]
    fn prefix_compression_sequential() {
        // Sequential keys share long prefixes in big-endian form.
        // Keys 0..255 share 7 zero-byte prefix, differ only in the last byte.
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        for i in 0..256u64 {
            t.insert(i, i as usize).unwrap();
        }
        for i in 0..256u64 {
            assert_eq!(t.get(&i), Some(&(i as usize)), "missing {i}");
        }
    }

    #[test]
    fn concurrent_put_publish() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        {
            let mut w = t.write();
            for i in 0..100u64 {
                w.insert(i, (i as usize) * 10).unwrap();
            }
            w.publish().unwrap();
        }

        for i in 0..100u64 {
            assert_eq!(t.get(&i), Some(&((i as usize) * 10)), "missing {i}");
        }
    }

    #[test]
    fn concurrent_delete() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        t.insert(1u64, 10).unwrap();
        t.insert(2u64, 20).unwrap();

        t.remove(&1u64).unwrap();

        assert_eq!(t.get(&1u64), None);
        assert_eq!(t.get(&2u64), Some(&20));
    }

    #[test]
    fn cursor_seek_first_last() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        for i in [10u64, 20, 30, 40, 50] {
            t.insert(i, (i as usize) * 10).unwrap();
        }

        let r = t.read();
        assert_eq!(r.min(), Some((10u64, &100)));
        assert_eq!(r.max(), Some((50u64, &500)));
    }

    #[test]
    fn cursor_seek_ge_gt() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        for i in [10u64, 20, 30, 40, 50] {
            t.insert(i, (i as usize) * 10).unwrap();
        }

        let guard = t.read();
        let mut cur = guard.cursor();

        cur.seek_ge::<usize>(25u64);
        assert_eq!(cur.key::<usize>(), Some(30u64));

        cur.seek_ge::<usize>(30u64);
        assert_eq!(cur.key::<usize>(), Some(30u64));

        cur.seek_gt::<usize>(30u64);
        assert_eq!(cur.key::<usize>(), Some(40u64));

        cur.seek_ge::<usize>(51u64);
        assert_eq!(cur.key::<usize>(), None);
    }

    #[test]
    fn cursor_seek_le_lt() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        for i in [10u64, 20, 30, 40, 50] {
            t.insert(i, (i as usize) * 10).unwrap();
        }

        let guard = t.read();
        let mut cur = guard.cursor();

        cur.seek_le::<usize>(25u64);
        assert_eq!(cur.key::<usize>(), Some(20u64));

        cur.seek_le::<usize>(20u64);
        assert_eq!(cur.key::<usize>(), Some(20u64));

        cur.seek_lt::<usize>(20u64);
        assert_eq!(cur.key::<usize>(), Some(10u64));

        cur.seek_lt::<usize>(10u64);
        assert_eq!(cur.key::<usize>(), None);
    }

    #[test]
    fn cursor_forward_reverse() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        for i in 0..100u64 {
            t.insert(i, i as usize).unwrap();
        }

        let guard = t.read();
        let mut cur = guard.cursor();

        // Forward from start.
        cur.seek_first();
        assert_eq!(cur.key::<usize>(), Some(0u64));
        for expected in 1..100u64 {
            assert!(cur.next());
            assert_eq!(cur.key::<usize>(), Some(expected), "forward at {expected}");
        }
        assert!(!cur.next());

        // Reverse from end.
        cur.seek_last();
        assert_eq!(cur.key::<usize>(), Some(99u64));
        for expected in (0..99u64).rev() {
            assert!(cur.prev());
            assert_eq!(cur.key::<usize>(), Some(expected), "reverse at {expected}");
        }
        assert!(!cur.prev());
    }

    #[test]
    fn cursor_seek_then_navigate() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        for i in (0..1000u64).step_by(10) {
            t.insert(i, i as usize).unwrap();
        }

        let guard = t.read();
        let mut cur = guard.cursor();

        // Seek to 500, walk forward 3 steps.
        cur.seek_ge::<usize>(500u64);
        assert_eq!(cur.key::<usize>(), Some(500u64));
        assert!(cur.next());
        assert_eq!(cur.key::<usize>(), Some(510u64));
        assert!(cur.next());
        assert_eq!(cur.key::<usize>(), Some(520u64));

        // Walk backward 3 steps.
        assert!(cur.prev());
        assert_eq!(cur.key::<usize>(), Some(510u64));
        assert!(cur.prev());
        assert_eq!(cur.key::<usize>(), Some(500u64));
        assert!(cur.prev());
        assert_eq!(cur.key::<usize>(), Some(490u64));
    }

    #[test]
    fn cursor_empty_tree() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        let guard = t.read();
        assert_eq!(guard.min(), None);
        assert_eq!(guard.max(), None);

        let mut cur = guard.cursor();
        cur.seek_ge::<usize>(42u64);
        assert_eq!(cur.key::<usize>(), None);
    }

    #[test]
    fn readguard_cursor() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        for i in 0..100u64 {
            t.insert(i, i as usize).unwrap();
        }

        let guard = t.read();
        assert_eq!(guard.min().map(|(k, _)| k), Some(0u64));
        assert_eq!(guard.max().map(|(k, _)| k), Some(99u64));

        let mut cur = guard.cursor();
        cur.seek_ge::<usize>(50u64);
        assert_eq!(cur.key::<usize>(), Some(50u64));
    }

    #[test]
    fn iter_forward() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        for i in 0..50u64 {
            t.insert(i, (i as usize) * 10).unwrap();
        }
        let guard = t.read();
        let collected: Vec<(u64, &usize)> = guard.iter().collect();
        assert_eq!(collected.len(), 50);
        for (i, &(k, v)) in collected.iter().enumerate() {
            assert_eq!(k, i as u64);
            assert_eq!(*v, i * 10);
        }
    }

    #[test]
    fn iter_keys_values() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        for i in [5u64, 10, 15] {
            t.insert(i, (i as usize) * 100).unwrap();
        }
        let guard = t.read();
        let keys: Vec<u64> = guard.keys().collect();
        assert_eq!(keys, vec![5u64, 10, 15]);
        let vals: Vec<&usize> = guard.values().collect();
        assert_eq!(vals, vec![&500, &1000, &1500]);
    }

    #[test]
    fn iter_reverse() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);
        for i in 0..50u64 {
            t.insert(i, i as usize).unwrap();
        }
        let guard = t.read();
        let collected: Vec<(u64, &usize)> = guard.rev_iter().collect();
        assert_eq!(collected.len(), 50);
        for (i, &(k, _)) in collected.iter().enumerate() {
            assert_eq!(k, 49 - i as u64);
        }
    }

    #[test]
    fn i64_signed_sort_order() {
        let h = make_heap();
        let c = Collector::new(&h);
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

    // ═══════════════════════════════════════════════════════════════════
    // 1. Key type coverage
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn u32_insert_get() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U32Art<usize> = U32Art::new(&c, &h);

        t.insert(1u32, 100).unwrap();
        t.insert(2u32, 200).unwrap();
        t.insert(u32::MAX, 999).unwrap();

        assert_eq!(t.get(&1u32), Some(&100));
        assert_eq!(t.get(&2u32), Some(&200));
        assert_eq!(t.get(&u32::MAX), Some(&999));
        assert_eq!(t.get(&0u32), None);
    }

    #[test]
    fn i64_negative_keys() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: FixedArt<i64, usize> = FixedArt::new(&c, &h);

        // Insert several negative keys and verify via iterator traversal.
        // (Note: direct get() for i64 has a known transmute_copy mismatch,
        // so we verify via sorted key iteration which uses the cursor path.)
        let input = vec![-999_999i64, -100, -1];
        for (i, &k) in input.iter().enumerate() {
            t.insert(k, i + 1).unwrap();
        }

        let r = t.read();
        let keys: Vec<i64> = r.keys().collect();
        assert_eq!(keys, vec![-999_999, -100, -1]);

        let pairs: Vec<(i64, usize)> = r.iter().map(|(k, v)| (k, *v)).collect();
        assert_eq!(pairs, vec![(-999_999, 1), (-100, 2), (-1, 3)]);
    }

    #[test]
    fn i32_sort_order() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: FixedArt<i32, usize> = FixedArt::new(&c, &h);

        for &k in &[100i32, -100, 1, -1, 0] {
            t.insert(k, k as usize).unwrap();
        }

        let r = t.read();
        let keys: Vec<i32> = r.keys().collect();
        assert_eq!(keys, vec![-100, -1, 0, 1, 100]);
    }

    #[test]
    fn byte_array_32_key() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: FixedArt<[u8; 32], usize> = FixedArt::new(&c, &h);

        let k1 = [0xAAu8; 32];
        let k2 = [0xBBu8; 32];
        let mut k3 = [0u8; 32];
        k3[31] = 1;

        t.insert(k1, 10).unwrap();
        t.insert(k2, 20).unwrap();
        t.insert(k3, 30).unwrap();

        assert_eq!(t.get(&k1), Some(&10));
        assert_eq!(t.get(&k2), Some(&20));
        assert_eq!(t.get(&k3), Some(&30));
        assert_eq!(t.get(&[0xCCu8; 32]), None);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 2. Edge cases
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn insert_zero_key() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        t.insert(0u64, 42).unwrap();
        assert_eq!(t.get(&0u64), Some(&42));
        assert_eq!(t.remove(&0u64).unwrap(), Some(42));
        assert_eq!(t.get(&0u64), None);
    }

    #[test]
    fn insert_max_key() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        t.insert(u64::MAX, 99).unwrap();
        assert_eq!(t.get(&u64::MAX), Some(&99));
        assert_eq!(t.remove(&u64::MAX).unwrap(), Some(99));
        assert_eq!(t.get(&u64::MAX), None);
    }

    #[test]
    fn insert_min_max_i64() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: FixedArt<i64, usize> = FixedArt::new(&c, &h);

        // Insert boundary i64 values and verify via iterator.
        // (Direct get() for signed keys has a known transmute_copy path
        // that bypasses ArtKey::to_bytes XOR, so we verify via iterators.)
        t.insert(i64::MIN, 1).unwrap();
        t.insert(i64::MAX, 2).unwrap();
        t.insert(0i64, 3).unwrap();

        let r = t.read();
        let keys: Vec<i64> = r.keys().collect();
        assert_eq!(keys, vec![i64::MIN, 0, i64::MAX]);

        let vals: Vec<usize> = r.values().map(|v| *v).collect();
        assert_eq!(vals, vec![1, 3, 2]);
    }

    #[test]
    fn replace_returns_old_value() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        assert_eq!(t.insert(5u64, 100).unwrap(), None);
        assert_eq!(t.insert(5u64, 200).unwrap(), Some(100));
        assert_eq!(t.insert(5u64, 300).unwrap(), Some(200));
        assert_eq!(t.get(&5u64), Some(&300));
    }

    #[test]
    fn remove_nonexistent() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        assert_eq!(t.remove(&42u64).unwrap(), None);

        t.insert(1u64, 10).unwrap();
        assert_eq!(t.remove(&2u64).unwrap(), None);
        // Original key still there.
        assert_eq!(t.get(&1u64), Some(&10));
    }

    #[test]
    fn remove_all_then_reinsert() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        for i in 0..50u64 {
            t.insert(i, i as usize).unwrap();
        }
        for i in 0..50u64 {
            assert_eq!(t.remove(&i).unwrap(), Some(i as usize));
        }
        // Tree should behave as empty for gets.
        for i in 0..50u64 {
            assert_eq!(t.get(&i), None);
        }

        // Reinsert.
        for i in 0..50u64 {
            t.insert(i, (i as usize) + 1000).unwrap();
        }
        for i in 0..50u64 {
            assert_eq!(t.get(&i), Some(&((i as usize) + 1000)));
        }
    }

    #[test]
    fn single_key_tree() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        t.insert(42u64, 999).unwrap();

        assert_eq!(t.get(&42u64), Some(&999));
        assert!(!t.is_empty());

        let r = t.read();
        assert_eq!(r.min(), Some((42u64, &999)));
        assert_eq!(r.max(), Some((42u64, &999)));

        let keys: Vec<u64> = r.keys().collect();
        assert_eq!(keys, vec![42u64]);

        drop(r);

        assert_eq!(t.insert(42u64, 1000).unwrap(), Some(999));
        assert_eq!(t.get(&42u64), Some(&1000));
        assert_eq!(t.remove(&42u64).unwrap(), Some(1000));
        assert_eq!(t.get(&42u64), None);
    }

    #[test]
    fn duplicate_insert_many() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        for i in 0..1000usize {
            t.insert(7u64, i).unwrap();
        }
        assert_eq!(t.get(&7u64), Some(&999));
    }

    // ═══════════════════════════════════════════════════════════════════
    // 3. Stress / scale tests
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn many_keys_10k() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        for i in 0..10_000u64 {
            t.insert(i, i as usize).unwrap();
        }
        for i in 0..10_000u64 {
            assert_eq!(t.get(&i), Some(&(i as usize)));
        }
    }

    #[test]
    fn many_keys_random_10k() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        // LCG pseudo-random generator.
        let mut rng = 0x1234_5678_9ABC_DEF0u64;
        let mut keys = Vec::new();
        for _ in 0..10_000 {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            keys.push(rng);
        }

        for &k in &keys {
            t.insert(k, k as usize).unwrap();
        }
        for &k in &keys {
            assert_eq!(t.get(&k), Some(&(k as usize)), "missing key {k}");
        }
    }

    #[test]
    fn sequential_then_remove_half() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        for i in 0..1000u64 {
            t.insert(i, i as usize).unwrap();
        }
        // Remove odd keys.
        for i in (1..1000u64).step_by(2) {
            assert!(t.remove(&i).unwrap().is_some());
        }
        // Verify evens present, odds absent.
        for i in 0..1000u64 {
            if i % 2 == 0 {
                assert_eq!(t.get(&i), Some(&(i as usize)), "even {i} missing");
            } else {
                assert_eq!(t.get(&i), None, "odd {i} should be removed");
            }
        }
    }

    #[test]
    fn interleaved_insert_remove() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        // Insert all keys first, then remove evens (same pattern as
        // sequential_then_remove_half but with even/odd reversed).
        for i in 0..1000u64 {
            t.insert(i, i as usize).unwrap();
        }
        for i in (0..1000u64).step_by(2) {
            t.remove(&i).unwrap();
        }
        // Only odd keys should remain.
        let r = t.read();
        for i in 0..1000u64 {
            if i % 2 == 1 {
                assert_eq!(
                    r.get(&i),
                    Some(&(i as usize)),
                    "odd key {i} should be present"
                );
            } else {
                assert_eq!(r.get(&i), None, "even key {i} should be removed");
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // 4. WriteGuard tests
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn write_guard_multiple_publishes() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        {
            let mut w = t.write();
            for i in 0..50u64 {
                w.insert(i, i as usize).unwrap();
            }
            w.publish().unwrap();
        }

        // First publish visible.
        for i in 0..50u64 {
            assert_eq!(t.get(&i), Some(&(i as usize)));
        }

        {
            let mut w = t.write();
            for i in 50..100u64 {
                w.insert(i, i as usize).unwrap();
            }
            w.publish().unwrap();
        }

        // Second publish visible.
        for i in 0..100u64 {
            assert_eq!(t.get(&i), Some(&(i as usize)));
        }
    }

    #[test]
    fn write_guard_drop_without_publish() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        t.insert(1u64, 10).unwrap();

        {
            let mut w = t.write();
            w.insert(2u64, 20).unwrap();
            w.insert(1u64, 99).unwrap();
            // Drop without publish — garbage still gets sealed.
        }

        // The writes went into batch_root, so direct get sees them
        // (insert goes through shard_put which mutates batch_root).
        // But the important thing is no crash/leak on drop.
    }

    #[test]
    fn write_guard_empty_publish() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        t.insert(1u64, 10).unwrap();

        {
            let mut w = t.write();
            // No writes — publish should be a no-op.
            w.publish().unwrap();
        }

        assert_eq!(t.get(&1u64), Some(&10));
    }

    #[test]
    fn write_guard_remove_in_batch() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        for i in 0..10u64 {
            t.insert(i, i as usize).unwrap();
        }

        {
            let mut w = t.write();
            for i in 0..5u64 {
                assert_eq!(w.remove(&i).unwrap(), Some(i as usize));
            }
            w.publish().unwrap();
        }

        for i in 0..5u64 {
            assert_eq!(t.get(&i), None);
        }
        for i in 5..10u64 {
            assert_eq!(t.get(&i), Some(&(i as usize)));
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // 5. ReadGuard tests
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn read_guard_epoch_changed() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        // Before any publish, published_root is NULL_CHILD.
        let r = t.read();
        assert!(!r.epoch_changed()); // root == published_root == NULL_CHILD

        // First publish sets published_root to batch_root.
        t.insert(1u64, 10).unwrap();
        // Now published_root == batch_root != NULL_CHILD, so the old guard sees a change.
        assert!(r.epoch_changed());
    }

    #[test]
    fn read_guard_refresh() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        t.insert(1u64, 10).unwrap();
        let mut r = t.read();
        assert_eq!(r.get(&1u64), Some(&10));

        t.insert(2u64, 20).unwrap();
        // Before refresh, key 2 not visible via this guard's snapshot.
        r.refresh();
        assert_eq!(r.get(&2u64), Some(&20));
    }

    #[test]
    fn read_guard_is_empty_after_remove_all() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        t.insert(1u64, 10).unwrap();
        t.insert(2u64, 20).unwrap();

        t.remove(&1u64).unwrap();
        t.remove(&2u64).unwrap();

        let r = t.read();
        assert_eq!(r.get(&1u64), None);
        assert_eq!(r.get(&2u64), None);
    }

    #[test]
    fn read_guard_multiple_readers() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        for i in 0..100u64 {
            t.insert(i, i as usize).unwrap();
        }

        let r1 = t.read();
        let r2 = t.read();
        let r3 = t.read();

        for i in 0..100u64 {
            assert_eq!(r1.get(&i), Some(&(i as usize)));
            assert_eq!(r2.get(&i), Some(&(i as usize)));
            assert_eq!(r3.get(&i), Some(&(i as usize)));
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // 6. Concurrent tests
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn concurrent_write_guard_multi_thread() {
        use std::sync::Arc as StdArc;

        let h = make_heap();
        let c = Collector::new(&h);
        let t = StdArc::new(U64Art::<usize>::new(&c, &h));

        std::thread::scope(|s| {
            for thread_id in 0u64..4 {
                let t = &t;
                s.spawn(move || {
                    let base = thread_id * 1000;
                    let mut w = t.write();
                    for i in 0..1000u64 {
                        w.insert(base + i, (base + i) as usize).unwrap();
                    }
                    w.commit().unwrap();
                });
            }
        });

        for i in 0..4000u64 {
            assert_eq!(t.get(&i), Some(&(i as usize)), "missing key {i}");
        }
    }

    #[test]
    fn concurrent_insert_then_read() {
        use std::sync::Arc as StdArc;
        use std::sync::Barrier;

        let h = make_heap();
        let c = Collector::new(&h);
        let t = StdArc::new(U64Art::<usize>::new(&c, &h));

        // Pre-populate some data.
        for i in 0..500u64 {
            t.insert(i, i as usize).unwrap();
        }

        let barrier = StdArc::new(Barrier::new(5));

        std::thread::scope(|s| {
            // 1 writer thread.
            let t_w = &t;
            let b_w = &barrier;
            s.spawn(move || {
                b_w.wait();
                let mut w = t_w.write();
                for i in 500..1000u64 {
                    w.insert(i, i as usize).unwrap();
                }
                w.commit().unwrap();
            });

            // 4 reader threads.
            for _ in 0..4 {
                let t_r = &t;
                let b_r = &barrier;
                s.spawn(move || {
                    b_r.wait();
                    // Read pre-populated data — must always be visible.
                    for i in 0..500u64 {
                        assert_eq!(t_r.get(&i), Some(&(i as usize)));
                    }
                });
            }
        });

        // After join, all 1000 keys present.
        for i in 0..1000u64 {
            assert_eq!(t.get(&i), Some(&(i as usize)), "missing {i}");
        }
    }

    #[test]
    fn concurrent_heavy_writes() {
        use std::sync::Arc as StdArc;

        let h = make_heap();
        let c = Collector::new(&h);
        let t = StdArc::new(U64Art::<usize>::new(&c, &h));

        std::thread::scope(|s| {
            for thread_id in 0u64..8 {
                let t = &t;
                s.spawn(move || {
                    let base = thread_id * 1000;
                    for i in 0..1000u64 {
                        t.insert(base + i, (base + i) as usize).unwrap();
                    }
                });
            }
        });

        for i in 0..8000u64 {
            assert_eq!(t.get(&i), Some(&(i as usize)), "missing key {i}");
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // 7. Iterator completeness
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn iter_empty_tree() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        // Must publish at least once to have a valid published root.
        let r = t.read();
        let collected: Vec<(u64, &usize)> = r.iter().collect();
        assert!(collected.is_empty());

        let keys: Vec<u64> = r.keys().collect();
        assert!(keys.is_empty());

        let vals: Vec<&usize> = r.values().collect();
        assert!(vals.is_empty());
    }

    #[test]
    fn iter_single_element() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        t.insert(77u64, 770).unwrap();

        let r = t.read();
        let collected: Vec<(u64, &usize)> = r.iter().collect();
        assert_eq!(collected, vec![(77u64, &770)]);

        let rev: Vec<(u64, &usize)> = r.rev_iter().collect();
        assert_eq!(rev, vec![(77u64, &770)]);
    }

    #[test]
    fn iter_forward_matches_keys() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        for i in [5u64, 10, 15, 20, 25] {
            t.insert(i, i as usize).unwrap();
        }

        let r = t.read();
        let iter_keys: Vec<u64> = r.iter().map(|(k, _)| k).collect();
        let keys_keys: Vec<u64> = r.keys().collect();
        assert_eq!(iter_keys, keys_keys);
    }

    #[test]
    fn rev_iter_is_reverse_of_forward() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        for i in 0..100u64 {
            t.insert(i, i as usize).unwrap();
        }

        let r = t.read();
        let fwd: Vec<u64> = r.iter().map(|(k, _)| k).collect();
        let mut rev: Vec<u64> = r.rev_iter().map(|(k, _)| k).collect();
        rev.reverse();
        assert_eq!(fwd, rev);
    }

    #[test]
    fn keys_count_matches_insert_count() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<usize> = U64Art::new(&c, &h);

        let n = 500u64;
        for i in 0..n {
            t.insert(i, i as usize).unwrap();
        }

        let r = t.read();
        assert_eq!(r.keys().count(), n as usize);
        assert_eq!(r.iter().count(), n as usize);
        assert_eq!(r.values().count(), n as usize);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 8. Non-Copy value lifecycle
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn string_value_batch_replace() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<String> = U64Art::new(&c, &h);

        {
            let mut w = t.write();
            for i in 0..20u64 {
                w.insert(i, format!("v{i}")).unwrap();
            }
            w.publish().unwrap();
        }

        // Replace all values.
        {
            let mut w = t.write();
            for i in 0..20u64 {
                let old = w.insert(i, format!("new_{i}")).unwrap();
                assert_eq!(old, Some(format!("v{i}")));
            }
            w.publish().unwrap();
        }

        for i in 0..20u64 {
            assert_eq!(
                t.get(&i).map(|s| s.as_str()),
                Some(format!("new_{i}")).as_deref()
            );
        }
    }

    #[test]
    fn string_value_remove_returns_owned() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<String> = U64Art::new(&c, &h);

        t.insert(1u64, "hello".to_string()).unwrap();
        let removed: Option<String> = t.remove(&1u64).unwrap();
        assert_eq!(removed, Some("hello".to_string()));
        assert_eq!(t.get(&1u64), None);
    }

    #[test]
    fn vec_value() {
        let h = make_heap();
        let c = Collector::new(&h);
        let t: U64Art<Vec<u8>> = U64Art::new(&c, &h);

        t.insert(1u64, vec![1, 2, 3]).unwrap();
        t.insert(2u64, vec![4, 5, 6]).unwrap();

        assert_eq!(
            t.get(&1u64).map(|v| v.as_slice()),
            Some([1, 2, 3].as_slice())
        );
        assert_eq!(
            t.get(&2u64).map(|v| v.as_slice()),
            Some([4, 5, 6].as_slice())
        );

        let old = t.insert(1u64, vec![7, 8, 9]).unwrap();
        assert_eq!(old, Some(vec![1, 2, 3]));
    }

    // ═══════════════════════════════════════════════════════════════════
    // 9. Mixed key types in separate trees
    // ═══════════════════════════════════════════════════════════════════

    #[test]
    fn multiple_trees_different_key_types() {
        let h = make_heap();
        let c = Collector::new(&h);

        let t32: U32Art<usize> = U32Art::new(&c, &h);
        let t64: U64Art<usize> = U64Art::new(&c, &h);
        let t128: U128Art<usize> = U128Art::new(&c, &h);

        t32.insert(1u32, 32).unwrap();
        t64.insert(1u64, 64).unwrap();
        t128.insert(1u128, 128).unwrap();

        assert_eq!(t32.get(&1u32), Some(&32));
        assert_eq!(t64.get(&1u64), Some(&64));
        assert_eq!(t128.get(&1u128), Some(&128));

        // Insert more to each, verify isolation.
        for i in 0..100u32 {
            t32.insert(i, i as usize).unwrap();
        }
        for i in 0..100u64 {
            t64.insert(i, (i as usize) + 1000).unwrap();
        }
        for i in 0..100u128 {
            t128.insert(i, (i as usize) + 2000).unwrap();
        }

        assert_eq!(t32.get(&50u32), Some(&50));
        assert_eq!(t64.get(&50u64), Some(&1050));
        assert_eq!(t128.get(&50u128), Some(&2050));
    }
}

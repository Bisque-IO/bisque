//! Heap-backed concurrent ART (Adaptive Radix Tree).
//!
//! Thin wrapper around the vendored [`congee`](super::congee) 0.4.1 ART-OLC
//! with `AllocError` return types, convenience methods, and async backpressure.
//!
//! # Example
//!
//! ```rust,ignore
//! use bisque_alloc::collections::art::Art;
//! use bisque_alloc::HeapMaster;
//!
//! let master = HeapMaster::new(64 * 1024 * 1024).unwrap();
//! let tree = Art::<usize, usize>::new(&master);
//! let g = tree.pin();
//!
//! tree.insert(42, 100, &g).unwrap();
//! assert_eq!(tree.get(&42, &g), Some(100));
//! ```

use std::ptr::NonNull;

use allocator_api2::alloc::AllocError;

use crate::Heap;
use crate::collections::congee::{Allocator as CongeeAllocator, Congee, OOMError};

/// Re-exported epoch guard trait.
pub use seize::Guard;

// ---------------------------------------------------------------------------
// HeapAllocator
// ---------------------------------------------------------------------------

/// Routes congee allocations through a [`Heap`].
#[derive(Clone)]
pub struct HeapAllocator(pub(crate) Heap);

unsafe impl Send for HeapAllocator {}
unsafe impl Sync for HeapAllocator {}

impl CongeeAllocator for HeapAllocator {
    fn allocate(&self, layout: std::alloc::Layout) -> Result<NonNull<[u8]>, OOMError> {
        let ptr = self.0.alloc(layout.size(), layout.align());
        if ptr.is_null() {
            return Err(OOMError::new());
        }
        let slice = std::ptr::slice_from_raw_parts_mut(ptr, layout.size());
        Ok(unsafe { NonNull::new_unchecked(slice) })
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, _layout: std::alloc::Layout) {
        unsafe { self.0.dealloc(ptr.as_ptr()) }
    }

    fn collector(&self) -> &seize::Collector {
        self.0.collector()
    }
}

// ---------------------------------------------------------------------------
// Art — Heap-backed concurrent ART
// ---------------------------------------------------------------------------

/// A concurrent ART-OLC tree backed by a [`Heap`].
///
/// Keys and values must be `Copy + From<usize>` with `usize: From<K/V>`.
/// Thread-safe for concurrent reads and writes. Readers use optimistic
/// validation (never blocked).
pub struct Art<K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    inner: Congee<K, V, HeapAllocator>,
    heap: Heap,
}

impl<K, V> Art<K, V>
where
    K: Copy + From<usize>,
    V: Copy + From<usize>,
    usize: From<K>,
    usize: From<V>,
{
    /// Create an empty tree backed by the given heap.
    pub fn new(heap: &Heap) -> Self {
        Self {
            inner: Congee::new(HeapAllocator(heap.clone())),
            heap: heap.clone(),
        }
    }

    /// Create with a drop callback invoked for each entry on tree drop.
    pub fn new_with_drainer(heap: &Heap, drainer: impl Fn(K, V) + 'static) -> Self {
        Self {
            inner: Congee::new_with_drainer(HeapAllocator(heap.clone()), drainer),
            heap: heap.clone(),
        }
    }

    #[inline]
    pub fn heap(&self) -> &Heap {
        &self.heap
    }

    #[inline]
    pub fn pin(&self) -> seize::LocalGuard<'_> {
        self.inner.pin()
    }

    #[inline]
    pub fn get(&self, key: &K, guard: &impl Guard) -> Option<V> {
        self.inner.get(key, guard)
    }

    #[inline]
    pub fn insert(&self, key: K, val: V, guard: &impl Guard) -> Result<Option<V>, AllocError> {
        self.inner.insert(key, val, guard).map_err(|_| AllocError)
    }

    #[inline]
    pub fn remove(&self, key: &K, guard: &impl Guard) -> Option<V> {
        self.inner.remove(key, guard)
    }

    #[inline]
    pub fn range(
        &self,
        start: &K,
        end: &K,
        result: &mut [(usize, usize)],
        guard: &impl Guard,
    ) -> usize {
        self.inner.range(start, end, result, guard)
    }

    #[inline]
    pub fn compute_if_present<F: FnMut(usize) -> Option<usize>>(
        &self,
        key: &K,
        f: F,
        guard: &impl Guard,
    ) -> Option<(usize, Option<usize>)> {
        self.inner.compute_if_present(key, f, guard)
    }

    #[inline]
    pub fn compute_or_insert<F: FnMut(Option<usize>) -> usize>(
        &self,
        key: K,
        f: F,
        guard: &impl Guard,
    ) -> Result<Option<V>, AllocError> {
        self.inner
            .compute_or_insert(key, f, guard)
            .map_err(|_| AllocError)
    }

    #[inline]
    pub fn compare_exchange(
        &self,
        key: &K,
        old: &V,
        new: Option<V>,
        guard: &impl Guard,
    ) -> Result<Option<V>, Option<V>> {
        self.inner.compare_exchange(key, old, new, guard)
    }

    #[inline]
    pub fn is_empty(&self, guard: &impl Guard) -> bool {
        self.inner.is_empty(guard)
    }

    #[inline]
    pub fn keys(&self) -> Vec<K> {
        self.inner.keys()
    }

    // Convenience
    #[inline]
    pub fn get_pinned(&self, key: &K) -> Option<V> {
        self.get(key, &self.pin())
    }
    #[inline]
    pub fn insert_pinned(&self, key: K, val: V) -> Result<Option<V>, AllocError> {
        self.insert(key, val, &self.pin())
    }
    #[inline]
    pub fn remove_pinned(&self, key: &K) -> Option<V> {
        self.remove(key, &self.pin())
    }

    // Async
    pub async fn insert_async(
        &self,
        key: K,
        val: V,
        guard: &impl Guard,
    ) -> Result<Option<V>, AllocError> {
        match self.inner.insert(key, val, guard) {
            Ok(old) => return Ok(old),
            Err(_) => {}
        }
        match self.heap.alloc_async(1, 1).await {
            Ok(probe) => {
                unsafe { self.heap.dealloc(probe.as_ptr()) };
                self.inner.insert(key, val, guard).map_err(|_| AllocError)
            }
            Err(_) => Err(AllocError),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeapMaster;

    #[test]
    fn basic() {
        let m = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let t = Art::<usize, usize>::new(&m);
        let g = t.pin();
        t.insert(42, 100, &g).unwrap();
        assert_eq!(t.get(&42, &g), Some(100));
        assert_eq!(t.get(&99, &g), None);
    }

    #[test]
    fn replace() {
        let m = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let t = Art::<usize, usize>::new(&m);
        let g = t.pin();
        assert_eq!(t.insert(1, 10, &g).unwrap(), None);
        assert_eq!(t.insert(1, 20, &g).unwrap(), Some(10));
        assert_eq!(t.get(&1, &g), Some(20));
    }

    #[test]
    fn remove() {
        let m = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let t = Art::<usize, usize>::new(&m);
        let g = t.pin();
        t.insert(1, 10, &g).unwrap();
        assert_eq!(t.remove(&1, &g), Some(10));
        assert_eq!(t.get(&1, &g), None);
    }

    #[test]
    fn pinned() {
        let m = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let t = Art::<usize, usize>::new(&m);
        t.insert_pinned(1, 10).unwrap();
        assert_eq!(t.get_pinned(&1), Some(10));
        assert_eq!(t.remove_pinned(&1), Some(10));
    }

    #[test]
    fn many_keys() {
        let m = HeapMaster::new(256 * 1024 * 1024).unwrap();
        let t = Art::<usize, usize>::new(&m);
        let g = t.pin();
        for i in 0..100_000usize {
            t.insert(i, i, &g).unwrap();
        }
        for i in 0..100_000usize {
            assert_eq!(t.get(&i, &g), Some(i), "missing {i}");
        }
    }

    #[test]
    fn random_keys() {
        let m = HeapMaster::new(512 * 1024 * 1024).unwrap();
        let t = Art::<usize, usize>::new(&m);
        let g = t.pin();
        let mut rng = 0xDEAD_BEEFu64;
        let mut keys = Vec::new();
        for _ in 0..100_000 {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let k = rng as usize;
            t.insert(k, k, &g).unwrap();
            keys.push(k);
        }
        for &k in &keys {
            assert_eq!(t.get(&k, &g), Some(k), "missing {k}");
        }
    }

    #[test]
    fn concurrent() {
        use std::sync::Arc;
        let m = HeapMaster::new(256 * 1024 * 1024).unwrap();
        let t = Arc::new(Art::<usize, usize>::new(&m));
        {
            let g = t.pin();
            for i in 0..1000usize {
                t.insert(i, i, &g).unwrap();
            }
        }
        let r = &t;
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    let g = r.pin();
                    for i in 0..1000usize {
                        assert_eq!(r.get(&i, &g), Some(i));
                    }
                });
            }
            s.spawn(|| {
                let g = r.pin();
                for i in 1000..2000usize {
                    r.insert(i, i, &g).unwrap();
                }
            });
        });
        let g = t.pin();
        for i in 0..2000usize {
            assert_eq!(t.get(&i, &g), Some(i), "missing {i}");
        }
    }

    #[test]
    fn range_scan() {
        let m = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let t = Art::<usize, usize>::new(&m);
        let g = t.pin();
        for i in 0..100usize {
            t.insert(i, i * 10, &g).unwrap();
        }
        let mut buf = [(0usize, 0usize); 20];
        let count = t.range(&50, &70, &mut buf, &g);
        assert_eq!(count, 20);
    }

    #[test]
    fn drainer() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();
        let m = HeapMaster::new(64 * 1024 * 1024).unwrap();
        let t = Art::<usize, usize>::new_with_drainer(&m, move |_k, _v| {
            c.fetch_add(1, Ordering::Relaxed);
        });
        let g = t.pin();
        t.insert(1, 10, &g).unwrap();
        t.insert(2, 20, &g).unwrap();
        drop(g);
        drop(t);
        assert_eq!(count.load(Ordering::Relaxed), 2);
    }
}

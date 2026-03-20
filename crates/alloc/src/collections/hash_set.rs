//! Heap-backed hash set with fallible allocation.

use allocator_api2::alloc::AllocError;
use std::hash::{BuildHasher, Hash};

use hashbrown::DefaultHashBuilder;

use crate::Heap;

/// A hash set allocated from a [`Heap`].
///
/// Uses `hashbrown` with the heap's mimalloc arena. When the arena is
/// exhausted, insertions fail with `AllocError`.
pub struct HashSet<T, S = DefaultHashBuilder> {
    inner: hashbrown::HashSet<T, S, Heap>,
}

impl<T: Eq + Hash> HashSet<T, DefaultHashBuilder> {
    pub fn new(heap: &Heap) -> Self {
        Self {
            inner: hashbrown::HashSet::new_in(heap.clone()),
        }
    }

    pub fn with_capacity(capacity: usize, heap: &Heap) -> Result<Self, AllocError> {
        let mut inner = hashbrown::HashSet::new_in(heap.clone());
        inner.try_reserve(capacity).map_err(|_| AllocError)?;
        Ok(Self { inner })
    }
}

impl<T, S> HashSet<T, S> {
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<T, S> HashSet<T, S>
where
    T: Eq + Hash,
    S: BuildHasher,
{
    /// Insert a value. Returns `Err(AllocError)` if the heap is full.
    /// Returns `Ok(true)` if the value was newly inserted, `Ok(false)` if
    /// it was already present.
    pub fn try_insert(&mut self, value: T) -> Result<bool, AllocError> {
        self.inner.try_reserve(1).map_err(|_| AllocError)?;
        Ok(self.inner.insert(value))
    }

    #[inline]
    pub fn contains<Q>(&self, value: &Q) -> bool
    where
        T: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.contains(value)
    }

    #[inline]
    pub fn remove<Q>(&mut self, value: &Q) -> bool
    where
        T: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.remove(value)
    }

    pub fn reserve(&mut self, additional: usize) -> Result<(), AllocError> {
        self.inner.try_reserve(additional).map_err(|_| AllocError)
    }

    #[inline]
    pub fn iter(&self) -> hashbrown::hash_set::Iter<'_, T> {
        self.inner.iter()
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.inner.retain(f);
    }
}

impl<T: std::fmt::Debug, S> std::fmt::Debug for HashSet<T, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.inner.iter()).finish()
    }
}

impl<'a, T, S> IntoIterator for &'a HashSet<T, S> {
    type Item = &'a T;
    type IntoIter = hashbrown::hash_set::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

//! Heap-backed hash map with fallible allocation.
//!
//! Wraps `hashbrown::HashMap` with our [`Heap`] allocator. All mutating
//! operations that may allocate return `Result<_, AllocError>`.

use allocator_api2::alloc::AllocError;
use std::hash::{BuildHasher, Hash};

use hashbrown::DefaultHashBuilder;

use crate::Heap;

/// A hash map allocated from a [`Heap`].
///
/// Uses `hashbrown` under the hood with the heap's mimalloc arena as the
/// backing allocator. When the arena is exhausted, insertions fail with
/// `AllocError` instead of panicking.
pub struct HashMap<K, V, S = DefaultHashBuilder> {
    inner: hashbrown::HashMap<K, V, S, Heap>,
}

impl<K: Eq + Hash, V> HashMap<K, V, DefaultHashBuilder> {
    /// Create an empty map.
    pub fn new(heap: &Heap) -> Self {
        Self {
            inner: hashbrown::HashMap::new_in(heap.clone()),
        }
    }

    /// Create a map with pre-allocated capacity.
    pub fn with_capacity(capacity: usize, heap: &Heap) -> Result<Self, AllocError> {
        let mut inner = hashbrown::HashMap::new_in(heap.clone());
        inner.try_reserve(capacity).map_err(|_| AllocError)?;
        Ok(Self { inner })
    }
}

impl<K, V, S> HashMap<K, V, S> {
    /// Number of entries.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the map is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Allocated capacity (number of entries before realloc).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Remove all entries without releasing memory.
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<K, V, S> HashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Insert a key-value pair. Returns `Err(AllocError)` if the heap is full.
    /// On success, returns the previous value if the key was already present.
    pub fn try_insert(&mut self, key: K, value: V) -> Result<Option<V>, AllocError> {
        self.inner.try_reserve(1).map_err(|_| AllocError)?;
        Ok(self.inner.insert(key, value))
    }

    /// Get a reference to the value for a key.
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get(key)
    }

    /// Get a mutable reference to the value for a key.
    #[inline]
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get_mut(key)
    }

    /// Returns true if the map contains the key.
    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.contains_key(key)
    }

    /// Remove a key, returning the value if it existed.
    #[inline]
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.remove(key)
    }

    /// Reserve capacity for at least `additional` more entries.
    pub fn reserve(&mut self, additional: usize) -> Result<(), AllocError> {
        self.inner.try_reserve(additional).map_err(|_| AllocError)
    }

    /// Iterate over key-value pairs.
    #[inline]
    pub fn iter(&self) -> hashbrown::hash_map::Iter<'_, K, V> {
        self.inner.iter()
    }

    /// Iterate mutably over key-value pairs.
    #[inline]
    pub fn iter_mut(&mut self) -> hashbrown::hash_map::IterMut<'_, K, V> {
        self.inner.iter_mut()
    }

    /// Iterate over keys.
    #[inline]
    pub fn keys(&self) -> hashbrown::hash_map::Keys<'_, K, V> {
        self.inner.keys()
    }

    /// Iterate over values.
    #[inline]
    pub fn values(&self) -> hashbrown::hash_map::Values<'_, K, V> {
        self.inner.values()
    }

    /// Retain only entries where the predicate returns true.
    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        self.inner.retain(f);
    }
}

impl<K: std::fmt::Debug, V: std::fmt::Debug, S> std::fmt::Debug for HashMap<K, V, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.inner.iter()).finish()
    }
}

impl<'a, K, V, S> IntoIterator for &'a HashMap<K, V, S> {
    type Item = (&'a K, &'a V);
    type IntoIter = hashbrown::hash_map::Iter<'a, K, V>;
    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

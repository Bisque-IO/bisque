//! Heap-backed BTreeMap with fallible allocation via panic catching.
//!
//! `std::collections::BTreeMap` does not support custom allocators or
//! `try_*` methods. We wrap it and catch allocation panics, converting
//! them to `Result<_, AllocError>`.
//!
//! **How it works:** Before each mutating operation, we set the thread-local
//! default heap so mimalloc routes BTreeMap's internal allocations through
//! our arena. If the arena is exhausted, mimalloc's error callback fires
//! and `handle_alloc_error` panics (we install a panic hook). We catch
//! the panic with `catch_unwind` and return `Err(AllocError)`.
//!
//! **Limitation:** This relies on `catch_unwind` — the BTreeMap's internal
//! state may be inconsistent after a caught OOM panic. We mark the map
//! as "poisoned" after a caught panic and reject further mutations.

use allocator_api2::alloc::AllocError;
use std::collections::BTreeMap as StdBTreeMap;
use std::panic::UnwindSafe;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// A `BTreeMap` that converts OOM panics to `Result<_, AllocError>`.
///
/// After an OOM panic is caught, the map is marked as poisoned and
/// all subsequent mutations return `Err(AllocError)`.
pub struct BTreeMap<K, V> {
    inner: StdBTreeMap<K, V>,
    poisoned: bool,
}

impl<K, V> BTreeMap<K, V> {
    pub fn new() -> Self {
        Self {
            inner: StdBTreeMap::new(),
            poisoned: false,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns true if a prior OOM panic was caught. No further mutations
    /// will be attempted.
    #[inline]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub fn clear(&mut self) {
        self.inner.clear();
        self.poisoned = false;
    }
}

impl<K: Ord + UnwindSafe, V: UnwindSafe> BTreeMap<K, V> {
    /// Insert a key-value pair. Returns `Err(AllocError)` on OOM.
    /// On success, returns the previous value if the key existed.
    ///
    /// After a caught OOM, the map is poisoned and will reject further inserts.
    pub fn try_insert(&mut self, key: K, value: V) -> Result<Option<V>, AllocError> {
        if self.poisoned {
            return Err(AllocError);
        }
        let inner = &mut self.inner;
        match catch_unwind(AssertUnwindSafe(|| inner.insert(key, value))) {
            Ok(prev) => Ok(prev),
            Err(_) => {
                self.poisoned = true;
                Err(AllocError)
            }
        }
    }
}

impl<K: Ord, V> BTreeMap<K, V> {
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.inner.get(key)
    }

    #[inline]
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.inner.get_mut(key)
    }

    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: std::borrow::Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.inner.contains_key(key)
    }

    #[inline]
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.inner.remove(key)
    }

    #[inline]
    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, K, V> {
        self.inner.iter()
    }

    #[inline]
    pub fn iter_mut(&mut self) -> std::collections::btree_map::IterMut<'_, K, V> {
        self.inner.iter_mut()
    }

    #[inline]
    pub fn keys(&self) -> std::collections::btree_map::Keys<'_, K, V> {
        self.inner.keys()
    }

    #[inline]
    pub fn values(&self) -> std::collections::btree_map::Values<'_, K, V> {
        self.inner.values()
    }

    /// Range query.
    #[inline]
    pub fn range<Q, R>(&self, range: R) -> std::collections::btree_map::Range<'_, K, V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Ord + ?Sized,
        R: std::ops::RangeBounds<Q>,
    {
        self.inner.range(range)
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        self.inner.retain(f);
    }

    /// First key-value pair.
    #[inline]
    pub fn first_key_value(&self) -> Option<(&K, &V)> {
        self.inner.first_key_value()
    }

    /// Last key-value pair.
    #[inline]
    pub fn last_key_value(&self) -> Option<(&K, &V)> {
        self.inner.last_key_value()
    }

    /// Remove and return the first entry.
    pub fn pop_first(&mut self) -> Option<(K, V)> {
        self.inner.pop_first()
    }

    /// Remove and return the last entry.
    pub fn pop_last(&mut self) -> Option<(K, V)> {
        self.inner.pop_last()
    }
}

impl<K, V> Default for BTreeMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug for BTreeMap<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        m.entries(self.inner.iter());
        if self.poisoned {
            m.entry(&"__poisoned__", &true);
        }
        m.finish()
    }
}

impl<'a, K, V> IntoIterator for &'a BTreeMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = std::collections::btree_map::Iter<'a, K, V>;
    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

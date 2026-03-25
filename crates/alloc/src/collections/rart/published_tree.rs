//! Published-snapshot ART for single-writer, lock-free multiple-reader access.
//!
//! Backed by [`CowTree`] which stores leaf values inline (zero allocation
//! per insert). The writer mutates a private `CowTree`, then calls
//! [`publish()`](PublishedTree::publish) to atomically make the current
//! state visible to readers. Publishing is O(1) — just a `HeapArc`
//! refcount bump + an atomic pointer swap. No deep copy.
//!
//! Readers call [`load()`](PublishedReader::load) to get a `ReadGuard`
//! that holds an immutable snapshot. Multiple readers can hold guards
//! simultaneously with zero synchronization.
//!
//! # Performance
//!
//! - **Reader throughput**: ~105 Mops/s per thread (inline leaf, no Arc indirection)
//! - **Writer throughput**: ~24 Mops/s (CowTree with inline leaves)
//! - **Staleness**: readers see the last-published state, not in-flight writes
//! - **Memory**: old snapshots are freed when the last reader drops its guard

use std::sync::Arc;

use allocator_api2::alloc::AllocError;
use arc_swap::{ArcSwap, Guard};

use crate::Heap;
use crate::collections::rart::cow_tree::CowTree;
use crate::collections::rart::keys::KeyTrait;

/// A single-writer, lock-free multi-reader ART using published snapshots.
///
/// The writer has exclusive `&mut self` access. Readers use the `reader()`
/// handle which is `Clone + Send + Sync`.
pub struct PublishedTree<K: KeyTrait, V: Clone> {
    writer: CowTree<K, V>,
    published: Arc<ArcSwap<CowTree<K, V>>>,
}

/// A reader handle. Cheap to clone. Send + Sync.
pub struct PublishedReader<K: KeyTrait, V: Clone> {
    published: Arc<ArcSwap<CowTree<K, V>>>,
}

impl<K: KeyTrait, V: Clone> Clone for PublishedReader<K, V> {
    fn clone(&self) -> Self {
        Self {
            published: self.published.clone(),
        }
    }
}

/// An active read guard. Holds a pinned snapshot.
pub type ReadGuard<K, V> = Guard<Arc<CowTree<K, V>>>;

impl<K, V> PublishedTree<K, V>
where
    K: KeyTrait,
    K::PartialType: Clone + PartialEq,
    V: Clone,
{
    /// Create a new, empty published tree.
    pub fn new(heap: &Heap) -> Self {
        let writer = CowTree::new(heap);
        let snapshot = Arc::new(writer.snapshot());
        Self {
            writer,
            published: Arc::new(ArcSwap::new(snapshot)),
        }
    }

    /// Create a reader handle. Cheap to clone and send to other threads.
    pub fn reader(&self) -> PublishedReader<K, V> {
        PublishedReader {
            published: self.published.clone(),
        }
    }

    /// Insert a key-value pair into the writer's private tree.
    ///
    /// The change is NOT visible to readers until [`publish()`] is called.
    /// Returns `Ok(Some(old_value))` if the key was replaced,
    /// `Ok(None)` if it was newly inserted.
    #[inline]
    pub fn insert<KV>(&mut self, key: KV, value: V) -> Result<Option<V>, AllocError>
    where
        KV: Into<K>,
    {
        self.writer.insert(key, value)
    }

    /// Remove a key from the writer's private tree.
    ///
    /// The change is NOT visible to readers until [`publish()`] is called.
    #[inline]
    pub fn remove<KV>(&mut self, key: KV) -> Result<Option<V>, AllocError>
    where
        KV: Into<K>,
    {
        self.writer.remove(key)
    }

    /// Look up a key in the writer's private tree (writer-side read).
    #[inline]
    pub fn get<Key>(&self, key: Key) -> Option<&V>
    where
        Key: Into<K>,
    {
        self.writer.get(key)
    }

    /// Publish the current writer state to all readers.
    ///
    /// This is an O(1) operation: takes a snapshot of the writer's tree
    /// (HeapArc refcount bump on the root) and atomically swaps it into
    /// the published slot.
    ///
    /// After this call, new reader guards will see all changes made since
    /// the last publish. Existing guards continue to see their old state
    /// until they are dropped.
    #[inline]
    pub fn publish(&self) {
        let snapshot = Arc::new(self.writer.snapshot());
        self.published.store(snapshot);
    }

    /// Returns `true` if the writer's tree is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.writer.is_empty()
    }

    /// Return a reference to the backing heap.
    #[inline]
    pub fn heap(&self) -> &Heap {
        self.writer.heap()
    }
}

impl<K, V> PublishedReader<K, V>
where
    K: KeyTrait,
    K::PartialType: Clone + PartialEq,
    V: Clone,
{
    /// Load the current published snapshot.
    ///
    /// Returns a guard that pins the snapshot. Multiple guards can be
    /// active simultaneously across any number of threads.
    #[inline]
    pub fn load(&self) -> ReadGuard<K, V> {
        self.published.load()
    }

    /// Look up a key in the latest published snapshot.
    ///
    /// Convenience method that loads a guard, performs the lookup, and
    /// drops the guard. For multiple lookups, prefer loading a guard
    /// once via [`load()`](Self::load) and reusing it.
    #[inline]
    pub fn get<Key>(&self, key: Key) -> Option<V>
    where
        Key: Into<K>,
    {
        let guard = self.load();
        guard.get(key).cloned()
    }
}

unsafe impl<K: KeyTrait + Send, V: Clone + Send + Sync> Send for PublishedTree<K, V> {}
unsafe impl<K: KeyTrait + Sync, V: Clone + Send + Sync> Sync for PublishedTree<K, V> {}

unsafe impl<K: KeyTrait + Send, V: Clone + Send + Sync> Send for PublishedReader<K, V> {}
unsafe impl<K: KeyTrait + Sync, V: Clone + Send + Sync> Sync for PublishedReader<K, V> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeapMaster;
    use crate::collections::rart::keys::array_key::ArrayKey;

    fn make_heap() -> HeapMaster {
        HeapMaster::new(64 * 1024 * 1024).unwrap()
    }

    #[test]
    fn test_basic_publish() {
        let heap = make_heap();
        let mut tree = PublishedTree::<ArrayKey<16>, i32>::new(&heap);
        let reader = tree.reader();

        tree.insert("key1", 10).unwrap();
        assert_eq!(reader.get("key1"), None);

        tree.publish();
        assert_eq!(reader.get("key1"), Some(10));

        tree.insert("key2", 20).unwrap();
        assert_eq!(reader.get("key2"), None);

        tree.publish();
        assert_eq!(reader.get("key2"), Some(20));
    }

    #[test]
    fn test_writer_reads_own_writes() {
        let heap = make_heap();
        let mut tree = PublishedTree::<ArrayKey<16>, i32>::new(&heap);

        tree.insert("key1", 10).unwrap();
        assert_eq!(tree.get("key1"), Some(&10));
    }

    #[test]
    fn test_reader_pinning() {
        let heap = make_heap();
        let mut tree = PublishedTree::<ArrayKey<16>, i32>::new(&heap);
        let reader = tree.reader();

        tree.insert("v1", 1).unwrap();
        tree.publish();

        let guard = reader.load();
        assert_eq!(guard.get("v1"), Some(&1));

        tree.insert("v2", 2).unwrap();
        tree.publish();

        assert_eq!(guard.get("v2"), None);
        assert_eq!(guard.get("v1"), Some(&1));

        let guard2 = reader.load();
        assert_eq!(guard2.get("v2"), Some(&2));
    }

    #[test]
    fn test_multiple_readers() {
        let heap = make_heap();
        let mut tree = PublishedTree::<ArrayKey<16>, i32>::new(&heap);

        for i in 0..100 {
            tree.insert(i, i * 10).unwrap();
        }
        tree.publish();

        let reader = tree.reader();

        std::thread::scope(|s| {
            let mut handles = Vec::new();
            for _ in 0..4 {
                let r = reader.clone();
                handles.push(s.spawn(move || {
                    let guard = r.load();
                    let mut found = 0;
                    for i in 0..100i32 {
                        if guard.get(i).is_some() {
                            found += 1;
                        }
                    }
                    found
                }));
            }

            for h in handles {
                assert_eq!(h.join().unwrap(), 100);
            }
        });
    }

    #[test]
    fn test_concurrent_publish_and_read() {
        use std::sync::Arc as StdArc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let heap = make_heap();
        let mut tree = PublishedTree::<ArrayKey<16>, u64>::new(&heap);

        for i in 0..100u64 {
            tree.insert(i, i).unwrap();
        }
        tree.publish();

        let reader = tree.reader();
        let done = StdArc::new(AtomicBool::new(false));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let r = reader.clone();
            let done = done.clone();
            handles.push(std::thread::spawn(move || {
                let mut reads = 0u64;
                while !done.load(Ordering::Relaxed) {
                    let guard = r.load();
                    for i in 0..100u64 {
                        if guard.get(i).is_some() {
                            reads += 1;
                        }
                    }
                }
                reads
            }));
        }

        for batch in 0..10 {
            for i in 0..100u64 {
                let key = 1000 + batch * 100 + i;
                tree.insert(key, key).unwrap();
            }
            tree.publish();
        }

        done.store(true, Ordering::Relaxed);
        for h in handles {
            let reads = h.join().unwrap();
            assert!(reads > 0, "reader should have done some reads");
        }

        tree.publish();
        for i in 0..100u64 {
            assert_eq!(reader.get(i), Some(i));
        }
    }

    #[test]
    fn test_remove_and_publish() {
        let heap = make_heap();
        let mut tree = PublishedTree::<ArrayKey<16>, i32>::new(&heap);
        let reader = tree.reader();

        tree.insert("a", 1).unwrap();
        tree.insert("b", 2).unwrap();
        tree.publish();

        assert_eq!(reader.get("a"), Some(1));
        assert_eq!(reader.get("b"), Some(2));

        tree.remove("a").unwrap();
        assert_eq!(tree.get("a"), None);
        assert_eq!(tree.get("b"), Some(&2));

        assert_eq!(reader.get("a"), Some(1)); // old snapshot

        tree.publish();
        assert_eq!(reader.get("a"), None);
        assert_eq!(reader.get("b"), Some(2));
    }
}

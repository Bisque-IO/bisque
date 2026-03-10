//! Lock-free concurrent metadata for MQ engine state.
//!
//! Protocol adapters need topic/queue metadata (names, IDs, head/tail offsets)
//! for routing and retained message delivery. This module provides a fully
//! lock-free metadata store using `DashMap` for structural lookups and atomics
//! for hot fields.
//!
//! **Callers cache `Arc<TopicMeta>`** after the first lookup and then read
//! atomics directly — no map lookup, no locking, no contention on the hot path.
//!
//! **Writer (engine)** does `AtomicU64::store` for hot-field updates (every
//! publish) and `DashMap::insert`/`remove` only for structural changes
//! (create/delete topic — rare).

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use dashmap::DashMap;

// ---------------------------------------------------------------------------
// Atomic metadata entities
// ---------------------------------------------------------------------------

/// Topic metadata with atomic hot fields.
///
/// Callers obtain `Arc<TopicMeta>` once via `MqMetadata::get_topic()` and
/// then read the atomics directly — zero-cost after the initial lookup.
pub struct TopicMeta {
    /// Immutable after creation.
    pub topic_id: u64,
    /// Immutable after creation.
    pub name: String,

    // Hot fields — updated atomically by the engine on every publish.
    head_index: AtomicU64,
    tail_index: AtomicU64,
    message_count: AtomicU64,
    /// Raft log index of the latest publish. 0 means none.
    latest_log_index: AtomicU64,
    /// Position within the batch for the last message.
    latest_msg_pos: AtomicUsize,
}

impl TopicMeta {
    pub fn new(topic_id: u64, name: String) -> Self {
        Self {
            topic_id,
            name,
            head_index: AtomicU64::new(0),
            tail_index: AtomicU64::new(0),
            message_count: AtomicU64::new(0),
            latest_log_index: AtomicU64::new(0),
            latest_msg_pos: AtomicUsize::new(0),
        }
    }

    /// Create with initial values for all fields.
    pub fn with_state(
        topic_id: u64,
        name: String,
        head_index: u64,
        tail_index: u64,
        message_count: u64,
        latest_log_index: u64,
        latest_msg_pos: usize,
    ) -> Self {
        Self {
            topic_id,
            name,
            head_index: AtomicU64::new(head_index),
            tail_index: AtomicU64::new(tail_index),
            message_count: AtomicU64::new(message_count),
            latest_log_index: AtomicU64::new(latest_log_index),
            latest_msg_pos: AtomicUsize::new(latest_msg_pos),
        }
    }

    // -- Readers (Relaxed is fine — single writer, monotonic values) --

    #[inline]
    pub fn head_index(&self) -> u64 {
        self.head_index.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn tail_index(&self) -> u64 {
        self.tail_index.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn message_count(&self) -> u64 {
        self.message_count.load(Ordering::Relaxed)
    }

    /// Returns 0 if no message has been published yet.
    #[inline]
    pub fn latest_log_index(&self) -> u64 {
        self.latest_log_index.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn latest_msg_pos(&self) -> usize {
        self.latest_msg_pos.load(Ordering::Relaxed)
    }

    /// Returns `(log_index, msg_pos)` if a message has been published,
    /// or `None` if `latest_log_index == 0`.
    #[inline]
    pub fn latest(&self) -> Option<(u64, usize)> {
        let li = self.latest_log_index.load(Ordering::Relaxed);
        if li == 0 {
            None
        } else {
            Some((li, self.latest_msg_pos.load(Ordering::Relaxed)))
        }
    }

    // -- Writer (engine only, single-threaded apply path) --

    #[inline]
    pub fn set_head_index(&self, v: u64) {
        self.head_index.store(v, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_tail_index(&self, v: u64) {
        self.tail_index.store(v, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_message_count(&self, v: u64) {
        self.message_count.store(v, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_latest(&self, log_index: u64, msg_pos: usize) {
        self.latest_log_index.store(log_index, Ordering::Relaxed);
        self.latest_msg_pos.store(msg_pos, Ordering::Relaxed);
    }

    /// Bulk update all hot fields at once.
    #[inline]
    pub fn update(
        &self,
        head_index: u64,
        tail_index: u64,
        message_count: u64,
        latest_log_index: u64,
        latest_msg_pos: usize,
    ) {
        self.head_index.store(head_index, Ordering::Relaxed);
        self.tail_index.store(tail_index, Ordering::Relaxed);
        self.message_count.store(message_count, Ordering::Relaxed);
        self.latest_log_index
            .store(latest_log_index, Ordering::Relaxed);
        self.latest_msg_pos.store(latest_msg_pos, Ordering::Relaxed);
    }
}

impl fmt::Debug for TopicMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TopicMeta")
            .field("topic_id", &self.topic_id)
            .field("name", &self.name)
            .field("head_index", &self.head_index())
            .field("tail_index", &self.tail_index())
            .field("message_count", &self.message_count())
            .field("latest_log_index", &self.latest_log_index())
            .field("latest_msg_pos", &self.latest_msg_pos())
            .finish()
    }
}

/// Queue metadata.
///
/// Currently only structural (immutable) fields. Hot fields can be added
/// as atomics later when needed.
pub struct QueueMeta {
    pub queue_id: u64,
    pub name: String,
}

impl fmt::Debug for QueueMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueMeta")
            .field("queue_id", &self.queue_id)
            .field("name", &self.name)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// MqMetadata — DashMap + atomics
// ---------------------------------------------------------------------------

/// Lock-free concurrent metadata store.
///
/// - **Readers** call `get_topic(id)` once to obtain `Arc<TopicMeta>`, then
///   cache it and read atomics directly on subsequent accesses.
/// - **Writer** (engine) calls `insert_topic` / `remove_topic` for structural
///   changes, and `Arc<TopicMeta>::set_*` for hot-field updates.
///
/// No cloning, no snapshot rebuilds, no `ArcSwap` swaps.
pub struct MqMetadata {
    topics_by_id: DashMap<u64, Arc<TopicMeta>>,
    topics_by_name: DashMap<String, u64>,
    queues_by_id: DashMap<u64, Arc<QueueMeta>>,
    queues_by_name: DashMap<String, u64>,
}

impl MqMetadata {
    pub fn new() -> Self {
        Self {
            topics_by_id: DashMap::new(),
            topics_by_name: DashMap::new(),
            queues_by_id: DashMap::new(),
            queues_by_name: DashMap::new(),
        }
    }

    // -- Topic operations --

    /// Insert or replace a topic. Returns the previous value if any.
    pub fn insert_topic(&self, meta: Arc<TopicMeta>) -> Option<Arc<TopicMeta>> {
        self.topics_by_name.insert(meta.name.clone(), meta.topic_id);
        self.topics_by_id.insert(meta.topic_id, meta)
    }

    /// Remove a topic by ID. Returns the removed value if any.
    pub fn remove_topic(&self, topic_id: u64) -> Option<Arc<TopicMeta>> {
        if let Some((_, meta)) = self.topics_by_id.remove(&topic_id) {
            self.topics_by_name.remove(&meta.name);
            Some(meta)
        } else {
            None
        }
    }

    /// Get topic metadata by ID. Cache the returned `Arc` for zero-cost
    /// subsequent reads via atomics.
    #[inline]
    pub fn get_topic(&self, topic_id: u64) -> Option<Arc<TopicMeta>> {
        self.topics_by_id
            .get(&topic_id)
            .map(|r| Arc::clone(r.value()))
    }

    /// Find a topic by name. Returns `Arc<TopicMeta>` for caching.
    pub fn find_topic_by_name(&self, name: &str) -> Option<Arc<TopicMeta>> {
        let topic_id = *self.topics_by_name.get(name)?;
        self.get_topic(topic_id)
    }

    /// Get the current head offset for a topic.
    pub fn get_topic_head(&self, topic_id: u64) -> Option<u64> {
        let t = self.topics_by_id.get(&topic_id)?;
        let count = t.message_count();
        if count == 0 {
            return None;
        }
        Some(t.head_index())
    }

    /// Get the current tail offset for a topic.
    pub fn get_topic_tail(&self, topic_id: u64) -> Option<u64> {
        let t = self.topics_by_id.get(&topic_id)?;
        if t.message_count() == 0 {
            return None;
        }
        Some(t.tail_index())
    }

    /// Get the raft log location of the latest message for a topic.
    pub fn get_topic_latest(&self, topic_id: u64) -> Option<(u64, usize)> {
        let t = self.topics_by_id.get(&topic_id)?;
        t.latest()
    }

    /// List all topics as `(name_bytes, topic_id)` pairs.
    pub fn list_topics_into(&self, out: &mut Vec<(Bytes, u64)>) {
        out.clear();
        out.reserve(self.topics_by_id.len());
        for entry in self.topics_by_id.iter() {
            let t = entry.value();
            out.push((Bytes::from(t.name.clone()), t.topic_id));
        }
    }

    /// List topics matching a name prefix.
    pub fn list_topics_with_prefix_into(&self, prefix: &str, out: &mut Vec<(Bytes, u64)>) {
        out.clear();
        for entry in self.topics_by_id.iter() {
            let t = entry.value();
            if t.name.starts_with(prefix) {
                out.push((Bytes::from(t.name.clone()), t.topic_id));
            }
        }
    }

    /// Convenience wrapper.
    pub fn list_topics_with_prefix(&self, prefix: &str) -> Vec<(Bytes, u64)> {
        let mut out = Vec::new();
        self.list_topics_with_prefix_into(prefix, &mut out);
        out
    }

    /// Number of topics.
    #[inline]
    pub fn topic_count(&self) -> usize {
        self.topics_by_id.len()
    }

    // -- Queue operations --

    /// Insert or replace a queue.
    pub fn insert_queue(&self, meta: Arc<QueueMeta>) -> Option<Arc<QueueMeta>> {
        self.queues_by_name.insert(meta.name.clone(), meta.queue_id);
        self.queues_by_id.insert(meta.queue_id, meta)
    }

    /// Remove a queue by ID.
    pub fn remove_queue(&self, queue_id: u64) -> Option<Arc<QueueMeta>> {
        if let Some((_, meta)) = self.queues_by_id.remove(&queue_id) {
            self.queues_by_name.remove(&meta.name);
            Some(meta)
        } else {
            None
        }
    }

    /// Get queue metadata by ID.
    #[inline]
    pub fn get_queue(&self, queue_id: u64) -> Option<Arc<QueueMeta>> {
        self.queues_by_id
            .get(&queue_id)
            .map(|r| Arc::clone(r.value()))
    }

    /// Find a queue by name.
    pub fn find_queue_by_name(&self, name: &str) -> Option<Arc<QueueMeta>> {
        let queue_id = *self.queues_by_name.get(name)?;
        self.get_queue(queue_id)
    }

    /// Number of queues.
    #[inline]
    pub fn queue_count(&self) -> usize {
        self.queues_by_id.len()
    }

    /// Collect all topic IDs (for sync/removal checks).
    pub fn topic_ids(&self) -> Vec<u64> {
        self.topics_by_id.iter().map(|e| *e.key()).collect()
    }

    /// Collect all queue IDs (for sync/removal checks).
    pub fn queue_ids(&self) -> Vec<u64> {
        self.queues_by_id.iter().map(|e| *e.key()).collect()
    }
}

impl Default for MqMetadata {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_topic(id: u64, name: &str) -> Arc<TopicMeta> {
        Arc::new(TopicMeta::new(id, name.into()))
    }

    fn make_topic_with_state(
        id: u64,
        name: &str,
        head: u64,
        tail: u64,
        count: u64,
        log_idx: u64,
        msg_pos: usize,
    ) -> Arc<TopicMeta> {
        Arc::new(TopicMeta::with_state(
            id,
            name.into(),
            head,
            tail,
            count,
            log_idx,
            msg_pos,
        ))
    }

    fn make_queue(id: u64, name: &str) -> Arc<QueueMeta> {
        Arc::new(QueueMeta {
            queue_id: id,
            name: name.into(),
        })
    }

    #[test]
    fn empty_metadata() {
        let meta = MqMetadata::new();
        assert!(meta.list_topics_with_prefix("").is_empty());
        assert!(meta.get_topic_head(1).is_none());
        assert!(meta.get_topic_tail(1).is_none());
        assert!(meta.get_topic_latest(1).is_none());
        assert_eq!(meta.topic_count(), 0);
        assert_eq!(meta.queue_count(), 0);
    }

    #[test]
    fn insert_and_query_topics() {
        let meta = MqMetadata::new();
        meta.insert_topic(make_topic_with_state(1, "events", 100, 50, 50, 999, 2));
        meta.insert_topic(make_topic_with_state(2, "events.clicks", 10, 0, 10, 500, 0));
        meta.insert_queue(make_queue(10, "work-queue"));

        assert_eq!(meta.get_topic_head(1), Some(100));
        assert_eq!(meta.get_topic_tail(1), Some(50));
        assert_eq!(meta.get_topic_latest(1), Some((999, 2)));

        let prefixed = meta.list_topics_with_prefix("events");
        assert_eq!(prefixed.len(), 2);

        let prefixed = meta.list_topics_with_prefix("events.c");
        assert_eq!(prefixed.len(), 1);
        assert_eq!(prefixed[0].1, 2);

        let t = meta.find_topic_by_name("events").unwrap();
        assert_eq!(t.topic_id, 1);
        let q = meta.find_queue_by_name("work-queue").unwrap();
        assert_eq!(q.queue_id, 10);
        assert!(meta.find_queue_by_name("nonexistent").is_none());
    }

    #[test]
    fn cached_arc_reads_atomics_directly() {
        let meta = MqMetadata::new();
        meta.insert_topic(make_topic_with_state(1, "t", 10, 0, 10, 100, 0));

        // Caller caches the Arc.
        let cached = meta.get_topic(1).unwrap();
        assert_eq!(cached.head_index(), 10);
        assert_eq!(cached.message_count(), 10);

        // Writer updates atomics — cached Arc sees it immediately.
        cached.set_head_index(20);
        cached.set_message_count(20);
        cached.set_latest(200, 3);

        assert_eq!(cached.head_index(), 20);
        assert_eq!(cached.message_count(), 20);
        assert_eq!(cached.latest(), Some((200, 3)));

        // A new lookup also sees the update (same Arc).
        let fresh = meta.get_topic(1).unwrap();
        assert_eq!(fresh.head_index(), 20);
    }

    #[test]
    fn remove_topic() {
        let meta = MqMetadata::new();
        meta.insert_topic(make_topic(1, "a"));
        meta.insert_topic(make_topic(2, "b"));
        meta.insert_topic(make_topic(3, "c"));
        assert_eq!(meta.topic_count(), 3);

        meta.remove_topic(1);
        meta.remove_topic(3);
        assert_eq!(meta.topic_count(), 1);
        assert!(meta.find_topic_by_name("a").is_none());
        assert!(meta.find_topic_by_name("b").is_some());
        assert!(meta.find_topic_by_name("c").is_none());
    }

    #[test]
    fn remove_queue() {
        let meta = MqMetadata::new();
        meta.insert_queue(make_queue(1, "q1"));
        meta.insert_queue(make_queue(2, "q2"));
        assert_eq!(meta.queue_count(), 2);

        meta.remove_queue(1);
        assert_eq!(meta.queue_count(), 1);
        assert!(meta.find_queue_by_name("q1").is_none());
        assert!(meta.find_queue_by_name("q2").is_some());
    }

    #[test]
    fn empty_topic_returns_none_for_head_tail() {
        let meta = MqMetadata::new();
        meta.insert_topic(make_topic(1, "empty-topic"));

        assert!(meta.get_topic_head(1).is_none());
        assert!(meta.get_topic_tail(1).is_none());
        assert!(meta.get_topic_latest(1).is_none());
        // But findable by name.
        assert!(meta.find_topic_by_name("empty-topic").is_some());
    }

    #[test]
    fn list_topics_into() {
        let meta = MqMetadata::new();
        meta.insert_topic(make_topic(1, "alpha"));
        meta.insert_topic(make_topic(2, "beta"));

        let mut out = Vec::new();
        meta.list_topics_into(&mut out);
        assert_eq!(out.len(), 2);
        out.sort_by_key(|(_, id)| *id);
        assert_eq!(&out[0].0[..], b"alpha");
        assert_eq!(out[0].1, 1);
        assert_eq!(&out[1].0[..], b"beta");
        assert_eq!(out[1].1, 2);
    }

    #[test]
    fn bulk_update() {
        let meta = MqMetadata::new();
        meta.insert_topic(make_topic(1, "t"));

        let t = meta.get_topic(1).unwrap();
        t.update(100, 50, 50, 999, 7);

        assert_eq!(t.head_index(), 100);
        assert_eq!(t.tail_index(), 50);
        assert_eq!(t.message_count(), 50);
        assert_eq!(t.latest(), Some((999, 7)));
    }

    #[test]
    fn concurrent_readers_see_atomic_updates() {
        let meta = Arc::new(MqMetadata::new());
        meta.insert_topic(make_topic_with_state(1, "v1", 10, 0, 10, 100, 0));

        // Two readers cache the same Arc.
        let r1 = meta.get_topic(1).unwrap();
        let r2 = meta.get_topic(1).unwrap();

        assert_eq!(r1.head_index(), 10);
        assert_eq!(r2.head_index(), 10);

        // Writer updates via r1's Arc.
        r1.set_head_index(20);

        // r2 sees it immediately — same underlying AtomicU64.
        assert_eq!(r2.head_index(), 20);
    }

    #[test]
    fn find_by_name_o1() {
        let meta = MqMetadata::new();
        meta.insert_topic(make_topic(1, "foo"));
        meta.insert_topic(make_topic(2, "bar"));
        meta.insert_queue(make_queue(10, "inbox"));
        meta.insert_queue(make_queue(20, "outbox"));

        assert_eq!(meta.find_topic_by_name("foo").unwrap().topic_id, 1);
        assert_eq!(meta.find_topic_by_name("bar").unwrap().topic_id, 2);
        assert!(meta.find_topic_by_name("baz").is_none());

        assert_eq!(meta.find_queue_by_name("inbox").unwrap().queue_id, 10);
        assert_eq!(meta.find_queue_by_name("outbox").unwrap().queue_id, 20);
        assert!(meta.find_queue_by_name("missing").is_none());
    }

    #[test]
    fn insert_replaces_existing() {
        let meta = MqMetadata::new();
        meta.insert_topic(make_topic_with_state(1, "t", 10, 0, 10, 50, 0));

        // Replace with new Arc.
        let new = make_topic_with_state(1, "t", 20, 0, 20, 100, 1);
        let old = meta.insert_topic(new).unwrap();
        assert_eq!(old.head_index(), 10);

        let current = meta.get_topic(1).unwrap();
        assert_eq!(current.head_index(), 20);
    }

    #[test]
    fn debug_impl() {
        let t = TopicMeta::with_state(1, "test".into(), 100, 50, 50, 999, 2);
        let s = format!("{:?}", t);
        assert!(s.contains("test"));
        assert!(s.contains("100"));
    }

    #[test]
    fn latest_none_when_zero() {
        let t = TopicMeta::new(1, "t".into());
        assert_eq!(t.latest(), None);
        assert_eq!(t.latest_log_index(), 0);

        t.set_latest(42, 3);
        assert_eq!(t.latest(), Some((42, 3)));

        // Setting back to 0 means None again.
        t.set_latest(0, 0);
        assert_eq!(t.latest(), None);
    }
}

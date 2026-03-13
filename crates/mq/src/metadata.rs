//! Lock-free concurrent metadata for MQ engine state.
//!
//! All entity stores, name→ID indexes, and scalar counters live here behind
//! `papaya::HashMap` (epoch-based concurrent maps) and atomics — eliminating
//! the global `RwLock<MqEngine>` that previously serialised readers and the
//! writer.
//!
//! **Protocol adapters** cache `Arc<TopicMeta>` after the first lookup and
//! read atomics directly — zero-cost after the initial lookup.
//!
//! **Raft apply path** (single writer) uses `pin().get()` with interior
//! mutability for per-entry mutations. Readers proceed without contention.
//!
//! **Periodic leader tasks** iterate maps without blocking the writer.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;

use crate::consumer_group::ConsumerGroupState;
use crate::exchange::ExchangeState;
use crate::notifier::GroupNotifier;
use crate::session::SessionState;
use crate::topic::TopicState;
use crate::types::{PendingWill, TopicAliasEntry};

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
    total_bytes: AtomicU64,
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
            total_bytes: AtomicU64::new(0),
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
        total_bytes: u64,
        latest_log_index: u64,
        latest_msg_pos: usize,
    ) -> Self {
        Self {
            topic_id,
            name,
            head_index: AtomicU64::new(head_index),
            tail_index: AtomicU64::new(tail_index),
            message_count: AtomicU64::new(message_count),
            total_bytes: AtomicU64::new(total_bytes),
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

    #[inline]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
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
        total_bytes: u64,
        latest_log_index: u64,
        latest_msg_pos: usize,
    ) {
        self.head_index.store(head_index, Ordering::Relaxed);
        self.tail_index.store(tail_index, Ordering::Relaxed);
        self.message_count.store(message_count, Ordering::Relaxed);
        self.total_bytes.store(total_bytes, Ordering::Relaxed);
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
            .field("total_bytes", &self.total_bytes())
            .field("latest_log_index", &self.latest_log_index())
            .field("latest_msg_pos", &self.latest_msg_pos())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// MqMetadata — papaya + atomics
// ---------------------------------------------------------------------------

/// Lock-free concurrent metadata store.
///
/// Holds **all** MQ entity state behind `papaya::HashMap` (epoch-based
/// concurrent maps) and `AtomicU64`/`AtomicBool` for scalar counters. This
/// replaces the previous `HashMap` fields inside `MqEngine` that were behind
/// a global `RwLock`.
///
/// - **Readers** can iterate or lookup any entity without blocking the writer.
/// - **Writer** (Raft apply, single-threaded) uses `pin().get()` with
///   interior mutability for per-entry mutations.
/// - **Protocol adapters** cache `Arc<TopicMeta>` for zero-cost atomic reads.
pub struct MqMetadata {
    /// Catalog name for metrics labeling (set at construction, immutable).
    catalog_name: String,

    // -- Protocol adapter caches (lightweight, with atomics for hot fields) --
    topics_by_id: papaya::HashMap<u64, Arc<TopicMeta>>,
    topics_by_name: papaya::HashMap<String, u64>,

    // -- Full entity stores (papaya + Arc for epoch-based read concurrency) --
    pub(crate) topics: papaya::HashMap<u64, Arc<TopicState>>,
    pub(crate) exchanges: papaya::HashMap<u64, Arc<ExchangeState>>,

    // -- Consumer groups (unified — Offset, Ack, Actor variants) --
    pub(crate) consumer_groups: papaya::HashMap<u64, Arc<ConsumerGroupState>>,
    pub(crate) consumer_group_names: papaya::HashMap<u64, u64>, // name_hash → group_id

    // -- Sessions (unified, replaces consumers + producers) --
    pub(crate) sessions: papaya::HashMap<u64, Arc<SessionState>>,

    // -- Name hash (CRC64-NVME) → ID lookup indexes --
    pub(crate) topic_names: papaya::HashMap<u64, u64>,
    pub(crate) exchange_names: papaya::HashMap<u64, u64>,

    // -- Consumer group reverse indexes for O(1) disconnect --
    /// session_id → set of group_ids where the session has in-flight messages or assignments.
    pub(crate) session_group_index: papaya::HashMap<u64, HashSet<u64>>,

    // -- Push-based group delivery --
    /// Notifies watchers when messages are available in a consumer group.
    pub group_notifier: GroupNotifier,

    // -- Topic aliases (MQTT 5.0) --
    /// session_id → list of topic alias entries.
    pub(crate) topic_aliases: papaya::HashMap<u64, Vec<TopicAliasEntry>>,

    // -- Pending will messages (MQTT 5.0 Will Delay Interval) --
    /// client_id name_hash → pending will with scheduled fire time.
    pub(crate) pending_wills: papaya::HashMap<u64, PendingWill>,

    // -- Publisher session dedup (MQTT 5.0 exactly-once) --
    /// session_id name_hash → set of received publisher_ids for dedup.
    pub(crate) publisher_dedup: papaya::HashMap<u64, HashSet<u64>>,

    // -- QoS 2 inbound dedup (MQTT 5.0 exactly-once inbound) --
    /// session_id → (packet_id → qos2 state: 0=RECEIVED, 1=COMPLETE).
    pub(crate) qos2_inbound: papaya::HashMap<u64, HashMap<u16, u8>>,

    // -- O(1) reverse indexes --
    /// binding_id → exchange_id for O(1) delete-binding lookup.
    pub(crate) binding_index: papaya::HashMap<u64, u64>,
    /// client_id name_hash → session_id for O(1) session restore.
    pub(crate) session_client_index: papaya::HashMap<u64, u64>,

    // -- Atomic scalars --
    pub(crate) next_id: AtomicU64,
    pub(crate) cached_purge_floor: AtomicU64,
    pub(crate) purge_floor_dirty: AtomicBool,

    /// Server start time (epoch ms) — used for compact delay index offsets.
    pub(crate) server_start_ms: u64,
}

impl MqMetadata {
    pub fn new(catalog_name: String) -> Self {
        Self {
            catalog_name,
            topics_by_id: papaya::HashMap::new(),
            topics_by_name: papaya::HashMap::new(),
            topics: papaya::HashMap::new(),
            exchanges: papaya::HashMap::new(),
            consumer_groups: papaya::HashMap::new(),
            consumer_group_names: papaya::HashMap::new(),
            sessions: papaya::HashMap::new(),
            topic_names: papaya::HashMap::new(),
            exchange_names: papaya::HashMap::new(),
            session_group_index: papaya::HashMap::new(),
            group_notifier: GroupNotifier::new(),
            topic_aliases: papaya::HashMap::new(),
            pending_wills: papaya::HashMap::new(),
            publisher_dedup: papaya::HashMap::new(),
            qos2_inbound: papaya::HashMap::new(),
            binding_index: papaya::HashMap::new(),
            session_client_index: papaya::HashMap::new(),
            next_id: AtomicU64::new(1),
            cached_purge_floor: AtomicU64::new(0),
            purge_floor_dirty: AtomicBool::new(true),
            server_start_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    /// Server start time in epoch milliseconds.
    #[inline]
    pub fn server_start_ms(&self) -> u64 {
        self.server_start_ms
    }

    /// The catalog name for metrics labeling.
    #[inline]
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// Allocate a monotonically increasing entity ID.
    /// Safe with single-writer (Raft apply) — Relaxed ordering suffices.
    #[inline]
    pub(crate) fn alloc_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Resolve an entity by type and name hash. Returns the entity ID if found.
    pub fn resolve_entity(&self, entity_type: u8, name_hash: u64) -> Option<u64> {
        match entity_type {
            0 => self.topic_names.pin().get(&name_hash).copied(),
            1 => self.exchange_names.pin().get(&name_hash).copied(),
            2 => self.consumer_group_names.pin().get(&name_hash).copied(),
            _ => None,
        }
    }

    /// Get the current head index for a topic.
    pub fn get_topic_head_from_state(&self, topic_id: u64) -> u64 {
        self.topics
            .pin()
            .get(&topic_id)
            .map(|t| t.head_index())
            .unwrap_or(0)
    }

    /// Get the partition map for a topic.
    pub fn get_topic_partitions(&self, topic_id: u64) -> Option<Vec<crate::types::PartitionInfo>> {
        self.topics
            .pin()
            .get(&topic_id)
            .map(|t| t.meta.partitions.clone())
    }

    // -- Topic operations --

    /// Insert or replace a topic. Returns the previous value if any.
    pub fn insert_topic(&self, meta: Arc<TopicMeta>) -> Option<Arc<TopicMeta>> {
        let guard = self.topics_by_id.pin();
        self.topics_by_name
            .pin()
            .insert(meta.name.clone(), meta.topic_id);
        guard.insert(meta.topic_id, meta).cloned()
    }

    /// Remove a topic by ID. Returns the removed value if any.
    pub fn remove_topic(&self, topic_id: u64) -> Option<Arc<TopicMeta>> {
        let guard = self.topics_by_id.pin();
        if let Some(meta) = guard.get(&topic_id).cloned() {
            guard.remove(&topic_id);
            self.topics_by_name.pin().remove(&meta.name);
            Some(meta)
        } else {
            None
        }
    }

    /// Get topic metadata by ID. Cache the returned `Arc` for zero-cost
    /// subsequent reads via atomics.
    #[inline]
    pub fn get_topic(&self, topic_id: u64) -> Option<Arc<TopicMeta>> {
        self.topics_by_id.pin().get(&topic_id).cloned()
    }

    /// Find a topic by name. Returns `Arc<TopicMeta>` for caching.
    pub fn find_topic_by_name(&self, name: &str) -> Option<Arc<TopicMeta>> {
        let topic_id = *self.topics_by_name.pin().get(name)?;
        self.get_topic(topic_id)
    }

    /// Get the current head offset for a topic.
    pub fn get_topic_head(&self, topic_id: u64) -> Option<u64> {
        let guard = self.topics_by_id.pin();
        let t = guard.get(&topic_id)?;
        let count = t.message_count();
        if count == 0 {
            return None;
        }
        Some(t.head_index())
    }

    /// Get the current tail offset for a topic.
    pub fn get_topic_tail(&self, topic_id: u64) -> Option<u64> {
        let guard = self.topics_by_id.pin();
        let t = guard.get(&topic_id)?;
        if t.message_count() == 0 {
            return None;
        }
        Some(t.tail_index())
    }

    /// Get the raft log location of the latest message for a topic.
    pub fn get_topic_latest(&self, topic_id: u64) -> Option<(u64, usize)> {
        let guard = self.topics_by_id.pin();
        let t = guard.get(&topic_id)?;
        t.latest()
    }

    /// List all topics as `(name_bytes, topic_id)` pairs.
    pub fn list_topics_into(&self, out: &mut Vec<(Bytes, u64)>) {
        out.clear();
        let guard = self.topics_by_id.pin();
        out.reserve(guard.len());
        for (_, t) in guard.iter() {
            out.push((Bytes::copy_from_slice(t.name.as_bytes()), t.topic_id));
        }
    }

    /// List topics matching a name prefix.
    pub fn list_topics_with_prefix_into(&self, prefix: &str, out: &mut Vec<(Bytes, u64)>) {
        out.clear();
        let guard = self.topics_by_id.pin();
        for (_, t) in guard.iter() {
            if t.name.starts_with(prefix) {
                out.push((Bytes::copy_from_slice(t.name.as_bytes()), t.topic_id));
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
        self.topics_by_id.pin().len()
    }

    /// Collect all topic IDs (for sync/removal checks).
    pub fn topic_ids(&self) -> Vec<u64> {
        self.topics_by_id.pin().iter().map(|(&k, _)| k).collect()
    }

    // -- Session / group reverse index operations --

    /// Track that a session has in-flight messages or assignments in a group.
    #[inline]
    pub(crate) fn track_session_group(&self, session_id: u64, group_id: u64) {
        let guard = self.session_group_index.pin();
        let mut set = guard.get(&session_id).cloned().unwrap_or_default();
        set.insert(group_id);
        guard.insert(session_id, set);
    }

    /// Remove a session from the reverse index. Returns the tracked group set.
    pub(crate) fn remove_session_group_index(&self, session_id: u64) -> Option<HashSet<u64>> {
        let guard = self.session_group_index.pin();
        let value = guard.get(&session_id).cloned();
        if value.is_some() {
            guard.remove(&session_id);
        }
        value
    }

    /// Clear all session reverse indexes (used during snapshot restore).
    pub(crate) fn clear_session_indexes(&self) {
        self.session_group_index.pin().clear();
    }

    // -- Consumer group operations (public API for Kafka adapter) --

    /// Resolve a consumer group name hash to its group ID.
    pub fn resolve_consumer_group(&self, name_hash: u64) -> Option<u64> {
        self.consumer_group_names.pin().get(&name_hash).copied()
    }

    /// Get a consumer group state by ID.
    pub fn get_consumer_group(&self, group_id: u64) -> Option<Arc<ConsumerGroupState>> {
        self.consumer_groups.pin().get(&group_id).cloned()
    }

    /// Iterate over all consumer groups, collecting `(group_id, Arc<state>)` pairs.
    pub fn iter_consumer_groups(&self) -> Vec<(u64, Arc<ConsumerGroupState>)> {
        self.consumer_groups
            .pin()
            .iter()
            .map(|(&k, v)| (k, Arc::clone(v)))
            .collect()
    }

    /// Find the group ID that a member belongs to (scans all groups).
    pub fn find_member_group(&self, member_id: &str) -> Option<u64> {
        let guard = self.consumer_groups.pin();
        for (&group_id, state) in guard.iter() {
            if state.has_member(member_id) {
                return Some(group_id);
            }
        }
        None
    }
}

impl Default for MqMetadata {
    fn default() -> Self {
        Self::new("default".to_string())
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
            0,
            log_idx,
            msg_pos,
        ))
    }

    #[test]
    fn empty_metadata() {
        let meta = MqMetadata::default();
        assert!(meta.list_topics_with_prefix("").is_empty());
        assert!(meta.get_topic_head(1).is_none());
        assert!(meta.get_topic_tail(1).is_none());
        assert!(meta.get_topic_latest(1).is_none());
        assert_eq!(meta.topic_count(), 0);
    }

    #[test]
    fn insert_and_query_topics() {
        let meta = MqMetadata::default();
        meta.insert_topic(make_topic_with_state(1, "events", 100, 50, 50, 999, 2));
        meta.insert_topic(make_topic_with_state(2, "events.clicks", 10, 0, 10, 500, 0));

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
    }

    #[test]
    fn cached_arc_reads_atomics_directly() {
        let meta = MqMetadata::default();
        meta.insert_topic(make_topic_with_state(1, "t", 10, 0, 10, 100, 0));

        let cached = meta.get_topic(1).unwrap();
        assert_eq!(cached.head_index(), 10);
        assert_eq!(cached.message_count(), 10);

        cached.set_head_index(20);
        cached.set_message_count(20);
        cached.set_latest(200, 3);

        assert_eq!(cached.head_index(), 20);
        assert_eq!(cached.message_count(), 20);
        assert_eq!(cached.latest(), Some((200, 3)));

        let fresh = meta.get_topic(1).unwrap();
        assert_eq!(fresh.head_index(), 20);
    }

    #[test]
    fn remove_topic() {
        let meta = MqMetadata::default();
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
    fn empty_topic_returns_none_for_head_tail() {
        let meta = MqMetadata::default();
        meta.insert_topic(make_topic(1, "empty-topic"));

        assert!(meta.get_topic_head(1).is_none());
        assert!(meta.get_topic_tail(1).is_none());
        assert!(meta.get_topic_latest(1).is_none());
        assert!(meta.find_topic_by_name("empty-topic").is_some());
    }

    #[test]
    fn list_topics_into() {
        let meta = MqMetadata::default();
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
        let meta = MqMetadata::default();
        meta.insert_topic(make_topic(1, "t"));

        let t = meta.get_topic(1).unwrap();
        t.update(100, 50, 50, 0, 999, 7);

        assert_eq!(t.head_index(), 100);
        assert_eq!(t.tail_index(), 50);
        assert_eq!(t.message_count(), 50);
        assert_eq!(t.latest(), Some((999, 7)));
    }

    #[test]
    fn concurrent_readers_see_atomic_updates() {
        let meta = Arc::new(MqMetadata::default());
        meta.insert_topic(make_topic_with_state(1, "v1", 10, 0, 10, 100, 0));

        let r1 = meta.get_topic(1).unwrap();
        let r2 = meta.get_topic(1).unwrap();

        assert_eq!(r1.head_index(), 10);
        assert_eq!(r2.head_index(), 10);

        r1.set_head_index(20);
        assert_eq!(r2.head_index(), 20);
    }

    #[test]
    fn find_by_name_o1() {
        let meta = MqMetadata::default();
        meta.insert_topic(make_topic(1, "foo"));
        meta.insert_topic(make_topic(2, "bar"));

        assert_eq!(meta.find_topic_by_name("foo").unwrap().topic_id, 1);
        assert_eq!(meta.find_topic_by_name("bar").unwrap().topic_id, 2);
        assert!(meta.find_topic_by_name("baz").is_none());
    }

    #[test]
    fn insert_replaces_existing() {
        let meta = MqMetadata::default();
        meta.insert_topic(make_topic_with_state(1, "t", 10, 0, 10, 50, 0));

        let new = make_topic_with_state(1, "t", 20, 0, 20, 100, 1);
        let old = meta.insert_topic(new).unwrap();
        assert_eq!(old.head_index(), 10);

        let current = meta.get_topic(1).unwrap();
        assert_eq!(current.head_index(), 20);
    }

    #[test]
    fn debug_impl() {
        let t = TopicMeta::with_state(1, "test".into(), 100, 50, 50, 0, 999, 2);
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

        t.set_latest(0, 0);
        assert_eq!(t.latest(), None);
    }

    #[test]
    fn session_group_tracking() {
        let meta = MqMetadata::default();
        meta.track_session_group(1, 10);
        meta.track_session_group(1, 20);
        meta.track_session_group(2, 10);

        let groups = meta.remove_session_group_index(1).unwrap();
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&10));
        assert!(groups.contains(&20));

        assert!(meta.remove_session_group_index(1).is_none());

        let groups = meta.remove_session_group_index(2).unwrap();
        assert_eq!(groups.len(), 1);
        assert!(groups.contains(&10));
    }
}

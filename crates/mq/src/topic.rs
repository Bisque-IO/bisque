use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::flat::FlatMessageMeta;
use crate::types::{
    PartitionInfo, RetentionPolicy, TopicCronConfig, TopicDedupConfig, TopicLifetimePolicy,
    name_hash,
};

/// Compute the next cron trigger time after the given timestamp (ms since epoch).
pub fn compute_next_trigger(cron_expr: &str, after: u64) -> u64 {
    use chrono::{TimeZone, Utc};
    use std::str::FromStr;

    let schedule = match cron::Schedule::from_str(cron_expr) {
        Ok(s) => s,
        Err(_) => return after + 60_000, // fallback: 1 minute
    };

    let dt = Utc
        .timestamp_millis_opt(after as i64)
        .single()
        .unwrap_or_else(Utc::now);

    schedule
        .after(&dt)
        .next()
        .map(|next| next.timestamp_millis() as u64)
        .unwrap_or(after + 60_000)
}

/// Sentinel: cached min needs full recompute.
const MIN_DIRTY: u64 = u64::MAX;
/// Sentinel: computed result is u64::MAX (no consumers).
const MIN_NONE: u64 = u64::MAX - 1;

// =============================================================================
// Topic Metadata (persisted to MDBX)
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMeta {
    pub topic_id: u64,
    pub name: String,
    pub created_at: u64,
    pub retention: RetentionPolicy,
    pub head_index: u64,
    pub tail_index: u64,
    pub message_count: u64,
    #[serde(default)]
    pub name_hash: u64,
    /// Number of partitions. 0 or 1 means unpartitioned (data in coordinator group).
    /// >1 means partitioned (data spread across partition raft groups).
    #[serde(default)]
    pub partition_count: u32,
    /// Partition map. Empty if unpartitioned.
    #[serde(default)]
    pub partitions: Vec<PartitionInfo>,
    /// Total byte size of all messages in the topic (value bytes only).
    #[serde(default)]
    pub total_bytes: u64,
    /// Raft log index of the most recent publish.
    #[serde(default)]
    pub latest_log_index: u64,
    /// Message position within the batch at `latest_log_index`.
    #[serde(default)]
    pub latest_msg_pos: usize,

    // -- Unified storage features --
    /// Topic lifetime policy.
    #[serde(default)]
    pub lifetime: TopicLifetimePolicy,
    /// Whether this topic stores a retained message (last published message).
    #[serde(default)]
    pub retained: bool,
    /// Dedup configuration (if enabled).
    #[serde(default)]
    pub dedup_config: Option<TopicDedupConfig>,
    /// Cron auto-publish configuration (replaces Job entity).
    #[serde(default)]
    pub cron_config: Option<TopicCronConfig>,
    /// Whether cron is currently enabled.
    #[serde(default)]
    pub cron_enabled: bool,
    /// Next cron trigger time (ms since epoch).
    #[serde(default)]
    pub next_trigger_at: u64,
}

impl TopicMeta {
    pub fn new(topic_id: u64, name: String, created_at: u64, retention: RetentionPolicy) -> Self {
        let hash = name_hash(&name);
        Self {
            topic_id,
            name,
            created_at,
            retention,
            head_index: 0,
            tail_index: 0,
            message_count: 0,
            name_hash: hash,
            partition_count: 0,
            partitions: Vec::new(),
            total_bytes: 0,
            latest_log_index: 0,
            latest_msg_pos: 0,
            lifetime: TopicLifetimePolicy::Permanent,
            retained: false,
            dedup_config: None,
            cron_config: None,
            cron_enabled: false,
            next_trigger_at: 0,
        }
    }

    /// Create a new partitioned topic metadata.
    pub fn new_partitioned(
        topic_id: u64,
        name: String,
        created_at: u64,
        retention: RetentionPolicy,
        partition_count: u32,
        partitions: Vec<PartitionInfo>,
    ) -> Self {
        let hash = name_hash(&name);
        Self {
            topic_id,
            name,
            created_at,
            retention,
            head_index: 0,
            tail_index: 0,
            message_count: 0,
            name_hash: hash,
            partition_count,
            partitions,
            total_bytes: 0,
            latest_log_index: 0,
            latest_msg_pos: 0,
            lifetime: TopicLifetimePolicy::Permanent,
            retained: false,
            dedup_config: None,
            cron_config: None,
            cron_enabled: false,
            next_trigger_at: 0,
        }
    }

    /// Returns true if this topic is partitioned.
    #[inline]
    pub fn is_partitioned(&self) -> bool {
        self.partition_count > 1
    }

    /// Ensure name_hash is populated (for deserialized data that may have default 0).
    pub fn ensure_name_hash(&mut self) {
        if self.name_hash == 0 {
            self.name_hash = name_hash(&self.name);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConsumerOffset {
    pub topic_id: u64,
    pub consumer_id: u64,
    pub committed_offset: u64,
    #[serde(default)]
    pub pending_offset: u64,
}

// =============================================================================
// Dedup Window
// =============================================================================

/// Sliding-window dedup tracker. Keys are stored in per-timestamp buckets.
pub struct DedupWindow {
    /// timestamp_sec → set of dedup keys seen in that second.
    buckets: Mutex<BTreeMap<u64, Vec<Bytes>>>,
}

impl DedupWindow {
    pub fn new() -> Self {
        Self {
            buckets: Mutex::new(BTreeMap::new()),
        }
    }

    /// Returns true if this key is a duplicate (already seen within the window).
    pub fn check_and_insert(&self, key: &Bytes, timestamp_sec: u64) -> bool {
        let mut buckets = self.buckets.lock();
        let bucket = buckets.entry(timestamp_sec).or_default();
        if bucket.iter().any(|k| k == key) {
            return true; // duplicate
        }
        bucket.push(key.clone());
        false // new key
    }

    /// Prune all entries with timestamp before `before_sec`.
    pub fn prune(&self, before_sec: u64) {
        let mut buckets = self.buckets.lock();
        // Split off everything before before_sec
        let remaining = buckets.split_off(&before_sec);
        *buckets = remaining;
    }

    /// Snapshot for serialization.
    pub fn snapshot(&self) -> Vec<(u64, Vec<Bytes>)> {
        let buckets = self.buckets.lock();
        buckets
            .iter()
            .map(|(&ts, keys)| (ts, keys.clone()))
            .collect()
    }

    /// Restore from snapshot.
    pub fn restore(&self, entries: Vec<(u64, Vec<Bytes>)>) {
        let mut buckets = self.buckets.lock();
        for (ts, keys) in entries {
            buckets.insert(ts, keys);
        }
    }
}

// =============================================================================
// In-memory Topic State
// =============================================================================

/// Lock-free topic state.
///
/// Immutable identity fields are stored directly. Mutable counters use atomics
/// for safe concurrent reads (leader tasks, protocol adapters) with single-writer
/// updates (Raft apply path).
pub struct TopicState {
    // -- Immutable identity (set once at creation, never changed) --
    pub meta: TopicMeta,

    // -- Mutable counters (atomics for concurrent readers + single writer) --
    head_index: AtomicU64,
    tail_index: AtomicU64,
    message_count: AtomicU64,
    total_bytes: AtomicU64,
    latest_log_index: AtomicU64,
    latest_msg_pos: AtomicUsize,

    pub(crate) consumer_offsets: DashMap<u64, TopicConsumerOffset>,
    /// Cached minimum consumer offset.
    cached_min_consumer_offset: AtomicU64,

    /// IDs of consumer groups attached to this topic.
    pub(crate) consumer_group_ids: DashMap<u64, ()>,

    /// Retained message (last published message bytes). Only used when meta.retained == true.
    pub(crate) retained_message: Mutex<Option<Bytes>>,

    /// Dedup window (only allocated when dedup_config is Some).
    pub(crate) dedup: Option<DedupWindow>,

    /// Cron state atomics.
    pub(crate) cron_enabled: AtomicBool,
    pub(crate) next_trigger_at: AtomicU64,

    // Pre-initialized metrics handles
    m_publish_count: metrics::Counter,
    m_publish_bytes: metrics::Counter,
}

impl TopicState {
    pub fn new(meta: TopicMeta, catalog_name: &str) -> Self {
        let labels = [
            ("catalog", catalog_name.to_owned()),
            ("topic", meta.name.clone()),
        ];
        let m_publish_count = metrics::counter!("mq.topic.publish.count", &labels);
        let m_publish_bytes = metrics::counter!("mq.topic.publish.bytes", &labels);

        let head_index = AtomicU64::new(meta.head_index);
        let tail_index = AtomicU64::new(meta.tail_index);
        let message_count = AtomicU64::new(meta.message_count);
        let total_bytes = AtomicU64::new(meta.total_bytes);
        let latest_log_index = AtomicU64::new(meta.latest_log_index);
        let latest_msg_pos = AtomicUsize::new(meta.latest_msg_pos);
        let cron_enabled = AtomicBool::new(meta.cron_enabled);
        let next_trigger_at = AtomicU64::new(meta.next_trigger_at);
        let dedup = meta.dedup_config.as_ref().map(|_| DedupWindow::new());

        Self {
            meta,
            head_index,
            tail_index,
            message_count,
            total_bytes,
            latest_log_index,
            latest_msg_pos,
            consumer_offsets: DashMap::new(),
            cached_min_consumer_offset: AtomicU64::new(MIN_NONE),
            consumer_group_ids: DashMap::new(),
            retained_message: Mutex::new(None),
            dedup,
            cron_enabled,
            next_trigger_at,
            m_publish_count,
            m_publish_bytes,
        }
    }

    // -- Atomic accessors --

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

    #[inline]
    pub fn latest_log_index(&self) -> u64 {
        self.latest_log_index.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn latest_msg_pos(&self) -> usize {
        self.latest_msg_pos.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn cron_enabled(&self) -> bool {
        self.cron_enabled.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn next_trigger_at(&self) -> u64 {
        self.next_trigger_at.load(Ordering::Relaxed)
    }

    /// Build a TopicMeta snapshot by flushing atomics back to a cloned meta.
    pub fn snapshot_meta(&self) -> TopicMeta {
        let mut m = self.meta.clone();
        m.head_index = self.head_index();
        m.tail_index = self.tail_index();
        m.message_count = self.message_count();
        m.total_bytes = self.total_bytes();
        m.latest_log_index = self.latest_log_index();
        m.latest_msg_pos = self.latest_msg_pos();
        m.cron_enabled = self.cron_enabled();
        m.next_trigger_at = self.next_trigger_at();
        m
    }

    /// Snapshot consumer offsets as a Vec (for serialization).
    pub fn consumer_offsets_snapshot(&self) -> Vec<TopicConsumerOffset> {
        self.consumer_offsets
            .iter()
            .map(|e| e.value().clone())
            .collect()
    }

    /// Apply a batch of pre-encoded flat messages to this topic.
    ///
    /// Assigns dense per-topic offsets (0, 1, 2, …).
    /// Returns `base_offset` — the first dense topic offset assigned.
    pub fn apply_publish(
        &self,
        log_index: u64,
        messages: impl ExactSizeIterator<Item = Bytes>,
    ) -> u64 {
        let base_offset = self.head_index();
        let count = messages.len() as u64;
        let mut total_bytes: u64 = 0;
        let mut last_pos: usize = 0;
        let mut last_msg: Option<Bytes> = None;
        for (i, m) in messages.enumerate() {
            total_bytes += FlatMessageMeta::value_len(&m).unwrap_or(0) as u64;
            last_pos = i;
            if self.meta.retained {
                last_msg = Some(m);
            }
        }

        // Update retained message if enabled
        if let Some(msg) = last_msg {
            *self.retained_message.lock() = Some(msg);
        }

        if self.message_count() == 0 {
            self.tail_index.store(base_offset, Ordering::Relaxed);
        }
        self.message_count.fetch_add(count, Ordering::Relaxed);
        self.total_bytes.fetch_add(total_bytes, Ordering::Relaxed);
        self.head_index
            .store(base_offset + count, Ordering::Relaxed);
        self.latest_log_index.store(log_index, Ordering::Relaxed);
        self.latest_msg_pos.store(last_pos, Ordering::Relaxed);

        self.m_publish_count.increment(count);
        self.m_publish_bytes.increment(total_bytes);

        base_offset
    }

    /// Check dedup before publishing. Returns true if the key is a duplicate.
    pub fn check_dedup(&self, key: &Bytes, timestamp_sec: u64) -> bool {
        if let Some(ref dedup) = self.dedup {
            dedup.check_and_insert(key, timestamp_sec)
        } else {
            false
        }
    }

    /// Prune dedup window entries older than `before_sec`.
    pub fn prune_dedup(&self, before_sec: u64) {
        if let Some(ref dedup) = self.dedup {
            dedup.prune(before_sec);
        }
    }

    pub fn apply_commit_offset(&self, consumer_id: u64, offset: u64) {
        let is_new_consumer = !self.consumer_offsets.contains_key(&consumer_id);
        let mut entry =
            self.consumer_offsets
                .entry(consumer_id)
                .or_insert_with(|| TopicConsumerOffset {
                    topic_id: self.meta.topic_id,
                    consumer_id,
                    committed_offset: 0,
                    pending_offset: 0,
                });
        if offset > entry.committed_offset {
            entry.committed_offset = offset;
            self.cached_min_consumer_offset
                .store(MIN_DIRTY, Ordering::Relaxed);
        } else if is_new_consumer {
            self.cached_min_consumer_offset
                .store(MIN_DIRTY, Ordering::Relaxed);
        }
    }

    pub fn apply_purge(&self, before_index: u64) {
        let current_tail = self.tail_index();
        if before_index > current_tail {
            self.tail_index.store(before_index, Ordering::Relaxed);
            let current_count = self.message_count();
            if current_count > 0 {
                let purged = before_index.saturating_sub(current_tail);
                self.message_count
                    .store(current_count.saturating_sub(purged), Ordering::Relaxed);
            }
        }
    }

    /// Attach a consumer group to this topic.
    pub fn attach_group(&self, group_id: u64) {
        self.consumer_group_ids.insert(group_id, ());
    }

    /// Detach a consumer group from this topic.
    pub fn detach_group(&self, group_id: u64) {
        self.consumer_group_ids.remove(&group_id);
    }

    /// Returns true if this topic should be auto-deleted (DeleteOnLastDetach + no groups).
    pub fn should_auto_delete(&self) -> bool {
        self.meta.lifetime == TopicLifetimePolicy::DeleteOnLastDetach
            && self.consumer_group_ids.is_empty()
    }

    /// Check if cron should trigger now.
    pub fn should_cron_trigger(&self, current_time: u64) -> bool {
        self.cron_enabled()
            && self.meta.cron_config.is_some()
            && current_time >= self.next_trigger_at()
    }

    /// Apply cron trigger: update next trigger time.
    pub fn apply_cron_trigger(&self, triggered_at: u64) {
        if let Some(ref cron) = self.meta.cron_config {
            let next = compute_next_trigger(&cron.cron_expression, triggered_at);
            self.next_trigger_at.store(next, Ordering::Relaxed);
        }
    }

    /// Returns the minimum log index required by this topic (for purge floor).
    pub fn min_required_index(&self) -> u64 {
        let cached = self.cached_min_consumer_offset.load(Ordering::Relaxed);
        let consumer_min = if cached == MIN_NONE {
            u64::MAX
        } else if cached != MIN_DIRTY {
            cached
        } else {
            // Recompute
            let min = self
                .consumer_offsets
                .iter()
                .map(|e| e.committed_offset)
                .min()
                .unwrap_or(u64::MAX);
            if min == u64::MAX {
                self.cached_min_consumer_offset
                    .store(MIN_NONE, Ordering::Relaxed);
            } else {
                self.cached_min_consumer_offset
                    .store(min, Ordering::Relaxed);
            }
            min
        };
        self.tail_index().min(consumer_min)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flat::FlatMessageBuilder;

    fn make_topic(name: &str) -> TopicState {
        let meta = TopicMeta::new(1, name.to_string(), 1000, RetentionPolicy::default());
        TopicState::new(meta, "test")
    }

    fn make_msg(value: &[u8]) -> Bytes {
        FlatMessageBuilder::new(Bytes::from(value.to_vec()))
            .timestamp(1000)
            .build()
    }

    #[test]
    fn test_topic_meta_defaults() {
        let meta = TopicMeta::new(42, "events".to_string(), 5000, RetentionPolicy::default());
        assert_eq!(meta.topic_id, 42);
        assert_eq!(meta.name, "events");
        assert_eq!(meta.head_index, 0);
        assert_eq!(meta.tail_index, 0);
        assert_eq!(meta.message_count, 0);
        assert_eq!(meta.lifetime, TopicLifetimePolicy::Permanent);
        assert!(!meta.retained);
        assert!(meta.dedup_config.is_none());
        assert!(meta.cron_config.is_none());
    }

    #[test]
    fn test_publish_single_message() {
        let topic = make_topic("test");
        let msgs = vec![make_msg(b"hello")];
        let base = topic.apply_publish(10, msgs.into_iter());

        assert_eq!(base, 0);
        assert_eq!(topic.head_index(), 1);
        assert_eq!(topic.tail_index(), 0);
        assert_eq!(topic.message_count(), 1);
        assert_eq!(topic.latest_log_index(), 10);
        assert_eq!(topic.latest_msg_pos(), 0);
    }

    #[test]
    fn test_publish_batch() {
        let topic = make_topic("test");
        let msgs = vec![make_msg(b"a"), make_msg(b"b"), make_msg(b"c")];
        let base = topic.apply_publish(100, msgs.into_iter());

        assert_eq!(base, 0);
        assert_eq!(topic.message_count(), 3);
        assert_eq!(topic.head_index(), 3);
    }

    #[test]
    fn test_retained_message() {
        let mut meta = TopicMeta::new(
            1,
            "retained-test".to_string(),
            1000,
            RetentionPolicy::default(),
        );
        meta.retained = true;
        let topic = TopicState::new(meta, "test");

        let msg = make_msg(b"last-value");
        topic.apply_publish(10, vec![msg.clone()].into_iter());

        let retained = topic.retained_message.lock();
        assert!(retained.is_some());
    }

    #[test]
    fn test_dedup_window() {
        let mut meta = TopicMeta::new(
            1,
            "dedup-test".to_string(),
            1000,
            RetentionPolicy::default(),
        );
        meta.dedup_config = Some(TopicDedupConfig { window_secs: 60 });
        let topic = TopicState::new(meta, "test");

        let key = Bytes::from_static(b"key1");
        assert!(!topic.check_dedup(&key, 100)); // first time - not duplicate
        assert!(topic.check_dedup(&key, 100)); // second time - duplicate

        let key2 = Bytes::from_static(b"key2");
        assert!(!topic.check_dedup(&key2, 100)); // different key - not duplicate
    }

    #[test]
    fn test_dedup_prune() {
        let mut meta = TopicMeta::new(
            1,
            "dedup-prune".to_string(),
            1000,
            RetentionPolicy::default(),
        );
        meta.dedup_config = Some(TopicDedupConfig { window_secs: 60 });
        let topic = TopicState::new(meta, "test");

        let key = Bytes::from_static(b"key1");
        topic.check_dedup(&key, 100);
        topic.prune_dedup(101); // prune entries before 101
        assert!(!topic.check_dedup(&key, 200)); // key is no longer in window
    }

    #[test]
    fn test_consumer_group_attach_detach() {
        let topic = make_topic("test");
        topic.attach_group(1);
        topic.attach_group(2);
        assert_eq!(topic.consumer_group_ids.len(), 2);

        topic.detach_group(1);
        assert_eq!(topic.consumer_group_ids.len(), 1);
        assert!(!topic.should_auto_delete()); // Permanent policy
    }

    #[test]
    fn test_auto_delete_policy() {
        let mut meta = TopicMeta::new(1, "temp".to_string(), 1000, RetentionPolicy::default());
        meta.lifetime = TopicLifetimePolicy::DeleteOnLastDetach;
        let topic = TopicState::new(meta, "test");

        topic.attach_group(1);
        assert!(!topic.should_auto_delete());

        topic.detach_group(1);
        assert!(topic.should_auto_delete());
    }

    #[test]
    fn test_commit_offset() {
        let topic = make_topic("test");
        topic.apply_publish(10, std::iter::once(make_msg(b"msg")));

        topic.apply_commit_offset(100, 0);
        assert_eq!(
            topic.consumer_offsets.get(&100).unwrap().committed_offset,
            0
        );

        topic.apply_commit_offset(100, 15);
        assert_eq!(
            topic.consumer_offsets.get(&100).unwrap().committed_offset,
            15
        );

        // Cannot go backwards
        topic.apply_commit_offset(100, 5);
        assert_eq!(
            topic.consumer_offsets.get(&100).unwrap().committed_offset,
            15
        );
    }

    #[test]
    fn test_purge() {
        let topic = make_topic("test");
        topic.apply_publish(10, std::iter::once(make_msg(b"a")));
        topic.apply_publish(20, std::iter::once(make_msg(b"b")));
        topic.apply_purge(1);
        assert_eq!(topic.tail_index(), 1);
    }

    #[test]
    fn test_min_required_index() {
        let topic = make_topic("test");
        topic.apply_publish(10, std::iter::once(make_msg(b"a")));
        topic.apply_publish(20, std::iter::once(make_msg(b"b")));
        assert_eq!(topic.min_required_index(), 0);

        topic.apply_purge(1);
        topic.apply_commit_offset(1, 0);
        assert_eq!(topic.min_required_index(), 0);

        topic.apply_commit_offset(1, 1);
        assert_eq!(topic.min_required_index(), 1);
    }

    #[test]
    fn test_partitioned_topic_meta() {
        use crate::types::{PartitionInfo, PartitionStatus};

        let partitions = vec![
            PartitionInfo {
                partition_index: 0,
                group_id: 100,
                status: PartitionStatus::Active,
            },
            PartitionInfo {
                partition_index: 1,
                group_id: 101,
                status: PartitionStatus::Active,
            },
        ];

        let meta = TopicMeta::new_partitioned(
            1,
            "events".to_string(),
            1000,
            RetentionPolicy::default(),
            2,
            partitions,
        );

        assert!(meta.is_partitioned());
        assert_eq!(meta.partition_count, 2);
        assert_eq!(meta.partitions.len(), 2);
    }
}

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::flat::FlatMessageMeta;
use crate::types::{PartitionInfo, RetentionPolicy, name_hash};

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
    /// Partition map. Empty if unpartitioned. Each entry maps a partition index
    /// to the raft group that owns it.
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
        }
    }

    /// Returns true if this topic is partitioned (data spread across partition groups).
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
// In-memory Topic State
// =============================================================================

/// Lock-free topic state.
///
/// Immutable identity fields are stored directly. Mutable counters use atomics
/// for safe concurrent reads (leader tasks, protocol adapters) with single-writer
/// updates (Raft apply path). No `UnsafeCell` needed.
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
    /// `MIN_DIRTY` = needs recompute, `MIN_NONE` = no consumers, else = cached value.
    cached_min_consumer_offset: AtomicU64,

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

    /// Build a TopicMeta snapshot by flushing atomics back to a cloned meta.
    pub fn snapshot_meta(&self) -> TopicMeta {
        let mut m = self.meta.clone();
        m.head_index = self.head_index();
        m.tail_index = self.tail_index();
        m.message_count = self.message_count();
        m.total_bytes = self.total_bytes();
        m.latest_log_index = self.latest_log_index();
        m.latest_msg_pos = self.latest_msg_pos();
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
    /// Offsets are `base_offset..base_offset + count`.
    pub fn apply_publish(
        &self,
        log_index: u64,
        messages: impl ExactSizeIterator<Item = Bytes>,
    ) -> u64 {
        let base_offset = self.head_index();
        let count = messages.len() as u64;
        let mut total_bytes: u64 = 0;
        let mut last_pos: usize = 0;
        for (i, m) in messages.enumerate() {
            total_bytes += FlatMessageMeta::value_len(&m).unwrap_or(0) as u64;
            last_pos = i;
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
    }

    #[test]
    fn test_publish_single_message() {
        let topic = make_topic("test");
        let msgs = vec![make_msg(b"hello")];
        let base = topic.apply_publish(10, msgs.into_iter());

        // Dense offset starts at 0
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
    fn test_publish_multiple_batches() {
        let topic = make_topic("test");
        topic.apply_publish(10, std::iter::once(make_msg(b"first")));
        topic.apply_publish(20, std::iter::once(make_msg(b"second")));

        assert_eq!(topic.head_index(), 2);
        assert_eq!(topic.tail_index(), 0);
        assert_eq!(topic.message_count(), 2);
    }

    #[test]
    fn test_latest_log_tracking() {
        let topic = make_topic("test");
        topic.apply_publish(10, vec![make_msg(b"a"), make_msg(b"b")].into_iter());
        assert_eq!(topic.latest_log_index(), 10);
        assert_eq!(topic.latest_msg_pos(), 1); // last msg in batch

        topic.apply_publish(20, std::iter::once(make_msg(b"c")));
        assert_eq!(topic.latest_log_index(), 20);
        assert_eq!(topic.latest_msg_pos(), 0); // single msg
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

        // Advance offset
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
    fn test_commit_offset_multiple_consumers() {
        let topic = make_topic("test");
        topic.apply_commit_offset(1, 10);
        topic.apply_commit_offset(2, 20);

        assert_eq!(topic.consumer_offsets.len(), 2);
        assert_eq!(topic.consumer_offsets.get(&1).unwrap().committed_offset, 10);
        assert_eq!(topic.consumer_offsets.get(&2).unwrap().committed_offset, 20);
    }

    #[test]
    fn test_purge() {
        let topic = make_topic("test");
        topic.apply_publish(10, std::iter::once(make_msg(b"a")));
        topic.apply_publish(20, std::iter::once(make_msg(b"b")));
        // Purge offsets below 1 (second message)
        topic.apply_purge(1);
        assert_eq!(topic.tail_index(), 1);
    }

    #[test]
    fn test_purge_no_op_if_before_tail() {
        let topic = make_topic("test");
        topic.apply_publish(20, std::iter::once(make_msg(b"a")));
        // tail_index is 0 (first dense offset), purge at 0 is no-op (not > tail)
        topic.apply_purge(0);
        assert_eq!(topic.tail_index(), 0);
    }

    #[test]
    fn test_min_required_index() {
        let topic = make_topic("test");
        topic.apply_publish(10, std::iter::once(make_msg(b"a")));
        topic.apply_publish(20, std::iter::once(make_msg(b"b")));
        assert_eq!(topic.min_required_index(), 0); // tail is 0

        // Consumer behind topic tail
        topic.apply_purge(1);
        topic.apply_commit_offset(1, 0);
        assert_eq!(topic.min_required_index(), 0);

        // Consumer caught up past tail
        topic.apply_commit_offset(1, 1);
        assert_eq!(topic.min_required_index(), 1);
    }

    #[test]
    fn test_consumer_offset_defaults() {
        let topic = make_topic("test");
        topic.apply_commit_offset(42, 100);
        let offset = topic.consumer_offsets.get(&42).unwrap();
        assert_eq!(offset.topic_id, 1);
        assert_eq!(offset.consumer_id, 42);
        assert_eq!(offset.pending_offset, 0);
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
            PartitionInfo {
                partition_index: 2,
                group_id: 102,
                status: PartitionStatus::Active,
            },
        ];

        let meta = TopicMeta::new_partitioned(
            1,
            "events".to_string(),
            1000,
            RetentionPolicy::default(),
            3,
            partitions,
        );

        assert!(meta.is_partitioned());
        assert_eq!(meta.partition_count, 3);
        assert_eq!(meta.partitions.len(), 3);
        assert_eq!(meta.partitions[0].group_id, 100);
        assert_eq!(meta.partitions[2].partition_index, 2);
        assert_eq!(meta.name_hash, name_hash("events"));
    }

    #[test]
    fn test_unpartitioned_topic_is_not_partitioned() {
        let meta = TopicMeta::new(1, "simple".to_string(), 1000, RetentionPolicy::default());
        assert!(!meta.is_partitioned());
        assert_eq!(meta.partition_count, 0);
        assert!(meta.partitions.is_empty());
    }

    #[test]
    fn test_partitioned_topic_serde_roundtrip() {
        use crate::types::{PartitionInfo, PartitionStatus};

        let partitions = vec![
            PartitionInfo {
                partition_index: 0,
                group_id: 50,
                status: PartitionStatus::Active,
            },
            PartitionInfo {
                partition_index: 1,
                group_id: 51,
                status: PartitionStatus::Draining,
            },
        ];
        let meta = TopicMeta::new_partitioned(
            42,
            "test".to_string(),
            500,
            RetentionPolicy::default(),
            2,
            partitions,
        );

        let encoded = bincode::serde::encode_to_vec(&meta, bincode::config::standard()).unwrap();
        let (decoded, _): (TopicMeta, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        assert_eq!(decoded.topic_id, 42);
        assert_eq!(decoded.partition_count, 2);
        assert_eq!(decoded.partitions.len(), 2);
        assert_eq!(decoded.partitions[0].group_id, 50);
        assert_eq!(decoded.partitions[1].status, PartitionStatus::Draining);
    }
}

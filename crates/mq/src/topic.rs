use std::cell::UnsafeCell;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use smallvec::SmallVec;

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
// Compact per-batch log entry tracking
// =============================================================================

/// Tracks one `apply_publish` call: the raft log index and how many messages
/// were published. Together with the dense topic offset sequence, this gives
/// `topic_offset → log_index` mapping without per-message storage.
#[derive(Clone, Debug)]
pub struct TopicLogEntry {
    /// First dense topic offset in this batch.
    pub base_offset: u64,
    /// Raft log index of the publish command.
    pub log_index: u64,
    /// Number of messages published in this batch.
    pub msg_count: u32,
}

// =============================================================================
// In-memory Topic State
// =============================================================================

/// Lock-free topic state.
///
/// All inner collections use interior mutability so methods take `&self`.
/// The Raft apply path (single writer) and leader tasks (readers) can
/// operate without contention on the outer DashMap.
pub struct TopicState {
    meta: UnsafeCell<TopicMeta>,
    pub(crate) consumer_offsets: DashMap<u64, TopicConsumerOffset>,
    /// Cached minimum consumer offset.
    /// `MIN_DIRTY` = needs recompute, `MIN_NONE` = no consumers, else = cached value.
    cached_min_consumer_offset: AtomicU64,

    /// Compact log entry index: one entry per `apply_publish` call.
    ///
    /// Maps dense topic offsets to raft log indexes. To find the log_index
    /// for topic offset N, binary search `log_entries` by `base_offset`.
    log_entries: Mutex<Vec<TopicLogEntry>>,

    // Pre-initialized metrics handles
    m_publish_count: metrics::Counter,
    m_publish_bytes: metrics::Counter,
}

// SAFETY: Single-writer (Raft apply path) guarantees no concurrent mutation.
unsafe impl Send for TopicState {}
unsafe impl Sync for TopicState {}

impl TopicState {
    pub fn new(meta: TopicMeta) -> Self {
        let labels = [("topic", meta.name.clone())];
        let m_publish_count = metrics::counter!("mq.topic.publish.count", &labels);
        let m_publish_bytes = metrics::counter!("mq.topic.publish.bytes", &labels);

        Self {
            meta: UnsafeCell::new(meta),
            consumer_offsets: DashMap::new(),
            cached_min_consumer_offset: AtomicU64::new(MIN_NONE),
            log_entries: Mutex::new(Vec::new()),
            m_publish_count,
            m_publish_bytes,
        }
    }

    /// Read-only access to meta.
    #[inline]
    pub fn meta(&self) -> &TopicMeta {
        unsafe { &*self.meta.get() }
    }

    /// Mutable access to meta. SAFETY: only called from single-writer Raft apply path.
    #[inline]
    pub(crate) fn meta_mut(&self) -> &mut TopicMeta {
        unsafe { &mut *self.meta.get() }
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
    /// Assigns dense per-topic offsets (0, 1, 2, …) and records which raft
    /// log entry contains these messages.
    ///
    /// NOTE: `meta.head_index`, `meta.tail_index`, and `meta.message_count`
    /// are mutated here. Since only the single-writer Raft apply path calls
    /// this, and the outer DashMap only requires a read guard (`get()`),
    /// these plain field mutations are safe.
    pub fn apply_publish(&self, log_index: u64, messages: &[Bytes]) -> SmallVec<[u64; 16]> {
        let mut offsets = SmallVec::with_capacity(messages.len());
        let base_offset = self.meta().head_index;
        let total_bytes: u64 = messages
            .iter()
            .map(|m| FlatMessageMeta::value_len(m).unwrap_or(0) as u64)
            .sum();

        // Record which raft log entry contains these messages.
        self.log_entries.lock().unwrap().push(TopicLogEntry {
            base_offset,
            log_index,
            msg_count: messages.len() as u32,
        });

        // SAFETY: single-writer Raft apply path — these fields are only written here.
        let meta = self.meta_mut();
        if meta.message_count == 0 {
            meta.tail_index = base_offset;
        }
        meta.message_count += messages.len() as u64;
        meta.head_index = base_offset + messages.len() as u64;

        for i in 0..messages.len() {
            offsets.push(base_offset + i as u64);
        }

        self.m_publish_count.increment(messages.len() as u64);
        self.m_publish_bytes.increment(total_bytes);

        offsets
    }

    /// Look up the raft log index and intra-batch position for a dense topic offset.
    ///
    /// Returns `(log_index, msg_index_within_batch)`.
    #[inline]
    pub fn get_log_entry(&self, offset: u64) -> Option<(u64, usize)> {
        if offset < self.meta().tail_index || offset >= self.meta().head_index {
            return None;
        }
        let log_entries = self.log_entries.lock().unwrap();
        // Binary search log_entries by base_offset
        let idx = match log_entries.binary_search_by_key(&offset, |e| e.base_offset) {
            Ok(i) => i,
            Err(i) => {
                if i == 0 {
                    return None;
                }
                i - 1
            }
        };
        let entry = &log_entries[idx];
        let pos = (offset - entry.base_offset) as usize;
        if pos < entry.msg_count as usize {
            Some((entry.log_index, pos))
        } else {
            None
        }
    }

    pub fn apply_commit_offset(&self, consumer_id: u64, offset: u64) {
        let is_new_consumer = !self.consumer_offsets.contains_key(&consumer_id);
        let mut entry =
            self.consumer_offsets
                .entry(consumer_id)
                .or_insert_with(|| TopicConsumerOffset {
                    topic_id: self.meta().topic_id,
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
        if before_index > self.meta().tail_index {
            let old_tail = self.meta().tail_index;
            // SAFETY: single-writer Raft apply path
            let meta = self.meta_mut();
            meta.tail_index = before_index;
            if meta.message_count > 0 {
                let purged = before_index.saturating_sub(old_tail);
                meta.message_count = meta.message_count.saturating_sub(purged);
            }
            // Remove log entries that are fully before the new tail
            let mut log_entries = self.log_entries.lock().unwrap();
            let first_kept =
                log_entries.partition_point(|e| e.base_offset + e.msg_count as u64 <= before_index);
            if first_kept > 0 {
                log_entries.drain(..first_kept);
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
        self.meta().tail_index.min(consumer_min)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flat::FlatMessageBuilder;

    fn make_topic(name: &str) -> TopicState {
        let meta = TopicMeta::new(1, name.to_string(), 1000, RetentionPolicy::default());
        TopicState::new(meta)
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
        let offsets = topic.apply_publish(10, &msgs);

        // Dense offset starts at 0
        assert_eq!(offsets.as_slice(), &[0u64]);
        assert_eq!(topic.meta().head_index, 1);
        assert_eq!(topic.meta().tail_index, 0);
        assert_eq!(topic.meta().message_count, 1);
        // Log entry recorded
        let entries = topic.log_entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_index, 10);
        assert_eq!(entries[0].base_offset, 0);
        assert_eq!(entries[0].msg_count, 1);
    }

    #[test]
    fn test_publish_batch() {
        let topic = make_topic("test");
        let msgs = vec![make_msg(b"a"), make_msg(b"b"), make_msg(b"c")];
        let offsets = topic.apply_publish(100, &msgs);

        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets.as_slice(), &[0, 1, 2]);
        assert_eq!(topic.meta().message_count, 3);
        assert_eq!(topic.meta().head_index, 3);
        assert_eq!(topic.log_entries.lock().unwrap()[0].msg_count, 3);
    }

    #[test]
    fn test_publish_multiple_batches() {
        let topic = make_topic("test");
        topic.apply_publish(10, &[make_msg(b"first")]);
        topic.apply_publish(20, &[make_msg(b"second")]);

        assert_eq!(topic.meta().head_index, 2);
        assert_eq!(topic.meta().tail_index, 0);
        assert_eq!(topic.meta().message_count, 2);
        assert_eq!(topic.log_entries.lock().unwrap().len(), 2);
    }

    #[test]
    fn test_get_log_entry() {
        let topic = make_topic("test");
        topic.apply_publish(10, &[make_msg(b"a"), make_msg(b"b")]);
        topic.apply_publish(20, &[make_msg(b"c")]);

        // Offset 0 → log_index 10, position 0
        assert_eq!(topic.get_log_entry(0), Some((10, 0)));
        // Offset 1 → log_index 10, position 1
        assert_eq!(topic.get_log_entry(1), Some((10, 1)));
        // Offset 2 → log_index 20, position 0
        assert_eq!(topic.get_log_entry(2), Some((20, 0)));
        // Out of range
        assert_eq!(topic.get_log_entry(3), None);
    }

    #[test]
    fn test_commit_offset() {
        let topic = make_topic("test");
        topic.apply_publish(10, &[make_msg(b"msg")]);

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
        topic.apply_publish(10, &[make_msg(b"a")]);
        topic.apply_publish(20, &[make_msg(b"b")]);
        // Purge offsets below 1 (second message)
        topic.apply_purge(1);
        assert_eq!(topic.meta().tail_index, 1);
        // First log entry (base_offset=0, count=1) is fully purged
        let entries = topic.log_entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_index, 20);
    }

    #[test]
    fn test_purge_no_op_if_before_tail() {
        let topic = make_topic("test");
        topic.apply_publish(20, &[make_msg(b"a")]);
        // tail_index is 0 (first dense offset), purge at 0 is no-op (not > tail)
        topic.apply_purge(0);
        assert_eq!(topic.meta().tail_index, 0);
    }

    #[test]
    fn test_min_required_index() {
        let topic = make_topic("test");
        topic.apply_publish(10, &[make_msg(b"a")]);
        topic.apply_publish(20, &[make_msg(b"b")]);
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

    #[test]
    fn test_get_log_entry_after_purge() {
        let topic = make_topic("test");
        // Batch 1: offsets 0,1 at log_index 10
        topic.apply_publish(10, &[make_msg(b"a"), make_msg(b"b")]);
        // Batch 2: offsets 2,3,4 at log_index 20
        topic.apply_publish(20, &[make_msg(b"c"), make_msg(b"d"), make_msg(b"e")]);

        // Purge offset 0,1 (fully removes batch 1)
        topic.apply_purge(2);

        // Batch 1 entries removed
        assert_eq!(topic.log_entries.lock().unwrap().len(), 1);
        // Offset 2 still findable
        assert_eq!(topic.get_log_entry(2), Some((20, 0)));
        assert_eq!(topic.get_log_entry(4), Some((20, 2)));
        // Purged offsets return None
        assert_eq!(topic.get_log_entry(0), None);
        assert_eq!(topic.get_log_entry(1), None);
    }

    #[test]
    fn test_get_log_entry_partial_purge() {
        let topic = make_topic("test");
        // Batch with 3 messages at offsets 0,1,2
        topic.apply_publish(10, &[make_msg(b"a"), make_msg(b"b"), make_msg(b"c")]);

        // Purge offset 0 — batch still partially valid
        topic.apply_purge(1);

        // The batch is not fully purged (base_offset=0 + count=3 > before_index=1)
        assert_eq!(topic.log_entries.lock().unwrap().len(), 1);
        // Offset 1 still findable (even though offset 0 is purged, tail guards it)
        assert_eq!(topic.get_log_entry(1), Some((10, 1)));
        assert_eq!(topic.get_log_entry(2), Some((10, 2)));
        // Purged offset
        assert_eq!(topic.get_log_entry(0), None);
    }
}

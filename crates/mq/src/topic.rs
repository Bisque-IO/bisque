use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use smallvec::SmallVec;

use crate::types::{MessagePayload, RetentionPolicy, SegmentRange, name_hash};

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
    pub segment_index: Vec<SegmentRange>,
    #[serde(default)]
    pub name_hash: u64,
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
            segment_index: Vec::new(),
            name_hash: hash,
        }
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

pub struct TopicState {
    pub meta: TopicMeta,
    pub consumer_offsets: HashMap<u64, TopicConsumerOffset>,
    /// Cached minimum consumer offset. None means needs recomputation.
    cached_min_consumer_offset: Option<u64>,

    // Pre-initialized metrics handles
    m_publish_count: metrics::Counter,
    m_publish_bytes: metrics::Counter,
}

impl TopicState {
    pub fn new(meta: TopicMeta) -> Self {
        let labels = [("topic", meta.name.clone())];
        let m_publish_count = metrics::counter!("mq.topic.publish.count", &labels);
        let m_publish_bytes = metrics::counter!("mq.topic.publish.bytes", &labels);

        Self {
            meta,
            consumer_offsets: HashMap::new(),
            cached_min_consumer_offset: None,
            m_publish_count,
            m_publish_bytes,
        }
    }

    pub fn apply_publish(
        &mut self,
        log_index: u64,
        messages: &[MessagePayload],
    ) -> SmallVec<[u64; 16]> {
        let mut offsets = SmallVec::with_capacity(messages.len());
        // Each message in the batch gets the same raft log index as its offset.
        // In practice, the log_index is the index of the raft entry containing
        // the entire Publish command.
        let offset = log_index;
        let total_bytes: u64 = messages.iter().map(|m| m.value.len() as u64).sum();

        self.meta.head_index = offset;
        if self.meta.message_count == 0 {
            self.meta.tail_index = offset;
        }
        self.meta.message_count += messages.len() as u64;

        for _ in messages {
            offsets.push(offset);
        }

        self.m_publish_count.increment(messages.len() as u64);
        self.m_publish_bytes.increment(total_bytes);

        offsets
    }

    pub fn apply_commit_offset(&mut self, consumer_id: u64, offset: u64) {
        let entry =
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
            // Invalidate cache — new offset may change the min
            self.cached_min_consumer_offset = None;
        }
    }

    pub fn apply_purge(&mut self, before_index: u64) {
        if before_index > self.meta.tail_index {
            let old_tail = self.meta.tail_index;
            self.meta.tail_index = before_index;
            // Approximate message count reduction
            if self.meta.message_count > 0 && old_tail > 0 {
                let purged = before_index.saturating_sub(old_tail);
                self.meta.message_count = self.meta.message_count.saturating_sub(purged);
            }
            // Remove segments that are fully below the new tail
            self.meta
                .segment_index
                .retain(|s| s.max_index >= before_index);
        }
    }

    /// Returns the minimum log index required by this topic (for purge floor).
    pub fn min_required_index(&mut self) -> u64 {
        let consumer_min = *self.cached_min_consumer_offset.get_or_insert_with(|| {
            self.consumer_offsets
                .values()
                .map(|o| o.committed_offset)
                .min()
                .unwrap_or(u64::MAX)
        });
        self.meta.tail_index.min(consumer_min)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_topic(name: &str) -> TopicState {
        let meta = TopicMeta::new(1, name.to_string(), 1000, RetentionPolicy::default());
        TopicState::new(meta)
    }

    fn make_msg(value: &[u8]) -> MessagePayload {
        MessagePayload {
            key: None,
            value: Bytes::from(value.to_vec()),
            headers: Vec::new(),
            timestamp: 1000,
            ttl_ms: None,
            routing_key: None,
        }
    }

    #[test]
    fn test_topic_meta_defaults() {
        let meta = TopicMeta::new(42, "events".to_string(), 5000, RetentionPolicy::default());
        assert_eq!(meta.topic_id, 42);
        assert_eq!(meta.name, "events");
        assert_eq!(meta.head_index, 0);
        assert_eq!(meta.tail_index, 0);
        assert_eq!(meta.message_count, 0);
        assert!(meta.segment_index.is_empty());
    }

    #[test]
    fn test_publish_single_message() {
        let mut topic = make_topic("test");
        let msgs = vec![make_msg(b"hello")];
        let offsets = topic.apply_publish(10, &msgs);

        assert_eq!(offsets.as_slice(), &[10u64]);
        assert_eq!(topic.meta.head_index, 10);
        assert_eq!(topic.meta.tail_index, 10);
        assert_eq!(topic.meta.message_count, 1);
    }

    #[test]
    fn test_publish_batch() {
        let mut topic = make_topic("test");
        let msgs = vec![make_msg(b"a"), make_msg(b"b"), make_msg(b"c")];
        let offsets = topic.apply_publish(100, &msgs);

        assert_eq!(offsets.len(), 3);
        assert!(offsets.iter().all(|&o| o == 100));
        assert_eq!(topic.meta.message_count, 3);
    }

    #[test]
    fn test_publish_multiple_batches() {
        let mut topic = make_topic("test");
        topic.apply_publish(10, &[make_msg(b"first")]);
        topic.apply_publish(20, &[make_msg(b"second")]);

        assert_eq!(topic.meta.head_index, 20);
        assert_eq!(topic.meta.tail_index, 10);
        assert_eq!(topic.meta.message_count, 2);
    }

    #[test]
    fn test_commit_offset() {
        let mut topic = make_topic("test");
        topic.apply_publish(10, &[make_msg(b"msg")]);

        topic.apply_commit_offset(100, 10);
        assert_eq!(topic.consumer_offsets[&100].committed_offset, 10);

        // Advance offset
        topic.apply_commit_offset(100, 15);
        assert_eq!(topic.consumer_offsets[&100].committed_offset, 15);

        // Cannot go backwards
        topic.apply_commit_offset(100, 5);
        assert_eq!(topic.consumer_offsets[&100].committed_offset, 15);
    }

    #[test]
    fn test_commit_offset_multiple_consumers() {
        let mut topic = make_topic("test");
        topic.apply_commit_offset(1, 10);
        topic.apply_commit_offset(2, 20);

        assert_eq!(topic.consumer_offsets.len(), 2);
        assert_eq!(topic.consumer_offsets[&1].committed_offset, 10);
        assert_eq!(topic.consumer_offsets[&2].committed_offset, 20);
    }

    #[test]
    fn test_purge() {
        let mut topic = make_topic("test");
        topic.apply_publish(10, &[make_msg(b"a")]);
        topic.apply_publish(20, &[make_msg(b"b")]);
        topic.meta.segment_index.push(SegmentRange {
            segment_id: 1,
            min_index: 5,
            max_index: 12,
        });
        topic.meta.segment_index.push(SegmentRange {
            segment_id: 2,
            min_index: 13,
            max_index: 25,
        });

        topic.apply_purge(15);
        assert_eq!(topic.meta.tail_index, 15);
        // Segment 1 (max_index=12 < 15) removed, segment 2 retained
        assert_eq!(topic.meta.segment_index.len(), 1);
        assert_eq!(topic.meta.segment_index[0].segment_id, 2);
    }

    #[test]
    fn test_purge_no_op_if_before_tail() {
        let mut topic = make_topic("test");
        topic.apply_publish(20, &[make_msg(b"a")]);
        topic.apply_purge(10); // before tail, no-op
        assert_eq!(topic.meta.tail_index, 20);
    }

    #[test]
    fn test_min_required_index() {
        let mut topic = make_topic("test");
        topic.apply_publish(10, &[make_msg(b"a")]);
        topic.apply_publish(20, &[make_msg(b"b")]);
        assert_eq!(topic.min_required_index(), 10);

        // Consumer behind topic tail
        topic.apply_commit_offset(1, 5);
        assert_eq!(topic.min_required_index(), 5);

        // Consumer caught up
        topic.apply_commit_offset(1, 15);
        assert_eq!(topic.min_required_index(), 10);
    }

    #[test]
    fn test_consumer_offset_defaults() {
        let mut topic = make_topic("test");
        topic.apply_commit_offset(42, 100);
        let offset = &topic.consumer_offsets[&42];
        assert_eq!(offset.topic_id, 1);
        assert_eq!(offset.consumer_id, 42);
        assert_eq!(offset.pending_offset, 0);
    }
}

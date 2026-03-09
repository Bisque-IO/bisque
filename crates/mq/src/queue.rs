use std::collections::{BTreeMap, HashMap, HashSet};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use smallvec::SmallVec;

use crate::config::QueueConfig;
use crate::types::{MessagePayload, MessageState, QueueMessageMeta, SegmentRange, name_hash};

// =============================================================================
// Queue Metadata (persisted to MDBX)
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMeta {
    pub queue_id: u64,
    pub name: String,
    pub created_at: u64,
    pub config: QueueConfig,
    pub pending_count: u64,
    pub in_flight_count: u64,
    pub dlq_count: u64,
    #[serde(default)]
    pub segment_index: Vec<SegmentRange>,
    #[serde(default)]
    pub name_hash: u64,
}

impl QueueMeta {
    pub fn new(queue_id: u64, name: String, created_at: u64, config: QueueConfig) -> Self {
        let hash = name_hash(&name);
        Self {
            queue_id,
            name,
            created_at,
            config,
            pending_count: 0,
            in_flight_count: 0,
            dlq_count: 0,
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

// =============================================================================
// Dedup Window
// =============================================================================

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DedupWindow {
    pub window_secs: u64,
    /// timestamp_bucket → set of dedup keys
    pub buckets: BTreeMap<u64, HashSet<Bytes>>,
}

impl DedupWindow {
    pub fn new(window_secs: u64) -> Self {
        Self {
            window_secs,
            buckets: BTreeMap::new(),
        }
    }

    pub fn contains(&self, key: &Bytes, current_time: u64) -> bool {
        let cutoff = current_time.saturating_sub(self.window_secs);
        for (_bucket_time, keys) in self.buckets.range(cutoff..) {
            if keys.contains(key) {
                return true;
            }
        }
        false
    }

    pub fn insert(&mut self, key: Bytes, current_time: u64) {
        // Bucket by seconds
        let bucket = current_time;
        self.buckets.entry(bucket).or_default().insert(key);
    }

    pub fn prune(&mut self, before_timestamp: u64) {
        self.buckets = self.buckets.split_off(&before_timestamp);
    }

    pub fn snapshot_entries(&self) -> Vec<(u64, Vec<Bytes>)> {
        self.buckets
            .iter()
            .map(|(&ts, keys)| (ts, keys.iter().cloned().collect()))
            .collect()
    }

    pub fn restore_entries(&mut self, entries: Vec<(u64, Vec<Bytes>)>) {
        for (ts, keys) in entries {
            let set = self.buckets.entry(ts).or_default();
            for k in keys {
                set.insert(k);
            }
        }
    }
}

// =============================================================================
// In-memory Queue State
// =============================================================================

pub struct QueueState {
    pub meta: QueueMeta,
    /// Message index: message_id → metadata (state, attempts, consumer, etc.)
    pub messages: HashMap<u64, QueueMessageMeta>,
    /// Pending messages ordered by (priority, message_id) for efficient delivery.
    pub pending: BTreeMap<(u8, u64), ()>,
    /// Deduplication window.
    pub dedup: DedupWindow,
    /// In-flight deadline index: deadline_ms → set of message_ids.
    /// Enables O(log n) expired message lookups instead of O(n) full scan.
    pub(crate) in_flight_deadlines: BTreeMap<u64, Vec<u64>>,
    /// Consumer in-flight index: consumer_id → set of message_ids.
    /// Enables O(1) lookup on consumer disconnect instead of O(n) full scan.
    pub(crate) consumer_in_flight: HashMap<u64, Vec<u64>>,

    /// Cached min message_id for purge floor. `None` = dirty, needs recompute.
    cached_min_message_id: Option<Option<u64>>,

    // Pre-initialized metrics handles
    m_enqueue_count: metrics::Counter,
    m_deliver_count: metrics::Counter,
    m_ack_count: metrics::Counter,
    m_nack_count: metrics::Counter,
    m_dlq_count: metrics::Counter,
    m_timeout_count: metrics::Counter,
}

impl QueueState {
    pub fn new(meta: QueueMeta) -> Self {
        let labels = [("queue", meta.name.clone())];
        let m_enqueue_count = metrics::counter!("mq.queue.enqueue.count", &labels);
        let m_deliver_count = metrics::counter!("mq.queue.deliver.count", &labels);
        let m_ack_count = metrics::counter!("mq.queue.ack.count", &labels);
        let m_nack_count = metrics::counter!("mq.queue.nack.count", &labels);
        let m_dlq_count = metrics::counter!("mq.queue.dlq.count", &labels);
        let m_timeout_count = metrics::counter!("mq.queue.timeout.count", &labels);

        let dedup_window_secs = meta.config.dedup_window_secs.unwrap_or(0);
        Self {
            meta,
            messages: HashMap::new(),
            pending: BTreeMap::new(),
            dedup: DedupWindow::new(dedup_window_secs),
            in_flight_deadlines: BTreeMap::new(),
            consumer_in_flight: HashMap::new(),
            cached_min_message_id: Some(None),
            m_enqueue_count,
            m_deliver_count,
            m_ack_count,
            m_nack_count,
            m_dlq_count,
            m_timeout_count,
        }
    }

    pub fn apply_enqueue(
        &mut self,
        log_index: u64,
        messages: &[MessagePayload],
        dedup_keys: &[Option<Bytes>],
        current_time: u64,
    ) -> SmallVec<[u64; 16]> {
        let mut offsets = SmallVec::with_capacity(messages.len());
        let dedup_enabled = self.dedup.window_secs > 0;
        let delay_ms = self.meta.config.delay_default_ms;
        let queue_id = self.meta.queue_id;

        for (i, _msg) in messages.iter().enumerate() {
            let dedup_key = dedup_keys.get(i).and_then(|k| k.as_ref());

            // Check dedup
            if let Some(key) = dedup_key {
                if dedup_enabled && self.dedup.contains(key, current_time) {
                    offsets.push(log_index);
                    continue;
                }
            }

            let message_id = log_index;
            let deliver_after = if delay_ms > 0 {
                current_time + delay_ms
            } else {
                0
            };

            let meta = QueueMessageMeta {
                message_id,
                queue_id,
                state: MessageState::Pending,
                priority: 0,
                deliver_after,
                attempts: 0,
                last_delivered_at: None,
                consumer_id: None,
                visibility_deadline: None,
                dedup_key: dedup_key.cloned(),
            };

            self.messages.insert(message_id, meta);
            self.pending.insert((0, message_id), ());
            self.meta.pending_count += 1;

            // Update cached min: new message can only lower the floor
            if let Some(ref mut cached) = self.cached_min_message_id {
                *cached = Some(cached.map_or(message_id, |m| m.min(message_id)));
            }

            if dedup_enabled {
                if let Some(key) = dedup_key {
                    self.dedup.insert(key.clone(), current_time);
                }
            }

            offsets.push(message_id);
        }

        self.m_enqueue_count.increment(offsets.len() as u64);
        offsets
    }

    pub fn apply_deliver(
        &mut self,
        consumer_id: u64,
        max_count: u32,
        current_time: u64,
        _log_index: u64,
    ) -> SmallVec<[u64; 16]> {
        let max = max_count as usize;
        let mut delivered = SmallVec::<[u64; 16]>::new();
        let timeout_ms = self.meta.config.visibility_timeout_ms;
        let mut to_remove = SmallVec::<[(u8, u64); 16]>::new();

        for (&(priority, msg_id), _) in &self.pending {
            if delivered.len() >= max {
                break;
            }

            if let Some(meta) = self.messages.get(&msg_id) {
                if meta.deliver_after > current_time && meta.deliver_after > 0 {
                    continue;
                }
            }

            to_remove.push((priority, msg_id));
            delivered.push(msg_id);
        }

        for key in &to_remove {
            self.pending.remove(key);
        }

        let deadline = current_time + timeout_ms;
        for &msg_id in &delivered {
            if let Some(meta) = self.messages.get_mut(&msg_id) {
                meta.state = MessageState::InFlight;
                meta.consumer_id = Some(consumer_id);
                meta.last_delivered_at = Some(current_time);
                meta.visibility_deadline = Some(deadline);
                meta.attempts += 1;
            }
            self.meta.pending_count = self.meta.pending_count.saturating_sub(1);
            self.meta.in_flight_count += 1;

            // Maintain deadline index
            self.in_flight_deadlines
                .entry(deadline)
                .or_default()
                .push(msg_id);
            // Maintain consumer in-flight index
            self.consumer_in_flight
                .entry(consumer_id)
                .or_default()
                .push(msg_id);
        }

        self.m_deliver_count.increment(delivered.len() as u64);
        delivered
    }

    pub fn apply_ack(&mut self, message_ids: &[u64]) {
        let mut count = 0u64;
        for &msg_id in message_ids {
            if let Some(meta) = self.messages.remove(&msg_id) {
                if meta.state == MessageState::InFlight {
                    self.meta.in_flight_count = self.meta.in_flight_count.saturating_sub(1);
                    self.remove_from_deadline_index(meta.visibility_deadline, msg_id);
                    self.remove_from_consumer_index(meta.consumer_id, msg_id);
                    count += 1;

                    // Invalidate cached min if this could have been it
                    if let Some(Some(cached_min)) = self.cached_min_message_id {
                        if msg_id <= cached_min {
                            self.cached_min_message_id = None;
                        }
                    }
                }
            }
        }
        self.m_ack_count.increment(count);
    }

    pub fn apply_nack(&mut self, message_ids: &[u64]) {
        let mut count = 0u64;
        for &msg_id in message_ids {
            if let Some(meta) = self.messages.get_mut(&msg_id) {
                if meta.state == MessageState::InFlight {
                    let old_deadline = meta.visibility_deadline;
                    let old_consumer = meta.consumer_id;
                    meta.state = MessageState::Pending;
                    meta.consumer_id = None;
                    meta.visibility_deadline = None;
                    self.pending.insert((meta.priority, msg_id), ());
                    self.meta.in_flight_count = self.meta.in_flight_count.saturating_sub(1);
                    self.meta.pending_count += 1;
                    self.remove_from_deadline_index(old_deadline, msg_id);
                    self.remove_from_consumer_index(old_consumer, msg_id);
                    count += 1;
                }
            }
        }
        self.m_nack_count.increment(count);
    }

    pub fn apply_extend_visibility(&mut self, message_ids: &[u64], extension_ms: u64) {
        for &msg_id in message_ids {
            if let Some(meta) = self.messages.get_mut(&msg_id) {
                if meta.state == MessageState::InFlight {
                    if let Some(ref mut deadline) = meta.visibility_deadline {
                        let old_deadline = *deadline;
                        *deadline += extension_ms;
                        let new_deadline = *deadline;
                        // Move in deadline index
                        self.remove_from_deadline_index(Some(old_deadline), msg_id);
                        self.in_flight_deadlines
                            .entry(new_deadline)
                            .or_default()
                            .push(msg_id);
                    }
                }
            }
        }
    }

    pub fn apply_timeout_expired(
        &mut self,
        message_ids: &[u64],
        _dead_letter_queue: Option<u64>,
    ) -> SmallVec<[u64; 4]> {
        let max_retries = self.meta.config.max_retries;
        let mut dead_lettered = SmallVec::<[u64; 4]>::new();

        for &msg_id in message_ids {
            if let Some(meta) = self.messages.get_mut(&msg_id) {
                if meta.state != MessageState::InFlight {
                    continue;
                }

                let old_deadline = meta.visibility_deadline;
                let old_consumer = meta.consumer_id;

                if meta.attempts >= max_retries {
                    meta.state = MessageState::DeadLetter;
                    meta.consumer_id = None;
                    meta.visibility_deadline = None;
                    self.meta.in_flight_count = self.meta.in_flight_count.saturating_sub(1);
                    self.meta.dlq_count += 1;
                    self.m_dlq_count.increment(1);
                    dead_lettered.push(msg_id);
                } else {
                    meta.state = MessageState::Pending;
                    meta.consumer_id = None;
                    meta.visibility_deadline = None;
                    self.pending.insert((meta.priority, msg_id), ());
                    self.meta.in_flight_count = self.meta.in_flight_count.saturating_sub(1);
                    self.meta.pending_count += 1;
                    self.m_timeout_count.increment(1);
                }

                self.remove_from_deadline_index(old_deadline, msg_id);
                self.remove_from_consumer_index(old_consumer, msg_id);
            }
        }

        dead_lettered
    }

    pub fn apply_prune_dedup(&mut self, before_timestamp: u64) {
        self.dedup.prune(before_timestamp);
    }

    /// Find in-flight messages whose visibility deadline has passed.
    /// Uses the deadline BTreeMap index for O(log n + k) instead of O(n).
    pub fn find_expired_messages(&self, current_time: u64) -> SmallVec<[u64; 16]> {
        let mut expired = SmallVec::new();
        for (_deadline, msg_ids) in self.in_flight_deadlines.range(..=current_time) {
            for &msg_id in msg_ids {
                // Verify the message is still in-flight (may have been acked/nacked)
                if let Some(meta) = self.messages.get(&msg_id) {
                    if meta.state == MessageState::InFlight {
                        expired.push(msg_id);
                    }
                }
            }
        }
        expired
    }

    /// Get message IDs that are in-flight for a specific consumer.
    /// Uses the consumer index for O(1) lookup instead of O(n) full scan.
    pub fn consumer_in_flight_ids(&self, consumer_id: u64) -> SmallVec<[u64; 16]> {
        match self.consumer_in_flight.get(&consumer_id) {
            Some(ids) => ids
                .iter()
                .copied()
                .filter(|&msg_id| {
                    self.messages.get(&msg_id).is_some_and(|m| {
                        m.state == MessageState::InFlight && m.consumer_id == Some(consumer_id)
                    })
                })
                .collect(),
            None => SmallVec::new(),
        }
    }

    /// Returns the minimum log index required by this queue (for purge floor).
    /// Uses a cache that is maintained incrementally by enqueue/ack/timeout.
    pub fn min_required_index(&mut self) -> Option<u64> {
        if let Some(cached) = self.cached_min_message_id {
            return cached;
        }
        let min = self.messages.keys().copied().min();
        self.cached_min_message_id = Some(min);
        min
    }

    /// Remove a message from the deadline index.
    fn remove_from_deadline_index(&mut self, deadline: Option<u64>, msg_id: u64) {
        if let Some(dl) = deadline {
            if let Some(ids) = self.in_flight_deadlines.get_mut(&dl) {
                ids.retain(|&id| id != msg_id);
                if ids.is_empty() {
                    self.in_flight_deadlines.remove(&dl);
                }
            }
        }
    }

    /// Remove a message from the consumer in-flight index.
    fn remove_from_consumer_index(&mut self, consumer_id: Option<u64>, msg_id: u64) {
        if let Some(cid) = consumer_id {
            if let Some(ids) = self.consumer_in_flight.get_mut(&cid) {
                ids.retain(|&id| id != msg_id);
                if ids.is_empty() {
                    self.consumer_in_flight.remove(&cid);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MessagePayload;

    fn make_queue(name: &str) -> QueueState {
        let meta = QueueMeta::new(1, name.to_string(), 1000, QueueConfig::default());
        QueueState::new(meta)
    }

    fn make_queue_with_config(name: &str, config: QueueConfig) -> QueueState {
        let meta = QueueMeta::new(1, name.to_string(), 1000, config);
        QueueState::new(meta)
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

    // =========================================================================
    // DedupWindow tests
    // =========================================================================

    #[test]
    fn test_dedup_window_basic() {
        let mut dw = DedupWindow::new(60);
        let key = Bytes::from_static(b"key1");

        assert!(!dw.contains(&key, 100));
        dw.insert(key.clone(), 100);
        assert!(dw.contains(&key, 100));
        assert!(dw.contains(&key, 159)); // within window
        assert!(!dw.contains(&key, 161)); // outside window (cutoff = 161-60 = 101 > 100)
    }

    #[test]
    fn test_dedup_window_prune() {
        let mut dw = DedupWindow::new(60);
        dw.insert(Bytes::from_static(b"old"), 10);
        dw.insert(Bytes::from_static(b"new"), 100);

        dw.prune(50);
        assert_eq!(dw.buckets.len(), 1);
        assert!(dw.buckets.contains_key(&100));
    }

    #[test]
    fn test_dedup_window_snapshot_restore() {
        let mut dw = DedupWindow::new(60);
        dw.insert(Bytes::from_static(b"k1"), 10);
        dw.insert(Bytes::from_static(b"k2"), 20);

        let entries = dw.snapshot_entries();
        let mut dw2 = DedupWindow::new(60);
        dw2.restore_entries(entries);

        assert!(dw2.contains(&Bytes::from_static(b"k1"), 10));
        assert!(dw2.contains(&Bytes::from_static(b"k2"), 20));
    }

    #[test]
    fn test_dedup_window_disabled() {
        let dw = DedupWindow::new(0);
        assert!(!dw.contains(&Bytes::from_static(b"key"), 100));
    }

    // =========================================================================
    // QueueState tests
    // =========================================================================

    #[test]
    fn test_enqueue_basic() {
        let mut q = make_queue("test");
        let msgs = vec![make_msg(b"hello")];
        let offsets = q.apply_enqueue(10, &msgs, &[None], 1000);

        assert_eq!(offsets.as_slice(), &[10u64]);
        assert_eq!(q.meta.pending_count, 1);
        assert!(q.messages.contains_key(&10));
        assert_eq!(q.messages[&10].state, MessageState::Pending);
        assert!(q.pending.contains_key(&(0, 10)));
    }

    #[test]
    fn test_enqueue_with_dedup() {
        let config = QueueConfig {
            dedup_window_secs: Some(60),
            ..Default::default()
        };
        let mut q = make_queue_with_config("test", config);
        let key = Bytes::from_static(b"dedup-key");

        let offsets1 = q.apply_enqueue(10, &[make_msg(b"first")], &[Some(key.clone())], 1000);
        assert_eq!(offsets1.len(), 1);
        assert_eq!(q.meta.pending_count, 1);

        // Duplicate within window — skipped
        let offsets2 = q.apply_enqueue(11, &[make_msg(b"second")], &[Some(key.clone())], 1005);
        assert_eq!(offsets2.len(), 1);
        assert_eq!(q.meta.pending_count, 1); // no new message
    }

    #[test]
    fn test_enqueue_with_delay() {
        let config = QueueConfig {
            delay_default_ms: 5000,
            ..Default::default()
        };
        let mut q = make_queue_with_config("test", config);
        q.apply_enqueue(10, &[make_msg(b"delayed")], &[None], 1000);
        assert_eq!(q.messages[&10].deliver_after, 6000);
    }

    #[test]
    fn test_deliver_basic() {
        let mut q = make_queue("test");
        q.apply_enqueue(10, &[make_msg(b"msg1")], &[None], 1000);
        q.apply_enqueue(11, &[make_msg(b"msg2")], &[None], 1000);

        let delivered = q.apply_deliver(100, 1, 2000, 20);
        assert_eq!(delivered.len(), 1);
        assert_eq!(q.meta.pending_count, 1);
        assert_eq!(q.meta.in_flight_count, 1);

        let msg = &q.messages[&delivered[0]];
        assert_eq!(msg.state, MessageState::InFlight);
        assert_eq!(msg.consumer_id, Some(100));
        assert_eq!(msg.attempts, 1);
        assert!(msg.visibility_deadline.is_some());
    }

    #[test]
    fn test_deliver_respects_max_count() {
        let mut q = make_queue("test");
        for i in 10..15 {
            q.apply_enqueue(i, &[make_msg(b"x")], &[None], 1000);
        }
        let delivered = q.apply_deliver(100, 3, 2000, 20);
        assert_eq!(delivered.len(), 3);
        assert_eq!(q.meta.pending_count, 2);
        assert_eq!(q.meta.in_flight_count, 3);
    }

    #[test]
    fn test_deliver_skips_delayed() {
        let config = QueueConfig {
            delay_default_ms: 5000,
            ..Default::default()
        };
        let mut q = make_queue_with_config("test", config);
        q.apply_enqueue(10, &[make_msg(b"delayed")], &[None], 1000);

        let delivered = q.apply_deliver(100, 10, 3000, 20);
        assert_eq!(delivered.len(), 0);

        let delivered = q.apply_deliver(100, 10, 7000, 21);
        assert_eq!(delivered.len(), 1);
    }

    #[test]
    fn test_ack() {
        let mut q = make_queue("test");
        q.apply_enqueue(10, &[make_msg(b"msg")], &[None], 1000);
        q.apply_deliver(100, 1, 2000, 20);

        q.apply_ack(&[10]);
        assert_eq!(q.meta.in_flight_count, 0);
        assert!(!q.messages.contains_key(&10));
    }

    #[test]
    fn test_nack() {
        let mut q = make_queue("test");
        q.apply_enqueue(10, &[make_msg(b"msg")], &[None], 1000);
        q.apply_deliver(100, 1, 2000, 20);

        q.apply_nack(&[10]);
        assert_eq!(q.meta.in_flight_count, 0);
        assert_eq!(q.meta.pending_count, 1);
        assert_eq!(q.messages[&10].state, MessageState::Pending);
        assert!(q.messages[&10].consumer_id.is_none());
        assert!(q.pending.contains_key(&(0, 10)));
    }

    #[test]
    fn test_extend_visibility() {
        let mut q = make_queue("test");
        q.apply_enqueue(10, &[make_msg(b"msg")], &[None], 1000);
        q.apply_deliver(100, 1, 2000, 20);

        let orig_deadline = q.messages[&10].visibility_deadline.unwrap();
        q.apply_extend_visibility(&[10], 5000);
        assert_eq!(
            q.messages[&10].visibility_deadline.unwrap(),
            orig_deadline + 5000
        );
    }

    #[test]
    fn test_timeout_expired_requeue() {
        let mut q = make_queue("test");
        q.apply_enqueue(10, &[make_msg(b"msg")], &[None], 1000);
        q.apply_deliver(100, 1, 2000, 20);

        let dead_lettered = q.apply_timeout_expired(&[10], None);
        assert!(dead_lettered.is_empty());
        assert_eq!(q.meta.pending_count, 1);
        assert_eq!(q.meta.in_flight_count, 0);
        assert_eq!(q.messages[&10].state, MessageState::Pending);
    }

    #[test]
    fn test_timeout_expired_dead_letter() {
        let config = QueueConfig {
            max_retries: 2,
            ..Default::default()
        };
        let mut q = make_queue_with_config("test", config);
        q.apply_enqueue(10, &[make_msg(b"msg")], &[None], 1000);

        q.apply_deliver(100, 1, 2000, 20);
        q.apply_timeout_expired(&[10], None); // attempt 1 → re-enqueue
        q.apply_deliver(100, 1, 3000, 30); // attempt 2
        let dead_lettered = q.apply_timeout_expired(&[10], None);

        assert_eq!(dead_lettered.as_slice(), &[10u64]);
        assert_eq!(q.messages[&10].state, MessageState::DeadLetter);
        assert_eq!(q.meta.dlq_count, 1);
        assert_eq!(q.meta.in_flight_count, 0);
    }

    #[test]
    fn test_find_expired_messages() {
        let mut q = make_queue("test");
        q.apply_enqueue(10, &[make_msg(b"a")], &[None], 1000);
        q.apply_enqueue(11, &[make_msg(b"b")], &[None], 1000);
        q.apply_deliver(100, 2, 2000, 20);

        assert!(q.find_expired_messages(2000).is_empty());

        // deadline = 2000 + 30000 = 32000
        let expired = q.find_expired_messages(33000);
        assert_eq!(expired.len(), 2);
    }

    #[test]
    fn test_prune_dedup() {
        let config = QueueConfig {
            dedup_window_secs: Some(60),
            ..Default::default()
        };
        let mut q = make_queue_with_config("test", config);
        q.dedup.insert(Bytes::from_static(b"old"), 10);
        q.dedup.insert(Bytes::from_static(b"new"), 100);

        q.apply_prune_dedup(50);
        assert!(!q.dedup.buckets.contains_key(&10));
        assert!(q.dedup.buckets.contains_key(&100));
    }

    #[test]
    fn test_min_required_index() {
        let mut q = make_queue("test");
        assert!(q.min_required_index().is_none());

        q.apply_enqueue(10, &[make_msg(b"a")], &[None], 1000);
        q.apply_enqueue(20, &[make_msg(b"b")], &[None], 1000);
        assert_eq!(q.min_required_index(), Some(10));

        q.apply_deliver(100, 1, 2000, 30);
        q.apply_ack(&[10]);
        assert_eq!(q.min_required_index(), Some(20));
    }
}

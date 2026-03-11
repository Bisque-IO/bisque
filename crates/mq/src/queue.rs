use parking_lot::Mutex;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use smallvec::SmallVec;

use crate::config::QueueConfig;
use crate::flat::FlatMessageMeta;
use crate::types::{MessageState, QueueMessageMeta, name_hash};

/// Sentinel: cached min needs full recompute.
const MIN_DIRTY: u64 = u64::MAX;
/// Sentinel: computed result is None (no messages).
const MIN_NONE: u64 = u64::MAX - 1;

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
    pub pending_bytes: u64,
    #[serde(default)]
    pub in_flight_bytes: u64,
    #[serde(default)]
    pub total_messages: u64,
    #[serde(default)]
    pub total_bytes: u64,
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
            pending_bytes: 0,
            in_flight_bytes: 0,
            total_messages: 0,
            total_bytes: 0,
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

/// Lock-free queue state.
///
/// All inner collections use interior mutability so methods take `&self`.
/// The Raft apply path (single writer) and leader tasks (readers) can
/// operate without contention on the outer DashMap.
pub struct QueueState {
    pub meta: QueueMeta,

    // -- Hot counters (atomics for interior mutability) --
    pending_count: AtomicU64,
    in_flight_count: AtomicU64,
    dlq_count: AtomicU64,
    pending_bytes: AtomicU64,
    in_flight_bytes: AtomicU64,
    total_messages: AtomicU64,
    total_bytes: AtomicU64,

    /// Message index: message_id → metadata (state, attempts, consumer, etc.)
    pub(crate) messages: DashMap<u64, QueueMessageMeta>,
    /// Pending messages ordered by (priority, message_id) for efficient delivery.
    pending: Mutex<BTreeMap<(u8, u64), ()>>,
    /// Deduplication window.
    dedup: Mutex<DedupWindow>,
    /// In-flight deadline index: deadline_ms → set of message_ids.
    /// Enables O(log n) expired message lookups instead of O(n) full scan.
    in_flight_deadlines: Mutex<BTreeMap<u64, Vec<u64>>>,
    /// Consumer in-flight index: consumer_id → set of message_ids.
    /// Enables O(1) lookup on consumer disconnect instead of O(n) full scan.
    consumer_in_flight: DashMap<u64, Vec<u64>>,
    /// Expiry deadline index: expires_at_ms → set of message_ids.
    /// Enables O(log n + k) scanning for messages that have exceeded their TTL.
    expires_at_deadlines: Mutex<BTreeMap<u64, Vec<u64>>>,

    /// Cached min message_id for purge floor.
    /// `MIN_DIRTY` = needs recompute, `MIN_NONE` = no messages, else = cached value.
    cached_min_message_id: AtomicU64,

    // Pre-initialized metrics handles
    m_enqueue_count: metrics::Counter,
    m_deliver_count: metrics::Counter,
    m_ack_count: metrics::Counter,
    m_nack_count: metrics::Counter,
    m_dlq_count: metrics::Counter,
    m_timeout_count: metrics::Counter,
}

impl QueueState {
    pub fn new(meta: QueueMeta, catalog_name: &str) -> Self {
        let labels = [
            ("catalog", catalog_name.to_owned()),
            ("queue", meta.name.clone()),
        ];
        let m_enqueue_count = metrics::counter!("mq.queue.enqueue.count", &labels);
        let m_deliver_count = metrics::counter!("mq.queue.deliver.count", &labels);
        let m_ack_count = metrics::counter!("mq.queue.ack.count", &labels);
        let m_nack_count = metrics::counter!("mq.queue.nack.count", &labels);
        let m_dlq_count = metrics::counter!("mq.queue.dlq.count", &labels);
        let m_timeout_count = metrics::counter!("mq.queue.timeout.count", &labels);

        let dedup_window_secs = meta.config.dedup_window_secs.unwrap_or(0);
        let pending_count = meta.pending_count;
        let in_flight_count = meta.in_flight_count;
        let dlq_count = meta.dlq_count;
        let pending_bytes = meta.pending_bytes;
        let in_flight_bytes = meta.in_flight_bytes;
        let total_messages = meta.total_messages;
        let total_bytes = meta.total_bytes;
        Self {
            meta,
            pending_count: AtomicU64::new(pending_count),
            in_flight_count: AtomicU64::new(in_flight_count),
            dlq_count: AtomicU64::new(dlq_count),
            pending_bytes: AtomicU64::new(pending_bytes),
            in_flight_bytes: AtomicU64::new(in_flight_bytes),
            total_messages: AtomicU64::new(total_messages),
            total_bytes: AtomicU64::new(total_bytes),
            messages: DashMap::new(),
            pending: Mutex::new(BTreeMap::new()),
            dedup: Mutex::new(DedupWindow::new(dedup_window_secs)),
            in_flight_deadlines: Mutex::new(BTreeMap::new()),
            consumer_in_flight: DashMap::new(),
            expires_at_deadlines: Mutex::new(BTreeMap::new()),
            cached_min_message_id: AtomicU64::new(MIN_NONE),
            m_enqueue_count,
            m_deliver_count,
            m_ack_count,
            m_nack_count,
            m_dlq_count,
            m_timeout_count,
        }
    }

    // -- Counter accessors --

    #[inline]
    pub fn pending_count(&self) -> u64 {
        self.pending_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn in_flight_count(&self) -> u64 {
        self.in_flight_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn dlq_count(&self) -> u64 {
        self.dlq_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn in_flight_bytes(&self) -> u64 {
        self.in_flight_bytes.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn total_messages(&self) -> u64 {
        self.total_messages.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Sync atomic counters back to meta for snapshotting.
    pub fn sync_meta_counts(&mut self) {
        self.meta.pending_count = self.pending_count.load(Ordering::Relaxed);
        self.meta.in_flight_count = self.in_flight_count.load(Ordering::Relaxed);
        self.meta.dlq_count = self.dlq_count.load(Ordering::Relaxed);
        self.meta.pending_bytes = self.pending_bytes.load(Ordering::Relaxed);
        self.meta.in_flight_bytes = self.in_flight_bytes.load(Ordering::Relaxed);
        self.meta.total_messages = self.total_messages.load(Ordering::Relaxed);
        self.meta.total_bytes = self.total_bytes.load(Ordering::Relaxed);
    }

    /// Check if the queue has any messages.
    pub fn has_messages(&self) -> bool {
        !self.messages.is_empty()
    }

    /// Whether the expiry deadlines index has any entries.
    pub fn has_expiry_deadlines(&self) -> bool {
        !self.expires_at_deadlines.lock().is_empty()
    }

    /// Get the dedup window_secs config.
    pub fn dedup_window_secs(&self) -> u64 {
        self.dedup.lock().window_secs
    }

    /// Whether the dedup window has any entries.
    pub fn has_dedup_entries(&self) -> bool {
        let guard = self.dedup.lock();
        guard.window_secs > 0 && !guard.buckets.is_empty()
    }

    /// Snapshot the dedup window entries.
    pub fn dedup_snapshot_entries(&self) -> Vec<(u64, Vec<Bytes>)> {
        self.dedup.lock().snapshot_entries()
    }

    /// Restore dedup entries from snapshot.
    pub fn dedup_restore_entries(&self, entries: Vec<(u64, Vec<Bytes>)>) {
        self.dedup.lock().restore_entries(entries);
    }

    /// Insert a message directly (used during snapshot restore).
    pub fn restore_message(&self, msg: QueueMessageMeta) {
        let msg_id = msg.message_id;
        let state = msg.state;
        let priority = msg.priority;
        let visibility_deadline = msg.visibility_deadline;
        let consumer_id = msg.consumer_id;

        if state == MessageState::Pending {
            self.pending.lock().insert((priority, msg_id), ());
        } else if state == MessageState::InFlight {
            if let Some(deadline) = visibility_deadline {
                self.in_flight_deadlines
                    .lock()
                    .entry(deadline)
                    .or_default()
                    .push(msg_id);
            }
            if let Some(cid) = consumer_id {
                self.consumer_in_flight.entry(cid).or_default().push(msg_id);
            }
        }
        self.messages.insert(msg_id, msg);
    }

    /// Clear all queue state (used by PURGE_QUEUE command).
    pub fn purge(&self) -> bool {
        let had_messages = !self.messages.is_empty();
        self.messages.clear();
        self.pending.lock().clear();
        self.in_flight_deadlines.lock().clear();
        self.consumer_in_flight.clear();
        self.expires_at_deadlines.lock().clear();
        self.pending_count.store(0, Ordering::Relaxed);
        self.in_flight_count.store(0, Ordering::Relaxed);
        self.pending_bytes.store(0, Ordering::Relaxed);
        self.in_flight_bytes.store(0, Ordering::Relaxed);
        self.cached_min_message_id
            .store(MIN_NONE, Ordering::Relaxed);
        had_messages
    }

    /// Enqueue pre-encoded flat messages.
    ///
    /// Each entry in `messages` is a raw flat-encoded `Bytes` buffer (see
    /// `flat::FlatMessageBuilder`). Only the fixed 32-byte header is parsed
    /// to extract `ttl_ms` and `delay_ms` — the variable-length payload is
    /// never deserialized on this path.
    pub fn apply_enqueue(
        &self,
        log_index: u64,
        messages: &[Bytes],
        dedup_keys: &[Option<Bytes>],
        current_time: u64,
    ) -> usize {
        let mut enqueued: usize = 0;
        let mut dedup_guard = self.dedup.lock();
        let dedup_enabled = dedup_guard.window_secs > 0;
        let default_delay_ms = self.meta.config.delay_default_ms;
        let queue_id = self.meta.queue_id;

        let mut pending_guard = self.pending.lock();
        let mut expires_guard = self.expires_at_deadlines.lock();

        for (i, msg_buf) in messages.iter().enumerate() {
            let dedup_key = dedup_keys.get(i).and_then(|k| k.as_ref());

            // Check dedup
            if let Some(key) = dedup_key {
                if dedup_enabled && dedup_guard.contains(key, current_time) {
                    enqueued += 1;
                    continue;
                }
            }

            let message_id = log_index;

            // Extract metadata from the flat header (first 32 bytes only)
            let flat_meta = FlatMessageMeta::parse(msg_buf);

            // Compute expires_at from per-message ttl_ms
            let expires_at = flat_meta
                .and_then(|m| m.ttl_ms_opt())
                .map(|ttl| current_time + ttl);

            // Per-message delay_ms overrides queue-level delay_default_ms
            let msg_delay = flat_meta.and_then(|m| m.delay_ms_opt());
            let effective_delay = msg_delay.unwrap_or(default_delay_ms);
            let deliver_after = if effective_delay > 0 {
                let da = current_time + effective_delay;
                // Validate: delay must not extend past expires_at
                if let Some(exp) = expires_at {
                    da.min(exp)
                } else {
                    da
                }
            } else {
                0
            };

            // Extract reply_to and correlation_id for request/reply pattern.
            let (reply_to, correlation_id) =
                if flat_meta.map_or(false, |m| m.has_reply_to() || m.has_correlation_id()) {
                    let flat = crate::flat::FlatMessage::new(msg_buf.clone());
                    (
                        flat.as_ref().and_then(|f| f.reply_to()),
                        flat.as_ref().and_then(|f| f.correlation_id()),
                    )
                } else {
                    (None, None)
                };

            let value_len = FlatMessageMeta::value_len(msg_buf).unwrap_or(0) as u32;

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
                expires_at,
                reply_to,
                correlation_id,
                value_len,
            };

            self.messages.insert(message_id, meta);
            pending_guard.insert((0, message_id), ());
            self.pending_count.fetch_add(1, Ordering::Relaxed);
            self.pending_bytes
                .fetch_add(value_len as u64, Ordering::Relaxed);
            self.total_messages.fetch_add(1, Ordering::Relaxed);
            self.total_bytes
                .fetch_add(value_len as u64, Ordering::Relaxed);

            // Maintain expiry deadline index for proactive scanning
            if let Some(exp) = expires_at {
                expires_guard.entry(exp).or_default().push(message_id);
            }

            // Update cached min: new message can only lower the floor
            let cached = self.cached_min_message_id.load(Ordering::Relaxed);
            if cached != MIN_DIRTY {
                if cached == MIN_NONE {
                    self.cached_min_message_id
                        .store(message_id, Ordering::Relaxed);
                } else {
                    self.cached_min_message_id
                        .store(cached.min(message_id), Ordering::Relaxed);
                }
            }

            if dedup_enabled {
                if let Some(key) = dedup_key {
                    dedup_guard.insert(key.clone(), current_time);
                }
            }

            enqueued += 1;
        }

        self.m_enqueue_count.increment(enqueued as u64);
        enqueued
    }

    pub fn apply_deliver(
        &self,
        consumer_id: u64,
        max_count: u32,
        current_time: u64,
        _log_index: u64,
    ) -> SmallVec<[u64; 16]> {
        let max = max_count as usize;
        let mut delivered = SmallVec::<[u64; 16]>::new();
        let timeout_ms = self.meta.config.visibility_timeout_ms;
        let mut to_remove = SmallVec::<[(u8, u64); 16]>::new();
        let mut expired_ids = SmallVec::<[(u8, u64); 4]>::new();

        let mut pending_guard = self.pending.lock();

        for (&(priority, msg_id), _) in pending_guard.iter() {
            if delivered.len() >= max {
                break;
            }

            if let Some(meta) = self.messages.get(&msg_id) {
                // Check if message has expired (total TTL exceeded)
                if let Some(exp) = meta.expires_at {
                    if current_time >= exp {
                        expired_ids.push((priority, msg_id));
                        continue;
                    }
                }
                if meta.deliver_after > current_time && meta.deliver_after > 0 {
                    continue;
                }
            }

            to_remove.push((priority, msg_id));
            delivered.push(msg_id);
        }

        // Remove expired messages from pending
        for &(priority, msg_id) in &expired_ids {
            pending_guard.remove(&(priority, msg_id));
            if let Some((_, meta)) = self.messages.remove(&msg_id) {
                self.pending_count.fetch_sub(1, Ordering::Relaxed);
                self.pending_bytes
                    .fetch_sub(meta.value_len as u64, Ordering::Relaxed);
                self.remove_from_expires_index_locked(meta.expires_at, msg_id);
                // Invalidate cached min if this could have been it
                let cached = self.cached_min_message_id.load(Ordering::Relaxed);
                if cached != MIN_DIRTY && cached != MIN_NONE && msg_id <= cached {
                    self.cached_min_message_id
                        .store(MIN_DIRTY, Ordering::Relaxed);
                }
            }
        }

        for key in &to_remove {
            pending_guard.remove(key);
        }
        drop(pending_guard);

        let deadline = current_time + timeout_ms;
        let mut deadlines_guard = self.in_flight_deadlines.lock();
        for &msg_id in &delivered {
            let msg_bytes = if let Some(mut meta) = self.messages.get_mut(&msg_id) {
                meta.state = MessageState::InFlight;
                meta.consumer_id = Some(consumer_id);
                meta.last_delivered_at = Some(current_time);
                meta.visibility_deadline = Some(deadline);
                meta.attempts += 1;
                meta.value_len as u64
            } else {
                0
            };
            self.pending_count.fetch_sub(1, Ordering::Relaxed);
            self.in_flight_count.fetch_add(1, Ordering::Relaxed);
            self.pending_bytes.fetch_sub(msg_bytes, Ordering::Relaxed);
            self.in_flight_bytes.fetch_add(msg_bytes, Ordering::Relaxed);

            // Maintain deadline index
            deadlines_guard.entry(deadline).or_default().push(msg_id);
            // Maintain consumer in-flight index
            self.consumer_in_flight
                .entry(consumer_id)
                .or_default()
                .push(msg_id);
        }
        drop(deadlines_guard);

        self.m_deliver_count.increment(delivered.len() as u64);
        delivered
    }

    pub fn apply_ack(&self, message_ids: &[u64]) {
        let mut count = 0u64;
        for &msg_id in message_ids {
            if let Some((_, meta)) = self.messages.remove(&msg_id) {
                if meta.state == MessageState::InFlight {
                    self.in_flight_count.fetch_sub(1, Ordering::Relaxed);
                    self.in_flight_bytes
                        .fetch_sub(meta.value_len as u64, Ordering::Relaxed);
                    self.remove_from_deadline_index(meta.visibility_deadline, msg_id);
                    self.remove_from_consumer_index(meta.consumer_id, msg_id);
                    self.remove_from_expires_index(meta.expires_at, msg_id);
                    count += 1;

                    // Invalidate cached min if this could have been it
                    let cached = self.cached_min_message_id.load(Ordering::Relaxed);
                    if cached != MIN_DIRTY && cached != MIN_NONE && msg_id <= cached {
                        self.cached_min_message_id
                            .store(MIN_DIRTY, Ordering::Relaxed);
                    }
                }
            }
        }
        self.m_ack_count.increment(count);
    }

    pub fn apply_nack(&self, message_ids: &[u64]) {
        let mut count = 0u64;
        let mut pending_guard = self.pending.lock();
        for &msg_id in message_ids {
            if let Some(mut meta) = self.messages.get_mut(&msg_id) {
                if meta.state == MessageState::InFlight {
                    let old_deadline = meta.visibility_deadline;
                    let old_consumer = meta.consumer_id;
                    let priority = meta.priority;
                    let msg_bytes = meta.value_len as u64;
                    meta.state = MessageState::Pending;
                    meta.consumer_id = None;
                    meta.visibility_deadline = None;
                    pending_guard.insert((priority, msg_id), ());
                    self.in_flight_count.fetch_sub(1, Ordering::Relaxed);
                    self.pending_count.fetch_add(1, Ordering::Relaxed);
                    self.in_flight_bytes.fetch_sub(msg_bytes, Ordering::Relaxed);
                    self.pending_bytes.fetch_add(msg_bytes, Ordering::Relaxed);
                    drop(meta);
                    self.remove_from_deadline_index(old_deadline, msg_id);
                    self.remove_from_consumer_index(old_consumer, msg_id);
                    count += 1;
                }
            }
        }
        drop(pending_guard);
        self.m_nack_count.increment(count);
    }

    pub fn apply_extend_visibility(&self, message_ids: &[u64], extension_ms: u64) {
        let mut deadlines_guard = self.in_flight_deadlines.lock();
        for &msg_id in message_ids {
            if let Some(mut meta) = self.messages.get_mut(&msg_id) {
                if meta.state == MessageState::InFlight {
                    if let Some(ref mut deadline) = meta.visibility_deadline {
                        let old_deadline = *deadline;
                        *deadline += extension_ms;
                        let new_deadline = *deadline;
                        // Move in deadline index
                        Self::remove_from_deadline_index_locked(
                            &mut deadlines_guard,
                            Some(old_deadline),
                            msg_id,
                        );
                        deadlines_guard
                            .entry(new_deadline)
                            .or_default()
                            .push(msg_id);
                    }
                }
            }
        }
    }

    pub fn apply_timeout_expired(
        &self,
        message_ids: &[u64],
        _dead_letter_topic: Option<u64>,
        current_time: u64,
    ) -> SmallVec<[u64; 4]> {
        let max_retries = self.meta.config.max_retries;
        let mut dead_lettered = SmallVec::<[u64; 4]>::new();
        let mut pending_guard = self.pending.lock();

        for &msg_id in message_ids {
            if let Some(mut meta) = self.messages.get_mut(&msg_id) {
                if meta.state != MessageState::InFlight {
                    continue;
                }

                let old_deadline = meta.visibility_deadline;
                let old_consumer = meta.consumer_id;
                let msg_expires_at = meta.expires_at;
                let msg_bytes = meta.value_len as u64;

                // If the message has exceeded its total TTL, dead-letter it
                // regardless of retry count.
                let total_expired = msg_expires_at.is_some_and(|exp| current_time >= exp);
                let is_dead = meta.attempts >= max_retries || total_expired;

                if is_dead {
                    meta.state = MessageState::DeadLetter;
                    meta.consumer_id = None;
                    meta.visibility_deadline = None;
                    self.in_flight_count.fetch_sub(1, Ordering::Relaxed);
                    self.in_flight_bytes.fetch_sub(msg_bytes, Ordering::Relaxed);
                    self.dlq_count.fetch_add(1, Ordering::Relaxed);
                    self.m_dlq_count.increment(1);
                    dead_lettered.push(msg_id);
                } else {
                    let priority = meta.priority;
                    meta.state = MessageState::Pending;
                    meta.consumer_id = None;
                    meta.visibility_deadline = None;
                    pending_guard.insert((priority, msg_id), ());
                    self.in_flight_count.fetch_sub(1, Ordering::Relaxed);
                    self.in_flight_bytes.fetch_sub(msg_bytes, Ordering::Relaxed);
                    self.pending_count.fetch_add(1, Ordering::Relaxed);
                    self.pending_bytes.fetch_add(msg_bytes, Ordering::Relaxed);
                    self.m_timeout_count.increment(1);
                }

                drop(meta);
                self.remove_from_deadline_index(old_deadline, msg_id);
                self.remove_from_consumer_index(old_consumer, msg_id);
                if is_dead {
                    self.remove_from_expires_index(msg_expires_at, msg_id);
                }
            }
        }

        dead_lettered
    }

    /// Remove dead-lettered messages from this queue after they have been
    /// published to the DLQ topic. This frees the entries from the message
    /// index so they no longer pin the purge floor.
    pub fn apply_remove_dead_lettered(&self, message_ids: &[u64]) {
        for &msg_id in message_ids {
            if let Some(meta) = self.messages.get(&msg_id) {
                if meta.state != MessageState::DeadLetter {
                    continue;
                }
                self.dlq_count.fetch_sub(1, Ordering::Relaxed);
                // Invalidate cached min if this could have been it
                let cached = self.cached_min_message_id.load(Ordering::Relaxed);
                if cached != MIN_DIRTY && cached != MIN_NONE && msg_id <= cached {
                    self.cached_min_message_id
                        .store(MIN_DIRTY, Ordering::Relaxed);
                }
            }
            self.messages.remove(&msg_id);
        }
    }

    pub fn apply_prune_dedup(&self, before_timestamp: u64) {
        self.dedup.lock().prune(before_timestamp);
    }

    /// Find in-flight messages whose visibility deadline has passed.
    /// Uses the deadline BTreeMap index for O(log n + k) instead of O(n).
    pub fn find_expired_messages(&self, current_time: u64) -> SmallVec<[u64; 16]> {
        let mut expired = SmallVec::new();
        let deadlines = self.in_flight_deadlines.lock();
        for (_deadline, msg_ids) in deadlines.range(..=current_time) {
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
    /// Uses an atomic cache maintained incrementally by enqueue/ack/timeout.
    pub fn min_required_index(&self) -> Option<u64> {
        let cached = self.cached_min_message_id.load(Ordering::Relaxed);
        if cached == MIN_NONE {
            return None;
        }
        if cached != MIN_DIRTY {
            return Some(cached);
        }
        // Recompute
        let mut min: Option<u64> = None;
        for entry in self.messages.iter() {
            let msg_id = *entry.key();
            min = Some(min.map_or(msg_id, |m: u64| m.min(msg_id)));
        }
        match min {
            Some(v) => {
                self.cached_min_message_id.store(v, Ordering::Relaxed);
                Some(v)
            }
            None => {
                self.cached_min_message_id
                    .store(MIN_NONE, Ordering::Relaxed);
                None
            }
        }
    }

    /// Find pending messages whose `expires_at` has passed.
    /// Uses the expires_at BTreeMap index for O(log n + k) efficiency.
    pub fn find_expired_pending(&self, current_time: u64) -> SmallVec<[u64; 16]> {
        let mut expired = SmallVec::new();
        let expires = self.expires_at_deadlines.lock();
        for (_exp, msg_ids) in expires.range(..=current_time) {
            for &msg_id in msg_ids {
                if let Some(meta) = self.messages.get(&msg_id) {
                    if meta.state == MessageState::Pending {
                        expired.push(msg_id);
                    }
                }
            }
        }
        expired
    }

    /// Remove expired pending messages from the queue.
    pub fn apply_expire_pending(&self, message_ids: &[u64]) {
        let mut pending_guard = self.pending.lock();
        for &msg_id in message_ids {
            if let Some(meta) = self.messages.get(&msg_id) {
                if meta.state != MessageState::Pending {
                    continue;
                }
                let priority = meta.priority;
                let expires_at = meta.expires_at;
                pending_guard.remove(&(priority, msg_id));
                drop(meta);
                self.remove_from_expires_index(expires_at, msg_id);
                self.pending_count.fetch_sub(1, Ordering::Relaxed);
                // Invalidate cached min if this could have been it
                let cached = self.cached_min_message_id.load(Ordering::Relaxed);
                if cached != MIN_DIRTY && cached != MIN_NONE && msg_id <= cached {
                    self.cached_min_message_id
                        .store(MIN_DIRTY, Ordering::Relaxed);
                }
            }
            self.messages.remove(&msg_id);
        }
    }

    /// Remove a message from the expires_at deadline index.
    fn remove_from_expires_index(&self, expires_at: Option<u64>, msg_id: u64) {
        if let Some(exp) = expires_at {
            let mut guard = self.expires_at_deadlines.lock();
            if let Some(ids) = guard.get_mut(&exp) {
                if let Some(pos) = ids.iter().position(|&id| id == msg_id) {
                    ids.swap_remove(pos);
                }
                if ids.is_empty() {
                    guard.remove(&exp);
                }
            }
        }
    }

    /// Remove from expires index when the lock is already held (during apply_deliver).
    fn remove_from_expires_index_locked(&self, expires_at: Option<u64>, msg_id: u64) {
        if let Some(exp) = expires_at {
            let mut guard = self.expires_at_deadlines.lock();
            if let Some(ids) = guard.get_mut(&exp) {
                if let Some(pos) = ids.iter().position(|&id| id == msg_id) {
                    ids.swap_remove(pos);
                }
                if ids.is_empty() {
                    guard.remove(&exp);
                }
            }
        }
    }

    /// Remove a message from the deadline index.
    fn remove_from_deadline_index(&self, deadline: Option<u64>, msg_id: u64) {
        if let Some(dl) = deadline {
            let mut guard = self.in_flight_deadlines.lock();
            Self::remove_from_deadline_index_locked(&mut guard, Some(dl), msg_id);
        }
    }

    /// Remove from deadline index with an existing lock guard.
    fn remove_from_deadline_index_locked(
        guard: &mut BTreeMap<u64, Vec<u64>>,
        deadline: Option<u64>,
        msg_id: u64,
    ) {
        if let Some(dl) = deadline {
            if let Some(ids) = guard.get_mut(&dl) {
                if let Some(pos) = ids.iter().position(|&id| id == msg_id) {
                    ids.swap_remove(pos);
                }
                if ids.is_empty() {
                    guard.remove(&dl);
                }
            }
        }
    }

    /// Remove a message from the consumer in-flight index.
    fn remove_from_consumer_index(&self, consumer_id: Option<u64>, msg_id: u64) {
        if let Some(cid) = consumer_id {
            if let Some(mut ids) = self.consumer_in_flight.get_mut(&cid) {
                if let Some(pos) = ids.iter().position(|&id| id == msg_id) {
                    ids.swap_remove(pos);
                }
                if ids.is_empty() {
                    drop(ids);
                    self.consumer_in_flight.remove(&cid);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flat::FlatMessageBuilder;

    fn make_queue(name: &str) -> QueueState {
        let meta = QueueMeta::new(1, name.to_string(), 1000, QueueConfig::default());
        QueueState::new(meta, "test")
    }

    fn make_queue_with_config(name: &str, config: QueueConfig) -> QueueState {
        let meta = QueueMeta::new(1, name.to_string(), 1000, config);
        QueueState::new(meta, "test")
    }

    /// Build a flat-encoded message with just a value and timestamp.
    fn make_msg(value: &[u8]) -> Bytes {
        FlatMessageBuilder::new(Bytes::from(value.to_vec()))
            .timestamp(1000)
            .build()
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
        let q = make_queue("test");
        let msgs = vec![make_msg(b"hello")];
        let count = q.apply_enqueue(10, &msgs, &[None], 1000);

        assert_eq!(count, 1);
        assert_eq!(q.pending_count(), 1);
        assert!(q.messages.contains_key(&10));
        assert_eq!(q.messages.get(&10).unwrap().state, MessageState::Pending);
        assert!(q.pending.lock().contains_key(&(0, 10)));
    }

    #[test]
    fn test_enqueue_with_dedup() {
        let config = QueueConfig {
            dedup_window_secs: Some(60),
            ..Default::default()
        };
        let q = make_queue_with_config("test", config);
        let key = Bytes::from_static(b"dedup-key");

        let count1 = q.apply_enqueue(10, &[make_msg(b"first")], &[Some(key.clone())], 1000);
        assert_eq!(count1, 1);
        assert_eq!(q.pending_count(), 1);

        // Duplicate within window — skipped
        let count2 = q.apply_enqueue(11, &[make_msg(b"second")], &[Some(key.clone())], 1005);
        assert_eq!(count2, 1);
        assert_eq!(q.pending_count(), 1); // no new message
    }

    #[test]
    fn test_enqueue_with_delay() {
        let config = QueueConfig {
            delay_default_ms: 5000,
            ..Default::default()
        };
        let q = make_queue_with_config("test", config);
        q.apply_enqueue(10, &[make_msg(b"delayed")], &[None], 1000);
        assert_eq!(q.messages.get(&10).unwrap().deliver_after, 6000);
    }

    #[test]
    fn test_deliver_basic() {
        let q = make_queue("test");
        q.apply_enqueue(10, &[make_msg(b"msg1")], &[None], 1000);
        q.apply_enqueue(11, &[make_msg(b"msg2")], &[None], 1000);

        let delivered = q.apply_deliver(100, 1, 2000, 20);
        assert_eq!(delivered.len(), 1);
        assert_eq!(q.pending_count(), 1);
        assert_eq!(q.in_flight_count(), 1);

        let msg = q.messages.get(&delivered[0]).unwrap();
        assert_eq!(msg.state, MessageState::InFlight);
        assert_eq!(msg.consumer_id, Some(100));
        assert_eq!(msg.attempts, 1);
        assert!(msg.visibility_deadline.is_some());
    }

    #[test]
    fn test_deliver_respects_max_count() {
        let q = make_queue("test");
        for i in 10..15 {
            q.apply_enqueue(i, &[make_msg(b"x")], &[None], 1000);
        }
        let delivered = q.apply_deliver(100, 3, 2000, 20);
        assert_eq!(delivered.len(), 3);
        assert_eq!(q.pending_count(), 2);
        assert_eq!(q.in_flight_count(), 3);
    }

    #[test]
    fn test_deliver_skips_delayed() {
        let config = QueueConfig {
            delay_default_ms: 5000,
            ..Default::default()
        };
        let q = make_queue_with_config("test", config);
        q.apply_enqueue(10, &[make_msg(b"delayed")], &[None], 1000);

        let delivered = q.apply_deliver(100, 10, 3000, 20);
        assert_eq!(delivered.len(), 0);

        let delivered = q.apply_deliver(100, 10, 7000, 21);
        assert_eq!(delivered.len(), 1);
    }

    #[test]
    fn test_ack() {
        let q = make_queue("test");
        q.apply_enqueue(10, &[make_msg(b"msg")], &[None], 1000);
        q.apply_deliver(100, 1, 2000, 20);

        q.apply_ack(&[10]);
        assert_eq!(q.in_flight_count(), 0);
        assert!(!q.messages.contains_key(&10));
    }

    #[test]
    fn test_nack() {
        let q = make_queue("test");
        q.apply_enqueue(10, &[make_msg(b"msg")], &[None], 1000);
        q.apply_deliver(100, 1, 2000, 20);

        q.apply_nack(&[10]);
        assert_eq!(q.in_flight_count(), 0);
        assert_eq!(q.pending_count(), 1);
        assert_eq!(q.messages.get(&10).unwrap().state, MessageState::Pending);
        assert!(q.messages.get(&10).unwrap().consumer_id.is_none());
        assert!(q.pending.lock().contains_key(&(0, 10)));
    }

    #[test]
    fn test_extend_visibility() {
        let q = make_queue("test");
        q.apply_enqueue(10, &[make_msg(b"msg")], &[None], 1000);
        q.apply_deliver(100, 1, 2000, 20);

        let orig_deadline = q.messages.get(&10).unwrap().visibility_deadline.unwrap();
        q.apply_extend_visibility(&[10], 5000);
        assert_eq!(
            q.messages.get(&10).unwrap().visibility_deadline.unwrap(),
            orig_deadline + 5000
        );
    }

    #[test]
    fn test_timeout_expired_requeue() {
        let q = make_queue("test");
        q.apply_enqueue(10, &[make_msg(b"msg")], &[None], 1000);
        q.apply_deliver(100, 1, 2000, 20);

        let dead_lettered = q.apply_timeout_expired(&[10], None, 32000);
        assert!(dead_lettered.is_empty());
        assert_eq!(q.pending_count(), 1);
        assert_eq!(q.in_flight_count(), 0);
        assert_eq!(q.messages.get(&10).unwrap().state, MessageState::Pending);
    }

    #[test]
    fn test_timeout_expired_dead_letter() {
        let config = QueueConfig {
            max_retries: 2,
            ..Default::default()
        };
        let q = make_queue_with_config("test", config);
        q.apply_enqueue(10, &[make_msg(b"msg")], &[None], 1000);

        q.apply_deliver(100, 1, 2000, 20);
        q.apply_timeout_expired(&[10], None, 32000); // attempt 1 → re-enqueue
        q.apply_deliver(100, 1, 3000, 30); // attempt 2
        let dead_lettered = q.apply_timeout_expired(&[10], None, 33000);

        assert_eq!(dead_lettered.as_slice(), &[10u64]);
        assert_eq!(q.messages.get(&10).unwrap().state, MessageState::DeadLetter);
        assert_eq!(q.dlq_count(), 1);
        assert_eq!(q.in_flight_count(), 0);
    }

    #[test]
    fn test_remove_dead_lettered_cleans_up() {
        let config = QueueConfig {
            max_retries: 1,
            ..Default::default()
        };
        let q = make_queue_with_config("test", config);
        q.apply_enqueue(10, &[make_msg(b"msg")], &[None], 1000);

        q.apply_deliver(100, 1, 2000, 20); // attempt 1
        let dead_lettered = q.apply_timeout_expired(&[10], None, 33000);

        assert_eq!(dead_lettered.as_slice(), &[10u64]);
        assert_eq!(q.dlq_count(), 1);
        assert!(q.messages.contains_key(&10));

        // Simulate PublishToDlq cleanup
        q.apply_remove_dead_lettered(&[10]);
        assert_eq!(q.dlq_count(), 0);
        assert!(!q.messages.contains_key(&10));
        assert!(q.min_required_index().is_none());
    }

    #[test]
    fn test_find_expired_messages() {
        let q = make_queue("test");
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
        let q = make_queue_with_config("test", config);
        {
            let mut dedup = q.dedup.lock();
            dedup.insert(Bytes::from_static(b"old"), 10);
            dedup.insert(Bytes::from_static(b"new"), 100);
        }

        q.apply_prune_dedup(50);
        let dedup = q.dedup.lock();
        assert!(!dedup.buckets.contains_key(&10));
        assert!(dedup.buckets.contains_key(&100));
    }

    #[test]
    fn test_min_required_index() {
        let q = make_queue("test");
        assert!(q.min_required_index().is_none());

        q.apply_enqueue(10, &[make_msg(b"a")], &[None], 1000);
        q.apply_enqueue(20, &[make_msg(b"b")], &[None], 1000);
        assert_eq!(q.min_required_index(), Some(10));

        q.apply_deliver(100, 1, 2000, 30);
        q.apply_ack(&[10]);
        assert_eq!(q.min_required_index(), Some(20));
    }
}

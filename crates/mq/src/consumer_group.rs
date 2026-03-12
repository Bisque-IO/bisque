//! Raft-replicated consumer group state with variant support.
//!
//! Three variants: Offset (Kafka), Ack (queue semantics), Actor (mailbox).
//! All group coordinator state is replicated through Raft.
//!
//! **Concurrency model**: single Raft-apply writer, concurrent readers.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::types::*;

// ─── Enums ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum AutoOffsetReset {
    Earliest = 0,
    Latest = 1,
    None = 2,
}

impl Default for AutoOffsetReset {
    fn default() -> Self {
        Self::Latest
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum GroupPhase {
    Empty = 0,
    PreparingRebalance = 1,
    CompletingRebalance = 2,
    Stable = 3,
    Dead = 4,
}

impl Default for GroupPhase {
    fn default() -> Self {
        Self::Empty
    }
}

impl GroupPhase {
    #[inline]
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Empty,
            1 => Self::PreparingRebalance,
            2 => Self::CompletingRebalance,
            3 => Self::Stable,
            4 => Self::Dead,
            _ => Self::Empty,
        }
    }
}

// ─── Persisted metadata (snapshot / MDBX) ────────────────────────────────────

/// Persisted consumer group metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupMeta {
    pub group_id: u64,
    pub name: String,
    #[serde(default)]
    pub name_hash: u64,
    pub created_at: u64,
    pub generation: i32,
    pub phase: GroupPhase,
    pub protocol_type: String,
    pub protocol_name: String,
    pub leader: Option<String>,
    pub auto_offset_reset: AutoOffsetReset,
    pub last_activity_at: u64,
    pub next_member_id: u64,
    pub members: Vec<GroupMemberMeta>,

    // -- Unified storage extensions --
    /// Consumer group variant (Offset / Ack / Actor). Immutable after creation.
    #[serde(default)]
    pub variant: GroupVariant,
    /// Source topic ID this group reads from.
    #[serde(default)]
    pub source_topic_id: u64,
    /// Variant-specific configuration.
    #[serde(default)]
    pub variant_config: VariantConfig,
}

/// Persisted member state within a consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMemberMeta {
    pub member_id: String,
    pub client_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    pub protocols: Vec<(String, Vec<u8>)>,
    #[serde(default)]
    pub assignment: Vec<u8>,
}

/// A committed offset for `(group, topic, partition)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupTopicPartitionOffset {
    pub topic_id: u64,
    pub partition_index: u32,
    pub committed_offset: u64,
    pub metadata: Option<String>,
    pub committed_at: u64,
}

/// Snapshot data for a consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupSnapshot {
    pub meta: ConsumerGroupMeta,
    pub offsets: Vec<GroupTopicPartitionOffset>,
    /// Ack variant state snapshot.
    #[serde(default)]
    pub ack_state: Option<AckStateSnapshot>,
    /// Actor variant state snapshot.
    #[serde(default)]
    pub actor_state: Option<ActorStateSnapshot>,
}

/// Snapshot of Ack variant state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckStateSnapshot {
    pub messages: Vec<AckMessageMeta>,
    pub dedup_entries: Vec<(u64, Vec<Bytes>)>,
    /// Delayed message entries: (delay_offset_ms, message_ids).
    #[serde(default)]
    pub delayed_entries: Vec<(u32, Vec<u64>)>,
}

/// Snapshot of Actor variant state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorStateSnapshot {
    pub actors: Vec<ActorSnapshot>,
}

/// Persisted per-actor state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorSnapshot {
    pub actor_id: Bytes,
    pub assigned_consumer_id: Option<u64>,
    pub pending_count: u32,
    #[serde(default)]
    pub pending_bytes: u64,
    #[serde(default)]
    pub in_flight_bytes: u64,
    pub head_index: u64,
    pub tail_index: u64,
    pub in_flight_index: Option<u64>,
    pub last_activity_at: u64,
    pub attempts: u32,
}

// ─── Ack Variant State ──────────────────────────────────────────────────────

/// Sentinel: cached min needs full recompute.
const MIN_DIRTY: u64 = u64::MAX;
/// Sentinel: computed result is None.
const MIN_NONE: u64 = u64::MAX - 1;
/// Sentinel for Option<u64> atomics: 0 means None.
const NONE_U64: u64 = 0;

#[inline]
fn opt_to_atomic(v: Option<u64>) -> u64 {
    v.unwrap_or(NONE_U64)
}

#[inline]
fn atomic_to_opt(v: u64) -> Option<u64> {
    if v == NONE_U64 { None } else { Some(v) }
}

/// In-memory Ack variant state (migrated from QueueState).
pub struct AckVariantState {
    pub messages: DashMap<u64, AckMessageMeta>,
    pub(crate) pending: Mutex<BTreeMap<(u8, u64), ()>>,
    pub(crate) in_flight_deadlines: Mutex<BTreeMap<u64, SmallVec<[u64; 4]>>>,
    pub(crate) consumer_in_flight: DashMap<u64, SmallVec<[u64; 8]>>,
    pub(crate) expires_at_deadlines: Mutex<BTreeMap<u64, SmallVec<[u64; 4]>>>,
    /// Tiered delay index: compact storage for delayed messages.
    /// Key = ms offset from server start epoch (u32), Value = message IDs.
    /// Delayed messages do NOT get full AckMessageMeta until materialized.
    pub(crate) delayed_index: Mutex<BTreeMap<u32, SmallVec<[u64; 1]>>>,
    /// Server start time in epoch ms — used to compute u32 delay offsets.
    pub(crate) server_start_ms: u64,
    // Atomic counters
    pub(crate) pending_count: AtomicU64,
    pub(crate) in_flight_count: AtomicU64,
    pub(crate) dlq_count: AtomicU64,
    pub(crate) delayed_count: AtomicU64,
    pub(crate) delayed_bytes: AtomicU64,
    pub(crate) pending_bytes: AtomicU64,
    pub(crate) in_flight_bytes: AtomicU64,
    pub(crate) total_messages: AtomicU64,
    pub(crate) total_bytes: AtomicU64,
    /// Cached minimum message ID (for purge floor).
    pub(crate) cached_min_required: AtomicU64,
    // Pre-initialized metrics
    m_enqueue_count: metrics::Counter,
    m_deliver_count: metrics::Counter,
    m_ack_count: metrics::Counter,
    m_dlq_count: metrics::Counter,
}

impl AckVariantState {
    pub fn new(catalog_name: &str, group_name: &str, server_start_ms: u64) -> Self {
        let labels = [
            ("catalog", catalog_name.to_owned()),
            ("group", group_name.to_owned()),
        ];
        Self {
            messages: DashMap::new(),
            pending: Mutex::new(BTreeMap::new()),
            in_flight_deadlines: Mutex::new(BTreeMap::new()),
            consumer_in_flight: DashMap::new(),
            expires_at_deadlines: Mutex::new(BTreeMap::new()),
            delayed_index: Mutex::new(BTreeMap::new()),
            server_start_ms,
            pending_count: AtomicU64::new(0),
            in_flight_count: AtomicU64::new(0),
            dlq_count: AtomicU64::new(0),
            delayed_count: AtomicU64::new(0),
            delayed_bytes: AtomicU64::new(0),
            pending_bytes: AtomicU64::new(0),
            in_flight_bytes: AtomicU64::new(0),
            total_messages: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            cached_min_required: AtomicU64::new(MIN_NONE),
            m_enqueue_count: metrics::counter!("mq.ack.enqueue.count", &labels),
            m_deliver_count: metrics::counter!("mq.ack.deliver.count", &labels),
            m_ack_count: metrics::counter!("mq.ack.ack.count", &labels),
            m_dlq_count: metrics::counter!("mq.ack.dlq.count", &labels),
        }
    }

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
    pub fn delayed_count(&self) -> u64 {
        self.delayed_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn delayed_bytes(&self) -> u64 {
        self.delayed_bytes.load(Ordering::Relaxed)
    }

    pub fn has_messages(&self) -> bool {
        !self.messages.is_empty() || self.delayed_count() > 0
    }

    /// Convert absolute timestamp to u32 offset from server start.
    #[inline]
    fn delay_offset(&self, abs_ms: u64) -> u32 {
        abs_ms.saturating_sub(self.server_start_ms) as u32
    }

    /// Enqueue a message into the Ack group.
    ///
    /// Messages with a delay are stored in the compact `delayed_index`
    /// (`BTreeMap<u32, SmallVec<[u64; 1]>>`) without allocating a full
    /// `AckMessageMeta`. They are materialized into the pending index when
    /// their delay expires via `collect_due_delayed` + `materialize_delayed`.
    pub fn apply_enqueue(
        &self,
        config: &AckVariantConfig,
        group_id: u64,
        log_index: u64,
        messages: &[Bytes],
        dedup_keys: &[Option<Bytes>],
        current_time: u64,
    ) -> u64 {
        let mut count = 0u64;
        let mut delayed_batch: SmallVec<[(u32, u64); 4]> = SmallVec::new();

        for (i, msg) in messages.iter().enumerate() {
            let value_len = crate::flat::FlatMessageMeta::value_len(msg).unwrap_or(0);
            let message_id = log_index + i as u64;

            let deliver_after = if config.delay_default_ms > 0 {
                current_time + config.delay_default_ms
            } else {
                0
            };

            // Always count towards totals (even delayed)
            self.total_messages.fetch_add(1, Ordering::Relaxed);
            self.total_bytes
                .fetch_add(value_len as u64, Ordering::Relaxed);

            // Update cached min
            let cached = self.cached_min_required.load(Ordering::Relaxed);
            if cached == MIN_NONE {
                self.cached_min_required
                    .store(message_id, Ordering::Relaxed);
            } else if cached != MIN_DIRTY && message_id < cached {
                self.cached_min_required
                    .store(message_id, Ordering::Relaxed);
            }

            // Delayed messages go to the compact delay index
            if deliver_after > current_time {
                let offset = self.delay_offset(deliver_after);
                delayed_batch.push((offset, message_id));
                self.delayed_count.fetch_add(1, Ordering::Relaxed);
                self.delayed_bytes
                    .fetch_add(value_len as u64, Ordering::Relaxed);
                count += 1;
                continue;
            }

            // Immediate messages get full AckMessageMeta
            let priority = crate::flat::FlatMessageMeta::priority(msg).unwrap_or(0);
            let (reply_to, correlation_id) = {
                let meta = crate::flat::FlatMessageMeta::parse(msg);
                let has_reply_to = meta.as_ref().map_or(false, |m| m.has_reply_to());
                let has_corr = meta.as_ref().map_or(false, |m| m.has_correlation_id());
                let rt = if has_reply_to {
                    crate::flat::FlatMessage::new(msg.clone()).and_then(|f| f.reply_to())
                } else {
                    None
                };
                let ci = if has_corr {
                    crate::flat::FlatMessage::new(msg.clone()).and_then(|f| f.correlation_id())
                } else {
                    None
                };
                (rt, ci)
            };

            let ttl_ms = crate::flat::FlatMessageMeta::ttl_ms(msg).unwrap_or(0);
            let expires_at = if ttl_ms > 0 {
                Some(current_time + ttl_ms)
            } else {
                None
            };

            let dedup_key = dedup_keys.get(i).and_then(|k| k.clone());

            let meta = AckMessageMeta {
                message_id,
                group_id,
                state: MessageState::Pending,
                priority,
                deliver_after: 0,
                attempts: 0,
                last_delivered_at: None,
                consumer_id: None,
                visibility_deadline: None,
                dedup_key,
                expires_at,
                reply_to,
                correlation_id,
                value_len,
            };

            self.messages.insert(message_id, meta);
            self.pending.lock().insert((priority, message_id), ());
            self.pending_count.fetch_add(1, Ordering::Relaxed);
            self.pending_bytes
                .fetch_add(value_len as u64, Ordering::Relaxed);

            if let Some(ref ea) = expires_at {
                self.expires_at_deadlines
                    .lock()
                    .entry(*ea)
                    .or_default()
                    .push(message_id);
            }

            count += 1;
        }

        // Batch-insert delayed entries
        if !delayed_batch.is_empty() {
            let mut delayed = self.delayed_index.lock();
            for (offset, msg_id) in delayed_batch {
                delayed.entry(offset).or_default().push(msg_id);
            }
        }

        self.m_enqueue_count.increment(count);
        count
    }

    /// Deliver messages to a consumer.
    pub fn apply_deliver(
        &self,
        config: &AckVariantConfig,
        consumer_id: u64,
        max_count: u32,
        current_time: u64,
        _log_index: u64,
    ) -> SmallVec<[u64; 8]> {
        let mut delivered = SmallVec::new();
        let mut pending = self.pending.lock();

        let mut keys_to_remove: SmallVec<[(u8, u64); 8]> = SmallVec::new();
        for (&(prio, msg_id), _) in pending.iter().rev() {
            if delivered.len() >= max_count as usize {
                break;
            }
            if let Some(mut msg) = self.messages.get_mut(&msg_id) {
                if msg.state != MessageState::Pending {
                    continue;
                }
                if msg.deliver_after > current_time {
                    continue;
                }
                msg.state = MessageState::InFlight;
                msg.consumer_id = Some(consumer_id);
                msg.attempts += 1;
                msg.last_delivered_at = Some(current_time);
                let deadline = current_time + config.visibility_timeout_ms;
                msg.visibility_deadline = Some(deadline);

                delivered.push(msg_id);
                keys_to_remove.push((prio, msg_id));

                // Track in-flight deadline
                self.in_flight_deadlines
                    .lock()
                    .entry(deadline)
                    .or_default()
                    .push(msg_id);
            }
        }

        for key in &keys_to_remove {
            pending.remove(key);
        }
        drop(pending);

        let count = delivered.len() as u64;
        if count > 0 {
            self.pending_count.fetch_sub(count, Ordering::Relaxed);
            self.in_flight_count.fetch_add(count, Ordering::Relaxed);

            // Track per-consumer in-flight
            self.consumer_in_flight
                .entry(consumer_id)
                .or_default()
                .extend(delivered.iter().copied());
        }

        self.m_deliver_count.increment(count);
        delivered
    }

    /// ACK messages.
    pub fn apply_ack(&self, message_ids: &[u64]) {
        for &msg_id in message_ids {
            if let Some((_, meta)) = self.messages.remove(&msg_id) {
                if meta.state == MessageState::InFlight {
                    self.in_flight_count.fetch_sub(1, Ordering::Relaxed);
                    self.in_flight_bytes
                        .fetch_sub(meta.value_len as u64, Ordering::Relaxed);
                    // Remove from consumer in-flight tracking
                    if let Some(cid) = meta.consumer_id {
                        if let Some(mut ids) = self.consumer_in_flight.get_mut(&cid) {
                            ids.retain(|id| *id != msg_id);
                        }
                    }
                }
                // Invalidate cached min if needed
                let cached = self.cached_min_required.load(Ordering::Relaxed);
                if cached != MIN_DIRTY && cached != MIN_NONE && msg_id <= cached {
                    self.cached_min_required.store(MIN_DIRTY, Ordering::Relaxed);
                }
            }
        }
        self.m_ack_count.increment(message_ids.len() as u64);
    }

    /// NACK messages — return to pending with attempt increment.
    pub fn apply_nack(&self, message_ids: &[u64]) {
        for &msg_id in message_ids {
            if let Some(mut msg) = self.messages.get_mut(&msg_id) {
                if msg.state == MessageState::InFlight {
                    msg.state = MessageState::Pending;
                    msg.consumer_id = None;
                    msg.visibility_deadline = None;
                    self.pending.lock().insert((msg.priority, msg_id), ());
                    self.pending_count.fetch_add(1, Ordering::Relaxed);
                    self.in_flight_count.fetch_sub(1, Ordering::Relaxed);
                    // Remove from consumer in-flight
                    if let Some(cid) = msg.consumer_id {
                        if let Some(mut ids) = self.consumer_in_flight.get_mut(&cid) {
                            ids.retain(|id| *id != msg_id);
                        }
                    }
                }
            }
        }
    }

    /// RELEASE messages — return to pending WITHOUT attempt increment.
    pub fn apply_release(&self, message_ids: &[u64]) {
        for &msg_id in message_ids {
            if let Some(mut msg) = self.messages.get_mut(&msg_id) {
                if msg.state == MessageState::InFlight {
                    msg.state = MessageState::Pending;
                    let cid = msg.consumer_id;
                    msg.consumer_id = None;
                    msg.visibility_deadline = None;
                    // Decrement attempt count (release = no attempt charge)
                    msg.attempts = msg.attempts.saturating_sub(1);
                    self.pending.lock().insert((msg.priority, msg_id), ());
                    self.pending_count.fetch_add(1, Ordering::Relaxed);
                    self.in_flight_count.fetch_sub(1, Ordering::Relaxed);
                    if let Some(cid) = cid {
                        if let Some(mut ids) = self.consumer_in_flight.get_mut(&cid) {
                            ids.retain(|id| *id != msg_id);
                        }
                    }
                }
            }
        }
    }

    /// Extend visibility timeout for in-flight messages.
    pub fn apply_extend_visibility(&self, message_ids: &[u64], extension_ms: u64) {
        for &msg_id in message_ids {
            if let Some(mut msg) = self.messages.get_mut(&msg_id) {
                if msg.state == MessageState::InFlight {
                    if let Some(ref mut deadline) = msg.visibility_deadline {
                        *deadline += extension_ms;
                    }
                }
            }
        }
    }

    /// Handle visibility timeout expiry.
    pub fn apply_timeout_expired(
        &self,
        message_ids: &[u64],
        config: &AckVariantConfig,
        _current_time: u64,
    ) -> SmallVec<[u64; 8]> {
        let mut dead_lettered = SmallVec::new();
        for &msg_id in message_ids {
            if let Some(mut msg) = self.messages.get_mut(&msg_id) {
                if msg.state != MessageState::InFlight {
                    continue;
                }
                if msg.attempts >= config.max_retries {
                    msg.state = MessageState::DeadLetter;
                    self.in_flight_count.fetch_sub(1, Ordering::Relaxed);
                    self.dlq_count.fetch_add(1, Ordering::Relaxed);
                    dead_lettered.push(msg_id);
                    self.m_dlq_count.increment(1);
                } else {
                    // Return to pending
                    msg.state = MessageState::Pending;
                    let cid = msg.consumer_id;
                    msg.consumer_id = None;
                    msg.visibility_deadline = None;
                    self.pending.lock().insert((msg.priority, msg_id), ());
                    self.pending_count.fetch_add(1, Ordering::Relaxed);
                    self.in_flight_count.fetch_sub(1, Ordering::Relaxed);
                    if let Some(cid) = cid {
                        if let Some(mut ids) = self.consumer_in_flight.get_mut(&cid) {
                            ids.retain(|id| *id != msg_id);
                        }
                    }
                }
            }
        }
        dead_lettered
    }

    /// Get in-flight message IDs for a consumer (for disconnect handling).
    pub fn consumer_in_flight_ids(&self, consumer_id: u64) -> SmallVec<[u64; 8]> {
        self.consumer_in_flight
            .get(&consumer_id)
            .map(|ids| ids.clone())
            .unwrap_or_default()
    }

    /// Remove dead-lettered messages after DLQ publish.
    pub fn apply_remove_dead_lettered(&self, message_ids: &SmallVec<[u64; 8]>) {
        for &msg_id in message_ids {
            if let Some((_, _meta)) = self.messages.remove(&msg_id) {
                self.dlq_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
        self.cached_min_required.store(MIN_DIRTY, Ordering::Relaxed);
    }

    /// Expire pending messages that have exceeded their TTL.
    pub fn apply_expire_pending(&self, message_ids: &[u64]) {
        for &msg_id in message_ids {
            if let Some((_, _meta)) = self.messages.remove(&msg_id) {
                self.pending.lock().remove(&(0, msg_id));
                self.pending_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
        self.cached_min_required.store(MIN_DIRTY, Ordering::Relaxed);
    }

    /// Purge all messages.
    pub fn purge(&self) -> bool {
        let had_messages = self.has_messages();
        self.messages.clear();
        self.pending.lock().clear();
        self.in_flight_deadlines.lock().clear();
        self.consumer_in_flight.clear();
        self.expires_at_deadlines.lock().clear();
        self.delayed_index.lock().clear();
        self.pending_count.store(0, Ordering::Relaxed);
        self.in_flight_count.store(0, Ordering::Relaxed);
        self.dlq_count.store(0, Ordering::Relaxed);
        self.delayed_count.store(0, Ordering::Relaxed);
        self.delayed_bytes.store(0, Ordering::Relaxed);
        self.pending_bytes.store(0, Ordering::Relaxed);
        self.in_flight_bytes.store(0, Ordering::Relaxed);
        self.cached_min_required.store(MIN_NONE, Ordering::Relaxed);
        had_messages
    }

    /// Minimum required index for purge floor.
    pub fn min_required_index(&self) -> Option<u64> {
        let cached = self.cached_min_required.load(Ordering::Relaxed);
        if cached == MIN_NONE {
            return None;
        }
        if cached != MIN_DIRTY {
            return Some(cached);
        }
        // Recompute
        let min = self.messages.iter().map(|e| e.message_id).min();
        match min {
            Some(v) => {
                self.cached_min_required.store(v, Ordering::Relaxed);
                Some(v)
            }
            None => {
                self.cached_min_required.store(MIN_NONE, Ordering::Relaxed);
                None
            }
        }
    }

    /// Collect delayed message IDs whose delay has expired.
    ///
    /// Returns message IDs that need materialization. The caller (engine tick)
    /// reads the flat message bytes from the raft log and calls
    /// `materialize_delayed` for each.
    pub fn collect_due_delayed(&self, current_time: u64) -> SmallVec<[u64; 8]> {
        let now_offset = self.delay_offset(current_time);
        let mut delayed = self.delayed_index.lock();
        // split_off returns entries >= now_offset+1; we keep those.
        let remaining = delayed.split_off(&(now_offset + 1));
        let due = std::mem::replace(&mut *delayed, remaining);
        drop(delayed);

        let mut ids = SmallVec::new();
        for (_offset, msg_ids) in due {
            for msg_id in msg_ids {
                ids.push(msg_id);
            }
        }
        ids
    }

    /// Materialize a delayed message into the pending index.
    ///
    /// Called by the engine after reading the flat message bytes from the raft
    /// log for a message ID returned by `collect_due_delayed`.
    pub fn materialize_delayed(
        &self,
        group_id: u64,
        message_id: u64,
        msg: &Bytes,
        current_time: u64,
    ) {
        let value_len = crate::flat::FlatMessageMeta::value_len(msg).unwrap_or(0);
        let priority = crate::flat::FlatMessageMeta::priority(msg).unwrap_or(0);

        let (reply_to, correlation_id) = {
            let meta = crate::flat::FlatMessageMeta::parse(msg);
            let has_reply_to = meta.as_ref().map_or(false, |m| m.has_reply_to());
            let has_corr = meta.as_ref().map_or(false, |m| m.has_correlation_id());
            let rt = if has_reply_to {
                crate::flat::FlatMessage::new(msg.clone()).and_then(|f| f.reply_to())
            } else {
                None
            };
            let ci = if has_corr {
                crate::flat::FlatMessage::new(msg.clone()).and_then(|f| f.correlation_id())
            } else {
                None
            };
            (rt, ci)
        };

        let ttl_ms = crate::flat::FlatMessageMeta::ttl_ms(msg).unwrap_or(0);
        let expires_at = if ttl_ms > 0 {
            Some(current_time + ttl_ms)
        } else {
            None
        };

        let meta = AckMessageMeta {
            message_id,
            group_id,
            state: MessageState::Pending,
            priority,
            deliver_after: 0,
            attempts: 0,
            last_delivered_at: None,
            consumer_id: None,
            visibility_deadline: None,
            dedup_key: None,
            expires_at,
            reply_to,
            correlation_id,
            value_len,
        };

        self.messages.insert(message_id, meta);
        self.pending.lock().insert((priority, message_id), ());
        self.pending_count.fetch_add(1, Ordering::Relaxed);
        self.pending_bytes
            .fetch_add(value_len as u64, Ordering::Relaxed);
        self.delayed_count.fetch_sub(1, Ordering::Relaxed);
        self.delayed_bytes.fetch_sub(
            (value_len as u64).min(self.delayed_bytes()),
            Ordering::Relaxed,
        );

        if let Some(ref ea) = expires_at {
            self.expires_at_deadlines
                .lock()
                .entry(*ea)
                .or_default()
                .push(message_id);
        }
    }

    /// Check if accepting additional messages would exceed back-pressure limits.
    ///
    /// Returns `true` if the limit would be exceeded (caller should reject).
    pub fn would_exceed_limit(
        &self,
        config: &AckVariantConfig,
        topic_total_messages: u64,
        topic_total_bytes: u64,
        additional_messages: u64,
        additional_bytes: u64,
    ) -> bool {
        // Check pending + in-flight + delayed message limit
        if let Some(max_msgs) = config.max_pending_messages {
            let unprocessed = self.pending_count()
                + self.in_flight_count()
                + self.delayed_count()
                + additional_messages;
            if unprocessed > max_msgs {
                return true;
            }
        }
        // Check pending + in-flight byte limit
        if let Some(max_bytes) = config.max_pending_bytes {
            let unprocessed = self.pending_bytes.load(Ordering::Relaxed)
                + self.in_flight_bytes.load(Ordering::Relaxed)
                + self.delayed_bytes()
                + additional_bytes;
            if unprocessed > max_bytes {
                return true;
            }
        }
        // Check delayed-specific message limit
        if let Some(max_delayed) = config.max_delayed_messages {
            if self.delayed_count() + additional_messages > max_delayed {
                return true;
            }
        }
        // Check delayed-specific byte limit
        if let Some(max_delayed_bytes) = config.max_delayed_bytes {
            if self.delayed_bytes() + additional_bytes > max_delayed_bytes {
                return true;
            }
        }
        false
    }

    /// Build snapshot of Ack state.
    pub fn snapshot(&self) -> AckStateSnapshot {
        let messages: Vec<AckMessageMeta> =
            self.messages.iter().map(|e| e.value().clone()).collect();
        let delayed_entries: Vec<(u32, Vec<u64>)> = self
            .delayed_index
            .lock()
            .iter()
            .map(|(&k, v)| (k, v.to_vec()))
            .collect();
        AckStateSnapshot {
            messages,
            dedup_entries: Vec::new(), // dedup is now topic-level
            delayed_entries,
        }
    }

    /// Restore from snapshot.
    pub fn restore(&self, snap: AckStateSnapshot, config: &AckVariantConfig) {
        for msg in snap.messages {
            let msg_id = msg.message_id;
            let prio = msg.priority;
            let state = msg.state;
            let consumer_id = msg.consumer_id;
            let value_len = msg.value_len as u64;

            self.messages.insert(msg_id, msg);

            match state {
                MessageState::Pending => {
                    self.pending.lock().insert((prio, msg_id), ());
                    self.pending_count.fetch_add(1, Ordering::Relaxed);
                    self.pending_bytes.fetch_add(value_len, Ordering::Relaxed);
                }
                MessageState::InFlight => {
                    self.in_flight_count.fetch_add(1, Ordering::Relaxed);
                    self.in_flight_bytes.fetch_add(value_len, Ordering::Relaxed);
                    if let Some(cid) = consumer_id {
                        self.consumer_in_flight.entry(cid).or_default().push(msg_id);
                    }
                }
                MessageState::DeadLetter => {
                    self.dlq_count.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
            self.total_messages.fetch_add(1, Ordering::Relaxed);
            self.total_bytes.fetch_add(value_len, Ordering::Relaxed);
        }

        // Restore delayed index
        if !snap.delayed_entries.is_empty() {
            let mut delayed = self.delayed_index.lock();
            for (offset, ids) in snap.delayed_entries {
                let count = ids.len() as u64;
                delayed.entry(offset).or_default().extend(ids);
                self.delayed_count.fetch_add(count, Ordering::Relaxed);
                self.total_messages.fetch_add(count, Ordering::Relaxed);
            }
        }

        self.cached_min_required.store(MIN_DIRTY, Ordering::Relaxed);
    }
}

// ─── Actor Variant State ─────────────────────────────────────────────────────

/// Lock-free per-actor instance.
pub struct ActorInMemory {
    pub group_id: u64,
    pub actor_id: Bytes,
    pub(crate) assigned_consumer_id: AtomicU64,
    pub(crate) pending_count: AtomicU32,
    pub(crate) pending_bytes: AtomicU64,
    pub(crate) in_flight_bytes: AtomicU64,
    pub(crate) head_index: AtomicU64,
    pub(crate) tail_index: AtomicU64,
    pub(crate) in_flight_index: AtomicU64,
    pub(crate) last_activity_at: AtomicU64,
    pub(crate) attempts: AtomicU32,
    pub(crate) mailbox: Mutex<VecDeque<u64>>,
    pub reply_to_map: DashMap<u64, Bytes>,
}

impl ActorInMemory {
    pub fn new(group_id: u64, actor_id: Bytes, current_time: u64) -> Self {
        Self {
            group_id,
            actor_id,
            assigned_consumer_id: AtomicU64::new(NONE_U64),
            pending_count: AtomicU32::new(0),
            pending_bytes: AtomicU64::new(0),
            in_flight_bytes: AtomicU64::new(0),
            head_index: AtomicU64::new(0),
            tail_index: AtomicU64::new(0),
            in_flight_index: AtomicU64::new(NONE_U64),
            last_activity_at: AtomicU64::new(current_time),
            attempts: AtomicU32::new(0),
            mailbox: Mutex::new(VecDeque::new()),
            reply_to_map: DashMap::new(),
        }
    }

    pub fn from_snapshot(group_id: u64, snap: ActorSnapshot) -> Self {
        Self {
            group_id,
            actor_id: snap.actor_id,
            assigned_consumer_id: AtomicU64::new(opt_to_atomic(snap.assigned_consumer_id)),
            pending_count: AtomicU32::new(snap.pending_count),
            pending_bytes: AtomicU64::new(snap.pending_bytes),
            in_flight_bytes: AtomicU64::new(snap.in_flight_bytes),
            head_index: AtomicU64::new(snap.head_index),
            tail_index: AtomicU64::new(snap.tail_index),
            in_flight_index: AtomicU64::new(opt_to_atomic(snap.in_flight_index)),
            last_activity_at: AtomicU64::new(snap.last_activity_at),
            attempts: AtomicU32::new(snap.attempts),
            mailbox: Mutex::new(VecDeque::new()),
            reply_to_map: DashMap::new(),
        }
    }

    #[inline]
    pub fn assigned_consumer_id(&self) -> Option<u64> {
        atomic_to_opt(self.assigned_consumer_id.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn pending_count(&self) -> u32 {
        self.pending_count.load(Ordering::Relaxed)
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
    pub fn in_flight_index(&self) -> Option<u64> {
        atomic_to_opt(self.in_flight_index.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn last_activity_at(&self) -> u64 {
        self.last_activity_at.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn attempts(&self) -> u32 {
        self.attempts.load(Ordering::Relaxed)
    }

    pub fn snapshot_state(&self) -> ActorSnapshot {
        ActorSnapshot {
            actor_id: self.actor_id.clone(),
            assigned_consumer_id: self.assigned_consumer_id(),
            pending_count: self.pending_count(),
            pending_bytes: self.pending_bytes(),
            in_flight_bytes: self.in_flight_bytes(),
            head_index: self.head_index.load(Ordering::Relaxed),
            tail_index: self.tail_index.load(Ordering::Relaxed),
            in_flight_index: self.in_flight_index(),
            last_activity_at: self.last_activity_at(),
            attempts: self.attempts(),
        }
    }
}

/// In-memory Actor variant state (migrated from ActorNamespaceState).
pub struct ActorVariantState {
    pub actors: DashMap<Bytes, ActorInMemory>,
    pub consumer_assignments: DashMap<u64, HashSet<Bytes>>,
    active_count: AtomicU64,
    cached_min_required: AtomicU64,
    // Pre-initialized metrics
    m_send_count: metrics::Counter,
    m_deliver_count: metrics::Counter,
    m_ack_count: metrics::Counter,
    m_evict_count: metrics::Counter,
}

impl ActorVariantState {
    pub fn new(catalog_name: &str, group_name: &str) -> Self {
        let labels = [
            ("catalog", catalog_name.to_owned()),
            ("group", group_name.to_owned()),
        ];
        Self {
            actors: DashMap::new(),
            consumer_assignments: DashMap::new(),
            active_count: AtomicU64::new(0),
            cached_min_required: AtomicU64::new(MIN_NONE),
            m_send_count: metrics::counter!("mq.actor.send.count", &labels),
            m_deliver_count: metrics::counter!("mq.actor.deliver.count", &labels),
            m_ack_count: metrics::counter!("mq.actor.ack.count", &labels),
            m_evict_count: metrics::counter!("mq.actor.evict.count", &labels),
        }
    }

    #[inline]
    pub fn active_count(&self) -> u64 {
        self.active_count.load(Ordering::Relaxed)
    }

    pub fn apply_send(
        &self,
        config: &ActorVariantConfig,
        group_id: u64,
        actor_id: &Bytes,
        log_index: u64,
        current_time: u64,
        reply_to: Option<Bytes>,
        value_len: u32,
    ) -> Result<(), MqError> {
        let max_depth = config.max_mailbox_depth;
        let actor = self.actors.entry(actor_id.clone()).or_insert_with(|| {
            self.active_count.fetch_add(1, Ordering::Relaxed);
            ActorInMemory::new(group_id, actor_id.clone(), current_time)
        });

        let pending = actor.pending_count();
        if pending >= max_depth {
            return Err(MqError::MailboxFull { pending });
        }

        // Back-pressure: per-actor message count limit
        if let Some(max_msgs) = config.max_pending_messages {
            let total = actor.pending_count() as u64
                + if actor.in_flight_index().is_some() {
                    1
                } else {
                    0
                };
            if total + 1 > max_msgs {
                return Err(MqError::BackPressure { group_id });
            }
        }
        // Back-pressure: per-actor byte limit
        if let Some(max_bytes) = config.max_pending_bytes {
            let total = actor.pending_bytes() + actor.in_flight_bytes();
            if total + value_len as u64 > max_bytes {
                return Err(MqError::BackPressure { group_id });
            }
        }

        actor.mailbox.lock().push_back(log_index);
        if let Some(rt) = reply_to {
            actor.reply_to_map.insert(log_index, rt);
        }
        actor.pending_count.fetch_add(1, Ordering::Relaxed);
        actor
            .pending_bytes
            .fetch_add(value_len as u64, Ordering::Relaxed);
        actor.head_index.store(log_index, Ordering::Relaxed);
        if actor.tail_index.load(Ordering::Relaxed) == 0 {
            actor.tail_index.store(log_index, Ordering::Relaxed);
        }
        actor
            .last_activity_at
            .store(current_time, Ordering::Relaxed);

        // Update cached min
        let cached = self.cached_min_required.load(Ordering::Relaxed);
        if cached != MIN_DIRTY {
            let in_flight = actor.in_flight_index();
            let new_min = in_flight.unwrap_or(actor.tail_index.load(Ordering::Relaxed));
            if cached == MIN_NONE {
                self.cached_min_required.store(new_min, Ordering::Relaxed);
            } else {
                self.cached_min_required
                    .store(cached.min(new_min), Ordering::Relaxed);
            }
        }

        self.m_send_count.increment(1);
        Ok(())
    }

    pub fn apply_deliver(&self, actor_id: &Bytes, consumer_id: u64, value_len: u32) -> Option<u64> {
        let actor = self.actors.get(actor_id)?;
        if actor.assigned_consumer_id() != Some(consumer_id) {
            return None;
        }
        if actor.in_flight_index().is_some() {
            return None;
        }
        let msg_index = actor.mailbox.lock().pop_front()?;
        actor.in_flight_index.store(msg_index, Ordering::Relaxed);
        let prev = actor.pending_count.load(Ordering::Relaxed);
        actor
            .pending_count
            .store(prev.saturating_sub(1), Ordering::Relaxed);
        // Move bytes from pending to in-flight
        let bytes = value_len as u64;
        actor
            .pending_bytes
            .fetch_sub(bytes.min(actor.pending_bytes()), Ordering::Relaxed);
        actor.in_flight_bytes.fetch_add(bytes, Ordering::Relaxed);
        actor.attempts.fetch_add(1, Ordering::Relaxed);
        self.m_deliver_count.increment(1);
        Some(msg_index)
    }

    pub fn apply_ack(&self, actor_id: &Bytes, message_id: u64, value_len: u32) -> Option<Bytes> {
        if let Some(actor) = self.actors.get(actor_id) {
            if actor.in_flight_index() == Some(message_id) {
                actor.in_flight_index.store(NONE_U64, Ordering::Relaxed);
                actor.attempts.store(0, Ordering::Relaxed);
                let bytes = value_len as u64;
                actor
                    .in_flight_bytes
                    .fetch_sub(bytes.min(actor.in_flight_bytes()), Ordering::Relaxed);
                let reply_to = actor.reply_to_map.remove(&message_id).map(|(_, v)| v);
                if let Some(&next) = actor.mailbox.lock().front() {
                    actor.tail_index.store(next, Ordering::Relaxed);
                }
                let cached = self.cached_min_required.load(Ordering::Relaxed);
                if cached != MIN_DIRTY && cached != MIN_NONE && message_id <= cached {
                    self.cached_min_required.store(MIN_DIRTY, Ordering::Relaxed);
                }
                self.m_ack_count.increment(1);
                return reply_to;
            }
        }
        None
    }

    pub fn apply_nack(&self, actor_id: &Bytes, message_id: u64, value_len: u32) {
        if let Some(actor) = self.actors.get(actor_id) {
            if actor.in_flight_index() == Some(message_id) {
                actor.mailbox.lock().push_front(message_id);
                actor.in_flight_index.store(NONE_U64, Ordering::Relaxed);
                actor.pending_count.fetch_add(1, Ordering::Relaxed);
                let bytes = value_len as u64;
                actor
                    .in_flight_bytes
                    .fetch_sub(bytes.min(actor.in_flight_bytes()), Ordering::Relaxed);
                actor.pending_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    pub fn apply_assign(&self, consumer_id: u64, actor_ids: &[Bytes]) {
        let mut set = self.consumer_assignments.entry(consumer_id).or_default();
        for actor_id in actor_ids {
            if let Some(actor) = self.actors.get(actor_id) {
                actor
                    .assigned_consumer_id
                    .store(consumer_id, Ordering::Relaxed);
                set.insert(actor_id.clone());
            }
        }
    }

    pub fn apply_release(&self, consumer_id: u64) {
        if let Some((_, actor_ids)) = self.consumer_assignments.remove(&consumer_id) {
            for actor_id in &actor_ids {
                if let Some(actor) = self.actors.get(actor_id) {
                    actor
                        .assigned_consumer_id
                        .store(NONE_U64, Ordering::Relaxed);
                    if let Some(msg_id) = actor.in_flight_index() {
                        actor.in_flight_index.store(NONE_U64, Ordering::Relaxed);
                        actor.mailbox.lock().push_front(msg_id);
                        actor.pending_count.fetch_add(1, Ordering::Relaxed);
                        // Move in-flight bytes back to pending
                        let bytes = actor.in_flight_bytes();
                        actor.in_flight_bytes.store(0, Ordering::Relaxed);
                        actor.pending_bytes.fetch_add(bytes, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    pub fn apply_evict_idle(&self, before_timestamp: u64) -> usize {
        let before = self.actors.len();
        self.actors.retain(|_, actor| {
            let keep = actor.pending_count() > 0
                || actor.in_flight_index().is_some()
                || actor.last_activity_at() >= before_timestamp;
            if !keep {
                self.active_count.fetch_sub(1, Ordering::Relaxed);
            }
            keep
        });
        let count = before - self.actors.len();
        if count > 0 {
            self.cached_min_required.store(MIN_DIRTY, Ordering::Relaxed);
        }
        self.m_evict_count.increment(count as u64);
        count
    }

    pub fn unassigned_actors_with_messages(&self) -> Vec<Bytes> {
        self.actors
            .iter()
            .filter(|entry| {
                let a = entry.value();
                a.assigned_consumer_id().is_none()
                    && (a.pending_count() > 0 || a.in_flight_index().is_some())
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub fn min_required_index(&self) -> Option<u64> {
        let cached = self.cached_min_required.load(Ordering::Relaxed);
        if cached == MIN_NONE {
            return None;
        }
        if cached != MIN_DIRTY {
            return Some(cached);
        }
        let mut min: Option<u64> = None;
        for entry in self.actors.iter() {
            let actor = entry.value();
            if actor.pending_count() > 0 || actor.in_flight_index().is_some() {
                let actor_min = actor
                    .in_flight_index()
                    .unwrap_or(actor.tail_index.load(Ordering::Relaxed));
                min = Some(min.map_or(actor_min, |m: u64| m.min(actor_min)));
            }
        }
        match min {
            Some(v) => {
                self.cached_min_required.store(v, Ordering::Relaxed);
                Some(v)
            }
            None => {
                self.cached_min_required.store(MIN_NONE, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn snapshot(&self) -> ActorStateSnapshot {
        let actors: Vec<ActorSnapshot> = self
            .actors
            .iter()
            .map(|entry| entry.value().snapshot_state())
            .collect();
        ActorStateSnapshot { actors }
    }

    pub fn restore(&self, snap: ActorStateSnapshot, group_id: u64) {
        for actor_snap in snap.actors {
            self.actors.insert(
                actor_snap.actor_id.clone(),
                ActorInMemory::from_snapshot(group_id, actor_snap),
            );
            self.active_count.fetch_add(1, Ordering::Relaxed);
        }
        self.cached_min_required.store(MIN_DIRTY, Ordering::Relaxed);
    }
}

// ─── Variant State Enum ──────────────────────────────────────────────────────

pub enum VariantState {
    Offset,
    Ack(AckVariantState),
    Actor(ActorVariantState),
}

// ─── In-memory Consumer Group State ──────────────────────────────────────────

pub struct ConsumerGroupState {
    pub meta: ConsumerGroupMeta,

    // ── Atomic mutable scalars ──
    generation: AtomicU32,
    last_activity_at: AtomicU64,
    next_member_id: AtomicU64,
    phase: AtomicU8,

    // ── Lock-free complex state ──
    leader: ArcSwapOption<String>,
    protocol_name: ArcSwap<String>,
    members: DashMap<String, GroupMemberState>,
    pub(crate) offsets: DashMap<(u64, u32), GroupTopicPartitionOffset>,

    /// Variant-specific state.
    pub(crate) variant_state: VariantState,

    pub phase_notify: Arc<tokio::sync::Notify>,

    // ── Pre-initialized metrics ──
    m_offset_commits: metrics::Counter,
    m_generation_bumps: metrics::Counter,
    m_rebalances: metrics::Counter,
}

/// In-memory member state within a consumer group.
pub struct GroupMemberState {
    pub member_id: String,
    pub client_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    pub protocols: SmallVec<[(String, Bytes); 2]>,
    pub assignment: RwLock<Bytes>,
    pub last_heartbeat_at: AtomicU64,
    pub joined_this_gen: AtomicBool,
    pub synced_this_gen: AtomicBool,
}

impl ConsumerGroupState {
    pub fn new(meta: ConsumerGroupMeta, catalog_name: &str, server_start_ms: u64) -> Self {
        let catalog = catalog_name.to_owned();
        let name = &meta.name;

        let variant_state = match meta.variant {
            GroupVariant::Offset => VariantState::Offset,
            GroupVariant::Ack => {
                VariantState::Ack(AckVariantState::new(catalog_name, name, server_start_ms))
            }
            GroupVariant::Actor => VariantState::Actor(ActorVariantState::new(catalog_name, name)),
        };

        Self {
            generation: AtomicU32::new(meta.generation as u32),
            last_activity_at: AtomicU64::new(meta.last_activity_at),
            next_member_id: AtomicU64::new(meta.next_member_id),
            phase: AtomicU8::new(meta.phase as u8),
            leader: ArcSwapOption::new(meta.leader.clone().map(Arc::new)),
            protocol_name: ArcSwap::new(Arc::new(meta.protocol_name.clone())),
            members: DashMap::new(),
            offsets: DashMap::new(),
            variant_state,
            phase_notify: Arc::new(tokio::sync::Notify::new()),
            m_offset_commits: metrics::counter!("mq.consumer_group.offset_commits", "catalog" => catalog.clone(), "group" => name.clone()),
            m_generation_bumps: metrics::counter!("mq.consumer_group.generation_bumps", "catalog" => catalog.clone(), "group" => name.clone()),
            m_rebalances: metrics::counter!("mq.consumer_group.rebalances", "catalog" => catalog, "group" => name.clone()),
            meta,
        }
    }

    pub fn from_snapshot(
        mut meta: ConsumerGroupMeta,
        offsets: Vec<GroupTopicPartitionOffset>,
        catalog_name: &str,
        server_start_ms: u64,
    ) -> Self {
        let snapshot_members = std::mem::take(&mut meta.members);
        let last_activity = meta.last_activity_at;
        let state = Self::new(meta, catalog_name, server_start_ms);

        for m in snapshot_members {
            let protocols: SmallVec<[(String, Bytes); 2]> = m
                .protocols
                .into_iter()
                .map(|(n, d)| (n, Bytes::from(d)))
                .collect();
            let member_id = m.member_id;
            state.members.insert(
                member_id.clone(),
                GroupMemberState {
                    member_id,
                    client_id: m.client_id,
                    session_timeout_ms: m.session_timeout_ms,
                    rebalance_timeout_ms: m.rebalance_timeout_ms,
                    protocol_type: m.protocol_type,
                    protocols,
                    assignment: RwLock::new(Bytes::from(m.assignment)),
                    last_heartbeat_at: AtomicU64::new(last_activity),
                    joined_this_gen: AtomicBool::new(false),
                    synced_this_gen: AtomicBool::new(false),
                },
            );
        }

        for o in offsets {
            state.offsets.insert((o.topic_id, o.partition_index), o);
        }

        state
    }

    // ── Variant accessors ──

    pub fn ack_state(&self) -> Option<&AckVariantState> {
        match &self.variant_state {
            VariantState::Ack(s) => Some(s),
            _ => None,
        }
    }

    pub fn actor_state(&self) -> Option<&ActorVariantState> {
        match &self.variant_state {
            VariantState::Actor(s) => Some(s),
            _ => None,
        }
    }

    pub fn ack_config(&self) -> Option<&AckVariantConfig> {
        match &self.meta.variant_config {
            VariantConfig::Ack(c) => Some(c),
            _ => None,
        }
    }

    pub fn actor_config(&self) -> Option<&ActorVariantConfig> {
        match &self.meta.variant_config {
            VariantConfig::Actor(c) => Some(c),
            _ => None,
        }
    }

    // ── Readers ──

    #[inline]
    pub fn generation(&self) -> i32 {
        self.generation.load(Ordering::Relaxed) as i32
    }

    #[inline]
    pub fn phase(&self) -> GroupPhase {
        GroupPhase::from_u8(self.phase.load(Ordering::Acquire))
    }

    #[inline]
    pub fn last_activity_at(&self) -> u64 {
        self.last_activity_at.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn next_member_id(&self) -> u64 {
        self.next_member_id.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn leader(&self) -> Option<Arc<String>> {
        self.leader.load_full()
    }

    #[inline]
    pub fn protocol_name(&self) -> Arc<String> {
        self.protocol_name.load_full()
    }

    #[inline]
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    #[inline]
    pub fn has_member(&self, member_id: &str) -> bool {
        self.members.contains_key(member_id)
    }

    #[inline]
    pub fn auto_offset_reset(&self) -> AutoOffsetReset {
        self.meta.auto_offset_reset
    }

    #[inline]
    pub fn get_offset(&self, topic_id: u64, partition_index: u32) -> Option<u64> {
        self.offsets
            .get(&(topic_id, partition_index))
            .map(|o| o.committed_offset)
    }

    #[inline]
    pub fn get_member_assignment(&self, member_id: &str) -> Option<Bytes> {
        self.members
            .get(member_id)
            .map(|m| m.assignment.read().clone())
    }

    pub fn member_protocols(&self) -> Vec<(String, Bytes)> {
        let chosen = self.protocol_name.load();
        self.members
            .iter()
            .map(|entry| {
                let m = entry.value();
                let meta_bytes = m
                    .protocols
                    .iter()
                    .find(|(name, _)| name.as_str() == chosen.as_str())
                    .map(|(_, data)| data.clone())
                    .unwrap_or_default();
                (m.member_id.clone(), meta_bytes)
            })
            .collect()
    }

    pub fn find_expired_members(&self, now_ms: u64) -> SmallVec<[String; 4]> {
        self.members
            .iter()
            .filter(|entry| {
                let m = entry.value();
                let timeout = m.session_timeout_ms as u64;
                let last = m.last_heartbeat_at.load(Ordering::Relaxed);
                now_ms > last + timeout
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    #[inline]
    pub fn all_members_joined(&self) -> bool {
        if self.members.is_empty() {
            return false;
        }
        self.members
            .iter()
            .all(|e| e.value().joined_this_gen.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn all_members_synced(&self) -> bool {
        if self.members.is_empty() {
            return false;
        }
        self.members
            .iter()
            .all(|e| e.value().synced_this_gen.load(Ordering::Relaxed))
    }

    // ── Writers ──

    #[inline]
    pub fn set_phase(&self, phase: GroupPhase) {
        self.phase.store(phase as u8, Ordering::Release);
    }

    #[inline]
    pub fn touch_activity(&self, at: u64) {
        self.last_activity_at.store(at, Ordering::Relaxed);
    }

    #[inline]
    pub fn increment_next_member_id(&self) -> u64 {
        self.next_member_id.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    pub fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::Relaxed);
        self.m_generation_bumps.increment(1);
    }

    pub fn set_leader(&self, leader: Option<String>) {
        self.leader.store(leader.map(Arc::new));
    }

    #[inline]
    pub fn clear_leader(&self) {
        self.leader.store(None);
    }

    pub fn set_protocol_name(&self, name: String) {
        self.protocol_name.store(Arc::new(name));
    }

    #[inline]
    pub fn upsert_member(&self, member: GroupMemberState) {
        self.members.insert(member.member_id.clone(), member);
    }

    #[inline]
    pub fn remove_member(&self, member_id: &str) {
        self.members.remove(member_id);
    }

    #[inline]
    pub fn mark_joined(&self, member_id: &str) {
        if let Some(m) = self.members.get(member_id) {
            m.joined_this_gen.store(true, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn mark_synced(&self, member_id: &str) {
        if let Some(m) = self.members.get(member_id) {
            m.synced_this_gen.store(true, Ordering::Relaxed);
        }
    }

    pub fn clear_join_marks(&self) {
        for entry in self.members.iter() {
            entry
                .value()
                .joined_this_gen
                .store(false, Ordering::Relaxed);
        }
    }

    pub fn clear_sync_marks(&self) {
        for entry in self.members.iter() {
            entry
                .value()
                .synced_this_gen
                .store(false, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn set_member_assignment(&self, member_id: &str, assignment: Bytes) {
        if let Some(m) = self.members.get(member_id) {
            *m.assignment.write() = assignment;
        }
    }

    #[inline]
    pub fn update_member_heartbeat(&self, member_id: &str, at: u64) {
        if let Some(m) = self.members.get(member_id) {
            m.last_heartbeat_at.store(at, Ordering::Relaxed);
        }
    }

    pub fn select_and_set_protocol(&self) {
        let mut counts: SmallVec<[(String, usize); 8]> = SmallVec::new();
        for entry in self.members.iter() {
            for (name, _) in entry.value().protocols.iter() {
                if let Some(c) = counts.iter_mut().find(|(n, _)| n == name) {
                    c.1 += 1;
                } else {
                    counts.push((name.clone(), 1));
                }
            }
        }
        let member_count = self.members.len();
        if let Some((name, _)) = counts
            .iter()
            .filter(|(_, c)| *c == member_count)
            .max_by_key(|(_, c)| *c)
        {
            self.set_protocol_name(name.clone());
        } else if let Some((name, _)) = counts.iter().max_by_key(|(_, c)| *c) {
            self.set_protocol_name(name.clone());
        }
    }

    pub fn elect_leader(&self) {
        let leader = self
            .members
            .iter()
            .next()
            .map(|e| Arc::new(e.key().clone()));
        self.leader.store(leader);
    }

    #[inline]
    pub fn record_offset_commit(&self) {
        self.m_offset_commits.increment(1);
    }

    #[inline]
    pub fn record_rebalance(&self) {
        self.m_rebalances.increment(1);
    }

    // ── Snapshot ──

    pub fn snapshot_meta(&self) -> ConsumerGroupMeta {
        let members: Vec<GroupMemberMeta> = self
            .members
            .iter()
            .map(|entry| {
                let ms = entry.value();
                GroupMemberMeta {
                    member_id: ms.member_id.clone(),
                    client_id: ms.client_id.clone(),
                    session_timeout_ms: ms.session_timeout_ms,
                    rebalance_timeout_ms: ms.rebalance_timeout_ms,
                    protocol_type: ms.protocol_type.clone(),
                    protocols: ms
                        .protocols
                        .iter()
                        .map(|(n, d)| (n.clone(), d.to_vec()))
                        .collect(),
                    assignment: ms.assignment.read().to_vec(),
                }
            })
            .collect();

        ConsumerGroupMeta {
            group_id: self.meta.group_id,
            name: self.meta.name.clone(),
            name_hash: self.meta.name_hash,
            created_at: self.meta.created_at,
            generation: self.generation(),
            phase: self.phase(),
            protocol_type: self.meta.protocol_type.clone(),
            protocol_name: (*self.protocol_name()).clone(),
            leader: self.leader().map(|a| (*a).clone()),
            auto_offset_reset: self.meta.auto_offset_reset,
            last_activity_at: self.last_activity_at(),
            next_member_id: self.next_member_id(),
            members,
            variant: self.meta.variant,
            source_topic_id: self.meta.source_topic_id,
            variant_config: self.meta.variant_config.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_group(id: u64, name: &str) -> ConsumerGroupState {
        let meta = ConsumerGroupMeta {
            group_id: id,
            name: name.to_string(),
            name_hash: 0,
            created_at: 1000,
            generation: 0,
            phase: GroupPhase::Empty,
            protocol_type: "consumer".to_string(),
            protocol_name: String::new(),
            leader: None,
            auto_offset_reset: AutoOffsetReset::Latest,
            last_activity_at: 1000,
            next_member_id: 1,
            members: Vec::new(),
            variant: GroupVariant::Offset,
            source_topic_id: 0,
            variant_config: VariantConfig::Offset,
        };
        ConsumerGroupState::new(meta, "test", 0)
    }

    fn make_ack_group(id: u64, name: &str) -> ConsumerGroupState {
        let meta = ConsumerGroupMeta {
            group_id: id,
            name: name.to_string(),
            name_hash: 0,
            created_at: 1000,
            generation: 0,
            phase: GroupPhase::Empty,
            protocol_type: "consumer".to_string(),
            protocol_name: String::new(),
            leader: None,
            auto_offset_reset: AutoOffsetReset::Latest,
            last_activity_at: 1000,
            next_member_id: 1,
            members: Vec::new(),
            variant: GroupVariant::Ack,
            source_topic_id: 1,
            variant_config: VariantConfig::Ack(AckVariantConfig::default()),
        };
        ConsumerGroupState::new(meta, "test", 0)
    }

    fn make_member(id: &str, protocols: &[(&str, &[u8])]) -> GroupMemberState {
        GroupMemberState {
            member_id: id.to_string(),
            client_id: "test-client".to_string(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            protocol_type: "consumer".to_string(),
            protocols: protocols
                .iter()
                .map(|(n, d)| (n.to_string(), Bytes::from(d.to_vec())))
                .collect(),
            assignment: RwLock::new(Bytes::new()),
            last_heartbeat_at: AtomicU64::new(1000),
            joined_this_gen: AtomicBool::new(false),
            synced_this_gen: AtomicBool::new(false),
        }
    }

    #[test]
    fn test_new_group_defaults() {
        let group = make_group(1, "test-group");
        assert_eq!(group.generation(), 0);
        assert_eq!(group.phase(), GroupPhase::Empty);
        assert_eq!(group.member_count(), 0);
        assert!(group.leader().is_none());
    }

    #[test]
    fn test_ack_variant_enqueue_deliver_ack() {
        let group = make_ack_group(1, "ack-group");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        // Enqueue
        let msg = crate::flat::FlatMessageBuilder::new(Bytes::from_static(b"hello"))
            .timestamp(1000)
            .build();
        let count = ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);
        assert_eq!(count, 1);
        assert_eq!(ack.pending_count(), 1);

        // Deliver
        let delivered = ack.apply_deliver(&config, 42, 10, 2000, 200);
        assert_eq!(delivered.len(), 1);
        assert_eq!(ack.pending_count(), 0);
        assert_eq!(ack.in_flight_count(), 1);

        // Ack
        ack.apply_ack(&delivered);
        assert_eq!(ack.in_flight_count(), 0);
    }

    #[test]
    fn test_ack_variant_nack() {
        let group = make_ack_group(1, "ack-group");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        let msg = crate::flat::FlatMessageBuilder::new(Bytes::from_static(b"data"))
            .timestamp(1000)
            .build();
        ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);
        let delivered = ack.apply_deliver(&config, 42, 10, 2000, 200);
        ack.apply_nack(&delivered);
        assert_eq!(ack.pending_count(), 1);
        assert_eq!(ack.in_flight_count(), 0);
    }

    #[test]
    fn test_ack_variant_release() {
        let group = make_ack_group(1, "ack-group");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        let msg = crate::flat::FlatMessageBuilder::new(Bytes::from_static(b"data"))
            .timestamp(1000)
            .build();
        ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);
        let delivered = ack.apply_deliver(&config, 42, 10, 2000, 200);

        // Check attempts before release
        let meta = ack.messages.get(&delivered[0]).unwrap();
        assert_eq!(meta.attempts, 1);
        drop(meta);

        ack.apply_release(&delivered);
        assert_eq!(ack.pending_count(), 1);
        assert_eq!(ack.in_flight_count(), 0);

        // Release should not increment attempts
        let meta = ack.messages.get(&delivered[0]).unwrap();
        assert_eq!(meta.attempts, 0); // decremented back
    }

    #[test]
    fn test_phase_transitions() {
        let group = make_group(1, "g");
        group.set_phase(GroupPhase::Stable);
        assert_eq!(group.phase(), GroupPhase::Stable);
    }

    #[test]
    fn test_generation_bump() {
        let group = make_group(1, "g");
        group.bump_generation();
        assert_eq!(group.generation(), 1);
    }

    #[test]
    fn test_upsert_and_remove_member() {
        let group = make_group(1, "g");
        group.upsert_member(make_member("m-1", &[("range", b"")]));
        assert_eq!(group.member_count(), 1);
        group.remove_member("m-1");
        assert_eq!(group.member_count(), 0);
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let group = make_group(1, "test-group");
        group.upsert_member(make_member("m-1", &[("range", b"\x01\x02")]));
        group.bump_generation();
        group.set_phase(GroupPhase::Stable);
        group.set_leader(Some("m-1".to_string()));

        let snap_meta = group.snapshot_meta();
        assert_eq!(snap_meta.generation, 1);
        assert_eq!(snap_meta.phase, GroupPhase::Stable);
        assert_eq!(snap_meta.variant, GroupVariant::Offset);
    }

    #[test]
    fn test_serde_roundtrip() {
        let meta = ConsumerGroupMeta {
            group_id: 42,
            name: "my-group".to_string(),
            name_hash: 123456,
            created_at: 1000,
            generation: 3,
            phase: GroupPhase::Stable,
            protocol_type: "consumer".to_string(),
            protocol_name: "range".to_string(),
            leader: Some("m-1".to_string()),
            auto_offset_reset: AutoOffsetReset::Earliest,
            last_activity_at: 5000,
            next_member_id: 10,
            members: vec![],
            variant: GroupVariant::Ack,
            source_topic_id: 99,
            variant_config: VariantConfig::Ack(AckVariantConfig::default()),
        };

        let snap = ConsumerGroupSnapshot {
            meta,
            offsets: vec![GroupTopicPartitionOffset {
                topic_id: 10,
                partition_index: 0,
                committed_offset: 42,
                metadata: None,
                committed_at: 4000,
            }],
            ack_state: None,
            actor_state: None,
        };

        let bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();
        let (decoded, _): (ConsumerGroupSnapshot, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        assert_eq!(decoded.meta.group_id, 42);
        assert_eq!(decoded.meta.variant, GroupVariant::Ack);
        assert_eq!(decoded.meta.source_topic_id, 99);
    }
}

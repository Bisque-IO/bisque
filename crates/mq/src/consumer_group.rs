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
    #[serde(with = "opt_bytes_as_string")]
    pub metadata: Option<Bytes>,
    pub committed_at: u64,
}

/// Serde helper: serialize `Option<Bytes>` as `Option<String>` for snapshot compatibility.
mod opt_bytes_as_string {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(val: &Option<Bytes>, ser: S) -> Result<S::Ok, S::Error> {
        match val {
            Some(b) => {
                let s = std::str::from_utf8(b).unwrap_or("");
                Some(s).serialize(ser)
            }
            None => Option::<&str>::None.serialize(ser),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Option<Bytes>, D::Error> {
        let opt: Option<String> = Option::deserialize(de)?;
        Ok(opt.map(Bytes::from))
    }
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
    pub dedup_entries: Vec<(u128, u64)>,
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
    pub messages: papaya::HashMap<u64, AckMessageMeta>,
    pub(crate) pending: Mutex<BTreeMap<(u8, u64), ()>>,
    pub(crate) in_flight_deadlines: Mutex<BTreeMap<u64, SmallVec<[u64; 4]>>>,
    pub(crate) consumer_in_flight: papaya::HashMap<u64, HashSet<u64>>,
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
            messages: papaya::HashMap::new(),
            pending: Mutex::new(BTreeMap::new()),
            in_flight_deadlines: Mutex::new(BTreeMap::new()),
            consumer_in_flight: papaya::HashMap::new(),
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
        !self.messages.pin().is_empty() || self.delayed_count() > 0
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
        let count = messages.len() as u64;
        if count == 0 {
            return 0;
        }

        let has_delay = config.delay_default_ms > 0;
        let deliver_after = if has_delay {
            current_time + config.delay_default_ms
        } else {
            0
        };

        // Batch accumulators — collect work, then take locks once
        let mut delayed_batch: SmallVec<[(u32, u64); 4]> = SmallVec::new();
        let mut pending_batch: SmallVec<[(u8, u64); 16]> = SmallVec::new();
        let mut expires_batch: SmallVec<[(u64, u64); 4]> = SmallVec::new();
        let mut total_bytes_acc: u64 = 0;
        let mut pending_bytes_acc: u64 = 0;
        let mut delayed_bytes_acc: u64 = 0;
        let mut delayed_count: u64 = 0;

        for (i, msg) in messages.iter().enumerate() {
            let message_id = log_index + i as u64;

            // Parse header once — extract all fields from single read
            let is_envelope = crate::flat::is_mqtt_envelope(msg);
            let envelope_meta = if is_envelope {
                crate::flat::MqttEnvelopeMeta::parse(msg)
            } else {
                None
            };
            let flat_meta = if !is_envelope {
                crate::flat::FlatMessageMeta::parse(msg)
            } else {
                None
            };

            let value_len = if is_envelope {
                crate::flat::MqttEnvelopeMeta::value_len(msg).unwrap_or(0)
            } else {
                crate::flat::FlatMessageMeta::value_len(msg).unwrap_or(0)
            };
            total_bytes_acc += value_len as u64;

            // Delayed messages go to compact delay index
            if has_delay && deliver_after > current_time {
                let offset = self.delay_offset(deliver_after);
                delayed_batch.push((offset, message_id));
                delayed_count += 1;
                delayed_bytes_acc += value_len as u64;
                continue;
            }

            // Extract fields from the parsed header
            let (priority, ttl_ms, publisher_id, reply_to, correlation_id) =
                if let Some(ref em) = envelope_meta {
                    let env = crate::flat::MqttEnvelope::new(msg);
                    let reply = env
                        .as_ref()
                        .and_then(|e| e.reply_to())
                        .map(Bytes::copy_from_slice);
                    let corr = env
                        .as_ref()
                        .and_then(|e| e.correlation_id())
                        .map(Bytes::copy_from_slice);
                    let ttl = if em.flags & (1 << 2) != 0 {
                        em.ttl_ms
                    } else {
                        0
                    };
                    (0u8, ttl, em.publisher_id, reply, corr)
                } else if let Some(ref m) = flat_meta {
                    let flat = crate::flat::FlatMessage::new(msg);
                    let reply = if m.has_reply_to() {
                        flat.as_ref()
                            .and_then(|f| f.reply_to())
                            .map(Bytes::copy_from_slice)
                    } else {
                        None
                    };
                    let corr = if m.has_correlation_id() {
                        flat.as_ref()
                            .and_then(|f| f.correlation_id())
                            .map(Bytes::copy_from_slice)
                    } else {
                        None
                    };
                    (
                        m.priority,
                        m.ttl_ms_opt().unwrap_or(0),
                        m.publisher_id,
                        reply,
                        corr,
                    )
                } else {
                    (0, 0, 0, None, None)
                };

            let expires_at = if ttl_ms > 0 {
                Some(current_time + ttl_ms)
            } else {
                None
            };

            let dedup_key = dedup_keys.get(i).and_then(|k| k.clone());

            let ack_meta = AckMessageMeta {
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
                publisher_id,
            };

            self.messages.pin().insert(message_id, ack_meta);
            pending_batch.push((priority, message_id));
            pending_bytes_acc += value_len as u64;

            if let Some(ea) = expires_at {
                expires_batch.push((ea, message_id));
            }
        }

        // Batch: single pending lock acquisition for all messages
        if !pending_batch.is_empty() {
            let mut pending = self.pending.lock();
            for (prio, msg_id) in &pending_batch {
                pending.insert((*prio, *msg_id), ());
            }
        }

        // Batch: single expires_at lock acquisition
        if !expires_batch.is_empty() {
            let mut deadlines = self.expires_at_deadlines.lock();
            for (ea, msg_id) in expires_batch {
                deadlines.entry(ea).or_default().push(msg_id);
            }
        }

        // Batch: single delayed_index lock acquisition
        if !delayed_batch.is_empty() {
            let mut delayed = self.delayed_index.lock();
            for (offset, msg_id) in delayed_batch {
                delayed.entry(offset).or_default().push(msg_id);
            }
        }

        // Batch atomic updates — single fetch_add per counter instead of per-message
        self.total_messages.fetch_add(count, Ordering::Relaxed);
        self.total_bytes
            .fetch_add(total_bytes_acc, Ordering::Relaxed);
        if !pending_batch.is_empty() {
            self.pending_count
                .fetch_add(pending_batch.len() as u64, Ordering::Relaxed);
            self.pending_bytes
                .fetch_add(pending_bytes_acc, Ordering::Relaxed);
        }
        if delayed_count > 0 {
            self.delayed_count
                .fetch_add(delayed_count, Ordering::Relaxed);
            self.delayed_bytes
                .fetch_add(delayed_bytes_acc, Ordering::Relaxed);
        }

        // Update cached min — only need first message_id (lowest)
        let first_id = log_index;
        let cached = self.cached_min_required.load(Ordering::Relaxed);
        if cached == MIN_NONE {
            self.cached_min_required.store(first_id, Ordering::Relaxed);
        } else if cached != MIN_DIRTY && first_id < cached {
            self.cached_min_required.store(first_id, Ordering::Relaxed);
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
        let mut keys_to_remove: SmallVec<[(u8, u64); 8]> = SmallVec::new();
        let deadline = current_time + config.visibility_timeout_ms;

        // Hold pending lock only for iteration + removal, no nested locks
        {
            let guard = self.messages.pin();
            let mut pending = self.pending.lock();
            for (&(prio, msg_id), _) in pending.iter().rev() {
                if delivered.len() >= max_count as usize {
                    break;
                }
                if let Some(msg) = guard.get(&msg_id) {
                    if msg.state != MessageState::Pending {
                        continue;
                    }
                    if msg.deliver_after > current_time {
                        continue;
                    }
                    let mut updated = msg.clone();
                    updated.state = MessageState::InFlight;
                    updated.consumer_id = Some(consumer_id);
                    updated.attempts += 1;
                    updated.last_delivered_at = Some(current_time);
                    updated.visibility_deadline = Some(deadline);
                    guard.insert(msg_id, updated);

                    delivered.push(msg_id);
                    keys_to_remove.push((prio, msg_id));
                }
            }
            for key in &keys_to_remove {
                pending.remove(key);
            }
        }
        // pending lock dropped before taking in_flight_deadlines lock

        let count = delivered.len() as u64;
        if count > 0 {
            self.pending_count.fetch_sub(count, Ordering::Relaxed);
            self.in_flight_count.fetch_add(count, Ordering::Relaxed);

            // Batch: single in_flight_deadlines lock for all delivered messages
            {
                let mut deadlines = self.in_flight_deadlines.lock();
                let entry = deadlines.entry(deadline).or_default();
                entry.extend(delivered.iter().copied());
            }

            // Track per-consumer in-flight
            {
                let cif = self.consumer_in_flight.pin();
                let mut set = cif.get(&consumer_id).cloned().unwrap_or_default();
                set.extend(delivered.iter().copied());
                cif.insert(consumer_id, set);
            }
        }

        self.m_deliver_count.increment(count);
        delivered
    }

    /// ACK messages.
    pub fn apply_ack(&self, message_ids: &[u64]) {
        let mut ack_count = 0u64;
        let mut ack_bytes = 0u64;
        let mut invalidate_min = false;
        let msg_guard = self.messages.pin();
        let cif_guard = self.consumer_in_flight.pin();
        for &msg_id in message_ids {
            if let Some(meta) = msg_guard.get(&msg_id).cloned() {
                msg_guard.remove(&msg_id);
                if meta.state == MessageState::InFlight {
                    ack_count += 1;
                    ack_bytes += meta.value_len as u64;
                    // Remove from consumer in-flight tracking
                    if let Some(cid) = meta.consumer_id {
                        if let Some(ids) = cif_guard.get(&cid) {
                            let mut updated = ids.clone();
                            updated.remove(&msg_id);
                            cif_guard.insert(cid, updated);
                        }
                    }
                }
                // Check cached min invalidation
                if !invalidate_min {
                    let cached = self.cached_min_required.load(Ordering::Relaxed);
                    if cached != MIN_DIRTY && cached != MIN_NONE && msg_id <= cached {
                        invalidate_min = true;
                    }
                }
            }
        }
        // Batch atomic updates
        if ack_count > 0 {
            self.in_flight_count.fetch_sub(ack_count, Ordering::Relaxed);
            self.in_flight_bytes.fetch_sub(ack_bytes, Ordering::Relaxed);
        }
        if invalidate_min {
            self.cached_min_required.store(MIN_DIRTY, Ordering::Relaxed);
        }
        self.m_ack_count.increment(message_ids.len() as u64);
    }

    /// NACK messages — return to pending with attempt increment.
    pub fn apply_nack(&self, message_ids: &[u64]) {
        let mut pending_batch: SmallVec<[(u8, u64); 8]> = SmallVec::new();
        let msg_guard = self.messages.pin();
        let cif_guard = self.consumer_in_flight.pin();
        for &msg_id in message_ids {
            if let Some(msg) = msg_guard.get(&msg_id) {
                if msg.state == MessageState::InFlight {
                    let cid = msg.consumer_id;
                    let priority = msg.priority;
                    let mut updated = msg.clone();
                    updated.state = MessageState::Pending;
                    updated.consumer_id = None;
                    updated.visibility_deadline = None;
                    msg_guard.insert(msg_id, updated);
                    pending_batch.push((priority, msg_id));
                    // Remove from consumer in-flight
                    if let Some(cid) = cid {
                        if let Some(ids) = cif_guard.get(&cid) {
                            let mut s = ids.clone();
                            s.remove(&msg_id);
                            cif_guard.insert(cid, s);
                        }
                    }
                }
            }
        }
        if !pending_batch.is_empty() {
            let count = pending_batch.len() as u64;
            let mut pending = self.pending.lock();
            for (prio, msg_id) in pending_batch {
                pending.insert((prio, msg_id), ());
            }
            self.pending_count.fetch_add(count, Ordering::Relaxed);
            self.in_flight_count.fetch_sub(count, Ordering::Relaxed);
        }
    }

    /// RELEASE messages — return to pending WITHOUT attempt increment.
    pub fn apply_release(&self, message_ids: &[u64]) {
        let mut pending_batch: SmallVec<[(u8, u64); 8]> = SmallVec::new();
        let msg_guard = self.messages.pin();
        let cif_guard = self.consumer_in_flight.pin();
        for &msg_id in message_ids {
            if let Some(msg) = msg_guard.get(&msg_id) {
                if msg.state == MessageState::InFlight {
                    let cid = msg.consumer_id;
                    let priority = msg.priority;
                    let mut updated = msg.clone();
                    updated.state = MessageState::Pending;
                    updated.consumer_id = None;
                    updated.visibility_deadline = None;
                    // Decrement attempt count (release = no attempt charge)
                    updated.attempts = updated.attempts.saturating_sub(1);
                    msg_guard.insert(msg_id, updated);
                    pending_batch.push((priority, msg_id));
                    if let Some(cid) = cid {
                        if let Some(ids) = cif_guard.get(&cid) {
                            let mut s = ids.clone();
                            s.remove(&msg_id);
                            cif_guard.insert(cid, s);
                        }
                    }
                }
            }
        }
        if !pending_batch.is_empty() {
            let count = pending_batch.len() as u64;
            let mut pending = self.pending.lock();
            for (prio, msg_id) in pending_batch {
                pending.insert((prio, msg_id), ());
            }
            self.pending_count.fetch_add(count, Ordering::Relaxed);
            self.in_flight_count.fetch_sub(count, Ordering::Relaxed);
        }
    }

    /// Extend visibility timeout for in-flight messages.
    pub fn apply_extend_visibility(&self, message_ids: &[u64], extension_ms: u64) {
        let guard = self.messages.pin();
        for &msg_id in message_ids {
            if let Some(msg) = guard.get(&msg_id) {
                if msg.state == MessageState::InFlight && msg.visibility_deadline.is_some() {
                    let mut updated = msg.clone();
                    if let Some(ref mut deadline) = updated.visibility_deadline {
                        *deadline += extension_ms;
                    }
                    guard.insert(msg_id, updated);
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
        let mut pending_batch: SmallVec<[(u8, u64); 8]> = SmallVec::new();
        let mut dlq_count = 0u64;
        let msg_guard = self.messages.pin();
        let cif_guard = self.consumer_in_flight.pin();
        for &msg_id in message_ids {
            if let Some(msg) = msg_guard.get(&msg_id) {
                if msg.state != MessageState::InFlight {
                    continue;
                }
                if msg.attempts >= config.max_retries {
                    let mut updated = msg.clone();
                    updated.state = MessageState::DeadLetter;
                    msg_guard.insert(msg_id, updated);
                    dlq_count += 1;
                    dead_lettered.push(msg_id);
                } else {
                    // Return to pending
                    let cid = msg.consumer_id;
                    let priority = msg.priority;
                    let mut updated = msg.clone();
                    updated.state = MessageState::Pending;
                    updated.consumer_id = None;
                    updated.visibility_deadline = None;
                    msg_guard.insert(msg_id, updated);
                    pending_batch.push((priority, msg_id));
                    if let Some(cid) = cid {
                        if let Some(ids) = cif_guard.get(&cid) {
                            let mut s = ids.clone();
                            s.remove(&msg_id);
                            cif_guard.insert(cid, s);
                        }
                    }
                }
            }
        }
        // Batch pending lock
        if !pending_batch.is_empty() {
            let count = pending_batch.len() as u64;
            let mut pending = self.pending.lock();
            for (prio, msg_id) in pending_batch {
                pending.insert((prio, msg_id), ());
            }
            self.pending_count.fetch_add(count, Ordering::Relaxed);
            self.in_flight_count.fetch_sub(count, Ordering::Relaxed);
        }
        if dlq_count > 0 {
            self.in_flight_count.fetch_sub(dlq_count, Ordering::Relaxed);
            self.dlq_count.fetch_add(dlq_count, Ordering::Relaxed);
            self.m_dlq_count.increment(dlq_count);
        }
        dead_lettered
    }

    /// Get in-flight message IDs for a consumer (for disconnect handling).
    pub fn consumer_in_flight_ids(&self, consumer_id: u64) -> SmallVec<[u64; 8]> {
        self.consumer_in_flight
            .pin()
            .get(&consumer_id)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Remove dead-lettered messages after DLQ publish.
    pub fn apply_remove_dead_lettered(&self, message_ids: &SmallVec<[u64; 8]>) {
        let guard = self.messages.pin();
        for &msg_id in message_ids {
            if guard.get(&msg_id).is_some() {
                guard.remove(&msg_id);
                self.dlq_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
        self.cached_min_required.store(MIN_DIRTY, Ordering::Relaxed);
    }

    /// Expire pending messages that have exceeded their TTL.
    pub fn apply_expire_pending(&self, message_ids: &[u64]) {
        let guard = self.messages.pin();
        for &msg_id in message_ids {
            if guard.get(&msg_id).is_some() {
                guard.remove(&msg_id);
                self.pending.lock().remove(&(0, msg_id));
                self.pending_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
        self.cached_min_required.store(MIN_DIRTY, Ordering::Relaxed);
    }

    /// Purge all messages.
    pub fn purge(&self) -> bool {
        let had_messages = self.has_messages();
        self.messages.pin().clear();
        self.pending.lock().clear();
        self.in_flight_deadlines.lock().clear();
        self.consumer_in_flight.pin().clear();
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
        let min = self.messages.pin().iter().map(|(_, e)| e.message_id).min();
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
            let flat = if has_reply_to || has_corr {
                crate::flat::FlatMessage::new(msg)
            } else {
                None
            };
            let rt = if has_reply_to {
                flat.as_ref()
                    .and_then(|f| f.reply_to())
                    .map(Bytes::copy_from_slice)
            } else {
                None
            };
            let ci = if has_corr {
                flat.as_ref()
                    .and_then(|f| f.correlation_id())
                    .map(Bytes::copy_from_slice)
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

        let publisher_id = crate::flat::FlatMessageMeta::parse(msg).map_or(0, |m| m.publisher_id);
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
            publisher_id,
        };

        self.messages.pin().insert(message_id, meta);
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
            self.messages.pin().iter().map(|(_, e)| e.clone()).collect();
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
        let msg_guard = self.messages.pin();
        let cif_guard = self.consumer_in_flight.pin();
        for msg in snap.messages {
            let msg_id = msg.message_id;
            let prio = msg.priority;
            let state = msg.state;
            let consumer_id = msg.consumer_id;
            let value_len = msg.value_len as u64;

            msg_guard.insert(msg_id, msg);

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
                        let mut set = cif_guard.get(&cid).cloned().unwrap_or_default();
                        set.insert(msg_id);
                        cif_guard.insert(cid, set);
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
    pub reply_to_map: papaya::HashMap<u64, Bytes>,
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
            reply_to_map: papaya::HashMap::new(),
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
            reply_to_map: papaya::HashMap::new(),
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
    pub actors: papaya::HashMap<Bytes, Arc<ActorInMemory>>,
    pub consumer_assignments: papaya::HashMap<u64, HashSet<Bytes>>,
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
            actors: papaya::HashMap::new(),
            consumer_assignments: papaya::HashMap::new(),
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
        let guard = self.actors.pin();
        let actor = match guard.get(actor_id) {
            Some(a) => a.clone(),
            None => {
                self.active_count.fetch_add(1, Ordering::Relaxed);
                let a = Arc::new(ActorInMemory::new(group_id, actor_id.clone(), current_time));
                guard.insert(actor_id.clone(), a.clone());
                a
            }
        };

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
            actor.reply_to_map.pin().insert(log_index, rt);
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
        let actor = self.actors.pin().get(actor_id)?.clone();
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
        if let Some(actor) = self.actors.pin().get(actor_id) {
            if actor.in_flight_index() == Some(message_id) {
                actor.in_flight_index.store(NONE_U64, Ordering::Relaxed);
                actor.attempts.store(0, Ordering::Relaxed);
                let bytes = value_len as u64;
                actor
                    .in_flight_bytes
                    .fetch_sub(bytes.min(actor.in_flight_bytes()), Ordering::Relaxed);
                let rt_guard = actor.reply_to_map.pin();
                let reply_to = rt_guard.get(&message_id).cloned();
                rt_guard.remove(&message_id);
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
        if let Some(actor) = self.actors.pin().get(actor_id) {
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
        let ca_guard = self.consumer_assignments.pin();
        let actors_guard = self.actors.pin();
        let mut set = ca_guard.get(&consumer_id).cloned().unwrap_or_default();
        for actor_id in actor_ids {
            if let Some(actor) = actors_guard.get(actor_id) {
                actor
                    .assigned_consumer_id
                    .store(consumer_id, Ordering::Relaxed);
                set.insert(actor_id.clone());
            }
        }
        ca_guard.insert(consumer_id, set);
    }

    pub fn apply_release(&self, consumer_id: u64) {
        let ca_guard = self.consumer_assignments.pin();
        if let Some(actor_ids) = ca_guard.get(&consumer_id).cloned() {
            ca_guard.remove(&consumer_id);
            let actors_guard = self.actors.pin();
            for actor_id in &actor_ids {
                if let Some(actor) = actors_guard.get(actor_id) {
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
        let guard = self.actors.pin();
        let to_remove: Vec<Bytes> = guard
            .iter()
            .filter(|(_, actor)| {
                actor.pending_count() == 0
                    && actor.in_flight_index().is_none()
                    && actor.last_activity_at() < before_timestamp
            })
            .map(|(k, _)| k.clone())
            .collect();
        let count = to_remove.len();
        for key in &to_remove {
            guard.remove(key);
            self.active_count.fetch_sub(1, Ordering::Relaxed);
        }
        if count > 0 {
            self.cached_min_required.store(MIN_DIRTY, Ordering::Relaxed);
        }
        self.m_evict_count.increment(count as u64);
        count
    }

    pub fn unassigned_actors_with_messages(&self) -> Vec<Bytes> {
        self.actors
            .pin()
            .iter()
            .filter(|(_, a)| {
                a.assigned_consumer_id().is_none()
                    && (a.pending_count() > 0 || a.in_flight_index().is_some())
            })
            .map(|(k, _)| k.clone())
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
        for (_, actor) in self.actors.pin().iter() {
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
            .pin()
            .iter()
            .map(|(_, actor)| actor.snapshot_state())
            .collect();
        ActorStateSnapshot { actors }
    }

    pub fn restore(&self, snap: ActorStateSnapshot, group_id: u64) {
        let guard = self.actors.pin();
        for actor_snap in snap.actors {
            guard.insert(
                actor_snap.actor_id.clone(),
                Arc::new(ActorInMemory::from_snapshot(group_id, actor_snap)),
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
    members: papaya::HashMap<String, Arc<GroupMemberState>>,
    pub(crate) offsets: papaya::HashMap<(u64, u32), GroupTopicPartitionOffset>,

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
            members: papaya::HashMap::new(),
            offsets: papaya::HashMap::new(),
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

        {
            let members_guard = state.members.pin();
            for m in snapshot_members {
                let protocols: SmallVec<[(String, Bytes); 2]> = m
                    .protocols
                    .into_iter()
                    .map(|(n, d)| (n, Bytes::from(d)))
                    .collect();
                let member_id = m.member_id;
                members_guard.insert(
                    member_id.clone(),
                    Arc::new(GroupMemberState {
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
                    }),
                );
            }
        }

        {
            let offsets_guard = state.offsets.pin();
            for o in offsets {
                offsets_guard.insert((o.topic_id, o.partition_index), o);
            }
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
        self.members.pin().len()
    }

    #[inline]
    pub fn has_member(&self, member_id: &str) -> bool {
        self.members.pin().get(member_id).is_some()
    }

    #[inline]
    pub fn auto_offset_reset(&self) -> AutoOffsetReset {
        self.meta.auto_offset_reset
    }

    #[inline]
    pub fn get_offset(&self, topic_id: u64, partition_index: u32) -> Option<u64> {
        self.offsets
            .pin()
            .get(&(topic_id, partition_index))
            .map(|o| o.committed_offset)
    }

    #[inline]
    pub fn get_member_assignment(&self, member_id: &str) -> Option<Bytes> {
        self.members
            .pin()
            .get(member_id)
            .map(|m| m.assignment.read().clone())
    }

    pub fn member_protocols(&self) -> Vec<(String, Bytes)> {
        let chosen = self.protocol_name.load();
        self.members
            .pin()
            .iter()
            .map(|(_, m)| {
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
            .pin()
            .iter()
            .filter(|(_, m)| {
                let timeout = m.session_timeout_ms as u64;
                let last = m.last_heartbeat_at.load(Ordering::Relaxed);
                now_ms > last + timeout
            })
            .map(|(k, _)| k.clone())
            .collect()
    }

    #[inline]
    pub fn all_members_joined(&self) -> bool {
        let guard = self.members.pin();
        if guard.is_empty() {
            return false;
        }
        guard
            .iter()
            .all(|(_, e)| e.joined_this_gen.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn all_members_synced(&self) -> bool {
        let guard = self.members.pin();
        if guard.is_empty() {
            return false;
        }
        guard
            .iter()
            .all(|(_, e)| e.synced_this_gen.load(Ordering::Relaxed))
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
        self.members
            .pin()
            .insert(member.member_id.clone(), Arc::new(member));
    }

    #[inline]
    pub fn remove_member(&self, member_id: &str) {
        self.members.pin().remove(member_id);
    }

    #[inline]
    pub fn mark_joined(&self, member_id: &str) {
        if let Some(m) = self.members.pin().get(member_id) {
            m.joined_this_gen.store(true, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn mark_synced(&self, member_id: &str) {
        if let Some(m) = self.members.pin().get(member_id) {
            m.synced_this_gen.store(true, Ordering::Relaxed);
        }
    }

    pub fn clear_join_marks(&self) {
        for (_, m) in self.members.pin().iter() {
            m.joined_this_gen.store(false, Ordering::Relaxed);
        }
    }

    pub fn clear_sync_marks(&self) {
        for (_, m) in self.members.pin().iter() {
            m.synced_this_gen.store(false, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn set_member_assignment(&self, member_id: &str, assignment: Bytes) {
        if let Some(m) = self.members.pin().get(member_id) {
            *m.assignment.write() = assignment;
        }
    }

    #[inline]
    pub fn update_member_heartbeat(&self, member_id: &str, at: u64) {
        if let Some(m) = self.members.pin().get(member_id) {
            m.last_heartbeat_at.store(at, Ordering::Relaxed);
        }
    }

    pub fn select_and_set_protocol(&self) {
        let mut counts: SmallVec<[(String, usize); 8]> = SmallVec::new();
        let guard = self.members.pin();
        for (_, m) in guard.iter() {
            for (name, _) in m.protocols.iter() {
                if let Some(c) = counts.iter_mut().find(|(n, _)| n == name) {
                    c.1 += 1;
                } else {
                    counts.push((name.clone(), 1));
                }
            }
        }
        let member_count = guard.len();
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
            .pin()
            .iter()
            .next()
            .map(|(k, _)| Arc::new(k.clone()));
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
            .pin()
            .iter()
            .map(|(_, ms)| GroupMemberMeta {
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
        let msg = crate::flat::FlatMessageBuilder::new(b"hello")
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

        let msg = crate::flat::FlatMessageBuilder::new(b"data")
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

        let msg = crate::flat::FlatMessageBuilder::new(b"data")
            .timestamp(1000)
            .build();
        ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);
        let delivered = ack.apply_deliver(&config, 42, 10, 2000, 200);

        // Check attempts before release
        assert_eq!(ack.messages.pin().get(&delivered[0]).unwrap().attempts, 1);

        ack.apply_release(&delivered);
        assert_eq!(ack.pending_count(), 1);
        assert_eq!(ack.in_flight_count(), 0);

        // Release should not increment attempts
        assert_eq!(ack.messages.pin().get(&delivered[0]).unwrap().attempts, 0); // decremented back
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

    // ── Helpers ──────────────────────────────────────────────────────────

    fn make_actor_group(id: u64, name: &str) -> ConsumerGroupState {
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
            variant: GroupVariant::Actor,
            source_topic_id: 2,
            variant_config: VariantConfig::Actor(ActorVariantConfig::default()),
        };
        ConsumerGroupState::new(meta, "test", 0)
    }

    fn build_msg(payload: &[u8]) -> Bytes {
        crate::flat::FlatMessageBuilder::new(payload)
            .timestamp(1000)
            .build()
    }

    fn build_msg_with_priority(payload: &[u8], prio: u8) -> Bytes {
        crate::flat::FlatMessageBuilder::new(payload)
            .timestamp(1000)
            .priority(prio)
            .build()
    }

    fn build_msg_with_ttl(payload: &[u8], ttl: u64) -> Bytes {
        crate::flat::FlatMessageBuilder::new(payload)
            .timestamp(1000)
            .ttl_ms(ttl)
            .build()
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ACK VARIANT STATE TESTS
    // ═══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_ack_enqueue_multiple() {
        let group = make_ack_group(1, "g");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        let msgs: Vec<Bytes> = (0..5)
            .map(|i| build_msg(format!("msg-{i}").as_bytes()))
            .collect();
        let dedup: Vec<Option<Bytes>> = vec![None; 5];
        let count = ack.apply_enqueue(&config, 1, 100, &msgs, &dedup, 1000);
        assert_eq!(count, 5);
        assert_eq!(ack.pending_count(), 5);
        assert_eq!(ack.total_messages.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn test_ack_delayed_enqueue() {
        let mut config = AckVariantConfig::default();
        config.delay_default_ms = 5000;
        let group = make_ack_group(1, "g");
        let ack = group.ack_state().unwrap();

        let msgs = vec![build_msg(b"delayed")];
        let count = ack.apply_enqueue(&config, 1, 100, &msgs, &[None], 1000);
        assert_eq!(count, 1);
        // Delayed messages don't go to pending
        assert_eq!(ack.pending_count(), 0);
        assert_eq!(ack.delayed_count(), 1);
        assert!(ack.delayed_bytes() > 0);
        assert_eq!(ack.total_messages.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_ack_collect_due_delayed() {
        let mut config = AckVariantConfig::default();
        config.delay_default_ms = 5000;
        let group = make_ack_group(1, "g");
        let ack = group.ack_state().unwrap();

        let msgs = vec![build_msg(b"delayed")];
        ack.apply_enqueue(&config, 1, 100, &msgs, &[None], 1000);
        assert_eq!(ack.delayed_count(), 1);

        // Not yet due
        let due = ack.collect_due_delayed(5000);
        assert!(due.is_empty());

        // Now due (current_time >= deliver_after)
        let due = ack.collect_due_delayed(6001);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0], 100);
    }

    #[test]
    fn test_ack_materialize_delayed() {
        let mut config = AckVariantConfig::default();
        config.delay_default_ms = 5000;
        let group = make_ack_group(1, "g");
        let ack = group.ack_state().unwrap();

        let msg = build_msg(b"delayed-msg");
        ack.apply_enqueue(&config, 1, 100, &[msg.clone()], &[None], 1000);
        assert_eq!(ack.delayed_count(), 1);
        assert_eq!(ack.pending_count(), 0);

        let due = ack.collect_due_delayed(6001);
        assert_eq!(due.len(), 1);

        ack.materialize_delayed(1, due[0], &msg, 6001);
        assert_eq!(ack.pending_count(), 1);
        assert_eq!(ack.delayed_count(), 0);
    }

    #[test]
    fn test_ack_extend_visibility() {
        let group = make_ack_group(1, "g");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        let msg = build_msg(b"data");
        ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);
        let delivered = ack.apply_deliver(&config, 42, 10, 2000, 200);
        assert_eq!(delivered.len(), 1);

        let original_deadline = ack
            .messages
            .pin()
            .get(&delivered[0])
            .unwrap()
            .visibility_deadline
            .unwrap();

        ack.apply_extend_visibility(&delivered, 10_000);

        assert_eq!(
            ack.messages
                .pin()
                .get(&delivered[0])
                .unwrap()
                .visibility_deadline
                .unwrap(),
            original_deadline + 10_000
        );
    }

    #[test]
    fn test_ack_timeout_under_max_retries() {
        let group = make_ack_group(1, "g");
        let mut config = AckVariantConfig::default();
        config.max_retries = 3;
        let ack = group.ack_state().unwrap();

        let msg = build_msg(b"data");
        ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);
        let delivered = ack.apply_deliver(&config, 42, 10, 2000, 200);
        assert_eq!(ack.in_flight_count(), 1);

        // Timeout with attempts(1) < max_retries(3) → returns to pending
        let dlq = ack.apply_timeout_expired(&delivered, &config, 35_000);
        assert!(dlq.is_empty());
        assert_eq!(ack.pending_count(), 1);
        assert_eq!(ack.in_flight_count(), 0);
    }

    #[test]
    fn test_ack_timeout_at_max_retries_dead_letters() {
        let group = make_ack_group(1, "g");
        let mut config = AckVariantConfig::default();
        config.max_retries = 1;
        let ack = group.ack_state().unwrap();

        let msg = build_msg(b"data");
        ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);
        let delivered = ack.apply_deliver(&config, 42, 10, 2000, 200);
        // After deliver, attempts = 1 which equals max_retries = 1
        let dlq = ack.apply_timeout_expired(&delivered, &config, 35_000);
        assert_eq!(dlq.len(), 1);
        assert_eq!(ack.dlq_count(), 1);
        assert_eq!(ack.in_flight_count(), 0);
        assert_eq!(ack.pending_count(), 0);
    }

    #[test]
    fn test_ack_remove_dead_lettered() {
        let group = make_ack_group(1, "g");
        let mut config = AckVariantConfig::default();
        config.max_retries = 1;
        let ack = group.ack_state().unwrap();

        let msg = build_msg(b"data");
        ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);
        let delivered = ack.apply_deliver(&config, 42, 10, 2000, 200);
        let dlq = ack.apply_timeout_expired(&delivered, &config, 35_000);
        assert_eq!(ack.dlq_count(), 1);

        ack.apply_remove_dead_lettered(&dlq);
        assert_eq!(ack.dlq_count(), 0);
        assert!(ack.messages.pin().is_empty());
    }

    #[test]
    fn test_ack_expire_pending() {
        let group = make_ack_group(1, "g");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        let msg = build_msg_with_ttl(b"data", 5000);
        ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);
        assert_eq!(ack.pending_count(), 1);

        ack.apply_expire_pending(&[100]);
        assert_eq!(ack.pending_count(), 0);
        assert!(ack.messages.pin().is_empty());
    }

    #[test]
    fn test_ack_purge() {
        let group = make_ack_group(1, "g");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        let msgs: Vec<Bytes> = (0..3)
            .map(|i| build_msg(format!("msg-{i}").as_bytes()))
            .collect();
        ack.apply_enqueue(&config, 1, 100, &msgs, &[None, None, None], 1000);
        let delivered = ack.apply_deliver(&config, 42, 1, 2000, 200);
        assert_eq!(ack.pending_count(), 2);
        assert_eq!(ack.in_flight_count(), 1);

        let had = ack.purge();
        assert!(had);
        assert_eq!(ack.pending_count(), 0);
        assert_eq!(ack.in_flight_count(), 0);
        assert_eq!(ack.dlq_count(), 0);
        assert_eq!(ack.delayed_count(), 0);
        assert!(ack.messages.pin().is_empty());

        // Purge empty returns false
        let had = ack.purge();
        assert!(!had);
    }

    #[test]
    fn test_ack_min_required_index() {
        let group = make_ack_group(1, "g");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        // Empty → None
        assert!(ack.min_required_index().is_none());

        let msgs = vec![build_msg(b"a"), build_msg(b"b"), build_msg(b"c")];
        ack.apply_enqueue(&config, 1, 100, &msgs, &[None, None, None], 1000);
        // Min should be 100 (first message)
        assert_eq!(ack.min_required_index(), Some(100));

        // Ack the min message → cache dirty → recompute
        ack.apply_ack(&[100]);
        assert_eq!(ack.min_required_index(), Some(101));
    }

    #[test]
    fn test_ack_consumer_in_flight_tracking() {
        let group = make_ack_group(1, "g");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        let msgs = vec![build_msg(b"a"), build_msg(b"b")];
        ack.apply_enqueue(&config, 1, 100, &msgs, &[None, None], 1000);

        let delivered = ack.apply_deliver(&config, 42, 10, 2000, 200);
        assert_eq!(delivered.len(), 2);

        let ids = ack.consumer_in_flight_ids(42);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&100));
        assert!(ids.contains(&101));

        // Unknown consumer
        let ids = ack.consumer_in_flight_ids(999);
        assert!(ids.is_empty());

        // Ack removes from tracking
        ack.apply_ack(&[100]);
        let ids = ack.consumer_in_flight_ids(42);
        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&101));
    }

    #[test]
    fn test_ack_would_exceed_limit_messages() {
        let group = make_ack_group(1, "g");
        let mut config = AckVariantConfig::default();
        config.max_pending_messages = Some(5);
        let ack = group.ack_state().unwrap();

        let msgs: Vec<Bytes> = (0..3).map(|_| build_msg(b"x")).collect();
        ack.apply_enqueue(&config, 1, 100, &msgs, &[None, None, None], 1000);

        // 3 pending + 3 additional = 6 > 5
        assert!(ack.would_exceed_limit(&config, 0, 0, 3, 0));
        // 3 pending + 2 additional = 5 ≤ 5
        assert!(!ack.would_exceed_limit(&config, 0, 0, 2, 0));
    }

    #[test]
    fn test_ack_would_exceed_limit_bytes() {
        let group = make_ack_group(1, "g");
        let mut config = AckVariantConfig::default();
        config.max_pending_bytes = Some(100);
        let ack = group.ack_state().unwrap();

        let msg = build_msg(b"12345678901234567890"); // 20 byte value
        ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);

        // pending_bytes ~ 20 + 80 additional = 100 ≤ 100
        let pb = ack.pending_bytes.load(Ordering::Relaxed);
        assert!(!ack.would_exceed_limit(&config, 0, 0, 0, 100 - pb));
        assert!(ack.would_exceed_limit(&config, 0, 0, 0, 101 - pb));
    }

    #[test]
    fn test_ack_would_exceed_delayed_limit() {
        let group = make_ack_group(1, "g");
        let mut config = AckVariantConfig::default();
        config.delay_default_ms = 5000;
        config.max_delayed_messages = Some(2);
        let ack = group.ack_state().unwrap();

        let msgs = vec![build_msg(b"a"), build_msg(b"b")];
        ack.apply_enqueue(&config, 1, 100, &msgs, &[None, None], 1000);
        assert_eq!(ack.delayed_count(), 2);

        assert!(ack.would_exceed_limit(&config, 0, 0, 1, 0));
        assert!(!ack.would_exceed_limit(&config, 0, 0, 0, 0));
    }

    #[test]
    fn test_ack_byte_accounting() {
        let group = make_ack_group(1, "g");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        let msg = build_msg(b"hello"); // 5 byte value
        ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);

        let pending_bytes = ack.pending_bytes.load(Ordering::Relaxed);
        assert!(pending_bytes > 0);
        assert_eq!(ack.in_flight_bytes.load(Ordering::Relaxed), 0);

        // Deliver moves bytes from pending accounting to in-flight
        let delivered = ack.apply_deliver(&config, 42, 10, 2000, 200);
        assert_eq!(delivered.len(), 1);
        // pending_bytes stays (not decremented in deliver — only pending_count changes)
        // in_flight_bytes not explicitly tracked in deliver currently

        // Ack removes from in_flight_bytes
        ack.apply_ack(&delivered);
        assert_eq!(ack.in_flight_count(), 0);
    }

    #[test]
    fn test_ack_snapshot_restore() {
        let group = make_ack_group(1, "g");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        // Enqueue several messages in various states
        let msgs = vec![build_msg(b"a"), build_msg(b"b"), build_msg(b"c")];
        ack.apply_enqueue(&config, 1, 100, &msgs, &[None, None, None], 1000);
        let delivered = ack.apply_deliver(&config, 42, 1, 2000, 200);
        assert_eq!(delivered.len(), 1);

        let snap = ack.snapshot();
        assert_eq!(snap.messages.len(), 3);

        // Restore into a fresh group
        let group2 = make_ack_group(2, "g2");
        let ack2 = group2.ack_state().unwrap();
        ack2.restore(snap, &config);

        assert_eq!(ack2.pending_count(), 2);
        assert_eq!(ack2.in_flight_count(), 1);
        assert_eq!(ack2.messages.pin().len(), 3);
        assert_eq!(ack2.total_messages.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_ack_snapshot_restore_with_delayed() {
        let mut config = AckVariantConfig::default();
        config.delay_default_ms = 5000;
        let group = make_ack_group(1, "g");
        let ack = group.ack_state().unwrap();

        let msgs = vec![build_msg(b"delayed")];
        ack.apply_enqueue(&config, 1, 100, &msgs, &[None], 1000);

        let snap = ack.snapshot();
        assert_eq!(snap.delayed_entries.len(), 1);

        let group2 = make_ack_group(2, "g2");
        let ack2 = group2.ack_state().unwrap();
        ack2.restore(snap, &config);
        assert_eq!(ack2.delayed_count(), 1);
    }

    #[test]
    fn test_ack_noop_on_nonexistent() {
        let group = make_ack_group(1, "g");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        // All of these should be no-ops on nonexistent message IDs
        ack.apply_ack(&[999]);
        ack.apply_nack(&[999]);
        ack.apply_release(&[999]);
        ack.apply_extend_visibility(&[999], 1000);
        let dlq = ack.apply_timeout_expired(&[999], &config, 1000);
        assert!(dlq.is_empty());
        assert_eq!(ack.pending_count(), 0);
        assert_eq!(ack.in_flight_count(), 0);
    }

    #[test]
    fn test_ack_nack_clears_consumer_id() {
        let group = make_ack_group(1, "g");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        let msg = build_msg(b"data");
        ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);
        let delivered = ack.apply_deliver(&config, 42, 10, 2000, 200);
        assert_eq!(ack.messages.pin().get(&100).unwrap().consumer_id, Some(42));

        ack.apply_nack(&delivered);
        let guard = ack.messages.pin();
        let meta = guard.get(&100).unwrap();
        assert_eq!(meta.consumer_id, None);
        assert_eq!(meta.state, MessageState::Pending);
    }

    #[test]
    fn test_ack_deliver_skips_non_pending() {
        let group = make_ack_group(1, "g");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        let msg = build_msg(b"data");
        ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);
        // First deliver takes the message
        let d1 = ack.apply_deliver(&config, 42, 10, 2000, 200);
        assert_eq!(d1.len(), 1);
        // Second deliver gets nothing
        let d2 = ack.apply_deliver(&config, 43, 10, 3000, 300);
        assert!(d2.is_empty());
    }

    #[test]
    fn test_ack_priority_ordering() {
        let group = make_ack_group(1, "g");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        // Higher priority = higher u8 = delivered first (BTreeMap iterated in reverse)
        let msg_low = build_msg_with_priority(b"low", 1);
        let msg_high = build_msg_with_priority(b"high", 9);
        ack.apply_enqueue(&config, 1, 100, &[msg_low], &[None], 1000);
        ack.apply_enqueue(&config, 1, 101, &[msg_high], &[None], 1000);

        // Deliver one at a time — should get high priority first (iter().rev())
        let d1 = ack.apply_deliver(&config, 42, 1, 2000, 200);
        assert_eq!(d1.len(), 1);
        assert_eq!(d1[0], 101); // high priority message

        let d2 = ack.apply_deliver(&config, 42, 1, 2000, 200);
        assert_eq!(d2.len(), 1);
        assert_eq!(d2[0], 100); // low priority message
    }

    #[test]
    fn test_ack_has_messages() {
        let group = make_ack_group(1, "g");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();
        assert!(!ack.has_messages());

        let msg = build_msg(b"x");
        ack.apply_enqueue(&config, 1, 100, &[msg], &[None], 1000);
        assert!(ack.has_messages());

        ack.purge();
        assert!(!ack.has_messages());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ACTOR VARIANT STATE TESTS
    // ═══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_actor_send_basic() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        let result = actor_state.apply_send(&config, 1, &actor_id, 100, 1000, None, 50);
        assert!(result.is_ok());
        assert_eq!(actor_state.active_count(), 1);

        let actor = actor_state.actors.pin().get(&actor_id).unwrap().clone();
        assert_eq!(actor.pending_count(), 1);
        assert_eq!(actor.pending_bytes(), 50);
        assert_eq!(actor.head_index.load(Ordering::Relaxed), 100);
        assert_eq!(actor.tail_index.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn test_actor_send_multiple() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        for i in 0..5u64 {
            actor_state
                .apply_send(&config, 1, &actor_id, 100 + i, 1000, None, 10)
                .unwrap();
        }

        let actor = actor_state.actors.pin().get(&actor_id).unwrap().clone();
        assert_eq!(actor.pending_count(), 5);
        assert_eq!(actor.pending_bytes(), 50);
        assert_eq!(actor.tail_index.load(Ordering::Relaxed), 100);
        assert_eq!(actor.head_index.load(Ordering::Relaxed), 104);
        assert_eq!(actor_state.active_count(), 1);
    }

    #[test]
    fn test_actor_send_mailbox_full() {
        let group = make_actor_group(1, "g");
        let mut config = ActorVariantConfig::default();
        config.max_mailbox_depth = 2;
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, None, 10)
            .unwrap();
        actor_state
            .apply_send(&config, 1, &actor_id, 101, 1000, None, 10)
            .unwrap();
        let result = actor_state.apply_send(&config, 1, &actor_id, 102, 1000, None, 10);
        assert!(matches!(result, Err(MqError::MailboxFull { pending: 2 })));
    }

    #[test]
    fn test_actor_send_backpressure_messages() {
        let group = make_actor_group(1, "g");
        let mut config = ActorVariantConfig::default();
        config.max_pending_messages = Some(2);
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, None, 10)
            .unwrap();
        actor_state
            .apply_send(&config, 1, &actor_id, 101, 1000, None, 10)
            .unwrap();
        let result = actor_state.apply_send(&config, 1, &actor_id, 102, 1000, None, 10);
        assert!(matches!(result, Err(MqError::BackPressure { .. })));
    }

    #[test]
    fn test_actor_send_backpressure_bytes() {
        let group = make_actor_group(1, "g");
        let mut config = ActorVariantConfig::default();
        config.max_pending_bytes = Some(50);
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, None, 30)
            .unwrap();
        let result = actor_state.apply_send(&config, 1, &actor_id, 101, 1000, None, 30);
        assert!(matches!(result, Err(MqError::BackPressure { .. })));
    }

    #[test]
    fn test_actor_deliver_basic() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, None, 50)
            .unwrap();
        actor_state.apply_assign(42, &[actor_id.clone()]);

        let msg = actor_state.apply_deliver(&actor_id, 42, 50);
        assert_eq!(msg, Some(100));

        let actor = actor_state.actors.pin().get(&actor_id).unwrap().clone();
        assert_eq!(actor.pending_count(), 0);
        assert_eq!(actor.in_flight_index(), Some(100));
        assert_eq!(actor.pending_bytes(), 0);
        assert_eq!(actor.in_flight_bytes(), 50);
        assert_eq!(actor.attempts(), 1);
    }

    #[test]
    fn test_actor_deliver_wrong_consumer() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, None, 50)
            .unwrap();
        actor_state.apply_assign(42, &[actor_id.clone()]);

        // Wrong consumer_id
        let msg = actor_state.apply_deliver(&actor_id, 99, 50);
        assert_eq!(msg, None);
    }

    #[test]
    fn test_actor_deliver_already_in_flight() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, None, 50)
            .unwrap();
        actor_state
            .apply_send(&config, 1, &actor_id, 101, 1000, None, 50)
            .unwrap();
        actor_state.apply_assign(42, &[actor_id.clone()]);

        let msg = actor_state.apply_deliver(&actor_id, 42, 50);
        assert_eq!(msg, Some(100));

        // Second deliver fails — already has in-flight
        let msg2 = actor_state.apply_deliver(&actor_id, 42, 50);
        assert_eq!(msg2, None);
    }

    #[test]
    fn test_actor_deliver_unassigned() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, None, 50)
            .unwrap();
        // Not assigned to any consumer
        let msg = actor_state.apply_deliver(&actor_id, 42, 50);
        assert_eq!(msg, None);
    }

    #[test]
    fn test_actor_deliver_empty_mailbox() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, None, 50)
            .unwrap();
        actor_state.apply_assign(42, &[actor_id.clone()]);

        // Deliver the only message
        actor_state.apply_deliver(&actor_id, 42, 50);
        actor_state.apply_ack(&actor_id, 100, 50);

        // Mailbox is empty now
        let msg = actor_state.apply_deliver(&actor_id, 42, 50);
        assert_eq!(msg, None);
    }

    #[test]
    fn test_actor_ack_basic() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, None, 50)
            .unwrap();
        actor_state.apply_assign(42, &[actor_id.clone()]);
        actor_state.apply_deliver(&actor_id, 42, 50);

        let reply = actor_state.apply_ack(&actor_id, 100, 50);
        assert_eq!(reply, None); // No reply_to was set

        let actor = actor_state.actors.pin().get(&actor_id).unwrap().clone();
        assert_eq!(actor.in_flight_index(), None);
        assert_eq!(actor.attempts(), 0);
        assert_eq!(actor.in_flight_bytes(), 0);
    }

    #[test]
    fn test_actor_ack_with_reply_to() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        let reply_to = Bytes::from_static(b"response-topic");
        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, Some(reply_to.clone()), 50)
            .unwrap();
        actor_state.apply_assign(42, &[actor_id.clone()]);
        actor_state.apply_deliver(&actor_id, 42, 50);

        let reply = actor_state.apply_ack(&actor_id, 100, 50);
        assert_eq!(reply, Some(reply_to));
    }

    #[test]
    fn test_actor_ack_wrong_message() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, None, 50)
            .unwrap();
        actor_state.apply_assign(42, &[actor_id.clone()]);
        actor_state.apply_deliver(&actor_id, 42, 50);

        // Wrong message_id
        let reply = actor_state.apply_ack(&actor_id, 999, 50);
        assert_eq!(reply, None);
        // In-flight should still be set
        let actor = actor_state.actors.pin().get(&actor_id).unwrap().clone();
        assert_eq!(actor.in_flight_index(), Some(100));
    }

    #[test]
    fn test_actor_nack() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, None, 50)
            .unwrap();
        actor_state.apply_assign(42, &[actor_id.clone()]);
        actor_state.apply_deliver(&actor_id, 42, 50);

        actor_state.apply_nack(&actor_id, 100, 50);

        let actor = actor_state.actors.pin().get(&actor_id).unwrap().clone();
        assert_eq!(actor.in_flight_index(), None);
        assert_eq!(actor.pending_count(), 1); // returned to mailbox
        assert_eq!(actor.in_flight_bytes(), 0);
        assert_eq!(actor.pending_bytes(), 50);
        // Message should be back at front of mailbox
        assert_eq!(*actor.mailbox.lock().front().unwrap(), 100);
    }

    #[test]
    fn test_actor_assign() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let a1 = Bytes::from_static(b"actor-1");
        let a2 = Bytes::from_static(b"actor-2");

        actor_state
            .apply_send(&config, 1, &a1, 100, 1000, None, 10)
            .unwrap();
        actor_state
            .apply_send(&config, 1, &a2, 101, 1000, None, 10)
            .unwrap();

        actor_state.apply_assign(42, &[a1.clone(), a2.clone()]);

        let actor1 = actor_state.actors.pin().get(&a1).unwrap().clone();
        assert_eq!(actor1.assigned_consumer_id(), Some(42));
        let actor2 = actor_state.actors.pin().get(&a2).unwrap().clone();
        assert_eq!(actor2.assigned_consumer_id(), Some(42));

        let assignments = actor_state
            .consumer_assignments
            .pin()
            .get(&42)
            .unwrap()
            .clone();
        assert!(assignments.contains(&a1));
        assert!(assignments.contains(&a2));
    }

    #[test]
    fn test_actor_release() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, None, 50)
            .unwrap();
        actor_state.apply_assign(42, &[actor_id.clone()]);
        actor_state.apply_deliver(&actor_id, 42, 50);

        let actor = actor_state.actors.pin().get(&actor_id).unwrap().clone();
        assert_eq!(actor.in_flight_index(), Some(100));
        assert_eq!(actor.in_flight_bytes(), 50);
        drop(actor);

        // Release consumer 42 → in-flight returns to pending
        actor_state.apply_release(42);

        let actor = actor_state.actors.pin().get(&actor_id).unwrap().clone();
        assert_eq!(actor.assigned_consumer_id(), None);
        assert_eq!(actor.in_flight_index(), None);
        assert_eq!(actor.pending_count(), 1); // returned to mailbox
        assert_eq!(actor.in_flight_bytes(), 0);
        assert!(actor.pending_bytes() > 0);

        assert!(actor_state.consumer_assignments.pin().get(&42).is_none());
    }

    #[test]
    fn test_actor_evict_idle() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();

        let idle = Bytes::from_static(b"idle-actor");
        let active = Bytes::from_static(b"active-actor");

        actor_state
            .apply_send(&config, 1, &idle, 100, 500, None, 10)
            .unwrap();
        actor_state
            .apply_send(&config, 1, &active, 101, 2000, None, 10)
            .unwrap();

        // Ack the idle actor's only message so it has pending=0
        actor_state.apply_assign(42, &[idle.clone()]);
        actor_state.apply_deliver(&idle, 42, 10);
        actor_state.apply_ack(&idle, 100, 10);

        // Evict actors idle before timestamp 1000
        let count = actor_state.apply_evict_idle(1000);
        assert_eq!(count, 1);
        assert!(actor_state.actors.pin().get(&idle).is_none());
        assert!(actor_state.actors.pin().get(&active).is_some());
        assert_eq!(actor_state.active_count(), 1);
    }

    #[test]
    fn test_actor_evict_keeps_actors_with_pending() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 500, None, 10)
            .unwrap();

        // Actor has pending messages, so evict should keep it even if idle
        let count = actor_state.apply_evict_idle(1000);
        assert_eq!(count, 0);
        assert!(actor_state.actors.pin().get(&actor_id).is_some());
    }

    #[test]
    fn test_actor_unassigned_with_messages() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();

        let a1 = Bytes::from_static(b"actor-1");
        let a2 = Bytes::from_static(b"actor-2");
        let a3 = Bytes::from_static(b"actor-3");

        actor_state
            .apply_send(&config, 1, &a1, 100, 1000, None, 10)
            .unwrap();
        actor_state
            .apply_send(&config, 1, &a2, 101, 1000, None, 10)
            .unwrap();
        actor_state
            .apply_send(&config, 1, &a3, 102, 1000, None, 10)
            .unwrap();

        // Assign only a1
        actor_state.apply_assign(42, &[a1.clone()]);

        let unassigned = actor_state.unassigned_actors_with_messages();
        assert_eq!(unassigned.len(), 2);
        assert!(unassigned.contains(&a2));
        assert!(unassigned.contains(&a3));
    }

    #[test]
    fn test_actor_min_required_index() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();

        assert!(actor_state.min_required_index().is_none());

        let a1 = Bytes::from_static(b"actor-1");
        let a2 = Bytes::from_static(b"actor-2");

        actor_state
            .apply_send(&config, 1, &a1, 100, 1000, None, 10)
            .unwrap();
        actor_state
            .apply_send(&config, 1, &a2, 200, 1000, None, 10)
            .unwrap();

        assert_eq!(actor_state.min_required_index(), Some(100));

        // Ack a1's message
        actor_state.apply_assign(42, &[a1.clone()]);
        actor_state.apply_deliver(&a1, 42, 10);
        actor_state.apply_ack(&a1, 100, 10);

        // Now min should be actor-2's tail
        assert_eq!(actor_state.min_required_index(), Some(200));
    }

    #[test]
    fn test_actor_snapshot_restore() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();

        let a1 = Bytes::from_static(b"actor-1");
        let a2 = Bytes::from_static(b"actor-2");

        actor_state
            .apply_send(&config, 1, &a1, 100, 1000, None, 50)
            .unwrap();
        actor_state
            .apply_send(&config, 1, &a1, 101, 1000, None, 50)
            .unwrap();
        actor_state
            .apply_send(&config, 1, &a2, 200, 2000, None, 30)
            .unwrap();
        actor_state.apply_assign(42, &[a1.clone()]);

        let snap = actor_state.snapshot();
        assert_eq!(snap.actors.len(), 2);

        let group2 = make_actor_group(2, "g2");
        let actor_state2 = group2.actor_state().unwrap();
        actor_state2.restore(snap, 2);

        assert_eq!(actor_state2.active_count(), 2);
        let a1_restored = actor_state2.actors.pin().get(&a1).unwrap().clone();
        assert_eq!(a1_restored.pending_count(), 2);
        assert_eq!(a1_restored.assigned_consumer_id(), Some(42));
        assert_eq!(a1_restored.pending_bytes(), 100);
    }

    #[test]
    fn test_actor_byte_accounting() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();
        let actor_id = Bytes::from_static(b"actor-1");

        actor_state
            .apply_send(&config, 1, &actor_id, 100, 1000, None, 100)
            .unwrap();
        let actor = actor_state.actors.pin().get(&actor_id).unwrap().clone();
        assert_eq!(actor.pending_bytes(), 100);
        assert_eq!(actor.in_flight_bytes(), 0);
        drop(actor);

        actor_state.apply_assign(42, &[actor_id.clone()]);
        actor_state.apply_deliver(&actor_id, 42, 100);

        let actor = actor_state.actors.pin().get(&actor_id).unwrap().clone();
        assert_eq!(actor.pending_bytes(), 0);
        assert_eq!(actor.in_flight_bytes(), 100);
        drop(actor);

        actor_state.apply_ack(&actor_id, 100, 100);
        let actor = actor_state.actors.pin().get(&actor_id).unwrap().clone();
        assert_eq!(actor.pending_bytes(), 0);
        assert_eq!(actor.in_flight_bytes(), 0);
    }

    #[test]
    fn test_actor_multiple_actors_independent() {
        let group = make_actor_group(1, "g");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();

        let a1 = Bytes::from_static(b"actor-1");
        let a2 = Bytes::from_static(b"actor-2");

        actor_state
            .apply_send(&config, 1, &a1, 100, 1000, None, 10)
            .unwrap();
        actor_state
            .apply_send(&config, 1, &a2, 101, 1000, None, 20)
            .unwrap();

        actor_state.apply_assign(42, &[a1.clone()]);
        actor_state.apply_assign(43, &[a2.clone()]);

        // Each delivers independently
        let m1 = actor_state.apply_deliver(&a1, 42, 10);
        let m2 = actor_state.apply_deliver(&a2, 43, 20);
        assert_eq!(m1, Some(100));
        assert_eq!(m2, Some(101));

        // Ack independently
        actor_state.apply_ack(&a1, 100, 10);
        let a1_ref = actor_state.actors.pin().get(&a1).unwrap().clone();
        assert_eq!(a1_ref.in_flight_index(), None);
        let a2_ref = actor_state.actors.pin().get(&a2).unwrap().clone();
        assert_eq!(a2_ref.in_flight_index(), Some(101));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // CONSUMER GROUP STATE TESTS
    // ═══════════════════════════════════════════════════════════════════════

    #[test]
    fn test_cg_phase_lifecycle() {
        let group = make_group(1, "g");
        assert_eq!(group.phase(), GroupPhase::Empty);

        group.set_phase(GroupPhase::PreparingRebalance);
        assert_eq!(group.phase(), GroupPhase::PreparingRebalance);

        group.set_phase(GroupPhase::CompletingRebalance);
        assert_eq!(group.phase(), GroupPhase::CompletingRebalance);

        group.set_phase(GroupPhase::Stable);
        assert_eq!(group.phase(), GroupPhase::Stable);

        group.set_phase(GroupPhase::Dead);
        assert_eq!(group.phase(), GroupPhase::Dead);
    }

    #[test]
    fn test_cg_members_joined_synced() {
        let group = make_group(1, "g");

        // Empty group: all_members_joined and all_members_synced return false
        assert!(!group.all_members_joined());
        assert!(!group.all_members_synced());

        group.upsert_member(make_member("m-1", &[("range", b"")]));
        group.upsert_member(make_member("m-2", &[("range", b"")]));

        assert!(!group.all_members_joined());

        group.mark_joined("m-1");
        assert!(!group.all_members_joined());

        group.mark_joined("m-2");
        assert!(group.all_members_joined());
        assert!(!group.all_members_synced());

        group.mark_synced("m-1");
        group.mark_synced("m-2");
        assert!(group.all_members_synced());

        // Clear marks
        group.clear_join_marks();
        assert!(!group.all_members_joined());
        group.clear_sync_marks();
        assert!(!group.all_members_synced());
    }

    #[test]
    fn test_cg_offsets() {
        let group = make_group(1, "g");

        assert!(group.get_offset(10, 0).is_none());

        group.offsets.pin().insert(
            (10, 0),
            GroupTopicPartitionOffset {
                topic_id: 10,
                partition_index: 0,
                committed_offset: 42,
                metadata: None,
                committed_at: 1000,
            },
        );
        assert_eq!(group.get_offset(10, 0), Some(42));

        // Different partition
        assert!(group.get_offset(10, 1).is_none());
    }

    #[test]
    fn test_cg_select_protocol() {
        let group = make_group(1, "g");

        group.upsert_member(make_member("m-1", &[("range", b""), ("roundrobin", b"")]));
        group.upsert_member(make_member("m-2", &[("range", b"")]));

        group.select_and_set_protocol();
        // "range" is supported by both members, "roundrobin" only by one
        assert_eq!(*group.protocol_name(), "range");
    }

    #[test]
    fn test_cg_elect_leader() {
        let group = make_group(1, "g");

        group.upsert_member(make_member("m-1", &[("range", b"")]));
        group.upsert_member(make_member("m-2", &[("range", b"")]));

        group.elect_leader();
        assert!(group.leader().is_some());
        // Leader should be one of the members
        let leader = group.leader().unwrap();
        assert!(leader.as_str() == "m-1" || leader.as_str() == "m-2");
    }

    #[test]
    fn test_cg_find_expired_members() {
        let group = make_group(1, "g");
        group.upsert_member(make_member("m-1", &[("range", b"")]));
        group.upsert_member(make_member("m-2", &[("range", b"")]));

        // Both members have last_heartbeat_at=1000, session_timeout=30_000
        // At time 29_000 neither is expired
        let expired = group.find_expired_members(29_000);
        assert!(expired.is_empty());

        // At time 31_001 both are expired (1000 + 30_000 = 31_000 < 31_001)
        let expired = group.find_expired_members(31_001);
        assert_eq!(expired.len(), 2);

        // Update heartbeat for m-1
        group.update_member_heartbeat("m-1", 30_000);
        let expired = group.find_expired_members(31_001);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], "m-2");
    }

    #[test]
    fn test_cg_member_assignment() {
        let group = make_group(1, "g");
        group.upsert_member(make_member("m-1", &[("range", b"")]));

        assert!(group.get_member_assignment("m-1").is_some());
        assert!(group.get_member_assignment("m-1").unwrap().is_empty());

        group.set_member_assignment("m-1", Bytes::from_static(b"\x01\x02\x03"));
        assert_eq!(
            &*group.get_member_assignment("m-1").unwrap(),
            b"\x01\x02\x03"
        );
    }

    #[test]
    fn test_cg_increment_next_member_id() {
        let group = make_group(1, "g");
        assert_eq!(group.next_member_id(), 1);
        let old = group.increment_next_member_id();
        assert_eq!(old, 1);
        assert_eq!(group.next_member_id(), 2);
    }

    #[test]
    fn test_cg_touch_activity() {
        let group = make_group(1, "g");
        assert_eq!(group.last_activity_at(), 1000);
        group.touch_activity(5000);
        assert_eq!(group.last_activity_at(), 5000);
    }

    #[test]
    fn test_cg_member_protocols() {
        let group = make_group(1, "g");
        group.upsert_member(make_member("m-1", &[("range", b"\x01")]));
        group.upsert_member(make_member("m-2", &[("range", b"\x02")]));
        group.set_protocol_name("range".to_string());

        let protocols = group.member_protocols();
        assert_eq!(protocols.len(), 2);
    }

    #[test]
    fn test_cg_snapshot_restore_offset() {
        let group = make_group(1, "test-group");
        group.upsert_member(make_member("m-1", &[("range", b"\x01\x02")]));
        group.bump_generation();
        group.set_phase(GroupPhase::Stable);
        group.set_leader(Some("m-1".to_string()));
        group.offsets.pin().insert(
            (10, 0),
            GroupTopicPartitionOffset {
                topic_id: 10,
                partition_index: 0,
                committed_offset: 42,
                metadata: Some(Bytes::from("meta")),
                committed_at: 2000,
            },
        );

        let meta = group.snapshot_meta();
        let offsets: Vec<GroupTopicPartitionOffset> =
            group.offsets.pin().iter().map(|(_, e)| e.clone()).collect();

        let restored = ConsumerGroupState::from_snapshot(meta, offsets, "test", 0);
        assert_eq!(restored.generation(), 1);
        assert_eq!(restored.phase(), GroupPhase::Stable);
        assert!(restored.leader().is_some());
        assert_eq!(*restored.leader().unwrap(), "m-1");
        assert_eq!(restored.member_count(), 1);
        assert!(restored.has_member("m-1"));
        assert_eq!(restored.get_offset(10, 0), Some(42));
    }

    #[test]
    fn test_cg_snapshot_restore_ack_variant() {
        let group = make_ack_group(1, "ack-cg");
        let config = AckVariantConfig::default();
        let ack = group.ack_state().unwrap();

        let msgs = vec![build_msg(b"a"), build_msg(b"b")];
        ack.apply_enqueue(&config, 1, 100, &msgs, &[None, None], 1000);
        ack.apply_deliver(&config, 42, 1, 2000, 200);

        let snap = ack.snapshot();
        let meta = group.snapshot_meta();

        let restored = ConsumerGroupState::from_snapshot(meta, vec![], "test", 0);
        let ack2 = restored.ack_state().unwrap();
        ack2.restore(snap, &config);

        assert_eq!(ack2.pending_count(), 1);
        assert_eq!(ack2.in_flight_count(), 1);
    }

    #[test]
    fn test_cg_snapshot_restore_actor_variant() {
        let group = make_actor_group(1, "actor-cg");
        let config = ActorVariantConfig::default();
        let actor_state = group.actor_state().unwrap();

        let a1 = Bytes::from_static(b"actor-1");
        actor_state
            .apply_send(&config, 1, &a1, 100, 1000, None, 50)
            .unwrap();
        actor_state.apply_assign(42, &[a1.clone()]);

        let snap = actor_state.snapshot();
        let meta = group.snapshot_meta();

        let restored = ConsumerGroupState::from_snapshot(meta, vec![], "test", 0);
        let actor_state2 = restored.actor_state().unwrap();
        actor_state2.restore(snap, 1);

        assert_eq!(actor_state2.active_count(), 1);
        let a1_ref = actor_state2.actors.pin().get(&a1).unwrap().clone();
        assert_eq!(a1_ref.pending_count(), 1);
        assert_eq!(a1_ref.assigned_consumer_id(), Some(42));
    }

    #[test]
    fn test_cg_variant_accessors() {
        let offset_group = make_group(1, "offset");
        assert!(offset_group.ack_state().is_none());
        assert!(offset_group.actor_state().is_none());

        let ack_group = make_ack_group(2, "ack");
        assert!(ack_group.ack_state().is_some());
        assert!(ack_group.actor_state().is_none());
        assert!(ack_group.ack_config().is_some());

        let actor_group = make_actor_group(3, "actor");
        assert!(actor_group.ack_state().is_none());
        assert!(actor_group.actor_state().is_some());
        assert!(actor_group.actor_config().is_some());
    }

    #[test]
    fn test_cg_clear_leader() {
        let group = make_group(1, "g");
        group.set_leader(Some("m-1".to_string()));
        assert!(group.leader().is_some());
        group.clear_leader();
        assert!(group.leader().is_none());
    }

    #[test]
    fn test_cg_from_snapshot_members() {
        let meta = ConsumerGroupMeta {
            group_id: 1,
            name: "g".to_string(),
            name_hash: 0,
            created_at: 1000,
            generation: 2,
            phase: GroupPhase::Stable,
            protocol_type: "consumer".to_string(),
            protocol_name: "range".to_string(),
            leader: Some("m-1".to_string()),
            auto_offset_reset: AutoOffsetReset::Latest,
            last_activity_at: 5000,
            next_member_id: 3,
            members: vec![
                GroupMemberMeta {
                    member_id: "m-1".to_string(),
                    client_id: "c-1".to_string(),
                    session_timeout_ms: 30_000,
                    rebalance_timeout_ms: 60_000,
                    protocol_type: "consumer".to_string(),
                    protocols: vec![("range".to_string(), vec![1, 2])],
                    assignment: vec![10, 20],
                },
                GroupMemberMeta {
                    member_id: "m-2".to_string(),
                    client_id: "c-2".to_string(),
                    session_timeout_ms: 30_000,
                    rebalance_timeout_ms: 60_000,
                    protocol_type: "consumer".to_string(),
                    protocols: vec![("range".to_string(), vec![3, 4])],
                    assignment: vec![30, 40],
                },
            ],
            variant: GroupVariant::Offset,
            source_topic_id: 0,
            variant_config: VariantConfig::Offset,
        };

        let restored = ConsumerGroupState::from_snapshot(meta, vec![], "test", 0);
        assert_eq!(restored.member_count(), 2);
        assert!(restored.has_member("m-1"));
        assert!(restored.has_member("m-2"));
        assert_eq!(restored.generation(), 2);
        assert_eq!(restored.phase(), GroupPhase::Stable);
        assert_eq!(restored.next_member_id(), 3);

        // Check member assignment restored
        let a1 = restored.get_member_assignment("m-1").unwrap();
        assert_eq!(&*a1, &[10, 20]);
        let a2 = restored.get_member_assignment("m-2").unwrap();
        assert_eq!(&*a2, &[30, 40]);
    }
}

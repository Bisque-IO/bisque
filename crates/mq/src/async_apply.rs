//! Async apply — pull-based worker architecture for the MQ state machine.
//!
//! Workers read committed entries directly from the mmap raft log,
//! filter by partition (`primary_id % N`), and apply matching commands.
//! The state machine's `apply()` drains the stream, advances a
//! high-water mark, and returns. Workers process asynchronously.
//!
//! Batch entries (`TAG_BATCH`) are processed synchronously in `apply()`
//! with a barrier to preserve ordering. Workers skip them when scanning.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex as ParkingMutex;
use tracing::debug;

use bisque_raft::SegmentPrefetcher;

use crate::config::ParallelApplyConfig;
use crate::cursor::MqSegmentScanner;
use crate::engine::MqEngine;
use crate::manifest::MqManifestManager;
use crate::state_machine::{classify_structural, collect_structural_writes};
use crate::types::{self, MqCommand};

// =============================================================================
// High-Water Mark
// =============================================================================

/// Coalescing high-water mark with single-permit notification.
///
/// The writer (state machine) advances the HWM and notifies. Multiple
/// advances before a worker wakes coalesce into a single wakeup. The
/// worker reads the HWM, processes everything since its cursor, then
/// waits again.
pub struct HighWaterMark {
    value: AtomicU64,
    notify: tokio::sync::Notify,
}

impl HighWaterMark {
    pub fn new(initial: u64) -> Self {
        Self {
            value: AtomicU64::new(initial),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// Advance the HWM and notify all waiting workers.
    #[inline]
    pub fn advance(&self, new_hwm: u64) {
        self.value.store(new_hwm, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Wait for the HWM to advance past `cursor`. Returns the new HWM.
    ///
    /// Fast path: if HWM > cursor, returns immediately.
    /// Slow path: registers a single waker, sleeps until notified.
    #[inline]
    pub async fn wait_for(&self, cursor: u64) -> u64 {
        loop {
            // Register interest BEFORE checking the value to avoid missed wakeups.
            // notify_waiters() does not store permits, so we must call enable()
            // to ensure the notification is captured even if it fires between the
            // check and the await.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let hwm = self.value.load(Ordering::Acquire);
            if hwm > cursor {
                return hwm;
            }
            notified.await;
        }
    }

    #[inline]
    pub fn current(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }
}

// =============================================================================
// Applied batch tracker (per-node, gap-aware)
// =============================================================================

/// Shared per-node high-water marks for applied `batch_seq` values.
///
/// Each slot is an `AtomicU64` holding the highest *contiguous* `batch_seq`
/// applied for that node_id.  `u64::MAX` = nothing applied yet.
///
/// Updated by partition-0's worker (which owns the gap-tracking state) and
/// readable by anyone with an `Arc` reference — e.g. the `ForwardClient`
/// can poll its own slot to know which batches have been committed to raft.
pub struct AppliedBatchTable {
    slots: [AtomicU64; 64],
}

impl AppliedBatchTable {
    pub fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| AtomicU64::new(u64::MAX)),
        }
    }

    /// Read the highest contiguous applied `batch_seq` for `node_id`.
    /// Returns `u64::MAX` if nothing has been applied yet.
    #[inline]
    pub fn high_water(&self, node_id: u32) -> u64 {
        self.slots[node_id as usize].load(Ordering::Acquire)
    }

    /// Publish a new high-water mark for `node_id`.
    #[inline]
    fn publish(&self, node_id: u32, hwm: u64) {
        self.slots[node_id as usize].store(hwm, Ordering::Release);
    }
}

/// Per-node gap-aware tracker that drives [`AppliedBatchTable`] updates.
///
/// Maintains a contiguous high-water mark and a set of above-HWM sequences
/// applied out of order.  The HWM advances automatically as gaps fill —
/// same algorithm as `FollowerDedup` on the leader side.
///
/// Lives inside partition-0's worker (not shared directly).
struct AppliedBatchTracker {
    high_water: u64,
    above_hwm: HashSet<u64>,
}

impl AppliedBatchTracker {
    fn new() -> Self {
        Self {
            high_water: u64::MAX,
            above_hwm: HashSet::new(),
        }
    }

    /// Record that `batch_seq` has been applied. Returns the new high-water
    /// mark if it advanced (caller should publish), or `None` if unchanged.
    fn record(&mut self, batch_seq: u64) -> Option<u64> {
        if self.high_water == u64::MAX {
            self.high_water = batch_seq;
            self.advance();
            return Some(self.high_water);
        }
        if batch_seq <= self.high_water {
            return None; // already covered
        }
        if !self.above_hwm.insert(batch_seq) {
            return None; // duplicate
        }
        let before = self.high_water;
        self.advance();
        if self.high_water != before {
            Some(self.high_water)
        } else {
            None
        }
    }

    fn advance(&mut self) {
        loop {
            let next = self.high_water.wrapping_add(1);
            if self.above_hwm.remove(&next) {
                self.high_water = next;
            } else {
                break;
            }
        }
    }
}

// =============================================================================
// Response Entry (flat wire format)
// =============================================================================

/// Minimal response entry for per-client delivery channels.
///
/// Uses flat binary encoding with 8-byte aligned header:
/// `[tag:1][status:1][pad:6][log_index:8][...fields]`.
///
/// All field offsets are multiples of their natural alignment,
/// enabling zero-copy access via direct reads.
#[derive(Clone, Debug)]
pub struct ResponseEntry {
    pub buf: Bytes,
}

impl serde::Serialize for ResponseEntry {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(&self.buf)
    }
}

impl<'de> serde::Deserialize<'de> for ResponseEntry {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = <Vec<u8> as serde::Deserialize>::deserialize(d)?;
        Ok(Self {
            buf: bytes::Bytes::from(v),
        })
    }
}

impl ResponseEntry {
    pub const STATUS_OK: u8 = 0;
    pub const STATUS_ERROR: u8 = 1;

    pub const TAG_PUBLISHED: u8 = 0x01;
    pub const TAG_ENTITY_CREATED: u8 = 0x02;
    pub const TAG_OK: u8 = 0x03;
    pub const TAG_ERROR: u8 = 0x04;
    pub const TAG_MESSAGES: u8 = 0x05;
    pub const TAG_BATCH: u8 = 0x06;
    pub const TAG_GROUP_JOINED: u8 = 0x07;
    pub const TAG_GROUP_SYNCED: u8 = 0x08;
    pub const TAG_DEAD_LETTERED: u8 = 0x09;
    pub const TAG_RETAINED_MESSAGES: u8 = 0x0A;
    pub const TAG_WILL_PENDING: u8 = 0x0B;
    pub const TAG_SESSION_RESTORED: u8 = 0x0C;
    pub const TAG_SESSION_NOT_FOUND: u8 = 0x0D;
    pub const TAG_MULTI_MESSAGES: u8 = 0x0E;
    pub const TAG_TOPIC_ALIASES: u8 = 0x0F;
    pub const TAG_WILLS_FIRED: u8 = 0x10;
    pub const TAG_STATS: u8 = 0x11;
    pub const TAG_COMMITTED: u8 = 0x12;

    pub const ERR_CUSTOM: u8 = 0;
    pub const ERR_NOT_FOUND: u8 = 1;
    pub const ERR_ALREADY_EXISTS: u8 = 2;
    pub const ERR_MAILBOX_FULL: u8 = 3;
    pub const ERR_BACK_PRESSURE: u8 = 4;
    pub const ERR_ILLEGAL_GENERATION: u8 = 5;
    pub const ERR_REBALANCE_IN_PROGRESS: u8 = 6;
    pub const ERR_UNKNOWN_MEMBER_ID: u8 = 7;

    pub fn split_from(buf: &mut BytesMut) -> Self {
        Self {
            buf: buf.split().freeze(),
        }
    }

    pub fn write_ok(buf: &mut BytesMut, log_index: u64) {
        buf.put_u8(Self::TAG_OK);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 6);
        buf.put_u64_le(log_index);
    }

    pub fn write_published(buf: &mut BytesMut, log_index: u64, base_offset: u64, count: u64) {
        buf.put_u8(Self::TAG_PUBLISHED);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 6);
        buf.put_u64_le(log_index);
        buf.put_u64_le(base_offset);
        buf.put_u64_le(count);
    }

    pub fn write_entity_created(buf: &mut BytesMut, log_index: u64, entity_id: u64) {
        buf.put_u8(Self::TAG_ENTITY_CREATED);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 6);
        buf.put_u64_le(log_index);
        buf.put_u64_le(entity_id);
    }

    pub fn write_error(buf: &mut BytesMut, log_index: u64, error_code: u32, msg: &str) {
        let msg_bytes = msg.as_bytes();
        let padded_msg_len = (msg_bytes.len() + 7) & !7;
        buf.put_u8(Self::TAG_ERROR);
        buf.put_u8(Self::STATUS_ERROR);
        buf.put_bytes(0, 6);
        buf.put_u64_le(log_index);
        buf.put_u32_le(error_code);
        buf.put_u32_le(msg_bytes.len() as u32);
        buf.put_slice(msg_bytes);
        let pad = padded_msg_len - msg_bytes.len();
        if pad > 0 {
            buf.put_bytes(0, pad);
        }
    }

    pub fn write_mq_error(buf: &mut BytesMut, log_index: u64, error: &crate::types::MqError) {
        use crate::types::MqError;
        let (error_kind, entity_id) = match error {
            MqError::NotFound { id, .. } => (Self::ERR_NOT_FOUND, *id),
            MqError::AlreadyExists { id, .. } => (Self::ERR_ALREADY_EXISTS, *id),
            MqError::MailboxFull { pending } => (Self::ERR_MAILBOX_FULL, *pending as u64),
            MqError::BackPressure { group_id } => (Self::ERR_BACK_PRESSURE, *group_id),
            MqError::IllegalGeneration => (Self::ERR_ILLEGAL_GENERATION, 0),
            MqError::RebalanceInProgress => (Self::ERR_REBALANCE_IN_PROGRESS, 0),
            MqError::UnknownMemberId => (Self::ERR_UNKNOWN_MEMBER_ID, 0),
            MqError::Custom(_) => (Self::ERR_CUSTOM, 0),
        };
        let msg = error.to_string();
        let msg_bytes = msg.as_bytes();
        let padded_msg_len = (msg_bytes.len() + 7) & !7;
        buf.put_u8(Self::TAG_ERROR);
        buf.put_u8(Self::STATUS_ERROR);
        buf.put_u8(error_kind);
        buf.put_bytes(0, 5); // pad to 8
        buf.put_u64_le(log_index);
        buf.put_u64_le(entity_id);
        buf.put_u32_le(msg_bytes.len() as u32);
        buf.put_bytes(0, 4); // pad
        buf.put_slice(msg_bytes);
        if padded_msg_len > msg_bytes.len() {
            buf.put_bytes(0, padded_msg_len - msg_bytes.len());
        }
    }

    pub fn write_messages(
        buf: &mut BytesMut,
        log_index: u64,
        messages: &[crate::types::DeliveredMessage],
    ) {
        buf.put_u8(Self::TAG_MESSAGES);
        buf.put_u8(Self::STATUS_OK);
        buf.put_u16_le(messages.len() as u16);
        buf.put_bytes(0, 4); // pad
        buf.put_u64_le(log_index);
        for m in messages {
            buf.put_u64_le(m.message_id);
            buf.put_u64_le(m.original_timestamp);
            buf.put_u64_le(m.group_id);
            buf.put_u32_le(m.attempt);
            buf.put_bytes(0, 4); // pad
        }
    }

    pub fn write_batch_response(buf: &mut BytesMut, log_index: u64, entries: &[ResponseEntry]) {
        buf.put_u8(Self::TAG_BATCH);
        buf.put_u8(Self::STATUS_OK);
        buf.put_u16_le(entries.len() as u16);
        buf.put_bytes(0, 4); // pad
        buf.put_u64_le(log_index);
        for entry in entries {
            let entry_bytes = entry.buf.as_ref();
            let padded = (entry_bytes.len() + 7) & !7;
            buf.put_u32_le(entry_bytes.len() as u32);
            buf.put_bytes(0, 4); // pad
            buf.put_slice(entry_bytes);
            let pad = padded - entry_bytes.len();
            if pad > 0 {
                buf.put_bytes(0, pad);
            }
        }
    }

    pub fn write_group_joined(
        buf: &mut BytesMut,
        log_index: u64,
        generation: i32,
        leader: &str,
        member_id: &str,
        protocol_name: &str,
        is_leader: bool,
        phase_complete: bool,
        members: &[(String, bytes::Bytes)],
    ) {
        let flags: u8 = (is_leader as u8) | ((phase_complete as u8) << 1);
        let member_count = if is_leader { members.len() } else { 0 };
        buf.put_u8(Self::TAG_GROUP_JOINED);
        buf.put_u8(Self::STATUS_OK);
        buf.put_u8(flags);
        buf.put_u8(0); // pad
        buf.put_i32_le(generation);
        buf.put_u64_le(log_index);
        buf.put_u16_le(leader.len() as u16);
        buf.put_u16_le(member_id.len() as u16);
        buf.put_u16_le(protocol_name.len() as u16);
        buf.put_u16_le(member_count as u16);
        // Write variable strings
        let strings_start = buf.len();
        buf.put_slice(leader.as_bytes());
        buf.put_slice(member_id.as_bytes());
        buf.put_slice(protocol_name.as_bytes());
        let strings_len = buf.len() - strings_start;
        let strings_pad = (8 - (strings_len % 8)) % 8;
        if strings_pad > 0 {
            buf.put_bytes(0, strings_pad);
        }
        // Write member list (only if leader)
        if is_leader {
            for (mid, meta) in members {
                let mid_padded = (mid.len() + 7) & !7;
                let meta_padded = (meta.len() + 7) & !7;
                buf.put_u32_le(mid.len() as u32);
                buf.put_u32_le(meta.len() as u32);
                buf.put_slice(mid.as_bytes());
                if mid_padded > mid.len() {
                    buf.put_bytes(0, mid_padded - mid.len());
                }
                buf.put_slice(meta.as_ref());
                if meta_padded > meta.len() {
                    buf.put_bytes(0, meta_padded - meta.len());
                }
            }
        }
    }

    pub fn write_group_synced(
        buf: &mut BytesMut,
        log_index: u64,
        assignment: &[u8],
        phase_complete: bool,
    ) {
        let padded = (assignment.len() + 7) & !7;
        buf.put_u8(Self::TAG_GROUP_SYNCED);
        buf.put_u8(Self::STATUS_OK);
        buf.put_u8(phase_complete as u8);
        buf.put_bytes(0, 5); // pad
        buf.put_u64_le(log_index);
        buf.put_u32_le(assignment.len() as u32);
        buf.put_bytes(0, 4); // pad
        buf.put_slice(assignment);
        if padded > assignment.len() {
            buf.put_bytes(0, padded - assignment.len());
        }
    }

    pub fn write_dead_lettered(
        buf: &mut BytesMut,
        log_index: u64,
        dlq_topic_id: u64,
        dead_letter_ids: &[u64],
    ) {
        buf.put_u8(Self::TAG_DEAD_LETTERED);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 2); // pad
        buf.put_u32_le(dead_letter_ids.len() as u32);
        buf.put_u64_le(log_index);
        buf.put_u64_le(dlq_topic_id);
        for &id in dead_letter_ids {
            buf.put_u64_le(id);
        }
    }

    pub fn write_retained_messages(
        buf: &mut BytesMut,
        log_index: u64,
        messages: &[crate::types::RetainedEntry],
    ) {
        buf.put_u8(Self::TAG_RETAINED_MESSAGES);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 2); // pad
        buf.put_u32_le(messages.len() as u32);
        buf.put_u64_le(log_index);
        for entry in messages {
            let rk_padded = (entry.routing_key.len() + 7) & !7;
            let msg_padded = (entry.message.len() + 7) & !7;
            buf.put_u32_le(entry.routing_key.len() as u32);
            buf.put_u32_le(entry.message.len() as u32);
            buf.put_slice(&entry.routing_key);
            if rk_padded > entry.routing_key.len() {
                buf.put_bytes(0, rk_padded - entry.routing_key.len());
            }
            buf.put_slice(&entry.message);
            if msg_padded > entry.message.len() {
                buf.put_bytes(0, msg_padded - entry.message.len());
            }
        }
    }

    pub fn write_will_pending(buf: &mut BytesMut, log_index: u64, session_id: u64, delay_ms: u64) {
        buf.put_u8(Self::TAG_WILL_PENDING);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 6); // pad
        buf.put_u64_le(log_index);
        buf.put_u64_le(session_id);
        buf.put_u64_le(delay_ms);
    }

    pub fn write_session_restored(
        buf: &mut BytesMut,
        log_index: u64,
        session_id: u64,
        session_expiry_ms: u64,
        subscription_data: &[u8],
    ) {
        let padded = (subscription_data.len() + 7) & !7;
        buf.put_u8(Self::TAG_SESSION_RESTORED);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 6); // pad
        buf.put_u64_le(log_index);
        buf.put_u64_le(session_id);
        buf.put_u64_le(session_expiry_ms);
        buf.put_u32_le(subscription_data.len() as u32);
        buf.put_bytes(0, 4); // pad
        buf.put_slice(subscription_data);
        if padded > subscription_data.len() {
            buf.put_bytes(0, padded - subscription_data.len());
        }
    }

    pub fn write_session_not_found(buf: &mut BytesMut, log_index: u64) {
        buf.put_u8(Self::TAG_SESSION_NOT_FOUND);
        buf.put_u8(Self::STATUS_ERROR);
        buf.put_bytes(0, 6); // pad
        buf.put_u64_le(log_index);
    }

    pub fn write_multi_messages(
        buf: &mut BytesMut,
        log_index: u64,
        groups: &[(u64, smallvec::SmallVec<[crate::types::DeliveredMessage; 8]>)],
    ) {
        buf.put_u8(Self::TAG_MULTI_MESSAGES);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 2); // pad
        buf.put_u32_le(groups.len() as u32);
        buf.put_u64_le(log_index);
        for (group_id, messages) in groups {
            buf.put_u64_le(*group_id);
            buf.put_u32_le(messages.len() as u32);
            buf.put_bytes(0, 4); // pad
            for m in messages {
                buf.put_u64_le(m.message_id);
                buf.put_u64_le(m.original_timestamp);
                buf.put_u64_le(m.group_id);
                buf.put_u32_le(m.attempt);
                buf.put_bytes(0, 4); // pad
            }
        }
    }

    pub fn write_topic_aliases(
        buf: &mut BytesMut,
        log_index: u64,
        aliases: &[crate::types::TopicAliasEntry],
    ) {
        buf.put_u8(Self::TAG_TOPIC_ALIASES);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 2); // pad
        buf.put_u32_le(aliases.len() as u32);
        buf.put_u64_le(log_index);
        for alias in aliases {
            let name_bytes = alias.topic_name.as_bytes();
            let name_padded = (name_bytes.len() + 7) & !7;
            buf.put_u16_le(alias.alias);
            buf.put_u16_le(name_bytes.len() as u16);
            buf.put_bytes(0, 4); // pad
            buf.put_slice(name_bytes);
            if name_padded > name_bytes.len() {
                buf.put_bytes(0, name_padded - name_bytes.len());
            }
        }
    }

    pub fn write_committed(buf: &mut BytesMut, log_index: u64) {
        buf.put_u8(Self::TAG_COMMITTED);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 6); // pad
        buf.put_u64_le(log_index);
    }

    pub fn write_wills_fired(buf: &mut BytesMut, log_index: u64, count: u32) {
        buf.put_u8(Self::TAG_WILLS_FIRED);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 2); // pad
        buf.put_u32_le(count);
        buf.put_u64_le(log_index);
    }

    pub fn write_stats(buf: &mut BytesMut, log_index: u64, stats: &crate::types::EntityStats) {
        use crate::types::EntityStats;
        match stats {
            EntityStats::Topic {
                topic_id,
                message_count,
                head_index,
                tail_index,
            } => {
                buf.put_u8(Self::TAG_STATS);
                buf.put_u8(Self::STATUS_OK);
                buf.put_u8(0); // kind=Topic
                buf.put_bytes(0, 5); // pad
                buf.put_u64_le(log_index);
                buf.put_u64_le(*topic_id);
                buf.put_u64_le(*message_count);
                buf.put_u64_le(*head_index);
                buf.put_u64_le(*tail_index);
            }
            EntityStats::ConsumerGroup {
                group_id,
                variant,
                pending_count,
                in_flight_count,
                dlq_count,
                active_actor_count,
            } => {
                buf.put_u8(Self::TAG_STATS);
                buf.put_u8(Self::STATUS_OK);
                buf.put_u8(1); // kind=ConsumerGroup
                buf.put_bytes(0, 5); // pad
                buf.put_u64_le(log_index);
                buf.put_u64_le(*group_id);
                buf.put_u32_le(*variant as u32);
                buf.put_bytes(0, 4); // pad
                buf.put_u64_le(*pending_count);
                buf.put_u64_le(*in_flight_count);
                buf.put_u64_le(*dlq_count);
                buf.put_u64_le(*active_actor_count);
            }
        }
    }

    pub fn ok(log_index: u64) -> Self {
        let mut buf = BytesMut::with_capacity(16);
        Self::write_ok(&mut buf, log_index);
        Self::split_from(&mut buf)
    }

    pub fn published(log_index: u64, base_offset: u64, count: u64) -> Self {
        let mut buf = BytesMut::with_capacity(32);
        Self::write_published(&mut buf, log_index, base_offset, count);
        Self::split_from(&mut buf)
    }

    pub fn entity_created(log_index: u64, entity_id: u64) -> Self {
        let mut buf = BytesMut::with_capacity(24);
        Self::write_entity_created(&mut buf, log_index, entity_id);
        Self::split_from(&mut buf)
    }

    pub fn error(log_index: u64, error_code: u32, msg: &str) -> Self {
        let msg_bytes = msg.as_bytes();
        let padded_msg_len = (msg_bytes.len() + 7) & !7;
        let mut buf = BytesMut::with_capacity(24 + padded_msg_len);
        Self::write_error(&mut buf, log_index, error_code, msg);
        Self::split_from(&mut buf)
    }

    // Zero-copy accessors.
    #[inline]
    pub fn tag(&self) -> u8 {
        self.buf[0]
    }
    #[inline]
    pub fn status(&self) -> u8 {
        self.buf[1]
    }
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.buf[1] == Self::STATUS_OK
    }
    // SAFETY: all fixed-offset accessors below rely on ResponseEntry being
    // constructed by our own write_ok / write_published / etc. helpers, which
    // always produce buffers of sufficient size.  The `try_into().unwrap()`
    // calls on fixed slices are therefore structurally guaranteed to succeed.
    #[inline]
    pub fn log_index(&self) -> u64 {
        u64::from_le_bytes(self.buf[8..16].try_into().unwrap())
    }

    /// For TAG_PUBLISHED: base_offset at buf[16..24]
    pub fn base_offset(&self) -> u64 {
        u64::from_le_bytes(self.buf[16..24].try_into().unwrap())
    }

    /// For TAG_PUBLISHED: count at buf[24..32]
    pub fn published_count(&self) -> u64 {
        u64::from_le_bytes(self.buf[24..32].try_into().unwrap())
    }

    /// For TAG_ENTITY_CREATED: entity_id at buf[16..24]
    pub fn entity_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[16..24].try_into().unwrap())
    }

    /// For TAG_MESSAGES: message count at buf[2..4] as u16
    pub fn message_count(&self) -> u16 {
        u16::from_le_bytes(self.buf[2..4].try_into().unwrap())
    }

    /// For TAG_MESSAGES: iterate delivered messages
    pub fn messages(&self) -> impl Iterator<Item = crate::types::DeliveredMessage> + '_ {
        let count = self.message_count() as usize;
        let buf_len = self.buf.len();
        (0..count).filter_map(move |i| {
            let off = 16 + i * 32;
            if off + 32 > buf_len {
                return None;
            }
            Some(crate::types::DeliveredMessage {
                message_id: u64::from_le_bytes(self.buf[off..off + 8].try_into().unwrap()),
                original_timestamp: u64::from_le_bytes(
                    self.buf[off + 8..off + 16].try_into().unwrap(),
                ),
                group_id: u64::from_le_bytes(self.buf[off + 16..off + 24].try_into().unwrap()),
                attempt: u32::from_le_bytes(self.buf[off + 24..off + 28].try_into().unwrap()),
            })
        })
    }

    /// For TAG_ERROR: error_kind at buf[2]
    pub fn error_kind(&self) -> u8 {
        self.buf[2]
    }

    /// For TAG_ERROR (write_mq_error format): entity_id at buf[16..24]
    pub fn error_entity_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[16..24].try_into().unwrap())
    }

    /// For TAG_ERROR (write_mq_error format): error message string at buf[32..]
    pub fn error_message(&self) -> &str {
        let msg_len = u32::from_le_bytes(self.buf[24..28].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[32..32 + msg_len]).unwrap_or("")
    }

    /// For TAG_STATS ConsumerGroup: pending_count at buf[32..40]
    pub fn stats_pending_count(&self) -> u64 {
        u64::from_le_bytes(self.buf[32..40].try_into().unwrap())
    }

    /// For TAG_STATS ConsumerGroup: in_flight_count at buf[40..48]
    pub fn stats_in_flight_count(&self) -> u64 {
        u64::from_le_bytes(self.buf[40..48].try_into().unwrap())
    }

    /// For TAG_STATS ConsumerGroup: dlq_count at buf[48..56]
    pub fn stats_dlq_count(&self) -> u64 {
        u64::from_le_bytes(self.buf[48..56].try_into().unwrap())
    }

    /// For TAG_STATS: kind byte at buf[2]; 0 = Topic, 1 = ConsumerGroup
    pub fn stats_kind(&self) -> u8 {
        self.buf[2]
    }

    /// Returns true if this is TAG_ERROR with ERR_ALREADY_EXISTS kind
    pub fn is_already_exists(&self) -> bool {
        self.tag() == Self::TAG_ERROR && self.buf[2] == Self::ERR_ALREADY_EXISTS
    }

    /// For TAG_BATCH: sub-entry count at buf[2..4]
    pub fn batch_count(&self) -> u16 {
        u16::from_le_bytes(self.buf[2..4].try_into().unwrap())
    }

    /// For TAG_BATCH: iterate sub-entries
    pub fn batch_entries(&self) -> impl Iterator<Item = ResponseEntry> + '_ {
        let count = self.batch_count() as usize;
        let mut pos = 16usize; // after [tag:1][status:1][count:2][pad:4][log_index:8]
        let buf = self.buf.clone();
        let mut done = false;
        (0..count).filter_map(move |_| {
            if done {
                return None;
            }
            if pos + 8 > buf.len() {
                done = true;
                return None;
            }
            let entry_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 8; // skip [entry_len:4][pad:4]
            let padded = (entry_len + 7) & !7;
            if pos + padded > buf.len() {
                done = true;
                return None;
            }
            let entry_buf = buf.slice(pos..pos + entry_len);
            pos += padded;
            Some(ResponseEntry { buf: entry_buf })
        })
    }

    /// For TAG_DEAD_LETTERED: dlq_topic_id at buf[16..24]
    pub fn dlq_topic_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[16..24].try_into().unwrap())
    }

    /// For TAG_DEAD_LETTERED: count at buf[4..8]
    pub fn dead_letter_count(&self) -> u32 {
        u32::from_le_bytes(self.buf[4..8].try_into().unwrap())
    }

    /// For TAG_DEAD_LETTERED: iterate dead letter ids
    pub fn dead_letter_ids(&self) -> impl Iterator<Item = u64> + '_ {
        let count = self.dead_letter_count() as usize;
        let buf_len = self.buf.len();
        (0..count).filter_map(move |i| {
            let off = 24 + i * 8;
            if off + 8 > buf_len {
                return None;
            }
            Some(u64::from_le_bytes(
                self.buf[off..off + 8].try_into().unwrap(),
            ))
        })
    }

    /// For TAG_WILL_PENDING: session_id at buf[16..24]
    pub fn will_pending_session_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[16..24].try_into().unwrap())
    }

    /// For TAG_WILL_PENDING: delay_ms at buf[24..32]
    pub fn will_pending_delay_ms(&self) -> u64 {
        u64::from_le_bytes(self.buf[24..32].try_into().unwrap())
    }

    /// For TAG_WILLS_FIRED: count at buf[4..8]
    pub fn wills_fired_count(&self) -> u32 {
        u32::from_le_bytes(self.buf[4..8].try_into().unwrap())
    }

    /// For TAG_GROUP_SYNCED: phase_complete from buf[2]
    pub fn group_synced_phase_complete(&self) -> bool {
        self.buf[2] != 0
    }

    /// For TAG_GROUP_SYNCED: assignment bytes
    pub fn group_synced_assignment(&self) -> &[u8] {
        let len = types::read_u32_le(&self.buf, 16).unwrap_or(0) as usize;
        let end = 24 + len;
        if end > self.buf.len() {
            return &[];
        }
        &self.buf[24..end]
    }

    /// For TAG_GROUP_JOINED: generation from buf[4..8] as i32
    pub fn group_joined_generation(&self) -> i32 {
        i32::from_le_bytes(self.buf[4..8].try_into().unwrap())
    }

    /// For TAG_GROUP_JOINED: is_leader flag (bit 0 of buf[2])
    pub fn group_joined_is_leader(&self) -> bool {
        self.buf[2] & 1 != 0
    }

    /// For TAG_GROUP_JOINED: phase_complete flag (bit 1 of buf[2])
    pub fn group_joined_phase_complete(&self) -> bool {
        self.buf[2] & 2 != 0
    }

    /// For TAG_GROUP_JOINED: parse leader, member_id, protocol_name, members
    /// Returns (leader, member_id, protocol_name, members: Vec<(String, Bytes)>)
    pub fn group_joined_fields(&self) -> (String, String, String, Vec<(String, bytes::Bytes)>) {
        let defaults = (String::new(), String::new(), String::new(), Vec::new());
        if self.buf.len() < 24 {
            return defaults;
        }
        let leader_len = u16::from_le_bytes(self.buf[16..18].try_into().unwrap()) as usize;
        let member_id_len = u16::from_le_bytes(self.buf[18..20].try_into().unwrap()) as usize;
        let protocol_len = u16::from_le_bytes(self.buf[20..22].try_into().unwrap()) as usize;
        let member_count = u16::from_le_bytes(self.buf[22..24].try_into().unwrap()) as usize;

        let mut pos = 24usize;
        // Bounds-check the total string region before slicing.
        let total_strings = leader_len + member_id_len + protocol_len;
        if pos + total_strings > self.buf.len() {
            return defaults;
        }
        let leader = String::from_utf8_lossy(&self.buf[pos..pos + leader_len]).into_owned();
        pos += leader_len;
        let member_id = String::from_utf8_lossy(&self.buf[pos..pos + member_id_len]).into_owned();
        pos += member_id_len;
        let protocol_name =
            String::from_utf8_lossy(&self.buf[pos..pos + protocol_len]).into_owned();
        pos += protocol_len;
        // Pad to 8-byte boundary
        let strings_total = leader_len + member_id_len + protocol_len;
        let strings_pad = (8 - (strings_total % 8)) % 8;
        pos += strings_pad;

        let buf = &self.buf;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            if pos + 8 > buf.len() {
                break;
            }
            let mid_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            let meta_len = u32::from_le_bytes(buf[pos + 4..pos + 8].try_into().unwrap()) as usize;
            pos += 8;
            let mid_padded = (mid_len + 7) & !7;
            let meta_padded = (meta_len + 7) & !7;
            if pos + mid_padded + meta_padded > buf.len() {
                break;
            }
            let mid = String::from_utf8_lossy(&buf[pos..pos + mid_len]).into_owned();
            pos += mid_padded;
            let meta = self.buf.slice(pos..pos + meta_len);
            pos += meta_padded;
            members.push((mid, meta));
        }
        (leader, member_id, protocol_name, members)
    }

    /// For TAG_SESSION_RESTORED: session_id at buf[16..24]
    pub fn session_restored_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[16..24].try_into().unwrap())
    }

    /// For TAG_SESSION_RESTORED: session_expiry_ms at buf[24..32]
    pub fn session_restored_expiry_ms(&self) -> u64 {
        u64::from_le_bytes(self.buf[24..32].try_into().unwrap())
    }

    /// For TAG_SESSION_RESTORED: subscription_data bytes
    pub fn session_restored_subscription_data(&self) -> bytes::Bytes {
        let len = types::read_u32_le(&self.buf, 32).unwrap_or(0) as usize;
        let end = 40 + len;
        if end > self.buf.len() {
            return bytes::Bytes::new();
        }
        self.buf.slice(40..end)
    }

    /// For TAG_RETAINED_MESSAGES: parse all entries into a Vec.
    ///
    /// Binary layout: `[tag:1][pad:3][count:4][pad:8][entries...]`
    /// Each entry: `[rk_len:4][msg_len:4][rk_bytes (padded to 8)][msg_bytes (padded to 8)]`
    pub fn retained_messages(&self) -> impl Iterator<Item = crate::types::RetainedEntry> + '_ {
        let count = u32::from_le_bytes(self.buf[4..8].try_into().unwrap()) as usize;
        let mut pos = 16usize;
        let mut remaining = count;
        let buf = self.buf.clone();
        std::iter::from_fn(move || {
            if remaining == 0 {
                return None;
            }
            remaining -= 1;
            if pos + 8 > buf.len() {
                remaining = 0;
                return None;
            }
            let rk_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            let msg_len = u32::from_le_bytes(buf[pos + 4..pos + 8].try_into().unwrap()) as usize;
            pos += 8;
            let rk_padded = (rk_len + 7) & !7;
            let msg_padded = (msg_len + 7) & !7;
            if pos + rk_padded + msg_padded > buf.len() {
                remaining = 0;
                return None;
            }
            let routing_key = buf.slice(pos..pos + rk_len);
            pos += rk_padded;
            let message = buf.slice(pos..pos + msg_len);
            pos += msg_padded;
            Some(crate::types::RetainedEntry {
                routing_key,
                message,
            })
        })
    }

    /// Write a complete dispatch frame into `buf` without a separate allocation.
    ///
    /// Frame layout: `[len:4 LE][client_id:4 LE][ResponseEntry bytes...]`
    /// where `len` = `4 (client_id field) + ResponseEntry byte count`.
    pub fn encode_frame_into(buf: &mut BytesMut, client_id: u32, response: &ResponseEntry) {
        let len_pos = buf.len();
        buf.put_u32_le(0); // placeholder — filled after encoding the entry
        buf.put_u32_le(client_id);
        buf.put_slice(&response.buf);
        let payload_len = (buf.len() - len_pos - 4) as u32; // client_id(4) + entry bytes
        buf[len_pos..len_pos + 4].copy_from_slice(&payload_len.to_le_bytes());
    }

    /// Write a log-index-only dispatch frame into `buf` without a separate allocation.
    ///
    /// Frame layout: `[len:4=12 LE][client_id:4 LE][log_index:8 LE]`
    #[inline]
    pub fn encode_log_index_frame(buf: &mut BytesMut, client_id: u32, log_index: u64) {
        buf.put_u32_le(12u32); // len = client_id(4) + log_index(8)
        buf.put_u32_le(client_id);
        buf.put_u64_le(log_index);
    }
}

// =============================================================================
// Shared responder channel type
// =============================================================================

/// Per-follower responder tx table, indexed directly by node_id (< 64).
pub type ResponderTxVec = Box<[Option<crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>>; 64]>;

/// Message type for responder update notifications sent to [`PartitionWorker`]s.
pub type ResponderUpdateMsg = (
    u32,
    Option<crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>>,
);

/// Broadcasts follower connect/disconnect events to all [`PartitionWorker`]s.
///
/// Each worker owns its local responder table and drains updates from its
/// `UnboundedReceiver` at the top of each apply loop iteration — no shared
/// state, no locking on the hot path.
#[derive(Clone)]
pub struct ResponderBroadcast {
    txs: Vec<tokio::sync::mpsc::UnboundedSender<ResponderUpdateMsg>>,
}

impl ResponderBroadcast {
    /// Create a broadcast handle with no registered workers (useful for tests/benches).
    pub fn new_empty() -> Self {
        Self { txs: vec![] }
    }

    pub fn insert(&self, node_id: u32, tx: crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>) {
        for sender in &self.txs {
            let _ = sender.send((node_id, Some(tx.clone())));
        }
    }

    pub fn remove(&self, node_id: u32) {
        for sender in &self.txs {
            let _ = sender.send((node_id, None));
        }
    }
}

// =============================================================================
// ResponseCallback / ClientPartition
// =============================================================================

/// Callback invoked by a [`ClientPartition`] when a response frame arrives.
///
/// - `slab`: the owning `Bytes` buffer — zero-copy clone via
///   `slab.slice(offset..offset + message.len())` if you need to retain it.
/// - `offset`: byte offset of `message` within `slab`.
/// - `message`: response payload (`[payload...]` after `[len:4][client_id:4]`).
/// - `is_done`: `true` after the partition has exhausted the current drain cycle;
///   the callback can use this signal to flush or batch its own output.
pub type ResponseCallback = Arc<dyn Fn(&Bytes, usize, &[u8], bool) + Send + Sync + 'static>;

// =============================================================================
// Partition Inbox — lock-free slot-based inbox for Bytes
// =============================================================================

/// Capacity of each inbox segment in slots (number of [`Bytes`] objects).
const INBOX_SEG_CAPACITY: u32 = 4096;

/// Bit 31 of `write_pos` is set by the drainer to seal the segment.
const INBOX_SEALED_BIT: u32 = 1 << 31;

/// One fixed-capacity slot buffer for a single inbox partition.
///
/// Mirrors [`OutboundSeg`](crate::forward) but stores [`Bytes`] fat pointers
/// instead of raw bytes, so the response payload is never copied.
struct InboxSeg {
    data: std::cell::UnsafeCell<Vec<Bytes>>,
    write_pos: AtomicU32, // bit 31 = INBOX_SEALED_BIT
    committed: AtomicU32,
    capacity: u32,
}

// Safety: producers write to non-overlapping slots claimed via CAS;
// the drainer reads only after all producers have committed.
unsafe impl Send for InboxSeg {}
unsafe impl Sync for InboxSeg {}

impl InboxSeg {
    fn new(capacity: u32) -> Box<Self> {
        let mut v = Vec::with_capacity(capacity as usize);
        // Safety: we manage initialization manually via write_pos / committed.
        unsafe { v.set_len(capacity as usize) };
        Box::new(Self {
            data: std::cell::UnsafeCell::new(v),
            write_pos: AtomicU32::new(0),
            committed: AtomicU32::new(0),
            capacity,
        })
    }
}

/// Per-partition state: an atomically-swappable active segment.
struct InboxPartition {
    active: std::sync::atomic::AtomicPtr<InboxSeg>,
}

unsafe impl Send for InboxPartition {}
unsafe impl Sync for InboxPartition {}

/// Lock-free inbox waker — coalesces rapid pushes into one task wakeup.
struct InboxWaker {
    pending: AtomicBool,
    notify: tokio::sync::Notify,
}

impl InboxWaker {
    #[inline]
    fn wake(&self) {
        if !self.pending.swap(true, Ordering::Release) {
            self.notify.notify_waiters();
        }
    }
}

/// Lock-free [`Bytes`]-slot inbox used by [`ClientPartition`].
///
/// Producers call [`try_push`] / [`push`] lock-free (CAS + fat-pointer write).
/// The drain loop calls [`drain`] after [`wait_for_data`] wakes it, installing
/// a fresh segment before processing the old one — identical discipline to
/// [`OutboundBuf`](crate::forward::OutboundBuf).
pub(crate) struct PartitionInbox {
    partition: InboxPartition,
    waker: InboxWaker,
}

impl PartitionInbox {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            partition: InboxPartition {
                active: std::sync::atomic::AtomicPtr::new(Box::into_raw(InboxSeg::new(
                    INBOX_SEG_CAPACITY,
                ))),
            },
            waker: InboxWaker {
                pending: AtomicBool::new(false),
                notify: tokio::sync::Notify::new(),
            },
        })
    }

    /// Try to push a [`Bytes`] slot lock-free. Returns `false` on backpressure
    /// (segment sealed or full).
    #[inline]
    pub fn try_push(&self, bytes: Bytes) -> bool {
        self.try_push_or_return(bytes).is_ok()
    }

    /// Async push — yields on backpressure until the slot is accepted.
    #[inline]
    pub async fn push(&self, mut bytes: Bytes) {
        loop {
            match self.try_push_or_return(bytes) {
                Ok(()) => return,
                Err(b) => {
                    bytes = b;
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    /// Try to push; on failure return ownership of `bytes` so the caller can retry
    /// without cloning.
    #[inline]
    fn try_push_or_return(&self, bytes: Bytes) -> Result<(), Bytes> {
        loop {
            let seg_ptr = self.partition.active.load(Ordering::Acquire);
            let seg = unsafe { &*seg_ptr };
            let pos = seg.write_pos.load(Ordering::Relaxed);
            if pos & INBOX_SEALED_BIT != 0 || pos >= seg.capacity {
                return Err(bytes);
            }
            if seg
                .write_pos
                .compare_exchange(pos, pos + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                continue;
            }
            unsafe {
                (*seg.data.get())
                    .as_mut_ptr()
                    .add(pos as usize)
                    .write(bytes);
            }
            seg.committed.fetch_add(1, Ordering::Release);
            self.waker.wake();
            return Ok(());
        }
    }

    /// Wait for data to be available.
    async fn wait_for_data(&self) {
        let notified = self.waker.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.waker.pending.swap(false, Ordering::AcqRel) {
            notified.await;
            self.waker.pending.store(false, Ordering::Relaxed);
        }
    }

    /// Seal the segment, install a fresh one (unblocks producers immediately),
    /// spin-wait for in-flight writes, then return the drained [`Bytes`] slots.
    fn drain(&self) -> Vec<Bytes> {
        let old_ptr = self.partition.active.load(Ordering::Acquire);
        let old_seg = unsafe { &*old_ptr };

        let sealed = old_seg
            .write_pos
            .fetch_or(INBOX_SEALED_BIT, Ordering::AcqRel);
        let final_pos = sealed & !INBOX_SEALED_BIT;

        if final_pos == 0 {
            // Empty — unseal and reuse.
            old_seg
                .write_pos
                .fetch_and(!INBOX_SEALED_BIT, Ordering::Relaxed);
            return Vec::new();
        }

        // Install fresh segment so producers can proceed immediately.
        let new_seg = Box::into_raw(InboxSeg::new(INBOX_SEG_CAPACITY));
        self.partition.active.store(new_seg, Ordering::Release);

        // Spin-wait for all in-flight producers to commit.
        while old_seg.committed.load(Ordering::Acquire) < final_pos {
            std::hint::spin_loop();
        }

        // Extract the initialized slots (they are fully written now).
        let slots: Vec<Bytes> = unsafe {
            let v = &mut *old_seg.data.get();
            // Truncate to initialized range, then drain.
            v.set_len(final_pos as usize);
            std::mem::take(v)
        };

        // Drop the old segment (Vec is now empty; no double-drop).
        unsafe { drop(Box::from_raw(old_ptr)) };

        slots
    }
}

impl Drop for PartitionInbox {
    fn drop(&mut self) {
        // Drop the active segment and any initialized Bytes slots in it.
        let ptr = self.partition.active.load(Ordering::Acquire);
        let seg = unsafe { &mut *ptr };
        let pos = seg.write_pos.load(Ordering::Relaxed) & !INBOX_SEALED_BIT;
        unsafe {
            let v = &mut *seg.data.get();
            v.set_len(pos as usize);
            // Vec drops when seg is dropped below.
            drop(Box::from_raw(ptr));
        }
    }
}

enum ClientPartitionMsg {
    Register(u32, ResponseCallback),
    Unregister(u32),
}

/// Per-partition task that dispatches response frames to registered client callbacks.
///
/// Receives [`Bytes`] chunks via a [`PartitionInbox`]. Each chunk contains
/// one or more wire frames: `[len:4 LE][client_id:4 LE][payload...]`.
/// The task drains the inbox lock-free, invokes the matching callback for frames
/// belonging to its partition (`client_id % num_partitions == partition_id`), and
/// signals `is_done = true` to all touched callbacks after each drain cycle.
///
/// The callback map is owned exclusively by this task — no mutex required.
struct ClientPartition {
    partition_id: usize,
    num_partitions: usize,
    inbox: Arc<PartitionInbox>,
    reg_rx: tokio::sync::mpsc::UnboundedReceiver<ClientPartitionMsg>,
    callbacks: HashMap<u32, ResponseCallback>,
    seen_ids: HashSet<u32>,
}

impl ClientPartition {
    /// Scan `slab` for complete frames belonging to this partition and invoke callbacks.
    ///
    /// When `num_partitions > 1` the slab is the full read buffer broadcast to all workers;
    /// each worker skips records whose `client_id % num_partitions != partition_id`.  The slab
    /// is already in L1/L2 cache from the TCP read so the extra linear scan is cheap.
    fn dispatch(&mut self, slab: &Bytes) {
        if self.callbacks.is_empty() {
            return;
        }
        let n = self.num_partitions;
        let p = self.partition_id;
        let buf = slab.as_ref();
        let mut pos = 0;
        while pos + 8 <= buf.len() {
            let frame_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            // Minimum frame: client_id(4) + request_seq(8) + log_index(8) = 20.
            // Short frames are skipped.
            if frame_len < 20 {
                if pos + 4 + frame_len > buf.len() {
                    break;
                }
                pos += 4 + frame_len;
                continue;
            }
            if pos + 4 + frame_len > buf.len() {
                break;
            }
            let payload_start = pos + 4;
            let client_id =
                u32::from_le_bytes(buf[payload_start..payload_start + 4].try_into().unwrap());
            if n == 1 || (client_id as usize) % n == p {
                if let Some(cb) = self.callbacks.get(&client_id) {
                    // msg starts after client_id: [request_seq:8][log_index:8][response_bytes]
                    let msg_offset = payload_start + 4;
                    let msg_end = payload_start + frame_len;
                    cb(slab, msg_offset, &buf[msg_offset..msg_end], false);
                    self.seen_ids.insert(client_id);
                }
            }
            pos = payload_start + frame_len;
        }
    }

    fn handle_reg(&mut self, msg: ClientPartitionMsg) {
        match msg {
            ClientPartitionMsg::Register(id, cb) => {
                self.callbacks.insert(id, cb);
            }
            ClientPartitionMsg::Unregister(id) => {
                self.callbacks.remove(&id);
            }
        }
    }

    async fn run(mut self) {
        let empty = Bytes::new();
        loop {
            // Drain pending registrations without blocking.
            while let Ok(msg) = self.reg_rx.try_recv() {
                self.handle_reg(msg);
            }

            tokio::select! {
                biased;
                msg = self.reg_rx.recv() => {
                    match msg {
                        Some(m) => self.handle_reg(m),
                        None => return,
                    }
                }
                _ = self.inbox.wait_for_data() => {
                    // Yield once to let producers batch more slots before we seal.
                    tokio::task::yield_now().await;
                    let batch = self.inbox.drain();
                    for slab in &batch {
                        self.dispatch(slab);
                    }
                    // Signal all touched callbacks: drain cycle complete.
                    for &id in &self.seen_ids {
                        if let Some(cb) = self.callbacks.get(&id) {
                            cb(&empty, 0, &[], true);
                        }
                    }
                    self.seen_ids.clear();
                }
            }
        }
    }
}

// =============================================================================
// Client Registry
// =============================================================================

/// Registry of active client response partitions.
///
/// Each client is assigned to a [`ClientPartition`] by `client_id % num_partitions`.
/// The partition task owns the callback map for its shard — no mutex required.
/// Response frames are pushed via lock-free [`PartitionInbox`]es; registrations via
/// unbounded tokio channels.
pub struct ClientRegistry {
    next_id: AtomicU32,
    num_partitions: usize,
    inboxes: Vec<Arc<PartitionInbox>>,
    reg_txs: Vec<tokio::sync::mpsc::UnboundedSender<ClientPartitionMsg>>,
}

impl ClientRegistry {
    /// Create a new `ClientRegistry` with `num_partitions` client partitions,
    /// each backed by a live [`ClientPartition`] task.
    ///
    /// Spawns `num_partitions` tokio tasks — must be called from within a tokio runtime.
    pub fn new(num_partitions: usize, _capacity: usize) -> Arc<Self> {
        let n = num_partitions.max(1);
        let mut inboxes = Vec::with_capacity(n);
        let mut reg_txs = Vec::with_capacity(n);

        for i in 0..n {
            let inbox = PartitionInbox::new();
            let (reg_tx, reg_rx) = tokio::sync::mpsc::unbounded_channel::<ClientPartitionMsg>();
            inboxes.push(Arc::clone(&inbox));
            reg_txs.push(reg_tx);

            let partition = ClientPartition {
                partition_id: i,
                num_partitions: n,
                inbox,
                reg_rx,
                callbacks: HashMap::new(),
                seen_ids: HashSet::new(),
            };
            tokio::spawn(partition.run());
        }

        Arc::new(Self {
            next_id: AtomicU32::new(1),
            num_partitions: n,
            inboxes,
            reg_txs,
        })
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Register a client with a response callback. Returns the assigned `client_id`.
    pub fn register(&self, callback: ResponseCallback) -> u32 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let partition = (id as usize) % self.num_partitions;
        let _ = self.reg_txs[partition].send(ClientPartitionMsg::Register(id, callback));
        id
    }

    /// Unregister a client. The partition task drops the callback asynchronously.
    pub fn unregister(&self, client_id: u32) {
        let partition = (client_id as usize) % self.num_partitions;
        let _ = self.reg_txs[partition].send(ClientPartitionMsg::Unregister(client_id));
    }

    /// Push a raw [`Bytes`] chunk (complete wire frames) to a specific partition.
    ///
    /// Lock-free on the fast path (CAS slot claim). Yields on backpressure
    /// (inbox segment full) — callers in sync contexts should prefer the async
    /// variant.
    pub fn send_to_partition(&self, partition: usize, bytes: Bytes) {
        debug_assert!(partition < self.num_partitions);
        if partition < self.num_partitions {
            // try_push covers the common case; if the segment is momentarily
            // full/sealed we drop rather than block in a sync context.
            let _ = self.inboxes[partition].try_push(bytes);
        }
    }

    /// Async variant — lock-free on the fast path, yields on backpressure.
    pub async fn send_to_partition_async(&self, partition: usize, bytes: Bytes) {
        if partition < self.num_partitions {
            self.inboxes[partition].push(bytes).await;
        }
    }
}

// =============================================================================
// Client ID Table
// =============================================================================

/// Maps log_index → client_id for response routing.
///
/// Populated by `apply()` when client_id is available. Workers read
/// and remove after applying.
pub(crate) struct ClientIdTable {
    entries: papaya::HashMap<u64, u32>,
}

impl ClientIdTable {
    pub fn new() -> Self {
        Self {
            entries: papaya::HashMap::new(),
        }
    }

    #[allow(dead_code)]
    pub fn insert(&self, log_index: u64, client_id: u32) {
        self.entries.pin().insert(log_index, client_id);
    }

    pub fn take(&self, log_index: u64) -> Option<u32> {
        self.entries.pin().remove(&log_index).copied()
    }

    /// Acquire an owned (Send) epoch guard for reuse across multiple `take` calls.
    pub fn guard(
        &self,
    ) -> papaya::HashMapRef<
        '_,
        u64,
        u32,
        std::collections::hash_map::RandomState,
        papaya::OwnedGuard<'_>,
    > {
        self.entries.pin_owned()
    }
}

// =============================================================================
// Partition Worker
// =============================================================================

/// Per-worker state for the pull-based log consumer.
#[allow(dead_code)]
struct PartitionWorker {
    partition_id: usize,
    num_partitions: usize,
    cursor: Arc<AtomicU64>,
    hwm: Arc<HighWaterMark>,
    prefetcher: SegmentPrefetcher,
    engine: Arc<MqEngine>,
    client_registry: Arc<ClientRegistry>,
    client_id_table: Arc<ClientIdTable>,
    batch_registry: Arc<ParkingMutex<HashMap<u32, tokio::sync::oneshot::Sender<ResponseEntry>>>>,
    /// Owned per-follower responder tx table, indexed by node_id.
    /// Updated via `responder_update_rx` when followers connect/disconnect.
    responder_txs: Box<[Option<crossfire::MAsyncTx<crossfire::mpsc::Array<Bytes>>>; 64]>,
    /// Receives (node_id, Option<tx>) updates from [`ResponderBroadcast`].
    responder_update_rx: tokio::sync::mpsc::UnboundedReceiver<ResponderUpdateMsg>,
    /// Per-follower output buffers indexed directly by node_id (< 64).
    /// Tight-packed `[client_id:4 LE][log_index:8 LE]` records, 12 bytes each.
    /// Direct indexing: O(1) access, no search required.
    response_buf: Box<[BytesMut; 64]>,
    /// Bitmask of non-empty slots in `response_buf`. Bit `i` is set when
    /// `response_buf[i]` contains data. Enables O(popcount) flush iteration.
    response_buf_mask: u64,
    /// Number of forwarded sub-command entries accumulated since last flush.
    entries_since_flush: usize,
    /// Number of bytes accumulated across all `response_buf` entries since last flush.
    bytes_since_flush: usize,
    /// Flush after this many accumulated entries (from config).
    config_flush_entries: usize,
    /// Flush after this many accumulated bytes (from config).
    config_flush_bytes: usize,
    manifest: Option<Arc<MqManifestManager>>,
    /// Purged segment IDs shared with log storage. Workers peek (without
    /// draining) after each range to sweep retained messages on their own
    /// exchanges. Detach is idempotent so overlapping sweeps are safe.
    purged_segments: Option<Arc<ParkingMutex<Vec<u64>>>>,
    group_id: u64,
    cursor_notify: Arc<tokio::sync::Notify>,
    /// Shared table of per-node applied batch high-water marks.
    /// All partitions hold a reference; only partition 0 writes.
    applied_batch_table: Arc<AppliedBatchTable>,
    /// Per-node gap-aware trackers. Only partition 0 allocates these;
    /// other partitions leave this `None`.
    applied_trackers: Option<Box<[Option<AppliedBatchTracker>; 64]>>,
    m_apply_count: metrics::Counter,
    m_skip_count: metrics::Counter,
}

impl PartitionWorker {
    async fn run(mut self) {
        debug!(
            partition = self.partition_id,
            "async partition worker started"
        );

        // Persistent scanner — kept alive across ranges. The active segment
        // uses a live SegmentView that reads logical_size atomically, so the
        // cursor always sees newly appended data without re-acquiring.
        let mut scanner: Option<MqSegmentScanner> = None;

        loop {
            self.drain_responder_updates();
            let cursor_val = self.cursor.load(Ordering::Acquire);
            let hwm = self.hwm.wait_for(cursor_val).await;

            // Shutdown sentinel.
            if hwm == u64::MAX {
                debug!(
                    partition = self.partition_id,
                    "async partition worker shutting down"
                );
                return;
            }

            let from = cursor_val + 1;
            self.process_range(from, hwm, &mut scanner).await;
            self.sweep_purged_retained();
            self.cursor.store(hwm, Ordering::Release);
            self.cursor_notify.notify_waiters();
        }
    }

    fn drain_responder_updates(&mut self) {
        while let Ok((node_id, tx)) = self.responder_update_rx.try_recv() {
            self.responder_txs[node_id as usize] = tx;
        }
    }

    /// Flush accumulated forwarded-response bytes to the appropriate follower
    /// responder channels.  Called at end-of-range (and optionally mid-range).
    async fn flush_response_buf(&mut self) {
        if self.response_buf_mask == 0 {
            return;
        }

        let mut mask = self.response_buf_mask;
        self.response_buf_mask = 0;
        self.entries_since_flush = 0;
        self.bytes_since_flush = 0;

        while mask != 0 {
            let node_id = mask.trailing_zeros();
            mask &= mask - 1;
            let buf = &mut self.response_buf[node_id as usize];
            if buf.is_empty() {
                continue;
            }
            let bytes = buf.split().freeze();
            if let Some(tx) = &self.responder_txs[node_id as usize] {
                let _ = tx.send(bytes).await;
            }
        }
    }

    async fn process_range(&mut self, from: u64, to: u64, scanner: &mut Option<MqSegmentScanner>) {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // One epoch guard for the entire range instead of one per applied entry.
        // Clone the Arc so the guard's lifetime is independent of `self`, allowing
        // `&mut self` borrows (e.g. flush_response_buf) to coexist.
        let cid_table = Arc::clone(&self.client_id_table);
        let cid_guard = cid_table.guard();

        // Reuse persistent scanner if available, otherwise create a new one.
        // The scanner's active segment uses a live SegmentView, so it always
        // sees newly appended data — no stale snapshot issues.
        if scanner.is_none() {
            let start_seg = match self.prefetcher.segment_id_for(from) {
                Some(seg) => seg,
                None => return, // entry purged or not yet written
            };
            *scanner = Some(MqSegmentScanner::new(self.prefetcher.clone(), start_seg));
        }
        // SAFETY: set to Some in the block above; the early-return guards the None case.
        let scan = scanner.as_mut().unwrap();

        let mut apply_count = 0u64;
        let mut skip_count = 0u64;

        loop {
            let Some(rec) = scan.next_record_raw() else {
                break; // segment(s) exhausted
            };

            // Skip entries before our range (mid-segment start).
            if rec.log_index < from {
                continue;
            }
            // Stop when past our range — put back for the next call.
            if rec.log_index > to {
                scan.put_back(rec);
                break;
            }

            let cmd = rec.command;

            // TAG_BATCH: designated worker (log_index % num_partitions == partition_id)
            // applies the full batch and delivers response via batch_registry.
            if cmd.tag() == MqCommand::TAG_BATCH {
                if (rec.log_index as usize) % self.num_partitions == self.partition_id {
                    let segment_id = scan.current_segment_id().unwrap_or(0);
                    let batch_id = match cmd.as_batch() {
                        Ok(b) => b.batch_id(),
                        Err(_) => {
                            continue;
                        }
                    };
                    let mut resp_buf = BytesMut::new();
                    self.engine.apply_command(
                        &cmd,
                        &mut resp_buf,
                        rec.log_index,
                        current_time,
                        Some(segment_id),
                    );
                    let response = ResponseEntry::split_from(&mut resp_buf);
                    self.handle_structural_writes(&cmd, &response, rec.log_index);
                    // Deliver to batcher via registry.
                    if batch_id != 0 {
                        if let Some(tx) = self.batch_registry.lock().remove(&batch_id) {
                            let _ = tx.send(response);
                        }
                    }
                }
                continue;
            }

            // TAG_FORWARDED_BATCH: accumulate response frames for follower responders
            // (by client_id % N), and apply sub-commands to the engine (by primary_id % N).
            if cmd.tag() == MqCommand::TAG_FORWARDED_BATCH {
                let view = match cmd.as_forwarded_batch() {
                    Ok(v) => v,
                    Err(_) => {
                        continue;
                    }
                };
                let node_id = view.node_id();
                debug_assert!(
                    (node_id as usize) < 64,
                    "node_id {node_id} exceeds response_buf capacity"
                );
                // Track applied batch_seq per-node (partition 0 only).
                // When the HWM advances, publish to the shared atomic table.
                if let Some(ref mut trackers) = self.applied_trackers {
                    let tracker =
                        trackers[node_id as usize].get_or_insert_with(AppliedBatchTracker::new);
                    if let Some(new_hwm) = tracker.record(view.batch_seq()) {
                        self.applied_batch_table.publish(node_id, new_hwm);
                    }
                }
                // Response routing: accumulate frames for sub-commands owned by this partition.
                for (client_id, request_seq, _) in view.iter() {
                    if (client_id as usize) % self.num_partitions == self.partition_id {
                        let buf = &mut self.response_buf[node_id as usize];
                        // Frame: [len:4][client_id:4][request_seq:8][log_index:8]
                        buf.put_u32_le(20u32); // 4 (client_id) + 8 (request_seq) + 8 (log_index)
                        buf.put_u32_le(client_id);
                        buf.put_u64_le(request_seq);
                        buf.put_u64_le(rec.log_index);
                        self.response_buf_mask |= 1u64 << node_id;
                        self.entries_since_flush += 1;
                        self.bytes_since_flush += 24;
                    }
                }
                if self.entries_since_flush >= self.config_flush_entries
                    || self.bytes_since_flush >= self.config_flush_bytes
                {
                    self.flush_response_buf().await;
                }
                // Engine apply: apply sub-commands owned by this partition (by primary_id % N).
                for (_, _, sub_cmd_bytes) in view.iter() {
                    let sub_cmd = MqCommand::from_bytes(cmd.buf.slice_ref(sub_cmd_bytes));
                    if sub_cmd.tag() == MqCommand::TAG_RESUME {
                        continue;
                    }
                    if !self.should_process(&sub_cmd, rec.log_index) {
                        continue;
                    }
                    let segment_id = scan.current_segment_id().unwrap_or(0);
                    let mut resp_buf = BytesMut::new();
                    self.engine.apply_command(
                        &sub_cmd,
                        &mut resp_buf,
                        rec.log_index,
                        current_time,
                        Some(segment_id),
                    );
                    let response = ResponseEntry::split_from(&mut resp_buf);
                    self.handle_structural_writes(&sub_cmd, &response, rec.log_index);
                }
                continue;
            }

            // Route check: does this entry belong to my partition?
            if !self.should_process(&cmd, rec.log_index) {
                skip_count += 1;
                continue;
            }

            // Segment location from the cursor — no index lookup needed.
            let segment_id = match scan.current_segment_id() {
                Some(id) => id,
                None => continue,
            };

            // Apply command.
            let mut resp_buf = BytesMut::new();
            self.engine.apply_command(
                &cmd,
                &mut resp_buf,
                rec.log_index,
                current_time,
                Some(segment_id),
            );
            let response = ResponseEntry::split_from(&mut resp_buf);

            // Post-apply: structural writes to MDBX.
            self.handle_structural_writes(&cmd, &response, rec.log_index);

            // Deliver response to the owning ClientPartition.
            if let Some(client_id) = cid_guard.remove(&rec.log_index).copied() {
                let partition = (client_id as usize) % self.num_partitions;
                let mut frame = BytesMut::with_capacity(4 + 4 + 32);
                ResponseEntry::encode_frame_into(&mut frame, client_id, &response);
                self.client_registry
                    .send_to_partition(partition, frame.freeze());
            }

            apply_count += 1;
        }

        if apply_count > 0 {
            self.m_apply_count.increment(apply_count);
        }
        if skip_count > 0 {
            self.m_skip_count.increment(skip_count);
        }

        // End-of-range flush: always send any remaining response bytes.
        self.flush_response_buf().await;
    }

    /// Determine if this worker should process the given command.
    #[inline]
    fn should_process(&self, cmd: &MqCommand, log_index: u64) -> bool {
        match cmd.tag() {
            // Structural: assigned by log_index for even distribution.
            MqCommand::TAG_CREATE_TOPIC
            | MqCommand::TAG_DELETE_TOPIC
            | MqCommand::TAG_CREATE_EXCHANGE
            | MqCommand::TAG_DELETE_EXCHANGE
            | MqCommand::TAG_CREATE_CONSUMER_GROUP
            | MqCommand::TAG_DELETE_CONSUMER_GROUP
            | MqCommand::TAG_CREATE_BINDING
            | MqCommand::TAG_DELETE_BINDING
            | MqCommand::TAG_CREATE_SESSION
            | MqCommand::TAG_DISCONNECT_SESSION => {
                (log_index as usize) % self.num_partitions == self.partition_id
            }

            // Data-plane: assigned by primary_id for entity co-location.
            _ => match cmd.try_primary_id() {
                Ok(id) => (id as usize) % self.num_partitions == self.partition_id,
                Err(_) => false, // malformed command — skip
            },
        }
    }

    /// Sweep retained messages on this worker's exchanges and topics that
    /// reference purged mmap segments. Copies the message bytes to heap so
    /// the segment can be freed. Only sweeps entities owned by this partition
    /// (`entity_id % num_partitions == partition_id`).
    fn sweep_purged_retained(&self) {
        let purged = match self.purged_segments {
            Some(ref p) => p,
            None => return,
        };

        // Peek — don't drain. The state machine drains after all workers
        // have caught up. Detach is idempotent so double-sweeps are harmless.
        let purged_set: HashSet<u64> = {
            let guard = purged.lock();
            if guard.is_empty() {
                return;
            }
            guard.iter().copied().collect()
        };

        let meta = self.engine.metadata();

        // Sweep exchange retained messages.
        let exchanges_guard = meta.exchanges.pin();
        for (&exchange_id, exchange) in exchanges_guard.iter() {
            if (exchange_id as usize) % self.num_partitions != self.partition_id {
                continue;
            }

            let mut retained = exchange.retained.write();
            let mut detached = false;
            for rv in retained.values_mut() {
                if let Some(seg_id) = rv.segment_id {
                    if purged_set.contains(&seg_id) {
                        rv.detach();
                        detached = true;
                    }
                }
            }

            if detached {
                if let Some(ref manifest) = self.manifest {
                    let entries: Vec<(String, Vec<u8>)> = retained
                        .iter()
                        .map(|(k, v)| (k.clone(), v.message.to_vec()))
                        .collect();
                    manifest.persist_retained_fire_and_forget(self.group_id, exchange_id, entries);
                }
            }
        }

        // Sweep topic retained messages.
        let topics_guard = meta.topics.pin();
        for (&topic_id, topic) in topics_guard.iter() {
            if !topic.meta.retained {
                continue;
            }
            if (topic_id as usize) % self.num_partitions != self.partition_id {
                continue;
            }

            let mut retained = topic.retained_message.write();
            if let Some(ref mut rv) = *retained {
                if let Some(seg_id) = rv.segment_id {
                    if purged_set.contains(&seg_id) {
                        rv.detach();
                    }
                }
            }
        }
    }

    fn handle_structural_writes(&self, cmd: &MqCommand, response: &ResponseEntry, log_index: u64) {
        let manifest = match self.manifest {
            Some(ref m) => m,
            None => return,
        };

        let kind = classify_structural(cmd);
        if let Some(writes) = collect_structural_writes(self.engine.metadata(), response, kind) {
            let next_id = self.engine.metadata().next_id.load(Ordering::Relaxed);
            for w in writes {
                manifest.structural_update_fire_and_forget(self.group_id, log_index, next_id, w);
            }
        }
    }
}

// =============================================================================
// Async Apply Manager
// =============================================================================

/// Coordinates pull-based partition workers and the high-water mark.
///
/// Created during state machine initialization. The state machine's
/// `apply()` advances the HWM; workers wake and pull from the log.
#[allow(dead_code)]
pub struct AsyncApplyManager {
    pub(crate) hwm: Arc<HighWaterMark>,
    pub(crate) client_registry: Arc<ClientRegistry>,
    pub(crate) client_id_table: Arc<ClientIdTable>,
    worker_cursors: Vec<Arc<AtomicU64>>,
    worker_handles: Vec<tokio::task::JoinHandle<()>>,
    cursor_notify: Arc<tokio::sync::Notify>,
    num_partitions: usize,
    /// Shared with log storage — workers peek after each range to sweep
    /// retained messages on their own exchanges. The state machine drains
    /// after all workers have caught up.
    purged_segments: Option<Arc<ParkingMutex<Vec<u64>>>>,
    /// Broadcasts follower connect/disconnect events to all partition workers.
    responder_broadcast: ResponderBroadcast,
    /// Registry for TAG_BATCH response delivery. Batcher registers before raft write,
    /// designated worker sends response after apply.
    pub(crate) batch_registry:
        Arc<ParkingMutex<HashMap<u32, tokio::sync::oneshot::Sender<ResponseEntry>>>>,
    batch_id_counter: Arc<AtomicU32>,
    /// Per-node applied batch high-water marks. Readable by anyone — e.g.
    /// the follower's `ForwardClient` can poll its own slot to know which
    /// `batch_seq` values have been committed through raft.
    applied_batch_table: Arc<AppliedBatchTable>,
}

impl AsyncApplyManager {
    /// Create the manager and spawn partition workers + client partitions.
    pub fn new(
        config: &ParallelApplyConfig,
        engine: Arc<MqEngine>,
        prefetcher: SegmentPrefetcher,
        manifest: Option<Arc<MqManifestManager>>,
        purged_segments: Option<Arc<ParkingMutex<Vec<u64>>>>,
        group_id: u64,
        initial_cursor: u64,
        catalog_name: &str,
    ) -> Self {
        let n = config.num_partitions;
        let hwm = Arc::new(HighWaterMark::new(initial_cursor));
        let client_id_table = Arc::new(ClientIdTable::new());
        let cursor_notify = Arc::new(tokio::sync::Notify::new());
        let mut responder_update_txs = Vec::with_capacity(n);
        let mut responder_update_rxs: Vec<
            tokio::sync::mpsc::UnboundedReceiver<ResponderUpdateMsg>,
        > = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            responder_update_txs.push(tx);
            responder_update_rxs.push(rx);
        }
        let responder_broadcast = ResponderBroadcast {
            txs: responder_update_txs,
        };

        // Build ClientRegistry inboxes and spawn ClientPartition tasks.
        let mut inboxes = Vec::with_capacity(n);
        let mut reg_txs = Vec::with_capacity(n);

        for i in 0..n {
            let inbox = PartitionInbox::new();
            let (reg_tx, reg_rx) = tokio::sync::mpsc::unbounded_channel::<ClientPartitionMsg>();
            inboxes.push(Arc::clone(&inbox));
            reg_txs.push(reg_tx);

            let partition = ClientPartition {
                partition_id: i,
                num_partitions: n,
                inbox,
                reg_rx,
                callbacks: HashMap::new(),
                seen_ids: HashSet::new(),
            };
            tokio::spawn(partition.run());
        }

        let client_registry = Arc::new(ClientRegistry {
            next_id: AtomicU32::new(1),
            num_partitions: n,
            inboxes,
            reg_txs,
        });

        let batch_registry: Arc<
            ParkingMutex<HashMap<u32, tokio::sync::oneshot::Sender<ResponseEntry>>>,
        > = Arc::new(ParkingMutex::new(HashMap::new()));
        let batch_id_counter = Arc::new(AtomicU32::new(1));

        let applied_batch_table = Arc::new(AppliedBatchTable::new());

        let mut worker_cursors = Vec::with_capacity(n);
        let mut worker_handles = Vec::with_capacity(n);

        for i in 0..n {
            let cursor = Arc::new(AtomicU64::new(initial_cursor));
            let partition_label = i.to_string();
            let catalog = catalog_name.to_owned();
            let m_apply_count = metrics::counter!(
                "mq.async_worker.apply_count",
                "catalog" => catalog.clone(),
                "partition" => partition_label.clone()
            );
            let m_skip_count = metrics::counter!(
                "mq.async_worker.skip_count",
                "catalog" => catalog,
                "partition" => partition_label
            );

            let worker = PartitionWorker {
                partition_id: i,
                num_partitions: n,
                cursor: Arc::clone(&cursor),
                hwm: Arc::clone(&hwm),
                prefetcher: prefetcher.clone(),
                engine: Arc::clone(&engine),
                client_registry: Arc::clone(&client_registry),
                client_id_table: Arc::clone(&client_id_table),
                batch_registry: Arc::clone(&batch_registry),
                responder_txs: Box::new(std::array::from_fn(|_| None)),
                responder_update_rx: responder_update_rxs.remove(0),
                response_buf: Box::new(std::array::from_fn(|_| BytesMut::new())),
                response_buf_mask: 0,
                entries_since_flush: 0,
                bytes_since_flush: 0,
                config_flush_entries: config.response_flush_entries,
                config_flush_bytes: config.response_flush_bytes,
                manifest: manifest.clone(),
                purged_segments: purged_segments.clone(),
                group_id,
                cursor_notify: Arc::clone(&cursor_notify),
                applied_batch_table: Arc::clone(&applied_batch_table),
                applied_trackers: if i == 0 {
                    Some(Box::new(std::array::from_fn(|_| None)))
                } else {
                    None
                },
                m_apply_count,
                m_skip_count,
            };

            let handle = tokio::spawn(worker.run());
            worker_cursors.push(cursor);
            worker_handles.push(handle);
        }

        Self {
            hwm,
            client_registry,
            client_id_table,
            worker_cursors,
            worker_handles,
            cursor_notify,
            num_partitions: n,
            purged_segments,
            responder_broadcast,
            batch_registry,
            batch_id_counter,
            applied_batch_table,
        }
    }

    pub fn responder_broadcast(&self) -> ResponderBroadcast {
        self.responder_broadcast.clone()
    }

    /// Shared table of per-node applied batch high-water marks.
    /// The follower can poll `table.high_water(my_node_id)` to know which
    /// `batch_seq` values have been committed through raft.
    pub fn applied_batch_table(&self) -> &Arc<AppliedBatchTable> {
        &self.applied_batch_table
    }

    /// Allocate a batch_id and register a response sender. Call BEFORE raft write.
    pub fn alloc_batch(&self, tx: tokio::sync::oneshot::Sender<ResponseEntry>) -> u32 {
        let id = self.batch_id_counter.fetch_add(1, Ordering::Relaxed);
        self.batch_registry.lock().insert(id, tx);
        id
    }

    /// Advance the HWM to the given index.
    #[inline]
    pub fn advance_hwm(&self, new_hwm: u64) {
        self.hwm.advance(new_hwm);
    }

    /// Advance the HWM and wait for all workers to reach it.
    pub async fn advance_and_wait(&self, target: u64) {
        self.hwm.advance(target);
        self.wait_for_workers(target).await;
    }

    /// Wait for all workers to reach at least `target`.
    pub async fn wait_for_workers(&self, target: u64) {
        loop {
            let notified = self.cursor_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.min_worker_cursor() >= target {
                return;
            }
            notified.await;
        }
    }

    /// Barrier: ensure all workers have processed up to `target`.
    pub async fn barrier(&self, target: u64) {
        self.advance_and_wait(target).await;
    }

    /// Returns the minimum cursor across all workers.
    pub fn min_worker_cursor(&self) -> u64 {
        self.worker_cursors
            .iter()
            .map(|c| c.load(Ordering::Acquire))
            .min()
            .unwrap_or(0)
    }

    /// Drain the purged segments vec. Call after all workers have had a
    /// chance to sweep (i.e., after their cursors have advanced past the
    /// HWM at which the segments were purged).
    pub fn drain_purged_segments(&self) {
        if let Some(ref purged) = self.purged_segments {
            let mut guard = purged.lock();
            if !guard.is_empty() {
                guard.clear();
            }
        }
    }

    /// Number of partition workers.
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Get a reference to the client registry.
    pub fn client_registry(&self) -> &Arc<ClientRegistry> {
        &self.client_registry
    }

    /// Shutdown all workers.
    pub async fn shutdown(&mut self) {
        self.hwm.advance(u64::MAX);
        for handle in self.worker_handles.drain(..) {
            let _ = handle.await;
        }
    }
}

// =============================================================================
// Adapter Integration (Phase 4)
// =============================================================================

/// Tracks in-flight requests for a single client connection.
///
/// Each adapter connection maintains one of these. After proposing a command
/// through Raft, the adapter stores the request context here keyed by the
/// returned `log_index`. When a `ResponseEntry` arrives on the client channel
/// matching that `log_index`, the adapter removes the pending request and
/// constructs the protocol-specific response.
pub struct PendingRequests {
    requests: std::collections::HashMap<u64, PendingRequest>,
}

/// Protocol-specific request context stored while awaiting a response.
pub enum PendingRequest {
    /// MQTT publish awaiting PUBACK.
    Publish { packet_id: u16, qos: u8 },
    /// AMQP disposition awaiting settlement.
    Disposition { delivery_id: u32 },
    /// Kafka produce awaiting partition response.
    ProducePartition { topic: Arc<str>, partition: i32 },
    /// Entity creation (topic, exchange, consumer group, etc.).
    CreateEntity { name: Arc<str> },
    /// Generic request with no protocol-specific metadata.
    Generic,
}

impl PendingRequests {
    pub fn new() -> Self {
        Self {
            requests: std::collections::HashMap::new(),
        }
    }

    /// Register a pending request. Called after `raft.client_write()` returns
    /// the `log_index`.
    pub fn insert(&mut self, log_index: u64, request: PendingRequest) {
        self.requests.insert(log_index, request);
    }

    /// Remove and return a pending request. Called when a `ResponseEntry`
    /// arrives matching the `log_index`.
    pub fn remove(&mut self, log_index: u64) -> Option<PendingRequest> {
        self.requests.remove(&log_index)
    }

    /// Number of in-flight requests.
    pub fn len(&self) -> usize {
        self.requests.len()
    }

    /// Whether there are no in-flight requests.
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }
}

impl Default for PendingRequests {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- HighWaterMark tests ----

    #[tokio::test]
    async fn hwm_immediate_return_when_ahead() {
        let hwm = HighWaterMark::new(10);
        // cursor=5, HWM=10 → should return immediately
        let val = hwm.wait_for(5).await;
        assert_eq!(val, 10);
    }

    #[tokio::test]
    async fn hwm_waits_then_wakes() {
        let hwm = Arc::new(HighWaterMark::new(0));
        let hwm2 = Arc::clone(&hwm);

        let handle = tokio::spawn(async move {
            // cursor=0, HWM=0 → should block
            hwm2.wait_for(0).await
        });

        // Give the task time to register the waiter.
        tokio::task::yield_now().await;

        // Advance HWM — should wake the waiter.
        hwm.advance(5);

        let val = handle.await.unwrap();
        assert_eq!(val, 5);
    }

    #[tokio::test]
    async fn hwm_coalescing() {
        let hwm = Arc::new(HighWaterMark::new(0));
        let hwm2 = Arc::clone(&hwm);

        let handle = tokio::spawn(async move { hwm2.wait_for(0).await });

        tokio::task::yield_now().await;

        // Multiple advances before wake — should coalesce.
        hwm.advance(1);
        hwm.advance(2);
        hwm.advance(5);
        hwm.advance(10);

        let val = handle.await.unwrap();
        assert_eq!(val, 10);
    }

    #[test]
    fn hwm_current() {
        let hwm = HighWaterMark::new(42);
        assert_eq!(hwm.current(), 42);
        hwm.advance(100);
        assert_eq!(hwm.current(), 100);
    }

    // ---- ResponseEntry tests ----

    #[test]
    fn response_entry_ok() {
        let entry = ResponseEntry::ok(42);
        assert_eq!(entry.tag(), ResponseEntry::TAG_OK);
        assert!(entry.is_ok());
        assert_eq!(entry.log_index(), 42);
        assert_eq!(entry.buf.len(), 16);
    }

    #[test]
    fn response_entry_published() {
        let entry = ResponseEntry::published(100, 50, 10);
        assert_eq!(entry.tag(), ResponseEntry::TAG_PUBLISHED);
        assert!(entry.is_ok());
        assert_eq!(entry.log_index(), 100);
        assert_eq!(entry.buf.len(), 32);

        // Read base_offset and count.
        let base_offset = u64::from_le_bytes(entry.buf[16..24].try_into().unwrap());
        let count = u64::from_le_bytes(entry.buf[24..32].try_into().unwrap());
        assert_eq!(base_offset, 50);
        assert_eq!(count, 10);
    }

    #[test]
    fn response_entry_entity_created() {
        let entry = ResponseEntry::entity_created(200, 999);
        assert_eq!(entry.tag(), ResponseEntry::TAG_ENTITY_CREATED);
        assert!(entry.is_ok());
        assert_eq!(entry.log_index(), 200);

        let entity_id = u64::from_le_bytes(entry.buf[16..24].try_into().unwrap());
        assert_eq!(entity_id, 999);
    }

    #[test]
    fn response_entry_error() {
        let entry = ResponseEntry::error(300, 42, "not found");
        assert_eq!(entry.tag(), ResponseEntry::TAG_ERROR);
        assert_eq!(entry.status(), ResponseEntry::STATUS_ERROR);
        assert!(!entry.is_ok());
        assert_eq!(entry.log_index(), 300);

        let error_code = u32::from_le_bytes(entry.buf[16..20].try_into().unwrap());
        let msg_len = u32::from_le_bytes(entry.buf[20..24].try_into().unwrap()) as usize;
        assert_eq!(error_code, 42);
        assert_eq!(msg_len, 9);
        assert_eq!(&entry.buf[24..24 + msg_len], b"not found");
        // Verify 8-byte alignment.
        assert_eq!(entry.buf.len() % 8, 0);
    }

    #[test]
    fn response_entry_from_mq_response() {
        let entry = ResponseEntry::ok(1);
        assert_eq!(entry.tag(), ResponseEntry::TAG_OK);

        let entry = ResponseEntry::published(2, 10, 5);
        assert_eq!(entry.tag(), ResponseEntry::TAG_PUBLISHED);
        assert_eq!(entry.log_index(), 2);

        let entry = ResponseEntry::entity_created(3, 77);
        assert_eq!(entry.tag(), ResponseEntry::TAG_ENTITY_CREATED);
    }

    #[test]
    fn response_entry_8byte_alignment() {
        // All response sizes should be 8-byte aligned.
        assert_eq!(ResponseEntry::ok(0).buf.len() % 8, 0);
        assert_eq!(ResponseEntry::published(0, 0, 0).buf.len() % 8, 0);
        assert_eq!(ResponseEntry::entity_created(0, 0).buf.len() % 8, 0);
        assert_eq!(ResponseEntry::error(0, 0, "").buf.len() % 8, 0);
        assert_eq!(ResponseEntry::error(0, 0, "x").buf.len() % 8, 0);
        assert_eq!(ResponseEntry::error(0, 0, "exactly8!").buf.len() % 8, 0);
    }

    // ---- ClientRegistry tests ----

    #[tokio::test]
    async fn client_registry_register_and_dispatch() {
        use std::sync::atomic::AtomicU64;

        let registry = ClientRegistry::new(2, 64);

        let log_idx = Arc::new(AtomicU64::new(0));
        let done = Arc::new(tokio::sync::Notify::new());
        let log_idx_cb = Arc::clone(&log_idx);
        let done_cb = Arc::clone(&done);

        let cb: ResponseCallback = Arc::new(move |_slab, _offset, msg, is_done| {
            if !is_done && msg.len() >= 16 {
                // ResponseEntry::ok layout: [tag:1][status:1][pad:6][log_index:8]
                let li = u64::from_le_bytes(msg[8..16].try_into().unwrap());
                log_idx_cb.store(li, Ordering::Relaxed);
                done_cb.notify_one();
            }
        });
        let id = registry.register(cb);
        assert!(id >= 1);

        let p = (id as usize) % registry.num_partitions();
        let mut frame = BytesMut::with_capacity(24);
        let ok_entry = ResponseEntry::ok(42);
        ResponseEntry::encode_frame_into(&mut frame, id, &ok_entry);
        registry.send_to_partition(p, frame.freeze());
        done.notified().await;
        assert_eq!(log_idx.load(Ordering::Relaxed), 42);

        registry.unregister(id);
    }

    #[tokio::test]
    async fn client_registry_multiple_clients() {
        let registry = ClientRegistry::new(2, 64);
        let total = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let done = Arc::new(tokio::sync::Notify::new());

        let mut ids = Vec::new();
        for _ in 0..4 {
            let total_cb = Arc::clone(&total);
            let done_cb = Arc::clone(&done);
            let cb: ResponseCallback = Arc::new(move |_slab, _offset, _msg, is_done| {
                if !is_done {
                    if total_cb.fetch_add(1, Ordering::Relaxed) + 1 >= 4 {
                        done_cb.notify_one();
                    }
                }
            });
            ids.push(registry.register(cb));
        }
        // IDs should be unique.
        let id_set: std::collections::HashSet<u32> = ids.iter().copied().collect();
        assert_eq!(id_set.len(), 4);

        for &id in &ids {
            let p = (id as usize) % registry.num_partitions();
            let mut frame = BytesMut::with_capacity(24);
            let ok_entry = ResponseEntry::ok(1);
            ResponseEntry::encode_frame_into(&mut frame, id, &ok_entry);
            registry.send_to_partition(p, frame.freeze());
        }
        done.notified().await;
        assert_eq!(total.load(Ordering::Relaxed), 4);

        for id in ids {
            registry.unregister(id);
        }
    }

    // ---- ClientIdTable tests ----

    #[test]
    fn client_id_table_insert_take() {
        let table = ClientIdTable::new();
        table.insert(100, 42);
        table.insert(200, 43);

        assert_eq!(table.take(100), Some(42));
        assert_eq!(table.take(100), None); // already taken
        assert_eq!(table.take(200), Some(43));
        assert_eq!(table.take(300), None);
    }

    #[test]
    fn client_id_table_overwrite() {
        let table = ClientIdTable::new();
        table.insert(100, 1);
        table.insert(100, 2); // overwrite
        assert_eq!(table.take(100), Some(2));
    }

    // ---- PendingRequests tests ----

    #[test]
    fn pending_requests_lifecycle() {
        let mut pending = PendingRequests::new();
        assert!(pending.is_empty());
        assert_eq!(pending.len(), 0);

        pending.insert(
            10,
            PendingRequest::Publish {
                packet_id: 1,
                qos: 1,
            },
        );
        pending.insert(11, PendingRequest::Disposition { delivery_id: 42 });
        pending.insert(12, PendingRequest::Generic);
        assert_eq!(pending.len(), 3);
        assert!(!pending.is_empty());

        match pending.remove(10) {
            Some(PendingRequest::Publish { packet_id, qos }) => {
                assert_eq!(packet_id, 1);
                assert_eq!(qos, 1);
            }
            _ => panic!("expected Publish"),
        }

        match pending.remove(11) {
            Some(PendingRequest::Disposition { delivery_id }) => {
                assert_eq!(delivery_id, 42);
            }
            _ => panic!("expected Disposition"),
        }

        assert!(pending.remove(10).is_none()); // already removed
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn pending_requests_all_variants() {
        let mut pending = PendingRequests::new();
        pending.insert(
            1,
            PendingRequest::Publish {
                packet_id: 100,
                qos: 2,
            },
        );
        pending.insert(2, PendingRequest::Disposition { delivery_id: 200 });
        pending.insert(
            3,
            PendingRequest::ProducePartition {
                topic: Arc::from("test-topic"),
                partition: 3,
            },
        );
        pending.insert(
            4,
            PendingRequest::CreateEntity {
                name: Arc::from("my-queue"),
            },
        );
        pending.insert(5, PendingRequest::Generic);
        assert_eq!(pending.len(), 5);

        match pending.remove(3) {
            Some(PendingRequest::ProducePartition { topic, partition }) => {
                assert_eq!(&*topic, "test-topic");
                assert_eq!(partition, 3);
            }
            _ => panic!("expected ProducePartition"),
        }

        match pending.remove(4) {
            Some(PendingRequest::CreateEntity { name }) => {
                assert_eq!(&*name, "my-queue");
            }
            _ => panic!("expected CreateEntity"),
        }
    }

    #[test]
    fn pending_requests_default() {
        let pending = PendingRequests::default();
        assert!(pending.is_empty());
    }

    // ---- ResponseEntry extended tests ----

    #[test]
    fn response_entry_from_error() {
        use crate::types::{EntityKind, MqError};
        let err = MqError::NotFound {
            entity: EntityKind::Topic,
            id: 42,
        };
        let mut buf = BytesMut::new();
        ResponseEntry::write_mq_error(&mut buf, 99, &err);
        let entry = ResponseEntry::split_from(&mut buf);
        assert_eq!(entry.tag(), ResponseEntry::TAG_ERROR);
        assert!(!entry.is_ok());
        assert_eq!(entry.log_index(), 99);
        assert_eq!(entry.buf.len() % 8, 0);
    }

    #[test]
    fn response_entry_error_empty_message() {
        let entry = ResponseEntry::error(1, 0, "");
        assert_eq!(entry.tag(), ResponseEntry::TAG_ERROR);
        assert_eq!(entry.log_index(), 1);
        let msg_len = u32::from_le_bytes(entry.buf[20..24].try_into().unwrap()) as usize;
        assert_eq!(msg_len, 0);
        assert_eq!(entry.buf.len() % 8, 0);
    }

    #[test]
    fn response_entry_error_long_message() {
        let long_msg = "a]".repeat(500);
        let entry = ResponseEntry::error(2, 99, &long_msg);
        assert_eq!(entry.tag(), ResponseEntry::TAG_ERROR);
        assert_eq!(entry.log_index(), 2);
        let error_code = u32::from_le_bytes(entry.buf[16..20].try_into().unwrap());
        assert_eq!(error_code, 99);
        let msg_len = u32::from_le_bytes(entry.buf[20..24].try_into().unwrap()) as usize;
        assert_eq!(msg_len, long_msg.len());
        assert_eq!(&entry.buf[24..24 + msg_len], long_msg.as_bytes());
        assert_eq!(entry.buf.len() % 8, 0);
    }

    // ---- HighWaterMark extended tests ----

    #[tokio::test]
    async fn hwm_shutdown_sentinel() {
        let hwm = Arc::new(HighWaterMark::new(0));
        let hwm2 = Arc::clone(&hwm);

        let handle = tokio::spawn(async move { hwm2.wait_for(0).await });

        tokio::task::yield_now().await;

        // Advance with shutdown sentinel.
        hwm.advance(u64::MAX);

        let val = handle.await.unwrap();
        assert_eq!(val, u64::MAX);
    }

    #[tokio::test]
    async fn hwm_multiple_waiters() {
        let hwm = Arc::new(HighWaterMark::new(0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let hwm2 = Arc::clone(&hwm);
            handles.push(tokio::spawn(async move { hwm2.wait_for(0).await }));
        }

        tokio::task::yield_now().await;
        hwm.advance(42);

        for h in handles {
            assert_eq!(h.await.unwrap(), 42);
        }
    }

    #[tokio::test]
    async fn hwm_sequential_advances() {
        let hwm = Arc::new(HighWaterMark::new(0));
        let hwm2 = Arc::clone(&hwm);

        // First wait.
        hwm.advance(5);
        let val = hwm2.wait_for(0).await;
        assert_eq!(val, 5);

        // Second wait from new cursor.
        let hwm3 = Arc::clone(&hwm);
        let handle = tokio::spawn(async move { hwm3.wait_for(5).await });
        tokio::task::yield_now().await;
        hwm.advance(10);
        assert_eq!(handle.await.unwrap(), 10);
    }

    // ---- ClientRegistry extended tests ----

    #[tokio::test]
    async fn client_registry_ids_are_unique() {
        let registry = ClientRegistry::new(2, 64);
        let noop: ResponseCallback = Arc::new(|_, _, _, _| {});
        let id1 = registry.register(Arc::clone(&noop));
        let id2 = registry.register(Arc::clone(&noop));
        let id3 = registry.register(Arc::clone(&noop));
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[tokio::test]
    async fn client_registry_send_multiple_responses() {
        use std::sync::atomic::AtomicUsize;
        let registry = ClientRegistry::new(2, 64);
        let count = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(tokio::sync::Notify::new());
        let count_cb = Arc::clone(&count);
        let done_cb = Arc::clone(&done);
        let cb: ResponseCallback = Arc::new(move |_slab, _offset, _msg, is_done| {
            if !is_done {
                if count_cb.fetch_add(1, Ordering::Relaxed) + 1 >= 10 {
                    done_cb.notify_one();
                }
            }
        });
        let id = registry.register(cb);
        let p = (id as usize) % registry.num_partitions();
        for i in 0..10u64 {
            let mut frame = BytesMut::with_capacity(24);
            let ok_entry = ResponseEntry::ok(i);
            ResponseEntry::encode_frame_into(&mut frame, id, &ok_entry);
            registry.send_to_partition(p, frame.freeze());
        }
        done.notified().await;
        assert_eq!(count.load(Ordering::Relaxed), 10);
        registry.unregister(id);
    }

    // ---- Worker routing tests ----

    /// Helper to test routing logic without constructing a PartitionWorker.
    fn route_partition(cmd: &MqCommand, log_index: u64, num_partitions: usize) -> usize {
        match cmd.tag() {
            MqCommand::TAG_CREATE_TOPIC
            | MqCommand::TAG_DELETE_TOPIC
            | MqCommand::TAG_CREATE_EXCHANGE
            | MqCommand::TAG_DELETE_EXCHANGE
            | MqCommand::TAG_CREATE_CONSUMER_GROUP
            | MqCommand::TAG_DELETE_CONSUMER_GROUP
            | MqCommand::TAG_CREATE_BINDING
            | MqCommand::TAG_DELETE_BINDING
            | MqCommand::TAG_CREATE_SESSION
            | MqCommand::TAG_DISCONNECT_SESSION => (log_index as usize) % num_partitions,
            _ => (cmd.primary_id() as usize) % num_partitions,
        }
    }

    #[test]
    fn worker_routing_structural_by_log_index() {
        let num_partitions = 4;

        // CREATE_TOPIC at log_index=9 → 9 % 4 = 1
        let mut buf = BytesMut::new();
        MqCommand::write_create_topic(
            &mut buf,
            "test",
            &crate::types::RetentionPolicy::default(),
            1,
        );
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(route_partition(&cmd, 9, num_partitions), 1);
    }

    #[test]
    fn worker_routing_data_plane_by_primary_id() {
        let num_partitions = 4;

        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 7, &[]);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(route_partition(&cmd, 0, num_partitions), 3); // 7 % 4 = 3

        MqCommand::write_publish_bytes(&mut buf, 8, &[]);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(route_partition(&cmd, 0, num_partitions), 0); // 8 % 4 = 0
    }

    #[test]
    fn worker_routing_all_structural_tags() {
        let n = 4;
        let mut buf = BytesMut::new();
        MqCommand::write_create_topic(&mut buf, "t", &crate::types::RetentionPolicy::default(), 0);
        let cmd_create_topic = MqCommand::split_from(&mut buf);
        MqCommand::write_delete_topic(&mut buf, 1);
        let cmd_delete_topic = MqCommand::split_from(&mut buf);
        MqCommand::write_create_exchange(&mut buf, "e", crate::types::ExchangeType::Fanout);
        let cmd_create_exchange = MqCommand::split_from(&mut buf);
        MqCommand::write_delete_exchange(&mut buf, 1);
        let cmd_delete_exchange = MqCommand::split_from(&mut buf);
        MqCommand::write_create_consumer_group(&mut buf, "g", 1);
        let cmd_create_consumer_group = MqCommand::split_from(&mut buf);
        MqCommand::write_delete_consumer_group(&mut buf, 1);
        let cmd_delete_consumer_group = MqCommand::split_from(&mut buf);
        MqCommand::write_create_session(&mut buf, 1, "c", 30000, 0);
        let cmd_create_session = MqCommand::split_from(&mut buf);
        MqCommand::write_disconnect_session(&mut buf, 1, false);
        let cmd_disconnect_session = MqCommand::split_from(&mut buf);
        let structural_cmds = vec![
            cmd_create_topic,
            cmd_delete_topic,
            cmd_create_exchange,
            cmd_delete_exchange,
            cmd_create_consumer_group,
            cmd_delete_consumer_group,
            cmd_create_session,
            cmd_disconnect_session,
        ];

        for cmd in &structural_cmds {
            // Structural commands at different log indices should spread across partitions.
            let mut seen = vec![false; n];
            for log_index in 0..n as u64 {
                seen[route_partition(cmd, log_index, n)] = true;
            }
            assert!(
                seen.iter().all(|&s| s),
                "tag {} should reach all partitions via log_index routing",
                cmd.tag()
            );
        }
    }

    #[test]
    fn worker_routing_data_plane_deterministic() {
        let n = 8;
        let mut buf = BytesMut::new();
        for topic_id in 0..100u64 {
            MqCommand::write_publish_bytes(&mut buf, topic_id, &[]);
            let cmd = MqCommand::split_from(&mut buf);
            let p1 = route_partition(&cmd, 1, n);
            MqCommand::write_publish_bytes(&mut buf, topic_id, &[]);
            let cmd = MqCommand::split_from(&mut buf);
            let p2 = route_partition(&cmd, 999, n);
            // Same primary_id always maps to same partition regardless of log_index.
            assert_eq!(
                p1, p2,
                "data-plane routing should be independent of log_index"
            );
        }
    }

    #[test]
    fn worker_routing_batch_not_structural() {
        // TAG_BATCH should NOT match any structural tag, so it falls through
        // to data-plane routing (which workers skip entirely for batch).
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 1, &[]);
        let inner = MqCommand::split_from(&mut buf);
        MqCommand::write_batch(&mut buf, &[inner]);
        let batch = MqCommand::split_from(&mut buf);
        assert_eq!(batch.tag(), MqCommand::TAG_BATCH);

        // Verify TAG_BATCH is not in the structural set.
        let p = route_partition(&batch, 100, 4);
        // Should use primary_id routing, not log_index.
        let expected = (batch.primary_id() as usize) % 4;
        assert_eq!(p, expected);
    }

    #[test]
    fn worker_routing_commit_offset_by_primary_id() {
        let mut buf = BytesMut::new();
        MqCommand::write_commit_offset(&mut buf, 42, 1, 100);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(route_partition(&cmd, 0, 8), (42usize) % 8);
    }

    #[test]
    fn worker_routing_single_partition() {
        // With 1 partition, everything goes to partition 0.
        let mut buf = BytesMut::new();
        MqCommand::write_publish_bytes(&mut buf, 999, &[]);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(route_partition(&cmd, 123, 1), 0);

        MqCommand::write_create_topic(&mut buf, "t", &crate::types::RetentionPolicy::default(), 0);
        let cmd = MqCommand::split_from(&mut buf);
        assert_eq!(route_partition(&cmd, 456, 1), 0);
    }
}

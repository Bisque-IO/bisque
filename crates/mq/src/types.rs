use std::collections::HashMap;
use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// CRC64-NVME hash of a name string, used as the key in name→ID lookup maps.
#[inline]
pub fn name_hash(name: &str) -> u64 {
    name_hash_bytes(name.as_bytes())
}

/// CRC64-NVME hash of raw bytes. Use when the input is already `&[u8]` or
/// `Bytes` to avoid a UTF-8 validation / String allocation round-trip.
#[inline]
pub fn name_hash_bytes(b: &[u8]) -> u64 {
    crc_fast::crc64_nvme(b)
}

// =============================================================================
// MqApplyResponse
// =============================================================================

/// Minimal raft apply acknowledgement. The log index is all callers need
/// to correlate with their pending request. Actual engine results are
/// delivered via the worker → ClientRegistry pipeline.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MqApplyResponse {
    pub log_index: u64,
}

// =============================================================================
// Delivered Message
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveredMessage {
    /// Topic offset or raft log index of this message.
    pub message_id: u64,
    pub attempt: u32,
    pub original_timestamp: u64,
    /// Consumer group ID this message was delivered from.
    #[serde(default)]
    pub group_id: u64,
}

// =============================================================================
// Entity Identifiers
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityType {
    Topic,
    Exchange,
    ConsumerGroup,
    Session,
}

// =============================================================================
// Exchange Types (for AMQP/MQTT routing)
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeType {
    /// Delivers to all bound topics (fanout).
    Fanout,
    /// Delivers to topics whose binding key exactly matches the routing key.
    Direct,
    /// Delivers to topics whose binding pattern matches the routing key
    /// using AMQP-style wildcards (`*` = one word, `#` = zero or more words).
    /// Also supports MQTT-style (`+` = one level, `#` = multi-level).
    Topic,
}

impl Default for ExchangeType {
    fn default() -> Self {
        Self::Fanout
    }
}

/// Notification emitted when a message is published to an exchange.
/// Used for QoS 0 fan-out to avoid polling latency.
#[derive(Debug, Clone)]
pub struct ExchangePublishNotification {
    pub exchange_id: u64,
    /// The flat message bytes published.
    pub messages: SmallVec<[Bytes; 8]>,
}

/// A binding from an exchange to a target topic with an optional routing key pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Binding {
    pub binding_id: u64,
    pub exchange_id: u64,
    pub target_topic_id: u64,
    /// Routing key pattern. For direct exchanges this is a literal match.
    /// For topic exchanges this supports `*` and `#` wildcards.
    #[serde(default)]
    pub routing_key: Option<String>,
    /// MQTT no-local flag — prevents messages from being delivered to the publisher.
    #[serde(default)]
    pub no_local: bool,
    /// MQTT shared subscription group name (if shared).
    #[serde(default)]
    pub shared_group: Option<String>,
    /// MQTT 5.0 subscription identifier (§3.8.2.1.2).
    #[serde(default)]
    pub subscription_id: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentRange {
    pub segment_id: u64,
    pub record_count: u64,
    pub total_bytes: u64,
}

// =============================================================================
// Topic Partitioning
// =============================================================================

/// Status of a single partition within a partitioned topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionStatus {
    /// Partition is actively accepting writes and serving reads.
    Active,
    /// Partition is draining (no new writes, consumers finishing).
    Draining,
    /// Partition is inactive (data may still exist but no I/O).
    Inactive,
}

/// Metadata for a single partition of a partitioned topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    /// Zero-based partition index within the topic.
    pub partition_index: u32,
    /// Raft group ID that owns this partition's data.
    pub group_id: u64,
    /// Current partition status.
    pub status: PartitionStatus,
}

// =============================================================================
// Retention Policy
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    #[serde(default)]
    pub max_age_secs: Option<u64>,
    #[serde(default)]
    pub max_bytes: Option<u64>,
    #[serde(default)]
    pub max_messages: Option<u64>,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age_secs: None,
            max_bytes: None,
            max_messages: None,
        }
    }
}

// =============================================================================
// Topic Configuration
// =============================================================================

/// Topic lifetime policy — determines when a topic is auto-deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopicLifetimePolicy {
    /// Topic lives until explicitly deleted.
    Permanent,
    /// Topic is deleted when the last consumer group detaches.
    DeleteOnLastDetach,
}

impl Default for TopicLifetimePolicy {
    fn default() -> Self {
        Self::Permanent
    }
}

/// Dedup configuration for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicDedupConfig {
    /// Dedup window duration in seconds.
    pub window_secs: u64,
    /// Maximum number of dedup entries before inline GC is triggered.
    /// Default: 100_000.
    #[serde(default = "default_dedup_max_entries")]
    pub max_entries: u64,
}

fn default_dedup_max_entries() -> u64 {
    100_000
}

/// Cron auto-publish configuration for a topic. Replaces the Job entity type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicCronConfig {
    /// Cron expression (6-field: sec min hour day month weekday).
    pub cron_expression: String,
    /// Timezone for cron evaluation (default: "UTC").
    #[serde(default = "default_timezone")]
    pub timezone: String,
    /// Max pending trigger messages before skipping (overlap policy).
    #[serde(default = "default_max_pending")]
    pub max_pending: u32,
    /// Optional payload bytes for the auto-published trigger message.
    #[serde(default)]
    pub payload: Option<Bytes>,
}

fn default_timezone() -> String {
    "UTC".to_string()
}

fn default_max_pending() -> u32 {
    10
}

// =============================================================================
// Consumer Group Variant Types
// =============================================================================

/// Consumer group variant — set at creation time, immutable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum GroupVariant {
    /// Kafka-compatible offset tracking.
    Offset = 0,
    /// Per-message ack/nack/release/modify (queue + job semantics).
    Ack = 1,
    /// Per-actor-key serialized delivery (mailbox semantics).
    Actor = 2,
}

impl Default for GroupVariant {
    fn default() -> Self {
        Self::Offset
    }
}

/// Per-message state in an Ack-variant consumer group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageState {
    Pending,
    InFlight,
    Acked,
    DeadLetter,
    /// QoS 2: PUBREC sent, message confirmed received.
    Received,
    /// QoS 2: PUBREL received, message released for completion.
    Released,
}

/// Per-message metadata in an Ack-variant consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckMessageMeta {
    /// Message ID = topic offset.
    pub message_id: u64,
    /// Consumer group ID this message belongs to.
    pub group_id: u64,
    pub state: MessageState,
    #[serde(default)]
    pub priority: u8,
    #[serde(default)]
    pub deliver_after: u64,
    pub attempts: u32,
    #[serde(default)]
    pub last_delivered_at: Option<u64>,
    #[serde(default)]
    pub consumer_id: Option<u64>,
    #[serde(default)]
    pub visibility_deadline: Option<u64>,
    #[serde(default)]
    pub dedup_key: Option<Bytes>,
    /// Absolute timestamp (ms) when this message expires regardless of retries.
    #[serde(default)]
    pub expires_at: Option<u64>,
    /// Topic name to publish a response to on ACK (request/reply pattern).
    #[serde(default)]
    pub reply_to: Option<Bytes>,
    /// Correlation ID for request/reply matching.
    #[serde(default)]
    pub correlation_id: Option<Bytes>,
    /// Value payload size in bytes, stored for byte-level accounting.
    #[serde(default)]
    pub value_len: u32,
    /// Publisher session ID for no-local filtering (MQTT 5.0).
    /// 0 = not set (no filtering).
    #[serde(default)]
    pub publisher_id: u64,
}

/// Ack variant configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckVariantConfig {
    #[serde(default = "default_visibility_timeout_ms")]
    pub visibility_timeout_ms: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// DLQ topic name. None = auto "{group}/dlq" when max_retries > 0.
    #[serde(default)]
    pub dead_letter_topic: Option<String>,
    #[serde(default)]
    pub delay_default_ms: u64,
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight_per_consumer: u32,
    /// Back-pressure: max unprocessed messages before rejecting publishes.
    #[serde(default)]
    pub max_pending_messages: Option<u64>,
    /// Back-pressure: max unprocessed bytes before rejecting publishes.
    #[serde(default)]
    pub max_pending_bytes: Option<u64>,
    /// Max total delayed messages in the delay index before rejecting.
    #[serde(default)]
    pub max_delayed_messages: Option<u64>,
    /// Max total delayed bytes in the delay index before rejecting.
    #[serde(default)]
    pub max_delayed_bytes: Option<u64>,
}

fn default_visibility_timeout_ms() -> u64 {
    30_000
}
fn default_max_retries() -> u32 {
    3
}
fn default_max_in_flight() -> u32 {
    100
}

impl Default for AckVariantConfig {
    fn default() -> Self {
        Self {
            visibility_timeout_ms: default_visibility_timeout_ms(),
            max_retries: default_max_retries(),
            dead_letter_topic: None,
            delay_default_ms: 0,
            max_in_flight_per_consumer: default_max_in_flight(),
            max_pending_messages: None,
            max_pending_bytes: None,
            max_delayed_messages: None,
            max_delayed_bytes: None,
        }
    }
}

/// Actor variant configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorVariantConfig {
    #[serde(default = "default_max_mailbox_depth")]
    pub max_mailbox_depth: u32,
    #[serde(default = "default_idle_eviction_secs")]
    pub idle_eviction_secs: u64,
    #[serde(default = "default_actor_ack_timeout_ms")]
    pub ack_timeout_ms: u64,
    #[serde(default = "default_actor_max_retries")]
    pub max_retries: u32,
    /// Back-pressure: max pending messages per actor before rejecting.
    #[serde(default)]
    pub max_pending_messages: Option<u64>,
    /// Back-pressure: max pending bytes per actor before rejecting.
    #[serde(default)]
    pub max_pending_bytes: Option<u64>,
}

fn default_max_mailbox_depth() -> u32 {
    10_000
}
fn default_idle_eviction_secs() -> u64 {
    3600
}
fn default_actor_ack_timeout_ms() -> u64 {
    30_000
}
fn default_actor_max_retries() -> u32 {
    3
}

impl Default for ActorVariantConfig {
    fn default() -> Self {
        Self {
            max_mailbox_depth: default_max_mailbox_depth(),
            idle_eviction_secs: default_idle_eviction_secs(),
            ack_timeout_ms: default_actor_ack_timeout_ms(),
            max_retries: default_actor_max_retries(),
            max_pending_messages: None,
            max_pending_bytes: None,
        }
    }
}

/// Variant-specific configuration (set at creation, immutable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VariantConfig {
    Offset,
    Ack(AckVariantConfig),
    Actor(ActorVariantConfig),
}

impl Default for VariantConfig {
    fn default() -> Self {
        Self::Offset
    }
}

// =============================================================================
// Session Types (unified, replaces ConsumerState + ProducerMeta)
// =============================================================================

/// Protocol-agnostic will message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WillConfig {
    pub topic_id: u64,
    pub payload: Bytes,
    #[serde(default)]
    pub delay_ms: u64,
    #[serde(default)]
    pub retained: bool,
}

/// A session subscription entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionSubscription {
    pub entity_type: EntityType,
    pub entity_id: u64,
}

/// Per-consumer topic alias entry (MQTT 5.0).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicAliasEntry {
    pub alias: u16,
    pub topic_name: String,
}

/// A will message waiting to be fired after a delay (MQTT 5.0 Will Delay Interval).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingWill {
    pub session_id: u64,
    pub client_id: String,
    pub will: WillConfig,
    /// Absolute timestamp (ms) when the will should fire.
    pub fire_at_ms: u64,
}

/// Unified name index entry — single namespace for all entity names.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NameEntry {
    pub name: String,
    pub topic_id: u64,
    /// Consumer group ID if this name is a consumer group (topic + group pair).
    #[serde(default)]
    pub consumer_group_id: Option<u64>,
}

// =============================================================================
// Entity Stats
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntityStats {
    Topic {
        topic_id: u64,
        message_count: u64,
        head_index: u64,
        tail_index: u64,
    },
    ConsumerGroup {
        group_id: u64,
        variant: GroupVariant,
        /// Ack variant: pending count.
        pending_count: u64,
        /// Ack variant: in-flight count.
        in_flight_count: u64,
        /// Ack variant: DLQ count.
        dlq_count: u64,
        /// Actor variant: active actor count.
        active_actor_count: u64,
    },
}

// =============================================================================
// MqCommand — zero-copy view over flat-encoded command buffer
// =============================================================================

/// Command type for the MQ engine.
///
/// Wraps a flat-encoded binary buffer (`Bytes`). On the write path, `write_*`
/// functions write fields directly into a caller-provided `BytesMut`, and
/// `MqCommand::split_from` splits the result out zero-copy. On the read path,
/// the buffer is decoded from the raft log zero-copy.
///
/// Zero-copy command wrapper over a flat binary buffer.
///
/// Most commands are a single contiguous `Bytes` (`extra` is empty).
/// The local batcher creates **scattered** commands where `header` holds
/// the 32-byte TAG_FORWARDED_BATCH header inline (no allocation) and
/// `extra` holds the sub-frame slabs — the Raft log writer scatter-writes
/// them to disk via [`BorrowPayload::extra_payload_segments`] with zero
/// copies of payload bytes.
///
/// Discriminant: `extra.is_empty()` → contiguous, else scattered.
///
/// Per-variant accessor structs (in `codec`) provide typed APIs over the
/// raw buffer.  They are only used on the read path (contiguous commands
/// from the Raft log); scattered commands are write-only.
#[derive(Clone)]
pub struct MqCommand {
    /// Full command bytes for contiguous commands; empty for scattered.
    pub(crate) buf: Bytes,
    /// Inline 32-byte header for scattered commands (no allocation).
    /// Zeroed and unused for contiguous commands.
    pub(crate) header: [u8; 32],
    /// Extra payload segments for scattered (vectored) writes.
    /// Empty for contiguous commands (the common case — no heap allocation).
    pub(crate) extra: Vec<Bytes>,
}

// Tag constants for MqCommand discriminants — unified allocation.
impl MqCommand {
    // -- Topics (0-5) --
    pub const TAG_CREATE_TOPIC: u8 = 0;
    pub const TAG_DELETE_TOPIC: u8 = 1;
    pub const TAG_PUBLISH: u8 = 2;
    pub const TAG_COMMIT_OFFSET: u8 = 3;
    pub const TAG_PURGE_TOPIC: u8 = 4;
    pub const TAG_SET_RETAINED: u8 = 5;

    // -- Exchanges (6-10) --
    pub const TAG_CREATE_EXCHANGE: u8 = 6;
    pub const TAG_DELETE_EXCHANGE: u8 = 7;
    pub const TAG_CREATE_BINDING: u8 = 8;
    pub const TAG_DELETE_BINDING: u8 = 9;
    pub const TAG_PUBLISH_TO_EXCHANGE: u8 = 10;

    // -- Consumer Groups (11-18) --
    pub const TAG_CREATE_CONSUMER_GROUP: u8 = 11;
    pub const TAG_DELETE_CONSUMER_GROUP: u8 = 12;
    pub const TAG_JOIN_CONSUMER_GROUP: u8 = 13;
    pub const TAG_SYNC_CONSUMER_GROUP: u8 = 14;
    pub const TAG_LEAVE_CONSUMER_GROUP: u8 = 15;
    pub const TAG_HEARTBEAT_CONSUMER_GROUP: u8 = 16;
    pub const TAG_COMMIT_GROUP_OFFSET: u8 = 17;
    pub const TAG_EXPIRE_GROUP_SESSIONS: u8 = 18;

    // -- Ack Variant (19-29) --
    pub const TAG_GROUP_DELIVER: u8 = 19;
    pub const TAG_GROUP_ACK: u8 = 20;
    pub const TAG_GROUP_NACK: u8 = 21;
    pub const TAG_GROUP_RELEASE: u8 = 22;
    pub const TAG_GROUP_MODIFY: u8 = 23;
    pub const TAG_GROUP_EXTEND_VISIBILITY: u8 = 24;
    pub const TAG_GROUP_TIMEOUT_EXPIRED: u8 = 25;
    pub const TAG_GROUP_PUBLISH_TO_DLQ: u8 = 26;
    pub const TAG_GROUP_EXPIRE_PENDING: u8 = 27;
    pub const TAG_GROUP_PURGE: u8 = 28;
    pub const TAG_GROUP_GET_ATTRIBUTES: u8 = 29;

    // -- Actor Variant (30-35) --
    pub const TAG_GROUP_DELIVER_ACTOR: u8 = 30;
    pub const TAG_GROUP_ACK_ACTOR: u8 = 31;
    pub const TAG_GROUP_NACK_ACTOR: u8 = 32;
    pub const TAG_GROUP_ASSIGN_ACTORS: u8 = 33;
    pub const TAG_GROUP_RELEASE_ACTORS: u8 = 34;
    pub const TAG_GROUP_EVICT_IDLE: u8 = 35;

    // -- Cron (36-39) --
    pub const TAG_CRON_ENABLE: u8 = 36;
    pub const TAG_CRON_DISABLE: u8 = 37;
    pub const TAG_CRON_TRIGGER: u8 = 38;
    pub const TAG_CRON_UPDATE: u8 = 39;

    // -- Sessions (40-48) --
    pub const TAG_CREATE_SESSION: u8 = 40;
    pub const TAG_DISCONNECT_SESSION: u8 = 41;
    pub const TAG_HEARTBEAT_SESSION: u8 = 42;
    pub const TAG_SET_WILL: u8 = 43;
    pub const TAG_CLEAR_WILL: u8 = 44;
    pub const TAG_FIRE_PENDING_WILLS: u8 = 45;
    pub const TAG_PERSIST_SESSION: u8 = 46;
    pub const TAG_RESTORE_SESSION: u8 = 47;
    pub const TAG_EXPIRE_SESSIONS: u8 = 48;

    // -- Retained --
    pub const TAG_GET_RETAINED: u8 = 51;
    pub const TAG_DELETE_RETAINED: u8 = 52;

    // -- Batch --
    pub const TAG_BATCH: u8 = 49;

    // -- Dedup --
    pub const TAG_PRUNE_DEDUP_WINDOW: u8 = 50;

    // -- MQTT slice variants --
    pub const TAG_PUBLISH_TO_EXCHANGE_MQTT: u8 = 53;
    pub const TAG_SET_RETAINED_MQTT: u8 = 54;

    // -- Forwarded batch (55) --
    /// A batch of commands forwarded from a follower node to the leader.
    ///
    /// Wire format (all 8-byte aligned):
    /// ```text
    /// @0  [size:4][fixed:2][tag:1][flags:1]   MqCommand header
    /// @8  node_id:    u32                     originating follower node
    /// @12 count:      u32                     number of sub-frames
    /// @16 batch_seq:  u64                     follower-assigned dedup sequence
    /// @24 leader_seq: u64                     leader-assigned total-order sequence
    /// @32 Frame 0: [payload_len:4][client_id:4][request_seq:8][cmd_bytes + opt_pad]
    /// @.. Frame 1: [payload_len:4][client_id:4][request_seq:8][cmd_bytes + opt_pad]
    /// ```
    ///
    /// Sub-frame `payload_len` = `12 + cmd_bytes.len()` (client_id:4 + request_seq:8 + cmd,
    /// excludes payload_len field itself). Each sub-frame may be padded to 8-byte alignment
    /// when built by the write_batcher (not by the TCP forwarding path).
    pub const TAG_FORWARDED_BATCH: u8 = 55;

    // -- Resume (56) --

    /// Client session resume command, sent as the first forwarded sub-frame after reconnect.
    ///
    /// Wire layout (16 bytes):
    /// ```text
    /// [TAG_RESUME:1][pad:3][session_client_id:4][last_acked_seq:8]
    /// ```
    pub const TAG_RESUME: u8 = 56;
}

/// Command header offsets.
///
/// Every command starts with an 8-byte header:
/// ```text
/// @0 [size:u32]   total message size in bytes
/// @4 [fixed:u16]  byte offset where flex region starts
/// @6 [tag:u8]     command tag
/// @7 [flags:u8]   per-command flags
/// ```
impl MqCommand {
    /// Header size in bytes (always 8).
    pub const HEADER_SIZE: usize = 8;

    /// Byte offset of the `size` field in the header.
    pub const OFF_SIZE: usize = 0;
    /// Byte offset of the `fixed` field in the header.
    pub const OFF_FIXED: usize = 4;
    /// Byte offset of the `tag` field in the header.
    pub const OFF_TAG: usize = 6;
    /// Byte offset of the `flags` field in the header.
    pub const OFF_FLAGS: usize = 7;
}

// =============================================================================
// Free-standing buffer accessors — read from raw &[u8] command buffers
// =============================================================================
// DecodeError
// =============================================================================

/// Error returned when decoding a binary buffer fails due to truncation or
/// corruption.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DecodeError {
    /// The buffer is too short to read the requested field.
    #[error("buffer too short: need {need} bytes at offset {offset}, have {have}")]
    BufferTooShort {
        need: usize,
        offset: usize,
        have: usize,
    },

    /// The data is structurally invalid.
    #[error("invalid data: {0}")]
    Invalid(String),
}

// =============================================================================
// Safe byte-reading helpers
// =============================================================================

/// Read a `u16` in little-endian from `buf` at `offset`, returning an error if
/// the buffer is too short.
#[inline]
pub fn read_u16_le(buf: &[u8], offset: usize) -> Result<u16, DecodeError> {
    const N: usize = 2;
    if offset + N > buf.len() {
        return Err(DecodeError::BufferTooShort {
            need: N,
            offset,
            have: buf.len(),
        });
    }
    Ok(u16::from_le_bytes([buf[offset], buf[offset + 1]]))
}

/// Read a `u32` in little-endian from `buf` at `offset`, returning an error if
/// the buffer is too short.
#[inline]
pub fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32, DecodeError> {
    const N: usize = 4;
    if offset + N > buf.len() {
        return Err(DecodeError::BufferTooShort {
            need: N,
            offset,
            have: buf.len(),
        });
    }
    Ok(u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]))
}

/// Read an `i32` in little-endian from `buf` at `offset`, returning an error if
/// the buffer is too short.
#[inline]
pub fn read_i32_le(buf: &[u8], offset: usize) -> Result<i32, DecodeError> {
    const N: usize = 4;
    if offset + N > buf.len() {
        return Err(DecodeError::BufferTooShort {
            need: N,
            offset,
            have: buf.len(),
        });
    }
    Ok(i32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]))
}

/// Read a `u64` in little-endian from `buf` at `offset`, returning an error if
/// the buffer is too short.
#[inline]
pub fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64, DecodeError> {
    const N: usize = 8;
    if offset + N > buf.len() {
        return Err(DecodeError::BufferTooShort {
            need: N,
            offset,
            have: buf.len(),
        });
    }
    Ok(u64::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
        buf[offset + 4],
        buf[offset + 5],
        buf[offset + 6],
        buf[offset + 7],
    ]))
}

/// Read a sub-slice from `buf` at `[offset..offset+len]`, returning an error if
/// the buffer is too short.
#[inline]
pub fn read_slice(buf: &[u8], offset: usize, len: usize) -> Result<&[u8], DecodeError> {
    if offset + len > buf.len() {
        return Err(DecodeError::BufferTooShort {
            need: len,
            offset,
            have: buf.len(),
        });
    }
    Ok(&buf[offset..offset + len])
}

/// Read a `u8` from `buf` at `offset`, returning an error if the buffer is too
/// short.
#[inline]
pub fn read_u8(buf: &[u8], offset: usize) -> Result<u8, DecodeError> {
    if offset >= buf.len() {
        return Err(DecodeError::BufferTooShort {
            need: 1,
            offset,
            have: buf.len(),
        });
    }
    Ok(buf[offset])
}

// =============================================================================

/// Read the command tag from a raw buffer (byte at offset 6).
#[inline]
pub fn buf_tag(buf: &[u8]) -> u8 {
    buf[MqCommand::OFF_TAG]
}

/// Read the flags byte from a raw buffer (byte at offset 7).
#[inline]
pub fn buf_flags(buf: &[u8]) -> u8 {
    buf[MqCommand::OFF_FLAGS]
}

/// Read a u64 LE field from a raw buffer at the given byte offset.
///
/// The caller must ensure `offset + 8 ≤ buf.len()` (via prior validation).
#[inline]
pub fn buf_field_u64(buf: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap())
}

/// Read a u32 LE field from a raw buffer at the given byte offset.
///
/// The caller must ensure `offset + 4 ≤ buf.len()` (via prior validation).
#[inline]
pub fn buf_field_u32(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap())
}

/// Read a vec_u64 slot from a raw buffer: `[count:4][offset:4]` in fixed.
///
/// Variable-length field — returns `Result` because internal data offsets are
/// untrusted even after fixed-region validation.
#[inline]
pub fn buf_field_vec_u64(
    buf: &[u8],
    offset: usize,
) -> Result<crate::codec::DecodeU64s<'_>, DecodeError> {
    let count = read_u32_le(buf, offset)? as usize;
    if count == 0 {
        return Ok(crate::codec::DecodeU64s::Owned(SmallVec::new()));
    }
    let data_offset = read_u32_le(buf, offset + 4)? as usize;
    Ok(crate::codec::decode_u64s_at(buf, data_offset, count))
}

impl MqCommand {
    /// Command type tag (byte 6 of the header).
    #[inline]
    pub fn tag(&self) -> u8 {
        self.buf[Self::OFF_TAG]
    }

    /// Flags byte from the header.
    #[inline]
    pub fn flags(&self) -> u8 {
        self.buf[Self::OFF_FLAGS]
    }

    /// Total message size from the header.
    ///
    /// Callers must ensure the buffer has been validated (≥ 8 bytes for the
    /// header).  All MqCommand construction paths guarantee this.
    #[inline]
    pub fn cmd_size(&self) -> u32 {
        // SAFETY: header is validated at construction; OFF_SIZE + 4 ≤ 8.
        u32::from_le_bytes(
            self.buf[Self::OFF_SIZE..Self::OFF_SIZE + 4]
                .try_into()
                .unwrap(),
        )
    }

    /// Fixed region size (= flex region start offset).
    #[inline]
    pub fn fixed_size(&self) -> u16 {
        // SAFETY: header is validated at construction; OFF_FIXED + 2 ≤ 8.
        u16::from_le_bytes(
            self.buf[Self::OFF_FIXED..Self::OFF_FIXED + 2]
                .try_into()
                .unwrap(),
        )
    }

    /// Primary entity ID — the first u64 field at offset 8.
    ///
    /// For topic commands this is `topic_id`, for consumer group commands
    /// this is `group_id`, for session commands this is `session_id`, etc.
    /// Used by async apply workers for partition routing.
    ///
    /// Callers must ensure `buf.len() >= 16`. All non-header-only commands
    /// satisfy this; for safety in untrusted contexts use [`try_primary_id`].
    #[inline]
    pub fn primary_id(&self) -> u64 {
        self.field_u64(8)
    }

    /// Fallible variant of [`primary_id`] for untrusted buffers.
    #[inline]
    pub fn try_primary_id(&self) -> Result<u64, DecodeError> {
        read_u64_le(&self.buf, 8)
    }

    /// Read a u64 LE field at the given byte offset.
    ///
    /// The caller must ensure `offset + 8 ≤ buf.len()` (typically proven by
    /// tag dispatch or view-struct construction validation).
    #[inline]
    pub(crate) fn field_u64(&self, offset: usize) -> u64 {
        u64::from_le_bytes(self.buf[offset..offset + 8].try_into().unwrap())
    }

    /// Read a u32 LE field at the given byte offset.
    ///
    /// The caller must ensure `offset + 4 ≤ buf.len()`.
    #[inline]
    pub(crate) fn field_u32(&self, offset: usize) -> u32 {
        u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap())
    }

    /// Read a flex8 str/bytes slot at the given offset.
    ///
    /// Bit-0 tag (all LE):
    /// - Small (≤7): `[(len<<1):u8][data:7]`              — bit 0 = 0
    /// - Large (>7): `[(offset<<1|1):u32_le][size:u32_le]` — bit 0 = 1, 31-bit offset
    #[inline]
    pub(crate) fn field_flex8(&self, offset: usize) -> Result<&[u8], DecodeError> {
        let first = read_u8(&self.buf, offset)?;
        if first & 1 == 0 {
            let len = (first >> 1) as usize;
            if len == 0 {
                return Ok(&[]);
            }
            read_slice(&self.buf, offset + 1, len)
        } else {
            let raw = read_u32_le(&self.buf, offset)?;
            let data_offset = (raw >> 1) as usize;
            let size = read_u32_le(&self.buf, offset + 4)? as usize;
            read_slice(&self.buf, data_offset, size)
        }
    }

    /// Read a flex8 slot as a str.
    #[inline]
    pub(crate) fn field_flex8_str(&self, offset: usize) -> Result<&str, DecodeError> {
        let bytes = self.field_flex8(offset)?;
        Ok(std::str::from_utf8(bytes).unwrap_or(""))
    }

    /// Read a flex8 slot as optional Bytes (None if length is 0).
    #[inline]
    pub(crate) fn field_opt_flex8_bytes(
        &self,
        offset: usize,
    ) -> Result<Option<Bytes>, DecodeError> {
        let first = read_u8(&self.buf, offset)?;
        if first & 1 == 0 {
            let len = (first >> 1) as usize;
            if len == 0 {
                return Ok(None);
            }
            let data = read_slice(&self.buf, offset + 1, len)?;
            Ok(Some(Bytes::copy_from_slice(data)))
        } else {
            let raw = read_u32_le(&self.buf, offset)?;
            let data_offset = (raw >> 1) as usize;
            let size = read_u32_le(&self.buf, offset + 4)? as usize;
            if size == 0 {
                Ok(None)
            } else {
                let data = read_slice(&self.buf, data_offset, size)?;
                Ok(Some(Bytes::copy_from_slice(data)))
            }
        }
    }

    /// Read a vec_u64 slot: `[count:4][offset:4]` in fixed, `[u64_0:8]...` in flex.
    ///
    /// Returns a zero-copy `&[u64]` when the data is 8-byte aligned (common),
    /// otherwise falls back to a bulk memcpy into SmallVec.
    #[inline]
    pub(crate) fn field_vec_u64(
        &self,
        offset: usize,
    ) -> Result<crate::codec::DecodeU64s<'_>, DecodeError> {
        let count = read_u32_le(&self.buf, offset)? as usize;
        if count == 0 {
            return Ok(crate::codec::DecodeU64s::Owned(SmallVec::new()));
        }
        let data_offset = read_u32_le(&self.buf, offset + 4)? as usize;
        Ok(crate::codec::decode_u64s_at(&self.buf, data_offset, count))
    }

    /// Read a vec_bytes descriptor table: `[count:4][offset:4]` in fixed,
    /// `[size0:4][offset0:4]...` descriptors in flex.
    ///
    /// Returns the count from the fixed-region slot. The caller must ensure
    /// the buffer is validated (fixed region is readable).
    #[inline]
    pub(crate) fn field_vec_bytes_count(&self, offset: usize) -> u32 {
        u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap())
    }

    /// Walk to the i-th element in a sequential vec_bytes stream.
    /// Slot format: `[count:4][data_start:4]`; stream: `[len:4][data]...`
    #[inline]
    pub(crate) fn field_vec_bytes_get(
        &self,
        slot_offset: usize,
        index: usize,
    ) -> Result<&[u8], DecodeError> {
        let data_start = read_u32_le(&self.buf, slot_offset + 4)? as usize;
        let mut pos = data_start;
        for _ in 0..index {
            let len = read_u32_le(&self.buf, pos)? as usize;
            pos += 4 + len;
        }
        let len = read_u32_le(&self.buf, pos)? as usize;
        read_slice(&self.buf, pos + 4, len)
    }

    /// Same as `field_vec_bytes_get` but returns a zero-copy `Bytes` slice.
    #[inline]
    pub(crate) fn field_vec_bytes_get_bytes(
        &self,
        slot_offset: usize,
        index: usize,
    ) -> Result<Bytes, DecodeError> {
        let data = self.field_vec_bytes_get(slot_offset, index)?;
        Ok(self.buf.slice_ref(data))
    }

    /// Validate that the buffer is large enough for its declared size.
    ///
    /// Returns `Ok(())` if the buffer has at least 8 bytes (the header) and
    /// the total `cmd_size()` fits within the buffer. Call this once at the
    /// boundary (network receive, raft decode) before using any field
    /// accessors.
    #[inline]
    pub fn validate(&self) -> Result<(), DecodeError> {
        if self.buf.len() < 8 {
            return Err(DecodeError::BufferTooShort {
                need: 8,
                offset: 0,
                have: self.buf.len(),
            });
        }
        let size = self.cmd_size() as usize;
        if size > self.buf.len() {
            return Err(DecodeError::BufferTooShort {
                need: size,
                offset: 0,
                have: self.buf.len(),
            });
        }
        Ok(())
    }

    /// Validate that the buffer is at least `min_size` bytes.
    ///
    /// Used by view-struct constructors to enforce the minimum fixed-region
    /// size for a specific command type.
    #[inline]
    pub fn validate_min(&self, min_size: usize) -> Result<(), DecodeError> {
        if self.buf.len() < min_size {
            return Err(DecodeError::BufferTooShort {
                need: min_size,
                offset: 0,
                have: self.buf.len(),
            });
        }
        Ok(())
    }

    /// Raw buffer access.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Wrap raw bytes as an MqCommand (zero-copy).
    #[inline]
    pub fn from_bytes(buf: Bytes) -> Self {
        Self {
            buf,
            header: [0u8; 32],
            extra: Vec::new(),
        }
    }

    /// Wrap a Vec<u8> as an MqCommand.
    #[inline]
    pub fn from_vec(buf: Vec<u8>) -> Self {
        Self {
            buf: Bytes::from(buf),
            header: [0u8; 32],
            extra: Vec::new(),
        }
    }

    /// Split the current content of `buf` into an `MqCommand` (zero-copy slab).
    /// The `BytesMut` is left empty but retains its backing capacity.
    #[inline]
    pub fn split_from(buf: &mut bytes::BytesMut) -> Self {
        Self {
            buf: buf.split().freeze(),
            header: [0u8; 32],
            extra: Vec::new(),
        }
    }

    /// Create a scattered command from an inline header and payload slabs.
    ///
    /// The header is stored inline (no allocation). The Raft log writer
    /// scatter-writes `header` followed by each slab in `extra` — zero
    /// copies of command payload bytes.
    #[inline]
    pub fn scattered(header: [u8; 32], slabs: Vec<Bytes>) -> Self {
        Self {
            buf: Bytes::new(),
            header,
            extra: slabs,
        }
    }

    /// Total encoded size in bytes (header + all extra segments).
    #[inline]
    pub fn total_encoded_size(&self) -> usize {
        if self.extra.is_empty() {
            self.buf.len()
        } else {
            32 + self.extra.iter().map(|b| b.len()).sum::<usize>()
        }
    }
}

impl fmt::Display for MqCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        crate::codec::fmt_mq_command(self, f)
    }
}

impl fmt::Debug for MqCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MqCommand(tag={}, {} bytes)",
            self.tag(),
            self.total_encoded_size()
        )
    }
}

// Custom Serialize/Deserialize — OpenRaft requires these bounds via AppData.
// The actual raft log codec uses our flat binary Encode/Decode, not serde.

impl Serialize for MqCommand {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.extra.is_empty() {
            serializer.serialize_bytes(&self.buf)
        } else {
            // Scattered: concatenate for serde (rare — only snapshot serialisation).
            let total = self.total_encoded_size();
            let mut all = Vec::with_capacity(total);
            all.extend_from_slice(&self.header);
            for seg in &self.extra {
                all.extend_from_slice(seg);
            }
            serializer.serialize_bytes(&all)
        }
    }
}

impl<'de> Deserialize<'de> for MqCommand {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        Ok(Self {
            buf: Bytes::from(bytes),
            header: [0u8; 32],
            extra: Vec::new(),
        })
    }
}

// =============================================================================
// MqError — zero-alloc error type for hot paths
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityKind {
    Topic,
    Exchange,
    Binding,
    ConsumerGroup,
    Session,
}

impl fmt::Display for EntityKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Topic => f.write_str("topic"),
            Self::Exchange => f.write_str("exchange"),
            Self::Binding => f.write_str("binding"),
            Self::ConsumerGroup => f.write_str("consumer group"),
            Self::Session => f.write_str("session"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MqError {
    /// Entity not found (zero-alloc)
    NotFound { entity: EntityKind, id: u64 },
    /// Entity with given name already exists
    AlreadyExists { entity: EntityKind, id: u64 },
    /// Actor mailbox is full
    MailboxFull { pending: u32 },
    /// Back-pressure: consumer group has too many unprocessed messages/bytes.
    BackPressure { group_id: u64 },
    /// Generation mismatch on offset commit or heartbeat
    IllegalGeneration,
    /// A rebalance is in progress — consumer should re-join
    RebalanceInProgress,
    /// Unknown member ID in a consumer group operation
    UnknownMemberId,
    /// Disk usage budget exceeded — new writes rejected until retention frees space.
    DiskFull,
    /// Dynamic error message (escape hatch)
    Custom(String),
}

impl fmt::Display for MqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { entity, id } => write!(f, "{} {} not found", entity, id),
            Self::AlreadyExists { entity, id } => {
                write!(f, "{} already exists (id={})", entity, id)
            }
            Self::MailboxFull { pending } => write!(f, "mailbox full ({} messages)", pending),
            Self::BackPressure { group_id } => {
                write!(
                    f,
                    "back-pressure: consumer group {} limit exceeded",
                    group_id
                )
            }
            Self::IllegalGeneration => f.write_str("illegal generation"),
            Self::RebalanceInProgress => f.write_str("rebalance in progress"),
            Self::UnknownMemberId => f.write_str("unknown member id"),
            Self::DiskFull => f.write_str("disk usage budget exceeded"),
            Self::Custom(msg) => f.write_str(msg),
        }
    }
}

// =============================================================================
// Retained Message Entry
// =============================================================================

/// A retained message entry stored per topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetainedEntry {
    /// MQTT topic/routing key.
    pub routing_key: Bytes,
    /// The flat-encoded message bytes.
    pub message: Bytes,
}

// =============================================================================
// Snapshot
// =============================================================================

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqSnapshotData {
    pub topics: Vec<TopicSnapshot>,
    pub consumer_groups: Vec<crate::consumer_group::ConsumerGroupSnapshot>,
    #[serde(default)]
    pub exchanges: Vec<ExchangeSnapshot>,
    #[serde(default)]
    pub sessions: Vec<SessionSnapshot>,
    #[serde(default)]
    pub pending_wills: Vec<PendingWill>,
    pub next_id: u64,
    /// Manifest of raft log segment files the follower needs to sync.
    #[serde(default)]
    pub file_manifest: Vec<bisque_raft::SnapshotFileEntry>,
    /// Address of the leader's segment sync server (e.g. "host:port").
    #[serde(default)]
    pub sync_addr: Option<String>,
    /// Per-client highest applied request_seq — for idempotent apply after reconnect.
    /// Key: client_id, Value: last applied request_seq.
    #[serde(default)]
    pub client_sessions: HashMap<u32, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSnapshot {
    pub meta: crate::topic::TopicMeta,
    pub consumer_offsets: Vec<crate::topic::TopicConsumerOffset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeSnapshot {
    pub meta: crate::exchange::ExchangeMeta,
    pub bindings: Vec<Binding>,
    #[serde(default)]
    pub retained: Vec<RetainedEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub meta: crate::session::SessionMeta,
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Display impls
    // =========================================================================

    // =========================================================================
    // Serde roundtrips
    // =========================================================================

    #[test]
    fn test_retention_policy_serde() {
        let policy = RetentionPolicy {
            max_age_secs: Some(3600),
            max_bytes: Some(1_000_000),
            max_messages: None,
        };
        let json = serde_json::to_string(&policy).unwrap();
        let decoded: RetentionPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.max_age_secs, Some(3600));
        assert_eq!(decoded.max_bytes, Some(1_000_000));
        assert!(decoded.max_messages.is_none());
    }

    #[test]
    fn test_retention_policy_defaults() {
        let json = "{}";
        let policy: RetentionPolicy = serde_json::from_str(json).unwrap();
        assert!(policy.max_age_secs.is_none());
        assert!(policy.max_bytes.is_none());
        assert!(policy.max_messages.is_none());
    }

    #[test]
    fn test_ack_message_meta_serde() {
        let meta = AckMessageMeta {
            message_id: 42,
            group_id: 1,
            state: MessageState::InFlight,
            priority: 5,
            deliver_after: 1000,
            attempts: 3,
            last_delivered_at: Some(900),
            consumer_id: Some(100),
            visibility_deadline: Some(31000),
            dedup_key: Some(Bytes::from_static(b"dedup")),
            expires_at: Some(60000),
            reply_to: Some(Bytes::from_static(b"reply-topic")),
            correlation_id: Some(Bytes::from_static(b"corr-123")),
            value_len: 256,
            publisher_id: 0,
        };
        let bytes = bincode::serde::encode_to_vec(&meta, bincode::config::standard()).unwrap();
        let (decoded, _): (AckMessageMeta, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        assert_eq!(decoded.message_id, 42);
        assert_eq!(decoded.state, MessageState::InFlight);
        assert_eq!(decoded.priority, 5);
        assert_eq!(decoded.attempts, 3);
        assert_eq!(decoded.consumer_id, Some(100));
        assert_eq!(decoded.dedup_key.as_deref(), Some(&b"dedup"[..]));
    }

    #[test]
    fn test_entity_stats_serde() {
        let stats = EntityStats::ConsumerGroup {
            group_id: 1,
            variant: GroupVariant::Ack,
            pending_count: 100,
            in_flight_count: 5,
            dlq_count: 2,
            active_actor_count: 0,
        };
        let bytes = bincode::serde::encode_to_vec(&stats, bincode::config::standard()).unwrap();
        let (decoded, _): (EntityStats, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        match decoded {
            EntityStats::ConsumerGroup {
                group_id,
                pending_count,
                in_flight_count,
                dlq_count,
                ..
            } => {
                assert_eq!(group_id, 1);
                assert_eq!(pending_count, 100);
                assert_eq!(in_flight_count, 5);
                assert_eq!(dlq_count, 2);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_message_state_variants() {
        let states = [
            MessageState::Pending,
            MessageState::InFlight,
            MessageState::Acked,
            MessageState::DeadLetter,
            MessageState::Received,
            MessageState::Released,
        ];
        for state in &states {
            let bytes = bincode::serde::encode_to_vec(state, bincode::config::standard()).unwrap();
            let (decoded, _): (MessageState, _) =
                bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
            assert_eq!(*state, decoded);
        }
    }

    #[test]
    fn test_group_variant_default() {
        assert_eq!(GroupVariant::default(), GroupVariant::Offset);
    }

    #[test]
    fn test_ack_variant_config_defaults() {
        let config = AckVariantConfig::default();
        assert_eq!(config.visibility_timeout_ms, 30_000);
        assert_eq!(config.max_retries, 3);
        assert!(config.dead_letter_topic.is_none());
        assert_eq!(config.delay_default_ms, 0);
        assert_eq!(config.max_in_flight_per_consumer, 100);
    }

    #[test]
    fn test_actor_variant_config_defaults() {
        let config = ActorVariantConfig::default();
        assert_eq!(config.max_mailbox_depth, 10_000);
        assert_eq!(config.idle_eviction_secs, 3600);
        assert_eq!(config.ack_timeout_ms, 30_000);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_topic_lifetime_policy_default() {
        assert_eq!(
            TopicLifetimePolicy::default(),
            TopicLifetimePolicy::Permanent
        );
    }
}

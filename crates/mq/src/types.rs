use std::fmt;

use bytes::Bytes;
use crc64fast_nvme::Digest;
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
    let mut digest = Digest::new();
    digest.write(b);
    digest.sum64()
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

/// Zero-copy command type for the MQ engine.
///
/// Wraps a flat-encoded binary buffer (`Bytes`). On the write path, constructor
/// methods encode fields into the buffer. On the read path (state machine apply),
/// the buffer is wrapped zero-copy from the mmap-backed raft log.
///
/// Per-variant accessor structs (in `codec`) provide typed zero-copy APIs
/// over the raw buffer.
#[derive(Clone)]
pub struct MqCommand {
    pub(crate) buf: Bytes,
    /// Scatter payload segments for zero-copy write path.
    /// When `Some`, `buf` contains only the header + descriptor table and the
    /// payloads live in these separate `Bytes` references.  `Encode::encode()`
    /// writes `buf` then each segment sequentially, producing a contiguous
    /// result in the Raft log writer's buffer.
    /// On the read path (decoded from mmap) this is always `None` — the data
    /// is already contiguous in `buf`.
    pub(crate) segments: Option<Vec<Bytes>>,
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
    #[inline]
    pub fn cmd_size(&self) -> u32 {
        u32::from_le_bytes(
            self.buf[Self::OFF_SIZE..Self::OFF_SIZE + 4]
                .try_into()
                .unwrap(),
        )
    }

    /// Fixed region size (= flex region start offset).
    #[inline]
    pub fn fixed_size(&self) -> u16 {
        u16::from_le_bytes(
            self.buf[Self::OFF_FIXED..Self::OFF_FIXED + 2]
                .try_into()
                .unwrap(),
        )
    }

    /// Read a u64 LE field at the given byte offset.
    #[inline]
    pub(crate) fn field_u64(&self, offset: usize) -> u64 {
        u64::from_le_bytes(self.buf[offset..offset + 8].try_into().unwrap())
    }

    /// Read a u32 LE field at the given byte offset.
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
    pub(crate) fn field_flex8(&self, offset: usize) -> &[u8] {
        let first = self.buf[offset];
        if first & 1 == 0 {
            let len = (first >> 1) as usize;
            if len == 0 {
                return &[];
            }
            &self.buf[offset + 1..offset + 1 + len]
        } else {
            let raw = u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap());
            let data_offset = (raw >> 1) as usize;
            let size =
                u32::from_le_bytes(self.buf[offset + 4..offset + 8].try_into().unwrap()) as usize;
            &self.buf[data_offset..data_offset + size]
        }
    }

    /// Read a flex8 slot as a str.
    #[inline]
    pub(crate) fn field_flex8_str(&self, offset: usize) -> &str {
        std::str::from_utf8(self.field_flex8(offset)).unwrap_or("")
    }

    /// Read a flex8 slot as optional Bytes (None if length is 0).
    #[inline]
    pub(crate) fn field_opt_flex8_bytes(&self, offset: usize) -> Option<Bytes> {
        let first = self.buf[offset];
        if first & 1 == 0 {
            let len = (first >> 1) as usize;
            if len == 0 {
                return None;
            }
            Some(self.buf.slice(offset + 1..offset + 1 + len))
        } else {
            let raw = u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap());
            let data_offset = (raw >> 1) as usize;
            let size =
                u32::from_le_bytes(self.buf[offset + 4..offset + 8].try_into().unwrap()) as usize;
            if size == 0 {
                None
            } else {
                Some(self.buf.slice(data_offset..data_offset + size))
            }
        }
    }

    /// Read a vec_u64 slot: `[count:4][offset:4]` in fixed, `[u64_0:8]...` in flex.
    ///
    /// Returns a zero-copy `&[u64]` when the data is 8-byte aligned (common),
    /// otherwise falls back to a bulk memcpy into SmallVec.
    #[inline]
    pub(crate) fn field_vec_u64(&self, offset: usize) -> crate::codec::DecodeU64s<'_> {
        let count = u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap()) as usize;
        if count == 0 {
            return crate::codec::DecodeU64s::Owned(SmallVec::new());
        }
        let data_offset =
            u32::from_le_bytes(self.buf[offset + 4..offset + 8].try_into().unwrap()) as usize;
        crate::codec::decode_u64s_at(&self.buf, data_offset, count)
    }

    /// Read a vec_bytes descriptor table: `[count:4][offset:4]` in fixed,
    /// `[size0:4][offset0:4]...` descriptors in flex.
    ///
    /// Returns an iterator yielding `&[u8]` slices for each element.
    #[inline]
    pub(crate) fn field_vec_bytes_count(&self, offset: usize) -> u32 {
        u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap())
    }

    /// Get the i-th element from a vec_bytes descriptor table.
    #[inline]
    pub(crate) fn field_vec_bytes_get(&self, slot_offset: usize, index: usize) -> &[u8] {
        let table_offset = u32::from_le_bytes(
            self.buf[slot_offset + 4..slot_offset + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let desc_offset = table_offset + index * 8;
        let size =
            u32::from_le_bytes(self.buf[desc_offset..desc_offset + 4].try_into().unwrap()) as usize;
        let data_offset = u32::from_le_bytes(
            self.buf[desc_offset + 4..desc_offset + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        &self.buf[data_offset..data_offset + size]
    }

    /// Get the i-th element from a vec_bytes descriptor table as Bytes (zero-copy).
    #[inline]
    pub(crate) fn field_vec_bytes_get_bytes(&self, slot_offset: usize, index: usize) -> Bytes {
        let table_offset = u32::from_le_bytes(
            self.buf[slot_offset + 4..slot_offset + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let desc_offset = table_offset + index * 8;
        let size =
            u32::from_le_bytes(self.buf[desc_offset..desc_offset + 4].try_into().unwrap()) as usize;
        let data_offset = u32::from_le_bytes(
            self.buf[desc_offset + 4..desc_offset + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        self.buf.slice(data_offset..data_offset + size)
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
            segments: None,
        }
    }

    /// Returns `true` if this command uses scatter mode (payloads not in buf).
    #[inline]
    pub fn is_scatter(&self) -> bool {
        self.segments.is_some()
    }

    /// Total encoded size including scatter segments.
    #[inline]
    pub fn total_encoded_size(&self) -> usize {
        match &self.segments {
            None => self.buf.len(),
            Some(segs) => self.buf.len() + segs.iter().map(|s| s.len()).sum::<usize>(),
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
            "MqCommand(tag={}, {} bytes{})",
            self.tag(),
            self.total_encoded_size(),
            if self.is_scatter() { ", scatter" } else { "" }
        )
    }
}

// Custom Serialize/Deserialize — OpenRaft requires these bounds via AppData.
// The actual raft log codec uses our flat binary Encode/Decode, not serde.

impl Serialize for MqCommand {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.segments {
            None => serializer.serialize_bytes(&self.buf),
            Some(segs) => {
                // Materialize scatter segments for serde (not hot path).
                let mut all = Vec::with_capacity(self.total_encoded_size());
                all.extend_from_slice(&self.buf);
                for seg in segs {
                    all.extend_from_slice(seg);
                }
                serializer.serialize_bytes(&all)
            }
        }
    }
}

impl<'de> Deserialize<'de> for MqCommand {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        Ok(Self {
            buf: Bytes::from(bytes),
            segments: None,
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
            Self::Custom(msg) => f.write_str(msg),
        }
    }
}

// =============================================================================
// MqResponse
// =============================================================================

/// A retained message entry stored per topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetainedEntry {
    /// MQTT topic/routing key.
    pub routing_key: Bytes,
    /// The flat-encoded message bytes.
    pub message: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MqResponse {
    Ok,
    Error(MqError),
    EntityCreated {
        id: u64,
    },
    Messages {
        messages: SmallVec<[DeliveredMessage; 8]>,
    },
    Published {
        /// First dense offset assigned.
        base_offset: u64,
        /// Number of messages published.
        count: u64,
    },
    Stats(EntityStats),
    BatchResponse(Box<SmallVec<[MqResponse; 8]>>),
    /// Consumer group join completed (or partially — check `phase_complete`).
    GroupJoined {
        generation: i32,
        leader: String,
        member_id: String,
        protocol_name: String,
        is_leader: bool,
        /// `(member_id, protocol_metadata)` — only populated for the leader.
        members: Vec<(String, Bytes)>,
        /// `true` if all members joined and generation was bumped.
        phase_complete: bool,
    },
    /// Consumer group sync completed.
    GroupSynced {
        assignment: Vec<u8>,
        /// `true` if group transitioned to `Stable`.
        phase_complete: bool,
    },
    /// Messages were dead-lettered. The caller (background timer) should read
    /// the original message bytes from the raft log and issue `GroupPublishToDlq`.
    DeadLettered {
        /// Raft log indexes of the dead-lettered messages.
        dead_letter_ids: SmallVec<[u64; 8]>,
        /// The configured DLQ topic to publish them to.
        dlq_topic_id: u64,
    },
    /// Retained messages matching a topic filter.
    RetainedMessages {
        messages: Vec<RetainedEntry>,
    },
    /// Will message is pending (has a delay).
    WillPending {
        session_id: u64,
        delay_ms: u64,
    },
    /// Session restored successfully.
    SessionRestored {
        session_id: u64,
        session_expiry_ms: u64,
        subscription_data: Bytes,
    },
    /// Session not found or expired.
    SessionNotFound,
    /// Multi-group deliver result.
    MultiMessages {
        groups: Vec<(u64, SmallVec<[DeliveredMessage; 8]>)>,
    },
    /// Topic aliases for a session.
    TopicAliases {
        aliases: Vec<TopicAliasEntry>,
    },
    /// Pending wills that were fired.
    WillsFired {
        count: u32,
    },
}

impl fmt::Display for MqResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok => write!(f, "Ok"),
            Self::Error(e) => write!(f, "Error({})", e),
            Self::EntityCreated { id } => write!(f, "EntityCreated({})", id),
            Self::Messages { messages } => write!(f, "Messages(count={})", messages.len()),
            Self::Published { base_offset, count } => {
                write!(f, "Published(base={base_offset}, count={count})")
            }
            Self::Stats(_) => write!(f, "Stats"),
            Self::GroupJoined {
                generation,
                member_id,
                is_leader,
                phase_complete,
                ..
            } => {
                write!(
                    f,
                    "GroupJoined(gen={generation}, member={member_id}, leader={is_leader}, complete={phase_complete})"
                )
            }
            Self::GroupSynced { phase_complete, .. } => {
                write!(f, "GroupSynced(complete={phase_complete})")
            }
            Self::BatchResponse(resps) => write!(f, "BatchResponse(count={})", resps.len()),
            Self::DeadLettered {
                dead_letter_ids,
                dlq_topic_id,
            } => {
                write!(
                    f,
                    "DeadLettered(count={}, dlq_topic={})",
                    dead_letter_ids.len(),
                    dlq_topic_id
                )
            }
            Self::RetainedMessages { messages } => {
                write!(f, "RetainedMessages(count={})", messages.len())
            }
            Self::WillPending {
                session_id,
                delay_ms,
            } => write!(f, "WillPending(session={session_id}, delay={delay_ms}ms)"),
            Self::SessionRestored { session_id, .. } => {
                write!(f, "SessionRestored(session={session_id})")
            }
            Self::SessionNotFound => write!(f, "SessionNotFound"),
            Self::MultiMessages { groups } => {
                let total: usize = groups.iter().map(|(_, msgs)| msgs.len()).sum();
                write!(f, "MultiMessages(groups={}, msgs={})", groups.len(), total)
            }
            Self::TopicAliases { aliases } => {
                write!(f, "TopicAliases(count={})", aliases.len())
            }
            Self::WillsFired { count } => write!(f, "WillsFired(count={count})"),
        }
    }
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

    #[test]
    fn test_mq_response_display() {
        assert_eq!(format!("{}", MqResponse::Ok), "Ok");
        assert_eq!(
            format!("{}", MqResponse::Error(MqError::Custom("fail".to_string()))),
            "Error(fail)"
        );
        assert_eq!(
            format!("{}", MqResponse::EntityCreated { id: 42 }),
            "EntityCreated(42)"
        );
        assert_eq!(
            format!(
                "{}",
                MqResponse::Messages {
                    messages: smallvec::smallvec![DeliveredMessage {
                        message_id: 1,
                        attempt: 1,
                        original_timestamp: 0,
                        group_id: 0,
                    }]
                }
            ),
            "Messages(count=1)"
        );
        assert_eq!(
            format!(
                "{}",
                MqResponse::Published {
                    base_offset: 1,
                    count: 3,
                }
            ),
            "Published(base=1, count=3)"
        );
    }

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

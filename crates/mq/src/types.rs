use std::fmt;

use bytes::Bytes;
use crc64fast_nvme::Digest;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// CRC64-NVME hash of a name string, used as the key in name→ID lookup maps.
#[inline]
pub fn name_hash(name: &str) -> u64 {
    let mut digest = Digest::new();
    digest.write(name.as_bytes());
    digest.sum64()
}

// =============================================================================
// Message Payload
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagePayload {
    #[serde(default)]
    pub key: Option<Bytes>,
    pub value: Bytes,
    #[serde(default)]
    pub headers: Vec<(String, Bytes)>,
    pub timestamp: u64,
    /// Per-message time-to-live in milliseconds.
    /// If set, the message expires at `enqueue_time + ttl_ms` regardless of retries.
    #[serde(default)]
    pub ttl_ms: Option<u64>,
    /// Routing key for exchange-based routing (AMQP/MQTT topic patterns).
    #[serde(default)]
    pub routing_key: Option<String>,
    /// Topic name to publish a response to (request/response pattern).
    #[serde(default)]
    pub reply_to: Option<String>,
    /// Client-specified correlation identifier for request/response matching.
    #[serde(default)]
    pub correlation_id: Option<String>,
    /// Per-message delay in milliseconds before the message becomes eligible
    /// for delivery. Overrides the queue-level `delay_default_ms` when set.
    #[serde(default)]
    pub delay_ms: Option<u64>,
}

impl MessagePayload {
    /// Encode this payload into the flat binary format for raft log storage.
    pub fn encode_flat(&self) -> Bytes {
        crate::flat::FlatMessageBuilder::from(self).build()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveredMessage {
    /// Raft log index of this message.
    pub message_id: u64,
    pub attempt: u32,
    pub original_timestamp: u64,
}

// =============================================================================
// Entity Identifiers
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityType {
    Topic,
    Queue,
    ActorNamespace,
    Job,
    Exchange,
}

// =============================================================================
// Exchange Types (for AMQP/MQTT routing)
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeType {
    /// Delivers to all bound queues (fanout).
    Fanout,
    /// Delivers to queues whose binding key exactly matches the routing key.
    Direct,
    /// Delivers to queues whose binding pattern matches the routing key
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
    pub messages: Vec<Bytes>,
}

/// A binding from an exchange to a queue with an optional routing key pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Binding {
    pub binding_id: u64,
    pub exchange_id: u64,
    pub queue_id: u64,
    /// Routing key pattern. For direct exchanges this is a literal match.
    /// For topic exchanges this supports `*` and `#` wildcards.
    #[serde(default)]
    pub routing_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub entity_type: EntityType,
    pub entity_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentRange {
    pub segment_id: u64,
    pub min_index: u64,
    pub max_index: u64,
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
///
/// Stored in the coordinator group's `TopicMeta.partitions` vector.
/// The actual message data lives in the partition's own raft group.
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
// Queue Types
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageState {
    Pending,
    InFlight,
    Acked,
    DeadLetter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMessageMeta {
    pub message_id: u64,
    pub queue_id: u64,
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
    /// Computed at enqueue time from `MessagePayload.ttl_ms`.
    #[serde(default)]
    pub expires_at: Option<u64>,
    /// Topic name to publish a response to on ACK (request/reply pattern).
    /// Extracted from the flat message header at enqueue time.
    #[serde(default)]
    pub reply_to: Option<String>,
    /// Correlation ID for request/reply matching.
    /// Extracted from the flat message header at enqueue time.
    #[serde(default)]
    pub correlation_id: Option<String>,
}

// =============================================================================
// Job Types
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OverlapPolicy {
    Skip,
    Queue,
}

impl Default for OverlapPolicy {
    fn default() -> Self {
        Self::Skip
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

fn default_max_retries() -> u32 {
    3
}
fn default_retry_delay_ms() -> u64 {
    5000
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            retry_delay_ms: default_retry_delay_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputSource {
    pub entity_type: EntityType,
    pub entity_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobState {
    #[serde(default)]
    pub assigned_consumer_id: Option<u64>,
    #[serde(default)]
    pub last_triggered_at: Option<u64>,
    #[serde(default)]
    pub last_completed_at: Option<u64>,
    pub next_trigger_at: u64,
    #[serde(default)]
    pub current_execution_id: Option<u64>,
    #[serde(default)]
    pub consecutive_failures: u32,
    #[serde(default)]
    pub queued_triggers: u32,
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
    Queue {
        queue_id: u64,
        pending_count: u64,
        in_flight_count: u64,
        dlq_count: u64,
    },
    ActorNamespace {
        namespace_id: u64,
        active_actor_count: u64,
    },
    Job {
        job_id: u64,
        state: JobState,
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
}

// Tag constants for MqCommand discriminants.
impl MqCommand {
    // -- Topics --
    pub const TAG_CREATE_TOPIC: u8 = 0;
    pub const TAG_DELETE_TOPIC: u8 = 1;
    pub const TAG_PUBLISH: u8 = 2;
    pub const TAG_COMMIT_OFFSET: u8 = 3;
    pub const TAG_PURGE_TOPIC: u8 = 4;
    // -- Queues --
    pub const TAG_CREATE_QUEUE: u8 = 5;
    pub const TAG_DELETE_QUEUE: u8 = 6;
    pub const TAG_ENQUEUE: u8 = 7;
    pub const TAG_DELIVER: u8 = 8;
    pub const TAG_ACK: u8 = 9;
    pub const TAG_NACK: u8 = 10;
    pub const TAG_EXTEND_VISIBILITY: u8 = 11;
    pub const TAG_TIMEOUT_EXPIRED: u8 = 12;
    pub const TAG_PUBLISH_TO_DLQ: u8 = 13;
    pub const TAG_PRUNE_DEDUP_WINDOW: u8 = 14;
    pub const TAG_EXPIRE_PENDING_MESSAGES: u8 = 15;
    pub const TAG_PURGE_QUEUE: u8 = 16;
    pub const TAG_GET_QUEUE_ATTRIBUTES: u8 = 17;
    // -- Exchanges --
    pub const TAG_CREATE_EXCHANGE: u8 = 18;
    pub const TAG_DELETE_EXCHANGE: u8 = 19;
    pub const TAG_CREATE_BINDING: u8 = 20;
    pub const TAG_DELETE_BINDING: u8 = 21;
    pub const TAG_PUBLISH_TO_EXCHANGE: u8 = 22;
    // -- Actors --
    pub const TAG_CREATE_ACTOR_NAMESPACE: u8 = 23;
    pub const TAG_DELETE_ACTOR_NAMESPACE: u8 = 24;
    pub const TAG_SEND_TO_ACTOR: u8 = 25;
    pub const TAG_DELIVER_ACTOR_MESSAGE: u8 = 26;
    pub const TAG_ACK_ACTOR_MESSAGE: u8 = 27;
    pub const TAG_NACK_ACTOR_MESSAGE: u8 = 28;
    pub const TAG_ASSIGN_ACTORS: u8 = 29;
    pub const TAG_RELEASE_ACTORS: u8 = 30;
    pub const TAG_EVICT_IDLE_ACTORS: u8 = 31;
    // -- Jobs --
    pub const TAG_CREATE_JOB: u8 = 32;
    pub const TAG_DELETE_JOB: u8 = 33;
    pub const TAG_UPDATE_JOB: u8 = 34;
    pub const TAG_ENABLE_JOB: u8 = 35;
    pub const TAG_DISABLE_JOB: u8 = 36;
    pub const TAG_TRIGGER_JOB: u8 = 37;
    pub const TAG_ASSIGN_JOB: u8 = 38;
    pub const TAG_COMPLETE_JOB: u8 = 39;
    pub const TAG_FAIL_JOB: u8 = 40;
    pub const TAG_TIMEOUT_JOB: u8 = 41;
    // -- Sessions --
    pub const TAG_REGISTER_CONSUMER: u8 = 42;
    pub const TAG_DISCONNECT_CONSUMER: u8 = 43;
    pub const TAG_HEARTBEAT: u8 = 44;
    pub const TAG_REGISTER_PRODUCER: u8 = 45;
    pub const TAG_DISCONNECT_PRODUCER: u8 = 46;
    // -- Batch --
    pub const TAG_BATCH: u8 = 47;
}

impl MqCommand {
    /// Command type tag (first byte of the buffer).
    #[inline]
    pub fn tag(&self) -> u8 {
        self.buf[0]
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

    /// Raw buffer access.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Wrap raw bytes as an MqCommand (zero-copy).
    #[inline]
    pub fn from_bytes(buf: Bytes) -> Self {
        Self { buf }
    }
}

impl fmt::Display for MqCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        crate::codec::fmt_mq_command(self, f)
    }
}

impl fmt::Debug for MqCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MqCommand(tag={}, {} bytes)", self.tag(), self.buf.len())
    }
}

// Custom Serialize/Deserialize — OpenRaft requires these bounds via AppData.
// The actual raft log codec uses our flat binary Encode/Decode, not serde.

impl Serialize for MqCommand {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.buf)
    }
}

impl<'de> Deserialize<'de> for MqCommand {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        Ok(Self {
            buf: Bytes::from(bytes),
        })
    }
}

// Note: Display impl for MqCommand is delegated to codec::fmt_mq_command
// (defined in codec.rs since it needs access to the encoding format).

// =============================================================================
// MqError — zero-alloc error type for hot paths
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityKind {
    Topic,
    Queue,
    ActorNamespace,
    Job,
    Consumer,
    Exchange,
    Binding,
}

impl fmt::Display for EntityKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Topic => f.write_str("topic"),
            Self::Queue => f.write_str("queue"),
            Self::ActorNamespace => f.write_str("actor namespace"),
            Self::Job => f.write_str("job"),
            Self::Consumer => f.write_str("consumer"),
            Self::Exchange => f.write_str("exchange"),
            Self::Binding => f.write_str("binding"),
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
            Self::Custom(msg) => f.write_str(msg),
        }
    }
}

// =============================================================================
// MqResponse
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MqResponse {
    Ok,
    Error(MqError),
    EntityCreated {
        id: u64,
    },
    Messages {
        messages: Vec<DeliveredMessage>,
    },
    Published {
        offsets: SmallVec<[u64; 16]>,
    },
    Stats(EntityStats),
    BatchResponse(Vec<MqResponse>),
    /// Messages were dead-lettered. The caller (background timer) should read
    /// the original message bytes from the raft log and issue `PublishToDlq`.
    DeadLettered {
        /// Raft log indexes of the dead-lettered messages.
        dead_letter_ids: Vec<u64>,
        /// The configured DLQ topic to publish them to.
        dlq_topic_id: u64,
    },
}

impl fmt::Display for MqResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok => write!(f, "Ok"),
            Self::Error(e) => write!(f, "Error({})", e),
            Self::EntityCreated { id } => write!(f, "EntityCreated({})", id),
            Self::Messages { messages } => write!(f, "Messages(count={})", messages.len()),
            Self::Published { offsets } => write!(f, "Published(count={})", offsets.len()),
            Self::Stats(_) => write!(f, "Stats"),
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
        }
    }
}

// =============================================================================
// Snapshot
// =============================================================================

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqSnapshotData {
    pub topics: Vec<TopicSnapshot>,
    pub queues: Vec<QueueSnapshot>,
    pub actor_namespaces: Vec<ActorNamespaceSnapshot>,
    pub jobs: Vec<JobSnapshot>,
    pub consumers: Vec<ConsumerSnapshot>,
    pub producers: Vec<ProducerSnapshot>,
    #[serde(default)]
    pub exchanges: Vec<ExchangeSnapshot>,
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
pub struct QueueSnapshot {
    pub meta: crate::queue::QueueMeta,
    pub messages: Vec<QueueMessageMeta>,
    pub dedup_entries: Vec<(u64, Vec<Bytes>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorNamespaceSnapshot {
    pub meta: crate::actor::ActorNamespaceMeta,
    pub actors: Vec<crate::actor::ActorState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSnapshot {
    pub meta: crate::job::JobMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerSnapshot {
    pub meta: crate::consumer::ConsumerMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerSnapshot {
    pub meta: crate::producer::ProducerMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeSnapshot {
    pub meta: crate::exchange::ExchangeMeta,
    pub bindings: Vec<Binding>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use crate::config::{ActorConfig, JobConfig, QueueConfig};

    // =========================================================================
    // Display impls
    // =========================================================================

    #[test]
    fn test_mq_command_display() {
        let cases: Vec<(MqCommand, &str)> = vec![
            (
                MqCommand::create_topic("events", RetentionPolicy::default(), 0),
                "CreateTopic(events)",
            ),
            (MqCommand::delete_topic(42), "DeleteTopic(42)"),
            (
                MqCommand::publish(
                    1,
                    &[crate::flat::FlatMessageBuilder::new(Bytes::from_static(b"x")).build()],
                ),
                "Publish(topic=1, count=1)",
            ),
            (
                MqCommand::create_queue("tasks", &QueueConfig::default()),
                "CreateQueue(tasks)",
            ),
            (MqCommand::ack(5, &[1, 2, 3], None), "Ack(queue=5, count=3)"),
            (MqCommand::heartbeat(99), "Heartbeat(99)"),
            (MqCommand::disconnect_consumer(7), "DisconnectConsumer(7)"),
        ];

        for (cmd, expected) in cases {
            assert_eq!(format!("{}", cmd), expected);
        }
    }

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
                    messages: vec![DeliveredMessage {
                        message_id: 1,
                        attempt: 1,
                        original_timestamp: 0,
                    }]
                }
            ),
            "Messages(count=1)"
        );
        assert_eq!(
            format!(
                "{}",
                MqResponse::Published {
                    offsets: smallvec::smallvec![1u64, 2, 3]
                }
            ),
            "Published(count=3)"
        );
    }

    // =========================================================================
    // Serde roundtrips
    // =========================================================================

    #[test]
    fn test_message_payload_serde() {
        let payload = MessagePayload {
            key: Some(Bytes::from_static(b"key")),
            value: Bytes::from_static(b"value"),
            headers: vec![("h1".to_string(), Bytes::from_static(b"v1"))],
            timestamp: 12345,
            ttl_ms: None,
            routing_key: None,
            reply_to: None,
            correlation_id: None,
            delay_ms: None,
        };
        let bytes = bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap();
        let (decoded, _): (MessagePayload, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        assert_eq!(decoded.key.as_deref(), Some(&b"key"[..]));
        assert_eq!(decoded.value.as_ref(), b"value");
        assert_eq!(decoded.headers.len(), 1);
        assert_eq!(decoded.timestamp, 12345);
    }

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
    fn test_queue_message_meta_serde() {
        let meta = QueueMessageMeta {
            message_id: 42,
            queue_id: 1,
            state: MessageState::InFlight,
            priority: 5,
            deliver_after: 1000,
            attempts: 3,
            last_delivered_at: Some(900),
            consumer_id: Some(100),
            visibility_deadline: Some(31000),
            dedup_key: Some(Bytes::from_static(b"dedup")),
            expires_at: Some(60000),
            reply_to: Some("reply-topic".into()),
            correlation_id: Some("corr-123".into()),
        };
        let bytes = bincode::serde::encode_to_vec(&meta, bincode::config::standard()).unwrap();
        let (decoded, _): (QueueMessageMeta, _) =
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
        let stats = EntityStats::Queue {
            queue_id: 1,
            pending_count: 100,
            in_flight_count: 5,
            dlq_count: 2,
        };
        let bytes = bincode::serde::encode_to_vec(&stats, bincode::config::standard()).unwrap();
        let (decoded, _): (EntityStats, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        match decoded {
            EntityStats::Queue {
                queue_id,
                pending_count,
                in_flight_count,
                dlq_count,
            } => {
                assert_eq!(queue_id, 1);
                assert_eq!(pending_count, 100);
                assert_eq!(in_flight_count, 5);
                assert_eq!(dlq_count, 2);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_mq_command_serde_roundtrip() {
        let flat_msg = crate::flat::FlatMessageBuilder::new(Bytes::from_static(b"v"))
            .key(Bytes::from_static(b"k"))
            .timestamp(999)
            .build();
        let cmd = MqCommand::enqueue(5, &[flat_msg], &[Some(Bytes::from_static(b"dk"))]);
        let bytes = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();
        let (decoded, _): (MqCommand, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        assert_eq!(decoded.tag(), MqCommand::TAG_ENQUEUE);
        let v = decoded.as_enqueue();
        assert_eq!(v.queue_id(), 5);
        assert_eq!(v.messages().count(), 1);
        assert_eq!(v.dedup_keys().count(), 1);
    }

    #[test]
    fn test_mq_response_serde_roundtrip() {
        let resp = MqResponse::Messages {
            messages: vec![DeliveredMessage {
                message_id: 42,
                attempt: 2,
                original_timestamp: 500,
            }],
        };
        let bytes = bincode::serde::encode_to_vec(&resp, bincode::config::standard()).unwrap();
        let (decoded, _): (MqResponse, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        match decoded {
            MqResponse::Messages { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].message_id, 42);
                assert_eq!(messages[0].attempt, 2);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_snapshot_data_serde_roundtrip() {
        let snap = MqSnapshotData {
            topics: vec![TopicSnapshot {
                meta: crate::topic::TopicMeta::new(
                    1,
                    "t".to_string(),
                    100,
                    RetentionPolicy::default(),
                ),
                consumer_offsets: Vec::new(),
            }],
            queues: Vec::new(),
            actor_namespaces: Vec::new(),
            jobs: Vec::new(),
            consumers: Vec::new(),
            producers: Vec::new(),
            next_id: 42,
            file_manifest: Vec::new(),
            sync_addr: None,
            exchanges: Vec::new(),
        };
        let bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();
        let (decoded, _): (MqSnapshotData, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        assert_eq!(decoded.topics.len(), 1);
        assert_eq!(decoded.topics[0].meta.name, "t");
        assert_eq!(decoded.next_id, 42);
    }

    #[test]
    fn test_overlap_policy_default() {
        assert_eq!(OverlapPolicy::default(), OverlapPolicy::Skip);
    }

    #[test]
    fn test_retry_config_default() {
        let rc = RetryConfig::default();
        assert_eq!(rc.max_retries, 3);
        assert_eq!(rc.retry_delay_ms, 5000);
    }

    #[test]
    fn test_message_state_variants() {
        let states = [
            MessageState::Pending,
            MessageState::InFlight,
            MessageState::Acked,
            MessageState::DeadLetter,
        ];
        for state in &states {
            let bytes = bincode::serde::encode_to_vec(state, bincode::config::standard()).unwrap();
            let (decoded, _): (MessageState, _) =
                bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
            assert_eq!(*state, decoded);
        }
    }

    #[test]
    fn test_batch_serde_roundtrip() {
        let sub_cmds = vec![
            MqCommand::create_topic("t1", RetentionPolicy::default(), 0),
            MqCommand::publish(
                1,
                &[
                    crate::flat::FlatMessageBuilder::new(Bytes::from_static(b"data"))
                        .timestamp(100)
                        .build(),
                ],
            ),
        ];
        let cmd = MqCommand::batch(&sub_cmds);
        let bytes = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();
        let (decoded, _): (MqCommand, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded.tag(), MqCommand::TAG_BATCH);
        let batch = decoded.as_batch();
        assert_eq!(batch.count(), 2);
        let cmds: Vec<MqCommand> = batch.commands().collect();
        assert_eq!(cmds[0].tag(), MqCommand::TAG_CREATE_TOPIC);
        assert_eq!(cmds[1].tag(), MqCommand::TAG_PUBLISH);

        let resp =
            MqResponse::BatchResponse(vec![MqResponse::Ok, MqResponse::EntityCreated { id: 5 }]);
        let bytes = bincode::serde::encode_to_vec(&resp, bincode::config::standard()).unwrap();
        let (decoded, _): (MqResponse, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        match decoded {
            MqResponse::BatchResponse(resps) => {
                assert_eq!(resps.len(), 2);
                assert!(matches!(resps[0], MqResponse::Ok));
                assert!(matches!(resps[1], MqResponse::EntityCreated { id: 5 }));
            }
            _ => panic!("expected BatchResponse"),
        }
    }

    #[test]
    fn test_batch_display() {
        let sub_cmds = vec![
            MqCommand::delete_topic(1),
            MqCommand::delete_queue(2),
            MqCommand::heartbeat(3),
        ];
        let cmd = MqCommand::batch(&sub_cmds);
        assert_eq!(format!("{}", cmd), "Batch(count=3)");

        let resp = MqResponse::BatchResponse(vec![MqResponse::Ok, MqResponse::Ok]);
        assert_eq!(format!("{}", resp), "BatchResponse(count=2)");
    }
}

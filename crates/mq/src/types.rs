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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveredMessage {
    /// Raft log index of this message.
    pub message_id: u64,
    pub payload: MessagePayload,
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
// MqCommand
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MqCommand {
    // -- Topics --
    CreateTopic {
        name: String,
        retention: RetentionPolicy,
    },
    DeleteTopic {
        topic_id: u64,
    },
    Publish {
        topic_id: u64,
        messages: Vec<MessagePayload>,
    },
    CommitOffset {
        topic_id: u64,
        consumer_id: u64,
        offset: u64,
    },
    PurgeTopic {
        topic_id: u64,
        before_index: u64,
    },

    // -- Queues --
    CreateQueue {
        name: String,
        config: crate::config::QueueConfig,
    },
    DeleteQueue {
        queue_id: u64,
    },
    Enqueue {
        queue_id: u64,
        messages: Vec<MessagePayload>,
        dedup_keys: Vec<Option<Bytes>>,
    },
    Deliver {
        queue_id: u64,
        consumer_id: u64,
        max_count: u32,
    },
    Ack {
        queue_id: u64,
        message_ids: Vec<u64>,
    },
    Nack {
        queue_id: u64,
        message_ids: Vec<u64>,
    },
    ExtendVisibility {
        queue_id: u64,
        message_ids: Vec<u64>,
        extension_ms: u64,
    },
    TimeoutExpired {
        queue_id: u64,
        message_ids: Vec<u64>,
    },
    PruneDedupWindow {
        queue_id: u64,
        before_timestamp: u64,
    },

    // -- Actors --
    CreateActorNamespace {
        name: String,
        config: crate::config::ActorConfig,
    },
    DeleteActorNamespace {
        namespace_id: u64,
    },
    SendToActor {
        namespace_id: u64,
        actor_id: Bytes,
        message: MessagePayload,
    },
    DeliverActorMessage {
        namespace_id: u64,
        actor_id: Bytes,
        consumer_id: u64,
    },
    AckActorMessage {
        namespace_id: u64,
        actor_id: Bytes,
        message_id: u64,
    },
    NackActorMessage {
        namespace_id: u64,
        actor_id: Bytes,
        message_id: u64,
    },
    AssignActors {
        namespace_id: u64,
        consumer_id: u64,
        actor_ids: Vec<Bytes>,
    },
    ReleaseActors {
        namespace_id: u64,
        consumer_id: u64,
    },
    EvictIdleActors {
        namespace_id: u64,
        before_timestamp: u64,
    },

    // -- Jobs --
    CreateJob {
        name: String,
        config: crate::config::JobConfig,
    },
    DeleteJob {
        job_id: u64,
    },
    UpdateJob {
        job_id: u64,
        config: crate::config::JobConfig,
    },
    EnableJob {
        job_id: u64,
    },
    DisableJob {
        job_id: u64,
    },
    TriggerJob {
        job_id: u64,
        execution_id: u64,
        triggered_at: u64,
    },
    AssignJob {
        job_id: u64,
        consumer_id: u64,
    },
    CompleteJob {
        job_id: u64,
        execution_id: u64,
    },
    FailJob {
        job_id: u64,
        execution_id: u64,
        error: String,
    },
    TimeoutJob {
        job_id: u64,
        execution_id: u64,
    },

    // -- Sessions --
    RegisterConsumer {
        consumer_id: u64,
        group_name: String,
        subscriptions: Vec<Subscription>,
    },
    DisconnectConsumer {
        consumer_id: u64,
    },
    Heartbeat {
        consumer_id: u64,
    },
    RegisterProducer {
        producer_id: u64,
        name: Option<String>,
    },
    DisconnectProducer {
        producer_id: u64,
    },

    // -- Batch --
    Batch(Vec<MqCommand>),
}

impl fmt::Display for MqCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateTopic { name, .. } => write!(f, "CreateTopic({})", name),
            Self::DeleteTopic { topic_id } => write!(f, "DeleteTopic({})", topic_id),
            Self::Publish { topic_id, messages } => {
                write!(f, "Publish(topic={}, count={})", topic_id, messages.len())
            }
            Self::CommitOffset {
                topic_id,
                consumer_id,
                offset,
            } => {
                write!(
                    f,
                    "CommitOffset(topic={}, consumer={}, offset={})",
                    topic_id, consumer_id, offset
                )
            }
            Self::PurgeTopic { topic_id, .. } => write!(f, "PurgeTopic({})", topic_id),
            Self::CreateQueue { name, .. } => write!(f, "CreateQueue({})", name),
            Self::DeleteQueue { queue_id } => write!(f, "DeleteQueue({})", queue_id),
            Self::Enqueue {
                queue_id, messages, ..
            } => {
                write!(f, "Enqueue(queue={}, count={})", queue_id, messages.len())
            }
            Self::Deliver {
                queue_id,
                consumer_id,
                max_count,
            } => {
                write!(
                    f,
                    "Deliver(queue={}, consumer={}, max={})",
                    queue_id, consumer_id, max_count
                )
            }
            Self::Ack {
                queue_id,
                message_ids,
            } => {
                write!(f, "Ack(queue={}, count={})", queue_id, message_ids.len())
            }
            Self::Nack {
                queue_id,
                message_ids,
            } => {
                write!(f, "Nack(queue={}, count={})", queue_id, message_ids.len())
            }
            Self::ExtendVisibility { queue_id, .. } => {
                write!(f, "ExtendVisibility(queue={})", queue_id)
            }
            Self::TimeoutExpired {
                queue_id,
                message_ids,
            } => {
                write!(
                    f,
                    "TimeoutExpired(queue={}, count={})",
                    queue_id,
                    message_ids.len()
                )
            }
            Self::PruneDedupWindow { queue_id, .. } => {
                write!(f, "PruneDedupWindow(queue={})", queue_id)
            }
            Self::CreateActorNamespace { name, .. } => write!(f, "CreateActorNamespace({})", name),
            Self::DeleteActorNamespace { namespace_id } => {
                write!(f, "DeleteActorNamespace({})", namespace_id)
            }
            Self::SendToActor {
                namespace_id,
                actor_id,
                ..
            } => {
                write!(
                    f,
                    "SendToActor(ns={}, actor={} bytes)",
                    namespace_id,
                    actor_id.len()
                )
            }
            Self::DeliverActorMessage { namespace_id, .. } => {
                write!(f, "DeliverActorMessage(ns={})", namespace_id)
            }
            Self::AckActorMessage { namespace_id, .. } => {
                write!(f, "AckActorMessage(ns={})", namespace_id)
            }
            Self::NackActorMessage { namespace_id, .. } => {
                write!(f, "NackActorMessage(ns={})", namespace_id)
            }
            Self::AssignActors {
                namespace_id,
                consumer_id,
                actor_ids,
            } => {
                write!(
                    f,
                    "AssignActors(ns={}, consumer={}, count={})",
                    namespace_id,
                    consumer_id,
                    actor_ids.len()
                )
            }
            Self::ReleaseActors {
                namespace_id,
                consumer_id,
            } => {
                write!(
                    f,
                    "ReleaseActors(ns={}, consumer={})",
                    namespace_id, consumer_id
                )
            }
            Self::EvictIdleActors { namespace_id, .. } => {
                write!(f, "EvictIdleActors(ns={})", namespace_id)
            }
            Self::CreateJob { name, .. } => write!(f, "CreateJob({})", name),
            Self::DeleteJob { job_id } => write!(f, "DeleteJob({})", job_id),
            Self::UpdateJob { job_id, .. } => write!(f, "UpdateJob({})", job_id),
            Self::EnableJob { job_id } => write!(f, "EnableJob({})", job_id),
            Self::DisableJob { job_id } => write!(f, "DisableJob({})", job_id),
            Self::TriggerJob {
                job_id,
                execution_id,
                ..
            } => {
                write!(f, "TriggerJob(job={}, exec={})", job_id, execution_id)
            }
            Self::AssignJob {
                job_id,
                consumer_id,
            } => {
                write!(f, "AssignJob(job={}, consumer={})", job_id, consumer_id)
            }
            Self::CompleteJob {
                job_id,
                execution_id,
            } => {
                write!(f, "CompleteJob(job={}, exec={})", job_id, execution_id)
            }
            Self::FailJob {
                job_id,
                execution_id,
                ..
            } => {
                write!(f, "FailJob(job={}, exec={})", job_id, execution_id)
            }
            Self::TimeoutJob {
                job_id,
                execution_id,
            } => {
                write!(f, "TimeoutJob(job={}, exec={})", job_id, execution_id)
            }
            Self::RegisterConsumer {
                consumer_id,
                group_name,
                ..
            } => {
                write!(
                    f,
                    "RegisterConsumer(id={}, group={})",
                    consumer_id, group_name
                )
            }
            Self::DisconnectConsumer { consumer_id } => {
                write!(f, "DisconnectConsumer({})", consumer_id)
            }
            Self::Heartbeat { consumer_id } => write!(f, "Heartbeat({})", consumer_id),
            Self::RegisterProducer { producer_id, .. } => {
                write!(f, "RegisterProducer({})", producer_id)
            }
            Self::DisconnectProducer { producer_id } => {
                write!(f, "DisconnectProducer({})", producer_id)
            }
            Self::Batch(cmds) => write!(f, "Batch(count={})", cmds.len()),
        }
    }
}

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
}

impl fmt::Display for EntityKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Topic => f.write_str("topic"),
            Self::Queue => f.write_str("queue"),
            Self::ActorNamespace => f.write_str("actor namespace"),
            Self::Job => f.write_str("job"),
            Self::Consumer => f.write_str("consumer"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MqError {
    /// Entity not found (zero-alloc)
    NotFound { entity: EntityKind, id: u64 },
    /// Entity with given name already exists
    AlreadyExists { entity: EntityKind },
    /// Actor mailbox is full
    MailboxFull { pending: u32 },
    /// Dynamic error message (escape hatch)
    Custom(String),
}

impl fmt::Display for MqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { entity, id } => write!(f, "{} {} not found", entity, id),
            Self::AlreadyExists { entity } => write!(f, "{} already exists", entity),
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
    EntityCreated { id: u64 },
    Messages { messages: Vec<DeliveredMessage> },
    Published { offsets: SmallVec<[u64; 16]> },
    Stats(EntityStats),
    BatchResponse(Vec<MqResponse>),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ActorConfig, JobConfig, QueueConfig};

    // =========================================================================
    // Display impls
    // =========================================================================

    #[test]
    fn test_mq_command_display() {
        let cases: Vec<(MqCommand, &str)> = vec![
            (
                MqCommand::CreateTopic {
                    name: "events".to_string(),
                    retention: RetentionPolicy::default(),
                },
                "CreateTopic(events)",
            ),
            (MqCommand::DeleteTopic { topic_id: 42 }, "DeleteTopic(42)"),
            (
                MqCommand::Publish {
                    topic_id: 1,
                    messages: vec![MessagePayload {
                        key: None,
                        value: Bytes::from_static(b"x"),
                        headers: Vec::new(),
                        timestamp: 0,
                    }],
                },
                "Publish(topic=1, count=1)",
            ),
            (
                MqCommand::CreateQueue {
                    name: "tasks".to_string(),
                    config: QueueConfig::default(),
                },
                "CreateQueue(tasks)",
            ),
            (
                MqCommand::Ack {
                    queue_id: 5,
                    message_ids: vec![1, 2, 3],
                },
                "Ack(queue=5, count=3)",
            ),
            (MqCommand::Heartbeat { consumer_id: 99 }, "Heartbeat(99)"),
            (
                MqCommand::DisconnectConsumer { consumer_id: 7 },
                "DisconnectConsumer(7)",
            ),
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
                        payload: MessagePayload {
                            key: None,
                            value: Bytes::new(),
                            headers: Vec::new(),
                            timestamp: 0,
                        },
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
        let cmd = MqCommand::Enqueue {
            queue_id: 5,
            messages: vec![MessagePayload {
                key: Some(Bytes::from_static(b"k")),
                value: Bytes::from_static(b"v"),
                headers: Vec::new(),
                timestamp: 999,
            }],
            dedup_keys: vec![Some(Bytes::from_static(b"dk"))],
        };
        let bytes = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();
        let (decoded, _): (MqCommand, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        match decoded {
            MqCommand::Enqueue {
                queue_id,
                messages,
                dedup_keys,
            } => {
                assert_eq!(queue_id, 5);
                assert_eq!(messages.len(), 1);
                assert_eq!(dedup_keys.len(), 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_mq_response_serde_roundtrip() {
        let resp = MqResponse::Messages {
            messages: vec![DeliveredMessage {
                message_id: 42,
                payload: MessagePayload {
                    key: None,
                    value: Bytes::from_static(b"data"),
                    headers: Vec::new(),
                    timestamp: 1000,
                },
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
        let cmd = MqCommand::Batch(vec![
            MqCommand::CreateTopic {
                name: "t1".to_string(),
                retention: RetentionPolicy::default(),
            },
            MqCommand::Publish {
                topic_id: 1,
                messages: vec![MessagePayload {
                    key: None,
                    value: Bytes::from_static(b"data"),
                    headers: Vec::new(),
                    timestamp: 100,
                }],
            },
        ]);
        let bytes = bincode::serde::encode_to_vec(&cmd, bincode::config::standard()).unwrap();
        let (decoded, _): (MqCommand, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        match decoded {
            MqCommand::Batch(cmds) => {
                assert_eq!(cmds.len(), 2);
                assert!(matches!(cmds[0], MqCommand::CreateTopic { .. }));
                assert!(matches!(cmds[1], MqCommand::Publish { .. }));
            }
            _ => panic!("expected Batch"),
        }

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
        let cmd = MqCommand::Batch(vec![
            MqCommand::DeleteTopic { topic_id: 1 },
            MqCommand::DeleteQueue { queue_id: 2 },
            MqCommand::Heartbeat { consumer_id: 3 },
        ]);
        assert_eq!(format!("{}", cmd), "Batch(count=3)");

        let resp = MqResponse::BatchResponse(vec![MqResponse::Ok, MqResponse::Ok]);
        assert_eq!(format!("{}", resp), "BatchResponse(count=2)");
    }
}

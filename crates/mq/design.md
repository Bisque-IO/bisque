# bisque-mq Design

## Overview

bisque-mq is a distributed messaging engine for bisque. It provides topics, queues, actors, and jobs — all replicated via Raft consensus and persisted using the shared raft log segments for message data and MDBX for manifest/metadata.

Like bisque-lance, bisque-mq is a raft state machine engine. Commands are proposed through Raft, applied deterministically on every replica, and the raft log segments **are** the durable message store. MDBX holds only lightweight metadata: entity definitions, consumer offsets, segment indexes, actor assignments, and job schedules.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  bisque-mq Engine                                       │
│                                                         │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌───────────┐   │
│  │ Topics  │  │ Queues  │  │ Actors  │  │   Jobs    │   │
│  │ (log)   │  │ (tasks) │  │ (serial)│  │ (cron)    │   │
│  └────┬────┘  └────┬────┘  └────┬────┘  └─────┬─────┘   │
│       │            │            │              │        │
│  ┌────▼────────────▼────────────▼──────────────▼─────┐  │
│  │              MQ State Machine                     │  │
│  │  (applies raft log entries, updates MDBX manifest)│  │
│  └────────────────────┬──────────────────────────────┘  │
│                       │                                 │
│  ┌────────────────────▼──────────────────────────────┐  │
│  │              MDBX Manifest                        │  │
│  │  - Entity definitions (topics, queues, actors,    │  │
│  │    jobs, consumers, producers)                    │  │
│  │  - Consumer offsets & ack state                   │  │
│  │  - Segment index (which raft segments hold data   │  │
│  │    for which entity)                              │  │
│  │  - Actor assignments & mailbox metadata           │  │
│  │  - Job schedules, ownership, and execution state  │  │
│  │  - Deduplication window state                     │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
└─────────────────────────────────────────────────────────┘
        │                                     ▲
        │  Raft log segments ARE the          │
        │  durable message store              │
        ▼                                     │
┌──────────────────────────────────────────────────────────┐
│  bisque-raft (shared infrastructure)                     │
│  - Mmap log segments (zero-copy reads)                   │
│  - Multi-raft manager                                    │
│  - TCP transport                                         │
└──────────────────────────────────────────────────────────┘
```

### Key Insight: Raft Log as Message Store

The raft log segments already provide:
- Ordered, durable, replicated append-only storage
- Efficient mmap-based zero-copy reads
- Segment-based lifecycle (sealed, purged)
- CRC64 integrity checks

Instead of maintaining a separate message store, bisque-mq treats raft log entries as the canonical message storage. The MDBX manifest maintains indexes that map entities (topics, queues, actors) to ranges within raft log segments, enabling efficient lookups without duplicating data.

**Retention & Purging:** Because the raft log is shared across all entities in a group, log segments can only be purged when *all* entities referencing them have either consumed the data or explicitly released it. The manifest tracks per-entity minimum required log indexes. The raft purge floor is set to the global minimum across all entities.

---

## Entity Types

### 1. Topics

Kafka-like append-only logs for pub/sub and event streaming.

#### Semantics
- **Append-only**: Messages are never removed or redelivered (log compaction is a future concern).
- **Multi-consumer**: Each consumer tracks its own offset independently.
- **Ordered**: Messages within a topic are totally ordered by raft log index.
- **Optionally partitioned**: By default, a topic is a single ordered log. Topics created with `partition_count > 1` distribute data across multiple raft groups for parallel writes and consumption (see [Topic Partitioning](#topic-partitioning)).

#### Data Model
```
TopicMeta (MDBX)
├── topic_id: u64
├── name: String
├── created_at: u64              (raft log index of creation)
├── retention_policy: RetentionPolicy
│   ├── max_age_secs: Option<u64>
│   ├── max_bytes: Option<u64>
│   └── max_messages: Option<u64>
├── head_index: u64              (raft log index of newest message)
├── tail_index: u64              (oldest non-purged message index)
├── message_count: u64
└── segment_index: Vec<SegmentRange>
    └── { segment_id, min_index, max_index }

TopicConsumerOffset (MDBX)
├── topic_id: u64
├── consumer_id: u64
├── committed_offset: u64       (last acknowledged raft log index)
└── pending_offset: u64         (last delivered but unacknowledged)
```

#### Operations
| Command | Description |
|---------|-------------|
| `CreateTopic { name, retention }` | Register a new topic in the manifest |
| `DeleteTopic { topic_id }` | Mark topic deleted, release segment references |
| `Publish { topic_id, messages }` | Append messages; raft log index becomes the offset |
| `CommitOffset { topic_id, consumer_id, offset }` | Advance consumer's committed position |

#### Consumer Reading
Consumers read directly from raft log segments via mmap. The manifest's segment index maps a desired offset to the correct segment file and byte position. This is a **local read path** — no raft proposal needed for reads, only for offset commits.

---

### 2. Queues

Reliable task queues with at-least-once delivery, visibility timeouts, retries, dead-letter routing, and deduplication.

#### Semantics
- **Competing consumers**: Each message is delivered to exactly one consumer at a time.
- **Visibility timeout**: After delivery, the message is invisible to other consumers for a configurable duration. If not acknowledged within the timeout, it becomes visible again.
- **Retry limit**: Messages exceeding `max_retries` are published to a dead-letter topic (DLQ). Topic-backed DLQs enable fan-out (alerting, retry services, audit), filtering by routing key, and replay from any offset.
- **Deduplication**: Optional sliding-window dedup based on a producer-supplied dedup key. Messages with duplicate keys within the window are silently dropped.
- **Ordering**: Best-effort FIFO. Redeliveries may cause out-of-order processing, but initial delivery follows raft log order.
- **Priority**: Optional integer priority (0 = highest). Higher priority messages are delivered first among visible messages.

#### Data Model
```
QueueMeta (MDBX)
├── queue_id: u64
├── name: String
├── created_at: u64
├── config: QueueConfig
│   ├── visibility_timeout_ms: u64       (default: 30_000)
│   ├── max_retries: u32                 (default: 3)
│   ├── dead_letter_topic_id: Option<u64>
│   ├── dedup_window_secs: Option<u64>   (e.g., 300 = 5 min)
│   ├── delay_default_ms: u64            (default: 0)
│   └── max_in_flight_per_consumer: u32  (default: 100)
├── pending_count: u64          (messages awaiting delivery)
├── in_flight_count: u64        (delivered but unacknowledged)
├── dlq_count: u64
├── segment_index: Vec<SegmentRange>
└── dedup_window: DedupWindow

QueueMessage (MDBX - index only, payload in raft log)
├── message_id: u64             (raft log index)
├── queue_id: u64
├── state: MessageState         (Pending | InFlight | Acked | DeadLetter)
├── priority: u8
├── deliver_after: u64          (timestamp, for delayed messages)
├── attempts: u32
├── last_delivered_at: Option<u64>
├── consumer_id: Option<u64>    (who has it in-flight)
├── visibility_deadline: Option<u64>
└── dedup_key: Option<Bytes>

DedupWindow (in-memory, checkpointed to MDBX)
├── window_secs: u64
└── keys: BTreeMap<u64, HashSet<Bytes>>  (timestamp_bucket → dedup keys)
```

#### Operations
| Command | Description |
|---------|-------------|
| `CreateQueue { name, config }` | Register queue with its configuration |
| `DeleteQueue { queue_id }` | Mark deleted, release segments |
| `Enqueue { queue_id, messages, dedup_keys }` | Add messages; dedup check inline |
| `Deliver { queue_id, consumer_id, max_count }` | Assign pending messages to consumer; set visibility deadline |
| `Ack { queue_id, message_ids }` | Mark messages as completed; remove from index |
| `Nack { queue_id, message_ids }` | Return messages to pending (immediate retry) |
| `PublishToDlq { source_queue_id, dlq_topic_id, dead_letter_ids, messages }` | Publish to DLQ topic after max retries; clean up source queue |
| `ExtendVisibility { queue_id, message_ids, extension_ms }` | Extend the visibility timeout for in-flight messages |

#### Visibility Timeout Enforcement
A leader-driven background task scans in-flight messages whose visibility deadline has passed. For each expired message:
1. If `attempts < max_retries`: re-enqueue as pending (increment attempts).
2. If `attempts >= max_retries`: mark as `DeadLetter`; return `DeadLettered` response so the caller reads the original bytes from the raft log and proposes `PublishToDlq` to publish them to the configured DLQ topic.

This scan is raft-proposed so all replicas agree on timeout expirations deterministically. The leader batches expired messages into a single `TimeoutExpired { queue_id, message_ids }` command.

#### Deduplication
The dedup window is a time-bucketed set of dedup keys. On `Enqueue`:
1. Check if `dedup_key` exists in any bucket within the window.
2. If found, drop the message silently (respond with success to producer).
3. If not, insert into the current bucket and proceed.

Expired buckets are garbage-collected by a leader-driven periodic command `PruneDedupWindow { queue_id, before_timestamp }`.

---

### 3. Actors

Mailbox-style message delivery serialized per actor ID. Each actor is a logical entity that receives messages one at a time through an assigned consumer.

#### Semantics
- **Serialized delivery**: Messages to the same actor ID are delivered strictly in order, one at a time.
- **Consumer affinity**: Each active actor is assigned to exactly one consumer for the duration of that consumer's session.
- **Lazy lifecycle**: Actors are created implicitly on first message. No explicit registration required.
- **Sparse manifest**: Only actors with pending messages or active assignments are tracked in the manifest. Idle actors with no pending messages are evicted from the manifest after a configurable grace period.
- **Backpressure**: Configurable max mailbox depth per actor. Producers receive backpressure errors when exceeded.

#### Data Model
```
ActorNamespaceMeta (MDBX)
├── namespace_id: u64
├── name: String
├── created_at: u64
├── config: ActorConfig
│   ├── max_mailbox_depth: u32           (default: 10_000)
│   ├── idle_eviction_secs: u64          (default: 3600)
│   ├── ack_timeout_ms: u64              (default: 30_000)
│   └── max_retries: u32                 (default: 3)
├── active_actor_count: u64
└── segment_index: Vec<SegmentRange>

ActorState (MDBX - only for actors with pending msgs or active assignments)
├── namespace_id: u64
├── actor_id: Bytes               (application-defined, variable length)
├── assigned_consumer_id: Option<u64>
├── pending_count: u32
├── head_index: u64               (newest message raft log index)
├── tail_index: u64               (oldest undelivered message)
├── in_flight_index: Option<u64>  (currently delivered message)
├── last_activity_at: u64         (for idle eviction)
└── attempts: u32                 (retry count for current message)
```

#### Operations
| Command | Description |
|---------|-------------|
| `CreateActorNamespace { name, config }` | Define an actor namespace |
| `DeleteActorNamespace { namespace_id }` | Remove namespace and all actor state |
| `SendToActor { namespace_id, actor_id, message }` | Enqueue message to actor's mailbox |
| `DeliverActorMessage { namespace_id, actor_id, consumer_id }` | Pop next message from mailbox for assigned consumer |
| `AckActorMessage { namespace_id, actor_id, message_id }` | Acknowledge processed message |
| `NackActorMessage { namespace_id, actor_id, message_id }` | Return message for retry |
| `AssignActors { namespace_id, consumer_id, actor_ids }` | Assign actors to a consumer |
| `ReleaseActors { namespace_id, consumer_id }` | Unassign all actors from a consumer (on disconnect) |

#### Consumer Assignment Strategy
When a consumer joins:
1. Leader identifies unassigned actors with pending messages.
2. Actors are assigned round-robin across connected consumers (balanced by count).
3. When a consumer disconnects, its actors are redistributed to remaining consumers.
4. Rebalancing is proposed through raft to ensure deterministic assignment across replicas.

#### Manifest Eviction
To handle unbounded actor ID growth:
- Actors with `pending_count == 0` and `last_activity_at` older than `idle_eviction_secs` are removed from MDBX.
- If a new message arrives for an evicted actor, it is re-created in the manifest.
- A leader-driven periodic `EvictIdleActors { namespace_id, before_timestamp }` command handles cleanup.

---

### 4. Jobs

Cron-scheduled tasks with cluster-wide singleton execution. Each job is assigned to exactly one consumer at a time, and the cron schedule is driven by the raft leader.

#### Semantics
- **Singleton execution**: At any moment, a job is assigned to at most one consumer across the entire cluster.
- **Cron-driven**: The raft leader evaluates cron schedules and proposes `TriggerJob` commands at the appropriate times.
- **Message input**: Jobs can optionally receive messages (from a configured source topic/queue) as input for each execution.
- **Failure handling**: If a job execution is not acknowledged within its timeout, it is reassigned to another consumer.
- **Overlap prevention**: A new cron trigger is skipped if the previous execution is still in progress (configurable: skip or queue).

#### Data Model
```
JobMeta (MDBX)
├── job_id: u64
├── name: String
├── created_at: u64
├── config: JobConfig
│   ├── cron_expression: String          (standard cron syntax)
│   ├── timezone: String                 (e.g., "UTC")
│   ├── execution_timeout_ms: u64       (default: 300_000 = 5 min)
│   ├── overlap_policy: OverlapPolicy   (Skip | Queue)
│   ├── max_queued: u32                  (if Queue policy, max pending triggers)
│   ├── input_source: Option<InputSource>
│   │   └── { entity_type: Topic|Queue, entity_id: u64 }
│   └── retry_config: RetryConfig
│       ├── max_retries: u32
│       └── retry_delay_ms: u64
├── state: JobState
│   ├── assigned_consumer_id: Option<u64>
│   ├── last_triggered_at: Option<u64>    (raft log index)
│   ├── last_completed_at: Option<u64>
│   ├── next_trigger_at: u64              (wall clock, from cron)
│   ├── current_execution_id: Option<u64>
│   ├── consecutive_failures: u32
│   └── queued_triggers: u32
└── enabled: bool
```

#### Operations
| Command | Description |
|---------|-------------|
| `CreateJob { name, config }` | Register a new cron job |
| `DeleteJob { job_id }` | Remove job |
| `UpdateJob { job_id, config }` | Modify job configuration |
| `EnableJob { job_id }` / `DisableJob { job_id }` | Toggle job scheduling |
| `TriggerJob { job_id, execution_id, triggered_at }` | Leader-proposed cron trigger |
| `AssignJob { job_id, consumer_id }` | Assign job to a consumer |
| `CompleteJob { job_id, execution_id, result }` | Consumer reports completion |
| `FailJob { job_id, execution_id, error }` | Consumer reports failure |
| `TimeoutJob { job_id, execution_id }` | Leader detects execution timeout |

#### Cron Scheduling
The raft leader runs a background timer:
1. Evaluate all enabled jobs' `next_trigger_at` against current wall clock.
2. For due jobs: if no in-flight execution (or `overlap_policy == Queue`), propose `TriggerJob`.
3. After applying `TriggerJob`, compute and store the next `next_trigger_at` from the cron expression.

Wall clock is read only on the leader; the `triggered_at` timestamp in the raft command ensures deterministic replay on followers.

---

## Consumers and Producers

### Consumers

A consumer is a connected client session that pulls messages from one or more entities.

```
ConsumerMeta (MDBX)
├── consumer_id: u64
├── group_name: String              (logical consumer group)
├── connected_at: u64               (raft log index)
├── last_heartbeat_at: u64
├── subscriptions: Vec<Subscription>
│   └── { entity_type, entity_id, config }
├── assigned_actors: Vec<(u64, Bytes)>   (namespace_id, actor_id)
├── assigned_jobs: Vec<u64>
└── in_flight_messages: HashMap<u64, Vec<u64>>  (entity_id → message_ids)
```

#### Consumer Lifecycle
1. **Connect**: Client establishes session; `RegisterConsumer` proposed via raft.
2. **Subscribe**: Consumer declares interest in topics/queues/actor-namespaces/jobs.
3. **Pull**: Consumer requests messages; leader evaluates subscriptions and delivers.
4. **Heartbeat**: Periodic heartbeat keeps session alive. Missed heartbeats trigger `DisconnectConsumer`.
5. **Disconnect**: Graceful or timeout-driven. All in-flight messages returned to pending. Actor assignments and job assignments released.

#### Consumer Groups
Multiple consumers with the same `group_name` form a consumer group:
- **Topics**: Each consumer in the group gets all messages (fan-out). If you want partitioned consumption, use separate topics.
- **Queues**: Messages are distributed across group members (competing consumers).
- **Actors**: Actors are distributed across group members (affinity-based assignment).
- **Jobs**: Each job is assigned to exactly one group member.

### Producers

Producers are simpler — they don't maintain long-lived state beyond session identity.

```
ProducerMeta (MDBX)
├── producer_id: u64
├── name: Option<String>
├── connected_at: u64
└── last_active_at: u64
```

Producers can publish to any entity type. The producer ID is recorded on messages for traceability.

---

## Raft Integration

### Type Configuration

```rust
pub type MqTypeConfig = bisque_raft::BisqueRaftTypeConfig<MqCommand, MqResponse>;
```

### Command Enum

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MqCommand {
    // Topics
    CreateTopic { name: String, retention: RetentionPolicy },
    DeleteTopic { topic_id: u64 },
    Publish { topic_id: u64, messages: Vec<MessagePayload> },
    CommitOffset { topic_id: u64, consumer_id: u64, offset: u64 },
    PurgeTopic { topic_id: u64, before_index: u64 },

    // Queues
    CreateQueue { name: String, config: QueueConfig },
    DeleteQueue { queue_id: u64 },
    Enqueue { queue_id: u64, messages: Vec<MessagePayload>, dedup_keys: Vec<Option<Bytes>> },
    Deliver { queue_id: u64, consumer_id: u64, max_count: u32 },
    Ack { queue_id: u64, message_ids: Vec<u64> },
    Nack { queue_id: u64, message_ids: Vec<u64> },
    ExtendVisibility { queue_id: u64, message_ids: Vec<u64>, extension_ms: u64 },
    TimeoutExpired { queue_id: u64, message_ids: Vec<u64> },
    PruneDedupWindow { queue_id: u64, before_timestamp: u64 },

    // Actors
    CreateActorNamespace { name: String, config: ActorConfig },
    DeleteActorNamespace { namespace_id: u64 },
    SendToActor { namespace_id: u64, actor_id: Bytes, message: MessagePayload },
    DeliverActorMessage { namespace_id: u64, actor_id: Bytes, consumer_id: u64 },
    AckActorMessage { namespace_id: u64, actor_id: Bytes, message_id: u64 },
    NackActorMessage { namespace_id: u64, actor_id: Bytes, message_id: u64 },
    AssignActors { namespace_id: u64, consumer_id: u64, actor_ids: Vec<Bytes> },
    ReleaseActors { namespace_id: u64, consumer_id: u64 },
    EvictIdleActors { namespace_id: u64, before_timestamp: u64 },

    // Jobs
    CreateJob { name: String, config: JobConfig },
    DeleteJob { job_id: u64 },
    UpdateJob { job_id: u64, config: JobConfig },
    EnableJob { job_id: u64 },
    DisableJob { job_id: u64 },
    TriggerJob { job_id: u64, execution_id: u64, triggered_at: u64 },
    AssignJob { job_id: u64, consumer_id: u64 },
    CompleteJob { job_id: u64, execution_id: u64 },
    FailJob { job_id: u64, execution_id: u64, error: String },
    TimeoutJob { job_id: u64, execution_id: u64 },

    // Sessions
    RegisterConsumer { consumer_id: u64, group_name: String, subscriptions: Vec<Subscription> },
    DisconnectConsumer { consumer_id: u64 },
    Heartbeat { consumer_id: u64 },
    RegisterProducer { producer_id: u64, name: Option<String> },
    DisconnectProducer { producer_id: u64 },
}
```

### Response Enum

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MqResponse {
    Ok,
    Error(String),
    EntityCreated { id: u64 },
    Messages { messages: Vec<DeliveredMessage> },
    Published { offsets: Vec<u64> },
    Stats(EntityStats),
}
```

### Message Payload

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagePayload {
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(String, Bytes)>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveredMessage {
    pub message_id: u64,          // raft log index
    pub payload: MessagePayload,
    pub attempt: u32,
    pub original_timestamp: u64,
}
```

---

## MDBX Manifest Layout

All metadata is stored in a per-group MDBX database at `{data_dir}/.mq_groups/{group_id}/`.

| Table | Key | Value | Purpose |
|-------|-----|-------|---------|
| `meta` | `"last_applied"` | `LogId` | Raft state tracking |
| `meta` | `"membership"` | `StoredMembership` | Raft membership |
| `topics` | `topic_id: u64` | `TopicMeta` | Topic definitions |
| `topic_offsets` | `(topic_id, consumer_id)` | `TopicConsumerOffset` | Consumer positions |
| `queues` | `queue_id: u64` | `QueueMeta` | Queue definitions |
| `queue_messages` | `(queue_id, message_id)` | `QueueMessageMeta` | Message state index |
| `queue_pending` | `(queue_id, priority, message_id)` | `()` | Pending delivery index |
| `queue_dedup` | `(queue_id, dedup_key_hash)` | `expiry_timestamp` | Dedup lookup |
| `actor_namespaces` | `namespace_id: u64` | `ActorNamespaceMeta` | Namespace defs |
| `actors` | `(namespace_id, actor_id_hash)` | `ActorState` | Active actor state |
| `actor_mailbox` | `(namespace_id, actor_id_hash, seq)` | `message_raft_index` | Mailbox index |
| `jobs` | `job_id: u64` | `JobMeta` | Job definitions |
| `consumers` | `consumer_id: u64` | `ConsumerMeta` | Consumer sessions |
| `producers` | `producer_id: u64` | `ProducerMeta` | Producer sessions |
| `segments` | `(entity_type, entity_id, segment_id)` | `SegmentRange` | Segment index |
| `id_gen` | `"next_id"` | `u64` | Monotonic ID generator |

### Write Batching

Following the bisque-lance manifest pattern:
- A dedicated thread owns the MDBX write handle.
- State machine `apply()` sends manifest updates via a crossfire mpsc channel.
- The writer thread batches multiple updates into a single MDBX write transaction.
- Read-only transactions are used concurrently for consumer reads (offset lookups, actor assignments, etc.).

---

## Leader-Driven Background Tasks

These tasks run only on the raft leader and propose commands through raft for deterministic application.

| Task | Interval | Description |
|------|----------|-------------|
| Visibility timeout scanner | 1s | Scan in-flight queue messages past deadline; propose `TimeoutExpired` |
| Cron evaluator | 1s | Check job schedules against wall clock; propose `TriggerJob` |
| Job execution timeout | 5s | Check running jobs past execution timeout; propose `TimeoutJob` |
| Consumer heartbeat monitor | 5s | Detect dead consumers; propose `DisconnectConsumer` |
| Actor idle eviction | 60s | Evict actors with no pending messages past grace period |
| Dedup window pruning | 30s | Remove expired dedup buckets |
| Raft log purge advisor | 30s | Compute global min required index across all entities; update purge floor |
| Actor rebalancer | 10s | Rebalance actor assignments when consumer count changes |

---

## Read Path Optimization

For high-throughput reads, consumers can read message payloads directly from mmap'd raft log segments without going through raft:

1. Consumer requests messages (e.g., topic read at offset X).
2. State machine returns `DeliveredMessage` with `message_id` (raft log index).
3. For the payload, the consumer reads directly from the raft log segment using the index.
4. The MDBX segment index maps `(entity_type, entity_id, raft_log_index)` → `(segment_id, byte_offset)`.

This avoids duplicating message data and leverages the kernel page cache for hot data.

For queues and actors, the `Deliver` command is a raft write (it changes message state), but the returned `DeliveredMessage` includes the payload inline to avoid an extra round trip.

---

## Snapshots

### Snapshot Strategy
The MDBX database is the snapshot. On `snapshot()`:
1. Create a consistent MDBX read transaction.
2. Serialize all tables to a binary format.
3. Include `last_applied` log ID so the follower knows where to resume from the raft log.

On `install_snapshot()`:
1. Replace local MDBX with received snapshot.
2. Resume applying raft log entries from `last_applied + 1`.

Note: Message payloads are NOT included in the snapshot — they live in the raft log segments which are transferred separately via the raft snapshot mechanism.

---

## Purge Floor Calculation

The purge floor determines the oldest raft log entry that cannot be deleted. It is the minimum of:

1. **Topic tail indexes**: Oldest non-purged message across all topics (respecting retention policy).
2. **Queue message indexes**: Oldest pending/in-flight message across all queues (dead-lettered messages are cleaned up after publishing to the DLQ topic).
3. **Actor mailbox indexes**: Oldest undelivered message across all active actors.
4. **Raft's own requirements**: Last applied index minus a configured buffer for slow followers.

The leader periodically computes this and communicates it to the raft log storage for segment purging.

---

## Crate Structure

```
crates/mq/
├── Cargo.toml
├── design.md
└── src/
    ├── lib.rs               # Public API, MqTypeConfig
    ├── engine.rs             # MqEngine: core state, apply logic
    ├── state_machine.rs      # MqStateMachine: RaftStateMachine impl
    ├── raft.rs               # MqRaftNode: raft handle + leader tasks
    ├── types.rs              # MqCommand, MqResponse, entity types
    ├── config.rs             # MqConfig, QueueConfig, ActorConfig, etc.
    ├── manifest.rs           # MDBX manifest manager
    ├── topic.rs              # Topic state and operations
    ├── queue.rs              # Queue state, visibility, dedup
    ├── actor.rs              # Actor mailbox, assignment, eviction
    ├── job.rs                # Job scheduling, cron evaluation
    ├── consumer.rs           # Consumer session management
    ├── producer.rs           # Producer session management
    └── purge.rs              # Purge floor calculation
```

### Dependencies

```toml
[dependencies]
bisque-raft = { workspace = true }
bisque-protocol = { workspace = true }
openraft = { workspace = true, features = ["tokio-rt"] }
tokio = { workspace = true, features = ["full"] }
libmdbx = { workspace = true }
serde = { workspace = true, features = ["derive"] }
bincode = { workspace = true, features = ["serde"] }
bytes = { workspace = true }
metrics = { workspace = true }
tracing = { workspace = true }
thiserror = { workspace = true }
parking_lot = { workspace = true }
cron = "0.13"                        # Cron expression parsing
```

---

## Topic Partitioning

### Motivation

A single raft group has a single leader for all writes. For workloads with millions of small messages, this becomes the throughput bottleneck. Topic partitioning distributes writes across multiple independent raft groups, each owning a subset of a topic's data.

This also enables Kafka connector parity: Kafka topics are inherently partitioned, and mapping Kafka partitions to bisque-mq partitions gives a natural 1:1 bridge.

### Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Partitioned Topic "events" (partition_count=4)           │
│                                                           │
│  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐ │
│  │ Partition 0│ │ Partition 1│ │ Partition 2│ │ Partition 3│ │
│  │ group=100 │ │ group=101 │ │ group=102 │ │ group=103 │ │
│  │ (raft)    │ │ (raft)    │ │ (raft)    │ │ (raft)    │ │
│  └───────────┘ └───────────┘ └───────────┘ └───────────┘ │
│        ▲              ▲            ▲             ▲         │
│        │              │            │             │         │
│  ┌─────┴──────────────┴────────────┴─────────────┴─────┐  │
│  │         Partition Router (hash / round-robin)       │  │
│  └─────────────────────────┬───────────────────────────┘  │
│                             │                              │
│  ┌──────────────────────────▼──────────────────────────┐  │
│  │  Coordinator Group (group_id from catalog)          │  │
│  │  - TopicMeta with partition_count & partition_map    │  │
│  │  - Consumer group ↔ partition assignment             │  │
│  │  - Queues, actors, jobs (non-partitioned entities)  │  │
│  └─────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

#### Two-Tier Group Model

**Coordinator group** (the existing single raft group per catalog):
- Stores all entity metadata: `TopicMeta`, `QueueMeta`, etc.
- Stores the **partition map**: topic_id → `Vec<PartitionInfo>` mapping each partition index to its raft group_id
- Handles `CreateTopic` / `DeleteTopic` — allocates partition groups
- Handles all non-partitioned entities (queues, actors, jobs, exchanges, consumers, producers)
- Handles consumer group → partition assignment for partitioned topics

**Partition groups** (one raft group per partition):
- Stores only message data for that partition (via raft log = message store)
- Handles `Publish` and `CommitOffset` for its partition
- Lightweight state machine: only `TopicState` for the single partition it owns
- Independent leader election — partitions can have different leaders for write parallelism

#### Partition Assignment

Messages are assigned to partitions using the **partition key**:

1. **Explicit key**: Producer provides a `key` in `MessagePayload`. The key is hashed (`crc64(key) % partition_count`) to select a partition. Messages with the same key always go to the same partition (ordering guarantee).
2. **No key**: Round-robin or random assignment across partitions (max throughput, no ordering).
3. **Explicit partition**: Producer specifies `partition: Option<u32>` directly (for Kafka connector).

#### Data Model Changes

```
TopicMeta (MDBX — coordinator group)
├── topic_id: u64
├── name: String
├── ... (existing fields)
├── partition_count: u32             (0 or 1 = unpartitioned, >1 = partitioned)
└── partitions: Vec<PartitionInfo>   (empty if unpartitioned)

PartitionInfo
├── partition_index: u32
├── group_id: u64                    (raft group owning this partition)
└── status: PartitionStatus          (Active | Draining | Inactive)
```

**Unpartitioned topics** (`partition_count <= 1`) work exactly as they do today — all data goes through the coordinator group's raft log. Zero behavioral change, zero overhead for existing topics.

**Partitioned topics** (`partition_count > 1`) have their data spread across partition groups. The coordinator stores only metadata; the partition groups store the actual messages.

#### Operations

| Operation | Where It Runs |
|-----------|--------------|
| `CreateTopic { partition_count > 1 }` | Coordinator: allocates N partition groups, stores partition map |
| `CreateTopic { partition_count <= 1 }` | Coordinator: existing behavior (data in coordinator group) |
| `Publish { topic_id, messages }` (unpartitioned) | Coordinator group (existing) |
| `Publish { topic_id, partition, messages }` (partitioned) | Routed to partition group by key/index |
| `CommitOffset` (partitioned) | Partition group (each partition tracks offsets independently) |
| `Subscribe` (partitioned) | Consumer gets assigned specific partitions via coordinator |
| `DeleteTopic` (partitioned) | Coordinator: marks partitions draining, then removes groups |

#### Consumer Group Partition Assignment

When a consumer subscribes to a partitioned topic:
1. Coordinator assigns partitions to consumers in the group (range or round-robin strategy)
2. Each consumer receives a `Subscribed` response with its assigned partition indexes
3. Consumer reads from assigned partition groups directly
4. On rebalance (consumer join/leave), coordinator reassigns and notifies consumers

```
PartitionAssignment (in coordinator ConsumerState)
├── topic_id: u64
├── consumer_id: u64
└── assigned_partitions: Vec<u32>    (partition indexes)
```

### Wire Protocol Changes

The existing `ClientFrame::Subscribe` already has `group_id` and `entity_type`/`name_hash`. For partitioned topics, the handler resolves the partition map and subscribes the consumer to the appropriate partition group(s).

For `ClientFrame::Publish`, the `group_id` identifies the coordinator, but messages for partitioned topics need routing to partition groups. The handler performs this routing transparently — the producer publishes to the topic and the server routes to the correct partition based on the message key.

### Implementation Phases

**Phase 1: Partition metadata** (this PR)
- Add `partition_count` and `partitions: Vec<PartitionInfo>` to `TopicMeta`
- Add `PartitionInfo` and `PartitionStatus` types
- Update `CreateTopic` command to accept `partition_count`
- Update `MqEngine::apply_command` to store partition metadata
- Update `MqRouter` trait with `get_partition_batcher(group_id, partition_index)` method
- No behavioral change for `partition_count <= 1`

**Phase 2: Partition group provisioning**
- `MqState::ensure_provisioned` creates N partition raft groups for partitioned topics
- Each partition group has its own `MqWriteBatcher`
- `BisqueMqRouter` tracks partition group batchers in a `DashMap<(u64, u32), Arc<MqWriteBatcher>>`

**Phase 3: Partition-aware publish and consume**
- Handler routes `Publish` to correct partition group based on message key
- Handler assigns partitions to consumers on `Subscribe`
- Consumer offset tracking per-partition

**Phase 4: Rebalancing**
- Consumer group rebalance on join/leave
- Partition draining for topic deletion
- Dynamic partition count changes (split/merge — future)

---

## Future Considerations (Out of Scope)

- **Topic compaction**: Key-based log compaction (keep only latest value per key).
- **Transactions**: Atomic publish across multiple topics/queues.
- **Schema registry**: Message schema validation (Protobuf, Avro, JSON Schema).
- **Consumer lag monitoring**: Metrics and alerting for slow consumers.
- **Rate limiting**: Per-producer and per-consumer rate limits.
- **Message filtering**: Server-side subscription filters (header-based, content-based).
- **Delayed/scheduled messages**: Queue messages with a future delivery time (partially supported via `delay_default_ms`).
- **Cross-cluster federation**: Multi-cluster topic mirroring and routing.
- **Dynamic repartitioning**: Changing partition count on a live topic (split/merge).

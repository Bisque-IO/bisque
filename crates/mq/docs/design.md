# Unified Storage Architecture: Topics as the Only Storage Primitive

## Context

bisque-mq currently has four separate entity types — Topics, Queues, Actors, Jobs — each with its own storage model, state machine, command tags, snapshot/recovery paths, and purge logic. This creates significant code duplication and prevents cross-cutting capabilities (e.g., replaying queue messages, attaching analytics to actor traffic). Jobs are just cron-triggered single executions — they don't need their own variant.

**Goal:** Converge all four into a unified model where **topics are the only storage primitive** and **consumer groups are the only consumption primitive**, with four variant modes that preserve the full semantics of each original entity type.

**Outcome:** One storage layer, one purge system, one snapshot path for message data. Queues become replayable. Actors become replayable. Multiple consumption patterns can coexist on the same data stream.

---

## Unified Model Overview

```
                    ┌───────────────────────────────┐
                    │       Topic (log storage)     │
                    │  append-only, retention,      │
                    │  dedup, retained, cron, parts │
                    └──────────────┬────────────────┘
                                   │
                ┌──────────────────┼──────────────────┐
                ▼                  ▼                  ▼
          ┌──────────┐      ┌──────────┐      ┌───────────┐
          │  Offset  │      │   Ack    │      │   Actor   │
          │  Group   │      │  Group   │      │   Group   │
          │ (Kafka)  │      │(Queue/   │      │ (Mailbox) │
          │          │      │  Job)    │      │           │
          └──────────┘      └──────────┘      └───────────┘
```

All messages go into topics. Consumer groups read from topics with variant-specific semantics.

---

## 1. Topic Changes

### New: Topic-Level Dedup

Dedup moves from queues to the topic. Configured at creation time, applied at publish time before offset assignment.

```rust
// Added to TopicState
pub struct TopicDedupConfig {
    pub window_secs: u64,  // dedup window duration
}

// TopicState gains:
pub(crate) dedup_config: Option<TopicDedupConfig>,
pub(crate) dedup: Option<Mutex<DedupWindow>>,  // reuse existing DedupWindow from queue.rs
```

`apply_publish` checks dedup (if configured) before assigning offsets. `TAG_PRUNE_DEDUP_WINDOW` becomes topic-level.

### New: Retained Messages

Topics can natively hold a retained message — the last published message, always available to new subscribers. Set at creation time, immutable.

```rust
// TopicState gains:
pub(crate) retained: bool,                          // if true, topic keeps last message for new subscribers
pub(crate) retained_message: Mutex<Option<Bytes>>,  // the current retained message bytes (FlatMessage)
```

**Behavior:**
- On `apply_publish` to a retained topic: overwrite `retained_message` with the last message in the batch.
- On publish with empty payload to a retained topic: clear `retained_message` (MQTT 5.0 semantics — empty payload deletes retained state).
- On subscribe (any variant): if topic has `retained=true` and `retained_message.is_some()`, deliver it immediately to the new subscriber with a retained flag.
- Retained message is included in topic snapshots for recovery.
- Retained message survives purge — it is logically separate from the topic log. Purge removes historical messages but the retained message is always the latest publish, not a log entry.

**Orthogonal to RetentionPolicy:** Retention governs how long historical log entries are kept. Retained governs whether the latest message is always available to new subscribers. A topic can have `max_age: 1h` (purge old messages) and `retained: true` (last message always available) simultaneously.

**Protocol mapping:**

| Protocol | Current Approach | Unified Approach |
|----------|-----------------|-----------------|
| **MQTT** | Separate `$mqtt/retained/{topic}` topic per retained message with `max_messages=1` | Main subscription topic has `retained=true`; no extra topics needed |
| **AMQP** | `ExchangeState.retained: HashMap<String, Bytes>` on exchanges | Exchange reads retained message from target topic; exchange-level retained map removed |
| **Native (mq-server)** | Not supported | Subscribers receive last message on subscribe — enables "current state" patterns (config, presence, last-known-good) |
| **Kafka** | Not applicable (Kafka has no retained concept) | `retained=false` (default) — no behavioral change |
| **SQS** | Not applicable | `retained=false` (default) — no behavioral change |

**MQTT retained message flow (simplified):**
```
PUBLISH retain=1, topic="sensor/temp", payload="22.5°C"
  → publish_to_exchange(mqtt_exchange, msg)        // fan-out to subscribers
  → also: publish(topic_id, msg)                   // topic has retained=true
  → topic.retained_message = Some(msg_bytes)       // overwrite retained

SUBSCRIBE filter="sensor/temp"
  → create Ack group on subscription topic
  → check source exchange's target topics for retained messages
  → if topic.retained=true && topic.retained_message.is_some():
      deliver retained message immediately with retain=1 flag
```

This eliminates the current `$mqtt/retained/*` topic proliferation — potentially thousands of tiny topics replaced by a single boolean flag per topic.

### New: Cron Auto-Publish

Topics can optionally auto-publish trigger messages on a cron schedule. This replaces the Job entity type entirely — jobs become "a topic with cron + an Ack group with `max_in_flight=1`."

```rust
pub struct TopicCronConfig {
    pub cron_expression: String,   // standard cron expression
    pub timezone: String,          // IANA timezone (e.g., "UTC", "America/New_York")
    pub max_pending: u32,          // max queued triggers (overlap policy: 1 = skip, N = queue)
    pub payload: Option<Bytes>,    // optional static payload for trigger messages
}

// TopicState gains:
pub(crate) cron_config: Option<TopicCronConfig>,
pub(crate) cron_enabled: AtomicBool,       // pause/resume without deleting config
pub(crate) cron_next_trigger_at: AtomicU64, // next scheduled trigger time
pub(crate) cron_last_triggered_at: AtomicU64,
```

**Behavior:**
- The engine's periodic timer loop checks all topics with `cron_config.is_some()` and `cron_enabled == true`.
- When `current_time >= cron_next_trigger_at`: publish a trigger message to the topic, advance `cron_next_trigger_at` to the next cron slot.
- The trigger publish is a raft command (`TAG_CRON_TRIGGER`) so it only fires on the leader and is replicated.
- `max_pending`: if the topic's attached Ack group has `pending_count >= max_pending`, skip this trigger (overlap_policy=Skip semantics). Set `max_pending=1` for "skip if busy", higher values for "queue up to N".
- Enable/disable via `TAG_CRON_ENABLE` / `TAG_CRON_DISABLE` commands.
- Cron config can be updated via `TAG_CRON_UPDATE` (changes expression, timezone, max_pending).

**Job semantics via cron + Ack group:**

| Old Job Concept | Unified Equivalent |
|----------------|-------------------|
| `create_job(name, cron, ...)` | `create_topic(name, cron_config=Some(...))` + `create_consumer_group(name, Ack, visibility_timeout=execution_timeout, max_in_flight=1)` |
| `delete_job` | `delete_consumer_group` + `delete_topic` |
| `enable_job` / `disable_job` | `TAG_CRON_ENABLE` / `TAG_CRON_DISABLE` on topic |
| `trigger_job` (manual) | `publish(topic_id, trigger_msg)` |
| `assign_job` | Ack group `group_deliver` (consumer gets the trigger message) |
| `complete_job` | `group_ack` |
| `fail_job` | `group_nack` (increments attempts, DLQ if max_retries exceeded) |
| `timeout_job` | `group_timeout_expired` (visibility timeout fires) |
| `overlap_policy: Skip` | `max_pending = 1` on cron config |
| `overlap_policy: Queue` | `max_pending = N` on cron config |
| `execution_timeout_ms` | `AckVariantConfig.visibility_timeout_ms` |
| `consecutive_failures` | `AckMessageMeta.attempts` (per trigger message) |
| `max_retries` | `AckVariantConfig.max_retries` (trigger → DLQ after N failures) |

### New: Auto-Delete Policy

Topics can be marked with a lifecycle policy for AMQP dynamic nodes and MQTT transient subscriptions:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopicLifetimePolicy {
    Permanent,          // default — never auto-deleted
    DeleteOnLastDetach, // delete when last consumer group detaches (AMQP DeleteOnNoLinks)
}
```

When the last consumer group attached to a topic with `DeleteOnLastDetach` is deleted, the engine auto-deletes the topic. This replaces AMQP's dynamic node destruction and MQTT's transient subscription queue cleanup.

### New: Consumer Group Purge Floor

Topics currently track `consumer_offsets: DashMap<u64, TopicConsumerOffset>` for direct subscribers. Add:

```rust
// TopicState gains:
pub(crate) consumer_group_ids: DashMap<u64, ()>,  // groups attached to this topic
pub(crate) lifetime_policy: TopicLifetimePolicy,
```

Purge floor for a topic = `min(retention_floor, min(all attached consumer groups' min_required_offset))`. Each variant computes its floor differently:
- **Offset:** `min(committed offsets)`
- **Ack:** `min(pending + in_flight message offsets)`
- **Actor:** `min(tail_index across all actors)`

### Unchanged
- head_index, tail_index, message_count, total_bytes, latest_log_index
- Partitioned topics (each partition = separate raft group)
- RetentionPolicy
- Direct consumer offset tracking (legacy/simple consumers)

---

## 2. Consumer Group Variants

### GroupVariant Enum

```rust
#[repr(u8)]
pub enum GroupVariant {
    Offset = 0,  // Kafka-compatible offset tracking
    Ack    = 1,  // Per-message ack/nack/release/modify (queue + job semantics)
    Actor  = 2,  // Per-actor-key serialized delivery
}
```

Set at creation time, immutable. Every consumer group has exactly one source topic.

### ConsumerGroupMeta Additions

```rust
pub variant: GroupVariant,          // defaults to Offset
pub source_topic_id: u64,          // topic this group reads from
pub variant_config: VariantConfig, // variant-specific config
```

### ConsumerGroupState Additions

```rust
// Added field
pub(crate) variant_state: VariantState,
```

### VariantState Enum

```rust
pub enum VariantState {
    Offset,                    // no additional state (offsets DashMap suffices)
    Ack(AckVariantState),      // queue + job semantics
    Actor(ActorVariantState),  // per-actor-key serialized delivery
}
```

---

## 3. Variant Details

### 3a. Offset Variant (unchanged)

Existing consumer group behavior. Kafka-compatible offset commits, partition assignment, rebalancing, session management. No per-message state.

### 3b. Ack Variant (replaces Queues)

**Config:**
```rust
pub struct AckVariantConfig {
    pub visibility_timeout_ms: u64,        // default 30_000
    pub max_retries: u32,                  // 0 = no DLQ, infinite retries
    pub dead_letter_topic: Option<String>, // None = auto-name "{group}/dlq" when max_retries > 0
    pub delay_default_ms: u64,             // default 0
    pub max_in_flight_per_consumer: u32,   // default 1000
}
```

See Section 4 for DLQ naming conventions. `dedup_window_secs` moves to `TopicDedupConfig` on the source topic.

**Per-Message Metadata:**
```rust
pub struct AckMessageMeta {
    pub message_id: u64,         // = topic offset
    pub group_id: u64,           // owning consumer group
    pub state: MessageState,     // Pending | InFlight | Acked | DeadLetter | Received | Released
    pub priority: u8,
    pub deliver_after: u64,      // absolute ms timestamp for delayed delivery
    pub attempts: u32,
    pub last_delivered_at: Option<u64>,
    pub consumer_id: Option<u64>,
    pub visibility_deadline: Option<u64>,
    pub expires_at: Option<u64>, // absolute ms, from flat message ttl_ms
    pub reply_to: Option<Bytes>, // topic name for request/reply routing
    pub correlation_id: Option<Bytes>, // opaque correlation data
    pub value_len: u32,          // payload size for byte accounting
}
```

This is the current `QueueMessageMeta` with `queue_id` renamed to `group_id` and `message_id` keyed by topic offset instead of raft log index.

**State** (migrated from `QueueState`):
```rust
pub struct AckVariantState {
    messages: DashMap<u64, AckMessageMeta>,             // keyed by topic offset
    pending: Mutex<BTreeMap<(u8, u64), ()>>,            // (priority, offset)
    in_flight_deadlines: Mutex<BTreeMap<u64, SmallVec<[u64; 4]>>>,
    consumer_in_flight: DashMap<u64, SmallVec<[u64; 8]>>,
    expires_at_deadlines: Mutex<BTreeMap<u64, SmallVec<[u64; 4]>>>,
    // Atomic counters
    pending_count: AtomicU64,
    in_flight_count: AtomicU64,
    dlq_count: AtomicU64,
    pending_bytes: AtomicU64,
    in_flight_bytes: AtomicU64,
    total_messages: AtomicU64,
    total_bytes: AtomicU64,
    cached_min_message_id: AtomicU64,
    // Pre-initialized metrics handles
    m_enqueue_count: metrics::Counter,
    m_deliver_count: metrics::Counter,
    m_ack_count: metrics::Counter,
    m_nack_count: metrics::Counter,
    m_dlq_count: metrics::Counter,
    m_timeout_count: metrics::Counter,
}
```

**Full feature parity with current queues:** ack, nack, release, modify, visibility timeout, DLQ, priority ordering, per-message delay, per-message TTL, request/reply routing (reply_to + correlation_id), consumer disconnect handling, MQTT QoS 2 states (Received/Released).

**Key difference:** `message_id` = topic offset (not raft log index). This is a clean 1:1 mapping since topics assign dense sequential offsets.

**Settlement semantics (4 actions):**

| Action | Behavior | Used By |
|--------|----------|---------|
| **Ack** (Accept) | Remove from in-flight, mark Acked. If `reply_to` is set, publish response to reply topic. | All adapters |
| **Nack** (Reject) | Increment attempts. If `attempts >= max_retries`, move to DLQ topic. Else return to pending. | MQTT, SQS, native |
| **Release** | Return to pending without incrementing attempts. Resets visibility deadline. | AMQP Released outcome |
| **Modify** | Return to pending, increment attempts, update `deliver_after` if specified. | AMQP Modified outcome |

The current `MessageState` enum already has the right variants:
```rust
pub enum MessageState {
    Pending,
    InFlight,
    Acked,
    DeadLetter,
    Received,   // MQTT QoS 2: PUBREC sent, awaiting PUBREL
    Released,   // MQTT QoS 2: PUBREL received, awaiting PUBCOMP
}
```

### 3c. Actor Variant (replaces Actor Namespaces)

**Config:**
```rust
pub struct ActorVariantConfig {
    pub max_mailbox_depth: u32,
    pub idle_eviction_secs: u64,
    pub ack_timeout_ms: u64,
    pub max_retries: u32,
}
```

**State** (migrated from `ActorNamespaceState` in `actor.rs`):
```rust
pub struct ActorVariantState {
    actors: DashMap<Bytes, ActorInMemory>,              // keyed by actor_id
    consumer_assignments: DashMap<u64, HashSet<Bytes>>, // consumer → actors
    active_count: AtomicU64,
    cached_min_required: AtomicU64,
    // Pre-initialized metrics handles
    m_send_count: metrics::Counter,
    m_deliver_count: metrics::Counter,
    m_ack_count: metrics::Counter,
    m_evict_count: metrics::Counter,
}
```

`ActorInMemory` unchanged — mailbox entries reference topic offsets. Per-actor serialized delivery (one in-flight), reply-to routing on ack, idle eviction. Dynamic actor creation on first message.

**Actor routing:** The actor_id is carried in the published message as a routing key/header. The Actor variant reads the topic and dispatches per-key. Dedicated topics per actor namespace (auto-created).

---

## 4. Unified Naming

All entity names live in a single global namespace. Since every entity is ultimately backed by a topic, names must be unique across the entire system — there is no per-type namespace.

### Name Resolution

```rust
// MqMetadata:
pub(crate) names: DashMap<u64, NameEntry>,  // name_hash → entry

pub struct NameEntry {
    pub name: String,
    pub topic_id: u64,                        // every name maps to a topic
    pub consumer_group_id: Option<u64>,       // if auto_create_topic was used
}
```

`name_hash(name)` → lookup in `names` → get `topic_id` (for publish) or `consumer_group_id` (for consume). This replaces `topics_by_name`, `queues_by_name`, `namespace_names`, `consumer_group_names`.

**Conflict detection:** `CREATE_TOPIC` and `CREATE_CONSUMER_GROUP` both check `names` first. If the name is taken, return `MqError::AlreadyExists` regardless of what type of entity holds it.

### Dead Letter Topic Naming

When an Ack-variant consumer group is created with `max_retries > 0` and DLQ enabled:

- **Explicit DLQ name:** The caller specifies `dead_letter_topic: Some("my-dlq")`. The engine looks up or creates a topic with that name. The DLQ topic has no default consumer group — it's a plain topic that can be consumed by any group (Offset for replay, Ack for reprocessing, etc.).

- **Auto-generated DLQ name:** If `dead_letter_topic` is `None` but DLQ is enabled (`max_retries > 0`), the engine creates a DLQ topic named `{group_name}/dlq`. For example, group `"orders"` gets DLQ topic `"orders/dlq"`.

- **DLQ topic properties:** Created with `retained=false`, `dedup=None`, `lifetime=Permanent` (DLQ data should never auto-delete). No consumer group is auto-created on the DLQ topic — the operator decides how to consume it.

```rust
pub struct AckVariantConfig {
    pub visibility_timeout_ms: u64,
    pub max_retries: u32,                  // 0 = no DLQ, infinite retries
    pub dead_letter_topic: Option<String>, // None = auto-name "{group}/dlq" if max_retries > 0
    pub delay_default_ms: u64,
    pub max_in_flight_per_consumer: u32,
}
```

**DLQ message flow:**
```
Message fails max_retries times
  → Engine publishes message to DLQ topic (with original headers + failure metadata)
  → AckVariantState removes message from pending/in-flight
  → dlq_count incremented
  → DLQ topic is a normal topic — operator can:
     - Create an Offset group to replay DLQ messages
     - Create an Ack group to reprocess with ack/nack
     - Tail directly for monitoring/alerting
```

---

## 5. Command Tag Allocation

No old tags are preserved. Clean allocation from scratch.

### Topic Tags

| Tag | Constant | Description |
|-----|----------|-------------|
| 0 | TAG_CREATE_TOPIC | Create topic with retention, dedup, retained, lifetime |
| 1 | TAG_DELETE_TOPIC | Delete topic |
| 2 | TAG_PUBLISH | Publish messages to topic |
| 3 | TAG_COMMIT_OFFSET | Commit consumer offset on topic |
| 4 | TAG_PURGE_TOPIC | Purge topic messages up to offset |
| 5 | TAG_PRUNE_TOPIC_DEDUP | Prune topic-level dedup window |

### Exchange Tags

| Tag | Constant | Description |
|-----|----------|-------------|
| 6 | TAG_CREATE_EXCHANGE | Create exchange |
| 7 | TAG_DELETE_EXCHANGE | Delete exchange |
| 8 | TAG_CREATE_BINDING | Create binding (exchange → topic) |
| 9 | TAG_DELETE_BINDING | Delete binding |
| 10 | TAG_PUBLISH_TO_EXCHANGE | Publish to exchange (fan-out to bound topics) |

### Consumer Group Tags (all variants)

| Tag | Constant | Description |
|-----|----------|-------------|
| 11 | TAG_CREATE_CONSUMER_GROUP | Create group (any variant, optional auto_create_topic) |
| 12 | TAG_DELETE_CONSUMER_GROUP | Delete group (+ auto-delete topic if DeleteOnLastDetach) |
| 13 | TAG_JOIN_CONSUMER_GROUP | Kafka-style join (Offset variant) |
| 14 | TAG_SYNC_CONSUMER_GROUP | Kafka-style sync (Offset variant) |
| 15 | TAG_LEAVE_CONSUMER_GROUP | Leave group |
| 16 | TAG_HEARTBEAT_CONSUMER_GROUP | Group-level heartbeat |
| 17 | TAG_COMMIT_GROUP_OFFSET | Commit offset within group |
| 18 | TAG_EXPIRE_GROUP_OFFSETS | Expire stale group offsets |

### Ack Variant Tags

| Tag | Constant | Description |
|-----|----------|-------------|
| 19 | TAG_GROUP_DELIVER | Deliver pending messages to consumer |
| 20 | TAG_GROUP_ACK | Acknowledge messages (accept) |
| 21 | TAG_GROUP_NACK | Reject messages (increment attempts, DLQ if max) |
| 22 | TAG_GROUP_RELEASE | Release messages (requeue, no attempt increment) |
| 23 | TAG_GROUP_MODIFY | Modify messages (requeue with changes) |
| 24 | TAG_GROUP_EXTEND_VISIBILITY | Extend visibility timeout |
| 25 | TAG_GROUP_TIMEOUT_EXPIRED | Internal: visibility timeout fired |
| 26 | TAG_GROUP_EXPIRE_PENDING | Internal: expire messages past TTL |
| 27 | TAG_GROUP_PURGE | Purge all group state |
| 28 | TAG_GROUP_GET_ATTRIBUTES | Get group stats (pending/in_flight/dlq counts) |
| 29 | TAG_PUBLISH_TO_DLQ | Internal: move message to DLQ topic |

### Actor Variant Tags

| Tag | Constant | Description |
|-----|----------|-------------|
| 30 | TAG_GROUP_DELIVER_ACTOR | Deliver next actor message |
| 31 | TAG_GROUP_ACK_ACTOR | Ack actor message (with optional reply) |
| 32 | TAG_GROUP_NACK_ACTOR | Nack actor message |
| 33 | TAG_GROUP_ASSIGN_ACTORS | Assign actors to consumer |
| 34 | TAG_GROUP_RELEASE_ACTORS | Release actors from consumer |
| 35 | TAG_GROUP_EVICT_IDLE | Evict idle actors |

### Cron Tags (topic-level)

| Tag | Constant | Description |
|-----|----------|-------------|
| 36 | TAG_CRON_TRIGGER | Internal: cron timer fired, publish trigger message |
| 37 | TAG_CRON_ENABLE | Enable cron on topic |
| 38 | TAG_CRON_DISABLE | Disable cron on topic |
| 39 | TAG_CRON_UPDATE | Update cron config (expression, timezone, max_pending) |

### Session Tags

| Tag | Constant | Description |
|-----|----------|-------------|
| 40 | TAG_REGISTER_SESSION | Register session (client_id, keep_alive, expiry, will) |
| 41 | TAG_DISCONNECT_SESSION | Disconnect session (reason, publish_will flag) |
| 42 | TAG_HEARTBEAT | Session heartbeat |
| 43 | TAG_SET_WILL | Update will on active session |
| 44 | TAG_CLEAR_WILL | Remove will from active session |
| 45 | TAG_SCHEDULE_WILL | Internal: schedule delayed will publish |
| 46 | TAG_CANCEL_WILL | Internal: cancel pending delayed will |
| 47 | TAG_FIRE_WILL | Internal: delayed will timer fired |
| 48 | TAG_EXPIRE_SESSION | Internal: session expiry timer fired |

### Batch Tag

| Tag | Constant | Description |
|-----|----------|-------------|
| 49 | TAG_BATCH | Batch of sub-commands (atomic execution) |

### Exchange Binding

```rust
pub struct Binding {
    pub binding_id: u64,
    pub exchange_id: u64,
    pub target_topic_id: u64,   // exchanges route to topics
    pub routing_key: Option<String>,
    pub no_local: bool,
    pub shared_group: Option<String>,
    pub subscription_id: Option<u32>,
}
```

---

## 6. MqMetadata

```rust
pub struct MqMetadata {
    // Core entities
    pub(crate) topics: DashMap<u64, TopicState>,
    pub(crate) consumer_groups: DashMap<u64, ConsumerGroupState>,
    pub(crate) exchanges: DashMap<u64, ExchangeState>,
    pub(crate) sessions: DashMap<u64, SessionState>,

    // Unified name index (single namespace)
    pub(crate) names: DashMap<u64, NameEntry>,              // name_hash → entry

    // Session indexes
    pub(crate) session_client_index: DashMap<u64, u64>,     // client_id_hash → session_id
    pub(crate) pending_wills: DashMap<u64, PendingWill>,    // session_id → delayed will
}
```

No `queues`, `actor_namespaces`, `jobs`, `consumers`, `producers` DashMaps. No per-type name maps. One `names` index covers everything.

---

## 7. Snapshot/Recovery

### Snapshot Structure

```rust
pub struct MqSnapshotData {
    pub topics: Vec<TopicSnapshot>,                   // includes retained_message per topic
    pub consumer_groups: Vec<ConsumerGroupSnapshot>,  // all variants
    pub exchanges: Vec<ExchangeSnapshot>,
    pub sessions: Vec<SessionSnapshot>,               // persistent + active sessions
    pub pending_wills: Vec<PendingWill>,              // delayed will messages
}
```

### ConsumerGroupSnapshot

```rust
pub struct ConsumerGroupSnapshot {
    pub meta: ConsumerGroupMeta,  // includes variant, source_topic_id, variant_config
    pub offsets: Vec<GroupTopicPartitionOffset>,
    pub ack_state: Option<AckStateSnapshot>,
    pub actor_state: Option<ActorStateSnapshot>,
}
```

Recovery dispatches on `meta.variant` to rebuild the correct `VariantState`.

---

## 8. Protocol Adapter Mapping

This section defines exactly how each protocol adapter maps onto the unified model, ensuring no functionality is lost.

### 7a. Kafka Adapter (mq-kafka)

**Current state:** Already uses topics + consumer groups (Offset variant). Cleanest fit.

**Changes required:** Minimal.

| Kafka Operation | Current MQ Operation | Unified Operation | Notes |
|----------------|---------------------|-------------------|-------|
| Produce | `MqCommand::publish(topic_id, msgs)` | Unchanged | |
| Fetch | `log_reader.read_topic_flat_messages()` | Unchanged | Zero-copy mmap path |
| CreateTopics | `MqCommand::create_topic(name, retention, 0)` | Unchanged | Per-partition topics (`{name}-{i}`) |
| DeleteTopics | `MqCommand::delete_topic(topic_id)` | Unchanged | |
| ListOffsets | `log_reader.get_topic_head/tail()` | Unchanged | |
| JoinGroup | `MqCommand::join_consumer_group(...)` | Unchanged | Offset variant (default) |
| SyncGroup | `MqCommand::sync_consumer_group(...)` | Unchanged | |
| LeaveGroup | `MqCommand::leave_consumer_group(...)` | Unchanged | |
| Heartbeat | `MqCommand::heartbeat_consumer_group(...)` | Unchanged | |
| OffsetCommit | `MqCommand::commit_group_offset(...)` | Unchanged | |
| OffsetFetch | `metadata.get_consumer_group().get_offset()` | Unchanged | |

**No breaking changes.** The Kafka adapter already operates on the target model.

### 7b. SQS Adapter (mq-sqs)

**Current state:** Uses queues exclusively. Must migrate to topic + Ack-variant consumer group.

**Compound create:** `create_queue(name, config)` becomes an atomic two-step:
1. Create topic `name` with `dedup_config` (if FIFO) and `RetentionPolicy::default()`
2. Create Ack-variant consumer group `name` with `source_topic_id` pointing to that topic

The engine handles this atomically via `CREATE_CONSUMER_GROUP` with `variant=Ack` and `auto_create_topic=true`.

| SQS Operation | Current MQ Operation | Unified Operation | Notes |
|---------------|---------------------|-------------------|-------|
| CreateQueue | `MqCommand::create_queue(name, config)` | `MqCommand::create_consumer_group(name, variant=Ack, config, auto_create_topic=true)` | Engine creates topic + group atomically |
| DeleteQueue | `MqCommand::delete_queue(queue_id)` | `MqCommand::delete_consumer_group(group_id)` | Topic auto-deleted if `DeleteOnLastDetach` |
| SendMessage | `MqCommand::enqueue(queue_id, msgs, dedup)` | `MqCommand::publish(source_topic_id, msgs)` | Dedup at topic level if FIFO |
| ReceiveMessage | `MqCommand::deliver(queue_id, consumer_id, max)` | `MqCommand::group_deliver(group_id, consumer_id, max)` | |
| DeleteMessage | `MqCommand::ack(queue_id, msg_ids, None)` | `MqCommand::group_ack(group_id, msg_ids)` | |
| ChangeMessageVisibility | `MqCommand::extend_visibility(queue_id, msg_ids, ms)` | `MqCommand::group_extend_visibility(group_id, msg_ids, ms)` | |
| PurgeQueue | `MqCommand::purge_queue(queue_id)` | `MqCommand::group_purge(group_id)` | Resets group state; topic data retained per retention policy |
| GetQueueAttributes | `MqCommand::get_queue_attributes(queue_id)` | `MqCommand::group_get_attributes(group_id)` | Returns pending/in_flight/dlq counts |

**SQS `QueueConfig` mapping:**
```
QueueConfig.visibility_timeout_ms  → AckVariantConfig.visibility_timeout_ms
QueueConfig.max_retries            → AckVariantConfig.max_retries
QueueConfig.dead_letter_topic_id   → AckVariantConfig.dead_letter_topic (name-based, auto "{group}/dlq")
QueueConfig.delay_default_ms       → AckVariantConfig.delay_default_ms
QueueConfig.max_in_flight_per_consumer → AckVariantConfig.max_in_flight_per_consumer
QueueConfig.dedup_window_secs      → TopicDedupConfig.window_secs (on source topic)
```

**SQS receipt handle:** Currently encodes `[queue_id:u64][message_id:u64]` → base64url. In the unified model, `queue_id` becomes `group_id` and `message_id` becomes topic offset. Encoding unchanged.

**SQS consumer_id:** Currently `rand_consumer_id()` per ReceiveMessage call. This works identically with `group_deliver` — the Ack variant tracks `consumer_in_flight` per consumer_id.

### 7c. MQTT Adapter (mq-mqtt)

**Current state:** Uses queues (per-client subscription, shared subscription), topics (retained messages), exchanges (pub/sub routing), consumer/producer sessions.

**Subscription queues become topics + Ack groups:**

Each MQTT subscription queue becomes a topic + Ack-variant consumer group pair. The MQTT adapter's `ensure_queue()` function becomes `ensure_subscription()` which:
1. Creates a topic for the subscription (or reuses existing for shared subscriptions)
2. Creates an Ack-variant consumer group on that topic

| MQTT Concept | Current MQ Entity | Unified Entity | Notes |
|-------------|-------------------|----------------|-------|
| Per-client subscription | Queue `mqtt/sub/{client_id}/{filter}` | Topic `mqtt/sub/{client_id}/{filter}` + Ack group (same name) | Topic has `DeleteOnLastDetach` for clean sessions |
| Shared subscription | Queue `mqtt/shared/{group}/{filter}` | Topic `mqtt/shared/{group}/{filter}` + Ack group (same name) | Multiple consumers on one Ack group |
| Retained message store | Separate topic `$mqtt/retained/{topic}` per retained msg (max_messages=1) | Native `retained=true` on the MQTT topic that exchange bindings target | No more `$mqtt/retained/*` topic proliferation |
| Exchange routing | Exchange `mqtt/exchange` | Exchange `mqtt/exchange` | Unchanged |
| Exchange bindings | Binding → `queue_id` | Binding → `target_topic_id` | Bindings now target the subscription topic |

| MQTT Operation | Current MQ Operation | Unified Operation | Notes |
|---------------|---------------------|-------------------|-------|
| SUBSCRIBE | `create_queue(name, config)` + `create_binding(exch, queue, key)` | `create_consumer_group(name, Ack, auto_create_topic=true)` + `create_binding(exch, topic_id, key)` | |
| UNSUBSCRIBE | `delete_binding(id)` + `delete_queue(id)` (clean session) | `delete_binding(id)` + `delete_consumer_group(id)` | Topic auto-deleted via `DeleteOnLastDetach` |
| PUBLISH (inbound) | `publish_to_exchange(exch, msg)` | `publish_to_exchange(exch, msg)` | Exchange routes to target topics now |
| Delivery (outbound) | `deliver(queue_id, session_id, batch)` | `group_deliver(group_id, session_id, batch)` | |
| PUBACK (QoS 1) | `ack(queue_id, msg_ids, None)` | `group_ack(group_id, msg_ids)` | |
| NACK (disconnect) | `nack(queue_id, msg_ids)` | `group_nack(group_id, msg_ids)` | Returns in-flight to pending |
| QoS 2 flow | `ack()` at PUBCOMP with Received/Released states | `group_ack()` with same MessageState transitions | Received/Released states preserved in AckMessageMeta |
| PUBLISH retain=1 | `create_topic($mqtt/retained/{topic})` + `publish(retained_topic_id, msg)` | `publish(topic_id, msg)` where topic has `retained=true` | Topic's `retained_message` auto-updated on publish |
| PUBLISH retain=1 empty | `purge_topic(retained_topic_id, u64::MAX)` | `publish(topic_id, empty_msg)` | Empty payload clears `retained_message` natively |
| SUBSCRIBE (retain delivery) | Read from `$mqtt/retained/{topic}` topic | Read `topic.retained_message` from target topics matched by exchange | No separate topic lookup needed |

**Retained messages:** MQTT topics that can receive retained publishes are created with `retained=true`. On PUBLISH with retain=1, the exchange routes the message to matching subscription topics as usual, and each target topic with `retained=true` updates its `retained_message`. On SUBSCRIBE, the adapter checks each matched topic for a `retained_message` and delivers it immediately. Empty-payload publish clears the retained message natively. This eliminates the current `$mqtt/retained/*` topic proliferation — potentially thousands of tiny topics replaced by a boolean flag on existing topics.

**Retain Handling per subscription:** MQTT 5.0 retain handling options (0=always send, 1=only new subscriptions, 2=never send) are handled at the adapter layer when deciding whether to read `topic.retained_message` on subscribe — no engine change needed.

**reply_to + correlation_id:** MQTT 5.0 Request/Response uses `FlatMessage.reply_to()` and `FlatMessage.correlation_id()`. These are extracted at publish time and stored in `AckMessageMeta.reply_to` and `AckMessageMeta.correlation_id`. On ACK, the engine checks `reply_to` and publishes the response to that topic — identical to current behavior.

**No Local filtering (M6):** The `publisher_session_id` header in FlatMessage is checked at delivery time against the consuming session. This is independent of the storage model — works identically with topic-backed delivery.

**Binding `no_local`, `shared_group`, `subscription_id`:** These fields on `Binding` stay as-is, except `queue_id` → `target_topic_id`. The exchange routing logic publishes to the target topic; the Ack group on that topic handles delivery semantics.

**Clean session cleanup:** On clean disconnect, MQTT deletes all subscription queues. In the unified model, delete the Ack consumer groups. Topics with `DeleteOnLastDetach` are automatically cleaned up.

**Session management (native):** MQTT session features map directly to native session primitives:

| MQTT Feature | Current Approach | Native Approach |
|-------------|-----------------|-----------------|
| CONNECT | `register_consumer()` + `register_producer()` | `register_session(session_id, client_id, keep_alive, session_expiry, will)` |
| Clean Start | Adapter-level `clean_session` flag, manual cleanup | `session_expiry_ms = 0` → engine expires session immediately on disconnect |
| Session Expiry | `InMemorySessionStore` + `PersistedSession` in adapter | `session_expiry_ms > 0` → engine preserves session state, subscriptions, consumer groups |
| Will Message | `WillMessage` in adapter, published at adapter layer | `WillConfig` in engine, published by engine on unexpected disconnect |
| Will Delay | `PendingWills: DashMap<String, JoinHandle>` in adapter | `pending_wills` in engine, raft-replicated (survives leader failover) |
| Keep-Alive | PINGREQ/PINGRESP at adapter layer | `keep_alive_ms` on session, engine enforces 1.5× timeout |
| Client Takeover | `ActiveSessions: DashMap<String, oneshot::Sender>` | Engine `session_client_index` + `takeover_notify` |
| Session Resume | `SessionStore.load()` + re-subscribe | Engine finds existing session by `client_id`, subscriptions already exist |
| DISCONNECT clean | Adapter clears will, calls `disconnect_consumer` | `disconnect_session(publish_will=false)` |
| DISCONNECT 0x04 | Adapter publishes will, calls `disconnect_consumer` | `disconnect_session(publish_will=true)` |

**Eliminated adapter-layer state:** `InMemorySessionStore`, `PersistedSession`, `PendingWills` JoinHandle map, `ActiveSessions` DashMap, will message publishing logic. All replaced by native engine primitives with raft-replicated durability.

**QoS packet ID tracking stays adapter-side:** MQTT packet IDs (u16), QoS 2 inbound/outbound state machines, topic alias mapping — these are MQTT-protocol-specific concerns that don't belong in the engine. The adapter maintains these per-connection. On session resume, the adapter can reconstruct QoS state from the consumer group's in-flight messages.

### 7d. AMQP Adapter (mq-amqp)

**Current state:** Uses a `MessageBroker` trait abstraction. The adapter never calls `MqCommand` directly — only the trait implementation does.

**MessageBroker trait unchanged, implementation updated:**

```rust
pub trait MessageBroker: Send + Sync + 'static {
    fn resolve_address(&self, address: &str) -> Result<(&'static str, u64), BrokerError>;
    fn publish_to_topic(&self, topic_id: u64, message: &AmqpMessage) -> Result<PublishResult, BrokerError>;
    fn enqueue(&self, queue_id: u64, message: &AmqpMessage) -> Result<PublishResult, BrokerError>;
    fn fetch_messages(&self, queue_id: u64, consumer_id: u64, max_count: u32) -> Result<Vec<BrokerMessage>, BrokerError>;
    fn fetch_topic_messages(&self, topic_id: u64, offset: u64, max_count: u32) -> Result<Vec<BrokerMessage>, BrokerError>;
    fn settle(&self, queue_id: u64, message_ids: &SmallVec<[u64; 16]>, action: SettleAction) -> Result<(), BrokerError>;
    fn register_consumer(&self, consumer_id: u64, entity_type: &str, entity_id: u64) -> Result<(), BrokerError>;
    fn disconnect_consumer(&self, consumer_id: u64) -> Result<(), BrokerError>;
    fn create_dynamic_node(&self, lifetime_policy: Option<LifetimePolicy>) -> Result<(&str, u64, String), BrokerError>;
    fn destroy_dynamic_node(&self, entity_id: u64) -> Result<(), BrokerError>;
}
```

The trait stays the same. The implementation changes under the hood:

| Trait Method | Current Implementation | Unified Implementation |
|-------------|----------------------|----------------------|
| `resolve_address("queue/foo")` | Lookup in `queues_by_name` | Lookup in `consumer_group_names` (Ack variant), return `source_topic_id` for publish or `group_id` for consume |
| `enqueue(queue_id, msg)` | `MqCommand::enqueue(queue_id, msg)` | `MqCommand::publish(source_topic_id, msg)` — the Ack group's topic |
| `fetch_messages(queue_id, ...)` | `MqCommand::deliver(queue_id, ...)` | `MqCommand::group_deliver(group_id, ...)` |
| `settle(queue_id, ids, Accept)` | `MqCommand::ack(queue_id, ids, None)` | `MqCommand::group_ack(group_id, ids)` |
| `settle(queue_id, ids, Reject)` | `MqCommand::nack(queue_id, ids)` | `MqCommand::group_nack(group_id, ids)` |
| `settle(queue_id, ids, Release)` | `MqCommand::nack(queue_id, ids)` | `MqCommand::group_release(group_id, ids)` — **new: no attempt increment** |
| `settle(queue_id, ids, Modify)` | `MqCommand::nack(queue_id, ids)` | `MqCommand::group_modify(group_id, ids)` — **new: attempt increment + annotations** |
| `create_dynamic_node(policy)` | Creates temp queue | Creates topic with `lifetime_policy` + Ack group |
| `destroy_dynamic_node(id)` | Deletes temp queue | Deletes Ack group; topic auto-deletes via `DeleteOnLastDetach` |

**Key AMQP semantic gaps now addressed:**

1. **Release vs Nack:** Currently both map to `MqCommand::nack()`. In the unified model, `GROUP_RELEASE` (tag 99) returns message to pending **without** incrementing `attempts`. `GROUP_NACK` (tag 78) **does** increment `attempts` and may route to DLQ. This correctly models AMQP's Released vs Rejected outcomes.

2. **Modify:** `GROUP_MODIFY` (tag 100) returns message to pending, increments `attempts`, and can optionally update `deliver_after` (for `delivery-failed` + `undeliverable-here` annotations). This is currently impossible — nack has no way to specify "requeue with modifications."

3. **Dynamic nodes:** `create_dynamic_node()` creates a topic with `DeleteOnLastDetach` policy + an Ack-variant consumer group. When the AMQP link detaches and the group is deleted, the topic auto-cleans. This replaces the current temp queue pattern.

4. **Transactions:** AMQP transactions buffer Publish/Settle actions and execute atomically on Discharge. The `MqCommand::batch()` already supports atomic multi-command execution. Transaction commit: batch all buffered publishes + settles into one `MqCommand::batch()`. Rollback: discard the buffer. No change needed at the engine level.

5. **Two-phase settlement (rcv-settle-mode=Second):** The AMQP adapter handles this at the protocol layer (Disposition settled=false → settled=true). The broker only sees the final settled=true call as a single `settle()` invocation. No engine change needed.

6. **Exchange retained messages:** The current `ExchangeState.retained: HashMap<String, Bytes>` is replaced by native topic-level retained messages. When AMQP publishes to an exchange, the exchange routes to target topics — if a target topic has `retained=true`, its `retained_message` is updated automatically. On link attach to a topic with a retained message, the broker delivers it immediately. This eliminates the duplicate retained storage on exchanges.

### 7e. Native Bisque Protocol (mq-server)

**Current state:** Dispatches on `entity_type` constants (TOPIC=0, QUEUE=1, ACTOR_NAMESPACE=2, JOB=3) for subscribe, deliver, ack, nack.

**Entity type dispatch becomes variant-aware:**

The server's `SubscriptionState` currently stores `entity_type: u8`. In the unified model:

```rust
// Before:
pub const ENTITY_TYPE_TOPIC: u8 = 0;
pub const ENTITY_TYPE_QUEUE: u8 = 1;
pub const ENTITY_TYPE_ACTOR_NAMESPACE: u8 = 2;
pub const ENTITY_TYPE_JOB: u8 = 3;

// After:
pub const ENTITY_TYPE_TOPIC: u8 = 0;          // direct topic consumption (Offset)
pub const ENTITY_TYPE_CONSUMER_GROUP: u8 = 1;  // any variant (Ack, Actor, or Offset)
```

The subscription's `entity_type` tells the server *how to deliver*, and the consumer group's `variant` tells the engine *what semantics to apply*.

| Server Operation | Current (Queue) | Current (Actor) | Unified |
|-----------------|----------------|----------------|---------|
| Subscribe | Ready when `has_capacity()` | Ready when `has_capacity()` | Ready when `has_capacity()` (unchanged) |
| Subscribe (Topic) | Ready when `offset < head` | — | Ready when `offset < head` (unchanged) |
| Deliver | `MqCommand::deliver(queue_id, cid, max)` | `MqCommand::deliver_actor_message(ns_id, [], cid)` | `MqCommand::group_deliver(group_id, cid, max)` — engine dispatches on variant |
| ACK | `MqCommand::ack(queue_id, msg_ids, None)` | `MqCommand::ack_actor_message(ns_id, [], mid, None)` | `MqCommand::group_ack(group_id, msg_ids)` — engine dispatches on variant |
| NACK | `MqCommand::nack(queue_id, msg_ids)` | `MqCommand::nack_actor_message(ns_id, [], mid)` | `MqCommand::group_nack(group_id, msg_ids)` — engine dispatches on variant |
| CommitOffset | `MqCommand::commit_offset(topic_id, cid, offset)` | — | Unchanged (topic-level, not group-level) |

**Unified dispatch in handler:** The server no longer needs to branch on entity_type for ACK/NACK/deliver. A single code path calls `group_ack`/`group_nack`/`group_deliver` and the engine handles variant-specific behavior internally.

**Retained message on subscribe:** When a native client subscribes to a topic with `retained=true`, the server delivers the `retained_message` immediately as the first message. This enables "current state" patterns — config distribution, last-known-good values, presence — natively in the bisque protocol without requiring clients to poll or maintain their own cache.

**Consumer group operations:** All existing consumer group commands (create, delete, join, sync, leave, heartbeat, offset commit/fetch, list, describe) remain unchanged. They operate on Offset-variant groups by default and work identically for the native protocol.

---

## 9. Exchange Integration

Exchanges currently route messages to queues via `Binding.queue_id`. In the unified model:

### Binding Changes

```rust
pub struct Binding {
    pub binding_id: u64,
    pub exchange_id: u64,
    pub target_topic_id: u64,   // was: queue_id
    pub routing_key: Option<String>,
    pub no_local: bool,
    pub shared_group: Option<String>,
    pub subscription_id: Option<u32>,
}
```

### Routing Flow

**Before:**
```
PUBLISH → Exchange → Binding → Queue (enqueue) → Consumer (deliver)
```

**After:**
```
PUBLISH → Exchange → Binding → Topic (publish) → Ack Group (deliver) → Consumer
```

`publish_to_exchange` iterates matching bindings and publishes to each binding's `target_topic_id`. The Ack-variant consumer group(s) on each target topic handle delivery semantics. If a target topic has `retained=true`, its `retained_message` is updated as a side-effect of the publish.

### ExchangeState Changes

```rust
pub struct ExchangeState {
    pub meta: ExchangeMeta,
    pub bindings: HashMap<u64, Binding>,
    pub direct_index: HashMap<u64, SmallVec<[u64; 4]>>,
    // REMOVED: pub retained: HashMap<String, Bytes>
    // Retained messages now live on the target topics, not the exchange.
}
```

The `retained` map on `ExchangeState` is removed. Retained message storage is now a topic-level concern via `topic.retained_message`. This eliminates duplicate storage and ensures retained messages are always consistent with the topic log.

### MQTT Exchange Flow

```
MQTT PUBLISH → publish_to_exchange(mqtt_exchange, msg)
  → Exchange matches routing key against binding patterns
  → For each match: publish(binding.target_topic_id, msg)
  → Each subscription topic has an Ack group
  → group_deliver() from Ack group to MQTT client
  → PUBACK → group_ack()
```

This preserves the current fan-out semantics: one MQTT PUBLISH can reach multiple subscription topics (one per matching subscriber), each with its own Ack group for independent delivery tracking.

---

## 10. Native Session Management

bisque-mq promotes four session-level capabilities — currently implemented at the MQTT adapter layer — to native engine-level primitives. This makes them available to all protocols.

### 9a. Session Model

Sessions are the connection-level identity. Currently, `ConsumerState` and `ProducerMeta` are separate entities. In the unified model, they merge into a single `Session`:

```rust
pub struct SessionMeta {
    pub session_id: u64,
    pub client_id: String,           // unique client identifier (enables takeover)
    pub client_id_hash: u64,         // name_hash for O(1) lookup
    pub created_at: u64,
    pub connected_at: u64,           // last connection time
    pub last_activity_at: u64,
    pub keep_alive_ms: u64,          // 0 = disabled
    pub session_expiry_ms: u64,      // 0 = expire on disconnect, u64::MAX = never expire
    pub will: Option<WillConfig>,    // published on unexpected disconnect
    pub subscriptions: Vec<SessionSubscription>,  // for session resume
    pub producer_name: Option<String>,
}

pub struct SessionState {
    pub meta: SessionMeta,
    // Atomic mutable scalars
    last_activity_at: AtomicU64,
    connected: AtomicBool,           // true while actively connected
    // Complex state
    will: RwLock<Option<WillConfig>>,
    in_flight: DashMap<u64, SmallVec<[u64; 8]>>,  // entity_id → message_ids
    // Pre-initialized metrics handles
    m_heartbeat_count: metrics::Counter,
    m_will_publish_count: metrics::Counter,
}
```

This replaces both `ConsumerState` and `ProducerMeta` with a single entity. The `session_id` serves as both consumer_id and producer_id.

**MqMetadata changes:**
```rust
// Before:
pub(crate) consumers: DashMap<u64, ConsumerState>,
pub(crate) producers: DashMap<u64, ProducerMeta>,

// After:
pub(crate) sessions: DashMap<u64, SessionState>,
pub(crate) session_client_index: DashMap<u64, u64>,  // client_id_hash → session_id
```

### 9b. Persistent Sessions

A session with `session_expiry_ms > 0` survives disconnection. When the client disconnects:

1. The session transitions to **disconnected** state (`connected = false`)
2. Its subscriptions, consumer group memberships, and in-flight tracking are preserved
3. Messages continue to accumulate in the session's consumer groups (Ack variant pending queues fill up; Offset variant offsets stay committed)
4. A background timer checks for sessions that have exceeded `session_expiry_ms` since disconnect and removes them

When a client reconnects with the same `client_id`:

1. The existing session is found via `session_client_index`
2. Subscriptions are restored — consumer groups already exist, no re-creation needed
3. Pending messages are immediately available for delivery
4. The session transitions back to **connected** state

**Session expiry values:**
- `0` — session expires immediately on disconnect (clean session / MQTT clean_start=true)
- `1..u64::MAX-1` — session expires N milliseconds after disconnect
- `u64::MAX` — session never expires (persistent indefinitely)

```rust
// Command
MqCommand::register_session(
    session_id: u64,
    client_id: &str,
    keep_alive_ms: u64,
    session_expiry_ms: u64,
    will: Option<&WillConfig>,
) -> MqCommand;

// Replaces both register_consumer and register_producer
```

**Session state in snapshots:**
```rust
pub struct SessionSnapshot {
    pub meta: SessionMeta,
    pub connected: bool,
    pub disconnected_at: Option<u64>,  // for expiry calculation
    pub in_flight: Vec<(u64, Vec<u64>)>,  // entity_id → message_ids
}
```

**Protocol mapping:**

| Protocol | Current Approach | Native Approach |
|----------|-----------------|-----------------|
| **MQTT** | `PersistedSession` in adapter with QoS packet IDs, subscription filters | `SessionState` in engine. QoS packet IDs stay adapter-side (protocol-specific). Subscriptions reference consumer group IDs. |
| **AMQP** | Durable terminus tracking on links | `SessionState` with `session_expiry_ms > 0`. Link resume finds existing session + consumer groups. |
| **Kafka** | Consumer group session (session_timeout_ms in JoinGroup) | Consumer group sessions unchanged. Client-level session adds keep-alive + will support. |
| **SQS** | No sessions (stateless HTTP) | Optional — SQS adapter can create ephemeral sessions (`session_expiry_ms = 0`). |
| **Native** | `ConsumerSession` in mq-server with session_token | `SessionState` in engine. session_token stays adapter-side. Persistent sessions enable client reconnect to resume subscriptions. |

### 9c. Last Will (Will Message)

A will message is published when a session disconnects unexpectedly. This already exists partially in core MQ (`ConsumerMeta.will`), but `WillMessage` is defined in mq-mqtt. The native version is protocol-agnostic:

```rust
pub struct WillConfig {
    pub topic_id: u64,               // topic to publish to (must exist)
    pub payload: Bytes,              // message bytes (FlatMessage)
    pub delay_ms: u64,               // delay before publishing (0 = immediate)
    pub retained: bool,              // if true, also update topic.retained_message
}
```

**Behavior:**

1. **Set at session registration:** `register_session(... will: Some(&will))`. Can also be updated via `TAG_SET_WILL`.
2. **Published on unexpected disconnect:** When a session disconnects without a clean disconnect command, the engine publishes the will. If `delay_ms > 0`, the engine schedules a delayed publish — cancelled if the client reconnects within the delay window.
3. **Cleared on clean disconnect:** A clean disconnect (`TAG_DISCONNECT_SESSION` with `publish_will=false`) clears the will without publishing. The client can also opt to publish will on clean disconnect (`publish_will=true`) — this maps to MQTT 5.0 reason code 0x04.
4. **Will + retained:** If `will.retained=true` and the target topic has `retained=true`, the will message also updates `topic.retained_message`. Enables "offline" presence — retained message updates to "offline" when client drops.

**Will delay lifecycle:**
```
Client connects → will registered
Client disconnects unexpectedly
  → if delay_ms == 0: publish will immediately
  → if delay_ms > 0: schedule delayed publish
     → Client reconnects within delay: cancel pending will
     → Delay expires: publish will, remove from pending
```

**Pending wills in engine state:**
```rust
// MqMetadata gains:
pub(crate) pending_wills: DashMap<u64, PendingWill>,  // session_id → will + deadline

pub struct PendingWill {
    pub session_id: u64,
    pub will: WillConfig,
    pub publish_at: u64,  // absolute timestamp
}
```

Pending wills are raft-replicated (a `TAG_SCHEDULE_WILL` command is logged on disconnect, `TAG_CANCEL_WILL` on reconnect, `TAG_FIRE_WILL` when the timer fires). This ensures wills survive leader failover.

**Protocol mapping:**

| Protocol | Current Will Support | Native Will |
|----------|---------------------|-------------|
| **MQTT** | Full — will topic, payload, QoS, retain, delay_interval, properties | `WillConfig` covers topic_id, payload (as FlatMessage with headers for MQTT properties), delay_ms, retained. MQTT QoS maps to delivery via Ack group on will topic. |
| **AMQP** | None | Clients can register a will via session properties — enables presence detection for AMQP services. |
| **Kafka** | None | Producers can register a will — enables "producer heartbeat" patterns (publish tombstone on disconnect). |
| **Native** | None | First-class will support — service mesh presence, distributed lock release, circuit breaker triggers. |
| **SQS** | Not applicable | Not applicable (stateless HTTP). |

### 9d. Keep-Alive

Sessions have a `keep_alive_ms` timeout. If the engine receives no activity (commands, heartbeats) from a session within `1.5 × keep_alive_ms`, the session is considered dead and disconnected (triggering will if configured).

```rust
// SessionState method:
pub fn is_dead(&self, current_time: u64) -> bool {
    if self.meta.keep_alive_ms == 0 { return false; }
    let deadline = self.last_activity_at.load(Ordering::Relaxed)
        + (self.meta.keep_alive_ms * 3 / 2);  // 1.5× grace period
    current_time > deadline
}
```

**Engine periodic check:** A timer task (already exists for visibility timeouts, job scheduling, etc.) checks for dead sessions and emits `TAG_DISCONNECT_SESSION` commands for any that have timed out. The 1.5× multiplier matches MQTT 5.0 spec §3.1.2.10.

**Activity tracking:** Any command from a session updates `last_activity_at`. Explicit `TAG_HEARTBEAT` also updates it. The adapter sends heartbeats at `keep_alive_ms / 2` intervals (before the deadline).

**Protocol mapping:**

| Protocol | Current Keep-Alive | Native Keep-Alive |
|----------|-------------------|-------------------|
| **MQTT** | PINGREQ/PINGRESP at adapter layer, disconnect on 1.5× timeout | `keep_alive_ms` set at registration. PINGREQ → `TAG_HEARTBEAT`. Engine handles timeout. |
| **AMQP** | Idle timeout on connection (§2.4.5) | `keep_alive_ms` = AMQP idle-timeout / 2. Adapter sends empty frames → `TAG_HEARTBEAT`. |
| **Kafka** | Consumer group session timeout (JoinGroup session_timeout_ms) | Consumer group timeout unchanged. Client-level keep-alive adds connection liveness. |
| **Native** | Ad-hoc heartbeat command | `keep_alive_ms` in handshake. Server sends `TAG_HEARTBEAT` periodically. |
| **SQS** | Not applicable (HTTP) | Not applicable. |

### 9e. Client Takeover

When a new session registers with a `client_id` that already has an **active** (connected) session, the engine forcefully disconnects the old session:

1. Engine looks up `session_client_index[client_id_hash]` → existing `session_id`
2. If existing session is **connected**: emit `TAG_DISCONNECT_SESSION` for the old session with `reason=SessionTakenOver` and `publish_will=false` (takeover is not an unexpected disconnect — will is NOT published)
3. If existing session is **disconnected** (persistent, waiting for reconnect): resume it — transition back to connected, restore subscriptions
4. New session inherits the existing session_id (preserves consumer group memberships, in-flight state)

```rust
// MqResponse for takeover:
pub enum DisconnectReason {
    Clean,            // client-initiated clean disconnect
    KeepAliveTimeout, // keep-alive expired
    SessionTakenOver, // new connection with same client_id
    SessionExpired,   // session_expiry_ms exceeded
    AdminForced,      // administrative disconnect
}
```

**Takeover notification:** The engine returns `MqResponse::SessionTakenOver { old_session_id }` to the new registrant, and the adapter layer is responsible for closing the old TCP connection. The engine provides a notification mechanism (similar to `phase_notify` on consumer groups) so the adapter holding the old connection can detect the takeover:

```rust
// SessionState gains:
pub takeover_notify: Arc<tokio::sync::Notify>,  // signaled when session is taken over
```

The adapter for the old connection awaits `takeover_notify.notified()` alongside its normal read loop. On signal, it sends a protocol-appropriate disconnect frame and closes.

**Protocol mapping:**

| Protocol | Current Takeover | Native Takeover |
|----------|-----------------|-----------------|
| **MQTT** | `ActiveSessions: DashMap<String, oneshot::Sender>` at adapter layer | Engine handles via `session_client_index`. Adapter receives `SessionTakenOver` and sends DISCONNECT 0x8E. |
| **AMQP** | Link stealing on reattach (same link name) | Session takeover on same `client_id`. Link-level stealing stays adapter-side (protocol detail). |
| **Kafka** | Consumer group rebalance on duplicate member_id | Consumer group handles member identity. Session takeover adds connection-level dedup. |
| **Native** | Not implemented | Full support — reconnecting client with same `client_id` takes over seamlessly. |

### 9f. Command Tags for Sessions

| Tag | Name | Description |
|-----|------|-------------|
| 42 | REGISTER_SESSION | Replaces REGISTER_CONSUMER + REGISTER_PRODUCER. Sets client_id, keep_alive, session_expiry, will. Triggers takeover if client_id collision. |
| 43 | DISCONNECT_SESSION | Replaces DISCONNECT_CONSUMER + DISCONNECT_PRODUCER. Params: session_id, reason, publish_will. |
| 44 | HEARTBEAT | Unchanged — updates session last_activity_at. |
| 101 | SET_WILL | Update will on an active session (without re-registering). |
| 102 | CLEAR_WILL | Remove will from an active session. |
| 103 | SCHEDULE_WILL | Internal: logged on unexpected disconnect with delay_ms > 0. |
| 104 | CANCEL_WILL | Internal: logged on reconnect to cancel pending delayed will. |
| 105 | FIRE_WILL | Internal: logged when delayed will timer fires; publishes the will. |
| 106 | EXPIRE_SESSION | Internal: logged when a disconnected session's expiry timer fires. |

Tags 45 (REGISTER_PRODUCER) and 46 (DISCONNECT_PRODUCER) become compatibility shims that translate to REGISTER_SESSION / DISCONNECT_SESSION.

---

## 11. Compound Create: `auto_create_topic`

Several adapters need to atomically create a topic + consumer group pair. Rather than requiring two separate commands, `CREATE_CONSUMER_GROUP` gains an `auto_create_topic` flag:

```rust
// MqCommand::create_consumer_group extended:
pub fn create_consumer_group(
    name: &str,
    variant: GroupVariant,
    variant_config: VariantConfig,
    auto_create_topic: bool,       // if true, engine creates source topic with same name
    topic_retention: RetentionPolicy,
    topic_dedup: Option<TopicDedupConfig>,
    topic_lifetime: TopicLifetimePolicy,
) -> MqCommand;
```

**Engine behavior when `auto_create_topic=true`:**
1. Create topic `{name}` with given retention, dedup, and lifetime policies
2. Set `source_topic_id` on the consumer group to the new topic's ID
3. Register the group in the topic's `consumer_group_ids`
4. Both created in a single raft log entry (atomic)

**When `auto_create_topic=false`:**
- `source_topic_id` must be provided explicitly
- Used when multiple consumer groups share one topic

This covers:
- **SQS:** `create_consumer_group("my-queue", Ack, config, auto_create_topic=true, default_retention, dedup_if_fifo, Permanent)`
- **MQTT subscriptions:** `create_consumer_group("mqtt/sub/...", Ack, config, auto_create_topic=true, default_retention, None, DeleteOnLastDetach)`
- **AMQP dynamic nodes:** `create_consumer_group("temp-queue://X", Ack, config, auto_create_topic=true, default_retention, None, DeleteOnLastDetach)`
- **Kafka consumer groups:** `create_consumer_group("group-1", Offset, {}, auto_create_topic=false, ...)` with explicit `source_topic_id`

---

## 12. Durability & Recovery

All consumer group variant state is durable through the same mechanism used today for queues, actors, and jobs:

### Raft Log (Primary)

Every variant operation (GROUP_DELIVER, GROUP_ACK, GROUP_NACK, GROUP_RELEASE, GROUP_MODIFY, GROUP_ASSIGN_ACTORS, GROUP_TRIGGER_JOB, etc.) is a raft command. The raft log is the source of truth. On normal startup, the engine replays the entire available log from index 1, calling `engine.apply_command()` for each entry, which rebuilds all in-memory state (DashMaps, BTreeMaps, atomics) from scratch.

This is unchanged from today — queue commands (ENQUEUE, ACK, NACK, TIMEOUT_EXPIRED) are already raft entries that rebuild `QueueState` on replay. In the unified model, those same operations rebuild `AckVariantState` instead.

### MDBX Structural Writes (Optimization)

When a consumer group is created (any variant), a fire-and-forget async write persists entity metadata to MDBX's `entities` table. This establishes a `structural_purge_floor`. On next startup:

1. Structural state is loaded from MDBX (group shells with variant/config)
2. Only raft entries **after** the structural floor are replayed
3. This avoids replaying the full log (e.g., 1M entries → only last 100K)

Variant-specific state (ack message metadata, actor mailboxes, job execution tracking) is **not** stored in MDBX — it's rebuilt from raft log replay. This is the same as today where `QueueMessageMeta` and `ActorInMemory` are rebuilt from log replay.

### Snapshots (New/Lagging Followers)

When a follower is too far behind, the leader serializes all state into `MqSnapshotData` (including variant-specific state in `ConsumerGroupSnapshot`) and sends it. The follower:

1. Calls `engine.restore(snap)` to populate all in-memory state
2. Persists entity metadata to MDBX
3. Only replays raft entries after the snapshot point

Each variant's snapshot includes its full state:
- **Ack:** All `AckMessageMeta` entries, pending/in-flight indexes
- **Actor:** All `ActorInMemory` instances with mailbox contents
- **Cron:** Topic cron state (next_trigger_at, enabled) included in topic snapshots

### Durability Guarantees (unchanged)

| State | Mechanism | Recovery Path |
|-------|-----------|---------------|
| Group metadata (variant, config) | Raft log + MDBX structural | MDBX load + partial log replay |
| Offset commits | Raft log + snapshots | Log replay rebuilds offsets DashMap |
| Ack message state (pending/in-flight) | Raft log + snapshots | Log replay rebuilds AckVariantState |
| Actor mailboxes & assignments | Raft log + snapshots | Log replay rebuilds ActorVariantState |
| Cron scheduling state | Raft log + snapshots | Log replay rebuilds topic cron state |
| Members & rebalance state | Raft log + snapshots | Log replay rebuilds member DashMap |
| Session state (will, keep-alive, expiry) | Raft log + snapshots | Log replay rebuilds SessionState |
| Pending wills | Raft log + snapshots | Log replay rebuilds pending_wills DashMap |

---

## 13. Implementation Phases

No backwards compatibility is required — bisque-mq, bisque-mq-protocol, and bisque-mq-server have not shipped. Old entity types (`QueueState`, `ActorNamespaceState`, `JobInstance`, `ConsumerState`, `ProducerMeta`) are deleted outright. Old command tags are removed. Old snapshot fields are removed. Jobs are eliminated as a variant — replaced by topic-level cron + Ack group.

### Phase 1: Core types and unified naming
Replace the type system wholesale. Add `GroupVariant` (Offset/Ack/Actor), `VariantConfig`, `VariantState`, `TopicLifetimePolicy`, `TopicDedupConfig`, `TopicCronConfig`, `WillConfig`, `SessionMeta`, `SessionState`, `NameEntry`, `AckMessageMeta`. New `MqMetadata` with `topics`, `consumer_groups`, `sessions`, `exchanges`, `names`. Delete `QueueState`, `ActorNamespaceState`, `JobInstance`, `ConsumerState`, `ProducerMeta`, `QueueMeta`, `QueueMessageMeta` and all per-type name maps.

**Files:** `types.rs`, `metadata.rs`, `consumer_group.rs`, `topic.rs`, `session.rs` (new). Delete: `queue.rs`, `actor.rs`, `job.rs`, `consumer.rs`, `producer.rs`

### Phase 2: Codec and command tags
New tag allocation (0-53). New `MqCommand` constructors and view structs for all unified commands. Remove all old constructors and view structs.

**Files:** `codec.rs`, `types.rs`

### Phase 3: Engine
Rewrite `engine.rs` apply handlers for unified commands. Topic apply (create, delete, publish with dedup + retained, purge). Consumer group apply (create with auto_create_topic, delete with auto-delete topic, variant-dispatched deliver/ack/nack/release/modify). Session apply (register, disconnect, heartbeat, will lifecycle, keep-alive, takeover, expiry). Exchange apply (create, delete, bindings → target_topic_id, publish_to_exchange). DLQ auto-creation and routing.

**Files:** `engine.rs`

### Phase 4: Snapshot/restore
New `MqSnapshotData` with topics, consumer_groups, sessions, exchanges, pending_wills. Restore rebuilds `MqMetadata` including `names` index.

**Files:** `engine.rs` (snapshot/restore methods), `types.rs`

### Phase 5: Adapters
Update all adapters to use unified operations:
- **mq-kafka:** Minimal changes — already on topics + Offset consumer groups. Update command constructors to new tag numbers.
- **mq-sqs:** Replace queue operations with topic + Ack group operations. `create_queue` → `create_consumer_group(Ack, auto_create_topic=true)`. `enqueue` → `publish`. `deliver` → `group_deliver`. DLQ via `"{name}/dlq"` convention.
- **mq-mqtt:** Replace subscription queues with topic + Ack group. Bindings target `topic_id`. Retained messages via native `retained=true`. Sessions via `register_session`. Remove `InMemorySessionStore`, `ActiveSessions`, `PendingWills` JoinHandle map, `$mqtt/retained/*` topic creation.
- **mq-amqp:** Update `MessageBroker` impl. `settle(Release)` → `group_release`. `settle(Modify)` → `group_modify`. Dynamic nodes via `auto_create_topic` + `DeleteOnLastDetach`. Exchange retained reads from target topics. Sessions via `register_session`.
- **mq-server:** Two entity types: `ENTITY_TYPE_TOPIC=0`, `ENTITY_TYPE_CONSUMER_GROUP=1`. Unified `group_deliver`/`group_ack`/`group_nack`. `register_session` replaces `register_consumer` + `register_producer`. Retained delivery on topic subscribe.

**Files:** `crates/mq-kafka/`, `crates/mq-sqs/`, `crates/mq-mqtt/`, `crates/mq-amqp/`, `crates/mq-server/`, `crates/mq-protocol/`

---

## 14. Tradeoffs

| Tradeoff | Assessment |
|----------|------------|
| **Queue messages go through topic first** | Same raft log path today; no extra latency in practice |
| **Actor routing needs dedicated topics** | Auto-create topic per actor namespace; no mixed traffic concern |
| **Jobs eliminated as a variant** | Cron on topic + Ack group is simpler and more composable (any group type can consume cron triggers) |
| **Priority/delay are consumer group state** | Semantically cleaner — same message can have different priority in different groups |
| **Single namespace for all names** | Simplifies resolution; prevents confusing collisions between topic "orders" and queue "orders" |
| **Snapshot size** | Roughly neutral — variant state replaces separate entity snapshots |
| **Release vs Nack now distinct** | Correctly models AMQP semantics; current nack-for-release is lossy |
| **Compound create (auto_create_topic)** | One raft entry instead of two; simpler adapter code |
| **Unified sessions replace consumer+producer** | One entity instead of two; enables will/keep-alive/takeover for all protocols |
| **Will messages are raft-replicated** | Small overhead (SCHEDULE/CANCEL/FIRE entries) but survives leader failover — critical for correctness |
| **Session persistence in engine** | Adapter-specific session stores (InMemorySessionStore) eliminated; raft provides durability for free |

---

## 15. Key Invariants

1. **Topics are storage; consumer groups are consumption.** No consumer group stores message bytes.
2. **One source topic per consumer group.** Multiple groups can read from the same topic.
3. **Variant is immutable after creation.**
4. **Purge floor is per-topic** = `min(retention_floor, min(all attached groups' required offset))`.
5. **Dedup is per-topic**, applied at publish time.
6. **Consumer group IDs subsume queue_ids, namespace_ids, job_ids.**
7. **Settlement has four distinct actions:** Ack (accept), Nack (reject/DLQ), Release (requeue, no retry increment), Modify (requeue with changes).
8. **`auto_create_topic` provides atomic topic+group creation** — adapters never see a group without its source topic.
9. **`DeleteOnLastDetach` topics are auto-cleaned** when their last consumer group is deleted.
10. **Retained messages are per-topic**, stored as the last published message. Orthogonal to retention policy and purge. Survives purge. Cleared by empty-payload publish.
11. **Exchanges no longer store retained messages.** Retained state lives on target topics. `ExchangeState.retained` map is removed.
12. **All names share a single namespace.** Topic, consumer group, and exchange names must be globally unique. `name_hash(name)` → `NameEntry` resolves to topic_id and optional consumer_group_id.
13. **DLQ topics are plain topics with no default consumer group.** Auto-named `"{group}/dlq"` if not explicitly specified. Operator chooses consumption pattern.
14. **Sessions are identified by `client_id`.** One active connection per `client_id`. New connection with same `client_id` triggers takeover.
15. **Will messages are only published on unexpected disconnect.** Clean disconnect clears will (unless explicitly opted in). Takeover does NOT publish will.
16. **Persistent sessions survive disconnect.** Session state (subscriptions, group memberships) preserved for `session_expiry_ms` duration. Consumer groups continue accumulating messages.
17. **Keep-alive is enforced by the engine.** 1.5× timeout with raft-replicated disconnect. Not adapter-dependent.
18. **No backwards compatibility.** Old entity types, command tags, and snapshot fields do not exist. Clean implementation only.

---

## 16. Verification

After each phase:
- Snapshot round-trip: save, restore, verify all state matches
- Purge floor: verify topic purge respects all attached consumer group variants
- Name uniqueness: verify create fails with `AlreadyExists` for duplicate names across entity types

**Per-adapter verification:**
- **mq-kafka:** All existing Kafka protocol tests pass unchanged.
- **mq-sqs:** CreateQueue/SendMessage/ReceiveMessage/DeleteMessage/ChangeVisibility/PurgeQueue/GetAttributes round-trips. FIFO dedup. DLQ routing.
- **mq-mqtt:** QoS 0/1/2 publish and subscribe. Retained messages via native `retained=true` topics (publish, clear via empty payload, delivery on subscribe with retain handling 0/1/2). Shared subscriptions. No Local filtering. Clean session cleanup deletes groups + auto-deletes topics. Request/Response (reply_to + correlation_id round-trip through ACK). Persistent sessions via native `session_expiry_ms`. Will message publish on unclean disconnect (with delay). Will cleared on clean disconnect. Client takeover via `client_id` collision. Keep-alive timeout triggers disconnect + will.
- **mq-amqp:** Accept/Reject/Release/Modify settlement semantics. Dynamic node create/destroy with lifetime policies. Transaction commit/rollback with batched pub+settle. Two-phase settlement. Link resume with unsettled map reconciliation. Exchange retained messages read from target topics. Durable terminus via persistent sessions.
- **mq-server:** Subscribe to topics (offset-based tailing). Retained message delivery on topic subscribe. Subscribe to Ack groups (capacity-based delivery). ACK/NACK dispatch. Consumer group lifecycle (join/sync/leave/heartbeat). Offset commit/fetch. Session registration with will, keep-alive, and session expiry. Client takeover on reconnect.

**Session-specific verification:**
- Session persistence: disconnect with `session_expiry_ms > 0`, reconnect with same `client_id`, verify subscriptions restored and pending messages delivered.
- Will message: unexpected disconnect publishes will to target topic. Clean disconnect does NOT publish will. Takeover does NOT publish will.
- Will delay: disconnect with `delay_ms > 0`, verify will not published immediately. Reconnect within delay cancels will. Delay expires → will published.
- Keep-alive: no activity for 1.5× `keep_alive_ms` → session disconnected, will published.
- Client takeover: new session with same `client_id` → old session forcefully disconnected via `takeover_notify`.
- Session expiry: disconnected session expires after `session_expiry_ms` → session removed, consumer groups detached.
- Snapshot round-trip: persistent sessions + pending wills survive snapshot/restore.

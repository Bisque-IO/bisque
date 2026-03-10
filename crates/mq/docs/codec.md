# bisque-mq Command & Codec Design

## Overview

`MqCommand` is a zero-copy bytes wrapper over flat-encoded binary buffers. Rather
than a Rust `enum` with 48 variants (which requires deserialization on every
apply), it stores the pre-encoded command bytes directly as `Bytes`. This design
eliminates serialization/deserialization on the raft apply hot path — the state
machine reads fields directly from the mmap-backed log entry.

```rust
pub struct MqCommand {
    pub(crate) buf: Bytes,
}
```

Key files:
- `types.rs` — struct definition, tag constants, serde impls
- `codec.rs` — constructors, view structs, Encode/Decode, Display
- `flat.rs` — FlatMessage wire format for message payloads

## Wire Format

Every command is encoded as:

```
[tag: u8][fields...]
```

The first byte is the discriminant tag. Fields follow immediately in a
tag-specific layout using little-endian encoding throughout.

### Field Encoding Rules

| Type             | Encoding                                       |
|------------------|------------------------------------------------|
| `u8`             | 1 byte LE                                      |
| `u32`            | 4 bytes LE                                     |
| `u64`            | 8 bytes LE                                     |
| `String`         | `[len: u32 LE][utf8 bytes]`                    |
| `Bytes`          | `[len: u32 LE][raw bytes]`                     |
| `Vec<T>`         | `[count: u32 LE][T₀][T₁]...`                  |
| `Option<T>`      | `[0u8]` if None, `[1u8][T]` if Some            |
| `Vec<Bytes>`     | `[count: u32 LE][len₀: u32][bytes₀]...`        |
| `Vec<u64>`       | `[count: u32 LE][u64₀][u64₁]...`               |
| Compound structs | Fields concatenated in declaration order        |

### Batch Encoding

Batch commands use length-prefixed sub-commands so each can be decoded
independently without scanning:

```
[TAG_BATCH: 1 byte][count: u32][len₁: u32][cmd₁ bytes][len₂: u32][cmd₂ bytes]...
```

## Tag Constants

48 tag constants are defined as `MqCommand::TAG_*` associated constants:

```
Tag  Constant                     Category
───  ───────────────────────────  ─────────
 0   TAG_CREATE_TOPIC             Topics
 1   TAG_DELETE_TOPIC
 2   TAG_PUBLISH
 3   TAG_COMMIT_OFFSET
 4   TAG_PURGE_TOPIC
 5   TAG_CREATE_QUEUE             Queues
 6   TAG_DELETE_QUEUE
 7   TAG_ENQUEUE
 8   TAG_DELIVER
 9   TAG_ACK
10   TAG_NACK
11   TAG_EXTEND_VISIBILITY
12   TAG_TIMEOUT_EXPIRED
13   TAG_PUBLISH_TO_DLQ
14   TAG_PRUNE_DEDUP_WINDOW
15   TAG_EXPIRE_PENDING_MESSAGES
16   TAG_PURGE_QUEUE
17   TAG_GET_QUEUE_ATTRIBUTES
18   TAG_CREATE_EXCHANGE          Exchanges
19   TAG_DELETE_EXCHANGE
20   TAG_CREATE_BINDING
21   TAG_DELETE_BINDING
22   TAG_PUBLISH_TO_EXCHANGE
23   TAG_CREATE_ACTOR_NAMESPACE   Actors
24   TAG_DELETE_ACTOR_NAMESPACE
25   TAG_SEND_TO_ACTOR
26   TAG_DELIVER_ACTOR_MESSAGE
27   TAG_ACK_ACTOR_MESSAGE
28   TAG_NACK_ACTOR_MESSAGE
29   TAG_ASSIGN_ACTORS
30   TAG_RELEASE_ACTORS
31   TAG_EVICT_IDLE_ACTORS
32   TAG_CREATE_JOB               Jobs
33   TAG_DELETE_JOB
34   TAG_UPDATE_JOB
35   TAG_ENABLE_JOB
36   TAG_DISABLE_JOB
37   TAG_TRIGGER_JOB
38   TAG_ASSIGN_JOB
39   TAG_COMPLETE_JOB
40   TAG_FAIL_JOB
41   TAG_TIMEOUT_JOB
42   TAG_REGISTER_CONSUMER        Sessions
43   TAG_DISCONNECT_CONSUMER
44   TAG_HEARTBEAT
45   TAG_REGISTER_PRODUCER
46   TAG_DISCONNECT_PRODUCER
47   TAG_BATCH                    Batch
```

## Per-Command Layouts

### Topics

**CreateTopic (tag 0)**
```
[0: u8][name_len: u32][name: bytes][retention: RetentionPolicy][partition_count: u32]
```
RetentionPolicy = `[max_age_secs: Option<u64>][max_bytes: Option<u64>][max_messages: Option<u64>]`

**DeleteTopic (tag 1)**
```
[1: u8][topic_id: u64]
```

**Publish (tag 2)**
```
[2: u8][topic_id: u64][msg_count: u32][msg₀_len: u32][msg₀_bytes]...
```
Each message is a FlatMessage (see below).

**CommitOffset (tag 3)**
```
[3: u8][topic_id: u64][consumer_id: u64][offset: u64]
```

**PurgeTopic (tag 4)**
```
[4: u8][topic_id: u64][before_index: u64]
```

### Queues

**CreateQueue (tag 5)**
```
[5: u8][name_len: u32][name: bytes][config: QueueConfig]
```
QueueConfig = `[visibility_timeout_ms: u64][max_retries: u32][dead_letter_topic_id: Option<u64>][dedup_window_secs: Option<u64>][delay_default_ms: u64][max_in_flight_per_consumer: u32]`

**DeleteQueue (tag 6)**
```
[6: u8][queue_id: u64]
```

**Enqueue (tag 7)**
```
[7: u8][queue_id: u64][msg_count: u32][msg₀_len: u32][msg₀_bytes]...[dedup_count: u32][dedup₀: Option<Bytes>]...
```

**Deliver (tag 8)**
```
[8: u8][queue_id: u64][consumer_id: u64][max_count: u32]
```

**Ack (tag 9)**
```
[9: u8][queue_id: u64][id_count: u32][msg_id₀: u64]...[response: Option<Bytes>]
```

**Nack (tag 10)**
```
[10: u8][queue_id: u64][id_count: u32][msg_id₀: u64]...
```

**ExtendVisibility (tag 11)**
```
[11: u8][queue_id: u64][id_count: u32][msg_id₀: u64]...[extension_ms: u64]
```

**TimeoutExpired (tag 12)**
```
[12: u8][queue_id: u64][id_count: u32][msg_id₀: u64]...
```

**PublishToDlq (tag 13)**
```
[13: u8][source_queue_id: u64][dlq_topic_id: u64][dl_count: u32][dl_id₀: u64]...[msg_count: u32][msg₀_len: u32][msg₀]...
```

**PruneDedupWindow (tag 14)**
```
[14: u8][queue_id: u64][before_timestamp: u64]
```

**ExpirePendingMessages (tag 15)**
```
[15: u8][queue_id: u64][id_count: u32][msg_id₀: u64]...
```

**PurgeQueue (tag 16)**
```
[16: u8][queue_id: u64]
```

**GetQueueAttributes (tag 17)**
```
[17: u8][queue_id: u64]
```

### Exchanges

**CreateExchange (tag 18)**
```
[18: u8][name_len: u32][name: bytes][exchange_type: u8]
```
ExchangeType: 0=Fanout, 1=Direct, 2=Topic

**DeleteExchange (tag 19)**
```
[19: u8][exchange_id: u64]
```

**CreateBinding (tag 20)**
```
[20: u8][exchange_id: u64][queue_id: u64][routing_key: Option<String>]
```

**DeleteBinding (tag 21)**
```
[21: u8][binding_id: u64]
```

**PublishToExchange (tag 22)**
```
[22: u8][exchange_id: u64][msg_count: u32][msg₀_len: u32][msg₀_bytes]...
```

### Actors

**CreateActorNamespace (tag 23)**
```
[23: u8][name_len: u32][name: bytes][config: ActorConfig]
```
ActorConfig = `[max_mailbox_depth: u32][idle_eviction_secs: u64][ack_timeout_ms: u64][max_retries: u32]`

**DeleteActorNamespace (tag 24)**
```
[24: u8][namespace_id: u64]
```

**SendToActor (tag 25)**
```
[25: u8][namespace_id: u64][actor_id_len: u32][actor_id: bytes][msg_len: u32][msg: bytes]
```

**DeliverActorMessage (tag 26)**
```
[26: u8][namespace_id: u64][actor_id_len: u32][actor_id: bytes][consumer_id: u64]
```

**AckActorMessage (tag 27)**
```
[27: u8][namespace_id: u64][actor_id_len: u32][actor_id: bytes][message_id: u64][response: Option<Bytes>]
```

**NackActorMessage (tag 28)**
```
[28: u8][namespace_id: u64][actor_id_len: u32][actor_id: bytes][message_id: u64]
```

**AssignActors (tag 29)**
```
[29: u8][namespace_id: u64][consumer_id: u64][id_count: u32][id₀_len: u32][id₀_bytes]...
```

**ReleaseActors (tag 30)**
```
[30: u8][namespace_id: u64][consumer_id: u64]
```

**EvictIdleActors (tag 31)**
```
[31: u8][namespace_id: u64][before_timestamp: u64]
```

### Jobs

**CreateJob (tag 32)**
```
[32: u8][name_len: u32][name: bytes][config: JobConfig]
```
JobConfig = `[cron_expression: String][timezone: String][execution_timeout_ms: u64][overlap_policy: u8][max_queued: u32][input_source: Option<InputSource>][retry_config: RetryConfig]`

**DeleteJob (tag 33)** — `[33: u8][job_id: u64]`

**UpdateJob (tag 34)** — `[34: u8][job_id: u64][config: JobConfig]`

**EnableJob (tag 35)** — `[35: u8][job_id: u64]`

**DisableJob (tag 36)** — `[36: u8][job_id: u64]`

**TriggerJob (tag 37)**
```
[37: u8][job_id: u64][execution_id: u64][triggered_at: u64]
```

**AssignJob (tag 38)** — `[38: u8][job_id: u64][consumer_id: u64]`

**CompleteJob (tag 39)** — `[39: u8][job_id: u64][execution_id: u64]`

**FailJob (tag 40)** — `[40: u8][job_id: u64][execution_id: u64][error: String]`

**TimeoutJob (tag 41)** — `[41: u8][job_id: u64][execution_id: u64]`

### Sessions

**RegisterConsumer (tag 42)**
```
[42: u8][consumer_id: u64][group_name: String][sub_count: u32][sub₀: Subscription]...
```
Subscription = `[entity_type: u8][entity_id: u64]`

**DisconnectConsumer (tag 43)** — `[43: u8][consumer_id: u64]`

**Heartbeat (tag 44)** — `[44: u8][consumer_id: u64]`

**RegisterProducer (tag 45)** — `[45: u8][producer_id: u64][name: Option<String>]`

**DisconnectProducer (tag 46)** — `[46: u8][producer_id: u64]`

## Constructor Methods

Commands are constructed via `MqCommand::*` factory methods defined in `codec.rs`.
These encode fields into a `Vec<u8>` and wrap as `Bytes`:

```rust
// Simple single-field commands
let cmd = MqCommand::delete_topic(42);
let cmd = MqCommand::heartbeat(consumer_id);

// Commands with compound fields
let cmd = MqCommand::create_topic("events", RetentionPolicy::default(), 0);
let cmd = MqCommand::publish(topic_id, &[flat_msg_bytes]);
let cmd = MqCommand::enqueue(queue_id, &messages, &dedup_keys);
let cmd = MqCommand::ack(queue_id, &message_ids, Some(&response_bytes));

// Batch
let cmd = MqCommand::batch(&[cmd1, cmd2, cmd3]);
```

## View Structs

Per-variant view structs provide typed zero-copy accessor APIs over the raw
buffer. Each wraps a `Bytes` (refcounted, no copy) and reads fields at known
offsets:

```rust
match cmd.tag() {
    MqCommand::TAG_PUBLISH => {
        let v = cmd.as_publish();        // -> CmdPublish
        let topic_id = v.topic_id();     // u64 at offset 1
        let count = v.message_count();   // u32 at offset 9
        for msg in v.messages() {        // FlatMessages iterator
            // msg: Bytes — zero-copy slice into the command buffer
        }
    }
    MqCommand::TAG_ENQUEUE => {
        let v = cmd.as_enqueue();        // -> CmdEnqueue
        let queue_id = v.queue_id();     // u64 at offset 1
        for msg in v.messages() { ... }
        for key in v.dedup_keys() { ... } // FlatOptBytes iterator
    }
    // Simple u64-only commands can use field_u64 directly:
    MqCommand::TAG_DELETE_TOPIC => {
        let topic_id = cmd.field_u64(1);
    }
    _ => { ... }
}
```

Available view structs:

| Accessor method                | View struct               |
|--------------------------------|---------------------------|
| `as_create_topic()`            | `CmdCreateTopic`          |
| `as_publish()`                 | `CmdPublish`              |
| `as_create_queue()`            | `CmdCreateQueue`          |
| `as_enqueue()`                 | `CmdEnqueue`              |
| `as_ack()`                     | `CmdAck`                  |
| `as_nack()`                    | `CmdNack`                 |
| `as_extend_visibility()`       | `CmdExtendVisibility`     |
| `as_timeout_expired()`         | `CmdTimeoutExpired`       |
| `as_publish_to_dlq()`          | `CmdPublishToDlq`         |
| `as_expire_pending_messages()` | `CmdExpirePendingMessages` |
| `as_create_exchange()`         | `CmdCreateExchange`       |
| `as_create_binding()`          | `CmdCreateBinding`        |
| `as_publish_to_exchange()`     | `CmdPublishToExchange`    |
| `as_create_actor_namespace()`  | `CmdCreateActorNamespace` |
| `as_send_to_actor()`           | `CmdSendToActor`          |
| `as_deliver_actor_message()`   | `CmdDeliverActorMessage`  |
| `as_ack_actor_message()`       | `CmdAckActorMessage`      |
| `as_nack_actor_message()`      | `CmdNackActorMessage`     |
| `as_assign_actors()`           | `CmdAssignActors`         |
| `as_create_job()`              | `CmdCreateJob`            |
| `as_update_job()`              | `CmdUpdateJob`            |
| `as_fail_job()`                | `CmdFailJob`              |
| `as_register_consumer()`       | `CmdRegisterConsumer`     |
| `as_register_producer()`       | `CmdRegisterProducer`     |
| `as_batch()`                   | `CmdBatch`                |

For simple commands with only u64 fields (DeleteTopic, DeleteQueue, Heartbeat,
etc.), use `cmd.field_u64(1)` directly instead of a view struct.

## Zero-Copy Iterator Types

**`FlatMessages`** — iterates over length-prefixed `Bytes` slices (messages):
```rust
pub struct FlatMessages { buf: Bytes, offset: usize, remaining: u32 }
// Yields: Bytes (zero-copy slice into the command buffer)
```

**`FlatOptBytes`** — iterates over `Option<Bytes>` sequences (dedup keys):
```rust
pub struct FlatOptBytes { buf: Bytes, offset: usize, remaining: u32 }
// Yields: Option<Bytes>
```

**`BatchIter`** — iterates over length-prefixed sub-commands in a Batch:
```rust
let batch = cmd.as_batch();
for sub_cmd in batch.commands() {   // -> BatchIter
    // sub_cmd: MqCommand — wraps a zero-copy slice of the batch buffer
}
```

## Raft Integration

### Encode/Decode

MqCommand implements `bisque_raft::codec::{Encode, Decode, BorrowPayload}`:

- **Encode**: passthrough — writes `self.buf` directly with no transformation
- **Decode via `decode_from_bytes`**: zero-copy wrap — `MqCommand { buf: data }`
- **Decode via `decode<R: Read>`**: reads all remaining bytes into a new `Vec<u8>`
  (used only for snapshot/network paths, not the mmap apply path)
- **BorrowPayload**: returns `&self.buf` for the raft log entry codec to frame

The raft entry codec frames each entry as `[len: u32][payload bytes]`, so
`Decode::decode` using `read_to_end` is correct — the reader is bounded by the
framing layer.

### Serde (OpenRaft compatibility)

OpenRaft's `AppData` trait requires `Serialize + Deserialize`. Custom impls
serialize the raw buffer bytes:

```rust
impl Serialize for MqCommand {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.buf)
    }
}
```

These are only used by OpenRaft's internal snapshot/vote persistence, not the
hot apply path.

## FlatMessage Wire Format

Message payloads (inside Publish, Enqueue, PublishToExchange, etc.) use the
`FlatMessage` format from `flat.rs`:

```
[fixed header: 32 bytes]
  flags:            u16   (bitfield)
  header_count:     u16
  span_count:       u16
  reserved:         u16
  timestamp:        u64
  ttl_ms:           u64   (0 = not set)
  delay_ms:         u64   (0 = not set)

[span index: span_count × 8 bytes]
  Fixed order: value, [key], [routing_key], [reply_to], [correlation_id],
               hdr_key₀, hdr_val₀, hdr_key₁, hdr_val₁, ...
  Each span: [offset: u32][length: u32]  (offset relative to data region)

[data region]
  Raw concatenated bytes — zero-copy sliceable from mmap.
```

Flag bits:
- `0x01` — has key
- `0x02` — has TTL
- `0x04` — has delay
- `0x08` — has routing key
- `0x10` — has reply_to
- `0x20` — has correlation_id

`FlatMessageMeta::parse()` extracts timestamp/TTL/delay from just the first
32 bytes without touching variable-length data — used by the state machine
on the apply path where only metadata matters.

## MqResponse Codec

`MqResponse` remains a Rust enum with hand-rolled `Encode`/`Decode`:

```
Tag  Variant
───  ──────────────
 0   Ok
 1   Error(MqError)
 2   EntityCreated { id: u64 }
 3   Messages { messages: Vec<DeliveredMessage> }
 4   Published { offsets: SmallVec<[u64; 16]> }
 5   Stats(EntityStats)
 6   BatchResponse(Vec<MqResponse>)
 7   DeadLettered { dead_letter_ids: Vec<u64>, dlq_topic_id: u64 }
```

## Write Batcher

The `MqWriteBatcher` coalesces individual command submissions into batched
raft proposals. It merges same-topic Publishes and same-queue Enqueues by
collecting messages from both commands' iterators and re-encoding a combined
command:

```
Writer A: Publish(topic=1, [msg_a])  ─┐
Writer B: Publish(topic=1, [msg_b])  ─┤──→  single Publish(topic=1, [msg_a, msg_b])
Writer C: DeleteTopic(2)             ─┘      + DeleteTopic(2) in Batch
```

Response distribution uses `ResponseSlot::MergedPublish` to split the
`Published { offsets }` response back to original callers by message count.

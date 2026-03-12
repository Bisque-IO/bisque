# bisque-mq Native MQTT Optimization Plan

> **Goal**: Eliminate adapter-layer overhead by pushing MQTT-specific semantics into the
> bisque-mq engine, achieving native-speed MQTT support with crash-safe durability.

## Table of Contents

- [Current Architecture Overview](#current-architecture-overview)
- [Optimization 1: Push-Based Queue Delivery](#optimization-1-push-based-queue-delivery)
- [Optimization 2: Native Retained Message Store](#optimization-2-native-retained-message-store)
- [Optimization 3: Native Will / Testament on Consumer](#optimization-3-native-will--testament-on-consumer)
- [Optimization 4: Native QoS 2 State Machine](#optimization-4-native-qos-2-state-machine)
- [Optimization 5: Message-Level Flags in FlatMessage](#optimization-5-message-level-flags-in-flatmessage)
- [Optimization 6: Native Session Persistence](#optimization-6-native-session-persistence)
- [Optimization 7: Batch Deliver / ACK](#optimization-7-batch-deliver--ack)
- [Optimization 8: Publisher ID in FlatMessage Fixed Header](#optimization-8-publisher-id-in-flatmessage-fixed-header)
- [Implementation Order & Dependencies](#implementation-order--dependencies)
- [Test Coverage Plan](#test-coverage-plan)
- [Tracking](#tracking)

---

## Current Architecture Overview

### How MQTT Maps to bisque-mq Today

| MQTT Concept | Current MQ Mapping | Overhead |
|---|---|---|
| Topic publish | `MqCommand::publish_to_exchange(exchange_id, &[flat_msg])` via `"mqtt/exchange"` (Topic type) | Exchange must be created/cached per session |
| Subscribe filter | Queue `"mqtt/sub/{client}/{dotted_filter}"` + Binding on `"mqtt/exchange"` | 2 entity creates per subscription |
| Retained messages | Separate topic `"$mqtt/retained/{topic}"` with `max_messages=1` | Extra entity per retained topic; extra publish per retain |
| Will messages | `tokio::spawn` delayed task; `DashMap<String, JoinHandle>` | Not raft-replicated; lost on failover |
| QoS 2 state | `HashMap<u16, QoS2InboundState>` / `HashMap<u16, QoS2OutboundState>` in session | Not persisted; lost on crash mid-handshake |
| Session persistence | `InMemorySessionStore` with `DashMap<String, PersistedSession>` | Duplicates MQ consumer state; not raft-replicated |
| No Local flag | Header `mqtt.publisher_session_id` (25-byte key + 8-byte value) | ~40 bytes per message + O(n) header scan on delivery |
| Retain As Published | Header `mqtt.original_retain` (20-byte key + 1-byte value) | ~25 bytes per message + O(n) header scan on delivery |
| Message delivery | Polling via `tokio::time::interval` every 50ms → `MqCommand::deliver()` | 0–50ms latency floor; wasted CPU when idle |
| ACK/NACK | Individual `MqCommand::ack()` per message | N raft proposals per delivery cycle |

### Current MqCommand Tags (57 total: 0–56)

```
Topics:           0-4   (CREATE_TOPIC, DELETE_TOPIC, PUBLISH, COMMIT_OFFSET, PURGE_TOPIC)
Queues:           5-17  (CREATE_QUEUE .. GET_QUEUE_ATTRIBUTES)
Exchanges:        18-22 (CREATE_EXCHANGE .. PUBLISH_TO_EXCHANGE)
Actors:           23-31 (CREATE_ACTOR_NAMESPACE .. EVICT_IDLE_ACTORS)
Jobs:             32-41 (CREATE_JOB .. TIMEOUT_JOB)
Sessions:         42-46 (REGISTER_CONSUMER .. DISCONNECT_PRODUCER)
Batch:            47    (BATCH)
Consumer Groups:  48-56 (CREATE_CONSUMER_GROUP .. EXPIRE_GROUP_SESSIONS)
```

New tags will be allocated starting at **57**.

### Current FlatMessage Fixed Header (32 bytes)

```
offset 0-1:   flags        (u16) — bits 0-5 used
offset 2-3:   header_count (u16)
offset 4-5:   span_count   (u16)
offset 6-7:   value_len    (u16) — RESERVED / unused
offset 8-15:  timestamp    (u64)
offset 16-23: ttl_ms       (u64)
offset 24-31: delay_ms     (u64)
```

Current flag bits used:
```
bit 0: FLAG_HAS_KEY
bit 1: FLAG_HAS_TTL
bit 2: FLAG_HAS_DELAY
bit 3: FLAG_HAS_ROUTING_KEY
bit 4: FLAG_HAS_REPLY_TO
bit 5: FLAG_HAS_CORRELATION_ID
```

Bits 6–15 are available. The `value_len` field at offset 6-7 is unused.

### Current MQTT Adapter Data Flow

**Publish (Inbound)**:
```
client.PUBLISH(topic, payload, qos, retain)
  → session.handle_publish()
    → FlatMessageBuilder with routing_key=topic, payload, headers:
        mqtt.publisher_session_id (25+8 bytes)
        mqtt.original_retain      (20+1 bytes)
        mqtt.content_type         (17+N bytes)
        mqtt.payload_format       (19+1 bytes)
    → PublishPlan { exchange_name, flat_message, retained? }
  → server.orchestrate_publish()
    → ensure_exchange("mqtt/exchange")
    → MqCommand::publish_to_exchange(exchange_id, &[flat_msg])
    → if retained: orchestrate_retained()
        → ensure_topic("$mqtt/retained/{topic}", max_messages=1)
        → MqCommand::publish(topic_id, &[msg])
```

**Deliver (Outbound)**:
```
delivery_interval.tick() (every 50ms)
  → deliver_outbound()
    → for each subscription queue:
        MqCommand::deliver(queue_id, session_id, remaining)
        for each message:
          log_reader.read_messages_at_into(message_id)
          FlatMessage::new(bytes)
          // Per-message header scans:
          extract_publisher_session_id()  → O(n) header iteration
          extract_original_retain()       → O(n) header iteration
          // Filtering:
          $-topic check, No Local check, expiry check, max packet size
          encode_publish_from_flat_with_expiry()
          MqCommand::ack(queue_id, &[message_id])
```

---

## Optimization 1: Push-Based Queue Delivery

### Problem

The MQTT delivery loop uses `tokio::time::interval(50ms)` to poll subscription queues
via `MqCommand::deliver()`. This creates:

1. **Latency floor**: Every message has 0–50ms added latency
2. **Wasted CPU**: Polling runs even when no messages are available
3. **Per-queue deliver commands**: Each subscription queue requires a separate `deliver` call

### Current Flow (server.rs:1210-1318)

```rust
let mut delivery_interval = tokio::time::interval(Duration::from_millis(delivery_poll_ms));
delivery_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

// Main select! loop:
_ = delivery_interval.tick(),
    if !session.is_inflight_full() && session.subscription_count() > 0 => {
    deliver_outbound(session, batcher, log_reader, stream, stats, ...).await?;
}
```

### Proposed Solution

Add an **out-of-band notification channel** from the engine to protocol adapters. When
`PublishToExchange` routes messages to bound queues, the engine notifies any registered
watchers immediately.

#### New Module: `crates/mq/src/notifier.rs`

```rust
use dashmap::DashMap;
use tokio::sync::mpsc;

/// Notifies protocol adapters when new messages are enqueued to a queue.
/// This is a LOCAL mechanism — not raft-replicated.
pub struct QueueNotifier {
    /// Per-queue list of notification senders.
    watchers: DashMap<u64, Vec<mpsc::UnboundedSender<()>>>,
}

impl QueueNotifier {
    pub fn new() -> Self {
        Self { watchers: DashMap::new() }
    }

    /// Register a watcher for a queue. Returns a receiver that fires when
    /// new messages are enqueued to the given queue.
    pub fn watch(&self, queue_id: u64) -> mpsc::UnboundedReceiver<()> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.watchers.entry(queue_id).or_default().push(tx);
        rx
    }

    /// Called by engine after apply_enqueue or exchange routing.
    /// Sends a coalescing notification to all watchers. Removes dead senders.
    pub fn notify(&self, queue_id: u64) {
        if let Some(mut entry) = self.watchers.get_mut(&queue_id) {
            entry.retain(|tx| tx.send(()).is_ok());
        }
    }

    /// Unregister all watchers for a queue.
    pub fn unwatch(&self, queue_id: u64) {
        self.watchers.remove(&queue_id);
    }
}
```

#### Engine Changes (engine.rs)

In the `PublishToExchange` handler, after routing messages to bound queues:

```rust
// After each queue.apply_enqueue() in exchange routing:
if let Some(notifier) = &self.queue_notifier {
    notifier.notify(queue_id);
}
```

Similarly after direct `apply_enqueue()`.

#### Metadata Changes (metadata.rs)

```rust
pub struct MqMetadata {
    // ... existing fields ...
    pub queue_notifier: Option<Arc<QueueNotifier>>,  // NEW
}
```

#### MQTT Adapter Changes (server.rs)

Replace the polling interval with notification-driven delivery:

```rust
// Per-subscription: register watcher
let mut notify_rxs: Vec<mpsc::UnboundedReceiver<()>> = Vec::new();
for sub in session.subscriptions() {
    if let Some(queue_id) = sub.queue_id {
        notify_rxs.push(notifier.watch(queue_id));
    }
}

// Merge all receivers into a single stream
// Main select! loop:
_ = merged_notify_stream.next(),
    if !session.is_inflight_full() && session.subscription_count() > 0 => {
    deliver_outbound(session, batcher, log_reader, stream, stats, ...).await?;
}
```

#### Fallback

Keep a low-frequency heartbeat poll (e.g., 500ms) as a safety net for edge cases
where notifications might be missed (e.g., during leadership transfer):

```rust
tokio::select! {
    _ = merged_notify_stream.next() => { deliver_outbound() }
    _ = fallback_interval.tick() => { deliver_outbound() }  // 500ms safety net
}
```

### Binary Format

No new commands — this is a local notification mechanism, not a raft-replicated command.

### Files to Modify

| File | Change |
|---|---|
| New: `crates/mq/src/notifier.rs` | `QueueNotifier` implementation |
| `crates/mq/src/lib.rs` | Add `pub mod notifier;` |
| `crates/mq/src/engine.rs` | Add `queue_notifier` field; call `notify()` after enqueue/exchange routing |
| `crates/mq/src/metadata.rs` | Add `queue_notifier: Option<Arc<QueueNotifier>>` to `MqMetadata` |
| `crates/mq-mqtt/src/server.rs` | Replace `delivery_interval.tick()` with notification-driven select; register watchers on subscribe; unwatch on unsubscribe |

### Impact

| Metric | Before | After |
|---|---|---|
| Delivery latency | 0–50ms (poll interval) | ~0ms (notification-driven) |
| CPU idle overhead | Continuous polling every 50ms | Zero (event-driven) |
| Raft commands per delivery | Unchanged | Unchanged (deliver/ack still needed) |
| Complexity | Simple polling loop | Notification registration + merged stream |

---

## Optimization 2: Native Retained Message Store

### Problem

Each retained message creates a separate bisque-mq **topic** entity:
- Topic name: `$mqtt/retained/{mqtt_topic}` with `max_messages=1`
- A broker with 10,000 retained topics creates 10,000 topic entities
- Each subscribe with `retain_handling=0` requires topic lookup + raft log read
- Two raft round-trips per retained publish (create_topic + publish)

### Current Flow

```
Publish with retain (session.rs:1400-1422):
    retained_topic_name = format!("{}{}", config.retained_prefix, mqtt_topic)
    plan.retained = Some(RetainedPlan { topic_name, flat_message })

Orchestrate retained (server.rs:544-575):
    topic_id = ensure_topic(topic_name, RetentionPolicy { max_messages: 1 })
      → MqCommand::create_topic(topic_name, retention, 0)  // raft round-trip
    if msg is Some:
      MqCommand::publish(topic_id, &[msg])                  // raft round-trip
    else:
      MqCommand::purge_topic(topic_id, u64::MAX)            // raft round-trip (clear)
```

### Proposed Solution

Add a dedicated retained-message store as part of the exchange entity. Retained
messages are keyed by routing key within an exchange and stored in a HashMap.

#### New Commands

| Tag | Name | Purpose |
|---|---|---|
| 57 | `TAG_SET_RETAINED` | Store or replace a retained message |
| 58 | `TAG_DELETE_RETAINED` | Clear a retained message for a routing key |
| 59 | `TAG_GET_RETAINED` | Fetch retained messages matching a pattern |

#### Binary Layouts

**TAG_SET_RETAINED (57)**
```
[tag:u8(57)][exchange_id:u64@1][routing_key:str@9][message:bytes@after_key]
  tag:          1 byte
  exchange_id:  8 bytes (LE u64)
  routing_key:  4 bytes (LE u32 length) + key bytes
  message:      4 bytes (LE u32 length) + FlatMessage bytes
```

**TAG_DELETE_RETAINED (58)**
```
[tag:u8(58)][exchange_id:u64@1][routing_key:str@9]
  tag:          1 byte
  exchange_id:  8 bytes (LE u64)
  routing_key:  4 bytes (LE u32 length) + key bytes
```

**TAG_GET_RETAINED (59)**
```
[tag:u8(59)][exchange_id:u64@1][pattern:str@9]
  tag:          1 byte
  exchange_id:  8 bytes (LE u64)
  pattern:      4 bytes (LE u32 length) + pattern bytes (wildcard filter)
```

#### New Types (types.rs)

```rust
/// A retained message entry returned by TAG_GET_RETAINED.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetainedEntry {
    pub routing_key: Bytes,
    pub message: Bytes,   // FlatMessage bytes
}

pub enum MqResponse {
    // ... existing variants ...
    RetainedMessages {
        messages: Vec<RetainedEntry>,
    },
}
```

#### Engine State Changes (exchange.rs)

```rust
/// A retained message stored in an exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetainedMessage {
    pub routing_key: String,
    pub message: Bytes,       // FlatMessage bytes
    pub timestamp: u64,
}

pub struct ExchangeState {
    pub meta: ExchangeMeta,
    pub bindings: HashMap<u64, Binding>,
    pub direct_index: HashMap<u64, Vec<u64>>,
    pub retained: HashMap<u64, RetainedMessage>,  // NEW: name_hash(routing_key) → msg
}
```

#### Constructor Methods (codec.rs)

```rust
impl MqCommand {
    pub fn set_retained(exchange_id: u64, routing_key: &str, message: &Bytes) -> Self {
        build_cmd!(Self::TAG_SET_RETAINED,
            |w| exchange_id.encode(w),
            |w| encode_str(w, routing_key),
            |w| encode_bytes(w, message)
        )
    }

    pub fn delete_retained(exchange_id: u64, routing_key: &str) -> Self {
        build_cmd!(Self::TAG_DELETE_RETAINED,
            |w| exchange_id.encode(w),
            |w| encode_str(w, routing_key)
        )
    }

    pub fn get_retained(exchange_id: u64, pattern: &str) -> Self {
        build_cmd!(Self::TAG_GET_RETAINED,
            |w| exchange_id.encode(w),
            |w| encode_str(w, pattern)
        )
    }
}
```

#### View Structs (codec.rs)

```rust
pub struct CmdSetRetained { buf: Bytes }
impl CmdSetRetained {
    pub fn exchange_id(&self) -> u64 { /* buf[1..9] */ }
    pub fn routing_key(&self) -> &str { /* decode_str at offset 9 */ }
    pub fn message(&self) -> Bytes { /* decode_bytes after routing_key */ }
}

pub struct CmdDeleteRetained { buf: Bytes }
impl CmdDeleteRetained {
    pub fn exchange_id(&self) -> u64 { /* buf[1..9] */ }
    pub fn routing_key(&self) -> &str { /* decode_str at offset 9 */ }
}

pub struct CmdGetRetained { buf: Bytes }
impl CmdGetRetained {
    pub fn exchange_id(&self) -> u64 { /* buf[1..9] */ }
    pub fn pattern(&self) -> &str { /* decode_str at offset 9 */ }
}
```

#### Engine Handlers (engine.rs)

```rust
MqCommand::TAG_SET_RETAINED => {
    let cmd = cmd.as_set_retained();
    let exchange_id = cmd.exchange_id();
    if let Some(mut exchange) = self.metadata.exchanges.get_mut(&exchange_id) {
        let key = cmd.routing_key();
        let key_hash = name_hash(key);
        exchange.retained.insert(key_hash, RetainedMessage {
            routing_key: key.to_string(),
            message: cmd.message(),
            timestamp: current_time,
        });
        MqResponse::Ok
    } else {
        MqResponse::Error(MqError::NotFound {
            entity: EntityKind::Exchange,
            id: exchange_id,
        })
    }
}

MqCommand::TAG_DELETE_RETAINED => {
    let cmd = cmd.as_delete_retained();
    let exchange_id = cmd.exchange_id();
    if let Some(mut exchange) = self.metadata.exchanges.get_mut(&exchange_id) {
        exchange.retained.remove(&name_hash(cmd.routing_key()));
        MqResponse::Ok
    } else {
        MqResponse::Error(MqError::NotFound {
            entity: EntityKind::Exchange,
            id: exchange_id,
        })
    }
}

MqCommand::TAG_GET_RETAINED => {
    let cmd = cmd.as_get_retained();
    let exchange_id = cmd.exchange_id();
    if let Some(exchange) = self.metadata.exchanges.get(&exchange_id) {
        let pattern = cmd.pattern();
        let mut results = Vec::new();

        if !pattern.contains('+') && !pattern.contains('#') && !pattern.contains('*') {
            // Exact match — O(1) hash lookup
            if let Some(entry) = exchange.retained.get(&name_hash(pattern)) {
                results.push(RetainedEntry {
                    routing_key: Bytes::from(entry.routing_key.clone()),
                    message: entry.message.clone(),
                });
            }
        } else {
            // Wildcard — scan all retained, apply topic_pattern_matches
            for entry in exchange.retained.values() {
                if topic_pattern_matches(pattern, &entry.routing_key) {
                    results.push(RetainedEntry {
                        routing_key: Bytes::from(entry.routing_key.clone()),
                        message: entry.message.clone(),
                    });
                }
            }
        }
        MqResponse::RetainedMessages { messages: results }
    } else {
        MqResponse::Error(MqError::NotFound {
            entity: EntityKind::Exchange,
            id: exchange_id,
        })
    }
}
```

#### MQTT Adapter Changes

**session.rs** — Replace `RetainedPlan`:
```rust
// Before:
//   plan.retained = Some(RetainedPlan { topic_name, flat_message })
// After:
if publish.retain {
    if payload.is_empty() {
        plan.commands.push(MqCommand::delete_retained(exchange_id, &topic));
    } else {
        plan.commands.push(MqCommand::set_retained(exchange_id, &topic, &flat_msg));
    }
}
```

**server.rs** — Replace `orchestrate_retained()`:
```rust
// On subscribe with retain_handling=0 or 1:
let resp = batcher.submit(MqCommand::get_retained(exchange_id, &filter)).await;
if let MqResponse::RetainedMessages { messages } = resp {
    for entry in messages {
        let flat = FlatMessage::new(entry.message).unwrap();
        encode_publish_from_flat_with_expiry(
            &flat, effective_qos, /*retain=*/true, /*dup=*/false,
            packet_id, is_v5, subscription_id, topic_alias, expiry_secs,
            &mut write_buf
        );
        stream.write_all(&write_buf).await?;
    }
}
```

#### Persistence

Retained messages are included in the `ExchangeState` serialization for MDBX snapshots.
The `ExchangeMeta` serde derive handles the new `retained` field with `#[serde(default)]`
for backward-compatible deserialization of old snapshots.

### Files to Modify

| File | Change |
|---|---|
| `crates/mq/src/types.rs` | Add TAG_SET_RETAINED=57, TAG_DELETE_RETAINED=58, TAG_GET_RETAINED=59; add `RetainedEntry`; add `MqResponse::RetainedMessages` |
| `crates/mq/src/codec.rs` | Add `set_retained()`, `delete_retained()`, `get_retained()` constructors; add `CmdSetRetained`, `CmdDeleteRetained`, `CmdGetRetained` view structs; add `as_set_retained()`, `as_delete_retained()`, `as_get_retained()` methods |
| `crates/mq/src/engine.rs` | Add 3 match arms for TAG 57-59 |
| `crates/mq/src/exchange.rs` | Add `RetainedMessage` struct; add `retained: HashMap<u64, RetainedMessage>` to `ExchangeState`; initialize in constructor |
| `crates/mq/src/manifest.rs` | Include retained in exchange serialization (via serde on ExchangeState) |
| `crates/mq-mqtt/src/session.rs` | Replace `RetainedPlan` with `set_retained`/`delete_retained` commands |
| `crates/mq-mqtt/src/server.rs` | Replace `orchestrate_retained()` with `get_retained()` on subscribe; remove `ensure_topic` calls for retained |

### Impact

| Metric | Before | After |
|---|---|---|
| Entity count for 10K retained topics | +10,000 topic entities | 0 extra entities |
| Retained publish raft commands | 2 (create_topic + publish) | 1 (set_retained) |
| Retained subscribe lookup | Topic name lookup + raft log read | O(1) hash lookup or pattern scan |
| Memory per retained topic | Full TopicState (~500+ bytes) | ~80 bytes (HashMap entry) |
| Retained in snapshot | 10K separate topic entries | Part of exchange state |

---

## Optimization 3: Native Will / Testament on Consumer

### Problem

Will messages are stored in the MQTT adapter and published via `tokio::spawn`:
- **Not raft-replicated**: Lost on node failover
- **Delayed will timer**: A spawned tokio task, lost on crash
- **Coordination overhead**: `DashMap<String, JoinHandle>` for pending wills

### Current Flow

```
Unclean disconnect (server.rs:1124-1158):
    if will_delay > 0:
        handle = tokio::spawn(async {
            sleep(Duration::from_secs(will_delay));
            MqCommand::publish_to_exchange(exchange_id, &[will_msg])
        })
        pending_wills.insert(client_id, handle)
    else:
        orchestrate_publish(will_plan)

Cancel on reconnect (server.rs:1086-1093):
    if let Some(handle) = pending_wills.remove(&client_id):
        handle.abort()
```

### Proposed Solution

Store will message data in the engine's `ConsumerMeta`. The engine publishes the
will automatically on `DisconnectConsumer` when the disconnect is unclean.

#### New Commands

| Tag | Name | Purpose |
|---|---|---|
| 60 | `TAG_SET_WILL` | Set/update will message for a consumer |
| 61 | `TAG_CLEAR_WILL` | Clear will (clean disconnect) |

#### Binary Layouts

**TAG_SET_WILL (60)**
```
[tag:u8(60)][consumer_id:u64@1][exchange_id:u64@9][will_delay_secs:u32@17][will_qos:u8@21][will_retain:u8@22][routing_key:str@23][message:bytes@after_key]
  tag:              1 byte
  consumer_id:      8 bytes (LE u64)
  exchange_id:      8 bytes (LE u64)
  will_delay_secs:  4 bytes (LE u32)
  will_qos:         1 byte
  will_retain:      1 byte (0 or 1)
  routing_key:      4 bytes (LE u32 length) + key bytes
  message:          4 bytes (LE u32 length) + FlatMessage bytes
```

**TAG_CLEAR_WILL (61)**
```
[tag:u8(61)][consumer_id:u64@1]
  tag:          1 byte
  consumer_id:  8 bytes (LE u64)
```

#### New Types

```rust
/// Will message stored in ConsumerMeta.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WillMessage {
    pub exchange_id: u64,
    pub routing_key: String,
    pub message: Bytes,           // FlatMessage bytes
    pub delay_secs: u32,
    pub qos: u8,
    pub retain: bool,
}
```

#### Engine State Changes (consumer.rs)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerMeta {
    pub consumer_id: u64,
    pub group_name: String,
    pub connected_at: u64,
    pub last_heartbeat_at: u64,
    pub subscriptions: HashSet<Subscription>,
    pub assigned_jobs: HashSet<u64>,
    #[serde(default)]
    pub will: Option<WillMessage>,          // NEW
}
```

#### New Response Variant

```rust
pub enum MqResponse {
    // ... existing ...
    /// Returned when a consumer disconnects with a delayed will.
    /// The adapter should schedule a timer and submit publish_to_exchange after delay.
    WillPending {
        consumer_id: u64,
        delay_secs: u32,
        exchange_id: u64,
        routing_key: String,
        message: Bytes,
        qos: u8,
        retain: bool,
    },
}
```

#### Constructor Methods (codec.rs)

```rust
impl MqCommand {
    pub fn set_will(
        consumer_id: u64,
        exchange_id: u64,
        will_delay_secs: u32,
        will_qos: u8,
        will_retain: bool,
        routing_key: &str,
        message: &Bytes,
    ) -> Self { ... }

    pub fn clear_will(consumer_id: u64) -> Self { ... }
}
```

#### Engine Handlers (engine.rs)

```rust
MqCommand::TAG_SET_WILL => {
    let cmd = cmd.as_set_will();
    if let Some(mut consumer) = self.metadata.consumers.get_mut(&cmd.consumer_id()) {
        consumer.meta.will = Some(WillMessage {
            exchange_id: cmd.exchange_id(),
            routing_key: cmd.routing_key().to_string(),
            message: cmd.message(),
            delay_secs: cmd.will_delay_secs(),
            qos: cmd.will_qos(),
            retain: cmd.will_retain(),
        });
        MqResponse::Ok
    } else {
        MqResponse::Error(MqError::NotFound {
            entity: EntityKind::Consumer,
            id: cmd.consumer_id(),
        })
    }
}

MqCommand::TAG_CLEAR_WILL => {
    let consumer_id = cmd.field_u64(1);
    if let Some(mut consumer) = self.metadata.consumers.get_mut(&consumer_id) {
        consumer.meta.will = None;
        MqResponse::Ok
    } else {
        MqResponse::Error(MqError::NotFound {
            entity: EntityKind::Consumer,
            id: consumer_id,
        })
    }
}

// Modified TAG_DISCONNECT_CONSUMER handler — add will check:
MqCommand::TAG_DISCONNECT_CONSUMER => {
    let consumer_id = cmd.field_u64(1);
    // ... existing disconnect logic (nack in-flight, release actors) ...

    if let Some(will) = consumer_meta.will.take() {
        if will.delay_secs == 0 {
            // Immediate will — publish to exchange inline
            if let Some(exchange) = self.metadata.exchanges.get(&will.exchange_id) {
                // Route will message through exchange
                let msg_bytes = will.message.clone();
                self.route_to_exchange(&exchange, &[msg_bytes], log_index, current_time);
            }
            // If will.retain, also set retained:
            if will.retain {
                if let Some(mut exchange) = self.metadata.exchanges.get_mut(&will.exchange_id) {
                    exchange.retained.insert(name_hash(&will.routing_key), RetainedMessage {
                        routing_key: will.routing_key,
                        message: will.message,
                        timestamp: current_time,
                    });
                }
            }
            MqResponse::Ok
        } else {
            // Delayed will — return to adapter for scheduling
            MqResponse::WillPending {
                consumer_id,
                delay_secs: will.delay_secs,
                exchange_id: will.exchange_id,
                routing_key: will.routing_key,
                message: will.message,
                qos: will.qos,
                retain: will.retain,
            }
        }
    } else {
        MqResponse::Ok
    }
}
```

For delayed wills, the engine returns `WillPending` and the adapter schedules a
timer. When the timer fires, the adapter submits `MqCommand::publish_to_exchange()`.
If the client reconnects before the timer fires, `TAG_CLEAR_WILL` cancels it.

**Alternative for delayed wills**: Use the existing Job system (`TAG_CREATE_JOB`
with a one-shot schedule), making delayed wills fully raft-replicated. This trades
complexity for full crash-safety on delayed wills.

#### MQTT Adapter Changes

```rust
// On CONNECT with will (server.rs):
let will_flat_msg = build_will_flat_message(&will);
batcher.submit(MqCommand::set_will(
    session.session_id, exchange_id,
    will.delay_secs, will.qos as u8, will.retain,
    &will.topic, &will_flat_msg,
)).await;

// On clean DISCONNECT:
batcher.submit(MqCommand::clear_will(session.session_id)).await;
batcher.submit(MqCommand::disconnect_consumer(session.session_id)).await;

// On unclean DISCONNECT:
let resp = batcher.submit(MqCommand::disconnect_consumer(session.session_id)).await;
if let MqResponse::WillPending { delay_secs, exchange_id, message, .. } = resp {
    // Schedule delayed will publication
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(delay_secs as u64)).await;
        batcher.submit(MqCommand::publish_to_exchange(exchange_id, &[message])).await;
    });
}
```

### Files to Modify

| File | Change |
|---|---|
| `crates/mq/src/types.rs` | Add TAG_SET_WILL=60, TAG_CLEAR_WILL=61; add `WillMessage` struct; add `MqResponse::WillPending` |
| `crates/mq/src/codec.rs` | Add `set_will()`, `clear_will()` constructors; add view structs |
| `crates/mq/src/engine.rs` | Add TAG_SET_WILL/TAG_CLEAR_WILL handlers; modify disconnect_consumer to check will |
| `crates/mq/src/consumer.rs` | Add `will: Option<WillMessage>` to `ConsumerMeta` with `#[serde(default)]` |
| `crates/mq-mqtt/src/server.rs` | Replace `tokio::spawn` will logic with `set_will`/`clear_will`; handle `WillPending` response |
| `crates/mq-mqtt/src/session.rs` | Remove adapter-level will state tracking |

### Impact

| Metric | Before | After |
|---|---|---|
| Will durability (immediate) | Lost on crash/failover | Raft-replicated; survives failover |
| Will publish on disconnect | Adapter-level spawned task | Engine-level atomic operation |
| Pending will tracking | `DashMap<String, JoinHandle>` | `ConsumerMeta.will` (persisted in MDBX) |
| Will cancellation | `handle.abort()` | `MqCommand::clear_will()` |
| Delayed will durability | Lost on crash | `WillPending` response + adapter timer (or Job system for full durability) |

---

## Optimization 4: Native QoS 2 State Machine

### Problem

QoS 2 exactly-once delivery requires a 4-phase handshake. The adapter tracks this
in per-session HashMaps that are **not crash-safe**:

```rust
// session.rs:300-304
qos2_inbound: HashMap<u16, QoS2InboundState>,     // PubRecSent | Complete
qos2_outbound: HashMap<u16, QoS2OutboundState>,    // PublishSent { queue_id, message_id }
                                                    // PubRelSent { queue_id, message_id }
```

If the broker crashes between PUBREC and PUBCOMP, the QoS 2 state is lost and the
message may be duplicated or lost — violating exactly-once semantics.

### Current Flow

```
Inbound QoS 2 (client publishing to broker):
  PUBLISH → insert qos2_inbound[packet_id] = PubRecSent → send PUBREC
  PUBREL  → qos2_inbound[packet_id] = Complete → send PUBCOMP
  (duplicate PUBLISH with same packet_id while PubRecSent → resend PUBREC, skip re-publish)

Outbound QoS 2 (broker delivering to client):
  deliver → insert qos2_outbound[packet_id] = PublishSent { queue_id, message_id }
  PUBREC  → qos2_outbound[packet_id] = PubRelSent { queue_id, message_id } → send PUBREL
  PUBCOMP → remove qos2_outbound[packet_id] → MqCommand::ack(queue_id, &[message_id])
```

### Proposed Solution

Extend the queue `MessageState` enum with two new states for QoS 2 tracking:

```
Current:  Pending → InFlight → Acked | DeadLetter
New:      Pending → InFlight → Received → Released → Acked | DeadLetter
```

- **Received**: PUBREC has been sent (outbound) or received (inbound). Message is
  committed but awaiting PUBREL.
- **Released**: PUBREL has been received and PUBCOMP sent. Message is fully released
  and can be acked.

#### New Commands

| Tag | Name | Purpose |
|---|---|---|
| 62 | `TAG_MARK_RECEIVED` | Transition InFlight → Received (PUBREC phase) |
| 63 | `TAG_MARK_RELEASED` | Transition Received → Released (PUBREL/PUBCOMP phase) |

#### Binary Layouts

**TAG_MARK_RECEIVED (62)**
```
[tag:u8(62)][queue_id:u64@1][message_ids:vec_u64@9]
  tag:          1 byte
  queue_id:     8 bytes (LE u64)
  message_ids:  4 bytes (LE u32 count) + count × 8 bytes (LE u64)
```

**TAG_MARK_RELEASED (63)**
```
[tag:u8(63)][queue_id:u64@1][message_ids:vec_u64@9]
  tag:          1 byte
  queue_id:     8 bytes (LE u64)
  message_ids:  4 bytes (LE u32 count) + count × 8 bytes (LE u64)
```

#### MessageState Changes (types.rs)

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageState {
    Pending,
    InFlight,
    Received,     // NEW: QoS 2 — PUBREC sent/received, awaiting PUBREL
    Released,     // NEW: QoS 2 — PUBREL received, PUBCOMP sent
    Acked,
    DeadLetter,
}
```

#### QueueState Changes (queue.rs)

```rust
impl QueueState {
    /// Transition messages from InFlight → Received.
    /// Called when PUBREC is sent (outbound QoS 2) or received (inbound QoS 2).
    pub fn apply_mark_received(&self, message_ids: &[u64]) -> MqResponse {
        for &id in message_ids {
            if let Some(mut msg) = self.messages.get_mut(&id) {
                if msg.state == MessageState::InFlight {
                    msg.state = MessageState::Received;
                    // Remove from in-flight count — no longer subject to visibility timeout
                    self.in_flight_count.fetch_sub(1, Ordering::Relaxed);
                    if let Some(len) = msg.value_len.checked_into() {
                        self.in_flight_bytes.fetch_sub(len as u64, Ordering::Relaxed);
                    }
                    // Remove from visibility deadline tracking
                    if let Some(deadline) = msg.visibility_deadline.take() {
                        if let Some(mut deadlines) = self.in_flight_deadlines.lock().unwrap()
                            .get_mut(&deadline) {
                            deadlines.retain(|&mid| mid != id);
                        }
                    }
                }
            }
        }
        MqResponse::Ok
    }

    /// Transition messages from Received → Released.
    /// Called when PUBREL is received (outbound QoS 2).
    pub fn apply_mark_released(&self, message_ids: &[u64]) -> MqResponse {
        for &id in message_ids {
            if let Some(mut msg) = self.messages.get_mut(&id) {
                if msg.state == MessageState::Received {
                    msg.state = MessageState::Released;
                }
            }
        }
        MqResponse::Ok
    }
}
```

#### Interaction with Existing State Transitions

- `apply_ack()`: Now also accepts messages in `Received` or `Released` state
  (not just `InFlight`). This handles edge cases where the adapter skips intermediate states.
- `apply_nack()`: Only works on `InFlight` state (not `Received`/`Released`).
  Once PUBREC is sent, the message cannot be nacked.
- `apply_timeout_expired()`: Only fires on `InFlight` state. `Received` and `Released`
  messages are not subject to visibility timeout.
- `purge()`: Clears all states including `Received` and `Released`.
- Snapshot: New states are serialized/deserialized via existing serde on `MessageState`.

#### Recovery Semantics

On consumer reconnect after crash:
- Messages in `Received` state → Adapter resends PUBREC (client will send PUBREL again)
- Messages in `Released` state → Adapter resends PUBCOMP (client ignores duplicate)
- Messages in `InFlight` state → Normal visibility timeout; message will be redelivered

#### MQTT Adapter Changes

```rust
// Outbound QoS 2 — on delivering a message:
// (keep existing: deliver → InFlight)

// On receiving PUBREC from client:
batcher.submit(MqCommand::mark_received(queue_id, &[message_id])).await;
// Send PUBREL

// On receiving PUBCOMP from client:
batcher.submit(MqCommand::mark_released(queue_id, &[message_id])).await;
batcher.submit(MqCommand::ack(queue_id, &[message_id], None)).await;
// Or combine mark_released + ack if desired

// On session recovery (reconnect):
// Query engine for messages in Received/Released states for this consumer
// Resend appropriate protocol messages
```

### Files to Modify

| File | Change |
|---|---|
| `crates/mq/src/types.rs` | Add TAG_MARK_RECEIVED=62, TAG_MARK_RELEASED=63; add `Received`/`Released` to `MessageState` |
| `crates/mq/src/codec.rs` | Add `mark_received()`, `mark_released()` constructors; add view structs |
| `crates/mq/src/engine.rs` | Add 2 match arms for TAG 62-63 |
| `crates/mq/src/queue.rs` | Add `apply_mark_received()`, `apply_mark_released()`; update `apply_ack()` to accept Received/Released; update `apply_timeout_expired()` to skip Received/Released |
| `crates/mq-mqtt/src/session.rs` | Remove `qos2_outbound` HashMap; use engine state instead |
| `crates/mq-mqtt/src/server.rs` | Submit mark_received/mark_released commands during QoS 2 handshake |

### Impact

| Metric | Before | After |
|---|---|---|
| QoS 2 crash safety | Lost on crash — messages may duplicate/lose | Raft-replicated; exactly-once preserved |
| State tracking | Adapter HashMap (per-session, ephemeral) | Engine QueueMessageMeta (persisted in MDBX snapshot) |
| Recovery after crash | Indeterminate state | Deterministic: Received → resend PUBREC; Released → resend PUBCOMP |
| Memory | Per-session HashMap entries | Per-message state in queue (already tracked) |

---

## Optimization 5: Message-Level Flags in FlatMessage

### Problem

MQTT-specific per-message metadata is stored as **string headers** in FlatMessage:

| Header Key | Key Size | Value Size | Span Overhead | Total |
|---|---|---|---|---|
| `mqtt.publisher_session_id` | 25 bytes | 8 bytes | 16 bytes (2 spans) | 49 bytes |
| `mqtt.original_retain` | 20 bytes | 1 byte | 16 bytes (2 spans) | 37 bytes |
| `mqtt.content_type` | 17 bytes | N bytes | 16 bytes (2 spans) | 33+N bytes |
| `mqtt.payload_format` | 19 bytes | 1 byte | 16 bytes (2 spans) | 36 bytes |

**Total overhead per MQTT message: ~155+ bytes** just for protocol metadata.

On delivery, extracting these requires O(n) iteration over all headers:
```rust
fn extract_publisher_session_id(flat: &FlatMessage) -> Option<u64> {
    for i in 0..flat.header_count() {
        let (k, v) = flat.header(i);
        if &k[..] == b"mqtt.publisher_session_id" && v.len() == 8 { ... }
    }
}
```

### Proposed Solution

Use unused bits in the FlatMessage `flags` field (u16, bits 6-15 are free).

#### New Flag Bits

```rust
// Existing flags (bits 0-5):
const FLAG_HAS_KEY:             u16 = 1 << 0;
const FLAG_HAS_TTL:             u16 = 1 << 1;
const FLAG_HAS_DELAY:           u16 = 1 << 2;
const FLAG_HAS_ROUTING_KEY:     u16 = 1 << 3;
const FLAG_HAS_REPLY_TO:        u16 = 1 << 4;
const FLAG_HAS_CORRELATION_ID:  u16 = 1 << 5;

// NEW flags (bits 6-9):
const FLAG_RETAIN:              u16 = 1 << 6;   // Original MQTT retain flag
const FLAG_NO_LOCAL:            u16 = 1 << 7;   // Publisher excludes own consumers
const FLAG_UTF8_PAYLOAD:        u16 = 1 << 8;   // Payload is valid UTF-8 (MQTT 5.0 §3.3.2.3.2)
const FLAG_HAS_PUBLISHER_ID:    u16 = 1 << 9;   // publisher_id field is set (used by Opt 8)
```

Bits 10-15 remain reserved for future use.

#### FlatMessage Accessors (flat.rs)

```rust
impl FlatMessage {
    /// Whether the original MQTT PUBLISH had the retain flag set.
    #[inline]
    pub fn is_retain(&self) -> bool {
        self.meta.flags & FLAG_RETAIN != 0
    }

    /// Whether this message should not be delivered to the publisher's own consumers
    /// (MQTT 5.0 No Local subscription option).
    #[inline]
    pub fn is_no_local(&self) -> bool {
        self.meta.flags & FLAG_NO_LOCAL != 0
    }

    /// Whether the payload is valid UTF-8 (MQTT 5.0 payload_format_indicator=1).
    #[inline]
    pub fn is_utf8_payload(&self) -> bool {
        self.meta.flags & FLAG_UTF8_PAYLOAD != 0
    }
}
```

#### FlatMessageBuilder Methods (flat.rs)

```rust
impl FlatMessageBuilder {
    /// Set the MQTT retain flag.
    pub fn retain(mut self, retain: bool) -> Self {
        if retain { self.flags |= FLAG_RETAIN; }
        self
    }

    /// Set the No Local flag (exclude publisher's own consumers).
    pub fn no_local(mut self, no_local: bool) -> Self {
        if no_local { self.flags |= FLAG_NO_LOCAL; }
        self
    }

    /// Set the UTF-8 payload indicator.
    pub fn utf8_payload(mut self, utf8: bool) -> Self {
        if utf8 { self.flags |= FLAG_UTF8_PAYLOAD; }
        self
    }
}
```

#### FlatMessageMeta Changes (flat.rs)

No structural changes — `FlatMessageMeta.flags` already captures all 16 bits.
The new flag bits are just defined as constants and accessed via methods.

#### MQTT Adapter Changes

**session.rs** — Replace headers with flags:
```rust
// Before:
builder = builder.header(HDR_ORIGINAL_RETAIN.clone(), Bytes::from_static(&[1]));
// After:
builder = builder.retain(publish.retain);

// Before:
if payload_format_indicator == 1 {
    builder = builder.header(HDR_PAYLOAD_FORMAT.clone(), Bytes::from_static(&[1]));
}
// After:
builder = builder.utf8_payload(payload_format_indicator == 1);
```

**server.rs** — Replace header scans with flag checks:
```rust
// Before:
let retain_flag = if retain_as_published {
    extract_original_retain(&flat)  // O(n) header scan
} else { false };

// After:
let retain_flag = if retain_as_published {
    flat.is_retain()  // O(1) bit check
} else { false };
```

#### Backward Compatibility

Old messages in the raft log have flags with bits 6-9 as 0, which means:
- `is_retain()` → false (correct default)
- `is_no_local()` → false (correct default)
- `is_utf8_payload()` → false (correct default)

Messages that were written with the old header-based approach will still have
the headers. The adapter can check both: flag first, then fall back to header
scan for old messages. Over time, as old messages are purged, the header
fallback becomes unnecessary.

### Files to Modify

| File | Change |
|---|---|
| `crates/mq-protocol/src/flat.rs` | Add FLAG_RETAIN, FLAG_NO_LOCAL, FLAG_UTF8_PAYLOAD, FLAG_HAS_PUBLISHER_ID constants; add `is_retain()`, `is_no_local()`, `is_utf8_payload()` methods; add `retain()`, `no_local()`, `utf8_payload()` builder methods |
| `crates/mq-mqtt/src/session.rs` | Use `builder.retain(true)` instead of `builder.header(HDR_ORIGINAL_RETAIN, ...)` |
| `crates/mq-mqtt/src/server.rs` | Use `flat.is_retain()` instead of `extract_original_retain()`; use `flat.is_no_local()` for initial check |

### Impact

| Metric | Before | After |
|---|---|---|
| Per-message overhead (retain flag) | 37 bytes (header + spans) | 0 bytes (flag bit) |
| Per-message overhead (no_local indication) | 49 bytes (header + spans) | 0 bytes (flag bit) |
| Per-message overhead (utf8 indicator) | 36 bytes (header + spans) | 0 bytes (flag bit) |
| Total savings per MQTT message | — | ~120 bytes |
| Delivery filter check | O(n) header scan per check | O(1) bitwise AND |
| Encoding overhead | 6 extra spans + header data | Zero additional encoding |

---

## Optimization 6: Native Session Persistence

### Problem

The MQTT adapter maintains its own `InMemorySessionStore` (session_store.rs):

```rust
pub struct InMemorySessionStore {
    sessions: DashMap<String, PersistedSession>,
}

pub struct PersistedSession {
    pub client_id: String,
    pub subscriptions: Vec<PersistedSubscription>,
    pub pending_qos1: Vec<u16>,
    pub pending_qos2: Vec<u16>,
    pub session_expiry_interval: u32,
    pub disconnected_at: Option<Instant>,
}
```

Problems:
- **Not raft-replicated**: Lost on node failover
- **Duplicates MQ state**: Consumer already has subscriptions in engine
- **Memory-only**: No disk persistence across restarts
- **Adapter-specific**: Other protocols (AMQP) can't reuse
- **Instant-based expiry**: Uses `Instant::now()` which is not deterministic across nodes

### Proposed Solution

Add session persistence commands to the engine. The engine stores session data
(subscriptions, QoS tracking, expiry) in a dedicated map, persisted via MDBX.

#### New Commands

| Tag | Name | Purpose |
|---|---|---|
| 64 | `TAG_PERSIST_SESSION` | Save session state on disconnect |
| 65 | `TAG_RESTORE_SESSION` | Load persisted session on reconnect |
| 66 | `TAG_EXPIRE_SESSIONS` | Periodic cleanup of expired sessions |

#### Binary Layouts

**TAG_PERSIST_SESSION (64)**
```
[tag:u8(64)][consumer_id:u64@1][client_id:str@9][session_expiry_secs:u32@after_id][subscription_data:bytes@after_expiry]
  tag:                  1 byte
  consumer_id:          8 bytes (LE u64)
  client_id:            4 bytes (LE u32 length) + client_id bytes
  session_expiry_secs:  4 bytes (LE u32) — 0 = delete on disconnect, 0xFFFFFFFF = never expire
  subscription_data:    4 bytes (LE u32 length) + opaque serialized subscription blob
```

The `subscription_data` is protocol-opaque: the engine stores it as-is and returns
it verbatim on restore. The MQTT adapter serializes/deserializes it using its own
format (Vec<PersistedSubscription>).

**TAG_RESTORE_SESSION (65)**
```
[tag:u8(65)][client_id:str@1]
  tag:        1 byte
  client_id:  4 bytes (LE u32 length) + client_id bytes
```

**TAG_EXPIRE_SESSIONS (66)**
```
[tag:u8(66)][now_ms:u64@1]
  tag:     1 byte
  now_ms:  8 bytes (LE u64) — current timestamp in milliseconds
```

#### New Response Variants (types.rs)

```rust
pub enum MqResponse {
    // ... existing ...
    /// Returned when a persisted session is found and still valid.
    SessionRestored {
        consumer_id: u64,
        session_expiry_secs: u32,
        subscription_data: Bytes,
    },
    /// Returned when no persisted session exists or it has expired.
    SessionNotFound,
}
```

#### Engine State (metadata.rs)

```rust
/// Persisted session metadata stored in the engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSessionMeta {
    pub client_id: String,
    pub consumer_id: u64,
    pub session_expiry_secs: u32,
    pub disconnected_at_ms: u64,         // Engine timestamp in ms
    pub subscription_data: Bytes,        // Opaque blob
}

pub struct MqMetadata {
    // ... existing fields ...
    pub persisted_sessions: DashMap<u64, PersistedSessionMeta>,  // name_hash(client_id) → session
}
```

#### Engine Handlers (engine.rs)

```rust
MqCommand::TAG_PERSIST_SESSION => {
    let cmd = cmd.as_persist_session();
    let client_hash = name_hash(cmd.client_id());
    self.metadata.persisted_sessions.insert(client_hash, PersistedSessionMeta {
        client_id: cmd.client_id().to_string(),
        consumer_id: cmd.consumer_id(),
        session_expiry_secs: cmd.session_expiry_secs(),
        disconnected_at_ms: current_time,
        subscription_data: cmd.subscription_data(),
    });
    MqResponse::Ok
}

MqCommand::TAG_RESTORE_SESSION => {
    let cmd = cmd.as_restore_session();
    let client_hash = name_hash(cmd.client_id());
    if let Some(session) = self.metadata.persisted_sessions.get(&client_hash) {
        // Check expiry
        if session.session_expiry_secs != 0xFFFFFFFF {
            let elapsed_ms = current_time.saturating_sub(session.disconnected_at_ms);
            let elapsed_secs = elapsed_ms / 1000;
            if elapsed_secs > session.session_expiry_secs as u64 {
                drop(session);
                self.metadata.persisted_sessions.remove(&client_hash);
                return MqResponse::SessionNotFound;
            }
        }
        MqResponse::SessionRestored {
            consumer_id: session.consumer_id,
            session_expiry_secs: session.session_expiry_secs,
            subscription_data: session.subscription_data.clone(),
        }
    } else {
        MqResponse::SessionNotFound
    }
}

MqCommand::TAG_EXPIRE_SESSIONS => {
    let now_ms = cmd.field_u64(1);
    self.metadata.persisted_sessions.retain(|_, session| {
        if session.session_expiry_secs == 0xFFFFFFFF { return true; }
        let elapsed_ms = now_ms.saturating_sub(session.disconnected_at_ms);
        let elapsed_secs = elapsed_ms / 1000;
        elapsed_secs <= session.session_expiry_secs as u64
    });
    MqResponse::Ok
}
```

#### MQTT Adapter Changes

**server.rs**:
```rust
// On disconnect (if should_persist):
let sub_data = session.serialize_subscriptions(); // encode to Bytes
batcher.submit(MqCommand::persist_session(
    session.session_id,
    &session.client_id,
    session.session_expiry_interval,
    &sub_data,
)).await;

// On connect (if !clean_session):
let resp = batcher.submit(MqCommand::restore_session(&connect.client_id)).await;
match resp {
    MqResponse::SessionRestored { consumer_id, subscription_data, .. } => {
        session.session_id = consumer_id;
        session.restore_subscriptions(&subscription_data);
        session_present = true;
    }
    MqResponse::SessionNotFound => {
        session_present = false;
    }
    _ => {}
}

// In accept loop (periodic cleanup):
batcher.submit(MqCommand::expire_sessions(now_ms())).await;
```

**session_store.rs** — Can be **removed entirely** once this optimization is complete.

### Files to Modify

| File | Change |
|---|---|
| `crates/mq/src/types.rs` | Add TAG_PERSIST_SESSION=64, TAG_RESTORE_SESSION=65, TAG_EXPIRE_SESSIONS=66; add `SessionRestored`/`SessionNotFound` response variants |
| `crates/mq/src/codec.rs` | Add constructors + view structs for 3 new commands |
| `crates/mq/src/engine.rs` | Add 3 match arms |
| `crates/mq/src/metadata.rs` | Add `PersistedSessionMeta` struct; add `persisted_sessions: DashMap<u64, PersistedSessionMeta>` |
| `crates/mq/src/manifest.rs` | Include persisted_sessions in MDBX snapshot serialization |
| `crates/mq-mqtt/src/server.rs` | Replace `InMemorySessionStore` usage with engine commands |
| `crates/mq-mqtt/src/session.rs` | Add `serialize_subscriptions()` / `restore_subscriptions(Bytes)` methods |
| `crates/mq-mqtt/src/session_store.rs` | Remove entirely |

### Impact

| Metric | Before | After |
|---|---|---|
| Session durability | In-memory only; lost on restart/failover | Raft-replicated + MDBX persisted |
| Session store module | Separate `session_store.rs` (561 lines) | Removed; built into engine |
| Expiry enforcement | `Instant`-based (non-deterministic across nodes) | Engine timestamp-based (deterministic) |
| Session restore consistency | May diverge across replicas | Raft-consistent across all replicas |
| Code in MQTT adapter | ~200 lines of session store logic | ~20 lines of command submission |

---

## Optimization 7: Batch Deliver / ACK

### Problem

Each delivery cycle issues **per-queue** deliver commands and **per-message** ACK
commands, each requiring a separate raft proposal:

```
For N subscriptions with M messages each:
  N × MqCommand::deliver()     = N raft proposals
  N × M × MqCommand::ack()     = N×M raft proposals
  Total: N + N×M raft proposals per delivery cycle
```

Example: 10 subscriptions × 10 messages = **110 raft proposals** per delivery cycle.

### Current Flow (server.rs:676-908)

```rust
for (queue_id, sub_info) in subscriptions {
    let resp = batcher.submit(MqCommand::deliver(queue_id, session_id, remaining)).await;
    if let MqResponse::Messages { messages } = resp {
        for delivered in messages {
            // ... process message ...
            batcher.submit(MqCommand::ack(queue_id, &[delivered.message_id], None)).await;
        }
    }
}
```

### Proposed Solution

#### A. Multi-Queue Deliver

Deliver from multiple queues in a single raft round-trip.

| Tag | Name | Purpose |
|---|---|---|
| 67 | `TAG_MULTI_DELIVER` | Deliver from multiple queues at once |
| 68 | `TAG_MULTI_ACK` | ACK messages across multiple queues |

**TAG_MULTI_DELIVER (67) Binary Layout:**
```
[tag:u8(67)][consumer_id:u64@1][queue_count:u32@9]
  [queue_id_1:u64][max_count_1:u32]
  [queue_id_2:u64][max_count_2:u32]
  ...
  [queue_id_N:u64][max_count_N:u32]
```

**TAG_MULTI_ACK (68) Binary Layout:**
```
[tag:u8(68)][queue_count:u32@1]
  [queue_id_1:u64][count_1:u32][msg_id_1a:u64][msg_id_1b:u64]...
  [queue_id_2:u64][count_2:u32][msg_id_2a:u64]...
  ...
```

#### New Response Variant

```rust
pub enum MqResponse {
    // ... existing ...
    /// Messages delivered from multiple queues in a single command.
    MultiMessages {
        queues: SmallVec<[(u64, SmallVec<[DeliveredMessage; 8]>); 4]>,
    },
}
```

#### B. Batch ACK Per-Queue (Adapter-Only Change)

The existing `MqCommand::ack(queue_id, message_ids, None)` **already** supports
multiple message_ids. The adapter currently calls it per-message but can batch:

```rust
// Before (per-message):
for msg in delivered {
    batcher.submit(MqCommand::ack(queue_id, &[msg.message_id], None)).await;
}

// After (batched per-queue):
let ack_ids: Vec<u64> = successful_delivers.iter().map(|m| m.message_id).collect();
if !ack_ids.is_empty() {
    batcher.submit(MqCommand::ack(queue_id, &ack_ids, None)).await;
}
```

This alone reduces N×M ack proposals to N (one per queue).

#### C. Multi-Queue ACK for Cross-Queue Batching

For the ultimate reduction (all acks in one proposal):

#### Constructor Methods (codec.rs)

```rust
impl MqCommand {
    pub fn multi_deliver(
        consumer_id: u64,
        queues: &[(u64, u32)],  // (queue_id, max_count) pairs
    ) -> Self { ... }

    pub fn multi_ack(
        queues: &[(u64, &[u64])],  // (queue_id, message_ids) pairs
    ) -> Self { ... }
}
```

#### Engine Handlers (engine.rs)

```rust
MqCommand::TAG_MULTI_DELIVER => {
    let cmd = cmd.as_multi_deliver();
    let consumer_id = cmd.consumer_id();
    let mut results = SmallVec::new();
    for (queue_id, max_count) in cmd.queues() {
        if let Some(queue) = self.metadata.queues.get(&queue_id) {
            let msgs = queue.apply_deliver(consumer_id, max_count, log_index, current_time);
            if !msgs.is_empty() {
                results.push((queue_id, msgs));
            }
        }
    }
    MqResponse::MultiMessages { queues: results }
}

MqCommand::TAG_MULTI_ACK => {
    let cmd = cmd.as_multi_ack();
    for (queue_id, message_ids) in cmd.queues() {
        if let Some(queue) = self.metadata.queues.get(&queue_id) {
            queue.apply_ack(&message_ids, None, log_index);
        }
    }
    MqResponse::Ok
}
```

#### MQTT Adapter Changes (server.rs)

```rust
// Collect all subscription queues:
let queue_specs: Vec<(u64, u32)> = session.subscriptions()
    .filter_map(|s| s.queue_id.map(|qid| (qid, remaining_per_queue)))
    .collect();

// Single multi-deliver:
let resp = batcher.submit(MqCommand::multi_deliver(session.session_id, &queue_specs)).await;

if let MqResponse::MultiMessages { queues } = resp {
    let mut all_acks: Vec<(u64, Vec<u64>)> = Vec::new();

    for (queue_id, messages) in queues {
        let mut ack_ids = Vec::new();
        for delivered in messages {
            // ... process message, encode PUBLISH ...
            ack_ids.push(delivered.message_id);
        }
        if !ack_ids.is_empty() {
            all_acks.push((queue_id, ack_ids));
        }
    }

    // Single multi-ack for all processed messages:
    if !all_acks.is_empty() {
        let ack_refs: Vec<(u64, &[u64])> = all_acks.iter()
            .map(|(qid, ids)| (*qid, ids.as_slice()))
            .collect();
        batcher.submit(MqCommand::multi_ack(&ack_refs)).await;
    }
}
```

### Files to Modify

| File | Change |
|---|---|
| `crates/mq/src/types.rs` | Add TAG_MULTI_DELIVER=67, TAG_MULTI_ACK=68; add `MqResponse::MultiMessages` |
| `crates/mq/src/codec.rs` | Add `multi_deliver()`, `multi_ack()` constructors; add view structs with iterator accessors |
| `crates/mq/src/engine.rs` | Add 2 match arms |
| `crates/mq-mqtt/src/server.rs` | Rewrite `deliver_outbound()` to use multi_deliver + multi_ack |

### Impact

| Metric | Before | After (batch per-queue) | After (multi commands) |
|---|---|---|---|
| Raft proposals: 10 subs × 10 msgs | 110 | 20 (10 deliver + 10 ack) | **2** (1 multi_deliver + 1 multi_ack) |
| Raft proposals: 1 sub × 100 msgs | 101 | 2 (1 deliver + 1 ack) | **2** |
| Engine lock contention | 110 apply_command calls | 20 calls | **2** calls |
| Latency per delivery cycle | 110 × raft RTT | 20 × raft RTT | **2** × raft RTT |

---

## Optimization 8: Publisher ID in FlatMessage Fixed Header

### Problem

The publisher session ID is stored as a string header in every MQTT message:
```
Key:   b"mqtt.publisher_session_id"  (25 bytes)
Value: u64 as 8-byte big-endian      (8 bytes)
Spans: 2 × 8 bytes                    (16 bytes)
Total: 49 bytes per message
```

Extraction requires O(n) iteration over all headers:
```rust
fn extract_publisher_session_id(flat: &FlatMessage) -> Option<u64> {
    for i in 0..flat.header_count() {
        let (k, v) = flat.header(i);
        if &k[..] == b"mqtt.publisher_session_id" && v.len() == 8 {
            return Some(u64::from_be_bytes(v[..8].try_into().unwrap()));
        }
    }
    None
}
```

### Proposed Solution

Extend the FlatMessage fixed header from 32 bytes to 40 bytes, adding a
`publisher_id: u64` field at offset 32-39.

#### New Wire Layout

```
[fixed header: 40 bytes]
  offset 0-1:   flags           (u16)
  offset 2-3:   header_count    (u16)
  offset 4-5:   span_count      (u16)
  offset 6-7:   value_len       (u16) — reserved
  offset 8-15:  timestamp       (u64)
  offset 16-23: ttl_ms          (u64)
  offset 24-31: delay_ms        (u64)
  offset 32-39: publisher_id    (u64)     ← NEW

[span index: span_count × 8 bytes]
[data region]
```

#### Flag for Publisher ID

Uses `FLAG_HAS_PUBLISHER_ID` (bit 9) defined in Optimization 5:
```rust
const FLAG_HAS_PUBLISHER_ID: u16 = 1 << 9;
```

When `FLAG_HAS_PUBLISHER_ID` is set, `publisher_id` at offset 32-39 is valid.
When not set, the field reads as 0 (no publisher attribution).

#### Backward Compatibility

**Critical**: Existing messages in the raft log have 32-byte headers. Strategy:

```rust
pub const HEADER_SIZE: usize = 40;           // NEW (was 32)
pub const HEADER_SIZE_LEGACY: usize = 32;    // For old message detection

impl FlatMessageMeta {
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() < HEADER_SIZE_LEGACY {
            return None;
        }
        let flags = u16::from_le_bytes([buf[0], buf[1]]);
        // ... parse existing fields ...

        // NEW: read publisher_id if buffer is large enough
        let publisher_id = if buf.len() >= HEADER_SIZE && flags & FLAG_HAS_PUBLISHER_ID != 0 {
            u64::from_le_bytes(buf[32..40].try_into().unwrap())
        } else {
            0
        };

        Some(Self {
            flags,
            header_count,
            span_count,
            timestamp,
            ttl_ms,
            delay_ms,
            publisher_id,
        })
    }
}
```

Old messages with 32-byte headers: `publisher_id()` returns 0 → adapter falls back
to header scan (if needed). New messages have 40-byte headers with the field set.

Over time, as old raft log segments are purged, all messages will have the new format.

#### FlatMessage Changes (flat.rs)

```rust
pub struct FlatMessageMeta {
    pub flags: u16,
    pub header_count: u16,
    pub span_count: u16,
    pub timestamp: u64,
    pub ttl_ms: u64,
    pub delay_ms: u64,
    pub publisher_id: u64,   // NEW
}

impl FlatMessage {
    /// Returns the publisher session/producer ID, or 0 if not set.
    #[inline]
    pub fn publisher_id(&self) -> u64 {
        self.meta.publisher_id
    }
}
```

#### FlatMessageBuilder Changes (flat.rs)

```rust
pub struct FlatMessageBuilder {
    // ... existing fields ...
    publisher_id: u64,   // NEW, default 0
}

impl FlatMessageBuilder {
    pub fn publisher_id(mut self, id: u64) -> Self {
        self.publisher_id = id;
        if id != 0 {
            self.flags |= FLAG_HAS_PUBLISHER_ID;
        }
        self
    }
}

// In build():
fn build(self) -> Bytes {
    // ... compute sizes ...
    let header_size = HEADER_SIZE;  // now 40

    let total_size = header_size + span_index_size + data_size;
    let mut buf = BytesMut::with_capacity(total_size);

    // Write fixed header
    buf.put_u16_le(self.flags);
    buf.put_u16_le(header_count);
    buf.put_u16_le(span_count);
    buf.put_u16_le(0); // reserved
    buf.put_u64_le(self.timestamp);
    buf.put_u64_le(self.ttl_ms);
    buf.put_u64_le(self.delay_ms);
    buf.put_u64_le(self.publisher_id);   // NEW field at offset 32

    // ... write span index and data ...
}
```

#### Span Index Offset Adjustment

The span index starts at `HEADER_SIZE` (now 40 instead of 32). All span offset
calculations are relative to the data region start, which is computed as:
```rust
let span_index_end = HEADER_SIZE + (meta.span_count as usize) * SPAN_SIZE;
```

This is already parameterized by `HEADER_SIZE`, so changing the constant is sufficient.

For **backward compatibility** during the transition, `FlatMessage::new()` must
detect the header size:

```rust
impl FlatMessage {
    pub fn new(buf: Bytes) -> Option<Self> {
        let meta = FlatMessageMeta::parse(&buf)?;
        // Determine actual header size based on publisher_id presence
        let header_size = if buf.len() >= HEADER_SIZE && meta.flags & FLAG_HAS_PUBLISHER_ID != 0 {
            HEADER_SIZE  // 40 bytes
        } else if buf.len() >= HEADER_SIZE_LEGACY {
            HEADER_SIZE_LEGACY  // 32 bytes (old format)
        } else {
            return None;
        };
        let span_index_end = header_size + (meta.span_count as usize) * SPAN_SIZE;
        if buf.len() < span_index_end { return None; }
        Some(Self { buf, meta, header_size })
    }
}
```

The `header_size` field is stored in `FlatMessage` to parameterize all span lookups.

#### MQTT Adapter Changes

**session.rs**:
```rust
// Before:
builder = builder.header(HDR_PUBLISHER_SESSION_ID.clone(), self.session_id_bytes.clone());
// After:
builder = builder.publisher_id(self.session_id);
```

**server.rs**:
```rust
// Before:
if let Some(pub_session_id) = extract_publisher_session_id(&flat) {
    if pub_session_id == session.session_id { /* skip no-local */ }
}
// After:
let pub_id = flat.publisher_id();
if pub_id != 0 && pub_id == session.session_id { /* skip no-local */ }
```

### Files to Modify

| File | Change |
|---|---|
| `crates/mq-protocol/src/flat.rs` | Extend HEADER_SIZE to 40; add publisher_id to FlatMessageMeta; add backward compat in parse/new; add publisher_id builder method; store header_size in FlatMessage for span offset calculations |
| `crates/mq-mqtt/src/session.rs` | Use `builder.publisher_id(session_id)` instead of header |
| `crates/mq-mqtt/src/server.rs` | Use `flat.publisher_id()` instead of `extract_publisher_session_id()`; remove `extract_publisher_session_id()` function |
| `crates/mq-amqp/src/codec.rs` | Update if AMQP uses publisher attribution headers |
| `crates/mq-kafka/src/codec.rs` | Update if Kafka uses publisher attribution headers |

### Impact

| Metric | Before | After |
|---|---|---|
| Per-message size (publisher ID) | 49 bytes (key+value+spans) | 8 bytes (fixed field) |
| Net savings per message | — | 41 bytes |
| Publisher ID extraction | O(n) header scan | O(1) fixed-offset read |
| No Local check | String compare + header iteration | Single u64 comparison |
| Fixed header size | 32 bytes | 40 bytes (+8 per message, all protocols) |
| Backward compat | N/A | Old 32-byte messages handled via buf.len() check |

---

## Implementation Order & Dependencies

### Phase Diagram

```
Phase 1 (Foundation — no engine command changes):
  ┌─────────────────────────────────────────────────┐
  │ Opt 5: Message-Level Flags in FlatMessage       │  ← standalone; enables Opt 8
  │ Opt 8: Publisher ID in Fixed Header             │  ← depends on Opt 5 flag bits
  └─────────────────────────────────────────────────┘
    Purely additive to FlatMessage. No engine commands or state changes.
    Immediate per-message overhead reduction.

Phase 2 (New engine commands — additive, can parallelize):
  ┌─────────────────────────────────────────────────┐
  │ Opt 2: Native Retained Message Store            │  ← 3 new tags (57-59)
  │ Opt 3: Native Will / Testament                  │  ← 2 new tags (60-61)
  │ Opt 4: Native QoS 2 State Machine              │  ← 2 new tags (62-63)
  └─────────────────────────────────────────────────┘
    Each adds new MqCommand tags and engine handlers.
    Can be implemented in parallel — no cross-dependencies.

Phase 3 (Session & batch — depends on Phase 2):
  ┌─────────────────────────────────────────────────┐
  │ Opt 6: Native Session Persistence               │  ← 3 new tags (64-66)
  │ Opt 7: Batch Deliver / ACK                      │  ← 2 new tags (67-68)
  └─────────────────────────────────────────────────┘
    Session persistence benefits from will (Opt 3) and QoS 2 (Opt 4)
    being in the engine. Batch deliver/ack is independent but best
    done after other engine changes stabilize.

Phase 4 (Notification infrastructure):
  ┌─────────────────────────────────────────────────┐
  │ Opt 1: Push-Based Queue Delivery                │  ← no new tags; local mechanism
  └─────────────────────────────────────────────────┘
    Most architecturally significant change. Modifies delivery loop
    fundamentals. Best done last when all other optimizations are stable.
```

### Dependency Graph

```
Opt 5 (flags) ──→ Opt 8 (publisher_id)
                       │
                       ▼
                  Opt 2 (retained)  ──┐
                  Opt 3 (will)     ──┤──→ Opt 6 (session persistence)
                  Opt 4 (QoS 2)   ──┘
                       │
                       ▼
                  Opt 7 (batch deliver/ack)
                       │
                       ▼
                  Opt 1 (push-based delivery)
```

### New Tag Allocation Summary

| Tag | Command | Optimization | Phase |
|---|---|---|---|
| 57 | `TAG_SET_RETAINED` | Opt 2: Native Retained Store | 2 |
| 58 | `TAG_DELETE_RETAINED` | Opt 2: Native Retained Store | 2 |
| 59 | `TAG_GET_RETAINED` | Opt 2: Native Retained Store | 2 |
| 60 | `TAG_SET_WILL` | Opt 3: Native Will/Testament | 2 |
| 61 | `TAG_CLEAR_WILL` | Opt 3: Native Will/Testament | 2 |
| 62 | `TAG_MARK_RECEIVED` | Opt 4: QoS 2 State Machine | 2 |
| 63 | `TAG_MARK_RELEASED` | Opt 4: QoS 2 State Machine | 2 |
| 64 | `TAG_PERSIST_SESSION` | Opt 6: Session Persistence | 3 |
| 65 | `TAG_RESTORE_SESSION` | Opt 6: Session Persistence | 3 |
| 66 | `TAG_EXPIRE_SESSIONS` | Opt 6: Session Persistence | 3 |
| 67 | `TAG_MULTI_DELIVER` | Opt 7: Batch Deliver/ACK | 3 |
| 68 | `TAG_MULTI_ACK` | Opt 7: Batch Deliver/ACK | 3 |

### Files Modified Per Phase

**Phase 1** (2 files core, 2 files adapter):
- `crates/mq-protocol/src/flat.rs`
- `crates/mq-mqtt/src/session.rs`
- `crates/mq-mqtt/src/server.rs`

**Phase 2** (5 files core, 2 files adapter):
- `crates/mq/src/types.rs`
- `crates/mq/src/codec.rs`
- `crates/mq/src/engine.rs`
- `crates/mq/src/exchange.rs` (retained)
- `crates/mq/src/consumer.rs` (will)
- `crates/mq/src/queue.rs` (QoS 2)
- `crates/mq-mqtt/src/session.rs`
- `crates/mq-mqtt/src/server.rs`

**Phase 3** (4 files core, 3 files adapter):
- `crates/mq/src/types.rs`
- `crates/mq/src/codec.rs`
- `crates/mq/src/engine.rs`
- `crates/mq/src/metadata.rs` (session persistence)
- `crates/mq/src/manifest.rs` (MDBX persistence)
- `crates/mq-mqtt/src/server.rs`
- `crates/mq-mqtt/src/session.rs`
- `crates/mq-mqtt/src/session_store.rs` (remove)

**Phase 4** (3 files core, 1 file adapter):
- New: `crates/mq/src/notifier.rs`
- `crates/mq/src/lib.rs`
- `crates/mq/src/engine.rs`
- `crates/mq/src/metadata.rs`
- `crates/mq-mqtt/src/server.rs`

---

## Test Coverage Plan

### Phase 1 Tests: FlatMessage Changes (Opts 5 & 8)

#### T1.1: Flag Bit Encoding/Decoding
```
test_flag_retain_roundtrip
  Build FlatMessage with .retain(true) → assert is_retain() == true
  Build FlatMessage without .retain() → assert is_retain() == false

test_flag_no_local_roundtrip
  Build FlatMessage with .no_local(true) → assert is_no_local() == true

test_flag_utf8_payload_roundtrip
  Build FlatMessage with .utf8_payload(true) → assert is_utf8_payload() == true

test_all_new_flags_combined
  Build with retain + no_local + utf8_payload → verify all three read back true

test_new_flags_dont_affect_existing
  Build with retain(true) + key + routing_key + ttl + delay → verify all existing
  accessors still work correctly (value, key, routing_key, ttl_ms, delay_ms, headers)

test_flags_backward_compat
  Parse a buffer with flags=0 → all new flag checks return false
```

#### T1.2: Publisher ID in Fixed Header
```
test_publisher_id_roundtrip
  Build with .publisher_id(12345) → assert publisher_id() == 12345

test_publisher_id_zero_default
  Build without .publisher_id() → assert publisher_id() == 0

test_publisher_id_backward_compat_32byte
  Create a 32-byte header buffer manually → parse → publisher_id() == 0

test_publisher_id_with_all_fields
  Build with publisher_id + key + routing_key + reply_to + correlation_id + headers
  → verify all fields read correctly (span offsets shifted to 40-byte header)

test_publisher_id_large_value
  Build with .publisher_id(u64::MAX) → assert publisher_id() == u64::MAX
```

#### T1.3: Combined Flags + Publisher ID
```
test_combined_retain_publisher_full_message
  Build with retain(true) + publisher_id(999) + routing_key("a/b") + value(b"hello")
  + 3 headers → verify all fields: is_retain, publisher_id, routing_key, value, headers

test_zero_copy_preserved
  Build message → read value() → verify Bytes is a slice (same allocation)
```

### Phase 2 Tests: Engine Commands (Opts 2, 3, 4)

#### T2.1: Retained Messages (Opt 2)
```
test_set_retained_basic
  Create exchange → set_retained("sensors/temp", msg) → Ok

test_get_retained_exact
  set_retained("sensors/temp", msg) → get_retained("sensors/temp") → returns msg

test_get_retained_wildcard_plus
  set_retained("sensors/temp/room1", m1) + set_retained("sensors/humidity/room1", m2)
  → get_retained("sensors/+/room1") → returns both

test_get_retained_wildcard_hash
  set_retained("sensors/temp", m1) + set_retained("sensors/temp/room1", m2)
  → get_retained("sensors/#") → returns both

test_get_retained_wildcard_all
  set_retained 3 different topics → get_retained("#") → returns all 3

test_update_retained_replaces
  set_retained("t", m1) → set_retained("t", m2) → get → returns m2 only

test_delete_retained
  set_retained("t", m1) → delete_retained("t") → get → empty

test_delete_retained_nonexistent
  delete_retained("nonexistent") → Ok (idempotent)

test_retained_nonexistent_exchange
  set_retained(999, "t", msg) → Error(NotFound)
  get_retained(999, "#") → Error(NotFound)

test_retained_survives_snapshot
  set_retained("t", msg) → snapshot → new engine from snapshot → get → returns msg

test_retained_many_topics
  set_retained for 100 different routing keys → get_retained("#") → returns all 100

test_get_retained_no_match
  set_retained("sensors/temp") → get_retained("actuators/+") → empty

test_get_retained_exact_vs_wildcard
  set_retained("a/b") → get_retained("a/b") uses O(1) path
  get_retained("a/+") uses scan path → both return same result
```

#### T2.2: Will Messages (Opt 3)
```
test_set_will_basic
  register_consumer → set_will(consumer_id, exchange_id, ...) → Ok

test_clear_will_basic
  set_will → clear_will(consumer_id) → Ok

test_will_on_disconnect
  create_exchange → register_consumer → set_will → disconnect_consumer
  → verify will message was routed through exchange

test_clean_disconnect_no_will
  set_will → clear_will → disconnect_consumer → no will published

test_will_nonexistent_consumer
  set_will(999, ...) → Error(NotFound)

test_will_with_delay
  set_will(delay=5) → disconnect_consumer → WillPending response with delay_secs=5

test_will_with_retain
  set_will(retain=true) → disconnect_consumer → verify retained message set

test_will_update_replaces
  set_will(msg1) → set_will(msg2) → disconnect → verify msg2 published (not msg1)

test_will_survives_snapshot
  set_will → snapshot → restore → disconnect → will published

test_will_qos_preserved
  set_will(qos=2) → disconnect → verify will published with correct qos
```

#### T2.3: QoS 2 State Machine (Opt 4)
```
test_mark_received_basic
  create_queue → enqueue → deliver → mark_received → verify state == Received

test_mark_released_basic
  mark_received → mark_released → verify state == Released

test_ack_after_released
  mark_released → ack → verify state == Acked (message removed)

test_ack_after_received
  mark_received → ack → verify state == Acked (skip Released step)

test_mark_received_invalid_state_pending
  enqueue (Pending) → mark_received → no state change (still Pending)

test_mark_released_invalid_state_inflight
  deliver (InFlight) → mark_released → no state change (still InFlight)

test_received_no_visibility_timeout
  deliver → mark_received → timeout_expired → message NOT redelivered (still Received)

test_inflight_visibility_timeout
  deliver (InFlight) → timeout_expired → message redelivered (back to Pending)

test_qos2_state_survives_snapshot
  deliver → mark_received → snapshot → restore → verify state == Received

test_batch_mark_received
  deliver 5 messages → mark_received([id1, id2, id3, id4, id5]) → all Received

test_batch_mark_released
  mark_received 5 → mark_released([id1, id2, id3, id4, id5]) → all Released

test_nack_only_works_on_inflight
  deliver → mark_received → nack → no state change (can't nack Received)
  deliver → nack → back to Pending (InFlight can be nacked)

test_mark_received_counts
  deliver → mark_received → verify in_flight_count decremented
  (Received is not counted as in-flight)
```

### Phase 3 Tests: Session & Batch (Opts 6, 7)

#### T3.1: Session Persistence (Opt 6)
```
test_persist_session_basic
  persist_session(consumer_id=1, "client1", expiry=3600, sub_data) → Ok

test_restore_session_basic
  persist_session → restore_session("client1") → SessionRestored with matching data

test_restore_session_expired
  persist_session(expiry=1) → apply restore at time +2000ms → SessionNotFound

test_restore_session_never_expire
  persist_session(expiry=0xFFFFFFFF) → apply restore at time +999999ms → SessionRestored

test_restore_session_nonexistent
  restore_session("unknown") → SessionNotFound

test_expire_sessions_cleanup
  persist 3 sessions with expiry=1, 10, 0xFFFFFFFF
  → expire_sessions(now+5000ms)
  → restore: session1=NotFound, session2=Restored, session3=Restored

test_persist_session_overwrite
  persist_session("client1", data1) → persist_session("client1", data2)
  → restore → data2 returned

test_session_survives_snapshot
  persist_session → snapshot → restore engine → restore_session → SessionRestored

test_expire_sessions_empty
  expire_sessions with no sessions → Ok (no-op)

test_subscription_data_opaque
  persist_session with arbitrary binary subscription_data
  → restore → exact same bytes returned
```

#### T3.2: Batch Deliver/ACK (Opt 7)
```
test_multi_deliver_basic
  create 3 queues, enqueue 5 msgs each
  → multi_deliver(consumer, [(q1,5),(q2,5),(q3,5)])
  → MultiMessages with 5 msgs per queue

test_multi_deliver_empty_queues
  create 3 empty queues → multi_deliver → MultiMessages with empty results

test_multi_deliver_partial
  1 queue with msgs, 2 empty → multi_deliver → results for 1 queue only

test_multi_deliver_max_count
  enqueue 100 per queue, max_count=5 → multi_deliver → 5 per queue

test_multi_deliver_nonexistent_queue
  multi_deliver with one invalid queue_id → other queues still deliver

test_multi_ack_basic
  deliver from 3 queues → multi_ack([(q1,[ids]),(q2,[ids]),(q3,[ids])])
  → all acked

test_multi_ack_idempotent
  multi_ack → multi_ack same ids → Ok (no error)

test_multi_ack_partial_valid
  multi_ack with one invalid queue_id → other queues still acked

test_batch_ack_per_queue
  deliver 10 msgs from 1 queue → ack(queue_id, [all 10 ids]) → all acked
  (tests existing ack batching behavior)

test_multi_deliver_respects_inflight
  deliver all from q1 → multi_deliver(q1, 5) → 0 messages (all in-flight)
```

### Phase 4 Tests: Push-Based Delivery (Opt 1)

#### T4.1: QueueNotifier
```
test_watch_notify_basic
  notifier.watch(queue_id=1) → notifier.notify(1) → receiver gets message

test_multiple_watchers
  3 × watch(queue_id=1) → notify(1) → all 3 receivers fire

test_unwatch
  watch(1) → unwatch(1) → notify(1) → no notification

test_dead_sender_cleanup
  watch(1) → drop receiver → notify(1) → sender removed from watchers

test_notify_no_watchers
  notify(999) → no panic, no-op

test_cross_queue_isolation
  watch(1) → notify(2) → receiver for queue 1 does NOT fire

test_multiple_notify_coalescing
  watch(1) → notify(1) × 5 → receiver has 5 messages (unbounded, not coalesced)
  (adapter should drain and process once)

test_watch_after_notify
  notify(1) → watch(1) → no retroactive notification (only future)
```

### Integration Tests

#### TI.1: MQTT End-to-End with Engine Optimizations
```
test_retained_via_engine_publish_subscribe
  Create exchange → publish with retain flag using set_retained
  → subscribe → get_retained → deliver retained to subscriber

test_will_via_engine_connect_disconnect
  register_consumer → set_will → unclean disconnect
  → verify will message published through exchange

test_qos2_full_handshake_via_engine
  enqueue → deliver → mark_received → mark_released → ack
  → verify complete QoS 2 lifecycle

test_session_restore_full_flow
  register → subscribe → persist_session → disconnect
  → reconnect → restore_session → subscriptions restored

test_no_local_via_flags
  build FlatMessage with publisher_id(session_id)
  → deliver to same session → verify skipped (no header scan needed)

test_batch_delivery_cycle
  10 queues with 10 msgs each → multi_deliver → process all
  → multi_ack all → verify all acked in 2 raft ops
```

#### TI.2: Crash Recovery
```
test_qos2_recovery_after_crash
  enqueue → deliver → mark_received → "crash" (snapshot + restore)
  → verify message still in Received state → complete handshake

test_will_recovery_after_crash
  set_will → "crash" (snapshot + restore) → disconnect
  → verify will published

test_session_recovery_after_crash
  persist_session → "crash" (snapshot + restore) → restore_session
  → verify session data intact

test_retained_recovery_after_crash
  set_retained → "crash" (snapshot + restore) → get_retained
  → verify retained message intact
```

---

## Tracking

### Status Key
- [ ] Not started
- [~] In progress
- [x] Complete

### Phase 1: FlatMessage Foundation
- [x] Opt 5: Add FLAG_RETAIN, FLAG_NO_LOCAL, FLAG_UTF8_PAYLOAD to flat.rs
- [x] Opt 5: Add builder methods (retain, no_local, utf8_payload)
- [x] Opt 5: Add accessor methods (is_retain, is_no_local, is_utf8_payload)
- [x] Opt 5: Update MQTT adapter to use flags instead of headers
- [x] Opt 5: Tests T1.1 (flag encoding/decoding) — 31 tests pass
- [x] Opt 8: Extend FlatMessage header to 40 bytes with publisher_id
- [x] Opt 8: Add FLAG_HAS_PUBLISHER_ID flag bit
- [x] Opt 8: Add backward compatibility guard (32-byte → 40-byte)
- [x] Opt 8: Update FlatMessageMeta with publisher_id field
- [x] Opt 8: Update MQTT adapter to use publisher_id field
- [x] Opt 8: Tests T1.2 (publisher_id encoding/decoding) — included in 31 tests
- [x] Opt 8: Tests T1.3 (combined flags + publisher_id) — included in 31 tests

### Phase 2: Engine Commands
- [x] Opt 2: Add TAG_SET_RETAINED (57), TAG_DELETE_RETAINED (58), TAG_GET_RETAINED (59)
- [x] Opt 2: Add RetainedMessage struct to ExchangeState
- [x] Opt 2: Add constructors + view structs in codec.rs
- [x] Opt 2: Add engine handlers for all 3 tags
- [x] Opt 2: Add RetainedMessages response variant
- [x] Opt 2: Add MDBX persistence for retained messages
- [x] Opt 2: Update MQTT adapter to use native retained
- [x] Opt 2: Tests T2.1 (retained messages — 13 test cases)
- [x] Opt 3: Add TAG_SET_WILL (60), TAG_CLEAR_WILL (61)
- [x] Opt 3: Add WillMessage struct to ConsumerMeta
- [x] Opt 3: Add constructors + view structs in codec.rs
- [x] Opt 3: Add engine handlers; modify disconnect_consumer
- [x] Opt 3: Update MQTT adapter to use native will
- [x] Opt 3: Tests T2.2 (will messages — 10 test cases)
- [x] Opt 4: Add TAG_MARK_RECEIVED (62), TAG_MARK_RELEASED (63)
- [x] Opt 4: Add Received/Released to MessageState enum
- [x] Opt 4: Add apply_mark_received/apply_mark_released to QueueState
- [x] Opt 4: Add constructors + view structs in codec.rs
- [x] Opt 4: Add engine handlers
- [x] Opt 4: Update MQTT adapter to use engine QoS 2 state
- [x] Opt 4: Tests T2.3 (QoS 2 state machine — 13 test cases)

### Phase 3: Session & Batch
- [x] Opt 6: Add TAG_PERSIST_SESSION (64), TAG_RESTORE_SESSION (65), TAG_EXPIRE_SESSIONS (66)
- [x] Opt 6: Add PersistedSessionMeta to metadata
- [x] Opt 6: Add constructors + view structs in codec.rs
- [x] Opt 6: Add engine handlers
- [x] Opt 6: Add MDBX persistence for sessions
- [x] Opt 6: Update MQTT adapter; remove session_store.rs
- [x] Opt 6: Tests T3.1 (session persistence — 9 test cases)
- [x] Opt 7: Add TAG_MULTI_DELIVER (67), TAG_MULTI_ACK (68)
- [x] Opt 7: Add MultiMessages response variant
- [x] Opt 7: Add constructors + view structs in codec.rs
- [x] Opt 7: Add engine handlers
- [x] Opt 7: Update MQTT delivery loop to use batch commands
- [x] Opt 7: Tests T3.2 (batch deliver/ack — 10 test cases)

### Phase 4: Push-Based Delivery
- [x] Opt 1: Create notifier.rs with QueueNotifier
- [x] Opt 1: Integrate QueueNotifier into MqMetadata
- [x] Opt 1: Call notify() in engine after enqueue/exchange routing
- [x] Opt 1: Replace MQTT polling loop with notification-driven select
- [x] Opt 1: Add fallback heartbeat poll (500ms safety net)
- [x] Opt 1: Tests T4.1 (QueueNotifier — 10 test cases)

### Integration & Validation
- [x] Integration tests TI.1 (end-to-end with optimizations — 6 test cases)
- [x] Integration tests TI.2 (crash recovery — 4 test cases)
- [ ] Benchmark: delivery latency before/after Opt 1
- [ ] Benchmark: per-message overhead before/after Opts 5+8
- [ ] Benchmark: raft proposals per cycle before/after Opt 7

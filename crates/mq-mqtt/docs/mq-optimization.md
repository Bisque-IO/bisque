# bisque-mq MQTT Optimization Plan — Phase 3

> **Goal**: Eliminate remaining adapter-layer overhead by migrating to native engine
> primitives, wiring push-based delivery, and removing per-message header scans
> from the hot path.

Building on the 16 optimizations completed in Phase 1 (`mq-optimizations.md`) and
Phase 2 (`mq-optimizations-2.md`), this plan addresses the final performance gaps
between the `bisque-mq` engine and the `bisque-mq-mqtt` adapter.

## Table of Contents

1. [Analysis: Remaining Overhead](#analysis-remaining-overhead)
2. [Optimization 1: Push-Based Delivery Wiring](#optimization-1-push-based-delivery-wiring)
3. [Optimization 2: Publisher ID Migration to FlatMessage Fixed Header](#optimization-2-publisher-id-migration-to-flatmessage-fixed-header)
4. [Optimization 3: Retain Flag Migration to FlatMessage FLAG_RETAIN](#optimization-3-retain-flag-migration-to-flatmessage-flag_retain)
5. [Optimization 4: Engine-Level No-Local Filtering](#optimization-4-engine-level-no-local-filtering)
6. [Optimization 5: Engine-Level Message Expiry Filtering](#optimization-5-engine-level-message-expiry-filtering)
7. [Optimization 6: Engine-Level $-Topic Filtering](#optimization-6-engine-level-system-topic-filtering)
8. [Optimization 7: Batch Subscribe](#optimization-7-batch-subscribe)
9. [Optimization 8: Zero-Copy Retained Message Delivery](#optimization-8-zero-copy-retained-message-delivery)
10. [Implementation Order & Dependencies](#implementation-order--dependencies)
11. [Test Coverage Plan](#test-coverage-plan)
12. [Tracking](#tracking)

---

## Analysis: Remaining Overhead

### Current Hot-Path Bottlenecks

After Phase 1+2, the MQTT adapter still carries significant per-message work:

| Overhead | Location | Cost | Root Cause |
|----------|----------|------|------------|
| **50ms polling interval** | `server.rs:1355-1462` | 0-50ms latency per message | `GroupNotifier` exists but adapter not wired to it |
| **Header scan: publisher_session_id** | `server.rs:765-774` | O(n) headers per message | Uses `mqtt.publisher_session_id` header instead of `FlatMessage.publisher_id` |
| **Header scan: original_retain** | `server.rs:778-786` | O(n) headers per message | Uses `mqtt.original_retain` header instead of `FlatMessage.is_retain()` |
| **No-local filtering** | `server.rs:879-893` | Per-message header scan + ACK | Adapter scans headers; engine could filter at deliver time |
| **Message expiry filtering** | `server.rs:898-921` | Per-message TTL check + ACK | Adapter filters post-deliver; engine could skip expired |
| **$-topic filtering** | `server.rs:863-876` | Per-message routing_key check | Adapter checks at deliver time; engine could filter at routing time |
| **Per-filter subscribe** | `server.rs:696-749` | N Raft proposals per SUBSCRIBE | Each filter creates queue+binding separately |
| **Per-message header bytes** | `session.rs:1394-1402` | ~65 bytes/msg wasted | `publisher_session_id` (33B) + `original_retain` (21B) stored as headers |

### Quantified Impact

For a typical MQTT workload (1000 msg/sec, 10 subscriptions per client):
- **Polling latency**: 25ms average added per message (50ms/2)
- **Header overhead**: 65 bytes × 1000 msg/sec = 65 KB/sec wasted bandwidth
- **Header scans**: 2 × O(n) scans × 1000 msg/sec per subscription = 20,000 header iterations/sec
- **Subscribe overhead**: 10 filters × 2 commands = 20 Raft proposals per SUBSCRIBE

---

## Optimization 1: Push-Based Delivery Wiring

### Problem

The `GroupNotifier` module was created in Phase 1 and the engine calls
`group_notifier.notify(group_id)` after every enqueue/exchange routing/nack
(7 call sites in `engine.rs`). However, the MQTT adapter still uses
`tokio::time::interval(50ms)` for delivery polling (`server.rs:1355-1462`).

This creates a **0-50ms latency floor** on every message and wastes CPU
polling idle queues.

### Current Flow

```
server.rs:1355: delivery_interval = tokio::time::interval(50ms)
server.rs:1462: _ = delivery_interval.tick() => deliver_outbound()
```

### Proposed Solution

Pass `Arc<GroupNotifier>` (from `MqMetadata`) to `MqttServer` and wire it
into the connection loop.

#### MqttServer Changes (server.rs)

```rust
pub struct MqttServer {
    // ... existing fields ...
    group_notifier: Arc<GroupNotifier>,
}
```

#### Connection Loop Changes (server.rs)

Replace polling interval with notification-driven delivery:

```rust
// On subscribe: register watcher for each subscription queue's group_id
let notify_rx = group_notifier.watch(group_id);

// In connection_loop select!:
_ = notify_rx.recv() => deliver_outbound(...)
// Keep 500ms fallback heartbeat for safety
_ = fallback_interval.tick() => deliver_outbound(...)
```

#### Aggregated Notification Channel

Since a session may have many subscriptions, use a single aggregated
notification channel per connection. On subscribe, register per-queue
watchers that forward to a single `mpsc::UnboundedSender<()>`:

```rust
struct DeliveryNotifier {
    tx: mpsc::UnboundedSender<()>,
    rx: mpsc::UnboundedReceiver<()>,
    watchers: Vec<tokio::task::JoinHandle<()>>,
}
```

Each watcher spawns a lightweight forwarder task:
```rust
tokio::spawn(async move {
    while let Some(()) = queue_rx.recv().await {
        let _ = tx.send(());
    }
});
```

### Files to Modify

| File | Change |
|------|--------|
| `crates/mq-mqtt/src/server.rs` | Accept `Arc<GroupNotifier>`, wire into connection loop |
| `crates/mq-mqtt/src/session.rs` | Track group_ids for notification registration |

### Impact

- **Latency**: 50ms average → sub-millisecond (push-based)
- **CPU**: Eliminates 20 poll cycles/sec per connection
- **No engine changes**: Uses existing `GroupNotifier` infrastructure

---

## Optimization 2: Publisher ID Migration to FlatMessage Fixed Header

### Problem

The adapter stores the publisher's session ID as a header:
```rust
// session.rs:1394-1397
builder = builder.header(
    HDR_PUBLISHER_SESSION_ID.clone(),  // b"mqtt.publisher_session_id" (25 bytes)
    self.session_id_bytes.clone(),     // 8 bytes
);
```

This adds **33 bytes per message** and requires an **O(n) header scan** on every
delivery to extract it (`server.rs:765-774`).

Phase 1 Opt 8 added `publisher_id: u64` to the FlatMessage fixed header (40 bytes),
but the adapter was never migrated to use it.

### Proposed Solution

#### Publish Path (session.rs)

Replace header with native field:
```rust
// Before:
builder = builder.header(HDR_PUBLISHER_SESSION_ID.clone(), self.session_id_bytes.clone());

// After:
builder = builder.publisher_id(self.session_id);
```

#### Delivery Path (server.rs)

Replace header scan with direct field access:
```rust
// Before: extract_publisher_session_id() — O(n) header scan
// After:
let pub_id = flat.publisher_id();  // O(1) fixed-offset read
```

#### Cleanup

Remove `HDR_PUBLISHER_SESSION_ID` static, `session_id_bytes` field, and
`extract_publisher_session_id()` function.

### Files to Modify

| File | Change |
|------|--------|
| `crates/mq-mqtt/src/session.rs` | Use `.publisher_id(session_id)` instead of header |
| `crates/mq-mqtt/src/server.rs` | Use `flat.publisher_id()` instead of header scan |

### Impact

- **Bandwidth**: -33 bytes per message
- **CPU**: O(1) instead of O(n) header scan per delivery
- **No engine changes**: Uses existing FlatMessage field

---

## Optimization 3: Retain Flag Migration to FlatMessage FLAG_RETAIN

### Problem

The adapter stores the original retain flag as a header:
```rust
// session.rs:1400-1402
if publish.retain {
    builder = builder.header(HDR_ORIGINAL_RETAIN.clone(), Bytes::from_static(&[1]));
}
```

This adds **~21 bytes per retained message** and requires an **O(n) header scan**
on every delivery (`server.rs:778-786`).

Phase 1 Opt 5 added `FLAG_RETAIN` to FlatMessage flags, but the adapter was
never migrated.

### Proposed Solution

#### Publish Path (session.rs)

```rust
// Before:
if publish.retain {
    builder = builder.header(HDR_ORIGINAL_RETAIN.clone(), Bytes::from_static(&[1]));
}

// After:
builder = builder.retain(publish.retain);
```

#### Delivery Path (server.rs)

```rust
// Before: extract_original_retain() — O(n) header scan
// After:
let retain_flag = flat.is_retain();  // O(1) flag bit check
```

#### Cleanup

Remove `HDR_ORIGINAL_RETAIN` static and `extract_original_retain()` function.

### Files to Modify

| File | Change |
|------|--------|
| `crates/mq-mqtt/src/session.rs` | Use `.retain(publish.retain)` instead of header |
| `crates/mq-mqtt/src/server.rs` | Use `flat.is_retain()` instead of header scan |

### Impact

- **Bandwidth**: -21 bytes per retained message
- **CPU**: O(1) flag check instead of O(n) header scan
- **No engine changes**: Uses existing FlatMessage flag

---

## Optimization 4: Engine-Level No-Local Filtering

### Problem

No-local filtering (MQTT 5.0 §3.8.3.1) prevents messages published by a client
from being delivered back to the same client. Currently handled at adapter level:

```rust
// server.rs:879-893
if no_local {
    if let Some(pub_session_id) = extract_publisher_session_id(&flat) {
        if pub_session_id == my_session_id {
            batcher.submit(MqCommand::group_ack(queue_id, &[delivered.message_id], None)).await;
            continue;
        }
    }
}
```

This has three costs:
1. Engine delivers the message (wasted I/O)
2. Adapter scans headers (CPU)
3. Adapter sends ACK back (another Raft proposal)

### Proposed Solution

Push no-local filtering into the engine's GROUP_DELIVER handler. When delivering
messages from a consumer group, check if the message's `publisher_id` matches
the requesting consumer's session_id.

#### Engine Changes (engine.rs / consumer_group.rs)

In `AckVariantState::apply_deliver()`, add an optional `exclude_publisher_id: u64`
parameter. When non-zero, skip messages whose publisher_id matches:

```rust
pub fn apply_deliver(
    &self,
    consumer_id: u64,
    max_count: u32,
    current_time: u64,
    exclude_publisher_id: u64,  // NEW: 0 = disabled
) -> SmallVec<[u64; 8]> {
    // ... for each pending message:
    if exclude_publisher_id != 0 {
        // Read publisher_id from the message's flat header
        if msg.publisher_id == exclude_publisher_id {
            self.apply_ack_single(msg_id);  // Auto-ACK at engine level
            continue;
        }
    }
}
```

#### MqCommand Changes

Extend `TAG_GROUP_DELIVER` to include an optional `exclude_publisher_id` field:
```
TAG_GROUP_DELIVER: [19][group_id:8][consumer_id:8][max_count:4][exclude_publisher_id:8]
```

The field is backward-compatible: if the fixed region is shorter (old format),
`exclude_publisher_id` defaults to 0 (disabled).

### Files to Modify

| File | Change |
|------|--------|
| `crates/mq/src/codec.rs` | Add `exclude_publisher_id` to `group_deliver()` constructor |
| `crates/mq/src/engine.rs` | Pass exclude_publisher_id to deliver handler |
| `crates/mq/src/consumer_group.rs` | Filter messages by publisher_id in deliver |
| `crates/mq-mqtt/src/server.rs` | Pass session_id as exclude_publisher_id when no_local=true |

### Impact

- **Raft proposals**: Eliminates per-message ACK for no-local filtered messages
- **CPU**: No header scan; single u64 compare at engine level
- **I/O**: Filtered messages never leave the engine

---

## Optimization 5: Engine-Level Message Expiry Filtering

### Problem

Message expiry (MQTT 5.0 §3.3.2.3.3) is checked at adapter level after delivery:

```rust
// server.rs:898-921
if let Some(ttl_ms) = flat.ttl_ms() {
    let elapsed_ms = now_ms.saturating_sub(ts);
    if elapsed_ms >= ttl_ms {
        batcher.submit(MqCommand::group_ack(queue_id, &[delivered.message_id], None)).await;
        continue;
    }
}
```

Expired messages are delivered by the engine, read from the log, then ACKed back.

### Proposed Solution

Pass `current_time_ms` to `GROUP_DELIVER` and filter expired messages at the engine
level during `apply_deliver()`.

#### Engine Changes (consumer_group.rs)

In `apply_deliver()`, check TTL before adding to the delivery batch:

```rust
if current_time > 0 {
    if let Some(ttl_ms) = msg_meta.ttl_ms {
        if current_time.saturating_sub(msg_meta.timestamp) >= ttl_ms {
            self.apply_ack_single(msg_id);  // Auto-ACK expired
            continue;
        }
    }
}
```

#### MqCommand Changes

`TAG_GROUP_DELIVER` already has room in the fixed region. Add `current_time_ms: u64`:
```
TAG_GROUP_DELIVER: [19][group_id:8][consumer_id:8][max_count:4][exclude_pub:8][current_time:8]
```

### Files to Modify

| File | Change |
|------|--------|
| `crates/mq/src/codec.rs` | Add `current_time_ms` to `group_deliver()` |
| `crates/mq/src/engine.rs` | Pass current_time to deliver handler |
| `crates/mq/src/consumer_group.rs` | Filter expired messages in deliver |
| `crates/mq-mqtt/src/server.rs` | Pass current time; remove adapter-level expiry check |

### Impact

- **Raft proposals**: Eliminates per-message ACK for expired messages
- **I/O**: Expired messages never read from log
- **Correctness**: Expiry enforced consistently across all adapters

---

## Optimization 6: Engine-Level $-Topic Filtering

### Problem

MQTT §4.7.2 requires that topics starting with `$` are not delivered to
subscriptions whose filter starts with `+` or `#`. Currently checked per-message:

```rust
// server.rs:863-876
if filter_starts_with_wildcard {
    if let Some(topic) = flat.routing_key() {
        if topic.first() == Some(&b'$') {
            batcher.submit(MqCommand::group_ack(...)).await;
            continue;
        }
    }
}
```

### Proposed Solution

Filter $-topics at **exchange routing time** rather than delivery time.

#### Exchange Changes (engine.rs)

In `TAG_PUBLISH_TO_EXCHANGE` routing, when matching bindings:

```rust
// Skip $-topic messages for bindings whose routing_key pattern starts with wildcard
if routing_key.starts_with("$") && binding.routing_key_starts_with_wildcard {
    continue;  // Don't enqueue into this binding's queue
}
```

#### Binding Metadata

Add `routing_key_starts_with_wildcard: bool` to the binding. Set during
`TAG_CREATE_BINDING` when the routing key starts with `*` or `#`.

### Files to Modify

| File | Change |
|------|--------|
| `crates/mq/src/engine.rs` | Skip $-topics for wildcard bindings during routing |
| `crates/mq/src/codec.rs` | Store wildcard flag on binding |
| `crates/mq-mqtt/src/server.rs` | Remove adapter-level $-topic check |

### Impact

- **Raft proposals**: No ACKs for $-topic mismatches
- **Queue space**: $-topic messages never enqueued to wildcard subscriptions
- **CPU**: Check once at routing vs. once per delivery per subscription

---

## Optimization 7: Batch Subscribe

### Problem

Each SUBSCRIBE filter requires 2 Raft proposals (create queue + create binding).
A SUBSCRIBE with 10 filters produces **20 Raft proposals** serialized sequentially.

### Proposed Solution

Use the existing `TAG_BATCH` mechanism to combine all queue and binding creates
into a single Raft proposal.

#### Adapter Changes (server.rs)

```rust
async fn orchestrate_subscribe_batched(
    session: &mut MqttSession,
    batcher: &MqWriteBatcher,
    filters: SmallVec<[SubscribeFilterPlan; 4]>,
) -> Result<(), ConnectionError> {
    let mut commands = SmallVec::<[MqCommand; 16]>::new();

    for filter in &filters {
        if session.cached_queue_id(&filter.queue_name).is_none() {
            commands.push(MqCommand::create_consumer_group(...));
        }
        if session.cached_binding_id(exchange_id, &filter.routing_key).is_none() {
            commands.push(MqCommand::create_binding_with_opts(...));
        }
    }

    if !commands.is_empty() {
        let resp = batcher.submit(MqCommand::batch(&commands)).await?;
        // Extract entity IDs from BatchResponse and cache them.
    }
}
```

### Files to Modify

| File | Change |
|------|--------|
| `crates/mq-mqtt/src/server.rs` | Batch subscribe orchestration |

### Impact

- **Raft proposals**: N×2 → 1 proposal per SUBSCRIBE
- **Latency**: Single round-trip instead of N sequential round-trips
- **No engine changes**: Uses existing TAG_BATCH

---

## Optimization 8: Zero-Copy Retained Message Delivery

### Problem

When a client subscribes with retain_handling=0 (send retained on subscribe),
the adapter:
1. Calls `TAG_GET_RETAINED` with exchange_id + filter pattern
2. Receives `RetainedMessages { Vec<RetainedEntry> }` — bulk list
3. Iterates and encodes each retained message as MQTT PUBLISH

The `RetainedEntry` contains cloned message bytes. For high-cardinality retained
topics this is expensive.

### Proposed Solution

Use mmap-backed zero-copy reads. The `RetainedEntry` already contains a
`message_id` (log offset). The adapter can read retained messages directly from
the log reader instead of cloning them in the response:

```rust
// Instead of cloning bytes in RetainedMessages response,
// return only metadata (topic, message_id, timestamp)
// and let the adapter read from MqReader.
```

#### Response Changes

```rust
struct RetainedEntry {
    pub routing_key: Bytes,
    pub message_id: u64,      // Log offset for MqReader
    pub timestamp: u64,
}
```

The adapter uses `log_reader.read_messages_at_into(message_id, buf)` to get
zero-copy mmap-backed bytes (same as normal delivery).

### Files to Modify

| File | Change |
|------|--------|
| `crates/mq/src/types.rs` | Slim RetainedEntry to metadata-only |
| `crates/mq/src/engine.rs` | Return message_id instead of cloning bytes |
| `crates/mq-mqtt/src/server.rs` | Read retained via log_reader |

### Impact

- **Memory**: Zero-copy retained delivery (mmap-backed)
- **CPU**: No bulk clone of retained message bytes

---

## Implementation Order & Dependencies

### Phase Diagram

```
Phase A (Zero Engine Changes — adapter migration):
  ├── Opt 2: Publisher ID → FlatMessage.publisher_id  (prerequisite for Opt 4)
  ├── Opt 3: Retain flag → FlatMessage.FLAG_RETAIN
  └── Opt 7: Batch subscribe (uses existing TAG_BATCH)

Phase B (Engine Enhancements):
  ├── Opt 1: Push-based delivery wiring (GroupNotifier → adapter)
  ├── Opt 4: Engine-level no-local filtering  (depends on Opt 2)
  ├── Opt 5: Engine-level message expiry filtering
  └── Opt 6: Engine-level $-topic filtering

Phase C (Response Optimization):
  └── Opt 8: Zero-copy retained message delivery
```

### Dependency Graph

```
Opt 2 → Opt 4 (no-local needs publisher_id in fixed header)
Opt 1 → (none, uses existing GroupNotifier)
Opt 3 → (none, uses existing FLAG_RETAIN)
Opt 5 → (none)
Opt 6 → (none)
Opt 7 → (none, uses existing TAG_BATCH)
Opt 8 → (none)
```

---

## Test Coverage Plan

### Phase A Tests

#### TA.1: Publisher ID Migration (Opt 2)
1. Publish sets `publisher_id` on FlatMessage (not header)
2. `publisher_id()` returns correct session_id
3. No `mqtt.publisher_session_id` header present
4. Backward compat: old messages without publisher_id return 0

#### TA.2: Retain Flag Migration (Opt 3)
1. Publish with retain=true sets FLAG_RETAIN
2. Publish with retain=false does not set FLAG_RETAIN
3. `is_retain()` returns correct value
4. No `mqtt.original_retain` header present

#### TA.3: Batch Subscribe (Opt 7)
1. Multiple filters batched into single command
2. All queues and bindings created
3. Entity IDs correctly cached
4. Idempotent: re-subscribe uses cache

### Phase B Tests

#### TB.1: Push-Based Delivery (Opt 1)
1. Message delivery triggers within 5ms (not 50ms)
2. Idle connections don't consume CPU
3. Multiple subscriptions aggregate notifications
4. Unsubscribe removes watcher

#### TB.2: Engine No-Local Filtering (Opt 4)
1. Messages with matching publisher_id skipped at engine level
2. Messages with different publisher_id delivered
3. exclude_publisher_id=0 delivers all
4. No adapter-level ACK for filtered messages

#### TB.3: Engine Expiry Filtering (Opt 5)
1. Expired messages auto-ACKed at engine level
2. Non-expired messages delivered with adjusted TTL
3. current_time_ms=0 disables expiry check
4. Mixed expired/valid batch handled correctly

#### TB.4: Engine $-Topic Filtering (Opt 6)
1. $SYS/... not enqueued to wildcard subscriptions
2. $SYS/... delivered to exact-match subscriptions
3. Non-$ topics delivered to wildcard subscriptions
4. Binding metadata tracks wildcard flag

### Phase C Tests

#### TC.1: Zero-Copy Retained (Opt 8)
1. Retained messages read via log_reader (mmap-backed)
2. Multiple retained messages delivered correctly
3. Retained message after purge handled gracefully

---

## Tracking

### Status Key
- [ ] Not started
- [~] In progress
- [x] Complete

### Phase A: Adapter Migration
- [x] Opt 2: Replace `HDR_PUBLISHER_SESSION_ID` header with `FlatMessage.publisher_id()`
- [x] Opt 2: Replace `extract_publisher_session_id()` with `flat.publisher_id()`
- [x] Opt 2: Remove `session_id_bytes` field and `HDR_PUBLISHER_SESSION_ID` static
- [x] Opt 2: Tests TA.1
- [x] Opt 3: Replace `HDR_ORIGINAL_RETAIN` header with `FlatMessage.retain()`
- [x] Opt 3: Replace `extract_original_retain()` with `flat.is_retain()`
- [x] Opt 3: Remove `HDR_ORIGINAL_RETAIN` static
- [x] Opt 3: Tests TA.2
- [x] Opt 7: Batch queue + binding creation in `orchestrate_subscribe()` (2 Raft proposals instead of 2N)
- [x] Opt 7: Tests TA.3 (348 tests pass)

### Phase B: Engine Enhancements
- [x] Opt 1: Accept `Arc<GroupNotifier>` in MqttServer
- [x] Opt 1: Implement aggregated DeliveryNotifier per connection
- [x] Opt 1: Wire notification-driven select! in connection_loop
- [x] Opt 1: Add 500ms fallback heartbeat
- [x] Opt 1: Tests TB.1 (6 new GroupNotifier tests)
- [x] Opt 4: Add `group_deliver_filtered()` with `exclude_publisher_id` field
- [x] Opt 4: Filter by publisher_id in engine TAG_GROUP_DELIVER handler
- [x] Opt 4: Remove adapter-level no-local filtering
- [x] Opt 4: Tests TB.2
- [x] Opt 5: Add `current_time_ms` to `group_deliver_filtered()` constructor
- [x] Opt 5: Filter expired messages in engine TAG_GROUP_DELIVER handler
- [x] Opt 5: Remove adapter-level expiry drop (keep adjusted TTL computation)
- [x] Opt 5: Tests TB.3
- [x] Opt 6: Filter $-topics during exchange `route()` (skip wildcard-starting bindings)
- [x] Opt 6: Remove adapter-level $-topic filtering
- [x] Opt 6: Tests TB.4 (`test_dollar_topic_filtering`)

### Phase C: Response Optimization
- [x] Opt 8: Already implemented — retained delivery reads directly from mmap-backed log_reader
- [x] Opt 8: No `RetainedEntry` or `TAG_GET_RETAINED` used by adapter
- [x] Opt 8: Tests TC.1 (existing retained delivery tests pass)

### Integration & Validation
- [x] All 339 bisque-mq lib tests pass
- [x] All 348 bisque-mq-mqtt tests pass (41 new vs Phase 2)
- [x] All 152 bisque-mq-protocol tests pass
- [x] Full cargo build succeeds (zero errors)

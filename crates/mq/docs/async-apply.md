# Async State Machine: Decoupled Apply with Per-Client Response Channels

## Problem

The current Raft `apply()` path is **synchronous from OpenRaft's perspective**:
OpenRaft calls `apply(stream)`, the state machine drains entries, dispatches
to partition workers, **waits for all workers to finish**, runs post-apply
processing (structural writes, segment tracking), sends responses through
OpenRaft's `ApplyResponder`, and only then returns. OpenRaft cannot call
`apply()` again until the previous call completes.

This creates a pipeline stall: the time spent waiting for worker responses
and running post-apply processing is time OpenRaft cannot deliver the next
batch of committed entries. On a system processing 30M msgs/sec, even a
few microseconds of stall per batch limits throughput.

The `ApplyResponder` mechanism also forces responses to flow back through
the Raft machinery, adding latency and coupling the apply path to the
response delivery path. Protocol adapters (MQTT, AMQP, Kafka) must wait for
the full round-trip through the state machine before they can acknowledge
to clients.

## Design

### Core Idea

Decouple response delivery from Raft's apply path entirely, and replace
push-based dispatch with pull-based log consumption:

1. **`apply()` just advances a high-water mark** — drain entries from
   the stream, stash client_ids, bump an atomic counter, return. ~1-5μs
   total. No routing, no dispatching, no waiting.

2. **Workers pull directly from the mmap raft log** — each worker reads
   entries from the log at its own pace, filters by partition
   (`primary_id % N == me`), and applies matching commands. Zero-copy
   reads from mmap. No channels, no dispatcher, no command cloning.

3. **Workers deliver responses directly** to per-client channels after
   applying each command. No routing through the state machine.

4. **Adapters subscribe to their client's channel** and correlate responses
   by log_id. The adapter already knows the full request context (topic name,
   QoS level, correlation ID) from when it proposed — the response only needs
   minimal confirmation data.

```
Current flow:
  Client → Adapter → Raft propose → commit → apply(SM) → wait → response → Adapter → Client
                                                            ↑ blocks here

Proposed flow:
  Client → Adapter → Raft propose → commit
                                       │
                        apply(SM): drain stream, advance HWM, return (~1μs)
                                       │
                     ┌─────────────────────────────────────┐
                     │     Raft log segments (mmap)        │
                     │  ┌──────┬──────┬──────┬──────┐      │
                     │  │ seg₀ │ seg₁ │ seg₂ │ seg₃ │ ...  │
                     │  └──┬───┴──┬───┴──┬───┴──┬───┘      │
                     └─────┼──────┼──────┼──────┼──────────┘
                           │      │      │      │
                        Worker₀ Worker₁ Worker₂ Worker₃
                        (pull)  (pull)  (pull)  (pull)
                           │      │      │      │
                           ▼      ▼      ▼      ▼
                     apply + deliver response to client channel
                           │
                        Adapter: read from client channel → send to client
```

### Why This Works

- **Idempotency is free**: the Raft log is the source of truth. Replaying
  the log rebuilds identical in-memory state. If an adapter disconnects
  before seeing the response, it reconnects and the state is already applied.
  No duplicate-apply risk.

- **Backpressure is pre-gated**: flow control happens before Raft proposal
  (topic/group-level limits, connection-level limits). By the time a command
  is committed, it's guaranteed to be acceptable. The response channel is
  purely a notification mechanism, not a flow-control mechanism.

- **Ordering per client is natural**: each client has its own channel.
  Commands from the same client are proposed in order, committed in order,
  and applied in order (within a partition). Responses arrive in order.

- **Follower replay is zero-cost**: on followers and during log replay,
  there are no client channels — `client_id` is `None`, workers just apply
  and discard responses.

## Per-Client Response Channels

### Client Registry

Each connected client (MQTT connection, AMQP session, Kafka client) gets a
unique `u64` client_id and a dedicated response channel. No shared log, no
filtering.

```rust
use dashmap::DashMap;
use tokio::sync::mpsc;

/// Registry of active client response channels.
///
/// Workers look up channels by client_id after applying each command.
/// Adapters register on connect, unregister on disconnect.
pub struct ClientRegistry {
    clients: DashMap<u64, mpsc::UnboundedSender<ResponseEntry>>,
    next_id: AtomicU64,
}

impl ClientRegistry {
    /// Register a new client. Returns (client_id, receiver).
    pub fn register(&self) -> (u64, mpsc::UnboundedReceiver<ResponseEntry>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::unbounded_channel();
        self.clients.insert(id, tx);
        (id, rx)
    }

    /// Unregister a client. Pending responses are discarded.
    pub fn unregister(&self, client_id: u64) {
        self.clients.remove(&client_id);
    }

    /// Send a response to a client. Returns false if the client disconnected.
    #[inline]
    pub fn send(&self, client_id: u64, entry: ResponseEntry) -> bool {
        if let Some(tx) = self.clients.get(&client_id) {
            tx.send(entry).is_ok()
        } else {
            false // client disconnected, response discarded
        }
    }
}
```

**Why unbounded?** Backpressure is enforced at proposal time. By the time a
response is written, the client has already been flow-controlled. The channel
is a notification pipe, not a buffer.

**Why per-client, not shared?** Zero filtering overhead. No contention between
clients. Clean disconnect semantics — drop the receiver, pending responses
are discarded silently.

### Response Entry (Flat Wire Format)

Responses use the same flat binary encoding as `MqCommand`. The adapter
already knows the full request context from proposal time — the response
only carries confirmation data.

```rust
/// Minimal response entry delivered to a client's channel.
///
/// Uses flat binary encoding: [tag:1][log_index:8][...fields].
/// The adapter correlates by log_index and knows the original request
/// context (topic, QoS, correlation_id) from when it proposed.
pub struct ResponseEntry {
    pub buf: Bytes,
}
```

**Wire format per response type (8-byte aligned header):**

All responses share an 8-byte header `[tag:1][status:1][pad:2][pad:4]`
followed by 8-byte aligned fields. This matches the `MqCommand` wire format
convention — all field offsets are multiples of their natural alignment,
enabling zero-copy access via direct pointer casts (`&buf[offset] as *const u64`).

```
Header (8 bytes, all responses):
  [tag:1][status:1][_:2][_:4]

  status: 0 = ok, 1 = error (for quick success/failure check without parsing)

Published (32 bytes):
  [header:8][log_index:8][base_offset:8][count:8]

EntityCreated (24 bytes):
  [header:8][log_index:8][entity_id:8]

Ok (16 bytes):
  [header:8][log_index:8]

Error (24+ bytes):
  [header:8][log_index:8][error_code:4][msg_len:4][msg_bytes...padded to 8]

Messages (24+ bytes):
  [header:8][log_index:8][count:4][_:4][msg_data...]

BatchResponse (24+ bytes):
  [header:8][log_index:8][count:4][_:4][sub_responses...]
```

The adapter reads `tag` at offset 0, `log_index` at offset 8 (always the
same position), and matches to its pending request table. Protocol-specific
fields are read from fixed offsets — no parsing, just pointer arithmetic.

**Response encoder** — single allocation, 8-byte aligned layout:

```rust
impl ResponseEntry {
    const STATUS_OK: u8 = 0;
    const STATUS_ERROR: u8 = 1;

    const TAG_PUBLISHED: u8 = 0x01;
    const TAG_ENTITY_CREATED: u8 = 0x02;
    const TAG_OK: u8 = 0x03;
    const TAG_ERROR: u8 = 0x04;
    const TAG_MESSAGES: u8 = 0x05;
    const TAG_BATCH: u8 = 0x06;

    /// Encode a Published response (32 bytes).
    pub fn published(log_index: u64, base_offset: u64, count: u64) -> Self {
        let mut buf = BytesMut::with_capacity(32);
        buf.put_u8(Self::TAG_PUBLISHED);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 6); // pad to 8
        buf.put_u64(log_index);
        buf.put_u64(base_offset);
        buf.put_u64(count);
        Self { buf: buf.freeze() }
    }

    /// Encode an Ok response (16 bytes).
    pub fn ok(log_index: u64) -> Self {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u8(Self::TAG_OK);
        buf.put_u8(Self::STATUS_OK);
        buf.put_bytes(0, 6); // pad to 8
        buf.put_u64(log_index);
        Self { buf: buf.freeze() }
    }

    // Zero-copy accessors.
    #[inline] pub fn tag(&self) -> u8 { self.buf[0] }
    #[inline] pub fn status(&self) -> u8 { self.buf[1] }
    #[inline] pub fn log_index(&self) -> u64 { u64::from_be_bytes(self.buf[8..16].try_into().unwrap()) }
    #[inline] pub fn is_ok(&self) -> bool { self.buf[1] == Self::STATUS_OK }

    // ... other constructors and accessors
}
```

### Client ID Propagation

The client_id is **proposal-time metadata**, not part of the Raft log.
On replay there's no client waiting — workers just apply and discard.

```
Proposal time:
  Adapter → Raft::client_write(MqCommand) → committed → log_id returned

Apply time (leader, live):
  EntryWork { cmd, log_index, client_id: Some(42), ... }
  → Worker applies cmd
  → Worker calls registry.send(42, ResponseEntry::published(...))

Apply time (follower / replay):
  EntryWork { cmd, log_index, client_id: None, ... }
  → Worker applies cmd
  → No response delivery
```

The `client_id` flows through the OpenRaft responder mechanism. Currently
`ApplyResponder` carries a oneshot sender. We replace this with our client_id:

```rust
/// Replaces OpenRaft's ApplyResponder with client_id routing.
///
/// On the leader, the proposer attaches the client_id when calling
/// client_write(). The state machine extracts it during apply() and
/// passes it through to workers via EntryWork.
pub struct EntryWork {
    pub cmd: MqCommand,
    pub log_index: u64,
    pub current_time: u64,
    pub segment_id: Option<u64>,
    pub record_location: Option<(u32, u32)>,
    pub client_id: Option<u64>,  // None on follower/replay
}
```

## Pull-Based Worker Architecture

### No Dispatcher, No Channels

Instead of the state machine pushing commands through a dispatcher and
channels, **workers pull directly from the mmap-backed raft log**. The
state machine just advances a high-water mark. Workers wake up, scan
new entries from the log, and apply those that belong to their partition.

```
Raft log segments (mmap, zero-copy)
  ├── Worker₀: scans entries, applies where primary_id % N == 0
  ├── Worker₁: scans entries, applies where primary_id % N == 1
  ├── ...
  └── Workerₙ: scans entries, applies where primary_id % N == N-1
                ↑
                each worker has its own cursor, reads at its own pace
```

This eliminates:
- **The dispatcher entirely** — no routing, no bucketing, no single-threaded bottleneck
- **All channel overhead** — no crossfire queues, no channel sends
- **The stream drain bottleneck** — workers read mmap in parallel
- **Command cloning** — workers read zero-copy from mmap, no Bytes allocation
- **The apply() synchronization point** — just advance HWM and return

### High-Water Mark: Coalescing Notification

The state machine publishes a high-water mark (HWM). Workers wake and
process all entries between their cursor and the current HWM. The
notification primitive must handle the common case where the HWM advances
thousands of times before a worker wakes up — **only one wakeup should
be in flight per worker**.

```rust
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
    /// Advance the HWM and notify all waiting workers.
    ///
    /// If workers are busy processing, the notify is a no-op — the
    /// permit is already stored. When they finish and check again,
    /// they'll see the latest value. No notification queue builds up.
    #[inline]
    pub fn advance(&self, new_hwm: u64) {
        self.value.store(new_hwm, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Wait for the HWM to advance past `cursor`. Returns the new HWM.
    ///
    /// Fast path: if HWM > cursor, returns immediately (no async overhead).
    /// Slow path: registers a single waker, sleeps until notified.
    #[inline]
    pub async fn wait_for(&self, cursor: u64) -> u64 {
        loop {
            let hwm = self.value.load(Ordering::Acquire);
            if hwm > cursor {
                return hwm;
            }
            self.notify.notified().await;
        }
    }

    pub fn current(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }
}
```

**Why `notify_waiters()` and not `notify_one()`?** All N workers need to
wake when new entries arrive — each checks if any entries belong to its
partition. At high throughput every worker has work; at low throughput the
unnecessary wakeups are negligible (worker loads atomic, finds no work,
sleeps again in ~10ns).

**Why not `tokio::sync::watch`?** `watch` uses a `Mutex` internally.
`AtomicU64` + `Notify` is lock-free on the fast path. The writer does
one atomic store + one `notify_waiters()` (which is a no-op if no one
is waiting). Zero contention.

### State Machine: `apply()` Becomes Trivial

```rust
async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
where
    Strm: futures::Stream<Item = Result<EntryResponder<MqTypeConfig>, io::Error>>
        + Unpin + OptionalSend,
{
    while let Some(entry_result) = entries.next().await {
        let (entry, responder) = entry_result?;
        self.last_applied = Some(entry.log_id);

        // Store client_id in the side table (indexed by log_index).
        // Workers read this after applying to route responses.
        if let EntryPayload::Normal(_) = &entry.payload {
            if let Some(r) = responder {
                let client_id = extract_client_id(&r);
                self.client_id_table.insert(entry.log_id.index, client_id);
            }
        } else {
            // Blank / Membership — respond immediately, no state change.
            if let Some(r) = responder { r.send(MqResponse::Ok); }
            if let EntryPayload::Membership(m) = entry.payload {
                self.last_membership =
                    StoredMembership::new(Some(entry.log_id), m);
            }
        }
    }

    // Advance HWM — workers wake and pull from the log.
    if let Some(ref la) = self.last_applied {
        self.high_water_mark.advance(la.index);
    }

    self.update_purge_floor();
    self.update_pin_ceiling();

    Ok(())
    // OpenRaft can immediately call apply() again.
}
```

The `apply()` method does almost nothing: drain the stream, stash
client_ids, advance the HWM. No routing, no dispatching, no waiting.

### Worker: Pull from Log, Apply, Respond

Each worker is an independent consumer of the raft log. It maintains its
own cursor, reads entries from mmap, filters by partition, and applies.

```rust
/// Per-worker state.
struct PartitionWorker {
    partition_id: usize,
    num_partitions: usize,
    cursor: u64,                          // last processed log index
    hwm: Arc<HighWaterMark>,
    prefetcher: SegmentPrefetcher,        // shared, pins segments
    engine: Arc<MqEngine>,
    registry: Arc<ClientRegistry>,
    client_id_table: Arc<ClientIdTable>,  // log_index → client_id
    manifest: Option<Arc<MqManifestManager>>,
    segment_indexes: Arc<SegmentIndexMap>,
    m_apply_count: metrics::Counter,
}

impl PartitionWorker {
    async fn run(mut self) {
        loop {
            // Wait for new entries.
            let hwm = self.hwm.wait_for(self.cursor).await;

            // Scan log entries [cursor+1 .. hwm] from mmap.
            self.process_range(self.cursor + 1, hwm);
            self.cursor = hwm;
        }
    }

    fn process_range(&mut self, from: u64, to: u64) {
        let mut count = 0u64;

        for log_index in from..=to {
            // Read entry from mmap (zero-copy).
            let Some(entry) = self.prefetcher.read_entry(log_index) else {
                continue; // gap or blank entry
            };

            // Parse command header — just read tag (byte 0) and
            // primary_id (bytes 8..16) from the mmap page.
            let cmd = MqCommand::from_mmap(entry);
            let tag = cmd.tag();

            // Route: does this entry belong to my partition?
            let target = route_tag(tag, &cmd, self.num_partitions);
            let dominated = match target {
                RouteResult::Mine(p) if p == self.partition_id => true,
                _ => false,
            };

            if !dominated {
                continue; // skip — belongs to another worker
            }

            // Pin the segment for the duration of apply.
            let segment_id = self.prefetcher.segment_for(log_index);
            let record_location = self.prefetcher.log_location(log_index);

            // Apply command.
            let current_time = timestamp_ms(); // or read from entry metadata
            let response = self.engine.apply_command(
                &cmd, log_index, current_time, segment_id,
            );

            // Post-apply: structural writes (fire-and-forget).
            self.handle_structural_write(&cmd, &response, log_index);

            // Segment index tracking.
            if let Some(loc) = record_location {
                self.segment_indexes.track_command(&cmd, loc);
            }

            // Deliver response to client.
            if let Some(client_id) = self.client_id_table.take(log_index) {
                let entry = ResponseEntry::from_response(log_index, &response);
                let _ = self.registry.send(client_id, entry);
            }

            count += 1;
        }

        self.m_apply_count.increment(count);
    }
}
```

### Structural Commands: Per-Worker, Per-Entity Ordering

Each worker processes **all command types** for its partition — including
structural commands (CREATE_TOPIC, DELETE_TOPIC, etc.). There is no
separate structural command path.

**Why this is safe**: routing is deterministic. `CREATE_TOPIC(id=5)` routes
to worker `5 % N`. All subsequent commands for topic 5 (PUBLISH, PURGE,
DELETE) also route to worker `5 % N`. Within one worker, commands execute
in log order. Perfect per-entity ordering.

**Cross-entity structural commands** (e.g., `CREATE_BINDING(exchange=3,
topic=5)`) reference entities on potentially different partitions. The
binding routes by `exchange_id` (stored in the exchange). The referenced
topic just needs to exist in the DashMap. Since DashMap reads are always
consistent (lock-free shard reads), and the topic was created by an
earlier log entry processed by another worker, the topic is visible as
soon as that worker applies it. In the rare case of a near-simultaneous
create + bind, the binding worker can observe the topic because:

1. Both workers read from the same monotonic log
2. The CREATE_TOPIC entry has a lower log_index than CREATE_BINDING
3. Workers process in log order, and the topic worker is at least as
   advanced (same HWM notification woke both workers)

In the unlikely edge case where the topic worker is slower, the binding
worker finds the topic doesn't exist yet and the engine returns an error.
The adapter sees the error and can retry — but in practice this is a
non-issue because structural commands are infrequent and workers process
at similar speeds.

### Shared SegmentPrefetcher

All workers share a single `SegmentPrefetcher` instance. The prefetcher:

- Manages mmap segment pinning with a global max-pinned-segments limit
- Workers call `prefetcher.pin(segment_id)` before reading, `unpin()` after
- Pin/unpin is an atomic refcount — multiple workers can pin the same segment
- The prefetcher tracks which segments are available and handles segment
  transitions (when the active segment rotates)

Each worker advances at its own pace. The **slowest worker** determines
the purge floor — segments cannot be unmapped until all workers have
advanced past them. This replaces the current single-cursor purge floor
with a per-worker cursor minimum:

```rust
fn compute_purge_floor(workers: &[PartitionWorker]) -> u64 {
    workers.iter().map(|w| w.cursor).min().unwrap_or(0)
}
```

## Adapter Integration

### Connection Lifecycle

```rust
// On client connect:
let (client_id, mut response_rx) = registry.register();

// Spawn response reader task for this client:
tokio::spawn(async move {
    while let Some(entry) = response_rx.recv().await {
        let log_index = entry.log_index();
        let tag = entry.tag();

        // Look up pending request by log_index.
        if let Some(pending) = pending_requests.remove(&log_index) {
            // Construct protocol-specific response.
            match protocol {
                Protocol::Mqtt => send_puback(pending, &entry),
                Protocol::Amqp => send_disposition(pending, &entry),
                Protocol::Kafka => send_produce_response(pending, &entry),
            }
        }
    }
});

// On client publish:
let cmd = MqCommand::publish(topic_id, &messages);
let log_id = raft.client_write_with_client_id(cmd, client_id).await?;
pending_requests.insert(log_id.index, PendingPublish { ... });

// On client disconnect:
registry.unregister(client_id);
// receiver dropped → response_rx.recv() returns None → reader task exits
```

### Pending Request Table

Each adapter connection maintains a lightweight map of in-flight requests:

```rust
/// Tracks in-flight requests for a single client connection.
struct PendingRequests {
    /// log_index → request context (protocol-specific metadata).
    requests: HashMap<u64, PendingRequest>,
}

enum PendingRequest {
    Publish { packet_id: u16, qos: u8 },           // MQTT
    Disposition { delivery_id: u32 },                // AMQP
    ProducePartition { topic: String, partition: i32 }, // Kafka
    CreateTopic { name: String },
    // ...
}
```

The adapter stores just enough context to construct the protocol response.
The `log_index` is the correlation key — no need for a separate request ID.

## Consistency Guarantees

### Ordering

- **Per-client**: commands from the same client are proposed in Raft order,
  applied in partition order (same partition for same entity), responses
  delivered in channel order. Clients see responses in request order.

- **Cross-client**: no ordering guarantee (same as any concurrent system).
  If client A's publish must be visible to client B's read, client B waits
  for the log_index to be applied (standard linearizable read pattern).

### Exactly-Once

Not needed at this layer. The Raft log provides exactly-once apply.
The response channel is best-effort notification — if the client disconnects
before receiving the response, the command was still applied exactly once.
The client can reconnect and observe the applied state.

### Snapshot Consistency

Snapshots still require a barrier — all partition workers must drain to a
consistent point before the snapshot is taken. The barrier mechanism is
unchanged from the parallel apply design.

## Performance Impact

### Apply Path Latency

| Component | Current | Pull-based |
|-----------|---------|------------|
| Stream drain | ~1μs/entry | ~1μs/entry (unchanged) |
| Route + bucket + channel send | ~50ns/entry | **0 (eliminated)** |
| **Wait for workers** | **~50-500μs/batch** | **0 (eliminated)** |
| Post-apply processing | ~100ns/entry | 0 (moved to workers) |
| Response delivery | ~50ns/entry | 0 (moved to workers) |
| HWM advance + notify | — | ~5ns (1 atomic store + notify) |
| **Total apply()** | **~100-600μs** | **~1-5μs** |

The `apply()` method becomes: drain stream, stash client_ids, one atomic
store. Constant time regardless of batch size (the stream drain is the
only O(N) part, and it's unavoidable).

### Worker Path Overhead

Each worker scans the log range and skips non-matching entries:

| Component | Cost |
|-----------|------|
| Read tag + primary_id from mmap | ~2ns/entry (L1 cache hit) |
| Route check (`id % N == me?`) | ~1ns |
| Skip non-matching entry | ~3ns total |
| Apply matching entry | ~100-500ns (same as today) |

With N=16 workers and uniform distribution, each worker applies 1/16th
of entries and skips 15/16ths. Scanning 1000 entries costs ~3μs in skips
+ ~30μs for the ~62 matching applies. The skip cost is negligible vs
the apply cost.

### Pipeline Depth

With the current synchronous model, OpenRaft can deliver one batch at a time.
With pull-based workers, `apply()` returns in ~1-5μs. OpenRaft can pipeline
batches as fast as it can commit them. Workers process independently — the
Raft commit pipeline is fully decoupled from the apply pipeline.

### Response Latency (Client-Visible)

- Current: commit + stream drain + dispatch + wait + post-apply + responder
- Proposed: commit + HWM notify + worker wakes + mmap read + apply + channel send

The critical path is shorter: no dispatcher routing, no channel send to
worker, no result collection. The worker reads directly from mmap and
delivers to the client channel. Expected improvement: ~10-50μs lower
per-request latency.

### Memory

- No crossfire queues (eliminated)
- No ResponseSlab (eliminated)
- No partition buckets (eliminated)
- Per-worker cursor: 8 bytes × N
- Client ID side table: ~16 bytes per in-flight entry (bounded by proposal rate)
- HighWaterMark: 1 AtomicU64 + 1 Notify (~64 bytes)
- Total overhead reduction: ~4MB saved (was crossfire queues) for N=16

## Implementation Plan

### Phase 1: HighWaterMark + Pull-Based Worker Loop

- Implement `HighWaterMark` (AtomicU64 + Notify, coalescing)
- Implement `PartitionWorker::run()` — wait for HWM, scan log range,
  filter by partition, apply matching commands
- Wire workers to shared `SegmentPrefetcher` for mmap log reads
- Workers handle all command types including structural (per-entity ordering)
- Unit tests: HWM notification coalescing, worker cursor advancement

### Phase 2: ResponseEntry + ClientRegistry

- Define `ResponseEntry` flat wire format (constructors, accessors)
- Implement `ClientRegistry` with register/unregister/send
- Implement `ClientIdTable` (log_index → client_id side table)
- Workers deliver responses after applying each command
- Unit tests for encoding/decoding round-trips

### Phase 3: Non-Blocking `apply()`

- Replace current `apply()` with HWM-based version: drain stream, stash
  client_ids, advance HWM, return
- Remove `ParallelApplyDispatcher` (no dispatcher needed)
- Remove `DispatchContext`, `ResponseSlab`, `CommandWork`, `WorkItem`
- Remove crossfire dependency
- Purge floor becomes min of all worker cursors
- Snapshot barrier: advance HWM, wait for all workers to reach it
- Integration tests: concurrent workers, log replay, snapshot barrier

### Phase 4: Adapter Integration

- Implement `PendingRequests` table in adapter connection state
- Wire `client_id` through Raft proposal path into `ClientIdTable`
- Replace `ApplyResponder`-based response delivery with channel reads
- Protocol-specific response construction from `ResponseEntry`
- End-to-end tests: propose → commit → worker apply → response → protocol ack

### Phase 5: Remove Legacy Response Path

- Remove `MqResponse` from `ApplyResponder` — Raft responder sends only
  `MqResponse::Ok` (or a minimal ack) to unblock the proposal path
- Remove `MqResponse` serde derives (no longer serialized through Raft)
- Clean up dead code paths in state machine

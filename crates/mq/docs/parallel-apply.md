# Parallel Apply: Partitioned State Machine Execution

## Problem

The Raft apply path is single-threaded: committed log entries are applied
sequentially by one async task calling `MqEngine::apply_command()`. All entity
mutations serialize through this single writer, even when commands operate on
completely independent entities (different topics, different consumer groups).

On a system with 100 active topics and 50 consumer groups, a publish to topic A
blocks a deliver from group B. The CPU is the bottleneck — the mmap write path
is fast, but apply-side work (DashMap lookups, BTreeMap inserts, atomic updates,
routing) consumes ~100-500ns per command and cannot scale beyond one core.

Current single-threaded throughput:
- topic::publish batch=100: ~27M msg/s
- ack::enqueue+deliver+ack: ~3.5M msg/s
- actor::send+deliver+ack: ~1.9M msg/s

With N=4 partition workers, theoretical aggregate: 4x single-threaded.

## Design

### Core Insight

The Raft contract requires **deterministic apply in log order**. But if two
commands operate on disjoint state, their execution order doesn't affect the
final state. As long as command routing is deterministic
(`entity_id % N → partition`), every replica produces identical state. The
parallelism is invisible to Raft.

### Architecture

```
Raft Committed Entries (ordered stream)
              │
       ┌──────▼────────┐
       │   Dispatcher  │  single task, routes by entity_id % N
       └──┬──┬──┬──┬───┘
          │  │  │  │
          ▼  ▼  ▼  ▼
       [Q₀][Q₁][Q₂][Q₃]   crossfire bounded queues (lock-free)
          │  │  │  │
          ▼  ▼  ▼  ▼
       [W₀][W₁][W₂][W₃]   tokio worker tasks
          │  │  │  │
          ▼  ▼  ▼  ▼
       ┌───────────────┐
       │   MqMetadata  │   shared DashMap + atomics (concurrent access)
       └───────────────┘
```

- **Dispatcher**: Single async task. Reads committed entries from the Raft
  `apply()` stream. Peeks at command tag + primary entity ID. Routes to
  `partition = entity_id % N`. Sends `(command, log_index, current_time,
  oneshot::Sender<MqResponse>)` through crossfire bounded queue.

- **Partition workers** (`W₀..Wₙ`): Each is a tokio task with its own crossfire
  receive queue. Calls `engine.apply_command()` for its assigned commands.
  Sends response back via oneshot. Workers process commands in FIFO order,
  maintaining per-partition ordering.

- **Shared state**: `Arc<MqMetadata>` with DashMaps and atomics. Workers write
  to their partition's entities via `DashMap::get_mut()` (shard lock). Protocol
  adapter readers continue reading via `DashMap::get()` concurrently. No change
  to the read path.

### Partition Routing

#### Co-location Model

Entities that interact on the hot path must be co-located on the same partition
to avoid cross-partition coordination on every operation.

**Key relationship**: A consumer group always has a `source_topic_id`. Publishes
to that topic auto-enqueue into attached groups. This is the most frequent
cross-entity interaction and must NOT require cross-partition dispatch.

**Routing rules**:

| Command | Partition Key | Rationale |
|---------|--------------|-----------|
| Topic ops (PUBLISH, PURGE, COMMIT_OFFSET) | `topic_id % N` | Topic-local |
| Consumer group ops (DELIVER, ACK, NACK, ...) | `source_topic_id % N` | Co-located with source topic |
| Consumer group (no source topic, e.g. Offset-only) | `group_id % N` | Independent |
| Exchange ops (CREATE, DELETE, BIND) | `exchange_id % N` | Rare, low-throughput |
| Session ops (CREATE, HEARTBEAT, DISCONNECT) | `session_id % N` | Session-local |
| PUBLISH_TO_EXCHANGE | Split (see below) | Multi-target |
| BATCH | Split or route (see below) | May contain mixed targets |

**Routing table**: The dispatcher maintains a lightweight mapping for consumer
groups:

```rust
/// Maps entity_id → partition index.
/// Updated on CREATE/DELETE commands (infrequent).
struct PartitionRouter {
    /// group_id → partition index (derived from source_topic_id)
    group_partition: HashMap<u64, usize>,
    num_partitions: usize,
}

impl PartitionRouter {
    fn route_topic(&self, topic_id: u64) -> usize {
        topic_id as usize % self.num_partitions
    }

    fn route_group(&self, group_id: u64) -> usize {
        self.group_partition
            .get(&group_id)
            .copied()
            .unwrap_or(group_id as usize % self.num_partitions)
    }

    fn route_command(&self, cmd: &MqCommand) -> PartitionTarget {
        match cmd.tag() {
            // Topic ops — route by topic_id
            MqCommand::TAG_PUBLISH
            | MqCommand::TAG_COMMIT_OFFSET
            | MqCommand::TAG_PURGE_TOPIC
            | MqCommand::TAG_SET_RETAINED
            | MqCommand::TAG_PRUNE_DEDUP_WINDOW => {
                PartitionTarget::Single(self.route_topic(cmd.primary_id()))
            }

            // Consumer group ops — route by co-located partition
            MqCommand::TAG_GROUP_DELIVER
            | MqCommand::TAG_GROUP_ACK
            | MqCommand::TAG_GROUP_NACK
            | MqCommand::TAG_GROUP_RELEASE
            | MqCommand::TAG_GROUP_TIMEOUT_EXPIRED
            | MqCommand::TAG_GROUP_DELIVER_ACTOR
            | MqCommand::TAG_GROUP_ACK_ACTOR
            // ... all group tags
            => {
                PartitionTarget::Single(self.route_group(cmd.primary_id()))
            }

            // Exchange publish — split into per-topic publishes
            MqCommand::TAG_PUBLISH_TO_EXCHANGE => {
                PartitionTarget::ExchangeFanout(cmd.primary_id())
            }

            // Batch — analyze sub-commands
            MqCommand::TAG_BATCH => PartitionTarget::Batch,

            // Creation/deletion — may need routing table update
            MqCommand::TAG_CREATE_TOPIC
            | MqCommand::TAG_CREATE_CONSUMER_GROUP => {
                PartitionTarget::Coordinator
            }

            _ => PartitionTarget::Single(
                cmd.primary_id() as usize % self.num_partitions
            ),
        }
    }
}

enum PartitionTarget {
    /// Route to a single partition.
    Single(usize),
    /// Exchange fanout — dispatcher resolves routing, splits per-partition.
    ExchangeFanout(u64),
    /// Batch — dispatcher splits sub-commands by partition.
    Batch,
    /// Structural command — dispatcher handles directly, updates routing table.
    Coordinator,
}
```

#### Why Co-location Works

The hot path for the three consumer group variants:

**Offset variant** (Kafka semantics):
```
PUBLISH(topic_id) → partition[topic_id % N]  ← topic-local
COMMIT_OFFSET     → partition[topic_id % N]  ← same partition
```

**Ack variant** (queue semantics):
```
PUBLISH(topic_id)     → partition[topic_id % N]  ← topic + auto-enqueue group
GROUP_DELIVER(grp_id) → partition[topic_id % N]  ← co-located with topic
GROUP_ACK(grp_id)     → partition[topic_id % N]  ← co-located with topic
```

**Actor variant** (mailbox semantics):
```
PUBLISH(topic_id)          → partition[topic_id % N]  ← topic + send to actors
GROUP_DELIVER_ACTOR(grp_id)→ partition[topic_id % N]  ← co-located
GROUP_ACK_ACTOR(grp_id)    → partition[topic_id % N]  ← co-located
```

All three hot paths execute entirely within one partition. Zero cross-partition
coordination on the critical path.

### Cross-Entity Operations

These operations touch multiple entities that may be on different partitions.
They are **infrequent** (control plane, not data plane) and can tolerate higher
latency.

#### PUBLISH_TO_EXCHANGE

Exchange routing resolves which topics receive the published messages. Target
topics may be on different partitions.

**Strategy**: The dispatcher resolves routing (exchange bindings are read-only
metadata), then dispatches per-partition publish sub-commands.

```
Dispatcher receives PUBLISH_TO_EXCHANGE(exchange_id, messages)
  1. Read exchange bindings (read-only, no lock contention)
  2. Route each message → target topic_ids
  3. Group by partition: { partition_0: [topics...], partition_1: [topics...] }
  4. For each partition group:
     - Construct synthetic PUBLISH commands for that partition's topics
     - Dispatch to partition queue
  5. Wait for all partition responses (CountdownLatch + oneshot)
  6. Merge responses, send back to Raft responder
```

This is the only frequently-called cross-partition operation. The dispatcher
does the routing resolution (cheap — just a HashMap lookup in the exchange)
and the actual publish + enqueue work happens in parallel across partitions.

#### CREATE_CONSUMER_GROUP (with auto-create topics)

May create source topic, DLQ topic, and response topic, then attach the group.
Topics may land on different partitions.

**Strategy**: Handle in the dispatcher as a coordinator command:
1. Generate entity IDs (next_id is atomic)
2. Dispatch topic creation to target partitions
3. Wait for all topic creations
4. Dispatch group creation to co-located partition (same as source topic)
5. Update routing table: `group_partition[group_id] = source_topic_id % N`

#### SESSION_DISCONNECT (with will publish)

Disconnection releases in-flight messages from consumer groups (potentially on
different partitions) and optionally publishes a will message.

**Strategy**: Dispatcher splits into per-partition work:
1. Look up session's group attachments (`session_group_index`)
2. Group by partition
3. Dispatch release commands to each partition
4. If will publish: route to target topic's partition

#### GROUP_ACK with reply_to

The ack is processed on the group's partition. The reply publish targets a
different topic potentially on a different partition.

**Strategy**: Fire-and-forget. The group's partition processes the ack and
dispatches the reply publish to the target topic's partition via an
inter-partition channel. No response coordination needed — the ack response
doesn't depend on the reply publish completing.

#### BATCH

Batch commands may contain sub-commands for different entities on different
partitions.

**Strategy**: The dispatcher splits the batch:
1. Iterate sub-commands, classify each by partition
2. Group into per-partition batches
3. Dispatch each per-partition batch (as a synthetic BATCH command)
4. Wait for all partition responses
5. Reassemble into original order for the combined BatchResponse

**Optimization**: If all sub-commands route to the same partition (common case
for write-batcher-merged publishes), dispatch the entire batch as-is. No
splitting overhead.

### Response Coordination

Single-partition commands: oneshot channel directly from worker to dispatcher.
No coordination needed.

Multi-partition commands: The dispatcher uses a shared response collector:

```rust
struct MultiPartitionResponse {
    remaining: AtomicUsize,
    responses: Mutex<Vec<(usize, MqResponse)>>,  // (partition_order, response)
    done: oneshot::Sender<Vec<MqResponse>>,
}
```

Each partition worker decrements `remaining`. The last worker to finish sends
the collected responses through the oneshot. The dispatcher awaits this oneshot
and merges the responses in the correct order.

### Engine Changes

#### `apply_command` becomes `&self`

Currently `apply_command(&mut self, ...)`. The `&mut` is unnecessary — all
mutable state lives in `Arc<MqMetadata>` (DashMaps + atomics). The engine
struct only holds `Arc<MqMetadata>`, `MqConfig` (immutable), and pre-initialized
metrics handles (interior-mutable). Changing to `&self` allows multiple workers
to share one `Arc<MqEngine>`.

```rust
// Before:
pub fn apply_command(&mut self, cmd: &MqCommand, ...) -> MqResponse

// After:
pub fn apply_command(&self, cmd: &MqCommand, ...) -> MqResponse
```

This is a one-line change — the method body already accesses all mutable state
through `&self.metadata` (DashMap shard locks) and `&self.m_*` (metrics handles
with interior mutability).

#### Partition-aware batch processing

The existing `apply_batch()` optimization (run detection for same-entity
commands) continues to work within each partition. Since the dispatcher
pre-groups commands by partition, each worker's batch is already filtered to
its entities. The run detection further optimizes by amortizing DashMap lookups
within a partition.

### Snapshot & Restore

#### Snapshot Barrier

All partition workers must drain to a consistent log index before a snapshot
can be taken.

```
Dispatcher:
  1. Stop dispatching new commands
  2. Send Barrier(log_index, oneshot) to each partition queue
  3. Each worker processes all commands up to barrier, then signals oneshot
  4. Dispatcher awaits all N barriers
  5. Take snapshot (iterate DashMaps — same as today)
  6. Resume dispatching
```

The barrier is a special internal message, not an MqCommand. Workers recognize
it and signal completion without applying anything.

#### Restore

On snapshot restore:
1. Dispatcher pauses
2. Clear all DashMaps (same as today)
3. Repopulate from snapshot (same as today — single-threaded, infrequent)
4. Rebuild routing table from restored consumer group metadata
5. Resume dispatcher

No per-partition restore needed — the DashMaps are shared and restored
atomically by a single task.

### Purge Floor & Segment Lifecycle

Purge floor computation iterates all topics and consumer groups. With parallel
apply, the `purge_floor_dirty` flag is set by any partition worker (it's already
`AtomicBool`). The dispatcher checks and recomputes after each apply batch,
same as today. The computation reads from DashMaps which is safe.

The segment index tracking (`track_command`) needs to be partition-safe. Options:
- Each worker has its own segment index writer (simplest)
- Shared segment index with lock-free append (more complex, marginal benefit)

Recommendation: Per-worker segment index writers. Merge on segment seal.

### Crossfire Queue Configuration

```rust
pub struct ParallelApplyConfig {
    /// Number of partition workers. Default: min(num_cpus, 8).
    /// Set to 1 to disable parallel apply (single-threaded fallback).
    pub num_partitions: usize,

    /// Crossfire queue capacity per partition worker.
    /// Must be large enough to absorb bursts without backpressure.
    pub partition_queue_capacity: usize,
}

impl Default for ParallelApplyConfig {
    fn default() -> Self {
        Self {
            num_partitions: std::thread::available_parallelism()
                .map(|n| n.get().min(8))
                .unwrap_or(4),
            partition_queue_capacity: 4096,
        }
    }
}
```

`N=1` is the degenerate case — equivalent to today's single-threaded apply.
This is the default for tests and the fallback if parallel apply is disabled.

### Dispatcher Implementation Sketch

```rust
struct PartitionWork {
    cmd: MqCommand,
    log_index: u64,
    current_time: u64,
    segment_id: Option<u64>,
    response_tx: oneshot::Sender<MqResponse>,
}

async fn dispatcher_loop(
    mut entries: impl Stream<Item = Result<EntryResponder<MqTypeConfig>, io::Error>> + Unpin,
    workers: Vec<crossfire::MAsyncTx<Array<PartitionWork>>>,
    router: &mut PartitionRouter,
    engine: Arc<MqEngine>,
    manifest: Option<Arc<MqManifestManager>>,
) {
    let current_time = timestamp_ms();

    while let Some(entry_result) = entries.next().await {
        let (entry, responder) = entry_result.unwrap();
        let log_index = entry.log_id.index;

        match entry.payload {
            EntryPayload::Blank => {
                if let Some(r) = responder { r.send(MqResponse::Ok); }
            }
            EntryPayload::Normal(cmd) => {
                match router.route_command(&cmd) {
                    PartitionTarget::Single(partition) => {
                        let (tx, rx) = oneshot::channel();
                        workers[partition].send(PartitionWork {
                            cmd, log_index, current_time,
                            segment_id: None, response_tx: tx,
                        }).await.unwrap();
                        // Await response and forward to Raft responder
                        let response = rx.await.unwrap();
                        handle_post_apply(
                            &manifest, &engine, &cmd, &response, log_index,
                        );
                        if let Some(r) = responder { r.send(response); }
                    }
                    PartitionTarget::ExchangeFanout(exchange_id) => {
                        let response = handle_exchange_fanout(
                            &engine, &workers, router,
                            exchange_id, &cmd, log_index, current_time,
                        ).await;
                        if let Some(r) = responder { r.send(response); }
                    }
                    PartitionTarget::Batch => {
                        let response = handle_batch_split(
                            &engine, &workers, router,
                            &cmd, log_index, current_time,
                        ).await;
                        if let Some(r) = responder { r.send(response); }
                    }
                    PartitionTarget::Coordinator => {
                        // Apply directly on dispatcher, update routing table
                        let response = engine.apply_command(
                            &cmd, log_index, current_time, None,
                        );
                        router.update_from_command(&cmd, &response);
                        if let Some(r) = responder { r.send(response); }
                    }
                }
            }
            EntryPayload::Membership(membership) => {
                // Handle membership change (dispatcher-local)
            }
        }
    }
}
```

### Partition Worker Implementation Sketch

```rust
async fn partition_worker(
    rx: crossfire::AsyncRx<Array<PartitionWork>>,
    engine: Arc<MqEngine>,
    partition_id: usize,
    m_apply_count: metrics::Counter,
) {
    while let Ok(work) = rx.recv().await {
        let response = engine.apply_command(
            &work.cmd,
            work.log_index,
            work.current_time,
            work.segment_id,
        );
        m_apply_count.increment(1);
        let _ = work.response_tx.send(response);
    }
}
```

Each worker is simple: receive command, apply, respond. All complexity lives in
the dispatcher's routing and coordination logic.

## Performance Analysis

### Expected Gains

| Workload | N=1 (current) | N=4 | Speedup |
|----------|--------------|-----|---------|
| 1 topic, publish | 27M msg/s | 27M msg/s | 1x (single partition) |
| 4 topics, publish | 27M msg/s | ~100M msg/s | ~4x |
| 1 ack group, full cycle | 3.5M msg/s | 3.5M msg/s | 1x |
| 4 ack groups, full cycle | 3.5M msg/s | ~14M msg/s | ~4x |
| Mixed: 8 topics + 4 groups | ~3.5M msg/s | ~12M msg/s | ~3.4x |

### Overhead

- Dispatcher routing: ~10-20ns per command (tag peek + hash)
- Crossfire send/recv: ~20-30ns per command
- Oneshot response: ~15-20ns
- Total overhead: ~50-70ns per command

At 3.5M msg/s single-threaded (286ns/msg), the 50-70ns overhead is ~20%.
Break-even is at N=2 partitions with 2+ active entities. N=4 with 4+ active
entities gives ~3.2x speedup after overhead.

### Memory

- N crossfire queues: N x capacity x sizeof(PartitionWork) ~ N x 4096 x 64B ~ 1MB for N=4
- Routing table: negligible (one entry per consumer group)
- No additional DashMap overhead (shared, not duplicated)

## Implementation Plan

### Phase 1: Foundation (engine refactor)

**Goal**: Make the engine safe for concurrent apply without changing the
threading model.

- [ ] Change `MqEngine::apply_command` from `&mut self` to `&self`
- [ ] Change `MqEngine::apply_batch` from `&mut self` to `&self`
- [ ] Wrap engine in `Arc<MqEngine>` where needed
- [ ] Add `MqCommand::primary_id()` — extracts the first u64 field (entity_id)
  from any command via the fixed region at offset 8
- [ ] All tests pass with zero behavior change

### Phase 2: Dispatcher + Workers

**Goal**: Implement the parallel dispatch infrastructure.

- [ ] Define `PartitionWork` struct and `PartitionTarget` enum
- [ ] Implement `PartitionRouter` with routing table
- [ ] Implement `dispatcher_loop` with single-partition fast path
- [ ] Implement `partition_worker` loop
- [ ] Wire into `MqStateMachine::apply()` — replace direct
  `engine.apply_command()` with dispatcher
- [ ] Add `ParallelApplyConfig` to `MqConfig`
- [ ] N=1 mode passes all existing tests unchanged
- [ ] N=4 mode passes all tests (entities spread across partitions)

### Phase 3: Cross-partition Operations

**Goal**: Handle multi-entity commands correctly.

- [ ] Exchange fanout: dispatcher resolves routing, splits per-partition
- [ ] Batch splitting: dispatcher groups sub-commands by partition
- [ ] Create consumer group: coordinator pattern with routing table update
- [ ] Session disconnect: per-partition release dispatch
- [ ] Reply-to publish: fire-and-forget cross-partition dispatch
- [ ] Response coordination: `MultiPartitionResponse` with atomic countdown
- [ ] Integration tests for cross-partition correctness

### Phase 4: Snapshot Barrier

**Goal**: Consistent snapshots with parallel apply.

- [ ] Implement barrier message type
- [ ] Dispatcher pause/resume around snapshot
- [ ] Per-worker segment index writers
- [ ] Segment index merge on seal
- [ ] Snapshot/restore integration tests with N>1

### Phase 5: Metrics & Tuning

**Goal**: Observability and production readiness.

- [ ] Per-partition metrics: `mq.partition.apply_count`, `mq.partition.queue_depth`
- [ ] Cross-partition dispatch counter: `mq.partition.cross_dispatch`
- [ ] Dispatcher routing latency histogram
- [ ] Configurable N with runtime validation
- [ ] Benchmark suite: single-entity vs multi-entity scaling
- [ ] Document tuning guidance (N vs entity count, queue capacity)

## Open Questions

1. **Adaptive N**: Should N adjust based on active entity count? Starting with
   fewer workers when entity count is low avoids unnecessary overhead. Could
   start at N=1 and scale up when cross-partition opportunity is detected.

2. **Priority lanes**: Should structural commands (CREATE/DELETE) bypass the
   partition queue and execute on the dispatcher directly? They're rare and
   updating the routing table requires synchronous handling anyway.

3. **Worker pinning**: Should partition workers be pinned to specific CPU cores
   via `core_affinity`? Could improve cache locality but reduces scheduler
   flexibility.

4. **Backpressure**: If a partition queue fills up (one hot topic), the
   dispatcher blocks. Should there be per-partition overflow handling, or is
   backpressure the correct behavior (slows the entire apply path, which slows
   Raft, which slows the leader)?

5. **Inter-partition channels**: For fire-and-forget cross-partition work
   (reply-to publish), should workers have direct channels to each other, or
   always route through the dispatcher?

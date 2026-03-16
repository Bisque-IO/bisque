# Unified Raft Forwarding Architecture

## Problem

Today, commands from follower nodes to the leader traverse two separate batching layers:

1. **Forward transport** (`forward.rs`): TCP naturally batches frames under write pressure. Wire format: `[len:4][client_id:8][cmd_bytes...]` per frame (12-byte header — not 8-byte aligned).
2. **MqWriteBatcher** (`write_batcher.rs`): Re-batches into `TAG_BATCH` with 5ms linger timer, allocates a `oneshot::channel()` per command for response routing, and merges same-topic publishes.

This double-batching introduces:
- **5ms latency floor** from the linger timer (even under load)
- **Per-command oneshot allocation** (~128 bytes each)
- **Extra mpsc hop** (`command_tx`) between forward acceptor and write batcher
- **Redundant serialization**: forward frames are parsed, then re-serialized into TAG_BATCH
- **Two different wire formats** for what is conceptually the same operation

## Proposal: TAG_FORWARDED_BATCH

Unify the forward wire format with MqCommand by introducing a new tag that embeds the forward TCP frames directly — zero-copy from TCP socket to raft log.

### Wire Format

All headers and sub-frames are 8-byte aligned. `client_id` is `u32` (4 billion IDs — more than sufficient for concurrent connections). This gives each sub-frame an 8-byte header `[len:4][client_id:4]`, and each sub-frame is padded to 8-byte alignment (matching TAG_BATCH convention).

```
TAG_FORWARDED_BATCH (tag 55):
┌─────────────────────────────────────────────────────┐
│ MqCommand header [size:4][fixed:2][tag:1][flags:1]  │  @0   8 bytes
├─────────────────────────────────────────────────────┤
│ node_id: u32                                        │  @8   4 bytes
│ count: u32                                          │  @12  4 bytes
├─────────────────────────────────────────────────────┤
│ Frame 0: [len:4][client_id:4][cmd_bytes + pad8]     │  @16  8n bytes
│ Frame 1: [len:4][client_id:4][cmd_bytes + pad8]     │       8n bytes
│ ...                                                 │
└─────────────────────────────────────────────────────┘
```

**Sub-frame layout** (each frame is padded to 8-byte boundary):
```
@0  len:        u32     total payload size (client_id + cmd_bytes, excludes len itself)
@4  client_id:  u32     client routing ID
@8  cmd_bytes:  [u8]    MqCommand bytes (already 8-byte aligned header)
    pad:        [u8]    0-7 zero bytes to reach next 8-byte boundary
```

Total sub-frame size: `4 + align8(len)` bytes (len field + padded payload).

The TCP reader can `split_to().freeze()` and wrap the bytes directly — no parsing, no re-serialization. The forward TCP wire format is updated to match: `[len:4][client_id:4]` per frame (was `[len:4][client_id:8]`).

### Alignment Guarantees

- Outer MqCommand header: 8 bytes (naturally aligned)
- Fixed region (node_id + count): 8 bytes → first sub-frame starts at offset 16 (8-aligned)
- Each sub-frame: `4 + align8(len)` bytes, always a multiple of 8
- Every cmd_bytes payload starts at `frame_start + 8`, which is 8-aligned since frame_start is 8-aligned
- MqCommand headers within cmd_bytes are naturally 8-byte aligned

### Key Insight: node_id Enables Response Routing

The `node_id` field tells the apply layer where to send responses:
- `node_id == local_node_id` → route via `ClientRegistry` (local connection)
- `node_id != local_node_id` → route via `NodeRoute` (TCP back to follower)

This eliminates the need for the `ClientIdTable` to track per-entry routing — the entire batch carries its routing key.

## Architecture Changes

### 1. Eliminate command_tx mpsc

**Before:**
```
TCP reader → parse frames → command_tx (mpsc) → MqWriteBatcher → TAG_BATCH → raft
```

**After:**
```
TCP reader → split_to().freeze() → TAG_FORWARDED_BATCH → raft (direct proposal)
```

The forward acceptor's TCP reader wraps the raw TCP bytes into a `TAG_FORWARDED_BATCH` MqCommand and proposes directly to raft. No intermediate channel, no parsing, no re-serialization.

### 2. LocalBatcher Replaces MqWriteBatcher

For local connections (clients connected directly to the leader), introduce `LocalBatcher`:

- Uses the same `TAG_FORWARDED_BATCH` format with `node_id = 0` (self)
- Natural pressure-based batching: drain the receiver, build one batch, propose
- No linger timer — latency is bounded only by raft commit
- No oneshot channels — responses routed via `ClientRegistry` using embedded `client_id`

```rust
struct LocalBatcher {
    rx: mpsc::Receiver<(u32, Bytes)>,  // (client_id, command)
    raft: RaftHandle,
    node_id: u32,
}

impl LocalBatcher {
    async fn run(&mut self) {
        let mut buf = BytesMut::with_capacity(64 * 1024);
        loop {
            // Wait for first command.
            let Some((client_id, cmd)) = self.rx.recv().await else { return };

            buf.clear();
            // Reserve header space: [hdr:8][node_id:4][count:4] = 16 bytes
            buf.put_bytes(0, 16);
            let mut count = 0u32;

            // Write first sub-frame (8-byte aligned).
            write_sub_frame(&mut buf, client_id, &cmd);
            count += 1;

            // Drain remaining without waiting.
            while let Ok((cid, c)) = self.rx.try_recv() {
                write_sub_frame(&mut buf, cid, &c);
                count += 1;
            }

            // Backfill header, freeze into Bytes (zero-copy).
            MqCommand::write_forwarded_batch_header(&mut buf, self.node_id, count);
            let cmd = MqCommand { buf: buf.split().freeze() };
            let _ = self.raft.propose(cmd).await;
        }
    }
}

/// Write one sub-frame: [len:4][client_id:4][cmd_bytes][pad to 8-byte boundary]
#[inline]
fn write_sub_frame(buf: &mut BytesMut, client_id: u32, cmd: &[u8]) {
    let payload_len = (4 + cmd.len()) as u32;  // client_id + cmd_bytes
    buf.put_u32_le(payload_len);
    buf.put_u32_le(client_id);
    buf.put_slice(cmd);
    // Pad to 8-byte alignment (payload_len excludes the len field itself).
    let padded = ((payload_len as usize) + 7) & !7;
    let pad = padded - payload_len as usize;
    if pad > 0 {
        buf.put_bytes(0, pad);
    }
}
```

### 3. Response Dispatch: ClientPartition + ResponseCallback

Responses are never queued per-client. Instead each **partition task** owns a local
`HashMap<client_id, ResponseCallback>` (no mutex — task-exclusive access) and receives
`Bytes` chunks via a crossfire MPSC channel.

**Two distinct partition kinds:**

| Type | Responsibility |
|------|---------------|
| `PartitionWorker` | Applies raft log commands (existing), routes responses for its client shard |
| `ClientPartition` | Owns callback map for `client_id % N == partition_id`; dispatches `Bytes` chunks to callbacks |

**Callback signature:**

```rust
pub type ResponseCallback = Arc<dyn Fn(&Bytes, usize, &[u8], bool) + Send + Sync + 'static>;
//                                      slab   offset  message  is_done
```

- `slab`: the owning `Bytes` — zero-copy clone via `slab.slice(offset..offset+message.len())`
- `offset`: byte position of `message` within `slab`
- `message`: response payload (after `[len:4][client_id:4]` header)
- `is_done`: `true` after the partition exhausts its current drain cycle — callback can flush/batch

**Follower inbound bitmap dispatch** (`run_inbound_loop`):

```
TCP read (up to 1MB) → scan headers → u64 bitmap of partitions → one Bytes clone per partition
```

Each `ClientPartition` receives the full `Bytes`, scans linearly for its `client_id % N` frames, calls callbacks, signals `is_done` after drain.

**`ClientRegistry`** (redesigned):

```rust
pub struct ClientRegistry {
    next_id: AtomicU32,
    num_partitions: usize,
    bytes_txs: Vec<crossfire::MAsyncTx<Array<Bytes>>>,   // TCP inbound → ClientPartition
    reg_txs:   Vec<UnboundedSender<ClientPartitionMsg>>,  // register/unregister callbacks
}
```

Methods: `register(callback) -> client_id`, `unregister(client_id)`,
`route_log_index(client_id, log_index)`, `route_response(client_id, log_index, &MqResponse)`,
`send_to_partition(partition, bytes)`, `send_to_partition_async(partition, bytes).await`.

### 4. Apply Path Changes

**State machine** (`apply_forwarded_batch_inline`): applies sub-commands only — **no routing**.

**`PartitionWorker`**: reads `TAG_FORWARDED_BATCH` from the mmap log, iterates sub-commands,
and for each where `client_id % N == partition_id` calls `router.route(node_id, client_id, log_index)`.

```rust
// PartitionWorker::process_range — TAG_FORWARDED_BATCH case:
if let Some(router) = self.forwarded_router.get() {
    let view = cmd.as_forwarded_batch();
    let node_id = view.node_id();
    for (client_id, _) in view.iter() {
        if (client_id as usize) % self.num_partitions == self.partition_id {
            router.route(node_id, client_id, rec.log_index);
        }
    }
}
```

`ForwardedResponseRouter::route` signature: `fn route(&self, node_id: u32, client_id: u32, log_index: u64)`.

Router dispatches: local (`node_id == 0 || local`) → `client_registry.route_log_index()`;
remote → `NodeRoute::send(client_id, log_index)`.

### 5. NodeRoute (leader → follower)

```rust
pub struct NodeRoute {
    tx: crossfire::MAsyncTx<Array<Bytes>>,  // pre-encoded frames → batch_response_writer
}

impl NodeRoute {
    pub fn send(&self, client_id: u32, log_index: u64) {
        // encodes [len:12][client_id:4][log_index:8] = 16 bytes, try_send non-blocking
    }
}
```

`batch_response_writer` drains the MPSC channel and writes accumulated frames to follower TCP in one flush per drain cycle — no per-frame encoding, no separate notify mechanism.

## What Gets Eliminated

| Component | Status | Reason |
|-----------|--------|--------|
| `MqWriteBatcher` | **Removed** | Replaced by `LocalBatcher` + direct forward proposal |
| `command_tx` mpsc | **Removed** | Forward acceptor proposes directly |
| 5ms linger timer | **Removed** | Natural pressure-based batching only |
| Per-command `oneshot` | **Removed** | Responses routed by embedded `(node_id: u32, client_id: u32)` |
| `ClientIdTable` (for forwarded) | **Removed** | node_id + client_id embedded in batch |
| Separate forward wire format | **Unified** | Forward frames are now MqCommand payload |
| `ResponseChannel` slab | **Removed** | Replaced by `ClientPartition` + crossfire MPSC |
| `ResponseEntry` (hot path) | **Removed** | Forwarded responses carry only `log_index: u64` |
| Per-client SPSC queues | **Removed** | Replaced by partition-level Bytes dispatch |
| `merge_pending()` | **Deferred** | Can be added as optional optimization in LocalBatcher |

## What Stays

| Component | Reason |
|-----------|--------|
| `ForwardClient` / `ForwardAcceptor` | TCP transport between follower and leader |
| `NodeRoute` + `ArcSwap` routing table | Response routing to remote follower nodes |
| `ClientRegistry` | Partition channel registry — redesigned, no slab |
| `ResponseEntry` | Encoding utility for full `MqResponse` → wire bytes (non-forwarded path) |
| `batch_response_writer` | Drains `NodeRoute` crossfire channel → TCP flush |
| `PartitionWorker` | Applies raft log; now also routes forwarded batch responses |
| `ClientPartition` | **New** — per-partition callback dispatch task |

## Implementation Phases

### Phase 0a: Migrate MqCommand.buf from Vec\<u8\> to Bytes

MqCommand.buf is **never mutated after construction** — all mutation happens inside
`CommandBuilder` (which uses `SmallVec<[u8; 128]>` internally). Every read goes through
`Deref<Target=[u8]>`, which `Bytes` also implements. Migration is surgical:

**Changes required (5 sites):**
- `MqCommand` struct (`types.rs:489`): `pub(crate) buf: Vec<u8>` → `pub(crate) buf: Bytes`
- `CommandBuilder::finish()` (`codec.rs:391`): `SmallVec::into_vec()` → `Bytes::from(self.buf.into_vec())`
- `MqCommand::from_bytes()` (`types.rs:816`): remove `.to_vec()` — just `Self { buf }` (zero-copy!)
- `Decode::decode_from_bytes()` (`codec.rs:1322`): remove `.to_vec()` — just `MqCommand { buf: data }` (zero-copy!)
- `Deserialize` impl (`types.rs:859`): `Ok(Self { buf: Bytes::from(bytes) })`

**No changes needed:**
- All accessor methods (`tag()`, `as_bytes()`, `read_u64()`, `read_flex8()`, etc.) — use `&self.buf[..]`
- `Encode` impl — passes `&self.buf` to `write_all()`
- `BorrowPayload` impl — returns `&self.buf`
- `Serialize` impl — passes `&self.buf` to `serialize_bytes()`
- `Decode::decode()` — reads into `Vec<u8>`, wraps as `Bytes::from(buf)`
- All consumer code — reads through slice indexing

**Zero-copy gains beyond forwarding:**
- Every segment log read (`cursor.rs`): `MqCommand::from_bytes(data.slice(...))` becomes zero-copy (currently allocates + copies)
- Every raft snapshot/log decode: `decode_from_bytes()` becomes zero-copy
- `Clone` on MqCommand becomes refcount bump instead of allocation + memcpy

### Phase 0b: Migrate client_id u64 → u32 and node_id u64 → u32 ✅ Done

All `client_id` and `node_id` fields are now `u32`.

### Phase 1: TAG_FORWARDED_BATCH MqCommand ✅ Done

`TAG_FORWARDED_BATCH = 55` with `CmdForwardedBatch` view, `CmdForwardedBatchIter`,
`MqCommand::forwarded_batch()` constructor, and 8-byte aligned sub-frame layout.

### Phase 2: Direct Raft Proposal from Forward Acceptor ✅ Done

Forward acceptor proposes `TAG_FORWARDED_BATCH` directly to raft. State machine applies
sub-commands in `apply_forwarded_batch_inline` — no response routing (routing moved to
`PartitionWorker`).

### Phase 3: LocalBatcher
- Replace `MqWriteBatcher` (`write_batcher.rs`) with `LocalBatcher` using `TAG_FORWARDED_BATCH` with `node_id = 0`
- Remove: `BatchedRequest`, `ResponseSlot`, `batcher_loop`, `merge_pending`, 5ms linger timer
- Remove per-command `oneshot::channel()` allocation
- Client connections feed `(client_id: u32, Bytes)` into `LocalBatcher`

### Phase 4: Response Dispatch — ClientPartition + ResponseCallback ✅ Done

Implemented in this session:

- `ClientPartition` — per-partition task, local `HashMap<client_id, ResponseCallback>`, no mutex
- `ResponseCallback = Arc<dyn Fn(&Bytes, usize, &[u8], bool) + Send + Sync>` — zero-copy `slab` access
- `ClientRegistry` redesigned: `bytes_txs` (crossfire MPSC) + `reg_txs` (tokio unbounded)
- `run_inbound_loop` uses `u64` bitmap to dispatch one `Bytes` clone per partition per TCP read
- `PartitionWorker` routes `TAG_FORWARDED_BATCH` responses (`log_index` only) via `ForwardedResponseRouter`
- `ForwardedResponseRouter::route(node_id, client_id, log_index: u64)` — no `ResponseEntry`
- `NodeRoute` redesigned: single crossfire MPSC sender; `send(client_id, log_index)` encodes 16-byte frame inline
- `batch_response_writer` drains crossfire channel and flushes to follower TCP
- `AsyncApplyManager::set_forwarded_router()` — wires router into workers via `OnceLock`
- `ParallelApplyConfig::response_partition_capacity` — configurable MPSC channel depth (default 4096)
- `ForwardConfig::node_route_capacity` — configurable NodeRoute channel depth (default 4096)
- Removed: `ResponseChannel` slab from all forwarding hot paths, `forwarded_router` from state machine

## Performance Impact

**Latency reduction:**
- Eliminate 5ms linger floor → sub-millisecond at low load
- Eliminate 2 mpsc hops (command_tx + write_batcher internal)
- Eliminate oneshot allocation + poll overhead

**Throughput improvement:**
- Zero-copy from TCP to raft log (no parse + re-serialize)
- Single allocation per batch (the MqCommand header wrapping)
- Natural backpressure batching scales with load

**Memory reduction:**
- No oneshot channels (128 bytes × concurrent commands)
- No intermediate command buffers in write_batcher
- No ClientIdTable entries for forwarded batches

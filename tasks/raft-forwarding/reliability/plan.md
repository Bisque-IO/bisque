# Forward Transport Reliability — Implementation Plan

Two independent tracks derived from `analysis.md`. **Track A** (forward TCP reliability) can be
built in isolation. **Track B** (client session reliability) depends on Track A's wire format
changes but is otherwise independent.

---

## Wire Format Reference

### Sub-frame (follower → leader, inside `OutboundBuf`)
```
Current:  [payload_len:4][client_id:4][cmd_bytes]
           payload_len = 4 + cmd.len()
           entry_size  = 8 + cmd.len()

Enhanced: [payload_len:4][client_id:4][request_seq:8][cmd_bytes]
           payload_len = 12 + cmd.len()   (client_id + request_seq + cmd_bytes)
           entry_size  = 16 + cmd.len()
```

### Batch TCP frame (follower → leader, wraps one drain event)
```
New:  [batch_seq:8][batch_payload_len:4][sub-frames...]
```
Leader reads the 12-byte batch header first, then exactly `batch_payload_len` bytes of sub-frames.

### TAG_FORWARDED_BATCH codec header (leader Raft entry)
```
Current:  [tag:1][pad:7][node_id:4][count:4]   sub-frames @ offset 16
Enhanced: [tag:1][pad:7][node_id:4][count:4][batch_seq:8]   sub-frames @ offset 24
```

### Response frame (leader → follower)
```
Current:  [client_id:4][log_index:8]                                   = 12 bytes
Enhanced: [len:4][client_id:4][request_seq:8][log_index:8][response_bytes]
           len = 20 + response_bytes.len()                             = 24-byte header
```

### Batch ACK frame (leader → follower, special response)
```
[len:4][client_id=0:4][batch_seq:8]    len = 12    total = 16 bytes
```
Detected by `client_id == 0` (never used by `ClientRegistry`, which starts at 1).

### TAG_RESUME command (client → state machine via forwarding)
```
[TAG_RESUME:1][pad:3][session_client_id:4][last_acked_seq:8]   = 16 bytes  (cmd_bytes)
```
Flows as a sub-frame in TAG_FORWARDED_BATCH like any other command.

---

## Track A: Forward TCP Reliability  ✅ COMPLETE (A1–A9)

### A1 — codec.rs: TAG_FORWARDED_BATCH — add `batch_seq` field  ✅

**Location:** `codec.rs` around line 2201 (TAG_FORWARDED_BATCH write/view section)

- Update `write_forwarded_batch_header()` to accept `batch_seq: u64` and write it at offset 16
  as `u64 LE` (8 bytes). Sub-frames now start at offset **24** instead of 16.
- Update `CmdForwardedBatch` view struct:
  - Add `pub fn batch_seq(&self) -> u64` → `u64::from_le_bytes(self.buf[16..24])`
  - Update `iter()` to start at `pos: 24` instead of `pos: 16`
- `CmdForwardedBatch::count()` — still at `buf[12..16]`, no change.
- `CmdForwardedBatch::node_id()` — still at `buf[8..12]`, no change.
- Update all callers of `write_forwarded_batch_header` to pass a `batch_seq`.
  Callers: `handle_follower_connection` (forward.rs ~1373), write_batcher.rs.

### A2 — codec.rs: `CmdForwardedBatchIter` — update sub-frame layout  ✅

**Location:** `codec.rs` `CmdForwardedBatchIter` `next()` impl

Current sub-frame parsing:
```rust
let payload_len = u32::from_le_bytes(buf[pos..pos+4]) as usize;
let client_id   = u32::from_le_bytes(buf[pos+4..pos+8]);
let cmd_bytes   = &buf[pos+8 .. pos+4+payload_len];
pos += 4 + payload_len;
```

New sub-frame parsing (request_seq at +8, cmd_bytes at +16):
```rust
let payload_len = u32::from_le_bytes(buf[pos..pos+4]) as usize;
let client_id   = u32::from_le_bytes(buf[pos+4..pos+8]);
let request_seq = u64::from_le_bytes(buf[pos+8..pos+16]);
let cmd_bytes   = &buf[pos+16 .. pos+4+payload_len];
pos += 4 + payload_len;
```

Change the iterator `Item` type from `(u32, &[u8])` to `(u32, u64, &[u8])`
(client_id, request_seq, cmd_bytes). Update all `iter()` consumers:
- `state_machine.rs:255` — `apply_forwarded_batch_inline`
- `async_apply.rs:735` — partition worker response building

### A3 — forward.rs: `OutboundBuf::drain_to_tcp` — accumulate + batch framing  ✅

**Location:** `forward.rs:248` `drain_to_tcp()`

**Current behavior:** loops over partitions, does one `write_all` per partition directly to TCP.

**New behavior:**
1. Loop over partitions, extract all `Bytes` slices into `Vec<Bytes>` (same CAS/spin/split
   logic, no TCP write yet).
2. If nothing extracted across all partitions, return `Ok(0)`.
3. Assign next `batch_seq`: `self.batch_seq.fetch_add(1, Ordering::Relaxed) + 1`.
4. Calculate `batch_payload_len: u32` = sum of all extracted slice lengths.
5. Check unacked deque cap (see A4) — if over limit, push back and return `Ok(0)` for
   backpressure; caller retries after the inbound task trims the deque.
6. Push `(batch_seq, parts.clone())` to unacked deque **before** any TCP write.
7. Build and write 12-byte batch header: `[batch_seq:8 LE][batch_payload_len:4 LE]`.
8. Write each partition `Bytes` slice in order, then flush.

Note: step 6 before step 7/8 ensures the entry is in the deque even if the TCP write panics.

### A4 — forward.rs: `UnackedSendBuf` — retransmit structure  ✅

Add a new struct in `forward.rs`:

```rust
struct UnackedSendBuf {
    deque: VecDeque<(u64 /*batch_seq*/, Vec<Bytes>)>,
    total_bytes: usize,
    cap_bytes: usize,       // e.g. 16 MiB
}

impl UnackedSendBuf {
    fn push(&mut self, seq: u64, parts: Vec<Bytes>) -> bool {
        let sz: usize = parts.iter().map(|b| b.len()).sum();
        if self.total_bytes + sz > self.cap_bytes { return false; }
        self.total_bytes += sz;
        self.deque.push_back((seq, parts));
        true
    }
    fn ack_up_to(&mut self, seq: u64) {
        while self.deque.front().map(|(s, _)| *s <= seq).unwrap_or(false) {
            if let Some((_, parts)) = self.deque.pop_front() {
                self.total_bytes -= parts.iter().map(|b| b.len()).sum::<usize>();
            }
        }
    }
}
```

Add `batch_seq: AtomicU64` and `unacked: Mutex<UnackedSendBuf>` to `OutboundBuf`:
```rust
pub struct OutboundBuf {
    partitions: Vec<Arc<OutboundPartition>>,
    waker: OutboundWaker,
    mask: u32,
    batch_seq: AtomicU64,               // NEW
    unacked: Mutex<UnackedSendBuf>,     // NEW
}
```

The `Mutex` is contended only between the outbound task (push on drain) and the inbound task
(ack on ACK receipt) — not per-command, so cost is amortized per batch.

Add:
- `OutboundBuf::ack_batch(&self, seq: u64)` → calls `unacked.lock().ack_up_to(seq)`
- `OutboundBuf::drain_unacked_to_tcp(&self, writer: &mut BoxedWriter) -> io::Result<()>` —
  sends all deque entries in order without modifying the deque (retransmit on reconnect).

### A5 — forward.rs: reconnect — resend unacked before resuming drain  ✅

**Location:** `forward.rs` `run_forward_client_connection()` (~line 877), called from the
reconnect loop at ~line 784.

After completing the handshake write (`[len=4][node_id:4]`), before spawning the outbound task:

```rust
// Retransmit any unacknowledged batches from the previous connection.
outbound.drain_unacked_to_tcp(&mut writer).await?;
```

Then spawn `run_outbound_loop` and `run_inbound_loop` as before. Retransmitted batches arrive
in order before new ones. The state machine deduplicates via batch_seq (A9).

### A6 — forward.rs: `handle_follower_connection` — parse batch frame header  ✅

**Location:** `forward.rs:1312` `handle_follower_connection()`

**Current:** scans a flat stream of sub-frames.

**New:** reads length-prefixed batches. Change the main read loop to:

1. Buffer bytes into `BytesMut` as before (bulk reads).
2. Scan for complete batch frames. A complete batch = 12 header bytes + `batch_payload_len` bytes.
   - Peek first 8 bytes → `batch_seq: u64`
   - Peek next 4 bytes → `batch_payload_len: u32`
   - Complete when `buf.len() >= 12 + batch_payload_len`
3. For each complete batch:
   - Extract `batch_seq` (first 8 bytes).
   - Slice `batch_payload_len` bytes of sub-frames.
   - Pass both to the Raft proposal task (A7).
   - Advance buffer past the full batch.

### A7 — forward.rs: `handle_follower_connection` — batch ACK after `client_write`  ✅

**Location:** inside the spawned Raft proposal task at `forward.rs:~1375`

Current:
```rust
if let Err(e) = raft2.client_write(cmd).await { ... }
```

New — on success, send batch ACK via the per-follower responder channel:
```rust
match raft2.client_write(cmd).await {
    Ok(_) => {
        let mut ack = BytesMut::with_capacity(16);
        ack.put_u32_le(12);          // len = client_id(4) + batch_seq(8)
        ack.put_u32_le(0);           // client_id = 0 sentinel
        ack.put_u64_le(batch_seq);
        let _ = responder_tx.send(ack.freeze());
    }
    Err(e) => { warn!(...); }
}
```

`batch_seq` is captured from the batch header in A6. `responder_tx` is the crossfire channel
sender for this follower — same channel that delivers client responses via `run_follower_responder`.
No new channel or TCP connection needed.

### A8 — forward.rs: `run_inbound_loop` — batch ACKs + new response format  ✅

**Location:** `forward.rs:918`

Switch from fixed-stride record scanning to length-prefix frame scanning. For each frame:

1. Need `4 + len` bytes complete in buffer.
2. Read `client_id` at offset 4.
3. **`client_id == 0`** → batch ACK:
   - Read `batch_seq` at offset 8 (u64 LE).
   - Call `outbound.ack_batch(batch_seq)`.
   - Do not route to `ClientRegistry`.
4. **`client_id != 0`** → normal response:
   - Slice the full frame `Bytes` (offset 0 to `4 + len`).
   - Route to `ClientRegistry::send_to_partition_async(client_id, frame)` as before.
   - Existing partition bitmap optimization applies here.

### A9 — state_machine.rs: batch deduplication  ✅

**Location:** `state_machine.rs:254` `apply_forwarded_batch_inline()`

Add to durable state:
```rust
last_batch_seq: HashMap<u32 /*node_id*/, u64>,
```

At the top of `apply_forwarded_batch_inline`:
```rust
let view = cmd.as_forwarded_batch();
let node_id    = view.node_id();
let batch_seq  = view.batch_seq();

if self.last_batch_seq.get(&node_id).copied().unwrap_or(0) >= batch_seq {
    return; // already applied — idempotent
}
self.last_batch_seq.insert(node_id, batch_seq);
// ... existing sub-command iteration
```

Include `last_batch_seq` in snapshots (see B8).

---

## Track B: Client Session Reliability

### B1 — forward.rs: `OutboundBuf::try_write` — add `request_seq` parameter  ✅

**Location:** `forward.rs:192`

Change signature:
```rust
pub fn try_write(&self, client_id: u32, request_seq: u64, cmd: &[u8]) -> bool {
```

Update size calculations:
```rust
let entry_size  = (16 + cmd.len()) as u32;   // was 8
let payload_len = (12 + cmd.len()) as u32;   // was 4
```

Update the unsafe write block:
```rust
(dst as *mut u32).write_unaligned(payload_len.to_le());              // @0
(dst.add(4)  as *mut u32).write_unaligned(client_id.to_le());        // @4
(dst.add(8)  as *mut u64).write_unaligned(request_seq.to_le());      // @8  NEW
std::ptr::copy_nonoverlapping(cmd.as_ptr(), dst.add(16), cmd.len()); // @16
```

Update `try_write` callers: `ForwardHandle::try_forward()` and `forward()` must accept
`request_seq: u64` and thread it through to `try_write`.

### B2 — forward.rs: `run_inbound_loop` — length-prefix response parsing  ✅ (covered by A8)

Covered by A8 — the same loop change that handles batch ACKs also switches the response
scanning from fixed-stride to length-prefix. No separate step needed.

### B3 — async_apply.rs: PartitionWorker — new response frame format  ✅

**Location:** `async_apply.rs:~734` TAG_FORWARDED_BATCH handling in the async partition worker

Current: builds tight-packed `[client_id:4][log_index:8]` records.

New — build a full length-prefixed frame per sub-command:
```rust
// view.iter() now yields (client_id: u32, request_seq: u64, sub_cmd_bytes: &[u8])
for (client_id, request_seq, sub_cmd_bytes) in view.iter() {
    let response_payload = Bytes::new(); // empty for write commands; populated for reads
    let inner_len = 4 + 8 + 8 + response_payload.len(); // client_id + req_seq + log_idx + payload
    let mut frame = BytesMut::with_capacity(4 + inner_len);
    frame.put_u32_le(inner_len as u32);
    frame.put_u32_le(client_id);
    frame.put_u64_le(request_seq);
    frame.put_u64_le(log_index);
    frame.put(response_payload);
    // accumulate into per-node_id response buffer as before
}
```

### B4 — async_apply.rs: `ClientPartition` — length-prefix frame scanning  ✅

**Location:** `async_apply.rs:342` `ClientPartition::run()`

Update frame scanning from fixed-stride to length-prefix:
```rust
let mut pos = 0;
while pos + 4 <= buf.len() {
    let len = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize;
    if pos + 4 + len > buf.len() { break; }
    let client_id = u32::from_le_bytes(buf[pos+4..pos+8].try_into().unwrap());
    if client_id as usize % num_partitions == partition_id {
        let frame = buf.slice(pos .. pos + 4 + len);
        invoke_callback(client_id, frame);
    }
    pos += 4 + len;
}
```

Client callbacks receive the full frame `Bytes` and parse `request_seq` at offset 8,
`log_index` at offset 16, `response_bytes` at offset 24.

### B5 — state_machine.rs: `ClientSession` + idempotent apply  ✅

**Location:** `state_machine.rs:254`

Add to durable state:
```rust
struct ClientSession {
    last_applied_seq: u64,
}
client_sessions: HashMap<u32 /*client_id*/, ClientSession>,
```

In `apply_forwarded_batch_inline` sub-command loop (after batch_seq check from A9):
```rust
for (client_id, request_seq, sub_cmd_bytes) in view.iter() {
    if request_seq > 0 {
        let session = self.client_sessions.entry(client_id).or_default();
        if request_seq <= session.last_applied_seq {
            continue; // already applied — skip
        }
    }

    self.engine.apply_command(sub_cmd_bytes, log_index, ...);

    if request_seq > 0 {
        self.client_sessions.entry(client_id)
            .or_default()
            .last_applied_seq = request_seq;
    }
}
```

`request_seq == 0` opts out of session tracking (system/internal commands).

Session GC: remove entries after a configurable idle TTL (measured in log_index distance or
wall clock). Implement lazily during snapshot or on explicit `TAG_DISCONNECT` command.

### B6 — codec.rs: `TAG_RESUME` command  ✅

Add tag constant:
```rust
pub const TAG_RESUME: u8 = 56;  // next after TAG_FORWARDED_BATCH = 55
```

Constructor:
```rust
pub fn resume(session_client_id: u32, last_acked_seq: u64) -> MqCommand {
    let mut buf = BytesMut::with_capacity(16);
    buf.put_u8(Self::TAG_RESUME);
    buf.put_bytes(0, 3);
    buf.put_u32_le(session_client_id);
    buf.put_u64_le(last_acked_seq);
    MqCommand { buf: buf.freeze() }
}
```

View struct:
```rust
pub struct CmdResume<'a> { buf: &'a [u8] }
impl<'a> CmdResume<'a> {
    pub fn session_client_id(&self) -> u32 { u32::from_le_bytes(self.buf[4..8].try_into().unwrap()) }
    pub fn last_acked_seq(&self)    -> u64 { u64::from_le_bytes(self.buf[8..16].try_into().unwrap()) }
}
```

Add `MqCommand::as_resume() -> CmdResume<'_>`.

Update `state_machine.rs` `classify()` to include `TAG_RESUME` as a write command (it reads
replicated state and must serialize through Raft for consistency).

### B7 — state_machine.rs: `TAG_RESUME` handler  ✅ (state machine side; PartitionWorker result propagation deferred)

Inside the sub-command dispatch in `apply_forwarded_batch_inline`:
```rust
if sub_cmd.tag() == MqCommand::TAG_RESUME {
    let view = sub_cmd.as_resume();
    let session_id = view.session_client_id();
    let server_last = self.client_sessions
        .get(&session_id)
        .map(|s| s.last_applied_seq)
        .unwrap_or(0);
    // Record server_last so the async worker (B3) can emit it as response_bytes.
    // Use the same per-log-index result storage that existing read commands use.
    self.resume_results.insert(log_index, (client_id, server_last));
    return; // no engine.apply_command needed
}
```

The async worker reads `resume_results` and encodes `server_last` as the 8-byte `response_bytes`
in the response frame. The client receives this and reconciles its pending command set.

### B8 — state_machine.rs: snapshots  ✅

**Location:** `state_machine.rs` snapshot `build()` and `restore()` (~lines 440-480)

Add to snapshot serialization:
- `last_batch_seq: HashMap<u32, u64>` (from A9)
- `client_sessions: HashMap<u32, u64>` (just `last_applied_seq` per client_id)

These maps are small: `last_batch_seq` has one entry per follower node; `client_sessions` has
one entry per connected client (GC'd by TTL).

### B9 — Client API: `request_seq` tracking + TAG_RESUME on reconnect

The client-facing layer must:

1. **Assign `request_seq`**: per-client `AtomicU64` counter, incremented before each command.
   Pass to `ForwardHandle::forward(client_id, request_seq, cmd)`.

2. **Track unacknowledged commands**: `BTreeMap<u64 /*request_seq*/, PendingCmd>`. Remove on
   response receipt. On timeout or reconnect, retry all pending with the same `request_seq`.

3. **Stable `client_id` across reconnects**: `ClientRegistry` assigns ids starting at 1.
   Add `ClientRegistry::register_with_id(id, callback)` so a reconnecting client can reclaim
   its previous id (same follower). On reconnect to a *different* follower, a new routing
   `client_id` is assigned but the old one is passed in `TAG_RESUME` as `session_client_id`.

4. **Send TAG_RESUME on reconnect**:
   - First command after connection: `MqCommand::resume(old_session_client_id, last_acked_seq)`.
   - Wait for RESUME response containing `server_last_applied_seq`.
   - Discard pending entries with `request_seq <= server_last_applied_seq`.
   - Replay remaining pending commands in order.

---

## Dependency Order & Build Sequence

```
A1 (codec: batch_seq field in TAG_FORWARDED_BATCH)
  └─ A2 (codec: iter yields (client_id, request_seq, cmd_bytes))
       ├─ A3 (drain: accumulate + batch TCP frame)
       │    └─ A4 (UnackedSendBuf)
       │         └─ A5 (resend unacked on reconnect)
       │              └─ A6 (leader: parse batch header)
       │                   └─ A7 (leader: batch ACK after client_write)
       │                        └─ A8 (inbound: ACK detection + length-prefix parse)
       │                             └─ A9 (state machine: batch dedup + snapshots)
       │
       └─ B1 (try_write: request_seq param)   ← safe to start after A2
            └─ B3 (async worker: new response format)
                 └─ B4 (ClientPartition: length-prefix scan)
                      └─ B5 (state machine: ClientSession)
                           └─ B6 (codec: TAG_RESUME)
                                └─ B7 (state machine: TAG_RESUME handler)
                                     └─ B8 (snapshots: last_batch_seq + client_sessions)
                                          └─ B9 (client API)
```

Track A ships independently. Track B requires A2 (iterator change) and A8 (inbound parse) as
foundations before any B step can be tested end-to-end.

---

## Invariants to Preserve

- `client_id == 0` is permanently reserved as the batch ACK sentinel. `ClientRegistry` already
  starts at 1 — do not change this.
- `request_seq == 0` means "no session tracking". Use 0 for internal/system commands.
- `batch_seq` is per-follower node, monotonically increasing, starting at 1. Never 0.
- A batch with `batch_seq <= last_batch_seq[node_id]` in the state machine is always a no-op.
- `OutboundBuf` partition count, `OUTBOUND_SEALED_BIT`, and the CAS loop in `try_write` are
  unchanged. Only `entry_size`/`payload_len` and the unsafe byte-write block change in B1.

Problem 1: Forward TCP Reliability (Follower → Leader)

### The Gap

`OutboundBuf.drain_to_tcp()` works in three phases:
1. Seal partitions, swap in a fresh segment
2. Spin-wait for producers, then **extract** bytes via zero-copy `split_to(committed)`
3. Write to TCP

Once step 2 completes, those bytes are gone from the buffer. If the TCP write in step 3 fails mid-send, those commands are silently lost. The reconnect loop brings the connection back up and resumes draining — but only from new writes, not the extracted-but-lost batch.

### Solution: Batch Sequence Numbers + Unacknowledged Buffer

**At the `OutboundBuf` / drainer level:**
- Before draining, assign a monotonically increasing `batch_seq: u64` (per follower, AtomicU64)
- Prepend it to each drained batch as a tiny 8-byte header
- After extracting the bytes but **before** writing to TCP, push them to a `VecDeque<(batch_seq, Bytes)>` — the "unacknowledged send buffer"
- Write to TCP as normal
- Shrink the deque only when the leader ACKs that `batch_seq`

**Leader side:**
- After successfully `client_write()`-ing the `TAG_FORWARDED_BATCH` to Raft (not on apply — just proposal acceptance), send back a lightweight ACK frame: `[batch_seq:8]` (8 bytes)
- This reuses the existing response TCP path

**On reconnect:**
- Resend everything in the unacknowledged deque first (in order), then resume normal drain
- The deque acts as a bounded retransmit buffer — you can cap it at e.g. 16 MiB and apply backpressure if the leader is too slow ACKing

**Deduplication at the state machine:**
- `TAG_FORWARDED_BATCH` gains a `batch_seq: u64` field in its header (currently `@8 node_id, @12 count`)
- State machine tracks `last_applied_batch_seq: u64` per `node_id`
- On apply: if `batch_seq <= last_applied`, skip (idempotent). This handles the case where the follower resends a batch the leader already proposed but hadn't ACKed yet before the drop.

Wire cost: +8 bytes per drained batch (minimal), +8 bytes per ACK frame.

---

## Problem 2: External Client Reliability (Client → Follower → Leader → Raft)

### The Gaps

There are two distinct failure modes:

**A. Follower→Leader connection is flapping.** A command may have entered `OutboundBuf` but the TCP drop ate it (covered by Problem 1 above, mostly). But the client already got a "forwarded" signal and is waiting for a response that will never come.

**B. Leader dies mid-flight.** The command may have been proposed to Raft but not yet committed (rolled back by new leader election), or it may have been committed but the response never made it back through the chain. The client has no way to know which.

### Solution: Per-Client Request Sequence Numbers + Durable Session State

**Wire format addition** — extend the forwarding sub-frame:
```
Current:  [len:4][client_id:4][cmd_bytes]
Enhanced: [len:4][client_id:4][request_seq:8][cmd_bytes]
```
Eight bytes per command. The `client_id` is already there; `request_seq` is a monotonic counter the client increments for each new command.

**Response wire format** — echo the `request_seq` back:
```
Current:  [client_id:4][log_index:8]          = 12 bytes
Enhanced: [len:4][client_id:4][request_seq:8][log_index:8][response_bytes] = 24 bytes header
```
Now the client knows exactly which request was acknowledged.

**State machine session table** — in durable Raft state (included in snapshots):
```rust
struct ClientSession {
    last_applied_seq: u64,
    // optionally cache last result for exactly-once delivery
}
client_sessions: HashMap<client_id, ClientSession>
```
On apply: if `request_seq <= last_applied_seq`, skip apply and return cached/no-op result. Sessions are garbage collected on explicit client disconnect or TTL expiry.

**Client reconnection protocol** — add a `TAG_RESUME` command:
```
[TAG_RESUME:1][pad:3][client_id:4][last_acked_seq:8]
```
The client sends this as the first command on reconnect. The state machine responds with the current `last_applied_seq` for that client. If `last_applied_seq >= last_acked_seq + N`, the client knows those N commands were applied and can advance its state without retrying. For anything below that threshold, the client retries safely — idempotent apply prevents double-execution.

**Leader failover handling:** Because `client_sessions` lives in the Raft state machine (durable, replicated), it survives leader death. The new leader's state machine already has the correct `last_applied_seq` for every client. A client reconnecting to any follower of the new leader can issue `TAG_RESUME` and get accurate recovery information.

---

## What the Wire Protocol Already Gives Us

The flat binary format is well-positioned for these additions:

- Adding `batch_seq` to `TAG_FORWARDED_BATCH` is a field append at a fixed offset — no structural change to the codec
- Adding `request_seq` to forwarding sub-frames is a fixed-width insert between `client_id` and `cmd_bytes` — existing view structs just shift their field offsets
- The existing `CmdForwardedBatch::iter()` already walks sub-commands — it just needs to yield `request_seq` alongside each one

The hardest part is the `OutboundBuf` retransmit deque: producers write commands without knowing their batch assignment (batching is decided at drain time). So `request_seq` needs to come from the **client layer** (already known at write time), while `batch_seq` is assigned by the drainer. These are separate concerns and separate fields.

---

## Summary Table

| Failure Scenario | Current Behavior | After Fix |
|---|---|---|
| Follower→Leader TCP drops, reconnects | Commands extracted during drain are **lost** | Unacked deque resent on reconnect |
| Follower→Leader TCP drops, batch already in Raft | Would be re-proposed on reconnect → **duplicate apply** | `batch_seq` dedup in state machine |
| Leader dies, command was committed | Response lost, client hangs | `TAG_RESUME` recovers `last_applied_seq` |
| Leader dies, command was not committed | Command lost, client hangs | Client retries with same `request_seq`, idempotent |
| Client reconnects to different follower | No recovery path | `TAG_RESUME` works cluster-wide (Raft state) |
| Client reconnects to same follower | No recovery path | Same as above |

The two systems (forward batch reliability, client session reliability) are independent and can be built in any order. Forward reliability is lower complexity and higher immediate impact since it affects all commands regardless of client behavior.

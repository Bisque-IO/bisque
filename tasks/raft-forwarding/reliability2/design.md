# Reliable Raft Forwarding Transport: ACK/Dedup Design

## Problem Statement

The current forwarding transport provides best-effort delivery: when a TCP connection
is severed mid-write, in-flight batches are silently lost. The follower has no way to
know which batches reached the leader and which were dropped by the OS TCP stack. This
creates a reliability gap between "command accepted by local adapter" and "command
committed to raft log."

Additionally, leader changes introduce two failure modes:
1. The old leader responds with a rejection (not-leader).
2. The connection breaks entirely and the follower must re-enqueue on the new leader.

## Design Goals

- **Guaranteed delivery or failure**: every forwarded command either commits to raft
  and gets ACKed back to the originating follower, or the follower receives a definitive
  failure it can surface to the client.
- **Idempotent retransmission**: connection failures trigger automatic re-send of
  unACKed batches. The leader deduplicates so retransmissions are safe.
- **Transport-layer agnosticism**: the reliability mechanism operates on opaque batches.
  It does not inspect or understand command semantics.
- **Minimal wire format changes**: extend the existing batch header and response frame
  with sequence numbers; no new framing layer.

## Architecture Overview

### Three-Layer Responsibility Model

```
┌─────────────────────────────────────────────────┐
│  Layer 1: Adapter (originating node)            │
│  • Speculative soft rejection                   │
│  • Catches ~90% of bad commands locally         │
│  • Rate limits, local dedup, schema validation  │
│  • Rejects before forwarding → no raft log cost │
├─────────────────────────────────────────────────┤
│  Layer 2: Transport (ForwardClient/Acceptor)    │
│  • Guaranteed delivery-or-failure via ACK/NACK  │
│  • Batch sequence numbers + dedup               │
│  • In-flight buffer + automatic retransmission  │
│  • Agnostic to command content                  │
├─────────────────────────────────────────────────┤
│  Layer 3: State Machine Apply (leader)          │
│  • Authoritative final filter                   │
│  • Topic-level dedup, rate limits, ACLs         │
│  • Partial rejection within a batch             │
│  • Deterministic — all replicas agree           │
└─────────────────────────────────────────────────┘
```

**Why three layers?**

- Layer 1 prevents obvious garbage from consuming raft log space. A follower's local
  state machine replica is a recent-enough approximation for soft checks like rate
  limits and duplicate detection.
- Layer 2 guarantees that commands accepted by Layer 1 either commit or fail
  definitively. The transport doesn't need to understand what the commands mean.
- Layer 3 is the only authoritative decision point. Because all replicas apply the same
  log in the same order, partial rejections during apply are deterministic and
  consistent across the cluster.

### ACK/NACK Mechanism

#### Batch Sequence Numbers

Each `OutboundBatch` gets a monotonically increasing `batch_seq: u64` assigned by the
group drainer. This sequence is per-follower (per `ForwardClient` instance), not
per-connection or per-raft-group.

```rust
struct OutboundBatch {
    batch_seq: u64,
    header: [u8; BATCH_HEADER_LEN],  // [batch_seq:8][raft_group_id:4][payload_len:4]
    frames: SmallVec<[Bytes; 16]>,
}
```

The `batch_seq` is written into the wire format so the leader can echo it back in
ACK/NACK responses.

#### Wire Format Changes

**Batch frame** (follower → leader) — extended header:

```
[batch_seq:8 LE][raft_group_id:4 LE][batch_payload_len:4 LE][sub-frames...]
```

`BATCH_HEADER_LEN` becomes 16 bytes (was 8).

**ACK response** (leader → follower), length-prefixed with tag:

```
[len:4 LE][0x01:1][batch_seq:8 LE][log_id:8 LE]
(len = 17)
```

- Sent after the batch's commands are committed via raft apply.
- `log_id` is the raft log index, useful for follower-side correlation/debugging.

**NACK response** (leader → follower), length-prefixed with tag:

```
[len:4 LE][0x02:1][batch_seq:8 LE][reason:1]
(len = 10)
```

NACK reasons:
- `0x01` — Not leader (leader changed).
- `0x02` — Raft proposal rejected (e.g., membership change in progress).
- `0x03` — Overloaded / backpressure.

**Existing per-command responses** (leader → follower) remain unchanged:

```
[len:4 LE][client_id:4 LE][request_seq:8 LE][log_index:8 LE][response_bytes]
(len = 20 + response_bytes.len())
```

ACK/NACK frames use uniform length-prefixed framing with a tag byte inside the
payload, keeping the outer framing consistent with existing response frames. The tag
byte (`0x01` for ACK, `0x02` for NACK) is the first byte of the payload, distinguishing
them from per-command responses where the first 4 payload bytes are `client_id`.

**Disambiguation rule**: after reading the 4-byte length prefix, check the payload
length. ACK payloads are exactly 17 bytes, NACK payloads are exactly 10 bytes. For
robustness, check the first payload byte: `0x01` = ACK, `0x02` = NACK, otherwise it's
a per-command response (where `client_id` values `1` and `2` are reserved/avoided, or
payload length alone is sufficient for disambiguation since per-command responses are
always `>= 20` bytes).

#### ACK Timing

ACKs are sent **after raft apply returns the log_id**, not on receipt. This means:

1. The batch was proposed to raft.
2. Raft replicated it to a quorum.
3. The state machine applied it (partial rejections may have occurred at this stage).
4. The leader sends the ACK with the resulting log_id.

This is the strongest guarantee: an ACK means the commands are committed and applied.
The downside is higher latency (full raft round-trip), but this is acceptable because:
- The per-command response frames already have this latency.
- ACKs are cheap — just 17 bytes per batch vs. the response frames per command.

#### In-Flight Buffer (Follower Side)

The follower maintains a bounded `VecDeque<OutboundBatch>` of unACKed batches:

```rust
struct InFlightTracker {
    /// Batches sent but not yet ACKed, ordered by batch_seq.
    pending: VecDeque<OutboundBatch>,
    /// Next batch_seq to assign.
    next_seq: u64,
    /// Maximum number of unACKed batches before backpressure.
    max_in_flight: usize,
}
```

**Normal flow**:
1. Group drainer assigns `batch_seq`, pushes batch to in-flight buffer, sends to MPMC.
2. TCP drainer writes batch to wire.
3. Inbound reader receives ACK with `batch_seq`, removes batch from in-flight buffer.

**Connection failure recovery**:
1. TCP connection dies (error or TTL).
2. All batches still in the in-flight buffer are re-enqueued to the MPMC batch queue.
3. New connection picks them up and retransmits.
4. Leader deduplicates by `batch_seq`.

**Backpressure**: when `pending.len() >= max_in_flight`, the group drainer blocks
until an ACK frees a slot. This prevents unbounded memory growth during sustained
leader unresponsiveness.

### Dedup on Leader Side

The leader maintains per-follower dedup state:

```rust
struct FollowerDedup {
    /// High-water mark: all batch_seq <= this value have been seen.
    high_water: u64,
    /// Out-of-order batch_seqs above high_water that have arrived.
    /// Small in practice because TCP preserves order within a connection.
    above_hwm: HashSet<u64>,
}
```

**Dedup algorithm**:
1. Receive batch with `batch_seq` from follower `node_id`.
2. If `batch_seq <= high_water` → duplicate, skip (send ACK immediately with
   previously recorded log_id, or a synthetic ACK).
3. If `batch_seq` is in `above_hwm` → duplicate, skip (send ACK immediately).
4. Otherwise → new batch, process it:
   a. If `batch_seq == high_water + 1` → increment `high_water`, drain any consecutive
      entries from `above_hwm`.
   b. Else → insert into `above_hwm`.
5. Propose batch to raft.
6. On apply, send ACK.

**Why high-water mark + HashSet?**

With MPMC and multiple connections, batches may arrive slightly out of order. The
high-water mark handles the common in-order case with O(1) space. The HashSet handles
the uncommon out-of-order case. Since MPMC re-ordering is bounded by the number of
connections, `above_hwm` stays small (typically < `num_outbound_connections`).

**Lifetime**: dedup state is per follower `node_id`, created on first connection, kept
alive for the duration of the leader's term. On leader change, all dedup state is
discarded (the new leader starts fresh, which is safe because followers reset their
`batch_seq` counters on leader change too).

### Leader Change Handling

Two failure modes collapse to the same recovery path:

**Mode 1: Old leader sends NACK (not-leader)**
- Follower receives NACK with reason `0x01`.
- Follower marks all in-flight batches for retransmission.
- `ForwardClient` discovers new leader via raft membership.
- All unACKed batches are pushed back to the MPMC batch queue.
- New connection to new leader picks them up.
- New leader's dedup state is empty → no false dedup hits.

**Mode 2: Connection breaks**
- TCP error triggers connection exit.
- Same recovery as Mode 1: push unACKed batches back to MPMC queue.
- Reconnect logic discovers new leader.
- Retransmit.

**Follower batch_seq reset on leader change**: when the follower switches to a new
leader, it resets `next_seq` to 0. This is safe because the new leader has a fresh
`FollowerDedup`. It also avoids the pathological case where a follower that has sent
millions of batches to the old leader carries over a huge `batch_seq` to the new
leader, creating a sparse high-water-mark gap.

### Speculative Rejection at Originating Node

Before forwarding, the originating node's adapter layer can perform lightweight
validation against its local state machine replica:

- **Rate limits**: per-topic or per-client publish rate checks.
- **Dedup**: topic-level message dedup (e.g., idempotency keys).
- **Schema validation**: message format checks.
- **Size limits**: max message size, max batch size.

These are "soft" rejections — the local replica may be slightly stale, so edge cases
near limits might pass here but get caught by Layer 3. That's acceptable: the goal is
to prevent obviously bad commands from consuming raft log space, not to provide
authoritative enforcement.

**Implementation**: this is an adapter-level concern, not a transport-level concern.
The `ForwardClient` API remains agnostic. Adapters (MQTT, Kafka, etc.) call their
validation logic before calling `outbound_buf.write()`.

### Partial Rejection During State Machine Apply

When a `TAG_FORWARDED_BATCH` is applied by the state machine, individual sub-commands
may be rejected (e.g., topic doesn't exist, rate limit exceeded, dedup hit). The state
machine:

1. Applies each sub-command in order.
2. For rejected sub-commands, generates an error response (existing per-command
   response frame with an error payload).
3. For accepted sub-commands, generates a success response with `log_index`.
4. After all sub-commands are processed, the batch ACK is sent.

The batch ACK confirms that the batch was *processed*, not that all sub-commands
succeeded. Individual sub-command outcomes are communicated through the existing
per-command response frames. This separation is important:

- The transport layer (Layer 2) cares about: "did the batch reach raft and get
  applied?" → ACK/NACK.
- The application layer cares about: "did my specific command succeed?" → per-command
  response.

## Summary of Wire Format

### Current (no reliability)

```
Follower → Leader:
  [raft_group_id:4 LE][batch_payload_len:4 LE][sub-frames...]
  (BATCH_HEADER_LEN = 8)

Leader → Follower:
  [len:4 LE][client_id:4 LE][request_seq:8 LE][log_index:8 LE][response_bytes]
```

### Proposed (with reliability)

```
Follower → Leader:
  [batch_seq:8 LE][raft_group_id:4 LE][batch_payload_len:4 LE][sub-frames...]
  (BATCH_HEADER_LEN = 16)

Leader → Follower (ACK):
  [len:4 LE = 17][0x01:1][batch_seq:8 LE][log_id:8 LE]

Leader → Follower (NACK):
  [len:4 LE = 10][0x02:1][batch_seq:8 LE][reason:1]

Leader → Follower (per-command response, unchanged):
  [len:4 LE][client_id:4 LE][request_seq:8 LE][log_index:8 LE][response_bytes]
  (len = 20 + response_bytes.len())
```

## Key Design Decisions

1. **ACK after raft apply, not on receipt** — strongest delivery guarantee.
2. **Per-follower batch_seq, not per-connection** — survives connection rotation.
3. **Reset batch_seq on leader change** — avoids stale dedup state issues.
4. **High-water mark + HashSet dedup** — O(1) for in-order, bounded overhead for
   out-of-order.
5. **Transport stays agnostic** — no command inspection in ACK/NACK logic.
6. **Speculative rejection is adapter-level** — not transport's responsibility.
7. **Partial rejection is state-machine-level** — deterministic, consistent across
   replicas.
8. **Uniform length-prefixed framing for ACK/NACK** — simpler parsing, no ambiguity.

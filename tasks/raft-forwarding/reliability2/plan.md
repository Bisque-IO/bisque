# Reliable Raft Forwarding Transport: Implementation Plan

Reference: [design.md](./design.md)

All changes are in `crates/mq/src/forward.rs` unless noted otherwise.

---

## Phase 1: Wire Format & Data Structures

### 1.1 Extend OutboundBatch with batch_seq

- Add `batch_seq: u64` field to `OutboundBatch`.
- Update `BATCH_HEADER_LEN` from 8 to 16.
- Update header encoding in `run_group_drainer`: write `[batch_seq:8 LE][raft_group_id:4 LE][payload_len:4 LE]`.
- Add `AtomicU64` sequence counter to `ForwardClient` (or pass into group drainer).

**Files**: `forward.rs`

### 1.2 ACK/NACK response types

- Define constants:
  ```rust
  const ACK_TAG: u8 = 0x01;
  const NACK_TAG: u8 = 0x02;
  const NACK_NOT_LEADER: u8 = 0x01;
  const NACK_PROPOSAL_REJECTED: u8 = 0x02;
  const NACK_OVERLOADED: u8 = 0x03;
  ```
- Add helper functions:
  ```rust
  fn encode_ack(batch_seq: u64, log_id: u64) -> Bytes
  fn encode_nack(batch_seq: u64, reason: u8) -> Bytes
  ```
- Both produce length-prefixed frames: `[len:4 LE][tag:1][batch_seq:8][...]`.

**Files**: `forward.rs`

### 1.3 Update batch header parsing on leader side

- Update `handle_follower_connection` and `handle_follower_connection_with_channel` to
  parse the new 16-byte batch header: extract `batch_seq` from first 8 bytes.
- Thread `batch_seq` through to the raft proposal so it can be echoed in the ACK.

**Files**: `forward.rs`

---

## Phase 2: Leader-Side Dedup

### 2.1 FollowerDedup struct

- Create `FollowerDedup` with `high_water: u64` and `above_hwm: HashSet<u64>`.
- Add method `fn check_and_record(&mut self, batch_seq: u64) -> bool` (returns true if
  new, false if duplicate).
- Add method `fn advance_hwm(&mut self)` to drain consecutive entries from `above_hwm`.

**Files**: `forward.rs`

### 2.2 Per-follower dedup table on ForwardAcceptor

- Add `dedup: HashMap<u32, FollowerDedup>` (keyed by follower `node_id`) to the
  acceptor's shared state, or pass into each follower connection handler.
- On batch receipt: call `check_and_record(batch_seq)`.
  - If duplicate: send ACK immediately (skip raft proposal).
  - If new: propose to raft, send ACK after apply.

**Files**: `forward.rs`

### 2.3 Dedup state lifecycle

- Create `FollowerDedup` on first batch from a follower.
- Clear all dedup state when the node loses leadership (on raft role change callback
  or when the acceptor shuts down).

**Files**: `forward.rs`, potentially `raft.rs` for leadership change hook

---

## Phase 3: Follower-Side In-Flight Tracking

### 3.1 InFlightTracker struct

```rust
struct InFlightTracker {
    pending: VecDeque<OutboundBatch>,
    next_seq: u64,
    max_in_flight: usize,
}
```

- `assign_seq(&mut self, batch: &mut OutboundBatch)` — sets `batch.batch_seq = self.next_seq++`, pushes clone to `pending`.
- `ack(&mut self, batch_seq: u64)` — removes the matching entry from `pending`.
- `nack_all(&mut self) -> Vec<OutboundBatch>` — drains all pending batches for re-enqueue.
- `is_full(&self) -> bool` — `pending.len() >= max_in_flight`.

**Files**: `forward.rs`

### 3.2 Integrate InFlightTracker with group drainer

- The `InFlightTracker` lives in `forward_client_loop`, shared between the group
  drainer and the inbound ACK reader via `Arc<Mutex<InFlightTracker>>` or a dedicated
  channel.
- **Option A (Mutex)**: group drainer locks to assign seq + push to pending. Inbound
  reader locks to remove on ACK.
- **Option B (channel)**: group drainer sends `(batch_seq, OutboundBatch)` to a tracker
  task. ACK reader sends `batch_seq` to the same tracker task. Tracker task manages
  the VecDeque internally. This avoids lock contention.
- **Recommended**: Option B (channel-based tracker task) for consistency with the
  existing channel-based architecture.

**Files**: `forward.rs`

### 3.3 Backpressure from InFlightTracker

- When the tracker is full (`pending.len() >= max_in_flight`), the group drainer must
  block. With Option B, the tracker task sends a permit/semaphore signal when space
  frees up.
- Use `tokio::sync::Semaphore` with `max_in_flight` permits. Group drainer acquires a
  permit before sending each batch. ACK processing releases a permit.

**Files**: `forward.rs`

### 3.4 Connection failure recovery

- On `ConnectionExit::Error` or `ConnectionExit::TtlExpired`:
  1. Drain all pending batches from `InFlightTracker`.
  2. Push them back to the MPMC batch queue (front-of-queue if possible, otherwise
     append — MPMC channels don't support push-front, so the TCP drainer on the new
     connection will pick them up in arrival order, which is fine because the leader
     deduplicates).
  3. New connection retransmits.

**Files**: `forward.rs`

---

## Phase 4: ACK/NACK Processing

### 4.1 Leader sends ACK after raft apply

- In the existing raft apply path for `TAG_FORWARDED_BATCH`:
  1. After all sub-commands are applied, encode an ACK frame with the `batch_seq`
     (threaded from the proposal) and the `log_id` from the apply result.
  2. Send the ACK frame through the follower's responder channel (same channel used
     for per-command response frames).
- The ACK is sent *in addition to* per-command responses, not instead of them.

**Files**: `forward.rs`, `engine.rs` or `state_machine.rs` (wherever `TAG_FORWARDED_BATCH` apply happens)

### 4.2 Leader sends NACK on rejection

- If the raft proposal is rejected (not-leader, proposal error):
  1. Encode a NACK frame with the `batch_seq` and appropriate reason.
  2. Send through the responder channel.
- If the node detects it is no longer leader before proposing, NACK immediately.

**Files**: `forward.rs`

### 4.3 Follower parses ACK/NACK responses

- Update `run_connection_inbound` to handle ACK/NACK frames:
  1. Read 4-byte length prefix.
  2. If payload length == 17 and first byte == `0x01`: parse ACK, notify tracker.
  3. If payload length == 10 and first byte == `0x02`: parse NACK, trigger recovery.
  4. Otherwise: existing per-command response handling.

**Files**: `forward.rs`

### 4.4 Leader change reset

- When the follower detects a leader change (via NACK reason `0x01` or connection
  failure followed by new leader discovery):
  1. Reset `InFlightTracker.next_seq` to 0.
  2. Drain and re-enqueue all pending batches.
- The new leader starts with fresh `FollowerDedup` state, so batch_seq 0 is safe.

**Files**: `forward.rs`

---

## Phase 5: Configuration & Plumbing

### 5.1 New ForwardConfig fields

```rust
pub struct ForwardConfig {
    // ... existing fields ...
    /// Maximum number of unACKed outbound batches before backpressure.
    /// Default: 1024.
    pub max_in_flight_batches: usize,
}
```

**Files**: `forward.rs`

### 5.2 Thread batch_seq through raft proposal

- The `batch_seq` must survive from TCP receipt through raft proposal and apply so it
  can be echoed in the ACK. Options:
  - **Option A**: Embed `batch_seq` in the `MqCommand` payload for `TAG_FORWARDED_BATCH`.
    This is the simplest — the batch_seq becomes part of the raft log entry.
  - **Option B**: Side-channel map from raft proposal to batch_seq. More complex but
    avoids putting transport metadata in the raft log.
  - **Recommended**: Option A. The 8-byte overhead per forwarded batch in the raft log
    is negligible, and it keeps the ACK path simple (state machine apply reads
    batch_seq from the command, sends ACK through responder channel).

**Files**: `forward.rs`, `codec.rs` (if modifying TAG_FORWARDED_BATCH format),
`state_machine.rs` or `engine.rs` (apply path)

### 5.3 Update TAG_FORWARDED_BATCH encoding

- Extend the `TAG_FORWARDED_BATCH` command to include `follower_node_id` and
  `batch_seq` so the apply path can route the ACK:
  ```
  [TAG_FORWARDED_BATCH:1][follower_node_id:4 LE][batch_seq:8 LE][sub-frames...]
  ```
- The apply path uses `follower_node_id` to look up the responder channel and
  `batch_seq` to construct the ACK.

**Files**: `codec.rs`, `forward.rs`, `engine.rs`/`state_machine.rs`

---

## Phase 6: Tests

### 6.1 Unit tests

- `InFlightTracker`: assign, ack, nack_all, is_full, reset.
- `FollowerDedup`: in-order dedup, out-of-order dedup, high-water advance, duplicate
  detection.
- ACK/NACK encode/decode round-trip.
- Extended batch header encode/decode (16-byte header).

**Files**: `forward.rs` (inline tests)

### 6.2 Integration tests

- **Happy path**: follower sends batches, leader applies, follower receives ACKs,
  in-flight buffer drains.
- **Connection failure**: sever TCP mid-stream, verify follower retransmits unACKed
  batches, leader deduplicates, no duplicate application.
- **Leader change**: simulate leader step-down, verify NACK with not-leader reason,
  follower reconnects to new leader, retransmits, batch_seq resets.
- **TTL rotation**: verify ACKs work correctly across connection rotation with MPMC.
- **Backpressure**: fill in-flight buffer to `max_in_flight`, verify group drainer
  blocks until ACKs arrive.

**Files**: `crates/mq/tests/` (new test file or extend existing)

### 6.3 Benchmark validation

- Run `forward_bench` to verify no performance regression from the added batch_seq
  overhead and ACK processing.

**Files**: `crates/mq/examples/forward_bench.rs`

---

## Implementation Order

```
Phase 1 (wire format)  ──→  Phase 2 (leader dedup)  ──→  Phase 4.1-4.2 (leader ACK/NACK)
                                                                    │
Phase 3 (follower tracker)  ──→  Phase 4.3-4.4 (follower ACK parse + recovery)
                                                                    │
Phase 5 (config + plumbing)  ←──────────────────────────────────────┘
                                    │
                              Phase 6 (tests)
```

Phases 1 and 3 can proceed in parallel. Phase 2 depends on Phase 1 (needs new header
format). Phase 4 depends on both Phase 2 and Phase 3. Phase 5 threads through all
phases. Phase 6 runs last.

---

## Risk Considerations

- **Raft log bloat from batch_seq**: 8 extra bytes per forwarded batch in the raft log.
  Negligible — a batch typically contains many sub-commands (hundreds of bytes to
  kilobytes).
- **Dedup memory on leader**: `FollowerDedup` per follower node. With N followers, each
  with a small `above_hwm` HashSet, memory is O(N × num_connections). Bounded and
  small.
- **In-flight buffer memory on follower**: `max_in_flight_batches` × average batch size.
  With default 1024 and ~1KB batches, this is ~1MB. Configurable.
- **ACK latency**: ACKs wait for full raft apply. This is inherent to the design choice
  of strongest-guarantee ACKs. Not a regression since per-command responses already
  have this latency.
- **Complexity**: the in-flight tracker + dedup + ACK parsing adds ~300-500 lines. The
  channel-based tracker task (Option B in Phase 3.2) keeps the complexity isolated and
  testable.

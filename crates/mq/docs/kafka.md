# Kafka Consumer Group Semantics — Full Implementation Plan

## Goal

Support the full Kafka consumer group contract with **perfect coordinator
recovery on leader failover** — an improvement over Apache Kafka, where failover
forces all consumer group members to re-join.

In bisque, the Raft-replicated state machine already holds all durable state.
By replicating the coordinator's group membership, generation, phase, and
partition assignments through Raft, a new leader can reconstruct a fully
functional `GroupCoordinator` from the applied state and resume serving
heartbeats, offset commits, and fetches without triggering a rebalance.

---

## Architecture

### Kafka's Weakness

In Apache Kafka, the `GroupCoordinator` lives entirely in memory on the
coordinator broker. On coordinator failover:

1. All in-memory group state (members, phase, assignments) is lost.
2. Every consumer gets `NOT_COORDINATOR` on its next heartbeat.
3. All consumers must re-discover the new coordinator and re-join.
4. A full rebalance occurs — partitions are temporarily unassigned.
5. Consumer lag spikes while the rebalance completes.

This is disruptive for large consumer groups (100+ members), where rebalance
can take 30+ seconds.

### bisque's Improvement

bisque replicates group coordinator state through Raft:

1. Group membership, generation, phase, leader, and assignments are
   Raft-replicated in the `ConsumerGroupState` entity.
2. On leader failover, the new leader reconstructs `GroupCoordinator` from
   the Raft state machine's `MqMetadata.consumer_groups`.
3. Consumers whose TCP connections survive (e.g., via a load balancer or
   reconnect to new leader) can continue heartbeating without re-joining.
4. Only consumers whose TCP connections actually break need to re-join.
5. No spurious rebalance — the group stays `Stable` through failover.

The key insight: **the Raft log is already the coordinator's write-ahead log**.
Every mutation to group state goes through Raft anyway (for offset commits),
so extending this to membership and assignments costs almost nothing.

### What Gets Replicated (Raft) vs What Stays Local

| State | Replicated? | Why |
|-------|-------------|-----|
| Group existence, name, config | Yes | Must survive failover |
| Member list (member_id, client_id, protocols) | Yes | Needed for recovery |
| Generation ID | Yes | Needed for offset fencing |
| Phase (Empty/Stable/PreparingRebalance/...) | Yes | Determines coordinator behavior |
| Leader member_id | Yes | Needed for SyncGroup routing |
| Partition assignments (per-member) | Yes | Needed for recovery without rebalance |
| Protocol type and name | Yes | Needed for JoinGroup responses |
| Committed offsets | Yes | Must survive failover |
| `pending_joins` / `pending_syncs` (oneshot channels) | No | Ephemeral — tied to TCP connections |
| `last_heartbeat: Instant` | No | Reset to "now" on recovery |
| `session_timeout_ms` per member | Yes | Needed for session expiry after recovery |
| `next_member_id` counter | Yes | Prevents member_id collisions |

---

## Phase 1: Core Types

### 1.1 New File: `crates/mq/src/consumer_group.rs`

```rust
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Sentinel for Option<u64> atomics: 0 means None.
const NONE_U64: u64 = 0;

// ── Persisted metadata ──────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum AutoOffsetReset {
    Earliest = 0,
    Latest = 1,
    None = 2,
}

impl Default for AutoOffsetReset {
    fn default() -> Self { Self::Latest }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum GroupPhase {
    Empty = 0,
    PreparingRebalance = 1,
    CompletingRebalance = 2,
    Stable = 3,
    Dead = 4,
}

impl Default for GroupPhase {
    fn default() -> Self { Self::Empty }
}

/// Persisted consumer group metadata (snapshot / MDBX).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupMeta {
    pub group_id: u64,
    pub name: String,
    #[serde(default)]
    pub name_hash: u64,
    pub created_at: u64,
    pub generation: i32,
    pub phase: GroupPhase,
    pub protocol_type: String,
    pub protocol_name: String,
    pub leader: Option<String>,
    pub auto_offset_reset: AutoOffsetReset,
    pub last_activity_at: u64,
    pub next_member_id: u64,
    pub members: Vec<GroupMemberMeta>,
}

/// Persisted member state within a consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMemberMeta {
    pub member_id: String,
    pub client_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    /// (protocol_name, metadata_bytes) — subscription metadata per protocol.
    pub protocols: Vec<(String, Vec<u8>)>,
    /// Serialized partition assignment (opaque bytes from SyncGroup).
    #[serde(default)]
    pub assignment: Vec<u8>,
}

/// A committed offset for (group, topic, partition).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupTopicPartitionOffset {
    pub topic_id: u64,
    pub partition_index: u32,
    pub committed_offset: u64,
    pub metadata: Option<String>,
    pub committed_at: u64,
}

/// Snapshot data for a consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupSnapshot {
    pub meta: ConsumerGroupMeta,
    pub offsets: Vec<GroupTopicPartitionOffset>,
}
```

### 1.2 In-Memory State

```rust
/// Lock-free in-memory consumer group state.
///
/// Immutable identity in `meta`. Mutable scalars use atomics.
/// Members and offsets use DashMap/RwLock for concurrent readers.
pub struct ConsumerGroupState {
    pub meta: ConsumerGroupMeta,

    // Atomic mutable scalars
    generation: AtomicU32,          // i32 stored as u32 (reinterpreted)
    phase: RwLock<GroupPhase>,      // infrequent changes, RwLock is fine
    last_activity_at: AtomicU64,
    next_member_id: AtomicU64,

    // Complex mutable state
    leader: RwLock<Option<String>>,
    protocol_name: RwLock<String>,
    members: DashMap<String, GroupMemberState>,

    // Group-keyed offsets: (topic_id, partition_index) → offset
    pub(crate) offsets: DashMap<(u64, u32), GroupTopicPartitionOffset>,

    // Phase transition notification (for async JoinGroup/SyncGroup completion)
    pub(crate) phase_notify: tokio::sync::Notify,

    // Pre-initialized metrics
    m_offset_commits: metrics::Counter,
    m_generation_bumps: metrics::Counter,
    m_rebalances: metrics::Counter,
}

/// In-memory member state.
pub struct GroupMemberState {
    // Immutable identity
    pub member_id: String,
    pub client_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    pub protocols: Vec<(String, Bytes)>,

    // Mutable
    pub assignment: RwLock<Bytes>,
    pub last_heartbeat_at: AtomicU64,  // unix_ms (not Instant — serializable)

    // Join/Sync tracking flags (for phase completion detection)
    pub joined_this_gen: AtomicBool,
    pub synced_this_gen: AtomicBool,
}
```

Key change from the current coordinator: **`last_heartbeat` uses `u64`
unix_ms instead of `std::time::Instant`**. `Instant` cannot be serialized
or replicated. Unix milliseconds can be stored in an `AtomicU64`, replicated
through Raft, and compared across nodes.

### 1.3 MqCommand Tags (`types.rs`)

```rust
pub const TAG_CREATE_CONSUMER_GROUP: u8 = 48;
pub const TAG_DELETE_CONSUMER_GROUP: u8 = 49;
pub const TAG_COMMIT_GROUP_OFFSET: u8 = 50;
pub const TAG_JOIN_CONSUMER_GROUP: u8 = 51;
pub const TAG_SYNC_CONSUMER_GROUP: u8 = 52;
pub const TAG_LEAVE_CONSUMER_GROUP: u8 = 53;
pub const TAG_HEARTBEAT_CONSUMER_GROUP: u8 = 54;
pub const TAG_EXPIRE_GROUP_OFFSETS: u8 = 55;
pub const TAG_EXPIRE_GROUP_SESSIONS: u8 = 56;
```

JoinGroup, SyncGroup, LeaveGroup, and Heartbeat are **Raft-replicated
commands**, not just local coordinator operations. This is the key difference
from Kafka.

### 1.4 MqSnapshotData (`types.rs`)

```rust
pub struct MqSnapshotData {
    // ... existing fields ...
    #[serde(default)]
    pub consumer_groups: Vec<ConsumerGroupSnapshot>,
}
```

### 1.5 MqMetadata (`metadata.rs`)

```rust
pub(crate) consumer_groups: DashMap<u64, ConsumerGroupState>,
pub(crate) consumer_group_names: DashMap<u64, u64>,  // name_hash → group_id
```

---

## Phase 2: Raft-Replicated Group Coordination

### 2.1 The Core Idea

Every group coordinator operation becomes a Raft command:

| Kafka Protocol Op | MqCommand Tag | What It Replicates |
|-------------------|---------------|--------------------|
| JoinGroup | `TAG_JOIN_CONSUMER_GROUP` | Add/update member, transition phase, bump generation |
| SyncGroup | `TAG_SYNC_CONSUMER_GROUP` | Store assignments, transition to Stable |
| Heartbeat | `TAG_HEARTBEAT_CONSUMER_GROUP` | Update `last_heartbeat_at` |
| LeaveGroup | `TAG_LEAVE_CONSUMER_GROUP` | Remove member, maybe trigger rebalance |
| OffsetCommit | `TAG_COMMIT_GROUP_OFFSET` | Store group-keyed offset |

### 2.2 Flow: JoinGroup Through Raft

```
Client                  KafkaHandler              Raft              MqEngine
  |                         |                       |                  |
  |-- JoinGroup req ------->|                       |                  |
  |                         |-- client_write ------>|                  |
  |                         |   (TAG_JOIN_...)      |-- apply -------->|
  |                         |                       |                  |-- update members
  |                         |                       |                  |-- check if all joined
  |                         |                       |                  |-- if yes: bump gen,
  |                         |                       |                  |   set CompletingRebalance
  |                         |                       |<-- MqResponse ---|
  |                         |<-- response ----------|                  |
  |                         |                       |                  |
  |                         |-- check phase ------->| (read from meta) |
  |                         |-- build JoinGroupResp |                  |
  |<-- JoinGroup resp ------|                       |                  |
```

The Kafka adapter's `handle_join_group` submits a Raft command and waits for
the response. The `MqEngine::apply_command` for `TAG_JOIN_CONSUMER_GROUP`:

1. Finds or creates the `ConsumerGroupState`.
2. Inserts/updates the member in the DashMap.
3. If phase is `Empty` or `Stable`, transitions to `PreparingRebalance`.
4. Checks if all known members have pending joins (tracked via `joined_this_gen`
   flag on each member).
5. If all joined: increments generation, selects protocol, picks leader,
   transitions to `CompletingRebalance`, fires `phase_notify`.
6. Returns `MqResponse::GroupJoined { generation, leader, protocol_name, is_leader, members, phase_complete }`.

The Kafka handler translates this into a `JoinGroupResponse`. If the join
phase is not yet complete (not all members have joined), the handler awaits
the `phase_notify` with a timeout.

### 2.3 Flow: SyncGroup Through Raft

1. Leader submits `TAG_SYNC_CONSUMER_GROUP` with assignments.
2. Apply path stores assignments on members, transitions to `Stable`,
   fires `phase_notify`.
3. Returns `MqResponse::GroupSynced { assignment }`.
4. Non-leader members await `phase_notify` until phase becomes `Stable`,
   then read their assignment from the replicated state.

### 2.4 Flow: Heartbeat Through Raft

Heartbeats are high-frequency. Two options:

**Option A: Raft-replicate every heartbeat.**
- Pro: Perfect recovery (exact heartbeat timestamps).
- Con: High Raft write load (N consumers × 1 heartbeat/3s = many log entries).

**Option B: Hybrid — local heartbeat + periodic Raft sync.**
- Heartbeat updates `last_heartbeat_at` locally (atomic store).
- A periodic leader task (every 5s) writes a batch
  `TAG_HEARTBEAT_CONSUMER_GROUP` with all members' timestamps to Raft.
- On failover, timestamps are at most 5s stale. Members get a grace period
  equal to `session_timeout_ms`, so 5s staleness is absorbed.
- Pro: Low Raft overhead.
- Con: Slightly stale heartbeats after failover (but within timeout window).

**Recommendation: Option B.** Heartbeat frequency is 3s by default, session
timeout is 30s. A 5s sync interval means worst-case 5s stale timestamps,
leaving 25s of grace period — more than enough.

### 2.5 Flow: LeaveGroup Through Raft

1. Submit `TAG_LEAVE_CONSUMER_GROUP`.
2. Apply path removes member, possibly transitions phase, fires `phase_notify`.
3. If group becomes empty, optionally remove the group entity.
4. Return `MqResponse::Ok`.

### 2.6 Generation Fencing for Offset Commits

`TAG_COMMIT_GROUP_OFFSET` includes `generation: i32`. Apply path:

```rust
TAG_COMMIT_GROUP_OFFSET => {
    let group = self.meta.consumer_groups.get(&group_id)?;
    let current_gen = group.generation();
    if generation != current_gen {
        return MqResponse::Error(MqError::IllegalGeneration);
    }
    group.offsets.insert((topic_id, partition), offset);
    group.touch_activity(current_time);
    MqResponse::Ok
}
```

---

## Phase 3: Perfect Recovery on Leader Failover

### 3.1 Recovery Procedure

When a new Raft leader is elected, no special recovery is needed. The
`MqMetadata.consumer_groups` DashMap is populated on every node by the Raft
apply path. The Kafka adapter reads directly from this replicated state.

On failover:

1. The new leader's Kafka adapter starts serving requests immediately.
2. Consumer group state (members, phase, generation, assignments) is already
   in the `MqMetadata` from Raft.
3. Members' `last_heartbeat_at` may be slightly stale (up to the heartbeat
   sync interval). On recovery, the session expiry leader task gives a grace
   period by only expiring members whose `last_heartbeat_at` is older than
   `session_timeout_ms` from the **current** time.

### 3.2 What Happens to In-Flight Requests

| Scenario | Outcome |
|----------|---------|
| Group in `Stable`, consumers heartbeating | Consumers reconnect to new leader, heartbeat succeeds. No rebalance. |
| Group in `PreparingRebalance`, pending joins | Pending async waiters time out (TCP connections broke). Members re-join after reconnecting. |
| Group in `CompletingRebalance`, pending syncs | Same — members re-sync after reconnect. |
| Consumer's TCP connection survived failover | Consumer continues heartbeating. No re-join needed. |
| Consumer's TCP connection broke | Consumer reconnects, resumes with same member_id. |

### 3.3 Comparison with Kafka

| Scenario | Kafka | bisque |
|----------|-------|--------|
| Coordinator failover | Full rebalance, all consumers re-join | No rebalance if TCP survives, members resume |
| Consumer group with 100 members | ~30s rebalance storm | 0s if connections survive, individual re-joins otherwise |
| Offset data after failover | Preserved (in __consumer_offsets topic) | Preserved (in Raft state machine) |
| Member assignments after failover | Lost, must recompute | Preserved, no recompute needed |
| Generation continuity | Preserved (in __consumer_offsets) | Preserved (in Raft state) |

---

## Phase 4: Partition Assignment

### 4.1 Server-Side Assignment (Improvement Over Kafka)

In Kafka, the **client** computes partition assignments (consumer group leader
runs the assignor). This means:

- The server cannot enforce assignment policies.
- Buggy or malicious clients can create unfair assignments.
- Server-side rebalance optimizations are impossible.

bisque supports **both** client-side and server-side assignment:

- **Client-side** (Kafka compatibility): The group leader's SyncGroup request
  contains assignments computed by the client. The server stores and
  distributes them.
- **Server-side** (bisque-native): If the SyncGroup leader provides no
  assignments (or if a config flag is set), the server computes assignments
  using a built-in strategy.

### 4.2 Built-in Assignment Strategies

New file: `crates/mq-kafka/src/assignment.rs`

```rust
pub trait PartitionAssignor: Send + Sync {
    fn name(&self) -> &str;
    fn assign(
        &self,
        members: &[AssignorMember],
        topic_partitions: &[(String, Vec<i32>)],
    ) -> Vec<(String, Vec<(String, Vec<i32>)>)>;
    // Returns: member_id → [(topic, [partitions])]
}

pub struct AssignorMember {
    pub member_id: String,
    pub subscribed_topics: Vec<String>,
}

pub struct RangeAssignor;       // Kafka default
pub struct RoundRobinAssignor;  // Even distribution
pub struct StickyAssignor;      // Minimize movement on rebalance
```

### 4.3 Sticky Assignment for Zero-Downtime Rebalance

The `StickyAssignor` reads previous assignments from the Raft-replicated
group state and only moves partitions that must move (e.g., a new consumer
joined). Combined with perfect recovery, this means:

- **Failover**: No rebalance at all (assignments preserved in Raft).
- **Scale-up** (new consumer joins): Only partitions assigned to the new
  consumer are moved. Other consumers keep their partitions.
- **Scale-down** (consumer leaves): Only the departing consumer's partitions
  are redistributed.

---

## Phase 5: Codec (`codec.rs`)

### 5.1 New Command Constructors

```rust
// TAG_CREATE_CONSUMER_GROUP = 48
// [48][name:str][auto_offset_reset:u8]
MqCommand::create_consumer_group(name: &str, auto_offset_reset: AutoOffsetReset)

// TAG_DELETE_CONSUMER_GROUP = 49
// [49][group_id:u64]
MqCommand::delete_consumer_group(group_id: u64)

// TAG_COMMIT_GROUP_OFFSET = 50
// [50][group_id:u64][generation:i32][topic_id:u64][partition:u32]
//     [offset:u64][metadata:opt_str][timestamp:u64]
MqCommand::commit_group_offset(
    group_id: u64,
    generation: i32,
    topic_id: u64,
    partition_index: u32,
    offset: u64,
    metadata: Option<&str>,
    timestamp: u64,
)

// TAG_JOIN_CONSUMER_GROUP = 51
// [51][group_id:u64][member_id:str][client_id:str][session_timeout_ms:i32]
//     [rebalance_timeout_ms:i32][protocol_type:str][protocols_count:u32]
//     [protocol_name:str][protocol_metadata:bytes]...
MqCommand::join_consumer_group(
    group_id: u64,
    member_id: &str,
    client_id: &str,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    protocol_type: &str,
    protocols: &[(&str, &[u8])],
)

// TAG_SYNC_CONSUMER_GROUP = 52
// [52][group_id:u64][generation:i32][member_id:str][assignments_count:u32]
//     [member_id:str][assignment:bytes]...
MqCommand::sync_consumer_group(
    group_id: u64,
    generation: i32,
    member_id: &str,
    assignments: &[(&str, &[u8])],
)

// TAG_LEAVE_CONSUMER_GROUP = 53
// [53][group_id:u64][member_id:str]
MqCommand::leave_consumer_group(group_id: u64, member_id: &str)

// TAG_HEARTBEAT_CONSUMER_GROUP = 54
// [54][group_id:u64][member_id:str][generation:i32]
MqCommand::heartbeat_consumer_group(
    group_id: u64,
    member_id: &str,
    generation: i32,
)

// TAG_EXPIRE_GROUP_OFFSETS = 55
// [55][before_timestamp:u64]
MqCommand::expire_group_offsets(before_timestamp: u64)

// TAG_EXPIRE_GROUP_SESSIONS = 56
// [56][now_ms:u64]
MqCommand::expire_group_sessions(now_ms: u64)
```

### 5.2 New View Structs

One view struct per command for zero-copy field access:

- `CmdCreateConsumerGroup` — `name()`, `auto_offset_reset()`
- `CmdCommitGroupOffset` — `group_id()`, `generation()`, `topic_id()`, `partition_index()`, `offset()`, `metadata()`, `timestamp()`
- `CmdJoinConsumerGroup` — `group_id()`, `member_id()`, `client_id()`, `session_timeout_ms()`, etc.
- `CmdSyncConsumerGroup` — `group_id()`, `generation()`, `member_id()`, `assignments()`
- `CmdLeaveConsumerGroup` — `group_id()`, `member_id()`
- `CmdHeartbeatConsumerGroup` — `group_id()`, `member_id()`, `generation()`

### 5.3 New MqResponse Variants

```rust
MqResponse::GroupJoined {
    generation: i32,
    leader: String,
    member_id: String,
    protocol_name: String,
    is_leader: bool,
    members: Vec<(String, Vec<u8>)>,  // (member_id, protocol_metadata)
    phase_complete: bool,             // true if join phase completed
}

MqResponse::GroupSynced {
    assignment: Vec<u8>,
    phase_complete: bool,             // true if sync phase completed
}
```

---

## Phase 6: Engine Apply Path (`engine.rs`)

### 6.1 TAG_CREATE_CONSUMER_GROUP

```rust
TAG_CREATE_CONSUMER_GROUP => {
    let v = cmd.as_create_consumer_group();
    let name = v.name();
    let name_hash = name_hash(name);

    if self.meta.consumer_group_names.contains_key(&name_hash) {
        return MqResponse::Error(MqError::AlreadyExists {
            entity: EntityKind::ConsumerGroup,
        });
    }

    let id = self.meta.next_id();
    let meta = ConsumerGroupMeta {
        group_id: id,
        name: name.to_string(),
        name_hash,
        created_at: current_time,
        generation: 0,
        phase: GroupPhase::Empty,
        protocol_type: String::new(),
        protocol_name: String::new(),
        leader: None,
        auto_offset_reset: v.auto_offset_reset(),
        last_activity_at: current_time,
        next_member_id: 1,
        members: Vec::new(),
    };

    let state = ConsumerGroupState::new(meta);
    self.meta.consumer_group_names.insert(name_hash, id);
    self.meta.consumer_groups.insert(id, state);
    MqResponse::EntityCreated { id }
}
```

### 6.2 TAG_JOIN_CONSUMER_GROUP

The most complex apply handler. Replicates the full JoinGroup logic:

```rust
TAG_JOIN_CONSUMER_GROUP => {
    let v = cmd.as_join_consumer_group();
    let group_id = v.group_id();
    let member_id = v.member_id();
    let client_id = v.client_id();

    let group = match self.meta.consumer_groups.get(&group_id) {
        Some(g) => g,
        None => return MqResponse::Error(MqError::NotFound {
            entity: EntityKind::ConsumerGroup, id: group_id
        }),
    };

    // Assign member_id if empty
    let actual_member_id = if member_id.is_empty() {
        let id = group.next_member_id();
        group.increment_next_member_id();
        format!("{}-{}", client_id, id)
    } else {
        member_id.to_string()
    };

    // Insert/update member
    group.upsert_member(GroupMemberState {
        member_id: actual_member_id.clone(),
        client_id: client_id.to_string(),
        session_timeout_ms: v.session_timeout_ms(),
        rebalance_timeout_ms: v.rebalance_timeout_ms(),
        protocol_type: v.protocol_type().to_string(),
        protocols: v.protocols(),
        assignment: RwLock::new(Bytes::new()),
        last_heartbeat_at: AtomicU64::new(current_time),
        joined_this_gen: AtomicBool::new(true),
        synced_this_gen: AtomicBool::new(false),
    });

    // Transition phase
    let phase = group.phase();
    if phase == GroupPhase::Empty || phase == GroupPhase::Stable {
        group.set_phase(GroupPhase::PreparingRebalance);
    }

    // Mark this member as "joined" for this generation
    group.mark_joined(&actual_member_id);

    // Check if all members have joined
    let all_joined = group.all_members_joined();
    if all_joined {
        group.bump_generation();
        group.select_and_set_protocol();
        group.elect_leader();
        group.set_phase(GroupPhase::CompletingRebalance);
        group.clear_join_marks();
        group.phase_notify.notify_waiters();
    }

    let is_leader = group.leader() == Some(&actual_member_id);

    MqResponse::GroupJoined {
        generation: group.generation(),
        leader: group.leader().unwrap_or_default().to_string(),
        member_id: actual_member_id,
        protocol_name: group.protocol_name().to_string(),
        is_leader,
        members: if is_leader {
            group.member_protocols()
        } else {
            vec![]
        },
        phase_complete: all_joined,
    }
}
```

### 6.3 TAG_SYNC_CONSUMER_GROUP

```rust
TAG_SYNC_CONSUMER_GROUP => {
    let v = cmd.as_sync_consumer_group();
    let group_id = v.group_id();
    let generation = v.generation();
    let member_id = v.member_id();

    let group = match self.meta.consumer_groups.get(&group_id) {
        Some(g) => g,
        None => return MqResponse::Error(MqError::NotFound { ... }),
    };

    if group.generation() != generation {
        return MqResponse::Error(MqError::IllegalGeneration);
    }

    // If this is the leader with assignments, store them
    let assignments = v.assignments();
    if !assignments.is_empty() {
        for (mid, data) in &assignments {
            group.set_member_assignment(mid, Bytes::from(data.clone()));
        }
        group.set_phase(GroupPhase::Stable);
        group.touch_activity(current_time);
        group.phase_notify.notify_waiters();
    }

    // Mark this member as "synced"
    group.mark_synced(member_id);

    // If all members synced but no assignments provided, use empty
    if group.all_members_synced() && group.phase() != GroupPhase::Stable {
        group.set_phase(GroupPhase::Stable);
        group.phase_notify.notify_waiters();
    }

    let assignment = group.get_member_assignment(member_id)
        .unwrap_or_default();

    MqResponse::GroupSynced {
        assignment: assignment.to_vec(),
        phase_complete: group.phase() == GroupPhase::Stable,
    }
}
```

### 6.4 TAG_HEARTBEAT_CONSUMER_GROUP

```rust
TAG_HEARTBEAT_CONSUMER_GROUP => {
    let v = cmd.as_heartbeat_consumer_group();
    let group_id = v.group_id();
    let member_id = v.member_id();
    let generation = v.generation();

    let group = match self.meta.consumer_groups.get(&group_id) {
        Some(g) => g,
        None => return MqResponse::Error(MqError::NotFound { ... }),
    };

    if group.generation() != generation {
        return MqResponse::Error(MqError::IllegalGeneration);
    }

    if !group.has_member(member_id) {
        return MqResponse::Error(MqError::UnknownMemberId);
    }

    group.update_member_heartbeat(member_id, current_time);
    group.touch_activity(current_time);

    if group.phase() == GroupPhase::PreparingRebalance {
        MqResponse::Error(MqError::RebalanceInProgress)
    } else {
        MqResponse::Ok
    }
}
```

### 6.5 TAG_LEAVE_CONSUMER_GROUP

```rust
TAG_LEAVE_CONSUMER_GROUP => {
    let v = cmd.as_leave_consumer_group();
    let group_id = v.group_id();
    let member_id = v.member_id();

    let group = match self.meta.consumer_groups.get(&group_id) {
        Some(g) => g,
        None => return MqResponse::Ok,  // Idempotent
    };

    group.remove_member(member_id);

    if group.member_count() == 0 {
        group.set_phase(GroupPhase::Empty);
        group.clear_leader();
    } else if group.phase() == GroupPhase::Stable {
        group.set_phase(GroupPhase::PreparingRebalance);
    }

    group.phase_notify.notify_waiters();
    MqResponse::Ok
}
```

### 6.6 TAG_COMMIT_GROUP_OFFSET

```rust
TAG_COMMIT_GROUP_OFFSET => {
    let v = cmd.as_commit_group_offset();
    let group_id = v.group_id();
    let generation = v.generation();

    let group = match self.meta.consumer_groups.get(&group_id) {
        Some(g) => g,
        None => return MqResponse::Error(MqError::NotFound { ... }),
    };

    if group.generation() != generation {
        return MqResponse::Error(MqError::IllegalGeneration);
    }

    group.offsets.insert(
        (v.topic_id(), v.partition_index()),
        GroupTopicPartitionOffset {
            topic_id: v.topic_id(),
            partition_index: v.partition_index(),
            committed_offset: v.offset(),
            metadata: v.metadata().map(|s| s.to_string()),
            committed_at: v.timestamp(),
        },
    );

    group.touch_activity(current_time);
    MqResponse::Ok
}
```

### 6.7 TAG_EXPIRE_GROUP_SESSIONS

Proposed by a leader task. Scans all groups for expired members:

```rust
TAG_EXPIRE_GROUP_SESSIONS => {
    let now_ms = cmd.field_u64(1);

    for entry in self.meta.consumer_groups.iter() {
        let group = entry.value();
        let phase = group.phase();
        if phase == GroupPhase::Dead || phase == GroupPhase::Empty {
            continue;
        }

        let expired_members = group.find_expired_members(now_ms);
        if !expired_members.is_empty() {
            for mid in &expired_members {
                group.remove_member(mid);
            }
            if group.member_count() == 0 {
                group.set_phase(GroupPhase::Empty);
                group.clear_leader();
            } else if group.phase() == GroupPhase::Stable {
                group.set_phase(GroupPhase::PreparingRebalance);
            }
            group.phase_notify.notify_waiters();
        }
    }

    MqResponse::Ok
}
```

### 6.8 TAG_EXPIRE_GROUP_OFFSETS

```rust
TAG_EXPIRE_GROUP_OFFSETS => {
    let before_timestamp = cmd.field_u64(1);

    // Remove groups that have been inactive and have no members
    let to_remove: Vec<u64> = self.meta.consumer_groups.iter()
        .filter(|entry| {
            let g = entry.value();
            g.phase() == GroupPhase::Empty
                && g.last_activity_at() < before_timestamp
        })
        .map(|entry| *entry.key())
        .collect();

    for group_id in to_remove {
        if let Some((_, group)) = self.meta.consumer_groups.remove(&group_id) {
            self.meta.consumer_group_names.remove(&group.meta.name_hash);
        }
    }

    MqResponse::Ok
}
```

---

## Phase 7: Async Join/Sync Completion in the Kafka Adapter

### 7.1 The Problem

The Raft apply path is synchronous and returns `MqResponse`. But JoinGroup
needs to block until **all** members have joined. In the current coordinator,
this is handled with oneshot channels. With Raft replication, we need a
different mechanism.

### 7.2 Solution: Notification-Based Completion

Each `ConsumerGroupState` has a `tokio::sync::Notify` field (`phase_notify`).
The engine apply path calls `notify_waiters()` after phase transitions. The
Kafka handler awaits the notification with a timeout:

```rust
async fn handle_join_group(
    &self,
    req: JoinGroupRequest,
    header: &RequestHeader,
) -> KafkaResponse {
    // Resolve or create group_id
    let group_id = self.resolve_or_create_group(&req.group_id).await;

    // Submit join through Raft
    let cmd = MqCommand::join_consumer_group(
        group_id,
        &req.member_id,
        header.client_id.as_deref().unwrap_or(""),
        req.session_timeout_ms,
        req.rebalance_timeout_ms,
        &req.protocol_type,
        &req.protocols,
    );
    let resp = self.batcher.submit_and_wait(cmd).await;

    match resp {
        MqResponse::GroupJoined {
            generation, leader, member_id, protocol_name,
            is_leader, members, phase_complete,
        } => {
            if phase_complete {
                // All members joined — return immediately
                return build_join_response(...);
            }

            // Not all members joined yet — wait for phase transition
            let deadline = tokio::time::Instant::now()
                + Duration::from_millis(req.rebalance_timeout_ms as u64);

            let group = self.metadata.consumer_groups.get(&group_id);
            if let Some(group) = group {
                loop {
                    tokio::select! {
                        _ = group.phase_notify.notified() => {
                            let phase = group.phase();
                            if phase == GroupPhase::CompletingRebalance
                                || phase == GroupPhase::Stable
                            {
                                return build_join_response_from_state(&group, &member_id);
                            }
                        }
                        _ = tokio::time::sleep_until(deadline) => {
                            return error_response(ErrorCode::RebalanceInProgress);
                        }
                    }
                }
            }

            error_response(ErrorCode::GroupCoordinatorNotAvailable)
        }
        _ => error_response(ErrorCode::GroupCoordinatorNotAvailable),
    }
}
```

SyncGroup follows the same pattern — submit through Raft, await
`phase_notify` if not yet `Stable`.

---

## Phase 8: Leader Tasks (`raft.rs`)

### 8.1 Session Expiry

```rust
// In MqRaftNode::start():
handles.push(self.spawn_leader_task(
    "mq-group-session-expiry",
    Duration::from_secs(5),
    move |raft, _node_id| {
        Box::pin(async move {
            let now = unix_ms();
            let cmd = MqCommand::expire_group_sessions(now);
            let _ = raft.client_write(cmd).await;
        })
    },
));
```

### 8.2 Offset Expiry

```rust
handles.push(self.spawn_leader_task(
    "mq-group-offset-expiry",
    Duration::from_secs(600),  // every 10 minutes
    move |raft, _node_id| {
        let retention_ms = config.group_offset_retention_ms;
        Box::pin(async move {
            let now = unix_ms();
            let before = now.saturating_sub(retention_ms);
            let cmd = MqCommand::expire_group_offsets(before);
            let _ = raft.client_write(cmd).await;
        })
    },
));
```

### 8.3 Config Additions (`config.rs`)

```rust
pub struct MqConfig {
    // ... existing fields ...

    /// How long to retain offsets for empty groups (default: 7 days).
    pub group_offset_retention_ms: u64,

    /// How often to check for expired group offsets (default: 10 minutes).
    pub group_offset_expiry_interval: Duration,

    /// How often to check for expired group sessions (default: 5 seconds).
    pub group_session_expiry_interval: Duration,
}
```

---

## Phase 9: Kafka Adapter Changes (`crates/mq-kafka`)

### 9.1 KafkaHandler Changes

The `GroupCoordinator` field is replaced with direct access to
`MqMetadata.consumer_groups`:

```rust
pub struct KafkaHandler {
    batcher: Arc<MqWriteBatcher>,
    log_reader: Arc<dyn KafkaLogReader>,
    metadata: Arc<MqMetadata>,         // NEW: direct access to replicated state
    partition_map: RwLock<PartitionMap>,
    // coordinator: RwLock<GroupCoordinator>,  // REMOVED
    broker_id: i32,
    advertised_host: String,
    advertised_port: i32,
    // ... metrics ...
}
```

Group operations go through Raft commands instead of the local coordinator:

| Before | After |
|--------|-------|
| `self.coordinator.write().join_group(req)` | `self.batcher.submit(MqCommand::join_consumer_group(...)).await` |
| `self.coordinator.write().sync_group(req)` | `self.batcher.submit(MqCommand::sync_consumer_group(...)).await` |
| `self.coordinator.write().heartbeat(req)` | `self.batcher.submit(MqCommand::heartbeat_consumer_group(...)).await` |
| `self.coordinator.write().leave_group(req)` | `self.batcher.submit(MqCommand::leave_consumer_group(...)).await` |

Heartbeats can optionally remain local (hybrid approach) — see Phase 2.4.

### 9.2 OffsetCommit/OffsetFetch

Replace the `name_hash` hack:

```rust
// Before:
let consumer_id = name_hash(&req.group_id);
let cmd = MqCommand::commit_offset(topic_id, consumer_id, offset);

// After:
let group_id = self.resolve_group_id(&req.group_id);
let generation = req.generation_id;
let cmd = MqCommand::commit_group_offset(
    group_id, generation, topic_id, partition_index, offset, metadata, timestamp,
);
```

OffsetFetch reads directly from
`metadata.consumer_groups.get(&group_id).offsets`:

```rust
fn handle_offset_fetch(&self, req: OffsetFetchRequest) -> KafkaResponse {
    let group_id = self.resolve_group_id(&req.group_id);

    for topic_data in &req.topics {
        for &partition in &topic_data.partitions {
            let topic_id = self.resolve_topic(&topic_data.topic_name, partition);
            let offset = self.metadata.consumer_groups.get(&group_id)
                .and_then(|g| g.offsets.get(&(topic_id, partition as u32)))
                .map(|o| o.committed_offset as i64)
                .unwrap_or(-1);
            // ...
        }
    }
}
```

### 9.3 Auto-Offset-Reset

In the fetch handler, when no committed offset exists:

```rust
let offset = match group_offset {
    Some(o) => o,
    None => {
        let reset = group.map(|g| g.auto_offset_reset())
            .unwrap_or(AutoOffsetReset::Latest);
        match reset {
            AutoOffsetReset::Earliest => {
                self.log_reader.get_topic_tail(topic_id).unwrap_or(0)
            }
            AutoOffsetReset::Latest => {
                self.log_reader.get_topic_head(topic_id).unwrap_or(0)
            }
            AutoOffsetReset::None => {
                return partition_error(ErrorCode::OffsetOutOfRange);
            }
        }
    }
};
```

### 9.4 Session Expiry

The adapter no longer needs its own session expiry loop. Session expiry is
handled by the `TAG_EXPIRE_GROUP_SESSIONS` leader task in `raft.rs`, which
goes through Raft and is applied on all nodes.

The `expire_sessions()` call in `server.rs` is removed.

### 9.5 Connection Disconnect

When a TCP connection closes, the adapter must submit a
`TAG_LEAVE_CONSUMER_GROUP` for each member associated with that connection:

```rust
pub fn on_disconnect(&self, member_ids: &[String]) {
    for member_id in member_ids {
        if let Some(group_id) = self.resolve_member_group(member_id) {
            let cmd = MqCommand::leave_consumer_group(group_id, member_id);
            self.batcher.submit_fire_and_forget(cmd);
        }
    }
}
```

---

## Phase 10: State Machine (`state_machine.rs`)

### 10.1 Structural Kinds

Add `StructuralKind::CreateConsumerGroup` and
`StructuralKind::DeleteConsumerGroup`. These cause MDBX persistence for
snapshot durability.

### 10.2 Non-Structural Commands

`TAG_JOIN_CONSUMER_GROUP`, `TAG_SYNC_CONSUMER_GROUP`,
`TAG_LEAVE_CONSUMER_GROUP`, `TAG_HEARTBEAT_CONSUMER_GROUP`,
`TAG_COMMIT_GROUP_OFFSET`, `TAG_EXPIRE_GROUP_SESSIONS`,
`TAG_EXPIRE_GROUP_OFFSETS` are non-structural — they mutate in-memory state
that is captured during snapshots.

---

## Phase 11: Snapshot/Restore (`engine.rs`)

### 11.1 Snapshot

```rust
let consumer_groups = self.meta.consumer_groups.iter()
    .map(|entry| {
        let group = entry.value();
        ConsumerGroupSnapshot {
            meta: group.snapshot_meta(),
            offsets: group.offsets.iter()
                .map(|e| e.value().clone())
                .collect(),
        }
    })
    .collect();
```

### 11.2 Restore

```rust
for cg in data.consumer_groups {
    let id = cg.meta.group_id;
    let name_hash = cg.meta.name_hash;
    let state = ConsumerGroupState::from_snapshot(cg.meta, cg.offsets);
    self.meta.consumer_group_names.insert(name_hash, id);
    self.meta.consumer_groups.insert(id, state);
}
```

---

## Phase 12: Metrics

Pre-initialized at construction time (per project convention):

```
mq.consumer_group.count              (Gauge)    — in MqMetadata
mq.consumer_group.offset_commits     (Counter)  — per-group
mq.consumer_group.generation_bumps   (Counter)  — per-group
mq.consumer_group.rebalances         (Counter)  — per-group
mq.consumer_group.member_count       (Gauge)    — per-group
mq.consumer_group.sessions_expired   (Counter)  — in engine apply
mq.consumer_group.offsets_expired    (Counter)  — in engine apply
kafka.join_group.requests            (Counter)  — in KafkaHandler
kafka.sync_group.requests            (Counter)  — in KafkaHandler
kafka.offset_commit.requests         (Counter)  — in KafkaHandler
kafka.offset_fetch.requests          (Counter)  — in KafkaHandler
```

---

## Phase 13: Migration and Backward Compatibility

### 13.1 Snapshot Compatibility

`MqSnapshotData.consumer_groups` has `#[serde(default)]`. Old snapshots
deserialize with an empty vec.

### 13.2 Old Offset Migration

Old per-consumer offsets in `TopicState::consumer_offsets` (keyed by
`name_hash(group_name)`) become orphaned. They are harmless and will be
cleaned up by existing purge mechanisms. No explicit migration needed.

### 13.3 TAG_COMMIT_OFFSET Coexistence

`TAG_COMMIT_OFFSET` (tag 3) continues to work for non-Kafka consumers
(AMQP, MQTT, SQS adapters). The two offset paths are independent:

- `TAG_COMMIT_OFFSET` → `TopicState::consumer_offsets` (per-consumer)
- `TAG_COMMIT_GROUP_OFFSET` → `ConsumerGroupState::offsets` (per-group)

---

## Implementation Order

| Batch | Scope | Files | Depends On |
|-------|-------|-------|------------|
| 1 | Core types + state | `consumer_group.rs`, `types.rs`, `metadata.rs` | — |
| 2 | Codec | `codec.rs` | Batch 1 |
| 3 | Engine apply + snapshot | `engine.rs`, `state_machine.rs` | Batch 2 |
| 4 | Leader tasks + config | `raft.rs`, `config.rs` | Batch 3 |
| 5 | Kafka adapter rewrite | `handler.rs`, `server.rs` | Batch 4 |
| 6 | Assignment strategies | `assignment.rs` | Batch 5 |
| 7 | Tests | All test files | Batch 6 |

---

## Summary: Why This Is Better Than Kafka

| Feature | Apache Kafka | bisque |
|---------|-------------|--------|
| Coordinator failover | Full rebalance storm | No rebalance (assignments preserved) |
| Recovery time (100 members) | ~30s | ~0s (TCP reconnect time only) |
| Offset storage | `__consumer_offsets` compacted topic | Raft state machine (DashMap) |
| Generation fencing | Yes | Yes (identical semantics) |
| Partition assignment | Client-side only | Client-side or server-side |
| Sticky rebalance | Client must implement | Server-side with Raft-backed history |
| Session expiry | Local timer, lost on failover | Raft-replicated timestamps |
| Offset expiry | `offsets.retention.minutes` | `group_offset_retention_ms` |
| Auto-offset-reset | `auto.offset.reset` config | `AutoOffsetReset` per-group |
| Consistency | Eventual (async replication) | Strong (Raft consensus) |

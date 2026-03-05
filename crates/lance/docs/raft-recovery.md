# Raft & Recovery

bisque-lance replicates all mutations through Raft consensus, providing strong
consistency across a cluster. An optional MDBX manifest provides
crash-consistent metadata for fast recovery without full log replay.

## Raft Integration

### LanceRaftNode

`LanceRaftNode` wires the `BisqueLance` engine and `LanceStateMachine` into
a Raft group and runs background leader-duty tasks.

```rust
let raft_node = Arc::new(
    LanceRaftNode::new(raft, engine, node_id)
        .with_group_id(group_id)
        .with_write_batcher(WriteBatcherConfig::default())
        .with_applied_watermark(watermark)
        .with_manifest(manifest)
        .with_seal_check_interval(Duration::from_secs(5))
        .with_flush_check_interval(Duration::from_secs(10))
        .with_compaction_check_interval(Duration::from_secs(60))
);
raft_node.start();  // Spawn background tasks
```

### Background Tasks

All background tasks are leader-only and shut down cleanly via `Notify`:

| Task | Interval | Purpose |
|------|----------|---------|
| **Leader watcher** | Event-driven | Detect leadership changes, trigger `recover_flush()` |
| **Seal checker** | 5s | Check all tables for seal thresholds (age/size) |
| **Flush orchestrator** | 10s | Copy sealed segments to S3, propose promotion |
| **Compaction loop** | 60s | Optimize fragment layout across all tiers |

### Write API

```rust
// Write through Raft (with optional batching)
let result = raft_node.write_records("events", &batches).await?;
// result.log_index = 42 (for read-after-write fencing)

// DDL through Raft
raft_node.create_table("events", &schema).await?;
raft_node.drop_table("events").await?;

// Direct Raft proposal
let result = raft_node.propose(command).await?;
```

### Error Handling

```rust
pub enum WriteError {
    NotLeader { leader_id: Option<u64>, leader_node: Option<BasicNode> },
    Encode(String),
    Raft(String),
    Fatal(String),
}
```

`NotLeader` responses include the current leader's address for client-side
forwarding.

## State Machine

### LanceStateMachine

Implements openraft's `RaftStateMachine` trait. Dispatches Raft log entries
to the appropriate table engine.

```rust
// Basic state machine
let sm = LanceStateMachine::new(engine.clone());

// With all features
let sm = LanceStateMachine::new(engine.clone())
    .with_catalog_events(catalog_bus)
    .with_manifest(manifest, group_id)
    .with_purge_floor(purge_floor);

// With async apply (returns watermark for read-fencing)
let (sm, watermark) = LanceStateMachine::with_async_apply(
    engine.clone(), AsyncApplyConfig::default()
);
```

### Command Processing

Each Raft log entry is dispatched based on its `LanceCommand` variant:

| Command | Processing |
|---------|-----------|
| `CreateTable` | Create table in engine, persist to manifest, emit `TableCreated` event |
| `DropTable` | Drain async buffer, drop table, remove from manifest, emit `TableDropped` |
| `AppendRecords` | Skip if already in S3; record log index; apply to active segment or enqueue to async buffer |
| `SealActiveSegment` | Drain async buffer; rotate active &rarr; sealed; compact sealed; create indices; emit `SegmentSealed` |
| `BeginFlush` | Mark flush in progress (leader bookkeeping) |
| `PromoteToDeepStorage` | Update catalog with S3 version; clear sealed segment; update purge floor; emit `SegmentPromoted` |

### Recovery Skip Optimization

During log replay after a crash, `AppendRecords` entries that have already
been promoted to S3 are skipped:

```
if log_index < table.min_safe_log_index() {
    // Data already in S3 — skip re-applying to active segment
    return;
}
```

This avoids re-materializing data that's already in deep storage, making
recovery proportional to the amount of un-flushed data rather than total
history.

## MDBX Manifest

### Purpose

The MDBX manifest provides crash-consistent metadata so that recovery can
restore from the last committed state rather than replaying the entire Raft
log.

### MDBX Layout

Per Raft group at `{base_dir}/.lance_groups/{group_id}/`:

| MDBX Table | Key | Value | Purpose |
|------------|-----|-------|---------|
| `lance_meta` | `b"raft"` | bincode `GroupMeta` | Last applied log ID, membership |
| `lance_tables` | table name (UTF-8) | bincode `PersistedTableEntry` | Full table state |
| `catalog_wal` | u64 big-endian seq | bincode `CatalogEvent` | Event log for client delta sync |

### GroupMeta

Flattened Raft metadata for MDBX storage:

```rust
struct GroupMeta {
    last_applied_term: u64,
    last_applied_node_id: u64,
    last_applied_index: u64,
    last_applied_present: bool,
    membership_bytes: Vec<u8>,  // bincode-encoded StoredMembership
}
```

### PersistedTableEntry

Complete table state for crash recovery:

```rust
pub struct PersistedTableEntry {
    pub config: PersistedTableConfig,        // Seal thresholds, S3 URI, indices, etc.
    pub catalog: SegmentCatalog,             // Active/sealed segment IDs, S3 version
    pub flush_state: FlushState,             // Idle or InProgress
    pub schema_history: Vec<SchemaVersion>,  // Schema evolution history
}
```

### Worker Thread Architecture

A dedicated `std::thread` owns all MDBX environments and handles all writes:

```
State Machine          Worker Thread              MDBX
     │                      │                       │
     │── send_update() ──►  │                       │
     │   (fire-and-forget)  │── recv (50ms batch) ─ │
     │                      │── coalesce updates ── │
     │── send_update() ──►  │                       │
     │                      │── commit_batch() ────►│
     │                      │                       │── fsync
     │                      │◄─ done ──────────────│
```

**Batching**: The worker blocks for up to 50ms, then drains up to 4096
additional commands. Per-group updates are coalesced — the latest metadata
and latest per-table entry win within a batch.

**Reads**: Concurrent read-only MDBX transactions via `Arc<GroupMdbxEnv>`.
Reads do not go through the worker thread.

**Durability**: `send_update_durable()` waits for the fsync to complete
before returning. Used during snapshot preparation.

## Catalog WAL

The catalog WAL stores `CatalogEvent` records keyed by monotonic sequence
numbers. This enables client delta sync — clients connect with
`?since=last_seq` and receive only missed events.

### Event Types

```rust
pub enum CatalogEventKind {
    TableCreated { table: String, schema_ipc: Vec<u8> },
    TableDropped { table: String },
    ActiveVersionBumped { table: String, version: u64 },
    SegmentSealed { table: String, active_version: u64, sealed_version: u64 },
    SegmentPromoted { table: String, s3_manifest_version: u64 },
}
```

### WAL Compaction

After each WAL append, if total WAL size exceeds 16 MiB, oldest entries
are removed:

```rust
const WAL_MAX_BYTES: u64 = 16 * 1024 * 1024;

// Walk from oldest entry, accumulate bytes until under limit
fn compact_wal_to_size(max_bytes: u64) {
    let total = wal_size_bytes();
    if total <= max_bytes { return; }
    // Delete oldest entries until remaining fits
}
```

Clients whose `last_seq` falls before the compacted region receive a
`snapshot_required` signal and must perform a full catalog fetch.

### CatalogEventBus

Real-time broadcast of catalog mutations via `tokio::sync::broadcast`
(bounded 1024 entries):

```rust
let bus = Arc::new(CatalogEventBus::new(0));

// Subscribe for real-time events
let mut rx = bus.subscribe();

// Publish (called from state machine)
let event = bus.publish(CatalogEventKind::TableCreated { ... });
// event.seq is monotonically increasing
```

Events published by the bus are also persisted to the MDBX WAL for
offline client sync.

## Crash Recovery

### Recovery Flow

1. **Read MDBX** — `applied_state()` reads `GroupMeta` from MDBX
   (last_applied, membership)
2. **Restore tables** — Read all `PersistedTableEntry` records, call
   `engine.restore_from_persisted_entries()` to reconstruct table state
3. **Resume Raft** — Raft core replays log entries after `last_applied`
4. **Apply with skip** — `AppendRecords` entries already promoted to S3 are
   skipped via `min_safe_log_index` check
5. **Recover flushes** — On becoming leader, `recover_flush()` resumes any
   incomplete S3 uploads from `FlushState::InProgress`

### Recovery Layers

| Layer | Durability | Recovery Speed |
|-------|-----------|---------------|
| **Raft log** (mmap) | Full | Slow (replays entire log) |
| **MDBX manifest** | Full | Fast (restores last committed state) |
| **In-memory** | None | Fastest (first boot only) |

Without MDBX, recovery replays the entire Raft log. With MDBX, recovery
restores from the last committed MDBX state and replays only entries since
`last_applied`. This makes recovery time proportional to the lag between
MDBX commits and the crash point.

### Snapshot Install

When a follower is too far behind for log replay:

1. Leader calls `get_snapshot_builder()`:
   - Drains async buffer to ensure Lance is caught up
   - Flushes manifest durably (waits for fsync)
   - Computes `min_safe_log_index` across all tables
   - Returns `SnapshotData` with all table entries

2. Snapshot is transmitted to follower

3. Follower calls `install_snapshot()`:
   - Shuts down existing tables
   - Restores tables from `PersistedTableEntry` data
   - Bulk-writes snapshot state to MDBX (atomic transaction)
   - Resumes log replay from snapshot's `last_applied + 1`

### Flush Recovery

If the leader crashes during an S3 flush:

1. New leader is elected
2. `on_become_leader()` calls `recover_flush()` on all tables
3. For tables with `FlushState::InProgress`:
   - Resume the S3 upload from checkpoint
   - On success, propose `PromoteToDeepStorage` via Raft
4. This ensures no orphaned flush states persist indefinitely

## Version Pinning

`VersionPinTracker` prevents compaction from deleting dataset versions that
remote clients are actively reading.

### Lifecycle

1. Client connects via WebSocket &rarr; `create_session()` returns session ID
2. Client reads dataset version N &rarr; `pin(session_id, PinKey { table, tier, version: N })`
3. Compaction checks `min_pinned_version(table, tier)` before cleanup
4. Client sends heartbeat every 10s &rarr; `heartbeat(session_id)` renews lease
5. Client disconnects &rarr; `remove_session(session_id)` releases all pins
6. Background reaper &rarr; `reap_expired()` cleans up sessions without
   heartbeat for 30 seconds

### Pin Key

```rust
pub struct PinKey {
    pub table: String,
    pub tier: PinTier,  // Active or Sealed
    pub version: u64,
}
```

### Safety Guarantee

Compaction and cleanup operations call `min_pinned_version(table, tier)` to
determine the oldest version that must be preserved. Versions at or above
this threshold are never deleted.

## Log Purging

The Raft log can be safely purged up to the `min_safe_log_index` — the
minimum first log index across all tables' hot and warm segments:

```
min_safe = min(
    table_a.active_first_log_index,
    table_a.sealed_first_log_index,
    table_b.active_first_log_index,
    ...
)
```

Once data is promoted to S3 via `PromoteToDeepStorage`, the sealed segment's
first log index is cleared, potentially allowing older log entries to be
purged.

The purge floor is shared between the state machine and log storage via
`Arc<AtomicU64>` with release/acquire ordering.

## Raft Command Types

```rust
pub enum LanceCommand {
    CreateTable {
        table_name: String,
        schema_ipc: Bytes,                    // Arrow IPC-encoded schema
    },
    DropTable {
        table_name: String,
    },
    AppendRecords {
        table_name: String,
        data: Bytes,                          // Arrow IPC-encoded RecordBatches
    },
    SealActiveSegment {
        table_name: String,
        sealed_segment_id: SegmentId,
        new_active_segment_id: SegmentId,
        reason: SealReason,                   // MaxAge or MaxSize
    },
    BeginFlush {
        table_name: String,
        segment_id: SegmentId,
    },
    PromoteToDeepStorage {
        table_name: String,
        segment_id: SegmentId,
        s3_manifest_version: u64,
    },
}

pub enum LanceResponse {
    Ok,
    Error(String),
}
```

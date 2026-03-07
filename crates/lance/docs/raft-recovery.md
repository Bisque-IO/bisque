# Raft & Recovery

bisque-lance replicates all mutations through Raft consensus, providing strong
consistency across a cluster. An optional MDBX manifest provides
crash-consistent metadata for fast recovery without full log replay. Fresh
nodes joining the cluster receive segment files via a dedicated TCP streaming
protocol, with a transfer guard system that prevents file deletion race
conditions during the transfer.

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
     │                      │◄─ done ───────────────│
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

### Snapshot Install with Segment File Streaming

When a follower is too far behind for log replay (or is a fresh node joining
the cluster), Raft installs a snapshot. This is a two-phase process: metadata
via Raft, then segment files via a dedicated TCP stream.

#### Phase 1: Leader Builds Snapshot

`get_snapshot_builder()` prepares the snapshot on the leader:

1. **Drain async buffer** — ensures all pending writes are applied to Lance
2. **Flush manifest durably** — MDBX fsync so metadata is crash-consistent
3. **Pin purge floor** — sets purge floor to `last_applied` so Raft log
   entries needed for post-snapshot catchup are not purged during the transfer
4. **Build file manifest** — walks all `tables/*/segments/*.lance/` directories,
   enumerating every file with its relative path and size
5. **Acquire snapshot transfer guard** — prevents file deletion during transfer
   (see [Snapshot Transfer Guard](#snapshot-transfer-guard) below)
6. **Return builder** with table entries, file manifest, and sync server address

`build_snapshot()` serializes this into a `SnapshotData` payload:

```rust
pub struct SnapshotData {
    pub tables: HashMap<String, PersistedTableEntry>,
    pub min_safe_log_index: Option<u64>,
    pub file_manifest: Vec<SnapshotFileEntry>,  // files to stream
    pub sync_addr: Option<String>,               // leader's sync server
}
```

#### Phase 2: Follower Installs Snapshot

`install_snapshot()` runs on the follower:

1. **Decode snapshot** — deserialize `SnapshotData` from bincode
2. **Shutdown existing engine** — clean slate for the restore
3. **Stream segment files from leader** — connect to `sync_addr` via the
   BSYN protocol and download all files in `file_manifest` to local disk
4. **Verify transfer** — post-sync verification checks every transferred
   file exists with the correct size; verification failures abort the install
5. **Restore tables** — reconstruct `TableEngine` instances from
   `PersistedTableEntry` data (segment files are now on disk)
6. **Update purge floor** — restore normal computation from table catalogs
7. **Bulk-write to MDBX** — atomic transaction persists the snapshot state
   for crash consistency
8. **Resume Raft** — log replay continues from `last_applied + 1`

#### Post-Snapshot Log Catchup

After snapshot install, the follower is at the leader's `last_applied` point
but may be behind by entries committed during the transfer. Raft automatically
replays these entries:

- The purge floor was pinned to `last_applied` during `get_snapshot_builder()`,
  so all log entries from that point forward are retained on the leader
- The follower applies these entries through normal `apply()`, bringing it
  fully up to date
- `AppendRecords` entries for data already in S3 are skipped via the
  `min_safe_log_index` optimization (no duplicate work)
- Once caught up, the follower participates normally in the Raft group

This ensures zero data loss even when the transfer takes minutes — all writes
committed during the transfer are replayed from the log after file sync
completes.

## Segment Sync Protocol

### Architecture

The segment sync system streams Lance dataset files directly from the
leader's disk to the follower's disk with zero intermediate copies. This
is necessary because Raft snapshots carry only metadata — the actual Lance
columnar data (active and sealed segments) must be transferred out-of-band.

```
Leader                              Follower
  |                                    |
  |-- Raft InstallSnapshot(metadata) ->|
  |                                    |-- connect to sync_addr -->|
  |                                    |<-- BSYN handshake --------|
  |<--- file manifest ----             |
  |--- stream file 1 data ------------>|
  |--- stream file 2 data ------------>|
  |--- ...                             |
  |--- end sentinel ------------------>|
  |                                    |-- verify files on disk
  |                                    |-- restore tables from metadata
  |                                    |-- resume Raft log replay
```

### BSYN Protocol

A framed TCP protocol for streaming files between leader and follower.

**Request** (follower &rarr; leader):
```
[4B magic: "BSYN"]
[4B version: u32 LE]
[4B manifest_len: u32 LE]
[manifest_len bytes: bincode-encoded Vec<SnapshotFileEntry>]
```

**Response** (leader &rarr; follower, per file):
```
[2B path_len: u16 LE]
[path_len bytes: UTF-8 relative path]
[8B file_len: u64 LE]
[file_len bytes: raw file data, streamed in 256 KB chunks]
```

**End sentinel**: `[2B path_len: 0]`

**Missing files**: If a file in the manifest no longer exists on the leader
(e.g., promoted to S3 during the transfer), the server sends `file_len = 0`
and the client records it as missing. This is not an error — the data is
safely in deep storage.

### Security

All paths are validated on both server and client sides before any filesystem
operations:

- **Path traversal prevention** — `validate_relative_path()` rejects paths
  containing `..` components, absolute paths (`/`, `\`), and null bytes
- **Server-side validation** — prevents a malicious manifest from reading
  files outside the data directory
- **Client-side validation** — prevents a compromised leader from writing
  files outside the follower's data directory
- **TLS support** — optional TLS/mTLS for encrypted transfers (feature-gated)

### SyncResult

Every sync operation returns a detailed result for observability:

```rust
pub struct SyncResult {
    pub files_requested: usize,
    pub files_transferred: usize,
    pub files_missing: usize,        // promoted to S3 during transfer
    pub bytes_transferred: u64,
    pub missing_paths: Vec<String>,
    pub verification_failures: Vec<String>,  // size mismatches
}
```

Post-sync verification checks every transferred file exists on disk with
the expected size from the manifest. Verification failures cause the
snapshot install to abort with an error.

### Server Lifecycle

The `SegmentSyncServer` runs as a background task on the leader:

- Binds to a configured address and accepts TCP connections
- Each connection is handled in a spawned task (concurrent clients supported)
- Shutdown signal stops accepting new connections; in-flight transfers
  complete to avoid leaving followers with partial data
- Client disconnects mid-transfer are handled gracefully — the spawned task
  logs the error and continues accepting new connections

### Configuration

```rust
// Server (leader)
let server = SegmentSyncServer::new(SegmentSyncServerConfig {
    data_dir: data_dir.to_path_buf(),
    bind_addr: "0.0.0.0:9876".parse().unwrap(),
});

// Client (follower, configured on state machine)
let client = SegmentSyncClient::new(SegmentSyncClientConfig {
    data_dir: data_dir.to_path_buf(),
});
```

## Snapshot Transfer Guard

### Problem

The leader continues processing Raft commands while a snapshot transfer is
in flight. Two commands can delete segment files that are being streamed:

| Command | Deletion | Risk |
|---------|----------|------|
| `PromoteToDeepStorage` | `remove_dir_all` on sealed segment directory | **HIGH** — most common |
| `DropTable` | `remove_dir_all` on entire table directory | **MEDIUM** — less frequent |

If either executes between manifest build and transfer completion, the
follower receives incomplete data and the snapshot install fails.

### Solution: Deferred Deletion

`SnapshotTransferGuard` defers all segment/table file deletions while any
snapshot transfer is active, then flushes them when the last transfer
completes.

```rust
pub struct SnapshotTransferGuard {
    active_count: AtomicUsize,              // concurrent transfer count
    deferred_deletions: Mutex<Vec<PathBuf>>, // paths queued for deletion
    active_watermarks: Mutex<Vec<u64>>,      // purge floor watermarks per transfer
}
```

### Guard Lifecycle

```
T0: get_snapshot_builder()    -> guard.acquire()        [active_count: 0->1]
T1: build_snapshot()          -> snapshot serialized
T2: Raft sends snapshot to follower
T3: apply_promote() arrives   -> defer_or_delete(path)  [path queued, NOT deleted]
T4: follower streams files    -> files still on disk
T5: guard handle dropped      -> active_count: 1->0, flush queue
T6: deferred paths deleted
```

### RAII Handle

`acquire()` returns a `SnapshotGuardHandle` with these safety properties:

- **Panic-safe**: `impl Drop` releases the guard during stack unwinding
- **Exactly-once release**: `AtomicBool` flag prevents double-release from
  Drop, explicit `release()`, and timeout task
- **Timeout safety net**: background task auto-releases after 10 minutes if
  the handle is leaked (e.g., follower crashes and never connects)
- **Concurrent transfers**: multiple handles can be active simultaneously
  (e.g., multiple nodes joining at once); deletions are deferred until ALL
  handles are released

```rust
// In apply_promote():
let segment_path = self.config.segment_path(segment_id);
if segment_path.exists() {
    if !self.snapshot_guard.defer_or_delete(segment_path.clone()) {
        tokio::fs::remove_dir_all(&segment_path).await?;
    }
}

// In drop_table():
let table_dir = self.config.table_data_dir(name);
if table_dir.exists() {
    if !self.snapshot_guard.defer_or_delete(table_dir.clone()) {
        tokio::fs::remove_dir_all(&table_dir).await?;
    }
}
```

### What Is Deferred vs. Immediate

Only the filesystem deletion is deferred. All other state changes happen
immediately:

| Operation | Immediate | Deferred |
|-----------|-----------|----------|
| Catalog update (sealed = None, S3 version) | Yes | |
| Sealed dataset handle dropped | Yes | |
| Purge floor updated | Yes | |
| `remove_dir_all` on segment/table dir | | Yes |
| Table removed from registry | Yes | |

This means the Raft state is always correct and consistent — deferred files
are harmless orphans that get cleaned up after the transfer.

### Guard Handle Storage

The state machine maintains a `Vec<SnapshotGuardHandle>` to keep handles
alive for the duration of each snapshot transfer:

```rust
// In get_snapshot_builder():
self.snapshot_guards.retain(|h| !h.is_released());  // prune completed
let handle = self.engine.snapshot_guard()
    .acquire(Duration::from_secs(600), watermark);  // watermark = last_applied index
self.snapshot_guards.push(handle);
```

Handles are released by:
1. **Timeout** (10 min) — the background task fires and releases
2. **Drop** — when the state machine is dropped or the Vec is cleared
3. **Explicit release** — via `handle.release()` if needed

### Edge Cases Handled

| Scenario | Behavior |
|----------|----------|
| Multiple concurrent transfers | All deletions deferred until last handle released |
| Promote during active transfer | Path queued, files survive until transfer completes |
| Drop table during active transfer | Path queued, directory survives |
| Follower never connects (crash) | 10-min timeout auto-releases guard, flushes deletions |
| Panic during snapshot handling | `Drop` impl releases guard during stack unwinding |
| Path already deleted externally | `do_release()` silently skips missing paths |
| `release()` + `Drop` + timeout race | `AtomicBool` ensures exactly one release per handle |
| Promote overwrites purge floor during transfer | `min_watermark()` caps floor at earliest active snapshot point |
| Concurrent snapshots with different watermarks | Floor stays at minimum until all transfers complete |

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

### Purge Floor Pinning During Snapshots

When `get_snapshot_builder()` runs, the purge floor is pinned to the
snapshot's `last_applied` index. This prevents Raft from purging log entries
that the follower will need to replay after the snapshot transfer completes:

```
                    pinned purge floor
                          |
  [purged] ... [log N] [log N+1] ... [log M] [log M+1] ...
                  ^                     ^         ^
          snapshot.last_applied    committed   new writes
                                  during       during
                                  transfer     transfer
```

Without pinning, the leader could purge entries between `last_applied` and
the current head, leaving the follower unable to catch up after receiving
the snapshot. Once `install_snapshot()` completes, the purge floor is
restored to normal computation based on table catalogs.

### Watermark-Aware Purge Floor

The purge floor is computed as:

```
effective_floor = min(
    compute_min_safe_log_index(),   // per-table hot/warm constraint
    guard.min_watermark()           // active snapshot transfer constraint
)
```

Each `SnapshotGuardHandle` stores the `last_applied` index (watermark) at
the time the snapshot was built. `min_watermark()` returns the minimum
across all active handles. This prevents a critical race condition:

**Without watermark tracking** (the bug):
```
T0: Snapshot 1 built at log 500 -> floor = 500
T1: PromoteToDeepStorage applied -> update_purge_floor() -> floor = 700 (OVERWRITES 500!)
T2: openraft purges logs 500-699
T3: Follower finishes file sync, needs logs 501+ -> UNAVAILABLE
T4: Leader must send another full snapshot (wasted transfer)
```

**With watermark tracking** (the fix):
```
T0: Snapshot 1 built at log 500 -> guard.acquire(watermark=500) -> min_watermark() = 500
T1: PromoteToDeepStorage applied -> update_purge_floor()
    -> effective = min(700, 500) = 500  (watermark prevents overwrite!)
T2: openraft purge capped at 499
T3: Follower finishes file sync, replays logs 501+ -> SUCCESS
T4: Guard handle dropped -> min_watermark() = None -> floor relaxes to 700
```

This also handles concurrent snapshots correctly:
```
T0: Snapshot 1 at log 500 -> min_watermark() = 500
T1: Snapshot 2 at log 700 -> min_watermark() = min(500, 700) = 500
T2: Transfer 1 completes  -> min_watermark() = 700
T3: Transfer 2 completes  -> min_watermark() = None
```

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

# Multi-Table Transactions

bisque-lance supports atomic multi-table transactions that span any number of
tables and any number of Raft log entries. Transactions use a chunked
prepare-then-commit protocol built on top of Raft consensus.

## Motivation

Single-command writes (AppendRecords, DeleteRecords, UpdateRecords) each target
one table and are atomic in isolation. Many workloads require atomicity across
multiple tables — for example, inserting an order and decrementing inventory, or
ingesting a batch of OpenTelemetry data that spans logs, metrics, and traces
tables simultaneously.

Multi-table transactions provide:

- **Atomicity**: All operations across all tables succeed or none do.
- **Large payload support**: Data is streamed as chunks across multiple Raft log
  entries, avoiding the need to fit an entire transaction in a single entry.
- **Serialized apply**: Operations are applied in chunk-sequence order,
  making rollback a simple dataset handle swap.

## Protocol Overview

```
Client                        Raft                    State Machine
  │                            │                           │
  │── TxnChunk(id, seq=0) ──►  │── replicate ────────────► │── buffer (inert)
  │── TxnChunk(id, seq=1) ──►  │── replicate ────────────► │── buffer (inert)
  │── TxnChunk(id, seq=2) ──►  │── replicate ────────────► │── buffer (inert)
  │                            │                           │
  │── TxnCommit(id, n=3) ───►  │── replicate ────────────► │── reassemble
  │                            │                           │── drain async buffers
  │                            │                           │── snapshot datasets
  │                            │                           │── apply all ops
  │                            │                           │── persist metadata
  │                            │                           │── emit catalog events
  │◄── TxnResult ───────────── │◄── response ──────────────│
```

A transaction consists of three phases:

1. **Chunk accumulation** — The client sends one or more `TxnChunk` commands
   through Raft. Each chunk contains a partial batch of operations. On apply,
   the state machine buffers the chunk in memory. No Lance writes occur. The
   data is durable in the Raft log the moment the chunk is committed.

2. **Commit** — The client sends a `TxnCommit` command. The state machine
   reassembles all chunks in sequence order, applies operations sequentially,
   and on success persists metadata for all affected tables.

3. **Abort (optional)** — The client sends a `TxnAbort` command to discard
   buffered chunks without applying. This is optional — abandoned transactions
   are garbage-collected automatically.

## Command Types

```rust
enum LanceCommand {
    // ... existing variants ...

    /// Accumulate a chunk of transaction data. Inert on apply — just buffers.
    TxnChunk {
        txn_id: TxnId,
        seq: u32,
        ops: Vec<TxnOp>,
    },

    /// Commit: reassemble all chunks and apply atomically.
    TxnCommit {
        txn_id: TxnId,
        total_chunks: u32,
    },

    /// Abort: discard buffered chunks without applying.
    TxnAbort {
        txn_id: TxnId,
    },
}
```

### TxnId

A `TxnId` is a 128-bit unique identifier (ULID) generated client-side. It
correlates all chunks belonging to the same transaction. No Raft round-trip is
needed to "begin" a transaction — the ID is purely local until the first chunk
is proposed.

### TxnOp

Individual operations within a transaction chunk:

```rust
enum TxnOp {
    /// Append IPC-encoded RecordBatches to a table.
    Append { table: Arc<str>, data: Bytes },

    /// Soft-delete rows matching a SQL filter predicate.
    Delete { table: Arc<str>, filter: String },

    /// Delete matching rows then append replacement data.
    Update { table: Arc<str>, filter: String, data: Bytes },

    /// Create a new table with the given IPC-encoded schema.
    CreateTable { table: Arc<str>, schema_ipc: Bytes },

    /// Drop a table and all its data.
    DropTable { table: Arc<str> },
}
```

Operations within a transaction are applied in chunk-order (seq 0, 1, 2, ...),
and within each chunk in array order. This gives the client full control over
operation ordering.

## State Machine Buffering

The state machine maintains a staging area for in-flight transactions:

```rust
struct PendingTxns {
    txns: HashMap<TxnId, PendingTxn>,
    /// Total bytes across all pending transactions (for global cap).
    total_bytes: usize,
}

struct PendingTxn {
    /// Chunks indexed by sequence number.
    chunks: BTreeMap<u32, Vec<TxnOp>>,
    /// Raft log index of the first chunk (for GC).
    first_log_index: u64,
    /// Total buffered bytes for this transaction.
    total_bytes: usize,
}
```

On `TxnChunk` apply:
1. Check per-txn and global byte caps. If exceeded, reject the chunk.
2. If overwriting an existing chunk at the same `seq`, subtract the old chunk's
   bytes before adding the new ones. (Note: the cap check is evaluated
   *before* subtraction, so overwrites near the cap limit may be conservatively
   rejected even if the net usage would fit.)
3. Insert ops into `pending_txns[txn_id].chunks[seq]`.
4. Update byte accounting on both `PendingTxn` and `PendingTxns`.
5. Return `LanceResponse::Ok` immediately.

No locks are acquired. No Lance writes occur. The chunk is inert.

## Commit Apply Flow

When `TxnCommit` is applied:

### 1. Remove and validate

Remove the `PendingTxn` from the staging area. If the transaction is not found
(never started, already committed, aborted, or GC'd), return an error.

Verify all chunks are present: sequence numbers `0..total_chunks` must all
exist in the `PendingTxn.chunks` map. If any are missing, the transaction
fails with an error response.

### 2. Reassemble

Flatten all chunks in sequence order (BTreeMap ascending key order) into a
single `Vec<TxnOp>`. The ops are applied in this flat order — they are NOT
grouped by table. This preserves the client's intended operation ordering
across tables.

### 3. Drain async apply buffers

For every affected table, drain the `AsyncApplyBuffer` to ensure all pending
asynchronous appends are materialized before the transaction modifies the
table. This reuses the existing `drain_table()` mechanism already used for
single-table deletes and updates.

### 4. Snapshot dataset handles

For each affected table **that already exists**, capture the current `Dataset`
handles for all three tiers (active, sealed, S3) via `Arc` clones (cheap
reference count bumps). These snapshots are the rollback points. All three
tiers must be captured because deletes and updates propagate to all tiers via
`apply_delete`.

Tables that will be *created* by the transaction are not in the engine yet and
thus have no snapshot. They are tracked separately for rollback (see step 6).

```rust
struct DatasetSnapshot {
    active: Option<Dataset>,
    sealed: Option<Dataset>,
    s3: Option<Dataset>,
}

let snapshots: HashMap<Arc<str>, DatasetSnapshot> = affected_tables
    .iter()
    .filter_map(|t| {
        engine.require_table(t).ok()
            .map(|te| (t.clone(), te.dataset_snapshot()))
    })
    .collect();
```

### 5. Apply operations

Operations are split into two phases:

**Phase A — Non-destructive ops** (Append, Delete, Update, CreateTable):
Applied in chunk-sequence order. Newly created tables are tracked in a
`tables_created` list. If any operation fails, proceed to rollback (step 6).

**Phase B — Destructive ops** (DropTable): Applied last, after all
non-destructive ops have succeeded. This ordering guarantees that a failed
non-drop op never leaves a table irrecoverably deleted.

For each operation:
- **Append**: Decode IPC, call `TableEngine::apply_append(batches)`
- **Delete**: Call `TableEngine::apply_delete(filter)` on all tiers
- **Update**: Call `TableEngine::apply_delete(filter)` then
  `TableEngine::apply_append(batches)`
- **CreateTable**: Create via `BisqueLance::create_table()`
- **DropTable**: Drop via `BisqueLance::drop_table()` (deferred to phase B)

### 6. Rollback on failure

On any error during step 5:

1. **Restore existing table snapshots**: For each table that existed before the
   transaction, restore all three tier `Dataset` handles (active, sealed, S3)
   from the snapshots captured in step 4.

2. **Drop newly created tables**: For each table created during the transaction
   (tracked in `tables_created`), call `engine.drop_table()` to undo the
   creation.

3. **DropTable ops were never applied**: Because DropTable ops are deferred to
   phase B, they are never applied if a phase A op fails. The targeted tables
   remain intact.

Rollback is safe because:

- **Lance is append-only**: Failed appends write orphan fragment files that
  are cleaned up later by `cleanup_old_versions()`. The old manifest version
  remains valid.
- **Deletion vectors are independent**: Failed deletes write deletion vector
  files that can be ignored by reverting to the prior manifest version.
- **Dataset handles are cheap**: Swapping back the `Arc<Dataset>` is an
  atomic pointer swap under a brief write lock.

Return `LanceResponse::Error(msg)` to the client.

### 7. Persist metadata atomically

On success, ALL table metadata updates and WAL events are bundled into a single
`ManifestCommand::ApplyBatch` and sent via `send_update_durable()`. This
guarantees:

- All table updates (Set for modified tables, Remove for dropped tables) land
  in a single MDBX write transaction.
- Catalog WAL events are atomically committed alongside the table state.
- The durable send blocks until the MDBX worker thread commits and fsyncs,
  ensuring persistence before the state machine returns.

```rust
let cmd = LanceManifestManager::build_apply_batch(
    group_id,
    &last_applied,
    &last_membership,
    table_updates,   // Vec<TableUpdate>
    wal_events,      // Vec<CatalogEvent>
);
manifest.send_update_durable(cmd).await?;
```

The regular Raft apply path's fire-and-forget `send_update()` at the call site
sends a meta-only update (harmless duplicate — the batch already wrote the
authoritative meta).

### 8. Emit catalog events

Before the durable persist in step 7, catalog events are published to the
`CatalogEventBus` for each applied operation:

- **Append / Delete / Update** → `CatalogEventKind::DataMutated` (with current
  tier versions)
- **CreateTable** → `CatalogEventKind::TableCreated` (with schema IPC)
- **DropTable** → `CatalogEventKind::TableDropped`

The events are included in the `ApplyBatch` command as WAL events, so they are
atomically persisted to MDBX alongside the table metadata (step 7).

No catalog events are emitted on rollback (failed transactions).

## Crash Recovery

Two crash scenarios:

### Crash before MDBX commit

The MDBX metadata still reflects the pre-transaction state. Some tables may
have partially applied Lance writes (new fragments, deletion vectors), but
their persisted `SegmentCatalog` versions point to the pre-transaction
manifests.

On recovery:

1. The state machine restores from MDBX: each table opens at its persisted
   dataset version via `Dataset::checkout(version)`. Partially written
   fragments from the failed transaction are ignored (orphans).
2. Raft replays the log from `last_applied`. The `TxnChunk` entries
   re-buffer into the staging area.
3. The `TxnCommit` entry re-applies the entire transaction on fresh
   (pre-transaction) dataset state. This time it succeeds cleanly.
4. Orphan fragments from the first failed attempt are cleaned up by the next
   `cleanup_old_versions()` run.

### Crash after MDBX commit

All metadata is consistent. All Lance datasets were successfully written
before the MDBX commit. On recovery, the state machine restores the
post-transaction state directly. No re-apply needed for this transaction.

### Why this works

The key invariant: **`cleanup_old_versions()` never runs inline during
transaction apply**. It is triggered by the compaction background task, which
only runs on the leader and is paused during state machine apply. This
guarantees that pre-transaction manifest versions and their fragments remain
on disk throughout the transaction and recovery window.

## Abort

When `TxnAbort` is applied:

1. Remove the `PendingTxn` from the staging area.
2. Return `LanceResponse::Ok`.

No Lance operations are performed. The buffered chunks contained only
serialized data, not materialized writes, so there is nothing to undo.

Aborting a transaction that was already committed (or GC'd, or never started)
is a silent no-op — it returns `LanceResponse::Ok`.

## Snapshot Install

When a snapshot is installed (e.g., a new node joining the cluster), all
pending transactions are cleared (`pending_txns.clear()`). In-flight
transactions that accumulated chunks before the snapshot are silently lost.
Clients will timeout and retry.

## Garbage Collection of Abandoned Transactions

A client may send chunks but never commit (client crash, network partition,
bug). The state machine garbage-collects abandoned transactions:

- Each `PendingTxn` records the Raft log index of its first chunk
  (`first_log_index`).
- Periodically during apply (e.g., every 1000 entries), the state machine
  evicts any `PendingTxn` where
  `current_log_index - first_log_index > txn_timeout_log_entries`.
- The default timeout is configurable (e.g., 100,000 log entries).
- GC is deterministic across all nodes since it is based on Raft log index,
  not wall-clock time.

## Interaction with Async Apply Buffer

Multi-table transactions **must not** use the async apply path. All operations
within a transaction are applied synchronously so that:

1. Failures are detected immediately for rollback.
2. Dataset snapshots accurately reflect pre/post transaction state.
3. The metadata persist captures the true final state.

The existing `drain_table()` call before applying operations ensures the
async buffer is flushed for affected tables before the transaction begins.
Other tables' async appends continue unaffected.

## Interaction with the Flush Pipeline

The 3-phase flush pipeline (`BeginFlush` → S3 upload → `PromoteToDeepStorage`)
operates on sealed segments, not the active segment. Multi-table transactions
write exclusively to the active segment. These two systems are independent and
do not interact. Normal Raft serialization ensures no flush commands interleave
with a transaction's commit apply.

## Interaction with Version Pinning

During transaction commit, dataset versions advance for each affected table.
Version pins on pre-transaction versions remain valid — Lance does not delete
old manifests while pins are held. The `VersionPinTracker` continues to
function unchanged since it tracks versions per-table independently.

## Memory Pressure

Buffered transaction chunks consume memory. Two safeguards:

1. **Per-transaction cap**: If a `PendingTxn` exceeds a configurable byte
   limit (default: 256 MB), subsequent `TxnChunk` commands for that
   transaction fail, and the client should abort.
2. **Global cap**: If total pending bytes across all transactions exceed a
   configurable limit (default: 1 GB), new `TxnChunk` commands are rejected
   until space is freed via commit, abort, or GC.

These limits are checked at `TxnChunk` apply time. Since the check is
deterministic (based on byte counts visible to all nodes), all replicas make
the same accept/reject decision.

When overwriting an existing chunk (same `txn_id` and `seq`), the cap check is
evaluated *before* subtracting the old chunk's bytes. This means a retry near
the cap limit may be conservatively rejected even if the net usage would fit.

## Client API

```rust
impl LanceRaftNode {
    /// Generate a new transaction ID. Local only — no Raft round-trip.
    pub fn begin_txn(&self) -> TxnId {
        TxnId::new()
    }

    /// Send a chunk of transaction operations through Raft.
    pub async fn txn_chunk(
        &self,
        txn_id: TxnId,
        seq: u32,
        ops: Vec<TxnOp>,
    ) -> Result<WriteResult, WriteError> {
        let cmd = LanceCommand::TxnChunk { txn_id, seq, ops };
        self.propose(cmd).await
    }

    /// Commit the transaction. Triggers reassembly and atomic apply.
    pub async fn txn_commit(
        &self,
        txn_id: TxnId,
        total_chunks: u32,
    ) -> Result<WriteResult, WriteError> {
        let cmd = LanceCommand::TxnCommit { txn_id, total_chunks };
        self.propose(cmd).await
    }

    /// Abort the transaction. Discards buffered chunks.
    pub async fn txn_abort(&self, txn_id: TxnId) -> Result<WriteResult, WriteError> {
        let cmd = LanceCommand::TxnAbort { txn_id };
        self.propose(cmd).await
    }
}
```

### Usage Example

```rust
let node: &LanceRaftNode = ...;

// Begin (local, no Raft)
let txn_id = node.begin_txn();

// Chunk 0: insert into orders
let ops0 = vec![TxnOp::Append {
    table: "orders".into(),
    data: encode_ipc(&order_batches),
}];
node.txn_chunk(txn_id, 0, ops0).await?;

// Chunk 1: update inventory
let ops1 = vec![TxnOp::Update {
    table: "inventory".into(),
    filter: "product_id = 42".to_string(),
    data: encode_ipc(&updated_inventory),
}];
node.txn_chunk(txn_id, 1, ops1).await?;

// Commit — atomic apply of both tables
node.txn_commit(txn_id, 2).await?;
```

## Wire Format (Binary Codec)

Three new discriminants extend the existing binary codec:

| Command | Discriminant | Format |
|---------|-------------|--------|
| `TxnChunk` | 12 | `[1B disc][16B txn_id][4B seq][4B n_ops][ops...]` |
| `TxnCommit` | 13 | `[1B disc][16B txn_id][4B total_chunks]` |
| `TxnAbort` | 14 | `[1B disc][16B txn_id]` |

Each `TxnOp` within a chunk is encoded as:

| Op | Discriminant | Format |
|----|-------------|--------|
| `Append` | 0 | `[1B disc][2B name_len][name][4B data_len][IPC data]` |
| `Delete` | 1 | `[1B disc][2B name_len][name][4B filter_len][filter]` |
| `Update` | 2 | `[1B disc][2B name_len][name][4B filter_len][filter][4B data_len][data]` |
| `CreateTable` | 3 | `[1B disc][2B name_len][name][4B schema_len][schema]` |
| `DropTable` | 4 | `[1B disc][2B name_len][name]` |

The `TxnChunk` codec supports `decode_from_bytes` for zero-copy extraction
of IPC payloads, matching the existing `AppendRecords` optimization.

## Limitations

- **Blind writes only**: Operations within a transaction cannot read results
  of prior operations. There is no "read table A, conditionally write table B"
  within the same transaction. For read-write transactions, clients should use
  optimistic concurrency: read versions, compute locally, submit a transaction
  with version preconditions (future enhancement).

- **Active segment only**: Transactions write to the active Lance segment.
  They do not directly interact with sealed or S3 segments (deletes still
  propagate to all tiers as usual via `apply_delete`).

- **Single Raft group**: Transactions are scoped to a single Raft group. All
  tables within the group share the same Raft log, so cross-table atomicity
  is guaranteed by Raft's sequential apply. Cross-group transactions are not
  supported.

- **No long-lived transactions**: Transaction chunks must be committed before
  the GC timeout (configurable, default ~100k log entries). There is no
  mechanism for transactions spanning minutes or hours.

- **DropTable is deferred**: `DropTable` ops are applied last (after all other
  ops succeed) because dropping a table deletes its data directory, making
  rollback impossible. If a `DropTable` op itself fails after non-destructive
  ops have already been applied, the non-destructive changes are rolled back
  but the overall transaction is reported as failed.

- **CreateTable / DropTable are idempotent**: For Raft replay safety,
  `create_table()` returns the existing table if it already exists and
  `drop_table()` returns `Ok` if the table is already gone (also cleaning up
  any orphaned data directory). This means replay after a crash never fails
  due to duplicate creates or drops.

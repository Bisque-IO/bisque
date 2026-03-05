# Storage Engine

bisque-lance organizes data into independent tables, each managed by a
`TableEngine` that drives data through three storage tiers. The multi-table
registry is held by `BisqueLance`, the top-level engine.

## Storage Tiers

Each table's data flows through three tiers:

```
┌─────────────────┐    seal     ┌─────────────────┐   flush    ┌─────────────────┐
│  Active Segment │  ────────►  │  Sealed Segment │  ───────►  │  Deep Storage   │
│  (local NVMe)   │             │  (local NVMe)   │            │  (S3 / Lance)   │
│  read + write   │             │  read-only      │            │  read-only      │
└─────────────────┘             └─────────────────┘            └─────────────────┘
```

**Active Segment** — A local Lance dataset on NVMe accepting writes. Each
table has exactly one active segment identified by a monotonically increasing
`SegmentId`. All Raft-replicated `AppendRecords` commands write here.

**Sealed Segment** — The previous active segment, now read-only. At most one
sealed segment exists per table at any time. Once sealed, indices are
automatically created (FTS, vector, BTree, RTree) based on table
configuration. The sealed segment awaits flush to deep storage.

**Deep Storage** — An S3-backed Lance dataset serving as the long-term archive.
Data is merged into the deep storage dataset during flush, and old sealed
segment files are removed after a successful `PromoteToDeepStorage` Raft
command.

Queries see all three tiers simultaneously via `UNION ALL` in DataFusion.

## Segment Lifecycle

### Sealing (Active &rarr; Sealed)

Sealing is triggered when the active segment exceeds a configurable threshold:

| Trigger | Config | Default |
|---------|--------|---------|
| Age | `seal_max_age` | 60 seconds |
| Size | `seal_max_size` | 1 GB |

The leader's background seal checker runs every 5 seconds. When a threshold is
crossed, it proposes `SealActiveSegment` via Raft. On all nodes:

1. The current active dataset becomes the sealed dataset
2. A new empty active dataset is created with the next `SegmentId`
3. The catalog is atomically updated
4. `active_bytes` and `active_created_at` are reset
5. A `SegmentSealed` catalog event is emitted

The sealed segment is never empty — `should_seal()` returns `None` if the
active segment has zero bytes.

### Indexing (Sealed Segments)

After sealing, indices specified in the table configuration are automatically
created on the sealed segment. Supported index types:

```rust
IndexSpec::fts("text_column")        // Full-text search (inverted index)
IndexSpec::vector("embedding")       // Vector search (IVF-HNSW-SQ)
IndexSpec::btree("timestamp")        // Scalar range queries (B-tree)
IndexSpec::rtree("location")         // Spatial queries (R-tree)
```

Index creation happens in the background and does not block reads.

### Flushing (Sealed &rarr; S3)

Flushing is leader-only. The flush orchestrator runs every 10 seconds:

1. **BeginFlush** — Leader proposes via Raft to mark flush in progress
2. **Execute** — Leader reads all batches from the sealed dataset and writes
   them to S3 (appending to the existing deep storage dataset, or creating it)
3. **PromoteToDeepStorage** — Leader proposes via Raft with the new S3
   manifest version. All nodes update their catalog, clear the sealed dataset,
   and delete local sealed segment files

If the leader crashes during flush, the new leader calls `recover_flush()` on
all tables to resume incomplete operations.

### Compaction

Compaction optimizes fragment layout within each tier independently:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `compaction_target_rows_per_fragment` | 1,000,000 | Target fragment size |
| `compaction_materialize_deletions` | true | Rewrite fragments with deletions |
| `compaction_deletion_threshold` | 0.1 | Fraction of deleted rows to trigger rewrite |
| `compaction_min_fragments` | 4 | Minimum fragments before compacting |

The compaction loop runs every 60 seconds (leader-only) and processes active,
sealed, and S3 tiers. Compaction respects version pins — it will not delete
dataset versions that remote clients are actively reading.

### Cleanup

After successful flush, `cleanup_s3()` garbage-collects orphaned S3 files
(old manifest versions and fragment files). This also respects version pins
via `VersionPinTracker`.

## BisqueLance Engine

The top-level engine holds a registry of `TableEngine` instances:

```rust
let engine = BisqueLance::open(config).await?;

// Create a table
let table = engine.create_table(table_config, None).await?;

// Access tables
let table = engine.require_table("events")?;
let names = engine.list_tables();

// Drop a table (removes local data)
engine.drop_table("events").await?;
```

For crash recovery, the engine can serialize/restore its entire state:

```rust
// Serialize all table state for snapshots
let entries = engine.table_entries();

// Restore from persisted state
engine.restore_from_persisted_entries(entries).await?;
```

## TableEngine Concurrency

The `TableEngine` uses a careful locking strategy to ensure reads are never
blocked by writes:

**Dataset handles** — Each tier uses `RwLock<Option<Dataset>>`. Readers clone
the `Dataset` handle (a cheap `Arc` increment) under a brief read lock, then
execute queries against the snapshot with no lock held.

**Writer serialization** — Each tier has a `tokio::sync::Mutex<()>` that
serializes writers. A writer clones the current dataset under a read lock,
performs I/O with no lock held, then briefly acquires the write lock to swap
the updated handle.

**Atomic counters** — `active_bytes` uses `AtomicU64` for lock-free seal
threshold checks. `active_first_log_index` and `sealed_first_log_index` use
compare-and-swap to record the first Raft log index written to each tier.

```
Reader 1 ──► clone Dataset (brief read lock) ──► query (no lock) ──► done
Reader 2 ──► clone Dataset (brief read lock) ──► query (no lock) ──► done
Writer   ──► acquire write mutex ──► clone Dataset ──► I/O (no lock)
         ──► brief write lock to swap ──► release write mutex
```

## Schema Evolution

Schema changes are tracked per table in a `schema_history` vector:

```rust
pub struct SchemaVersion {
    pub version: u64,            // Monotonically increasing
    pub schema_ipc: Vec<u8>,     // Arrow IPC-encoded schema
    pub created_at_millis: i64,  // Creation timestamp
}
```

The first write to a table auto-detects the schema if not specified during
table creation. Schema history is persisted in the MDBX manifest for crash
recovery.

## Log Index Tracking

Each table tracks the first Raft log index written to its active and sealed
segments. This enables two optimizations:

1. **Recovery skip** — During Raft log replay, `AppendRecords` entries with
   log indices already promoted to S3 are skipped (they would be re-applied
   to data that's already in deep storage)
2. **Log purging** — `min_safe_log_index()` computes the minimum log index
   still needed by hot/warm segments, allowing older Raft log entries to be
   safely deleted

## File Layout

```
{local_data_dir}/
├── tables/
│   ├── events/
│   │   └── segments/
│   │       ├── 1.lance/     # Active segment
│   │       └── 2.lance/     # Sealed segment (if any)
│   └── metrics/
│       └── segments/
│           └── 3.lance/     # Active segment
└── .lance_groups/
    └── 1/
        └── mdbx.dat         # MDBX manifest (if enabled)
```

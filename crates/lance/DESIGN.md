# bisque-lance — Distributed Search & Analytics Storage Engine

## Project Overview

`bisque-lance` is a Rust module that implements a distributed, Raft-replicated "Hot-Cold" LSM storage pipeline on top of the Lance columnar format. It provides the storage layer for a unified search and analytics engine capable of handling Logs (FTS), Metrics (Columnar/TSDB), and Vectors (AI/Semantic search) through a single format and query interface.

The module is the bridge between the Raft consensus layer (Openraft) and the underlying Lance datasets, managing the full lifecycle of data from ingestion through local NVMe indexing to deep S3 archival.

---

## Core Technology Stack

| Layer | Technology | Role |
|---|---|---|
| Consensus | Openraft | Distributed state, leader election, log replication |
| Local Metadata & WAL | MDBX (via `libmdbx` crate) | Raft log storage, segment catalog, flush state tracking |
| Query Engine | Apache DataFusion | SQL parsing, query planning, SIMD-vectorized execution |
| Storage Format | Lance (`lance` crate) | Columnar datasets with native FTS, vector indices, S3-optimized |
| In-Memory Layout | Apache Arrow | Zero-copy data transfer between all layers |
| Object Storage | `object_store` crate | S3/GCS/Azure abstraction for deep storage tier |

---

## Architecture: The "Hot-Cold" LSM Pipeline

Data is organized as **Segments** — individual Lance datasets that graduate through a three-tier lifecycle.

### Tier 1: Active Segment (Local NVMe)

- **Ingestion**: The Raft Leader receives raw Arrow RecordBatches, replicates them to followers via `AppendRecords` log entries, and all nodes append data to a local Active Lance Dataset.
- **Indexing**: Lance builds Full-Text Search (FTS) and Vector indices locally on NVMe in real-time. Data is searchable within milliseconds of ingestion.
- **Sealing**: The active segment is sealed (becomes read-only) when **either** a max-age threshold (e.g., 60 seconds) **or** a max-size threshold (e.g., 1GB) is reached, whichever comes first. A new active segment is created immediately.

### Tier 2: Sealed Segment (Local NVMe)

- At most **one** sealed segment exists at a time in the query path (the most recently sealed).
- Provides high-speed local NVMe search while awaiting flush to deep storage.
- The MDBX state machine tracks the active and sealed segment IDs across the cluster.

### Tier 3: Deep Storage (S3 + Lance)

- **Flush**: A background task on the Leader reads the sealed local segment and appends its data to a master S3 Lance Dataset.
- **Promotion**: Once the upload completes and the S3 manifest is committed, the Leader proposes a `PromoteToDeepStorage` Raft log entry containing the new S3 manifest version.
- **Cleanup**: Followers update their S3 version pointer and delete the local sealed segment to reclaim NVMe space.

### Fixed-Slot Query View

The DataFusion query path always operates against exactly **three slots**:

```
UNION ALL(active_segment, sealed_segment, s3_deep_storage)
```

This produces a predictable query plan with constant planning overhead. Execution strategy is specialized per slot:
- **Active + Sealed**: Memory-mapped local NVMe scans (zero-copy)
- **S3 Deep**: Async HTTP Range Request reads for specific columnar chunks

---

## Raft State Machine (MDBX)

The MDBX-backed state machine tracks the following persistent state:

### Segment Catalog

```rust
struct SegmentCatalog {
    /// Currently active segment accepting writes
    active_segment: SegmentId,

    /// Most recently sealed segment (at most one)
    sealed_segment: Option<SegmentId>,

    /// Current S3 deep storage manifest version
    s3_manifest_version: u64,

    /// S3 dataset URI
    s3_dataset_uri: String,
}
```

### Flush State Tracking

```rust
enum FlushState {
    /// No flush in progress
    Idle,

    /// Leader is flushing sealed segment to S3
    /// Records the segment ID and the S3 fragment paths being written
    /// so a new leader can detect and clean up orphans
    InProgress {
        segment_id: SegmentId,
        started_at: Timestamp,
        /// S3 keys of fragment files written so far (for orphan cleanup)
        fragment_paths: Vec<String>,
    },
}
```

### Raft Log Entry Types

```rust
enum LogEntry {
    /// Replicate raw data to all nodes
    /// Every node appends this data to its local active Lance dataset
    AppendRecords {
        data: Vec<RecordBatch>,
    },

    /// Seal the current active segment and create a new one
    SealActiveSegment {
        sealed_segment_id: SegmentId,
        new_active_segment_id: SegmentId,
        reason: SealReason, // MaxAge | MaxSize
    },

    /// Mark the start of a flush operation (leader only writes to S3)
    BeginFlush {
        segment_id: SegmentId,
    },

    /// Promote sealed segment data to S3 deep storage
    /// Applied after successful S3 manifest commit
    PromoteToDeepStorage {
        segment_id: SegmentId,
        s3_manifest_version: u64,
    },
}
```

---

## Lance Integration Details

### Local Segment Management

Each local segment is an independent Lance dataset on NVMe:

```
/data/segments/{segment_id}.lance/
├── data/
│   └── *.lance          # columnar data files (fragments)
├── _indices/            # FTS + vector indices
├── _versions/
│   └── {version}.manifest
└── _deletions/
```

**Write path per `AppendRecords`**:
1. Deserialize Arrow RecordBatches from the Raft log entry
2. Call `dataset.append()` on the local active segment's Lance dataset
3. Lance atomically writes new fragment files and updates the manifest via `RenameCommitHandler` (local filesystem supports atomic rename)

**Sealing**:
1. Close the active Lance dataset handle (no more writes)
2. Rotate: sealed_segment = old active, create new active segment
3. Optionally trigger FTS/vector index optimization on the sealed segment before flush

### S3 Deep Storage — Atomic Commit Model

Lance's versioning is manifest-based. The commit flow for S3 flush:

1. **Data files are written first** — fragment `.lance` files are uploaded to S3 under `data/`. At this point they are invisible to readers because no manifest references them.
2. **Manifest commit** — the `CommitHandler` atomically makes the new manifest visible. On S3, this requires either:
   - A `DynamoDB`-based commit handler (conditional put for version coordination), **or**
   - A custom `CommitHandler` since bisque-lance has a single-writer guarantee (only the Raft leader writes to S3)
3. **Visibility** — readers only see data referenced by the current manifest. Orphaned fragment files from failed writes are invisible.

**Key property**: A write is **all-or-nothing**. If the leader crashes after uploading fragments but before the manifest commit, those fragments are unreferenced orphans — not corrupt data.

### Orphan Detection and Cleanup

Lance's `cleanup_old_versions` API handles garbage collection:

```rust
// Safe to call when no flush is in progress (verified via Raft state)
let stats = dataset
    .cleanup_old_versions(
        Duration::zero(),       // clean up all old versions
        Some(true),             // delete_unverified: remove orphaned files
        Some(false),            // don't error on tagged versions
    )
    .await?;
```

**`delete_unverified`** is the critical flag:
- `false` (default): Only deletes files referenced by at least one manifest version. Keeps unverified files because they might belong to an in-progress transaction.
- `true`: Deletes all unreferenced files regardless. **Safe to use when the Raft leader confirms no flush is in-progress.**

**Raft-aware cleanup strategy**:
1. New leader checks MDBX for `FlushState::InProgress`
2. If found, the `fragment_paths` field tells us exactly which S3 keys are orphans from the crashed leader's incomplete flush
3. Delete those specific keys (surgical cleanup) OR call `cleanup_old_versions(delete_unverified=true)` (broad cleanup)
4. Clear the stale `FlushState` entry
5. Re-initiate flush from local sealed segment (still on NVMe)

### Lance Configuration

```rust
// For local NVMe segments (high-frequency writes)
let write_params = WriteParams {
    max_rows_per_file: 1_000_000,
    max_rows_per_group: 10_000,
    mode: WriteMode::Append,
    skip_auto_cleanup: true,  // We manage cleanup ourselves
    data_storage_version: Some(LanceFileVersion::Stable),
    ..Default::default()
};

// For S3 deep storage
let s3_write_params = WriteParams {
    max_rows_per_file: 5_000_000,  // Larger files for S3 efficiency
    max_rows_per_group: 50_000,
    mode: WriteMode::Append,
    commit_handler: Some(commit_handler),  // DynamoDB or custom single-writer
    enable_v2_manifest_paths: true,  // Constant-time manifest lookup on S3
    skip_auto_cleanup: true,
    ..Default::default()
};
```

---

## Module Public API

```rust
pub struct BisqueLance {
    config: BisqueLanceConfig,
    segment_catalog: SegmentCatalog,
    active_dataset: Arc<RwLock<Dataset>>,
    sealed_dataset: Arc<RwLock<Option<Dataset>>>,
    s3_dataset: Arc<RwLock<Dataset>>,
    flush_state: Arc<RwLock<FlushState>>,
}

pub struct BisqueLanceConfig {
    /// Local NVMe path for segment storage
    pub local_data_dir: PathBuf,

    /// S3 URI for deep storage dataset
    pub s3_uri: String,

    /// S3 storage options (credentials, region, endpoint)
    pub s3_storage_options: HashMap<String, String>,

    /// Max age before sealing active segment
    pub seal_max_age: Duration,

    /// Max size in bytes before sealing active segment
    pub seal_max_size: u64,

    /// Whether this node is the Raft leader (controls S3 write permissions)
    pub is_leader: bool,

    /// Commit handler for S3 (DynamoDB or custom)
    pub s3_commit_handler: Option<Arc<dyn CommitHandler>>,
}

impl BisqueLance {
    /// Initialize storage engine, open or create local segments and S3 dataset
    pub async fn open(config: BisqueLanceConfig) -> Result<Self>;

    /// --- Write Path (called by Raft state machine apply) ---

    /// Append records to the active local segment
    /// Called on ALL nodes when an AppendRecords log is applied
    pub async fn apply_append(&self, batches: &[RecordBatch]) -> Result<()>;

    /// Seal the active segment and rotate to a new one
    /// Called on ALL nodes when a SealActiveSegment log is applied
    pub async fn apply_seal(&self, new_segment_id: SegmentId) -> Result<()>;

    /// Begin flush of sealed segment to S3 (leader only)
    /// Records flush state for crash recovery
    pub async fn begin_flush(&self) -> Result<FlushHandle>;

    /// Execute the S3 flush: read sealed segment, append to S3 dataset
    /// Returns the new S3 manifest version on success
    pub async fn execute_flush(&self, handle: &FlushHandle) -> Result<u64>;

    /// Apply promotion: update S3 version pointer, clean up local sealed segment
    /// Called on ALL nodes when PromoteToDeepStorage log is applied
    pub async fn apply_promote(&self, s3_manifest_version: u64) -> Result<()>;

    /// --- Read Path ---

    /// Get a DataFusion TableProvider that unions all three tiers
    /// Used to register with a DataFusion SessionContext for SQL queries
    pub fn table_provider(&self) -> Arc<dyn TableProvider>;

    /// Direct scan across all tiers with optional filter pushdown
    pub async fn scan(
        &self,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream>;

    /// --- Maintenance ---

    /// Recover from a crashed flush (new leader calls this on startup)
    /// Detects orphaned S3 files and cleans them up
    pub async fn recover_flush(&self) -> Result<()>;

    /// Run cleanup on the S3 dataset (call when no flush is in-progress)
    pub async fn cleanup_s3(&self) -> Result<RemovalStats>;

    /// Check if the active segment should be sealed (age or size threshold)
    pub fn should_seal(&self) -> Option<SealReason>;

    /// --- Lifecycle ---

    /// Gracefully shutdown, ensuring no partial writes
    pub async fn shutdown(&self) -> Result<()>;
}
```

---

## Leader Election and Failover

When a new leader is elected:

1. **Check flush state** in MDBX:
   - If `FlushState::InProgress`, call `recover_flush()` to clean up orphaned S3 fragments
   - The sealed segment should still be intact on local NVMe (it's only deleted after `PromoteToDeepStorage` is committed)
2. **Re-initiate flush** if a sealed segment exists but hasn't been promoted
3. **Resume normal operation**: accept writes to active segment, monitor seal thresholds

### Follower Catch-Up (Snapshot)

If a follower falls far behind and cannot replay the Raft log:

1. Openraft snapshot mechanism is used
2. The snapshot contains: the MDBX segment catalog state + the S3 manifest version
3. Follower bootstraps by: opening the S3 dataset at the specified version + receiving the current active segment data via log replay from the snapshot point
4. No need to transfer local Lance datasets — followers rebuild them from log replay

---

## DataFusion Integration

### Table Provider

`BisqueLance` implements `datafusion::catalog::TableProvider` by creating a `UnionExec` over three child plans:

```rust
impl TableProvider for BisqueLanceTableProvider {
    fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. Create LanceScanExec for active segment (local NVMe)
        // 2. Create LanceScanExec for sealed segment if present (local NVMe)
        // 3. Create LanceScanExec for S3 deep storage
        // 4. Return UnionExec over all present plans

        // Lance's DataFusion integration handles:
        // - FTS filter pushdown (MATCHES predicates)
        // - Vector distance pushdown (array_distance ORDER BY)
        // - Projection pushdown (only read needed columns)
        // - Predicate pushdown (scalar filters on indexed columns)
    }
}
```

### Query Capabilities

The engine should support these query patterns through DataFusion SQL:

```sql
-- Full-text search across all tiers
SELECT * FROM logs WHERE MATCHES(message, 'error AND timeout') ORDER BY timestamp DESC LIMIT 100;

-- Vector similarity search
SELECT * FROM embeddings ORDER BY array_distance(vector, [0.1, 0.2, ...]) LIMIT 10;

-- Hybrid: FTS + vector + scalar filters
SELECT * FROM events
WHERE MATCHES(body, 'authentication')
  AND severity >= 3
ORDER BY array_distance(embedding, ?) LIMIT 20;

-- Time-range metrics query (benefits from columnar layout)
SELECT host, avg(cpu_usage), max(memory_mb)
FROM metrics
WHERE timestamp BETWEEN '2025-01-01' AND '2025-01-02'
GROUP BY host;
```

---

## File System Layout

```
/data/
├── mdbx/                          # Raft state machine + segment catalog
│   ├── data.mdb
│   └── lock.mdb
├── segments/
│   ├── {active_id}.lance/         # Tier 1: Active segment
│   │   ├── data/
│   │   ├── _indices/
│   │   └── _versions/
│   └── {sealed_id}.lance/         # Tier 2: Sealed segment (0 or 1)
│       ├── data/
│       ├── _indices/
│       └── _versions/
└── raft/                          # Openraft log storage (MDBX)

s3://bucket/deep-storage.lance/    # Tier 3: Deep storage
├── data/
│   └── *.lance
├── _indices/
├── _versions/
│   └── {version}.manifest
└── _deletions/
```

---

## Error Handling and Edge Cases

### Partial Append on Crash
- Local NVMe uses `RenameCommitHandler` — atomic rename means a crash mid-append leaves the manifest at the previous version. Partial fragment files are orphans, cleaned up on next `cleanup_old_versions`.

### Leader Crash Mid-Flush
- `FlushState::InProgress` persists in MDBX (replicated via Raft)
- New leader detects this, uses recorded `fragment_paths` for surgical S3 cleanup
- Sealed segment is still on local NVMe — re-flush it

### Follower Divergence
- Should never happen if `AppendRecords` replay is deterministic at the data level
- Lance indices (FTS, vector) may differ in internal structure across nodes but produce equivalent query results
- This is acceptable — nodes are not expected to be byte-identical, only query-equivalent

### S3 Eventual Consistency
- S3 now provides strong read-after-write consistency for PUTs
- Lance's manifest-based versioning means readers never see partial data
- The `enable_v2_manifest_paths` option provides constant-time manifest lookup regardless of version count

---

## Dependencies (Cargo.toml)

```toml
[dependencies]
lance = { version = "0.39", features = ["dynamodb"] }
lance-io = "0.39"
lance-table = "0.39"
lance-core = "0.39"

arrow = { version = "56", features = ["prettyprint"] }
arrow-array = "56"
arrow-schema = "56"

datafusion = "50"

object_store = { version = "0.12", features = ["aws"] }

openraft = "0.10"
libmdbx = "0.6"

tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
uuid = { version = "1", features = ["v4"] }
chrono = "0.4"
```

---

## Implementation Priority

### Phase 1: Local Storage Engine
1. `BisqueLance::open()` — create/open local Lance datasets
2. `apply_append()` — write RecordBatches to active segment
3. `should_seal()` + `apply_seal()` — segment rotation with age/size thresholds
4. `table_provider()` — DataFusion integration for local-only queries

### Phase 2: S3 Deep Storage
5. `begin_flush()` + `execute_flush()` — sealed segment → S3 pipeline
6. `apply_promote()` — S3 version tracking + local cleanup
7. `recover_flush()` — crash recovery for incomplete S3 writes
8. `cleanup_s3()` — orphan garbage collection

### Phase 3: Raft Integration
9. MDBX state machine for segment catalog + flush state
10. Openraft log entry serialization/deserialization
11. Snapshot mechanism for follower catch-up
12. Leader election hooks (flush recovery, role transition)

### Phase 4: Query Optimization
13. FTS index creation on seal
14. Vector index creation on seal
15. Filter pushdown verification through DataFusion → Lance
16. Compaction of small sealed segments before S3 flush

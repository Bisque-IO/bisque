# bisque-lance Architecture

bisque-lance is a distributed, Raft-replicated storage engine built on the
[Lance](https://lancedb.github.io/lance/) columnar format. It manages the full
lifecycle of data from ingestion through local NVMe segments to deep S3
archival, with support for multiple independent tables.

## Design Principles

1. **Raft log is the WAL.** All mutations are sequenced through Raft consensus.
   Lance datasets are materialized views that safely lag behind the log.
2. **Copy-on-write storage.** Lance never modifies files in place — readers see
   consistent point-in-time snapshots even during concurrent writes.
3. **Per-table isolation.** Each table has independent segment lifecycle, seal
   thresholds, flush state, and compaction schedule.
4. **Zero-copy where possible.** IPC-encoded Arrow payloads are passed as
   `Bytes` slices through the Raft log and codec without copying.

## Storage Tiers

Data flows through three tiers per table:

```
┌─────────────────┐    seal     ┌─────────────────┐   flush    ┌─────────────────┐
│  Active Segment │  ────────►  │  Sealed Segment │  ───────►  │  Deep Storage   │
│  (local NVMe)   │             │  (local NVMe)   │            │  (S3 / Lance)   │
│  read + write   │             │  read-only      │            │  read-only      │
└─────────────────┘             └─────────────────┘            └─────────────────┘
```

- **Active Segment**: Local Lance dataset accepting `AppendRecords` writes via
  Raft. Created with a monotonically increasing `SegmentId`.
- **Sealed Segment**: The previous active segment, now read-only. At most one
  sealed segment exists per table at any time. Awaits flush to deep storage.
- **Deep Storage**: S3-backed Lance dataset. The long-term archive. Queries
  across all three tiers are unified via `UNION ALL` in DataFusion.

### Segment Lifecycle

```
         ┌───────────────────────────────────────────────────────────┐
         │                     Raft Consensus                       │
         └──────────┬──────────────┬──────────────┬─────────────────┘
                    │              │              │
              AppendRecords   SealActive    PromoteToDeep
                    │              │              │
                    ▼              ▼              ▼
              ┌──────────┐  ┌──────────┐  ┌──────────────┐
              │  Active  │─►│  Sealed  │─►│  S3 Dataset  │
              └──────────┘  └──────────┘  └──────────────┘
```

**Sealing** is triggered when the active segment exceeds a size or age
threshold. The leader proposes `SealActiveSegment` via Raft, and all nodes
atomically rotate: the active dataset becomes sealed, and a new active dataset
is created.

**Flushing** is leader-only. The leader copies sealed segment data to S3, then
proposes `PromoteToDeepStorage` via Raft. All nodes update their catalog and
discard the local sealed segment.

## Core Components

### BisqueLance (engine.rs)

Multi-table storage engine. Holds a registry of `TableEngine` instances keyed
by table name.

```rust
let config = BisqueLanceConfig::new("/data")
    .with_seal_max_age(Duration::from_secs(60))
    .with_s3_uri("s3://my-bucket/lance");

let engine = BisqueLance::open(config).await?;
```

### TableEngine (table_engine.rs)

Manages a single table's data across all three storage tiers. Key design
patterns:

- **Dataset snapshots**: `RwLock<Option<Dataset>>` allows readers to clone the
  dataset handle (cheap `Arc` increment) without blocking.
- **Serialized writers**: `tokio::sync::Mutex<()>` ensures at most one writer
  per tier. Writers clone the dataset, perform I/O, then briefly acquire the
  write lock to swap the updated handle.
- **Atomic size tracking**: `AtomicU64` for `active_bytes` enables lock-free
  seal threshold checks.

### LanceRaftNode (raft.rs)

Wires the engine and state machine into a Raft group. Runs background tasks:

| Task | Leader-only | Interval | Purpose |
|------|-------------|----------|---------|
| Seal checker | Yes | 5s | Check if any active segment exceeds age/size thresholds |
| Flush orchestrator | Yes | 10s | Copy sealed segments to S3 |
| Compaction | Yes | 60s | Optimize fragments across all tiers |

```rust
let raft_node = LanceRaftNode::new(raft, engine, node_id)
    .with_write_batcher(WriteBatcherConfig::default())
    .with_applied_watermark(watermark)
    .with_manifest(manifest, group_id);

raft_node.start();  // Spawns background tasks
```

### LanceStateMachine (state_machine.rs)

Implements openraft's `RaftStateMachine` trait. Dispatches Raft log entries to
the appropriate table:

| Command | Effect |
|---------|--------|
| `CreateTable` | Create table in engine, emit `TableCreated` event |
| `DropTable` | Remove table and local data, emit `TableDropped` event |
| `AppendRecords` | Append IPC-encoded batches to active segment |
| `SealActiveSegment` | Rotate active → sealed, create new active |
| `BeginFlush` | Mark flush in progress (leader bookkeeping) |
| `PromoteToDeepStorage` | Update catalog, discard local sealed segment |

**Skip optimization**: During recovery, `AppendRecords` entries already
promoted to S3 are skipped using per-table `min_safe_log_index`.

## Write Path

```
Client                WriteBatcher              Raft              State Machine
  │                       │                      │                      │
  │── write_records() ──► │                      │                      │
  │                       │── linger (5ms) ──►   │                      │
  │                       │── coalesce IPC ──►   │                      │
  │                       │── processor? ──►     │                      │
  │                       │── propose ──────────►│                      │
  │                       │                      │── replicate ──────► │
  │                       │                      │                     │── apply_append()
  │                       │                      │◄── response ───────│
  │◄── WriteResult ──────│◄── ack ──────────────│                      │
```

### WriteBatcher (write_batcher.rs)

Optional write coalescing layer. Per-table channels accumulate writes over a
configurable linger window or until a byte threshold is reached, then submit
a single `AppendRecords` proposal.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `linger` | 5ms | Wait time after first write before flushing |
| `max_batch_bytes` | 8 MB | Flush when accumulated bytes exceed this |
| `channel_capacity` | 1024 | Per-table channel depth |

### WriteProcessor (write_processor.rs)

Optional transform applied to batches before Raft proposal. The `WriteProcessor`
trait allows pre-aggregation (e.g., summing counters), schema transforms, and
materialized writes to other tables.

Built-in processors: `CounterAggregator`, `GaugeAggregator`,
`HistogramAggregator`, and OTel-specific variants.

### AsyncApplyBuffer (async_apply.rs)

Decouples Lance I/O from the Raft apply path. Since the Raft log is the
durable WAL, Lance writes can safely lag behind.

- State machine's `apply()` enqueues work and returns immediately.
- Background task materializes writes to Lance datasets.
- `AppliedWatermark` lets queries wait until a specific log index is
  materialized.
- **Backpressure**: When pending bytes exceed `max_pending_bytes` (64 MB),
  `enqueue()` blocks the apply path, naturally throttling Raft.

## Query Path

Queries execute locally against snapshotted Lance datasets via DataFusion:

```
SQL Query ──► SessionContext ──► BisqueLanceTableProvider ──► UNION ALL
                                         │                       │
                                    ┌────┴────┐            ┌────┴────┐
                                    │ Active  │            │ Sealed  │
                                    │ Scanner │            │ Scanner │
                                    └─────────┘            └────┬────┘
                                                                │
                                                           ┌────┴────┐
                                                           │   S3    │
                                                           │ Scanner │
                                                           └─────────┘
```

### BisqueLanceTableProvider (query.rs)

DataFusion `TableProvider` that unions active, sealed, and S3 datasets.
Projection, filter, and limit pushdown are delegated to Lance's native
scanner.

### Read-After-Write Fencing

When async apply is enabled, writes return a `WriteResult { log_index }`.
Queries can pass this as `x-bisque-min-log-id` (Flight SQL header) to wait
until that index is materialized before executing.

```rust
let result = raft_node.write_records("events", &batches).await?;
// result.log_index = 42

// Later query will wait until log index 42 is applied to Lance:
// Flight SQL header: x-bisque-min-log-id: 42
```

## Protocol Interfaces

### Arrow Flight SQL (flight.rs)

Full Flight SQL service backed by `LanceRaftNode`:

- **Query**: `CommandStatementQuery` — SQL execution via DataFusion
- **Write**: `CommandStatementIngest` — Bulk write via `do_put`
- **DDL**: `CommandStatementUpdate` — `CREATE TABLE`, `DROP TABLE`
- **Catalog**: `CommandGetTables`, `CommandGetCatalogs`, `CommandGetDbSchemas`
- **Actions**: `create_table` (IPC schema), `drop_table` (name)

```rust
serve_flight(raft_node, "0.0.0.0:50051".parse()?).await?;
```

### PostgreSQL Wire Protocol (postgres.rs)

Read-only SQL access via the PostgreSQL wire protocol (uses
`datafusion-postgres`). Tables created/dropped at runtime are immediately
visible to new connections.

```rust
serve_postgres(raft_node, PostgresServerConfig::default()).await?;
```

### S3-Compatible HTTP API (s3_server.rs)

Exposes active and sealed segment files via an S3-compatible HTTP API,
enabling remote clients to read hot/warm data directly.

| Operation | Endpoint |
|-----------|----------|
| GetObject | `GET /{bucket}/{table}/{tier}/{path}` |
| HeadObject | `HEAD /{bucket}/{table}/{tier}/{path}` |
| ListObjectsV2 | `GET /{bucket}?list-type=2&prefix=...` |
| Catalog | `GET /{bucket}/_bisque/catalog` |
| WebSocket | `GET /{bucket}/_bisque/ws` |

The WebSocket endpoint pushes real-time catalog events and supports version
pinning for compaction safety.

```rust
serve_s3(engine, addr, Some(catalog_bus), Some(manifest), group_id).await?;
```

## Client

### BisqueClient (client.rs)

Remote query client that auto-syncs with a bisque-lance cluster:

```
┌─────────────────────────────┐   HTTP/S3 + WebSocket   ┌───────────────────────┐
│  BisqueClient               │ ◄──────────────────────► │  bisque-lance cluster │
│  - auto-syncing datasets    │   GET/HEAD/LIST + WS     │  S3 API + WS push    │
│  - SessionContext + SQL     │                          │  catalog event bus    │
└─────────────────────────────┘                          └───────────────────────┘
```

- **Auto-discovery**: Fetches catalog on connect, opens Lance datasets per table
- **Real-time sync**: WebSocket listener receives catalog events, atomically
  swaps dataset handles via `ArcSwap`
- **Client-side SQL**: Full DataFusion `SessionContext` — queries execute
  locally, reading data through the S3 API
- **Version pinning**: Pins dataset versions over WebSocket to prevent
  server-side compaction from deleting in-use files

```rust
let credentials = Arc::new(CredentialConfig::new()
    .with_credential("aws_access_key_id", "AKIA...")
    .with_credential("aws_secret_access_key", "..."));

let client = BisqueClient::connect(
    "http://cluster:3300",
    "my-bucket",
    credentials,
    Some(Path::new(".bisque_client")),  // MDBX persistence path
).await?;

let batches = client.sql("SELECT * FROM events WHERE user_id = 'alice'").await?;
client.close().await;
```

### Client-Side MDBX Persistence (client_store.rs)

When a `persist_path` is provided, `BisqueClient` stores catalog state in a
local MDBX database. On reconnect:

1. Load persisted `last_seq` and per-table catalog entries
2. Connect WebSocket with `?since=last_seq` for delta sync
3. Open datasets lazily on first query (not on connect)
4. If too far behind, server signals `snapshot_required` for full re-sync

This makes reconnection near-instant for large clusters with many tables.

### CredentialConfig (cold_store.rs)

Client-side S3 credential management. The server provides non-credential
options (region, endpoint, allow_http) via the catalog API. The client layers
credentials on top.

```rust
let config = CredentialConfig::new()
    .with_credential("aws_access_key_id", "AKIA...")
    .with_credential("aws_secret_access_key", "...")
    .with_table_credentials("audit", HashMap::from([
        ("aws_access_key_id".into(), "OTHER_KEY".into()),
    ]));
```

ObjectStore instances are cached by bucket URI + credential fingerprint.

### BisqueRoutingStore (s3_store.rs)

`ObjectStore` implementation that routes reads by data temperature:

- **Hot/warm paths** (`{table}/active/...`, `{table}/sealed/...`): HTTP
  requests to the cluster's S3 API
- **Cold paths** (deep storage): Delegates to a real S3 `ObjectStore` resolved
  via `CredentialConfig`

Segment catalog is cached with a 5-second TTL and refreshed on demand.

## Crash Recovery

### MDBX Manifest (manifest.rs)

Optional crash-consistent metadata store using MDBX (an embedded B-tree
database). Per Raft group, stores:

| MDBX Table | Key | Value | Purpose |
|------------|-----|-------|---------|
| `lance_meta` | `b"meta"` | `GroupMeta` | Last applied log ID, membership |
| `lance_tables` | table name | `PersistedTableEntry` (JSON) | Full table state |
| `catalog_wal` | sequence number | `CatalogEvent` (bincode) | Event log for client delta sync |

**WAL compaction**: After each append, if total WAL size exceeds 16 MiB
(configurable), oldest entries are removed to bound growth.

### Recovery Layers

1. **Raft log** — bisque-raft's mmap-backed log (source of truth)
2. **MDBX manifest** — crash-consistent metadata (optional, avoids full replay)
3. **In-memory state** — fallback for first boot

Without MDBX, recovery replays the entire Raft log. With MDBX, recovery
restores from the last committed MDBX state and replays only entries since
`last_applied`.

## Catalog Events

### CatalogEventBus (catalog_events.rs)

Broadcasts catalog mutations in real-time via `tokio::sync::broadcast`
(bounded 1024 entries). Each event carries a monotonically increasing `seq`.

| Event | Trigger |
|-------|---------|
| `TableCreated { table, schema_ipc }` | `CreateTable` applied |
| `TableDropped { table }` | `DropTable` applied |
| `ActiveVersionBumped { table, version }` | `AppendRecords` applied |
| `SegmentSealed { table, active_version, sealed_version }` | `SealActiveSegment` applied |
| `SegmentPromoted { table, s3_manifest_version }` | `PromoteToDeepStorage` applied |

Events are persisted to the MDBX WAL (when manifest is enabled) and pushed
to WebSocket clients.

### VersionPinTracker (version_pins.rs)

Tracks which Lance dataset versions are pinned by remote clients, preventing
compaction from deleting in-use files.

- Sessions auto-expire if heartbeats stop (30-second timeout)
- Clients send `pin`/`unpin` messages over WebSocket
- `is_pinned(table, tier, version)` checked before cleanup operations

## OpenTelemetry Integration

### OtlpReceiver (otel/)

Full OTLP gRPC receiver that converts OpenTelemetry signals to Arrow
RecordBatches and writes them via Raft:

- **Metrics**: counters, gauges, histograms, exponential histograms, summaries
- **Traces**: spans, span events, span links
- **Logs**: log records with severity, body, attributes

Tables are auto-created on first signal receipt. Pre-aggregation processors
reduce write amplification before Raft proposal.

### Query APIs

Grafana-compatible HTTP APIs for querying stored telemetry:

| API | Protocol | Data |
|-----|----------|------|
| Tempo | HTTP | Traces — search, get by ID |
| Prometheus/Mimir | HTTP | Metrics — PromQL, remote-read, remote-write |
| Loki | HTTP | Logs — LogQL, push |

```rust
let otlp = OtlpReceiver::new(raft_node);
otlp.ensure_tables().await?;
serve_otlp(otlp.clone(), otlp_grpc_addr).await?;
serve_http(otlp, http_addr).await?;
```

## Configuration Reference

### BisqueLanceConfig

Node-wide defaults applied to all tables.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `local_data_dir` | required | Base directory for segment files |
| `seal_max_age` | 60s | Seal active segment after this duration |
| `seal_max_size` | 1 GB | Seal active segment at this byte size |
| `s3_uri` | None | S3 base URI (None disables deep storage) |
| `s3_storage_options` | empty | S3 credentials, region, endpoint |
| `s3_max_rows_per_file` | 5M | Target rows per S3 file |
| `s3_max_rows_per_group` | 50K | Row group size within files |
| `seal_indices` | empty | Indices to create on sealed segments |
| `compaction_target_rows_per_fragment` | 1M | Target fragment size |
| `compaction_materialize_deletions` | true | Rewrite fragments with deletions |
| `compaction_deletion_threshold` | 0.1 | Fraction of deleted rows to trigger rewrite |
| `compaction_min_fragments` | 4 | Minimum fragments before compacting |

### IndexSpec

Indices created automatically on sealed segments:

```rust
IndexSpec::fts("text_column")        // Full-text search (inverted index)
IndexSpec::vector("embedding")       // Vector search (IVF-HNSW-SQ)
IndexSpec::btree("timestamp")        // Scalar range queries
IndexSpec::rtree("location")         // Spatial queries
```

### WriteBatcherConfig

| Parameter | Default | Description |
|-----------|---------|-------------|
| `linger` | 5ms | Coalescing window |
| `max_batch_bytes` | 8 MB | Byte threshold for flush |
| `channel_capacity` | 1024 | Per-table channel depth |

### AsyncApplyConfig

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_pending_bytes` | 64 MB | Backpressure threshold |

## Concurrency Model

### Reader-Writer Isolation

Each dataset tier uses `RwLock<Option<Dataset>>`:

- **Readers** acquire a read lock, clone the `Dataset` handle (cheap `Arc`
  increment), release the lock, then execute queries against the snapshot.
- **Writers** acquire `AsyncMutex<()>` to serialize, clone the current
  dataset, perform I/O without any lock, then briefly acquire the write lock
  to swap the updated handle.

Reads are never blocked by writes. Writers contend only on the brief swap.

### Backpressure

When async apply is enabled:
1. Raft entries enqueued to `AsyncApplyBuffer`
2. If pending bytes exceed `max_pending_bytes`, `enqueue()` blocks
3. This blocks the state machine's `apply()`, which blocks Raft
4. Raft stops accepting proposals until the buffer drains

This provides natural flow control without explicit throttling.

## File Layout

```
{local_data_dir}/
├── tables/
│   ├── {table_name}/
│   │   └── segments/
│   │       ├── {segment_id}.lance/    # Active segment
│   │       └── {segment_id}.lance/    # Sealed segment (if any)
│   └── ...
└── .lance_groups/
    └── {group_id}/
        └── mdbx.dat                   # MDBX manifest (if enabled)
```

## Dependencies

| Category | Crates |
|----------|--------|
| Storage | `lance`, `lance-index`, `lance-table` |
| Query | `datafusion`, `arrow-*` |
| Replication | `openraft`, `bisque-raft` |
| Persistence | `libmdbx` |
| Protocols | `tonic`, `axum`, `fastwebsockets`, `datafusion-postgres` |
| Serialization | `serde`, `bincode`, `prost` |
| Async | `tokio`, `futures`, `crossfire` |
| Utilities | `parking_lot`, `arc-swap`, `tracing` |

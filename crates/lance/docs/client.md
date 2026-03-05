# BisqueClient

`BisqueClient` is a remote query client that executes SQL locally against
auto-syncing Lance datasets. The client reads data through the cluster's
S3-compatible HTTP API while receiving real-time catalog updates over
WebSocket. All query execution happens client-side via DataFusion — the
server only serves files and events.

## Architecture

```
┌─────────────────────────────────┐         ┌──────────────────────────┐
│  BisqueClient                   │         │  bisque-lance cluster    │
│                                 │         │                          │
│  ┌───────────────────────┐      │  HTTP   │  ┌────────────────────┐  │
│  │ RemoteLanceTableProvider│◄────┼─────────┼──│  S3 API (files)    │  │
│  │ active: ArcSwap<Dataset>│     │  GET    │  └────────────────────┘  │
│  │ sealed: ArcSwap<Dataset>│     │  HEAD   │                          │
│  └───────┬───────────────┘      │  LIST   │  ┌────────────────────┐  │
│          │                      │         │  │  Catalog endpoint  │  │
│  ┌───────▼───────────────┐      │  WS     │  └────────────────────┘  │
│  │ SessionContext (SQL)  │      │◄────────┤                          │
│  │ DataFusion engine     │      │  events │  ┌────────────────────┐  │
│  └───────────────────────┘      │  pins   │  │  WebSocket push    │  │
│                                 │         │  │  + version pinning │  │
│  ┌───────────────────────┐      │         │  └────────────────────┘  │
│  │ ClientStore (MDBX)    │      │         │                          │
│  │ catalog persistence   │      │         │  ┌────────────────────┐  │
│  └───────────────────────┘      │         │  │  MDBX Manifest WAL │  │
└─────────────────────────────────┘         └──┴────────────────────┴──┘
```

## Quick Start

```rust
use bisque_lance::{BisqueClient, CredentialConfig};
use std::sync::Arc;
use std::path::Path;

let credentials = Arc::new(CredentialConfig::new()
    .with_credential("aws_access_key_id", "AKIA...")
    .with_credential("aws_secret_access_key", "..."));

let client = BisqueClient::connect(
    "http://cluster:3300",
    "my-bucket",
    credentials,
    Some(Path::new(".bisque_client")),  // optional MDBX path
).await?;

// Execute SQL locally — data read from cluster via HTTP
let batches = client.sql(
    "SELECT user_id, COUNT(*) as cnt FROM events GROUP BY user_id"
).await?;

// Access the underlying DataFusion SessionContext
let df = client.ctx().sql("SELECT * FROM metrics LIMIT 10").await?;

client.close().await;
```

## Connection Flow

### Full Fetch (First Connect)

When no persisted state exists, `connect()` performs a full catalog fetch:

1. **Fetch catalog** — HTTP GET to `/{bucket}/_bisque/catalog`
2. **Open datasets** — For each table, open active and sealed Lance datasets
   through the `BisqueRoutingStore` (reads via cluster's S3 API)
3. **Register providers** — Create `RemoteLanceTableProvider` for each table
   and register it in the DataFusion `SessionContext`
4. **Pin versions** — Send `pin` messages over WebSocket for each active/sealed
   version
5. **Start listeners** — Spawn WebSocket event listener and heartbeat tasks
6. **Persist state** — If `persist_path` was provided, save catalog to MDBX

### Delta Sync (Reconnect)

When persisted state matches the cluster URL and bucket:

1. **Load MDBX** — Read `ClientMeta` (last_seq, cluster_url, bucket) and
   per-table `PersistedCatalogEntry` from local MDBX
2. **Restore providers** — Create `RemoteLanceTableProvider` for each table
   with persisted schema but **no datasets** (lazy opening)
3. **Connect WebSocket** — Connect with `?since=last_seq` for delta events
4. **Apply delta** — Process missed catalog events (version bumps, new/dropped
   tables)
5. **Lazy open** — Datasets are opened on first query, not on connect

This makes reconnection near-instant even for clusters with hundreds of tables.

### Snapshot Required

If the server's WAL has been compacted past the client's `last_seq`, the
server sends `snapshot_required`. The client falls back to a full catalog
fetch.

## Lazy Dataset Opening

When restoring from persisted state, `RemoteLanceTableProvider` starts with
`active: None` and `sealed: None`. Datasets are opened on first `scan()`:

```rust
pub struct RemoteLanceTableProvider {
    schema: SchemaRef,
    active: Arc<ArcSwap<Option<Dataset>>>,
    sealed: Arc<ArcSwap<Option<Dataset>>>,
    needs_active_open: AtomicBool,   // true = not yet opened
    needs_sealed_open: AtomicBool,
    routing_store: Option<Arc<BisqueRoutingStore>>,
    table_name: String,
}
```

The `ensure_datasets_open()` method uses `compare_exchange` on the atomic
flags so that only one concurrent query triggers the open. Subsequent queries
find the dataset already loaded.

## Real-Time Sync

The WebSocket listener processes catalog events and atomically updates
datasets:

| Event | Client Action |
|-------|---------------|
| `ActiveVersionBumped` | Re-open active dataset, pin new version, unpin old |
| `SegmentSealed` | Re-open both active and sealed, update segment IDs |
| `TableCreated` | Register new table provider with schema from event |
| `TableDropped` | Deregister table, unpin all versions |
| `SegmentPromoted` | Clear sealed dataset, unpin sealed version |

Dataset swaps use `ArcSwap` for lock-free atomic pointer updates. Queries
in flight continue using the old snapshot; new queries see the updated dataset.

## Version Pinning

The client pins dataset versions to prevent server-side compaction from
deleting files it's actively reading:

```
Client ──► {"type": "pin", "table": "events", "tier": "active", "version": 15}
Server ──► (prevents cleanup of version 15 until unpinned)
Client ──► {"type": "unpin", "table": "events", "tier": "active", "version": 14}
Client ──► {"type": "heartbeat"}  (every 10 seconds)
```

- Pins are sent when datasets are opened or updated
- Old versions are unpinned when new versions are swapped in
- Heartbeats keep the session alive (server reaps after 30s timeout)

## Client-Side MDBX Persistence

When a `persist_path` is provided, catalog state is stored in a local MDBX
database for fast reconnection.

### Storage Layout

Two MDBX tables:

| Table | Key | Value | Purpose |
|-------|-----|-------|---------|
| `client_meta` | `b"state"` | bincode `ClientMeta` | Connection state |
| `client_tables` | table name bytes | bincode `PersistedCatalogEntry` | Per-table catalog |

```rust
pub struct ClientMeta {
    pub last_seq: u64,          // Last event sequence seen
    pub cluster_url: String,    // For mismatch detection
    pub bucket: String,
}

pub struct PersistedCatalogEntry {
    pub active_segment: u64,
    pub sealed_segment: Option<u64>,
    pub s3_dataset_uri: String,
    pub s3_storage_options: HashMap<String, String>,
    pub active_version: Option<u64>,
    pub sealed_version: Option<u64>,
    pub schema_ipc: Vec<u8>,   // Arrow IPC-encoded schema
}
```

### Event Persistence

As WebSocket events arrive, the client immediately persists updates:
- `ActiveVersionBumped` &rarr; update `active_version` in table entry
- `SegmentSealed` &rarr; update `active_version`, `sealed_version`,
  `sealed_segment`
- `TableCreated` &rarr; insert new entry with `schema_ipc`
- `TableDropped` &rarr; remove entry
- `SegmentPromoted` &rarr; clear `sealed_segment` and `sealed_version`
- After each event &rarr; update `last_seq`

## CredentialConfig

Client-side S3 credential management. The server provides non-credential
options (region, endpoint, allow_http) via the catalog. The client layers
credentials on top.

```rust
let config = CredentialConfig::new()
    .with_credential("aws_access_key_id", "AKIA...")
    .with_credential("aws_secret_access_key", "...");

// Per-table credential overrides (e.g., different AWS account)
let config = config.with_table_credentials("audit", HashMap::from([
    ("aws_access_key_id".into(), "OTHER_KEY".into()),
    ("aws_secret_access_key".into(), "OTHER_SECRET".into()),
]));
```

### Merge Order (Later Wins)

1. Server-provided options (region, endpoint, etc.)
2. Client default credentials
3. Per-table credential overrides

### ObjectStore Caching

Opened `ObjectStore` instances are cached by bucket URI + credential
fingerprint. Tables sharing the same bucket and credentials reuse the same
store instance.

## Query Execution

Queries execute entirely on the client via DataFusion:

1. Client-side `SessionContext` holds `RemoteLanceTableProvider` per table
2. DataFusion parses SQL and builds a query plan
3. Lance scanners read data through `BisqueRoutingStore`:
   - Active/sealed files &rarr; HTTP GET to cluster's S3 API
   - Cold/deep files &rarr; direct S3 reads via `CredentialConfig`
4. DataFusion executes the plan locally and returns results

Filter, projection, and limit pushdown are applied at the Lance scanner level,
minimizing data transfer.

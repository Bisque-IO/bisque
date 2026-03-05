# Query Engines

bisque-lance exposes data through four query interfaces, all backed by
DataFusion and the same underlying Lance datasets. Tables created or dropped
via Raft are immediately visible to all protocols.

## DataFusion Integration

### BisqueLanceTableProvider

The core query abstraction implements DataFusion's `TableProvider` trait. Each
query scans all three storage tiers simultaneously:

```
SELECT * FROM events WHERE user_id = 'alice'
                    │
                    ▼
         BisqueLanceTableProvider
                    │
         ┌──────────┼──────────┐
         ▼          ▼          ▼
      Active     Sealed       S3
      Scanner    Scanner    Scanner
         │          │          │
         └──────────┼──────────┘
                    ▼
              UnionExec
```

**Pushdown support**:
- **Projection** — Column indices mapped to names, passed to Lance scanner
- **Filters** — Combined with `AND`, pushed to Lance's native filter engine
- **Limit** — Passed directly to Lance scanner
- Filter pushdown is reported as `Inexact`, so DataFusion also applies
  filtering at the plan level for correctness

**Snapshot isolation**: Each scan pins the current dataset version. Concurrent
writes create new versions without affecting in-flight queries.

### Dynamic Catalog Provider

All protocols share a `BisqueLanceCatalogProvider` that presents a "bisque"
catalog with a "public" schema. The schema provider delegates directly to the
live engine:

```rust
// table_names() → engine.list_tables()
// table(name) → BisqueLanceTableProvider::new(engine.require_table(name))
```

Tables created via Raft are immediately queryable — no restart or refresh
needed.

## Arrow Flight SQL

Full Flight SQL service over gRPC, backed by `LanceRaftNode`.

```rust
serve_flight(raft_node, "0.0.0.0:50051".parse()?).await?;
```

### Supported Commands

| Command | Description |
|---------|-------------|
| `CommandStatementQuery` | SQL query execution (SELECT) |
| `CommandStatementUpdate` | DDL operations (CREATE TABLE, DROP TABLE) |
| `CommandStatementIngest` | Bulk write via `do_put` stream |
| `CommandGetTables` | List tables with schemas |
| `CommandGetCatalogs` | Returns "bisque" catalog |
| `CommandGetDbSchemas` | Returns "public" schema |
| `CommandGetTableTypes` | Returns "TABLE" |
| `CommandGetSqlInfo` | Server metadata |

### Query Execution Flow

1. **`get_flight_info_statement`** — Parse SQL, execute to get schema (fast
   path), return `FlightInfo` with a `TicketStatementQuery` embedding the SQL
2. **`do_get_statement`** — Re-execute SQL, stream `RecordBatch` results via
   `FlightDataEncoder`

### Bulk Ingestion

`CommandStatementIngest` streams `RecordBatch` data via `do_put`:

```rust
let cmd = CommandStatementIngest {
    table: "events".to_string(),
    ..Default::default()
};
let rows = client.execute_ingest(cmd, batch_stream).await?;
```

Batches are decoded from the Flight stream and written through
`LanceRaftNode::write_records()`, returning the committed row count.

### Custom Actions

Two custom actions accessible via plain Flight `do_action`:

| Action | Body Format | Description |
|--------|-------------|-------------|
| `create_table` | `[u16 BE name_len][name][IPC schema]` | Create table with schema |
| `drop_table` | `[UTF-8 table name]` | Drop table |

Both are routed through Raft consensus.

### Read-After-Write Fencing

When async apply is enabled, queries can fence on a specific Raft log index:

```
Flight SQL header: x-bisque-min-log-id: 42
```

The query handler calls `watermark.wait_for(42, 5s)` before executing.
This ensures the query sees all data up to log index 42. Without the header,
queries execute immediately against whatever has been materialized.

### Prepared Statements

Stateless prepared statement support — the SQL text is embedded in the handle
and re-executed on each `do_get`. No server-side session state is maintained.

## PostgreSQL Wire Protocol

Read-only SQL access via the PostgreSQL wire protocol, compatible with `psql`,
JDBC, and any PostgreSQL client library.

```rust
serve_postgres(raft_node, PostgresServerConfig {
    host: "127.0.0.1".into(),
    port: 5432,
}).await?;
```

Uses `datafusion-postgres` for wire protocol handling. Registers `pg_catalog`
tables for `psql` compatibility (tab completion, `\dt`, etc.).

**Capabilities**:
- Full SQL SELECT support via DataFusion
- Dynamic table discovery (tables created via Raft appear immediately)
- Standard PostgreSQL connection parameters

**Limitations**:
- Read-only — INSERT/UPDATE/DELETE are not supported
- Use Flight SQL for writes and DDL operations

## S3-Compatible HTTP API

Exposes active and sealed segment files via an S3-compatible HTTP API. This
enables remote `BisqueClient` instances and any S3-compatible tool to read
hot/warm data directly.

```rust
serve_s3(engine, addr, Some(catalog_bus), Some(manifest), group_id).await?;
```

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/{bucket}/{key}` | GetObject — read file with optional Range header |
| `HEAD` | `/{bucket}/{key}` | HeadObject — file metadata |
| `GET` | `/{bucket}?list-type=2&prefix=...` | ListObjectsV2 — directory listing |
| `GET` | `/{bucket}/_bisque/catalog` | Catalog — segment metadata JSON |
| `GET` | `/{bucket}/_bisque/ws` | WebSocket — real-time catalog events |

### S3 Key Format

```
{table_name}/{tier}/{relative_path}
```

Where `tier` is `active` or `sealed`. Files are streamed directly from the
Lance ObjectStore with support for byte range requests (206 Partial Content).

### Catalog Endpoint

Returns current segment metadata and S3 storage options for all tables:

```json
{
  "tables": {
    "events": {
      "active_segment": 3,
      "sealed_segment": 2,
      "s3_dataset_uri": "s3://bucket/lance/events",
      "active_version": 15,
      "sealed_version": 8,
      "s3_storage_options": {
        "region": "us-east-1",
        "endpoint": "https://s3.amazonaws.com",
        "allow_http": "false"
      }
    }
  }
}
```

Credential-related keys (access_key, secret, token, password) are
automatically filtered from `s3_storage_options`.

### WebSocket Protocol

The WebSocket endpoint provides real-time catalog event streaming and version
pinning for compaction safety.

**Server &rarr; Client messages**:
```json
{"type": "session", "session_id": 456}
{"seq": 123, "event": {"type": "active_version_bumped", "table": "events", "version": 15}}
{"seq": 124, "event": {"type": "segment_sealed", "table": "events", "active_version": 16, "sealed_version": 15}}
{"type": "snapshot_required"}
```

**Client &rarr; Server messages**:
```json
{"type": "pin", "table": "events", "tier": "active", "version": 15}
{"type": "unpin", "table": "events", "tier": "active", "version": 14}
{"type": "heartbeat"}
```

**Delta sync**: Clients connect with `?since=last_seq` to receive only events
they missed. If the WAL has been compacted past the client's position, the
server sends `snapshot_required`.

**Version pinning**: Pinned versions prevent server-side compaction from
deleting dataset files that clients are actively reading. Sessions expire
after 30 seconds without a heartbeat.

### BisqueRoutingStore

The client-side `ObjectStore` implementation that routes reads by data
temperature:

- **Hot/warm paths** (`{table}/active/...`, `{table}/sealed/...`) — HTTP
  requests to the cluster's S3 API
- **Cold paths** (deep storage) — Delegates to a real S3 `ObjectStore`
  resolved via `CredentialConfig`

The segment catalog is cached with a 5-second TTL. Cold store resolution
merges server-provided options (region, endpoint) with client credentials.

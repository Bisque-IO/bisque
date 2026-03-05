# bisque-lance

A distributed, Raft-replicated storage engine built on the
[Lance](https://lancedb.github.io/lance/) columnar format. bisque-lance
manages the full lifecycle of data from ingestion through local NVMe segments
to deep S3 archival, with support for multiple independent tables, real-time
catalog sync, and integrated OpenTelemetry observability.

## Features at a Glance

- **Hot-cold LSM storage** with automatic tiering: active (NVMe) &rarr; sealed
  (NVMe, indexed) &rarr; deep (S3)
- **Raft-replicated writes** for strong consistency across a cluster
- **Multiple query protocols**: Arrow Flight SQL, PostgreSQL wire protocol,
  S3-compatible HTTP API
- **Client-side query execution** via `BisqueClient` with auto-syncing datasets
  and local SQL through DataFusion
- **OpenTelemetry receiver** with OTLP gRPC, OTel-Arrow, and Grafana-compatible
  query APIs (Tempo, Prometheus, Loki)
- **Write batching and pre-aggregation** with pluggable processors (counters,
  gauges, histograms)
- **Crash-consistent metadata** via MDBX manifest with WAL-based client delta sync
- **Automatic indexing** on sealed segments (FTS, vector, BTree, RTree)
- **Version pinning** for compaction safety across remote clients

## Documentation

| Page | Description |
|------|-------------|
| [Architecture](architecture.md) | High-level system design and component overview |
| [Storage Engine](storage-engine.md) | Table engine, segment lifecycle, compaction, indexing |
| [Write Path](write-path.md) | Write batching, processors, async apply, backpressure |
| [Query Engines](query-engines.md) | Flight SQL, PostgreSQL, S3 API, DataFusion integration |
| [Client](client.md) | BisqueClient auto-sync, delta sync, lazy datasets, version pinning |
| [OpenTelemetry](opentelemetry.md) | OTLP receiver, OTel-Arrow, Tempo/Prometheus/Loki APIs |
| [Raft & Recovery](raft-recovery.md) | Raft integration, MDBX manifest, crash recovery, catalog events |
| [Configuration](configuration.md) | All configuration options with defaults |

## Quick Start

```rust
use bisque_lance::*;
use std::sync::Arc;
use std::time::Duration;

// 1. Create the storage engine
let config = BisqueLanceConfig::new("/data")
    .with_seal_max_age(Duration::from_secs(60))
    .with_s3_uri("s3://my-bucket/lance");
let engine = Arc::new(BisqueLance::open(config).await?);

// 2. Wire into Raft
let (sm, watermark) = LanceStateMachine::with_async_apply(
    engine.clone(), AsyncApplyConfig::default()
);
let raft_node = Arc::new(
    LanceRaftNode::new(raft, engine, node_id)
        .with_write_batcher(WriteBatcherConfig::default())
        .with_applied_watermark(watermark)
        .with_manifest(manifest, group_id)
);
raft_node.start();

// 3. Expose query protocols
serve_flight(raft_node.clone(), flight_addr).await?;
serve_postgres(raft_node.clone(), PostgresServerConfig::default()).await?;
serve_s3(engine, s3_addr, Some(catalog_bus), Some(manifest), group_id).await?;

// 4. Write data
let result = raft_node.write_records("events", &batches).await?;
// result.log_index can be used for read-after-write fencing
```

## Client-Side Queries

```rust
use bisque_lance::{BisqueClient, CredentialConfig};

let credentials = Arc::new(CredentialConfig::new()
    .with_credential("aws_access_key_id", "AKIA...")
    .with_credential("aws_secret_access_key", "..."));

let client = BisqueClient::connect(
    "http://cluster:3300", "my-bucket",
    credentials, Some(Path::new(".bisque_client")),
).await?;

let batches = client.sql("SELECT * FROM events WHERE user_id = 'alice'").await?;
client.close().await;
```

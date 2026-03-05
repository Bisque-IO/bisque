# Configuration Reference

## BisqueLanceConfig

Node-wide defaults applied to all tables. Created with `BisqueLanceConfig::new(dir)`.

```rust
let config = BisqueLanceConfig::new("/data")
    .with_seal_max_age(Duration::from_secs(300))
    .with_seal_max_size(256 * 1024 * 1024)
    .with_s3_uri("s3://my-bucket/lance")
    .with_s3_storage_options(HashMap::from([
        ("region".into(), "us-east-1".into()),
    ]))
    .with_seal_index(IndexSpec::fts("text_column"))
    .with_seal_index(IndexSpec::btree("timestamp"));
```

### Storage

| Parameter | Default | Description |
|-----------|---------|-------------|
| `local_data_dir` | *required* | Base directory for segment files. Layout: `{dir}/tables/{name}/segments/` |

### Segment Sealing

| Parameter | Default | Description |
|-----------|---------|-------------|
| `seal_max_age` | 60 seconds | Seal active segment after this duration |
| `seal_max_size` | 1 GB | Seal active segment when bytes exceed this |
| `seal_indices` | empty | Indices to create on sealed segments |

### Deep Storage (S3)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `s3_uri` | None | S3 base URI (e.g., `s3://bucket/prefix`). None disables deep storage |
| `s3_storage_options` | empty | AWS credentials, region, endpoint |
| `s3_max_rows_per_file` | 5,000,000 | Target rows per Lance file in S3 |
| `s3_max_rows_per_group` | 50,000 | Row group size within files |

### Compaction

| Parameter | Default | Description |
|-----------|---------|-------------|
| `compaction_target_rows_per_fragment` | 1,000,000 | Target fragment size |
| `compaction_materialize_deletions` | true | Rewrite fragments with deletions |
| `compaction_deletion_threshold` | 0.1 | Fraction of deleted rows to trigger rewrite |
| `compaction_min_fragments` | 4 | Minimum fragments before compacting |

## IndexSpec

Indices created automatically on sealed segments.

```rust
IndexSpec::fts("body")              // Full-text search (inverted index)
IndexSpec::vector("embedding")      // Vector search (IVF-HNSW-SQ)
IndexSpec::btree("timestamp")       // Scalar range queries (B-tree)
IndexSpec::rtree("location")        // Spatial queries (R-tree)
```

All Lance index types are supported: `Scalar`, `BTree`, `Bitmap`, `Inverted`,
`RTree`, `IvfHnswSq`, `IvfHnswPq`, `IvfPq`.

## LanceRaftNode

Operational intervals for background tasks.

```rust
let node = LanceRaftNode::new(raft, engine, node_id)
    .with_group_id(1)
    .with_seal_check_interval(Duration::from_secs(5))
    .with_flush_check_interval(Duration::from_secs(10))
    .with_compaction_check_interval(Duration::from_secs(60));
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `group_id` | 0 | Raft group ID (for per-group MDBX storage) |
| `seal_check_interval` | 5 seconds | How often to check seal thresholds |
| `flush_check_interval` | 10 seconds | How often to attempt S3 flush |
| `compaction_check_interval` | 60 seconds | How often to compact fragments |

## WriteBatcherConfig

Per-table write coalescing configuration.

```rust
let config = WriteBatcherConfig::default()
    .with_linger(Duration::from_millis(5))
    .with_max_batch_bytes(8 * 1024 * 1024)
    .with_channel_capacity(1024)
    .with_processor(Arc::new(CounterAggregator::new(...)));
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `linger` | 5ms | Coalescing window after first write arrives |
| `max_batch_bytes` | 8 MB | Flush early when accumulated bytes exceed this |
| `channel_capacity` | 1024 | Per-table crossfire channel depth |
| `processor` | None | Optional `WriteProcessor` for pre-aggregation |

## AsyncApplyConfig

Controls the async apply buffer for decoupling Lance I/O from Raft apply.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_pending_bytes` | 64 MB | Backpressure threshold — enqueue blocks when exceeded |

## CredentialConfig

Client-side S3 credential management.

```rust
let config = CredentialConfig::new()
    .with_credential("aws_access_key_id", "AKIA...")
    .with_credential("aws_secret_access_key", "...")
    .with_credential("aws_region", "us-east-1");

// Per-table overrides
let config = config.with_table_credentials("audit", HashMap::from([
    ("aws_access_key_id".into(), "OTHER_KEY".into()),
    ("aws_secret_access_key".into(), "OTHER_SECRET".into()),
]));
```

### Supported Options

| Option | Aliases | Description |
|--------|---------|-------------|
| `aws_access_key_id` | `access_key_id` | AWS access key |
| `aws_secret_access_key` | `secret_access_key` | AWS secret key |
| `aws_session_token` | `session_token` | Temporary session token |
| `aws_region` | `region` | AWS region |
| `aws_endpoint` | `endpoint`, `endpoint_url` | Custom S3 endpoint |
| `aws_allow_http` | `allow_http` | Allow HTTP (not HTTPS) |
| `aws_virtual_hosted_style` | `virtual_hosted_style_request` | Virtual hosted style |

### Merge Order

When resolving an `ObjectStore`, options are merged (later wins):

1. **Server-provided options** — Region, endpoint, etc. from the catalog API
2. **Client default credentials** — From `with_credential()`
3. **Per-table overrides** — From `with_table_credentials()`

Opened `ObjectStore` instances are cached by bucket URI + credential
fingerprint.

## PostgresServerConfig

```rust
let config = PostgresServerConfig {
    host: "127.0.0.1".into(),
    port: 5432,
};
serve_postgres(raft_node, config).await?;
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | `127.0.0.1` | Bind address |
| `port` | 5432 | Listen port |

## S3 Server

```rust
serve_s3(
    engine,                    // Arc<BisqueLance>
    addr,                      // SocketAddr
    Some(catalog_bus),         // Optional CatalogEventBus for WebSocket
    Some(manifest),            // Optional LanceManifestManager for WAL replay
    group_id,                  // Raft group ID
).await?;
```

### Version Pin Tracker

| Parameter | Default | Description |
|-----------|---------|-------------|
| `lease_timeout` | 30 seconds | Session expiry without heartbeat |
| Reap interval | 10 seconds | Background cleanup of expired sessions |

### Catalog WAL

| Parameter | Default | Description |
|-----------|---------|-------------|
| `WAL_MAX_BYTES` | 16 MB | Maximum WAL size before oldest entries are compacted |

## OtlpReceiver

```rust
let otlp = OtlpReceiver::new(raft_node);
otlp.ensure_tables().await?;

serve_otlp(otlp.clone(), "0.0.0.0:4317".parse()?).await?;
serve_http(otlp, "0.0.0.0:3200".parse()?).await?;
```

### Default Ports

| Service | Default Port | Description |
|---------|-------------|-------------|
| OTLP gRPC | 4317 | OTLP protobuf + OTel-Arrow |
| HTTP query APIs | 3200 | Tempo + Prometheus + Loki |

### Processor Defaults

| Table | Processor | Timestamp Resolution |
|-------|-----------|---------------------|
| `otel_counters` | `OtelSumAggregator` | 60 seconds |
| `otel_gauges` | `OtelGaugeProcessor` | N/A (last-write-wins) |
| `otel_histograms` | `OtelHistogramProcessor` | N/A (merge buckets) |
| `otel_exp_histograms` | `OtelExpHistogramAggregator` | N/A (merge buckets) |
| `otel_summaries` | None | N/A (append-only) |
| Trace/log tables | None | N/A (append-only) |

## MDBX Manifest

```rust
let manifest = Arc::new(LanceManifestManager::new(base_dir));
manifest.open_group(group_id).await?;
```

### Client-Side MDBX

| Parameter | Default | Description |
|-----------|---------|-------------|
| Max DB size | 64 MB | Maximum MDBX database size |
| Min DB size | 64 KB | Initial database size |
| Growth step | 64 KB | Growth increment |
| Shrink threshold | 256 KB | Threshold before shrinking |
| Max tables | 4 | MDBX table limit (client_meta + client_tables + spare) |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Tracing filter (e.g., `warn`, `bisque_lance=debug`) |
| `AWS_ACCESS_KEY_ID` | AWS credentials (used by ObjectStore if not set in config) |
| `AWS_SECRET_ACCESS_KEY` | AWS credentials |
| `AWS_DEFAULT_REGION` | AWS region |
| `AWS_ENDPOINT_URL` | Custom S3 endpoint |

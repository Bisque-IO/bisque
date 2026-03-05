# Write Path

All writes in bisque-lance flow through Raft consensus. The write path
includes optional batching, pre-aggregation via pluggable processors, and
asynchronous materialization to Lance datasets with backpressure control.

## Overview

```
Client                WriteBatcher              Raft              State Machine
  │                       │                      │                      │
  │── write_records() ──► │                      │                      │
  │                       │── linger (5ms) ──    │                      │
  │                       │── coalesce IPC ──    │                      │
  │                       │── processor? ──      │                      │
  │                       │── propose ──────────►│                      │
  │                       │                      │── replicate ──────► │
  │                       │                      │                     │── apply_append()
  │                       │                      │◄── response ───────│
  │◄── WriteResult ──────│◄── ack ──────────────│                      │
```

Every write returns a `WriteResult { log_index }` — the Raft log index where
the write was committed. This can be used for read-after-write fencing.

## Write Batcher

The `WriteBatcher` coalesces per-table writes into batched Raft proposals,
reducing proposal overhead by 10-100x in typical metrics workloads.

### How It Works

Each table gets its own batcher loop (spawned lazily on first write):

1. **Wait for first request** — Block until data arrives on the per-table
   channel
2. **Linger window** — Wait up to `linger` duration (default 5ms) for more
   writes to accumulate. If accumulated bytes exceed `max_batch_bytes`, flush
   immediately
3. **Greedy drain** — After linger expires, non-blocking `try_recv()` grabs
   all queued requests
4. **Coalesce** — Flatten all `RecordBatch` vectors into a single collection
5. **Process** — Apply optional `WriteProcessor` (pre-aggregation)
6. **Propose** — IPC-encode and submit as a single `AppendRecords` Raft
   proposal
7. **Broadcast** — Send `WriteResult` or error to all pending callers via
   oneshot channels

### Configuration

```rust
let config = WriteBatcherConfig::default()
    .with_linger(Duration::from_millis(5))
    .with_max_batch_bytes(8 * 1024 * 1024)
    .with_channel_capacity(1024)
    .with_processor(Arc::new(CounterAggregator::new(...)));
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `linger` | 5ms | Coalescing window after first write |
| `max_batch_bytes` | 8 MB | Flush early if accumulated bytes exceed this |
| `channel_capacity` | 1024 | Per-table crossfire channel depth |
| `processor` | None | Optional `WriteProcessor` for pre-aggregation |

Per-table configuration is supported:

```rust
raft_node.configure_table_batcher("otel_counters", config)?;
```

Batcher configuration is persisted to the MDBX manifest and restored on
startup.

### Materialized Writes

Processors can emit `MaterializedWrite` entries — additional batches destined
for other tables. These become separate `AppendRecords` Raft proposals.
Failures in materialized writes are logged but do not fail the primary write.

## Write Processors

The `WriteProcessor` trait allows pre-aggregation of data before it enters
the Raft log. This reduces write amplification and storage costs, especially
for high-cardinality metrics.

```rust
pub trait WriteProcessor: Send + Sync + 'static {
    /// Transform accumulated batches before Raft proposal.
    fn process(&self, batches: Vec<RecordBatch>) -> ProcessorOutput;

    /// Flush any internal state on shutdown.
    fn drain(&self) -> Vec<MaterializedWrite> { Vec::new() }

    /// Metadata for crash-recovery reconstruction.
    fn descriptor(&self) -> Option<ProcessorDescriptor> { None }
}
```

### CounterAggregator

Sums counter values per composite key within each batch window.

**Grouping**: All UTF8 key columns + truncated timestamp form the composite key.
Timestamps are floored to a configurable resolution boundary (default: 60
seconds).

**Aggregation**:
- `value` — summed across all rows for each key (converted to f64)
- `timestamp` — truncated to resolution boundary, included in key
- `start_time` — minimum across group (preserved)
- Passthrough columns — first row wins

```rust
let processor = CounterAggregator::new(
    vec!["metric_name".into(), "attributes".into()],
    "value_double",
)
.with_timestamp("timestamp", 60_000)  // 60s resolution
.with_start_time("start_time");
```

**Example**: 100 counter increments for the same metric within a 60-second
window are reduced to a single row with the summed value.

### GaugeAggregator

Last-write-wins aggregation for gauge metrics.

**Grouping**: All UTF8 key columns (no timestamp in key).

**Aggregation**: Each new row for a key completely replaces the previous value.
The last-seen value, timestamp, and start_time win.

```rust
let processor = GaugeAggregator::new(
    vec!["metric_name".into(), "attributes".into()],
    "value_double",
)
.with_timestamp("timestamp");
```

**Example**: 50 CPU percentage readings within a batch window collapse to the
single most recent reading per host.

### HistogramAggregator

Merges histogram buckets across data points for the same key.

**Grouping**: All UTF8 key columns.

**Aggregation**:
- `bucket_counts` — element-wise sum (additive)
- `boundaries` — first row's boundaries win (assumed identical per key)
- `sum` — additive (null-safe)
- `count` — additive
- `min` — minimum of minimums (null-safe)
- `max` — maximum of maximums (null-safe)
- `timestamp` — maximum (latest)
- `start_time` — minimum (earliest)

```rust
let processor = HistogramAggregator::new(
    vec!["metric_name".into(), "attributes".into()],
)
.with_column_names("boundaries", "bucket_counts", "sum", "count")
.with_timestamp("timestamp")
.with_min_column("min")
.with_max_column("max");
```

### OTel-Specific Processors

Four processors tailored for OpenTelemetry metrics, using the 11-column OTel
key structure (metric_name, description, unit, metadata, attributes,
resource_attributes, resource_schema_url, scope_name, scope_version,
scope_attributes, scope_schema_url):

| Processor | Strategy | Used By |
|-----------|----------|---------|
| `OtelSumAggregator` | Sum values, truncate timestamps to 60s | `otel_counters` table |
| `OtelGaugeProcessor` | Last-write-wins | `otel_gauges` table |
| `OtelHistogramProcessor` | Merge buckets element-wise | `otel_histograms` table |
| `OtelExpHistogramAggregator` | Merge exponential buckets | `otel_exp_histograms` table |

These are automatically configured when `OtlpReceiver::ensure_tables()` creates
the OTel tables.

### Processor Persistence

Processors are serialized via `ProcessorDescriptor` for crash recovery:

```rust
pub enum ProcessorDescriptor {
    Counter { key_columns, value_column, timestamp_column, ... },
    Gauge { key_columns, value_column, ... },
    Histogram { key_columns, ... },
    OtelSum { timestamp_resolution_ms },
    OtelGauge,
    OtelHistogram,
    OtelExpHistogram,
}
```

On startup, persisted descriptors are used to reconstruct processor instances
without user intervention.

## Async Apply

The `AsyncApplyBuffer` decouples Lance I/O from the Raft apply path. Since
the Raft log is the durable WAL, Lance dataset writes can safely lag behind
without risking data loss.

### Architecture

```
Raft apply()              AsyncApplyBuffer              Lance I/O
     │                          │                           │
     │── enqueue(log_id, ──►    │                           │
     │   table, data)           │── accumulate per-table ── │
     │                          │                           │
     │── enqueue(...) ────►     │                           │
     │                          │── flush threshold ──────► │
     │                          │                      apply_append()
     │                          │◄── done ─────────────────│
     │                          │── update watermark ──     │
     │                          │                           │
```

### Backpressure

When pending bytes exceed `max_pending_bytes` (default 64 MB):

1. `enqueue()` spin-waits with `yield_now()` until space is available
2. This blocks the state machine's `apply()` method
3. Which blocks Raft from accepting new proposals
4. Natural flow control: Lance I/O throughput determines ingestion rate

```rust
// Backpressure loop in enqueue()
while pending_bytes.load() > max_pending_bytes {
    tokio::task::yield_now().await;
}
pending_bytes.fetch_add(data_len);
```

### Read-After-Write Fencing

The `AppliedWatermark` tracks the highest Raft log index materialized to Lance:

```rust
let result = raft_node.write_records("events", &batches).await?;
// result.log_index = 42

// Query handler waits for materialization:
let ready = watermark.wait_for(42, Duration::from_secs(5)).await;
```

In Flight SQL, the `x-bisque-min-log-id` header triggers this fence
automatically. Without async apply, writes are applied synchronously and the
fence is unnecessary.

### Per-Table Ordering

The async buffer maintains log order within each table but may parallelize
across tables. This means:
- Writes to the same table are always applied in Raft log order
- Writes to different tables may complete out of order
- The watermark reports the highest log index that has been fully applied

## Binary Codec

Commands are serialized with a compact binary codec for Raft log entries:

| Command | Discriminant | Format |
|---------|-------------|--------|
| `AppendRecords` | 0 | `[1B disc][2B name_len][name][4B data_len][IPC data]` |
| `SealActiveSegment` | 1 | `[1B disc][2B name_len][name][8B sealed_id][8B new_id][1B reason]` |
| `BeginFlush` | 2 | `[1B disc][2B name_len][name][8B segment_id]` |
| `PromoteToDeepStorage` | 3 | `[1B disc][2B name_len][name][8B segment_id][8B version]` |
| `CreateTable` | 4 | `[1B disc][2B name_len][name][4B schema_len][IPC schema]` |
| `DropTable` | 5 | `[1B disc][2B name_len][name]` |

**Zero-copy decoding**: `AppendRecords` and `CreateTable` use `Bytes::slice()`
to avoid copying large IPC payloads from mmap-backed buffers. The codec's
`decode_from_bytes()` override extracts zero-copy views directly from the
source buffer.

# OpenTelemetry Integration

bisque-lance includes a full-featured OpenTelemetry receiver that ingests
metrics, traces, and logs via OTLP gRPC (with optional OTel-Arrow streaming),
stores them in Lance tables, and exposes Grafana-compatible query APIs for
Tempo, Prometheus, and Loki.

## Setup

```rust
let otlp = OtlpReceiver::new(raft_node);
otlp.ensure_tables().await?;  // Auto-creates all 10 tables

// gRPC receiver (OTLP + OTel-Arrow)
serve_otlp(otlp.clone(), "0.0.0.0:4317".parse()?).await?;

// HTTP query APIs (Tempo + Prometheus + Loki)
serve_http(otlp, "0.0.0.0:3200".parse()?).await?;
```

## Ingestion

### OTLP gRPC (Port 4317)

Standard OTLP protobuf services with gzip compression:

| Service | Method | Input |
|---------|--------|-------|
| `MetricsService` | `Export` | `ExportMetricsServiceRequest` |
| `TraceService` | `Export` | `ExportTraceServiceRequest` |
| `LogsService` | `Export` | `ExportLogsServiceRequest` |

Metrics are decomposed by type — each OTLP metric type maps to its own table.
Traces are split into spans, events, and links. Logs go to a single table.

All writes flow through Raft consensus with per-table write batching and
pre-aggregation processors.

### OTel-Arrow Protocol

Bidirectional Arrow IPC streaming over gRPC for high-throughput ingestion:

| Service | Method | Stream |
|---------|--------|--------|
| `ArrowTracesService` | `ArrowTraces` | `BatchArrowRecords` &harr; `BatchStatus` |
| `ArrowLogsService` | `ArrowLogs` | `BatchArrowRecords` &harr; `BatchStatus` |
| `ArrowMetricsService` | `ArrowMetrics` | `BatchArrowRecords` &harr; `BatchStatus` |

Each `BatchArrowRecords` contains `ArrowPayload` entries with Arrow IPC-encoded
RecordBatches. Payloads are routed to tables based on their `ArrowPayloadType`:

| Payload Type | Table |
|-------------|-------|
| `Spans` (40) | `otel_spans` |
| `SpanEvents` (42) | `otel_span_events` |
| `SpanLinks` (43) | `otel_span_links` |
| `Logs` (30) | `otel_logs` |
| `NumberDataPoints` (11) | `otel_counters` |
| `HistogramDataPoints` (13) | `otel_histograms` |
| `ExpHistogramDataPoints` (14) | `otel_exp_histograms` |
| `NumberDpExemplars` (19) | `otel_exemplars` |
| Other attribute/resource types | Auto-created `otap_*` tables |

Tables are auto-created on first payload using the incoming Arrow schema.

### OTLP HTTP Ingest

Standard OTLP HTTP endpoints (protobuf or JSON, with optional gzip):

| Method | Path | Signal |
|--------|------|--------|
| POST | `/v1/traces` | Traces |
| POST | `/v1/metrics` | Metrics |
| POST | `/v1/logs` | Logs |

## Tables & Schemas

### Metric Tables

All metric tables share 11 UTF8 key columns used for grouping by processors:

1. `metric_name`, `description`, `unit`, `metadata`
2. `attributes`, `resource_attributes`, `resource_schema_url`
3. `scope_name`, `scope_version`, `scope_attributes`, `scope_schema_url`

#### otel_counters

OTLP Sum metrics (monotonic and non-monotonic counters).

| Column | Type | Notes |
|--------|------|-------|
| *11 key columns* | Utf8 | Group key for aggregation |
| `timestamp` | Timestamp[ns] | Data point timestamp |
| `start_time` | Timestamp[ns] | Data point start time |
| `value_int` | Int64 (nullable) | Integer value |
| `value_double` | Float64 (nullable) | Double value |
| `aggregation_temporality` | Int32 | OTLP enum (cumulative/delta) |
| `is_monotonic` | Boolean | Monotonic counter flag |
| `flags` | UInt32 | OTLP data point flags |
| `resource_dropped_attributes_count` | UInt32 | |
| `scope_dropped_attributes_count` | UInt32 | |

**Processor**: `OtelSumAggregator` — sums values per key with 60-second
timestamp truncation.

#### otel_gauges

OTLP Gauge metrics (point-in-time measurements).

Same as counters but without `aggregation_temporality` and `is_monotonic`.

**Processor**: `OtelGaugeProcessor` — last-write-wins per key.

#### otel_histograms

OTLP Histogram metrics (latency distributions, request sizes).

| Column | Type | Notes |
|--------|------|-------|
| *11 key columns* | Utf8 | Group key |
| `timestamp` | Timestamp[ns] | |
| `start_time` | Timestamp[ns] | |
| `boundaries` | List&lt;Float64&gt; | Bucket boundaries |
| `bucket_counts` | List&lt;UInt64&gt; | Count per bucket |
| `sum` | Float64 (nullable) | Sum of observations |
| `count` | UInt64 | Total observations |
| `min` | Float64 (nullable) | Minimum value |
| `max` | Float64 (nullable) | Maximum value |
| `aggregation_temporality` | Int32 | |
| `flags` | UInt32 | |
| ...dropped counts | UInt32 | |

**Processor**: `OtelHistogramProcessor` — merges buckets element-wise per key.

#### otel_exp_histograms

OTLP Exponential Histogram metrics.

| Column | Type | Notes |
|--------|------|-------|
| *11 key columns* | Utf8 | Group key |
| `timestamp`, `start_time` | Timestamp[ns] | |
| `scale` | Int32 | Exponential scale factor |
| `zero_count` | UInt64 | Near-zero observations |
| `zero_threshold` | Float64 | Zero bucket threshold |
| `positive_offset` | Int32 | Positive bucket offset |
| `positive_bucket_counts` | List&lt;UInt64&gt; | Positive buckets |
| `negative_offset` | Int32 | Negative bucket offset |
| `negative_bucket_counts` | List&lt;UInt64&gt; | Negative buckets |
| `sum`, `count`, `min`, `max` | various | Aggregate stats |
| `aggregation_temporality` | Int32 | |

**Processor**: `OtelExpHistogramAggregator` — merges exponential buckets per key.

#### otel_summaries

OTLP Summary metrics (append-only, no processor).

| Column | Type |
|--------|------|
| `metric_name` ... `scope_schema_url` | Utf8 |
| `timestamp`, `start_time` | Timestamp[ns] |
| `count` | UInt64 |
| `sum` | Float64 |
| `quantile_values` | Utf8 (JSON array) |
| `flags` | UInt32 |

#### otel_exemplars

Exemplars linked to metrics for cross-signal correlation.

| Column | Type |
|--------|------|
| `metric_name` | Utf8 |
| `metric_table` | Utf8 (source table name) |
| `attributes` | Utf8 (JSON) |
| `timestamp` | Timestamp[ns] |
| `trace_id` | FixedSizeBinary[16] |
| `span_id` | FixedSizeBinary[8] |
| `value_int` | Int64 (nullable) |
| `value_double` | Float64 (nullable) |

### Trace Tables

#### otel_spans

| Column | Type | Notes |
|--------|------|-------|
| `trace_id` | FixedSizeBinary[16] | 128-bit trace ID |
| `span_id` | FixedSizeBinary[8] | 64-bit span ID |
| `parent_span_id` | FixedSizeBinary[8] | Parent (zeros = root) |
| `trace_state` | Utf8 | W3C trace state |
| `name` | Utf8 | Operation name |
| `kind` | Int32 | SpanKind enum |
| `start_time` | Timestamp[ns] | |
| `end_time` | Timestamp[ns] | |
| `attributes` | Utf8 | JSON-encoded key-value pairs |
| `status_code` | Int32 | Status enum |
| `status_message` | Utf8 | |
| `resource_attributes` | Utf8 | JSON-encoded |
| `resource_schema_url` | Utf8 | |
| `scope_name`, `scope_version` | Utf8 | |
| `scope_attributes` | Utf8 | JSON-encoded |
| `flags` | UInt32 | |
| `dropped_*_count` | UInt32 | Attributes, events, links |

#### otel_span_events

| Column | Type |
|--------|------|
| `trace_id` | FixedSizeBinary[16] |
| `span_id` | FixedSizeBinary[8] |
| `timestamp` | Timestamp[ns] |
| `name` | Utf8 |
| `attributes` | Utf8 (JSON) |
| `dropped_attributes_count` | UInt32 |

#### otel_span_links

| Column | Type |
|--------|------|
| `trace_id` | FixedSizeBinary[16] |
| `span_id` | FixedSizeBinary[8] |
| `linked_trace_id` | FixedSizeBinary[16] |
| `linked_span_id` | FixedSizeBinary[8] |
| `trace_state` | Utf8 |
| `attributes` | Utf8 (JSON) |
| `flags` | UInt32 |
| `dropped_attributes_count` | UInt32 |

### Log Table

#### otel_logs

| Column | Type | Notes |
|--------|------|-------|
| `timestamp` | Timestamp[ns] | Event time |
| `observed_timestamp` | Timestamp[ns] | Collection time |
| `trace_id` | FixedSizeBinary[16] | For trace correlation |
| `span_id` | FixedSizeBinary[8] | |
| `severity_number` | Int32 | OTLP SeverityNumber enum |
| `severity_text` | Utf8 | e.g., "INFO", "ERROR" |
| `body` | Utf8 | Log body (JSON-serialized from AnyValue) |
| `body_type` | Utf8 | Original AnyValue type name |
| `attributes` | Utf8 | JSON-encoded |
| `resource_attributes` | Utf8 | JSON-encoded |
| `scope_name`, `scope_version` | Utf8 | |
| `scope_attributes` | Utf8 | JSON-encoded |
| `event_name` | Utf8 | |
| `flags` | UInt32 | |
| `dropped_attributes_count` | UInt32 | |

## Grafana-Compatible Query APIs

### Tempo (Trace Query)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/traces/{traceID}` | Fetch complete trace by 32-hex-char ID |
| GET | `/api/search` | Search traces by tags, duration, time range |
| GET | `/api/search/tags` | List available tag names |
| GET | `/api/search/tag/{tag}/values` | List values for a tag |
| GET | `/api/v2/search/tags` | Tags grouped by scope (span/resource/scope) |
| GET | `/api/v2/search/tag/{tag}/values` | Typed tag values |

**GET /api/traces/{traceID}**

Fetches all spans, events, and links for a trace. Reconstructs the full
OTLP `TracesData` protobuf structure, grouping by resource and scope.
Supports `Accept: application/protobuf` and `application/json`.

**GET /api/search**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `tags` | | Logfmt-style filters: `service.name=foo http.method=GET` |
| `minDuration` | | e.g., "100ms", "1.5s" |
| `maxDuration` | | e.g., "5s" |
| `limit` | 20 | Max results (capped at 1000) |
| `start` | | Unix epoch seconds |
| `end` | | Unix epoch seconds |

Returns trace summaries:

```json
{
  "traces": [
    {
      "traceID": "abc123...",
      "rootServiceName": "frontend",
      "rootTraceName": "GET /api/users",
      "startTimeUnixNano": "1700000000000000000",
      "durationMs": 42
    }
  ]
}
```

Tag matching checks both span `attributes` and `resource_attributes` via
JSON parsing. Root spans are identified by an all-zeros `parent_span_id`.

Duration parsing supports: `ns`, `us`/`µs`, `ms`, `s`, `m`, `h`.

### Prometheus / Mimir (Metric Query)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET/POST | `/api/v1/query` | PromQL instant query |
| GET/POST | `/api/v1/query_range` | PromQL range query |
| GET | `/api/v1/labels` | Label name discovery |
| GET | `/api/v1/label/{name}/values` | Label value enumeration |
| GET | `/api/v1/series` | Series matching |
| GET | `/api/v1/metadata` | Metric metadata (type, help, unit) |
| POST | `/api/v1/read` | Prometheus remote-read (Snappy protobuf) |
| POST | `/api/v1/push` | Prometheus remote-write (Snappy protobuf) |

**PromQL Support**

Queries are parsed by the `promql-parser` crate and translated to SQL +
post-processing:

- **Selectors**: `metric_name{label="value"}` &rarr; SQL WHERE on metric_name +
  post-filter on attributes
- **Range vectors**: `rate(counter[5m])` &rarr; fetch data, compute per-second
  rate in Rust
- **Aggregations**: `sum by (job) (rate(...))` &rarr; group by label, apply
  aggregate function

| Function | Status |
|----------|--------|
| `rate`, `increase`, `irate` | Supported |
| `sum`, `avg`, `min`, `max`, `count` | Supported |
| `topk`, `bottomk` | Supported |
| `abs`, `ceil`, `floor`, `round`, `sqrt`, `ln`, `log2`, `log10`, `exp` | Supported |
| `label_replace`, `label_join` | Recognized |
| `histogram_quantile` | In progress |

**Label Mapping**:
- `__name__` &rarr; `metric_name`
- `job` &rarr; `service.name` from `resource_attributes`
- `instance` &rarr; `service.instance.id` from `resource_attributes`
- Other labels &rarr; extracted from `attributes` JSON

**Response format** (Prometheus-compatible):

```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {"__name__": "http_requests_total", "job": "api"},
        "value": [1700000000, "42"]
      }
    ]
  }
}
```

Range queries return `"resultType": "matrix"` with `"values"` arrays.

**Remote-Read** (`POST /api/v1/read`):
Snappy-compressed protobuf `ReadRequest` with label matchers and time range.
Queries `otel_counters` and `otel_gauges` tables. Returns Snappy-compressed
`ReadResponse` with `TimeSeries` containing samples.

**Remote-Write** (`POST /api/v1/push`):
Snappy-compressed protobuf `WriteRequest`. Routes to `otel_counters` or
`otel_gauges` based on metric type metadata.

### Loki (Log Query)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/loki/api/v1/query` | LogQL instant query |
| GET | `/loki/api/v1/query_range` | LogQL range query |
| GET | `/loki/api/v1/labels` | Label name discovery |
| GET | `/loki/api/v1/label/{name}/values` | Label value enumeration |
| GET | `/loki/api/v1/series` | Series matching |
| POST | `/loki/api/v1/push` | Log push (Snappy protobuf or JSON) |

**LogQL Support**

```
{job="app"} |= "error" != "timeout" |~ "status_code=5\\d+"
```

**Stream selectors**: `{label="value"}` with operators `=`, `!=`, `=~`
(regex), `!~` (not regex).

**Pipeline filters**:
- `|= "text"` — line contains
- `!= "text"` — line does not contain
- `|~ "regex"` — line matches regex
- `!~ "regex"` — line does not match regex

**Metric queries**:
- `count_over_time({job="app"}[5m])` — count log entries in window
- `rate({job="app"}[5m])` — per-second rate
- `bytes_over_time({job="app"}[5m])` — sum of bytes
- `bytes_rate({job="app"}[5m])` — bytes per second

**Aggregations**: `sum`, `avg`, `min`, `max`, `count` with optional
`by(label1, label2)` grouping.

**Label mapping**:
- `job` &rarr; `service.name` from `resource_attributes`
- `level` &rarr; `severity_text`
- Other labels &rarr; extracted from `attributes` and `resource_attributes` JSON

**GET /loki/api/v1/query**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `query` | required | LogQL expression |
| `time` | now | Query time (nanoseconds or float seconds) |
| `limit` | 100 | Max results |
| `direction` | backward | "forward" or "backward" |

**Response format** (Loki-compatible):

```json
{
  "status": "success",
  "data": {
    "resultType": "streams",
    "result": [
      {
        "stream": {"job": "api", "level": "error"},
        "values": [
          ["1700000000000000000", "Connection refused to database"]
        ]
      }
    ]
  }
}
```

Metric queries return `"resultType": "vector"` with numeric values.

**POST /loki/api/v1/push**

Accepts Snappy-compressed protobuf or JSON:

```json
{
  "streams": [
    {
      "stream": {"job": "app", "level": "info"},
      "values": [
        ["1700000000000000000", "Request completed in 42ms"]
      ]
    }
  ]
}
```

Labels are parsed from the `stream` object. `job` maps to
`resource.service.name`, `level` maps to `severity_text`. Log lines are stored
in the `body` column of `otel_logs`.

## Pre-Aggregation Processors

Metric tables are configured with write processors that aggregate data within
each batch window before Raft proposal:

| Table | Processor | Strategy |
|-------|-----------|----------|
| `otel_counters` | `OtelSumAggregator` | Sum values, truncate timestamps to 60s |
| `otel_gauges` | `OtelGaugeProcessor` | Last-write-wins per key |
| `otel_histograms` | `OtelHistogramProcessor` | Merge buckets element-wise |
| `otel_exp_histograms` | `OtelExpHistogramAggregator` | Merge exponential buckets |
| `otel_summaries` | None | Append-only |
| Trace/log tables | None | Append-only |

All processors group on the 11 UTF8 key columns. This reduces write
amplification by collapsing high-frequency metrics into aggregated rows
before they enter the Raft log.

## Data Conversion

### Attribute Encoding

OTLP `KeyValue` arrays are serialized to JSON strings for storage:

```json
{"service.name": "frontend", "http.method": "GET", "http.status_code": 200}
```

This enables flexible querying via JSON functions in DataFusion SQL while
keeping the schema flat.

### Binary ID Handling

Trace IDs (16 bytes) and span IDs (8 bytes) are stored as
`FixedSizeBinary`. IDs shorter than the target width are zero-padded; longer
IDs are truncated.

### Number Value Encoding

OTLP `NumberDataPoint.value` is a oneof of `as_int` (int64) and `as_double`
(double). These map to two nullable columns (`value_int`, `value_double`)
where exactly one is non-null per row. Processors convert to float64 for
aggregation.

### Concurrent Writes

All metric type tables are written concurrently via `tokio::join!` after
decomposition:

```rust
let (counter_res, gauge_res, hist_res, exp_res, summary_res, exemplar_res) =
    tokio::join!(
        write_counters, write_gauges, write_histograms,
        write_exp_histograms, write_summaries, write_exemplars
    );
```

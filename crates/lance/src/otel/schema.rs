//! Arrow schema definitions for OpenTelemetry tables.
//!
//! Defines the columnar layout for each OTEL signal type stored in bisque-lance.
//! Schemas are cached via `LazyLock` to avoid per-request allocation.

use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, Field, Schema, TimeUnit};

// Table name constants
pub const COUNTERS_TABLE: &str = "otel_counters";
pub const GAUGES_TABLE: &str = "otel_gauges";
pub const HISTOGRAMS_TABLE: &str = "otel_histograms";
pub const SPANS_TABLE: &str = "otel_spans";
pub const LOGS_TABLE: &str = "otel_logs";

// Cached Arc<Schema> instances — allocated once, reused on every export call.

static COUNTER_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| Arc::new(build_counter_schema()));
static HISTOGRAM_SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(build_histogram_schema()));
static SPAN_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| Arc::new(build_span_schema()));
static LOG_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| Arc::new(build_log_schema()));

/// Schema for counter metrics (OTEL Sum data points).
pub fn counter_schema() -> Schema {
    build_counter_schema()
}

/// Cached `Arc<Schema>` for counter/gauge metrics.
pub fn counter_schema_ref() -> &'static Arc<Schema> {
    &COUNTER_SCHEMA
}

/// Schema for gauge metrics (OTEL Gauge data points).
/// Same layout as counters — only the write processor differs.
pub fn gauge_schema() -> Schema {
    build_counter_schema()
}

/// Schema for histogram metrics (OTEL Histogram data points).
pub fn histogram_schema() -> Schema {
    build_histogram_schema()
}

/// Cached `Arc<Schema>` for histogram metrics.
pub fn histogram_schema_ref() -> &'static Arc<Schema> {
    &HISTOGRAM_SCHEMA
}

/// Schema for trace spans.
pub fn span_schema() -> Schema {
    build_span_schema()
}

/// Cached `Arc<Schema>` for trace spans.
pub fn span_schema_ref() -> &'static Arc<Schema> {
    &SPAN_SCHEMA
}

/// Schema for log records.
pub fn log_schema() -> Schema {
    build_log_schema()
}

/// Cached `Arc<Schema>` for log records.
pub fn log_schema_ref() -> &'static Arc<Schema> {
    &LOG_SCHEMA
}

// ---------------------------------------------------------------------------
// Schema builders (called once by LazyLock, or by the non-cached accessors
// used during table creation where a plain Schema is needed).
// ---------------------------------------------------------------------------

fn build_counter_schema() -> Schema {
    Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
    ])
}

fn build_histogram_schema() -> Schema {
    Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "boundaries",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            false,
        ),
        Field::new(
            "bucket_counts",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            false,
        ),
        Field::new("sum", DataType::Float64, false),
        Field::new("count", DataType::UInt64, false),
    ])
}

fn build_span_schema() -> Schema {
    Schema::new(vec![
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        Field::new("parent_span_id", DataType::FixedSizeBinary(8), false),
        Field::new("trace_state", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Int32, false),
        Field::new(
            "start_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "end_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("status_code", DataType::Int32, false),
        Field::new("status_message", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
    ])
}

fn build_log_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "observed_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        Field::new("severity_number", DataType::Int32, false),
        Field::new("severity_text", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
    ])
}

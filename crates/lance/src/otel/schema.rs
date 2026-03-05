//! Arrow schema definitions for OpenTelemetry tables.
//!
//! Defines the columnar layout for each OTEL signal type stored in bisque-lance.
//! Schemas are cached via `LazyLock` to avoid per-request allocation.
//!
//! Every field from the OTLP protobuf specification (opentelemetry-proto v0.31.0)
//! is captured, including resource/scope enrichment, schema URLs, aggregation
//! metadata, data point flags, and all metric types (including Summary).
//!
//! ## Metric table column ordering
//!
//! Metric tables (counters, gauges, histograms) use a specific column order
//! designed for compatibility with write processors (CounterAggregator, etc.):
//!
//! 1. Utf8 key columns (used as processor group key)
//! 2. Timestamp columns
//! 3. Value/data columns
//! 4. Non-Utf8 passthrough columns (stable within a group, preserved as-is)
//!
//! ## Number value columns
//!
//! OTLP `NumberDataPoint.value` is a oneof of `as_double` (double) and
//! `as_int` (sfixed64).  To preserve full fidelity we store two nullable
//! columns — `value_int` (Int64) and `value_double` (Float64) — exactly one
//! of which is non-null per row.

use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, Field, Schema, TimeUnit};

// ---------------------------------------------------------------------------
// Table name constants
// ---------------------------------------------------------------------------

pub const COUNTERS_TABLE: &str = "otel_counters";
pub const GAUGES_TABLE: &str = "otel_gauges";
pub const HISTOGRAMS_TABLE: &str = "otel_histograms";
pub const EXP_HISTOGRAMS_TABLE: &str = "otel_exp_histograms";
pub const SUMMARIES_TABLE: &str = "otel_summaries";
pub const SPANS_TABLE: &str = "otel_spans";
pub const SPAN_EVENTS_TABLE: &str = "otel_span_events";
pub const SPAN_LINKS_TABLE: &str = "otel_span_links";
pub const LOGS_TABLE: &str = "otel_logs";
pub const EXEMPLARS_TABLE: &str = "otel_exemplars";

// ---------------------------------------------------------------------------
// Cached Arc<Schema> instances — allocated once, reused on every export call.
// ---------------------------------------------------------------------------

static COUNTER_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| Arc::new(build_counter_schema()));
static GAUGE_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| Arc::new(build_gauge_schema()));
static HISTOGRAM_SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(build_histogram_schema()));
static EXP_HISTOGRAM_SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(build_exp_histogram_schema()));
static SUMMARY_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| Arc::new(build_summary_schema()));
static SPAN_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| Arc::new(build_span_schema()));
static SPAN_EVENT_SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(build_span_event_schema()));
static SPAN_LINK_SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(build_span_link_schema()));
static LOG_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| Arc::new(build_log_schema()));
static EXEMPLAR_SCHEMA: LazyLock<Arc<Schema>> =
    LazyLock::new(|| Arc::new(build_exemplar_schema()));

// ---------------------------------------------------------------------------
// Public schema accessors
// ---------------------------------------------------------------------------

pub fn counter_schema() -> Schema {
    build_counter_schema()
}

pub fn counter_schema_ref() -> &'static Arc<Schema> {
    &COUNTER_SCHEMA
}

pub fn gauge_schema() -> Schema {
    build_gauge_schema()
}

pub fn gauge_schema_ref() -> &'static Arc<Schema> {
    &GAUGE_SCHEMA
}

pub fn histogram_schema() -> Schema {
    build_histogram_schema()
}

pub fn histogram_schema_ref() -> &'static Arc<Schema> {
    &HISTOGRAM_SCHEMA
}

pub fn exp_histogram_schema() -> Schema {
    build_exp_histogram_schema()
}

pub fn exp_histogram_schema_ref() -> &'static Arc<Schema> {
    &EXP_HISTOGRAM_SCHEMA
}

pub fn summary_schema() -> Schema {
    build_summary_schema()
}

pub fn summary_schema_ref() -> &'static Arc<Schema> {
    &SUMMARY_SCHEMA
}

pub fn span_schema() -> Schema {
    build_span_schema()
}

pub fn span_schema_ref() -> &'static Arc<Schema> {
    &SPAN_SCHEMA
}

pub fn span_event_schema() -> Schema {
    build_span_event_schema()
}

pub fn span_event_schema_ref() -> &'static Arc<Schema> {
    &SPAN_EVENT_SCHEMA
}

pub fn span_link_schema() -> Schema {
    build_span_link_schema()
}

pub fn span_link_schema_ref() -> &'static Arc<Schema> {
    &SPAN_LINK_SCHEMA
}

pub fn log_schema() -> Schema {
    build_log_schema()
}

pub fn log_schema_ref() -> &'static Arc<Schema> {
    &LOG_SCHEMA
}

pub fn exemplar_schema() -> Schema {
    build_exemplar_schema()
}

pub fn exemplar_schema_ref() -> &'static Arc<Schema> {
    &EXEMPLAR_SCHEMA
}

// ---------------------------------------------------------------------------
// Metric processor configuration helpers
// ---------------------------------------------------------------------------

/// Returns the ordered list of Utf8 column names used as group key in metric
/// processors (CounterAggregator, GaugeAggregator, HistogramAggregator).
///
/// The order matches the column order in the metric table schemas.
pub fn metric_processor_key_columns() -> Vec<String> {
    METRIC_KEY_COLUMN_NAMES
        .iter()
        .map(|s| s.to_string())
        .collect()
}

const METRIC_KEY_COLUMN_NAMES: &[&str] = &[
    "metric_name",
    "description",
    "unit",
    "metadata",
    "attributes",
    "resource_attributes",
    "resource_schema_url",
    "scope_name",
    "scope_version",
    "scope_attributes",
    "scope_schema_url",
];

// ---------------------------------------------------------------------------
// Common field groups (shared across signal types)
// ---------------------------------------------------------------------------

/// Utf8 key columns for metric tables — group key in processors.
fn metric_key_fields() -> Vec<Field> {
    vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, false),
        Field::new("metadata", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("resource_schema_url", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
        Field::new("scope_attributes", DataType::Utf8, false),
        Field::new("scope_schema_url", DataType::Utf8, false),
    ]
}

/// Non-Utf8 passthrough columns for counter/histogram/exp-histogram tables —
/// preserved through processor aggregation unchanged (first-row-wins per group).
///
/// Includes `aggregation_temporality` and `is_monotonic` which are semantically
/// meaningful for Sum, Histogram, and ExponentialHistogram metric types.
fn counter_passthrough_tail_fields() -> Vec<Field> {
    vec![
        Field::new("aggregation_temporality", DataType::Int32, false),
        Field::new("is_monotonic", DataType::Boolean, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new(
            "resource_dropped_attributes_count",
            DataType::UInt32,
            false,
        ),
        Field::new("scope_dropped_attributes_count", DataType::UInt32, false),
    ]
}

/// Non-Utf8 passthrough columns for gauge tables.
///
/// Gauges have no `aggregation_temporality` or `is_monotonic` per the OTLP spec.
fn gauge_passthrough_tail_fields() -> Vec<Field> {
    vec![
        Field::new("flags", DataType::UInt32, false),
        Field::new(
            "resource_dropped_attributes_count",
            DataType::UInt32,
            false,
        ),
        Field::new("scope_dropped_attributes_count", DataType::UInt32, false),
    ]
}

fn ts_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )
}

/// Two nullable columns for OTLP NumberDataPoint value (oneof as_int / as_double).
fn number_value_fields() -> Vec<Field> {
    vec![
        Field::new("value_int", DataType::Int64, true),
        Field::new("value_double", DataType::Float64, true),
    ]
}

// ---------------------------------------------------------------------------
// Schema builders
// ---------------------------------------------------------------------------

fn build_counter_schema() -> Schema {
    let mut fields = metric_key_fields();
    fields.push(ts_field("timestamp"));
    fields.push(ts_field("start_time"));
    fields.extend(number_value_fields());
    fields.extend(counter_passthrough_tail_fields());
    Schema::new(fields)
}

fn build_gauge_schema() -> Schema {
    let mut fields = metric_key_fields();
    fields.push(ts_field("timestamp"));
    fields.push(ts_field("start_time"));
    fields.extend(number_value_fields());
    fields.extend(gauge_passthrough_tail_fields());
    Schema::new(fields)
}

fn build_histogram_schema() -> Schema {
    let mut fields = metric_key_fields();
    fields.push(ts_field("timestamp"));
    fields.push(ts_field("start_time"));
    fields.push(Field::new(
        "boundaries",
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        false,
    ));
    fields.push(Field::new(
        "bucket_counts",
        DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
        false,
    ));
    // sum, min, max are optional per the OTLP spec (may not be collected).
    fields.push(Field::new("sum", DataType::Float64, true));
    fields.push(Field::new("count", DataType::UInt64, false));
    fields.push(Field::new("min", DataType::Float64, true));
    fields.push(Field::new("max", DataType::Float64, true));
    fields.extend(counter_passthrough_tail_fields());
    Schema::new(fields)
}

fn build_exp_histogram_schema() -> Schema {
    let mut fields = metric_key_fields();
    fields.push(ts_field("timestamp"));
    fields.push(ts_field("start_time"));
    fields.push(Field::new("scale", DataType::Int32, false));
    fields.push(Field::new("zero_count", DataType::UInt64, false));
    fields.push(Field::new("zero_threshold", DataType::Float64, false));
    fields.push(Field::new("positive_offset", DataType::Int32, false));
    fields.push(Field::new(
        "positive_bucket_counts",
        DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
        false,
    ));
    fields.push(Field::new("negative_offset", DataType::Int32, false));
    fields.push(Field::new(
        "negative_bucket_counts",
        DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
        false,
    ));
    // sum, min, max are optional per the OTLP spec (may not be collected).
    fields.push(Field::new("sum", DataType::Float64, true));
    fields.push(Field::new("count", DataType::UInt64, false));
    fields.push(Field::new("min", DataType::Float64, true));
    fields.push(Field::new("max", DataType::Float64, true));
    fields.extend(counter_passthrough_tail_fields());
    Schema::new(fields)
}

fn build_summary_schema() -> Schema {
    // Summary is append-only (no processor), so column order doesn't need to
    // be processor-compatible. Uses natural grouping: identity, resource, scope,
    // timestamps, data, flags.
    Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, false),
        Field::new("metadata", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new(
            "resource_dropped_attributes_count",
            DataType::UInt32,
            false,
        ),
        Field::new("resource_schema_url", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
        Field::new("scope_attributes", DataType::Utf8, false),
        Field::new("scope_dropped_attributes_count", DataType::UInt32, false),
        Field::new("scope_schema_url", DataType::Utf8, false),
        ts_field("timestamp"),
        ts_field("start_time"),
        Field::new("count", DataType::UInt64, false),
        Field::new("sum", DataType::Float64, false),
        Field::new("quantile_values", DataType::Utf8, false),
        Field::new("flags", DataType::UInt32, false),
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
        ts_field("start_time"),
        ts_field("end_time"),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("status_code", DataType::Int32, false),
        Field::new("status_message", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new(
            "resource_dropped_attributes_count",
            DataType::UInt32,
            false,
        ),
        Field::new("resource_schema_url", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
        Field::new("scope_attributes", DataType::Utf8, false),
        Field::new("scope_dropped_attributes_count", DataType::UInt32, false),
        Field::new("scope_schema_url", DataType::Utf8, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("dropped_attributes_count", DataType::UInt32, false),
        Field::new("dropped_events_count", DataType::UInt32, false),
        Field::new("dropped_links_count", DataType::UInt32, false),
    ])
}

fn build_span_event_schema() -> Schema {
    Schema::new(vec![
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        ts_field("timestamp"),
        Field::new("name", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("dropped_attributes_count", DataType::UInt32, false),
    ])
}

fn build_span_link_schema() -> Schema {
    Schema::new(vec![
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        Field::new("linked_trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("linked_span_id", DataType::FixedSizeBinary(8), false),
        Field::new("trace_state", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("dropped_attributes_count", DataType::UInt32, false),
    ])
}

fn build_log_schema() -> Schema {
    Schema::new(vec![
        ts_field("timestamp"),
        ts_field("observed_timestamp"),
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        Field::new("severity_number", DataType::Int32, false),
        Field::new("severity_text", DataType::Utf8, false),
        // body is JSON-encoded; body_type preserves the original AnyValue variant.
        Field::new("body", DataType::Utf8, false),
        Field::new("body_type", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new(
            "resource_dropped_attributes_count",
            DataType::UInt32,
            false,
        ),
        Field::new("resource_schema_url", DataType::Utf8, false),
        Field::new("scope_name", DataType::Utf8, false),
        Field::new("scope_version", DataType::Utf8, false),
        Field::new("scope_attributes", DataType::Utf8, false),
        Field::new("scope_dropped_attributes_count", DataType::UInt32, false),
        Field::new("scope_schema_url", DataType::Utf8, false),
        Field::new("flags", DataType::UInt32, false),
        Field::new("dropped_attributes_count", DataType::UInt32, false),
        Field::new("event_name", DataType::Utf8, false),
    ])
}

fn build_exemplar_schema() -> Schema {
    Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_table", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        ts_field("timestamp"),
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        // Two nullable columns to preserve int vs double fidelity.
        Field::new("value_int", DataType::Int64, true),
        Field::new("value_double", DataType::Float64, true),
    ])
}

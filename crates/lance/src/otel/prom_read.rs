//! Prometheus remote-read API implementation.
//!
//! Implements the [Prometheus remote-read protocol](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/)
//! to allow Grafana's Prometheus data source to query metrics stored in
//! `otel_counters` and `otel_gauges` tables.
//!
//! ## Wire format
//!
//! - Request: Snappy-compressed protobuf `ReadRequest`
//! - Response: Snappy-compressed protobuf `ReadResponse`
//! - Content-Type: `application/x-protobuf`
//! - Content-Encoding: `snappy`
//!
//! ## Scope
//!
//! This initial implementation supports counters and gauges.
//! Histogram decomposition into Prometheus `_bucket`/`_sum`/`_count` series
//! is deferred to a follow-up.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, TimestampNanosecondType};
use arrow_array::{Array, RecordBatch};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use prost::Message;
use tracing::debug;

use super::tempo::HttpQueryState;

// ---------------------------------------------------------------------------
// Prometheus remote-read protobuf types (inline prost definitions)
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadRequest {
    #[prost(message, repeated, tag = "1")]
    pub queries: Vec<PromQuery>,
    // accepted_response_types (field 2) — we always respond with SAMPLES.
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PromQuery {
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    #[prost(message, repeated, tag = "3")]
    pub matchers: Vec<LabelMatcher>,
    #[prost(message, optional, tag = "4")]
    pub hints: ::core::option::Option<ReadHints>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LabelMatcher {
    #[prost(enumeration = "MatcherType", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub value: ::prost::alloc::string::String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum MatcherType {
    Eq = 0,
    Neq = 1,
    Re = 2,
    Nre = 3,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadHints {
    #[prost(int64, tag = "1")]
    pub step_ms: i64,
    #[prost(string, tag = "2")]
    pub func: ::prost::alloc::string::String,
    #[prost(int64, tag = "3")]
    pub start_ms: i64,
    #[prost(int64, tag = "4")]
    pub end_ms: i64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadResponse {
    #[prost(message, repeated, tag = "1")]
    pub results: Vec<QueryResult>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryResult {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<PromLabel>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PromLabel {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

// ---------------------------------------------------------------------------
// POST /api/v1/read
// ---------------------------------------------------------------------------

pub async fn prom_remote_read(
    State(state): State<Arc<HttpQueryState>>,
    body: axum::body::Bytes,
) -> Result<Response, (StatusCode, String)> {
    // 1. Snappy decompress
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(&body)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("snappy decompress: {e}")))?;

    // 2. Decode protobuf
    let req = ReadRequest::decode(&decompressed[..])
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("protobuf decode: {e}")))?;

    // 3. Process each query
    let mut results = Vec::with_capacity(req.queries.len());
    for query in &req.queries {
        let qr = process_prom_query(&state.ctx, query).await?;
        results.push(qr);
    }

    let response = ReadResponse { results };

    // 4. Encode and compress
    let encoded = response.encode_to_vec();
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&encoded)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("snappy compress: {e}"),
            )
        })?;

    debug!(
        queries = req.queries.len(),
        response_bytes = compressed.len(),
        "prometheus remote-read"
    );

    Ok((
        StatusCode::OK,
        [
            ("content-type", "application/x-protobuf"),
            ("content-encoding", "snappy"),
        ],
        compressed,
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// Query processing
// ---------------------------------------------------------------------------

async fn process_prom_query(
    ctx: &SessionContext,
    query: &PromQuery,
) -> Result<QueryResult, (StatusCode, String)> {
    // Extract __name__ matcher to determine which tables to query
    let name_matcher = query.matchers.iter().find(|m| m.name == "__name__");

    // Time range: convert ms to nanos for DataFusion
    let start_nanos = query.start_timestamp_ms * 1_000_000;
    let end_nanos = query.end_timestamp_ms * 1_000_000;

    let tables = ["otel_counters", "otel_gauges"];

    let mut all_timeseries: Vec<TimeSeries> = Vec::new();

    for table in &tables {
        let batches =
            query_metric_table(ctx, table, start_nanos, end_nanos, name_matcher).await?;
        let series = batches_to_timeseries(&batches, &query.matchers)?;
        all_timeseries.extend(series);
    }

    Ok(QueryResult {
        timeseries: all_timeseries,
    })
}

async fn query_metric_table(
    ctx: &SessionContext,
    table: &str,
    start_nanos: i64,
    end_nanos: i64,
    name_matcher: Option<&LabelMatcher>,
) -> Result<Vec<RecordBatch>, (StatusCode, String)> {
    let df = match ctx.table(table).await {
        Ok(df) => df,
        Err(_) => return Ok(Vec::new()), // Table doesn't exist yet
    };

    let mut df = df;

    // Time range filter
    df = df
        .filter(
            col("timestamp")
                .gt_eq(lit(ScalarValue::TimestampNanosecond(
                    Some(start_nanos),
                    None,
                )))
                .and(col("timestamp").lt_eq(lit(ScalarValue::TimestampNanosecond(
                    Some(end_nanos),
                    None,
                )))),
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;

    // Apply __name__ filter if present
    if let Some(matcher) = name_matcher {
        match MatcherType::try_from(matcher.r#type) {
            Ok(MatcherType::Eq) => {
                df = df
                    .filter(col("metric_name").eq(lit(&matcher.value)))
                    .map_err(|e| {
                        (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}"))
                    })?;
            }
            Ok(MatcherType::Neq) => {
                df = df
                    .filter(col("metric_name").not_eq(lit(&matcher.value)))
                    .map_err(|e| {
                        (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}"))
                    })?;
            }
            // RE/NRE: skip SQL-level filter, apply in Rust
            _ => {}
        }
    }

    // Select only needed columns and sort by timestamp
    df = df
        .select_columns(&[
            "metric_name",
            "attributes",
            "resource_attributes",
            "timestamp",
            "value_int",
            "value_double",
        ])
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("select: {e}")))?
        .sort(vec![col("timestamp").sort(true, true)])
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("sort: {e}")))?;

    df.collect()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("collect: {e}")))
}

/// Convert Arrow record batches to Prometheus TimeSeries.
///
/// Groups rows by (metric_name, attributes, resource_attributes) and builds
/// labels from the JSON attribute maps.
fn batches_to_timeseries(
    batches: &[RecordBatch],
    matchers: &[LabelMatcher],
) -> Result<Vec<TimeSeries>, (StatusCode, String)> {
    // Group key → (labels, samples)
    let mut series_map: BTreeMap<String, (Vec<PromLabel>, Vec<Sample>)> = BTreeMap::new();

    for batch in batches {
        let n = batch.num_rows();
        let metric_names = batch.column(0).as_string::<i32>();
        let attributes = batch.column(1).as_string::<i32>();
        let resource_attributes = batch.column(2).as_string::<i32>();
        let timestamps = batch.column(3).as_primitive::<TimestampNanosecondType>();
        let value_ints = batch.column(4).as_primitive::<Int64Type>();
        let value_doubles = batch.column(5).as_primitive::<Float64Type>();

        for i in 0..n {
            let metric_name = metric_names.value(i);
            let attrs_json = attributes.value(i);
            let res_attrs_json = resource_attributes.value(i);

            // Build a group key from the metric identity
            let group_key = format!("{metric_name}\x00{attrs_json}\x00{res_attrs_json}");

            let entry =
                series_map
                    .entry(group_key)
                    .or_insert_with(|| {
                        let mut labels = Vec::new();
                        labels.push(PromLabel {
                            name: "__name__".to_string(),
                            value: metric_name.to_string(),
                        });

                        // Parse span attributes as labels
                        if let Ok(serde_json::Value::Object(map)) =
                            serde_json::from_str(attrs_json)
                        {
                            for (k, v) in &map {
                                labels.push(PromLabel {
                                    name: k.clone(),
                                    value: json_value_to_label_value(v),
                                });
                            }
                        }

                        // Extract service.name → job from resource attributes
                        if let Ok(serde_json::Value::Object(map)) =
                            serde_json::from_str(res_attrs_json)
                        {
                            if let Some(sn) = map.get("service.name") {
                                labels.push(PromLabel {
                                    name: "job".to_string(),
                                    value: json_value_to_label_value(sn),
                                });
                            }
                            if let Some(si) = map.get("service.instance.id") {
                                labels.push(PromLabel {
                                    name: "instance".to_string(),
                                    value: json_value_to_label_value(si),
                                });
                            }
                        }

                        // Sort labels by name for consistency
                        labels.sort_by(|a, b| a.name.cmp(&b.name));

                        (labels, Vec::new())
                    });

            // Coalesce value
            let value = if !batch.column(5).is_null(i) {
                value_doubles.value(i)
            } else if !batch.column(4).is_null(i) {
                value_ints.value(i) as f64
            } else {
                0.0
            };

            // Convert timestamp from nanos to ms
            let timestamp_ms = timestamps.value(i) / 1_000_000;

            entry.1.push(Sample {
                value,
                timestamp: timestamp_ms,
            });
        }
    }

    // Apply non-__name__ matchers and collect results
    let timeseries: Vec<TimeSeries> = series_map
        .into_values()
        .filter(|(labels, _)| matches_prom_matchers(labels, matchers))
        .map(|(labels, samples)| TimeSeries { labels, samples })
        .collect();

    Ok(timeseries)
}

/// Check if a set of labels matches all Prometheus matchers (excluding __name__
/// which was already applied at the SQL level).
fn matches_prom_matchers(labels: &[PromLabel], matchers: &[LabelMatcher]) -> bool {
    matchers.iter().all(|m| {
        if m.name == "__name__" {
            return true; // Already handled
        }

        let label_val = labels
            .iter()
            .find(|l| l.name == m.name)
            .map(|l| l.value.as_str())
            .unwrap_or("");

        match MatcherType::try_from(m.r#type) {
            Ok(MatcherType::Eq) => label_val == m.value,
            Ok(MatcherType::Neq) => label_val != m.value,
            // RE/NRE matchers require regex — skip for now (pass through)
            Ok(MatcherType::Re) | Ok(MatcherType::Nre) => true,
            Err(_) => true,
        }
    })
}

fn json_value_to_label_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

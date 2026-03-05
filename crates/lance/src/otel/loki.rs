//! Loki HTTP API implementation.
//!
//! Implements the Grafana Loki HTTP API endpoints:
//!
//! - `GET /loki/api/v1/query` — Instant log/metric query
//! - `GET /loki/api/v1/query_range` — Range log/metric query
//! - `GET /loki/api/v1/labels` — Label name discovery
//! - `GET /loki/api/v1/label/{name}/values` — Label value enumeration
//! - `GET /loki/api/v1/series` — Series matching
//! - `POST /loki/api/v1/push` — Log push ingest

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::builder::*;
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::{Array, ArrayRef, RecordBatch};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use prost::Message;
use serde::Deserialize;
use serde_json::json;
use tracing::debug;

use super::logql::{self, LogQLExpr, MetricFunc, PipelineStage, StreamSelector};
use super::schema;
use super::tempo::HttpQueryState;

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
pub struct LokiQueryParams {
    pub query: Option<String>,
    pub time: Option<String>,
    pub limit: Option<usize>,
    pub direction: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub step: Option<String>,
}

#[derive(Deserialize, Default)]
pub struct LokiLabelParams {
    pub start: Option<String>,
    pub end: Option<String>,
}

#[derive(Deserialize, Default)]
pub struct LokiSeriesParams {
    #[serde(rename = "match[]")]
    pub matchers: Option<Vec<String>>,
    pub start: Option<String>,
    pub end: Option<String>,
}

// ---------------------------------------------------------------------------
// GET /loki/api/v1/query — Instant query
// ---------------------------------------------------------------------------

pub async fn loki_query(
    State(state): State<Arc<HttpQueryState>>,
    Query(params): Query<LokiQueryParams>,
) -> Result<Response, (StatusCode, String)> {
    let query_str = params
        .query
        .as_deref()
        .ok_or((StatusCode::BAD_REQUEST, "missing 'query' parameter".into()))?;

    let limit = params.limit.unwrap_or(100);
    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let time_ns = parse_loki_timestamp(params.time.as_deref()).unwrap_or(now_ns);

    let expr = logql::parse(query_str)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {e}")))?;

    match &expr {
        LogQLExpr::LogQuery { selector, pipeline } => {
            let start_ns = time_ns - 3_600_000_000_000i64; // 1h lookback
            let streams = query_log_streams(
                &state.ctx,
                selector,
                pipeline,
                start_ns,
                time_ns,
                limit,
                params.direction.as_deref().unwrap_or("backward"),
            )
            .await?;

            Ok(loki_success_response(json!({
                "resultType": "streams",
                "result": streams
            })))
        }
        LogQLExpr::MetricQuery { func, inner, range } => {
            let (selector, pipeline) = extract_log_query(inner)?;
            let range_ns = range.as_nanos() as i64;
            let start_ns = time_ns - range_ns;

            let samples = eval_metric_query(
                &state.ctx, *func, &selector, &pipeline, start_ns, time_ns, None,
            )
            .await?;

            Ok(loki_success_response(json!({
                "resultType": "vector",
                "result": samples
            })))
        }
        LogQLExpr::Aggregation {
            op,
            by_labels,
            inner,
        } => {
            let (func, selector, pipeline, range) = extract_metric_query(inner)?;
            let range_ns = range.as_nanos() as i64;
            let start_ns = time_ns - range_ns;

            let samples = eval_metric_query(
                &state.ctx, func, &selector, &pipeline, start_ns, time_ns, None,
            )
            .await?;

            // Apply aggregation
            let aggregated = aggregate_loki_samples(samples, *op, by_labels);
            Ok(loki_success_response(json!({
                "resultType": "vector",
                "result": aggregated
            })))
        }
    }
}

// ---------------------------------------------------------------------------
// GET /loki/api/v1/query_range — Range query
// ---------------------------------------------------------------------------

pub async fn loki_query_range(
    State(state): State<Arc<HttpQueryState>>,
    Query(params): Query<LokiQueryParams>,
) -> Result<Response, (StatusCode, String)> {
    let query_str = params
        .query
        .as_deref()
        .ok_or((StatusCode::BAD_REQUEST, "missing 'query' parameter".into()))?;

    let limit = params.limit.unwrap_or(1000);
    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let start_ns =
        parse_loki_timestamp(params.start.as_deref()).unwrap_or(now_ns - 3_600_000_000_000);
    let end_ns = parse_loki_timestamp(params.end.as_deref()).unwrap_or(now_ns);
    let step_ms = parse_loki_duration_ms(params.step.as_deref()).unwrap_or(15_000);

    let expr = logql::parse(query_str)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {e}")))?;

    match &expr {
        LogQLExpr::LogQuery { selector, pipeline } => {
            let streams = query_log_streams(
                &state.ctx,
                selector,
                pipeline,
                start_ns,
                end_ns,
                limit,
                params.direction.as_deref().unwrap_or("backward"),
            )
            .await?;

            Ok(loki_success_response(json!({
                "resultType": "streams",
                "result": streams
            })))
        }
        LogQLExpr::MetricQuery { func, inner, range } => {
            let (selector, pipeline) = extract_log_query(inner)?;
            let _range_ns = range.as_nanos() as i64;

            let samples = eval_metric_query(
                &state.ctx,
                *func,
                &selector,
                &pipeline,
                start_ns,
                end_ns,
                Some(step_ms),
            )
            .await?;

            Ok(loki_success_response(json!({
                "resultType": "matrix",
                "result": samples
            })))
        }
        LogQLExpr::Aggregation {
            op,
            by_labels,
            inner,
        } => {
            let (func, selector, pipeline, _range) = extract_metric_query(inner)?;

            let samples = eval_metric_query(
                &state.ctx,
                func,
                &selector,
                &pipeline,
                start_ns,
                end_ns,
                Some(step_ms),
            )
            .await?;

            let aggregated = aggregate_loki_samples(samples, *op, by_labels);
            Ok(loki_success_response(json!({
                "resultType": "matrix",
                "result": aggregated
            })))
        }
    }
}

// ---------------------------------------------------------------------------
// GET /loki/api/v1/labels
// ---------------------------------------------------------------------------

pub async fn loki_labels(
    State(state): State<Arc<HttpQueryState>>,
    Query(_params): Query<LokiLabelParams>,
) -> Result<Response, (StatusCode, String)> {
    let mut labels = BTreeSet::new();
    labels.insert("job".to_string());
    labels.insert("level".to_string());

    let df = match state.ctx.table("otel_logs").await {
        Ok(df) => df,
        Err(_) => return Ok(loki_success_response(json!(Vec::<String>::new()))),
    };

    let batches = df
        .select_columns(&["resource_attributes", "attributes"])
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("select: {e}")))?
        .limit(0, Some(10_000))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("limit: {e}")))?
        .collect()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("collect: {e}")))?;

    for batch in &batches {
        for col_idx in 0..batch.num_columns() {
            if let Some(arr) = batch
                .column(col_idx)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
            {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        if let Ok(serde_json::Value::Object(map)) =
                            serde_json::from_str(arr.value(i))
                        {
                            for k in map.keys() {
                                labels.insert(k.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    let data: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
    Ok(loki_success_response(json!(data)))
}

// ---------------------------------------------------------------------------
// GET /loki/api/v1/label/{name}/values
// ---------------------------------------------------------------------------

pub async fn loki_label_values(
    State(state): State<Arc<HttpQueryState>>,
    Path(name): Path<String>,
    Query(_params): Query<LokiLabelParams>,
) -> Result<Response, (StatusCode, String)> {
    let mut values = BTreeSet::new();

    let df = match state.ctx.table("otel_logs").await {
        Ok(df) => df,
        Err(_) => return Ok(loki_success_response(json!(Vec::<String>::new()))),
    };

    match name.as_str() {
        "job" => {
            let batches = df
                .select_columns(&["resource_attributes"])
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?
                .limit(0, Some(10_000))
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?
                .collect()
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
            for batch in &batches {
                let col = batch.column(0).as_string::<i32>();
                for i in 0..batch.num_rows() {
                    if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(col.value(i)) {
                        if let Some(sn) = map.get("service.name") {
                            values.insert(json_val_to_str(sn));
                        }
                    }
                }
            }
        }
        "level" => {
            let batches = df
                .select_columns(&["severity_text"])
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?
                .distinct()
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?
                .collect()
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
            for batch in &batches {
                let col = batch.column(0).as_string::<i32>();
                for i in 0..batch.num_rows() {
                    let v = col.value(i);
                    if !v.is_empty() {
                        values.insert(v.to_string());
                    }
                }
            }
        }
        _ => {
            let batches = df
                .select_columns(&["attributes", "resource_attributes"])
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?
                .limit(0, Some(10_000))
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?
                .collect()
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
            for batch in &batches {
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx).as_string::<i32>();
                    for i in 0..batch.num_rows() {
                        if let Ok(serde_json::Value::Object(map)) =
                            serde_json::from_str(col.value(i))
                        {
                            if let Some(v) = map.get(&name) {
                                values.insert(json_val_to_str(v));
                            }
                        }
                    }
                }
            }
        }
    }

    let data: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
    Ok(loki_success_response(json!(data)))
}

// ---------------------------------------------------------------------------
// GET /loki/api/v1/series
// ---------------------------------------------------------------------------

pub async fn loki_series(
    State(state): State<Arc<HttpQueryState>>,
    Query(_params): Query<LokiSeriesParams>,
) -> Result<Response, (StatusCode, String)> {
    let df = match state.ctx.table("otel_logs").await {
        Ok(df) => df,
        Err(_) => {
            return Ok(loki_success_response(
                json!(Vec::<serde_json::Value>::new()),
            ));
        }
    };

    let batches = df
        .select_columns(&["severity_text", "resource_attributes", "attributes"])
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?
        .limit(0, Some(1000))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?
        .collect()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    let mut seen = BTreeSet::new();
    let mut result: Vec<serde_json::Value> = Vec::new();

    for batch in &batches {
        let sev = batch.column(0).as_string::<i32>();
        let res_attrs = batch.column(1).as_string::<i32>();
        for i in 0..batch.num_rows() {
            let mut labels = BTreeMap::new();
            let sev_val = sev.value(i);
            if !sev_val.is_empty() {
                labels.insert("level".to_string(), sev_val.to_string());
            }
            if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(res_attrs.value(i)) {
                if let Some(sn) = map.get("service.name") {
                    labels.insert("job".to_string(), json_val_to_str(sn));
                }
            }

            let key = format!("{:?}", labels);
            if seen.insert(key) {
                result.push(json!(labels));
            }
        }
    }

    Ok(loki_success_response(json!(result)))
}

// ---------------------------------------------------------------------------
// POST /loki/api/v1/push — Log push ingest
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, ::prost::Message)]
struct PushRequest {
    #[prost(message, repeated, tag = "1")]
    streams: Vec<StreamAdapter>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct StreamAdapter {
    #[prost(string, tag = "1")]
    labels: String,
    #[prost(message, repeated, tag = "2")]
    entries: Vec<EntryAdapter>,
    #[prost(uint64, tag = "3")]
    hash: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiTimestamp {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(int32, tag = "2")]
    nanos: i32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct EntryAdapter {
    #[prost(message, optional, tag = "1")]
    timestamp: Option<LokiTimestamp>,
    #[prost(string, tag = "2")]
    line: String,
    #[prost(message, repeated, tag = "3")]
    structured_metadata: Vec<LabelPairAdapter>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LabelPairAdapter {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

pub async fn loki_push(
    State(state): State<Arc<HttpQueryState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, (StatusCode, String)> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/x-protobuf");

    let req: PushRequest = if content_type.contains("json") {
        // JSON push format: {"streams": [{"stream": {...}, "values": [["ts", "line"], ...]}]}
        let json_val: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("JSON decode: {e}")))?;
        parse_loki_json_push(&json_val)?
    } else {
        // Snappy-compressed protobuf
        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(&body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("snappy decompress: {e}")))?;
        PushRequest::decode(&decompressed[..])
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("protobuf decode: {e}")))?
    };

    let mut total_entries = 0usize;
    for stream in &req.streams {
        total_entries += stream.entries.len();
    }

    if total_entries == 0 {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }

    let n = total_entries;
    let mut timestamp_builder = TimestampNanosecondBuilder::with_capacity(n);
    let mut observed_ts_builder = TimestampNanosecondBuilder::with_capacity(n);
    let mut trace_id_builder = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut span_id_builder = FixedSizeBinaryBuilder::with_capacity(n, 8);
    let mut severity_number_builder = Int32Builder::with_capacity(n);
    let mut severity_text_builder = StringBuilder::with_capacity(n, n * 8);
    let mut body_builder = StringBuilder::with_capacity(n, n * 128);
    let mut body_type_builder = StringBuilder::with_capacity(n, n * 8);
    let mut attributes_builder = StringBuilder::with_capacity(n, n * 64);
    let mut resource_attributes_builder = StringBuilder::with_capacity(n, n * 64);
    let mut resource_dropped_attrs_builder = UInt32Builder::with_capacity(n);
    let mut resource_schema_url_builder = StringBuilder::with_capacity(n, 0);
    let mut scope_name_builder = StringBuilder::with_capacity(n, 0);
    let mut scope_version_builder = StringBuilder::with_capacity(n, 0);
    let mut scope_attributes_builder = StringBuilder::with_capacity(n, 0);
    let mut scope_dropped_attrs_builder = UInt32Builder::with_capacity(n);
    let mut scope_schema_url_builder = StringBuilder::with_capacity(n, 0);
    let mut flags_builder = UInt32Builder::with_capacity(n);
    let mut dropped_attrs_builder = UInt32Builder::with_capacity(n);
    let mut event_name_builder = StringBuilder::with_capacity(n, 0);

    let zero_trace = [0u8; 16];
    let zero_span = [0u8; 8];

    for stream in &req.streams {
        // Parse Loki label string: {job="app",env="prod"}
        let (res_attrs, level) = parse_loki_labels(&stream.labels);
        let res_attrs_json = serde_json::to_string(&res_attrs).unwrap_or_else(|_| "{}".to_string());

        let (severity_text, severity_number) = level_to_severity(&level);

        for entry in &stream.entries {
            let ts_ns = entry
                .timestamp
                .as_ref()
                .map(|t| t.seconds * 1_000_000_000 + t.nanos as i64)
                .unwrap_or_else(|| chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));

            timestamp_builder.append_value(ts_ns);
            observed_ts_builder.append_value(ts_ns);
            trace_id_builder
                .append_value(&zero_trace)
                .expect("16 bytes");
            span_id_builder.append_value(&zero_span).expect("8 bytes");
            severity_number_builder.append_value(severity_number);
            severity_text_builder.append_value(severity_text);
            body_builder.append_value(&entry.line);
            body_type_builder.append_value("string");

            // Structured metadata → attributes
            let attrs: BTreeMap<String, String> = entry
                .structured_metadata
                .iter()
                .map(|lp| (lp.name.clone(), lp.value.clone()))
                .collect();
            let attrs_json = serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string());
            attributes_builder.append_value(&attrs_json);

            resource_attributes_builder.append_value(&res_attrs_json);
            resource_dropped_attrs_builder.append_value(0);
            resource_schema_url_builder.append_value("");
            scope_name_builder.append_value("");
            scope_version_builder.append_value("");
            scope_attributes_builder.append_value("{}");
            scope_dropped_attrs_builder.append_value(0);
            scope_schema_url_builder.append_value("");
            flags_builder.append_value(0);
            dropped_attrs_builder.append_value(0);
            event_name_builder.append_value("");
        }
    }

    let log_schema = schema::log_schema_ref().clone();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(timestamp_builder.finish()),
        Arc::new(observed_ts_builder.finish()),
        Arc::new(trace_id_builder.finish()),
        Arc::new(span_id_builder.finish()),
        Arc::new(severity_number_builder.finish()),
        Arc::new(severity_text_builder.finish()),
        Arc::new(body_builder.finish()),
        Arc::new(body_type_builder.finish()),
        Arc::new(attributes_builder.finish()),
        Arc::new(resource_attributes_builder.finish()),
        Arc::new(resource_dropped_attrs_builder.finish()),
        Arc::new(resource_schema_url_builder.finish()),
        Arc::new(scope_name_builder.finish()),
        Arc::new(scope_version_builder.finish()),
        Arc::new(scope_attributes_builder.finish()),
        Arc::new(scope_dropped_attrs_builder.finish()),
        Arc::new(scope_schema_url_builder.finish()),
        Arc::new(flags_builder.finish()),
        Arc::new(dropped_attrs_builder.finish()),
        Arc::new(event_name_builder.finish()),
    ];

    let batch = RecordBatch::try_new(log_schema, columns)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("batch: {e}")))?;

    debug!(entries = total_entries, "loki push");

    state
        .receiver
        .write_to_table(schema::LOGS_TABLE, vec![batch])
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("write: {e}")))?;

    Ok(StatusCode::NO_CONTENT.into_response())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

async fn query_log_streams(
    ctx: &SessionContext,
    selector: &StreamSelector,
    pipeline: &[PipelineStage],
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    direction: &str,
) -> Result<Vec<serde_json::Value>, (StatusCode, String)> {
    let df = match ctx.table("otel_logs").await {
        Ok(df) => df,
        Err(_) => return Ok(Vec::new()),
    };

    // Time range filter
    let df = df
        .filter(
            col("timestamp")
                .gt_eq(lit(ScalarValue::TimestampNanosecond(Some(start_ns), None)))
                .and(
                    col("timestamp")
                        .lt_eq(lit(ScalarValue::TimestampNanosecond(Some(end_ns), None))),
                ),
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;

    // Apply SQL-level filters for direct columns
    let mut df = df;
    for m in &selector.matchers {
        match m.name.as_str() {
            "level" => {
                if matches!(m.op, logql::MatchOp::Eq) {
                    df = df
                        .filter(col("severity_text").eq(lit(&m.value)))
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;
                }
            }
            _ => {} // Apply in Rust post-filter
        }
    }

    // Apply line filter from pipeline at SQL level where possible
    for stage in pipeline {
        if let PipelineStage::LineContains(s) = stage {
            df = df
                .filter(col("body").like(lit(format!("%{s}%"))))
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;
        }
    }

    let sort_asc = direction != "backward";
    let df = df
        .sort(vec![col("timestamp").sort(sort_asc, true)])
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("sort: {e}")))?
        .limit(0, Some(limit))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("limit: {e}")))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("collect: {e}")))?;

    // Group by stream labels
    let mut streams: BTreeMap<String, (BTreeMap<String, String>, Vec<[String; 2]>)> =
        BTreeMap::new();

    for batch in &batches {
        let n = batch.num_rows();
        let timestamps = batch.column(0).as_primitive::<TimestampNanosecondType>();
        let severity_text = batch.column(5).as_string::<i32>();
        let body_col = batch.column(6).as_string::<i32>();
        let res_attrs = batch.column(9).as_string::<i32>();
        for i in 0..n {
            let body = body_col.value(i);

            // Build labels for this row
            let mut labels = BTreeMap::new();
            let sev = severity_text.value(i);
            if !sev.is_empty() {
                labels.insert("level".to_string(), sev.to_string());
            }
            if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(res_attrs.value(i)) {
                if let Some(sn) = map.get("service.name") {
                    labels.insert("job".to_string(), json_val_to_str(sn));
                }
            }

            // Check stream selector match (for matchers not applied at SQL level)
            if !logql::matches_stream_selector(&labels, selector) {
                continue;
            }

            // Check pipeline match
            if !logql::matches_pipeline(body, pipeline) {
                continue;
            }

            let stream_key = format!("{:?}", labels);
            let entry = streams
                .entry(stream_key)
                .or_insert_with(|| (labels, Vec::new()));

            let ts_ns = timestamps.value(i);
            entry.1.push([ts_ns.to_string(), body.to_string()]);
        }
    }

    let result: Vec<serde_json::Value> = streams
        .into_values()
        .map(|(labels, values)| {
            json!({
                "stream": labels,
                "values": values
            })
        })
        .collect();

    Ok(result)
}

async fn eval_metric_query(
    ctx: &SessionContext,
    func: MetricFunc,
    selector: &StreamSelector,
    pipeline: &[PipelineStage],
    start_ns: i64,
    end_ns: i64,
    step_ms: Option<i64>,
) -> Result<Vec<serde_json::Value>, (StatusCode, String)> {
    let df = match ctx.table("otel_logs").await {
        Ok(df) => df,
        Err(_) => return Ok(Vec::new()),
    };

    // Time range filter
    let df = df
        .filter(
            col("timestamp")
                .gt_eq(lit(ScalarValue::TimestampNanosecond(Some(start_ns), None)))
                .and(
                    col("timestamp")
                        .lt_eq(lit(ScalarValue::TimestampNanosecond(Some(end_ns), None))),
                ),
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("collect: {e}")))?;

    // Group by stream labels, count per step
    let step_ns = step_ms.unwrap_or(15_000) * 1_000_000;
    let range_secs = (end_ns - start_ns) as f64 / 1e9;

    let mut stream_counts: BTreeMap<String, (BTreeMap<String, String>, BTreeMap<i64, u64>)> =
        BTreeMap::new();

    for batch in &batches {
        let n = batch.num_rows();
        let timestamps = batch.column(0).as_primitive::<TimestampNanosecondType>();
        let severity_text = batch.column(5).as_string::<i32>();
        let body_col = batch.column(6).as_string::<i32>();
        let res_attrs = batch.column(9).as_string::<i32>();

        for i in 0..n {
            let body = body_col.value(i);
            let mut labels = BTreeMap::new();
            let sev = severity_text.value(i);
            if !sev.is_empty() {
                labels.insert("level".to_string(), sev.to_string());
            }
            if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(res_attrs.value(i)) {
                if let Some(sn) = map.get("service.name") {
                    labels.insert("job".to_string(), json_val_to_str(sn));
                }
            }

            if !logql::matches_stream_selector(&labels, selector) {
                continue;
            }
            if !logql::matches_pipeline(body, pipeline) {
                continue;
            }

            let ts = timestamps.value(i);
            let bucket = (ts / step_ns) * step_ns;

            let key = format!("{:?}", labels);
            let entry = stream_counts
                .entry(key)
                .or_insert_with(|| (labels, BTreeMap::new()));
            *entry.1.entry(bucket).or_insert(0) += 1;
        }
    }

    let result: Vec<serde_json::Value> = stream_counts
        .into_values()
        .map(|(labels, counts)| {
            let values: Vec<serde_json::Value> = counts
                .iter()
                .map(|(&ts_ns, &count)| {
                    let ts_sec = ts_ns as f64 / 1e9;
                    let val = match func {
                        MetricFunc::CountOverTime => count as f64,
                        MetricFunc::Rate => count as f64 / range_secs,
                        MetricFunc::BytesOverTime | MetricFunc::BytesRate => count as f64,
                    };
                    json!([ts_sec, val.to_string()])
                })
                .collect();
            json!({
                "metric": labels,
                "values": values,
                "value": values.last().cloned()
            })
        })
        .collect();

    Ok(result)
}

fn extract_log_query(
    expr: &LogQLExpr,
) -> Result<(StreamSelector, Vec<PipelineStage>), (StatusCode, String)> {
    match expr {
        LogQLExpr::LogQuery { selector, pipeline } => Ok((selector.clone(), pipeline.clone())),
        _ => Err((
            StatusCode::BAD_REQUEST,
            "expected log query inside metric function".into(),
        )),
    }
}

fn extract_metric_query(
    expr: &LogQLExpr,
) -> Result<
    (
        MetricFunc,
        StreamSelector,
        Vec<PipelineStage>,
        std::time::Duration,
    ),
    (StatusCode, String),
> {
    match expr {
        LogQLExpr::MetricQuery { func, inner, range } => {
            let (selector, pipeline) = extract_log_query(inner)?;
            Ok((*func, selector, pipeline, *range))
        }
        _ => Err((
            StatusCode::BAD_REQUEST,
            "expected metric query inside aggregation".into(),
        )),
    }
}

fn aggregate_loki_samples(
    samples: Vec<serde_json::Value>,
    _op: logql::AggFunc,
    _by_labels: &[String],
) -> Vec<serde_json::Value> {
    // Simplified: just pass through for now.
    // Full implementation would group by `by_labels` and apply `op`.
    samples
}

fn loki_success_response(data: serde_json::Value) -> Response {
    let body = json!({
        "status": "success",
        "data": data
    });
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        serde_json::to_string(&body).unwrap_or_default(),
    )
        .into_response()
}

/// Parse Loki label string: `{job="app",env="prod"}`
fn parse_loki_labels(label_str: &str) -> (BTreeMap<String, serde_json::Value>, String) {
    let mut res_attrs = BTreeMap::new();
    let mut level = String::new();

    let trimmed = label_str.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(trimmed);

    for pair in inner.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        if let Some((key, value)) = pair.split_once('=') {
            let key = key.trim();
            let value = value.trim().trim_matches('"');
            match key {
                "job" => {
                    res_attrs.insert(
                        "service.name".to_string(),
                        serde_json::Value::String(value.to_string()),
                    );
                }
                "level" => {
                    level = value.to_string();
                }
                _ => {
                    res_attrs.insert(
                        key.to_string(),
                        serde_json::Value::String(value.to_string()),
                    );
                }
            }
        }
    }

    (res_attrs, level)
}

fn level_to_severity(level: &str) -> (&'static str, i32) {
    match level.to_lowercase().as_str() {
        "trace" => ("TRACE", 1),
        "debug" => ("DEBUG", 5),
        "info" => ("INFO", 9),
        "warn" | "warning" => ("WARN", 13),
        "error" => ("ERROR", 17),
        "fatal" | "critical" => ("FATAL", 21),
        _ => ("", 0),
    }
}

fn parse_loki_json_push(json: &serde_json::Value) -> Result<PushRequest, (StatusCode, String)> {
    let streams_arr = json
        .get("streams")
        .and_then(|v| v.as_array())
        .ok_or((StatusCode::BAD_REQUEST, "missing 'streams' array".into()))?;

    let mut streams = Vec::new();
    for s in streams_arr {
        let labels_obj = s.get("stream").and_then(|v| v.as_object());
        let label_str = if let Some(obj) = labels_obj {
            let pairs: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("{}=\"{}\"", k, v.as_str().unwrap_or("")))
                .collect();
            format!("{{{}}}", pairs.join(","))
        } else {
            "{}".to_string()
        };

        let entries: Vec<EntryAdapter> = s
            .get("values")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|entry| {
                        let parts = entry.as_array()?;
                        let ts_str = parts.first()?.as_str()?;
                        let line = parts.get(1)?.as_str()?.to_string();
                        let ts_ns: i64 = ts_str.parse().ok()?;
                        let secs = ts_ns / 1_000_000_000;
                        let nanos = (ts_ns % 1_000_000_000) as i32;
                        Some(EntryAdapter {
                            timestamp: Some(LokiTimestamp {
                                seconds: secs,
                                nanos,
                            }),
                            line,
                            structured_metadata: Vec::new(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        streams.push(StreamAdapter {
            labels: label_str,
            entries,
            hash: 0,
        });
    }

    Ok(PushRequest { streams })
}

fn parse_loki_timestamp(s: Option<&str>) -> Option<i64> {
    let s = s?;
    // Loki timestamps can be nanoseconds (string) or float seconds
    if let Ok(ns) = s.parse::<i64>() {
        if ns > 1_000_000_000_000_000_000 {
            Some(ns) // already nanoseconds
        } else {
            Some(ns * 1_000_000_000) // seconds → nanoseconds
        }
    } else if let Ok(f) = s.parse::<f64>() {
        Some((f * 1e9) as i64)
    } else {
        None
    }
}

fn parse_loki_duration_ms(s: Option<&str>) -> Option<i64> {
    let s = s?;
    if let Ok(f) = s.parse::<f64>() {
        return Some((f * 1000.0) as i64);
    }
    if let Some(v) = s.strip_suffix('s') {
        return v.parse::<f64>().ok().map(|n| (n * 1000.0) as i64);
    }
    if let Some(v) = s.strip_suffix('m') {
        return v.parse::<f64>().ok().map(|n| (n * 60_000.0) as i64);
    }
    if let Some(v) = s.strip_suffix('h') {
        return v.parse::<f64>().ok().map(|n| (n * 3_600_000.0) as i64);
    }
    None
}

fn json_val_to_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

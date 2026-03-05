//! Prometheus/Mimir HTTP API implementation.
//!
//! Implements the Prometheus HTTP API endpoints used by Grafana's
//! Prometheus data source, including:
//!
//! - `GET/POST /api/v1/query` — Instant query
//! - `GET/POST /api/v1/query_range` — Range query
//! - `GET /api/v1/labels` — Label name discovery
//! - `GET /api/v1/label/{name}/values` — Label value enumeration
//! - `GET /api/v1/series` — Series matching
//! - `GET /api/v1/metadata` — Metric metadata
//! - `POST /api/v1/push` — Prometheus remote-write ingest (Mimir-compatible)

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use datafusion::execution::context::SessionContext;
use prost::Message;
use serde::Deserialize;
use serde_json::json;
use tracing::debug;

use super::prom_read::{PromLabel, Sample};
use super::promql;
use super::schema;
use super::tempo::HttpQueryState;

// ---------------------------------------------------------------------------
// Query parameters
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
pub struct InstantQueryParams {
    pub query: Option<String>,
    pub time: Option<String>,
}

#[derive(Deserialize, Default)]
pub struct RangeQueryParams {
    pub query: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub step: Option<String>,
}

#[derive(Deserialize, Default)]
pub struct LabelParams {
    pub start: Option<String>,
    pub end: Option<String>,
    #[serde(rename = "match[]")]
    pub matchers: Option<Vec<String>>,
}

#[derive(Deserialize, Default)]
pub struct SeriesParams {
    #[serde(rename = "match[]")]
    pub matchers: Option<Vec<String>>,
    pub start: Option<String>,
    pub end: Option<String>,
}

// ---------------------------------------------------------------------------
// GET/POST /api/v1/query — Instant query
// ---------------------------------------------------------------------------

pub async fn prom_instant_query(
    State(state): State<Arc<HttpQueryState>>,
    Query(params): Query<InstantQueryParams>,
) -> Result<Response, (StatusCode, String)> {
    let query = params
        .query
        .as_deref()
        .ok_or((StatusCode::BAD_REQUEST, "missing 'query' parameter".into()))?;

    let time_ms = parse_prom_timestamp(params.time.as_deref())
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

    let series = promql::eval_instant(&state.ctx, query, time_ms)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    // Format as Prometheus instant vector response
    let result: Vec<serde_json::Value> = series
        .iter()
        .filter_map(|s| {
            s.samples.last().map(|&(ts, val)| {
                json!({
                    "metric": s.labels,
                    "value": [ts as f64 / 1000.0, val.to_string()]
                })
            })
        })
        .collect();

    Ok(prom_success_response(json!({
        "resultType": "vector",
        "result": result
    })))
}

// ---------------------------------------------------------------------------
// GET/POST /api/v1/query_range — Range query
// ---------------------------------------------------------------------------

pub async fn prom_range_query(
    State(state): State<Arc<HttpQueryState>>,
    Query(params): Query<RangeQueryParams>,
) -> Result<Response, (StatusCode, String)> {
    let query = params
        .query
        .as_deref()
        .ok_or((StatusCode::BAD_REQUEST, "missing 'query' parameter".into()))?;

    let now_ms = chrono::Utc::now().timestamp_millis();
    let start_ms = parse_prom_timestamp(params.start.as_deref()).unwrap_or(now_ms - 3_600_000);
    let end_ms = parse_prom_timestamp(params.end.as_deref()).unwrap_or(now_ms);
    let step_ms = parse_prom_duration(params.step.as_deref()).unwrap_or(15_000);

    let series = promql::eval_range(&state.ctx, query, start_ms, end_ms, step_ms)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    let result: Vec<serde_json::Value> = series
        .iter()
        .map(|s| {
            let values: Vec<serde_json::Value> = s
                .samples
                .iter()
                .map(|&(ts, val)| json!([ts as f64 / 1000.0, val.to_string()]))
                .collect();
            json!({
                "metric": s.labels,
                "values": values
            })
        })
        .collect();

    Ok(prom_success_response(json!({
        "resultType": "matrix",
        "result": result
    })))
}

// ---------------------------------------------------------------------------
// GET /api/v1/labels — Label name discovery
// ---------------------------------------------------------------------------

pub async fn prom_labels(
    State(state): State<Arc<HttpQueryState>>,
    Query(_params): Query<LabelParams>,
) -> Result<Response, (StatusCode, String)> {
    let mut labels = BTreeSet::new();
    labels.insert("__name__".to_string());
    labels.insert("job".to_string());
    labels.insert("instance".to_string());

    for table in &["otel_counters", "otel_gauges"] {
        let batches = query_attribute_columns(&state.ctx, table, 10_000).await?;
        for batch in &batches {
            extract_json_keys_from_batch(batch, &mut labels);
        }
    }

    let data: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
    Ok(prom_success_response(json!(data)))
}

// ---------------------------------------------------------------------------
// GET /api/v1/label/{name}/values — Label value enumeration
// ---------------------------------------------------------------------------

pub async fn prom_label_values(
    State(state): State<Arc<HttpQueryState>>,
    Path(name): Path<String>,
    Query(_params): Query<LabelParams>,
) -> Result<Response, (StatusCode, String)> {
    let mut values = BTreeSet::new();

    match name.as_str() {
        "__name__" => {
            for table in &["otel_counters", "otel_gauges"] {
                let batches = query_distinct_column(&state.ctx, table, "metric_name").await?;
                for batch in &batches {
                    let col = batch.column(0).as_string::<i32>();
                    for i in 0..batch.num_rows() {
                        values.insert(col.value(i).to_string());
                    }
                }
            }
        }
        "job" => {
            for table in &["otel_counters", "otel_gauges"] {
                let batches =
                    query_distinct_column(&state.ctx, table, "resource_attributes").await?;
                for batch in &batches {
                    let col = batch.column(0).as_string::<i32>();
                    for i in 0..batch.num_rows() {
                        if let Ok(serde_json::Value::Object(map)) =
                            serde_json::from_str(col.value(i))
                        {
                            if let Some(sn) = map.get("service.name") {
                                values.insert(json_val_to_str(sn));
                            }
                        }
                    }
                }
            }
        }
        _ => {
            for table in &["otel_counters", "otel_gauges"] {
                let batches = query_attribute_columns(&state.ctx, table, 10_000).await?;
                for batch in &batches {
                    let attrs_col = batch.column(0).as_string::<i32>();
                    let res_attrs_col = batch.column(1).as_string::<i32>();
                    for i in 0..batch.num_rows() {
                        for col_data in [attrs_col.value(i), res_attrs_col.value(i)] {
                            if let Ok(serde_json::Value::Object(map)) =
                                serde_json::from_str(col_data)
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
    }

    let data: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
    Ok(prom_success_response(json!(data)))
}

// ---------------------------------------------------------------------------
// GET /api/v1/series — Series matching
// ---------------------------------------------------------------------------

pub async fn prom_series(
    State(state): State<Arc<HttpQueryState>>,
    Query(_params): Query<SeriesParams>,
) -> Result<Response, (StatusCode, String)> {
    // Return distinct label sets from metric tables
    let mut result: Vec<serde_json::Value> = Vec::new();

    for table in &["otel_counters", "otel_gauges"] {
        let batches = query_attribute_columns(&state.ctx, table, 1000).await?;
        for batch in &batches {
            let metric_names = batch.column(0).as_string::<i32>();
            let attrs_col = batch.column(1).as_string::<i32>();
            let res_attrs_col = batch.column(2).as_string::<i32>();
            for i in 0..batch.num_rows() {
                let mut labels = BTreeMap::new();
                labels.insert(
                    "__name__".to_string(),
                    serde_json::Value::String(metric_names.value(i).to_string()),
                );
                if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(attrs_col.value(i))
                {
                    for (k, v) in map {
                        labels.insert(k, v);
                    }
                }
                if let Ok(serde_json::Value::Object(map)) =
                    serde_json::from_str(res_attrs_col.value(i))
                {
                    if let Some(sn) = map.get("service.name") {
                        labels.insert("job".to_string(), sn.clone());
                    }
                }
                result.push(json!(labels));
            }
        }
    }

    Ok(prom_success_response(json!(result)))
}

// ---------------------------------------------------------------------------
// GET /api/v1/metadata — Metric metadata
// ---------------------------------------------------------------------------

pub async fn prom_metadata(
    State(state): State<Arc<HttpQueryState>>,
    Query(_params): Query<LabelParams>,
) -> Result<Response, (StatusCode, String)> {
    let mut metadata: BTreeMap<String, Vec<serde_json::Value>> = BTreeMap::new();

    for (table, prom_type) in &[
        ("otel_counters", "counter"),
        ("otel_gauges", "gauge"),
        ("otel_histograms", "histogram"),
    ] {
        let batches = query_metric_metadata(&state.ctx, table).await?;
        for batch in &batches {
            let names = batch.column(0).as_string::<i32>();
            let descriptions = batch.column(1).as_string::<i32>();
            let units = batch.column(2).as_string::<i32>();
            for i in 0..batch.num_rows() {
                let name = names.value(i).to_string();
                let entry = metadata.entry(name).or_default();
                entry.push(json!({
                    "type": prom_type,
                    "help": descriptions.value(i),
                    "unit": units.value(i),
                }));
            }
        }
    }

    Ok(prom_success_response(json!(metadata)))
}

// ---------------------------------------------------------------------------
// POST /api/v1/push — Prometheus remote-write ingest
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, ::prost::Message)]
struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    timeseries: Vec<PromWriteTimeSeries>,
    #[prost(message, repeated, tag = "3")]
    metadata: Vec<MetricMetadata>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct PromWriteTimeSeries {
    #[prost(message, repeated, tag = "1")]
    labels: Vec<PromLabel>,
    #[prost(message, repeated, tag = "2")]
    samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct MetricMetadata {
    #[prost(enumeration = "MetricType", tag = "1")]
    r#type: i32,
    #[prost(string, tag = "2")]
    metric_family_name: String,
    #[prost(string, tag = "3")]
    help: String,
    #[prost(string, tag = "4")]
    unit: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
enum MetricType {
    Unknown = 0,
    Counter = 1,
    Gauge = 2,
    Summary = 3,
    Histogram = 4,
    GaugeHistogram = 5,
    Info = 6,
    Stateset = 7,
}

pub async fn prom_remote_write(
    State(state): State<Arc<HttpQueryState>>,
    body: axum::body::Bytes,
) -> Result<Response, (StatusCode, String)> {
    // Snappy decompress
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(&body)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("snappy decompress: {e}")))?;

    let req = WriteRequest::decode(&decompressed[..])
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("protobuf decode: {e}")))?;

    // Build metadata type map
    let mut type_map: BTreeMap<String, i32> = BTreeMap::new();
    for md in &req.metadata {
        type_map.insert(md.metric_family_name.clone(), md.r#type);
    }

    // Group timeseries by target table
    let mut counter_rows: Vec<MetricRow> = Vec::new();
    let mut gauge_rows: Vec<MetricRow> = Vec::new();

    for ts in &req.timeseries {
        let mut metric_name = String::new();
        let mut attrs = BTreeMap::new();
        let mut res_attrs = BTreeMap::new();

        for label in &ts.labels {
            match label.name.as_str() {
                "__name__" => metric_name = label.value.clone(),
                "job" => {
                    res_attrs.insert(
                        "service.name".to_string(),
                        serde_json::Value::String(label.value.clone()),
                    );
                }
                "instance" => {
                    res_attrs.insert(
                        "service.instance.id".to_string(),
                        serde_json::Value::String(label.value.clone()),
                    );
                }
                _ => {
                    attrs.insert(
                        label.name.clone(),
                        serde_json::Value::String(label.value.clone()),
                    );
                }
            }
        }

        let attrs_json = serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string());
        let res_attrs_json = serde_json::to_string(&res_attrs).unwrap_or_else(|_| "{}".to_string());

        let is_counter = type_map
            .get(&metric_name)
            .map(|t| *t == MetricType::Counter as i32)
            .unwrap_or(false);

        for sample in &ts.samples {
            let row = MetricRow {
                metric_name: metric_name.clone(),
                attrs_json: attrs_json.clone(),
                res_attrs_json: res_attrs_json.clone(),
                timestamp_ns: sample.timestamp * 1_000_000,
                value_double: sample.value,
            };
            if is_counter {
                counter_rows.push(row);
            } else {
                gauge_rows.push(row);
            }
        }
    }

    // Write counter rows
    if !counter_rows.is_empty() {
        let batch = build_metric_batch(&counter_rows, schema::counter_schema_ref(), true);
        state
            .receiver
            .write_to_table(schema::COUNTERS_TABLE, vec![batch])
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("write counters: {e}"),
                )
            })?;
    }

    // Write gauge rows
    if !gauge_rows.is_empty() {
        let batch = build_metric_batch(&gauge_rows, schema::gauge_schema_ref(), false);
        state
            .receiver
            .write_to_table(schema::GAUGES_TABLE, vec![batch])
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("write gauges: {e}"),
                )
            })?;
    }

    debug!(
        timeseries = req.timeseries.len(),
        counters = counter_rows.len(),
        gauges = gauge_rows.len(),
        "prometheus remote-write"
    );

    Ok(StatusCode::NO_CONTENT.into_response())
}

struct MetricRow {
    metric_name: String,
    attrs_json: String,
    res_attrs_json: String,
    timestamp_ns: i64,
    value_double: f64,
}

fn build_metric_batch(
    rows: &[MetricRow],
    schema: &Arc<arrow_schema::Schema>,
    has_temporality: bool,
) -> RecordBatch {
    use arrow_array::ArrayRef;
    use arrow_array::builder::*;

    let n = rows.len();
    let mut metric_name = StringBuilder::with_capacity(n, n * 32);
    let mut description = StringBuilder::with_capacity(n, 0);
    let mut unit = StringBuilder::with_capacity(n, 0);
    let mut metadata_col = StringBuilder::with_capacity(n, 0);
    let mut attributes = StringBuilder::with_capacity(n, n * 64);
    let mut resource_attributes = StringBuilder::with_capacity(n, n * 64);
    let mut resource_schema_url = StringBuilder::with_capacity(n, 0);
    let mut scope_name = StringBuilder::with_capacity(n, 0);
    let mut scope_version = StringBuilder::with_capacity(n, 0);
    let mut scope_attributes = StringBuilder::with_capacity(n, 0);
    let mut scope_schema_url = StringBuilder::with_capacity(n, 0);
    let mut timestamp = TimestampNanosecondBuilder::with_capacity(n);
    let mut start_time = TimestampNanosecondBuilder::with_capacity(n);
    let mut value_int = Int64Builder::with_capacity(n);
    let mut value_double = Float64Builder::with_capacity(n);

    for row in rows {
        metric_name.append_value(&row.metric_name);
        description.append_value("");
        unit.append_value("");
        metadata_col.append_value("{}");
        attributes.append_value(&row.attrs_json);
        resource_attributes.append_value(&row.res_attrs_json);
        resource_schema_url.append_value("");
        scope_name.append_value("");
        scope_version.append_value("");
        scope_attributes.append_value("{}");
        scope_schema_url.append_value("");
        timestamp.append_value(row.timestamp_ns);
        start_time.append_value(0);
        value_int.append_null();
        value_double.append_value(row.value_double);
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(metric_name.finish()),
        Arc::new(description.finish()),
        Arc::new(unit.finish()),
        Arc::new(metadata_col.finish()),
        Arc::new(attributes.finish()),
        Arc::new(resource_attributes.finish()),
        Arc::new(resource_schema_url.finish()),
        Arc::new(scope_name.finish()),
        Arc::new(scope_version.finish()),
        Arc::new(scope_attributes.finish()),
        Arc::new(scope_schema_url.finish()),
        Arc::new(timestamp.finish()),
        Arc::new(start_time.finish()),
        Arc::new(value_int.finish()),
        Arc::new(value_double.finish()),
    ];

    // Tail fields: counter has aggregation_temporality + is_monotonic + flags + resource_dropped + scope_dropped
    // Gauge has flags + resource_dropped + scope_dropped
    if has_temporality {
        let mut agg_temp = Int32Builder::with_capacity(n);
        let mut is_monotonic = BooleanBuilder::with_capacity(n);
        for _ in 0..n {
            agg_temp.append_value(1); // DELTA
            is_monotonic.append_value(true);
        }
        columns.push(Arc::new(agg_temp.finish()));
        columns.push(Arc::new(is_monotonic.finish()));
    }

    let mut flags_builder = UInt32Builder::with_capacity(n);
    let mut res_dropped = UInt32Builder::with_capacity(n);
    let mut scope_dropped = UInt32Builder::with_capacity(n);
    for _ in 0..n {
        flags_builder.append_value(0);
        res_dropped.append_value(0);
        scope_dropped.append_value(0);
    }
    columns.push(Arc::new(flags_builder.finish()));
    columns.push(Arc::new(res_dropped.finish()));
    columns.push(Arc::new(scope_dropped.finish()));

    RecordBatch::try_new(schema.clone(), columns).expect("schema mismatch in remote-write batch")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn prom_success_response(data: serde_json::Value) -> Response {
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

async fn query_attribute_columns(
    ctx: &SessionContext,
    table: &str,
    limit: usize,
) -> Result<Vec<RecordBatch>, (StatusCode, String)> {
    let df = match ctx.table(table).await {
        Ok(df) => df,
        Err(_) => return Ok(Vec::new()),
    };
    // For series endpoint, also select metric_name
    let has_metric_name = df
        .schema()
        .field_with_unqualified_name("metric_name")
        .is_ok();
    let cols = if has_metric_name {
        vec!["metric_name", "attributes", "resource_attributes"]
    } else {
        vec!["attributes", "resource_attributes"]
    };
    df.select_columns(&cols)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("select: {e}")))?
        .limit(0, Some(limit))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("limit: {e}")))?
        .collect()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("collect: {e}")))
}

async fn query_distinct_column(
    ctx: &SessionContext,
    table: &str,
    column: &str,
) -> Result<Vec<RecordBatch>, (StatusCode, String)> {
    let df = match ctx.table(table).await {
        Ok(df) => df,
        Err(_) => return Ok(Vec::new()),
    };
    df.select_columns(&[column])
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("select: {e}")))?
        .distinct()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("distinct: {e}")))?
        .limit(0, Some(10_000))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("limit: {e}")))?
        .collect()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("collect: {e}")))
}

async fn query_metric_metadata(
    ctx: &SessionContext,
    table: &str,
) -> Result<Vec<RecordBatch>, (StatusCode, String)> {
    let df = match ctx.table(table).await {
        Ok(df) => df,
        Err(_) => return Ok(Vec::new()),
    };
    df.select_columns(&["metric_name", "description", "unit"])
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("select: {e}")))?
        .distinct()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("distinct: {e}")))?
        .collect()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("collect: {e}")))
}

fn extract_json_keys_from_batch(batch: &RecordBatch, keys: &mut BTreeSet<String>) {
    for col_idx in 0..batch.num_columns() {
        if let Some(str_arr) = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
        {
            for i in 0..str_arr.len() {
                if !str_arr.is_null(i) {
                    if let Ok(serde_json::Value::Object(map)) =
                        serde_json::from_str(str_arr.value(i))
                    {
                        for k in map.keys() {
                            keys.insert(k.clone());
                        }
                    }
                }
            }
        }
    }
}

fn json_val_to_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn parse_prom_timestamp(s: Option<&str>) -> Option<i64> {
    let s = s?;
    // Prometheus timestamps can be float seconds or RFC3339
    if let Ok(f) = s.parse::<f64>() {
        Some((f * 1000.0) as i64)
    } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        Some(dt.timestamp_millis())
    } else {
        None
    }
}

fn parse_prom_duration(s: Option<&str>) -> Option<i64> {
    let s = s?;
    // Try as float seconds first
    if let Ok(f) = s.parse::<f64>() {
        return Some((f * 1000.0) as i64);
    }
    // Try Go-style duration (15s, 1m, 1h)
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

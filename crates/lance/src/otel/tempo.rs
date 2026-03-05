//! Tempo HTTP API implementation.
//!
//! Implements a subset of the [Grafana Tempo HTTP API](https://grafana.com/docs/tempo/latest/api_docs/)
//! to allow Grafana's Tempo data source (and compatible clients) to query
//! traces stored in the `otel_spans`, `otel_span_events`, and `otel_span_links` tables.
//!
//! ## Supported endpoints
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | GET | `/api/traces/{traceID}` | Fetch a complete trace |
//! | GET | `/api/search` | Search traces by tags / duration / time |
//! | GET | `/api/search/tags` | List available tag names |
//! | GET | `/api/search/tag/{tag}/values` | List values for a tag |
//! | GET | `/api/v2/search/tags` | List tags grouped by scope |
//! | GET | `/api/v2/search/tag/{tag}/values` | List tag values with types |

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, TimestampNanosecondType, UInt32Type};
use arrow_array::RecordBatch;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span, Status as SpanStatus, TracesData,
    span::Event as SpanEvent, span::Link as SpanLink,
};
use prost::Message;
use serde::Deserialize;
use tracing::debug;

use super::convert::json_to_key_values;

/// Shared state for all HTTP query handlers.
pub struct HttpQueryState {
    pub ctx: Arc<SessionContext>,
    pub receiver: Arc<super::OtlpReceiver>,
}

// ---------------------------------------------------------------------------
// GET /api/traces/{traceID}
// ---------------------------------------------------------------------------

pub async fn get_trace(
    State(state): State<Arc<HttpQueryState>>,
    Path(trace_id_hex): Path<String>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let trace_id_bytes = hex::decode(&trace_id_hex)
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid trace ID hex".to_string()))?;
    if trace_id_bytes.len() != 16 {
        return Err((
            StatusCode::BAD_REQUEST,
            "trace ID must be 32 hex chars (16 bytes)".to_string(),
        ));
    }

    let filter = col("trace_id").eq(lit(ScalarValue::FixedSizeBinary(
        16,
        Some(trace_id_bytes.clone()),
    )));

    let (span_batches, event_batches, link_batches) = tokio::try_join!(
        query_table(&state.ctx, "otel_spans", filter.clone()),
        query_table(&state.ctx, "otel_span_events", filter.clone()),
        query_table(&state.ctx, "otel_span_links", filter),
    )
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    let total_spans: usize = span_batches.iter().map(|b| b.num_rows()).sum();
    if total_spans == 0 {
        return Err((StatusCode::NOT_FOUND, "trace not found".to_string()));
    }

    let traces_data = reconstruct_trace(&span_batches, &event_batches, &link_batches);

    debug!(
        trace_id = %trace_id_hex,
        spans = total_spans,
        "tempo get_trace"
    );

    // Check Accept header for protobuf vs JSON
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if accept.contains("application/protobuf") {
        use prost::Message;
        let body = traces_data.encode_to_vec();
        Ok((
            StatusCode::OK,
            [("content-type", "application/protobuf")],
            body,
        )
            .into_response())
    } else {
        // JSON response using serde (opentelemetry-proto has with-serde)
        let body = serde_json::to_vec(&traces_data)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("json encode: {e}")))?;
        Ok((
            StatusCode::OK,
            [("content-type", "application/json")],
            body,
        )
            .into_response())
    }
}

// ---------------------------------------------------------------------------
// GET /api/search
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
pub struct SearchParams {
    /// Logfmt tag filters: `service.name=foo http.method=GET`
    pub tags: Option<String>,
    /// Minimum span duration, e.g. "100ms", "1.5s"
    #[serde(rename = "minDuration")]
    pub min_duration: Option<String>,
    /// Maximum span duration
    #[serde(rename = "maxDuration")]
    pub max_duration: Option<String>,
    /// Max results (default 20)
    pub limit: Option<usize>,
    /// Start of time range (unix epoch seconds)
    pub start: Option<i64>,
    /// End of time range (unix epoch seconds)
    pub end: Option<i64>,
}

pub async fn search_traces(
    State(state): State<Arc<HttpQueryState>>,
    Query(params): Query<SearchParams>,
) -> Result<axum::Json<serde_json::Value>, (StatusCode, String)> {
    let limit = params.limit.unwrap_or(20).min(1000);

    let df = state
        .ctx
        .table("otel_spans")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("table: {e}")))?;

    let mut df = df;

    // Time range filters
    if let Some(start) = params.start {
        let start_nanos = start * 1_000_000_000;
        df = df
            .filter(col("start_time").gt_eq(lit(ScalarValue::TimestampNanosecond(
                Some(start_nanos),
                None,
            ))))
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;
    }
    if let Some(end) = params.end {
        let end_nanos = end * 1_000_000_000;
        df = df
            .filter(col("start_time").lt_eq(lit(ScalarValue::TimestampNanosecond(
                Some(end_nanos),
                None,
            ))))
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;
    }

    // Duration filters
    if let Some(ref min_dur) = params.min_duration {
        let min_nanos = parse_duration_nanos(min_dur)
            .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
        df = df
            .filter(
                (col("end_time") - col("start_time")).gt_eq(lit(ScalarValue::TimestampNanosecond(
                    Some(min_nanos),
                    None,
                ))),
            )
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;
    }
    if let Some(ref max_dur) = params.max_duration {
        let max_nanos = parse_duration_nanos(max_dur)
            .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
        df = df
            .filter(
                (col("end_time") - col("start_time")).lt_eq(lit(ScalarValue::TimestampNanosecond(
                    Some(max_nanos),
                    None,
                ))),
            )
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("filter: {e}")))?;
    }

    // Sort by start_time descending, fetch extra for post-filtering
    let fetch_limit = if params.tags.is_some() {
        limit * 10
    } else {
        limit
    };
    df = df
        .sort(vec![col("start_time").sort(false, true)])
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("sort: {e}")))?
        .limit(0, Some(fetch_limit))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("limit: {e}")))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("query: {e}")))?;

    // Parse tag filters
    let tag_filters = params
        .tags
        .as_deref()
        .map(parse_logfmt_tags)
        .unwrap_or_default();

    // Group spans by trace_id, find root span, apply tag filters
    let mut traces: BTreeMap<String, TraceInfo> = BTreeMap::new();

    for batch in &batches {
        let n = batch.num_rows();
        let trace_ids = batch.column(0).as_fixed_size_binary();
        let parent_span_ids = batch.column(2).as_fixed_size_binary();
        let names = batch.column(4).as_string::<i32>();
        let start_times = batch.column(6).as_primitive::<TimestampNanosecondType>();
        let end_times = batch.column(7).as_primitive::<TimestampNanosecondType>();
        let attributes = batch.column(8).as_string::<i32>();
        let resource_attributes = batch.column(11).as_string::<i32>();

        for i in 0..n {
            let tid_bytes = trace_ids.value(i);
            let tid_hex = hex::encode(tid_bytes);

            // Apply tag filters (check both span attributes and resource attributes)
            if !tag_filters.is_empty() {
                let attrs_json = attributes.value(i);
                let res_attrs_json = resource_attributes.value(i);
                if !matches_tags(&tag_filters, attrs_json, res_attrs_json) {
                    continue;
                }
            }

            let start_ns = start_times.value(i);
            let end_ns = end_times.value(i);
            let is_root = is_zero_span_id(parent_span_ids.value(i));

            let info = traces.entry(tid_hex.clone()).or_insert_with(|| TraceInfo {
                trace_id: tid_hex,
                root_service_name: String::new(),
                root_trace_name: String::new(),
                start_time_unix_nano: start_ns as u64,
                duration_ns: 0,
            });

            // Track earliest start and longest duration
            if (start_ns as u64) < info.start_time_unix_nano {
                info.start_time_unix_nano = start_ns as u64;
            }
            let trace_duration = (end_ns as u64).saturating_sub(info.start_time_unix_nano);
            if trace_duration > info.duration_ns {
                info.duration_ns = trace_duration;
            }

            if is_root {
                info.root_trace_name = names.value(i).to_string();
                // Extract service.name from resource attributes
                if let Ok(parsed) =
                    serde_json::from_str::<serde_json::Value>(resource_attributes.value(i))
                {
                    if let Some(sn) = parsed.get("service.name").and_then(|v| v.as_str()) {
                        info.root_service_name = sn.to_string();
                    }
                }
            }
        }
    }

    // Limit and build response
    let traces_vec: Vec<serde_json::Value> = traces
        .into_values()
        .take(limit)
        .map(|t| {
            serde_json::json!({
                "traceID": t.trace_id,
                "rootServiceName": t.root_service_name,
                "rootTraceName": t.root_trace_name,
                "startTimeUnixNano": t.start_time_unix_nano.to_string(),
                "durationMs": t.duration_ns / 1_000_000,
            })
        })
        .collect();

    debug!(results = traces_vec.len(), "tempo search");

    Ok(axum::Json(serde_json::json!({ "traces": traces_vec })))
}

struct TraceInfo {
    trace_id: String,
    root_service_name: String,
    root_trace_name: String,
    start_time_unix_nano: u64,
    duration_ns: u64,
}

// ---------------------------------------------------------------------------
// GET /api/search/tags
// ---------------------------------------------------------------------------

pub async fn get_tags(
    State(state): State<Arc<HttpQueryState>>,
) -> Result<axum::Json<serde_json::Value>, (StatusCode, String)> {
    let tags = collect_tag_names(&state.ctx).await?;
    Ok(axum::Json(serde_json::json!({ "tagNames": tags })))
}

// ---------------------------------------------------------------------------
// GET /api/v2/search/tags
// ---------------------------------------------------------------------------

pub async fn get_tags_v2(
    State(state): State<Arc<HttpQueryState>>,
) -> Result<axum::Json<serde_json::Value>, (StatusCode, String)> {
    let (span_tags, resource_tags, scope_tags) =
        collect_tag_names_by_scope(&state.ctx).await?;

    let scopes = serde_json::json!([
        { "name": "span", "tags": span_tags.into_iter().collect::<Vec<_>>() },
        { "name": "resource", "tags": resource_tags.into_iter().collect::<Vec<_>>() },
        { "name": "scope", "tags": scope_tags.into_iter().collect::<Vec<_>>() },
    ]);
    Ok(axum::Json(serde_json::json!({ "scopes": scopes })))
}

// ---------------------------------------------------------------------------
// GET /api/search/tag/{tag}/values
// ---------------------------------------------------------------------------

pub async fn get_tag_values(
    State(state): State<Arc<HttpQueryState>>,
    Path(tag): Path<String>,
) -> Result<axum::Json<serde_json::Value>, (StatusCode, String)> {
    let values = collect_tag_values(&state.ctx, &tag).await?;
    Ok(axum::Json(
        serde_json::json!({ "tagValues": values.into_iter().collect::<Vec<_>>() }),
    ))
}

// ---------------------------------------------------------------------------
// GET /api/v2/search/tag/{tag}/values
// ---------------------------------------------------------------------------

pub async fn get_tag_values_v2(
    State(state): State<Arc<HttpQueryState>>,
    Path(tag): Path<String>,
) -> Result<axum::Json<serde_json::Value>, (StatusCode, String)> {
    let values = collect_tag_values(&state.ctx, &tag).await?;
    let typed_values: Vec<serde_json::Value> = values
        .into_iter()
        .map(|v| serde_json::json!({"type": "string", "value": v}))
        .collect();
    Ok(axum::Json(serde_json::json!({ "tagValues": typed_values })))
}

// ---------------------------------------------------------------------------
// Trace reconstruction: Arrow → OTLP protobuf
// ---------------------------------------------------------------------------

/// Reconstruct a `TracesData` from flat Arrow record batches.
///
/// Groups spans by (resource_attributes, resource_schema_url) → ResourceSpans,
/// then by (scope_name, scope_version, scope_attributes, scope_schema_url) → ScopeSpans.
fn reconstruct_trace(
    span_batches: &[RecordBatch],
    event_batches: &[RecordBatch],
    link_batches: &[RecordBatch],
) -> TracesData {
    // Index events by span_id
    let mut events_by_span: HashMap<Vec<u8>, Vec<SpanEvent>> = HashMap::new();
    for batch in event_batches {
        let n = batch.num_rows();
        let span_ids = batch.column(1).as_fixed_size_binary();
        let timestamps = batch.column(2).as_primitive::<TimestampNanosecondType>();
        let names = batch.column(3).as_string::<i32>();
        let attributes = batch.column(4).as_string::<i32>();
        let dropped_attrs = batch.column(5).as_primitive::<UInt32Type>();

        for i in 0..n {
            let sid = span_ids.value(i).to_vec();
            events_by_span.entry(sid).or_default().push(SpanEvent {
                time_unix_nano: timestamps.value(i) as u64,
                name: names.value(i).to_string(),
                attributes: json_to_key_values(attributes.value(i)),
                dropped_attributes_count: dropped_attrs.value(i),
            });
        }
    }

    // Index links by span_id
    let mut links_by_span: HashMap<Vec<u8>, Vec<SpanLink>> = HashMap::new();
    for batch in link_batches {
        let n = batch.num_rows();
        let span_ids = batch.column(1).as_fixed_size_binary();
        let linked_trace_ids = batch.column(2).as_fixed_size_binary();
        let linked_span_ids = batch.column(3).as_fixed_size_binary();
        let trace_states = batch.column(4).as_string::<i32>();
        let attributes = batch.column(5).as_string::<i32>();
        let flags_col = batch.column(6).as_primitive::<UInt32Type>();
        let dropped_attrs = batch.column(7).as_primitive::<UInt32Type>();

        for i in 0..n {
            let sid = span_ids.value(i).to_vec();
            links_by_span.entry(sid).or_default().push(SpanLink {
                trace_id: linked_trace_ids.value(i).to_vec(),
                span_id: linked_span_ids.value(i).to_vec(),
                trace_state: trace_states.value(i).to_string(),
                attributes: json_to_key_values(attributes.value(i)),
                flags: flags_col.value(i),
                dropped_attributes_count: dropped_attrs.value(i),
            });
        }
    }

    // Group key: (resource_attributes_json, resource_schema_url)
    // Sub-key: (scope_name, scope_version, scope_attributes_json, scope_schema_url)
    type ScopeKey = (String, String, String, String);
    type ResourceKey = (String, String);

    let mut resource_map: BTreeMap<ResourceKey, BTreeMap<ScopeKey, Vec<Span>>> = BTreeMap::new();
    // Track resource metadata per resource key
    let mut resource_meta: HashMap<ResourceKey, (u32,)> = HashMap::new();
    // Track scope metadata per scope key
    let mut scope_meta: HashMap<ScopeKey, u32> = HashMap::new();

    for batch in span_batches {
        let n = batch.num_rows();
        let trace_ids = batch.column(0).as_fixed_size_binary();
        let span_ids = batch.column(1).as_fixed_size_binary();
        let parent_span_ids = batch.column(2).as_fixed_size_binary();
        let trace_states = batch.column(3).as_string::<i32>();
        let names = batch.column(4).as_string::<i32>();
        let kinds = batch.column(5).as_primitive::<Int32Type>();
        let start_times = batch.column(6).as_primitive::<TimestampNanosecondType>();
        let end_times = batch.column(7).as_primitive::<TimestampNanosecondType>();
        let attributes = batch.column(8).as_string::<i32>();
        let status_codes = batch.column(9).as_primitive::<Int32Type>();
        let status_messages = batch.column(10).as_string::<i32>();
        let resource_attributes = batch.column(11).as_string::<i32>();
        let resource_dropped = batch.column(12).as_primitive::<UInt32Type>();
        let resource_schema_urls = batch.column(13).as_string::<i32>();
        let scope_names = batch.column(14).as_string::<i32>();
        let scope_versions = batch.column(15).as_string::<i32>();
        let scope_attributes = batch.column(16).as_string::<i32>();
        let scope_dropped = batch.column(17).as_primitive::<UInt32Type>();
        let scope_schema_urls = batch.column(18).as_string::<i32>();
        let flags_col = batch.column(19).as_primitive::<UInt32Type>();
        let dropped_attrs = batch.column(20).as_primitive::<UInt32Type>();
        let dropped_events = batch.column(21).as_primitive::<UInt32Type>();
        let dropped_links = batch.column(22).as_primitive::<UInt32Type>();

        for i in 0..n {
            let sid = span_ids.value(i).to_vec();

            let span = Span {
                trace_id: trace_ids.value(i).to_vec(),
                span_id: sid.clone(),
                parent_span_id: parent_span_ids.value(i).to_vec(),
                trace_state: trace_states.value(i).to_string(),
                name: names.value(i).to_string(),
                kind: kinds.value(i),
                start_time_unix_nano: start_times.value(i) as u64,
                end_time_unix_nano: end_times.value(i) as u64,
                attributes: json_to_key_values(attributes.value(i)),
                status: Some(SpanStatus {
                    code: status_codes.value(i),
                    message: status_messages.value(i).to_string(),
                }),
                events: events_by_span.remove(&sid).unwrap_or_default(),
                links: links_by_span.remove(&sid).unwrap_or_default(),
                flags: flags_col.value(i),
                dropped_attributes_count: dropped_attrs.value(i),
                dropped_events_count: dropped_events.value(i),
                dropped_links_count: dropped_links.value(i),
            };

            let rk: ResourceKey = (
                resource_attributes.value(i).to_string(),
                resource_schema_urls.value(i).to_string(),
            );
            let sk: ScopeKey = (
                scope_names.value(i).to_string(),
                scope_versions.value(i).to_string(),
                scope_attributes.value(i).to_string(),
                scope_schema_urls.value(i).to_string(),
            );

            resource_meta
                .entry(rk.clone())
                .or_insert((resource_dropped.value(i),));
            scope_meta
                .entry(sk.clone())
                .or_insert(scope_dropped.value(i));

            resource_map
                .entry(rk)
                .or_default()
                .entry(sk)
                .or_default()
                .push(span);
        }
    }

    // Build ResourceSpans
    let resource_spans: Vec<ResourceSpans> = resource_map
        .into_iter()
        .map(|(rk, scope_map)| {
            let (res_dropped,) = resource_meta.get(&rk).copied().unwrap_or((0,));
            let resource = Resource {
                attributes: json_to_key_values(&rk.0),
                dropped_attributes_count: res_dropped,
                entity_refs: vec![],
            };

            let scope_spans: Vec<ScopeSpans> = scope_map
                .into_iter()
                .map(|(sk, spans)| {
                    let sc_dropped = scope_meta.get(&sk).copied().unwrap_or(0);
                    let scope = InstrumentationScope {
                        name: sk.0,
                        version: sk.1,
                        attributes: json_to_key_values(&sk.2),
                        dropped_attributes_count: sc_dropped,
                    };
                    ScopeSpans {
                        scope: Some(scope),
                        spans,
                        schema_url: sk.3,
                    }
                })
                .collect();

            ResourceSpans {
                resource: Some(resource),
                scope_spans,
                schema_url: rk.1,
            }
        })
        .collect();

    TracesData { resource_spans }
}

// ---------------------------------------------------------------------------
// Tag collection helpers
// ---------------------------------------------------------------------------

async fn collect_tag_names(
    ctx: &SessionContext,
) -> Result<Vec<String>, (StatusCode, String)> {
    let (span_tags, resource_tags, scope_tags) = collect_tag_names_by_scope(ctx).await?;
    let mut all: BTreeSet<String> = BTreeSet::new();
    all.extend(span_tags);
    for t in resource_tags {
        all.insert(format!("resource.{t}"));
    }
    for t in scope_tags {
        all.insert(format!("scope.{t}"));
    }
    Ok(all.into_iter().collect())
}

async fn collect_tag_names_by_scope(
    ctx: &SessionContext,
) -> Result<(BTreeSet<String>, BTreeSet<String>, BTreeSet<String>), (StatusCode, String)> {
    let batches = ctx
        .sql("SELECT attributes, resource_attributes, scope_attributes FROM otel_spans LIMIT 1000")
        .await
        .and_then(|df| futures::executor::block_on(df.collect()))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("query: {e}")))?;

    let mut span_tags = BTreeSet::new();
    let mut resource_tags = BTreeSet::new();
    let mut scope_tags = BTreeSet::new();

    for batch in &batches {
        let attrs = batch.column(0).as_string::<i32>();
        let res_attrs = batch.column(1).as_string::<i32>();
        let sc_attrs = batch.column(2).as_string::<i32>();

        for i in 0..batch.num_rows() {
            collect_json_keys(attrs.value(i), &mut span_tags);
            collect_json_keys(res_attrs.value(i), &mut resource_tags);
            collect_json_keys(sc_attrs.value(i), &mut scope_tags);
        }
    }

    Ok((span_tags, resource_tags, scope_tags))
}

async fn collect_tag_values(
    ctx: &SessionContext,
    tag: &str,
) -> Result<BTreeSet<String>, (StatusCode, String)> {
    // Determine which column to search: resource.*, scope.*, or span attributes
    let (column, key) = if let Some(stripped) = tag.strip_prefix("resource.") {
        ("resource_attributes", stripped)
    } else if let Some(stripped) = tag.strip_prefix("scope.") {
        ("scope_attributes", stripped)
    } else {
        ("attributes", tag)
    };

    let sql = format!(
        "SELECT {column} FROM otel_spans LIMIT 10000"
    );
    let batches = ctx
        .sql(&sql)
        .await
        .and_then(|df| futures::executor::block_on(df.collect()))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("query: {e}")))?;

    let mut values = BTreeSet::new();
    for batch in &batches {
        let col = batch.column(0).as_string::<i32>();
        for i in 0..batch.num_rows() {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(col.value(i)) {
                if let Some(val) = parsed.get(key) {
                    match val {
                        serde_json::Value::String(s) => {
                            values.insert(s.clone());
                        }
                        other => {
                            values.insert(other.to_string());
                        }
                    }
                }
            }
        }
    }

    Ok(values)
}

fn collect_json_keys(json_str: &str, keys: &mut BTreeSet<String>) {
    if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(json_str) {
        for key in map.keys() {
            keys.insert(key.clone());
        }
    }
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

async fn query_table(
    ctx: &SessionContext,
    table: &str,
    filter: Expr,
) -> Result<Vec<RecordBatch>, String> {
    let df = ctx
        .table(table)
        .await
        .map_err(|e| format!("table {table}: {e}"))?;
    df.filter(filter)
        .map_err(|e| format!("filter {table}: {e}"))?
        .collect()
        .await
        .map_err(|e| format!("collect {table}: {e}"))
}

// ---------------------------------------------------------------------------
// Tag matching
// ---------------------------------------------------------------------------

/// Parse logfmt-style tag string: `key1=val1 key2=val2`
fn parse_logfmt_tags(s: &str) -> Vec<(String, String)> {
    s.split_whitespace()
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect()
}

/// Check if a span's attributes match all tag filters.
fn matches_tags(
    filters: &[(String, String)],
    attrs_json: &str,
    resource_attrs_json: &str,
) -> bool {
    let attrs: serde_json::Value =
        serde_json::from_str(attrs_json).unwrap_or(serde_json::Value::Null);
    let res_attrs: serde_json::Value =
        serde_json::from_str(resource_attrs_json).unwrap_or(serde_json::Value::Null);

    filters.iter().all(|(key, val)| {
        // Check span attributes first, then resource attributes
        let (column_key, actual_key) = if let Some(stripped) = key.strip_prefix("resource.") {
            (&res_attrs, stripped)
        } else {
            (&attrs, key.as_str())
        };

        match column_key.get(actual_key) {
            Some(serde_json::Value::String(s)) => s == val,
            Some(other) => other.to_string().trim_matches('"') == val,
            None => {
                // Also check resource attrs if the key didn't have a prefix
                if !key.starts_with("resource.") {
                    match res_attrs.get(key.as_str()) {
                        Some(serde_json::Value::String(s)) => s == val,
                        Some(other) => other.to_string().trim_matches('"') == val,
                        None => false,
                    }
                } else {
                    false
                }
            }
        }
    })
}

/// Check if a span_id (8 bytes) is all zeros (indicates root span).
fn is_zero_span_id(bytes: &[u8]) -> bool {
    bytes.iter().all(|&b| b == 0)
}

// ---------------------------------------------------------------------------
// Duration parsing
// ---------------------------------------------------------------------------

/// Parse a Tempo-style duration string into nanoseconds.
///
/// Supports: `ns`, `us`/`µs`, `ms`, `s`, `m`, `h`.
fn parse_duration_nanos(s: &str) -> Result<i64, String> {
    if let Some(v) = s.strip_suffix("ns") {
        let n: f64 = v.parse().map_err(|e| format!("invalid duration: {e}"))?;
        Ok(n as i64)
    } else if let Some(v) = s.strip_suffix("us").or_else(|| s.strip_suffix("µs")) {
        let n: f64 = v.parse().map_err(|e| format!("invalid duration: {e}"))?;
        Ok((n * 1_000.0) as i64)
    } else if let Some(v) = s.strip_suffix("ms") {
        let n: f64 = v.parse().map_err(|e| format!("invalid duration: {e}"))?;
        Ok((n * 1_000_000.0) as i64)
    } else if let Some(v) = s.strip_suffix('s') {
        let n: f64 = v.parse().map_err(|e| format!("invalid duration: {e}"))?;
        Ok((n * 1_000_000_000.0) as i64)
    } else if let Some(v) = s.strip_suffix('m') {
        let n: f64 = v.parse().map_err(|e| format!("invalid duration: {e}"))?;
        Ok((n * 60_000_000_000.0) as i64)
    } else if let Some(v) = s.strip_suffix('h') {
        let n: f64 = v.parse().map_err(|e| format!("invalid duration: {e}"))?;
        Ok((n * 3_600_000_000_000.0) as i64)
    } else {
        Err(format!("unknown duration format: {s}"))
    }
}

// ---------------------------------------------------------------------------
// OTLP HTTP Ingest: POST /v1/traces, /v1/metrics, /v1/logs
// ---------------------------------------------------------------------------

/// POST /v1/traces — OTLP HTTP trace ingest (protobuf or JSON).
pub async fn otlp_ingest_traces(
    State(state): State<Arc<HttpQueryState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, (StatusCode, String)> {
    let req: ExportTraceServiceRequest = decode_otlp_body(&headers, &body)?;
    let grpc_req = tonic::Request::new(req);
    TraceService::export(state.receiver.as_ref(), grpc_req)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()))?;
    Ok(StatusCode::OK.into_response())
}

/// POST /v1/metrics — OTLP HTTP metrics ingest (protobuf or JSON).
pub async fn otlp_ingest_metrics(
    State(state): State<Arc<HttpQueryState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, (StatusCode, String)> {
    let req: ExportMetricsServiceRequest = decode_otlp_body(&headers, &body)?;
    let grpc_req = tonic::Request::new(req);
    MetricsService::export(state.receiver.as_ref(), grpc_req)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()))?;
    Ok(StatusCode::OK.into_response())
}

/// POST /v1/logs — OTLP HTTP logs ingest (protobuf or JSON).
pub async fn otlp_ingest_logs(
    State(state): State<Arc<HttpQueryState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, (StatusCode, String)> {
    let req: ExportLogsServiceRequest = decode_otlp_body(&headers, &body)?;
    let grpc_req = tonic::Request::new(req);
    LogsService::export(state.receiver.as_ref(), grpc_req)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.message().to_string()))?;
    Ok(StatusCode::OK.into_response())
}

/// Decode an OTLP HTTP body (protobuf or JSON) based on Content-Type header.
fn decode_otlp_body<T: Message + Default + serde::de::DeserializeOwned>(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<T, (StatusCode, String)> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/x-protobuf");

    if content_type.contains("json") {
        serde_json::from_slice(body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("JSON decode: {e}")))
    } else {
        T::decode(body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("protobuf decode: {e}")))
    }
}

//! OpenTelemetry OTLP gRPC receiver for bisque-lance.
//!
//! Implements the standard OTLP collector services and OTel-Arrow services:
//! - **MetricsService** — decomposes metrics by type into separate tables
//! - **TraceService** — writes spans, events, and links
//! - **LogsService** — writes log records
//! - **ArrowTracesService** / **ArrowLogsService** / **ArrowMetricsService** — OTel-Arrow receivers
//!
//! HTTP query APIs:
//! - **Tempo** — trace query and search endpoints
//! - **Prometheus/Mimir** — PromQL query, metadata, remote-read, and remote-write
//! - **Loki** — LogQL query, metadata, and push endpoints
//! - **OTLP HTTP** — `/v1/traces`, `/v1/metrics`, `/v1/logs` ingest
//!
//! Each service converts data to Arrow RecordBatches and writes
//! through the Raft-replicated [`LanceRaftNode`].

pub mod arrow_proto;
pub mod arrow_receiver;
pub mod convert;
pub mod logql;
pub mod logs;
pub mod loki;
pub mod metrics;
pub mod processors;
pub mod prom_api;
pub mod prom_read;
pub mod promql;
pub mod schema;
pub mod tempo;
pub mod traces;

use std::sync::Arc;

use arrow_array::RecordBatch;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;
use tracing::info;

use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;

use crate::postgres::BisqueLanceCatalogProvider;
use crate::raft::{LanceRaftNode, WriteError};
use crate::write_batcher::WriteBatcherConfig;

use self::arrow_proto::{
    ArrowLogsServiceServer, ArrowMetricsServiceServer, ArrowTracesServiceServer,
};
use self::processors::{
    OtelExpHistogramAggregator, OtelGaugeProcessor, OtelHistogramProcessor, OtelSumAggregator,
};

/// OTLP gRPC receiver backed by a bisque-lance Raft node.
///
/// Implements `MetricsService`, `TraceService`, and `LogsService` from the
/// OTLP collector protocol, plus the OTel-Arrow streaming services.
/// Data is written through Raft consensus via [`LanceRaftNode::write_records`].
/// Pre-built metric handles for a single OTel signal+table combination.
struct OtelTableMetrics {
    received: ::metrics::Counter,
}

pub struct OtlpReceiver {
    raft_node: Arc<LanceRaftNode>,
    // Pre-built per-table received counters (keyed by table name).
    table_metrics: std::collections::HashMap<&'static str, OtelTableMetrics>,
    // Pre-built per-signal latency/error handles.
    m_metrics_latency: ::metrics::Histogram,
    m_traces_latency: ::metrics::Histogram,
    m_logs_latency: ::metrics::Histogram,
    m_unknown_latency: ::metrics::Histogram,
    m_metrics_errors: ::metrics::Counter,
    m_traces_errors: ::metrics::Counter,
    m_logs_errors: ::metrics::Counter,
    m_unknown_errors: ::metrics::Counter,
}

impl OtlpReceiver {
    /// Create a new OTLP receiver wrapping the given Raft node.
    pub fn new(raft_node: Arc<LanceRaftNode>) -> Self {
        let cat = raft_node.catalog_name().to_string();
        let otel_tables: &[(&str, &str)] = &[
            (schema::COUNTERS_TABLE, "metrics"),
            (schema::GAUGES_TABLE, "metrics"),
            (schema::HISTOGRAMS_TABLE, "metrics"),
            (schema::EXP_HISTOGRAMS_TABLE, "metrics"),
            (schema::SUMMARIES_TABLE, "metrics"),
            (schema::EXEMPLARS_TABLE, "metrics"),
            (schema::SPANS_TABLE, "traces"),
            (schema::SPAN_EVENTS_TABLE, "traces"),
            (schema::SPAN_LINKS_TABLE, "traces"),
            (schema::LOGS_TABLE, "logs"),
        ];
        let mut table_metrics = std::collections::HashMap::new();
        for &(table, signal) in otel_tables {
            table_metrics.insert(
                table,
                OtelTableMetrics {
                    received: ::metrics::counter!(
                        "bisque_otel_received_total",
                        "catalog" => cat.clone(),
                        "signal" => signal,
                        "table" => table.to_string()
                    ),
                },
            );
        }

        Self {
            raft_node,
            table_metrics,
            m_metrics_latency: ::metrics::histogram!("bisque_otel_ingest_latency_ms", "catalog" => cat.clone(), "signal" => "metrics"),
            m_traces_latency: ::metrics::histogram!("bisque_otel_ingest_latency_ms", "catalog" => cat.clone(), "signal" => "traces"),
            m_logs_latency: ::metrics::histogram!("bisque_otel_ingest_latency_ms", "catalog" => cat.clone(), "signal" => "logs"),
            m_unknown_latency: ::metrics::histogram!("bisque_otel_ingest_latency_ms", "catalog" => cat.clone(), "signal" => "unknown"),
            m_metrics_errors: ::metrics::counter!("bisque_otel_ingest_errors_total", "catalog" => cat.clone(), "signal" => "metrics"),
            m_traces_errors: ::metrics::counter!("bisque_otel_ingest_errors_total", "catalog" => cat.clone(), "signal" => "traces"),
            m_logs_errors: ::metrics::counter!("bisque_otel_ingest_errors_total", "catalog" => cat.clone(), "signal" => "logs"),
            m_unknown_errors: ::metrics::counter!("bisque_otel_ingest_errors_total", "catalog" => cat, "signal" => "unknown"),
        }
    }

    /// Ensure all OTEL tables exist, creating them if needed.
    pub async fn ensure_tables(&self) -> Result<(), WriteError> {
        let tables = [
            (schema::COUNTERS_TABLE, schema::counter_schema()),
            (schema::GAUGES_TABLE, schema::gauge_schema()),
            (schema::HISTOGRAMS_TABLE, schema::histogram_schema()),
            (schema::EXP_HISTOGRAMS_TABLE, schema::exp_histogram_schema()),
            (schema::SUMMARIES_TABLE, schema::summary_schema()),
            (schema::SPANS_TABLE, schema::span_schema()),
            (schema::SPAN_EVENTS_TABLE, schema::span_event_schema()),
            (schema::SPAN_LINKS_TABLE, schema::span_link_schema()),
            (schema::LOGS_TABLE, schema::log_schema()),
            (schema::EXEMPLARS_TABLE, schema::exemplar_schema()),
        ];

        for (name, table_schema) in &tables {
            if !self.raft_node.engine().has_table(name) {
                self.raft_node.create_table(name, table_schema).await?;
                info!(table = name, "created OTEL table");
            }
        }

        // Specialized OTEL processors for metric tables — pre-aggregate data
        // before hitting Raft consensus.
        self.raft_node.configure_table_batcher(
            schema::COUNTERS_TABLE,
            WriteBatcherConfig::default().with_processor(Arc::new(
                OtelSumAggregator::new(60_000), // 60s timestamp truncation
            )),
        );

        self.raft_node.configure_table_batcher(
            schema::GAUGES_TABLE,
            WriteBatcherConfig::default().with_processor(Arc::new(OtelGaugeProcessor::new())),
        );

        self.raft_node.configure_table_batcher(
            schema::HISTOGRAMS_TABLE,
            WriteBatcherConfig::default().with_processor(Arc::new(OtelHistogramProcessor::new())),
        );

        self.raft_node.configure_table_batcher(
            schema::EXP_HISTOGRAMS_TABLE,
            WriteBatcherConfig::default()
                .with_processor(Arc::new(OtelExpHistogramAggregator::new())),
        );

        // Summaries, traces, logs, events, links, exemplars
        // are append-only (no processor), but still benefit from batching.
        let append_tables = [
            schema::SUMMARIES_TABLE,
            schema::SPANS_TABLE,
            schema::SPAN_EVENTS_TABLE,
            schema::SPAN_LINKS_TABLE,
            schema::LOGS_TABLE,
            schema::EXEMPLARS_TABLE,
        ];
        for table in append_tables {
            self.raft_node
                .configure_table_batcher(table, WriteBatcherConfig::default());
        }

        Ok(())
    }

    /// Write record batches to a table through the Raft node.
    pub(crate) async fn write_to_table(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), WriteError> {
        let start = std::time::Instant::now();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        let result = self.raft_node.write_records(table_name, &batches).await;
        if let Some(tm) = self.table_metrics.get(table_name) {
            tm.received.increment(row_count as u64);
        }
        let (latency_handle, error_handle) = match table_name {
            "otel_counters"
            | "otel_gauges"
            | "otel_histograms"
            | "otel_exp_histograms"
            | "otel_summaries"
            | "otel_exemplars" => (&self.m_metrics_latency, &self.m_metrics_errors),
            "otel_spans" | "otel_span_events" | "otel_span_links" => {
                (&self.m_traces_latency, &self.m_traces_errors)
            }
            "otel_logs" => (&self.m_logs_latency, &self.m_logs_errors),
            _ => (&self.m_unknown_latency, &self.m_unknown_errors),
        };
        latency_handle.record(start.elapsed().as_millis() as f64);
        if result.is_err() {
            error_handle.increment(1);
        }
        result?;
        Ok(())
    }

    /// Get a reference to the underlying Raft node.
    pub(crate) fn raft_node(&self) -> &Arc<LanceRaftNode> {
        &self.raft_node
    }
}

/// Start an OTLP gRPC server with both standard OTLP and OTel-Arrow services.
///
/// Creates the OTEL tables if they don't exist, then serves all collector
/// services (metrics, traces, logs) on the given address.
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # use bisque_lance::otel::serve_otlp;
/// # async fn run(raft_node: Arc<bisque_lance::LanceRaftNode>) {
/// let addr = "0.0.0.0:4317".parse().unwrap();
/// serve_otlp(raft_node, addr).await.unwrap();
/// # }
/// ```
pub async fn serve_otlp(
    raft_node: Arc<LanceRaftNode>,
    addr: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let receiver = Arc::new(OtlpReceiver::new(raft_node));

    info!(%addr, "starting OTLP gRPC server (with OTel-Arrow support)");

    Server::builder()
        // Standard OTLP protobuf services
        .add_service(
            MetricsServiceServer::from_arc(receiver.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .add_service(
            TraceServiceServer::from_arc(receiver.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .add_service(
            LogsServiceServer::from_arc(receiver.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        // OTel-Arrow streaming services
        .add_service(
            ArrowTracesServiceServer::from_arc(receiver.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .add_service(
            ArrowLogsServiceServer::from_arc(receiver.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .add_service(
            ArrowMetricsServiceServer::from_arc(receiver.clone())
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .serve(addr)
        .await?;

    Ok(())
}

/// Start an HTTP server for Tempo, Prometheus, Loki, and OTLP HTTP APIs.
///
/// Shares a DataFusion `SessionContext` with the same dynamic catalog
/// used by Flight SQL and the PostgreSQL wire protocol.
///
/// # Endpoints
///
/// - **Tempo**: `/api/traces/{traceID}`, `/api/search`, `/api/search/tags`, etc.
/// - **Prometheus/Mimir**: `/api/v1/query`, `/api/v1/query_range`, `/api/v1/labels`,
///   `/api/v1/label/{name}/values`, `/api/v1/series`, `/api/v1/metadata`,
///   `/api/v1/read` (remote-read), `/api/v1/push` (remote-write)
/// - **Loki**: `/loki/api/v1/query`, `/loki/api/v1/query_range`, `/loki/api/v1/labels`,
///   `/loki/api/v1/label/{name}/values`, `/loki/api/v1/series`, `/loki/api/v1/push`
/// - **OTLP HTTP**: `POST /v1/traces`, `POST /v1/metrics`, `POST /v1/logs`
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # use bisque_lance::otel::serve_http;
/// # async fn run(raft_node: Arc<bisque_lance::LanceRaftNode>) {
/// let addr = "0.0.0.0:3200".parse().unwrap();
/// serve_http(raft_node, addr).await.unwrap();
/// # }
/// ```
/// Build an axum Router for Tempo, Prometheus, Loki, and OTLP HTTP APIs.
///
/// Returns a composable Router that can be merged into a larger server.
/// Call [`OtlpReceiver::ensure_tables`] before calling this function.
pub fn otel_http_router(
    raft_node: Arc<LanceRaftNode>,
    receiver: Arc<OtlpReceiver>,
) -> axum::Router {
    let engine = raft_node.engine().clone();

    let session_config = SessionConfig::new()
        .with_default_catalog_and_schema("bisque", "public")
        .with_create_default_catalog_and_schema(false);
    let ctx = SessionContext::new_with_config(session_config);

    let catalog = Arc::new(BisqueLanceCatalogProvider::new(engine));
    ctx.register_catalog("bisque", catalog);

    let state = Arc::new(tempo::HttpQueryState {
        ctx: Arc::new(ctx),
        receiver,
    });

    axum::Router::new()
        // --- Tempo endpoints ---
        .route(
            "/api/traces/{traceID}",
            axum::routing::get(tempo::get_trace),
        )
        .route("/api/search", axum::routing::get(tempo::search_traces))
        .route("/api/search/tags", axum::routing::get(tempo::get_tags))
        .route(
            "/api/search/tag/{tag}/values",
            axum::routing::get(tempo::get_tag_values),
        )
        .route(
            "/api/v2/search/tags",
            axum::routing::get(tempo::get_tags_v2),
        )
        .route(
            "/api/v2/search/tag/{tag}/values",
            axum::routing::get(tempo::get_tag_values_v2),
        )
        // --- OTLP HTTP ingest ---
        .route("/v1/traces", axum::routing::post(tempo::otlp_ingest_traces))
        .route(
            "/v1/metrics",
            axum::routing::post(tempo::otlp_ingest_metrics),
        )
        .route("/v1/logs", axum::routing::post(tempo::otlp_ingest_logs))
        // --- Prometheus/Mimir endpoints ---
        .route(
            "/api/v1/query",
            axum::routing::get(prom_api::prom_instant_query).post(prom_api::prom_instant_query),
        )
        .route(
            "/api/v1/query_range",
            axum::routing::get(prom_api::prom_range_query).post(prom_api::prom_range_query),
        )
        .route("/api/v1/labels", axum::routing::get(prom_api::prom_labels))
        .route(
            "/api/v1/label/{name}/values",
            axum::routing::get(prom_api::prom_label_values),
        )
        .route("/api/v1/series", axum::routing::get(prom_api::prom_series))
        .route(
            "/api/v1/metadata",
            axum::routing::get(prom_api::prom_metadata),
        )
        .route(
            "/api/v1/read",
            axum::routing::post(prom_read::prom_remote_read),
        )
        .route(
            "/api/v1/push",
            axum::routing::post(prom_api::prom_remote_write),
        )
        // --- Loki endpoints ---
        .route("/loki/api/v1/query", axum::routing::get(loki::loki_query))
        .route(
            "/loki/api/v1/query_range",
            axum::routing::get(loki::loki_query_range),
        )
        .route("/loki/api/v1/labels", axum::routing::get(loki::loki_labels))
        .route(
            "/loki/api/v1/label/{name}/values",
            axum::routing::get(loki::loki_label_values),
        )
        .route("/loki/api/v1/series", axum::routing::get(loki::loki_series))
        .route("/loki/api/v1/push", axum::routing::post(loki::loki_push))
        .with_state(state)
}

pub async fn serve_http(
    raft_node: Arc<LanceRaftNode>,
    addr: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let receiver = OtlpReceiver::new(raft_node.clone());
    receiver.ensure_tables().await?;
    let receiver = Arc::new(receiver);

    let app = otel_http_router(raft_node, receiver);

    info!(%addr, "starting Tempo/Prometheus/Loki HTTP server");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

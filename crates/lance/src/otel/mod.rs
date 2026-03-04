//! OpenTelemetry OTLP gRPC receiver for bisque-lance.
//!
//! Implements the three standard OTLP collector services:
//! - **MetricsService** — decomposes metrics by type into separate tables
//! - **TraceService** — writes spans to `otel_spans`
//! - **LogsService** — writes log records to `otel_logs`
//!
//! Each service converts protobuf data to Arrow RecordBatches and writes
//! through the Raft-replicated [`LanceRaftNode`].

pub mod convert;
pub mod logs;
pub mod metrics;
pub mod schema;
pub mod traces;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::TimeUnit;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;
use tracing::info;

use crate::processors::{CounterAggregator, GaugeAggregator, HistogramAggregator};
use crate::raft::{LanceRaftNode, WriteError};
use crate::write_batcher::WriteBatcherConfig;

/// OTLP gRPC receiver backed by a bisque-lance Raft node.
///
/// Implements `MetricsService`, `TraceService`, and `LogsService` from the
/// OTLP collector protocol. Data is written through Raft consensus via
/// [`LanceRaftNode::write_records`].
pub struct OtlpReceiver {
    raft_node: Arc<LanceRaftNode>,
}

impl OtlpReceiver {
    /// Create a new OTLP receiver wrapping the given Raft node.
    pub fn new(raft_node: Arc<LanceRaftNode>) -> Self {
        Self { raft_node }
    }

    /// Ensure all OTEL tables exist, creating them if needed.
    ///
    /// Creates: `otel_counters`, `otel_gauges`, `otel_histograms`, `otel_spans`, `otel_logs`.
    pub async fn ensure_tables(&self) -> Result<(), WriteError> {
        let tables = [
            (schema::COUNTERS_TABLE, schema::counter_schema()),
            (schema::GAUGES_TABLE, schema::gauge_schema()),
            (schema::HISTOGRAMS_TABLE, schema::histogram_schema()),
            (schema::SPANS_TABLE, schema::span_schema()),
            (schema::LOGS_TABLE, schema::log_schema()),
        ];

        for (name, table_schema) in &tables {
            if !self.raft_node.engine().has_table(name) {
                self.raft_node.create_table(name, table_schema).await?;
                info!(table = name, "created OTEL table");
            }
        }

        // Configure WriteProcessors for metric tables so data is pre-aggregated
        // before hitting Raft consensus.
        let counter_key_cols = vec![
            "metric_name".to_string(),
            "attributes".to_string(),
            "resource_attributes".to_string(),
            "scope_name".to_string(),
            "scope_version".to_string(),
        ];

        self.raft_node.configure_table_batcher(
            schema::COUNTERS_TABLE,
            WriteBatcherConfig::default().with_processor(Arc::new(
                CounterAggregator::new(counter_key_cols.clone(), "value")
                    .with_timestamp("timestamp", 60_000)
                    .with_timestamp_unit(TimeUnit::Nanosecond),
            )),
        );

        self.raft_node.configure_table_batcher(
            schema::GAUGES_TABLE,
            WriteBatcherConfig::default().with_processor(Arc::new(
                GaugeAggregator::new(counter_key_cols.clone(), "value")
                    .with_timestamp("timestamp")
                    .with_timestamp_unit(TimeUnit::Nanosecond),
            )),
        );

        self.raft_node.configure_table_batcher(
            schema::HISTOGRAMS_TABLE,
            WriteBatcherConfig::default().with_processor(Arc::new(
                HistogramAggregator::new(counter_key_cols)
                    .with_timestamp("timestamp")
                    .with_timestamp_unit(TimeUnit::Nanosecond),
            )),
        );

        // Traces and logs are append-only (no processor), but still benefit
        // from batching — coalesces many small Export() calls into fewer Raft
        // proposals per linger window.
        self.raft_node
            .configure_table_batcher(schema::SPANS_TABLE, WriteBatcherConfig::default());
        self.raft_node
            .configure_table_batcher(schema::LOGS_TABLE, WriteBatcherConfig::default());

        Ok(())
    }

    /// Write record batches to a table through the Raft node.
    pub(crate) async fn write_to_table(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), WriteError> {
        self.raft_node.write_records(table_name, &batches).await?;
        Ok(())
    }
}

/// Start an OTLP gRPC server.
///
/// Creates the OTEL tables if they don't exist, then serves the three
/// OTLP collector services (metrics, traces, logs) on the given address.
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
    let receiver = OtlpReceiver::new(raft_node);
    receiver.ensure_tables().await?;

    let receiver = Arc::new(receiver);

    info!(%addr, "starting OTLP gRPC server");

    Server::builder()
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
        .serve(addr)
        .await?;

    Ok(())
}

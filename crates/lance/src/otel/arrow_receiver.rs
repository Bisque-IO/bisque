//! OTel-Arrow receiver implementation.
//!
//! Implements the three Arrow streaming services (`ArrowTracesService`,
//! `ArrowLogsService`, `ArrowMetricsService`) for the [`OtlpReceiver`].
//!
//! Each service receives a bidirectional gRPC stream of `BatchArrowRecords`,
//! decodes the Arrow IPC payloads into RecordBatches, and writes them to
//! the appropriate tables through Raft consensus.

use std::collections::HashMap;
use std::io::Cursor;
use std::pin::Pin;

use arrow_ipc::reader::StreamReader;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

use super::arrow_proto::{
    ArrowLogsService, ArrowMetricsService, ArrowPayloadType, ArrowTracesService,
    BatchArrowRecords, BatchStatus, StatusCode,
};
use super::schema;
use super::OtlpReceiver;

/// Map an `ArrowPayloadType` to the target table name.
fn payload_type_to_table(payload_type: i32) -> Option<&'static str> {
    match ArrowPayloadType::try_from(payload_type) {
        Ok(ArrowPayloadType::ResourceAttrs) => Some("otap_resource_attrs"),
        Ok(ArrowPayloadType::ScopeAttrs) => Some("otap_scope_attrs"),
        Ok(ArrowPayloadType::Spans) => Some(schema::SPANS_TABLE),
        Ok(ArrowPayloadType::SpanAttrs) => Some("otap_span_attrs"),
        Ok(ArrowPayloadType::SpanEvents) => Some(schema::SPAN_EVENTS_TABLE),
        Ok(ArrowPayloadType::SpanLinks) => Some(schema::SPAN_LINKS_TABLE),
        Ok(ArrowPayloadType::SpanEventAttrs) => Some("otap_span_event_attrs"),
        Ok(ArrowPayloadType::SpanLinkAttrs) => Some("otap_span_link_attrs"),
        Ok(ArrowPayloadType::Logs) => Some(schema::LOGS_TABLE),
        Ok(ArrowPayloadType::LogAttrs) => Some("otap_log_attrs"),
        Ok(ArrowPayloadType::UnivariateMetrics) => Some(schema::COUNTERS_TABLE),
        Ok(ArrowPayloadType::MultivariateMetrics) => Some("otap_multivariate_metrics"),
        Ok(ArrowPayloadType::MetricAttrs) => Some("otap_metric_attrs"),
        Ok(ArrowPayloadType::NumberDataPoints) => Some(schema::COUNTERS_TABLE),
        Ok(ArrowPayloadType::SummaryDataPoints) => Some("otap_summary_data_points"),
        Ok(ArrowPayloadType::HistogramDataPoints) => Some(schema::HISTOGRAMS_TABLE),
        Ok(ArrowPayloadType::ExpHistogramDataPoints) => Some(schema::EXP_HISTOGRAMS_TABLE),
        Ok(ArrowPayloadType::NumberDpAttrs) => Some("otap_number_dp_attrs"),
        Ok(ArrowPayloadType::SummaryDpAttrs) => Some("otap_summary_dp_attrs"),
        Ok(ArrowPayloadType::HistogramDpAttrs) => Some("otap_histogram_dp_attrs"),
        Ok(ArrowPayloadType::ExpHistogramDpAttrs) => Some("otap_exp_histogram_dp_attrs"),
        Ok(ArrowPayloadType::NumberDpExemplars) => Some(schema::EXEMPLARS_TABLE),
        Ok(ArrowPayloadType::HistogramDpExemplars) => Some(schema::EXEMPLARS_TABLE),
        Ok(ArrowPayloadType::ExpHistogramDpExemplars) => Some(schema::EXEMPLARS_TABLE),
        Ok(ArrowPayloadType::NumberDpExemplarAttrs) => Some("otap_exemplar_attrs"),
        Ok(ArrowPayloadType::HistogramDpExemplarAttrs) => Some("otap_exemplar_attrs"),
        Ok(ArrowPayloadType::ExpHistogramDpExemplarAttrs) => Some("otap_exemplar_attrs"),
        Ok(ArrowPayloadType::Unknown) | Err(_) => None,
    }
}

impl OtlpReceiver {
    /// Process a stream of `BatchArrowRecords` and write the decoded Arrow IPC
    /// payloads to the appropriate tables. Returns a stream of `BatchStatus`
    /// acknowledgments.
    async fn process_arrow_stream(
        &self,
        mut stream: Streaming<BatchArrowRecords>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<BatchStatus, Status>> + Send>>>,
        Status,
    > {
        let (tx, rx) = mpsc::channel(64);
        let raft_node = self.raft_node().clone();

        tokio::spawn(async move {
            // Cache IPC readers by schema_id to reuse across payloads.
            let mut readers: HashMap<String, ()> = HashMap::new();

            while let Some(result) = stream.next().await {
                let batch = match result {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(error = %e, "error receiving arrow batch");
                        break;
                    }
                };

                let batch_id = batch.batch_id;
                let mut status_code = StatusCode::Ok as i32;
                let mut status_message = String::new();

                for payload in &batch.arrow_payloads {
                    let table_name = match payload_type_to_table(payload.r#type) {
                        Some(name) => name,
                        None => {
                            warn!(
                                payload_type = payload.r#type,
                                "unknown arrow payload type, skipping"
                            );
                            continue;
                        }
                    };

                    // Track schema changes
                    readers.entry(payload.schema_id.clone()).or_insert(());

                    // Decode Arrow IPC bytes into RecordBatch
                    let cursor = Cursor::new(&payload.record);
                    let reader = match StreamReader::try_new(cursor, None) {
                        Ok(r) => r,
                        Err(e) => {
                            warn!(
                                error = %e,
                                schema_id = %payload.schema_id,
                                "failed to create Arrow IPC reader"
                            );
                            status_code = StatusCode::InvalidArgument as i32;
                            status_message =
                                format!("failed to decode Arrow IPC: {e}");
                            continue;
                        }
                    };

                    let mut batches = Vec::new();
                    for batch_result in reader {
                        match batch_result {
                            Ok(record_batch) => {
                                if record_batch.num_rows() > 0 {
                                    batches.push(record_batch);
                                }
                            }
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    "failed to read Arrow record batch"
                                );
                                status_code = StatusCode::InvalidArgument as i32;
                                status_message =
                                    format!("failed to read record batch: {e}");
                            }
                        }
                    }

                    if !batches.is_empty() {
                        // Auto-create table if it doesn't exist, using the
                        // schema from the incoming Arrow data.
                        if !raft_node.engine().has_table(table_name) {
                            let table_schema = batches[0].schema();
                            if let Err(e) = raft_node
                                .create_table(table_name, table_schema.as_ref())
                                .await
                            {
                                warn!(
                                    error = %e,
                                    table = table_name,
                                    "failed to create table for arrow payload"
                                );
                                status_code = StatusCode::Internal as i32;
                                status_message =
                                    format!("failed to create table {table_name}: {e}");
                                continue;
                            }
                            debug!(table = table_name, "auto-created OTAP table");
                        }

                        if let Err(e) =
                            raft_node.write_records(table_name, &batches).await
                        {
                            warn!(
                                error = %e,
                                table = table_name,
                                "failed to write arrow records"
                            );
                            status_code = StatusCode::Internal as i32;
                            status_message =
                                format!("failed to write to {table_name}: {e}");
                        } else {
                            let total_rows: usize =
                                batches.iter().map(|b| b.num_rows()).sum();
                            debug!(
                                table = table_name,
                                rows = total_rows,
                                "wrote arrow payload"
                            );
                        }
                    }
                }

                let response = BatchStatus {
                    batch_id,
                    status_code,
                    status_message,
                };

                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(stream)
                as Pin<Box<dyn Stream<Item = Result<BatchStatus, Status>> + Send>>,
        ))
    }
}

#[tonic::async_trait]
impl ArrowTracesService for OtlpReceiver {
    type ArrowTracesStream =
        Pin<Box<dyn Stream<Item = Result<BatchStatus, Status>> + Send>>;

    async fn arrow_traces(
        &self,
        request: Request<Streaming<BatchArrowRecords>>,
    ) -> Result<Response<Self::ArrowTracesStream>, Status> {
        debug!("otel-arrow traces stream opened");
        self.process_arrow_stream(request.into_inner()).await
    }
}

#[tonic::async_trait]
impl ArrowLogsService for OtlpReceiver {
    type ArrowLogsStream =
        Pin<Box<dyn Stream<Item = Result<BatchStatus, Status>> + Send>>;

    async fn arrow_logs(
        &self,
        request: Request<Streaming<BatchArrowRecords>>,
    ) -> Result<Response<Self::ArrowLogsStream>, Status> {
        debug!("otel-arrow logs stream opened");
        self.process_arrow_stream(request.into_inner()).await
    }
}

#[tonic::async_trait]
impl ArrowMetricsService for OtlpReceiver {
    type ArrowMetricsStream =
        Pin<Box<dyn Stream<Item = Result<BatchStatus, Status>> + Send>>;

    async fn arrow_metrics(
        &self,
        request: Request<Streaming<BatchArrowRecords>>,
    ) -> Result<Response<Self::ArrowMetricsStream>, Status> {
        debug!("otel-arrow metrics stream opened");
        self.process_arrow_stream(request.into_inner()).await
    }
}

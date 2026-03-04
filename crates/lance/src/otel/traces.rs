//! OTLP TraceService implementation.
//!
//! Receives OTEL trace/span data via gRPC, converts to Arrow RecordBatches,
//! and writes to the `otel_spans` table through Raft consensus.

use std::sync::Arc;

use arrow_array::builder::{
    FixedSizeBinaryBuilder, Int32Builder, StringBuilder, TimestampNanosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use tonic::{Request, Response, Status};
use tracing::debug;

use super::convert::{key_values_to_json, pad_or_truncate};
use super::schema;
use super::OtlpReceiver;

#[tonic::async_trait]
impl TraceService for OtlpReceiver {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let req = request.into_inner();
        let mut total_spans = 0usize;

        // Pre-count spans for builder capacity.
        for rs in &req.resource_spans {
            for ss in &rs.scope_spans {
                total_spans += ss.spans.len();
            }
        }

        if total_spans == 0 {
            return Ok(Response::new(ExportTraceServiceResponse {
                partial_success: None,
            }));
        }

        let n = total_spans;
        let mut trace_id = FixedSizeBinaryBuilder::with_capacity(n, 16);
        let mut span_id = FixedSizeBinaryBuilder::with_capacity(n, 8);
        let mut parent_span_id = FixedSizeBinaryBuilder::with_capacity(n, 8);
        let mut trace_state = StringBuilder::with_capacity(n, n * 16);
        let mut name = StringBuilder::with_capacity(n, n * 32);
        let mut kind = Int32Builder::with_capacity(n);
        let mut start_time = TimestampNanosecondBuilder::with_capacity(n);
        let mut end_time = TimestampNanosecondBuilder::with_capacity(n);
        let mut attributes = StringBuilder::with_capacity(n, n * 64);
        let mut status_code = Int32Builder::with_capacity(n);
        let mut status_message = StringBuilder::with_capacity(n, n * 32);
        let mut resource_attributes = StringBuilder::with_capacity(n, n * 64);
        let mut scope_name = StringBuilder::with_capacity(n, n * 32);
        let mut scope_version = StringBuilder::with_capacity(n, n * 16);

        for rs in &req.resource_spans {
            let res_attrs_json = rs
                .resource
                .as_ref()
                .map(|r| key_values_to_json(&r.attributes))
                .unwrap_or_else(|| "{}".to_string());

            for ss in &rs.scope_spans {
                let (sc_name, sc_version) = ss
                    .scope
                    .as_ref()
                    .map(|s| (s.name.as_str(), s.version.as_str()))
                    .unwrap_or(("", ""));

                for span in &ss.spans {
                    // trace_id: pad/truncate to exactly 16 bytes
                    let tid = pad_or_truncate(&span.trace_id, 16);
                    trace_id.append_value(&tid).expect("trace_id must be 16 bytes");

                    // span_id: pad/truncate to exactly 8 bytes
                    let sid = pad_or_truncate(&span.span_id, 8);
                    span_id.append_value(&sid).expect("span_id must be 8 bytes");

                    // parent_span_id: pad/truncate to exactly 8 bytes (zeros if empty)
                    let psid = pad_or_truncate(&span.parent_span_id, 8);
                    parent_span_id
                        .append_value(&psid)
                        .expect("parent_span_id must be 8 bytes");

                    trace_state.append_value(&span.trace_state);
                    name.append_value(&span.name);
                    kind.append_value(span.kind);
                    start_time.append_value(span.start_time_unix_nano as i64);
                    end_time.append_value(span.end_time_unix_nano as i64);
                    attributes.append_value(key_values_to_json(&span.attributes));

                    let (sc, sm) = span
                        .status
                        .as_ref()
                        .map(|s| (s.code, s.message.as_str()))
                        .unwrap_or((0, ""));
                    status_code.append_value(sc);
                    status_message.append_value(sm);

                    resource_attributes.append_value(&res_attrs_json);
                    scope_name.append_value(sc_name);
                    scope_version.append_value(sc_version);
                }
            }
        }

        let schema = schema::span_schema_ref().clone();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(trace_id.finish()),
            Arc::new(span_id.finish()),
            Arc::new(parent_span_id.finish()),
            Arc::new(trace_state.finish()),
            Arc::new(name.finish()),
            Arc::new(kind.finish()),
            Arc::new(start_time.finish()),
            Arc::new(end_time.finish()),
            Arc::new(attributes.finish()),
            Arc::new(status_code.finish()),
            Arc::new(status_message.finish()),
            Arc::new(resource_attributes.finish()),
            Arc::new(scope_name.finish()),
            Arc::new(scope_version.finish()),
        ];

        let batch =
            RecordBatch::try_new(schema, columns).expect("schema mismatch in span batch");

        debug!(spans = total_spans, "otlp trace export");

        self.write_to_table(schema::SPANS_TABLE, vec![batch])
            .await
            .map_err(|e| Status::internal(format!("failed to write spans: {e}")))?;

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

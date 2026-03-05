//! OTLP LogsService implementation.
//!
//! Receives OTEL log records via gRPC, converts to Arrow RecordBatches,
//! and writes to the `otel_logs` table through Raft consensus.

use std::sync::Arc;

use arrow_array::builder::{
    FixedSizeBinaryBuilder, Int32Builder, StringBuilder, TimestampNanosecondBuilder, UInt32Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use tonic::{Request, Response, Status};
use tracing::debug;

use super::OtlpReceiver;
use super::convert::{any_value_to_string_typed, key_values_to_json, pad_or_truncate};
use super::schema;

#[tonic::async_trait]
impl LogsService for OtlpReceiver {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let req = request.into_inner();
        let mut total_logs = 0usize;

        // Pre-count log records for builder capacity.
        for rl in &req.resource_logs {
            for sl in &rl.scope_logs {
                total_logs += sl.log_records.len();
            }
        }

        if total_logs == 0 {
            return Ok(Response::new(ExportLogsServiceResponse {
                partial_success: None,
            }));
        }

        let n = total_logs;
        let mut timestamp = TimestampNanosecondBuilder::with_capacity(n);
        let mut observed_timestamp = TimestampNanosecondBuilder::with_capacity(n);
        let mut trace_id = FixedSizeBinaryBuilder::with_capacity(n, 16);
        let mut span_id = FixedSizeBinaryBuilder::with_capacity(n, 8);
        let mut severity_number = Int32Builder::with_capacity(n);
        let mut severity_text = StringBuilder::with_capacity(n, n * 8);
        let mut body = StringBuilder::with_capacity(n, n * 128);
        let mut body_type = StringBuilder::with_capacity(n, n * 8);
        let mut attributes = StringBuilder::with_capacity(n, n * 64);
        let mut resource_attributes = StringBuilder::with_capacity(n, n * 64);
        let mut resource_dropped_attributes_count = UInt32Builder::with_capacity(n);
        let mut resource_schema_url = StringBuilder::with_capacity(n, n * 32);
        let mut scope_name = StringBuilder::with_capacity(n, n * 32);
        let mut scope_version = StringBuilder::with_capacity(n, n * 16);
        let mut scope_attributes = StringBuilder::with_capacity(n, n * 64);
        let mut scope_dropped_attributes_count = UInt32Builder::with_capacity(n);
        let mut scope_schema_url = StringBuilder::with_capacity(n, n * 32);
        let mut flags = UInt32Builder::with_capacity(n);
        let mut dropped_attributes_count = UInt32Builder::with_capacity(n);
        let mut event_name = StringBuilder::with_capacity(n, n * 32);

        for rl in &req.resource_logs {
            let res_attrs_json = rl
                .resource
                .as_ref()
                .map(|r| key_values_to_json(&r.attributes))
                .unwrap_or_else(|| "{}".to_string());
            let res_dropped = rl
                .resource
                .as_ref()
                .map(|r| r.dropped_attributes_count)
                .unwrap_or(0);
            let res_schema = &rl.schema_url;

            for sl in &rl.scope_logs {
                let (sc_name, sc_version) = sl
                    .scope
                    .as_ref()
                    .map(|s| (s.name.as_str(), s.version.as_str()))
                    .unwrap_or(("", ""));
                let sc_attrs_json = sl
                    .scope
                    .as_ref()
                    .map(|s| key_values_to_json(&s.attributes))
                    .unwrap_or_else(|| "{}".to_string());
                let sc_dropped = sl
                    .scope
                    .as_ref()
                    .map(|s| s.dropped_attributes_count)
                    .unwrap_or(0);
                let sc_schema = &sl.schema_url;

                for lr in &sl.log_records {
                    timestamp.append_value(lr.time_unix_nano as i64);
                    observed_timestamp.append_value(lr.observed_time_unix_nano as i64);

                    let tid = pad_or_truncate(&lr.trace_id, 16);
                    trace_id
                        .append_value(&tid)
                        .expect("trace_id must be 16 bytes");

                    let sid = pad_or_truncate(&lr.span_id, 8);
                    span_id.append_value(&sid).expect("span_id must be 8 bytes");

                    severity_number.append_value(lr.severity_number);
                    severity_text.append_value(&lr.severity_text);
                    let (body_str, body_ty) = any_value_to_string_typed(lr.body.as_ref());
                    body.append_value(body_str);
                    body_type.append_value(body_ty);
                    attributes.append_value(key_values_to_json(&lr.attributes));
                    resource_attributes.append_value(&res_attrs_json);
                    resource_dropped_attributes_count.append_value(res_dropped);
                    resource_schema_url.append_value(res_schema);
                    scope_name.append_value(sc_name);
                    scope_version.append_value(sc_version);
                    scope_attributes.append_value(&sc_attrs_json);
                    scope_dropped_attributes_count.append_value(sc_dropped);
                    scope_schema_url.append_value(sc_schema);
                    flags.append_value(lr.flags);
                    dropped_attributes_count.append_value(lr.dropped_attributes_count);
                    event_name.append_value(&lr.event_name);
                }
            }
        }

        let schema = schema::log_schema_ref().clone();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(timestamp.finish()),
            Arc::new(observed_timestamp.finish()),
            Arc::new(trace_id.finish()),
            Arc::new(span_id.finish()),
            Arc::new(severity_number.finish()),
            Arc::new(severity_text.finish()),
            Arc::new(body.finish()),
            Arc::new(body_type.finish()),
            Arc::new(attributes.finish()),
            Arc::new(resource_attributes.finish()),
            Arc::new(resource_dropped_attributes_count.finish()),
            Arc::new(resource_schema_url.finish()),
            Arc::new(scope_name.finish()),
            Arc::new(scope_version.finish()),
            Arc::new(scope_attributes.finish()),
            Arc::new(scope_dropped_attributes_count.finish()),
            Arc::new(scope_schema_url.finish()),
            Arc::new(flags.finish()),
            Arc::new(dropped_attributes_count.finish()),
            Arc::new(event_name.finish()),
        ];

        let batch = RecordBatch::try_new(schema, columns).expect("schema mismatch in log batch");

        debug!(logs = total_logs, "otlp logs export");

        self.write_to_table(schema::LOGS_TABLE, vec![batch])
            .await
            .map_err(|e| Status::internal(format!("failed to write logs: {e}")))?;

        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

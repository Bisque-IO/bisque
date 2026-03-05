//! OTLP TraceService implementation.
//!
//! Receives OTEL trace/span data via gRPC, converts to Arrow RecordBatches,
//! and writes to the `otel_spans`, `otel_span_events`, and `otel_span_links`
//! tables through Raft consensus.

use std::sync::Arc;

use arrow_array::builder::{
    FixedSizeBinaryBuilder, Int32Builder, StringBuilder, TimestampNanosecondBuilder, UInt32Builder,
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

        // Pre-count spans, events, and links for builder capacity.
        let mut total_spans = 0usize;
        let mut total_events = 0usize;
        let mut total_links = 0usize;

        for rs in &req.resource_spans {
            for ss in &rs.scope_spans {
                for span in &ss.spans {
                    total_spans += 1;
                    total_events += span.events.len();
                    total_links += span.links.len();
                }
            }
        }

        if total_spans == 0 {
            return Ok(Response::new(ExportTraceServiceResponse {
                partial_success: None,
            }));
        }

        // Span builders
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
        let mut resource_dropped_attributes_count = UInt32Builder::with_capacity(n);
        let mut resource_schema_url = StringBuilder::with_capacity(n, n * 32);
        let mut scope_name = StringBuilder::with_capacity(n, n * 32);
        let mut scope_version = StringBuilder::with_capacity(n, n * 16);
        let mut scope_attributes = StringBuilder::with_capacity(n, n * 64);
        let mut scope_dropped_attributes_count = UInt32Builder::with_capacity(n);
        let mut scope_schema_url = StringBuilder::with_capacity(n, n * 32);
        let mut flags = UInt32Builder::with_capacity(n);
        let mut dropped_attributes_count = UInt32Builder::with_capacity(n);
        let mut dropped_events_count = UInt32Builder::with_capacity(n);
        let mut dropped_links_count = UInt32Builder::with_capacity(n);

        // Event builders
        let mut evt_trace_id = FixedSizeBinaryBuilder::with_capacity(total_events, 16);
        let mut evt_span_id = FixedSizeBinaryBuilder::with_capacity(total_events, 8);
        let mut evt_timestamp =
            TimestampNanosecondBuilder::with_capacity(total_events);
        let mut evt_name = StringBuilder::with_capacity(total_events, total_events * 32);
        let mut evt_attributes =
            StringBuilder::with_capacity(total_events, total_events * 64);
        let mut evt_dropped_attrs = UInt32Builder::with_capacity(total_events);

        // Link builders
        let mut lnk_trace_id = FixedSizeBinaryBuilder::with_capacity(total_links, 16);
        let mut lnk_span_id = FixedSizeBinaryBuilder::with_capacity(total_links, 8);
        let mut lnk_linked_trace_id =
            FixedSizeBinaryBuilder::with_capacity(total_links, 16);
        let mut lnk_linked_span_id =
            FixedSizeBinaryBuilder::with_capacity(total_links, 8);
        let mut lnk_trace_state =
            StringBuilder::with_capacity(total_links, total_links * 16);
        let mut lnk_attributes =
            StringBuilder::with_capacity(total_links, total_links * 64);
        let mut lnk_flags = UInt32Builder::with_capacity(total_links);
        let mut lnk_dropped_attrs = UInt32Builder::with_capacity(total_links);

        for rs in &req.resource_spans {
            let res_attrs_json = rs
                .resource
                .as_ref()
                .map(|r| key_values_to_json(&r.attributes))
                .unwrap_or_else(|| "{}".to_string());
            let res_dropped = rs
                .resource
                .as_ref()
                .map(|r| r.dropped_attributes_count)
                .unwrap_or(0);
            let res_schema = &rs.schema_url;

            for ss in &rs.scope_spans {
                let (sc_name, sc_version) = ss
                    .scope
                    .as_ref()
                    .map(|s| (s.name.as_str(), s.version.as_str()))
                    .unwrap_or(("", ""));
                let sc_attrs_json = ss
                    .scope
                    .as_ref()
                    .map(|s| key_values_to_json(&s.attributes))
                    .unwrap_or_else(|| "{}".to_string());
                let sc_dropped = ss
                    .scope
                    .as_ref()
                    .map(|s| s.dropped_attributes_count)
                    .unwrap_or(0);
                let sc_schema = &ss.schema_url;

                for span in &ss.spans {
                    let tid = pad_or_truncate(&span.trace_id, 16);
                    trace_id.append_value(&tid).expect("trace_id must be 16 bytes");

                    let sid = pad_or_truncate(&span.span_id, 8);
                    span_id.append_value(&sid).expect("span_id must be 8 bytes");

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
                    resource_dropped_attributes_count.append_value(res_dropped);
                    resource_schema_url.append_value(res_schema);
                    scope_name.append_value(sc_name);
                    scope_version.append_value(sc_version);
                    scope_attributes.append_value(&sc_attrs_json);
                    scope_dropped_attributes_count.append_value(sc_dropped);
                    scope_schema_url.append_value(sc_schema);
                    flags.append_value(span.flags);
                    dropped_attributes_count.append_value(span.dropped_attributes_count);
                    dropped_events_count.append_value(span.dropped_events_count);
                    dropped_links_count.append_value(span.dropped_links_count);

                    // Span events
                    for event in &span.events {
                        evt_trace_id
                            .append_value(&tid)
                            .expect("trace_id must be 16 bytes");
                        evt_span_id
                            .append_value(&sid)
                            .expect("span_id must be 8 bytes");
                        evt_timestamp.append_value(event.time_unix_nano as i64);
                        evt_name.append_value(&event.name);
                        evt_attributes.append_value(key_values_to_json(&event.attributes));
                        evt_dropped_attrs.append_value(event.dropped_attributes_count);
                    }

                    // Span links
                    for link in &span.links {
                        lnk_trace_id
                            .append_value(&tid)
                            .expect("trace_id must be 16 bytes");
                        lnk_span_id
                            .append_value(&sid)
                            .expect("span_id must be 8 bytes");

                        let linked_tid = pad_or_truncate(&link.trace_id, 16);
                        lnk_linked_trace_id
                            .append_value(&linked_tid)
                            .expect("linked_trace_id must be 16 bytes");

                        let linked_sid = pad_or_truncate(&link.span_id, 8);
                        lnk_linked_span_id
                            .append_value(&linked_sid)
                            .expect("linked_span_id must be 8 bytes");

                        lnk_trace_state.append_value(&link.trace_state);
                        lnk_attributes.append_value(key_values_to_json(&link.attributes));
                        lnk_flags.append_value(link.flags);
                        lnk_dropped_attrs.append_value(link.dropped_attributes_count);
                    }
                }
            }
        }

        let span_batch = {
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
                Arc::new(resource_dropped_attributes_count.finish()),
                Arc::new(resource_schema_url.finish()),
                Arc::new(scope_name.finish()),
                Arc::new(scope_version.finish()),
                Arc::new(scope_attributes.finish()),
                Arc::new(scope_dropped_attributes_count.finish()),
                Arc::new(scope_schema_url.finish()),
                Arc::new(flags.finish()),
                Arc::new(dropped_attributes_count.finish()),
                Arc::new(dropped_events_count.finish()),
                Arc::new(dropped_links_count.finish()),
            ];
            RecordBatch::try_new(schema, columns).expect("schema mismatch in span batch")
        };

        debug!(
            spans = total_spans,
            events = total_events,
            links = total_links,
            "otlp trace export"
        );

        // Write spans, events, and links concurrently.
        let span_fut = self.write_to_table(schema::SPANS_TABLE, vec![span_batch]);

        let event_fut = async {
            if total_events > 0 {
                let schema = schema::span_event_schema_ref().clone();
                let columns: Vec<ArrayRef> = vec![
                    Arc::new(evt_trace_id.finish()),
                    Arc::new(evt_span_id.finish()),
                    Arc::new(evt_timestamp.finish()),
                    Arc::new(evt_name.finish()),
                    Arc::new(evt_attributes.finish()),
                    Arc::new(evt_dropped_attrs.finish()),
                ];
                let batch = RecordBatch::try_new(schema, columns)
                    .expect("schema mismatch in span event batch");
                self.write_to_table(schema::SPAN_EVENTS_TABLE, vec![batch])
                    .await
            } else {
                Ok(())
            }
        };

        let link_fut = async {
            if total_links > 0 {
                let schema = schema::span_link_schema_ref().clone();
                let columns: Vec<ArrayRef> = vec![
                    Arc::new(lnk_trace_id.finish()),
                    Arc::new(lnk_span_id.finish()),
                    Arc::new(lnk_linked_trace_id.finish()),
                    Arc::new(lnk_linked_span_id.finish()),
                    Arc::new(lnk_trace_state.finish()),
                    Arc::new(lnk_attributes.finish()),
                    Arc::new(lnk_flags.finish()),
                    Arc::new(lnk_dropped_attrs.finish()),
                ];
                let batch = RecordBatch::try_new(schema, columns)
                    .expect("schema mismatch in span link batch");
                self.write_to_table(schema::SPAN_LINKS_TABLE, vec![batch])
                    .await
            } else {
                Ok(())
            }
        };

        let (span_res, event_res, link_res) = tokio::join!(span_fut, event_fut, link_fut);

        let mut errors = Vec::new();
        if let Err(e) = span_res {
            errors.push(format!("spans: {e}"));
        }
        if let Err(e) = event_res {
            errors.push(format!("span events: {e}"));
        }
        if let Err(e) = link_res {
            errors.push(format!("span links: {e}"));
        }

        if !errors.is_empty() {
            return Err(Status::internal(errors.join("; ")));
        }

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

//! OTLP MetricsService implementation.
//!
//! Receives OTEL metric data via gRPC, decomposes by metric type
//! (Sum → counters, Gauge → gauges, Histogram → histograms), converts
//! to Arrow RecordBatches, and writes through Raft consensus.

use std::sync::Arc;

use arrow_array::builder::{
    Float64Builder, ListBuilder, StringBuilder, TimestampNanosecondBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::metrics::v1::metric;
use opentelemetry_proto::tonic::metrics::v1::number_data_point;
use tonic::{Request, Response, Status};
use tracing::debug;

use super::convert::key_values_to_json;
use super::schema;
use super::OtlpReceiver;

/// Arrow builders for numeric metric data points (counters and gauges).
struct NumberBuilders {
    metric_name: StringBuilder,
    attributes: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    timestamp: TimestampNanosecondBuilder,
    value: Float64Builder,
    count: usize,
}

impl NumberBuilders {
    fn with_capacity(n: usize) -> Self {
        Self {
            metric_name: StringBuilder::with_capacity(n, n * 32),
            attributes: StringBuilder::with_capacity(n, n * 64),
            resource_attributes: StringBuilder::with_capacity(n, n * 64),
            scope_name: StringBuilder::with_capacity(n, n * 32),
            scope_version: StringBuilder::with_capacity(n, n * 16),
            timestamp: TimestampNanosecondBuilder::with_capacity(n),
            value: Float64Builder::with_capacity(n),
            count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn finish(mut self, table_schema: Arc<arrow_schema::Schema>) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.metric_name.finish()),
            Arc::new(self.attributes.finish()),
            Arc::new(self.resource_attributes.finish()),
            Arc::new(self.scope_name.finish()),
            Arc::new(self.scope_version.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.value.finish()),
        ];
        RecordBatch::try_new(table_schema, columns)
            .expect("schema mismatch in metric number batch")
    }
}

/// Arrow builders for histogram metric data points.
struct HistogramBuilders {
    metric_name: StringBuilder,
    attributes: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    timestamp: TimestampNanosecondBuilder,
    boundaries: ListBuilder<Float64Builder>,
    bucket_counts: ListBuilder<UInt64Builder>,
    sum: Float64Builder,
    count_col: UInt64Builder,
    count: usize,
}

impl HistogramBuilders {
    fn with_capacity(n: usize) -> Self {
        Self {
            metric_name: StringBuilder::with_capacity(n, n * 32),
            attributes: StringBuilder::with_capacity(n, n * 64),
            resource_attributes: StringBuilder::with_capacity(n, n * 64),
            scope_name: StringBuilder::with_capacity(n, n * 32),
            scope_version: StringBuilder::with_capacity(n, n * 16),
            timestamp: TimestampNanosecondBuilder::with_capacity(n),
            boundaries: ListBuilder::new(Float64Builder::new()),
            bucket_counts: ListBuilder::new(UInt64Builder::new()),
            sum: Float64Builder::with_capacity(n),
            count_col: UInt64Builder::with_capacity(n),
            count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn finish(mut self, table_schema: Arc<arrow_schema::Schema>) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.metric_name.finish()),
            Arc::new(self.attributes.finish()),
            Arc::new(self.resource_attributes.finish()),
            Arc::new(self.scope_name.finish()),
            Arc::new(self.scope_version.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.boundaries.finish()),
            Arc::new(self.bucket_counts.finish()),
            Arc::new(self.sum.finish()),
            Arc::new(self.count_col.finish()),
        ];
        RecordBatch::try_new(table_schema, columns)
            .expect("schema mismatch in histogram batch")
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpReceiver {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let req = request.into_inner();

        // Pre-count data points per metric type for builder capacity.
        let mut num_counter = 0usize;
        let mut num_gauge = 0usize;
        let mut num_histogram = 0usize;

        for rm in &req.resource_metrics {
            for sm in &rm.scope_metrics {
                for m in &sm.metrics {
                    match &m.data {
                        Some(metric::Data::Sum(sum)) => num_counter += sum.data_points.len(),
                        Some(metric::Data::Gauge(gauge)) => num_gauge += gauge.data_points.len(),
                        Some(metric::Data::Histogram(hist)) => {
                            num_histogram += hist.data_points.len()
                        }
                        _ => {}
                    }
                }
            }
        }

        let total = num_counter + num_gauge + num_histogram;
        if total == 0 {
            return Ok(Response::new(ExportMetricsServiceResponse {
                partial_success: None,
            }));
        }

        // Build Arrow arrays directly with pre-allocated capacity.
        let mut counters = NumberBuilders::with_capacity(num_counter);
        let mut gauges = NumberBuilders::with_capacity(num_gauge);
        let mut histograms = HistogramBuilders::with_capacity(num_histogram);

        for rm in &req.resource_metrics {
            let resource_attrs_json = rm
                .resource
                .as_ref()
                .map(|r| key_values_to_json(&r.attributes))
                .unwrap_or_else(|| "{}".to_string());

            for sm in &rm.scope_metrics {
                let (scope_name, scope_version) = sm
                    .scope
                    .as_ref()
                    .map(|s| (s.name.as_str(), s.version.as_str()))
                    .unwrap_or(("", ""));

                for metric in &sm.metrics {
                    match &metric.data {
                        Some(metric::Data::Sum(sum)) => {
                            for dp in &sum.data_points {
                                append_number_point(
                                    &mut counters,
                                    &metric.name,
                                    &resource_attrs_json,
                                    scope_name,
                                    scope_version,
                                    dp,
                                );
                            }
                        }
                        Some(metric::Data::Gauge(gauge)) => {
                            for dp in &gauge.data_points {
                                append_number_point(
                                    &mut gauges,
                                    &metric.name,
                                    &resource_attrs_json,
                                    scope_name,
                                    scope_version,
                                    dp,
                                );
                            }
                        }
                        Some(metric::Data::Histogram(hist)) => {
                            for dp in &hist.data_points {
                                histograms.metric_name.append_value(&metric.name);
                                histograms
                                    .attributes
                                    .append_value(key_values_to_json(&dp.attributes));
                                histograms
                                    .resource_attributes
                                    .append_value(&resource_attrs_json);
                                histograms.scope_name.append_value(scope_name);
                                histograms.scope_version.append_value(scope_version);
                                histograms
                                    .timestamp
                                    .append_value(dp.time_unix_nano as i64);

                                let b_inner = histograms.boundaries.values();
                                for &b in &dp.explicit_bounds {
                                    b_inner.append_value(b);
                                }
                                histograms.boundaries.append(true);

                                let bc_inner = histograms.bucket_counts.values();
                                for &c in &dp.bucket_counts {
                                    bc_inner.append_value(c);
                                }
                                histograms.bucket_counts.append(true);

                                histograms.sum.append_value(dp.sum.unwrap_or(0.0));
                                histograms.count_col.append_value(dp.count);
                                histograms.count += 1;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        debug!(
            counters = counters.count,
            gauges = gauges.count,
            histograms = histograms.count,
            "otlp metrics export"
        );

        // Write all metric types concurrently using cached schemas.
        let counter_fut = async {
            if !counters.is_empty() {
                let batch = counters.finish(schema::counter_schema_ref().clone());
                self.write_to_table(schema::COUNTERS_TABLE, vec![batch])
                    .await
            } else {
                Ok(())
            }
        };

        let gauge_fut = async {
            if !gauges.is_empty() {
                let batch = gauges.finish(schema::counter_schema_ref().clone());
                self.write_to_table(schema::GAUGES_TABLE, vec![batch])
                    .await
            } else {
                Ok(())
            }
        };

        let hist_fut = async {
            if !histograms.is_empty() {
                let batch = histograms.finish(schema::histogram_schema_ref().clone());
                self.write_to_table(schema::HISTOGRAMS_TABLE, vec![batch])
                    .await
            } else {
                Ok(())
            }
        };

        let (counter_res, gauge_res, hist_res) =
            tokio::join!(counter_fut, gauge_fut, hist_fut);

        let mut errors = Vec::new();
        if let Err(e) = counter_res {
            errors.push(format!("counters: {e}"));
        }
        if let Err(e) = gauge_res {
            errors.push(format!("gauges: {e}"));
        }
        if let Err(e) = hist_res {
            errors.push(format!("histograms: {e}"));
        }

        if errors.is_empty() {
            Ok(Response::new(ExportMetricsServiceResponse {
                partial_success: None,
            }))
        } else {
            Ok(Response::new(ExportMetricsServiceResponse {
                partial_success: Some(
                    opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsPartialSuccess {
                        rejected_data_points: total as i64,
                        error_message: errors.join("; "),
                    },
                ),
            }))
        }
    }
}

/// Append a single numeric data point directly to Arrow builders.
fn append_number_point(
    builders: &mut NumberBuilders,
    metric_name: &str,
    resource_attrs_json: &str,
    scope_name: &str,
    scope_version: &str,
    dp: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
) {
    let value = match &dp.value {
        Some(number_data_point::Value::AsDouble(d)) => *d,
        Some(number_data_point::Value::AsInt(i)) => *i as f64,
        None => 0.0,
    };

    builders.metric_name.append_value(metric_name);
    builders
        .attributes
        .append_value(key_values_to_json(&dp.attributes));
    builders
        .resource_attributes
        .append_value(resource_attrs_json);
    builders.scope_name.append_value(scope_name);
    builders.scope_version.append_value(scope_version);
    builders.timestamp.append_value(dp.time_unix_nano as i64);
    builders.value.append_value(value);
    builders.count += 1;
}

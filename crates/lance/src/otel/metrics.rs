//! OTLP MetricsService implementation.
//!
//! Receives OTEL metric data via gRPC, decomposes by metric type
//! (Sum → counters, Gauge → gauges, Histogram → histograms,
//! ExponentialHistogram → exp_histograms, Summary → summaries),
//! converts to Arrow RecordBatches, and writes through Raft consensus.
//! Exemplars are extracted to a separate `otel_exemplars` table.

use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int32Builder, Int64Builder,
    ListBuilder, StringBuilder, TimestampNanosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::metrics::v1::{Exemplar, metric, number_data_point};
use tonic::{Request, Response, Status};
use tracing::debug;

use super::OtlpReceiver;
use super::convert::{key_values_to_json, pad_or_truncate};
use super::schema;

// ---------------------------------------------------------------------------
// Shared resource/scope context extracted once per ResourceMetrics/ScopeMetrics
// ---------------------------------------------------------------------------

struct ResourceContext {
    attributes_json: String,
    dropped_attributes_count: u32,
    schema_url: String,
}

struct ScopeContext<'a> {
    name: &'a str,
    version: &'a str,
    attributes_json: String,
    dropped_attributes_count: u32,
    schema_url: &'a str,
}

// ---------------------------------------------------------------------------
// Counter builders (Sum metric type)
// ---------------------------------------------------------------------------

/// Arrow builders for Sum (counter) data points.
/// Column order matches `schema::counter_schema()`.
struct CounterBuilders {
    // Utf8 key columns
    metric_name: StringBuilder,
    description: StringBuilder,
    unit: StringBuilder,
    metadata: StringBuilder,
    attributes: StringBuilder,
    resource_attributes: StringBuilder,
    resource_schema_url: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    scope_schema_url: StringBuilder,
    // Timestamps
    timestamp: TimestampNanosecondBuilder,
    start_time: TimestampNanosecondBuilder,
    // Value — two nullable columns for int vs double fidelity
    value_int: Int64Builder,
    value_double: Float64Builder,
    // Passthrough (non-Utf8 metadata)
    aggregation_temporality: Int32Builder,
    is_monotonic: BooleanBuilder,
    flags: UInt32Builder,
    resource_dropped_attributes_count: UInt32Builder,
    scope_dropped_attributes_count: UInt32Builder,
    count: usize,
}

impl CounterBuilders {
    fn with_capacity(n: usize) -> Self {
        Self {
            metric_name: StringBuilder::with_capacity(n, n * 32),
            description: StringBuilder::with_capacity(n, n * 64),
            unit: StringBuilder::with_capacity(n, n * 8),
            metadata: StringBuilder::with_capacity(n, n * 32),
            attributes: StringBuilder::with_capacity(n, n * 64),
            resource_attributes: StringBuilder::with_capacity(n, n * 64),
            resource_schema_url: StringBuilder::with_capacity(n, n * 32),
            scope_name: StringBuilder::with_capacity(n, n * 32),
            scope_version: StringBuilder::with_capacity(n, n * 16),
            scope_attributes: StringBuilder::with_capacity(n, n * 64),
            scope_schema_url: StringBuilder::with_capacity(n, n * 32),
            timestamp: TimestampNanosecondBuilder::with_capacity(n),
            start_time: TimestampNanosecondBuilder::with_capacity(n),
            value_int: Int64Builder::with_capacity(n),
            value_double: Float64Builder::with_capacity(n),
            aggregation_temporality: Int32Builder::with_capacity(n),
            is_monotonic: BooleanBuilder::with_capacity(n),
            flags: UInt32Builder::with_capacity(n),
            resource_dropped_attributes_count: UInt32Builder::with_capacity(n),
            scope_dropped_attributes_count: UInt32Builder::with_capacity(n),
            count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn finish(mut self, table_schema: Arc<arrow_schema::Schema>) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.metric_name.finish()),
            Arc::new(self.description.finish()),
            Arc::new(self.unit.finish()),
            Arc::new(self.metadata.finish()),
            Arc::new(self.attributes.finish()),
            Arc::new(self.resource_attributes.finish()),
            Arc::new(self.resource_schema_url.finish()),
            Arc::new(self.scope_name.finish()),
            Arc::new(self.scope_version.finish()),
            Arc::new(self.scope_attributes.finish()),
            Arc::new(self.scope_schema_url.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.start_time.finish()),
            Arc::new(self.value_int.finish()),
            Arc::new(self.value_double.finish()),
            Arc::new(self.aggregation_temporality.finish()),
            Arc::new(self.is_monotonic.finish()),
            Arc::new(self.flags.finish()),
            Arc::new(self.resource_dropped_attributes_count.finish()),
            Arc::new(self.scope_dropped_attributes_count.finish()),
        ];
        RecordBatch::try_new(table_schema, columns).expect("schema mismatch in counter batch")
    }
}

// ---------------------------------------------------------------------------
// Gauge builders (Gauge metric type — no aggregation_temporality/is_monotonic)
// ---------------------------------------------------------------------------

/// Arrow builders for Gauge data points.
/// Column order matches `schema::gauge_schema()`.
struct GaugeBuilders {
    // Utf8 key columns
    metric_name: StringBuilder,
    description: StringBuilder,
    unit: StringBuilder,
    metadata: StringBuilder,
    attributes: StringBuilder,
    resource_attributes: StringBuilder,
    resource_schema_url: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    scope_schema_url: StringBuilder,
    // Timestamps
    timestamp: TimestampNanosecondBuilder,
    start_time: TimestampNanosecondBuilder,
    // Value — two nullable columns for int vs double fidelity
    value_int: Int64Builder,
    value_double: Float64Builder,
    // Passthrough (no agg_temp/is_monotonic for Gauge per OTLP spec)
    flags: UInt32Builder,
    resource_dropped_attributes_count: UInt32Builder,
    scope_dropped_attributes_count: UInt32Builder,
    count: usize,
}

impl GaugeBuilders {
    fn with_capacity(n: usize) -> Self {
        Self {
            metric_name: StringBuilder::with_capacity(n, n * 32),
            description: StringBuilder::with_capacity(n, n * 64),
            unit: StringBuilder::with_capacity(n, n * 8),
            metadata: StringBuilder::with_capacity(n, n * 32),
            attributes: StringBuilder::with_capacity(n, n * 64),
            resource_attributes: StringBuilder::with_capacity(n, n * 64),
            resource_schema_url: StringBuilder::with_capacity(n, n * 32),
            scope_name: StringBuilder::with_capacity(n, n * 32),
            scope_version: StringBuilder::with_capacity(n, n * 16),
            scope_attributes: StringBuilder::with_capacity(n, n * 64),
            scope_schema_url: StringBuilder::with_capacity(n, n * 32),
            timestamp: TimestampNanosecondBuilder::with_capacity(n),
            start_time: TimestampNanosecondBuilder::with_capacity(n),
            value_int: Int64Builder::with_capacity(n),
            value_double: Float64Builder::with_capacity(n),
            flags: UInt32Builder::with_capacity(n),
            resource_dropped_attributes_count: UInt32Builder::with_capacity(n),
            scope_dropped_attributes_count: UInt32Builder::with_capacity(n),
            count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn finish(mut self, table_schema: Arc<arrow_schema::Schema>) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.metric_name.finish()),
            Arc::new(self.description.finish()),
            Arc::new(self.unit.finish()),
            Arc::new(self.metadata.finish()),
            Arc::new(self.attributes.finish()),
            Arc::new(self.resource_attributes.finish()),
            Arc::new(self.resource_schema_url.finish()),
            Arc::new(self.scope_name.finish()),
            Arc::new(self.scope_version.finish()),
            Arc::new(self.scope_attributes.finish()),
            Arc::new(self.scope_schema_url.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.start_time.finish()),
            Arc::new(self.value_int.finish()),
            Arc::new(self.value_double.finish()),
            Arc::new(self.flags.finish()),
            Arc::new(self.resource_dropped_attributes_count.finish()),
            Arc::new(self.scope_dropped_attributes_count.finish()),
        ];
        RecordBatch::try_new(table_schema, columns).expect("schema mismatch in gauge batch")
    }
}

// ---------------------------------------------------------------------------
// Histogram builders
// ---------------------------------------------------------------------------

struct HistogramBuilders {
    // Utf8 key columns
    metric_name: StringBuilder,
    description: StringBuilder,
    unit: StringBuilder,
    metadata: StringBuilder,
    attributes: StringBuilder,
    resource_attributes: StringBuilder,
    resource_schema_url: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    scope_schema_url: StringBuilder,
    // Timestamps
    timestamp: TimestampNanosecondBuilder,
    start_time: TimestampNanosecondBuilder,
    // Histogram data
    boundaries: ListBuilder<Float64Builder>,
    bucket_counts: ListBuilder<UInt64Builder>,
    sum: Float64Builder, // nullable
    count_col: UInt64Builder,
    min: Float64Builder, // nullable
    max: Float64Builder, // nullable
    // Passthrough
    aggregation_temporality: Int32Builder,
    is_monotonic: BooleanBuilder,
    flags: UInt32Builder,
    resource_dropped_attributes_count: UInt32Builder,
    scope_dropped_attributes_count: UInt32Builder,
    count: usize,
}

impl HistogramBuilders {
    fn with_capacity(n: usize) -> Self {
        Self {
            metric_name: StringBuilder::with_capacity(n, n * 32),
            description: StringBuilder::with_capacity(n, n * 64),
            unit: StringBuilder::with_capacity(n, n * 8),
            metadata: StringBuilder::with_capacity(n, n * 32),
            attributes: StringBuilder::with_capacity(n, n * 64),
            resource_attributes: StringBuilder::with_capacity(n, n * 64),
            resource_schema_url: StringBuilder::with_capacity(n, n * 32),
            scope_name: StringBuilder::with_capacity(n, n * 32),
            scope_version: StringBuilder::with_capacity(n, n * 16),
            scope_attributes: StringBuilder::with_capacity(n, n * 64),
            scope_schema_url: StringBuilder::with_capacity(n, n * 32),
            timestamp: TimestampNanosecondBuilder::with_capacity(n),
            start_time: TimestampNanosecondBuilder::with_capacity(n),
            boundaries: ListBuilder::new(Float64Builder::new()),
            bucket_counts: ListBuilder::new(UInt64Builder::new()),
            sum: Float64Builder::with_capacity(n),
            count_col: UInt64Builder::with_capacity(n),
            min: Float64Builder::with_capacity(n),
            max: Float64Builder::with_capacity(n),
            aggregation_temporality: Int32Builder::with_capacity(n),
            is_monotonic: BooleanBuilder::with_capacity(n),
            flags: UInt32Builder::with_capacity(n),
            resource_dropped_attributes_count: UInt32Builder::with_capacity(n),
            scope_dropped_attributes_count: UInt32Builder::with_capacity(n),
            count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn finish(mut self, table_schema: Arc<arrow_schema::Schema>) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.metric_name.finish()),
            Arc::new(self.description.finish()),
            Arc::new(self.unit.finish()),
            Arc::new(self.metadata.finish()),
            Arc::new(self.attributes.finish()),
            Arc::new(self.resource_attributes.finish()),
            Arc::new(self.resource_schema_url.finish()),
            Arc::new(self.scope_name.finish()),
            Arc::new(self.scope_version.finish()),
            Arc::new(self.scope_attributes.finish()),
            Arc::new(self.scope_schema_url.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.start_time.finish()),
            Arc::new(self.boundaries.finish()),
            Arc::new(self.bucket_counts.finish()),
            Arc::new(self.sum.finish()),
            Arc::new(self.count_col.finish()),
            Arc::new(self.min.finish()),
            Arc::new(self.max.finish()),
            Arc::new(self.aggregation_temporality.finish()),
            Arc::new(self.is_monotonic.finish()),
            Arc::new(self.flags.finish()),
            Arc::new(self.resource_dropped_attributes_count.finish()),
            Arc::new(self.scope_dropped_attributes_count.finish()),
        ];
        RecordBatch::try_new(table_schema, columns).expect("schema mismatch in histogram batch")
    }
}

// ---------------------------------------------------------------------------
// Exponential histogram builders
// ---------------------------------------------------------------------------

struct ExpHistogramBuilders {
    // Utf8 key columns
    metric_name: StringBuilder,
    description: StringBuilder,
    unit: StringBuilder,
    metadata: StringBuilder,
    attributes: StringBuilder,
    resource_attributes: StringBuilder,
    resource_schema_url: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    scope_schema_url: StringBuilder,
    // Timestamps
    timestamp: TimestampNanosecondBuilder,
    start_time: TimestampNanosecondBuilder,
    // Exp histogram data
    scale: Int32Builder,
    zero_count: UInt64Builder,
    zero_threshold: Float64Builder,
    positive_offset: Int32Builder,
    positive_bucket_counts: ListBuilder<UInt64Builder>,
    negative_offset: Int32Builder,
    negative_bucket_counts: ListBuilder<UInt64Builder>,
    sum: Float64Builder, // nullable
    count_col: UInt64Builder,
    min: Float64Builder, // nullable
    max: Float64Builder, // nullable
    // Passthrough
    aggregation_temporality: Int32Builder,
    is_monotonic: BooleanBuilder,
    flags: UInt32Builder,
    resource_dropped_attributes_count: UInt32Builder,
    scope_dropped_attributes_count: UInt32Builder,
    count: usize,
}

impl ExpHistogramBuilders {
    fn with_capacity(n: usize) -> Self {
        Self {
            metric_name: StringBuilder::with_capacity(n, n * 32),
            description: StringBuilder::with_capacity(n, n * 64),
            unit: StringBuilder::with_capacity(n, n * 8),
            metadata: StringBuilder::with_capacity(n, n * 32),
            attributes: StringBuilder::with_capacity(n, n * 64),
            resource_attributes: StringBuilder::with_capacity(n, n * 64),
            resource_schema_url: StringBuilder::with_capacity(n, n * 32),
            scope_name: StringBuilder::with_capacity(n, n * 32),
            scope_version: StringBuilder::with_capacity(n, n * 16),
            scope_attributes: StringBuilder::with_capacity(n, n * 64),
            scope_schema_url: StringBuilder::with_capacity(n, n * 32),
            timestamp: TimestampNanosecondBuilder::with_capacity(n),
            start_time: TimestampNanosecondBuilder::with_capacity(n),
            scale: Int32Builder::with_capacity(n),
            zero_count: UInt64Builder::with_capacity(n),
            zero_threshold: Float64Builder::with_capacity(n),
            positive_offset: Int32Builder::with_capacity(n),
            positive_bucket_counts: ListBuilder::new(UInt64Builder::new()),
            negative_offset: Int32Builder::with_capacity(n),
            negative_bucket_counts: ListBuilder::new(UInt64Builder::new()),
            sum: Float64Builder::with_capacity(n),
            count_col: UInt64Builder::with_capacity(n),
            min: Float64Builder::with_capacity(n),
            max: Float64Builder::with_capacity(n),
            aggregation_temporality: Int32Builder::with_capacity(n),
            is_monotonic: BooleanBuilder::with_capacity(n),
            flags: UInt32Builder::with_capacity(n),
            resource_dropped_attributes_count: UInt32Builder::with_capacity(n),
            scope_dropped_attributes_count: UInt32Builder::with_capacity(n),
            count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn finish(mut self, table_schema: Arc<arrow_schema::Schema>) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.metric_name.finish()),
            Arc::new(self.description.finish()),
            Arc::new(self.unit.finish()),
            Arc::new(self.metadata.finish()),
            Arc::new(self.attributes.finish()),
            Arc::new(self.resource_attributes.finish()),
            Arc::new(self.resource_schema_url.finish()),
            Arc::new(self.scope_name.finish()),
            Arc::new(self.scope_version.finish()),
            Arc::new(self.scope_attributes.finish()),
            Arc::new(self.scope_schema_url.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.start_time.finish()),
            Arc::new(self.scale.finish()),
            Arc::new(self.zero_count.finish()),
            Arc::new(self.zero_threshold.finish()),
            Arc::new(self.positive_offset.finish()),
            Arc::new(self.positive_bucket_counts.finish()),
            Arc::new(self.negative_offset.finish()),
            Arc::new(self.negative_bucket_counts.finish()),
            Arc::new(self.sum.finish()),
            Arc::new(self.count_col.finish()),
            Arc::new(self.min.finish()),
            Arc::new(self.max.finish()),
            Arc::new(self.aggregation_temporality.finish()),
            Arc::new(self.is_monotonic.finish()),
            Arc::new(self.flags.finish()),
            Arc::new(self.resource_dropped_attributes_count.finish()),
            Arc::new(self.scope_dropped_attributes_count.finish()),
        ];
        RecordBatch::try_new(table_schema, columns).expect("schema mismatch in exp histogram batch")
    }
}

// ---------------------------------------------------------------------------
// Exemplar builders
// ---------------------------------------------------------------------------

struct ExemplarBuilders {
    metric_name: StringBuilder,
    metric_table: StringBuilder,
    attributes: StringBuilder,
    timestamp: TimestampNanosecondBuilder,
    trace_id: FixedSizeBinaryBuilder,
    span_id: FixedSizeBinaryBuilder,
    value_int: Int64Builder,
    value_double: Float64Builder,
    count: usize,
}

impl ExemplarBuilders {
    fn with_capacity(n: usize) -> Self {
        Self {
            metric_name: StringBuilder::with_capacity(n, n * 32),
            metric_table: StringBuilder::with_capacity(n, n * 16),
            attributes: StringBuilder::with_capacity(n, n * 64),
            timestamp: TimestampNanosecondBuilder::with_capacity(n),
            trace_id: FixedSizeBinaryBuilder::with_capacity(n, 16),
            span_id: FixedSizeBinaryBuilder::with_capacity(n, 8),
            value_int: Int64Builder::with_capacity(n),
            value_double: Float64Builder::with_capacity(n),
            count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn append(&mut self, metric_name: &str, metric_table: &str, exemplar: &Exemplar) {
        use opentelemetry_proto::tonic::metrics::v1::exemplar;

        self.metric_name.append_value(metric_name);
        self.metric_table.append_value(metric_table);
        self.attributes
            .append_value(key_values_to_json(&exemplar.filtered_attributes));
        self.timestamp.append_value(exemplar.time_unix_nano as i64);

        let tid = pad_or_truncate(&exemplar.trace_id, 16);
        self.trace_id
            .append_value(&tid)
            .expect("trace_id must be 16 bytes");

        let sid = pad_or_truncate(&exemplar.span_id, 8);
        self.span_id
            .append_value(&sid)
            .expect("span_id must be 8 bytes");

        match &exemplar.value {
            Some(exemplar::Value::AsInt(i)) => {
                self.value_int.append_value(*i);
                self.value_double.append_null();
            }
            Some(exemplar::Value::AsDouble(d)) => {
                self.value_int.append_null();
                self.value_double.append_value(*d);
            }
            None => {
                self.value_int.append_null();
                self.value_double.append_null();
            }
        }
        self.count += 1;
    }

    fn finish(mut self, table_schema: Arc<arrow_schema::Schema>) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.metric_name.finish()),
            Arc::new(self.metric_table.finish()),
            Arc::new(self.attributes.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.trace_id.finish()),
            Arc::new(self.span_id.finish()),
            Arc::new(self.value_int.finish()),
            Arc::new(self.value_double.finish()),
        ];
        RecordBatch::try_new(table_schema, columns).expect("schema mismatch in exemplar batch")
    }
}

// ---------------------------------------------------------------------------
// Shared helpers for appending context columns
// ---------------------------------------------------------------------------

/// Append the 11 Utf8 key columns + resource/scope context to any metric builder.
macro_rules! append_metric_key_cols {
    ($builders:expr, $metric:expr, $metadata_json:expr, $dp_attrs:expr, $res:expr, $scope:expr) => {
        $builders.metric_name.append_value(&$metric.name);
        $builders.description.append_value(&$metric.description);
        $builders.unit.append_value(&$metric.unit);
        $builders.metadata.append_value($metadata_json);
        $builders.attributes.append_value($dp_attrs);
        $builders
            .resource_attributes
            .append_value(&$res.attributes_json);
        $builders.resource_schema_url.append_value(&$res.schema_url);
        $builders.scope_name.append_value($scope.name);
        $builders.scope_version.append_value($scope.version);
        $builders
            .scope_attributes
            .append_value(&$scope.attributes_json);
        $builders.scope_schema_url.append_value($scope.schema_url);
    };
}

/// Append the 5 non-Utf8 passthrough columns for counter/histogram/exp-histogram.
macro_rules! append_counter_passthrough {
    ($builders:expr, $agg_temp:expr, $is_monotonic:expr, $flags:expr, $res:expr, $scope:expr) => {
        $builders.aggregation_temporality.append_value($agg_temp);
        $builders.is_monotonic.append_value($is_monotonic);
        $builders.flags.append_value($flags);
        $builders
            .resource_dropped_attributes_count
            .append_value($res.dropped_attributes_count);
        $builders
            .scope_dropped_attributes_count
            .append_value($scope.dropped_attributes_count);
    };
}

/// Append the 3 non-Utf8 passthrough columns for gauge (no agg_temp/is_monotonic).
macro_rules! append_gauge_passthrough {
    ($builders:expr, $flags:expr, $res:expr, $scope:expr) => {
        $builders.flags.append_value($flags);
        $builders
            .resource_dropped_attributes_count
            .append_value($res.dropped_attributes_count);
        $builders
            .scope_dropped_attributes_count
            .append_value($scope.dropped_attributes_count);
    };
}

/// Append a NumberDataPoint value as two nullable columns (value_int, value_double).
macro_rules! append_number_value {
    ($builders:expr, $dp_value:expr) => {
        match &$dp_value {
            Some(number_data_point::Value::AsInt(i)) => {
                $builders.value_int.append_value(*i);
                $builders.value_double.append_null();
            }
            Some(number_data_point::Value::AsDouble(d)) => {
                $builders.value_int.append_null();
                $builders.value_double.append_value(*d);
            }
            None => {
                $builders.value_int.append_null();
                $builders.value_double.append_null();
            }
        }
    };
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
        let mut num_exp_histogram = 0usize;
        let mut num_summary = 0usize;
        let mut num_exemplars = 0usize;

        for rm in &req.resource_metrics {
            for sm in &rm.scope_metrics {
                for m in &sm.metrics {
                    match &m.data {
                        Some(metric::Data::Sum(sum)) => {
                            num_counter += sum.data_points.len();
                            for dp in &sum.data_points {
                                num_exemplars += dp.exemplars.len();
                            }
                        }
                        Some(metric::Data::Gauge(gauge)) => {
                            num_gauge += gauge.data_points.len();
                            for dp in &gauge.data_points {
                                num_exemplars += dp.exemplars.len();
                            }
                        }
                        Some(metric::Data::Histogram(hist)) => {
                            num_histogram += hist.data_points.len();
                            for dp in &hist.data_points {
                                num_exemplars += dp.exemplars.len();
                            }
                        }
                        Some(metric::Data::ExponentialHistogram(eh)) => {
                            num_exp_histogram += eh.data_points.len();
                            for dp in &eh.data_points {
                                num_exemplars += dp.exemplars.len();
                            }
                        }
                        Some(metric::Data::Summary(s)) => {
                            num_summary += s.data_points.len();
                        }
                        _ => {}
                    }
                }
            }
        }

        let total = num_counter + num_gauge + num_histogram + num_exp_histogram + num_summary;
        if total == 0 {
            return Ok(Response::new(ExportMetricsServiceResponse {
                partial_success: None,
            }));
        }

        let mut counters = CounterBuilders::with_capacity(num_counter);
        let mut gauges = GaugeBuilders::with_capacity(num_gauge);
        let mut histograms = HistogramBuilders::with_capacity(num_histogram);
        let mut exp_histograms = ExpHistogramBuilders::with_capacity(num_exp_histogram);
        let mut summaries = SummaryBuildersSimple::with_capacity(num_summary);
        let mut exemplars = ExemplarBuilders::with_capacity(num_exemplars);

        for rm in &req.resource_metrics {
            let res_ctx = ResourceContext {
                attributes_json: rm
                    .resource
                    .as_ref()
                    .map(|r| key_values_to_json(&r.attributes))
                    .unwrap_or_else(|| "{}".to_string()),
                dropped_attributes_count: rm
                    .resource
                    .as_ref()
                    .map(|r| r.dropped_attributes_count)
                    .unwrap_or(0),
                schema_url: rm.schema_url.clone(),
            };

            for sm in &rm.scope_metrics {
                let scope_ctx = ScopeContext {
                    name: sm.scope.as_ref().map(|s| s.name.as_str()).unwrap_or(""),
                    version: sm.scope.as_ref().map(|s| s.version.as_str()).unwrap_or(""),
                    attributes_json: sm
                        .scope
                        .as_ref()
                        .map(|s| key_values_to_json(&s.attributes))
                        .unwrap_or_else(|| "{}".to_string()),
                    dropped_attributes_count: sm
                        .scope
                        .as_ref()
                        .map(|s| s.dropped_attributes_count)
                        .unwrap_or(0),
                    schema_url: &sm.schema_url,
                };

                for metric in &sm.metrics {
                    let metadata_json = key_values_to_json(&metric.metadata);

                    match &metric.data {
                        Some(metric::Data::Sum(sum)) => {
                            let agg_temp = sum.aggregation_temporality;
                            let is_mono = sum.is_monotonic;
                            for dp in &sum.data_points {
                                let dp_attrs = key_values_to_json(&dp.attributes);
                                append_metric_key_cols!(
                                    counters,
                                    metric,
                                    &metadata_json,
                                    &dp_attrs,
                                    res_ctx,
                                    scope_ctx
                                );
                                counters.timestamp.append_value(dp.time_unix_nano as i64);
                                counters
                                    .start_time
                                    .append_value(dp.start_time_unix_nano as i64);
                                append_number_value!(counters, dp.value);
                                append_counter_passthrough!(
                                    counters, agg_temp, is_mono, dp.flags, res_ctx, scope_ctx
                                );
                                counters.count += 1;

                                for ex in &dp.exemplars {
                                    exemplars.append(&metric.name, schema::COUNTERS_TABLE, ex);
                                }
                            }
                        }
                        Some(metric::Data::Gauge(gauge)) => {
                            for dp in &gauge.data_points {
                                let dp_attrs = key_values_to_json(&dp.attributes);
                                append_metric_key_cols!(
                                    gauges,
                                    metric,
                                    &metadata_json,
                                    &dp_attrs,
                                    res_ctx,
                                    scope_ctx
                                );
                                gauges.timestamp.append_value(dp.time_unix_nano as i64);
                                gauges
                                    .start_time
                                    .append_value(dp.start_time_unix_nano as i64);
                                append_number_value!(gauges, dp.value);
                                append_gauge_passthrough!(gauges, dp.flags, res_ctx, scope_ctx);
                                gauges.count += 1;

                                for ex in &dp.exemplars {
                                    exemplars.append(&metric.name, schema::GAUGES_TABLE, ex);
                                }
                            }
                        }
                        Some(metric::Data::Histogram(hist)) => {
                            let agg_temp = hist.aggregation_temporality;
                            for dp in &hist.data_points {
                                let dp_attrs = key_values_to_json(&dp.attributes);
                                append_metric_key_cols!(
                                    histograms,
                                    metric,
                                    &metadata_json,
                                    &dp_attrs,
                                    res_ctx,
                                    scope_ctx
                                );
                                histograms.timestamp.append_value(dp.time_unix_nano as i64);
                                histograms
                                    .start_time
                                    .append_value(dp.start_time_unix_nano as i64);

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

                                // sum, min, max are optional per OTLP spec
                                histograms.sum.append_option(dp.sum);
                                histograms.count_col.append_value(dp.count);
                                histograms.min.append_option(dp.min);
                                histograms.max.append_option(dp.max);
                                append_counter_passthrough!(
                                    histograms, agg_temp, false, dp.flags, res_ctx, scope_ctx
                                );
                                histograms.count += 1;

                                for ex in &dp.exemplars {
                                    exemplars.append(&metric.name, schema::HISTOGRAMS_TABLE, ex);
                                }
                            }
                        }
                        Some(metric::Data::ExponentialHistogram(eh)) => {
                            let agg_temp = eh.aggregation_temporality;
                            for dp in &eh.data_points {
                                let dp_attrs = key_values_to_json(&dp.attributes);
                                append_metric_key_cols!(
                                    exp_histograms,
                                    metric,
                                    &metadata_json,
                                    &dp_attrs,
                                    res_ctx,
                                    scope_ctx
                                );
                                exp_histograms
                                    .timestamp
                                    .append_value(dp.time_unix_nano as i64);
                                exp_histograms
                                    .start_time
                                    .append_value(dp.start_time_unix_nano as i64);
                                exp_histograms.scale.append_value(dp.scale);
                                exp_histograms.zero_count.append_value(dp.zero_count);
                                exp_histograms
                                    .zero_threshold
                                    .append_value(dp.zero_threshold);

                                let (pos_offset, pos_counts) = dp
                                    .positive
                                    .as_ref()
                                    .map(|b| (b.offset, &b.bucket_counts[..]))
                                    .unwrap_or((0, &[]));
                                exp_histograms.positive_offset.append_value(pos_offset);
                                let pos_inner = exp_histograms.positive_bucket_counts.values();
                                for &c in pos_counts {
                                    pos_inner.append_value(c);
                                }
                                exp_histograms.positive_bucket_counts.append(true);

                                let (neg_offset, neg_counts) = dp
                                    .negative
                                    .as_ref()
                                    .map(|b| (b.offset, &b.bucket_counts[..]))
                                    .unwrap_or((0, &[]));
                                exp_histograms.negative_offset.append_value(neg_offset);
                                let neg_inner = exp_histograms.negative_bucket_counts.values();
                                for &c in neg_counts {
                                    neg_inner.append_value(c);
                                }
                                exp_histograms.negative_bucket_counts.append(true);

                                // sum, min, max are optional per OTLP spec
                                exp_histograms.sum.append_option(dp.sum);
                                exp_histograms.count_col.append_value(dp.count);
                                exp_histograms.min.append_option(dp.min);
                                exp_histograms.max.append_option(dp.max);
                                append_counter_passthrough!(
                                    exp_histograms,
                                    agg_temp,
                                    false,
                                    dp.flags,
                                    res_ctx,
                                    scope_ctx
                                );
                                exp_histograms.count += 1;

                                for ex in &dp.exemplars {
                                    exemplars.append(
                                        &metric.name,
                                        schema::EXP_HISTOGRAMS_TABLE,
                                        ex,
                                    );
                                }
                            }
                        }
                        Some(metric::Data::Summary(s)) => {
                            for dp in &s.data_points {
                                summaries.append(metric, &metadata_json, &res_ctx, &scope_ctx, dp);
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
            exp_histograms = exp_histograms.count,
            summaries = summaries.count,
            exemplars = exemplars.count,
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
                let batch = gauges.finish(schema::gauge_schema_ref().clone());
                self.write_to_table(schema::GAUGES_TABLE, vec![batch]).await
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

        let exp_hist_fut = async {
            if !exp_histograms.is_empty() {
                let batch = exp_histograms.finish(schema::exp_histogram_schema_ref().clone());
                self.write_to_table(schema::EXP_HISTOGRAMS_TABLE, vec![batch])
                    .await
            } else {
                Ok(())
            }
        };

        let summary_fut = async {
            if !summaries.is_empty() {
                let batch = summaries.finish(schema::summary_schema_ref().clone());
                self.write_to_table(schema::SUMMARIES_TABLE, vec![batch])
                    .await
            } else {
                Ok(())
            }
        };

        let exemplar_fut = async {
            if !exemplars.is_empty() {
                let batch = exemplars.finish(schema::exemplar_schema_ref().clone());
                self.write_to_table(schema::EXEMPLARS_TABLE, vec![batch])
                    .await
            } else {
                Ok(())
            }
        };

        let (counter_res, gauge_res, hist_res, exp_hist_res, summary_res, exemplar_res) = tokio::join!(
            counter_fut,
            gauge_fut,
            hist_fut,
            exp_hist_fut,
            summary_fut,
            exemplar_fut
        );

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
        if let Err(e) = exp_hist_res {
            errors.push(format!("exp_histograms: {e}"));
        }
        if let Err(e) = summary_res {
            errors.push(format!("summaries: {e}"));
        }
        if let Err(e) = exemplar_res {
            errors.push(format!("exemplars: {e}"));
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

// ---------------------------------------------------------------------------
// Summary builders — matches corrected summary_schema column order:
// identity(5) + resource(3) + scope(5) + timestamps(2) + data(3) + flags(1)
// ---------------------------------------------------------------------------

struct SummaryBuildersSimple {
    metric_name: StringBuilder,
    description: StringBuilder,
    unit: StringBuilder,
    metadata: StringBuilder,
    attributes: StringBuilder,
    resource_attributes: StringBuilder,
    resource_dropped_attributes_count: UInt32Builder,
    resource_schema_url: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    scope_dropped_attributes_count: UInt32Builder,
    scope_schema_url: StringBuilder,
    timestamp: TimestampNanosecondBuilder,
    start_time: TimestampNanosecondBuilder,
    count_col: UInt64Builder,
    sum: Float64Builder,
    quantile_values: StringBuilder,
    flags: UInt32Builder,
    count: usize,
}

impl SummaryBuildersSimple {
    fn with_capacity(n: usize) -> Self {
        Self {
            metric_name: StringBuilder::with_capacity(n, n * 32),
            description: StringBuilder::with_capacity(n, n * 64),
            unit: StringBuilder::with_capacity(n, n * 8),
            metadata: StringBuilder::with_capacity(n, n * 32),
            attributes: StringBuilder::with_capacity(n, n * 64),
            resource_attributes: StringBuilder::with_capacity(n, n * 64),
            resource_dropped_attributes_count: UInt32Builder::with_capacity(n),
            resource_schema_url: StringBuilder::with_capacity(n, n * 32),
            scope_name: StringBuilder::with_capacity(n, n * 32),
            scope_version: StringBuilder::with_capacity(n, n * 16),
            scope_attributes: StringBuilder::with_capacity(n, n * 64),
            scope_dropped_attributes_count: UInt32Builder::with_capacity(n),
            scope_schema_url: StringBuilder::with_capacity(n, n * 32),
            timestamp: TimestampNanosecondBuilder::with_capacity(n),
            start_time: TimestampNanosecondBuilder::with_capacity(n),
            count_col: UInt64Builder::with_capacity(n),
            sum: Float64Builder::with_capacity(n),
            quantile_values: StringBuilder::with_capacity(n, n * 128),
            flags: UInt32Builder::with_capacity(n),
            count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn append(
        &mut self,
        metric: &opentelemetry_proto::tonic::metrics::v1::Metric,
        metadata_json: &str,
        res: &ResourceContext,
        scope: &ScopeContext<'_>,
        dp: &opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint,
    ) {
        let dp_attrs = key_values_to_json(&dp.attributes);

        self.metric_name.append_value(&metric.name);
        self.description.append_value(&metric.description);
        self.unit.append_value(&metric.unit);
        self.metadata.append_value(metadata_json);
        self.attributes.append_value(&dp_attrs);
        self.resource_attributes.append_value(&res.attributes_json);
        self.resource_dropped_attributes_count
            .append_value(res.dropped_attributes_count);
        self.resource_schema_url.append_value(&res.schema_url);
        self.scope_name.append_value(scope.name);
        self.scope_version.append_value(scope.version);
        self.scope_attributes.append_value(&scope.attributes_json);
        self.scope_dropped_attributes_count
            .append_value(scope.dropped_attributes_count);
        self.scope_schema_url.append_value(scope.schema_url);
        self.timestamp.append_value(dp.time_unix_nano as i64);
        self.start_time.append_value(dp.start_time_unix_nano as i64);
        self.count_col.append_value(dp.count);
        self.sum.append_value(dp.sum);

        // Serialize quantile values as JSON array
        let qv_json = serde_json::to_string(
            &dp.quantile_values
                .iter()
                .map(|qv| {
                    serde_json::json!({
                        "quantile": qv.quantile,
                        "value": qv.value
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| "[]".to_string());
        self.quantile_values.append_value(&qv_json);
        self.flags.append_value(dp.flags);
        self.count += 1;
    }

    fn finish(mut self, table_schema: Arc<arrow_schema::Schema>) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.metric_name.finish()),
            Arc::new(self.description.finish()),
            Arc::new(self.unit.finish()),
            Arc::new(self.metadata.finish()),
            Arc::new(self.attributes.finish()),
            Arc::new(self.resource_attributes.finish()),
            Arc::new(self.resource_dropped_attributes_count.finish()),
            Arc::new(self.resource_schema_url.finish()),
            Arc::new(self.scope_name.finish()),
            Arc::new(self.scope_version.finish()),
            Arc::new(self.scope_attributes.finish()),
            Arc::new(self.scope_dropped_attributes_count.finish()),
            Arc::new(self.scope_schema_url.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.start_time.finish()),
            Arc::new(self.count_col.finish()),
            Arc::new(self.sum.finish()),
            Arc::new(self.quantile_values.finish()),
            Arc::new(self.flags.finish()),
        ];
        RecordBatch::try_new(table_schema, columns).expect("schema mismatch in summary batch")
    }
}

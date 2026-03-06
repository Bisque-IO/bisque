//! Internal metrics for bisque-lance.
//!
//! Collects operational metrics (counters, gauges, histograms) via the [`metrics`]
//! crate and periodically flushes them into the existing `otel_counters`,
//! `otel_gauges`, and `otel_histograms` tables. Internal metrics are distinguished
//! from external telemetry by resource attributes (`service.name=bisque-lance`).
//!
//! # Architecture
//!
//! ```text
//! metrics::counter!() / gauge!() / histogram!() — zero-alloc atomic hot path
//!     ↓
//! BisqueMetricsRecorder (DashMap + AtomicU64 / Mutex<HistogramState>)
//!     ↓  (periodic 60s flush, leader-only)
//! MetricsFlusher → Arrow RecordBatches matching otel_* schemas
//!     ↓
//! LanceRaftNode::write_records("otel_counters" / "otel_gauges" / "otel_histograms")
//!     ↓  (existing WriteBatcher + OtelSumAggregator / OtelHistogramProcessor)
//! Raft → TableEngine → Lance → queryable via PromQL / SQL
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
    TimestampNanosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::RecordBatch;
use dashmap::DashMap;
use metrics::{
    Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::otel::schema;
use crate::raft::LanceRaftNode;

// ---------------------------------------------------------------------------
// Default histogram boundaries (Prometheus-compatible latency buckets)
// ---------------------------------------------------------------------------

const NUM_BOUNDARIES: usize = 11;
const NUM_BUCKETS: usize = NUM_BOUNDARIES + 1;

const DEFAULT_BOUNDARIES: [f64; NUM_BOUNDARIES] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

const FLUSH_INTERVAL: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// In-memory histogram state
// ---------------------------------------------------------------------------

struct HistogramState {
    counts: [u64; NUM_BUCKETS],
    sum: f64,
    count: u64,
    min: f64,
    max: f64,
}

impl HistogramState {
    fn new() -> Self {
        Self {
            counts: [0; NUM_BUCKETS],
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    fn record(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        let idx = DEFAULT_BOUNDARIES
            .partition_point(|&b| b < value)
            .min(NUM_BUCKETS - 1);
        self.counts[idx] += 1;
    }

    fn take(&mut self) -> HistogramSnapshot {
        let snap = HistogramSnapshot {
            counts: std::mem::replace(&mut self.counts, [0; NUM_BUCKETS]),
            sum: self.sum,
            count: self.count,
            min: self.min,
            max: self.max,
        };
        self.sum = 0.0;
        self.count = 0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
        snap
    }
}

struct HistogramSnapshot {
    counts: [u64; NUM_BUCKETS],
    sum: f64,
    count: u64,
    min: f64,
    max: f64,
}

// ---------------------------------------------------------------------------
// Metric key (name + sorted labels)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct MetricKey {
    name: String,
    labels: Vec<(String, String)>,
}

impl From<&Key> for MetricKey {
    fn from(key: &Key) -> Self {
        let mut labels: Vec<(String, String)> = key
            .labels()
            .map(|l| (l.key().to_string(), l.value().to_string()))
            .collect();
        labels.sort_by(|a, b| a.0.cmp(&b.0));
        Self {
            name: key.name().to_string(),
            labels,
        }
    }
}

// ---------------------------------------------------------------------------
// Handle types for the metrics crate
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct BisqueCounter(Arc<AtomicU64>);

impl metrics::CounterFn for BisqueCounter {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    fn absolute(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct BisqueGauge(Arc<AtomicU64>);

impl metrics::GaugeFn for BisqueGauge {
    fn increment(&self, value: f64) {
        loop {
            let old = self.0.load(Ordering::Relaxed);
            let new = f64::from_bits(old) + value;
            if self
                .0
                .compare_exchange_weak(old, new.to_bits(), Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    fn decrement(&self, value: f64) {
        self.increment(-value);
    }

    fn set(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct BisqueHistogram(Arc<Mutex<HistogramState>>);

impl metrics::HistogramFn for BisqueHistogram {
    fn record(&self, value: f64) {
        self.0.lock().record(value);
    }
}

// ---------------------------------------------------------------------------
// BisqueMetricsRecorder
// ---------------------------------------------------------------------------

/// In-memory metrics recorder that implements the [`metrics::Recorder`] trait.
///
/// All metric updates are atomic / lock-free for counters and gauges.
/// Histograms use a `Mutex` per unique key for bucket management.
pub struct BisqueMetricsRecorder {
    counters: DashMap<MetricKey, Arc<AtomicU64>>,
    gauges: DashMap<MetricKey, Arc<AtomicU64>>,
    histograms: DashMap<MetricKey, Arc<Mutex<HistogramState>>>,
    descriptions: DashMap<String, (String, Option<Unit>)>,
}

impl BisqueMetricsRecorder {
    fn new() -> Self {
        Self {
            counters: DashMap::new(),
            gauges: DashMap::new(),
            histograms: DashMap::new(),
            descriptions: DashMap::new(),
        }
    }
}

impl Recorder for BisqueMetricsRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.descriptions
            .insert(key.as_str().to_string(), (description.to_string(), unit));
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.descriptions
            .insert(key.as_str().to_string(), (description.to_string(), unit));
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.descriptions
            .insert(key.as_str().to_string(), (description.to_string(), unit));
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        let mk = MetricKey::from(key);
        let atomic = self
            .counters
            .entry(mk)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        Counter::from_arc(Arc::new(BisqueCounter(atomic)))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        let mk = MetricKey::from(key);
        let atomic = self
            .gauges
            .entry(mk)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        Gauge::from_arc(Arc::new(BisqueGauge(atomic)))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        let mk = MetricKey::from(key);
        let state = self
            .histograms
            .entry(mk)
            .or_insert_with(|| Arc::new(Mutex::new(HistogramState::new())))
            .clone();
        Histogram::from_arc(Arc::new(BisqueHistogram(state)))
    }
}

// ---------------------------------------------------------------------------
// MetricsFlusher
// ---------------------------------------------------------------------------

struct MetricsFlusher {
    recorder: Arc<BisqueMetricsRecorder>,
    raft_node: Arc<LanceRaftNode>,
    shutdown: Arc<Notify>,
}

impl MetricsFlusher {
    async fn run(self) {
        let mut interval = tokio::time::interval(FLUSH_INTERVAL);
        interval.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if self.raft_node.is_leader() {
                        self.flush().await;
                    }
                }
                _ = self.shutdown.notified() => {
                    break;
                }
            }
        }
    }

    async fn flush(&self) {
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        let start_nanos = now_nanos - FLUSH_INTERVAL.as_nanos() as i64;

        let node_id = self.raft_node.node_id();
        let group_id = self.raft_node.group_id();
        let resource_attributes = format!(
            r#"{{"service.name":"bisque-lance","service.instance.id":"{}","bisque.group_id":"{}"}}"#,
            node_id, group_id
        );

        // Flush counters
        if let Err(e) = self
            .flush_counters(now_nanos, start_nanos, &resource_attributes)
            .await
        {
            warn!(error = %e, "failed to flush internal counter metrics");
        }

        // Flush gauges
        if let Err(e) = self
            .flush_gauges(now_nanos, start_nanos, &resource_attributes)
            .await
        {
            warn!(error = %e, "failed to flush internal gauge metrics");
        }

        // Flush histograms
        if let Err(e) = self
            .flush_histograms(now_nanos, start_nanos, &resource_attributes)
            .await
        {
            warn!(error = %e, "failed to flush internal histogram metrics");
        }
    }

    async fn flush_counters(
        &self,
        now_nanos: i64,
        start_nanos: i64,
        resource_attributes: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Snapshot counters — read and reset deltas
        let mut entries: Vec<(MetricKey, u64)> = Vec::new();
        for entry in self.recorder.counters.iter() {
            let val = entry.value().swap(0, Ordering::Relaxed);
            if val > 0 {
                entries.push((entry.key().clone(), val));
            }
        }
        if entries.is_empty() {
            return Ok(());
        }

        let n = entries.len();
        let mut metric_name = StringBuilder::with_capacity(n, n * 32);
        let mut description = StringBuilder::with_capacity(n, n * 64);
        let mut unit = StringBuilder::with_capacity(n, n * 8);
        let mut metadata = StringBuilder::with_capacity(n, n * 4);
        let mut attributes = StringBuilder::with_capacity(n, n * 64);
        let mut resource_attrs = StringBuilder::with_capacity(n, n * 128);
        let mut resource_schema_url = StringBuilder::with_capacity(n, n * 4);
        let mut scope_name = StringBuilder::with_capacity(n, n * 16);
        let mut scope_version = StringBuilder::with_capacity(n, n * 8);
        let mut scope_attributes = StringBuilder::with_capacity(n, n * 4);
        let mut scope_schema_url = StringBuilder::with_capacity(n, n * 4);
        let mut timestamp = TimestampNanosecondBuilder::with_capacity(n);
        let mut start_time = TimestampNanosecondBuilder::with_capacity(n);
        let mut value_int = Int64Builder::with_capacity(n);
        let mut value_double = Float64Builder::with_capacity(n);
        let mut agg_temporality = Int32Builder::with_capacity(n);
        let mut is_monotonic = BooleanBuilder::with_capacity(n);
        let mut flags = UInt32Builder::with_capacity(n);
        let mut resource_dropped = UInt32Builder::with_capacity(n);
        let mut scope_dropped = UInt32Builder::with_capacity(n);

        for (key, val) in &entries {
            metric_name.append_value(&key.name);

            let (desc, u) = self
                .recorder
                .descriptions
                .get(&key.name)
                .map(|e| (e.0.clone(), e.1.clone()))
                .unwrap_or_default();
            description.append_value(&desc);
            unit.append_value(unit_str(u.as_ref()));

            metadata.append_value("");
            attributes.append_value(labels_to_json(&key.labels));
            resource_attrs.append_value(resource_attributes);
            resource_schema_url.append_value("");
            scope_name.append_value("bisque-lance");
            scope_version.append_value(env!("CARGO_PKG_VERSION"));
            scope_attributes.append_value("");
            scope_schema_url.append_value("");
            timestamp.append_value(now_nanos);
            start_time.append_value(start_nanos);
            value_int.append_null();
            value_double.append_value(*val as f64);
            agg_temporality.append_value(1); // DELTA
            is_monotonic.append_value(true);
            flags.append_value(0);
            resource_dropped.append_value(0);
            scope_dropped.append_value(0);
        }

        let batch = RecordBatch::try_new(
            schema::counter_schema_ref().clone(),
            vec![
                Arc::new(metric_name.finish()),
                Arc::new(description.finish()),
                Arc::new(unit.finish()),
                Arc::new(metadata.finish()),
                Arc::new(attributes.finish()),
                Arc::new(resource_attrs.finish()),
                Arc::new(resource_schema_url.finish()),
                Arc::new(scope_name.finish()),
                Arc::new(scope_version.finish()),
                Arc::new(scope_attributes.finish()),
                Arc::new(scope_schema_url.finish()),
                Arc::new(timestamp.finish()),
                Arc::new(start_time.finish()),
                Arc::new(value_int.finish()),
                Arc::new(value_double.finish()),
                Arc::new(agg_temporality.finish()),
                Arc::new(is_monotonic.finish()),
                Arc::new(flags.finish()),
                Arc::new(resource_dropped.finish()),
                Arc::new(scope_dropped.finish()),
            ],
        )?;

        self.raft_node
            .write_records(schema::COUNTERS_TABLE, &[batch])
            .await?;
        Ok(())
    }

    async fn flush_gauges(
        &self,
        now_nanos: i64,
        start_nanos: i64,
        resource_attributes: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut entries: Vec<(MetricKey, f64)> = Vec::new();
        for entry in self.recorder.gauges.iter() {
            let bits = entry.value().load(Ordering::Relaxed);
            let val = f64::from_bits(bits);
            if val != 0.0 || bits != 0 {
                entries.push((entry.key().clone(), val));
            }
        }
        if entries.is_empty() {
            return Ok(());
        }

        let n = entries.len();
        let mut metric_name = StringBuilder::with_capacity(n, n * 32);
        let mut description_b = StringBuilder::with_capacity(n, n * 64);
        let mut unit_b = StringBuilder::with_capacity(n, n * 8);
        let mut metadata_b = StringBuilder::with_capacity(n, n * 4);
        let mut attributes_b = StringBuilder::with_capacity(n, n * 64);
        let mut resource_attrs_b = StringBuilder::with_capacity(n, n * 128);
        let mut resource_schema_url_b = StringBuilder::with_capacity(n, n * 4);
        let mut scope_name_b = StringBuilder::with_capacity(n, n * 16);
        let mut scope_version_b = StringBuilder::with_capacity(n, n * 8);
        let mut scope_attributes_b = StringBuilder::with_capacity(n, n * 4);
        let mut scope_schema_url_b = StringBuilder::with_capacity(n, n * 4);
        let mut timestamp_b = TimestampNanosecondBuilder::with_capacity(n);
        let mut start_time_b = TimestampNanosecondBuilder::with_capacity(n);
        let mut value_int_b = Int64Builder::with_capacity(n);
        let mut value_double_b = Float64Builder::with_capacity(n);
        let mut flags_b = UInt32Builder::with_capacity(n);
        let mut resource_dropped_b = UInt32Builder::with_capacity(n);
        let mut scope_dropped_b = UInt32Builder::with_capacity(n);

        for (key, val) in &entries {
            metric_name.append_value(&key.name);
            let (desc, u) = self
                .recorder
                .descriptions
                .get(&key.name)
                .map(|e| (e.0.clone(), e.1.clone()))
                .unwrap_or_default();
            description_b.append_value(&desc);
            unit_b.append_value(unit_str(u.as_ref()));
            metadata_b.append_value("");
            attributes_b.append_value(labels_to_json(&key.labels));
            resource_attrs_b.append_value(resource_attributes);
            resource_schema_url_b.append_value("");
            scope_name_b.append_value("bisque-lance");
            scope_version_b.append_value(env!("CARGO_PKG_VERSION"));
            scope_attributes_b.append_value("");
            scope_schema_url_b.append_value("");
            timestamp_b.append_value(now_nanos);
            start_time_b.append_value(start_nanos);
            value_int_b.append_null();
            value_double_b.append_value(*val);
            flags_b.append_value(0);
            resource_dropped_b.append_value(0);
            scope_dropped_b.append_value(0);
        }

        let batch = RecordBatch::try_new(
            schema::gauge_schema_ref().clone(),
            vec![
                Arc::new(metric_name.finish()),
                Arc::new(description_b.finish()),
                Arc::new(unit_b.finish()),
                Arc::new(metadata_b.finish()),
                Arc::new(attributes_b.finish()),
                Arc::new(resource_attrs_b.finish()),
                Arc::new(resource_schema_url_b.finish()),
                Arc::new(scope_name_b.finish()),
                Arc::new(scope_version_b.finish()),
                Arc::new(scope_attributes_b.finish()),
                Arc::new(scope_schema_url_b.finish()),
                Arc::new(timestamp_b.finish()),
                Arc::new(start_time_b.finish()),
                Arc::new(value_int_b.finish()),
                Arc::new(value_double_b.finish()),
                Arc::new(flags_b.finish()),
                Arc::new(resource_dropped_b.finish()),
                Arc::new(scope_dropped_b.finish()),
            ],
        )?;

        self.raft_node
            .write_records(schema::GAUGES_TABLE, &[batch])
            .await?;
        Ok(())
    }

    async fn flush_histograms(
        &self,
        now_nanos: i64,
        start_nanos: i64,
        resource_attributes: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut entries: Vec<(MetricKey, HistogramSnapshot)> = Vec::new();
        for entry in self.recorder.histograms.iter() {
            let snap = entry.value().lock().take();
            if snap.count > 0 {
                entries.push((entry.key().clone(), snap));
            }
        }
        if entries.is_empty() {
            return Ok(());
        }

        let n = entries.len();
        let mut metric_name = StringBuilder::with_capacity(n, n * 32);
        let mut description_b = StringBuilder::with_capacity(n, n * 64);
        let mut unit_b = StringBuilder::with_capacity(n, n * 8);
        let mut metadata_b = StringBuilder::with_capacity(n, n * 4);
        let mut attributes_b = StringBuilder::with_capacity(n, n * 64);
        let mut resource_attrs_b = StringBuilder::with_capacity(n, n * 128);
        let mut resource_schema_url_b = StringBuilder::with_capacity(n, n * 4);
        let mut scope_name_b = StringBuilder::with_capacity(n, n * 16);
        let mut scope_version_b = StringBuilder::with_capacity(n, n * 8);
        let mut scope_attributes_b = StringBuilder::with_capacity(n, n * 4);
        let mut scope_schema_url_b = StringBuilder::with_capacity(n, n * 4);
        let mut timestamp_b = TimestampNanosecondBuilder::with_capacity(n);
        let mut start_time_b = TimestampNanosecondBuilder::with_capacity(n);
        let mut boundaries_b = ListBuilder::new(Float64Builder::with_capacity(n * 12));
        let mut bucket_counts_b = ListBuilder::new(UInt64Builder::with_capacity(n * 12));
        let mut sum_b = Float64Builder::with_capacity(n);
        let mut count_b = UInt64Builder::with_capacity(n);
        let mut min_b = Float64Builder::with_capacity(n);
        let mut max_b = Float64Builder::with_capacity(n);
        let mut agg_temporality_b = Int32Builder::with_capacity(n);
        let mut is_monotonic_b = BooleanBuilder::with_capacity(n);
        let mut flags_b = UInt32Builder::with_capacity(n);
        let mut resource_dropped_b = UInt32Builder::with_capacity(n);
        let mut scope_dropped_b = UInt32Builder::with_capacity(n);

        for (key, snap) in &entries {
            metric_name.append_value(&key.name);
            let (desc, u) = self
                .recorder
                .descriptions
                .get(&key.name)
                .map(|e| (e.0.clone(), e.1.clone()))
                .unwrap_or_default();
            description_b.append_value(&desc);
            unit_b.append_value(unit_str(u.as_ref()));
            metadata_b.append_value("");
            attributes_b.append_value(labels_to_json(&key.labels));
            resource_attrs_b.append_value(resource_attributes);
            resource_schema_url_b.append_value("");
            scope_name_b.append_value("bisque-lance");
            scope_version_b.append_value(env!("CARGO_PKG_VERSION"));
            scope_attributes_b.append_value("");
            scope_schema_url_b.append_value("");
            timestamp_b.append_value(now_nanos);
            start_time_b.append_value(start_nanos);

            // boundaries
            let boundary_values = boundaries_b.values();
            for &b in &DEFAULT_BOUNDARIES {
                boundary_values.append_value(b);
            }
            boundaries_b.append(true);

            // bucket_counts
            let count_values = bucket_counts_b.values();
            for &c in &snap.counts {
                count_values.append_value(c);
            }
            bucket_counts_b.append(true);

            sum_b.append_value(snap.sum);
            count_b.append_value(snap.count);
            if snap.min == f64::INFINITY {
                min_b.append_null();
            } else {
                min_b.append_value(snap.min);
            }
            if snap.max == f64::NEG_INFINITY {
                max_b.append_null();
            } else {
                max_b.append_value(snap.max);
            }

            agg_temporality_b.append_value(1); // DELTA
            is_monotonic_b.append_value(false);
            flags_b.append_value(0);
            resource_dropped_b.append_value(0);
            scope_dropped_b.append_value(0);
        }

        let batch = RecordBatch::try_new(
            schema::histogram_schema_ref().clone(),
            vec![
                Arc::new(metric_name.finish()),
                Arc::new(description_b.finish()),
                Arc::new(unit_b.finish()),
                Arc::new(metadata_b.finish()),
                Arc::new(attributes_b.finish()),
                Arc::new(resource_attrs_b.finish()),
                Arc::new(resource_schema_url_b.finish()),
                Arc::new(scope_name_b.finish()),
                Arc::new(scope_version_b.finish()),
                Arc::new(scope_attributes_b.finish()),
                Arc::new(scope_schema_url_b.finish()),
                Arc::new(timestamp_b.finish()),
                Arc::new(start_time_b.finish()),
                Arc::new(boundaries_b.finish()),
                Arc::new(bucket_counts_b.finish()),
                Arc::new(sum_b.finish()),
                Arc::new(count_b.finish()),
                Arc::new(min_b.finish()),
                Arc::new(max_b.finish()),
                Arc::new(agg_temporality_b.finish()),
                Arc::new(is_monotonic_b.finish()),
                Arc::new(flags_b.finish()),
                Arc::new(resource_dropped_b.finish()),
                Arc::new(scope_dropped_b.finish()),
            ],
        )?;

        self.raft_node
            .write_records(schema::HISTOGRAMS_TABLE, &[batch])
            .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn labels_to_json(labels: &[(String, String)]) -> String {
    if labels.is_empty() {
        return "{}".to_string();
    }
    let mut s = String::from('{');
    for (i, (k, v)) in labels.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        // Simple JSON escaping for known-safe metric label keys/values
        s.push('"');
        s.push_str(k);
        s.push_str("\":\"");
        s.push_str(v);
        s.push('"');
    }
    s.push('}');
    s
}

fn unit_str(u: Option<&Unit>) -> &'static str {
    match u {
        Some(Unit::Seconds) => "s",
        Some(Unit::Bytes) => "By",
        Some(Unit::Count) => "1",
        _ => "",
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Install the global metrics recorder and spawn the periodic flusher.
///
/// Call this once after the `LanceRaftNode` is started and OTel tables exist.
/// The flusher only writes when this node is the Raft leader.
///
/// Returns a shutdown handle. Call `shutdown.notify_waiters()` to stop the flusher,
/// along with the background task handle.
pub fn install(raft_node: Arc<LanceRaftNode>) -> (Arc<Notify>, JoinHandle<()>) {
    let recorder = Arc::new(BisqueMetricsRecorder::new());
    let recorder_clone = recorder.clone();

    match metrics::set_global_recorder(recorder) {
        Ok(()) => {
            info!("installed bisque-lance internal metrics recorder");
        }
        Err(_) => {
            warn!("metrics recorder already installed, internal metrics will be no-op");
        }
    }

    let shutdown = Arc::new(Notify::new());
    let flusher = MetricsFlusher {
        recorder: recorder_clone,
        raft_node,
        shutdown: shutdown.clone(),
    };

    let handle = tokio::spawn(flusher.run());
    (shutdown, handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_labels_to_json() {
        assert_eq!(labels_to_json(&[]), "{}");
        assert_eq!(
            labels_to_json(&[("table".into(), "events".into())]),
            r#"{"table":"events"}"#
        );
        assert_eq!(
            labels_to_json(&[
                ("op".into(), "query".into()),
                ("protocol".into(), "flight".into()),
            ]),
            r#"{"op":"query","protocol":"flight"}"#
        );
    }

    #[test]
    fn test_histogram_state() {
        let mut state = HistogramState::new();
        // DEFAULT_BOUNDARIES: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        state.record(0.003); // bucket 0: [0, 0.005)
        state.record(0.03); // bucket 3: [0.025, 0.05)
        state.record(0.7); // bucket 7: [0.5, 1.0)
        state.record(20.0); // bucket 11: [10.0, +inf)

        assert_eq!(state.count, 4);
        assert!((state.sum - 20.733).abs() < 1e-10);
        assert!((state.min - 0.003).abs() < 1e-10);
        assert!((state.max - 20.0).abs() < 1e-10);
        assert_eq!(state.counts[0], 1); // [0, 0.005)
        assert_eq!(state.counts[3], 1); // [0.025, 0.05)
        assert_eq!(state.counts[7], 1); // [0.5, 1.0)
        assert_eq!(state.counts[11], 1); // [10.0, +inf)

        let snap = state.take();
        assert_eq!(snap.count, 4);
        assert_eq!(snap.counts[0], 1);
        assert_eq!(snap.counts[3], 1);
        assert_eq!(state.count, 0);
        assert_eq!(state.counts, [0u64; NUM_BUCKETS]);
    }

    #[test]
    fn test_metric_key_from_key() {
        let key = Key::from_parts(
            "bisque_requests_total",
            vec![
                metrics::Label::new("protocol", "flight"),
                metrics::Label::new("op", "query"),
            ],
        );
        let mk = MetricKey::from(&key);
        assert_eq!(mk.name, "bisque_requests_total");
        // Labels should be sorted by key
        assert_eq!(mk.labels[0].0, "op");
        assert_eq!(mk.labels[1].0, "protocol");
    }

    #[test]
    fn test_recorder_registers_and_increments() {
        let recorder = BisqueMetricsRecorder::new();
        let key = Key::from_parts("test_counter", vec![metrics::Label::new("a", "b")]);
        let metadata = Metadata::new(module_path!(), metrics::Level::INFO, None);

        let counter = recorder.register_counter(&key, &metadata);
        counter.increment(5);
        counter.increment(3);

        let mk = MetricKey::from(&key);
        let val = recorder.counters.get(&mk).unwrap().load(Ordering::Relaxed);
        assert_eq!(val, 8);
    }

    #[test]
    fn test_recorder_gauge_set() {
        let recorder = BisqueMetricsRecorder::new();
        let key: Key = "test_gauge".into();
        let metadata = Metadata::new(module_path!(), metrics::Level::INFO, None);

        let gauge = recorder.register_gauge(&key, &metadata);
        gauge.set(42.5);

        let mk = MetricKey::from(&key);
        let bits = recorder.gauges.get(&mk).unwrap().load(Ordering::Relaxed);
        assert!((f64::from_bits(bits) - 42.5).abs() < 1e-10);
    }

    #[test]
    fn test_recorder_histogram_record() {
        let recorder = BisqueMetricsRecorder::new();
        let key: Key = "test_hist".into();
        let metadata = Metadata::new(module_path!(), metrics::Level::INFO, None);

        let hist = recorder.register_histogram(&key, &metadata);
        hist.record(0.05);
        hist.record(0.5);

        let mk = MetricKey::from(&key);
        let state = recorder.histograms.get(&mk).unwrap();
        let guard = state.lock();
        assert_eq!(guard.count, 2);
        assert!((guard.sum - 0.55).abs() < 1e-10);
    }
}

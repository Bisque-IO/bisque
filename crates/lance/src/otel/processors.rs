//! Specialized OTEL metric write processors.
//!
//! These processors know the exact column layout of OTEL metric tables
//! (as defined in `schema.rs`) and produce output batches matching the
//! table schema. Unlike the generic processors, they handle all OTEL
//! columns natively without requiring passthrough configuration.
//!
//! - [`OtelSumAggregator`] — sums values, truncates timestamps, min(start_time)
//! - [`OtelGaugeProcessor`] — last-write-wins for value/timestamps
//! - [`OtelHistogramProcessor`] — merges buckets, sums counts, min/max tracking
//! - [`OtelExpHistogramAggregator`] — merges exponential histogram buckets

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
    TimestampNanosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray, RecordBatch,
    StringArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
};

use crate::otel::schema;
use crate::types::ProcessorDescriptor;
use crate::write_processor::{ProcessorOutput, WriteProcessor};

const NUM_KEY_COLS: usize = 11;

// ---------------------------------------------------------------------------
// Shared types — counter/histogram/exp-histogram passthrough
// ---------------------------------------------------------------------------

/// Non-Utf8 columns stable within a counter/histogram group (first-row-wins).
struct CounterPassthrough {
    aggregation_temporality: i32,
    is_monotonic: bool,
    flags: u32,
    resource_dropped_attributes_count: u32,
    scope_dropped_attributes_count: u32,
}

struct CounterPassthroughArrays<'a> {
    aggregation_temporality: &'a Int32Array,
    is_monotonic: &'a BooleanArray,
    flags: &'a UInt32Array,
    resource_dropped_attributes_count: &'a UInt32Array,
    scope_dropped_attributes_count: &'a UInt32Array,
}

/// Non-Utf8 columns stable within a gauge group (no agg_temp/is_monotonic).
struct GaugePassthrough {
    flags: u32,
    resource_dropped_attributes_count: u32,
    scope_dropped_attributes_count: u32,
}

struct GaugePassthroughArrays<'a> {
    flags: &'a UInt32Array,
    resource_dropped_attributes_count: &'a UInt32Array,
    scope_dropped_attributes_count: &'a UInt32Array,
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn get_str<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' not found"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("column '{name}' must be Utf8"))
}

fn get_i32<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int32Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' not found"))
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap_or_else(|| panic!("column '{name}' must be Int32"))
}

fn get_i64<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' not found"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("column '{name}' must be Int64"))
}

fn get_f64<'a>(batch: &'a RecordBatch, name: &str) -> &'a Float64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' not found"))
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| panic!("column '{name}' must be Float64"))
}

fn get_u64<'a>(batch: &'a RecordBatch, name: &str) -> &'a UInt64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' not found"))
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap_or_else(|| panic!("column '{name}' must be UInt64"))
}

fn get_ts<'a>(batch: &'a RecordBatch, name: &str) -> &'a TimestampNanosecondArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' not found"))
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap_or_else(|| panic!("column '{name}' must be TimestampNanosecond"))
}

fn get_list<'a>(batch: &'a RecordBatch, name: &str) -> &'a ListArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column '{name}' not found"))
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap_or_else(|| panic!("column '{name}' must be List"))
}

fn extract_keys(batch: &RecordBatch) -> [&StringArray; NUM_KEY_COLS] {
    [
        get_str(batch, "metric_name"),
        get_str(batch, "description"),
        get_str(batch, "unit"),
        get_str(batch, "metadata"),
        get_str(batch, "attributes"),
        get_str(batch, "resource_attributes"),
        get_str(batch, "resource_schema_url"),
        get_str(batch, "scope_name"),
        get_str(batch, "scope_version"),
        get_str(batch, "scope_attributes"),
        get_str(batch, "scope_schema_url"),
    ]
}

fn extract_counter_pt(batch: &RecordBatch) -> CounterPassthroughArrays<'_> {
    CounterPassthroughArrays {
        aggregation_temporality: batch
            .column_by_name("aggregation_temporality")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap(),
        is_monotonic: batch
            .column_by_name("is_monotonic")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap(),
        flags: batch
            .column_by_name("flags")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap(),
        resource_dropped_attributes_count: batch
            .column_by_name("resource_dropped_attributes_count")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap(),
        scope_dropped_attributes_count: batch
            .column_by_name("scope_dropped_attributes_count")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap(),
    }
}

fn read_counter_pt(a: &CounterPassthroughArrays<'_>, row: usize) -> CounterPassthrough {
    CounterPassthrough {
        aggregation_temporality: a.aggregation_temporality.value(row),
        is_monotonic: a.is_monotonic.value(row),
        flags: a.flags.value(row),
        resource_dropped_attributes_count: a.resource_dropped_attributes_count.value(row),
        scope_dropped_attributes_count: a.scope_dropped_attributes_count.value(row),
    }
}

fn extract_gauge_pt(batch: &RecordBatch) -> GaugePassthroughArrays<'_> {
    GaugePassthroughArrays {
        flags: batch
            .column_by_name("flags")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap(),
        resource_dropped_attributes_count: batch
            .column_by_name("resource_dropped_attributes_count")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap(),
        scope_dropped_attributes_count: batch
            .column_by_name("scope_dropped_attributes_count")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap(),
    }
}

fn read_gauge_pt(a: &GaugePassthroughArrays<'_>, row: usize) -> GaugePassthrough {
    GaugePassthrough {
        flags: a.flags.value(row),
        resource_dropped_attributes_count: a.resource_dropped_attributes_count.value(row),
        scope_dropped_attributes_count: a.scope_dropped_attributes_count.value(row),
    }
}

fn build_key(key_arrays: &[&StringArray; NUM_KEY_COLS], row: usize, buf: &mut String) {
    buf.clear();
    for (i, arr) in key_arrays.iter().enumerate() {
        if i > 0 {
            buf.push('\0');
        }
        buf.push_str(arr.value(row));
    }
}

/// Split composite keys back into NUM_KEY_COLS StringBuilder columns.
fn build_key_columns(accum_keys: impl Iterator<Item = impl AsRef<str>>, n: usize) -> Vec<ArrayRef> {
    let mut builders: Vec<StringBuilder> = (0..NUM_KEY_COLS)
        .map(|_| StringBuilder::with_capacity(n, n * 32))
        .collect();
    for key in accum_keys {
        let mut parts = key.as_ref().split('\0');
        for builder in builders.iter_mut() {
            builder.append_value(parts.next().unwrap_or(""));
        }
    }
    builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as ArrayRef)
        .collect()
}

/// Build the 5 counter passthrough columns from accumulated entries.
fn build_counter_pt_columns(
    entries: impl Iterator<Item = impl AsRef<CounterPassthrough>>,
    n: usize,
) -> Vec<ArrayRef> {
    let mut at = Int32Builder::with_capacity(n);
    let mut im = BooleanBuilder::with_capacity(n);
    let mut fl = UInt32Builder::with_capacity(n);
    let mut rdac = UInt32Builder::with_capacity(n);
    let mut sdac = UInt32Builder::with_capacity(n);
    for pt_ref in entries {
        let pt = pt_ref.as_ref();
        at.append_value(pt.aggregation_temporality);
        im.append_value(pt.is_monotonic);
        fl.append_value(pt.flags);
        rdac.append_value(pt.resource_dropped_attributes_count);
        sdac.append_value(pt.scope_dropped_attributes_count);
    }
    vec![
        Arc::new(at.finish()),
        Arc::new(im.finish()),
        Arc::new(fl.finish()),
        Arc::new(rdac.finish()),
        Arc::new(sdac.finish()),
    ]
}

/// Build the 3 gauge passthrough columns from accumulated entries.
fn build_gauge_pt_columns(
    entries: impl Iterator<Item = impl AsRef<GaugePassthrough>>,
    n: usize,
) -> Vec<ArrayRef> {
    let mut fl = UInt32Builder::with_capacity(n);
    let mut rdac = UInt32Builder::with_capacity(n);
    let mut sdac = UInt32Builder::with_capacity(n);
    for pt_ref in entries {
        let pt = pt_ref.as_ref();
        fl.append_value(pt.flags);
        rdac.append_value(pt.resource_dropped_attributes_count);
        sdac.append_value(pt.scope_dropped_attributes_count);
    }
    vec![
        Arc::new(fl.finish()),
        Arc::new(rdac.finish()),
        Arc::new(sdac.finish()),
    ]
}

impl AsRef<CounterPassthrough> for CounterPassthrough {
    fn as_ref(&self) -> &CounterPassthrough {
        self
    }
}

impl AsRef<GaugePassthrough> for GaugePassthrough {
    fn as_ref(&self) -> &GaugePassthrough {
        self
    }
}

// ---------------------------------------------------------------------------
// Number value helper — reads value_int / value_double nullable columns
// ---------------------------------------------------------------------------

/// OTLP NumberDataPoint value read from two nullable columns.
#[derive(Clone, Copy)]
enum NumberValue {
    Int(i64),
    Double(f64),
    None,
}

impl NumberValue {
    fn read(value_int: &Int64Array, value_double: &Float64Array, row: usize) -> Self {
        if !value_int.is_null(row) {
            NumberValue::Int(value_int.value(row))
        } else if !value_double.is_null(row) {
            NumberValue::Double(value_double.value(row))
        } else {
            NumberValue::None
        }
    }

    fn as_f64(self) -> f64 {
        match self {
            NumberValue::Int(i) => i as f64,
            NumberValue::Double(d) => d,
            NumberValue::None => 0.0,
        }
    }
}

/// Build two nullable value columns (value_int, value_double) from entries.
fn build_number_value_columns<I, F>(entries: I, n: usize, extract: F) -> Vec<ArrayRef>
where
    I: Iterator,
    F: Fn(I::Item) -> NumberValue,
{
    let mut vi = Int64Builder::with_capacity(n);
    let mut vd = Float64Builder::with_capacity(n);
    for item in entries {
        match extract(item) {
            NumberValue::Int(i) => {
                vi.append_value(i);
                vd.append_null();
            }
            NumberValue::Double(d) => {
                vi.append_null();
                vd.append_value(d);
            }
            NumberValue::None => {
                vi.append_null();
                vd.append_null();
            }
        }
    }
    vec![Arc::new(vi.finish()), Arc::new(vd.finish())]
}

// ===========================================================================
// OtelSumAggregator
// ===========================================================================

struct CounterAccum {
    timestamp: i64,
    start_time: i64,
    /// Aggregated sum — always stored as f64 since summing may mix int/double.
    value: f64,
    pt: CounterPassthrough,
}

/// OTEL counter aggregator — sums Float64 values per group key, truncates
/// timestamps to a configurable resolution, keeps min(start_time).
///
/// The aggregated output always uses `value_double` (summing may mix int/double
/// inputs, and the sum of large ints may exceed i64 range).
#[derive(Debug, Clone)]
pub struct OtelSumAggregator {
    timestamp_resolution_ns: i64,
}

impl OtelSumAggregator {
    pub fn new(timestamp_resolution_ms: i64) -> Self {
        Self {
            timestamp_resolution_ns: timestamp_resolution_ms * 1_000_000,
        }
    }

    fn aggregate(&self, batches: &[RecordBatch]) -> RecordBatch {
        let mut accum: HashMap<String, CounterAccum> = HashMap::new();
        let mut key_buf = String::new();

        for batch in batches {
            let keys = extract_keys(batch);
            let ts = get_ts(batch, "timestamp");
            let st = get_ts(batch, "start_time");
            let value_int = get_i64(batch, "value_int");
            let value_double = get_f64(batch, "value_double");
            let pt_arrays = extract_counter_pt(batch);

            for row in 0..batch.num_rows() {
                build_key(&keys, row, &mut key_buf);

                // Append truncated timestamp to key for grouping.
                let truncated =
                    (ts.value(row) / self.timestamp_resolution_ns) * self.timestamp_resolution_ns;
                key_buf.push('\0');
                let _ = std::fmt::Write::write_fmt(&mut key_buf, format_args!("{truncated}"));

                let nv = NumberValue::read(value_int, value_double, row);

                if let Some(entry) = accum.get_mut(key_buf.as_str()) {
                    entry.value += nv.as_f64();
                    entry.start_time = entry.start_time.min(st.value(row));
                } else {
                    accum.insert(
                        key_buf.clone(),
                        CounterAccum {
                            timestamp: truncated,
                            start_time: st.value(row),
                            value: nv.as_f64(),
                            pt: read_counter_pt(&pt_arrays, row),
                        },
                    );
                }
            }
        }

        self.build_output(&accum)
    }

    fn build_output(&self, accum: &HashMap<String, CounterAccum>) -> RecordBatch {
        let n = accum.len();
        let entries: Vec<(&String, &CounterAccum)> = accum.iter().collect();

        let mut columns: Vec<ArrayRef> = build_key_columns(entries.iter().map(|(k, _)| k), n);

        // Timestamps
        let mut ts_builder = TimestampNanosecondBuilder::with_capacity(n);
        let mut st_builder = TimestampNanosecondBuilder::with_capacity(n);
        for (_, entry) in &entries {
            ts_builder.append_value(entry.timestamp);
            st_builder.append_value(entry.start_time);
        }
        columns.push(Arc::new(ts_builder.finish()));
        columns.push(Arc::new(st_builder.finish()));

        // Value — aggregated sum always stored as value_double
        columns.extend(build_number_value_columns(entries.iter(), n, |(_, e)| {
            NumberValue::Double(e.value)
        }));

        // Passthrough
        columns.extend(build_counter_pt_columns(
            entries.iter().map(|(_, e)| &e.pt),
            n,
        ));

        let table_schema = schema::counter_schema_ref().clone();
        RecordBatch::try_new(table_schema, columns).expect("schema mismatch in otel counter output")
    }
}

impl WriteProcessor for OtelSumAggregator {
    fn process(&self, batches: Vec<RecordBatch>) -> ProcessorOutput {
        if batches.is_empty() {
            return ProcessorOutput::primary_only(Vec::new());
        }
        ProcessorOutput::primary_only(vec![self.aggregate(&batches)])
    }

    fn descriptor(&self) -> Option<ProcessorDescriptor> {
        Some(ProcessorDescriptor::OtelSum {
            timestamp_resolution_ms: self.timestamp_resolution_ns / 1_000_000,
        })
    }
}

// ===========================================================================
// OtelGaugeProcessor
// ===========================================================================

struct GaugeAccum {
    timestamp: i64,
    start_time: i64,
    value: NumberValue,
    pt: GaugePassthrough,
}

/// OTEL gauge aggregator — keeps the last value per group key
/// (last-write-wins for value, timestamp, and start_time).
///
/// Preserves the original value type (int vs double) through aggregation.
#[derive(Debug, Clone)]
pub struct OtelGaugeProcessor;

impl OtelGaugeProcessor {
    pub fn new() -> Self {
        Self
    }

    fn aggregate(&self, batches: &[RecordBatch]) -> RecordBatch {
        let mut accum: HashMap<String, GaugeAccum> = HashMap::new();
        let mut key_buf = String::new();

        for batch in batches {
            let keys = extract_keys(batch);
            let ts = get_ts(batch, "timestamp");
            let st = get_ts(batch, "start_time");
            let value_int = get_i64(batch, "value_int");
            let value_double = get_f64(batch, "value_double");
            let pt_arrays = extract_gauge_pt(batch);

            for row in 0..batch.num_rows() {
                build_key(&keys, row, &mut key_buf);

                let nv = NumberValue::read(value_int, value_double, row);

                if let Some(entry) = accum.get_mut(key_buf.as_str()) {
                    entry.timestamp = ts.value(row);
                    entry.start_time = st.value(row);
                    entry.value = nv;
                    entry.pt = read_gauge_pt(&pt_arrays, row);
                } else {
                    accum.insert(
                        key_buf.clone(),
                        GaugeAccum {
                            timestamp: ts.value(row),
                            start_time: st.value(row),
                            value: nv,
                            pt: read_gauge_pt(&pt_arrays, row),
                        },
                    );
                }
            }
        }

        self.build_output(&accum)
    }

    fn build_output(&self, accum: &HashMap<String, GaugeAccum>) -> RecordBatch {
        let n = accum.len();
        let entries: Vec<(&String, &GaugeAccum)> = accum.iter().collect();

        let mut columns: Vec<ArrayRef> = build_key_columns(entries.iter().map(|(k, _)| k), n);

        let mut ts_builder = TimestampNanosecondBuilder::with_capacity(n);
        let mut st_builder = TimestampNanosecondBuilder::with_capacity(n);
        for (_, entry) in &entries {
            ts_builder.append_value(entry.timestamp);
            st_builder.append_value(entry.start_time);
        }
        columns.push(Arc::new(ts_builder.finish()));
        columns.push(Arc::new(st_builder.finish()));

        // Value — preserve original type (last-write-wins)
        columns.extend(build_number_value_columns(entries.iter(), n, |(_, e)| {
            e.value
        }));

        // Gauge passthrough (no agg_temp/is_monotonic)
        columns.extend(build_gauge_pt_columns(
            entries.iter().map(|(_, e)| &e.pt),
            n,
        ));

        let table_schema = schema::gauge_schema_ref().clone();
        RecordBatch::try_new(table_schema, columns).expect("schema mismatch in otel gauge output")
    }
}

impl WriteProcessor for OtelGaugeProcessor {
    fn process(&self, batches: Vec<RecordBatch>) -> ProcessorOutput {
        if batches.is_empty() {
            return ProcessorOutput::primary_only(Vec::new());
        }
        ProcessorOutput::primary_only(vec![self.aggregate(&batches)])
    }

    fn descriptor(&self) -> Option<ProcessorDescriptor> {
        Some(ProcessorDescriptor::OtelGauge)
    }
}

// ===========================================================================
// OtelHistogramProcessor
// ===========================================================================

struct HistogramAccum {
    timestamp: i64,
    start_time: i64,
    boundaries: Vec<f64>,
    bucket_counts: Vec<u64>,
    sum: Option<f64>,
    count: u64,
    min: Option<f64>,
    max: Option<f64>,
    pt: CounterPassthrough,
}

/// OTEL histogram aggregator — merges histogram buckets (element-wise sum),
/// sums `sum`/`count`, min-of-mins, max-of-maxes, max timestamp,
/// min start_time. Nullable `sum`/`min`/`max` are propagated correctly:
/// if any input is non-null, the aggregate is non-null.
#[derive(Debug, Clone)]
pub struct OtelHistogramProcessor;

impl OtelHistogramProcessor {
    pub fn new() -> Self {
        Self
    }

    fn aggregate(&self, batches: &[RecordBatch]) -> RecordBatch {
        let mut accum: HashMap<String, HistogramAccum> = HashMap::new();
        let mut key_buf = String::new();

        for batch in batches {
            let keys = extract_keys(batch);
            let ts = get_ts(batch, "timestamp");
            let st = get_ts(batch, "start_time");
            let boundaries_list = get_list(batch, "boundaries");
            let bucket_counts_list = get_list(batch, "bucket_counts");
            let sum_arr = get_f64(batch, "sum");
            let count_arr = get_u64(batch, "count");
            let min_arr = get_f64(batch, "min");
            let max_arr = get_f64(batch, "max");
            let pt_arrays = extract_counter_pt(batch);

            for row in 0..batch.num_rows() {
                build_key(&keys, row, &mut key_buf);

                let row_sum = if sum_arr.is_null(row) {
                    None
                } else {
                    Some(sum_arr.value(row))
                };
                let row_min = if min_arr.is_null(row) {
                    None
                } else {
                    Some(min_arr.value(row))
                };
                let row_max = if max_arr.is_null(row) {
                    None
                } else {
                    Some(max_arr.value(row))
                };

                if let Some(existing) = accum.get_mut(key_buf.as_str()) {
                    // Merge bucket counts
                    let bc_values = bucket_counts_list.value(row);
                    let bc_array = bc_values.as_any().downcast_ref::<UInt64Array>().unwrap();
                    let merge_len = existing.bucket_counts.len().min(bc_array.len());
                    for i in 0..merge_len {
                        existing.bucket_counts[i] += bc_array.value(i);
                    }
                    for i in merge_len..bc_array.len() {
                        existing.bucket_counts.push(bc_array.value(i));
                    }

                    existing.sum = merge_option_sum(existing.sum, row_sum);
                    existing.count += count_arr.value(row);
                    existing.timestamp = existing.timestamp.max(ts.value(row));
                    existing.start_time = existing.start_time.min(st.value(row));
                    existing.min = merge_option_min(existing.min, row_min);
                    existing.max = merge_option_max(existing.max, row_max);
                } else {
                    let b_values = boundaries_list.value(row);
                    let b_array = b_values.as_any().downcast_ref::<Float64Array>().unwrap();
                    let boundaries: Vec<f64> =
                        (0..b_array.len()).map(|i| b_array.value(i)).collect();

                    let bc_values = bucket_counts_list.value(row);
                    let bc_array = bc_values.as_any().downcast_ref::<UInt64Array>().unwrap();
                    let bucket_counts: Vec<u64> =
                        (0..bc_array.len()).map(|i| bc_array.value(i)).collect();

                    accum.insert(
                        key_buf.clone(),
                        HistogramAccum {
                            timestamp: ts.value(row),
                            start_time: st.value(row),
                            boundaries,
                            bucket_counts,
                            sum: row_sum,
                            count: count_arr.value(row),
                            min: row_min,
                            max: row_max,
                            pt: read_counter_pt(&pt_arrays, row),
                        },
                    );
                }
            }
        }

        self.build_output(&accum)
    }

    fn build_output(&self, accum: &HashMap<String, HistogramAccum>) -> RecordBatch {
        let n = accum.len();
        let entries: Vec<(&String, &HistogramAccum)> = accum.iter().collect();

        let mut columns: Vec<ArrayRef> = build_key_columns(entries.iter().map(|(k, _)| k), n);

        // Timestamps
        let mut ts_builder = TimestampNanosecondBuilder::with_capacity(n);
        let mut st_builder = TimestampNanosecondBuilder::with_capacity(n);
        for (_, entry) in &entries {
            ts_builder.append_value(entry.timestamp);
            st_builder.append_value(entry.start_time);
        }
        columns.push(Arc::new(ts_builder.finish()));
        columns.push(Arc::new(st_builder.finish()));

        // Histogram data columns
        let total_buckets: usize = entries.iter().map(|(_, h)| h.bucket_counts.len()).sum();
        let mut boundaries_builder = ListBuilder::new(Float64Builder::with_capacity(total_buckets));
        let mut bucket_counts_builder =
            ListBuilder::new(UInt64Builder::with_capacity(total_buckets));
        let mut sum_builder = Float64Builder::with_capacity(n);
        let mut count_builder = UInt64Builder::with_capacity(n);
        let mut min_builder = Float64Builder::with_capacity(n);
        let mut max_builder = Float64Builder::with_capacity(n);

        for (_, entry) in &entries {
            let b_inner = boundaries_builder.values();
            for &b in &entry.boundaries {
                b_inner.append_value(b);
            }
            boundaries_builder.append(true);

            let bc_inner = bucket_counts_builder.values();
            for &c in &entry.bucket_counts {
                bc_inner.append_value(c);
            }
            bucket_counts_builder.append(true);

            sum_builder.append_option(entry.sum);
            count_builder.append_value(entry.count);
            min_builder.append_option(entry.min);
            max_builder.append_option(entry.max);
        }

        columns.push(Arc::new(boundaries_builder.finish()));
        columns.push(Arc::new(bucket_counts_builder.finish()));
        columns.push(Arc::new(sum_builder.finish()));
        columns.push(Arc::new(count_builder.finish()));
        columns.push(Arc::new(min_builder.finish()));
        columns.push(Arc::new(max_builder.finish()));

        // Passthrough
        columns.extend(build_counter_pt_columns(
            entries.iter().map(|(_, e)| &e.pt),
            n,
        ));

        let table_schema = schema::histogram_schema_ref().clone();
        RecordBatch::try_new(table_schema, columns)
            .expect("schema mismatch in otel histogram output")
    }
}

impl WriteProcessor for OtelHistogramProcessor {
    fn process(&self, batches: Vec<RecordBatch>) -> ProcessorOutput {
        if batches.is_empty() {
            return ProcessorOutput::primary_only(Vec::new());
        }
        ProcessorOutput::primary_only(vec![self.aggregate(&batches)])
    }

    fn descriptor(&self) -> Option<ProcessorDescriptor> {
        Some(ProcessorDescriptor::OtelHistogram)
    }
}

// ===========================================================================
// OtelExpHistogramAggregator
// ===========================================================================

struct ExpHistogramAccum {
    timestamp: i64,
    start_time: i64,
    scale: i32,
    zero_count: u64,
    zero_threshold: f64,
    positive_offset: i32,
    positive_bucket_counts: Vec<u64>,
    negative_offset: i32,
    negative_bucket_counts: Vec<u64>,
    sum: Option<f64>,
    count: u64,
    min: Option<f64>,
    max: Option<f64>,
    pt: CounterPassthrough,
}

/// OTEL exponential histogram aggregator — merges exponential histogram
/// data points per group key. Sums `count`/`sum`/`zero_count`, element-wise
/// sums bucket counts, min-of-mins, max-of-maxes, max timestamp,
/// min start_time.
///
/// Assumes all data points within a group share the same `scale` and offsets.
/// If scales differ, the first row's scale wins (correct merging across
/// scales requires re-bucketing which is out of scope for pre-aggregation).
#[derive(Debug, Clone)]
pub struct OtelExpHistogramAggregator;

impl OtelExpHistogramAggregator {
    pub fn new() -> Self {
        Self
    }

    fn aggregate(&self, batches: &[RecordBatch]) -> RecordBatch {
        let mut accum: HashMap<String, ExpHistogramAccum> = HashMap::new();
        let mut key_buf = String::new();

        for batch in batches {
            let keys = extract_keys(batch);
            let ts = get_ts(batch, "timestamp");
            let st = get_ts(batch, "start_time");
            let scale_arr = get_i32(batch, "scale");
            let zero_count_arr = get_u64(batch, "zero_count");
            let zero_threshold_arr = get_f64(batch, "zero_threshold");
            let pos_offset_arr = get_i32(batch, "positive_offset");
            let pos_counts_list = get_list(batch, "positive_bucket_counts");
            let neg_offset_arr = get_i32(batch, "negative_offset");
            let neg_counts_list = get_list(batch, "negative_bucket_counts");
            let sum_arr = get_f64(batch, "sum");
            let count_arr = get_u64(batch, "count");
            let min_arr = get_f64(batch, "min");
            let max_arr = get_f64(batch, "max");
            let pt_arrays = extract_counter_pt(batch);

            for row in 0..batch.num_rows() {
                build_key(&keys, row, &mut key_buf);

                let row_sum = if sum_arr.is_null(row) {
                    None
                } else {
                    Some(sum_arr.value(row))
                };
                let row_min = if min_arr.is_null(row) {
                    None
                } else {
                    Some(min_arr.value(row))
                };
                let row_max = if max_arr.is_null(row) {
                    None
                } else {
                    Some(max_arr.value(row))
                };

                if let Some(existing) = accum.get_mut(key_buf.as_str()) {
                    // Merge positive bucket counts (element-wise sum)
                    let pc_values = pos_counts_list.value(row);
                    let pc_array = pc_values.as_any().downcast_ref::<UInt64Array>().unwrap();
                    let merge_len = existing.positive_bucket_counts.len().min(pc_array.len());
                    for i in 0..merge_len {
                        existing.positive_bucket_counts[i] += pc_array.value(i);
                    }
                    for i in merge_len..pc_array.len() {
                        existing.positive_bucket_counts.push(pc_array.value(i));
                    }

                    // Merge negative bucket counts (element-wise sum)
                    let nc_values = neg_counts_list.value(row);
                    let nc_array = nc_values.as_any().downcast_ref::<UInt64Array>().unwrap();
                    let merge_len = existing.negative_bucket_counts.len().min(nc_array.len());
                    for i in 0..merge_len {
                        existing.negative_bucket_counts[i] += nc_array.value(i);
                    }
                    for i in merge_len..nc_array.len() {
                        existing.negative_bucket_counts.push(nc_array.value(i));
                    }

                    existing.zero_count += zero_count_arr.value(row);
                    existing.sum = merge_option_sum(existing.sum, row_sum);
                    existing.count += count_arr.value(row);
                    existing.timestamp = existing.timestamp.max(ts.value(row));
                    existing.start_time = existing.start_time.min(st.value(row));
                    existing.min = merge_option_min(existing.min, row_min);
                    existing.max = merge_option_max(existing.max, row_max);
                } else {
                    let pc_values = pos_counts_list.value(row);
                    let pc_array = pc_values.as_any().downcast_ref::<UInt64Array>().unwrap();
                    let positive_bucket_counts: Vec<u64> =
                        (0..pc_array.len()).map(|i| pc_array.value(i)).collect();

                    let nc_values = neg_counts_list.value(row);
                    let nc_array = nc_values.as_any().downcast_ref::<UInt64Array>().unwrap();
                    let negative_bucket_counts: Vec<u64> =
                        (0..nc_array.len()).map(|i| nc_array.value(i)).collect();

                    accum.insert(
                        key_buf.clone(),
                        ExpHistogramAccum {
                            timestamp: ts.value(row),
                            start_time: st.value(row),
                            scale: scale_arr.value(row),
                            zero_count: zero_count_arr.value(row),
                            zero_threshold: zero_threshold_arr.value(row),
                            positive_offset: pos_offset_arr.value(row),
                            positive_bucket_counts,
                            negative_offset: neg_offset_arr.value(row),
                            negative_bucket_counts,
                            sum: row_sum,
                            count: count_arr.value(row),
                            min: row_min,
                            max: row_max,
                            pt: read_counter_pt(&pt_arrays, row),
                        },
                    );
                }
            }
        }

        self.build_output(&accum)
    }

    fn build_output(&self, accum: &HashMap<String, ExpHistogramAccum>) -> RecordBatch {
        let n = accum.len();
        let entries: Vec<(&String, &ExpHistogramAccum)> = accum.iter().collect();

        let mut columns: Vec<ArrayRef> = build_key_columns(entries.iter().map(|(k, _)| k), n);

        // Timestamps
        let mut ts_builder = TimestampNanosecondBuilder::with_capacity(n);
        let mut st_builder = TimestampNanosecondBuilder::with_capacity(n);
        for (_, entry) in &entries {
            ts_builder.append_value(entry.timestamp);
            st_builder.append_value(entry.start_time);
        }
        columns.push(Arc::new(ts_builder.finish()));
        columns.push(Arc::new(st_builder.finish()));

        // Exp histogram data columns
        let mut scale_builder = Int32Builder::with_capacity(n);
        let mut zero_count_builder = UInt64Builder::with_capacity(n);
        let mut zero_threshold_builder = Float64Builder::with_capacity(n);
        let mut pos_offset_builder = Int32Builder::with_capacity(n);
        let total_pos_buckets: usize = entries
            .iter()
            .map(|(_, e)| e.positive_bucket_counts.len())
            .sum();
        let mut pos_counts_builder =
            ListBuilder::new(UInt64Builder::with_capacity(total_pos_buckets));
        let mut neg_offset_builder = Int32Builder::with_capacity(n);
        let total_neg_buckets: usize = entries
            .iter()
            .map(|(_, e)| e.negative_bucket_counts.len())
            .sum();
        let mut neg_counts_builder =
            ListBuilder::new(UInt64Builder::with_capacity(total_neg_buckets));
        let mut sum_builder = Float64Builder::with_capacity(n);
        let mut count_builder = UInt64Builder::with_capacity(n);
        let mut min_builder = Float64Builder::with_capacity(n);
        let mut max_builder = Float64Builder::with_capacity(n);

        for (_, entry) in &entries {
            scale_builder.append_value(entry.scale);
            zero_count_builder.append_value(entry.zero_count);
            zero_threshold_builder.append_value(entry.zero_threshold);

            pos_offset_builder.append_value(entry.positive_offset);
            let pc_inner = pos_counts_builder.values();
            for &c in &entry.positive_bucket_counts {
                pc_inner.append_value(c);
            }
            pos_counts_builder.append(true);

            neg_offset_builder.append_value(entry.negative_offset);
            let nc_inner = neg_counts_builder.values();
            for &c in &entry.negative_bucket_counts {
                nc_inner.append_value(c);
            }
            neg_counts_builder.append(true);

            sum_builder.append_option(entry.sum);
            count_builder.append_value(entry.count);
            min_builder.append_option(entry.min);
            max_builder.append_option(entry.max);
        }

        columns.push(Arc::new(scale_builder.finish()));
        columns.push(Arc::new(zero_count_builder.finish()));
        columns.push(Arc::new(zero_threshold_builder.finish()));
        columns.push(Arc::new(pos_offset_builder.finish()));
        columns.push(Arc::new(pos_counts_builder.finish()));
        columns.push(Arc::new(neg_offset_builder.finish()));
        columns.push(Arc::new(neg_counts_builder.finish()));
        columns.push(Arc::new(sum_builder.finish()));
        columns.push(Arc::new(count_builder.finish()));
        columns.push(Arc::new(min_builder.finish()));
        columns.push(Arc::new(max_builder.finish()));

        // Passthrough
        columns.extend(build_counter_pt_columns(
            entries.iter().map(|(_, e)| &e.pt),
            n,
        ));

        let table_schema = schema::exp_histogram_schema_ref().clone();
        RecordBatch::try_new(table_schema, columns)
            .expect("schema mismatch in otel exp histogram output")
    }
}

impl WriteProcessor for OtelExpHistogramAggregator {
    fn process(&self, batches: Vec<RecordBatch>) -> ProcessorOutput {
        if batches.is_empty() {
            return ProcessorOutput::primary_only(Vec::new());
        }
        ProcessorOutput::primary_only(vec![self.aggregate(&batches)])
    }

    fn descriptor(&self) -> Option<ProcessorDescriptor> {
        Some(ProcessorDescriptor::OtelExpHistogram)
    }
}

// ---------------------------------------------------------------------------
// Nullable Option<f64> merge helpers for histogram sum/min/max
// ---------------------------------------------------------------------------

/// Merge two optional sums: if either is non-null, the result is non-null.
fn merge_option_sum(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x + y),
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (None, None) => None,
    }
}

/// Merge two optional mins: if either is non-null, take the minimum.
fn merge_option_min(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (None, None) => None,
    }
}

/// Merge two optional maxes: if either is non-null, take the maximum.
fn merge_option_max(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (None, None) => None,
    }
}

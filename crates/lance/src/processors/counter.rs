//! Counter aggregator — sums values per group key.
//!
//! Designed for metrics counters where multiple samples with the same
//! key columns should be reduced to a single summed row per linger window.
//!
//! # Example
//!
//! ```text
//! Input (10 rows):
//!   metric_name="cpu", value=1.0
//!   metric_name="cpu", value=2.0
//!   metric_name="mem", value=5.0
//!   ...
//!
//! Output (2 rows):
//!   metric_name="cpu", value=3.0
//!   metric_name="mem", value=5.0
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    Float64Builder, StringBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
};
use arrow_array::{
    ArrayRef, Float64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::types::{ProcessorDescriptor, time_unit_to_string};
use crate::write_processor::{ProcessorOutput, WriteProcessor};

/// Accumulated value for a counter group.
struct CounterAccum {
    /// Truncated timestamp (in the configured unit), if timestamp is enabled.
    timestamp: Option<i64>,
    /// Minimum start_time seen, if start_time column is enabled.
    start_time: Option<i64>,
    /// Accumulated sum.
    sum: f64,
}

/// Stateless counter aggregator — groups by key columns and sums a Float64
/// value column. Optionally truncates a timestamp column to a configurable
/// resolution.
#[derive(Debug, Clone)]
pub struct CounterAggregator {
    /// Column names that form the group key (must be Utf8).
    key_columns: Vec<String>,
    /// Column name containing the counter value (must be Float64).
    value_column: String,
    /// Optional timestamp column name.
    timestamp_column: Option<String>,
    /// Resolution in milliseconds for timestamp truncation.
    /// E.g., 60_000 truncates to the minute boundary.
    timestamp_resolution_ms: i64,
    /// Time unit for the timestamp column.
    timestamp_unit: TimeUnit,
    /// Optional start_time column — aggregated as min(start_time) per group.
    start_time_column: Option<String>,
}

impl CounterAggregator {
    /// Create a new counter aggregator.
    ///
    /// - `key_columns`: column names forming the group key (must be Utf8)
    /// - `value_column`: column name with counter values (must be Float64)
    pub fn new(key_columns: Vec<String>, value_column: impl Into<String>) -> Self {
        Self {
            key_columns,
            value_column: value_column.into(),
            timestamp_column: None,
            timestamp_resolution_ms: 60_000,
            timestamp_unit: TimeUnit::Millisecond,
            start_time_column: None,
        }
    }

    /// Add timestamp truncation. The timestamp column will be floored to
    /// the given resolution and included in the group key.
    pub fn with_timestamp(mut self, column: impl Into<String>, resolution_ms: i64) -> Self {
        self.timestamp_column = Some(column.into());
        self.timestamp_resolution_ms = resolution_ms;
        self
    }

    /// Set the time unit for the timestamp column (default: Millisecond).
    pub fn with_timestamp_unit(mut self, unit: TimeUnit) -> Self {
        self.timestamp_unit = unit;
        self
    }

    /// Add a start_time column. During aggregation, the minimum start_time
    /// across all rows in a group is preserved.
    pub fn with_start_time(mut self, column: impl Into<String>) -> Self {
        self.start_time_column = Some(column.into());
        self
    }

    /// Resolution converted to the configured timestamp unit.
    fn resolution_in_unit(&self) -> i64 {
        match self.timestamp_unit {
            TimeUnit::Second => self.timestamp_resolution_ms / 1_000,
            TimeUnit::Millisecond => self.timestamp_resolution_ms,
            TimeUnit::Microsecond => self.timestamp_resolution_ms * 1_000,
            TimeUnit::Nanosecond => self.timestamp_resolution_ms * 1_000_000,
        }
    }

    fn output_schema(&self) -> Schema {
        let mut fields: Vec<Field> = self
            .key_columns
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, false))
            .collect();

        if let Some(ts_col) = &self.timestamp_column {
            fields.push(Field::new(
                ts_col,
                DataType::Timestamp(self.timestamp_unit, None),
                false,
            ));
        }

        if let Some(st_col) = &self.start_time_column {
            fields.push(Field::new(
                st_col,
                DataType::Timestamp(self.timestamp_unit, None),
                false,
            ));
        }

        fields.push(Field::new(&self.value_column, DataType::Float64, false));
        Schema::new(fields)
    }

    fn aggregate(&self, batches: &[RecordBatch]) -> RecordBatch {
        // Use a single joined String key (null-byte separated) instead of Vec<String>.
        // The timestamp is stored separately in the accumulator, not stringified into the key.
        let mut accum: HashMap<String, CounterAccum> = HashMap::new();
        let resolution = self.resolution_in_unit();
        let num_key_cols = self.key_columns.len();

        // Reusable buffer for building composite keys.
        let mut key_buf = String::new();

        for batch in batches {
            let num_rows = batch.num_rows();

            let key_arrays: Vec<&StringArray> = self
                .key_columns
                .iter()
                .map(|name| {
                    batch
                        .column_by_name(name)
                        .unwrap_or_else(|| panic!("key column '{}' not found", name))
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap_or_else(|| panic!("key column '{}' must be Utf8", name))
                })
                .collect();

            let ts_values = self
                .timestamp_column
                .as_ref()
                .map(|name| extract_timestamp_array(batch, name, self.timestamp_unit));

            let st_values = self
                .start_time_column
                .as_ref()
                .map(|name| extract_timestamp_array(batch, name, self.timestamp_unit));

            let value_array = batch
                .column_by_name(&self.value_column)
                .unwrap_or_else(|| panic!("value column '{}' not found", self.value_column))
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap_or_else(|| panic!("value column '{}' must be Float64", self.value_column));

            for row in 0..num_rows {
                // Build composite key: "col0\0col1\0...\0truncated_ts"
                key_buf.clear();
                for (i, arr) in key_arrays.iter().enumerate() {
                    if i > 0 {
                        key_buf.push('\0');
                    }
                    key_buf.push_str(arr.value(row));
                }

                let truncated_ts = ts_values.as_ref().map(|ts| {
                    let ts_val = ts[row];
                    let truncated = (ts_val / resolution) * resolution;
                    // Include truncated timestamp in the key for grouping.
                    key_buf.push('\0');
                    // Use fixed-width encoding to avoid ambiguity.
                    let _ = std::fmt::Write::write_fmt(&mut key_buf, format_args!("{truncated}"));
                    truncated
                });

                let st = st_values.as_ref().map(|st| st[row]);

                if let Some(entry) = accum.get_mut(key_buf.as_str()) {
                    entry.sum += value_array.value(row);
                    // Keep minimum start_time across group.
                    if let Some(new_st) = st {
                        entry.start_time =
                            Some(entry.start_time.map_or(new_st, |old| old.min(new_st)));
                    }
                } else {
                    accum.insert(
                        key_buf.clone(),
                        CounterAccum {
                            timestamp: truncated_ts,
                            start_time: st,
                            sum: value_array.value(row),
                        },
                    );
                }
            }
        }

        self.build_output(&accum, num_key_cols)
    }

    fn build_output(
        &self,
        accum: &HashMap<String, CounterAccum>,
        num_key_cols: usize,
    ) -> RecordBatch {
        let num_groups = accum.len();
        let has_ts = self.timestamp_column.is_some();
        let has_st = self.start_time_column.is_some();

        let mut key_builders: Vec<StringBuilder> = (0..num_key_cols)
            .map(|_| StringBuilder::with_capacity(num_groups, num_groups * 32))
            .collect();
        let mut value_builder = Float64Builder::with_capacity(num_groups);
        let mut ts_values: Vec<i64> = if has_ts {
            Vec::with_capacity(num_groups)
        } else {
            Vec::new()
        };
        let mut st_values: Vec<i64> = if has_st {
            Vec::with_capacity(num_groups)
        } else {
            Vec::new()
        };

        for (composite_key, entry) in accum {
            let mut parts = composite_key.split('\0');
            for builder in key_builders.iter_mut() {
                builder.append_value(parts.next().unwrap());
            }
            if let Some(ts) = entry.timestamp {
                ts_values.push(ts);
            }
            if let Some(st) = entry.start_time {
                st_values.push(st);
            }
            value_builder.append_value(entry.sum);
        }

        let schema = Arc::new(self.output_schema());
        let mut columns: Vec<ArrayRef> = key_builders
            .into_iter()
            .map(|mut b| Arc::new(b.finish()) as ArrayRef)
            .collect();

        if has_ts {
            columns.push(build_timestamp_array(
                &ts_values,
                self.timestamp_unit,
                num_groups,
            ));
        }

        if has_st {
            columns.push(build_timestamp_array(
                &st_values,
                self.timestamp_unit,
                num_groups,
            ));
        }

        columns.push(Arc::new(value_builder.finish()) as ArrayRef);

        RecordBatch::try_new(schema, columns).expect("schema mismatch in counter aggregation")
    }
}

impl WriteProcessor for CounterAggregator {
    fn process(&self, batches: Vec<RecordBatch>) -> ProcessorOutput {
        if batches.is_empty() {
            return ProcessorOutput::primary_only(Vec::new());
        }
        let aggregated = self.aggregate(&batches);
        ProcessorOutput::primary_only(vec![aggregated])
    }

    fn descriptor(&self) -> Option<ProcessorDescriptor> {
        Some(ProcessorDescriptor::Counter {
            key_columns: self.key_columns.clone(),
            value_column: self.value_column.clone(),
            timestamp_column: self.timestamp_column.clone(),
            timestamp_resolution_ms: self.timestamp_resolution_ms,
            timestamp_unit: time_unit_to_string(self.timestamp_unit),
            start_time_column: self.start_time_column.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// Shared timestamp helpers (used by counter, gauge, histogram)
// ---------------------------------------------------------------------------

/// Extract all timestamp values from a batch column as a slice of i64.
pub(crate) fn extract_timestamp_array<'a>(
    batch: &'a RecordBatch,
    name: &str,
    unit: TimeUnit,
) -> &'a [i64] {
    let col = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("timestamp column '{}' not found", name));

    match unit {
        TimeUnit::Millisecond => col
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap_or_else(|| panic!("timestamp column '{}' must be TimestampMillisecond", name))
            .values(),
        TimeUnit::Nanosecond => col
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap_or_else(|| panic!("timestamp column '{}' must be TimestampNanosecond", name))
            .values(),
        other => panic!("unsupported timestamp unit: {:?}", other),
    }
}

/// Build a timestamp ArrayRef from collected i64 values.
pub(crate) fn build_timestamp_array(values: &[i64], unit: TimeUnit, capacity: usize) -> ArrayRef {
    match unit {
        TimeUnit::Millisecond => {
            let mut builder = TimestampMillisecondBuilder::with_capacity(capacity);
            for &v in values {
                builder.append_value(v);
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TimeUnit::Nanosecond => {
            let mut builder = TimestampNanosecondBuilder::with_capacity(capacity);
            for &v in values {
                builder.append_value(v);
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        other => panic!("unsupported timestamp unit: {:?}", other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counter_schema() -> Schema {
        Schema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])
    }

    fn make_counter_batch(names: &[&str], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(counter_schema());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    names.iter().map(|s| *s).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn sums_by_key() {
        let agg = CounterAggregator::new(vec!["metric_name".to_string()], "value");

        let batch = make_counter_batch(
            &["cpu", "cpu", "mem", "cpu", "mem"],
            &[1.0, 2.0, 5.0, 3.0, 10.0],
        );

        let output = agg.process(vec![batch]);
        assert!(output.materialized.is_empty());
        assert_eq!(output.primary.len(), 1);

        let result = &output.primary[0];
        assert_eq!(result.num_rows(), 2);

        let names = result
            .column_by_name("metric_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = result
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let mut results: HashMap<String, f64> = HashMap::new();
        for i in 0..result.num_rows() {
            results.insert(names.value(i).to_string(), values.value(i));
        }

        assert_eq!(results["cpu"], 6.0);
        assert_eq!(results["mem"], 15.0);
    }

    #[test]
    fn multiple_batches() {
        let agg = CounterAggregator::new(vec!["metric_name".to_string()], "value");

        let b1 = make_counter_batch(&["cpu", "mem"], &[1.0, 2.0]);
        let b2 = make_counter_batch(&["cpu", "disk"], &[3.0, 4.0]);

        let output = agg.process(vec![b1, b2]);
        let result = &output.primary[0];
        assert_eq!(result.num_rows(), 3);

        let names = result
            .column_by_name("metric_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = result
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let mut results: HashMap<String, f64> = HashMap::new();
        for i in 0..result.num_rows() {
            results.insert(names.value(i).to_string(), values.value(i));
        }

        assert_eq!(results["cpu"], 4.0);
        assert_eq!(results["mem"], 2.0);
        assert_eq!(results["disk"], 4.0);
    }

    #[test]
    fn empty_input() {
        let agg = CounterAggregator::new(vec!["metric_name".to_string()], "value");
        let output = agg.process(Vec::new());
        assert!(output.primary.is_empty());
        assert!(output.materialized.is_empty());
    }

    #[test]
    fn with_timestamp_truncation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["cpu", "cpu", "cpu"])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    60_000,  // :00 of minute 1
                    75_000,  // :15 of minute 1
                    120_000, // :00 of minute 2
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 5.0])),
            ],
        )
        .unwrap();

        let agg = CounterAggregator::new(vec!["metric_name".to_string()], "value")
            .with_timestamp("timestamp", 60_000);

        let output = agg.process(vec![batch]);
        let result = &output.primary[0];
        // minute 1: 1.0 + 2.0 = 3.0, minute 2: 5.0
        assert_eq!(result.num_rows(), 2);
    }
}

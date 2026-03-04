//! Gauge aggregator — keeps the last value per group key.
//!
//! For gauge metrics, only the most recent value matters within a linger window.
//! This aggregator implements last-write-wins semantics: the last row seen for
//! each unique group key overwrites any previous values.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float64Builder, StringBuilder};
use arrow_array::{ArrayRef, Float64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use super::counter::{build_timestamp_array, extract_timestamp_array};
use crate::types::{ProcessorDescriptor, time_unit_to_string};
use crate::write_processor::{ProcessorOutput, WriteProcessor};

/// Accumulated state for a gauge group.
struct GaugeAccum {
    /// Last-seen timestamp (in the configured unit), if timestamp is enabled.
    timestamp: Option<i64>,
    /// Last-seen value.
    value: f64,
}

/// Stateless gauge aggregator — groups by key columns and keeps the last
/// Float64 value per group. Optionally preserves a timestamp column.
#[derive(Debug, Clone)]
pub struct GaugeAggregator {
    /// Column names that form the group key (must be Utf8).
    key_columns: Vec<String>,
    /// Column name containing the gauge value (must be Float64).
    value_column: String,
    /// Optional timestamp column name.
    timestamp_column: Option<String>,
    /// Time unit for the timestamp column.
    timestamp_unit: TimeUnit,
}

impl GaugeAggregator {
    /// Create a new gauge aggregator.
    ///
    /// - `key_columns`: column names forming the group key (must be Utf8)
    /// - `value_column`: column name with gauge values (must be Float64)
    pub fn new(key_columns: Vec<String>, value_column: impl Into<String>) -> Self {
        Self {
            key_columns,
            value_column: value_column.into(),
            timestamp_column: None,
            timestamp_unit: TimeUnit::Millisecond,
        }
    }

    /// Include a timestamp column in the output. The timestamp from the last
    /// write for each group key is preserved.
    pub fn with_timestamp(mut self, column: impl Into<String>) -> Self {
        self.timestamp_column = Some(column.into());
        self
    }

    /// Set the time unit for the timestamp column (default: Millisecond).
    pub fn with_timestamp_unit(mut self, unit: TimeUnit) -> Self {
        self.timestamp_unit = unit;
        self
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

        fields.push(Field::new(&self.value_column, DataType::Float64, false));
        Schema::new(fields)
    }

    fn aggregate(&self, batches: &[RecordBatch]) -> RecordBatch {
        let mut accum: HashMap<String, GaugeAccum> = HashMap::new();
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

            let ts_values = self.timestamp_column.as_ref().map(|name| {
                extract_timestamp_array(batch, name, self.timestamp_unit)
            });

            let value_array = batch
                .column_by_name(&self.value_column)
                .unwrap_or_else(|| panic!("value column '{}' not found", self.value_column))
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap_or_else(|| {
                    panic!("value column '{}' must be Float64", self.value_column)
                });

            for row in 0..num_rows {
                // Build composite key: "col0\0col1\0..."
                key_buf.clear();
                for (i, arr) in key_arrays.iter().enumerate() {
                    if i > 0 {
                        key_buf.push('\0');
                    }
                    key_buf.push_str(arr.value(row));
                }

                let ts = ts_values.as_ref().map(|ts| ts[row]);
                let value = value_array.value(row);

                // Last-write-wins: overwrite on hit, avoiding key clone.
                if let Some(entry) = accum.get_mut(key_buf.as_str()) {
                    entry.value = value;
                    entry.timestamp = ts;
                } else {
                    accum.insert(
                        key_buf.clone(),
                        GaugeAccum {
                            timestamp: ts,
                            value,
                        },
                    );
                }
            }
        }

        self.build_output(&accum, num_key_cols)
    }

    fn build_output(&self, accum: &HashMap<String, GaugeAccum>, num_key_cols: usize) -> RecordBatch {
        let num_groups = accum.len();
        let has_ts = self.timestamp_column.is_some();

        let mut key_builders: Vec<StringBuilder> = (0..num_key_cols)
            .map(|_| StringBuilder::with_capacity(num_groups, num_groups * 32))
            .collect();
        let mut value_builder = Float64Builder::with_capacity(num_groups);
        let mut ts_values: Vec<i64> = if has_ts {
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
            value_builder.append_value(entry.value);
        }

        let schema = Arc::new(self.output_schema());
        let mut columns: Vec<ArrayRef> = key_builders
            .into_iter()
            .map(|mut b| Arc::new(b.finish()) as ArrayRef)
            .collect();

        if has_ts {
            columns.push(build_timestamp_array(&ts_values, self.timestamp_unit, num_groups));
        }

        columns.push(Arc::new(value_builder.finish()) as ArrayRef);

        RecordBatch::try_new(schema, columns).expect("schema mismatch in gauge aggregation")
    }
}

impl WriteProcessor for GaugeAggregator {
    fn process(&self, batches: Vec<RecordBatch>) -> ProcessorOutput {
        if batches.is_empty() {
            return ProcessorOutput::primary_only(Vec::new());
        }
        let aggregated = self.aggregate(&batches);
        ProcessorOutput::primary_only(vec![aggregated])
    }

    fn descriptor(&self) -> Option<ProcessorDescriptor> {
        Some(ProcessorDescriptor::Gauge {
            key_columns: self.key_columns.clone(),
            value_column: self.value_column.clone(),
            timestamp_column: self.timestamp_column.clone(),
            timestamp_unit: time_unit_to_string(self.timestamp_unit),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gauge_schema() -> Schema {
        Schema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])
    }

    fn make_gauge_batch(names: &[&str], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(gauge_schema());
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
    fn keeps_last_value() {
        let agg = GaugeAggregator::new(vec!["metric_name".to_string()], "value");

        let batch = make_gauge_batch(
            &["cpu", "cpu", "mem", "cpu", "mem"],
            &[1.0, 2.0, 5.0, 3.0, 10.0],
        );

        let output = agg.process(vec![batch]);
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

        // Last value for cpu=3.0, mem=10.0
        assert_eq!(results["cpu"], 3.0);
        assert_eq!(results["mem"], 10.0);
    }

    #[test]
    fn multiple_batches_last_wins() {
        let agg = GaugeAggregator::new(vec!["metric_name".to_string()], "value");

        let b1 = make_gauge_batch(&["cpu", "mem"], &[1.0, 2.0]);
        let b2 = make_gauge_batch(&["cpu", "mem"], &[99.0, 88.0]);

        let output = agg.process(vec![b1, b2]);
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

        assert_eq!(results["cpu"], 99.0);
        assert_eq!(results["mem"], 88.0);
    }

    #[test]
    fn empty_input() {
        let agg = GaugeAggregator::new(vec!["metric_name".to_string()], "value");
        let output = agg.process(Vec::new());
        assert!(output.primary.is_empty());
    }
}

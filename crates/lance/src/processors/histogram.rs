//! Histogram aggregator — merges histogram bucket data per group key.
//!
//! Expected schema:
//! - Key columns: Utf8
//! - `boundaries`: `List<Float64>` — histogram bucket boundaries
//! - `bucket_counts`: `List<UInt64>` — counts per bucket
//! - `sum`: `Float64` — total sum of observed values
//! - `count`: `UInt64` — total number of observations
//!
//! Merge strategy: element-wise sum of bucket counts, sum of sum and count.
//! Boundary arrays are assumed identical for same-key rows (standard for
//! OpenTelemetry/Prometheus histograms).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Float64Builder, ListBuilder, StringBuilder, UInt64Builder};
use arrow_array::{
    ArrayRef, Float64Array, ListArray, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use super::counter::{build_timestamp_array, extract_timestamp_array};
use crate::types::{ProcessorDescriptor, time_unit_to_string};
use crate::write_processor::{ProcessorOutput, WriteProcessor};

/// Accumulated histogram state for a single group key.
struct HistogramAccum {
    /// Bucket boundaries (stored once per key — assumed identical for same key).
    boundaries: Vec<f64>,
    /// Merged bucket counts (element-wise sum).
    bucket_counts: Vec<u64>,
    /// Accumulated sum of observed values.
    sum: f64,
    /// Accumulated total count.
    count: u64,
    /// Latest (max) timestamp, if timestamp is enabled.
    timestamp: Option<i64>,
    /// Minimum start_time across all rows in the group.
    start_time: Option<i64>,
    /// Minimum of all min values in the group.
    min: Option<f64>,
    /// Maximum of all max values in the group.
    max: Option<f64>,
}

/// Stateless histogram aggregator — groups by key columns and merges
/// histogram bucket data (counts, sum, count) per group.
#[derive(Debug, Clone)]
pub struct HistogramAggregator {
    /// Column names that form the group key (must be Utf8).
    key_columns: Vec<String>,
    /// Column name for bucket boundaries (must be `List<Float64>`).
    boundaries_column: String,
    /// Column name for bucket counts (must be `List<UInt64>`).
    bucket_counts_column: String,
    /// Column name for sum (must be Float64).
    sum_column: String,
    /// Column name for count (must be UInt64).
    count_column: String,
    /// Optional timestamp column name.
    timestamp_column: Option<String>,
    /// Time unit for the timestamp column.
    timestamp_unit: TimeUnit,
    /// Optional start_time column — aggregated as min(start_time) per group.
    start_time_column: Option<String>,
    /// Optional min column — aggregated as min(all mins) per group.
    min_column: Option<String>,
    /// Optional max column — aggregated as max(all maxes) per group.
    max_column: Option<String>,
}

impl HistogramAggregator {
    /// Create a new histogram aggregator with default column names.
    ///
    /// Default column names: `boundaries`, `bucket_counts`, `sum`, `count`.
    pub fn new(key_columns: Vec<String>) -> Self {
        Self {
            key_columns,
            boundaries_column: "boundaries".to_string(),
            bucket_counts_column: "bucket_counts".to_string(),
            sum_column: "sum".to_string(),
            count_column: "count".to_string(),
            timestamp_column: None,
            timestamp_unit: TimeUnit::Millisecond,
            start_time_column: None,
            min_column: None,
            max_column: None,
        }
    }

    /// Override the default column names for histogram data.
    pub fn with_column_names(
        mut self,
        boundaries: impl Into<String>,
        bucket_counts: impl Into<String>,
        sum: impl Into<String>,
        count: impl Into<String>,
    ) -> Self {
        self.boundaries_column = boundaries.into();
        self.bucket_counts_column = bucket_counts.into();
        self.sum_column = sum.into();
        self.count_column = count.into();
        self
    }

    /// Include a timestamp column in the output. When merging histograms,
    /// the latest (max) timestamp is preserved.
    pub fn with_timestamp(mut self, column: impl Into<String>) -> Self {
        self.timestamp_column = Some(column.into());
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

    /// Add a min column. During aggregation, the minimum value across all
    /// rows in a group is preserved.
    pub fn with_min_column(mut self, column: impl Into<String>) -> Self {
        self.min_column = Some(column.into());
        self
    }

    /// Add a max column. During aggregation, the maximum value across all
    /// rows in a group is preserved.
    pub fn with_max_column(mut self, column: impl Into<String>) -> Self {
        self.max_column = Some(column.into());
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

        if let Some(st_col) = &self.start_time_column {
            fields.push(Field::new(
                st_col,
                DataType::Timestamp(self.timestamp_unit, None),
                false,
            ));
        }

        fields.push(Field::new(
            &self.boundaries_column,
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            false,
        ));
        fields.push(Field::new(
            &self.bucket_counts_column,
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            false,
        ));
        fields.push(Field::new(&self.sum_column, DataType::Float64, false));
        fields.push(Field::new(&self.count_column, DataType::UInt64, false));

        if let Some(min_col) = &self.min_column {
            fields.push(Field::new(min_col, DataType::Float64, false));
        }
        if let Some(max_col) = &self.max_column {
            fields.push(Field::new(max_col, DataType::Float64, false));
        }

        Schema::new(fields)
    }

    fn aggregate(&self, batches: &[RecordBatch]) -> RecordBatch {
        let mut accum: HashMap<String, HistogramAccum> = HashMap::new();
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

            let boundaries_list = batch
                .column_by_name(&self.boundaries_column)
                .unwrap_or_else(|| {
                    panic!("boundaries column '{}' not found", self.boundaries_column)
                })
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap_or_else(|| {
                    panic!(
                        "boundaries column '{}' must be List<Float64>",
                        self.boundaries_column
                    )
                });

            let bucket_counts_list = batch
                .column_by_name(&self.bucket_counts_column)
                .unwrap_or_else(|| {
                    panic!(
                        "bucket_counts column '{}' not found",
                        self.bucket_counts_column
                    )
                })
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap_or_else(|| {
                    panic!(
                        "bucket_counts column '{}' must be List<UInt64>",
                        self.bucket_counts_column
                    )
                });

            let sum_array = batch
                .column_by_name(&self.sum_column)
                .unwrap_or_else(|| panic!("sum column '{}' not found", self.sum_column))
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap_or_else(|| panic!("sum column '{}' must be Float64", self.sum_column));

            let count_array = batch
                .column_by_name(&self.count_column)
                .unwrap_or_else(|| panic!("count column '{}' not found", self.count_column))
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap_or_else(|| panic!("count column '{}' must be UInt64", self.count_column));

            let ts_values = self.timestamp_column.as_ref().map(|name| {
                extract_timestamp_array(batch, name, self.timestamp_unit)
            });

            let st_values = self.start_time_column.as_ref().map(|name| {
                extract_timestamp_array(batch, name, self.timestamp_unit)
            });

            let min_array = self.min_column.as_ref().map(|name| {
                batch
                    .column_by_name(name)
                    .unwrap_or_else(|| panic!("min column '{}' not found", name))
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap_or_else(|| panic!("min column '{}' must be Float64", name))
            });

            let max_array = self.max_column.as_ref().map(|name| {
                batch
                    .column_by_name(name)
                    .unwrap_or_else(|| panic!("max column '{}' not found", name))
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap_or_else(|| panic!("max column '{}' must be Float64", name))
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
                let st = st_values.as_ref().map(|st| st[row]);
                let sum = sum_array.value(row);
                let count = count_array.value(row);
                let row_min = min_array.as_ref().map(|a| a.value(row));
                let row_max = max_array.as_ref().map(|a| a.value(row));

                // Last-write-wins for key lookup; merge on hit.
                if let Some(existing) = accum.get_mut(key_buf.as_str()) {
                    // Merge bucket counts directly from Arrow array — no intermediate Vec.
                    let bc_values = bucket_counts_list.value(row);
                    let bc_array = bc_values
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    let merge_len = existing.bucket_counts.len().min(bc_array.len());
                    for i in 0..merge_len {
                        existing.bucket_counts[i] += bc_array.value(i);
                    }
                    // If incoming has more buckets, extend.
                    for i in merge_len..bc_array.len() {
                        existing.bucket_counts.push(bc_array.value(i));
                    }

                    existing.sum += sum;
                    existing.count += count;

                    // Keep the latest (max) timestamp.
                    if let Some(new_ts) = ts {
                        existing.timestamp =
                            Some(existing.timestamp.map_or(new_ts, |old| old.max(new_ts)));
                    }
                    // Keep the earliest (min) start_time.
                    if let Some(new_st) = st {
                        existing.start_time =
                            Some(existing.start_time.map_or(new_st, |old| old.min(new_st)));
                    }
                    // Aggregate min (take minimum).
                    if let Some(v) = row_min {
                        existing.min =
                            Some(existing.min.map_or(v, |old| old.min(v)));
                    }
                    // Aggregate max (take maximum).
                    if let Some(v) = row_max {
                        existing.max =
                            Some(existing.max.map_or(v, |old| old.max(v)));
                    }
                } else {
                    // First occurrence: extract boundaries and bucket counts.
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
                            boundaries,
                            bucket_counts,
                            sum,
                            count,
                            timestamp: ts,
                            start_time: st,
                            min: row_min,
                            max: row_max,
                        },
                    );
                }
            }
        }

        self.build_output(&accum, num_key_cols)
    }

    fn build_output(
        &self,
        accum: &HashMap<String, HistogramAccum>,
        num_key_cols: usize,
    ) -> RecordBatch {
        let num_groups = accum.len();
        let has_ts = self.timestamp_column.is_some();
        let has_st = self.start_time_column.is_some();
        let has_min = self.min_column.is_some();
        let has_max = self.max_column.is_some();

        let mut key_builders: Vec<StringBuilder> = (0..num_key_cols)
            .map(|_| StringBuilder::with_capacity(num_groups, num_groups * 32))
            .collect();

        // Estimate total inner capacity for list builders.
        let total_buckets: usize = accum.values().map(|h| h.bucket_counts.len()).sum();

        let mut boundaries_builder =
            ListBuilder::new(Float64Builder::with_capacity(total_buckets));
        let mut bucket_counts_builder =
            ListBuilder::new(UInt64Builder::with_capacity(total_buckets));
        let mut sum_builder = Float64Builder::with_capacity(num_groups);
        let mut count_builder = UInt64Builder::with_capacity(num_groups);
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
        let mut min_builder = if has_min {
            Some(Float64Builder::with_capacity(num_groups))
        } else {
            None
        };
        let mut max_builder = if has_max {
            Some(Float64Builder::with_capacity(num_groups))
        } else {
            None
        };

        for (composite_key, entry) in accum.iter() {
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

            sum_builder.append_value(entry.sum);
            count_builder.append_value(entry.count);

            if let Some(ref mut builder) = min_builder {
                builder.append_value(entry.min.unwrap_or(0.0));
            }
            if let Some(ref mut builder) = max_builder {
                builder.append_value(entry.max.unwrap_or(0.0));
            }
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

        columns.push(Arc::new(boundaries_builder.finish()) as ArrayRef);
        columns.push(Arc::new(bucket_counts_builder.finish()) as ArrayRef);
        columns.push(Arc::new(sum_builder.finish()) as ArrayRef);
        columns.push(Arc::new(count_builder.finish()) as ArrayRef);

        if let Some(mut builder) = min_builder {
            columns.push(Arc::new(builder.finish()) as ArrayRef);
        }
        if let Some(mut builder) = max_builder {
            columns.push(Arc::new(builder.finish()) as ArrayRef);
        }

        RecordBatch::try_new(schema, columns).expect("schema mismatch in histogram aggregation")
    }
}

impl WriteProcessor for HistogramAggregator {
    fn process(&self, batches: Vec<RecordBatch>) -> ProcessorOutput {
        if batches.is_empty() {
            return ProcessorOutput::primary_only(Vec::new());
        }
        let aggregated = self.aggregate(&batches);
        ProcessorOutput::primary_only(vec![aggregated])
    }

    fn descriptor(&self) -> Option<ProcessorDescriptor> {
        Some(ProcessorDescriptor::Histogram {
            key_columns: self.key_columns.clone(),
            boundaries_column: self.boundaries_column.clone(),
            bucket_counts_column: self.bucket_counts_column.clone(),
            sum_column: self.sum_column.clone(),
            count_column: self.count_column.clone(),
            timestamp_column: self.timestamp_column.clone(),
            timestamp_unit: time_unit_to_string(self.timestamp_unit),
            start_time_column: self.start_time_column.clone(),
            min_column: self.min_column.clone(),
            max_column: self.max_column.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn histogram_schema() -> Schema {
        Schema::new(vec![
            Field::new("metric_name", DataType::Utf8, false),
            Field::new(
                "boundaries",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                false,
            ),
            Field::new(
                "bucket_counts",
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
                false,
            ),
            Field::new("sum", DataType::Float64, false),
            Field::new("count", DataType::UInt64, false),
        ])
    }

    fn make_histogram_batch(
        names: &[&str],
        boundaries: &[Vec<f64>],
        bucket_counts: &[Vec<u64>],
        sums: &[f64],
        counts: &[u64],
    ) -> RecordBatch {
        let schema = Arc::new(histogram_schema());

        let name_array = StringArray::from(names.iter().map(|s| *s).collect::<Vec<_>>());

        let mut b_builder = ListBuilder::new(Float64Builder::new());
        for b in boundaries {
            let inner = b_builder.values();
            for &v in b {
                inner.append_value(v);
            }
            b_builder.append(true);
        }

        let mut bc_builder = ListBuilder::new(UInt64Builder::new());
        for bc in bucket_counts {
            let inner = bc_builder.values();
            for &v in bc {
                inner.append_value(v);
            }
            bc_builder.append(true);
        }

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(name_array),
                Arc::new(b_builder.finish()),
                Arc::new(bc_builder.finish()),
                Arc::new(Float64Array::from(sums.to_vec())),
                Arc::new(UInt64Array::from(counts.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn merges_histograms() {
        let agg = HistogramAggregator::new(vec!["metric_name".to_string()]);

        let batch = make_histogram_batch(
            &["latency", "latency"],
            &[vec![10.0, 50.0, 100.0], vec![10.0, 50.0, 100.0]],
            &[vec![5, 10, 3], vec![2, 8, 1]],
            &[150.0, 80.0],
            &[18, 11],
        );

        let output = agg.process(vec![batch]);
        assert_eq!(output.primary.len(), 1);

        let result = &output.primary[0];
        assert_eq!(result.num_rows(), 1);

        let sum_arr = result
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(sum_arr.value(0), 230.0); // 150 + 80

        let count_arr = result
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(count_arr.value(0), 29); // 18 + 11

        let bc_list = result
            .column_by_name("bucket_counts")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let bc_values = bc_list.value(0);
        let bc_array = bc_values.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(bc_array.value(0), 7); // 5 + 2
        assert_eq!(bc_array.value(1), 18); // 10 + 8
        assert_eq!(bc_array.value(2), 4); // 3 + 1
    }

    #[test]
    fn multiple_keys() {
        let agg = HistogramAggregator::new(vec!["metric_name".to_string()]);

        let batch = make_histogram_batch(
            &["latency", "throughput"],
            &[vec![10.0, 50.0], vec![100.0, 500.0]],
            &[vec![5, 10], vec![20, 30]],
            &[100.0, 200.0],
            &[15, 50],
        );

        let output = agg.process(vec![batch]);
        let result = &output.primary[0];
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn empty_input() {
        let agg = HistogramAggregator::new(vec!["metric_name".to_string()]);
        let output = agg.process(Vec::new());
        assert!(output.primary.is_empty());
    }

    #[test]
    fn mismatched_bucket_lengths_extends() {
        let agg = HistogramAggregator::new(vec!["metric_name".to_string()]);

        // First row has 2 buckets, second has 3 — should extend to 3 on merge.
        let batch = make_histogram_batch(
            &["latency", "latency"],
            &[vec![10.0, 50.0], vec![10.0, 50.0, 100.0]],
            &[vec![5, 10], vec![2, 8, 4]],
            &[100.0, 50.0],
            &[15, 10],
        );

        let output = agg.process(vec![batch]);
        let result = &output.primary[0];
        assert_eq!(result.num_rows(), 1);

        let bc_list = result
            .column_by_name("bucket_counts")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let bc_values = bc_list.value(0);
        let bc_array = bc_values.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(bc_array.len(), 3);
        assert_eq!(bc_array.value(0), 7); // 5 + 2
        assert_eq!(bc_array.value(1), 18); // 10 + 8
        assert_eq!(bc_array.value(2), 4); // 0 (didn't exist) + 4

        let sum_arr = result
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(sum_arr.value(0), 150.0);

        let count_arr = result
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(count_arr.value(0), 25);
    }
}

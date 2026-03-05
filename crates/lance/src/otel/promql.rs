//! PromQL → DataFusion SQL translator.
//!
//! Parses a PromQL expression using `promql-parser`, walks the AST, and
//! produces a `PromQueryPlan` that is executed as SQL (via DataFusion) plus
//! Rust post-processing operations (rate, aggregation, etc.).
//!
//! This hybrid approach mirrors what Thanos and VictoriaMetrics do:
//! SQL handles data fetch + time filtering, Rust handles rate/aggregation/label
//! matching on JSON attributes.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, TimestampNanosecondType};
use datafusion::common::ScalarValue;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use promql_parser::label::{MatchOp, Matchers};
use promql_parser::parser::{self, Expr as PromExpr, LabelModifier, token};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A series of (timestamp_ms, value) samples with labels.
#[derive(Debug, Clone)]
pub struct PromSeries {
    pub labels: BTreeMap<String, String>,
    pub samples: Vec<(i64, f64)>,
}

/// Post-processing operations applied after SQL data fetch.
#[derive(Debug, Clone)]
enum PostOp {
    Rate {
        range_secs: f64,
    },
    Increase {
        range_secs: f64,
    },
    Irate,
    Aggregate {
        op: AggOp,
        by_labels: Vec<String>,
        without: bool,
    },
    HistogramQuantile {
        _quantile: f64,
    },
    ScalarMultiply(f64),
    ScalarDivide(f64),
    ScalarAdd(f64),
    ScalarSubtract(f64),
}

#[derive(Debug, Clone, Copy)]
enum AggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Topk,
    Bottomk,
}

/// Internal query plan built from PromQL AST.
struct PromQueryPlan {
    tables: Vec<&'static str>,
    metric_name: Option<String>,
    label_matchers: Vec<(String, MatchOp, String)>,
    start_ns: i64,
    end_ns: i64,
    step_ns: Option<i64>,
    range_ns: Option<i64>,
    post_ops: Vec<PostOp>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Execute a PromQL instant query, returning result series.
pub async fn eval_instant(
    ctx: &SessionContext,
    query: &str,
    time_ms: i64,
) -> Result<Vec<PromSeries>, String> {
    let expr = parser::parse(query).map_err(|e| format!("PromQL parse error: {e}"))?;

    let lookback_ns = 5 * 60 * 1_000_000_000i64; // 5m default lookback
    let time_ns = time_ms * 1_000_000;

    let mut plan = PromQueryPlan {
        tables: Vec::new(),
        metric_name: None,
        label_matchers: Vec::new(),
        start_ns: time_ns - lookback_ns,
        end_ns: time_ns,
        step_ns: None,
        range_ns: None,
        post_ops: Vec::new(),
    };

    walk_expr(&expr, &mut plan)?;

    if plan.tables.is_empty() {
        plan.tables = vec!["otel_counters", "otel_gauges"];
    }

    let mut all_series = fetch_and_filter(ctx, &plan).await?;
    apply_post_ops(&mut all_series, &plan)?;

    Ok(all_series)
}

/// Execute a PromQL range query, returning result series.
pub async fn eval_range(
    ctx: &SessionContext,
    query: &str,
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
) -> Result<Vec<PromSeries>, String> {
    let expr = parser::parse(query).map_err(|e| format!("PromQL parse error: {e}"))?;

    let start_ns = start_ms * 1_000_000;
    let end_ns = end_ms * 1_000_000;
    let step_ns = step_ms * 1_000_000;

    let mut plan = PromQueryPlan {
        tables: Vec::new(),
        metric_name: None,
        label_matchers: Vec::new(),
        start_ns,
        end_ns,
        step_ns: Some(step_ns),
        range_ns: None,
        post_ops: Vec::new(),
    };

    walk_expr(&expr, &mut plan)?;

    if plan.tables.is_empty() {
        plan.tables = vec!["otel_counters", "otel_gauges"];
    }

    // Extend start backward by range duration for rate/increase calculations
    if let Some(range_ns) = plan.range_ns {
        plan.start_ns -= range_ns;
    }

    let mut all_series = fetch_and_filter(ctx, &plan).await?;
    apply_post_ops(&mut all_series, &plan)?;

    // For range queries, align samples to step boundaries
    if plan.step_ns.is_some() {
        for series in &mut all_series {
            align_to_steps(series, start_ms, end_ms, step_ms);
        }
    }

    Ok(all_series)
}

// ---------------------------------------------------------------------------
// AST walking
// ---------------------------------------------------------------------------

fn walk_expr(expr: &PromExpr, plan: &mut PromQueryPlan) -> Result<(), String> {
    match expr {
        PromExpr::VectorSelector(vs) => {
            if let Some(name) = &vs.name {
                plan.metric_name = Some(name.clone());
            }
            extract_matchers(&vs.matchers, plan);
            Ok(())
        }
        PromExpr::MatrixSelector(ms) => {
            let range_ns = ms.range.as_nanos() as i64;
            plan.range_ns = Some(range_ns);
            // Walk the inner vector selector
            if let Some(name) = &ms.vs.name {
                plan.metric_name = Some(name.clone());
            }
            extract_matchers(&ms.vs.matchers, plan);
            Ok(())
        }
        PromExpr::Call(call) => {
            let func_name = call.func.name;
            match func_name {
                "rate" => {
                    if let Some(inner) = call.args.args.first() {
                        walk_expr(inner.as_ref(), plan)?;
                    }
                    let range_secs = plan.range_ns.unwrap_or(300_000_000_000) as f64 / 1e9;
                    plan.post_ops.push(PostOp::Rate { range_secs });
                    Ok(())
                }
                "increase" => {
                    if let Some(inner) = call.args.args.first() {
                        walk_expr(inner.as_ref(), plan)?;
                    }
                    let range_secs = plan.range_ns.unwrap_or(300_000_000_000) as f64 / 1e9;
                    plan.post_ops.push(PostOp::Increase { range_secs });
                    Ok(())
                }
                "irate" => {
                    if let Some(inner) = call.args.args.first() {
                        walk_expr(inner.as_ref(), plan)?;
                    }
                    plan.post_ops.push(PostOp::Irate);
                    Ok(())
                }
                "histogram_quantile" => {
                    let quantile = match call.args.args.first().map(|b| b.as_ref()) {
                        Some(PromExpr::NumberLiteral(n)) => n.val,
                        _ => 0.95,
                    };
                    if let Some(inner) = call.args.args.get(1) {
                        walk_expr(inner.as_ref(), plan)?;
                    }
                    plan.tables = vec!["otel_histograms"];
                    plan.post_ops.push(PostOp::HistogramQuantile {
                        _quantile: quantile,
                    });
                    Ok(())
                }
                "abs" | "ceil" | "floor" | "round" | "sqrt" | "ln" | "log2" | "log10" | "exp"
                | "absent" | "clamp_min" | "clamp_max" | "timestamp" | "label_replace"
                | "label_join" | "sort" | "sort_desc" => {
                    // Simple pass-through: walk inner, ignore transform
                    if let Some(inner) = call.args.args.first() {
                        walk_expr(inner.as_ref(), plan)?;
                    }
                    Ok(())
                }
                _ => {
                    // Unknown function: try to walk first arg
                    if let Some(inner) = call.args.args.first() {
                        walk_expr(inner.as_ref(), plan)?;
                    }
                    Ok(())
                }
            }
        }
        PromExpr::Aggregate(agg) => {
            walk_expr(&agg.expr, plan)?;

            let (by_labels, without) = match &agg.modifier {
                Some(LabelModifier::Include(labels)) => (labels.labels.clone(), false),
                Some(LabelModifier::Exclude(labels)) => (labels.labels.clone(), true),
                None => (Vec::new(), false),
            };

            let op = match agg.op.id() {
                token::T_SUM => AggOp::Sum,
                token::T_AVG => AggOp::Avg,
                token::T_MIN => AggOp::Min,
                token::T_MAX => AggOp::Max,
                token::T_COUNT => AggOp::Count,
                token::T_TOPK => AggOp::Topk,
                token::T_BOTTOMK => AggOp::Bottomk,
                _ => AggOp::Sum, // fallback
            };

            plan.post_ops.push(PostOp::Aggregate {
                op,
                by_labels,
                without,
            });
            Ok(())
        }
        PromExpr::Binary(bin) => {
            // Handle scalar binary ops (e.g., metric * 100)
            match (bin.lhs.as_ref(), bin.rhs.as_ref()) {
                (PromExpr::NumberLiteral(n), other) | (other, PromExpr::NumberLiteral(n)) => {
                    walk_expr(other, plan)?;
                    let val = n.val;
                    let is_lhs_scalar = matches!(bin.lhs.as_ref(), PromExpr::NumberLiteral(_));
                    match bin.op.id() {
                        token::T_MUL => plan.post_ops.push(PostOp::ScalarMultiply(val)),
                        token::T_DIV => {
                            if is_lhs_scalar {
                                // scalar / vector not supported well, just pass through
                            } else {
                                plan.post_ops.push(PostOp::ScalarDivide(val));
                            }
                        }
                        token::T_ADD => plan.post_ops.push(PostOp::ScalarAdd(val)),
                        token::T_SUB => {
                            if is_lhs_scalar {
                                // scalar - vector, negate and add
                            } else {
                                plan.post_ops.push(PostOp::ScalarSubtract(val));
                            }
                        }
                        _ => {}
                    }
                    Ok(())
                }
                (lhs, _rhs) => {
                    // Vector-vector binary: just walk lhs for now
                    walk_expr(lhs, plan)
                }
            }
        }
        PromExpr::Paren(p) => walk_expr(&p.expr, plan),
        PromExpr::Subquery(sq) => walk_expr(&sq.expr, plan),
        PromExpr::Unary(u) => walk_expr(&u.expr, plan),
        PromExpr::NumberLiteral(_) | PromExpr::StringLiteral(_) => Ok(()),
        _ => Ok(()),
    }
}

fn extract_matchers(matchers: &Matchers, plan: &mut PromQueryPlan) {
    for m in &matchers.matchers {
        if m.name == "__name__" {
            match &m.op {
                MatchOp::Equal => {
                    plan.metric_name = Some(m.value.clone());
                }
                _ => {
                    plan.label_matchers
                        .push((m.name.clone(), m.op.clone(), m.value.clone()));
                }
            }
        } else {
            plan.label_matchers
                .push((m.name.clone(), m.op.clone(), m.value.clone()));
        }
    }
}

// ---------------------------------------------------------------------------
// Data fetch via DataFusion
// ---------------------------------------------------------------------------

async fn fetch_and_filter(
    ctx: &SessionContext,
    plan: &PromQueryPlan,
) -> Result<Vec<PromSeries>, String> {
    let mut all_series: Vec<PromSeries> = Vec::new();

    for table in &plan.tables {
        let df = match ctx.table(*table).await {
            Ok(df) => df,
            Err(_) => continue, // table doesn't exist yet
        };

        // Time range filter
        let df = df
            .filter(
                col("timestamp")
                    .gt_eq(lit(ScalarValue::TimestampNanosecond(
                        Some(plan.start_ns),
                        None,
                    )))
                    .and(col("timestamp").lt_eq(lit(ScalarValue::TimestampNanosecond(
                        Some(plan.end_ns),
                        None,
                    )))),
            )
            .map_err(|e| format!("filter: {e}"))?;

        // Metric name filter
        let df = if let Some(name) = &plan.metric_name {
            df.filter(col("metric_name").eq(lit(name.as_str())))
                .map_err(|e| format!("filter: {e}"))?
        } else {
            df
        };

        // Select columns and sort
        let columns = if *table == "otel_histograms" {
            vec![
                "metric_name",
                "attributes",
                "resource_attributes",
                "timestamp",
                "boundaries",
                "bucket_counts",
                "sum",
                "count",
            ]
        } else {
            vec![
                "metric_name",
                "attributes",
                "resource_attributes",
                "timestamp",
                "value_int",
                "value_double",
            ]
        };

        let df = df
            .select_columns(&columns)
            .map_err(|e| format!("select: {e}"))?
            .sort(vec![col("timestamp").sort(true, true)])
            .map_err(|e| format!("sort: {e}"))?;

        let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;

        if *table == "otel_histograms" {
            // Histogram batches handled separately for histogram_quantile
            let series = histogram_batches_to_series(&batches, &plan.label_matchers)?;
            all_series.extend(series);
        } else {
            let series = batches_to_series(&batches, &plan.label_matchers)?;
            all_series.extend(series);
        }
    }

    Ok(all_series)
}

/// Convert Arrow record batches to PromSeries, grouping by (metric_name, attributes, resource_attributes).
fn batches_to_series(
    batches: &[RecordBatch],
    matchers: &[(String, MatchOp, String)],
) -> Result<Vec<PromSeries>, String> {
    let mut series_map: BTreeMap<String, PromSeries> = BTreeMap::new();

    for batch in batches {
        let n = batch.num_rows();
        let metric_names = batch.column(0).as_string::<i32>();
        let attributes = batch.column(1).as_string::<i32>();
        let resource_attributes = batch.column(2).as_string::<i32>();
        let timestamps = batch.column(3).as_primitive::<TimestampNanosecondType>();
        let value_ints = batch.column(4).as_primitive::<Int64Type>();
        let value_doubles = batch.column(5).as_primitive::<Float64Type>();

        for i in 0..n {
            let metric_name = metric_names.value(i);
            let attrs_json = attributes.value(i);
            let res_attrs_json = resource_attributes.value(i);

            let group_key = format!("{metric_name}\x00{attrs_json}\x00{res_attrs_json}");

            let entry = series_map.entry(group_key).or_insert_with(|| {
                let mut labels = BTreeMap::new();
                labels.insert("__name__".to_string(), metric_name.to_string());

                if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(attrs_json) {
                    for (k, v) in &map {
                        labels.insert(k.clone(), json_value_to_string(v));
                    }
                }

                if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(res_attrs_json) {
                    if let Some(sn) = map.get("service.name") {
                        labels.insert("job".to_string(), json_value_to_string(sn));
                    }
                    if let Some(si) = map.get("service.instance.id") {
                        labels.insert("instance".to_string(), json_value_to_string(si));
                    }
                }

                PromSeries {
                    labels,
                    samples: Vec::new(),
                }
            });

            let value = if !batch.column(5).is_null(i) {
                value_doubles.value(i)
            } else if !batch.column(4).is_null(i) {
                value_ints.value(i) as f64
            } else {
                0.0
            };

            let timestamp_ms = timestamps.value(i) / 1_000_000;
            entry.samples.push((timestamp_ms, value));
        }
    }

    // Apply label matchers
    let result: Vec<PromSeries> = series_map
        .into_values()
        .filter(|s| matches_label_matchers(&s.labels, matchers))
        .collect();

    Ok(result)
}

fn histogram_batches_to_series(
    _batches: &[RecordBatch],
    _matchers: &[(String, MatchOp, String)],
) -> Result<Vec<PromSeries>, String> {
    // TODO: decompose histograms into _bucket/_sum/_count series
    Ok(Vec::new())
}

fn matches_label_matchers(
    labels: &BTreeMap<String, String>,
    matchers: &[(String, MatchOp, String)],
) -> bool {
    matchers.iter().all(|(name, op, value)| {
        if name == "__name__" {
            return true; // Already filtered at SQL level
        }

        let label_val = labels.get(name).map(|s| s.as_str()).unwrap_or("");

        match op {
            MatchOp::Equal => label_val == value,
            MatchOp::NotEqual => label_val != value,
            MatchOp::Re(re) => re.is_match(label_val),
            MatchOp::NotRe(re) => !re.is_match(label_val),
        }
    })
}

fn json_value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Post-processing
// ---------------------------------------------------------------------------

fn apply_post_ops(series: &mut Vec<PromSeries>, plan: &PromQueryPlan) -> Result<(), String> {
    for op in &plan.post_ops {
        match op {
            PostOp::Rate { range_secs } => {
                apply_rate(
                    series,
                    *range_secs,
                    plan.step_ns,
                    plan.start_ns / 1_000_000,
                    plan.end_ns / 1_000_000,
                );
            }
            PostOp::Increase { range_secs } => {
                apply_increase(
                    series,
                    *range_secs,
                    plan.step_ns,
                    plan.start_ns / 1_000_000,
                    plan.end_ns / 1_000_000,
                );
            }
            PostOp::Irate => {
                apply_irate(series);
            }
            PostOp::Aggregate {
                op,
                by_labels,
                without,
            } => {
                *series = apply_aggregate(series, *op, by_labels, *without);
            }
            PostOp::HistogramQuantile { .. } => {
                // TODO: implement histogram_quantile
            }
            PostOp::ScalarMultiply(v) => {
                for s in series.iter_mut() {
                    for sample in &mut s.samples {
                        sample.1 *= v;
                    }
                }
            }
            PostOp::ScalarDivide(v) => {
                if *v != 0.0 {
                    for s in series.iter_mut() {
                        for sample in &mut s.samples {
                            sample.1 /= v;
                        }
                    }
                }
            }
            PostOp::ScalarAdd(v) => {
                for s in series.iter_mut() {
                    for sample in &mut s.samples {
                        sample.1 += v;
                    }
                }
            }
            PostOp::ScalarSubtract(v) => {
                for s in series.iter_mut() {
                    for sample in &mut s.samples {
                        sample.1 -= v;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Compute per-second rate over the range window for each evaluation step.
fn apply_rate(
    series: &mut Vec<PromSeries>,
    range_secs: f64,
    step_ns: Option<i64>,
    _start_ms: i64,
    _end_ms: i64,
) {
    for s in series.iter_mut() {
        if s.samples.len() < 2 {
            s.samples.clear();
            continue;
        }

        if let Some(_step_ns) = step_ns {
            // Range query: compute rate at each step
            let raw = std::mem::take(&mut s.samples);
            let range_ms = (range_secs * 1000.0) as i64;
            let mut result = Vec::new();

            for &(ts, _) in &raw {
                let window_start = ts - range_ms;
                let window: Vec<_> = raw
                    .iter()
                    .filter(|(t, _)| *t >= window_start && *t <= ts)
                    .collect();
                if window.len() >= 2 {
                    let first = window.first().unwrap();
                    let last = window.last().unwrap();
                    let dt = (last.0 - first.0) as f64 / 1000.0;
                    if dt > 0.0 {
                        let rate = (last.1 - first.1) / dt;
                        result.push((ts, rate));
                    }
                }
            }
            s.samples = result;
        } else {
            // Instant query: compute rate over entire sample set
            let first = s.samples.first().unwrap();
            let last = s.samples.last().unwrap();
            let dt = (last.0 - first.0) as f64 / 1000.0;
            if dt > 0.0 {
                let rate = (last.1 - first.1) / dt;
                s.samples = vec![(last.0, rate)];
            } else {
                s.samples.clear();
            }
        }
    }
    series.retain(|s| !s.samples.is_empty());
}

/// Compute increase (delta over range window).
fn apply_increase(
    series: &mut Vec<PromSeries>,
    range_secs: f64,
    step_ns: Option<i64>,
    start_ms: i64,
    end_ms: i64,
) {
    // Increase = rate * range_seconds
    apply_rate(series, range_secs, step_ns, start_ms, end_ms);
    for s in series.iter_mut() {
        for sample in &mut s.samples {
            sample.1 *= range_secs;
        }
    }
}

/// Compute instantaneous rate from the last two samples.
fn apply_irate(series: &mut Vec<PromSeries>) {
    for s in series.iter_mut() {
        if s.samples.len() < 2 {
            s.samples.clear();
            continue;
        }
        let n = s.samples.len();
        let prev = s.samples[n - 2];
        let last = s.samples[n - 1];
        let dt = (last.0 - prev.0) as f64 / 1000.0;
        if dt > 0.0 {
            let irate = (last.1 - prev.1) / dt;
            s.samples = vec![(last.0, irate)];
        } else {
            s.samples.clear();
        }
    }
    series.retain(|s| !s.samples.is_empty());
}

/// Apply an aggregation operation, grouping series by labels.
fn apply_aggregate(
    series: &[PromSeries],
    op: AggOp,
    by_labels: &[String],
    without: bool,
) -> Vec<PromSeries> {
    // Group series by the specified labels
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<&PromSeries>> = BTreeMap::new();

    for s in series {
        let group_key: BTreeMap<String, String> = if without {
            s.labels
                .iter()
                .filter(|(k, _)| !by_labels.contains(k) && *k != "__name__")
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        } else if by_labels.is_empty() {
            BTreeMap::new() // aggregate all into one group
        } else {
            by_labels
                .iter()
                .filter_map(|l| s.labels.get(l).map(|v| (l.clone(), v.clone())))
                .collect()
        };
        groups.entry(group_key).or_default().push(s);
    }

    groups
        .into_iter()
        .map(|(labels, group)| {
            let samples = aggregate_samples(&group, op);
            PromSeries { labels, samples }
        })
        .collect()
}

/// Aggregate samples across multiple series at each timestamp.
fn aggregate_samples(group: &[&PromSeries], op: AggOp) -> Vec<(i64, f64)> {
    // Collect all unique timestamps
    let mut ts_set: BTreeMap<i64, Vec<f64>> = BTreeMap::new();
    for s in group {
        for &(ts, val) in &s.samples {
            ts_set.entry(ts).or_default().push(val);
        }
    }

    ts_set
        .into_iter()
        .map(|(ts, values)| {
            let val = match op {
                AggOp::Sum => values.iter().sum(),
                AggOp::Avg => values.iter().sum::<f64>() / values.len() as f64,
                AggOp::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
                AggOp::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                AggOp::Count => values.len() as f64,
                AggOp::Topk | AggOp::Bottomk => {
                    // Topk/bottomk operate on series, not individual samples
                    values.iter().sum()
                }
            };
            (ts, val)
        })
        .collect()
}

/// Align samples to step boundaries by picking the closest sample to each step.
fn align_to_steps(series: &mut PromSeries, start_ms: i64, end_ms: i64, step_ms: i64) {
    if series.samples.is_empty() || step_ms <= 0 {
        return;
    }

    let raw = std::mem::take(&mut series.samples);
    let mut result = Vec::new();
    let mut ts = start_ms;

    while ts <= end_ms {
        // Find closest sample within step/2
        let half_step = step_ms / 2;
        if let Some(&(_, val)) = raw
            .iter()
            .filter(|(t, _)| (*t - ts).abs() <= half_step)
            .min_by_key(|(t, _)| (*t - ts).abs())
        {
            result.push((ts, val));
        }
        ts += step_ms;
    }

    series.samples = result;
}

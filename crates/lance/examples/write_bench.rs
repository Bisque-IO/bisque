//! Write Throughput Benchmark
//!
//! Measures `apply_append` performance on local filesystem across
//! different batch sizes and append patterns.
//!
//! Run with: cargo run --example write_bench -p bisque-lance --release
//!
//! Override defaults with env vars:
//!   TOTAL_ROWS=500000  BATCH_SIZES=1000,10000,100000  cargo run --example write_bench -p bisque-lance --release

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int32Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use bisque_lance::{BisqueLance, BisqueLanceConfig};

// =============================================================================
// Schema & Data Generation
// =============================================================================

fn log_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("level", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
        Field::new("status_code", DataType::Int32, true),
    ]))
}

/// Pre-generate `count` rows as a single RecordBatch.
fn generate_batch(schema: &Arc<Schema>, offset: i64, count: usize) -> RecordBatch {
    let levels = ["INFO", "WARN", "ERROR", "DEBUG"];
    let services = ["api-gateway", "auth-service", "user-service", "payment-service"];
    let messages = [
        "Request processed successfully",
        "Connection pool exhausted, retrying",
        "Failed to authenticate token",
        "Cache miss for user profile",
        "Database query timeout exceeded",
        "Rate limit reached for client",
        "Health check passed",
        "TLS handshake completed",
    ];

    let base_ts = 1_700_000_000_000i64 + offset;

    let timestamps: Vec<i64> = (0..count).map(|i| base_ts + i as i64).collect();
    let level_vals: Vec<&str> = (0..count).map(|i| levels[i % levels.len()]).collect();
    let svc_vals: Vec<&str> = (0..count).map(|i| services[i % services.len()]).collect();
    let msg_vals: Vec<&str> = (0..count).map(|i| messages[i % messages.len()]).collect();
    let codes: Vec<Option<i32>> = (0..count)
        .map(|i| match levels[i % levels.len()] {
            "ERROR" => Some(500),
            "WARN" => Some(429),
            _ => Some(200),
        })
        .collect();

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(StringArray::from(level_vals)),
            Arc::new(StringArray::from(svc_vals)),
            Arc::new(StringArray::from(msg_vals)),
            Arc::new(Int32Array::from(codes)),
        ],
    )
    .unwrap()
}

/// Estimate in-memory byte size of a RecordBatch (Arrow buffers).
fn batch_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|c| c.get_array_memory_size())
        .sum()
}

// =============================================================================
// Benchmark Harness
// =============================================================================

struct BenchResult {
    batch_size: usize,
    total_rows: usize,
    num_appends: usize,
    elapsed: Duration,
    latencies: Vec<Duration>,
}

impl BenchResult {
    fn rows_per_sec(&self) -> f64 {
        self.total_rows as f64 / self.elapsed.as_secs_f64()
    }

    fn mb_per_sec(&self, bytes_per_row: f64) -> f64 {
        (self.total_rows as f64 * bytes_per_row) / self.elapsed.as_secs_f64() / (1024.0 * 1024.0)
    }

    fn avg_latency(&self) -> Duration {
        let total: Duration = self.latencies.iter().sum();
        total / self.latencies.len() as u32
    }

    fn p50(&self) -> Duration {
        self.percentile(50)
    }

    fn p99(&self) -> Duration {
        self.percentile(99)
    }

    fn p999(&self) -> Duration {
        self.percentile(999)
    }

    fn percentile(&self, p_tenths: usize) -> Duration {
        let mut sorted = self.latencies.clone();
        sorted.sort();
        let idx = (sorted.len() * p_tenths / 1000).min(sorted.len() - 1);
        sorted[idx]
    }
}

async fn run_bench(
    batch_size: usize,
    total_rows: usize,
    schema: &Arc<Schema>,
) -> anyhow::Result<BenchResult> {
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("bench-data");

    let config = BisqueLanceConfig::new(&data_dir)
        .with_schema(schema.clone())
        .with_seal_max_age(Duration::from_secs(86400))
        .with_seal_max_size(u64::MAX);

    let engine = Arc::new(BisqueLance::open(config, None).await?);

    let num_appends = total_rows / batch_size;
    let actual_total = num_appends * batch_size;

    // Pre-generate all batches so generation time isn't included
    let batches: Vec<RecordBatch> = (0..num_appends)
        .map(|i| generate_batch(schema, (i * batch_size) as i64, batch_size))
        .collect();

    // Warmup: 3 appends (or fewer if num_appends is small)
    let warmup_count = num_appends.min(3);
    for batch in batches.iter().take(warmup_count) {
        engine.apply_append(vec![batch.clone()]).await?;
    }

    // Re-open with a fresh engine so warmup fragments don't affect measurement
    engine.shutdown().await?;
    drop(engine);
    tokio::fs::remove_dir_all(&data_dir).await?;

    let config = BisqueLanceConfig::new(&data_dir)
        .with_schema(schema.clone())
        .with_seal_max_age(Duration::from_secs(86400))
        .with_seal_max_size(u64::MAX);

    let engine = Arc::new(BisqueLance::open(config, None).await?);

    // Timed run
    let mut latencies = Vec::with_capacity(num_appends);
    let start = Instant::now();

    for batch in &batches {
        let lap = Instant::now();
        engine.apply_append(vec![batch.clone()]).await?;
        latencies.push(lap.elapsed());
    }

    let elapsed = start.elapsed();

    engine.shutdown().await?;

    Ok(BenchResult {
        batch_size,
        total_rows: actual_total,
        num_appends,
        elapsed,
        latencies,
    })
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let total_rows: usize = std::env::var("TOTAL_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500_000);

    let batch_sizes: Vec<usize> = std::env::var("BATCH_SIZES")
        .ok()
        .map(|v| {
            v.split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect()
        })
        .unwrap_or_else(|| vec![1_000, 5_000, 10_000, 50_000, 100_000]);

    let schema = log_schema();

    // Compute bytes-per-row from a sample batch
    let sample = generate_batch(&schema, 0, 1000);
    let bytes_per_row = batch_bytes(&sample) as f64 / sample.num_rows() as f64;

    println!("=== bisque-lance: Write Throughput Benchmark ===\n");
    println!("  Target rows per run : {}", fmt_num(total_rows));
    println!("  Batch sizes         : {:?}", batch_sizes);
    println!("  ~Bytes per row      : {:.0}", bytes_per_row);
    println!("  Schema columns      : {}", schema.fields().len());
    println!();

    // Header
    println!(
        "{:>10} {:>10} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "batch_size", "appends", "rows/sec", "MB/sec", "avg_lat", "p50", "p99", "p99.9"
    );
    println!("{}", "-".repeat(92));

    for &batch_size in &batch_sizes {
        if batch_size == 0 || batch_size > total_rows {
            continue;
        }

        let result = run_bench(batch_size, total_rows, &schema).await?;

        println!(
            "{:>10} {:>10} {:>12} {:>10.1} {:>10} {:>10} {:>10} {:>10}",
            fmt_num(result.batch_size),
            fmt_num(result.num_appends),
            fmt_num(result.rows_per_sec() as usize),
            result.mb_per_sec(bytes_per_row),
            fmt_dur(result.avg_latency()),
            fmt_dur(result.p50()),
            fmt_dur(result.p99()),
            fmt_dur(result.p999()),
        );
    }

    println!("\n=== Benchmark complete ===\n");
    Ok(())
}

// =============================================================================
// Formatting Helpers
// =============================================================================

fn fmt_num(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn fmt_dur(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1_000 {
        format!("{}µs", us)
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

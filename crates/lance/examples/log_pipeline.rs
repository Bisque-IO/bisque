//! Hot-Cold Storage Pipeline Example
//!
//! Demonstrates bisque-lance's three-tier storage lifecycle using application logs:
//!
//! 1. **Hot (Active)**: Write log data to the active local segment
//! 2. **Seal**: Rotate the active segment to sealed (read-only)
//! 3. **Cold (Deep Storage)**: Flush sealed segment to deep storage (local path)
//! 4. **Query**: Run SQL across all tiers as a single unified table
//!
//! ```text
//! ┌──────────────┐     seal      ┌──────────────┐     flush     ┌──────────────┐
//! │   Active     │ ──────────▶   │   Sealed     │ ──────────▶   │ Deep Storage │
//! │  (hot, rw)   │               │ (warm, ro)   │               │  (cold, ro)  │
//! └──────────────┘               └──────────────┘               └──────────────┘
//!        ▲                              │                              │
//!        │                              │                              │
//!        └──────────────────────────────┴──────────────────────────────┘
//!                          UNION ALL (single table view)
//! ```
//!
//! Run with: cargo run --example log_pipeline -p bisque-lance

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int32Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::execution::context::SessionContext;

use bisque_lance::{BisqueLance, BisqueLanceConfig, BisqueLanceTableProvider, IndexSpec, SealReason};
use lance_index::scalar::FullTextSearchQuery;

// =============================================================================
// Log Data Generation
// =============================================================================

/// Build the Arrow schema for application logs.
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

/// Generate a batch of fake log entries.
///
/// `batch_id` offsets the timestamps so each batch represents a different time window.
fn generate_log_batch(schema: &Arc<Schema>, batch_id: i64, num_rows: usize) -> RecordBatch {
    let base_ts = 1_700_000_000_000i64 + batch_id * 60_000; // offset by 60s per batch
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

    let timestamps: Vec<i64> = (0..num_rows)
        .map(|i| base_ts + (i as i64) * 100) // 100ms apart
        .collect();

    let level_values: Vec<&str> = (0..num_rows).map(|i| levels[i % levels.len()]).collect();

    let service_values: Vec<&str> = (0..num_rows)
        .map(|i| services[i % services.len()])
        .collect();

    let message_values: Vec<&str> = (0..num_rows)
        .map(|i| messages[i % messages.len()])
        .collect();

    let status_codes: Vec<Option<i32>> = (0..num_rows)
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
            Arc::new(StringArray::from(level_values)),
            Arc::new(StringArray::from(service_values)),
            Arc::new(StringArray::from(message_values)),
            Arc::new(Int32Array::from(status_codes)),
        ],
    )
    .expect("Failed to create RecordBatch")
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Set up tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    println!("=== bisque-lance: Hot-Cold Log Pipeline Example ===\n");

    // Use a temp directory for the example
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("lance-data");
    let deep_storage_dir = temp_dir.path().join("deep-storage.lance");

    let schema = log_schema();

    // =========================================================================
    // Step 1: Configure and open the engine
    // =========================================================================
    println!("--- Step 1: Configure and Open Engine ---\n");

    let config = BisqueLanceConfig::new(&data_dir)
        .with_schema(schema.clone())
        .with_seal_max_age(Duration::from_secs(3600)) // won't auto-seal in this example
        .with_seal_max_size(u64::MAX) // won't auto-seal by size
        // Use a local path as "deep storage" (normally this would be an S3 URI)
        .with_s3_uri(deep_storage_dir.to_str().unwrap())
        // Build FTS index on `message` column when a segment is sealed
        .with_seal_index(IndexSpec::fts("message"));

    let engine = Arc::new(BisqueLance::open(config, None).await?);

    println!("  Engine opened at: {}", data_dir.display());
    println!("  Deep storage at:  {}", deep_storage_dir.display());
    println!(
        "  Active segment:   {}",
        engine.catalog().active_segment
    );
    println!();

    // =========================================================================
    // Step 2: Write first batch to the HOT (active) segment
    // =========================================================================
    println!("--- Step 2: Write Batch 1 to Hot Storage (Active Segment) ---\n");

    let batch1 = generate_log_batch(&schema, 0, 100);
    println!("  Generated {} log entries for batch 1", batch1.num_rows());

    engine.apply_append(vec![batch1]).await?;

    let active_rows = engine
        .active_dataset_snapshot()
        .await
        .unwrap()
        .count_rows(None)
        .await?;
    println!("  Active segment now has {} rows", active_rows);
    println!();

    // =========================================================================
    // Step 3: Seal the active segment (hot → warm)
    // =========================================================================
    println!("--- Step 3: Seal Active Segment (Hot → Warm) ---\n");

    let sealed_id = engine.catalog().active_segment;
    let new_active_id = engine.next_segment_id();

    engine
        .apply_seal(sealed_id, new_active_id, SealReason::MaxAge)
        .await?;

    println!("  Sealed segment {} (now read-only)", sealed_id);
    println!("  New active segment: {}", new_active_id);
    println!(
        "  Catalog: active={}, sealed={:?}",
        engine.catalog().active_segment,
        engine.catalog().sealed_segment
    );
    println!();

    // =========================================================================
    // Step 3b: Build FTS index on sealed segment
    // =========================================================================
    println!("--- Step 3b: Build FTS Index on Sealed Segment ---\n");

    engine.create_seal_indices().await?;
    println!("  FTS index created on 'message' column of sealed segment");

    // Run FTS search on the sealed segment using Lance Scanner API
    let sealed_ds = engine.sealed_dataset_snapshot().await.unwrap();

    println!("\n  >> FTS search: \"timeout\" (match any log mentioning timeout)\n");
    let fts_query = FullTextSearchQuery::new("timeout".to_owned()).limit(Some(10));
    let results = sealed_ds
        .scan()
        .full_text_search(fts_query)?
        .try_into_batch()
        .await?;
    println!("  Found {} matching rows:", results.num_rows());
    let msg_col = results.column_by_name("message").unwrap();
    let score_col = results.column_by_name("_score").unwrap();
    let msgs = msg_col
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let scores = score_col
        .as_any()
        .downcast_ref::<arrow_array::Float32Array>()
        .unwrap();
    for i in 0..results.num_rows().min(5) {
        println!("    [{:.4}] {}", scores.value(i), msgs.value(i));
    }

    println!("\n  >> FTS search: \"authenticate\" (match auth-related logs)\n");
    let fts_query = FullTextSearchQuery::new("authenticate".to_owned()).limit(Some(10));
    let results = sealed_ds
        .scan()
        .full_text_search(fts_query)?
        .try_into_batch()
        .await?;
    println!("  Found {} matching rows:", results.num_rows());
    let msg_col = results.column_by_name("message").unwrap();
    let score_col = results.column_by_name("_score").unwrap();
    let svc_col = results.column_by_name("service").unwrap();
    let msgs = msg_col
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let scores = score_col
        .as_any()
        .downcast_ref::<arrow_array::Float32Array>()
        .unwrap();
    let svcs = svc_col
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    for i in 0..results.num_rows().min(5) {
        println!(
            "    [{:.4}] service={}, message={}",
            scores.value(i),
            svcs.value(i),
            msgs.value(i)
        );
    }

    println!();

    // =========================================================================
    // Step 4: Write second batch to the NEW hot segment
    // =========================================================================
    println!("--- Step 4: Write Batch 2 to New Hot Segment ---\n");

    let batch2 = generate_log_batch(&schema, 1, 50);
    println!("  Generated {} log entries for batch 2", batch2.num_rows());

    engine.apply_append(vec![batch2]).await?;

    let active_rows = engine
        .active_dataset_snapshot()
        .await
        .unwrap()
        .count_rows(None)
        .await?;
    let sealed_rows = engine
        .sealed_dataset_snapshot()
        .await
        .unwrap()
        .count_rows(None)
        .await?;
    println!("  Active segment: {} rows (hot)", active_rows);
    println!("  Sealed segment: {} rows (warm)", sealed_rows);
    println!();

    // =========================================================================
    // Step 5: Query across hot + warm tiers
    // =========================================================================
    println!("--- Step 5: Query Across Hot + Warm Tiers ---\n");

    let provider = Arc::new(BisqueLanceTableProvider::new(engine.clone(), schema.clone()));
    let ctx = SessionContext::new();
    ctx.register_table("logs", provider.clone())?;

    println!("  Registered 'logs' table (active + sealed segments)\n");

    // Count all rows
    println!("  >> SELECT COUNT(*) as total_logs FROM logs\n");
    ctx.sql("SELECT COUNT(*) as total_logs FROM logs")
        .await?
        .show()
        .await?;

    // Count by log level
    println!("\n  >> SELECT level, COUNT(*) as count FROM logs GROUP BY level ORDER BY count DESC\n");
    ctx.sql("SELECT level, COUNT(*) as count FROM logs GROUP BY level ORDER BY count DESC")
        .await?
        .show()
        .await?;

    // Errors only
    println!("\n  >> SELECT service, message, status_code FROM logs WHERE level = 'ERROR' LIMIT 5\n");
    ctx.sql("SELECT service, message, status_code FROM logs WHERE level = 'ERROR' LIMIT 5")
        .await?
        .show()
        .await?;

    // =========================================================================
    // Step 6: Flush sealed segment to deep (cold) storage
    // =========================================================================
    println!("\n--- Step 6: Flush Sealed → Deep Storage (Cold) ---\n");

    let flush_handle = engine.begin_flush()?;
    println!(
        "  Flush handle: segment_id={}",
        flush_handle.segment_id
    );

    engine.apply_begin_flush(flush_handle.segment_id);

    let s3_version = engine.execute_flush(&flush_handle).await?;
    println!(
        "  Deep storage write complete: version={}",
        s3_version
    );

    engine
        .apply_promote(flush_handle.segment_id, s3_version)
        .await?;

    println!("  Promoted segment {} to deep storage", flush_handle.segment_id);
    println!(
        "  Catalog: active={}, sealed={:?}, deep_storage_version={}",
        engine.catalog().active_segment,
        engine.catalog().sealed_segment,
        engine.catalog().s3_manifest_version,
    );
    println!();

    // =========================================================================
    // Step 7: Write a third batch to hot storage
    // =========================================================================
    println!("--- Step 7: Write Batch 3 to Hot Storage ---\n");

    let batch3 = generate_log_batch(&schema, 2, 25);
    println!("  Generated {} log entries for batch 3", batch3.num_rows());

    engine.apply_append(vec![batch3]).await?;

    let active_rows = engine
        .active_dataset_snapshot()
        .await
        .unwrap()
        .count_rows(None)
        .await?;
    println!("  Active segment: {} rows (hot)", active_rows);
    println!(
        "  Deep storage:   version {} (cold)",
        engine.catalog().s3_manifest_version
    );
    println!();

    // =========================================================================
    // Step 8: Query across hot + cold tiers (the sealed segment is gone)
    // =========================================================================
    println!("--- Step 8: Query Across Hot + Cold Tiers ---\n");

    // Re-register with fresh provider to pick up the promoted deep storage
    let provider = Arc::new(BisqueLanceTableProvider::new(engine.clone(), schema.clone()));
    let ctx = SessionContext::new();
    ctx.register_table("logs", provider)?;

    println!("  Registered 'logs' table (active + deep storage)\n");

    // Total count (should be 100 + 50 + 25 = 175)
    println!("  >> SELECT COUNT(*) as total_logs FROM logs\n");
    ctx.sql("SELECT COUNT(*) as total_logs FROM logs")
        .await?
        .show()
        .await?;

    // Per-service breakdown across both tiers
    println!("\n  >> SELECT service, COUNT(*) as count, MIN(status_code) as min_status, MAX(status_code) as max_status FROM logs GROUP BY service ORDER BY count DESC\n");
    ctx.sql(
        "SELECT service, COUNT(*) as count, \
         MIN(status_code) as min_status, MAX(status_code) as max_status \
         FROM logs GROUP BY service ORDER BY count DESC",
    )
    .await?
    .show()
    .await?;

    // Time range query spanning both tiers
    println!("\n  >> SELECT level, COUNT(*) as count FROM logs WHERE status_code >= 400 GROUP BY level ORDER BY count DESC\n");
    ctx.sql(
        "SELECT level, COUNT(*) as count FROM logs \
         WHERE status_code >= 400 GROUP BY level ORDER BY count DESC",
    )
    .await?
    .show()
    .await?;

    // =========================================================================
    // Cleanup
    // =========================================================================
    println!("\n--- Shutdown ---\n");
    engine.shutdown().await?;
    println!("  Engine shut down gracefully");
    println!("\n=== Example completed successfully! ===\n");

    Ok(())
}

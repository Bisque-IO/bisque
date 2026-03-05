//! Integration tests for write processors.
//!
//! Each test bootstraps a single-node Raft cluster with a write processor
//! configured on the batcher, creates appropriate tables, and verifies
//! that data is reduced or materialized correctly.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Float64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::TryStreamExt;
use openraft::Config;
use openraft::impls::BasicNode;

use bisque_lance::{
    BisqueLance, BisqueLanceConfig, CounterAggregator, GaugeAggregator, LanceRaftNode,
    LanceStateMachine, LanceTypeConfig, WriteBatcherConfig, WriteProcessor,
};
use bisque_raft::multi::{
    BisqueTcpTransport, BisqueTcpTransportConfig, DefaultNodeRegistry, MmapStorageConfig,
    MultiRaftManager, MultiplexedLogStorage, NodeAddressResolver,
};

// =============================================================================
// Test helpers
// =============================================================================

type NodeRegistry = DefaultNodeRegistry<u64>;
type Transport = BisqueTcpTransport<LanceTypeConfig>;
type Storage = MultiplexedLogStorage<LanceTypeConfig>;
type Manager = MultiRaftManager<LanceTypeConfig, Transport, Storage>;

/// Bootstrap a single-node Raft cluster with a processor-configured batcher.
async fn setup_with_processor(
    base_dir: &std::path::Path,
    processor: Arc<dyn WriteProcessor>,
    linger: Duration,
) -> Arc<LanceRaftNode> {
    let node_id: u64 = 1;
    let group_id: u64 = 1;

    let lance_dir = base_dir.join("lance-data");
    let config = BisqueLanceConfig::new(&lance_dir);
    let engine = Arc::new(BisqueLance::open(config).await.unwrap());

    let raft_dir = base_dir.join("raft-data");
    std::fs::create_dir_all(&raft_dir).unwrap();
    let storage_config = MmapStorageConfig::new(&raft_dir).with_segment_size(8 * 1024 * 1024);
    let storage = Storage::new(storage_config).await.unwrap();

    let registry = Arc::new(NodeRegistry::new());
    registry.register(node_id, "127.0.0.1:0".parse().unwrap());
    let transport = Transport::new(BisqueTcpTransportConfig::default(), registry);

    let manager: Arc<Manager> = Arc::new(MultiRaftManager::new(transport, storage));

    let raft_config = Arc::new(
        Config {
            heartbeat_interval: 200,
            election_timeout_min: 400,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    let state_machine = LanceStateMachine::new(engine.clone());
    let raft = manager
        .add_group(group_id, node_id, raft_config, state_machine)
        .await
        .unwrap();

    let mut members = BTreeMap::new();
    members.insert(node_id, BasicNode::default());
    let _ = raft.initialize(members).await;

    let batcher_config = WriteBatcherConfig::default()
        .with_linger(linger)
        .with_processor(processor);

    let raft_node =
        Arc::new(LanceRaftNode::new(raft, engine, node_id).with_write_batcher(batcher_config));
    raft_node.start();

    // Wait for leadership.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(raft_node.is_leader(), "single node should be leader");

    raft_node
}

fn counter_schema() -> Schema {
    Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ])
}

fn counter_schema_with_ts() -> Schema {
    Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
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

fn make_counter_batch_with_ts(names: &[&str], timestamps: &[i64], values: &[f64]) -> RecordBatch {
    let schema = Arc::new(counter_schema_with_ts());
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                names.iter().map(|s| *s).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

/// Read all rows from the active dataset and collect values into maps.
/// Returns (metric_name -> Vec<f64>) for simple verification.
async fn read_metric_values(
    node: &LanceRaftNode,
    table_name: &str,
    key_column: &str,
    value_column: &str,
) -> HashMap<String, Vec<f64>> {
    let table = node.engine().get_table(table_name).unwrap();
    let ds = table.active_dataset_snapshot().await.unwrap();
    let batches: Vec<RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    let mut results: HashMap<String, Vec<f64>> = HashMap::new();
    for batch in &batches {
        let names = batch
            .column_by_name(key_column)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column_by_name(value_column)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            results
                .entry(names.value(i).to_string())
                .or_default()
                .push(values.value(i));
        }
    }
    results
}

async fn count_rows(node: &LanceRaftNode, table_name: &str) -> usize {
    let table = node.engine().get_table(table_name).unwrap();
    let ds = table.active_dataset_snapshot().await.unwrap();
    ds.count_rows(None).await.unwrap()
}

// =============================================================================
// Tests
// =============================================================================

/// CounterAggregator reduces duplicate keys within a linger window.
#[tokio::test]
async fn counter_aggregator_reduces_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let processor = Arc::new(CounterAggregator::new(
        vec!["metric_name".to_string()],
        "value",
    ));

    let node = setup_with_processor(tmp.path(), processor, Duration::from_millis(50)).await;

    // Create table with the counter output schema (metric_name + value).
    let schema = counter_schema();
    node.create_table("counters", &schema).await.unwrap();

    // Write 5 rows with 2 unique keys: cpu=3 times, mem=2 times.
    let batch = make_counter_batch(
        &["cpu", "cpu", "mem", "cpu", "mem"],
        &[1.0, 2.0, 5.0, 3.0, 10.0],
    );
    node.write_records("counters", &[batch]).await.unwrap();

    // Should have 2 rows (one per key), not 5.
    let rows = count_rows(&node, "counters").await;
    assert_eq!(rows, 2, "expected 2 aggregated rows, got {}", rows);

    // Verify sums.
    let values = read_metric_values(&node, "counters", "metric_name", "value").await;
    let cpu_sum: f64 = values["cpu"].iter().sum();
    let mem_sum: f64 = values["mem"].iter().sum();
    assert_eq!(cpu_sum, 6.0); // 1 + 2 + 3
    assert_eq!(mem_sum, 15.0); // 5 + 10

    node.shutdown().await;
}

/// GaugeAggregator keeps only the last value per key.
#[tokio::test]
async fn gauge_aggregator_keeps_last_value() {
    let tmp = tempfile::tempdir().unwrap();
    let processor = Arc::new(GaugeAggregator::new(
        vec!["metric_name".to_string()],
        "value",
    ));

    let node = setup_with_processor(tmp.path(), processor, Duration::from_millis(50)).await;

    let schema = counter_schema(); // Same schema works for gauges
    node.create_table("gauges", &schema).await.unwrap();

    // Write multiple values per key — only last should survive.
    let batch = make_counter_batch(
        &["cpu", "cpu", "mem", "cpu", "mem"],
        &[1.0, 2.0, 5.0, 99.0, 88.0],
    );
    node.write_records("gauges", &[batch]).await.unwrap();

    let rows = count_rows(&node, "gauges").await;
    assert_eq!(rows, 2, "expected 2 gauge rows, got {}", rows);

    let values = read_metric_values(&node, "gauges", "metric_name", "value").await;
    assert_eq!(values["cpu"][0], 99.0); // Last value
    assert_eq!(values["mem"][0], 88.0); // Last value

    node.shutdown().await;
}

/// CounterAggregator with timestamp truncation collapses same-window rows.
#[tokio::test]
async fn counter_with_timestamp_truncation() {
    let tmp = tempfile::tempdir().unwrap();
    let processor = Arc::new(
        CounterAggregator::new(vec!["metric_name".to_string()], "value")
            .with_timestamp("timestamp", 60_000), // 1-minute resolution
    );

    let node = setup_with_processor(tmp.path(), processor, Duration::from_millis(50)).await;

    let schema = counter_schema_with_ts();
    node.create_table("ts_counters", &schema).await.unwrap();

    // Three rows: two in minute 1, one in minute 2.
    let batch = make_counter_batch_with_ts(
        &["cpu", "cpu", "cpu"],
        &[60_000, 75_000, 120_000], // :00 and :15 of min 1, :00 of min 2
        &[1.0, 2.0, 5.0],
    );
    node.write_records("ts_counters", &[batch]).await.unwrap();

    // Should have 2 rows: minute 1 (sum=3.0) and minute 2 (sum=5.0).
    let rows = count_rows(&node, "ts_counters").await;
    assert_eq!(rows, 2, "expected 2 time-bucketed rows, got {}", rows);

    node.shutdown().await;
}

/// Empty batch with processor configured returns Ok without errors.
#[tokio::test]
async fn processor_with_empty_batches() {
    let tmp = tempfile::tempdir().unwrap();
    let processor = Arc::new(CounterAggregator::new(
        vec!["metric_name".to_string()],
        "value",
    ));

    let node = setup_with_processor(tmp.path(), processor, Duration::from_millis(10)).await;

    let schema = counter_schema();
    node.create_table("empty_test", &schema).await.unwrap();

    // Empty batch should return Ok immediately.
    let result = node.write_records("empty_test", &[]).await;
    assert!(result.is_ok());

    node.shutdown().await;
}

/// Concurrent writes through a processor are all aggregated correctly.
#[tokio::test]
async fn concurrent_writes_through_processor() {
    let tmp = tempfile::tempdir().unwrap();
    let processor = Arc::new(CounterAggregator::new(
        vec!["metric_name".to_string()],
        "value",
    ));

    // Longer linger to increase chance of coalescing concurrent writes.
    let node = setup_with_processor(tmp.path(), processor, Duration::from_millis(100)).await;

    let schema = counter_schema();
    node.create_table("concurrent", &schema).await.unwrap();

    // Spawn 10 concurrent writers, each writing cpu=1.0 and mem=1.0.
    let mut handles = Vec::new();
    for _ in 0..10 {
        let n = node.clone();
        handles.push(tokio::spawn(async move {
            let batch = make_counter_batch(&["cpu", "mem"], &[1.0, 1.0]);
            n.write_records("concurrent", &[batch]).await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // All writes aggregated. Total: cpu=10.0, mem=10.0.
    // Due to multiple linger windows, there may be multiple rows per key,
    // but the total sum must be correct.
    let values = read_metric_values(&node, "concurrent", "metric_name", "value").await;
    let cpu_sum: f64 = values["cpu"].iter().sum();
    let mem_sum: f64 = values["mem"].iter().sum();

    assert_eq!(cpu_sum, 10.0);
    assert_eq!(mem_sum, 10.0);

    node.shutdown().await;
}

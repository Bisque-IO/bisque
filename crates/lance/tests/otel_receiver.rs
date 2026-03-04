//! Integration tests for the OTLP gRPC receiver.
//!
//! Each test bootstraps a single-node Raft cluster, creates the OTEL tables
//! via `OtlpReceiver::ensure_tables()`, then feeds protobuf data through the
//! service traits and verifies the results in Lance tables.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{
    Float64Array, FixedSizeBinaryArray, Int32Array, RecordBatch, StringArray, UInt64Array,
};
use futures::TryStreamExt;
use openraft::impls::BasicNode;
use openraft::Config;
use tonic::Request;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    Sum, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span, Status as SpanStatus,
};

use bisque_lance::otel::schema;
use bisque_lance::otel::OtlpReceiver;
use bisque_lance::{
    BisqueLance, BisqueLanceConfig, LanceRaftNode, LanceStateMachine, LanceTypeConfig,
    WriteBatcherConfig,
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

/// Bootstrap a single-node Raft cluster (no write processor).
async fn setup_node(base_dir: &std::path::Path) -> Arc<LanceRaftNode> {
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

    let batcher_config = WriteBatcherConfig::default().with_linger(Duration::from_millis(10));

    let raft_node = Arc::new(
        LanceRaftNode::new(raft, engine, node_id).with_write_batcher(batcher_config),
    );
    raft_node.start();

    // Wait for leadership.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(raft_node.is_leader(), "single node should be leader");

    raft_node
}

fn kv(key: &str, val: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(val.to_string())),
        }),
    }
}

async fn count_rows(node: &LanceRaftNode, table_name: &str) -> usize {
    let table = node.engine().get_table(table_name).unwrap();
    let ds = table.active_dataset_snapshot().await.unwrap();
    ds.count_rows(None).await.unwrap()
}

async fn read_batches(node: &LanceRaftNode, table_name: &str) -> Vec<RecordBatch> {
    let table = node.engine().get_table(table_name).unwrap();
    let ds = table.active_dataset_snapshot().await.unwrap();
    ds.scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

// =============================================================================
// Tests
// =============================================================================

/// ensure_tables creates all 5 OTEL tables.
#[tokio::test]
async fn ensure_tables_creates_all() {
    let tmp = tempfile::tempdir().unwrap();
    let node = setup_node(tmp.path()).await;

    let receiver = OtlpReceiver::new(node.clone());
    receiver.ensure_tables().await.unwrap();

    assert!(node.engine().has_table(schema::COUNTERS_TABLE));
    assert!(node.engine().has_table(schema::GAUGES_TABLE));
    assert!(node.engine().has_table(schema::HISTOGRAMS_TABLE));
    assert!(node.engine().has_table(schema::SPANS_TABLE));
    assert!(node.engine().has_table(schema::LOGS_TABLE));

    // Calling ensure_tables again should be idempotent.
    receiver.ensure_tables().await.unwrap();

    node.shutdown().await;
}

/// MetricsService routes Sum data points to otel_counters.
#[tokio::test]
async fn metrics_sum_to_counters() {
    let tmp = tempfile::tempdir().unwrap();
    let node = setup_node(tmp.path()).await;

    let receiver = OtlpReceiver::new(node.clone());
    receiver.ensure_tables().await.unwrap();

    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "test-svc")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "test-lib".to_string(),
                    version: "1.0".to_string(),
                    ..Default::default()
                }),
                metrics: vec![Metric {
                    name: "http.requests".to_string(),
                    data: Some(metric::Data::Sum(Sum {
                        data_points: vec![
                            NumberDataPoint {
                                attributes: vec![kv("method", "GET")],
                                time_unix_nano: 1_000_000_000,
                                value: Some(number_data_point::Value::AsDouble(42.0)),
                                ..Default::default()
                            },
                            NumberDataPoint {
                                attributes: vec![kv("method", "POST")],
                                time_unix_nano: 2_000_000_000,
                                value: Some(number_data_point::Value::AsInt(10)),
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    MetricsService::export(&receiver, Request::new(request)).await.unwrap();

    let rows = count_rows(&node, schema::COUNTERS_TABLE).await;
    assert_eq!(rows, 2, "expected 2 counter rows, got {rows}");

    let batches = read_batches(&node, schema::COUNTERS_TABLE).await;
    let batch = &batches[0];

    let names = batch
        .column_by_name("metric_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "http.requests");
    assert_eq!(names.value(1), "http.requests");

    let values = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    // Row order is non-deterministic (HashMap iteration), so collect and sort.
    let mut vals: Vec<f64> = (0..batch.num_rows()).map(|i| values.value(i)).collect();
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(vals, vec![10.0, 42.0]);

    node.shutdown().await;
}

/// MetricsService routes Gauge data points to otel_gauges.
#[tokio::test]
async fn metrics_gauge_to_gauges() {
    let tmp = tempfile::tempdir().unwrap();
    let node = setup_node(tmp.path()).await;

    let receiver = OtlpReceiver::new(node.clone());
    receiver.ensure_tables().await.unwrap();

    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("host", "server-1")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "cpu.usage".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 5_000_000_000,
                            value: Some(number_data_point::Value::AsDouble(73.5)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    MetricsService::export(&receiver, Request::new(request)).await.unwrap();

    let rows = count_rows(&node, schema::GAUGES_TABLE).await;
    assert_eq!(rows, 1);

    let batches = read_batches(&node, schema::GAUGES_TABLE).await;
    let batch = &batches[0];
    let values = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.value(0), 73.5);

    node.shutdown().await;
}

/// MetricsService routes Histogram data points to otel_histograms.
#[tokio::test]
async fn metrics_histogram_to_histograms() {
    let tmp = tempfile::tempdir().unwrap();
    let node = setup_node(tmp.path()).await;

    let receiver = OtlpReceiver::new(node.clone());
    receiver.ensure_tables().await.unwrap();

    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "latency".to_string(),
                    data: Some(metric::Data::Histogram(Histogram {
                        data_points: vec![HistogramDataPoint {
                            time_unix_nano: 3_000_000_000,
                            count: 100,
                            sum: Some(5000.0),
                            explicit_bounds: vec![10.0, 50.0, 100.0],
                            bucket_counts: vec![20, 50, 25, 5],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    <OtlpReceiver as MetricsService>::export(&receiver, Request::new(request)).await.unwrap();

    let rows = count_rows(&node, schema::HISTOGRAMS_TABLE).await;
    assert_eq!(rows, 1);

    let batches = read_batches(&node, schema::HISTOGRAMS_TABLE).await;
    let batch = &batches[0];

    let sum_arr = batch
        .column_by_name("sum")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(sum_arr.value(0), 5000.0);

    let count_arr = batch
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(count_arr.value(0), 100);

    node.shutdown().await;
}

/// TraceService writes spans to otel_spans.
#[tokio::test]
async fn trace_service_writes_spans() {
    let tmp = tempfile::tempdir().unwrap();
    let node = setup_node(tmp.path()).await;

    let receiver = OtlpReceiver::new(node.clone());
    receiver.ensure_tables().await.unwrap();

    let trace_id = vec![1u8; 16];
    let span_id = vec![2u8; 8];
    let parent_span_id = vec![0u8; 8];

    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "my-service")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "tracer-lib".to_string(),
                    version: "2.0".to_string(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: trace_id.clone(),
                    span_id: span_id.clone(),
                    parent_span_id: parent_span_id.clone(),
                    name: "GET /api/users".to_string(),
                    kind: 2, // SERVER
                    start_time_unix_nano: 1_000_000_000,
                    end_time_unix_nano: 2_000_000_000,
                    attributes: vec![kv("http.status_code", "200")],
                    status: Some(SpanStatus {
                        code: 1, // OK
                        message: "success".to_string(),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    TraceService::export(&receiver, Request::new(request)).await.unwrap();

    let rows = count_rows(&node, schema::SPANS_TABLE).await;
    assert_eq!(rows, 1);

    let batches = read_batches(&node, schema::SPANS_TABLE).await;
    let batch = &batches[0];

    let names = batch
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "GET /api/users");

    let kinds = batch
        .column_by_name("kind")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(kinds.value(0), 2);

    let trace_ids = batch
        .column_by_name("trace_id")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(trace_ids.value(0), &[1u8; 16]);

    node.shutdown().await;
}

/// LogsService writes log records to otel_logs.
#[tokio::test]
async fn logs_service_writes_logs() {
    let tmp = tempfile::tempdir().unwrap();
    let node = setup_node(tmp.path()).await;

    let receiver = OtlpReceiver::new(node.clone());
    receiver.ensure_tables().await.unwrap();

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "log-svc")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "logger".to_string(),
                    version: "1.0".to_string(),
                    ..Default::default()
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 10_000_000_000,
                    observed_time_unix_nano: 10_000_100_000,
                    severity_number: 9, // INFO
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("User logged in".to_string())),
                    }),
                    attributes: vec![kv("user.id", "alice")],
                    trace_id: vec![3u8; 16],
                    span_id: vec![4u8; 8],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    LogsService::export(&receiver, Request::new(request)).await.unwrap();

    let rows = count_rows(&node, schema::LOGS_TABLE).await;
    assert_eq!(rows, 1);

    let batches = read_batches(&node, schema::LOGS_TABLE).await;
    let batch = &batches[0];

    let body_arr = batch
        .column_by_name("body")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(body_arr.value(0), "User logged in");

    let severity = batch
        .column_by_name("severity_number")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(severity.value(0), 9);

    let severity_text = batch
        .column_by_name("severity_text")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(severity_text.value(0), "INFO");

    node.shutdown().await;
}

/// Empty requests are handled gracefully.
#[tokio::test]
async fn empty_requests() {
    let tmp = tempfile::tempdir().unwrap();
    let node = setup_node(tmp.path()).await;

    let receiver = OtlpReceiver::new(node.clone());
    receiver.ensure_tables().await.unwrap();

    // Empty metrics
    let resp = MetricsService::export(
        &receiver,
        Request::new(ExportMetricsServiceRequest {
            resource_metrics: vec![],
        }),
    )
    .await
    .unwrap();
    assert!(resp.into_inner().partial_success.is_none());

    // Empty traces
    let resp = TraceService::export(
        &receiver,
        Request::new(ExportTraceServiceRequest {
            resource_spans: vec![],
        }),
    )
    .await
    .unwrap();
    assert!(resp.into_inner().partial_success.is_none());

    // Empty logs
    let resp = LogsService::export(
        &receiver,
        Request::new(ExportLogsServiceRequest {
            resource_logs: vec![],
        }),
    )
    .await
    .unwrap();
    assert!(resp.into_inner().partial_success.is_none());

    node.shutdown().await;
}

/// Mixed metric types in a single request are decomposed correctly.
#[tokio::test]
async fn mixed_metric_types() {
    let tmp = tempfile::tempdir().unwrap();
    let node = setup_node(tmp.path()).await;

    let receiver = OtlpReceiver::new(node.clone());
    receiver.ensure_tables().await.unwrap();

    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![
                    Metric {
                        name: "counter_metric".to_string(),
                        data: Some(metric::Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_000_000_000,
                                value: Some(number_data_point::Value::AsDouble(1.0)),
                                ..Default::default()
                            }],
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "gauge_metric".to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_000_000_000,
                                value: Some(number_data_point::Value::AsDouble(99.0)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "hist_metric".to_string(),
                        data: Some(metric::Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                time_unix_nano: 1_000_000_000,
                                count: 10,
                                sum: Some(100.0),
                                explicit_bounds: vec![5.0, 10.0],
                                bucket_counts: vec![3, 5, 2],
                                ..Default::default()
                            }],
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    MetricsService::export(&receiver, Request::new(request)).await.unwrap();

    assert_eq!(count_rows(&node, schema::COUNTERS_TABLE).await, 1);
    assert_eq!(count_rows(&node, schema::GAUGES_TABLE).await, 1);
    assert_eq!(count_rows(&node, schema::HISTOGRAMS_TABLE).await, 1);

    node.shutdown().await;
}

//! Smoke tests for multi-raft functionality
//!
//! These tests verify basic functionality without requiring full cluster setup.
//! More comprehensive integration tests can be added later.

use std::collections::BTreeMap;
use std::fmt;
use std::net::{SocketAddr, TcpListener};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bisque_raft::multi::NodeAddressResolver;
use bisque_raft::multi::codec::{Decode, Encode};
use bisque_raft::multi::network::GroupNetworkFactory;
use bisque_raft::multi::{
    BisqueRpcServer, BisqueRpcServerConfig, BisqueTcpTransport, BisqueTcpTransportConfig,
    DefaultNodeRegistry, MultiRaftConfig, MultiRaftManager, MultiRaftNetworkFactory,
    MultiplexedLogStorage, MultiplexedStorageConfig,
};
use bisque_raft::{BisqueRaftTypeConfig, multi::codec};
use futures::FutureExt;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::network::RaftNetworkFactory;
use openraft::network::v2::RaftNetworkV2;
use openraft::storage::RaftStateMachine;
use openraft::{LogId, OptionalSend};

fn pick_unused_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephem");
    listener.local_addr().expect("local_addr")
}

fn run_async<F>(f: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");
    rt.block_on(async move {
        match AssertUnwindSafe(f).catch_unwind().await {
            Ok(v) => v,
            Err(p) => std::panic::resume_unwind(p),
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct TestData(Vec<u8>);

impl fmt::Display for TestData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestData({} bytes)", self.0.len())
    }
}

impl codec::ToCodec<codec::RawBytes> for TestData {
    fn to_codec(&self) -> codec::RawBytes {
        codec::RawBytes(self.0.clone())
    }
}

impl codec::FromCodec<codec::RawBytes> for TestData {
    fn from_codec(codec: codec::RawBytes) -> Self {
        TestData(codec.0)
    }
}

type TestConfig = BisqueRaftTypeConfig<TestData, ()>;

#[derive(Clone)]
struct TestStateMachine {
    applied_normal: Arc<AtomicU64>,
}

impl TestStateMachine {
    fn new() -> Self {
        Self {
            applied_normal: Arc::new(AtomicU64::new(0)),
        }
    }

    fn applied(&self) -> u64 {
        self.applied_normal.load(Ordering::SeqCst)
    }
}

impl RaftStateMachine<TestConfig> for TestStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<TestConfig>>,
            openraft::StoredMembership<TestConfig>,
        ),
        std::io::Error,
    > {
        Ok((None, openraft::StoredMembership::default()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), std::io::Error>
    where
        Strm: futures::Stream<
                Item = Result<openraft::storage::EntryResponder<TestConfig>, std::io::Error>,
            > + Unpin
            + OptionalSend,
    {
        use futures::StreamExt;
        while let Some(Ok((entry, responder))) = entries.next().await {
            if let openraft::EntryPayload::Normal(_data) = &entry.payload {
                self.applied_normal.fetch_add(1, Ordering::SeqCst);
            }
            if let Some(r) = responder {
                r.send(());
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<std::io::Cursor<Vec<u8>>, std::io::Error> {
        Ok(std::io::Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &openraft::storage::SnapshotMeta<TestConfig>,
        snapshot: std::io::Cursor<Vec<u8>>,
    ) -> Result<(), std::io::Error> {
        let _ = snapshot.into_inner();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::storage::Snapshot<TestConfig>>, std::io::Error> {
        Ok(None)
    }
}

impl openraft::RaftSnapshotBuilder<TestConfig> for TestStateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::storage::Snapshot<TestConfig>, std::io::Error> {
        let meta = openraft::storage::SnapshotMeta {
            last_log_id: None,
            last_membership: openraft::StoredMembership::default(),
            snapshot_id: "multi-raft-smoke".to_string(),
        };
        Ok(openraft::storage::Snapshot {
            meta,
            snapshot: std::io::Cursor::new(Vec::new()),
        })
    }
}

#[test]
fn test_multi_raft_config_default() {
    let config = MultiRaftConfig::default();
    assert_eq!(config.heartbeat_interval.as_millis(), 100);
}

#[test]
fn test_rpc_server_config_default() {
    let config = BisqueRpcServerConfig::default();
    assert_eq!(config.max_connections, 1000);
    assert_eq!(config.max_concurrent_requests, 256);
}

#[test]
fn test_transport_config_default() {
    let config = BisqueTcpTransportConfig::default();
    assert_eq!(config.connections_per_addr, 4);
    assert_eq!(config.max_concurrent_requests_per_conn, 256);
    assert_eq!(config.connection_ttl.as_secs(), 300);
}

// Note: Full cluster integration tests would require:
// - Setting up multiple nodes with ManiacRpcServer
// - Creating Raft groups on each node
// - Testing leader election and replication
// - Verifying heartbeat coalescing across groups
// These are marked as #[ignore] and can be run with: cargo test -- --ignored

#[test]
fn test_two_node_two_group_cluster() {
    run_async(async {
        // Node addresses (ephemeral ports)
        let addr1 = pick_unused_local_addr();
        let addr2 = pick_unused_local_addr();

        // Shared node registry
        let node_registry = Arc::new(DefaultNodeRegistry::<u64>::new());
        node_registry.register(1, addr1);
        node_registry.register(2, addr2);

        // Storage (temp dirs per node)
        let dir1 = tempfile::tempdir().expect("tempdir node1");
        let dir2 = tempfile::tempdir().expect("tempdir node2");

        let storage1 = MultiplexedLogStorage::<TestConfig>::new(MultiplexedStorageConfig {
            base_dir: dir1.path().to_path_buf(),
            num_shards: 1,
            segment_size: 4 * 1024 * 1024,
            fsync_interval: None,
            max_cache_entries_per_group: 1024,
            max_record_size: Some(1024 * 1024),
        })
        .await
        .expect("storage1");
        let storage2 = MultiplexedLogStorage::<TestConfig>::new(MultiplexedStorageConfig {
            base_dir: dir2.path().to_path_buf(),
            num_shards: 1,
            segment_size: 4 * 1024 * 1024,
            fsync_interval: None,
            max_cache_entries_per_group: 1024,
            max_record_size: Some(1024 * 1024),
        })
        .await
        .expect("storage2");

        // Transport
        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(2),
            connections_per_addr: 2,
            max_concurrent_requests_per_conn: 64,
            connection_ttl: Duration::from_secs(30),
            tcp_nodelay: true,
        };

        let transport1 =
            BisqueTcpTransport::<TestConfig>::new(transport_cfg.clone(), node_registry.clone());
        let transport2 =
            BisqueTcpTransport::<TestConfig>::new(transport_cfg.clone(), node_registry.clone());

        let multi_cfg = MultiRaftConfig {
            heartbeat_interval: Duration::from_millis(50),
        };

        let manager1 = Arc::new(MultiRaftManager::<TestConfig, _, _>::new(
            transport1,
            storage1,
            multi_cfg.clone(),
        ));
        let manager2 = Arc::new(MultiRaftManager::<TestConfig, _, _>::new(
            transport2,
            storage2,
            multi_cfg.clone(),
        ));

        // Start RPC servers
        let server1 = Arc::new(BisqueRpcServer::new(
            BisqueRpcServerConfig {
                bind_addr: addr1,
                ..Default::default()
            },
            manager1.clone(),
        ));
        let server2 = Arc::new(BisqueRpcServer::new(
            BisqueRpcServerConfig {
                bind_addr: addr2,
                ..Default::default()
            },
            manager2.clone(),
        ));

        tokio::spawn({
            let s = server1.clone();
            async move {
                let _ = s.serve().await;
            }
        });
        tokio::spawn({
            let s = server2.clone();
            async move {
                let _ = s.serve().await;
            }
        });

        // Give servers time to bind
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Raft config
        let raft_cfg = Arc::new(
            openraft::Config {
                heartbeat_interval: 200,
                election_timeout_min: 400,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()
            .expect("raft config validate"),
        );

        // Membership
        let mut members = BTreeMap::new();
        members.insert(1u64, openraft::impls::BasicNode::default());
        members.insert(2u64, openraft::impls::BasicNode::default());

        // Add two groups on both nodes, keeping state machines so we can assert isolation.
        let sm_1_1 = TestStateMachine::new();
        let sm_1_2 = TestStateMachine::new();
        let sm_2_1 = TestStateMachine::new();
        let sm_2_2 = TestStateMachine::new();

        manager1
            .add_group(1, 1, raft_cfg.clone(), sm_1_1.clone())
            .await
            .expect("add group1 node1");
        manager1
            .add_group(2, 1, raft_cfg.clone(), sm_1_2.clone())
            .await
            .expect("add group2 node1");

        manager2
            .add_group(1, 2, raft_cfg.clone(), sm_2_1.clone())
            .await
            .expect("add group1 node2");
        manager2
            .add_group(2, 2, raft_cfg.clone(), sm_2_2.clone())
            .await
            .expect("add group2 node2");

        // Initialize both groups from node1.
        for gid in [1u64, 2u64] {
            let raft = manager1.get_group(gid).expect("raft exists");
            tokio::time::timeout(Duration::from_secs(5), raft.initialize(members.clone()))
                .await
                .expect("init timeout")
                .expect("init ok");
        }

        // Wait until node1 is leader for both groups (deterministic for this setup).
        for gid in [1u64, 2u64] {
            let raft = manager1.get_group(gid).unwrap();
            tokio::time::timeout(Duration::from_secs(8), async {
                loop {
                    let m = raft.metrics().borrow_watched().clone();
                    if m.current_leader == Some(1) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("leader wait timeout");
        }

        // Write to group 1 only.
        let raft_g1 = manager1.get_group(1).unwrap();
        raft_g1
            .client_write(TestData(b"g1".to_vec()))
            .await
            .expect("client_write group1");

        // Wait until follower applied exactly 1 normal entry for group1.
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if sm_2_1.applied() >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("apply wait timeout");

        // Verify group isolation: group2 should not have applied any normal entries.
        assert_eq!(sm_1_2.applied(), 0, "node1 group2 should be untouched");
        assert_eq!(sm_2_2.applied(), 0, "node2 group2 should be untouched");
    });
}

#[test]
fn test_heartbeat_coalescing_verification() {
    run_async(async {
        use tokio::net::TcpListener as TokioTcpListener;

        // Fake server that just counts HeartbeatBatchMulti messages and replies with success.
        let server_addr = pick_unused_local_addr();
        let batches_seen = Arc::new(AtomicU64::new(0));

        let server_handle = {
            let batches_seen = batches_seen.clone();
            tokio::spawn(async move {
                let listener = TokioTcpListener::bind(server_addr)
                    .await
                    .expect("bind fake server");
                loop {
                    let (mut stream, _peer) = match listener.accept().await {
                        Ok(v) => v,
                        Err(_) => return,
                    };
                    let batches_seen = batches_seen.clone();
                    tokio::spawn(async move {
                        loop {
                            let frame =
                                match bisque_raft::multi::tcp_transport::read_frame(&mut stream)
                                    .await
                                {
                                    Ok(v) => v,
                                    Err(_) => return,
                                };
                            let msg: codec::RpcMessage<codec::RawBytes> =
                                match codec::RpcMessage::decode_from_slice(&frame) {
                                    Ok(m) => m,
                                    Err(_) => return,
                                };

                            match msg {
                                codec::RpcMessage::HeartbeatBatchMulti {
                                    request_id,
                                    heartbeats,
                                } => {
                                    batches_seen.fetch_add(1, Ordering::SeqCst);
                                    let responses = heartbeats
                                        .into_iter()
                                        .map(|(gid, _)| {
                                            (gid, codec::AppendEntriesResponse::Success)
                                        })
                                        .collect::<Vec<_>>();
                                    let resp = codec::RpcMessage::<codec::RawBytes>::HeartbeatBatchMultiResponse {
                                        request_id,
                                        responses,
                                    };
                                    let out = resp.encode_to_vec().expect("encode response");
                                    let _ = bisque_raft::multi::tcp_transport::write_frame(
                                        &mut stream,
                                        &out,
                                    )
                                    .await;
                                }
                                other => {
                                    // Best-effort error response.
                                    let resp = codec::RpcMessage::<codec::RawBytes>::Error {
                                        request_id: other.request_id(),
                                        error: "unsupported".to_string(),
                                    };
                                    let out = resp.encode_to_vec().expect("encode error");
                                    let _ = bisque_raft::multi::tcp_transport::write_frame(
                                        &mut stream,
                                        &out,
                                    )
                                    .await;
                                }
                            }
                        }
                    });
                }
            })
        };

        // Client-side setup: real ManiacTcpTransport + MultiRaftNetworkFactory coalescing.
        let node_registry = Arc::new(DefaultNodeRegistry::<u64>::new());
        node_registry.register(2, server_addr);

        let transport = BisqueTcpTransport::<TestConfig>::new(
            BisqueTcpTransportConfig {
                connect_timeout: Duration::from_secs(2),
                request_timeout: Duration::from_secs(2),
                connections_per_addr: 1,
                max_concurrent_requests_per_conn: 128,
                connection_ttl: Duration::from_secs(30),
                tcp_nodelay: true,
            },
            node_registry.clone(),
        );

        let factory = Arc::new(MultiRaftNetworkFactory::<TestConfig, _>::new(
            Arc::new(transport),
            MultiRaftConfig {
                heartbeat_interval: Duration::from_millis(10),
            },
        ));

        let node = openraft::impls::BasicNode {
            addr: server_addr.to_string(),
        };

        // Fire heartbeats for many groups concurrently to the same target.
        let groups: u64 = 100;
        let mut futs = Vec::new();
        for gid in 1..=groups {
            let mut gf = GroupNetworkFactory::new(factory.clone(), gid);
            let mut net = gf.new_client(2, &node).await;
            let hb = openraft::raft::AppendEntriesRequest::<TestConfig> {
                vote: openraft::impls::Vote::new(1, 1),
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
            };
            futs.push(async move {
                net.append_entries(
                    hb,
                    openraft::network::RPCOption::new(Duration::from_secs(3)),
                )
                .await
            });
        }

        let results = futures::future::join_all(futs).await;
        assert!(
            results.iter().all(|r| r.is_ok()),
            "all heartbeats should succeed"
        );

        // Coalescing expectation: should be much closer to "per peer" than "per group".
        // With groups=100 and MAX_COALESCED_HEARTBEATS=256, we expect ~1 batch (maybe a couple due to timing).
        let seen = batches_seen.load(Ordering::SeqCst);
        assert!(seen > 0, "should have sent at least one batch");
        assert!(
            seen <= 5,
            "expected strong coalescing, got {} HeartbeatBatchMulti messages for {} groups",
            seen,
            groups
        );

        // Keep server_handle alive until here.
        drop(server_handle);
    });
}

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

use bisque_raft::NodeAddressResolver;
use bisque_raft::codec::{Decode, Encode};
use bisque_raft::network::GroupNetworkFactory;
use bisque_raft::test_support::TestTempDir;
use bisque_raft::{BisqueRaftTypeConfig, multi::codec};
use bisque_raft::{
    BisqueRpcServer, BisqueRpcServerConfig, BisqueTcpTransport, BisqueTcpTransportConfig,
    DefaultNodeRegistry, MmapStorageConfig, MultiRaftManager, MultiplexedLogStorage,
};
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

impl codec::Encode for TestData {
    fn encode<W: std::io::Write>(&self, writer: &mut W) -> Result<(), codec::CodecError> {
        (self.0.len() as u32).encode(writer)?;
        writer.write_all(&self.0)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        4 + self.0.len()
    }
}

impl codec::Decode for TestData {
    fn decode<R: std::io::Read>(reader: &mut R) -> Result<Self, codec::CodecError> {
        let len = u32::decode(reader)? as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        Ok(Self(buf))
    }
}

impl codec::BorrowPayload for TestData {
    fn payload_bytes(&self) -> &[u8] {
        &self.0
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
fn test_rpc_server_config_default() {
    let config = BisqueRpcServerConfig::default();
    assert_eq!(config.max_connections, 1000);
    assert_eq!(config.max_concurrent_requests, 256);
}

#[test]
fn test_transport_config_default() {
    let config = BisqueTcpTransportConfig::default();
    assert_eq!(config.write_channel_capacity, 256);
    assert_eq!(config.max_batch_bytes, 64 * 1024);
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
        let dir1 = TestTempDir::new();
        let dir2 = TestTempDir::new();

        let storage1 = MultiplexedLogStorage::<TestConfig>::new(
            MmapStorageConfig::new(dir1.path())
                .with_segment_size(4 * 1024 * 1024)
                .with_fsync_delay(Duration::ZERO),
        )
        .await
        .expect("storage1");
        let storage2 = MultiplexedLogStorage::<TestConfig>::new(
            MmapStorageConfig::new(dir2.path())
                .with_segment_size(4 * 1024 * 1024)
                .with_fsync_delay(Duration::ZERO),
        )
        .await
        .expect("storage2");

        // Transport
        let transport_cfg = BisqueTcpTransportConfig {
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(2),
            connection_ttl: Duration::from_secs(30),
            tcp_nodelay: true,
            ..Default::default()
        };

        let transport1 =
            BisqueTcpTransport::<TestConfig>::new(transport_cfg.clone(), node_registry.clone());
        let transport2 =
            BisqueTcpTransport::<TestConfig>::new(transport_cfg.clone(), node_registry.clone());

        let manager1 = Arc::new(MultiRaftManager::<TestConfig, _, _>::new(
            transport1, storage1,
        ));
        let manager2 = Arc::new(MultiRaftManager::<TestConfig, _, _>::new(
            transport2, storage2,
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

// (heartbeat coalescing verification test was removed — heartbeats now go
// directly through BisqueTcpTransport which batches writes naturally)

//! Multi-Raft Cluster Example
//!
//! This example demonstrates how to create a multi-raft cluster using the bisque-raft crate.
//! It shows:
//! - Creating multiple Raft groups that share networking and storage
//! - Setting up the TCP transport layer with connection multiplexing
//! - Creating and initializing a cluster with multiple nodes
//! - Proposing commands to different Raft groups
//! - Leader election and cluster membership changes
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                         Node 1 (127.0.0.1:9001)             │
//! │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐    │
//! │  │   Group 1     │  │   Group 2     │  │   Group 3     │    │
//! │  └───────┬───────┘  └───────┬───────┘  └───────┬───────┘    │
//! │          │                  │                  │            │
//! │  ┌───────┴──────────────────┴──────────────────┴───────┐    │
//! │  │           Multiplexed TCP Transport + Storage       │    │
//! │  └───────────────────────────┬─────────────────────────┘    │
//! └──────────────────────────────┼──────────────────────────────┘
//!                                │
//!        ┌───────────────────────┼───────────────────────┐
//!        │                       │                       │
//! ┌──────┴──────┐         ┌──────┴──────┐         ┌──────┴──────┐
//! │   Node 2    │         │   Node 3    │         │    ...      │
//! │ :9002       │         │ :9003       │         │             │
//! │ Group 1,2,3 │         │ Group 1,2,3 │         │             │
//! └─────────────┘         └─────────────┘         └─────────────┘
//! ```
//!
//! ## Running the Example
//!
//! This example runs a 3-node cluster with 3 Raft groups. Each node participates
//! in all groups, demonstrating how multi-raft enables horizontal scaling.

use bisque_raft::BisqueRaftTypeConfig;
use bisque_raft::multi::codec::{BorrowPayload, CodecError, Decode, Encode};
use bisque_raft::multi::{
    BisqueRpcServer, BisqueRpcServerConfig, BisqueTcpTransport, BisqueTcpTransportConfig,
    DefaultNodeRegistry, MmapStorageConfig, MultiRaftManager, MultiplexedLogStorage,
    NodeAddressResolver,
};
use futures::StreamExt;
use openraft::impls::BasicNode;
use openraft::storage::RaftStateMachine;
use openraft::type_config::async_runtime::watch::WatchReceiver;
use openraft::{Config, LogId, OptionalSend, SnapshotMeta, StoredMembership};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Application Data Types
// =============================================================================

/// The application request type - a simple key-value store command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvCommand {
    /// Set a key to a value
    Set { key: String, value: String },
    /// Delete a key
    Delete { key: String },
}

// Required by openraft::AppData
impl fmt::Display for KvCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvCommand::Set { key, value } => write!(f, "SET {}={}", key, value),
            KvCommand::Delete { key } => write!(f, "DELETE {}", key),
        }
    }
}

/// The application response type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvResponse {
    /// Operation succeeded
    Ok,
    /// Get result
    Value(Option<String>),
}

// Required by openraft::AppDataResponse
impl fmt::Display for KvResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvResponse::Ok => write!(f, "OK"),
            KvResponse::Value(Some(v)) => write!(f, "{}", v),
            KvResponse::Value(None) => write!(f, "(nil)"),
        }
    }
}

// Implement codec traits for KvCommand to work with multi-raft
impl Encode for KvCommand {
    fn encode<W: std::io::Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let bytes = bincode::serde::encode_to_vec(self, bincode::config::standard())
            .expect("serialization should not fail");
        (bytes.len() as u32).encode(writer)?;
        writer.write_all(&bytes)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        let bytes = bincode::serde::encode_to_vec(self, bincode::config::standard())
            .expect("serialization should not fail");
        4 + bytes.len()
    }
}

impl Decode for KvCommand {
    fn decode<R: std::io::Read>(reader: &mut R) -> Result<Self, CodecError> {
        let len = u32::decode(reader)? as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        let (cmd, _): (KvCommand, _) =
            bincode::serde::decode_from_slice(&buf, bincode::config::standard())
                .map_err(|e| CodecError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())))?;
        Ok(cmd)
    }
}

impl BorrowPayload for KvCommand {
    fn payload_bytes(&self) -> &[u8] {
        // KvCommand doesn't own a contiguous byte buffer, so we return an empty slice.
        // The mmap storage uses this for size hints; actual encoding goes through Encode.
        &[]
    }
}

// =============================================================================
// Type Configuration
// =============================================================================

/// Our Raft type configuration using KvCommand and KvResponse
pub type KvTypeConfig = BisqueRaftTypeConfig<KvCommand, KvResponse>;

// =============================================================================
// State Machine
// =============================================================================

/// Simple in-memory key-value store state machine.
pub struct KvStateMachine {
    /// The group ID this state machine belongs to
    group_id: u64,
    /// The key-value store
    data: BTreeMap<String, String>,
    /// Last applied log ID
    last_applied: Option<LogId<KvTypeConfig>>,
    /// Last membership
    last_membership: StoredMembership<KvTypeConfig>,
}

impl KvStateMachine {
    pub fn new(group_id: u64) -> Self {
        Self {
            group_id,
            data: BTreeMap::new(),
            last_applied: None,
            last_membership: StoredMembership::default(),
        }
    }
}

impl RaftStateMachine<KvTypeConfig> for KvStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<KvTypeConfig>>, StoredMembership<KvTypeConfig>), std::io::Error> {
        Ok((self.last_applied.clone(), self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), std::io::Error>
    where
        Strm: futures::Stream<
                Item = Result<openraft::storage::EntryResponder<KvTypeConfig>, std::io::Error>,
            > + Unpin
            + OptionalSend,
    {
        while let Some(entry_result) = entries.next().await {
            let (entry, responder) = entry_result?;
            self.last_applied = Some(entry.log_id.clone());

            let response = match entry.payload {
                openraft::EntryPayload::Blank => KvResponse::Ok,
                openraft::EntryPayload::Normal(cmd) => match cmd {
                    KvCommand::Set { key, value } => {
                        println!("  [Group {}] SET {} = {}", self.group_id, key, value);
                        self.data.insert(key, value);
                        KvResponse::Ok
                    }
                    KvCommand::Delete { key } => {
                        println!("  [Group {}] DELETE {}", self.group_id, key);
                        self.data.remove(&key);
                        KvResponse::Ok
                    }
                },
                openraft::EntryPayload::Membership(m) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id.clone()), m);
                    KvResponse::Ok
                }
            };

            // Send the response if a responder is present
            if let Some(r) = responder {
                r.send(response);
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // For simplicity, we return a clone
        KvStateMachine {
            group_id: self.group_id,
            data: self.data.clone(),
            last_applied: self.last_applied.clone(),
            last_membership: self.last_membership.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, std::io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<KvTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), std::io::Error> {
        // Deserialize the snapshot data
        let data: BTreeMap<String, String> =
            bincode::serde::decode_from_slice(snapshot.get_ref(), bincode::config::standard())
                .map(|(d, _)| d)
                .unwrap_or_default();

        self.data = data;
        self.last_applied = meta.last_log_id.clone();
        self.last_membership = meta.last_membership.clone();

        println!(
            "  [Group {}] Installed snapshot at {:?}",
            self.group_id, meta.last_log_id
        );

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::Snapshot<KvTypeConfig>>, std::io::Error> {
        // For this example, we don't implement snapshotting
        Ok(None)
    }
}

impl openraft::storage::RaftSnapshotBuilder<KvTypeConfig> for KvStateMachine {
    async fn build_snapshot(&mut self) -> Result<openraft::Snapshot<KvTypeConfig>, std::io::Error> {
        let data = bincode::serde::encode_to_vec(&self.data, bincode::config::standard())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let last_applied = self.last_applied.clone().unwrap_or(LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 0,
                node_id: 0,
            },
            index: 0,
        });

        let meta = SnapshotMeta {
            last_log_id: Some(last_applied),
            last_membership: self.last_membership.clone(),
            snapshot_id: format!(
                "snapshot-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            ),
        };

        Ok(openraft::Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

// =============================================================================
// Main Example
// =============================================================================

/// Node configuration for a multi-raft node
struct NodeConfig {
    node_id: u64,
    addr: SocketAddr,
}

/// Type alias for our node registry
type NodeRegistry = DefaultNodeRegistry<u64>;

/// Type alias for our transport
type Transport = BisqueTcpTransport<KvTypeConfig>;

/// Type alias for our storage
type Storage = MultiplexedLogStorage<KvTypeConfig>;

/// Type alias for our manager
type Manager = MultiRaftManager<KvTypeConfig, Transport, Storage>;

/// Type alias for our RPC server
type RpcServer = BisqueRpcServer<KvTypeConfig, Transport, Storage>;

/// Create a node's infrastructure (storage, transport, manager)
async fn create_node(
    node_id: u64,
    node_addr: SocketAddr,
    base_dir: &std::path::Path,
    node_registry: Arc<NodeRegistry>,
) -> Arc<Manager> {
    // Create per-node data directory
    let node_dir = base_dir.join(format!("node-{}", node_id));
    std::fs::create_dir_all(&node_dir).expect("Failed to create node dir");

    // Create storage configuration
    let storage_config = MmapStorageConfig::new(&node_dir)
        .with_segment_size(64 * 1024 * 1024); // 64MB segment files

    // Create the mmap log storage
    let storage = MultiplexedLogStorage::<KvTypeConfig>::new(storage_config)
        .await
        .expect("Failed to create storage");

    println!("  Node {}: Created storage at {:?}", node_id, node_dir);

    // Create TCP transport
    let transport_config = BisqueTcpTransportConfig {
        connect_timeout: Duration::from_secs(5),
        // Set slightly lower than election timeout (4000ms) to ensure transport
        // fails fast and rotates connections before Raft gives up.
        request_timeout: Duration::from_millis(3000),
        connection_ttl: Duration::from_secs(300),
        tcp_nodelay: true,
        ..Default::default()
    };

    let transport = Transport::new(transport_config, node_registry);

    println!("  Node {}: Created TCP transport at {}", node_id, node_addr);

    // Create and return the manager wrapped in Arc
    Arc::new(MultiRaftManager::new(transport, storage))
}

const NUM_GROUPS: u64 = 3;

#[tokio::main]
async fn main() {
    use tracing_subscriber::EnvFilter;

    // Initialize logging
    // Use RUST_LOG env var if set, otherwise default to INFO to avoid chatty debug logs
    // Setup logging
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .init();

    println!("=== Multi-Raft Cluster Example ===\n");
    println!("This example demonstrates:");
    println!("  1. Creating a multi-raft node");
    println!("  2. Running multiple independent Raft groups");
    println!("  3. Shared networking and storage infrastructure");
    println!("  4. Proposing commands to different groups\n");

    // Create temporary data directory
    let temp_dir = std::env::temp_dir().join("multi-raft-example");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).expect("Failed to create temp dir");

    println!("--- Setting up 3-Node Multi-Raft Cluster ---\n");

    // Define our 3 nodes
    let nodes = vec![
        NodeConfig {
            node_id: 1,
            addr: "127.0.0.1:9001".parse().unwrap(),
        },
        NodeConfig {
            node_id: 2,
            addr: "127.0.0.1:9002".parse().unwrap(),
        },
        NodeConfig {
            node_id: 3,
            addr: "127.0.0.1:9003".parse().unwrap(),
        },
    ];

    // Create shared node registry with all nodes registered
    let node_registry = Arc::new(NodeRegistry::new());
    for node in &nodes {
        node_registry.register(node.node_id, node.addr);
        println!("  Registered node {} at {}", node.node_id, node.addr);
    }
    println!();

    // Create Raft configuration (shared across all nodes)
    // Use longer timeouts to give TCP transport time to deliver messages
    let raft_config = Arc::new(
        Config {
            heartbeat_interval: 500,
            election_timeout_min: 1000,
            election_timeout_max: 1500,
            ..Default::default()
        }
        .validate()
        .expect("Invalid raft config"),
    );

    // Create all node managers concurrently using tokio::spawn for true parallelism
    let mut handles = Vec::new();
    for node in &nodes {
        let node_id = node.node_id;
        let addr = node.addr;
        let temp_dir = temp_dir.clone();
        let node_registry = node_registry.clone();
        let handle = tokio::spawn(async move {
            let manager = create_node(node_id, addr, &temp_dir, node_registry).await;
            (node_id, addr, manager)
        });
        handles.push(handle);
    }

    // Wait for all nodes to be created
    let mut node_results = Vec::new();
    for handle in handles {
        node_results.push(handle.await.expect("Node creation task panicked"));
    }

    // Start RPC servers for all nodes
    let mut managers = Vec::new();
    for (node_id, addr, manager) in node_results {
        // Create and start RPC server for this node
        let rpc_config = BisqueRpcServerConfig {
            bind_addr: addr,
            ..Default::default()
        };
        let rpc_server = Arc::new(RpcServer::new(rpc_config, manager.clone()));

        // Spawn the RPC server in the background
        let server = rpc_server.clone();
        tokio::spawn(async move {
            if let Err(e) = server.serve().await {
                eprintln!("RPC server error: {:?}", e);
            }
        });

        println!("  Node {}: Started RPC server at {}", node_id, addr);
        managers.push((node_id, manager));
    }

    // Give the servers a moment to bind
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\n--- Creating Raft groups on all nodes ---\n");

    // Build membership with all 3 nodes
    let mut members = BTreeMap::new();
    for node in &nodes {
        members.insert(node.node_id, BasicNode::default());
    }

    // Add three Raft groups to each node
    for group_id in 1..=NUM_GROUPS {
        println!("Group {}:", group_id);

        for (node_id, manager) in &managers {
            let state_machine = KvStateMachine::new(group_id);

            match manager
                .add_group(group_id, *node_id, raft_config.clone(), state_machine)
                .await
            {
                Ok(_raft) => {
                    println!("  Node {}: Added group {}", node_id, group_id);
                }
                Err(e) => {
                    println!(
                        "  Node {}: Failed to add group {}: {:?}",
                        node_id, group_id, e
                    );
                }
            }
        }
        println!();
    }

    // Initialize cluster from node 1 (it will become the initial leader)
    println!("--- Initializing cluster from node 1 ---\n");

    let (_node1_id, node1_manager) = &managers[0];

    // Initialize groups sequentially for stability
    for group_id in 1..=NUM_GROUPS {
        if let Some(raft) = node1_manager.get_group(group_id) {
            match tokio::time::timeout(Duration::from_secs(5), raft.initialize(members.clone()))
                .await
            {
                Ok(Ok(_)) => {
                    println!("  Group {} initialized successfully", group_id);
                }
                Ok(Err(e)) => {
                    println!(
                        "  Warning: Could not initialize group {}: {:?}",
                        group_id, e
                    );
                }
                Err(_) => {
                    println!(
                        "  TIMEOUT: Initialize for group {} timed out after 5s",
                        group_id
                    );
                }
            }
        }
    }

    // Wait for leader election (need more time for 3 groups to elect leaders)
    println!("\n--- Waiting for leader election ---\n");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check leader status for each group
    for group_id in 1..=NUM_GROUPS {
        for (node_id, manager) in &managers {
            if let Some(raft) = manager.get_group(group_id) {
                let metrics = raft.metrics().borrow_watched().clone();
                if metrics.current_leader == Some(*node_id) {
                    println!("  Group {}: Node {} is the LEADER", group_id, node_id);
                } else {
                    println!(
                        "  Group {}: Node {} is a follower (leader: {:?})",
                        group_id, node_id, metrics.current_leader
                    );
                }
            }
        }
    }

    // Show final state
    println!("\n--- Cluster Status ---\n");
    for (node_id, manager) in &managers {
        println!(
            "  Node {}: {} groups active",
            node_id,
            manager.group_count()
        );
    }

    // Keep cluster running for testing/interaction
    // Press Ctrl+C to shut down gracefully
    println!("\nCluster is running. Press Ctrl+C to shutdown...\n");
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }

    // Cleanup
    println!("\n--- Shutting down ---\n");
    for (node_id, manager) in &managers {
        for group_id in 1..=NUM_GROUPS {
            manager.remove_group(group_id).await;
        }
        println!("  Node {}: Removed all groups", node_id);
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
    println!("  Cleaned up temporary directory");

    println!("\n=== Example completed successfully! ===\n");
}

// =============================================================================
// Additional Documentation
// =============================================================================

/// # Multi-Raft Design Notes
///
/// ## Why Multi-Raft?
///
/// In a large-scale distributed system, using a single Raft group becomes a bottleneck:
/// - All reads/writes must go through a single leader
/// - A single log grows unboundedly
/// - Scaling to many nodes increases election latency
///
/// Multi-Raft solves this by partitioning data across multiple Raft groups:
/// - Each group handles a subset of the data (e.g., by key range or hash)
/// - Groups elect leaders independently
/// - Network and storage can be shared to reduce overhead
///
/// ## Shared Infrastructure
///
/// This implementation shares:
/// - **TCP Transport**: Connection pools are shared across groups
/// - **Log Storage**: Single log file with group_id prefix for entries
/// - **Configuration**: Heartbeat intervals, timeouts, etc.
///
/// ## Scaling Patterns
///
/// 1. **Horizontal scaling**: Add more nodes to each group
/// 2. **Sharding**: Split data across groups by key range
/// 3. **Placement**: Locate group leaders near their data
///
/// ## Example Use Cases
///
/// - **Key-Value Store**: Each group handles a key range
/// - **Message Queue**: Each group handles a topic/partition
/// - **Database**: Each group handles a table or shard
#[allow(dead_code)]
fn _documentation() {}

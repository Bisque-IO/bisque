use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use openraft::Config;
use openraft::LogId;

use openraft::RaftLogReader;
use openraft::SnapshotMeta;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

// --- Application Data Types ---

pub type NodeId = u64;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub key: String,
    pub value: String,
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Set {} = {}", self.key, self.value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub value: Option<String>,
}

impl std::fmt::Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.value {
            Some(v) => write!(f, "{}", v),
            None => write!(f, "None"),
        }
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = NodeId,
        Node = openraft::impls::BasicNode,
        Entry = openraft::impls::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
//...

// --- Storage Implementation (In-Memory) ---

use openraft::network::RPCOption;

use openraft::network::RaftNetworkFactory;

#[derive(Debug, Clone)]
pub struct LogStore {
    log: Arc<RwLock<BTreeMap<u64, openraft::impls::Entry<TypeConfig>>>>,
    vote: Arc<RwLock<Option<openraft::impls::Vote<TypeConfig>>>>,
}

impl Default for LogStore {
    fn default() -> Self {
        Self {
            log: Arc::new(RwLock::new(BTreeMap::new())),
            vote: Arc::new(RwLock::new(None)),
        }
    }
}

use openraft::error::decompose::DecomposeResult;

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::impls::Entry<TypeConfig>>, std::io::Error> {
        let log = self.log.read().await;
        let response = log
            .range(range)
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();
        Ok(response)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<openraft::impls::Vote<TypeConfig>>, std::io::Error> {
        Ok(self.vote.read().await.clone())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, std::io::Error> {
        let log = self.log.read().await;
        let last_serialized = log.iter().last().map(|(_, entry)| entry);
        let last_log_id = last_serialized.map(|entry| entry.log_id);

        let last_purged_log_id = None;

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &openraft::impls::Vote<TypeConfig>,
    ) -> Result<(), std::io::Error> {
        let mut v = self.vote.write().await;
        *v = Some(vote.clone());
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::IOFlushed<TypeConfig>,
    ) -> Result<(), std::io::Error>
    where
        I: IntoIterator<Item = openraft::impls::Entry<TypeConfig>> + Send,
    {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.log_id.index, entry);
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        after: Option<LogId<TypeConfig>>,
    ) -> Result<(), std::io::Error> {
        let mut log = self.log.write().await;
        match after {
            Some(log_id) => {
                log.split_off(&(log_id.index + 1));
            }
            None => {
                log.clear();
            }
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<TypeConfig>) -> Result<(), std::io::Error> {
        let mut log = self.log.write().await;
        let keys_to_remove: Vec<u64> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys_to_remove {
            log.remove(&k);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct StateMachineStore {
    data: Arc<RwLock<BTreeMap<String, String>>>,
    last_applied_log: Arc<RwLock<Option<LogId<TypeConfig>>>>,
}

use futures::StreamExt;
use openraft::OptionalSend;
use openraft::StoredMembership;

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<TypeConfig>>, StoredMembership<TypeConfig>), std::io::Error> {
        Ok((
            self.last_applied_log.read().await.clone(),
            StoredMembership::default(),
        ))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), std::io::Error>
    where
        Strm: futures::Stream<
                Item = Result<openraft::storage::EntryResponder<TypeConfig>, std::io::Error>,
            > + Unpin
            + OptionalSend,
    {
        let mut data = self.data.write().await;
        let mut last_applied = self.last_applied_log.write().await;

        while let Some(Ok((entry, responder))) = entries.next().await {
            *last_applied = Some(entry.log_id);

            let resp = match entry.payload {
                openraft::EntryPayload::Normal(req) => {
                    data.insert(req.key.clone(), req.value.clone());
                    Response {
                        value: Some(req.value),
                    }
                }
                openraft::EntryPayload::Blank => Response { value: None },
                openraft::EntryPayload::Membership(_) => Response { value: None },
            };

            if let Some(r) = responder {
                r.send(resp);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, std::io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<TypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), std::io::Error> {
        let mut data = self.data.write().await;
        let mut last_applied = self.last_applied_log.write().await;

        let content = snapshot.into_inner();
        let new_data: BTreeMap<String, String> =
            bincode::serde::decode_from_slice(&content, bincode::config::standard())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                .map(|(d, _)| d)?;

        *data = new_data;
        *last_applied = meta.last_log_id;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, std::io::Error> {
        let data = self.data.read().await;
        let last_applied = self.last_applied_log.read().await;

        if let Some(last) = *last_applied {
            let content =
                bincode::serde::encode_to_vec(&*data, bincode::config::standard()).unwrap();
            let meta = SnapshotMeta {
                last_log_id: Some(last),
                last_membership: Default::default(), // Simplified
                snapshot_id: "1".to_string(),
            };
            Ok(Some(Snapshot {
                meta,
                snapshot: Cursor::new(content),
            }))
        } else {
            Ok(None)
        }
    }
}

impl openraft::RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, std::io::Error> {
        let data = self.data.read().await;
        let last_applied = self.last_applied_log.read().await;

        let content = bincode::serde::encode_to_vec(&*data, bincode::config::standard()).unwrap();
        let meta = SnapshotMeta {
            last_log_id: *last_applied,
            last_membership: Default::default(),
            snapshot_id: format!(
                "{}-{:?}",
                last_applied.unwrap().index,
                last_applied.unwrap().leader_id
            ),
        };

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(content),
        })
    }
}

// --- Network Implementation (In-Memory Channels) ---

pub struct InMemNetwork {
    nodes: Arc<Mutex<HashMap<NodeId, openraft::Raft<TypeConfig>>>>,
}

impl InMemNetwork {
    pub fn new(nodes: Arc<Mutex<HashMap<NodeId, openraft::Raft<TypeConfig>>>>) -> Self {
        Self { nodes }
    }
}

impl RaftNetworkFactory<TypeConfig> for InMemNetwork {
    type Network = InMemNetworkConnection;

    async fn new_client(
        &mut self,
        target: NodeId,
        _node: &openraft::impls::BasicNode,
    ) -> Self::Network {
        InMemNetworkConnection {
            target,
            nodes: self.nodes.clone(),
        }
    }
}

pub struct InMemNetworkConnection {
    target: NodeId,
    nodes: Arc<Mutex<HashMap<NodeId, openraft::Raft<TypeConfig>>>>,
}

use openraft::error::StreamingError;
use openraft::network::v2::RaftNetworkV2;

// NOTE: We don't implement the RaftNetwork generic trait directly,
// OpenRaft expects us to implement specific network methods.
impl RaftNetworkV2<TypeConfig> for InMemNetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig, openraft::error::Infallible>>
    {
        let nodes = self.nodes.lock().await;
        let node = nodes.get(&self.target).ok_or_else(|| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                &AnyError::error("Node not found"),
            ))
        })?;

        node.append_entries(req)
            .await
            .map_err(|e| {
                openraft::error::RPCError::RemoteError(openraft::error::RemoteError::new(
                    self.target,
                    e,
                ))
            })
            .decompose_infallible()
    }

    async fn vote(
        &mut self,
        req: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, openraft::error::Infallible>> {
        let nodes = self.nodes.lock().await;
        let node = nodes.get(&self.target).ok_or_else(|| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                &AnyError::error("Node not found"),
            ))
        })?;
        node.vote(req)
            .await
            .map_err(|e| {
                openraft::error::RPCError::RemoteError(openraft::error::RemoteError::new(
                    self.target,
                    e,
                ))
            })
            .decompose_infallible()
    }

    async fn full_snapshot(
        &mut self,
        vote: openraft::impls::Vote<TypeConfig>,
        snapshot: Snapshot<TypeConfig>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<openraft::raft::SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let nodes = self.nodes.lock().await;
        let node = nodes.get(&self.target).ok_or_else(|| {
            openraft::error::StreamingError::Network(openraft::error::NetworkError::new(
                &AnyError::error("Node not found"),
            ))
        })?;

        let res = node.install_full_snapshot(vote, snapshot).await;
        match res {
            Ok(r) => Ok(r),
            Err(e) => {
                let net_err =
                    openraft::error::NetworkError::new(&openraft::AnyError::error(e.to_string()));
                Err(openraft::error::StreamingError::Network(net_err))
            }
        }
    }
}

use openraft::AnyError;
use openraft::BasicNode;

// --- Main ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup logging
    // tracing_subscriber::fmt().with_env_filter("info").init();

    // Setup network registry
    let nodes = Arc::new(Mutex::new(HashMap::new()));

    // Create 3 nodes
    let mut tasks = Vec::new();

    for i in 1..=3 {
        let node_id = i;
        let config = Arc::new(Config::default().validate().unwrap());
        let log_store = LogStore::default();
        let sm_store = StateMachineStore::default();
        let network = InMemNetwork::new(nodes.clone());

        let raft = openraft::Raft::new(node_id, config.clone(), network, log_store, sm_store)
            .await
            .unwrap();

        nodes.lock().await.insert(node_id, raft.clone());

        // Keep raft alive
        let raft_clone = raft.clone();
        tasks.push((node_id, raft_clone));
    }

    println!("--- Initializing Cluster ---");
    // Initialize leader
    let node1 = nodes.lock().await.get(&1).unwrap().clone();

    let mut nodes_map = BTreeMap::new();
    nodes_map.insert(1, BasicNode::new("127.0.0.1:9001"));
    nodes_map.insert(2, BasicNode::new("127.0.0.1:9002"));
    nodes_map.insert(3, BasicNode::new("127.0.0.1:9003"));

    node1.initialize(nodes_map).await?;

    println!("--- Waiting for Leader ---");
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("--- Writing Data ---");
    node1
        .client_write(Request {
            key: "foo".to_string(),
            value: "bar".to_string(),
        })
        .await?;

    println!("--- Reading Data ---");
    // In a real system you'd read from state machine, here we can just check logs or assume success if write returned
    println!("Write successful!");

    // Cleanup
    println!("--- Done ---");

    Ok(())
}

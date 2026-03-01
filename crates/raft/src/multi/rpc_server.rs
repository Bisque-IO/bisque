//! Tokio RPC Server for Multi-Raft
//!
//! Accepts incoming TCP connections and handles RPC requests from peers.
//! Integrates with MultiRaftManager to route requests to the appropriate Raft groups.
//!
//! ## True Connection Multiplexing
//!
//! This server properly supports connection multiplexing where:
//! - Multiple RPC requests are received and processed concurrently
//! - Responses are sent back out-of-order as soon as they're ready
//! - Each connection has separate reader and writer tasks
//! - Request IDs correlate responses to their original requests

use crate::multi::codec::{
    Decode, Encode, ResponseMessage as CodecResponseMessage, RpcMessage as CodecRpcMessage,
    SnapshotMeta as CodecSnapshotMeta, Vote as CodecVote,
};
use crate::multi::manager::MultiRaftManager;
use crate::multi::network::MultiplexedTransport;
use crate::multi::storage::MultiRaftLogStorage;
use crate::multi::tcp_transport::{read_frame, return_encode_buffer, write_frame_vectored};
use dashmap::DashMap;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use openraft::RaftTypeConfig;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub use protocol::{ResponseMessage, RpcMessage};

/// Key for identifying an in-progress snapshot transfer
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SnapshotTransferKey {
    /// The group receiving the snapshot
    group_id: u64,
    /// The snapshot ID being transferred
    snapshot_id: String,
}

/// State for an in-progress chunked snapshot transfer
struct SnapshotAccumulator {
    /// Vote from the leader sending the snapshot
    vote: CodecVote,
    /// Snapshot metadata
    meta: CodecSnapshotMeta,
    /// Accumulated data chunks
    data: Vec<u8>,
    /// Expected next offset
    next_offset: u64,
    /// Last activity timestamp for timeout
    last_activity: Instant,
}

impl SnapshotAccumulator {
    fn new(vote: CodecVote, meta: CodecSnapshotMeta) -> Self {
        Self {
            vote,
            meta,
            data: Vec::new(),
            next_offset: 0,
            last_activity: Instant::now(),
        }
    }

    /// Append a chunk of data at the expected offset
    /// Returns true if the chunk was accepted, false if offset mismatch
    fn append_chunk(&mut self, offset: u64, chunk: &[u8]) -> bool {
        if offset != self.next_offset {
            tracing::warn!(
                "Snapshot chunk offset mismatch: expected {}, got {}",
                self.next_offset,
                offset
            );
            return false;
        }
        self.data.extend_from_slice(chunk);
        self.next_offset = offset + chunk.len() as u64;
        self.last_activity = Instant::now();
        true
    }

    /// Check if this accumulator has timed out
    fn is_expired(&self, timeout: Duration) -> bool {
        self.last_activity.elapsed() > timeout
    }
}

/// Manages in-progress snapshot transfers across all groups
struct SnapshotTransferManager {
    /// In-progress transfers keyed by (group_id, snapshot_id)
    transfers: DashMap<SnapshotTransferKey, SnapshotAccumulator>,
    /// Timeout for incomplete transfers (default 5 minutes)
    transfer_timeout: Duration,
}

impl SnapshotTransferManager {
    fn new(transfer_timeout: Duration) -> Self {
        Self {
            transfers: DashMap::new(),
            transfer_timeout,
        }
    }

    /// Get or create an accumulator for a snapshot transfer
    fn get_or_create(
        &self,
        group_id: u64,
        snapshot_id: String,
        vote: CodecVote,
        meta: CodecSnapshotMeta,
    ) -> dashmap::mapref::one::RefMut<'_, SnapshotTransferKey, SnapshotAccumulator> {
        let key = SnapshotTransferKey {
            group_id,
            snapshot_id: snapshot_id.clone(),
        };

        self.transfers
            .entry(key)
            .or_insert_with(|| SnapshotAccumulator::new(vote, meta))
    }

    /// Remove a completed or aborted transfer
    fn remove(&self, group_id: u64, snapshot_id: &str) -> Option<SnapshotAccumulator> {
        let key = SnapshotTransferKey {
            group_id,
            snapshot_id: snapshot_id.to_string(),
        };
        self.transfers.remove(&key).map(|(_, v)| v)
    }

    /// Clean up expired transfers
    fn cleanup_expired(&self) {
        let timeout = self.transfer_timeout;
        self.transfers.retain(|_, acc| !acc.is_expired(timeout));
    }
}

/// RPC server configuration
#[derive(Debug, Clone)]
pub struct ManiacRpcServerConfig {
    /// Address to bind to
    pub bind_addr: SocketAddr,
    /// Max number of concurrent connections
    pub max_connections: usize,
    /// Connection read timeout (idle timeout)
    pub connection_timeout: std::time::Duration,
    /// Maximum number of concurrent in-flight requests per connection.
    /// Default: 256
    pub max_concurrent_requests: usize,
    /// Timeout for incomplete snapshot transfers.
    /// Default: 5 minutes
    pub snapshot_transfer_timeout: Duration,
}

impl Default for ManiacRpcServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:5000".parse().unwrap(),
            max_connections: 1000,
            connection_timeout: std::time::Duration::from_secs(60),
            max_concurrent_requests: 256,
            snapshot_transfer_timeout: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// RPC server for handling incoming Raft requests with true multiplexing
pub struct ManiacRpcServer<C, T, S>
where
    C: RaftTypeConfig,
    T: MultiplexedTransport<C>,
    S: MultiRaftLogStorage<C>,
{
    config: ManiacRpcServerConfig,
    manager: Arc<MultiRaftManager<C, T, S>>,
    /// Active connection count
    active_connections: AtomicU64,
    /// Manages in-progress chunked snapshot transfers
    snapshot_transfers: Arc<SnapshotTransferManager>,
    _phantom: PhantomData<(C, T, S)>,
}

impl<C, T, S> ManiacRpcServer<C, T, S>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
            Vote = openraft::impls::Vote<C>,
            Node = openraft::impls::BasicNode,
            Entry = openraft::impls::Entry<C>,
            SnapshotData = std::io::Cursor<Vec<u8>>,
        >,
    C::Entry: Clone,
    C::D: crate::multi::codec::ToCodec<crate::multi::codec::RawBytes>
        + crate::multi::codec::FromCodec<crate::multi::codec::RawBytes>,
    T: MultiplexedTransport<C>,
    S: MultiRaftLogStorage<C>,
{
    /// Create a new RPC server
    pub fn new(config: ManiacRpcServerConfig, manager: Arc<MultiRaftManager<C, T, S>>) -> Self {
        let snapshot_transfers = Arc::new(SnapshotTransferManager::new(
            config.snapshot_transfer_timeout,
        ));
        Self {
            config,
            manager,
            active_connections: AtomicU64::new(0),
            snapshot_transfers,
            _phantom: PhantomData,
        }
    }

    /// Start the server and listen for connections
    pub async fn serve(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        let actual_addr = listener.local_addr()?;

        tracing::info!(
            "Raft RPC server listening on: {} (max_concurrent={})",
            actual_addr,
            self.config.max_concurrent_requests
        );

        loop {
            let (stream, peer_addr) = listener.accept().await?;

            // Check connection limit
            let current = self.active_connections.fetch_add(1, Ordering::Relaxed);
            if current >= self.config.max_connections as u64 {
                self.active_connections.fetch_sub(1, Ordering::Relaxed);
                tracing::warn!(
                    "Connection limit reached, rejecting connection from {}",
                    peer_addr
                );
                continue;
            }

            let server = self.clone();

            // Spawn connection handler
            tokio::spawn(async move {
                match server
                    .handle_multiplexed_connection(stream, peer_addr)
                    .await
                {
                    Ok(_) => {
                        tracing::debug!("Connection from {} closed gracefully", peer_addr);
                    }
                    Err(e) => {
                        tracing::debug!("Connection from {} closed: {}", peer_addr, e);
                    }
                }
                server.active_connections.fetch_sub(1, Ordering::Relaxed);
            });
        }
    }

    /// Handle a multiplexed connection with true out-of-order response support
    ///
    /// Uses separate tasks for reading and writing to avoid busy-waiting
    async fn handle_multiplexed_connection(
        &self,
        stream: TcpStream,
        peer_addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::sync::atomic::{AtomicBool, Ordering};

        tracing::debug!(
            "New multiplexed connection from: {} (max_concurrent={})",
            peer_addr,
            self.config.max_concurrent_requests
        );

        // Shared connection alive flag
        let alive = Arc::new(AtomicBool::new(true));

        // Channel for responses from handler tasks - use flume for reliability
        let (response_tx, response_rx) = flume::bounded::<Vec<u8>>(256);

        // Split the stream using tokio's into_split()
        let (read_half, write_half) = stream.into_split();

        // Spawn writer task
        let alive_clone = alive.clone();
        let _writer_handle = tokio::spawn(async move {
            Self::response_writer_loop(write_half, response_rx, alive_clone).await;
        });

        // Run reader in current task
        let result = self
            .request_reader_loop(read_half, peer_addr, response_tx, alive.clone())
            .await;

        // Mark connection as done
        alive.store(false, Ordering::Release);

        result
    }

    /// Writer loop - sends responses back to client
    async fn response_writer_loop(
        mut write_half: tokio::net::tcp::OwnedWriteHalf,
        response_rx: flume::Receiver<Vec<u8>>,
        alive: Arc<std::sync::atomic::AtomicBool>,
    ) {
        use std::sync::atomic::Ordering;

        loop {
            if !alive.load(Ordering::Acquire) {
                tracing::debug!("RPC writer: connection no longer alive, exiting");
                return;
            }

            match response_rx.recv_async().await {
                Ok(response_data) => {
                    tracing::debug!(
                        "RPC writer: sending response ({} bytes)",
                        response_data.len()
                    );
                    // Use vectored I/O to avoid copying the payload
                    match write_frame_vectored(&mut write_half, response_data).await {
                        Ok(returned_buf) => {
                            // Return the buffer to the thread-local pool for reuse
                            return_encode_buffer(returned_buf);
                            tracing::debug!("RPC writer: response sent successfully");
                        }
                        Err(e) => {
                            tracing::error!("RPC writer: failed to write response: {}", e);
                            alive.store(false, Ordering::Release);
                            return;
                        }
                    }
                }
                Err(_) => {
                    // Channel closed
                    tracing::debug!("RPC writer: channel closed, exiting");
                    return;
                }
            }
        }
    }

    /// Reader loop - reads requests from client
    async fn request_reader_loop(
        &self,
        mut read_half: tokio::net::tcp::OwnedReadHalf,
        peer_addr: SocketAddr,
        response_tx: flume::Sender<Vec<u8>>,
        alive: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::sync::atomic::Ordering;

        loop {
            if !alive.load(Ordering::Acquire) {
                return Ok(());
            }

            // Wait for request from socket
            match timeout(self.config.connection_timeout, read_frame(&mut read_half)).await {
                Ok(Ok(request_data)) => {
                    // Decode request using zero-copy codec
                    let request: CodecRpcMessage<crate::multi::codec::RawBytes> =
                        match CodecRpcMessage::decode_from_slice(&request_data) {
                            Ok(req) => req,
                            Err(e) => {
                                tracing::error!(
                                    "RPC reader: failed to decode request from {}: {}",
                                    peer_addr,
                                    e
                                );
                                // Return error to close connection, preventing busy loop on garbage data
                                return Err(Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    e,
                                )));
                            }
                        };

                    let request_id = request.request_id();
                    tracing::debug!(
                        "RPC reader: received request {} from {}",
                        request_id,
                        peer_addr
                    );
                    let manager = self.manager.clone();
                    let snapshot_transfers = self.snapshot_transfers.clone();
                    let tx = response_tx.clone();
                    let max_concurrent_requests = self.config.max_concurrent_requests;

                    // Spawn handler for this request
                    tokio::spawn(async move {
                        tracing::debug!("RPC handler: processing request {}", request_id);
                        let response = Self::process_codec_request(
                            &manager,
                            &snapshot_transfers,
                            request,
                            max_concurrent_requests,
                        )
                        .await;
                        tracing::debug!(
                            "RPC handler: request {} processed, encoding response",
                            request_id
                        );

                        // Serialize and send response to writer
                        match response.encode_to_vec() {
                            Ok(response_data) => {
                                tracing::debug!(
                                    "RPC handler: sending response for request {} ({} bytes)",
                                    request_id,
                                    response_data.len()
                                );
                                if tx.send_async(response_data).await.is_err() {
                                    tracing::debug!(
                                        "RPC handler: response channel closed for request {}",
                                        request_id
                                    );
                                } else {
                                    tracing::debug!(
                                        "RPC handler: response for request {} sent to writer",
                                        request_id
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "RPC handler: failed to encode response for request {}: {}",
                                    request_id,
                                    e
                                );
                            }
                        }
                    });
                }
                Ok(Err(e)) => {
                    if let crate::multi::tcp_transport::ManiacTransportError::IoError(ref io_err) =
                        e
                    {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            tracing::debug!("RPC reader: connection closed by peer: {}", peer_addr);
                            return Ok(());
                        }
                    }
                    return Err(Box::new(e));
                }
                Err(_) => {
                    tracing::debug!("RPC reader: connection timeout from: {}", peer_addr);
                    return Ok(());
                }
            }
        }
    }

    /// Process a codec request and return a codec response
    async fn process_codec_request(
        manager: &Arc<MultiRaftManager<C, T, S>>,
        snapshot_transfers: &Arc<SnapshotTransferManager>,
        request: CodecRpcMessage<crate::multi::codec::RawBytes>,
        max_concurrent_requests: usize,
    ) -> CodecRpcMessage<crate::multi::codec::RawBytes> {
        use crate::multi::codec::{
            AppendEntriesResponse as CodecAppendEntriesResponse, FromCodec,
            InstallSnapshotResponse as CodecInstallSnapshotResponse, ToCodec,
            VoteResponse as CodecVoteResponse,
        };

        match request {
            CodecRpcMessage::AppendEntries {
                request_id,
                group_id,
                rpc,
            } => {
                tracing::trace!(
                    "Processing AppendEntries for group {} (req_id={}, entries={})",
                    group_id,
                    request_id,
                    rpc.entries.len()
                );

                if let Some(raft) = manager.get_group(group_id) {
                    // Convert codec types to raft types using FromCodec
                    let vote = openraft::impls::Vote::<C>::from_codec(rpc.vote);
                    let prev_log_id = rpc.prev_log_id.map(|l| openraft::LogId::<C>::from_codec(l));
                    let leader_commit = rpc
                        .leader_commit
                        .map(|l| openraft::LogId::<C>::from_codec(l));

                    // Convert entries using FromCodec trait - pre-allocate capacity
                    let entry_count = rpc.entries.len();
                    let mut entries: Vec<C::Entry> = Vec::with_capacity(entry_count);
                    for e in rpc.entries {
                        entries.push(openraft::impls::Entry::<C>::from_codec(e));
                    }
                    let raft_rpc = openraft::raft::AppendEntriesRequest {
                        vote,
                        prev_log_id,
                        entries,
                        leader_commit,
                    };

                    tracing::trace!(
                        "AppendEntries handler: calling raft.append_entries (req_id={}, entries={})",
                        request_id,
                        entry_count
                    );
                    match raft.append_entries(raft_rpc).await {
                        Ok(response) => {
                            tracing::trace!(
                                "AppendEntries handler: got Ok response (req_id={})",
                                request_id
                            );
                            let codec_response = match response {
                                openraft::raft::AppendEntriesResponse::Success => {
                                    CodecAppendEntriesResponse::Success
                                }
                                openraft::raft::AppendEntriesResponse::PartialSuccess(lid) => {
                                    CodecAppendEntriesResponse::PartialSuccess(
                                        lid.map(|l| l.to_codec()),
                                    )
                                }
                                openraft::raft::AppendEntriesResponse::Conflict => {
                                    CodecAppendEntriesResponse::Conflict
                                }
                                openraft::raft::AppendEntriesResponse::HigherVote(v) => {
                                    CodecAppendEntriesResponse::HigherVote(v.to_codec())
                                }
                            };
                            CodecRpcMessage::Response {
                                request_id,
                                message: CodecResponseMessage::AppendEntries(codec_response),
                            }
                        }
                        Err(e) => {
                            tracing::trace!(
                                "AppendEntries handler: got Err response (req_id={}): {}",
                                request_id,
                                e
                            );
                            CodecRpcMessage::Error {
                                request_id,
                                error: format!("AppendEntries failed: {}", e),
                            }
                        }
                    }
                } else {
                    CodecRpcMessage::Error {
                        request_id,
                        error: format!("Group {} not found", group_id),
                    }
                }
            }

            CodecRpcMessage::Vote {
                request_id,
                group_id,
                rpc,
            } => {
                tracing::trace!(
                    "Processing Vote for group {} (req_id={})",
                    group_id,
                    request_id
                );

                if let Some(raft) = manager.get_group(group_id) {
                    let vote = openraft::impls::Vote::<C>::from_codec(rpc.vote);
                    let last_log_id = rpc.last_log_id.map(|l| openraft::LogId::<C>::from_codec(l));
                    let raft_rpc = openraft::raft::VoteRequest { vote, last_log_id };

                    match raft.vote(raft_rpc).await {
                        Ok(response) => {
                            let codec_response = CodecVoteResponse {
                                vote: response.vote.to_codec(),
                                vote_granted: response.vote_granted,
                                last_log_id: response.last_log_id.map(|l| l.to_codec()),
                            };
                            CodecRpcMessage::Response {
                                request_id,
                                message: CodecResponseMessage::Vote(codec_response),
                            }
                        }
                        Err(e) => CodecRpcMessage::Error {
                            request_id,
                            error: format!("Vote failed: {}", e),
                        },
                    }
                } else {
                    CodecRpcMessage::Error {
                        request_id,
                        error: format!("Group {} not found", group_id),
                    }
                }
            }

            CodecRpcMessage::InstallSnapshot {
                request_id,
                group_id,
                rpc,
            } => {
                let snapshot_id = rpc.meta.snapshot_id.clone();
                tracing::trace!(
                    "Processing InstallSnapshot for group {} (req_id={}, snapshot_id={}, offset={}, done={}, data_len={})",
                    group_id,
                    request_id,
                    snapshot_id,
                    rpc.offset,
                    rpc.done,
                    rpc.data.0.len()
                );

                if let Some(raft) = manager.get_group(group_id) {
                    // Clean up any expired transfers periodically
                    snapshot_transfers.cleanup_expired();

                    if rpc.offset == 0 && rpc.done {
                        // Full snapshot in one piece - no accumulation needed
                        let vote = openraft::impls::Vote::<C>::from_codec(rpc.vote);
                        let meta = openraft::storage::SnapshotMeta::<C>::from_codec(rpc.meta);
                        let snapshot = openraft::storage::Snapshot {
                            meta,
                            snapshot: std::io::Cursor::new(rpc.data.0),
                        };

                        match raft.install_full_snapshot(vote, snapshot).await {
                            Ok(response) => {
                                let codec_response = CodecInstallSnapshotResponse {
                                    vote: response.vote.to_codec(),
                                };
                                CodecRpcMessage::Response {
                                    request_id,
                                    message: CodecResponseMessage::InstallSnapshot(codec_response),
                                }
                            }
                            Err(e) => CodecRpcMessage::Error {
                                request_id,
                                error: format!("InstallSnapshot failed: {}", e),
                            },
                        }
                    } else if rpc.offset == 0 {
                        // First chunk of a multi-chunk transfer - create accumulator
                        tracing::debug!(
                            "Starting chunked snapshot transfer for group {} (snapshot_id={}, first_chunk_len={})",
                            group_id,
                            snapshot_id,
                            rpc.data.0.len()
                        );

                        let mut acc = snapshot_transfers.get_or_create(
                            group_id,
                            snapshot_id.clone(),
                            rpc.vote.clone(),
                            rpc.meta.clone(),
                        );

                        // Append the first chunk
                        if !acc.append_chunk(rpc.offset, &rpc.data.0) {
                            drop(acc);
                            snapshot_transfers.remove(group_id, &snapshot_id);
                            return CodecRpcMessage::Error {
                                request_id,
                                error: "Snapshot chunk offset mismatch".to_string(),
                            };
                        }

                        // Return success to continue receiving chunks
                        let codec_response = CodecInstallSnapshotResponse { vote: rpc.vote };
                        CodecRpcMessage::Response {
                            request_id,
                            message: CodecResponseMessage::InstallSnapshot(codec_response),
                        }
                    } else {
                        // Subsequent chunk - append to existing accumulator
                        let key = SnapshotTransferKey {
                            group_id,
                            snapshot_id: snapshot_id.clone(),
                        };

                        if let Some(mut acc) = snapshot_transfers.transfers.get_mut(&key) {
                            let expected_offset = acc.next_offset;
                            if !acc.append_chunk(rpc.offset, &rpc.data.0) {
                                drop(acc);
                                snapshot_transfers.remove(group_id, &snapshot_id);
                                return CodecRpcMessage::Error {
                                    request_id,
                                    error: format!(
                                        "Snapshot chunk offset mismatch: expected {}, got {}",
                                        expected_offset, rpc.offset
                                    ),
                                };
                            }

                            if rpc.done {
                                // Final chunk - install the complete snapshot
                                let vote = openraft::impls::Vote::<C>::from_codec(acc.vote.clone());
                                let meta = openraft::storage::SnapshotMeta::<C>::from_codec(
                                    acc.meta.clone(),
                                );
                                let data = std::mem::take(&mut acc.data);
                                drop(acc);

                                // Remove the accumulator since we're done
                                snapshot_transfers.remove(group_id, &snapshot_id);

                                tracing::debug!(
                                    "Completed chunked snapshot transfer for group {} (snapshot_id={}, total_len={})",
                                    group_id,
                                    snapshot_id,
                                    data.len()
                                );

                                let snapshot = openraft::storage::Snapshot {
                                    meta,
                                    snapshot: std::io::Cursor::new(data),
                                };

                                match raft.install_full_snapshot(vote, snapshot).await {
                                    Ok(response) => {
                                        let codec_response = CodecInstallSnapshotResponse {
                                            vote: response.vote.to_codec(),
                                        };
                                        CodecRpcMessage::Response {
                                            request_id,
                                            message: CodecResponseMessage::InstallSnapshot(
                                                codec_response,
                                            ),
                                        }
                                    }
                                    Err(e) => CodecRpcMessage::Error {
                                        request_id,
                                        error: format!("InstallSnapshot failed: {}", e),
                                    },
                                }
                            } else {
                                // More chunks to come
                                tracing::trace!(
                                    "Received snapshot chunk for group {} (snapshot_id={}, offset={}, accumulated={})",
                                    group_id,
                                    snapshot_id,
                                    rpc.offset,
                                    acc.next_offset
                                );

                                let codec_response =
                                    CodecInstallSnapshotResponse { vote: rpc.vote };
                                CodecRpcMessage::Response {
                                    request_id,
                                    message: CodecResponseMessage::InstallSnapshot(codec_response),
                                }
                            }
                        } else {
                            // No accumulator found - this is an error (received non-first chunk without first chunk)
                            tracing::warn!(
                                "Received snapshot chunk without first chunk for group {} (snapshot_id={}, offset={})",
                                group_id,
                                snapshot_id,
                                rpc.offset
                            );
                            CodecRpcMessage::Error {
                                request_id,
                                error: format!(
                                    "Snapshot transfer not found for snapshot_id={} (received offset {} without starting chunk)",
                                    snapshot_id, rpc.offset
                                ),
                            }
                        }
                    }
                } else {
                    CodecRpcMessage::Error {
                        request_id,
                        error: format!("Group {} not found", group_id),
                    }
                }
            }

            CodecRpcMessage::HeartbeatBatch {
                request_id,
                group_id,
                rpc,
            } => {
                tracing::trace!(
                    "Processing HeartbeatBatch for group {} (req_id={})",
                    group_id,
                    request_id
                );

                if let Some(raft) = manager.get_group(group_id) {
                    // Heartbeats typically have no entries
                    let vote = openraft::impls::Vote::<C>::from_codec(rpc.vote);
                    let prev_log_id = rpc.prev_log_id.map(|l| openraft::LogId::<C>::from_codec(l));
                    let leader_commit = rpc
                        .leader_commit
                        .map(|l| openraft::LogId::<C>::from_codec(l));

                    // Convert entries (typically empty for heartbeats) - pre-allocate capacity
                    let mut entries: Vec<C::Entry> = Vec::with_capacity(rpc.entries.len());
                    for e in rpc.entries {
                        entries.push(openraft::impls::Entry::<C>::from_codec(e));
                    }

                    let raft_rpc = openraft::raft::AppendEntriesRequest {
                        vote,
                        prev_log_id,
                        entries,
                        leader_commit,
                    };

                    match raft.append_entries(raft_rpc).await {
                        Ok(response) => {
                            let codec_response = match response {
                                openraft::raft::AppendEntriesResponse::Success => {
                                    CodecAppendEntriesResponse::Success
                                }
                                openraft::raft::AppendEntriesResponse::PartialSuccess(lid) => {
                                    CodecAppendEntriesResponse::PartialSuccess(
                                        lid.map(|l| l.to_codec()),
                                    )
                                }
                                openraft::raft::AppendEntriesResponse::Conflict => {
                                    CodecAppendEntriesResponse::Conflict
                                }
                                openraft::raft::AppendEntriesResponse::HigherVote(v) => {
                                    CodecAppendEntriesResponse::HigherVote(v.to_codec())
                                }
                            };
                            CodecRpcMessage::Response {
                                request_id,
                                message: CodecResponseMessage::AppendEntries(codec_response),
                            }
                        }
                        Err(e) => CodecRpcMessage::Error {
                            request_id,
                            error: format!("HeartbeatBatch failed: {}", e),
                        },
                    }
                } else {
                    CodecRpcMessage::Error {
                        request_id,
                        error: format!("Group {} not found", group_id),
                    }
                }
            }

            CodecRpcMessage::HeartbeatBatchMulti {
                request_id,
                heartbeats,
            } => {
                use futures::stream::{self, StreamExt, TryStreamExt};

                tracing::trace!(
                    "Processing HeartbeatBatchMulti (req_id={}, groups={})",
                    request_id,
                    heartbeats.len()
                );

                // Bound the amount of parallel work triggered by a single batch.
                // We cap at max_concurrent_requests (per connection) but also at the batch size.
                let concurrency = std::cmp::max(
                    1usize,
                    std::cmp::min(max_concurrent_requests, heartbeats.len()),
                );

                let responses: Result<Vec<(u64, CodecAppendEntriesResponse)>, String> =
                    stream::iter(heartbeats.into_iter())
                        .map(|(group_id, rpc)| {
                            let manager = manager.clone();
                            async move {
                                if let Some(raft) = manager.get_group(group_id) {
                                    let vote = openraft::impls::Vote::<C>::from_codec(rpc.vote);
                                    let prev_log_id = rpc
                                        .prev_log_id
                                        .map(|l| openraft::LogId::<C>::from_codec(l));
                                    let leader_commit = rpc
                                        .leader_commit
                                        .map(|l| openraft::LogId::<C>::from_codec(l));

                                    // Pre-allocate entry conversion vector
                                    let mut entries: Vec<C::Entry> =
                                        Vec::with_capacity(rpc.entries.len());
                                    for e in rpc.entries {
                                        entries.push(openraft::impls::Entry::<C>::from_codec(e));
                                    }

                                    let raft_rpc = openraft::raft::AppendEntriesRequest {
                                        vote,
                                        prev_log_id,
                                        entries,
                                        leader_commit,
                                    };

                                    match raft.append_entries(raft_rpc).await {
                                        Ok(response) => {
                                            let codec_response = match response {
                                        openraft::raft::AppendEntriesResponse::Success => {
                                            CodecAppendEntriesResponse::Success
                                        }
                                        openraft::raft::AppendEntriesResponse::PartialSuccess(
                                            lid,
                                        ) => CodecAppendEntriesResponse::PartialSuccess(
                                            lid.map(|l| l.to_codec()),
                                        ),
                                        openraft::raft::AppendEntriesResponse::Conflict => {
                                            CodecAppendEntriesResponse::Conflict
                                        }
                                        openraft::raft::AppendEntriesResponse::HigherVote(v) => {
                                            CodecAppendEntriesResponse::HigherVote(v.to_codec())
                                        }
                                    };

                                            Ok((group_id, codec_response))
                                        }
                                        Err(e) => Err(format!(
                                            "HeartbeatBatchMulti failed for group {}: {}",
                                            group_id, e
                                        )),
                                    }
                                } else {
                                    Err(format!("Group {} not found", group_id))
                                }
                            }
                        })
                        .buffer_unordered(concurrency)
                        .try_collect()
                        .await;

                match responses {
                    Ok(responses) => CodecRpcMessage::HeartbeatBatchMultiResponse {
                        request_id,
                        responses,
                    },
                    Err(e) => CodecRpcMessage::Error {
                        request_id,
                        error: e,
                    },
                }
            }

            CodecRpcMessage::Response { request_id, .. }
            | CodecRpcMessage::BatchResponse { request_id, .. }
            | CodecRpcMessage::HeartbeatBatchMultiResponse { request_id, .. }
            | CodecRpcMessage::Error { request_id, .. } => CodecRpcMessage::Error {
                request_id,
                error: "Invalid request type: received response message as request".to_string(),
            },
        }
    }
}

// Re-export message types for use by both client and server
pub mod protocol {
    //! Shared protocol types for transport (serde-based, kept for compatibility)
    //!
    //! Note: The actual wire protocol now uses the zero-copy codec from `codec.rs`.
    //! These types are kept for API compatibility.

    use openraft::RaftTypeConfig;
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    };

    /// RPC message wrapper (serde version for compatibility)
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    #[serde(bound = "")]
    pub enum RpcMessage<C: RaftTypeConfig> {
        AppendEntries {
            request_id: u64,
            group_id: u64,
            rpc: AppendEntriesRequest<C>,
        },
        Vote {
            request_id: u64,
            group_id: u64,
            rpc: VoteRequest<C>,
        },
        InstallSnapshot {
            request_id: u64,
            group_id: u64,
            rpc: InstallSnapshotRequest<C>,
        },
        HeartbeatBatch {
            request_id: u64,
            group_id: u64,
            rpc: AppendEntriesRequest<C>,
        },
        Response {
            request_id: u64,
            message: ResponseMessage<C>,
        },
        BatchResponse {
            request_id: u64,
            responses: Vec<(u64, ResponseMessage<C>)>,
        },
        Error {
            request_id: u64,
            error: String,
        },
    }

    impl<C: RaftTypeConfig> RpcMessage<C> {
        pub fn request_id(&self) -> u64 {
            match self {
                RpcMessage::AppendEntries { request_id, .. } => *request_id,
                RpcMessage::Vote { request_id, .. } => *request_id,
                RpcMessage::InstallSnapshot { request_id, .. } => *request_id,
                RpcMessage::HeartbeatBatch { request_id, .. } => *request_id,
                RpcMessage::Response { request_id, .. } => *request_id,
                RpcMessage::BatchResponse { request_id, .. } => *request_id,
                RpcMessage::Error { request_id, .. } => *request_id,
            }
        }
    }

    /// Response message wrapper
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    #[serde(bound = "")]
    pub enum ResponseMessage<C: RaftTypeConfig> {
        AppendEntries(AppendEntriesResponse<C>),
        Vote(VoteResponse<C>),
        InstallSnapshot(InstallSnapshotResponse<C>),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multi::codec::{RawBytes, SnapshotMeta as CodecSnapshotMeta, Vote as CodecVote};
    use std::time::{Duration, Instant};

    #[test]
    fn test_snapshot_accumulator_append_chunk() {
        let vote = CodecVote {
            leader_id: crate::multi::codec::LeaderId {
                term: 1,
                node_id: 1,
            },
            committed: true,
        };
        let meta = CodecSnapshotMeta {
            last_log_id: None,
            last_membership: crate::multi::codec::StoredMembership::default(),
            snapshot_id: "test-snap".to_string(),
        };
        let mut acc = SnapshotAccumulator::new(vote.clone(), meta.clone());

        // Append first chunk
        assert!(acc.append_chunk(0, b"chunk1"));
        assert_eq!(acc.next_offset, 6);
        assert_eq!(acc.data, b"chunk1");

        // Append second chunk
        assert!(acc.append_chunk(6, b"chunk2"));
        assert_eq!(acc.next_offset, 12);
        assert_eq!(acc.data, b"chunk1chunk2");

        // Offset mismatch should fail
        assert!(!acc.append_chunk(10, b"bad"));
        assert_eq!(acc.data, b"chunk1chunk2"); // Should not have changed
    }

    #[test]
    fn test_snapshot_accumulator_expiry() {
        let vote = CodecVote {
            leader_id: crate::multi::codec::LeaderId {
                term: 1,
                node_id: 1,
            },
            committed: true,
        };
        let meta = CodecSnapshotMeta {
            last_log_id: None,
            last_membership: crate::multi::codec::StoredMembership::default(),
            snapshot_id: "test-snap".to_string(),
        };
        let mut acc = SnapshotAccumulator::new(vote, meta);

        // Should not be expired immediately
        assert!(!acc.is_expired(Duration::from_secs(60)));

        // Manually set last_activity to past
        acc.last_activity = Instant::now() - Duration::from_secs(120);
        assert!(acc.is_expired(Duration::from_secs(60)));
    }

    #[test]
    fn test_snapshot_transfer_manager() {
        let manager = SnapshotTransferManager::new(Duration::from_secs(60));

        let vote = CodecVote {
            leader_id: crate::multi::codec::LeaderId {
                term: 1,
                node_id: 1,
            },
            committed: true,
        };
        let meta = CodecSnapshotMeta {
            last_log_id: None,
            last_membership: crate::multi::codec::StoredMembership::default(),
            snapshot_id: "snap-1".to_string(),
        };

        // Get or create accumulator
        let mut acc = manager.get_or_create(1, "snap-1".to_string(), vote.clone(), meta.clone());
        acc.append_chunk(0, b"data");
        // IMPORTANT: drop the DashMap guard before re-entering `get_or_create` on the same key,
        // otherwise we can deadlock by trying to take the same shard lock twice.
        drop(acc);

        // Should be able to retrieve it
        let acc2 = manager.get_or_create(1, "snap-1".to_string(), vote, meta);
        assert_eq!(acc2.data.len(), 4);
        // Same re-entrancy issue: release guard before calling into the manager again.
        drop(acc2);

        // Remove it
        let removed = manager.remove(1, "snap-1");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().data, b"data");

        // Should be gone
        let removed_again = manager.remove(1, "snap-1");
        assert!(removed_again.is_none());
    }

    #[test]
    fn test_snapshot_transfer_manager_cleanup_expired() {
        let manager = SnapshotTransferManager::new(Duration::from_secs(1));

        let vote = CodecVote {
            leader_id: crate::multi::codec::LeaderId {
                term: 1,
                node_id: 1,
            },
            committed: true,
        };
        let meta = CodecSnapshotMeta {
            last_log_id: None,
            last_membership: crate::multi::codec::StoredMembership::default(),
            snapshot_id: "snap-1".to_string(),
        };

        // Create an accumulator
        let mut acc = manager.get_or_create(1, "snap-1".to_string(), vote, meta);
        // Manually expire it
        acc.last_activity = Instant::now() - Duration::from_secs(10);
        drop(acc);

        // Cleanup should remove it
        manager.cleanup_expired();
        let removed = manager.remove(1, "snap-1");
        assert!(removed.is_none());
    }

    #[test]
    fn test_rpc_server_config_default() {
        let config = ManiacRpcServerConfig::default();
        assert_eq!(config.max_connections, 1000);
        assert_eq!(config.max_concurrent_requests, 256);
        assert_eq!(config.snapshot_transfer_timeout, Duration::from_secs(300));
    }
}

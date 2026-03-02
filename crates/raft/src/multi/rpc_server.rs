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
    Decode, RawBytes, ResponseMessage as CodecResponseMessage, RpcMessage as CodecRpcMessage,
    SnapshotMeta as CodecSnapshotMeta, Vote as CodecVote,
};
use crate::multi::manager::MultiRaftManager;
use crate::multi::network::MultiplexedTransport;
use crate::multi::storage::MultiRaftLogStorage;
use crate::multi::tcp_transport::{BoxedReader, BoxedWriter, FRAME_PREFIX_LEN, encode_framed_append};
use bytes::{Buf, BytesMut};
use dashmap::DashMap;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use openraft::RaftTypeConfig;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::time::timeout;

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
    /// Counter for periodic cleanup (every N calls)
    cleanup_counter: AtomicU64,
}

impl SnapshotTransferManager {
    fn new(transfer_timeout: Duration) -> Self {
        Self {
            transfers: DashMap::new(),
            transfer_timeout,
            cleanup_counter: AtomicU64::new(0),
        }
    }

    /// Periodically clean up expired transfers (every 64 calls)
    fn maybe_cleanup_expired(&self) {
        let count = self.cleanup_counter.fetch_add(1, Ordering::Relaxed);
        if count % 64 == 0 {
            self.cleanup_expired();
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
pub struct BisqueRpcServerConfig {
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
    /// TLS server configuration for incoming connections.
    /// When set, all incoming connections require TLS.
    #[cfg(feature = "tls")]
    pub tls_server_config: Option<Arc<rustls::ServerConfig>>,
}

impl Default for BisqueRpcServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:5000".parse().unwrap(),
            max_connections: 1000,
            connection_timeout: std::time::Duration::from_secs(60),
            max_concurrent_requests: 256,
            snapshot_transfer_timeout: Duration::from_secs(300), // 5 minutes
            #[cfg(feature = "tls")]
            tls_server_config: None,
        }
    }
}

/// RPC server for handling incoming Raft requests with true multiplexing
pub struct BisqueRpcServer<C, T, S>
where
    C: RaftTypeConfig,
    T: MultiplexedTransport<C>,
    S: MultiRaftLogStorage<C>,
{
    config: BisqueRpcServerConfig,
    manager: Arc<MultiRaftManager<C, T, S>>,
    /// Active connection count
    active_connections: AtomicU64,
    /// Manages in-progress chunked snapshot transfers
    snapshot_transfers: Arc<SnapshotTransferManager>,
    /// Shutdown signal sender
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    /// Shutdown signal receiver (cloneable)
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    _phantom: PhantomData<(C, T, S)>,
}

impl<C, T, S> BisqueRpcServer<C, T, S>
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
    pub fn new(config: BisqueRpcServerConfig, manager: Arc<MultiRaftManager<C, T, S>>) -> Self {
        let snapshot_transfers = Arc::new(SnapshotTransferManager::new(
            config.snapshot_transfer_timeout,
        ));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        Self {
            config,
            manager,
            active_connections: AtomicU64::new(0),
            snapshot_transfers,
            shutdown_tx,
            shutdown_rx,
            _phantom: PhantomData,
        }
    }

    /// Trigger graceful shutdown of the server.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Start the server and listen for connections.
    /// Returns when the shutdown token is cancelled or on fatal error.
    pub async fn serve(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        let actual_addr = listener.local_addr()?;

        #[cfg(feature = "tls")]
        let tls_acceptor = self
            .config
            .tls_server_config
            .as_ref()
            .map(|cfg| tokio_rustls::TlsAcceptor::from(cfg.clone()));

        tracing::info!(
            "Raft RPC server listening on: {} (max_concurrent={})",
            actual_addr,
            self.config.max_concurrent_requests
        );

        let max_connections = self.config.max_connections as u64;

        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            // Wait for either a new connection or shutdown
            let accept_result = tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    tracing::info!("RPC server shutting down");
                    return Ok(());
                }
                result = listener.accept() => result,
            };

            let (stream, peer_addr): (tokio::net::TcpStream, SocketAddr) = accept_result?;

            // Disable Nagle's algorithm for low-latency RPC responses
            let _ = stream.set_nodelay(true);

            // Atomically check and increment connection count (CAS loop)
            let accepted = loop {
                let current = self.active_connections.load(Ordering::Relaxed);
                if current >= max_connections {
                    break false;
                }
                if self
                    .active_connections
                    .compare_exchange_weak(
                        current,
                        current + 1,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    break true;
                }
            };

            if !accepted {
                tracing::warn!(
                    "Connection limit reached, rejecting connection from {}",
                    peer_addr
                );
                continue;
            }

            let server = self.clone();

            #[cfg(feature = "tls")]
            let tls_acceptor = tls_acceptor.clone();

            // Spawn connection handler (long-lived: one per connection)
            tokio::spawn(async move {
                // Split the stream into read/write halves, optionally wrapping with TLS
                let (reader, writer): (BoxedReader, BoxedWriter) = {
                    #[cfg(feature = "tls")]
                    if let Some(ref acceptor) = tls_acceptor {
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                let (r, w) = tokio::io::split(tls_stream);
                                (Box::new(r), Box::new(w))
                            }
                            Err(e) => {
                                tracing::debug!("TLS handshake failed from {}: {}", peer_addr, e);
                                server.active_connections.fetch_sub(1, Ordering::Relaxed);
                                return;
                            }
                        }
                    } else {
                        let (r, w) = stream.into_split();
                        (Box::new(r) as BoxedReader, Box::new(w) as BoxedWriter)
                    }

                    #[cfg(not(feature = "tls"))]
                    {
                        let (r, w) = stream.into_split();
                        (Box::new(r) as BoxedReader, Box::new(w) as BoxedWriter)
                    }
                };

                match server
                    .handle_multiplexed_connection(reader, writer, peer_addr)
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

    /// Handle a multiplexed connection with true out-of-order response support.
    ///
    /// Architecture: two long-lived tasks per connection (no per-request spawns).
    /// - Writer task: drains response channel, batches multiple responses into a
    ///   single write when possible.
    /// - Reader+dispatcher (current task): reads frames, processes requests via a
    ///   bounded `FuturesUnordered` (provides backpressure at `max_concurrent_requests`),
    ///   and feeds completed responses to the writer channel.
    async fn handle_multiplexed_connection(
        &self,
        read_half: BoxedReader,
        write_half: BoxedWriter,
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

        // Channel for response messages (structured data, encoding deferred to writer)
        let (response_tx, response_rx) =
            crossfire::mpsc::bounded_async::<CodecRpcMessage<RawBytes>>(256);

        // Spawn writer task (long-lived: one per connection)
        let alive_writer = alive.clone();
        let writer_handle = tokio::spawn(async move {
            Self::response_writer_loop(write_half, response_rx, alive_writer).await;
        });

        // Run reader+dispatcher in current task
        let result = self
            .request_reader_loop(read_half, peer_addr, response_tx, alive.clone())
            .await;

        // Mark connection as done and wait for writer to finish
        alive.store(false, Ordering::Release);
        let _ = writer_handle.await;

        result
    }

    /// Writer loop — receives structured response messages and encodes into a
    /// single reusable buffer. Zero allocation after warmup.
    ///
    /// Encoding is deferred to the writer instead of the reader, so:
    /// - No per-response `Vec<u8>` allocation from `encode_framed` / TLS pool
    /// - Single reusable `encode_buf` grows to steady-state and stays there
    /// - Batch: multiple responses are appended into the same buffer, one `write_all`
    async fn response_writer_loop(
        mut write_half: BoxedWriter,
        response_rx: crossfire::AsyncRx<crossfire::mpsc::Array<CodecRpcMessage<RawBytes>>>,
        alive: Arc<std::sync::atomic::AtomicBool>,
    ) {
        use crossfire::TryRecvError;
        use std::sync::atomic::Ordering;
        use tokio::io::AsyncWriteExt;

        // Single reusable buffer — grows to max response size, zero alloc after warmup
        let mut encode_buf: Vec<u8> = Vec::with_capacity(4096);

        loop {
            if !alive.load(Ordering::Acquire) {
                tracing::trace!("RPC writer: connection no longer alive, exiting");
                return;
            }

            // Block for the first response message
            let first = match response_rx.recv().await {
                Ok(msg) => msg,
                Err(_) => {
                    tracing::trace!("RPC writer: channel closed, exiting");
                    return;
                }
            };

            // Encode first response into reusable buffer
            encode_buf.clear();
            if let Err(e) = encode_framed_append(&first, &mut encode_buf) {
                tracing::error!("RPC writer: failed to encode response: {}", e);
                continue;
            }

            // Drain all immediately ready responses into the same buffer (batch)
            loop {
                match response_rx.try_recv() {
                    Ok(msg) => {
                        if let Err(e) = encode_framed_append(&msg, &mut encode_buf) {
                            tracing::error!("RPC writer: failed to encode batched response: {}", e);
                        }
                    }
                    Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                }
            }

            // Single write_all for all batched responses
            if let Err(e) = write_half.write_all(&encode_buf).await {
                tracing::error!("RPC writer: failed to write response batch: {}", e);
                alive.store(false, Ordering::Release);
                return;
            }

            // Flush to ensure data is sent, especially important for TLS
            if let Err(e) = write_half.flush().await {
                tracing::error!("RPC writer: failed to flush: {}", e);
                alive.store(false, Ordering::Release);
                return;
            }
        }
    }

    /// Reader + dispatcher loop — reads frames and processes requests in-task.
    ///
    /// Uses `FuturesUnordered` for bounded concurrency (no per-request spawns).
    /// Backpressure: stops reading new frames when `in_flight` reaches
    /// `max_concurrent_requests`, resuming when a slot frees up.
    async fn request_reader_loop(
        &self,
        mut read_half: BoxedReader,
        peer_addr: SocketAddr,
        response_tx: crossfire::MAsyncTx<crossfire::mpsc::Array<CodecRpcMessage<RawBytes>>>,
        alive: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::sync::atomic::Ordering;
        use tokio::io::AsyncReadExt;

        let mut buf = BytesMut::with_capacity(64 * 1024);
        let max_concurrent = self.config.max_concurrent_requests;
        let mut in_flight = FuturesUnordered::new();
        let mut eof = false;

        loop {
            // Parse all complete frames from buffer, respecting concurrency limit
            while !eof && in_flight.len() < max_concurrent {
                if buf.len() < FRAME_PREFIX_LEN {
                    break;
                }
                let payload_len =
                    u32::from_le_bytes(buf[..FRAME_PREFIX_LEN].try_into().unwrap()) as usize;

                if payload_len == 0 {
                    buf.advance(FRAME_PREFIX_LEN);
                    continue;
                }

                if buf.len() < FRAME_PREFIX_LEN + payload_len {
                    break; // Incomplete frame, need more data
                }

                // Complete frame — decode directly from buffer
                let frame_start = FRAME_PREFIX_LEN;
                let frame_end = FRAME_PREFIX_LEN + payload_len;
                let request: CodecRpcMessage<RawBytes> =
                    match CodecRpcMessage::decode_from_slice(&buf[frame_start..frame_end]) {
                        Ok(req) => req,
                        Err(e) => {
                            tracing::error!(
                                "RPC reader: failed to decode request from {}: {}",
                                peer_addr,
                                e
                            );
                            return Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                e,
                            )));
                        }
                    };

                buf.advance(FRAME_PREFIX_LEN + payload_len);

                let request_id = request.request_id();
                tracing::trace!(
                    "RPC reader: received request {} from {}",
                    request_id,
                    peer_addr
                );

                let manager = self.manager.clone();
                let snapshot_transfers = self.snapshot_transfers.clone();
                let mcr = max_concurrent;

                // Process in FuturesUnordered — no tokio::spawn, no Box::pin, no encoding
                // Response message is sent as structured data; encoding deferred to writer
                in_flight.push(async move {
                    Self::process_codec_request(&manager, &snapshot_transfers, request, mcr)
                        .await
                });
            }

            // If EOF and no more in-flight work, we're done
            if eof && in_flight.is_empty() {
                return Ok(());
            }

            if !alive.load(Ordering::Acquire) {
                return Ok(());
            }

            // Reserve space if running low
            if buf.capacity() - buf.len() < 4096 {
                buf.reserve(64 * 1024);
            }

            // Multiplex: drive in-flight futures AND read more data
            if eof {
                // No more reads, just drain remaining futures
                if let Some(response) = in_flight.next().await {
                    if response_tx.send(response).await.is_err() {
                        tracing::trace!("RPC reader: response channel closed");
                        return Ok(());
                    }
                }
            } else if in_flight.is_empty() {
                // Nothing in flight — just read (avoids polling empty FuturesUnordered)
                match timeout(self.config.connection_timeout, read_half.read_buf(&mut buf)).await {
                    Ok(Ok(0)) => {
                        tracing::trace!("RPC reader: connection closed by peer: {}", peer_addr);
                        eof = true;
                    }
                    Ok(Ok(_)) => {} // Data read, loop back to parse frames
                    Ok(Err(e)) => {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            tracing::trace!(
                                "RPC reader: connection closed by peer: {}",
                                peer_addr
                            );
                            eof = true;
                        } else {
                            return Err(Box::new(e));
                        }
                    }
                    Err(_) => {
                        tracing::trace!("RPC reader: connection timeout from: {}", peer_addr);
                        return Ok(());
                    }
                }
            } else if in_flight.len() >= max_concurrent {
                // At capacity — only drain futures (backpressure: stop reading)
                if let Some(response) = in_flight.next().await {
                    if response_tx.send(response).await.is_err() {
                        tracing::trace!("RPC reader: response channel closed");
                        return Ok(());
                    }
                }
            } else {
                // Both reading and processing concurrently
                tokio::select! {
                    biased;
                    // Prefer completing in-flight work
                    Some(response) = in_flight.next() => {
                        if response_tx.send(response).await.is_err() {
                            tracing::trace!("RPC reader: response channel closed");
                            return Ok(());
                        }
                    }
                    // Read more data
                    read_result = timeout(self.config.connection_timeout, read_half.read_buf(&mut buf)) => {
                        match read_result {
                            Ok(Ok(0)) => {
                                tracing::trace!("RPC reader: connection closed by peer: {}", peer_addr);
                                eof = true;
                            }
                            Ok(Ok(_)) => {} // Data read, loop back to parse
                            Ok(Err(e)) => {
                                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                                    tracing::trace!("RPC reader: connection closed by peer: {}", peer_addr);
                                    eof = true;
                                } else {
                                    return Err(Box::new(e));
                                }
                            }
                            Err(_) => {
                                tracing::trace!("RPC reader: connection timeout from: {}", peer_addr);
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }

    /// Process a codec request and return a codec response
    async fn process_codec_request(
        manager: &Arc<MultiRaftManager<C, T, S>>,
        snapshot_transfers: &Arc<SnapshotTransferManager>,
        request: CodecRpcMessage<RawBytes>,
        max_concurrent_requests: usize,
    ) -> CodecRpcMessage<RawBytes> {
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
                    // Periodic cleanup (every 64th call, not every request)
                    snapshot_transfers.maybe_cleanup_expired();

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

            CodecRpcMessage::Response { request_id, .. }
            | CodecRpcMessage::BatchResponse { request_id, .. }
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
        let config = BisqueRpcServerConfig::default();
        assert_eq!(config.max_connections, 1000);
        assert_eq!(config.max_concurrent_requests, 256);
        assert_eq!(config.snapshot_transfer_timeout, Duration::from_secs(300));
    }
}

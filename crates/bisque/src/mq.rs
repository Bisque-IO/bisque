//! MQ consumer protocol integration.
//!
//! Implements `MqRouter` (from bisque-mq-server) and provides the TCP accept
//! loop that bridges the binary wire protocol to `ConsumerHandler`.
//!
//! MQ is provisioned lazily when the first catalog with `engine: Mq` is created.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::BytesMut;
use dashmap::DashMap;
use openraft::BasicNode;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use bisque_mq::MqMetadata;
use bisque_mq::write_batcher::MqWriteBatcher;
use bisque_mq_protocol::frame::{
    decode_client_frame_bytes, encode_server_frame_prefixed, read_tcp_frame_bytes,
};
use bisque_mq_protocol::types::{ClientFrame, ServerFrame};
use bisque_mq_server::handler::{ConsumerHandler, ConsumerHandlerConfig, MqRouter};
use tokio::io::AsyncWriteExt;

use bisque_meta::engine::MetaEngine;
use bisque_meta::token::TokenManager;
use bisque_meta::types::EngineType;
use bisque_raft::{
    BisqueTcpTransport, BisqueTcpTransportConfig, DefaultNodeRegistry, MmapStorageConfig,
    MultiRaftManager, MultiplexedLogStorage, NodeAddressResolver,
};

type MqManager = MultiRaftManager<
    bisque_mq::MqTypeConfig,
    BisqueTcpTransport<bisque_mq::MqTypeConfig>,
    MultiplexedLogStorage<bisque_mq::MqTypeConfig>,
>;

/// Lazily-provisioned MQ engine state.
///
/// Created early during server startup but only provisions the MQ raft group
/// when the first `engine: Mq` catalog is created (or on restart if one exists).
pub struct MqState {
    inner: tokio::sync::Mutex<Option<MqInner>>,
    data_dir: PathBuf,
    node_id: u64,
    http_addr: SocketAddr,
    mq_addr: Option<SocketAddr>,
    token_manager: Arc<TokenManager>,
    meta_engine: Arc<MetaEngine>,
}

struct MqInner {
    raft_node: Arc<bisque_mq::MqRaftNode>,
    manager: Arc<MqManager>,
    #[allow(dead_code)]
    router: Arc<BisqueMqRouter>,
    tcp_addr: Option<SocketAddr>,
}

impl MqState {
    pub fn new(
        data_dir: PathBuf,
        node_id: u64,
        http_addr: SocketAddr,
        mq_addr: Option<SocketAddr>,
        token_manager: Arc<TokenManager>,
        meta_engine: Arc<MetaEngine>,
    ) -> Self {
        Self {
            inner: tokio::sync::Mutex::new(None),
            data_dir,
            node_id,
            http_addr,
            mq_addr,
            token_manager,
            meta_engine,
        }
    }

    /// Provision the MQ raft group if not already running. Idempotent.
    pub async fn ensure_provisioned(
        &self,
        group_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut guard = self.inner.lock().await;
        if guard.is_some() {
            return Ok(());
        }

        info!(group_id, "provisioning MQ engine");

        // MQ engine
        let mq_dir = self.data_dir.join("mq");
        std::fs::create_dir_all(&mq_dir)?;
        let mq_config = bisque_mq::MqConfig::new(&mq_dir);
        let mq_engine = bisque_mq::MqEngine::new(mq_config.clone());

        // Raft log storage
        let mq_raft_dir = self.data_dir.join("mq-raft-data");
        std::fs::create_dir_all(&mq_raft_dir)?;
        let mq_storage_config =
            MmapStorageConfig::new(&mq_raft_dir).with_segment_size(8 * 1024 * 1024);
        let mq_storage = MultiplexedLogStorage::new(mq_storage_config).await?;

        // Transport
        let mq_registry = Arc::new(DefaultNodeRegistry::new());
        mq_registry.register(self.node_id, self.http_addr);
        let mq_transport =
            BisqueTcpTransport::new(BisqueTcpTransportConfig::default(), mq_registry);

        // Multi-raft manager
        let mq_manager: Arc<MqManager> = Arc::new(MultiRaftManager::new(mq_transport, mq_storage));

        // MDBX manifest
        let mq_manifest_dir = self.data_dir.join("mq-manifest");
        std::fs::create_dir_all(&mq_manifest_dir)?;
        let mq_manifest = Arc::new(bisque_mq::MqManifestManager::new(&mq_manifest_dir)?);
        mq_manifest.open_group(group_id)?;

        // State machine
        let mq_state_machine =
            bisque_mq::MqStateMachine::new(mq_engine).with_manifest(mq_manifest.clone(), group_id);
        let mq_shared_metadata = mq_state_machine.shared_metadata();

        // Raft group
        let mq_raft_config = Arc::new(
            openraft::Config {
                heartbeat_interval: 200,
                election_timeout_min: 400,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()?,
        );

        let mq_raft = mq_manager
            .add_group(group_id, self.node_id, mq_raft_config, mq_state_machine)
            .await?;

        // Initialize single-node membership
        let mut members = BTreeMap::new();
        members.insert(self.node_id, BasicNode::default());
        match mq_raft.initialize(members).await {
            Ok(_) => info!(
                node_id = self.node_id,
                group_id, "MQ raft group initialized"
            ),
            Err(_) if mq_raft.is_initialized().await.unwrap_or(false) => {
                info!(
                    node_id = self.node_id,
                    group_id, "MQ raft group already initialized"
                );
            }
            Err(e) => return Err(Box::new(e)),
        }

        // MqRaftNode
        let mq_raft_node = Arc::new(
            bisque_mq::MqRaftNode::new(mq_raft.clone(), self.node_id, mq_config)
                .with_group_id(group_id)
                .with_metadata(mq_shared_metadata.clone())
                .with_manifest(mq_manifest),
        );
        mq_raft_node.start();

        // Wait for leadership
        {
            let mut elected = false;
            for _ in 0..40 {
                if mq_raft.current_leader().await == Some(self.node_id) {
                    elected = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            if elected {
                info!(node_id = self.node_id, "MQ raft node is leader");
            } else {
                warn!(
                    node_id = self.node_id,
                    "MQ raft node is NOT leader after 2s"
                );
            }
        }

        // Write batcher
        let mq_batcher = Arc::new(bisque_mq::MqWriteBatcher::new(
            bisque_mq::MqWriteBatcherConfig::default(),
            mq_raft,
            group_id,
        ));

        // Router
        let mq_router = Arc::new(
            BisqueMqRouter::new(
                mq_shared_metadata,
                self.token_manager.clone(),
                self.meta_engine.clone(),
            )
            .with_batcher(group_id, mq_batcher),
        );

        // TCP server
        let mq_tcp_addr = if let Some(mq_addr) = self.mq_addr {
            let mq_listener = {
                let domain = if mq_addr.is_ipv6() {
                    socket2::Domain::IPV6
                } else {
                    socket2::Domain::IPV4
                };
                let socket = socket2::Socket::new(
                    domain,
                    socket2::Type::STREAM,
                    Some(socket2::Protocol::TCP),
                )?;
                socket.set_reuse_address(true)?;
                socket.set_tcp_nodelay(true)?;
                socket.set_nonblocking(true)?;
                socket.bind(&mq_addr.into())?;
                socket.listen(1024)?;
                TcpListener::from_std(std::net::TcpListener::from(socket))?
            };
            let addr = mq_listener.local_addr()?;
            tokio::spawn(serve_mq_tcp(mq_listener, mq_router.clone()));
            Some(addr)
        } else {
            None
        };

        info!(group_id, ?mq_tcp_addr, "MQ engine provisioned");

        *guard = Some(MqInner {
            raft_node: mq_raft_node,
            manager: mq_manager,
            router: mq_router,
            tcp_addr: mq_tcp_addr,
        });

        Ok(())
    }

    /// Find an existing MQ raft group ID by scanning catalogs.
    /// Returns `None` if no MQ catalog exists yet.
    pub fn find_mq_group_id(&self) -> Option<u64> {
        self.meta_engine
            .list_all_catalogs()
            .into_iter()
            .find(|c| c.engine == EngineType::Mq)
            .map(|c| c.raft_group_id)
    }

    /// Get the MQ TCP listen address (if provisioned and mq_addr was set).
    pub async fn tcp_addr(&self) -> Option<SocketAddr> {
        self.inner.lock().await.as_ref().and_then(|i| i.tcp_addr)
    }

    /// Shut down the MQ engine if provisioned.
    pub async fn shutdown(&self) {
        let guard = self.inner.lock().await;
        if let Some(ref inner) = *guard {
            inner.raft_node.shutdown().await;
            inner.manager.storage().stop();
        }
    }
}

/// MQ router implementation that bridges the bisque auth + engine layer
/// to the transport-agnostic consumer handler.
///
/// Hot-path methods use lock-free `DashMap` and atomics via `MqMetadata`:
/// - Entity name→id lookups go through a `DashMap` cache (populated on miss)
/// - Topic heads use `AtomicU64` for lock-free reads
/// - Topic watchers use `DashMap` instead of `RwLock<HashMap>`
/// - Batcher lookup uses a direct `Arc` (single-group) with `DashMap` fallback
pub struct BisqueMqRouter {
    /// Lock-free MQ metadata — all lookups are DashMap/atomic reads.
    metadata: Arc<MqMetadata>,
    /// Primary batcher (single-group fast path).
    primary_batcher: Option<(u64, Arc<MqWriteBatcher>)>,
    /// Partition group batchers: partition_group_id → batcher.
    /// Populated when partitioned topics are provisioned.
    partition_batchers: DashMap<u64, Arc<MqWriteBatcher>>,
    /// Token manager for handshake validation.
    token_manager: Arc<TokenManager>,
    /// Lock-free entity name→id cache: (entity_type, name_hash) → entity_id.
    /// Populated on first lookup, invalidated entries return None and re-query.
    entity_cache: DashMap<(u8, u64), u64>,
    /// Lock-free topic head cache: topic_id → head_index (AtomicU64).
    /// Updated by notify watchers; falls back to engine on cache miss.
    topic_head_cache: DashMap<u64, AtomicU64>,
    /// Topic publish watchers: (group_id, entity_id) → watch sender.
    /// DashMap eliminates the exclusive write lock on every new subscription.
    topic_watchers: DashMap<(u64, u64), tokio::sync::watch::Sender<u64>>,
}

impl BisqueMqRouter {
    pub fn new(
        metadata: Arc<MqMetadata>,
        token_manager: Arc<TokenManager>,
        _meta_engine: Arc<MetaEngine>,
    ) -> Self {
        Self {
            metadata,
            primary_batcher: None,
            partition_batchers: DashMap::new(),
            token_manager,
            entity_cache: DashMap::new(),
            topic_head_cache: DashMap::new(),
            topic_watchers: DashMap::new(),
        }
    }

    pub fn with_batcher(mut self, group_id: u64, batcher: Arc<MqWriteBatcher>) -> Self {
        self.primary_batcher = Some((group_id, batcher));
        self
    }

    /// Register a batcher for a partition group.
    pub fn add_partition_batcher(&self, partition_group_id: u64, batcher: Arc<MqWriteBatcher>) {
        self.partition_batchers.insert(partition_group_id, batcher);
    }
}

impl MqRouter for BisqueMqRouter {
    fn resolve_entity(&self, _group_id: u64, entity_type: u8, name_hash: u64) -> Option<u64> {
        // Fast path: check DashMap cache (lock-free per-shard read).
        let key = (entity_type, name_hash);
        if let Some(entry) = self.entity_cache.get(&key) {
            return Some(*entry);
        }
        // Slow path: lock-free DashMap lookup on metadata, populate cache.
        let entity_id = self.metadata.resolve_entity(entity_type, name_hash)?;
        self.entity_cache.insert(key, entity_id);
        Some(entity_id)
    }

    fn get_batcher(&self, group_id: u64) -> Option<Arc<MqWriteBatcher>> {
        // Fast path: single-group direct Arc clone (no map lookup).
        if let Some((gid, ref batcher)) = self.primary_batcher {
            if gid == group_id {
                return Some(Arc::clone(batcher));
            }
        }
        None
    }

    fn validate_token(
        &self,
        token: &[u8],
        _consumer_id: Option<u64>,
    ) -> Result<(u64, Vec<u8>), String> {
        let token_str =
            std::str::from_utf8(token).map_err(|_| "invalid UTF-8 in token".to_string())?;
        let claims = self
            .token_manager
            .verify(token_str)
            .ok_or_else(|| "token verification failed".to_string())?;

        let consumer_id = claims.key_id;
        Ok((consumer_id, token.to_vec()))
    }

    fn get_topic_head(&self, _group_id: u64, entity_id: u64) -> u64 {
        // Fast path: atomic read from cache (no lock).
        if let Some(entry) = self.topic_head_cache.get(&entity_id) {
            return entry.load(Ordering::Relaxed);
        }
        // Slow path: lock-free DashMap read from metadata, populate cache.
        let head = self.metadata.get_topic_head_from_state(entity_id);
        self.topic_head_cache
            .insert(entity_id, AtomicU64::new(head));
        head
    }

    fn subscribe_topic_notify(
        &self,
        group_id: u64,
        entity_id: u64,
    ) -> tokio::sync::watch::Receiver<u64> {
        let key = (group_id, entity_id);
        // Fast path: existing watcher — DashMap per-shard read lock only.
        if let Some(entry) = self.topic_watchers.get(&key) {
            return entry.subscribe();
        }
        // Slow path: create new watcher — DashMap per-shard write lock (not global).
        // Use entry API to avoid TOCTOU race between get and insert.
        let entry = self.topic_watchers.entry(key).or_insert_with(|| {
            let head = self.metadata.get_topic_head_from_state(entity_id);
            let (tx, _) = tokio::sync::watch::channel(head);
            tx
        });
        entry.subscribe()
    }

    fn get_prefetcher(&self, _group_id: u64) -> Option<bisque_raft::SegmentPrefetcher> {
        None // TODO: wire up raft segment prefetcher for message payload access
    }

    fn get_topic_partitions(
        &self,
        _group_id: u64,
        entity_id: u64,
    ) -> Option<Vec<bisque_mq::types::PartitionInfo>> {
        self.metadata.get_topic_partitions(entity_id)
    }

    fn get_partition_batcher(&self, partition_group_id: u64) -> Option<Arc<MqWriteBatcher>> {
        self.partition_batchers
            .get(&partition_group_id)
            .map(|entry| Arc::clone(entry.value()))
    }
}

/// Start the MQ consumer TCP server.
pub async fn serve_mq_tcp(listener: TcpListener, router: Arc<BisqueMqRouter>) {
    let addr = listener.local_addr().unwrap();
    info!(%addr, "MQ consumer TCP server listening");

    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!(error = %e, "MQ TCP accept error");
                continue;
            }
        };

        let router = Arc::clone(&router);
        tokio::spawn(async move {
            debug!(%peer_addr, "MQ consumer connected");
            if let Err(e) = handle_mq_connection(stream, router).await {
                debug!(%peer_addr, error = %e, "MQ consumer disconnected");
            }
        });
    }
}

async fn handle_mq_connection(
    stream: tokio::net::TcpStream,
    router: Arc<BisqueMqRouter>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    stream.set_nodelay(true)?;
    let (reader, writer) = tokio::io::split(stream);

    let (client_tx, client_rx) = mpsc::channel::<ClientFrame>(64);
    let (server_tx, mut server_rx) = mpsc::channel::<ServerFrame>(64);

    let reader_handle = tokio::spawn(async move {
        let mut reader = reader;
        let mut buf = BytesMut::with_capacity(4096);
        loop {
            match read_tcp_frame_bytes(&mut reader, &mut buf).await {
                Ok(bytes) => match decode_client_frame_bytes(bytes) {
                    Ok(frame) => {
                        if client_tx.send(frame).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "MQ protocol decode error");
                        return;
                    }
                },
                Err(bisque_mq_protocol::error::ProtocolError::ConnectionClosed) => return,
                Err(e) => {
                    warn!(error = %e, "MQ TCP read error");
                    return;
                }
            }
        }
    });

    let writer_handle = tokio::spawn(async move {
        let mut writer = writer;
        // Reusable buffer: accumulates one or more length-prefixed frames,
        // then flushes in a single write_all syscall.
        let mut write_buf = Vec::with_capacity(8192);
        let mut frame_buf = Vec::with_capacity(4096);
        while let Some(frame) = server_rx.recv().await {
            write_buf.clear();
            // Encode the first frame.
            if let Err(e) = encode_server_frame_prefixed(&frame, &mut frame_buf) {
                warn!(error = %e, "MQ protocol encode error");
                return;
            }
            write_buf.extend_from_slice(&frame_buf);

            // Drain any additional frames already queued in the channel.
            while let Ok(extra) = server_rx.try_recv() {
                if let Err(e) = encode_server_frame_prefixed(&extra, &mut frame_buf) {
                    warn!(error = %e, "MQ protocol encode error");
                    return;
                }
                write_buf.extend_from_slice(&frame_buf);
            }

            // Single syscall for all coalesced frames.
            if let Err(e) = writer.write_all(&write_buf).await {
                debug!(error = %e, "MQ TCP write error");
                return;
            }
        }
    });

    let config = ConsumerHandlerConfig::default();
    let handler = ConsumerHandler::new(router, config, client_rx, server_tx);
    let result = handler.run().await;

    reader_handle.abort();
    writer_handle.abort();

    result.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
}

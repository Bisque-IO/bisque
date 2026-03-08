//! Peer connection management for the cluster mesh.
//!
//! Each outbound peer connection is managed by a dedicated tokio task that
//! handles handshake, snapshot exchange, steady-state messaging, and
//! automatic reconnection with jittered exponential backoff.
//!
//! # Reliability features
//!
//! - **TCP keepalive** via socket2 (30s idle, 10s interval, 3 retries)
//! - **Per-frame read/write timeouts** (configurable, default 30s)
//! - **Connection TTL** — transparent refresh after configurable lifetime (default 10min)
//! - **Sequence gap detection** — auto-requests full snapshot on missed messages
//! - **Jittered exponential backoff** — prevents thundering herd on cluster recovery
//! - **Frame size limits** — rejects frames > 16 MiB to prevent DoS/OOM
//! - **Broadcast lag recovery** — requests snapshot from peer when local broadcast drops messages
//! - **Graceful close** — sends Close message before disconnecting
//! - **Inbound dedup** — superseded connections receive cancel signal via watch channel

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bisque_raft::codec::{Decode, Encode};
use bisque_raft::transport_tcp::write_frame;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, watch};
use tracing::{debug, info, warn};

use super::MeshConfig;
use super::protocol::MeshMessage;
use super::state::ClusterState;
use super::tls::MeshStream;

/// Maximum allowed frame size for mesh protocol (16 MiB).
const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Current mesh protocol version. Peers must match exactly.
const PROTOCOL_VERSION: u8 = 1;

/// Minimum interval between honoring snapshot requests from a single peer (rate limit).
const SNAPSHOT_REQUEST_COOLDOWN: Duration = Duration::from_secs(5);

/// Error type for mesh connections.
#[derive(Debug, thiserror::Error)]
pub enum MeshConnectionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Codec error: {0}")]
    Codec(#[from] bisque_raft::codec::CodecError),
    #[error("Transport error: {0}")]
    Transport(#[from] bisque_raft::transport_tcp::BisqueTransportError),
    #[error("Unexpected message: expected {expected}, got {got}")]
    UnexpectedMessage {
        expected: &'static str,
        got: &'static str,
    },
    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),
    #[error("Connection timed out")]
    Timeout,
    #[error("Frame too large: {0} bytes (max {MAX_FRAME_SIZE})")]
    FrameTooLarge(usize),
    #[error("Peer sent graceful close")]
    PeerClosed,
    #[error("Connection superseded")]
    Superseded,
}

// ---------------------------------------------------------------------------
// TCP keepalive configuration
// ---------------------------------------------------------------------------

/// Configure TCP keepalive on a stream using socket2.
pub fn configure_keepalive_tcp(stream: &TcpStream) -> Result<(), std::io::Error> {
    use socket2::SockRef;

    let sock_ref = SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(30))
        .with_interval(Duration::from_secs(10))
        .with_retries(3);
    sock_ref.set_tcp_keepalive(&keepalive)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Frame reading with size validation
// ---------------------------------------------------------------------------

/// Read a frame with size validation to prevent DoS/OOM.
///
/// Reads a 4-byte LE length prefix, validates against `MAX_FRAME_SIZE`,
/// then reads the payload.
async fn read_frame_validated<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Vec<u8>, MeshConnectionError> {
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(MeshConnectionError::Io)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > MAX_FRAME_SIZE {
        return Err(MeshConnectionError::FrameTooLarge(len));
    }
    if len == 0 {
        return Ok(Vec::new());
    }

    let mut payload = vec![0u8; len];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(MeshConnectionError::Io)?;
    Ok(payload)
}

// ---------------------------------------------------------------------------
// Outbound peer connection
// ---------------------------------------------------------------------------

/// Manages a single outbound peer connection with auto-reconnection.
///
/// This function runs forever, reconnecting on failure with jittered exponential
/// backoff. It exits when `shutdown_rx` signals (graceful shutdown).
pub async fn peer_connection_loop(
    local_node_id: u64,
    peer_node_id: u64,
    peer_addr: SocketAddr,
    config: MeshConfig,
    state: Arc<ClusterState>,
    mut local_change_rx: broadcast::Receiver<MeshMessage>,
    snapshot_fn: Arc<dyn Fn() -> MeshMessage + Send + Sync>,
    mut shutdown_rx: watch::Receiver<()>,
    tls_client_config: Option<Arc<rustls::ClientConfig>>,
) {
    let mut backoff = config.reconnect_base;

    loop {
        debug!(
            peer = peer_node_id,
            addr = %peer_addr,
            "Attempting mesh connection"
        );

        match connect_and_run(
            local_node_id,
            peer_node_id,
            peer_addr,
            &config,
            &state,
            &mut local_change_rx,
            &snapshot_fn,
            &mut shutdown_rx,
            tls_client_config.as_ref(),
        )
        .await
        {
            Ok(()) => {
                // Clean disconnect (TTL refresh or peer close), reset backoff
                backoff = config.reconnect_base;
                info!(
                    peer = peer_node_id,
                    "Mesh peer disconnected cleanly, reconnecting"
                );
            }
            Err(MeshConnectionError::Superseded) => {
                info!(peer = peer_node_id, "Mesh connection shutdown requested");
                return;
            }
            Err(e) => {
                warn!(
                    peer = peer_node_id,
                    addr = %peer_addr,
                    "Mesh connection error: {e}, reconnecting in {backoff:?}"
                );
                state.mark_unreachable(peer_node_id);
            }
        }

        // Jittered backoff to prevent thundering herd
        let jitter = rand::random_range(0.75..1.25);
        let jittered = Duration::from_secs_f64(backoff.as_secs_f64() * jitter);

        tokio::select! {
            _ = tokio::time::sleep(jittered) => {}
            _ = shutdown_rx.changed() => {
                info!(peer = peer_node_id, "Mesh connection shutdown during backoff");
                return;
            }
        }

        backoff = (backoff * 2).min(config.reconnect_max);
    }
}

/// Connect to a peer, exchange handshakes and snapshots, then enter
/// the steady-state read/write loop.
async fn connect_and_run(
    local_node_id: u64,
    peer_node_id: u64,
    peer_addr: SocketAddr,
    config: &MeshConfig,
    state: &Arc<ClusterState>,
    local_change_rx: &mut broadcast::Receiver<MeshMessage>,
    snapshot_fn: &Arc<dyn Fn() -> MeshMessage + Send + Sync>,
    shutdown_rx: &mut watch::Receiver<()>,
    tls_client_config: Option<&Arc<rustls::ClientConfig>>,
) -> Result<(), MeshConnectionError> {
    // 1. TCP connect with timeout
    let tcp_stream = tokio::time::timeout(config.connect_timeout, TcpStream::connect(peer_addr))
        .await
        .map_err(|_| MeshConnectionError::Timeout)?
        .map_err(MeshConnectionError::Io)?;

    tcp_stream.set_nodelay(true)?;
    configure_keepalive_tcp(&tcp_stream)?;

    // 2. TLS handshake (if configured) or wrap as plain
    let stream = if let Some(tls_config) = tls_client_config {
        let connector = tokio_rustls::TlsConnector::from(tls_config.clone());
        let server_name = super::tls::mesh_server_name(peer_node_id);
        let tls_stream = connector
            .connect(server_name, tcp_stream)
            .await
            .map_err(MeshConnectionError::Io)?;
        MeshStream::ClientTls(tls_stream)
    } else {
        MeshStream::Plain(tcp_stream)
    };

    let (mut reader, mut writer) = tokio::io::split(stream);

    // 2–5. Handshake + snapshot exchange with timeout
    tokio::time::timeout(config.handshake_timeout, async {
        // 2. Send handshake
        let handshake = MeshMessage::Handshake {
            node_id: local_node_id,
            mesh_port: config.listen_port,
            protocol_version: PROTOCOL_VERSION,
        };
        let encoded = handshake
            .encode_to_vec()
            .map_err(MeshConnectionError::Codec)?;
        write_frame(&mut writer, &encoded).await?;

        // 3. Read peer handshake
        let peer_frame = read_frame_validated(&mut reader).await?;
        let peer_msg = MeshMessage::decode_from_slice(&peer_frame)?;
        match &peer_msg {
            MeshMessage::Handshake {
                node_id,
                protocol_version,
                ..
            } => {
                if *node_id != peer_node_id {
                    return Err(MeshConnectionError::HandshakeFailed(format!(
                        "expected node {peer_node_id}, got {node_id}"
                    )));
                }
                if *protocol_version != PROTOCOL_VERSION {
                    return Err(MeshConnectionError::HandshakeFailed(format!(
                        "incompatible protocol version: local={PROTOCOL_VERSION}, remote={protocol_version}"
                    )));
                }
            }
            _ => {
                return Err(MeshConnectionError::UnexpectedMessage {
                    expected: "Handshake",
                    got: msg_name(&peer_msg),
                });
            }
        }

        // 4. Send our local state snapshot
        let snapshot = (snapshot_fn)();
        let encoded = snapshot
            .encode_to_vec()
            .map_err(MeshConnectionError::Codec)?;
        write_frame(&mut writer, &encoded).await?;

        // 5. Read peer's state snapshot
        let peer_snap_frame = read_frame_validated(&mut reader).await?;
        let peer_snap = MeshMessage::decode_from_slice(&peer_snap_frame)?;
        apply_message(state, &peer_snap);

        Ok(())
    })
    .await
    .map_err(|_| MeshConnectionError::Timeout)??;

    info!(peer = peer_node_id, "Mesh peer connected and synced");

    // 6. Steady-state: concurrent read and write with TTL
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Vec<u8>>(64);

    // Writer task: forward local changes to the peer
    let writer_handle = {
        let outbound_tx = outbound_tx.clone();
        let mut rx = local_change_rx.resubscribe();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(msg) => match msg.encode_to_vec() {
                        Ok(encoded) => {
                            // Non-blocking send: drop message if channel full rather
                            // than blocking the broadcast receiver (head-of-line blocking).
                            match outbound_tx.try_send(encoded) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    warn!("Outbound channel full, dropping message to peer");
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => break,
                            }
                        }
                        Err(e) => {
                            warn!("Failed to encode mesh message: {e}");
                        }
                    },
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Broadcast lagged by {n} messages, requesting snapshot from peer");
                        let req = MeshMessage::RequestSnapshot;
                        if let Ok(encoded) = req.encode_to_vec() {
                            match outbound_tx.try_send(encoded) {
                                Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                                Err(mpsc::error::TrySendError::Closed(_)) => break,
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    };

    // Connection TTL deadline
    let ttl_deadline = tokio::time::sleep(config.connection_ttl);
    tokio::pin!(ttl_deadline);

    // Rate limit: track last time we honored a RequestSnapshot
    let mut last_snapshot_served = tokio::time::Instant::now() - SNAPSHOT_REQUEST_COOLDOWN;

    let result: Result<(), MeshConnectionError> = async {
        loop {
            tokio::select! {
                // Read inbound message from peer (with timeout)
                frame_result = tokio::time::timeout(
                    config.frame_timeout,
                    read_frame_validated(&mut reader),
                ) => {
                    let frame = frame_result
                        .map_err(|_| MeshConnectionError::Timeout)??;
                    let msg = MeshMessage::decode_from_slice(&frame)?;

                    if matches!(msg, MeshMessage::Close) {
                        info!(peer = peer_node_id, "Peer sent graceful close");
                        break Ok(());
                    } else if matches!(msg, MeshMessage::RequestSnapshot) {
                        let now = tokio::time::Instant::now();
                        if now.duration_since(last_snapshot_served) >= SNAPSHOT_REQUEST_COOLDOWN {
                            last_snapshot_served = now;
                            let snap = (snapshot_fn)();
                            let encoded = snap.encode_to_vec().map_err(MeshConnectionError::Codec)?;
                            if outbound_tx.send(encoded).await.is_err() {
                                break Ok(());
                            }
                        } else {
                            debug!(peer = peer_node_id, "Rate-limited snapshot request from peer");
                        }
                    } else {
                        let gap = apply_message(state, &msg);
                        if gap {
                            warn!(peer = peer_node_id, "Sequence gap detected, requesting snapshot");
                            let req = MeshMessage::RequestSnapshot;
                            let encoded = req.encode_to_vec().map_err(MeshConnectionError::Codec)?;
                            if outbound_tx.send(encoded).await.is_err() {
                                break Ok(());
                            }
                        }
                    }
                }

                // Write outbound message to peer (with timeout)
                Some(encoded) = outbound_rx.recv() => {
                    tokio::time::timeout(
                        config.frame_timeout,
                        write_frame(&mut writer, &encoded),
                    )
                    .await
                    .map_err(|_| MeshConnectionError::Timeout)??;
                }

                // Connection TTL expired — refresh
                _ = &mut ttl_deadline => {
                    info!(peer = peer_node_id, "Connection TTL expired, refreshing");
                    let close = MeshMessage::Close;
                    if let Ok(encoded) = close.encode_to_vec() {
                        let _ = tokio::time::timeout(
                            Duration::from_secs(2),
                            write_frame(&mut writer, &encoded),
                        ).await;
                    }
                    break Ok(());
                }

                // Shutdown signal
                _ = shutdown_rx.changed() => {
                    let close = MeshMessage::Close;
                    if let Ok(encoded) = close.encode_to_vec() {
                        let _ = tokio::time::timeout(
                            Duration::from_secs(2),
                            write_frame(&mut writer, &encoded),
                        ).await;
                    }
                    return Err(MeshConnectionError::Superseded);
                }
            }
        }
    }
    .await;

    writer_handle.abort();
    result
}

// ---------------------------------------------------------------------------
// Inbound peer connection (split into handshake + steady-state)
// ---------------------------------------------------------------------------

/// Perform the inbound handshake: read peer handshake, send ours,
/// exchange snapshots. Returns the peer's node_id.
///
/// The stream is borrowed mutably so the caller retains ownership
/// for the steady-state loop. TCP nodelay and keepalive are configured
/// by the caller before TLS wrapping.
pub async fn inbound_handshake(
    local_node_id: u64,
    stream: &mut MeshStream,
    config: &MeshConfig,
    state: &Arc<ClusterState>,
    snapshot_fn: &Arc<dyn Fn() -> MeshMessage + Send + Sync>,
) -> Result<u64, MeshConnectionError> {
    // Handshake is sequential (read, write, read, write), so we use
    // the stream directly without splitting.

    // 1. Read peer's handshake
    let peer_frame = read_frame_validated(stream).await?;
    let peer_msg = MeshMessage::decode_from_slice(&peer_frame)?;
    let peer_node_id = match &peer_msg {
        MeshMessage::Handshake {
            node_id,
            protocol_version,
            ..
        } => {
            if *protocol_version != PROTOCOL_VERSION {
                return Err(MeshConnectionError::HandshakeFailed(format!(
                    "incompatible protocol version: local={PROTOCOL_VERSION}, remote={protocol_version}"
                )));
            }
            *node_id
        }
        _ => {
            return Err(MeshConnectionError::UnexpectedMessage {
                expected: "Handshake",
                got: msg_name(&peer_msg),
            });
        }
    };

    // 2. Send our handshake
    let handshake = MeshMessage::Handshake {
        node_id: local_node_id,
        mesh_port: config.listen_port,
        protocol_version: PROTOCOL_VERSION,
    };
    let encoded = handshake
        .encode_to_vec()
        .map_err(MeshConnectionError::Codec)?;
    write_frame(stream, &encoded).await?;

    // 3. Read peer's state snapshot
    let peer_snap_frame = read_frame_validated(stream).await?;
    let peer_snap = MeshMessage::decode_from_slice(&peer_snap_frame)?;
    apply_message(state, &peer_snap);

    // 4. Send our state snapshot
    let snapshot = (snapshot_fn)();
    let encoded = snapshot
        .encode_to_vec()
        .map_err(MeshConnectionError::Codec)?;
    write_frame(stream, &encoded).await?;

    info!(
        peer = peer_node_id,
        "Inbound mesh peer connected and synced"
    );

    Ok(peer_node_id)
}

/// Run the inbound steady-state loop after handshake.
///
/// The `cancel_rx` watch channel is used for connection deduplication:
/// when a newer connection from the same peer arrives, the old handler's
/// cancel_rx fires.
pub async fn run_inbound_loop(
    peer_node_id: u64,
    stream: MeshStream,
    config: &MeshConfig,
    state: &Arc<ClusterState>,
    local_change_tx: &broadcast::Sender<MeshMessage>,
    snapshot_fn: &Arc<dyn Fn() -> MeshMessage + Send + Sync>,
    mut cancel_rx: watch::Receiver<()>,
) -> Result<(), MeshConnectionError> {
    let (mut reader, mut writer) = tokio::io::split(stream);

    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Vec<u8>>(64);

    // Writer task: forward local changes to the peer
    let writer_handle = {
        let outbound_tx = outbound_tx.clone();
        let mut rx = local_change_tx.subscribe();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(msg) => match msg.encode_to_vec() {
                        Ok(encoded) => match outbound_tx.try_send(encoded) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                warn!("Outbound channel full, dropping message to peer");
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => break,
                        },
                        Err(e) => {
                            warn!("Failed to encode mesh message: {e}");
                        }
                    },
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Broadcast lagged by {n} messages, requesting snapshot from peer");
                        let req = MeshMessage::RequestSnapshot;
                        if let Ok(encoded) = req.encode_to_vec() {
                            match outbound_tx.try_send(encoded) {
                                Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                                Err(mpsc::error::TrySendError::Closed(_)) => break,
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    };

    // Connection TTL deadline
    let ttl_deadline = tokio::time::sleep(config.connection_ttl);
    tokio::pin!(ttl_deadline);

    // Rate limit: track last time we honored a RequestSnapshot
    let mut last_snapshot_served = tokio::time::Instant::now() - SNAPSHOT_REQUEST_COOLDOWN;

    let result: Result<(), MeshConnectionError> = async {
        loop {
            tokio::select! {
                // Read inbound message from peer (with timeout)
                frame_result = tokio::time::timeout(
                    config.frame_timeout,
                    read_frame_validated(&mut reader),
                ) => {
                    let frame = frame_result
                        .map_err(|_| MeshConnectionError::Timeout)??;
                    let msg = MeshMessage::decode_from_slice(&frame)?;

                    if matches!(msg, MeshMessage::Close) {
                        info!(peer = peer_node_id, "Peer sent graceful close");
                        break Ok(());
                    } else if matches!(msg, MeshMessage::RequestSnapshot) {
                        let now = tokio::time::Instant::now();
                        if now.duration_since(last_snapshot_served) >= SNAPSHOT_REQUEST_COOLDOWN {
                            last_snapshot_served = now;
                            let snap = (snapshot_fn)();
                            let encoded = snap.encode_to_vec().map_err(MeshConnectionError::Codec)?;
                            if outbound_tx.send(encoded).await.is_err() {
                                break Ok(());
                            }
                        } else {
                            debug!(peer = peer_node_id, "Rate-limited snapshot request from peer");
                        }
                    } else {
                        let gap = apply_message(state, &msg);
                        if gap {
                            warn!(peer = peer_node_id, "Sequence gap detected, requesting snapshot");
                            let req = MeshMessage::RequestSnapshot;
                            let encoded = req.encode_to_vec().map_err(MeshConnectionError::Codec)?;
                            if outbound_tx.send(encoded).await.is_err() {
                                break Ok(());
                            }
                        }
                    }
                }

                // Write outbound message to peer (with timeout)
                Some(encoded) = outbound_rx.recv() => {
                    tokio::time::timeout(
                        config.frame_timeout,
                        write_frame(&mut writer, &encoded),
                    )
                    .await
                    .map_err(|_| MeshConnectionError::Timeout)??;
                }

                // Connection TTL expired
                _ = &mut ttl_deadline => {
                    info!(peer = peer_node_id, "Inbound connection TTL expired");
                    let close = MeshMessage::Close;
                    if let Ok(encoded) = close.encode_to_vec() {
                        let _ = tokio::time::timeout(
                            Duration::from_secs(2),
                            write_frame(&mut writer, &encoded),
                        ).await;
                    }
                    break Ok(());
                }

                // Cancel signal (superseded by newer connection from same peer)
                _ = cancel_rx.changed() => {
                    info!(peer = peer_node_id, "Inbound connection superseded by new connection");
                    let close = MeshMessage::Close;
                    if let Ok(encoded) = close.encode_to_vec() {
                        let _ = tokio::time::timeout(
                            Duration::from_secs(2),
                            write_frame(&mut writer, &encoded),
                        ).await;
                    }
                    break Ok(());
                }
            }
        }
    }
    .await;

    writer_handle.abort();
    result
}

// ---------------------------------------------------------------------------
// Message application
// ---------------------------------------------------------------------------

/// Apply an inbound message to the cluster state.
///
/// Returns `true` if a sequence gap was detected (caller should request snapshot).
fn apply_message(state: &ClusterState, msg: &MeshMessage) -> bool {
    match msg {
        MeshMessage::StateSnapshot {
            node_id,
            operations,
            health,
            seq,
        } => {
            state.apply_snapshot(*node_id, operations.clone(), health.clone(), *seq);
            false // snapshot resets state, no gap possible
        }
        MeshMessage::OperationUpdate {
            node_id,
            operation,
            seq,
        } => state.apply_operation_update(*node_id, operation.clone(), *seq),
        MeshMessage::Heartbeat {
            node_id,
            health,
            seq,
        } => state.apply_heartbeat(*node_id, health.clone(), *seq),
        MeshMessage::Handshake { .. } | MeshMessage::RequestSnapshot | MeshMessage::Close => false,
    }
}

fn msg_name(msg: &MeshMessage) -> &'static str {
    match msg {
        MeshMessage::Handshake { .. } => "Handshake",
        MeshMessage::StateSnapshot { .. } => "StateSnapshot",
        MeshMessage::OperationUpdate { .. } => "OperationUpdate",
        MeshMessage::Heartbeat { .. } => "Heartbeat",
        MeshMessage::RequestSnapshot => "RequestSnapshot",
        MeshMessage::Close => "Close",
    }
}

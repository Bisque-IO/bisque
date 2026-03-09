//! MQTT TCP server — accepts connections and spawns session handlers.
//!
//! The `MqttServer` binds to a TCP port, accepts incoming connections, and
//! spawns a Tokio task per connection that runs the MQTT codec + session loop.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bisque_mq::MqWriteBatcher;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::codec::{self, CodecError};
use crate::session::{MqttSession, MqttSessionConfig};
use crate::types::MqttPacket;

// =============================================================================
// Server Configuration
// =============================================================================

/// Configuration for the MQTT server.
#[derive(Debug, Clone)]
pub struct MqttServerConfig {
    /// Address to bind the TCP listener.
    pub bind_addr: SocketAddr,
    /// Session configuration applied to all new connections.
    pub session_config: MqttSessionConfig,
    /// TCP read buffer size in bytes.
    pub read_buffer_size: usize,
    /// Maximum time to wait for CONNECT after TCP accept.
    pub connect_timeout: Duration,
    /// TCP keepalive interval.
    pub tcp_keepalive: Option<Duration>,
}

impl Default for MqttServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:1883".parse().unwrap(),
            session_config: MqttSessionConfig::default(),
            read_buffer_size: 8192,
            connect_timeout: Duration::from_secs(10),
            tcp_keepalive: Some(Duration::from_secs(60)),
        }
    }
}

// =============================================================================
// Server Statistics
// =============================================================================

/// Atomic server-level statistics.
pub struct MqttServerStats {
    active_connections: std::sync::atomic::AtomicU64,
    total_connections: std::sync::atomic::AtomicU64,
    total_packets_received: std::sync::atomic::AtomicU64,
    total_packets_sent: std::sync::atomic::AtomicU64,
}

impl MqttServerStats {
    fn new() -> Self {
        Self {
            active_connections: std::sync::atomic::AtomicU64::new(0),
            total_connections: std::sync::atomic::AtomicU64::new(0),
            total_packets_received: std::sync::atomic::AtomicU64::new(0),
            total_packets_sent: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Current number of active connections.
    pub fn active_connections(&self) -> u64 {
        self.active_connections
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Total connections accepted since server start.
    pub fn total_connections(&self) -> u64 {
        self.total_connections
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Total MQTT packets received across all connections.
    pub fn total_packets_received(&self) -> u64 {
        self.total_packets_received
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Total MQTT packets sent across all connections.
    pub fn total_packets_sent(&self) -> u64 {
        self.total_packets_sent
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

// =============================================================================
// MqttServer
// =============================================================================

/// MQTT TCP server that accepts connections and translates MQTT to bisque-mq.
pub struct MqttServer {
    config: MqttServerConfig,
    batcher: Arc<MqWriteBatcher>,
    stats: Arc<MqttServerStats>,
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
}

impl MqttServer {
    /// Create a new MQTT server.
    pub fn new(config: MqttServerConfig, batcher: Arc<MqWriteBatcher>) -> Self {
        Self {
            config,
            batcher,
            stats: Arc::new(MqttServerStats::new()),
            shutdown_tx: None,
        }
    }

    /// Get a reference to the server statistics.
    pub fn stats(&self) -> Arc<MqttServerStats> {
        Arc::clone(&self.stats)
    }

    /// Start the MQTT server. Returns a JoinHandle for the accept loop.
    ///
    /// The server runs until the returned handle is dropped or `shutdown()` is called.
    pub async fn start(
        &mut self,
    ) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        info!(addr = %self.config.bind_addr, "MQTT server listening");

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        let config = self.config.clone();
        let batcher = Arc::clone(&self.batcher);
        let stats = Arc::clone(&self.stats);

        let handle = tokio::spawn(accept_loop(listener, config, batcher, stats, shutdown_rx));

        Ok(handle)
    }

    /// Signal the server to shut down gracefully.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
    }
}

// =============================================================================
// Accept Loop
// =============================================================================

async fn accept_loop(
    listener: TcpListener,
    config: MqttServerConfig,
    batcher: Arc<MqWriteBatcher>,
    stats: Arc<MqttServerStats>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        stats.total_connections.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        stats.active_connections.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                        debug!(peer = %peer_addr, "new MQTT connection");

                        let session_config = config.session_config.clone();
                        let batcher = Arc::clone(&batcher);
                        let stats = Arc::clone(&stats);
                        let read_buffer_size = config.read_buffer_size;
                        let connect_timeout = config.connect_timeout;

                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(
                                stream,
                                peer_addr,
                                session_config,
                                batcher,
                                Arc::clone(&stats),
                                read_buffer_size,
                                connect_timeout,
                            ).await {
                                debug!(peer = %peer_addr, error = %e, "connection closed with error");
                            }
                            stats.active_connections.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "failed to accept connection");
                        // Brief pause to avoid tight loop on persistent errors.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("MQTT server shutting down");
                    break;
                }
            }
        }
    }
}

// =============================================================================
// Connection Error
// =============================================================================

/// Errors that can occur during connection handling.
#[derive(Debug, thiserror::Error)]
enum ConnectionError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),
    #[error("connect timeout")]
    ConnectTimeout,
    #[error("first packet was not CONNECT")]
    NotConnect,
    #[error("batcher error: {0}")]
    Batcher(#[from] bisque_mq::MqBatcherError),
    #[error("connection closed by client")]
    Closed,
}

// =============================================================================
// Connection Handler
// =============================================================================

async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    session_config: MqttSessionConfig,
    batcher: Arc<MqWriteBatcher>,
    stats: Arc<MqttServerStats>,
    read_buffer_size: usize,
    connect_timeout: Duration,
) -> Result<(), ConnectionError> {
    let mut read_buf = BytesMut::with_capacity(read_buffer_size);
    let mut session = MqttSession::new(session_config);

    // Wait for CONNECT packet with timeout.
    let first_packet = tokio::time::timeout(connect_timeout, async {
        loop {
            let n = stream.read_buf(&mut read_buf).await?;
            if n == 0 {
                return Err(ConnectionError::Closed);
            }

            match codec::decode_packet(&read_buf) {
                Ok((packet, consumed)) => {
                    let _ = read_buf.split_to(consumed);
                    return Ok(packet);
                }
                Err(CodecError::Incomplete) => {
                    // Need more data.
                    continue;
                }
                Err(e) => return Err(ConnectionError::Codec(e)),
            }
        }
    })
    .await
    .map_err(|_| ConnectionError::ConnectTimeout)??;

    // Validate that the first packet is CONNECT.
    if !matches!(first_packet, MqttPacket::Connect(_)) {
        return Err(ConnectionError::NotConnect);
    }

    // Process CONNECT.
    let (commands, responses) = session.process_packet(&first_packet);

    // Submit commands to bisque-mq.
    for cmd in commands {
        let _ = batcher.submit(cmd).await;
    }

    // Send response packets.
    for response in responses {
        let mut write_buf = BytesMut::new();
        codec::encode_packet(&response, &mut write_buf);
        stream.write_all(&write_buf).await?;
        stats
            .total_packets_sent
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    stats
        .total_packets_received
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    info!(
        peer = %peer_addr,
        client_id = %session.client_id,
        "MQTT client connected"
    );

    // Main packet processing loop.
    let result = connection_loop(&mut stream, &mut session, &batcher, &stats, &mut read_buf).await;

    // Handle disconnect.
    let disconnect_commands = match &result {
        Ok(()) => {
            // Clean disconnect (DISCONNECT packet received).
            // handle_disconnect was already called in process_packet.
            vec![]
        }
        Err(_) => {
            // Unclean disconnect.
            session.handle_unclean_disconnect()
        }
    };

    for cmd in disconnect_commands {
        let _ = batcher.submit(cmd).await;
    }

    info!(
        peer = %peer_addr,
        client_id = %session.client_id,
        clean = result.is_ok(),
        "MQTT client disconnected"
    );

    result
}

async fn connection_loop(
    stream: &mut TcpStream,
    session: &mut MqttSession,
    batcher: &Arc<MqWriteBatcher>,
    stats: &Arc<MqttServerStats>,
    read_buf: &mut BytesMut,
) -> Result<(), ConnectionError> {
    // Keep-alive timeout: 1.5x the negotiated keep-alive per MQTT spec.
    let keep_alive_timeout = if session.keep_alive > 0 {
        Duration::from_secs(session.keep_alive as u64 * 3 / 2)
    } else {
        // No keep-alive: use a generous timeout.
        Duration::from_secs(3600)
    };

    loop {
        // Read with keep-alive timeout.
        let read_result = tokio::time::timeout(keep_alive_timeout, stream.read_buf(read_buf)).await;

        match read_result {
            Ok(Ok(0)) => {
                // Connection closed.
                return Err(ConnectionError::Closed);
            }
            Ok(Ok(_n)) => {
                // Process all complete packets in the buffer.
                loop {
                    match codec::decode_packet(read_buf) {
                        Ok((packet, consumed)) => {
                            let _ = read_buf.split_to(consumed);
                            stats
                                .total_packets_received
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            // Check for DISCONNECT before processing.
                            let is_disconnect = matches!(packet, MqttPacket::Disconnect(_));

                            let (commands, responses) = session.process_packet(&packet);

                            // Submit commands to bisque-mq.
                            for cmd in commands {
                                match batcher.submit(cmd).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        warn!(error = %e, "failed to submit command to batcher");
                                    }
                                }
                            }

                            // Send response packets.
                            for response in responses {
                                let mut write_buf = BytesMut::new();
                                codec::encode_packet(&response, &mut write_buf);
                                stream.write_all(&write_buf).await?;
                                stats
                                    .total_packets_sent
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }

                            if is_disconnect {
                                return Ok(());
                            }
                        }
                        Err(CodecError::Incomplete) => {
                            // Need more data; break inner loop and read again.
                            break;
                        }
                        Err(e) => {
                            return Err(ConnectionError::Codec(e));
                        }
                    }
                }
            }
            Ok(Err(e)) => {
                return Err(ConnectionError::Io(e));
            }
            Err(_) => {
                // Keep-alive timeout expired.
                warn!(
                    client_id = %session.client_id,
                    timeout_secs = keep_alive_timeout.as_secs(),
                    "keep-alive timeout, disconnecting"
                );
                return Err(ConnectionError::Closed);
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config_defaults() {
        let config = MqttServerConfig::default();
        assert_eq!(config.bind_addr.port(), 1883);
        assert_eq!(config.read_buffer_size, 8192);
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
    }

    #[test]
    fn test_server_stats() {
        let stats = MqttServerStats::new();
        assert_eq!(stats.active_connections(), 0);
        assert_eq!(stats.total_connections(), 0);
        assert_eq!(stats.total_packets_received(), 0);
        assert_eq!(stats.total_packets_sent(), 0);

        stats
            .active_connections
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(stats.active_connections(), 1);
    }

    #[tokio::test]
    async fn test_encode_decode_integration() {
        // Verify a full CONNECT -> encode -> decode -> session roundtrip.
        use crate::types::*;

        let connect = MqttPacket::Connect(Connect {
            protocol_name: "MQTT".to_string(),
            protocol_version: ProtocolVersion::V311,
            flags: ConnectFlags {
                username: false,
                password: false,
                will_retain: false,
                will_qos: QoS::AtMostOnce,
                will: false,
                clean_session: true,
            },
            keep_alive: 30,
            client_id: "integration-test".to_string(),
            will: None,
            username: None,
            password: None,
            properties: Properties::default(),
        });

        // Encode
        let mut buf = BytesMut::new();
        codec::encode_packet(&connect, &mut buf);

        // Decode
        let (decoded, consumed) = codec::decode_packet(&buf).unwrap();
        assert_eq!(consumed, buf.len());

        // Feed to session
        let mut session = MqttSession::new(MqttSessionConfig::default());
        let (commands, responses) = session.process_packet(&decoded);

        assert!(session.connected);
        assert_eq!(session.client_id, "integration-test");
        assert_eq!(session.keep_alive, 30);
        assert!(!commands.is_empty());
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], MqttPacket::ConnAck(_)));
    }
}

//! AMQP 1.0 TCP server.
//!
//! `AmqpServer` binds a TCP socket and accepts connections, spawning
//! a task per connection that runs the AMQP 1.0 protocol state machine.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::connection::{AmqpConnection, ConnectionError, ConnectionPhase};
use crate::types::condition;

// =============================================================================
// Server Configuration
// =============================================================================

/// Configuration for the AMQP 1.0 server.
#[derive(Debug, Clone)]
pub struct AmqpServerConfig {
    /// Address to bind to.
    pub bind_addr: SocketAddr,
    /// TCP read buffer size.
    pub read_buf_size: usize,
    /// Maximum number of concurrent connections (0 = unlimited).
    pub max_connections: usize,
}

impl Default for AmqpServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:5672".parse().unwrap(),
            read_buf_size: 8192,
            max_connections: 0,
        }
    }
}

impl AmqpServerConfig {
    pub fn with_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = addr;
        self
    }

    pub fn with_read_buf_size(mut self, size: usize) -> Self {
        self.read_buf_size = size;
        self
    }

    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }
}

// =============================================================================
// AmqpServer
// =============================================================================

/// AMQP 1.0 TCP server.
///
/// Accepts TCP connections and drives the AMQP 1.0 protocol for each one.
pub struct AmqpServer {
    config: AmqpServerConfig,
    /// Active connection count.
    active_connections: Arc<std::sync::atomic::AtomicUsize>,
}

impl AmqpServer {
    /// Create a new AMQP server with the given configuration.
    pub fn new(config: AmqpServerConfig) -> Self {
        Self {
            config,
            active_connections: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Run the server, accepting connections until the shutdown signal fires.
    pub async fn run(&self, shutdown: tokio::sync::watch::Receiver<bool>) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        info!(addr = %self.config.bind_addr, "AMQP 1.0 server listening");

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            if self.config.max_connections > 0 {
                                let current = self.active_connections.load(
                                    std::sync::atomic::Ordering::Relaxed
                                );
                                if current >= self.config.max_connections {
                                    warn!(
                                        peer = %peer_addr,
                                        current,
                                        max = self.config.max_connections,
                                        "rejecting connection: limit reached"
                                    );
                                    drop(stream);
                                    continue;
                                }
                            }

                            let active = self.active_connections.clone();
                            let read_buf_size = self.config.read_buf_size;
                            let mut shutdown_rx = shutdown.clone();

                            active.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            tokio::spawn(async move {
                                debug!(peer = %peer_addr, "accepted AMQP 1.0 connection");
                                match handle_connection(stream, peer_addr, read_buf_size, &mut shutdown_rx).await {
                                    Ok(()) => {
                                        debug!(peer = %peer_addr, "AMQP connection closed normally");
                                    }
                                    Err(e) => {
                                        warn!(peer = %peer_addr, error = %e, "AMQP connection error");
                                    }
                                }
                                active.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "failed to accept TCP connection");
                        }
                    }
                }
                _ = wait_for_shutdown(&shutdown) => {
                    info!("AMQP 1.0 server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Get the current number of active connections.
    pub fn active_connection_count(&self) -> usize {
        self.active_connections
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

// =============================================================================
// Per-Connection Handler
// =============================================================================

/// Handle a single AMQP 1.0 connection.
async fn handle_connection(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    read_buf_size: usize,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), ConnectionError> {
    let mut conn = AmqpConnection::new();
    let mut read_buf = vec![0u8; read_buf_size];

    info!(
        conn_id = conn.id,
        peer = %peer_addr,
        "starting AMQP 1.0 connection handler"
    );

    loop {
        // Flush any pending writes first.
        if conn.has_pending_writes() {
            let data = conn.take_write_buf();
            if let Err(e) = stream.write_all(&data).await {
                warn!(
                    conn_id = conn.id,
                    error = %e,
                    "write error"
                );
                return Ok(());
            }
        }

        if conn.phase == ConnectionPhase::Closed {
            return Ok(());
        }

        // Read more data from the socket.
        tokio::select! {
            read_result = stream.read(&mut read_buf) => {
                match read_result {
                    Ok(0) => {
                        debug!(conn_id = conn.id, "peer closed connection");
                        return Ok(());
                    }
                    Ok(n) => {
                        conn.feed_data(&read_buf[..n]);
                        match conn.process() {
                            Ok(true) => {
                                // Continue processing.
                            }
                            Ok(false) => {
                                // Connection closed gracefully. Flush final writes.
                                if conn.has_pending_writes() {
                                    let data = conn.take_write_buf();
                                    let _ = stream.write_all(&data).await;
                                }
                                return Ok(());
                            }
                            Err(e) => {
                                warn!(
                                    conn_id = conn.id,
                                    error = %e,
                                    "protocol error"
                                );
                                // Try to flush any error frames we generated.
                                if conn.has_pending_writes() {
                                    let data = conn.take_write_buf();
                                    let _ = stream.write_all(&data).await;
                                }
                                return Err(e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            conn_id = conn.id,
                            error = %e,
                            "read error"
                        );
                        return Ok(());
                    }
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!(conn_id = conn.id, "shutting down connection");
                    conn.send_connection_error(
                        condition::CONNECTION_FORCED,
                        "server shutting down",
                    );
                    if conn.has_pending_writes() {
                        let data = conn.take_write_buf();
                        let _ = stream.write_all(&data).await;
                    }
                    return Ok(());
                }
            }
        }
    }
}

/// Wait until the shutdown signal is received.
async fn wait_for_shutdown(shutdown: &tokio::sync::watch::Receiver<bool>) {
    let mut rx = shutdown.clone();
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
        if *rx.borrow() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config_defaults() {
        let config = AmqpServerConfig::default();
        assert_eq!(config.bind_addr, "0.0.0.0:5672".parse().unwrap());
        assert_eq!(config.read_buf_size, 8192);
        assert_eq!(config.max_connections, 0);
    }

    #[test]
    fn test_server_config_builder() {
        let addr: SocketAddr = "127.0.0.1:15672".parse().unwrap();
        let config = AmqpServerConfig::default()
            .with_bind_addr(addr)
            .with_read_buf_size(16384)
            .with_max_connections(100);
        assert_eq!(config.bind_addr, addr);
        assert_eq!(config.read_buf_size, 16384);
        assert_eq!(config.max_connections, 100);
    }

    #[test]
    fn test_server_new() {
        let server = AmqpServer::new(AmqpServerConfig::default());
        assert_eq!(server.active_connection_count(), 0);
    }

    #[tokio::test]
    async fn test_server_accept_and_shutdown() {
        let config = AmqpServerConfig::default().with_bind_addr("127.0.0.1:0".parse().unwrap());

        let listener = TcpListener::bind(config.bind_addr).await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        drop(listener);

        let config = config.with_bind_addr(local_addr);
        let server = Arc::new(AmqpServer::new(config));

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let server_clone = server.clone();
        let handle = tokio::spawn(async move {
            server_clone.run(shutdown_rx).await.unwrap();
        });

        // Give the server a moment to start listening.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Connect a client that sends the AMQP 1.0 protocol header.
        let mut client = TcpStream::connect(local_addr).await.unwrap();
        client.write_all(&crate::types::AMQP_HEADER).await.unwrap();

        // Read response (AMQP header + Open frame).
        let mut buf = [0u8; 4096];
        let n = client.read(&mut buf).await.unwrap();
        assert!(n > 0);

        // Signal shutdown.
        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }
}

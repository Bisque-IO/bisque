use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

use crate::connection::KafkaConnection;
use crate::handler::KafkaHandler;

// =============================================================================
// Server Configuration
// =============================================================================

pub struct KafkaServerConfig {
    pub bind_addr: SocketAddr,
    pub max_connections: usize,
    pub read_buffer_size: usize,
    pub session_expiry_interval: Duration,
}

impl Default for KafkaServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:9092".parse().unwrap(),
            max_connections: 10_000,
            read_buffer_size: 65536,
            session_expiry_interval: Duration::from_secs(10),
        }
    }
}

// =============================================================================
// Server Stats
// =============================================================================

pub struct KafkaServerStats {
    pub active_connections: AtomicU64,
    pub total_connections: AtomicU64,
    pub total_requests: AtomicU64,
}

impl KafkaServerStats {
    fn new() -> Self {
        Self {
            active_connections: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
        }
    }
}

// =============================================================================
// Kafka Server
// =============================================================================

pub struct KafkaServer {
    config: KafkaServerConfig,
    handler: Arc<KafkaHandler>,
    stats: Arc<KafkaServerStats>,
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
}

impl KafkaServer {
    pub fn new(config: KafkaServerConfig, handler: Arc<KafkaHandler>) -> Self {
        Self {
            config,
            handler,
            stats: Arc::new(KafkaServerStats::new()),
            shutdown_tx: None,
        }
    }

    pub fn stats(&self) -> &Arc<KafkaServerStats> {
        &self.stats
    }

    /// Start the Kafka protocol server.
    pub async fn run(&mut self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;
        info!("Kafka server listening on {}", self.config.bind_addr);

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        // Periodic session expiry task
        let handler_clone = Arc::clone(&self.handler);
        let mut expiry_rx = shutdown_rx.clone();
        let expiry_interval = self.config.session_expiry_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(expiry_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        handler_clone.expire_sessions();
                    }
                    _ = expiry_rx.changed() => break,
                }
            }
        });

        let max_conns = self.config.max_connections;
        let read_buf_size = self.config.read_buffer_size;

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = result?;
                    let active = self.stats.active_connections.load(Ordering::Relaxed);
                    if active >= max_conns as u64 {
                        warn!("Max connections reached ({max_conns}), rejecting {addr}");
                        drop(stream);
                        continue;
                    }

                    self.stats.active_connections.fetch_add(1, Ordering::Relaxed);
                    self.stats.total_connections.fetch_add(1, Ordering::Relaxed);

                    let handler = Arc::clone(&self.handler);
                    let stats = Arc::clone(&self.stats);
                    let mut conn_shutdown_rx = shutdown_rx.clone();

                    tokio::spawn(async move {
                        debug!("Kafka connection from {addr}");
                        if let Err(e) = handle_connection(
                            stream,
                            handler.clone(),
                            &stats,
                            &mut conn_shutdown_rx,
                            read_buf_size,
                        ).await {
                            debug!("Connection {addr} closed: {e}");
                        }
                        stats.active_connections.fetch_sub(1, Ordering::Relaxed);
                    });
                }
                _ = shutdown_rx.changed() => {
                    info!("Kafka server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Signal the server to shut down.
    pub fn shutdown(&self) {
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(true);
        }
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    handler: Arc<KafkaHandler>,
    stats: &KafkaServerStats,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    read_buf_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut reader, mut writer) = stream.into_split();
    let mut conn = KafkaConnection::new();
    let mut read_buf = vec![0u8; read_buf_size];

    loop {
        tokio::select! {
            n = reader.read(&mut read_buf) => {
                let n = n?;
                if n == 0 {
                    // Connection closed
                    break;
                }
                conn.feed_data(&read_buf[..n]);

                // Process all complete requests in the buffer
                loop {
                    match conn.try_decode_request() {
                        Ok(Some((header, request))) => {
                            stats.total_requests.fetch_add(1, Ordering::Relaxed);
                            let correlation_id = header.correlation_id;
                            let response = handler.handle(&header, request).await;
                            conn.encode_response(correlation_id, &response);
                        }
                        Ok(None) => break, // need more data
                        Err(e) => {
                            error!("Kafka codec error: {e}");
                            return Err(e.into());
                        }
                    }
                }

                // Flush all pending writes
                if conn.has_pending_writes() {
                    let buf = conn.take_write_buf();
                    writer.write_all(&buf).await?;
                }
            }
            _ = shutdown_rx.changed() => {
                break;
            }
        }
    }

    // Cleanup: remove consumer group memberships
    handler.on_disconnect(&conn.consumer_member_ids);

    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = KafkaServerConfig::default();
        assert_eq!(config.bind_addr.port(), 9092);
        assert_eq!(config.max_connections, 10_000);
    }

    #[test]
    fn test_server_stats() {
        let stats = KafkaServerStats::new();
        assert_eq!(stats.active_connections.load(Ordering::Relaxed), 0);
        assert_eq!(stats.total_connections.load(Ordering::Relaxed), 0);
        stats.active_connections.fetch_add(1, Ordering::Relaxed);
        assert_eq!(stats.active_connections.load(Ordering::Relaxed), 1);
    }
}

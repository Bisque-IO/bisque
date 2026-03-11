//! AMQP 1.0 TCP server.
//!
//! `AmqpServer` binds a TCP socket and accepts connections, spawning
//! a task per connection that runs the AMQP 1.0 protocol state machine.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::broker::{BrokerAction, MessageBroker, NoopBroker};
use crate::connection::{AmqpConnection, ConnectionError, ConnectionPhase};
use crate::types::{AmqpMessage, condition};

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
    /// Catalog name for metrics labels.
    pub catalog_name: String,
}

impl Default for AmqpServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:5672".parse().unwrap(),
            read_buf_size: 8192,
            max_connections: 0,
            catalog_name: String::new(),
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

    pub fn with_catalog_name(mut self, name: impl Into<String>) -> Self {
        self.catalog_name = name.into();
        self
    }
}

// =============================================================================
// AmqpServer
// =============================================================================

/// AMQP 1.0 TCP server.
///
/// Accepts TCP connections and drives the AMQP 1.0 protocol for each one.
pub struct AmqpServer<B: MessageBroker = NoopBroker> {
    config: AmqpServerConfig,
    /// Active connection count.
    active_connections: Arc<AtomicUsize>,
    /// Message broker for engine integration.
    broker: Arc<B>,
}

impl AmqpServer<NoopBroker> {
    /// Create a new AMQP server with the given configuration and no-op broker.
    pub fn new(config: AmqpServerConfig) -> Self {
        Self {
            config,
            active_connections: Arc::new(AtomicUsize::new(0)),
            broker: Arc::new(NoopBroker),
        }
    }
}

impl<B: MessageBroker> AmqpServer<B> {
    /// Create a new AMQP server with the given configuration and broker.
    pub fn with_broker(config: AmqpServerConfig, broker: B) -> Self {
        Self {
            config,
            active_connections: Arc::new(AtomicUsize::new(0)),
            broker: Arc::new(broker),
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
                                let current = self.active_connections.load(Ordering::Relaxed);
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
                            let broker = self.broker.clone();
                            let catalog_name = self.config.catalog_name.clone();

                            active.fetch_add(1, Ordering::Relaxed);

                            tokio::spawn(async move {
                                debug!(peer = %peer_addr, "accepted AMQP 1.0 connection");
                                match handle_connection(stream, peer_addr, read_buf_size, broker, &catalog_name, &mut shutdown_rx).await {
                                    Ok(()) => {
                                        debug!(peer = %peer_addr, "AMQP connection closed normally");
                                    }
                                    Err(e) => {
                                        warn!(peer = %peer_addr, error = %e, "AMQP connection error");
                                    }
                                }
                                active.fetch_sub(1, Ordering::Relaxed);
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
        self.active_connections.load(Ordering::Relaxed)
    }
}

// =============================================================================
// Per-Connection Handler
// =============================================================================

/// Handle a single AMQP 1.0 connection.
async fn handle_connection<B: MessageBroker>(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    read_buf_size: usize,
    broker: Arc<B>,
    catalog_name: &str,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), ConnectionError> {
    let mut conn = AmqpConnection::new(catalog_name);
    // Reusable read buffer — allocated once per connection, reused for every read.
    let mut read_buf = vec![0u8; read_buf_size];

    info!(
        conn_id = conn.id,
        peer = %peer_addr,
        "starting AMQP 1.0 connection handler"
    );

    loop {
        // C2: Drain pending broker actions and execute them asynchronously.
        let actions = conn.take_pending_actions();
        for action in actions {
            execute_broker_action(&*broker, &mut conn, action).await;
        }

        // Flush any pending writes first.
        if conn.has_pending_writes() {
            let data = conn.take_write_bytes();
            if let Err(e) = stream.write_all(&data).await {
                warn!(
                    conn_id = conn.id,
                    error = %e,
                    "write error"
                );
                return Ok(());
            }
            conn.last_frame_sent = std::time::Instant::now();
        }

        if conn.phase == ConnectionPhase::Closed {
            return Ok(());
        }

        // Determine heartbeat/idle timeout interval
        let heartbeat_ms = conn.heartbeat_interval_ms();
        let timeout_duration = if heartbeat_ms > 0 {
            std::time::Duration::from_millis(heartbeat_ms)
        } else {
            std::time::Duration::from_secs(60) // default poll interval
        };

        // Read more data from the socket, with heartbeat timer.
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
                                    let data = conn.take_write_bytes();
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
                                    let data = conn.take_write_bytes();
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
            // Heartbeat / idle timeout timer
            _ = tokio::time::sleep(timeout_duration) => {
                // Check idle timeout (did we stop hearing from the peer?)
                if conn.check_idle_timeout() {
                    if conn.has_pending_writes() {
                        let data = conn.take_write_bytes();
                        let _ = stream.write_all(&data).await;
                    }
                    return Ok(());
                }
                // Send heartbeat if needed
                conn.maybe_send_heartbeat();
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!(conn_id = conn.id, "shutting down connection");
                    conn.send_connection_error(
                        condition::CONNECTION_FORCED,
                        "server shutting down",
                    );
                    if conn.has_pending_writes() {
                        let data = conn.take_write_bytes();
                        let _ = stream.write_all(&data).await;
                    }
                    return Ok(());
                }
            }
        }
    }
}

/// Execute a single broker action asynchronously (C2).
async fn execute_broker_action<B: MessageBroker>(
    broker: &B,
    conn: &mut AmqpConnection,
    action: BrokerAction,
) {
    match action {
        BrokerAction::ResolveAddress {
            link_handle,
            session_channel,
            address,
        } => {
            match broker.resolve_address(&address).await {
                Ok((entity_type, entity_id)) => {
                    // Update the link with resolved entity info
                    if let Some(session) = conn.sessions.get_mut(&session_channel) {
                        if let Some(link) = session.links.get_mut(&link_handle) {
                            link.entity_type = Some(entity_type);
                            link.entity_id = Some(entity_id);

                            // If sender-role, register as consumer
                            if link.role == crate::types::Role::Sender {
                                if let Some(cid) = link.consumer_id {
                                    if let Err(e) =
                                        broker.register_consumer(cid, entity_type, entity_id).await
                                    {
                                        warn!(
                                            link_handle,
                                            error = %e,
                                            "failed to register consumer"
                                        );
                                    }
                                }
                            }

                            debug!(link_handle, entity_type, entity_id, "address resolved");
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        link_handle,
                        address = %address,
                        error = %e,
                        "failed to resolve address"
                    );
                }
            }
        }
        BrokerAction::RegisterConsumer {
            consumer_id,
            entity_type,
            entity_id,
            ..
        } => {
            if let Err(e) = broker
                .register_consumer(consumer_id, entity_type, entity_id)
                .await
            {
                warn!(consumer_id, error = %e, "failed to register consumer");
            }
        }
        BrokerAction::Publish {
            entity_id, payload, ..
        } => {
            let msg = AmqpMessage {
                body: smallvec::smallvec![payload],
                ..Default::default()
            };
            if let Err(e) = broker.publish_to_topic(entity_id, &msg).await {
                warn!(entity_id, error = %e, "failed to publish to topic");
            }
        }
        BrokerAction::Enqueue {
            entity_id, payload, ..
        } => {
            let msg = AmqpMessage {
                body: smallvec::smallvec![payload],
                ..Default::default()
            };
            if let Err(e) = broker.enqueue(entity_id, &msg).await {
                warn!(entity_id, error = %e, "failed to enqueue message");
            }
        }
        BrokerAction::Settle {
            entity_id,
            message_ids,
            action,
        } => {
            if let Err(e) = broker.settle(entity_id, &message_ids, action).await {
                warn!(entity_id, error = %e, "failed to settle deliveries");
            }
        }
        BrokerAction::FetchMessages {
            link_handle,
            session_channel,
            entity_type,
            entity_id,
            consumer_id,
            max_count,
        } => {
            let messages = if entity_type == "topic" {
                broker
                    .fetch_topic_messages(entity_id, 0, max_count)
                    .await
                    .unwrap_or_default()
            } else {
                broker
                    .fetch_messages(entity_id, consumer_id, max_count)
                    .await
                    .unwrap_or_default()
            };

            if !messages.is_empty() {
                conn.deliver_outbound(session_channel, link_handle, messages);
            }
        }
        BrokerAction::DisconnectConsumer { consumer_id } => {
            if let Err(e) = broker.disconnect_consumer(consumer_id).await {
                warn!(consumer_id, error = %e, "failed to disconnect consumer");
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

    // =================================================================
    // Gap 1: AmqpServerConfig::with_catalog_name() builder
    // =================================================================

    #[test]
    fn test_server_config_with_catalog_name() {
        let config = AmqpServerConfig::default().with_catalog_name("my-catalog");
        assert_eq!(config.catalog_name, "my-catalog");
    }

    #[test]
    fn test_server_config_with_catalog_name_from_string() {
        let name = String::from("dynamic-catalog");
        let config = AmqpServerConfig::default().with_catalog_name(name);
        assert_eq!(config.catalog_name, "dynamic-catalog");
    }

    // =================================================================
    // Gap 2: AmqpServer::with_broker() - create server with NoopBroker
    // =================================================================

    #[test]
    fn test_server_with_broker() {
        let config = AmqpServerConfig::default();
        let server = AmqpServer::with_broker(config, NoopBroker);
        assert_eq!(server.active_connection_count(), 0);
    }

    // =================================================================
    // Gap 3: Server max_connections enforcement
    // =================================================================

    #[tokio::test]
    async fn test_server_max_connections_enforcement() {
        let config = AmqpServerConfig::default()
            .with_bind_addr("127.0.0.1:0".parse().unwrap())
            .with_max_connections(1);

        // Bind to get an actual port.
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

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // First connection: should be accepted.
        let mut client1 = TcpStream::connect(local_addr).await.unwrap();
        client1.write_all(&crate::types::AMQP_HEADER).await.unwrap();
        let mut buf = [0u8; 4096];
        let n = client1.read(&mut buf).await.unwrap();
        assert!(n > 0, "first connection should be accepted");

        // Give server time to update active count.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(server.active_connection_count(), 1);

        // Second connection: should be rejected (dropped immediately).
        let client2_result = TcpStream::connect(local_addr).await;
        if let Ok(mut client2) = client2_result {
            // The server accepts the TCP connection but drops it immediately.
            // Reading should return 0 bytes (connection closed).
            let mut buf2 = [0u8; 4096];
            let n2 = client2.read(&mut buf2).await.unwrap_or(0);
            assert_eq!(n2, 0, "rejected connection should be closed by server");
        }

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }

    // =================================================================
    // Gap 4: Multiple concurrent connections
    // =================================================================

    #[tokio::test]
    async fn test_multiple_concurrent_connections() {
        let config = AmqpServerConfig::default()
            .with_bind_addr("127.0.0.1:0".parse().unwrap())
            .with_max_connections(10);

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

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Connect 3 clients.
        let mut clients = Vec::new();
        for _ in 0..3 {
            let mut client = TcpStream::connect(local_addr).await.unwrap();
            client.write_all(&crate::types::AMQP_HEADER).await.unwrap();
            let mut buf = [0u8; 4096];
            let n = client.read(&mut buf).await.unwrap();
            assert!(n > 0);
            clients.push(client);
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(server.active_connection_count(), 3);

        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }

    // =================================================================
    // Gap 5: Server bind failure - port already in use
    // =================================================================

    #[tokio::test]
    async fn test_server_bind_failure_port_in_use() {
        // Bind a listener to grab a port.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Keep listener alive so the port stays occupied.

        let config = AmqpServerConfig::default().with_bind_addr(addr);
        let server = AmqpServer::new(config);

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let result = server.run(shutdown_rx).await;
        assert!(
            result.is_err(),
            "binding to an already-used port should fail"
        );

        drop(listener);
    }
}

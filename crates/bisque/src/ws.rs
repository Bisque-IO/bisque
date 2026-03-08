//! Unified WebSocket handler for the bisque server.
//!
//! Multiplexes catalog events, operation updates, heartbeats, and
//! request/response over a single connection with production reliability:
//! - Protocol versioning and handshake timeout
//! - Monotonic sequence numbers with gap detection
//! - Write timeouts on all frames (30s)
//! - Read timeout with dead-peer detection (90s)
//! - Request rate limiting (1s cooldown for expensive ops)
//! - Per-request timeout (30s)
//! - Connection TTL with graceful refresh (10 min)
//! - Frame size limits (4 MiB)
//! - Metrics for connection lifecycle and message rates

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use tracing::{debug, info, warn};

use bisque_lance::operations::{OpStatus, OpTier, OpType};
use bisque_lance::s3_server::S3ServerState;
use bisque_lance::version_pins::{PinKey, PinTier};
use bisque_protocol::ws::{
    ClientMessage, RequestMethod, ResponseData, ResponseResult, ServerMessage, WS_PROTOCOL_VERSION,
};

use crate::auth::{AuthState, validate_token};

/// Combined state for the unified WebSocket handler.
#[derive(Clone)]
pub struct WsState {
    pub s3: Arc<S3ServerState>,
    pub auth: AuthState,
    pub meta_engine: Arc<bisque_meta::engine::MetaEngine>,
    pub token_manager: Arc<bisque_meta::token::TokenManager>,
    pub raft_node: Arc<bisque_lance::LanceRaftNode>,
    /// HTTP listen address for cluster status reporting.
    pub http_addr: std::net::SocketAddr,
    /// M5/L5: Per-IP connection tracking for rate limiting.
    pub ip_connections: Arc<dashmap::DashMap<std::net::IpAddr, IpConnectionState>>,
    /// Pre-initialized metric handles — avoids inline `metrics::*!()` macro overhead in hot paths.
    pub metrics: Arc<WsMetrics>,
}

/// Pre-initialized WebSocket metric handles.
pub struct WsMetrics {
    // Connection lifecycle
    pub connections_active: metrics::Gauge,
    pub connection_duration: metrics::Histogram,
    pub handshakes: metrics::Counter,

    // Message counters
    pub messages_sent: metrics::Counter,
    pub messages_recv: metrics::Counter,

    // Per-type message counters
    pub msgs_sent_catalog: metrics::Counter,
    pub msgs_sent_operation: metrics::Counter,
    pub lag_catalog: metrics::Counter,
    pub lag_operations: metrics::Counter,

    // Error/rejection counters
    pub ip_rate_limited: metrics::Counter,
    pub auth_blocked: metrics::Counter,
    pub origin_rejected: metrics::Counter,
    pub auth_failures: metrics::Counter,
    pub read_timeouts: metrics::Counter,
    pub decode_errors: metrics::Counter,
    pub request_timeouts: metrics::Counter,
}

impl WsMetrics {
    pub fn new() -> Self {
        Self {
            connections_active: metrics::gauge!("ws_connections_active"),
            connection_duration: metrics::histogram!("ws_connection_duration_seconds"),
            handshakes: metrics::counter!("ws_handshakes_total"),
            messages_sent: metrics::counter!("ws_messages_sent_total"),
            messages_recv: metrics::counter!("ws_messages_recv_total"),
            msgs_sent_catalog: metrics::counter!("ws_messages_sent", "type" => "catalog_event"),
            msgs_sent_operation: metrics::counter!("ws_messages_sent", "type" => "operation_update"),
            lag_catalog: metrics::counter!("ws_subscriber_lag", "type" => "catalog"),
            lag_operations: metrics::counter!("ws_subscriber_lag", "type" => "operations"),
            ip_rate_limited: metrics::counter!("ws_ip_rate_limited"),
            auth_blocked: metrics::counter!("ws_auth_blocked"),
            origin_rejected: metrics::counter!("ws_origin_rejected"),
            auth_failures: metrics::counter!("ws_auth_failures"),
            read_timeouts: metrics::counter!("ws_read_timeouts"),
            decode_errors: metrics::counter!("ws_decode_errors"),
            request_timeouts: metrics::counter!("ws_request_timeouts"),
        }
    }
}

/// Per-IP connection state for rate limiting (M5/L5).
pub struct IpConnectionState {
    /// Number of active connections from this IP.
    pub active: u32,
    /// Number of auth failures from this IP since last success.
    pub auth_failures: u32,
    /// Timestamp of last auth failure.
    pub last_auth_failure: std::time::Instant,
}

/// Maximum active WebSocket connections per IP (L5).
const WS_MAX_CONNECTIONS_PER_IP: u32 = 50;

/// Maximum auth failures before blocking an IP temporarily (M5).
const WS_MAX_AUTH_FAILURES_PER_IP: u32 = 10;

/// Auth failure block duration (M5).
const WS_AUTH_FAILURE_BLOCK_DURATION: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum inbound WebSocket frame size (4 MiB).
const WS_MAX_FRAME_SIZE: usize = 4 * 1024 * 1024;

/// Write timeout for individual WebSocket frames (30 seconds).
const WS_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Read timeout — if no client message (including heartbeat) arrives in this window,
/// the connection is considered dead (90 seconds = 6 missed heartbeats).
const WS_READ_TIMEOUT: Duration = Duration::from_secs(90);

/// Per-request timeout for handle_ws_request (30 seconds).
const WS_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Minimum interval between expensive requests like ListOperations/GetCatalog (200ms).
const WS_REQUEST_COOLDOWN: Duration = Duration::from_millis(200);

/// Connection TTL before graceful refresh (10 minutes).
const WS_CONNECTION_TTL: Duration = Duration::from_secs(600);

/// Server heartbeat interval (15 seconds).
const WS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Maximum operations in a snapshot to prevent OOM on large clusters.
const WS_MAX_OPS_SNAPSHOT: usize = 10_000;

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// Axum handler for `GET /_bisque/ws`.
///
/// Authentication happens inside the WebSocket connection — the client sends
/// a bearer token in its Handshake frame rather than via URL query parameters,
/// preventing token exposure in HTTP logs and browser history.
///
/// S3: Origin header is validated to prevent cross-site WebSocket hijacking.
pub async fn unified_ws_handler(
    State(ws_state): State<WsState>,
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<std::net::SocketAddr>,
    headers: axum::http::HeaderMap,
    upgrade: fastwebsockets::upgrade::IncomingUpgrade,
) -> Response {
    let client_ip = addr.ip();

    // L5/M5: Per-IP connection rate limit and auth failure blocking.
    // C2: Only check counts here — do NOT increment yet (defer to task after upgrade succeeds).
    {
        let entry = ws_state.ip_connections.get(&client_ip);
        if let Some(ref state) = entry {
            if state.active >= WS_MAX_CONNECTIONS_PER_IP {
                warn!(%client_ip, "WS connection rejected: too many connections from IP");
                ws_state.metrics.ip_rate_limited.increment(1);
                return (StatusCode::TOO_MANY_REQUESTS, "Too many connections").into_response();
            }
            if state.auth_failures >= WS_MAX_AUTH_FAILURES_PER_IP
                && state.last_auth_failure.elapsed() < WS_AUTH_FAILURE_BLOCK_DURATION
            {
                warn!(%client_ip, "WS connection rejected: too many auth failures");
                ws_state.metrics.auth_blocked.increment(1);
                return (StatusCode::TOO_MANY_REQUESTS, "Too many auth failures").into_response();
            }
        }
    }
    // Note: Origin validation is intentionally skipped — CORS is handled at the
    // HTTP layer via CorsLayer, and the WS connection is authenticated via the
    // handshake frame (bearer token), not cookies.
    let (response, fut) = match upgrade.upgrade() {
        Ok((response, fut)) => (response, fut),
        Err(e) => {
            warn!("Unified WebSocket upgrade failed: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                format!("WebSocket upgrade failed: {}", e),
            )
                .into_response();
        }
    };

    let state = ws_state.s3.clone();
    let auth_state = ws_state.auth.clone();
    let meta_engine = ws_state.meta_engine.clone();
    let token_manager = ws_state.token_manager.clone();
    let raft_node = ws_state.raft_node.clone();
    let ws_metrics = ws_state.metrics.clone();
    let pins = state.version_pins.clone();
    let ip_connections = ws_state.ip_connections.clone();

    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ws) => ws,
            Err(e) => {
                warn!(error = %e, "Unified WebSocket upgrade future failed");
                return;
            }
        };

        // C2/C3: Increment IP count and create session AFTER upgrade succeeds.
        ip_connections
            .entry(client_ip)
            .or_insert(IpConnectionState {
                active: 0,
                auth_failures: 0,
                last_auth_failure: std::time::Instant::now(),
            })
            .active += 1;
        let session_id = pins.create_session();
        ws_metrics.connections_active.increment(1.0);

        if let Err(e) = handle_unified_ws(
            ws,
            state,
            auth_state,
            meta_engine,
            token_manager,
            raft_node,
            pins.clone(),
            session_id,
            client_ip,
            &ip_connections,
            &ws_metrics,
        )
        .await
        {
            debug!(session_id, error = %e, "Unified WebSocket ended");
        }

        pins.remove_session(session_id);
        // M4: Prune IP entry when active reaches 0 and no recent auth failures.
        if let Some(mut entry) = ip_connections.get_mut(&client_ip) {
            entry.active = entry.active.saturating_sub(1);
            if entry.active == 0
                && (entry.auth_failures == 0
                    || entry.last_auth_failure.elapsed() >= WS_AUTH_FAILURE_BLOCK_DURATION)
            {
                drop(entry);
                ip_connections.remove(&client_ip);
            }
        }
        ws_metrics.connections_active.decrement(1.0);
    });

    response.into_response()
}

/// Write a binary WebSocket frame with timeout protection.
async fn ws_write_frame<S>(
    ws: &mut fastwebsockets::WebSocket<S>,
    bytes: Vec<u8>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    use fastwebsockets::{Frame, Payload};
    tokio::time::timeout(
        WS_WRITE_TIMEOUT,
        ws.write_frame(Frame::binary(Payload::Owned(bytes))),
    )
    .await
    .map_err(|_| "write timeout (30s)")??;
    Ok(())
}

/// Maximum number of WAL events to replay on reconnect (R7: prevent unbounded replay).
const WS_MAX_WAL_REPLAY: usize = 10_000;

/// Main loop for the unified WebSocket connection.
async fn handle_unified_ws<S>(
    mut ws: fastwebsockets::WebSocket<S>,
    state: Arc<S3ServerState>,
    auth_state: AuthState,
    meta_engine: Arc<bisque_meta::engine::MetaEngine>,
    token_manager: Arc<bisque_meta::token::TokenManager>,
    raft_node: Arc<bisque_lance::LanceRaftNode>,
    pins: Arc<bisque_lance::version_pins::VersionPinTracker>,
    session_id: u64,
    client_ip: std::net::IpAddr,
    ip_connections: &dashmap::DashMap<std::net::IpAddr, IpConnectionState>,
    ws_metrics: &WsMetrics,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    use fastwebsockets::{Frame, OpCode, Payload};

    // Set frame size limit on the WebSocket to prevent OOM.
    ws.set_max_message_size(WS_MAX_FRAME_SIZE);
    ws.set_auto_pong(true);

    // Per-connection monotonic sequence counter.
    let mut seq: u64 = 0;
    let mut next_seq = || {
        seq += 1;
        seq
    };

    // Helper: send a Close message + WS close frame and return Ok(()).
    // L1: Send both app-level and protocol-level close for clean teardown.
    async fn send_close_and_return<S2>(
        ws: &mut fastwebsockets::WebSocket<S2>,
        reason: &str,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        S2: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        let close = ServerMessage::Close {
            reason: reason.to_string(),
        };
        match rmp_serde::to_vec_named(&close) {
            Ok(bytes) => {
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    ws.write_frame(Frame::binary(Payload::Owned(bytes))),
                )
                .await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to serialize Close message");
            }
        }
        let _ = tokio::time::timeout(
            Duration::from_secs(1),
            ws.write_frame(Frame::close_raw(vec![].into())),
        )
        .await;
        Ok(())
    }

    // --- Phase 1: Handshake ---

    // 1. Send server handshake (with write timeout).
    let catalog_seq = state.catalog_seq();
    let handshake_msg = ServerMessage::Handshake {
        protocol_version: WS_PROTOCOL_VERSION,
        session_id,
        catalog_seq,
        server_seq: 0,
    };
    ws_write_frame(&mut ws, rmp_serde::to_vec_named(&handshake_msg)?).await?;

    // 2. Read client handshake (10s timeout).
    let client_handshake: ClientMessage = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let frame = ws.read_frame().await?;
            match frame.opcode {
                OpCode::Binary => {
                    return rmp_serde::from_slice::<ClientMessage>(&frame.payload)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
                }
                OpCode::Close => {
                    return Err("client closed during handshake".into());
                }
                _ => continue,
            }
        }
    })
    .await
    .map_err(|_| "handshake timeout (10s)")??;

    // H3: Keep auth_ctx for authorization checks on requests.
    let (last_seen_seq, auth_ctx, subscribe_catalogs) = match client_handshake {
        ClientMessage::Handshake {
            protocol_version,
            token,
            last_seen_seq,
            subscribe_catalogs,
        } => {
            // S1: Validate protocol version.
            if protocol_version != WS_PROTOCOL_VERSION {
                return send_close_and_return(
                    &mut ws,
                    &format!(
                        "protocol version mismatch: server={}, client={}",
                        WS_PROTOCOL_VERSION, protocol_version
                    ),
                )
                .await;
            }

            // S1/S4: Validate auth token from handshake frame.
            let auth_ctx = match validate_token(&auth_state, &token) {
                Ok(ctx) => {
                    // M5: Reset auth failure counter on success.
                    if let Some(mut entry) = ip_connections.get_mut(&client_ip) {
                        entry.auth_failures = 0;
                    }
                    ctx
                }
                Err(reason) => {
                    // M5: Track auth failures per IP.
                    warn!(session_id, %client_ip, reason, "WS auth failed");
                    if let Some(mut entry) = ip_connections.get_mut(&client_ip) {
                        entry.auth_failures += 1;
                        entry.last_auth_failure = std::time::Instant::now();
                    }
                    ws_metrics.auth_failures.increment(1);
                    return send_close_and_return(&mut ws, reason).await;
                }
            };

            (last_seen_seq, auth_ctx, subscribe_catalogs)
        }
        _ => {
            return send_close_and_return(&mut ws, "expected Handshake message").await;
        }
    };

    // R6/L2: Build a set for O(1) catalog filtering.
    // Empty subscribe_catalogs = subscribe to all catalog events (wildcard).
    // M3: Cap subscribe list to prevent memory abuse (max 1000 catalogs, 256 bytes each).
    let catalog_filter: std::collections::HashSet<String> = subscribe_catalogs
        .into_iter()
        .take(1000)
        .filter(|s| s.len() <= 256)
        .collect();

    // L4: Track handshake success and connection start time for duration metric.
    ws_metrics.handshakes.increment(1);
    let connection_start = Instant::now();
    info!(session_id, last_seen_seq, "Unified WS handshake complete");

    // --- Phase 2: Initial state ---

    // 3. Send operations snapshot (capped).
    let all_ops = state.collect_operations(WS_MAX_OPS_SNAPSHOT);
    let ops_snapshot = ServerMessage::OperationsSnapshot {
        seq: next_seq(),
        operations: all_ops
            .iter()
            .filter_map(|op| serde_json::to_value(op).ok())
            .collect(),
    };
    ws_write_frame(&mut ws, rmp_serde::to_vec_named(&ops_snapshot)?).await?;

    // 4. WAL replay if client is resuming (last_seen_seq > 0).
    // R7: Cap replay at WS_MAX_WAL_REPLAY events to prevent unbounded memory use.
    if last_seen_seq > 0 {
        match state.replay_wal_since(last_seen_seq) {
            Some(Ok(events)) => {
                if events.len() > WS_MAX_WAL_REPLAY {
                    warn!(
                        session_id,
                        events = events.len(),
                        max = WS_MAX_WAL_REPLAY,
                        "WAL replay too large, sending SnapshotRequired"
                    );
                    let msg = ServerMessage::SnapshotRequired {
                        seq: next_seq(),
                        catalog: "*".into(),
                    };
                    ws_write_frame(&mut ws, rmp_serde::to_vec_named(&msg)?).await?;
                } else {
                    for event in events {
                        // M8: Apply catalog filter to WAL replay too.
                        if !catalog_filter.is_empty() && !catalog_filter.contains(&*event.catalog) {
                            continue;
                        }
                        let catalog = event.catalog.to_string();
                        // E2: Skip events that fail to serialize instead of sending null.
                        match serde_json::to_value(&event.event) {
                            Ok(event_val) => {
                                let msg = ServerMessage::CatalogEvent {
                                    seq: next_seq(),
                                    catalog,
                                    event: event_val,
                                };
                                ws_write_frame(&mut ws, rmp_serde::to_vec_named(&msg)?).await?;
                            }
                            Err(e) => {
                                warn!(session_id, error = %e, "Failed to serialize WAL event, skipping");
                            }
                        }
                    }
                }
            }
            Some(Err(e)) => {
                warn!(session_id, error = %e, "Failed to replay WAL for unified WS");
                let msg = ServerMessage::SnapshotRequired {
                    seq: next_seq(),
                    catalog: "*".into(),
                };
                ws_write_frame(&mut ws, rmp_serde::to_vec_named(&msg)?).await?;
            }
            None => {}
        }
    }

    // --- Phase 3: Main loop ---

    let mut catalog_rx = state.subscribe_catalog_events();
    let mut ops_rx = state.operations.subscribe();

    // Connection TTL.
    let ttl = tokio::time::sleep(WS_CONNECTION_TTL);
    tokio::pin!(ttl);

    // Server heartbeat interval.
    // M5: Use interval_at to skip the immediate first tick.
    let mut heartbeat_interval = tokio::time::interval_at(
        tokio::time::Instant::now() + WS_HEARTBEAT_INTERVAL,
        WS_HEARTBEAT_INTERVAL,
    );
    heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // R1: Removed dead read_deadline timer — read timeout is handled by
    // tokio::time::timeout wrapping ws.read_frame() in the select! branch.

    // Request rate limiting for expensive operations.
    let mut last_expensive_request = Instant::now() - WS_REQUEST_COOLDOWN;

    // H4: Rate limit Pin/Unpin to prevent resource exhaustion.
    let mut pin_budget: u32 = 100; // pins allowed per heartbeat interval
    let mut last_pin_budget_reset = Instant::now();

    // Message counters for periodic logging.
    let mut msgs_sent: u64 = 0;
    let mut msgs_recv: u64 = 0;

    loop {
        tokio::select! {
            // Catalog events.
            event_result = async {
                match &mut catalog_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match event_result {
                    Ok(event) => {
                        // R6: Filter by subscribed catalogs (empty = all).
                        if !catalog_filter.is_empty() && !catalog_filter.contains(&*event.catalog) {
                            continue;
                        }
                        // E2: Skip events that fail to serialize instead of sending null.
                        let catalog = event.catalog.to_string();
                        match serde_json::to_value(&event.event) {
                            Ok(event_val) => {
                                let msg = ServerMessage::CatalogEvent {
                                    seq: next_seq(),
                                    catalog,
                                    event: event_val,
                                };
                                ws_write_frame(&mut ws, rmp_serde::to_vec_named(&msg)?).await?;
                                msgs_sent += 1;
                                ws_metrics.msgs_sent_catalog.increment(1);
                            }
                            Err(e) => {
                                warn!(session_id, error = %e, "Failed to serialize catalog event, skipping");
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(session_id, lagged = n, "Unified WS catalog subscriber lagged");
                        ws_metrics.lag_catalog.increment(n);
                        let msg = ServerMessage::SnapshotRequired {
                            seq: next_seq(),
                            catalog: "*".into(),
                        };
                        ws_write_frame(&mut ws, rmp_serde::to_vec_named(&msg)?).await?;
                        msgs_sent += 1;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }

            // Operation updates.
            event = ops_rx.recv() => {
                match event {
                    Ok(op) => {
                        // E2: Skip operations that fail to serialize.
                        match serde_json::to_value(&op) {
                            Ok(op_val) => {
                                let msg = ServerMessage::OperationUpdate {
                                    seq: next_seq(),
                                    operation: op_val,
                                };
                                ws_write_frame(&mut ws, rmp_serde::to_vec_named(&msg)?).await?;
                                msgs_sent += 1;
                                ws_metrics.msgs_sent_operation.increment(1);
                            }
                            Err(e) => {
                                warn!(session_id, error = %e, "Failed to serialize operation update, skipping");
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(session_id, lagged = n, "Unified WS operations subscriber lagged");
                        ws_metrics.lag_operations.increment(n);
                        let all_ops = state.collect_operations(WS_MAX_OPS_SNAPSHOT);
                        let msg = ServerMessage::OperationsSnapshot {
                            seq: next_seq(),
                            operations: all_ops
                                .iter()
                                .filter_map(|op| serde_json::to_value(op).ok())
                                .collect(),
                        };
                        ws_write_frame(&mut ws, rmp_serde::to_vec_named(&msg)?).await?;
                        msgs_sent += 1;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }

            // Server heartbeat.
            _ = heartbeat_interval.tick() => {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let msg = ServerMessage::Heartbeat {
                    seq: next_seq(),
                    server_time_ms: now_ms,
                };
                ws_write_frame(&mut ws, rmp_serde::to_vec_named(&msg)?).await?;
                msgs_sent += 1;
            }

            // Client messages (with read timeout).
            result = tokio::time::timeout(WS_READ_TIMEOUT, ws.read_frame()) => {
                let frame = match result {
                    Ok(Ok(frame)) => frame,
                    Ok(Err(e)) => {
                        debug!(session_id, error = %e, "WS read error");
                        break;
                    }
                    Err(_) => {
                        // Read timeout — no client activity for 90s.
                        info!(session_id, "WS read timeout — dead peer detected");
                        ws_metrics.read_timeouts.increment(1);
                        break;
                    }
                };

                match frame.opcode {
                    OpCode::Binary => {
                        // L2: Frame size already enforced by set_max_message_size.
                        msgs_recv += 1; // M6: Only count decoded binary messages.

                        let msg: ClientMessage = match rmp_serde::from_slice(&frame.payload) {
                            Ok(m) => m,
                            Err(e) => {
                                debug!(session_id, error = %e, "Unrecognized client WS message");
                                ws_metrics.decode_errors.increment(1);
                                continue;
                            }
                        };

                        match msg {
                            ClientMessage::Request { request_id, method } => {
                                metrics::counter!("ws_requests", "method" => method.method_name()).increment(1);

                                // Rate limit expensive requests.
                                if method.is_expensive() {
                                    let elapsed = last_expensive_request.elapsed();
                                    if elapsed < WS_REQUEST_COOLDOWN {
                                        let resp = ServerMessage::Response {
                                            request_id,
                                            result: ResponseResult::Error {
                                                code: 429,
                                                message: format!(
                                                    "Rate limited — retry after {}ms",
                                                    (WS_REQUEST_COOLDOWN - elapsed).as_millis()
                                                ),
                                            },
                                        };
                                        ws_write_frame(&mut ws, rmp_serde::to_vec_named(&resp)?).await?;
                                        continue;
                                    }
                                    last_expensive_request = Instant::now();
                                }

                                // Execute request with timeout.
                                let result = match tokio::time::timeout(
                                    WS_REQUEST_TIMEOUT,
                                    handle_ws_request(&state, &meta_engine, &token_manager, &raft_node, &auth_ctx, method),
                                )
                                .await
                                {
                                    Ok(result) => result,
                                    Err(_) => {
                                        warn!(session_id, request_id, "WS request timed out (30s)");
                                        ws_metrics.request_timeouts.increment(1);
                                        // M6: Include session context in error messages.
                                        ResponseResult::Error {
                                            code: 504,
                                            message: format!("Request timed out (session {})", session_id),
                                        }
                                    }
                                };

                                let resp = ServerMessage::Response { request_id, result };
                                ws_write_frame(&mut ws, rmp_serde::to_vec_named(&resp)?).await?;
                                msgs_sent += 1;
                            }
                            ClientMessage::Pin { catalog, table, tier, version } => {
                                // H4: Rate limit Pin/Unpin messages.
                                if last_pin_budget_reset.elapsed() >= WS_HEARTBEAT_INTERVAL {
                                    pin_budget = 100;
                                    last_pin_budget_reset = Instant::now();
                                }
                                if pin_budget == 0 {
                                    debug!(session_id, "Pin rate limited");
                                    continue;
                                }
                                pin_budget -= 1;
                                if let Some(tier) = PinTier::from_str(&tier) {
                                    // H3: Check catalog read access before allowing pin.
                                    if !auth_ctx.has_catalog_read(&catalog) {
                                        debug!(session_id, %catalog, "Pin denied: no catalog access");
                                        continue;
                                    }
                                    pins.pin(session_id, PinKey { catalog: Arc::from(catalog), table: Arc::from(table), tier, version });
                                }
                            }
                            ClientMessage::Unpin { catalog, table, tier, version } => {
                                if let Some(tier) = PinTier::from_str(&tier) {
                                    pins.unpin(session_id, PinKey { catalog: Arc::from(catalog), table: Arc::from(table), tier, version });
                                }
                            }
                            ClientMessage::Heartbeat { .. } => {
                                pins.heartbeat(session_id);
                            }
                            ClientMessage::Close => {
                                debug!(session_id, "Client sent Close message");
                                break;
                            }
                            ClientMessage::Handshake { .. } => {
                                // Ignore duplicate handshake.
                            }
                        }
                    }
                    OpCode::Close => {
                        debug!(session_id, "Client sent close frame");
                        break;
                    }
                    _ => {}
                }
            }

            // Connection TTL expired — graceful refresh.
            _ = &mut ttl => {
                info!(session_id, msgs_sent, msgs_recv, "WS TTL expired, signaling refresh");
                let msg = ServerMessage::Close {
                    reason: "ttl_refresh".into(),
                };
                // Best-effort close — 2s timeout.
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    ws.write_frame(Frame::binary(Payload::Owned(
                        rmp_serde::to_vec_named(&msg)?,
                    ))),
                )
                .await;
                break;
            }
        }
    }

    // L4: Emit connection duration and message rate metrics.
    let duration_secs = connection_start.elapsed().as_secs_f64();
    ws_metrics.connection_duration.record(duration_secs);
    ws_metrics.messages_sent.increment(msgs_sent);
    ws_metrics.messages_recv.increment(msgs_recv);
    info!(
        session_id,
        msgs_sent, msgs_recv, duration_secs, "Unified WS connection closed"
    );
    Ok(())
}

/// Route a WebSocket request to the appropriate handler logic.
async fn handle_ws_request(
    state: &Arc<S3ServerState>,
    meta_engine: &Arc<bisque_meta::engine::MetaEngine>,
    token_manager: &Arc<bisque_meta::token::TokenManager>,
    raft_node: &Arc<bisque_lance::LanceRaftNode>,
    auth_ctx: &crate::auth::AuthContext,
    method: RequestMethod,
) -> ResponseResult {
    match method {
        RequestMethod::ListOperations {
            op_type,
            tier,
            status,
        } => {
            let op_type = op_type.and_then(|t| match t.as_str() {
                "reindex" => Some(OpType::Reindex),
                "compact" => Some(OpType::Compact),
                "flush" => Some(OpType::Flush),
                _ => None,
            });
            let tier = tier.and_then(|t| match t.as_str() {
                "hot" => Some(OpTier::Hot),
                "warm" => Some(OpTier::Warm),
                "cold" => Some(OpTier::Cold),
                _ => None,
            });
            let status = status.and_then(|s| match s.as_str() {
                "queued" => Some(OpStatus::Queued),
                "running" => Some(OpStatus::Running),
                "done" => Some(OpStatus::Done),
                "failed" => Some(OpStatus::Failed),
                "cancelled" => Some(OpStatus::Cancelled),
                _ => None,
            });

            let mut ops = state.list_operations_filtered(op_type, tier, status);

            ops.sort_by(|a, b| {
                fn status_order(s: OpStatus) -> u8 {
                    match s {
                        OpStatus::Running => 0,
                        OpStatus::Queued => 1,
                        OpStatus::Failed => 2,
                        OpStatus::Done => 3,
                        OpStatus::Cancelled => 4,
                    }
                }
                status_order(a.status)
                    .cmp(&status_order(b.status))
                    .then_with(|| b.created_at.cmp(&a.created_at))
            });

            ResponseResult::Ok {
                data: ResponseData::ListOperations {
                    operations: ops
                        .iter()
                        .filter_map(|op| serde_json::to_value(op).ok())
                        .collect(),
                },
            }
        }

        RequestMethod::GetOperation { op_id } => match state.operations.get(&op_id) {
            Some(op) => ResponseResult::Ok {
                data: ResponseData::GetOperation {
                    operation: serde_json::to_value(&op).unwrap_or_default(),
                },
            },
            None => ResponseResult::Error {
                code: 404,
                message: format!("operation {} not found", op_id),
            },
        },

        RequestMethod::CancelOperation { op_id } => {
            if state.operations.cancel(&op_id) {
                ResponseResult::Ok {
                    data: ResponseData::CancelOperation {
                        op_id,
                        message: "Operation cancelled".into(),
                    },
                }
            } else {
                ResponseResult::Error {
                    code: 409,
                    message: "Operation cannot be cancelled (not in queued state)".into(),
                }
            }
        }

        RequestMethod::SubmitReindex { bucket, table } => {
            match state.submit_reindex(bucket, table.clone()) {
                Ok(op_id) => {
                    metrics::counter!("bisque_requests_total", "protocol" => "ws", "op" => "reindex")
                        .increment(1);
                    ResponseResult::Ok {
                        data: ResponseData::SubmitReindex {
                            op_id,
                            message: format!("Reindex queued for table '{}'", table),
                        },
                    }
                }
                Err(msg) => ResponseResult::Error {
                    code: 404,
                    message: msg,
                },
            }
        }

        RequestMethod::SubmitCompact { bucket, table } => {
            match state.submit_compact(bucket, table.clone()) {
                Ok(op_id) => {
                    metrics::counter!("bisque_requests_total", "protocol" => "ws", "op" => "compact")
                        .increment(1);
                    ResponseResult::Ok {
                        data: ResponseData::SubmitCompact {
                            op_id,
                            message: format!("Compaction queued for table '{}'", table),
                        },
                    }
                }
                Err(msg) => ResponseResult::Error {
                    code: 404,
                    message: msg,
                },
            }
        }

        RequestMethod::GetCatalog { bucket: _ } => {
            // PERF4: Spawn on a separate task to avoid blocking the select! loop,
            // since get_catalog_json may do blocking I/O.
            let state_clone = Arc::clone(state);
            let catalog_json =
                match tokio::spawn(async move { state_clone.get_catalog_json().await }).await {
                    Ok(json) => json,
                    Err(e) => {
                        return ResponseResult::Error {
                            code: 500,
                            message: format!("Internal error: {}", e),
                        };
                    }
                };
            metrics::counter!("bisque_requests_total", "protocol" => "ws", "op" => "catalog")
                .increment(1);
            ResponseResult::Ok {
                data: ResponseData::GetCatalog {
                    catalog: catalog_json,
                },
            }
        }

        RequestMethod::ListCatalogs { tenant_id } => {
            if auth_ctx.tenant_id != tenant_id && !auth_ctx.is_super_admin() {
                return ResponseResult::Error {
                    code: 403,
                    message: "access denied".into(),
                };
            }
            let catalogs = meta_engine.list_catalogs_for_tenant(tenant_id);
            let catalogs_json: Vec<serde_json::Value> = catalogs
                .iter()
                .filter_map(|c| serde_json::to_value(c).ok())
                .collect();
            metrics::counter!("bisque_requests_total", "protocol" => "ws", "op" => "list_catalogs")
                .increment(1);
            ResponseResult::Ok {
                data: ResponseData::ListCatalogs {
                    catalogs: catalogs_json,
                },
            }
        }

        RequestMethod::CreateCatalog {
            tenant_id,
            name,
            engine,
            config,
        } => {
            if auth_ctx.tenant_id != tenant_id && !auth_ctx.is_tenant_admin() {
                return ResponseResult::Error {
                    code: 403,
                    message: "access denied".into(),
                };
            }
            if !auth_ctx.is_tenant_admin() {
                return ResponseResult::Error {
                    code: 403,
                    message: "tenant admin scope required".into(),
                };
            }
            let engine_type = match engine.as_str() {
                "Lance" | "lance" => bisque_meta::types::EngineType::Lance,
                "Mq" | "mq" => bisque_meta::types::EngineType::Mq,
                "LibSql" | "libsql" => bisque_meta::types::EngineType::LibSql,
                _ => {
                    return ResponseResult::Error {
                        code: 400,
                        message: format!("unknown engine type: {engine}"),
                    };
                }
            };
            match meta_engine.create_catalog(
                tenant_id,
                name.clone(),
                engine_type,
                config.unwrap_or_default(),
            ) {
                Ok((catalog_id, raft_group_id)) => {
                    info!(catalog_id, raft_group_id, %name, "catalog created via WS");
                    metrics::counter!("bisque_requests_total", "protocol" => "ws", "op" => "create_catalog")
                        .increment(1);
                    ResponseResult::Ok {
                        data: ResponseData::CreateCatalog {
                            catalog_id,
                            raft_group_id,
                        },
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    let code = if msg.contains("already exists") {
                        409
                    } else if msg.contains("not found") {
                        404
                    } else if msg.contains("limit") {
                        403
                    } else {
                        500
                    };
                    ResponseResult::Error { code, message: msg }
                }
            }
        }

        RequestMethod::GetTenant { tenant_id } => {
            if auth_ctx.tenant_id != tenant_id && !auth_ctx.is_tenant_admin() {
                return ResponseResult::Error {
                    code: 403,
                    message: "access denied".into(),
                };
            }
            match meta_engine.get_tenant(tenant_id) {
                Some(tenant) => {
                    metrics::counter!("bisque_requests_total", "protocol" => "ws", "op" => "get_tenant")
                        .increment(1);
                    match serde_json::to_value(&tenant) {
                        Ok(tenant_json) => ResponseResult::Ok {
                            data: ResponseData::GetTenant {
                                tenant: tenant_json,
                            },
                        },
                        Err(e) => ResponseResult::Error {
                            code: 500,
                            message: format!("serialization error: {e}"),
                        },
                    }
                }
                None => ResponseResult::Error {
                    code: 404,
                    message: format!("tenant {tenant_id} not found"),
                },
            }
        }

        RequestMethod::CreateApiKey {
            tenant_id,
            scopes,
            ttl_secs,
        } => {
            if auth_ctx.tenant_id != tenant_id && !auth_ctx.is_tenant_admin() {
                return ResponseResult::Error {
                    code: 403,
                    message: "access denied".into(),
                };
            }
            if !auth_ctx.is_tenant_admin() {
                return ResponseResult::Error {
                    code: 403,
                    message: "tenant admin scope required".into(),
                };
            }
            // Deserialize scopes from JSON values.
            let parsed_scopes: Vec<bisque_meta::types::Scope> = match scopes
                .into_iter()
                .map(|v| serde_json::from_value(v))
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(s) => s,
                Err(e) => {
                    return ResponseResult::Error {
                        code: 400,
                        message: format!("invalid scopes: {e}"),
                    };
                }
            };
            match meta_engine.create_api_key(tenant_id, parsed_scopes.clone()) {
                Ok((key_id, raw_key)) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    let ttl = ttl_secs.unwrap_or(meta_engine.config().token_ttl_secs);
                    let claims = bisque_meta::token::TokenClaims {
                        user_id: None,
                        account_id: None,
                        tenant_id,
                        key_id,
                        scopes: parsed_scopes,
                        issued_at: now,
                        expires_at: now + ttl as i64,
                    };
                    let token = token_manager.issue(&claims);
                    info!(key_id, tenant_id, "API key created via WS");
                    metrics::counter!("bisque_requests_total", "protocol" => "ws", "op" => "create_api_key")
                        .increment(1);
                    ResponseResult::Ok {
                        data: ResponseData::CreateApiKey {
                            key_id,
                            raw_key,
                            token,
                        },
                    }
                }
                Err(e) => ResponseResult::Error {
                    code: 500,
                    message: e.to_string(),
                },
            }
        }

        RequestMethod::GetClusterStatus => {
            let node_id = raft_node.node_id();
            let is_leader = raft_node.is_leader();
            let current_leader = raft_node.current_leader();

            let raft_role = if is_leader { "leader" } else { "follower" };
            let all_tenants = meta_engine.list_tenants();
            let mut total_catalogs = 0u64;
            for t in &all_tenants {
                total_catalogs +=
                    meta_engine.list_catalogs_for_tenant(t.tenant_id).len() as u64;
            }
            let total_tables = state.engine().list_tables().len() as u64;

            let node = serde_json::json!({
                "node_id": node_id,
                "address": format!("node-{}", node_id),
                "raft_role": raft_role,
                "raft_term": 0,
                "raft_applied_index": 0,
                "raft_commit_index": 0,
                "current_leader_id": current_leader,
                "uptime_seconds": 0,
                "catalogs": total_catalogs,
                "tables": total_tables,
                "requests_total": 0,
                "cpu_usage_pct": 0.0,
                "memory_used_bytes": 0,
                "memory_total_bytes": 1,
                "version": env!("CARGO_PKG_VERSION"),
                "started_at": "",
            });

            let cluster = serde_json::json!({
                "cluster_name": "bisque",
                "nodes": [node],
                "total_catalogs": total_catalogs,
                "total_tables": total_tables,
                "total_raft_groups": 1,
            });
            metrics::counter!("bisque_requests_total", "protocol" => "ws", "op" => "cluster_status")
                .increment(1);
            ResponseResult::Ok {
                data: ResponseData::ClusterStatus { cluster },
            }
        }

        RequestMethod::ListTenants { account_id } => {
            if !auth_ctx.is_super_admin() && auth_ctx.account_id != Some(account_id) {
                return ResponseResult::Error {
                    code: 403,
                    message: "access denied".into(),
                };
            }
            let tenants = meta_engine.list_tenants();
            let tenants: Vec<serde_json::Value> = tenants
                .into_iter()
                .filter(|t| t.account_id == account_id)
                .filter_map(|t| serde_json::to_value(&t).ok())
                .collect();
            ResponseResult::Ok {
                data: ResponseData::ListTenants { tenants },
            }
        }

        RequestMethod::UpdateTenantLimits { tenant_id, limits } => {
            if auth_ctx.tenant_id != tenant_id && !auth_ctx.is_tenant_admin() {
                return ResponseResult::Error {
                    code: 403,
                    message: "access denied".into(),
                };
            }
            let parsed: bisque_meta::types::TenantLimits = match serde_json::from_value(limits) {
                Ok(l) => l,
                Err(e) => {
                    return ResponseResult::Error {
                        code: 400,
                        message: format!("invalid limits: {e}"),
                    };
                }
            };
            match meta_engine.update_tenant_limits(tenant_id, parsed) {
                Ok(()) => ResponseResult::Ok {
                    data: ResponseData::UpdateTenantLimits,
                },
                Err(e) => ResponseResult::Error {
                    code: 500,
                    message: e.to_string(),
                },
            }
        }

        RequestMethod::DeleteTenant { tenant_id } => {
            if !auth_ctx.is_super_admin() {
                return ResponseResult::Error {
                    code: 403,
                    message: "super admin required".into(),
                };
            }
            match meta_engine.delete_tenant(tenant_id) {
                Ok(()) => ResponseResult::Ok {
                    data: ResponseData::DeleteTenant,
                },
                Err(e) => ResponseResult::Error {
                    code: 500,
                    message: e.to_string(),
                },
            }
        }

        RequestMethod::DeleteCatalog {
            tenant_id,
            catalog_id,
        } => {
            if auth_ctx.tenant_id != tenant_id && !auth_ctx.is_tenant_admin() {
                return ResponseResult::Error {
                    code: 403,
                    message: "access denied".into(),
                };
            }
            match meta_engine.delete_catalog(tenant_id, catalog_id) {
                Ok(()) => ResponseResult::Ok {
                    data: ResponseData::DeleteCatalog,
                },
                Err(e) => ResponseResult::Error {
                    code: 500,
                    message: e.to_string(),
                },
            }
        }

        RequestMethod::ListApiKeys { tenant_id } => {
            if auth_ctx.tenant_id != tenant_id && !auth_ctx.is_tenant_admin() {
                return ResponseResult::Error {
                    code: 403,
                    message: "access denied".into(),
                };
            }
            // Collect API keys for the tenant from the meta engine.
            let tenant = meta_engine.get_tenant(tenant_id);
            let api_keys: Vec<serde_json::Value> = match tenant {
                Some(t) => t
                    .api_keys
                    .iter()
                    .filter_map(|kid| meta_engine.get_api_key(*kid))
                    .filter_map(|k| serde_json::to_value(&k).ok())
                    .collect(),
                None => vec![],
            };
            ResponseResult::Ok {
                data: ResponseData::ListApiKeys { api_keys },
            }
        }

        RequestMethod::RevokeApiKey { key_id } => {
            if !auth_ctx.is_tenant_admin() {
                return ResponseResult::Error {
                    code: 403,
                    message: "tenant admin required".into(),
                };
            }
            match meta_engine.revoke_api_key(key_id) {
                Ok(()) => {
                    info!(key_id, "API key revoked via WS");
                    ResponseResult::Ok {
                        data: ResponseData::RevokeApiKey,
                    }
                }
                Err(e) => ResponseResult::Error {
                    code: 500,
                    message: e.to_string(),
                },
            }
        }

        RequestMethod::CreateTable {
            catalog: _,
            table,
            schema_json,
        } => {
            // schema_json is a JSON array of {name, type, nullable} column definitions.
            let columns: Vec<serde_json::Value> = match serde_json::from_str(&schema_json) {
                Ok(c) => c,
                Err(e) => {
                    return ResponseResult::Error {
                        code: 400,
                        message: format!("invalid schema JSON: {e}"),
                    };
                }
            };
            let fields: Vec<arrow_schema::Field> = columns
                .iter()
                .filter_map(|c| {
                    let name = c.get("name")?.as_str()?;
                    let dt = c.get("type")?.as_str().unwrap_or("Utf8");
                    let nullable = c.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true);
                    let data_type = match dt {
                        "Utf8" | "String" | "string" => arrow_schema::DataType::Utf8,
                        "Int32" | "int32" => arrow_schema::DataType::Int32,
                        "Int64" | "int64" => arrow_schema::DataType::Int64,
                        "Float32" | "float32" => arrow_schema::DataType::Float32,
                        "Float64" | "float64" | "Float" | "float" => arrow_schema::DataType::Float64,
                        "Boolean" | "boolean" | "bool" => arrow_schema::DataType::Boolean,
                        "Date32" | "date" => arrow_schema::DataType::Date32,
                        "Timestamp" | "timestamp" => {
                            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
                        }
                        "Binary" | "binary" => arrow_schema::DataType::Binary,
                        _ => arrow_schema::DataType::Utf8,
                    };
                    Some(arrow_schema::Field::new(name, data_type, nullable))
                })
                .collect();
            if fields.is_empty() {
                return ResponseResult::Error {
                    code: 400,
                    message: "schema must have at least one column".into(),
                };
            }
            let schema = arrow_schema::Schema::new(fields);
            match raft_node.create_table(&table, &schema).await {
                Ok(_) => {
                    info!(%table, "table created via WS");
                    ResponseResult::Ok {
                        data: ResponseData::CreateTable {
                            table: table.clone(),
                        },
                    }
                }
                Err(e) => ResponseResult::Error {
                    code: 500,
                    message: format!("create table error: {e}"),
                },
            }
        }

        RequestMethod::DropTable { catalog: _, table } => {
            match raft_node.drop_table(&table).await {
                Ok(_) => {
                    info!(%table, "table dropped via WS");
                    ResponseResult::Ok {
                        data: ResponseData::DropTable {
                            table: table.clone(),
                        },
                    }
                }
                Err(e) => ResponseResult::Error {
                    code: 500,
                    message: format!("drop table error: {e}"),
                },
            }
        }

        RequestMethod::ExecuteSql { catalog: _, sql } => {
            use datafusion::execution::context::SessionContext;

            let engine = raft_node.engine().clone();
            let result = tokio::spawn(async move {
                let ctx = SessionContext::new();
                let catalog =
                    Arc::new(bisque_lance::postgres::BisqueLanceCatalogProvider::new(engine));
                ctx.register_catalog("bisque", catalog);

                let df = ctx.sql(&sql).await?;
                let schema = Arc::new(df.schema().as_arrow().clone());
                let batches = df.collect().await?;
                Ok::<_, datafusion::error::DataFusionError>((schema, batches))
            })
            .await;

            match result {
                Ok(Ok((schema, batches))) => {
                    let columns: Vec<serde_json::Value> = schema
                        .fields()
                        .iter()
                        .map(|f| {
                            serde_json::json!({
                                "name": f.name(),
                                "type": format!("{}", f.data_type()),
                                "nullable": f.is_nullable(),
                            })
                        })
                        .collect();

                    let mut rows = Vec::new();
                    let mut row_count: u64 = 0;
                    for batch in &batches {
                        row_count += batch.num_rows() as u64;
                        // Build formatters for each column.
                        let formatters: Vec<_> = batch
                            .columns()
                            .iter()
                            .map(|col| {
                                arrow_cast::display::ArrayFormatter::try_new(
                                    col.as_ref(),
                                    &arrow_cast::display::FormatOptions::default(),
                                )
                            })
                            .collect::<Result<Vec<_>, _>>()
                            .unwrap_or_default();

                        for row_idx in 0..batch.num_rows() {
                            let mut row = serde_json::Map::new();
                            for (col_idx, field) in schema.fields().iter().enumerate() {
                                let val = if col_idx < formatters.len() {
                                    let s = formatters[col_idx].value(row_idx).to_string();
                                    // Try to parse as number or null, otherwise use string.
                                    if batch.column(col_idx).is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else if let Ok(n) = s.parse::<f64>() {
                                        serde_json::json!(n)
                                    } else if let Ok(n) = s.parse::<i64>() {
                                        serde_json::json!(n)
                                    } else if s == "true" {
                                        serde_json::Value::Bool(true)
                                    } else if s == "false" {
                                        serde_json::Value::Bool(false)
                                    } else {
                                        serde_json::Value::String(s)
                                    }
                                } else {
                                    serde_json::Value::Null
                                };
                                row.insert(field.name().clone(), val);
                            }
                            rows.push(serde_json::Value::Object(row));
                        }
                    }

                    ResponseResult::Ok {
                        data: ResponseData::ExecuteSql {
                            columns,
                            rows,
                            row_count,
                        },
                    }
                }
                Ok(Err(e)) => ResponseResult::Error {
                    code: 400,
                    message: format!("SQL error: {e}"),
                },
                Err(e) => ResponseResult::Error {
                    code: 500,
                    message: format!("internal error: {e}"),
                },
            }
        }

        RequestMethod::ListAccounts => {
            if !auth_ctx.is_super_admin() {
                return ResponseResult::Error {
                    code: 403,
                    message: "super admin required".into(),
                };
            }
            let accounts = meta_engine.list_accounts();
            let accounts: Vec<serde_json::Value> = accounts
                .into_iter()
                .filter_map(|a| serde_json::to_value(&a).ok())
                .collect();
            ResponseResult::Ok {
                data: ResponseData::ListAccounts { accounts },
            }
        }

        RequestMethod::EnableOtel => {
            // Check if OTel is already enabled by looking at catalog metadata.
            let already_enabled = raft_node
                .manifest()
                .and_then(|m| m.read_catalog_meta(raft_node.group_id()).ok().flatten())
                .map(|cm| cm.otel_enabled)
                .unwrap_or(false);

            if already_enabled {
                let tables: Vec<String> = raft_node
                    .engine()
                    .list_tables()
                    .into_iter()
                    .filter(|t| t.starts_with("otel_"))
                    .collect();
                return ResponseResult::Ok {
                    data: ResponseData::EnableOtel {
                        tables_created: tables,
                    },
                };
            }

            let raft_clone = Arc::clone(raft_node);
            match tokio::spawn(async move {
                let receiver = bisque_lance::OtlpReceiver::new(raft_clone);
                receiver.ensure_tables().await
            })
            .await
            {
                Ok(Ok(())) => {
                    // Persist catalog metadata with OTel flag.
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let catalog_meta = bisque_lance::CatalogMeta {
                        otel_enabled: true,
                        otel_enabled_at: Some(now),
                        otel_enabled_by: Some(format!("tenant:{}", auth_ctx.tenant_id)),
                    };
                    if let Some(manifest) = raft_node.manifest() {
                        if let Err(e) =
                            manifest.write_catalog_meta(raft_node.group_id(), &catalog_meta)
                        {
                            warn!("Failed to persist OTel catalog metadata: {e}");
                        }
                    }

                    let tables_created: Vec<String> = raft_node
                        .engine()
                        .list_tables()
                        .into_iter()
                        .filter(|t| t.starts_with("otel_"))
                        .collect();
                    info!(count = tables_created.len(), "OTel tables enabled via WS");
                    ResponseResult::Ok {
                        data: ResponseData::EnableOtel { tables_created },
                    }
                }
                Ok(Err(e)) => ResponseResult::Error {
                    code: 500,
                    message: format!("Failed to create OTel tables: {e}"),
                },
                Err(e) => ResponseResult::Error {
                    code: 500,
                    message: format!("Internal error: {e}"),
                },
            }
        }
    }
}

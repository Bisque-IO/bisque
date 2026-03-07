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
    /// M5/L5: Per-IP connection tracking for rate limiting.
    pub ip_connections: Arc<dashmap::DashMap<std::net::IpAddr, IpConnectionState>>,
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

/// Minimum interval between expensive requests like ListOperations/GetCatalog (1 second).
const WS_REQUEST_COOLDOWN: Duration = Duration::from_secs(1);

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
                metrics::counter!("ws_ip_rate_limited").increment(1);
                return (StatusCode::TOO_MANY_REQUESTS, "Too many connections").into_response();
            }
            if state.auth_failures >= WS_MAX_AUTH_FAILURES_PER_IP
                && state.last_auth_failure.elapsed() < WS_AUTH_FAILURE_BLOCK_DURATION
            {
                warn!(%client_ip, "WS connection rejected: too many auth failures");
                metrics::counter!("ws_auth_blocked").increment(1);
                return (StatusCode::TOO_MANY_REQUESTS, "Too many auth failures").into_response();
            }
        }
    }
    // S3: Reject requests with a mismatched Origin header.
    // Allow requests with no Origin (non-browser clients) or same-origin.
    if let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) {
        if let Some(host) = headers.get("host").and_then(|v| v.to_str().ok()) {
            let origin_host = origin
                .strip_prefix("https://")
                .or_else(|| origin.strip_prefix("http://"))
                .unwrap_or(origin);
            if origin_host != host {
                warn!(origin, host, "WS connection rejected: origin mismatch");
                metrics::counter!("ws_origin_rejected").increment(1);
                return (StatusCode::FORBIDDEN, "Origin not allowed").into_response();
            }
        }
    }
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
        metrics::gauge!("ws_connections_active").increment(1.0);

        if let Err(e) = handle_unified_ws(
            ws,
            state,
            auth_state,
            pins.clone(),
            session_id,
            client_ip,
            &ip_connections,
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
        metrics::gauge!("ws_connections_active").decrement(1.0);
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
    pins: Arc<bisque_lance::version_pins::VersionPinTracker>,
    session_id: u64,
    client_ip: std::net::IpAddr,
    ip_connections: &dashmap::DashMap<std::net::IpAddr, IpConnectionState>,
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
                    metrics::counter!("ws_auth_failures").increment(1);
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
    metrics::counter!("ws_handshakes_total").increment(1);
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
                        if !catalog_filter.is_empty() && !catalog_filter.contains(&event.catalog) {
                            continue;
                        }
                        let catalog = event.catalog.clone();
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
                        if !catalog_filter.is_empty() && !catalog_filter.contains(&event.catalog) {
                            continue;
                        }
                        // E2: Skip events that fail to serialize instead of sending null.
                        let catalog = event.catalog.clone();
                        match serde_json::to_value(&event.event) {
                            Ok(event_val) => {
                                let msg = ServerMessage::CatalogEvent {
                                    seq: next_seq(),
                                    catalog,
                                    event: event_val,
                                };
                                ws_write_frame(&mut ws, rmp_serde::to_vec_named(&msg)?).await?;
                                msgs_sent += 1;
                                metrics::counter!("ws_messages_sent", "type" => "catalog_event").increment(1);
                            }
                            Err(e) => {
                                warn!(session_id, error = %e, "Failed to serialize catalog event, skipping");
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(session_id, lagged = n, "Unified WS catalog subscriber lagged");
                        metrics::counter!("ws_subscriber_lag", "type" => "catalog").increment(n);
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
                                metrics::counter!("ws_messages_sent", "type" => "operation_update").increment(1);
                            }
                            Err(e) => {
                                warn!(session_id, error = %e, "Failed to serialize operation update, skipping");
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(session_id, lagged = n, "Unified WS operations subscriber lagged");
                        metrics::counter!("ws_subscriber_lag", "type" => "operations").increment(n);
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
                        metrics::counter!("ws_read_timeouts").increment(1);
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
                                metrics::counter!("ws_decode_errors").increment(1);
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
                                    handle_ws_request(&state, method),
                                )
                                .await
                                {
                                    Ok(result) => result,
                                    Err(_) => {
                                        warn!(session_id, request_id, "WS request timed out (30s)");
                                        metrics::counter!("ws_request_timeouts").increment(1);
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
                                    pins.pin(session_id, PinKey { catalog, table, tier, version });
                                }
                            }
                            ClientMessage::Unpin { catalog, table, tier, version } => {
                                if let Some(tier) = PinTier::from_str(&tier) {
                                    pins.unpin(session_id, PinKey { catalog, table, tier, version });
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
    metrics::histogram!("ws_connection_duration_seconds").record(duration_secs);
    metrics::counter!("ws_messages_sent_total").increment(msgs_sent);
    metrics::counter!("ws_messages_recv_total").increment(msgs_recv);
    info!(
        session_id,
        msgs_sent, msgs_recv, duration_secs, "Unified WS connection closed"
    );
    Ok(())
}

/// Route a WebSocket request to the appropriate handler logic.
async fn handle_ws_request(state: &Arc<S3ServerState>, method: RequestMethod) -> ResponseResult {
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

        // These methods delegate to the meta engine which will be wired up
        // when the unified server passes the meta engine reference.
        RequestMethod::ListCatalogs { .. }
        | RequestMethod::CreateCatalog { .. }
        | RequestMethod::GetTenant { .. }
        | RequestMethod::CreateApiKey { .. }
        | RequestMethod::GetClusterStatus => ResponseResult::Error {
            code: 501,
            message: "Not yet implemented over WebSocket — use HTTP API".into(),
        },
    }
}

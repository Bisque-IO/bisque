//! Unified bisque server composing meta control plane and engine APIs.
//!
//! Builds a single axum HTTP server that mounts:
//! - Management API at `/_bisque/v1/...` for tenant/catalog/key CRUD
//! - Lance S3-compatible API at `/{bucket}/{key}` for data access
//! - Unified WebSocket at `/_bisque/ws` for real-time push + RPC
//!
//! gRPC services (Flight SQL) run on a separate port.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use bisque_lance::{
    BisqueLance, BisqueLanceConfig, CatalogEventBus, LanceManifestManager, LanceRaftNode,
    LanceStateMachine, OtlpReceiver, PostgresServerConfig, S3ServerState, WriteBatcherConfig,
    flight::serve_flight, otel_http_router, s3_router, serve_otlp, serve_postgres,
};
use bisque_meta::MetaConfig;
use bisque_meta::engine::MetaEngine;
use bisque_meta::token::{self, TokenClaims, TokenManager};
use bisque_meta::types::{Account, CatalogEntry, EngineType, Scope, Tenant, TenantLimits};
use bisque_raft::{
    BisqueTcpTransport, BisqueTcpTransportConfig, DefaultNodeRegistry, MmapStorageConfig,
    MultiRaftManager, MultiplexedLogStorage, NodeAddressResolver,
};

use crate::auth::{AuthContext, AuthState, auth_middleware};
use crate::config::BisqueConfig;
use crate::error::ApiError;
use crate::ws::{WsMetrics, WsState, unified_ws_handler};

/// Shared application state for the unified server.
#[derive(Clone)]
pub struct AppState {
    pub meta_engine: Arc<MetaEngine>,
    pub token_manager: Arc<TokenManager>,
}

type LanceManager = MultiRaftManager<
    bisque_lance::LanceTypeConfig,
    BisqueTcpTransport<bisque_lance::LanceTypeConfig>,
    MultiplexedLogStorage<bisque_lance::LanceTypeConfig>,
>;

/// Handle to a running bisque server, returned by [`start()`].
///
/// Keeps all server resources alive. Call [`shutdown()`](ServerHandle::shutdown)
/// for graceful teardown, or simply drop to stop.
pub struct ServerHandle {
    /// Actual HTTP listen address (resolved from `:0` if used).
    pub http_addr: SocketAddr,
    /// Meta engine for direct access in tests.
    pub meta_engine: Arc<MetaEngine>,
    /// Token manager for issuing/verifying tokens in tests.
    pub token_manager: Arc<TokenManager>,
    /// Raft node for direct access.
    pub raft_node: Arc<LanceRaftNode>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    join_handle: Option<tokio::task::JoinHandle<()>>,
    manager: Arc<LanceManager>,
}

impl ServerHandle {
    /// Gracefully shut down the server and all background services.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.await;
        }
        self.raft_node.shutdown().await;
        self.manager.storage().stop();
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Start the unified bisque server and return a handle for programmatic control.
///
/// Use `127.0.0.1:0` for ports to let the OS assign available ones.
/// The actual bound addresses are available via the returned [`ServerHandle`].
pub async fn start(config: BisqueConfig) -> Result<ServerHandle, Box<dyn std::error::Error>> {
    start_inner(config).await
}

/// Build the unified bisque HTTP server and start listening.
pub async fn run(config: BisqueConfig) -> Result<(), Box<dyn std::error::Error>> {
    let handle = start(config).await?;
    shutdown_signal().await;
    handle.shutdown().await;
    Ok(())
}

async fn start_inner(config: BisqueConfig) -> Result<ServerHandle, Box<dyn std::error::Error>> {
    // -----------------------------------------------------------------------
    // 1. Meta engine (control plane)
    // -----------------------------------------------------------------------
    let meta_config =
        MetaConfig::new(config.token_secret.clone()).with_token_ttl_secs(config.token_ttl_secs);

    let meta_engine = Arc::new(MetaEngine::new(meta_config));
    let token_manager = Arc::new(TokenManager::new(config.token_secret.clone()));

    let app_state = AppState {
        meta_engine: meta_engine.clone(),
        token_manager: token_manager.clone(),
    };

    let auth_state = AuthState {
        token_manager: token_manager.clone(),
        meta_engine: meta_engine.clone(),
    };

    // -----------------------------------------------------------------------
    // 2. Lance engine (analytics storage)
    // -----------------------------------------------------------------------
    let lance_dir = config.data_dir.join("lance");
    let raft_dir = config.data_dir.join("raft-data");
    let group_id: u64 = 1;
    let node_id = config.node_id;

    // 2a. Open the BisqueLance multi-table storage engine.
    let lance_config = BisqueLanceConfig::new(&lance_dir);
    let catalog_name = "bisque".to_string();
    let engine =
        Arc::new(BisqueLance::open_with_catalog(lance_config, catalog_name.clone()).await?);
    info!(data_dir = %lance_dir.display(), "lance engine opened");

    // 2b. Set up Raft log storage (memory-mapped).
    std::fs::create_dir_all(&raft_dir)?;
    let storage_config = MmapStorageConfig::new(&raft_dir).with_segment_size(8 * 1024 * 1024);
    let storage = MultiplexedLogStorage::new(storage_config).await?;
    info!(raft_dir = %raft_dir.display(), "raft log storage initialized");

    // 2c. Set up Raft transport (TCP for clustering, single-node for now).
    // M13: Register with actual HTTP addr for multi-node compatibility.
    let registry = Arc::new(DefaultNodeRegistry::new());
    registry.register(node_id, config.http_addr);
    let transport = BisqueTcpTransport::new(BisqueTcpTransportConfig::default(), registry);

    // 2d. Create the multi-raft manager.
    let manager: Arc<LanceManager> = Arc::new(MultiRaftManager::new(transport, storage));

    // 2e. MDBX manifest for crash-consistent catalog metadata and WAL.
    let manifest_dir = config.data_dir.join("manifest");
    std::fs::create_dir_all(&manifest_dir)?;
    let manifest = Arc::new(LanceManifestManager::new(&manifest_dir)?);
    manifest.open_group(group_id)?;
    info!(manifest_dir = %manifest_dir.display(), group_id, "MDBX manifest opened");

    // 2f. Catalog event bus — seed from WAL's latest seq so post-restart events
    // continue the monotonic sequence and reconnecting clients can delta-sync.
    let initial_seq = manifest.latest_wal_seq(group_id).unwrap_or(0);
    let catalog_bus = Arc::new(CatalogEventBus::new(initial_seq));
    info!(initial_seq, "catalog event bus initialized from WAL");

    // 2g. Create the Raft state machine with catalog events, manifest, and WAL.
    let state_machine = LanceStateMachine::new(engine.clone())
        .with_catalog_events(catalog_bus.clone())
        .with_catalog_name("bisque".to_string())
        .with_manifest(manifest.clone(), group_id);

    // 2h. Create the Raft group and initialize single-node membership.
    let raft_config = Arc::new(
        openraft::Config {
            heartbeat_interval: 200,
            election_timeout_min: 400,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()?,
    );

    let raft = manager
        .add_group(group_id, node_id, raft_config, state_machine)
        .await?;

    let mut members = BTreeMap::new();
    members.insert(node_id, BasicNode::default());
    match raft.initialize(members).await {
        Ok(_) => info!(node_id, group_id, "raft group initialized"),
        Err(e) => {
            // Already initialized is fine (e.g., restart with existing data).
            if raft.is_initialized().await.unwrap_or(false) {
                info!(node_id, group_id, "raft group already initialized");
            } else {
                return Err(Box::new(e));
            }
        }
    }

    // 2i. Create the LanceRaftNode and start background tasks.
    let raft_node = Arc::new(
        LanceRaftNode::new(raft, engine.clone(), node_id)
            .with_group_id(group_id)
            .with_catalog_name(catalog_name.clone())
            .with_write_batcher(WriteBatcherConfig::default()),
    );
    raft_node.start();

    // H3: Poll for leadership instead of a fixed sleep.
    // Election timeout is 150-600ms, so poll up to 2s with 50ms intervals.
    {
        let mut elected = false;
        for _ in 0..40 {
            if raft_node.is_leader() {
                elected = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        if elected {
            info!(node_id, "raft node is leader");
        } else {
            warn!(
                node_id,
                "raft node is NOT leader after 2s — writes may fail until election completes"
            );
        }
    }

    // 2j. Build the S3ServerState (used by S3 router and WS handler).
    let s3_state = S3ServerState::new(
        engine.clone(),
        Some(catalog_bus),
        Some(manifest),
        group_id,
        catalog_name,
        None, // ClusterMesh — wire up for multi-node
    );

    // -----------------------------------------------------------------------
    // 3. Build HTTP routes
    // -----------------------------------------------------------------------

    // Public routes (no auth required)
    let public_routes = axum::Router::new()
        .route("/auth/login", post(login))
        .with_state(app_state.clone());

    // Management API routes (require auth)
    let management_routes = axum::Router::new()
        .route("/accounts", post(create_account).get(list_accounts))
        .route("/accounts/{account_id}", get(get_account))
        .route("/tenants", post(create_tenant))
        .route("/tenants/{tenant_id}", get(get_tenant))
        .route(
            "/tenants/{tenant_id}/catalogs",
            post(create_catalog).get(list_catalogs),
        )
        .route("/tenants/{tenant_id}/api-keys", post(create_api_key))
        .route("/api-keys/{key_id}", delete(revoke_api_key))
        .layer(axum::middleware::from_fn(auth_middleware))
        .layer(Extension(auth_state.clone()))
        .with_state(app_state.clone());

    // Health check (no auth)
    let health_route = axum::Router::new().route("/_bisque/health", get(health_check));

    // Unified WebSocket (auth via handshake frame)
    let ws_state = WsState {
        s3: s3_state.clone(),
        auth: auth_state.clone(),
        meta_engine: meta_engine.clone(),
        token_manager: token_manager.clone(),
        raft_node: raft_node.clone(),
        ip_connections: Arc::new(dashmap::DashMap::new()),
        metrics: Arc::new(WsMetrics::new()),
    };
    let ws_route =
        axum::Router::new().route("/_bisque/ws", get(unified_ws_handler).with_state(ws_state));

    // Lance S3-compatible API routes
    let s3_routes = s3_router(s3_state);

    // OTLP HTTP routes (Tempo, Prometheus/Mimir, Loki, OTLP HTTP ingest)
    let otel_receiver = Arc::new(OtlpReceiver::new(raft_node.clone()));
    otel_receiver.ensure_tables().await?;
    let otel_routes = otel_http_router(raft_node.clone(), otel_receiver);

    // Compose: specific paths first, then OTLP HTTP, then S3 (wildcard catch-all last)
    let mut app = axum::Router::new()
        .nest("/_bisque/v1", public_routes)
        .nest("/_bisque/v1", management_routes)
        .merge(health_route)
        .merge(ws_route)
        .merge(otel_routes)
        .merge(s3_routes);

    // Serve static UI files if ui_dir is configured
    if let Some(ref ui_dir) = config.ui_dir {
        let serve_dir = tower_http::services::ServeDir::new(ui_dir).not_found_service(
            tower_http::services::ServeFile::new(ui_dir.join("index.html")),
        );
        app = app.fallback_service(serve_dir);
        info!(ui_dir = %ui_dir.display(), "serving static UI files");
    }

    // -----------------------------------------------------------------------
    // 4. Start gRPC services on separate ports
    // -----------------------------------------------------------------------

    // Flight SQL gRPC
    let flight_addr = config.flight_addr;
    let flight_node = raft_node.clone();
    tokio::spawn(async move {
        info!(%flight_addr, "starting Flight SQL gRPC server");
        if let Err(e) = serve_flight(flight_node, flight_addr).await {
            tracing::error!(error = %e, "Flight SQL server failed");
        }
    });

    // OTLP gRPC (metrics, traces, logs)
    let otlp_addr = config.otlp_grpc_addr;
    let otlp_node = raft_node.clone();
    tokio::spawn(async move {
        info!(%otlp_addr, "starting OTLP gRPC server");
        if let Err(e) = serve_otlp(otlp_node, otlp_addr).await {
            tracing::error!(error = %e, "OTLP gRPC server failed");
        }
    });

    // PostgreSQL wire protocol (optional)
    if let Some(pg_addr) = config.postgres_addr {
        let pg_node = raft_node.clone();
        tokio::spawn(async move {
            let pg_config = PostgresServerConfig {
                host: pg_addr.ip().to_string(),
                port: pg_addr.port(),
            };
            info!(%pg_addr, "starting PostgreSQL wire protocol server");
            if let Err(e) = serve_postgres(pg_node, pg_config).await {
                tracing::error!(error = %e, "PostgreSQL server failed");
            }
        });
    }

    // -----------------------------------------------------------------------
    // 5. Start unified HTTP server with graceful shutdown
    // -----------------------------------------------------------------------
    let listener = {
        let addr = config.http_addr;
        let domain = if addr.is_ipv6() {
            socket2::Domain::IPV6
        } else {
            socket2::Domain::IPV4
        };
        let socket =
            socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
        socket.set_reuse_address(true)?;
        socket.set_tcp_nodelay(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;
        socket.listen(1024)?;
        tokio::net::TcpListener::from_std(std::net::TcpListener::from(socket))?
    };
    let http_addr = listener.local_addr()?;
    info!(addr = %http_addr, "starting bisque unified HTTP server");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let join_handle = tokio::spawn(async move {
        let shutdown_fut = async {
            let _ = shutdown_rx.await;
        };
        if let Err(e) = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_fut)
        .await
        {
            tracing::error!(error = %e, "HTTP server error");
        }
    });

    Ok(ServerHandle {
        http_addr,
        meta_engine,
        token_manager,
        raft_node,
        shutdown_tx: Some(shutdown_tx),
        join_handle: Some(join_handle),
        manager,
    })
}

/// Wait for SIGINT (Ctrl+C) or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => info!("received SIGINT"),
            _ = sigterm.recv() => info!("received SIGTERM"),
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
        info!("received SIGINT");
    }
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

// ---------------------------------------------------------------------------
// Request/Response types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct LoginResponse {
    user_id: u64,
    token: String,
    accounts: Vec<Account>,
}

#[derive(Deserialize)]
struct CreateAccountRequest {
    name: String,
}

#[derive(Serialize)]
struct CreateAccountResponse {
    account_id: u64,
}

#[derive(Deserialize)]
struct CreateTenantRequest {
    account_id: u64,
    name: String,
    #[serde(default)]
    limits: Option<TenantLimits>,
}

#[derive(Serialize)]
struct CreateTenantResponse {
    tenant_id: u64,
}

#[derive(Deserialize)]
struct CreateCatalogRequest {
    name: String,
    engine: EngineType,
    #[serde(default)]
    config: String,
}

#[derive(Serialize)]
struct CreateCatalogResponse {
    catalog_id: u64,
    raft_group_id: u64,
}

#[derive(Deserialize)]
struct CreateApiKeyRequest {
    scopes: Vec<Scope>,
    #[serde(default)]
    ttl_secs: Option<u64>,
}

#[derive(Serialize)]
struct CreateApiKeyResponse {
    key_id: u64,
    /// The raw API key. Only returned at creation time — store securely.
    raw_key: String,
    /// A signed bearer token valid for `token_ttl_secs`.
    token: String,
}

// ---------------------------------------------------------------------------
// Handlers — Auth (public)
// ---------------------------------------------------------------------------

async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Result<Response, ApiError> {
    let user = state
        .meta_engine
        .find_user_by_username(&req.username)
        .ok_or_else(|| ApiError::Unauthorized("invalid credentials".into()))?;

    if user.disabled {
        return Err(ApiError::Unauthorized("account disabled".into()));
    }

    if !token::verify_password(&user.password_hash, req.password.as_bytes()) {
        return Err(ApiError::Unauthorized("invalid credentials".into()));
    }

    // Gather account memberships
    let memberships = state.meta_engine.get_user_memberships(user.user_id);
    let accounts: Vec<Account> = memberships
        .iter()
        .filter_map(|m| state.meta_engine.get_account(m.account_id))
        .collect();

    // Build scopes from memberships.
    // C1: Members get CatalogRead("*") — NOT AccountAdmin.
    let scopes: Vec<Scope> = memberships
        .iter()
        .map(|m| match m.role {
            bisque_meta::types::AccountRole::Admin => Scope::AccountAdmin(m.account_id),
            bisque_meta::types::AccountRole::Member => Scope::CatalogRead("*".into()),
        })
        .collect();

    let first_account_id = accounts.first().map(|a| a.account_id);
    let now = chrono::Utc::now().timestamp();
    let ttl = state.meta_engine.config().token_ttl_secs;

    let claims = TokenClaims {
        user_id: Some(user.user_id),
        account_id: first_account_id,
        tenant_id: 0,
        key_id: 0,
        scopes,
        issued_at: now,
        expires_at: now + ttl as i64,
    };
    let jwt = state.token_manager.issue(&claims);

    info!(user_id = user.user_id, username = %req.username, "user logged in");
    Ok((
        StatusCode::OK,
        Json(LoginResponse {
            user_id: user.user_id,
            token: jwt,
            accounts,
        }),
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// Handlers — Accounts
// ---------------------------------------------------------------------------

async fn create_account(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<CreateAccountRequest>,
) -> Result<Response, ApiError> {
    if !auth.is_super_admin() {
        return Err(ApiError::Forbidden("super admin scope required".into()));
    }

    let account_id = state
        .meta_engine
        .create_account(req.name.clone())
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    info!(account_id, name = %req.name, "account created");
    Ok((
        StatusCode::CREATED,
        Json(CreateAccountResponse { account_id }),
    )
        .into_response())
}

async fn list_accounts(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<Account>>, ApiError> {
    if auth.is_super_admin() {
        return Ok(Json(state.meta_engine.list_accounts()));
    }

    // Non-super admins see only their own accounts
    let memberships = match auth.user_id {
        Some(uid) => state.meta_engine.get_user_memberships(uid),
        None => vec![],
    };

    let accounts: Vec<Account> = memberships
        .iter()
        .filter_map(|m| state.meta_engine.get_account(m.account_id))
        .collect();

    Ok(Json(accounts))
}

async fn get_account(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    AxumPath(account_id): AxumPath<u64>,
) -> Result<Json<Account>, ApiError> {
    if !auth.is_super_admin() && !auth.is_account_admin(account_id) {
        return Err(ApiError::Forbidden("access denied".into()));
    }

    state
        .meta_engine
        .get_account(account_id)
        .ok_or_else(|| ApiError::NotFound(format!("account {account_id} not found")))
        .map(Json)
}

// ---------------------------------------------------------------------------
// Handlers — Tenants
// ---------------------------------------------------------------------------

async fn create_tenant(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<CreateTenantRequest>,
) -> Result<Response, ApiError> {
    // Only tenant admins (or bootstrap) can create tenants
    if !auth.is_tenant_admin() {
        return Err(ApiError::Forbidden("tenant admin scope required".into()));
    }

    let limits = req.limits.unwrap_or_default();
    let result = state
        .meta_engine
        .create_tenant(req.account_id, req.name.clone(), limits);
    match result {
        Ok(tenant_id) => {
            info!(tenant_id, name = %req.name, "tenant created");
            Ok((
                StatusCode::CREATED,
                Json(CreateTenantResponse { tenant_id }),
            )
                .into_response())
        }
        Err(e) => {
            if e.to_string().contains("already exists") {
                Err(ApiError::Conflict(e.to_string()))
            } else {
                Err(ApiError::Internal(e.to_string()))
            }
        }
    }
}

async fn get_tenant(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    AxumPath(tenant_id): AxumPath<u64>,
) -> Result<Json<Tenant>, ApiError> {
    // Must be requesting own tenant or be an admin
    if auth.tenant_id != tenant_id && !auth.is_tenant_admin() {
        return Err(ApiError::Forbidden("access denied".into()));
    }

    state
        .meta_engine
        .get_tenant(tenant_id)
        .ok_or_else(|| ApiError::NotFound(format!("tenant {tenant_id} not found")))
        .map(Json)
}

async fn create_catalog(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    AxumPath(tenant_id): AxumPath<u64>,
    Json(req): Json<CreateCatalogRequest>,
) -> Result<Response, ApiError> {
    if auth.tenant_id != tenant_id && !auth.is_tenant_admin() {
        return Err(ApiError::Forbidden("access denied".into()));
    }
    if !auth.is_tenant_admin() {
        return Err(ApiError::Forbidden("tenant admin scope required".into()));
    }

    let result = state.meta_engine.create_catalog(
        tenant_id,
        req.name.clone(),
        req.engine,
        req.config.clone(),
    );

    match result {
        Ok((catalog_id, raft_group_id)) => {
            info!(catalog_id, raft_group_id, name = %req.name, "catalog created");
            Ok((
                StatusCode::CREATED,
                Json(CreateCatalogResponse {
                    catalog_id,
                    raft_group_id,
                }),
            )
                .into_response())
        }
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("already exists") {
                Err(ApiError::Conflict(msg))
            } else if msg.contains("not found") {
                Err(ApiError::NotFound(msg))
            } else if msg.contains("limit") {
                Err(ApiError::Forbidden(msg))
            } else {
                Err(ApiError::Internal(msg))
            }
        }
    }
}

async fn list_catalogs(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    AxumPath(tenant_id): AxumPath<u64>,
) -> Result<Json<Vec<CatalogEntry>>, ApiError> {
    if auth.tenant_id != tenant_id && !auth.is_tenant_admin() {
        return Err(ApiError::Forbidden("access denied".into()));
    }

    let catalogs = state.meta_engine.list_catalogs_for_tenant(tenant_id);
    Ok(Json(catalogs))
}

async fn create_api_key(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    AxumPath(tenant_id): AxumPath<u64>,
    Json(req): Json<CreateApiKeyRequest>,
) -> Result<Response, ApiError> {
    if auth.tenant_id != tenant_id && !auth.is_tenant_admin() {
        return Err(ApiError::Forbidden("access denied".into()));
    }
    if !auth.is_tenant_admin() {
        return Err(ApiError::Forbidden("tenant admin scope required".into()));
    }

    let result = state
        .meta_engine
        .create_api_key(tenant_id, req.scopes.clone());
    match result {
        Ok((key_id, raw_key)) => {
            // Issue a bearer token for convenience
            let now = chrono::Utc::now().timestamp();
            let ttl = req
                .ttl_secs
                .unwrap_or(state.meta_engine.config().token_ttl_secs);
            let claims = TokenClaims {
                user_id: None,
                account_id: None,
                tenant_id,
                key_id,
                scopes: req.scopes,
                issued_at: now,
                expires_at: now + ttl as i64,
            };
            let token = state.token_manager.issue(&claims);

            info!(key_id, tenant_id, "API key created");
            Ok((
                StatusCode::CREATED,
                Json(CreateApiKeyResponse {
                    key_id,
                    raw_key,
                    token,
                }),
            )
                .into_response())
        }
        Err(e) => Err(ApiError::Internal(e.to_string())),
    }
}

async fn revoke_api_key(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    AxumPath(key_id): AxumPath<u64>,
) -> Result<Response, ApiError> {
    // Verify the key belongs to the caller's tenant
    let api_key = state
        .meta_engine
        .get_api_key(key_id)
        .ok_or_else(|| ApiError::NotFound(format!("API key {key_id} not found")))?;

    if api_key.tenant_id != auth.tenant_id && !auth.is_tenant_admin() {
        return Err(ApiError::Forbidden("access denied".into()));
    }

    state
        .meta_engine
        .revoke_api_key(key_id)
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    info!(key_id, "API key revoked");
    Ok(StatusCode::NO_CONTENT.into_response())
}

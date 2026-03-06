//! Unified bisque server composing meta control plane and engine APIs.
//!
//! Builds a single axum HTTP server that mounts:
//! - Management API at `/_bisque/v1/...` for tenant/catalog/key CRUD
//! - Engine-specific routes (S3, OTLP HTTP) with auth middleware
//!
//! gRPC services (Flight SQL, OTLP gRPC) run on separate ports.

use std::sync::Arc;

use axum::extract::{Extension, Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::info;

use bisque_meta::engine::MetaEngine;
use bisque_meta::token::{self, TokenClaims, TokenManager};
use bisque_meta::types::{
    Account, CatalogEntry, EngineType, Scope, Tenant, TenantLimits,
};
use bisque_meta::MetaConfig;

use crate::auth::{AuthContext, AuthState, auth_middleware};
use crate::config::BisqueConfig;
use crate::error::ApiError;

/// Shared application state for the unified server.
#[derive(Clone)]
pub struct AppState {
    pub meta_engine: Arc<MetaEngine>,
    pub token_manager: Arc<TokenManager>,
}

/// Build the unified bisque HTTP server and start listening.
pub async fn run(config: BisqueConfig) -> Result<(), Box<dyn std::error::Error>> {
    let meta_config = MetaConfig::new(config.token_secret.clone())
        .with_token_ttl_secs(config.token_ttl_secs);

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
        .route("/tenants/{tenant_id}/catalogs", post(create_catalog).get(list_catalogs))
        .route("/tenants/{tenant_id}/api-keys", post(create_api_key))
        .route("/api-keys/{key_id}", delete(revoke_api_key))
        .layer(axum::middleware::from_fn(auth_middleware))
        .layer(Extension(auth_state.clone()))
        .with_state(app_state.clone());

    // Health check (no auth)
    let health_route = axum::Router::new()
        .route("/_bisque/health", get(health_check));

    let mut app = axum::Router::new()
        .nest("/_bisque/v1", public_routes)
        .nest("/_bisque/v1", management_routes)
        .merge(health_route);

    // Serve static UI files if ui_dir is configured
    if let Some(ref ui_dir) = config.ui_dir {
        let serve_dir = tower_http::services::ServeDir::new(ui_dir)
            .not_found_service(tower_http::services::ServeFile::new(
                ui_dir.join("index.html"),
            ));
        app = app.fallback_service(serve_dir);
        info!(ui_dir = %ui_dir.display(), "serving static UI files");
    }

    info!(addr = %config.http_addr, "starting bisque unified HTTP server");

    let listener = tokio::net::TcpListener::bind(config.http_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
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

    // Build scopes from memberships
    let scopes: Vec<Scope> = memberships
        .iter()
        .map(|m| match m.role {
            bisque_meta::types::AccountRole::Admin => Scope::AccountAdmin(m.account_id),
            bisque_meta::types::AccountRole::Member => Scope::AccountAdmin(m.account_id),
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
    Ok((StatusCode::CREATED, Json(CreateAccountResponse { account_id })).into_response())
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
    let result = state.meta_engine.create_tenant(req.account_id, req.name.clone(), limits);
    match result {
        Ok(tenant_id) => {
            info!(tenant_id, name = %req.name, "tenant created");
            Ok((StatusCode::CREATED, Json(CreateTenantResponse { tenant_id })).into_response())
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

    let result = state.meta_engine.create_api_key(tenant_id, req.scopes.clone());
    match result {
        Ok((key_id, raw_key)) => {
            // Issue a bearer token for convenience
            let now = chrono::Utc::now().timestamp();
            let ttl = req.ttl_secs.unwrap_or(state.meta_engine.config().token_ttl_secs);
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

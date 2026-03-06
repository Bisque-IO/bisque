//! Authentication middleware for the bisque unified server.
//!
//! Extracts `Authorization: Bearer <token>` from requests, verifies the token
//! via `TokenManager`, checks that the API key is not revoked, and injects an
//! `AuthContext` into request extensions for downstream handlers.

use std::sync::Arc;

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use bisque_meta::engine::MetaEngine;
use bisque_meta::token::TokenManager;
use bisque_meta::types::Scope;

/// Authenticated request context available to handlers via request extensions.
#[derive(Clone, Debug)]
pub struct AuthContext {
    pub user_id: Option<u64>,
    pub account_id: Option<u64>,
    pub tenant_id: u64,
    pub key_id: u64,
    pub scopes: Vec<Scope>,
}

impl AuthContext {
    /// Check if this context grants full read/write access to the named catalog.
    pub fn has_catalog_access(&self, catalog: &str) -> bool {
        self.scopes.iter().any(|s| match s {
            Scope::SuperAdmin | Scope::TenantAdmin => true,
            Scope::AccountAdmin(_) => true,
            Scope::Catalog(name) => name == catalog,
            Scope::CatalogRead(_) => false,
        })
    }

    /// Check if this context grants at least read access to the named catalog.
    pub fn has_catalog_read(&self, catalog: &str) -> bool {
        self.scopes.iter().any(|s| match s {
            Scope::SuperAdmin | Scope::TenantAdmin => true,
            Scope::AccountAdmin(_) => true,
            Scope::Catalog(name) | Scope::CatalogRead(name) => name == catalog,
        })
    }

    /// Check if this context has tenant-wide admin privileges.
    pub fn is_tenant_admin(&self) -> bool {
        self.scopes
            .iter()
            .any(|s| matches!(s, Scope::TenantAdmin | Scope::SuperAdmin | Scope::AccountAdmin(_)))
    }

    pub fn is_super_admin(&self) -> bool {
        self.scopes.iter().any(|s| matches!(s, Scope::SuperAdmin))
    }

    pub fn is_account_admin(&self, account_id: u64) -> bool {
        self.scopes.iter().any(|s| match s {
            Scope::SuperAdmin => true,
            Scope::AccountAdmin(aid) => *aid == account_id,
            _ => false,
        })
    }
}

/// Shared state for the auth middleware layer.
#[derive(Clone)]
pub struct AuthState {
    pub token_manager: Arc<TokenManager>,
    pub meta_engine: Arc<MetaEngine>,
}

/// Axum middleware function that validates Bearer tokens.
///
/// On success, injects `AuthContext` into request extensions.
/// On failure, returns 401 Unauthorized.
pub async fn auth_middleware(
    request: Request,
    next: Next,
) -> Response {
    let auth_state = request.extensions().get::<AuthState>().cloned();
    let Some(auth_state) = auth_state else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "auth not configured").into_response();
    };

    // Extract Bearer token
    let token = match extract_bearer_token(&request) {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({ "error": "missing or invalid Authorization header" })),
            )
                .into_response();
        }
    };

    // Verify token signature and expiry
    let claims = match auth_state.token_manager.verify(token) {
        Some(c) => c,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({ "error": "invalid or expired token" })),
            )
                .into_response();
        }
    };

    // Check API key is not revoked (skip for user-login tokens with key_id=0)
    if claims.key_id != 0 {
        match auth_state.meta_engine.get_api_key(claims.key_id) {
            Some(key) => {
                if key.revoked {
                    return (
                        StatusCode::UNAUTHORIZED,
                        axum::Json(serde_json::json!({ "error": "API key has been revoked" })),
                    )
                        .into_response();
                }
                if key.tenant_id != claims.tenant_id {
                    return (
                        StatusCode::UNAUTHORIZED,
                        axum::Json(serde_json::json!({ "error": "tenant mismatch" })),
                    )
                        .into_response();
                }
            }
            None => {
                return (
                    StatusCode::UNAUTHORIZED,
                    axum::Json(serde_json::json!({ "error": "API key not found" })),
                )
                    .into_response();
            }
        }
    }

    // Inject AuthContext for downstream handlers
    let auth_ctx = AuthContext {
        user_id: claims.user_id,
        account_id: claims.account_id,
        tenant_id: claims.tenant_id,
        key_id: claims.key_id,
        scopes: claims.scopes,
    };

    let mut request = request;
    request.extensions_mut().insert(auth_ctx);
    next.run(request).await
}

/// Extract the Bearer token string from the Authorization header.
fn extract_bearer_token(request: &Request) -> Option<&str> {
    request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_context_tenant_admin() {
        let ctx = AuthContext {
            user_id: None,
            account_id: None,
            tenant_id: 1,
            key_id: 1,
            scopes: vec![Scope::TenantAdmin],
        };
        assert!(ctx.is_tenant_admin());
        assert!(ctx.has_catalog_access("anything"));
        assert!(ctx.has_catalog_read("anything"));
    }

    #[test]
    fn test_auth_context_catalog_scope() {
        let ctx = AuthContext {
            user_id: None,
            account_id: None,
            tenant_id: 1,
            key_id: 1,
            scopes: vec![Scope::Catalog("analytics".into())],
        };
        assert!(!ctx.is_tenant_admin());
        assert!(ctx.has_catalog_access("analytics"));
        assert!(ctx.has_catalog_read("analytics"));
        assert!(!ctx.has_catalog_access("other"));
        assert!(!ctx.has_catalog_read("other"));
    }

    #[test]
    fn test_auth_context_catalog_read_scope() {
        let ctx = AuthContext {
            user_id: None,
            account_id: None,
            tenant_id: 1,
            key_id: 1,
            scopes: vec![Scope::CatalogRead("events".into())],
        };
        assert!(!ctx.has_catalog_access("events"));
        assert!(ctx.has_catalog_read("events"));
    }
}

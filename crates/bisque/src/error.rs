//! API error types for the bisque unified server.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// Errors returned by bisque API handlers.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// 401 — missing, invalid, or expired token.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// 403 — valid token but insufficient scope for the requested operation.
    #[error("forbidden: {0}")]
    Forbidden(String),

    /// 404 — tenant, catalog, or resource not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// 409 — duplicate name or conflicting state.
    #[error("conflict: {0}")]
    Conflict(String),

    /// 500 — internal server error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            ApiError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg.clone()),
            ApiError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg.clone()),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            ApiError::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
        };

        let body = serde_json::json!({ "error": message });
        (status, axum::Json(body)).into_response()
    }
}

impl From<bisque_meta::WriteError> for ApiError {
    fn from(e: bisque_meta::WriteError) -> Self {
        match e {
            bisque_meta::WriteError::NotLeader { .. } => {
                ApiError::Internal("not leader".into())
            }
            bisque_meta::WriteError::Application(msg) => {
                if msg.contains("not found") {
                    ApiError::NotFound(msg)
                } else if msg.contains("already exists") {
                    ApiError::Conflict(msg)
                } else {
                    ApiError::Internal(msg)
                }
            }
            other => ApiError::Internal(other.to_string()),
        }
    }
}

//! Unified WebSocket protocol for the bisque UI.
//!
//! Defines a single, reliable WebSocket protocol that multiplexes:
//! - **Push events**: catalog mutations, operation updates, heartbeats
//! - **Request/response**: replaces most HTTP API calls with WS-based RPC
//!
//! # Reliability
//!
//! - Protocol version validation during handshake
//! - Monotonic per-connection sequence numbers on all push messages
//! - Client tracks `last_seen_seq` for gap detection
//! - WAL replay on reconnect via `last_seen_seq`
//! - Connection TTL with transparent refresh
//! - Backpressure via bounded broadcast channels
//! - Bidirectional heartbeats every 15 seconds
//!
//! # Type independence
//!
//! This crate uses `serde_json::Value` for catalog events and operations
//! so it has zero dependencies on engine crates. Engine handlers serialize
//! their concrete types into Values before constructing protocol messages.

use serde::{Deserialize, Serialize};

/// Protocol version — bump on breaking changes.
pub const WS_PROTOCOL_VERSION: u8 = 1;

// ---------------------------------------------------------------------------
// Server → Client messages
// ---------------------------------------------------------------------------

/// Server-to-client envelope. Push messages carry a monotonic `seq`.
/// Response messages echo the client's `request_id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    /// First message after WebSocket upgrade. Confirms protocol compatibility.
    Handshake {
        protocol_version: u8,
        session_id: u64,
        /// Current catalog event bus seq (global, WAL-backed).
        catalog_seq: u64,
        /// Starting push seq for this connection (always 0).
        server_seq: u64,
    },

    /// Response to a client `Request`.
    Response {
        request_id: u32,
        #[serde(flatten)]
        result: ResponseResult,
    },

    /// Push: a catalog mutation event.
    CatalogEvent {
        seq: u64,
        catalog: String,
        event: serde_json::Value,
    },

    /// Push: a single operation was created or its state changed.
    OperationUpdate {
        seq: u64,
        operation: serde_json::Value,
    },

    /// Push: full operations snapshot (sent on connect and after lag recovery).
    OperationsSnapshot {
        seq: u64,
        operations: Vec<serde_json::Value>,
    },

    /// Push: server heartbeat / keepalive with seq checkpoint.
    Heartbeat { seq: u64, server_time_ms: u64 },

    /// Push: client fell behind on catalog events, must re-fetch.
    SnapshotRequired { seq: u64, catalog: String },

    /// Graceful close notification. Client should reconnect if reason is `ttl_refresh`.
    Close { reason: String },
}

/// Result wrapper for request/response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum ResponseResult {
    /// Successful response with typed data.
    #[serde(rename = "ok")]
    Ok {
        #[serde(flatten)]
        data: ResponseData,
    },
    /// Error response with code and message.
    #[serde(rename = "error")]
    Error { code: u16, message: String },
}

/// Typed response payloads keyed by method name.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum ResponseData {
    #[serde(rename = "list_catalogs")]
    ListCatalogs { catalogs: Vec<serde_json::Value> },
    #[serde(rename = "create_catalog")]
    CreateCatalog { catalog_id: u64, raft_group_id: u64 },
    #[serde(rename = "get_catalog")]
    GetCatalog { catalog: serde_json::Value },
    #[serde(rename = "get_tenant")]
    GetTenant { tenant: serde_json::Value },
    #[serde(rename = "list_operations")]
    ListOperations { operations: Vec<serde_json::Value> },
    #[serde(rename = "get_operation")]
    GetOperation { operation: serde_json::Value },
    #[serde(rename = "cancel_operation")]
    CancelOperation { op_id: String, message: String },
    #[serde(rename = "submit_reindex")]
    SubmitReindex { op_id: String, message: String },
    #[serde(rename = "submit_compact")]
    SubmitCompact { op_id: String, message: String },
    #[serde(rename = "create_api_key")]
    CreateApiKey {
        key_id: u64,
        raw_key: String,
        token: String,
    },
    #[serde(rename = "cluster_status")]
    ClusterStatus { cluster: serde_json::Value },
    #[serde(rename = "pinned")]
    Pinned,
    #[serde(rename = "unpinned")]
    Unpinned,
    #[serde(rename = "list_tenants")]
    ListTenants { tenants: Vec<serde_json::Value> },
    #[serde(rename = "update_tenant_limits")]
    UpdateTenantLimits,
    #[serde(rename = "delete_tenant")]
    DeleteTenant,
    #[serde(rename = "delete_catalog")]
    DeleteCatalog,
    #[serde(rename = "list_api_keys")]
    ListApiKeys { api_keys: Vec<serde_json::Value> },
    #[serde(rename = "revoke_api_key")]
    RevokeApiKey,
    #[serde(rename = "create_table")]
    CreateTable { table: String },
    #[serde(rename = "drop_table")]
    DropTable { table: String },
    #[serde(rename = "execute_sql")]
    ExecuteSql {
        columns: Vec<serde_json::Value>,
        rows: Vec<serde_json::Value>,
        row_count: u64,
    },
    #[serde(rename = "list_accounts")]
    ListAccounts { accounts: Vec<serde_json::Value> },
    #[serde(rename = "enable_otel")]
    EnableOtel { tables_created: Vec<String> },
}

// ---------------------------------------------------------------------------
// Client → Server messages
// ---------------------------------------------------------------------------

/// Client-to-server envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    /// Client handshake response with resume state and auth token.
    Handshake {
        protocol_version: u8,
        /// Bearer token for authentication (moved from URL to avoid log exposure).
        #[serde(default)]
        token: String,
        /// Last seq the client successfully processed. Server replays from here.
        last_seen_seq: u64,
        /// Catalogs to subscribe to for push events.
        subscribe_catalogs: Vec<String>,
    },

    /// Request/response call.
    Request {
        request_id: u32,
        #[serde(flatten)]
        method: RequestMethod,
    },

    /// Version pin.
    Pin {
        catalog: String,
        table: String,
        tier: String,
        version: u64,
    },

    /// Version unpin.
    Unpin {
        catalog: String,
        table: String,
        tier: String,
        version: u64,
    },

    /// Client heartbeat — includes last_seen_seq for gap detection.
    Heartbeat { last_seen_seq: u64 },

    /// Graceful close.
    Close,
}

/// Request methods the client can invoke.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum RequestMethod {
    #[serde(rename = "list_catalogs")]
    ListCatalogs { tenant_id: u64 },

    #[serde(rename = "create_catalog")]
    CreateCatalog {
        tenant_id: u64,
        name: String,
        engine: String,
        #[serde(default)]
        config: Option<String>,
    },

    #[serde(rename = "get_catalog")]
    GetCatalog { bucket: String },

    #[serde(rename = "get_tenant")]
    GetTenant { tenant_id: u64 },

    #[serde(rename = "list_operations")]
    ListOperations {
        #[serde(default)]
        op_type: Option<String>,
        #[serde(default)]
        tier: Option<String>,
        #[serde(default)]
        status: Option<String>,
    },

    #[serde(rename = "get_operation")]
    GetOperation { op_id: String },

    #[serde(rename = "cancel_operation")]
    CancelOperation { op_id: String },

    #[serde(rename = "submit_reindex")]
    SubmitReindex { bucket: String, table: String },

    #[serde(rename = "submit_compact")]
    SubmitCompact { bucket: String, table: String },

    #[serde(rename = "create_api_key")]
    CreateApiKey {
        tenant_id: u64,
        scopes: Vec<serde_json::Value>,
        #[serde(default)]
        ttl_secs: Option<u64>,
    },

    #[serde(rename = "cluster_status")]
    GetClusterStatus,

    #[serde(rename = "list_tenants")]
    ListTenants { account_id: u64 },

    #[serde(rename = "update_tenant_limits")]
    UpdateTenantLimits {
        tenant_id: u64,
        limits: serde_json::Value,
    },

    #[serde(rename = "delete_tenant")]
    DeleteTenant { tenant_id: u64 },

    #[serde(rename = "delete_catalog")]
    DeleteCatalog { tenant_id: u64, catalog_id: u64 },

    #[serde(rename = "list_api_keys")]
    ListApiKeys { tenant_id: u64 },

    #[serde(rename = "revoke_api_key")]
    RevokeApiKey { key_id: u64 },

    #[serde(rename = "create_table")]
    CreateTable {
        catalog: String,
        table: String,
        schema_json: String,
    },

    #[serde(rename = "drop_table")]
    DropTable { catalog: String, table: String },

    #[serde(rename = "execute_sql")]
    ExecuteSql { catalog: String, sql: String },

    #[serde(rename = "list_accounts")]
    ListAccounts,

    #[serde(rename = "enable_otel")]
    EnableOtel,
}

impl RequestMethod {
    /// Returns a short label for metrics and logging.
    pub fn method_name(&self) -> &'static str {
        match self {
            Self::ListCatalogs { .. } => "list_catalogs",
            Self::CreateCatalog { .. } => "create_catalog",
            Self::GetCatalog { .. } => "get_catalog",
            Self::GetTenant { .. } => "get_tenant",
            Self::ListOperations { .. } => "list_operations",
            Self::GetOperation { .. } => "get_operation",
            Self::CancelOperation { .. } => "cancel_operation",
            Self::SubmitReindex { .. } => "submit_reindex",
            Self::SubmitCompact { .. } => "submit_compact",
            Self::CreateApiKey { .. } => "create_api_key",
            Self::GetClusterStatus => "cluster_status",
            Self::ListTenants { .. } => "list_tenants",
            Self::UpdateTenantLimits { .. } => "update_tenant_limits",
            Self::DeleteTenant { .. } => "delete_tenant",
            Self::DeleteCatalog { .. } => "delete_catalog",
            Self::ListApiKeys { .. } => "list_api_keys",
            Self::RevokeApiKey { .. } => "revoke_api_key",
            Self::CreateTable { .. } => "create_table",
            Self::DropTable { .. } => "drop_table",
            Self::ExecuteSql { .. } => "execute_sql",
            Self::ListAccounts => "list_accounts",
            Self::EnableOtel => "enable_otel",
        }
    }

    /// Returns true for methods that are computationally expensive
    /// and should be subject to rate limiting.
    pub fn is_expensive(&self) -> bool {
        matches!(
            self,
            Self::ListCatalogs { .. }
                | Self::ListOperations { .. }
                | Self::GetCatalog { .. }
                | Self::GetClusterStatus
                | Self::ListTenants { .. }
                | Self::ListApiKeys { .. }
                | Self::ListAccounts
                | Self::ExecuteSql { .. }
                | Self::EnableOtel
        )
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Encode a server message to MessagePack bytes.
pub fn encode_server_msg(msg: &ServerMessage) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    rmp_serde::to_vec_named(msg)
}

/// Decode a client message from MessagePack bytes.
pub fn decode_client_msg(data: &[u8]) -> Result<ClientMessage, rmp_serde::decode::Error> {
    rmp_serde::from_slice(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_response_msgpack_roundtrip() {
        let msg = ServerMessage::Response {
            request_id: 42,
            result: ResponseResult::Ok {
                data: ResponseData::GetCatalog {
                    catalog: json!({"tables": {"t1": {"active_version": 1}}}),
                },
            },
        };

        let encoded = encode_server_msg(&msg).unwrap();

        // Decode as raw serde_json::Value to see exactly what the JS client would see
        let decoded: serde_json::Value = rmp_serde::from_slice(&encoded).unwrap();
        println!(
            "Decoded GetCatalog response: {}",
            serde_json::to_string_pretty(&decoded).unwrap()
        );

        // Verify the shape matches what the JS client expects
        assert_eq!(decoded["type"], "Response");
        assert_eq!(decoded["request_id"], 42);
        assert_eq!(decoded["status"], "ok");
        assert_eq!(decoded["method"], "get_catalog");
        assert!(
            decoded["catalog"].is_object(),
            "catalog field should be an object, got: {:?}",
            decoded["catalog"]
        );
    }

    #[test]
    fn test_request_msgpack_roundtrip() {
        // Simulate what the JS client sends
        let js_request = json!({
            "type": "Request",
            "request_id": 1,
            "method": "get_catalog",
            "bucket": "my_catalog"
        });

        let encoded = rmp_serde::to_vec_named(&js_request).unwrap();

        // Try to decode as ClientMessage
        let decoded: Result<ClientMessage, _> = rmp_serde::from_slice(&encoded);
        println!("Decode result: {:?}", decoded);
        assert!(
            decoded.is_ok(),
            "Should decode successfully: {:?}",
            decoded.err()
        );

        if let Ok(ClientMessage::Request {
            request_id,
            method: RequestMethod::GetCatalog { bucket },
        }) = decoded
        {
            assert_eq!(request_id, 1);
            assert_eq!(bucket, "my_catalog");
        } else {
            panic!("Expected Request/GetCatalog, got: {:?}", decoded);
        }
    }

    #[test]
    fn test_cluster_status_response() {
        let msg = ServerMessage::Response {
            request_id: 1,
            result: ResponseResult::Ok {
                data: ResponseData::ClusterStatus {
                    cluster: json!({"cluster_name": "bisque", "nodes": []}),
                },
            },
        };

        let encoded = encode_server_msg(&msg).unwrap();
        let decoded: serde_json::Value = rmp_serde::from_slice(&encoded).unwrap();
        println!(
            "Decoded ClusterStatus: {}",
            serde_json::to_string_pretty(&decoded).unwrap()
        );

        assert_eq!(decoded["type"], "Response");
        assert_eq!(decoded["status"], "ok");
        assert_eq!(decoded["method"], "cluster_status");
        assert!(decoded["cluster"].is_object());
    }

    #[test]
    fn test_error_response() {
        let msg = ServerMessage::Response {
            request_id: 5,
            result: ResponseResult::Error {
                code: 429,
                message: "Rate limited".into(),
            },
        };

        let encoded = encode_server_msg(&msg).unwrap();
        let decoded: serde_json::Value = rmp_serde::from_slice(&encoded).unwrap();
        println!(
            "Decoded error: {}",
            serde_json::to_string_pretty(&decoded).unwrap()
        );

        assert_eq!(decoded["type"], "Response");
        assert_eq!(decoded["request_id"], 5);
        assert_eq!(decoded["status"], "error");
        assert_eq!(decoded["code"], 429);
    }
}

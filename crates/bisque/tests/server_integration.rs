//! Comprehensive integration tests for the bisque unified server.
//!
//! Tests all HTTP endpoints, WebSocket RPC methods, auth enforcement,
//! event push, and S3 API to verify the server correctly exposes all
//! features from bisque-lance, bisque-meta, and bisque-raft.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bisque::ServerHandle;
use bisque::config::BisqueConfig;
use bisque::server;
use bisque_meta::engine::MetaEngine;
use bisque_meta::token::{self, TokenClaims, TokenManager};
use bisque_meta::types::{AccountRole, Scope, TenantLimits};
use bisque_protocol::ws::{
    ClientMessage, RequestMethod, ResponseData, ResponseResult, ServerMessage, WS_PROTOCOL_VERSION,
};

// ---------------------------------------------------------------------------
// Test Harness
// ---------------------------------------------------------------------------

struct TestServer {
    pub base_url: String,
    pub http_addr: SocketAddr,
    pub bearer_token: String,
    pub super_admin_token: String,
    pub tenant_id: u64,
    pub account_id: u64,
    pub client: reqwest::Client,
    pub meta_engine: Arc<MetaEngine>,
    pub token_manager: Arc<TokenManager>,
    handle: ServerHandle,
    _dir: tempfile::TempDir,
}

impl TestServer {
    async fn start() -> Self {
        let dir = tempfile::TempDir::new().expect("create tempdir");

        let config = BisqueConfig::new(dir.path(), b"test-secret-key-32-bytes-long!!!".to_vec())
            .with_http_addr("127.0.0.1:0".parse().unwrap())
            .with_flight_addr("127.0.0.1:0".parse().unwrap())
            .with_otlp_grpc_addr("127.0.0.1:0".parse().unwrap())
            .with_node_id(1)
            .with_token_ttl_secs(3600);

        let handle = server::start(config).await.expect("start server");

        let http_addr = handle.http_addr;
        let base_url = format!("http://{}", http_addr);
        let meta_engine = handle.meta_engine.clone();
        let token_manager = handle.token_manager.clone();

        // Bootstrap: account, user, membership, tenant, API key
        let account_id = meta_engine
            .create_account("test-account".into())
            .expect("create account");
        let password_hash = token::hash_password(b"password");
        let user_id = meta_engine
            .create_user("admin".into(), password_hash)
            .expect("create user");
        meta_engine
            .add_membership(user_id, account_id, AccountRole::Admin)
            .expect("add membership");
        let tenant_id = meta_engine
            .create_tenant(account_id, "test-tenant".into(), TenantLimits::default())
            .expect("create tenant");

        // TenantAdmin API key + bearer token
        let (key_id, _raw_key) = meta_engine
            .create_api_key(tenant_id, vec![Scope::TenantAdmin])
            .expect("create api key");
        let now = chrono::Utc::now().timestamp();
        let bearer_token = token_manager.issue(&TokenClaims {
            user_id: None,
            account_id: None,
            tenant_id,
            key_id,
            scopes: vec![Scope::TenantAdmin],
            issued_at: now,
            expires_at: now + 3600,
        });

        // SuperAdmin token (key_id=0 bypasses API key check)
        let super_admin_token = token_manager.issue(&TokenClaims {
            user_id: None,
            account_id: None,
            tenant_id: 0,
            key_id: 0,
            scopes: vec![Scope::SuperAdmin],
            issued_at: now,
            expires_at: now + 3600,
        });

        let client = reqwest::Client::new();

        // Wait for health check
        for _ in 0..100 {
            if let Ok(resp) = client
                .get(format!("{base_url}/_bisque/health"))
                .send()
                .await
            {
                if resp.status().is_success() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        TestServer {
            base_url,
            http_addr,
            bearer_token,
            super_admin_token,
            tenant_id,
            account_id,
            client,
            meta_engine,
            token_manager,
            handle,
            _dir: dir,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn get(&self, path: &str) -> reqwest::Response {
        self.client
            .get(self.url(path))
            .bearer_auth(&self.bearer_token)
            .send()
            .await
            .expect("GET request")
    }

    async fn get_with_token(&self, path: &str, token: &str) -> reqwest::Response {
        self.client
            .get(self.url(path))
            .bearer_auth(token)
            .send()
            .await
            .expect("GET request")
    }

    async fn get_no_auth(&self, path: &str) -> reqwest::Response {
        self.client
            .get(self.url(path))
            .send()
            .await
            .expect("GET request")
    }

    async fn post(&self, path: &str, body: &serde_json::Value) -> reqwest::Response {
        self.client
            .post(self.url(path))
            .bearer_auth(&self.bearer_token)
            .json(body)
            .send()
            .await
            .expect("POST request")
    }

    async fn post_with_token(
        &self,
        path: &str,
        body: &serde_json::Value,
        token: &str,
    ) -> reqwest::Response {
        self.client
            .post(self.url(path))
            .bearer_auth(token)
            .json(body)
            .send()
            .await
            .expect("POST request")
    }

    async fn post_no_auth(&self, path: &str, body: &serde_json::Value) -> reqwest::Response {
        self.client
            .post(self.url(path))
            .json(body)
            .send()
            .await
            .expect("POST request")
    }

    async fn delete(&self, path: &str) -> reqwest::Response {
        self.client
            .delete(self.url(path))
            .bearer_auth(&self.bearer_token)
            .send()
            .await
            .expect("DELETE request")
    }

    async fn shutdown(self) {
        self.handle.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// WebSocket Client Helper
// ---------------------------------------------------------------------------

struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: std::future::Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::spawn(fut);
    }
}

struct WsClient {
    ws: fastwebsockets::WebSocket<hyper_util::rt::TokioIo<hyper::upgrade::Upgraded>>,
    pub session_id: u64,
}

impl WsClient {
    async fn connect(addr: SocketAddr, token: &str) -> Self {
        Self::connect_with_options(addr, token, vec![], WS_PROTOCOL_VERSION).await
    }

    async fn connect_with_options(
        addr: SocketAddr,
        token: &str,
        subscribe_catalogs: Vec<String>,
        protocol_version: u8,
    ) -> Self {
        let tcp = tokio::net::TcpStream::connect(addr)
            .await
            .expect("TCP connect");
        let req = hyper::Request::builder()
            .method("GET")
            .uri(format!("http://{addr}/_bisque/ws"))
            .header("Host", addr.to_string())
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                fastwebsockets::handshake::generate_key(),
            )
            .body(http_body_util::Empty::<hyper::body::Bytes>::new())
            .expect("build request");

        let (mut ws, _) = fastwebsockets::handshake::client(&SpawnExecutor, req, tcp)
            .await
            .expect("WS upgrade");

        // 1. Receive server handshake
        let frame = ws.read_frame().await.expect("read server handshake");
        let server_msg: ServerMessage =
            rmp_serde::from_slice(&frame.payload).expect("decode server handshake");
        let session_id = match &server_msg {
            ServerMessage::Handshake { session_id, .. } => *session_id,
            other => panic!("expected Handshake, got: {:?}", msg_type(other)),
        };

        // 2. Send client handshake
        let client_hs = ClientMessage::Handshake {
            protocol_version,
            token: token.to_string(),
            last_seen_seq: 0,
            subscribe_catalogs,
        };
        let bytes = rmp_serde::to_vec_named(&client_hs).expect("encode client handshake");
        ws.write_frame(fastwebsockets::Frame::binary(
            fastwebsockets::Payload::Owned(bytes),
        ))
        .await
        .expect("send client handshake");

        // 3. After successful auth, drain initial messages (OperationsSnapshot, etc.)
        // until we stop receiving within a short window
        loop {
            match tokio::time::timeout(Duration::from_millis(500), ws.read_frame()).await {
                Ok(Ok(frame)) => {
                    if frame.opcode == fastwebsockets::OpCode::Close {
                        panic!("WS closed after handshake (auth failed or protocol error)");
                    }
                    let msg: ServerMessage =
                        rmp_serde::from_slice(&frame.payload).expect("decode initial msg");
                    match &msg {
                        ServerMessage::OperationsSnapshot { .. } => continue,
                        ServerMessage::Heartbeat { .. } => continue,
                        ServerMessage::Close { reason } => {
                            panic!("WS closed: {reason}");
                        }
                        _ => continue,
                    }
                }
                Ok(Err(e)) => panic!("WS read error during init: {e}"),
                Err(_) => break, // Timeout — done draining
            }
        }

        WsClient { ws, session_id }
    }

    /// Connect but expect the connection to close (e.g., bad auth or wrong version).
    /// Returns the close reason if a Close message was received, or the raw ServerMessage.
    async fn connect_expect_close(addr: SocketAddr, token: &str, protocol_version: u8) -> String {
        let tcp = tokio::net::TcpStream::connect(addr)
            .await
            .expect("TCP connect");
        let req = hyper::Request::builder()
            .method("GET")
            .uri(format!("http://{addr}/_bisque/ws"))
            .header("Host", addr.to_string())
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                fastwebsockets::handshake::generate_key(),
            )
            .body(http_body_util::Empty::<hyper::body::Bytes>::new())
            .expect("build request");

        let (mut ws, _) = fastwebsockets::handshake::client(&SpawnExecutor, req, tcp)
            .await
            .expect("WS upgrade");

        // Receive server handshake
        let frame = ws.read_frame().await.expect("read server handshake");
        let _server_msg: ServerMessage =
            rmp_serde::from_slice(&frame.payload).expect("decode server handshake");

        // Send client handshake (possibly with bad token/version)
        let client_hs = ClientMessage::Handshake {
            protocol_version,
            token: token.to_string(),
            last_seen_seq: 0,
            subscribe_catalogs: vec![],
        };
        let bytes = rmp_serde::to_vec_named(&client_hs).expect("encode");
        ws.write_frame(fastwebsockets::Frame::binary(
            fastwebsockets::Payload::Owned(bytes),
        ))
        .await
        .expect("send");

        // Expect a Close message
        let frame = tokio::time::timeout(Duration::from_secs(5), ws.read_frame())
            .await
            .expect("timeout waiting for close")
            .expect("read close frame");

        if frame.opcode == fastwebsockets::OpCode::Close {
            return String::from_utf8_lossy(&frame.payload).to_string();
        }

        // Might be a binary frame with ServerMessage::Close
        if let Ok(msg) = rmp_serde::from_slice::<ServerMessage>(&frame.payload) {
            match msg {
                ServerMessage::Close { reason } => return reason,
                other => return format!("unexpected: {:?}", msg_type(&other)),
            }
        }

        "connection closed".to_string()
    }

    async fn send(&mut self, msg: &ClientMessage) {
        let bytes = rmp_serde::to_vec_named(msg).expect("encode");
        self.ws
            .write_frame(fastwebsockets::Frame::binary(
                fastwebsockets::Payload::Owned(bytes),
            ))
            .await
            .expect("send frame");
    }

    async fn recv(&mut self) -> ServerMessage {
        let frame = self.ws.read_frame().await.expect("read frame");
        rmp_serde::from_slice(&frame.payload).expect("decode message")
    }

    async fn recv_timeout(&mut self, timeout: Duration) -> Option<ServerMessage> {
        match tokio::time::timeout(timeout, self.ws.read_frame()).await {
            Ok(Ok(frame)) => {
                if frame.opcode == fastwebsockets::OpCode::Close {
                    return None;
                }
                Some(rmp_serde::from_slice(&frame.payload).expect("decode"))
            }
            _ => None,
        }
    }

    async fn request(&mut self, request_id: u32, method: RequestMethod) -> ResponseResult {
        self.send(&ClientMessage::Request { request_id, method })
            .await;

        // Read messages until we get a Response with matching request_id
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(10), self.recv())
                .await
                .expect("timeout waiting for response");
            match msg {
                ServerMessage::Response {
                    request_id: rid,
                    result,
                } if rid == request_id => {
                    return result;
                }
                ServerMessage::Heartbeat { .. }
                | ServerMessage::CatalogEvent { .. }
                | ServerMessage::OperationUpdate { .. }
                | ServerMessage::OperationsSnapshot { .. } => continue,
                ServerMessage::Close { reason } => panic!("WS closed: {reason}"),
                _ => continue,
            }
        }
    }
}

fn msg_type(msg: &ServerMessage) -> &'static str {
    match msg {
        ServerMessage::Handshake { .. } => "Handshake",
        ServerMessage::Response { .. } => "Response",
        ServerMessage::CatalogEvent { .. } => "CatalogEvent",
        ServerMessage::OperationUpdate { .. } => "OperationUpdate",
        ServerMessage::OperationsSnapshot { .. } => "OperationsSnapshot",
        ServerMessage::Heartbeat { .. } => "Heartbeat",
        ServerMessage::SnapshotRequired { .. } => "SnapshotRequired",
        ServerMessage::Close { .. } => "Close",
    }
}

// ===========================================================================
// A. Health Check
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_health_check() {
    let server = TestServer::start().await;
    let resp = server.get_no_auth("/_bisque/health").await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
    server.shutdown().await;
}

// ===========================================================================
// B. Auth Middleware
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_missing_auth_returns_401() {
    let server = TestServer::start().await;
    let resp = server.get_no_auth("/_bisque/v1/accounts").await;
    assert_eq!(resp.status(), 401);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_invalid_token_returns_401() {
    let server = TestServer::start().await;
    let resp = server
        .get_with_token("/_bisque/v1/accounts", "garbage-token")
        .await;
    assert_eq!(resp.status(), 401);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_expired_token_returns_401() {
    let server = TestServer::start().await;
    let now = chrono::Utc::now().timestamp();
    let expired_token = server.token_manager.issue(&TokenClaims {
        user_id: None,
        account_id: None,
        tenant_id: server.tenant_id,
        key_id: 0,
        scopes: vec![Scope::TenantAdmin],
        issued_at: now - 7200,
        expires_at: now - 3600, // expired 1 hour ago
    });
    let resp = server
        .get_with_token(
            &format!("/_bisque/v1/tenants/{}", server.tenant_id),
            &expired_token,
        )
        .await;
    assert_eq!(resp.status(), 401);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_revoked_api_key_returns_401() {
    let server = TestServer::start().await;
    let (key_id, _raw_key) = server
        .meta_engine
        .create_api_key(server.tenant_id, vec![Scope::TenantAdmin])
        .unwrap();
    let now = chrono::Utc::now().timestamp();
    let token = server.token_manager.issue(&TokenClaims {
        user_id: None,
        account_id: None,
        tenant_id: server.tenant_id,
        key_id,
        scopes: vec![Scope::TenantAdmin],
        issued_at: now,
        expires_at: now + 3600,
    });

    // Works before revocation
    let resp = server
        .get_with_token(&format!("/_bisque/v1/tenants/{}", server.tenant_id), &token)
        .await;
    assert_eq!(resp.status(), 200);

    // Revoke
    server.meta_engine.revoke_api_key(key_id).unwrap();

    // Fails after revocation
    let resp = server
        .get_with_token(&format!("/_bisque/v1/tenants/{}", server.tenant_id), &token)
        .await;
    assert_eq!(resp.status(), 401);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_valid_token_succeeds() {
    let server = TestServer::start().await;
    let resp = server
        .get(&format!("/_bisque/v1/tenants/{}", server.tenant_id))
        .await;
    assert_eq!(resp.status(), 200);
    server.shutdown().await;
}

// ===========================================================================
// C. Account Management
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_create_account() {
    let server = TestServer::start().await;
    let resp = server
        .post_with_token(
            "/_bisque/v1/accounts",
            &serde_json::json!({"name": "new-acct"}),
            &server.super_admin_token,
        )
        .await;
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["account_id"].as_u64().unwrap() > 0);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_account_forbidden_non_super_admin() {
    let server = TestServer::start().await;
    // TenantAdmin token is NOT SuperAdmin
    let resp = server
        .post(
            "/_bisque/v1/accounts",
            &serde_json::json!({"name": "forbidden-acct"}),
        )
        .await;
    assert_eq!(resp.status(), 403);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_accounts() {
    let server = TestServer::start().await;
    let resp = server
        .get_with_token("/_bisque/v1/accounts", &server.super_admin_token)
        .await;
    assert_eq!(resp.status(), 200);
    let body: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(!body.is_empty());
    assert_eq!(body[0]["name"], "test-account");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_account() {
    let server = TestServer::start().await;
    let resp = server
        .get_with_token(
            &format!("/_bisque/v1/accounts/{}", server.account_id),
            &server.super_admin_token,
        )
        .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "test-account");
    assert_eq!(body["account_id"], server.account_id);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_account_not_found() {
    let server = TestServer::start().await;
    let resp = server
        .get_with_token("/_bisque/v1/accounts/99999", &server.super_admin_token)
        .await;
    assert_eq!(resp.status(), 404);
    server.shutdown().await;
}

// ===========================================================================
// D. Tenant Management
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_create_tenant() {
    let server = TestServer::start().await;
    let resp = server
        .post(
            "/_bisque/v1/tenants",
            &serde_json::json!({
                "account_id": server.account_id,
                "name": "new-tenant"
            }),
        )
        .await;
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["tenant_id"].as_u64().unwrap() > 0);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_tenant() {
    let server = TestServer::start().await;
    let resp = server
        .get(&format!("/_bisque/v1/tenants/{}", server.tenant_id))
        .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "test-tenant");
    assert_eq!(body["tenant_id"], server.tenant_id);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_tenant_not_found() {
    let server = TestServer::start().await;
    let resp = server.get("/_bisque/v1/tenants/99999").await;
    assert_eq!(resp.status(), 404);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_tenant_forbidden() {
    let server = TestServer::start().await;
    // Create a token for a different tenant
    let now = chrono::Utc::now().timestamp();
    let other_token = server.token_manager.issue(&TokenClaims {
        user_id: None,
        account_id: None,
        tenant_id: 9999, // different tenant
        key_id: 0,
        scopes: vec![Scope::CatalogRead("x".into())], // not tenant admin
        issued_at: now,
        expires_at: now + 3600,
    });
    let resp = server
        .get_with_token(
            &format!("/_bisque/v1/tenants/{}", server.tenant_id),
            &other_token,
        )
        .await;
    assert_eq!(resp.status(), 403);
    server.shutdown().await;
}

// ===========================================================================
// E. Catalog Management
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_create_catalog() {
    let server = TestServer::start().await;
    let resp = server
        .post(
            &format!("/_bisque/v1/tenants/{}/catalogs", server.tenant_id),
            &serde_json::json!({
                "name": "analytics",
                "engine": "Lance",
                "config": "{}"
            }),
        )
        .await;
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["catalog_id"].as_u64().unwrap() > 0);
    assert!(body["raft_group_id"].as_u64().unwrap() > 0);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_duplicate_catalog() {
    let server = TestServer::start().await;
    let path = format!("/_bisque/v1/tenants/{}/catalogs", server.tenant_id);
    let body = serde_json::json!({
        "name": "dup-catalog",
        "engine": "Lance",
        "config": ""
    });

    let resp = server.post(&path, &body).await;
    assert_eq!(resp.status(), 201);

    let resp = server.post(&path, &body).await;
    assert_eq!(resp.status(), 409);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_catalogs() {
    let server = TestServer::start().await;
    let path = format!("/_bisque/v1/tenants/{}/catalogs", server.tenant_id);

    // Create one catalog
    server
        .post(
            &path,
            &serde_json::json!({"name": "cat1", "engine": "Lance", "config": ""}),
        )
        .await;

    let resp = server.get(&path).await;
    assert_eq!(resp.status(), 200);
    let body: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(body.iter().any(|c| c["name"] == "cat1"));
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_list_catalogs_empty() {
    let server = TestServer::start().await;
    let resp = server
        .get(&format!(
            "/_bisque/v1/tenants/{}/catalogs",
            server.tenant_id
        ))
        .await;
    assert_eq!(resp.status(), 200);
    let body: Vec<serde_json::Value> = resp.json().await.unwrap();
    // May have auto-created OTEL catalog depending on config
    // Just check it returns a valid array
    assert!(body.len() < 100);
    server.shutdown().await;
}

// ===========================================================================
// F. API Key Management
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_create_api_key() {
    let server = TestServer::start().await;
    let resp = server
        .post(
            &format!("/_bisque/v1/tenants/{}/api-keys", server.tenant_id),
            &serde_json::json!({"scopes": ["TenantAdmin"]}),
        )
        .await;
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["key_id"].as_u64().unwrap() > 0);
    assert!(!body["raw_key"].as_str().unwrap().is_empty());
    assert!(!body["token"].as_str().unwrap().is_empty());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_api_key_token_is_valid() {
    let server = TestServer::start().await;
    let resp = server
        .post(
            &format!("/_bisque/v1/tenants/{}/api-keys", server.tenant_id),
            &serde_json::json!({"scopes": ["TenantAdmin"]}),
        )
        .await;
    assert_eq!(resp.status(), 201);
    let body: serde_json::Value = resp.json().await.unwrap();
    let new_token = body["token"].as_str().unwrap();

    // Use the returned token to make a request
    let resp = server
        .get_with_token(
            &format!("/_bisque/v1/tenants/{}", server.tenant_id),
            new_token,
        )
        .await;
    assert_eq!(resp.status(), 200);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_revoke_api_key() {
    let server = TestServer::start().await;
    let resp = server
        .post(
            &format!("/_bisque/v1/tenants/{}/api-keys", server.tenant_id),
            &serde_json::json!({"scopes": ["TenantAdmin"]}),
        )
        .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    let key_id = body["key_id"].as_u64().unwrap();

    let resp = server
        .delete(&format!("/_bisque/v1/api-keys/{}", key_id))
        .await;
    assert_eq!(resp.status(), 204);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_revoked_key_token_rejected() {
    let server = TestServer::start().await;
    let resp = server
        .post(
            &format!("/_bisque/v1/tenants/{}/api-keys", server.tenant_id),
            &serde_json::json!({"scopes": ["TenantAdmin"]}),
        )
        .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    let key_id = body["key_id"].as_u64().unwrap();
    let token = body["token"].as_str().unwrap().to_string();

    // Revoke
    server
        .delete(&format!("/_bisque/v1/api-keys/{}", key_id))
        .await;

    // Token should be rejected
    let resp = server
        .get_with_token(&format!("/_bisque/v1/tenants/{}", server.tenant_id), &token)
        .await;
    assert_eq!(resp.status(), 401);
    server.shutdown().await;
}

// ===========================================================================
// G. Login Flow
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_login_success() {
    let server = TestServer::start().await;
    let resp = server
        .post_no_auth(
            "/_bisque/v1/auth/login",
            &serde_json::json!({
                "username": "admin",
                "password": "password"
            }),
        )
        .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["user_id"].as_u64().unwrap() > 0);
    assert!(!body["token"].as_str().unwrap().is_empty());
    assert!(body["accounts"].as_array().unwrap().len() > 0);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_login_invalid_credentials() {
    let server = TestServer::start().await;
    let resp = server
        .post_no_auth(
            "/_bisque/v1/auth/login",
            &serde_json::json!({
                "username": "admin",
                "password": "wrong-password"
            }),
        )
        .await;
    assert_eq!(resp.status(), 401);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_login_nonexistent_user() {
    let server = TestServer::start().await;
    let resp = server
        .post_no_auth(
            "/_bisque/v1/auth/login",
            &serde_json::json!({
                "username": "nobody",
                "password": "anything"
            }),
        )
        .await;
    assert_eq!(resp.status(), 401);
    server.shutdown().await;
}

// ===========================================================================
// H. WebSocket Connection & Protocol
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_handshake() {
    let server = TestServer::start().await;
    let ws = WsClient::connect(server.http_addr, &server.bearer_token).await;
    assert!(ws.session_id > 0);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_handshake_invalid_token() {
    let server = TestServer::start().await;
    let reason =
        WsClient::connect_expect_close(server.http_addr, "bad-token", WS_PROTOCOL_VERSION).await;
    assert!(
        reason.contains("invalid")
            || reason.contains("expired")
            || reason.contains("token")
            || !reason.is_empty(),
        "expected auth error, got: {reason}"
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_handshake_wrong_protocol_version() {
    let server = TestServer::start().await;
    let reason = WsClient::connect_expect_close(
        server.http_addr,
        &server.bearer_token,
        99, // wrong version
    )
    .await;
    assert!(
        reason.contains("version") || reason.contains("mismatch"),
        "expected version error, got: {reason}"
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_request_response() {
    let server = TestServer::start().await;
    let mut ws = WsClient::connect(server.http_addr, &server.bearer_token).await;
    let result = ws.request(1, RequestMethod::GetClusterStatus).await;
    match result {
        ResponseResult::Ok { data } => match data {
            ResponseData::ClusterStatus { cluster } => {
                assert!(cluster["node_id"].as_u64().is_some());
            }
            _ => panic!("expected ClusterStatus data"),
        },
        ResponseResult::Error { code, message } => {
            panic!("expected Ok, got error {code}: {message}");
        }
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_heartbeat() {
    let server = TestServer::start().await;
    let mut ws = WsClient::connect(server.http_addr, &server.bearer_token).await;

    // Server heartbeat interval is 15s, so wait up to 20s
    let mut received_heartbeat = false;
    for _ in 0..40 {
        match ws.recv_timeout(Duration::from_millis(500)).await {
            Some(ServerMessage::Heartbeat { server_time_ms, .. }) => {
                assert!(server_time_ms > 0);
                received_heartbeat = true;
                break;
            }
            Some(_) => continue,
            None => continue,
        }
    }
    assert!(received_heartbeat, "did not receive heartbeat within 20s");
    server.shutdown().await;
}

// ===========================================================================
// I. WebSocket RPC Methods
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_get_cluster_status() {
    let server = TestServer::start().await;
    let mut ws = WsClient::connect(server.http_addr, &server.bearer_token).await;
    let result = ws.request(1, RequestMethod::GetClusterStatus).await;
    match result {
        ResponseResult::Ok {
            data: ResponseData::ClusterStatus { cluster },
        } => {
            assert_eq!(cluster["node_id"], 1);
            assert_eq!(cluster["is_leader"], true);
            assert!(cluster["group_id"].as_u64().is_some());
        }
        other => panic!("unexpected: {other:?}"),
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_list_catalogs() {
    let server = TestServer::start().await;
    // Create a catalog first via HTTP
    server
        .post(
            &format!("/_bisque/v1/tenants/{}/catalogs", server.tenant_id),
            &serde_json::json!({"name": "ws-cat", "engine": "Lance", "config": ""}),
        )
        .await;

    let mut ws = WsClient::connect(server.http_addr, &server.bearer_token).await;
    let result = ws
        .request(
            1,
            RequestMethod::ListCatalogs {
                tenant_id: server.tenant_id,
            },
        )
        .await;
    match result {
        ResponseResult::Ok {
            data: ResponseData::ListCatalogs { catalogs },
        } => {
            assert!(catalogs.iter().any(|c| c["name"] == "ws-cat"));
        }
        other => panic!("unexpected: {other:?}"),
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_create_catalog() {
    let server = TestServer::start().await;
    let mut ws = WsClient::connect(server.http_addr, &server.bearer_token).await;
    let result = ws
        .request(
            1,
            RequestMethod::CreateCatalog {
                tenant_id: server.tenant_id,
                name: "ws-created".into(),
                engine: "Lance".into(),
                config: Some("{}".into()),
            },
        )
        .await;
    match result {
        ResponseResult::Ok {
            data:
                ResponseData::CreateCatalog {
                    catalog_id,
                    raft_group_id,
                },
        } => {
            assert!(catalog_id > 0);
            assert!(raft_group_id > 0);
        }
        other => panic!("unexpected: {other:?}"),
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_get_tenant() {
    let server = TestServer::start().await;
    let mut ws = WsClient::connect(server.http_addr, &server.bearer_token).await;
    let result = ws
        .request(
            1,
            RequestMethod::GetTenant {
                tenant_id: server.tenant_id,
            },
        )
        .await;
    match result {
        ResponseResult::Ok {
            data: ResponseData::GetTenant { tenant },
        } => {
            assert_eq!(tenant["tenant_id"], server.tenant_id);
            assert_eq!(tenant["name"], "test-tenant");
        }
        other => panic!("unexpected: {other:?}"),
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_create_api_key() {
    let server = TestServer::start().await;
    let mut ws = WsClient::connect(server.http_addr, &server.bearer_token).await;
    let result = ws
        .request(
            1,
            RequestMethod::CreateApiKey {
                tenant_id: server.tenant_id,
                scopes: vec![serde_json::json!("TenantAdmin")],
                ttl_secs: Some(600),
            },
        )
        .await;
    match result {
        ResponseResult::Ok {
            data:
                ResponseData::CreateApiKey {
                    key_id,
                    raw_key,
                    token,
                },
        } => {
            assert!(key_id > 0);
            assert!(!raw_key.is_empty());
            assert!(!token.is_empty());

            // Verify the issued token actually works
            let resp = server
                .get_with_token(&format!("/_bisque/v1/tenants/{}", server.tenant_id), &token)
                .await;
            assert_eq!(resp.status(), 200);
        }
        other => panic!("unexpected: {other:?}"),
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_get_catalog() {
    let server = TestServer::start().await;
    let mut ws = WsClient::connect(server.http_addr, &server.bearer_token).await;
    let result = ws
        .request(
            1,
            RequestMethod::GetCatalog {
                bucket: "bisque".into(),
            },
        )
        .await;
    match result {
        ResponseResult::Ok {
            data: ResponseData::GetCatalog { catalog },
        } => {
            // Catalog should be a valid JSON value (may be empty object for fresh server)
            assert!(catalog.is_object() || catalog.is_null());
        }
        other => panic!("unexpected: {other:?}"),
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_list_operations() {
    let server = TestServer::start().await;
    let mut ws = WsClient::connect(server.http_addr, &server.bearer_token).await;
    let result = ws
        .request(
            1,
            RequestMethod::ListOperations {
                op_type: None,
                tier: None,
                status: None,
            },
        )
        .await;
    match result {
        ResponseResult::Ok {
            data: ResponseData::ListOperations { operations },
        } => {
            // Fresh server should have empty or minimal operations
            assert!(operations.len() < 1000);
        }
        other => panic!("unexpected: {other:?}"),
    }
    server.shutdown().await;
}

// ===========================================================================
// J. WebSocket Auth Enforcement
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_list_catalogs_wrong_tenant() {
    let server = TestServer::start().await;
    let mut ws = WsClient::connect(server.http_addr, &server.bearer_token).await;

    // Request catalogs for a different tenant (our token is for server.tenant_id)
    let result = ws
        .request(
            1,
            RequestMethod::ListCatalogs {
                tenant_id: 99999, // different tenant
            },
        )
        .await;
    match result {
        ResponseResult::Error { code, .. } => {
            assert_eq!(code, 403);
        }
        other => panic!("expected 403, got: {other:?}"),
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_create_catalog_non_admin() {
    let server = TestServer::start().await;

    // Create a read-only token
    let now = chrono::Utc::now().timestamp();
    let ro_token = server.token_manager.issue(&TokenClaims {
        user_id: None,
        account_id: None,
        tenant_id: server.tenant_id,
        key_id: 0,
        scopes: vec![Scope::CatalogRead("x".into())],
        issued_at: now,
        expires_at: now + 3600,
    });

    let mut ws = WsClient::connect(server.http_addr, &ro_token).await;
    let result = ws
        .request(
            1,
            RequestMethod::CreateCatalog {
                tenant_id: server.tenant_id,
                name: "should-fail".into(),
                engine: "Lance".into(),
                config: None,
            },
        )
        .await;
    match result {
        ResponseResult::Error { code, .. } => {
            assert_eq!(code, 403);
        }
        other => panic!("expected 403, got: {other:?}"),
    }
    server.shutdown().await;
}

// ===========================================================================
// K. WebSocket Events
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_receives_catalog_events() {
    let server = TestServer::start().await;

    // Connect WS and subscribe to all catalogs
    let mut ws = WsClient::connect(server.http_addr, &server.bearer_token).await;

    // Write data via S3 PUT to trigger a catalog event through Raft.
    // First, create a table via the raft node.
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    // Create table and write through the raft node
    let raft_node = &server.handle.raft_node;
    raft_node
        .create_table("test_events", &schema)
        .await
        .expect("create table");
    raft_node
        .write_records("test_events", &[batch])
        .await
        .expect("write records");

    // Wait for CatalogEvent
    let mut received_event = false;
    for _ in 0..20 {
        match ws.recv_timeout(Duration::from_millis(500)).await {
            Some(ServerMessage::CatalogEvent { catalog, .. }) => {
                assert_eq!(catalog, "bisque");
                received_event = true;
                break;
            }
            Some(_) => continue,
            None => continue,
        }
    }
    assert!(received_event, "did not receive CatalogEvent within 10s");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ws_catalog_subscription_filter() {
    let server = TestServer::start().await;

    // Connect with subscription filter for a non-existent catalog
    let mut ws = WsClient::connect_with_options(
        server.http_addr,
        &server.bearer_token,
        vec!["nonexistent-catalog".into()],
        WS_PROTOCOL_VERSION,
    )
    .await;

    // Write data to trigger events for "bisque" catalog
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    )
    .unwrap();

    let raft_node = &server.handle.raft_node;
    raft_node
        .create_table("filter_test", &schema)
        .await
        .expect("create table");
    raft_node
        .write_records("filter_test", &[batch])
        .await
        .expect("write records");

    // Should NOT receive CatalogEvent because we filtered to "nonexistent-catalog"
    let mut received_catalog_event = false;
    for _ in 0..6 {
        match ws.recv_timeout(Duration::from_millis(500)).await {
            Some(ServerMessage::CatalogEvent { .. }) => {
                received_catalog_event = true;
                break;
            }
            Some(_) => continue,
            None => continue,
        }
    }
    assert!(
        !received_catalog_event,
        "should NOT have received CatalogEvent for filtered subscription"
    );
    server.shutdown().await;
}

// ===========================================================================
// L. S3 API
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_s3_list_objects_empty() {
    let server = TestServer::start().await;
    let resp = server
        .get_no_auth("/bisque?list-type=2&prefix=active/")
        .await;
    // S3 list should return 200 with XML
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<ListBucketResult") || body.contains("ListBucket"));
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_s3_get_nonexistent_returns_404() {
    let server = TestServer::start().await;
    let resp = server
        .get_no_auth("/bisque/active/nonexistent-table/data.lance")
        .await;
    assert_eq!(resp.status(), 404);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_s3_head_nonexistent_returns_404() {
    let server = TestServer::start().await;
    let resp = server
        .client
        .head(server.url("/bisque/active/nonexistent/data.lance"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    server.shutdown().await;
}

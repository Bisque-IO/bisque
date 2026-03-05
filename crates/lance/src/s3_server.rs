//! S3-compatible HTTP server for bisque-lance.
//!
//! Exposes local Lance segment files (active + sealed) via a minimal S3 API,
//! enabling remote bisque-lance instances to query hot/warm data as if it were
//! an S3-compatible object store.
//!
//! # Consistency
//!
//! All file reads go through Lance's own `ObjectStore`, accessed via pinned
//! `Dataset` snapshots. This guarantees:
//! - Reads see a consistent point-in-time view (pinned manifest version)
//! - Copy-on-write semantics: existing data files are never modified in place
//! - No races with concurrent writes, compaction, or segment lifecycle transitions
//!
//! # Supported Operations
//!
//! - **GetObject** (`GET /{bucket}/{key..}`) — read a file with optional byte-range
//! - **HeadObject** (`HEAD /{bucket}/{key..}`) — file metadata
//! - **ListObjectsV2** (`GET /{bucket}?list-type=2`) — directory listing
//! - **Catalog** (`GET /{bucket}/_bisque/catalog`) — segment catalog metadata
//!
//! All write operations return 403 (read-only).

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::TryStreamExt;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::catalog_events::CatalogEventBus;
use crate::engine::BisqueLance;
use crate::manifest::LanceManifestManager;
use crate::version_pins::{PinKey, PinTier, VersionPinTracker};

/// Shared state for the S3-compatible HTTP server.
pub struct S3ServerState {
    engine: Arc<BisqueLance>,
    catalog_events: Option<Arc<CatalogEventBus>>,
    manifest: Option<Arc<LanceManifestManager>>,
    group_id: u64,
    /// Tracks which dataset versions are pinned by remote clients.
    pub version_pins: Arc<VersionPinTracker>,
}

/// Start an S3-compatible HTTP server exposing local Lance segment files.
///
/// Reads are served through Lance's own ObjectStore via pinned Dataset snapshots,
/// guaranteeing consistent point-in-time views even while writes are in progress.
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # async fn run(engine: Arc<bisque_lance::BisqueLance>) {
/// let addr = "0.0.0.0:3300".parse().unwrap();
/// bisque_lance::serve_s3(engine, addr, None, None, 0).await.unwrap();
/// # }
/// ```
pub async fn serve_s3(
    engine: Arc<BisqueLance>,
    addr: std::net::SocketAddr,
    catalog_events: Option<Arc<CatalogEventBus>>,
    manifest: Option<Arc<LanceManifestManager>>,
    group_id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let version_pins = Arc::new(VersionPinTracker::new(std::time::Duration::from_secs(30)));

    // Start background reaper for expired version pin sessions
    let reaper_pins = version_pins.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            let reaped = reaper_pins.reap_expired();
            if reaped > 0 {
                info!(reaped, "Reaped expired version pin sessions");
            }
        }
    });

    let state = Arc::new(S3ServerState {
        engine,
        catalog_events,
        manifest,
        group_id,
        version_pins,
    });

    let app = axum::Router::new()
        // Catalog metadata (custom endpoint)
        .route(
            "/{bucket}/_bisque/catalog",
            axum::routing::get(get_catalog),
        )
        // WebSocket endpoint for real-time catalog events
        .route(
            "/{bucket}/_bisque/ws",
            axum::routing::get(ws_handler),
        )
        // ListObjectsV2 — must come before the wildcard
        .route("/{bucket}", axum::routing::get(list_objects))
        // GetObject / HeadObject
        .route(
            "/{bucket}/{*key}",
            axum::routing::get(get_object).head(head_object),
        )
        .with_state(state);

    info!(%addr, "starting S3-compatible HTTP server");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Key parsing — extract table name, segment tier, and relative path
// ---------------------------------------------------------------------------

/// Parsed S3 key identifying a file within a specific segment dataset.
struct ParsedKey {
    table_name: String,
    tier: SegmentTier,
    /// Path relative to the segment's Lance dataset root.
    relative_path: String,
}

#[derive(Debug, Clone, Copy)]
enum SegmentTier {
    Active,
    Sealed,
}

/// Parse an S3 key like `otel_counters/active/data/foo.lance` into components.
///
/// Key format: `{table_name}/{tier}/{relative_path}`
/// where tier is `active` or `sealed`.
fn parse_key(key: &str) -> Option<ParsedKey> {
    if key.contains("..") {
        return None;
    }

    let mut parts = key.splitn(3, '/');
    let table_name = parts.next()?;
    let tier_str = parts.next()?;
    let relative_path = parts.next().unwrap_or("");

    let tier = match tier_str {
        "active" => SegmentTier::Active,
        "sealed" => SegmentTier::Sealed,
        _ => return None,
    };

    Some(ParsedKey {
        table_name: table_name.to_string(),
        tier,
        relative_path: relative_path.to_string(),
    })
}

/// Resolve a parsed key to a Lance ObjectStore and the path within it.
///
/// Takes a dataset snapshot (pinning the manifest version) and returns the
/// inner `object_store::ObjectStore` plus the resolved path.
async fn resolve_to_object_store(
    state: &S3ServerState,
    parsed: &ParsedKey,
) -> Option<(Arc<dyn ObjectStore>, object_store::path::Path)> {
    let table = state.engine.get_table(&parsed.table_name)?;

    let dataset = match parsed.tier {
        SegmentTier::Active => table.active_dataset_snapshot().await?,
        SegmentTier::Sealed => table.sealed_dataset_snapshot().await?,
    };

    // Get the inner object_store::ObjectStore from the Lance dataset
    let inner_store = dataset.object_store().inner.clone();

    // Build the full path: dataset base + relative path segments
    let base = dataset.branch_location().path;
    let full_path = if parsed.relative_path.is_empty() {
        base
    } else {
        let mut path = base;
        for segment in parsed.relative_path.split('/') {
            if !segment.is_empty() {
                path = path.child(segment);
            }
        }
        path
    };

    Some((inner_store, full_path))
}

// ---------------------------------------------------------------------------
// Catalog endpoint
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct CatalogResponse {
    tables: std::collections::HashMap<String, CatalogTableInfo>,
}

#[derive(Serialize)]
struct CatalogTableInfo {
    active_segment: u64,
    sealed_segment: Option<u64>,
    s3_dataset_uri: String,
    /// Lance dataset version for the active segment (for consistency pinning).
    #[serde(skip_serializing_if = "Option::is_none")]
    active_version: Option<u64>,
    /// Lance dataset version for the sealed segment.
    #[serde(skip_serializing_if = "Option::is_none")]
    sealed_version: Option<u64>,
    /// Non-credential S3 storage options (region, endpoint, etc.) so clients
    /// only need to supply their own credentials.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    s3_storage_options: HashMap<String, String>,
}

async fn get_catalog(
    State(state): State<Arc<S3ServerState>>,
    AxumPath(_bucket): AxumPath<String>,
) -> impl IntoResponse {
    let tables = state.engine.list_tables();
    let mut catalog = std::collections::HashMap::new();

    for name in &tables {
        if let Some(table) = state.engine.get_table(name) {
            let seg_catalog = table.catalog();

            let active_version = table
                .active_dataset_snapshot()
                .await
                .map(|ds| ds.manifest.version);
            let sealed_version = table
                .sealed_dataset_snapshot()
                .await
                .map(|ds| ds.manifest.version);

            // Include non-credential S3 storage options from the table config.
            let s3_storage_options = filter_non_credential_options(
                &table.config().s3_storage_options,
            );

            catalog.insert(
                name.clone(),
                CatalogTableInfo {
                    active_segment: seg_catalog.active_segment,
                    sealed_segment: seg_catalog.sealed_segment,
                    s3_dataset_uri: seg_catalog.s3_dataset_uri.clone(),
                    active_version,
                    sealed_version,
                    s3_storage_options,
                },
            );
        }
    }

    axum::Json(CatalogResponse { tables: catalog })
}

/// Filter S3 storage options to exclude credentials.
///
/// Returns only non-sensitive options (region, endpoint, etc.) that are safe
/// to serve to clients. Clients provide their own credentials.
fn filter_non_credential_options(options: &HashMap<String, String>) -> HashMap<String, String> {
    options
        .iter()
        .filter(|(k, _)| {
            let k = k.to_lowercase();
            // Exclude credential keys
            !(k.contains("key")
                || k.contains("secret")
                || k.contains("token")
                || k.contains("password")
                || k.contains("credential"))
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

// ---------------------------------------------------------------------------
// GetObject
// ---------------------------------------------------------------------------

async fn get_object(
    State(state): State<Arc<S3ServerState>>,
    AxumPath((_bucket, key)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, Response> {
    let parsed = parse_key(&key).ok_or_else(|| s3_not_found(&key))?;

    let (store, path) =
        resolve_to_object_store(&state, &parsed)
            .await
            .ok_or_else(|| s3_not_found(&key))?;

    // Build GetOptions from Range header
    let get_opts = if let Some(range_header) = headers.get("range") {
        let range_str = range_header.to_str().map_err(|_| s3_invalid_range())?;
        let range = parse_range_to_get_range(range_str)?;
        object_store::GetOptions {
            range: Some(range),
            ..Default::default()
        }
    } else {
        Default::default()
    };

    let result = store
        .get_opts(&path, get_opts)
        .await
        .map_err(|e| match e {
            object_store::Error::NotFound { .. } => s3_not_found(&key),
            _ => s3_internal_error(&e.to_string()),
        })?;

    let meta = &result.meta;
    let total_size = meta.size;
    let etag = meta
        .e_tag
        .as_deref()
        .unwrap_or("\"unknown\"")
        .to_string();
    let last_modified = meta.last_modified.to_rfc2822();
    let range = result.range.clone();
    let content_length = range.end - range.start;

    // Stream the payload directly — no buffering
    let stream = result.into_stream();
    let body = Body::from_stream(stream);

    // Determine if this is a partial response
    let is_partial = range.start > 0 || range.end < total_size;

    let mut builder = Response::builder()
        .header("content-length", content_length.to_string())
        .header("content-type", "application/octet-stream")
        .header("etag", &etag)
        .header("accept-ranges", "bytes")
        .header("last-modified", &last_modified);

    if is_partial {
        builder = builder
            .status(StatusCode::PARTIAL_CONTENT)
            .header(
                "content-range",
                format!(
                    "bytes {}-{}/{}",
                    range.start,
                    range.end.saturating_sub(1),
                    total_size
                ),
            );
    } else {
        builder = builder.status(StatusCode::OK);
    }

    Ok(builder.body(body).unwrap())
}

/// Parse an HTTP Range header into an `object_store::GetRange`.
fn parse_range_to_get_range(range_str: &str) -> Result<object_store::GetRange, Response> {
    let range_str = range_str
        .strip_prefix("bytes=")
        .ok_or_else(s3_invalid_range)?;

    let parts: Vec<&str> = range_str.splitn(2, '-').collect();
    if parts.len() != 2 {
        return Err(s3_invalid_range());
    }

    if parts[0].is_empty() {
        // Suffix range: bytes=-N
        let n: u64 = parts[1].parse().map_err(|_| s3_invalid_range())?;
        Ok(object_store::GetRange::Suffix(n))
    } else if parts[1].is_empty() {
        // Open-ended: bytes=N-
        let offset: u64 = parts[0].parse().map_err(|_| s3_invalid_range())?;
        Ok(object_store::GetRange::Offset(offset))
    } else {
        // Bounded: bytes=start-end
        let start: u64 = parts[0].parse().map_err(|_| s3_invalid_range())?;
        let end: u64 = parts[1].parse().map_err(|_| s3_invalid_range())?;
        Ok(object_store::GetRange::Bounded(start..end + 1))
    }
}

// ---------------------------------------------------------------------------
// HeadObject
// ---------------------------------------------------------------------------

async fn head_object(
    State(state): State<Arc<S3ServerState>>,
    AxumPath((_bucket, key)): AxumPath<(String, String)>,
) -> Result<Response, Response> {
    let parsed = parse_key(&key).ok_or_else(|| s3_not_found(&key))?;

    let (store, path) =
        resolve_to_object_store(&state, &parsed)
            .await
            .ok_or_else(|| s3_not_found(&key))?;

    let meta = store.head(&path).await.map_err(|e| match e {
        object_store::Error::NotFound { .. } => s3_not_found(&key),
        _ => s3_internal_error(&e.to_string()),
    })?;

    let etag = meta.e_tag.as_deref().unwrap_or("\"unknown\"");
    let last_modified = meta.last_modified.to_rfc2822();

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-length", meta.size.to_string())
        .header("content-type", "application/octet-stream")
        .header("etag", etag)
        .header("accept-ranges", "bytes")
        .header("last-modified", &last_modified)
        .body(Body::empty())
        .unwrap())
}

// ---------------------------------------------------------------------------
// ListObjectsV2
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ListParams {
    #[serde(default, rename = "list-type")]
    _list_type: Option<String>,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    delimiter: Option<String>,
    #[serde(default, rename = "max-keys")]
    max_keys: Option<usize>,
    #[serde(default, rename = "continuation-token")]
    _continuation_token: Option<String>,
}

async fn list_objects(
    State(state): State<Arc<S3ServerState>>,
    AxumPath(bucket): AxumPath<String>,
    Query(params): Query<ListParams>,
) -> Result<Response, Response> {
    let prefix = params.prefix.unwrap_or_default();
    let delimiter = params.delimiter.unwrap_or_default();
    let max_keys = params.max_keys.unwrap_or(1000).min(10000);

    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut seen_prefixes = std::collections::HashSet::new();

    // For each table, list files from active and sealed dataset snapshots
    let tables = state.engine.list_tables();

    for table_name in &tables {
        if objects.len() >= max_keys {
            break;
        }

        let table = match state.engine.get_table(table_name) {
            Some(t) => t,
            None => continue,
        };

        // List files from each tier
        for (tier_name, dataset_opt) in [
            ("active", table.active_dataset_snapshot().await),
            ("sealed", table.sealed_dataset_snapshot().await),
        ] {
            if objects.len() >= max_keys {
                break;
            }

            let dataset = match dataset_opt {
                Some(ds) => ds,
                None => continue,
            };

            let inner_store = dataset.object_store().inner.clone();
            let base = dataset.branch_location().path;

            // List all files under this dataset
            let list_stream = inner_store.list(Some(&base));

            let items: Vec<_> = match list_stream.try_collect().await {
                Ok(items) => items,
                Err(e) => {
                    warn!(table = %table_name, tier = %tier_name, error = %e, "failed to list segment files");
                    continue;
                }
            };

            for meta in items {
                if objects.len() >= max_keys {
                    break;
                }

                // Convert ObjectStore path to our S3 key format:
                // {table_name}/{tier}/{relative_path_within_dataset}
                let obj_path = meta.location.as_ref();
                let relative = if let Some(rest) = obj_path.strip_prefix(base.as_ref()) {
                    let rest = rest.strip_prefix('/').unwrap_or(rest);
                    format!("{}/{}/{}", table_name, tier_name, rest)
                } else {
                    format!("{}/{}/{}", table_name, tier_name, obj_path)
                };

                // Apply prefix filter
                if !relative.starts_with(&prefix) {
                    continue;
                }

                // Apply delimiter logic
                if !delimiter.is_empty() {
                    let rest_after_prefix = &relative[prefix.len()..];
                    if let Some(idx) = rest_after_prefix.find(&delimiter) {
                        let cp = format!("{}{}", &prefix, &rest_after_prefix[..=idx]);
                        if seen_prefixes.insert(cp.clone()) {
                            common_prefixes.push(cp);
                        }
                        continue;
                    }
                }

                let last_modified = meta
                    .last_modified
                    .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                    .to_string();

                objects.push(S3Object {
                    key: relative,
                    size: meta.size,
                    last_modified,
                    etag: meta.e_tag.unwrap_or_default(),
                });
            }
        }
    }

    let is_truncated = objects.len() > max_keys;
    if is_truncated {
        objects.truncate(max_keys);
    }

    let next_token = if is_truncated {
        objects.last().map(|o| o.key.clone())
    } else {
        None
    };

    Ok(list_objects_xml_response(
        &bucket,
        &prefix,
        &objects,
        &common_prefixes,
        is_truncated,
        next_token.as_deref(),
    ))
}

struct S3Object {
    key: String,
    size: u64,
    last_modified: String,
    etag: String,
}

fn list_objects_xml_response(
    bucket: &str,
    prefix: &str,
    objects: &[S3Object],
    common_prefixes: &[String],
    is_truncated: bool,
    next_continuation_token: Option<&str>,
) -> Response {
    let mut xml = String::with_capacity(4096);
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    xml.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    xml.push_str(&format!("<Name>{}</Name>", escape_xml(bucket)));
    xml.push_str(&format!("<Prefix>{}</Prefix>", escape_xml(prefix)));
    xml.push_str(&format!("<KeyCount>{}</KeyCount>", objects.len()));
    xml.push_str(&format!(
        "<MaxKeys>{}</MaxKeys>",
        objects.len().max(1000)
    ));
    xml.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        is_truncated
    ));

    if let Some(token) = next_continuation_token {
        xml.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            escape_xml(token)
        ));
    }

    for obj in objects {
        xml.push_str("<Contents>");
        xml.push_str(&format!("<Key>{}</Key>", escape_xml(&obj.key)));
        xml.push_str(&format!(
            "<LastModified>{}</LastModified>",
            &obj.last_modified
        ));
        xml.push_str(&format!("<ETag>{}</ETag>", escape_xml(&obj.etag)));
        xml.push_str(&format!("<Size>{}</Size>", obj.size));
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        xml.push_str("</Contents>");
    }

    for cp in common_prefixes {
        xml.push_str("<CommonPrefixes>");
        xml.push_str(&format!("<Prefix>{}</Prefix>", escape_xml(cp)));
        xml.push_str("</CommonPrefixes>");
    }

    xml.push_str("</ListBucketResult>");

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

// ---------------------------------------------------------------------------
// WebSocket — real-time catalog event push
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct WsParams {
    /// Resume from this sequence number. Events after `since` will be replayed.
    #[serde(default)]
    since: Option<u64>,
}

async fn ws_handler(
    State(state): State<Arc<S3ServerState>>,
    AxumPath(_bucket): AxumPath<String>,
    Query(params): Query<WsParams>,
    upgrade: fastwebsockets::upgrade::IncomingUpgrade,
) -> Response {
    let bus = match &state.catalog_events {
        Some(bus) => bus.clone(),
        None => {
            return Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(Body::from("catalog events not configured"))
                .unwrap();
        }
    };

    let (response, fut) = match upgrade.upgrade() {
        Ok((response, fut)) => (response, fut),
        Err(e) => {
            warn!("WebSocket upgrade failed: {}", e);
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("WebSocket upgrade failed: {}", e)))
                .unwrap();
        }
    };

    let manifest = state.manifest.clone();
    let group_id = state.group_id;
    let since = params.since;
    let pins = state.version_pins.clone();
    let session_id = pins.create_session();

    tokio::spawn(async move {
        match fut.await {
            Ok(ws) => {
                if let Err(e) = handle_ws_connection(ws, bus, manifest, group_id, since, pins.clone(), session_id).await {
                    debug!("WebSocket connection ended: {}", e);
                }
            }
            Err(e) => {
                warn!("WebSocket handshake failed: {}", e);
            }
        }
        // Always clean up session pins on disconnect
        pins.remove_session(session_id);
    });

    response.into_response()
}

/// JSON messages clients can send for version pinning.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientWsMessage {
    Pin { table: String, tier: String, version: u64 },
    Unpin { table: String, tier: String, version: u64 },
    Heartbeat,
}

async fn handle_ws_connection<S>(
    mut ws: fastwebsockets::WebSocket<S>,
    bus: Arc<CatalogEventBus>,
    manifest: Option<Arc<LanceManifestManager>>,
    group_id: u64,
    since: Option<u64>,
    pins: Arc<VersionPinTracker>,
    session_id: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    use fastwebsockets::{Frame, OpCode, Payload};

    // Send session_id to client so it knows its identity
    let hello = serde_json::json!({"type": "session", "session_id": session_id});
    let hello_bytes = hello.to_string();
    ws.write_frame(Frame::text(Payload::Borrowed(hello_bytes.as_bytes())))
        .await?;

    // Replay WAL events if `since` was provided and we have a manifest
    if let (Some(since_seq), Some(manifest)) = (since, &manifest) {
        match manifest.read_wal_since(group_id, since_seq) {
            Ok(events) => {
                for event in events {
                    let json = serde_json::to_string(&event)?;
                    ws.write_frame(Frame::text(Payload::Borrowed(json.as_bytes())))
                        .await?;
                }
            }
            Err(e) => {
                warn!("Failed to read WAL for replay: {}", e);
                let msg = r#"{"type":"snapshot_required"}"#;
                ws.write_frame(Frame::text(Payload::Borrowed(msg.as_bytes())))
                    .await?;
            }
        }
    }

    // Subscribe to live events
    let mut rx = bus.subscribe();

    // Set up fragmented read for bidirectional communication
    ws.set_auto_pong(true);

    loop {
        tokio::select! {
            // Forward catalog events to client
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        let json = serde_json::to_string(&event)?;
                        ws.write_frame(Frame::text(Payload::Borrowed(json.as_bytes())))
                            .await?;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(lagged = n, "WebSocket subscriber lagged, sending snapshot_required");
                        let msg = r#"{"type":"snapshot_required"}"#;
                        ws.write_frame(Frame::text(Payload::Borrowed(msg.as_bytes())))
                            .await?;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
            // Handle client messages (pin/unpin/heartbeat)
            frame = ws.read_frame() => {
                let frame = frame?;
                match frame.opcode {
                    OpCode::Text => {
                        let text = std::str::from_utf8(&frame.payload)?;
                        handle_client_pin_message(text, &pins, session_id);
                    }
                    OpCode::Close => {
                        debug!(session_id, "Client sent close frame");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

fn handle_client_pin_message(text: &str, pins: &VersionPinTracker, session_id: u64) {
    let msg: ClientWsMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(_) => {
            debug!(session_id, "Ignoring unrecognized client WS message");
            return;
        }
    };

    match msg {
        ClientWsMessage::Pin { table, tier, version } => {
            if let Some(tier) = PinTier::from_str(&tier) {
                debug!(session_id, %table, %version, "Client pinned version");
                pins.pin(session_id, PinKey { table, tier, version });
            }
        }
        ClientWsMessage::Unpin { table, tier, version } => {
            if let Some(tier) = PinTier::from_str(&tier) {
                debug!(session_id, %table, %version, "Client unpinned version");
                pins.unpin(session_id, PinKey { table, tier, version });
            }
        }
        ClientWsMessage::Heartbeat => {
            pins.heartbeat(session_id);
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

// ---------------------------------------------------------------------------
// S3 error responses
// ---------------------------------------------------------------------------

fn s3_not_found(key: &str) -> Response {
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Key>{}</Key>
</Error>"#,
        escape_xml(key)
    );

    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("content-type", "application/xml")
        .body(Body::from(body))
        .unwrap()
}

#[allow(dead_code)]
fn s3_access_denied(message: &str) -> Response {
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>AccessDenied</Code>
  <Message>{}</Message>
</Error>"#,
        escape_xml(message)
    );

    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header("content-type", "application/xml")
        .body(Body::from(body))
        .unwrap()
}

fn s3_invalid_range() -> Response {
    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>InvalidRange</Code>
  <Message>The requested range is not satisfiable.</Message>
</Error>"#;

    Response::builder()
        .status(StatusCode::RANGE_NOT_SATISFIABLE)
        .header("content-type", "application/xml")
        .body(Body::from(body))
        .unwrap()
}

fn s3_internal_error(message: &str) -> Response {
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>InternalError</Code>
  <Message>{}</Message>
</Error>"#,
        escape_xml(message)
    );

    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header("content-type", "application/xml")
        .body(Body::from(body))
        .unwrap()
}

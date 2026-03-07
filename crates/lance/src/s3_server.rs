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

use lance_index::DatasetIndexExt;

use bisque_meta::mesh::{ClusterMesh, OperationSnapshot};

use crate::catalog_events::{CatalogEvent, CatalogEventBus};
use crate::engine::BisqueLance;
use crate::manifest::LanceManifestManager;
use crate::operations::{OpStatus, OpTier, OpType, Operation, OperationsManager};
use crate::version_pins::{PinKey, PinTier, VersionPinTracker};

/// Shared state for the S3-compatible HTTP server.
pub struct S3ServerState {
    engine: Arc<BisqueLance>,
    catalog_events: Option<Arc<CatalogEventBus>>,
    manifest: Option<Arc<LanceManifestManager>>,
    group_id: u64,
    /// Tracks which dataset versions are pinned by remote clients.
    pub version_pins: Arc<VersionPinTracker>,
    /// Background operations manager for reindex/compact.
    pub operations: Arc<OperationsManager>,
    /// Optional cluster mesh for cross-node operation visibility.
    pub mesh: Option<ClusterMesh>,
}

impl S3ServerState {
    /// Create a new S3 server state and spawn the version-pin reaper task.
    pub fn new(
        engine: Arc<BisqueLance>,
        catalog_events: Option<Arc<CatalogEventBus>>,
        manifest: Option<Arc<LanceManifestManager>>,
        group_id: u64,
        mesh: Option<ClusterMesh>,
    ) -> Arc<Self> {
        // M3: 60s lease timeout — gives clients enough time between heartbeats (15s interval).
        let version_pins = Arc::new(VersionPinTracker::new(std::time::Duration::from_secs(60)));

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

        let operations = OperationsManager::new(group_id, 4, 2);

        Arc::new(Self {
            engine,
            catalog_events,
            manifest,
            group_id,
            version_pins,
            operations,
            mesh,
        })
    }

    // -----------------------------------------------------------------------
    // Public facade for the unified WebSocket handler (lives in bisque crate)
    // -----------------------------------------------------------------------

    /// Current catalog event bus sequence number.
    pub fn catalog_seq(&self) -> u64 {
        self.catalog_events
            .as_ref()
            .map(|b| b.current_seq())
            .unwrap_or(0)
    }

    /// Subscribe to the catalog event broadcast channel.
    pub fn subscribe_catalog_events(
        &self,
    ) -> Option<tokio::sync::broadcast::Receiver<CatalogEvent>> {
        self.catalog_events.as_ref().map(|b| b.subscribe())
    }

    /// Replay WAL entries since the given sequence number.
    pub fn replay_wal_since(&self, since_seq: u64) -> Option<std::io::Result<Vec<CatalogEvent>>> {
        let manifest = self.manifest.as_ref()?;
        Some(manifest.read_wal_since(self.group_id, since_seq))
    }

    /// Collect all operations (local + remote mesh) capped at `max`.
    pub fn collect_operations(&self, max: usize) -> Vec<Operation> {
        let mut all_ops = self.operations.list(None, None, None);
        if let Some(ref mesh) = self.mesh {
            for snap in &mesh.cluster_state().all_remote_operations() {
                if all_ops.len() >= max {
                    break;
                }
                all_ops.push(snapshot_to_operation(snap));
            }
        }
        all_ops.truncate(max);
        all_ops
    }

    /// List operations with optional filters, including remote mesh operations.
    pub fn list_operations_filtered(
        &self,
        op_type: Option<OpType>,
        tier: Option<OpTier>,
        status: Option<OpStatus>,
    ) -> Vec<Operation> {
        let mut ops = self.operations.list(op_type, tier, status);
        if let Some(ref mesh) = self.mesh {
            for snap in &mesh.cluster_state().all_remote_operations() {
                let op = snapshot_to_operation(snap);
                if let Some(t) = op_type {
                    if op.op_type != t {
                        continue;
                    }
                }
                if let Some(t) = tier {
                    if op.tier != t {
                        continue;
                    }
                }
                if let Some(s) = status {
                    if op.status != s {
                        continue;
                    }
                }
                ops.push(op);
            }
        }
        ops
    }

    /// Submit a reindex operation. Returns `Ok(op_id)` or `Err(message)`.
    pub fn submit_reindex(&self, bucket: String, table: String) -> Result<String, String> {
        if self.engine.get_table(&table).is_none() {
            return Err(format!("table '{}' not found", table));
        }
        let op_id = self.operations.submit_reindex(
            String::new(),
            bucket,
            "Lance".to_string(),
            table,
            self.engine.clone(),
        );
        Ok(op_id)
    }

    /// Submit a compact operation. Returns `Ok(op_id)` or `Err(message)`.
    pub fn submit_compact(&self, bucket: String, table: String) -> Result<String, String> {
        if self.engine.get_table(&table).is_none() {
            return Err(format!("table '{}' not found", table));
        }
        let op_id = self.operations.submit_compact(
            String::new(),
            bucket,
            "Lance".to_string(),
            table,
            self.engine.clone(),
        );
        Ok(op_id)
    }

    /// Get the full catalog as a JSON value.
    pub async fn get_catalog_json(&self) -> serde_json::Value {
        let tables = self.engine.list_tables();
        let mut catalog = std::collections::HashMap::new();

        for name in &tables {
            if let Some(table) = self.engine.get_table(name) {
                let seg_catalog = table.catalog();
                let active_version = table
                    .active_dataset_snapshot()
                    .await
                    .map(|ds| ds.manifest.version);
                let sealed_ds = table.sealed_dataset_snapshot().await;
                let sealed_version = sealed_ds.as_ref().map(|ds| ds.manifest.version);
                let indexes = if let Some(ds) = &sealed_ds {
                    collect_index_info(ds).await
                } else {
                    Vec::new()
                };
                let s3_storage_options =
                    filter_non_credential_options(&table.config().s3_storage_options);

                catalog.insert(
                    name.clone(),
                    CatalogTableInfo {
                        active_segment: seg_catalog.active_segment,
                        sealed_segment: seg_catalog.sealed_segment,
                        s3_dataset_uri: seg_catalog.s3_dataset_uri.clone(),
                        active_version,
                        sealed_version,
                        s3_storage_options,
                        indexes,
                    },
                );
            }
        }

        serde_json::to_value(CatalogResponse { tables: catalog }).unwrap_or_default()
    }
}

/// Build the S3-compatible axum Router without starting a listener.
///
/// Used by the `bisque` crate to compose into a unified server.
pub fn s3_router(state: Arc<S3ServerState>) -> axum::Router {
    axum::Router::new()
        .route(
            "/_bisque/v1/operations",
            axum::routing::get(list_operations),
        )
        .route(
            "/_bisque/v1/operations/{op_id}",
            axum::routing::get(get_operation).delete(cancel_operation),
        )
        .route("/{bucket}/_bisque/catalog", axum::routing::get(get_catalog))
        .route(
            "/{bucket}/_bisque/reindex/{table}",
            axum::routing::post(reindex_table),
        )
        .route(
            "/{bucket}/_bisque/compact/{table}",
            axum::routing::post(compact_table),
        )
        .route("/{bucket}", axum::routing::get(list_objects))
        .route(
            "/{bucket}/{*key}",
            axum::routing::get(get_object).head(head_object),
        )
        .with_state(state)
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
    let state = S3ServerState::new(engine, catalog_events, manifest, group_id, None);
    let app = s3_router(state);

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
    /// Index metadata for the sealed segment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    indexes: Vec<CatalogIndexInfo>,
}

#[derive(Serialize, Default)]
struct CatalogIndexInfo {
    name: String,
    columns: Vec<String>,
    index_type: String,
    dataset_version: u64,
    fragment_count: u64,
    total_fragments: u64,
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

            let sealed_ds = table.sealed_dataset_snapshot().await;
            let sealed_version = sealed_ds.as_ref().map(|ds| ds.manifest.version);

            // Collect index metadata from the sealed dataset.
            let indexes = if let Some(ds) = &sealed_ds {
                collect_index_info(ds).await
            } else {
                Vec::new()
            };

            // Include non-credential S3 storage options from the table config.
            let s3_storage_options =
                filter_non_credential_options(&table.config().s3_storage_options);

            catalog.insert(
                name.clone(),
                CatalogTableInfo {
                    active_segment: seg_catalog.active_segment,
                    sealed_segment: seg_catalog.sealed_segment,
                    s3_dataset_uri: seg_catalog.s3_dataset_uri.clone(),
                    active_version,
                    sealed_version,
                    s3_storage_options,
                    indexes,
                },
            );
        }
    }

    metrics::counter!("bisque_requests_total", "protocol" => "s3", "op" => "catalog").increment(1);
    axum::Json(CatalogResponse { tables: catalog })
}

/// Collect index metadata from a Lance dataset.
async fn collect_index_info(ds: &lance::dataset::Dataset) -> Vec<CatalogIndexInfo> {
    let indices = match ds.load_indices().await {
        Ok(idx) => idx,
        Err(e) => {
            warn!("Failed to load indices: {}", e);
            return Vec::new();
        }
    };

    let schema = ds.schema();
    let total_fragments = ds.get_fragments().len() as u64;

    indices
        .iter()
        .filter(|idx| !idx.name.starts_with("__"))
        .map(|idx| {
            let columns: Vec<String> = idx
                .fields
                .iter()
                .filter_map(|field_id| schema.field_by_id(*field_id).map(|f| f.name.clone()))
                .collect();

            let index_type = idx
                .index_details
                .as_ref()
                .map(|d| {
                    // Extract type name from protobuf type_url like "/lance.table.BTreeIndexDetails"
                    d.type_url
                        .rsplit('.')
                        .next()
                        .unwrap_or(&d.type_url)
                        .strip_suffix("IndexDetails")
                        .unwrap_or(&d.type_url)
                        .to_string()
                })
                .unwrap_or_else(|| "Unknown".to_string());

            let fragment_count = idx
                .fragment_bitmap
                .as_ref()
                .map(|bm| bm.len() as u64)
                .unwrap_or(0);

            CatalogIndexInfo {
                name: idx.name.clone(),
                columns,
                index_type,
                dataset_version: idx.dataset_version,
                fragment_count,
                total_fragments,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Operations endpoints
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct SubmitResponse {
    op_id: String,
    message: String,
}

/// Queue a reindex operation on a table's S3 (cold) dataset.
///
/// POST `/{bucket}/_bisque/reindex/{table}`
async fn reindex_table(
    State(state): State<Arc<S3ServerState>>,
    AxumPath((bucket, table_name)): AxumPath<(String, String)>,
) -> Result<Response, Response> {
    state
        .engine
        .get_table(&table_name)
        .ok_or_else(|| s3_not_found(&table_name))?;

    let op_id = state.operations.submit_reindex(
        String::new(),
        bucket,
        "Lance".to_string(),
        table_name.clone(),
        state.engine.clone(),
    );

    metrics::counter!("bisque_requests_total", "protocol" => "s3", "op" => "reindex").increment(1);

    Ok(axum::Json(SubmitResponse {
        op_id,
        message: format!("Reindex queued for table '{}'", table_name),
    })
    .into_response())
}

/// Queue a compaction operation on a table's S3 (cold) dataset.
///
/// POST `/{bucket}/_bisque/compact/{table}`
async fn compact_table(
    State(state): State<Arc<S3ServerState>>,
    AxumPath((bucket, table_name)): AxumPath<(String, String)>,
) -> Result<Response, Response> {
    state
        .engine
        .get_table(&table_name)
        .ok_or_else(|| s3_not_found(&table_name))?;

    let op_id = state.operations.submit_compact(
        String::new(),
        bucket,
        "Lance".to_string(),
        table_name.clone(),
        state.engine.clone(),
    );

    metrics::counter!("bisque_requests_total", "protocol" => "s3", "op" => "compact").increment(1);

    Ok(axum::Json(SubmitResponse {
        op_id,
        message: format!("Compaction queued for table '{}'", table_name),
    })
    .into_response())
}

#[derive(Deserialize)]
struct ListOpsParams {
    #[serde(default, rename = "type")]
    op_type: Option<String>,
    #[serde(default)]
    tier: Option<String>,
    #[serde(default)]
    status: Option<String>,
}

/// Convert a mesh `OperationSnapshot` into a local `Operation` for JSON serialization.
fn snapshot_to_operation(snap: &OperationSnapshot) -> Operation {
    Operation {
        id: snap.id.clone(),
        node_id: snap.node_id,
        op_type: match snap.op_type {
            0 => OpType::Compact,
            1 => OpType::Reindex,
            _ => OpType::Flush,
        },
        tier: match snap.tier {
            0 => OpTier::Hot,
            1 => OpTier::Warm,
            _ => OpTier::Cold,
        },
        tenant: snap.tenant.clone(),
        catalog: snap.catalog.clone(),
        catalog_type: snap.catalog_type.clone(),
        table: snap.table.clone(),
        status: match snap.status {
            0 => OpStatus::Queued,
            1 => OpStatus::Running,
            2 => OpStatus::Done,
            3 => OpStatus::Failed,
            _ => OpStatus::Cancelled,
        },
        // L6: Guard against NaN/Inf from corrupted progress_bits.
        progress: {
            let v = f64::from_bits(snap.progress_bits);
            if v.is_finite() { v } else { 0.0 }
        },
        created_at: snap.created_at.clone(),
        started_at: snap.started_at.clone(),
        finished_at: snap.finished_at.clone(),
        error: snap.error.clone(),
        fragments_done: snap.fragments_done,
        fragments_total: snap.fragments_total,
    }
}

/// Convert a local `Operation` into a mesh `OperationSnapshot` for broadcasting.
fn operation_to_snapshot(op: &Operation) -> OperationSnapshot {
    OperationSnapshot {
        id: op.id.clone(),
        node_id: op.node_id,
        op_type: match op.op_type {
            OpType::Compact => 0,
            OpType::Reindex => 1,
            OpType::Flush => 2,
        },
        tier: match op.tier {
            OpTier::Hot => 0,
            OpTier::Warm => 1,
            OpTier::Cold => 2,
        },
        tenant: op.tenant.clone(),
        catalog: op.catalog.clone(),
        catalog_type: op.catalog_type.clone(),
        table: op.table.clone(),
        status: match op.status {
            OpStatus::Queued => 0,
            OpStatus::Running => 1,
            OpStatus::Done => 2,
            OpStatus::Failed => 3,
            OpStatus::Cancelled => 4,
        },
        progress_bits: op.progress.to_bits(),
        created_at: op.created_at.clone(),
        started_at: op.started_at.clone(),
        finished_at: op.finished_at.clone(),
        error: op.error.clone(),
        fragments_done: op.fragments_done,
        fragments_total: op.fragments_total,
    }
}

/// List all operations, optionally filtered by type, tier, and status.
///
/// GET `/_bisque/v1/operations?type=reindex&tier=cold&status=running`
async fn list_operations(
    State(state): State<Arc<S3ServerState>>,
    Query(params): Query<ListOpsParams>,
) -> impl IntoResponse {
    let op_type = params.op_type.and_then(|t| match t.as_str() {
        "reindex" => Some(OpType::Reindex),
        "compact" => Some(OpType::Compact),
        "flush" => Some(OpType::Flush),
        _ => None,
    });

    let tier = params.tier.and_then(|t| match t.as_str() {
        "hot" => Some(OpTier::Hot),
        "warm" => Some(OpTier::Warm),
        "cold" => Some(OpTier::Cold),
        _ => None,
    });

    let status = params.status.and_then(|s| match s.as_str() {
        "queued" => Some(OpStatus::Queued),
        "running" => Some(OpStatus::Running),
        "done" => Some(OpStatus::Done),
        "failed" => Some(OpStatus::Failed),
        "cancelled" => Some(OpStatus::Cancelled),
        _ => None,
    });

    let mut ops = state.operations.list(op_type, tier, status);

    // Merge remote operations from the cluster mesh
    if let Some(mesh) = &state.mesh {
        let remote_snapshots = mesh.cluster_state().all_remote_operations();
        for snap in &remote_snapshots {
            let op = snapshot_to_operation(snap);
            // Apply same filters
            if let Some(t) = op_type {
                if op.op_type != t {
                    continue;
                }
            }
            if let Some(t) = tier {
                if op.tier != t {
                    continue;
                }
            }
            if let Some(s) = status {
                if op.status != s {
                    continue;
                }
            }
            ops.push(op);
        }
    }

    // Sort: running first, then queued, then by created_at desc
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

    axum::Json(ops)
}

/// Get a single operation by ID.
///
/// GET `/_bisque/v1/operations/{op_id}`
async fn get_operation(
    State(state): State<Arc<S3ServerState>>,
    AxumPath(op_id): AxumPath<String>,
) -> Result<Response, Response> {
    let op = state
        .operations
        .get(&op_id)
        .ok_or_else(|| s3_not_found(&op_id))?;

    Ok(axum::Json(op).into_response())
}

/// Cancel a queued operation.
///
/// DELETE `/_bisque/v1/operations/{op_id}`
async fn cancel_operation(
    State(state): State<Arc<S3ServerState>>,
    AxumPath(op_id): AxumPath<String>,
) -> Result<Response, Response> {
    if state.operations.cancel(&op_id) {
        Ok(axum::Json(serde_json::json!({
            "message": "Operation cancelled",
            "op_id": op_id,
        }))
        .into_response())
    } else {
        Err(Response::builder()
            .status(StatusCode::CONFLICT)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "error": "Operation cannot be cancelled (not in queued state)"
                })
                .to_string(),
            ))
            .unwrap())
    }
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
    let req_start = std::time::Instant::now();
    let parsed = parse_key(&key).ok_or_else(|| s3_not_found(&key))?;

    let (store, path) = resolve_to_object_store(&state, &parsed)
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

    let result = store.get_opts(&path, get_opts).await.map_err(|e| match e {
        object_store::Error::NotFound { .. } => s3_not_found(&key),
        _ => s3_internal_error(&e.to_string()),
    })?;

    let meta = &result.meta;
    let total_size = meta.size;
    let etag = meta.e_tag.as_deref().unwrap_or("\"unknown\"").to_string();
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
        builder = builder.status(StatusCode::PARTIAL_CONTENT).header(
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

    metrics::counter!("bisque_requests_total", "protocol" => "s3", "op" => "get").increment(1);
    metrics::histogram!("bisque_request_latency_seconds", "protocol" => "s3", "op" => "get")
        .record(req_start.elapsed().as_secs_f64());
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
    let req_start = std::time::Instant::now();
    let parsed = parse_key(&key).ok_or_else(|| s3_not_found(&key))?;

    let (store, path) = resolve_to_object_store(&state, &parsed)
        .await
        .ok_or_else(|| s3_not_found(&key))?;

    let meta = store.head(&path).await.map_err(|e| match e {
        object_store::Error::NotFound { .. } => s3_not_found(&key),
        _ => s3_internal_error(&e.to_string()),
    })?;

    let etag = meta.e_tag.as_deref().unwrap_or("\"unknown\"");
    let last_modified = meta.last_modified.to_rfc2822();

    metrics::counter!("bisque_requests_total", "protocol" => "s3", "op" => "head").increment(1);
    metrics::histogram!("bisque_request_latency_seconds", "protocol" => "s3", "op" => "head")
        .record(req_start.elapsed().as_secs_f64());
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
    metrics::counter!("bisque_requests_total", "protocol" => "s3", "op" => "list").increment(1);
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
    xml.push_str(&format!("<MaxKeys>{}</MaxKeys>", objects.len().max(1000)));
    xml.push_str(&format!("<IsTruncated>{}</IsTruncated>", is_truncated));

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
// Helpers
// ---------------------------------------------------------------------------

// NOTE: The unified WebSocket handler has been moved to the bisque crate
// (bisque/src/ws.rs) where it belongs as global server infrastructure.
// Engine-specific data is accessed via the S3ServerState public facade methods.

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

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // parse_key
    // -----------------------------------------------------------------------

    #[test]
    fn parse_key_valid_active_key() {
        let parsed = parse_key("my_table/active/data/foo.lance").unwrap();
        assert_eq!(parsed.table_name, "my_table");
        assert!(matches!(parsed.tier, SegmentTier::Active));
        assert_eq!(parsed.relative_path, "data/foo.lance");
    }

    #[test]
    fn parse_key_valid_sealed_key() {
        let parsed = parse_key("otel_counters/sealed/indices/idx.lance").unwrap();
        assert_eq!(parsed.table_name, "otel_counters");
        assert!(matches!(parsed.tier, SegmentTier::Sealed));
        assert_eq!(parsed.relative_path, "indices/idx.lance");
    }

    #[test]
    fn parse_key_rejects_path_traversal() {
        assert!(parse_key("table/../etc/passwd").is_none());
        assert!(parse_key("table/active/../../secret").is_none());
    }

    #[test]
    fn parse_key_rejects_unknown_tier() {
        assert!(parse_key("table/warm/data/file").is_none());
        assert!(parse_key("table/cold/data/file").is_none());
        assert!(parse_key("table/ACTIVE/data/file").is_none());
    }

    #[test]
    fn parse_key_with_no_relative_path() {
        let parsed = parse_key("table/active").unwrap();
        assert_eq!(parsed.table_name, "table");
        assert!(matches!(parsed.tier, SegmentTier::Active));
        assert_eq!(parsed.relative_path, "");
    }

    // -----------------------------------------------------------------------
    // parse_range_to_get_range
    // -----------------------------------------------------------------------

    #[test]
    fn parse_range_bounded() {
        let range = parse_range_to_get_range("bytes=0-499").unwrap();
        match range {
            object_store::GetRange::Bounded(r) => {
                assert_eq!(r.start, 0);
                assert_eq!(r.end, 500); // end is exclusive (parsed as 499+1)
            }
            other => panic!("expected Bounded, got {:?}", other),
        }
    }

    #[test]
    fn parse_range_suffix() {
        let range = parse_range_to_get_range("bytes=-500").unwrap();
        match range {
            object_store::GetRange::Suffix(n) => assert_eq!(n, 500),
            other => panic!("expected Suffix, got {:?}", other),
        }
    }

    #[test]
    fn parse_range_offset() {
        let range = parse_range_to_get_range("bytes=100-").unwrap();
        match range {
            object_store::GetRange::Offset(o) => assert_eq!(o, 100),
            other => panic!("expected Offset, got {:?}", other),
        }
    }

    #[test]
    fn parse_range_rejects_invalid_format() {
        assert!(parse_range_to_get_range("invalid").is_err());
        assert!(parse_range_to_get_range("bytes=abc-def").is_err());
        assert!(parse_range_to_get_range("chars=0-100").is_err());
    }

    // -----------------------------------------------------------------------
    // filter_non_credential_options
    // -----------------------------------------------------------------------

    #[test]
    fn filter_removes_credential_keys() {
        let mut opts = HashMap::new();
        opts.insert("aws_access_key_id".into(), "AKIA...".into());
        opts.insert("aws_secret_access_key".into(), "secret123".into());
        opts.insert("aws_session_token".into(), "tok".into());
        opts.insert("password".into(), "hunter2".into());
        opts.insert("credential_provider".into(), "some".into());
        opts.insert("region".into(), "us-east-1".into());

        let filtered = filter_non_credential_options(&opts);
        assert!(!filtered.contains_key("aws_access_key_id"));
        assert!(!filtered.contains_key("aws_secret_access_key"));
        assert!(!filtered.contains_key("aws_session_token"));
        assert!(!filtered.contains_key("password"));
        assert!(!filtered.contains_key("credential_provider"));
        assert_eq!(filtered.get("region").unwrap(), "us-east-1");
    }

    #[test]
    fn filter_keeps_region_and_endpoint() {
        let mut opts = HashMap::new();
        opts.insert("region".into(), "us-west-2".into());
        opts.insert("endpoint".into(), "https://s3.example.com".into());
        opts.insert("force_path_style".into(), "true".into());

        let filtered = filter_non_credential_options(&opts);
        assert_eq!(filtered.len(), 3);
        assert_eq!(filtered["region"], "us-west-2");
        assert_eq!(filtered["endpoint"], "https://s3.example.com");
        assert_eq!(filtered["force_path_style"], "true");
    }

    // -----------------------------------------------------------------------
    // escape_xml
    // -----------------------------------------------------------------------

    #[test]
    fn escape_xml_special_characters() {
        assert_eq!(escape_xml("a & b"), "a &amp; b");
        assert_eq!(escape_xml("<tag>"), "&lt;tag&gt;");
        assert_eq!(escape_xml("he said \"hi\""), "he said &quot;hi&quot;");
        assert_eq!(escape_xml("it's"), "it&apos;s");
        assert_eq!(escape_xml("plain text"), "plain text");
        assert_eq!(
            escape_xml("<a>&\"'</a>"),
            "&lt;a&gt;&amp;&quot;&apos;&lt;/a&gt;"
        );
    }

    // -----------------------------------------------------------------------
    // list_objects_xml_response
    // -----------------------------------------------------------------------

    #[test]
    fn list_objects_xml_produces_valid_structure() {
        let objects = vec![S3Object {
            key: "table/active/data/file.lance".into(),
            size: 1024,
            last_modified: "2025-01-01T00:00:00.000Z".into(),
            etag: "\"abc123\"".into(),
        }];
        let common_prefixes = vec!["table/active/data/".to_string()];

        let resp = list_objects_xml_response(
            "my-bucket",
            "table/",
            &objects,
            &common_prefixes,
            false,
            None,
        );

        // Verify status and content-type
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/xml"
        );

        // Extract body synchronously via into_body
        let body_bytes = {
            use axum::body::to_bytes;
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(to_bytes(resp.into_body(), usize::MAX)).unwrap()
        };
        let xml = String::from_utf8(body_bytes.to_vec()).unwrap();

        assert!(xml.starts_with("<?xml version=\"1.0\""));
        assert!(xml.contains("<ListBucketResult"));
        assert!(xml.contains("<Name>my-bucket</Name>"));
        assert!(xml.contains("<Prefix>table/</Prefix>"));
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(xml.contains("<Key>table/active/data/file.lance</Key>"));
        assert!(xml.contains("<Size>1024</Size>"));
        assert!(xml.contains("<CommonPrefixes>"));
        assert!(xml.contains("<Prefix>table/active/data/</Prefix>"));
        // No continuation token when is_truncated is false
        assert!(!xml.contains("<NextContinuationToken>"));
    }

    // -----------------------------------------------------------------------
    // snapshot_to_operation / operation_to_snapshot roundtrip
    // -----------------------------------------------------------------------

    #[test]
    fn snapshot_operation_roundtrip() {
        let original = Operation {
            id: "op-123".into(),
            node_id: 42,
            op_type: OpType::Reindex,
            tier: OpTier::Cold,
            tenant: "tenant-a".into(),
            catalog: "catalog-1".into(),
            catalog_type: "Lance".into(),
            table: "events".into(),
            status: OpStatus::Running,
            progress: 0.75,
            created_at: "2025-01-01T00:00:00Z".into(),
            started_at: Some("2025-01-01T00:01:00Z".into()),
            finished_at: None,
            error: None,
            fragments_done: Some(30),
            fragments_total: Some(40),
        };

        let snapshot = operation_to_snapshot(&original);
        let recovered = snapshot_to_operation(&snapshot);

        assert_eq!(recovered.id, original.id);
        assert_eq!(recovered.node_id, original.node_id);
        assert_eq!(recovered.op_type, original.op_type);
        assert_eq!(recovered.tier, original.tier);
        assert_eq!(recovered.tenant, original.tenant);
        assert_eq!(recovered.catalog, original.catalog);
        assert_eq!(recovered.catalog_type, original.catalog_type);
        assert_eq!(recovered.table, original.table);
        assert_eq!(recovered.status, original.status);
        assert!((recovered.progress - original.progress).abs() < f64::EPSILON);
        assert_eq!(recovered.created_at, original.created_at);
        assert_eq!(recovered.started_at, original.started_at);
        assert_eq!(recovered.finished_at, original.finished_at);
        assert_eq!(recovered.error, original.error);
        assert_eq!(recovered.fragments_done, original.fragments_done);
        assert_eq!(recovered.fragments_total, original.fragments_total);
    }
}

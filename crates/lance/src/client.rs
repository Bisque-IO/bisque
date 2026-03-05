//! BisqueClient — auto-syncing remote query client for bisque-lance.
//!
//! Connects to a bisque-lance cluster, opens remote Lance datasets through
//! the S3-compatible API, and maintains a `SessionContext` that automatically
//! updates when the server pushes catalog events over WebSocket.
//!
//! # Architecture
//!
//! - Each table is backed by a [`RemoteLanceTableProvider`] that holds
//!   `Arc<ArcSwap<Dataset>>` handles per tier (active, sealed).
//! - When the server pushes events (e.g. `ActiveVersionBumped`), the client
//!   re-opens the affected dataset and atomically swaps it in.
//! - Queries in flight keep pinned snapshots; new queries see the latest data.
//!
//! # Persistence
//!
//! When `persist_path` is provided to [`BisqueClient::connect`], the client
//! stores catalog state in a local MDBX database. On reconnect, it does a
//! delta sync via `?since=last_seq` instead of a full catalog fetch, and
//! opens datasets lazily on first query.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arc_swap::ArcSwap;
use arrow_schema::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::Result as DfResult;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::Expr;
use datafusion_physical_plan::union::UnionExec;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance_table::io::commit::RenameCommitHandler;
use object_store::ObjectStore;
use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::catalog_events::{CatalogEvent, CatalogEventKind};
use crate::client_store::{ClientMeta, ClientStore, PersistedCatalogEntry};
use crate::cold_store::CredentialConfig;
use crate::s3_store::BisqueRoutingStore;

/// Outbound message sent to the WebSocket writer for version pinning.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WsOutbound {
    Pin {
        table: String,
        tier: String,
        version: u64,
    },
    Unpin {
        table: String,
        tier: String,
        version: u64,
    },
    Heartbeat,
}

/// A remote query client that auto-syncs with a bisque-lance cluster.
///
/// Maintains a `SessionContext` with dynamically updated table providers
/// that reflect real-time catalog changes pushed over WebSocket.
pub struct BisqueClient {
    ctx: SessionContext,
    routing_store: Arc<BisqueRoutingStore>,
    tables: Arc<RwLock<HashMap<String, Arc<RemoteLanceTableProvider>>>>,
    ws_handle: Option<JoinHandle<()>>,
    shutdown: Arc<Notify>,
    last_seq: Arc<AtomicU64>,
    cluster_url: String,
    bucket: String,
    ws_tx: mpsc::UnboundedSender<WsOutbound>,
}

impl BisqueClient {
    /// Connect to a bisque-lance cluster and set up auto-syncing.
    ///
    /// Fetches the initial catalog, opens all datasets, registers table
    /// providers in a new `SessionContext`, and starts a WebSocket listener
    /// for real-time updates.
    ///
    /// If `persist_path` is `Some`, catalog state is persisted to a local
    /// MDBX database. On subsequent connections, the client does delta sync
    /// and opens datasets lazily for near-instant startup.
    pub async fn connect(
        cluster_url: impl Into<String>,
        bucket: impl Into<String>,
        credentials: Arc<CredentialConfig>,
        persist_path: Option<&Path>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let cluster_url = cluster_url.into().trim_end_matches('/').to_string();
        let bucket = bucket.into();

        let routing_store = Arc::new(BisqueRoutingStore::new(&cluster_url, &bucket, credentials));

        let ctx = SessionContext::new();
        let tables: Arc<RwLock<HashMap<String, Arc<RemoteLanceTableProvider>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let shutdown = Arc::new(Notify::new());
        let last_seq = Arc::new(AtomicU64::new(0));
        let (ws_tx, ws_rx) = mpsc::unbounded_channel::<WsOutbound>();

        // Try to restore from persisted state
        let client_store = persist_path
            .and_then(|p| ClientStore::open(p).ok())
            .map(Arc::new);
        let restored = client_store
            .as_ref()
            .and_then(|store| match store.load_state() {
                Ok(Some((meta, entries)))
                    if meta.cluster_url == cluster_url && meta.bucket == bucket =>
                {
                    Some((meta, entries))
                }
                _ => None,
            });

        if let Some((meta, entries)) = restored {
            // Delta sync: restore from persisted catalog, open datasets lazily
            info!(
                last_seq = meta.last_seq,
                tables = entries.len(),
                "Restoring from persisted catalog state"
            );
            last_seq.store(meta.last_seq, Ordering::Relaxed);

            for (table_name, entry) in &entries {
                let schema = match crate::ipc::schema_from_ipc(&entry.schema_ipc) {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        warn!(table = %table_name, "Failed to decode persisted schema: {}", e);
                        continue;
                    }
                };

                // Create provider with lazy opening flags
                let provider = Arc::new(RemoteLanceTableProvider::new_lazy(
                    schema,
                    routing_store.clone(),
                    table_name.clone(),
                    entry.active_version.is_some(),
                    entry.sealed_segment.is_some(),
                ));

                if let Err(e) = ctx.register_table(table_name, provider.clone()) {
                    warn!(table = %table_name, "Failed to register restored table: {}", e);
                } else {
                    tables.write().insert(table_name.clone(), provider);
                }
            }
        } else {
            // Full fetch: same as before
            let catalog = fetch_catalog(&cluster_url, &bucket).await?;
            let mut persisted_entries = HashMap::new();

            for (table_name, info) in &catalog.tables {
                let provider = open_table_provider(&routing_store, table_name, info).await?;
                let provider = Arc::new(provider);

                // Pin initial versions
                if let Some(v) = info.active_version {
                    let _ = ws_tx.send(WsOutbound::Pin {
                        table: table_name.clone(),
                        tier: "active".into(),
                        version: v,
                    });
                }
                if info.sealed_segment.is_some() {
                    if let Some(v) = info.sealed_version {
                        let _ = ws_tx.send(WsOutbound::Pin {
                            table: table_name.clone(),
                            tier: "sealed".into(),
                            version: v,
                        });
                    }
                }

                // Build persisted entry before consuming provider
                if client_store.is_some() {
                    persisted_entries.insert(
                        table_name.clone(),
                        PersistedCatalogEntry {
                            active_segment: info.active_segment,
                            sealed_segment: info.sealed_segment,
                            s3_dataset_uri: info.s3_dataset_uri.clone(),
                            s3_storage_options: info.s3_storage_options.clone(),
                            active_version: info.active_version,
                            sealed_version: info.sealed_version,
                            schema_ipc: crate::ipc::schema_to_ipc(&provider.schema)
                                .unwrap_or_default(),
                        },
                    );
                }

                ctx.register_table(table_name, provider.clone())?;
                tables.write().insert(table_name.clone(), provider);
            }

            // Persist initial state
            if let Some(store) = &client_store {
                let meta = ClientMeta {
                    last_seq: 0,
                    cluster_url: cluster_url.clone(),
                    bucket: bucket.clone(),
                };
                if let Err(e) = store.save_state(&meta, &persisted_entries) {
                    warn!("Failed to persist initial catalog state: {}", e);
                }
            }
        }

        // Start heartbeat task
        let heartbeat_tx = ws_tx.clone();
        let heartbeat_shutdown = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                        if heartbeat_tx.send(WsOutbound::Heartbeat).is_err() {
                            break;
                        }
                    }
                    _ = heartbeat_shutdown.notified() => break,
                }
            }
        });

        // Start WebSocket listener
        let ws_handle = {
            let cluster_url = cluster_url.clone();
            let bucket = bucket.clone();
            let routing_store = routing_store.clone();
            let tables = tables.clone();
            let ctx = ctx.clone();
            let shutdown = shutdown.clone();
            let last_seq = last_seq.clone();
            let ws_tx_clone = ws_tx.clone();
            let client_store_clone = client_store.clone();

            tokio::spawn(async move {
                ws_listener_loop(
                    &cluster_url,
                    &bucket,
                    routing_store,
                    tables,
                    ctx,
                    shutdown,
                    last_seq,
                    ws_rx,
                    ws_tx_clone,
                    client_store_clone,
                )
                .await;
            })
        };

        Ok(Self {
            ctx,
            routing_store,
            tables,
            ws_handle: Some(ws_handle),
            shutdown,
            last_seq,
            cluster_url,
            bucket,
            ws_tx,
        })
    }

    /// Execute a SQL query and collect results.
    pub async fn sql(&self, query: &str) -> DfResult<Vec<arrow::array::RecordBatch>> {
        let df = self.ctx.sql(query).await?;
        df.collect().await
    }

    /// Access the underlying `SessionContext` for advanced queries.
    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    /// Get the last received event sequence number.
    pub fn last_seq(&self) -> u64 {
        self.last_seq.load(Ordering::Relaxed)
    }

    /// Shut down the client, stopping the WebSocket listener.
    pub async fn close(mut self) {
        self.shutdown.notify_waiters();
        if let Some(handle) = self.ws_handle.take() {
            let _ = handle.await;
        }
    }
}

// ---------------------------------------------------------------------------
// Remote table provider with swappable datasets
// ---------------------------------------------------------------------------

/// A DataFusion `TableProvider` backed by remote Lance datasets.
///
/// Datasets are held in `ArcSwap` so they can be atomically updated
/// when the server pushes catalog events. When restored from persisted
/// state, datasets start as `None` and are opened lazily on first query.
pub struct RemoteLanceTableProvider {
    schema: SchemaRef,
    active: Arc<ArcSwap<Option<Dataset>>>,
    sealed: Arc<ArcSwap<Option<Dataset>>>,
    /// For lazy opening: routing store reference.
    routing_store: Option<Arc<BisqueRoutingStore>>,
    /// For lazy opening: table name.
    table_name: String,
    /// Whether the active dataset needs to be lazily opened.
    needs_active_open: AtomicBool,
    /// Whether the sealed dataset needs to be lazily opened.
    needs_sealed_open: AtomicBool,
}

impl std::fmt::Debug for RemoteLanceTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteLanceTableProvider")
            .field("schema", &self.schema)
            .field("table_name", &self.table_name)
            .finish()
    }
}

impl RemoteLanceTableProvider {
    fn new(schema: SchemaRef, active: Option<Dataset>, sealed: Option<Dataset>) -> Self {
        Self {
            schema,
            active: Arc::new(ArcSwap::from_pointee(active)),
            sealed: Arc::new(ArcSwap::from_pointee(sealed)),
            routing_store: None,
            table_name: String::new(),
            needs_active_open: AtomicBool::new(false),
            needs_sealed_open: AtomicBool::new(false),
        }
    }

    /// Create a provider that opens datasets lazily on first scan.
    fn new_lazy(
        schema: SchemaRef,
        routing_store: Arc<BisqueRoutingStore>,
        table_name: String,
        has_active: bool,
        has_sealed: bool,
    ) -> Self {
        Self {
            schema,
            active: Arc::new(ArcSwap::from_pointee(None)),
            sealed: Arc::new(ArcSwap::from_pointee(None)),
            routing_store: Some(routing_store),
            table_name,
            needs_active_open: AtomicBool::new(has_active),
            needs_sealed_open: AtomicBool::new(has_sealed),
        }
    }

    fn swap_active(&self, ds: Option<Dataset>) {
        self.needs_active_open.store(false, Ordering::Relaxed);
        self.active.store(Arc::new(ds));
    }

    fn swap_sealed(&self, ds: Option<Dataset>) {
        self.needs_sealed_open.store(false, Ordering::Relaxed);
        self.sealed.store(Arc::new(ds));
    }

    /// Get the current active dataset's manifest version, if any.
    fn active_version(&self) -> Option<u64> {
        let guard = self.active.load();
        guard.as_ref().as_ref().map(|ds| ds.manifest.version)
    }

    /// Get the current sealed dataset's manifest version, if any.
    fn sealed_version(&self) -> Option<u64> {
        let guard = self.sealed.load();
        guard.as_ref().as_ref().map(|ds| ds.manifest.version)
    }

    /// Lazily open datasets that haven't been loaded yet.
    async fn ensure_datasets_open(&self) {
        if self
            .needs_active_open
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            if let Some(store) = &self.routing_store {
                match open_remote_dataset(store, &self.table_name, "active").await {
                    Ok(ds) => {
                        self.active.store(Arc::new(Some(ds)));
                        debug!(table = %self.table_name, "Lazily opened active dataset");
                    }
                    Err(e) => {
                        warn!(table = %self.table_name, "Failed to lazily open active dataset: {}", e);
                        // Reset flag so we retry next time
                        self.needs_active_open.store(true, Ordering::Relaxed);
                    }
                }
            }
        }

        if self
            .needs_sealed_open
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            if let Some(store) = &self.routing_store {
                match open_remote_dataset(store, &self.table_name, "sealed").await {
                    Ok(ds) => {
                        self.sealed.store(Arc::new(Some(ds)));
                        debug!(table = %self.table_name, "Lazily opened sealed dataset");
                    }
                    Err(e) => {
                        warn!(table = %self.table_name, "Failed to lazily open sealed dataset: {}", e);
                        self.needs_sealed_open.store(true, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for RemoteLanceTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion_expr::TableType {
        datafusion_expr::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Lazy open datasets if needed
        self.ensure_datasets_open().await;

        let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(2);

        // Load current dataset snapshots (pinned for this query)
        let active_guard = self.active.load();
        if let Some(ds) = active_guard.as_ref() {
            let plan = build_scan_plan(ds, &self.schema, projection, filters, limit).await?;
            plans.push(plan);
        }

        let sealed_guard = self.sealed.load();
        if let Some(ds) = sealed_guard.as_ref() {
            let plan = build_scan_plan(ds, &self.schema, projection, filters, limit).await?;
            plans.push(plan);
        }

        if plans.is_empty() {
            // Return an empty exec with the correct schema
            Ok(Arc::new(datafusion_physical_plan::empty::EmptyExec::new(
                self.schema.clone(),
            )))
        } else if plans.len() == 1 {
            Ok(plans.into_iter().next().unwrap())
        } else {
            UnionExec::try_new(plans)
        }
    }
}

async fn build_scan_plan(
    dataset: &Dataset,
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let mut scan = dataset.scan();

    // Projection pushdown
    match projection {
        Some(proj) if proj.is_empty() => {
            scan.empty_project()
                .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        }
        Some(proj) => {
            let columns: Vec<&str> = proj
                .iter()
                .filter_map(|&idx| schema.fields().get(idx).map(|f| f.name().as_str()))
                .collect();
            if !columns.is_empty() {
                scan.project(&columns)
                    .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
            }
        }
        None => {}
    }

    // Filter pushdown — Lance scanner applies filters via string expressions.
    // DataFusion handles filter execution at the plan level, so we skip
    // complex filter translation here and rely on DataFusion's native filtering.
    let _ = filters;

    // Limit pushdown
    if let Some(limit) = limit {
        scan.limit(Some(limit as i64), None)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
    }

    scan.create_plan()
        .await
        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))
}

// ---------------------------------------------------------------------------
// Catalog fetching
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct CatalogResponse {
    tables: HashMap<String, CatalogTableInfo>,
}

#[derive(Deserialize)]
struct CatalogTableInfo {
    active_segment: u64,
    sealed_segment: Option<u64>,
    s3_dataset_uri: String,
    active_version: Option<u64>,
    sealed_version: Option<u64>,
    /// Non-credential S3 storage options from the server (region, endpoint, etc.).
    /// Consumed by `BisqueRoutingStore` via its own catalog fetch.
    #[serde(default)]
    s3_storage_options: HashMap<String, String>,
}

async fn fetch_catalog(
    cluster_url: &str,
    bucket: &str,
) -> Result<CatalogResponse, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}/{}/{}", cluster_url, bucket, "_bisque/catalog");
    let resp = reqwest::Client::new().get(&url).send().await?;
    let catalog: CatalogResponse = resp.json().await?;
    Ok(catalog)
}

async fn open_remote_dataset(
    routing_store: &Arc<BisqueRoutingStore>,
    table_name: &str,
    tier: &str,
) -> Result<Dataset, Box<dyn std::error::Error + Send + Sync>> {
    let path = format!("{}/{}", table_name, tier);
    let url = Url::parse(&format!("bisque://cluster/{}", path))?;

    let ds = DatasetBuilder::from_uri(url.as_str())
        .with_object_store(
            routing_store.clone() as Arc<dyn ObjectStore>,
            url,
            Arc::new(RenameCommitHandler),
        )
        .load()
        .await?;
    Ok(ds)
}

async fn open_table_provider(
    routing_store: &Arc<BisqueRoutingStore>,
    table_name: &str,
    info: &CatalogTableInfo,
) -> Result<RemoteLanceTableProvider, Box<dyn std::error::Error + Send + Sync>> {
    // Open active dataset
    let active_ds = match info.active_version {
        Some(_) => match open_remote_dataset(routing_store, table_name, "active").await {
            Ok(ds) => Some(ds),
            Err(e) => {
                warn!(table = %table_name, "Failed to open active dataset: {}", e);
                None
            }
        },
        None => None,
    };

    // Open sealed dataset
    let sealed_ds = match info.sealed_segment {
        Some(_) => match open_remote_dataset(routing_store, table_name, "sealed").await {
            Ok(ds) => Some(ds),
            Err(e) => {
                warn!(table = %table_name, "Failed to open sealed dataset: {}", e);
                None
            }
        },
        None => None,
    };

    // Determine schema from whichever dataset is available
    let schema = active_ds
        .as_ref()
        .or(sealed_ds.as_ref())
        .map(|ds| Arc::new(ds.schema().into()) as SchemaRef)
        .ok_or_else(|| format!("No datasets available for table '{}'", table_name))?;

    Ok(RemoteLanceTableProvider::new(schema, active_ds, sealed_ds))
}

// ---------------------------------------------------------------------------
// WebSocket listener
// ---------------------------------------------------------------------------

async fn ws_listener_loop(
    cluster_url: &str,
    bucket: &str,
    routing_store: Arc<BisqueRoutingStore>,
    tables: Arc<RwLock<HashMap<String, Arc<RemoteLanceTableProvider>>>>,
    ctx: SessionContext,
    shutdown: Arc<Notify>,
    last_seq: Arc<AtomicU64>,
    mut ws_rx: mpsc::UnboundedReceiver<WsOutbound>,
    ws_tx: mpsc::UnboundedSender<WsOutbound>,
    client_store: Option<Arc<ClientStore>>,
) {
    loop {
        let since = last_seq.load(Ordering::Relaxed);
        match ws_connect_and_listen(
            cluster_url,
            bucket,
            &routing_store,
            &tables,
            &ctx,
            &shutdown,
            &last_seq,
            since,
            &mut ws_rx,
            &ws_tx,
            &client_store,
        )
        .await
        {
            Ok(()) => {
                info!("WebSocket listener shut down cleanly");
                return;
            }
            Err(e) => {
                warn!("WebSocket connection lost: {}, reconnecting in 1s...", e);
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                    _ = shutdown.notified() => return,
                }
            }
        }
    }
}

async fn ws_connect_and_listen(
    cluster_url: &str,
    bucket: &str,
    routing_store: &Arc<BisqueRoutingStore>,
    tables: &Arc<RwLock<HashMap<String, Arc<RemoteLanceTableProvider>>>>,
    ctx: &SessionContext,
    shutdown: &Arc<Notify>,
    last_seq: &Arc<AtomicU64>,
    since: u64,
    ws_rx: &mut mpsc::UnboundedReceiver<WsOutbound>,
    ws_tx: &mpsc::UnboundedSender<WsOutbound>,
    client_store: &Option<Arc<ClientStore>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Convert http(s):// to ws(s)://
    let ws_url = cluster_url
        .replace("http://", "ws://")
        .replace("https://", "wss://");
    let url = if since > 0 {
        format!("{}/{}/{}/ws?since={}", ws_url, bucket, "_bisque", since)
    } else {
        format!("{}/{}/{}/ws", ws_url, bucket, "_bisque")
    };

    info!(%url, "Connecting to WebSocket for catalog events");

    let uri: hyper::Uri = url.parse()?;
    let host = uri.host().unwrap_or("localhost");
    let port = uri.port_u16().unwrap_or(80);
    let addr = format!("{}:{}", host, port);

    let tcp = tokio::net::TcpStream::connect(&addr).await?;
    let req = hyper::Request::builder()
        .method("GET")
        .uri(&url)
        .header("Host", host)
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            fastwebsockets::handshake::generate_key(),
        )
        .body(http_body_util::Empty::<hyper::body::Bytes>::new())?;

    let (mut ws, _) = fastwebsockets::handshake::client(&TokioSpawnExecutor, req, tcp).await?;

    info!("WebSocket connected, listening for catalog events");

    // Drain any queued outbound messages (pins from initial connect, heartbeats)
    while let Ok(msg) = ws_rx.try_recv() {
        let json = serde_json::to_string(&msg)?;
        ws.write_frame(fastwebsockets::Frame::text(fastwebsockets::Payload::Owned(
            json.into_bytes(),
        )))
        .await?;
    }

    loop {
        tokio::select! {
            frame = ws.read_frame() => {
                let frame = frame?;
                match frame.opcode {
                    fastwebsockets::OpCode::Text => {
                        let text = std::str::from_utf8(&frame.payload)?;
                        handle_ws_message(text, routing_store, tables, ctx, last_seq, ws_tx, client_store).await;
                    }
                    fastwebsockets::OpCode::Close => {
                        debug!("WebSocket server sent close frame");
                        return Ok(());
                    }
                    _ => {}
                }
            }
            msg = ws_rx.recv() => {
                match msg {
                    Some(outbound) => {
                        let json = serde_json::to_string(&outbound)?;
                        ws.write_frame(fastwebsockets::Frame::text(
                            fastwebsockets::Payload::Owned(json.into_bytes()),
                        )).await?;
                    }
                    None => {
                        // Channel closed, shutdown
                        let _ = ws.write_frame(fastwebsockets::Frame::close_raw(vec![].into())).await;
                        return Ok(());
                    }
                }
            }
            _ = shutdown.notified() => {
                let _ = ws.write_frame(fastwebsockets::Frame::close_raw(vec![].into())).await;
                return Ok(());
            }
        }
    }
}

async fn handle_ws_message(
    text: &str,
    routing_store: &Arc<BisqueRoutingStore>,
    tables: &Arc<RwLock<HashMap<String, Arc<RemoteLanceTableProvider>>>>,
    ctx: &SessionContext,
    last_seq: &Arc<AtomicU64>,
    ws_tx: &mpsc::UnboundedSender<WsOutbound>,
    client_store: &Option<Arc<ClientStore>>,
) {
    // Check for snapshot_required signal
    if text.contains("snapshot_required") {
        warn!("Received snapshot_required — full catalog re-sync needed");
        // TODO: implement full re-sync from catalog endpoint
        return;
    }

    // Ignore session messages from server
    if text.contains("\"type\":\"session\"") {
        return;
    }

    // Parse as CatalogEvent
    let event: CatalogEvent = match serde_json::from_str(text) {
        Ok(e) => e,
        Err(e) => {
            warn!("Failed to parse catalog event: {}", e);
            return;
        }
    };

    last_seq.store(event.seq, Ordering::Relaxed);

    match &event.event {
        CatalogEventKind::ActiveVersionBumped { table, version } => {
            let provider = tables.read().get(table).cloned();
            if let Some(provider) = provider {
                let old_version = provider.active_version();
                match open_remote_dataset(routing_store, table, "active").await {
                    Ok(ds) => {
                        // Pin new version, swap, then unpin old
                        let _ = ws_tx.send(WsOutbound::Pin {
                            table: table.clone(),
                            tier: "active".into(),
                            version: *version,
                        });
                        provider.swap_active(Some(ds));
                        if let Some(old_v) = old_version {
                            let _ = ws_tx.send(WsOutbound::Unpin {
                                table: table.clone(),
                                tier: "active".into(),
                                version: old_v,
                            });
                        }
                        debug!(table = %table, seq = event.seq, "Swapped active dataset");
                    }
                    Err(e) => {
                        warn!(table = %table, "Failed to re-open active dataset: {}", e);
                    }
                }
            }
            // Persist
            persist_version_update(client_store, table, |entry| {
                entry.active_version = Some(*version);
            });
        }
        CatalogEventKind::SegmentSealed {
            table,
            active_version,
            sealed_version,
        } => {
            let provider = tables.read().get(table).cloned();
            if let Some(provider) = provider {
                let old_active_v = provider.active_version();
                let old_sealed_v = provider.sealed_version();

                // Re-open both tiers
                match open_remote_dataset(routing_store, table, "active").await {
                    Ok(ds) => {
                        let _ = ws_tx.send(WsOutbound::Pin {
                            table: table.clone(),
                            tier: "active".into(),
                            version: *active_version,
                        });
                        provider.swap_active(Some(ds));
                        if let Some(old_v) = old_active_v {
                            let _ = ws_tx.send(WsOutbound::Unpin {
                                table: table.clone(),
                                tier: "active".into(),
                                version: old_v,
                            });
                        }
                    }
                    Err(e) => warn!(table = %table, "Failed to re-open active dataset: {}", e),
                }
                match open_remote_dataset(routing_store, table, "sealed").await {
                    Ok(ds) => {
                        let _ = ws_tx.send(WsOutbound::Pin {
                            table: table.clone(),
                            tier: "sealed".into(),
                            version: *sealed_version,
                        });
                        provider.swap_sealed(Some(ds));
                        if let Some(old_v) = old_sealed_v {
                            let _ = ws_tx.send(WsOutbound::Unpin {
                                table: table.clone(),
                                tier: "sealed".into(),
                                version: old_v,
                            });
                        }
                    }
                    Err(e) => warn!(table = %table, "Failed to re-open sealed dataset: {}", e),
                }
                debug!(table = %table, seq = event.seq, "Swapped active+sealed datasets after seal");
            }
            // Persist
            persist_version_update(client_store, table, |entry| {
                entry.active_version = Some(*active_version);
                entry.sealed_version = Some(*sealed_version);
                // sealed_segment gets set but we don't know the exact ID from the event
                // — the important thing is it's Some
                if entry.sealed_segment.is_none() {
                    entry.sealed_segment = Some(1);
                }
            });
        }
        CatalogEventKind::TableCreated { table, schema_ipc } => {
            let schema = match crate::ipc::schema_from_ipc(schema_ipc) {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    error!(table = %table, "Failed to decode schema from event: {}", e);
                    return;
                }
            };
            let provider = Arc::new(RemoteLanceTableProvider::new(schema.clone(), None, None));
            if let Err(e) = ctx.register_table(table, provider.clone()) {
                warn!(table = %table, "Failed to register new table: {}", e);
            } else {
                tables.write().insert(table.clone(), provider);
                info!(table = %table, seq = event.seq, "Registered new table from event");
            }
            // Persist
            if let Some(store) = client_store {
                let entry = PersistedCatalogEntry {
                    active_segment: 1,
                    sealed_segment: None,
                    s3_dataset_uri: String::new(),
                    s3_storage_options: HashMap::new(),
                    active_version: None,
                    sealed_version: None,
                    schema_ipc: schema_ipc.clone(),
                };
                if let Err(e) = store.update_table(table, &entry) {
                    warn!(table = %table, "Failed to persist new table: {}", e);
                }
            }
        }
        CatalogEventKind::TableDropped { table } => {
            // Unpin all versions for this table
            let provider = tables.read().get(table).cloned();
            if let Some(provider) = provider {
                if let Some(v) = provider.active_version() {
                    let _ = ws_tx.send(WsOutbound::Unpin {
                        table: table.clone(),
                        tier: "active".into(),
                        version: v,
                    });
                }
                if let Some(v) = provider.sealed_version() {
                    let _ = ws_tx.send(WsOutbound::Unpin {
                        table: table.clone(),
                        tier: "sealed".into(),
                        version: v,
                    });
                }
            }
            tables.write().remove(table);
            if let Err(e) = ctx.deregister_table(table) {
                warn!(table = %table, "Failed to deregister table: {}", e);
            } else {
                info!(table = %table, seq = event.seq, "Deregistered dropped table");
            }
            // Persist
            if let Some(store) = client_store {
                if let Err(e) = store.remove_table(table) {
                    warn!(table = %table, "Failed to remove persisted table: {}", e);
                }
            }
        }
        CatalogEventKind::SegmentPromoted { table, .. } => {
            let provider = tables.read().get(table).cloned();
            if let Some(provider) = provider {
                if let Some(v) = provider.sealed_version() {
                    let _ = ws_tx.send(WsOutbound::Unpin {
                        table: table.clone(),
                        tier: "sealed".into(),
                        version: v,
                    });
                }
                provider.swap_sealed(None);
                debug!(table = %table, seq = event.seq, "Cleared sealed dataset after promote");
            }
            // Persist
            persist_version_update(client_store, table, |entry| {
                entry.sealed_segment = None;
                entry.sealed_version = None;
            });
        }
    }

    // Update persisted sequence number
    if let Some(store) = client_store {
        if let Err(e) = store.update_seq(event.seq) {
            warn!("Failed to persist event seq {}: {}", event.seq, e);
        }
    }
}

/// Helper to read-modify-write a persisted table entry.
fn persist_version_update(
    client_store: &Option<Arc<ClientStore>>,
    table: &str,
    mutate: impl FnOnce(&mut PersistedCatalogEntry),
) {
    let Some(store) = client_store else { return };

    // Load existing entry, update it, save it back
    match store.load_state() {
        Ok(Some((_, tables))) => {
            if let Some(mut entry) = tables.get(table).cloned() {
                mutate(&mut entry);
                if let Err(e) = store.update_table(table, &entry) {
                    warn!(table = %table, "Failed to persist table update: {}", e);
                }
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Hyper executor adapter for tokio
// ---------------------------------------------------------------------------

struct TokioSpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for TokioSpawnExecutor
where
    Fut: std::future::Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn(fut);
    }
}

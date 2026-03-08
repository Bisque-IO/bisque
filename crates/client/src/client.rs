//! BisqueClient — multi-catalog auto-syncing remote query client for bisque-lance.
//!
//! Connects to a bisque-lance cluster over a single WebSocket and opens remote
//! Lance datasets through the S3-compatible API for any number of catalogs
//! within a tenant. Maintains a `SessionContext` that automatically updates
//! when the server pushes catalog events.
//!
//! # Architecture
//!
//! - One WebSocket connection per client, multiplexing events from N catalogs.
//! - Each catalog maps to a DataFusion `CatalogProvider`, enabling cross-catalog
//!   queries like `SELECT * FROM catalog_a.public.t1 JOIN catalog_b.public.t2`.
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
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
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
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::client_store::{ClientMeta, ClientStore, PersistedCatalogEntry};
use crate::cold_store::CredentialConfig;
use crate::s3_store::BisqueRoutingStore;
use bisque_protocol::catalog_events::{CatalogEvent, CatalogEventKind};
use bisque_protocol::ws::{ClientMessage, ServerMessage, WS_PROTOCOL_VERSION};

// ---------------------------------------------------------------------------
// Per-catalog state
// ---------------------------------------------------------------------------

/// State for a single catalog (raft group) within the multi-catalog client.
pub(crate) struct CatalogState {
    pub name: String,
    pub routing_store: Arc<BisqueRoutingStore>,
    pub tables: RwLock<HashMap<String, Arc<RemoteLanceTableProvider>>>,
    pub last_seq: AtomicU64,
}

impl std::fmt::Debug for CatalogState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogState")
            .field("name", &self.name)
            .field("tables", &self.tables.read().keys().collect::<Vec<_>>())
            .field(
                "last_seq",
                &self.last_seq.load(std::sync::atomic::Ordering::Relaxed),
            )
            .finish()
    }
}

// ---------------------------------------------------------------------------
// BisqueClient
// ---------------------------------------------------------------------------

/// A remote query client that auto-syncs with a bisque-lance cluster.
///
/// Supports connecting to multiple catalogs within a tenant over a single
/// WebSocket connection. Each catalog is registered as a DataFusion
/// `CatalogProvider`, enabling cross-catalog SQL queries.
pub struct BisqueClient {
    ctx: SessionContext,
    catalogs: Arc<RwLock<HashMap<String, Arc<CatalogState>>>>,
    ws_handle: Option<JoinHandle<()>>,
    shutdown: Arc<Notify>,
    /// Durable flag that survives missed Notify wakeups.
    shutdown_flag: Arc<AtomicBool>,
    cluster_url: String,
    ws_tx: mpsc::UnboundedSender<ClientMessage>,
    credentials: Arc<CredentialConfig>,
    /// Optional auth token for WebSocket handshake (C3).
    auth_token: String,
}

impl BisqueClient {
    /// Connect to a bisque-lance cluster and set up auto-syncing for multiple catalogs.
    ///
    /// Fetches the initial catalog for each catalog name, opens all datasets,
    /// registers table providers in a `SessionContext` (namespaced by catalog),
    /// and starts a WebSocket listener for real-time updates across all catalogs.
    ///
    /// Tables are accessible as `catalog_name.public.table_name` in SQL queries.
    /// Connect to a bisque-lance cluster with an optional auth token.
    ///
    /// The `auth_token` is sent in the WebSocket handshake frame for authentication.
    /// Pass an empty string for unauthenticated connections.
    pub async fn connect(
        cluster_url: impl Into<String>,
        catalog_names: Vec<String>,
        credentials: Arc<CredentialConfig>,
        persist_path: Option<&Path>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::connect_with_token(
            cluster_url,
            catalog_names,
            credentials,
            persist_path,
            String::new(),
        )
        .await
    }

    /// Connect with an explicit auth token for WebSocket authentication.
    pub async fn connect_with_token(
        cluster_url: impl Into<String>,
        catalog_names: Vec<String>,
        credentials: Arc<CredentialConfig>,
        persist_path: Option<&Path>,
        auth_token: String,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let cluster_url = cluster_url.into().trim_end_matches('/').to_string();

        // When there's a single catalog, set it as the default so queries
        // can use unqualified table names (e.g., `SELECT * FROM events`
        // instead of `SELECT * FROM catalog.public.events`).
        let ctx = if catalog_names.len() == 1 {
            let config = datafusion::prelude::SessionConfig::new()
                .with_default_catalog_and_schema(&catalog_names[0], "public")
                .with_create_default_catalog_and_schema(false);
            SessionContext::new_with_config(config)
        } else {
            SessionContext::new()
        };
        let catalogs: Arc<RwLock<HashMap<String, Arc<CatalogState>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let shutdown = Arc::new(Notify::new());
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let (ws_tx, ws_rx) = mpsc::unbounded_channel::<ClientMessage>();

        // Try to restore from persisted state
        let client_store = persist_path
            .and_then(|p| ClientStore::open(p).ok())
            .map(Arc::new);

        // Initialize each catalog
        for catalog_name in &catalog_names {
            let routing_store = Arc::new(BisqueRoutingStore::new(
                &cluster_url,
                catalog_name,
                credentials.clone(),
            ));

            let catalog_state = Arc::new(CatalogState {
                name: catalog_name.clone(),
                routing_store: routing_store.clone(),
                tables: RwLock::new(HashMap::new()),
                last_seq: AtomicU64::new(0),
            });

            // Try restore from persisted state for this catalog
            let restored = client_store.as_ref().and_then(|store| {
                match store.load_state() {
                    Ok(Some((meta, entries)))
                        if meta.cluster_url == cluster_url
                            && meta.catalogs.contains(catalog_name) =>
                    {
                        // Filter entries for this catalog (compound key: catalog\0table)
                        let prefix = format!("{}\0", catalog_name);
                        let catalog_entries: HashMap<String, PersistedCatalogEntry> = entries
                            .into_iter()
                            .filter_map(|(k, v)| {
                                k.strip_prefix(&prefix).map(|table| (table.to_string(), v))
                            })
                            .collect();
                        if catalog_entries.is_empty() {
                            None
                        } else {
                            Some(catalog_entries)
                        }
                    }
                    _ => None,
                }
            });

            if let Some(entries) = restored {
                info!(
                    catalog = %catalog_name,
                    tables = entries.len(),
                    "Restoring catalog from persisted state"
                );

                for (table_name, entry) in &entries {
                    let schema = match crate::ipc::schema_from_ipc(&entry.schema_ipc) {
                        Ok(s) => Arc::new(s),
                        Err(e) => {
                            warn!(catalog = %catalog_name, table = %table_name,
                                  "Failed to decode persisted schema: {}", e);
                            continue;
                        }
                    };

                    let provider = Arc::new(RemoteLanceTableProvider::new_lazy(
                        schema,
                        routing_store.clone(),
                        table_name.clone(),
                        entry.active_version.is_some(),
                        entry.sealed_segment.is_some(),
                    ));

                    catalog_state
                        .tables
                        .write()
                        .insert(table_name.clone(), provider);
                }
            } else {
                // Full fetch for this catalog
                let catalog_resp = fetch_catalog(&cluster_url, catalog_name).await?;

                for (table_name, info) in &catalog_resp.tables {
                    let provider = open_table_provider(&routing_store, table_name, info).await?;
                    let provider = Arc::new(provider);

                    // Pin initial versions
                    if let Some(v) = info.active_version {
                        let _ = ws_tx.send(ClientMessage::Pin {
                            catalog: catalog_name.clone(),
                            table: table_name.clone(),
                            tier: "active".into(),
                            version: v,
                        });
                    }
                    if info.sealed_segment.is_some() {
                        if let Some(v) = info.sealed_version {
                            let _ = ws_tx.send(ClientMessage::Pin {
                                catalog: catalog_name.clone(),
                                table: table_name.clone(),
                                tier: "sealed".into(),
                                version: v,
                            });
                        }
                    }

                    catalog_state
                        .tables
                        .write()
                        .insert(table_name.clone(), provider);
                }
            }

            // Register as DataFusion CatalogProvider
            let df_catalog = Arc::new(BisqueClientCatalogProvider {
                state: catalog_state.clone(),
            });
            ctx.register_catalog(catalog_name, df_catalog);

            catalogs.write().insert(catalog_name.clone(), catalog_state);
        }

        // Persist initial state if needed
        if let Some(store) = &client_store {
            let meta = ClientMeta {
                cluster_url: cluster_url.clone(),
                catalogs: catalog_names.clone(),
                bucket: String::new(),
                last_seq: 0,
            };
            let mut all_entries = HashMap::new();
            for (cat_name, cat_state) in catalogs.read().iter() {
                for (table_name, provider) in cat_state.tables.read().iter() {
                    let key = format!("{}\0{}", cat_name, table_name);
                    all_entries.insert(
                        key,
                        PersistedCatalogEntry {
                            active_segment: 0,
                            sealed_segment: None,
                            s3_dataset_uri: String::new(),
                            s3_storage_options: HashMap::new(),
                            active_version: provider.active_version(),
                            sealed_version: provider.sealed_version(),
                            schema_ipc: crate::ipc::schema_to_ipc(&provider.schema)
                                .unwrap_or_default(),
                        },
                    );
                }
            }
            if let Err(e) = store.save_state(&meta, &all_entries) {
                warn!("Failed to persist initial catalog state: {}", e);
            }
        }

        // H6: Start heartbeat task — reads min seq across catalogs instead of always 0.
        let heartbeat_tx = ws_tx.clone();
        let heartbeat_shutdown = shutdown.clone();
        let heartbeat_shutdown_flag = shutdown_flag.clone();
        let heartbeat_catalogs = catalogs.clone();
        tokio::spawn(async move {
            loop {
                if heartbeat_shutdown_flag.load(Ordering::Relaxed) {
                    break;
                }
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                        let last_seen_seq = {
                            let cats = heartbeat_catalogs.read();
                            cats.values()
                                .map(|s| s.last_seq.load(Ordering::Relaxed))
                                .min()
                                .unwrap_or(0)
                        };
                        if heartbeat_tx.send(ClientMessage::Heartbeat { last_seen_seq }).is_err() {
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
            let catalog_names = catalog_names.clone();
            let catalogs = catalogs.clone();
            let shutdown = shutdown.clone();
            let ws_shutdown_flag = shutdown_flag.clone();
            let ws_tx_clone = ws_tx.clone();
            let client_store_clone = client_store.clone();
            let auth_token = auth_token.clone();

            tokio::spawn(async move {
                ws_listener_loop(
                    &cluster_url,
                    &catalog_names,
                    catalogs,
                    shutdown,
                    ws_shutdown_flag,
                    ws_rx,
                    ws_tx_clone,
                    client_store_clone,
                    &auth_token,
                )
                .await;
            })
        };

        Ok(Self {
            ctx,
            catalogs,
            ws_handle: Some(ws_handle),
            shutdown,
            shutdown_flag,
            cluster_url,
            ws_tx,
            credentials,
            auth_token,
        })
    }

    /// Execute a SQL query and collect results.
    ///
    /// Tables are accessible as `catalog_name.public.table_name`.
    pub async fn sql(&self, query: &str) -> DfResult<Vec<arrow::array::RecordBatch>> {
        let df = self.ctx.sql(query).await?;
        df.collect().await
    }

    /// Access the underlying `SessionContext` for advanced queries.
    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    /// Get the last received event sequence number for a specific catalog.
    pub fn last_seq(&self, catalog: &str) -> Option<u64> {
        self.catalogs
            .read()
            .get(catalog)
            .map(|s| s.last_seq.load(Ordering::Relaxed))
    }

    /// List the catalog names this client is connected to.
    pub fn catalog_names(&self) -> Vec<String> {
        self.catalogs.read().keys().cloned().collect()
    }

    /// Shut down the client, stopping the WebSocket listener.
    pub async fn close(mut self) {
        // Set the durable flag first so loops see it even if the Notify
        // wakeup is missed (race between select! branches).
        self.shutdown_flag.store(true, Ordering::Release);
        self.shutdown.notify_waiters();
        if let Some(handle) = self.ws_handle.take() {
            let _ = handle.await;
        }
    }
}

// ---------------------------------------------------------------------------
// DataFusion CatalogProvider integration
// ---------------------------------------------------------------------------

/// DataFusion `CatalogProvider` backed by a single bisque-lance catalog.
#[derive(Debug)]
struct BisqueClientCatalogProvider {
    state: Arc<CatalogState>,
}

impl CatalogProvider for BisqueClientCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["public".to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "public" {
            Some(Arc::new(BisqueClientSchemaProvider {
                state: self.state.clone(),
            }))
        } else {
            None
        }
    }
}

/// DataFusion `SchemaProvider` that lists tables from a single catalog.
#[derive(Debug)]
struct BisqueClientSchemaProvider {
    state: Arc<CatalogState>,
}

#[async_trait::async_trait]
impl SchemaProvider for BisqueClientSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.state.tables.read().keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        Ok(self
            .state
            .tables
            .read()
            .get(name)
            .cloned()
            .map(|p| p as Arc<dyn TableProvider>))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.state.tables.read().contains_key(name)
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
    routing_store: Option<Arc<BisqueRoutingStore>>,
    table_name: String,
    needs_active_open: AtomicBool,
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

    fn active_version(&self) -> Option<u64> {
        let guard = self.active.load();
        guard.as_ref().as_ref().map(|ds| ds.manifest.version)
    }

    fn sealed_version(&self) -> Option<u64> {
        let guard = self.sealed.load();
        guard.as_ref().as_ref().map(|ds| ds.manifest.version)
    }

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
        self.ensure_datasets_open().await;

        let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(2);

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

    let _ = filters;

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
    #[serde(default)]
    s3_storage_options: HashMap<String, String>,
}

/// L9: Reuse a single reqwest client across fetch_catalog calls.
fn shared_http_client() -> &'static reqwest::Client {
    use std::sync::OnceLock;
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| reqwest::Client::new())
}

async fn fetch_catalog(
    cluster_url: &str,
    catalog_name: &str,
) -> Result<CatalogResponse, Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}/{}/{}", cluster_url, catalog_name, "_bisque/catalog");
    let resp = shared_http_client().get(&url).send().await?;
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

/// Reconnect constants (C1: jittered exponential backoff).
const WS_INITIAL_RECONNECT_DELAY_MS: u64 = 1000;
const WS_MAX_RECONNECT_DELAY_MS: u64 = 30_000;
/// Handshake timeout (H1).
const WS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Read timeout — if no server message in this window, consider connection dead (H2).
/// 45 seconds = 3 missed heartbeats at 15s interval.
const WS_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);
/// TTL refresh reconnect delay (M1).
const WS_TTL_REFRESH_DELAY_MS: u64 = 100;

async fn ws_listener_loop(
    cluster_url: &str,
    catalog_names: &[String],
    catalogs: Arc<RwLock<HashMap<String, Arc<CatalogState>>>>,
    shutdown: Arc<Notify>,
    shutdown_flag: Arc<AtomicBool>,
    mut ws_rx: mpsc::UnboundedReceiver<ClientMessage>,
    ws_tx: mpsc::UnboundedSender<ClientMessage>,
    client_store: Option<Arc<ClientStore>>,
    auth_token: &str,
) {
    let mut reconnect_delay_ms = WS_INITIAL_RECONNECT_DELAY_MS;

    loop {
        // Check the durable flag at the top of each iteration so we exit
        // even if the Notify wakeup was missed between select! branches.
        if shutdown_flag.load(Ordering::Acquire) {
            info!("WebSocket listener shut down (flag)");
            return;
        }

        // Use min sequence across all catalogs for delta sync
        let since = {
            let cats = catalogs.read();
            cats.values()
                .map(|s| s.last_seq.load(Ordering::Relaxed))
                .min()
                .unwrap_or(0)
        };

        // Wrap the connect+listen call so shutdown can interrupt it
        // even during TCP connect or WS handshake.
        let result = tokio::select! {
            r = ws_connect_and_listen(
                cluster_url,
                catalog_names,
                &catalogs,
                &shutdown,
                since,
                &mut ws_rx,
                &ws_tx,
                &client_store,
                auth_token,
            ) => r,
            _ = shutdown.notified() => {
                info!("WebSocket listener shut down cleanly");
                return;
            }
        };

        match result {
            Ok(WsDisconnectReason::Shutdown) => {
                info!("WebSocket listener shut down cleanly");
                return;
            }
            Ok(WsDisconnectReason::TtlRefresh) => {
                // M1: TTL refresh — reconnect immediately with short delay.
                debug!(
                    "WebSocket TTL refresh, reconnecting in {}ms",
                    WS_TTL_REFRESH_DELAY_MS
                );
                reconnect_delay_ms = WS_INITIAL_RECONNECT_DELAY_MS; // Reset backoff
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_millis(WS_TTL_REFRESH_DELAY_MS)) => {}
                    _ = shutdown.notified() => return,
                }
            }
            Err(e) => {
                // C1: Jittered exponential backoff.
                let jitter = 0.75 + (rand_jitter() * 0.5); // 0.75..1.25
                let delay = (reconnect_delay_ms as f64 * jitter) as u64;
                warn!(
                    "WebSocket connection lost: {}, reconnecting in {}ms...",
                    e, delay
                );
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_millis(delay)) => {}
                    _ = shutdown.notified() => return,
                }
                reconnect_delay_ms = (reconnect_delay_ms * 2).min(WS_MAX_RECONNECT_DELAY_MS);
            }
        }
    }
}

/// Simple pseudo-random jitter [0.0, 1.0) without pulling in rand crate.
/// M15: Uses multiple entropy sources to avoid deterministic sequences.
fn rand_jitter() -> f64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut hasher = DefaultHasher::new();
    std::time::Instant::now().hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    COUNTER.fetch_add(1, Ordering::Relaxed).hash(&mut hasher);
    (hasher.finish() % 10_000) as f64 / 10_000.0
}

/// Why the WS connection ended cleanly (vs Err for unexpected disconnects).
enum WsDisconnectReason {
    Shutdown,
    TtlRefresh,
}

async fn ws_connect_and_listen(
    cluster_url: &str,
    catalog_names: &[String],
    catalogs: &Arc<RwLock<HashMap<String, Arc<CatalogState>>>>,
    shutdown: &Arc<Notify>,
    since: u64,
    ws_rx: &mut mpsc::UnboundedReceiver<ClientMessage>,
    ws_tx: &mpsc::UnboundedSender<ClientMessage>,
    client_store: &Option<Arc<ClientStore>>,
    auth_token: &str,
) -> Result<WsDisconnectReason, Box<dyn std::error::Error + Send + Sync>> {
    // Connect to unified WS endpoint
    let ws_url = cluster_url
        .replace("http://", "ws://")
        .replace("https://", "wss://");
    let url = format!("{}/_bisque/ws", ws_url);

    info!(%url, "Connecting to WebSocket for catalog events");

    let uri: hyper::Uri = url.parse()?;
    let host = uri.host().unwrap_or("localhost");
    // C4: Use scheme-appropriate default port (443 for wss, 80 for ws).
    let is_tls = uri.scheme_str() == Some("wss");
    let port = uri.port_u16().unwrap_or(if is_tls { 443 } else { 80 });
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

    info!("WebSocket connected, waiting for server handshake");

    // H1: Handshake timeout — don't hang forever waiting for server handshake.
    let frame = tokio::time::timeout(WS_HANDSHAKE_TIMEOUT, ws.read_frame())
        .await
        .map_err(|_| "Server handshake timed out")??;
    if frame.opcode != fastwebsockets::OpCode::Binary {
        return Err("Expected binary handshake frame from server".into());
    }
    let server_handshake: ServerMessage = rmp_serde::from_slice(&frame.payload)?;
    match &server_handshake {
        ServerMessage::Handshake {
            protocol_version,
            session_id,
            ..
        } => {
            if *protocol_version != WS_PROTOCOL_VERSION {
                return Err(format!(
                    "Protocol version mismatch: server={}, client={}",
                    protocol_version, WS_PROTOCOL_VERSION
                )
                .into());
            }
            debug!(session_id, "Received server handshake");
        }
        _ => {
            return Err("Expected Handshake message from server".into());
        }
    }

    // Respond with client handshake (C3: includes auth token for authentication)
    let client_handshake = ClientMessage::Handshake {
        protocol_version: WS_PROTOCOL_VERSION,
        token: auth_token.to_string(),
        last_seen_seq: since,
        subscribe_catalogs: catalog_names.to_vec(),
    };
    let handshake_bytes = rmp_serde::to_vec_named(&client_handshake)?;
    ws.write_frame(fastwebsockets::Frame::binary(
        fastwebsockets::Payload::Owned(handshake_bytes),
    ))
    .await?;

    info!("Handshake complete, listening for catalog events");

    // Drain any queued outbound messages (pins from initial connect, heartbeats)
    while let Ok(msg) = ws_rx.try_recv() {
        let bytes = rmp_serde::to_vec_named(&msg)?;
        ws.write_frame(fastwebsockets::Frame::binary(
            fastwebsockets::Payload::Owned(bytes),
        ))
        .await?;
    }

    loop {
        tokio::select! {
            // H2: Read timeout — if no server message in WS_READ_TIMEOUT, connection is dead.
            result = tokio::time::timeout(WS_READ_TIMEOUT, ws.read_frame()) => {
                let frame = result.map_err(|_| "WebSocket read timed out (no heartbeat)")??;
                match frame.opcode {
                    fastwebsockets::OpCode::Binary => {
                        let action = handle_ws_message(&frame.payload, catalogs, ws_tx, client_store).await;
                        match action {
                            WsAction::Continue => {}
                            // M1: TTL refresh — server is closing for TTL, reconnect quickly.
                            WsAction::TtlRefresh => return Ok(WsDisconnectReason::TtlRefresh),
                            // C2: SnapshotRequired — re-fetch full catalog state.
                            // H7: Spawn to avoid blocking the WS receive loop.
                            WsAction::SnapshotRequired(catalog) => {
                                let cats = catalogs.clone();
                                let tx = ws_tx.clone();
                                let store = client_store.clone();
                                tokio::spawn(async move {
                                    handle_snapshot_required(&catalog, &cats, &tx, &store).await;
                                });
                            }
                        }
                    }
                    fastwebsockets::OpCode::Close => {
                        // H9: Server WS close frame should trigger reconnect, not clean shutdown.
                        // Only explicit app-level Close with shutdown reason is a clean exit.
                        debug!("WebSocket server sent close frame, will reconnect");
                        return Err("Server sent close frame".into());
                    }
                    _ => {}
                }
            }
            msg = ws_rx.recv() => {
                match msg {
                    Some(outbound) => {
                        let bytes = rmp_serde::to_vec_named(&outbound)?;
                        ws.write_frame(fastwebsockets::Frame::binary(
                            fastwebsockets::Payload::Owned(bytes),
                        )).await?;
                    }
                    None => {
                        let _ = ws.write_frame(fastwebsockets::Frame::close_raw(vec![].into())).await;
                        return Ok(WsDisconnectReason::Shutdown);
                    }
                }
            }
            _ = shutdown.notified() => {
                let _ = ws.write_frame(fastwebsockets::Frame::close_raw(vec![].into())).await;
                return Ok(WsDisconnectReason::Shutdown);
            }
        }
    }
}

/// Action returned by handle_ws_message to signal the connection loop.
enum WsAction {
    Continue,
    TtlRefresh,
    SnapshotRequired(String),
}

/// C2: Handle SnapshotRequired — re-fetch all tables for the given catalog.
async fn handle_snapshot_required(
    catalog_name: &str,
    catalogs: &Arc<RwLock<HashMap<String, Arc<CatalogState>>>>,
    ws_tx: &mpsc::UnboundedSender<ClientMessage>,
    client_store: &Option<Arc<ClientStore>>,
) {
    let catalog_state = catalogs.read().get(catalog_name).cloned();
    let Some(cat) = catalog_state else {
        warn!(catalog = %catalog_name, "SnapshotRequired for unknown catalog, ignoring");
        return;
    };

    info!(catalog = %catalog_name, "Performing full catalog re-sync (SnapshotRequired)");

    // Re-open every registered table's datasets from scratch.
    let table_names: Vec<String> = cat.tables.read().keys().cloned().collect();
    for table_name in &table_names {
        let provider = cat.tables.read().get(table_name).cloned();
        let Some(provider) = provider else { continue };
        let routing_store = &cat.routing_store;

        // Re-open active tier
        match open_remote_dataset(routing_store, table_name, "active").await {
            Ok(ds) => {
                let new_version = ds.version().version;
                let old_version = provider.active_version();
                let _ = ws_tx.send(ClientMessage::Pin {
                    catalog: catalog_name.to_string(),
                    table: table_name.clone(),
                    tier: "active".into(),
                    version: new_version,
                });
                provider.swap_active(Some(ds));
                if let Some(old_v) = old_version {
                    let _ = ws_tx.send(ClientMessage::Unpin {
                        catalog: catalog_name.to_string(),
                        table: table_name.clone(),
                        tier: "active".into(),
                        version: old_v,
                    });
                }
            }
            Err(e) => {
                warn!(catalog = %catalog_name, table = %table_name, "Failed to re-open active on snapshot: {}", e);
            }
        }

        // Re-open sealed tier
        match open_remote_dataset(routing_store, table_name, "sealed").await {
            Ok(ds) => {
                let new_version = ds.version().version;
                let old_version = provider.sealed_version();
                let _ = ws_tx.send(ClientMessage::Pin {
                    catalog: catalog_name.to_string(),
                    table: table_name.clone(),
                    tier: "sealed".into(),
                    version: new_version,
                });
                provider.swap_sealed(Some(ds));
                if let Some(old_v) = old_version {
                    let _ = ws_tx.send(ClientMessage::Unpin {
                        catalog: catalog_name.to_string(),
                        table: table_name.clone(),
                        tier: "sealed".into(),
                        version: old_v,
                    });
                }
            }
            Err(e) => {
                debug!(catalog = %catalog_name, table = %table_name, "No sealed tier on snapshot: {}", e);
            }
        }
    }

    // Reset seq to 0 so the next reconnect does a full replay.
    cat.last_seq.store(0, Ordering::Relaxed);
    info!(catalog = %catalog_name, tables = table_names.len(), "Snapshot re-sync complete");
}

async fn handle_ws_message(
    data: &[u8],
    catalogs: &Arc<RwLock<HashMap<String, Arc<CatalogState>>>>,
    ws_tx: &mpsc::UnboundedSender<ClientMessage>,
    client_store: &Option<Arc<ClientStore>>,
) -> WsAction {
    let msg: ServerMessage = match rmp_serde::from_slice(data) {
        Ok(m) => m,
        Err(e) => {
            warn!("Failed to parse server WS message: {}", e);
            return WsAction::Continue;
        }
    };

    match msg {
        ServerMessage::Handshake { session_id, .. } => {
            debug!(
                session_id,
                "Received server handshake (unexpected in message loop)"
            );
        }
        ServerMessage::CatalogEvent { event, .. } => {
            match serde_json::from_value::<CatalogEvent>(event) {
                Ok(catalog_event) => {
                    handle_catalog_event(catalog_event, catalogs, ws_tx, client_store).await;
                }
                Err(e) => {
                    warn!("Failed to decode CatalogEvent from Value: {}", e);
                }
            }
        }
        ServerMessage::SnapshotRequired { catalog, .. } => {
            warn!(catalog = %catalog, "Received snapshot_required — full catalog re-sync needed");
            return WsAction::SnapshotRequired(catalog);
        }
        ServerMessage::Heartbeat { .. } => {
            // Server heartbeat — no action needed for Rust SDK client
        }
        ServerMessage::Close { reason } => {
            debug!(%reason, "Server sent close message");
            if reason == "ttl_refresh" {
                return WsAction::TtlRefresh;
            }
        }
        _ => {
            // Response, OperationUpdate, OperationsSnapshot — not used by Rust SDK client
        }
    }
    WsAction::Continue
}

async fn handle_catalog_event(
    event: CatalogEvent,
    catalogs: &Arc<RwLock<HashMap<String, Arc<CatalogState>>>>,
    ws_tx: &mpsc::UnboundedSender<ClientMessage>,
    client_store: &Option<Arc<ClientStore>>,
) {
    let catalog_name = event.catalog.to_string();
    let catalog_state = catalogs.read().get(&catalog_name).cloned();
    let Some(catalog_state) = catalog_state else {
        debug!(catalog = %catalog_name, "Ignoring event for unknown catalog");
        return;
    };

    catalog_state.last_seq.store(event.seq, Ordering::Relaxed);
    let routing_store = &catalog_state.routing_store;

    match &event.event {
        CatalogEventKind::ActiveVersionBumped { table, version } => {
            let provider = catalog_state.tables.read().get(&**table).cloned();
            if let Some(provider) = provider {
                let old_version = provider.active_version();
                match open_remote_dataset(routing_store, table, "active").await {
                    Ok(ds) => {
                        let _ = ws_tx.send(ClientMessage::Pin {
                            catalog: catalog_name.clone(),
                            table: table.to_string(),
                            tier: "active".into(),
                            version: *version,
                        });
                        provider.swap_active(Some(ds));
                        if let Some(old_v) = old_version {
                            let _ = ws_tx.send(ClientMessage::Unpin {
                                catalog: catalog_name.clone(),
                                table: table.to_string(),
                                tier: "active".into(),
                                version: old_v,
                            });
                        }
                        debug!(catalog = %catalog_name, table = %table, seq = event.seq,
                               "Swapped active dataset");
                    }
                    Err(e) => {
                        warn!(table = %table, "Failed to re-open active dataset: {}", e);
                    }
                }
            }
            persist_version_update(client_store, &catalog_name, table, |entry| {
                entry.active_version = Some(*version);
            });
        }
        CatalogEventKind::SegmentSealed {
            table,
            active_version,
            sealed_version,
        } => {
            let provider = catalog_state.tables.read().get(&**table).cloned();
            if let Some(provider) = provider {
                let old_active_v = provider.active_version();
                let old_sealed_v = provider.sealed_version();

                match open_remote_dataset(routing_store, table, "active").await {
                    Ok(ds) => {
                        let _ = ws_tx.send(ClientMessage::Pin {
                            catalog: catalog_name.clone(),
                            table: table.to_string(),
                            tier: "active".into(),
                            version: *active_version,
                        });
                        provider.swap_active(Some(ds));
                        if let Some(old_v) = old_active_v {
                            let _ = ws_tx.send(ClientMessage::Unpin {
                                catalog: catalog_name.clone(),
                                table: table.to_string(),
                                tier: "active".into(),
                                version: old_v,
                            });
                        }
                    }
                    Err(e) => warn!(table = %table, "Failed to re-open active dataset: {}", e),
                }
                match open_remote_dataset(routing_store, table, "sealed").await {
                    Ok(ds) => {
                        let _ = ws_tx.send(ClientMessage::Pin {
                            catalog: catalog_name.clone(),
                            table: table.to_string(),
                            tier: "sealed".into(),
                            version: *sealed_version,
                        });
                        provider.swap_sealed(Some(ds));
                        if let Some(old_v) = old_sealed_v {
                            let _ = ws_tx.send(ClientMessage::Unpin {
                                catalog: catalog_name.clone(),
                                table: table.to_string(),
                                tier: "sealed".into(),
                                version: old_v,
                            });
                        }
                    }
                    Err(e) => warn!(table = %table, "Failed to re-open sealed dataset: {}", e),
                }
                debug!(catalog = %catalog_name, table = %table, seq = event.seq,
                       "Swapped active+sealed datasets after seal");
            }
            persist_version_update(client_store, &catalog_name, table, |entry| {
                entry.active_version = Some(*active_version);
                entry.sealed_version = Some(*sealed_version);
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
            catalog_state
                .tables
                .write()
                .insert(table.to_string(), provider);
            info!(catalog = %catalog_name, table = %table, seq = event.seq,
                  "Registered new table from event");

            if let Some(store) = client_store {
                let key = format!("{}\0{}", catalog_name, table);
                let entry = PersistedCatalogEntry {
                    active_segment: 1,
                    sealed_segment: None,
                    s3_dataset_uri: String::new(),
                    s3_storage_options: HashMap::new(),
                    active_version: None,
                    sealed_version: None,
                    schema_ipc: schema_ipc.clone(),
                };
                if let Err(e) = store.update_table(&key, &entry) {
                    warn!(table = %table, "Failed to persist new table: {}", e);
                }
            }
        }
        CatalogEventKind::TableDropped { table } => {
            let provider = catalog_state.tables.read().get(&**table).cloned();
            if let Some(provider) = provider {
                if let Some(v) = provider.active_version() {
                    let _ = ws_tx.send(ClientMessage::Unpin {
                        catalog: catalog_name.clone(),
                        table: table.to_string(),
                        tier: "active".into(),
                        version: v,
                    });
                }
                if let Some(v) = provider.sealed_version() {
                    let _ = ws_tx.send(ClientMessage::Unpin {
                        catalog: catalog_name.clone(),
                        table: table.to_string(),
                        tier: "sealed".into(),
                        version: v,
                    });
                }
            }
            catalog_state.tables.write().remove(&**table);
            info!(catalog = %catalog_name, table = %table, seq = event.seq,
                  "Removed dropped table");

            if let Some(store) = client_store {
                let key = format!("{}\0{}", catalog_name, table);
                if let Err(e) = store.remove_table(&key) {
                    warn!(table = %table, "Failed to remove persisted table: {}", e);
                }
            }
        }
        CatalogEventKind::SegmentPromoted { table, .. } => {
            let provider = catalog_state.tables.read().get(&**table).cloned();
            if let Some(provider) = provider {
                if let Some(v) = provider.sealed_version() {
                    let _ = ws_tx.send(ClientMessage::Unpin {
                        catalog: catalog_name.clone(),
                        table: table.to_string(),
                        tier: "sealed".into(),
                        version: v,
                    });
                }
                provider.swap_sealed(None);
                debug!(catalog = %catalog_name, table = %table, seq = event.seq,
                       "Cleared sealed dataset after promote");
            }
            persist_version_update(client_store, &catalog_name, table, |entry| {
                entry.sealed_segment = None;
                entry.sealed_version = None;
            });
        }
        CatalogEventKind::DataMutated {
            table,
            active_version,
            sealed_version,
            ..
        } => {
            let provider = catalog_state.tables.read().get(&**table).cloned();
            if let Some(provider) = provider {
                // Re-open active dataset to pick up new deletion vectors
                if let Some(new_v) = active_version {
                    let old_version = provider.active_version();
                    match open_remote_dataset(routing_store, table, "active").await {
                        Ok(ds) => {
                            let _ = ws_tx.send(ClientMessage::Pin {
                                catalog: catalog_name.clone(),
                                table: table.to_string(),
                                tier: "active".into(),
                                version: *new_v,
                            });
                            provider.swap_active(Some(ds));
                            if let Some(old_v) = old_version {
                                let _ = ws_tx.send(ClientMessage::Unpin {
                                    catalog: catalog_name.clone(),
                                    table: table.to_string(),
                                    tier: "active".into(),
                                    version: old_v,
                                });
                            }
                        }
                        Err(e) => {
                            warn!(table = %table, "Failed to re-open active dataset after mutation: {}", e);
                        }
                    }
                }
                // Re-open sealed dataset to pick up new deletion vectors
                if let Some(new_v) = sealed_version {
                    let old_version = provider.sealed_version();
                    match open_remote_dataset(routing_store, table, "sealed").await {
                        Ok(ds) => {
                            let _ = ws_tx.send(ClientMessage::Pin {
                                catalog: catalog_name.clone(),
                                table: table.to_string(),
                                tier: "sealed".into(),
                                version: *new_v,
                            });
                            provider.swap_sealed(Some(ds));
                            if let Some(old_v) = old_version {
                                let _ = ws_tx.send(ClientMessage::Unpin {
                                    catalog: catalog_name.clone(),
                                    table: table.to_string(),
                                    tier: "sealed".into(),
                                    version: old_v,
                                });
                            }
                        }
                        Err(e) => {
                            warn!(table = %table, "Failed to re-open sealed dataset after mutation: {}", e);
                        }
                    }
                }
                debug!(catalog = %catalog_name, table = %table, seq = event.seq,
                       "Refreshed datasets after data mutation");
            }
        }
    }

    // Update persisted sequence number
    if let Some(store) = client_store {
        let _seq_key = format!("seq:{}", catalog_name);
        if let Err(e) = store.update_seq(event.seq) {
            warn!("Failed to persist event seq {}: {}", event.seq, e);
        }
    }
}

/// Helper to read-modify-write a persisted table entry.
fn persist_version_update(
    client_store: &Option<Arc<ClientStore>>,
    catalog: &str,
    table: &str,
    mutate: impl FnOnce(&mut PersistedCatalogEntry),
) {
    let Some(store) = client_store else { return };

    let key = format!("{}\0{}", catalog, table);
    match store.load_state() {
        Ok(Some((_, tables))) => {
            if let Some(mut entry) = tables.get(&key).cloned() {
                mutate(&mut entry);
                if let Err(e) = store.update_table(&key, &entry) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn test_rand_jitter_range() {
        // rand_jitter should return values in [0.0, 1.0)
        for _ in 0..100 {
            let val = rand_jitter();
            assert!(val >= 0.0, "jitter {} should be >= 0.0", val);
            assert!(val < 1.0, "jitter {} should be < 1.0", val);
        }
    }

    #[test]
    fn test_rand_jitter_not_constant() {
        // Calling rand_jitter multiple times should not always return the same value
        let values: Vec<f64> = (0..10).map(|_| rand_jitter()).collect();
        let all_same = values.windows(2).all(|w| w[0] == w[1]);
        assert!(!all_same, "rand_jitter should produce varying values");
    }

    #[test]
    fn test_remote_lance_table_provider_new_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let provider = RemoteLanceTableProvider::new(schema.clone(), None, None);

        // Schema should be preserved
        assert_eq!(provider.schema.fields().len(), 2);
        assert_eq!(provider.schema.field(0).name(), "id");
        assert_eq!(provider.schema.field(1).name(), "name");

        // Versions should be None when no datasets are loaded
        assert!(provider.active_version().is_none());
        assert!(provider.sealed_version().is_none());
    }

    #[test]
    fn test_remote_lance_table_provider_table_type() {
        use datafusion::datasource::TableProvider;
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let provider = RemoteLanceTableProvider::new(schema, None, None);
        assert_eq!(provider.table_type(), datafusion_expr::TableType::Base);
    }

    #[test]
    fn test_catalog_response_deserialization() {
        let json = r#"{
            "tables": {
                "events": {
                    "active_segment": 1,
                    "sealed_segment": null,
                    "s3_dataset_uri": "s3://bucket/path",
                    "active_version": 5,
                    "sealed_version": null
                }
            }
        }"#;

        let resp: CatalogResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.tables.len(), 1);
        let info = resp.tables.get("events").unwrap();
        assert_eq!(info.active_segment, 1);
        assert!(info.sealed_segment.is_none());
        assert_eq!(info.s3_dataset_uri, "s3://bucket/path");
        assert_eq!(info.active_version, Some(5));
        assert!(info.sealed_version.is_none());
    }

    #[test]
    fn test_catalog_response_with_storage_options() {
        let json = r#"{
            "tables": {
                "metrics": {
                    "active_segment": 2,
                    "sealed_segment": 1,
                    "s3_dataset_uri": "s3://my-bucket/data",
                    "active_version": 10,
                    "sealed_version": 3,
                    "s3_storage_options": {
                        "region": "us-east-1"
                    }
                }
            }
        }"#;

        let resp: CatalogResponse = serde_json::from_str(json).unwrap();
        let info = resp.tables.get("metrics").unwrap();
        assert_eq!(info.sealed_segment, Some(1));
        assert_eq!(info.sealed_version, Some(3));
        assert_eq!(
            info.s3_storage_options.get("region").map(|s| s.as_str()),
            Some("us-east-1")
        );
    }

    #[test]
    fn test_bisque_client_catalog_provider_schema_names() {
        // BisqueClientCatalogProvider always reports "public" as its only schema
        let state = Arc::new(CatalogState {
            name: "test_catalog".to_string(),
            routing_store: Arc::new(BisqueRoutingStore::new(
                "http://localhost:9000",
                "test_catalog",
                Arc::new(CredentialConfig::default()),
            )),
            tables: RwLock::new(HashMap::new()),
            last_seq: AtomicU64::new(0),
        });

        let provider = BisqueClientCatalogProvider {
            state: state.clone(),
        };

        let names = provider.schema_names();
        assert_eq!(names, vec!["public".to_string()]);
    }

    #[test]
    fn test_bisque_client_catalog_provider_schema_lookup() {
        let state = Arc::new(CatalogState {
            name: "test_catalog".to_string(),
            routing_store: Arc::new(BisqueRoutingStore::new(
                "http://localhost:9000",
                "test_catalog",
                Arc::new(CredentialConfig::default()),
            )),
            tables: RwLock::new(HashMap::new()),
            last_seq: AtomicU64::new(0),
        });

        let provider = BisqueClientCatalogProvider {
            state: state.clone(),
        };

        // "public" should return Some
        assert!(provider.schema("public").is_some());
        // Any other name should return None
        assert!(provider.schema("other").is_none());
        assert!(provider.schema("information_schema").is_none());
    }
}

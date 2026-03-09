//! PostgreSQL wire protocol integration for bisque-lance.
//!
//! Exposes the storage engine over the PostgreSQL wire protocol via
//! `datafusion-postgres`, allowing any standard PostgreSQL client (psql,
//! DBeaver, pgcli, JDBC/ODBC drivers, BI tools) to query bisque-lance tables.
//!
//! The integration uses a dynamic [`CatalogProvider`] / [`SchemaProvider`]
//! that delegates to the live [`BisqueLance`] engine, so tables created or
//! dropped at runtime are immediately visible to new queries.
//!
//! # Usage
//!
//! ```ignore
//! use bisque_lance::postgres::{PostgresServerConfig, serve_postgres};
//!
//! serve_postgres(raft_node, PostgresServerConfig::default()).await?;
//! ```

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use parking_lot::RwLock;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use datafusion_postgres::ServerOptions;
use datafusion_postgres::datafusion_pg_catalog::pg_catalog::context::EmptyContextProvider;
use datafusion_postgres::datafusion_pg_catalog::setup_pg_catalog;
use tracing::info;

use crate::engine::BisqueLance;
use crate::query::BisqueLanceTableProvider;
use crate::raft::LanceRaftNode;

// ---------------------------------------------------------------------------
// SchemaProvider — delegates table lookups to the live BisqueLance engine
// ---------------------------------------------------------------------------

/// A DataFusion [`SchemaProvider`] backed by the live [`BisqueLance`] engine.
///
/// Every call to [`table_names`], [`table_exist`], or [`table`] reads the
/// current engine state, so newly created or dropped tables are reflected
/// immediately without any cache refresh.
pub struct BisqueLanceSchemaProvider {
    engine: Arc<BisqueLance>,
}

impl BisqueLanceSchemaProvider {
    pub fn new(engine: Arc<BisqueLance>) -> Self {
        Self { engine }
    }
}

impl fmt::Debug for BisqueLanceSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BisqueLanceSchemaProvider")
            .field("tables", &self.engine.list_tables())
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for BisqueLanceSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.engine.list_tables()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.engine.has_table(name)
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let table = match self.engine.get_table(name) {
            Some(t) => t,
            None => return Ok(None),
        };

        let schema = match table.schema().await {
            Some(s) => s,
            None => return Ok(None),
        };

        Ok(Some(Arc::new(BisqueLanceTableProvider::new(table, schema))))
    }
}

// ---------------------------------------------------------------------------
// CatalogProvider — presents a single "public" schema
// ---------------------------------------------------------------------------

/// A DataFusion [`CatalogProvider`] that exposes a `"public"` schema backed
/// by the live [`BisqueLance`] engine, plus any additional schemas registered
/// at runtime (e.g. `pg_catalog` for PostgreSQL compatibility).
pub struct BisqueLanceCatalogProvider {
    public: Arc<BisqueLanceSchemaProvider>,
    extra: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl BisqueLanceCatalogProvider {
    pub fn new(engine: Arc<BisqueLance>) -> Self {
        Self {
            public: Arc::new(BisqueLanceSchemaProvider::new(engine)),
            extra: RwLock::new(HashMap::new()),
        }
    }

    /// Create a catalog provider that registers the given schema provider under
    /// both `"public"` and `alias` so that `SELECT * FROM alias.table` works.
    pub fn new_with_schema(schema: Arc<BisqueLanceSchemaProvider>, alias: &str) -> Self {
        let mut extra = HashMap::new();
        extra.insert(alias.to_string(), schema.clone() as Arc<dyn SchemaProvider>);
        Self {
            public: schema,
            extra: RwLock::new(extra),
        }
    }
}

impl fmt::Debug for BisqueLanceCatalogProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let extra_names: Vec<String> = self.extra.read().keys().cloned().collect();
        f.debug_struct("BisqueLanceCatalogProvider")
            .field("public", &self.public)
            .field("extra_schemas", &extra_names)
            .finish()
    }
}

impl CatalogProvider for BisqueLanceCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let mut names = vec!["public".to_string()];
        names.extend(self.extra.read().keys().cloned());
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "public" {
            Some(self.public.clone())
        } else {
            self.extra.read().get(name).cloned()
        }
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.extra.write().insert(name.to_string(), schema))
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the PostgreSQL wire protocol server.
pub struct PostgresServerConfig {
    /// Host address to bind (default: `"127.0.0.1"`).
    pub host: String,
    /// Port to listen on (default: `5432`).
    pub port: u16,
}

impl Default for PostgresServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 5432,
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Start a PostgreSQL wire protocol server backed by a bisque-lance Raft node.
///
/// The server presents a `"bisque"` catalog with a `"public"` schema containing
/// all tables managed by the engine. Tables are resolved dynamically, so
/// creates/drops via Flight SQL or Raft are immediately visible.
///
/// This is currently **read-only** — `INSERT` / `UPDATE` / `DELETE` statements
/// will return a DataFusion error. Writes should be performed via Flight SQL
/// or the Raft write path.
pub async fn serve_postgres(
    raft_node: Arc<LanceRaftNode>,
    config: PostgresServerConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let engine = raft_node.engine();

    // Build a SessionContext with our dynamic catalog as the default
    let session_config = SessionConfig::new()
        .with_default_catalog_and_schema("bisque", "public")
        .with_create_default_catalog_and_schema(false);
    let ctx = SessionContext::new_with_config(session_config);

    let catalog = Arc::new(BisqueLanceCatalogProvider::new(engine.clone()));
    ctx.register_catalog("bisque", catalog);

    // Register pg_catalog tables for psql \dt / \d compatibility
    setup_pg_catalog(&ctx, "bisque", EmptyContextProvider)?;

    let opts = ServerOptions::new()
        .with_host(config.host.clone())
        .with_port(config.port);

    info!(
        host = %config.host,
        port = %config.port,
        "Starting PostgreSQL wire protocol server"
    );

    datafusion_postgres::serve(Arc::new(ctx), &opts).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BisqueLanceConfig;

    #[test]
    fn test_postgres_server_config_default() {
        let config = PostgresServerConfig::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 5432);
    }

    #[tokio::test]
    async fn test_catalog_provider_schema_names_include_public() {
        let tmp = tempfile::tempdir().unwrap();
        let lance_config = BisqueLanceConfig::new(tmp.path());
        let engine = Arc::new(BisqueLance::open(lance_config).await.unwrap());

        let provider = BisqueLanceCatalogProvider::new(engine);
        let names = provider.schema_names();
        assert!(
            names.contains(&"public".to_string()),
            "schema_names should contain 'public', got: {:?}",
            names
        );
    }

    #[tokio::test]
    async fn test_catalog_provider_schema_lookup() {
        let tmp = tempfile::tempdir().unwrap();
        let lance_config = BisqueLanceConfig::new(tmp.path());
        let engine = Arc::new(BisqueLance::open(lance_config).await.unwrap());

        let provider = BisqueLanceCatalogProvider::new(engine);

        // "public" schema should be resolvable
        assert!(provider.schema("public").is_some());

        // Unknown schema should return None
        assert!(provider.schema("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_register_schema_adds_to_schema_names() {
        let tmp = tempfile::tempdir().unwrap();
        let lance_config = BisqueLanceConfig::new(tmp.path());
        let engine = Arc::new(BisqueLance::open(lance_config).await.unwrap());

        let provider = BisqueLanceCatalogProvider::new(engine.clone());

        // Register a new schema (reuse the same engine-backed schema provider)
        let extra_schema = Arc::new(BisqueLanceSchemaProvider::new(engine));
        let prev = provider
            .register_schema("pg_catalog", extra_schema)
            .unwrap();
        assert!(prev.is_none(), "first registration should return None");

        // Now schema_names should include both "public" and "pg_catalog"
        let names = provider.schema_names();
        assert!(names.contains(&"public".to_string()));
        assert!(names.contains(&"pg_catalog".to_string()));

        // And the schema should be resolvable
        assert!(provider.schema("pg_catalog").is_some());
    }
}

//! BisqueLance multi-table storage engine — manages a registry of tables,
//! each with its own segment lifecycle (active → sealed → deep storage).

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{info, warn};

use crate::config::{BisqueLanceConfig, TableOpenConfig};
use crate::error::{Error, Result};
use crate::ipc;
use crate::table_engine::TableEngine;
use crate::types::{PersistedTableEntry, SegmentCatalog, TableSnapshot};

/// Multi-table storage engine. Holds a registry of [`TableEngine`] instances
/// keyed by table name.
///
/// Use [`create_table`](Self::create_table) to add tables and
/// [`get_table`](Self::get_table) to access them by name.
pub struct BisqueLance {
    config: BisqueLanceConfig,
    tables: RwLock<HashMap<String, Arc<TableEngine>>>,
}

impl BisqueLance {
    /// Create a new multi-table engine with no tables.
    ///
    /// Tables are added via [`create_table`](Self::create_table) or
    /// restored from a snapshot via [`restore_tables`](Self::restore_tables).
    pub async fn open(config: BisqueLanceConfig) -> Result<Self> {
        // Ensure the base data directory exists
        tokio::fs::create_dir_all(&config.local_data_dir).await?;

        info!(
            data_dir = %config.local_data_dir.display(),
            "BisqueLance multi-table engine opened"
        );

        Ok(Self {
            config,
            tables: RwLock::new(HashMap::new()),
        })
    }

    /// Create a new table and register it in the engine.
    ///
    /// Returns an `Arc<TableEngine>` for the newly created table.
    pub async fn create_table(
        &self,
        config: TableOpenConfig,
        catalog: Option<SegmentCatalog>,
    ) -> Result<Arc<TableEngine>> {
        let name = config.name.clone();

        // Check for existing table
        if self.tables.read().contains_key(&name) {
            return Err(Error::TableAlreadyExists(name));
        }

        let engine = Arc::new(TableEngine::open(config, catalog, None).await?);
        self.tables.write().insert(name.clone(), engine.clone());

        info!(table = %name, "Table created");
        Ok(engine)
    }

    /// Create or restore a table from snapshot data (catalog + schema history).
    pub async fn restore_table(
        &self,
        config: TableOpenConfig,
        snapshot: &TableSnapshot,
    ) -> Result<Arc<TableEngine>> {
        let name = config.name.clone();

        let engine = Arc::new(
            TableEngine::open(
                config,
                Some(snapshot.catalog.clone()),
                Some(snapshot.schema_history.clone()),
            )
            .await?,
        );
        self.tables.write().insert(name.clone(), engine.clone());

        info!(table = %name, "Table restored from snapshot");
        Ok(engine)
    }

    /// Get a table engine by name.
    pub fn get_table(&self, name: &str) -> Option<Arc<TableEngine>> {
        self.tables.read().get(name).cloned()
    }

    /// Get a table engine by name, returning an error if not found.
    pub fn require_table(&self, name: &str) -> Result<Arc<TableEngine>> {
        self.get_table(name)
            .ok_or_else(|| Error::TableNotFound(name.to_string()))
    }

    /// List all table names.
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    /// Check if a table exists.
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.read().contains_key(name)
    }

    /// Drop a table — shuts it down and removes from registry.
    ///
    /// Also deletes the table's local data directory.
    pub async fn drop_table(&self, name: &str) -> Result<()> {
        let engine = self
            .tables
            .write()
            .remove(name)
            .ok_or_else(|| Error::TableNotFound(name.to_string()))?;

        engine.shutdown().await?;

        // Remove local data directory
        let table_dir = self.config.table_data_dir(name);
        if table_dir.exists() {
            tokio::fs::remove_dir_all(&table_dir).await?;
            info!(table = %name, path = %table_dir.display(), "Removed table data directory");
        }

        info!(table = %name, "Table dropped");
        Ok(())
    }

    /// Get the node-level config.
    pub fn config(&self) -> &BisqueLanceConfig {
        &self.config
    }

    /// Collect full persisted entries for all tables (config + state).
    ///
    /// Used by the snapshot builder — this is the system-of-record data
    /// that gets serialized into Raft snapshots.
    pub fn table_entries(&self) -> HashMap<String, PersistedTableEntry> {
        let tables = self.tables.read();
        tables
            .iter()
            .map(|(name, engine)| {
                let entry = PersistedTableEntry {
                    config: engine.config().to_persisted(),
                    catalog: engine.catalog(),
                    flush_state: engine.flush_state(),
                    schema_history: engine.schema_history(),
                };
                (name.clone(), entry)
            })
            .collect()
    }

    /// Restore tables from MDBX manifest data (full persisted entries).
    ///
    /// Called during crash recovery — reads per-table entries from the
    /// manifest and recreates each `TableEngine` with the persisted config,
    /// catalog, flush state, and schema history.
    pub async fn restore_from_persisted_entries(
        &self,
        entries: HashMap<String, PersistedTableEntry>,
    ) -> Result<()> {
        for (table_name, entry) in &entries {
            let schema = if let Some(latest) = entry.schema_history.last() {
                match ipc::schema_from_ipc(&latest.schema_ipc) {
                    Ok(s) => Some(Arc::new(s)),
                    Err(e) => {
                        warn!(table = %table_name, "Failed to decode schema from manifest: {}", e);
                        None
                    }
                }
            } else {
                None
            };

            let config =
                TableOpenConfig::from_persisted(table_name, &entry.config, schema, &self.config);

            let snapshot = entry.to_snapshot();
            if let Err(e) = self.restore_table(config, &snapshot).await {
                warn!(table = %table_name, "Failed to restore table from manifest: {}", e);
            }
        }

        info!(tables = entries.len(), "Restored tables from MDBX manifest");
        Ok(())
    }

    /// Gracefully shutdown all tables.
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down BisqueLance engine (all tables)");
        let tables: Vec<(String, Arc<TableEngine>)> = {
            self.tables
                .read()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };

        for (name, engine) in tables {
            if let Err(e) = engine.shutdown().await {
                warn!(table = %name, "Error shutting down table: {}", e);
            }
        }

        self.tables.write().clear();
        Ok(())
    }
}

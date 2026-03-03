use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use lance::dataset::optimize::CompactionOptions;
use lance_index::IndexType;

/// Specifies an index to create on sealed segments.
#[derive(Debug, Clone)]
pub struct IndexSpec {
    /// Column(s) to index.
    pub columns: Vec<String>,
    /// Lance index type (e.g., `Inverted` for FTS, `IvfHnswSq` for vectors).
    pub index_type: IndexType,
    /// Optional index name. If None, Lance auto-generates one.
    pub name: Option<String>,
}

impl IndexSpec {
    /// Create an FTS (inverted) index spec on a column.
    pub fn fts(column: impl Into<String>) -> Self {
        let col = column.into();
        Self {
            name: Some(format!("{}_fts", col)),
            columns: vec![col],
            index_type: IndexType::Inverted,
        }
    }

    /// Create a vector index spec (IVF-HNSW-SQ) on a column.
    pub fn vector(column: impl Into<String>) -> Self {
        let col = column.into();
        Self {
            name: Some(format!("{}_vector", col)),
            columns: vec![col],
            index_type: IndexType::IvfHnswSq,
        }
    }

    /// Create a BTree scalar index spec on a column.
    pub fn btree(column: impl Into<String>) -> Self {
        let col = column.into();
        Self {
            name: Some(format!("{}_btree", col)),
            columns: vec![col],
            index_type: IndexType::BTree,
        }
    }

    /// Create a RTree scalar index spec on a column.
    pub fn rtree(column: impl Into<String>) -> Self {
        let col = column.into();
        Self {
            name: Some(format!("{}_rtree", col)),
            columns: vec![col],
            index_type: IndexType::RTree,
        }
    }
}

/// Node-level configuration for the BisqueLance multi-table storage engine.
///
/// Contains defaults that apply to all tables. Per-table overrides are
/// specified via [`TableOpenConfig`] when creating individual tables.
pub struct BisqueLanceConfig {
    /// Local NVMe path for data storage. Tables are stored under `{local_data_dir}/tables/{name}/`.
    pub local_data_dir: PathBuf,

    /// Default max age before sealing active segment.
    pub seal_max_age: Duration,

    /// Default max size in bytes before sealing active segment.
    pub seal_max_size: u64,

    /// Default Arrow schema for tables (used when not specified per-table).
    /// If None, the schema is inferred from the first write.
    pub schema: Option<arrow_schema::SchemaRef>,

    /// Default S3 URI prefix for deep storage (tables append their name).
    /// If None, S3 deep storage is disabled.
    pub s3_uri: Option<String>,

    /// S3 storage options (credentials, region, endpoint).
    pub s3_storage_options: HashMap<String, String>,

    /// Default max rows per file for S3 deep storage writes.
    pub s3_max_rows_per_file: usize,

    /// Default max rows per row group for S3 deep storage writes.
    pub s3_max_rows_per_group: usize,

    /// Default indices to create on sealed segments.
    pub seal_indices: Vec<IndexSpec>,

    /// Default target number of rows per fragment for compaction.
    pub compaction_target_rows_per_fragment: usize,

    /// Default whether to materialize deletions during compaction.
    pub compaction_materialize_deletions: bool,

    /// Default deletion threshold for compaction.
    pub compaction_deletion_threshold: f32,

    /// Default minimum fragments before compaction.
    pub compaction_min_fragments: usize,
}

impl BisqueLanceConfig {
    pub fn new(local_data_dir: impl Into<PathBuf>) -> Self {
        Self {
            local_data_dir: local_data_dir.into(),
            seal_max_age: Duration::from_secs(60),
            seal_max_size: 1024 * 1024 * 1024, // 1 GB
            schema: None,
            s3_uri: None,
            s3_storage_options: HashMap::new(),
            s3_max_rows_per_file: 5_000_000,
            s3_max_rows_per_group: 50_000,
            seal_indices: Vec::new(),
            compaction_target_rows_per_fragment: 1_048_576, // 1M rows
            compaction_materialize_deletions: true,
            compaction_deletion_threshold: 0.1,
            compaction_min_fragments: 4,
        }
    }

    pub fn with_seal_max_age(mut self, age: Duration) -> Self {
        self.seal_max_age = age;
        self
    }

    pub fn with_seal_max_size(mut self, size: u64) -> Self {
        self.seal_max_size = size;
        self
    }

    pub fn with_schema(mut self, schema: arrow_schema::SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_s3_uri(mut self, uri: impl Into<String>) -> Self {
        self.s3_uri = Some(uri.into());
        self
    }

    pub fn with_s3_storage_options(mut self, opts: HashMap<String, String>) -> Self {
        self.s3_storage_options = opts;
        self
    }

    pub fn with_s3_storage_option(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.s3_storage_options.insert(key.into(), value.into());
        self
    }

    pub fn with_s3_max_rows_per_file(mut self, n: usize) -> Self {
        self.s3_max_rows_per_file = n;
        self
    }

    pub fn with_s3_max_rows_per_group(mut self, n: usize) -> Self {
        self.s3_max_rows_per_group = n;
        self
    }

    /// Add an index to create on sealed segments.
    pub fn with_seal_index(mut self, spec: IndexSpec) -> Self {
        self.seal_indices.push(spec);
        self
    }

    /// Add multiple indices to create on sealed segments.
    pub fn with_seal_indices(mut self, specs: Vec<IndexSpec>) -> Self {
        self.seal_indices.extend(specs);
        self
    }

    pub fn with_compaction_target_rows_per_fragment(mut self, n: usize) -> Self {
        self.compaction_target_rows_per_fragment = n;
        self
    }

    pub fn with_compaction_materialize_deletions(mut self, v: bool) -> Self {
        self.compaction_materialize_deletions = v;
        self
    }

    pub fn with_compaction_deletion_threshold(mut self, t: f32) -> Self {
        self.compaction_deletion_threshold = t;
        self
    }

    pub fn with_compaction_min_fragments(mut self, n: usize) -> Self {
        self.compaction_min_fragments = n;
        self
    }

    /// Build Lance `CompactionOptions` from default config.
    pub fn compaction_options(&self) -> CompactionOptions {
        CompactionOptions {
            target_rows_per_fragment: self.compaction_target_rows_per_fragment,
            materialize_deletions: self.compaction_materialize_deletions,
            materialize_deletions_threshold: self.compaction_deletion_threshold,
            ..Default::default()
        }
    }

    /// Whether S3 deep storage is configured.
    pub fn has_s3(&self) -> bool {
        self.s3_uri.is_some()
    }

    // =========================================================================
    // Table path helpers
    // =========================================================================

    /// Path to a table's data directory.
    pub fn table_data_dir(&self, table_name: &str) -> PathBuf {
        self.local_data_dir.join("tables").join(table_name)
    }

    /// Build a [`TableOpenConfig`] for a table using this engine's defaults.
    pub fn build_table_config(
        &self,
        name: &str,
        schema: arrow_schema::SchemaRef,
    ) -> TableOpenConfig {
        TableOpenConfig {
            name: name.to_string(),
            table_data_dir: self.table_data_dir(name),
            schema: Some(schema),
            seal_indices: self.seal_indices.clone(),
            s3_uri: self.s3_uri.clone(),
            s3_storage_options: self.s3_storage_options.clone(),
            s3_max_rows_per_file: self.s3_max_rows_per_file,
            s3_max_rows_per_group: self.s3_max_rows_per_group,
            seal_max_age: self.seal_max_age,
            seal_max_size: self.seal_max_size,
            compaction_target_rows_per_fragment: self.compaction_target_rows_per_fragment,
            compaction_materialize_deletions: self.compaction_materialize_deletions,
            compaction_deletion_threshold: self.compaction_deletion_threshold,
            compaction_min_fragments: self.compaction_min_fragments,
        }
    }

    /// Build a [`TableOpenConfig`] for a table with custom S3 URI.
    pub fn build_table_config_with_s3(
        &self,
        name: &str,
        schema: arrow_schema::SchemaRef,
        s3_uri: impl Into<String>,
    ) -> TableOpenConfig {
        let mut config = self.build_table_config(name, schema);
        config.s3_uri = Some(s3_uri.into());
        config
    }
}

/// Per-table configuration for opening a table within a BisqueLance engine.
pub struct TableOpenConfig {
    /// Table name.
    pub name: String,
    /// Resolved path to this table's data directory (e.g., `{data_dir}/tables/{name}`).
    pub table_data_dir: PathBuf,
    /// Arrow schema for this table. If None, inferred from first write.
    pub schema: Option<arrow_schema::SchemaRef>,
    /// Indices to create on sealed segments for this table.
    pub seal_indices: Vec<IndexSpec>,
    /// S3 URI for deep storage of this table. If None, S3 disabled for this table.
    pub s3_uri: Option<String>,
    /// S3 storage options (credentials, region, endpoint).
    pub s3_storage_options: HashMap<String, String>,
    /// Max rows per file for S3 writes.
    pub s3_max_rows_per_file: usize,
    /// Max rows per row group for S3 writes.
    pub s3_max_rows_per_group: usize,
    /// Max age before sealing active segment.
    pub seal_max_age: Duration,
    /// Max size in bytes before sealing active segment.
    pub seal_max_size: u64,
    /// Target rows per fragment for compaction.
    pub compaction_target_rows_per_fragment: usize,
    /// Whether to materialize deletions during compaction.
    pub compaction_materialize_deletions: bool,
    /// Deletion threshold for compaction.
    pub compaction_deletion_threshold: f32,
    /// Minimum fragments before compaction runs.
    pub compaction_min_fragments: usize,
}

impl TableOpenConfig {
    /// Path to this table's segments directory.
    pub fn segments_dir(&self) -> PathBuf {
        self.table_data_dir.join("segments")
    }

    /// Path to a specific segment's Lance dataset within this table.
    pub fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.segments_dir().join(format!("{}.lance", segment_id))
    }

    /// Whether S3 deep storage is configured for this table.
    pub fn has_s3(&self) -> bool {
        self.s3_uri.is_some()
    }

    /// Build Lance `CompactionOptions` from this table's config.
    pub fn compaction_options(&self) -> CompactionOptions {
        CompactionOptions {
            target_rows_per_fragment: self.compaction_target_rows_per_fragment,
            materialize_deletions: self.compaction_materialize_deletions,
            materialize_deletions_threshold: self.compaction_deletion_threshold,
            ..Default::default()
        }
    }
}

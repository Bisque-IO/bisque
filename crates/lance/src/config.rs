use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lance::dataset::optimize::CompactionOptions;
use lance_index::IndexType;

use crate::types::{PersistedIndexSpec, PersistedTableConfig, duration_to_ms};

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

    /// Convert to a serializable representation.
    pub fn to_persisted(&self) -> PersistedIndexSpec {
        PersistedIndexSpec {
            columns: self.columns.clone(),
            index_type: format!("{:?}", self.index_type),
            name: self.name.clone(),
        }
    }

    /// Reconstruct from a serializable representation.
    pub fn from_persisted(p: &PersistedIndexSpec) -> Result<Self, String> {
        let index_type = match p.index_type.as_str() {
            "Scalar" => IndexType::Scalar,
            "BTree" => IndexType::BTree,
            "Bitmap" => IndexType::Bitmap,
            "Inverted" => IndexType::Inverted,
            "RTree" => IndexType::RTree,
            "IvfHnswSq" => IndexType::IvfHnswSq,
            other => return Err(format!("unknown IndexType: {}", other)),
        };
        Ok(Self {
            columns: p.columns.clone(),
            index_type,
            name: p.name.clone(),
        })
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

    /// Build a file manifest of all Lance segment files under `local_data_dir`.
    ///
    /// Walks every table's `segments/` directory, collecting relative paths
    /// and sizes for all files. Used by the snapshot builder to tell fresh
    /// nodes what files they need to fetch.
    pub async fn build_file_manifest(
        &self,
    ) -> std::io::Result<Vec<crate::types::SnapshotFileEntry>> {
        use tokio::fs;

        let mut entries = Vec::new();
        let tables_dir = self.local_data_dir.join("tables");

        if !tables_dir.exists() {
            return Ok(entries);
        }

        let mut table_dirs = fs::read_dir(&tables_dir).await?;
        while let Some(table_entry) = table_dirs.next_entry().await? {
            let segments_dir = table_entry.path().join("segments");
            if !segments_dir.is_dir() {
                continue;
            }

            // Recursively walk the segments directory
            let mut stack = vec![segments_dir];
            while let Some(dir) = stack.pop() {
                let mut dir_entries = fs::read_dir(&dir).await?;
                while let Some(entry) = dir_entries.next_entry().await? {
                    let path = entry.path();
                    let metadata = entry.metadata().await?;

                    if metadata.is_dir() {
                        stack.push(path);
                    } else if metadata.is_file() {
                        if let Ok(relative) = path.strip_prefix(&self.local_data_dir) {
                            entries.push(crate::types::SnapshotFileEntry {
                                relative_path: relative.to_string_lossy().into_owned(),
                                size: metadata.len(),
                            });
                        }
                    }
                }
            }
        }

        Ok(entries)
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

    /// Convert to a serializable representation.
    ///
    /// Does NOT include `s3_storage_options` (credentials), `table_data_dir`
    /// (derived at runtime), or `schema` (stored in schema_history).
    pub fn to_persisted(&self) -> PersistedTableConfig {
        PersistedTableConfig {
            seal_indices: self.seal_indices.iter().map(|s| s.to_persisted()).collect(),
            s3_uri: self.s3_uri.clone(),
            s3_max_rows_per_file: self.s3_max_rows_per_file,
            s3_max_rows_per_group: self.s3_max_rows_per_group,
            seal_max_age_ms: duration_to_ms(self.seal_max_age),
            seal_max_size: self.seal_max_size,
            compaction_target_rows_per_fragment: self.compaction_target_rows_per_fragment,
            compaction_materialize_deletions: self.compaction_materialize_deletions,
            compaction_deletion_threshold: self.compaction_deletion_threshold,
            compaction_min_fragments: self.compaction_min_fragments,
            batcher: None,
            processor: None,
        }
    }

    /// Reconstruct from persisted config, merging with engine-level settings
    /// for fields not stored in MDBX (credentials, data dir, schema).
    pub fn from_persisted(
        name: &str,
        persisted: &PersistedTableConfig,
        schema: Option<arrow_schema::SchemaRef>,
        engine_config: &BisqueLanceConfig,
    ) -> Self {
        let seal_indices = persisted
            .seal_indices
            .iter()
            .filter_map(|p| IndexSpec::from_persisted(p).ok())
            .collect();

        let schema = schema.unwrap_or_else(|| {
            use arrow_schema::{DataType, Field, Schema};
            Arc::new(Schema::new(vec![Field::new(
                "_placeholder",
                DataType::Null,
                true,
            )]))
        });

        Self {
            name: name.to_string(),
            table_data_dir: engine_config.table_data_dir(name),
            schema: Some(schema),
            seal_indices,
            s3_uri: persisted.s3_uri.clone(),
            s3_storage_options: engine_config.s3_storage_options.clone(),
            s3_max_rows_per_file: persisted.s3_max_rows_per_file,
            s3_max_rows_per_group: persisted.s3_max_rows_per_group,
            seal_max_age: Duration::from_millis(persisted.seal_max_age_ms),
            seal_max_size: persisted.seal_max_size,
            compaction_target_rows_per_fragment: persisted.compaction_target_rows_per_fragment,
            compaction_materialize_deletions: persisted.compaction_materialize_deletions,
            compaction_deletion_threshold: persisted.compaction_deletion_threshold,
            compaction_min_fragments: persisted.compaction_min_fragments,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    // =========================================================================
    // IndexSpec constructor tests
    // =========================================================================

    #[test]
    fn index_spec_fts_constructor() {
        let spec = IndexSpec::fts("content");
        assert_eq!(spec.columns, vec!["content".to_string()]);
        assert!(matches!(spec.index_type, IndexType::Inverted));
        assert_eq!(spec.name, Some("content_fts".to_string()));
    }

    #[test]
    fn index_spec_vector_constructor() {
        let spec = IndexSpec::vector("embedding");
        assert_eq!(spec.columns, vec!["embedding".to_string()]);
        assert!(matches!(spec.index_type, IndexType::IvfHnswSq));
        assert_eq!(spec.name, Some("embedding_vector".to_string()));
    }

    #[test]
    fn index_spec_btree_constructor() {
        let spec = IndexSpec::btree("user_id");
        assert_eq!(spec.columns, vec!["user_id".to_string()]);
        assert!(matches!(spec.index_type, IndexType::BTree));
        assert_eq!(spec.name, Some("user_id_btree".to_string()));
    }

    #[test]
    fn index_spec_rtree_constructor() {
        let spec = IndexSpec::rtree("location");
        assert_eq!(spec.columns, vec!["location".to_string()]);
        assert!(matches!(spec.index_type, IndexType::RTree));
        assert_eq!(spec.name, Some("location_rtree".to_string()));
    }

    // =========================================================================
    // IndexSpec to_persisted / from_persisted roundtrip tests
    // =========================================================================

    #[test]
    fn index_spec_roundtrip_fts() {
        let original = IndexSpec::fts("body");
        let persisted = original.to_persisted();
        assert_eq!(persisted.index_type, "Inverted");
        assert_eq!(persisted.columns, vec!["body".to_string()]);
        assert_eq!(persisted.name, Some("body_fts".to_string()));

        let restored = IndexSpec::from_persisted(&persisted).unwrap();
        assert_eq!(restored.columns, original.columns);
        assert!(matches!(restored.index_type, IndexType::Inverted));
        assert_eq!(restored.name, original.name);
    }

    #[test]
    fn index_spec_roundtrip_vector() {
        let original = IndexSpec::vector("vec");
        let persisted = original.to_persisted();
        assert_eq!(persisted.index_type, "IvfHnswSq");

        let restored = IndexSpec::from_persisted(&persisted).unwrap();
        assert!(matches!(restored.index_type, IndexType::IvfHnswSq));
        assert_eq!(restored.columns, original.columns);
        assert_eq!(restored.name, original.name);
    }

    #[test]
    fn index_spec_roundtrip_btree() {
        let original = IndexSpec::btree("id");
        let persisted = original.to_persisted();
        assert_eq!(persisted.index_type, "BTree");

        let restored = IndexSpec::from_persisted(&persisted).unwrap();
        assert!(matches!(restored.index_type, IndexType::BTree));
        assert_eq!(restored.columns, original.columns);
    }

    #[test]
    fn index_spec_roundtrip_rtree() {
        let original = IndexSpec::rtree("geo");
        let persisted = original.to_persisted();
        assert_eq!(persisted.index_type, "RTree");

        let restored = IndexSpec::from_persisted(&persisted).unwrap();
        assert!(matches!(restored.index_type, IndexType::RTree));
        assert_eq!(restored.columns, original.columns);
    }

    #[test]
    fn index_spec_from_persisted_unknown_type_returns_err() {
        let bad = PersistedIndexSpec {
            columns: vec!["col".to_string()],
            index_type: "UnknownType".to_string(),
            name: None,
        };
        let result = IndexSpec::from_persisted(&bad);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown IndexType"));
    }

    // =========================================================================
    // BisqueLanceConfig defaults
    // =========================================================================

    #[test]
    fn bisque_lance_config_new_defaults() {
        let cfg = BisqueLanceConfig::new("/tmp/data");
        assert_eq!(cfg.local_data_dir, PathBuf::from("/tmp/data"));
        assert_eq!(cfg.seal_max_age, Duration::from_secs(60));
        assert_eq!(cfg.seal_max_size, 1024 * 1024 * 1024);
        assert!(cfg.schema.is_none());
        assert!(cfg.s3_uri.is_none());
        assert!(cfg.s3_storage_options.is_empty());
        assert_eq!(cfg.s3_max_rows_per_file, 5_000_000);
        assert_eq!(cfg.s3_max_rows_per_group, 50_000);
        assert!(cfg.seal_indices.is_empty());
        assert_eq!(cfg.compaction_target_rows_per_fragment, 1_048_576);
        assert!(cfg.compaction_materialize_deletions);
        assert!((cfg.compaction_deletion_threshold - 0.1).abs() < f32::EPSILON);
        assert_eq!(cfg.compaction_min_fragments, 4);
    }

    // =========================================================================
    // BisqueLanceConfig builder methods
    // =========================================================================

    #[test]
    fn bisque_lance_config_with_seal_max_age() {
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_seal_max_age(Duration::from_secs(300));
        assert_eq!(cfg.seal_max_age, Duration::from_secs(300));
    }

    #[test]
    fn bisque_lance_config_with_seal_max_size() {
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_seal_max_size(512 * 1024);
        assert_eq!(cfg.seal_max_size, 512 * 1024);
    }

    #[test]
    fn bisque_lance_config_with_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_schema(schema.clone());
        assert!(cfg.schema.is_some());
        assert_eq!(cfg.schema.unwrap().fields().len(), 1);
    }

    #[test]
    fn bisque_lance_config_with_s3_uri() {
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_s3_uri("s3://my-bucket/prefix");
        assert_eq!(cfg.s3_uri, Some("s3://my-bucket/prefix".to_string()));
    }

    #[test]
    fn bisque_lance_config_with_s3_storage_options() {
        let mut opts = HashMap::new();
        opts.insert("region".to_string(), "us-east-1".to_string());
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_s3_storage_options(opts);
        assert_eq!(cfg.s3_storage_options.get("region").unwrap(), "us-east-1");
    }

    #[test]
    fn bisque_lance_config_with_s3_storage_option_single() {
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_s3_storage_option("endpoint", "http://localhost:9000");
        assert_eq!(
            cfg.s3_storage_options.get("endpoint").unwrap(),
            "http://localhost:9000"
        );
    }

    #[test]
    fn bisque_lance_config_with_s3_max_rows_per_file() {
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_s3_max_rows_per_file(1_000_000);
        assert_eq!(cfg.s3_max_rows_per_file, 1_000_000);
    }

    #[test]
    fn bisque_lance_config_with_s3_max_rows_per_group() {
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_s3_max_rows_per_group(10_000);
        assert_eq!(cfg.s3_max_rows_per_group, 10_000);
    }

    #[test]
    fn bisque_lance_config_with_seal_index() {
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_seal_index(IndexSpec::fts("text"))
            .with_seal_index(IndexSpec::btree("id"));
        assert_eq!(cfg.seal_indices.len(), 2);
    }

    #[test]
    fn bisque_lance_config_with_seal_indices() {
        let indices = vec![IndexSpec::fts("text"), IndexSpec::vector("emb")];
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_seal_indices(indices);
        assert_eq!(cfg.seal_indices.len(), 2);
    }

    #[test]
    fn bisque_lance_config_with_compaction_target_rows() {
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_compaction_target_rows_per_fragment(500_000);
        assert_eq!(cfg.compaction_target_rows_per_fragment, 500_000);
    }

    #[test]
    fn bisque_lance_config_with_compaction_materialize_deletions() {
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_compaction_materialize_deletions(false);
        assert!(!cfg.compaction_materialize_deletions);
    }

    #[test]
    fn bisque_lance_config_with_compaction_deletion_threshold() {
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_compaction_deletion_threshold(0.5);
        assert!((cfg.compaction_deletion_threshold - 0.5).abs() < f32::EPSILON);
    }

    #[test]
    fn bisque_lance_config_with_compaction_min_fragments() {
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_compaction_min_fragments(8);
        assert_eq!(cfg.compaction_min_fragments, 8);
    }

    // =========================================================================
    // BisqueLanceConfig: table_data_dir
    // =========================================================================

    #[test]
    fn bisque_lance_config_table_data_dir() {
        let cfg = BisqueLanceConfig::new("/data/lance");
        let dir = cfg.table_data_dir("my_table");
        assert_eq!(dir, PathBuf::from("/data/lance/tables/my_table"));
    }

    // =========================================================================
    // BisqueLanceConfig: has_s3
    // =========================================================================

    #[test]
    fn bisque_lance_config_has_s3_false_without_uri() {
        let cfg = BisqueLanceConfig::new("/tmp/data");
        assert!(!cfg.has_s3());
    }

    #[test]
    fn bisque_lance_config_has_s3_true_with_uri() {
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_s3_uri("s3://bucket/path");
        assert!(cfg.has_s3());
    }

    // =========================================================================
    // BisqueLanceConfig: build_table_config inherits engine defaults
    // =========================================================================

    #[test]
    fn bisque_lance_config_build_table_config_inherits_defaults() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let cfg = BisqueLanceConfig::new("/data")
            .with_seal_max_age(Duration::from_secs(120))
            .with_seal_max_size(2048)
            .with_s3_uri("s3://bucket")
            .with_s3_storage_option("region", "us-west-2")
            .with_s3_max_rows_per_file(100_000)
            .with_s3_max_rows_per_group(1_000)
            .with_seal_index(IndexSpec::fts("value"))
            .with_compaction_target_rows_per_fragment(500_000)
            .with_compaction_materialize_deletions(false)
            .with_compaction_deletion_threshold(0.25)
            .with_compaction_min_fragments(2);

        let tc = cfg.build_table_config("events", schema.clone());

        assert_eq!(tc.name, "events");
        assert_eq!(tc.table_data_dir, PathBuf::from("/data/tables/events"));
        assert_eq!(tc.schema.as_ref().unwrap().fields().len(), 2);
        assert_eq!(tc.seal_max_age, Duration::from_secs(120));
        assert_eq!(tc.seal_max_size, 2048);
        assert_eq!(tc.s3_uri, Some("s3://bucket".to_string()));
        assert_eq!(tc.s3_storage_options.get("region").unwrap(), "us-west-2");
        assert_eq!(tc.s3_max_rows_per_file, 100_000);
        assert_eq!(tc.s3_max_rows_per_group, 1_000);
        assert_eq!(tc.seal_indices.len(), 1);
        assert_eq!(tc.compaction_target_rows_per_fragment, 500_000);
        assert!(!tc.compaction_materialize_deletions);
        assert!((tc.compaction_deletion_threshold - 0.25).abs() < f32::EPSILON);
        assert_eq!(tc.compaction_min_fragments, 2);
    }

    // =========================================================================
    // BisqueLanceConfig: build_table_config_with_s3 overrides s3 uri
    // =========================================================================

    #[test]
    fn bisque_lance_config_build_table_config_with_s3_overrides() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let cfg = BisqueLanceConfig::new("/data")
            .with_s3_uri("s3://default-bucket");

        let tc = cfg.build_table_config_with_s3("logs", schema, "s3://override-bucket/logs");

        assert_eq!(tc.s3_uri, Some("s3://override-bucket/logs".to_string()));
        assert_eq!(tc.name, "logs");
    }

    // =========================================================================
    // BisqueLanceConfig: compaction_options
    // =========================================================================

    #[test]
    fn bisque_lance_config_compaction_options() {
        let cfg = BisqueLanceConfig::new("/tmp/data")
            .with_compaction_target_rows_per_fragment(2_000_000)
            .with_compaction_materialize_deletions(false)
            .with_compaction_deletion_threshold(0.3);

        let opts = cfg.compaction_options();
        assert_eq!(opts.target_rows_per_fragment, 2_000_000);
        assert!(!opts.materialize_deletions);
        assert!((opts.materialize_deletions_threshold - 0.3).abs() < f32::EPSILON);
    }

    // =========================================================================
    // BisqueLanceConfig: build_file_manifest
    // =========================================================================

    #[tokio::test]
    async fn bisque_lance_config_build_file_manifest_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = BisqueLanceConfig::new(tmp.path());
        // No tables/ dir exists at all
        let manifest = cfg.build_file_manifest().await.unwrap();
        assert!(manifest.is_empty());
    }

    #[tokio::test]
    async fn bisque_lance_config_build_file_manifest_nonexistent_dir() {
        let cfg = BisqueLanceConfig::new("/nonexistent/path/that/does/not/exist");
        let manifest = cfg.build_file_manifest().await.unwrap();
        assert!(manifest.is_empty());
    }

    #[tokio::test]
    async fn bisque_lance_config_build_file_manifest_finds_segment_files() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = BisqueLanceConfig::new(tmp.path());

        // Create tables/my_table/segments/ with some files
        let seg_dir = tmp.path().join("tables").join("my_table").join("segments");
        tokio::fs::create_dir_all(&seg_dir).await.unwrap();

        // Create a fake lance file in a subdirectory
        let lance_dir = seg_dir.join("1.lance").join("data");
        tokio::fs::create_dir_all(&lance_dir).await.unwrap();
        tokio::fs::write(lance_dir.join("0.lance"), b"fake data 1234")
            .await
            .unwrap();

        // Create another file directly in segments/
        tokio::fs::write(seg_dir.join("manifest.json"), b"{}")
            .await
            .unwrap();

        let manifest = cfg.build_file_manifest().await.unwrap();
        assert_eq!(manifest.len(), 2);

        let paths: Vec<&str> = manifest.iter().map(|e| e.relative_path.as_str()).collect();
        assert!(paths.iter().any(|p| p.contains("0.lance")));
        assert!(paths.iter().any(|p| p.contains("manifest.json")));

        // Verify sizes
        let lance_entry = manifest.iter().find(|e| e.relative_path.contains("0.lance")).unwrap();
        assert_eq!(lance_entry.size, 14); // b"fake data 1234".len()
    }

    #[tokio::test]
    async fn bisque_lance_config_build_file_manifest_skips_tables_without_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = BisqueLanceConfig::new(tmp.path());

        // Create a table directory without a segments/ subdirectory
        let table_dir = tmp.path().join("tables").join("no_segments_table");
        tokio::fs::create_dir_all(&table_dir).await.unwrap();
        tokio::fs::write(table_dir.join("metadata.json"), b"{}").await.unwrap();

        let manifest = cfg.build_file_manifest().await.unwrap();
        assert!(manifest.is_empty());
    }

    // =========================================================================
    // TableOpenConfig: segments_dir and segment_path
    // =========================================================================

    fn make_test_table_config(name: &str) -> TableOpenConfig {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let cfg = BisqueLanceConfig::new("/data/lance");
        cfg.build_table_config(name, schema)
    }

    #[test]
    fn table_open_config_segments_dir() {
        let tc = make_test_table_config("events");
        assert_eq!(
            tc.segments_dir(),
            PathBuf::from("/data/lance/tables/events/segments")
        );
    }

    #[test]
    fn table_open_config_segment_path() {
        let tc = make_test_table_config("events");
        assert_eq!(
            tc.segment_path(42),
            PathBuf::from("/data/lance/tables/events/segments/42.lance")
        );
    }

    #[test]
    fn table_open_config_segment_path_zero() {
        let tc = make_test_table_config("events");
        assert_eq!(
            tc.segment_path(0),
            PathBuf::from("/data/lance/tables/events/segments/0.lance")
        );
    }

    // =========================================================================
    // TableOpenConfig: has_s3
    // =========================================================================

    #[test]
    fn table_open_config_has_s3_false() {
        let tc = make_test_table_config("t");
        assert!(!tc.has_s3());
    }

    #[test]
    fn table_open_config_has_s3_true() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let cfg = BisqueLanceConfig::new("/data")
            .with_s3_uri("s3://bucket");
        let tc = cfg.build_table_config("t", schema);
        assert!(tc.has_s3());
    }

    // =========================================================================
    // TableOpenConfig: compaction_options
    // =========================================================================

    #[test]
    fn table_open_config_compaction_options() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let cfg = BisqueLanceConfig::new("/data")
            .with_compaction_target_rows_per_fragment(750_000)
            .with_compaction_materialize_deletions(true)
            .with_compaction_deletion_threshold(0.2);
        let tc = cfg.build_table_config("t", schema);

        let opts = tc.compaction_options();
        assert_eq!(opts.target_rows_per_fragment, 750_000);
        assert!(opts.materialize_deletions);
        assert!((opts.materialize_deletions_threshold - 0.2).abs() < f32::EPSILON);
    }

    // =========================================================================
    // TableOpenConfig: to_persisted / from_persisted roundtrip
    // =========================================================================

    #[test]
    fn table_open_config_to_persisted_from_persisted_roundtrip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, true),
        ]));
        let cfg = BisqueLanceConfig::new("/data")
            .with_seal_max_age(Duration::from_secs(180))
            .with_seal_max_size(4096)
            .with_s3_uri("s3://bucket/prefix")
            .with_s3_storage_option("region", "eu-west-1")
            .with_s3_max_rows_per_file(200_000)
            .with_s3_max_rows_per_group(5_000)
            .with_seal_index(IndexSpec::fts("text"))
            .with_seal_index(IndexSpec::btree("id"))
            .with_compaction_target_rows_per_fragment(800_000)
            .with_compaction_materialize_deletions(false)
            .with_compaction_deletion_threshold(0.15)
            .with_compaction_min_fragments(6);

        let original = cfg.build_table_config("roundtrip_table", schema.clone());
        let persisted = original.to_persisted();

        // Verify persisted fields
        assert_eq!(persisted.seal_max_age_ms, 180_000);
        assert_eq!(persisted.seal_max_size, 4096);
        assert_eq!(persisted.s3_uri, Some("s3://bucket/prefix".to_string()));
        assert_eq!(persisted.s3_max_rows_per_file, 200_000);
        assert_eq!(persisted.s3_max_rows_per_group, 5_000);
        assert_eq!(persisted.seal_indices.len(), 2);
        assert_eq!(persisted.compaction_target_rows_per_fragment, 800_000);
        assert!(!persisted.compaction_materialize_deletions);
        assert!((persisted.compaction_deletion_threshold - 0.15).abs() < f32::EPSILON);
        assert_eq!(persisted.compaction_min_fragments, 6);

        // Roundtrip back
        let restored = TableOpenConfig::from_persisted(
            "roundtrip_table",
            &persisted,
            Some(schema.clone()),
            &cfg,
        );

        assert_eq!(restored.name, "roundtrip_table");
        assert_eq!(restored.table_data_dir, cfg.table_data_dir("roundtrip_table"));
        assert_eq!(restored.seal_max_age, Duration::from_secs(180));
        assert_eq!(restored.seal_max_size, 4096);
        assert_eq!(restored.s3_uri, Some("s3://bucket/prefix".to_string()));
        assert_eq!(restored.s3_storage_options.get("region").unwrap(), "eu-west-1");
        assert_eq!(restored.s3_max_rows_per_file, 200_000);
        assert_eq!(restored.s3_max_rows_per_group, 5_000);
        assert_eq!(restored.seal_indices.len(), 2);
        assert_eq!(restored.compaction_target_rows_per_fragment, 800_000);
        assert!(!restored.compaction_materialize_deletions);
        assert!((restored.compaction_deletion_threshold - 0.15).abs() < f32::EPSILON);
        assert_eq!(restored.compaction_min_fragments, 6);

        // Verify schema was preserved
        let restored_schema = restored.schema.unwrap();
        assert_eq!(restored_schema.fields().len(), 2);
        assert_eq!(restored_schema.field(0).name(), "id");
        assert_eq!(restored_schema.field(1).name(), "text");
    }

    // =========================================================================
    // TableOpenConfig: from_persisted uses engine defaults for unset fields
    // =========================================================================

    #[test]
    fn table_open_config_from_persisted_uses_engine_defaults_for_unset() {
        let engine_cfg = BisqueLanceConfig::new("/engine/data")
            .with_s3_storage_option("access_key", "AKID123")
            .with_s3_storage_option("secret_key", "SECRET");

        // Persisted config has no s3_storage_options (they are not persisted),
        // but from_persisted should pull them from the engine config.
        let persisted = PersistedTableConfig {
            seal_indices: Vec::new(),
            s3_uri: Some("s3://recovered-bucket".to_string()),
            s3_max_rows_per_file: 3_000_000,
            s3_max_rows_per_group: 30_000,
            seal_max_age_ms: 90_000,
            seal_max_size: 2 * 1024 * 1024 * 1024,
            compaction_target_rows_per_fragment: 2_000_000,
            compaction_materialize_deletions: true,
            compaction_deletion_threshold: 0.05,
            compaction_min_fragments: 10,
            batcher: None,
            processor: None,
        };

        let restored = TableOpenConfig::from_persisted(
            "recovered_table",
            &persisted,
            None, // no schema provided
            &engine_cfg,
        );

        // table_data_dir should come from engine_config
        assert_eq!(
            restored.table_data_dir,
            PathBuf::from("/engine/data/tables/recovered_table")
        );

        // s3_storage_options should come from engine_config
        assert_eq!(restored.s3_storage_options.get("access_key").unwrap(), "AKID123");
        assert_eq!(restored.s3_storage_options.get("secret_key").unwrap(), "SECRET");

        // When schema is None, from_persisted provides a placeholder schema
        let schema = restored.schema.unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "_placeholder");

        // Verify persisted values were used (not engine defaults)
        assert_eq!(restored.seal_max_age, Duration::from_millis(90_000));
        assert_eq!(restored.seal_max_size, 2 * 1024 * 1024 * 1024);
        assert_eq!(restored.s3_uri, Some("s3://recovered-bucket".to_string()));
        assert_eq!(restored.s3_max_rows_per_file, 3_000_000);
        assert_eq!(restored.s3_max_rows_per_group, 30_000);
        assert_eq!(restored.compaction_target_rows_per_fragment, 2_000_000);
        assert!(restored.compaction_materialize_deletions);
        assert!((restored.compaction_deletion_threshold - 0.05).abs() < f32::EPSILON);
        assert_eq!(restored.compaction_min_fragments, 10);
    }

    #[test]
    fn table_open_config_from_persisted_filters_bad_indices() {
        let engine_cfg = BisqueLanceConfig::new("/data");

        let persisted = PersistedTableConfig {
            seal_indices: vec![
                PersistedIndexSpec {
                    columns: vec!["col_a".to_string()],
                    index_type: "BTree".to_string(),
                    name: Some("col_a_btree".to_string()),
                },
                PersistedIndexSpec {
                    columns: vec!["col_b".to_string()],
                    index_type: "BadType".to_string(),
                    name: None,
                },
                PersistedIndexSpec {
                    columns: vec!["col_c".to_string()],
                    index_type: "Inverted".to_string(),
                    name: Some("col_c_fts".to_string()),
                },
            ],
            ..PersistedTableConfig::default()
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Int32, false),
        ]));
        let restored = TableOpenConfig::from_persisted(
            "filter_test",
            &persisted,
            Some(schema),
            &engine_cfg,
        );

        // "BadType" should be filtered out, leaving 2 valid indices
        assert_eq!(restored.seal_indices.len(), 2);
        assert!(matches!(restored.seal_indices[0].index_type, IndexType::BTree));
        assert!(matches!(restored.seal_indices[1].index_type, IndexType::Inverted));
    }

    // =========================================================================
    // Builder chaining: all methods can be chained together
    // =========================================================================

    #[test]
    fn bisque_lance_config_full_builder_chain() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("msg", DataType::Utf8, true),
        ]));

        let cfg = BisqueLanceConfig::new("/nvme/bisque")
            .with_seal_max_age(Duration::from_secs(30))
            .with_seal_max_size(512 * 1024 * 1024)
            .with_schema(schema.clone())
            .with_s3_uri("s3://prod-bucket/lance")
            .with_s3_storage_option("region", "us-east-1")
            .with_s3_storage_option("endpoint", "https://s3.amazonaws.com")
            .with_s3_max_rows_per_file(10_000_000)
            .with_s3_max_rows_per_group(100_000)
            .with_seal_index(IndexSpec::fts("msg"))
            .with_seal_index(IndexSpec::btree("ts"))
            .with_compaction_target_rows_per_fragment(2_000_000)
            .with_compaction_materialize_deletions(true)
            .with_compaction_deletion_threshold(0.05)
            .with_compaction_min_fragments(8);

        assert_eq!(cfg.local_data_dir, PathBuf::from("/nvme/bisque"));
        assert_eq!(cfg.seal_max_age, Duration::from_secs(30));
        assert_eq!(cfg.seal_max_size, 512 * 1024 * 1024);
        assert!(cfg.schema.is_some());
        assert_eq!(cfg.s3_uri, Some("s3://prod-bucket/lance".to_string()));
        assert_eq!(cfg.s3_storage_options.len(), 2);
        assert_eq!(cfg.s3_max_rows_per_file, 10_000_000);
        assert_eq!(cfg.s3_max_rows_per_group, 100_000);
        assert_eq!(cfg.seal_indices.len(), 2);
        assert_eq!(cfg.compaction_target_rows_per_fragment, 2_000_000);
        assert!(cfg.compaction_materialize_deletions);
        assert!((cfg.compaction_deletion_threshold - 0.05).abs() < f32::EPSILON);
        assert_eq!(cfg.compaction_min_fragments, 8);
    }
}

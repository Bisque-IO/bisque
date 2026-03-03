use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

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
}

/// Configuration for the BisqueLance storage engine.
pub struct BisqueLanceConfig {
    /// Local NVMe path for segment storage.
    pub local_data_dir: PathBuf,

    /// Max age before sealing active segment.
    pub seal_max_age: Duration,

    /// Max size in bytes before sealing active segment.
    pub seal_max_size: u64,

    /// Arrow schema for the dataset.
    /// If None, the schema is inferred from the first write.
    pub schema: Option<arrow_schema::SchemaRef>,

    /// S3 URI for deep storage dataset (e.g., "s3://bucket/deep-storage.lance").
    /// If None, S3 deep storage is disabled.
    pub s3_uri: Option<String>,

    /// S3 storage options (credentials, region, endpoint).
    /// Keys follow the object_store S3ConfigKey conventions:
    /// `access_key_id`, `secret_access_key`, `region`, `endpoint`, etc.
    pub s3_storage_options: HashMap<String, String>,

    /// Max rows per file for S3 deep storage writes (larger = more efficient on S3).
    pub s3_max_rows_per_file: usize,

    /// Max rows per row group for S3 deep storage writes.
    pub s3_max_rows_per_group: usize,

    /// Indices to create on sealed segments.
    ///
    /// When a segment is sealed, these indices are built on the sealed
    /// dataset before it becomes available for queries. This enables
    /// FTS and vector search on sealed data.
    pub seal_indices: Vec<IndexSpec>,
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

    /// Path to the segments directory.
    pub fn segments_dir(&self) -> PathBuf {
        self.local_data_dir.join("segments")
    }

    /// Path to a specific segment's Lance dataset.
    pub fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.segments_dir().join(format!("{}.lance", segment_id))
    }

    /// Whether S3 deep storage is configured.
    pub fn has_s3(&self) -> bool {
        self.s3_uri.is_some()
    }
}

//! Federated sys catalog for cross-node OTel queries.
//!
//! Provides `SysSchemaProvider` that wraps `BisqueLanceSchemaProvider` and
//! augments every table with a `node_id: UInt64` column. For multi-node
//! deployments, it unions local data with remote peer data accessed via
//! each peer's S3-compatible HTTP API (Lance datasets over ObjectStore).

use std::any::Any;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, Session};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_physical_plan::union::UnionExec;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

use crate::postgres::BisqueLanceSchemaProvider;
use crate::query::{NodeIdExec, schema_with_node_id};

// ---------------------------------------------------------------------------
// PeerObjectStore — lightweight ObjectStore for a peer node's S3 API
// ---------------------------------------------------------------------------

/// ObjectStore implementation that routes reads to a peer node's S3 HTTP API.
///
/// Key format: `{table_name}/{tier}/{relative_path}` — same as the local S3 server.
pub struct PeerObjectStore {
    client: reqwest::Client,
    base_url: String,
}

impl PeerObjectStore {
    /// Create a new peer object store.
    ///
    /// `base_url` should be the peer's HTTP address (e.g. `http://10.0.0.2:3200`).
    /// The bucket name `sys` is used for sys catalog requests.
    pub fn new(client: reqwest::Client, peer_addr: SocketAddr) -> Self {
        Self {
            client,
            base_url: format!("http://{}/sys", peer_addr),
        }
    }

    fn object_url(&self, path: &str) -> String {
        if path.is_empty() {
            self.base_url.clone()
        } else {
            format!("{}/{}", self.base_url, path)
        }
    }
}

impl fmt::Debug for PeerObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerObjectStore")
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl fmt::Display for PeerObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PeerObjectStore({})", self.base_url)
    }
}

#[async_trait]
impl ObjectStore for PeerObjectStore {
    async fn put_opts(
        &self,
        _location: &ObjectPath,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> OsResult<PutResult> {
        Err(object_store::Error::NotImplemented)
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectPath,
        _opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotImplemented)
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> OsResult<GetResult> {
        let url = self.object_url(location.as_ref());
        let mut req = self.client.get(&url);

        if let Some(range) = &options.range {
            let range_header = match range {
                object_store::GetRange::Bounded(r) => {
                    format!("bytes={}-{}", r.start, r.end.saturating_sub(1))
                }
                object_store::GetRange::Offset(offset) => format!("bytes={}-", offset),
                object_store::GetRange::Suffix(n) => format!("bytes=-{}", n),
            };
            req = req.header("range", &range_header);
        }

        let resp = req.send().await.map_err(|e| object_store::Error::Generic {
            store: "PeerObjectStore",
            source: Box::new(e),
        })?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "not found on peer".into(),
            });
        }

        if !resp.status().is_success() {
            return Err(object_store::Error::Generic {
                store: "PeerObjectStore",
                source: format!("peer GET failed: {}", resp.status()).into(),
            });
        }

        let content_length = resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let range = if resp.status() == reqwest::StatusCode::PARTIAL_CONTENT {
            if let Some(cr) = resp.headers().get("content-range") {
                if let Ok(cr_str) = cr.to_str() {
                    parse_content_range(cr_str).unwrap_or(0..content_length)
                } else {
                    0..content_length
                }
            } else {
                0..content_length
            }
        } else {
            0..content_length
        };

        let last_modified = resp
            .headers()
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| chrono::DateTime::parse_from_rfc2822(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified,
            size: content_length,
            e_tag: etag,
            version: None,
        };

        let byte_stream = resp.bytes_stream().map(|result| {
            result.map_err(|e| object_store::Error::Generic {
                store: "PeerObjectStore",
                source: Box::new(e),
            })
        });

        Ok(GetResult {
            payload: GetResultPayload::Stream(byte_stream.boxed()),
            meta,
            range,
            attributes: Default::default(),
        })
    }

    async fn delete(&self, _location: &ObjectPath) -> OsResult<()> {
        Err(object_store::Error::NotImplemented)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'static, OsResult<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        let client = self.client.clone();
        let base_url = self.base_url.clone();

        futures::stream::once(async move {
            let url = format!(
                "{}?list-type=2&prefix={}&max-keys=10000",
                base_url, prefix_str,
            );
            let resp = client
                .get(&url)
                .send()
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "PeerObjectStore",
                    source: Box::new(e),
                })?;
            let xml = resp
                .text()
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "PeerObjectStore",
                    source: Box::new(e),
                })?;
            Ok(parse_list_xml(&xml))
        })
        .flat_map(|result: OsResult<Vec<ObjectMeta>>| match result {
            Ok(items) => futures::stream::iter(items.into_iter().map(Ok)).boxed(),
            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> OsResult<ListResult> {
        let prefix_str = prefix.map(|p| p.as_ref()).unwrap_or("");
        let url = format!(
            "{}?list-type=2&prefix={}&delimiter=/&max-keys=10000",
            self.base_url, prefix_str,
        );
        let resp =
            self.client
                .get(&url)
                .send()
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "PeerObjectStore",
                    source: Box::new(e),
                })?;
        let xml = resp
            .text()
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "PeerObjectStore",
                source: Box::new(e),
            })?;
        let objects = parse_list_xml(&xml);
        Ok(ListResult {
            common_prefixes: vec![],
            objects,
        })
    }

    async fn copy(&self, _from: &ObjectPath, _to: &ObjectPath) -> OsResult<()> {
        Err(object_store::Error::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &ObjectPath, _to: &ObjectPath) -> OsResult<()> {
        Err(object_store::Error::NotImplemented)
    }
}

// ---------------------------------------------------------------------------
// FederatedSysTableProvider
// ---------------------------------------------------------------------------

/// A `TableProvider` that unions local + remote peer Lance datasets for a
/// single sys table, with each source wrapped in `NodeIdExec` to inject the
/// `node_id` column.
pub struct FederatedSysTableProvider {
    table_name: String,
    local_node_id: u64,
    local_table: Arc<dyn TableProvider>,
    peers: Arc<Vec<(u64, SocketAddr)>>,
    schema: SchemaRef,
    http_client: reqwest::Client,
}

impl FederatedSysTableProvider {
    pub fn new(
        table_name: String,
        local_node_id: u64,
        local_table: Arc<dyn TableProvider>,
        peers: Arc<Vec<(u64, SocketAddr)>>,
        http_client: reqwest::Client,
    ) -> Self {
        let schema = Arc::new(schema_with_node_id(local_table.schema().as_ref()));
        Self {
            table_name,
            local_node_id,
            local_table,
            peers,
            schema,
            http_client,
        }
    }
}

impl fmt::Debug for FederatedSysTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FederatedSysTableProvider")
            .field("table_name", &self.table_name)
            .field("local_node_id", &self.local_node_id)
            .field("peers", &self.peers.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for FederatedSysTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // Adjust projection: if caller projects columns, we need to map indices
        // because the inner table doesn't have the node_id column.
        // The node_id column is the last column in our schema.
        let node_id_idx = self.schema.fields().len() - 1;

        let wants_node_id = projection.map(|p| p.contains(&node_id_idx)).unwrap_or(true);

        // Build inner projection without node_id
        let inner_projection: Option<Vec<usize>> = projection.map(|p| {
            p.iter()
                .filter(|&&idx| idx != node_id_idx)
                .copied()
                .collect()
        });

        let inner_proj_ref = inner_projection.as_ref();

        // 1. Local plan
        let local_plan = self
            .local_table
            .scan(state, inner_proj_ref, filters, limit)
            .await?;
        let local_plan = if wants_node_id {
            Arc::new(NodeIdExec::new(local_plan, self.local_node_id)) as Arc<dyn ExecutionPlan>
        } else {
            local_plan
        };

        let mut plans: Vec<Arc<dyn ExecutionPlan>> = vec![local_plan];

        // 2. Remote peer plans
        for &(peer_id, peer_addr) in self.peers.iter() {
            let store = Arc::new(PeerObjectStore::new(self.http_client.clone(), peer_addr));

            // Open remote Lance dataset via S3 API.
            // Key format: {table_name}/active/ (we query the active tier)
            let uri = format!("{}/active", self.table_name);
            let base_url = url::Url::parse(&format!("http://{}/sys/", peer_addr))
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let dataset = match lance::dataset::builder::DatasetBuilder::from_uri(&uri)
                .with_object_store(
                    store.clone() as Arc<dyn ObjectStore>,
                    base_url,
                    Arc::new(lance_table::io::commit::RenameCommitHandler),
                )
                .load()
                .await
            {
                Ok(ds) => ds,
                Err(e) => {
                    tracing::warn!(
                        peer_id,
                        %peer_addr,
                        table = %self.table_name,
                        "Failed to open remote dataset: {e}"
                    );
                    continue; // Skip unavailable peers gracefully
                }
            };

            let peer_plan = crate::query::build_scan_plan(
                &dataset,
                &self.local_table.schema(),
                inner_proj_ref,
                filters,
                limit,
                true,
            )
            .await;

            match peer_plan {
                Ok(plan) => {
                    let plan = if wants_node_id {
                        Arc::new(NodeIdExec::new(plan, peer_id)) as Arc<dyn ExecutionPlan>
                    } else {
                        plan
                    };
                    plans.push(plan);
                }
                Err(e) => {
                    tracing::warn!(
                        peer_id,
                        %peer_addr,
                        table = %self.table_name,
                        "Failed to build scan plan for peer: {e}"
                    );
                }
            }
        }

        if plans.len() == 1 {
            Ok(plans.into_iter().next().unwrap())
        } else {
            UnionExec::try_new(plans)
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }
}

// ---------------------------------------------------------------------------
// SysSchemaProvider — wraps BisqueLanceSchemaProvider
// ---------------------------------------------------------------------------

/// A DataFusion `SchemaProvider` for the `sys` schema that wraps each table
/// in a `FederatedSysTableProvider` to add `node_id` and union with peers.
pub struct SysSchemaProvider {
    inner: Arc<BisqueLanceSchemaProvider>,
    local_node_id: u64,
    peers: Arc<Vec<(u64, SocketAddr)>>,
    http_client: reqwest::Client,
}

impl SysSchemaProvider {
    pub fn new(
        inner: Arc<BisqueLanceSchemaProvider>,
        local_node_id: u64,
        peers: Arc<Vec<(u64, SocketAddr)>>,
    ) -> Self {
        Self {
            inner,
            local_node_id,
            peers,
            http_client: reqwest::Client::new(),
        }
    }
}

impl fmt::Debug for SysSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SysSchemaProvider")
            .field("local_node_id", &self.local_node_id)
            .field("peers", &self.peers.len())
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for SysSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let inner_table = match self.inner.table(name).await? {
            Some(t) => t,
            None => return Ok(None),
        };

        Ok(Some(Arc::new(FederatedSysTableProvider::new(
            name.to_string(),
            self.local_node_id,
            inner_table,
            self.peers.clone(),
            self.http_client.clone(),
        ))))
    }
}

// ---------------------------------------------------------------------------
// Helpers (same parsing as in client/s3_store.rs)
// ---------------------------------------------------------------------------

fn parse_content_range(header: &str) -> Option<std::ops::Range<u64>> {
    let rest = header.strip_prefix("bytes ")?;
    let (range_part, _total) = rest.split_once('/')?;
    let (start_str, end_str) = range_part.split_once('-')?;
    let start: u64 = start_str.parse().ok()?;
    let end: u64 = end_str.parse().ok()?;
    Some(start..end + 1)
}

fn parse_list_xml(xml: &str) -> Vec<ObjectMeta> {
    let mut items = Vec::new();
    for contents in xml.split("<Contents>").skip(1) {
        let end = match contents.find("</Contents>") {
            Some(i) => i,
            None => continue,
        };
        let block = &contents[..end];

        let key = extract_xml_tag(block, "Key").unwrap_or("");
        let size: u64 = extract_xml_tag(block, "Size")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let etag = extract_xml_tag(block, "ETag").map(|s| s.to_string());
        let last_modified = extract_xml_tag(block, "LastModified")
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);

        if !key.is_empty() {
            items.push(ObjectMeta {
                location: ObjectPath::from(key),
                last_modified,
                size,
                e_tag: etag,
                version: None,
            });
        }
    }
    items
}

/// Extract the text content of an XML tag without heap-allocating tag strings.
///
/// Searches for `<tag>...</tag>` by scanning bytes directly instead of
/// building format strings. Returns a borrowed slice into `block`.
fn extract_xml_tag<'a>(block: &'a str, tag: &str) -> Option<&'a str> {
    let tag_bytes = tag.as_bytes();
    let block_bytes = block.as_bytes();

    // Find "<tag>"
    let mut pos = 0;
    let content_start = loop {
        let remaining = &block_bytes[pos..];
        let idx = remaining.iter().position(|&b| b == b'<')?;
        let abs = pos + idx;
        let after_open = abs + 1;
        let after_tag = after_open + tag_bytes.len();
        if after_tag < block_bytes.len()
            && block_bytes[after_open..after_tag] == *tag_bytes
            && block_bytes[after_tag] == b'>'
        {
            break after_tag + 1;
        }
        pos = abs + 1;
    };

    // Find "</tag>" in the remainder
    let rest = &block_bytes[content_start..];
    let mut cpos = 0;
    let content_len = loop {
        let remaining = &rest[cpos..];
        let idx = remaining.iter().position(|&b| b == b'<')?;
        let abs = cpos + idx;
        if abs + 2 + tag_bytes.len() < rest.len()
            && rest[abs + 1] == b'/'
            && rest[abs + 2..abs + 2 + tag_bytes.len()] == *tag_bytes
            && rest[abs + 2 + tag_bytes.len()] == b'>'
        {
            break abs;
        }
        cpos = abs + 1;
    };

    Some(&block[content_start..content_start + content_len])
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::TableType;

    // -----------------------------------------------------------------------
    // XML helper tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_xml_tag_basic() {
        let xml = "<Key>foo/bar.lance</Key><Size>1024</Size>";
        assert_eq!(extract_xml_tag(xml, "Key"), Some("foo/bar.lance"));
        assert_eq!(extract_xml_tag(xml, "Size"), Some("1024"));
        assert_eq!(extract_xml_tag(xml, "Missing"), None);
    }

    #[test]
    fn test_extract_xml_tag_empty_value() {
        let xml = "<Key></Key>";
        assert_eq!(extract_xml_tag(xml, "Key"), Some(""));
    }

    #[test]
    fn test_extract_xml_tag_at_exact_boundary() {
        // Closing tag ends exactly at string boundary
        assert_eq!(extract_xml_tag("<Size>1024</Size>", "Size"), Some("1024"));
        assert_eq!(extract_xml_tag("<K>v</K>", "K"), Some("v"));
        // Empty content at boundary
        assert_eq!(extract_xml_tag("<X></X>", "X"), Some(""));
    }

    #[test]
    fn test_parse_content_range_basic() {
        assert_eq!(parse_content_range("bytes 0-499/1000"), Some(0..500));
        assert_eq!(parse_content_range("bytes 500-999/1000"), Some(500..1000));
    }

    #[test]
    fn test_parse_content_range_single_byte() {
        assert_eq!(parse_content_range("bytes 0-0/1"), Some(0..1));
    }

    #[test]
    fn test_parse_content_range_invalid() {
        assert_eq!(parse_content_range("invalid"), None);
        assert_eq!(parse_content_range("bytes /100"), None);
        assert_eq!(parse_content_range("bytes abc-def/100"), None);
    }

    #[test]
    fn test_parse_list_xml_multiple_entries() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents>
    <Key>table1/active/data.lance</Key>
    <Size>2048</Size>
    <ETag>"abc123"</ETag>
    <LastModified>2024-01-15T10:30:00Z</LastModified>
  </Contents>
  <Contents>
    <Key>table1/active/index.lance</Key>
    <Size>512</Size>
  </Contents>
</ListBucketResult>"#;

        let items = parse_list_xml(xml);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].location.as_ref(), "table1/active/data.lance");
        assert_eq!(items[0].size, 2048);
        assert_eq!(items[0].e_tag.as_deref(), Some("\"abc123\""));
        assert_eq!(items[1].location.as_ref(), "table1/active/index.lance");
        assert_eq!(items[1].size, 512);
    }

    #[test]
    fn test_parse_list_xml_empty() {
        let xml = r#"<ListBucketResult></ListBucketResult>"#;
        assert!(parse_list_xml(xml).is_empty());
    }

    #[test]
    fn test_parse_list_xml_skips_empty_key() {
        let xml = r#"<Contents><Key></Key><Size>100</Size></Contents>"#;
        // Empty key should be skipped
        assert!(parse_list_xml(xml).is_empty());
    }

    // -----------------------------------------------------------------------
    // FederatedSysTableProvider tests
    // -----------------------------------------------------------------------

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn test_mem_table() -> Arc<MemTable> {
        let schema = test_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    #[test]
    fn test_federated_schema_has_node_id() {
        let mem = test_mem_table();
        let provider = FederatedSysTableProvider::new(
            "test_table".into(),
            1,
            mem,
            Arc::new(vec![]),
            reqwest::Client::new(),
        );

        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "node_id");
        assert_eq!(schema.field(2).data_type(), &DataType::UInt64);
    }

    #[test]
    fn test_federated_table_type() {
        let mem = test_mem_table();
        let provider = FederatedSysTableProvider::new(
            "t".into(),
            1,
            mem,
            Arc::new(vec![]),
            reqwest::Client::new(),
        );
        assert_eq!(provider.table_type(), TableType::Base);
    }

    #[tokio::test]
    async fn test_federated_scan_single_node_no_peers() {
        let mem = test_mem_table();
        let provider = FederatedSysTableProvider::new(
            "t".into(),
            7,
            mem,
            Arc::new(vec![]),
            reqwest::Client::new(),
        );

        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let df = ctx
            .sql("SELECT id, name, node_id FROM t ORDER BY id")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // Verify node_id column is constant 7
        for batch in &batches {
            let node_ids = batch
                .column_by_name("node_id")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                assert_eq!(node_ids.value(i), 7);
            }
        }
    }

    #[tokio::test]
    async fn test_federated_scan_with_filter() {
        let mem = test_mem_table();
        let provider = FederatedSysTableProvider::new(
            "t".into(),
            1,
            mem,
            Arc::new(vec![]),
            reqwest::Client::new(),
        );

        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let df = ctx
            .sql("SELECT id, node_id FROM t WHERE id > 1")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_federated_scan_select_star() {
        let mem = test_mem_table();
        let provider = FederatedSysTableProvider::new(
            "t".into(),
            5,
            mem,
            Arc::new(vec![]),
            reqwest::Client::new(),
        );

        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let df = ctx.sql("SELECT * FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // SELECT * should include node_id
        let schema = batches[0].schema();
        assert!(schema.column_with_name("node_id").is_some());
    }

    #[tokio::test]
    async fn test_federated_scan_without_node_id_projection() {
        let mem = test_mem_table();
        let provider = FederatedSysTableProvider::new(
            "t".into(),
            1,
            mem,
            Arc::new(vec![]),
            reqwest::Client::new(),
        );

        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        // Only select non-node_id columns
        let df = ctx.sql("SELECT id, name FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // node_id should NOT be in the output schema
        let schema = batches[0].schema();
        assert!(schema.column_with_name("node_id").is_none());
    }

    // -----------------------------------------------------------------------
    // SysSchemaProvider tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_peer_object_store_display() {
        let store = PeerObjectStore::new(reqwest::Client::new(), "127.0.0.1:3200".parse().unwrap());
        let display = format!("{}", store);
        assert!(display.contains("127.0.0.1:3200"));
    }

    #[test]
    fn test_peer_object_store_url() {
        let store = PeerObjectStore::new(reqwest::Client::new(), "10.0.0.1:3200".parse().unwrap());
        assert_eq!(
            store.object_url("table1/active/data.lance"),
            "http://10.0.0.1:3200/sys/table1/active/data.lance"
        );
        assert_eq!(store.object_url(""), "http://10.0.0.1:3200/sys");
    }
}

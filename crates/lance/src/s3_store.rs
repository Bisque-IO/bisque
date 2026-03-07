//! Routing `ObjectStore` for bisque-lance client mode.
//!
//! Routes reads based on data temperature:
//! - **Hot/warm** paths (active + sealed segments) → HTTP requests to the cluster's S3 API
//! - **Cold** paths (deep storage) → delegate to a real S3 `ObjectStore`
//!
//! This enables a remote bisque-lance instance to transparently query data across
//! all tiers without direct access to the cluster's local filesystem.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use parking_lot::RwLock;
use serde::Deserialize;

use crate::cold_store::CredentialConfig;

/// Routing `ObjectStore` that directs reads to a bisque-lance cluster (hot/warm)
/// or a real S3 store (cold), based on the segment catalog.
pub struct BisqueRoutingStore {
    /// HTTP client for requests to the bisque-lance cluster.
    client: reqwest::Client,
    /// Base URL of the bisque-lance S3 API (e.g., `http://cluster:3300`).
    cluster_url: String,
    /// Bucket name used in S3 path construction.
    bucket: String,
    /// Client credentials for S3 ObjectStore resolution.
    credentials: Arc<CredentialConfig>,
    /// Cached segment catalog from the cluster.
    catalog: Arc<RwLock<CachedCatalog>>,
}

struct CachedCatalog {
    tables: HashMap<String, SegmentInfo>,
    fetched_at: Instant,
    ttl: std::time::Duration,
}

#[derive(Debug, Clone, Deserialize)]
struct SegmentInfo {
    #[allow(dead_code)]
    active_segment: u64,
    #[allow(dead_code)]
    sealed_segment: Option<u64>,
    s3_dataset_uri: String,
    /// Non-credential S3 storage options from the server (region, endpoint, etc.).
    #[serde(default)]
    s3_storage_options: HashMap<String, String>,
}

#[derive(Deserialize)]
struct CatalogResponse {
    tables: HashMap<String, SegmentInfo>,
}

impl CachedCatalog {
    fn empty() -> Self {
        Self {
            tables: HashMap::new(),
            fetched_at: Instant::now() - std::time::Duration::from_secs(3600),
            ttl: std::time::Duration::from_secs(5),
        }
    }

    fn is_stale(&self) -> bool {
        self.fetched_at.elapsed() > self.ttl
    }
}

impl BisqueRoutingStore {
    /// Create a new routing store.
    ///
    /// - `cluster_url`: Base URL of the bisque-lance S3 API (e.g., `http://host:3300`)
    /// - `bucket`: Bucket name for S3 path construction
    /// - `credentials`: Client credentials for S3 ObjectStore resolution
    pub fn new(
        cluster_url: impl Into<String>,
        bucket: impl Into<String>,
        credentials: Arc<CredentialConfig>,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            cluster_url: cluster_url.into().trim_end_matches('/').to_string(),
            bucket: bucket.into(),
            credentials,
            catalog: Arc::new(RwLock::new(CachedCatalog::empty())),
        }
    }

    /// Resolve the cold ObjectStore for a given path.
    ///
    /// Looks up the table name from the path, finds its `s3_dataset_uri`,
    /// and resolves through `CredentialConfig` with server-provided options.
    fn resolve_cold_store(&self, path: &ObjectPath) -> Option<Arc<dyn ObjectStore>> {
        let path_str = path.as_ref();
        let table_name = path_str.split('/').next()?;

        let catalog = self.catalog.read();
        let info = catalog.tables.get(table_name)?;
        if info.s3_dataset_uri.is_empty() {
            return None;
        }

        self.credentials
            .resolve(table_name, &info.s3_dataset_uri, &info.s3_storage_options)
            .ok()
            .map(|(store, _)| store)
    }

    /// Refresh the segment catalog from the cluster.
    async fn refresh_catalog(&self) -> OsResult<()> {
        let url = format!("{}/_bisque/catalog", self.object_url(""));
        let resp =
            self.client
                .get(&url)
                .send()
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "BisqueRoutingStore",
                    source: Box::new(e),
                })?;

        if !resp.status().is_success() {
            return Err(object_store::Error::Generic {
                store: "BisqueRoutingStore",
                source: format!("catalog fetch failed: {}", resp.status()).into(),
            });
        }

        let catalog_resp: CatalogResponse =
            resp.json()
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "BisqueRoutingStore",
                    source: Box::new(e),
                })?;

        let mut cached = self.catalog.write();
        cached.tables = catalog_resp.tables;
        cached.fetched_at = Instant::now();
        Ok(())
    }

    /// Ensure the catalog is fresh, refreshing if stale.
    async fn ensure_catalog(&self) -> OsResult<()> {
        if self.catalog.read().is_stale() {
            self.refresh_catalog().await?;
        }
        Ok(())
    }

    /// Check if a path corresponds to a hot/warm segment on the cluster.
    ///
    /// Key format: `{table_name}/{tier}/{relative_path}`
    /// where tier is `active` or `sealed`.
    fn is_cluster_path(&self, path: &ObjectPath) -> bool {
        let path_str = path.as_ref();
        let mut parts = path_str.splitn(3, '/');

        let table_name = match parts.next() {
            Some(t) if !t.is_empty() => t,
            _ => return false,
        };

        match parts.next() {
            Some("active") | Some("sealed") => {}
            _ => return false,
        };

        // Check that this table exists in the catalog
        let catalog = self.catalog.read();
        catalog.tables.contains_key(table_name)
    }

    /// Build the full URL for a cluster S3 request.
    fn object_url(&self, path: &str) -> String {
        if path.is_empty() {
            format!("{}/{}", self.cluster_url, self.bucket)
        } else {
            format!("{}/{}/{}", self.cluster_url, self.bucket, path)
        }
    }

    /// List objects from the cluster's S3 API, parsing the ListObjectsV2 XML response.
    async fn cluster_list(&self, prefix: &str) -> OsResult<Vec<ObjectMeta>> {
        let url = format!(
            "{}?list-type=2&prefix={}&max-keys=10000",
            self.object_url(""),
            url_encode(prefix),
        );

        let resp =
            self.client
                .get(&url)
                .send()
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "BisqueRoutingStore",
                    source: Box::new(e),
                })?;

        if !resp.status().is_success() {
            return Err(object_store::Error::Generic {
                store: "BisqueRoutingStore",
                source: format!("cluster LIST failed: {}", resp.status()).into(),
            });
        }

        let xml = resp
            .text()
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "BisqueRoutingStore",
                source: Box::new(e),
            })?;

        Ok(parse_list_xml(&xml))
    }

    /// Perform a GET request against the cluster's S3 API.
    async fn cluster_get(&self, path: &ObjectPath, options: GetOptions) -> OsResult<GetResult> {
        let url = self.object_url(path.as_ref());
        let mut req = self.client.get(&url);

        // Apply range header if specified
        if let Some(range) = &options.range {
            let range_header = match range {
                object_store::GetRange::Bounded(r) => {
                    format!("bytes={}-{}", r.start, r.end.saturating_sub(1))
                }
                object_store::GetRange::Offset(offset) => {
                    format!("bytes={}-", offset)
                }
                object_store::GetRange::Suffix(n) => {
                    format!("bytes=-{}", n)
                }
            };
            req = req.header("range", &range_header);
        }

        let resp = req.send().await.map_err(|e| object_store::Error::Generic {
            store: "BisqueRoutingStore",
            source: Box::new(e),
        })?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(object_store::Error::NotFound {
                path: path.to_string(),
                source: "not found on cluster".into(),
            });
        }

        if !resp.status().is_success() {
            return Err(object_store::Error::Generic {
                store: "BisqueRoutingStore",
                source: format!("cluster GET failed: {}", resp.status()).into(),
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

        // Determine range from response
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

        // Parse Last-Modified from response header if available
        let last_modified = resp
            .headers()
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| chrono::DateTime::parse_from_rfc2822(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);

        let meta = ObjectMeta {
            location: path.clone(),
            last_modified,
            size: content_length,
            e_tag: etag,
            version: None,
        };

        // Stream the response body directly — no buffering
        let byte_stream = resp.bytes_stream().map(|result| {
            result.map_err(|e| object_store::Error::Generic {
                store: "BisqueRoutingStore",
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
}

impl fmt::Debug for BisqueRoutingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BisqueRoutingStore")
            .field("cluster_url", &self.cluster_url)
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl fmt::Display for BisqueRoutingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BisqueRoutingStore({})", self.cluster_url)
    }
}

#[async_trait]
impl ObjectStore for BisqueRoutingStore {
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
        self.ensure_catalog().await?;
        if self.is_cluster_path(location) {
            self.cluster_get(location, options).await
        } else {
            let store =
                self.resolve_cold_store(location)
                    .ok_or_else(|| object_store::Error::Generic {
                        store: "BisqueRoutingStore",
                        source: format!("no cold store configured for path: {}", location).into(),
                    })?;
            store.get_opts(location, options).await
        }
    }

    async fn delete(&self, _location: &ObjectPath) -> OsResult<()> {
        Err(object_store::Error::NotImplemented)
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        let prefix_owned = prefix.cloned();
        let credentials = self.credentials.clone();
        let catalog = self.catalog.clone();
        // Clone for the first async block
        let client1 = self.client.clone();
        let cluster_url1 = self.cluster_url.clone();
        let bucket1 = self.bucket.clone();
        // Clone for the flat_map closure
        let client2 = self.client.clone();
        let cluster_url2 = self.cluster_url.clone();
        let bucket2 = self.bucket.clone();
        let catalog2 = self.catalog.clone();

        futures::stream::once(async move {
            // Ensure catalog is fresh
            ensure_catalog_static(&client1, &cluster_url1, &bucket1, &catalog).await;

            let is_cluster = prefix_owned
                .as_ref()
                .map(|p| is_cluster_path_static(p, &catalog))
                .unwrap_or(false);

            Ok((is_cluster, prefix_owned))
        })
        .flat_map(
            move |result: OsResult<(bool, Option<ObjectPath>)>| match result {
                Ok((true, prefix)) => {
                    let prefix_str = prefix
                        .as_ref()
                        .map(|p| p.as_ref())
                        .unwrap_or("")
                        .to_string();
                    let client = client2.clone();
                    let cluster_url = cluster_url2.clone();
                    let bucket = bucket2.clone();

                    futures::stream::once(async move {
                        let url = format!(
                            "{}/{}?list-type=2&prefix={}&max-keys=10000",
                            cluster_url,
                            bucket,
                            url_encode(&prefix_str),
                        );
                        let resp = client.get(&url).send().await.map_err(|e| {
                            object_store::Error::Generic {
                                store: "BisqueRoutingStore",
                                source: Box::new(e),
                            }
                        })?;
                        let xml = resp
                            .text()
                            .await
                            .map_err(|e| object_store::Error::Generic {
                                store: "BisqueRoutingStore",
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
                Ok((false, prefix)) => {
                    // Resolve cold store for this path using server options + client credentials
                    let store = prefix.as_ref().and_then(|p| {
                        let table_name = p.as_ref().split('/').next()?;
                        let catalog = catalog2.read();
                        let info = catalog.tables.get(table_name)?;
                        if info.s3_dataset_uri.is_empty() {
                            return None;
                        }
                        credentials
                            .resolve(table_name, &info.s3_dataset_uri, &info.s3_storage_options)
                            .ok()
                            .map(|(s, _)| s)
                    });

                    match store {
                        Some(s) => s.list(prefix.as_ref()),
                        None => futures::stream::once(async {
                            Err(object_store::Error::Generic {
                                store: "BisqueRoutingStore",
                                source: "no cold store configured".into(),
                            })
                        })
                        .boxed(),
                    }
                }
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            },
        )
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> OsResult<ListResult> {
        self.ensure_catalog().await?;

        let is_cluster = prefix.map(|p| self.is_cluster_path(p)).unwrap_or(false);

        if is_cluster {
            let prefix_str = prefix.map(|p| p.as_ref()).unwrap_or("");
            let items = self.cluster_list(prefix_str).await?;

            // Split into objects directly under prefix vs common prefixes
            let delimiter = "/";
            let prefix_len = if prefix_str.is_empty() {
                0
            } else {
                prefix_str.len() + 1
            };

            let mut objects = Vec::new();
            let mut common_prefixes = std::collections::HashSet::new();

            for meta in items {
                let key = meta.location.as_ref();
                let rest = if prefix_len > 0 && key.len() > prefix_len {
                    &key[prefix_len..]
                } else if prefix_len == 0 {
                    key
                } else {
                    objects.push(meta);
                    continue;
                };

                if let Some(idx) = rest.find(delimiter) {
                    let cp = format!(
                        "{}{}{}",
                        if prefix_str.is_empty() {
                            ""
                        } else {
                            prefix_str
                        },
                        if prefix_str.is_empty() { "" } else { "/" },
                        &rest[..=idx]
                    );
                    common_prefixes.insert(cp);
                } else {
                    objects.push(meta);
                }
            }

            Ok(ListResult {
                common_prefixes: common_prefixes
                    .into_iter()
                    .map(|p| ObjectPath::from(p.as_str()))
                    .collect(),
                objects,
            })
        } else {
            let store = prefix
                .and_then(|p| self.resolve_cold_store(p))
                .ok_or_else(|| object_store::Error::Generic {
                    store: "BisqueRoutingStore",
                    source: "no cold store configured".into(),
                })?;
            store.list_with_delimiter(prefix).await
        }
    }

    async fn copy(&self, _from: &ObjectPath, _to: &ObjectPath) -> OsResult<()> {
        Err(object_store::Error::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &ObjectPath, _to: &ObjectPath) -> OsResult<()> {
        Err(object_store::Error::NotImplemented)
    }
}

/// Ensure the catalog is fresh (static version for use in closures that can't capture `&self`).
async fn ensure_catalog_static(
    client: &reqwest::Client,
    cluster_url: &str,
    bucket: &str,
    catalog: &Arc<RwLock<CachedCatalog>>,
) {
    if !catalog.read().is_stale() {
        return;
    }
    let url = format!("{}/{}/_bisque/catalog", cluster_url, bucket);
    if let Ok(resp) = client.get(&url).send().await {
        if resp.status().is_success() {
            if let Ok(cat_resp) = resp.json::<CatalogResponse>().await {
                let mut cached = catalog.write();
                cached.tables = cat_resp.tables;
                cached.fetched_at = Instant::now();
            }
        }
    }
}

/// Check if a path is a cluster path (static version for use in closures).
fn is_cluster_path_static(path: &ObjectPath, catalog: &Arc<RwLock<CachedCatalog>>) -> bool {
    let path_str = path.as_ref();
    let mut parts = path_str.splitn(3, '/');
    let table_name = match parts.next() {
        Some(t) if !t.is_empty() => t,
        _ => return false,
    };
    match parts.next() {
        Some("active") | Some("sealed") => {}
        _ => return false,
    };
    catalog.read().tables.contains_key(table_name)
}

/// Percent-encode a string for use in URL query parameters.
fn url_encode(s: &str) -> String {
    let mut encoded = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                encoded.push(b as char);
            }
            _ => {
                encoded.push_str(&format!("%{:02X}", b));
            }
        }
    }
    encoded
}

/// Parse S3 ListObjectsV2 XML response into `ObjectMeta` items.
fn parse_list_xml(xml: &str) -> Vec<ObjectMeta> {
    let mut items = Vec::new();

    for contents in xml.split("<Contents>").skip(1) {
        let end = match contents.find("</Contents>") {
            Some(i) => i,
            None => continue,
        };
        let block = &contents[..end];

        let key = extract_xml_tag(block, "Key").unwrap_or_default();
        let size: u64 = extract_xml_tag(block, "Size")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let etag = extract_xml_tag(block, "ETag");
        let last_modified = extract_xml_tag(block, "LastModified")
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);

        if !key.is_empty() {
            items.push(ObjectMeta {
                location: ObjectPath::from(key.as_str()),
                last_modified,
                size,
                e_tag: etag,
                version: None,
            });
        }
    }

    items
}

/// Extract text content from a simple XML tag like `<Key>value</Key>`.
fn extract_xml_tag(block: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = block.find(&open)? + open.len();
    let end = block[start..].find(&close)? + start;
    Some(block[start..end].to_string())
}

/// Parse a Content-Range header value like "bytes 0-499/1000" into a Range<u64>.
fn parse_content_range(header: &str) -> Option<std::ops::Range<u64>> {
    let rest = header.strip_prefix("bytes ")?;
    let (range_part, _total) = rest.split_once('/')?;
    let (start_str, end_str) = range_part.split_once('-')?;
    let start: u64 = start_str.parse().ok()?;
    let end: u64 = end_str.parse().ok()?;
    Some(start..end + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // url_encode
    // -----------------------------------------------------------------------

    #[test]
    fn url_encode_alphanumerics_unchanged() {
        assert_eq!(url_encode("abcXYZ012"), "abcXYZ012");
        assert_eq!(
            url_encode("hello-world_v1.0~draft"),
            "hello-world_v1.0~draft"
        );
    }

    #[test]
    fn url_encode_special_characters() {
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(url_encode("a=b&c=d"), "a%3Db%26c%3Dd");
        assert_eq!(url_encode("100%"), "100%25");
    }

    #[test]
    fn url_encode_preserves_slashes() {
        assert_eq!(url_encode("table/active/data"), "table/active/data");
        assert_eq!(
            url_encode("prefix/with spaces/file"),
            "prefix/with%20spaces/file"
        );
    }

    // -----------------------------------------------------------------------
    // parse_list_xml
    // -----------------------------------------------------------------------

    #[test]
    fn parse_list_xml_with_contents() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents>
    <Key>table/active/data/file.lance</Key>
    <Size>2048</Size>
    <ETag>"abc123"</ETag>
    <LastModified>2025-06-15T12:00:00.000Z</LastModified>
  </Contents>
</ListBucketResult>"#;

        let items = parse_list_xml(xml);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].location.as_ref(), "table/active/data/file.lance");
        assert_eq!(items[0].size, 2048);
        assert_eq!(items[0].e_tag.as_deref(), Some("\"abc123\""));
    }

    #[test]
    fn parse_list_xml_empty() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>bucket</Name>
  <KeyCount>0</KeyCount>
</ListBucketResult>"#;

        let items = parse_list_xml(xml);
        assert!(items.is_empty());
    }

    #[test]
    fn parse_list_xml_multiple_objects() {
        let xml = r#"<ListBucketResult>
  <Contents>
    <Key>a/active/f1.lance</Key>
    <Size>100</Size>
    <LastModified>2025-01-01T00:00:00Z</LastModified>
  </Contents>
  <Contents>
    <Key>a/active/f2.lance</Key>
    <Size>200</Size>
    <LastModified>2025-01-02T00:00:00Z</LastModified>
  </Contents>
  <Contents>
    <Key>b/sealed/f3.lance</Key>
    <Size>300</Size>
    <LastModified>2025-01-03T00:00:00Z</LastModified>
  </Contents>
</ListBucketResult>"#;

        let items = parse_list_xml(xml);
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].location.as_ref(), "a/active/f1.lance");
        assert_eq!(items[0].size, 100);
        assert_eq!(items[1].location.as_ref(), "a/active/f2.lance");
        assert_eq!(items[1].size, 200);
        assert_eq!(items[2].location.as_ref(), "b/sealed/f3.lance");
        assert_eq!(items[2].size, 300);
    }

    // -----------------------------------------------------------------------
    // extract_xml_tag
    // -----------------------------------------------------------------------

    #[test]
    fn extract_xml_tag_existing() {
        let block = "<Key>my/path/file.lance</Key><Size>1024</Size>";
        assert_eq!(
            extract_xml_tag(block, "Key"),
            Some("my/path/file.lance".to_string())
        );
        assert_eq!(extract_xml_tag(block, "Size"), Some("1024".to_string()));
    }

    #[test]
    fn extract_xml_tag_missing_returns_none() {
        let block = "<Key>file.lance</Key>";
        assert_eq!(extract_xml_tag(block, "Size"), None);
        assert_eq!(extract_xml_tag(block, "ETag"), None);
    }

    // -----------------------------------------------------------------------
    // parse_content_range
    // -----------------------------------------------------------------------

    #[test]
    fn parse_content_range_valid() {
        let range = parse_content_range("bytes 0-499/1000").unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 500); // end is exclusive (499 + 1)

        let range2 = parse_content_range("bytes 500-999/1000").unwrap();
        assert_eq!(range2.start, 500);
        assert_eq!(range2.end, 1000);
    }

    #[test]
    fn parse_content_range_invalid_returns_none() {
        assert!(parse_content_range("invalid").is_none());
        assert!(parse_content_range("bytes abc-def/1000").is_none());
        assert!(parse_content_range("chars 0-499/1000").is_none());
        assert!(parse_content_range("bytes 0-499").is_none()); // missing /total
    }

    // -----------------------------------------------------------------------
    // CachedCatalog
    // -----------------------------------------------------------------------

    #[test]
    fn cached_catalog_empty_is_stale() {
        let catalog = CachedCatalog::empty();
        assert!(catalog.is_stale());
    }

    #[test]
    fn cached_catalog_fresh_then_stale() {
        let catalog = CachedCatalog {
            tables: HashMap::new(),
            fetched_at: Instant::now(),
            ttl: std::time::Duration::from_millis(50),
        };
        // Freshly created should not be stale
        assert!(!catalog.is_stale());

        // After waiting past TTL, should be stale
        std::thread::sleep(std::time::Duration::from_millis(60));
        assert!(catalog.is_stale());
    }

    // -----------------------------------------------------------------------
    // BisqueRoutingStore::is_cluster_path
    // -----------------------------------------------------------------------

    #[test]
    fn is_cluster_path_classification() {
        let creds = Arc::new(CredentialConfig::default());
        let store = BisqueRoutingStore::new("http://localhost:3300", "bucket", creds);

        // Populate catalog with a known table
        {
            let mut cached = store.catalog.write();
            cached.tables.insert(
                "events".to_string(),
                SegmentInfo {
                    active_segment: 1,
                    sealed_segment: Some(0),
                    s3_dataset_uri: "s3://bucket/events".to_string(),
                    s3_storage_options: HashMap::new(),
                },
            );
            cached.fetched_at = Instant::now();
        }

        // Known table with active tier -> cluster path
        assert!(store.is_cluster_path(&ObjectPath::from("events/active/data/file")));
        // Known table with sealed tier -> cluster path
        assert!(store.is_cluster_path(&ObjectPath::from("events/sealed/data/file")));
        // Unknown table -> not cluster path
        assert!(!store.is_cluster_path(&ObjectPath::from("unknown/active/data/file")));
        // Invalid tier -> not cluster path
        assert!(!store.is_cluster_path(&ObjectPath::from("events/cold/data/file")));
        // Empty path -> not cluster path
        assert!(!store.is_cluster_path(&ObjectPath::from("")));
    }
}

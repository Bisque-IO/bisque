//! Client-side credential configuration for S3/cold storage.
//!
//! Resolves per-table `ObjectStore` instances by merging server-provided
//! storage options (region, endpoint, etc.) with client-held credentials.
//! Tables sharing the same bucket and credentials reuse the same store.
//!
//! # Example
//!
//! ```no_run
//! use bisque_client::CredentialConfig;
//!
//! let config = CredentialConfig::new()
//!     .with_credential("aws_access_key_id", "AKIA...")
//!     .with_credential("aws_secret_access_key", "...");
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use parking_lot::RwLock;
use tracing::debug;

/// Client-side credentials for S3 storage, with per-table overrides.
///
/// ObjectStore instances are cached and shared by canonical bucket URI
/// plus credential fingerprint.
pub struct CredentialConfig {
    /// Default credentials applied to all tables.
    credentials: HashMap<String, String>,
    /// Per-table credential overrides (merged on top of defaults).
    table_overrides: HashMap<String, HashMap<String, String>>,
    /// Cache of opened stores keyed by canonical bucket URI + credentials.
    stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl CredentialConfig {
    /// Create a new empty config (no credentials).
    pub fn new() -> Self {
        Self {
            credentials: HashMap::new(),
            table_overrides: HashMap::new(),
            stores: RwLock::new(HashMap::new()),
        }
    }

    /// Set a credential (e.g. `aws_access_key_id`, `aws_secret_access_key`).
    pub fn with_credential(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.credentials.insert(key.into(), value.into());
        self
    }

    /// Set multiple credentials at once.
    pub fn with_credentials(mut self, creds: HashMap<String, String>) -> Self {
        self.credentials.extend(creds);
        self
    }

    /// Set per-table credential overrides.
    pub fn with_table_credentials(
        mut self,
        table: impl Into<String>,
        creds: HashMap<String, String>,
    ) -> Self {
        self.table_overrides.insert(table.into(), creds);
        self
    }

    /// Resolve an `ObjectStore` for the given table and S3 dataset URI.
    ///
    /// Merges options in order (later wins):
    /// 1. `server_options` — non-credential options from the server catalog
    /// 2. `credentials` — client-level credentials
    /// 3. `table_overrides` — per-table client credentials
    ///
    /// Returns the `ObjectStore` and the path prefix within the store.
    pub fn resolve(
        &self,
        table: &str,
        s3_uri: &str,
        server_options: &HashMap<String, String>,
    ) -> Result<(Arc<dyn ObjectStore>, String), Box<dyn std::error::Error + Send + Sync>> {
        let (bucket_key, path_prefix) = parse_bucket_and_path(s3_uri)?;

        // Merge: server options → client credentials → per-table overrides
        let mut options = server_options.clone();
        options.extend(self.credentials.iter().map(|(k, v)| (k.clone(), v.clone())));
        if let Some(overrides) = self.table_overrides.get(table) {
            options.extend(overrides.iter().map(|(k, v)| (k.clone(), v.clone())));
        }

        let cache_key = build_cache_key(&bucket_key, &options);

        // Check cache
        if let Some(store) = self.stores.read().get(&cache_key) {
            return Ok((store.clone(), path_prefix));
        }

        // Open a new store
        let store = open_s3_store(&bucket_key, &options)?;
        let store: Arc<dyn ObjectStore> = Arc::new(store);
        self.stores.write().insert(cache_key.clone(), store.clone());
        debug!(bucket = %bucket_key, table, "Opened new S3 ObjectStore");

        Ok((store, path_prefix))
    }
}

impl Default for CredentialConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse `s3://bucket/path/to/dataset` into (`s3://bucket`, `path/to/dataset`).
fn parse_bucket_and_path(
    uri: &str,
) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
    let (scheme, rest) = uri
        .split_once("://")
        .ok_or_else(|| format!("invalid S3 URI (no scheme): {}", uri))?;

    let (bucket, path) = match rest.split_once('/') {
        Some((b, p)) => (b, p.trim_end_matches('/').to_string()),
        None => (rest, String::new()),
    };

    let bucket_key = format!("{}://{}", scheme, bucket);
    Ok((bucket_key, path))
}

/// Build a cache key from bucket URI and options.
fn build_cache_key(bucket_key: &str, options: &HashMap<String, String>) -> String {
    let mut parts = vec![bucket_key.to_string()];
    let mut relevant: Vec<_> = options
        .iter()
        .filter(|(k, _)| {
            let k = k.to_lowercase();
            k.contains("key")
                || k.contains("secret")
                || k.contains("token")
                || k.contains("region")
                || k.contains("endpoint")
                || k.contains("profile")
        })
        .collect();
    relevant.sort_by_key(|(k, _)| (*k).clone());
    for (k, v) in relevant {
        parts.push(format!("{}={}", k, v));
    }
    parts.join("|")
}

/// Open an S3 ObjectStore from a bucket URI and options.
fn open_s3_store(
    bucket_key: &str,
    options: &HashMap<String, String>,
) -> Result<impl ObjectStore, Box<dyn std::error::Error + Send + Sync>> {
    let (_scheme, bucket) = bucket_key
        .split_once("://")
        .ok_or_else(|| format!("invalid bucket key: {}", bucket_key))?;

    let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

    for (key, value) in options {
        let k = key.to_lowercase();
        match k.as_str() {
            "aws_region" | "region" => {
                builder = builder.with_region(value);
            }
            "aws_access_key_id" | "access_key_id" => {
                builder = builder.with_access_key_id(value);
            }
            "aws_secret_access_key" | "secret_access_key" => {
                builder = builder.with_secret_access_key(value);
            }
            "aws_session_token" | "session_token" => {
                builder = builder.with_token(value);
            }
            "aws_endpoint" | "endpoint" | "endpoint_url" => {
                builder = builder.with_endpoint(value);
            }
            "aws_allow_http" | "allow_http" => {
                if value == "true" || value == "1" {
                    builder = builder.with_allow_http(true);
                }
            }
            "aws_virtual_hosted_style" | "virtual_hosted_style_request" => {
                if value == "true" || value == "1" {
                    builder = builder.with_virtual_hosted_style_request(true);
                }
            }
            _ => {}
        }
    }

    Ok(builder.build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bucket_and_path() {
        let (bucket, path) = parse_bucket_and_path("s3://my-bucket/data/events/deep").unwrap();
        assert_eq!(bucket, "s3://my-bucket");
        assert_eq!(path, "data/events/deep");

        let (bucket, path) = parse_bucket_and_path("s3://my-bucket").unwrap();
        assert_eq!(bucket, "s3://my-bucket");
        assert_eq!(path, "");

        let (bucket, path) = parse_bucket_and_path("s3://my-bucket/").unwrap();
        assert_eq!(bucket, "s3://my-bucket");
        assert_eq!(path, "");
    }

    #[test]
    fn test_cache_key_varies_by_credentials() {
        let mut opts1 = HashMap::new();
        opts1.insert("aws_access_key_id".into(), "AKIA1".into());

        let mut opts2 = HashMap::new();
        opts2.insert("aws_access_key_id".into(), "AKIA2".into());

        let key1 = build_cache_key("s3://bucket", &opts1);
        let key2 = build_cache_key("s3://bucket", &opts2);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_cache_key_same_for_same_credentials() {
        let mut opts1 = HashMap::new();
        opts1.insert("aws_access_key_id".into(), "AKIA1".into());
        opts1.insert("other_setting".into(), "foo".into());

        let mut opts2 = HashMap::new();
        opts2.insert("aws_access_key_id".into(), "AKIA1".into());
        opts2.insert("other_setting".into(), "bar".into());

        let key1 = build_cache_key("s3://bucket", &opts1);
        let key2 = build_cache_key("s3://bucket", &opts2);
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_config_builder() {
        let config = CredentialConfig::new()
            .with_credential("aws_region", "us-east-1")
            .with_table_credentials(
                "audit",
                HashMap::from([("aws_region".into(), "eu-west-1".into())]),
            );

        assert_eq!(config.credentials.get("aws_region").unwrap(), "us-east-1");
        assert_eq!(
            config
                .table_overrides
                .get("audit")
                .unwrap()
                .get("aws_region")
                .unwrap(),
            "eu-west-1"
        );
    }
}

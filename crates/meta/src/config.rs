/// Configuration for the bisque-meta control plane.
pub struct MetaConfig {
    /// HMAC secret key for token signing (32+ bytes recommended).
    pub token_secret: Vec<u8>,
    /// Token default TTL in seconds (default: 3600).
    pub token_ttl_secs: u64,
    /// Whether to auto-create the `_otel` catalog for new tenants.
    pub auto_create_otel_catalog: bool,
}

impl MetaConfig {
    pub fn new(token_secret: Vec<u8>) -> Self {
        Self {
            token_secret,
            token_ttl_secs: 3600,
            auto_create_otel_catalog: true,
        }
    }

    pub fn with_token_ttl_secs(mut self, secs: u64) -> Self {
        self.token_ttl_secs = secs;
        self
    }

    pub fn with_auto_create_otel_catalog(mut self, v: bool) -> Self {
        self.auto_create_otel_catalog = v;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let config = MetaConfig::new(b"secret".to_vec());
        assert_eq!(config.token_secret, b"secret");
        assert_eq!(config.token_ttl_secs, 3600);
        assert!(config.auto_create_otel_catalog);
    }

    #[test]
    fn test_builder_token_ttl() {
        let config = MetaConfig::new(b"s".to_vec()).with_token_ttl_secs(7200);
        assert_eq!(config.token_ttl_secs, 7200);
        assert!(config.auto_create_otel_catalog); // unchanged
    }

    #[test]
    fn test_builder_disable_auto_otel() {
        let config = MetaConfig::new(b"s".to_vec()).with_auto_create_otel_catalog(false);
        assert!(!config.auto_create_otel_catalog);
        assert_eq!(config.token_ttl_secs, 3600); // unchanged
    }

    #[test]
    fn test_builder_chaining() {
        let config = MetaConfig::new(b"s".to_vec())
            .with_token_ttl_secs(1800)
            .with_auto_create_otel_catalog(false);
        assert_eq!(config.token_ttl_secs, 1800);
        assert!(!config.auto_create_otel_catalog);
    }
}

/// Configuration for the bisque-meta control plane.
pub struct MetaConfig {
    /// HMAC secret key for token signing (32+ bytes recommended).
    pub token_secret: Vec<u8>,
    /// Token default TTL in seconds (default: 3600).
    pub token_ttl_secs: u64,
}

impl MetaConfig {
    pub fn new(token_secret: Vec<u8>) -> Self {
        Self {
            token_secret,
            token_ttl_secs: 3600,
        }
    }

    pub fn with_token_ttl_secs(mut self, secs: u64) -> Self {
        self.token_ttl_secs = secs;
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
    }

    #[test]
    fn test_builder_token_ttl() {
        let config = MetaConfig::new(b"s".to_vec()).with_token_ttl_secs(7200);
        assert_eq!(config.token_ttl_secs, 7200);
    }

    #[test]
    fn test_builder_chaining() {
        let config = MetaConfig::new(b"s".to_vec()).with_token_ttl_secs(1800);
        assert_eq!(config.token_ttl_secs, 1800);
    }
}

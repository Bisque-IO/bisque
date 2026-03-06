//! Simple HMAC-SHA256 token manager for tenant authentication.
//!
//! Tokens are `base64url(payload).base64url(signature)` strings where:
//! - `payload` = bincode-encoded `TokenClaims`
//! - `signature` = SHA256(secret || payload) (32 bytes)
//!
//! Nodes verify tokens locally using the shared signing secret.
//!
//! TODO: Replace with proper HMAC-SHA256 via `ring` or `aws-lc-rs` for production.

use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use argon2::password_hash::SaltString;
use argon2::password_hash::rand_core::OsRng;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

use crate::types::Scope;

/// Hash a password using Argon2id.
pub fn hash_password(password: &[u8]) -> Vec<u8> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(password, &salt)
        .expect("failed to hash password");
    hash.to_string().into_bytes()
}

/// Verify a password against an Argon2 hash.
pub fn verify_password(hash: &[u8], password: &[u8]) -> bool {
    let hash_str = match std::str::from_utf8(hash) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let parsed = match PasswordHash::new(hash_str) {
        Ok(h) => h,
        Err(_) => return false,
    };
    Argon2::default().verify_password(password, &parsed).is_ok()
}

/// Token payload that gets signed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenClaims {
    /// None for legacy API-key-only tokens.
    pub user_id: Option<u64>,
    /// For account-scoped operations.
    pub account_id: Option<u64>,
    /// Tenant context. 0 for super-admin tokens that span all tenants.
    pub tenant_id: u64,
    pub key_id: u64,
    pub scopes: Vec<Scope>,
    /// Seconds since epoch.
    pub issued_at: i64,
    /// Seconds since epoch.
    pub expires_at: i64,
}

/// Simple HMAC token manager.
pub struct TokenManager {
    secret: Vec<u8>,
}

impl TokenManager {
    pub fn new(secret: Vec<u8>) -> Self {
        Self { secret }
    }

    /// Issue a token for the given claims.
    ///
    /// Returns a `base64url(payload).base64url(signature)` string.
    pub fn issue(&self, claims: &TokenClaims) -> String {
        let payload =
            bincode::serde::encode_to_vec(claims, bincode::config::standard()).expect("encode");
        let sig = self.sign(&payload);
        let payload_b64 = URL_SAFE_NO_PAD.encode(&payload);
        let sig_b64 = URL_SAFE_NO_PAD.encode(sig);
        format!("{payload_b64}.{sig_b64}")
    }

    /// Verify a token string and return the decoded claims.
    ///
    /// Returns `None` if the signature is invalid or the token is expired.
    pub fn verify(&self, token: &str) -> Option<TokenClaims> {
        let (payload_b64, sig_b64) = token.split_once('.')?;
        let payload = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
        let sig = URL_SAFE_NO_PAD.decode(sig_b64).ok()?;

        // Verify signature
        let expected = self.sign(&payload);
        if !constant_time_eq(&sig, &expected) {
            return None;
        }

        // Decode claims
        let (claims, _): (TokenClaims, _) =
            bincode::serde::decode_from_slice(&payload, bincode::config::standard()).ok()?;

        // Check expiry
        let now = chrono::Utc::now().timestamp();
        if now > claims.expires_at {
            return None;
        }

        Some(claims)
    }

    /// SHA256(secret || data) — simplified signing for v1.
    fn sign(&self, data: &[u8]) -> [u8; 32] {
        // Simple Merkle-Damgard style: H(key || message)
        // This is NOT a proper HMAC (vulnerable to length extension), but
        // sufficient for the v1 scaffold. Use ring::hmac or aws-lc-rs for production.
        simple_sha256(&[&self.secret, data])
    }
}

/// Deterministic key derivation: SHA256(secret || key_id_bytes || tenant_id_bytes).
///
/// Used by `MetaEngine::create_api_key` so all Raft replicas produce the
/// same key value for a given (key_id, tenant_id) pair.
pub fn derive_key(secret: &[u8], key_id_bytes: &[u8], tenant_id_bytes: &[u8]) -> [u8; 32] {
    simple_sha256(&[secret, key_id_bytes, tenant_id_bytes])
}

/// Minimal SHA-256 implementation (pure Rust, no external deps).
///
/// Uses the standard FIPS 180-4 algorithm. Only used for token signing —
/// replace with `ring` or `aws-lc-rs` for production workloads.
fn simple_sha256(chunks: &[&[u8]]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    // Collect all input bytes + padding
    let mut data = Vec::new();
    for chunk in chunks {
        data.extend_from_slice(chunk);
    }
    let bit_len = (data.len() as u64) * 8;

    // Padding: append 1 bit, then zeros, then 64-bit big-endian length
    data.push(0x80);
    while (data.len() % 64) != 56 {
        data.push(0);
    }
    data.extend_from_slice(&bit_len.to_be_bytes());

    // Process each 512-bit (64-byte) block
    for block in data.chunks_exact(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                block[i * 4],
                block[i * 4 + 1],
                block[i * 4 + 2],
                block[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut result = [0u8; 32];
    for (i, &val) in h.iter().enumerate() {
        result[i * 4..i * 4 + 4].copy_from_slice(&val.to_be_bytes());
    }
    result
}

/// Constant-time comparison to prevent timing attacks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_issue_and_verify() {
        let mgr = TokenManager::new(b"test-secret-key-32-bytes-long!!!".to_vec());
        let claims = TokenClaims {
            user_id: None,
            account_id: None,
            tenant_id: 1,
            key_id: 42,
            scopes: vec![Scope::TenantAdmin],
            issued_at: chrono::Utc::now().timestamp(),
            expires_at: chrono::Utc::now().timestamp() + 3600,
        };
        let token = mgr.issue(&claims);
        let verified = mgr.verify(&token).expect("should verify");
        assert_eq!(verified.tenant_id, 1);
        assert_eq!(verified.key_id, 42);
        assert_eq!(verified.scopes, vec![Scope::TenantAdmin]);
    }

    #[test]
    fn test_verify_tampered_token_fails() {
        let mgr = TokenManager::new(b"test-secret".to_vec());
        let claims = TokenClaims {
            user_id: None,
            account_id: None,
            tenant_id: 1,
            key_id: 1,
            scopes: vec![],
            issued_at: 0,
            expires_at: i64::MAX,
        };
        let mut token = mgr.issue(&claims);
        // Tamper with a character in the payload
        unsafe {
            let bytes = token.as_bytes_mut();
            if bytes[0] == b'A' {
                bytes[0] = b'B';
            } else {
                bytes[0] = b'A';
            }
        }
        assert!(mgr.verify(&token).is_none());
    }

    #[test]
    fn test_verify_expired_token_fails() {
        let mgr = TokenManager::new(b"test-secret".to_vec());
        let claims = TokenClaims {
            user_id: None,
            account_id: None,
            tenant_id: 1,
            key_id: 1,
            scopes: vec![],
            issued_at: 0,
            expires_at: 0, // already expired
        };
        let token = mgr.issue(&claims);
        assert!(mgr.verify(&token).is_none());
    }

    #[test]
    fn test_verify_wrong_secret_fails() {
        let mgr1 = TokenManager::new(b"secret-one".to_vec());
        let mgr2 = TokenManager::new(b"secret-two".to_vec());
        let claims = TokenClaims {
            user_id: None,
            account_id: None,
            tenant_id: 1,
            key_id: 1,
            scopes: vec![],
            issued_at: 0,
            expires_at: i64::MAX,
        };
        let token = mgr1.issue(&claims);
        assert!(mgr2.verify(&token).is_none());
    }

    #[test]
    fn test_sha256_known_vector() {
        // SHA256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        let hash = simple_sha256(&[b""]);
        let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_sha256_known_vector_abc() {
        // SHA256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        let hash = simple_sha256(&[b"abc"]);
        let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn test_sha256_multi_chunk_matches_single() {
        // SHA256("abc") should be the same whether passed as one chunk or multiple
        let single = simple_sha256(&[b"abc"]);
        let multi = simple_sha256(&[b"a", b"b", b"c"]);
        assert_eq!(single, multi);
    }

    #[test]
    fn test_issue_and_verify_multiple_scopes() {
        let mgr = TokenManager::new(b"secret".to_vec());
        let claims = TokenClaims {
            user_id: None,
            account_id: None,
            tenant_id: 1,
            key_id: 1,
            scopes: vec![
                Scope::TenantAdmin,
                Scope::Catalog("analytics".into()),
                Scope::CatalogRead("events".into()),
            ],
            issued_at: chrono::Utc::now().timestamp(),
            expires_at: chrono::Utc::now().timestamp() + 3600,
        };
        let token = mgr.issue(&claims);
        let verified = mgr.verify(&token).unwrap();
        assert_eq!(verified.scopes.len(), 3);
        assert_eq!(verified.scopes[0], Scope::TenantAdmin);
        assert_eq!(verified.scopes[1], Scope::Catalog("analytics".into()));
        assert_eq!(verified.scopes[2], Scope::CatalogRead("events".into()));
    }

    #[test]
    fn test_issue_and_verify_empty_scopes() {
        let mgr = TokenManager::new(b"secret".to_vec());
        let claims = TokenClaims {
            user_id: None,
            account_id: None,
            tenant_id: 42,
            key_id: 7,
            scopes: vec![],
            issued_at: 0,
            expires_at: i64::MAX,
        };
        let token = mgr.issue(&claims);
        let verified = mgr.verify(&token).unwrap();
        assert_eq!(verified.tenant_id, 42);
        assert_eq!(verified.key_id, 7);
        assert!(verified.scopes.is_empty());
    }

    #[test]
    fn test_verify_malformed_token() {
        let mgr = TokenManager::new(b"secret".to_vec());

        // No dot separator
        assert!(mgr.verify("nodot").is_none());

        // Empty parts
        assert!(mgr.verify(".").is_none());

        // Invalid base64
        assert!(mgr.verify("!!!.!!!").is_none());

        // Valid base64 but garbage payload
        assert!(mgr.verify("AAAA.BBBB").is_none());
    }

    #[test]
    fn test_derive_key_deterministic() {
        let secret = b"my-secret";
        let kid = 1u64.to_le_bytes();
        let tid = 2u64.to_le_bytes();

        let k1 = derive_key(secret, &kid, &tid);
        let k2 = derive_key(secret, &kid, &tid);
        assert_eq!(k1, k2);
    }

    #[test]
    fn test_derive_key_different_inputs() {
        let secret = b"my-secret";

        let k1 = derive_key(secret, &1u64.to_le_bytes(), &1u64.to_le_bytes());
        let k2 = derive_key(secret, &1u64.to_le_bytes(), &2u64.to_le_bytes());
        let k3 = derive_key(secret, &2u64.to_le_bytes(), &1u64.to_le_bytes());
        let k4 = derive_key(b"other-secret", &1u64.to_le_bytes(), &1u64.to_le_bytes());

        assert_ne!(k1, k2);
        assert_ne!(k1, k3);
        assert_ne!(k2, k3);
        assert_ne!(k1, k4);
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn test_token_preserves_timestamps() {
        let mgr = TokenManager::new(b"secret".to_vec());
        let issued = chrono::Utc::now().timestamp();
        let expires = issued + 3600;
        let claims = TokenClaims {
            user_id: None,
            account_id: None,
            tenant_id: 1,
            key_id: 1,
            scopes: vec![],
            issued_at: issued,
            expires_at: expires,
        };
        let token = mgr.issue(&claims);
        let verified = mgr.verify(&token).unwrap();
        assert_eq!(verified.issued_at, issued);
        assert_eq!(verified.expires_at, expires);
    }

    #[test]
    fn test_different_tokens_for_different_claims() {
        let mgr = TokenManager::new(b"secret".to_vec());
        let c1 = TokenClaims {
            user_id: None,
            account_id: None,
            tenant_id: 1,
            key_id: 1,
            scopes: vec![],
            issued_at: 0,
            expires_at: i64::MAX,
        };
        let c2 = TokenClaims {
            user_id: None,
            account_id: None,
            tenant_id: 2,
            key_id: 1,
            scopes: vec![],
            issued_at: 0,
            expires_at: i64::MAX,
        };
        assert_ne!(mgr.issue(&c1), mgr.issue(&c2));
    }
}

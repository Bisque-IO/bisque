//! MQTT 5.0 Enhanced Authentication (AUTH packet support).
//!
//! Provides a pluggable authentication provider trait and a state machine
//! for multi-step SASL-like authentication flows.
//!
//! MQTT 5.0 SS 3.15 / SS 4.12: Enhanced authentication enables challenge-response
//! auth (e.g., SCRAM-SHA-256) via the AUTH packet. The flow is:
//!   1. Client sends CONNECT with `authentication_method` + optional `authentication_data`
//!   2. Server may send AUTH (reason 0x18 = Continue Authentication) with challenge data
//!   3. Client responds with AUTH (reason 0x18) with response data
//!   4. Steps 2-3 repeat until auth completes
//!   5. Server sends CONNACK (success) or CONNACK with error reason code

use std::collections::HashMap;

use bytes::Bytes;

// =============================================================================
// Auth Flow Types
// =============================================================================

/// Result of an authentication step.
#[derive(Debug)]
pub enum AuthResult {
    /// Authentication succeeded. The connection is accepted.
    Success,
    /// Authentication requires another round-trip. Send this challenge to the client.
    Continue {
        /// Challenge data to send in the AUTH packet.
        authentication_data: Bytes,
    },
    /// Authentication failed. Reject the connection with this reason code.
    Failed {
        /// MQTT 5.0 reason code (e.g., 0x86 = Bad User Name or Password, 0x87 = Not Authorized).
        reason_code: u8,
        /// Optional reason string.
        reason_string: Option<String>,
    },
}

/// State of an in-progress enhanced authentication.
#[derive(Debug)]
pub enum AuthState {
    /// No enhanced auth is in progress.
    None,
    /// Waiting for client AUTH response (challenge-response in progress).
    WaitingForResponse { method: String, step: u32 },
    /// Authentication complete (success or failure already sent).
    Complete,
}

impl Default for AuthState {
    fn default() -> Self {
        Self::None
    }
}

// =============================================================================
// Auth Provider Trait
// =============================================================================

/// Pluggable authentication provider for MQTT 5.0 Enhanced Authentication.
///
/// Implementations provide the server-side logic for challenge-response
/// authentication flows (SCRAM, Kerberos, etc.) as well as simple
/// username/password validation.
pub trait AuthProvider: Send + Sync {
    /// Check if this provider supports the given authentication method.
    fn supports_method(&self, method: &str) -> bool;

    /// Begin authentication from a CONNECT packet.
    ///
    /// Called when the client sends CONNECT with `authentication_method`.
    /// Returns `AuthResult::Success` for immediate acceptance,
    /// `AuthResult::Continue` to send a challenge, or `AuthResult::Failed`.
    fn authenticate_connect(
        &self,
        method: &str,
        client_id: &str,
        username: Option<&str>,
        password: Option<&[u8]>,
        auth_data: Option<&[u8]>,
    ) -> AuthResult;

    /// Continue authentication from a client AUTH packet.
    ///
    /// Called when the client responds to a challenge with an AUTH packet.
    fn authenticate_continue(
        &self,
        method: &str,
        client_id: &str,
        auth_data: Option<&[u8]>,
        step: u32,
    ) -> AuthResult;

    /// GAP-9: Authorize a PUBLISH or SUBSCRIBE operation on a specific topic.
    ///
    /// Called before accepting a client PUBLISH or processing a SUBSCRIBE filter.
    /// Returns `true` if the client is allowed to perform the operation.
    ///
    /// Default implementation allows all operations.
    fn authorize_topic(
        &self,
        _client_id: &str,
        _username: Option<&str>,
        _topic: &str,
        _action: TopicAction,
    ) -> bool {
        true
    }
}

/// The type of topic operation being authorized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicAction {
    /// Client is publishing to this topic.
    Publish,
    /// Client is subscribing to this topic filter.
    Subscribe,
}

// =============================================================================
// Simple Username/Password Auth Provider
// =============================================================================

/// A simple authentication provider that validates username/password pairs.
///
/// This provider does not use enhanced auth (AUTH packets). It validates
/// credentials from the CONNECT packet directly.
pub struct SimpleAuthProvider {
    /// Allowed username/password pairs. O(1) lookup via HashMap.
    credentials: HashMap<String, Vec<u8>>,
    /// Whether to allow anonymous connections (no username).
    allow_anonymous: bool,
}

impl SimpleAuthProvider {
    /// Create a new provider with the given credentials.
    pub fn new(credentials: Vec<(String, Vec<u8>)>, allow_anonymous: bool) -> Self {
        Self {
            credentials: credentials.into_iter().collect(),
            allow_anonymous,
        }
    }
}

impl AuthProvider for SimpleAuthProvider {
    fn supports_method(&self, _method: &str) -> bool {
        false // Simple auth doesn't use enhanced auth methods.
    }

    fn authenticate_connect(
        &self,
        _method: &str,
        _client_id: &str,
        username: Option<&str>,
        password: Option<&[u8]>,
        _auth_data: Option<&[u8]>,
    ) -> AuthResult {
        match username {
            Some(user) => {
                let pw = password.unwrap_or(&[]);
                // O(1) HashMap lookup instead of O(n) linear scan.
                if let Some(stored_pw) = self.credentials.get(user) {
                    if stored_pw.as_slice() == pw {
                        return AuthResult::Success;
                    }
                }
                AuthResult::Failed {
                    reason_code: 0x86, // Bad User Name or Password
                    reason_string: None,
                }
            }
            None => {
                if self.allow_anonymous {
                    AuthResult::Success
                } else {
                    AuthResult::Failed {
                        reason_code: 0x87, // Not Authorized
                        reason_string: Some("anonymous connections not allowed".to_string()),
                    }
                }
            }
        }
    }

    fn authenticate_continue(
        &self,
        _method: &str,
        _client_id: &str,
        _auth_data: Option<&[u8]>,
        _step: u32,
    ) -> AuthResult {
        AuthResult::Failed {
            reason_code: 0x8C, // Bad authentication method
            reason_string: Some("simple auth does not support enhanced authentication".to_string()),
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_auth_valid_credentials() {
        let provider =
            SimpleAuthProvider::new(vec![("admin".to_string(), b"secret".to_vec())], false);
        match provider.authenticate_connect("", "", Some("admin"), Some(b"secret"), None) {
            AuthResult::Success => {}
            other => panic!("expected Success, got {:?}", other),
        }
    }

    #[test]
    fn test_simple_auth_invalid_credentials() {
        let provider =
            SimpleAuthProvider::new(vec![("admin".to_string(), b"secret".to_vec())], false);
        match provider.authenticate_connect("", "", Some("admin"), Some(b"wrong"), None) {
            AuthResult::Failed { reason_code, .. } => assert_eq!(reason_code, 0x86),
            other => panic!("expected Failed, got {:?}", other),
        }
    }

    #[test]
    fn test_simple_auth_anonymous_allowed() {
        let provider = SimpleAuthProvider::new(vec![], true);
        match provider.authenticate_connect("", "", None, None, None) {
            AuthResult::Success => {}
            other => panic!("expected Success, got {:?}", other),
        }
    }

    #[test]
    fn test_simple_auth_anonymous_rejected() {
        let provider = SimpleAuthProvider::new(vec![], false);
        match provider.authenticate_connect("", "", None, None, None) {
            AuthResult::Failed { reason_code, .. } => assert_eq!(reason_code, 0x87),
            other => panic!("expected Failed, got {:?}", other),
        }
    }

    #[test]
    fn test_simple_auth_no_enhanced_auth() {
        let provider = SimpleAuthProvider::new(vec![], true);
        assert!(!provider.supports_method("SCRAM-SHA-256"));
        match provider.authenticate_continue("SCRAM-SHA-256", "client", None, 0) {
            AuthResult::Failed { reason_code, .. } => assert_eq!(reason_code, 0x8C),
            other => panic!("expected Failed, got {:?}", other),
        }
    }

    #[test]
    fn test_auth_state_default() {
        let state = AuthState::default();
        assert!(matches!(state, AuthState::None));
    }

    // ---- Additional coverage tests ----

    #[test]
    fn test_auth_result_variants() {
        // Test Debug formatting for all variants.
        let success = AuthResult::Success;
        assert!(format!("{:?}", success).contains("Success"));

        let cont = AuthResult::Continue {
            authentication_data: bytes::Bytes::from_static(b"challenge"),
        };
        assert!(format!("{:?}", cont).contains("Continue"));

        let failed = AuthResult::Failed {
            reason_code: 0x87,
            reason_string: Some("not authorized".to_string()),
        };
        assert!(format!("{:?}", failed).contains("Failed"));
    }

    #[test]
    fn test_auth_state_variants() {
        let waiting = AuthState::WaitingForResponse {
            method: "SCRAM-SHA-256".to_string(),
            step: 1,
        };
        assert!(format!("{:?}", waiting).contains("WaitingForResponse"));
        assert!(format!("{:?}", waiting).contains("SCRAM-SHA-256"));

        let complete = AuthState::Complete;
        assert!(format!("{:?}", complete).contains("Complete"));
    }

    #[test]
    fn test_simple_auth_multiple_credentials() {
        let provider = SimpleAuthProvider::new(
            vec![
                ("alice".to_string(), b"pass1".to_vec()),
                ("bob".to_string(), b"pass2".to_vec()),
                ("charlie".to_string(), b"pass3".to_vec()),
            ],
            false,
        );

        // All should authenticate successfully.
        assert!(matches!(
            provider.authenticate_connect("", "", Some("alice"), Some(b"pass1"), None),
            AuthResult::Success
        ));
        assert!(matches!(
            provider.authenticate_connect("", "", Some("bob"), Some(b"pass2"), None),
            AuthResult::Success
        ));
        assert!(matches!(
            provider.authenticate_connect("", "", Some("charlie"), Some(b"pass3"), None),
            AuthResult::Success
        ));

        // Wrong password for alice.
        assert!(matches!(
            provider.authenticate_connect("", "", Some("alice"), Some(b"wrong"), None),
            AuthResult::Failed {
                reason_code: 0x86,
                ..
            }
        ));

        // Unknown user.
        assert!(matches!(
            provider.authenticate_connect("", "", Some("unknown"), Some(b"pass"), None),
            AuthResult::Failed {
                reason_code: 0x86,
                ..
            }
        ));
    }

    #[test]
    fn test_simple_auth_empty_password() {
        let provider = SimpleAuthProvider::new(vec![("user".to_string(), b"".to_vec())], false);

        // Empty password should match.
        assert!(matches!(
            provider.authenticate_connect("", "", Some("user"), Some(b""), None),
            AuthResult::Success
        ));

        // No password (None) should also match empty.
        assert!(matches!(
            provider.authenticate_connect("", "", Some("user"), None, None),
            AuthResult::Success
        ));
    }

    #[test]
    fn test_simple_auth_username_with_no_password_fails() {
        let provider =
            SimpleAuthProvider::new(vec![("user".to_string(), b"secret".to_vec())], false);

        // Username provided but no password — should fail (empty != "secret").
        assert!(matches!(
            provider.authenticate_connect("", "", Some("user"), None, None),
            AuthResult::Failed {
                reason_code: 0x86,
                ..
            }
        ));
    }
}

//! SASL authentication for the Kafka protocol adapter.
//!
//! Supports PLAIN mechanism with a pluggable authenticator trait.

/// Trait for authenticating Kafka clients via SASL/PLAIN.
///
/// Implementations can check credentials against a database, LDAP, etc.
pub trait KafkaAuthenticator: Send + Sync + 'static {
    /// Authenticate a user. Returns `Ok(())` on success, `Err(message)` on failure.
    /// Uses `&'static str` for error messages to avoid heap allocation on the error path.
    fn authenticate(&self, username: &str, password: &str) -> Result<(), &'static str>;
}

/// Default authenticator that accepts all credentials.
pub struct AllowAllAuthenticator;

impl KafkaAuthenticator for AllowAllAuthenticator {
    fn authenticate(&self, _username: &str, _password: &str) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Parse SASL/PLAIN auth bytes: `\0username\0password`
pub fn parse_sasl_plain(data: &[u8]) -> Option<(&str, &str)> {
    // Format: [authzid]\0username\0password
    // authzid is typically empty, so we get: \0username\0password
    let mut parts = data.splitn(3, |&b| b == 0);
    let _authzid = parts.next()?; // skip authzid
    let username = parts.next()?;
    let password = parts.next()?;
    let username = std::str::from_utf8(username).ok()?;
    let password = std::str::from_utf8(password).ok()?;
    Some((username, password))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sasl_plain() {
        let data = b"\0user\0pass";
        let (u, p) = parse_sasl_plain(data).unwrap();
        assert_eq!(u, "user");
        assert_eq!(p, "pass");
    }

    #[test]
    fn test_parse_sasl_plain_with_authzid() {
        let data = b"admin\0user\0pass";
        let (u, p) = parse_sasl_plain(data).unwrap();
        assert_eq!(u, "user");
        assert_eq!(p, "pass");
    }

    #[test]
    fn test_parse_sasl_plain_empty() {
        assert!(parse_sasl_plain(b"").is_none());
    }

    #[test]
    fn test_allow_all_authenticator() {
        let auth = AllowAllAuthenticator;
        assert!(auth.authenticate("any", "thing").is_ok());
    }

    #[test]
    fn test_parse_sasl_plain_no_password() {
        // Only authzid + username, no password
        let data = b"\0user";
        assert!(parse_sasl_plain(data).is_none());
    }

    #[test]
    fn test_parse_sasl_plain_single_null() {
        assert!(parse_sasl_plain(b"\0").is_none());
    }

    #[test]
    fn test_parse_sasl_plain_empty_username_password() {
        // \0\0 means authzid="", username="", password="" — but username has no password following
        // Actually: splitn(3, |b| b==0) on b"\0\0" -> ["", "", ""]
        let data = b"\0\0";
        let result = parse_sasl_plain(data);
        assert!(result.is_some());
        let (u, p) = result.unwrap();
        assert_eq!(u, "");
        assert_eq!(p, "");
    }

    #[test]
    fn test_parse_sasl_plain_empty_password() {
        let data = b"\0user\0";
        let result = parse_sasl_plain(data);
        assert!(result.is_some());
        let (u, p) = result.unwrap();
        assert_eq!(u, "user");
        assert_eq!(p, "");
    }

    #[test]
    fn test_parse_sasl_plain_extra_nulls() {
        // Extra null bytes in the password portion should be part of the password
        let data = b"\0user\0pass\0extra";
        let result = parse_sasl_plain(data);
        assert!(result.is_some());
        let (u, p) = result.unwrap();
        assert_eq!(u, "user");
        assert_eq!(p, "pass\0extra");
    }

    #[test]
    fn test_parse_sasl_plain_invalid_utf8() {
        let data: &[u8] = &[0, 0xFF, 0xFE, 0, b'p', b'a', b's', b's'];
        let result = parse_sasl_plain(data);
        // Username bytes are invalid UTF-8, should return None
        assert!(result.is_none());
    }

    #[test]
    fn test_custom_authenticator() {
        struct DenyAll;
        impl KafkaAuthenticator for DenyAll {
            fn authenticate(&self, _: &str, _: &str) -> Result<(), &'static str> {
                Err("access denied")
            }
        }
        let auth = DenyAll;
        let result = auth.authenticate("user", "pass");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "access denied");
    }

    #[test]
    fn test_selective_authenticator() {
        struct SelectiveAuth;
        impl KafkaAuthenticator for SelectiveAuth {
            fn authenticate(&self, username: &str, password: &str) -> Result<(), &'static str> {
                if username == "admin" && password == "secret" {
                    Ok(())
                } else {
                    Err("bad credentials")
                }
            }
        }
        let auth = SelectiveAuth;
        assert!(auth.authenticate("admin", "secret").is_ok());
        assert!(auth.authenticate("admin", "wrong").is_err());
        assert!(auth.authenticate("user", "secret").is_err());
    }
}

use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("tenant not found: {0}")]
    TenantNotFound(u64),

    #[error("tenant already exists: {0}")]
    TenantAlreadyExists(String),

    #[error("catalog not found: {0}")]
    CatalogNotFound(u64),

    #[error("catalog already exists: {0} for tenant {1}")]
    CatalogAlreadyExists(String, u64),

    #[error("API key not found: {0}")]
    ApiKeyNotFound(u64),

    #[error("limit exceeded: {0}")]
    LimitExceeded(String),

    #[error("invalid state: {0}")]
    InvalidState(String),

    #[error("token error: {0}")]
    Token(String),
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        io::Error::new(io::ErrorKind::Other, e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        assert_eq!(
            Error::TenantNotFound(42).to_string(),
            "tenant not found: 42"
        );
        assert_eq!(
            Error::TenantAlreadyExists("acme".into()).to_string(),
            "tenant already exists: acme"
        );
        assert_eq!(
            Error::CatalogNotFound(7).to_string(),
            "catalog not found: 7"
        );
        assert_eq!(
            Error::CatalogAlreadyExists("analytics".into(), 1).to_string(),
            "catalog already exists: analytics for tenant 1"
        );
        assert_eq!(
            Error::ApiKeyNotFound(99).to_string(),
            "API key not found: 99"
        );
        assert_eq!(
            Error::LimitExceeded("too many".into()).to_string(),
            "limit exceeded: too many"
        );
        assert_eq!(
            Error::InvalidState("bad".into()).to_string(),
            "invalid state: bad"
        );
        assert_eq!(
            Error::Token("expired".into()).to_string(),
            "token error: expired"
        );
    }

    #[test]
    fn test_error_from_io() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = Error::from(io_err);
        assert!(matches!(err, Error::Io(_)));
        assert!(err.to_string().contains("file not found"));
    }

    #[test]
    fn test_error_into_io() {
        let err = Error::TenantNotFound(5);
        let io_err: io::Error = err.into();
        assert_eq!(io_err.kind(), io::ErrorKind::Other);
        assert!(io_err.to_string().contains("tenant not found: 5"));
    }
}

use std::io;

/// Errors returned by bisque-lance operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Lance error: {0}")]
    Lance(#[from] lance::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Segment not found: {0}")]
    SegmentNotFound(u64),

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("Codec error: {0}")]
    Codec(String),

    #[error("S3 not configured")]
    S3NotConfigured,

    #[error("Flush already in progress for segment {0}")]
    FlushInProgress(u64),

    #[error("No sealed segment to flush")]
    NoSealedSegment,

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("Segment sync error: {0}")]
    SegmentSync(String),

    #[error("Delete failed: {0}")]
    DeleteFailed(String),
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        io::Error::new(io::ErrorKind::Other, e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

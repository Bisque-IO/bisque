use std::io;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("unknown frame tag: 0x{0:02x}")]
    UnknownTag(u8),
    #[error("frame too large: {size} bytes (max {max})")]
    FrameTooLarge { size: u32, max: u32 },
    #[error("truncated frame: need {need} bytes, have {have}")]
    Truncated { need: usize, have: usize },
    #[error("invalid UTF-8")]
    InvalidUtf8,
    #[error("connection closed")]
    ConnectionClosed,
}

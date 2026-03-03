//! Test support utilities for multi-raft tests
//!
//! Provides helpers to run async tests inside Tokio runtime and common fixtures.

use crate::multi::codec::{BorrowPayload, CodecError, Decode, Encode};
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use tempfile::TempDir;

/// Byte wrapper for tests that implements all required traits.
/// Backed by `bytes::Bytes` to enable zero-copy decoding from mmap storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestBytes(pub Bytes);

impl TestBytes {
    /// Create from a Vec<u8> (convenience for tests).
    pub fn from_vec(v: Vec<u8>) -> Self {
        Self(Bytes::from(v))
    }
}

impl From<Vec<u8>> for TestBytes {
    fn from(v: Vec<u8>) -> Self {
        Self(Bytes::from(v))
    }
}

impl std::fmt::Display for TestBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestBytes(len={})", self.0.len())
    }
}

impl Encode for TestBytes {
    fn encode<W: std::io::Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        (self.0.len() as u32).encode(writer)?;
        writer.write_all(&self.0)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        4 + self.0.len()
    }
}

impl Decode for TestBytes {
    fn decode<R: std::io::Read>(reader: &mut R) -> Result<Self, CodecError> {
        let len = u32::decode(reader)? as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        Ok(Self(Bytes::from(buf)))
    }

    /// Zero-copy decode from a `Bytes` buffer (e.g. mmap-backed).
    /// Reads the 4-byte length prefix and slices the data without copying.
    fn decode_from_bytes(data: Bytes) -> Result<Self, CodecError> {
        if data.len() < 4 {
            return Err(CodecError::BufferTooSmall {
                needed: 4,
                have: data.len(),
            });
        }
        let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
        if data.len() < 4 + len {
            return Err(CodecError::BufferTooSmall {
                needed: 4 + len,
                have: data.len(),
            });
        }
        Ok(Self(data.slice(4..4 + len)))
    }
}

impl BorrowPayload for TestBytes {
    fn payload_bytes(&self) -> &[u8] {
        &self.0
    }
}

// Manual serde impls — openraft's `serde` feature requires `C::D: Serialize + Deserialize`.
// We serialize as raw bytes rather than enabling the `serde` feature on the `bytes` crate.
impl serde::Serialize for TestBytes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for TestBytes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let v: Vec<u8> = serde::Deserialize::deserialize(deserializer)?;
        Ok(Self(Bytes::from(v)))
    }
}

/// Type config alias for tests using TestBytes
pub type TestConfig = crate::BisqueRaftTypeConfig<TestBytes, ()>;

/// Run an async future inside a Tokio runtime
///
/// This creates a Tokio runtime and runs the future within it.
pub fn run_async<F>(f: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");

    rt.block_on(f)
}

/// Port allocator for tests to avoid conflicts
pub struct PortAllocator {
    next: AtomicU16,
}

impl PortAllocator {
    pub fn new(start: u16) -> Self {
        Self {
            next: AtomicU16::new(start),
        }
    }

    pub fn next(&self) -> u16 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for PortAllocator {
    fn default() -> Self {
        Self::new(50000)
    }
}

/// Temporary directory helper for storage tests
pub struct TestTempDir {
    inner: TempDir,
}

impl TestTempDir {
    pub fn new() -> Self {
        Self {
            inner: tempfile::tempdir().expect("Failed to create temp directory"),
        }
    }

    pub fn path(&self) -> PathBuf {
        self.inner.path().to_path_buf()
    }
}

impl Default for TestTempDir {
    fn default() -> Self {
        Self::new()
    }
}

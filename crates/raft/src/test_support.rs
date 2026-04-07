//! Test support utilities for multi-raft tests
//!
//! Provides helpers to run async tests inside Tokio runtime and common fixtures.

use crate::codec::{BorrowPayload, CodecError, Decode, Encode};
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};

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

    /// Zero-copy decode from a `Bytes` buffer (e.g. mmap-backed storage).
    /// The storage writes `BorrowPayload::payload_bytes()` (raw bytes, no prefix)
    /// so this path receives raw data — no length prefix to strip.
    fn decode_from_bytes(data: Bytes) -> Result<Self, CodecError> {
        Ok(Self(data))
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

/// Concrete entry type for tests (matches TestConfig's Entry associated type)
pub type TestEntry = openraft::impls::Entry<
    crate::type_config::BisqueCommittedLeaderId,
    TestBytes,
    crate::type_config::BisqueNodeId,
    openraft::impls::BasicNode,
>;

/// Concrete entry-payload type for tests
pub type TestEntryPayload =
    openraft::EntryPayload<TestBytes, crate::type_config::BisqueNodeId, openraft::impls::BasicNode>;

/// Concrete LogId for tests
pub type TestLogId = openraft::LogId<crate::type_config::BisqueCommittedLeaderId>;

/// Concrete Vote for tests
pub type TestVote = openraft::impls::Vote<crate::type_config::BisqueLeaderId>;

/// Concrete StoredMembership for tests
pub type TestStoredMembership = openraft::StoredMembership<
    crate::type_config::BisqueCommittedLeaderId,
    crate::type_config::BisqueNodeId,
    openraft::impls::BasicNode,
>;

/// Concrete SnapshotMeta for tests
pub type TestSnapshotMeta = openraft::storage::SnapshotMeta<
    crate::type_config::BisqueCommittedLeaderId,
    crate::type_config::BisqueNodeId,
    openraft::impls::BasicNode,
>;

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

/// Temporary directory helper for storage tests.
///
/// Uses a local `.tmp` directory in the project root instead of OS `/tmp`
/// to avoid disk quota issues.  Each instance creates a UUID subdirectory
/// that is removed on drop.
pub struct TestTempDir {
    path: PathBuf,
}

/// Base directory for all test temp dirs (relative to the crate root).
const TEST_TMP_BASE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/.tmp");

impl TestTempDir {
    pub fn new() -> Self {
        let base = std::path::Path::new(TEST_TMP_BASE);
        std::fs::create_dir_all(base).expect("Failed to create .tmp base directory");
        static COUNTER: AtomicU16 = AtomicU16::new(0);
        let id = format!(
            "{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
        );
        let path = base.join(id.to_string());
        std::fs::create_dir_all(&path).expect("Failed to create test temp directory");
        Self { path }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Default for TestTempDir {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TestTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl std::ops::Deref for TestTempDir {
    type Target = std::path::Path;
    fn deref(&self) -> &std::path::Path {
        &self.path
    }
}

impl AsRef<std::path::Path> for TestTempDir {
    fn as_ref(&self) -> &std::path::Path {
        &self.path
    }
}

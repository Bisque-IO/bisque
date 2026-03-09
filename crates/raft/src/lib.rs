//! Tokio runtime adapter for OpenRaft.
//!
//! This crate uses OpenRaft's built-in `TokioRuntime` for async operations.

pub mod codec;
pub mod config;
pub mod manager;
pub(crate) mod manifest_mdbx;
pub mod network;
pub mod record_format;
pub mod rpc_server;
pub mod segment_sync;
pub mod storage;
pub mod storage_mmap;
pub mod transport_tcp;
pub mod type_config;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod network_tests;

pub use manager::MultiRaftManager;
pub use network::{MultiRaftNetwork, MultiRaftNetworkFactory, MultiplexedTransport};
pub use openraft::TokioRuntime;
pub use rpc_server::{
    BisqueRpcServer, BisqueRpcServerConfig,
    protocol::{ResponseMessage, RpcMessage},
};
pub use segment_sync::{
    SegmentSyncClient, SegmentSyncClientConfig, SegmentSyncServer, SegmentSyncServerConfig,
    SnapshotFileEntry, SyncResult, list_segment_files,
};
pub use storage::MultiRaftLogStorage;
pub use storage_mmap::MmapGroupLogStorage;
pub use storage_mmap::MmapGroupLogStorage as GroupLogStorage;
pub use storage_mmap::MmapPerGroupLogStorage;
pub use storage_mmap::MmapPerGroupLogStorage as MultiplexedLogStorage;
pub use storage_mmap::MmapStorageConfig;
pub use storage_mmap::MmapStorageConfig as MultiplexedStorageConfig;
pub use storage_mmap::SegmentPrefetcher;
pub use transport_tcp::{
    BisqueTcpTransport, BisqueTcpTransportConfig, BisqueTransportError, BoxedReader, BoxedWriter,
    DefaultNodeRegistry, NodeAddressResolver,
};
pub use type_config::BisqueRaftTypeConfig;

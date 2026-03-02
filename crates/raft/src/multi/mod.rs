pub mod codec;
pub mod config;
pub mod manager;
pub(crate) mod manifest_mdbx;
pub mod network;
pub mod rpc_server;
pub(crate) mod segment_footer;
pub mod storage;
pub mod storage_impl;
pub mod storage_mmap;
pub mod tcp_transport;
pub mod type_config;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod network_tests;

// storage_tests module removed - tests integrated into storage_impl.rs

pub use config::MultiRaftConfig;
pub use manager::MultiRaftManager;
pub use network::{MultiRaftNetwork, MultiRaftNetworkFactory, MultiplexedTransport};
pub use storage::MultiRaftLogStorage;

pub use rpc_server::{
    BisqueRpcServer, BisqueRpcServerConfig,
    protocol::{ResponseMessage, RpcMessage},
};
pub use storage_impl::{GroupLogStorage, MultiplexedLogStorage, MultiplexedStorageConfig};
pub use storage_mmap::{MmapGroupLogStorage, MmapPerGroupLogStorage, MmapStorageConfig};
pub use tcp_transport::{
    BisqueTcpTransport, BisqueTcpTransportConfig, BisqueTransportError, BoxedReader, BoxedWriter,
    DefaultNodeRegistry, NodeAddressResolver,
};

pub mod codec;
pub mod config;
pub mod manager;
pub(crate) mod manifest_mdbx;
pub mod network;
pub mod rpc_server;
pub(crate) mod segment_footer;
pub mod storage;
pub mod storage_impl;
pub mod storage_mdbx;
pub mod storage_mmap;
pub mod tcp_transport;
pub mod type_config;

#[cfg(test)]
mod test_support;

// storage_tests module removed - tests integrated into storage_impl.rs



pub use config::MultiRaftConfig;
pub use manager::MultiRaftManager;
pub use network::{MultiRaftNetwork, MultiRaftNetworkFactory, MultiplexedTransport};
pub use storage::MultiRaftLogStorage;

pub use rpc_server::{
    ManiacRpcServer, ManiacRpcServerConfig,
    protocol::{ResponseMessage, RpcMessage},
};
pub use storage_impl::{GroupLogStorage, MultiplexedLogStorage, MultiplexedStorageConfig};
pub use storage_mdbx::{MdbxGroupLogStorage, MdbxPerGroupLogStorage, MdbxStorageConfig};
pub use storage_mmap::{MmapGroupLogStorage, MmapPerGroupLogStorage, MmapStorageConfig};
pub use tcp_transport::{
    DefaultNodeRegistry, ManiacTcpTransport, ManiacTcpTransportConfig, ManiacTransportError,
    NodeAddressResolver,
};

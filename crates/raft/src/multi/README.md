# Maniac Multi-Raft Implementation

A high-performance, multi-group Raft consensus implementation built on the maniac async runtime, providing a complete networking and filesystem stack for distributed systems.

## Overview

The `multi` module implements **Multi-Raft**, a pattern that runs multiple independent Raft consensus groups on a single node. This is ideal for scenarios requiring many small, isolated consensus clusters (e.g., key-value stores with per-shard replication, multi-tenant databases).

### Key Features

- **Pure Maniac Implementation**: Zero Tokio dependencies, built entirely on maniac's io-uring and polling-based async runtime
- **Efficient Heartbeat Coalescing**: Batches heartbeats across multiple groups to reduce network overhead
- **Connection Pooling**: Reusable TCP connections with configurable pooling per peer
- **Filesystem Persistence**: Async file I/O using maniac's filesystem layer
- **Multiplexed Transport**: Single transport layer handles all RPC types for all Raft groups
- **Scalable Architecture**: DashMap-based concurrent data structures for high concurrency

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Application Layer                         │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│              MultiRaftManager<C, T, S>                      │
│  - Manages multiple Raft<C> instances                       │
│  - Routes requests to appropriate groups                    │
│  - Coordinates transport and storage                        │
└──────────────────────┬──────────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
┌───────▼──────┐ ┌────▼─────┐ ┌─────▼──────┐
│  Transport   │ │ Network  │ │  Storage   │
│    Layer     │ │  Layer   │ │   Layer    │
└──────────────┘ └──────────┘ └────────────┘
```

### Component Breakdown

#### 1. **MultiRaftManager** (`manager.rs`)
Central orchestrator that:
- Manages a map of `group_id -> Raft<C>` instances
- Provides `add_group()`, `get_group()`, `remove_group()` operations
- Coordinates between transport and storage layers

#### 2. **Multiplexed Transport** (`network.rs`, `maniac_tcp_transport.rs`)
The transport layer implements heartbeat coalescing:

```rust
pub trait MultiplexedTransport<C: RaftTypeConfig> {
    async fn send_append_entries(&self, target: NodeId, group_id: u64, rpc: AppendEntriesRequest<C>) -> Result<...>;
    async fn send_vote(&self, target: NodeId, group_id: u64, rpc: VoteRequest<C>) -> Result<...>;
    async fn send_install_snapshot(&self, target: NodeId, group_id: u64, rpc: InstallSnapshotRequest<C>) -> Result<...>;
    async fn send_heartbeat_batch(&self, target: NodeId, batch: &[(u64, AppendEntriesRequest<C>)]) -> Result<...>;
}
```

**ManiacTcpTransport** provides:
- Connection pooling with `max_connections_per_addr`
- Configurable timeouts (connect, read, write)
- TCP keepalive and nodelay options
- Custom zero-copy binary codec over length-prefixed frames (4-byte LE length + payload)

#### 3. **Network Layer** (`network.rs`)
Implements RaftNetworkV2 with heartbeat batching:
- `MultiRaftNetworkFactory`: Creates networks per target node
- `HeartbeatBuffer`: Batches heartbeats per target to coalesce multiple groups
- Automatic flush on `heartbeat_interval` (or when a max batch size is reached)

#### 4. **Storage Layer** (`storage.rs`, `maniac_storage.rs`)
**MultiRaftStorage** trait:
```rust
pub trait MultiRaftStorage<C: RaftTypeConfig> {
    type LogStorage: RaftLogStorage<C>;
    type StateMachine: RaftStateMachine<C>;
    
    async fn get_log_storage(&self, group_id: u64) -> Self::LogStorage;
    async fn get_state_machine(&self, group_id: u64) -> Self::StateMachine;
}
```

**ManiacRaftStorage** provides:
- Per-group directory isolation (`./raft-data/group-{id}/`)
- Separate directories for logs, state machine, and snapshots
- Async file I/O using maniac's filesystem primitives
- Configurable sync-on-write for durability/performance tradeoffs
- In-memory caching of log entries

#### 5. **RPC Server** (`maniac_rpc_server.rs`)
Accepts incoming TCP connections and routes to appropriate Raft groups:
- Length-prefixed binary protocol
- Frame-based message parsing
- Per-connection async handlers
- Integration with `MultiRaftManager`

## Quick Start

### Basic Setup

```rust
use maniac::raft::multi::*;
use maniac::sync::Arc;
use std::net::SocketAddr;

// 1. Configure transport
let transport_config = ManiacTcpTransportConfig {
    connect_timeout: Duration::from_secs(10),
    max_connections_per_addr: 5,
    tcp_nodelay: true,
    ..Default::default()
};
let transport = ManiacTcpTransport::<YourRaftConfig>::new(transport_config);

// 2. Configure storage
let storage_config = ManiacStorageConfig {
    base_dir: PathBuf::from("./raft-data"),
    sync_on_write: true,
    max_log_file_size: 64 * 1024 * 1024,
};
let storage = ManiacRaftStorage::new(storage_config)?;

// 3. Create manager
let multi_config = MultiRaftConfig {
    heartbeat_interval: Duration::from_millis(100),
};
let manager = Arc::new(MultiRaftManager::new(transport, storage, multi_config));

// 4. Start RPC server
let server_config = ManiacRpcServerConfig {
    bind_addr: "0.0.0.0:5000".parse()?,
    ..Default::default()
};
let server = Arc::new(ManiacRpcServer::new(server_config, manager.clone()));

// 5. Spawn server
maniac::spawn(async move {
    server.serve().await.expect("Server failed");
});

// 6. Create Raft group
let raft = manager.add_group(
    1, // group_id
    1, // node_id
    Arc::new(openraft::Config::default())
).await?;
```

### Running a Multi-Node Cluster

See the complete example in `examples/raft_multi_node.rs`:

```bash
# Terminal 1
cargo run --example raft_multi_node -- \
  --node-id 1 --port 5001 \
  --peer 127.0.0.1:5002 --peer 127.0.0.1:5003

# Terminal 2
cargo run --example raft_multi_node -- \
  --node-id 2 --port 5002 \
  --peer 127.0.0.1:5001 --peer 127.0.0.1:5003

# Terminal 3
cargo run --example raft_multi_node -- \
  --node-id 3 --port 5003 \
  --peer 127.0.0.1:5001 --peer 127.0.0.1:5002
```

## Configuration

### Transport Configuration (`ManiacTcpTransportConfig`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connect_timeout` | `Duration` | 10s | TCP connection timeout |
| `read_timeout` | `Duration` | 30s | Socket read timeout |
| `write_timeout` | `Duration` | 30s | Socket write timeout |
| `max_connections_per_addr` | `usize` | 5 | Connection pool size per peer |
| `tcp_keepalive` | `bool` | true | Enable TCP keepalive |
| `tcp_nodelay` | `bool` | true | Disable Nagle's algorithm |

### Storage Configuration (`ManiacStorageConfig`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_dir` | `PathBuf` | `./raft-data` | Root storage directory |
| `sync_on_write` | `bool` | false | `fsync()` after each write (durability vs performance) |
| `max_log_file_size` | `u64` | 64MB | Log rotation threshold |

### Multi-Raft Configuration (`MultiRaftConfig`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `heartbeat_interval` | `Duration` | 100ms | Coalesced heartbeat flush interval |

## Performance Characteristics

### Heartbeat Coalescing

Without coalescing, N groups × M peers = N×M heartbeat messages per interval. With coalescing, this becomes M messages (one per peer) containing batched heartbeats for all groups.

**Example**: 100 groups, 5 peers
- Without: 500 messages/interval
- With: 5 messages/interval
- **99% reduction** in heartbeat traffic

### Connection Pooling

Each peer maintains a reusable connection pool, eliminating TCP handshake overhead. Pool size is configurable per address.

### Async I/O

Built on maniac's io-uring (Linux) or poll-based (cross-platform) async runtime, providing true async filesystem operations without blocking threads.

## Directory Structure

```
raft-data/
└── group-{id}/
    ├── logs/
    │   ├── entries.log      # Appended log entries
    │   └── vote.dat         # Persistent vote state
    ├── state_machine/
    │   └── applied.dat      # Last applied log index
    └── snapshots/
        └── {id}.snap        # Snapshot files
```

## Advanced Usage

### Custom Transport Implementation

Implement `MultiplexedTransport` for alternative transports (Unix sockets, QUIC, etc.):

```rust
struct MyCustomTransport;

impl<C: RaftTypeConfig> MultiplexedTransport<C> for MyCustomTransport {
    async fn send_append_entries(&self, target: C::NodeId, group_id: u64, rpc: AppendEntriesRequest<C>) -> Result<...> {
        // Your implementation
    }
    // ... other methods
}
```

### Custom Storage Backend

Implement `MultiRaftStorage` for alternative storage (RocksDB, S3, etc.):

```rust
struct MyCustomStorage;

impl<C: RaftTypeConfig> MultiRaftStorage<C> for MyCustomStorage {
    type LogStorage = MyLogStorage;
    type StateMachine = MyStateMachine;
    
    async fn get_log_storage(&self, group_id: u64) -> Self::LogStorage {
        // Your implementation
    }
    
    async fn get_state_machine(&self, group_id: u64) -> Self::StateMachine {
        // Your implementation
    }
}
```

## Implementation Details

### Serialization Format

Uses a **custom binary codec** implemented in `codec.rs`. Messages are length-prefixed (4-byte LE length header + codec payload) and include a `request_id` for true multiplexing over a single TCP connection.

### Error Handling

- Transport errors: `ManiacTransportError` with detailed error kinds
- Storage errors: Converted to `StorageError<C>` via maniac-raft's error system
- Network errors: Wrapped in `RPCError<C>` with decomposition support

### Concurrency Model

- `DashMap` for concurrent group management
- `Arc<Mutex<TcpStream>>` for connection pooling
- `Arc<Mutex<...>>` for state machine state
- Per-group isolation prevents contention

## Testing

Run the example cluster:

```bash
# Clean previous data
rm -rf ./raft-data

# Run tests
cargo test -p maniac multi

# Run example
cargo run --example raft_multi_node -- --help
```

## Future Enhancements

- [ ] QUIC transport implementation
- [ ] Compression for large snapshots
- [ ] Metrics/tracing integration
- [ ] Dynamic membership changes
- [ ] Log compaction strategies
- [ ] WAL-based log storage for durability

## License

MIT OR Apache-2.0
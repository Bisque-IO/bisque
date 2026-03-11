# Kafka Protocol Compatibility Matrix

## Overview

bisque-mq-kafka implements the Apache Kafka binary protocol, allowing standard Kafka
clients (librdkafka, kafka-python, franz-go, sarama, etc.) to produce and consume
messages via bisque-mq's Raft-replicated log engine.

**Target**: 100% compatibility with Kafka protocol for all client-facing APIs.

---

## API Key Support Matrix

| API Key | Name | Status | Versions | Notes |
|---------|------|--------|----------|-------|
| 0 | Produce | DONE | 0-3 | Full record batch v2, compression support |
| 1 | Fetch | DONE | 0-4 | Long-polling with max_wait_ms/min_bytes |
| 2 | ListOffsets | DONE | 0-1 | Earliest (-2), latest (-1), timestamp lookup |
| 3 | Metadata | DONE | 0-1 | Topic/partition/broker discovery |
| 8 | OffsetCommit | DONE | 0-2 | Raft-replicated group offsets + legacy |
| 9 | OffsetFetch | DONE | 0-1 | Group-based + legacy fallback |
| 10 | FindCoordinator | DONE | 0-1 | Returns self (single-broker model) |
| 11 | JoinGroup | DONE | 0-1 | Raft-replicated consumer groups |
| 12 | Heartbeat | DONE | 0-0 | Generation validation |
| 13 | LeaveGroup | DONE | 0-0 | Triggers rebalance |
| 14 | SyncGroup | DONE | 0-0 | Leader assignments via Raft |
| 15 | DescribeGroups | DONE | 0-0 | Reads from Raft-replicated state |
| 16 | ListGroups | DONE | 0-0 | DashMap iteration |
| 17 | SaslHandshake | DONE | 0-1 | PLAIN mechanism |
| 18 | ApiVersions | DONE | 0-0 | Reports all supported APIs |
| 19 | CreateTopics | DONE | 0-0 | Multi-partition via naming convention |
| 20 | DeleteTopics | DONE | 0-0 | Deletes all partitions |
| 21 | DeleteRecords | DONE | 0-0 | Truncates log to target offset |
| 22 | InitProducerId | DONE | 0-0 | Monotonic PID assignment |
| 24 | AddPartitionsToTxn | DONE | 0-0 | Transaction partition registration |
| 25 | AddOffsetsToTxn | DONE | 0-0 | Transaction offset group registration |
| 26 | EndTxn | DONE | 0-0 | Commit/abort transaction |
| 28 | TxnOffsetCommit | DONE | 0-0 | Transactional offset commit |
| 32 | DescribeConfigs | DONE | 0-0 | Returns default config values |
| 33 | AlterConfigs | DONE | 0-0 | Accepts but no-ops (single-node) |
| 36 | SaslAuthenticate | DONE | 0-1 | PLAIN mechanism with pluggable auth |
| 37 | CreatePartitions | DONE | 0-0 | Adds partitions to existing topics |
| 42 | DeleteGroups | DONE | 0-0 | Removes empty consumer groups |
| 47 | OffsetDelete | DONE | 0-0 | Deletes committed offsets |

---

## Feature Compatibility

### Record Format
| Feature | Status | Notes |
|---------|--------|-------|
| Record Batch v2 (magic=2) | DONE | Full encode/decode |
| Compression: None | DONE | Default |
| Compression: GZIP | DONE | flate2 |
| Compression: Snappy | DONE | snap crate (Xerial framing) |
| Compression: LZ4 | DONE | lz4_flex (Kafka KafkaLZ4 framing) |
| Compression: ZSTD | DONE | zstd crate |
| Record headers | DONE | Key-value pairs preserved |
| Null keys/values | DONE | Nullable varint-bytes |
| Timestamps | DONE | CreateTime from producer |

### Producer
| Feature | Status | Notes |
|---------|--------|-------|
| Basic produce | DONE | acks=-1/0/1 |
| Idempotent produce | DONE | InitProducerId, PID/epoch/seq tracking |
| Transactional produce | DONE | Full transaction lifecycle |
| Compression | DONE | All 4 codecs on decode, none on encode |

### Consumer
| Feature | Status | Notes |
|---------|--------|-------|
| Basic fetch | DONE | Offset-based reads |
| Fetch long-polling | DONE | max_wait_ms + min_bytes |
| Consumer groups | DONE | JoinGroup/SyncGroup/Heartbeat/LeaveGroup |
| Partition assignment | DONE | Range, RoundRobin, Sticky |
| Offset commit/fetch | DONE | Raft-replicated group offsets |
| Auto-offset reset | DONE | Earliest/Latest via ListOffsets |

### Authentication
| Feature | Status | Notes |
|---------|--------|-------|
| SASL/PLAIN | DONE | Username/password via pluggable trait |
| SASL/SCRAM | — | Not implemented |
| TLS/SSL | — | Handled at transport layer (outside adapter) |

### Admin Operations
| Feature | Status | Notes |
|---------|--------|-------|
| CreateTopics | DONE | Multi-partition |
| DeleteTopics | DONE | All partitions |
| CreatePartitions | DONE | Add partitions to existing topic |
| DeleteGroups | DONE | Remove empty groups |
| DeleteRecords | DONE | Truncate to offset |
| DescribeConfigs | DONE | Returns defaults |
| AlterConfigs | DONE | Accepts, no-ops |
| OffsetDelete | DONE | Remove committed offsets |

### Transactions
| Feature | Status | Notes |
|---------|--------|-------|
| InitProducerId | DONE | PID allocation with transactional ID |
| AddPartitionsToTxn | DONE | Register partitions |
| AddOffsetsToTxn | DONE | Register offset group |
| EndTxn (commit) | DONE | Commit transaction |
| EndTxn (abort) | DONE | Abort transaction |
| TxnOffsetCommit | DONE | Commit offsets in transaction |

---

## Implementation Phases

### Phase 1: Compression Support (Critical)
- [x] Add flate2, snap, lz4_flex dependencies
- [x] Decompress record batch on decode (all 4 codecs)
- [x] Support compressed batches from producers
- [x] Tests for each compression type

### Phase 2: Fetch Long-Polling (Critical)
- [x] Add tokio::sync::Notify for topic write notifications
- [x] Implement max_wait_ms timeout in fetch handler
- [x] min_bytes threshold check
- [x] Zero-latency path when data is already available

### Phase 3: Idempotent Producer (Critical)
- [x] Add InitProducerId API (key 22)
- [x] Monotonic PID counter
- [x] Producer epoch tracking
- [x] Sequence number deduplication in produce handler

### Phase 4: SASL Authentication (Major)
- [x] Add SaslHandshake API (key 17)
- [x] Add SaslAuthenticate API (key 36)
- [x] PLAIN mechanism implementation
- [x] Pluggable KafkaAuthenticator trait
- [x] Connection-level auth state

### Phase 5: Admin APIs (Major)
- [x] CreatePartitions (key 37)
- [x] DeleteGroups (key 42)
- [x] DeleteRecords (key 21)
- [x] DescribeConfigs (key 32)
- [x] AlterConfigs (key 33)
- [x] OffsetDelete (key 47)

### Phase 6: Transaction Support (Major)
- [x] AddPartitionsToTxn (key 24)
- [x] AddOffsetsToTxn (key 25)
- [x] EndTxn (key 26)
- [x] TxnOffsetCommit (key 28)
- [x] Transaction coordinator state

### Phase 7: Remaining APIs (Minor)
- Deferred: DescribeCluster, DescribeLogDirs, DescribeProducers — internal/rarely used

---

## Architecture Notes

### Zero-Copy Design
All string/bytes fields use `Bytes` slices into the incoming wire frame. No allocations
for request parsing. Response encoding writes directly to `BytesMut`.

### Lock-Free Hot Path
- Partition map: `ArcSwap<PartitionMap>` — readers never block
- Consumer groups: `MqMetadata.consumer_groups` via `DashMap` — lock-free iteration
- Metrics: pre-allocated handles — no inline `metrics::counter!()` calls

### Raft Integration
All writes (publish, group operations, offsets, transactions) go through
`MqWriteBatcher` → Raft consensus. Reads from `MqMetadata` are fully replicated state.

### Compression Strategy
- **Decode**: Always decompress (clients may send any codec)
- **Encode**: Never compress fetch responses (clients decompress on their side;
  the original data is stored uncompressed in bisque-mq)

### Transaction Model
Transactions use a lightweight coordinator that tracks PID→transaction state.
On commit, pending messages are made visible; on abort, they are discarded.
This provides exactly-once semantics for produce+consume workflows.

# bisque-mq-kafka — Kafka Binary Protocol Adapter

## Overview

Implements Kafka's binary TCP protocol (versions 0.10+) as a thin adapter over
bisque-mq's Raft-replicated topic/queue engine. Kafka clients (librdkafka,
kafka-python, franz-go, sarama, etc.) connect to bisque-mq as if it were a
Kafka broker.

## Architecture

```
TCP listener  (port 9092 default)
  └─ KafkaCodec        frame parse / serialize (4-byte length-prefixed)
       └─ KafkaConnection  per-connection state, correlation ID echo
            ├─ ApiVersions   → static capability list
            ├─ Metadata      → enumerate topics via PartitionMap
            ├─ Produce       → MqCommand::Publish via MqWriteBatcher
            ├─ Fetch         → read topic log from offset (KafkaLogReader trait)
            ├─ ListOffsets   → head/tail index from TopicMeta
            ├─ FindCoordinator → return self
            ├─ JoinGroup / SyncGroup / Heartbeat / LeaveGroup
            │      └─ GroupCoordinator state machine
            ├─ OffsetCommit  → MqCommand::CommitOffset
            └─ OffsetFetch   → query TopicConsumerOffset
```

## Partition Model

Kafka topics are multi-partition; bisque-mq topics are single-log.

**Mapping**: Kafka topic `events` with 3 partitions becomes 3 bisque-mq topics:
`events-0`, `events-1`, `events-2`.

- `PartitionMap` scans bisque-mq topics matching `{name}-{digit+}` to build
  the Kafka topic → partition list mapping.
- Producers partition via key hash (`murmur2(key) % num_partitions`) — computed
  client-side (standard Kafka behavior). The adapter just routes to the
  correct bisque-mq topic.
- `CreateTopics` API auto-creates N bisque-mq topics for the requested
  partition count.

## Consumer Groups

Kafka's consumer-group protocol (JoinGroup/SyncGroup/Heartbeat/LeaveGroup) is
implemented server-side in `GroupCoordinator`:

- **GroupPhase state machine**: Empty → PreparingRebalance → CompletingRebalance → Stable → Dead
- **Member tracking**: member_id, client_id, session_timeout, supported protocols
- **Rebalance trigger**: member join/leave/timeout transitions group to PreparingRebalance
- **Leader election**: first member to join becomes leader; leader proposes assignments via SyncGroup
- **Assignment distribution**: coordinator holds SyncGroup responses until leader submits, then fans out
- **Session expiry**: background sweep detects stale heartbeats and triggers rebalance

## Offset Model

- bisque-mq offsets = Raft log indices (u64), monotonically increasing
- Direct mapping to Kafka offsets (i64, but always positive)
- `OffsetCommit` → `MqCommand::CommitOffset { topic_id, consumer_id, offset }`
- `OffsetFetch` → read `TopicConsumerOffset` from engine state
- `ListOffsets` → `tail_index` (earliest) / `head_index` (latest) from TopicMeta

## Wire Protocol

Kafka uses a custom binary protocol over TCP:
- **Frame**: `[i32 size][payload]` — size covers everything after itself
- **Request header**: `[i16 api_key][i16 api_version][i32 correlation_id][nullable_string client_id]`
- **Response header**: `[i32 correlation_id]`
- **Data types**: i8, i16, i32, i64, varint (zigzag), nullable strings (i16-prefixed),
  byte arrays (i32-prefixed), arrays (i32-prefixed count)
- **RecordBatch**: Kafka v2 record batch with base_offset, CRC32C, attributes,
  and variable-length records using zigzag varints

## Supported API Keys (Phase 1)

| API Key | Name             | Version | Direction |
|---------|------------------|---------|-----------|
| 0       | Produce          | 3       | →Publish  |
| 1       | Fetch            | 4       | →ReadLog  |
| 2       | ListOffsets      | 1       | →TopicMeta|
| 3       | Metadata         | 1       | →Engine   |
| 8       | OffsetCommit     | 2       | →CommitOffset |
| 9       | OffsetFetch      | 1       | →TopicConsumerOffset |
| 10      | FindCoordinator  | 0       | →Self     |
| 11      | JoinGroup        | 1       | →Coordinator |
| 12      | Heartbeat        | 0       | →Coordinator |
| 13      | LeaveGroup       | 0       | →Coordinator |
| 14      | SyncGroup        | 0       | →Coordinator |
| 18      | ApiVersions      | 0       | →Static   |

## Kafka → bisque-mq Command Mapping

| Kafka Operation | MqCommand | Notes |
|----------------|-----------|-------|
| Produce | `Publish { topic_id, messages }` | Per-partition, key in MessagePayload |
| Fetch | *read-only* | Read messages from raft log at offset |
| OffsetCommit | `CommitOffset { topic_id, consumer_id, offset }` | Per-partition |
| JoinGroup | `RegisterConsumer` | Plus coordinator state |
| LeaveGroup | `DisconnectConsumer` | Plus coordinator state |
| Heartbeat | `Heartbeat { consumer_id }` | Keep session alive |
| CreateTopics | `CreateTopic` × N | One per partition |

## Message Format Mapping

| Kafka Record Field | MessagePayload Field | Notes |
|-------------------|---------------------|-------|
| key | key: Option<Bytes> | Direct |
| value | value: Bytes | Direct |
| headers | headers: Vec<(String, Bytes)> | Direct |
| timestamp | timestamp: u64 | Milliseconds |
| offset | message_id (raft log index) | Assigned on publish |

## Fetch Long-Polling

Kafka clients issue `Fetch` with `max_wait_ms`. If no data is immediately
available, the server holds the connection for up to `max_wait_ms`:

- Use `tokio::time::timeout(max_wait_ms)` with a topic notification channel
- The `KafkaLogReader` trait provides both message reading and new-data
  notification

## Replication & Durability

- All writes go through Raft consensus (equivalent to `acks=all`)
- `acks=0` (fire-and-forget) can be supported by not awaiting the batcher response
- `acks=1` is equivalent to `acks=all` in our model (Raft always requires quorum)
- ISR concept is implicit: all Raft voters are "in sync"

## Open Questions

1. **Multi-node metadata**: How to advertise multiple brokers when bisque-mq runs
   as a Raft group? Each node could advertise itself; clients would connect to
   the leader for produces and any node for reads.
2. **Transactions**: Kafka's exactly-once with PID/epoch not implemented.
   Phase 2 consideration.
3. **Compression**: Kafka RecordBatch supports gzip/snappy/lz4/zstd. Initial
   implementation stores uncompressed; could add snappy/zstd (already in
   workspace deps).
4. **Log compaction**: Kafka's `cleanup.policy=compact` for key-based retention.
   Not supported yet.
5. **Quotas**: Per-client produce/fetch rate limiting.
6. **SASL authentication**: Initial implementation uses no auth. Could integrate
   with bisque-meta's token system via SASL/PLAIN or SASL/OAUTHBEARER.
7. **Schema registry compatibility**: Clients embedding schema IDs in payloads
   should work transparently (opaque bytes).
8. **Fetch from raft log**: Need efficient random access to raft log entries
   by index range. May need a `KafkaLogReader` trait that the server binary
   implements.
9. **CreateTopics / DeleteTopics APIs**: Should be straightforward mapping to
   existing `MqCommand::CreateTopic` / `MqCommand::DeleteTopic`.
10. **Consumer __consumer_offsets topic**: Some Kafka tools expect this internal
    topic to exist. May need to be stubbed.

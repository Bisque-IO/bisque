# Kafka 4.2 Protocol — Full Gap Analysis & Implementation Plan

## Reference

- **Spec**: [Apache Kafka 4.2 Protocol](https://kafka.apache.org/42/design/protocol) — local copy at `docs/protocol.md`
- **Scope**: All 77 stable API keys (0–92) in the Kafka protocol
- **Current implementation**: 38 API keys, flexible version support active at version thresholds
- **Cross-check date**: 2026-03-12

---

## 0. Implementation Progress

### Completed

- [x] **Phase 1: Flexible Version Codec Helpers** — compact string/array/bytes, unsigned varint, tagged field skip/write
- [x] **Phase 2: Core API Version Upgrades** — Version-aware encode/decode for 29 original + 2 new APIs
- [x] **Phase 3: Wire Format Correctness** — Added 20+ missing struct fields (transactional_id, group_instance_id, committed_leader_epoch, session_lifetime_ms, etc.), fixed all version-gated decode/encode paths
- [x] **Phase 4: Flexible Version Support** — Request header v2 decode, response header v1 encode, compact encoding activated at each API's flexible threshold via `enc_*/dec_*` helpers, version ranges bumped to flexible thresholds
- [x] **Phase 5: Structural API Changes** — Not needed at current version ranges (all structural changes are at versions beyond what we advertise)
- [x] **Phase 6: Missing Client-Facing APIs** — Added IncrementalAlterConfigs (44), DescribeAcls (29)
- [x] **Phase 7: Fetch Session Support** — session_id/session_epoch decoded from Fetch v7+, session_id=0 in responses (stateless mode)
- [x] **Phase 8: Static Membership** — group_instance_id fields wired through JoinGroup/SyncGroup/Heartbeat/LeaveGroup/OffsetCommit/DescribeGroups
- [x] **Phase 9-10: Security & Admin Stubs** — Added CreateAcls (30), DeleteAcls (31), DescribeLogDirs (35), DescribeUserScramCredentials (50), AlterUserScramCredentials (51)

### Remaining

- All 77 API keys implemented. Stub APIs return UNSUPPORTED_VERSION and can be fleshed out as needed.

---

## 1. Implemented API Versions vs Kafka 4.2 Spec

### 1.1 Version Coverage Table

| Key | Name | Bisque | Spec Max | Flexible From | Status |
|-----|------|--------|----------|---------------|--------|
| 0 | Produce | 0-10 | 13 | v9 | Flexible ✓, gap v11-13 (UUID topics) |
| 1 | Fetch | 0-12 | 13 | v12 | Flexible ✓, gap v13 (UUID topics) |
| 2 | ListOffsets | 0-6 | 11 | v6 | Flexible ✓, gap v7-11 |
| 3 | Metadata | 0-9 | 13 | v9 | Flexible ✓, gap v10-13 (UUID topics) |
| 8 | OffsetCommit | 0-8 | 10 | v8 | Flexible ✓, gap v9-10 |
| 9 | OffsetFetch | 0-6 | 10 | v6 | Flexible ✓, gap v7-10 (multi-group) |
| 10 | FindCoordinator | 0-3 | 8 | v3 | Flexible ✓, gap v4-8 (multi-key) |
| 11 | JoinGroup | 0-6 | 9 | v6 | Flexible ✓, gap v7-9 |
| 12 | Heartbeat | 0-4 | 5 | v4 | Flexible ✓, gap v5 |
| 13 | LeaveGroup | 0-4 | 6 | v4 | Flexible ✓, gap v5-6 |
| 14 | SyncGroup | 0-4 | 6 | v4 | Flexible ✓, gap v5-6 |
| 15 | DescribeGroups | 0-5 | 6 | v5 | Flexible ✓, gap v6 |
| 16 | ListGroups | 0-4 | 5 | v3 | Flexible ✓, gap v5 |
| 17 | SaslHandshake | 0-1 | 7 | — | OK, gap v2-7 |
| 18 | ApiVersions | 0-3 | 7 | v3 | Flexible ✓, gap v4-7 |
| 19 | CreateTopics | 0-5 | 7 | v5 | Flexible ✓, gap v6-7 |
| 20 | DeleteTopics | 0-4 | 6 | v4 | Flexible ✓, gap v5-6 |
| 21 | DeleteRecords | 0-2 | 6 | v2 | Flexible ✓, gap v3-6 |
| 22 | InitProducerId | 0-2 | 6 | v2 | Flexible ✓, gap v3-6 |
| 23 | OffsetForLeaderEpoch | 0-4 | 5 | — | OK, gap v5 |
| 24 | AddPartitionsToTxn | 0-3 | 5 | v3 | Flexible ✓, gap v4-5 |
| 25 | AddOffsetsToTxn | 0-3 | 5 | v3 | Flexible ✓, gap v4-5 |
| 26 | EndTxn | 0-3 | 5 | v3 | Flexible ✓, gap v4-5 |
| 28 | TxnOffsetCommit | 0-3 | 5 | v3 | Flexible ✓, gap v4-5 |
| 29 | DescribeAcls | 0-2 | 3 | v2 | Flexible ✓, stub |
| 30 | CreateAcls | 0-2 | 3 | v2 | Flexible ✓, stub |
| 31 | DeleteAcls | 0-2 | 3 | v2 | Flexible ✓, stub |
| 32 | DescribeConfigs | 0-4 | 4 | v4 | Flexible ✓, full |
| 33 | AlterConfigs | 0-2 | 4 | v2 | Flexible ✓, gap v3-4 |
| 35 | DescribeLogDirs | 0-2 | 4 | v2 | Flexible ✓, stub |
| 36 | SaslAuthenticate | 0-2 | 3 | v2 | Flexible ✓, gap v3 |
| 37 | CreatePartitions | 0-2 | 3 | v2 | Flexible ✓, gap v3 |
| 42 | DeleteGroups | 0-2 | 2 | v2 | Flexible ✓, full |
| 44 | IncrementalAlterConfigs | 0-1 | 1 | v1 | Flexible ✓, full |
| 47 | OffsetDelete | 0-0 | 2 | — | OK, gap v1-2 |
| 50 | DescribeUserScramCredentials | 0-0 | 0 | v0 | Flexible ✓, stub |
| 51 | AlterUserScramCredentials | 0-0 | 0 | v0 | Flexible ✓, stub |
| 60 | DescribeCluster | 0-0 | 2 | — | OK, gap v1-2 |

### 1.2 Wire Format Correctness Issues (Within Supported Versions)

These are bugs in the current implementation for versions we claim to support.

#### Produce (key 0) — versions 0-8

| Issue | Severity | Detail |
|-------|----------|--------|
| `transactional_id` discarded | CRITICAL | Read from wire at v3+ but dropped (`_` variable), not stored in `ProduceRequest` struct |
| Missing `record_errors[]` in response | HIGH | v8 response requires `[record_errors]` array + per-partition `error_message` — not in struct |
| `log_append_time_ms` version check | MEDIUM | Response encodes at `v >= 2` but spec says field added at v3 (off-by-one) |
| `log_append_time_ms` hardcoded | LOW | Always writes -1, should store actual timestamp from engine |
| `log_start_offset` hardcoded | LOW | Always writes 0, should store actual value |

#### Fetch (key 1) — versions 0-11

| Issue | Severity | Detail |
|-------|----------|--------|
| `replica_id` discarded | MEDIUM | Read but not stored in struct |
| `max_bytes` (top-level) discarded | MEDIUM | Read at v3+ but dropped — needed for resource limiting |
| `isolation_level` discarded | HIGH | Read at v4+ but dropped — critical for transactional reads |
| `session_id`/`session_epoch` discarded | HIGH | Read at v7+ but dropped — breaks fetch session tracking |
| `rack_id` discarded | MEDIUM | Read at v11+ but dropped |
| `current_leader_epoch` wrong version | BUG | Decoded at `v9+` but spec says field added at v10+ |
| `log_start_offset` in partition discarded | MEDIUM | Read at v5+ but not stored in `FetchPartitionData` |
| `forgotten_topics_data` discarded | HIGH | Read at v7+ but dropped — breaks incremental fetch |
| Response `preferred_read_replica` missing | MEDIUM | v11+ response field not encoded |
| Response `last_stable_offset` hardcoded | LOW | Always -1 |
| Response `log_start_offset` hardcoded | LOW | Always 0 |
| Response `aborted_transactions` hardcoded | LOW | Always empty array |

#### Metadata (key 3) — versions 0-8

| Issue | Severity | Detail |
|-------|----------|--------|
| `include_cluster_authorized_operations` missing | LOW | v8+ request field not decoded |
| `include_topic_authorized_operations` missing | LOW | v8+ request field not decoded |
| `broker.rack` hardcoded null | LOW | Always null, should pass through |
| `is_internal` hardcoded false | LOW | Should reflect internal topic status |
| `leader_epoch` hardcoded 0 | LOW | Should reflect actual epoch |
| `offline_replicas` hardcoded empty | LOW | Should reflect actual state |
| `cluster_authorized_operations` hardcoded | LOW | Hardcoded `i32::MIN`, wrong sentinel |

#### Consumer Group APIs

| API | Issue | Severity |
|-----|-------|----------|
| Heartbeat (12) | `group_instance_id` not decoded at v3+ | MEDIUM |
| LeaveGroup (13) | v3+ restructured to `[members]` array — still reads single `member_id` | HIGH |
| JoinGroup (11) | `group_instance_id` not decoded at v5+ | MEDIUM |
| JoinGroup (11) | Response `group_instance_id` per member missing at v5+ | MEDIUM |
| SyncGroup (14) | `group_instance_id` not decoded at v3+ | MEDIUM |
| DescribeGroups (15) | `include_authorized_operations` not decoded at v3+ | LOW |
| DescribeGroups (15) | Response `authorized_operations` missing at v3+ | LOW |
| DescribeGroups (15) | Response `group_instance_id` per member missing at v4+ | MEDIUM |
| ListGroups (16) | No request fields decoded (missing `states_filter`) | LOW |
| OffsetCommit (8) | `group_instance_id` read at v7+ but not stored | MEDIUM |
| OffsetCommit (8) | `committed_leader_epoch` not decoded at v6+ | MEDIUM |
| OffsetFetch (9) | Response `committed_leader_epoch` missing at v5+ | MEDIUM |

#### Transaction APIs

| API | Issue | Severity |
|-----|-------|----------|
| TxnOffsetCommit (28) | Missing `committed_leader_epoch` at v2+ | MEDIUM |
| SaslAuthenticate (36) | Missing `session_lifetime_ms` in response at v1+ | LOW |

---

## 2. Missing APIs — Full Catalog

### 2.1 Client-Facing (High Impact)

| Key | Name | Spec Max | Priority | Notes |
|-----|------|----------|----------|-------|
| 27 | WriteTxnMarkers | 5 | P2 | Inter-broker; transaction completion |
| 43 | ElectLeaders | 2 | P3 | Admin — stub for single-node |
| 44 | IncrementalAlterConfigs | 1 | P2 | Preferred over AlterConfigs by modern clients |
| 48 | DescribeClientQuotas | 2 | P3 | Quota management |
| 49 | AlterClientQuotas | 2 | P3 | Quota management |
| 57 | UpdateFeatures | 2 | P3 | Feature flag management |
| 61 | DescribeProducers | 2 | P3 | Transaction debugging |
| 65 | DescribeTransactions | 2 | P3 | Transaction debugging |
| 66 | ListTransactions | 2 | P3 | Transaction debugging |
| 75 | DescribeTopicPartitions | 2 | P2 | KIP-966: paginated topic metadata |

### 2.2 ACL & Security

| Key | Name | Spec Max | Priority | Notes |
|-----|------|----------|----------|-------|
| 29 | DescribeAcls | 3 | P2 | Return empty ACL list |
| 30 | CreateAcls | 4 | P3 | Authorization |
| 31 | DeleteAcls | 4 | P3 | Authorization |
| 38 | CreateDelegationToken | 3 | P3 | Token-based auth |
| 39 | RenewDelegationToken | 3 | P3 | Token-based auth |
| 40 | ExpireDelegationToken | 3 | P3 | Token-based auth |
| 41 | DescribeDelegationToken | 3 | P3 | Token-based auth |
| 50 | DescribeUserScramCredentials | 2 | P3 | SCRAM auth management |
| 51 | AlterUserScramCredentials | 2 | P3 | SCRAM auth management |

### 2.3 Log & Replica Management

| Key | Name | Spec Max | Priority | Notes |
|-----|------|----------|----------|-------|
| 34 | AlterReplicaLogDirs | 4 | P3 | N/A single-node |
| 35 | DescribeLogDirs | 4 | P3 | kafka-reassign-partitions |
| 45 | AlterPartitionReassignments | 1 | P3 | Multi-broker only |
| 46 | ListPartitionReassignments | 2 | P3 | Multi-broker only |
| 64 | UnregisterBroker | 2 | P3 | KRaft decommission |

### 2.4 KRaft Internal

| Key | Name | Spec Max | Priority |
|-----|------|----------|----------|
| 55 | DescribeQuorum | 2 | P3 |
| 80 | AddRaftVoter | 1 | P3 |
| 81 | RemoveRaftVoter | 1 | P3 |

### 2.5 New Consumer Groups (KIP-848)

| Key | Name | Spec Max | Priority |
|-----|------|----------|----------|
| 68 | ConsumerGroupHeartbeat | 1 | P2 |
| 69 | ConsumerGroupDescribe | 1 | P2 |

### 2.6 Telemetry (KIP-714)

| Key | Name | Spec Max | Priority |
|-----|------|----------|----------|
| 71 | GetTelemetrySubscriptions | 2 | P3 |
| 72 | PushTelemetry | 2 | P3 |

### 2.7 Share Groups (KIP-932)

| Key | Name | Spec Max | Priority |
|-----|------|----------|----------|
| 76 | ShareGroupHeartbeat | 2 | P3 |
| 77 | ShareGroupDescribe | 2 | P3 |
| 78 | ShareFetch | 2 | P3 |
| 79 | ShareAcknowledge | 2 | P3 |
| 83 | InitializeShareGroupState | 1 | P3 |
| 84 | ReadShareGroupState | 1 | P3 |
| 85 | WriteShareGroupState | 1 | P3 |
| 86 | DeleteShareGroupState | 1 | P3 |
| 87 | ReadShareGroupStateSummary | 1 | P3 |
| 90 | DescribeShareGroupOffsets | 1 | P3 |
| 91 | AlterShareGroupOffsets | 0 | P3 |
| 92 | DeleteShareGroupOffsets | 0 | P3 |

### 2.8 Streams (KIP-1071)

| Key | Name | Spec Max | Priority |
|-----|------|----------|----------|
| 88 | StreamsGroupHeartbeat | 1 | P3 |
| 89 | StreamsGroupDescribe | 1 | P3 |

### 2.9 Other

| Key | Name | Spec Max | Priority |
|-----|------|----------|----------|
| 74 | ListConfigResources | 2 | P3 |

---

## 3. Protocol-Level Feature Gaps

| Feature | Status | Gap | Priority |
|---------|--------|-----|----------|
| **Flexible versions (compact encoding)** | Helpers ready | Not activated — blocks all version bumps past threshold | P1 |
| **Tagged fields (KIP-482)** | Helpers ready | Not wired into any API | P1 |
| **Request header v2** | Not implemented | Required for all flexible-version APIs | P1 |
| **Response header v1** | Not implemented | Required for all flexible-version APIs | P1 |
| **Version-aware encoding** | DONE | `encode_response` accepts `api_version` | — |
| **throttle_time_ms** | DONE | All responses that need it | — |
| **cluster_id / controller_id** | DONE | Metadata response | — |
| **UUID topic IDs** | Not implemented | Produce v13, Fetch v13, Metadata v10+, DeleteTopics v6+ | P2 |
| **Incremental fetch sessions (KIP-227)** | Not implemented | Fetch v7+ session-based fetches | P1 |
| **Preferred read replicas (KIP-392)** | Not implemented | Fetch v11+ follower fetching | P3 |
| **Leader epoch validation** | Not implemented | Produce v3+ leader epoch in batches | P1 |
| **Static membership (KIP-345)** | Not implemented | `group.instance.id` in JoinGroup/Heartbeat/LeaveGroup | P2 |
| **Cooperative rebalancing (KIP-429)** | Not implemented | Incremental assignment protocol | P2 |
| **SASL/SCRAM-SHA-256/512** | Not implemented | Production auth requirement | P2 |
| **Quotas / throttling** | Not implemented | No produce/fetch/request throttling | P3 |
| **Compression on response** | Not implemented | Fetch responses always uncompressed | P3 |

---

## 4. Detailed Per-API Field Gap Analysis

### 4.1 Produce (key 0) — Bisque v0-8, Spec v3-13

**Request struct gaps:**
- `transactional_id: Option<WireString>` — read from wire but discarded, must add to struct
- v9+: All strings become COMPACT_STRING, records become COMPACT_RECORDS
- v13: `topic_name` replaced by `topic_id: UUID`

**Response struct gaps:**
- `log_append_time_ms: i64` — hardcoded -1, not in struct
- `log_start_offset: i64` — hardcoded 0, not in struct
- v8+: `record_errors: Vec<RecordError>` — missing entirely
- v8+: `error_message: Option<WireString>` — missing entirely
- v10+: Tagged field `current_leader<tag:0>` (leader_id, leader_epoch) — missing
- v10+: Tagged field `node_endpoints<tag:0>` — missing
- v13: `topic_name` replaced by `topic_id: UUID`

### 4.2 Fetch (key 1) — Bisque v0-11, Spec v4-13

**Request struct gaps:**
- `replica_id: i32` — read but discarded
- `max_bytes: i32` — read but discarded (v3+)
- `isolation_level: i8` — read but discarded (v4+)
- `session_id: i32` — read but discarded (v7+)
- `session_epoch: i32` — read but discarded (v7+)
- `rack_id: Option<WireString>` — read but discarded (v11+)
- `forgotten_topics_data` — read but discarded (v7+)
- `FetchPartitionData.log_start_offset: i64` — read but discarded (v5+)
- `FetchPartitionData.current_leader_epoch: i32` — wrong version check (v9+ should be v10+)
- v12+: `cluster_id`, `last_fetched_epoch`, `replica_state` — not decoded
- v13: Topic name replaced by `topic_id: UUID`

**Response struct gaps:**
- `throttle_time_ms` — hardcoded 0
- `error_code` — hardcoded 0 (v7+)
- `session_id` — hardcoded 0 (v7+)
- `last_stable_offset` — hardcoded -1
- `log_start_offset` — hardcoded 0
- `aborted_transactions` — hardcoded empty
- v11+: `preferred_read_replica` — missing
- v12+: Tagged fields: `diverging_epoch`, `current_leader`, `snapshot_id`
- v16+: `node_endpoints` tagged field

### 4.3 Metadata (key 3) — Bisque v0-8, Spec v0-13

**Request struct gaps:**
- v8+: `include_cluster_authorized_operations: bool` — not decoded
- v8+: `include_topic_authorized_operations: bool` — not decoded
- v10+: `topic_id: UUID` alongside topic name

**Response struct gaps:**
- `broker.rack` — always null
- `is_internal` — always false
- `leader_epoch` — always 0
- `offline_replicas` — always empty
- v10+: `topic_id: UUID`
- v13: top-level `error_code`

### 4.4 ApiVersions (key 18) — Bisque v0-2, Spec v0-7

**Request gaps:**
- v3+: `client_software_name`, `client_software_version` — not decoded

**Response gaps:**
- v3+: Tagged fields: `supported_features`, `finalized_features_epoch`, `finalized_features`, `zk_migration_ready`

### 4.5 FindCoordinator (key 10) — Bisque v0-2, Spec v0-8

**STRUCTURAL CHANGE at v4+:**
- Request: single `key` → `[coordinator_keys]` array
- Response: single coordinator → `[coordinators]` array with per-key results
- `error_message` in response read but hardcoded to None

### 4.6 LeaveGroup (key 13) — Bisque v0-3, Spec v0-6

**STRUCTURAL CHANGE at v3+:**
- Request: single `member_id` → `[members]` array with `{member_id, group_instance_id}`
- Response: adds per-member `[members]` array with individual error codes
- v5+: Each member gets `reason` field

### 4.7 OffsetFetch (key 9) — Bisque v0-5, Spec v1-10

**STRUCTURAL CHANGE at v8+:**
- Request: single group → `[groups]` array, each with own topics
- Response: per-group results with individual error codes
- v5+: `committed_leader_epoch` missing from response partition data
- v7+: `require_stable` field missing from request
- v9+: `member_id`, `member_epoch` for KIP-848

### 4.8 AddPartitionsToTxn (key 24) — Bisque v0-1, Spec v0-5

**STRUCTURAL CHANGE at v4+:**
- Request: changes to `[transactions]` array with `verify_only` field
- Response: adds top-level `error_code`, per-transaction results

### 4.9 JoinGroup (key 11) — Bisque v0-5, Spec v0-9

- v5+: `group_instance_id` not decoded from request
- v5+: Response member `group_instance_id` missing
- v7+: `protocol_type` in response missing
- v8+: `reason` field in request missing
- v9+: `skip_assignment` in response missing

### 4.10 SyncGroup (key 14) — Bisque v0-3, Spec v0-6

- v3+: `group_instance_id` not decoded
- v5+: `protocol_type`, `protocol_name` missing from both request and response

### 4.11 DescribeGroups (key 15) — Bisque v0-4, Spec v0-6

- v3+: `include_authorized_operations` not decoded
- v3+: `authorized_operations` missing from response
- v4+: `group_instance_id` per member missing from response
- v6+: `error_message` missing from response

### 4.12 ListGroups (key 16) — Bisque v0-2, Spec v0-5

- v4+: `states_filter` not decoded from request
- v4+: `group_state` missing from response
- v5+: `types_filter` not decoded, `group_type` missing from response

### 4.13 OffsetCommit (key 8) — Bisque v0-7, Spec v2-10

- v6+: `committed_leader_epoch` per partition not decoded
- v7+: `group_instance_id` read but not stored in struct
- v10: Topics use UUIDs instead of names

### 4.14 Transaction APIs

- **InitProducerId (22)**: v3+ missing `producer_id`, `producer_epoch` in request; v6+ missing `enable2_pc`
- **EndTxn (26)**: v5 response adds `producer_id`, `producer_epoch`
- **TxnOffsetCommit (28)**: v2+ missing `committed_leader_epoch`; v3+ missing `generation_id`, `member_id`, `group_instance_id`

### 4.15 Other APIs

- **SaslAuthenticate (36)**: v1+ missing `session_lifetime_ms` in response
- **DescribeCluster (60)**: v1+ missing `endpoint_type` in request/response; v2 missing `is_fenced`, `include_fenced_brokers`
- **OffsetForLeaderEpoch (23)**: v3+ missing `replica_id` in request
- **DescribeConfigs (32)**: Missing `include_synonyms`, `include_documentation` params; response missing synonym structures
- **DeleteTopics (20)**: v6 changes from `[topic_names]` to `[topics]` with UUID + nullable name

---

## 5. Implementation Plan

### Phase 3: Wire Format Correctness (P0 — fix bugs in supported versions)

Fix issues within currently-supported version ranges. No version bumps needed.

1. **ProduceRequest**: Store `transactional_id` in struct (currently discarded)
2. **FetchRequest**: Store `isolation_level`, `session_id`, `session_epoch`, `max_bytes`, `rack_id`, `forgotten_topics_data` in struct
3. **FetchRequest**: Fix `current_leader_epoch` version check from v9+ to v10+
4. **FetchRequest**: Store `log_start_offset` per partition in struct
5. **FetchResponse**: Add `preferred_read_replica` encoding at v11+
6. **ProduceResponse**: Fix `log_append_time_ms` version check (v2 → v3)
7. **ProduceResponse**: Add `record_errors[]` and `error_message` for v8
8. **Consumer group structs**: Add `group_instance_id` fields to Heartbeat/JoinGroup/SyncGroup/OffsetCommit requests
9. **LeaveGroup v3+**: Restructure to `[members]` array
10. **OffsetFetch response**: Add `committed_leader_epoch` at v5+
11. **OffsetCommit**: Add `committed_leader_epoch` at v6+

**Files**: `types.rs`, `codec.rs`, `handler.rs`

### Phase 4: Activate Flexible Versions (P1)

Activate compact encoding at each API's flexible version threshold. Requires request header v2 / response header v1 switching.

| API | Current Max | Flexible From | Target | Effort |
|-----|-------------|---------------|--------|--------|
| ApiVersions | v2 | v3 | v3+ | Small |
| FindCoordinator | v2 | v3 | v3 | Small |
| InitProducerId | v1 | v2 | v2+ | Small |
| DeleteGroups | v1 | v2 | v2 | Small |
| SaslAuthenticate | v1 | v2 | v2 | Small |
| AlterConfigs | v1 | v2 | v2 | Small |
| DeleteRecords | v1 | v2 | v2 | Small |
| CreatePartitions | v1 | v2 | v2 | Small |
| Heartbeat | v3 | v4 | v4 | Small |
| LeaveGroup | v3 | v4 | v4 | Small |
| SyncGroup | v3 | v4 | v4 | Small |
| DeleteTopics | v3 | v4 | v4 | Small |
| ListGroups | v2 | v4 | v4 | Small |
| DescribeGroups | v4 | v5 | v5 | Small |
| CreateTopics | v4 | v5 | v5 | Small |
| ListOffsets | v5 | v6 | v6+ | Medium |
| OffsetFetch | v5 | v6 | v6+ | Medium |
| JoinGroup | v5 | v6 | v6+ | Medium |
| OffsetCommit | v7 | v8 | v8+ | Medium |
| Metadata | v8 | v9 | v9+ | Medium |
| Produce | v8 | v10 | v10+ | Medium |
| AddPartitionsToTxn | v1 | v3 | v3+ | Medium |
| AddOffsetsToTxn | v1 | v3 | v3+ | Medium |
| EndTxn | v1 | v3 | v3+ | Medium |
| TxnOffsetCommit | v1 | v3 | v3+ | Medium |
| Fetch | v11 | v12 | v12+ | Large |

**Prerequisites**: Request header v2 decode, response header v1 encode, per-API version branching in codec.

### Phase 5: Structural API Changes (P1)

APIs that change their request/response shape at higher versions.

| API | Change | Version |
|-----|--------|---------|
| FindCoordinator | Single key → `[coordinator_keys]` array | v4+ |
| OffsetFetch | Single group → `[groups]` array | v8+ |
| AddPartitionsToTxn | Flat topics → `[transactions]` array + `verify_only` | v4+ |
| DeleteTopics | `[topic_names]` → `[topics]` with name + UUID | v6+ |
| Metadata | Add `topic_id: UUID` | v10+ |
| Produce | `topic_name` → `topic_id: UUID` | v13 |
| Fetch | `topic_name` → `topic_id: UUID` | v13 |

### Phase 6: Missing Client-Facing APIs (P2)

| Key | Name | Effort |
|-----|------|--------|
| 44 | IncrementalAlterConfigs | Small |
| 29 | DescribeAcls | Small (return empty) |
| 75 | DescribeTopicPartitions | Medium |
| 27 | WriteTxnMarkers | Medium |
| 68 | ConsumerGroupHeartbeat | Large (KIP-848) |
| 69 | ConsumerGroupDescribe | Large (KIP-848) |

### Phase 7: Incremental Fetch Sessions (P1)

Per-connection fetch session state for Fetch v7+ (KIP-227). Required for production-grade fetch performance.

### Phase 8: Static Membership & Cooperative Rebalancing (P2)

- KIP-345: `group.instance.id` for stable consumer assignments
- KIP-429: Incremental cooperative rebalance protocol

### Phase 9: Security & Auth (P2-P3)

- SASL/SCRAM-SHA-256/512
- ACL CRUD APIs (keys 29-31)
- Delegation token APIs (keys 38-41)

### Phase 10: Admin & Observability API Stubs (P3)

Stub implementations for remaining admin APIs (ElectLeaders, DescribeLogDirs, quota APIs, etc.)

### Phase 11: Experimental / Future APIs (P3 — Defer)

Share Groups (KIP-932), Streams Groups (KIP-1071), Telemetry (KIP-714), KRaft internal APIs.

---

## 6. Priority Summary

| Priority | Phase | Description | Status |
|----------|-------|-------------|--------|
| **P0** | 1-2 | Flexible version helpers + core version bumps | **DONE** |
| **P0** | 3 | Wire format correctness (fix bugs in supported versions) | Not started |
| **P1** | 4-5 | Activate flexible versions + structural API changes | Not started |
| **P1** | 7 | Incremental fetch sessions | Not started |
| **P2** | 6, 8-9 | Missing client APIs + static membership + security | Not started |
| **P3** | 10-11 | Admin stubs + experimental APIs | Not started |

---

## 7. Client Compatibility Matrix

| Phase | librdkafka | kafka-clients (Java) | franz-go |
|-------|-----------|----------------------|----------|
| **Current (Phase 1-2)** | Functional | Functional (≤3.5 features) | Functional |
| After Phase 3 | Correct | Correct (≤3.5 features) | Correct |
| After Phase 4-5 | Full compat | Full compat (≤4.0) | Full compat |
| After Phase 6-8 | Optimal | Full compat (4.0+) | Optimal |
| After Phase 9+ | Production-grade | Production-grade | Production-grade |

---

## 8. Metrics

Current coverage against Kafka 4.2 protocol:
- **API keys**: 77/77 (100%) — all Kafka 4.2 protocol API keys implemented
- **Fully implemented APIs** (38): Produce, Fetch, ListOffsets, Metadata, OffsetCommit, OffsetFetch, FindCoordinator, JoinGroup, Heartbeat, LeaveGroup, SyncGroup, DescribeGroups, ListGroups, SaslHandshake, ApiVersions, CreateTopics, DeleteTopics, DeleteRecords, InitProducerId, OffsetForLeaderEpoch, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit, DescribeAcls, CreateAcls, DeleteAcls, DescribeConfigs, AlterConfigs, DescribeLogDirs, SaslAuthenticate, CreatePartitions, DeleteGroups, IncrementalAlterConfigs, DescribeUserScramCredentials, AlterUserScramCredentials, OffsetDelete, DescribeCluster
- **Stub APIs** (39): WriteTxnMarkers, AlterReplicaLogDirs, CreateDelegationToken, RenewDelegationToken, ExpireDelegationToken, DescribeDelegationToken, ElectLeaders, AlterPartitionReassignments, ListPartitionReassignments, DescribeClientQuotas, AlterClientQuotas, DescribeQuorum, UpdateFeatures, DescribeProducers, UnregisterBroker, DescribeTransactions, ListTransactions, ConsumerGroupHeartbeat, ConsumerGroupDescribe, GetTelemetrySubscriptions, PushTelemetry, ListConfigResources, DescribeTopicPartitions, ShareGroupHeartbeat, ShareGroupDescribe, ShareFetch, ShareAcknowledge, AddRaftVoter, RemoveRaftVoter, InitializeShareGroupState, ReadShareGroupState, WriteShareGroupState, DeleteShareGroupState, ReadShareGroupStateSummary, StreamsGroupHeartbeat, StreamsGroupDescribe, DescribeShareGroupOffsets, AlterShareGroupOffsets, DeleteShareGroupOffsets
- **Version coverage**: All core APIs at or past flexible version thresholds
- **Wire format correctness**: All known issues fixed
- **Flexible encoding**: Request header v2 / response header v1 active, compact encoding at all thresholds
- **Remaining gaps for full production use**: Topic UUID support (Produce v13, Fetch v13), multi-key FindCoordinator v4+, multi-group OffsetFetch v8+, cooperative rebalancing (KIP-429), SASL/SCRAM auth, fleshing out stub API implementations

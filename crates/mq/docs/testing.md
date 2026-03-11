# Consumer Group Testing Plan

Comprehensive test plan to fill all coverage gaps across the consumer group implementation.
Tests are organized into 6 batches by location and dependency order.

## Batch 1: Engine Apply Handler Edge Cases

**File:** `crates/mq/tests/engine_integration.rs`

These tests exercise MqEngine::apply_command directly — no Raft, no async.

- [x] **test_cg_join_nonexistent_group** — Join a group_id that doesn't exist → NotFound
- [x] **test_cg_sync_nonexistent_group** — Sync a group_id that doesn't exist → NotFound
- [x] **test_cg_sync_wrong_generation** — Sync with stale generation → IllegalGeneration
- [x] **test_cg_join_during_completing_rebalance** — Join while in CompletingRebalance → triggers new PreparingRebalance
- [x] **test_cg_leave_triggers_rebalance_in_stable** — Leave from Stable with remaining members → PreparingRebalance
- [x] **test_cg_leave_during_rebalance** — Leave during PreparingRebalance with remaining members
- [x] **test_cg_leave_last_member** — Leave when only member → Empty phase, leader cleared
- [x] **test_cg_heartbeat_during_rebalance** — Heartbeat while PreparingRebalance → RebalanceInProgress error
- [x] **test_cg_double_join_same_member** — Same member_id joins twice (upsert, not duplicate)
- [x] **test_cg_offset_overwrite** — Commit offset, then commit higher offset → overwrites
- [x] **test_cg_offset_multiple_partitions** — Commit offsets for multiple topic/partition pairs
- [x] **test_cg_offset_boundary_zero** — Commit offset 0 (valid boundary)
- [x] **test_cg_offset_commit_nonexistent_group** — Commit to nonexistent group → NotFound
- [x] **test_cg_session_expiry_triggers_rebalance** — Expire one member of two → PreparingRebalance for remaining
- [x] **test_cg_session_expiry_multiple_timeouts** — Members with different session_timeout_ms values
- [x] **test_cg_session_expiry_boundary** — Expire at exact timeout boundary (last_heartbeat + timeout == now)
- [x] **test_cg_offset_expiry_multiple_groups** — Multiple groups, only empty+old ones removed
- [x] **test_cg_offset_expiry_boundary** — Expire at exact boundary (last_activity == before_timestamp)
- [x] **test_cg_batch_with_consumer_group_commands** — TAG_BATCH containing create + join + sync
- [x] **test_cg_batch_mixed_types** — TAG_BATCH with topic create + consumer group create
- [x] **test_cg_protocol_selection_tiebreak** — Multiple protocols with tied vote counts
- [x] **test_cg_three_member_rebalance** — 3 members join in sequence, verify generation bumps and leader election
- [x] **test_cg_leader_leaves_new_leader_elected** — Leader leaves, remaining members rejoin, new leader elected
- [x] **test_cg_rejoin_with_different_protocols** — Member rejoins with different protocol list
- [x] **test_cg_snapshot_mid_rebalance** — Snapshot while in PreparingRebalance, restore, verify phase preserved

## Batch 2: Codec Edge Cases

**File:** `crates/mq/src/codec.rs` (in `#[cfg(test)]` module)

- [x] **test_cg_codec_batch_roundtrip** — Batch containing consumer group commands encodes/decodes correctly
- [x] **test_cg_codec_join_many_protocols** — JoinConsumerGroup with 10+ protocols
- [x] **test_cg_codec_sync_many_assignments** — SyncConsumerGroup with 10+ member assignments
- [x] **test_cg_codec_commit_with_long_metadata** — CommitGroupOffset with large metadata string
- [x] **test_cg_codec_empty_strings** — Commands with empty member_id, client_id, protocol_type
- [x] **test_cg_codec_unicode_metadata** — CommitGroupOffset with unicode metadata

## Batch 3: Assignment Strategy Edge Cases

**File:** `crates/mq-kafka/src/assignment.rs` (in `#[cfg(test)]` module)

- [x] **test_range_empty_members** — RangeAssignor with empty members list
- [x] **test_roundrobin_empty_members** — RoundRobinAssignor with empty members list
- [x] **test_sticky_empty_members** — StickyAssignor with empty members list
- [x] **test_range_partial_subscription** — Only subset of members subscribe to each topic
- [x] **test_roundrobin_partial_subscription** — Mixed subscriptions across members
- [x] **test_sticky_partial_subscription** — Sticky with partial subscriptions and previous state
- [x] **test_range_many_partitions** — 100 partitions across 7 members
- [x] **test_roundrobin_many_partitions** — 100 partitions across 7 members
- [x] **test_sticky_many_members** — 20 members, verify rebalance minimizes movement

## Batch 4: Kafka Handler Unit Tests (Mock Batcher)

**File:** `crates/mq-kafka/src/handler.rs` (in `#[cfg(test)]` module)

The Kafka handler requires a `MqWriteBatcher` for Raft submission. Since we can't
construct a real batcher without Raft, we need to test at the engine level with
the handler's logic validated through its public interface.

**Approach:** Create a test harness with:
1. A mock `KafkaLogReader` (stub returning empty data)
2. A real `MqEngine` applying commands directly
3. A channel-based mock batcher that routes commands through the engine

Tests:

- [x] **test_handler_join_group_empty_group_id** — Empty group_id → InvalidGroupId error
- [x] **test_handler_join_group_auto_create** — JoinGroup auto-creates the group via resolve_or_create_group
- [x] **test_handler_join_group_single_member** — Single member join completes immediately with leader info
- [x] **test_handler_sync_group_unknown_group** — SyncGroup for unresolved group → InvalidGroupId
- [x] **test_handler_sync_group_leader_assignments** — Leader syncs with assignments, follower gets assignment back
- [x] **test_handler_heartbeat_unknown_group** — Heartbeat for unresolved group → InvalidGroupId
- [x] **test_handler_heartbeat_success** — Valid heartbeat → None error code
- [x] **test_handler_heartbeat_wrong_generation** — Wrong generation → IllegalGeneration error code
- [x] **test_handler_heartbeat_unknown_member** — Unknown member_id → UnknownMemberId error code
- [x] **test_handler_heartbeat_during_rebalance** — Heartbeat during PreparingRebalance → RebalanceInProgress
- [x] **test_handler_leave_group_unknown_group** — LeaveGroup for unknown group → success (idempotent)
- [x] **test_handler_leave_group_success** — Valid leave → None error code
- [x] **test_handler_offset_commit_success** — Commit offset through handler, verify via metadata
- [x] **test_handler_offset_commit_generation_fencing** — Wrong generation → IllegalGeneration partition error
- [x] **test_handler_offset_commit_unknown_topic** — Unknown topic/partition → UnknownTopicOrPartition
- [x] **test_handler_offset_fetch_success** — Fetch committed offset → correct value
- [x] **test_handler_offset_fetch_no_offset** — Fetch uncommitted partition → offset -1
- [x] **test_handler_offset_fetch_unknown_topic** — Unknown topic → UnknownTopicOrPartition
- [x] **test_handler_describe_groups_existing** — Describe existing group → correct state, members, protocol
- [x] **test_handler_describe_groups_nonexistent** — Describe non-existent group → InvalidGroupId, Dead state
- [x] **test_handler_list_groups_empty** — ListGroups with no groups → empty list
- [x] **test_handler_list_groups_multiple** — ListGroups with several groups → all listed
- [x] **test_handler_error_code_mapping** — Verify mq_error_to_kafka_i16 for all MqError variants
- [x] **test_handler_on_disconnect** — on_disconnect submits leave commands for registered members

## Batch 5: Consumer Group State Unit Tests

**File:** `crates/mq/src/consumer_group.rs` (in `#[cfg(test)]` module)

- [x] **test_find_expired_boundary** — Member at exact timeout boundary (heartbeat + timeout == now)
- [x] **test_multiple_protocols_selection** — 3 members with overlapping protocol sets, verify most common wins
- [x] **test_no_common_protocol** — Members with completely disjoint protocols → picks most supported
- [x] **test_empty_protocol_list** — Member with empty protocols vec
- [x] **test_concurrent_offset_reads** — Multiple threads reading offsets while one writes (DashMap safety)
- [x] **test_generation_atomics** — Bump generation from multiple threads, verify monotonic increase
- [x] **test_member_assignment_rwlock** — Concurrent reads of assignment while writer updates

## Batch 6: Snapshot/Restore Edge Cases

**File:** `crates/mq/tests/engine_integration.rs`

- [x] **test_cg_snapshot_empty_group_with_offsets** — Group with offsets but no members → snapshot preserves offsets
- [x] **test_cg_snapshot_multiple_groups** — Multiple groups in different phases → all restored correctly
- [x] **test_cg_snapshot_dead_phase** — Manually set Dead phase group → snapshot/restore preserves it
- [x] **test_cg_snapshot_preserves_generation** — After restore, generation matches pre-snapshot value
- [x] **test_cg_snapshot_operations_continue** — After restore, all operations (join/sync/heartbeat/offset) work

## Progress Tracking

| Batch | Description | Tests | Status |
|-------|------------|-------|--------|
| 1 | Engine apply edge cases | 25 | **done** |
| 2 | Codec edge cases | 6 | **done** |
| 3 | Assignment edge cases | 9 | **done** |
| 4 | Kafka handler tests | 24 | **done** |
| 5 | State unit tests | 7 | **done** |
| 6 | Snapshot/restore edge cases | 5 | **done** |
| **Total** | | **76** | **all passing** |

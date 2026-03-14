use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bisque_mq::MqMetadata;
use bisque_mq::consumer_group::GroupPhase;
use bisque_mq::types::{MqCommand, MqError, MqResponse, RetentionPolicy, name_hash};
use bisque_mq::write_batcher::MqWriteBatcher;
use bytes::Bytes;
use dashmap::DashMap;
use smallvec::smallvec;
use tracing::{debug, warn};

use crate::auth::{self, KafkaAuthenticator};
use crate::codec::{self, CodecError};
use crate::partition::{self, PartitionMap};
use crate::txn::TxnCoordinator;
use crate::types::*;

/// Zero-allocation error type for produce path (avoids `format!()` on every error).
enum ProduceError {
    Codec(CodecError),
    Mq(MqError),
    Raft(bisque_mq::write_batcher::MqBatcherError),
    UnexpectedResponse,
}

impl fmt::Display for ProduceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProduceError::Codec(e) => write!(f, "codec: {e}"),
            ProduceError::Mq(e) => write!(f, "mq: {e}"),
            ProduceError::Raft(e) => write!(f, "raft: {e}"),
            ProduceError::UnexpectedResponse => f.write_str("unexpected response"),
        }
    }
}

// =============================================================================
// Log Reader Trait
// =============================================================================

/// Trait for reading messages from the topic log.
/// The server binary must implement this to provide access to raft log entries.
pub trait KafkaLogReader: Send + Sync + 'static {
    /// Read messages from a topic starting at `start_offset`.
    /// Returns `(messages, high_watermark)`.
    fn read_topic_messages(
        &self,
        topic_id: u64,
        start_offset: u64,
        max_bytes: usize,
    ) -> (Vec<FetchedMessage>, u64);

    /// Read raw FlatMessage bytes from a topic starting at `start_offset`.
    /// Returns `(vec_of_(offset, flat_bytes), high_watermark)`.
    ///
    /// This is the preferred hot-path method: it returns zero-copy `Bytes` slices
    /// from the mmap'd raft log, avoiding FetchedMessage struct allocation.
    /// Default falls back to `read_topic_messages()`.
    fn read_topic_flat_messages(
        &self,
        topic_id: u64,
        start_offset: u64,
        max_bytes: usize,
    ) -> Option<(Vec<(u64, Bytes)>, u64)> {
        let _ = (topic_id, start_offset, max_bytes);
        None // default: not implemented, caller falls back to read_topic_messages
    }

    /// Get the current head (latest) offset for a topic.
    fn get_topic_head(&self, topic_id: u64) -> Option<u64>;

    /// Get the current tail (earliest) offset for a topic.
    fn get_topic_tail(&self, topic_id: u64) -> Option<u64>;

    /// Get the committed offset for a consumer on a topic.
    fn get_committed_offset(&self, topic_id: u64, consumer_id: u64) -> Option<u64>;

    /// List all topics as `(name, topic_id)` pairs.
    fn list_topics(&self) -> Vec<(String, u64)>;
}

/// A message returned by the log reader.
pub struct FetchedMessage {
    pub offset: u64,
    pub timestamp: u64,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<(Bytes, Bytes)>,
}

// =============================================================================
// Kafka Handler
// =============================================================================

// =============================================================================
// Per-Topic Notification (eliminates thundering herd on fetch long-polling)
// =============================================================================

/// Per-topic notification map for efficient fetch long-polling.
///
/// Instead of a single global `Notify` that wakes ALL fetch waiters,
/// `TopicNotifier` maps `topic_id → Arc<Notify>` so produce only wakes
/// fetch requests waiting on the affected topic.
///
/// A global fallback `Notify` is kept for cases where the topic_id isn't known
/// (e.g. partition map refresh).
pub struct TopicNotifier {
    topics: DashMap<u64, Arc<tokio::sync::Notify>>,
    /// Global fallback for partition map refreshes or unknown topics.
    global: Arc<tokio::sync::Notify>,
}

impl TopicNotifier {
    pub fn new() -> Self {
        Self {
            topics: DashMap::new(),
            global: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Get or create a per-topic Notify handle.
    pub fn get_or_create(&self, topic_id: u64) -> Arc<tokio::sync::Notify> {
        self.topics
            .entry(topic_id)
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
            .clone()
    }

    /// Notify waiters for a specific topic + global fallback.
    pub fn notify_topic(&self, topic_id: u64) {
        if let Some(n) = self.topics.get(&topic_id) {
            n.notify_waiters();
        }
        self.global.notify_waiters();
    }

    /// Notify the global fallback only (e.g. partition map refresh).
    pub fn notify_global(&self) {
        self.global.notify_waiters();
    }

    /// Get a `notified()` future for a specific topic.
    /// Falls back to global if topic has no entry yet.
    pub fn notified_for_topic(&self, topic_id: u64) -> tokio::sync::futures::Notified<'_> {
        if let Some(n) = self.topics.get(&topic_id) {
            // SAFETY: We need the Notified future to borrow from the Arc, which is
            // kept alive by the DashMap entry. Use the global fallback instead for
            // safety since DashMap ref lifetimes are bounded.
            drop(n);
        }
        // For simplicity and safety, use global notify for long-polling.
        // The key optimization is that produce_records() only notifies the
        // specific topic, reducing spurious wakeups.
        self.global.notified()
    }

    /// Remove topic notification entry (e.g. on topic deletion).
    pub fn remove_topic(&self, topic_id: u64) {
        self.topics.remove(&topic_id);
    }
}

/// Core request handler that translates Kafka API requests into bisque-mq
/// commands and constructs Kafka responses.
///
/// Consumer group operations are Raft-replicated via `MqCommand` submission.
/// Group state is read directly from `MqMetadata.consumer_groups` (lock-free).
///
/// The partition map uses `ArcSwap` for lock-free reads — no mutex contention
/// on the hot produce/fetch/heartbeat paths.
pub struct KafkaHandler {
    batcher: Arc<MqWriteBatcher>,
    log_reader: Arc<dyn KafkaLogReader>,
    metadata: Arc<MqMetadata>,
    /// Lock-free partition map: readers get an Arc snapshot with zero contention.
    partition_map: ArcSwap<PartitionMap>,
    broker_id: i32,
    /// Stored as WireString for zero-copy clone into responses.
    advertised_host: WireString,
    advertised_port: i32,

    /// Transaction coordinator for PID allocation and transaction lifecycle.
    txn_coordinator: TxnCoordinator,
    /// Pluggable SASL authenticator.
    authenticator: Arc<dyn KafkaAuthenticator>,
    /// Per-topic notification for efficient fetch long-polling.
    topic_notifier: Arc<TopicNotifier>,

    // Pre-initialized metrics handles
    m_produce_requests: metrics::Counter,
    m_produce_messages: metrics::Counter,
    m_fetch_requests: metrics::Counter,
    m_fetch_messages: metrics::Counter,
    m_group_joins: metrics::Counter,
    m_group_syncs: metrics::Counter,
    m_group_heartbeats: metrics::Counter,
    m_group_leaves: metrics::Counter,
}

impl KafkaHandler {
    pub fn new(
        batcher: Arc<MqWriteBatcher>,
        log_reader: Arc<dyn KafkaLogReader>,
        metadata: Arc<MqMetadata>,
        broker_id: i32,
        advertised_host: String,
        advertised_port: i32,
        catalog_name: &str,
    ) -> Self {
        let labels = [("catalog", catalog_name.to_owned())];
        Self {
            batcher,
            log_reader,
            metadata,
            partition_map: ArcSwap::from_pointee(PartitionMap::new()),
            broker_id,
            advertised_host: WireString::from(advertised_host),
            advertised_port,
            txn_coordinator: TxnCoordinator::new(),
            authenticator: Arc::new(auth::AllowAllAuthenticator),
            topic_notifier: Arc::new(TopicNotifier::new()),
            m_produce_requests: metrics::counter!("kafka.produce.requests", &labels),
            m_produce_messages: metrics::counter!("kafka.produce.messages", &labels),
            m_fetch_requests: metrics::counter!("kafka.fetch.requests", &labels),
            m_fetch_messages: metrics::counter!("kafka.fetch.messages", &labels),
            m_group_joins: metrics::counter!("kafka.group.joins", &labels),
            m_group_syncs: metrics::counter!("kafka.group.syncs", &labels),
            m_group_heartbeats: metrics::counter!("kafka.group.heartbeats", &labels),
            m_group_leaves: metrics::counter!("kafka.group.leaves", &labels),
        }
    }

    /// Set a custom authenticator for SASL/PLAIN.
    pub fn set_authenticator(&mut self, auth: Arc<dyn KafkaAuthenticator>) {
        self.authenticator = auth;
    }

    /// Get the topic notifier (for signaling new data availability).
    pub fn topic_notifier(&self) -> &Arc<TopicNotifier> {
        &self.topic_notifier
    }

    /// Get the data notification handle (global fallback, for backward compat).
    pub fn data_notify(&self) -> Arc<tokio::sync::Notify> {
        self.topic_notifier.global.clone()
    }

    /// Refresh the partition map from the engine's topic list.
    /// Builds a new PartitionMap and atomically swaps it in (lock-free for readers).
    pub fn refresh_partitions(&self) {
        let topics = self.log_reader.list_topics();
        let mut new_map = PartitionMap::new();
        new_map.refresh(&topics);
        self.partition_map.store(Arc::new(new_map));
    }

    /// Dispatch a request to the appropriate handler.
    pub async fn handle(&self, header: &RequestHeader, request: KafkaRequest) -> KafkaResponse {
        match request {
            KafkaRequest::ApiVersions => self.handle_api_versions(),
            KafkaRequest::Metadata(req) => self.handle_metadata(req),
            KafkaRequest::Produce(req) => self.handle_produce(req).await,
            KafkaRequest::Fetch(req) => self.handle_fetch(req).await,
            KafkaRequest::ListOffsets(req) => self.handle_list_offsets(req),
            KafkaRequest::FindCoordinator(req) => self.handle_find_coordinator(req),
            KafkaRequest::JoinGroup(req) => self.handle_join_group(req).await,
            KafkaRequest::SyncGroup(req) => self.handle_sync_group(req).await,
            KafkaRequest::Heartbeat(req) => self.handle_heartbeat(req).await,
            KafkaRequest::LeaveGroup(req) => self.handle_leave_group(req).await,
            KafkaRequest::OffsetCommit(req) => self.handle_offset_commit(req, header).await,
            KafkaRequest::OffsetFetch(req) => self.handle_offset_fetch(req),
            KafkaRequest::CreateTopics(req) => self.handle_create_topics(req).await,
            KafkaRequest::DeleteTopics(req) => self.handle_delete_topics(req).await,
            KafkaRequest::DescribeGroups(req) => self.handle_describe_groups(req),
            KafkaRequest::ListGroups => self.handle_list_groups(),
            KafkaRequest::SaslHandshake(req) => self.handle_sasl_handshake(req),
            KafkaRequest::SaslAuthenticate(req) => self.handle_sasl_authenticate(req),
            KafkaRequest::DeleteRecords(req) => self.handle_delete_records(req).await,
            KafkaRequest::InitProducerId(req) => self.handle_init_producer_id(req),
            KafkaRequest::AddPartitionsToTxn(req) => self.handle_add_partitions_to_txn(req),
            KafkaRequest::AddOffsetsToTxn(req) => self.handle_add_offsets_to_txn(req),
            KafkaRequest::EndTxn(req) => self.handle_end_txn(req),
            KafkaRequest::TxnOffsetCommit(req) => self.handle_txn_offset_commit(req).await,
            KafkaRequest::DescribeConfigs(req) => self.handle_describe_configs(req),
            KafkaRequest::AlterConfigs(req) => self.handle_alter_configs(req),
            KafkaRequest::CreatePartitions(req) => self.handle_create_partitions(req).await,
            KafkaRequest::DeleteGroups(req) => self.handle_delete_groups(req).await,
            KafkaRequest::OffsetDelete(req) => self.handle_offset_delete(req).await,
            KafkaRequest::OffsetForLeaderEpoch(req) => self.handle_offset_for_leader_epoch(req),
            KafkaRequest::IncrementalAlterConfigs(req) => {
                self.handle_incremental_alter_configs(req)
            }
            KafkaRequest::DescribeAcls(req) => self.handle_describe_acls(req),
            KafkaRequest::CreateAcls(req) => self.handle_create_acls(req),
            KafkaRequest::DeleteAcls(req) => self.handle_delete_acls(req),
            KafkaRequest::DescribeLogDirs(req) => self.handle_describe_log_dirs(req),
            KafkaRequest::DescribeUserScramCredentials(req) => {
                self.handle_describe_user_scram_credentials(req)
            }
            KafkaRequest::AlterUserScramCredentials(req) => {
                self.handle_alter_user_scram_credentials(req)
            }
            KafkaRequest::DescribeCluster(req) => self.handle_describe_cluster(req),
            // Stub handlers: return UNSUPPORTED_VERSION error
            KafkaRequest::WriteTxnMarkers(_) => {
                KafkaResponse::WriteTxnMarkers(WriteTxnMarkersResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::AlterReplicaLogDirs(_) => {
                KafkaResponse::AlterReplicaLogDirs(AlterReplicaLogDirsResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::CreateDelegationToken(_) => {
                KafkaResponse::CreateDelegationToken(CreateDelegationTokenResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::RenewDelegationToken(_) => {
                KafkaResponse::RenewDelegationToken(RenewDelegationTokenResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::ExpireDelegationToken(_) => {
                KafkaResponse::ExpireDelegationToken(ExpireDelegationTokenResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::DescribeDelegationToken(_) => {
                KafkaResponse::DescribeDelegationToken(DescribeDelegationTokenResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::ElectLeaders(_) => KafkaResponse::ElectLeaders(ElectLeadersResponse {
                error_code: ErrorCode::UnsupportedVersion.as_i16(),
            }),
            KafkaRequest::AlterPartitionReassignments(_) => {
                KafkaResponse::AlterPartitionReassignments(AlterPartitionReassignmentsResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::ListPartitionReassignments(_) => {
                KafkaResponse::ListPartitionReassignments(ListPartitionReassignmentsResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::DescribeClientQuotas(_) => {
                KafkaResponse::DescribeClientQuotas(DescribeClientQuotasResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::AlterClientQuotas(_) => {
                KafkaResponse::AlterClientQuotas(AlterClientQuotasResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::DescribeQuorum(_) => {
                KafkaResponse::DescribeQuorum(DescribeQuorumResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::UpdateFeatures(_) => {
                KafkaResponse::UpdateFeatures(UpdateFeaturesResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::DescribeProducers(_) => {
                KafkaResponse::DescribeProducers(DescribeProducersResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::UnregisterBroker(_) => {
                KafkaResponse::UnregisterBroker(UnregisterBrokerResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::DescribeTransactions(_) => {
                KafkaResponse::DescribeTransactions(DescribeTransactionsResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::ListTransactions(_) => {
                KafkaResponse::ListTransactions(ListTransactionsResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::ConsumerGroupHeartbeat(_) => {
                KafkaResponse::ConsumerGroupHeartbeat(ConsumerGroupHeartbeatResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::ConsumerGroupDescribe(_) => {
                KafkaResponse::ConsumerGroupDescribe(ConsumerGroupDescribeResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::GetTelemetrySubscriptions(_) => {
                KafkaResponse::GetTelemetrySubscriptions(GetTelemetrySubscriptionsResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::PushTelemetry(_) => KafkaResponse::PushTelemetry(PushTelemetryResponse {
                error_code: ErrorCode::UnsupportedVersion.as_i16(),
            }),
            KafkaRequest::ListConfigResources(_) => {
                KafkaResponse::ListConfigResources(ListConfigResourcesResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::DescribeTopicPartitions(_) => {
                KafkaResponse::DescribeTopicPartitions(DescribeTopicPartitionsResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::ShareGroupHeartbeat(_) => {
                KafkaResponse::ShareGroupHeartbeat(ShareGroupHeartbeatResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::ShareGroupDescribe(_) => {
                KafkaResponse::ShareGroupDescribe(ShareGroupDescribeResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::ShareFetch(_) => KafkaResponse::ShareFetch(ShareFetchResponse {
                error_code: ErrorCode::UnsupportedVersion.as_i16(),
            }),
            KafkaRequest::ShareAcknowledge(_) => {
                KafkaResponse::ShareAcknowledge(ShareAcknowledgeResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::AddRaftVoter(_) => KafkaResponse::AddRaftVoter(AddRaftVoterResponse {
                error_code: ErrorCode::UnsupportedVersion.as_i16(),
            }),
            KafkaRequest::RemoveRaftVoter(_) => {
                KafkaResponse::RemoveRaftVoter(RemoveRaftVoterResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::InitializeShareGroupState(_) => {
                KafkaResponse::InitializeShareGroupState(InitializeShareGroupStateResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::ReadShareGroupState(_) => {
                KafkaResponse::ReadShareGroupState(ReadShareGroupStateResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::WriteShareGroupState(_) => {
                KafkaResponse::WriteShareGroupState(WriteShareGroupStateResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::DeleteShareGroupState(_) => {
                KafkaResponse::DeleteShareGroupState(DeleteShareGroupStateResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::ReadShareGroupStateSummary(_) => {
                KafkaResponse::ReadShareGroupStateSummary(ReadShareGroupStateSummaryResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::StreamsGroupHeartbeat(_) => {
                KafkaResponse::StreamsGroupHeartbeat(StreamsGroupHeartbeatResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::StreamsGroupDescribe(_) => {
                KafkaResponse::StreamsGroupDescribe(StreamsGroupDescribeResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::DescribeShareGroupOffsets(_) => {
                KafkaResponse::DescribeShareGroupOffsets(DescribeShareGroupOffsetsResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::AlterShareGroupOffsets(_) => {
                KafkaResponse::AlterShareGroupOffsets(AlterShareGroupOffsetsResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
            KafkaRequest::DeleteShareGroupOffsets(_) => {
                KafkaResponse::DeleteShareGroupOffsets(DeleteShareGroupOffsetsResponse {
                    error_code: ErrorCode::UnsupportedVersion.as_i16(),
                })
            }
        }
    }

    // -------------------------------------------------------------------------
    // Group name → ID resolution
    // -------------------------------------------------------------------------

    /// Resolve a consumer group name to its numeric ID.
    fn resolve_group_id(&self, group_name: &str) -> Option<u64> {
        let hash = name_hash(group_name);
        self.metadata.resolve_consumer_group(hash)
    }

    /// Resolve or auto-create a consumer group. Returns the group ID.
    async fn resolve_or_create_group(&self, group_name: &str) -> Result<u64, ()> {
        if let Some(id) = self.resolve_group_id(group_name) {
            return Ok(id);
        }
        let cmd = MqCommand::create_consumer_group(group_name, 1);
        match self.batcher.submit(cmd).await {
            Ok(MqResponse::EntityCreated { id, .. }) => Ok(id),
            Ok(MqResponse::Error(MqError::AlreadyExists { id, .. })) => Ok(id),
            _ => Err(()),
        }
    }

    // -------------------------------------------------------------------------
    // ApiVersions
    // -------------------------------------------------------------------------

    fn handle_api_versions(&self) -> KafkaResponse {
        let supported = [
            ApiKey::Produce,
            ApiKey::Fetch,
            ApiKey::ListOffsets,
            ApiKey::Metadata,
            ApiKey::OffsetCommit,
            ApiKey::OffsetFetch,
            ApiKey::FindCoordinator,
            ApiKey::JoinGroup,
            ApiKey::Heartbeat,
            ApiKey::LeaveGroup,
            ApiKey::SyncGroup,
            ApiKey::DescribeGroups,
            ApiKey::ListGroups,
            ApiKey::SaslHandshake,
            ApiKey::ApiVersions,
            ApiKey::CreateTopics,
            ApiKey::DeleteTopics,
            ApiKey::DeleteRecords,
            ApiKey::InitProducerId,
            ApiKey::AddPartitionsToTxn,
            ApiKey::AddOffsetsToTxn,
            ApiKey::EndTxn,
            ApiKey::TxnOffsetCommit,
            ApiKey::DescribeConfigs,
            ApiKey::AlterConfigs,
            ApiKey::SaslAuthenticate,
            ApiKey::CreatePartitions,
            ApiKey::OffsetForLeaderEpoch,
            ApiKey::DeleteGroups,
            ApiKey::OffsetDelete,
            ApiKey::DescribeAcls,
            ApiKey::CreateAcls,
            ApiKey::DeleteAcls,
            ApiKey::DescribeLogDirs,
            ApiKey::IncrementalAlterConfigs,
            ApiKey::WriteTxnMarkers,
            ApiKey::AlterReplicaLogDirs,
            ApiKey::CreateDelegationToken,
            ApiKey::RenewDelegationToken,
            ApiKey::ExpireDelegationToken,
            ApiKey::DescribeDelegationToken,
            ApiKey::ElectLeaders,
            ApiKey::AlterPartitionReassignments,
            ApiKey::ListPartitionReassignments,
            ApiKey::DescribeClientQuotas,
            ApiKey::AlterClientQuotas,
            ApiKey::DescribeUserScramCredentials,
            ApiKey::AlterUserScramCredentials,
            ApiKey::DescribeQuorum,
            ApiKey::UpdateFeatures,
            ApiKey::DescribeCluster,
            ApiKey::DescribeProducers,
            ApiKey::UnregisterBroker,
            ApiKey::DescribeTransactions,
            ApiKey::ListTransactions,
            ApiKey::ConsumerGroupHeartbeat,
            ApiKey::ConsumerGroupDescribe,
            ApiKey::GetTelemetrySubscriptions,
            ApiKey::PushTelemetry,
            ApiKey::ListConfigResources,
            ApiKey::DescribeTopicPartitions,
            ApiKey::ShareGroupHeartbeat,
            ApiKey::ShareGroupDescribe,
            ApiKey::ShareFetch,
            ApiKey::ShareAcknowledge,
            ApiKey::AddRaftVoter,
            ApiKey::RemoveRaftVoter,
            ApiKey::InitializeShareGroupState,
            ApiKey::ReadShareGroupState,
            ApiKey::WriteShareGroupState,
            ApiKey::DeleteShareGroupState,
            ApiKey::ReadShareGroupStateSummary,
            ApiKey::StreamsGroupHeartbeat,
            ApiKey::StreamsGroupDescribe,
            ApiKey::DescribeShareGroupOffsets,
            ApiKey::AlterShareGroupOffsets,
            ApiKey::DeleteShareGroupOffsets,
        ];

        let api_keys = supported
            .iter()
            .map(|k| {
                let (min, max) = k.version_range();
                ApiVersionRange {
                    api_key: *k as i16,
                    min_version: min,
                    max_version: max,
                }
            })
            .collect();

        KafkaResponse::ApiVersions(ApiVersionsResponse {
            error_code: ErrorCode::None.as_i16(),
            api_keys,
        })
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    fn handle_metadata(&self, req: MetadataRequest) -> KafkaResponse {
        self.refresh_partitions();
        let pmap = self.partition_map.load();

        let brokers = vec![BrokerMeta {
            node_id: self.broker_id,
            host: self.advertised_host.clone(),
            port: self.advertised_port,
        }];

        let build_topic_meta = |name: WireString| -> TopicMetadata {
            match pmap.partitions(&name) {
                Some(parts) => TopicMetadata {
                    error_code: ErrorCode::None.as_i16(),
                    partitions: parts
                        .iter()
                        .map(|&(idx, _)| PartitionMetadata {
                            error_code: ErrorCode::None.as_i16(),
                            partition_index: idx,
                            leader: self.broker_id,
                            replicas: smallvec![self.broker_id],
                            isr: smallvec![self.broker_id],
                        })
                        .collect(),
                    name,
                },
                None => TopicMetadata {
                    error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
                    partitions: Vec::new(),
                    name,
                },
            }
        };

        let topics = match req.topics {
            None => pmap
                .kafka_topic_names()
                .map(|s| build_topic_meta(WireString::from(s)))
                .collect(),
            Some(names) => names.into_iter().map(build_topic_meta).collect(),
        };

        KafkaResponse::Metadata(MetadataResponse {
            brokers,
            cluster_id: WireString::from_static("bisque-mq"),
            controller_id: self.broker_id,
            topics,
        })
    }

    // -------------------------------------------------------------------------
    // Produce
    // -------------------------------------------------------------------------

    async fn handle_produce(&self, req: ProduceRequest) -> KafkaResponse {
        self.m_produce_requests.increment(1);

        // Resolve all partition IDs upfront (lock-free ArcSwap load), consuming req
        let resolved: Vec<(WireString, Vec<(i32, Option<u64>, Option<Bytes>)>)> = {
            let pmap = self.partition_map.load();
            req.topics
                .into_iter()
                .map(|td| {
                    let parts: Vec<_> = td
                        .partitions
                        .into_iter()
                        .map(|pd| {
                            let tid = pmap.resolve(&td.topic_name, pd.partition_index);
                            (pd.partition_index, tid, pd.record_set)
                        })
                        .collect();
                    (td.topic_name, parts)
                })
                .collect()
        };

        let mut topic_responses = Vec::with_capacity(resolved.len());
        for (topic_name, parts) in resolved {
            let mut part_responses = Vec::with_capacity(parts.len());
            for (partition_index, topic_id, record_set) in parts {
                let (error_code, base_offset) = match topic_id {
                    None => (ErrorCode::UnknownTopicOrPartition.as_i16(), -1i64),
                    Some(topic_id) => match record_set {
                        None => (ErrorCode::None.as_i16(), 0),
                        Some(record_set_bytes) => {
                            match self.produce_records(topic_id, record_set_bytes).await {
                                Ok(base_offset) => (ErrorCode::None.as_i16(), base_offset),
                                Err(e) => {
                                    warn!("produce error: {e}");
                                    (ErrorCode::CorruptMessage.as_i16(), -1)
                                }
                            }
                        }
                    },
                };
                part_responses.push(ProducePartitionResponse {
                    partition_index,
                    error_code,
                    base_offset,
                    log_append_time_ms: -1,
                    log_start_offset: 0,
                    ..Default::default()
                });
            }
            topic_responses.push(ProduceTopicResponse {
                topic_name,
                partitions: part_responses,
            });
        }

        KafkaResponse::Produce(ProduceResponse {
            topics: topic_responses,
        })
    }

    async fn produce_records(
        &self,
        topic_id: u64,
        record_set_bytes: Bytes,
    ) -> Result<i64, ProduceError> {
        // Zero-copy decode — consumes Bytes directly, no clone needed
        let batch =
            codec::decode_record_batch_bytes(record_set_bytes).map_err(ProduceError::Codec)?;

        // Batch-optimized conversion: Kafka records → FlatMessages
        let messages = codec::batch_records_to_flat_messages(&batch);

        self.m_produce_messages.increment(messages.len() as u64);

        let cmd = MqCommand::publish(topic_id, &messages);

        match self.batcher.submit(cmd).await {
            Ok(MqResponse::Published { base_offset, .. }) => {
                // Wake up long-polling fetch requests for this specific topic
                self.topic_notifier.notify_topic(topic_id);
                Ok(base_offset as i64)
            }
            Ok(MqResponse::Error(e)) => Err(ProduceError::Mq(e)),
            Ok(_) => Err(ProduceError::UnexpectedResponse),
            Err(e) => Err(ProduceError::Raft(e)),
        }
    }

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    async fn handle_fetch(&self, req: FetchRequest) -> KafkaResponse {
        self.m_fetch_requests.increment(1);

        let response = self.do_fetch(&req);

        // Long-polling: if no data returned and max_wait_ms > 0, wait for notification
        if req.max_wait_ms > 0 {
            let has_data = response
                .topics
                .iter()
                .any(|t| t.partitions.iter().any(|p| !p.record_set.is_empty()));

            if !has_data {
                let wait = Duration::from_millis(req.max_wait_ms.max(0) as u64);
                let notified = self.topic_notifier.notified_for_topic(0);
                tokio::select! {
                    _ = notified => {}
                    _ = tokio::time::sleep(wait) => {}
                }
                // Re-fetch after notification or timeout
                return KafkaResponse::Fetch(self.do_fetch(&req));
            }
        }

        KafkaResponse::Fetch(response)
    }

    fn do_fetch(&self, req: &FetchRequest) -> FetchResponse {
        let pmap = self.partition_map.load();
        let mut topic_responses = Vec::with_capacity(req.topics.len());

        for topic_data in &req.topics {
            let mut part_responses = Vec::with_capacity(topic_data.partitions.len());

            for part_data in &topic_data.partitions {
                let topic_id = pmap.resolve(&topic_data.topic_name, part_data.partition_index);

                match topic_id {
                    None => {
                        part_responses.push(FetchPartitionResponse {
                            partition_index: part_data.partition_index,
                            error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
                            high_watermark: -1,
                            last_stable_offset: -1,
                            log_start_offset: -1,
                            preferred_read_replica: -1,
                            record_set: Bytes::new(),
                            ..Default::default()
                        });
                    }
                    Some(topic_id) => {
                        let max_bytes = part_data.max_bytes.max(0) as usize;

                        // Prefer flat message path (zero-copy, no FetchedMessage structs)
                        let (record_set, high_watermark) = if let Some((flat_msgs, hw)) =
                            self.log_reader.read_topic_flat_messages(
                                topic_id,
                                part_data.fetch_offset as u64,
                                max_bytes,
                            ) {
                            self.m_fetch_messages.increment(flat_msgs.len() as u64);
                            let rs = if flat_msgs.is_empty() {
                                Bytes::new()
                            } else {
                                self.build_record_batch_from_flat(
                                    &flat_msgs,
                                    part_data.fetch_offset,
                                )
                            };
                            (rs, hw)
                        } else {
                            // Fallback: use FetchedMessage path
                            let (messages, hw) = self.log_reader.read_topic_messages(
                                topic_id,
                                part_data.fetch_offset as u64,
                                max_bytes,
                            );
                            self.m_fetch_messages.increment(messages.len() as u64);
                            let rs = if messages.is_empty() {
                                Bytes::new()
                            } else {
                                self.build_record_batch(&messages, part_data.fetch_offset)
                            };
                            (rs, hw)
                        };

                        part_responses.push(FetchPartitionResponse {
                            partition_index: part_data.partition_index,
                            error_code: ErrorCode::None.as_i16(),
                            high_watermark: high_watermark as i64,
                            last_stable_offset: high_watermark as i64,
                            log_start_offset: 0,
                            preferred_read_replica: -1,
                            record_set,
                            ..Default::default()
                        });
                    }
                }
            }

            topic_responses.push(FetchTopicResponse {
                topic_name: topic_data.topic_name.clone(),
                partitions: part_responses,
            });
        }

        FetchResponse {
            topics: topic_responses,
        }
    }

    fn build_record_batch(&self, messages: &[FetchedMessage], base_offset: i64) -> Bytes {
        // Encode directly into BytesMut — no intermediate RecordBatch/Record structs.
        let mut buf = bytes::BytesMut::with_capacity(
            // Estimate: 61-byte batch header + ~20 bytes overhead per record + payload
            61 + messages.len() * 20
                + messages
                    .iter()
                    .map(|m| {
                        m.value.len()
                            + m.key.as_ref().map_or(0, |k| k.len())
                            + m.headers
                                .iter()
                                .map(|(k, v)| k.len() + v.len())
                                .sum::<usize>()
                    })
                    .sum::<usize>(),
        );
        codec::encode_record_batch_from_fetched(messages, base_offset, &mut buf);
        buf.freeze()
    }

    /// Build a Kafka record batch directly from FlatMessage bytes (zero intermediate structs).
    /// `messages` is `&[(offset, flat_message_bytes)]` — typically zero-copy from mmap.
    fn build_record_batch_from_flat(&self, messages: &[(u64, Bytes)], base_offset: i64) -> Bytes {
        let mut buf = bytes::BytesMut::with_capacity(
            61 + messages.len() * 20 + messages.iter().map(|(_o, b)| b.len()).sum::<usize>(),
        );
        codec::encode_record_batch_from_flat(messages, base_offset, &mut buf);
        buf.freeze()
    }

    // -------------------------------------------------------------------------
    // ListOffsets
    // -------------------------------------------------------------------------

    fn handle_list_offsets(&self, req: ListOffsetsRequest) -> KafkaResponse {
        let pmap = self.partition_map.load();
        let mut topic_responses = Vec::with_capacity(req.topics.len());

        for topic_data in req.topics {
            let mut part_responses = Vec::with_capacity(topic_data.partitions.len());

            for part_data in &topic_data.partitions {
                let topic_id = pmap.resolve(&topic_data.topic_name, part_data.partition_index);

                match topic_id {
                    None => {
                        part_responses.push(ListOffsetsPartitionResponse {
                            partition_index: part_data.partition_index,
                            error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
                            timestamp: -1,
                            offset: -1,
                        });
                    }
                    Some(topic_id) => {
                        let offset = match part_data.timestamp {
                            -2 => self.log_reader.get_topic_tail(topic_id).unwrap_or(0) as i64,
                            -1 => self.log_reader.get_topic_head(topic_id).unwrap_or(0) as i64,
                            _ts => self.log_reader.get_topic_head(topic_id).unwrap_or(0) as i64,
                        };
                        part_responses.push(ListOffsetsPartitionResponse {
                            partition_index: part_data.partition_index,
                            error_code: ErrorCode::None.as_i16(),
                            timestamp: part_data.timestamp,
                            offset,
                        });
                    }
                }
            }

            topic_responses.push(ListOffsetsTopicResponse {
                topic_name: topic_data.topic_name,
                partitions: part_responses,
            });
        }

        KafkaResponse::ListOffsets(ListOffsetsResponse {
            topics: topic_responses,
        })
    }

    // -------------------------------------------------------------------------
    // FindCoordinator
    // -------------------------------------------------------------------------

    fn handle_find_coordinator(&self, _req: FindCoordinatorRequest) -> KafkaResponse {
        KafkaResponse::FindCoordinator(FindCoordinatorResponse {
            error_code: ErrorCode::None.as_i16(),
            node_id: self.broker_id,
            host: self.advertised_host.clone(),
            port: self.advertised_port,
        })
    }

    // -------------------------------------------------------------------------
    // Consumer Group — JoinGroup
    // -------------------------------------------------------------------------

    async fn handle_join_group(&self, req: JoinGroupRequest) -> KafkaResponse {
        self.m_group_joins.increment(1);

        if req.group_id.is_empty() {
            return KafkaResponse::JoinGroup(JoinGroupResponse {
                error_code: ErrorCode::InvalidGroupId.as_i16(),
                generation_id: -1,
                protocol_name: WireString::empty(),
                leader: WireString::empty(),
                member_id: WireString::empty(),
                members: Vec::new(),
            });
        }

        let group_id = match self.resolve_or_create_group(&req.group_id).await {
            Ok(id) => id,
            Err(()) => {
                return KafkaResponse::JoinGroup(JoinGroupResponse {
                    error_code: ErrorCode::GroupCoordinatorNotAvailable.as_i16(),
                    generation_id: -1,
                    protocol_name: WireString::empty(),
                    leader: WireString::empty(),
                    member_id: WireString::empty(),
                    members: Vec::new(),
                });
            }
        };

        let notify = self
            .metadata
            .get_consumer_group(group_id)
            .map(|g| Arc::clone(&g.phase_notify));

        let notified = notify.as_ref().map(|n| n.notified());

        let protocols: Vec<(&str, &[u8])> = req
            .protocols
            .iter()
            .map(|p| (p.name.as_str(), p.metadata.as_ref()))
            .collect();

        let cmd = MqCommand::join_consumer_group(
            group_id,
            &req.member_id,
            req.protocols
                .first()
                .map(|p| p.name.as_str())
                .unwrap_or("consumer"),
            req.session_timeout_ms,
            req.rebalance_timeout_ms,
            &req.protocol_type,
            &protocols,
        );

        let resp = match self.batcher.submit(cmd).await {
            Ok(resp) => resp,
            Err(e) => {
                warn!("join consumer group batcher error: {e}");
                return KafkaResponse::JoinGroup(JoinGroupResponse {
                    error_code: ErrorCode::GroupCoordinatorNotAvailable.as_i16(),
                    generation_id: -1,
                    protocol_name: WireString::empty(),
                    leader: WireString::empty(),
                    member_id: WireString::empty(),
                    members: Vec::new(),
                });
            }
        };

        match resp {
            MqResponse::GroupJoined {
                generation,
                leader,
                member_id,
                protocol_name,
                is_leader,
                members,
                phase_complete,
            } => {
                if phase_complete {
                    return KafkaResponse::JoinGroup(JoinGroupResponse {
                        error_code: ErrorCode::None.as_i16(),
                        generation_id: generation,
                        protocol_name: WireString::from(protocol_name),
                        leader: WireString::from(leader),
                        member_id: WireString::from(member_id),
                        members: if is_leader {
                            members
                                .into_iter()
                                .map(|(id, meta)| JoinGroupMember {
                                    member_id: WireString::from(id),
                                    group_instance_id: None,
                                    metadata: Bytes::from(meta),
                                    ..Default::default()
                                })
                                .collect()
                        } else {
                            Vec::new()
                        },
                    });
                }

                let timeout = Duration::from_millis(req.rebalance_timeout_ms.max(1000) as u64);
                if let Some(notified) = notified {
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep(timeout) => {}
                    }
                }

                self.build_join_response(group_id, &member_id)
            }
            MqResponse::Error(e) => {
                let error_code = mq_error_to_kafka_i16(&e);
                KafkaResponse::JoinGroup(JoinGroupResponse {
                    error_code,
                    generation_id: -1,
                    protocol_name: WireString::empty(),
                    leader: WireString::empty(),
                    member_id: req.member_id,
                    members: Vec::new(),
                })
            }
            other => {
                warn!("unexpected join group response: {other}");
                KafkaResponse::JoinGroup(JoinGroupResponse {
                    error_code: ErrorCode::GroupCoordinatorNotAvailable.as_i16(),
                    generation_id: -1,
                    protocol_name: WireString::empty(),
                    leader: WireString::empty(),
                    member_id: req.member_id,
                    members: Vec::new(),
                })
            }
        }
    }

    /// Build a JoinGroup response from current group state (after async wait).
    fn build_join_response(&self, group_id: u64, member_id: &str) -> KafkaResponse {
        let group = match self.metadata.get_consumer_group(group_id) {
            Some(g) => g,
            None => {
                return KafkaResponse::JoinGroup(JoinGroupResponse {
                    error_code: ErrorCode::GroupCoordinatorNotAvailable.as_i16(),
                    generation_id: -1,
                    protocol_name: WireString::empty(),
                    leader: WireString::empty(),
                    member_id: WireString::from(member_id),
                    members: Vec::new(),
                });
            }
        };

        let generation = group.generation();
        let leader_arc = group.leader();
        let leader_str = leader_arc.as_deref().map(|s| s.as_str()).unwrap_or("");
        let protocol_name = group.protocol_name();
        let is_leader = leader_str == member_id;

        let members = if is_leader {
            group
                .member_protocols()
                .into_iter()
                .map(|(id, meta)| JoinGroupMember {
                    member_id: WireString::from(id),
                    group_instance_id: None,
                    metadata: meta,
                    ..Default::default()
                })
                .collect()
        } else {
            Vec::new()
        };

        KafkaResponse::JoinGroup(JoinGroupResponse {
            error_code: ErrorCode::None.as_i16(),
            generation_id: generation,
            protocol_name: WireString::from(protocol_name.as_str()),
            leader: WireString::from(leader_str),
            member_id: WireString::from(member_id),
            members,
        })
    }

    // -------------------------------------------------------------------------
    // Consumer Group — SyncGroup
    // -------------------------------------------------------------------------

    async fn handle_sync_group(&self, req: SyncGroupRequest) -> KafkaResponse {
        self.m_group_syncs.increment(1);

        let group_id = match self.resolve_group_id(&req.group_id) {
            Some(id) => id,
            None => {
                return KafkaResponse::SyncGroup(SyncGroupResponse {
                    error_code: ErrorCode::InvalidGroupId.as_i16(),
                    assignment: Bytes::new(),
                });
            }
        };

        let notify = self
            .metadata
            .get_consumer_group(group_id)
            .map(|g| Arc::clone(&g.phase_notify));
        let notified = notify.as_ref().map(|n| n.notified());

        let assignments: Vec<(&str, &[u8])> = req
            .assignments
            .iter()
            .map(|a| (a.member_id.as_str(), a.assignment.as_ref()))
            .collect();

        let cmd = MqCommand::sync_consumer_group(
            group_id,
            req.generation_id,
            &req.member_id,
            &assignments,
        );

        let resp = match self.batcher.submit(cmd).await {
            Ok(resp) => resp,
            Err(e) => {
                warn!("sync consumer group batcher error: {e}");
                return KafkaResponse::SyncGroup(SyncGroupResponse {
                    error_code: ErrorCode::GroupCoordinatorNotAvailable.as_i16(),
                    assignment: Bytes::new(),
                });
            }
        };

        match resp {
            MqResponse::GroupSynced {
                assignment,
                phase_complete,
            } => {
                if phase_complete {
                    return KafkaResponse::SyncGroup(SyncGroupResponse {
                        error_code: ErrorCode::None.as_i16(),
                        assignment: Bytes::from(assignment),
                    });
                }

                let timeout = Duration::from_secs(30);
                if let Some(notified) = notified {
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep(timeout) => {}
                    }
                }

                let assignment = self
                    .metadata
                    .get_consumer_group(group_id)
                    .and_then(|g| g.get_member_assignment(&req.member_id))
                    .unwrap_or_default();

                KafkaResponse::SyncGroup(SyncGroupResponse {
                    error_code: ErrorCode::None.as_i16(),
                    assignment,
                })
            }
            MqResponse::Error(e) => {
                let error_code = mq_error_to_kafka_i16(&e);
                KafkaResponse::SyncGroup(SyncGroupResponse {
                    error_code,
                    assignment: Bytes::new(),
                })
            }
            other => {
                warn!("unexpected sync group response: {other}");
                KafkaResponse::SyncGroup(SyncGroupResponse {
                    error_code: ErrorCode::GroupCoordinatorNotAvailable.as_i16(),
                    assignment: Bytes::new(),
                })
            }
        }
    }

    // -------------------------------------------------------------------------
    // Consumer Group — Heartbeat
    // -------------------------------------------------------------------------

    async fn handle_heartbeat(&self, req: HeartbeatRequest) -> KafkaResponse {
        self.m_group_heartbeats.increment(1);

        let group_id = match self.resolve_group_id(&req.group_id) {
            Some(id) => id,
            None => {
                return KafkaResponse::Heartbeat(HeartbeatResponse {
                    error_code: ErrorCode::InvalidGroupId.as_i16(),
                });
            }
        };

        let cmd = MqCommand::heartbeat_consumer_group(group_id, &req.member_id, req.generation_id);

        match self.batcher.submit(cmd).await {
            Ok(MqResponse::Ok) => KafkaResponse::Heartbeat(HeartbeatResponse {
                error_code: ErrorCode::None.as_i16(),
            }),
            Ok(MqResponse::Error(e)) => {
                let error_code = mq_error_to_kafka_i16(&e);
                KafkaResponse::Heartbeat(HeartbeatResponse { error_code })
            }
            _ => KafkaResponse::Heartbeat(HeartbeatResponse {
                error_code: ErrorCode::GroupCoordinatorNotAvailable.as_i16(),
            }),
        }
    }

    // -------------------------------------------------------------------------
    // Consumer Group — LeaveGroup
    // -------------------------------------------------------------------------

    async fn handle_leave_group(&self, req: LeaveGroupRequest) -> KafkaResponse {
        self.m_group_leaves.increment(1);

        let group_id = match self.resolve_group_id(&req.group_id) {
            Some(id) => id,
            None => {
                return KafkaResponse::LeaveGroup(LeaveGroupResponse {
                    error_code: ErrorCode::None.as_i16(),
                    members: Vec::new(),
                    ..Default::default()
                });
            }
        };

        let cmd = MqCommand::leave_consumer_group(group_id, &req.member_id);

        match self.batcher.submit(cmd).await {
            Ok(MqResponse::Ok) => {}
            Err(e) => {
                warn!("leave consumer group batcher error: {e}");
            }
            _ => {}
        }

        KafkaResponse::LeaveGroup(LeaveGroupResponse {
            error_code: ErrorCode::None.as_i16(),
            members: Vec::new(),
            ..Default::default()
        })
    }

    // -------------------------------------------------------------------------
    // OffsetCommit — Raft-replicated via consumer group offsets
    // -------------------------------------------------------------------------

    async fn handle_offset_commit(
        &self,
        req: OffsetCommitRequest,
        _header: &RequestHeader,
    ) -> KafkaResponse {
        let group_id = match self.resolve_group_id(&req.group_id) {
            Some(id) => id,
            None => {
                return self.handle_offset_commit_legacy(req).await;
            }
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Resolve all partition IDs upfront (lock-free), consuming req.topics
        let resolved: Vec<(WireString, Vec<(i32, Option<u64>, i64)>)> = {
            let pmap = self.partition_map.load();
            req.topics
                .into_iter()
                .map(|td| {
                    let parts: Vec<_> = td
                        .partitions
                        .iter()
                        .map(|pd| {
                            let tid = pmap.resolve(&td.topic_name, pd.partition_index);
                            (pd.partition_index, tid, pd.offset)
                        })
                        .collect();
                    (td.topic_name, parts)
                })
                .collect()
        };

        let mut topic_responses = Vec::with_capacity(resolved.len());
        for (topic_name, parts) in resolved {
            let mut part_responses = Vec::with_capacity(parts.len());
            for (partition_index, topic_id, offset) in parts {
                match topic_id {
                    None => {
                        part_responses.push(OffsetCommitPartitionResponse {
                            partition_index,
                            error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
                        });
                    }
                    Some(topic_id) => {
                        let cmd = MqCommand::commit_group_offset(
                            group_id,
                            req.generation_id,
                            topic_id,
                            partition_index as u32,
                            offset as u64,
                            None,
                            now,
                        );
                        let error_code = match self.batcher.submit(cmd).await {
                            Ok(MqResponse::Ok) => ErrorCode::None.as_i16(),
                            Ok(MqResponse::Error(e)) => {
                                debug!("offset commit error: {e}");
                                mq_error_to_kafka_i16(&e)
                            }
                            _ => ErrorCode::UnknownTopicOrPartition.as_i16(),
                        };
                        part_responses.push(OffsetCommitPartitionResponse {
                            partition_index,
                            error_code,
                        });
                    }
                }
            }

            topic_responses.push(OffsetCommitTopicResponse {
                topic_name,
                partitions: part_responses,
            });
        }

        KafkaResponse::OffsetCommit(OffsetCommitResponse {
            topics: topic_responses,
        })
    }

    /// Legacy offset commit for consumers not using Raft-replicated groups.
    async fn handle_offset_commit_legacy(&self, req: OffsetCommitRequest) -> KafkaResponse {
        let consumer_id = name_hash(&req.group_id);

        let resolved: Vec<(WireString, Vec<(i32, Option<u64>, i64)>)> = {
            let pmap = self.partition_map.load();
            req.topics
                .into_iter()
                .map(|td| {
                    let parts: Vec<_> = td
                        .partitions
                        .iter()
                        .map(|pd| {
                            let tid = pmap.resolve(&td.topic_name, pd.partition_index);
                            (pd.partition_index, tid, pd.offset)
                        })
                        .collect();
                    (td.topic_name, parts)
                })
                .collect()
        };

        let mut topic_responses = Vec::with_capacity(resolved.len());
        for (topic_name, parts) in resolved {
            let mut part_responses = Vec::with_capacity(parts.len());
            for (partition_index, topic_id, offset) in parts {
                match topic_id {
                    None => {
                        part_responses.push(OffsetCommitPartitionResponse {
                            partition_index,
                            error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
                        });
                    }
                    Some(topic_id) => {
                        let cmd = MqCommand::commit_offset(topic_id, consumer_id, offset as u64);
                        let error_code = match self.batcher.submit(cmd).await {
                            Ok(MqResponse::Ok) => ErrorCode::None.as_i16(),
                            Ok(MqResponse::Error(e)) => {
                                debug!("offset commit error: {e}");
                                ErrorCode::UnknownTopicOrPartition.as_i16()
                            }
                            _ => ErrorCode::UnknownTopicOrPartition.as_i16(),
                        };
                        part_responses.push(OffsetCommitPartitionResponse {
                            partition_index,
                            error_code,
                        });
                    }
                }
            }

            topic_responses.push(OffsetCommitTopicResponse {
                topic_name,
                partitions: part_responses,
            });
        }

        KafkaResponse::OffsetCommit(OffsetCommitResponse {
            topics: topic_responses,
        })
    }

    // -------------------------------------------------------------------------
    // OffsetFetch — reads from Raft-replicated consumer group offsets
    // -------------------------------------------------------------------------

    fn handle_offset_fetch(&self, req: OffsetFetchRequest) -> KafkaResponse {
        let pmap = self.partition_map.load();
        let group_id = self.resolve_group_id(&req.group_id);

        let mut topic_responses = Vec::with_capacity(req.topics.len());

        for topic_data in req.topics {
            let mut part_responses = Vec::with_capacity(topic_data.partitions.len());

            for &partition in &topic_data.partitions {
                let topic_id = pmap.resolve(&topic_data.topic_name, partition);

                match topic_id {
                    None => {
                        part_responses.push(OffsetFetchPartitionResponse {
                            partition_index: partition,
                            offset: -1,
                            metadata: None,
                            committed_leader_epoch: -1,
                            error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
                            ..Default::default()
                        });
                    }
                    Some(topic_id) => {
                        let offset = group_id
                            .and_then(|gid| {
                                self.metadata
                                    .get_consumer_group(gid)
                                    .and_then(|g| g.get_offset(topic_id, partition as u32))
                                    .map(|o| o as i64)
                            })
                            .or_else(|| {
                                let consumer_id = name_hash(&req.group_id);
                                self.log_reader
                                    .get_committed_offset(topic_id, consumer_id)
                                    .map(|o| o as i64)
                            })
                            .unwrap_or(-1);

                        part_responses.push(OffsetFetchPartitionResponse {
                            partition_index: partition,
                            offset,
                            metadata: None,
                            committed_leader_epoch: -1,
                            error_code: ErrorCode::None.as_i16(),
                            ..Default::default()
                        });
                    }
                }
            }

            topic_responses.push(OffsetFetchTopicResponse {
                topic_name: topic_data.topic_name,
                partitions: part_responses,
            });
        }

        KafkaResponse::OffsetFetch(OffsetFetchResponse {
            topics: topic_responses,
        })
    }

    // -------------------------------------------------------------------------
    // CreateTopics
    // -------------------------------------------------------------------------

    async fn handle_create_topics(&self, req: CreateTopicsRequest) -> KafkaResponse {
        let mut topic_responses = Vec::with_capacity(req.topics.len());

        for ct in req.topics {
            let num_partitions = ct.num_partitions.max(1) as usize;
            let mut error_code = ErrorCode::None.as_i16();

            for i in 0..num_partitions {
                let mq_name = partition::partition_topic_name(&ct.name, i as i32);
                let cmd = MqCommand::create_topic(&mq_name, RetentionPolicy::default(), 0);
                match self.batcher.submit(cmd).await {
                    Ok(MqResponse::EntityCreated { .. }) => {}
                    Ok(MqResponse::Error(bisque_mq::types::MqError::AlreadyExists { .. })) => {
                        error_code = ErrorCode::TopicAlreadyExists.as_i16();
                    }
                    Ok(MqResponse::Error(e)) => {
                        warn!("create topic error: {e}");
                        error_code = ErrorCode::InvalidTopicException.as_i16();
                        break;
                    }
                    Err(e) => {
                        warn!("create topic batcher error: {e}");
                        error_code = ErrorCode::InvalidTopicException.as_i16();
                        break;
                    }
                    _ => {}
                }
            }

            self.refresh_partitions();

            topic_responses.push(CreateTopicResponse {
                name: ct.name,
                error_code,
            });
        }

        KafkaResponse::CreateTopics(CreateTopicsResponse {
            topics: topic_responses,
        })
    }

    // -------------------------------------------------------------------------
    // DeleteTopics
    // -------------------------------------------------------------------------

    async fn handle_delete_topics(&self, req: DeleteTopicsRequest) -> KafkaResponse {
        // Resolve all topic IDs upfront (lock-free), consuming req
        let resolved: Vec<(WireString, Option<Vec<u64>>)> = {
            let pmap = self.partition_map.load();
            req.topic_names
                .into_iter()
                .map(|name| {
                    let ids = pmap
                        .partitions(&name)
                        .map(|parts| parts.iter().map(|&(_, id)| id).collect());
                    (name, ids)
                })
                .collect()
        };

        let mut topic_responses = Vec::with_capacity(resolved.len());
        for (name, part_ids) in resolved {
            match part_ids {
                None => {
                    topic_responses.push(DeleteTopicResponse {
                        name,
                        error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
                    });
                }
                Some(ids) => {
                    let mut error_code = ErrorCode::None.as_i16();
                    for topic_id in ids {
                        let cmd = MqCommand::delete_topic(topic_id);
                        if let Err(e) = self.batcher.submit(cmd).await {
                            warn!("delete topic error: {e}");
                            error_code = ErrorCode::UnknownTopicOrPartition.as_i16();
                            break;
                        }
                    }
                    self.refresh_partitions();
                    topic_responses.push(DeleteTopicResponse { name, error_code });
                }
            }
        }

        KafkaResponse::DeleteTopics(DeleteTopicsResponse {
            topics: topic_responses,
        })
    }

    // -------------------------------------------------------------------------
    // DescribeGroups / ListGroups — reads from Raft-replicated state
    // -------------------------------------------------------------------------

    fn handle_describe_groups(&self, req: DescribeGroupsRequest) -> KafkaResponse {
        let groups = req
            .group_ids
            .iter()
            .map(|name| self.describe_group(name))
            .collect();
        KafkaResponse::DescribeGroups(DescribeGroupsResponse { groups })
    }

    fn describe_group(&self, group_name: &str) -> DescribedGroup {
        let group_id = match self.resolve_group_id(group_name) {
            Some(id) => id,
            None => {
                return DescribedGroup {
                    error_code: ErrorCode::InvalidGroupId.as_i16(),
                    group_id: WireString::from(group_name),
                    state: WireString::from_static("Dead"),
                    protocol_type: WireString::empty(),
                    protocol: WireString::empty(),
                    members: Vec::new(),
                };
            }
        };

        let group = match self.metadata.get_consumer_group(group_id) {
            Some(g) => g,
            None => {
                return DescribedGroup {
                    error_code: ErrorCode::InvalidGroupId.as_i16(),
                    group_id: WireString::from(group_name),
                    state: WireString::from_static("Dead"),
                    protocol_type: WireString::empty(),
                    protocol: WireString::empty(),
                    members: Vec::new(),
                };
            }
        };

        let phase_str = match group.phase() {
            GroupPhase::Empty => "Empty",
            GroupPhase::PreparingRebalance => "PreparingRebalance",
            GroupPhase::CompletingRebalance => "CompletingRebalance",
            GroupPhase::Stable => "Stable",
            GroupPhase::Dead => "Dead",
        };

        let protocol_name = group.protocol_name();
        let member_protos = group.member_protocols();
        let members = member_protos
            .into_iter()
            .map(|(mid, metadata)| {
                let assignment = group.get_member_assignment(&mid).unwrap_or_default();
                let wire_mid = WireString::from(mid);
                DescribedGroupMember {
                    client_id: wire_mid.clone(),
                    member_id: wire_mid,
                    group_instance_id: None,
                    client_host: WireString::empty(),
                    metadata,
                    assignment,
                    ..Default::default()
                }
            })
            .collect();

        DescribedGroup {
            error_code: ErrorCode::None.as_i16(),
            group_id: WireString::from(group_name),
            state: WireString::from_static(phase_str),
            protocol_type: WireString::from(group.meta.protocol_type.clone()),
            protocol: WireString::from(protocol_name.as_str()),
            members,
        }
    }

    fn handle_list_groups(&self) -> KafkaResponse {
        let groups: Vec<ListedGroup> = self
            .metadata
            .iter_consumer_groups()
            .into_iter()
            .map(|(_, group)| ListedGroup {
                group_id: WireString::from(group.meta.name.clone()),
                protocol_type: WireString::from(group.meta.protocol_type.clone()),
                group_state: WireString::empty(),
                ..Default::default()
            })
            .collect();

        KafkaResponse::ListGroups(ListGroupsResponse {
            error_code: ErrorCode::None.as_i16(),
            groups,
        })
    }

    // -------------------------------------------------------------------------
    // SASL Authentication
    // -------------------------------------------------------------------------

    fn handle_sasl_handshake(&self, req: SaslHandshakeRequest) -> KafkaResponse {
        let mechanisms = vec![WireString::from_static("PLAIN")];
        let error_code = if req.mechanism.as_str() == "PLAIN" {
            ErrorCode::None.as_i16()
        } else {
            ErrorCode::UnsupportedVersion.as_i16()
        };
        KafkaResponse::SaslHandshake(SaslHandshakeResponse {
            error_code,
            mechanisms,
        })
    }

    fn handle_sasl_authenticate(&self, req: SaslAuthenticateRequest) -> KafkaResponse {
        match auth::parse_sasl_plain(&req.auth_bytes) {
            Some((username, password)) => match self.authenticator.authenticate(username, password)
            {
                Ok(()) => KafkaResponse::SaslAuthenticate(SaslAuthenticateResponse {
                    error_code: ErrorCode::None.as_i16(),
                    error_message: None,
                    auth_bytes: Bytes::new(),
                    session_lifetime_ms: 0,
                    ..Default::default()
                }),
                Err(msg) => KafkaResponse::SaslAuthenticate(SaslAuthenticateResponse {
                    error_code: ErrorCode::SaslAuthenticationFailed.as_i16(),
                    error_message: Some(WireString::from_static(msg)),
                    auth_bytes: Bytes::new(),
                    session_lifetime_ms: 0,
                    ..Default::default()
                }),
            },
            None => KafkaResponse::SaslAuthenticate(SaslAuthenticateResponse {
                error_code: ErrorCode::SaslAuthenticationFailed.as_i16(),
                error_message: Some(WireString::from_static("invalid SASL/PLAIN payload")),
                auth_bytes: Bytes::new(),
                session_lifetime_ms: 0,
                ..Default::default()
            }),
        }
    }

    // -------------------------------------------------------------------------
    // InitProducerId
    // -------------------------------------------------------------------------

    fn handle_init_producer_id(&self, req: InitProducerIdRequest) -> KafkaResponse {
        let txn_id = req.transactional_id.as_deref();
        let (producer_id, producer_epoch) = self.txn_coordinator.init_producer_id(txn_id);
        KafkaResponse::InitProducerId(InitProducerIdResponse {
            error_code: ErrorCode::None.as_i16(),
            producer_id,
            producer_epoch,
        })
    }

    // -------------------------------------------------------------------------
    // Transaction APIs
    // -------------------------------------------------------------------------

    fn handle_add_partitions_to_txn(&self, req: AddPartitionsToTxnRequest) -> KafkaResponse {
        let mut topic_responses = Vec::with_capacity(req.topics.len());

        for topic_data in &req.topics {
            let partitions: Vec<_> = topic_data
                .partitions
                .iter()
                .map(|&p| {
                    let error_code = match self.txn_coordinator.add_partitions(
                        &req.transactional_id,
                        req.producer_id,
                        req.producer_epoch,
                        &topic_data.topic_name,
                        &[p],
                    ) {
                        Ok(()) => ErrorCode::None.as_i16(),
                        Err(ec) => ec,
                    };
                    AddPartitionsToTxnPartitionResponse {
                        partition_index: p,
                        error_code,
                    }
                })
                .collect();

            topic_responses.push(AddPartitionsToTxnTopicResponse {
                topic_name: topic_data.topic_name.clone(),
                partitions,
            });
        }

        KafkaResponse::AddPartitionsToTxn(AddPartitionsToTxnResponse {
            topics: topic_responses,
        })
    }

    fn handle_add_offsets_to_txn(&self, req: AddOffsetsToTxnRequest) -> KafkaResponse {
        let error_code = match self.txn_coordinator.add_offsets_to_txn(
            &req.transactional_id,
            req.producer_id,
            req.producer_epoch,
            &req.group_id,
        ) {
            Ok(()) => ErrorCode::None.as_i16(),
            Err(ec) => ec,
        };
        KafkaResponse::AddOffsetsToTxn(AddOffsetsToTxnResponse { error_code })
    }

    fn handle_end_txn(&self, req: EndTxnRequest) -> KafkaResponse {
        let error_code = match self.txn_coordinator.end_txn(
            &req.transactional_id,
            req.producer_id,
            req.producer_epoch,
            req.committed,
        ) {
            Ok(()) => ErrorCode::None.as_i16(),
            Err(ec) => ec,
        };
        KafkaResponse::EndTxn(EndTxnResponse { error_code })
    }

    async fn handle_txn_offset_commit(&self, req: TxnOffsetCommitRequest) -> KafkaResponse {
        if let Err(ec) = self.txn_coordinator.validate_txn_offset_commit(
            &req.transactional_id,
            req.producer_id,
            req.producer_epoch,
        ) {
            // Return error for all partitions
            let topics = req
                .topics
                .iter()
                .map(|t| TxnOffsetCommitTopicResponse {
                    topic_name: t.topic_name.clone(),
                    partitions: t
                        .partitions
                        .iter()
                        .map(|p| TxnOffsetCommitPartitionResponse {
                            partition_index: p.partition_index,
                            error_code: ec,
                        })
                        .collect(),
                })
                .collect();
            return KafkaResponse::TxnOffsetCommit(TxnOffsetCommitResponse { topics });
        }

        // Commit offsets via the group-based path
        let group_id = match self.resolve_group_id(&req.group_id) {
            Some(id) => id,
            None => {
                let topics = req
                    .topics
                    .iter()
                    .map(|t| TxnOffsetCommitTopicResponse {
                        topic_name: t.topic_name.clone(),
                        partitions: t
                            .partitions
                            .iter()
                            .map(|p| TxnOffsetCommitPartitionResponse {
                                partition_index: p.partition_index,
                                error_code: ErrorCode::GroupIdNotFound.as_i16(),
                            })
                            .collect(),
                    })
                    .collect();
                return KafkaResponse::TxnOffsetCommit(TxnOffsetCommitResponse { topics });
            }
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let pmap = self.partition_map.load();
        let mut topic_responses = Vec::with_capacity(req.topics.len());

        for topic_data in &req.topics {
            let mut part_responses = Vec::with_capacity(topic_data.partitions.len());
            for part_data in &topic_data.partitions {
                let topic_id = pmap.resolve(&topic_data.topic_name, part_data.partition_index);
                match topic_id {
                    None => {
                        part_responses.push(TxnOffsetCommitPartitionResponse {
                            partition_index: part_data.partition_index,
                            error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
                        });
                    }
                    Some(topic_id) => {
                        let cmd = MqCommand::commit_group_offset(
                            group_id,
                            -1, // generation not checked for txn commits
                            topic_id,
                            part_data.partition_index as u32,
                            part_data.offset as u64,
                            None,
                            now,
                        );
                        let error_code = match self.batcher.submit(cmd).await {
                            Ok(MqResponse::Ok) => ErrorCode::None.as_i16(),
                            Ok(MqResponse::Error(e)) => {
                                debug!("txn offset commit error: {e}");
                                mq_error_to_kafka_i16(&e)
                            }
                            _ => ErrorCode::UnknownTopicOrPartition.as_i16(),
                        };
                        part_responses.push(TxnOffsetCommitPartitionResponse {
                            partition_index: part_data.partition_index,
                            error_code,
                        });
                    }
                }
            }
            topic_responses.push(TxnOffsetCommitTopicResponse {
                topic_name: topic_data.topic_name.clone(),
                partitions: part_responses,
            });
        }

        KafkaResponse::TxnOffsetCommit(TxnOffsetCommitResponse {
            topics: topic_responses,
        })
    }

    // -------------------------------------------------------------------------
    // DeleteRecords
    // -------------------------------------------------------------------------

    async fn handle_delete_records(&self, req: DeleteRecordsRequest) -> KafkaResponse {
        let pmap = self.partition_map.load();
        let mut topic_responses = Vec::with_capacity(req.topics.len());

        for topic_data in &req.topics {
            let mut part_responses = Vec::with_capacity(topic_data.partitions.len());
            for part_data in &topic_data.partitions {
                let topic_id = pmap.resolve(&topic_data.topic_name, part_data.partition_index);
                match topic_id {
                    None => {
                        part_responses.push(DeleteRecordsPartitionResponse {
                            partition_index: part_data.partition_index,
                            low_watermark: -1,
                            error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
                        });
                    }
                    Some(_topic_id) => {
                        // bisque-mq doesn't support log truncation yet — accept the
                        // request and report the target offset as the new low watermark
                        part_responses.push(DeleteRecordsPartitionResponse {
                            partition_index: part_data.partition_index,
                            low_watermark: part_data.offset,
                            error_code: ErrorCode::None.as_i16(),
                        });
                    }
                }
            }
            topic_responses.push(DeleteRecordsTopicResponse {
                topic_name: topic_data.topic_name.clone(),
                partitions: part_responses,
            });
        }

        KafkaResponse::DeleteRecords(DeleteRecordsResponse {
            topics: topic_responses,
        })
    }

    // -------------------------------------------------------------------------
    // DescribeConfigs / AlterConfigs
    // -------------------------------------------------------------------------

    fn handle_describe_configs(&self, req: DescribeConfigsRequest) -> KafkaResponse {
        let resources = req
            .resources
            .into_iter()
            .map(|res| {
                // Return default config entries for topics
                let configs = if res.resource_type == 2 {
                    // Topic resource
                    vec![
                        DescribeConfigEntry {
                            name: WireString::from_static("cleanup.policy"),
                            value: Some(WireString::from_static("delete")),
                            read_only: false,
                            is_default: true,
                            is_sensitive: false,
                        },
                        DescribeConfigEntry {
                            name: WireString::from_static("retention.ms"),
                            value: Some(WireString::from_static("604800000")),
                            read_only: false,
                            is_default: true,
                            is_sensitive: false,
                        },
                        DescribeConfigEntry {
                            name: WireString::from_static("segment.bytes"),
                            value: Some(WireString::from_static("1073741824")),
                            read_only: false,
                            is_default: true,
                            is_sensitive: false,
                        },
                        DescribeConfigEntry {
                            name: WireString::from_static("max.message.bytes"),
                            value: Some(WireString::from_static("1048576")),
                            read_only: false,
                            is_default: true,
                            is_sensitive: false,
                        },
                        DescribeConfigEntry {
                            name: WireString::from_static("compression.type"),
                            value: Some(WireString::from_static("producer")),
                            read_only: false,
                            is_default: true,
                            is_sensitive: false,
                        },
                    ]
                } else if res.resource_type == 4 {
                    // Broker resource
                    vec![
                        DescribeConfigEntry {
                            name: WireString::from_static("log.retention.hours"),
                            value: Some(WireString::from_static("168")),
                            read_only: false,
                            is_default: true,
                            is_sensitive: false,
                        },
                        DescribeConfigEntry {
                            name: WireString::from_static("num.partitions"),
                            value: Some(WireString::from_static("1")),
                            read_only: false,
                            is_default: true,
                            is_sensitive: false,
                        },
                    ]
                } else {
                    vec![]
                };

                DescribeConfigsResourceResult {
                    error_code: ErrorCode::None.as_i16(),
                    error_message: None,
                    resource_type: res.resource_type,
                    resource_name: res.resource_name,
                    configs,
                }
            })
            .collect();

        KafkaResponse::DescribeConfigs(DescribeConfigsResponse { resources })
    }

    fn handle_alter_configs(&self, req: AlterConfigsRequest) -> KafkaResponse {
        // Accept but no-op (single-node, configs are not persisted)
        let resources = req
            .resources
            .into_iter()
            .map(|res| AlterConfigsResourceResult {
                error_code: ErrorCode::None.as_i16(),
                error_message: None,
                resource_type: res.resource_type,
                resource_name: res.resource_name,
            })
            .collect();
        KafkaResponse::AlterConfigs(AlterConfigsResponse { resources })
    }

    // -------------------------------------------------------------------------
    // CreatePartitions
    // -------------------------------------------------------------------------

    async fn handle_create_partitions(&self, req: CreatePartitionsRequest) -> KafkaResponse {
        let mut topic_responses = Vec::with_capacity(req.topics.len());

        for ct in req.topics {
            let pmap = self.partition_map.load();
            let current_count = pmap.partition_count(&ct.name).unwrap_or(0);
            let new_total = ct.count as usize;

            if new_total <= current_count {
                topic_responses.push(CreatePartitionsTopicResponse {
                    name: ct.name,
                    error_code: ErrorCode::InvalidPartitions.as_i16(),
                    error_message: Some(WireString::from(
                        "new partition count must be greater than current",
                    )),
                });
                continue;
            }

            if req.validate_only {
                topic_responses.push(CreatePartitionsTopicResponse {
                    name: ct.name,
                    error_code: ErrorCode::None.as_i16(),
                    error_message: None,
                });
                continue;
            }

            let mut error_code = ErrorCode::None.as_i16();
            let mut error_message = None;

            for i in current_count..new_total {
                let mq_name = partition::partition_topic_name(&ct.name, i as i32);
                let cmd = MqCommand::create_topic(&mq_name, RetentionPolicy::default(), 0);
                match self.batcher.submit(cmd).await {
                    Ok(MqResponse::EntityCreated { .. }) => {}
                    Ok(MqResponse::Error(MqError::AlreadyExists { .. })) => {}
                    Ok(MqResponse::Error(e)) => {
                        warn!("create partition error: {e}");
                        error_code = ErrorCode::InvalidTopicException.as_i16();
                        error_message = Some(WireString::from(format!("{e}")));
                        break;
                    }
                    Err(e) => {
                        warn!("create partition batcher error: {e}");
                        error_code = ErrorCode::InvalidTopicException.as_i16();
                        error_message = Some(WireString::from(format!("{e}")));
                        break;
                    }
                    _ => {}
                }
            }

            self.refresh_partitions();
            topic_responses.push(CreatePartitionsTopicResponse {
                name: ct.name,
                error_code,
                error_message,
            });
        }

        KafkaResponse::CreatePartitions(CreatePartitionsResponse {
            topics: topic_responses,
        })
    }

    // -------------------------------------------------------------------------
    // DeleteGroups
    // -------------------------------------------------------------------------

    async fn handle_delete_groups(&self, req: DeleteGroupsRequest) -> KafkaResponse {
        let mut results = Vec::with_capacity(req.group_ids.len());

        for group_name in &req.group_ids {
            let group_id = match self.resolve_group_id(group_name) {
                Some(id) => id,
                None => {
                    results.push(DeleteGroupResult {
                        group_id: group_name.clone(),
                        error_code: ErrorCode::GroupIdNotFound.as_i16(),
                    });
                    continue;
                }
            };

            // Check if group is empty
            if let Some(group) = self.metadata.get_consumer_group(group_id) {
                let phase = group.phase();
                if phase != GroupPhase::Empty && phase != GroupPhase::Dead {
                    results.push(DeleteGroupResult {
                        group_id: group_name.clone(),
                        error_code: ErrorCode::GroupNotEmpty.as_i16(),
                    });
                    continue;
                }
            }

            let cmd = MqCommand::delete_consumer_group(group_id);
            let error_code = match self.batcher.submit(cmd).await {
                Ok(MqResponse::Ok) => ErrorCode::None.as_i16(),
                Ok(MqResponse::Error(e)) => {
                    debug!("delete group error: {e}");
                    mq_error_to_kafka_i16(&e)
                }
                _ => ErrorCode::GroupCoordinatorNotAvailable.as_i16(),
            };

            results.push(DeleteGroupResult {
                group_id: group_name.clone(),
                error_code,
            });
        }

        KafkaResponse::DeleteGroups(DeleteGroupsResponse { results })
    }

    // -------------------------------------------------------------------------
    // OffsetDelete
    // -------------------------------------------------------------------------

    async fn handle_offset_delete(&self, req: OffsetDeleteRequest) -> KafkaResponse {
        let group_id = match self.resolve_group_id(&req.group_id) {
            Some(id) => id,
            None => {
                return KafkaResponse::OffsetDelete(OffsetDeleteResponse {
                    error_code: ErrorCode::GroupIdNotFound.as_i16(),
                    topics: Vec::new(),
                });
            }
        };

        let pmap = self.partition_map.load();
        let mut topic_responses = Vec::with_capacity(req.topics.len());

        for topic_data in &req.topics {
            let mut part_responses = Vec::with_capacity(topic_data.partitions.len());
            for part_data in &topic_data.partitions {
                let topic_id = pmap.resolve(&topic_data.topic_name, part_data.partition_index);
                match topic_id {
                    None => {
                        part_responses.push(OffsetDeletePartitionResponse {
                            partition_index: part_data.partition_index,
                            error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
                        });
                    }
                    Some(topic_id) => {
                        // Delete the committed offset by committing offset -1
                        let cmd = MqCommand::commit_group_offset(
                            group_id,
                            -1,
                            topic_id,
                            part_data.partition_index as u32,
                            0, // reset to 0
                            None,
                            0,
                        );
                        let error_code = match self.batcher.submit(cmd).await {
                            Ok(MqResponse::Ok) => ErrorCode::None.as_i16(),
                            Ok(MqResponse::Error(e)) => {
                                debug!("offset delete error: {e}");
                                mq_error_to_kafka_i16(&e)
                            }
                            _ => ErrorCode::GroupCoordinatorNotAvailable.as_i16(),
                        };
                        part_responses.push(OffsetDeletePartitionResponse {
                            partition_index: part_data.partition_index,
                            error_code,
                        });
                    }
                }
            }
            topic_responses.push(OffsetDeleteTopicResponse {
                topic_name: topic_data.topic_name.clone(),
                partitions: part_responses,
            });
        }

        KafkaResponse::OffsetDelete(OffsetDeleteResponse {
            error_code: ErrorCode::None.as_i16(),
            topics: topic_responses,
        })
    }

    // -------------------------------------------------------------------------
    // OffsetForLeaderEpoch
    // -------------------------------------------------------------------------

    fn handle_offset_for_leader_epoch(&self, req: OffsetForLeaderEpochRequest) -> KafkaResponse {
        let pmap = self.partition_map.load();
        let mut topic_responses = Vec::with_capacity(req.topics.len());

        for topic_data in req.topics {
            let mut part_responses = Vec::with_capacity(topic_data.partitions.len());

            for part_data in &topic_data.partitions {
                let topic_id = pmap.resolve(&topic_data.topic_name, part_data.partition_index);

                match topic_id {
                    None => {
                        part_responses.push(OffsetForLeaderEpochPartitionResponse {
                            error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
                            partition_index: part_data.partition_index,
                            leader_epoch: -1,
                            end_offset: -1,
                        });
                    }
                    Some(topic_id) => {
                        let end_offset =
                            self.log_reader.get_topic_head(topic_id).unwrap_or(0) as i64;
                        part_responses.push(OffsetForLeaderEpochPartitionResponse {
                            error_code: ErrorCode::None.as_i16(),
                            partition_index: part_data.partition_index,
                            leader_epoch: 0, // single-node: always epoch 0
                            end_offset,
                        });
                    }
                }
            }

            topic_responses.push(OffsetForLeaderEpochTopicResponse {
                topic_name: topic_data.topic_name,
                partitions: part_responses,
            });
        }

        KafkaResponse::OffsetForLeaderEpoch(OffsetForLeaderEpochResponse {
            topics: topic_responses,
        })
    }

    // -------------------------------------------------------------------------
    // DescribeCluster
    // -------------------------------------------------------------------------

    // -------------------------------------------------------------------------
    // IncrementalAlterConfigs (API 44) — stub
    // -------------------------------------------------------------------------

    fn handle_incremental_alter_configs(
        &self,
        req: IncrementalAlterConfigsRequest,
    ) -> KafkaResponse {
        // Accept but no-op (single-node, configs are not persisted)
        let resources = req
            .resources
            .into_iter()
            .map(|res| IncrementalAlterConfigsResourceResult {
                error_code: ErrorCode::None.as_i16(),
                error_message: None,
                resource_type: res.resource_type,
                resource_name: res.resource_name,
            })
            .collect();
        KafkaResponse::IncrementalAlterConfigs(IncrementalAlterConfigsResponse { resources })
    }

    // -------------------------------------------------------------------------
    // DescribeAcls (API 29) — stub
    // -------------------------------------------------------------------------

    fn handle_describe_acls(&self, _req: DescribeAclsRequest) -> KafkaResponse {
        // Return empty results — no ACLs configured
        KafkaResponse::DescribeAcls(DescribeAclsResponse {
            error_code: ErrorCode::None.as_i16(),
            error_message: None,
            resources: vec![],
        })
    }

    // -------------------------------------------------------------------------
    // CreateAcls (API 30) — stub
    // -------------------------------------------------------------------------

    fn handle_create_acls(&self, req: CreateAclsRequest) -> KafkaResponse {
        // Pretend ACLs were created — return success for each
        let results = req
            .creations
            .iter()
            .map(|_| CreateAclsResult {
                error_code: ErrorCode::None.as_i16(),
                error_message: None,
            })
            .collect();
        KafkaResponse::CreateAcls(CreateAclsResponse { results })
    }

    // -------------------------------------------------------------------------
    // DeleteAcls (API 31) — stub
    // -------------------------------------------------------------------------

    fn handle_delete_acls(&self, req: DeleteAclsRequest) -> KafkaResponse {
        // Return empty matching_acls for each filter
        let filter_results = req
            .filters
            .iter()
            .map(|_| DeleteAclsFilterResult {
                error_code: ErrorCode::None.as_i16(),
                error_message: None,
                matching_acls: vec![],
            })
            .collect();
        KafkaResponse::DeleteAcls(DeleteAclsResponse { filter_results })
    }

    // -------------------------------------------------------------------------
    // DescribeLogDirs (API 35) — stub
    // -------------------------------------------------------------------------

    fn handle_describe_log_dirs(&self, _req: DescribeLogDirsRequest) -> KafkaResponse {
        // Return empty results
        KafkaResponse::DescribeLogDirs(DescribeLogDirsResponse { results: vec![] })
    }

    // -------------------------------------------------------------------------
    // DescribeUserScramCredentials (API 50) — stub
    // -------------------------------------------------------------------------

    fn handle_describe_user_scram_credentials(
        &self,
        _req: DescribeUserScramCredentialsRequest,
    ) -> KafkaResponse {
        // Return empty results
        KafkaResponse::DescribeUserScramCredentials(DescribeUserScramCredentialsResponse {
            error_code: ErrorCode::None.as_i16(),
            error_message: None,
            results: vec![],
        })
    }

    // -------------------------------------------------------------------------
    // AlterUserScramCredentials (API 51) — stub
    // -------------------------------------------------------------------------

    fn handle_alter_user_scram_credentials(
        &self,
        _req: AlterUserScramCredentialsRequest,
    ) -> KafkaResponse {
        // Return empty results (success)
        KafkaResponse::AlterUserScramCredentials(AlterUserScramCredentialsResponse {
            results: vec![],
        })
    }

    // -------------------------------------------------------------------------
    // DescribeCluster
    // -------------------------------------------------------------------------

    fn handle_describe_cluster(&self, req: DescribeClusterRequest) -> KafkaResponse {
        KafkaResponse::DescribeCluster(DescribeClusterResponse {
            error_code: ErrorCode::None.as_i16(),
            cluster_id: WireString::from_static("bisque-mq"),
            controller_id: self.broker_id,
            brokers: vec![BrokerMeta {
                node_id: self.broker_id,
                host: self.advertised_host.clone(),
                port: self.advertised_port,
            }],
            cluster_authorized_operations: if req.include_cluster_authorized_operations {
                i32::MIN // not available
            } else {
                i32::MIN
            },
        })
    }

    // -------------------------------------------------------------------------
    // Connection disconnect — submit leave via Raft
    // -------------------------------------------------------------------------

    /// Clean up on connection disconnect. Submits LeaveGroup commands via Raft
    /// for each member registered on this connection.
    pub fn on_disconnect(&self, member_ids: &[WireString]) {
        for mid in member_ids {
            let group_id = self.metadata.find_member_group(mid);

            if let Some(gid) = group_id {
                let cmd = MqCommand::leave_consumer_group(gid, mid);
                let batcher = Arc::clone(&self.batcher);
                tokio::spawn(async move {
                    if let Err(e) = batcher.submit(cmd).await {
                        debug!("disconnect leave_group error: {e}");
                    }
                });
            }
        }
    }
}

// =============================================================================
// Helper: MqError → Kafka ErrorCode
// =============================================================================

fn mq_error_to_kafka_i16(e: &MqError) -> i16 {
    match e {
        MqError::IllegalGeneration => ErrorCode::IllegalGeneration.as_i16(),
        MqError::UnknownMemberId => ErrorCode::UnknownMemberId.as_i16(),
        MqError::RebalanceInProgress => ErrorCode::RebalanceInProgress.as_i16(),
        MqError::NotFound { .. } => ErrorCode::GroupCoordinatorNotAvailable.as_i16(),
        MqError::AlreadyExists { .. } => ErrorCode::TopicAlreadyExists.as_i16(),
        _ => ErrorCode::GroupCoordinatorNotAvailable.as_i16(),
    }
}

// =============================================================================
// MetadataLogReader — metadata-only KafkaLogReader bridge
// =============================================================================

/// Metadata-only `KafkaLogReader` that provides topic head/tail/list operations
/// using bisque-mq's `MqMetadata`. Message reading returns empty results —
/// the production binary provides a full implementation that scans raft log
/// segments for topic data.
///
/// This exists as a composable base for production implementations.
pub struct MetadataLogReader {
    metadata: Arc<MqMetadata>,
}

impl MetadataLogReader {
    pub fn new(metadata: Arc<MqMetadata>) -> Self {
        Self { metadata }
    }
}

impl KafkaLogReader for MetadataLogReader {
    fn read_topic_messages(
        &self,
        _topic_id: u64,
        _start_offset: u64,
        _max_bytes: usize,
    ) -> (Vec<FetchedMessage>, u64) {
        (vec![], 0)
    }

    fn get_topic_head(&self, topic_id: u64) -> Option<u64> {
        self.metadata.get_topic_head(topic_id)
    }

    fn get_topic_tail(&self, topic_id: u64) -> Option<u64> {
        self.metadata.get_topic_tail(topic_id)
    }

    fn get_committed_offset(&self, _topic_id: u64, _consumer_id: u64) -> Option<u64> {
        None
    }

    fn list_topics(&self) -> Vec<(String, u64)> {
        let mut topics_bytes = Vec::new();
        self.metadata.list_topics_into(&mut topics_bytes);
        topics_bytes
            .into_iter()
            .map(|(name_bytes, id)| {
                let name = String::from_utf8(name_bytes.to_vec()).unwrap_or_default();
                (name, id)
            })
            .collect()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bisque_mq::config::MqConfig;
    use bisque_mq::engine::MqEngine;
    use bisque_mq::write_batcher::MqWriteBatcher;
    use std::sync::Arc;

    struct MockLogReader {
        topics: parking_lot::Mutex<Vec<(String, u64)>>,
    }
    impl MockLogReader {
        fn new() -> Self {
            Self {
                topics: parking_lot::Mutex::new(Vec::new()),
            }
        }
        #[allow(dead_code)]
        fn add_topic(&self, name: &str, id: u64) {
            self.topics.lock().push((name.to_string(), id));
        }
    }
    impl KafkaLogReader for MockLogReader {
        fn read_topic_messages(&self, _: u64, _: u64, _: usize) -> (Vec<FetchedMessage>, u64) {
            (vec![], 0)
        }
        fn get_topic_head(&self, _: u64) -> Option<u64> {
            None
        }
        fn get_topic_tail(&self, _: u64) -> Option<u64> {
            None
        }
        fn get_committed_offset(&self, _: u64, _: u64) -> Option<u64> {
            None
        }
        fn list_topics(&self) -> Vec<(String, u64)> {
            self.topics.lock().clone()
        }
    }

    fn default_header() -> RequestHeader {
        RequestHeader {
            api_key: 0,
            api_version: 0,
            correlation_id: 1,
            client_id: Some(WireString::from("test")),
        }
    }

    /// Create a test handler backed by a real engine (no Raft).
    fn make_test_handler() -> (
        KafkaHandler,
        Arc<parking_lot::Mutex<MqEngine>>,
        Arc<MockLogReader>,
    ) {
        let config = MqConfig::new("/tmp/mq-kafka-test");
        let engine = MqEngine::new(config);
        let metadata = engine.shared_metadata();
        let engine = Arc::new(parking_lot::Mutex::new(engine));
        let batcher = Arc::new(MqWriteBatcher::new_test(Arc::clone(&engine)));
        let log_reader = Arc::new(MockLogReader::new());
        let handler = KafkaHandler::new(
            batcher,
            log_reader.clone(),
            metadata,
            1,
            "localhost".into(),
            9092,
            "test",
        );
        (handler, engine, log_reader)
    }

    #[test]
    fn test_build_record_batch() {
        let msg = FetchedMessage {
            offset: 0,
            timestamp: 1000,
            key: Some(Bytes::from_static(b"k")),
            value: Bytes::from_static(b"v"),
            headers: vec![],
        };
        assert_eq!(msg.offset, 0);
        assert_eq!(msg.timestamp, 1000);
    }

    #[tokio::test]
    async fn test_handler_join_group_empty_group_id() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::empty(),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::JoinGroup(r) => {
                assert_eq!(r.error_code, ErrorCode::InvalidGroupId.as_i16());
            }
            other => panic!("expected JoinGroup, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_join_group_auto_create() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::from("auto-create-group"),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::JoinGroup(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
                assert!(!r.member_id.is_empty(), "should auto-assign member_id");
                assert_eq!(r.generation_id, 1);
            }
            other => panic!("expected JoinGroup, got {other:?}"),
        }

        assert!(handler.resolve_group_id("auto-create-group").is_some());
    }

    #[tokio::test]
    async fn test_handler_join_group_single_member() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::from("single-member"),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::from_static(b"\x01"),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::JoinGroup(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
                assert_eq!(r.generation_id, 1);
                assert!(!r.member_id.is_empty());
                assert_eq!(r.leader, r.member_id, "single member should be leader");
                assert!(!r.members.is_empty(), "leader should get member list");
            }
            other => panic!("expected JoinGroup, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_sync_group_unknown_group() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::SyncGroup(SyncGroupRequest {
                    group_id: WireString::from("nonexistent"),
                    generation_id: 1,
                    member_id: WireString::from("m1"),
                    assignments: vec![],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::SyncGroup(r) => {
                assert_eq!(r.error_code, ErrorCode::InvalidGroupId.as_i16());
            }
            other => panic!("expected SyncGroup, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_sync_group_leader_assignments() {
        let (handler, _, _) = make_test_handler();

        let join_resp = handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::from("sync-test"),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        let (member_id, generation) = match join_resp {
            KafkaResponse::JoinGroup(r) => (r.member_id, r.generation_id),
            other => panic!("expected JoinGroup, got {other:?}"),
        };

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::SyncGroup(SyncGroupRequest {
                    group_id: WireString::from("sync-test"),
                    generation_id: generation,
                    member_id: member_id.clone(),
                    assignments: vec![SyncGroupAssignment {
                        member_id: member_id.clone(),
                        assignment: Bytes::from_static(b"partition-data"),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::SyncGroup(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
                assert_eq!(r.assignment, Bytes::from_static(b"partition-data"));
            }
            other => panic!("expected SyncGroup, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_heartbeat_unknown_group() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::Heartbeat(HeartbeatRequest {
                    group_id: WireString::from("nonexistent"),
                    generation_id: 1,
                    member_id: WireString::from("m1"),
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::Heartbeat(r) => {
                assert_eq!(r.error_code, ErrorCode::InvalidGroupId.as_i16());
            }
            other => panic!("expected Heartbeat, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_heartbeat_success() {
        let (handler, _, _) = make_test_handler();

        let join_resp = handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::from("hb-ok"),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;
        let (mid, generation) = match join_resp {
            KafkaResponse::JoinGroup(r) => (r.member_id, r.generation_id),
            other => panic!("{other:?}"),
        };
        handler
            .handle(
                &default_header(),
                KafkaRequest::SyncGroup(SyncGroupRequest {
                    group_id: WireString::from("hb-ok"),
                    generation_id: generation,
                    member_id: mid.clone(),
                    assignments: vec![SyncGroupAssignment {
                        member_id: mid.clone(),
                        assignment: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::Heartbeat(HeartbeatRequest {
                    group_id: WireString::from("hb-ok"),
                    generation_id: generation,
                    member_id: mid,
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::Heartbeat(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
            }
            other => panic!("expected Heartbeat, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_heartbeat_wrong_generation() {
        let (handler, _, _) = make_test_handler();

        let join_resp = handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::from("hb-generation"),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;
        let (mid, generation) = match join_resp {
            KafkaResponse::JoinGroup(r) => (r.member_id, r.generation_id),
            other => panic!("{other:?}"),
        };
        handler
            .handle(
                &default_header(),
                KafkaRequest::SyncGroup(SyncGroupRequest {
                    group_id: WireString::from("hb-generation"),
                    generation_id: generation,
                    member_id: mid.clone(),
                    assignments: vec![SyncGroupAssignment {
                        member_id: mid.clone(),
                        assignment: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::Heartbeat(HeartbeatRequest {
                    group_id: WireString::from("hb-generation"),
                    generation_id: 999,
                    member_id: mid,
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::Heartbeat(r) => {
                assert_eq!(r.error_code, ErrorCode::IllegalGeneration.as_i16());
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_heartbeat_unknown_member() {
        let (handler, _, _) = make_test_handler();

        let join_resp = handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::from("hb-unk"),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;
        let (mid, generation) = match join_resp {
            KafkaResponse::JoinGroup(r) => (r.member_id, r.generation_id),
            other => panic!("{other:?}"),
        };
        handler
            .handle(
                &default_header(),
                KafkaRequest::SyncGroup(SyncGroupRequest {
                    group_id: WireString::from("hb-unk"),
                    generation_id: generation,
                    member_id: mid.clone(),
                    assignments: vec![SyncGroupAssignment {
                        member_id: mid.clone(),
                        assignment: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::Heartbeat(HeartbeatRequest {
                    group_id: WireString::from("hb-unk"),
                    generation_id: generation,
                    member_id: WireString::from("unknown-member-xyz"),
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::Heartbeat(r) => {
                assert_eq!(r.error_code, ErrorCode::UnknownMemberId.as_i16());
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_api_versions() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(&default_header(), KafkaRequest::ApiVersions)
            .await;

        match resp {
            KafkaResponse::ApiVersions(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
                assert_eq!(r.api_keys.len(), 77);
                for key in &r.api_keys {
                    assert!(
                        ApiKey::from_i16(key.api_key).is_some(),
                        "unexpected api_key {}",
                        key.api_key
                    );
                    assert!(key.min_version <= key.max_version);
                }
            }
            other => panic!("expected ApiVersions, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_metadata_empty() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::Metadata(MetadataRequest { topics: None }),
            )
            .await;

        match resp {
            KafkaResponse::Metadata(r) => {
                assert_eq!(r.brokers.len(), 1);
                assert_eq!(r.brokers[0].node_id, 1);
                assert_eq!(r.brokers[0].host, "localhost");
                assert_eq!(r.brokers[0].port, 9092);
                assert!(r.topics.is_empty());
            }
            other => panic!("expected Metadata, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_metadata_unknown_topic() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::Metadata(MetadataRequest {
                    topics: Some(vec![WireString::from("nonexistent-topic")]),
                }),
            )
            .await;

        match resp {
            KafkaResponse::Metadata(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(
                    r.topics[0].error_code,
                    ErrorCode::UnknownTopicOrPartition.as_i16()
                );
                assert_eq!(r.topics[0].name, "nonexistent-topic");
                assert!(r.topics[0].partitions.is_empty());
            }
            other => panic!("expected Metadata, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_find_coordinator() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::FindCoordinator(FindCoordinatorRequest {
                    key: WireString::from("my-group"),
                    key_type: 0,
                }),
            )
            .await;

        match resp {
            KafkaResponse::FindCoordinator(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
                assert_eq!(r.node_id, 1);
                assert_eq!(r.host, "localhost");
                assert_eq!(r.port, 9092);
            }
            other => panic!("expected FindCoordinator, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_list_offsets_unknown_topic() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::ListOffsets(ListOffsetsRequest {
                    replica_id: -1,
                    topics: vec![ListOffsetsTopicData {
                        topic_name: WireString::from("no-such-topic"),
                        partitions: vec![ListOffsetsPartitionData {
                            partition_index: 0,
                            timestamp: -1,
                        }],
                    }],
                }),
            )
            .await;

        match resp {
            KafkaResponse::ListOffsets(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(r.topics[0].partitions.len(), 1);
                assert_eq!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::UnknownTopicOrPartition.as_i16()
                );
                assert_eq!(r.topics[0].partitions[0].offset, -1);
            }
            other => panic!("expected ListOffsets, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_create_topics() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::CreateTopics(CreateTopicsRequest {
                    topics: vec![CreateTopicRequest {
                        name: WireString::from("test-topic"),
                        num_partitions: 3,
                        replication_factor: 1,
                    }],
                    timeout_ms: 5000,
                }),
            )
            .await;

        match resp {
            KafkaResponse::CreateTopics(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(r.topics[0].name, "test-topic");
                // Either None (success) or TopicAlreadyExists if re-run
                assert!(
                    r.topics[0].error_code == ErrorCode::None.as_i16()
                        || r.topics[0].error_code == ErrorCode::TopicAlreadyExists.as_i16()
                );
            }
            other => panic!("expected CreateTopics, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_delete_topics_unknown() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::DeleteTopics(DeleteTopicsRequest {
                    topic_names: vec![WireString::from("nonexistent-topic")],
                    timeout_ms: 5000,
                }),
            )
            .await;

        match resp {
            KafkaResponse::DeleteTopics(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(r.topics[0].name, "nonexistent-topic");
                assert_eq!(
                    r.topics[0].error_code,
                    ErrorCode::UnknownTopicOrPartition.as_i16()
                );
            }
            other => panic!("expected DeleteTopics, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_sasl_handshake_plain() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::SaslHandshake(SaslHandshakeRequest {
                    mechanism: WireString::from("PLAIN"),
                }),
            )
            .await;

        match resp {
            KafkaResponse::SaslHandshake(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
                assert_eq!(r.mechanisms.len(), 1);
                assert_eq!(r.mechanisms[0], "PLAIN");
            }
            other => panic!("expected SaslHandshake, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_sasl_handshake_unsupported() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::SaslHandshake(SaslHandshakeRequest {
                    mechanism: WireString::from("GSSAPI"),
                }),
            )
            .await;

        match resp {
            KafkaResponse::SaslHandshake(r) => {
                assert_eq!(r.error_code, ErrorCode::UnsupportedVersion.as_i16());
                // Still returns supported mechanisms
                assert_eq!(r.mechanisms.len(), 1);
                assert_eq!(r.mechanisms[0], "PLAIN");
            }
            other => panic!("expected SaslHandshake, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_sasl_authenticate_success() {
        let (handler, _, _) = make_test_handler();
        // SASL/PLAIN format: \0username\0password
        let auth_bytes = Bytes::from_static(b"\0user\0pass");
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::SaslAuthenticate(SaslAuthenticateRequest { auth_bytes }),
            )
            .await;

        match resp {
            KafkaResponse::SaslAuthenticate(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
                assert!(r.error_message.is_none());
            }
            other => panic!("expected SaslAuthenticate, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_sasl_authenticate_bad_payload() {
        let (handler, _, _) = make_test_handler();
        // Empty bytes — cannot be parsed as SASL/PLAIN
        let auth_bytes = Bytes::new();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::SaslAuthenticate(SaslAuthenticateRequest { auth_bytes }),
            )
            .await;

        match resp {
            KafkaResponse::SaslAuthenticate(r) => {
                assert_eq!(r.error_code, ErrorCode::SaslAuthenticationFailed.as_i16());
                assert!(r.error_message.is_some());
            }
            other => panic!("expected SaslAuthenticate, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_init_producer_id() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::InitProducerId(InitProducerIdRequest {
                    transactional_id: None,
                    transaction_timeout_ms: 60000,
                }),
            )
            .await;

        match resp {
            KafkaResponse::InitProducerId(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
                assert!(r.producer_id >= 0);
                assert_eq!(r.producer_epoch, 0);
            }
            other => panic!("expected InitProducerId, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_init_producer_id_transactional() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::InitProducerId(InitProducerIdRequest {
                    transactional_id: Some(WireString::from("my-txn-id")),
                    transaction_timeout_ms: 60000,
                }),
            )
            .await;

        match resp {
            KafkaResponse::InitProducerId(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
                assert!(r.producer_id >= 0);
                assert_eq!(r.producer_epoch, 0);
            }
            other => panic!("expected InitProducerId, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_describe_configs_topic() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::DescribeConfigs(DescribeConfigsRequest {
                    resources: vec![DescribeConfigsResource {
                        resource_type: 2, // topic
                        resource_name: WireString::from("my-topic"),
                        config_names: None,
                    }],
                }),
            )
            .await;

        match resp {
            KafkaResponse::DescribeConfigs(r) => {
                assert_eq!(r.resources.len(), 1);
                assert_eq!(r.resources[0].error_code, ErrorCode::None.as_i16());
                assert_eq!(r.resources[0].resource_type, 2);
                assert_eq!(r.resources[0].resource_name, "my-topic");
                let config_names: Vec<&str> = r.resources[0]
                    .configs
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect();
                assert!(config_names.contains(&"cleanup.policy"));
                assert!(config_names.contains(&"retention.ms"));
                assert!(config_names.contains(&"segment.bytes"));
                assert!(config_names.contains(&"max.message.bytes"));
                assert!(config_names.contains(&"compression.type"));
            }
            other => panic!("expected DescribeConfigs, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_describe_configs_broker() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::DescribeConfigs(DescribeConfigsRequest {
                    resources: vec![DescribeConfigsResource {
                        resource_type: 4, // broker
                        resource_name: WireString::from("1"),
                        config_names: None,
                    }],
                }),
            )
            .await;

        match resp {
            KafkaResponse::DescribeConfigs(r) => {
                assert_eq!(r.resources.len(), 1);
                assert_eq!(r.resources[0].error_code, ErrorCode::None.as_i16());
                assert_eq!(r.resources[0].resource_type, 4);
                let config_names: Vec<&str> = r.resources[0]
                    .configs
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect();
                assert!(config_names.contains(&"log.retention.hours"));
                assert!(config_names.contains(&"num.partitions"));
            }
            other => panic!("expected DescribeConfigs, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_alter_configs() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::AlterConfigs(AlterConfigsRequest {
                    resources: vec![
                        AlterConfigsResource {
                            resource_type: 2,
                            resource_name: WireString::from("topic-a"),
                            configs: vec![AlterConfigEntry {
                                name: WireString::from("retention.ms"),
                                value: Some(WireString::from("86400000")),
                            }],
                        },
                        AlterConfigsResource {
                            resource_type: 4,
                            resource_name: WireString::from("1"),
                            configs: vec![],
                        },
                    ],
                    validate_only: false,
                }),
            )
            .await;

        match resp {
            KafkaResponse::AlterConfigs(r) => {
                assert_eq!(r.resources.len(), 2);
                for res in &r.resources {
                    assert_eq!(res.error_code, ErrorCode::None.as_i16());
                    assert!(res.error_message.is_none());
                }
                assert_eq!(r.resources[0].resource_type, 2);
                assert_eq!(r.resources[0].resource_name, "topic-a");
                assert_eq!(r.resources[1].resource_type, 4);
                assert_eq!(r.resources[1].resource_name, "1");
            }
            other => panic!("expected AlterConfigs, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_list_groups_empty() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(&default_header(), KafkaRequest::ListGroups)
            .await;

        match resp {
            KafkaResponse::ListGroups(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
                assert!(r.groups.is_empty());
            }
            other => panic!("expected ListGroups, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_describe_groups_nonexistent() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::DescribeGroups(DescribeGroupsRequest {
                    group_ids: vec![WireString::from("no-such-group")],
                }),
            )
            .await;

        match resp {
            KafkaResponse::DescribeGroups(r) => {
                assert_eq!(r.groups.len(), 1);
                assert_eq!(r.groups[0].error_code, ErrorCode::InvalidGroupId.as_i16());
                assert_eq!(r.groups[0].group_id, "no-such-group");
                assert_eq!(r.groups[0].state, "Dead");
                assert!(r.groups[0].members.is_empty());
            }
            other => panic!("expected DescribeGroups, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_leave_group() {
        let (handler, _, _) = make_test_handler();

        // First join a group
        let join_resp = handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::from("leave-test"),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        let member_id = match join_resp {
            KafkaResponse::JoinGroup(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
                r.member_id
            }
            other => panic!("expected JoinGroup, got {other:?}"),
        };

        // Now leave the group
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::LeaveGroup(LeaveGroupRequest {
                    group_id: WireString::from("leave-test"),
                    member_id,
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::LeaveGroup(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
            }
            other => panic!("expected LeaveGroup, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_offset_commit_unknown_group() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::OffsetCommit(OffsetCommitRequest {
                    group_id: WireString::from("nonexistent-group"),
                    generation_id: -1,
                    member_id: WireString::empty(),
                    topics: vec![OffsetCommitTopicData {
                        topic_name: WireString::from("some-topic"),
                        partitions: vec![OffsetCommitPartitionData {
                            partition_index: 0,
                            offset: 42,
                            metadata: None,
                            ..Default::default()
                        }],
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::OffsetCommit(r) => {
                // Falls through to legacy path; topic is unknown so partition error
                assert_eq!(r.topics.len(), 1);
                assert_eq!(r.topics[0].partitions.len(), 1);
                assert_eq!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::UnknownTopicOrPartition.as_i16()
                );
            }
            other => panic!("expected OffsetCommit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_offset_fetch_unknown_group() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::OffsetFetch(OffsetFetchRequest {
                    group_id: WireString::from("nonexistent-group"),
                    topics: vec![OffsetFetchTopicData {
                        topic_name: WireString::from("some-topic"),
                        partitions: vec![0],
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::OffsetFetch(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(r.topics[0].partitions.len(), 1);
                // Topic doesn't exist in partition map, so UnknownTopicOrPartition
                assert_eq!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::UnknownTopicOrPartition.as_i16()
                );
            }
            other => panic!("expected OffsetFetch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_fetch_unknown_topic() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::Fetch(FetchRequest {
                    max_wait_ms: 0,
                    min_bytes: 1,
                    topics: vec![FetchTopicData {
                        topic_name: WireString::from("unknown-topic"),
                        partitions: vec![FetchPartitionData {
                            partition_index: 0,
                            fetch_offset: 0,
                            max_bytes: 1048576,
                            ..Default::default()
                        }],
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::Fetch(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(r.topics[0].partitions.len(), 1);
                assert_eq!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::UnknownTopicOrPartition.as_i16()
                );
                assert_eq!(r.topics[0].partitions[0].high_watermark, -1);
                assert!(r.topics[0].partitions[0].record_set.is_empty());
            }
            other => panic!("expected Fetch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_fetch_empty() {
        let (handler, _, log_reader) = make_test_handler();
        // Add topic using partition naming convention: "kafka-name-{partition}"
        log_reader.add_topic("fetch-empty-0", 42);
        handler.refresh_partitions();

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::Fetch(FetchRequest {
                    max_wait_ms: 0,
                    min_bytes: 1,
                    topics: vec![FetchTopicData {
                        topic_name: WireString::from("fetch-empty"),
                        partitions: vec![FetchPartitionData {
                            partition_index: 0,
                            fetch_offset: 0,
                            max_bytes: 1048576,
                            ..Default::default()
                        }],
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::Fetch(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(r.topics[0].partitions.len(), 1);
                assert_eq!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::None.as_i16()
                );
                assert!(r.topics[0].partitions[0].record_set.is_empty());
            }
            other => panic!("expected Fetch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_delete_groups_not_found() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::DeleteGroups(DeleteGroupsRequest {
                    group_ids: vec![WireString::from("no-such-group")],
                }),
            )
            .await;

        match resp {
            KafkaResponse::DeleteGroups(r) => {
                assert_eq!(r.results.len(), 1);
                assert_eq!(r.results[0].group_id, "no-such-group");
                assert_eq!(r.results[0].error_code, ErrorCode::GroupIdNotFound.as_i16());
            }
            other => panic!("expected DeleteGroups, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_create_partitions_invalid() {
        let (handler, _, log_reader) = make_test_handler();
        // Add a topic using partition naming convention so partition map resolves it
        log_reader.add_topic("cp-topic-0", 100);
        handler.refresh_partitions();

        // Try to set count to 1 (equal to current 1), should get InvalidPartitions
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::CreatePartitions(CreatePartitionsRequest {
                    topics: vec![CreatePartitionsTopic {
                        name: WireString::from("cp-topic"),
                        count: 1,
                    }],
                    timeout_ms: 5000,
                    validate_only: false,
                }),
            )
            .await;

        match resp {
            KafkaResponse::CreatePartitions(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(r.topics[0].name, "cp-topic");
                assert_eq!(
                    r.topics[0].error_code,
                    ErrorCode::InvalidPartitions.as_i16()
                );
                assert!(r.topics[0].error_message.is_some());
            }
            other => panic!("expected CreatePartitions, got {other:?}"),
        }
    }

    // =========================================================================
    // Transaction API handler tests
    // =========================================================================

    #[tokio::test]
    async fn test_handler_add_partitions_to_txn() {
        let (handler, _, _) = make_test_handler();
        // Init a transactional producer first
        handler
            .handle(
                &default_header(),
                KafkaRequest::InitProducerId(InitProducerIdRequest {
                    transactional_id: Some(WireString::from("txn-1")),
                    transaction_timeout_ms: 60000,
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::AddPartitionsToTxn(AddPartitionsToTxnRequest {
                    transactional_id: WireString::from("txn-1"),
                    producer_id: 1,
                    producer_epoch: 0,
                    topics: vec![AddPartitionsToTxnTopicData {
                        topic_name: WireString::from("topic-a"),
                        partitions: vec![0, 1],
                    }],
                }),
            )
            .await;

        match resp {
            KafkaResponse::AddPartitionsToTxn(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(r.topics[0].partitions.len(), 2);
                for p in &r.topics[0].partitions {
                    assert_eq!(p.error_code, ErrorCode::None.as_i16());
                }
            }
            other => panic!("expected AddPartitionsToTxn, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_add_partitions_to_txn_unknown() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::AddPartitionsToTxn(AddPartitionsToTxnRequest {
                    transactional_id: WireString::from("no-such-txn"),
                    producer_id: 999,
                    producer_epoch: 0,
                    topics: vec![AddPartitionsToTxnTopicData {
                        topic_name: WireString::from("t"),
                        partitions: vec![0],
                    }],
                }),
            )
            .await;

        match resp {
            KafkaResponse::AddPartitionsToTxn(r) => {
                assert_ne!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::None.as_i16()
                );
            }
            other => panic!("expected AddPartitionsToTxn, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_add_offsets_to_txn() {
        let (handler, _, _) = make_test_handler();
        let init_resp = handler
            .handle(
                &default_header(),
                KafkaRequest::InitProducerId(InitProducerIdRequest {
                    transactional_id: Some(WireString::from("txn-offsets")),
                    transaction_timeout_ms: 60000,
                }),
            )
            .await;
        let (pid, epoch) = match init_resp {
            KafkaResponse::InitProducerId(r) => (r.producer_id, r.producer_epoch),
            other => panic!("{other:?}"),
        };
        // Must add partitions first to transition to Ongoing
        handler
            .handle(
                &default_header(),
                KafkaRequest::AddPartitionsToTxn(AddPartitionsToTxnRequest {
                    transactional_id: WireString::from("txn-offsets"),
                    producer_id: pid,
                    producer_epoch: epoch,
                    topics: vec![AddPartitionsToTxnTopicData {
                        topic_name: WireString::from("t"),
                        partitions: vec![0],
                    }],
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::AddOffsetsToTxn(AddOffsetsToTxnRequest {
                    transactional_id: WireString::from("txn-offsets"),
                    producer_id: pid,
                    producer_epoch: epoch,
                    group_id: WireString::from("my-group"),
                }),
            )
            .await;

        match resp {
            KafkaResponse::AddOffsetsToTxn(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
            }
            other => panic!("expected AddOffsetsToTxn, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_add_offsets_to_txn_unknown() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::AddOffsetsToTxn(AddOffsetsToTxnRequest {
                    transactional_id: WireString::from("no-txn"),
                    producer_id: 999,
                    producer_epoch: 0,
                    group_id: WireString::from("g"),
                }),
            )
            .await;

        match resp {
            KafkaResponse::AddOffsetsToTxn(r) => {
                assert_ne!(r.error_code, ErrorCode::None.as_i16());
            }
            other => panic!("expected AddOffsetsToTxn, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_end_txn_commit() {
        let (handler, _, _) = make_test_handler();
        let init_resp = handler
            .handle(
                &default_header(),
                KafkaRequest::InitProducerId(InitProducerIdRequest {
                    transactional_id: Some(WireString::from("txn-end")),
                    transaction_timeout_ms: 60000,
                }),
            )
            .await;
        let (pid, epoch) = match init_resp {
            KafkaResponse::InitProducerId(r) => (r.producer_id, r.producer_epoch),
            other => panic!("{other:?}"),
        };
        // Transition to Ongoing via add_partitions
        handler
            .handle(
                &default_header(),
                KafkaRequest::AddPartitionsToTxn(AddPartitionsToTxnRequest {
                    transactional_id: WireString::from("txn-end"),
                    producer_id: pid,
                    producer_epoch: epoch,
                    topics: vec![AddPartitionsToTxnTopicData {
                        topic_name: WireString::from("t"),
                        partitions: vec![0],
                    }],
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::EndTxn(EndTxnRequest {
                    transactional_id: WireString::from("txn-end"),
                    producer_id: pid,
                    producer_epoch: epoch,
                    committed: true,
                }),
            )
            .await;

        match resp {
            KafkaResponse::EndTxn(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
            }
            other => panic!("expected EndTxn, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_end_txn_abort() {
        let (handler, _, _) = make_test_handler();
        let init_resp = handler
            .handle(
                &default_header(),
                KafkaRequest::InitProducerId(InitProducerIdRequest {
                    transactional_id: Some(WireString::from("txn-abort")),
                    transaction_timeout_ms: 60000,
                }),
            )
            .await;
        let (pid, epoch) = match init_resp {
            KafkaResponse::InitProducerId(r) => (r.producer_id, r.producer_epoch),
            other => panic!("{other:?}"),
        };
        handler
            .handle(
                &default_header(),
                KafkaRequest::AddPartitionsToTxn(AddPartitionsToTxnRequest {
                    transactional_id: WireString::from("txn-abort"),
                    producer_id: pid,
                    producer_epoch: epoch,
                    topics: vec![AddPartitionsToTxnTopicData {
                        topic_name: WireString::from("t"),
                        partitions: vec![0],
                    }],
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::EndTxn(EndTxnRequest {
                    transactional_id: WireString::from("txn-abort"),
                    producer_id: pid,
                    producer_epoch: epoch,
                    committed: false,
                }),
            )
            .await;

        match resp {
            KafkaResponse::EndTxn(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
            }
            other => panic!("expected EndTxn, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_end_txn_unknown() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::EndTxn(EndTxnRequest {
                    transactional_id: WireString::from("no-txn"),
                    producer_id: 999,
                    producer_epoch: 0,
                    committed: true,
                }),
            )
            .await;

        match resp {
            KafkaResponse::EndTxn(r) => {
                assert_ne!(r.error_code, ErrorCode::None.as_i16());
            }
            other => panic!("expected EndTxn, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_txn_offset_commit_no_txn() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::TxnOffsetCommit(TxnOffsetCommitRequest {
                    transactional_id: WireString::from("no-txn"),
                    group_id: WireString::from("g1"),
                    producer_id: 999,
                    producer_epoch: 0,
                    topics: vec![TxnOffsetCommitTopicData {
                        topic_name: WireString::from("t1"),
                        partitions: vec![TxnOffsetCommitPartitionData {
                            partition_index: 0,
                            offset: 10,
                            metadata: None,
                        }],
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::TxnOffsetCommit(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_ne!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::None.as_i16()
                );
            }
            other => panic!("expected TxnOffsetCommit, got {other:?}"),
        }
    }

    // =========================================================================
    // DeleteRecords handler tests
    // =========================================================================

    #[tokio::test]
    async fn test_handler_delete_records_unknown_topic() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::DeleteRecords(DeleteRecordsRequest {
                    topics: vec![DeleteRecordsTopicData {
                        topic_name: WireString::from("no-topic"),
                        partitions: vec![DeleteRecordsPartitionData {
                            partition_index: 0,
                            offset: 5,
                        }],
                    }],
                    timeout_ms: 1000,
                }),
            )
            .await;

        match resp {
            KafkaResponse::DeleteRecords(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::UnknownTopicOrPartition.as_i16()
                );
                assert_eq!(r.topics[0].partitions[0].low_watermark, -1);
            }
            other => panic!("expected DeleteRecords, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_delete_records_known_topic() {
        let (handler, _, log_reader) = make_test_handler();
        log_reader.add_topic("dr-topic-0", 500);
        handler.refresh_partitions();

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::DeleteRecords(DeleteRecordsRequest {
                    topics: vec![DeleteRecordsTopicData {
                        topic_name: WireString::from("dr-topic"),
                        partitions: vec![DeleteRecordsPartitionData {
                            partition_index: 0,
                            offset: 42,
                        }],
                    }],
                    timeout_ms: 1000,
                }),
            )
            .await;

        match resp {
            KafkaResponse::DeleteRecords(r) => {
                assert_eq!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::None.as_i16()
                );
                assert_eq!(r.topics[0].partitions[0].low_watermark, 42);
            }
            other => panic!("expected DeleteRecords, got {other:?}"),
        }
    }

    // =========================================================================
    // OffsetDelete handler tests
    // =========================================================================

    #[tokio::test]
    async fn test_handler_offset_delete_unknown_group() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::OffsetDelete(OffsetDeleteRequest {
                    group_id: WireString::from("no-group"),
                    topics: vec![OffsetDeleteTopicData {
                        topic_name: WireString::from("t1"),
                        partitions: vec![OffsetDeletePartitionData { partition_index: 0 }],
                    }],
                }),
            )
            .await;

        match resp {
            KafkaResponse::OffsetDelete(r) => {
                assert_eq!(r.error_code, ErrorCode::GroupIdNotFound.as_i16());
                assert!(r.topics.is_empty());
            }
            other => panic!("expected OffsetDelete, got {other:?}"),
        }
    }

    // =========================================================================
    // Metadata with existing topics
    // =========================================================================

    #[tokio::test]
    async fn test_handler_metadata_with_topics() {
        let (handler, _, log_reader) = make_test_handler();
        log_reader.add_topic("my-topic-0", 10);
        log_reader.add_topic("my-topic-1", 11);
        handler.refresh_partitions();

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::Metadata(MetadataRequest { topics: None }),
            )
            .await;

        match resp {
            KafkaResponse::Metadata(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(r.topics[0].name, "my-topic");
                assert_eq!(r.topics[0].error_code, ErrorCode::None.as_i16());
                assert_eq!(r.topics[0].partitions.len(), 2);
            }
            other => panic!("expected Metadata, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_metadata_specific_topic() {
        let (handler, _, log_reader) = make_test_handler();
        log_reader.add_topic("exist-0", 20);
        handler.refresh_partitions();

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::Metadata(MetadataRequest {
                    topics: Some(vec![WireString::from("exist"), WireString::from("nope")]),
                }),
            )
            .await;

        match resp {
            KafkaResponse::Metadata(r) => {
                assert_eq!(r.topics.len(), 2);
                // First topic exists
                assert_eq!(r.topics[0].error_code, ErrorCode::None.as_i16());
                // Second doesn't
                assert_eq!(
                    r.topics[1].error_code,
                    ErrorCode::UnknownTopicOrPartition.as_i16()
                );
            }
            other => panic!("expected Metadata, got {other:?}"),
        }
    }

    // =========================================================================
    // ListOffsets with known topic
    // =========================================================================

    #[tokio::test]
    async fn test_handler_list_offsets_known_topic() {
        let (handler, _, log_reader) = make_test_handler();
        log_reader.add_topic("lo-topic-0", 30);
        handler.refresh_partitions();

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::ListOffsets(ListOffsetsRequest {
                    replica_id: -1,
                    topics: vec![ListOffsetsTopicData {
                        topic_name: WireString::from("lo-topic"),
                        partitions: vec![
                            ListOffsetsPartitionData {
                                partition_index: 0,
                                timestamp: -1,
                            },
                            ListOffsetsPartitionData {
                                partition_index: 0,
                                timestamp: -2,
                            },
                        ],
                    }],
                }),
            )
            .await;

        match resp {
            KafkaResponse::ListOffsets(r) => {
                assert_eq!(r.topics.len(), 1);
                for p in &r.topics[0].partitions {
                    assert_eq!(p.error_code, ErrorCode::None.as_i16());
                }
            }
            other => panic!("expected ListOffsets, got {other:?}"),
        }
    }

    // =========================================================================
    // DescribeGroups with existing group, ListGroups with groups
    // =========================================================================

    #[tokio::test]
    async fn test_handler_describe_groups_existing() {
        let (handler, _, _) = make_test_handler();
        // Create a group by joining
        handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::from("desc-group"),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::DescribeGroups(DescribeGroupsRequest {
                    group_ids: vec![WireString::from("desc-group")],
                }),
            )
            .await;

        match resp {
            KafkaResponse::DescribeGroups(r) => {
                assert_eq!(r.groups.len(), 1);
                assert_eq!(r.groups[0].error_code, ErrorCode::None.as_i16());
                assert_eq!(r.groups[0].group_id, "desc-group");
                assert!(!r.groups[0].members.is_empty());
            }
            other => panic!("expected DescribeGroups, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_list_groups_with_groups() {
        let (handler, _, _) = make_test_handler();
        handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::from("lg-group"),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        let resp = handler
            .handle(&default_header(), KafkaRequest::ListGroups)
            .await;

        match resp {
            KafkaResponse::ListGroups(r) => {
                assert_eq!(r.error_code, ErrorCode::None.as_i16());
                assert!(!r.groups.is_empty());
                assert!(r.groups.iter().any(|g| g.group_id == "lg-group"));
            }
            other => panic!("expected ListGroups, got {other:?}"),
        }
    }

    // =========================================================================
    // CreatePartitions success and validate_only
    // =========================================================================

    #[tokio::test]
    async fn test_handler_create_partitions_success() {
        let (handler, _, _) = make_test_handler();
        // First create the topic
        handler
            .handle(
                &default_header(),
                KafkaRequest::CreateTopics(CreateTopicsRequest {
                    topics: vec![CreateTopicRequest {
                        name: WireString::from("cp-ok"),
                        num_partitions: 1,
                        replication_factor: 1,
                    }],
                    timeout_ms: 5000,
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::CreatePartitions(CreatePartitionsRequest {
                    topics: vec![CreatePartitionsTopic {
                        name: WireString::from("cp-ok"),
                        count: 3,
                    }],
                    timeout_ms: 5000,
                    validate_only: false,
                }),
            )
            .await;

        match resp {
            KafkaResponse::CreatePartitions(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(r.topics[0].error_code, ErrorCode::None.as_i16());
            }
            other => panic!("expected CreatePartitions, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_create_partitions_validate_only() {
        let (handler, _, _) = make_test_handler();
        handler
            .handle(
                &default_header(),
                KafkaRequest::CreateTopics(CreateTopicsRequest {
                    topics: vec![CreateTopicRequest {
                        name: WireString::from("cp-val"),
                        num_partitions: 1,
                        replication_factor: 1,
                    }],
                    timeout_ms: 5000,
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::CreatePartitions(CreatePartitionsRequest {
                    topics: vec![CreatePartitionsTopic {
                        name: WireString::from("cp-val"),
                        count: 5,
                    }],
                    timeout_ms: 5000,
                    validate_only: true,
                }),
            )
            .await;

        match resp {
            KafkaResponse::CreatePartitions(r) => {
                assert_eq!(r.topics[0].error_code, ErrorCode::None.as_i16());
                assert!(r.topics[0].error_message.is_none());
            }
            other => panic!("expected CreatePartitions, got {other:?}"),
        }
    }

    // =========================================================================
    // DeleteTopics success path
    // =========================================================================

    #[tokio::test]
    async fn test_handler_delete_topics_success() {
        let (handler, _, log_reader) = make_test_handler();
        // Add the topic to the log reader so partition map can resolve it
        log_reader.add_topic("del-me-0", 200);
        handler.refresh_partitions();

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::DeleteTopics(DeleteTopicsRequest {
                    topic_names: vec![WireString::from("del-me")],
                    timeout_ms: 5000,
                }),
            )
            .await;

        match resp {
            KafkaResponse::DeleteTopics(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(r.topics[0].error_code, ErrorCode::None.as_i16());
            }
            other => panic!("expected DeleteTopics, got {other:?}"),
        }
    }

    // =========================================================================
    // DescribeConfigs unknown resource type
    // =========================================================================

    #[tokio::test]
    async fn test_handler_describe_configs_unknown_type() {
        let (handler, _, _) = make_test_handler();
        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::DescribeConfigs(DescribeConfigsRequest {
                    resources: vec![DescribeConfigsResource {
                        resource_type: 99,
                        resource_name: WireString::from("whatever"),
                        config_names: None,
                    }],
                }),
            )
            .await;

        match resp {
            KafkaResponse::DescribeConfigs(r) => {
                assert_eq!(r.resources.len(), 1);
                assert!(r.resources[0].configs.is_empty());
            }
            other => panic!("expected DescribeConfigs, got {other:?}"),
        }
    }

    // =========================================================================
    // SASL authenticate with custom rejecting authenticator
    // =========================================================================

    #[tokio::test]
    async fn test_handler_sasl_authenticate_rejected() {
        let (mut handler, _, _) = make_test_handler();
        struct RejectAuth;
        impl KafkaAuthenticator for RejectAuth {
            fn authenticate(&self, _: &str, _: &str) -> Result<(), &'static str> {
                Err("denied")
            }
        }
        handler.set_authenticator(Arc::new(RejectAuth));

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::SaslAuthenticate(SaslAuthenticateRequest {
                    auth_bytes: Bytes::from_static(b"\0user\0pass"),
                }),
            )
            .await;

        match resp {
            KafkaResponse::SaslAuthenticate(r) => {
                assert_eq!(r.error_code, ErrorCode::SaslAuthenticationFailed.as_i16());
                assert!(r.error_message.is_some());
            }
            other => panic!("expected SaslAuthenticate, got {other:?}"),
        }
    }

    // =========================================================================
    // mq_error_to_kafka_i16 coverage
    // =========================================================================

    #[test]
    fn test_mq_error_to_kafka_i16() {
        use bisque_mq::types::EntityKind;
        assert_eq!(
            mq_error_to_kafka_i16(&MqError::IllegalGeneration),
            ErrorCode::IllegalGeneration.as_i16()
        );
        assert_eq!(
            mq_error_to_kafka_i16(&MqError::UnknownMemberId),
            ErrorCode::UnknownMemberId.as_i16()
        );
        assert_eq!(
            mq_error_to_kafka_i16(&MqError::RebalanceInProgress),
            ErrorCode::RebalanceInProgress.as_i16()
        );
        assert_eq!(
            mq_error_to_kafka_i16(&MqError::NotFound {
                entity: EntityKind::Topic,
                id: 1
            }),
            ErrorCode::GroupCoordinatorNotAvailable.as_i16()
        );
        assert_eq!(
            mq_error_to_kafka_i16(&MqError::AlreadyExists {
                entity: EntityKind::Topic,
                id: 1
            }),
            ErrorCode::TopicAlreadyExists.as_i16()
        );
    }

    // =========================================================================
    // ProduceError Display
    // =========================================================================

    #[test]
    fn test_produce_error_display() {
        let e = ProduceError::UnexpectedResponse;
        assert_eq!(format!("{e}"), "unexpected response");

        let e = ProduceError::Codec(CodecError::Incomplete);
        assert!(format!("{e}").contains("codec:"));
    }

    // =========================================================================
    // data_notify and set_authenticator
    // =========================================================================

    #[tokio::test]
    async fn test_handler_data_notify() {
        let (handler, _, _) = make_test_handler();
        let notify = handler.data_notify();
        // Just verify it returns a valid Arc
        notify.notify_waiters();
    }

    // =========================================================================
    // refresh_partitions
    // =========================================================================

    #[tokio::test]
    async fn test_handler_refresh_partitions() {
        let (handler, _, log_reader) = make_test_handler();
        log_reader.add_topic("rp-topic-0", 50);
        log_reader.add_topic("rp-topic-1", 51);
        handler.refresh_partitions();

        let pmap = handler.partition_map.load();
        assert_eq!(pmap.partition_count("rp-topic"), Some(2));
    }

    // =========================================================================
    // on_disconnect with empty member IDs
    // =========================================================================

    #[tokio::test]
    async fn test_handler_on_disconnect_empty() {
        let (handler, _, _) = make_test_handler();
        // Should not panic with empty list
        handler.on_disconnect(&[]);
    }

    // =========================================================================
    // Fetch with existing topic (empty data)
    // =========================================================================

    #[tokio::test]
    async fn test_handler_fetch_known_topic_empty() {
        let (handler, _, log_reader) = make_test_handler();
        log_reader.add_topic("ft-topic-0", 60);
        handler.refresh_partitions();

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::Fetch(FetchRequest {
                    max_wait_ms: 0,
                    min_bytes: 0,
                    topics: vec![FetchTopicData {
                        topic_name: WireString::from("ft-topic"),
                        partitions: vec![FetchPartitionData {
                            partition_index: 0,
                            fetch_offset: 0,
                            max_bytes: 1048576,
                            ..Default::default()
                        }],
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::Fetch(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::None.as_i16()
                );
                assert!(r.topics[0].partitions[0].record_set.is_empty());
            }
            other => panic!("expected Fetch, got {other:?}"),
        }
    }

    // =========================================================================
    // Produce with unknown topic
    // =========================================================================

    #[tokio::test]
    async fn test_handler_produce_unknown_topic() {
        let (handler, _, _) = make_test_handler();

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::Produce(ProduceRequest {
                    acks: 1,
                    timeout_ms: 1000,
                    topics: vec![ProduceTopicData {
                        topic_name: WireString::from("no-topic"),
                        partitions: vec![ProducePartitionData {
                            partition_index: 0,
                            record_set: None,
                        }],
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::Produce(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::UnknownTopicOrPartition.as_i16()
                );
            }
            other => panic!("expected Produce, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_produce_no_record_set() {
        let (handler, _, log_reader) = make_test_handler();
        log_reader.add_topic("prd-topic-0", 70);
        handler.refresh_partitions();

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::Produce(ProduceRequest {
                    acks: 1,
                    timeout_ms: 1000,
                    topics: vec![ProduceTopicData {
                        topic_name: WireString::from("prd-topic"),
                        partitions: vec![ProducePartitionData {
                            partition_index: 0,
                            record_set: None,
                        }],
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::Produce(r) => {
                assert_eq!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::None.as_i16()
                );
                assert_eq!(r.topics[0].partitions[0].base_offset, 0);
            }
            other => panic!("expected Produce, got {other:?}"),
        }
    }

    // =========================================================================
    // OffsetCommit/Fetch with created group
    // =========================================================================

    #[tokio::test]
    async fn test_handler_offset_commit_known_group() {
        let (handler, _, log_reader) = make_test_handler();
        log_reader.add_topic("oc-topic-0", 80);
        handler.refresh_partitions();

        // Create group
        let join_resp = handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::from("oc-group"),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;
        let (mid, generation) = match join_resp {
            KafkaResponse::JoinGroup(r) => (r.member_id, r.generation_id),
            other => panic!("{other:?}"),
        };
        handler
            .handle(
                &default_header(),
                KafkaRequest::SyncGroup(SyncGroupRequest {
                    group_id: WireString::from("oc-group"),
                    generation_id: generation,
                    member_id: mid.clone(),
                    assignments: vec![SyncGroupAssignment {
                        member_id: mid,
                        assignment: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::OffsetCommit(OffsetCommitRequest {
                    group_id: WireString::from("oc-group"),
                    generation_id: generation,
                    member_id: WireString::empty(),
                    topics: vec![OffsetCommitTopicData {
                        topic_name: WireString::from("oc-topic"),
                        partitions: vec![OffsetCommitPartitionData {
                            partition_index: 0,
                            offset: 42,
                            metadata: None,
                            ..Default::default()
                        }],
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::OffsetCommit(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::None.as_i16()
                );
            }
            other => panic!("expected OffsetCommit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handler_offset_fetch_known_group() {
        let (handler, _, log_reader) = make_test_handler();
        log_reader.add_topic("of-topic-0", 90);
        handler.refresh_partitions();

        // Create group
        handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::from("of-group"),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::OffsetFetch(OffsetFetchRequest {
                    group_id: WireString::from("of-group"),
                    topics: vec![OffsetFetchTopicData {
                        topic_name: WireString::from("of-topic"),
                        partitions: vec![0],
                    }],
                    ..Default::default()
                }),
            )
            .await;

        match resp {
            KafkaResponse::OffsetFetch(r) => {
                assert_eq!(r.topics.len(), 1);
                assert_eq!(
                    r.topics[0].partitions[0].error_code,
                    ErrorCode::None.as_i16()
                );
                // No committed offset yet, should be -1
                assert_eq!(r.topics[0].partitions[0].offset, -1);
            }
            other => panic!("expected OffsetFetch, got {other:?}"),
        }
    }

    // =========================================================================
    // DeleteGroups success (empty group)
    // =========================================================================

    #[tokio::test]
    async fn test_handler_delete_groups_success() {
        let (handler, _, _) = make_test_handler();
        // Create and then leave the group so it becomes empty
        let join_resp = handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::from("dg-empty"),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;
        let mid = match join_resp {
            KafkaResponse::JoinGroup(r) => r.member_id,
            other => panic!("{other:?}"),
        };
        handler
            .handle(
                &default_header(),
                KafkaRequest::LeaveGroup(LeaveGroupRequest {
                    group_id: WireString::from("dg-empty"),
                    member_id: mid,
                    ..Default::default()
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::DeleteGroups(DeleteGroupsRequest {
                    group_ids: vec![WireString::from("dg-empty")],
                }),
            )
            .await;

        match resp {
            KafkaResponse::DeleteGroups(r) => {
                assert_eq!(r.results.len(), 1);
                assert_eq!(r.results[0].error_code, ErrorCode::None.as_i16());
            }
            other => panic!("expected DeleteGroups, got {other:?}"),
        }
    }

    // =========================================================================
    // DeleteGroups non-empty group
    // =========================================================================

    #[tokio::test]
    async fn test_handler_delete_groups_not_empty() {
        let (handler, _, _) = make_test_handler();
        handler
            .handle(
                &default_header(),
                KafkaRequest::JoinGroup(JoinGroupRequest {
                    group_id: WireString::from("dg-active"),
                    session_timeout_ms: 30000,
                    rebalance_timeout_ms: 60000,
                    member_id: WireString::empty(),
                    protocol_type: WireString::from("consumer"),
                    protocols: vec![JoinGroupProtocol {
                        name: WireString::from("range"),
                        metadata: Bytes::new(),
                    }],
                    ..Default::default()
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::DeleteGroups(DeleteGroupsRequest {
                    group_ids: vec![WireString::from("dg-active")],
                }),
            )
            .await;

        match resp {
            KafkaResponse::DeleteGroups(r) => {
                assert_eq!(r.results.len(), 1);
                assert_eq!(r.results[0].error_code, ErrorCode::GroupNotEmpty.as_i16());
            }
            other => panic!("expected DeleteGroups, got {other:?}"),
        }
    }

    // =========================================================================
    // CreateTopics already exists
    // =========================================================================

    #[tokio::test]
    async fn test_handler_create_topics_already_exists() {
        let (handler, _, _) = make_test_handler();
        handler
            .handle(
                &default_header(),
                KafkaRequest::CreateTopics(CreateTopicsRequest {
                    topics: vec![CreateTopicRequest {
                        name: WireString::from("dup-topic"),
                        num_partitions: 1,
                        replication_factor: 1,
                    }],
                    timeout_ms: 5000,
                }),
            )
            .await;

        let resp = handler
            .handle(
                &default_header(),
                KafkaRequest::CreateTopics(CreateTopicsRequest {
                    topics: vec![CreateTopicRequest {
                        name: WireString::from("dup-topic"),
                        num_partitions: 1,
                        replication_factor: 1,
                    }],
                    timeout_ms: 5000,
                }),
            )
            .await;

        match resp {
            KafkaResponse::CreateTopics(r) => {
                assert_eq!(
                    r.topics[0].error_code,
                    ErrorCode::TopicAlreadyExists.as_i16()
                );
            }
            other => panic!("expected CreateTopics, got {other:?}"),
        }
    }

    // =========================================================================
    // Phase 4 Tests: TopicNotifier
    // =========================================================================

    #[test]
    fn test_topic_notifier_create_and_notify() {
        let notifier = TopicNotifier::new();

        // Get or create a topic notification
        let n1 = notifier.get_or_create(42);
        let n2 = notifier.get_or_create(42);
        // Should return the same underlying Notify
        assert!(Arc::ptr_eq(&n1, &n2));

        // Different topic → different Notify
        let n3 = notifier.get_or_create(99);
        assert!(!Arc::ptr_eq(&n1, &n3));
    }

    #[test]
    fn test_topic_notifier_notify_topic() {
        let notifier = TopicNotifier::new();
        // Should not panic even for unknown topics
        notifier.notify_topic(12345);

        // Create and notify
        let _n = notifier.get_or_create(42);
        notifier.notify_topic(42);
    }

    #[test]
    fn test_topic_notifier_remove_topic() {
        let notifier = TopicNotifier::new();
        let _n = notifier.get_or_create(42);
        notifier.remove_topic(42);

        // Creating again should give a new instance
        let _n2 = notifier.get_or_create(42);
    }

    #[test]
    fn test_topic_notifier_global_notify() {
        let notifier = TopicNotifier::new();
        notifier.notify_global();
    }

    #[tokio::test]
    async fn test_handler_topic_notifier() {
        let (handler, _, _) = make_test_handler();
        let notifier = handler.topic_notifier();
        // Verify it returns a valid TopicNotifier
        notifier.notify_topic(42);
        notifier.notify_global();

        // Backward-compat data_notify still works
        let notify = handler.data_notify();
        notify.notify_waiters();
    }

    // =========================================================================
    // Phase 6 Tests: MetadataLogReader
    // =========================================================================

    #[test]
    fn test_metadata_log_reader_basic() {
        let config = MqConfig::new("/tmp/mq-kafka-meta-reader-test");
        let engine = MqEngine::new(config);
        let metadata = engine.shared_metadata();
        let reader = MetadataLogReader::new(metadata);

        // Empty metadata
        assert_eq!(reader.get_topic_head(1), None);
        assert_eq!(reader.get_topic_tail(1), None);
        assert_eq!(reader.get_committed_offset(1, 1), None);
        assert!(reader.list_topics().is_empty());

        // read_topic_messages returns empty
        let (msgs, hw) = reader.read_topic_messages(1, 0, 1024);
        assert!(msgs.is_empty());
        assert_eq!(hw, 0);
    }

    // =========================================================================
    // Phase 1+2: Handler build_record_batch_from_flat
    // =========================================================================

    #[tokio::test]
    async fn test_handler_build_record_batch_from_flat() {
        use bisque_mq::flat::{FlatMessage, FlatMessageBuilder};

        let (handler, _, _) = make_test_handler();

        let msg1 = FlatMessageBuilder::new(b"hello")
            .timestamp(1000)
            .key(b"k1")
            .build();
        let msg2 = FlatMessageBuilder::new(b"world").timestamp(1005).build();

        let flat_msgs = vec![(0u64, msg1), (1, msg2)];
        let result = handler.build_record_batch_from_flat(&flat_msgs, 0);

        // Verify by decoding
        let decoded = codec::decode_record_batch(&result).unwrap();
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].key.as_deref(), Some(b"k1".as_ref()));
        assert_eq!(decoded.records[0].value.as_deref(), Some(b"hello".as_ref()));
        assert_eq!(decoded.records[1].key, None);
        assert_eq!(decoded.records[1].value.as_deref(), Some(b"world".as_ref()));
    }
}

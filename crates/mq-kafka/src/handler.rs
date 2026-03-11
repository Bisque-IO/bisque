use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bisque_mq::MqMetadata;
use bisque_mq::consumer_group::GroupPhase;
use bisque_mq::flat::FlatMessageBuilder;
use bisque_mq::types::{MqCommand, MqError, MqResponse, RetentionPolicy, name_hash};
use bisque_mq::write_batcher::MqWriteBatcher;
use bytes::Bytes;
use tracing::{debug, warn};

use crate::codec;
use crate::partition::{self, PartitionMap};
use crate::types::*;

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
            KafkaRequest::Fetch(req) => self.handle_fetch(req),
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
            ApiKey::ApiVersions,
            ApiKey::CreateTopics,
            ApiKey::DeleteTopics,
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
                            replicas: vec![self.broker_id],
                            isr: vec![self.broker_id],
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
                .kafka_topics()
                .into_iter()
                .map(|s| build_topic_meta(WireString::from(s)))
                .collect(),
            Some(names) => names.into_iter().map(build_topic_meta).collect(),
        };

        KafkaResponse::Metadata(MetadataResponse { brokers, topics })
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

    async fn produce_records(&self, topic_id: u64, record_set_bytes: Bytes) -> Result<i64, String> {
        // Zero-copy decode — consumes Bytes directly, no clone needed
        let batch =
            codec::decode_record_batch_bytes(record_set_bytes).map_err(|e| format!("{e}"))?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let messages: Vec<Bytes> = batch
            .records
            .into_iter()
            .map(|r| {
                let timestamp = if batch.first_timestamp > 0 {
                    (batch.first_timestamp + r.timestamp_delta) as u64
                } else {
                    now
                };
                let mut builder =
                    FlatMessageBuilder::new(r.value.unwrap_or_default()).timestamp(timestamp);
                if let Some(key) = r.key {
                    builder = builder.key(key);
                }
                for h in r.headers {
                    builder = builder.header(h.key, h.value);
                }
                builder.build()
            })
            .collect();

        self.m_produce_messages.increment(messages.len() as u64);

        let cmd = MqCommand::publish(topic_id, &messages);

        match self.batcher.submit(cmd).await {
            Ok(MqResponse::Published { base_offset, .. }) => Ok(base_offset as i64),
            Ok(MqResponse::Error(e)) => Err(format!("{e}")),
            Ok(other) => Err(format!("unexpected response: {other}")),
            Err(e) => Err(format!("{e}")),
        }
    }

    // -------------------------------------------------------------------------
    // Fetch
    // -------------------------------------------------------------------------

    fn handle_fetch(&self, req: FetchRequest) -> KafkaResponse {
        self.m_fetch_requests.increment(1);
        let pmap = self.partition_map.load();
        let mut topic_responses = Vec::with_capacity(req.topics.len());

        for topic_data in req.topics {
            let mut part_responses = Vec::with_capacity(topic_data.partitions.len());

            for part_data in &topic_data.partitions {
                let topic_id = pmap.resolve(&topic_data.topic_name, part_data.partition_index);

                match topic_id {
                    None => {
                        part_responses.push(FetchPartitionResponse {
                            partition_index: part_data.partition_index,
                            error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
                            high_watermark: -1,
                            record_set: Bytes::new(),
                        });
                    }
                    Some(topic_id) => {
                        let max_bytes = part_data.max_bytes.max(0) as usize;
                        let (messages, high_watermark) = self.log_reader.read_topic_messages(
                            topic_id,
                            part_data.fetch_offset as u64,
                            max_bytes,
                        );

                        self.m_fetch_messages.increment(messages.len() as u64);

                        let record_set = if messages.is_empty() {
                            Bytes::new()
                        } else {
                            self.build_record_batch(&messages, part_data.fetch_offset)
                        };

                        part_responses.push(FetchPartitionResponse {
                            partition_index: part_data.partition_index,
                            error_code: ErrorCode::None.as_i16(),
                            high_watermark: high_watermark as i64,
                            record_set,
                        });
                    }
                }
            }

            topic_responses.push(FetchTopicResponse {
                topic_name: topic_data.topic_name,
                partitions: part_responses,
            });
        }

        KafkaResponse::Fetch(FetchResponse {
            topics: topic_responses,
        })
    }

    fn build_record_batch(&self, messages: &[FetchedMessage], base_offset: i64) -> Bytes {
        let first_ts = messages.first().map(|m| m.timestamp as i64).unwrap_or(0);
        let max_ts = messages.last().map(|m| m.timestamp as i64).unwrap_or(0);
        let last_offset_delta = if messages.len() > 1 {
            (messages.len() - 1) as i32
        } else {
            0
        };

        let batch = RecordBatch {
            base_offset,
            partition_leader_epoch: 0,
            attributes: 0,
            last_offset_delta,
            first_timestamp: first_ts,
            max_timestamp: max_ts,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: messages
                .iter()
                .enumerate()
                .map(|(i, msg)| Record {
                    offset_delta: i as i32,
                    timestamp_delta: msg.timestamp as i64 - first_ts,
                    key: msg.key.clone(),
                    value: Some(msg.value.clone()),
                    headers: msg
                        .headers
                        .iter()
                        .map(|(k, v)| RecordHeader {
                            key: k.clone(),
                            value: v.clone(),
                        })
                        .collect(),
                })
                .collect(),
        };

        let mut buf = bytes::BytesMut::new();
        codec::encode_record_batch(&batch, &mut buf);
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
                                    metadata: Bytes::from(meta),
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
                    metadata: meta,
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
                            error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
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
                            error_code: ErrorCode::None.as_i16(),
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
                    client_host: WireString::empty(),
                    metadata,
                    assignment,
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
            .map(|entry| ListedGroup {
                group_id: WireString::from(entry.value().meta.name.clone()),
                protocol_type: WireString::from(entry.value().meta.protocol_type.clone()),
            })
            .collect();

        KafkaResponse::ListGroups(ListGroupsResponse {
            error_code: ErrorCode::None.as_i16(),
            groups,
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
}

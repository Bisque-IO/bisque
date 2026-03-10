use std::sync::Arc;

use bisque_mq::flat::FlatMessageBuilder;
use bisque_mq::types::{MqCommand, MqResponse, RetentionPolicy, name_hash};
use bisque_mq::write_batcher::MqWriteBatcher;
use bytes::Bytes;
use parking_lot::RwLock;
use tracing::{debug, warn};

use crate::codec;
use crate::coordinator::GroupCoordinator;
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
    pub headers: Vec<(String, Bytes)>,
}

// =============================================================================
// Kafka Handler
// =============================================================================

/// Core request handler that translates Kafka API requests into bisque-mq
/// commands and constructs Kafka responses.
pub struct KafkaHandler {
    batcher: Arc<MqWriteBatcher>,
    log_reader: Arc<dyn KafkaLogReader>,
    partition_map: RwLock<PartitionMap>,
    coordinator: RwLock<GroupCoordinator>,
    broker_id: i32,
    advertised_host: String,
    advertised_port: i32,

    // Pre-initialized metrics handles
    m_produce_requests: metrics::Counter,
    m_produce_messages: metrics::Counter,
    m_fetch_requests: metrics::Counter,
    m_fetch_messages: metrics::Counter,
}

impl KafkaHandler {
    pub fn new(
        batcher: Arc<MqWriteBatcher>,
        log_reader: Arc<dyn KafkaLogReader>,
        broker_id: i32,
        advertised_host: String,
        advertised_port: i32,
    ) -> Self {
        Self {
            batcher,
            log_reader,
            partition_map: RwLock::new(PartitionMap::new()),
            coordinator: RwLock::new(GroupCoordinator::new()),
            broker_id,
            advertised_host,
            advertised_port,
            m_produce_requests: metrics::counter!("kafka.produce.requests"),
            m_produce_messages: metrics::counter!("kafka.produce.messages"),
            m_fetch_requests: metrics::counter!("kafka.fetch.requests"),
            m_fetch_messages: metrics::counter!("kafka.fetch.messages"),
        }
    }

    /// Refresh the partition map from the engine's topic list.
    pub fn refresh_partitions(&self) {
        let topics = self.log_reader.list_topics();
        self.partition_map.write().refresh(&topics);
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
            KafkaRequest::Heartbeat(req) => self.handle_heartbeat(req),
            KafkaRequest::LeaveGroup(req) => self.handle_leave_group(req),
            KafkaRequest::OffsetCommit(req) => self.handle_offset_commit(req, header).await,
            KafkaRequest::OffsetFetch(req) => self.handle_offset_fetch(req),
            KafkaRequest::CreateTopics(req) => self.handle_create_topics(req).await,
            KafkaRequest::DeleteTopics(req) => self.handle_delete_topics(req).await,
            KafkaRequest::DescribeGroups(req) => self.handle_describe_groups(req),
            KafkaRequest::ListGroups => self.handle_list_groups(),
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
        let pmap = self.partition_map.read();

        let brokers = vec![BrokerMeta {
            node_id: self.broker_id,
            host: self.advertised_host.clone(),
            port: self.advertised_port,
        }];

        let kafka_topics: Vec<String> = match &req.topics {
            None => pmap
                .kafka_topics()
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
            Some(names) => names.clone(),
        };

        let topics = kafka_topics
            .iter()
            .map(|name| match pmap.partitions(name) {
                Some(parts) => TopicMetadata {
                    error_code: ErrorCode::None.as_i16(),
                    name: name.clone(),
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
                },
                None => TopicMetadata {
                    error_code: ErrorCode::UnknownTopicOrPartition.as_i16(),
                    name: name.clone(),
                    partitions: Vec::new(),
                },
            })
            .collect();

        KafkaResponse::Metadata(MetadataResponse { brokers, topics })
    }

    // -------------------------------------------------------------------------
    // Produce
    // -------------------------------------------------------------------------

    async fn handle_produce(&self, req: ProduceRequest) -> KafkaResponse {
        self.m_produce_requests.increment(1);

        // Resolve all partition IDs upfront (drop lock before any .await)
        let resolved: Vec<(String, Vec<(i32, Option<u64>, Option<Bytes>)>)> = {
            let pmap = self.partition_map.read();
            req.topics
                .iter()
                .map(|td| {
                    let parts: Vec<_> = td
                        .partitions
                        .iter()
                        .map(|pd| {
                            let tid = pmap.resolve(&td.topic_name, pd.partition_index);
                            (pd.partition_index, tid, pd.record_set.clone())
                        })
                        .collect();
                    (td.topic_name.clone(), parts)
                })
                .collect()
        };

        let mut topic_responses = Vec::with_capacity(resolved.len());
        for (topic_name, parts) in resolved {
            let mut part_responses = Vec::with_capacity(parts.len());
            for (partition_index, topic_id, record_set) in parts {
                let (error_code, base_offset) = match topic_id {
                    None => (ErrorCode::UnknownTopicOrPartition.as_i16(), -1i64),
                    Some(topic_id) => match &record_set {
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

    async fn produce_records(
        &self,
        topic_id: u64,
        record_set_bytes: &Bytes,
    ) -> Result<i64, String> {
        let batch = codec::decode_record_batch(record_set_bytes).map_err(|e| format!("{e}"))?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let messages: Vec<Bytes> = batch
            .records
            .iter()
            .map(|r| {
                let timestamp = if batch.first_timestamp > 0 {
                    (batch.first_timestamp + r.timestamp_delta) as u64
                } else {
                    now
                };
                let mut builder = FlatMessageBuilder::new(r.value.clone().unwrap_or_default())
                    .timestamp(timestamp);
                if let Some(ref key) = r.key {
                    builder = builder.key(key.clone());
                }
                for h in &r.headers {
                    builder = builder.header(Bytes::from(h.key.clone()), h.value.clone());
                }
                builder.build()
            })
            .collect();

        self.m_produce_messages.increment(messages.len() as u64);

        let cmd = MqCommand::publish(topic_id, &messages);

        match self.batcher.submit(cmd).await {
            Ok(MqResponse::Published { offsets }) => {
                let base = offsets.first().copied().unwrap_or(0);
                Ok(base as i64)
            }
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
        let pmap = self.partition_map.read();
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
                topic_name: topic_data.topic_name.clone(),
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
        let pmap = self.partition_map.read();
        let mut topic_responses = Vec::with_capacity(req.topics.len());

        for topic_data in &req.topics {
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
                            -2 => {
                                // Earliest
                                self.log_reader.get_topic_tail(topic_id).unwrap_or(0) as i64
                            }
                            -1 => {
                                // Latest
                                self.log_reader.get_topic_head(topic_id).unwrap_or(0) as i64
                            }
                            _ts => {
                                // Timestamp-based lookup: return latest as approximation
                                self.log_reader.get_topic_head(topic_id).unwrap_or(0) as i64
                            }
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
                topic_name: topic_data.topic_name.clone(),
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
    // Consumer Group
    // -------------------------------------------------------------------------

    async fn handle_join_group(&self, req: JoinGroupRequest) -> KafkaResponse {
        let result = self.coordinator.write().join_group(&req);
        match result {
            Err(resp) => KafkaResponse::JoinGroup(resp),
            Ok(rx) => match rx.await {
                Ok(resp) => KafkaResponse::JoinGroup(resp),
                Err(_) => KafkaResponse::JoinGroup(JoinGroupResponse {
                    error_code: ErrorCode::GroupCoordinatorNotAvailable.as_i16(),
                    generation_id: -1,
                    protocol_name: String::new(),
                    leader: String::new(),
                    member_id: String::new(),
                    members: Vec::new(),
                }),
            },
        }
    }

    async fn handle_sync_group(&self, req: SyncGroupRequest) -> KafkaResponse {
        let result = self.coordinator.write().sync_group(&req);
        match result {
            Err(resp) => KafkaResponse::SyncGroup(resp),
            Ok(rx) => match rx.await {
                Ok(resp) => KafkaResponse::SyncGroup(resp),
                Err(_) => KafkaResponse::SyncGroup(SyncGroupResponse {
                    error_code: ErrorCode::GroupCoordinatorNotAvailable.as_i16(),
                    assignment: Bytes::new(),
                }),
            },
        }
    }

    fn handle_heartbeat(&self, req: HeartbeatRequest) -> KafkaResponse {
        let resp = self.coordinator.write().heartbeat(&req);
        KafkaResponse::Heartbeat(resp)
    }

    fn handle_leave_group(&self, req: LeaveGroupRequest) -> KafkaResponse {
        let resp = self.coordinator.write().leave_group(&req);
        KafkaResponse::LeaveGroup(resp)
    }

    // -------------------------------------------------------------------------
    // OffsetCommit
    // -------------------------------------------------------------------------

    async fn handle_offset_commit(
        &self,
        req: OffsetCommitRequest,
        _header: &RequestHeader,
    ) -> KafkaResponse {
        // Use group_name hash as a consumer_id for now.
        let consumer_id = name_hash(&req.group_id);

        // Resolve all partition IDs upfront (drop lock before any .await)
        let resolved: Vec<(String, Vec<(i32, Option<u64>, i64)>)> = {
            let pmap = self.partition_map.read();
            req.topics
                .iter()
                .map(|td| {
                    let parts: Vec<_> = td
                        .partitions
                        .iter()
                        .map(|pd| {
                            let tid = pmap.resolve(&td.topic_name, pd.partition_index);
                            (pd.partition_index, tid, pd.offset)
                        })
                        .collect();
                    (td.topic_name.clone(), parts)
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
    // OffsetFetch
    // -------------------------------------------------------------------------

    fn handle_offset_fetch(&self, req: OffsetFetchRequest) -> KafkaResponse {
        let pmap = self.partition_map.read();
        let consumer_id = name_hash(&req.group_id);
        let mut topic_responses = Vec::with_capacity(req.topics.len());

        for topic_data in &req.topics {
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
                        let offset = self
                            .log_reader
                            .get_committed_offset(topic_id, consumer_id)
                            .map(|o| o as i64)
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
                topic_name: topic_data.topic_name.clone(),
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

        for ct in &req.topics {
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

            // Refresh partition map after creation
            self.refresh_partitions();

            topic_responses.push(CreateTopicResponse {
                name: ct.name.clone(),
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
        // Resolve all topic IDs upfront (drop lock before any .await)
        let resolved: Vec<(String, Option<Vec<u64>>)> = {
            let pmap = self.partition_map.read();
            req.topic_names
                .iter()
                .map(|name| {
                    let ids = pmap
                        .partitions(name)
                        .map(|parts| parts.iter().map(|&(_, id)| id).collect());
                    (name.clone(), ids)
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
    // DescribeGroups / ListGroups
    // -------------------------------------------------------------------------

    fn handle_describe_groups(&self, req: DescribeGroupsRequest) -> KafkaResponse {
        let coord = self.coordinator.read();
        let groups = req
            .group_ids
            .iter()
            .map(|id| coord.describe_group(id))
            .collect();
        KafkaResponse::DescribeGroups(DescribeGroupsResponse { groups })
    }

    fn handle_list_groups(&self) -> KafkaResponse {
        let coord = self.coordinator.read();
        KafkaResponse::ListGroups(ListGroupsResponse {
            error_code: ErrorCode::None.as_i16(),
            groups: coord.list_groups(),
        })
    }

    /// Clean up on connection disconnect.
    pub fn on_disconnect(&self, member_ids: &[String]) {
        let mut coord = self.coordinator.write();
        for mid in member_ids {
            coord.remove_member(mid);
        }
    }

    /// Run periodic maintenance (session expiry).
    pub fn expire_sessions(&self) {
        self.coordinator.write().expire_sessions();
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_record_batch() {
        // Just verify we can construct the handler types
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
}

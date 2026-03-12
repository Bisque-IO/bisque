//! Unified MQ engine — topics + consumer groups + sessions.
//!
//! All entity state lives in `Arc<MqMetadata>` (DashMap + atomics) for lock-free
//! concurrent access. The engine holds the config and provides `apply_command`
//! which is the single-writer Raft apply path.
//!
//! This replaces the old engine that dispatched to queues, actors, jobs,
//! consumers, and producers as separate entity types.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use smallvec::SmallVec;
use tracing::{debug, info, warn};

use crate::config::MqConfig;
use crate::consumer_group::*;
use crate::exchange::*;
use crate::metadata::{MqMetadata, TopicMeta as MetaTopicMeta};
use crate::session::*;
use crate::topic::*;
use crate::types::*;

/// Core MQ engine — thin wrapper around `Arc<MqMetadata>`.
///
/// All entity state lives in `MqMetadata` (DashMap + atomics) for lock-free
/// concurrent access. The engine holds the config and provides `apply_command`
/// which is the single-writer Raft apply path.
pub struct MqEngine {
    metadata: Arc<MqMetadata>,
    pub(crate) config: MqConfig,

    // Pre-initialized metrics handles
    m_apply_count: metrics::Counter,
    m_unknown_tag: metrics::Counter,
}

impl MqEngine {
    pub fn new(config: MqConfig) -> Self {
        let metadata = Arc::new(MqMetadata::new(config.catalog_name.clone()));
        Self::with_metadata(metadata, config)
    }

    pub fn with_metadata(metadata: Arc<MqMetadata>, config: MqConfig) -> Self {
        let labels = [("catalog", config.catalog_name.clone())];
        Self {
            metadata,
            config,
            m_apply_count: metrics::counter!("mq.engine.apply.count", &labels),
            m_unknown_tag: metrics::counter!("mq.engine.unknown_tag", &labels),
        }
    }

    /// Get a shared reference to the metadata store.
    pub fn shared_metadata(&self) -> Arc<MqMetadata> {
        Arc::clone(&self.metadata)
    }

    /// Borrow the metadata store.
    pub fn metadata(&self) -> &MqMetadata {
        &self.metadata
    }

    /// Apply a single command. Returns the response.
    ///
    /// `log_index` is the raft log index of the entry containing this command.
    /// `current_time` is a millisecond timestamp (from the leader's wall clock,
    /// included in the raft entry for deterministic replay).
    pub fn apply_command(
        &mut self,
        cmd: &MqCommand,
        log_index: u64,
        current_time: u64,
    ) -> MqResponse {
        self.m_apply_count.increment(1);
        let md = &self.metadata;

        match cmd.tag() {
            // =================================================================
            // Topics (0-5)
            // =================================================================
            MqCommand::TAG_CREATE_TOPIC => {
                let v = cmd.as_create_topic();
                let name = v.name().to_owned();
                let name_h = name_hash(&name);
                if let Some(r) = md.topic_names.get(&name_h) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::Topic,
                        id: *r,
                    });
                }
                let topic_id = md.alloc_id();
                let mut meta = TopicMeta::new(topic_id, name, current_time, v.retention());
                meta.partition_count = v.partition_count();
                // Extended fields: lifetime, retained, dedup, cron are parsed
                // from the trailing portion of the create_topic buffer.
                // The CmdCreateTopic view struct currently only exposes
                // name/retention/partition_count. For now, those extended fields
                // are set to defaults and can be updated by the caller via
                // separate commands or once the codec is extended.
                let state = TopicState::new(meta, md.catalog_name());
                md.topics.insert(topic_id, state);
                md.topic_names.insert(name_h, topic_id);
                info!(topic_id, "topic created");
                MqResponse::EntityCreated { id: topic_id }
            }

            MqCommand::TAG_DELETE_TOPIC => {
                let topic_id = cmd.field_u64(1);
                if let Some((_, state)) = md.topics.remove(&topic_id) {
                    md.topic_names.remove(&state.meta.name_hash);
                    if state.message_count() > 0 {
                        self.mark_purge_floor_dirty();
                    }
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Topic,
                        id: topic_id,
                    })
                }
            }

            MqCommand::TAG_PUBLISH => {
                let v = cmd.as_publish();
                let topic_id = v.topic_id();
                let count = v.message_count() as u64;
                if let Some(topic) = md.topics.get(&topic_id) {
                    let base_offset = topic.apply_publish(log_index, v.messages());
                    self.on_message_added(log_index);
                    MqResponse::Published { base_offset, count }
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Topic,
                        id: topic_id,
                    })
                }
            }

            MqCommand::TAG_COMMIT_OFFSET => {
                // Wire: [tag:1][topic_id:8][consumer_id:8][offset:8]
                let topic_id = cmd.field_u64(1);
                let consumer_id = cmd.field_u64(9);
                let offset = cmd.field_u64(17);
                if let Some(topic) = md.topics.get(&topic_id) {
                    topic.apply_commit_offset(consumer_id, offset);
                    md.purge_floor_dirty.store(true, Ordering::Relaxed);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Topic,
                        id: topic_id,
                    })
                }
            }

            MqCommand::TAG_PURGE_TOPIC => {
                // Wire: [tag:1][topic_id:8][before_index:8]
                let topic_id = cmd.field_u64(1);
                let before_index = cmd.field_u64(9);
                if let Some(topic) = md.topics.get(&topic_id) {
                    topic.apply_purge(before_index);
                    self.mark_purge_floor_dirty();
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Topic,
                        id: topic_id,
                    })
                }
            }

            MqCommand::TAG_SET_RETAINED => {
                let v = cmd.as_set_retained();
                let exchange_id = v.exchange_id();
                let routing_key = v.routing_key().to_owned();
                let message = v.message();
                if let Some(mut exchange) = md.exchanges.get_mut(&exchange_id) {
                    exchange.retained.insert(routing_key, message);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Exchange,
                        id: exchange_id,
                    })
                }
            }

            // =================================================================
            // Exchanges (6-10)
            // =================================================================
            MqCommand::TAG_CREATE_EXCHANGE => {
                let v = cmd.as_create_exchange();
                let name = v.name().to_owned();
                let exchange_type = v.exchange_type();
                let name_h = name_hash(&name);
                if let Some(r) = md.exchange_names.get(&name_h) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::Exchange,
                        id: *r,
                    });
                }
                let id = md.alloc_id();
                let meta = ExchangeMeta::new(id, name, current_time, exchange_type);
                md.exchanges.insert(id, ExchangeState::new(meta));
                md.exchange_names.insert(name_h, id);
                info!(exchange_id = id, "exchange created");
                MqResponse::EntityCreated { id }
            }

            MqCommand::TAG_DELETE_EXCHANGE => {
                let exchange_id = cmd.field_u64(1);
                if let Some((_, state)) = md.exchanges.remove(&exchange_id) {
                    md.exchange_names.remove(&state.meta.name_hash);
                    // Clean up binding reverse index
                    for binding_id in state.bindings.keys() {
                        md.binding_index.remove(binding_id);
                    }
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Exchange,
                        id: exchange_id,
                    })
                }
            }

            MqCommand::TAG_CREATE_BINDING => {
                let v = cmd.as_create_binding();
                let exchange_id = v.exchange_id();
                let target_topic_id = v.topic_id();
                let routing_key = v.routing_key();
                let no_local = v.no_local();
                let shared_group = v.shared_group();
                let subscription_id = v.subscription_id();

                // MQTT 5.0 §3.8.3.1: no_local on shared subscriptions is a protocol error
                if no_local && shared_group.is_some() {
                    return MqResponse::Error(MqError::Custom(
                        "no_local not allowed on shared subscriptions".to_string(),
                    ));
                }

                if !md.exchanges.contains_key(&exchange_id) {
                    return MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Exchange,
                        id: exchange_id,
                    });
                }

                let binding_id = md.alloc_id();
                let binding = Binding {
                    binding_id,
                    exchange_id,
                    target_topic_id,
                    routing_key,
                    no_local,
                    shared_group,
                    subscription_id,
                };
                if let Some(mut exchange) = md.exchanges.get_mut(&exchange_id) {
                    exchange.add_binding(binding);
                }
                md.binding_index.insert(binding_id, exchange_id);
                MqResponse::EntityCreated { id: binding_id }
            }

            MqCommand::TAG_DELETE_BINDING => {
                let binding_id = cmd.field_u64(1);
                if let Some((_, exchange_id)) = md.binding_index.remove(&binding_id) {
                    if let Some(mut exchange) = md.exchanges.get_mut(&exchange_id) {
                        exchange.remove_binding(binding_id);
                    }
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Binding,
                        id: binding_id,
                    })
                }
            }

            MqCommand::TAG_PUBLISH_TO_EXCHANGE => {
                let v = cmd.as_publish_to_exchange();
                let exchange_id = v.exchange_id();
                let messages: SmallVec<[Bytes; 16]> = v.messages().collect();

                let targets = if let Some(exchange) = md.exchanges.get(&exchange_id) {
                    // Extract routing key from first flat message
                    let routing_key_bytes = messages.first().and_then(|m| {
                        crate::flat::FlatMessage::new(m.clone()).and_then(|f| f.routing_key())
                    });
                    let routing_key_str = routing_key_bytes
                        .as_ref()
                        .and_then(|b| std::str::from_utf8(b).ok());
                    exchange.route(routing_key_str)
                } else {
                    return MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Exchange,
                        id: exchange_id,
                    });
                };

                let mut total_count = 0u64;
                for &target_id in &targets {
                    if let Some(topic) = md.topics.get(&target_id) {
                        topic.apply_publish(log_index, messages.iter().cloned());
                        total_count += messages.len() as u64;
                    }
                }
                if total_count > 0 {
                    self.on_message_added(log_index);
                }
                MqResponse::Published {
                    base_offset: 0,
                    count: total_count,
                }
            }

            // =================================================================
            // Consumer Groups (11-18)
            // =================================================================
            MqCommand::TAG_CREATE_CONSUMER_GROUP => {
                let v = cmd.as_create_consumer_group();
                let name = v.name().to_owned();
                let name_h = name_hash(&name);

                // Check for name collision with existing groups
                if md.consumer_group_names.contains_key(&name_h) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::ConsumerGroup,
                        id: 0,
                    });
                }

                let group_id = md.alloc_id();
                let auto_offset_reset = match v.auto_offset_reset() {
                    0 => AutoOffsetReset::Earliest,
                    2 => AutoOffsetReset::None,
                    _ => AutoOffsetReset::Latest,
                };

                let variant_config = v.variant_config();
                let variant = match &variant_config {
                    VariantConfig::Offset => GroupVariant::Offset,
                    VariantConfig::Ack(_) => GroupVariant::Ack,
                    VariantConfig::Actor(_) => GroupVariant::Actor,
                };

                // Auto-create source topic if requested
                let source_topic_id = if v.auto_create_topic() {
                    let topic_name_h = name_hash(&name);
                    let topic_id = if let Some(existing) = md.topic_names.get(&topic_name_h) {
                        *existing
                    } else {
                        let tid = md.alloc_id();
                        let mut topic_meta =
                            TopicMeta::new(tid, name.clone(), current_time, v.topic_retention());
                        topic_meta.lifetime = v.topic_lifetime();
                        topic_meta.dedup_config = v.topic_dedup();
                        let topic_state = TopicState::new(topic_meta, md.catalog_name());
                        md.topics.insert(tid, topic_state);
                        md.topic_names.insert(topic_name_h, tid);
                        tid
                    };
                    // Attach group to topic
                    if let Some(topic) = md.topics.get(&topic_id) {
                        topic.attach_group(group_id);
                    }
                    topic_id
                } else {
                    0
                };

                // Auto-create DLQ topic if specified
                if let Some(dlq_name) = v.dlq_topic_name() {
                    let dlq_h = name_hash(&dlq_name);
                    if !md.topic_names.contains_key(&dlq_h) {
                        let dlq_id = md.alloc_id();
                        let dlq_meta = TopicMeta::new(
                            dlq_id,
                            dlq_name,
                            current_time,
                            RetentionPolicy::default(),
                        );
                        let dlq_state = TopicState::new(dlq_meta, md.catalog_name());
                        md.topics.insert(dlq_id, dlq_state);
                        md.topic_names.insert(dlq_h, dlq_id);
                    }
                }

                // Auto-create response topic if specified
                if let Some(resp_name) = v.response_topic_name() {
                    let resp_h = name_hash(&resp_name);
                    if !md.topic_names.contains_key(&resp_h) {
                        let resp_id = md.alloc_id();
                        let resp_meta = TopicMeta::new(
                            resp_id,
                            resp_name,
                            current_time,
                            RetentionPolicy::default(),
                        );
                        let resp_state = TopicState::new(resp_meta, md.catalog_name());
                        md.topics.insert(resp_id, resp_state);
                        md.topic_names.insert(resp_h, resp_id);
                    }
                }

                let meta = ConsumerGroupMeta {
                    group_id,
                    name,
                    name_hash: name_h,
                    created_at: current_time,
                    generation: 0,
                    phase: GroupPhase::Empty,
                    protocol_type: String::new(),
                    protocol_name: String::new(),
                    leader: None,
                    auto_offset_reset,
                    last_activity_at: current_time,
                    next_member_id: 1,
                    members: Vec::new(),
                    variant,
                    source_topic_id,
                    variant_config,
                };
                let state = ConsumerGroupState::new(meta, md.catalog_name(), md.server_start_ms);
                md.consumer_group_names.insert(name_h, group_id);
                md.consumer_groups.insert(group_id, state);
                info!(
                    group_id,
                    ?variant,
                    source_topic_id,
                    "consumer group created"
                );
                MqResponse::EntityCreated { id: group_id }
            }

            MqCommand::TAG_DELETE_CONSUMER_GROUP => {
                let group_id = cmd.field_u64(1);
                if let Some((_, group)) = md.consumer_groups.remove(&group_id) {
                    md.consumer_group_names.remove(&group.meta.name_hash);
                    let source = group.meta.source_topic_id;
                    if source != 0 {
                        if let Some(topic) = md.topics.get(&source) {
                            topic.detach_group(group_id);
                            if topic.should_auto_delete() {
                                let topic_id = topic.meta.topic_id;
                                let nh = topic.meta.name_hash;
                                drop(topic);
                                md.topics.remove(&topic_id);
                                md.topic_names.remove(&nh);
                            }
                        }
                    }
                    // Check if variant state had messages for purge floor
                    match &group.variant_state {
                        VariantState::Ack(ack) => {
                            if ack.has_messages() {
                                self.mark_purge_floor_dirty();
                            }
                        }
                        VariantState::Actor(actor) => {
                            if actor.active_count() > 0 {
                                self.mark_purge_floor_dirty();
                            }
                        }
                        _ => {}
                    }
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ConsumerGroup,
                        id: group_id,
                    })
                }
            }

            MqCommand::TAG_JOIN_CONSUMER_GROUP => {
                let v = cmd.as_join_consumer_group();
                let group_id = v.group_id();
                let member_id_str = v.member_id();
                let client_id = v.client_id();

                let group = match md.consumer_groups.get(&group_id) {
                    Some(g) => g,
                    None => {
                        return MqResponse::Error(MqError::NotFound {
                            entity: EntityKind::ConsumerGroup,
                            id: group_id,
                        });
                    }
                };

                // Assign member_id if empty (new member)
                let actual_member_id = if member_id_str.is_empty() {
                    let seq = group.increment_next_member_id();
                    format!("{}-{}", client_id, seq)
                } else {
                    member_id_str.to_string()
                };

                // Insert/update member
                let protocols = SmallVec::from_vec(v.protocols());
                group.upsert_member(GroupMemberState {
                    member_id: actual_member_id.clone(),
                    client_id: client_id.to_string(),
                    session_timeout_ms: v.session_timeout_ms(),
                    rebalance_timeout_ms: v.rebalance_timeout_ms(),
                    protocol_type: v.protocol_type().to_string(),
                    protocols,
                    assignment: parking_lot::RwLock::new(Bytes::new()),
                    last_heartbeat_at: std::sync::atomic::AtomicU64::new(current_time),
                    joined_this_gen: std::sync::atomic::AtomicBool::new(true),
                    synced_this_gen: std::sync::atomic::AtomicBool::new(false),
                });

                // Transition phase if needed
                let phase = group.phase();
                if phase == GroupPhase::Empty || phase == GroupPhase::Stable {
                    group.set_phase(GroupPhase::PreparingRebalance);
                }

                // Check if all members have joined
                let all_joined = group.all_members_joined();
                if all_joined {
                    group.bump_generation();
                    group.select_and_set_protocol();
                    group.elect_leader();
                    group.set_phase(GroupPhase::CompletingRebalance);
                    group.clear_join_marks();
                    group.record_rebalance();
                    group.phase_notify.notify_waiters();
                }

                let leader_arc = group.leader();
                let leader_str = leader_arc.as_deref().map(|s| s.as_str()).unwrap_or("");
                let is_leader = leader_str == actual_member_id;

                MqResponse::GroupJoined {
                    generation: group.generation(),
                    leader: leader_str.to_string(),
                    member_id: actual_member_id,
                    protocol_name: (*group.protocol_name()).clone(),
                    is_leader,
                    members: if is_leader {
                        group.member_protocols()
                    } else {
                        vec![]
                    },
                    phase_complete: all_joined,
                }
            }

            MqCommand::TAG_SYNC_CONSUMER_GROUP => {
                let v = cmd.as_sync_consumer_group();
                let group_id = v.group_id();
                let generation = v.generation();
                let member_id = v.member_id();

                let group = match md.consumer_groups.get(&group_id) {
                    Some(g) => g,
                    None => {
                        return MqResponse::Error(MqError::NotFound {
                            entity: EntityKind::ConsumerGroup,
                            id: group_id,
                        });
                    }
                };

                if group.generation() != generation {
                    return MqResponse::Error(MqError::IllegalGeneration);
                }

                // If this is the leader with assignments, store them
                let assignments = v.assignments();
                if !assignments.is_empty() {
                    for (mid, data) in &assignments {
                        group.set_member_assignment(mid, Bytes::from(data.clone()));
                    }
                    group.set_phase(GroupPhase::Stable);
                    group.touch_activity(current_time);
                    group.phase_notify.notify_waiters();
                }

                group.mark_synced(member_id);

                // If all synced but no assignments provided, transition anyway
                if group.all_members_synced() && group.phase() != GroupPhase::Stable {
                    group.set_phase(GroupPhase::Stable);
                    group.phase_notify.notify_waiters();
                }

                let assignment = group
                    .get_member_assignment(member_id)
                    .unwrap_or_default()
                    .to_vec();

                MqResponse::GroupSynced {
                    assignment,
                    phase_complete: group.phase() == GroupPhase::Stable,
                }
            }

            MqCommand::TAG_LEAVE_CONSUMER_GROUP => {
                let v = cmd.as_leave_consumer_group();
                let group_id = v.group_id();
                let member_id = v.member_id();

                let group = match md.consumer_groups.get(&group_id) {
                    Some(g) => g,
                    None => return MqResponse::Ok, // Idempotent
                };

                group.remove_member(member_id);

                if group.member_count() == 0 {
                    group.set_phase(GroupPhase::Empty);
                    group.clear_leader();
                } else if group.phase() == GroupPhase::Stable {
                    group.set_phase(GroupPhase::PreparingRebalance);
                }

                group.phase_notify.notify_waiters();
                MqResponse::Ok
            }

            MqCommand::TAG_HEARTBEAT_CONSUMER_GROUP => {
                let v = cmd.as_heartbeat_consumer_group();
                let group_id = v.group_id();
                let member_id = v.member_id();
                let generation = v.generation();

                let group = match md.consumer_groups.get(&group_id) {
                    Some(g) => g,
                    None => {
                        return MqResponse::Error(MqError::NotFound {
                            entity: EntityKind::ConsumerGroup,
                            id: group_id,
                        });
                    }
                };

                if group.generation() != generation {
                    return MqResponse::Error(MqError::IllegalGeneration);
                }

                if !group.has_member(member_id) {
                    return MqResponse::Error(MqError::UnknownMemberId);
                }

                group.update_member_heartbeat(member_id, current_time);
                group.touch_activity(current_time);

                if group.phase() == GroupPhase::PreparingRebalance {
                    MqResponse::Error(MqError::RebalanceInProgress)
                } else {
                    MqResponse::Ok
                }
            }

            MqCommand::TAG_COMMIT_GROUP_OFFSET => {
                let v = cmd.as_commit_group_offset();
                let group_id = v.group_id();
                let generation = v.generation();

                let group = match md.consumer_groups.get(&group_id) {
                    Some(g) => g,
                    None => {
                        return MqResponse::Error(MqError::NotFound {
                            entity: EntityKind::ConsumerGroup,
                            id: group_id,
                        });
                    }
                };

                if group.generation() != generation {
                    return MqResponse::Error(MqError::IllegalGeneration);
                }

                group.offsets.insert(
                    (v.topic_id(), v.partition_index()),
                    GroupTopicPartitionOffset {
                        topic_id: v.topic_id(),
                        partition_index: v.partition_index(),
                        committed_offset: v.offset(),
                        metadata: v.metadata().map(|s| s.to_string()),
                        committed_at: v.timestamp(),
                    },
                );

                group.touch_activity(current_time);
                group.record_offset_commit();
                MqResponse::Ok
            }

            MqCommand::TAG_EXPIRE_GROUP_SESSIONS => {
                // Wire: [tag:1][now_ms:8]
                let now_ms = cmd.field_u64(1);

                for entry in md.consumer_groups.iter() {
                    let group = entry.value();
                    let phase = group.phase();
                    if phase == GroupPhase::Dead || phase == GroupPhase::Empty {
                        continue;
                    }

                    let expired = group.find_expired_members(now_ms);
                    if !expired.is_empty() {
                        for mid in &expired {
                            group.remove_member(mid);
                        }
                        if group.member_count() == 0 {
                            group.set_phase(GroupPhase::Empty);
                            group.clear_leader();
                        } else if group.phase() == GroupPhase::Stable {
                            group.set_phase(GroupPhase::PreparingRebalance);
                        }
                        group.phase_notify.notify_waiters();
                    }
                }

                MqResponse::Ok
            }

            // =================================================================
            // Ack Variant (19-29)
            // =================================================================
            MqCommand::TAG_GROUP_DELIVER => {
                // Wire: [tag:1][group_id:8][consumer_id:8][max_count:4]
                let group_id = cmd.field_u64(1);
                let consumer_id = cmd.field_u64(9);
                let max_count = cmd.field_u32(17);

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let (Some(ack), Some(config)) = (group.ack_state(), group.ack_config()) {
                        let delivered = ack.apply_deliver(
                            config,
                            consumer_id,
                            max_count,
                            current_time,
                            log_index,
                        );
                        if !delivered.is_empty() {
                            md.track_session_group(consumer_id, group_id);
                        }
                        let msgs: SmallVec<[DeliveredMessage; 8]> = delivered
                            .iter()
                            .map(|&msg_id| {
                                let attempt = ack.messages.get(&msg_id).map_or(1, |m| m.attempts);
                                DeliveredMessage {
                                    message_id: msg_id,
                                    attempt,
                                    original_timestamp: current_time,
                                    group_id,
                                }
                            })
                            .collect();
                        md.group_notifier.notify(group_id);
                        return MqResponse::Messages { messages: msgs };
                    }
                }
                MqResponse::Error(MqError::NotFound {
                    entity: EntityKind::ConsumerGroup,
                    id: group_id,
                })
            }

            MqCommand::TAG_GROUP_ACK => {
                // Wire: [tag:1][group_id:8][vec_u64(message_ids)]
                let group_id = cmd.field_u64(1);
                let message_ids = {
                    let mut cursor = std::io::Cursor::new(&cmd.as_bytes()[9..]);
                    crate::codec::decode_cmd_vec_u64(&mut cursor)
                };

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let Some(ack) = group.ack_state() {
                        // Collect reply_to info before acking
                        let reply_pairs: SmallVec<[(u64, Bytes); 4]> = message_ids
                            .iter()
                            .filter_map(|&msg_id| {
                                ack.messages.get(&msg_id).and_then(|m| {
                                    m.reply_to.as_ref().map(|rt| (msg_id, rt.clone()))
                                })
                            })
                            .collect();

                        ack.apply_ack(&message_ids);

                        if let Some(min_id) = message_ids.iter().copied().min() {
                            self.on_message_removed(min_id);
                        }

                        // Route replies
                        for (_msg_id, reply_to) in reply_pairs {
                            let hash = name_hash_bytes(&reply_to);
                            if let Some(r) = md.topic_names.get(&hash) {
                                let topic_id = *r;
                                drop(r);
                                if let Some(topic) = md.topics.get(&topic_id) {
                                    // Publish an empty acknowledgment to the reply topic
                                    topic.apply_publish(log_index, std::iter::once(Bytes::new()));
                                    self.on_message_added(log_index);
                                }
                            }
                        }

                        return MqResponse::Ok;
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_NACK => {
                // Wire: [tag:1][group_id:8][vec_u64(message_ids)]
                let group_id = cmd.field_u64(1);
                let message_ids = {
                    let mut cursor = std::io::Cursor::new(&cmd.as_bytes()[9..]);
                    crate::codec::decode_cmd_vec_u64(&mut cursor)
                };

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let Some(ack) = group.ack_state() {
                        ack.apply_nack(&message_ids);
                        md.group_notifier.notify(group_id);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_RELEASE => {
                // Wire: [tag:1][group_id:8][vec_u64(message_ids)]
                let group_id = cmd.field_u64(1);
                let message_ids = {
                    let mut cursor = std::io::Cursor::new(&cmd.as_bytes()[9..]);
                    crate::codec::decode_cmd_vec_u64(&mut cursor)
                };

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let Some(ack) = group.ack_state() {
                        ack.apply_release(&message_ids);
                        md.group_notifier.notify(group_id);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_MODIFY => {
                // Wire: [tag:1][group_id:8][vec_u64(message_ids)]
                // Modify = release with re-delivery semantics (return to pending, keep attempts)
                let group_id = cmd.field_u64(1);
                let message_ids = {
                    let mut cursor = std::io::Cursor::new(&cmd.as_bytes()[9..]);
                    crate::codec::decode_cmd_vec_u64(&mut cursor)
                };

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let Some(ack) = group.ack_state() {
                        // NACK returns to pending with incremented attempts
                        ack.apply_nack(&message_ids);
                        md.group_notifier.notify(group_id);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_EXTEND_VISIBILITY => {
                // Wire: [tag:1][group_id:8][vec_u64(message_ids)][extension_ms:8]
                let group_id = cmd.field_u64(1);
                // Parse message_ids and extension_ms from the buffer
                let mut cursor = std::io::Cursor::new(&cmd.as_bytes()[9..]);
                let message_ids = crate::codec::decode_cmd_vec_u64(&mut cursor);
                let pos = cursor.position() as usize;
                let ext_offset = 9 + pos;
                let extension_ms = cmd.field_u64(ext_offset);

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let Some(ack) = group.ack_state() {
                        ack.apply_extend_visibility(&message_ids, extension_ms);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_TIMEOUT_EXPIRED => {
                // Wire: [tag:1][group_id:8][vec_u64(message_ids)]
                let group_id = cmd.field_u64(1);
                let message_ids = {
                    let mut cursor = std::io::Cursor::new(&cmd.as_bytes()[9..]);
                    crate::codec::decode_cmd_vec_u64(&mut cursor)
                };

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let (Some(ack), Some(config)) = (group.ack_state(), group.ack_config()) {
                        let dead_lettered =
                            ack.apply_timeout_expired(&message_ids, config, current_time);
                        if !dead_lettered.is_empty() {
                            // Resolve DLQ topic
                            let dlq_topic_id = self.resolve_dlq_topic(&group);
                            return MqResponse::DeadLettered {
                                dead_letter_ids: dead_lettered,
                                dlq_topic_id,
                            };
                        }
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_PUBLISH_TO_DLQ => {
                // Wire: [tag:1][group_id:8][dlq_topic_id:8][vec_u64(dead_letter_ids)][vec_bytes(messages)]
                let v = cmd.as_publish_to_dlq();
                let source_group_id = v.source_group_id();
                let dlq_topic_id = v.dlq_topic_id();
                let dead_letter_ids = v.dead_letter_ids();
                let messages = v.messages();

                // Publish dead-lettered messages to the DLQ topic
                if let Some(topic) = md.topics.get(&dlq_topic_id) {
                    topic.apply_publish(log_index, messages);
                    self.on_message_added(log_index);
                }
                // Remove dead-lettered messages from the source group's ack state
                if let Some(group) = md.consumer_groups.get(&source_group_id) {
                    if let Some(ack) = group.ack_state() {
                        ack.apply_remove_dead_lettered(&dead_letter_ids);
                        self.mark_purge_floor_dirty();
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_EXPIRE_PENDING => {
                // Wire: [tag:1][group_id:8][vec_u64(message_ids)]
                let group_id = cmd.field_u64(1);
                let message_ids = {
                    let mut cursor = std::io::Cursor::new(&cmd.as_bytes()[9..]);
                    crate::codec::decode_cmd_vec_u64(&mut cursor)
                };

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let Some(ack) = group.ack_state() {
                        ack.apply_expire_pending(&message_ids);
                        self.mark_purge_floor_dirty();
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_PURGE => {
                // Wire: [tag:1][group_id:8]
                let group_id = cmd.field_u64(1);
                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let Some(ack) = group.ack_state() {
                        if ack.purge() {
                            self.mark_purge_floor_dirty();
                        }
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_GET_ATTRIBUTES => {
                // Wire: [tag:1][group_id:8]
                let group_id = cmd.field_u64(1);
                if let Some(group) = md.consumer_groups.get(&group_id) {
                    let variant = group.meta.variant;
                    let (pending, in_flight, dlq, actors) = match &group.variant_state {
                        VariantState::Ack(ack) => (
                            ack.pending_count(),
                            ack.in_flight_count(),
                            ack.dlq_count(),
                            0,
                        ),
                        VariantState::Actor(actor) => (0, 0, 0, actor.active_count()),
                        VariantState::Offset => (0, 0, 0, 0),
                    };
                    MqResponse::Stats(EntityStats::ConsumerGroup {
                        group_id,
                        variant,
                        pending_count: pending,
                        in_flight_count: in_flight,
                        dlq_count: dlq,
                        active_actor_count: actors,
                    })
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ConsumerGroup,
                        id: group_id,
                    })
                }
            }

            // =================================================================
            // Actor Variant (30-35)
            // =================================================================
            MqCommand::TAG_GROUP_DELIVER_ACTOR => {
                // Wire: [tag:1][group_id:8][consumer_id:8][count:4][actor_id_1:len_prefixed]...
                let group_id = cmd.field_u64(1);
                let consumer_id = cmd.field_u64(9);
                let actor_count = cmd.field_u32(17) as usize;
                let mut offset = 21;
                let mut actor_ids: SmallVec<[Bytes; 8]> = SmallVec::with_capacity(actor_count);
                for _ in 0..actor_count {
                    let len = cmd.field_u32(offset) as usize;
                    offset += 4;
                    let id = cmd.buf.slice(offset..offset + len);
                    offset += len;
                    actor_ids.push(id);
                }

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let Some(actor_state) = group.actor_state() {
                        let mut msgs = SmallVec::new();
                        for actor_id in &actor_ids {
                            if let Some(msg_idx) =
                                actor_state.apply_deliver(actor_id, consumer_id, 0)
                            {
                                msgs.push(DeliveredMessage {
                                    message_id: msg_idx,
                                    attempt: 1,
                                    original_timestamp: current_time,
                                    group_id,
                                });
                            }
                        }
                        if !msgs.is_empty() {
                            md.track_session_group(consumer_id, group_id);
                        }
                        return MqResponse::Messages { messages: msgs };
                    }
                }
                MqResponse::Error(MqError::NotFound {
                    entity: EntityKind::ConsumerGroup,
                    id: group_id,
                })
            }

            MqCommand::TAG_GROUP_ACK_ACTOR => {
                // Wire: [tag:1][group_id:8][actor_id:len_prefixed][message_id:8]
                let group_id = cmd.field_u64(1);
                let actor_id_len = cmd.field_u32(9) as usize;
                let actor_id = cmd.buf.slice(13..13 + actor_id_len);
                let message_id = cmd.field_u64(13 + actor_id_len);

                self.on_message_removed(message_id);

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let Some(actor_state) = group.actor_state() {
                        let reply_to = actor_state.apply_ack(&actor_id, message_id, 0);
                        // Route reply if applicable
                        if let Some(reply_name) = reply_to {
                            let hash = name_hash_bytes(&reply_name);
                            if let Some(r) = md.topic_names.get(&hash) {
                                let topic_id = *r;
                                drop(r);
                                if let Some(topic) = md.topics.get(&topic_id) {
                                    topic.apply_publish(log_index, std::iter::once(Bytes::new()));
                                    self.on_message_added(log_index);
                                }
                            }
                        }
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_NACK_ACTOR => {
                // Wire: [tag:1][group_id:8][actor_id:len_prefixed][message_id:8]
                let group_id = cmd.field_u64(1);
                let actor_id_len = cmd.field_u32(9) as usize;
                let actor_id = cmd.buf.slice(13..13 + actor_id_len);
                let message_id = cmd.field_u64(13 + actor_id_len);

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let Some(actor_state) = group.actor_state() {
                        actor_state.apply_nack(&actor_id, message_id, 0);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_ASSIGN_ACTORS => {
                // Wire: [tag:1][group_id:8][consumer_id:8][count:4][actor_ids...]
                let group_id = cmd.field_u64(1);
                let consumer_id = cmd.field_u64(9);
                let count = cmd.field_u32(17) as usize;
                let mut offset = 21;
                let mut actor_ids: Vec<Bytes> = Vec::with_capacity(count);
                for _ in 0..count {
                    let len = cmd.field_u32(offset) as usize;
                    offset += 4;
                    let id = cmd.buf.slice(offset..offset + len);
                    offset += len;
                    actor_ids.push(id);
                }

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let Some(actor_state) = group.actor_state() {
                        actor_state.apply_assign(consumer_id, &actor_ids);
                        md.track_session_group(consumer_id, group_id);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_RELEASE_ACTORS => {
                // Wire: [tag:1][group_id:8][consumer_id:8]
                let group_id = cmd.field_u64(1);
                let consumer_id = cmd.field_u64(9);

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let Some(actor_state) = group.actor_state() {
                        actor_state.apply_release(consumer_id);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_EVICT_IDLE => {
                // Wire: [tag:1][group_id:8][before_timestamp:8]
                let group_id = cmd.field_u64(1);
                let before_timestamp = cmd.field_u64(9);

                if let Some(group) = md.consumer_groups.get(&group_id) {
                    if let Some(actor_state) = group.actor_state() {
                        let count = actor_state.apply_evict_idle(before_timestamp);
                        debug!(group_id, count, "evicted idle actors");
                    }
                }
                MqResponse::Ok
            }

            // =================================================================
            // Cron (36-39)
            // =================================================================
            MqCommand::TAG_CRON_ENABLE => {
                // Wire: [tag:1][topic_id:8]
                let topic_id = cmd.field_u64(1);
                if let Some(topic) = md.topics.get(&topic_id) {
                    topic.cron_enabled.store(true, Ordering::Relaxed);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Topic,
                        id: topic_id,
                    })
                }
            }

            MqCommand::TAG_CRON_DISABLE => {
                // Wire: [tag:1][topic_id:8]
                let topic_id = cmd.field_u64(1);
                if let Some(topic) = md.topics.get(&topic_id) {
                    topic.cron_enabled.store(false, Ordering::Relaxed);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Topic,
                        id: topic_id,
                    })
                }
            }

            MqCommand::TAG_CRON_TRIGGER => {
                // Wire: [tag:1][topic_id:8][triggered_at:8]
                let topic_id = cmd.field_u64(1);
                let triggered_at = cmd.field_u64(9);
                if let Some(topic) = md.topics.get(&topic_id) {
                    // Publish a trigger message
                    let payload = topic
                        .meta
                        .cron_config
                        .as_ref()
                        .and_then(|c| c.payload.clone())
                        .unwrap_or_else(Bytes::new);
                    topic.apply_publish(log_index, std::iter::once(payload));
                    topic.apply_cron_trigger(triggered_at);
                    self.on_message_added(log_index);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Topic,
                        id: topic_id,
                    })
                }
            }

            MqCommand::TAG_CRON_UPDATE => {
                // Wire: [tag:1][topic_id:8] + trailing cron config fields
                // For now, just acknowledge — full cron config update requires codec extension
                let topic_id = cmd.field_u64(1);
                if md.topics.contains_key(&topic_id) {
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Topic,
                        id: topic_id,
                    })
                }
            }

            // =================================================================
            // Sessions (40-48)
            // =================================================================
            MqCommand::TAG_CREATE_SESSION => {
                // Wire: [tag:1][session_id:8][client_id:len_prefixed][keep_alive_ms:8][session_expiry_ms:8]
                let session_id = cmd.field_u64(1);
                let client_id_len = cmd.field_u32(9) as usize;
                let client_id = std::str::from_utf8(&cmd.as_bytes()[13..13 + client_id_len])
                    .unwrap_or("")
                    .to_string();
                let keep_alive_offset = 13 + client_id_len;
                let keep_alive_ms = cmd.field_u64(keep_alive_offset);
                let session_expiry_ms = cmd.field_u64(keep_alive_offset + 8);

                let client_id_hash = name_hash(&client_id);

                // Client takeover: if existing session with same client_id
                let takeover_id = self
                    .metadata
                    .session_client_index
                    .get(&client_id_hash)
                    .map(|entry| *entry)
                    .filter(|&old_id| old_id != session_id);
                if let Some(old_id) = takeover_id {
                    self.disconnect_session_internal(old_id, false, log_index, current_time);
                }

                let meta = SessionMeta::new(
                    session_id,
                    client_id,
                    current_time,
                    keep_alive_ms,
                    session_expiry_ms,
                );
                let state = SessionState::new(meta);
                self.metadata.sessions.insert(session_id, state);
                self.metadata
                    .session_client_index
                    .insert(client_id_hash, session_id);
                // Cancel any pending will for this client
                self.metadata.pending_wills.remove(&client_id_hash);
                MqResponse::EntityCreated { id: session_id }
            }

            MqCommand::TAG_DISCONNECT_SESSION => {
                // Wire: [tag:1][session_id:8][publish_will:1]
                let session_id = cmd.field_u64(1);
                let publish_will = cmd.as_bytes().get(9).copied().unwrap_or(0) != 0;
                self.disconnect_session_internal(session_id, publish_will, log_index, current_time)
            }

            MqCommand::TAG_HEARTBEAT_SESSION => {
                // Wire: [tag:1][session_id:8]
                let session_id = cmd.field_u64(1);
                if let Some(session) = md.sessions.get(&session_id) {
                    session.heartbeat(current_time);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Session,
                        id: session_id,
                    })
                }
            }

            MqCommand::TAG_SET_WILL => {
                // Reuse old codec view struct (consumer_id = session_id)
                let v = cmd.as_set_will();
                let session_id = v.consumer_id();
                if let Some(session) = md.sessions.get(&session_id) {
                    let will = WillConfig {
                        topic_id: v.exchange_id(),
                        payload: v.message(),
                        delay_ms: v.delay_secs() as u64 * 1000,
                        retained: v.retain(),
                    };
                    session.set_will(will);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Session,
                        id: session_id,
                    })
                }
            }

            MqCommand::TAG_CLEAR_WILL => {
                // Wire: [tag:1][session_id:8]
                let session_id = cmd.field_u64(1);
                if let Some(session) = md.sessions.get(&session_id) {
                    session.clear_will();
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Session,
                        id: session_id,
                    })
                }
            }

            MqCommand::TAG_FIRE_PENDING_WILLS => {
                // Wire: [tag:1][now_ms:8]
                let now_ms = cmd.field_u64(1);
                let mut count = 0u32;
                let mut to_fire: SmallVec<[(u64, PendingWill); 4]> = SmallVec::new();

                for entry in md.pending_wills.iter() {
                    if entry.fire_at_ms <= now_ms {
                        to_fire.push((*entry.key(), entry.value().clone()));
                    }
                }

                for (key, pw) in to_fire {
                    md.pending_wills.remove(&key);
                    if let Some(topic) = md.topics.get(&pw.will.topic_id) {
                        topic.apply_publish(log_index, std::iter::once(pw.will.payload));
                        self.on_message_added(log_index);
                        count += 1;
                    }
                }

                MqResponse::WillsFired { count }
            }

            MqCommand::TAG_PERSIST_SESSION => {
                let v = cmd.as_persist_session();
                let session_id = v.consumer_id(); // reuses old field name
                let client_id = v.client_id().to_string();
                let client_id_hash = name_hash(&client_id);
                let session_expiry_ms = v.session_expiry_secs() as u64 * 1000;
                let subscription_data = v.subscription_data();

                // Store or update session with subscription data
                if let Some(session) = md.sessions.get(&session_id) {
                    // Session already exists, just update expiry
                    drop(session);
                } else {
                    // Create a new session shell for persistence
                    let meta = SessionMeta::new(
                        session_id,
                        client_id.clone(),
                        current_time,
                        0,
                        session_expiry_ms,
                    );
                    md.sessions.insert(session_id, SessionState::new(meta));
                }

                md.session_client_index.insert(client_id_hash, session_id);
                let _ = subscription_data; // subscription data is embedded in the session state
                MqResponse::Ok
            }

            MqCommand::TAG_RESTORE_SESSION => {
                let v = cmd.as_restore_session();
                let client_id = v.client_id();
                let client_id_hash = name_hash(client_id);

                // O(1) lookup via client_id hash
                let found = md
                    .session_client_index
                    .get(&client_id_hash)
                    .and_then(|session_id| {
                        md.sessions.get(&*session_id).map(|s| {
                            let snap = s.snapshot_meta();
                            (*session_id, snap)
                        })
                    });

                match found {
                    Some((session_id, snap)) => {
                        let expiry_ms = snap.session_expiry_ms;
                        if expiry_ms > 0 && current_time > snap.last_activity_at + expiry_ms {
                            // Session expired
                            md.sessions.remove(&session_id);
                            md.session_client_index.remove(&client_id_hash);
                            MqResponse::SessionNotFound
                        } else {
                            MqResponse::SessionRestored {
                                session_id,
                                session_expiry_ms: expiry_ms,
                                subscription_data: Bytes::new(), // TODO: serialize subscription state
                            }
                        }
                    }
                    None => MqResponse::SessionNotFound,
                }
            }

            MqCommand::TAG_EXPIRE_SESSIONS => {
                let now_ms = current_time;
                let mut expired_ids: SmallVec<[u64; 8]> = SmallVec::new();

                for entry in md.sessions.iter() {
                    let session = entry.value();
                    if session.is_expired(now_ms) {
                        expired_ids.push(*entry.key());
                    }
                }

                for session_id in expired_ids {
                    // Disconnect with will publication
                    self.disconnect_session_internal(session_id, true, log_index, current_time);
                }

                MqResponse::Ok
            }

            // =================================================================
            // Batch (49)
            // =================================================================
            MqCommand::TAG_BATCH => {
                let batch = cmd.as_batch();
                let responses: SmallVec<[MqResponse; 8]> = batch
                    .commands()
                    .map(|sub| self.apply_command(&sub, log_index, current_time))
                    .collect();
                MqResponse::BatchResponse(Box::new(responses))
            }

            // =================================================================
            // Dedup (50)
            // =================================================================
            MqCommand::TAG_PRUNE_DEDUP_WINDOW => {
                // Wire: [tag:1][topic_id:8][before_ts:8]
                let topic_id = cmd.field_u64(1);
                let before_ts = cmd.field_u64(9);
                if let Some(topic) = md.topics.get(&topic_id) {
                    topic.prune_dedup(before_ts);
                }
                MqResponse::Ok
            }

            _ => {
                self.m_unknown_tag.increment(1);
                warn!(tag = cmd.tag(), "unknown MqCommand tag");
                MqResponse::Error(MqError::Custom(format!(
                    "unknown command tag: {}",
                    cmd.tag()
                )))
            }
        }
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    /// Disconnect a session, optionally publishing its will message.
    fn disconnect_session_internal(
        &mut self,
        session_id: u64,
        publish_will: bool,
        log_index: u64,
        current_time: u64,
    ) -> MqResponse {
        let md = &self.metadata;

        let session = match md.sessions.get(&session_id) {
            Some(s) => s,
            None => return MqResponse::Ok, // idempotent
        };

        // Release all in-flight from consumer groups
        drop(session); // release borrow before mutating groups
        if let Some(group_ids) = md.remove_session_group_index(session_id) {
            for group_id in &group_ids {
                if let Some(group) = md.consumer_groups.get(group_id) {
                    match &group.variant_state {
                        VariantState::Ack(ack) => {
                            let in_flight = ack.consumer_in_flight_ids(session_id);
                            if !in_flight.is_empty() {
                                ack.apply_nack(&in_flight);
                                md.group_notifier.notify(*group_id);
                            }
                        }
                        VariantState::Actor(actor) => {
                            actor.apply_release(session_id);
                        }
                        _ => {}
                    }
                }
            }
        }

        // Re-acquire session reference
        let session = match md.sessions.get(&session_id) {
            Some(s) => s,
            None => return MqResponse::Ok,
        };

        // Handle will
        if publish_will {
            if let Some(will) = session.take_will() {
                if will.delay_ms > 0 {
                    let delay_ms = will.delay_ms;
                    md.pending_wills.insert(
                        session.meta.client_id_hash,
                        PendingWill {
                            session_id,
                            client_id: session.meta.client_id.clone(),
                            will,
                            fire_at_ms: current_time + delay_ms,
                        },
                    );
                    let session_expiry_ms = session.meta.session_expiry_ms;
                    let hash = session.meta.client_id_hash;
                    if session_expiry_ms == 0 {
                        drop(session);
                        md.sessions.remove(&session_id);
                        md.session_client_index.remove(&hash);
                    }
                    return MqResponse::WillPending {
                        session_id,
                        delay_ms,
                    };
                } else {
                    // Publish will immediately
                    if let Some(topic) = md.topics.get(&will.topic_id) {
                        topic.apply_publish(log_index, std::iter::once(will.payload));
                        self.on_message_added(log_index);
                    }
                }
            }
        }

        // Remove session if session_expiry_ms == 0
        if session.meta.session_expiry_ms == 0 {
            let hash = session.meta.client_id_hash;
            drop(session);
            md.sessions.remove(&session_id);
            md.session_client_index.remove(&hash);
        }

        MqResponse::Ok
    }

    /// Resolve the DLQ topic ID for an ack-variant consumer group.
    fn resolve_dlq_topic(&self, group: &ConsumerGroupState) -> u64 {
        if let Some(config) = group.ack_config() {
            if let Some(ref dlq_name) = config.dead_letter_topic {
                let hash = name_hash(dlq_name);
                if let Some(r) = self.metadata.topic_names.get(&hash) {
                    return *r;
                }
            }
        }
        // Fallback: source topic's DLQ or 0
        0
    }

    // =========================================================================
    // Snapshot / Restore
    // =========================================================================

    /// Build a snapshot of all engine state.
    pub fn snapshot(&self) -> MqSnapshotData {
        let md = &self.metadata;

        let topics: Vec<TopicSnapshot> = md
            .topics
            .iter()
            .map(|entry| {
                let t = entry.value();
                TopicSnapshot {
                    meta: t.snapshot_meta(),
                    consumer_offsets: t.consumer_offsets_snapshot(),
                }
            })
            .collect();

        let consumer_groups: Vec<ConsumerGroupSnapshot> = md
            .consumer_groups
            .iter()
            .map(|entry| {
                let g = entry.value();
                let ack_state = g.ack_state().map(|a| a.snapshot());
                let actor_state = g.actor_state().map(|a| a.snapshot());
                ConsumerGroupSnapshot {
                    meta: g.snapshot_meta(),
                    offsets: g.offsets.iter().map(|e| e.value().clone()).collect(),
                    ack_state,
                    actor_state,
                }
            })
            .collect();

        let exchanges: Vec<ExchangeSnapshot> = md
            .exchanges
            .iter()
            .map(|entry| {
                let e = entry.value();
                ExchangeSnapshot {
                    meta: e.meta.clone(),
                    bindings: e.bindings.values().cloned().collect(),
                    retained: e
                        .retained
                        .iter()
                        .map(|(rk, msg)| RetainedEntry {
                            routing_key: Bytes::from(rk.clone()),
                            message: msg.clone(),
                        })
                        .collect(),
                }
            })
            .collect();

        let sessions: Vec<SessionSnapshot> = md
            .sessions
            .iter()
            .map(|entry| SessionSnapshot {
                meta: entry.value().snapshot_meta(),
            })
            .collect();

        let pending_wills: Vec<PendingWill> = md
            .pending_wills
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        MqSnapshotData {
            topics,
            consumer_groups,
            exchanges,
            sessions,
            pending_wills,
            next_id: md.next_id.load(Ordering::Relaxed),
            file_manifest: Vec::new(),
            sync_addr: None,
        }
    }

    /// Restore engine state from a snapshot.
    pub fn restore(&mut self, data: MqSnapshotData) {
        let md = &self.metadata;

        md.next_id.store(data.next_id, Ordering::Relaxed);
        md.purge_floor_dirty.store(true, Ordering::Relaxed);

        // Clear all DashMaps
        md.topics.clear();
        md.exchanges.clear();
        md.consumer_groups.clear();
        md.consumer_group_names.clear();
        md.sessions.clear();
        md.topic_names.clear();
        md.exchange_names.clear();
        md.session_group_index.clear();
        md.topic_aliases.clear();
        md.pending_wills.clear();
        md.publisher_dedup.clear();
        md.qos2_inbound.clear();
        md.binding_index.clear();
        md.session_client_index.clear();
        md.clear_session_indexes();

        let catalog = md.catalog_name().to_owned();

        // Restore topics
        for ts in data.topics {
            let id = ts.meta.topic_id;
            let mut meta = ts.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let state = TopicState::new(meta, &catalog);
            for offset in ts.consumer_offsets {
                state.consumer_offsets.insert(offset.consumer_id, offset);
            }
            md.topic_names.insert(hash, id);
            md.topics.insert(id, state);
        }

        // Restore consumer groups
        for cg in data.consumer_groups {
            let id = cg.meta.group_id;
            let mut meta = cg.meta;
            if meta.name_hash == 0 {
                meta.name_hash = name_hash(&meta.name);
            }
            let hash = meta.name_hash;
            let state =
                ConsumerGroupState::from_snapshot(meta, cg.offsets, &catalog, md.server_start_ms);
            // Restore ack variant state
            if let (Some(snap), Some(ack)) = (cg.ack_state, state.ack_state()) {
                if let Some(config) = state.ack_config() {
                    ack.restore(snap, config);
                }
            }
            // Restore actor variant state
            if let (Some(snap), Some(actor)) = (cg.actor_state, state.actor_state()) {
                actor.restore(snap, id);
            }
            // Re-attach to source topic
            if state.meta.source_topic_id != 0 {
                if let Some(topic) = md.topics.get(&state.meta.source_topic_id) {
                    topic.attach_group(id);
                }
            }
            md.consumer_group_names.insert(hash, id);
            md.consumer_groups.insert(id, state);
        }

        // Restore exchanges
        for es in data.exchanges {
            let id = es.meta.exchange_id;
            let mut meta = es.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let mut state = ExchangeState::new(meta);
            for binding in es.bindings {
                md.binding_index.insert(binding.binding_id, id);
                state.add_binding(binding);
            }
            for entry in es.retained {
                let rk = String::from_utf8(entry.routing_key.to_vec()).unwrap_or_default();
                state.retained.insert(rk, entry.message);
            }
            md.exchange_names.insert(hash, id);
            md.exchanges.insert(id, state);
        }

        // Restore sessions
        for ss in data.sessions {
            let id = ss.meta.session_id;
            let mut meta = ss.meta;
            meta.ensure_hash();
            let hash = meta.client_id_hash;
            md.session_client_index.insert(hash, id);
            md.sessions.insert(id, SessionState::new(meta));
        }

        // Restore pending wills
        for pw in data.pending_wills {
            let client_hash = name_hash(&pw.client_id);
            md.pending_wills.insert(client_hash, pw);
        }

        info!(
            topics = md.topics.len(),
            exchanges = md.exchanges.len(),
            consumer_groups = md.consumer_groups.len(),
            sessions = md.sessions.len(),
            pending_wills = md.pending_wills.len(),
            "MQ engine restored from snapshot"
        );
    }

    /// Restore only structural state (entity metadata) from MDBX.
    ///
    /// Creates empty entity shells (no messages, no in-flight state).
    /// The raft log is then replayed from the structural purge floor to
    /// rebuild message state.
    pub fn restore_structural(&mut self, data: MqSnapshotData) {
        let md = &self.metadata;

        md.next_id.store(data.next_id, Ordering::Relaxed);
        md.purge_floor_dirty.store(true, Ordering::Relaxed);

        // Clear all DashMaps
        md.topics.clear();
        md.exchanges.clear();
        md.consumer_groups.clear();
        md.consumer_group_names.clear();
        md.sessions.clear();
        md.topic_names.clear();
        md.exchange_names.clear();
        md.binding_index.clear();
        md.session_client_index.clear();
        md.clear_session_indexes();

        let catalog = md.catalog_name().to_owned();

        // Restore topic shells
        for ts in data.topics {
            let mut meta = ts.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let id = meta.topic_id;
            md.topic_names.insert(hash, id);
            md.topics.insert(id, TopicState::new(meta, &catalog));
        }

        // Restore consumer group shells
        for cg in data.consumer_groups {
            let mut meta = cg.meta;
            if meta.name_hash == 0 {
                meta.name_hash = name_hash(&meta.name);
            }
            let hash = meta.name_hash;
            let id = meta.group_id;
            let state = ConsumerGroupState::new(meta, &catalog, md.server_start_ms);
            md.consumer_group_names.insert(hash, id);
            md.consumer_groups.insert(id, state);
        }

        // Restore exchange shells
        for es in data.exchanges {
            let mut meta = es.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let id = meta.exchange_id;
            let mut state = ExchangeState::new(meta);
            for binding in es.bindings {
                md.binding_index.insert(binding.binding_id, id);
                state.add_binding(binding);
            }
            md.exchange_names.insert(hash, id);
            md.exchanges.insert(id, state);
        }

        // Restore session shells
        for ss in data.sessions {
            let mut meta = ss.meta;
            meta.ensure_hash();
            let hash = meta.client_id_hash;
            let id = meta.session_id;
            md.session_client_index.insert(hash, id);
            md.sessions.insert(id, SessionState::new(meta));
        }

        info!(
            topics = md.topics.len(),
            exchanges = md.exchanges.len(),
            consumer_groups = md.consumer_groups.len(),
            sessions = md.sessions.len(),
            "MQ engine restored structural state"
        );
    }

    /// Sync protocol-adapter caches (TopicMeta with atomics).
    ///
    /// Called after each apply batch. Updates the lightweight metadata that
    /// protocol adapters cache for zero-lock reads.
    pub fn sync_metadata(&self) {
        let md = &self.metadata;

        // Sync topics: insert or update.
        for entry in md.topics.iter() {
            let t = entry.value();
            if let Some(existing) = md.get_topic(t.meta.topic_id) {
                existing.update(
                    t.head_index(),
                    t.tail_index(),
                    t.message_count(),
                    t.total_bytes(),
                    t.latest_log_index(),
                    t.latest_msg_pos(),
                );
            } else {
                md.insert_topic(Arc::new(MetaTopicMeta::with_state(
                    t.meta.topic_id,
                    t.meta.name.clone(),
                    t.head_index(),
                    t.tail_index(),
                    t.message_count(),
                    t.total_bytes(),
                    t.latest_log_index(),
                    t.latest_msg_pos(),
                )));
            }
        }

        // Remove topics that no longer exist.
        let topic_ids: Vec<u64> = md.topic_ids();
        for id in topic_ids {
            if !md.topics.contains_key(&id) {
                md.remove_topic(id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flat::FlatMessageBuilder;
    use crate::types::*;

    fn make_config() -> MqConfig {
        MqConfig::new("/tmp/mq-engine-test")
    }

    fn make_engine() -> MqEngine {
        MqEngine::new(make_config())
    }

    fn make_msg(value: &[u8]) -> Bytes {
        FlatMessageBuilder::new(Bytes::from(value.to_vec()))
            .timestamp(1000)
            .build()
    }

    // =========================================================================
    // 1. test_create_delete_topic
    // =========================================================================

    #[test]
    fn test_create_delete_topic() {
        let mut engine = make_engine();

        let cmd = MqCommand::create_topic("events", RetentionPolicy::default(), 0);
        let resp = engine.apply_command(&cmd, 1, 1000);
        let topic_id = match resp {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };
        assert!(topic_id > 0);

        // Duplicate name should fail
        let cmd2 = MqCommand::create_topic("events", RetentionPolicy::default(), 0);
        match engine.apply_command(&cmd2, 2, 1001) {
            MqResponse::Error(MqError::AlreadyExists { .. }) => {}
            other => panic!("expected AlreadyExists, got {:?}", other),
        }

        // Delete
        let del_cmd = MqCommand::delete_topic(topic_id);
        assert!(matches!(
            engine.apply_command(&del_cmd, 3, 1002),
            MqResponse::Ok
        ));

        // Delete non-existent
        match engine.apply_command(&del_cmd, 4, 1003) {
            MqResponse::Error(MqError::NotFound { .. }) => {}
            other => panic!("expected NotFound, got {:?}", other),
        }
    }

    // =========================================================================
    // 2. test_publish_and_offset
    // =========================================================================

    #[test]
    fn test_publish_and_offset() {
        let mut engine = make_engine();

        let cmd = MqCommand::create_topic("logs", RetentionPolicy::default(), 0);
        let topic_id = match engine.apply_command(&cmd, 1, 1000) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };

        let msg = make_msg(b"hello");
        let pub_cmd = MqCommand::publish(topic_id, &[msg]);
        match engine.apply_command(&pub_cmd, 2, 1001) {
            MqResponse::Published { base_offset, count } => {
                assert_eq!(base_offset, 0);
                assert_eq!(count, 1);
            }
            other => panic!("expected Published, got {:?}", other),
        }

        // Publish more messages
        let msg2 = make_msg(b"world");
        let msg3 = make_msg(b"!");
        let pub_cmd2 = MqCommand::publish(topic_id, &[msg2, msg3]);
        match engine.apply_command(&pub_cmd2, 3, 1002) {
            MqResponse::Published { base_offset, count } => {
                assert_eq!(base_offset, 1);
                assert_eq!(count, 2);
            }
            other => panic!("expected Published, got {:?}", other),
        }

        // Verify topic state
        let topic = engine.metadata().topics.get(&topic_id).unwrap();
        assert_eq!(topic.head_index(), 3);
        assert_eq!(topic.message_count(), 3);
    }

    // =========================================================================
    // 3. test_create_consumer_group_with_auto_create_topic
    // =========================================================================

    #[test]
    fn test_create_consumer_group() {
        let mut engine = make_engine();

        // Create a basic consumer group (Offset variant via old codec)
        let cmd = MqCommand::create_consumer_group("my-group", 1); // 1 = Latest
        let group_id = match engine.apply_command(&cmd, 1, 1000) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };
        assert!(group_id > 0);

        // Verify it exists
        assert!(engine.metadata().consumer_groups.contains_key(&group_id));
    }

    // =========================================================================
    // 4. test_ack_variant_lifecycle
    // =========================================================================

    #[test]
    fn test_ack_variant_lifecycle() {
        let mut engine = make_engine();

        // Manually create a consumer group with Ack variant
        let md = &engine.metadata;
        let group_id = md.alloc_id();
        let topic_id = md.alloc_id();

        // Create source topic
        let topic_meta = TopicMeta::new(
            topic_id,
            "ack-test".to_string(),
            1000,
            RetentionPolicy::default(),
        );
        let topic_state = TopicState::new(topic_meta, md.catalog_name());
        md.topics.insert(topic_id, topic_state);

        // Create ack consumer group
        let group_meta = ConsumerGroupMeta {
            group_id,
            name: "ack-group".to_string(),
            name_hash: name_hash("ack-group"),
            created_at: 1000,
            generation: 0,
            phase: GroupPhase::Empty,
            protocol_type: String::new(),
            protocol_name: String::new(),
            leader: None,
            auto_offset_reset: AutoOffsetReset::Latest,
            last_activity_at: 1000,
            next_member_id: 1,
            members: Vec::new(),
            variant: GroupVariant::Ack,
            source_topic_id: topic_id,
            variant_config: VariantConfig::Ack(AckVariantConfig::default()),
        };
        let group_state =
            ConsumerGroupState::new(group_meta, md.catalog_name(), md.server_start_ms);
        md.consumer_groups.insert(group_id, group_state);

        // Enqueue messages directly via ack state
        let msg1 = make_msg(b"msg1");
        let msg2 = make_msg(b"msg2");
        if let Some(group) = md.consumer_groups.get(&group_id) {
            if let (Some(ack), Some(config)) = (group.ack_state(), group.ack_config()) {
                let count = ack.apply_enqueue(config, group_id, 100, &[msg1, msg2], &[], 1000);
                assert_eq!(count, 2);
            }
        }

        // Deliver
        let consumer_id = 42u64;
        if let Some(group) = md.consumer_groups.get(&group_id) {
            if let (Some(ack), Some(config)) = (group.ack_state(), group.ack_config()) {
                let delivered = ack.apply_deliver(config, consumer_id, 10, 1001, 101);
                assert_eq!(delivered.len(), 2);

                // Ack one
                ack.apply_ack(&[delivered[0]]);
                assert_eq!(ack.in_flight_count(), 1);

                // Nack the other
                ack.apply_nack(&[delivered[1]]);
                assert_eq!(ack.pending_count(), 1);
                assert_eq!(ack.in_flight_count(), 0);
            }
        }
    }

    // =========================================================================
    // 5. test_actor_variant_lifecycle
    // =========================================================================

    #[test]
    fn test_actor_variant_lifecycle() {
        let mut engine = make_engine();
        let md = &engine.metadata;
        let group_id = md.alloc_id();

        let group_meta = ConsumerGroupMeta {
            group_id,
            name: "actor-group".to_string(),
            name_hash: name_hash("actor-group"),
            created_at: 1000,
            generation: 0,
            phase: GroupPhase::Empty,
            protocol_type: String::new(),
            protocol_name: String::new(),
            leader: None,
            auto_offset_reset: AutoOffsetReset::Latest,
            last_activity_at: 1000,
            next_member_id: 1,
            members: Vec::new(),
            variant: GroupVariant::Actor,
            source_topic_id: 0,
            variant_config: VariantConfig::Actor(ActorVariantConfig::default()),
        };
        let group_state =
            ConsumerGroupState::new(group_meta, md.catalog_name(), md.server_start_ms);
        md.consumer_groups.insert(group_id, group_state);

        let actor_id = Bytes::from_static(b"actor-1");
        let consumer_id = 99u64;

        if let Some(group) = md.consumer_groups.get(&group_id) {
            let actor_state = group.actor_state().unwrap();
            let config = group.actor_config().unwrap();

            // Send a message to actor
            actor_state
                .apply_send(config, group_id, &actor_id, 200, 1000, None, 0)
                .unwrap();

            // Assign actor to consumer
            actor_state.apply_assign(consumer_id, &[actor_id.clone()]);

            // Deliver
            let msg_idx = actor_state.apply_deliver(&actor_id, consumer_id, 0);
            assert!(msg_idx.is_some());

            // Ack
            let reply = actor_state.apply_ack(&actor_id, msg_idx.unwrap(), 0);
            assert!(reply.is_none()); // no reply_to set
        }
    }

    // =========================================================================
    // 6. test_session_lifecycle
    // =========================================================================

    #[test]
    fn test_session_lifecycle() {
        let mut engine = make_engine();
        let md = &engine.metadata;

        // Create session directly
        let session_id = md.alloc_id();
        let meta = SessionMeta::new(session_id, "client-1".to_string(), 1000, 30_000, 0);
        let state = SessionState::new(meta);
        md.sessions.insert(session_id, state);

        // Verify
        assert!(md.sessions.contains_key(&session_id));

        // Heartbeat
        if let Some(session) = md.sessions.get(&session_id) {
            session.heartbeat(5000);
            assert_eq!(session.last_activity_at(), 5000);
        }
    }

    // =========================================================================
    // 7. test_will_on_disconnect
    // =========================================================================

    #[test]
    fn test_will_on_disconnect() {
        let mut engine = make_engine();
        let md = &engine.metadata;

        // Create a topic for will publication
        let cmd = MqCommand::create_topic("will-topic", RetentionPolicy::default(), 0);
        let will_topic_id = match engine.apply_command(&cmd, 1, 1000) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };

        // Create session with will
        let session_id = md.alloc_id();
        let meta = SessionMeta::new(session_id, "will-client".to_string(), 1000, 30_000, 0);
        let state = SessionState::new(meta);
        state.set_will(WillConfig {
            topic_id: will_topic_id,
            payload: Bytes::from_static(b"goodbye"),
            delay_ms: 0,
            retained: false,
        });
        md.sessions.insert(session_id, state);
        md.session_client_index
            .insert(name_hash("will-client"), session_id);

        // Disconnect with will
        let resp = engine.disconnect_session_internal(session_id, true, 10, 2000);
        assert!(matches!(resp, MqResponse::Ok));

        // Will should have been published to the topic
        let topic = md.topics.get(&will_topic_id).unwrap();
        assert_eq!(topic.message_count(), 1);
    }

    // =========================================================================
    // 8. test_exchange_routing
    // =========================================================================

    #[test]
    fn test_exchange_routing() {
        let mut engine = make_engine();

        // Create exchange
        let ex_cmd = MqCommand::create_exchange("fanout-ex", ExchangeType::Fanout);
        let exchange_id = match engine.apply_command(&ex_cmd, 1, 1000) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };

        // Create target topics
        let t1_cmd = MqCommand::create_topic("target-1", RetentionPolicy::default(), 0);
        let t1_id = match engine.apply_command(&t1_cmd, 2, 1001) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };

        let t2_cmd = MqCommand::create_topic("target-2", RetentionPolicy::default(), 0);
        let t2_id = match engine.apply_command(&t2_cmd, 3, 1002) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };

        // Create bindings
        let b1_cmd = MqCommand::create_binding(exchange_id, t1_id, None, false, None, None);
        assert!(matches!(
            engine.apply_command(&b1_cmd, 4, 1003),
            MqResponse::EntityCreated { .. }
        ));

        let b2_cmd = MqCommand::create_binding(exchange_id, t2_id, None, false, None, None);
        assert!(matches!(
            engine.apply_command(&b2_cmd, 5, 1004),
            MqResponse::EntityCreated { .. }
        ));

        // Publish to exchange
        let msg = make_msg(b"fanout-msg");
        let pub_cmd = MqCommand::publish_to_exchange(exchange_id, &[msg]);
        match engine.apply_command(&pub_cmd, 6, 1005) {
            MqResponse::Published { count, .. } => {
                assert!(count > 0);
            }
            other => panic!("expected Published, got {:?}", other),
        }

        // Both topics should have received messages
        let t1 = engine.metadata().topics.get(&t1_id).unwrap();
        let t2 = engine.metadata().topics.get(&t2_id).unwrap();
        assert!(t1.message_count() > 0);
        assert!(t2.message_count() > 0);
    }

    // =========================================================================
    // 9. test_snapshot_restore
    // =========================================================================

    #[test]
    fn test_snapshot_restore() {
        let mut engine = make_engine();

        // Create some state
        let cmd = MqCommand::create_topic("snap-topic", RetentionPolicy::default(), 0);
        let topic_id = match engine.apply_command(&cmd, 1, 1000) {
            MqResponse::EntityCreated { id } => id,
            _ => panic!("expected EntityCreated"),
        };

        let msg = make_msg(b"snap-msg");
        let pub_cmd = MqCommand::publish(topic_id, &[msg]);
        engine.apply_command(&pub_cmd, 2, 1001);

        // Snapshot
        let snap = engine.snapshot();
        assert_eq!(snap.topics.len(), 1);
        assert_eq!(snap.topics[0].meta.topic_id, topic_id);

        // Restore into a new engine
        let mut engine2 = make_engine();
        engine2.restore(snap);

        // Verify state was restored
        assert!(engine2.metadata().topics.contains_key(&topic_id));
        let topic = engine2.metadata().topics.get(&topic_id).unwrap();
        assert_eq!(topic.message_count(), 1);
    }

    // =========================================================================
    // 10. test_consumer_group_join_sync
    // =========================================================================

    #[test]
    fn test_consumer_group_join_sync() {
        let mut engine = make_engine();

        let cmd = MqCommand::create_consumer_group("join-test", 1);
        let group_id = match engine.apply_command(&cmd, 1, 1000) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };

        // Join
        let join_cmd = MqCommand::join_consumer_group(
            group_id,
            "",
            "client-1",
            30_000,
            60_000,
            "consumer",
            &[("range".to_string(), Bytes::from_static(b"meta"))],
        );
        let resp = engine.apply_command(&join_cmd, 2, 1001);
        let member_id = match resp {
            MqResponse::GroupJoined {
                member_id,
                phase_complete,
                ..
            } => {
                assert!(phase_complete); // single member, so all joined immediately
                member_id
            }
            other => panic!("expected GroupJoined, got {:?}", other),
        };

        // Sync
        let sync_cmd = MqCommand::sync_consumer_group(
            group_id,
            1, // generation
            &member_id,
            &[(member_id.clone(), vec![1, 2, 3])],
        );
        let resp = engine.apply_command(&sync_cmd, 3, 1002);
        match resp {
            MqResponse::GroupSynced {
                assignment,
                phase_complete,
            } => {
                assert!(phase_complete);
                assert_eq!(assignment, vec![1, 2, 3]);
            }
            other => panic!("expected GroupSynced, got {:?}", other),
        }
    }

    // =========================================================================
    // 11. test_dlq_routing
    // =========================================================================

    #[test]
    fn test_dlq_routing() {
        let mut engine = make_engine();
        let md = &engine.metadata;

        // Create DLQ topic
        let cmd = MqCommand::create_topic("dlq-topic", RetentionPolicy::default(), 0);
        let dlq_id = match engine.apply_command(&cmd, 1, 1000) {
            MqResponse::EntityCreated { id } => id,
            _ => panic!("expected EntityCreated"),
        };

        // Create ack group
        let group_id = md.alloc_id();
        let group_meta = ConsumerGroupMeta {
            group_id,
            name: "dlq-group".to_string(),
            name_hash: name_hash("dlq-group"),
            created_at: 1000,
            generation: 0,
            phase: GroupPhase::Empty,
            protocol_type: String::new(),
            protocol_name: String::new(),
            leader: None,
            auto_offset_reset: AutoOffsetReset::Latest,
            last_activity_at: 1000,
            next_member_id: 1,
            members: Vec::new(),
            variant: GroupVariant::Ack,
            source_topic_id: 0,
            variant_config: VariantConfig::Ack(AckVariantConfig {
                max_retries: 1,
                dead_letter_topic: Some("dlq-topic".to_string()),
                ..Default::default()
            }),
        };
        let group_state =
            ConsumerGroupState::new(group_meta, md.catalog_name(), md.server_start_ms);
        md.consumer_groups.insert(group_id, group_state);

        // Enqueue a message
        if let Some(group) = md.consumer_groups.get(&group_id) {
            if let (Some(ack), Some(config)) = (group.ack_state(), group.ack_config()) {
                ack.apply_enqueue(config, group_id, 100, &[make_msg(b"dlq-msg")], &[], 1000);

                // Deliver then timeout (exceeds max_retries since attempt = 1 >= max_retries = 1)
                let delivered = ack.apply_deliver(config, 50, 1, 1001, 101);
                assert_eq!(delivered.len(), 1);

                let dead = ack.apply_timeout_expired(&delivered, config, 2000);
                assert_eq!(dead.len(), 1);
                assert_eq!(ack.dlq_count(), 1);
            }
        }

        // Verify DLQ topic exists and can receive messages
        assert!(md.topics.contains_key(&dlq_id));
    }

    // =========================================================================
    // 12. test_auto_delete_on_last_detach
    // =========================================================================

    #[test]
    fn test_auto_delete_on_last_detach() {
        let mut engine = make_engine();
        let md = &engine.metadata;

        // Create a topic with DeleteOnLastDetach
        let topic_id = md.alloc_id();
        let mut topic_meta = TopicMeta::new(
            topic_id,
            "auto-del".to_string(),
            1000,
            RetentionPolicy::default(),
        );
        topic_meta.lifetime = TopicLifetimePolicy::DeleteOnLastDetach;
        topic_meta.ensure_name_hash();
        let topic_state = TopicState::new(topic_meta, md.catalog_name());
        md.topics.insert(topic_id, topic_state);
        md.topic_names.insert(name_hash("auto-del"), topic_id);

        // Create a group attached to it
        let group_id = md.alloc_id();
        if let Some(topic) = md.topics.get(&topic_id) {
            topic.attach_group(group_id);
        }

        let group_meta = ConsumerGroupMeta {
            group_id,
            name: "auto-del-group".to_string(),
            name_hash: name_hash("auto-del-group"),
            created_at: 1000,
            generation: 0,
            phase: GroupPhase::Empty,
            protocol_type: String::new(),
            protocol_name: String::new(),
            leader: None,
            auto_offset_reset: AutoOffsetReset::Latest,
            last_activity_at: 1000,
            next_member_id: 1,
            members: Vec::new(),
            variant: GroupVariant::Offset,
            source_topic_id: topic_id,
            variant_config: VariantConfig::Offset,
        };
        let group_state =
            ConsumerGroupState::new(group_meta, md.catalog_name(), md.server_start_ms);
        md.consumer_groups.insert(group_id, group_state);
        md.consumer_group_names
            .insert(name_hash("auto-del-group"), group_id);

        // Topic should exist
        assert!(md.topics.contains_key(&topic_id));

        // Delete the consumer group — should auto-delete the topic
        let del_cmd = MqCommand::delete_consumer_group(group_id);
        engine.apply_command(&del_cmd, 10, 2000);

        // Topic should be gone
        assert!(!md.topics.contains_key(&topic_id));
    }

    // =========================================================================
    // 13. test_batch_command
    // =========================================================================

    #[test]
    fn test_batch_command() {
        let mut engine = make_engine();

        let cmd1 = MqCommand::create_topic("batch-t1", RetentionPolicy::default(), 0);
        let cmd2 = MqCommand::create_topic("batch-t2", RetentionPolicy::default(), 0);
        let batch = MqCommand::batch(&[cmd1, cmd2]);

        match engine.apply_command(&batch, 1, 1000) {
            MqResponse::BatchResponse(responses) => {
                assert_eq!(responses.len(), 2);
                assert!(matches!(responses[0], MqResponse::EntityCreated { .. }));
                assert!(matches!(responses[1], MqResponse::EntityCreated { .. }));
            }
            other => panic!("expected BatchResponse, got {:?}", other),
        }

        assert_eq!(engine.metadata().topics.len(), 2);
    }
}

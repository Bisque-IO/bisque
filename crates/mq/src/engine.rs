//! Unified MQ engine — topics + consumer groups + sessions.
//!
//! All entity state lives in `Arc<MqMetadata>` (papaya::HashMap + atomics) for lock-free
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
/// All entity state lives in `MqMetadata` (papaya::HashMap + atomics) for lock-free
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
        &self,
        cmd: &MqCommand,
        log_index: u64,
        current_time: u64,
        segment_id: Option<u64>,
    ) -> MqResponse {
        self.m_apply_count.increment(1);
        let md = &self.metadata;

        match cmd.tag() {
            // =================================================================
            // Topics (0-5)
            // =================================================================
            MqCommand::TAG_CREATE_TOPIC => {
                let v = cmd.as_create_topic();
                let name_str = v.name();
                let name_h = name_hash(name_str);
                if let Some(r) = md.topic_names.pin().get(&name_h) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::Topic,
                        id: *r,
                    });
                }
                let topic_id = md.alloc_id();
                let name = name_str.to_owned();
                let mut meta = TopicMeta::new(topic_id, name, current_time, v.retention());
                meta.partition_count = v.partition_count();
                // Extended fields: lifetime, retained, dedup, cron are parsed
                // from the trailing portion of the create_topic buffer.
                // The CmdCreateTopic view struct currently only exposes
                // name/retention/partition_count. For now, those extended fields
                // are set to defaults and can be updated by the caller via
                // separate commands or once the codec is extended.
                let state = TopicState::new(meta, md.catalog_name());
                md.topics.pin().insert(topic_id, Arc::new(state));
                md.topic_names.pin().insert(name_h, topic_id);
                info!(topic_id, "topic created");
                MqResponse::EntityCreated { id: topic_id }
            }

            MqCommand::TAG_DELETE_TOPIC => {
                let topic_id = cmd.field_u64(8);
                let guard = md.topics.pin();
                if let Some(state) = guard.get(&topic_id).cloned() {
                    guard.remove(&topic_id);
                    md.topic_names.pin().remove(&state.meta.name_hash);
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
                if let Some(topic) = md.topics.pin().get(&topic_id) {
                    if topic.consumer_group_ids.is_empty() {
                        // Fast path: no attached groups, skip collect + enqueue
                        let base_offset = topic.apply_publish(log_index, v.messages(), segment_id);
                        self.on_message_added(log_index);
                        MqResponse::Published { base_offset, count }
                    } else {
                        let messages: SmallVec<[Bytes; 16]> = v.messages().collect();
                        let base_offset =
                            topic.apply_publish(log_index, messages.iter().cloned(), segment_id);
                        let group_ids: SmallVec<[u64; 4]> = topic
                            .consumer_group_ids
                            .pin()
                            .iter()
                            .map(|(&k, _)| k)
                            .collect();
                        Self::auto_enqueue_attached_groups(
                            md,
                            &group_ids,
                            &messages,
                            log_index,
                            current_time,
                        );
                        self.on_message_added(log_index);
                        MqResponse::Published { base_offset, count }
                    }
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Topic,
                        id: topic_id,
                    })
                }
            }

            MqCommand::TAG_COMMIT_OFFSET => {
                // Wire: [tag:1][topic_id:8][consumer_id:8][offset:8]
                let topic_id = cmd.field_u64(8);
                let consumer_id = cmd.field_u64(16);
                let offset = cmd.field_u64(24);
                if let Some(topic) = md.topics.pin().get(&topic_id) {
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
                let topic_id = cmd.field_u64(8);
                let before_index = cmd.field_u64(16);
                if let Some(topic) = md.topics.pin().get(&topic_id) {
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
                if let Some(exchange) = md.exchanges.pin().get(&exchange_id) {
                    let routing_key = v.routing_key().to_owned();
                    let message = v.message();
                    let retained = match segment_id {
                        Some(seg_id) => RetainedValue::mmap_backed(seg_id, message),
                        None => RetainedValue::heap(message),
                    };
                    exchange.retained.write().insert(routing_key, retained);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Exchange,
                        id: exchange_id,
                    })
                }
            }

            MqCommand::TAG_GET_RETAINED => {
                let v = cmd.as_get_retained();
                let exchange_id = v.exchange_id();
                let filter = v.routing_key_filter();
                if let Some(exchange) = md.exchanges.pin().get(&exchange_id) {
                    let retained = exchange.retained.read();
                    let messages: Vec<RetainedEntry> = retained
                        .iter()
                        .filter(|(key, _)| match &filter {
                            Some(pattern) => crate::exchange::topic_pattern_matches(pattern, key),
                            None => true,
                        })
                        .map(|(key, rv)| RetainedEntry {
                            routing_key: Bytes::from(key.clone()),
                            message: rv.message.clone(),
                        })
                        .collect();
                    MqResponse::RetainedMessages { messages }
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Exchange,
                        id: exchange_id,
                    })
                }
            }

            MqCommand::TAG_DELETE_RETAINED => {
                let v = cmd.as_delete_retained();
                let exchange_id = v.exchange_id();
                if let Some(exchange) = md.exchanges.pin().get(&exchange_id) {
                    let routing_key = v.routing_key();
                    exchange.retained.write().remove(routing_key);
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
                let name_str = v.name();
                let exchange_type = v.exchange_type();
                let name_h = name_hash(name_str);
                if let Some(r) = md.exchange_names.pin().get(&name_h) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::Exchange,
                        id: *r,
                    });
                }
                let id = md.alloc_id();
                let name = name_str.to_owned();
                let meta = ExchangeMeta::new(id, name, current_time, exchange_type);
                md.exchanges
                    .pin()
                    .insert(id, Arc::new(ExchangeState::new(meta)));
                md.exchange_names.pin().insert(name_h, id);
                info!(exchange_id = id, "exchange created");
                MqResponse::EntityCreated { id }
            }

            MqCommand::TAG_DELETE_EXCHANGE => {
                let exchange_id = cmd.field_u64(8);
                let guard = md.exchanges.pin();
                if let Some(state) = guard.get(&exchange_id).cloned() {
                    guard.remove(&exchange_id);
                    md.exchange_names.pin().remove(&state.meta.name_hash);
                    // Clean up binding reverse index
                    let binding_guard = md.binding_index.pin();
                    for binding_id in state.bindings.read().keys() {
                        binding_guard.remove(binding_id);
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

                if !md.exchanges.pin().contains_key(&exchange_id) {
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
                if let Some(exchange) = md.exchanges.pin().get(&exchange_id) {
                    exchange.add_binding(binding);
                }
                md.binding_index.pin().insert(binding_id, exchange_id);
                MqResponse::EntityCreated { id: binding_id }
            }

            MqCommand::TAG_DELETE_BINDING => {
                let binding_id = cmd.field_u64(8);
                let binding_guard = md.binding_index.pin();
                if let Some(&exchange_id) = binding_guard.get(&binding_id) {
                    binding_guard.remove(&binding_id);
                    if let Some(exchange) = md.exchanges.pin().get(&exchange_id) {
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

                let exchange_guard = md.exchanges.pin();
                let exchange = match exchange_guard.get(&exchange_id) {
                    Some(e) => e,
                    None => {
                        return MqResponse::Error(MqError::NotFound {
                            entity: EntityKind::Exchange,
                            id: exchange_id,
                        });
                    }
                };

                let messages: SmallVec<[Bytes; 16]> = v.messages().collect();
                let first_msg = messages.first();
                let envelope = first_msg.and_then(|m| crate::flat::MqttEnvelope::new(m));
                let flat = if envelope.is_none() {
                    first_msg.and_then(|m| crate::flat::FlatMessage::new(m))
                } else {
                    None
                };
                let routing_key_bytes: Option<&[u8]> = envelope
                    .as_ref()
                    .map(|e| e.topic())
                    .or_else(|| flat.as_ref().and_then(|f| f.routing_key()));
                let routing_key_str = routing_key_bytes
                    .as_ref()
                    .and_then(|b| std::str::from_utf8(b).ok());
                let targets = exchange.route(routing_key_str);

                let mut total_count = 0u64;
                for &target_id in &targets {
                    if let Some(topic) = md.topics.pin().get(&target_id) {
                        topic.apply_publish(log_index, messages.iter().cloned(), segment_id);
                        total_count += messages.len() as u64;
                        if !topic.consumer_group_ids.is_empty() {
                            let group_ids: SmallVec<[u64; 4]> = topic
                                .consumer_group_ids
                                .pin()
                                .iter()
                                .map(|(&k, _)| k)
                                .collect();
                            Self::auto_enqueue_attached_groups(
                                md,
                                &group_ids,
                                &messages,
                                log_index,
                                current_time,
                            );
                        }
                    } else if let Some(group) = md.consumer_groups.pin().get(&target_id) {
                        // Target is a consumer group — publish to its source topic
                        // and enqueue into its ack state
                        let src = group.meta.source_topic_id;
                        let group_id = target_id;
                        if src != 0 {
                            if let Some(topic) = md.topics.pin().get(&src) {
                                topic.apply_publish(
                                    log_index,
                                    messages.iter().cloned(),
                                    segment_id,
                                );
                                total_count += messages.len() as u64;
                            }
                        }
                        if let (Some(ack), Some(config)) = (group.ack_state(), group.ack_config()) {
                            ack.apply_enqueue(
                                config,
                                group_id,
                                log_index,
                                &messages,
                                &[],
                                current_time,
                            );
                            md.group_notifier.notify(group_id);
                        }
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
                let name_str = v.name();
                let name_h = name_hash(name_str);

                // Check for name collision with existing groups
                if md.consumer_group_names.pin().contains_key(&name_h) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::ConsumerGroup,
                        id: 0,
                    });
                }
                let name = name_str.to_owned();

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
                    let topic_id = if let Some(existing) = md.topic_names.pin().get(&topic_name_h) {
                        *existing
                    } else {
                        let tid = md.alloc_id();
                        let mut topic_meta =
                            TopicMeta::new(tid, name.clone(), current_time, v.topic_retention());
                        topic_meta.lifetime = v.topic_lifetime();
                        topic_meta.dedup_config = v.topic_dedup();
                        let topic_state = TopicState::new(topic_meta, md.catalog_name());
                        md.topics.pin().insert(tid, Arc::new(topic_state));
                        md.topic_names.pin().insert(topic_name_h, tid);
                        tid
                    };
                    // Attach group to topic
                    if let Some(topic) = md.topics.pin().get(&topic_id) {
                        topic.attach_group(group_id);
                    }
                    topic_id
                } else {
                    0
                };

                // Auto-create DLQ topic if specified
                if let Some(dlq_name) = v.dlq_topic_name() {
                    let dlq_h = name_hash(&dlq_name);
                    if !md.topic_names.pin().contains_key(&dlq_h) {
                        let dlq_id = md.alloc_id();
                        let dlq_meta = TopicMeta::new(
                            dlq_id,
                            dlq_name,
                            current_time,
                            RetentionPolicy::default(),
                        );
                        let dlq_state = TopicState::new(dlq_meta, md.catalog_name());
                        md.topics.pin().insert(dlq_id, Arc::new(dlq_state));
                        md.topic_names.pin().insert(dlq_h, dlq_id);
                    }
                }

                // Auto-create response topic if specified
                if let Some(resp_name) = v.response_topic_name() {
                    let resp_h = name_hash(&resp_name);
                    if !md.topic_names.pin().contains_key(&resp_h) {
                        let resp_id = md.alloc_id();
                        let resp_meta = TopicMeta::new(
                            resp_id,
                            resp_name,
                            current_time,
                            RetentionPolicy::default(),
                        );
                        let resp_state = TopicState::new(resp_meta, md.catalog_name());
                        md.topics.pin().insert(resp_id, Arc::new(resp_state));
                        md.topic_names.pin().insert(resp_h, resp_id);
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
                md.consumer_group_names.pin().insert(name_h, group_id);
                md.consumer_groups.pin().insert(group_id, Arc::new(state));
                info!(
                    group_id,
                    ?variant,
                    source_topic_id,
                    "consumer group created"
                );
                MqResponse::EntityCreated { id: group_id }
            }

            MqCommand::TAG_DELETE_CONSUMER_GROUP => {
                let group_id = cmd.field_u64(8);
                let cg_guard = md.consumer_groups.pin();
                if let Some(group) = cg_guard.get(&group_id).cloned() {
                    cg_guard.remove(&group_id);
                    md.consumer_group_names.pin().remove(&group.meta.name_hash);
                    let source = group.meta.source_topic_id;
                    if source != 0 {
                        if let Some(topic) = md.topics.pin().get(&source) {
                            topic.detach_group(group_id);
                            if topic.should_auto_delete() {
                                let topic_id = topic.meta.topic_id;
                                let nh = topic.meta.name_hash;
                                md.topics.pin().remove(&topic_id);
                                md.topic_names.pin().remove(&nh);
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

                let cg_guard = md.consumer_groups.pin();
                let group = match cg_guard.get(&group_id) {
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

                let cg_guard = md.consumer_groups.pin();
                let group = match cg_guard.get(&group_id) {
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

                let cg_guard = md.consumer_groups.pin();
                let group = match cg_guard.get(&group_id) {
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

                let cg_guard = md.consumer_groups.pin();
                let group = match cg_guard.get(&group_id) {
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

                let cg_guard = md.consumer_groups.pin();
                let group = match cg_guard.get(&group_id) {
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

                group.offsets.pin().insert(
                    (v.topic_id(), v.partition_index()),
                    GroupTopicPartitionOffset {
                        topic_id: v.topic_id(),
                        partition_index: v.partition_index(),
                        committed_offset: v.offset(),
                        metadata: v.metadata_bytes().map(Bytes::copy_from_slice),
                        committed_at: v.timestamp(),
                    },
                );

                group.touch_activity(current_time);
                group.record_offset_commit();
                MqResponse::Ok
            }

            MqCommand::TAG_EXPIRE_GROUP_SESSIONS => {
                // Wire: [tag:1][now_ms:8]
                let now_ms = cmd.field_u64(8);

                for (_, group) in md.consumer_groups.pin().iter() {
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
                // Wire: @8 group_id:8, @16 consumer_id:8, @24 max_count:4,
                //       @32 exclude_publisher_id:8, @40 current_time_ms:8
                let group_id = cmd.field_u64(8);
                let consumer_id = cmd.field_u64(16);
                let max_count = cmd.field_u32(24);
                // Extended fields (backward-compat: default to 0 if fixed region is shorter).
                let fixed_sz = cmd.fixed_size() as usize;
                let exclude_publisher_id = if fixed_sz >= 40 { cmd.field_u64(32) } else { 0 };
                let deliver_time_ms = if fixed_sz >= 48 { cmd.field_u64(40) } else { 0 };

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
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
                        let msgs_guard = ack.messages.pin();

                        // Engine-level filtering: no-local and message expiry.
                        let needs_filter = exclude_publisher_id != 0 || deliver_time_ms != 0;
                        let mut filtered_ids: SmallVec<[u64; 4]> = SmallVec::new();
                        let msgs: SmallVec<[DeliveredMessage; 8]> = delivered
                            .iter()
                            .filter_map(|&msg_id| {
                                if needs_filter {
                                    if let Some(meta) = msgs_guard.get(&msg_id) {
                                        // No-local: skip messages from same publisher session.
                                        if exclude_publisher_id != 0
                                            && meta.publisher_id == exclude_publisher_id
                                        {
                                            filtered_ids.push(msg_id);
                                            return None;
                                        }
                                        // Message expiry: skip messages past TTL.
                                        if deliver_time_ms != 0 {
                                            if let Some(expires_at) = meta.expires_at {
                                                if deliver_time_ms >= expires_at {
                                                    filtered_ids.push(msg_id);
                                                    return None;
                                                }
                                            }
                                        }
                                    }
                                }
                                let attempt = msgs_guard.get(&msg_id).map_or(1, |m| m.attempts);
                                Some(DeliveredMessage {
                                    message_id: msg_id,
                                    attempt,
                                    original_timestamp: current_time,
                                    group_id,
                                })
                            })
                            .collect();
                        // ACK filtered messages at engine level.
                        if !filtered_ids.is_empty() {
                            ack.apply_ack(&filtered_ids);
                        }
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
                // Wire v2: @8 group_id:u64, @16 message_ids:vec_u64, @24 response:opt_flex8
                let group_id = cmd.field_u64(8);
                let message_ids = cmd.field_vec_u64(16);

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
                    if let Some(ack) = group.ack_state() {
                        // Collect reply_to info before acking
                        let msgs_guard = ack.messages.pin();
                        let reply_pairs: SmallVec<[(u64, Bytes); 4]> = message_ids
                            .iter()
                            .filter_map(|&msg_id| {
                                msgs_guard.get(&msg_id).and_then(|m| {
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
                            if let Some(&topic_id) = md.topic_names.pin().get(&hash) {
                                if let Some(topic) = md.topics.pin().get(&topic_id) {
                                    // Publish an empty acknowledgment to the reply topic
                                    topic.apply_publish(
                                        log_index,
                                        std::iter::once(Bytes::new()),
                                        segment_id,
                                    );
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
                // Wire v2: @8 group_id:u64, @16 message_ids:vec_u64
                let group_id = cmd.field_u64(8);
                let message_ids = cmd.field_vec_u64(16);

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
                    if let Some(ack) = group.ack_state() {
                        ack.apply_nack(&message_ids);
                        md.group_notifier.notify(group_id);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_RELEASE => {
                // Wire v2: @8 group_id:u64, @16 message_ids:vec_u64
                let group_id = cmd.field_u64(8);
                let message_ids = cmd.field_vec_u64(16);

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
                    if let Some(ack) = group.ack_state() {
                        ack.apply_release(&message_ids);
                        md.group_notifier.notify(group_id);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_MODIFY => {
                // Wire v2: @8 group_id:u64, @16 message_ids:vec_u64
                let group_id = cmd.field_u64(8);
                let message_ids = cmd.field_vec_u64(16);

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
                    if let Some(ack) = group.ack_state() {
                        // NACK returns to pending with incremented attempts
                        ack.apply_nack(&message_ids);
                        md.group_notifier.notify(group_id);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_EXTEND_VISIBILITY => {
                // Wire v2: @8 group_id:u64, @16 message_ids:vec_u64, @24 extension_ms:u64
                let group_id = cmd.field_u64(8);
                let message_ids = cmd.field_vec_u64(16);
                let extension_ms = cmd.field_u64(24);

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
                    if let Some(ack) = group.ack_state() {
                        ack.apply_extend_visibility(&message_ids, extension_ms);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_TIMEOUT_EXPIRED => {
                // Wire v2: @8 group_id:u64, @16 message_ids:vec_u64
                let group_id = cmd.field_u64(8);
                let message_ids = cmd.field_vec_u64(16);

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
                    if let (Some(ack), Some(config)) = (group.ack_state(), group.ack_config()) {
                        let dead_lettered =
                            ack.apply_timeout_expired(&message_ids, config, current_time);
                        if !dead_lettered.is_empty() {
                            // Resolve DLQ topic
                            let dlq_topic_id = self.resolve_dlq_topic(group);
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
                if let Some(topic) = md.topics.pin().get(&dlq_topic_id) {
                    topic.apply_publish(log_index, messages, None);
                    self.on_message_added(log_index);
                }
                // Remove dead-lettered messages from the source group's ack state
                if let Some(group) = md.consumer_groups.pin().get(&source_group_id) {
                    if let Some(ack) = group.ack_state() {
                        ack.apply_remove_dead_lettered(&dead_letter_ids);
                        self.mark_purge_floor_dirty();
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_EXPIRE_PENDING => {
                // Wire v2: @8 group_id:u64, @16 message_ids:vec_u64
                let group_id = cmd.field_u64(8);
                let message_ids = cmd.field_vec_u64(16);

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
                    if let Some(ack) = group.ack_state() {
                        ack.apply_expire_pending(&message_ids);
                        self.mark_purge_floor_dirty();
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_PURGE => {
                // Wire: [tag:1][group_id:8]
                let group_id = cmd.field_u64(8);
                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
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
                let group_id = cmd.field_u64(8);
                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
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
                // Wire v2: @8 group_id:u64, @16 consumer_id:u64, @24 actor_ids:vec_bytes
                let group_id = cmd.field_u64(8);
                let consumer_id = cmd.field_u64(16);
                let actor_count = cmd.field_vec_bytes_count(24) as usize;
                let mut actor_ids: SmallVec<[Bytes; 8]> = SmallVec::with_capacity(actor_count);
                for i in 0..actor_count {
                    actor_ids.push(cmd.field_vec_bytes_get_bytes(24, i));
                }

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
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
                // Wire v2: @8 group_id:u64, @16 message_id:u64, @24 actor_id:flex8
                let group_id = cmd.field_u64(8);
                let message_id = cmd.field_u64(16);
                let actor_id_data = cmd.field_flex8(24);
                let actor_id = Bytes::copy_from_slice(actor_id_data);

                self.on_message_removed(message_id);

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
                    if let Some(actor_state) = group.actor_state() {
                        let reply_to = actor_state.apply_ack(&actor_id, message_id, 0);
                        // Route reply if applicable
                        if let Some(reply_name) = reply_to {
                            let hash = name_hash_bytes(&reply_name);
                            if let Some(&topic_id) = md.topic_names.pin().get(&hash) {
                                if let Some(topic) = md.topics.pin().get(&topic_id) {
                                    topic.apply_publish(
                                        log_index,
                                        std::iter::once(Bytes::new()),
                                        segment_id,
                                    );
                                    self.on_message_added(log_index);
                                }
                            }
                        }
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_NACK_ACTOR => {
                // Wire v2: @8 group_id:u64, @16 message_id:u64, @24 actor_id:flex8
                let group_id = cmd.field_u64(8);
                let message_id = cmd.field_u64(16);
                let actor_id_data = cmd.field_flex8(24);
                let actor_id = Bytes::copy_from_slice(actor_id_data);

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
                    if let Some(actor_state) = group.actor_state() {
                        actor_state.apply_nack(&actor_id, message_id, 0);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_ASSIGN_ACTORS => {
                // Wire v2: @8 group_id:u64, @16 consumer_id:u64, @24 actor_ids:vec_bytes
                let group_id = cmd.field_u64(8);
                let consumer_id = cmd.field_u64(16);
                let count = cmd.field_vec_bytes_count(24) as usize;
                let mut actor_ids: Vec<Bytes> = Vec::with_capacity(count);
                for i in 0..count {
                    actor_ids.push(cmd.field_vec_bytes_get_bytes(24, i));
                }

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
                    if let Some(actor_state) = group.actor_state() {
                        actor_state.apply_assign(consumer_id, &actor_ids);
                        md.track_session_group(consumer_id, group_id);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_RELEASE_ACTORS => {
                // Wire: [tag:1][group_id:8][consumer_id:8]
                let group_id = cmd.field_u64(8);
                let consumer_id = cmd.field_u64(16);

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
                    if let Some(actor_state) = group.actor_state() {
                        actor_state.apply_release(consumer_id);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_GROUP_EVICT_IDLE => {
                // Wire: [tag:1][group_id:8][before_timestamp:8]
                let group_id = cmd.field_u64(8);
                let before_timestamp = cmd.field_u64(16);

                if let Some(group) = md.consumer_groups.pin().get(&group_id) {
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
                let topic_id = cmd.field_u64(8);
                if let Some(topic) = md.topics.pin().get(&topic_id) {
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
                let topic_id = cmd.field_u64(8);
                if let Some(topic) = md.topics.pin().get(&topic_id) {
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
                let topic_id = cmd.field_u64(8);
                let triggered_at = cmd.field_u64(16);
                if let Some(topic) = md.topics.pin().get(&topic_id) {
                    // Publish a trigger message
                    let payload = topic
                        .meta
                        .cron_config
                        .as_ref()
                        .and_then(|c| c.payload.clone())
                        .unwrap_or_else(Bytes::new);
                    topic.apply_publish(log_index, std::iter::once(payload), None);
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
                let topic_id = cmd.field_u64(8);
                if md.topics.pin().contains_key(&topic_id) {
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
                // Wire v2: @8 session_id:u64, @16 keep_alive_ms:u64, @24 session_expiry_ms:u64, @32 client_id:flex8
                let session_id = cmd.field_u64(8);
                let client_id = cmd.field_flex8_str(32).to_string();
                let keep_alive_ms = cmd.field_u64(16);
                let session_expiry_ms = cmd.field_u64(24);

                let client_id_hash = name_hash(&client_id);

                // Client takeover: if existing session with same client_id
                let takeover_id = self
                    .metadata
                    .session_client_index
                    .pin()
                    .get(&client_id_hash)
                    .copied()
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
                self.metadata
                    .sessions
                    .pin()
                    .insert(session_id, Arc::new(state));
                self.metadata
                    .session_client_index
                    .pin()
                    .insert(client_id_hash, session_id);
                // Cancel any pending will for this client
                self.metadata.pending_wills.pin().remove(&client_id_hash);
                MqResponse::EntityCreated { id: session_id }
            }

            MqCommand::TAG_DISCONNECT_SESSION => {
                // Wire v2: flags=publish_will, @8 session_id:u64
                let session_id = cmd.field_u64(8);
                let publish_will = cmd.flags() != 0;
                self.disconnect_session_internal(session_id, publish_will, log_index, current_time)
            }

            MqCommand::TAG_HEARTBEAT_SESSION => {
                // Wire: [tag:1][session_id:8]
                let session_id = cmd.field_u64(8);
                if let Some(session) = md.sessions.pin().get(&session_id) {
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
                if let Some(session) = md.sessions.pin().get(&session_id) {
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
                let session_id = cmd.field_u64(8);
                if let Some(session) = md.sessions.pin().get(&session_id) {
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
                let now_ms = cmd.field_u64(8);
                let mut count = 0u32;
                let mut to_fire: SmallVec<[(u64, PendingWill); 4]> = SmallVec::new();

                for (&key, pw) in md.pending_wills.pin().iter() {
                    if pw.fire_at_ms <= now_ms {
                        to_fire.push((key, pw.clone()));
                    }
                }

                let pw_guard = md.pending_wills.pin();
                for (key, pw) in to_fire {
                    pw_guard.remove(&key);
                    if self.publish_will_payload(pw.will.topic_id, pw.will.payload, log_index) {
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

                // Remove old session for this client_id if it maps to a different session_id
                if let Some(&old_id) = md.session_client_index.pin().get(&client_id_hash) {
                    if old_id != session_id {
                        md.sessions.pin().remove(&old_id);
                    }
                }

                // Preserve will from existing session if present
                let existing_will = md
                    .sessions
                    .pin()
                    .get(&session_id)
                    .and_then(|s| s.take_will());

                // Create or replace session with updated persistence state
                let mut meta = SessionMeta::new(
                    session_id,
                    client_id.clone(),
                    current_time,
                    0,
                    session_expiry_ms,
                );
                meta.subscription_data = subscription_data;
                meta.will = existing_will;
                md.sessions
                    .pin()
                    .insert(session_id, Arc::new(SessionState::new(meta)));

                md.session_client_index
                    .pin()
                    .insert(client_id_hash, session_id);
                MqResponse::Ok
            }

            MqCommand::TAG_RESTORE_SESSION => {
                let v = cmd.as_restore_session();
                let client_id = v.client_id();
                let client_id_hash = name_hash(client_id);

                // O(1) lookup via client_id hash
                let found =
                    md.session_client_index
                        .pin()
                        .get(&client_id_hash)
                        .and_then(|&session_id| {
                            md.sessions.pin().get(&session_id).map(|s| {
                                let snap = s.snapshot_meta();
                                (session_id, snap)
                            })
                        });

                match found {
                    Some((session_id, snap)) => {
                        let expiry_ms = snap.session_expiry_ms;
                        if expiry_ms > 0 && current_time > snap.last_activity_at + expiry_ms {
                            // Session expired
                            md.sessions.pin().remove(&session_id);
                            md.session_client_index.pin().remove(&client_id_hash);
                            MqResponse::SessionNotFound
                        } else {
                            MqResponse::SessionRestored {
                                session_id,
                                session_expiry_ms: expiry_ms,
                                subscription_data: snap.subscription_data.clone(),
                            }
                        }
                    }
                    None => MqResponse::SessionNotFound,
                }
            }

            MqCommand::TAG_EXPIRE_SESSIONS => {
                let now_ms = current_time;
                let mut expired_ids: SmallVec<[u64; 8]> = SmallVec::new();

                for (&session_id, session) in md.sessions.pin().iter() {
                    if session.is_expired(now_ms) {
                        expired_ids.push(session_id);
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
            MqCommand::TAG_BATCH => self.apply_batch(cmd, log_index, current_time, segment_id),

            // =================================================================
            // Dedup (50)
            // =================================================================
            MqCommand::TAG_PRUNE_DEDUP_WINDOW => {
                // Legacy — dedup GC is now local (not Raft-replicated).
                // Kept as no-op for rolling-upgrade wire compatibility.
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

    /// Process a TAG_BATCH command, detecting runs of consecutive sub-commands
    /// that target the same entity to amortize map lookups.
    fn apply_batch(
        &self,
        cmd: &MqCommand,
        log_index: u64,
        current_time: u64,
        segment_id: Option<u64>,
    ) -> MqResponse {
        let batch = cmd.as_batch();
        let count = batch.count() as usize;
        let cmds: SmallVec<[&[u8]; 16]> = batch.commands().collect();
        let mut responses: SmallVec<[MqResponse; 8]> = SmallVec::with_capacity(count);

        let mut i = 0;
        while i < cmds.len() {
            let tag = buf_tag(cmds[i]);

            // Detect consecutive runs of same (tag, entity_id) for batch optimization.
            // Entity ID is at @8 (offset 8..16) in v2 format.
            let has_entity_id = cmds[i].len() >= 16;

            if has_entity_id && tag == MqCommand::TAG_PUBLISH {
                let topic_id = buf_field_u64(cmds[i], 8);
                let run_end = self.find_run_end(&cmds, i, tag, topic_id);

                if run_end - i > 1 {
                    self.apply_publish_run(
                        &cmds[i..run_end],
                        topic_id,
                        log_index,
                        current_time,
                        segment_id,
                        &mut responses,
                    );
                    i = run_end;
                    continue;
                }
            }

            if has_entity_id
                && matches!(
                    tag,
                    MqCommand::TAG_GROUP_ACK
                        | MqCommand::TAG_GROUP_NACK
                        | MqCommand::TAG_GROUP_RELEASE
                )
            {
                let group_id = buf_field_u64(cmds[i], 8);
                let run_end = self.find_run_end(&cmds, i, tag, group_id);

                if run_end - i > 1 {
                    self.apply_ack_variant_run(tag, &cmds[i..run_end], group_id, &mut responses);
                    i = run_end;
                    continue;
                }
            }

            // Default: process single sub-command via temporary MqCommand wrapper
            let sub_cmd = MqCommand::from_vec(cmds[i].to_vec());
            responses.push(self.apply_command(&sub_cmd, log_index, current_time, segment_id));
            i += 1;
        }

        MqResponse::BatchResponse(Box::new(responses))
    }

    /// Find the end of a consecutive run of commands with the same tag and entity_id.
    #[inline]
    fn find_run_end(&self, cmds: &[&[u8]], start: usize, tag: u8, entity_id: u64) -> usize {
        let mut end = start + 1;
        while end < cmds.len()
            && buf_tag(cmds[end]) == tag
            && cmds[end].len() >= 16
            && buf_field_u64(cmds[end], 8) == entity_id
        {
            end += 1;
        }
        end
    }

    /// Process a run of consecutive TAG_PUBLISH commands targeting the same topic.
    /// Single map lookup for the entire run.
    fn apply_publish_run(
        &self,
        cmds: &[&[u8]],
        topic_id: u64,
        log_index: u64,
        current_time: u64,
        segment_id: Option<u64>,
        responses: &mut SmallVec<[MqResponse; 8]>,
    ) {
        use crate::codec::CmdPublish;
        let md = &self.metadata;
        if let Some(topic) = md.topics.pin().get(&topic_id) {
            let has_groups = !topic.consumer_group_ids.is_empty();
            if has_groups {
                let mut all_messages: SmallVec<[SmallVec<[Bytes; 16]>; 4]> =
                    SmallVec::with_capacity(cmds.len());
                for sub in cmds {
                    let v = CmdPublish::from_buf(sub);
                    let msgs: SmallVec<[Bytes; 16]> = v.messages().collect();
                    let msg_count = msgs.len() as u64;
                    let base_offset =
                        topic.apply_publish(log_index, msgs.iter().cloned(), segment_id);
                    responses.push(MqResponse::Published {
                        base_offset,
                        count: msg_count,
                    });
                    all_messages.push(msgs);
                }
                let group_ids: SmallVec<[u64; 4]> = topic
                    .consumer_group_ids
                    .pin()
                    .iter()
                    .map(|(&k, _)| k)
                    .collect();
                for msgs in &all_messages {
                    Self::auto_enqueue_attached_groups(
                        md,
                        &group_ids,
                        msgs,
                        log_index,
                        current_time,
                    );
                }
            } else {
                for sub in cmds {
                    let v = CmdPublish::from_buf(sub);
                    let msg_count = v.message_count() as u64;
                    let base_offset = topic.apply_publish(log_index, v.messages(), segment_id);
                    responses.push(MqResponse::Published {
                        base_offset,
                        count: msg_count,
                    });
                }
            }
            self.on_message_added(log_index);
        } else {
            for _ in cmds {
                responses.push(MqResponse::Error(MqError::NotFound {
                    entity: EntityKind::Topic,
                    id: topic_id,
                }));
            }
        }
    }

    /// Process a run of consecutive ack-variant commands (ACK/NACK/RELEASE)
    /// targeting the same consumer group. Single map lookup for the run.
    fn apply_ack_variant_run(
        &self,
        tag: u8,
        cmds: &[&[u8]],
        group_id: u64,
        responses: &mut SmallVec<[MqResponse; 8]>,
    ) {
        let md = &self.metadata;
        if let Some(group) = md.consumer_groups.pin().get(&group_id) {
            if let Some(ack) = group.ack_state() {
                for sub in cmds {
                    let ids = buf_field_vec_u64(sub, 16);
                    match tag {
                        MqCommand::TAG_GROUP_ACK => {
                            ack.apply_ack(&ids);
                            if let Some(min_id) = ids.iter().copied().min() {
                                self.on_message_removed(min_id);
                            }
                        }
                        MqCommand::TAG_GROUP_NACK => {
                            ack.apply_nack(&ids);
                        }
                        MqCommand::TAG_GROUP_RELEASE => {
                            ack.apply_release(&ids);
                        }
                        _ => unreachable!(),
                    }
                    responses.push(MqResponse::Ok);
                }
                if tag == MqCommand::TAG_GROUP_NACK || tag == MqCommand::TAG_GROUP_RELEASE {
                    md.group_notifier.notify(group_id);
                }
            } else {
                for _ in cmds {
                    responses.push(MqResponse::Ok);
                }
            }
        } else {
            for _ in cmds {
                responses.push(MqResponse::Ok);
            }
        }
    }

    /// Enqueue messages into ack-variant consumer groups attached to a topic.
    ///
    /// `group_ids` should be pre-collected from `topic.consumer_group_ids` so
    /// the topic map ref is already released before we borrow
    /// `md.consumer_groups`.
    #[inline]
    fn auto_enqueue_attached_groups(
        md: &MqMetadata,
        group_ids: &[u64],
        messages: &[Bytes],
        log_index: u64,
        current_time: u64,
    ) {
        let cg_guard = md.consumer_groups.pin();
        for gid in group_ids {
            if let Some(group) = cg_guard.get(gid) {
                if let (Some(ack), Some(config)) = (group.ack_state(), group.ack_config()) {
                    ack.apply_enqueue(config, *gid, log_index, messages, &[], current_time);
                    md.group_notifier.notify(*gid);
                }
            }
        }
    }

    /// Publish a will payload to a topic or exchange. Returns true if published.
    fn publish_will_payload(&self, topic_id: u64, payload: Bytes, log_index: u64) -> bool {
        let md = &self.metadata;
        let current_time = 0; // Will messages use log_index for message_id
        // Try direct topic first
        if let Some(topic) = md.topics.pin().get(&topic_id) {
            topic.apply_publish(log_index, std::iter::once(payload), None);
            self.on_message_added(log_index);
            return true;
        }
        // Fall back to exchange routing
        if let Some(exchange) = md.exchanges.pin().get(&topic_id) {
            let flat = crate::flat::FlatMessage::new(&payload);
            let routing_key_bytes = flat.as_ref().and_then(|f| f.routing_key());
            let routing_key_str = routing_key_bytes
                .as_ref()
                .and_then(|b| std::str::from_utf8(b).ok());
            let targets = exchange.route(routing_key_str);
            let mut published = false;
            for &target_id in &targets {
                // Try direct topic publish
                if let Some(topic) = md.topics.pin().get(&target_id) {
                    topic.apply_publish(log_index, std::iter::once(payload.clone()), None);
                    published = true;
                } else if let Some(group) = md.consumer_groups.pin().get(&target_id) {
                    // Target is a consumer group — publish to its source topic
                    // and enqueue into its ack state if applicable
                    let src = group.meta.source_topic_id;
                    let group_id = target_id;
                    if src != 0 {
                        if let Some(topic) = md.topics.pin().get(&src) {
                            topic.apply_publish(log_index, std::iter::once(payload.clone()), None);
                            published = true;
                        }
                    }
                    // Also enqueue into ack state for ack-variant groups
                    if let (Some(ack), Some(config)) = (group.ack_state(), group.ack_config()) {
                        ack.apply_enqueue(
                            config,
                            group_id,
                            log_index,
                            &[payload.clone()],
                            &[],
                            current_time,
                        );
                    }
                }
            }
            if published {
                self.on_message_added(log_index);
            }
            return published;
        }
        false
    }

    /// Disconnect a session, optionally publishing its will message.
    fn disconnect_session_internal(
        &self,
        session_id: u64,
        publish_will: bool,
        log_index: u64,
        current_time: u64,
    ) -> MqResponse {
        let md = &self.metadata;

        if md.sessions.pin().get(&session_id).is_none() {
            return MqResponse::Ok; // idempotent
        }

        // Release all in-flight from consumer groups
        if let Some(group_ids) = md.remove_session_group_index(session_id) {
            for group_id in &group_ids {
                if let Some(group) = md.consumer_groups.pin().get(group_id) {
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
        let session = match md.sessions.pin().get(&session_id).cloned() {
            Some(s) => s,
            None => return MqResponse::Ok,
        };

        // Handle will
        if publish_will {
            if let Some(will) = session.take_will() {
                if will.delay_ms > 0 {
                    let delay_ms = will.delay_ms;
                    md.pending_wills.pin().insert(
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
                        md.sessions.pin().remove(&session_id);
                        md.session_client_index.pin().remove(&hash);
                    }
                    return MqResponse::WillPending {
                        session_id,
                        delay_ms,
                    };
                } else {
                    // Publish will immediately
                    self.publish_will_payload(will.topic_id, will.payload, log_index);
                }
            }
        }

        // Remove session if session_expiry_ms == 0
        if session.meta.session_expiry_ms == 0 {
            let hash = session.meta.client_id_hash;
            md.sessions.pin().remove(&session_id);
            md.session_client_index.pin().remove(&hash);
        }

        MqResponse::Ok
    }

    /// Resolve the DLQ topic ID for an ack-variant consumer group.
    fn resolve_dlq_topic(&self, group: &ConsumerGroupState) -> u64 {
        if let Some(config) = group.ack_config() {
            if let Some(ref dlq_name) = config.dead_letter_topic {
                let hash = name_hash(dlq_name);
                if let Some(&r) = self.metadata.topic_names.pin().get(&hash) {
                    return r;
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
            .pin()
            .iter()
            .map(|(_, t)| TopicSnapshot {
                meta: t.snapshot_meta(),
                consumer_offsets: t.consumer_offsets_snapshot(),
            })
            .collect();

        let consumer_groups: Vec<ConsumerGroupSnapshot> = md
            .consumer_groups
            .pin()
            .iter()
            .map(|(_, g)| {
                let ack_state = g.ack_state().map(|a| a.snapshot());
                let actor_state = g.actor_state().map(|a| a.snapshot());
                ConsumerGroupSnapshot {
                    meta: g.snapshot_meta(),
                    offsets: g.offsets.pin().iter().map(|(_, e)| e.clone()).collect(),
                    ack_state,
                    actor_state,
                }
            })
            .collect();

        let exchanges: Vec<ExchangeSnapshot> = md
            .exchanges
            .pin()
            .iter()
            .map(|(_, e)| ExchangeSnapshot {
                meta: e.meta.clone(),
                bindings: e.bindings.read().values().cloned().collect(),
                retained: e
                    .retained
                    .read()
                    .iter()
                    .map(|(rk, rv)| RetainedEntry {
                        routing_key: Bytes::from(rk.clone()),
                        message: rv.message.clone(),
                    })
                    .collect(),
            })
            .collect();

        let sessions: Vec<SessionSnapshot> = md
            .sessions
            .pin()
            .iter()
            .map(|(_, s)| SessionSnapshot {
                meta: s.snapshot_meta(),
            })
            .collect();

        let pending_wills: Vec<PendingWill> = md
            .pending_wills
            .pin()
            .iter()
            .map(|(_, pw)| pw.clone())
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
    pub fn restore(&self, data: MqSnapshotData) {
        let md = &self.metadata;

        md.next_id.store(data.next_id, Ordering::Relaxed);
        md.purge_floor_dirty.store(true, Ordering::Relaxed);

        // Clear all maps
        md.topics.pin().clear();
        md.exchanges.pin().clear();
        md.consumer_groups.pin().clear();
        md.consumer_group_names.pin().clear();
        md.sessions.pin().clear();
        md.topic_names.pin().clear();
        md.exchange_names.pin().clear();
        md.session_group_index.pin().clear();
        md.topic_aliases.pin().clear();
        md.pending_wills.pin().clear();
        md.publisher_dedup.pin().clear();
        md.qos2_inbound.pin().clear();
        md.binding_index.pin().clear();
        md.session_client_index.pin().clear();
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
                state
                    .consumer_offsets
                    .pin()
                    .insert(offset.consumer_id, offset);
            }
            md.topic_names.pin().insert(hash, id);
            md.topics.pin().insert(id, Arc::new(state));
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
                if let Some(topic) = md.topics.pin().get(&state.meta.source_topic_id) {
                    topic.attach_group(id);
                }
            }
            md.consumer_group_names.pin().insert(hash, id);
            md.consumer_groups.pin().insert(id, Arc::new(state));
        }

        // Restore exchanges
        for es in data.exchanges {
            let id = es.meta.exchange_id;
            let mut meta = es.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let state = ExchangeState::new(meta);
            for binding in es.bindings {
                md.binding_index.pin().insert(binding.binding_id, id);
                state.add_binding(binding);
            }
            for entry in es.retained {
                let rk = String::from_utf8(entry.routing_key.to_vec()).unwrap_or_default();
                state
                    .retained
                    .write()
                    .insert(rk, RetainedValue::heap(entry.message));
            }
            md.exchange_names.pin().insert(hash, id);
            md.exchanges.pin().insert(id, Arc::new(state));
        }

        // Restore sessions
        for ss in data.sessions {
            let id = ss.meta.session_id;
            let mut meta = ss.meta;
            meta.ensure_hash();
            let hash = meta.client_id_hash;
            md.session_client_index.pin().insert(hash, id);
            md.sessions
                .pin()
                .insert(id, Arc::new(SessionState::new(meta)));
        }

        // Restore pending wills
        for pw in data.pending_wills {
            let client_hash = name_hash(&pw.client_id);
            md.pending_wills.pin().insert(client_hash, pw);
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
    pub fn restore_structural(&self, data: MqSnapshotData) {
        let md = &self.metadata;

        md.next_id.store(data.next_id, Ordering::Relaxed);
        md.purge_floor_dirty.store(true, Ordering::Relaxed);

        // Clear all maps
        md.topics.pin().clear();
        md.exchanges.pin().clear();
        md.consumer_groups.pin().clear();
        md.consumer_group_names.pin().clear();
        md.sessions.pin().clear();
        md.topic_names.pin().clear();
        md.exchange_names.pin().clear();
        md.binding_index.pin().clear();
        md.session_client_index.pin().clear();
        md.clear_session_indexes();

        let catalog = md.catalog_name().to_owned();

        // Restore topic shells
        for ts in data.topics {
            let mut meta = ts.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let id = meta.topic_id;
            md.topic_names.pin().insert(hash, id);
            md.topics
                .pin()
                .insert(id, Arc::new(TopicState::new(meta, &catalog)));
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
            md.consumer_group_names.pin().insert(hash, id);
            md.consumer_groups.pin().insert(id, Arc::new(state));
        }

        // Restore exchange shells
        for es in data.exchanges {
            let mut meta = es.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let id = meta.exchange_id;
            let state = ExchangeState::new(meta);
            for binding in es.bindings {
                md.binding_index.pin().insert(binding.binding_id, id);
                state.add_binding(binding);
            }
            md.exchange_names.pin().insert(hash, id);
            md.exchanges.pin().insert(id, Arc::new(state));
        }

        // Restore session shells
        for ss in data.sessions {
            let mut meta = ss.meta;
            meta.ensure_hash();
            let hash = meta.client_id_hash;
            let id = meta.session_id;
            md.session_client_index.pin().insert(hash, id);
            md.sessions
                .pin()
                .insert(id, Arc::new(SessionState::new(meta)));
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
        for (_, t) in md.topics.pin().iter() {
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
            if !md.topics.pin().contains_key(&id) {
                md.remove_topic(id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flat::FlatMessageBuilder;

    fn make_config() -> MqConfig {
        MqConfig::new("/tmp/mq-engine-test")
    }

    fn make_engine() -> MqEngine {
        MqEngine::new(make_config())
    }

    fn make_msg(value: &[u8]) -> Bytes {
        FlatMessageBuilder::new(value).timestamp(1000).build()
    }

    // =========================================================================
    // 1. test_create_delete_topic
    // =========================================================================

    #[test]
    fn test_create_delete_topic() {
        let mut engine = make_engine();

        let cmd = MqCommand::create_topic("events", RetentionPolicy::default(), 0);
        let resp = engine.apply_command(&cmd, 1, 1000, None);
        let topic_id = match resp {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };
        assert!(topic_id > 0);

        // Duplicate name should fail
        let cmd2 = MqCommand::create_topic("events", RetentionPolicy::default(), 0);
        match engine.apply_command(&cmd2, 2, 1001, None) {
            MqResponse::Error(MqError::AlreadyExists { .. }) => {}
            other => panic!("expected AlreadyExists, got {:?}", other),
        }

        // Delete
        let del_cmd = MqCommand::delete_topic(topic_id);
        assert!(matches!(
            engine.apply_command(&del_cmd, 3, 1002, None),
            MqResponse::Ok
        ));

        // Delete non-existent
        match engine.apply_command(&del_cmd, 4, 1003, None) {
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
        let topic_id = match engine.apply_command(&cmd, 1, 1000, None) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };

        let msg = make_msg(b"hello");
        let pub_cmd = MqCommand::publish(topic_id, &[msg]);
        match engine.apply_command(&pub_cmd, 2, 1001, None) {
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
        match engine.apply_command(&pub_cmd2, 3, 1002, None) {
            MqResponse::Published { base_offset, count } => {
                assert_eq!(base_offset, 1);
                assert_eq!(count, 2);
            }
            other => panic!("expected Published, got {:?}", other),
        }

        // Verify topic state
        let topic = engine
            .metadata()
            .topics
            .pin()
            .get(&topic_id)
            .unwrap()
            .clone();
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
        let group_id = match engine.apply_command(&cmd, 1, 1000, None) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };
        assert!(group_id > 0);

        // Verify it exists
        assert!(
            engine
                .metadata()
                .consumer_groups
                .pin()
                .contains_key(&group_id)
        );
    }

    // =========================================================================
    // 4. test_ack_variant_lifecycle
    // =========================================================================

    #[test]
    fn test_ack_variant_lifecycle() {
        let mut engine = make_engine();

        // Manually create a consumer group with Ack variant
        let md = engine.shared_metadata();
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
        md.topics.pin().insert(topic_id, Arc::new(topic_state));

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
        md.consumer_groups
            .pin()
            .insert(group_id, Arc::new(group_state));

        // Enqueue messages directly via ack state
        let msg1 = make_msg(b"msg1");
        let msg2 = make_msg(b"msg2");
        if let Some(group) = md.consumer_groups.pin().get(&group_id) {
            if let (Some(ack), Some(config)) = (group.ack_state(), group.ack_config()) {
                let count = ack.apply_enqueue(config, group_id, 100, &[msg1, msg2], &[], 1000);
                assert_eq!(count, 2);
            }
        }

        // Deliver
        let consumer_id = 42u64;
        if let Some(group) = md.consumer_groups.pin().get(&group_id) {
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
        let md = engine.shared_metadata();
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
        md.consumer_groups
            .pin()
            .insert(group_id, Arc::new(group_state));

        let actor_id = Bytes::from_static(b"actor-1");
        let consumer_id = 99u64;

        if let Some(group) = md.consumer_groups.pin().get(&group_id) {
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
        let md = engine.shared_metadata();

        // Create session directly
        let session_id = md.alloc_id();
        let meta = SessionMeta::new(session_id, "client-1".to_string(), 1000, 30_000, 0);
        let state = SessionState::new(meta);
        md.sessions.pin().insert(session_id, Arc::new(state));

        // Verify
        assert!(md.sessions.pin().contains_key(&session_id));

        // Heartbeat
        if let Some(session) = md.sessions.pin().get(&session_id) {
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
        let md = engine.shared_metadata();

        // Create a topic for will publication
        let cmd = MqCommand::create_topic("will-topic", RetentionPolicy::default(), 0);
        let will_topic_id = match engine.apply_command(&cmd, 1, 1000, None) {
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
        md.sessions.pin().insert(session_id, Arc::new(state));
        md.session_client_index
            .pin()
            .insert(name_hash("will-client"), session_id);

        // Disconnect with will
        let resp = engine.disconnect_session_internal(session_id, true, 10, 2000);
        assert!(matches!(resp, MqResponse::Ok));

        // Will should have been published to the topic
        let topic = md.topics.pin().get(&will_topic_id).unwrap().clone();
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
        let exchange_id = match engine.apply_command(&ex_cmd, 1, 1000, None) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };

        // Create target topics
        let t1_cmd = MqCommand::create_topic("target-1", RetentionPolicy::default(), 0);
        let t1_id = match engine.apply_command(&t1_cmd, 2, 1001, None) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };

        let t2_cmd = MqCommand::create_topic("target-2", RetentionPolicy::default(), 0);
        let t2_id = match engine.apply_command(&t2_cmd, 3, 1002, None) {
            MqResponse::EntityCreated { id } => id,
            other => panic!("expected EntityCreated, got {:?}", other),
        };

        // Create bindings
        let b1_cmd = MqCommand::create_binding(exchange_id, t1_id, None);
        assert!(matches!(
            engine.apply_command(&b1_cmd, 4, 1003, None),
            MqResponse::EntityCreated { .. }
        ));

        let b2_cmd = MqCommand::create_binding(exchange_id, t2_id, None);
        assert!(matches!(
            engine.apply_command(&b2_cmd, 5, 1004, None),
            MqResponse::EntityCreated { .. }
        ));

        // Publish to exchange
        let msg = make_msg(b"fanout-msg");
        let pub_cmd = MqCommand::publish_to_exchange(exchange_id, &[msg]);
        match engine.apply_command(&pub_cmd, 6, 1005, None) {
            MqResponse::Published { count, .. } => {
                assert!(count > 0);
            }
            other => panic!("expected Published, got {:?}", other),
        }

        // Both topics should have received messages
        let t1 = engine.metadata().topics.pin().get(&t1_id).unwrap().clone();
        let t2 = engine.metadata().topics.pin().get(&t2_id).unwrap().clone();
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
        let topic_id = match engine.apply_command(&cmd, 1, 1000, None) {
            MqResponse::EntityCreated { id } => id,
            _ => panic!("expected EntityCreated"),
        };

        let msg = make_msg(b"snap-msg");
        let pub_cmd = MqCommand::publish(topic_id, &[msg]);
        engine.apply_command(&pub_cmd, 2, 1001, None);

        // Snapshot
        let snap = engine.snapshot();
        assert_eq!(snap.topics.len(), 1);
        assert_eq!(snap.topics[0].meta.topic_id, topic_id);

        // Restore into a new engine
        let mut engine2 = make_engine();
        engine2.restore(snap);

        // Verify state was restored
        assert!(engine2.metadata().topics.pin().contains_key(&topic_id));
        let topic = engine2
            .metadata()
            .topics
            .pin()
            .get(&topic_id)
            .unwrap()
            .clone();
        assert_eq!(topic.message_count(), 1);
    }

    // =========================================================================
    // 10. test_consumer_group_join_sync
    // =========================================================================

    #[test]
    fn test_consumer_group_join_sync() {
        let mut engine = make_engine();

        let cmd = MqCommand::create_consumer_group("join-test", 1);
        let group_id = match engine.apply_command(&cmd, 1, 1000, None) {
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
            &[("range", b"meta" as &[u8])],
        );
        let resp = engine.apply_command(&join_cmd, 2, 1001, None);
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
            &[(&member_id as &str, &[1u8, 2, 3] as &[u8])],
        );
        let resp = engine.apply_command(&sync_cmd, 3, 1002, None);
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
        let md = engine.shared_metadata();

        // Create DLQ topic
        let cmd = MqCommand::create_topic("dlq-topic", RetentionPolicy::default(), 0);
        let dlq_id = match engine.apply_command(&cmd, 1, 1000, None) {
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
        md.consumer_groups
            .pin()
            .insert(group_id, Arc::new(group_state));

        // Enqueue a message
        if let Some(group) = md.consumer_groups.pin().get(&group_id) {
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
        assert!(md.topics.pin().contains_key(&dlq_id));
    }

    // =========================================================================
    // 12. test_auto_delete_on_last_detach
    // =========================================================================

    #[test]
    fn test_auto_delete_on_last_detach() {
        let mut engine = make_engine();
        let md = engine.shared_metadata();

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
        md.topics.pin().insert(topic_id, Arc::new(topic_state));
        md.topic_names.pin().insert(name_hash("auto-del"), topic_id);

        // Create a group attached to it
        let group_id = md.alloc_id();
        if let Some(topic) = md.topics.pin().get(&topic_id) {
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
        md.consumer_groups
            .pin()
            .insert(group_id, Arc::new(group_state));
        md.consumer_group_names
            .pin()
            .insert(name_hash("auto-del-group"), group_id);

        // Topic should exist
        assert!(md.topics.pin().contains_key(&topic_id));

        // Delete the consumer group — should auto-delete the topic
        let del_cmd = MqCommand::delete_consumer_group(group_id);
        engine.apply_command(&del_cmd, 10, 2000, None);

        // Topic should be gone
        assert!(!md.topics.pin().contains_key(&topic_id));
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

        match engine.apply_command(&batch, 1, 1000, None) {
            MqResponse::BatchResponse(responses) => {
                assert_eq!(responses.len(), 2);
                assert!(matches!(responses[0], MqResponse::EntityCreated { .. }));
                assert!(matches!(responses[1], MqResponse::EntityCreated { .. }));
            }
            other => panic!("expected BatchResponse, got {:?}", other),
        }

        assert_eq!(engine.metadata().topics.len(), 2); // papaya len() works without pin
    }
}

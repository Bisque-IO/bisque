use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use tracing::{debug, info, warn};

use crate::actor::ActorNamespaceState;
use crate::config::MqConfig;
use crate::consumer::ConsumerState;
use crate::exchange::ExchangeState;
use crate::job::JobInstance;
use crate::metadata::MqMetadata;
use crate::producer::ProducerMeta;
use crate::queue::QueueState;
use crate::topic::TopicState;
use crate::types::*;

/// Core MQ engine — thin wrapper around `Arc<MqMetadata>`.
///
/// All entity state lives in `MqMetadata` (DashMap + atomics) for lock-free
/// concurrent access. The engine holds the config and provides `apply_command`
/// which is the single-writer Raft apply path.
pub struct MqEngine {
    pub(crate) config: MqConfig,
    pub(crate) meta: Arc<MqMetadata>,
}

impl MqEngine {
    pub fn new(config: MqConfig) -> Self {
        Self {
            config,
            meta: Arc::new(MqMetadata::new()),
        }
    }

    pub fn with_metadata(config: MqConfig, meta: Arc<MqMetadata>) -> Self {
        Self { config, meta }
    }

    /// Get a shared reference to the metadata store.
    pub fn shared_metadata(&self) -> Arc<MqMetadata> {
        Arc::clone(&self.meta)
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
        match cmd.tag() {
            // =================================================================
            // Topics
            // =================================================================
            MqCommand::TAG_CREATE_TOPIC => {
                let v = cmd.as_create_topic();
                let name = v.name().to_owned();
                let retention = v.retention();
                let hash = name_hash(&name);
                if let Some(r) = self.meta.topic_names.get(&hash) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::Topic,
                        id: *r,
                    });
                }
                let id = self.meta.alloc_id();
                let meta = crate::topic::TopicMeta::new(id, name, log_index, retention);
                self.meta.topics.insert(id, TopicState::new(meta));
                self.meta.topic_names.insert(hash, id);
                info!(topic_id = id, "topic created");
                MqResponse::EntityCreated { id }
            }

            MqCommand::TAG_DELETE_TOPIC => {
                let topic_id = cmd.field_u64(1);
                if let Some((_, state)) = self.meta.topics.remove(&topic_id) {
                    self.meta.topic_names.remove(&state.meta().name_hash);
                    if state.meta().message_count > 0 {
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
                let messages: smallvec::SmallVec<[Bytes; 16]> = v.messages().collect();
                if let Some(topic) = self.meta.topics.get(&topic_id) {
                    let offsets = topic.apply_publish(log_index, &messages);
                    self.on_message_added(log_index);
                    MqResponse::Published { offsets }
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Topic,
                        id: topic_id,
                    })
                }
            }

            MqCommand::TAG_COMMIT_OFFSET => {
                let topic_id = cmd.field_u64(1);
                let consumer_id = cmd.field_u64(9);
                let offset = cmd.field_u64(17);
                if let Some(topic) = self.meta.topics.get(&topic_id) {
                    topic.apply_commit_offset(consumer_id, offset);
                    self.mark_purge_floor_dirty();
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Topic,
                        id: topic_id,
                    })
                }
            }

            MqCommand::TAG_PURGE_TOPIC => {
                let topic_id = cmd.field_u64(1);
                let before_index = cmd.field_u64(9);
                if let Some(topic) = self.meta.topics.get(&topic_id) {
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

            // =================================================================
            // Queues
            // =================================================================
            MqCommand::TAG_CREATE_QUEUE => {
                let v = cmd.as_create_queue();
                let name = v.name().to_owned();
                let config = v.config();
                let hash = name_hash(&name);
                if let Some(r) = self.meta.queue_names.get(&hash) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::Queue,
                        id: *r,
                    });
                }
                let id = self.meta.alloc_id();
                let meta = crate::queue::QueueMeta::new(id, name, log_index, config);
                self.meta.queues.insert(id, QueueState::new(meta));
                self.meta.queue_names.insert(hash, id);
                info!(queue_id = id, "queue created");
                MqResponse::EntityCreated { id }
            }

            MqCommand::TAG_DELETE_QUEUE => {
                let queue_id = cmd.field_u64(1);
                if let Some((_, state)) = self.meta.queues.remove(&queue_id) {
                    self.meta.queue_names.remove(&state.meta.name_hash);
                    if state.has_messages() {
                        self.mark_purge_floor_dirty();
                    }
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::TAG_ENQUEUE => {
                let v = cmd.as_enqueue();
                let queue_id = v.queue_id();
                let messages: smallvec::SmallVec<[Bytes; 16]> = v.messages().collect();
                let dedup_keys: smallvec::SmallVec<[Option<Bytes>; 16]> = v.dedup_keys().collect();
                if let Some(queue) = self.meta.queues.get(&queue_id) {
                    let offsets =
                        queue.apply_enqueue(log_index, &messages, &dedup_keys, current_time);
                    self.on_message_added(log_index);
                    MqResponse::Published { offsets }
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::TAG_DELIVER => {
                let queue_id = cmd.field_u64(1);
                let consumer_id = cmd.field_u64(9);
                let max_count = cmd.field_u32(17);
                if let Some(queue) = self.meta.queues.get(&queue_id) {
                    let msg_ids =
                        queue.apply_deliver(consumer_id, max_count, current_time, log_index);
                    if !msg_ids.is_empty() {
                        // Track consumer→queue for O(1) disconnect
                        self.meta.track_consumer_queue(consumer_id, queue_id);
                    }
                    // Build DeliveredMessage stubs (payload must be fetched from raft log)
                    let messages: smallvec::SmallVec<[DeliveredMessage; 8]> = msg_ids
                        .iter()
                        .filter_map(|&id| {
                            queue.messages.get(&id).map(|meta| DeliveredMessage {
                                message_id: id,
                                attempt: meta.attempts,
                                original_timestamp: current_time,
                            })
                        })
                        .collect();
                    MqResponse::Messages { messages }
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::TAG_ACK => {
                let v = cmd.as_ack();
                let queue_id = v.queue_id();
                let message_ids = v.message_ids();
                let response = v.response();
                if let Some(min_id) = message_ids.iter().copied().min() {
                    self.on_message_removed(min_id);
                }
                // Collect reply_to info before acking
                let reply_info: Option<(u64, Bytes)> = response.and_then(|resp| {
                    let queue = self.meta.queues.get(&queue_id)?;
                    for &msg_id in &message_ids {
                        if let Some(meta) = queue.messages.get(&msg_id) {
                            if let Some(ref reply_to_bytes) = meta.reply_to {
                                let hash = name_hash_bytes(reply_to_bytes);
                                if let Some(r) = self.meta.topic_names.get(&hash) {
                                    return Some((*r, resp));
                                }
                            }
                        }
                    }
                    None
                });
                if let Some(queue) = self.meta.queues.get(&queue_id) {
                    queue.apply_ack(&message_ids);
                } else {
                    return MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    });
                }
                // Route response to reply topic if applicable
                if let Some((topic_id, resp)) = reply_info {
                    if let Some(topic) = self.meta.topics.get(&topic_id) {
                        topic.apply_publish(log_index, &[resp]);
                        self.on_message_added(log_index);
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_NACK => {
                let v = cmd.as_nack();
                let queue_id = v.queue_id();
                let message_ids = v.message_ids();
                if let Some(queue) = self.meta.queues.get(&queue_id) {
                    queue.apply_nack(&message_ids);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::TAG_EXTEND_VISIBILITY => {
                let v = cmd.as_extend_visibility();
                let queue_id = v.queue_id();
                let message_ids = v.message_ids();
                let extension_ms = v.extension_ms();
                if let Some(queue) = self.meta.queues.get(&queue_id) {
                    queue.apply_extend_visibility(&message_ids, extension_ms);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::TAG_TIMEOUT_EXPIRED => {
                let v = cmd.as_timeout_expired();
                let queue_id = v.queue_id();
                let message_ids = v.message_ids();
                if let Some(queue) = self.meta.queues.get(&queue_id) {
                    let dlq_id = queue.meta.config.dead_letter_topic_id;
                    let _dead_lettered =
                        queue.apply_timeout_expired(&message_ids, dlq_id, current_time);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::TAG_PUBLISH_TO_DLQ => {
                let v = cmd.as_publish_to_dlq();
                let source_queue_id = v.source_queue_id();
                let dlq_topic_id = v.dlq_topic_id();
                let dead_letter_ids = v.dead_letter_ids();
                let messages: Vec<Bytes> = v.messages().collect();
                // Publish dead-lettered messages to the DLQ topic
                if let Some(topic) = self.meta.topics.get(&dlq_topic_id) {
                    topic.apply_publish(log_index, &messages);
                    self.on_message_added(log_index);
                }
                // Remove dead-lettered messages from the source queue
                if let Some(queue) = self.meta.queues.get(&source_queue_id) {
                    queue.apply_remove_dead_lettered(&dead_letter_ids);
                    self.mark_purge_floor_dirty();
                }
                MqResponse::Ok
            }

            MqCommand::TAG_PRUNE_DEDUP_WINDOW => {
                let queue_id = cmd.field_u64(1);
                let before_timestamp = cmd.field_u64(9);
                if let Some(queue) = self.meta.queues.get(&queue_id) {
                    queue.apply_prune_dedup(before_timestamp);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::TAG_EXPIRE_PENDING_MESSAGES => {
                let v = cmd.as_expire_pending_messages();
                let queue_id = v.queue_id();
                let message_ids = v.message_ids();
                if let Some(queue) = self.meta.queues.get(&queue_id) {
                    queue.apply_expire_pending(&message_ids);
                    self.mark_purge_floor_dirty();
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::TAG_PURGE_QUEUE => {
                let queue_id = cmd.field_u64(1);
                if let Some(queue) = self.meta.queues.get(&queue_id) {
                    if queue.purge() {
                        self.mark_purge_floor_dirty();
                    }
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::TAG_GET_QUEUE_ATTRIBUTES => {
                let queue_id = cmd.field_u64(1);
                if let Some(queue) = self.meta.queues.get(&queue_id) {
                    MqResponse::Stats(EntityStats::Queue {
                        queue_id,
                        pending_count: queue.pending_count(),
                        in_flight_count: queue.in_flight_count(),
                        dlq_count: queue.dlq_count(),
                    })
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            // =================================================================
            // Exchanges
            // =================================================================
            MqCommand::TAG_CREATE_EXCHANGE => {
                let v = cmd.as_create_exchange();
                let name = v.name().to_owned();
                let exchange_type = v.exchange_type();
                let hash = name_hash(&name);
                if let Some(r) = self.meta.exchange_names.get(&hash) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::Exchange,
                        id: *r,
                    });
                }
                let id = self.meta.alloc_id();
                let meta = crate::exchange::ExchangeMeta::new(id, name, log_index, exchange_type);
                self.meta.exchanges.insert(id, ExchangeState::new(meta));
                self.meta.exchange_names.insert(hash, id);
                info!(exchange_id = id, "exchange created");
                MqResponse::EntityCreated { id }
            }

            MqCommand::TAG_DELETE_EXCHANGE => {
                let exchange_id = cmd.field_u64(1);
                if let Some((_, state)) = self.meta.exchanges.remove(&exchange_id) {
                    self.meta.exchange_names.remove(&state.meta.name_hash);
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
                let queue_id = v.queue_id();
                let routing_key = v.routing_key();
                // Verify both exchange and queue exist
                if !self.meta.exchanges.contains_key(&exchange_id) {
                    return MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Exchange,
                        id: exchange_id,
                    });
                }
                if !self.meta.queues.contains_key(&queue_id) {
                    return MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    });
                }
                let binding_id = self.meta.alloc_id();
                let binding = Binding {
                    binding_id,
                    exchange_id,
                    queue_id,
                    routing_key,
                };
                if let Some(mut exchange) = self.meta.exchanges.get_mut(&exchange_id) {
                    exchange.add_binding(binding);
                }
                MqResponse::EntityCreated { id: binding_id }
            }

            MqCommand::TAG_DELETE_BINDING => {
                let binding_id = cmd.field_u64(1);
                // Find which exchange owns this binding
                let mut found = false;
                for mut exchange in self.meta.exchanges.iter_mut() {
                    if exchange.bindings.contains_key(&binding_id) {
                        exchange.remove_binding(binding_id);
                        found = true;
                        break;
                    }
                }
                if found {
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
                let messages: smallvec::SmallVec<[Bytes; 16]> = v.messages().collect();
                // Route messages to bound queues
                let target_queue_ids = if let Some(exchange) = self.meta.exchanges.get(&exchange_id)
                {
                    // Extract routing key from first flat message (zero-copy)
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

                // Enqueue to each target queue (empty dedup_keys slice — no allocation)
                let mut total_enqueued = 0u64;
                for qid in target_queue_ids {
                    if let Some(queue) = self.meta.queues.get(&qid) {
                        queue.apply_enqueue(log_index, &messages, &[], current_time);
                        total_enqueued += messages.len() as u64;
                    }
                }
                if total_enqueued > 0 {
                    self.on_message_added(log_index);
                }
                MqResponse::Ok
            }

            // =================================================================
            // Actors
            // =================================================================
            MqCommand::TAG_CREATE_ACTOR_NAMESPACE => {
                let v = cmd.as_create_actor_namespace();
                let name = v.name().to_owned();
                let config = v.config();
                let hash = name_hash(&name);
                if let Some(r) = self.meta.namespace_names.get(&hash) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::ActorNamespace,
                        id: *r,
                    });
                }
                let id = self.meta.alloc_id();
                let meta = crate::actor::ActorNamespaceMeta::new(id, name, log_index, config);
                self.meta
                    .actor_namespaces
                    .insert(id, ActorNamespaceState::new(meta));
                self.meta.namespace_names.insert(hash, id);
                info!(namespace_id = id, "actor namespace created");
                MqResponse::EntityCreated { id }
            }

            MqCommand::TAG_DELETE_ACTOR_NAMESPACE => {
                let namespace_id = cmd.field_u64(1);
                if let Some((_, state)) = self.meta.actor_namespaces.remove(&namespace_id) {
                    self.meta.namespace_names.remove(&state.meta.name_hash);
                    if !state.actors.is_empty() {
                        self.mark_purge_floor_dirty();
                    }
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ActorNamespace,
                        id: namespace_id,
                    })
                }
            }

            MqCommand::TAG_SEND_TO_ACTOR => {
                let v = cmd.as_send_to_actor();
                let namespace_id = v.namespace_id();
                let actor_id = v.actor_id();
                let message = v.message();
                let reply_to = {
                    let meta = crate::flat::FlatMessageMeta::parse(&message);
                    if meta.map_or(false, |m| m.has_reply_to()) {
                        crate::flat::FlatMessage::new(message.clone()).and_then(|f| f.reply_to())
                    } else {
                        None
                    }
                };
                if let Some(ns) = self.meta.actor_namespaces.get(&namespace_id) {
                    match ns.apply_send(&actor_id, log_index, current_time, reply_to) {
                        Ok(()) => {
                            self.on_message_added(log_index);
                            MqResponse::Ok
                        }
                        Err(e) => MqResponse::Error(e),
                    }
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ActorNamespace,
                        id: namespace_id,
                    })
                }
            }

            MqCommand::TAG_DELIVER_ACTOR_MESSAGE => {
                let v = cmd.as_deliver_actor_message();
                let namespace_id = v.namespace_id();
                let actor_id = v.actor_id();
                let consumer_id = v.consumer_id();
                if let Some(ns) = self.meta.actor_namespaces.get(&namespace_id) {
                    match ns.apply_deliver(&actor_id, consumer_id) {
                        Some(msg_index) => MqResponse::Messages {
                            messages: smallvec::smallvec![DeliveredMessage {
                                message_id: msg_index,
                                attempt: 1,
                                original_timestamp: current_time,
                            }],
                        },
                        None => MqResponse::Messages {
                            messages: smallvec::SmallVec::new(),
                        },
                    }
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ActorNamespace,
                        id: namespace_id,
                    })
                }
            }

            MqCommand::TAG_ACK_ACTOR_MESSAGE => {
                let v = cmd.as_ack_actor_message();
                let namespace_id = v.namespace_id();
                let actor_id = v.actor_id();
                let message_id = v.message_id();
                let response = v.response();
                self.on_message_removed(message_id);
                let reply_to_name = if let Some(ns) = self.meta.actor_namespaces.get(&namespace_id)
                {
                    ns.apply_ack(&actor_id, message_id)
                } else {
                    return MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ActorNamespace,
                        id: namespace_id,
                    });
                };
                // Route response to reply topic if applicable
                if let (Some(resp), Some(reply_name)) = (response, reply_to_name) {
                    let hash = name_hash_bytes(&reply_name);
                    if let Some(r) = self.meta.topic_names.get(&hash) {
                        let topic_id = *r;
                        drop(r);
                        if let Some(topic) = self.meta.topics.get(&topic_id) {
                            topic.apply_publish(log_index, &[resp]);
                            self.on_message_added(log_index);
                        }
                    }
                }
                MqResponse::Ok
            }

            MqCommand::TAG_NACK_ACTOR_MESSAGE => {
                let v = cmd.as_nack_actor_message();
                let namespace_id = v.namespace_id();
                let actor_id = v.actor_id();
                let message_id = v.message_id();
                if let Some(ns) = self.meta.actor_namespaces.get(&namespace_id) {
                    ns.apply_nack(&actor_id, message_id);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ActorNamespace,
                        id: namespace_id,
                    })
                }
            }

            MqCommand::TAG_ASSIGN_ACTORS => {
                let v = cmd.as_assign_actors();
                let namespace_id = v.namespace_id();
                let consumer_id = v.consumer_id();
                let actor_ids = v.actor_ids();
                if let Some(ns) = self.meta.actor_namespaces.get(&namespace_id) {
                    ns.apply_assign(consumer_id, &actor_ids);
                    // Track consumer→namespace for O(1) disconnect
                    self.meta.track_consumer_actor_ns(consumer_id, namespace_id);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ActorNamespace,
                        id: namespace_id,
                    })
                }
            }

            MqCommand::TAG_RELEASE_ACTORS => {
                let namespace_id = cmd.field_u64(1);
                let consumer_id = cmd.field_u64(9);
                if let Some(ns) = self.meta.actor_namespaces.get(&namespace_id) {
                    ns.apply_release(consumer_id);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ActorNamespace,
                        id: namespace_id,
                    })
                }
            }

            MqCommand::TAG_EVICT_IDLE_ACTORS => {
                let namespace_id = cmd.field_u64(1);
                let before_timestamp = cmd.field_u64(9);
                if let Some(ns) = self.meta.actor_namespaces.get(&namespace_id) {
                    let count = ns.apply_evict_idle(before_timestamp);
                    debug!(namespace_id, count, "evicted idle actors");
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ActorNamespace,
                        id: namespace_id,
                    })
                }
            }

            // =================================================================
            // Jobs
            // =================================================================
            MqCommand::TAG_CREATE_JOB => {
                let v = cmd.as_create_job();
                let name = v.name().to_owned();
                let config = v.config();
                let hash = name_hash(&name);
                if let Some(r) = self.meta.job_names.get(&hash) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::Job,
                        id: *r,
                    });
                }
                let id = self.meta.alloc_id();
                let meta = crate::job::JobMeta::new(id, name, log_index, config);
                self.meta.jobs.insert(id, JobInstance::new(meta));
                self.meta.job_names.insert(hash, id);
                info!(job_id = id, "job created");
                MqResponse::EntityCreated { id }
            }

            MqCommand::TAG_DELETE_JOB => {
                let job_id = cmd.field_u64(1);
                if let Some((_, job)) = self.meta.jobs.remove(&job_id) {
                    self.meta.job_names.remove(&job.meta().name_hash);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::TAG_UPDATE_JOB => {
                let v = cmd.as_update_job();
                let job_id = v.job_id();
                let config = v.config();
                if let Some(job) = self.meta.jobs.get(&job_id) {
                    job.apply_set_config(config);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::TAG_ENABLE_JOB => {
                let job_id = cmd.field_u64(1);
                if let Some(job) = self.meta.jobs.get(&job_id) {
                    job.apply_set_enabled(true);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::TAG_DISABLE_JOB => {
                let job_id = cmd.field_u64(1);
                if let Some(job) = self.meta.jobs.get(&job_id) {
                    job.apply_set_enabled(false);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::TAG_TRIGGER_JOB => {
                let job_id = cmd.field_u64(1);
                let execution_id = cmd.field_u64(9);
                let triggered_at = cmd.field_u64(17);
                if let Some(job) = self.meta.jobs.get(&job_id) {
                    job.apply_trigger(execution_id, triggered_at);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::TAG_ASSIGN_JOB => {
                let job_id = cmd.field_u64(1);
                let consumer_id = cmd.field_u64(9);
                if let Some(job) = self.meta.jobs.get(&job_id) {
                    job.apply_assign(consumer_id);
                    // Track consumer→job for O(1) disconnect
                    self.meta.track_consumer_job(consumer_id, job_id);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::TAG_COMPLETE_JOB => {
                let job_id = cmd.field_u64(1);
                let execution_id = cmd.field_u64(9);
                if let Some(job) = self.meta.jobs.get(&job_id) {
                    job.apply_complete(execution_id, current_time);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::TAG_FAIL_JOB => {
                let v = cmd.as_fail_job();
                let job_id = v.job_id();
                let execution_id = v.execution_id();
                let error = v.error();
                if let Some(job) = self.meta.jobs.get(&job_id) {
                    warn!(job_id, execution_id, error, "job execution failed");
                    job.apply_fail(execution_id);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::TAG_TIMEOUT_JOB => {
                let job_id = cmd.field_u64(1);
                let execution_id = cmd.field_u64(9);
                if let Some(job) = self.meta.jobs.get(&job_id) {
                    job.apply_timeout(execution_id);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            // =================================================================
            // Sessions
            // =================================================================
            MqCommand::TAG_REGISTER_CONSUMER => {
                let v = cmd.as_register_consumer();
                let consumer_id = v.consumer_id();
                let group_name = v.group_name().to_owned();
                let subscriptions = v.subscriptions().into_iter().collect();
                let meta = crate::consumer::ConsumerMeta::new(
                    consumer_id,
                    group_name,
                    log_index,
                    subscriptions,
                );
                self.meta
                    .consumers
                    .insert(consumer_id, ConsumerState::new(meta));
                debug!(consumer_id, "consumer registered");
                MqResponse::Ok
            }

            MqCommand::TAG_DISCONNECT_CONSUMER => {
                let consumer_id = cmd.field_u64(1);
                if self.meta.consumers.remove(&consumer_id).is_some() {
                    // O(1) disconnect using reverse indexes
                    let (queue_ids, actor_ns_ids, job_ids) =
                        self.meta.remove_consumer_indexes(consumer_id);

                    // Release actor assignments (O(1) — only tracked namespaces)
                    if let Some(ns_ids) = actor_ns_ids {
                        for ns_id in ns_ids {
                            if let Some(ns) = self.meta.actor_namespaces.get(&ns_id) {
                                ns.apply_release(consumer_id);
                            }
                        }
                    }

                    // Release job assignments (O(1) — only tracked jobs)
                    if let Some(j_ids) = job_ids {
                        for job_id in j_ids {
                            if let Some(job) = self.meta.jobs.get(&job_id) {
                                if job.meta().state.assigned_consumer_id == Some(consumer_id) {
                                    job.apply_unassign();
                                }
                            }
                        }
                    }

                    // Return in-flight queue messages (O(1) — only tracked queues)
                    let mut had_in_flight = false;
                    if let Some(q_ids) = queue_ids {
                        for queue_id in q_ids {
                            if let Some(queue) = self.meta.queues.get(&queue_id) {
                                let in_flight_ids = queue.consumer_in_flight_ids(consumer_id);
                                if !in_flight_ids.is_empty() {
                                    had_in_flight = true;
                                    queue.apply_nack(&in_flight_ids);
                                }
                            }
                        }
                    }
                    if had_in_flight {
                        self.mark_purge_floor_dirty();
                    }
                    debug!(consumer_id, "consumer disconnected");
                    MqResponse::Ok
                } else {
                    MqResponse::Ok // idempotent
                }
            }

            MqCommand::TAG_HEARTBEAT => {
                let consumer_id = cmd.field_u64(1);
                if let Some(mut consumer) = self.meta.consumers.get_mut(&consumer_id) {
                    consumer.heartbeat(current_time);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Consumer,
                        id: consumer_id,
                    })
                }
            }

            MqCommand::TAG_REGISTER_PRODUCER => {
                let v = cmd.as_register_producer();
                let producer_id = v.producer_id();
                let name = v.name();
                let meta = ProducerMeta::new(producer_id, name, log_index);
                self.meta.producers.insert(producer_id, meta);
                MqResponse::Ok
            }

            MqCommand::TAG_DISCONNECT_PRODUCER => {
                let producer_id = cmd.field_u64(1);
                self.meta.producers.remove(&producer_id);
                MqResponse::Ok
            }

            MqCommand::TAG_BATCH => {
                let batch = cmd.as_batch();
                let responses: Vec<MqResponse> = batch
                    .commands()
                    .map(|sub| self.apply_command(&sub, log_index, current_time))
                    .collect();
                MqResponse::BatchResponse(responses)
            }

            _ => {
                warn!(tag = cmd.tag(), "unknown MqCommand tag");
                MqResponse::Error(MqError::Custom(format!(
                    "unknown command tag: {}",
                    cmd.tag()
                )))
            }
        }
    }

    // =========================================================================
    // Snapshot / Restore
    // =========================================================================

    /// Build a snapshot of all engine state.
    pub fn snapshot(&self) -> MqSnapshotData {
        let topics = self
            .meta
            .topics
            .iter()
            .map(|entry| {
                let t = entry.value();
                TopicSnapshot {
                    meta: t.meta().clone(),
                    consumer_offsets: t.consumer_offsets_snapshot(),
                }
            })
            .collect();

        let queues = self
            .meta
            .queues
            .iter()
            .map(|entry| {
                let q = entry.value();
                let mut meta = q.meta.clone();
                // Sync atomic counters to meta for snapshot
                meta.pending_count = q.pending_count();
                meta.in_flight_count = q.in_flight_count();
                meta.dlq_count = q.dlq_count();
                QueueSnapshot {
                    meta,
                    messages: q.messages.iter().map(|e| e.value().clone()).collect(),
                    dedup_entries: q.dedup_snapshot_entries(),
                }
            })
            .collect();

        let actor_namespaces = self
            .meta
            .actor_namespaces
            .iter()
            .map(|entry| {
                let ns = entry.value();
                let mut meta = ns.meta.clone();
                meta.active_actor_count = ns.active_count();
                ActorNamespaceSnapshot {
                    meta,
                    actors: ns.actors.iter().map(|e| e.value().state.clone()).collect(),
                }
            })
            .collect();

        let jobs = self
            .meta
            .jobs
            .iter()
            .map(|entry| JobSnapshot {
                meta: entry.value().meta().clone(),
            })
            .collect();

        let consumers = self
            .meta
            .consumers
            .iter()
            .map(|entry| ConsumerSnapshot {
                meta: entry.value().meta.clone(),
            })
            .collect();

        let producers = self
            .meta
            .producers
            .iter()
            .map(|entry| ProducerSnapshot {
                meta: entry.value().clone(),
            })
            .collect();

        let exchanges = self
            .meta
            .exchanges
            .iter()
            .map(|entry| {
                let e = entry.value();
                ExchangeSnapshot {
                    meta: e.meta.clone(),
                    bindings: e.bindings.values().cloned().collect(),
                }
            })
            .collect();

        MqSnapshotData {
            topics,
            queues,
            actor_namespaces,
            jobs,
            consumers,
            producers,
            exchanges,
            next_id: self.meta.next_id.load(Ordering::Relaxed),
            file_manifest: Vec::new(),
            sync_addr: None,
        }
    }

    /// Sync protocol-adapter caches (TopicMeta/QueueMeta with atomics).
    ///
    /// Called after each apply batch. Updates the lightweight metadata that
    /// protocol adapters cache for zero-lock reads.
    pub fn sync_metadata(&self) {
        // Sync topics: insert or update.
        for entry in self.meta.topics.iter() {
            let t = entry.value();
            let tm = t.meta();
            let (latest_log_index, latest_msg_pos) = if tm.message_count > 0 && tm.head_index > 0 {
                t.get_log_entry(tm.head_index - 1)
                    .map(|(li, pos)| (li, pos))
                    .unwrap_or((0, 0))
            } else {
                (0, 0)
            };

            if let Some(existing) = self.meta.get_topic(tm.topic_id) {
                existing.update(
                    tm.head_index,
                    tm.tail_index,
                    tm.message_count,
                    latest_log_index,
                    latest_msg_pos,
                );
            } else {
                self.meta
                    .insert_topic(Arc::new(crate::metadata::TopicMeta::with_state(
                        tm.topic_id,
                        tm.name.clone(),
                        tm.head_index,
                        tm.tail_index,
                        tm.message_count,
                        latest_log_index,
                        latest_msg_pos,
                    )));
            }
        }

        // Remove topics that no longer exist.
        let topic_ids: Vec<u64> = self.meta.topic_ids();
        for id in topic_ids {
            if !self.meta.topics.contains_key(&id) {
                self.meta.remove_topic(id);
            }
        }

        // Sync queues: insert new ones.
        for entry in self.meta.queues.iter() {
            let q = entry.value();
            if self.meta.get_queue(q.meta.queue_id).is_none() {
                self.meta.insert_queue(Arc::new(crate::metadata::QueueMeta {
                    queue_id: q.meta.queue_id,
                    name: q.meta.name.clone(),
                }));
            }
        }

        // Remove queues that no longer exist.
        let queue_ids: Vec<u64> = self.meta.queue_ids();
        for id in queue_ids {
            if !self.meta.queues.contains_key(&id) {
                self.meta.remove_queue(id);
            }
        }
    }

    /// Restore engine state from a snapshot.
    pub fn restore(&mut self, data: MqSnapshotData) {
        self.meta.next_id.store(data.next_id, Ordering::Relaxed);
        self.meta.purge_floor_dirty.store(true, Ordering::Relaxed);
        // Note: segment index clearing is handled by the SegmentIndexMap owner.

        // Clear all DashMaps
        self.meta.topics.clear();
        self.meta.queues.clear();
        self.meta.actor_namespaces.clear();
        self.meta.jobs.clear();
        self.meta.exchanges.clear();
        self.meta.consumers.clear();
        self.meta.producers.clear();
        self.meta.topic_names.clear();
        self.meta.queue_names.clear();
        self.meta.namespace_names.clear();
        self.meta.job_names.clear();
        self.meta.exchange_names.clear();
        self.meta.clear_consumer_indexes();

        // Restore topics
        for ts in data.topics {
            let id = ts.meta.topic_id;
            let mut meta = ts.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let state = TopicState::new(meta);
            for offset in ts.consumer_offsets {
                state.consumer_offsets.insert(offset.consumer_id, offset);
            }
            self.meta.topic_names.insert(hash, id);
            self.meta.topics.insert(id, state);
        }

        // Restore queues (rebuild in-flight indexes via restore_message)
        for qs in data.queues {
            let id = qs.meta.queue_id;
            let mut meta = qs.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let state = QueueState::new(meta);
            for msg in qs.messages {
                state.restore_message(msg);
            }
            state.dedup_restore_entries(qs.dedup_entries);
            self.meta.queue_names.insert(hash, id);
            self.meta.queues.insert(id, state);
        }

        // Restore actor namespaces
        for ans in data.actor_namespaces {
            let id = ans.meta.namespace_id;
            let mut meta = ans.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let state = ActorNamespaceState::new(meta);
            for actor_state in ans.actors {
                let actor_id = actor_state.actor_id.clone();
                state.actors.insert(
                    actor_id,
                    crate::actor::ActorInMemory {
                        state: actor_state,
                        mailbox: std::collections::VecDeque::new(), // mailbox rebuilt from raft log
                        reply_to_map: dashmap::DashMap::new(),
                    },
                );
            }
            self.meta.namespace_names.insert(hash, id);
            self.meta.actor_namespaces.insert(id, state);
        }

        // Restore jobs
        for js in data.jobs {
            let id = js.meta.job_id;
            let mut meta = js.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            self.meta.job_names.insert(hash, id);
            self.meta.jobs.insert(id, JobInstance::new(meta));
        }

        // Restore consumers
        for cs in data.consumers {
            let id = cs.meta.consumer_id;
            self.meta.consumers.insert(id, ConsumerState::new(cs.meta));
        }

        // Restore producers
        for ps in data.producers {
            let id = ps.meta.producer_id;
            self.meta.producers.insert(id, ps.meta);
        }

        // Restore exchanges
        for es in data.exchanges {
            let id = es.meta.exchange_id;
            let mut meta = es.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let mut state = ExchangeState::new(meta);
            for binding in es.bindings {
                state.add_binding(binding);
            }
            self.meta.exchange_names.insert(hash, id);
            self.meta.exchanges.insert(id, state);
        }

        info!(
            topics = self.meta.topics.len(),
            queues = self.meta.queues.len(),
            actor_namespaces = self.meta.actor_namespaces.len(),
            jobs = self.meta.jobs.len(),
            exchanges = self.meta.exchanges.len(),
            consumers = self.meta.consumers.len(),
            "MQ engine restored from snapshot"
        );
    }

    /// Restore only structural state (entity metadata) from MDBX.
    ///
    /// This creates empty entity shells (no messages, no in-flight state).
    /// The raft log is then replayed from the structural purge floor to
    /// rebuild message state.
    pub(crate) fn restore_structural(&mut self, state: crate::manifest::StructuralState) {
        self.meta.next_id.store(state.next_id, Ordering::Relaxed);
        self.meta.purge_floor_dirty.store(true, Ordering::Relaxed);
        // Note: segment index clearing is handled by the SegmentIndexMap owner.

        // Clear all DashMaps
        self.meta.topics.clear();
        self.meta.queues.clear();
        self.meta.actor_namespaces.clear();
        self.meta.jobs.clear();
        self.meta.consumers.clear();
        self.meta.producers.clear();
        self.meta.topic_names.clear();
        self.meta.queue_names.clear();
        self.meta.namespace_names.clear();
        self.meta.job_names.clear();

        for mut meta in state.topics {
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let id = meta.topic_id;
            self.meta.topic_names.insert(hash, id);
            self.meta.topics.insert(id, TopicState::new(meta));
        }

        for mut meta in state.queues {
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let id = meta.queue_id;
            self.meta.queue_names.insert(hash, id);
            self.meta.queues.insert(id, QueueState::new(meta));
        }

        for mut meta in state.actor_namespaces {
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let id = meta.namespace_id;
            self.meta.namespace_names.insert(hash, id);
            self.meta
                .actor_namespaces
                .insert(id, ActorNamespaceState::new(meta));
        }

        for mut meta in state.jobs {
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let id = meta.job_id;
            self.meta.job_names.insert(hash, id);
            self.meta.jobs.insert(id, JobInstance::new(meta));
        }

        info!(
            topics = self.meta.topics.len(),
            queues = self.meta.queues.len(),
            actor_namespaces = self.meta.actor_namespaces.len(),
            jobs = self.meta.jobs.len(),
            next_id = state.next_id,
            "MQ engine restored structural state from MDBX"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ActorConfig, JobConfig, QueueConfig};
    use crate::flat::FlatMessageBuilder;
    use crate::segment_index::SegmentIndex as _;
    use crate::types::name_hash;

    fn make_engine() -> MqEngine {
        MqEngine::new(MqConfig::new("/tmp/mq-test"))
    }

    fn make_flat_msg(value: &[u8]) -> Bytes {
        FlatMessageBuilder::new(Bytes::from(value.to_vec()))
            .timestamp(1000)
            .build()
    }

    // =========================================================================
    // Topic commands
    // =========================================================================

    #[test]
    fn test_create_topic() {
        let mut engine = make_engine();
        let resp = engine.apply_command(
            &MqCommand::create_topic("events", RetentionPolicy::default(), 0),
            1,
            1000,
        );
        match resp {
            MqResponse::EntityCreated { id } => {
                assert_eq!(id, 1);
                assert!(engine.meta.topics.contains_key(&1));
                assert_eq!(
                    *engine.meta.topic_names.get(&name_hash("events")).unwrap(),
                    1
                );
            }
            _ => panic!("expected EntityCreated"),
        }
    }

    #[test]
    fn test_create_duplicate_topic() {
        let mut engine = make_engine();
        engine.apply_command(
            &MqCommand::create_topic("events", RetentionPolicy::default(), 0),
            1,
            1000,
        );
        let resp = engine.apply_command(
            &MqCommand::create_topic("events", RetentionPolicy::default(), 0),
            2,
            1001,
        );
        assert!(matches!(resp, MqResponse::Error(_)));
    }

    #[test]
    fn test_delete_topic() {
        let mut engine = make_engine();
        engine.apply_command(
            &MqCommand::create_topic("events", RetentionPolicy::default(), 0),
            1,
            1000,
        );
        let resp = engine.apply_command(&MqCommand::delete_topic(1), 2, 1001);
        assert!(matches!(resp, MqResponse::Ok));
        assert!(!engine.meta.topics.contains_key(&1));
        assert!(!engine.meta.topic_names.contains_key(&name_hash("events")));
    }

    #[test]
    fn test_publish_and_commit_offset() {
        let mut engine = make_engine();
        engine.apply_command(
            &MqCommand::create_topic("events", RetentionPolicy::default(), 0),
            1,
            1000,
        );
        let flat_msg = make_flat_msg(b"hello");
        let resp = engine.apply_command(&MqCommand::publish(1, &[flat_msg]), 2, 1001);
        assert!(matches!(resp, MqResponse::Published { offsets } if offsets.len() == 1));

        let resp = engine.apply_command(&MqCommand::commit_offset(1, 100, 2), 3, 1002);
        assert!(matches!(resp, MqResponse::Ok));
    }

    // =========================================================================
    // Queue commands
    // =========================================================================

    #[test]
    fn test_queue_lifecycle() {
        let mut engine = make_engine();
        let resp = engine.apply_command(
            &MqCommand::create_queue("tasks", &QueueConfig::default()),
            1,
            1000,
        );
        let queue_id = match resp {
            MqResponse::EntityCreated { id } => id,
            _ => panic!("expected EntityCreated"),
        };

        // Enqueue
        let flat_msg = make_flat_msg(b"task1");
        let resp =
            engine.apply_command(&MqCommand::enqueue(queue_id, &[flat_msg], &[None]), 2, 1001);
        assert!(matches!(resp, MqResponse::Published { .. }));

        // Deliver
        let resp = engine.apply_command(&MqCommand::deliver(queue_id, 100, 10), 3, 1002);
        let msg_ids: Vec<u64> = match &resp {
            MqResponse::Messages { messages } => {
                assert_eq!(messages.len(), 1);
                messages.iter().map(|m| m.message_id).collect()
            }
            _ => panic!("expected Messages"),
        };

        // Ack
        let resp = engine.apply_command(&MqCommand::ack(queue_id, &msg_ids, None), 4, 1003);
        assert!(matches!(resp, MqResponse::Ok));
        assert_eq!(
            engine
                .meta
                .queues
                .get(&queue_id)
                .unwrap()
                .meta
                .in_flight_count,
            0
        );
    }

    #[test]
    fn test_queue_nack_and_redeliver() {
        let mut engine = make_engine();
        engine.apply_command(
            &MqCommand::create_queue("q", &QueueConfig::default()),
            1,
            1000,
        );
        let flat_msg = make_flat_msg(b"msg");
        engine.apply_command(&MqCommand::enqueue(1, &[flat_msg], &[None]), 2, 1001);
        engine.apply_command(&MqCommand::deliver(1, 100, 1), 3, 1002);
        engine.apply_command(&MqCommand::nack(1, &[2]), 4, 1003);

        assert_eq!(engine.meta.queues.get(&1).unwrap().pending_count(), 1);
        assert_eq!(engine.meta.queues.get(&1).unwrap().in_flight_count(), 0);

        // Re-deliver
        let resp = engine.apply_command(&MqCommand::deliver(1, 100, 1), 5, 1004);
        match resp {
            MqResponse::Messages { messages } => assert_eq!(messages.len(), 1),
            _ => panic!("expected Messages"),
        }
    }

    // =========================================================================
    // Actor commands
    // =========================================================================

    #[test]
    fn test_actor_namespace_lifecycle() {
        let mut engine = make_engine();
        let resp = engine.apply_command(
            &MqCommand::create_actor_namespace("users", &ActorConfig::default()),
            1,
            1000,
        );
        let ns_id = match resp {
            MqResponse::EntityCreated { id } => id,
            _ => panic!("expected EntityCreated"),
        };

        // Send to actor
        let actor_id = Bytes::from_static(b"user-42");
        let flat_msg = make_flat_msg(b"hello");
        let resp = engine.apply_command(
            &MqCommand::send_to_actor(ns_id, &actor_id, &flat_msg),
            2,
            1001,
        );
        assert!(matches!(resp, MqResponse::Ok));

        // Assign
        engine.apply_command(
            &MqCommand::assign_actors(ns_id, 100, &[actor_id.clone()]),
            3,
            1002,
        );

        // Deliver
        let resp = engine.apply_command(
            &MqCommand::deliver_actor_message(ns_id, &actor_id, 100),
            4,
            1003,
        );
        match resp {
            MqResponse::Messages { messages } => assert_eq!(messages.len(), 1),
            _ => panic!("expected Messages"),
        }

        // Ack
        let resp = engine.apply_command(
            &MqCommand::ack_actor_message(ns_id, &actor_id, 2, None),
            5,
            1004,
        );
        assert!(matches!(resp, MqResponse::Ok));
    }

    // =========================================================================
    // Job commands
    // =========================================================================

    #[test]
    fn test_job_lifecycle() {
        let mut engine = make_engine();
        let config = JobConfig {
            cron_expression: "0 * * * * *".to_string(),
            ..Default::default()
        };
        let resp = engine.apply_command(&MqCommand::create_job("cleanup", &config), 1, 1000);
        let job_id = match resp {
            MqResponse::EntityCreated { id } => id,
            _ => panic!("expected EntityCreated"),
        };

        // Trigger
        engine.apply_command(&MqCommand::trigger_job(job_id, 100, 5000), 2, 5000);
        assert_eq!(
            engine
                .meta
                .jobs
                .get(&job_id)
                .unwrap()
                .meta()
                .state
                .current_execution_id,
            Some(100)
        );

        // Complete
        engine.apply_command(&MqCommand::complete_job(job_id, 100), 3, 6000);
        assert!(
            engine
                .meta
                .jobs
                .get(&job_id)
                .unwrap()
                .meta()
                .state
                .current_execution_id
                .is_none()
        );
    }

    #[test]
    fn test_job_enable_disable() {
        let mut engine = make_engine();
        engine.apply_command(&MqCommand::create_job("j", &JobConfig::default()), 1, 1000);

        engine.apply_command(&MqCommand::disable_job(1), 2, 1001);
        assert!(!engine.meta.jobs.get(&1).unwrap().meta().enabled);

        engine.apply_command(&MqCommand::enable_job(1), 3, 1002);
        assert!(engine.meta.jobs.get(&1).unwrap().meta().enabled);
    }

    // =========================================================================
    // Session commands
    // =========================================================================

    #[test]
    fn test_consumer_lifecycle() {
        let mut engine = make_engine();
        engine.apply_command(&MqCommand::register_consumer(100, "group-1", &[]), 1, 1000);
        assert!(engine.meta.consumers.contains_key(&100));

        engine.apply_command(&MqCommand::heartbeat(100), 2, 2000);
        assert_eq!(
            engine
                .meta
                .consumers
                .get(&100)
                .unwrap()
                .meta
                .last_heartbeat_at,
            2000
        );

        engine.apply_command(&MqCommand::disconnect_consumer(100), 3, 3000);
        assert!(!engine.meta.consumers.contains_key(&100));
    }

    #[test]
    fn test_disconnect_consumer_releases_actors() {
        let mut engine = make_engine();
        engine.apply_command(
            &MqCommand::create_actor_namespace("ns", &ActorConfig::default()),
            1,
            1000,
        );
        engine.apply_command(&MqCommand::register_consumer(100, "g", &[]), 2, 1000);
        let aid = Bytes::from_static(b"actor-1");
        let flat_msg = make_flat_msg(b"msg");
        engine.apply_command(&MqCommand::send_to_actor(1, &aid, &flat_msg), 3, 1001);
        engine.apply_command(&MqCommand::assign_actors(1, 100, &[aid.clone()]), 4, 1002);

        // Disconnect — should release actor assignments
        engine.apply_command(&MqCommand::disconnect_consumer(100), 5, 1003);
        let ns = engine.meta.actor_namespaces.get(&1).unwrap();
        assert!(
            ns.actors
                .get(&aid)
                .unwrap()
                .state
                .assigned_consumer_id
                .is_none()
        );
    }

    #[test]
    fn test_disconnect_consumer_nacks_queue_messages() {
        let mut engine = make_engine();
        engine.apply_command(
            &MqCommand::create_queue("q", &QueueConfig::default()),
            1,
            1000,
        );
        engine.apply_command(&MqCommand::register_consumer(100, "g", &[]), 2, 1000);
        let flat_msg = make_flat_msg(b"msg");
        engine.apply_command(&MqCommand::enqueue(1, &[flat_msg], &[None]), 3, 1001);
        engine.apply_command(&MqCommand::deliver(1, 100, 1), 4, 1002);

        assert_eq!(engine.meta.queues.get(&1).unwrap().in_flight_count(), 1);

        engine.apply_command(&MqCommand::disconnect_consumer(100), 5, 1003);
        assert_eq!(engine.meta.queues.get(&1).unwrap().in_flight_count(), 0);
        assert_eq!(engine.meta.queues.get(&1).unwrap().pending_count(), 1);
    }

    #[test]
    fn test_producer_lifecycle() {
        let mut engine = make_engine();
        engine.apply_command(
            &MqCommand::register_producer(200, Some("producer-1")),
            1,
            1000,
        );
        assert!(engine.meta.producers.contains_key(&200));

        engine.apply_command(&MqCommand::disconnect_producer(200), 2, 1001);
        assert!(!engine.meta.producers.contains_key(&200));
    }

    // =========================================================================
    // Snapshot and Restore
    // =========================================================================

    #[test]
    fn test_snapshot_empty() {
        let engine = make_engine();
        let snap = engine.snapshot();
        assert!(snap.topics.is_empty());
        assert!(snap.queues.is_empty());
        assert!(snap.actor_namespaces.is_empty());
        assert!(snap.jobs.is_empty());
        assert!(snap.consumers.is_empty());
        assert!(snap.producers.is_empty());
    }

    #[test]
    fn test_snapshot_and_restore() {
        let mut engine = make_engine();

        // Create entities
        engine.apply_command(
            &MqCommand::create_topic("t1", RetentionPolicy::default(), 0),
            1,
            1000,
        );
        engine.apply_command(
            &MqCommand::create_queue("q1", &QueueConfig::default()),
            2,
            1001,
        );
        engine.apply_command(
            &MqCommand::create_actor_namespace("ns1", &ActorConfig::default()),
            3,
            1002,
        );
        engine.apply_command(
            &MqCommand::create_job(
                "j1",
                &JobConfig {
                    cron_expression: "0 * * * * *".to_string(),
                    ..Default::default()
                },
            ),
            4,
            1003,
        );

        // Publish some data
        let flat_msg1 = make_flat_msg(b"msg1");
        engine.apply_command(&MqCommand::publish(1, &[flat_msg1]), 5, 1004);
        let flat_task1 = make_flat_msg(b"task1");
        engine.apply_command(&MqCommand::enqueue(2, &[flat_task1], &[None]), 6, 1005);

        let snap = engine.snapshot();
        assert_eq!(snap.topics.len(), 1);
        assert_eq!(snap.queues.len(), 1);
        assert_eq!(snap.actor_namespaces.len(), 1);
        assert_eq!(snap.jobs.len(), 1);

        // Restore into fresh engine
        let mut engine2 = make_engine();
        engine2.restore(snap);

        assert_eq!(engine2.meta.topics.len(), 1);
        assert_eq!(engine2.meta.queues.len(), 1);
        assert_eq!(engine2.meta.actor_namespaces.len(), 1);
        assert_eq!(engine2.meta.jobs.len(), 1);
        assert_eq!(*engine2.meta.topic_names.get(&name_hash("t1")).unwrap(), 1);
        assert_eq!(*engine2.meta.queue_names.get(&name_hash("q1")).unwrap(), 2);
        assert_eq!(
            *engine2.meta.namespace_names.get(&name_hash("ns1")).unwrap(),
            3
        );
        assert_eq!(*engine2.meta.job_names.get(&name_hash("j1")).unwrap(), 4);
        assert_eq!(
            engine2
                .meta
                .next_id
                .load(std::sync::atomic::Ordering::Relaxed),
            engine
                .meta
                .next_id
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    #[test]
    fn test_snapshot_preserves_queue_messages() {
        let mut engine = make_engine();
        engine.apply_command(
            &MqCommand::create_queue("q", &QueueConfig::default()),
            1,
            1000,
        );
        // Enqueue two messages in separate raft entries (different log indexes)
        let flat_a = make_flat_msg(b"a");
        engine.apply_command(&MqCommand::enqueue(1, &[flat_a], &[None]), 2, 1001);
        let flat_b = make_flat_msg(b"b");
        engine.apply_command(&MqCommand::enqueue(1, &[flat_b], &[None]), 3, 1002);
        // Deliver one
        engine.apply_command(&MqCommand::deliver(1, 100, 1), 4, 1003);

        let snap = engine.snapshot();
        let mut engine2 = make_engine();
        engine2.restore(snap);

        let q = engine2.meta.queues.get(&1).unwrap();
        assert_eq!(q.messages.len(), 2);
        assert_eq!(
            q.pending_count(),
            engine.meta.queues.get(&1).unwrap().pending_count()
        );
        assert_eq!(
            q.in_flight_count(),
            engine.meta.queues.get(&1).unwrap().in_flight_count()
        );
    }

    // =========================================================================
    // Purge floor
    // =========================================================================

    #[test]
    fn test_purge_floor_empty() {
        let mut engine = make_engine();
        assert_eq!(engine.compute_purge_floor(), 0);
    }

    #[test]
    fn test_purge_floor_with_data() {
        let mut engine = make_engine();
        engine.apply_command(
            &MqCommand::create_topic("t1", RetentionPolicy::default(), 0),
            1,
            1000,
        );
        let flat_msg = make_flat_msg(b"msg");
        engine.apply_command(&MqCommand::publish(1, &[flat_msg]), 10, 1001);
        engine.apply_command(
            &MqCommand::create_queue("q1", &QueueConfig::default()),
            2,
            1000,
        );
        let flat_task = make_flat_msg(b"task");
        engine.apply_command(&MqCommand::enqueue(2, &[flat_task], &[None]), 5, 1001);

        // Queue msg at 5, topic msg at 10.
        // Purge floor = min of message indices only (creation indices
        // are tracked separately via structural purge floor in MDBX).
        let floor = engine.compute_purge_floor();
        assert_eq!(floor, 5);
    }

    // =========================================================================
    // Error cases
    // =========================================================================

    #[test]
    fn test_operations_on_nonexistent_entities() {
        let mut engine = make_engine();

        assert!(matches!(
            engine.apply_command(&MqCommand::delete_topic(999), 1, 1000),
            MqResponse::Error(_)
        ));
        assert!(matches!(
            engine.apply_command(&MqCommand::delete_queue(999), 2, 1000),
            MqResponse::Error(_)
        ));
        assert!(matches!(
            engine.apply_command(&MqCommand::publish(999, &[]), 3, 1000,),
            MqResponse::Error(_)
        ));
        assert!(matches!(
            engine.apply_command(&MqCommand::deliver(999, 1, 1), 4, 1000,),
            MqResponse::Error(_)
        ));
    }

    #[test]
    fn test_id_allocation() {
        let mut engine = make_engine();
        engine.apply_command(
            &MqCommand::create_topic("t1", RetentionPolicy::default(), 0),
            1,
            1000,
        );
        engine.apply_command(
            &MqCommand::create_queue("q1", &QueueConfig::default()),
            2,
            1001,
        );
        engine.apply_command(&MqCommand::create_job("j1", &JobConfig::default()), 3, 1002);

        // IDs should be monotonically increasing
        assert!(engine.meta.topics.contains_key(&1));
        assert!(engine.meta.queues.contains_key(&2));
        assert!(engine.meta.jobs.contains_key(&3));
        assert_eq!(
            engine
                .meta
                .next_id
                .load(std::sync::atomic::Ordering::Relaxed),
            4
        );
    }

    // =========================================================================
    // Segment Index Tracking (via SegmentIndexMap)
    // =========================================================================

    #[test]
    fn test_segment_index_tracking_publish() {
        let mut engine = make_engine();
        engine.apply_command(
            &MqCommand::create_topic("t1", RetentionPolicy::default(), 0),
            1,
            1000,
        );

        let sidx = crate::segment_index::SegmentIndexMap::new();
        let cmd = MqCommand::publish(1, &[make_flat_msg(b"msg")]);
        engine.apply_command(&cmd, 2, 1001);
        sidx.track_command(&cmd, (1, 100));

        assert!(sidx.get_builder(1).is_some());
        assert!(!sidx.get_builder(1).unwrap().is_empty());
    }

    #[test]
    fn test_segment_index_tracking_enqueue() {
        let mut engine = make_engine();
        engine.apply_command(
            &MqCommand::create_queue("q1", &QueueConfig::default()),
            1,
            1000,
        );

        let sidx = crate::segment_index::SegmentIndexMap::new();
        let cmd = MqCommand::enqueue(1, &[make_flat_msg(b"task")], &[None]);
        engine.apply_command(&cmd, 2, 1001);
        sidx.track_command(&cmd, (1, 200));

        assert!(sidx.get_builder(1).is_some());
    }

    #[test]
    fn test_segment_index_tracking_actor() {
        let mut engine = make_engine();
        engine.apply_command(
            &MqCommand::create_actor_namespace("ns1", &ActorConfig::default()),
            1,
            1000,
        );

        let sidx = crate::segment_index::SegmentIndexMap::new();
        let cmd = MqCommand::send_to_actor(1, b"actor-1", &make_flat_msg(b"msg"));
        engine.apply_command(&cmd, 2, 1001);
        sidx.track_command(&cmd, (1, 300));

        assert!(sidx.get_builder(1).is_some());
    }

    #[test]
    fn test_segment_index_no_tracking_for_structural() {
        let sidx = crate::segment_index::SegmentIndexMap::new();
        let cmd = MqCommand::create_topic("t1", RetentionPolicy::default(), 0);
        sidx.track_command(&cmd, (1, 0));

        // Structural commands are not tracked
        assert!(sidx.is_empty());
    }

    #[test]
    fn test_take_sealed_indexes_basic() {
        let sidx = crate::segment_index::SegmentIndexMap::new();

        // Add entries in segment 1 and segment 2
        let cmd1 = MqCommand::publish(1, &[make_flat_msg(b"a")]);
        sidx.track_command(&cmd1, (1, 100));

        let cmd2 = MqCommand::publish(1, &[make_flat_msg(b"b")]);
        sidx.track_command(&cmd2, (2, 50));

        // Drain with current=2 → segment 1 should be sealed
        let sealed = sidx.take_sealed(Some(2));
        assert_eq!(sealed.len(), 1);
        assert_eq!(sealed[0].0, 1); // segment_id=1

        // Segment 2 index should still be present
        assert!(sidx.get_builder(2).is_some());
        assert!(sidx.get_builder(1).is_none());
    }

    #[test]
    fn test_take_sealed_indexes_empty() {
        let sidx = crate::segment_index::SegmentIndexMap::new();
        let sealed = sidx.take_sealed(Some(1));
        assert!(sealed.is_empty());
    }

    #[test]
    fn test_take_sealed_indexes_all_when_none_current() {
        let sidx = crate::segment_index::SegmentIndexMap::new();

        let cmd = MqCommand::publish(1, &[make_flat_msg(b"a")]);
        sidx.track_command(&cmd, (1, 100));

        let sealed = sidx.take_sealed(None);
        assert_eq!(sealed.len(), 1);
        assert!(sidx.is_empty());
    }

    #[test]
    fn test_segment_index_cleared() {
        let sidx = crate::segment_index::SegmentIndexMap::new();

        let cmd = MqCommand::publish(1, &[make_flat_msg(b"a")]);
        sidx.track_command(&cmd, (1, 100));

        assert!(!sidx.is_empty());

        sidx.clear();
        assert!(sidx.is_empty());
    }

    #[test]
    fn test_segment_index_roundtrip() {
        use crate::segment_index::{ENTITY_QUEUE, ENTITY_TOPIC};

        let sidx = crate::segment_index::SegmentIndexMap::new();

        // Publish to topic (topic_id=1)
        let pub_cmd = MqCommand::publish(1, &[make_flat_msg(b"msg1")]);
        sidx.track_command(&pub_cmd, (1, 100));

        // Enqueue to queue (queue_id=2)
        let enq_cmd = MqCommand::enqueue(2, &[make_flat_msg(b"task1")], &[None]);
        sidx.track_command(&enq_cmd, (1, 300));

        // Take the index for segment 1
        let sealed = sidx.take_sealed(Some(999));
        assert_eq!(sealed.len(), 1);
        let (seg_id, idx) = sealed.into_iter().next().unwrap();
        assert_eq!(seg_id, 1);

        // Serialize and load as frozen
        let buf = idx.serialize();
        let loaded = crate::segment_index::FrozenSegmentIndex::from_vec(buf).unwrap();
        assert_eq!(loaded.segment_id(), 1);
        assert_eq!(loaded.entry_count(), 2); // topic + queue

        let t = loaded.find_entity(ENTITY_TOPIC, 1);
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].offset, 100);

        let q = loaded.find_entity(ENTITY_QUEUE, 2);
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].offset, 300);
    }

    #[test]
    fn test_segment_index_batch_tracking() {
        use crate::segment_index::{ENTITY_QUEUE, ENTITY_TOPIC};

        let sidx = crate::segment_index::SegmentIndexMap::new();

        let sub_cmds = vec![
            MqCommand::publish(1, &[make_flat_msg(b"a")]),
            MqCommand::enqueue(2, &[make_flat_msg(b"b")], &[None]),
        ];
        let batch_cmd = MqCommand::batch(&sub_cmds);
        sidx.track_command(&batch_cmd, (1, 500));

        let sealed = sidx.take_sealed(Some(999));
        assert_eq!(sealed.len(), 1);
        let (_, idx) = sealed.into_iter().next().unwrap();

        let buf = idx.serialize();
        let loaded = crate::segment_index::FrozenSegmentIndex::from_vec(buf).unwrap();

        let t = loaded.find_entity(ENTITY_TOPIC, 1);
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].offset, 500);

        let q = loaded.find_entity(ENTITY_QUEUE, 2);
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].offset, 500);
    }

    #[test]
    fn test_segment_index_multi_segment_lifecycle() {
        let sidx = crate::segment_index::SegmentIndexMap::new();

        // Simulate writes across 3 segments
        for seg in 1..=3u32 {
            for i in 0..5u32 {
                let cmd = MqCommand::publish(1, &[make_flat_msg(b"msg")]);
                let offset = i * 100;
                sidx.track_command(&cmd, (seg, offset));
            }
        }

        assert_eq!(sidx.len(), 3);

        // Seal segments 1 and 2 (current=3)
        let sealed = sidx.take_sealed(Some(3));
        assert_eq!(sealed.len(), 2);
        let seg_ids: Vec<u32> = sealed.iter().map(|(id, _)| *id).collect();
        assert!(seg_ids.contains(&1));
        assert!(seg_ids.contains(&2));

        assert_eq!(sidx.len(), 1);
        assert!(sidx.get_builder(3).is_some());

        // Each sealed index should have 5 entries for topic 1
        for (_, idx) in sealed {
            let buf = idx.serialize();
            let loaded = crate::segment_index::FrozenSegmentIndex::from_vec(buf).unwrap();
            let entries = loaded.find_entity(crate::segment_index::ENTITY_TOPIC, 1);
            assert_eq!(entries.len(), 5);
        }
    }
}

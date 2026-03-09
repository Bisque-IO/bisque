use std::collections::HashMap;

use bytes::Bytes;
use tracing::{debug, info, warn};

use crate::actor::ActorNamespaceState;
use crate::config::MqConfig;
use crate::consumer::ConsumerState;
use crate::exchange::ExchangeState;
use crate::job::JobInstance;
use crate::producer::ProducerMeta;
use crate::queue::QueueState;
use crate::topic::TopicState;
use crate::types::*;

/// Core MQ engine state.
///
/// All mutation goes through `apply_command`, which is called from the
/// Raft state machine's `apply()` method.
pub struct MqEngine {
    pub(crate) config: MqConfig,

    // Entity stores
    pub(crate) topics: HashMap<u64, TopicState>,
    pub(crate) queues: HashMap<u64, QueueState>,
    pub(crate) actor_namespaces: HashMap<u64, ActorNamespaceState>,
    pub(crate) jobs: HashMap<u64, JobInstance>,
    pub(crate) exchanges: HashMap<u64, ExchangeState>,

    // Session stores
    pub(crate) consumers: HashMap<u64, ConsumerState>,
    pub(crate) producers: HashMap<u64, ProducerMeta>,

    // Name hash (CRC64-NVME) → ID lookup indexes
    pub(crate) topic_names: HashMap<u64, u64>,
    pub(crate) queue_names: HashMap<u64, u64>,
    pub(crate) namespace_names: HashMap<u64, u64>,
    pub(crate) job_names: HashMap<u64, u64>,
    pub(crate) exchange_names: HashMap<u64, u64>,

    // Monotonic ID generator
    pub(crate) next_id: u64,

    // Cached purge floor — avoids O(n) entity scan when nothing changed
    pub(crate) cached_purge_floor: u64,
    pub(crate) purge_floor_dirty: bool,
}

impl MqEngine {
    pub fn new(config: MqConfig) -> Self {
        Self {
            config,
            topics: HashMap::new(),
            queues: HashMap::new(),
            actor_namespaces: HashMap::new(),
            jobs: HashMap::new(),
            exchanges: HashMap::new(),
            consumers: HashMap::new(),
            producers: HashMap::new(),
            topic_names: HashMap::new(),
            queue_names: HashMap::new(),
            namespace_names: HashMap::new(),
            job_names: HashMap::new(),
            exchange_names: HashMap::new(),
            next_id: 1,
            cached_purge_floor: 0,
            purge_floor_dirty: true,
        }
    }

    fn alloc_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Apply a single command. Returns the response.
    ///
    /// `log_index` is the raft log index of the entry containing this command.
    /// `current_time` is a millisecond timestamp (from the leader's wall clock,
    /// included in the raft entry for deterministic replay).
    pub fn apply_command(
        &mut self,
        cmd: MqCommand,
        log_index: u64,
        current_time: u64,
    ) -> MqResponse {
        match cmd {
            // =================================================================
            // Topics
            // =================================================================
            MqCommand::CreateTopic { name, retention } => {
                let hash = name_hash(&name);
                if self.topic_names.contains_key(&hash) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::Topic,
                    });
                }
                let id = self.alloc_id();
                let meta = crate::topic::TopicMeta::new(id, name, log_index, retention);
                self.topics.insert(id, TopicState::new(meta));
                self.topic_names.insert(hash, id);
                info!(topic_id = id, "topic created");
                MqResponse::EntityCreated { id }
            }

            MqCommand::DeleteTopic { topic_id } => {
                if let Some(state) = self.topics.remove(&topic_id) {
                    self.topic_names.remove(&state.meta.name_hash);
                    if state.meta.message_count > 0 {
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

            MqCommand::Publish { topic_id, messages } => {
                if let Some(topic) = self.topics.get_mut(&topic_id) {
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

            MqCommand::CommitOffset {
                topic_id,
                consumer_id,
                offset,
            } => {
                if let Some(topic) = self.topics.get_mut(&topic_id) {
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

            MqCommand::PurgeTopic {
                topic_id,
                before_index,
            } => {
                if let Some(topic) = self.topics.get_mut(&topic_id) {
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
            MqCommand::CreateQueue { name, config } => {
                let hash = name_hash(&name);
                if self.queue_names.contains_key(&hash) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::Queue,
                    });
                }
                let id = self.alloc_id();
                let meta = crate::queue::QueueMeta::new(id, name, log_index, config);
                self.queues.insert(id, QueueState::new(meta));
                self.queue_names.insert(hash, id);
                info!(queue_id = id, "queue created");
                MqResponse::EntityCreated { id }
            }

            MqCommand::DeleteQueue { queue_id } => {
                if let Some(state) = self.queues.remove(&queue_id) {
                    self.queue_names.remove(&state.meta.name_hash);
                    if !state.messages.is_empty() {
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

            MqCommand::Enqueue {
                queue_id,
                messages,
                dedup_keys,
            } => {
                if let Some(queue) = self.queues.get_mut(&queue_id) {
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

            MqCommand::Deliver {
                queue_id,
                consumer_id,
                max_count,
            } => {
                if let Some(queue) = self.queues.get_mut(&queue_id) {
                    let msg_ids =
                        queue.apply_deliver(consumer_id, max_count, current_time, log_index);
                    // Build DeliveredMessage stubs (payload must be fetched from raft log)
                    let messages = msg_ids
                        .iter()
                        .filter_map(|&id| {
                            queue.messages.get(&id).map(|meta| DeliveredMessage {
                                message_id: id,
                                payload: MessagePayload {
                                    key: None,
                                    value: Bytes::new(),
                                    headers: Vec::new(),
                                    timestamp: current_time,
                                    ttl_ms: None,
                                    routing_key: None,
                                },
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

            MqCommand::Ack {
                queue_id,
                message_ids,
            } => {
                // Check before borrowing queue mutably
                if let Some(min_id) = message_ids.iter().copied().min() {
                    self.on_message_removed(min_id);
                }
                if let Some(queue) = self.queues.get_mut(&queue_id) {
                    queue.apply_ack(&message_ids);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::Nack {
                queue_id,
                message_ids,
            } => {
                if let Some(queue) = self.queues.get_mut(&queue_id) {
                    queue.apply_nack(&message_ids);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::ExtendVisibility {
                queue_id,
                message_ids,
                extension_ms,
            } => {
                if let Some(queue) = self.queues.get_mut(&queue_id) {
                    queue.apply_extend_visibility(&message_ids, extension_ms);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::TimeoutExpired {
                queue_id,
                message_ids,
            } => {
                if let Some(queue) = self.queues.get_mut(&queue_id) {
                    let dlq_id = queue.meta.config.dead_letter_queue_id;
                    let _dead_lettered = queue.apply_timeout_expired(&message_ids, dlq_id);
                    // TODO: move dead_lettered messages to DLQ if configured
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::PruneDedupWindow {
                queue_id,
                before_timestamp,
            } => {
                if let Some(queue) = self.queues.get_mut(&queue_id) {
                    queue.apply_prune_dedup(before_timestamp);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    })
                }
            }

            MqCommand::PurgeQueue { queue_id } => {
                if let Some(queue) = self.queues.get_mut(&queue_id) {
                    let had_messages = !queue.messages.is_empty();
                    queue.messages.clear();
                    queue.pending.clear();
                    queue.in_flight_deadlines.clear();
                    queue.consumer_in_flight.clear();
                    queue.meta.pending_count = 0;
                    queue.meta.in_flight_count = 0;
                    if had_messages {
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

            MqCommand::GetQueueAttributes { queue_id } => {
                if let Some(queue) = self.queues.get(&queue_id) {
                    MqResponse::Stats(EntityStats::Queue {
                        queue_id,
                        pending_count: queue.meta.pending_count,
                        in_flight_count: queue.meta.in_flight_count,
                        dlq_count: queue.meta.dlq_count,
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
            MqCommand::CreateExchange {
                name,
                exchange_type,
            } => {
                let hash = name_hash(&name);
                if self.exchange_names.contains_key(&hash) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::Exchange,
                    });
                }
                let id = self.alloc_id();
                let meta = crate::exchange::ExchangeMeta::new(id, name, log_index, exchange_type);
                self.exchanges.insert(id, ExchangeState::new(meta));
                self.exchange_names.insert(hash, id);
                info!(exchange_id = id, "exchange created");
                MqResponse::EntityCreated { id }
            }

            MqCommand::DeleteExchange { exchange_id } => {
                if let Some(state) = self.exchanges.remove(&exchange_id) {
                    self.exchange_names.remove(&state.meta.name_hash);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Exchange,
                        id: exchange_id,
                    })
                }
            }

            MqCommand::CreateBinding {
                exchange_id,
                queue_id,
                routing_key,
            } => {
                // Verify both exchange and queue exist
                if !self.exchanges.contains_key(&exchange_id) {
                    return MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Exchange,
                        id: exchange_id,
                    });
                }
                if !self.queues.contains_key(&queue_id) {
                    return MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Queue,
                        id: queue_id,
                    });
                }
                let binding_id = self.alloc_id();
                let binding = Binding {
                    binding_id,
                    exchange_id,
                    queue_id,
                    routing_key,
                };
                if let Some(exchange) = self.exchanges.get_mut(&exchange_id) {
                    exchange.add_binding(binding);
                }
                MqResponse::EntityCreated { id: binding_id }
            }

            MqCommand::DeleteBinding { binding_id } => {
                // Find which exchange owns this binding
                let mut found = false;
                for exchange in self.exchanges.values_mut() {
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

            MqCommand::PublishToExchange {
                exchange_id,
                messages,
            } => {
                // Route messages to bound queues
                let target_queue_ids = if let Some(exchange) = self.exchanges.get(&exchange_id) {
                    // Use first message's routing key for routing
                    let routing_key = messages.first().and_then(|m| m.routing_key.as_deref());
                    exchange.route(routing_key)
                } else {
                    return MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Exchange,
                        id: exchange_id,
                    });
                };

                // Enqueue to each target queue
                let mut total_enqueued = 0u64;
                for qid in target_queue_ids {
                    if let Some(queue) = self.queues.get_mut(&qid) {
                        let dedup_keys: Vec<Option<Bytes>> =
                            messages.iter().map(|_| None).collect();
                        queue.apply_enqueue(log_index, &messages, &dedup_keys, current_time);
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
            MqCommand::CreateActorNamespace { name, config } => {
                let hash = name_hash(&name);
                if self.namespace_names.contains_key(&hash) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::ActorNamespace,
                    });
                }
                let id = self.alloc_id();
                let meta = crate::actor::ActorNamespaceMeta::new(id, name, log_index, config);
                self.actor_namespaces
                    .insert(id, ActorNamespaceState::new(meta));
                self.namespace_names.insert(hash, id);
                info!(namespace_id = id, "actor namespace created");
                MqResponse::EntityCreated { id }
            }

            MqCommand::DeleteActorNamespace { namespace_id } => {
                if let Some(state) = self.actor_namespaces.remove(&namespace_id) {
                    self.namespace_names.remove(&state.meta.name_hash);
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

            MqCommand::SendToActor {
                namespace_id,
                actor_id,
                message: _,
            } => {
                if let Some(ns) = self.actor_namespaces.get_mut(&namespace_id) {
                    match ns.apply_send(&actor_id, log_index, current_time) {
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

            MqCommand::DeliverActorMessage {
                namespace_id,
                actor_id,
                consumer_id,
            } => {
                if let Some(ns) = self.actor_namespaces.get_mut(&namespace_id) {
                    match ns.apply_deliver(&actor_id, consumer_id) {
                        Some(msg_index) => MqResponse::Messages {
                            messages: vec![DeliveredMessage {
                                message_id: msg_index,
                                payload: MessagePayload {
                                    key: None,
                                    value: Bytes::new(),
                                    headers: Vec::new(),
                                    timestamp: current_time,
                                    ttl_ms: None,
                                    routing_key: None,
                                },
                                attempt: 1,
                                original_timestamp: current_time,
                            }],
                        },
                        None => MqResponse::Messages {
                            messages: Vec::new(),
                        },
                    }
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ActorNamespace,
                        id: namespace_id,
                    })
                }
            }

            MqCommand::AckActorMessage {
                namespace_id,
                actor_id,
                message_id,
            } => {
                self.on_message_removed(message_id);
                if let Some(ns) = self.actor_namespaces.get_mut(&namespace_id) {
                    ns.apply_ack(&actor_id, message_id);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ActorNamespace,
                        id: namespace_id,
                    })
                }
            }

            MqCommand::NackActorMessage {
                namespace_id,
                actor_id,
                message_id,
            } => {
                if let Some(ns) = self.actor_namespaces.get_mut(&namespace_id) {
                    ns.apply_nack(&actor_id, message_id);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ActorNamespace,
                        id: namespace_id,
                    })
                }
            }

            MqCommand::AssignActors {
                namespace_id,
                consumer_id,
                actor_ids,
            } => {
                if let Some(ns) = self.actor_namespaces.get_mut(&namespace_id) {
                    ns.apply_assign(consumer_id, &actor_ids);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ActorNamespace,
                        id: namespace_id,
                    })
                }
            }

            MqCommand::ReleaseActors {
                namespace_id,
                consumer_id,
            } => {
                if let Some(ns) = self.actor_namespaces.get_mut(&namespace_id) {
                    ns.apply_release(consumer_id);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::ActorNamespace,
                        id: namespace_id,
                    })
                }
            }

            MqCommand::EvictIdleActors {
                namespace_id,
                before_timestamp,
            } => {
                if let Some(ns) = self.actor_namespaces.get_mut(&namespace_id) {
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
            MqCommand::CreateJob { name, config } => {
                let hash = name_hash(&name);
                if self.job_names.contains_key(&hash) {
                    return MqResponse::Error(MqError::AlreadyExists {
                        entity: EntityKind::Job,
                    });
                }
                let id = self.alloc_id();
                let meta = crate::job::JobMeta::new(id, name, log_index, config);
                self.jobs.insert(id, JobInstance::new(meta));
                self.job_names.insert(hash, id);
                info!(job_id = id, "job created");
                MqResponse::EntityCreated { id }
            }

            MqCommand::DeleteJob { job_id } => {
                if let Some(job) = self.jobs.remove(&job_id) {
                    self.job_names.remove(&job.meta.name_hash);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::UpdateJob { job_id, config } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.meta.config = config;
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::EnableJob { job_id } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.meta.enabled = true;
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::DisableJob { job_id } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.meta.enabled = false;
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::TriggerJob {
                job_id,
                execution_id,
                triggered_at,
            } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.apply_trigger(execution_id, triggered_at);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::AssignJob {
                job_id,
                consumer_id,
            } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.apply_assign(consumer_id);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::CompleteJob {
                job_id,
                execution_id,
            } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.apply_complete(execution_id, current_time);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::FailJob {
                job_id,
                execution_id,
                error,
            } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    warn!(job_id, execution_id, %error, "job execution failed");
                    job.apply_fail(execution_id);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Job,
                        id: job_id,
                    })
                }
            }

            MqCommand::TimeoutJob {
                job_id,
                execution_id,
            } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
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
            MqCommand::RegisterConsumer {
                consumer_id,
                group_name,
                subscriptions,
            } => {
                let meta = crate::consumer::ConsumerMeta::new(
                    consumer_id,
                    group_name,
                    log_index,
                    subscriptions,
                );
                self.consumers.insert(consumer_id, ConsumerState::new(meta));
                debug!(consumer_id, "consumer registered");
                MqResponse::Ok
            }

            MqCommand::DisconnectConsumer { consumer_id } => {
                if let Some(_consumer) = self.consumers.remove(&consumer_id) {
                    // Release all actor assignments for this consumer
                    for ns in self.actor_namespaces.values_mut() {
                        ns.apply_release(consumer_id);
                    }
                    // Release job assignments
                    for job in self.jobs.values_mut() {
                        if job.meta.state.assigned_consumer_id == Some(consumer_id) {
                            job.meta.state.assigned_consumer_id = None;
                        }
                    }
                    // Return in-flight queue messages (uses consumer index)
                    let mut had_in_flight = false;
                    for queue in self.queues.values_mut() {
                        let in_flight_ids = queue.consumer_in_flight_ids(consumer_id);
                        if !in_flight_ids.is_empty() {
                            had_in_flight = true;
                            queue.apply_nack(&in_flight_ids);
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

            MqCommand::Heartbeat { consumer_id } => {
                if let Some(consumer) = self.consumers.get_mut(&consumer_id) {
                    consumer.heartbeat(current_time);
                    MqResponse::Ok
                } else {
                    MqResponse::Error(MqError::NotFound {
                        entity: EntityKind::Consumer,
                        id: consumer_id,
                    })
                }
            }

            MqCommand::RegisterProducer { producer_id, name } => {
                let meta = ProducerMeta::new(producer_id, name, log_index);
                self.producers.insert(producer_id, meta);
                MqResponse::Ok
            }

            MqCommand::DisconnectProducer { producer_id } => {
                self.producers.remove(&producer_id);
                MqResponse::Ok
            }

            MqCommand::Batch(cmds) => {
                let responses: Vec<MqResponse> = cmds
                    .into_iter()
                    .map(|cmd| self.apply_command(cmd, log_index, current_time))
                    .collect();
                MqResponse::BatchResponse(responses)
            }
        }
    }

    // =========================================================================
    // Public accessors for server integration
    // =========================================================================

    /// Resolve an entity by type and name hash. Returns the entity ID if found.
    pub fn resolve_entity(&self, entity_type: u8, name_hash: u64) -> Option<u64> {
        match entity_type {
            0 => self.topic_names.get(&name_hash).copied(), // ENTITY_TYPE_TOPIC
            1 => self.queue_names.get(&name_hash).copied(), // ENTITY_TYPE_QUEUE
            2 => self.namespace_names.get(&name_hash).copied(), // ENTITY_TYPE_ACTOR_NAMESPACE
            3 => self.job_names.get(&name_hash).copied(),   // ENTITY_TYPE_JOB
            4 => self.exchange_names.get(&name_hash).copied(), // ENTITY_TYPE_EXCHANGE
            _ => None,
        }
    }

    /// Get the current head index for a topic.
    pub fn get_topic_head(&self, topic_id: u64) -> u64 {
        self.topics
            .get(&topic_id)
            .map(|t| t.meta.head_index)
            .unwrap_or(0)
    }

    /// Build a snapshot of all engine state.
    pub fn snapshot(&self) -> MqSnapshotData {
        let topics = self
            .topics
            .values()
            .map(|t| TopicSnapshot {
                meta: t.meta.clone(),
                consumer_offsets: t.consumer_offsets.values().cloned().collect(),
            })
            .collect();

        let queues = self
            .queues
            .values()
            .map(|q| QueueSnapshot {
                meta: q.meta.clone(),
                messages: q.messages.values().cloned().collect(),
                dedup_entries: q.dedup.snapshot_entries(),
            })
            .collect();

        let actor_namespaces = self
            .actor_namespaces
            .values()
            .map(|ns| ActorNamespaceSnapshot {
                meta: ns.meta.clone(),
                actors: ns.actors.values().map(|a| a.state.clone()).collect(),
            })
            .collect();

        let jobs = self
            .jobs
            .values()
            .map(|j| JobSnapshot {
                meta: j.meta.clone(),
            })
            .collect();

        let consumers = self
            .consumers
            .values()
            .map(|c| ConsumerSnapshot {
                meta: c.meta.clone(),
            })
            .collect();

        let producers = self
            .producers
            .values()
            .map(|p| ProducerSnapshot { meta: p.clone() })
            .collect();

        let exchanges = self
            .exchanges
            .values()
            .map(|e| ExchangeSnapshot {
                meta: e.meta.clone(),
                bindings: e.bindings.values().cloned().collect(),
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
            next_id: self.next_id,
            file_manifest: Vec::new(),
            sync_addr: None,
        }
    }

    /// Restore engine state from a snapshot.
    pub fn restore(&mut self, data: MqSnapshotData) {
        self.next_id = data.next_id;
        self.purge_floor_dirty = true;

        // Replace maps with pre-allocated capacity
        let n_topics = data.topics.len();
        let n_queues = data.queues.len();
        let n_ns = data.actor_namespaces.len();
        let n_jobs = data.jobs.len();
        let n_consumers = data.consumers.len();
        let n_producers = data.producers.len();

        let n_exchanges = data.exchanges.len();

        self.topics = HashMap::with_capacity(n_topics);
        self.queues = HashMap::with_capacity(n_queues);
        self.actor_namespaces = HashMap::with_capacity(n_ns);
        self.jobs = HashMap::with_capacity(n_jobs);
        self.exchanges = HashMap::with_capacity(n_exchanges);
        self.consumers = HashMap::with_capacity(n_consumers);
        self.producers = HashMap::with_capacity(n_producers);
        self.topic_names = HashMap::with_capacity(n_topics);
        self.queue_names = HashMap::with_capacity(n_queues);
        self.namespace_names = HashMap::with_capacity(n_ns);
        self.job_names = HashMap::with_capacity(n_jobs);
        self.exchange_names = HashMap::with_capacity(n_exchanges);

        // Restore topics
        for ts in data.topics {
            let id = ts.meta.topic_id;
            let mut meta = ts.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let mut state = TopicState::new(meta);
            for offset in ts.consumer_offsets {
                state.consumer_offsets.insert(offset.consumer_id, offset);
            }
            self.topic_names.insert(hash, id);
            self.topics.insert(id, state);
        }

        // Restore queues (rebuild in-flight indexes)
        for qs in data.queues {
            let id = qs.meta.queue_id;
            let mut meta = qs.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let mut state = QueueState::new(meta);
            for msg in qs.messages {
                if msg.state == MessageState::Pending {
                    state.pending.insert((msg.priority, msg.message_id), ());
                } else if msg.state == MessageState::InFlight {
                    // Rebuild deadline index
                    if let Some(deadline) = msg.visibility_deadline {
                        state
                            .in_flight_deadlines
                            .entry(deadline)
                            .or_default()
                            .push(msg.message_id);
                    }
                    // Rebuild consumer in-flight index
                    if let Some(consumer_id) = msg.consumer_id {
                        state
                            .consumer_in_flight
                            .entry(consumer_id)
                            .or_default()
                            .push(msg.message_id);
                    }
                }
                state.messages.insert(msg.message_id, msg);
            }
            state.dedup.restore_entries(qs.dedup_entries);
            self.queue_names.insert(hash, id);
            self.queues.insert(id, state);
        }

        // Restore actor namespaces
        for ans in data.actor_namespaces {
            let id = ans.meta.namespace_id;
            let mut meta = ans.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let mut state = ActorNamespaceState::new(meta);
            for actor_state in ans.actors {
                let actor_id = actor_state.actor_id.clone();
                state.actors.insert(
                    actor_id,
                    crate::actor::ActorInMemory {
                        state: actor_state,
                        mailbox: std::collections::VecDeque::new(), // mailbox rebuilt from raft log
                    },
                );
            }
            self.namespace_names.insert(hash, id);
            self.actor_namespaces.insert(id, state);
        }

        // Restore jobs
        for js in data.jobs {
            let id = js.meta.job_id;
            let mut meta = js.meta;
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            self.job_names.insert(hash, id);
            self.jobs.insert(id, JobInstance::new(meta));
        }

        // Restore consumers
        for cs in data.consumers {
            let id = cs.meta.consumer_id;
            self.consumers.insert(id, ConsumerState::new(cs.meta));
        }

        // Restore producers
        for ps in data.producers {
            let id = ps.meta.producer_id;
            self.producers.insert(id, ps.meta);
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
            self.exchange_names.insert(hash, id);
            self.exchanges.insert(id, state);
        }

        info!(
            topics = self.topics.len(),
            queues = self.queues.len(),
            actor_namespaces = self.actor_namespaces.len(),
            jobs = self.jobs.len(),
            exchanges = self.exchanges.len(),
            consumers = self.consumers.len(),
            "MQ engine restored from snapshot"
        );
    }

    /// Restore only structural state (entity metadata) from MDBX.
    ///
    /// This creates empty entity shells (no messages, no in-flight state).
    /// The raft log is then replayed from the structural purge floor to
    /// rebuild message state.
    pub(crate) fn restore_structural(&mut self, state: crate::manifest::StructuralState) {
        self.next_id = state.next_id;
        self.purge_floor_dirty = true;

        self.topics = HashMap::with_capacity(state.topics.len());
        self.queues = HashMap::with_capacity(state.queues.len());
        self.actor_namespaces = HashMap::with_capacity(state.actor_namespaces.len());
        self.jobs = HashMap::with_capacity(state.jobs.len());
        self.consumers = HashMap::new();
        self.producers = HashMap::new();
        self.topic_names = HashMap::with_capacity(state.topics.len());
        self.queue_names = HashMap::with_capacity(state.queues.len());
        self.namespace_names = HashMap::with_capacity(state.actor_namespaces.len());
        self.job_names = HashMap::with_capacity(state.jobs.len());

        for mut meta in state.topics {
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let id = meta.topic_id;
            self.topic_names.insert(hash, id);
            self.topics.insert(id, TopicState::new(meta));
        }

        for mut meta in state.queues {
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let id = meta.queue_id;
            self.queue_names.insert(hash, id);
            self.queues.insert(id, QueueState::new(meta));
        }

        for mut meta in state.actor_namespaces {
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let id = meta.namespace_id;
            self.namespace_names.insert(hash, id);
            self.actor_namespaces
                .insert(id, ActorNamespaceState::new(meta));
        }

        for mut meta in state.jobs {
            meta.ensure_name_hash();
            let hash = meta.name_hash;
            let id = meta.job_id;
            self.job_names.insert(hash, id);
            self.jobs.insert(id, JobInstance::new(meta));
        }

        info!(
            topics = self.topics.len(),
            queues = self.queues.len(),
            actor_namespaces = self.actor_namespaces.len(),
            jobs = self.jobs.len(),
            next_id = self.next_id,
            "MQ engine restored structural state from MDBX"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ActorConfig, JobConfig, QueueConfig};
    use crate::types::name_hash;

    fn make_engine() -> MqEngine {
        MqEngine::new(MqConfig::new("/tmp/mq-test"))
    }

    fn make_msg(value: &[u8]) -> MessagePayload {
        MessagePayload {
            key: None,
            value: Bytes::from(value.to_vec()),
            headers: Vec::new(),
            timestamp: 1000,
            ttl_ms: None,
            routing_key: None,
        }
    }

    // =========================================================================
    // Topic commands
    // =========================================================================

    #[test]
    fn test_create_topic() {
        let mut engine = make_engine();
        let resp = engine.apply_command(
            MqCommand::CreateTopic {
                name: "events".to_string(),
                retention: RetentionPolicy::default(),
            },
            1,
            1000,
        );
        match resp {
            MqResponse::EntityCreated { id } => {
                assert_eq!(id, 1);
                assert!(engine.topics.contains_key(&1));
                assert_eq!(engine.topic_names[&name_hash("events")], 1);
            }
            _ => panic!("expected EntityCreated"),
        }
    }

    #[test]
    fn test_create_duplicate_topic() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateTopic {
                name: "events".to_string(),
                retention: RetentionPolicy::default(),
            },
            1,
            1000,
        );
        let resp = engine.apply_command(
            MqCommand::CreateTopic {
                name: "events".to_string(),
                retention: RetentionPolicy::default(),
            },
            2,
            1001,
        );
        assert!(matches!(resp, MqResponse::Error(_)));
    }

    #[test]
    fn test_delete_topic() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateTopic {
                name: "events".to_string(),
                retention: RetentionPolicy::default(),
            },
            1,
            1000,
        );
        let resp = engine.apply_command(MqCommand::DeleteTopic { topic_id: 1 }, 2, 1001);
        assert!(matches!(resp, MqResponse::Ok));
        assert!(!engine.topics.contains_key(&1));
        assert!(!engine.topic_names.contains_key(&name_hash("events")));
    }

    #[test]
    fn test_publish_and_commit_offset() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateTopic {
                name: "events".to_string(),
                retention: RetentionPolicy::default(),
            },
            1,
            1000,
        );
        let resp = engine.apply_command(
            MqCommand::Publish {
                topic_id: 1,
                messages: vec![make_msg(b"hello")],
            },
            2,
            1001,
        );
        assert!(matches!(resp, MqResponse::Published { offsets } if offsets.len() == 1));

        let resp = engine.apply_command(
            MqCommand::CommitOffset {
                topic_id: 1,
                consumer_id: 100,
                offset: 2,
            },
            3,
            1002,
        );
        assert!(matches!(resp, MqResponse::Ok));
    }

    // =========================================================================
    // Queue commands
    // =========================================================================

    #[test]
    fn test_queue_lifecycle() {
        let mut engine = make_engine();
        let resp = engine.apply_command(
            MqCommand::CreateQueue {
                name: "tasks".to_string(),
                config: QueueConfig::default(),
            },
            1,
            1000,
        );
        let queue_id = match resp {
            MqResponse::EntityCreated { id } => id,
            _ => panic!("expected EntityCreated"),
        };

        // Enqueue
        let resp = engine.apply_command(
            MqCommand::Enqueue {
                queue_id,
                messages: vec![make_msg(b"task1")],
                dedup_keys: vec![None],
            },
            2,
            1001,
        );
        assert!(matches!(resp, MqResponse::Published { .. }));

        // Deliver
        let resp = engine.apply_command(
            MqCommand::Deliver {
                queue_id,
                consumer_id: 100,
                max_count: 10,
            },
            3,
            1002,
        );
        let msg_ids: Vec<u64> = match &resp {
            MqResponse::Messages { messages } => {
                assert_eq!(messages.len(), 1);
                messages.iter().map(|m| m.message_id).collect()
            }
            _ => panic!("expected Messages"),
        };

        // Ack
        let resp = engine.apply_command(
            MqCommand::Ack {
                queue_id,
                message_ids: msg_ids,
            },
            4,
            1003,
        );
        assert!(matches!(resp, MqResponse::Ok));
        assert_eq!(engine.queues[&queue_id].meta.in_flight_count, 0);
    }

    #[test]
    fn test_queue_nack_and_redeliver() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateQueue {
                name: "q".to_string(),
                config: QueueConfig::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::Enqueue {
                queue_id: 1,
                messages: vec![make_msg(b"msg")],
                dedup_keys: vec![None],
            },
            2,
            1001,
        );
        engine.apply_command(
            MqCommand::Deliver {
                queue_id: 1,
                consumer_id: 100,
                max_count: 1,
            },
            3,
            1002,
        );
        engine.apply_command(
            MqCommand::Nack {
                queue_id: 1,
                message_ids: vec![2],
            },
            4,
            1003,
        );

        assert_eq!(engine.queues[&1].meta.pending_count, 1);
        assert_eq!(engine.queues[&1].meta.in_flight_count, 0);

        // Re-deliver
        let resp = engine.apply_command(
            MqCommand::Deliver {
                queue_id: 1,
                consumer_id: 100,
                max_count: 1,
            },
            5,
            1004,
        );
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
            MqCommand::CreateActorNamespace {
                name: "users".to_string(),
                config: ActorConfig::default(),
            },
            1,
            1000,
        );
        let ns_id = match resp {
            MqResponse::EntityCreated { id } => id,
            _ => panic!("expected EntityCreated"),
        };

        // Send to actor
        let actor_id = Bytes::from_static(b"user-42");
        let resp = engine.apply_command(
            MqCommand::SendToActor {
                namespace_id: ns_id,
                actor_id: actor_id.clone(),
                message: make_msg(b"hello"),
            },
            2,
            1001,
        );
        assert!(matches!(resp, MqResponse::Ok));

        // Assign
        engine.apply_command(
            MqCommand::AssignActors {
                namespace_id: ns_id,
                consumer_id: 100,
                actor_ids: vec![actor_id.clone()],
            },
            3,
            1002,
        );

        // Deliver
        let resp = engine.apply_command(
            MqCommand::DeliverActorMessage {
                namespace_id: ns_id,
                actor_id: actor_id.clone(),
                consumer_id: 100,
            },
            4,
            1003,
        );
        match resp {
            MqResponse::Messages { messages } => assert_eq!(messages.len(), 1),
            _ => panic!("expected Messages"),
        }

        // Ack
        let resp = engine.apply_command(
            MqCommand::AckActorMessage {
                namespace_id: ns_id,
                actor_id: actor_id.clone(),
                message_id: 2, // log_index from send
            },
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
        let resp = engine.apply_command(
            MqCommand::CreateJob {
                name: "cleanup".to_string(),
                config,
            },
            1,
            1000,
        );
        let job_id = match resp {
            MqResponse::EntityCreated { id } => id,
            _ => panic!("expected EntityCreated"),
        };

        // Trigger
        engine.apply_command(
            MqCommand::TriggerJob {
                job_id,
                execution_id: 100,
                triggered_at: 5000,
            },
            2,
            5000,
        );
        assert_eq!(
            engine.jobs[&job_id].meta.state.current_execution_id,
            Some(100)
        );

        // Complete
        engine.apply_command(
            MqCommand::CompleteJob {
                job_id,
                execution_id: 100,
            },
            3,
            6000,
        );
        assert!(
            engine.jobs[&job_id]
                .meta
                .state
                .current_execution_id
                .is_none()
        );
    }

    #[test]
    fn test_job_enable_disable() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateJob {
                name: "j".to_string(),
                config: JobConfig::default(),
            },
            1,
            1000,
        );

        engine.apply_command(MqCommand::DisableJob { job_id: 1 }, 2, 1001);
        assert!(!engine.jobs[&1].meta.enabled);

        engine.apply_command(MqCommand::EnableJob { job_id: 1 }, 3, 1002);
        assert!(engine.jobs[&1].meta.enabled);
    }

    // =========================================================================
    // Session commands
    // =========================================================================

    #[test]
    fn test_consumer_lifecycle() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::RegisterConsumer {
                consumer_id: 100,
                group_name: "group-1".to_string(),
                subscriptions: Vec::new(),
            },
            1,
            1000,
        );
        assert!(engine.consumers.contains_key(&100));

        engine.apply_command(MqCommand::Heartbeat { consumer_id: 100 }, 2, 2000);
        assert_eq!(engine.consumers[&100].meta.last_heartbeat_at, 2000);

        engine.apply_command(MqCommand::DisconnectConsumer { consumer_id: 100 }, 3, 3000);
        assert!(!engine.consumers.contains_key(&100));
    }

    #[test]
    fn test_disconnect_consumer_releases_actors() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateActorNamespace {
                name: "ns".to_string(),
                config: ActorConfig::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::RegisterConsumer {
                consumer_id: 100,
                group_name: "g".to_string(),
                subscriptions: Vec::new(),
            },
            2,
            1000,
        );
        let aid = Bytes::from_static(b"actor-1");
        engine.apply_command(
            MqCommand::SendToActor {
                namespace_id: 1,
                actor_id: aid.clone(),
                message: make_msg(b"msg"),
            },
            3,
            1001,
        );
        engine.apply_command(
            MqCommand::AssignActors {
                namespace_id: 1,
                consumer_id: 100,
                actor_ids: vec![aid.clone()],
            },
            4,
            1002,
        );

        // Disconnect — should release actor assignments
        engine.apply_command(MqCommand::DisconnectConsumer { consumer_id: 100 }, 5, 1003);
        let ns = &engine.actor_namespaces[&1];
        assert!(ns.actors[&aid].state.assigned_consumer_id.is_none());
    }

    #[test]
    fn test_disconnect_consumer_nacks_queue_messages() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateQueue {
                name: "q".to_string(),
                config: QueueConfig::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::RegisterConsumer {
                consumer_id: 100,
                group_name: "g".to_string(),
                subscriptions: Vec::new(),
            },
            2,
            1000,
        );
        engine.apply_command(
            MqCommand::Enqueue {
                queue_id: 1,
                messages: vec![make_msg(b"msg")],
                dedup_keys: vec![None],
            },
            3,
            1001,
        );
        engine.apply_command(
            MqCommand::Deliver {
                queue_id: 1,
                consumer_id: 100,
                max_count: 1,
            },
            4,
            1002,
        );

        assert_eq!(engine.queues[&1].meta.in_flight_count, 1);

        engine.apply_command(MqCommand::DisconnectConsumer { consumer_id: 100 }, 5, 1003);
        assert_eq!(engine.queues[&1].meta.in_flight_count, 0);
        assert_eq!(engine.queues[&1].meta.pending_count, 1);
    }

    #[test]
    fn test_producer_lifecycle() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::RegisterProducer {
                producer_id: 200,
                name: Some("producer-1".to_string()),
            },
            1,
            1000,
        );
        assert!(engine.producers.contains_key(&200));

        engine.apply_command(MqCommand::DisconnectProducer { producer_id: 200 }, 2, 1001);
        assert!(!engine.producers.contains_key(&200));
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
            MqCommand::CreateTopic {
                name: "t1".to_string(),
                retention: RetentionPolicy::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::CreateQueue {
                name: "q1".to_string(),
                config: QueueConfig::default(),
            },
            2,
            1001,
        );
        engine.apply_command(
            MqCommand::CreateActorNamespace {
                name: "ns1".to_string(),
                config: ActorConfig::default(),
            },
            3,
            1002,
        );
        engine.apply_command(
            MqCommand::CreateJob {
                name: "j1".to_string(),
                config: JobConfig {
                    cron_expression: "0 * * * * *".to_string(),
                    ..Default::default()
                },
            },
            4,
            1003,
        );

        // Publish some data
        engine.apply_command(
            MqCommand::Publish {
                topic_id: 1,
                messages: vec![make_msg(b"msg1")],
            },
            5,
            1004,
        );
        engine.apply_command(
            MqCommand::Enqueue {
                queue_id: 2,
                messages: vec![make_msg(b"task1")],
                dedup_keys: vec![None],
            },
            6,
            1005,
        );

        let snap = engine.snapshot();
        assert_eq!(snap.topics.len(), 1);
        assert_eq!(snap.queues.len(), 1);
        assert_eq!(snap.actor_namespaces.len(), 1);
        assert_eq!(snap.jobs.len(), 1);

        // Restore into fresh engine
        let mut engine2 = make_engine();
        engine2.restore(snap);

        assert_eq!(engine2.topics.len(), 1);
        assert_eq!(engine2.queues.len(), 1);
        assert_eq!(engine2.actor_namespaces.len(), 1);
        assert_eq!(engine2.jobs.len(), 1);
        assert_eq!(engine2.topic_names[&name_hash("t1")], 1);
        assert_eq!(engine2.queue_names[&name_hash("q1")], 2);
        assert_eq!(engine2.namespace_names[&name_hash("ns1")], 3);
        assert_eq!(engine2.job_names[&name_hash("j1")], 4);
        assert_eq!(engine2.next_id, engine.next_id);
    }

    #[test]
    fn test_snapshot_preserves_queue_messages() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateQueue {
                name: "q".to_string(),
                config: QueueConfig::default(),
            },
            1,
            1000,
        );
        // Enqueue two messages in separate raft entries (different log indexes)
        engine.apply_command(
            MqCommand::Enqueue {
                queue_id: 1,
                messages: vec![make_msg(b"a")],
                dedup_keys: vec![None],
            },
            2,
            1001,
        );
        engine.apply_command(
            MqCommand::Enqueue {
                queue_id: 1,
                messages: vec![make_msg(b"b")],
                dedup_keys: vec![None],
            },
            3,
            1002,
        );
        // Deliver one
        engine.apply_command(
            MqCommand::Deliver {
                queue_id: 1,
                consumer_id: 100,
                max_count: 1,
            },
            4,
            1003,
        );

        let snap = engine.snapshot();
        let mut engine2 = make_engine();
        engine2.restore(snap);

        let q = &engine2.queues[&1];
        assert_eq!(q.messages.len(), 2);
        assert_eq!(q.meta.pending_count, engine.queues[&1].meta.pending_count);
        assert_eq!(
            q.meta.in_flight_count,
            engine.queues[&1].meta.in_flight_count
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
            MqCommand::CreateTopic {
                name: "t1".to_string(),
                retention: RetentionPolicy::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::Publish {
                topic_id: 1,
                messages: vec![make_msg(b"msg")],
            },
            10,
            1001,
        );
        engine.apply_command(
            MqCommand::CreateQueue {
                name: "q1".to_string(),
                config: QueueConfig::default(),
            },
            2,
            1000,
        );
        engine.apply_command(
            MqCommand::Enqueue {
                queue_id: 2,
                messages: vec![make_msg(b"task")],
                dedup_keys: vec![None],
            },
            5,
            1001,
        );

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
            engine.apply_command(MqCommand::DeleteTopic { topic_id: 999 }, 1, 1000),
            MqResponse::Error(_)
        ));
        assert!(matches!(
            engine.apply_command(MqCommand::DeleteQueue { queue_id: 999 }, 2, 1000),
            MqResponse::Error(_)
        ));
        assert!(matches!(
            engine.apply_command(
                MqCommand::Publish {
                    topic_id: 999,
                    messages: Vec::new()
                },
                3,
                1000
            ),
            MqResponse::Error(_)
        ));
        assert!(matches!(
            engine.apply_command(
                MqCommand::Deliver {
                    queue_id: 999,
                    consumer_id: 1,
                    max_count: 1
                },
                4,
                1000
            ),
            MqResponse::Error(_)
        ));
    }

    #[test]
    fn test_id_allocation() {
        let mut engine = make_engine();
        engine.apply_command(
            MqCommand::CreateTopic {
                name: "t1".to_string(),
                retention: RetentionPolicy::default(),
            },
            1,
            1000,
        );
        engine.apply_command(
            MqCommand::CreateQueue {
                name: "q1".to_string(),
                config: QueueConfig::default(),
            },
            2,
            1001,
        );
        engine.apply_command(
            MqCommand::CreateJob {
                name: "j1".to_string(),
                config: JobConfig::default(),
            },
            3,
            1002,
        );

        // IDs should be monotonically increasing
        assert!(engine.topics.contains_key(&1));
        assert!(engine.queues.contains_key(&2));
        assert!(engine.jobs.contains_key(&3));
        assert_eq!(engine.next_id, 4);
    }
}

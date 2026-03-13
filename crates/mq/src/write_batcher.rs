//! Write batcher — coalesces individual MqCommand submissions into batched
//! Raft proposals.
//!
//! When ingesting lots of small writes, each individual command would produce
//! a separate Raft proposal. The write batcher accumulates commands over a
//! short linger window (or until a count threshold is reached) and submits
//! them as a single coalesced `MqCommand::Batch`.
//!
//! Uses [`crossfire`] lock-free bounded channels for the ingestion queue.

use std::collections::HashMap;
use std::time::Duration;

use openraft::Raft;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::MqTypeConfig;
use crate::types::{MqCommand, MqError, MqResponse};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the MQ write batcher.
#[derive(Debug, Clone)]
pub struct MqWriteBatcherConfig {
    /// How long to wait after the first request before flushing a batch.
    pub linger: Duration,
    /// Maximum number of commands to accumulate before flushing early.
    pub max_batch_count: usize,
    /// Crossfire channel capacity.
    pub channel_capacity: usize,
}

impl Default for MqWriteBatcherConfig {
    fn default() -> Self {
        Self {
            linger: Duration::from_millis(5),
            max_batch_count: 256,
            channel_capacity: 1024,
        }
    }
}

impl MqWriteBatcherConfig {
    pub fn with_linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }

    pub fn with_max_batch_count(mut self, max: usize) -> Self {
        self.max_batch_count = max;
        self
    }

    pub fn with_channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap;
        self
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum MqBatcherError {
    #[error("batcher channel closed")]
    ChannelClosed,
    #[error("response channel dropped")]
    ResponseDropped,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

struct BatchedRequest {
    command: MqCommand,
    response_tx: oneshot::Sender<MqResponse>,
}

/// Tracks how to distribute a merged response back to original callers.
enum ResponseSlot {
    /// Single original caller — forward response directly.
    Single(oneshot::Sender<MqResponse>),
    /// Merged Publish callers — split `Published` by message count per caller.
    MergedPublish(Vec<(oneshot::Sender<MqResponse>, usize)>),
}

// ---------------------------------------------------------------------------
// MqWriteBatcher
// ---------------------------------------------------------------------------

/// Coalesces individual `MqCommand` submissions into batched Raft proposals.
///
/// Thread-safe and designed to be shared via `Arc`.
pub struct MqWriteBatcher {
    tx: crossfire::MAsyncTx<crossfire::mpsc::Array<BatchedRequest>>,
    task: parking_lot::Mutex<Option<JoinHandle<()>>>,
    // Pre-initialized metrics handles.
    m_flush_count: metrics::Counter,
    m_flush_linger: metrics::Counter,
    m_commands_batched: metrics::Histogram,
}

impl MqWriteBatcher {
    /// Create a new `MqWriteBatcher` and spawn the batcher loop.
    pub fn new(
        config: MqWriteBatcherConfig,
        raft: Raft<MqTypeConfig>,
        group_id: u64,
        catalog_name: &str,
    ) -> Self {
        let (tx, rx) = crossfire::mpsc::bounded_async::<BatchedRequest>(config.channel_capacity);

        let catalog = catalog_name.to_owned();
        let group_label = group_id.to_string();
        let m_flush_count = metrics::counter!(
            "mq.batcher.flushes",
            "catalog" => catalog.clone(),
            "group" => group_label.clone(),
            "reason" => "count"
        );
        let m_flush_linger = metrics::counter!(
            "mq.batcher.flushes",
            "catalog" => catalog.clone(),
            "group" => group_label.clone(),
            "reason" => "linger"
        );
        let m_commands_batched = metrics::histogram!(
            "mq.batcher.commands_per_flush",
            "catalog" => catalog,
            "group" => group_label
        );

        let task = tokio::spawn(batcher_loop(
            rx,
            raft,
            config,
            m_flush_count.clone(),
            m_flush_linger.clone(),
            m_commands_batched.clone(),
        ));

        Self {
            tx,
            task: parking_lot::Mutex::new(Some(task)),
            m_flush_count,
            m_flush_linger,
            m_commands_batched,
        }
    }

    /// Create a test batcher that routes commands through the given engine.
    ///
    /// Each submitted command is applied directly (no batching, no Raft).
    /// Engine uses interior mutability (papaya + atomics), no Mutex needed.
    #[cfg(any(test, feature = "test-util"))]
    pub fn new_test(engine: std::sync::Arc<crate::engine::MqEngine>) -> Self {
        let (tx, rx) = crossfire::mpsc::bounded_async::<BatchedRequest>(1024);
        let task = tokio::spawn(async move {
            let mut log_index = 1u64;
            while let Ok(req) = rx.recv().await {
                let resp = engine.apply_command(&req.command, log_index, log_index * 1000, None);
                log_index += 1;
                let _ = req.response_tx.send(resp);
            }
        });
        Self {
            tx,
            task: parking_lot::Mutex::new(Some(task)),
            m_flush_count: metrics::counter!("test.flush_count"),
            m_flush_linger: metrics::counter!("test.flush_linger"),
            m_commands_batched: metrics::histogram!("test.commands_batched"),
        }
    }

    /// Submit a single command. Blocks (async) until the coalesced Raft
    /// proposal completes and the response is available.
    pub async fn submit(&self, command: MqCommand) -> Result<MqResponse, MqBatcherError> {
        let (response_tx, response_rx) = oneshot::channel();

        let request = BatchedRequest {
            command,
            response_tx,
        };

        self.tx
            .send(request)
            .await
            .map_err(|_| MqBatcherError::ChannelClosed)?;

        response_rx
            .await
            .map_err(|_| MqBatcherError::ResponseDropped)
    }

    /// Shut down the batcher. Drops the sender so the loop exits, then
    /// awaits task completion.
    pub async fn shutdown(self) {
        drop(self.tx);
        if let Some(task) = self.task.lock().take() {
            let _ = task.await;
        }
    }
}

// ---------------------------------------------------------------------------
// Batcher loop
// ---------------------------------------------------------------------------

/// Merge same-topic `Publish` commands in `pending` to reduce per-command overhead.
///
/// Returns `(merged_commands, response_slots)` where each slot knows how to
/// distribute the raft response back to original callers.
///
/// `publish_idx` is a caller-owned scratch map, reused across flushes to avoid
/// per-flush HashMap allocation.
fn merge_pending(
    pending: &mut Vec<BatchedRequest>,
    publish_idx: &mut HashMap<u64, usize>,
) -> (Vec<MqCommand>, Vec<ResponseSlot>) {
    let mut commands: Vec<MqCommand> = Vec::with_capacity(pending.len());
    let mut slots = Vec::with_capacity(pending.len());

    publish_idx.clear();

    for mut req in pending.drain(..) {
        match req.command.tag() {
            MqCommand::TAG_PUBLISH => {
                let topic_id = req.command.as_publish().topic_id();
                let msg_count = req.command.as_publish().message_count() as usize;
                if let Some(&idx) = publish_idx.get(&topic_id) {
                    // Merge: combine payload segments from both commands
                    // into a single scatter publish (zero-copy).
                    let mut all_msgs = commands[idx].take_publish_segments();
                    all_msgs.extend(req.command.take_publish_segments());
                    commands[idx] = MqCommand::publish_scatter(topic_id, all_msgs);
                    if let ResponseSlot::MergedPublish(ref mut callers) = slots[idx] {
                        callers.push((req.response_tx, msg_count));
                    }
                } else {
                    let idx = commands.len();
                    publish_idx.insert(topic_id, idx);
                    commands.push(req.command);
                    slots.push(ResponseSlot::MergedPublish(vec![(
                        req.response_tx,
                        msg_count,
                    )]));
                }
            }
            _ => {
                commands.push(req.command);
                slots.push(ResponseSlot::Single(req.response_tx));
            }
        }
    }

    (commands, slots)
}

/// Distribute a response to its original caller(s) via the slot.
fn dispatch_response(slot: ResponseSlot, response: MqResponse) {
    match slot {
        ResponseSlot::Single(tx) => {
            let _ = tx.send(response);
        }
        ResponseSlot::MergedPublish(callers) => {
            if callers.len() == 1 {
                let _ = callers.into_iter().next().unwrap().0.send(response);
                return;
            }
            match response {
                MqResponse::Published {
                    base_offset,
                    count: _,
                } => {
                    let mut consumed = 0u64;
                    for (tx, caller_count) in callers {
                        let _ = tx.send(MqResponse::Published {
                            base_offset: base_offset + consumed,
                            count: caller_count as u64,
                        });
                        consumed += caller_count as u64;
                    }
                }
                MqResponse::Error(_) => {
                    for (tx, _) in callers {
                        let _ = tx.send(response.clone());
                    }
                }
                other => {
                    for (tx, _) in callers {
                        let _ = tx.send(other.clone());
                    }
                }
            }
        }
    }
}

async fn batcher_loop(
    rx: crossfire::AsyncRx<crossfire::mpsc::Array<BatchedRequest>>,
    raft: Raft<MqTypeConfig>,
    config: MqWriteBatcherConfig,
    m_flush_count: metrics::Counter,
    m_flush_linger: metrics::Counter,
    m_commands_batched: metrics::Histogram,
) {
    let mut pending: Vec<BatchedRequest> = Vec::with_capacity(config.max_batch_count);
    // Reusable scratch map for merge_pending — avoids per-flush HashMap allocation.
    let mut publish_idx: HashMap<u64, usize> = HashMap::new();

    loop {
        // Step 1: Block until the first request arrives.
        let first = match rx.recv().await {
            Ok(req) => req,
            Err(_) => {
                debug!("mq batcher_loop: channel closed, exiting");
                return;
            }
        };

        pending.clear();
        pending.push(first);

        // Step 2: If below threshold, wait linger then drain.
        if pending.len() < config.max_batch_count {
            tokio::time::sleep(config.linger).await;

            loop {
                match rx.try_recv() {
                    Ok(req) => {
                        pending.push(req);
                        if pending.len() >= config.max_batch_count {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        let num_commands = pending.len();
        let flushed_by_count = num_commands >= config.max_batch_count;

        // Step 3: Merge same-topic Publishes.
        let (commands, slots) = merge_pending(&mut pending, &mut publish_idx);

        // Step 4: Build command and propose through Raft.
        if commands.len() == 1 {
            // Single-command fast path: no Batch wrapper.
            let cmd = commands.into_iter().next().unwrap();
            let slot = slots.into_iter().next().unwrap();
            match raft.client_write(cmd).await {
                Ok(resp) => {
                    dispatch_response(slot, resp.response().clone());
                }
                Err(e) => {
                    warn!("mq batcher: raft error: {}", e);
                    dispatch_response(
                        slot,
                        MqResponse::Error(MqError::Custom(format!("raft error: {}", e))),
                    );
                }
            }
        } else {
            let batch_cmd = MqCommand::batch(&commands);

            match raft.client_write(batch_cmd).await {
                Ok(resp) => {
                    let response = resp.response().clone();
                    match response {
                        MqResponse::BatchResponse(responses) => {
                            for (slot, individual_response) in
                                slots.into_iter().zip(responses.into_iter())
                            {
                                dispatch_response(slot, individual_response);
                            }
                        }
                        other => {
                            for slot in slots {
                                dispatch_response(slot, other.clone());
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("mq batcher: raft batch error: {}", e);
                    let error_resp =
                        MqResponse::Error(MqError::Custom(format!("raft error: {}", e)));
                    for slot in slots {
                        dispatch_response(slot, error_resp.clone());
                    }
                }
            }
        }

        if flushed_by_count {
            m_flush_count.increment(1);
        } else {
            m_flush_linger.increment(1);
        }
        m_commands_batched.record(num_commands as f64);

        debug!(commands = num_commands, "mq batcher_loop: flushed batch");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = MqWriteBatcherConfig::default();
        assert_eq!(config.linger, Duration::from_millis(5));
        assert_eq!(config.max_batch_count, 256);
        assert_eq!(config.channel_capacity, 1024);
    }

    #[test]
    fn test_config_builder() {
        let config = MqWriteBatcherConfig::default()
            .with_linger(Duration::from_millis(10))
            .with_max_batch_count(512)
            .with_channel_capacity(2048);
        assert_eq!(config.linger, Duration::from_millis(10));
        assert_eq!(config.max_batch_count, 512);
        assert_eq!(config.channel_capacity, 2048);
    }

    #[test]
    fn test_error_display() {
        let e = MqBatcherError::ChannelClosed;
        assert_eq!(e.to_string(), "batcher channel closed");
        let e = MqBatcherError::ResponseDropped;
        assert_eq!(e.to_string(), "response channel dropped");
    }
}

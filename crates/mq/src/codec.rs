//! Flat binary codec for MqCommand and MqResponse.
//!
//! Hand-rolled binary encoding replaces serde/bincode for zero-copy reads
//! from mmap-backed raft log segments. Each type is encoded as:
//!
//! ```text
//! [tag: u8][fields...]
//! ```
//!
//! Variable-length fields use length-prefixed encoding:
//! - `String`:       [len: u32 LE][utf8 bytes]
//! - `Bytes`:        [len: u32 LE][raw bytes]
//! - `Vec<T>`:       [count: u32 LE][T₀][T₁]...
//! - `Option<T>`:    [0u8] if None, [1u8][T] if Some

use std::io::{Read, Write};

use bisque_raft::codec::{BorrowPayload, CodecError, Decode, Encode};
use bytes::Bytes;
use smallvec::SmallVec;

use crate::config::{ActorConfig, JobConfig, QueueConfig};
use crate::types::*;

// =============================================================================
// MqCommand tag aliases (delegate to MqCommand::TAG_*)
// =============================================================================

pub(crate) const TAG_PUBLISH: u8 = MqCommand::TAG_PUBLISH;
pub(crate) const TAG_PUBLISH_TO_EXCHANGE: u8 = MqCommand::TAG_PUBLISH_TO_EXCHANGE;

// =============================================================================
// MqResponse tag constants
// =============================================================================

const TAG_RESP_OK: u8 = 0;
const TAG_RESP_ERROR: u8 = 1;
const TAG_RESP_ENTITY_CREATED: u8 = 2;
const TAG_RESP_MESSAGES: u8 = 3;
const TAG_RESP_PUBLISHED: u8 = 4;
const TAG_RESP_STATS: u8 = 5;
const TAG_RESP_BATCH: u8 = 6;
const TAG_RESP_DEAD_LETTERED: u8 = 7;

// =============================================================================
// Bytes encoding helpers
// =============================================================================

#[inline]
fn encode_bytes<W: Write>(w: &mut W, b: &[u8]) -> Result<(), CodecError> {
    (b.len() as u32).encode(w)?;
    w.write_all(b)?;
    Ok(())
}

#[inline]
fn decode_bytes_owned<R: Read>(r: &mut R) -> Result<Bytes, CodecError> {
    let len = u32::decode(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(Bytes::from(buf))
}

#[inline]
fn encode_opt_bytes<W: Write>(w: &mut W, v: &Option<Bytes>) -> Result<(), CodecError> {
    match v {
        None => 0u8.encode(w),
        Some(b) => {
            1u8.encode(w)?;
            encode_bytes(w, b)
        }
    }
}

#[inline]
fn decode_opt_bytes<R: Read>(r: &mut R) -> Result<Option<Bytes>, CodecError> {
    if u8::decode(r)? == 0 {
        Ok(None)
    } else {
        Ok(Some(decode_bytes_owned(r)?))
    }
}

#[inline]
fn encode_opt_bytes_ref<W: Write>(w: &mut W, v: Option<&Bytes>) -> Result<(), CodecError> {
    match v {
        None => 0u8.encode(w),
        Some(b) => {
            1u8.encode(w)?;
            encode_bytes(w, b)
        }
    }
}

#[inline]
fn encode_vec_bytes<W: Write>(w: &mut W, v: &[Bytes]) -> Result<(), CodecError> {
    (v.len() as u32).encode(w)?;
    for b in v {
        encode_bytes(w, b)?;
    }
    Ok(())
}

#[inline]
fn decode_vec_bytes<R: Read>(r: &mut R) -> Result<Vec<Bytes>, CodecError> {
    let count = u32::decode(r)? as usize;
    let mut v = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        v.push(decode_bytes_owned(r)?);
    }
    Ok(v)
}

#[inline]
fn encode_vec_u64<W: Write>(w: &mut W, v: &[u64]) -> Result<(), CodecError> {
    (v.len() as u32).encode(w)?;
    for &x in v {
        x.encode(w)?;
    }
    Ok(())
}

#[inline]
fn decode_vec_u64<R: Read>(r: &mut R) -> Result<Vec<u64>, CodecError> {
    let count = u32::decode(r)? as usize;
    let mut v = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        v.push(u64::decode(r)?);
    }
    Ok(v)
}

#[inline]
fn encode_vec_opt_bytes<W: Write>(w: &mut W, v: &[Option<Bytes>]) -> Result<(), CodecError> {
    (v.len() as u32).encode(w)?;
    for opt in v {
        encode_opt_bytes(w, opt)?;
    }
    Ok(())
}

#[inline]
fn decode_vec_opt_bytes<R: Read>(r: &mut R) -> Result<Vec<Option<Bytes>>, CodecError> {
    let count = u32::decode(r)? as usize;
    let mut v = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        v.push(decode_opt_bytes(r)?);
    }
    Ok(v)
}

#[inline]
fn encode_opt_string<W: Write>(w: &mut W, v: &Option<String>) -> Result<(), CodecError> {
    match v {
        None => 0u8.encode(w),
        Some(s) => {
            1u8.encode(w)?;
            s.encode(w)
        }
    }
}

#[inline]
fn encode_opt_str<W: Write>(w: &mut W, v: Option<&str>) -> Result<(), CodecError> {
    match v {
        None => 0u8.encode(w),
        Some(s) => {
            1u8.encode(w)?;
            s.to_string().encode(w)
        }
    }
}

#[inline]
fn decode_opt_string<R: Read>(r: &mut R) -> Result<Option<String>, CodecError> {
    if u8::decode(r)? == 0 {
        Ok(None)
    } else {
        Ok(Some(String::decode(r)?))
    }
}

// =============================================================================
// Sub-type codecs
// =============================================================================

// -- EntityType --

impl Encode for EntityType {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        let tag: u8 = match self {
            EntityType::Topic => 0,
            EntityType::Queue => 1,
            EntityType::ActorNamespace => 2,
            EntityType::Job => 3,
            EntityType::Exchange => 4,
        };
        tag.encode(w)
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for EntityType {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(EntityType::Topic),
            1 => Ok(EntityType::Queue),
            2 => Ok(EntityType::ActorNamespace),
            3 => Ok(EntityType::Job),
            4 => Ok(EntityType::Exchange),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- ExchangeType --

impl Encode for ExchangeType {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        let tag: u8 = match self {
            ExchangeType::Fanout => 0,
            ExchangeType::Direct => 1,
            ExchangeType::Topic => 2,
        };
        tag.encode(w)
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for ExchangeType {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(ExchangeType::Fanout),
            1 => Ok(ExchangeType::Direct),
            2 => Ok(ExchangeType::Topic),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- OverlapPolicy --

impl Encode for OverlapPolicy {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        let tag: u8 = match self {
            OverlapPolicy::Skip => 0,
            OverlapPolicy::Queue => 1,
        };
        tag.encode(w)
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for OverlapPolicy {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(OverlapPolicy::Skip),
            1 => Ok(OverlapPolicy::Queue),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- RetentionPolicy --

impl Encode for RetentionPolicy {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.max_age_secs.encode(w)?;
        self.max_bytes.encode(w)?;
        self.max_messages.encode(w)
    }
    fn encoded_size(&self) -> usize {
        self.max_age_secs.encoded_size()
            + self.max_bytes.encoded_size()
            + self.max_messages.encoded_size()
    }
}

impl Decode for RetentionPolicy {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            max_age_secs: Option::decode(r)?,
            max_bytes: Option::decode(r)?,
            max_messages: Option::decode(r)?,
        })
    }
}

// -- Subscription --

impl Encode for Subscription {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.entity_type.encode(w)?;
        self.entity_id.encode(w)
    }
    fn encoded_size(&self) -> usize {
        1 + 8
    }
}

impl Decode for Subscription {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            entity_type: EntityType::decode(r)?,
            entity_id: u64::decode(r)?,
        })
    }
}

// -- RetryConfig --

impl Encode for RetryConfig {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.max_retries.encode(w)?;
        self.retry_delay_ms.encode(w)
    }
    fn encoded_size(&self) -> usize {
        4 + 8
    }
}

impl Decode for RetryConfig {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            max_retries: u32::decode(r)?,
            retry_delay_ms: u64::decode(r)?,
        })
    }
}

// -- InputSource --

impl Encode for InputSource {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.entity_type.encode(w)?;
        self.entity_id.encode(w)
    }
    fn encoded_size(&self) -> usize {
        1 + 8
    }
}

impl Decode for InputSource {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            entity_type: EntityType::decode(r)?,
            entity_id: u64::decode(r)?,
        })
    }
}

// -- QueueConfig --

impl Encode for QueueConfig {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.visibility_timeout_ms.encode(w)?;
        self.max_retries.encode(w)?;
        self.dead_letter_topic_id.encode(w)?;
        self.dedup_window_secs.encode(w)?;
        self.delay_default_ms.encode(w)?;
        self.max_in_flight_per_consumer.encode(w)
    }
    fn encoded_size(&self) -> usize {
        8 + 4
            + self.dead_letter_topic_id.encoded_size()
            + self.dedup_window_secs.encoded_size()
            + 8
            + 4
    }
}

impl Decode for QueueConfig {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            visibility_timeout_ms: u64::decode(r)?,
            max_retries: u32::decode(r)?,
            dead_letter_topic_id: Option::decode(r)?,
            dedup_window_secs: Option::decode(r)?,
            delay_default_ms: u64::decode(r)?,
            max_in_flight_per_consumer: u32::decode(r)?,
        })
    }
}

// -- ActorConfig --

impl Encode for ActorConfig {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.max_mailbox_depth.encode(w)?;
        self.idle_eviction_secs.encode(w)?;
        self.ack_timeout_ms.encode(w)?;
        self.max_retries.encode(w)
    }
    fn encoded_size(&self) -> usize {
        4 + 8 + 8 + 4
    }
}

impl Decode for ActorConfig {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            max_mailbox_depth: u32::decode(r)?,
            idle_eviction_secs: u64::decode(r)?,
            ack_timeout_ms: u64::decode(r)?,
            max_retries: u32::decode(r)?,
        })
    }
}

// -- JobConfig --

impl Encode for JobConfig {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.cron_expression.encode(w)?;
        self.timezone.encode(w)?;
        self.execution_timeout_ms.encode(w)?;
        self.overlap_policy.encode(w)?;
        self.max_queued.encode(w)?;
        self.input_source.encode(w)?;
        self.retry_config.encode(w)
    }
    fn encoded_size(&self) -> usize {
        self.cron_expression.encoded_size()
            + self.timezone.encoded_size()
            + 8
            + 1
            + 4
            + self.input_source.encoded_size()
            + self.retry_config.encoded_size()
    }
}

impl Decode for JobConfig {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            cron_expression: String::decode(r)?,
            timezone: String::decode(r)?,
            execution_timeout_ms: u64::decode(r)?,
            overlap_policy: OverlapPolicy::decode(r)?,
            max_queued: u32::decode(r)?,
            input_source: Option::decode(r)?,
            retry_config: RetryConfig::decode(r)?,
        })
    }
}

// -- EntityKind --

impl Encode for EntityKind {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        let tag: u8 = match self {
            EntityKind::Topic => 0,
            EntityKind::Queue => 1,
            EntityKind::ActorNamespace => 2,
            EntityKind::Job => 3,
            EntityKind::Consumer => 4,
            EntityKind::Exchange => 5,
            EntityKind::Binding => 6,
        };
        tag.encode(w)
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for EntityKind {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(EntityKind::Topic),
            1 => Ok(EntityKind::Queue),
            2 => Ok(EntityKind::ActorNamespace),
            3 => Ok(EntityKind::Job),
            4 => Ok(EntityKind::Consumer),
            5 => Ok(EntityKind::Exchange),
            6 => Ok(EntityKind::Binding),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- MqError --

impl Encode for MqError {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        match self {
            MqError::NotFound { entity, id } => {
                0u8.encode(w)?;
                entity.encode(w)?;
                id.encode(w)
            }
            MqError::AlreadyExists { entity, id } => {
                1u8.encode(w)?;
                entity.encode(w)?;
                id.encode(w)
            }
            MqError::MailboxFull { pending } => {
                2u8.encode(w)?;
                pending.encode(w)
            }
            MqError::Custom(msg) => {
                3u8.encode(w)?;
                msg.encode(w)
            }
        }
    }
    fn encoded_size(&self) -> usize {
        1 + match self {
            MqError::NotFound { .. } | MqError::AlreadyExists { .. } => 1 + 8,
            MqError::MailboxFull { .. } => 4,
            MqError::Custom(msg) => msg.encoded_size(),
        }
    }
}

impl Decode for MqError {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(MqError::NotFound {
                entity: EntityKind::decode(r)?,
                id: u64::decode(r)?,
            }),
            1 => Ok(MqError::AlreadyExists {
                entity: EntityKind::decode(r)?,
                id: u64::decode(r)?,
            }),
            2 => Ok(MqError::MailboxFull {
                pending: u32::decode(r)?,
            }),
            3 => Ok(MqError::Custom(String::decode(r)?)),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- DeliveredMessage --

impl Encode for DeliveredMessage {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.message_id.encode(w)?;
        self.attempt.encode(w)?;
        self.original_timestamp.encode(w)
    }
    fn encoded_size(&self) -> usize {
        8 + 4 + 8
    }
}

impl Decode for DeliveredMessage {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            message_id: u64::decode(r)?,
            attempt: u32::decode(r)?,
            original_timestamp: u64::decode(r)?,
        })
    }
}

// -- JobState --

impl Encode for JobState {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.assigned_consumer_id.encode(w)?;
        self.last_triggered_at.encode(w)?;
        self.last_completed_at.encode(w)?;
        self.next_trigger_at.encode(w)?;
        self.current_execution_id.encode(w)?;
        self.consecutive_failures.encode(w)?;
        self.queued_triggers.encode(w)
    }
    fn encoded_size(&self) -> usize {
        self.assigned_consumer_id.encoded_size()
            + self.last_triggered_at.encoded_size()
            + self.last_completed_at.encoded_size()
            + 8
            + self.current_execution_id.encoded_size()
            + 4
            + 4
    }
}

impl Decode for JobState {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            assigned_consumer_id: Option::decode(r)?,
            last_triggered_at: Option::decode(r)?,
            last_completed_at: Option::decode(r)?,
            next_trigger_at: u64::decode(r)?,
            current_execution_id: Option::decode(r)?,
            consecutive_failures: u32::decode(r)?,
            queued_triggers: u32::decode(r)?,
        })
    }
}

// -- EntityStats --

impl Encode for EntityStats {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        match self {
            EntityStats::Topic {
                topic_id,
                message_count,
                head_index,
                tail_index,
            } => {
                0u8.encode(w)?;
                topic_id.encode(w)?;
                message_count.encode(w)?;
                head_index.encode(w)?;
                tail_index.encode(w)
            }
            EntityStats::Queue {
                queue_id,
                pending_count,
                in_flight_count,
                dlq_count,
            } => {
                1u8.encode(w)?;
                queue_id.encode(w)?;
                pending_count.encode(w)?;
                in_flight_count.encode(w)?;
                dlq_count.encode(w)
            }
            EntityStats::ActorNamespace {
                namespace_id,
                active_actor_count,
            } => {
                2u8.encode(w)?;
                namespace_id.encode(w)?;
                active_actor_count.encode(w)
            }
            EntityStats::Job { job_id, state } => {
                3u8.encode(w)?;
                job_id.encode(w)?;
                state.encode(w)
            }
        }
    }
    fn encoded_size(&self) -> usize {
        1 + match self {
            EntityStats::Topic { .. } => 4 * 8,
            EntityStats::Queue { .. } => 4 * 8,
            EntityStats::ActorNamespace { .. } => 2 * 8,
            EntityStats::Job { state, .. } => 8 + state.encoded_size(),
        }
    }
}

impl Decode for EntityStats {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(EntityStats::Topic {
                topic_id: u64::decode(r)?,
                message_count: u64::decode(r)?,
                head_index: u64::decode(r)?,
                tail_index: u64::decode(r)?,
            }),
            1 => Ok(EntityStats::Queue {
                queue_id: u64::decode(r)?,
                pending_count: u64::decode(r)?,
                in_flight_count: u64::decode(r)?,
                dlq_count: u64::decode(r)?,
            }),
            2 => Ok(EntityStats::ActorNamespace {
                namespace_id: u64::decode(r)?,
                active_actor_count: u64::decode(r)?,
            }),
            3 => Ok(EntityStats::Job {
                job_id: u64::decode(r)?,
                state: JobState::decode(r)?,
            }),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// =============================================================================
// MqCommand — Encode (passthrough, buffer already contains encoded bytes)
// =============================================================================

impl Encode for MqCommand {
    #[inline]
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        w.write_all(&self.buf)?;
        Ok(())
    }

    #[inline]
    fn encoded_size(&self) -> usize {
        self.buf.len()
    }
}

// =============================================================================
// MqCommand — Decode (zero-copy wrap)
// =============================================================================

impl Decode for MqCommand {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        Ok(MqCommand {
            buf: Bytes::from(buf),
        })
    }

    fn decode_from_bytes(data: Bytes) -> Result<Self, CodecError> {
        Ok(MqCommand { buf: data })
    }
}

impl BorrowPayload for MqCommand {
    fn payload_bytes(&self) -> &[u8] {
        &self.buf
    }
}

// =============================================================================
// MqCommand — Constructor methods
// =============================================================================

/// Helper: encode fields into a Vec<u8> and wrap as MqCommand.
macro_rules! build_cmd {
    ($tag:expr, $($encode:expr),* $(,)?) => {{
        let mut buf = Vec::new();
        $tag.encode(&mut buf).unwrap();
        $( $encode(&mut buf).unwrap(); )*
        MqCommand { buf: Bytes::from(buf) }
    }};
}

impl MqCommand {
    // -- Topics --

    pub fn create_topic(name: &str, retention: RetentionPolicy, partition_count: u32) -> Self {
        build_cmd!(
            Self::TAG_CREATE_TOPIC,
            |w: &mut Vec<u8>| name.to_string().encode(w),
            |w: &mut Vec<u8>| retention.encode(w),
            |w: &mut Vec<u8>| partition_count.encode(w)
        )
    }

    pub fn delete_topic(topic_id: u64) -> Self {
        build_cmd!(Self::TAG_DELETE_TOPIC, |w: &mut Vec<u8>| topic_id.encode(w))
    }

    pub fn publish(topic_id: u64, messages: &[Bytes]) -> Self {
        build_cmd!(
            Self::TAG_PUBLISH,
            |w: &mut Vec<u8>| topic_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_bytes(w, messages)
        )
    }

    pub fn commit_offset(topic_id: u64, consumer_id: u64, offset: u64) -> Self {
        build_cmd!(
            Self::TAG_COMMIT_OFFSET,
            |w: &mut Vec<u8>| topic_id.encode(w),
            |w: &mut Vec<u8>| consumer_id.encode(w),
            |w: &mut Vec<u8>| offset.encode(w)
        )
    }

    pub fn purge_topic(topic_id: u64, before_index: u64) -> Self {
        build_cmd!(
            Self::TAG_PURGE_TOPIC,
            |w: &mut Vec<u8>| topic_id.encode(w),
            |w: &mut Vec<u8>| before_index.encode(w)
        )
    }

    // -- Queues --

    pub fn create_queue(name: &str, config: &QueueConfig) -> Self {
        build_cmd!(
            Self::TAG_CREATE_QUEUE,
            |w: &mut Vec<u8>| name.to_string().encode(w),
            |w: &mut Vec<u8>| config.encode(w)
        )
    }

    pub fn delete_queue(queue_id: u64) -> Self {
        build_cmd!(Self::TAG_DELETE_QUEUE, |w: &mut Vec<u8>| queue_id.encode(w))
    }

    pub fn enqueue(queue_id: u64, messages: &[Bytes], dedup_keys: &[Option<Bytes>]) -> Self {
        build_cmd!(
            Self::TAG_ENQUEUE,
            |w: &mut Vec<u8>| queue_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_bytes(w, messages),
            |w: &mut Vec<u8>| encode_vec_opt_bytes(w, dedup_keys)
        )
    }

    pub fn deliver(queue_id: u64, consumer_id: u64, max_count: u32) -> Self {
        build_cmd!(
            Self::TAG_DELIVER,
            |w: &mut Vec<u8>| queue_id.encode(w),
            |w: &mut Vec<u8>| consumer_id.encode(w),
            |w: &mut Vec<u8>| max_count.encode(w)
        )
    }

    pub fn ack(queue_id: u64, message_ids: &[u64], response: Option<&Bytes>) -> Self {
        build_cmd!(
            Self::TAG_ACK,
            |w: &mut Vec<u8>| queue_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, message_ids),
            |w: &mut Vec<u8>| encode_opt_bytes_ref(w, response)
        )
    }

    pub fn nack(queue_id: u64, message_ids: &[u64]) -> Self {
        build_cmd!(
            Self::TAG_NACK,
            |w: &mut Vec<u8>| queue_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, message_ids)
        )
    }

    pub fn extend_visibility(queue_id: u64, message_ids: &[u64], extension_ms: u64) -> Self {
        build_cmd!(
            Self::TAG_EXTEND_VISIBILITY,
            |w: &mut Vec<u8>| queue_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, message_ids),
            |w: &mut Vec<u8>| extension_ms.encode(w)
        )
    }

    pub fn timeout_expired(queue_id: u64, message_ids: &[u64]) -> Self {
        build_cmd!(
            Self::TAG_TIMEOUT_EXPIRED,
            |w: &mut Vec<u8>| queue_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, message_ids)
        )
    }

    pub fn publish_to_dlq(
        source_queue_id: u64,
        dlq_topic_id: u64,
        dead_letter_ids: &[u64],
        messages: &[Bytes],
    ) -> Self {
        build_cmd!(
            Self::TAG_PUBLISH_TO_DLQ,
            |w: &mut Vec<u8>| source_queue_id.encode(w),
            |w: &mut Vec<u8>| dlq_topic_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, dead_letter_ids),
            |w: &mut Vec<u8>| encode_vec_bytes(w, messages)
        )
    }

    pub fn prune_dedup_window(queue_id: u64, before_timestamp: u64) -> Self {
        build_cmd!(
            Self::TAG_PRUNE_DEDUP_WINDOW,
            |w: &mut Vec<u8>| queue_id.encode(w),
            |w: &mut Vec<u8>| before_timestamp.encode(w)
        )
    }

    pub fn expire_pending_messages(queue_id: u64, message_ids: &[u64]) -> Self {
        build_cmd!(
            Self::TAG_EXPIRE_PENDING_MESSAGES,
            |w: &mut Vec<u8>| queue_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, message_ids)
        )
    }

    pub fn purge_queue(queue_id: u64) -> Self {
        build_cmd!(Self::TAG_PURGE_QUEUE, |w: &mut Vec<u8>| queue_id.encode(w))
    }

    pub fn get_queue_attributes(queue_id: u64) -> Self {
        build_cmd!(Self::TAG_GET_QUEUE_ATTRIBUTES, |w: &mut Vec<u8>| queue_id
            .encode(w))
    }

    // -- Exchanges --

    pub fn create_exchange(name: &str, exchange_type: ExchangeType) -> Self {
        build_cmd!(
            Self::TAG_CREATE_EXCHANGE,
            |w: &mut Vec<u8>| name.to_string().encode(w),
            |w: &mut Vec<u8>| exchange_type.encode(w)
        )
    }

    pub fn delete_exchange(exchange_id: u64) -> Self {
        build_cmd!(Self::TAG_DELETE_EXCHANGE, |w: &mut Vec<u8>| exchange_id
            .encode(w))
    }

    pub fn create_binding(exchange_id: u64, queue_id: u64, routing_key: Option<&str>) -> Self {
        build_cmd!(
            Self::TAG_CREATE_BINDING,
            |w: &mut Vec<u8>| exchange_id.encode(w),
            |w: &mut Vec<u8>| queue_id.encode(w),
            |w: &mut Vec<u8>| encode_opt_str(w, routing_key)
        )
    }

    pub fn delete_binding(binding_id: u64) -> Self {
        build_cmd!(Self::TAG_DELETE_BINDING, |w: &mut Vec<u8>| binding_id
            .encode(w))
    }

    pub fn publish_to_exchange(exchange_id: u64, messages: &[Bytes]) -> Self {
        build_cmd!(
            Self::TAG_PUBLISH_TO_EXCHANGE,
            |w: &mut Vec<u8>| exchange_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_bytes(w, messages)
        )
    }

    // -- Actors --

    pub fn create_actor_namespace(name: &str, config: &ActorConfig) -> Self {
        build_cmd!(
            Self::TAG_CREATE_ACTOR_NAMESPACE,
            |w: &mut Vec<u8>| name.to_string().encode(w),
            |w: &mut Vec<u8>| config.encode(w)
        )
    }

    pub fn delete_actor_namespace(namespace_id: u64) -> Self {
        build_cmd!(Self::TAG_DELETE_ACTOR_NAMESPACE, |w: &mut Vec<u8>| {
            namespace_id.encode(w)
        })
    }

    pub fn send_to_actor(namespace_id: u64, actor_id: &[u8], message: &[u8]) -> Self {
        build_cmd!(
            Self::TAG_SEND_TO_ACTOR,
            |w: &mut Vec<u8>| namespace_id.encode(w),
            |w: &mut Vec<u8>| encode_bytes(w, actor_id),
            |w: &mut Vec<u8>| encode_bytes(w, message)
        )
    }

    pub fn deliver_actor_message(namespace_id: u64, actor_id: &[u8], consumer_id: u64) -> Self {
        build_cmd!(
            Self::TAG_DELIVER_ACTOR_MESSAGE,
            |w: &mut Vec<u8>| namespace_id.encode(w),
            |w: &mut Vec<u8>| encode_bytes(w, actor_id),
            |w: &mut Vec<u8>| consumer_id.encode(w)
        )
    }

    pub fn ack_actor_message(
        namespace_id: u64,
        actor_id: &[u8],
        message_id: u64,
        response: Option<&Bytes>,
    ) -> Self {
        build_cmd!(
            Self::TAG_ACK_ACTOR_MESSAGE,
            |w: &mut Vec<u8>| namespace_id.encode(w),
            |w: &mut Vec<u8>| encode_bytes(w, actor_id),
            |w: &mut Vec<u8>| message_id.encode(w),
            |w: &mut Vec<u8>| encode_opt_bytes_ref(w, response)
        )
    }

    pub fn nack_actor_message(namespace_id: u64, actor_id: &[u8], message_id: u64) -> Self {
        build_cmd!(
            Self::TAG_NACK_ACTOR_MESSAGE,
            |w: &mut Vec<u8>| namespace_id.encode(w),
            |w: &mut Vec<u8>| encode_bytes(w, actor_id),
            |w: &mut Vec<u8>| message_id.encode(w)
        )
    }

    pub fn assign_actors(namespace_id: u64, consumer_id: u64, actor_ids: &[Bytes]) -> Self {
        build_cmd!(
            Self::TAG_ASSIGN_ACTORS,
            |w: &mut Vec<u8>| namespace_id.encode(w),
            |w: &mut Vec<u8>| consumer_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_bytes(w, actor_ids)
        )
    }

    pub fn release_actors(namespace_id: u64, consumer_id: u64) -> Self {
        build_cmd!(
            Self::TAG_RELEASE_ACTORS,
            |w: &mut Vec<u8>| namespace_id.encode(w),
            |w: &mut Vec<u8>| consumer_id.encode(w)
        )
    }

    pub fn evict_idle_actors(namespace_id: u64, before_timestamp: u64) -> Self {
        build_cmd!(
            Self::TAG_EVICT_IDLE_ACTORS,
            |w: &mut Vec<u8>| namespace_id.encode(w),
            |w: &mut Vec<u8>| before_timestamp.encode(w)
        )
    }

    // -- Jobs --

    pub fn create_job(name: &str, config: &JobConfig) -> Self {
        build_cmd!(
            Self::TAG_CREATE_JOB,
            |w: &mut Vec<u8>| name.to_string().encode(w),
            |w: &mut Vec<u8>| config.encode(w)
        )
    }

    pub fn delete_job(job_id: u64) -> Self {
        build_cmd!(Self::TAG_DELETE_JOB, |w: &mut Vec<u8>| job_id.encode(w))
    }

    pub fn update_job(job_id: u64, config: &JobConfig) -> Self {
        build_cmd!(
            Self::TAG_UPDATE_JOB,
            |w: &mut Vec<u8>| job_id.encode(w),
            |w: &mut Vec<u8>| config.encode(w)
        )
    }

    pub fn enable_job(job_id: u64) -> Self {
        build_cmd!(Self::TAG_ENABLE_JOB, |w: &mut Vec<u8>| job_id.encode(w))
    }

    pub fn disable_job(job_id: u64) -> Self {
        build_cmd!(Self::TAG_DISABLE_JOB, |w: &mut Vec<u8>| job_id.encode(w))
    }

    pub fn trigger_job(job_id: u64, execution_id: u64, triggered_at: u64) -> Self {
        build_cmd!(
            Self::TAG_TRIGGER_JOB,
            |w: &mut Vec<u8>| job_id.encode(w),
            |w: &mut Vec<u8>| execution_id.encode(w),
            |w: &mut Vec<u8>| triggered_at.encode(w)
        )
    }

    pub fn assign_job(job_id: u64, consumer_id: u64) -> Self {
        build_cmd!(
            Self::TAG_ASSIGN_JOB,
            |w: &mut Vec<u8>| job_id.encode(w),
            |w: &mut Vec<u8>| consumer_id.encode(w)
        )
    }

    pub fn complete_job(job_id: u64, execution_id: u64) -> Self {
        build_cmd!(
            Self::TAG_COMPLETE_JOB,
            |w: &mut Vec<u8>| job_id.encode(w),
            |w: &mut Vec<u8>| execution_id.encode(w)
        )
    }

    pub fn fail_job(job_id: u64, execution_id: u64, error: &str) -> Self {
        build_cmd!(
            Self::TAG_FAIL_JOB,
            |w: &mut Vec<u8>| job_id.encode(w),
            |w: &mut Vec<u8>| execution_id.encode(w),
            |w: &mut Vec<u8>| error.to_string().encode(w)
        )
    }

    pub fn timeout_job(job_id: u64, execution_id: u64) -> Self {
        build_cmd!(
            Self::TAG_TIMEOUT_JOB,
            |w: &mut Vec<u8>| job_id.encode(w),
            |w: &mut Vec<u8>| execution_id.encode(w)
        )
    }

    // -- Sessions --

    pub fn register_consumer(
        consumer_id: u64,
        group_name: &str,
        subscriptions: &[Subscription],
    ) -> Self {
        build_cmd!(
            Self::TAG_REGISTER_CONSUMER,
            |w: &mut Vec<u8>| consumer_id.encode(w),
            |w: &mut Vec<u8>| group_name.to_string().encode(w),
            |w: &mut Vec<u8>| subscriptions.to_vec().encode(w)
        )
    }

    pub fn disconnect_consumer(consumer_id: u64) -> Self {
        build_cmd!(Self::TAG_DISCONNECT_CONSUMER, |w: &mut Vec<u8>| consumer_id
            .encode(w))
    }

    pub fn heartbeat(consumer_id: u64) -> Self {
        build_cmd!(Self::TAG_HEARTBEAT, |w: &mut Vec<u8>| consumer_id.encode(w))
    }

    pub fn register_producer(producer_id: u64, name: Option<&str>) -> Self {
        build_cmd!(
            Self::TAG_REGISTER_PRODUCER,
            |w: &mut Vec<u8>| producer_id.encode(w),
            |w: &mut Vec<u8>| encode_opt_str(w, name)
        )
    }

    pub fn disconnect_producer(producer_id: u64) -> Self {
        build_cmd!(Self::TAG_DISCONNECT_PRODUCER, |w: &mut Vec<u8>| producer_id
            .encode(w))
    }

    // -- Batch (length-prefixed sub-commands) --

    pub fn batch(commands: &[MqCommand]) -> Self {
        let mut buf = Vec::new();
        Self::TAG_BATCH.encode(&mut buf).unwrap();
        (commands.len() as u32).encode(&mut buf).unwrap();
        for cmd in commands {
            // Length-prefixed: [len:4][cmd_bytes]
            (cmd.buf.len() as u32).encode(&mut buf).unwrap();
            buf.extend_from_slice(&cmd.buf);
        }
        MqCommand {
            buf: Bytes::from(buf),
        }
    }
}

// =============================================================================
// MqCommand — View accessor methods
// =============================================================================

impl MqCommand {
    pub fn as_create_topic(&self) -> CmdCreateTopic {
        CmdCreateTopic {
            buf: self.buf.clone(),
        }
    }

    pub fn as_publish(&self) -> CmdPublish {
        CmdPublish {
            buf: self.buf.clone(),
        }
    }

    pub fn as_create_queue(&self) -> CmdCreateQueue {
        CmdCreateQueue {
            buf: self.buf.clone(),
        }
    }

    pub fn as_enqueue(&self) -> CmdEnqueue {
        CmdEnqueue {
            buf: self.buf.clone(),
        }
    }

    pub fn as_ack(&self) -> CmdAck {
        CmdAck {
            buf: self.buf.clone(),
        }
    }

    pub fn as_nack(&self) -> CmdNack {
        CmdNack {
            buf: self.buf.clone(),
        }
    }

    pub fn as_extend_visibility(&self) -> CmdExtendVisibility {
        CmdExtendVisibility {
            buf: self.buf.clone(),
        }
    }

    pub fn as_timeout_expired(&self) -> CmdTimeoutExpired {
        CmdTimeoutExpired {
            buf: self.buf.clone(),
        }
    }

    pub fn as_publish_to_dlq(&self) -> CmdPublishToDlq {
        CmdPublishToDlq {
            buf: self.buf.clone(),
        }
    }

    pub fn as_expire_pending_messages(&self) -> CmdExpirePendingMessages {
        CmdExpirePendingMessages {
            buf: self.buf.clone(),
        }
    }

    pub fn as_create_exchange(&self) -> CmdCreateExchange {
        CmdCreateExchange {
            buf: self.buf.clone(),
        }
    }

    pub fn as_create_binding(&self) -> CmdCreateBinding {
        CmdCreateBinding {
            buf: self.buf.clone(),
        }
    }

    pub fn as_publish_to_exchange(&self) -> CmdPublishToExchange {
        CmdPublishToExchange {
            buf: self.buf.clone(),
        }
    }

    pub fn as_create_actor_namespace(&self) -> CmdCreateActorNamespace {
        CmdCreateActorNamespace {
            buf: self.buf.clone(),
        }
    }

    pub fn as_send_to_actor(&self) -> CmdSendToActor {
        CmdSendToActor {
            buf: self.buf.clone(),
        }
    }

    pub fn as_deliver_actor_message(&self) -> CmdDeliverActorMessage {
        CmdDeliverActorMessage {
            buf: self.buf.clone(),
        }
    }

    pub fn as_ack_actor_message(&self) -> CmdAckActorMessage {
        CmdAckActorMessage {
            buf: self.buf.clone(),
        }
    }

    pub fn as_nack_actor_message(&self) -> CmdNackActorMessage {
        CmdNackActorMessage {
            buf: self.buf.clone(),
        }
    }

    pub fn as_assign_actors(&self) -> CmdAssignActors {
        CmdAssignActors {
            buf: self.buf.clone(),
        }
    }

    pub fn as_create_job(&self) -> CmdCreateJob {
        CmdCreateJob {
            buf: self.buf.clone(),
        }
    }

    pub fn as_update_job(&self) -> CmdUpdateJob {
        CmdUpdateJob {
            buf: self.buf.clone(),
        }
    }

    pub fn as_fail_job(&self) -> CmdFailJob {
        CmdFailJob {
            buf: self.buf.clone(),
        }
    }

    pub fn as_register_consumer(&self) -> CmdRegisterConsumer {
        CmdRegisterConsumer {
            buf: self.buf.clone(),
        }
    }

    pub fn as_register_producer(&self) -> CmdRegisterProducer {
        CmdRegisterProducer {
            buf: self.buf.clone(),
        }
    }

    pub fn as_batch(&self) -> CmdBatch {
        CmdBatch {
            buf: self.buf.clone(),
        }
    }

    // -- Message extraction helpers --

    /// For `Publish` or `PublishToExchange`: iterate messages zero-copy.
    /// Returns `None` for other command types.
    #[inline]
    pub fn publish_messages(&self) -> Option<FlatMessages> {
        let tag = self.tag();
        if (tag != Self::TAG_PUBLISH && tag != Self::TAG_PUBLISH_TO_EXCHANGE) || self.buf.len() < 13
        {
            return None;
        }
        let count = u32::from_le_bytes(self.buf[9..13].try_into().unwrap());
        Some(FlatMessages {
            buf: self.buf.clone(),
            offset: 13,
            remaining: count,
        })
    }

    /// If this is a `Publish` for the given `topic_id`, return a zero-copy
    /// message iterator. Returns `None` otherwise.
    #[inline]
    pub fn publish_messages_for_topic(&self, topic_id: u64) -> Option<FlatMessages> {
        if self.tag() != Self::TAG_PUBLISH || self.buf.len() < 13 {
            return None;
        }
        let tid = u64::from_le_bytes(self.buf[1..9].try_into().unwrap());
        if tid != topic_id {
            return None;
        }
        let count = u32::from_le_bytes(self.buf[9..13].try_into().unwrap());
        Some(FlatMessages {
            buf: self.buf.clone(),
            offset: 13,
            remaining: count,
        })
    }
}

// =============================================================================
// View structs — zero-copy accessors over MqCommand buffers
// =============================================================================

/// Zero-copy view over a CreateTopic command.
pub struct CmdCreateTopic {
    buf: Bytes,
}

impl CmdCreateTopic {
    pub fn name(&self) -> &str {
        let len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[5..5 + len]).unwrap_or("")
    }

    pub fn retention(&self) -> RetentionPolicy {
        let name_len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        let offset = 5 + name_len;
        let mut cursor = std::io::Cursor::new(&self.buf[offset..]);
        RetentionPolicy::decode(&mut cursor).unwrap_or_default()
    }

    pub fn partition_count(&self) -> u32 {
        let name_len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        let offset = 5 + name_len;
        let mut cursor = std::io::Cursor::new(&self.buf[offset..]);
        let _ = RetentionPolicy::decode(&mut cursor);
        let pos = cursor.position() as usize;
        u32::from_le_bytes(self.buf[offset + pos..offset + pos + 4].try_into().unwrap())
    }
}

/// Zero-copy view over a Publish command.
pub struct CmdPublish {
    buf: Bytes,
}

impl CmdPublish {
    pub fn topic_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn message_count(&self) -> u32 {
        u32::from_le_bytes(self.buf[9..13].try_into().unwrap())
    }

    pub fn messages(&self) -> FlatMessages {
        let count = self.message_count();
        FlatMessages {
            buf: self.buf.clone(),
            offset: 13,
            remaining: count,
        }
    }
}

/// Zero-copy view over a CreateQueue command.
pub struct CmdCreateQueue {
    buf: Bytes,
}

impl CmdCreateQueue {
    pub fn name(&self) -> &str {
        let len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[5..5 + len]).unwrap_or("")
    }

    pub fn config(&self) -> QueueConfig {
        let name_len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        let offset = 5 + name_len;
        let mut cursor = std::io::Cursor::new(&self.buf[offset..]);
        QueueConfig::decode(&mut cursor).unwrap_or_default()
    }
}

/// Zero-copy view over an Enqueue command.
pub struct CmdEnqueue {
    buf: Bytes,
}

impl CmdEnqueue {
    pub fn queue_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn message_count(&self) -> u32 {
        u32::from_le_bytes(self.buf[9..13].try_into().unwrap())
    }

    pub fn messages(&self) -> FlatMessages {
        let count = u32::from_le_bytes(self.buf[9..13].try_into().unwrap());
        FlatMessages {
            buf: self.buf.clone(),
            offset: 13,
            remaining: count,
        }
    }

    /// Skip past the messages to find where dedup_keys start.
    fn dedup_keys_offset(&self) -> usize {
        let msg_count = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        let mut offset = 13;
        for _ in 0..msg_count {
            let len = u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4 + len;
        }
        offset
    }

    pub fn dedup_keys(&self) -> FlatOptBytes {
        let offset = self.dedup_keys_offset();
        let count = u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap());
        FlatOptBytes {
            buf: self.buf.clone(),
            offset: offset + 4,
            remaining: count,
        }
    }
}

/// Zero-copy view over an Ack command.
pub struct CmdAck {
    buf: Bytes,
}

impl CmdAck {
    pub fn queue_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn message_ids(&self) -> Vec<u64> {
        let mut cursor = std::io::Cursor::new(&self.buf[9..]);
        decode_vec_u64(&mut cursor).unwrap_or_default()
    }

    pub fn response(&self) -> Option<Bytes> {
        // Skip past queue_id(8) + vec_u64
        let mut cursor = std::io::Cursor::new(&self.buf[9..]);
        let _ = decode_vec_u64(&mut cursor);
        let pos = cursor.position() as usize;
        let offset = 9 + pos;
        if offset >= self.buf.len() {
            return None;
        }
        let present = self.buf[offset];
        if present == 0 {
            None
        } else {
            let len =
                u32::from_le_bytes(self.buf[offset + 1..offset + 5].try_into().unwrap()) as usize;
            Some(self.buf.slice(offset + 5..offset + 5 + len))
        }
    }
}

/// Zero-copy view over a Nack command.
pub struct CmdNack {
    buf: Bytes,
}

impl CmdNack {
    pub fn queue_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn message_ids(&self) -> Vec<u64> {
        let mut cursor = std::io::Cursor::new(&self.buf[9..]);
        decode_vec_u64(&mut cursor).unwrap_or_default()
    }
}

/// Zero-copy view over an ExtendVisibility command.
pub struct CmdExtendVisibility {
    buf: Bytes,
}

impl CmdExtendVisibility {
    pub fn queue_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn message_ids(&self) -> Vec<u64> {
        let mut cursor = std::io::Cursor::new(&self.buf[9..]);
        decode_vec_u64(&mut cursor).unwrap_or_default()
    }

    pub fn extension_ms(&self) -> u64 {
        let mut cursor = std::io::Cursor::new(&self.buf[9..]);
        let _ = decode_vec_u64(&mut cursor);
        let pos = cursor.position() as usize;
        let offset = 9 + pos;
        u64::from_le_bytes(self.buf[offset..offset + 8].try_into().unwrap())
    }
}

/// Zero-copy view over a TimeoutExpired command.
pub struct CmdTimeoutExpired {
    buf: Bytes,
}

impl CmdTimeoutExpired {
    pub fn queue_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn message_ids(&self) -> Vec<u64> {
        let mut cursor = std::io::Cursor::new(&self.buf[9..]);
        decode_vec_u64(&mut cursor).unwrap_or_default()
    }
}

/// Zero-copy view over a PublishToDlq command.
pub struct CmdPublishToDlq {
    buf: Bytes,
}

impl CmdPublishToDlq {
    pub fn source_queue_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn dlq_topic_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[9..17].try_into().unwrap())
    }

    pub fn dead_letter_ids(&self) -> Vec<u64> {
        let mut cursor = std::io::Cursor::new(&self.buf[17..]);
        decode_vec_u64(&mut cursor).unwrap_or_default()
    }

    pub fn messages(&self) -> FlatMessages {
        let mut cursor = std::io::Cursor::new(&self.buf[17..]);
        let _ = decode_vec_u64(&mut cursor);
        let pos = cursor.position() as usize;
        let offset = 17 + pos;
        let count = u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap());
        FlatMessages {
            buf: self.buf.clone(),
            offset: offset + 4,
            remaining: count,
        }
    }
}

/// Zero-copy view over an ExpirePendingMessages command.
pub struct CmdExpirePendingMessages {
    buf: Bytes,
}

impl CmdExpirePendingMessages {
    pub fn queue_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn message_ids(&self) -> Vec<u64> {
        let mut cursor = std::io::Cursor::new(&self.buf[9..]);
        decode_vec_u64(&mut cursor).unwrap_or_default()
    }
}

/// Zero-copy view over a CreateExchange command.
pub struct CmdCreateExchange {
    buf: Bytes,
}

impl CmdCreateExchange {
    pub fn name(&self) -> &str {
        let len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[5..5 + len]).unwrap_or("")
    }

    pub fn exchange_type(&self) -> ExchangeType {
        let name_len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        let offset = 5 + name_len;
        let mut cursor = std::io::Cursor::new(&self.buf[offset..]);
        ExchangeType::decode(&mut cursor).unwrap_or_default()
    }
}

/// Zero-copy view over a CreateBinding command.
pub struct CmdCreateBinding {
    buf: Bytes,
}

impl CmdCreateBinding {
    pub fn exchange_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn queue_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[9..17].try_into().unwrap())
    }

    pub fn routing_key(&self) -> Option<String> {
        let mut cursor = std::io::Cursor::new(&self.buf[17..]);
        decode_opt_string(&mut cursor).unwrap_or(None)
    }
}

/// Zero-copy view over a PublishToExchange command.
pub struct CmdPublishToExchange {
    buf: Bytes,
}

impl CmdPublishToExchange {
    pub fn exchange_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn messages(&self) -> FlatMessages {
        let count = u32::from_le_bytes(self.buf[9..13].try_into().unwrap());
        FlatMessages {
            buf: self.buf.clone(),
            offset: 13,
            remaining: count,
        }
    }
}

/// Zero-copy view over a CreateActorNamespace command.
pub struct CmdCreateActorNamespace {
    buf: Bytes,
}

impl CmdCreateActorNamespace {
    pub fn name(&self) -> &str {
        let len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[5..5 + len]).unwrap_or("")
    }

    pub fn config(&self) -> ActorConfig {
        let name_len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        let offset = 5 + name_len;
        let mut cursor = std::io::Cursor::new(&self.buf[offset..]);
        ActorConfig::decode(&mut cursor).unwrap_or_default()
    }
}

/// Zero-copy view over a SendToActor command.
pub struct CmdSendToActor {
    buf: Bytes,
}

impl CmdSendToActor {
    pub fn namespace_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn actor_id(&self) -> Bytes {
        let len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        self.buf.slice(13..13 + len)
    }

    pub fn message(&self) -> Bytes {
        let actor_id_len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        let msg_offset = 13 + actor_id_len;
        let msg_len =
            u32::from_le_bytes(self.buf[msg_offset..msg_offset + 4].try_into().unwrap()) as usize;
        self.buf.slice(msg_offset + 4..msg_offset + 4 + msg_len)
    }
}

/// Zero-copy view over a DeliverActorMessage command.
pub struct CmdDeliverActorMessage {
    buf: Bytes,
}

impl CmdDeliverActorMessage {
    pub fn namespace_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn actor_id(&self) -> Bytes {
        let len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        self.buf.slice(13..13 + len)
    }

    pub fn consumer_id(&self) -> u64 {
        let actor_id_len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        let offset = 13 + actor_id_len;
        u64::from_le_bytes(self.buf[offset..offset + 8].try_into().unwrap())
    }
}

/// Zero-copy view over an AckActorMessage command.
pub struct CmdAckActorMessage {
    buf: Bytes,
}

impl CmdAckActorMessage {
    pub fn namespace_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn actor_id(&self) -> Bytes {
        let len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        self.buf.slice(13..13 + len)
    }

    pub fn message_id(&self) -> u64 {
        let actor_id_len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        let offset = 13 + actor_id_len;
        u64::from_le_bytes(self.buf[offset..offset + 8].try_into().unwrap())
    }

    pub fn response(&self) -> Option<Bytes> {
        let actor_id_len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        let offset = 13 + actor_id_len + 8; // skip actor_id + message_id
        if offset >= self.buf.len() {
            return None;
        }
        let present = self.buf[offset];
        if present == 0 {
            None
        } else {
            let len =
                u32::from_le_bytes(self.buf[offset + 1..offset + 5].try_into().unwrap()) as usize;
            Some(self.buf.slice(offset + 5..offset + 5 + len))
        }
    }
}

/// Zero-copy view over a NackActorMessage command.
pub struct CmdNackActorMessage {
    buf: Bytes,
}

impl CmdNackActorMessage {
    pub fn namespace_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn actor_id(&self) -> Bytes {
        let len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        self.buf.slice(13..13 + len)
    }

    pub fn message_id(&self) -> u64 {
        let actor_id_len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        let offset = 13 + actor_id_len;
        u64::from_le_bytes(self.buf[offset..offset + 8].try_into().unwrap())
    }
}

/// Zero-copy view over an AssignActors command.
pub struct CmdAssignActors {
    buf: Bytes,
}

impl CmdAssignActors {
    pub fn namespace_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn consumer_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[9..17].try_into().unwrap())
    }

    pub fn actor_ids(&self) -> Vec<Bytes> {
        let mut cursor = std::io::Cursor::new(&self.buf[17..]);
        decode_vec_bytes(&mut cursor).unwrap_or_default()
    }
}

/// Zero-copy view over a CreateJob command.
pub struct CmdCreateJob {
    buf: Bytes,
}

impl CmdCreateJob {
    pub fn name(&self) -> &str {
        let len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[5..5 + len]).unwrap_or("")
    }

    pub fn config(&self) -> JobConfig {
        let name_len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        let offset = 5 + name_len;
        let mut cursor = std::io::Cursor::new(&self.buf[offset..]);
        JobConfig::decode(&mut cursor).unwrap_or_default()
    }
}

/// Zero-copy view over an UpdateJob command.
pub struct CmdUpdateJob {
    buf: Bytes,
}

impl CmdUpdateJob {
    pub fn job_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn config(&self) -> JobConfig {
        let mut cursor = std::io::Cursor::new(&self.buf[9..]);
        JobConfig::decode(&mut cursor).unwrap_or_default()
    }
}

/// Zero-copy view over a FailJob command.
pub struct CmdFailJob {
    buf: Bytes,
}

impl CmdFailJob {
    pub fn job_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn execution_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[9..17].try_into().unwrap())
    }

    pub fn error(&self) -> &str {
        let len = u32::from_le_bytes(self.buf[17..21].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[21..21 + len]).unwrap_or("")
    }
}

/// Zero-copy view over a RegisterConsumer command.
pub struct CmdRegisterConsumer {
    buf: Bytes,
}

impl CmdRegisterConsumer {
    pub fn consumer_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn group_name(&self) -> &str {
        let len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[13..13 + len]).unwrap_or("")
    }

    pub fn subscriptions(&self) -> Vec<Subscription> {
        let name_len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        let offset = 13 + name_len;
        let mut cursor = std::io::Cursor::new(&self.buf[offset..]);
        Vec::<Subscription>::decode(&mut cursor).unwrap_or_default()
    }
}

/// Zero-copy view over a RegisterProducer command.
pub struct CmdRegisterProducer {
    buf: Bytes,
}

impl CmdRegisterProducer {
    pub fn producer_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn name(&self) -> Option<String> {
        let mut cursor = std::io::Cursor::new(&self.buf[9..]);
        decode_opt_string(&mut cursor).unwrap_or(None)
    }
}

/// Zero-copy view over a Batch command (length-prefixed sub-commands).
pub struct CmdBatch {
    buf: Bytes,
}

impl CmdBatch {
    pub fn count(&self) -> u32 {
        u32::from_le_bytes(self.buf[1..5].try_into().unwrap())
    }

    pub fn commands(&self) -> BatchIter {
        BatchIter {
            buf: self.buf.clone(),
            offset: 5,
            remaining: self.count(),
        }
    }
}

// =============================================================================
// BatchIter — zero-copy iterator over length-prefixed sub-commands
// =============================================================================

pub struct BatchIter {
    buf: Bytes,
    offset: usize,
    remaining: u32,
}

impl Iterator for BatchIter {
    type Item = MqCommand;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let len =
            u32::from_le_bytes(self.buf[self.offset..self.offset + 4].try_into().unwrap()) as usize;
        self.offset += 4;
        let cmd = MqCommand {
            buf: self.buf.slice(self.offset..self.offset + len),
        };
        self.offset += len;
        Some(cmd)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.remaining as usize;
        (r, Some(r))
    }
}

impl ExactSizeIterator for BatchIter {}

// =============================================================================
// FlatOptBytes — zero-copy iterator over Option<Bytes> sequences
// =============================================================================

pub struct FlatOptBytes {
    buf: Bytes,
    offset: usize,
    remaining: u32,
}

impl Iterator for FlatOptBytes {
    type Item = Option<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let present = self.buf[self.offset];
        self.offset += 1;
        if present == 0 {
            Some(None)
        } else {
            let len = u32::from_le_bytes(self.buf[self.offset..self.offset + 4].try_into().unwrap())
                as usize;
            self.offset += 4;
            let val = self.buf.slice(self.offset..self.offset + len);
            self.offset += len;
            Some(Some(val))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.remaining as usize;
        (r, Some(r))
    }
}

impl ExactSizeIterator for FlatOptBytes {}

// =============================================================================
// fmt_mq_command — Display formatter for MqCommand
// =============================================================================

pub fn fmt_mq_command(cmd: &MqCommand, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match cmd.tag() {
        MqCommand::TAG_CREATE_TOPIC => {
            let v = cmd.as_create_topic();
            write!(f, "CreateTopic({})", v.name())
        }
        MqCommand::TAG_DELETE_TOPIC => write!(f, "DeleteTopic({})", cmd.field_u64(1)),
        MqCommand::TAG_PUBLISH => {
            let v = cmd.as_publish();
            write!(
                f,
                "Publish(topic={}, count={})",
                v.topic_id(),
                v.message_count()
            )
        }
        MqCommand::TAG_COMMIT_OFFSET => {
            write!(
                f,
                "CommitOffset(topic={}, consumer={}, offset={})",
                cmd.field_u64(1),
                cmd.field_u64(9),
                cmd.field_u64(17)
            )
        }
        MqCommand::TAG_PURGE_TOPIC => {
            write!(
                f,
                "PurgeTopic(topic={}, before={})",
                cmd.field_u64(1),
                cmd.field_u64(9)
            )
        }
        MqCommand::TAG_CREATE_QUEUE => {
            let v = cmd.as_create_queue();
            write!(f, "CreateQueue({})", v.name())
        }
        MqCommand::TAG_DELETE_QUEUE => write!(f, "DeleteQueue({})", cmd.field_u64(1)),
        MqCommand::TAG_ENQUEUE => {
            let v = cmd.as_enqueue();
            write!(f, "Enqueue(queue={})", v.queue_id())
        }
        MqCommand::TAG_DELIVER => {
            write!(
                f,
                "Deliver(queue={}, consumer={}, max={})",
                cmd.field_u64(1),
                cmd.field_u64(9),
                cmd.field_u32(17)
            )
        }
        MqCommand::TAG_ACK => {
            let v = cmd.as_ack();
            let ids = v.message_ids();
            write!(f, "Ack(queue={}, count={})", v.queue_id(), ids.len())
        }
        MqCommand::TAG_NACK => {
            let v = cmd.as_nack();
            let ids = v.message_ids();
            write!(f, "Nack(queue={}, count={})", v.queue_id(), ids.len())
        }
        MqCommand::TAG_EXTEND_VISIBILITY => {
            let v = cmd.as_extend_visibility();
            write!(f, "ExtendVisibility(queue={})", v.queue_id())
        }
        MqCommand::TAG_TIMEOUT_EXPIRED => {
            let v = cmd.as_timeout_expired();
            write!(f, "TimeoutExpired(queue={})", v.queue_id())
        }
        MqCommand::TAG_PUBLISH_TO_DLQ => {
            let v = cmd.as_publish_to_dlq();
            write!(
                f,
                "PublishToDlq(src_queue={}, dlq_topic={})",
                v.source_queue_id(),
                v.dlq_topic_id()
            )
        }
        MqCommand::TAG_PRUNE_DEDUP_WINDOW => {
            write!(f, "PruneDedupWindow(queue={})", cmd.field_u64(1))
        }
        MqCommand::TAG_EXPIRE_PENDING_MESSAGES => {
            write!(f, "ExpirePendingMessages(queue={})", cmd.field_u64(1))
        }
        MqCommand::TAG_PURGE_QUEUE => write!(f, "PurgeQueue({})", cmd.field_u64(1)),
        MqCommand::TAG_GET_QUEUE_ATTRIBUTES => {
            write!(f, "GetQueueAttributes({})", cmd.field_u64(1))
        }
        MqCommand::TAG_CREATE_EXCHANGE => {
            let v = cmd.as_create_exchange();
            write!(f, "CreateExchange({})", v.name())
        }
        MqCommand::TAG_DELETE_EXCHANGE => write!(f, "DeleteExchange({})", cmd.field_u64(1)),
        MqCommand::TAG_CREATE_BINDING => {
            let v = cmd.as_create_binding();
            write!(
                f,
                "CreateBinding(exchange={}, queue={})",
                v.exchange_id(),
                v.queue_id()
            )
        }
        MqCommand::TAG_DELETE_BINDING => write!(f, "DeleteBinding({})", cmd.field_u64(1)),
        MqCommand::TAG_PUBLISH_TO_EXCHANGE => {
            let v = cmd.as_publish_to_exchange();
            write!(f, "PublishToExchange(exchange={})", v.exchange_id())
        }
        MqCommand::TAG_CREATE_ACTOR_NAMESPACE => {
            let v = cmd.as_create_actor_namespace();
            write!(f, "CreateActorNamespace({})", v.name())
        }
        MqCommand::TAG_DELETE_ACTOR_NAMESPACE => {
            write!(f, "DeleteActorNamespace({})", cmd.field_u64(1))
        }
        MqCommand::TAG_SEND_TO_ACTOR => {
            let v = cmd.as_send_to_actor();
            write!(f, "SendToActor(ns={})", v.namespace_id())
        }
        MqCommand::TAG_DELIVER_ACTOR_MESSAGE => {
            let v = cmd.as_deliver_actor_message();
            write!(f, "DeliverActorMessage(ns={})", v.namespace_id())
        }
        MqCommand::TAG_ACK_ACTOR_MESSAGE => {
            let v = cmd.as_ack_actor_message();
            write!(f, "AckActorMessage(ns={})", v.namespace_id())
        }
        MqCommand::TAG_NACK_ACTOR_MESSAGE => {
            let v = cmd.as_nack_actor_message();
            write!(f, "NackActorMessage(ns={})", v.namespace_id())
        }
        MqCommand::TAG_ASSIGN_ACTORS => {
            let v = cmd.as_assign_actors();
            write!(
                f,
                "AssignActors(ns={}, consumer={})",
                v.namespace_id(),
                v.consumer_id()
            )
        }
        MqCommand::TAG_RELEASE_ACTORS => {
            write!(
                f,
                "ReleaseActors(ns={}, consumer={})",
                cmd.field_u64(1),
                cmd.field_u64(9)
            )
        }
        MqCommand::TAG_EVICT_IDLE_ACTORS => {
            write!(f, "EvictIdleActors(ns={})", cmd.field_u64(1))
        }
        MqCommand::TAG_CREATE_JOB => {
            let v = cmd.as_create_job();
            write!(f, "CreateJob({})", v.name())
        }
        MqCommand::TAG_DELETE_JOB => write!(f, "DeleteJob({})", cmd.field_u64(1)),
        MqCommand::TAG_UPDATE_JOB => {
            let v = cmd.as_update_job();
            write!(f, "UpdateJob({})", v.job_id())
        }
        MqCommand::TAG_ENABLE_JOB => write!(f, "EnableJob({})", cmd.field_u64(1)),
        MqCommand::TAG_DISABLE_JOB => write!(f, "DisableJob({})", cmd.field_u64(1)),
        MqCommand::TAG_TRIGGER_JOB => {
            write!(
                f,
                "TriggerJob(job={}, exec={})",
                cmd.field_u64(1),
                cmd.field_u64(9)
            )
        }
        MqCommand::TAG_ASSIGN_JOB => {
            write!(
                f,
                "AssignJob(job={}, consumer={})",
                cmd.field_u64(1),
                cmd.field_u64(9)
            )
        }
        MqCommand::TAG_COMPLETE_JOB => {
            write!(
                f,
                "CompleteJob(job={}, exec={})",
                cmd.field_u64(1),
                cmd.field_u64(9)
            )
        }
        MqCommand::TAG_FAIL_JOB => {
            let v = cmd.as_fail_job();
            write!(f, "FailJob(job={}, exec={})", v.job_id(), v.execution_id())
        }
        MqCommand::TAG_TIMEOUT_JOB => {
            write!(
                f,
                "TimeoutJob(job={}, exec={})",
                cmd.field_u64(1),
                cmd.field_u64(9)
            )
        }
        MqCommand::TAG_REGISTER_CONSUMER => {
            let v = cmd.as_register_consumer();
            write!(
                f,
                "RegisterConsumer(id={}, group={})",
                v.consumer_id(),
                v.group_name()
            )
        }
        MqCommand::TAG_DISCONNECT_CONSUMER => {
            write!(f, "DisconnectConsumer({})", cmd.field_u64(1))
        }
        MqCommand::TAG_HEARTBEAT => write!(f, "Heartbeat({})", cmd.field_u64(1)),
        MqCommand::TAG_REGISTER_PRODUCER => {
            let v = cmd.as_register_producer();
            write!(f, "RegisterProducer({})", v.producer_id())
        }
        MqCommand::TAG_DISCONNECT_PRODUCER => {
            write!(f, "DisconnectProducer({})", cmd.field_u64(1))
        }
        MqCommand::TAG_BATCH => {
            let v = cmd.as_batch();
            write!(f, "Batch(count={})", v.count())
        }
        _ => write!(f, "MqCommand(tag={})", cmd.tag()),
    }
}

// =============================================================================
// MqResponse — Encode / Decode
// =============================================================================

impl Encode for MqResponse {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        match self {
            MqResponse::Ok => TAG_RESP_OK.encode(w),
            MqResponse::Error(err) => {
                TAG_RESP_ERROR.encode(w)?;
                err.encode(w)
            }
            MqResponse::EntityCreated { id } => {
                TAG_RESP_ENTITY_CREATED.encode(w)?;
                id.encode(w)
            }
            MqResponse::Messages { messages } => {
                TAG_RESP_MESSAGES.encode(w)?;
                messages.encode(w)
            }
            MqResponse::Published { offsets } => {
                TAG_RESP_PUBLISHED.encode(w)?;
                (offsets.len() as u32).encode(w)?;
                for &o in offsets.iter() {
                    o.encode(w)?;
                }
                Ok(())
            }
            MqResponse::Stats(stats) => {
                TAG_RESP_STATS.encode(w)?;
                stats.encode(w)
            }
            MqResponse::BatchResponse(resps) => {
                TAG_RESP_BATCH.encode(w)?;
                (resps.len() as u32).encode(w)?;
                for resp in resps {
                    resp.encode(w)?;
                }
                Ok(())
            }
            MqResponse::DeadLettered {
                dead_letter_ids,
                dlq_topic_id,
            } => {
                TAG_RESP_DEAD_LETTERED.encode(w)?;
                encode_vec_u64(w, dead_letter_ids)?;
                dlq_topic_id.encode(w)
            }
        }
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            MqResponse::Ok => 0,
            MqResponse::Error(err) => err.encoded_size(),
            MqResponse::EntityCreated { .. } => 8,
            MqResponse::Messages { messages } => messages.encoded_size(),
            MqResponse::Published { offsets } => 4 + offsets.len() * 8,
            MqResponse::Stats(stats) => stats.encoded_size(),
            MqResponse::BatchResponse(resps) => {
                4 + resps.iter().map(|r| r.encoded_size()).sum::<usize>()
            }
            MqResponse::DeadLettered {
                dead_letter_ids, ..
            } => 4 + dead_letter_ids.len() * 8 + 8,
        }
    }
}

impl Decode for MqResponse {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            TAG_RESP_OK => Ok(MqResponse::Ok),
            TAG_RESP_ERROR => Ok(MqResponse::Error(MqError::decode(r)?)),
            TAG_RESP_ENTITY_CREATED => Ok(MqResponse::EntityCreated {
                id: u64::decode(r)?,
            }),
            TAG_RESP_MESSAGES => Ok(MqResponse::Messages {
                messages: Vec::decode(r)?,
            }),
            TAG_RESP_PUBLISHED => {
                let count = u32::decode(r)? as usize;
                let mut offsets = SmallVec::with_capacity(count.min(256));
                for _ in 0..count {
                    offsets.push(u64::decode(r)?);
                }
                Ok(MqResponse::Published { offsets })
            }
            TAG_RESP_STATS => Ok(MqResponse::Stats(EntityStats::decode(r)?)),
            TAG_RESP_BATCH => {
                let count = u32::decode(r)? as usize;
                let mut resps = Vec::with_capacity(count.min(256));
                for _ in 0..count {
                    resps.push(MqResponse::decode(r)?);
                }
                Ok(MqResponse::BatchResponse(resps))
            }
            TAG_RESP_DEAD_LETTERED => Ok(MqResponse::DeadLettered {
                dead_letter_ids: decode_vec_u64(r)?,
                dlq_topic_id: u64::decode(r)?,
            }),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

impl BorrowPayload for MqResponse {
    fn payload_bytes(&self) -> &[u8] {
        &[]
    }
}

/// Zero-copy iterator over length-prefixed messages in a flat command buffer.
///
/// Each `next()` call returns a `Bytes` slice backed by the same mmap segment
/// as the parent `FlatMqCommand`. No allocations occur during iteration.
pub struct FlatMessages {
    buf: Bytes,
    offset: usize,
    remaining: u32,
}

impl FlatMessages {
    /// Number of messages remaining in the iterator.
    #[inline]
    pub fn remaining(&self) -> u32 {
        self.remaining
    }
}

impl Iterator for FlatMessages {
    type Item = Bytes;

    #[inline]
    fn next(&mut self) -> Option<Bytes> {
        if self.remaining == 0 {
            return None;
        }
        let buf_len = self.buf.len();
        if self.offset + 4 > buf_len {
            self.remaining = 0;
            return None;
        }
        let len =
            u32::from_le_bytes(self.buf[self.offset..self.offset + 4].try_into().unwrap()) as usize;
        self.offset += 4;
        if self.offset + len > buf_len {
            self.remaining = 0;
            return None;
        }
        let slice = self.buf.slice(self.offset..self.offset + len);
        self.offset += len;
        self.remaining -= 1;
        Some(slice)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let r = self.remaining as usize;
        (r, Some(r))
    }
}

impl ExactSizeIterator for FlatMessages {}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_resp(resp: &MqResponse) -> MqResponse {
        let encoded = resp.encode_to_vec().unwrap();
        MqResponse::decode_from_slice(&encoded).unwrap()
    }

    #[test]
    fn publish_roundtrip() {
        let cmd = MqCommand::publish(
            42,
            &[Bytes::from_static(b"hello"), Bytes::from_static(b"world")],
        );
        let v = cmd.as_publish();
        assert_eq!(v.topic_id(), 42);
        let msgs: Vec<Bytes> = v.messages().collect();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0], Bytes::from_static(b"hello"));
        assert_eq!(msgs[1], Bytes::from_static(b"world"));
    }

    #[test]
    fn create_topic_roundtrip() {
        let cmd = MqCommand::create_topic(
            "my-topic",
            RetentionPolicy {
                max_age_secs: Some(3600),
                max_bytes: None,
                max_messages: Some(1_000_000),
            },
            8,
        );
        let v = cmd.as_create_topic();
        assert_eq!(v.name(), "my-topic");
        let ret = v.retention();
        assert_eq!(ret.max_age_secs, Some(3600));
        assert_eq!(ret.max_bytes, None);
        assert_eq!(ret.max_messages, Some(1_000_000));
        assert_eq!(v.partition_count(), 8);
    }

    #[test]
    fn batch_roundtrip() {
        let cmds = vec![
            MqCommand::delete_topic(1),
            MqCommand::publish(2, &[Bytes::from_static(b"msg")]),
            MqCommand::heartbeat(99),
        ];
        let cmd = MqCommand::batch(&cmds);
        let v = cmd.as_batch();
        assert_eq!(v.count(), 3);
        let sub_cmds: Vec<MqCommand> = v.commands().collect();
        assert_eq!(sub_cmds.len(), 3);
        assert_eq!(sub_cmds[0].tag(), MqCommand::TAG_DELETE_TOPIC);
        assert_eq!(sub_cmds[1].tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(sub_cmds[2].tag(), MqCommand::TAG_HEARTBEAT);
    }

    #[test]
    fn enqueue_roundtrip() {
        let cmd = MqCommand::enqueue(
            5,
            &[Bytes::from_static(b"m1")],
            &[None, Some(Bytes::from_static(b"key1"))],
        );
        let v = cmd.as_enqueue();
        assert_eq!(v.queue_id(), 5);
        let msgs: Vec<Bytes> = v.messages().collect();
        assert_eq!(msgs.len(), 1);
        let dedup: Vec<Option<Bytes>> = v.dedup_keys().collect();
        assert_eq!(dedup.len(), 2);
        assert_eq!(dedup[0], None);
        assert_eq!(dedup[1], Some(Bytes::from_static(b"key1")));
    }

    #[test]
    fn response_roundtrips() {
        let cases: Vec<MqResponse> = vec![
            MqResponse::Ok,
            MqResponse::Error(MqError::NotFound {
                entity: EntityKind::Topic,
                id: 42,
            }),
            MqResponse::EntityCreated { id: 7 },
            MqResponse::Published {
                offsets: SmallVec::from_vec(vec![1, 2, 3]),
            },
            MqResponse::DeadLettered {
                dead_letter_ids: vec![10, 20],
                dlq_topic_id: 99,
            },
        ];
        for resp in &cases {
            let decoded = roundtrip_resp(resp);
            let enc1 = resp.encode_to_vec().unwrap();
            let enc2 = decoded.encode_to_vec().unwrap();
            assert_eq!(enc1, enc2, "roundtrip mismatch for {:?}", resp);
        }
    }

    #[test]
    fn publish_messages_zero_copy() {
        let cmd = MqCommand::publish(
            42,
            &[Bytes::from_static(b"aaa"), Bytes::from_static(b"bbbbb")],
        );

        assert_eq!(cmd.tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(cmd.as_publish().topic_id(), 42);

        let msgs: Vec<Bytes> = cmd.publish_messages().unwrap().collect();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0], Bytes::from_static(b"aaa"));
        assert_eq!(msgs[1], Bytes::from_static(b"bbbbb"));
    }

    #[test]
    fn publish_messages_for_topic_filter() {
        let cmd = MqCommand::publish(7, &[Bytes::from_static(b"x")]);

        assert!(cmd.publish_messages_for_topic(7).is_some());
        assert!(cmd.publish_messages_for_topic(8).is_none());
    }

    #[test]
    fn non_publish_returns_none() {
        let cmd = MqCommand::delete_topic(1);

        assert!(cmd.publish_messages().is_none());
        assert!(cmd.publish_messages_for_topic(1).is_none());
    }

    #[test]
    fn all_simple_variants_roundtrip() {
        // Test that encoding via constructors produces valid buffers
        // by verifying tag bytes and field reads.
        let cases: Vec<(MqCommand, u8)> = vec![
            (MqCommand::delete_topic(1), MqCommand::TAG_DELETE_TOPIC),
            (
                MqCommand::commit_offset(1, 2, 100),
                MqCommand::TAG_COMMIT_OFFSET,
            ),
            (MqCommand::purge_topic(1, 50), MqCommand::TAG_PURGE_TOPIC),
            (MqCommand::delete_queue(3), MqCommand::TAG_DELETE_QUEUE),
            (MqCommand::deliver(3, 4, 10), MqCommand::TAG_DELIVER),
            (MqCommand::purge_queue(3), MqCommand::TAG_PURGE_QUEUE),
            (
                MqCommand::get_queue_attributes(3),
                MqCommand::TAG_GET_QUEUE_ATTRIBUTES,
            ),
            (
                MqCommand::delete_exchange(5),
                MqCommand::TAG_DELETE_EXCHANGE,
            ),
            (MqCommand::delete_binding(6), MqCommand::TAG_DELETE_BINDING),
            (
                MqCommand::delete_actor_namespace(7),
                MqCommand::TAG_DELETE_ACTOR_NAMESPACE,
            ),
            (
                MqCommand::release_actors(7, 8),
                MqCommand::TAG_RELEASE_ACTORS,
            ),
            (
                MqCommand::evict_idle_actors(7, 999),
                MqCommand::TAG_EVICT_IDLE_ACTORS,
            ),
            (MqCommand::delete_job(9), MqCommand::TAG_DELETE_JOB),
            (MqCommand::enable_job(9), MqCommand::TAG_ENABLE_JOB),
            (MqCommand::disable_job(9), MqCommand::TAG_DISABLE_JOB),
            (
                MqCommand::trigger_job(9, 10, 1000),
                MqCommand::TAG_TRIGGER_JOB,
            ),
            (MqCommand::assign_job(9, 11), MqCommand::TAG_ASSIGN_JOB),
            (MqCommand::complete_job(9, 10), MqCommand::TAG_COMPLETE_JOB),
            (MqCommand::timeout_job(9, 10), MqCommand::TAG_TIMEOUT_JOB),
            (
                MqCommand::disconnect_consumer(12),
                MqCommand::TAG_DISCONNECT_CONSUMER,
            ),
            (MqCommand::heartbeat(12), MqCommand::TAG_HEARTBEAT),
            (
                MqCommand::disconnect_producer(13),
                MqCommand::TAG_DISCONNECT_PRODUCER,
            ),
        ];
        for (cmd, expected_tag) in &cases {
            assert_eq!(cmd.tag(), *expected_tag, "tag mismatch");
            // Verify encode/decode roundtrip via Encode trait (passthrough)
            let encoded = cmd.encode_to_vec().unwrap();
            assert_eq!(encoded.len(), cmd.encoded_size());
            assert_eq!(&encoded[..], &cmd.buf[..]);
        }
    }

    #[test]
    fn ack_view() {
        let cmd = MqCommand::ack(5, &[1, 2, 3], Some(&Bytes::from_static(b"resp")));
        let v = cmd.as_ack();
        assert_eq!(v.queue_id(), 5);
        assert_eq!(v.message_ids(), vec![1, 2, 3]);
        assert_eq!(v.response(), Some(Bytes::from_static(b"resp")));

        let cmd2 = MqCommand::ack(5, &[1], None);
        let v2 = cmd2.as_ack();
        assert_eq!(v2.response(), None);
    }

    #[test]
    fn nack_view() {
        let cmd = MqCommand::nack(7, &[10, 20]);
        let v = cmd.as_nack();
        assert_eq!(v.queue_id(), 7);
        assert_eq!(v.message_ids(), vec![10, 20]);
    }

    #[test]
    fn create_exchange_view() {
        let cmd = MqCommand::create_exchange("my-exchange", ExchangeType::Topic);
        let v = cmd.as_create_exchange();
        assert_eq!(v.name(), "my-exchange");
        assert_eq!(v.exchange_type(), ExchangeType::Topic);
    }

    #[test]
    fn create_binding_view() {
        let cmd = MqCommand::create_binding(1, 2, Some("routing.key"));
        let v = cmd.as_create_binding();
        assert_eq!(v.exchange_id(), 1);
        assert_eq!(v.queue_id(), 2);
        assert_eq!(v.routing_key(), Some("routing.key".to_string()));
    }

    #[test]
    fn send_to_actor_view() {
        let cmd = MqCommand::send_to_actor(1, b"actor-1", b"hello");
        let v = cmd.as_send_to_actor();
        assert_eq!(v.namespace_id(), 1);
        assert_eq!(v.actor_id(), Bytes::from_static(b"actor-1"));
        assert_eq!(v.message(), Bytes::from_static(b"hello"));
    }

    #[test]
    fn fail_job_view() {
        let cmd = MqCommand::fail_job(5, 10, "something went wrong");
        let v = cmd.as_fail_job();
        assert_eq!(v.job_id(), 5);
        assert_eq!(v.execution_id(), 10);
        assert_eq!(v.error(), "something went wrong");
    }

    #[test]
    fn register_consumer_view() {
        let subs = vec![
            Subscription {
                entity_type: EntityType::Queue,
                entity_id: 1,
            },
            Subscription {
                entity_type: EntityType::Topic,
                entity_id: 2,
            },
        ];
        let cmd = MqCommand::register_consumer(99, "my-group", &subs);
        let v = cmd.as_register_consumer();
        assert_eq!(v.consumer_id(), 99);
        assert_eq!(v.group_name(), "my-group");
        let decoded_subs = v.subscriptions();
        assert_eq!(decoded_subs.len(), 2);
        assert_eq!(decoded_subs[0].entity_id, 1);
        assert_eq!(decoded_subs[1].entity_id, 2);
    }

    #[test]
    fn display_format() {
        let cases: Vec<(MqCommand, &str)> = vec![
            (
                MqCommand::create_topic("events", RetentionPolicy::default(), 0),
                "CreateTopic(events)",
            ),
            (MqCommand::delete_topic(42), "DeleteTopic(42)"),
            (
                MqCommand::publish(
                    1,
                    &[crate::flat::FlatMessageBuilder::new(Bytes::from_static(b"x")).build()],
                ),
                "Publish(topic=1, count=1)",
            ),
            (
                MqCommand::create_queue("tasks", &QueueConfig::default()),
                "CreateQueue(tasks)",
            ),
            (MqCommand::ack(5, &[1, 2, 3], None), "Ack(queue=5, count=3)"),
            (MqCommand::heartbeat(99), "Heartbeat(99)"),
            (MqCommand::disconnect_consumer(7), "DisconnectConsumer(7)"),
        ];

        for (cmd, expected) in cases {
            assert_eq!(format!("{}", cmd), expected);
        }
    }
}

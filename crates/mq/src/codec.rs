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
const TAG_RESP_GROUP_JOINED: u8 = 8;
const TAG_RESP_GROUP_SYNCED: u8 = 9;
const TAG_RESP_RETAINED_MESSAGES: u8 = 10;
const TAG_RESP_WILL_PENDING: u8 = 11;
const TAG_RESP_SESSION_RESTORED: u8 = 12;
const TAG_RESP_SESSION_NOT_FOUND: u8 = 13;
const TAG_RESP_MULTI_MESSAGES: u8 = 14;
const TAG_RESP_TOPIC_ALIASES: u8 = 15;
const TAG_RESP_WILLS_FIRED: u8 = 16;

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
fn decode_bytes<R: Read>(r: &mut R) -> Result<Vec<u8>, CodecError> {
    let len = u32::decode(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
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
fn decode_vec_u64<R: Read>(r: &mut R) -> Result<SmallVec<[u64; 8]>, CodecError> {
    let count = u32::decode(r)? as usize;
    let mut v = SmallVec::with_capacity(count.min(4096));
    for _ in 0..count {
        v.push(u64::decode(r)?);
    }
    Ok(v)
}

/// Public helper used by engine.rs for parsing vec_u64 from raw command buffers.
pub fn decode_cmd_vec_u64<R: Read>(r: &mut R) -> SmallVec<[u64; 8]> {
    decode_vec_u64(r).unwrap_or_default()
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

#[inline]
fn encode_opt_u32<W: Write>(w: &mut W, v: Option<u32>) -> Result<(), CodecError> {
    match v {
        None => 0u8.encode(w),
        Some(val) => {
            1u8.encode(w)?;
            val.encode(w)
        }
    }
}

#[inline]
fn decode_opt_u32<R: Read>(r: &mut R) -> Result<Option<u32>, CodecError> {
    if u8::decode(r)? == 0 {
        Ok(None)
    } else {
        Ok(Some(u32::decode(r)?))
    }
}

#[inline]
fn encode_opt_u64<W: Write>(w: &mut W, v: Option<u64>) -> Result<(), CodecError> {
    match v {
        None => 0u8.encode(w),
        Some(val) => {
            1u8.encode(w)?;
            val.encode(w)
        }
    }
}

#[inline]
fn decode_opt_u64<R: Read>(r: &mut R) -> Result<Option<u64>, CodecError> {
    if u8::decode(r)? == 0 {
        Ok(None)
    } else {
        Ok(Some(u64::decode(r)?))
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
            EntityType::Exchange => 1,
            EntityType::ConsumerGroup => 2,
            EntityType::Session => 3,
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
            1 => Ok(EntityType::Exchange),
            2 => Ok(EntityType::ConsumerGroup),
            3 => Ok(EntityType::Session),
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

// -- EntityKind --

impl Encode for EntityKind {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        let tag: u8 = match self {
            EntityKind::Topic => 0,
            EntityKind::Exchange => 1,
            EntityKind::Binding => 2,
            EntityKind::ConsumerGroup => 3,
            EntityKind::Session => 4,
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
            1 => Ok(EntityKind::Exchange),
            2 => Ok(EntityKind::Binding),
            3 => Ok(EntityKind::ConsumerGroup),
            4 => Ok(EntityKind::Session),
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
            MqError::IllegalGeneration => 4u8.encode(w),
            MqError::RebalanceInProgress => 5u8.encode(w),
            MqError::UnknownMemberId => 6u8.encode(w),
            MqError::Custom(msg) => {
                3u8.encode(w)?;
                msg.encode(w)
            }
            MqError::BackPressure { group_id } => {
                7u8.encode(w)?;
                group_id.encode(w)
            }
        }
    }
    fn encoded_size(&self) -> usize {
        1 + match self {
            MqError::NotFound { .. } | MqError::AlreadyExists { .. } => 1 + 8,
            MqError::MailboxFull { .. } => 4,
            MqError::IllegalGeneration
            | MqError::RebalanceInProgress
            | MqError::UnknownMemberId => 0,
            MqError::Custom(msg) => msg.encoded_size(),
            MqError::BackPressure { .. } => 8,
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
            4 => Ok(MqError::IllegalGeneration),
            5 => Ok(MqError::RebalanceInProgress),
            6 => Ok(MqError::UnknownMemberId),
            7 => Ok(MqError::BackPressure {
                group_id: u64::decode(r)?,
            }),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- DeliveredMessage --

impl Encode for DeliveredMessage {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.message_id.encode(w)?;
        self.attempt.encode(w)?;
        self.original_timestamp.encode(w)?;
        self.group_id.encode(w)
    }
    fn encoded_size(&self) -> usize {
        8 + 4 + 8 + 8
    }
}

impl Decode for DeliveredMessage {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            message_id: u64::decode(r)?,
            attempt: u32::decode(r)?,
            original_timestamp: u64::decode(r)?,
            group_id: u64::decode(r)?,
        })
    }
}

// -- GroupVariant --

impl Encode for GroupVariant {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        (*self as u8).encode(w)
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for GroupVariant {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(GroupVariant::Offset),
            1 => Ok(GroupVariant::Ack),
            2 => Ok(GroupVariant::Actor),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- TopicLifetimePolicy --

impl Encode for TopicLifetimePolicy {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        let tag: u8 = match self {
            TopicLifetimePolicy::Permanent => 0,
            TopicLifetimePolicy::DeleteOnLastDetach => 1,
        };
        tag.encode(w)
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for TopicLifetimePolicy {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(TopicLifetimePolicy::Permanent),
            1 => Ok(TopicLifetimePolicy::DeleteOnLastDetach),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
    }
}

// -- TopicDedupConfig --

impl Encode for TopicDedupConfig {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.window_secs.encode(w)
    }
    fn encoded_size(&self) -> usize {
        8
    }
}

impl Decode for TopicDedupConfig {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            window_secs: u64::decode(r)?,
        })
    }
}

// -- AckVariantConfig --

impl Encode for AckVariantConfig {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.visibility_timeout_ms.encode(w)?;
        self.max_retries.encode(w)?;
        encode_opt_string(w, &self.dead_letter_topic)?;
        self.delay_default_ms.encode(w)?;
        self.max_in_flight_per_consumer.encode(w)?;
        encode_opt_u64(w, self.max_pending_messages)?;
        encode_opt_u64(w, self.max_pending_bytes)?;
        encode_opt_u64(w, self.max_delayed_messages)?;
        encode_opt_u64(w, self.max_delayed_bytes)
    }
    fn encoded_size(&self) -> usize {
        8 + 4
            + 1
            + self.dead_letter_topic.as_ref().map_or(0, |s| 4 + s.len())
            + 8
            + 4
            + 1
            + self.max_pending_messages.map_or(0, |_| 8)
            + 1
            + self.max_pending_bytes.map_or(0, |_| 8)
            + 1
            + self.max_delayed_messages.map_or(0, |_| 8)
            + 1
            + self.max_delayed_bytes.map_or(0, |_| 8)
    }
}

impl Decode for AckVariantConfig {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            visibility_timeout_ms: u64::decode(r)?,
            max_retries: u32::decode(r)?,
            dead_letter_topic: decode_opt_string(r)?,
            delay_default_ms: u64::decode(r)?,
            max_in_flight_per_consumer: u32::decode(r)?,
            max_pending_messages: decode_opt_u64(r)?,
            max_pending_bytes: decode_opt_u64(r)?,
            max_delayed_messages: decode_opt_u64(r)?,
            max_delayed_bytes: decode_opt_u64(r)?,
        })
    }
}

// -- ActorVariantConfig --

impl Encode for ActorVariantConfig {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        self.max_mailbox_depth.encode(w)?;
        self.idle_eviction_secs.encode(w)?;
        self.ack_timeout_ms.encode(w)?;
        self.max_retries.encode(w)?;
        encode_opt_u64(w, self.max_pending_messages)?;
        encode_opt_u64(w, self.max_pending_bytes)
    }
    fn encoded_size(&self) -> usize {
        4 + 8
            + 8
            + 4
            + 1
            + self.max_pending_messages.map_or(0, |_| 8)
            + 1
            + self.max_pending_bytes.map_or(0, |_| 8)
    }
}

impl Decode for ActorVariantConfig {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            max_mailbox_depth: u32::decode(r)?,
            idle_eviction_secs: u64::decode(r)?,
            ack_timeout_ms: u64::decode(r)?,
            max_retries: u32::decode(r)?,
            max_pending_messages: decode_opt_u64(r)?,
            max_pending_bytes: decode_opt_u64(r)?,
        })
    }
}

// -- VariantConfig --

impl Encode for VariantConfig {
    fn encode<W: Write>(&self, w: &mut W) -> Result<(), CodecError> {
        match self {
            VariantConfig::Offset => 0u8.encode(w),
            VariantConfig::Ack(c) => {
                1u8.encode(w)?;
                c.encode(w)
            }
            VariantConfig::Actor(c) => {
                2u8.encode(w)?;
                c.encode(w)
            }
        }
    }
    fn encoded_size(&self) -> usize {
        1 + match self {
            VariantConfig::Offset => 0,
            VariantConfig::Ack(c) => c.encoded_size(),
            VariantConfig::Actor(c) => c.encoded_size(),
        }
    }
}

impl Decode for VariantConfig {
    fn decode<R: Read>(r: &mut R) -> Result<Self, CodecError> {
        match u8::decode(r)? {
            0 => Ok(VariantConfig::Offset),
            1 => Ok(VariantConfig::Ack(AckVariantConfig::decode(r)?)),
            2 => Ok(VariantConfig::Actor(ActorVariantConfig::decode(r)?)),
            t => Err(CodecError::InvalidDiscriminant(t)),
        }
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
            EntityStats::ConsumerGroup {
                group_id,
                variant,
                pending_count,
                in_flight_count,
                dlq_count,
                active_actor_count,
            } => {
                1u8.encode(w)?;
                group_id.encode(w)?;
                variant.encode(w)?;
                pending_count.encode(w)?;
                in_flight_count.encode(w)?;
                dlq_count.encode(w)?;
                active_actor_count.encode(w)
            }
        }
    }
    fn encoded_size(&self) -> usize {
        1 + match self {
            EntityStats::Topic { .. } => 4 * 8,
            EntityStats::ConsumerGroup { .. } => 8 + 1 + 4 * 8,
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
            1 => Ok(EntityStats::ConsumerGroup {
                group_id: u64::decode(r)?,
                variant: GroupVariant::decode(r)?,
                pending_count: u64::decode(r)?,
                in_flight_count: u64::decode(r)?,
                dlq_count: u64::decode(r)?,
                active_actor_count: u64::decode(r)?,
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

    pub fn set_retained(exchange_id: u64, routing_key: &str, message: &Bytes) -> Self {
        build_cmd!(
            Self::TAG_SET_RETAINED,
            |w: &mut Vec<u8>| exchange_id.encode(w),
            |w: &mut Vec<u8>| routing_key.to_string().encode(w),
            |w: &mut Vec<u8>| encode_bytes(w, message)
        )
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

    pub fn create_binding(exchange_id: u64, topic_id: u64, routing_key: Option<&str>) -> Self {
        build_cmd!(
            Self::TAG_CREATE_BINDING,
            |w: &mut Vec<u8>| exchange_id.encode(w),
            |w: &mut Vec<u8>| topic_id.encode(w),
            |w: &mut Vec<u8>| encode_opt_str(w, routing_key),
            |w: &mut Vec<u8>| false.encode(w),
            |w: &mut Vec<u8>| encode_opt_str(w, None),
            |w: &mut Vec<u8>| encode_opt_u32(w, None)
        )
    }

    pub fn create_binding_with_opts(
        exchange_id: u64,
        topic_id: u64,
        routing_key: Option<&str>,
        no_local: bool,
        shared_group: Option<&str>,
        subscription_id: Option<u32>,
    ) -> Self {
        build_cmd!(
            Self::TAG_CREATE_BINDING,
            |w: &mut Vec<u8>| exchange_id.encode(w),
            |w: &mut Vec<u8>| topic_id.encode(w),
            |w: &mut Vec<u8>| encode_opt_str(w, routing_key),
            |w: &mut Vec<u8>| no_local.encode(w),
            |w: &mut Vec<u8>| encode_opt_str(w, shared_group),
            |w: &mut Vec<u8>| encode_opt_u32(w, subscription_id)
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

    // -- Consumer Groups --

    /// Wire: [tag:1][name:str][auto_offset_reset:u8][variant_config:VariantConfig]
    ///        [auto_create_topic:u8][topic_retention:RetentionPolicy]
    ///        [topic_dedup:opt(TopicDedupConfig)][topic_lifetime:TopicLifetimePolicy]
    ///        [dlq_topic_name:opt_str][response_topic_name:opt_str]
    pub fn create_consumer_group_full(
        name: &str,
        auto_offset_reset: u8,
        variant_config: &VariantConfig,
        auto_create_topic: bool,
        topic_retention: &RetentionPolicy,
        topic_dedup: Option<&TopicDedupConfig>,
        topic_lifetime: TopicLifetimePolicy,
        dlq_topic_name: Option<&str>,
        response_topic_name: Option<&str>,
    ) -> Self {
        let mut buf = Vec::new();
        Self::TAG_CREATE_CONSUMER_GROUP.encode(&mut buf).unwrap();
        name.to_string().encode(&mut buf).unwrap();
        auto_offset_reset.encode(&mut buf).unwrap();
        variant_config.encode(&mut buf).unwrap();
        (auto_create_topic as u8).encode(&mut buf).unwrap();
        topic_retention.encode(&mut buf).unwrap();
        match topic_dedup {
            None => 0u8.encode(&mut buf).unwrap(),
            Some(d) => {
                1u8.encode(&mut buf).unwrap();
                d.encode(&mut buf).unwrap();
            }
        }
        topic_lifetime.encode(&mut buf).unwrap();
        encode_opt_str(&mut buf, dlq_topic_name).unwrap();
        encode_opt_str(&mut buf, response_topic_name).unwrap();
        MqCommand {
            buf: Bytes::from(buf),
        }
    }

    /// Simple create_consumer_group (Offset variant, no auto-create topic).
    pub fn create_consumer_group(name: &str, auto_offset_reset: u8) -> Self {
        Self::create_consumer_group_full(
            name,
            auto_offset_reset,
            &VariantConfig::Offset,
            false,
            &RetentionPolicy::default(),
            None,
            TopicLifetimePolicy::Permanent,
            None,
            None,
        )
    }

    // -- Compound create convenience APIs --

    /// Create an Ack-variant consumer group with an auto-created source topic.
    ///
    /// - `enable_dlq`: if true, creates a DLQ topic; `dlq_name` overrides the
    ///   default `"{name}:dlq"`.
    /// - `enable_response`: if true, creates a response topic; `response_name`
    ///   overrides the default `"{name}:resp"`.
    pub fn create_queue(
        name: &str,
        config: AckVariantConfig,
        retention: RetentionPolicy,
        dedup: Option<&TopicDedupConfig>,
        enable_dlq: bool,
        dlq_name: Option<&str>,
        enable_response: bool,
        response_name: Option<&str>,
    ) -> Self {
        let dlq = if enable_dlq {
            Some(
                dlq_name
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("{}:dlq", name)),
            )
        } else {
            None
        };
        let resp = if enable_response {
            Some(
                response_name
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("{}:resp", name)),
            )
        } else {
            None
        };
        Self::create_consumer_group_full(
            name,
            1, // Latest
            &VariantConfig::Ack(config),
            true,
            &retention,
            dedup,
            TopicLifetimePolicy::Permanent,
            dlq.as_deref(),
            resp.as_deref(),
        )
    }

    /// Create an Actor-variant consumer group with an auto-created source topic.
    ///
    /// - `enable_response`: if true, creates a response topic; `response_name`
    ///   overrides the default `"{name}:resp"`.
    pub fn create_actor_group(
        name: &str,
        config: ActorVariantConfig,
        retention: RetentionPolicy,
        enable_response: bool,
        response_name: Option<&str>,
    ) -> Self {
        let resp = if enable_response {
            Some(
                response_name
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("{}:resp", name)),
            )
        } else {
            None
        };
        Self::create_consumer_group_full(
            name,
            1, // Latest
            &VariantConfig::Actor(config),
            true,
            &retention,
            None,
            TopicLifetimePolicy::Permanent,
            None,
            resp.as_deref(),
        )
    }

    /// Create an Ack-variant consumer group with cron auto-publish on the source topic.
    /// The topic is auto-created with the given cron config; the Ack group consumes triggers.
    pub fn create_job_cron(
        name: &str,
        ack_config: AckVariantConfig,
        retention: RetentionPolicy,
    ) -> Self {
        Self::create_consumer_group_full(
            name,
            1, // Latest
            &VariantConfig::Ack(ack_config),
            true,
            &retention,
            None,
            TopicLifetimePolicy::Permanent,
            None,
            None,
        )
    }

    pub fn delete_consumer_group(group_id: u64) -> Self {
        build_cmd!(Self::TAG_DELETE_CONSUMER_GROUP, |w: &mut Vec<u8>| group_id
            .encode(w))
    }

    pub fn commit_group_offset(
        group_id: u64,
        generation: i32,
        topic_id: u64,
        partition_index: u32,
        offset: u64,
        metadata: Option<&str>,
        timestamp: u64,
    ) -> Self {
        build_cmd!(
            Self::TAG_COMMIT_GROUP_OFFSET,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| generation.encode(w),
            |w: &mut Vec<u8>| topic_id.encode(w),
            |w: &mut Vec<u8>| partition_index.encode(w),
            |w: &mut Vec<u8>| offset.encode(w),
            |w: &mut Vec<u8>| encode_opt_str(w, metadata),
            |w: &mut Vec<u8>| timestamp.encode(w)
        )
    }

    pub fn join_consumer_group(
        group_id: u64,
        member_id: &str,
        client_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        protocol_type: &str,
        protocols: &[(&str, &[u8])],
    ) -> Self {
        let mut buf = Vec::new();
        Self::TAG_JOIN_CONSUMER_GROUP.encode(&mut buf).unwrap();
        group_id.encode(&mut buf).unwrap();
        member_id.to_string().encode(&mut buf).unwrap();
        client_id.to_string().encode(&mut buf).unwrap();
        session_timeout_ms.encode(&mut buf).unwrap();
        rebalance_timeout_ms.encode(&mut buf).unwrap();
        protocol_type.to_string().encode(&mut buf).unwrap();
        (protocols.len() as u32).encode(&mut buf).unwrap();
        for (name, meta) in protocols {
            name.to_string().encode(&mut buf).unwrap();
            encode_bytes(&mut buf, meta).unwrap();
        }
        MqCommand {
            buf: Bytes::from(buf),
        }
    }

    pub fn sync_consumer_group(
        group_id: u64,
        generation: i32,
        member_id: &str,
        assignments: &[(&str, &[u8])],
    ) -> Self {
        let mut buf = Vec::new();
        Self::TAG_SYNC_CONSUMER_GROUP.encode(&mut buf).unwrap();
        group_id.encode(&mut buf).unwrap();
        generation.encode(&mut buf).unwrap();
        member_id.to_string().encode(&mut buf).unwrap();
        (assignments.len() as u32).encode(&mut buf).unwrap();
        for (mid, data) in assignments {
            mid.to_string().encode(&mut buf).unwrap();
            encode_bytes(&mut buf, data).unwrap();
        }
        MqCommand {
            buf: Bytes::from(buf),
        }
    }

    pub fn leave_consumer_group(group_id: u64, member_id: &str) -> Self {
        build_cmd!(
            Self::TAG_LEAVE_CONSUMER_GROUP,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| member_id.to_string().encode(w)
        )
    }

    pub fn heartbeat_consumer_group(group_id: u64, member_id: &str, generation: i32) -> Self {
        build_cmd!(
            Self::TAG_HEARTBEAT_CONSUMER_GROUP,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| member_id.to_string().encode(w),
            |w: &mut Vec<u8>| generation.encode(w)
        )
    }

    pub fn expire_group_sessions(now_ms: u64) -> Self {
        build_cmd!(Self::TAG_EXPIRE_GROUP_SESSIONS, |w: &mut Vec<u8>| {
            now_ms.encode(w)
        })
    }

    // -- Ack Variant --

    /// Wire: [tag:1][group_id:8][consumer_id:8][max_count:4]
    pub fn group_deliver(group_id: u64, consumer_id: u64, max_count: u32) -> Self {
        build_cmd!(
            Self::TAG_GROUP_DELIVER,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| consumer_id.encode(w),
            |w: &mut Vec<u8>| max_count.encode(w)
        )
    }

    /// Wire: [tag:1][group_id:8][vec_u64(message_ids)]
    pub fn group_ack(group_id: u64, message_ids: &[u64], response: Option<&Bytes>) -> Self {
        build_cmd!(
            Self::TAG_GROUP_ACK,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, message_ids),
            |w: &mut Vec<u8>| encode_opt_bytes_ref(w, response)
        )
    }

    /// Wire: [tag:1][group_id:8][vec_u64(message_ids)]
    pub fn group_nack(group_id: u64, message_ids: &[u64]) -> Self {
        build_cmd!(
            Self::TAG_GROUP_NACK,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, message_ids)
        )
    }

    /// Wire: [tag:1][group_id:8][vec_u64(message_ids)]
    pub fn group_release(group_id: u64, message_ids: &[u64]) -> Self {
        build_cmd!(
            Self::TAG_GROUP_RELEASE,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, message_ids)
        )
    }

    /// Wire: [tag:1][group_id:8][vec_u64(message_ids)]
    pub fn group_modify(group_id: u64, message_ids: &[u64]) -> Self {
        build_cmd!(
            Self::TAG_GROUP_MODIFY,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, message_ids)
        )
    }

    /// Wire: [tag:1][group_id:8][vec_u64(message_ids)][extension_ms:8]
    pub fn group_extend_visibility(group_id: u64, message_ids: &[u64], extension_ms: u64) -> Self {
        build_cmd!(
            Self::TAG_GROUP_EXTEND_VISIBILITY,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, message_ids),
            |w: &mut Vec<u8>| extension_ms.encode(w)
        )
    }

    /// Wire: [tag:1][group_id:8][vec_u64(message_ids)]
    pub fn group_timeout_expired(group_id: u64, message_ids: &[u64]) -> Self {
        build_cmd!(
            Self::TAG_GROUP_TIMEOUT_EXPIRED,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, message_ids)
        )
    }

    /// Wire: [tag:1][source_group_id:8][dlq_topic_id:8][vec_u64(dead_letter_ids)][vec_bytes(messages)]
    pub fn group_publish_to_dlq(
        source_group_id: u64,
        dlq_topic_id: u64,
        dead_letter_ids: &[u64],
        messages: &[Bytes],
    ) -> Self {
        build_cmd!(
            Self::TAG_GROUP_PUBLISH_TO_DLQ,
            |w: &mut Vec<u8>| source_group_id.encode(w),
            |w: &mut Vec<u8>| dlq_topic_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, dead_letter_ids),
            |w: &mut Vec<u8>| encode_vec_bytes(w, messages)
        )
    }

    /// Wire: [tag:1][group_id:8][vec_u64(message_ids)]
    pub fn group_expire_pending(group_id: u64, message_ids: &[u64]) -> Self {
        build_cmd!(
            Self::TAG_GROUP_EXPIRE_PENDING,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_u64(w, message_ids)
        )
    }

    /// Wire: [tag:1][group_id:8]
    pub fn group_purge(group_id: u64) -> Self {
        build_cmd!(Self::TAG_GROUP_PURGE, |w: &mut Vec<u8>| group_id.encode(w))
    }

    /// Wire: [tag:1][group_id:8]
    pub fn group_get_attributes(group_id: u64) -> Self {
        build_cmd!(Self::TAG_GROUP_GET_ATTRIBUTES, |w: &mut Vec<u8>| group_id
            .encode(w))
    }

    // -- Actor Variant --

    /// Wire: [tag:1][group_id:8][consumer_id:8][count:4][actor_ids...]
    pub fn group_deliver_actor(group_id: u64, consumer_id: u64, actor_ids: &[Bytes]) -> Self {
        build_cmd!(
            Self::TAG_GROUP_DELIVER_ACTOR,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| consumer_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_bytes(w, actor_ids)
        )
    }

    /// Wire: [tag:1][group_id:8][actor_id:len_prefixed][message_id:8][opt_response:opt_bytes]
    pub fn group_ack_actor(
        group_id: u64,
        actor_id: &[u8],
        message_id: u64,
        response: Option<&Bytes>,
    ) -> Self {
        build_cmd!(
            Self::TAG_GROUP_ACK_ACTOR,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| encode_bytes(w, actor_id),
            |w: &mut Vec<u8>| message_id.encode(w),
            |w: &mut Vec<u8>| encode_opt_bytes_ref(w, response)
        )
    }

    /// Wire: [tag:1][group_id:8][actor_id:len_prefixed][message_id:8]
    pub fn group_nack_actor(group_id: u64, actor_id: &[u8], message_id: u64) -> Self {
        build_cmd!(
            Self::TAG_GROUP_NACK_ACTOR,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| encode_bytes(w, actor_id),
            |w: &mut Vec<u8>| message_id.encode(w)
        )
    }

    /// Wire: [tag:1][group_id:8][consumer_id:8][vec_bytes(actor_ids)]
    pub fn group_assign_actors(group_id: u64, consumer_id: u64, actor_ids: &[Bytes]) -> Self {
        build_cmd!(
            Self::TAG_GROUP_ASSIGN_ACTORS,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| consumer_id.encode(w),
            |w: &mut Vec<u8>| encode_vec_bytes(w, actor_ids)
        )
    }

    /// Wire: [tag:1][group_id:8][consumer_id:8]
    pub fn group_release_actors(group_id: u64, consumer_id: u64) -> Self {
        build_cmd!(
            Self::TAG_GROUP_RELEASE_ACTORS,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| consumer_id.encode(w)
        )
    }

    /// Wire: [tag:1][group_id:8][before_timestamp:8]
    pub fn group_evict_idle(group_id: u64, before_timestamp: u64) -> Self {
        build_cmd!(
            Self::TAG_GROUP_EVICT_IDLE,
            |w: &mut Vec<u8>| group_id.encode(w),
            |w: &mut Vec<u8>| before_timestamp.encode(w)
        )
    }

    // -- Cron --

    /// Wire: [tag:1][topic_id:8]
    pub fn cron_enable(topic_id: u64) -> Self {
        build_cmd!(Self::TAG_CRON_ENABLE, |w: &mut Vec<u8>| topic_id.encode(w))
    }

    /// Wire: [tag:1][topic_id:8]
    pub fn cron_disable(topic_id: u64) -> Self {
        build_cmd!(Self::TAG_CRON_DISABLE, |w: &mut Vec<u8>| topic_id.encode(w))
    }

    /// Wire: [tag:1][topic_id:8][triggered_at:8]
    pub fn cron_trigger(topic_id: u64, triggered_at: u64) -> Self {
        build_cmd!(
            Self::TAG_CRON_TRIGGER,
            |w: &mut Vec<u8>| topic_id.encode(w),
            |w: &mut Vec<u8>| triggered_at.encode(w)
        )
    }

    /// Wire: [tag:1][topic_id:8]
    pub fn cron_update(topic_id: u64) -> Self {
        build_cmd!(Self::TAG_CRON_UPDATE, |w: &mut Vec<u8>| topic_id.encode(w))
    }

    // -- Sessions --

    /// Wire: [tag:1][session_id:8][client_id:str][keep_alive_ms:8][session_expiry_ms:8]
    pub fn create_session(
        session_id: u64,
        client_id: &str,
        keep_alive_ms: u64,
        session_expiry_ms: u64,
    ) -> Self {
        build_cmd!(
            Self::TAG_CREATE_SESSION,
            |w: &mut Vec<u8>| session_id.encode(w),
            |w: &mut Vec<u8>| client_id.to_string().encode(w),
            |w: &mut Vec<u8>| keep_alive_ms.encode(w),
            |w: &mut Vec<u8>| session_expiry_ms.encode(w)
        )
    }

    /// Wire: [tag:1][session_id:8][publish_will:1]
    pub fn disconnect_session(session_id: u64, publish_will: bool) -> Self {
        build_cmd!(
            Self::TAG_DISCONNECT_SESSION,
            |w: &mut Vec<u8>| session_id.encode(w),
            |w: &mut Vec<u8>| (publish_will as u8).encode(w)
        )
    }

    /// Wire: [tag:1][session_id:8]
    pub fn heartbeat_session(session_id: u64) -> Self {
        build_cmd!(Self::TAG_HEARTBEAT_SESSION, |w: &mut Vec<u8>| session_id
            .encode(w))
    }

    pub fn set_will(
        session_id: u64,
        topic_id: u64,
        delay_secs: u32,
        qos: u8,
        retain: bool,
        routing_key: &str,
        message: &Bytes,
    ) -> Self {
        build_cmd!(
            Self::TAG_SET_WILL,
            |w: &mut Vec<u8>| session_id.encode(w),
            |w: &mut Vec<u8>| topic_id.encode(w),
            |w: &mut Vec<u8>| delay_secs.encode(w),
            |w: &mut Vec<u8>| qos.encode(w),
            |w: &mut Vec<u8>| (retain as u8).encode(w),
            |w: &mut Vec<u8>| routing_key.to_string().encode(w),
            |w: &mut Vec<u8>| encode_bytes(w, message)
        )
    }

    pub fn clear_will(session_id: u64) -> Self {
        build_cmd!(Self::TAG_CLEAR_WILL, |w: &mut Vec<u8>| session_id.encode(w))
    }

    pub fn fire_pending_wills(now_ms: u64) -> Self {
        build_cmd!(Self::TAG_FIRE_PENDING_WILLS, |w: &mut Vec<u8>| now_ms
            .encode(w))
    }

    pub fn persist_session(
        session_id: u64,
        client_id: &str,
        session_expiry_secs: u32,
        subscription_data: &Bytes,
        inbound_qos_inflight: u32,
        outbound_qos1_count: u32,
        remaining_quota: u64,
    ) -> Self {
        build_cmd!(
            Self::TAG_PERSIST_SESSION,
            |w: &mut Vec<u8>| session_id.encode(w),
            |w: &mut Vec<u8>| client_id.to_string().encode(w),
            |w: &mut Vec<u8>| session_expiry_secs.encode(w),
            |w: &mut Vec<u8>| encode_bytes(w, subscription_data),
            |w: &mut Vec<u8>| inbound_qos_inflight.encode(w),
            |w: &mut Vec<u8>| outbound_qos1_count.encode(w),
            |w: &mut Vec<u8>| remaining_quota.encode(w)
        )
    }

    pub fn restore_session(client_id: &str) -> Self {
        build_cmd!(Self::TAG_RESTORE_SESSION, |w: &mut Vec<u8>| client_id
            .to_string()
            .encode(w))
    }

    pub fn expire_sessions(now_ms: u64) -> Self {
        build_cmd!(Self::TAG_EXPIRE_SESSIONS, |w: &mut Vec<u8>| now_ms
            .encode(w))
    }

    // -- Batch (length-prefixed sub-commands) --

    pub fn batch(commands: &[MqCommand]) -> Self {
        let mut buf = Vec::new();
        Self::TAG_BATCH.encode(&mut buf).unwrap();
        (commands.len() as u32).encode(&mut buf).unwrap();
        for cmd in commands {
            (cmd.buf.len() as u32).encode(&mut buf).unwrap();
            buf.extend_from_slice(&cmd.buf);
        }
        MqCommand {
            buf: Bytes::from(buf),
        }
    }

    // -- Dedup --

    /// Wire: [tag:1][topic_id:8][before_timestamp:8]
    pub fn prune_dedup_window(topic_id: u64, before_timestamp: u64) -> Self {
        build_cmd!(
            Self::TAG_PRUNE_DEDUP_WINDOW,
            |w: &mut Vec<u8>| topic_id.encode(w),
            |w: &mut Vec<u8>| before_timestamp.encode(w)
        )
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

    pub fn as_batch(&self) -> CmdBatch {
        CmdBatch {
            buf: self.buf.clone(),
        }
    }

    pub fn as_create_consumer_group(&self) -> CmdCreateConsumerGroup {
        CmdCreateConsumerGroup {
            buf: self.buf.clone(),
        }
    }

    pub fn as_commit_group_offset(&self) -> CmdCommitGroupOffset {
        CmdCommitGroupOffset {
            buf: self.buf.clone(),
        }
    }

    pub fn as_join_consumer_group(&self) -> CmdJoinConsumerGroup {
        CmdJoinConsumerGroup {
            buf: self.buf.clone(),
        }
    }

    pub fn as_sync_consumer_group(&self) -> CmdSyncConsumerGroup {
        CmdSyncConsumerGroup {
            buf: self.buf.clone(),
        }
    }

    pub fn as_leave_consumer_group(&self) -> CmdLeaveConsumerGroup {
        CmdLeaveConsumerGroup {
            buf: self.buf.clone(),
        }
    }

    pub fn as_heartbeat_consumer_group(&self) -> CmdHeartbeatConsumerGroup {
        CmdHeartbeatConsumerGroup {
            buf: self.buf.clone(),
        }
    }

    pub fn as_set_retained(&self) -> CmdSetRetained {
        CmdSetRetained {
            buf: self.buf.clone(),
        }
    }

    pub fn as_set_will(&self) -> CmdSetWill {
        CmdSetWill {
            buf: self.buf.clone(),
        }
    }

    pub fn as_persist_session(&self) -> CmdPersistSession {
        CmdPersistSession {
            buf: self.buf.clone(),
        }
    }

    pub fn as_restore_session(&self) -> CmdRestoreSession {
        CmdRestoreSession {
            buf: self.buf.clone(),
        }
    }

    pub fn as_publish_to_dlq(&self) -> CmdPublishToDlq {
        CmdPublishToDlq {
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

    pub fn topic_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[9..17].try_into().unwrap())
    }

    pub fn routing_key(&self) -> Option<String> {
        let mut cursor = std::io::Cursor::new(&self.buf[17..]);
        decode_opt_string(&mut cursor).unwrap_or(None)
    }

    pub fn no_local(&self) -> bool {
        let mut cursor = std::io::Cursor::new(&self.buf[17..]);
        let _ = decode_opt_string(&mut cursor); // skip routing_key
        let pos = cursor.position() as usize + 17;
        if pos < self.buf.len() {
            self.buf[pos] != 0
        } else {
            false
        }
    }

    pub fn shared_group(&self) -> Option<String> {
        let mut cursor = std::io::Cursor::new(&self.buf[17..]);
        let _ = decode_opt_string(&mut cursor); // skip routing_key
        let _ = u8::decode(&mut cursor); // skip no_local
        decode_opt_string(&mut cursor).unwrap_or(None)
    }

    pub fn subscription_id(&self) -> Option<u32> {
        let mut cursor = std::io::Cursor::new(&self.buf[17..]);
        let _ = decode_opt_string(&mut cursor); // skip routing_key
        let _ = u8::decode(&mut cursor); // skip no_local
        let _ = decode_opt_string(&mut cursor); // skip shared_group
        decode_opt_u32(&mut cursor).unwrap_or(None)
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

/// Zero-copy view over a PublishToDlq command.
/// Wire: [tag:1][source_group_id:8][dlq_topic_id:8][vec_u64][vec_bytes]
pub struct CmdPublishToDlq {
    buf: Bytes,
}

impl CmdPublishToDlq {
    pub fn source_group_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn dlq_topic_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[9..17].try_into().unwrap())
    }

    pub fn dead_letter_ids(&self) -> SmallVec<[u64; 8]> {
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
// Consumer Group view structs
// =============================================================================

/// Zero-copy view over a CreateConsumerGroup command.
/// Layout: `[11][name:str][auto_offset_reset:u8]`
pub struct CmdCreateConsumerGroup {
    buf: Bytes,
}

impl CmdCreateConsumerGroup {
    pub fn name(&self) -> &str {
        let len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[5..5 + len]).unwrap_or("")
    }

    pub fn auto_offset_reset(&self) -> u8 {
        let name_len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        self.buf[5 + name_len]
    }

    /// Cursor positioned after auto_offset_reset (start of extended fields).
    fn cursor_after_basic(&self) -> std::io::Cursor<&[u8]> {
        let name_len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        let pos = 5 + name_len + 1; // tag(1) + name_len(4) + name + auto_offset_reset(1)
        std::io::Cursor::new(&self.buf[pos..])
    }

    pub fn variant_config(&self) -> VariantConfig {
        let mut c = self.cursor_after_basic();
        VariantConfig::decode(&mut c).unwrap_or_default()
    }

    pub fn auto_create_topic(&self) -> bool {
        let mut c = self.cursor_after_basic();
        let _ = VariantConfig::decode(&mut c);
        u8::decode(&mut c).unwrap_or(0) != 0
    }

    pub fn topic_retention(&self) -> RetentionPolicy {
        let mut c = self.cursor_after_basic();
        let _ = VariantConfig::decode(&mut c);
        let _ = u8::decode(&mut c); // auto_create_topic
        RetentionPolicy::decode(&mut c).unwrap_or_default()
    }

    pub fn topic_dedup(&self) -> Option<TopicDedupConfig> {
        let mut c = self.cursor_after_basic();
        let _ = VariantConfig::decode(&mut c);
        let _ = u8::decode(&mut c); // auto_create_topic
        let _ = RetentionPolicy::decode(&mut c);
        match u8::decode(&mut c).unwrap_or(0) {
            0 => None,
            _ => TopicDedupConfig::decode(&mut c).ok(),
        }
    }

    pub fn topic_lifetime(&self) -> TopicLifetimePolicy {
        let mut c = self.cursor_after_basic();
        let _ = VariantConfig::decode(&mut c);
        let _ = u8::decode(&mut c); // auto_create_topic
        let _ = RetentionPolicy::decode(&mut c);
        // skip dedup opt
        if u8::decode(&mut c).unwrap_or(0) != 0 {
            let _ = TopicDedupConfig::decode(&mut c);
        }
        TopicLifetimePolicy::decode(&mut c).unwrap_or_default()
    }

    pub fn dlq_topic_name(&self) -> Option<String> {
        let mut c = self.cursor_after_basic();
        let _ = VariantConfig::decode(&mut c);
        let _ = u8::decode(&mut c); // auto_create_topic
        let _ = RetentionPolicy::decode(&mut c);
        if u8::decode(&mut c).unwrap_or(0) != 0 {
            let _ = TopicDedupConfig::decode(&mut c);
        }
        let _ = TopicLifetimePolicy::decode(&mut c);
        decode_opt_string(&mut c).unwrap_or(None)
    }

    pub fn response_topic_name(&self) -> Option<String> {
        let mut c = self.cursor_after_basic();
        let _ = VariantConfig::decode(&mut c);
        let _ = u8::decode(&mut c); // auto_create_topic
        let _ = RetentionPolicy::decode(&mut c);
        if u8::decode(&mut c).unwrap_or(0) != 0 {
            let _ = TopicDedupConfig::decode(&mut c);
        }
        let _ = TopicLifetimePolicy::decode(&mut c);
        let _ = decode_opt_string(&mut c); // skip dlq
        decode_opt_string(&mut c).unwrap_or(None)
    }
}

/// Zero-copy view over a CommitGroupOffset command.
/// Layout: `[17][group_id:u64][generation:i32][topic_id:u64][partition:u32][offset:u64][metadata:opt_str][timestamp:u64]`
pub struct CmdCommitGroupOffset {
    buf: Bytes,
}

impl CmdCommitGroupOffset {
    pub fn group_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn generation(&self) -> i32 {
        i32::from_le_bytes(self.buf[9..13].try_into().unwrap())
    }

    pub fn topic_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[13..21].try_into().unwrap())
    }

    pub fn partition_index(&self) -> u32 {
        u32::from_le_bytes(self.buf[21..25].try_into().unwrap())
    }

    pub fn offset(&self) -> u64 {
        u64::from_le_bytes(self.buf[25..33].try_into().unwrap())
    }

    pub fn metadata(&self) -> Option<&str> {
        let flag = self.buf[33];
        if flag == 0 {
            None
        } else {
            let len = u32::from_le_bytes(self.buf[34..38].try_into().unwrap()) as usize;
            Some(std::str::from_utf8(&self.buf[38..38 + len]).unwrap_or(""))
        }
    }

    pub fn timestamp(&self) -> u64 {
        let flag = self.buf[33];
        let offset = if flag == 0 {
            34
        } else {
            let len = u32::from_le_bytes(self.buf[34..38].try_into().unwrap()) as usize;
            38 + len
        };
        u64::from_le_bytes(self.buf[offset..offset + 8].try_into().unwrap())
    }
}

/// Zero-copy view over a JoinConsumerGroup command.
pub struct CmdJoinConsumerGroup {
    buf: Bytes,
}

impl CmdJoinConsumerGroup {
    pub fn group_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    fn member_id_range(&self) -> (usize, usize) {
        let len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        (13, len)
    }

    pub fn member_id(&self) -> &str {
        let (start, len) = self.member_id_range();
        std::str::from_utf8(&self.buf[start..start + len]).unwrap_or("")
    }

    fn client_id_offset(&self) -> usize {
        let (start, len) = self.member_id_range();
        start + len
    }

    pub fn client_id(&self) -> &str {
        let off = self.client_id_offset();
        let len = u32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[off + 4..off + 4 + len]).unwrap_or("")
    }

    fn timeouts_offset(&self) -> usize {
        let off = self.client_id_offset();
        let len = u32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap()) as usize;
        off + 4 + len
    }

    pub fn session_timeout_ms(&self) -> i32 {
        let off = self.timeouts_offset();
        i32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap())
    }

    pub fn rebalance_timeout_ms(&self) -> i32 {
        let off = self.timeouts_offset() + 4;
        i32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap())
    }

    fn protocol_type_offset(&self) -> usize {
        self.timeouts_offset() + 8
    }

    pub fn protocol_type(&self) -> &str {
        let off = self.protocol_type_offset();
        let len = u32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[off + 4..off + 4 + len]).unwrap_or("")
    }

    fn protocols_offset(&self) -> usize {
        let off = self.protocol_type_offset();
        let len = u32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap()) as usize;
        off + 4 + len
    }

    pub fn protocols_count(&self) -> u32 {
        let off = self.protocols_offset();
        u32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap())
    }

    pub fn protocols(&self) -> Vec<(String, Bytes)> {
        let count = self.protocols_count() as usize;
        let mut offset = self.protocols_offset() + 4;
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            let name_len =
                u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let name = std::str::from_utf8(&self.buf[offset..offset + name_len]).unwrap_or("");
            offset += name_len;
            let meta_len =
                u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let meta = self.buf.slice(offset..offset + meta_len);
            offset += meta_len;
            result.push((name.to_string(), meta));
        }
        result
    }
}

/// Zero-copy view over a SyncConsumerGroup command.
pub struct CmdSyncConsumerGroup {
    buf: Bytes,
}

impl CmdSyncConsumerGroup {
    pub fn group_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn generation(&self) -> i32 {
        i32::from_le_bytes(self.buf[9..13].try_into().unwrap())
    }

    fn member_id_range(&self) -> (usize, usize) {
        let len = u32::from_le_bytes(self.buf[13..17].try_into().unwrap()) as usize;
        (17, len)
    }

    pub fn member_id(&self) -> &str {
        let (start, len) = self.member_id_range();
        std::str::from_utf8(&self.buf[start..start + len]).unwrap_or("")
    }

    fn assignments_offset(&self) -> usize {
        let (start, len) = self.member_id_range();
        start + len
    }

    pub fn assignments_count(&self) -> u32 {
        let off = self.assignments_offset();
        u32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap())
    }

    pub fn assignments(&self) -> Vec<(String, Vec<u8>)> {
        let count = self.assignments_count() as usize;
        let mut offset = self.assignments_offset() + 4;
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            let mid_len =
                u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let mid = std::str::from_utf8(&self.buf[offset..offset + mid_len]).unwrap_or("");
            offset += mid_len;
            let data_len =
                u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let data = self.buf[offset..offset + data_len].to_vec();
            offset += data_len;
            result.push((mid.to_string(), data));
        }
        result
    }
}

/// Zero-copy view over a LeaveConsumerGroup command.
pub struct CmdLeaveConsumerGroup {
    buf: Bytes,
}

impl CmdLeaveConsumerGroup {
    pub fn group_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn member_id(&self) -> &str {
        let len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[13..13 + len]).unwrap_or("")
    }
}

/// Zero-copy view over a HeartbeatConsumerGroup command.
pub struct CmdHeartbeatConsumerGroup {
    buf: Bytes,
}

impl CmdHeartbeatConsumerGroup {
    pub fn group_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn member_id(&self) -> &str {
        let len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[13..13 + len]).unwrap_or("")
    }

    pub fn generation(&self) -> i32 {
        let len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        let off = 13 + len;
        i32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap())
    }
}

// =============================================================================
// Session / MQTT view structs
// =============================================================================

pub struct CmdSetRetained {
    buf: Bytes,
}

impl CmdSetRetained {
    pub fn exchange_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn routing_key(&self) -> &str {
        let len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[13..13 + len]).unwrap_or("")
    }

    pub fn message(&self) -> Bytes {
        let rk_len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        let offset = 13 + rk_len;
        let msg_len = u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap()) as usize;
        self.buf.slice(offset + 4..offset + 4 + msg_len)
    }
}

pub struct CmdSetWill {
    buf: Bytes,
}

impl CmdSetWill {
    pub fn consumer_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn exchange_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[9..17].try_into().unwrap())
    }

    pub fn delay_secs(&self) -> u32 {
        u32::from_le_bytes(self.buf[17..21].try_into().unwrap())
    }

    pub fn qos(&self) -> u8 {
        self.buf[21]
    }

    pub fn retain(&self) -> bool {
        self.buf[22] != 0
    }

    pub fn routing_key(&self) -> String {
        let len = u32::from_le_bytes(self.buf[23..27].try_into().unwrap()) as usize;
        String::from_utf8(self.buf[27..27 + len].to_vec()).unwrap_or_default()
    }

    pub fn message(&self) -> Bytes {
        let rk_len = u32::from_le_bytes(self.buf[23..27].try_into().unwrap()) as usize;
        let offset = 27 + rk_len;
        let msg_len = u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap()) as usize;
        self.buf.slice(offset + 4..offset + 4 + msg_len)
    }
}

pub struct CmdPersistSession {
    buf: Bytes,
}

impl CmdPersistSession {
    pub fn consumer_id(&self) -> u64 {
        u64::from_le_bytes(self.buf[1..9].try_into().unwrap())
    }

    pub fn client_id(&self) -> &str {
        let len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[13..13 + len]).unwrap_or("")
    }

    pub fn session_expiry_secs(&self) -> u32 {
        let cid_len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        let offset = 13 + cid_len;
        u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap())
    }

    pub fn subscription_data(&self) -> Bytes {
        let cid_len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        let offset = 13 + cid_len + 4;
        let data_len =
            u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap()) as usize;
        self.buf.slice(offset + 4..offset + 4 + data_len)
    }

    fn flow_control_offset(&self) -> usize {
        let cid_len = u32::from_le_bytes(self.buf[9..13].try_into().unwrap()) as usize;
        let sub_offset = 13 + cid_len + 4;
        let data_len =
            u32::from_le_bytes(self.buf[sub_offset..sub_offset + 4].try_into().unwrap()) as usize;
        sub_offset + 4 + data_len
    }

    pub fn inbound_qos_inflight(&self) -> u32 {
        let off = self.flow_control_offset();
        if off + 4 <= self.buf.len() {
            u32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap())
        } else {
            0
        }
    }

    pub fn outbound_qos1_count(&self) -> u32 {
        let off = self.flow_control_offset() + 4;
        if off + 4 <= self.buf.len() {
            u32::from_le_bytes(self.buf[off..off + 4].try_into().unwrap())
        } else {
            0
        }
    }

    pub fn remaining_quota(&self) -> u64 {
        let off = self.flow_control_offset() + 8;
        if off + 8 <= self.buf.len() {
            u64::from_le_bytes(self.buf[off..off + 8].try_into().unwrap())
        } else {
            0
        }
    }
}

pub struct CmdRestoreSession {
    buf: Bytes,
}

impl CmdRestoreSession {
    pub fn client_id(&self) -> &str {
        let len = u32::from_le_bytes(self.buf[1..5].try_into().unwrap()) as usize;
        std::str::from_utf8(&self.buf[5..5 + len]).unwrap_or("")
    }
}

// =============================================================================
// Iterators
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

/// Zero-copy iterator over length-prefixed messages in a flat command buffer.
pub struct FlatMessages {
    buf: Bytes,
    offset: usize,
    remaining: u32,
}

impl FlatMessages {
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
        MqCommand::TAG_SET_RETAINED => {
            let v = cmd.as_set_retained();
            write!(
                f,
                "SetRetained(exchange={}, rk={})",
                v.exchange_id(),
                v.routing_key()
            )
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
                "CreateBinding(exchange={}, topic={})",
                v.exchange_id(),
                v.topic_id()
            )
        }
        MqCommand::TAG_DELETE_BINDING => write!(f, "DeleteBinding({})", cmd.field_u64(1)),
        MqCommand::TAG_PUBLISH_TO_EXCHANGE => {
            let v = cmd.as_publish_to_exchange();
            write!(f, "PublishToExchange(exchange={})", v.exchange_id())
        }
        MqCommand::TAG_CREATE_CONSUMER_GROUP => {
            let v = cmd.as_create_consumer_group();
            write!(f, "CreateConsumerGroup(name={})", v.name())
        }
        MqCommand::TAG_DELETE_CONSUMER_GROUP => {
            write!(f, "DeleteConsumerGroup({})", cmd.field_u64(1))
        }
        MqCommand::TAG_JOIN_CONSUMER_GROUP => {
            let v = cmd.as_join_consumer_group();
            write!(
                f,
                "JoinConsumerGroup(group={}, member={}, client={})",
                v.group_id(),
                v.member_id(),
                v.client_id()
            )
        }
        MqCommand::TAG_SYNC_CONSUMER_GROUP => {
            let v = cmd.as_sync_consumer_group();
            write!(
                f,
                "SyncConsumerGroup(group={}, gen={}, member={})",
                v.group_id(),
                v.generation(),
                v.member_id()
            )
        }
        MqCommand::TAG_LEAVE_CONSUMER_GROUP => {
            let v = cmd.as_leave_consumer_group();
            write!(
                f,
                "LeaveConsumerGroup(group={}, member={})",
                v.group_id(),
                v.member_id()
            )
        }
        MqCommand::TAG_HEARTBEAT_CONSUMER_GROUP => {
            let v = cmd.as_heartbeat_consumer_group();
            write!(
                f,
                "HeartbeatConsumerGroup(group={}, member={}, gen={})",
                v.group_id(),
                v.member_id(),
                v.generation()
            )
        }
        MqCommand::TAG_COMMIT_GROUP_OFFSET => {
            let v = cmd.as_commit_group_offset();
            write!(
                f,
                "CommitGroupOffset(group={}, gen={}, topic={}, part={}, offset={})",
                v.group_id(),
                v.generation(),
                v.topic_id(),
                v.partition_index(),
                v.offset()
            )
        }
        MqCommand::TAG_EXPIRE_GROUP_SESSIONS => {
            write!(f, "ExpireGroupSessions(now={})", cmd.field_u64(1))
        }
        MqCommand::TAG_GROUP_DELIVER => {
            write!(
                f,
                "GroupDeliver(group={}, consumer={}, max={})",
                cmd.field_u64(1),
                cmd.field_u64(9),
                cmd.field_u32(17)
            )
        }
        MqCommand::TAG_GROUP_ACK => {
            write!(f, "GroupAck(group={})", cmd.field_u64(1))
        }
        MqCommand::TAG_GROUP_NACK => {
            write!(f, "GroupNack(group={})", cmd.field_u64(1))
        }
        MqCommand::TAG_GROUP_RELEASE => {
            write!(f, "GroupRelease(group={})", cmd.field_u64(1))
        }
        MqCommand::TAG_GROUP_MODIFY => {
            write!(f, "GroupModify(group={})", cmd.field_u64(1))
        }
        MqCommand::TAG_GROUP_EXTEND_VISIBILITY => {
            write!(f, "GroupExtendVisibility(group={})", cmd.field_u64(1))
        }
        MqCommand::TAG_GROUP_TIMEOUT_EXPIRED => {
            write!(f, "GroupTimeoutExpired(group={})", cmd.field_u64(1))
        }
        MqCommand::TAG_GROUP_PUBLISH_TO_DLQ => {
            let v = cmd.as_publish_to_dlq();
            write!(
                f,
                "GroupPublishToDlq(group={}, dlq_topic={})",
                v.source_group_id(),
                v.dlq_topic_id()
            )
        }
        MqCommand::TAG_GROUP_EXPIRE_PENDING => {
            write!(f, "GroupExpirePending(group={})", cmd.field_u64(1))
        }
        MqCommand::TAG_GROUP_PURGE => {
            write!(f, "GroupPurge(group={})", cmd.field_u64(1))
        }
        MqCommand::TAG_GROUP_GET_ATTRIBUTES => {
            write!(f, "GroupGetAttributes(group={})", cmd.field_u64(1))
        }
        MqCommand::TAG_GROUP_DELIVER_ACTOR => {
            write!(
                f,
                "GroupDeliverActor(group={}, consumer={})",
                cmd.field_u64(1),
                cmd.field_u64(9)
            )
        }
        MqCommand::TAG_GROUP_ACK_ACTOR => {
            write!(f, "GroupAckActor(group={})", cmd.field_u64(1))
        }
        MqCommand::TAG_GROUP_NACK_ACTOR => {
            write!(f, "GroupNackActor(group={})", cmd.field_u64(1))
        }
        MqCommand::TAG_GROUP_ASSIGN_ACTORS => {
            write!(
                f,
                "GroupAssignActors(group={}, consumer={})",
                cmd.field_u64(1),
                cmd.field_u64(9)
            )
        }
        MqCommand::TAG_GROUP_RELEASE_ACTORS => {
            write!(
                f,
                "GroupReleaseActors(group={}, consumer={})",
                cmd.field_u64(1),
                cmd.field_u64(9)
            )
        }
        MqCommand::TAG_GROUP_EVICT_IDLE => {
            write!(f, "GroupEvictIdle(group={})", cmd.field_u64(1))
        }
        MqCommand::TAG_CRON_ENABLE => {
            write!(f, "CronEnable(topic={})", cmd.field_u64(1))
        }
        MqCommand::TAG_CRON_DISABLE => {
            write!(f, "CronDisable(topic={})", cmd.field_u64(1))
        }
        MqCommand::TAG_CRON_TRIGGER => {
            write!(
                f,
                "CronTrigger(topic={}, at={})",
                cmd.field_u64(1),
                cmd.field_u64(9)
            )
        }
        MqCommand::TAG_CRON_UPDATE => {
            write!(f, "CronUpdate(topic={})", cmd.field_u64(1))
        }
        MqCommand::TAG_CREATE_SESSION => {
            write!(f, "CreateSession(session={})", cmd.field_u64(1))
        }
        MqCommand::TAG_DISCONNECT_SESSION => {
            write!(f, "DisconnectSession(session={})", cmd.field_u64(1))
        }
        MqCommand::TAG_HEARTBEAT_SESSION => {
            write!(f, "HeartbeatSession(session={})", cmd.field_u64(1))
        }
        MqCommand::TAG_SET_WILL => {
            let v = cmd.as_set_will();
            write!(
                f,
                "SetWill(session={}, topic={})",
                v.consumer_id(),
                v.exchange_id()
            )
        }
        MqCommand::TAG_CLEAR_WILL => {
            write!(f, "ClearWill(session={})", cmd.field_u64(1))
        }
        MqCommand::TAG_FIRE_PENDING_WILLS => {
            write!(f, "FirePendingWills(now={})", cmd.field_u64(1))
        }
        MqCommand::TAG_PERSIST_SESSION => {
            let v = cmd.as_persist_session();
            write!(
                f,
                "PersistSession(session={}, client={})",
                v.consumer_id(),
                v.client_id()
            )
        }
        MqCommand::TAG_RESTORE_SESSION => {
            let v = cmd.as_restore_session();
            write!(f, "RestoreSession(client={})", v.client_id())
        }
        MqCommand::TAG_EXPIRE_SESSIONS => {
            write!(f, "ExpireSessions(now={})", cmd.field_u64(1))
        }
        MqCommand::TAG_BATCH => {
            let v = cmd.as_batch();
            write!(f, "Batch(count={})", v.count())
        }
        MqCommand::TAG_PRUNE_DEDUP_WINDOW => {
            write!(
                f,
                "PruneDedupWindow(topic={}, before={})",
                cmd.field_u64(1),
                cmd.field_u64(9)
            )
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
                (messages.len() as u32).encode(w)?;
                for msg in messages.iter() {
                    msg.encode(w)?;
                }
                Ok(())
            }
            MqResponse::Published { base_offset, count } => {
                TAG_RESP_PUBLISHED.encode(w)?;
                base_offset.encode(w)?;
                count.encode(w)
            }
            MqResponse::Stats(stats) => {
                TAG_RESP_STATS.encode(w)?;
                stats.encode(w)
            }
            MqResponse::BatchResponse(resps) => {
                TAG_RESP_BATCH.encode(w)?;
                (resps.len() as u32).encode(w)?;
                for resp in (*resps).iter() {
                    resp.encode(w)?;
                }
                Ok(())
            }
            MqResponse::GroupJoined {
                generation,
                leader,
                member_id,
                protocol_name,
                is_leader,
                members,
                phase_complete,
            } => {
                TAG_RESP_GROUP_JOINED.encode(w)?;
                generation.encode(w)?;
                leader.encode(w)?;
                member_id.encode(w)?;
                protocol_name.encode(w)?;
                (*is_leader as u8).encode(w)?;
                (members.len() as u32).encode(w)?;
                for (mid, meta) in members {
                    mid.encode(w)?;
                    encode_bytes(w, meta)?;
                }
                (*phase_complete as u8).encode(w)
            }
            MqResponse::GroupSynced {
                assignment,
                phase_complete,
            } => {
                TAG_RESP_GROUP_SYNCED.encode(w)?;
                encode_bytes(w, assignment)?;
                (*phase_complete as u8).encode(w)
            }
            MqResponse::DeadLettered {
                dead_letter_ids,
                dlq_topic_id,
            } => {
                TAG_RESP_DEAD_LETTERED.encode(w)?;
                encode_vec_u64(w, dead_letter_ids)?;
                dlq_topic_id.encode(w)
            }
            MqResponse::RetainedMessages { messages } => {
                TAG_RESP_RETAINED_MESSAGES.encode(w)?;
                (messages.len() as u32).encode(w)?;
                for entry in messages {
                    encode_bytes(w, &entry.routing_key)?;
                    encode_bytes(w, &entry.message)?;
                }
                Ok(())
            }
            MqResponse::WillPending {
                session_id,
                delay_ms,
            } => {
                TAG_RESP_WILL_PENDING.encode(w)?;
                session_id.encode(w)?;
                delay_ms.encode(w)
            }
            MqResponse::SessionRestored {
                session_id,
                session_expiry_ms,
                subscription_data,
            } => {
                TAG_RESP_SESSION_RESTORED.encode(w)?;
                session_id.encode(w)?;
                session_expiry_ms.encode(w)?;
                encode_bytes(w, subscription_data)
            }
            MqResponse::SessionNotFound => TAG_RESP_SESSION_NOT_FOUND.encode(w),
            MqResponse::MultiMessages { groups } => {
                TAG_RESP_MULTI_MESSAGES.encode(w)?;
                (groups.len() as u32).encode(w)?;
                for (group_id, messages) in groups {
                    group_id.encode(w)?;
                    (messages.len() as u32).encode(w)?;
                    for msg in messages.iter() {
                        msg.encode(w)?;
                    }
                }
                Ok(())
            }
            MqResponse::TopicAliases { aliases } => {
                TAG_RESP_TOPIC_ALIASES.encode(w)?;
                (aliases.len() as u32).encode(w)?;
                for a in aliases {
                    a.alias.encode(w)?;
                    a.topic_name.encode(w)?;
                }
                Ok(())
            }
            MqResponse::WillsFired { count } => {
                TAG_RESP_WILLS_FIRED.encode(w)?;
                count.encode(w)
            }
        }
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            MqResponse::Ok => 0,
            MqResponse::Error(err) => err.encoded_size(),
            MqResponse::EntityCreated { .. } => 8,
            MqResponse::Messages { messages } => {
                4 + messages.iter().map(|m| m.encoded_size()).sum::<usize>()
            }
            MqResponse::Published { .. } => 8 + 8,
            MqResponse::Stats(stats) => stats.encoded_size(),
            MqResponse::BatchResponse(resps) => {
                4 + resps.iter().map(|r| r.encoded_size()).sum::<usize>()
            }
            MqResponse::GroupJoined {
                generation: _,
                leader,
                member_id,
                protocol_name,
                members,
                ..
            } => {
                4 + leader.encoded_size()
                    + member_id.encoded_size()
                    + protocol_name.encoded_size()
                    + 1 // is_leader
                    + 4 // members count
                    + members.iter().map(|(mid, meta)| mid.encoded_size() + 4 + meta.len()).sum::<usize>()
                    + 1 // phase_complete
            }
            MqResponse::GroupSynced { assignment, .. } => 4 + assignment.len() + 1,
            MqResponse::DeadLettered {
                dead_letter_ids, ..
            } => 4 + dead_letter_ids.len() * 8 + 8,
            MqResponse::RetainedMessages { messages } => {
                4 + messages
                    .iter()
                    .map(|e| 4 + e.routing_key.len() + 4 + e.message.len())
                    .sum::<usize>()
            }
            MqResponse::WillPending { .. } => 8 + 8,
            MqResponse::SessionRestored {
                subscription_data, ..
            } => 8 + 8 + 4 + subscription_data.len(),
            MqResponse::SessionNotFound => 0,
            MqResponse::MultiMessages { groups } => {
                4 + groups
                    .iter()
                    .map(|(_, msgs)| 8 + 4 + msgs.iter().map(|m| m.encoded_size()).sum::<usize>())
                    .sum::<usize>()
            }
            MqResponse::TopicAliases { aliases } => {
                4 + aliases
                    .iter()
                    .map(|a| 2 + a.topic_name.encoded_size())
                    .sum::<usize>()
            }
            MqResponse::WillsFired { .. } => 4,
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
            TAG_RESP_MESSAGES => {
                let count = u32::decode(r)? as usize;
                let mut messages = SmallVec::with_capacity(count.min(256));
                for _ in 0..count {
                    messages.push(DeliveredMessage::decode(r)?);
                }
                Ok(MqResponse::Messages { messages })
            }
            TAG_RESP_PUBLISHED => Ok(MqResponse::Published {
                base_offset: u64::decode(r)?,
                count: u64::decode(r)?,
            }),
            TAG_RESP_STATS => Ok(MqResponse::Stats(EntityStats::decode(r)?)),
            TAG_RESP_BATCH => {
                let count = u32::decode(r)? as usize;
                let mut resps = SmallVec::with_capacity(count.min(256));
                for _ in 0..count {
                    resps.push(MqResponse::decode(r)?);
                }
                Ok(MqResponse::BatchResponse(Box::new(resps)))
            }
            TAG_RESP_GROUP_JOINED => {
                let generation = i32::decode(r)?;
                let leader = String::decode(r)?;
                let member_id = String::decode(r)?;
                let protocol_name = String::decode(r)?;
                let is_leader = u8::decode(r)? != 0;
                let count = u32::decode(r)? as usize;
                let mut members = Vec::with_capacity(count.min(256));
                for _ in 0..count {
                    let mid = String::decode(r)?;
                    let meta = decode_bytes_owned(r)?;
                    members.push((mid, meta));
                }
                let phase_complete = u8::decode(r)? != 0;
                Ok(MqResponse::GroupJoined {
                    generation,
                    leader,
                    member_id,
                    protocol_name,
                    is_leader,
                    members,
                    phase_complete,
                })
            }
            TAG_RESP_GROUP_SYNCED => {
                let assignment = decode_bytes(r)?;
                let phase_complete = u8::decode(r)? != 0;
                Ok(MqResponse::GroupSynced {
                    assignment,
                    phase_complete,
                })
            }
            TAG_RESP_DEAD_LETTERED => Ok(MqResponse::DeadLettered {
                dead_letter_ids: decode_vec_u64(r)?,
                dlq_topic_id: u64::decode(r)?,
            }),
            TAG_RESP_RETAINED_MESSAGES => {
                let count = u32::decode(r)? as usize;
                let mut messages = Vec::with_capacity(count.min(1024));
                for _ in 0..count {
                    let routing_key = decode_bytes_owned(r)?;
                    let message = decode_bytes_owned(r)?;
                    messages.push(RetainedEntry {
                        routing_key,
                        message,
                    });
                }
                Ok(MqResponse::RetainedMessages { messages })
            }
            TAG_RESP_WILL_PENDING => Ok(MqResponse::WillPending {
                session_id: u64::decode(r)?,
                delay_ms: u64::decode(r)?,
            }),
            TAG_RESP_SESSION_RESTORED => Ok(MqResponse::SessionRestored {
                session_id: u64::decode(r)?,
                session_expiry_ms: u64::decode(r)?,
                subscription_data: decode_bytes_owned(r)?,
            }),
            TAG_RESP_SESSION_NOT_FOUND => Ok(MqResponse::SessionNotFound),
            TAG_RESP_MULTI_MESSAGES => {
                let count = u32::decode(r)? as usize;
                let mut groups = Vec::with_capacity(count.min(256));
                for _ in 0..count {
                    let group_id = u64::decode(r)?;
                    let msg_count = u32::decode(r)? as usize;
                    let mut messages = SmallVec::with_capacity(msg_count.min(256));
                    for _ in 0..msg_count {
                        messages.push(DeliveredMessage::decode(r)?);
                    }
                    groups.push((group_id, messages));
                }
                Ok(MqResponse::MultiMessages { groups })
            }
            TAG_RESP_TOPIC_ALIASES => {
                let count = u32::decode(r)? as usize;
                let mut aliases = Vec::with_capacity(count.min(256));
                for _ in 0..count {
                    aliases.push(TopicAliasEntry {
                        alias: u16::decode(r)?,
                        topic_name: String::decode(r)?,
                    });
                }
                Ok(MqResponse::TopicAliases { aliases })
            }
            TAG_RESP_WILLS_FIRED => Ok(MqResponse::WillsFired {
                count: u32::decode(r)?,
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
            MqCommand::heartbeat_session(99),
        ];
        let cmd = MqCommand::batch(&cmds);
        let v = cmd.as_batch();
        assert_eq!(v.count(), 3);
        let sub_cmds: Vec<MqCommand> = v.commands().collect();
        assert_eq!(sub_cmds.len(), 3);
        assert_eq!(sub_cmds[0].tag(), MqCommand::TAG_DELETE_TOPIC);
        assert_eq!(sub_cmds[1].tag(), MqCommand::TAG_PUBLISH);
        assert_eq!(sub_cmds[2].tag(), MqCommand::TAG_HEARTBEAT_SESSION);
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
                base_offset: 1,
                count: 3,
            },
            MqResponse::DeadLettered {
                dead_letter_ids: smallvec::smallvec![10, 20],
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
        assert_eq!(v.topic_id(), 2);
        assert_eq!(v.routing_key(), Some("routing.key".to_string()));
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
                MqCommand::heartbeat_session(99),
                "HeartbeatSession(session=99)",
            ),
        ];

        for (cmd, expected) in cases {
            assert_eq!(format!("{}", cmd), expected);
        }
    }

    #[test]
    fn consumer_group_command_roundtrips() {
        let cmd = MqCommand::create_consumer_group("my-group", 1);
        let v = cmd.as_create_consumer_group();
        assert_eq!(v.name(), "my-group");
        assert_eq!(v.auto_offset_reset(), 1);

        let cmd = MqCommand::delete_consumer_group(42);
        assert_eq!(cmd.field_u64(1), 42);

        let cmd = MqCommand::commit_group_offset(10, 3, 20, 0, 100, Some("md"), 5000);
        let v = cmd.as_commit_group_offset();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.generation(), 3);
        assert_eq!(v.topic_id(), 20);
        assert_eq!(v.partition_index(), 0);
        assert_eq!(v.offset(), 100);
        assert_eq!(v.metadata(), Some("md"));
        assert_eq!(v.timestamp(), 5000);

        let cmd = MqCommand::commit_group_offset(10, 3, 20, 0, 100, None, 5000);
        let v = cmd.as_commit_group_offset();
        assert_eq!(v.metadata(), None);
        assert_eq!(v.timestamp(), 5000);

        let cmd = MqCommand::join_consumer_group(
            10,
            "member-1",
            "client-1",
            30_000,
            60_000,
            "consumer",
            &[("range", b"\x01\x02"), ("roundrobin", b"\x03")],
        );
        let v = cmd.as_join_consumer_group();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.member_id(), "member-1");
        assert_eq!(v.client_id(), "client-1");
        assert_eq!(v.session_timeout_ms(), 30_000);
        assert_eq!(v.rebalance_timeout_ms(), 60_000);
        assert_eq!(v.protocol_type(), "consumer");
        let protocols = v.protocols();
        assert_eq!(protocols[0].0, "range");
        assert_eq!(protocols[1].0, "roundrobin");

        let cmd = MqCommand::sync_consumer_group(
            10,
            5,
            "member-1",
            &[("member-1", b"assign-1"), ("member-2", b"assign-2")],
        );
        let v = cmd.as_sync_consumer_group();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.generation(), 5);
        assert_eq!(v.member_id(), "member-1");
        let assignments = v.assignments();
        assert_eq!(assignments.len(), 2);

        let cmd = MqCommand::leave_consumer_group(10, "member-1");
        let v = cmd.as_leave_consumer_group();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.member_id(), "member-1");

        let cmd = MqCommand::heartbeat_consumer_group(10, "member-1", 7);
        let v = cmd.as_heartbeat_consumer_group();
        assert_eq!(v.group_id(), 10);
        assert_eq!(v.member_id(), "member-1");
        assert_eq!(v.generation(), 7);
    }

    #[test]
    fn consumer_group_response_roundtrips() {
        let resp = MqResponse::GroupJoined {
            generation: 3,
            leader: "m-1".to_string(),
            member_id: "m-2".to_string(),
            protocol_name: "range".to_string(),
            is_leader: false,
            members: vec![("m-1".to_string(), Bytes::from_static(&[1, 2, 3]))],
            phase_complete: true,
        };
        let decoded = roundtrip_resp(&resp);
        match decoded {
            MqResponse::GroupJoined {
                generation,
                leader,
                member_id,
                protocol_name,
                is_leader,
                members,
                phase_complete,
            } => {
                assert_eq!(generation, 3);
                assert_eq!(leader, "m-1");
                assert_eq!(member_id, "m-2");
                assert_eq!(protocol_name, "range");
                assert!(!is_leader);
                assert_eq!(members.len(), 1);
                assert!(phase_complete);
            }
            _ => panic!("wrong variant"),
        }

        let resp = MqResponse::GroupSynced {
            assignment: vec![4, 5, 6],
            phase_complete: false,
        };
        let decoded = roundtrip_resp(&resp);
        match decoded {
            MqResponse::GroupSynced {
                assignment,
                phase_complete,
            } => {
                assert_eq!(assignment, vec![4, 5, 6]);
                assert!(!phase_complete);
            }
            _ => panic!("wrong variant"),
        }

        let resp = MqResponse::Error(MqError::IllegalGeneration);
        match roundtrip_resp(&resp) {
            MqResponse::Error(MqError::IllegalGeneration) => {}
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn group_ack_variant_roundtrips() {
        let cmd = MqCommand::group_deliver(5, 10, 100);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_DELIVER);
        assert_eq!(cmd.field_u64(1), 5);
        assert_eq!(cmd.field_u64(9), 10);
        assert_eq!(cmd.field_u32(17), 100);

        let cmd = MqCommand::group_ack(5, &[1, 2, 3], None);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_ACK);
        assert_eq!(cmd.field_u64(1), 5);
        let mut cursor = std::io::Cursor::new(&cmd.as_bytes()[9..]);
        let ids = decode_cmd_vec_u64(&mut cursor);
        assert_eq!(ids.as_slice(), &[1, 2, 3]);

        let cmd = MqCommand::group_nack(7, &[10, 20]);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_NACK);

        let cmd = MqCommand::group_extend_visibility(3, &[1], 5000);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_EXTEND_VISIBILITY);

        let cmd = MqCommand::group_publish_to_dlq(1, 2, &[10], &[Bytes::from_static(b"dead")]);
        assert_eq!(cmd.tag(), MqCommand::TAG_GROUP_PUBLISH_TO_DLQ);
        let v = cmd.as_publish_to_dlq();
        assert_eq!(v.source_group_id(), 1);
        assert_eq!(v.dlq_topic_id(), 2);
    }

    #[test]
    fn session_command_roundtrips() {
        let cmd = MqCommand::create_session(1, "client-1", 30000, 3600000);
        assert_eq!(cmd.tag(), MqCommand::TAG_CREATE_SESSION);
        assert_eq!(cmd.field_u64(1), 1);

        let cmd = MqCommand::disconnect_session(1, true);
        assert_eq!(cmd.tag(), MqCommand::TAG_DISCONNECT_SESSION);
        assert_eq!(cmd.field_u64(1), 1);

        let cmd = MqCommand::heartbeat_session(1);
        assert_eq!(cmd.tag(), MqCommand::TAG_HEARTBEAT_SESSION);
        assert_eq!(cmd.field_u64(1), 1);
    }

    #[test]
    fn cron_command_roundtrips() {
        let cmd = MqCommand::cron_enable(5);
        assert_eq!(cmd.tag(), MqCommand::TAG_CRON_ENABLE);
        assert_eq!(cmd.field_u64(1), 5);

        let cmd = MqCommand::cron_disable(5);
        assert_eq!(cmd.tag(), MqCommand::TAG_CRON_DISABLE);

        let cmd = MqCommand::cron_trigger(5, 12345);
        assert_eq!(cmd.tag(), MqCommand::TAG_CRON_TRIGGER);
        assert_eq!(cmd.field_u64(1), 5);
        assert_eq!(cmd.field_u64(9), 12345);
    }
}

//! Zero-copy binary codec for Multi-Raft RPC messages.
//!
//! This module provides efficient serialization without serde dependencies,
//! using simple length-prefixed binary encoding with zero-copy reads where possible.
//!
//! ## Wire Format
//!
//! All multi-byte integers are encoded in little-endian format.
//! Strings and byte arrays are length-prefixed with a u32 length.
//! Options use a u8 tag (0 = None, 1 = Some).
//! Enums use a u8 discriminant followed by variant data.

use std::io::{self, Cursor, Read, Write};

/// Error type for codec operations
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid message type: {0}")]
    InvalidMessageType(u8),
    #[error("Buffer too small: need {needed}, have {have}")]
    BufferTooSmall { needed: usize, have: usize },
    #[error("Invalid UTF-8 string")]
    InvalidUtf8,
    #[error("Data corruption")]
    DataCorruption,
    #[error("Invalid discriminant: {0}")]
    InvalidDiscriminant(u8),
}

/// Trait for types that can be encoded to bytes
pub trait Encode {
    /// Encode self into the writer
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError>;

    /// Return the encoded size in bytes
    fn encoded_size(&self) -> usize;

    /// Encode to a new Vec<u8>
    fn encode_to_vec(&self) -> Result<Vec<u8>, CodecError> {
        let mut buf = Vec::with_capacity(self.encoded_size());
        self.encode(&mut buf)?;
        Ok(buf)
    }

    /// Encode into an existing Vec<u8>, clearing it first and reserving capacity.
    /// This avoids allocation when the buffer already has sufficient capacity.
    fn encode_into(&self, buf: &mut Vec<u8>) -> Result<(), CodecError> {
        buf.clear();
        buf.reserve(self.encoded_size());
        self.encode(buf)
    }
}

/// Trait for types that can be decoded from bytes
pub trait Decode: Sized {
    /// Decode from the reader
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError>;

    /// Decode from a byte slice
    fn decode_from_slice(data: &[u8]) -> Result<Self, CodecError> {
        let mut cursor = Cursor::new(data);
        Self::decode(&mut cursor)
    }
}

// =============================================================================
// Primitive implementations
// =============================================================================

impl Encode for u8 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_all(&[*self])?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for u8 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }
}

impl Encode for u16 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        2
    }
}

impl Decode for u16 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;
        Ok(u16::from_le_bytes(buf))
    }
}

impl Encode for u32 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        4
    }
}

impl Decode for u32 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }
}

impl Encode for u64 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        8
    }
}

impl Decode for u64 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }
}

impl Encode for i64 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        8
    }
}

impl Decode for i64 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }
}

impl Encode for bool {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        (*self as u8).encode(writer)
    }
    fn encoded_size(&self) -> usize {
        1
    }
}

impl Decode for bool {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(u8::decode(reader)? != 0)
    }
}

impl Encode for String {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let bytes = self.as_bytes();
        (bytes.len() as u32).encode(writer)?;
        writer.write_all(bytes)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        4 + self.len()
    }
}

impl Decode for String {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let len = u32::decode(reader)? as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        String::from_utf8(buf).map_err(|_| CodecError::InvalidUtf8)
    }
}

/// Wrapper for raw bytes with optimized serialization.
/// Use this instead of `Vec<u8>` for raw byte data to avoid
/// conflicting with the generic `Vec<T>` implementation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RawBytes(pub Vec<u8>);

impl From<Vec<u8>> for RawBytes {
    fn from(v: Vec<u8>) -> Self {
        RawBytes(v)
    }
}

impl From<RawBytes> for Vec<u8> {
    fn from(r: RawBytes) -> Self {
        r.0
    }
}

impl Encode for RawBytes {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        (self.0.len() as u32).encode(writer)?;
        writer.write_all(&self.0)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        4 + self.0.len()
    }
}

impl Decode for RawBytes {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let len = u32::decode(reader)? as usize;
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        Ok(RawBytes(buf))
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        match self {
            Some(v) => {
                1u8.encode(writer)?;
                v.encode(writer)?;
            }
            None => {
                0u8.encode(writer)?;
            }
        }
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        1 + self.as_ref().map(|v| v.encoded_size()).unwrap_or(0)
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let tag = u8::decode(reader)?;
        match tag {
            0 => Ok(None),
            1 => Ok(Some(T::decode(reader)?)),
            _ => Err(CodecError::DataCorruption),
        }
    }
}

impl<T: Encode> Encode for Vec<T> {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        (self.len() as u32).encode(writer)?;
        for item in self {
            item.encode(writer)?;
        }
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        4 + self.iter().map(|v| v.encoded_size()).sum::<usize>()
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let len = u32::decode(reader)? as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(T::decode(reader)?);
        }
        Ok(vec)
    }
}

impl<A: Encode, B: Encode> Encode for (A, B) {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.0.encode(writer)?;
        self.1.encode(writer)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        self.0.encoded_size() + self.1.encoded_size()
    }
}

impl<A: Decode, B: Decode> Decode for (A, B) {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok((A::decode(reader)?, B::decode(reader)?))
    }
}

// =============================================================================
// Raft type implementations
// =============================================================================

/// A leader ID with term and node_id (both u64 for ManiacRaftTypeConfig)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct LeaderId {
    pub term: u64,
    pub node_id: u64,
}

impl Encode for LeaderId {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.term.encode(writer)?;
        self.node_id.encode(writer)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        16 // 8 + 8
    }
}

impl Decode for LeaderId {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            term: u64::decode(reader)?,
            node_id: u64::decode(reader)?,
        })
    }
}

/// Committed leader ID (same as LeaderId for leader_id_adv mode)
pub type CommittedLeaderId = LeaderId;

/// Log ID with committed leader_id and index
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct LogId {
    pub leader_id: CommittedLeaderId,
    pub index: u64,
}

impl Encode for LogId {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.leader_id.encode(writer)?;
        self.index.encode(writer)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        24 // 16 + 8
    }
}

impl Decode for LogId {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            leader_id: CommittedLeaderId::decode(reader)?,
            index: u64::decode(reader)?,
        })
    }
}

/// Vote with leader_id and committed flag
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Vote {
    pub leader_id: LeaderId,
    pub committed: bool,
}

impl Encode for Vote {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.leader_id.encode(writer)?;
        self.committed.encode(writer)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        17 // 16 + 1
    }
}

impl Decode for Vote {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            leader_id: LeaderId::decode(reader)?,
            committed: bool::decode(reader)?,
        })
    }
}

/// Basic node with address string
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BasicNode {
    pub addr: String,
}

impl Encode for BasicNode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.addr.encode(writer)
    }
    fn encoded_size(&self) -> usize {
        self.addr.encoded_size()
    }
}

impl Decode for BasicNode {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            addr: String::decode(reader)?,
        })
    }
}

/// Entry payload discriminants
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryPayloadType {
    Blank = 0,
    Normal = 1,
    Membership = 2,
}

impl TryFrom<u8> for EntryPayloadType {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(EntryPayloadType::Blank),
            1 => Ok(EntryPayloadType::Normal),
            2 => Ok(EntryPayloadType::Membership),
            _ => Err(CodecError::InvalidDiscriminant(value)),
        }
    }
}

/// Entry payload - generic over the application data type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryPayload<D> {
    Blank,
    Normal(D),
    Membership(Membership),
}

impl<D: Encode> Encode for EntryPayload<D> {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        match self {
            EntryPayload::Blank => {
                (EntryPayloadType::Blank as u8).encode(writer)?;
            }
            EntryPayload::Normal(data) => {
                (EntryPayloadType::Normal as u8).encode(writer)?;
                data.encode(writer)?;
            }
            EntryPayload::Membership(membership) => {
                (EntryPayloadType::Membership as u8).encode(writer)?;
                membership.encode(writer)?;
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            EntryPayload::Blank => 0,
            EntryPayload::Normal(data) => data.encoded_size(),
            EntryPayload::Membership(membership) => membership.encoded_size(),
        }
    }
}

impl<D: Decode> Decode for EntryPayload<D> {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let tag = EntryPayloadType::try_from(u8::decode(reader)?)?;
        match tag {
            EntryPayloadType::Blank => Ok(EntryPayload::Blank),
            EntryPayloadType::Normal => Ok(EntryPayload::Normal(D::decode(reader)?)),
            EntryPayloadType::Membership => {
                Ok(EntryPayload::Membership(Membership::decode(reader)?))
            }
        }
    }
}

/// Membership configuration - simplified for binary encoding
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Membership {
    /// Node configurations: Vec<(node_id, BasicNode)>
    pub configs: Vec<Vec<u64>>,
    /// Node information mapping
    pub nodes: Vec<(u64, BasicNode)>,
}

impl Encode for Membership {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        // Encode configs as Vec<Vec<u64>>
        (self.configs.len() as u32).encode(writer)?;
        for config in &self.configs {
            (config.len() as u32).encode(writer)?;
            for node_id in config {
                node_id.encode(writer)?;
            }
        }
        // Encode nodes
        (self.nodes.len() as u32).encode(writer)?;
        for (node_id, node) in &self.nodes {
            node_id.encode(writer)?;
            node.encode(writer)?;
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        let configs_size: usize = 4 + self.configs.iter().map(|c| 4 + c.len() * 8).sum::<usize>();
        let nodes_size: usize = 4 + self
            .nodes
            .iter()
            .map(|(_, n)| 8 + n.encoded_size())
            .sum::<usize>();
        configs_size + nodes_size
    }
}

impl Decode for Membership {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let configs_len = u32::decode(reader)? as usize;
        let mut configs = Vec::with_capacity(configs_len);
        for _ in 0..configs_len {
            let config_len = u32::decode(reader)? as usize;
            let mut config = Vec::with_capacity(config_len);
            for _ in 0..config_len {
                config.push(u64::decode(reader)?);
            }
            configs.push(config);
        }
        let nodes_len = u32::decode(reader)? as usize;
        let mut nodes = Vec::with_capacity(nodes_len);
        for _ in 0..nodes_len {
            let node_id = u64::decode(reader)?;
            let node = BasicNode::decode(reader)?;
            nodes.push((node_id, node));
        }
        Ok(Self { configs, nodes })
    }
}

/// A log entry
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry<D> {
    pub log_id: LogId,
    pub payload: EntryPayload<D>,
}

impl<D: Encode> Encode for Entry<D> {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.log_id.encode(writer)?;
        self.payload.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.log_id.encoded_size() + self.payload.encoded_size()
    }
}

impl<D: Decode> Decode for Entry<D> {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            log_id: LogId::decode(reader)?,
            payload: EntryPayload::decode(reader)?,
        })
    }
}

/// Stored membership with log_id
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoredMembership {
    pub log_id: Option<LogId>,
    pub membership: Membership,
}

impl Encode for StoredMembership {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.log_id.encode(writer)?;
        self.membership.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.log_id.encoded_size() + self.membership.encoded_size()
    }
}

impl Decode for StoredMembership {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            log_id: Option::<LogId>::decode(reader)?,
            membership: Membership::decode(reader)?,
        })
    }
}

/// Snapshot metadata
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SnapshotMeta {
    pub last_log_id: Option<LogId>,
    pub last_membership: StoredMembership,
    pub snapshot_id: String,
}

impl Encode for SnapshotMeta {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.last_log_id.encode(writer)?;
        self.last_membership.encode(writer)?;
        self.snapshot_id.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.last_log_id.encoded_size()
            + self.last_membership.encoded_size()
            + self.snapshot_id.encoded_size()
    }
}

impl Decode for SnapshotMeta {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            last_log_id: Option::<LogId>::decode(reader)?,
            last_membership: StoredMembership::decode(reader)?,
            snapshot_id: String::decode(reader)?,
        })
    }
}

// =============================================================================
// RPC Message types
// =============================================================================

/// AppendEntries request
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesRequest<D> {
    pub vote: Vote,
    pub prev_log_id: Option<LogId>,
    pub entries: Vec<Entry<D>>,
    pub leader_commit: Option<LogId>,
}

impl<D: Encode> Encode for AppendEntriesRequest<D> {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.vote.encode(writer)?;
        self.prev_log_id.encode(writer)?;
        self.entries.encode(writer)?;
        self.leader_commit.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.vote.encoded_size()
            + self.prev_log_id.encoded_size()
            + self.entries.encoded_size()
            + self.leader_commit.encoded_size()
    }
}

impl<D: Decode> Decode for AppendEntriesRequest<D> {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            vote: Vote::decode(reader)?,
            prev_log_id: Option::<LogId>::decode(reader)?,
            entries: Vec::<Entry<D>>::decode(reader)?,
            leader_commit: Option::<LogId>::decode(reader)?,
        })
    }
}

/// AppendEntries response discriminants
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendEntriesResponseType {
    Success = 0,
    PartialSuccess = 1,
    Conflict = 2,
    HigherVote = 3,
}

impl TryFrom<u8> for AppendEntriesResponseType {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(AppendEntriesResponseType::Success),
            1 => Ok(AppendEntriesResponseType::PartialSuccess),
            2 => Ok(AppendEntriesResponseType::Conflict),
            3 => Ok(AppendEntriesResponseType::HigherVote),
            _ => Err(CodecError::InvalidDiscriminant(value)),
        }
    }
}

/// AppendEntries response
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendEntriesResponse {
    Success,
    PartialSuccess(Option<LogId>),
    Conflict,
    HigherVote(Vote),
}

impl Encode for AppendEntriesResponse {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        match self {
            AppendEntriesResponse::Success => {
                (AppendEntriesResponseType::Success as u8).encode(writer)?;
            }
            AppendEntriesResponse::PartialSuccess(log_id) => {
                (AppendEntriesResponseType::PartialSuccess as u8).encode(writer)?;
                log_id.encode(writer)?;
            }
            AppendEntriesResponse::Conflict => {
                (AppendEntriesResponseType::Conflict as u8).encode(writer)?;
            }
            AppendEntriesResponse::HigherVote(vote) => {
                (AppendEntriesResponseType::HigherVote as u8).encode(writer)?;
                vote.encode(writer)?;
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            AppendEntriesResponse::Success => 0,
            AppendEntriesResponse::PartialSuccess(log_id) => log_id.encoded_size(),
            AppendEntriesResponse::Conflict => 0,
            AppendEntriesResponse::HigherVote(vote) => vote.encoded_size(),
        }
    }
}

impl Decode for AppendEntriesResponse {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let tag = AppendEntriesResponseType::try_from(u8::decode(reader)?)?;
        match tag {
            AppendEntriesResponseType::Success => Ok(AppendEntriesResponse::Success),
            AppendEntriesResponseType::PartialSuccess => Ok(AppendEntriesResponse::PartialSuccess(
                Option::<LogId>::decode(reader)?,
            )),
            AppendEntriesResponseType::Conflict => Ok(AppendEntriesResponse::Conflict),
            AppendEntriesResponseType::HigherVote => {
                Ok(AppendEntriesResponse::HigherVote(Vote::decode(reader)?))
            }
        }
    }
}

/// Vote request
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRequest {
    pub vote: Vote,
    pub last_log_id: Option<LogId>,
}

impl Encode for VoteRequest {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.vote.encode(writer)?;
        self.last_log_id.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.vote.encoded_size() + self.last_log_id.encoded_size()
    }
}

impl Decode for VoteRequest {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            vote: Vote::decode(reader)?,
            last_log_id: Option::<LogId>::decode(reader)?,
        })
    }
}

/// Vote response
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteResponse {
    pub vote: Vote,
    pub vote_granted: bool,
    pub last_log_id: Option<LogId>,
}

impl Encode for VoteResponse {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.vote.encode(writer)?;
        self.vote_granted.encode(writer)?;
        self.last_log_id.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.vote.encoded_size() + 1 + self.last_log_id.encoded_size()
    }
}

impl Decode for VoteResponse {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            vote: Vote::decode(reader)?,
            vote_granted: bool::decode(reader)?,
            last_log_id: Option::<LogId>::decode(reader)?,
        })
    }
}

/// InstallSnapshot request
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotRequest {
    pub vote: Vote,
    pub meta: SnapshotMeta,
    pub offset: u64,
    pub data: RawBytes,
    pub done: bool,
}

impl Encode for InstallSnapshotRequest {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.vote.encode(writer)?;
        self.meta.encode(writer)?;
        self.offset.encode(writer)?;
        self.data.encode(writer)?;
        self.done.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.vote.encoded_size() + self.meta.encoded_size() + 8 + self.data.encoded_size() + 1
    }
}

impl Decode for InstallSnapshotRequest {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            vote: Vote::decode(reader)?,
            meta: SnapshotMeta::decode(reader)?,
            offset: u64::decode(reader)?,
            data: RawBytes::decode(reader)?,
            done: bool::decode(reader)?,
        })
    }
}

/// InstallSnapshot response
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotResponse {
    pub vote: Vote,
}

impl Encode for InstallSnapshotResponse {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.vote.encode(writer)
    }

    fn encoded_size(&self) -> usize {
        self.vote.encoded_size()
    }
}

impl Decode for InstallSnapshotResponse {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            vote: Vote::decode(reader)?,
        })
    }
}

// =============================================================================
// RPC Message wrapper
// =============================================================================

/// Message type discriminants for RPC protocol
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    AppendEntries = 1,
    Vote = 2,
    InstallSnapshot = 3,
    Response = 5,
    BatchResponse = 6,
    ErrorResponse = 7,
}

impl TryFrom<u8> for MessageType {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, CodecError> {
        match value {
            1 => Ok(MessageType::AppendEntries),
            2 => Ok(MessageType::Vote),
            3 => Ok(MessageType::InstallSnapshot),
            5 => Ok(MessageType::Response),
            6 => Ok(MessageType::BatchResponse),
            7 => Ok(MessageType::ErrorResponse),
            _ => Err(CodecError::InvalidMessageType(value)),
        }
    }
}

/// Response type discriminants
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseType {
    AppendEntries = 1,
    Vote = 2,
    InstallSnapshot = 3,
}

impl TryFrom<u8> for ResponseType {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ResponseType::AppendEntries),
            2 => Ok(ResponseType::Vote),
            3 => Ok(ResponseType::InstallSnapshot),
            _ => Err(CodecError::InvalidMessageType(value)),
        }
    }
}

/// Response message wrapper
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseMessage {
    AppendEntries(AppendEntriesResponse),
    Vote(VoteResponse),
    InstallSnapshot(InstallSnapshotResponse),
}

impl Encode for ResponseMessage {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        match self {
            ResponseMessage::AppendEntries(resp) => {
                (ResponseType::AppendEntries as u8).encode(writer)?;
                resp.encode(writer)?;
            }
            ResponseMessage::Vote(resp) => {
                (ResponseType::Vote as u8).encode(writer)?;
                resp.encode(writer)?;
            }
            ResponseMessage::InstallSnapshot(resp) => {
                (ResponseType::InstallSnapshot as u8).encode(writer)?;
                resp.encode(writer)?;
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            ResponseMessage::AppendEntries(resp) => resp.encoded_size(),
            ResponseMessage::Vote(resp) => resp.encoded_size(),
            ResponseMessage::InstallSnapshot(resp) => resp.encoded_size(),
        }
    }
}

impl Decode for ResponseMessage {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let tag = ResponseType::try_from(u8::decode(reader)?)?;
        match tag {
            ResponseType::AppendEntries => Ok(ResponseMessage::AppendEntries(
                AppendEntriesResponse::decode(reader)?,
            )),
            ResponseType::Vote => Ok(ResponseMessage::Vote(VoteResponse::decode(reader)?)),
            ResponseType::InstallSnapshot => Ok(ResponseMessage::InstallSnapshot(
                InstallSnapshotResponse::decode(reader)?,
            )),
        }
    }
}

/// RPC message wrapper for serialization over the wire.
///
/// Each variant includes a `request_id` for multiplexing support,
/// allowing multiple concurrent requests over the same connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcMessage<D> {
    /// Append entries request
    AppendEntries {
        request_id: u64,
        group_id: u64,
        rpc: AppendEntriesRequest<D>,
    },
    /// Vote request
    Vote {
        request_id: u64,
        group_id: u64,
        rpc: VoteRequest,
    },
    /// Install snapshot request
    InstallSnapshot {
        request_id: u64,
        group_id: u64,
        rpc: InstallSnapshotRequest,
    },
    /// Single response message
    Response {
        request_id: u64,
        message: ResponseMessage,
    },
    /// Batched response message
    BatchResponse {
        request_id: u64,
        responses: Vec<(u64, ResponseMessage)>,
    },
    /// Error response
    Error { request_id: u64, error: String },
}

impl<D> RpcMessage<D> {
    /// Get the request ID from any message variant
    pub fn request_id(&self) -> u64 {
        match self {
            RpcMessage::AppendEntries { request_id, .. } => *request_id,
            RpcMessage::Vote { request_id, .. } => *request_id,
            RpcMessage::InstallSnapshot { request_id, .. } => *request_id,
            RpcMessage::Response { request_id, .. } => *request_id,
            RpcMessage::BatchResponse { request_id, .. } => *request_id,
            RpcMessage::Error { request_id, .. } => *request_id,
        }
    }
}

impl<D: Encode> Encode for RpcMessage<D> {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        match self {
            RpcMessage::AppendEntries {
                request_id,
                group_id,
                rpc,
            } => {
                (MessageType::AppendEntries as u8).encode(writer)?;
                request_id.encode(writer)?;
                group_id.encode(writer)?;
                rpc.encode(writer)?;
            }
            RpcMessage::Vote {
                request_id,
                group_id,
                rpc,
            } => {
                (MessageType::Vote as u8).encode(writer)?;
                request_id.encode(writer)?;
                group_id.encode(writer)?;
                rpc.encode(writer)?;
            }
            RpcMessage::InstallSnapshot {
                request_id,
                group_id,
                rpc,
            } => {
                (MessageType::InstallSnapshot as u8).encode(writer)?;
                request_id.encode(writer)?;
                group_id.encode(writer)?;
                rpc.encode(writer)?;
            }
            RpcMessage::Response {
                request_id,
                message,
            } => {
                (MessageType::Response as u8).encode(writer)?;
                request_id.encode(writer)?;
                message.encode(writer)?;
            }
            RpcMessage::BatchResponse {
                request_id,
                responses,
            } => {
                (MessageType::BatchResponse as u8).encode(writer)?;
                request_id.encode(writer)?;
                responses.encode(writer)?;
            }
            RpcMessage::Error { request_id, error } => {
                (MessageType::ErrorResponse as u8).encode(writer)?;
                request_id.encode(writer)?;
                error.encode(writer)?;
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            RpcMessage::AppendEntries {
                request_id,
                group_id,
                rpc,
            } => request_id.encoded_size() + group_id.encoded_size() + rpc.encoded_size(),
            RpcMessage::Vote {
                request_id,
                group_id,
                rpc,
            } => request_id.encoded_size() + group_id.encoded_size() + rpc.encoded_size(),
            RpcMessage::InstallSnapshot {
                request_id,
                group_id,
                rpc,
            } => request_id.encoded_size() + group_id.encoded_size() + rpc.encoded_size(),
            RpcMessage::Response {
                request_id,
                message,
            } => request_id.encoded_size() + message.encoded_size(),
            RpcMessage::BatchResponse {
                request_id,
                responses,
            } => request_id.encoded_size() + responses.encoded_size(),
            RpcMessage::Error { request_id, error } => {
                request_id.encoded_size() + error.encoded_size()
            }
        }
    }
}

impl<D: Decode> Decode for RpcMessage<D> {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let msg_type = MessageType::try_from(u8::decode(reader)?)?;
        match msg_type {
            MessageType::AppendEntries => Ok(RpcMessage::AppendEntries {
                request_id: u64::decode(reader)?,
                group_id: u64::decode(reader)?,
                rpc: AppendEntriesRequest::decode(reader)?,
            }),
            MessageType::Vote => Ok(RpcMessage::Vote {
                request_id: u64::decode(reader)?,
                group_id: u64::decode(reader)?,
                rpc: VoteRequest::decode(reader)?,
            }),
            MessageType::InstallSnapshot => Ok(RpcMessage::InstallSnapshot {
                request_id: u64::decode(reader)?,
                group_id: u64::decode(reader)?,
                rpc: InstallSnapshotRequest::decode(reader)?,
            }),
            MessageType::Response => Ok(RpcMessage::Response {
                request_id: u64::decode(reader)?,
                message: ResponseMessage::decode(reader)?,
            }),
            MessageType::BatchResponse => Ok(RpcMessage::BatchResponse {
                request_id: u64::decode(reader)?,
                responses: Vec::<(u64, ResponseMessage)>::decode(reader)?,
            }),
            MessageType::ErrorResponse => Ok(RpcMessage::Error {
                request_id: u64::decode(reader)?,
                error: String::decode(reader)?,
            }),
        }
    }
}

// =============================================================================
// Conversion traits between codec types and maniac-raft types
// =============================================================================

/// Trait for converting from maniac-raft types to codec types
pub trait ToCodec<T> {
    fn to_codec(&self) -> T;
}

/// Trait for converting from codec types to maniac-raft types
pub trait FromCodec<T> {
    fn from_codec(codec: T) -> Self;
}

/// Trait for borrowing the raw payload bytes without allocation.
/// Implementors can provide zero-copy encoding instead of going through `ToCodec<RawBytes>`
/// which clones the data.
pub trait BorrowPayload {
    fn payload_bytes(&self) -> &[u8];
    fn payload_len(&self) -> usize {
        self.payload_bytes().len()
    }
}

impl BorrowPayload for RawBytes {
    fn payload_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl BorrowPayload for Vec<u8> {
    fn payload_bytes(&self) -> &[u8] {
        self
    }
}

// =============================================================================
// Conversions for ManiacRaftTypeConfig types
// =============================================================================

use openraft::RaftTypeConfig;

/// Convert maniac-raft LeaderId to codec LeaderId
impl<C> ToCodec<LeaderId> for openraft::impls::leader_id_adv::LeaderId<C>
where
    C: RaftTypeConfig<NodeId = u64, Term = u64>,
{
    fn to_codec(&self) -> LeaderId {
        LeaderId {
            term: self.term,
            node_id: self.node_id,
        }
    }
}

/// Convert codec LeaderId to maniac-raft LeaderId
impl<C> FromCodec<LeaderId> for openraft::impls::leader_id_adv::LeaderId<C>
where
    C: RaftTypeConfig<NodeId = u64, Term = u64>,
{
    fn from_codec(codec: LeaderId) -> Self {
        Self {
            term: codec.term,
            node_id: codec.node_id,
        }
    }
}

/// Convert maniac-raft LogId to codec LogId
/// Note: This requires C::LeaderId = leader_id_adv::LeaderId<C> which has Committed = Self
impl<C> ToCodec<LogId> for openraft::LogId<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn to_codec(&self) -> LogId {
        LogId {
            leader_id: LeaderId {
                term: self.leader_id.term,
                node_id: self.leader_id.node_id,
            },
            index: self.index,
        }
    }
}

/// Convert codec LogId to maniac-raft LogId
impl<C> FromCodec<LogId> for openraft::LogId<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn from_codec(codec: LogId) -> Self {
        Self {
            leader_id: openraft::impls::leader_id_adv::LeaderId::<C> {
                term: codec.leader_id.term,
                node_id: codec.leader_id.node_id,
            },
            index: codec.index,
        }
    }
}

/// Convert maniac-raft Vote to codec Vote
impl<C> ToCodec<Vote> for openraft::impls::Vote<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn to_codec(&self) -> Vote {
        Vote {
            leader_id: LeaderId {
                term: self.leader_id.term,
                node_id: self.leader_id.node_id,
            },
            committed: self.committed,
        }
    }
}

/// Convert codec Vote to maniac-raft Vote
impl<C> FromCodec<Vote> for openraft::impls::Vote<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn from_codec(codec: Vote) -> Self {
        Self {
            leader_id: openraft::impls::leader_id_adv::LeaderId::<C> {
                term: codec.leader_id.term,
                node_id: codec.leader_id.node_id,
            },
            committed: codec.committed,
        }
    }
}

/// Convert maniac-raft BasicNode to codec BasicNode
impl ToCodec<BasicNode> for openraft::impls::BasicNode {
    fn to_codec(&self) -> BasicNode {
        BasicNode {
            addr: self.addr.clone(),
        }
    }
}

/// Convert codec BasicNode to maniac-raft BasicNode
impl FromCodec<BasicNode> for openraft::impls::BasicNode {
    fn from_codec(codec: BasicNode) -> Self {
        Self { addr: codec.addr }
    }
}

/// Convert maniac-raft Membership to codec Membership
impl<C> ToCodec<Membership> for openraft::Membership<C>
where
    C: RaftTypeConfig<NodeId = u64, Node = openraft::impls::BasicNode>,
{
    fn to_codec(&self) -> Membership {
        let configs: Vec<Vec<u64>> = self
            .get_joint_config()
            .iter()
            .map(|c| c.iter().copied().collect())
            .collect();
        let nodes: Vec<(u64, BasicNode)> = self
            .nodes()
            .map(|(id, node)| {
                (
                    *id,
                    BasicNode {
                        addr: node.addr.clone(),
                    },
                )
            })
            .collect();
        Membership { configs, nodes }
    }
}

/// Convert codec Membership to maniac-raft Membership
impl<C> FromCodec<Membership> for openraft::Membership<C>
where
    C: RaftTypeConfig<NodeId = u64, Node = openraft::impls::BasicNode>,
{
    fn from_codec(codec: Membership) -> Self {
        use std::collections::{BTreeMap, BTreeSet};
        let configs: Vec<BTreeSet<u64>> = codec
            .configs
            .into_iter()
            .map(|c| c.into_iter().collect())
            .collect();
        let nodes: BTreeMap<u64, openraft::impls::BasicNode> = codec
            .nodes
            .into_iter()
            .map(|(id, n)| (id, openraft::impls::BasicNode { addr: n.addr }))
            .collect();
        // Use From impl which calls new_unchecked internally
        if configs.is_empty() {
            return openraft::Membership::default();
        }
        // Try to create a valid membership, fall back to default on error
        openraft::Membership::new(configs, nodes).unwrap_or_default()
    }
}

/// Convert maniac-raft EntryPayload to codec EntryPayload
impl<C, D> ToCodec<EntryPayload<D>> for openraft::EntryPayload<C>
where
    C: RaftTypeConfig<NodeId = u64, Node = openraft::impls::BasicNode>,
    C::D: ToCodec<D>,
{
    fn to_codec(&self) -> EntryPayload<D> {
        match self {
            openraft::EntryPayload::Blank => EntryPayload::Blank,
            openraft::EntryPayload::Normal(data) => EntryPayload::Normal(data.to_codec()),
            openraft::EntryPayload::Membership(m) => EntryPayload::Membership(m.to_codec()),
        }
    }
}

/// Convert codec EntryPayload to maniac-raft EntryPayload
impl<C, D> FromCodec<EntryPayload<D>> for openraft::EntryPayload<C>
where
    C: RaftTypeConfig<NodeId = u64, Node = openraft::impls::BasicNode>,
    C::D: FromCodec<D>,
{
    fn from_codec(codec: EntryPayload<D>) -> Self {
        match codec {
            EntryPayload::Blank => openraft::EntryPayload::Blank,
            EntryPayload::Normal(data) => openraft::EntryPayload::Normal(C::D::from_codec(data)),
            EntryPayload::Membership(m) => {
                openraft::EntryPayload::Membership(openraft::Membership::<C>::from_codec(m))
            }
        }
    }
}

/// Convert maniac-raft Entry to codec Entry
impl<C, D> ToCodec<Entry<D>> for openraft::impls::Entry<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::D: ToCodec<D>,
{
    fn to_codec(&self) -> Entry<D> {
        Entry {
            log_id: self.log_id.to_codec(),
            payload: self.payload.to_codec(),
        }
    }
}

/// Convert codec Entry to maniac-raft Entry
impl<C, D> FromCodec<Entry<D>> for openraft::impls::Entry<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::D: FromCodec<D>,
{
    fn from_codec(codec: Entry<D>) -> Self {
        use openraft::entry::RaftEntry;
        openraft::impls::Entry::new(
            openraft::LogId::<C>::from_codec(codec.log_id),
            openraft::EntryPayload::<C>::from_codec(codec.payload),
        )
    }
}

/// Convert maniac-raft StoredMembership to codec StoredMembership
impl<C> ToCodec<StoredMembership> for openraft::StoredMembership<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn to_codec(&self) -> StoredMembership {
        StoredMembership {
            log_id: self.log_id().map(|lid| lid.to_codec()),
            membership: self.membership().to_codec(),
        }
    }
}

/// Convert codec StoredMembership to maniac-raft StoredMembership
impl<C> FromCodec<StoredMembership> for openraft::StoredMembership<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn from_codec(codec: StoredMembership) -> Self {
        openraft::StoredMembership::new(
            codec
                .log_id
                .map(|lid| openraft::LogId::<C>::from_codec(lid)),
            openraft::Membership::<C>::from_codec(codec.membership),
        )
    }
}

/// Convert maniac-raft SnapshotMeta to codec SnapshotMeta
impl<C> ToCodec<SnapshotMeta> for openraft::storage::SnapshotMeta<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn to_codec(&self) -> SnapshotMeta {
        SnapshotMeta {
            last_log_id: self.last_log_id.as_ref().map(|lid| lid.to_codec()),
            last_membership: self.last_membership.to_codec(),
            snapshot_id: self.snapshot_id.clone(),
        }
    }
}

/// Convert codec SnapshotMeta to maniac-raft SnapshotMeta
impl<C> FromCodec<SnapshotMeta> for openraft::storage::SnapshotMeta<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn from_codec(codec: SnapshotMeta) -> Self {
        openraft::storage::SnapshotMeta {
            last_log_id: codec
                .last_log_id
                .map(|lid| openraft::LogId::<C>::from_codec(lid)),
            last_membership: openraft::StoredMembership::<C>::from_codec(codec.last_membership),
            snapshot_id: codec.snapshot_id,
        }
    }
}

// ==================================================================================
// Implementations for common data types (Vec<u8>, RawBytes)
// ==================================================================================

/// Vec<u8> to/from codec - identity conversion
impl ToCodec<RawBytes> for Vec<u8> {
    fn to_codec(&self) -> RawBytes {
        RawBytes(self.clone())
    }
}

impl FromCodec<RawBytes> for Vec<u8> {
    fn from_codec(codec: RawBytes) -> Self {
        codec.0
    }
}

/// RawBytes to itself - identity conversion
impl ToCodec<RawBytes> for RawBytes {
    fn to_codec(&self) -> RawBytes {
        self.clone()
    }
}

impl FromCodec<RawBytes> for RawBytes {
    fn from_codec(codec: RawBytes) -> Self {
        codec
    }
}

// =============================================================================
// Serialization helpers for storage
// =============================================================================

/// Serialize a Vote using zero-copy codec
pub fn serialize_vote<C>(vote: &openraft::impls::Vote<C>) -> Result<Vec<u8>, CodecError>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    vote.to_codec().encode_to_vec()
}

/// Deserialize a Vote using zero-copy codec
pub fn deserialize_vote<C>(data: &[u8]) -> Result<openraft::impls::Vote<C>, CodecError>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    let codec_vote = Vote::decode_from_slice(data)?;
    Ok(openraft::impls::Vote::<C>::from_codec(codec_vote))
}

/// Serialize a LogId using zero-copy codec
pub fn serialize_log_id<C>(log_id: &openraft::LogId<C>) -> Result<Vec<u8>, CodecError>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    log_id.to_codec().encode_to_vec()
}

/// Deserialize a LogId using zero-copy codec
pub fn deserialize_log_id<C>(data: &[u8]) -> Result<openraft::LogId<C>, CodecError>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    let codec_log_id = LogId::decode_from_slice(data)?;
    Ok(openraft::LogId::<C>::from_codec(codec_log_id))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u64_roundtrip() {
        let val: u64 = 0x123456789ABCDEF0;
        let encoded = val.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), 8);
        let decoded = u64::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_string_roundtrip() {
        let val = "Hello, World!".to_string();
        let encoded = val.encode_to_vec().unwrap();
        let decoded = String::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_option_roundtrip() {
        let some_val: Option<u64> = Some(42);
        let none_val: Option<u64> = None;

        let encoded_some = some_val.encode_to_vec().unwrap();
        let encoded_none = none_val.encode_to_vec().unwrap();

        assert_eq!(
            Option::<u64>::decode_from_slice(&encoded_some).unwrap(),
            some_val
        );
        assert_eq!(
            Option::<u64>::decode_from_slice(&encoded_none).unwrap(),
            none_val
        );
    }

    #[test]
    fn test_vec_roundtrip() {
        let val: Vec<u64> = vec![1, 2, 3, 4, 5];
        let encoded = val.encode_to_vec().unwrap();
        let decoded = Vec::<u64>::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_leader_id_roundtrip() {
        let val = LeaderId {
            term: 5,
            node_id: 10,
        };
        let encoded = val.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), 16);
        let decoded = LeaderId::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_log_id_roundtrip() {
        let val = LogId {
            leader_id: LeaderId {
                term: 5,
                node_id: 10,
            },
            index: 100,
        };
        let encoded = val.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), 24);
        let decoded = LogId::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_vote_roundtrip() {
        let val = Vote {
            leader_id: LeaderId {
                term: 5,
                node_id: 10,
            },
            committed: true,
        };
        let encoded = val.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), 17);
        let decoded = Vote::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_vote_request_roundtrip() {
        let val = VoteRequest {
            vote: Vote {
                leader_id: LeaderId {
                    term: 5,
                    node_id: 10,
                },
                committed: false,
            },
            last_log_id: Some(LogId {
                leader_id: LeaderId {
                    term: 4,
                    node_id: 10,
                },
                index: 50,
            }),
        };
        let encoded = val.encode_to_vec().unwrap();
        let decoded = VoteRequest::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_vote_response_roundtrip() {
        let val = VoteResponse {
            vote: Vote {
                leader_id: LeaderId {
                    term: 5,
                    node_id: 10,
                },
                committed: true,
            },
            vote_granted: true,
            last_log_id: None,
        };
        let encoded = val.encode_to_vec().unwrap();
        let decoded = VoteResponse::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_append_entries_request_roundtrip() {
        let val = AppendEntriesRequest::<Vec<u8>> {
            vote: Vote {
                leader_id: LeaderId {
                    term: 5,
                    node_id: 10,
                },
                committed: true,
            },
            prev_log_id: Some(LogId {
                leader_id: LeaderId {
                    term: 4,
                    node_id: 10,
                },
                index: 99,
            }),
            entries: vec![
                Entry {
                    log_id: LogId {
                        leader_id: LeaderId {
                            term: 5,
                            node_id: 10,
                        },
                        index: 100,
                    },
                    payload: EntryPayload::Normal(vec![1, 2, 3]),
                },
                Entry {
                    log_id: LogId {
                        leader_id: LeaderId {
                            term: 5,
                            node_id: 10,
                        },
                        index: 101,
                    },
                    payload: EntryPayload::Blank,
                },
            ],
            leader_commit: Some(LogId {
                leader_id: LeaderId {
                    term: 5,
                    node_id: 10,
                },
                index: 98,
            }),
        };
        let encoded = val.encode_to_vec().unwrap();
        let decoded = AppendEntriesRequest::<Vec<u8>>::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_append_entries_response_roundtrip() {
        // Test all variants
        let variants = vec![
            AppendEntriesResponse::Success,
            AppendEntriesResponse::PartialSuccess(Some(LogId {
                leader_id: LeaderId {
                    term: 5,
                    node_id: 10,
                },
                index: 100,
            })),
            AppendEntriesResponse::PartialSuccess(None),
            AppendEntriesResponse::Conflict,
            AppendEntriesResponse::HigherVote(Vote {
                leader_id: LeaderId {
                    term: 6,
                    node_id: 11,
                },
                committed: true,
            }),
        ];

        for val in variants {
            let encoded = val.encode_to_vec().unwrap();
            let decoded = AppendEntriesResponse::decode_from_slice(&encoded).unwrap();
            assert_eq!(val, decoded);
        }
    }

    #[test]
    fn test_rpc_message_roundtrip() {
        let msg = RpcMessage::<Vec<u8>>::Vote {
            request_id: 12345,
            group_id: 1,
            rpc: VoteRequest {
                vote: Vote {
                    leader_id: LeaderId {
                        term: 5,
                        node_id: 10,
                    },
                    committed: false,
                },
                last_log_id: Some(LogId {
                    leader_id: LeaderId {
                        term: 4,
                        node_id: 10,
                    },
                    index: 50,
                }),
            },
        };

        let encoded = msg.encode_to_vec().unwrap();
        let decoded = RpcMessage::<Vec<u8>>::decode_from_slice(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_rpc_response_roundtrip() {
        let msg = RpcMessage::<Vec<u8>>::Response {
            request_id: 12345,
            message: ResponseMessage::Vote(VoteResponse {
                vote: Vote {
                    leader_id: LeaderId {
                        term: 5,
                        node_id: 10,
                    },
                    committed: true,
                },
                vote_granted: true,
                last_log_id: None,
            }),
        };

        let encoded = msg.encode_to_vec().unwrap();
        let decoded = RpcMessage::<Vec<u8>>::decode_from_slice(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_membership_roundtrip() {
        let val = Membership {
            configs: vec![vec![1, 2, 3], vec![4, 5]],
            nodes: vec![
                (
                    1,
                    BasicNode {
                        addr: "127.0.0.1:5001".to_string(),
                    },
                ),
                (
                    2,
                    BasicNode {
                        addr: "127.0.0.1:5002".to_string(),
                    },
                ),
            ],
        };
        let encoded = val.encode_to_vec().unwrap();
        let decoded = Membership::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_entry_with_membership_roundtrip() {
        let val = Entry::<Vec<u8>> {
            log_id: LogId {
                leader_id: LeaderId {
                    term: 5,
                    node_id: 10,
                },
                index: 100,
            },
            payload: EntryPayload::Membership(Membership {
                configs: vec![vec![1, 2, 3]],
                nodes: vec![(
                    1,
                    BasicNode {
                        addr: "127.0.0.1:5001".to_string(),
                    },
                )],
            }),
        };
        let encoded = val.encode_to_vec().unwrap();
        let decoded = Entry::<Vec<u8>>::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_install_snapshot_roundtrip() {
        let val = InstallSnapshotRequest {
            vote: Vote {
                leader_id: LeaderId {
                    term: 5,
                    node_id: 10,
                },
                committed: true,
            },
            meta: SnapshotMeta {
                last_log_id: Some(LogId {
                    leader_id: LeaderId {
                        term: 5,
                        node_id: 10,
                    },
                    index: 100,
                }),
                last_membership: StoredMembership {
                    log_id: Some(LogId {
                        leader_id: LeaderId {
                            term: 5,
                            node_id: 10,
                        },
                        index: 50,
                    }),
                    membership: Membership {
                        configs: vec![vec![1, 2, 3]],
                        nodes: vec![(
                            1,
                            BasicNode {
                                addr: "127.0.0.1:5001".to_string(),
                            },
                        )],
                    },
                },
                snapshot_id: "snap-12345".to_string(),
            },
            offset: 0,
            data: RawBytes(vec![1, 2, 3, 4, 5, 6, 7, 8]),
            done: false,
        };
        let encoded = val.encode_to_vec().unwrap();
        let decoded = InstallSnapshotRequest::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_invalid_message_type() {
        let mut data = vec![0xFFu8]; // Invalid message type
        data.extend_from_slice(&0u64.to_le_bytes()); // request_id
        assert!(RpcMessage::<Vec<u8>>::decode_from_slice(&data).is_err());
    }

    #[test]
    fn test_invalid_response_type() {
        let mut data = vec![0xFFu8]; // Invalid response type
        assert!(ResponseMessage::decode_from_slice(&data).is_err());
    }

    #[test]
    fn test_invalid_option_tag() {
        let mut data = vec![2u8]; // Invalid tag (should be 0 or 1)
        data.extend_from_slice(&42u64.to_le_bytes());
        assert!(Option::<u64>::decode_from_slice(&data).is_err());
    }

    #[test]
    fn test_truncated_buffer() {
        // Valid u64 encoding is 8 bytes, test with only 4
        let data = vec![0x00, 0x00, 0x00, 0x00];
        assert!(u64::decode_from_slice(&data).is_err());
    }

    #[test]
    fn test_truncated_string() {
        // String encoding: len(4) + data
        // Test with incomplete length prefix
        let data = vec![0x05, 0x00, 0x00]; // Only 3 bytes for length
        assert!(String::decode_from_slice(&data).is_err());
    }

    #[test]
    fn test_truncated_string_data() {
        // String encoding with length but incomplete data
        let mut data = vec![0x05, 0x00, 0x00, 0x00]; // length = 5
        data.extend_from_slice(b"hel"); // Only 3 bytes instead of 5
        assert!(String::decode_from_slice(&data).is_err());
    }

    #[test]
    fn test_invalid_entry_payload_discriminant() {
        let mut data = vec![0xFFu8]; // Invalid EntryPayloadType
        assert!(EntryPayload::<Vec<u8>>::decode_from_slice(&data).is_err());
    }

    #[test]
    fn test_invalid_append_entries_response_discriminant() {
        let mut data = vec![0xFFu8]; // Invalid AppendEntriesResponseType
        assert!(AppendEntriesResponse::decode_from_slice(&data).is_err());
    }

    #[test]
    fn test_corrupted_vote() {
        let vote = Vote {
            leader_id: LeaderId {
                term: 5,
                node_id: 10,
            },
            committed: true,
        };
        let encoded = vote.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), 17);

        // Vote doesn't include CRC validation; the only reliable failure mode is truncation.
        let invalid_too_short = vec![0xFFu8; 16];
        assert!(Vote::decode_from_slice(&invalid_too_short).is_err());
    }

    #[test]
    fn test_corrupted_log_id() {
        // Test with invalid data length - LogId should be 24 bytes
        let invalid_data = vec![0xFFu8; 20]; // Too short
        assert!(LogId::decode_from_slice(&invalid_data).is_err());
    }

    #[test]
    fn test_empty_buffer() {
        assert!(u64::decode_from_slice(&[]).is_err());
        assert!(String::decode_from_slice(&[]).is_err());
        assert!(Option::<u64>::decode_from_slice(&[]).is_err());
    }

    #[test]
    fn test_invalid_utf8_string() {
        // Length prefix says 3 bytes, but provide invalid UTF-8
        let mut data = vec![0x03, 0x00, 0x00, 0x00]; // length = 3
        data.extend_from_slice(&[0xFF, 0xFE, 0xFD]); // Invalid UTF-8
        assert!(String::decode_from_slice(&data).is_err());
    }
}

//! Zero-copy binary codec for Multi-Raft RPC messages.
//!
//! This module provides efficient serialization by implementing `Encode`/`Decode`
//! directly on openraft types, without intermediate conversion types.
//!
//! ## Wire Format
//!
//! All multi-byte integers are encoded in little-endian format.
//! Strings and byte arrays are length-prefixed with a u32 length.
//! Options use a u8 tag (0 = None, 1 = Some).
//! Enums use a u8 discriminant followed by variant data.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Cursor, Read, Write};

use openraft::RaftTypeConfig;

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

    /// Decode from a `Bytes` buffer. Override this for zero-copy decoding from
    /// mmap-backed `Bytes`. The default implementation copies via `decode_from_slice`.
    fn decode_from_bytes(data: bytes::Bytes) -> Result<Self, CodecError> {
        Self::decode_from_slice(&data)
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

impl Encode for i32 {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        4
    }
}

impl Decode for i32 {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
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
// BorrowPayload trait for zero-copy storage writes
// =============================================================================

/// Trait for borrowing the raw payload bytes without allocation.
/// Implementors can provide zero-copy encoding of application data for the
/// storage fast path, avoiding full Encode overhead.
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
// Encode/Decode impls for openraft types
// =============================================================================

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

// --- LeaderId ---

impl<C> Encode for openraft::impls::leader_id_adv::LeaderId<C>
where
    C: RaftTypeConfig<NodeId = u64, Term = u64>,
{
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.term.encode(writer)?;
        self.node_id.encode(writer)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        16 // 8 + 8
    }
}

impl<C> Decode for openraft::impls::leader_id_adv::LeaderId<C>
where
    C: RaftTypeConfig<NodeId = u64, Term = u64>,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            term: u64::decode(reader)?,
            node_id: u64::decode(reader)?,
        })
    }
}

// --- LogId ---

impl<C> Encode for openraft::LogId<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.leader_id.term.encode(writer)?;
        self.leader_id.node_id.encode(writer)?;
        self.index.encode(writer)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        24 // 8 + 8 + 8
    }
}

impl<C> Decode for openraft::LogId<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let term = u64::decode(reader)?;
        let node_id = u64::decode(reader)?;
        let index = u64::decode(reader)?;
        Ok(Self {
            leader_id: openraft::impls::leader_id_adv::LeaderId::<C> { term, node_id },
            index,
        })
    }
}

// --- Vote ---

impl<C> Encode for openraft::impls::Vote<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.leader_id.encode(writer)?;
        self.committed.encode(writer)?;
        Ok(())
    }
    fn encoded_size(&self) -> usize {
        17 // 16 + 1
    }
}

impl<C> Decode for openraft::impls::Vote<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            leader_id: openraft::impls::leader_id_adv::LeaderId::<C>::decode(reader)?,
            committed: bool::decode(reader)?,
        })
    }
}

// --- BasicNode ---

impl Encode for openraft::impls::BasicNode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.addr.encode(writer)
    }
    fn encoded_size(&self) -> usize {
        self.addr.encoded_size()
    }
}

impl Decode for openraft::impls::BasicNode {
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            addr: String::decode(reader)?,
        })
    }
}

// --- Membership ---

impl<C> Encode for openraft::Membership<C>
where
    C: RaftTypeConfig<NodeId = u64, Node = openraft::impls::BasicNode>,
{
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        let configs = self.get_joint_config();
        (configs.len() as u32).encode(writer)?;
        for config in configs {
            (config.len() as u32).encode(writer)?;
            for &node_id in config {
                node_id.encode(writer)?;
            }
        }
        // Iterate twice to avoid collecting into Vec: once for count, once for data.
        let node_count = self.nodes().count();
        (node_count as u32).encode(writer)?;
        for (node_id, node) in self.nodes() {
            node_id.encode(writer)?;
            node.encode(writer)?;
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        let configs = self.get_joint_config();
        let configs_size: usize = 4 + configs.iter().map(|c| 4 + c.len() * 8).sum::<usize>();
        // Iterate directly — no Vec allocation needed for size computation.
        let nodes_size: usize = 4 + self
            .nodes()
            .map(|(_, n)| 8 + n.encoded_size())
            .sum::<usize>();
        configs_size + nodes_size
    }
}

impl<C> Decode for openraft::Membership<C>
where
    C: RaftTypeConfig<NodeId = u64, Node = openraft::impls::BasicNode>,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let configs_len = u32::decode(reader)? as usize;
        let mut configs = Vec::with_capacity(configs_len);
        for _ in 0..configs_len {
            let config_len = u32::decode(reader)? as usize;
            let mut config = BTreeSet::new();
            for _ in 0..config_len {
                config.insert(u64::decode(reader)?);
            }
            configs.push(config);
        }
        let nodes_len = u32::decode(reader)? as usize;
        let mut nodes = BTreeMap::new();
        for _ in 0..nodes_len {
            let node_id = u64::decode(reader)?;
            let node = openraft::impls::BasicNode::decode(reader)?;
            nodes.insert(node_id, node);
        }
        if configs.is_empty() {
            return Ok(openraft::Membership::default());
        }
        Ok(openraft::Membership::new(configs, nodes).unwrap_or_default())
    }
}

// --- EntryPayload ---

impl<C> Encode for openraft::EntryPayload<C>
where
    C: RaftTypeConfig<NodeId = u64, Node = openraft::impls::BasicNode>,
    C::D: Encode,
{
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        match self {
            openraft::EntryPayload::Blank => {
                (EntryPayloadType::Blank as u8).encode(writer)?;
            }
            openraft::EntryPayload::Normal(data) => {
                (EntryPayloadType::Normal as u8).encode(writer)?;
                data.encode(writer)?;
            }
            openraft::EntryPayload::Membership(membership) => {
                (EntryPayloadType::Membership as u8).encode(writer)?;
                membership.encode(writer)?;
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            openraft::EntryPayload::Blank => 0,
            openraft::EntryPayload::Normal(data) => data.encoded_size(),
            openraft::EntryPayload::Membership(membership) => membership.encoded_size(),
        }
    }
}

impl<C> Decode for openraft::EntryPayload<C>
where
    C: RaftTypeConfig<NodeId = u64, Node = openraft::impls::BasicNode>,
    C::D: Decode,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let tag = EntryPayloadType::try_from(u8::decode(reader)?)?;
        match tag {
            EntryPayloadType::Blank => Ok(openraft::EntryPayload::Blank),
            EntryPayloadType::Normal => Ok(openraft::EntryPayload::Normal(C::D::decode(reader)?)),
            EntryPayloadType::Membership => Ok(openraft::EntryPayload::Membership(
                openraft::Membership::<C>::decode(reader)?,
            )),
        }
    }

    fn decode_from_bytes(data: bytes::Bytes) -> Result<Self, CodecError> {
        if data.is_empty() {
            return Err(CodecError::BufferTooSmall { needed: 1, have: 0 });
        }
        let tag = EntryPayloadType::try_from(data[0])?;
        match tag {
            EntryPayloadType::Blank => Ok(openraft::EntryPayload::Blank),
            EntryPayloadType::Normal => {
                let payload_bytes = data.slice(1..);
                Ok(openraft::EntryPayload::Normal(C::D::decode_from_bytes(
                    payload_bytes,
                )?))
            }
            EntryPayloadType::Membership => {
                let payload_bytes = data.slice(1..);
                Ok(openraft::EntryPayload::Membership(
                    openraft::Membership::<C>::decode_from_slice(&payload_bytes)?,
                ))
            }
        }
    }
}

// --- Entry ---

impl<C> Encode for openraft::impls::Entry<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::D: Encode,
{
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.log_id.encode(writer)?;
        self.payload.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.log_id.encoded_size() + self.payload.encoded_size()
    }
}

impl<C> Decode for openraft::impls::Entry<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::D: Decode,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            log_id: openraft::LogId::<C>::decode(reader)?,
            payload: openraft::EntryPayload::<C>::decode(reader)?,
        })
    }

    fn decode_from_bytes(data: bytes::Bytes) -> Result<Self, CodecError> {
        // Entry layout: [term:8][node_id:8][index:8][payload_tag:1][payload...]
        if data.len() < 25 {
            return Err(CodecError::BufferTooSmall {
                needed: 25,
                have: data.len(),
            });
        }
        let term = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let node_id = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let index = u64::from_le_bytes(data[16..24].try_into().unwrap());

        let log_id = openraft::LogId::<C> {
            leader_id: openraft::impls::leader_id_adv::LeaderId::<C> { term, node_id },
            index,
        };

        let payload_bytes = data.slice(24..);
        let payload = openraft::EntryPayload::<C>::decode_from_bytes(payload_bytes)?;

        Ok(Self { log_id, payload })
    }
}

// --- StoredMembership ---

impl<C> Encode for openraft::StoredMembership<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        // log_id() returns Option<&LogId<C>> — encode manually
        match self.log_id() {
            Some(lid) => {
                1u8.encode(writer)?;
                lid.encode(writer)?;
            }
            None => {
                0u8.encode(writer)?;
            }
        }
        self.membership().encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        let lid_size = match self.log_id() {
            Some(lid) => 1 + lid.encoded_size(),
            None => 1,
        };
        lid_size + self.membership().encoded_size()
    }
}

impl<C> Decode for openraft::StoredMembership<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let log_id = Option::<openraft::LogId<C>>::decode(reader)?;
        let membership = openraft::Membership::<C>::decode(reader)?;
        Ok(openraft::StoredMembership::new(log_id, membership))
    }
}

// --- SnapshotMeta ---

impl<C> Encode for openraft::storage::SnapshotMeta<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
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

impl<C> Decode for openraft::storage::SnapshotMeta<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(openraft::storage::SnapshotMeta {
            last_log_id: Option::<openraft::LogId<C>>::decode(reader)?,
            last_membership: openraft::StoredMembership::<C>::decode(reader)?,
            snapshot_id: String::decode(reader)?,
        })
    }
}

// =============================================================================
// RPC request/response types — Encode/Decode on openraft types
// =============================================================================

// --- AppendEntriesRequest ---

impl<C> Encode for openraft::raft::AppendEntriesRequest<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Encode,
    C::Entry: Encode,
{
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

impl<C> Decode for openraft::raft::AppendEntriesRequest<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Decode,
    C::Entry: Decode,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            vote: C::Vote::decode(reader)?,
            prev_log_id: Option::<openraft::LogId<C>>::decode(reader)?,
            entries: Vec::<C::Entry>::decode(reader)?,
            leader_commit: Option::<openraft::LogId<C>>::decode(reader)?,
        })
    }
}

// --- AppendEntriesResponse ---

impl<C> Encode for openraft::raft::AppendEntriesResponse<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Encode,
{
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        match self {
            openraft::raft::AppendEntriesResponse::Success => {
                (AppendEntriesResponseType::Success as u8).encode(writer)?;
            }
            openraft::raft::AppendEntriesResponse::PartialSuccess(log_id) => {
                (AppendEntriesResponseType::PartialSuccess as u8).encode(writer)?;
                log_id.encode(writer)?;
            }
            openraft::raft::AppendEntriesResponse::Conflict => {
                (AppendEntriesResponseType::Conflict as u8).encode(writer)?;
            }
            openraft::raft::AppendEntriesResponse::HigherVote(vote) => {
                (AppendEntriesResponseType::HigherVote as u8).encode(writer)?;
                vote.encode(writer)?;
            }
        }
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        1 + match self {
            openraft::raft::AppendEntriesResponse::Success => 0,
            openraft::raft::AppendEntriesResponse::PartialSuccess(log_id) => log_id.encoded_size(),
            openraft::raft::AppendEntriesResponse::Conflict => 0,
            openraft::raft::AppendEntriesResponse::HigherVote(vote) => vote.encoded_size(),
        }
    }
}

impl<C> Decode for openraft::raft::AppendEntriesResponse<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Decode,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let tag = AppendEntriesResponseType::try_from(u8::decode(reader)?)?;
        match tag {
            AppendEntriesResponseType::Success => {
                Ok(openraft::raft::AppendEntriesResponse::Success)
            }
            AppendEntriesResponseType::PartialSuccess => {
                Ok(openraft::raft::AppendEntriesResponse::PartialSuccess(
                    Option::<openraft::LogId<C>>::decode(reader)?,
                ))
            }
            AppendEntriesResponseType::Conflict => {
                Ok(openraft::raft::AppendEntriesResponse::Conflict)
            }
            AppendEntriesResponseType::HigherVote => Ok(
                openraft::raft::AppendEntriesResponse::HigherVote(C::Vote::decode(reader)?),
            ),
        }
    }
}

// --- VoteRequest ---

impl<C> Encode for openraft::raft::VoteRequest<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Encode,
{
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.vote.encode(writer)?;
        self.last_log_id.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.vote.encoded_size() + self.last_log_id.encoded_size()
    }
}

impl<C> Decode for openraft::raft::VoteRequest<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Decode,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            vote: C::Vote::decode(reader)?,
            last_log_id: Option::<openraft::LogId<C>>::decode(reader)?,
        })
    }
}

// --- VoteResponse ---

impl<C> Encode for openraft::raft::VoteResponse<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Encode,
{
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

impl<C> Decode for openraft::raft::VoteResponse<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Decode,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            vote: C::Vote::decode(reader)?,
            vote_granted: bool::decode(reader)?,
            last_log_id: Option::<openraft::LogId<C>>::decode(reader)?,
        })
    }
}

// --- InstallSnapshotRequest ---
// Note: `data: Vec<u8>` is encoded as a raw byte blob (len + bytes),
// not element-by-element like the generic Vec<T> impl.

impl<C> Encode for openraft::raft::InstallSnapshotRequest<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Encode,
{
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.vote.encode(writer)?;
        self.meta.encode(writer)?;
        self.offset.encode(writer)?;
        // Encode data as raw bytes blob for performance
        (self.data.len() as u32).encode(writer)?;
        writer.write_all(&self.data)?;
        self.done.encode(writer)?;
        Ok(())
    }

    fn encoded_size(&self) -> usize {
        self.vote.encoded_size() + self.meta.encoded_size() + 8 + 4 + self.data.len() + 1
    }
}

impl<C> Decode for openraft::raft::InstallSnapshotRequest<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Decode,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let vote = C::Vote::decode(reader)?;
        let meta = openraft::storage::SnapshotMeta::<C>::decode(reader)?;
        let offset = u64::decode(reader)?;
        let data_len = u32::decode(reader)? as usize;
        let mut data = vec![0u8; data_len];
        reader.read_exact(&mut data)?;
        let done = bool::decode(reader)?;
        Ok(Self {
            vote,
            meta,
            offset,
            data,
            done,
        })
    }
}

// --- InstallSnapshotResponse ---

impl<C> Encode for openraft::raft::InstallSnapshotResponse<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Encode,
{
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.vote.encode(writer)
    }

    fn encoded_size(&self) -> usize {
        self.vote.encoded_size()
    }
}

impl<C> Decode for openraft::raft::InstallSnapshotResponse<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Decode,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        Ok(Self {
            vote: C::Vote::decode(reader)?,
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
#[derive(Debug)]
pub enum ResponseMessage<C: RaftTypeConfig> {
    AppendEntries(openraft::raft::AppendEntriesResponse<C>),
    Vote(openraft::raft::VoteResponse<C>),
    InstallSnapshot(openraft::raft::InstallSnapshotResponse<C>),
}

impl<C> Encode for ResponseMessage<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Encode,
{
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

impl<C> Decode for ResponseMessage<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Decode,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let tag = ResponseType::try_from(u8::decode(reader)?)?;
        match tag {
            ResponseType::AppendEntries => Ok(ResponseMessage::AppendEntries(
                openraft::raft::AppendEntriesResponse::<C>::decode(reader)?,
            )),
            ResponseType::Vote => Ok(ResponseMessage::Vote(
                openraft::raft::VoteResponse::<C>::decode(reader)?,
            )),
            ResponseType::InstallSnapshot => Ok(ResponseMessage::InstallSnapshot(
                openraft::raft::InstallSnapshotResponse::<C>::decode(reader)?,
            )),
        }
    }
}

/// RPC message wrapper for serialization over the wire.
///
/// Each variant includes a `request_id` for multiplexing support,
/// allowing multiple concurrent requests over the same connection.
#[derive(Debug)]
pub enum RpcMessage<C: RaftTypeConfig> {
    /// Append entries request
    AppendEntries {
        request_id: u64,
        group_id: u64,
        rpc: openraft::raft::AppendEntriesRequest<C>,
    },
    /// Vote request
    Vote {
        request_id: u64,
        group_id: u64,
        rpc: openraft::raft::VoteRequest<C>,
    },
    /// Install snapshot request
    InstallSnapshot {
        request_id: u64,
        group_id: u64,
        rpc: openraft::raft::InstallSnapshotRequest<C>,
    },
    /// Single response message
    Response {
        request_id: u64,
        message: ResponseMessage<C>,
    },
    /// Batched response message
    BatchResponse {
        request_id: u64,
        responses: Vec<(u64, ResponseMessage<C>)>,
    },
    /// Error response
    Error { request_id: u64, error: String },
}

impl<C: RaftTypeConfig> RpcMessage<C> {
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

impl<C> Encode for RpcMessage<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Encode,
    C::Entry: Encode,
{
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

impl<C> Decode for RpcMessage<C>
where
    C: RaftTypeConfig<
            NodeId = u64,
            Term = u64,
            Node = openraft::impls::BasicNode,
            LeaderId = openraft::impls::leader_id_adv::LeaderId<C>,
        >,
    C::Vote: Decode,
    C::Entry: Decode,
{
    fn decode<R: Read>(reader: &mut R) -> Result<Self, CodecError> {
        let msg_type = MessageType::try_from(u8::decode(reader)?)?;
        match msg_type {
            MessageType::AppendEntries => Ok(RpcMessage::AppendEntries {
                request_id: u64::decode(reader)?,
                group_id: u64::decode(reader)?,
                rpc: openraft::raft::AppendEntriesRequest::<C>::decode(reader)?,
            }),
            MessageType::Vote => Ok(RpcMessage::Vote {
                request_id: u64::decode(reader)?,
                group_id: u64::decode(reader)?,
                rpc: openraft::raft::VoteRequest::<C>::decode(reader)?,
            }),
            MessageType::InstallSnapshot => Ok(RpcMessage::InstallSnapshot {
                request_id: u64::decode(reader)?,
                group_id: u64::decode(reader)?,
                rpc: openraft::raft::InstallSnapshotRequest::<C>::decode(reader)?,
            }),
            MessageType::Response => Ok(RpcMessage::Response {
                request_id: u64::decode(reader)?,
                message: ResponseMessage::<C>::decode(reader)?,
            }),
            MessageType::BatchResponse => Ok(RpcMessage::BatchResponse {
                request_id: u64::decode(reader)?,
                responses: Vec::<(u64, ResponseMessage<C>)>::decode(reader)?,
            }),
            MessageType::ErrorResponse => Ok(RpcMessage::Error {
                request_id: u64::decode(reader)?,
                error: String::decode(reader)?,
            }),
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    type C = crate::test_support::TestConfig;

    fn make_leader_id(term: u64, node_id: u64) -> openraft::impls::leader_id_adv::LeaderId<C> {
        openraft::impls::leader_id_adv::LeaderId::<C> { term, node_id }
    }

    fn make_log_id(term: u64, node_id: u64, index: u64) -> openraft::LogId<C> {
        openraft::LogId::<C> {
            leader_id: make_leader_id(term, node_id),
            index,
        }
    }

    fn make_vote(term: u64, node_id: u64, committed: bool) -> openraft::impls::Vote<C> {
        openraft::impls::Vote::<C> {
            leader_id: make_leader_id(term, node_id),
            committed,
        }
    }

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
        let val = make_leader_id(5, 10);
        let encoded = val.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), 16);
        let decoded =
            openraft::impls::leader_id_adv::LeaderId::<C>::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_log_id_roundtrip() {
        let val = make_log_id(5, 10, 100);
        let encoded = val.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), 24);
        let decoded = openraft::LogId::<C>::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_vote_roundtrip() {
        let val = make_vote(5, 10, true);
        let encoded = val.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), 17);
        let decoded = openraft::impls::Vote::<C>::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_vote_request_roundtrip() {
        let val = openraft::raft::VoteRequest::<C> {
            vote: make_vote(5, 10, false),
            last_log_id: Some(make_log_id(4, 10, 50)),
        };
        let encoded = val.encode_to_vec().unwrap();
        let decoded = openraft::raft::VoteRequest::<C>::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_vote_response_roundtrip() {
        let val = openraft::raft::VoteResponse::<C> {
            vote: make_vote(5, 10, true),
            vote_granted: true,
            last_log_id: None,
        };
        let encoded = val.encode_to_vec().unwrap();
        let decoded = openraft::raft::VoteResponse::<C>::decode_from_slice(&encoded).unwrap();
        assert_eq!(val, decoded);
    }

    #[test]
    fn test_append_entries_request_roundtrip() {
        let val = openraft::raft::AppendEntriesRequest::<C> {
            vote: make_vote(5, 10, true),
            prev_log_id: Some(make_log_id(4, 10, 99)),
            entries: vec![
                openraft::impls::Entry::<C> {
                    log_id: make_log_id(5, 10, 100),
                    payload: openraft::EntryPayload::Normal(crate::test_support::TestBytes(
                        vec![1, 2, 3].into(),
                    )),
                },
                openraft::impls::Entry::<C> {
                    log_id: make_log_id(5, 10, 101),
                    payload: openraft::EntryPayload::Blank,
                },
            ],
            leader_commit: Some(make_log_id(5, 10, 98)),
        };
        let encoded = val.encode_to_vec().unwrap();
        let decoded =
            openraft::raft::AppendEntriesRequest::<C>::decode_from_slice(&encoded).unwrap();
        assert_eq!(val.vote, decoded.vote);
        assert_eq!(val.prev_log_id, decoded.prev_log_id);
        assert_eq!(val.leader_commit, decoded.leader_commit);
        assert_eq!(val.entries.len(), decoded.entries.len());
        for (a, b) in val.entries.iter().zip(decoded.entries.iter()) {
            assert_eq!(a.log_id, b.log_id);
            assert_eq!(format!("{:?}", a.payload), format!("{:?}", b.payload));
        }
    }

    #[test]
    fn test_append_entries_response_roundtrip() {
        let variants: Vec<openraft::raft::AppendEntriesResponse<C>> = vec![
            openraft::raft::AppendEntriesResponse::Success,
            openraft::raft::AppendEntriesResponse::PartialSuccess(Some(make_log_id(5, 10, 100))),
            openraft::raft::AppendEntriesResponse::PartialSuccess(None),
            openraft::raft::AppendEntriesResponse::Conflict,
            openraft::raft::AppendEntriesResponse::HigherVote(make_vote(6, 11, true)),
        ];

        for val in variants {
            let encoded = val.encode_to_vec().unwrap();
            let decoded =
                openraft::raft::AppendEntriesResponse::<C>::decode_from_slice(&encoded).unwrap();
            assert_eq!(val, decoded);
        }
    }

    #[test]
    fn test_rpc_message_roundtrip() {
        let msg = RpcMessage::<C>::Vote {
            request_id: 12345,
            group_id: 1,
            rpc: openraft::raft::VoteRequest::<C> {
                vote: make_vote(5, 10, false),
                last_log_id: Some(make_log_id(4, 10, 50)),
            },
        };

        let encoded = msg.encode_to_vec().unwrap();
        let decoded = RpcMessage::<C>::decode_from_slice(&encoded).unwrap();
        // Compare request_id since RpcMessage doesn't derive PartialEq with openraft types
        assert_eq!(msg.request_id(), decoded.request_id());
        if let (RpcMessage::Vote { rpc: rpc1, .. }, RpcMessage::Vote { rpc: rpc2, .. }) =
            (&msg, &decoded)
        {
            assert_eq!(rpc1, rpc2);
        } else {
            panic!("Expected Vote variant");
        }
    }

    #[test]
    fn test_rpc_response_roundtrip() {
        let msg = RpcMessage::<C>::Response {
            request_id: 12345,
            message: ResponseMessage::Vote(openraft::raft::VoteResponse::<C> {
                vote: make_vote(5, 10, true),
                vote_granted: true,
                last_log_id: None,
            }),
        };

        let encoded = msg.encode_to_vec().unwrap();
        let decoded = RpcMessage::<C>::decode_from_slice(&encoded).unwrap();
        assert_eq!(msg.request_id(), decoded.request_id());
        if let (
            RpcMessage::Response {
                message: ResponseMessage::Vote(resp1),
                ..
            },
            RpcMessage::Response {
                message: ResponseMessage::Vote(resp2),
                ..
            },
        ) = (&msg, &decoded)
        {
            assert_eq!(resp1, resp2);
        } else {
            panic!("Expected Response/Vote variant");
        }
    }

    #[test]
    fn test_membership_roundtrip() {
        let configs = vec![
            vec![1u64, 2, 3].into_iter().collect::<BTreeSet<u64>>(),
            vec![4u64, 5].into_iter().collect::<BTreeSet<u64>>(),
        ];
        let mut nodes = BTreeMap::new();
        for id in [1u64, 2, 3, 4, 5] {
            nodes.insert(
                id,
                openraft::impls::BasicNode {
                    addr: format!("127.0.0.1:{}", 5000 + id),
                },
            );
        }
        let val = openraft::Membership::<C>::new(configs, nodes).unwrap();
        let encoded = val.encode_to_vec().unwrap();
        let decoded = openraft::Membership::<C>::decode_from_slice(&encoded).unwrap();
        // Compare via accessors since Membership may not derive PartialEq
        assert_eq!(val.get_joint_config(), decoded.get_joint_config());
    }

    #[test]
    fn test_entry_with_membership_roundtrip() {
        let configs = vec![vec![1u64, 2, 3].into_iter().collect::<BTreeSet<u64>>()];
        let membership = openraft::Membership::<C>::new_with_defaults(configs, Vec::<u64>::new());

        let val = openraft::impls::Entry::<C> {
            log_id: make_log_id(5, 10, 100),
            payload: openraft::EntryPayload::Membership(membership),
        };
        let encoded = val.encode_to_vec().unwrap();
        let decoded = openraft::impls::Entry::<C>::decode_from_slice(&encoded).unwrap();
        assert_eq!(val.log_id, decoded.log_id);
    }

    #[test]
    fn test_install_snapshot_roundtrip() {
        let configs = vec![vec![1u64, 2, 3].into_iter().collect::<BTreeSet<u64>>()];
        let membership = openraft::Membership::<C>::new_with_defaults(configs, Vec::<u64>::new());

        let val = openraft::raft::InstallSnapshotRequest::<C> {
            vote: make_vote(5, 10, true),
            meta: openraft::storage::SnapshotMeta {
                last_log_id: Some(make_log_id(5, 10, 100)),
                last_membership: openraft::StoredMembership::new(
                    Some(make_log_id(5, 10, 50)),
                    membership,
                ),
                snapshot_id: "snap-12345".to_string(),
            },
            offset: 0,
            data: vec![1, 2, 3, 4, 5, 6, 7, 8],
            done: false,
        };
        let encoded = val.encode_to_vec().unwrap();
        let decoded =
            openraft::raft::InstallSnapshotRequest::<C>::decode_from_slice(&encoded).unwrap();
        assert_eq!(val.vote, decoded.vote);
        assert_eq!(val.offset, decoded.offset);
        assert_eq!(val.data, decoded.data);
        assert_eq!(val.done, decoded.done);
    }

    #[test]
    fn test_invalid_message_type() {
        let mut data = vec![0xFFu8]; // Invalid message type
        data.extend_from_slice(&0u64.to_le_bytes()); // request_id
        assert!(RpcMessage::<C>::decode_from_slice(&data).is_err());
    }

    #[test]
    fn test_invalid_response_type() {
        let data = vec![0xFFu8]; // Invalid response type
        assert!(ResponseMessage::<C>::decode_from_slice(&data).is_err());
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
        let data = vec![0xFFu8]; // Invalid EntryPayloadType
        assert!(openraft::EntryPayload::<C>::decode_from_slice(&data).is_err());
    }

    #[test]
    fn test_invalid_append_entries_response_discriminant() {
        let data = vec![0xFFu8]; // Invalid AppendEntriesResponseType
        assert!(openraft::raft::AppendEntriesResponse::<C>::decode_from_slice(&data).is_err());
    }

    #[test]
    fn test_corrupted_vote() {
        let vote = make_vote(5, 10, true);
        let encoded = vote.encode_to_vec().unwrap();
        assert_eq!(encoded.len(), 17);

        // Vote doesn't include CRC validation; the only reliable failure mode is truncation.
        let invalid_too_short = vec![0xFFu8; 16];
        assert!(openraft::impls::Vote::<C>::decode_from_slice(&invalid_too_short).is_err());
    }

    #[test]
    fn test_corrupted_log_id() {
        // Test with invalid data length - LogId should be 24 bytes
        let invalid_data = vec![0xFFu8; 20]; // Too short
        assert!(openraft::LogId::<C>::decode_from_slice(&invalid_data).is_err());
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

    // ===================================================================
    // Additional codec edge cases
    // ===================================================================

    #[test]
    fn test_codec_empty_entries_list() {
        // AppendEntries with 0 entries — roundtrip
        let vote = make_vote(1, 1, true);
        let req = openraft::raft::AppendEntriesRequest::<C> {
            vote,
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };

        let msg = RpcMessage::<C>::AppendEntries {
            request_id: 1,
            group_id: 0,
            rpc: req.clone(),
        };

        let encoded = msg.encode_to_vec().unwrap();
        let decoded = RpcMessage::<C>::decode_from_slice(&encoded).unwrap();

        match decoded {
            RpcMessage::AppendEntries { rpc, .. } => {
                assert!(rpc.entries.is_empty());
                assert_eq!(rpc.prev_log_id, None);
                assert_eq!(rpc.leader_commit, None);
            }
            _ => panic!("Expected AppendEntries"),
        }
    }

    #[test]
    fn test_codec_many_entries_roundtrip() {
        // AppendEntries with many entries — roundtrip
        use crate::test_support::TestBytes;

        let vote = make_vote(5, 2, true);
        let entries: Vec<openraft::impls::Entry<C>> = (1..=100)
            .map(|i| openraft::impls::Entry::<C> {
                log_id: make_log_id(5, 2, i),
                payload: openraft::EntryPayload::Normal(TestBytes::from_vec(vec![i as u8; 50])),
            })
            .collect();

        let req = openraft::raft::AppendEntriesRequest::<C> {
            vote,
            prev_log_id: Some(make_log_id(4, 1, 0)),
            entries,
            leader_commit: Some(make_log_id(5, 2, 50)),
        };

        let msg = RpcMessage::<C>::AppendEntries {
            request_id: 42,
            group_id: 7,
            rpc: req,
        };

        let encoded = msg.encode_to_vec().unwrap();
        let decoded = RpcMessage::<C>::decode_from_slice(&encoded).unwrap();

        match decoded {
            RpcMessage::AppendEntries {
                rpc,
                request_id,
                group_id,
            } => {
                assert_eq!(request_id, 42);
                assert_eq!(group_id, 7);
                assert_eq!(rpc.entries.len(), 100);
                assert_eq!(rpc.entries[0].log_id.index, 1);
                assert_eq!(rpc.entries[99].log_id.index, 100);
            }
            _ => panic!("Expected AppendEntries"),
        }
    }

    #[test]
    fn test_codec_snapshot_zero_offset() {
        let vote = make_vote(1, 1, true);
        let meta = openraft::storage::SnapshotMeta::<C> {
            last_log_id: None,
            last_membership: openraft::StoredMembership::default(),
            snapshot_id: "snap-0".to_string(),
        };

        let req = openraft::raft::InstallSnapshotRequest::<C> {
            vote,
            meta,
            offset: 0,
            data: vec![1, 2, 3],
            done: false,
        };

        let msg = RpcMessage::<C>::InstallSnapshot {
            request_id: 10,
            group_id: 1,
            rpc: req,
        };

        let encoded = msg.encode_to_vec().unwrap();
        let decoded = RpcMessage::<C>::decode_from_slice(&encoded).unwrap();

        match decoded {
            RpcMessage::InstallSnapshot { rpc, .. } => {
                assert_eq!(rpc.offset, 0);
                assert_eq!(rpc.data, vec![1, 2, 3]);
                assert!(!rpc.done);
            }
            _ => panic!("Expected InstallSnapshot"),
        }
    }

    #[test]
    fn test_codec_snapshot_large_data() {
        let vote = make_vote(1, 1, true);
        let meta = openraft::storage::SnapshotMeta::<C> {
            last_log_id: Some(make_log_id(1, 1, 100)),
            last_membership: openraft::StoredMembership::default(),
            snapshot_id: "snap-large".to_string(),
        };

        let large_data = vec![0xAB; 1024 * 1024]; // 1MB
        let req = openraft::raft::InstallSnapshotRequest::<C> {
            vote,
            meta,
            offset: 512 * 1024,
            data: large_data.clone(),
            done: true,
        };

        let msg = RpcMessage::<C>::InstallSnapshot {
            request_id: 99,
            group_id: 5,
            rpc: req,
        };

        let encoded = msg.encode_to_vec().unwrap();
        let decoded = RpcMessage::<C>::decode_from_slice(&encoded).unwrap();

        match decoded {
            RpcMessage::InstallSnapshot { rpc, .. } => {
                assert_eq!(rpc.data.len(), 1024 * 1024);
                assert_eq!(rpc.offset, 512 * 1024);
                assert!(rpc.done);
            }
            _ => panic!("Expected InstallSnapshot"),
        }
    }

    #[test]
    fn test_codec_error_message_roundtrip() {
        let msg = RpcMessage::<C>::Error {
            request_id: 123,
            error: "Something went wrong".to_string(),
        };

        let encoded = msg.encode_to_vec().unwrap();
        let decoded = RpcMessage::<C>::decode_from_slice(&encoded).unwrap();

        match decoded {
            RpcMessage::Error { request_id, error } => {
                assert_eq!(request_id, 123);
                assert_eq!(error, "Something went wrong");
            }
            _ => panic!("Expected Error"),
        }
    }

    #[test]
    fn test_codec_response_message_roundtrip() {
        let vote = make_vote(3, 2, true);

        // Vote response
        let resp = openraft::raft::VoteResponse::<C> {
            vote: vote.clone(),
            vote_granted: true,
            last_log_id: Some(make_log_id(2, 1, 50)),
        };

        let msg = RpcMessage::<C>::Response {
            request_id: 77,
            message: ResponseMessage::Vote(resp),
        };

        let encoded = msg.encode_to_vec().unwrap();
        let decoded = RpcMessage::<C>::decode_from_slice(&encoded).unwrap();

        match decoded {
            RpcMessage::Response {
                request_id,
                message: ResponseMessage::Vote(resp),
            } => {
                assert_eq!(request_id, 77);
                assert!(resp.vote_granted);
                assert_eq!(resp.last_log_id.unwrap().index, 50);
            }
            _ => panic!("Expected Response(Vote)"),
        }
    }
}

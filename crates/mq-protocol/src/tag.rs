use crate::error::ProtocolError;

/// Client-to-server frame tags.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientTag {
    Handshake = 0x01,
    Subscribe = 0x02,
    Unsubscribe = 0x03,
    SetMaxInFlight = 0x04,
    Ack = 0x05,
    Nack = 0x06,
    CommitOffset = 0x07,
    Heartbeat = 0x08,
    Close = 0x09,
    SetByteBudget = 0x0A,
    Publish = 0x0B,
    // Consumer group operations
    CreateGroup = 0x0C,
    DeleteGroup = 0x0D,
    JoinGroup = 0x0E,
    SyncGroup = 0x0F,
    LeaveGroup = 0x10,
    GroupHeartbeat = 0x11,
    CommitGroupOffset = 0x12,
    FetchGroupOffsets = 0x13,
    ListGroups = 0x14,
    DescribeGroup = 0x15,
}

impl TryFrom<u8> for ClientTag {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Handshake),
            0x02 => Ok(Self::Subscribe),
            0x03 => Ok(Self::Unsubscribe),
            0x04 => Ok(Self::SetMaxInFlight),
            0x05 => Ok(Self::Ack),
            0x06 => Ok(Self::Nack),
            0x07 => Ok(Self::CommitOffset),
            0x08 => Ok(Self::Heartbeat),
            0x09 => Ok(Self::Close),
            0x0A => Ok(Self::SetByteBudget),
            0x0B => Ok(Self::Publish),
            0x0C => Ok(Self::CreateGroup),
            0x0D => Ok(Self::DeleteGroup),
            0x0E => Ok(Self::JoinGroup),
            0x0F => Ok(Self::SyncGroup),
            0x10 => Ok(Self::LeaveGroup),
            0x11 => Ok(Self::GroupHeartbeat),
            0x12 => Ok(Self::CommitGroupOffset),
            0x13 => Ok(Self::FetchGroupOffsets),
            0x14 => Ok(Self::ListGroups),
            0x15 => Ok(Self::DescribeGroup),
            _ => Err(ProtocolError::UnknownTag(value)),
        }
    }
}

/// Server-to-client frame tags.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerTag {
    HandshakeOk = 0x81,
    HandshakeErr = 0x82,
    Subscribed = 0x83,
    Message = 0x84,
    SubscriptionErr = 0x85,
    Heartbeat = 0x86,
    Close = 0x87,
    MessageBatch = 0x88,
    // Consumer group responses
    GroupCreated = 0x89,
    GroupDeleted = 0x8A,
    GroupJoined = 0x8B,
    GroupSynced = 0x8C,
    GroupLeft = 0x8D,
    GroupHeartbeatOk = 0x8E,
    GroupOffsetCommitted = 0x8F,
    GroupOffsetsFetched = 0x90,
    GroupList = 0x91,
    GroupDescription = 0x92,
    GroupError = 0x93,
}

impl TryFrom<u8> for ServerTag {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x81 => Ok(Self::HandshakeOk),
            0x82 => Ok(Self::HandshakeErr),
            0x83 => Ok(Self::Subscribed),
            0x84 => Ok(Self::Message),
            0x85 => Ok(Self::SubscriptionErr),
            0x86 => Ok(Self::Heartbeat),
            0x87 => Ok(Self::Close),
            0x88 => Ok(Self::MessageBatch),
            0x89 => Ok(Self::GroupCreated),
            0x8A => Ok(Self::GroupDeleted),
            0x8B => Ok(Self::GroupJoined),
            0x8C => Ok(Self::GroupSynced),
            0x8D => Ok(Self::GroupLeft),
            0x8E => Ok(Self::GroupHeartbeatOk),
            0x8F => Ok(Self::GroupOffsetCommitted),
            0x90 => Ok(Self::GroupOffsetsFetched),
            0x91 => Ok(Self::GroupList),
            0x92 => Ok(Self::GroupDescription),
            0x93 => Ok(Self::GroupError),
            _ => Err(ProtocolError::UnknownTag(value)),
        }
    }
}

/// Returns true if the tag byte is in the client range (0x01..=0x1F).
pub fn is_client_tag(tag: u8) -> bool {
    (0x01..=0x1F).contains(&tag)
}

/// Returns true if the tag byte is in the server range (0x81..=0x9F).
pub fn is_server_tag(tag: u8) -> bool {
    (0x81..=0x9F).contains(&tag)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_tag_roundtrip() {
        for tag in [
            ClientTag::Handshake,
            ClientTag::Subscribe,
            ClientTag::Unsubscribe,
            ClientTag::SetMaxInFlight,
            ClientTag::Ack,
            ClientTag::Nack,
            ClientTag::CommitOffset,
            ClientTag::Heartbeat,
            ClientTag::Close,
            ClientTag::SetByteBudget,
            ClientTag::Publish,
            ClientTag::CreateGroup,
            ClientTag::DeleteGroup,
            ClientTag::JoinGroup,
            ClientTag::SyncGroup,
            ClientTag::LeaveGroup,
            ClientTag::GroupHeartbeat,
            ClientTag::CommitGroupOffset,
            ClientTag::FetchGroupOffsets,
            ClientTag::ListGroups,
            ClientTag::DescribeGroup,
        ] {
            let byte = tag as u8;
            assert_eq!(ClientTag::try_from(byte).unwrap(), tag);
            assert!(is_client_tag(byte));
            assert!(!is_server_tag(byte));
        }
    }

    #[test]
    fn test_server_tag_roundtrip() {
        for tag in [
            ServerTag::HandshakeOk,
            ServerTag::HandshakeErr,
            ServerTag::Subscribed,
            ServerTag::Message,
            ServerTag::SubscriptionErr,
            ServerTag::Heartbeat,
            ServerTag::Close,
            ServerTag::MessageBatch,
            ServerTag::GroupCreated,
            ServerTag::GroupDeleted,
            ServerTag::GroupJoined,
            ServerTag::GroupSynced,
            ServerTag::GroupLeft,
            ServerTag::GroupHeartbeatOk,
            ServerTag::GroupOffsetCommitted,
            ServerTag::GroupOffsetsFetched,
            ServerTag::GroupList,
            ServerTag::GroupDescription,
            ServerTag::GroupError,
        ] {
            let byte = tag as u8;
            assert_eq!(ServerTag::try_from(byte).unwrap(), tag);
            assert!(is_server_tag(byte));
            assert!(!is_client_tag(byte));
        }
    }

    #[test]
    fn test_unknown_tag() {
        assert!(matches!(
            ClientTag::try_from(0xFF),
            Err(ProtocolError::UnknownTag(0xFF))
        ));
        assert!(matches!(
            ServerTag::try_from(0x00),
            Err(ProtocolError::UnknownTag(0x00))
        ));
    }
}

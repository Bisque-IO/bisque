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
            _ => Err(ProtocolError::UnknownTag(value)),
        }
    }
}

/// Returns true if the tag byte is in the client range (0x01..=0x0F).
pub fn is_client_tag(tag: u8) -> bool {
    (0x01..=0x0F).contains(&tag)
}

/// Returns true if the tag byte is in the server range (0x81..=0x8F).
pub fn is_server_tag(tag: u8) -> bool {
    (0x81..=0x8F).contains(&tag)
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

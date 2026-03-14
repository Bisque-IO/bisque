use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::ProtocolError;
use crate::types::{ClientFrame, Decode, Encode, ServerFrame};

/// Maximum frame payload size (4 MiB).
pub const MAX_FRAME_SIZE: u32 = 4 * 1024 * 1024;

/// Read a TCP frame: `[u32 len][payload...]`.
///
/// Returns the number of bytes read into `buf`. The caller can then decode
/// the frame via `decode_client_frame` or `decode_server_frame`.
pub async fn read_tcp_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut Vec<u8>,
) -> Result<usize, ProtocolError> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(ProtocolError::ConnectionClosed);
        }
        Err(e) => return Err(ProtocolError::Io(e)),
    }
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge {
            size: len,
            max: MAX_FRAME_SIZE,
        });
    }
    if len == 0 {
        return Err(ProtocolError::Truncated { need: 1, have: 0 });
    }
    let len = len as usize;
    buf.resize(len, 0);
    reader.read_exact(&mut buf[..len]).await?;
    Ok(len)
}

/// Read a TCP frame into a `BytesMut`, returning a frozen `Bytes`.
///
/// Enables zero-copy decode: the returned `Bytes` can be passed to
/// `ClientFrame::decode_from_bytes` or `ServerFrame::decode_from_bytes`
/// to get zero-copy field slices with no per-field allocation.
pub async fn read_tcp_frame_bytes<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut BytesMut,
) -> Result<Bytes, ProtocolError> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(ProtocolError::ConnectionClosed);
        }
        Err(e) => return Err(ProtocolError::Io(e)),
    }
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge {
            size: len,
            max: MAX_FRAME_SIZE,
        });
    }
    if len == 0 {
        return Err(ProtocolError::Truncated { need: 1, have: 0 });
    }
    let len = len as usize;
    buf.resize(len, 0);
    reader.read_exact(&mut buf[..len]).await?;
    Ok(buf.split_to(len).freeze())
}

/// Write a TCP frame: `[u32 len][data]`.
///
/// Zero-allocation: writes the 4-byte length prefix and payload separately.
/// Callers should wrap the writer in `BufWriter` to coalesce into a single
/// syscall when needed.
pub async fn write_tcp_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
) -> Result<(), ProtocolError> {
    let len_bytes = (data.len() as u32).to_le_bytes();
    writer.write_all(&len_bytes).await?;
    writer.write_all(data).await?;
    Ok(())
}

/// Write a TCP frame from a pre-built buffer that includes the length prefix.
///
/// Use with `encode_server_frame_prefixed` for a single write with zero extra copies.
pub async fn write_tcp_frame_prefixed<W: AsyncWrite + Unpin>(
    writer: &mut W,
    prefixed_data: &[u8],
) -> Result<(), ProtocolError> {
    writer.write_all(prefixed_data).await?;
    Ok(())
}

/// Decode a client frame from raw bytes (tag byte at offset 0).
pub fn decode_client_frame(data: &[u8]) -> Result<ClientFrame, ProtocolError> {
    ClientFrame::decode_from_slice(data)
}

/// Decode a server frame from raw bytes.
pub fn decode_server_frame(data: &[u8]) -> Result<ServerFrame, ProtocolError> {
    ServerFrame::decode_from_slice(data)
}

/// Zero-copy decode a client frame from `Bytes`.
pub fn decode_client_frame_bytes(data: Bytes) -> Result<ClientFrame, ProtocolError> {
    ClientFrame::decode_from_bytes(data)
}

/// Zero-copy decode a server frame from `Bytes`.
pub fn decode_server_frame_bytes(data: Bytes) -> Result<ServerFrame, ProtocolError> {
    ServerFrame::decode_from_bytes(data)
}

/// Encode a server frame to bytes (tag + payload).
pub fn encode_server_frame(frame: &ServerFrame) -> Result<Vec<u8>, ProtocolError> {
    frame.encode_to_vec()
}

/// Encode a server frame into a reusable buffer. Avoids per-frame allocation.
pub fn encode_server_frame_into(
    frame: &ServerFrame,
    buf: &mut Vec<u8>,
) -> Result<(), ProtocolError> {
    frame.encode_into(buf)
}

/// Encode a server frame with the TCP length prefix included.
/// Enables a single `write_all` call via `write_tcp_frame_prefixed`.
pub fn encode_server_frame_prefixed(
    frame: &ServerFrame,
    buf: &mut Vec<u8>,
) -> Result<(), ProtocolError> {
    buf.clear();
    let size = frame.encoded_size();
    buf.reserve(4 + size);
    buf.extend_from_slice(&[0u8; 4]);
    frame.encode(buf)?;
    let len = (buf.len() - 4) as u32;
    buf[..4].copy_from_slice(&len.to_le_bytes());
    Ok(())
}

/// Encode a client frame to bytes.
pub fn encode_client_frame(frame: &ClientFrame) -> Result<Vec<u8>, ProtocolError> {
    frame.encode_to_vec()
}

/// Encode a client frame into a reusable buffer.
pub fn encode_client_frame_into(
    frame: &ClientFrame,
    buf: &mut Vec<u8>,
) -> Result<(), ProtocolError> {
    frame.encode_into(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tcp_frame_roundtrip() {
        let (mut client, mut server) = tokio::io::duplex(4096);

        let frame = ClientFrame::Subscribe {
            sub_id: 1,
            group_id: 7,
            entity_type: 0,
            name_hash: 0xCAFEBABE,
            start_offset: 0,
            max_in_flight: 100,
        };
        let encoded = encode_client_frame(&frame).unwrap();

        write_tcp_frame(&mut client, &encoded).await.unwrap();
        drop(client);

        let mut buf = Vec::new();
        let len = read_tcp_frame(&mut server, &mut buf).await.unwrap();
        assert_eq!(len, encoded.len());

        let decoded = decode_client_frame(&buf).unwrap();
        assert_eq!(decoded, frame);
    }

    #[tokio::test]
    async fn test_tcp_frame_server_message_roundtrip() {
        use crate::flat::{FlatMessage, FlatMessageBuilder};
        use crate::types::WireMessage;
        let (mut client, mut server) = tokio::io::duplex(8192);

        let flat = FlatMessageBuilder::new(b"hello")
            .key(b"key")
            .timestamp(1_700_000_000)
            .header(b"h1", b"v1")
            .build();
        let frame = ServerFrame::Message(WireMessage {
            sub_id: 5,
            message_id: 42,
            flat_bytes: flat,
        });
        let encoded = encode_server_frame(&frame).unwrap();

        write_tcp_frame(&mut server, &encoded).await.unwrap();
        drop(server);

        let mut buf = Vec::new();
        read_tcp_frame(&mut client, &mut buf).await.unwrap();
        let decoded = decode_server_frame(&buf).unwrap();
        assert_eq!(decoded, frame);
    }

    #[tokio::test]
    async fn test_tcp_frame_multiple_frames() {
        let (mut writer, mut reader) = tokio::io::duplex(8192);

        let frames = vec![
            ClientFrame::Heartbeat,
            ClientFrame::Ack {
                sub_id: 1,
                message_ids: vec![10, 20],
            },
            ClientFrame::Close,
        ];

        for frame in &frames {
            let encoded = encode_client_frame(frame).unwrap();
            write_tcp_frame(&mut writer, &encoded).await.unwrap();
        }
        drop(writer);

        let mut buf = Vec::new();
        let mut decoded = Vec::new();
        loop {
            match read_tcp_frame(&mut reader, &mut buf).await {
                Ok(_) => decoded.push(decode_client_frame(&buf).unwrap()),
                Err(ProtocolError::ConnectionClosed) => break,
                Err(e) => panic!("unexpected error: {}", e),
            }
        }
        assert_eq!(decoded, frames);
    }

    #[tokio::test]
    async fn test_tcp_frame_too_large() {
        let (mut writer, mut reader) = tokio::io::duplex(64);

        let fake_len: u32 = MAX_FRAME_SIZE + 1;
        writer.write_all(&fake_len.to_le_bytes()).await.unwrap();
        drop(writer);

        let mut buf = Vec::new();
        let err = read_tcp_frame(&mut reader, &mut buf).await.unwrap_err();
        assert!(matches!(err, ProtocolError::FrameTooLarge { .. }));
    }

    #[tokio::test]
    async fn test_tcp_frame_connection_closed() {
        let (writer, mut reader) = tokio::io::duplex(64);
        drop(writer);

        let mut buf = Vec::new();
        let err = read_tcp_frame(&mut reader, &mut buf).await.unwrap_err();
        assert!(matches!(err, ProtocolError::ConnectionClosed));
    }

    #[tokio::test]
    async fn test_tcp_frame_zero_length() {
        let (mut writer, mut reader) = tokio::io::duplex(64);

        writer.write_all(&0u32.to_le_bytes()).await.unwrap();
        drop(writer);

        let mut buf = Vec::new();
        let err = read_tcp_frame(&mut reader, &mut buf).await.unwrap_err();
        assert!(matches!(err, ProtocolError::Truncated { need: 1, have: 0 }));
    }

    #[tokio::test]
    async fn test_read_tcp_frame_bytes_zero_copy() {
        use crate::flat::{FlatMessage, FlatMessageBuilder};
        use crate::types::WireMessage;
        let (mut writer, mut reader) = tokio::io::duplex(8192);

        let flat = FlatMessageBuilder::new(b"value")
            .key(b"key")
            .timestamp(1000)
            .header(b"h1", b"v1")
            .build();
        let frame = ServerFrame::Message(WireMessage {
            sub_id: 1,
            message_id: 42,
            flat_bytes: flat,
        });
        let encoded = encode_server_frame(&frame).unwrap();
        write_tcp_frame(&mut writer, &encoded).await.unwrap();
        drop(writer);

        let mut buf = BytesMut::new();
        let bytes = read_tcp_frame_bytes(&mut reader, &mut buf).await.unwrap();
        let decoded = decode_server_frame_bytes(bytes).unwrap();
        assert_eq!(decoded, frame);
    }

    #[tokio::test]
    async fn test_encode_server_frame_prefixed() {
        use crate::flat::{FlatMessage, FlatMessageBuilder};
        use crate::types::WireMessage;
        let (mut writer, mut reader) = tokio::io::duplex(8192);

        let flat = FlatMessageBuilder::new(b"test").timestamp(1).build();
        let frame = ServerFrame::Message(WireMessage {
            sub_id: 1,
            message_id: 1,
            flat_bytes: flat,
        });

        let mut buf = Vec::new();
        encode_server_frame_prefixed(&frame, &mut buf).unwrap();
        write_tcp_frame_prefixed(&mut writer, &buf).await.unwrap();
        drop(writer);

        let mut read_buf = Vec::new();
        read_tcp_frame(&mut reader, &mut read_buf).await.unwrap();
        let decoded = decode_server_frame(&read_buf).unwrap();
        assert_eq!(decoded, frame);
    }
}

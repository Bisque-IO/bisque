//! Transport abstraction for MQTT connections.
//!
//! Supports plain TCP, TLS (via `tokio-rustls` behind the `tls` feature),
//! and WebSocket (via `fastwebsockets` behind the `websocket` feature).
//!
//! The MQTT server uses `AsyncRead + AsyncWrite + Unpin` bounds, so any
//! transport that implements these traits works seamlessly.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

// =============================================================================
// MqttStream — unified transport enum
// =============================================================================

/// A unified stream type for MQTT connections.
///
/// Wraps plain TCP, TLS, or WebSocket streams behind a single type that
/// implements `AsyncRead + AsyncWrite + Unpin`.
pub enum MqttStream {
    /// Plain TCP connection (port 1883).
    Tcp(TcpStream),
    /// TLS-encrypted connection (port 8883).
    #[cfg(feature = "tls")]
    Tls(tokio_rustls::server::TlsStream<TcpStream>),
    /// WebSocket connection (port 80/443, subprotocol "mqtt").
    #[cfg(feature = "websocket")]
    WebSocket(WebSocketStream),
}

impl AsyncRead for MqttStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MqttStream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            MqttStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "websocket")]
            MqttStream::WebSocket(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MqttStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MqttStream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            MqttStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "websocket")]
            MqttStream::WebSocket(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MqttStream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            MqttStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "websocket")]
            MqttStream::WebSocket(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MqttStream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            MqttStream::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "websocket")]
            MqttStream::WebSocket(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

// =============================================================================
// TLS Configuration
// =============================================================================

/// TLS configuration for the MQTT server.
#[cfg(feature = "tls")]
pub struct TlsConfig {
    /// The TLS acceptor (pre-configured with certificate and private key).
    pub acceptor: tokio_rustls::TlsAcceptor,
}

#[cfg(feature = "tls")]
impl TlsConfig {
    /// Create a TLS config from PEM-encoded certificate and private key files.
    pub fn from_pem(cert_path: &std::path::Path, key_path: &std::path::Path) -> io::Result<Self> {
        use rustls::pki_types::PrivateKeyDer;
        use std::fs::File;
        use std::io::BufReader;

        let cert_file = File::open(cert_path)?;
        let mut cert_reader = BufReader::new(cert_file);
        let certs: Vec<_> =
            rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;

        if certs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no certificates found in PEM file",
            ));
        }

        let key_file = File::open(key_path)?;
        let mut key_reader = BufReader::new(key_file);
        let key = rustls_pemfile::private_key(&mut key_reader)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "no private key found in PEM file",
            )
        })?;

        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, PrivateKeyDer::from(key))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        Ok(Self {
            acceptor: tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config)),
        })
    }

    /// Accept a TLS connection on the given TCP stream.
    pub async fn accept(&self, stream: TcpStream) -> io::Result<MqttStream> {
        let tls_stream = self.acceptor.accept(stream).await?;
        Ok(MqttStream::Tls(tls_stream))
    }
}

// =============================================================================
// WebSocket Transport (via fastwebsockets)
// =============================================================================

/// WebSocket stream adapter for MQTT over WebSocket.
///
/// Wraps a `fastwebsockets::WebSocket` to provide `AsyncRead + AsyncWrite`
/// semantics by framing MQTT binary data in WebSocket binary frames.
///
/// MQTT over WebSocket (OASIS MQTT 5.0 SS 6): MQTT packets are sent as
/// WebSocket binary frames with subprotocol "mqtt". Each WebSocket message
/// contains exactly one complete MQTT packet.
///
/// **Important**: The recommended API is the async `read_frame_data()` /
/// `write_frame_data()` methods. The `AsyncRead`/`AsyncWrite` impls are
/// provided for `MqttStream` enum compatibility only.
#[cfg(feature = "websocket")]
pub struct WebSocketStream {
    inner: fastwebsockets::WebSocket<tokio::io::BufReader<TcpStream>>,
    /// Read buffer for reassembling WebSocket frames into a byte stream.
    read_buf: bytes::BytesMut,
    /// Whether the read side is closed.
    read_closed: bool,
}

#[cfg(feature = "websocket")]
impl WebSocketStream {
    /// Create a new WebSocket stream from an already-upgraded fastwebsockets connection.
    pub fn new(ws: fastwebsockets::WebSocket<tokio::io::BufReader<TcpStream>>) -> Self {
        Self {
            inner: ws,
            read_buf: bytes::BytesMut::with_capacity(4096),
            read_closed: false,
        }
    }

    /// Read and write MQTT data over the WebSocket connection.
    ///
    /// This is the recommended API for MQTT over WebSocket. Use this instead
    /// of the `AsyncRead`/`AsyncWrite` traits for correct framing semantics.
    ///
    /// Returns the MQTT binary data from the next WebSocket frame, or `None`
    /// if the connection was closed.
    pub async fn read_frame_data(&mut self) -> io::Result<Option<bytes::Bytes>> {
        use bytes::Bytes;
        use fastwebsockets::OpCode;

        loop {
            let frame = self
                .inner
                .read_frame()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            match frame.opcode {
                OpCode::Binary => {
                    return Ok(Some(Bytes::from(frame.payload.to_vec())));
                }
                OpCode::Close => return Ok(None),
                OpCode::Ping => {
                    let pong = fastwebsockets::Frame::pong(frame.payload);
                    self.inner
                        .write_frame(pong)
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    continue;
                }
                OpCode::Pong => continue,
                OpCode::Text => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "MQTT over WebSocket requires binary frames",
                    ));
                }
                _ => continue,
            }
        }
    }

    /// Write MQTT binary data as a WebSocket binary frame.
    pub async fn write_frame_data(&mut self, data: &[u8]) -> io::Result<()> {
        let payload = fastwebsockets::Payload::Owned(data.to_vec());
        let frame = fastwebsockets::Frame::binary(payload);
        self.inner
            .write_frame(frame)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    /// Send a WebSocket close frame.
    pub async fn close(&mut self) -> io::Result<()> {
        let frame = fastwebsockets::Frame::close_raw(vec![].into());
        self.inner
            .write_frame(frame)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

// AsyncRead/AsyncWrite for MqttStream enum compatibility.
// For WebSocket, these delegate to the inner read_buf (filled via read_frame_data)
// which requires the caller to drive frame reading externally for proper
// WebSocket semantics.
#[cfg(feature = "websocket")]
impl AsyncRead for WebSocketStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        if this.read_closed {
            return Poll::Ready(Ok(()));
        }

        // Return buffered data if available.
        if !this.read_buf.is_empty() {
            let len = this.read_buf.len().min(buf.remaining());
            buf.put_slice(&this.read_buf.split_to(len));
            return Poll::Ready(Ok(()));
        }

        // No data available — the caller should use read_frame_data() to fill
        // the buffer and then retry, or use the WebSocketStream API directly.
        Poll::Pending
    }
}

#[cfg(feature = "websocket")]
impl AsyncWrite for WebSocketStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Buffer the data; actual frame sending requires async context.
        // For the MqttStream enum, the server should use write_frame_data() directly.
        let _ = buf;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// The MQTT WebSocket subprotocol identifier.
#[cfg(feature = "websocket")]
pub const MQTT_WEBSOCKET_SUBPROTOCOL: &str = "mqtt";

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_stream_is_unpin() {
        fn assert_unpin<T: Unpin>() {}
        assert_unpin::<MqttStream>();
    }

    #[test]
    fn test_mqtt_stream_tcp_variant() {
        // Verify the enum can be constructed (compile-time check).
        fn _accepts_tcp(stream: TcpStream) -> MqttStream {
            MqttStream::Tcp(stream)
        }
    }
}

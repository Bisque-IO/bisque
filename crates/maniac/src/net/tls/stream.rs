//! TLS stream implementation.

use std::io::{self, Read, Write};
use std::net::SocketAddr;

use rustls::Connection;

use crate::buf::{BufResult, IoBuf, IoBufMut, SetLen};
use crate::io::{AsyncRead, AsyncWrite};
use crate::net::TcpStream;

/// Default size for the internal TLS read buffer (matches TLS max record size).
const TLS_BUF_SIZE: usize = 16 * 1024;

/// A TLS-encrypted stream wrapping a [`TcpStream`].
///
/// Provides encrypted communication over TCP using rustls with aws-lc-rs.
/// Implements [`AsyncRead`] and [`AsyncWrite`] for integration with maniac's
/// IO ecosystem.
///
/// Unlike [`TcpStream`], all IO operations require `&mut self` because TLS
/// state is inherently sequential.
pub struct TlsStream {
    tcp: TcpStream,
    conn: Connection,
    /// Reusable buffer for receiving encrypted data from the network.
    recv_buf: Vec<u8>,
}

impl TlsStream {
    /// Create a new TlsStream from a TcpStream and an established rustls Connection.
    pub(super) fn new(tcp: TcpStream, conn: Connection) -> Self {
        Self {
            tcp,
            conn,
            recv_buf: Vec::with_capacity(TLS_BUF_SIZE),
        }
    }

    /// Returns the socket address of the remote peer.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.tcp.peer_addr()
    }

    /// Returns the socket address of the local half.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.tcp.local_addr()
    }

    /// Gets the value of the `TCP_NODELAY` option on the underlying socket.
    pub fn nodelay(&self) -> io::Result<bool> {
        self.tcp.nodelay()
    }

    /// Sets the value of the `TCP_NODELAY` option on the underlying socket.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.tcp.set_nodelay(nodelay)
    }

    /// Returns a reference to the underlying rustls [`Connection`].
    pub fn rustls_connection(&self) -> &Connection {
        &self.conn
    }

    /// Returns a reference to the underlying [`TcpStream`].
    pub fn tcp_stream(&self) -> &TcpStream {
        &self.tcp
    }

    /// Get the negotiated ALPN protocol, if any.
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.conn.alpn_protocol()
    }

    /// Complete the TLS handshake.
    ///
    /// Loops until the handshake is finished, exchanging encrypted bytes
    /// with the TCP peer.
    pub(super) async fn complete_handshake(&mut self) -> io::Result<()> {
        while self.conn.is_handshaking() {
            if self.conn.wants_write() {
                self.flush_tls_to_tcp().await?;
            }

            if self.conn.wants_read() {
                let n = self.read_tls_from_tcp().await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "peer disconnected during TLS handshake",
                    ));
                }

                self.conn
                    .process_new_packets()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            }
        }

        // Flush any remaining handshake data (e.g., Finished message).
        if self.conn.wants_write() {
            self.flush_tls_to_tcp().await?;
        }

        Ok(())
    }

    /// Receive encrypted bytes from TCP and feed them to rustls.
    async fn read_tls_from_tcp(&mut self) -> io::Result<usize> {
        // Take the buffer out of self so we can pass ownership to tcp.recv().
        let mut buf = std::mem::take(&mut self.recv_buf);
        buf.clear();
        if buf.capacity() < TLS_BUF_SIZE {
            buf.reserve(TLS_BUF_SIZE - buf.capacity());
        }

        let BufResult(result, returned_buf) = self.tcp.recv(buf).await;
        self.recv_buf = returned_buf;

        let n = result?;
        if n == 0 {
            return Ok(0);
        }

        // Feed encrypted bytes to rustls via a Cursor (implements std::io::Read).
        let mut cursor = io::Cursor::new(&self.recv_buf[..n]);
        let fed = self
            .conn
            .read_tls(&mut cursor)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(fed)
    }

    /// Extract encrypted bytes from rustls and send them all over TCP.
    async fn flush_tls_to_tcp(&mut self) -> io::Result<()> {
        let mut outgoing = Vec::new();
        self.conn
            .write_tls(&mut outgoing)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        if outgoing.is_empty() {
            return Ok(());
        }

        let mut written = 0;
        while written < outgoing.len() {
            let data = outgoing[written..].to_vec();
            let BufResult(result, _) = self.tcp.send(data).await;
            let sent = result?;
            if sent == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to send TLS data",
                ));
            }
            written += sent;
        }

        Ok(())
    }

    /// Try to read plaintext from rustls into `dst`. Returns `Some(n)` if
    /// data was available, `None` if rustls needs more encrypted input.
    fn try_read_plaintext(&mut self, dst: &mut [u8]) -> Option<io::Result<usize>> {
        if dst.is_empty() {
            return Some(Ok(0));
        }
        match self.conn.reader().read(dst) {
            Ok(n) if n > 0 => Some(Ok(n)),
            Ok(_) => Some(Ok(0)), // clean EOF
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => None,
            Err(e) => Some(Err(e)),
        }
    }

    /// Receive plaintext data from the TLS stream.
    ///
    /// Returns a [`BufResult`] with the number of bytes read and the buffer.
    /// Returns `Ok(0)` when the TLS session has been cleanly shut down.
    pub async fn recv<B: IoBufMut + SetLen + Send>(&mut self, mut buf: B) -> BufResult<usize, B> {
        loop {
            // Try reading already-buffered plaintext from rustls.
            let dst = unsafe {
                let uninit = buf.as_uninit();
                std::slice::from_raw_parts_mut(uninit.as_mut_ptr() as *mut u8, uninit.len())
            };

            match self.try_read_plaintext(dst) {
                Some(Ok(n)) => {
                    unsafe { buf.set_len(n) };
                    return BufResult(Ok(n), buf);
                }
                Some(Err(e)) => {
                    return BufResult(Err(e), buf);
                }
                None => {
                    // Need more encrypted data from the network.
                }
            }

            // Read encrypted data from TCP and feed to rustls.
            let n = match self.read_tls_from_tcp().await {
                Ok(n) => n,
                Err(e) => return BufResult(Err(e), buf),
            };

            if n == 0 {
                unsafe { buf.set_len(0) };
                return BufResult(Ok(0), buf);
            }

            // Process the encrypted data through rustls, then loop
            // back to try reading plaintext again.
            if let Err(e) = self.conn.process_new_packets() {
                return BufResult(Err(io::Error::new(io::ErrorKind::InvalidData, e)), buf);
            }
        }
    }

    /// Send plaintext data over the TLS stream.
    ///
    /// Returns a [`BufResult`] with the number of plaintext bytes written and
    /// the buffer.
    pub async fn send<T: IoBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
        let plaintext = buf.as_init();
        if plaintext.is_empty() {
            return BufResult(Ok(0), buf);
        }

        // Write plaintext into rustls.
        let written = match self.conn.writer().write(plaintext) {
            Ok(n) => n,
            Err(e) => return BufResult(Err(e), buf),
        };

        // Extract encrypted bytes from rustls and send over TCP.
        match self.flush_tls_to_tcp().await {
            Ok(()) => BufResult(Ok(written), buf),
            Err(e) => BufResult(Err(e), buf),
        }
    }
}

impl AsyncRead for TlsStream {
    #[inline]
    async fn read<B: IoBufMut + SetLen + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        self.recv(buf).await
    }
}

impl AsyncWrite for TlsStream {
    #[inline]
    async fn write<T: IoBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
        self.send(buf).await
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.flush_tls_to_tcp().await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.conn.send_close_notify();
        self.flush_tls_to_tcp().await?;
        self.tcp.socket().shutdown(std::net::Shutdown::Write)
    }
}

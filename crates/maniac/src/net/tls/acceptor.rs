//! Server-side TLS acceptor.

use std::io;
use std::sync::Arc;

use rustls::ServerConfig;

use crate::net::TcpStream;

use super::TlsStream;

/// A TLS acceptor for establishing server-side TLS connections.
///
/// Wraps a [`rustls::ServerConfig`] and provides a method to perform
/// a TLS handshake with a connecting client.
#[derive(Clone)]
pub struct TlsAcceptor {
    config: Arc<ServerConfig>,
}

impl TlsAcceptor {
    /// Create a new `TlsAcceptor` with the given server configuration.
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self { config }
    }

    /// Create a `TlsAcceptor` from PEM-encoded certificate and private key data.
    pub fn from_pem(cert_pem: &[u8], key_pem: &[u8]) -> io::Result<Self> {
        let certs: Vec<_> =
            rustls_pemfile::certs(&mut io::BufReader::new(cert_pem)).collect::<Result<_, _>>()?;

        let key =
            rustls_pemfile::private_key(&mut io::BufReader::new(key_pem))?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "no private key found in PEM data",
                )
            })?;

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        Ok(Self {
            config: Arc::new(config),
        })
    }

    /// Accept a TLS connection from a client.
    ///
    /// Performs the server-side TLS handshake over the given TCP stream.
    /// Returns a [`TlsStream`] ready for encrypted communication.
    pub async fn accept(&self, tcp: TcpStream) -> io::Result<TlsStream> {
        let server_conn = rustls::ServerConnection::new(Arc::clone(&self.config))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let conn = rustls::Connection::Server(server_conn);
        let mut stream = TlsStream::new(tcp, conn);
        stream.complete_handshake().await?;
        Ok(stream)
    }
}

impl From<Arc<ServerConfig>> for TlsAcceptor {
    fn from(config: Arc<ServerConfig>) -> Self {
        Self::new(config)
    }
}

//! Client-side TLS connector.

use std::io;
use std::sync::Arc;

use rustls::ClientConfig;
use rustls::pki_types::ServerName;

use crate::net::TcpStream;

use super::TlsStream;

/// A TLS connector for establishing client-side TLS connections.
///
/// Wraps a [`rustls::ClientConfig`] and provides a method to perform
/// a TLS handshake over an existing TCP connection.
#[derive(Clone)]
pub struct TlsConnector {
    config: Arc<ClientConfig>,
}

impl TlsConnector {
    /// Create a new `TlsConnector` with the given client configuration.
    pub fn new(config: Arc<ClientConfig>) -> Self {
        Self { config }
    }

    /// Create a `TlsConnector` with default configuration using
    /// Mozilla's root certificates for server verification.
    pub fn with_webpki_roots() -> Self {
        let root_store =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Self {
            config: Arc::new(config),
        }
    }

    /// Perform a TLS handshake over the given TCP stream.
    ///
    /// The `server_name` is the DNS name of the server, used for
    /// SNI (Server Name Indication) and certificate verification.
    ///
    /// Returns a [`TlsStream`] ready for encrypted communication.
    pub async fn connect(
        &self,
        server_name: impl TryInto<ServerName<'static>, Error = impl std::fmt::Display>,
        tcp: TcpStream,
    ) -> io::Result<TlsStream> {
        let server_name = server_name
            .try_into()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        let client_conn =
            rustls::ClientConnection::new(Arc::clone(&self.config), server_name)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let conn = rustls::Connection::Client(client_conn);
        let mut stream = TlsStream::new(tcp, conn);
        stream.complete_handshake().await?;
        Ok(stream)
    }
}

impl From<Arc<ClientConfig>> for TlsConnector {
    fn from(config: Arc<ClientConfig>) -> Self {
        Self::new(config)
    }
}

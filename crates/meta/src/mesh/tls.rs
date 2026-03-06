//! TLS support for mesh connections.
//!
//! Provides optional TLS encryption for mesh TCP connections using
//! `tokio-rustls`. Supports mutual TLS (mTLS) where both peers present
//! certificates for authentication.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// TLS configuration for mesh connections.
#[derive(Debug, Clone)]
pub struct MeshTlsConfig {
    /// PEM-encoded certificate chain for this node.
    pub cert_pem: Vec<u8>,
    /// PEM-encoded private key for this node.
    pub key_pem: Vec<u8>,
    /// PEM-encoded CA certificate(s) for verifying peers.
    /// If empty, uses system root certificates.
    pub ca_pem: Vec<u8>,
}

/// Build a rustls `ServerConfig` for the mesh listener.
///
/// Configures mTLS: the server requires client certificates signed by the CA.
pub fn build_server_config(tls: &MeshTlsConfig) -> io::Result<Arc<ServerConfig>> {
    let certs = load_certs(&tls.cert_pem)?;
    let key = load_key(&tls.key_pem)?;

    let client_verifier = if tls.ca_pem.is_empty() {
        // No CA provided — accept any client (TLS encryption only, no mTLS)
        rustls::server::WebPkiClientVerifier::no_client_auth()
    } else {
        let mut root_store = RootCertStore::empty();
        let ca_certs = load_certs(&tls.ca_pem)?;
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        }
        rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
    };

    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    Ok(Arc::new(config))
}

/// Build a rustls `ClientConfig` for outbound mesh connections.
///
/// Configures mTLS: the client presents its certificate and verifies the
/// server's certificate against the CA.
pub fn build_client_config(tls: &MeshTlsConfig) -> io::Result<Arc<ClientConfig>> {
    let certs = load_certs(&tls.cert_pem)?;
    let key = load_key(&tls.key_pem)?;

    if tls.ca_pem.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "CA certificate is required for mesh TLS client config",
        ));
    }
    let mut root_store = RootCertStore::empty();
    let ca_certs = load_certs(&tls.ca_pem)?;
    for cert in ca_certs {
        root_store
            .add(cert)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    }

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    Ok(Arc::new(config))
}

fn load_certs(pem: &[u8]) -> io::Result<Vec<CertificateDer<'static>>> {
    rustls_pemfile::certs(&mut io::BufReader::new(pem)).collect()
}

fn load_key(pem: &[u8]) -> io::Result<PrivateKeyDer<'static>> {
    rustls_pemfile::private_key(&mut io::BufReader::new(pem))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no private key found in PEM"))
}

// ---------------------------------------------------------------------------
// MeshStream: unified stream type for plain and TLS connections
// ---------------------------------------------------------------------------

/// A mesh connection stream — either plain TCP or TLS-encrypted.
///
/// Implements `AsyncRead + AsyncWrite` so it can be used interchangeably
/// with the framing functions throughout the mesh connection code.
pub enum MeshStream {
    /// Unencrypted TCP connection.
    Plain(TcpStream),
    /// TLS-encrypted client connection (outbound).
    ClientTls(tokio_rustls::client::TlsStream<TcpStream>),
    /// TLS-encrypted server connection (inbound).
    ServerTls(tokio_rustls::server::TlsStream<TcpStream>),
}

impl MeshStream {
    /// Configure TCP nodelay on the underlying socket.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        match self {
            MeshStream::Plain(s) => s.set_nodelay(nodelay),
            MeshStream::ClientTls(s) => s.get_ref().0.set_nodelay(nodelay),
            MeshStream::ServerTls(s) => s.get_ref().0.set_nodelay(nodelay),
        }
    }

    /// Get a reference to the underlying TCP stream for socket configuration.
    pub fn tcp_ref(&self) -> &TcpStream {
        match self {
            MeshStream::Plain(s) => s,
            MeshStream::ClientTls(s) => s.get_ref().0,
            MeshStream::ServerTls(s) => s.get_ref().0,
        }
    }
}

impl AsyncRead for MeshStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MeshStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MeshStream::ClientTls(s) => Pin::new(s).poll_read(cx, buf),
            MeshStream::ServerTls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MeshStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MeshStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MeshStream::ClientTls(s) => Pin::new(s).poll_write(cx, buf),
            MeshStream::ServerTls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MeshStream::Plain(s) => Pin::new(s).poll_flush(cx),
            MeshStream::ClientTls(s) => Pin::new(s).poll_flush(cx),
            MeshStream::ServerTls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MeshStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MeshStream::ClientTls(s) => Pin::new(s).poll_shutdown(cx),
            MeshStream::ServerTls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Create a DNS name for mesh peer connections.
///
/// Uses a synthetic name `mesh-node-{node_id}` since mesh peers identify
/// by node ID, not DNS. The CA must issue certs with this SAN or use
/// IP-based SANs.
pub fn mesh_server_name(node_id: u64) -> ServerName<'static> {
    ServerName::try_from(format!("mesh-node-{node_id}"))
        .expect("mesh server name is valid DNS")
}

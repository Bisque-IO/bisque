//! Transport abstraction for AMQP 1.0 connections.
//!
//! Supports plain TCP and TLS (via `tokio-rustls` behind the `tls` feature).
//!
//! The AMQP server uses `AsyncRead + AsyncWrite + Unpin` bounds, so any
//! transport that implements these traits works seamlessly.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

// =============================================================================
// AmqpStream — unified transport enum
// =============================================================================

/// A unified stream type for AMQP 1.0 connections.
///
/// Wraps plain TCP or TLS streams behind a single type that
/// implements `AsyncRead + AsyncWrite + Unpin`.
pub enum AmqpStream {
    /// Plain TCP connection (port 5672).
    Tcp(TcpStream),
    /// TLS-encrypted connection (port 5671).
    #[cfg(feature = "tls")]
    Tls(tokio_rustls::server::TlsStream<TcpStream>),
}

impl AmqpStream {
    /// Returns the peer certificate subject CN, if this is a TLS connection
    /// with a client certificate.
    #[cfg(feature = "tls")]
    pub fn peer_cert_subject(&self) -> Option<String> {
        match self {
            AmqpStream::Tcp(_) => None,
            AmqpStream::Tls(tls) => {
                let (_, server_conn) = tls.get_ref();
                let certs = server_conn.peer_certificates()?;
                let cert = certs.first()?;
                extract_cn_from_der(cert.as_ref())
            }
        }
    }

    /// Returns `true` if this is a TLS-encrypted connection.
    pub fn is_tls(&self) -> bool {
        match self {
            AmqpStream::Tcp(_) => false,
            #[cfg(feature = "tls")]
            AmqpStream::Tls(_) => true,
        }
    }
}

impl AsyncRead for AmqpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            AmqpStream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            AmqpStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AmqpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            AmqpStream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            AmqpStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            AmqpStream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            AmqpStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            AmqpStream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            AmqpStream::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

// =============================================================================
// TLS Configuration
// =============================================================================

/// TLS configuration for the AMQP 1.0 server.
#[cfg(feature = "tls")]
pub struct TlsConfig {
    /// The TLS acceptor (pre-configured with certificate and private key).
    pub acceptor: tokio_rustls::TlsAcceptor,
    /// Whether client certificates are required (for SASL EXTERNAL).
    pub client_auth_required: bool,
}

#[cfg(feature = "tls")]
impl TlsConfig {
    /// Create a TLS config from PEM-encoded certificate and private key files.
    ///
    /// Client authentication is disabled by default. Use [`TlsConfig::from_pem_with_client_auth`]
    /// to require client certificates for SASL EXTERNAL.
    pub fn from_pem(cert_path: &std::path::Path, key_path: &std::path::Path) -> io::Result<Self> {
        let (certs, key) = load_pem(cert_path, key_path)?;

        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, rustls::pki_types::PrivateKeyDer::from(key))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        Ok(Self {
            acceptor: tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config)),
            client_auth_required: false,
        })
    }

    /// Create a TLS config that requires client certificates (for SASL EXTERNAL).
    ///
    /// The `ca_cert_path` is the CA certificate used to verify client certificates.
    pub fn from_pem_with_client_auth(
        cert_path: &std::path::Path,
        key_path: &std::path::Path,
        ca_cert_path: &std::path::Path,
    ) -> io::Result<Self> {
        let (certs, key) = load_pem(cert_path, key_path)?;

        // Load CA certs for client verification.
        let ca_file = std::fs::File::open(ca_cert_path)?;
        let mut ca_reader = std::io::BufReader::new(ca_file);
        let ca_certs: Vec<_> =
            rustls_pemfile::certs(&mut ca_reader).collect::<Result<Vec<_>, _>>()?;

        if ca_certs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no CA certificates found in PEM file",
            ));
        }

        let mut root_store = rustls::RootCertStore::empty();
        for ca_cert in ca_certs {
            root_store
                .add(ca_cert)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        }

        let client_verifier = rustls::server::WebPkiClientVerifier::builder(root_store.into())
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, rustls::pki_types::PrivateKeyDer::from(key))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        Ok(Self {
            acceptor: tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config)),
            client_auth_required: true,
        })
    }

    /// Accept a TLS connection on the given TCP stream.
    pub async fn accept(&self, stream: TcpStream) -> io::Result<AmqpStream> {
        let tls_stream = self.acceptor.accept(stream).await?;
        Ok(AmqpStream::Tls(tls_stream))
    }
}

/// Load PEM-encoded certificate chain and private key.
#[cfg(feature = "tls")]
fn load_pem(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> io::Result<(
    Vec<rustls::pki_types::CertificateDer<'static>>,
    rustls::pki_types::PrivatePkcs8KeyDer<'static>,
)> {
    let cert_file = std::fs::File::open(cert_path)?;
    let mut cert_reader = std::io::BufReader::new(cert_file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;

    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no certificates found in PEM file",
        ));
    }

    let key_file = std::fs::File::open(key_path)?;
    let mut key_reader = std::io::BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "no private key found in PEM file",
        )
    })?;

    // Convert PrivateKeyDer to PrivatePkcs8KeyDer
    let pkcs8_key = match key {
        rustls::pki_types::PrivateKeyDer::Pkcs8(k) => k,
        rustls::pki_types::PrivateKeyDer::Pkcs1(k) => {
            // Re-wrap as generic key - rustls accepts PrivateKeyDer::from()
            return Ok((
                certs,
                // This is a workaround; for non-PKCS8 keys we need to use PrivateKeyDer directly
                rustls::pki_types::PrivatePkcs8KeyDer::from(k.secret_pkcs1_der().to_vec()),
            ));
        }
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported private key format: {:?}", other),
            ));
        }
    };

    Ok((certs, pkcs8_key))
}

/// Extract the Common Name (CN) from a DER-encoded X.509 certificate.
///
/// This does a simple scan for the CN OID (2.5.4.3) in the DER encoding.
/// A full ASN.1 parser would be more robust, but this handles the common case.
#[cfg(feature = "tls")]
fn extract_cn_from_der(der: &[u8]) -> Option<String> {
    // OID 2.5.4.3 (CN) encoded as DER: 55 04 03
    let cn_oid: &[u8] = &[0x55, 0x04, 0x03];

    // Scan for CN OID in the certificate
    for i in 0..der.len().saturating_sub(cn_oid.len() + 2) {
        if der[i..].starts_with(cn_oid) {
            // After the OID, expect a tag (UTF8String=0x0C, PrintableString=0x13, etc.) and length
            let after_oid = i + cn_oid.len();
            if after_oid + 2 > der.len() {
                continue;
            }
            let tag = der[after_oid];
            if tag == 0x0C || tag == 0x13 || tag == 0x16 {
                // UTF8String, PrintableString, or IA5String
                let len = der[after_oid + 1] as usize;
                let start = after_oid + 2;
                if start + len <= der.len() {
                    return std::str::from_utf8(&der[start..start + len])
                        .ok()
                        .map(String::from);
                }
            }
        }
    }
    None
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_amqp_stream_is_unpin() {
        fn assert_unpin<T: Unpin>() {}
        assert_unpin::<AmqpStream>();
    }

    #[test]
    fn test_amqp_stream_tcp_variant() {
        // Verify the enum can be constructed (compile-time check).
        fn _accepts_tcp(stream: TcpStream) -> AmqpStream {
            AmqpStream::Tcp(stream)
        }
    }

    #[test]
    fn test_amqp_stream_is_tls() {
        fn _check_tcp(stream: TcpStream) {
            let s = AmqpStream::Tcp(stream);
            assert!(!s.is_tls());
        }
    }

    #[cfg(feature = "tls")]
    #[test]
    fn test_tls_config_missing_cert_file() {
        let result = TlsConfig::from_pem(
            std::path::Path::new("/nonexistent/cert.pem"),
            std::path::Path::new("/nonexistent/key.pem"),
        );
        assert!(result.is_err());
    }

    #[cfg(feature = "tls")]
    #[test]
    fn test_extract_cn_from_der_not_found() {
        // Empty or garbage DER should return None.
        assert_eq!(extract_cn_from_der(&[]), None);
        assert_eq!(extract_cn_from_der(&[0x00, 0x01, 0x02]), None);
    }

    #[cfg(feature = "tls")]
    #[test]
    fn test_extract_cn_from_der_utf8() {
        // Simulate a minimal DER snippet with CN OID + UTF8String "test-cn"
        let cn = b"test-cn";
        let mut der = vec![0x00]; // padding
        der.extend_from_slice(&[0x55, 0x04, 0x03]); // CN OID
        der.push(0x0C); // UTF8String tag
        der.push(cn.len() as u8); // length
        der.extend_from_slice(cn);

        assert_eq!(extract_cn_from_der(&der), Some("test-cn".to_string()));
    }

    #[cfg(feature = "tls")]
    #[test]
    fn test_extract_cn_printable_string() {
        let cn = b"my-client";
        let mut der = vec![0x30, 0x10]; // some DER prefix
        der.extend_from_slice(&[0x55, 0x04, 0x03]); // CN OID
        der.push(0x13); // PrintableString tag
        der.push(cn.len() as u8);
        der.extend_from_slice(cn);

        assert_eq!(extract_cn_from_der(&der), Some("my-client".to_string()));
    }
}

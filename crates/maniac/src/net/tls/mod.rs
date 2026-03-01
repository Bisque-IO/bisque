//! TLS support for maniac networking.
//!
//! This module provides TLS client and server support using rustls with
//! aws-lc-rs as the cryptographic provider.
//!
//! # Feature
//!
//! This module requires the `tls` feature to be enabled.
//!
//! # Types
//!
//! - [`TlsConnector`] — Client-side TLS connector
//! - [`TlsAcceptor`] — Server-side TLS acceptor
//! - [`TlsStream`] — A TLS-encrypted stream wrapping a TCP connection

#![allow(async_fn_in_trait)]

mod acceptor;
mod connector;
mod stream;

pub use acceptor::TlsAcceptor;
pub use connector::TlsConnector;
pub use stream::TlsStream;

// Re-export rustls types that users commonly need for configuration.
pub use rustls::pki_types::ServerName;
pub use rustls::{ClientConfig, ServerConfig};

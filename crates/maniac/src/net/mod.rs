//! Network utilities.
//!
//! Currently, TCP/UDP/Unix socket are implemented.
//!
//! This module provides async networking primitives that integrate with
//! the maniac IO driver pool.

#![allow(async_fn_in_trait)]

mod opts;
mod socket;
mod split;
mod tcp;
#[cfg(feature = "tls")]
pub mod tls;
mod udp;
#[cfg(unix)]
mod unix;

pub use opts::SocketOpts;
pub(crate) use socket::Socket;
pub use split::*;
pub use tcp::*;
pub use udp::*;
#[cfg(unix)]
pub use unix::*;

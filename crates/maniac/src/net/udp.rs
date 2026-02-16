//! UDP networking primitives.
//!
//! This module provides `UdpSocket` for async UDP networking.

use std::io;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};
use std::sync::Arc;

use socket2::{Protocol, SockAddr, Socket as Socket2, Type};

use crate::buf::{BufResult, IoBuf, IoBufMut, SetLen};
use crate::driver::{IoPool, RecvFromResult};
use crate::net::{Socket, SocketOpts};

/// A UDP socket.
///
/// UDP is "connectionless", unlike TCP. Meaning, regardless of what address
/// you've bound to, a `UdpSocket` is free to communicate with many different
/// remotes. There are basically two main ways to use `UdpSocket`:
///
/// * one to many: [`bind`](`UdpSocket::bind`) and use
///   [`send_to`](`UdpSocket::send_to`) and
///   [`recv_from`](`UdpSocket::recv_from`) to communicate with many different
///   addresses
/// * one to one: [`connect`](`UdpSocket::connect`) and associate with a single
///   address, using [`send`](`UdpSocket::send`) and [`recv`](`UdpSocket::recv`)
///   to communicate only with that remote address
///
/// # Examples
///
/// ```ignore
/// use maniac::net::UdpSocket;
/// use maniac::driver::IoPool;
///
/// let pool = IoPool::new()?;
/// let socket = UdpSocket::bind("127.0.0.1:0", pool.clone())?;
/// let addr = socket.local_addr()?;
///
/// socket.send_to(b"Hello", "127.0.0.1:8080").await?;
/// ```
#[derive(Debug)]
pub struct UdpSocket {
    inner: Socket,
}

impl UdpSocket {
    /// Creates a new UDP socket and attempt to bind it to the addr provided.
    pub fn bind(addr: impl Into<SocketAddr>, pool: Arc<IoPool>) -> io::Result<Self> {
        Self::bind_with_options(addr, &SocketOpts::default(), pool)
    }

    /// Creates a new UDP socket with [`SocketOpts`] and attempt to bind it to
    /// the addr provided.
    pub fn bind_with_options(
        addr: impl Into<SocketAddr>,
        opts: &SocketOpts,
        pool: Arc<IoPool>,
    ) -> io::Result<Self> {
        let addr: SocketAddr = addr.into();
        let socket = Socket::bind(
            &SockAddr::from(addr),
            Type::DGRAM,
            Some(Protocol::UDP),
            &pool,
        )?;
        opts.setup_socket(&socket)?;
        Ok(Self { inner: socket })
    }

    /// Connects this UDP socket to a remote address, allowing the `send` and
    /// `recv` to be used to send data and also applies filters to only
    /// receive data from the specified address.
    ///
    /// Note that usually, a successful `connect` call does not specify
    /// that there is a remote server listening on the port, rather, such an
    /// error would only be detected after the first send.
    pub async fn connect(&self, addr: impl Into<SocketAddr>) -> io::Result<()> {
        let addr: SocketAddr = addr.into();
        self.inner.connect(&SockAddr::from(addr)).await
    }

    /// Creates new UdpSocket from a std::net::UdpSocket.
    pub fn from_std(socket: std::net::UdpSocket, pool: &IoPool) -> io::Result<Self> {
        Ok(Self {
            inner: Socket::from_socket2(Socket2::from(socket), pool)?,
        })
    }

    /// Close the socket.
    pub fn close(self) {
        drop(self);
    }

    /// Returns the socket address of the remote peer this socket was connected
    /// to.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner
            .peer_addr()
            .map(|addr| addr.as_socket().expect("should be SocketAddr"))
    }

    /// Returns the local address that this socket is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner
            .local_addr()
            .map(|addr| addr.as_socket().expect("should be SocketAddr"))
    }

    /// Receives a packet of data from the socket into the buffer, returning the
    /// original buffer and quantity of data received.
    pub async fn recv<T: IoBufMut + SetLen + Send>(&self, buffer: T) -> BufResult<usize, T> {
        self.inner.recv(buffer).await
    }

    /// Sends some data to the socket from the buffer, returning the original
    /// buffer and quantity of data sent.
    pub async fn send<T: IoBuf + Send>(&self, buffer: T) -> BufResult<usize, T> {
        self.inner.send(buffer).await
    }

    /// Receives a single datagram message on the socket. On success, returns
    /// the number of bytes received and the origin.
    pub async fn recv_from<T: IoBufMut + SetLen + Send>(
        &self,
        buffer: T,
    ) -> BufResult<(usize, SocketAddr), T> {
        let RecvFromResult { result, buf, addr } = self.inner.recv_from(buffer).await;
        match result {
            Ok(n) => {
                let socket_addr = addr.expect("recv_from should return address");
                BufResult(Ok((n, socket_addr)), buf)
            }
            Err(e) => BufResult(Err(e), buf),
        }
    }

    /// Sends data on the socket to the given address. On success, returns the
    /// number of bytes sent.
    pub async fn send_to<T: IoBuf + Send>(
        &self,
        buffer: T,
        addr: impl Into<SocketAddr>,
    ) -> BufResult<usize, T> {
        let addr: SocketAddr = addr.into();
        self.inner.send_to(buffer, &SockAddr::from(addr)).await
    }

    /// Gets a socket option.
    ///
    /// # Safety
    ///
    /// The caller must ensure `T` is the correct type for `level` and `name`.
    pub unsafe fn get_socket_option<T: Copy>(&self, level: i32, name: i32) -> io::Result<T> {
        unsafe { self.inner.get_socket_option(level, name) }
    }

    /// Sets a socket option.
    ///
    /// # Safety
    ///
    /// The caller must ensure `T` is the correct type for `level` and `name`.
    pub unsafe fn set_socket_option<T: Copy>(
        &self,
        level: i32,
        name: i32,
        value: &T,
    ) -> io::Result<()> {
        unsafe { self.inner.set_socket_option(level, name, value) }
    }
}

#[cfg(unix)]
impl AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(windows)]
impl AsRawSocket for UdpSocket {
    fn as_raw_socket(&self) -> RawSocket {
        self.inner.as_raw_socket()
    }
}

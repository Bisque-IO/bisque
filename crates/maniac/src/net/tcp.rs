//! TCP networking primitives.
//!
//! This module provides `TcpListener` and `TcpStream` for async TCP networking.

use std::io;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, FromRawSocket, RawSocket};
use std::sync::Arc;

use socket2::{Protocol, SockAddr, Socket as Socket2, Type};

use crate::buf::{BufResult, IoBuf, IoBufMut, SetLen};
use crate::driver::{AcceptResult, IoPool};
use crate::io::{AsyncRead, AsyncWrite};
use crate::loop_read_vectored;
use crate::loop_write_vectored;
use crate::net::{
    OwnedReadHalf, OwnedWriteHalf, ReadHalf, Socket, SocketOpts, WriteHalf, into_split, split,
};

/// A TCP socket server, listening for connections.
///
/// You can accept a new connection by using the
/// [`accept`](`TcpListener::accept`) method.
///
/// # Examples
///
/// ```ignore
/// use maniac::net::{TcpListener, TcpStream};
/// use maniac::driver::IoPool;
///
/// let pool = IoPool::new()?;
/// let listener = TcpListener::listen("127.0.0.1:0", &pool)?;
/// let addr = listener.local_addr()?;
///
/// let (stream, peer_addr) = listener.accept().await?;
/// ```
#[derive(Debug)]
pub struct TcpListener {
    inner: Socket,
    pool: Arc<IoPool>,
}

impl TcpListener {
    /// Creates a new `TcpListener` bound to the specified address and ready
    /// to accept connections.
    ///
    /// This is a convenience method that binds and listens in one step.
    /// Binding with a port number of 0 will request that the OS assigns a port.
    ///
    /// Enables `SO_REUSEADDR` by default.
    pub fn listen(addr: impl Into<SocketAddr>, pool: Arc<IoPool>) -> io::Result<Self> {
        Self::listen_with_options(addr, &SocketOpts::default().reuse_address(true), 128, pool)
    }

    /// Creates a new `TcpListener` bound to the specified address with custom
    /// socket options and backlog.
    ///
    /// The `backlog` parameter sets the maximum number of pending connections
    /// queued by the OS before new connections are refused.
    pub fn listen_with_options(
        addr: impl Into<SocketAddr>,
        options: &SocketOpts,
        backlog: i32,
        pool: Arc<IoPool>,
    ) -> io::Result<Self> {
        let addr: SocketAddr = addr.into();
        let sa = SockAddr::from(addr);
        let socket = Socket::new(sa.domain(), Type::STREAM, Some(Protocol::TCP), &pool)?;
        options.setup_socket(&socket)?;
        socket.inner.bind(&sa)?;
        socket.listen(backlog)?;
        Ok(Self {
            inner: socket,
            pool,
        })
    }

    /// Creates new TcpListener from a [`std::net::TcpListener`].
    pub fn from_std(listener: std::net::TcpListener, pool: Arc<IoPool>) -> io::Result<Self> {
        Ok(Self {
            inner: Socket::from_socket2(Socket2::from(listener), &pool)?,
            pool,
        })
    }

    /// Close the socket.
    pub fn close(self) {
        drop(self);
    }

    /// Accepts a new incoming connection from this listener.
    ///
    /// This function will yield once a new TCP connection is established. When
    /// established, the corresponding [`TcpStream`] and the remote peer's
    /// address will be returned.
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        self.accept_with_options(&SocketOpts::default()).await
    }

    /// Accepts a new incoming connection from this listener, and sets options.
    ///
    /// This function will yield once a new TCP connection is established. When
    /// established, the corresponding [`TcpStream`] and the remote peer's
    /// address will be returned.
    pub async fn accept_with_options(
        &self,
        options: &SocketOpts,
    ) -> io::Result<(TcpStream, SocketAddr)> {
        let AcceptResult { result, addr } = self.inner.accept().await;
        let fd = result?;

        // Create a new socket from the accepted fd
        #[cfg(unix)]
        let socket2 = unsafe { Socket2::from_raw_fd(fd) };
        #[cfg(windows)]
        let socket2 = unsafe { Socket2::from_raw_socket(fd as RawSocket) };

        let socket = Socket::from_socket2(socket2, &self.pool)?;
        options.setup_socket(&socket)?;

        let stream = TcpStream { inner: socket };

        // Get peer address - either from accept result or via getpeername
        let peer_addr = match addr {
            Some(a) => a,
            None => stream.peer_addr()?,
        };

        Ok((stream, peer_addr))
    }

    /// Returns the local address that this listener is bound to.
    ///
    /// This can be useful, for example, when binding to port 0 to
    /// figure out which port was actually bound.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner
            .local_addr()
            .map(|addr| addr.as_socket().expect("should be SocketAddr"))
    }

    /// Get a reference to the underlying IO pool.
    pub fn pool(&self) -> &Arc<IoPool> {
        &self.pool
    }
}

#[cfg(unix)]
impl AsRawFd for TcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(windows)]
impl AsRawSocket for TcpListener {
    fn as_raw_socket(&self) -> RawSocket {
        self.inner.as_raw_socket()
    }
}

/// A TCP stream between a local and a remote socket.
///
/// A TCP stream can either be created by connecting to an endpoint, via the
/// `connect` method, or by accepting a connection from a listener.
///
/// # Examples
///
/// ```ignore
/// use maniac::net::TcpStream;
/// use maniac::driver::IoPool;
///
/// let pool = IoPool::new()?;
/// let mut stream = TcpStream::connect("127.0.0.1:8080", pool).await?;
/// // Write some data using async IO
/// ```
#[derive(Debug)]
pub struct TcpStream {
    inner: Socket,
}

impl TcpStream {
    /// Opens a TCP connection to a remote host.
    pub async fn connect(addr: impl Into<SocketAddr>, pool: Arc<IoPool>) -> io::Result<Self> {
        Self::connect_with_options(addr, &SocketOpts::default(), pool).await
    }

    /// Opens a TCP connection to a remote host using `SocketOpts`.
    #[cfg(windows)]
    pub async fn connect_with_options(
        addr: impl Into<SocketAddr>,
        options: &SocketOpts,
        pool: Arc<IoPool>,
    ) -> io::Result<Self> {
        use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

        let addr: SocketAddr = addr.into();
        let addr2 = SockAddr::from(addr);

        // On Windows, we need to bind before connect
        let bind_addr = if addr.is_ipv4() {
            SockAddr::from(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
        } else {
            SockAddr::from(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0))
        };
        let socket = Socket::bind(&bind_addr, Type::STREAM, Some(Protocol::TCP), &pool)?;

        options.setup_socket(&socket)?;
        socket.connect(&addr2).await?;
        Ok(Self { inner: socket })
    }

    /// Opens a TCP connection to a remote host using `SocketOpts`.
    #[cfg(not(windows))]
    pub async fn connect_with_options(
        addr: impl Into<SocketAddr>,
        options: &SocketOpts,
        pool: Arc<IoPool>,
    ) -> io::Result<Self> {
        let addr: SocketAddr = addr.into();
        let addr2 = SockAddr::from(addr);

        let socket = Socket::new(addr2.domain(), Type::STREAM, Some(Protocol::TCP), &pool)?;

        options.setup_socket(&socket)?;
        socket.connect(&addr2).await?;
        Ok(Self { inner: socket })
    }

    /// Bind to `bind_addr` then opens a TCP connection to a remote host.
    pub async fn bind_and_connect(
        bind_addr: SocketAddr,
        addr: impl Into<SocketAddr>,
        pool: Arc<IoPool>,
    ) -> io::Result<Self> {
        Self::bind_and_connect_with_options(bind_addr, addr, &SocketOpts::default(), pool).await
    }

    /// Bind to `bind_addr` then opens a TCP connection to a remote host using
    /// `SocketOpts`.
    pub async fn bind_and_connect_with_options(
        bind_addr: SocketAddr,
        addr: impl Into<SocketAddr>,
        options: &SocketOpts,
        pool: Arc<IoPool>,
    ) -> io::Result<Self> {
        let addr: SocketAddr = addr.into();
        let addr = SockAddr::from(addr);
        let bind_addr = SockAddr::from(bind_addr);

        let socket = Socket::bind(&bind_addr, Type::STREAM, Some(Protocol::TCP), &pool)?;
        options.setup_socket(&socket)?;
        socket.connect(&addr).await?;
        Ok(Self { inner: socket })
    }

    /// Creates new TcpStream from a [`std::net::TcpStream`].
    pub fn from_std(stream: std::net::TcpStream, pool: &IoPool) -> io::Result<Self> {
        Ok(Self {
            inner: Socket::from_socket2(Socket2::from(stream), pool)?,
        })
    }

    /// Close the socket.
    pub fn close(self) {
        drop(self);
    }

    /// Returns the socket address of the remote peer of this TCP connection.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner
            .peer_addr()
            .map(|addr| addr.as_socket().expect("should be SocketAddr"))
    }

    /// Returns the socket address of the local half of this TCP connection.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner
            .local_addr()
            .map(|addr| addr.as_socket().expect("should be SocketAddr"))
    }

    /// Receive data from the stream.
    ///
    /// Returns a future that resolves to the number of bytes read and the buffer.
    pub async fn recv<B: IoBufMut + SetLen + Send>(&self, buf: B) -> BufResult<usize, B> {
        self.inner.recv(buf).await
    }

    /// Send data to the stream.
    ///
    /// Returns a future that resolves to the number of bytes written and the buffer.
    pub async fn send<T: IoBuf + Send>(&self, buf: T) -> BufResult<usize, T> {
        self.inner.send(buf).await
    }

    /// Splits a [`TcpStream`] into a read half and a write half, which can be
    /// used to read and write the stream concurrently.
    ///
    /// This method is more efficient than
    /// [`into_split`](TcpStream::into_split), but the halves cannot
    /// be moved into independently spawned tasks.
    pub fn split(&self) -> (ReadHalf<'_, Self>, WriteHalf<'_, Self>) {
        split(self)
    }

    /// Splits a [`TcpStream`] into a read half and a write half, which can be
    /// used to read and write the stream concurrently.
    ///
    /// Unlike [`split`](TcpStream::split), the owned halves can be moved to
    /// separate tasks, however this comes at the cost of a heap allocation.
    pub fn into_split(self) -> (OwnedReadHalf<Self>, OwnedWriteHalf<Self>) {
        into_split(self)
    }

    /// Gets the value of the `TCP_NODELAY` option on this socket.
    ///
    /// For more information about this option, see
    /// [`TcpStream::set_nodelay`].
    pub fn nodelay(&self) -> io::Result<bool> {
        self.inner.inner.tcp_nodelay()
    }

    /// Sets the value of the TCP_NODELAY option on this socket.
    ///
    /// If set, this option disables the Nagle algorithm. This means
    /// that segments are always sent as soon as possible, even if
    /// there is only a small amount of data. When not set, data is
    /// buffered until there is a sufficient amount to send out,
    /// thereby avoiding the frequent sending of small packets.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.inner.inner.set_tcp_nodelay(nodelay)
    }

    /// Get the underlying socket.
    #[allow(dead_code)]
    pub(crate) fn socket(&self) -> &Socket {
        &self.inner
    }
}

impl AsyncRead for TcpStream {
    #[inline]
    async fn read<B: IoBufMut + SetLen + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        (&*self).read(buf).await
    }

    #[inline]
    async fn read_vectored<V: crate::buf::IoVectoredBufMut + Send>(
        &mut self,
        buf: V,
    ) -> BufResult<usize, V> {
        (&*self).read_vectored(buf).await
    }
}

impl AsyncRead for &TcpStream {
    async fn read<B: IoBufMut + SetLen + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        self.inner.recv(buf).await
    }

    async fn read_vectored<V: crate::buf::IoVectoredBufMut + Send>(
        &mut self,
        buf: V,
    ) -> BufResult<usize, V> {
        loop_read_vectored!(buf, iter, self.read(iter))
    }
}

impl AsyncWrite for TcpStream {
    #[inline]
    async fn write<T: IoBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
        (&*self).write(buf).await
    }

    #[inline]
    async fn write_vectored<T: crate::buf::IoVectoredBuf + Send>(
        &mut self,
        buf: T,
    ) -> BufResult<usize, T> {
        (&*self).write_vectored(buf).await
    }

    #[inline]
    async fn flush(&mut self) -> io::Result<()> {
        (&*self).flush().await
    }

    #[inline]
    async fn shutdown(&mut self) -> io::Result<()> {
        (&*self).shutdown().await
    }
}

impl AsyncWrite for &TcpStream {
    async fn write<T: IoBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
        self.inner.send(buf).await
    }

    async fn write_vectored<T: crate::buf::IoVectoredBuf + Send>(
        &mut self,
        buf: T,
    ) -> BufResult<usize, T> {
        loop_write_vectored!(buf, iter, self.write(iter))
    }

    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.inner.shutdown(std::net::Shutdown::Write)
    }
}

#[cfg(unix)]
impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(windows)]
impl AsRawSocket for TcpStream {
    fn as_raw_socket(&self) -> RawSocket {
        self.inner.as_raw_socket()
    }
}

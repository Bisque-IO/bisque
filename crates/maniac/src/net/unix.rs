//! Unix domain socket networking primitives.
//!
//! This module provides `UnixListener` and `UnixStream` for async Unix socket networking.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::Path;
use std::sync::Arc;

use socket2::{Domain, SockAddr, Socket as Socket2, Type};

use crate::buf::{BufResult, IoBuf, IoBufMut, IoVectoredBuf, IoVectoredBufMut, SetLen};
use crate::driver::{AcceptResult, IoPool};
use crate::io::{AsyncRead, AsyncWrite};
use crate::loop_read_vectored;
use crate::loop_write_vectored;
use crate::net::{
    OwnedReadHalf, OwnedWriteHalf, ReadHalf, Socket, SocketOpts, WriteHalf, into_split, split,
};

/// A Unix socket server, listening for connections.
///
/// You can accept a new connection by using the [`UnixListener::accept`]
/// method.
///
/// # Examples
///
/// ```ignore
/// use maniac::net::{UnixListener, UnixStream};
/// use maniac::driver::IoPool;
///
/// let pool = IoPool::new()?;
/// let listener = UnixListener::listen("/tmp/my.sock", pool.clone())?;
/// let (stream, addr) = listener.accept().await?;
/// ```
#[derive(Debug)]
pub struct UnixListener {
    inner: Socket,
    pool: Arc<IoPool>,
}

impl UnixListener {
    /// Creates a new [`UnixListener`] bound to the specified path and ready
    /// to accept connections.
    ///
    /// The file path cannot yet exist, and will be cleaned up upon dropping.
    pub fn listen(path: impl AsRef<Path>, pool: Arc<IoPool>) -> io::Result<Self> {
        Self::listen_addr(&SockAddr::unix(path)?, pool)
    }

    /// Creates a new [`UnixListener`] bound to the specified [`SockAddr`]
    /// and ready to accept connections.
    pub fn listen_addr(addr: &SockAddr, pool: Arc<IoPool>) -> io::Result<Self> {
        Self::listen_with_options(addr, &SocketOpts::default(), 1024, pool)
    }

    /// Creates a new [`UnixListener`] with custom socket options and backlog.
    ///
    /// The `backlog` parameter sets the maximum number of pending connections
    /// queued by the OS before new connections are refused.
    pub fn listen_with_options(
        addr: &SockAddr,
        opts: &SocketOpts,
        backlog: i32,
        pool: Arc<IoPool>,
    ) -> io::Result<Self> {
        if !addr.is_unix() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "addr is not unix socket address",
            ));
        }

        let socket = Socket::bind(addr, Type::STREAM, None, &pool)?;
        opts.setup_socket(&socket)?;
        socket.listen(backlog)?;
        Ok(UnixListener {
            inner: socket,
            pool,
        })
    }

    /// Creates new UnixListener from a [`std::os::unix::net::UnixListener`].
    pub fn from_std(
        listener: std::os::unix::net::UnixListener,
        pool: Arc<IoPool>,
    ) -> io::Result<Self> {
        use std::os::fd::IntoRawFd;
        Ok(Self {
            inner: Socket::from_socket2(
                unsafe { Socket2::from_raw_fd(listener.into_raw_fd()) },
                &pool,
            )?,
            pool,
        })
    }

    /// Close the socket.
    pub fn close(self) {
        drop(self);
    }

    /// Accepts a new incoming connection from this listener.
    ///
    /// This function will yield once a new Unix domain socket connection
    /// is established. When established, the corresponding [`UnixStream`] and
    /// will be returned.
    pub async fn accept(&self) -> io::Result<(UnixStream, SockAddr)> {
        self.accept_with_options(&SocketOpts::default()).await
    }

    /// Accepts a new incoming connection from this listener, and sets options.
    ///
    /// This function will yield once a new Unix domain socket connection
    /// is established. When established, the corresponding [`UnixStream`] and
    /// will be returned.
    pub async fn accept_with_options(
        &self,
        options: &SocketOpts,
    ) -> io::Result<(UnixStream, SockAddr)> {
        let AcceptResult { result, addr } = self.inner.accept().await;
        let fd = result?;

        // Create a new socket from the accepted fd
        let socket2 = unsafe { Socket2::from_raw_fd(fd) };
        let socket = Socket::from_socket2(socket2, &self.pool)?;
        options.setup_socket(&socket)?;

        let stream = UnixStream { inner: socket };

        // For Unix sockets, peer address might not always be available
        // Create a SockAddr from the socket's peer_addr if available
        let peer_addr = if let Some(socket_addr) = addr {
            SockAddr::from(socket_addr)
        } else {
            // Try to get peer address from the stream
            stream.peer_addr().unwrap_or_else(|_| {
                // Return an empty unix address as fallback
                SockAddr::unix("").unwrap_or_else(|_| unsafe { std::mem::zeroed() })
            })
        };

        Ok((stream, peer_addr))
    }

    /// Returns the local address that this listener is bound to.
    pub fn local_addr(&self) -> io::Result<SockAddr> {
        self.inner.local_addr()
    }

    /// Get a reference to the underlying IO pool.
    pub fn pool(&self) -> &Arc<IoPool> {
        &self.pool
    }
}

impl AsRawFd for UnixListener {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

/// A Unix stream between two local sockets.
///
/// A Unix stream can either be created by connecting to an endpoint, via the
/// `connect` method, or by accepting a connection from a listener.
///
/// # Examples
///
/// ```ignore
/// use maniac::net::UnixStream;
/// use maniac::driver::IoPool;
///
/// let pool = IoPool::new()?;
/// let stream = UnixStream::connect("/tmp/my.sock", pool).await?;
/// ```
#[derive(Debug)]
pub struct UnixStream {
    inner: Socket,
}

impl UnixStream {
    /// Opens a Unix connection to the specified file path. There must be a
    /// [`UnixListener`] or equivalent listening on the corresponding Unix
    /// domain socket to successfully connect and return a [`UnixStream`].
    pub async fn connect(path: impl AsRef<Path>, pool: Arc<IoPool>) -> io::Result<Self> {
        Self::connect_addr(&SockAddr::unix(path)?, pool).await
    }

    /// Opens a Unix connection to the specified address. There must be a
    /// [`UnixListener`] or equivalent listening on the corresponding Unix
    /// domain socket to successfully connect and return a [`UnixStream`].
    pub async fn connect_addr(addr: &SockAddr, pool: Arc<IoPool>) -> io::Result<Self> {
        Self::connect_with_options(addr, &SocketOpts::default(), pool).await
    }

    /// Opens a Unix connection to the specified address with [`SocketOpts`].
    /// There must be a [`UnixListener`] or equivalent listening on the
    /// corresponding Unix domain socket to successfully connect and return
    /// a [`UnixStream`].
    pub async fn connect_with_options(
        addr: &SockAddr,
        options: &SocketOpts,
        pool: Arc<IoPool>,
    ) -> io::Result<Self> {
        if !addr.is_unix() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "addr is not unix socket address",
            ));
        }

        let socket = Socket::new(Domain::UNIX, Type::STREAM, None, &pool)?;
        options.setup_socket(&socket)?;
        socket.connect(addr).await?;
        Ok(UnixStream { inner: socket })
    }

    /// Creates new UnixStream from a [`std::os::unix::net::UnixStream`].
    pub fn from_std(stream: std::os::unix::net::UnixStream, pool: &IoPool) -> io::Result<Self> {
        use std::os::fd::IntoRawFd;
        Ok(Self {
            inner: Socket::from_socket2(
                unsafe { Socket2::from_raw_fd(stream.into_raw_fd()) },
                pool,
            )?,
        })
    }

    /// Close the socket.
    pub fn close(self) {
        drop(self);
    }

    /// Returns the socket path of the remote peer of this connection.
    pub fn peer_addr(&self) -> io::Result<SockAddr> {
        self.inner.peer_addr()
    }

    /// Returns the socket path of the local half of this connection.
    pub fn local_addr(&self) -> io::Result<SockAddr> {
        self.inner.local_addr()
    }

    /// Splits a [`UnixStream`] into a read half and a write half, which can be
    /// used to read and write the stream concurrently.
    ///
    /// This method is more efficient than
    /// [`into_split`](UnixStream::into_split), but the halves cannot
    /// be moved into independently spawned tasks.
    pub fn split(&self) -> (ReadHalf<'_, Self>, WriteHalf<'_, Self>) {
        split(self)
    }

    /// Splits a [`UnixStream`] into a read half and a write half, which can be
    /// used to read and write the stream concurrently.
    ///
    /// Unlike [`split`](UnixStream::split), the owned halves can be moved to
    /// separate tasks, however this comes at the cost of a heap allocation.
    pub fn into_split(self) -> (OwnedReadHalf<Self>, OwnedWriteHalf<Self>) {
        into_split(self)
    }
}

impl AsyncRead for UnixStream {
    #[inline]
    async fn read<B: IoBufMut + SetLen + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        (&*self).read(buf).await
    }

    #[inline]
    async fn read_vectored<V: IoVectoredBufMut + Send>(&mut self, buf: V) -> BufResult<usize, V> {
        (&*self).read_vectored(buf).await
    }
}

impl AsyncRead for &UnixStream {
    async fn read<B: IoBufMut + SetLen + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        self.inner.recv(buf).await
    }

    async fn read_vectored<V: IoVectoredBufMut + Send>(&mut self, buf: V) -> BufResult<usize, V> {
        loop_read_vectored!(buf, iter, self.read(iter))
    }
}

impl AsyncWrite for UnixStream {
    #[inline]
    async fn write<T: IoBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
        (&*self).write(buf).await
    }

    #[inline]
    async fn write_vectored<T: IoVectoredBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
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

impl AsyncWrite for &UnixStream {
    async fn write<T: IoBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
        self.inner.send(buf).await
    }

    async fn write_vectored<T: IoVectoredBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
        loop_write_vectored!(buf, iter, self.write(iter))
    }

    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.inner.shutdown(std::net::Shutdown::Write)
    }
}

impl AsRawFd for UnixStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

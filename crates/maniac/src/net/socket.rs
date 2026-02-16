//! Core socket abstraction.
//!
//! This module provides a Socket wrapper that integrates with the maniac IO driver.

use std::io;
use std::mem::{ManuallyDrop, MaybeUninit};
#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};

use socket2::{Domain, Protocol, SockAddr, Socket as Socket2, Type};

use crate::buf::{IoBuf, IoBufMut, SetLen};
use crate::driver::{
    AcceptFuture, ConnectFuture, IoHandle, IoPool, ReadFuture, RecvFromFuture, SendToFuture,
    WriteFuture,
};

/// Socket wrapper that integrates with maniac's IO driver.
///
/// This type holds both a socket2::Socket for synchronous setup operations
/// and an IoHandle for async IO operations through the driver.
///
/// IMPORTANT: The Socket2 is wrapped in ManuallyDrop to prevent it from
/// closing the fd when dropped. Instead, Drop notifies the IO driver to
/// unregister and close the fd properly. This prevents race conditions
/// where pending IO operations reference a closed/reused fd.
#[derive(Debug)]
pub struct Socket {
    /// The underlying socket (used for setup and address queries).
    /// ManuallyDrop prevents Socket2 from closing the fd - we handle that ourselves.
    pub(crate) inner: ManuallyDrop<Socket2>,
    /// Handle for async IO operations
    pub(crate) handle: IoHandle,
}

impl Socket {
    /// Create a new socket from a socket2 socket, registering it with the IO pool.
    pub fn from_socket2(socket: Socket2, pool: &IoPool) -> io::Result<Self> {
        // Set non-blocking mode for async operations
        socket.set_nonblocking(true)?;

        #[cfg(unix)]
        let fd = socket.as_raw_fd();
        #[cfg(windows)]
        let fd = socket.as_raw_socket() as i32;

        let handle = pool.open(fd);
        Ok(Self {
            inner: ManuallyDrop::new(socket),
            handle,
        })
    }

    /// Create a new socket from a socket2 socket with specific thread assignment.
    pub fn from_socket2_on_thread(
        socket: Socket2,
        pool: &IoPool,
        thread_idx: usize,
    ) -> io::Result<Self> {
        socket.set_nonblocking(true)?;

        #[cfg(unix)]
        let fd = socket.as_raw_fd();
        #[cfg(windows)]
        let fd = socket.as_raw_socket() as i32;

        let handle = pool.open_on_thread(fd, thread_idx);
        Ok(Self {
            inner: ManuallyDrop::new(socket),
            handle,
        })
    }

    /// Get the peer address of the socket.
    pub fn peer_addr(&self) -> io::Result<SockAddr> {
        self.inner.peer_addr()
    }

    /// Get the local address of the socket.
    pub fn local_addr(&self) -> io::Result<SockAddr> {
        self.inner.local_addr()
    }

    /// Create a new socket.
    pub fn new(
        domain: Domain,
        ty: Type,
        protocol: Option<Protocol>,
        pool: &IoPool,
    ) -> io::Result<Self> {
        let socket = Socket2::new(domain, ty, protocol)?;
        Self::from_socket2(socket, pool)
    }

    /// Create and bind a socket.
    pub fn bind(
        addr: &SockAddr,
        ty: Type,
        protocol: Option<Protocol>,
        pool: &IoPool,
    ) -> io::Result<Self> {
        let socket = Self::new(addr.domain(), ty, protocol, pool)?;
        socket.inner.bind(addr)?;
        Ok(socket)
    }

    /// Set socket to listen mode.
    pub fn listen(&self, backlog: i32) -> io::Result<()> {
        self.inner.listen(backlog)
    }

    /// Connect to an address asynchronously.
    pub fn connect(&self, addr: &SockAddr) -> ConnectFuture {
        self.handle.connect(addr)
    }

    /// Accept a new connection asynchronously.
    ///
    /// Returns a future that resolves to the new socket fd and peer address.
    pub fn accept(&self) -> AcceptFuture {
        self.handle.accept()
    }

    /// Receive data asynchronously.
    pub fn recv<B: IoBufMut + SetLen + Send>(&self, buf: B) -> ReadFuture<B> {
        self.handle.recv(buf)
    }

    /// Send data asynchronously.
    pub fn send<B: IoBuf + Send>(&self, buf: B) -> WriteFuture<B> {
        self.handle.send(buf)
    }

    /// Read from socket asynchronously (alias for recv).
    pub fn read<B: IoBufMut + SetLen + Send>(&self, buf: B) -> ReadFuture<B> {
        self.handle.read(buf)
    }

    /// Write to socket asynchronously (alias for send).
    pub fn write<B: IoBuf + Send>(&self, buf: B) -> WriteFuture<B> {
        self.handle.write(buf)
    }

    /// Receive data with sender address (for UDP).
    pub fn recv_from<B: IoBufMut + SetLen + Send>(&self, buf: B) -> RecvFromFuture<B> {
        self.handle.recv_from(buf)
    }

    /// Send data to a specific address (for UDP).
    pub fn send_to<B: IoBuf + Send>(&self, buf: B, addr: &SockAddr) -> SendToFuture<B> {
        self.handle.send_to(buf, addr)
    }

    /// Shutdown the socket.
    pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        self.inner.shutdown(how)
    }

    /// Get the underlying IoHandle.
    pub fn handle(&self) -> &IoHandle {
        &self.handle
    }

    /// Get the file descriptor.
    #[cfg(unix)]
    pub fn fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }

    /// Get the socket handle.
    #[cfg(windows)]
    pub fn socket_handle(&self) -> RawSocket {
        self.inner.as_raw_socket()
    }

    // ========================================================================
    // Synchronous socket options (these don't need async)
    // ========================================================================

    /// Get a socket option.
    ///
    /// # Safety
    /// The caller must ensure `T` is the correct type for `level` and `name`.
    #[cfg(unix)]
    pub unsafe fn get_socket_option<T: Copy>(&self, level: i32, name: i32) -> io::Result<T> {
        let mut value: MaybeUninit<T> = MaybeUninit::uninit();
        let mut len = size_of::<T>() as libc::socklen_t;
        let result = unsafe {
            libc::getsockopt(
                self.inner.as_raw_fd(),
                level,
                name,
                value.as_mut_ptr() as *mut _,
                &mut len,
            )
        };
        if result == 0 {
            debug_assert_eq!(len as usize, size_of::<T>());
            Ok(unsafe { value.assume_init() })
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Get a socket option.
    ///
    /// # Safety
    /// The caller must ensure `T` is the correct type for `level` and `name`.
    #[cfg(windows)]
    pub unsafe fn get_socket_option<T: Copy>(&self, level: i32, name: i32) -> io::Result<T> {
        use windows_sys::Win32::Networking::WinSock::getsockopt;
        let mut value: MaybeUninit<T> = MaybeUninit::uninit();
        let mut len = size_of::<T>() as i32;
        let result = getsockopt(
            self.inner.as_raw_socket() as _,
            level,
            name,
            value.as_mut_ptr() as *mut _,
            &mut len,
        );
        if result == 0 {
            debug_assert_eq!(len as usize, size_of::<T>());
            Ok(value.assume_init())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Set a socket option.
    ///
    /// # Safety
    /// The caller must ensure `T` is the correct type for `level` and `name`.
    #[cfg(unix)]
    pub unsafe fn set_socket_option<T: Copy>(
        &self,
        level: i32,
        name: i32,
        value: &T,
    ) -> io::Result<()> {
        let result = unsafe {
            libc::setsockopt(
                self.inner.as_raw_fd(),
                level,
                name,
                value as *const _ as *const _,
                std::mem::size_of::<T>() as libc::socklen_t,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Set a socket option.
    ///
    /// # Safety
    /// The caller must ensure `T` is the correct type for `level` and `name`.
    #[cfg(windows)]
    pub unsafe fn set_socket_option<T: Copy>(
        &self,
        level: i32,
        name: i32,
        value: &T,
    ) -> io::Result<()> {
        use windows_sys::Win32::Networking::WinSock::setsockopt;
        let result = setsockopt(
            self.inner.as_raw_socket() as _,
            level,
            name,
            value as *const _ as *const _,
            std::mem::size_of::<T>() as i32,
        );
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(unix)]
impl AsRawFd for Socket {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[cfg(windows)]
impl AsRawSocket for Socket {
    fn as_raw_socket(&self) -> RawSocket {
        self.inner.as_raw_socket()
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        // Queue a Close operation to the IO thread. This ensures:
        // 1. The fd is deregistered from the poller before closing
        // 2. Any pending IO operations are properly canceled
        // 3. The TCP FIN is sent after proper cleanup
        //
        // The close_sync() method pushes a Close op to the contract queue,
        // and the PollWaker ensures the IO thread wakes up to process it.
        // This avoids race conditions where we close an fd that's still
        // registered with the poller.
        self.handle.close_sync();
    }
}

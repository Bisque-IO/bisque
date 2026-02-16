//! IO handle with thread affinity and async futures.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use socket2::SockAddr;

use crate::buf::{BufResult, IoBuf, IoBufMut, SetLen};
use crate::ptr::StripedArc;

use super::contract::{IoContract, IoContractGroupInner, Op, OpType};
use super::pool::IoPoolInner;

/// A handle to a file descriptor with IO thread affinity.
///
/// Once created via [`IoPool::open`], all IO operations on this handle
/// are routed to the same IO thread for better cache locality.
#[derive(Debug)]
pub struct IoHandle {
    fd: i32,
    thread_idx: usize,
    pool: StripedArc<IoPoolInner>,
}

impl IoHandle {
    pub(crate) fn new(fd: i32, thread_idx: usize, pool: StripedArc<IoPoolInner>) -> Self {
        Self {
            fd,
            thread_idx,
            pool,
        }
    }

    #[inline]
    pub fn fd(&self) -> i32 {
        self.fd
    }

    #[inline]
    pub fn thread_idx(&self) -> usize {
        self.thread_idx
    }

    #[inline]
    fn contract_group(&self) -> &IoContractGroupInner {
        self.pool.contract_group(self.thread_idx)
    }

    // ========================================================================
    // File Operations
    // ========================================================================

    /// Read from file at the specified offset into the provided buffer.
    ///
    /// Takes ownership of the buffer and returns it along with the result.
    /// On success, the buffer's initialized length is updated to reflect
    /// the bytes read.
    pub fn read_at<B: IoBufMut + Send>(&self, offset: u64, mut buf: B) -> ReadFuture<'_, B> {
        let buf_ptr = buf.buf_mut_ptr() as *mut u8;
        let buf_capacity = buf.buf_capacity() as u32;

        let op = Op::with_params(
            OpType::ReadAt as u8,
            0,
            self.fd,
            offset,
            buf_ptr,
            buf_capacity,
        );

        ReadFuture::new(self.contract_group(), op, buf)
    }

    /// Write to file at the specified offset from the provided buffer.
    ///
    /// Takes ownership of the buffer and returns it along with the result.
    pub fn write_at<B: IoBuf + Send>(&self, offset: u64, buf: B) -> WriteFuture<'_, B> {
        let buf_ptr = buf.buf_ptr() as *mut u8;
        let buf_len = buf.buf_len() as u32;

        let op = Op::with_params(OpType::WriteAt as u8, 0, self.fd, offset, buf_ptr, buf_len);

        WriteFuture::new(self.contract_group(), op, buf)
    }

    /// Sync file data to disk.
    pub fn sync(&self, datasync: bool) -> SyncFuture<'_> {
        let op_type = if datasync {
            OpType::Fdatasync
        } else {
            OpType::Fsync
        };

        let op = Op::with_params(op_type as u8, 0, self.fd, 0, std::ptr::null_mut(), 0);

        SyncFuture::new(self.contract_group(), op)
    }

    // ========================================================================
    // Socket Operations
    // ========================================================================

    /// Read from a socket/pipe into the provided buffer.
    ///
    /// Takes ownership of the buffer and returns it along with the result.
    pub fn read<B: IoBufMut + Send>(&self, mut buf: B) -> ReadFuture<'_, B> {
        let buf_ptr = buf.buf_mut_ptr() as *mut u8;
        let buf_capacity = buf.buf_capacity() as u32;

        let op = Op::with_params(OpType::Read as u8, 0, self.fd, 0, buf_ptr, buf_capacity);

        ReadFuture::new(self.contract_group(), op, buf)
    }

    /// Write to a socket/pipe from the provided buffer.
    ///
    /// Takes ownership of the buffer and returns it along with the result.
    pub fn write<B: IoBuf + Send>(&self, buf: B) -> WriteFuture<'_, B> {
        let buf_ptr = buf.buf_ptr() as *mut u8;
        let buf_len = buf.buf_len() as u32;

        let op = Op::with_params(OpType::Write as u8, 0, self.fd, 0, buf_ptr, buf_len);

        WriteFuture::new(self.contract_group(), op, buf)
    }

    /// Accept an incoming connection.
    /// Returns the new socket fd and peer address on success.
    pub fn accept(&self) -> AcceptFuture<'_> {
        // Address storage pointer will be set on first poll when future is pinned
        let op = Op::with_params(
            OpType::Accept as u8,
            0,
            self.fd,
            0, // Will be filled with addr_storage ptr on first poll
            std::ptr::null_mut(),
            0,
        );

        AcceptFuture::new(self.contract_group(), op)
    }

    /// Close the file descriptor.
    pub fn close(self) -> SyncFuture<'static> {
        // Extract what we need before consuming self
        let fd = self.fd;
        let group = self.contract_group() as *const IoContractGroupInner;

        let op = Op::with_params(OpType::Close as u8, 0, fd, 0, std::ptr::null_mut(), 0);

        // Prevent Drop from running (which would close fd) - we're closing it async
        std::mem::forget(self);

        // SAFETY: The IoPoolInner (and thus IoContractGroupInner) lives for 'static
        // because we're forgetting self and the pool uses Arc internally.
        // The future will complete before the pool is dropped.
        unsafe { SyncFuture::new(&*group, op) }
    }

    /// Close the file descriptor synchronously (for use in Drop).
    ///
    /// This submits a Close operation to the IO driver but does NOT wait for
    /// completion. The IO driver will process it and:
    /// 1. Unregister the fd from the poller
    /// 2. Cancel any pending operations for this fd
    /// 3. Close the fd
    ///
    /// This is safe because we use ManuallyDrop on Socket2 to prevent double-close.
    pub(crate) fn close_sync(&self) {
        let op = Op::with_params(OpType::Close as u8, 0, self.fd, 0, std::ptr::null_mut(), 0);

        // Use a no-op waker since we don't need to be notified of completion.
        // The Close operation is processed synchronously by the IO driver.
        let waker = std::task::Waker::noop();

        // Submit the close operation. We don't care if it fails (e.g., pool full)
        // because the worst case is a resource leak, which is better than a crash.
        let _ = self.contract_group().push(op, &waker);
    }

    /// Receive data from a socket into the provided buffer.
    ///
    /// Takes ownership of the buffer and returns it along with the result.
    pub fn recv<B: IoBufMut + Send>(&self, mut buf: B) -> ReadFuture<'_, B> {
        let buf_ptr = buf.buf_mut_ptr() as *mut u8;
        let buf_capacity = buf.buf_capacity() as u32;

        let op = Op::with_params(OpType::Recv as u8, 0, self.fd, 0, buf_ptr, buf_capacity);

        ReadFuture::new(self.contract_group(), op, buf)
    }

    /// Send data from a buffer to a socket.
    ///
    /// Takes ownership of the buffer and returns it along with the result.
    pub fn send<B: IoBuf + Send>(&self, buf: B) -> WriteFuture<'_, B> {
        let buf_ptr = buf.buf_ptr() as *mut u8;
        let buf_len = buf.buf_len() as u32;

        let op = Op::with_params(OpType::Send as u8, 0, self.fd, 0, buf_ptr, buf_len);

        WriteFuture::new(self.contract_group(), op, buf)
    }

    /// Connect to a remote address.
    ///
    /// Returns 0 on success.
    pub fn connect(&self, addr: &SockAddr) -> ConnectFuture<'_> {
        // Store address data for the IO thread to use
        let addr_ptr = addr.as_ptr() as *mut u8;
        let addr_len = addr.len() as u32;

        let op = Op::with_params(OpType::Connect as u8, 0, self.fd, 0, addr_ptr, addr_len);

        ConnectFuture::new(self.contract_group(), op, addr.clone())
    }

    /// Receive data from a socket with sender address (for UDP).
    ///
    /// Returns the number of bytes received and the sender's address.
    pub fn recv_from<B: IoBufMut + Send>(&self, mut buf: B) -> RecvFromFuture<'_, B> {
        let buf_ptr = buf.buf_mut_ptr() as *mut u8;
        let buf_capacity = buf.buf_capacity() as u32;

        // Use offset field to pass address storage pointer - will be set on first poll
        // when the future is pinned and addr_storage has a stable address
        let op = Op::with_params(
            OpType::RecvFrom as u8,
            0,
            self.fd,
            0, // Will be filled with addr_storage ptr on first poll
            buf_ptr,
            buf_capacity,
        );

        RecvFromFuture::new(self.contract_group(), op, buf)
    }

    /// Send data to a specific address (for UDP).
    ///
    /// Returns the number of bytes sent.
    pub fn send_to<B: IoBuf + Send>(&self, buf: B, addr: &SockAddr) -> SendToFuture<'_, B> {
        let buf_ptr = buf.buf_ptr() as *mut u8;
        let buf_len = buf.buf_len() as u32;

        // Store address pointer in offset field
        let addr_ptr = addr.as_ptr() as u64;
        let addr_len = addr.len() as u64;

        // Pack addr_len into flags (it's small, typically 16 or 28 bytes)
        let op = Op::with_params(
            OpType::SendTo as u8,
            addr_len as u8,
            self.fd,
            addr_ptr,
            buf_ptr,
            buf_len,
        );

        SendToFuture::new(self.contract_group(), op, buf, addr.clone())
    }

    /// Shutdown the socket.
    ///
    /// The `how` parameter specifies what to shut down:
    /// - 0: Shutdown read side (SHUT_RD)
    /// - 1: Shutdown write side (SHUT_WR)
    /// - 2: Shutdown both sides (SHUT_RDWR)
    pub fn shutdown(&self, how: i32) -> SyncFuture<'_> {
        let op = Op::with_params(
            OpType::Shutdown as u8,
            0,
            self.fd,
            how as u64,
            std::ptr::null_mut(),
            0,
        );

        SyncFuture::new(self.contract_group(), op)
    }
}

// ============================================================================
// AddrStorage - for recv_from operations
// ============================================================================

/// Storage for socket address returned by recv_from.
#[repr(C)]
pub struct AddrStorage {
    pub storage: socket2::SockAddrStorage,
    pub len: socket2::socklen_t,
}

impl AddrStorage {
    fn new() -> Self {
        Self {
            storage: unsafe { std::mem::zeroed() },
            len: std::mem::size_of::<socket2::SockAddrStorage>() as socket2::socklen_t,
        }
    }

    /// Convert to a SocketAddr if valid.
    pub fn to_socket_addr(&self) -> Option<SocketAddr> {
        // SAFETY: We're reading from initialized storage
        unsafe {
            let storage = std::ptr::read(&self.storage);
            let addr = SockAddr::new(storage, self.len);
            addr.as_socket()
        }
    }
}

// ============================================================================
// FutureState
// ============================================================================

/// State of an IO future.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FutureState {
    /// Not yet submitted - waiting for first poll
    NotSubmitted,
    /// Submitted and waiting for completion
    Pending,
    /// Operation completed
    Complete,
}

// ============================================================================
// ReadFuture - for read operations
// ============================================================================

/// A future for read operations that own a buffer.
///
/// Returns `BufResult<usize, B>` which contains both the result and the buffer.
/// On success, the buffer's initialized length is updated via `SetLen`.
///
/// The future borrows from `IoHandle` and must not outlive it.
pub struct ReadFuture<'a, B: Send + 'static> {
    /// Contract group reference (borrowed from IoHandle).
    group: &'a IoContractGroupInner,
    /// Operation to submit on first poll
    op: Op,
    /// Contract reference (only valid after submission)
    contract: Option<&'a IoContract>,
    buf: Option<B>,
    state: FutureState,
}

unsafe impl<B: Send + 'static> Send for ReadFuture<'_, B> {}
impl<B: Send + 'static> Unpin for ReadFuture<'_, B> {}

impl<'a, B: Send + 'static> ReadFuture<'a, B> {
    fn new(group: &'a IoContractGroupInner, op: Op, buf: B) -> Self {
        Self {
            group,
            op,
            contract: None,
            buf: Some(buf),
            state: FutureState::NotSubmitted,
        }
    }
}

impl<B: IoBufMut + SetLen + Send + 'static> Future for ReadFuture<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match this.state {
            FutureState::NotSubmitted => {
                // First poll - submit the operation
                match this.group.push(this.op, cx.waker()) {
                    Ok(id) => {
                        // Get reference to contract - it's valid for the lifetime of the group
                        this.contract = this.group.get(id);
                        this.state = FutureState::Pending;
                        Poll::Pending
                    }
                    Err(()) => {
                        let buf = this.buf.take().expect("buffer already taken");
                        Poll::Ready(BufResult(Err(io::Error::other("contract group full")), buf))
                    }
                }
            }
            FutureState::Pending => {
                let contract = this
                    .contract
                    .expect("contract should be set in Pending state");

                if let Some(result) = this.group.try_to_complete(contract) {
                    this.state = FutureState::Complete;
                    let mut buf = this.buf.take().expect("buffer already taken");

                    // Update buffer's initialized length on success
                    if let Ok(n) = &result {
                        unsafe {
                            buf.set_len(*n);
                        }
                    }

                    Poll::Ready(BufResult(result, buf))
                } else {
                    Poll::Pending
                }
            }
            FutureState::Complete => {
                panic!("ReadFuture polled after completion");
            }
        }
    }
}

impl<B: Send + 'static> Drop for ReadFuture<'_, B> {
    fn drop(&mut self) {
        if self.state == FutureState::Pending {
            if let Some(contract) = self.contract {
                // IO is in flight - cancel with buf_drop
                if let Some(buf) = self.buf.take() {
                    let buf_drop: Box<dyn FnOnce() + Send> = Box::new(move || drop(buf));
                    self.group.cancel(contract.id(), Some(buf_drop));
                } else {
                    self.group.cancel(contract.id(), None);
                }
            }
        }
    }
}

// ============================================================================
// WriteFuture - for write operations
// ============================================================================

/// A future for write operations that own a buffer.
///
/// Returns `BufResult<usize, B>` which contains both the result and the buffer.
pub struct WriteFuture<'a, B: Send + 'static> {
    /// Contract group reference (borrowed from IoHandle).
    group: &'a IoContractGroupInner,
    /// Operation to submit on first poll
    op: Op,
    /// Contract reference (only valid after submission)
    contract: Option<&'a IoContract>,
    buf: Option<B>,
    state: FutureState,
}

unsafe impl<B: Send + 'static> Send for WriteFuture<'_, B> {}
impl<B: Send + 'static> Unpin for WriteFuture<'_, B> {}

impl<'a, B: Send + 'static> WriteFuture<'a, B> {
    fn new(group: &'a IoContractGroupInner, op: Op, buf: B) -> Self {
        Self {
            group,
            op,
            contract: None,
            buf: Some(buf),
            state: FutureState::NotSubmitted,
        }
    }
}

impl<B: IoBuf + Send + 'static> Future for WriteFuture<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match this.state {
            FutureState::NotSubmitted => {
                // First poll - submit the operation
                match this.group.push(this.op, cx.waker()) {
                    Ok(id) => {
                        this.contract = this.group.get(id);
                        this.state = FutureState::Pending;
                        Poll::Pending
                    }
                    Err(()) => {
                        let buf = this.buf.take().expect("buffer already taken");
                        Poll::Ready(BufResult(Err(io::Error::other("contract group full")), buf))
                    }
                }
            }
            FutureState::Pending => {
                let contract = this
                    .contract
                    .expect("contract should be set in Pending state");

                if let Some(result) = this.group.try_to_complete(contract) {
                    this.state = FutureState::Complete;
                    let buf = this.buf.take().expect("buffer already taken");
                    Poll::Ready(BufResult(result, buf))
                } else {
                    Poll::Pending
                }
            }
            FutureState::Complete => {
                panic!("WriteFuture polled after completion");
            }
        }
    }
}

impl<B: Send + 'static> Drop for WriteFuture<'_, B> {
    fn drop(&mut self) {
        if self.state == FutureState::Pending {
            if let Some(contract) = self.contract {
                if let Some(buf) = self.buf.take() {
                    let buf_drop: Box<dyn FnOnce() + Send> = Box::new(move || drop(buf));
                    self.group.cancel(contract.id(), Some(buf_drop));
                } else {
                    self.group.cancel(contract.id(), None);
                }
            }
        }
    }
}

// ============================================================================
// SyncFuture - for operations without buffers (sync, accept, close)
// ============================================================================

/// A future for IO operations that don't involve a buffer.
pub struct SyncFuture<'a> {
    /// Contract group reference (borrowed from IoHandle).
    group: &'a IoContractGroupInner,
    /// Operation to submit on first poll
    op: Op,
    /// Contract reference (only valid after submission)
    contract: Option<&'a IoContract>,
    state: FutureState,
}

unsafe impl Send for SyncFuture<'_> {}

impl<'a> SyncFuture<'a> {
    fn new(group: &'a IoContractGroupInner, op: Op) -> Self {
        Self {
            group,
            op,
            contract: None,
            state: FutureState::NotSubmitted,
        }
    }
}

impl Future for SyncFuture<'_> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.state {
            FutureState::NotSubmitted => match self.group.push(self.op, cx.waker()) {
                Ok(id) => {
                    self.contract = self.group.get(id);
                    self.state = FutureState::Pending;
                    Poll::Pending
                }
                Err(()) => Poll::Ready(Err(io::Error::other("contract group full"))),
            },
            FutureState::Pending => {
                let contract = self
                    .contract
                    .expect("contract should be set in Pending state");

                if let Some(result) = self.group.try_to_complete(contract) {
                    self.state = FutureState::Complete;
                    Poll::Ready(result)
                } else {
                    Poll::Pending
                }
            }
            FutureState::Complete => {
                panic!("SyncFuture polled after completion");
            }
        }
    }
}

impl Drop for SyncFuture<'_> {
    fn drop(&mut self) {
        if self.state == FutureState::Pending {
            if let Some(contract) = self.contract {
                self.group.cancel(contract.id(), None);
            }
        }
    }
}

// ============================================================================
// ConnectFuture - for connect operations (keeps address alive)
// ============================================================================

/// A future for connect operations.
///
/// Holds a reference to the address to ensure it stays valid during the async operation.
pub struct ConnectFuture<'a> {
    /// Contract group reference (borrowed from IoHandle).
    group: &'a IoContractGroupInner,
    /// Operation to submit on first poll
    op: Op,
    /// Contract reference (only valid after submission)
    contract: Option<&'a IoContract>,
    state: FutureState,
    _addr: SockAddr, // Keep address alive during operation
}

unsafe impl Send for ConnectFuture<'_> {}

impl<'a> ConnectFuture<'a> {
    fn new(group: &'a IoContractGroupInner, op: Op, addr: SockAddr) -> Self {
        Self {
            group,
            op,
            contract: None,
            state: FutureState::NotSubmitted,
            _addr: addr,
        }
    }
}

impl Future for ConnectFuture<'_> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.state {
            FutureState::NotSubmitted => match self.group.push(self.op, cx.waker()) {
                Ok(id) => {
                    self.contract = self.group.get(id);
                    self.state = FutureState::Pending;
                    Poll::Pending
                }
                Err(()) => Poll::Ready(Err(io::Error::other("contract group full"))),
            },
            FutureState::Pending => {
                let contract = self
                    .contract
                    .expect("contract should be set in Pending state");

                if let Some(result) = self.group.try_to_complete(contract) {
                    self.state = FutureState::Complete;
                    Poll::Ready(result.map(|_| ()))
                } else {
                    Poll::Pending
                }
            }
            FutureState::Complete => {
                panic!("ConnectFuture polled after completion");
            }
        }
    }
}

impl Drop for ConnectFuture<'_> {
    fn drop(&mut self) {
        if self.state == FutureState::Pending {
            if let Some(contract) = self.contract {
                self.group.cancel(contract.id(), None);
            }
        }
    }
}

// ============================================================================
// RecvFromFuture - for recvfrom operations (returns address)
// ============================================================================

/// A future for recvfrom operations.
///
/// Returns the buffer and the sender's address on success.
pub struct RecvFromFuture<'a, B: Send + 'static> {
    /// Contract group reference (borrowed from IoHandle).
    group: &'a IoContractGroupInner,
    /// Base operation (offset will be filled with addr_storage ptr on first poll)
    op: Op,
    /// Contract reference (only valid after submission)
    contract: Option<&'a IoContract>,
    buf: Option<B>,
    /// Inline storage for sender address - no heap allocation
    addr_storage: AddrStorage,
    state: FutureState,
}

unsafe impl<B: Send + 'static> Send for RecvFromFuture<'_, B> {}
// Note: NOT Unpin because addr_storage address is passed to the kernel

impl<'a, B: Send + 'static> RecvFromFuture<'a, B> {
    fn new(group: &'a IoContractGroupInner, op: Op, buf: B) -> Self {
        Self {
            group,
            op,
            contract: None,
            buf: Some(buf),
            addr_storage: AddrStorage::new(),
            state: FutureState::NotSubmitted,
        }
    }
}

/// Result type for recv_from operations.
pub struct RecvFromResult<B> {
    /// The result (bytes received or error).
    pub result: io::Result<usize>,
    /// The buffer.
    pub buf: B,
    /// The sender's address (if successful).
    pub addr: Option<SocketAddr>,
}

impl<B: IoBufMut + SetLen + Send + 'static> Future for RecvFromFuture<'_, B> {
    type Output = RecvFromResult<B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: We need to get mutable access but we're careful not to move addr_storage
        let this = unsafe { self.get_unchecked_mut() };

        match this.state {
            FutureState::NotSubmitted => {
                // Set the address storage pointer now that we're pinned
                this.op.offset = &this.addr_storage as *const AddrStorage as u64;

                match this.group.push(this.op, cx.waker()) {
                    Ok(id) => {
                        this.contract = this.group.get(id);
                        this.state = FutureState::Pending;
                        Poll::Pending
                    }
                    Err(()) => {
                        let buf = this.buf.take().expect("buffer already taken");
                        Poll::Ready(RecvFromResult {
                            result: Err(io::Error::other("contract group full")),
                            buf,
                            addr: None,
                        })
                    }
                }
            }
            FutureState::Pending => {
                let contract = this
                    .contract
                    .expect("contract should be set in Pending state");

                if let Some(result) = this.group.try_to_complete(contract) {
                    this.state = FutureState::Complete;
                    let mut buf = this.buf.take().expect("buffer already taken");

                    let addr = this.addr_storage.to_socket_addr();

                    if let Ok(n) = &result {
                        unsafe {
                            buf.set_len(*n);
                        }
                    }

                    Poll::Ready(RecvFromResult { result, buf, addr })
                } else {
                    Poll::Pending
                }
            }
            FutureState::Complete => {
                panic!("RecvFromFuture polled after completion");
            }
        }
    }
}

impl<B: Send + 'static> Drop for RecvFromFuture<'_, B> {
    fn drop(&mut self) {
        if self.state == FutureState::Pending {
            if let Some(contract) = self.contract {
                if let Some(buf) = self.buf.take() {
                    let buf_drop: Box<dyn FnOnce() + Send> = Box::new(move || drop(buf));
                    self.group.cancel(contract.id(), Some(buf_drop));
                } else {
                    self.group.cancel(contract.id(), None);
                }
            }
        }
    }
}

// ============================================================================
// SendToFuture - for sendto operations (keeps address alive)
// ============================================================================

/// A future for sendto operations.
///
/// Holds a reference to the address to ensure it stays valid during the async operation.
pub struct SendToFuture<'a, B: Send + 'static> {
    /// Contract group reference (borrowed from IoHandle).
    group: &'a IoContractGroupInner,
    /// Operation to submit on first poll
    op: Op,
    /// Contract reference (only valid after submission)
    contract: Option<&'a IoContract>,
    buf: Option<B>,
    state: FutureState,
    _addr: SockAddr, // Keep address alive during operation
}

unsafe impl<B: Send + 'static> Send for SendToFuture<'_, B> {}
impl<B: Send + 'static> Unpin for SendToFuture<'_, B> {}

impl<'a, B: Send + 'static> SendToFuture<'a, B> {
    fn new(group: &'a IoContractGroupInner, op: Op, buf: B, addr: SockAddr) -> Self {
        Self {
            group,
            op,
            contract: None,
            buf: Some(buf),
            state: FutureState::NotSubmitted,
            _addr: addr,
        }
    }
}

impl<B: IoBuf + Send + 'static> Future for SendToFuture<'_, B> {
    type Output = BufResult<usize, B>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match this.state {
            FutureState::NotSubmitted => match this.group.push(this.op, cx.waker()) {
                Ok(id) => {
                    this.contract = this.group.get(id);
                    this.state = FutureState::Pending;
                    Poll::Pending
                }
                Err(()) => {
                    let buf = this.buf.take().expect("buffer already taken");
                    Poll::Ready(BufResult(Err(io::Error::other("contract group full")), buf))
                }
            },
            FutureState::Pending => {
                let contract = this
                    .contract
                    .expect("contract should be set in Pending state");

                if let Some(result) = this.group.try_to_complete(contract) {
                    this.state = FutureState::Complete;
                    let buf = this.buf.take().expect("buffer already taken");

                    Poll::Ready(BufResult(result, buf))
                } else {
                    Poll::Pending
                }
            }
            FutureState::Complete => {
                panic!("SendToFuture polled after completion");
            }
        }
    }
}

impl<B: Send + 'static> Drop for SendToFuture<'_, B> {
    fn drop(&mut self) {
        if self.state == FutureState::Pending {
            if let Some(contract) = self.contract {
                if let Some(buf) = self.buf.take() {
                    let buf_drop: Box<dyn FnOnce() + Send> = Box::new(move || drop(buf));
                    self.group.cancel(contract.id(), Some(buf_drop));
                } else {
                    self.group.cancel(contract.id(), None);
                }
            }
        }
    }
}

// ============================================================================
// AcceptFuture - for accept operations (returns fd and address)
// ============================================================================

/// A future for accept operations.
///
/// Returns the new socket fd and the peer's address on success.
pub struct AcceptFuture<'a> {
    /// Contract group reference (borrowed from IoHandle).
    group: &'a IoContractGroupInner,
    /// Base operation (offset will be filled with addr_storage ptr on first poll)
    op: Op,
    /// Contract reference (only valid after submission)
    contract: Option<&'a IoContract>,
    /// Inline storage for peer address - no heap allocation
    addr_storage: AddrStorage,
    state: FutureState,
}

unsafe impl Send for AcceptFuture<'_> {}
// Note: NOT Unpin because addr_storage address is passed to the kernel

impl<'a> AcceptFuture<'a> {
    fn new(group: &'a IoContractGroupInner, op: Op) -> Self {
        Self {
            group,
            op,
            contract: None,
            addr_storage: AddrStorage::new(),
            state: FutureState::NotSubmitted,
        }
    }
}

/// Result type for accept operations.
pub struct AcceptResult {
    /// The new socket fd (or error).
    pub result: io::Result<i32>,
    /// The peer's address (if successful).
    pub addr: Option<SocketAddr>,
}

impl Future for AcceptFuture<'_> {
    type Output = AcceptResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: We need to get mutable access but we're careful not to move addr_storage
        let this = unsafe { self.get_unchecked_mut() };

        match this.state {
            FutureState::NotSubmitted => {
                // Set the address storage pointer now that we're pinned
                this.op.offset = &this.addr_storage as *const AddrStorage as u64;

                match this.group.push(this.op, cx.waker()) {
                    Ok(id) => {
                        this.contract = this.group.get(id);
                        this.state = FutureState::Pending;
                        Poll::Pending
                    }
                    Err(()) => Poll::Ready(AcceptResult {
                        result: Err(io::Error::other("contract group full")),
                        addr: None,
                    }),
                }
            }
            FutureState::Pending => {
                let contract = this
                    .contract
                    .expect("contract should be set in Pending state");

                if let Some(result) = this.group.try_to_complete(contract) {
                    this.state = FutureState::Complete;

                    let addr = this.addr_storage.to_socket_addr();

                    Poll::Ready(AcceptResult {
                        result: result.map(|n| n as i32),
                        addr,
                    })
                } else {
                    Poll::Pending
                }
            }
            FutureState::Complete => {
                panic!("AcceptFuture polled after completion");
            }
        }
    }
}

impl Drop for AcceptFuture<'_> {
    fn drop(&mut self) {
        if self.state == FutureState::Pending {
            if let Some(contract) = self.contract {
                self.group.cancel(contract.id(), None);
            }
        }
    }
}

//! Polling-based IO driver using the `polling` crate with level-triggered mode.
//!
//! This driver uses readiness-based polling (epoll/kqueue/IOCP) for socket
//! operations and a thread pool for blocking file operations.
//!
//! We use level-triggered mode to avoid the edge-triggered races that can cause
//! missed events when socket close/FIN arrives between checking and re-registering.

use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use polling::{Event, Events, PollMode, Poller};

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, BorrowedFd};

use super::asyncify::AsyncifyPool;
use super::contract::{ContractId, IoContract, IoContractGroupInner, Op, OpType, PollWaker};
use crate::ptr::StripedArc;

// ============================================================================
// Helper structs for complex operations (must match uring.rs definitions)
// ============================================================================

/// Paths for rename operation.
#[repr(C)]
pub struct RenamePaths {
    pub old_path: *const i8,
    pub new_path: *const i8,
}

/// Paths for symlink operation.
#[repr(C)]
pub struct SymlinkPaths {
    pub target: *const i8,
    pub linkpath: *const i8,
}

/// Paths for link operation.
#[repr(C)]
pub struct LinkPaths {
    pub old_path: *const i8,
    pub new_path: *const i8,
}

/// Data for splice operation.
#[repr(C)]
pub struct SpliceData {
    pub off_in: i64,
    pub fd_out: i32,
    pub off_out: i64,
}

const DEFAULT_THREAD_LIMIT: usize = 256;
const DEFAULT_RECV_TIMEOUT: Duration = Duration::from_secs(60);

/// Key 0 is reserved for the notifier/waker
const NOTIFY_KEY: usize = 0;

/// Shared state for PollingWaker.
struct WakerState {
    poller: Arc<Poller>,
    inflight: std::sync::atomic::AtomicBool,
}

/// Wrapper around polling's notify that implements PollWaker.
pub struct PollingWaker(Arc<WakerState>);

impl PollingWaker {
    fn new(state: Arc<WakerState>) -> Self {
        Self(state)
    }
}

impl PollWaker for PollingWaker {
    fn wake(&self) {
        // Only one wake can be inflight at a time
        if self
            .0
            .inflight
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let _ = self.0.poller.notify();
    }
}

/// Polling-based IO driver using level-triggered mode.
pub struct PollDriver {
    poller: Arc<Poller>,
    waker_state: Arc<WakerState>,
    events: Events,
    contract_group: StripedArc<IoContractGroupInner>,
    asyncify: AsyncifyPool,
}

impl PollDriver {
    pub fn new(
        capacity: usize,
        contract_group: StripedArc<IoContractGroupInner>,
    ) -> io::Result<Self> {
        Self::with_asyncify_config(
            contract_group,
            DEFAULT_THREAD_LIMIT,
            DEFAULT_RECV_TIMEOUT,
            capacity,
        )
    }

    pub fn with_asyncify_config(
        contract_group: StripedArc<IoContractGroupInner>,
        thread_limit: usize,
        recv_timeout: Duration,
        capacity: usize,
    ) -> io::Result<Self> {
        let poller = Arc::new(Poller::new()?);
        let waker_state = Arc::new(WakerState {
            poller: Arc::clone(&poller),
            inflight: std::sync::atomic::AtomicBool::new(false),
        });

        // Register our waker with the contract group so it can wake us when work is pushed
        let poll_waker = Arc::new(PollingWaker::new(Arc::clone(&waker_state)));
        contract_group.set_poll_waker(poll_waker);

        Ok(Self {
            poller,
            waker_state,
            events: Events::with_capacity(NonZeroUsize::new(capacity.max(64)).unwrap()),
            contract_group,
            asyncify: AsyncifyPool::new(thread_limit, recv_timeout),
        })
    }

    /// Submit an IO operation from a contract.
    pub fn submit(&mut self, contract: &IoContract) -> io::Result<()> {
        let op = unsafe { contract.op() };
        let Some(op_type) = OpType::from_u8(op.op_type) else {
            return Err(io::Error::other("invalid op type"));
        };

        match op_type {
            // === Immediate operations (no async needed) ===
            OpType::Nop => {
                self.complete(contract.id(), Ok(0));
            }

            OpType::Close => {
                self.close_fd(op.fd);
                self.complete(contract.id(), Ok(0));
            }

            OpType::Shutdown => {
                let result = sys::shutdown(op.fd, op.offset as i32);
                self.complete(contract.id(), result);
            }

            // These are no-ops in poll driver (socket already created)
            OpType::Socket | OpType::Bind | OpType::Listen => {
                self.complete(contract.id(), Ok(0));
            }

            // === File operations (use thread pool) ===
            OpType::Fsync
            | OpType::Fdatasync
            | OpType::ReadAt
            | OpType::WriteAt
            | OpType::Open
            | OpType::Truncate
            | OpType::Stat
            | OpType::CreateDir
            | OpType::Unlink
            | OpType::Rmdir
            | OpType::Rename
            | OpType::Symlink
            | OpType::Link
            | OpType::Readlink
            | OpType::Splice
            | OpType::Pipe
            | OpType::ReadVectoredAt
            | OpType::WriteVectoredAt => {
                self.dispatch_blocking(contract.id(), op);
            }

            // === Socket read operations (poll for readable) ===
            OpType::Read
            | OpType::Recv
            | OpType::RecvFrom
            | OpType::Accept
            | OpType::RecvVectored
            | OpType::RecvMsg
            | OpType::ReadVectored
            | OpType::Poll => {
                self.submit_socket_read(contract);
            }

            // === Socket write operations (poll for writable) ===
            OpType::Write
            | OpType::Send
            | OpType::SendTo
            | OpType::SendVectored
            | OpType::SendMsg
            | OpType::WriteVectored => {
                self.submit_socket_write(contract);
            }

            OpType::Connect => {
                // Start non-blocking connect, then poll for writable
                let addr = op.buf_ptr as *const libc::sockaddr;
                let addrlen = op.buf_len as u32;

                match sys::connect_start(op.fd, addr, addrlen) {
                    Ok(()) => {
                        // Connect in progress, wait for writable
                        self.submit_socket_write(contract);
                    }
                    Err(e) => {
                        self.complete(contract.id(), Err(e));
                    }
                }
            }

            OpType::Cancel | OpType::GetSockOpt | OpType::SetSockOpt | OpType::ConnectNamedPipe => {
                self.complete(
                    contract.id(),
                    Err(io::Error::other(format!("{:?} not supported", op_type))),
                );
            }
        }

        Ok(())
    }

    /// Dispatch a blocking operation to the thread pool.
    fn dispatch_blocking(&mut self, id: ContractId, op: &Op) {
        let contract_group = StripedArc::clone(&self.contract_group);

        // Copy all fields we need - Op contains raw pointers so we need to be careful
        let op_type = op.op_type;
        let fd = op.fd;
        let offset = op.offset;
        let buf_ptr = op.buf_ptr as usize;
        let buf_len = op.buf_len as usize;

        let result = self.asyncify.dispatch(move || {
            let result = match OpType::from_u8(op_type) {
                Some(OpType::Fsync) => sys::fsync(fd, false),
                Some(OpType::Fdatasync) => sys::fsync(fd, true),
                Some(OpType::ReadAt) => sys::pread(fd, buf_ptr as *mut u8, buf_len, offset),
                Some(OpType::WriteAt) => sys::pwrite(fd, buf_ptr as *const u8, buf_len, offset),
                Some(OpType::Open) => {
                    // buf_ptr points to the path, offset contains flags, buf_len contains mode
                    sys::open(buf_ptr as *const i8, offset as i32, buf_len as u32)
                }
                Some(OpType::Truncate) => sys::ftruncate(fd, offset),
                Some(OpType::Stat) => {
                    // buf_ptr points to statx struct, offset contains flags, buf_len contains mask
                    sys::fstat(fd, buf_ptr as *mut libc::stat)
                }
                Some(OpType::CreateDir) => sys::mkdir(buf_ptr as *const i8, buf_len as u32),
                Some(OpType::Unlink) => sys::unlink(buf_ptr as *const i8),
                Some(OpType::Rmdir) => sys::rmdir(buf_ptr as *const i8),
                Some(OpType::Rename) => {
                    let paths = buf_ptr as *const RenamePaths;
                    unsafe { sys::rename((*paths).old_path, (*paths).new_path) }
                }
                Some(OpType::Symlink) => {
                    let paths = buf_ptr as *const SymlinkPaths;
                    unsafe { sys::symlink((*paths).target, (*paths).linkpath) }
                }
                Some(OpType::Link) => {
                    let paths = buf_ptr as *const LinkPaths;
                    unsafe { sys::link((*paths).old_path, (*paths).new_path) }
                }
                Some(OpType::Readlink) => {
                    // buf_ptr points to path, offset points to output buffer, buf_len is output buffer size
                    sys::readlink(buf_ptr as *const i8, offset as *mut i8, buf_len)
                }
                Some(OpType::Splice) => {
                    let splice_data = buf_ptr as *const SpliceData;
                    unsafe {
                        sys::splice(
                            fd,
                            (*splice_data).off_in,
                            (*splice_data).fd_out,
                            (*splice_data).off_out,
                            buf_len,
                        )
                    }
                }
                Some(OpType::Pipe) => {
                    // buf_ptr points to [i32; 2] for the pipe fds
                    sys::pipe(buf_ptr as *mut i32)
                }
                Some(OpType::ReadVectoredAt) => {
                    let iovecs = buf_ptr as *const libc::iovec;
                    sys::preadv(fd, iovecs, buf_len as i32, offset)
                }
                Some(OpType::WriteVectoredAt) => {
                    let iovecs = buf_ptr as *const libc::iovec;
                    sys::pwritev(fd, iovecs, buf_len as i32, offset)
                }
                Some(OpType::ReadVectored) => {
                    let iovecs = buf_ptr as *const libc::iovec;
                    sys::readv(fd, iovecs, buf_len as i32)
                }
                Some(OpType::WriteVectored) => {
                    let iovecs = buf_ptr as *const libc::iovec;
                    sys::writev(fd, iovecs, buf_len as i32)
                }
                _ => Err(io::Error::other("not implemented")),
            };
            contract_group.complete(id, result);
        });

        if result.is_err() {
            self.complete(id, Err(io::Error::other("thread pool busy")));
        }
    }

    /// Submit a socket operation that needs readable.
    fn submit_socket_read(&mut self, contract: &IoContract) {
        let id = contract.id();
        let op = unsafe { contract.op() };

        // Try immediate completion
        match Self::try_read_op(op) {
            Ok(n) => self.complete(id, Ok(n)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Register fd with ContractId as key
                if let Err(e) = self.register_for_poll(op.fd, id) {
                    self.complete(id, Err(e));
                }
            }
            Err(e) => self.complete(id, Err(e)),
        }
    }

    /// Submit a socket operation that needs writable.
    fn submit_socket_write(&mut self, contract: &IoContract) {
        let id = contract.id();
        let op = unsafe { contract.op() };

        // Try immediate completion
        match Self::try_write_op(op) {
            Ok(n) => self.complete(id, Ok(n)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Register fd with ContractId as key
                if let Err(e) = self.register_for_poll(op.fd, id) {
                    self.complete(id, Err(e));
                }
            }
            Err(e) => self.complete(id, Err(e)),
        }
    }

    /// Register fd with poller using ContractId as the key.
    fn register_for_poll(&self, fd: i32, id: ContractId) -> io::Result<()> {
        // Use ContractId as key (offset by 1 to avoid key 0 which is reserved for notify)
        let key = (id.0 as usize) + 1;

        // SAFETY: fd is valid and we're adding it to the poller
        unsafe {
            let borrowed = BorrowedFd::borrow_raw(fd);
            // Use Level mode to avoid edge-triggered races
            self.poller
                .add_with_mode(borrowed.as_raw_fd(), Event::all(key), PollMode::Level)?;
        }

        Ok(())
    }

    /// Close an fd.
    fn close_fd(&mut self, fd: i32) {
        // Just close the fd - any pending operations will get errors on next poll
        let _ = sys::close(fd);
    }

    /// Poll for events and process completions.
    pub fn poll(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.events.clear();
        self.poller.wait(&mut self.events, timeout)?;

        // Clear inflight so new wakes can come through
        self.waker_state
            .inflight
            .store(false, std::sync::atomic::Ordering::Release);

        // Process events - key is ContractId + 1
        for ev in self.events.iter() {
            if ev.key == NOTIFY_KEY {
                continue;
            }

            let id = ContractId((ev.key - 1) as u32);
            let Some(contract) = self.contract_group.get(id) else {
                continue;
            };

            // SAFETY: Contract is in EXECUTING state
            let op = unsafe { contract.op() };
            let op_type = OpType::from_u8(op.op_type);

            let result = match op_type {
                Some(t) if Self::is_read_op(t) => Self::try_read_op(op),
                Some(_) => Self::try_write_op(op),
                None => Err(io::Error::other("invalid op type")),
            };

            match result {
                Ok(n) => {
                    // Unregister fd from poller
                    unsafe {
                        let borrowed = BorrowedFd::borrow_raw(op.fd);
                        let _ = self.poller.delete(&borrowed);
                    }
                    self.complete(id, Ok(n));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // Still not ready, will get another event with level-triggered
                }
                Err(e) => {
                    // Unregister fd from poller
                    unsafe {
                        let borrowed = BorrowedFd::borrow_raw(op.fd);
                        let _ = self.poller.delete(&borrowed);
                    }
                    self.complete(id, Err(e));
                }
            }
        }

        Ok(())
    }

    fn is_read_op(op_type: OpType) -> bool {
        matches!(
            op_type,
            OpType::Accept
                | OpType::Read
                | OpType::Recv
                | OpType::RecvFrom
                | OpType::RecvVectored
                | OpType::RecvMsg
                | OpType::ReadVectored
                | OpType::Poll
        )
    }

    /// Try a read-type operation.
    fn try_read_op(op: &Op) -> io::Result<usize> {
        let op_type = OpType::from_u8(op.op_type);
        match op_type {
            Some(OpType::Accept) => sys::accept(op.fd, op.offset),
            Some(OpType::Read | OpType::Recv) => sys::recv(op.fd, op.buf_ptr, op.buf_len as usize),
            Some(OpType::RecvFrom) => {
                sys::recvfrom(op.fd, op.buf_ptr, op.buf_len as usize, op.offset)
            }
            Some(OpType::RecvVectored | OpType::RecvMsg) => {
                sys::recvmsg(op.fd, op.buf_ptr as *mut libc::msghdr)
            }
            Some(OpType::ReadVectored) => {
                sys::readv(op.fd, op.buf_ptr as *const libc::iovec, op.buf_len as i32)
            }
            Some(OpType::Poll) => Ok(0),
            _ => Err(io::Error::other("invalid read op")),
        }
    }

    /// Try a write-type operation.
    fn try_write_op(op: &Op) -> io::Result<usize> {
        let op_type = OpType::from_u8(op.op_type);
        match op_type {
            Some(OpType::Connect) => sys::connect_finish(op.fd),
            Some(OpType::Write | OpType::Send) => {
                sys::send(op.fd, op.buf_ptr as *const u8, op.buf_len as usize)
            }
            Some(OpType::SendTo) => {
                // addr_ptr in offset, addr_len in flags
                let addr_ptr = (op.offset & 0x00FFFFFFFFFFFFFF) as *const libc::sockaddr;
                let addr_len = (op.offset >> 56) as u32;
                sys::sendto(
                    op.fd,
                    op.buf_ptr as *const u8,
                    op.buf_len as usize,
                    addr_ptr,
                    addr_len,
                )
            }
            Some(OpType::SendVectored | OpType::SendMsg) => {
                sys::sendmsg(op.fd, op.buf_ptr as *const libc::msghdr)
            }
            Some(OpType::WriteVectored) => {
                sys::writev(op.fd, op.buf_ptr as *const libc::iovec, op.buf_len as i32)
            }
            _ => Err(io::Error::other("invalid write op")),
        }
    }

    /// Complete a contract with a result.
    #[inline]
    fn complete(&self, id: ContractId, result: io::Result<usize>) {
        self.contract_group.complete(id, result);
    }

    /// Get a waker that can wake this driver's poll loop.
    pub fn waker(&self) -> PollingWaker {
        PollingWaker::new(Arc::clone(&self.waker_state))
    }
}

// =============================================================================
// Platform-specific syscall wrappers
// =============================================================================

#[cfg(unix)]
mod sys {
    use std::io;

    pub fn accept(fd: i32, addr_ptr: u64) -> io::Result<usize> {
        let result = if addr_ptr != 0 {
            let storage = addr_ptr as *mut super::super::handle::AddrStorage;
            unsafe {
                libc::accept(
                    fd,
                    &mut (*storage).storage as *mut _ as *mut libc::sockaddr,
                    &mut (*storage).len,
                )
            }
        } else {
            unsafe { libc::accept(fd, std::ptr::null_mut(), std::ptr::null_mut()) }
        };

        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn recv(fd: i32, buf: *mut u8, len: usize) -> io::Result<usize> {
        let result = unsafe { libc::recv(fd, buf as *mut _, len, 0) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn recvfrom(fd: i32, buf: *mut u8, len: usize, addr_ptr: u64) -> io::Result<usize> {
        let result = if addr_ptr != 0 {
            let storage = addr_ptr as *mut super::super::handle::AddrStorage;
            unsafe {
                libc::recvfrom(
                    fd,
                    buf as *mut _,
                    len,
                    0,
                    &mut (*storage).storage as *mut _ as *mut libc::sockaddr,
                    &mut (*storage).len,
                )
            }
        } else {
            unsafe {
                libc::recvfrom(
                    fd,
                    buf as *mut _,
                    len,
                    0,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            }
        };

        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn recvmsg(fd: i32, msg: *mut libc::msghdr) -> io::Result<usize> {
        let result = unsafe { libc::recvmsg(fd, msg, 0) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn send(fd: i32, buf: *const u8, len: usize) -> io::Result<usize> {
        let result = unsafe { libc::send(fd, buf as *const _, len, 0) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn sendto(
        fd: i32,
        buf: *const u8,
        len: usize,
        addr: *const libc::sockaddr,
        addrlen: u32,
    ) -> io::Result<usize> {
        let result = unsafe { libc::sendto(fd, buf as *const _, len, 0, addr, addrlen) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn sendmsg(fd: i32, msg: *const libc::msghdr) -> io::Result<usize> {
        let result = unsafe { libc::sendmsg(fd, msg, 0) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn connect_start(fd: i32, addr: *const libc::sockaddr, addrlen: u32) -> io::Result<()> {
        let result = unsafe { libc::connect(fd, addr, addrlen) };
        if result == 0 {
            Ok(())
        } else {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINPROGRESS) {
                Ok(()) // Connect in progress
            } else {
                Err(err)
            }
        }
    }

    pub fn connect_finish(fd: i32) -> io::Result<usize> {
        let mut err: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        let result = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                &mut err as *mut _ as *mut _,
                &mut len,
            )
        };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else if err != 0 {
            Err(io::Error::from_raw_os_error(err))
        } else {
            Ok(0)
        }
    }

    pub fn shutdown(fd: i32, how: i32) -> io::Result<usize> {
        let result = unsafe { libc::shutdown(fd, how) };
        if result == 0 {
            Ok(0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn close(fd: i32) -> io::Result<usize> {
        let result = unsafe { libc::close(fd) };
        if result == 0 {
            Ok(0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn fsync(fd: i32, datasync: bool) -> io::Result<usize> {
        #[cfg(target_os = "linux")]
        let result = if datasync {
            unsafe { libc::fdatasync(fd) }
        } else {
            unsafe { libc::fsync(fd) }
        };
        #[cfg(not(target_os = "linux"))]
        let result = {
            let _ = datasync;
            unsafe { libc::fsync(fd) }
        };
        if result == 0 {
            Ok(0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn pread(fd: i32, buf: *mut u8, len: usize, offset: u64) -> io::Result<usize> {
        let result = unsafe { libc::pread(fd, buf as *mut _, len, offset as libc::off_t) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn pwrite(fd: i32, buf: *const u8, len: usize, offset: u64) -> io::Result<usize> {
        let result = unsafe { libc::pwrite(fd, buf as *const _, len, offset as libc::off_t) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn open(path: *const i8, flags: i32, mode: u32) -> io::Result<usize> {
        let result = unsafe { libc::open(path, flags, mode) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn ftruncate(fd: i32, length: u64) -> io::Result<usize> {
        let result = unsafe { libc::ftruncate(fd, length as libc::off_t) };
        if result == 0 {
            Ok(0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn fstat(fd: i32, stat: *mut libc::stat) -> io::Result<usize> {
        let result = unsafe { libc::fstat(fd, stat) };
        if result == 0 {
            Ok(0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn mkdir(path: *const i8, mode: u32) -> io::Result<usize> {
        let result = unsafe { libc::mkdir(path, mode) };
        if result == 0 {
            Ok(0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn unlink(path: *const i8) -> io::Result<usize> {
        let result = unsafe { libc::unlink(path) };
        if result == 0 {
            Ok(0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn rmdir(path: *const i8) -> io::Result<usize> {
        let result = unsafe { libc::rmdir(path) };
        if result == 0 {
            Ok(0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn rename(old_path: *const i8, new_path: *const i8) -> io::Result<usize> {
        let result = unsafe { libc::rename(old_path, new_path) };
        if result == 0 {
            Ok(0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn symlink(target: *const i8, linkpath: *const i8) -> io::Result<usize> {
        let result = unsafe { libc::symlink(target, linkpath) };
        if result == 0 {
            Ok(0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn link(old_path: *const i8, new_path: *const i8) -> io::Result<usize> {
        let result = unsafe { libc::link(old_path, new_path) };
        if result == 0 {
            Ok(0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn readlink(path: *const i8, buf: *mut i8, bufsiz: usize) -> io::Result<usize> {
        let result = unsafe { libc::readlink(path, buf, bufsiz) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(target_os = "linux")]
    pub fn splice(
        fd_in: i32,
        off_in: i64,
        fd_out: i32,
        off_out: i64,
        len: usize,
    ) -> io::Result<usize> {
        let mut off_in_val = off_in;
        let mut off_out_val = off_out;
        let off_in_ptr = if off_in < 0 {
            std::ptr::null_mut()
        } else {
            &mut off_in_val
        };
        let off_out_ptr = if off_out < 0 {
            std::ptr::null_mut()
        } else {
            &mut off_out_val
        };
        let result = unsafe {
            libc::splice(
                fd_in,
                off_in_ptr,
                fd_out,
                off_out_ptr,
                len,
                libc::SPLICE_F_NONBLOCK,
            )
        };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    pub fn splice(
        fd_in: i32,
        off_in: i64,
        fd_out: i32,
        off_out: i64,
        len: usize,
    ) -> io::Result<usize> {
        // Fallback: read from fd_in, write to fd_out
        let mut buf = vec![0u8; len.min(65536)];
        let mut total = 0usize;
        let mut remaining = len;

        // Seek if offset specified
        if off_in >= 0 {
            let r = unsafe { libc::lseek(fd_in, off_in as libc::off_t, libc::SEEK_SET) };
            if r < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        if off_out >= 0 {
            let r = unsafe { libc::lseek(fd_out, off_out as libc::off_t, libc::SEEK_SET) };
            if r < 0 {
                return Err(io::Error::last_os_error());
            }
        }

        while remaining > 0 {
            let to_read = remaining.min(buf.len());
            let n = unsafe { libc::read(fd_in, buf.as_mut_ptr() as *mut _, to_read) };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            if n == 0 {
                break; // EOF
            }

            let mut written = 0;
            while written < n as usize {
                let w = unsafe {
                    libc::write(
                        fd_out,
                        buf[written..].as_ptr() as *const _,
                        (n as usize) - written,
                    )
                };
                if w < 0 {
                    return Err(io::Error::last_os_error());
                }
                written += w as usize;
            }
            total += n as usize;
            remaining -= n as usize;
        }
        Ok(total)
    }

    pub fn pipe(fds: *mut i32) -> io::Result<usize> {
        let result = unsafe { libc::pipe(fds) };
        if result == 0 {
            Ok(0)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn readv(fd: i32, iov: *const libc::iovec, iovcnt: i32) -> io::Result<usize> {
        let result = unsafe { libc::readv(fd, iov, iovcnt) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn writev(fd: i32, iov: *const libc::iovec, iovcnt: i32) -> io::Result<usize> {
        let result = unsafe { libc::writev(fd, iov, iovcnt) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn preadv(fd: i32, iov: *const libc::iovec, iovcnt: i32, offset: u64) -> io::Result<usize> {
        let result = unsafe { libc::preadv(fd, iov, iovcnt, offset as libc::off_t) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn pwritev(
        fd: i32,
        iov: *const libc::iovec,
        iovcnt: i32,
        offset: u64,
    ) -> io::Result<usize> {
        let result = unsafe { libc::pwritev(fd, iov, iovcnt, offset as libc::off_t) };
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

//! io-uring driver for Linux.
//!
//! This provides true async IO - the kernel performs the operations
//! and we just collect completions.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use io_uring::{IoUring, opcode, types};

use super::contract::{ContractId, IoContract, IoContractGroupInner, Op, OpType, PollWaker};
use crate::ptr::StripedArc;

// ============================================================================
// Helper structs for complex operations
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

// ============================================================================
// UringWaker
// ============================================================================

/// Shared state for UringWaker.
struct WakerState {
    eventfd: i32,
    inflight: std::sync::atomic::AtomicBool,
}

/// Waker for io-uring driver using eventfd.
pub struct UringWaker(Arc<WakerState>);

impl UringWaker {
    fn new(state: Arc<WakerState>) -> Self {
        Self(state)
    }
}

impl PollWaker for UringWaker {
    fn wake(&self) {
        // Only one wake can be inflight at a time
        if self.0.inflight.swap(true, Ordering::AcqRel) {
            return;
        }
        let buf = 1u64;
        unsafe { libc::write(self.0.eventfd, &buf as *const _ as *const libc::c_void, 8) };
    }
}

// ============================================================================
// UringDriver
// ============================================================================

/// io-uring based IO driver.
pub struct UringDriver {
    uring: IoUring,
    /// Pending operations indexed by user_data
    pending: HashMap<u64, PendingOp>,
    /// Next user_data value
    next_id: u64,
    /// Reference to the contract group for completions
    contract_group: StripedArc<IoContractGroupInner>,
    /// Waker state for wake-up notifications
    waker_state: Arc<WakerState>,
    /// Bias for fair work selection
    bias: u64,
    /// Max contracts to pop per consume call
    batch_size: u32,
}

/// Result of building an operation - either an io_uring entry or an immediate result.
enum BuildResult {
    /// Submit this entry to io_uring
    Entry(io_uring::squeue::Entry),
    /// Complete immediately with this result
    Immediate(io::Result<usize>),
}

struct PendingOp {
    contract_id: ContractId,
}

impl UringDriver {
    /// Create a new io-uring driver with the specified queue depth.
    pub fn new(entries: u32, contract_group: StripedArc<IoContractGroupInner>) -> io::Result<Self> {
        let uring = IoUring::new(entries)?;

        // Create an eventfd for wake-up notifications
        let eventfd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if eventfd < 0 {
            return Err(io::Error::last_os_error());
        }

        let waker_state = Arc::new(WakerState {
            eventfd,
            inflight: std::sync::atomic::AtomicBool::new(false),
        });

        Ok(Self {
            uring,
            pending: HashMap::new(),
            next_id: 1,
            contract_group,
            waker_state,
            bias: 0,
            batch_size: entries,
        })
    }

    /// Submit an IO operation from a contract.
    pub fn submit(&mut self, contract: &IoContract) -> io::Result<()> {
        let user_data = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);

        // SAFETY: Contract is in EXECUTING state, so we can read the op
        let op = unsafe { contract.op() };

        match Self::build_entry(op, user_data)? {
            BuildResult::Entry(entry) => {
                // Track the pending operation
                self.pending.insert(
                    user_data,
                    PendingOp {
                        contract_id: contract.id(),
                    },
                );

                // Submit to the uring
                unsafe {
                    self.uring
                        .submission()
                        .push(&entry)
                        .map_err(|_| io::Error::other("submission queue full"))?;
                }
            }
            BuildResult::Immediate(result) => {
                // Complete immediately without going through io_uring
                self.contract_group.complete(contract.id(), result);
            }
        }

        Ok(())
    }

    /// Build an io-uring submission entry for the operation, or return immediate result.
    fn build_entry(op: &Op, user_data: u64) -> io::Result<BuildResult> {
        let op_type = OpType::from_u8(op.op_type);
        let fd = types::Fd(op.fd);

        let entry = match op_type {
            // File operations
            Some(OpType::ReadAt) => opcode::Read::new(fd, op.buf_ptr, op.buf_len)
                .offset(op.offset)
                .build()
                .user_data(user_data),
            Some(OpType::WriteAt) => opcode::Write::new(fd, op.buf_ptr, op.buf_len)
                .offset(op.offset)
                .build()
                .user_data(user_data),
            Some(OpType::Read) => opcode::Read::new(fd, op.buf_ptr, op.buf_len)
                .build()
                .user_data(user_data),
            Some(OpType::Write) => opcode::Write::new(fd, op.buf_ptr, op.buf_len)
                .build()
                .user_data(user_data),
            Some(OpType::ReadVectoredAt) => {
                // buf_ptr points to an array of iovec, buf_len is the count
                let iovecs = op.buf_ptr as *const libc::iovec;
                opcode::Readv::new(fd, iovecs, op.buf_len)
                    .offset(op.offset)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::WriteVectoredAt) => {
                let iovecs = op.buf_ptr as *const libc::iovec;
                opcode::Writev::new(fd, iovecs, op.buf_len)
                    .offset(op.offset)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::ReadVectored) => {
                let iovecs = op.buf_ptr as *const libc::iovec;
                opcode::Readv::new(fd, iovecs, op.buf_len)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::WriteVectored) => {
                let iovecs = op.buf_ptr as *const libc::iovec;
                opcode::Writev::new(fd, iovecs, op.buf_len)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Fsync) => opcode::Fsync::new(fd).build().user_data(user_data),
            Some(OpType::Fdatasync) => opcode::Fsync::new(fd)
                .flags(io_uring::types::FsyncFlags::DATASYNC)
                .build()
                .user_data(user_data),
            Some(OpType::Open) => {
                // buf_ptr points to the path (null-terminated), offset contains flags, buf_len contains mode
                let path = op.buf_ptr as *const i8;
                opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path)
                    .flags(op.offset as i32)
                    .mode(op.buf_len)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Close) => opcode::Close::new(fd).build().user_data(user_data),
            Some(OpType::Truncate) => {
                // offset contains the new size
                opcode::Fallocate::new(fd, op.offset)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Stat) => {
                // buf_ptr points to statx struct, offset contains flags, buf_len contains mask
                let statx = op.buf_ptr as *mut types::statx;
                opcode::Statx::new(fd, c"".as_ptr(), statx)
                    .flags(op.offset as i32)
                    .mask(op.buf_len)
                    .build()
                    .user_data(user_data)
            }

            // Socket operations
            Some(OpType::Accept) => {
                opcode::Accept::new(fd, std::ptr::null_mut(), std::ptr::null_mut())
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Connect) => {
                // buf_ptr points to sockaddr, buf_len is the address length
                let addr = op.buf_ptr as *const libc::sockaddr;
                opcode::Connect::new(fd, addr, op.buf_len)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Recv) => opcode::Recv::new(fd, op.buf_ptr, op.buf_len)
                .build()
                .user_data(user_data),
            Some(OpType::Send) => opcode::Send::new(fd, op.buf_ptr, op.buf_len)
                .build()
                .user_data(user_data),
            Some(OpType::RecvFrom) => {
                // For recvfrom, we use recvmsg with a msghdr
                let msghdr = op.buf_ptr as *mut libc::msghdr;
                opcode::RecvMsg::new(fd, msghdr)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::SendTo) => {
                // For sendto, we use sendmsg with a msghdr
                let msghdr = op.buf_ptr as *const libc::msghdr;
                opcode::SendMsg::new(fd, msghdr)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::RecvVectored) => {
                let msghdr = op.buf_ptr as *mut libc::msghdr;
                opcode::RecvMsg::new(fd, msghdr)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::SendVectored) => {
                let msghdr = op.buf_ptr as *const libc::msghdr;
                opcode::SendMsg::new(fd, msghdr)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::RecvMsg) => {
                let msghdr = op.buf_ptr as *mut libc::msghdr;
                opcode::RecvMsg::new(fd, msghdr)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::SendMsg) => {
                let msghdr = op.buf_ptr as *const libc::msghdr;
                opcode::SendMsg::new(fd, msghdr)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Shutdown) => {
                // offset contains the how parameter (SHUT_RD, SHUT_WR, SHUT_RDWR)
                opcode::Shutdown::new(fd, op.offset as i32)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Socket) => {
                // fd field contains domain, offset contains type, buf_len contains protocol
                opcode::Socket::new(op.fd, op.offset as i32, op.buf_len as i32)
                    .build()
                    .user_data(user_data)
            }

            // Directory operations
            Some(OpType::CreateDir) => {
                let path = op.buf_ptr as *const i8;
                opcode::MkDirAt::new(types::Fd(libc::AT_FDCWD), path)
                    .mode(op.buf_len)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Unlink) => {
                let path = op.buf_ptr as *const i8;
                opcode::UnlinkAt::new(types::Fd(libc::AT_FDCWD), path)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Rmdir) => {
                let path = op.buf_ptr as *const i8;
                opcode::UnlinkAt::new(types::Fd(libc::AT_FDCWD), path)
                    .flags(libc::AT_REMOVEDIR)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Rename) => {
                // buf_ptr points to a struct with old_path and new_path
                let paths = op.buf_ptr as *const RenamePaths;
                unsafe {
                    opcode::RenameAt::new(
                        types::Fd(libc::AT_FDCWD),
                        (*paths).old_path,
                        types::Fd(libc::AT_FDCWD),
                        (*paths).new_path,
                    )
                    .build()
                    .user_data(user_data)
                }
            }
            Some(OpType::Symlink) => {
                let paths = op.buf_ptr as *const SymlinkPaths;
                unsafe {
                    opcode::SymlinkAt::new(
                        types::Fd(libc::AT_FDCWD),
                        (*paths).target,
                        (*paths).linkpath,
                    )
                    .build()
                    .user_data(user_data)
                }
            }
            Some(OpType::Link) => {
                let paths = op.buf_ptr as *const LinkPaths;
                unsafe {
                    opcode::LinkAt::new(
                        types::Fd(libc::AT_FDCWD),
                        (*paths).old_path,
                        types::Fd(libc::AT_FDCWD),
                        (*paths).new_path,
                    )
                    .build()
                    .user_data(user_data)
                }
            }

            // Advanced operations
            Some(OpType::Splice) => {
                let splice_data = op.buf_ptr as *const SpliceData;
                unsafe {
                    opcode::Splice::new(
                        fd,
                        (*splice_data).off_in,
                        types::Fd((*splice_data).fd_out),
                        (*splice_data).off_out,
                        op.buf_len,
                    )
                    .build()
                    .user_data(user_data)
                }
            }
            Some(OpType::Poll) => {
                // offset contains poll mask
                opcode::PollAdd::new(fd, op.offset as u32)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Cancel) => {
                // offset contains the user_data of the operation to cancel
                opcode::AsyncCancel::new(op.offset)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Nop) => opcode::Nop::new().build().user_data(user_data),

            Some(OpType::Bind) => {
                // buf_ptr points to sockaddr, buf_len is address length
                let addr = op.buf_ptr as *const libc::sockaddr;
                opcode::Bind::new(fd, addr, op.buf_len as libc::socklen_t)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Listen) => {
                // offset contains backlog
                opcode::Listen::new(fd, op.offset as i32)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::SetSockOpt) => {
                // buf_ptr points to value, buf_len is value length
                // offset encodes level (low 32 bits) and optname (high 32 bits)
                let level = (op.offset & 0xFFFFFFFF) as u32;
                let optname = (op.offset >> 32) as u32;
                opcode::SetSockOpt::new(
                    fd,
                    level,
                    optname,
                    op.buf_ptr as *const libc::c_void,
                    op.buf_len as u32,
                )
                .build()
                .user_data(user_data)
            }
            Some(OpType::Pipe) => {
                // buf_ptr points to [i32; 2] for pipe fds
                let fds = op.buf_ptr as *mut i32;
                // offset contains flags (e.g., O_CLOEXEC, O_NONBLOCK)
                opcode::Pipe::new(fds)
                    .flags(op.offset as u32)
                    .build()
                    .user_data(user_data)
            }
            Some(OpType::Readlink) => {
                // No io_uring opcode for readlink - use synchronous call
                // buf_ptr points to path, offset points to output buffer, buf_len is buffer size
                let path = op.buf_ptr as *const i8;
                let buf = op.offset as *mut i8;
                let result = unsafe { libc::readlink(path, buf, op.buf_len as usize) };
                if result >= 0 {
                    return Ok(BuildResult::Immediate(Ok(result as usize)));
                } else {
                    return Ok(BuildResult::Immediate(Err(io::Error::last_os_error())));
                }
            }
            Some(OpType::GetSockOpt) => {
                // No io_uring opcode for getsockopt - use synchronous call
                // buf_ptr points to value buffer, buf_len points to socklen_t for value length
                // offset encodes level (low 32 bits) and optname (high 32 bits)
                let level = (op.offset & 0xFFFFFFFF) as i32;
                let optname = (op.offset >> 32) as i32;
                let optlen = op.buf_len as *mut libc::socklen_t;
                let result = unsafe {
                    libc::getsockopt(
                        op.fd,
                        level,
                        optname,
                        op.buf_ptr as *mut libc::c_void,
                        optlen,
                    )
                };
                if result == 0 {
                    return Ok(BuildResult::Immediate(Ok(0)));
                } else {
                    return Ok(BuildResult::Immediate(Err(io::Error::last_os_error())));
                }
            }
            Some(OpType::ConnectNamedPipe) => {
                // Windows-only operation
                return Err(io::Error::other("ConnectNamedPipe is Windows-only"));
            }

            None => {
                return Err(io::Error::other("invalid op type"));
            }
        };

        Ok(BuildResult::Entry(entry))
    }

    /// Submit pending entries to the kernel and wait for at least one completion.
    pub fn submit_and_wait(&mut self, wait_for: u32) -> io::Result<()> {
        // Consume new operations before waiting
        self.consume_contract_group();

        self.uring.submit_and_wait(wait_for as usize)?;
        self.process_completions();
        Ok(())
    }

    /// Process completions without waiting.
    pub fn poll(&mut self) -> io::Result<()> {
        // Consume new operations
        self.consume_contract_group();

        self.uring.submit()?;
        self.process_completions();

        Ok(())
    }

    /// Consume operations from the contract group and submit them to io-uring.
    fn consume_contract_group(&mut self) {
        for _ in 0..self.batch_size {
            let Some(contract) = self.contract_group.pop(&mut self.bias) else {
                break;
            };
            let contract_id = contract.id();
            // SAFETY: Contract is in EXECUTING state
            let op = unsafe { contract.op() };

            let user_data = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);

            match Self::build_entry(op, user_data) {
                Ok(BuildResult::Entry(entry)) => {
                    self.pending.insert(user_data, PendingOp { contract_id });
                    unsafe {
                        if let Err(_) = self.uring.submission().push(&entry) {
                            self.pending.remove(&user_data);
                            self.contract_group.complete(
                                contract_id,
                                Err(io::Error::other("submission queue full")),
                            );
                        }
                    }
                }
                Ok(BuildResult::Immediate(result)) => {
                    self.contract_group.complete(contract_id, result);
                }
                Err(e) => {
                    self.contract_group.complete(contract_id, Err(e));
                }
            }
        }

        // Drain the eventfd to clear wake-up notifications
        self.drain_eventfd();
    }

    /// Drain the eventfd to clear pending wake-up notifications.
    fn drain_eventfd(&self) {
        let mut buf: u64 = 0;

        // Read non-blockingly - EAGAIN is expected if there's nothing to read
        unsafe {
            libc::read(
                self.waker_state.eventfd,
                &mut buf as *mut _ as *mut libc::c_void,
                std::mem::size_of_val(&buf),
            )
        };

        // Clear inflight so new wakes can come through
        self.waker_state.inflight.store(false, Ordering::Release);
    }

    /// Process all available completions.
    fn process_completions(&mut self) {
        for cqe in self.uring.completion() {
            let user_data = cqe.user_data();
            let result = cqe.result();

            if let Some(pending) = self.pending.remove(&user_data) {
                let bytes_result = if result < 0 {
                    Err(io::Error::from_raw_os_error(-result))
                } else {
                    Ok(result as usize)
                };

                // Signal completion on the contract group
                self.contract_group
                    .complete(pending.contract_id, bytes_result);
            }
        }
    }

    /// Number of pending operations.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Get a waker that can wake this driver's poll loop.
    pub fn waker(&self) -> UringWaker {
        UringWaker::new(Arc::clone(&self.waker_state))
    }
}

impl Drop for UringDriver {
    fn drop(&mut self) {
        // Close the eventfd
        unsafe { libc::close(self.waker_state.eventfd) };
    }
}

//! Unix standard I/O implementation.
//!
//! On Unix, we use blocking I/O for stdio since it's simpler and more reliable.
//! The async wrappers just delegate to std::io.

use std::io::{self, Read, Write};

use crate::buf::{BufResult, IoBuf, IoBufMut, IoVectoredBuf, IoVectoredBufMut, SetLen};
use crate::io::{AsyncRead, AsyncWrite};

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};

/// A handle to the standard input stream of a process.
///
/// See [`stdin`](super::stdin).
#[derive(Debug, Clone)]
pub struct Stdin {
    _private: (),
}

impl Stdin {
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }
}

impl AsyncRead for Stdin {
    async fn read<B: IoBufMut + SetLen + Send>(&mut self, mut buf: B) -> BufResult<usize, B> {
        // Use blocking read - stdin is typically line-buffered anyway
        // Cast MaybeUninit<u8> pointer to u8 pointer for std::io::Read
        let ptr = buf.buf_mut_ptr() as *mut u8;
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr, buf.buf_capacity()) };
        match io::stdin().read(slice) {
            Ok(n) => {
                unsafe { buf.set_len(n) };
                BufResult(Ok(n), buf)
            }
            Err(e) => BufResult(Err(e), buf),
        }
    }

    async fn read_vectored<V: IoVectoredBufMut + Send>(&mut self, buf: V) -> BufResult<usize, V> {
        // Fall back to reading into first buffer
        crate::loop_read_vectored!(buf, iter, self.read(iter))
    }
}

#[cfg(unix)]
impl AsRawFd for Stdin {
    fn as_raw_fd(&self) -> RawFd {
        libc::STDIN_FILENO
    }
}

/// A handle to the standard output stream of a process.
///
/// See [`stdout`](super::stdout).
#[derive(Debug, Clone)]
pub struct Stdout {
    _private: (),
}

impl Stdout {
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }
}

impl AsyncWrite for Stdout {
    async fn write<T: IoBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
        match io::stdout().write(buf.as_init()) {
            Ok(n) => BufResult(Ok(n), buf),
            Err(e) => BufResult(Err(e), buf),
        }
    }

    async fn write_vectored<T: IoVectoredBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
        crate::loop_write_vectored!(buf, iter, self.write(iter))
    }

    async fn flush(&mut self) -> io::Result<()> {
        io::stdout().flush()
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.flush().await
    }
}

#[cfg(unix)]
impl AsRawFd for Stdout {
    fn as_raw_fd(&self) -> RawFd {
        libc::STDOUT_FILENO
    }
}

/// A handle to the standard error stream of a process.
///
/// See [`stderr`](super::stderr).
#[derive(Debug, Clone)]
pub struct Stderr {
    _private: (),
}

impl Stderr {
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }
}

impl AsyncWrite for Stderr {
    async fn write<T: IoBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
        match io::stderr().write(buf.as_init()) {
            Ok(n) => BufResult(Ok(n), buf),
            Err(e) => BufResult(Err(e), buf),
        }
    }

    async fn write_vectored<T: IoVectoredBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
        crate::loop_write_vectored!(buf, iter, self.write(iter))
    }

    async fn flush(&mut self) -> io::Result<()> {
        io::stderr().flush()
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.flush().await
    }
}

#[cfg(unix)]
impl AsRawFd for Stderr {
    fn as_raw_fd(&self) -> RawFd {
        libc::STDERR_FILENO
    }
}

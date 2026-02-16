//! Async file type.

use std::io;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, IntoRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, FromRawHandle, IntoRawHandle, RawHandle};

use crate::buf::{BufResult, IoBuf, IoBufMut, SetLen};
use crate::driver::{IoHandle, IoPool, ReadFuture, SyncFuture, WriteFuture};
use crate::io::{AsyncReadAt, AsyncWriteAt};

use super::OpenOptions;

/// A reference to an open file on the filesystem.
///
/// An instance of a `File` can be read and/or written depending on what options
/// it was opened with. The `File` type provides **positional** read and write
/// operations. The file does not maintain an internal cursor. The caller is
/// required to specify an offset when issuing an operation.
///
/// # Examples
///
/// ```ignore
/// use maniac::fs::File;
/// use maniac::driver::IoPool;
///
/// let pool = IoPool::new()?;
/// let file = File::open(&pool, "foo.txt")?;
/// let (n, buf) = file.read_at(0, vec![0u8; 1024]).await.unwrap();
/// println!("Read {} bytes", n);
/// ```
#[derive(Debug)]
pub struct File {
    handle: IoHandle,
}

impl File {
    /// Creates a `File` from a standard library `File`.
    ///
    /// The file descriptor is extracted and registered with the given [`IoPool`].
    pub fn from_std(pool: &IoPool, file: std::fs::File) -> io::Result<Self> {
        #[cfg(unix)]
        let fd = file.into_raw_fd();
        #[cfg(windows)]
        let fd = file.into_raw_handle() as i32;

        let handle = pool.open(fd);
        Ok(Self { handle })
    }

    /// Attempts to open a file in read-only mode.
    ///
    /// See the [`OpenOptions::open`] method for more details.
    pub fn open(pool: &IoPool, path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open(pool, path)
    }

    /// Opens a file in write-only mode.
    ///
    /// This function will create a file if it does not exist,
    /// and will truncate it if it does.
    pub fn create(pool: &IoPool, path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(pool, path)
    }

    /// Returns the raw file descriptor.
    #[cfg(unix)]
    pub fn as_raw_fd(&self) -> RawFd {
        self.handle.fd()
    }

    /// Returns the raw handle.
    #[cfg(windows)]
    pub fn as_raw_handle(&self) -> RawHandle {
        self.handle.fd() as RawHandle
    }

    /// Read from the file at the specified offset.
    ///
    /// Takes ownership of the buffer and returns it along with the result.
    pub fn read_at<B: IoBufMut + SetLen + Send>(&self, offset: u64, buf: B) -> ReadFuture<'_, B> {
        self.handle.read_at(offset, buf)
    }

    /// Write to the file at the specified offset.
    ///
    /// Takes ownership of the buffer and returns it along with the result.
    pub fn write_at<B: IoBuf + Send>(&self, offset: u64, buf: B) -> WriteFuture<'_, B> {
        self.handle.write_at(offset, buf)
    }

    /// Attempts to sync all OS-internal metadata to disk.
    ///
    /// This function will attempt to ensure that all in-memory data reaches the
    /// filesystem before returning.
    pub fn sync_all(&self) -> SyncFuture<'_> {
        self.handle.sync(false)
    }

    /// This function is similar to [`sync_all`], except that it might not
    /// synchronize file metadata to the filesystem.
    ///
    /// [`sync_all`]: File::sync_all
    pub fn sync_data(&self) -> SyncFuture<'_> {
        self.handle.sync(true)
    }

    /// Close the file.
    pub fn close(self) -> SyncFuture<'static> {
        self.handle.close()
    }
}

impl AsyncReadAt for File {
    async fn read_at<B: IoBufMut + SetLen + Send>(&self, buf: B, pos: u64) -> BufResult<usize, B> {
        self.handle.read_at(pos, buf).await
    }
}

impl AsyncWriteAt for File {
    async fn write_at<B: IoBuf + Send>(&mut self, buf: B, pos: u64) -> BufResult<usize, B> {
        self.handle.write_at(pos, buf).await
    }
}

impl AsyncWriteAt for &File {
    async fn write_at<B: IoBuf + Send>(&mut self, buf: B, pos: u64) -> BufResult<usize, B> {
        self.handle.write_at(pos, buf).await
    }
}

#[cfg(unix)]
impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.handle.fd()
    }
}

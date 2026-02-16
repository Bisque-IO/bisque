//! Unix pipe types.

use std::{
    io,
    os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
    path::Path,
};

use crate::buf::{BufResult, IoBuf, IoBufMut, IoVectoredBuf, IoVectoredBufMut, SetLen};
use crate::driver::{IoPool, ReadFuture, SyncFuture, WriteFuture};
use crate::io::{AsyncRead, AsyncWrite};

use super::File;

/// Creates a pair of anonymous pipe.
///
/// ```ignore
/// use maniac::fs::pipe::anonymous;
/// use maniac::driver::IoPool;
///
/// let pool = IoPool::new()?;
/// let (mut rx, mut tx) = anonymous(&pool)?;
///
/// tx.write_all("Hello world!").await?;
/// let (_, buf) = rx.read_exact(Vec::with_capacity(12)).await?;
/// assert_eq!(&buf, b"Hello world!");
/// ```
pub fn anonymous(pool: &IoPool) -> io::Result<(Receiver, Sender)> {
    let (receiver, sender) = os_pipe::pipe()?;
    let receiver = Receiver::from_file(File::from_std(pool, unsafe {
        std::fs::File::from_raw_fd(receiver.into_raw_fd())
    })?)?;
    let sender = Sender::from_file(File::from_std(pool, unsafe {
        std::fs::File::from_raw_fd(sender.into_raw_fd())
    })?)?;
    Ok((receiver, sender))
}

/// Options and flags which can be used to configure how a FIFO file is opened.
///
/// This builder allows configuring how to create a pipe end from a FIFO file.
/// Generally speaking, when using `OpenOptions`, you'll first call [`new`],
/// then chain calls to methods to set each option, then call either
/// [`open_receiver`] or [`open_sender`], passing the path of the FIFO file you
/// are trying to open. This will give you a [`io::Result`] with a pipe end
/// inside that you can further operate on.
///
/// [`new`]: OpenOptions::new
/// [`open_receiver`]: OpenOptions::open_receiver
/// [`open_sender`]: OpenOptions::open_sender
///
/// # Examples
///
/// Opening a pair of pipe ends from a FIFO file:
///
/// ```ignore
/// use maniac::fs::pipe;
/// use maniac::driver::IoPool;
///
/// const FIFO_NAME: &str = "path/to/a/fifo";
///
/// let pool = IoPool::new()?;
/// let rx = pipe::OpenOptions::new().open_receiver(&pool, FIFO_NAME)?;
/// let tx = pipe::OpenOptions::new().open_sender(&pool, FIFO_NAME)?;
/// ```
#[derive(Clone, Debug)]
pub struct OpenOptions {
    #[cfg(target_os = "linux")]
    read_write: bool,
    unchecked: bool,
}

impl OpenOptions {
    /// Creates a blank new set of options ready for configuration.
    ///
    /// All options are initially set to `false`.
    pub fn new() -> OpenOptions {
        OpenOptions {
            #[cfg(target_os = "linux")]
            read_write: false,
            unchecked: false,
        }
    }

    /// Sets the option for read-write access.
    ///
    /// This option, when true, will indicate that a FIFO file will be opened
    /// in read-write access mode. This operation is not defined by the POSIX
    /// standard and is only guaranteed to work on Linux.
    #[cfg(target_os = "linux")]
    #[cfg_attr(docsrs, doc(cfg(target_os = "linux")))]
    pub fn read_write(&mut self, value: bool) -> &mut Self {
        self.read_write = value;
        self
    }

    /// Sets the option to skip the check for FIFO file type.
    ///
    /// By default, [`open_receiver`] and [`open_sender`] functions will check
    /// if the opened file is a FIFO file. Set this option to `true` if you are
    /// sure the file is a FIFO file.
    ///
    /// [`open_receiver`]: OpenOptions::open_receiver
    /// [`open_sender`]: OpenOptions::open_sender
    pub fn unchecked(&mut self, value: bool) -> &mut Self {
        self.unchecked = value;
        self
    }

    /// Creates a [`Receiver`] from a FIFO file with the options specified by
    /// `self`.
    ///
    /// This function will open the FIFO file at the specified path, possibly
    /// check if it is a pipe, and associate the pipe with the given IoPool
    /// for reading.
    ///
    /// # Errors
    ///
    /// If the file type check fails, this function will fail with
    /// `io::ErrorKind::InvalidInput`. This function may also fail with
    /// other standard OS errors.
    pub fn open_receiver<P: AsRef<Path>>(&self, pool: &IoPool, path: P) -> io::Result<Receiver> {
        let file = self.open(pool, path.as_ref(), PipeEnd::Receiver)?;
        Receiver::from_file(file)
    }

    /// Creates a [`Sender`] from a FIFO file with the options specified by
    /// `self`.
    ///
    /// This function will open the FIFO file at the specified path, possibly
    /// check if it is a pipe, and associate the pipe with the given IoPool
    /// for writing.
    ///
    /// # Errors
    ///
    /// If the file type check fails, this function will fail with
    /// `io::ErrorKind::InvalidInput`. If the file is not opened in
    /// read-write access mode and the file is not currently open for
    /// reading, this function will fail with `ENXIO`. This function may
    /// also fail with other standard OS errors.
    pub fn open_sender<P: AsRef<Path>>(&self, pool: &IoPool, path: P) -> io::Result<Sender> {
        let file = self.open(pool, path.as_ref(), PipeEnd::Sender)?;
        Sender::from_file(file)
    }

    fn open(&self, pool: &IoPool, path: &Path, pipe_end: PipeEnd) -> io::Result<File> {
        let mut options = crate::fs::OpenOptions::new();
        options
            .read(pipe_end == PipeEnd::Receiver)
            .write(pipe_end == PipeEnd::Sender);

        #[cfg(target_os = "linux")]
        if self.read_write {
            options.read(true).write(true);
        }

        let file = options.open(pool, path)?;

        if !self.unchecked && !is_fifo(&file)? {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "not a pipe"));
        }

        Ok(file)
    }
}

impl Default for OpenOptions {
    fn default() -> OpenOptions {
        OpenOptions::new()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PipeEnd {
    Sender,
    Receiver,
}

/// Writing end of a Unix pipe.
///
/// It can be constructed from a FIFO file with [`OpenOptions::open_sender`].
#[derive(Debug)]
pub struct Sender {
    file: File,
}

impl Sender {
    pub(crate) fn from_file(file: File) -> io::Result<Sender> {
        set_nonblocking(&file)?;
        Ok(Sender { file })
    }

    /// Close the pipe.
    pub fn close(self) -> SyncFuture<'static> {
        self.file.close()
    }

    /// Write to the pipe at offset 0.
    pub fn write<B: IoBuf + Send>(&self, buf: B) -> WriteFuture<'_, B> {
        self.file.write_at(0, buf)
    }
}

impl AsyncWrite for Sender {
    async fn write<T: IoBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
        self.file.write_at(0, buf).await
    }

    async fn write_vectored<T: IoVectoredBuf + Send>(&mut self, buf: T) -> BufResult<usize, T> {
        crate::loop_write_vectored!(buf, iter, self.write(iter))
    }

    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl AsRawFd for Sender {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

/// Reading end of a Unix pipe.
///
/// It can be constructed from a FIFO file with [`OpenOptions::open_receiver`].
#[derive(Debug)]
pub struct Receiver {
    file: File,
}

impl Receiver {
    pub(crate) fn from_file(file: File) -> io::Result<Receiver> {
        set_nonblocking(&file)?;
        Ok(Receiver { file })
    }

    /// Close the pipe.
    pub fn close(self) -> SyncFuture<'static> {
        self.file.close()
    }

    /// Read from the pipe at offset 0.
    pub fn read<B: IoBufMut + SetLen + Send>(&self, buf: B) -> ReadFuture<'_, B> {
        self.file.read_at(0, buf)
    }
}

impl AsyncRead for Receiver {
    async fn read<B: IoBufMut + SetLen + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        self.file.read_at(0, buf).await
    }
}

impl AsRawFd for Receiver {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

/// Checks if file is a FIFO
fn is_fifo(file: &File) -> io::Result<bool> {
    use std::os::unix::prelude::FileTypeExt;

    let metadata = std::fs::metadata(format!("/proc/self/fd/{}", file.as_raw_fd()))?;
    Ok(metadata.file_type().is_fifo())
}

/// Sets file's flags with O_NONBLOCK by fcntl.
fn set_nonblocking(file: &impl AsRawFd) -> io::Result<()> {
    let fd = file.as_raw_fd();
    let current_flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if current_flags == -1 {
        return Err(io::Error::last_os_error());
    }
    let flags = current_flags | libc::O_NONBLOCK;
    if flags != current_flags {
        let result = unsafe { libc::fcntl(fd, libc::F_SETFL, flags) };
        if result == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

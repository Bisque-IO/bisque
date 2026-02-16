//! Functionality to split an I/O type into separate read and write halves.

use std::fmt::Debug;

use crate::buf::{BufResult, IoBuf, IoBufMut, IoVectoredBuf, IoVectoredBufMut};

use super::super::{AsyncRead, AsyncReadAt, AsyncWrite, AsyncWriteAt, IoResult};
use super::bilock::BiLock;

/// Splits a single value implementing `AsyncRead + AsyncWrite` into separate
/// [`AsyncRead`] and [`AsyncWrite`] handles without internal synchronization
/// (not `Send` and `Sync`).
pub fn split<T: AsyncRead + AsyncWrite>(stream: T) -> (ReadHalf<T>, WriteHalf<T>) {
    Split::new(stream).split()
}

/// A trait for types that can be split into separate read and write halves.
///
/// This trait enables an I/O type to be divided into two separate components:
/// one for reading and one for writing. This is particularly useful in async
/// contexts where you might want to perform concurrent read and write
/// operations from different tasks.
///
/// # Implementor
/// - Any `(R, W)` tuple implements this trait.
/// - Duplex types like TCP and Unix sockets can implement this trait directly
///   without any lock thanks to the underlying sockets' duplex nature.
pub trait Splittable {
    /// The type of the read half, which normally implements [`AsyncRead`] or
    /// [`AsyncReadAt`].
    type ReadHalf;

    /// The type of the write half, which normally implements [`AsyncWrite`] or
    /// [`AsyncWriteAt`].
    type WriteHalf;

    /// Consumes `self` and returns a tuple containing separate read and write
    /// halves.
    ///
    /// The returned halves can be used independently to perform read and write
    /// operations respectively, potentially from different tasks
    /// concurrently.
    fn split(self) -> (Self::ReadHalf, Self::WriteHalf);
}

/// Enables splitting an I/O type into separate read and write halves.
#[derive(Debug)]
pub struct Split<T>(BiLock<T>, BiLock<T>);

impl<T> Split<T> {
    /// Creates a new `Split` from the given stream.
    pub fn new(stream: T) -> Self {
        let (r, w) = BiLock::new(stream);
        Split(r, w)
    }
}

impl<T> Splittable for Split<T> {
    type ReadHalf = ReadHalf<T>;
    type WriteHalf = WriteHalf<T>;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        (ReadHalf(self.0), WriteHalf(self.1))
    }
}

impl<R, W> Splittable for (R, W) {
    type ReadHalf = R;
    type WriteHalf = W;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        self
    }
}

/// The readable half of a value returned from [`split`].
#[derive(Debug)]
pub struct ReadHalf<T>(BiLock<T>);

impl<T> ReadHalf<T> {
    /// Reunites with a previously split [`WriteHalf`].
    ///
    /// # Panics
    ///
    /// If this [`ReadHalf`] and the given [`WriteHalf`] do not
    /// originate from the same [`split`] operation this method will panic.
    #[track_caller]
    pub fn unsplit(self, other: WriteHalf<T>) -> T {
        self.0
            .try_join(other.0)
            .expect("`ReadHalf` and `WriteHalf` must originate from the same `Split`")
    }
}

impl<T: AsyncRead> AsyncRead for ReadHalf<T> {
    async fn read<B: IoBufMut + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        self.0.lock().await.read(buf).await
    }

    async fn read_vectored<V: IoVectoredBufMut + Send>(&mut self, buf: V) -> BufResult<usize, V> {
        self.0.lock().await.read_vectored(buf).await
    }
}

impl<T: AsyncReadAt> AsyncReadAt for ReadHalf<T> {
    async fn read_at<B: IoBufMut + Send>(&self, buf: B, pos: u64) -> BufResult<usize, B> {
        self.0.lock().await.read_at(buf, pos).await
    }
}

/// The writable half of a value returned from [`split`].
#[derive(Debug)]
pub struct WriteHalf<T>(BiLock<T>);

impl<T: AsyncWrite> AsyncWrite for WriteHalf<T> {
    async fn write<B: IoBuf + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        self.0.lock().await.write(buf).await
    }

    async fn write_vectored<B: IoVectoredBuf + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        self.0.lock().await.write_vectored(buf).await
    }

    async fn flush(&mut self) -> IoResult<()> {
        self.0.lock().await.flush().await
    }

    async fn shutdown(&mut self) -> IoResult<()> {
        self.0.lock().await.shutdown().await
    }
}

impl<T: AsyncWriteAt> AsyncWriteAt for WriteHalf<T> {
    async fn write_at<B: IoBuf + Send>(&mut self, buf: B, pos: u64) -> BufResult<usize, B> {
        self.0.lock().await.write_at(buf, pos).await
    }

    async fn write_vectored_at<B: IoVectoredBuf + Send>(
        &mut self,
        buf: B,
        pos: u64,
    ) -> BufResult<usize, B> {
        self.0.lock().await.write_vectored_at(buf, pos).await
    }
}

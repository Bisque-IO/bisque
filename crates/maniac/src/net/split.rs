//! Stream splitting utilities.
//!
//! Allows splitting a stream into read and write halves that can be
//! used concurrently.

use std::sync::Arc;
use std::{error::Error, fmt, io};

use crate::buf::{BufResult, IoBuf, IoBufMut, IoVectoredBuf, IoVectoredBufMut, SetLen};
use crate::io::{AsyncRead, AsyncWrite};

pub(crate) fn split<T>(stream: &T) -> (ReadHalf<'_, T>, WriteHalf<'_, T>)
where
    for<'a> &'a T: AsyncRead + AsyncWrite,
{
    (ReadHalf(stream), WriteHalf(stream))
}

/// Borrowed read half.
#[derive(Debug)]
pub struct ReadHalf<'a, T>(pub(crate) &'a T);

impl<T> AsyncRead for ReadHalf<'_, T>
where
    for<'a> &'a T: AsyncRead,
{
    async fn read<B: IoBufMut + SetLen + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        self.0.read(buf).await
    }

    async fn read_vectored<V: IoVectoredBufMut + Send>(&mut self, buf: V) -> BufResult<usize, V> {
        self.0.read_vectored(buf).await
    }
}

/// Borrowed write half.
#[derive(Debug)]
pub struct WriteHalf<'a, T>(pub(crate) &'a T);

impl<T> AsyncWrite for WriteHalf<'_, T>
where
    for<'a> &'a T: AsyncWrite,
{
    async fn write<B: IoBuf + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        self.0.write(buf).await
    }

    async fn write_vectored<B: IoVectoredBuf + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        self.0.write_vectored(buf).await
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.0.flush().await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        self.0.shutdown().await
    }
}

pub(crate) fn into_split<T>(stream: T) -> (OwnedReadHalf<T>, OwnedWriteHalf<T>)
where
    for<'a> &'a T: AsyncRead + AsyncWrite,
{
    let shared = Arc::new(stream);
    (OwnedReadHalf(Arc::clone(&shared)), OwnedWriteHalf(shared))
}

/// Owned read half.
#[derive(Debug)]
pub struct OwnedReadHalf<T>(pub(crate) Arc<T>);

impl<T> OwnedReadHalf<T> {
    /// Attempts to put the two halves of a stream back together and
    /// recover the original socket. Succeeds only if the two halves
    /// originated from the same call to `into_split`.
    pub fn reunite(self, w: OwnedWriteHalf<T>) -> Result<T, ReuniteError<T>> {
        if Arc::ptr_eq(&self.0, &w.0) {
            drop(w);
            // Now self.0 is the only Arc, we can unwrap it
            // unwrap() is safe here since we just dropped the other reference
            Ok(Arc::try_unwrap(self.0)
                .ok()
                .expect("reunite: Arc should have single owner after dropping other half"))
        } else {
            Err(ReuniteError(self, w))
        }
    }
}

impl<T> AsyncRead for OwnedReadHalf<T>
where
    for<'a> &'a T: AsyncRead,
{
    async fn read<B: IoBufMut + SetLen + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        (&*self.0).read(buf).await
    }

    async fn read_vectored<V: IoVectoredBufMut + Send>(&mut self, buf: V) -> BufResult<usize, V> {
        (&*self.0).read_vectored(buf).await
    }
}

/// Owned write half.
#[derive(Debug)]
pub struct OwnedWriteHalf<T>(pub(crate) Arc<T>);

impl<T> AsyncWrite for OwnedWriteHalf<T>
where
    for<'a> &'a T: AsyncWrite,
{
    async fn write<B: IoBuf + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        (&*self.0).write(buf).await
    }

    async fn write_vectored<B: IoVectoredBuf + Send>(&mut self, buf: B) -> BufResult<usize, B> {
        (&*self.0).write_vectored(buf).await
    }

    async fn flush(&mut self) -> io::Result<()> {
        (&*self.0).flush().await
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        (&*self.0).shutdown().await
    }
}

/// Error indicating that two halves were not from the same socket, and thus
/// could not be reunited.
#[derive(Debug)]
pub struct ReuniteError<T>(pub OwnedReadHalf<T>, pub OwnedWriteHalf<T>);

impl<T> fmt::Display for ReuniteError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "tried to reunite halves that are not from the same socket"
        )
    }
}

impl<T: fmt::Debug> Error for ReuniteError<T> {}

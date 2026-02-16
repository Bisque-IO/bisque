use crate::buf::{BufResult, IoBufMut};

use super::super::{AsyncBufRead, AsyncRead, AsyncWrite, IoResult};
use super::Splittable;

/// An empty reader and writer constructed via [`null`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Null {
    _p: (),
}

impl AsyncRead for Null {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> crate::buf::BufResult<usize, B> {
        BufResult(Ok(0), buf)
    }
}

impl AsyncBufRead for Null {
    async fn fill_buf(&mut self) -> IoResult<&'_ [u8]> {
        Ok(&[])
    }

    fn consume(&mut self, _: usize) {}
}

impl AsyncWrite for Null {
    async fn write<T: crate::buf::IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        BufResult(Ok(0), buf)
    }

    async fn write_vectored<T: crate::buf::IoVectoredBuf>(
        &mut self,
        buf: T,
    ) -> BufResult<usize, T> {
        BufResult(Ok(0), buf)
    }

    async fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl Splittable for Null {
    type ReadHalf = Null;
    type WriteHalf = Null;

    fn split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        (Null { _p: () }, Null { _p: () })
    }
}

/// Create a new [`Null`] reader and writer which acts like a black hole.
///
/// All reads from and writes to this reader will return
/// `BufResult(Ok(0), buf)` and leave the buffer unchanged.
#[inline(always)]
pub fn null() -> Null {
    Null { _p: () }
}

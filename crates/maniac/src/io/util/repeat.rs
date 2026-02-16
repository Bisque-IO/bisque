use std::mem::MaybeUninit;

use crate::buf::BufResult;

use super::super::{AsyncBufRead, AsyncRead, IoResult};

/// A reader that infinitely repeats one byte constructed via [`repeat`].
///
/// All reads from this reader will succeed by filling the specified buffer with
/// the given byte.
pub struct Repeat(u8);

impl AsyncRead for Repeat {
    async fn read<B: crate::buf::IoBufMut>(
        &mut self,
        mut buf: B,
    ) -> crate::buf::BufResult<usize, B> {
        let slice = buf.as_uninit();

        let len = slice.len();
        slice.fill(MaybeUninit::new(self.0));
        unsafe { buf.advance_to(len) };

        BufResult(Ok(len), buf)
    }
}

impl AsyncBufRead for Repeat {
    async fn fill_buf(&mut self) -> IoResult<&'_ [u8]> {
        Ok(std::slice::from_ref(&self.0))
    }

    fn consume(&mut self, _: usize) {}
}

/// Creates a reader that infinitely repeats one byte.
///
/// All reads from this reader will succeed by filling the specified buffer with
/// the given byte.
pub fn repeat(byte: u8) -> Repeat {
    Repeat(byte)
}

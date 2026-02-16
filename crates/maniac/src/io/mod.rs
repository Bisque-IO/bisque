//! Traits and utilities for completion-based IO.
//!
//! # Contents
//! ### Fundamental
//!
//! - [`AsyncRead`]: Async read into a buffer implements [`IoBufMut`]
//! - [`AsyncReadAt`]: Async read into a buffer implements [`IoBufMut`] with
//!   offset
//! - [`AsyncWrite`]: Async write from a buffer implements [`IoBuf`]
//! - [`AsyncWriteAt`]: Async write from a buffer implements [`IoBuf`] with
//!   offset
//!
//! ### Buffered IO
//!
//! - [`AsyncBufRead`]: Trait of async read with buffered content
//! - [`BufReader`]: An async reader with internal buffer
//! - [`BufWriter`]: An async writer with internal buffer
//!
//! ### Extension
//!
//! - [`AsyncReadExt`]: Extension trait for [`AsyncRead`]
//! - [`AsyncReadAtExt`]: Extension trait for [`AsyncReadAt`]
//! - [`AsyncWriteExt`]: Extension trait for [`AsyncWrite`]
//! - [`AsyncWriteAtExt`]: Extension trait for [`AsyncWriteAt`]
//!
//! [`IoBufMut`]: crate::buf::IoBufMut
//! [`IoBuf`]: crate::buf::IoBuf

#![allow(async_fn_in_trait)]

mod buffer;
mod read;
pub mod util;
mod write;

pub(crate) type IoResult<T> = std::io::Result<T>;

pub use read::*;
#[doc(inline)]
pub use util::{copy, null, repeat, split::split};
pub use write::*;

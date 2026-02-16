//! Filesystem utilities built on maniac's IO driver.
//!
//! This module provides async file operations using the [`IoPool`] driver.
//!
//! # Example
//!
//! ```ignore
//! use maniac::fs::File;
//! use maniac::driver::IoPool;
//!
//! let pool = IoPool::new()?;
//! let file = File::open(&pool, "foo.txt")?;
//! let (n, buf) = file.read_at(0, vec![0u8; 1024]).await.unwrap();
//! ```

mod file;
pub use file::*;

mod open_options;
pub use open_options::*;

mod metadata;
pub use metadata::*;

mod stdio;
pub use stdio::*;

mod utils;
pub use utils::*;

#[cfg(windows)]
pub mod named_pipe;

#[cfg(unix)]
pub mod pipe;

/// Convert a path to a CString for use with libc functions.
#[cfg(unix)]
#[allow(dead_code)]
pub(crate) fn path_string(path: impl AsRef<std::path::Path>) -> std::io::Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;

    std::ffi::CString::new(path.as_ref().as_os_str().as_bytes().to_vec()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "file name contained an unexpected NUL byte",
        )
    })
}

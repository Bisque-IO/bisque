//! Filesystem utility functions.

#[cfg(unix)]
#[path = "unix.rs"]
mod sys;

#[cfg(windows)]
#[path = "windows.rs"]
mod sys;

use std::{io, path::Path};

use crate::buf::{BufResult, IoBuf};
use crate::buf_try;
use crate::driver::IoPool;
use crate::io::AsyncWriteAtExt;

use super::{File, metadata};

/// Removes a file from the filesystem.
pub fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    sys::remove_file(path)
}

/// Removes an empty directory.
pub fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    sys::remove_dir(path)
}

/// Creates a new, empty directory at the provided path.
pub fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    DirBuilder::new().create(path)
}

/// Recursively create a directory and all of its parent components if they are
/// missing.
pub fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    DirBuilder::new().recursive(true).create(path)
}

/// Rename a file or directory to a new name, replacing the original file if
/// `to` already exists.
pub fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    sys::rename(from, to)
}

/// Creates a new symbolic link on the filesystem.
#[cfg(unix)]
pub fn symlink(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    sys::symlink(original, link)
}

/// Creates a new symlink to a non-directory file on the filesystem.
#[cfg(windows)]
pub fn symlink_file(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    sys::symlink_file(original, link)
}

/// Creates a new symlink to a directory on the filesystem.
#[cfg(windows)]
pub fn symlink_dir(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    sys::symlink_dir(original, link)
}

/// Creates a new hard link on the filesystem.
pub fn hard_link(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    sys::hard_link(original, link)
}

/// Write a slice as the entire contents of a file.
///
/// This function will create a file if it does not exist,
/// and will entirely replace its contents if it does.
pub async fn write<P: AsRef<Path>, B: IoBuf + Send>(
    pool: &IoPool,
    path: P,
    buf: B,
) -> BufResult<(), B> {
    let (mut file, buf) = buf_try!(File::create(pool, path), buf);
    file.write_all_at(buf, 0).await
}

/// Read the entire contents of a file into a bytes vector.
pub fn read<P: AsRef<Path>>(_pool: &IoPool, path: P) -> io::Result<Vec<u8>> {
    let contents = std::fs::read(path)?;
    Ok(contents)
}

/// A builder used to create directories in various manners.
pub struct DirBuilder {
    inner: sys::DirBuilder,
    recursive: bool,
}

impl Default for DirBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DirBuilder {
    /// Creates a new set of options with default mode/security settings for all
    /// platforms and also non-recursive.
    pub fn new() -> Self {
        Self {
            inner: sys::DirBuilder::new(),
            recursive: false,
        }
    }

    /// Indicates that directories should be created recursively, creating all
    /// parent directories. Parents that do not exist are created with the same
    /// security and permissions settings.
    pub fn recursive(&mut self, recursive: bool) -> &mut Self {
        self.recursive = recursive;
        self
    }

    /// Creates the specified directory with the options configured in this
    /// builder.
    pub fn create(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let path = path.as_ref();
        if self.recursive {
            self.create_dir_all(path)
        } else {
            self.inner.create(path)
        }
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        if path == Path::new("") {
            return Ok(());
        }

        match self.inner.create(path) {
            Ok(()) => return Ok(()),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(_) if metadata(path).map(|m| m.is_dir()).unwrap_or_default() => return Ok(()),
            Err(e) => return Err(e),
        }
        match path.parent() {
            Some(p) => self.create_dir_all(p)?,
            None => {
                return Err(io::Error::other("failed to create whole tree"));
            }
        }
        match self.inner.create(path) {
            Ok(()) => Ok(()),
            Err(_) if metadata(path).map(|m| m.is_dir()).unwrap_or_default() => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(unix)]
impl std::os::unix::fs::DirBuilderExt for DirBuilder {
    fn mode(&mut self, mode: u32) -> &mut Self {
        self.inner.mode(mode);
        self
    }
}

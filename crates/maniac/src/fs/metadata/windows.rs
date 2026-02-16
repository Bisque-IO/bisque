//! Windows-specific metadata implementation.

pub use std::fs::{FileType, Metadata, Permissions};
use std::{io, path::Path};

pub fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    std::fs::metadata(path)
}

pub fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    std::fs::symlink_metadata(path)
}

pub fn set_permissions(path: impl AsRef<Path>, perm: Permissions) -> io::Result<()> {
    std::fs::set_permissions(path, perm)
}

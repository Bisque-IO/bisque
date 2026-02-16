//! Windows-specific filesystem utility implementations.

use std::{io, path::Path};

pub fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    std::fs::remove_file(path)
}

pub fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    std::fs::remove_dir(path)
}

pub fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    std::fs::rename(from, to)
}

pub fn symlink_file(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    std::os::windows::fs::symlink_file(original, link)
}

pub fn symlink_dir(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    std::os::windows::fs::symlink_dir(original, link)
}

pub fn hard_link(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    std::fs::hard_link(original, link)
}

pub struct DirBuilder;

impl DirBuilder {
    pub fn new() -> Self {
        Self
    }

    pub fn create(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir(path)
    }
}

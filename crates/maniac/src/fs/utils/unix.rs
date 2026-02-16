//! Unix-specific filesystem utility implementations.

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

pub fn symlink(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    std::os::unix::fs::symlink(original, link)
}

pub fn hard_link(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
    std::fs::hard_link(original, link)
}

pub struct DirBuilder {
    mode: u32,
}

impl DirBuilder {
    pub fn new() -> Self {
        Self { mode: 0o777 }
    }

    pub fn mode(&mut self, mode: u32) {
        self.mode = mode;
    }

    pub fn create(&self, path: &Path) -> io::Result<()> {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(self.mode);
        builder.create(path)
    }
}

//! Unix-specific file opening implementation.

use std::{fs, io, path::Path};

use crate::driver::IoPool;

use super::super::File;

#[derive(Clone, Debug)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
    custom_flags: i32,
    mode: libc::mode_t,
}

impl OpenOptions {
    pub fn new() -> OpenOptions {
        OpenOptions {
            read: false,
            write: false,
            truncate: false,
            create: false,
            create_new: false,
            custom_flags: 0,
            mode: 0o666,
        }
    }

    pub fn read(&mut self, read: bool) {
        self.read = read;
    }

    pub fn write(&mut self, write: bool) {
        self.write = write;
    }

    pub fn truncate(&mut self, truncate: bool) {
        self.truncate = truncate;
    }

    pub fn create(&mut self, create: bool) {
        self.create = create;
    }

    pub fn create_new(&mut self, create_new: bool) {
        self.create_new = create_new;
    }

    pub fn custom_flags(&mut self, flags: i32) {
        self.custom_flags = flags;
    }

    pub fn mode(&mut self, mode: u32) {
        self.mode = mode as libc::mode_t;
    }

    #[allow(dead_code)]
    fn get_access_mode(&self) -> io::Result<libc::c_int> {
        match (self.read, self.write) {
            (true, false) => Ok(libc::O_RDONLY),
            (false, true) => Ok(libc::O_WRONLY),
            (true, true) => Ok(libc::O_RDWR),
            (false, false) => Err(io::Error::from_raw_os_error(libc::EINVAL)),
        }
    }

    #[allow(dead_code)]
    fn get_creation_mode(&self) -> io::Result<libc::c_int> {
        if !self.write && (self.truncate || self.create || self.create_new) {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }

        Ok(match (self.create, self.truncate, self.create_new) {
            (false, false, false) => 0,
            (true, false, false) => libc::O_CREAT,
            (false, true, false) => libc::O_TRUNC,
            (true, true, false) => libc::O_CREAT | libc::O_TRUNC,
            (_, _, true) => libc::O_CREAT | libc::O_EXCL,
        })
    }

    pub fn open(&self, pool: &IoPool, path: impl AsRef<Path>) -> io::Result<File> {
        // Use std::fs::OpenOptions to open the file synchronously,
        // then register with the IoPool.
        let mut opts = fs::OpenOptions::new();
        opts.read(self.read)
            .write(self.write)
            .truncate(self.truncate)
            .create(self.create)
            .create_new(self.create_new);

        // Apply Unix-specific options
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(self.mode as u32);

        // Build custom flags
        let custom = self.custom_flags & !libc::O_ACCMODE;
        if custom != 0 {
            opts.custom_flags(custom);
        }

        let std_file = opts.open(path)?;
        File::from_std(pool, std_file)
    }
}

//! Unix-specific metadata implementation.

pub use std::fs::Permissions;
use std::{
    io,
    mem::MaybeUninit,
    os::unix::prelude::{FileTypeExt, MetadataExt, PermissionsExt},
    path::Path,
    time::{Duration, SystemTime},
};

pub fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    std::fs::metadata(path).map(|m| Metadata::from_std(m))
}

pub fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    std::fs::symlink_metadata(path).map(|m| Metadata::from_std(m))
}

pub fn set_permissions(path: impl AsRef<Path>, perm: Permissions) -> io::Result<()> {
    std::fs::set_permissions(path, perm)
}

#[derive(Clone)]
pub struct Metadata {
    stat: libc::stat,
}

impl Metadata {
    /// Create from std::fs::Metadata by extracting stat info.
    pub fn from_std(m: std::fs::Metadata) -> Self {
        // Build a stat struct from std::fs::Metadata using MetadataExt
        // Use MaybeUninit to avoid accessing private fields
        let stat = unsafe {
            let mut stat: MaybeUninit<libc::stat> = MaybeUninit::zeroed();
            let s = stat.as_mut_ptr();
            (*s).st_dev = m.dev() as _;
            (*s).st_ino = m.ino() as _;
            (*s).st_mode = m.mode() as _;
            (*s).st_nlink = m.nlink() as _;
            (*s).st_uid = m.uid() as _;
            (*s).st_gid = m.gid() as _;
            (*s).st_rdev = m.rdev() as _;
            (*s).st_size = m.size() as _;
            (*s).st_atime = m.atime() as _;
            (*s).st_atime_nsec = m.atime_nsec() as _;
            (*s).st_mtime = m.mtime() as _;
            (*s).st_mtime_nsec = m.mtime_nsec() as _;
            (*s).st_ctime = m.ctime() as _;
            (*s).st_ctime_nsec = m.ctime_nsec() as _;
            (*s).st_blksize = m.blksize() as _;
            (*s).st_blocks = m.blocks() as _;
            stat.assume_init()
        };
        Self { stat }
    }

    /// Create from [`libc::stat`].
    pub fn from_stat(stat: libc::stat) -> Self {
        Self { stat }
    }

    pub fn file_type(&self) -> FileType {
        FileType(self.stat.st_mode as _)
    }

    pub fn is_dir(&self) -> bool {
        self.file_type().is_dir()
    }

    pub fn is_file(&self) -> bool {
        self.file_type().is_file()
    }

    pub fn is_symlink(&self) -> bool {
        self.file_type().is_symlink()
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.stat.st_size as _
    }

    pub fn permissions(&self) -> Permissions {
        Permissions::from_mode(self.stat.st_mode as _)
    }

    pub fn modified(&self) -> io::Result<SystemTime> {
        Ok(SystemTime::UNIX_EPOCH
            + Duration::from_secs(self.stat.st_mtime as _)
            + Duration::from_nanos(self.stat.st_mtime_nsec as _))
    }

    pub fn accessed(&self) -> io::Result<SystemTime> {
        Ok(SystemTime::UNIX_EPOCH
            + Duration::from_secs(self.stat.st_atime as _)
            + Duration::from_nanos(self.stat.st_atime_nsec as _))
    }

    pub fn created(&self) -> io::Result<SystemTime> {
        // On Linux, use ctime as a proxy for creation time
        // (it's actually the last status change time, but that's the best we can do)
        Ok(SystemTime::UNIX_EPOCH
            + Duration::from_secs(self.stat.st_ctime as _)
            + Duration::from_nanos(self.stat.st_ctime_nsec as _))
    }
}

impl MetadataExt for Metadata {
    fn dev(&self) -> u64 {
        self.stat.st_dev as _
    }

    fn ino(&self) -> u64 {
        self.stat.st_ino as _
    }

    fn mode(&self) -> u32 {
        self.stat.st_mode as _
    }

    fn nlink(&self) -> u64 {
        self.stat.st_nlink as _
    }

    fn uid(&self) -> u32 {
        self.stat.st_uid as _
    }

    fn gid(&self) -> u32 {
        self.stat.st_gid as _
    }

    fn rdev(&self) -> u64 {
        self.stat.st_rdev as _
    }

    fn size(&self) -> u64 {
        self.stat.st_size as _
    }

    fn atime(&self) -> i64 {
        self.stat.st_atime as _
    }

    fn atime_nsec(&self) -> i64 {
        self.stat.st_atime_nsec as _
    }

    fn mtime(&self) -> i64 {
        self.stat.st_mtime as _
    }

    fn mtime_nsec(&self) -> i64 {
        self.stat.st_mtime_nsec as _
    }

    fn ctime(&self) -> i64 {
        self.stat.st_ctime as _
    }

    fn ctime_nsec(&self) -> i64 {
        self.stat.st_ctime_nsec as _
    }

    fn blksize(&self) -> u64 {
        self.stat.st_blksize as _
    }

    fn blocks(&self) -> u64 {
        self.stat.st_blocks as _
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct FileType(pub(crate) libc::mode_t);

impl FileType {
    pub fn is_dir(&self) -> bool {
        self.is(libc::S_IFDIR)
    }

    pub fn is_file(&self) -> bool {
        self.is(libc::S_IFREG)
    }

    pub fn is_symlink(&self) -> bool {
        self.is(libc::S_IFLNK)
    }

    fn is(&self, mode: libc::mode_t) -> bool {
        self.masked() == mode
    }

    fn masked(&self) -> libc::mode_t {
        self.0 & libc::S_IFMT
    }
}

impl FileTypeExt for FileType {
    fn is_block_device(&self) -> bool {
        self.is(libc::S_IFBLK)
    }

    fn is_char_device(&self) -> bool {
        self.is(libc::S_IFCHR)
    }

    fn is_fifo(&self) -> bool {
        self.is(libc::S_IFIFO)
    }

    fn is_socket(&self) -> bool {
        self.is(libc::S_IFSOCK)
    }
}

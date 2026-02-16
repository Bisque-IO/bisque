//! [Windows named pipes](https://learn.microsoft.com/en-us/windows/win32/ipc/named-pipes).

use std::{ffi::OsStr, io, os::windows::io::FromRawHandle, ptr::null};

use crate::buf::{BufResult, IoBuf, IoBufMut, IoVectoredBuf, IoVectoredBufMut, SetLen};
use crate::driver::{IoPool, ReadFuture, SyncFuture, WriteFuture};
use crate::io::{AsyncRead, AsyncWrite};

use widestring::U16CString;
use windows_sys::Win32::{
    Foundation::HANDLE,
    Storage::FileSystem::{
        FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, PIPE_ACCESS_INBOUND,
        PIPE_ACCESS_OUTBOUND, WRITE_DAC, WRITE_OWNER,
    },
    System::{
        Pipes::{
            CreateNamedPipeW, DisconnectNamedPipe, GetNamedPipeInfo, PIPE_ACCEPT_REMOTE_CLIENTS,
            PIPE_READMODE_BYTE, PIPE_READMODE_MESSAGE, PIPE_REJECT_REMOTE_CLIENTS, PIPE_SERVER_END,
            PIPE_TYPE_BYTE, PIPE_TYPE_MESSAGE, PIPE_UNLIMITED_INSTANCES,
        },
        SystemServices::ACCESS_SYSTEM_SECURITY,
    },
};

use super::{File, OpenOptions};

#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, RawHandle};

/// Helper macro for Windows syscalls that return BOOL
macro_rules! syscall_bool {
    ($e:expr) => {{
        let res = $e;
        if res == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(res)
        }
    }};
}

/// Helper macro for Windows syscalls that return HANDLE
macro_rules! syscall_handle {
    ($e:expr) => {{
        let res = $e;
        if res == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
            Err(io::Error::last_os_error())
        } else {
            Ok(res)
        }
    }};
}

/// A [Windows named pipe] server.
///
/// Accepting client connections involves creating a server with
/// [`ServerOptions::create`] and waiting for clients to connect using
/// [`NamedPipeServer::connect`].
///
/// [Windows named pipe]: https://docs.microsoft.com/en-us/windows/win32/ipc/named-pipes
#[derive(Debug)]
pub struct NamedPipeServer {
    file: File,
}

impl NamedPipeServer {
    /// Retrieves information about the named pipe the server is associated
    /// with.
    pub fn info(&self) -> io::Result<PipeInfo> {
        unsafe { named_pipe_info(self.as_raw_handle() as _) }
    }

    /// Disconnects the server end of a named pipe instance from a client
    /// process.
    pub fn disconnect(&self) -> io::Result<()> {
        syscall_bool!(unsafe { DisconnectNamedPipe(self.as_raw_handle() as _) })?;
        Ok(())
    }

    /// Read from the pipe.
    pub fn read<B: IoBufMut + SetLen>(&self, buf: B) -> ReadFuture<B> {
        self.file.read_at(0, buf)
    }

    /// Write to the pipe.
    pub fn write<B: IoBuf>(&self, buf: B) -> WriteFuture<B> {
        self.file.write_at(0, buf)
    }

    /// Close the pipe.
    pub fn close(self) -> SyncFuture {
        self.file.close()
    }
}

impl AsyncRead for NamedPipeServer {
    async fn read<B: IoBufMut + SetLen>(&mut self, buf: B) -> BufResult<usize, B> {
        self.file.read_at(0, buf).await
    }

    async fn read_vectored<V: IoVectoredBufMut>(&mut self, buf: V) -> BufResult<usize, V> {
        crate::loop_read_vectored!(buf, iter, self.read(iter))
    }
}

impl AsyncWrite for NamedPipeServer {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        self.file.write_at(0, buf).await
    }

    async fn write_vectored<T: IoVectoredBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        crate::loop_write_vectored!(buf, iter, self.write(iter))
    }

    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(windows)]
impl AsRawHandle for NamedPipeServer {
    fn as_raw_handle(&self) -> RawHandle {
        self.file.as_raw_handle()
    }
}

/// A [Windows named pipe] client.
///
/// Constructed using [`ClientOptions::open`].
///
/// [Windows named pipe]: https://docs.microsoft.com/en-us/windows/win32/ipc/named-pipes
#[derive(Debug)]
pub struct NamedPipeClient {
    file: File,
}

impl NamedPipeClient {
    /// Retrieves information about the named pipe the client is associated
    /// with.
    pub fn info(&self) -> io::Result<PipeInfo> {
        unsafe { named_pipe_info(self.as_raw_handle() as _) }
    }

    /// Read from the pipe.
    pub fn read<B: IoBufMut + SetLen>(&self, buf: B) -> ReadFuture<B> {
        self.file.read_at(0, buf)
    }

    /// Write to the pipe.
    pub fn write<B: IoBuf>(&self, buf: B) -> WriteFuture<B> {
        self.file.write_at(0, buf)
    }

    /// Close the pipe.
    pub fn close(self) -> SyncFuture {
        self.file.close()
    }
}

impl AsyncRead for NamedPipeClient {
    async fn read<B: IoBufMut + SetLen>(&mut self, buf: B) -> BufResult<usize, B> {
        self.file.read_at(0, buf).await
    }

    async fn read_vectored<V: IoVectoredBufMut>(&mut self, buf: V) -> BufResult<usize, V> {
        crate::loop_read_vectored!(buf, iter, self.read(iter))
    }
}

impl AsyncWrite for NamedPipeClient {
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        self.file.write_at(0, buf).await
    }

    async fn write_vectored<T: IoVectoredBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        crate::loop_write_vectored!(buf, iter, self.write(iter))
    }

    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(windows)]
impl AsRawHandle for NamedPipeClient {
    fn as_raw_handle(&self) -> RawHandle {
        self.file.as_raw_handle()
    }
}

/// A builder structure for construct a named pipe with named pipe-specific
/// options. This is required to use for named pipe servers who wants to modify
/// pipe-related options.
///
/// See [`ServerOptions::create`].
#[derive(Debug, Clone)]
pub struct ServerOptions {
    // dwOpenMode
    access_inbound: bool,
    access_outbound: bool,
    first_pipe_instance: bool,
    write_dac: bool,
    write_owner: bool,
    access_system_security: bool,
    // dwPipeMode
    pipe_mode: PipeMode,
    reject_remote_clients: bool,
    // other options
    max_instances: u32,
    out_buffer_size: u32,
    in_buffer_size: u32,
    default_timeout: u32,
}

impl ServerOptions {
    /// Creates a new named pipe builder with the default settings.
    pub fn new() -> ServerOptions {
        ServerOptions {
            access_inbound: true,
            access_outbound: true,
            first_pipe_instance: false,
            write_dac: false,
            write_owner: false,
            access_system_security: false,
            pipe_mode: PipeMode::Byte,
            reject_remote_clients: true,
            max_instances: PIPE_UNLIMITED_INSTANCES,
            out_buffer_size: 65536,
            in_buffer_size: 65536,
            default_timeout: 0,
        }
    }

    /// The pipe mode.
    ///
    /// The default pipe mode is [`PipeMode::Byte`]. See [`PipeMode`] for
    /// documentation of what each mode means.
    pub fn pipe_mode(&mut self, pipe_mode: PipeMode) -> &mut Self {
        self.pipe_mode = pipe_mode;
        self
    }

    /// The flow of data in the pipe goes from client to server only.
    ///
    /// This corresponds to setting [`PIPE_ACCESS_INBOUND`].
    pub fn access_inbound(&mut self, allowed: bool) -> &mut Self {
        self.access_inbound = allowed;
        self
    }

    /// The flow of data in the pipe goes from server to client only.
    ///
    /// This corresponds to setting [`PIPE_ACCESS_OUTBOUND`].
    pub fn access_outbound(&mut self, allowed: bool) -> &mut Self {
        self.access_outbound = allowed;
        self
    }

    /// If you attempt to create multiple instances of a pipe with this flag
    /// set, creation of the first server instance succeeds, but creation of any
    /// subsequent instances will fail with
    /// [`std::io::ErrorKind::PermissionDenied`].
    ///
    /// This corresponds to setting [`FILE_FLAG_FIRST_PIPE_INSTANCE`].
    pub fn first_pipe_instance(&mut self, first: bool) -> &mut Self {
        self.first_pipe_instance = first;
        self
    }

    /// Requests permission to modify the pipe's discretionary access control
    /// list.
    ///
    /// This corresponds to setting [`WRITE_DAC`] in dwOpenMode.
    pub fn write_dac(&mut self, requested: bool) -> &mut Self {
        self.write_dac = requested;
        self
    }

    /// Requests permission to modify the pipe's owner.
    ///
    /// This corresponds to setting [`WRITE_OWNER`] in dwOpenMode.
    pub fn write_owner(&mut self, requested: bool) -> &mut Self {
        self.write_owner = requested;
        self
    }

    /// Requests permission to modify the pipe's system access control list.
    ///
    /// This corresponds to setting [`ACCESS_SYSTEM_SECURITY`] in dwOpenMode.
    pub fn access_system_security(&mut self, requested: bool) -> &mut Self {
        self.access_system_security = requested;
        self
    }

    /// Indicates whether this server can accept remote clients or not. Remote
    /// clients are disabled by default.
    ///
    /// This corresponds to setting [`PIPE_REJECT_REMOTE_CLIENTS`].
    pub fn reject_remote_clients(&mut self, reject: bool) -> &mut Self {
        self.reject_remote_clients = reject;
        self
    }

    /// The maximum number of instances that can be created for this pipe. The
    /// first instance of the pipe can specify this value; the same number must
    /// be specified for other instances of the pipe. Acceptable values are in
    /// the range 1 through 254. The default value is unlimited.
    ///
    /// # Panics
    ///
    /// This function will panic if more than 254 instances are specified.
    #[track_caller]
    pub fn max_instances(&mut self, instances: usize) -> &mut Self {
        assert!(instances < 255, "cannot specify more than 254 instances");
        self.max_instances = instances as u32;
        self
    }

    /// The number of bytes to reserve for the output buffer.
    pub fn out_buffer_size(&mut self, buffer: u32) -> &mut Self {
        self.out_buffer_size = buffer;
        self
    }

    /// The number of bytes to reserve for the input buffer.
    pub fn in_buffer_size(&mut self, buffer: u32) -> &mut Self {
        self.in_buffer_size = buffer;
        self
    }

    /// Creates the named pipe identified by `addr` for use as a server.
    ///
    /// This uses the [`CreateNamedPipe`] function.
    ///
    /// [`CreateNamedPipe`]: https://docs.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-createnamedpipea
    pub fn create(&self, pool: &IoPool, addr: impl AsRef<OsStr>) -> io::Result<NamedPipeServer> {
        let addr = U16CString::from_os_str(addr)
            .map_err(|e| io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let pipe_mode = {
            let mut mode = if matches!(self.pipe_mode, PipeMode::Message) {
                PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE
            } else {
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE
            };
            if self.reject_remote_clients {
                mode |= PIPE_REJECT_REMOTE_CLIENTS;
            } else {
                mode |= PIPE_ACCEPT_REMOTE_CLIENTS;
            }
            mode
        };
        let open_mode = {
            let mut mode = FILE_FLAG_OVERLAPPED;
            if self.access_inbound {
                mode |= PIPE_ACCESS_INBOUND;
            }
            if self.access_outbound {
                mode |= PIPE_ACCESS_OUTBOUND;
            }
            if self.first_pipe_instance {
                mode |= FILE_FLAG_FIRST_PIPE_INSTANCE;
            }
            if self.write_dac {
                mode |= WRITE_DAC;
            }
            if self.write_owner {
                mode |= WRITE_OWNER;
            }
            if self.access_system_security {
                mode |= ACCESS_SYSTEM_SECURITY;
            }
            mode
        };

        let h = syscall_handle!(unsafe {
            CreateNamedPipeW(
                addr.as_ptr(),
                open_mode,
                pipe_mode,
                self.max_instances,
                self.out_buffer_size,
                self.in_buffer_size,
                self.default_timeout,
                null(),
            )
        })?;

        let std_file = unsafe { std::fs::File::from_raw_handle(h as _) };
        let file = File::from_std(pool, std_file)?;

        Ok(NamedPipeServer { file })
    }
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// A builder suitable for building and interacting with named pipes from the
/// client side.
///
/// See [`ClientOptions::open`].
#[derive(Debug, Clone)]
pub struct ClientOptions {
    options: OpenOptions,
    pipe_mode: PipeMode,
}

impl ClientOptions {
    /// Creates a new named pipe builder with the default settings.
    pub fn new() -> Self {
        use windows_sys::Win32::Storage::FileSystem::SECURITY_IDENTIFICATION;

        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(true)
            .security_qos_flags(SECURITY_IDENTIFICATION);
        Self {
            options,
            pipe_mode: PipeMode::Byte,
        }
    }

    /// If the client supports reading data. This is enabled by default.
    pub fn read(&mut self, allowed: bool) -> &mut Self {
        self.options.read(allowed);
        self
    }

    /// If the created pipe supports writing data. This is enabled by default.
    pub fn write(&mut self, allowed: bool) -> &mut Self {
        self.options.write(allowed);
        self
    }

    /// Sets qos flags which are combined with other flags and attributes in the
    /// call to [`CreateFile`].
    ///
    /// [`CreateFile`]: https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea
    pub fn security_qos_flags(&mut self, flags: u32) -> &mut Self {
        self.options.security_qos_flags(flags);
        self
    }

    /// The pipe mode.
    ///
    /// The default pipe mode is [`PipeMode::Byte`]. See [`PipeMode`] for
    /// documentation of what each mode means.
    pub fn pipe_mode(&mut self, pipe_mode: PipeMode) -> &mut Self {
        self.pipe_mode = pipe_mode;
        self
    }

    /// Opens the named pipe identified by `addr`.
    ///
    /// This opens the client using [`CreateFile`] with the
    /// `dwCreationDisposition` option set to `OPEN_EXISTING`.
    ///
    /// [`CreateFile`]: https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilea
    pub fn open(&self, pool: &IoPool, addr: impl AsRef<OsStr>) -> io::Result<NamedPipeClient> {
        use windows_sys::Win32::System::Pipes::SetNamedPipeHandleState;

        let file = self.options.open(pool, addr.as_ref())?;

        if matches!(self.pipe_mode, PipeMode::Message) {
            let mode = PIPE_READMODE_MESSAGE;
            syscall_bool!(unsafe {
                SetNamedPipeHandleState(file.as_raw_handle() as _, &mode, null(), null())
            })?;
        }

        Ok(NamedPipeClient { file })
    }
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// The pipe mode of a named pipe.
///
/// Set through [`ServerOptions::pipe_mode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PipeMode {
    /// Data is written to the pipe as a stream of bytes. The pipe does not
    /// distinguish bytes written during different write operations.
    ///
    /// Corresponds to [`PIPE_TYPE_BYTE`].
    Byte,
    /// Data is written to the pipe as a stream of messages. The pipe treats the
    /// bytes written during each write operation as a message unit.
    ///
    /// Corresponds to [`PIPE_TYPE_MESSAGE`].
    Message,
}

/// Indicates the end of a named pipe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PipeEnd {
    /// The named pipe refers to the client end of a named pipe instance.
    Client,
    /// The named pipe refers to the server end of a named pipe instance.
    Server,
}

/// Information about a named pipe.
///
/// Constructed through [`NamedPipeServer::info`] or [`NamedPipeClient::info`].
#[derive(Debug)]
pub struct PipeInfo {
    /// Indicates the mode of a named pipe.
    pub mode: PipeMode,
    /// Indicates the end of a named pipe.
    pub end: PipeEnd,
    /// The maximum number of instances that can be created for this pipe.
    pub max_instances: u32,
    /// The number of bytes to reserve for the output buffer.
    pub out_buffer_size: u32,
    /// The number of bytes to reserve for the input buffer.
    pub in_buffer_size: u32,
}

/// Internal function to get the info out of a raw named pipe.
unsafe fn named_pipe_info(handle: HANDLE) -> io::Result<PipeInfo> {
    let mut flags = 0;
    let mut out_buffer_size = 0;
    let mut in_buffer_size = 0;
    let mut max_instances = 0;

    syscall_bool!(GetNamedPipeInfo(
        handle,
        &mut flags,
        &mut out_buffer_size,
        &mut in_buffer_size,
        &mut max_instances,
    ))?;

    let mut end = PipeEnd::Client;
    let mut mode = PipeMode::Byte;

    if flags & PIPE_SERVER_END != 0 {
        end = PipeEnd::Server;
    }

    if flags & PIPE_TYPE_MESSAGE != 0 {
        mode = PipeMode::Message;
    }

    Ok(PipeInfo {
        end,
        mode,
        out_buffer_size,
        in_buffer_size,
        max_instances,
    })
}

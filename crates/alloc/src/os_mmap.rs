//! Cross-platform virtual memory allocation.
//!
//! Provides `alloc_pages` / `free_pages` backed by:
//! - `mmap` / `munmap` on Unix (Linux, macOS, FreeBSD, etc.)
//! - `VirtualAlloc` / `VirtualFree` on Windows

use std::ptr;

/// Allocate `size` bytes of committed, read-write virtual memory.
///
/// The returned pointer is page-aligned. Returns null on failure.
/// `size` should be a multiple of the OS page size.
pub fn alloc_pages(size: usize) -> *mut u8 {
    if size == 0 {
        return ptr::null_mut();
    }

    #[cfg(unix)]
    {
        let p = unsafe {
            libc::mmap(
                ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if p == libc::MAP_FAILED || p.is_null() {
            ptr::null_mut()
        } else {
            p as *mut u8
        }
    }

    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Memory::*;
        let p = unsafe {
            VirtualAlloc(
                ptr::null_mut(),
                size,
                MEM_COMMIT | MEM_RESERVE,
                PAGE_READWRITE,
            )
        };
        if p.is_null() {
            ptr::null_mut()
        } else {
            p as *mut u8
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        // Fallback: use the global allocator for unsupported platforms.
        let layout = std::alloc::Layout::from_size_align(size, 4096).unwrap();
        unsafe { std::alloc::alloc_zeroed(layout) }
    }
}

/// Free `size` bytes of virtual memory at `ptr`.
///
/// # Safety
///
/// `ptr` must have been returned by `alloc_pages` with the same `size`.
pub unsafe fn free_pages(ptr: *mut u8, size: usize) {
    if ptr.is_null() || size == 0 {
        return;
    }

    #[cfg(unix)]
    unsafe {
        libc::munmap(ptr as *mut libc::c_void, size);
    }

    #[cfg(windows)]
    unsafe {
        use windows_sys::Win32::System::Memory::*;
        VirtualFree(ptr as *mut _, 0, MEM_RELEASE);
    }

    #[cfg(not(any(unix, windows)))]
    unsafe {
        let layout = std::alloc::Layout::from_size_align(size, 4096).unwrap();
        std::alloc::dealloc(ptr, layout);
    }
}

//! Cross-platform virtual memory allocation.
//!
//! Provides `alloc_pages` / `free_pages` backed by:
//! - `mmap` / `munmap` on Unix (Linux, macOS, FreeBSD, etc.)
//! - `VirtualAlloc` / `VirtualFree` on Windows

use std::ptr;

/// Allocate `size` bytes of committed, read-write virtual memory aligned to `alignment`.
///
/// Returns `(aligned_ptr, mmap_ptr, mmap_len)` where `aligned_ptr` is the usable
/// aligned region, and `mmap_ptr`/`mmap_len` are the original mmap for `free_pages`.
/// Returns `(null, null, 0)` on failure.
///
/// `alignment` must be a power of two and >= page size.
pub fn alloc_pages_aligned(size: usize, alignment: usize) -> (*mut u8, *mut u8, usize) {
    debug_assert!(alignment.is_power_of_two() && alignment >= 4096);
    // Over-allocate to guarantee alignment within the mmap.
    let mmap_len = size + alignment;
    let mmap_ptr = alloc_pages(mmap_len);
    if mmap_ptr.is_null() {
        return (ptr::null_mut(), ptr::null_mut(), 0);
    }
    let aligned = ((mmap_ptr as usize) + alignment - 1) & !(alignment - 1);
    (aligned as *mut u8, mmap_ptr, mmap_len)
}

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

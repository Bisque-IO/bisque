use std::io;
use std::mem;
use std::os::raw::c_void;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::SysStack;
use windows_sys::Win32::System::Memory::{
    MEM_COMMIT, MEM_RELEASE, MEM_RESERVE, PAGE_GUARD, PAGE_READONLY, PAGE_READWRITE, VirtualAlloc,
    VirtualFree, VirtualProtect,
};
use windows_sys::Win32::System::SystemInformation::{GetSystemInfo, SYSTEM_INFO};

#[path = "overflow_windows.rs"]
pub mod overflow;

pub unsafe fn allocate_stack(size: usize) -> io::Result<SysStack> {
    let ptr = VirtualAlloc(
        ptr::null_mut(),
        size,
        MEM_COMMIT | MEM_RESERVE,
        PAGE_READWRITE,
    );

    if ptr.is_null() {
        Err(io::Error::last_os_error())
    } else {
        Ok(SysStack::new(
            (ptr as usize + size) as *mut c_void,
            ptr as *mut c_void,
        ))
    }
}

pub unsafe fn protect_stack(stack: &SysStack) -> io::Result<SysStack> {
    let page_size = page_size();
    let mut old_prot: u32 = 0;

    debug_assert!(stack.len() % page_size == 0 && stack.len() != 0);

    let protect_result = VirtualProtect(
        stack.bottom() as *mut _,
        page_size,
        PAGE_READONLY | PAGE_GUARD,
        &mut old_prot,
    );

    if protect_result != 0 {
        let bottom = (stack.bottom() as usize + page_size) as *mut c_void;
        Ok(SysStack::new(stack.top(), bottom))
    } else {
        Err(io::Error::last_os_error())
    }
}

pub unsafe fn deallocate_stack(ptr: *mut c_void, _: usize) {
    let _ = VirtualFree(ptr as *mut _, 0, MEM_RELEASE);
}

pub fn page_size() -> usize {
    static PAGE_SIZE: AtomicUsize = AtomicUsize::new(0);

    let mut ret = PAGE_SIZE.load(Ordering::Relaxed);

    if ret == 0 {
        let mut info = mem::MaybeUninit::<SYSTEM_INFO>::uninit();
        unsafe {
            GetSystemInfo(info.as_mut_ptr());
        }
        let info = unsafe { info.assume_init() };
        ret = info.dwPageSize as usize;
        PAGE_SIZE.store(ret, Ordering::Relaxed);
    }

    ret
}

// Windows does not seem to provide a stack limit API
pub fn min_stack_size() -> usize {
    page_size()
}

// Windows does not seem to provide a stack limit API
pub fn max_stack_size() -> usize {
    usize::MAX
}

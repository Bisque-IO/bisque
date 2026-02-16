//! Preemptive scheduling support for worker threads.
//!
//! This module provides platform-specific mechanisms to **interrupt** worker threads
//! without terminating them, allowing generator context switching.
//!
//! # Architecture
//!
//! ## The "Trampoline" Approach
//!
//! To safely preempt a thread (on both Unix and Windows), we cannot simply run code
//! inside a signal handler or a suspended thread context, because:
//! 1. **Unix**: Signal handlers run with signals blocked and are extremely restricted (async-signal-safety).
//!    Accessing Thread-Local Storage (TLS) or acquiring locks can deadlock or panic.
//! 2. **Windows**: `SuspendThread` can stop a thread holding a lock (e.g., heap lock), leading to deadlocks
//!    if we try to allocate or use locks. Register corruption is also a major risk.
//!
//! **Solution**: We "inject" a function call into the target thread's stream of execution.
//!
//! 1. **Interrupt**: We stop the thread (Signal on Unix, SuspendThread on Windows).
//! 2. **Inject**: We modify the thread's stack and instruction pointer (RIP/PC) to simulate a call
//!    to a `trampoline` function, saving the original RIP/PC on the stack.
//! 3. **Resume**: The thread resumes execution at the `trampoline` (outside signal/suspend context).
//! 4. **Trampoline**:
//!    - Saves *all* volatile registers (preserving application state).
//!    - Calls `rust_preemption_helper()` (safe Rust code, can touch TLS/Locks).
//!    - Restores registers.
//!    - Returns to the original code (via the saved RIP/PC).
//!
//! This ensures full safety: the actual preemption logic (checking flags, yielding) runs
//! as normal thread code, not in an interrupt context.

#[cfg(test)]
mod tests;

use crate::generator;
use crate::runtime::worker::rust_preemption_helper;
use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};

thread_local! {
    static CURRENT_GENERATOR_SCOPE: Cell<*mut ()> = Cell::new(ptr::null_mut());
    static IN_PREEMPTION_HANDLER: Cell<bool> = const { Cell::new(false) };
}

pub fn get_generator_scope() -> *mut () {
    CURRENT_GENERATOR_SCOPE.with(|cell| cell.get())
}

pub fn set_generator_scope(ptr: *mut ()) {
    CURRENT_GENERATOR_SCOPE.with(|cell| cell.set(ptr));
}

pub fn clear_generator_scope() {
    CURRENT_GENERATOR_SCOPE.with(|cell| cell.set(core::ptr::null_mut()));
}

/// Guard that blocks the preemption signal (Unix) while it is in scope.
#[cfg(unix)]
pub struct PreemptionSignalGuard {
    old_mask: libc::sigset_t,
    active: bool,
}

#[cfg(unix)]
impl PreemptionSignalGuard {
    pub fn new() -> Self {
        unsafe {
            let mut set = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
            libc::sigemptyset(set.as_mut_ptr());
            libc::sigaddset(set.as_mut_ptr(), libc::SIGVTALRM);

            let mut old = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
            let rc = libc::pthread_sigmask(libc::SIG_BLOCK, set.as_ptr(), old.as_mut_ptr());
            if rc == 0 {
                Self {
                    old_mask: old.assume_init(),
                    active: true,
                }
            } else {
                Self {
                    old_mask: std::mem::MaybeUninit::zeroed().assume_init(),
                    active: false,
                }
            }
        }
    }
}

#[cfg(unix)]
impl Drop for PreemptionSignalGuard {
    fn drop(&mut self) {
        if self.active {
            unsafe {
                libc::pthread_sigmask(libc::SIG_SETMASK, &self.old_mask, core::ptr::null_mut());
            }
        }
    }
}

#[cfg(not(unix))]
pub struct PreemptionSignalGuard;

#[cfg(not(unix))]
impl PreemptionSignalGuard {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(not(unix))]
impl Drop for PreemptionSignalGuard {
    fn drop(&mut self) {}
}

/// Blocks the preemption signal for the current scope, returning a guard that
/// restores the previous mask when dropped.
pub fn block_preemption_signal() -> PreemptionSignalGuard {
    PreemptionSignalGuard::new()
}

/// Reasons the worker generator yields control back to the scheduler/trampoline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GeneratorYieldReason {
    Shutdown = 0,
    /// Cooperative yield point inside the worker loop (scheduler driven).
    Cooperative = 1,
    /// Non-cooperative preemption triggered via signal/APC.
    Preempted = 2,
    Panic = 3,
}

impl GeneratorYieldReason {
    #[inline]
    pub const fn as_usize(self) -> usize {
        self as usize
    }

    #[inline]
    pub const fn from_usize(value: usize) -> Option<Self> {
        match value {
            0 => Some(Self::Shutdown),
            1 => Some(Self::Cooperative),
            2 => Some(Self::Preempted),
            3 => Some(Self::Panic),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum PreemptionError {
    SignalSetupFailed,
    ThreadSetupFailed,
    InterruptFailed,
    UnsupportedPlatform,
}

impl std::fmt::Display for PreemptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl std::error::Error for PreemptionError {}

// ============================================================================
// Platform Implementations
// ============================================================================

#[cfg(not(any(
    target_arch = "x86_64",
    target_arch = "aarch64",
    target_arch = "riscv64",
    target_arch = "loongarch64"
)))]
mod unsupported {
    use super::*;
    // Stub implementation for unsupported platforms
    pub struct WorkerThreadHandle {}
    impl WorkerThreadHandle {
        pub fn interrupt(&self) -> Result<(), PreemptionError> {
            Err(PreemptionError::UnsupportedPlatform)
        }
    }
    pub(crate) fn init_worker_preemption() -> Result<PreemptionHandle, PreemptionError> {
        Ok(PreemptionHandle {})
    }
    pub(crate) struct PreemptionHandle {}
}

// ----------------------------------------------------------------------------
// x86_64 Implementation
// ----------------------------------------------------------------------------
#[cfg(target_arch = "x86_64")]
mod impl_x64 {
    use super::*;

    // x86_64 Trampoline (Unix)
    #[cfg(unix)]
    use std::arch::global_asm;

    #[cfg(unix)]
    macro_rules! define_x64_unix_trampoline {
        ($section_directive:literal) => {
            global_asm!(
                $section_directive,
                ".global preemption_trampoline",
                "preemption_trampoline:",
                // Context: RSP points to Saved RIP.
                // SysV ABI Volatiles: RAX, RCX, RDX, RSI, RDI, R8-R11. XMM0-XMM15.
                "pushfq",
                "push rax",
                "push rcx",
                "push rdx",
                "push rsi",
                "push rdi",
                "push r8",
                "push r9",
                "push r10",
                "push r11",
                // Save callee-saved registers (required by x86_64 System V ABI)
                "push rbx",
                "push rbp",
                "push r12",
                "push r13",
                "push r14",
                "push r15",
                // Save XMM0-XMM15 (16 regs * 16 bytes = 256 bytes)
                "sub rsp, 256",
                "movdqu [rsp + 240], xmm0",
                "movdqu [rsp + 224], xmm1",
                "movdqu [rsp + 208], xmm2",
                "movdqu [rsp + 192], xmm3",
                "movdqu [rsp + 176], xmm4",
                "movdqu [rsp + 160], xmm5",
                "movdqu [rsp + 144], xmm6",
                "movdqu [rsp + 128], xmm7",
                "movdqu [rsp + 112], xmm8",
                "movdqu [rsp + 96], xmm9",
                "movdqu [rsp + 80], xmm10",
                "movdqu [rsp + 64], xmm11",
                "movdqu [rsp + 48], xmm12",
                "movdqu [rsp + 32], xmm13",
                "movdqu [rsp + 16], xmm14",
                "movdqu [rsp], xmm15",
                // Call helper (stack should already be aligned by signal handler)
                "call rust_preemption_helper",
                // Restore XMMs
                "movdqu xmm15, [rsp]",
                "movdqu xmm14, [rsp + 16]",
                "movdqu xmm13, [rsp + 32]",
                "movdqu xmm12, [rsp + 48]",
                "movdqu xmm11, [rsp + 64]",
                "movdqu xmm10, [rsp + 80]",
                "movdqu xmm9, [rsp + 96]",
                "movdqu xmm8, [rsp + 112]",
                "movdqu xmm7, [rsp + 128]",
                "movdqu xmm6, [rsp + 144]",
                "movdqu xmm5, [rsp + 160]",
                "movdqu xmm4, [rsp + 176]",
                "movdqu xmm3, [rsp + 192]",
                "movdqu xmm2, [rsp + 208]",
                "movdqu xmm1, [rsp + 224]",
                "movdqu xmm0, [rsp + 240]",
                "add rsp, 256",
                // Restore callee-saved registers (in reverse order)
                "pop r15",
                "pop r14",
                "pop r13",
                "pop r12",
                "pop rbp",
                "pop rbx",
                // Restore caller-saved registers
                "pop r11",
                "pop r10",
                "pop r9",
                "pop r8",
                "pop rdi",
                "pop rsi",
                "pop rdx",
                "pop rcx",
                "pop rax",
                "popfq",
                // At this point, all registers restored to pre-signal values
                // Stack layout: [RSP+0] = original_rsp, [RSP+8] = original_rip
                // We need to switch to original_rsp and jump to original_rip
                // WITHOUT clobbering any registers (including RAX and RBX).

                // 1. Load original_rsp into RAX, save RAX to stack
                "xchg rax, [rsp]", // RAX = original_rsp, [RSP] = saved_rax
                // 2. Load original_rip into RBX, save RBX to stack
                "xchg rbx, [rsp + 8]", // RBX = original_rip, [RSP+8] = saved_rbx
                // 3. Build return frame on ORIGINAL stack (using RAX as base)
                // [original_rsp - 8] = original_rip (Return Address)
                "mov [rax - 8], rbx",
                // 4. Move saved_rax to original stack
                "mov rbx, [rsp]",      // RBX = saved_rax
                "mov [rax - 16], rbx", // [original_rsp - 16] = saved_rax
                // 5. Move saved_rbx to original stack
                "mov rbx, [rsp + 8]",  // RBX = saved_rbx
                "mov [rax - 24], rbx", // [original_rsp - 24] = saved_rbx
                // 6. Switch to original stack
                "lea rsp, [rax - 24]", // RSP = original_rsp - 24
                // 7. Restore registers and return
                "pop rbx", // Restore RBX
                "pop rax", // Restore RAX
                "ret"      // Pop original_rip and jump, RSP becomes original_rsp
            );
        };
    }

    #[cfg(all(unix, target_os = "macos"))]
    define_x64_unix_trampoline!(".section __TEXT,__text");

    #[cfg(all(unix, not(target_os = "macos")))]
    define_x64_unix_trampoline!(".section .text");

    // x86_64 Trampoline (Windows)
    #[cfg(windows)]
    use std::arch::global_asm;

    #[cfg(windows)]
    global_asm!(
        ".section .text",
        ".global preemption_trampoline",
        "preemption_trampoline:",
        // Save RFLAGS. RSP is now 16-byte aligned (assuming we entered with [RIP] on stack).
        "pushfq",
        // Save All GPRs (Volatile & Non-Volatile)
        "push rax",
        "push rcx",
        "push rdx",
        "push r8",
        "push r9",
        "push r10",
        "push r11",
        "push rbx",
        "push rbp",
        "push rdi",
        "push rsi",
        "push r12",
        "push r13",
        "push r14",
        "push r15",
        // Use RBP as frame pointer to handle dynamic stack alignment
        // RBP is callee-saved, so it is preserved by the helper call (and swap_registers)
        "mov rbp, rsp",
        // Align stack to 16 bytes
        "and rsp, -16",
        // Save All XMMs (0-15)
        "sub rsp, 256",
        "movdqu [rsp + 240], xmm0",
        "movdqu [rsp + 224], xmm1",
        "movdqu [rsp + 208], xmm2",
        "movdqu [rsp + 192], xmm3",
        "movdqu [rsp + 176], xmm4",
        "movdqu [rsp + 160], xmm5",
        "movdqu [rsp + 144], xmm6",
        "movdqu [rsp + 128], xmm7",
        "movdqu [rsp + 112], xmm8",
        "movdqu [rsp + 96], xmm9",
        "movdqu [rsp + 80], xmm10",
        "movdqu [rsp + 64], xmm11",
        "movdqu [rsp + 48], xmm12",
        "movdqu [rsp + 32], xmm13",
        "movdqu [rsp + 16], xmm14",
        "movdqu [rsp], xmm15",
        // Allocate shadow space (32 bytes) and align stack
        // (Stack is already 16-byte aligned here due to pushfq + 15 pushes + 256 sub)
        "sub rsp, 32",
        "call rust_preemption_helper",
        "add rsp, 32",
        // Restore XMMs
        "movdqu xmm15, [rsp]",
        "movdqu xmm14, [rsp + 16]",
        "movdqu xmm13, [rsp + 32]",
        "movdqu xmm12, [rsp + 48]",
        "movdqu xmm11, [rsp + 64]",
        "movdqu xmm10, [rsp + 80]",
        "movdqu xmm9, [rsp + 96]",
        "movdqu xmm8, [rsp + 112]",
        "movdqu xmm7, [rsp + 128]",
        "movdqu xmm6, [rsp + 144]",
        "movdqu xmm5, [rsp + 160]",
        "movdqu xmm4, [rsp + 176]",
        "movdqu xmm3, [rsp + 192]",
        "movdqu xmm2, [rsp + 208]",
        "movdqu xmm1, [rsp + 224]",
        "movdqu xmm0, [rsp + 240]",
        "add rsp, 256",
        // Restore RSP from RBP (undoes alignment and XMM alloc)
        "mov rsp, rbp",
        // Restore GPRs
        "pop r15",
        "pop r14",
        "pop r13",
        "pop r12",
        "pop rsi",
        "pop rdi",
        "pop rbp",
        "pop rbx",
        "pop r11",
        "pop r10",
        "pop r9",
        "pop r8",
        "pop rdx",
        "pop rcx",
        "pop rax",
        "popfq",
        "ret"
    );

    #[cfg(unix)]
    pub(crate) use unix_impl::init_worker_preemption;
    #[cfg(unix)]
    pub use unix_impl::{PreemptionHandle, WorkerThreadHandle};

    #[cfg(unix)]
    mod unix_impl {
        use super::super::*;
        use std::mem::MaybeUninit;

        unsafe extern "C" {
            fn preemption_trampoline();
        }

        pub struct WorkerThreadHandle {
            pthread: libc::pthread_t,
        }
        unsafe impl Send for WorkerThreadHandle {}
        unsafe impl Sync for WorkerThreadHandle {}

        impl WorkerThreadHandle {
            pub fn current() -> Result<Self, PreemptionError> {
                Ok(Self {
                    pthread: unsafe { libc::pthread_self() },
                })
            }
            pub fn interrupt(&self) -> Result<(), PreemptionError> {
                unsafe {
                    if libc::pthread_kill(self.pthread, libc::SIGVTALRM) == 0 {
                        Ok(())
                    } else {
                        Err(PreemptionError::InterruptFailed)
                    }
                }
            }
        }

        pub struct PreemptionHandle {
            old_handler: libc::sigaction,
        }
        impl Drop for PreemptionHandle {
            fn drop(&mut self) {
                unsafe {
                    libc::sigaction(libc::SIGVTALRM, &self.old_handler, ptr::null_mut());
                }
            }
        }
        pub(crate) fn init_worker_preemption() -> Result<PreemptionHandle, PreemptionError> {
            init_preemption()
        }
        fn init_preemption() -> Result<PreemptionHandle, PreemptionError> {
            unsafe {
                let mut sa = MaybeUninit::<libc::sigaction>::uninit();
                let sa_ptr = sa.as_mut_ptr();
                (*sa_ptr).sa_sigaction = sigalrm_handler as *const () as usize;
                libc::sigemptyset(&mut (*sa_ptr).sa_mask);
                // Block SIGVTALRM during handler execution to prevent reentrancy
                libc::sigaddset(&mut (*sa_ptr).sa_mask, libc::SIGVTALRM);
                (*sa_ptr).sa_flags = libc::SA_RESTART | libc::SA_SIGINFO;
                let sa = sa.assume_init();

                let mut old_sa: libc::sigaction = MaybeUninit::zeroed().assume_init();
                if libc::sigaction(libc::SIGVTALRM, &sa, &mut old_sa) != 0 {
                    return Err(PreemptionError::SignalSetupFailed);
                }
                Ok(PreemptionHandle {
                    old_handler: old_sa,
                })
            }
        }

        extern "C" fn sigalrm_handler(
            _signum: libc::c_int,
            _info: *mut libc::siginfo_t,
            context: *mut libc::c_void,
        ) {
            unsafe {
                let ctx = context as *mut libc::ucontext_t;
                let mcontext = &mut (*ctx).uc_mcontext;

                #[cfg(target_os = "linux")]
                let (rip_ptr, rsp_ptr) = (
                    &mut mcontext.gregs[libc::REG_RIP as usize] as *mut _ as *mut u64,
                    &mut mcontext.gregs[libc::REG_RSP as usize] as *mut _ as *mut u64,
                );
                #[cfg(target_os = "macos")]
                let (rip_ptr, rsp_ptr) = {
                    let mctx = *mcontext;
                    (
                        &mut (*mctx).__ss.__rip as *mut u64,
                        &mut (*mctx).__ss.__rsp as *mut u64,
                    )
                };
                #[cfg(target_os = "freebsd")]
                let (rip_ptr, rsp_ptr) = (
                    &mut mcontext.mc_rip as *mut _ as *mut u64,
                    &mut mcontext.mc_rsp as *mut _ as *mut u64,
                );
                #[cfg(target_os = "openbsd")]
                let (rip_ptr, rsp_ptr) = (
                    &mut mcontext.sc_rip as *mut _ as *mut u64,
                    &mut mcontext.sc_rsp as *mut _ as *mut u64,
                );
                #[cfg(target_os = "netbsd")]
                let (rip_ptr, rsp_ptr) = (
                    &mut mcontext.__gregs[libc::_REG_RIP as usize] as *mut _ as *mut u64,
                    &mut mcontext.__gregs[libc::_REG_RSP as usize] as *mut _ as *mut u64,
                );

                let original_rip = *rip_ptr;
                let original_rsp = *rsp_ptr;
                let mut sp = original_rsp;
                // x86_64 System V ABI: Reserve red zone and ensure proper alignment
                sp -= 128; // Red Zone (System V ABI)
                // Align to 16-byte boundary, then subtract 16 for two values we'll push
                sp = (sp & !15) - 16;
                // Push original RSP and original RIP onto the new stack
                *(sp as *mut u64) = original_rsp; // Original RSP
                *((sp + 8) as *mut u64) = original_rip; // Original RIP
                *rsp_ptr = sp;
                *rip_ptr = preemption_trampoline as *const () as u64;

                // Memory barrier: Ensure all context modifications are visible before
                // returning from the handler. This prevents reordering that could cause
                // the trampoline to execute with stale register values.
                std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::Release);
            }
        }
    }

    #[cfg(windows)]
    pub(crate) use windows_impl::init_worker_preemption;
    #[cfg(windows)]
    pub use windows_impl::{PreemptionHandle, WorkerThreadHandle};

    #[cfg(windows)]
    mod windows_impl {
        use super::super::*;
        use windows_sys::Win32::Foundation::{CloseHandle, DuplicateHandle, HANDLE};
        use windows_sys::Win32::System::Diagnostics::Debug::{
            CONTEXT, GetThreadContext, SetThreadContext, WriteProcessMemory,
        };
        use windows_sys::Win32::System::Threading::{
            GetCurrentProcess, GetCurrentThread, ResumeThread, SuspendThread, THREAD_GET_CONTEXT,
            THREAD_QUERY_INFORMATION, THREAD_SET_CONTEXT, THREAD_SUSPEND_RESUME,
        };

        // Windows CONTEXT flags (architecture-independent values)
        #[cfg(target_arch = "x86_64")]
        const CONTEXT_CONTROL: u32 = 0x00100001;
        #[cfg(target_arch = "x86_64")]
        const CONTEXT_INTEGER: u32 = 0x00100002;

        #[cfg(target_arch = "aarch64")]
        const CONTEXT_CONTROL: u32 = 0x00400001;
        #[cfg(target_arch = "aarch64")]
        const CONTEXT_INTEGER: u32 = 0x00400002;

        unsafe extern "C" {
            fn preemption_trampoline();
        }

        pub struct WorkerThreadHandle {
            thread_handle: HANDLE,
            preemption_flag: *const AtomicBool,
        }
        unsafe impl Send for WorkerThreadHandle {}
        unsafe impl Sync for WorkerThreadHandle {}

        impl WorkerThreadHandle {
            pub fn current(preemption_flag: &AtomicBool) -> Result<Self, PreemptionError> {
                unsafe {
                    let mut real_handle: HANDLE = std::ptr::null_mut();
                    let pseudo_handle = GetCurrentThread();
                    let current_process = GetCurrentProcess();
                    // Request specific access rights explicitly
                    let access = THREAD_SUSPEND_RESUME
                        | THREAD_GET_CONTEXT
                        | THREAD_SET_CONTEXT
                        | THREAD_QUERY_INFORMATION;
                    if DuplicateHandle(
                        current_process,
                        pseudo_handle,
                        current_process,
                        &mut real_handle,
                        access,
                        0,
                        0,
                    ) == 0
                    {
                        tracing::trace!(
                            "DuplicateHandle failed: {}",
                            std::io::Error::last_os_error()
                        );
                        return Err(PreemptionError::ThreadSetupFailed);
                    }
                    tracing::trace!("DuplicateHandle succeeded, handle: {:?}", real_handle);
                    Ok(Self {
                        thread_handle: real_handle,
                        preemption_flag: preemption_flag as *const AtomicBool,
                    })
                }
            }

            pub fn interrupt(&self) -> Result<(), PreemptionError> {
                unsafe {
                    if SuspendThread(self.thread_handle) == u32::MAX {
                        tracing::trace!(
                            "SuspendThread failed: {}",
                            std::io::Error::last_os_error()
                        );
                        return Err(PreemptionError::InterruptFailed);
                    }
                    (*self.preemption_flag).store(true, Ordering::Release);

                    // Ensure 16-byte alignment for CONTEXT
                    #[repr(align(16))]
                    struct AlignedContext(CONTEXT);
                    let mut aligned = std::mem::zeroed::<AlignedContext>();
                    let context = &mut aligned.0;

                    context.ContextFlags = CONTEXT_CONTROL | CONTEXT_INTEGER;

                    // Check alignment
                    let addr = context as *const _ as usize;
                    if addr % 16 != 0 {
                        tracing::trace!("CONTEXT not aligned: 0x{:x}", addr);
                    }

                    if GetThreadContext(self.thread_handle, context) == 0 {
                        tracing::trace!(
                            "GetThreadContext failed: {} (addr: 0x{:x})",
                            std::io::Error::last_os_error(),
                            addr
                        );
                        ResumeThread(self.thread_handle);
                        return Err(PreemptionError::InterruptFailed);
                    }
                    let original_rip = context.Rip;
                    let mut rsp = context.Rsp;
                    rsp -= 8;

                    // Use WriteProcessMemory to safely write to the target stack
                    let stack_ptr = rsp as *mut std::ffi::c_void;
                    let mut written: usize = 0;
                    let process = GetCurrentProcess();

                    if WriteProcessMemory(
                        process,
                        stack_ptr,
                        &original_rip as *const _ as *const std::ffi::c_void,
                        8,
                        &mut written,
                    ) == 0
                    {
                        tracing::trace!(
                            "WriteProcessMemory failed: {}",
                            std::io::Error::last_os_error()
                        );
                        ResumeThread(self.thread_handle);
                        return Err(PreemptionError::InterruptFailed);
                    }

                    context.Rsp = rsp;
                    context.Rip = preemption_trampoline as *const () as u64;
                    if SetThreadContext(self.thread_handle, context) == 0 {
                        tracing::trace!(
                            "SetThreadContext failed: {}",
                            std::io::Error::last_os_error()
                        );
                        ResumeThread(self.thread_handle);
                        return Err(PreemptionError::InterruptFailed);
                    }
                    ResumeThread(self.thread_handle);
                    Ok(())
                }
            }
        }
        impl Drop for WorkerThreadHandle {
            fn drop(&mut self) {
                unsafe {
                    CloseHandle(self.thread_handle);
                }
            }
        }
        pub struct PreemptionHandle {
            _marker: std::marker::PhantomData<()>,
        }
        pub(crate) fn init_preemption() -> Result<PreemptionHandle, PreemptionError> {
            Ok(PreemptionHandle {
                _marker: std::marker::PhantomData,
            })
        }
        pub(crate) fn init_worker_preemption() -> Result<PreemptionHandle, PreemptionError> {
            init_preemption()
        }
    }
}

// ----------------------------------------------------------------------------
// AArch64 Implementation
// ----------------------------------------------------------------------------
#[cfg(target_arch = "aarch64")]
mod impl_aarch64 {
    use super::*;
    use std::arch::global_asm;

    // AArch64 Trampoline
    // We use x18 as a temporary (platform reserve) or stick to caller-saved.
    // AAPCS64 Volatiles: x0-x18, x30 (LR). SIMD v0-v7, v16-v31.
    // We must save all of them.
    // Stack must be 16-byte aligned.
    #[cfg(not(windows))]
    macro_rules! define_aarch64_trampoline {
        ($section_directive:literal) => {
            global_asm!(
                $section_directive,
                ".global preemption_trampoline",
                "preemption_trampoline:",
                // Save all GPRs (x0-x30) to preserve context during async preemption
                // x29=FP, x30=LR. x19-x28 are callee-saved. x0-x18 are volatile.
                "stp x29, x30, [sp, #-16]!",
                "stp x27, x28, [sp, #-16]!",
                "stp x25, x26, [sp, #-16]!",
                "stp x23, x24, [sp, #-16]!",
                "stp x21, x22, [sp, #-16]!",
                "stp x19, x20, [sp, #-16]!",
                "stp x17, x18, [sp, #-16]!",
                "stp x15, x16, [sp, #-16]!",
                "stp x13, x14, [sp, #-16]!",
                "stp x11, x12, [sp, #-16]!",
                "stp x9, x10, [sp, #-16]!",
                "stp x7, x8, [sp, #-16]!",
                "stp x5, x6, [sp, #-16]!",
                "stp x3, x4, [sp, #-16]!",
                "stp x1, x2, [sp, #-16]!",
                // Save x0 and padding (xzr) to maintain 16-byte alignment
                "stp x0, xzr, [sp, #-16]!",
                // Save all FP/SIMD registers (v0-v31)
                "stp q0, q1, [sp, #-32]!",
                "stp q2, q3, [sp, #-32]!",
                "stp q4, q5, [sp, #-32]!",
                "stp q6, q7, [sp, #-32]!",
                "stp q8, q9, [sp, #-32]!",
                "stp q10, q11, [sp, #-32]!",
                "stp q12, q13, [sp, #-32]!",
                "stp q14, q15, [sp, #-32]!",
                "stp q16, q17, [sp, #-32]!",
                "stp q18, q19, [sp, #-32]!",
                "stp q20, q21, [sp, #-32]!",
                "stp q22, q23, [sp, #-32]!",
                "stp q24, q25, [sp, #-32]!",
                "stp q26, q27, [sp, #-32]!",
                "stp q28, q29, [sp, #-32]!",
                "stp q30, q31, [sp, #-32]!",
                // Call helper
                "bl rust_preemption_helper",
                // Restore FP/SIMD
                "ldp q30, q31, [sp], #32",
                "ldp q28, q29, [sp], #32",
                "ldp q26, q27, [sp], #32",
                "ldp q24, q25, [sp], #32",
                "ldp q22, q23, [sp], #32",
                "ldp q20, q21, [sp], #32",
                "ldp q18, q19, [sp], #32",
                "ldp q16, q17, [sp], #32",
                "ldp q14, q15, [sp], #32",
                "ldp q12, q13, [sp], #32",
                "ldp q10, q11, [sp], #32",
                "ldp q8, q9, [sp], #32",
                "ldp q6, q7, [sp], #32",
                "ldp q4, q5, [sp], #32",
                "ldp q2, q3, [sp], #32",
                "ldp q0, q1, [sp], #32",
                // Restore GPR
                "ldp x0, xzr, [sp], #16",
                "ldp x1, x2, [sp], #16",
                "ldp x3, x4, [sp], #16",
                "ldp x5, x6, [sp], #16",
                "ldp x7, x8, [sp], #16",
                "ldp x9, x10, [sp], #16",
                "ldp x11, x12, [sp], #16",
                "ldp x13, x14, [sp], #16",
                "ldp x15, x16, [sp], #16",
                "ldp x17, x18, [sp], #16",
                "ldp x19, x20, [sp], #16",
                "ldp x21, x22, [sp], #16",
                "ldp x23, x24, [sp], #16",
                "ldp x25, x26, [sp], #16",
                "ldp x27, x28, [sp], #16",
                "ldp x29, x30, [sp], #16",
                // Return
                "ret"
            );
        };
    }

    #[cfg(all(not(windows), target_os = "macos"))]
    define_aarch64_trampoline!(".section __TEXT,__text");

    #[cfg(all(not(windows), not(target_os = "macos")))]
    define_aarch64_trampoline!(".section .text");

    #[cfg(windows)]
    use std::arch::global_asm;

    #[cfg(windows)]
    global_asm!(
        ".section .text",
        ".global preemption_trampoline",
        "preemption_trampoline:",
        // Save all GPRs (x0-x30)
        "stp x29, x30, [sp, #-16]!",
        "stp x27, x28, [sp, #-16]!",
        "stp x25, x26, [sp, #-16]!",
        "stp x23, x24, [sp, #-16]!",
        "stp x21, x22, [sp, #-16]!",
        "stp x19, x20, [sp, #-16]!",
        "stp x17, x18, [sp, #-16]!",
        "stp x15, x16, [sp, #-16]!",
        "stp x13, x14, [sp, #-16]!",
        "stp x11, x12, [sp, #-16]!",
        "stp x9, x10, [sp, #-16]!",
        "stp x7, x8, [sp, #-16]!",
        "stp x5, x6, [sp, #-16]!",
        "stp x3, x4, [sp, #-16]!",
        "stp x1, x2, [sp, #-16]!",
        "stp x0, xzr, [sp, #-16]!",
        // Save all FP/SIMD (v0-v31)
        "stp q0, q1, [sp, #-32]!",
        "stp q2, q3, [sp, #-32]!",
        "stp q4, q5, [sp, #-32]!",
        "stp q6, q7, [sp, #-32]!",
        "stp q8, q9, [sp, #-32]!",
        "stp q10, q11, [sp, #-32]!",
        "stp q12, q13, [sp, #-32]!",
        "stp q14, q15, [sp, #-32]!",
        "stp q16, q17, [sp, #-32]!",
        "stp q18, q19, [sp, #-32]!",
        "stp q20, q21, [sp, #-32]!",
        "stp q22, q23, [sp, #-32]!",
        "stp q24, q25, [sp, #-32]!",
        "stp q26, q27, [sp, #-32]!",
        "stp q28, q29, [sp, #-32]!",
        "stp q30, q31, [sp, #-32]!",
        // Call helper
        "sub sp, sp, #32", // shadow space for Windows/ARM64
        "bl rust_preemption_helper",
        "add sp, sp, #32",
        // Restore FP/SIMD
        "ldp q30, q31, [sp], #32",
        "ldp q28, q29, [sp], #32",
        "ldp q26, q27, [sp], #32",
        "ldp q24, q25, [sp], #32",
        "ldp q22, q23, [sp], #32",
        "ldp q20, q21, [sp], #32",
        "ldp q18, q19, [sp], #32",
        "ldp q16, q17, [sp], #32",
        "ldp q14, q15, [sp], #32",
        "ldp q12, q13, [sp], #32",
        "ldp q10, q11, [sp], #32",
        "ldp q8, q9, [sp], #32",
        "ldp q6, q7, [sp], #32",
        "ldp q4, q5, [sp], #32",
        "ldp q2, q3, [sp], #32",
        "ldp q0, q1, [sp], #32",
        // Restore GPR
        "ldp x0, xzr, [sp], #16",
        "ldp x1, x2, [sp], #16",
        "ldp x3, x4, [sp], #16",
        "ldp x5, x6, [sp], #16",
        "ldp x7, x8, [sp], #16",
        "ldp x9, x10, [sp], #16",
        "ldp x11, x12, [sp], #16",
        "ldp x13, x14, [sp], #16",
        "ldp x15, x16, [sp], #16",
        "ldp x17, x18, [sp], #16",
        "ldp x19, x20, [sp], #16",
        "ldp x21, x22, [sp], #16",
        "ldp x23, x24, [sp], #16",
        "ldp x25, x26, [sp], #16",
        "ldp x27, x28, [sp], #16",
        "ldp x29, x30, [sp], #16",
        "ret"
    );

    #[cfg(unix)]
    pub(crate) use unix_impl::init_worker_preemption;
    #[cfg(unix)]
    pub use unix_impl::{PreemptionHandle, WorkerThreadHandle};

    #[cfg(unix)]
    mod unix_impl {
        use super::super::*;
        use std::mem::MaybeUninit;

        unsafe extern "C" {
            fn preemption_trampoline();
        }

        pub struct WorkerThreadHandle {
            pthread: libc::pthread_t,
        }
        unsafe impl Send for WorkerThreadHandle {}
        unsafe impl Sync for WorkerThreadHandle {}

        impl WorkerThreadHandle {
            pub fn current() -> Result<Self, PreemptionError> {
                Ok(Self {
                    pthread: unsafe { libc::pthread_self() },
                })
            }
            pub fn interrupt(&self) -> Result<(), PreemptionError> {
                unsafe {
                    if libc::pthread_kill(self.pthread, libc::SIGVTALRM) == 0 {
                        Ok(())
                    } else {
                        Err(PreemptionError::InterruptFailed)
                    }
                }
            }
        }

        pub struct PreemptionHandle {
            old_handler: libc::sigaction,
        }
        impl Drop for PreemptionHandle {
            fn drop(&mut self) {
                unsafe {
                    libc::sigaction(libc::SIGVTALRM, &self.old_handler, ptr::null_mut());
                }
            }
        }
        pub(crate) fn init_worker_preemption() -> Result<PreemptionHandle, PreemptionError> {
            init_preemption()
        }
        fn init_preemption() -> Result<PreemptionHandle, PreemptionError> {
            unsafe {
                let mut sa: libc::sigaction = MaybeUninit::zeroed().assume_init();
                sa.sa_sigaction = sigalrm_handler as usize;
                libc::sigemptyset(&mut sa.sa_mask);
                // Block SIGVTALRM during handler execution to prevent reentrancy
                libc::sigaddset(&mut sa.sa_mask, libc::SIGVTALRM);
                sa.sa_flags = libc::SA_RESTART | libc::SA_SIGINFO;
                let mut old_sa: libc::sigaction = MaybeUninit::zeroed().assume_init();
                if libc::sigaction(libc::SIGVTALRM, &sa, &mut old_sa) != 0 {
                    return Err(PreemptionError::SignalSetupFailed);
                }
                Ok(PreemptionHandle {
                    old_handler: old_sa,
                })
            }
        }

        extern "C" fn sigalrm_handler(
            _signum: libc::c_int,
            _info: *mut libc::siginfo_t,
            context: *mut libc::c_void,
        ) {
            unsafe {
                let ctx = context as *mut libc::ucontext_t;
                let mcontext = &mut (*ctx).uc_mcontext;

                #[cfg(target_os = "linux")]
                let (pc_ptr, sp_ptr, lr_ptr) = (
                    &mut mcontext.pc as *mut u64,
                    &mut mcontext.sp as *mut u64,
                    &mut mcontext.regs[30] as *mut u64, // x30 is LR
                );

                #[cfg(target_os = "macos")]
                let (pc_ptr, sp_ptr, lr_ptr) = {
                    let mctx = *mcontext; // Deref &mut *mut -> *mut
                    (
                        &mut (*mctx).__ss.__pc as *mut u64,
                        &mut (*mctx).__ss.__sp as *mut u64,
                        &mut (*mctx).__ss.__lr as *mut u64,
                    )
                };

                #[cfg(target_os = "freebsd")]
                let (pc_ptr, sp_ptr, lr_ptr) = (
                    &mut mcontext.mc_gpregs.gp_elr as *mut _ as *mut u64,
                    &mut mcontext.mc_gpregs.gp_sp as *mut _ as *mut u64,
                    &mut mcontext.mc_gpregs.gp_lr as *mut _ as *mut u64,
                );

                #[cfg(target_os = "openbsd")]
                let (pc_ptr, sp_ptr, lr_ptr) = (
                    &mut mcontext.sc_elr as *mut _ as *mut u64,
                    &mut mcontext.sc_sp as *mut _ as *mut u64,
                    &mut mcontext.sc_lr as *mut _ as *mut u64,
                );

                #[cfg(target_os = "netbsd")]
                let (pc_ptr, sp_ptr, lr_ptr) = (
                    &mut mcontext.__gregs[libc::_REG_PC] as *mut _ as *mut u64,
                    &mut mcontext.__gregs[libc::_REG_SP] as *mut _ as *mut u64,
                    &mut mcontext.__gregs[libc::_REG_LR] as *mut _ as *mut u64,
                );

                let original_pc = *pc_ptr;

                // On AArch64, "BL" writes PC+4 to LR.
                // We are simulating a BL to the trampoline.
                // We want the trampoline to return to original_pc.
                // The trampoline ends with "ret", which jumps to LR (x30).
                // So we must set LR = original_pc.

                *lr_ptr = original_pc;

                // Set PC to trampoline
                *pc_ptr = preemption_trampoline as *const () as u64;

                // Stack alignment: AArch64 AAPCS64 requires 16-byte alignment at public
                // interfaces. Since we're not modifying SP (return address is in LR, not
                // on stack), the existing alignment is preserved. The trampoline uses
                // "stp" instructions which maintain 16-byte alignment.

                // CRITICAL: Keep SIGVTALRM blocked in the resumed context to prevent
                // reentrant signals during trampoline execution.
                libc::sigaddset(&mut (*ctx).uc_sigmask, libc::SIGVTALRM);

                // Memory barrier: Ensure all context modifications are visible before
                // returning from the handler.
                std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::Release);
            }
        }
    }

    #[cfg(windows)]
    pub(crate) use windows_impl::init_worker_preemption;
    #[cfg(windows)]
    pub use windows_impl::{PreemptionHandle, WorkerThreadHandle};

    #[cfg(windows)]
    mod windows_impl {
        use super::super::*;
        use windows_sys::Win32::Foundation::{CloseHandle, DuplicateHandle, HANDLE};
        use windows_sys::Win32::System::Diagnostics::Debug::{
            CONTEXT, GetThreadContext, SetThreadContext,
        };
        use windows_sys::Win32::System::Memory::WriteProcessMemory;
        use windows_sys::Win32::System::Threading::{
            GetCurrentProcess, GetCurrentThread, ResumeThread, SuspendThread, THREAD_GET_CONTEXT,
            THREAD_QUERY_INFORMATION, THREAD_SET_CONTEXT, THREAD_SUSPEND_RESUME,
        };

        // Windows CONTEXT flags (architecture-independent values)
        #[cfg(target_arch = "x86_64")]
        const CONTEXT_CONTROL: u32 = 0x00100001;
        #[cfg(target_arch = "x86_64")]
        const CONTEXT_INTEGER: u32 = 0x00100002;

        #[cfg(target_arch = "aarch64")]
        const CONTEXT_CONTROL: u32 = 0x00400001;
        #[cfg(target_arch = "aarch64")]
        const CONTEXT_INTEGER: u32 = 0x00400002;

        unsafe extern "C" {
            fn preemption_trampoline();
        }

        pub struct WorkerThreadHandle {
            thread_handle: HANDLE,
            preemption_flag: *const AtomicBool,
        }
        unsafe impl Send for WorkerThreadHandle {}
        unsafe impl Sync for WorkerThreadHandle {}

        impl WorkerThreadHandle {
            pub fn current(preemption_flag: &AtomicBool) -> Result<Self, PreemptionError> {
                unsafe {
                    let mut real_handle: HANDLE = std::ptr::null_mut();
                    let pseudo_handle = GetCurrentThread();
                    let current_process = GetCurrentProcess();
                    // Request specific access rights explicitly
                    let access = THREAD_SUSPEND_RESUME
                        | THREAD_GET_CONTEXT
                        | THREAD_SET_CONTEXT
                        | THREAD_QUERY_INFORMATION;
                    if DuplicateHandle(
                        current_process,
                        pseudo_handle,
                        current_process,
                        &mut real_handle,
                        access,
                        0,
                        0,
                    ) == 0
                    {
                        tracing::trace!(
                            "DuplicateHandle failed: {}",
                            std::io::Error::last_os_error()
                        );
                        return Err(PreemptionError::ThreadSetupFailed);
                    }
                    tracing::trace!("DuplicateHandle succeeded, handle: {:?}", real_handle);
                    Ok(Self {
                        thread_handle: real_handle,
                        preemption_flag: preemption_flag as *const AtomicBool,
                    })
                }
            }

            pub fn interrupt(&self) -> Result<(), PreemptionError> {
                // Note: CONTEXT structure for ARM64 is different from x86_64.
                // We need to check what windows-sys provides for ARM64.
                // Typically CONTEXT_ARM64 (if configured) or just CONTEXT.
                // Fields: Pc, Sp, Lr.

                unsafe {
                    if (*self.preemption_flag).load(Ordering::Acquire) {
                        return Ok(());
                    }

                    if SuspendThread(self.thread_handle) == u32::MAX {
                        tracing::trace!(
                            "SuspendThread failed: {}",
                            std::io::Error::last_os_error()
                        );
                        return Err(PreemptionError::InterruptFailed);
                    }

                    // Double-check after suspend to avoid race where flag was cleared just before suspend
                    if (*self.preemption_flag).load(Ordering::Acquire) {
                        ResumeThread(self.thread_handle);
                        return Ok(());
                    }

                    (*self.preemption_flag).store(true, Ordering::Release);
                    let mut context: CONTEXT = std::mem::zeroed();
                    context.ContextFlags = CONTEXT_CONTROL | CONTEXT_INTEGER;
                    if GetThreadContext(self.thread_handle, &mut context) == 0 {
                        tracing::trace!(
                            "GetThreadContext failed: {}",
                            std::io::Error::last_os_error()
                        );
                        ResumeThread(self.thread_handle);
                        return Err(PreemptionError::InterruptFailed);
                    }

                    // Handle architecture-specific context manipulation
                    // Handle architecture-specific context manipulation
                    #[cfg(target_arch = "aarch64")]
                    {
                        // Simulate Call: Set LR to PC
                        let original_pc = context.Pc;
                        context.Lr = original_pc;

                        // Jump to trampoline
                        context.Pc = preemption_trampoline as *const () as u64;
                    }

                    if SetThreadContext(self.thread_handle, &context) == 0 {
                        tracing::trace!(
                            "SetThreadContext failed: {}",
                            std::io::Error::last_os_error()
                        );
                        ResumeThread(self.thread_handle);
                        return Err(PreemptionError::InterruptFailed);
                    }
                    ResumeThread(self.thread_handle);
                    Ok(())
                }
            }
        }
        impl Drop for WorkerThreadHandle {
            fn drop(&mut self) {
                unsafe {
                    CloseHandle(self.thread_handle);
                }
            }
        }
        pub struct PreemptionHandle {
            _marker: std::marker::PhantomData<()>,
        }
        pub(crate) fn init_preemption() -> Result<PreemptionHandle, PreemptionError> {
            Ok(PreemptionHandle {
                _marker: std::marker::PhantomData,
            })
        }
        pub(crate) fn init_worker_preemption() -> Result<PreemptionHandle, PreemptionError> {
            init_preemption()
        }
    }
}

// ----------------------------------------------------------------------------
// RISC-V 64 Implementation (Linux only)
// ----------------------------------------------------------------------------
#[cfg(target_arch = "riscv64")]
mod impl_riscv64 {
    use super::*;
    use std::arch::global_asm;

    // RISC-V 64 Trampoline
    // Volatiles: ra, t0-t6, a0-a7.
    // Stack align: 16 bytes.
    macro_rules! define_riscv64_trampoline {
        ($section_directive:literal) => {
            global_asm!(
                $section_directive,
                ".global preemption_trampoline",
                "preemption_trampoline:",
                // Reserve space for volatile and non-volatile registers.
                // GPRs: ra, t0-t6 (7), a0-a7 (8), s0-s11 (12). Total 28.
                // FPRs: ft0-ft11 (12), fa0-fa7 (8), fs0-fs11 (12). Total 32.
                // Total: 60 registers * 8 bytes = 480 bytes.
                // Stack must be 16-byte aligned. 480 is a multiple of 16.
                "addi sp, sp, -480",
                // Save GPRs
                "sd ra, 0(sp)",
                "sd t0, 8(sp)",
                "sd t1, 16(sp)",
                "sd t2, 24(sp)",
                "sd a0, 32(sp)",
                "sd a1, 40(sp)",
                "sd a2, 48(sp)",
                "sd a3, 56(sp)",
                "sd a4, 64(sp)",
                "sd a5, 72(sp)",
                "sd a6, 80(sp)",
                "sd a7, 88(sp)",
                "sd t3, 96(sp)",
                "sd t4, 104(sp)",
                "sd t5, 112(sp)",
                "sd t6, 120(sp)",
                "sd s0, 128(sp)",
                "sd s1, 136(sp)",
                "sd s2, 144(sp)",
                "sd s3, 152(sp)",
                "sd s4, 160(sp)",
                "sd s5, 168(sp)",
                "sd s6, 176(sp)",
                "sd s7, 184(sp)",
                "sd s8, 192(sp)",
                "sd s9, 200(sp)",
                "sd s10, 208(sp)",
                "sd s11, 216(sp)",
                // Save FPRs
                "fsd ft0, 224(sp)",
                "fsd ft1, 232(sp)",
                "fsd ft2, 240(sp)",
                "fsd ft3, 248(sp)",
                "fsd ft4, 256(sp)",
                "fsd ft5, 264(sp)",
                "fsd ft6, 272(sp)",
                "fsd ft7, 280(sp)",
                "fsd fa0, 288(sp)",
                "fsd fa1, 296(sp)",
                "fsd fa2, 304(sp)",
                "fsd fa3, 312(sp)",
                "fsd fa4, 320(sp)",
                "fsd fa5, 328(sp)",
                "fsd fa6, 336(sp)",
                "fsd fa7, 344(sp)",
                "fsd ft8, 352(sp)",
                "fsd ft9, 360(sp)",
                "fsd ft10, 368(sp)",
                "fsd ft11, 376(sp)",
                "fsd fs0, 384(sp)",
                "fsd fs1, 392(sp)",
                "fsd fs2, 400(sp)",
                "fsd fs3, 408(sp)",
                "fsd fs4, 416(sp)",
                "fsd fs5, 424(sp)",
                "fsd fs6, 432(sp)",
                "fsd fs7, 440(sp)",
                "fsd fs8, 448(sp)",
                "fsd fs9, 456(sp)",
                "fsd fs10, 464(sp)",
                "fsd fs11, 472(sp)",
                "call rust_preemption_helper",
                // Restore FPRs
                "fld fs11, 472(sp)",
                "fld fs10, 464(sp)",
                "fld fs9, 456(sp)",
                "fld fs8, 448(sp)",
                "fld fs7, 440(sp)",
                "fld fs6, 432(sp)",
                "fld fs5, 424(sp)",
                "fld fs4, 416(sp)",
                "fld fs3, 408(sp)",
                "fld fs2, 400(sp)",
                "fld fs1, 392(sp)",
                "fld fs0, 384(sp)",
                "fld ft11, 376(sp)",
                "fld ft10, 368(sp)",
                "fld ft9, 360(sp)",
                "fld ft8, 352(sp)",
                "fld fa7, 344(sp)",
                "fld fa6, 336(sp)",
                "fld fa5, 328(sp)",
                "fld fa4, 320(sp)",
                "fld fa3, 312(sp)",
                "fld fa2, 304(sp)",
                "fld fa1, 296(sp)",
                "fld fa0, 288(sp)",
                "fld ft7, 280(sp)",
                "fld ft6, 272(sp)",
                "fld ft5, 264(sp)",
                "fld ft4, 256(sp)",
                "fld ft3, 248(sp)",
                "fld ft2, 240(sp)",
                "fld ft1, 232(sp)",
                "fld ft0, 224(sp)",
                // Restore GPRs
                "ld s11, 216(sp)",
                "ld s10, 208(sp)",
                "ld s9, 200(sp)",
                "ld s8, 192(sp)",
                "ld s7, 184(sp)",
                "ld s6, 176(sp)",
                "ld s5, 168(sp)",
                "ld s4, 160(sp)",
                "ld s3, 152(sp)",
                "ld s2, 144(sp)",
                "ld s1, 136(sp)",
                "ld s0, 128(sp)",
                "ld t6, 120(sp)",
                "ld t5, 112(sp)",
                "ld t4, 104(sp)",
                "ld t3, 96(sp)",
                "ld a7, 88(sp)",
                "ld a6, 80(sp)",
                "ld a5, 72(sp)",
                "ld a4, 64(sp)",
                "ld a3, 56(sp)",
                "ld a2, 48(sp)",
                "ld a1, 40(sp)",
                "ld a0, 32(sp)",
                "ld t2, 24(sp)",
                "ld t1, 16(sp)",
                "ld t0, 8(sp)",
                "ld ra, 0(sp)",
                "addi sp, sp, 480",
                "ret"
            );
        };
    }

    #[cfg(not(target_os = "macos"))]
    define_riscv64_trampoline!(".section .text");

    #[cfg(unix)]
    pub(crate) use unix_impl::init_worker_preemption;
    #[cfg(unix)]
    pub use unix_impl::{PreemptionHandle, WorkerThreadHandle};

    #[cfg(unix)]
    mod unix_impl {
        use super::super::*;
        use std::mem::MaybeUninit;

        unsafe extern "C" {
            fn preemption_trampoline();
        }

        pub struct WorkerThreadHandle {
            pthread: libc::pthread_t,
        }
        unsafe impl Send for WorkerThreadHandle {}
        unsafe impl Sync for WorkerThreadHandle {}

        impl WorkerThreadHandle {
            pub fn current() -> Result<Self, PreemptionError> {
                Ok(Self {
                    pthread: unsafe { libc::pthread_self() },
                })
            }
            pub fn interrupt(&self) -> Result<(), PreemptionError> {
                unsafe {
                    if libc::pthread_kill(self.pthread, libc::SIGVTALRM) == 0 {
                        Ok(())
                    } else {
                        Err(PreemptionError::InterruptFailed)
                    }
                }
            }
        }

        pub struct PreemptionHandle {
            old_handler: libc::sigaction,
        }
        impl Drop for PreemptionHandle {
            fn drop(&mut self) {
                unsafe {
                    libc::sigaction(libc::SIGVTALRM, &self.old_handler, ptr::null_mut());
                }
            }
        }
        pub(crate) fn init_worker_preemption() -> Result<PreemptionHandle, PreemptionError> {
            init_preemption()
        }
        fn init_preemption() -> Result<PreemptionHandle, PreemptionError> {
            unsafe {
                let mut sa = MaybeUninit::<libc::sigaction>::uninit();
                let sa_ptr = sa.as_mut_ptr();
                (*sa_ptr).sa_sigaction = sigalrm_handler as usize;
                libc::sigemptyset(&mut (*sa_ptr).sa_mask);
                // Block SIGVTALRM during handler execution to prevent reentrancy
                libc::sigaddset(&mut (*sa_ptr).sa_mask, libc::SIGVTALRM);
                (*sa_ptr).sa_flags = libc::SA_RESTART | libc::SA_SIGINFO;
                let sa = sa.assume_init();

                let mut old_sa: libc::sigaction = MaybeUninit::zeroed().assume_init();
                if libc::sigaction(libc::SIGVTALRM, &sa, &mut old_sa) != 0 {
                    return Err(PreemptionError::SignalSetupFailed);
                }
                Ok(PreemptionHandle {
                    old_handler: old_sa,
                })
            }
        }

        extern "C" fn sigalrm_handler(
            _signum: libc::c_int,
            _info: *mut libc::siginfo_t,
            context: *mut libc::c_void,
        ) {
            unsafe {
                let ctx = context as *mut libc::ucontext_t;
                let mcontext = &mut (*ctx).uc_mcontext;

                // Linux RISC-V 64
                #[cfg(target_os = "linux")]
                let (pc_ptr, ra_ptr) = (
                    &mut mcontext.__gregs[0] as *mut _ as *mut u64, // REG_PC = 0
                    &mut mcontext.__gregs[1] as *mut _ as *mut u64, // REG_RA = 1
                );

                #[cfg(target_os = "freebsd")]
                let (pc_ptr, ra_ptr) = (
                    &mut mcontext.mc_gpregs.gp_sepc as *mut _ as *mut u64,
                    &mut mcontext.mc_gpregs.gp_ra as *mut _ as *mut u64,
                );

                #[cfg(target_os = "openbsd")]
                let (pc_ptr, ra_ptr) = (
                    &mut mcontext.sc_sepc as *mut _ as *mut u64,
                    &mut mcontext.sc_ra as *mut _ as *mut u64,
                );

                #[cfg(target_os = "netbsd")]
                let (pc_ptr, ra_ptr) = (
                    &mut mcontext.__gregs[libc::_REG_PC] as *mut _ as *mut u64,
                    &mut mcontext.__gregs[libc::_REG_RA] as *mut _ as *mut u64,
                );

                let original_pc = *pc_ptr;

                // Simulate CALL (JAL):
                // Set RA (Return Address) to original PC
                *ra_ptr = original_pc;

                // Jump to Trampoline
                *pc_ptr = preemption_trampoline as *const () as u64;

                // Stack alignment: RISC-V calling convention requires 16-byte alignment.
                // Since we're not modifying SP (return address is in RA register, not on
                // stack), the existing alignment is preserved. The trampoline adjusts SP
                // by 320 bytes (multiple of 16) to maintain alignment.

                // CRITICAL: Keep SIGVTALRM blocked in the resumed context to prevent
                // reentrant signals during trampoline execution.
                libc::sigaddset(&mut (*ctx).uc_sigmask, libc::SIGVTALRM);

                // Memory barrier: Ensure all context modifications are visible before
                // returning from the handler.
                std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::Release);
            }
        }
    }
}

#[cfg(target_arch = "loongarch64")]
mod impl_loongarch64 {
    use super::*;
    use std::arch::global_asm;

    #[cfg(not(target_os = "linux"))]
    compile_error!("LoongArch64 preemption is currently only implemented for Linux.");

    #[cfg(target_os = "linux")]
    global_asm!(
        ".section .text",
        ".global preemption_trampoline",
        "preemption_trampoline:",
        // Reserve space for 30 GPRs and 32 FPRs (496 bytes total).
        "addi.d $sp, $sp, -496",
        // GPR spill area (0..232)
        "st.d $ra, $sp, 0",
        "st.d $tp, $sp, 8",
        "st.d $a0, $sp, 16",
        "st.d $a1, $sp, 24",
        "st.d $a2, $sp, 32",
        "st.d $a3, $sp, 40",
        "st.d $a4, $sp, 48",
        "st.d $a5, $sp, 56",
        "st.d $a6, $sp, 64",
        "st.d $a7, $sp, 72",
        "st.d $t0, $sp, 80",
        "st.d $t1, $sp, 88",
        "st.d $t2, $sp, 96",
        "st.d $t3, $sp, 104",
        "st.d $t4, $sp, 112",
        "st.d $t5, $sp, 120",
        "st.d $t6, $sp, 128",
        "st.d $t7, $sp, 136",
        "st.d $t8, $sp, 144",
        "st.d $u0, $sp, 152",
        "st.d $fp, $sp, 160",
        "st.d $s0, $sp, 168",
        "st.d $s1, $sp, 176",
        "st.d $s2, $sp, 184",
        "st.d $s3, $sp, 192",
        "st.d $s4, $sp, 200",
        "st.d $s5, $sp, 208",
        "st.d $s6, $sp, 216",
        "st.d $s7, $sp, 224",
        "st.d $s8, $sp, 232",
        // FPR spill area (240..488)
        "fst.d $f0,  $sp, 240",
        "fst.d $f1,  $sp, 248",
        "fst.d $f2,  $sp, 256",
        "fst.d $f3,  $sp, 264",
        "fst.d $f4,  $sp, 272",
        "fst.d $f5,  $sp, 280",
        "fst.d $f6,  $sp, 288",
        "fst.d $f7,  $sp, 296",
        "fst.d $f8,  $sp, 304",
        "fst.d $f9,  $sp, 312",
        "fst.d $f10, $sp, 320",
        "fst.d $f11, $sp, 328",
        "fst.d $f12, $sp, 336",
        "fst.d $f13, $sp, 344",
        "fst.d $f14, $sp, 352",
        "fst.d $f15, $sp, 360",
        "fst.d $f16, $sp, 368",
        "fst.d $f17, $sp, 376",
        "fst.d $f18, $sp, 384",
        "fst.d $f19, $sp, 392",
        "fst.d $f20, $sp, 400",
        "fst.d $f21, $sp, 408",
        "fst.d $f22, $sp, 416",
        "fst.d $f23, $sp, 424",
        "fst.d $f24, $sp, 432",
        "fst.d $f25, $sp, 440",
        "fst.d $f26, $sp, 448",
        "fst.d $f27, $sp, 456",
        "fst.d $f28, $sp, 464",
        "fst.d $f29, $sp, 472",
        "fst.d $f30, $sp, 480",
        "fst.d $f31, $sp, 488",
        "bl rust_preemption_helper",
        // Restore FPRs (descending order)
        "fld.d $f31, $sp, 488",
        "fld.d $f30, $sp, 480",
        "fld.d $f29, $sp, 472",
        "fld.d $f28, $sp, 464",
        "fld.d $f27, $sp, 456",
        "fld.d $f26, $sp, 448",
        "fld.d $f25, $sp, 440",
        "fld.d $f24, $sp, 432",
        "fld.d $f23, $sp, 424",
        "fld.d $f22, $sp, 416",
        "fld.d $f21, $sp, 408",
        "fld.d $f20, $sp, 400",
        "fld.d $f19, $sp, 392",
        "fld.d $f18, $sp, 384",
        "fld.d $f17, $sp, 376",
        "fld.d $f16, $sp, 368",
        "fld.d $f15, $sp, 360",
        "fld.d $f14, $sp, 352",
        "fld.d $f13, $sp, 344",
        "fld.d $f12, $sp, 336",
        "fld.d $f11, $sp, 328",
        "fld.d $f10, $sp, 320",
        "fld.d $f9,  $sp, 312",
        "fld.d $f8,  $sp, 304",
        "fld.d $f7,  $sp, 296",
        "fld.d $f6,  $sp, 288",
        "fld.d $f5,  $sp, 280",
        "fld.d $f4,  $sp, 272",
        "fld.d $f3,  $sp, 264",
        "fld.d $f2,  $sp, 256",
        "fld.d $f1,  $sp, 248",
        "fld.d $f0,  $sp, 240",
        // Restore GPRs (reverse order)
        "ld.d $s8, $sp, 232",
        "ld.d $s7, $sp, 224",
        "ld.d $s6, $sp, 216",
        "ld.d $s5, $sp, 208",
        "ld.d $s4, $sp, 200",
        "ld.d $s3, $sp, 192",
        "ld.d $s2, $sp, 184",
        "ld.d $s1, $sp, 176",
        "ld.d $s0, $sp, 168",
        "ld.d $fp, $sp, 160",
        "ld.d $u0, $sp, 152",
        "ld.d $t8, $sp, 144",
        "ld.d $t7, $sp, 136",
        "ld.d $t6, $sp, 128",
        "ld.d $t5, $sp, 120",
        "ld.d $t4, $sp, 112",
        "ld.d $t3, $sp, 104",
        "ld.d $t2, $sp, 96",
        "ld.d $t1, $sp, 88",
        "ld.d $t0, $sp, 80",
        "ld.d $a7, $sp, 72",
        "ld.d $a6, $sp, 64",
        "ld.d $a5, $sp, 56",
        "ld.d $a4, $sp, 48",
        "ld.d $a3, $sp, 40",
        "ld.d $a2, $sp, 32",
        "ld.d $a1, $sp, 24",
        "ld.d $a0, $sp, 16",
        "ld.d $tp, $sp, 8",
        "ld.d $ra, $sp, 0",
        "addi.d $sp, $sp, 496",
        "jirl $zero, $ra, 0"
    );

    #[cfg(target_os = "linux")]
    pub(crate) use unix_impl::init_worker_preemption;
    #[cfg(target_os = "linux")]
    pub use unix_impl::{PreemptionHandle, WorkerThreadHandle};

    #[cfg(target_os = "linux")]
    mod unix_impl {
        use super::super::*;
        use std::mem::MaybeUninit;

        unsafe extern "C" {
            fn preemption_trampoline();
        }

        pub struct WorkerThreadHandle {
            pthread: libc::pthread_t,
        }
        unsafe impl Send for WorkerThreadHandle {}
        unsafe impl Sync for WorkerThreadHandle {}

        impl WorkerThreadHandle {
            pub fn current() -> Result<Self, PreemptionError> {
                Ok(Self {
                    pthread: unsafe { libc::pthread_self() },
                })
            }
            pub fn interrupt(&self) -> Result<(), PreemptionError> {
                unsafe {
                    if libc::pthread_kill(self.pthread, libc::SIGVTALRM) == 0 {
                        Ok(())
                    } else {
                        Err(PreemptionError::InterruptFailed)
                    }
                }
            }
        }

        pub struct PreemptionHandle {
            old_handler: libc::sigaction,
        }
        impl Drop for PreemptionHandle {
            fn drop(&mut self) {
                unsafe {
                    libc::sigaction(libc::SIGVTALRM, &self.old_handler, ptr::null_mut());
                }
            }
        }
        pub(crate) fn init_worker_preemption() -> Result<PreemptionHandle, PreemptionError> {
            init_preemption()
        }
        fn init_preemption() -> Result<PreemptionHandle, PreemptionError> {
            unsafe {
                let mut sa = MaybeUninit::<libc::sigaction>::uninit();
                let sa_ptr = sa.as_mut_ptr();
                (*sa_ptr).sa_sigaction = sigalrm_handler as usize;
                libc::sigemptyset(&mut (*sa_ptr).sa_mask);
                // Block SIGVTALRM during handler execution to prevent reentrancy
                libc::sigaddset(&mut (*sa_ptr).sa_mask, libc::SIGVTALRM);
                (*sa_ptr).sa_flags = libc::SA_RESTART | libc::SA_SIGINFO;
                let sa = sa.assume_init();

                let mut old_sa: libc::sigaction = MaybeUninit::zeroed().assume_init();
                if libc::sigaction(libc::SIGVTALRM, &sa, &mut old_sa) != 0 {
                    return Err(PreemptionError::SignalSetupFailed);
                }
                Ok(PreemptionHandle {
                    old_handler: old_sa,
                })
            }
        }

        extern "C" fn sigalrm_handler(
            _signum: libc::c_int,
            _info: *mut libc::siginfo_t,
            context: *mut libc::c_void,
        ) {
            unsafe {
                let ctx = context as *mut libc::ucontext_t;
                let mcontext = &mut (*ctx).uc_mcontext;

                let pc_ptr = &mut mcontext.__pc as *mut _ as *mut u64;
                let ra_ptr = &mut mcontext.__gregs[1] as *mut _ as *mut u64; // $ra lives in r1

                let original_pc = *pc_ptr;
                *ra_ptr = original_pc;
                *pc_ptr = preemption_trampoline as *const () as u64;

                // Stack alignment: LoongArch64 requires 16-byte alignment. Since we're not
                // modifying SP (return address is in $ra register), the existing alignment
                // is preserved. The trampoline adjusts SP by 496 bytes (multiple of 16).

                // CRITICAL: Keep SIGVTALRM blocked in the resumed context to prevent
                // reentrant signals during trampoline execution.
                libc::sigaddset(&mut (*ctx).uc_sigmask, libc::SIGVTALRM);

                // Memory barrier: Ensure all context modifications are visible before
                // returning from the handler.
                std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::Release);
            }
        }
    }
}

#[cfg(target_arch = "aarch64")]
pub use impl_aarch64::*;
#[cfg(target_arch = "loongarch64")]
pub use impl_loongarch64::*;
#[cfg(target_arch = "riscv64")]
pub use impl_riscv64::*;
#[cfg(target_arch = "x86_64")]
pub use impl_x64::*;
#[cfg(not(any(
    target_arch = "x86_64",
    target_arch = "aarch64",
    target_arch = "riscv64",
    target_arch = "loongarch64"
)))]
pub use unsupported::*;

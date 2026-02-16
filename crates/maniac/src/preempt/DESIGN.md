# Preemptive Scheduling Design

This document details the design and implementation of the preemptive scheduling mechanism in `maniac-runtime`. The
system allows worker threads executing long-running synchronous code (within generators) to be interrupted and
cooperatively yielded back to the scheduler.

## 1. Core Architecture: The "Trampoline" Injection

Traditional signal handlers (Unix) or suspended thread contexts (Windows) are extremely restricted execution
environments. They cannot safely access Thread-Local Storage (TLS), acquire mutexes, or perform allocations without
risking deadlocks or corruption ("Async-Signal Safety").

To solve this, we use an **Injection Strategy**:

1. **Interrupt** the target thread.
2. **Modify** the thread's execution context (registers/stack) to simulate a function call to a **Trampoline**.
3. **Resume** the thread. The thread "returns" from the interrupt into our Trampoline.
4. The Trampoline saves the entire machine state and calls a safe Rust helper (`rust_preemption_helper`).
5. The helper performs the actual generator yield (context switch).

This ensures that the complex logic of yielding and scheduling happens in a standard thread context, not an interrupt
context.

---

## 2. Platform Implementation

### 2.1 Unix (Linux, macOS, *BSD)

**Mechanism:** POSIX Signals (`SIGVTALRM`).

1. **Setup:** `init_worker_preemption` installs a signal handler for `SIGVTALRM` with `SA_SIGINFO`.
2. **Trigger:** The scheduler thread calls `pthread_kill(target, SIGVTALRM)`.
3. **Signal Handler (`sigalrm_handler`):**
    * Extracts the original Instruction Pointer (RIP/PC) and Stack Pointer (RSP/SP) from `ucontext_t`.
    * **x86_64:** Pushes `RIP` onto the stack (or sets up stack for return) and sets `RIP` to `preemption_trampoline`.
    * **AArch64/RISC-V:** Sets the Link Register (`LR` / `ra`) to the original `PC`, and sets `PC` to
      `preemption_trampoline`. This emulates a branch-with-link instruction (`BL` / `CALL`).
    * **Critical:** Adds `SIGVTALRM` to the thread's blocked signal mask in the resumed context to prevent recursive
      preemption loops.
4. **Trampoline:** See Architecture section.

### 2.2 Windows

**Mechanism:** `SuspendThread` / `GetThreadContext` / `SetThreadContext`.

1. **Trigger:** The scheduler thread calls `WorkerThreadHandle::interrupt`.
2. **Safety Check:** Reads an atomic `preemption_flag` to ensure we don't interrupt a thread that is already processing
   a preemption (prevents stack overflow).
3. **Injection:**
    * Calls `SuspendThread`.
    * Calls `GetThreadContext` (getting `CONTEXT_CONTROL | CONTEXT_INTEGER`).
    * **Call Simulation (x86_64):**
        * Decrements `RSP` by 8.
        * Writes the original `RIP` to the new stack location using `WriteProcessMemory` (safely handling memory
          protection).
        * Sets `RIP` to `preemption_trampoline`.
    * Calls `SetThreadContext` and `ResumeThread`.

---

## 3. CPU Architecture & Assembly

Since preemption happens asynchronously (between any two instructions), the trampoline must preserve **strict ABI
compliance** and save **all registers**, including those normally considered "callee-saved" (non-volatile), because the
`yield` operation involves a stack switch that would otherwise clobber them.

### 3.1 x86_64 (System V & Windows)

* **Register Preservation:** Saves all GPRs (`RAX`..`R15`) and XMM registers (`XMM0`..`XMM15`).
* **Windows Specifics:**
    * **Shadow Space:** Allocates 32 bytes of shadow space before calling `rust_preemption_helper` (Win64 ABI
      requirement).
    * **Dynamic Alignment:** Uses `RBP` as a frame pointer to dynamically align `RSP` to 16 bytes. This is necessary
      because `SuspendThread` can stop execution anywhere (e.g., inside a function prologue where the stack is
      temporarily misaligned).
* **Unix Specifics:**
    * **Stack Pivoting:** Uses a robust `xchg` technique to restore the original stack pointer and return address
      without clobbering registers during the sensitive transition back from the signal stack frame to the user stack.

### 3.2 AArch64 (ARM64)

* **Mechanism:** Emulates a `BL` (Branch with Link). Original PC is stored in `x30` (LR).
* **Register Preservation:** Saves pairs of GPRs (`stp x0, x1`, ..., `x29, x30`) and all SIMD registers (`q0`..`q31`).
  Saves `x19`-`x28` (callee-saved) which are critical for the context switch.
* **Windows ARM64:** Allocates 32 bytes of shadow space.

### 3.3 RISC-V (64-bit)

* **Mechanism:** Emulates `JAL`. Original PC is stored in `ra`.
* **Register Preservation:** Saves `ra`, `t0`-`t6` (temporaries), `a0`-`a7` (arguments), and critical callee-saved
  registers `s0`-`s11` and `fs0`-`fs11`.

---

## 4. Generator Interaction

The bridge between the assembly trampoline and the runtime logic is `rust_preemption_helper`.

1. **State Lookup:** It accesses `CURRENT_GENERATOR_SCOPE` (TLS) or the `Worker` struct to find the currently running
   generator.
2. **Validation:** Checks if the pointer is valid. If the generator has finished or is in an invalid state, it returns
   immediately (trampoline restores registers and resumes original code).
3. **Yielding:**
    * Calls `scope.yield_(GeneratorYieldReason::Preempted)`.
    * This invokes the low-level context switch (`swap_registers`), saving the current stack (inside the trampoline) and
      switching to the scheduler's stack.
4. **Resuming:**
    * When the scheduler picks this task again, `swap_registers` returns.
    * `rust_preemption_helper` returns.
    * Trampoline restores registers.
    * Execution continues exactly where it was interrupted.

## 5. Soundness & Safety Analysis

1. **Async-Signal Safety:** The Unix signal handler performs no heap allocations, locking, or complex logic. It only
   manipulates `ucontext_t` registers.
2. **Memory Safety (Windows):** We use `WriteProcessMemory` instead of raw pointer dereferences to inject the return
   address. This handles edge cases where the stack might be guarded or invalid.
3. **Stack Alignment:** All trampolines forcefully align the stack to 16 bytes (ABI requirement). This prevents
   undefined behavior in Rust/LLVM generated code (e.g., aligned SSE loads).
4. **Register Integrity:** By saving *all* registers (including FPU/SIMD and callee-saved GPRs), we ensure that the
   interruption is completely transparent to the running code. This is vital because the context switch (
   `swap_registers`) modifies callee-saved registers; without saving them in the trampoline, the interrupted function
   would see corrupted state upon return.
5. **Re-entrancy Protection:**
    * **Unix:** `sa_mask` blocks `SIGVTALRM` automatically during the handler. We explicitly keep it blocked in the
      trampoline context.
    * **Windows:** Atomic `preemption_flag` check prevents injecting a new trampoline before the previous one has
      yielded or finished.


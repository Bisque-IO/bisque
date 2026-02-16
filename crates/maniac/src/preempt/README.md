# Preemption System Design

This document describes the design and implementation of the preemption system in `maniac-runtime`. The system allows
worker threads executing generator-based tasks to be interrupted safely, enabling time-sliced scheduling and preventing
CPU-bound tasks from starving I/O operations.

## Architecture Overview

The system upgrades the cooperative `generator` crate (which handles stack switching) into a preemption-safe mechanism.
It achieves this by injecting a "trampoline" function into the execution stream of a running thread, which then performs
a cooperative yield on behalf of the interrupted code.

### The Challenge

Standard cooperative multitasking (like `Gn`) relies on the compiler and ABI conventions to save "callee-saved"
registers during a context switch. However, an asynchronous interrupt (like a Unix signal or Windows thread suspension)
can occur at *any* instruction boundary, where "volatile" registers (RAX, RCX, etc.) are live and hold critical values.

If we simply called `yield_()` from an interrupt handler:

1. **Corruption**: The `yield_` function would clobber volatile registers that the interrupted code expects to be
   preserved.
2. **Deadlocks (Unix)**: Signal handlers run with signals blocked and are not async-signal-safe. Accessing TLS or
   locks (required for yielding) can cause deadlocks.
3. **Deadlocks (Windows)**: `SuspendThread` can stop a thread while it holds the system heap lock. Allocating memory in
   the handler would deadlock the process.

### The Solution: Trampoline Injection

Instead of yielding *inside* the interrupt context, we redirect the thread's execution flow to a safe "trampoline"
function that runs in normal user mode.

#### 1. Interrupt Phase

* **Unix (Linux/macOS)**: A timer thread sends `SIGVTALRM` to the worker thread. The signal handler executes on the
  worker's stack.
* **Windows**: A timer thread calls `SuspendThread` on the worker thread.

#### 2. Injection Phase

The handler modifies the interrupted thread's saved context (PC/RIP and Stack Pointer):

* **Simulate Call**: It pushes the original Program Counter (PC/RIP) onto the stack (or Link Register on ARM64) to
  simulate a function call.
* **Redirect**: It sets the PC/RIP to point to `preemption_trampoline`.
* **Resume**: The thread is resumed (or signal handler returns). The CPU "returns" not to the original code, but to our
  trampoline.

#### 3. Trampoline Phase

The `preemption_trampoline` is a naked assembly function that:

1. **Save State**: Pushes *all* volatile registers (General Purpose and SIMD/FP) onto the current stack (the **generator
   stack**).
2. **Enter Rust**: Calls `rust_preemption_helper()`. This is safe Rust code running in a normal thread context (not a
   signal handler), so it can access TLS and locks safely.
3. **Yield**: The helper calls `scope.yield_(1)`. This triggers the standard `Gn` stack switch, saving the trampoline's
   stack frame (with our saved registers) and switching to the worker loop.

#### 4. Resume Phase

When the scheduler decides to resume the task:

1. `Gn` switches back to the generator stack.
2. Execution resumes inside `rust_preemption_helper`, which returns to the trampoline.
3. **Restore State**: The trampoline pops all saved registers, restoring the CPU to the *exact* state it was in before
   the interrupt.
4. **Return**: The trampoline executes `ret`, jumping to the original PC/RIP we saved in step 2. The task continues as
   if nothing happened.

## Platform Details

### x86_64 (Unix & Windows)

* **Trampoline**: Saves all SysV (Unix) or Win64 (Windows) volatile registers.
* **Alignment**: Ensures 16-byte stack alignment before calling Rust code.
* **Red Zone (Unix)**: Explicitly preserves the 128-byte Red Zone below the stack pointer to avoid corrupting leaf
  function data.

### AArch64 (Linux, macOS, *BSD, Windows)

* **Trampoline**: Saves pairs of registers (`stp`) including x0-x18 (volatile GPRs) and v0-v7/v16-v31 (volatile SIMD/FP)
  on every target.
* **Alignment & Shadow Space**: Guarantees 16-byte alignment universally and reserves the Windows ARM64 “home space” (32
  bytes) before calling into Rust so that the Win64 ABI invariants hold even when interrupted mid-instruction.
* **Injection**: Updates the Link Register (`x30`) to point to the original PC (simulating a `BL` instruction) and sets
  PC to the trampoline.

### RISC-V 64 (Linux & *BSD)

* **Trampoline**: Saves volatile GPRs (ra, t0-t6, a0-a7) and all volatile FP registers (ft0-ft11, fa0-fa7), ensuring no
  live SIMD value is lost on entry.
* **Injection**: Sets `RA` to original PC and jumps to trampoline.

### LoongArch64 (Linux)

* **Trampoline**: Saves every caller-visible GPR (ra, tp, a/t/u/s classes) plus the full 32-entry FP register file so
  asynchronous interrupts can’t corrupt SIMD/FPU state.
* **Injection**: Writes the interrupted PC into `$ra` inside the signal context and redirects execution to
  `preemption_trampoline`, mirroring a synthetic `bl`.

## Correctness Guarantees

1. **Stack Agnostic**: The system works regardless of stack depth because it uses the current stack pointer (which
   points to the active generator stack).
2. **Register Integrity**: By saving *all* volatile registers, we bridge the gap between "async interrupt ABI" (where
   everything matters) and "function call ABI" (where only callee-saved matters).
3. **Signal Safety**: We never lock or yield inside a signal handler. The handler only performs atomic context
   manipulation.
4. **Linker Compatibility**: Explicit `.section .text` (ELF/Windows) and `.section __TEXT,__text` (Mach-O) directives
   ensure the trampoline is correctly linked and executable on all platforms.


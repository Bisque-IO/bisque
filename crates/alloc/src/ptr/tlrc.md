# Tlrc — Thread-Local Reference Counted Smart Pointer

## Overview

`Tlrc<T>` is a single-owner smart pointer that produces lightweight `TlrcRef<T>`
references. Clone and drop of `TlrcRef` are **zero-contention** on the hot path:
each thread maintains a private counter that requires no atomic read-modify-write
instructions, no CAS, no fences, and no loops under normal operation.

Destruction uses a scanner-based protocol: when the master (`Tlrc`) drops and
per-thread counters sum to zero, the data is destroyed. The scanner uses
per-counter CAS (cold path only) to safely read counters without racing with
active threads.

## Architecture

```text
┌──────────┐     ┌────────────────────────────────────────┐
│ Tlrc<T>  │────►│ TlrcInner<T>                           │
│ (master) │     │   counters: Mutex<Vec<*mut TLCounter>> │
└──────────┘     │   destroying: AtomicBool               │
                 │   data: ManuallyDrop<T>                │
┌──────────┐     └────────────────────────────────────────┘
│ TlrcRef  │              │ counters Vec contains:
│ (8 bytes)│              ▼
└──────────┘   ┌────────────────────────┐
               │ TLCounter (heap)       │ ◄── Thread A's map[key]
               │   count: AtomicI64     │     (cached for O(1) lookup)
               │   seq: AtomicU32       │
               │   orphaned: AtomicBool │
               │   tlrc_ptr: AtomicU64  │
               ├────────────────────────┤
               │ TLCounter (heap)       │ ◄── Thread B's map[key]
               │   ...                  │
               └────────────────────────┘
```

**Key:** Each `TLCounter` exists in exactly two places:
1. The owning thread's `TLCounterMap` HashMap (thread-local, keyed by `inner_ptr`)
2. The `TlrcInner.counters` Vec (shared, Mutex-protected)

## Hot Path: NO CAS, NO Loop, NO Fence

### Clone (`tlrc_increment`)

```text
COUNTER_MAP.with → cached hit?
  YES → do_bracket(+1)
  NO  → HashMap lookup → stale check → do_bracket(+1)
  MISS → cold path: epoch guard → Mutex lock → alloc counter → register
```

### Drop (`tlrc_decrement`)

```text
COUNTER_MAP.with → cached hit?
  YES → read tlrc_ptr (master_dropped check) → do_bracket(-1)
        if master_dropped && count ≤ 0 → try_destroy
  NO  → HashMap lookup → stale check → same
  MISS → cold path: epoch guard → Mutex lock → alloc counter(-1) → register
```

### Bracket (`do_bracket`)

```text
s = seq.load(Relaxed)                    ← store-forwarded: own value
if s == LOCKED_SEQ: spin (scanner active, extremely rare)
seq.store(FROZEN_SEQ, Release)           ← signal "busy" to scanner
c = count.load(Relaxed)
count.store(c + delta, Release)          ← modify count
seq.store(next, Release)                 ← signal "idle"
```

**x86 codegen:** 5 plain `MOV` instructions. No `LOCK` prefix, no `MFENCE`.

**ARM codegen:** 5 `STLR`/`LDR` pairs. No `LDXR`/`STXR` (LL/SC), no `DMB`.

## Destruction Protocol

### Tlrc::drop

```text
1. Fast path: counters.is_empty() → drop T + dealloc immediately
2. Remove master's counter from thread-local map, mark orphaned
3. Set tlrc_ptr = MASTER_DROPPED_SENTINEL on ALL counters (under Mutex)
4. Call try_destroy (all refs might already be gone)
```

### try_destroy

```text
1. Enter epoch guard (keeps shell alive if another thread retires it)
2. Fast bail: if destroying == true → return false
3. Lock Mutex (serializes try_destroy callers, blocks new registrations)
4. Re-check destroying under lock
5. Scan all counters:
   for each counter:
     CAS seq → LOCKED (mutual exclusion with thread's bracket)
     read count
     restore seq
     sum += count
6. If sum ≠ 0 → return false (refs still exist)
7. Mark all counters stale (tlrc_ptr = 0)
   Free orphaned counters (owning thread dead)
8. Set destroying = true
9. Drop Mutex
10. ManuallyDrop::drop(data) — T::drop runs (wrapped in catch_unwind)
11. Epoch-retire the shell (deferred deallocation)
12. Resume panic if T::drop panicked
```

### Scan Protocol: Scanner-Side CAS

```text
Scanner (try_destroy)        Thread (do_bracket)
─────────────────────        ───────────────────
                             s = seq.load(Relaxed) → own value
                             seq.store(FROZEN_SEQ) → "busy"
                             count.store(c±1)
seq.load(Acquire)
  if FROZEN → spin           seq.store(next) → "idle"
  if LOCKED → spin (other scanner)
CAS(seq, idle → LOCKED) ───┐
  count.load(Acquire)      │ mutual exclusion:
  seq.store(idle, Release) ┘ CAS fails if thread entered
```

The CAS is on the **scanner side only** (cold path). The thread's bracket
uses plain stores. If the scanner's CAS finds the counter busy (FROZEN):
it spins until idle. If the CAS fails (thread entered between load and
CAS): the scanner retries.

## Memory Ordering Analysis

### x86 (TSO)

All `Release` stores are plain `MOV` (no LOCK prefix). All `Acquire`
loads are plain `MOV`. TSO guarantees stores from one core are visible
in program order to other cores. The scanner's `Acquire` load of `count`
after a successful CAS (which is `LOCK CMPXCHG`) sees all stores from
the owning thread that preceded the thread's last idle `seq.store`.

### ARM (Weakly Ordered)

`Release` stores use `STLR` (store-release). `Acquire` loads use `LDAR`
(load-acquire). The scanner's `LDAR` of `count` synchronizes with the
thread's `STLR` of `count` via the CAS-based mutual exclusion on `seq`.
When the scanner CAS's `seq → LOCKED`, the `AcqRel` ordering ensures
all prior thread stores (including `count`) are visible.

### Ordering Table

| Operation | Store | Load | x86 | ARM |
|---|---|---|---|---|
| count (thread) | Release | Relaxed | MOV | STLR + LDR |
| seq (thread) | Release | Relaxed | MOV | STLR + LDR |
| count (scanner) | — | Acquire | MOV | LDAR |
| seq (scanner CAS) | AcqRel | — | LOCK CMPXCHG | LDAXR/STLXR |
| tlrc_ptr | Release | Acquire | MOV | STLR + LDAR |
| destroying | Release | Acquire | MOV | STLR + LDAR |
| counters Mutex | — | — | LOCK CMPXCHG | LDAXR/STLXR |

## TLCounter Lifecycle

### States

```text
┌──────────────┐  alloc_counter()  ┌─────────────────┐
│  (not exist) │ ────────────────► │ ACTIVE          │
└──────────────┘                   │ tlrc_ptr = key  │
                                   │ orphaned = false│
                                   └───────┬─────────┘
                                           │
                        ┌──────────────────┼──────────────────┐
                        ▼                  ▼                  ▼
               ┌────────────────┐ ┌─────────────────┐ ┌────────────────┐
               │ MASTER_DROPPED │ │ STALE           │ │ ORPHANED       │
               │ tlrc_ptr = 1   │ │ tlrc_ptr = 0    │ │ orphaned = true│
               │ (master dropped│ │ (Tlrc destroyed)│ │ (thread exited)│
               │  inner alive)  │ │                 │ │                │
               └───────┬────────┘ └───────┬─────────┘ └───────┬────────┘
                       │                  │                   │
                       │  try_destroy     │                   │
                       ▼  sets tlrc_ptr=0 │                   │
               ┌────────────────┐         │  try_destroy      │
               │ STALE+ORPHANED │◄────────┘  sets tlrc_ptr=0  │
               │ tlrc_ptr = 0   │◄────────────────────────────┘
               │ orphaned = true│
               └───────┬────────┘
                       │
                       ▼  freed by try_destroy (orphaned=true)
               ┌────────────────┐  OR by sweep_stale/get_or_stale
               │ FREED          │  OR by TLCounterMap::Drop (tlrc_ptr=0)
               └────────────────┘
```

### Who Frees What

| Ordering | Who frees | How |
|---|---|---|
| Tlrc dies first, thread alive | Owning thread | `sweep_stale`, `get_or_stale`, or next cold-path op |
| Thread dies first, Tlrc alive | `try_destroy` | Checks `orphaned=true` + `tlrc_ptr=0` → `Box::from_raw` |
| Both concurrent | Whoever runs second | Thread exit sees stale → frees. OR try_destroy sees orphaned → frees. |
| Master's own counter | `try_destroy` | `Tlrc::drop` marks it `orphaned=true` before removing from map |

**Invariant:** Every counter is freed exactly once. No leaks, no double-frees.

## Shell Lifecycle (Epoch-Based)

After `try_destroy` drops `T::data`, the `TlrcInner` shell (Mutex, destroying flag)
must stay alive for concurrent `try_destroy` callers to safely check `destroying`
and bail out. The shell is retired via `seize::Collector::retire()`:

```text
try_destroy:
  1. ManuallyDrop::drop(data)    ← T::drop runs synchronously
  2. collector().retire(shell)   ← deferred deallocation

Other thread's try_destroy:
  1. collector().enter()         ← epoch guard
  2. destroying.load() == true   ← shell is alive (epoch protects it)
  3. return false                ← bail out
  4. guard drops                 ← epoch advances

reclaim_shell:                   ← runs after all guards advance
  drop(Box::from_raw(shell))     ← frees Mutex + Vec buffer
```

**No `Arc` is used anywhere.** The epoch guard on cold paths (~2ns) replaces
the `Arc::clone` (~5ns `fetch_add`) that was previously on every decrement.

## Panic Safety

If `T::drop` panics during `try_destroy`:

1. `catch_unwind` catches the panic
2. Shell is epoch-retired (cleanup always runs)
3. `resume_unwind` re-raises the panic

No shell leak, no counter leak, no stuck `destroying` flag.

## Performance Characteristics

| Operation | Cost (x86) | Atomic RMW? | Notes |
|---|---|---|---|
| `TlrcRef::clone` (hot) | ~3-5ns | **No** | Thread-local cache hit + bracket |
| `TlrcRef::drop` (hot) | ~3-5ns | **No** | Same + `tlrc_ptr` check |
| `TlrcRef::clone` (cold) | ~50ns | Yes (Mutex) | First clone on a new thread |
| `TlrcRef::drop` (cold) | ~50ns | Yes (Mutex) | First drop on a new thread |
| `TlrcRef::drop` (scan trigger) | ~1μs | Yes (CAS × N) | After master drops, count ≤ 0 |
| `Tlrc::drop` (no refs) | ~10ns | No | Fast path: empty counters |
| `Tlrc::drop` (with refs) | ~1μs | Yes (CAS × N) | Scanner CAS per counter |
| `TlrcRef` size | 8 bytes | — | Single pointer |

**Comparison with `Arc`:**

| | Arc | Tlrc (hot path) |
|---|---|---|
| Clone | `fetch_add` (~5ns, contended) | `load + store` (~3ns, uncontended) |
| Drop | `fetch_sub` (~5ns, contended) | `load + store` (~3ns, uncontended) |
| Multi-thread scaling | Degrades (cache-line bouncing) | Linear (per-thread counters) |
| Destruction latency | Immediate | Deferred (scan + epoch) |
| Memory overhead | 0 extra | ~24 bytes per thread per Tlrc |

## Stacked Borrows Compliance

All accesses to `TlrcInner` fields in `try_destroy` and cold-path registration
use `std::ptr::addr_of!` to avoid creating `&TlrcInner<T>` references. This
prevents Stacked Borrows violations where a shared borrow of the entire struct
would conflict with `ManuallyDrop::drop`'s mutable access to `data`.

```rust
// WRONG: creates &TlrcInner<T> → Stacked Borrows conflict with data drop
let inner = unsafe { &*inner_ptr };
inner.destroying.load(...)

// CORRECT: raw pointer field access → no retag
unsafe { &*std::ptr::addr_of!((*ptr).destroying) }.load(...)
```

Validated by Miri with `-Zmiri-preemption-rate=0.1` across 10 concurrent tests.

## Test Coverage

**50 normal tests + 10 Miri tests:**

| Category | Count | Coverage |
|---|---|---|
| Basic lifecycle | 8 | new, deref, clone, drop order, single/multi-thread |
| Stale lifecycle | 3 | address reuse, sweep pressure, master-drops-first |
| Orphan lifecycle | 3 | thread-exits-first, concurrent death, both-dead |
| Dormant counters | 3 | all-dormant, mixed active+dormant, master-first |
| Cross-thread debt | 4 | unbalanced, deep chains (3 levels), clone chain, ping-pong |
| Contention | 5 | high contention, concurrent master drop, storm, scanner interleave, simultaneous scan |
| Scale | 4 | many instances, many short-lived, thread pool, rapid lifecycle |
| Minimal | 2 | no refs, single ref, many refs single thread |
| Miri (data race) | 10 | all above categories at small scale with thread preemption |

## Known Limitations

1. **~24 bytes per thread per Tlrc:** Each thread that clones/drops a TlrcRef
   allocates a TLCounter. Freed lazily (stale detection or thread exit).

2. **Scanner blocks on active threads:** `try_destroy` spins on FROZEN counters.
   A thread in a tight bracket loop delays the scan by ~5ns per iteration.
   Bounded by bracket duration (no unbounded livelock).

3. **Cold-path epoch guard:** First clone/drop on a new thread enters an epoch
   guard (~2ns) and locks the Mutex (~20ns). Subsequent ops hit the cache.

4. **No `Weak` equivalent:** `Tlrc` does not support weak references. The
   master (`Tlrc`) must outlive or concurrently drop with all refs for
   destruction to occur.

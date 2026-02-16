# StripedArc Benchmark

Comprehensive multi-threaded benchmark suite comparing `StripedArc` against `std::Arc` across various concurrency scenarios.

## Overview

`StripedArc` is a striped reference-counted smart pointer that distributes reference counting operations across multiple cache lines (stripes) to reduce contention under high parallelism. This benchmark measures the performance characteristics and correctness of `StripedArc` compared to the standard library's `Arc`.

## Architecture

### `std::Arc`
- Uses a single atomic counter for all threads
- Highly optimized but suffers from cache line contention
- Best for single-threaded or low-contention scenarios
- One `fetch_add`/`fetch_sub` operation per clone/drop

### `StripedArc`
- Distributes counter across 512 cache lines (or `32 * num_cpus` stripes)
- Each thread operates on its own stripe (hashed from thread ID)
- Reduces cache contention but adds overhead for stripe selection
- Uses bit-packed counters: value in lower 63 bits, close flag in bit 63
- SeqCst fences and CAS-based cleanup to ensure correctness

## Running the Benchmark

```bash
# Build release version
cargo build --release --example benchmark_striped_arc

# Run the benchmark
./target/release/examples/benchmark_striped_arc
```

Expected runtime: ~2-3 minutes on a typical 16-core system.

## Benchmark Scenarios

### 1. Clone/Drop Throughput
Tests pure reference counting operations with minimal payload.

**Workload**: Each thread repeatedly clones and drops the same arc.
- **Threads**: 2, 4, 8, 16, 32
- **Operations**: 100,000 total per thread count

**When `StripedArc` wins**: High thread counts where contention dominates the cost of stripe selection.

### 2. Read-Heavy Workload
Simulates a real-world scenario where data is read frequently with occasional arc clones.

**Workload**: 
- Reader threads: continuously read data, clone every 100 operations
- Writer threads:频繁 clone, read, and drop arcs
- **Readers/Writers**: (4, 4) and (16, 4)
- **Duration**: 3 seconds each

**When `StripedArc` wins**: When the read-to-clone ratio is very high.

### 3. Burst Contention
Tests worst-case contention with synchronized bursts of activity.

**Workload**: Threads synchronize and simultaneously perform 1,000 clone-drop operations per burst.
- **Threads**: 8, 16, 32
- **Bursts**: 100 bursts
- **Operations per burst**: 1,000

**When `StripedArc` wins**: When burst size is large and threads synchronize frequently.

### 4. Cleanup Under Load
Correctness test ensuring cleanup works under high concurrency with random clone/drop patterns.

**Workload**: Threads randomly clone or drop arcs from a chain, then the master drops all original arcs.
- **Threads**: 8, 16
- **Arc depth**: 10
- **Iterations**: 100,000 operations per thread

**Expected**: No deadlocks, crashes, or double-free errors.

## Benchmark Results Interpretation

### Expected Performance Characteristics

| Scenario | `std::Arc` | `StripedArc` | Winner |
|----------|------------|--------------|--------|
| Low contention (2-4 threads) | Excellent | Good | `std::Arc` |
| Medium contention (8 threads) | Good | Good | Context-dependent |
| High contention (16+ threads) | Degraded | Excellent | `StripedArc` |
| Single clone, many reads | Best | Good | `std::Arc` |
| Many clones/drops | Good | Best | `StripedArc` |

### Why `striped.rs` is Usually Slower in Simple Benchmarks

The current implementation shows `std::Arc` performing better in most scenarios because:

1. **Hashing Overhead**: `StripedArc` computes a hash of the thread ID on every clone/drop
2. **Masking Operations**: Additional bitwise operations for stripe selection
3. **Memory Layout**: Indirect access through `counters[idx]` vs direct pointer access
4. **Ordering Costs**: `SeqCst` fences and `AcqRel` ordering add overhead
5. **False Sharing**: Despite cache padding, the stripes may still cause some cache coherence traffic

### When `StripedArc` Shines

`StripedArc` is designed for scenarios where:
- Many threads simultaneously share the same arc
- Clone/drop operations happen frequently relative to reads
- The system has sufficient physical cores (not hyperthreads)
- Cache coherence bandwidth is a bottleneck

The design targets server workloads where many worker threads share reference-counted objects without creating many clones of the same object.

## Correctness Guarantees

The benchmark includes correctness tests that verify:

1. **No Double-Free**: CAS-based cleanup ownership prevents multiple threads from freeing the same object
2. **No Memory Leaks**: `SeqCst` fences ensure all decrements are visible before cleanup
3. **Race Condition Safety**: `AcqRel` ordering on decrement ensures zero-crossing detection sees all prior increments
4. **Closed State Handling**: The close flag is encoded in the counter value using bit 63

## Implementation Details

### Counter Storage
```rust
struct Counter {
    value: AtomicI64,  // Lower 63 bits: count, Bit 63: closed flag
}
```

### Memory Ordering

| Operation | Ordering | Rationale |
|-----------|----------|-----------|
| `load()` | Acquire | Ensures we see prior updates |
| `is_closed()` | Relaxed | Flag doesn't need ordering |
| `increment()` | Release | Protects data being accessed |
| `decrement()` | AcqRel | Must see prior increments when crossing zero |
| `close()` | Release | Ensure close is visible |
| Cleanup fence | SeqCst | Global ordering across stripes |

### Stripe Selection
```rust
thread_stripe() & mask  // mask = num_stripes - 1 (power of two)
```

This uses fast bitwise AND instead of modulo, as the number of stripes is always a power of two.

## Future Optimizations

Potential improvements to make `StripedArc` more competitive:

1. **Pre-computed Thread Stripes**: Cache stripe index in thread-local storage (partially implemented)
2. **NUMA Awareness**: Bind stripes to NUMA nodes
3. **Dynamic Stripe Count**: Adjust based on contention levels
4. **Relaxed Ordering Cleanup**: Use `Acquire` instead of `SeqCst` if acceptable
5. **Fast Path Optimizations**: Special cases for single-thread scenarios

## References

- **Design**: Similar to `crossbeam::Arc` and other striped reference counting implementations
- **Inspiration**: Lock-free data structures, cache-aware algorithms
- **Comparison**: `std::sync::Arc` source code for baseline understanding

## License

This benchmark is part of the `maniac` library. See project LICENSE for details.
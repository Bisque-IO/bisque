//! Benchmark for IoContractGroup - stress tests the contract-based IO submission mechanism.
//!
//! This benchmark tests:
//! 1. Single-threaded push/pop throughput
//! 2. Multi-producer single-consumer (MPSC) throughput
//! 3. Contention under heavy load
//! 4. Waker notification overhead
//!
//! Run with: cargo run --release --example io_contract_bench

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::Waker;
use std::thread;
use std::time::{Duration, Instant};

use maniac::driver::{IoContractGroup, IoContractGroupConfig, Op, OpType};

const WARMUP_OPS: u64 = 10_000;
const BENCH_OPS: u64 = 10_000_000;

#[cfg(target_os = "linux")]
fn set_cpu_affinity(cpu: usize) -> bool {
    use std::mem;

    unsafe {
        let mut cpuset: libc::cpu_set_t = mem::zeroed();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(cpu, &mut cpuset);
        libc::pthread_setaffinity_np(libc::pthread_self(), mem::size_of_val(&cpuset), &cpuset) == 0
    }
}

#[cfg(not(target_os = "linux"))]
fn set_cpu_affinity(_cpu: usize) -> bool {
    true
}

fn main() {
    println!("=== IoContractGroup Benchmark ===\n");

    // bench_single_thread_throughput();
    // bench_push_pop_latency();
    bench_multi_producer_single_consumer();
    // bench_completion_throughput();
    // bench_high_contention();

    println!("\n=== Benchmark Complete ===");
}

/// Benchmark single-threaded push/pop throughput
fn bench_single_thread_throughput() {
    println!("--- Single-Thread Push/Pop Throughput ---");

    let group = IoContractGroup::with_capacity(65536 * 2);
    let waker = Waker::noop();

    // Warmup
    for _ in 0..WARMUP_OPS {
        let op = Op::with_params(OpType::Nop as u8, 0, 0, 0, std::ptr::null_mut(), 0);
        let id = group.push(op, &waker).expect("push failed");
        let mut bias = 0u64;
        let contract = group.pop(&mut bias).expect("pop failed");
        group.complete(contract.id(), Ok(0));
        group.release_contract(contract.id());
        assert_eq!(contract.id(), id);
    }

    // Benchmark
    let start = Instant::now();
    for _ in 0..BENCH_OPS {
        let op = Op::with_params(OpType::Nop as u8, 0, 0, 0, std::ptr::null_mut(), 0);
        let id = group.push(op, &waker).expect("push failed");
        let mut bias = 0u64;
        let contract = group.pop(&mut bias).expect("pop failed");
        group.complete(contract.id(), Ok(0));
        group.release_contract(contract.id());
        assert_eq!(contract.id(), id);
    }
    let elapsed = start.elapsed();

    let ops_per_sec = BENCH_OPS as f64 / elapsed.as_secs_f64();
    let ns_per_op = elapsed.as_nanos() as f64 / BENCH_OPS as f64;
    println!(
        "  Operations: {}, Time: {:?}, Throughput: {:.2}M ops/sec, Latency: {:.1} ns/op",
        BENCH_OPS,
        elapsed,
        ops_per_sec / 1_000_000.0,
        ns_per_op
    );
}

/// Benchmark push/pop latency with separate producer and consumer threads
fn bench_push_pop_latency() {
    println!("\n--- Push/Pop Latency (N producers, 1 consumer) ---");

    const DURATION_SECS: u64 = 2;

    for num_producers in [1, 2, 4, 8] {
        let group = Arc::new(IoContractGroup::with_capacity(4096 * 16));
        let stop = Arc::new(AtomicBool::new(false));
        let pushed = Arc::new(AtomicU64::new(0));
        let popped = Arc::new(AtomicU64::new(0));

        // Spawn producer threads
        let mut producers = Vec::new();
        for _ in 0..num_producers {
            let producer_group = Arc::clone(&group);
            let producer_stop = Arc::clone(&stop);
            let producer_pushed = Arc::clone(&pushed);
            let handle = thread::spawn(move || {
                let waker = Waker::noop();
                let mut local_pushed = 0u64;

                while !producer_stop.load(Ordering::Relaxed) {
                    let op = Op::with_params(OpType::Nop as u8, 0, 0, 0, std::ptr::null_mut(), 0);
                    if producer_group.push(op, &waker).is_ok() {
                        local_pushed += 1;
                    } else {
                        thread::yield_now();
                    }
                }

                producer_pushed.fetch_add(local_pushed, Ordering::Relaxed);
            });
            producers.push(handle);
        }

        // Consumer thread - constantly popping
        let consumer_group = Arc::clone(&group);
        let consumer_stop = Arc::clone(&stop);
        let consumer_popped = Arc::clone(&popped);
        let consumer = thread::spawn(move || {
            let mut local_popped = 0u64;
            let mut bias = 0u64;

            while !consumer_stop.load(Ordering::Relaxed) {
                if let Some(contract) = consumer_group.pop(&mut bias) {
                    let id = contract.id();
                    consumer_group.complete(id, Ok(0));
                    consumer_group.release_contract(id);
                    local_popped += 1;
                } else {
                    thread::yield_now();
                }
            }

            // Drain remaining
            while let Some(contract) = consumer_group.pop(&mut bias) {
                let id = contract.id();
                consumer_group.complete(id, Ok(0));
                consumer_group.release_contract(id);
                local_popped += 1;
            }

            consumer_popped.fetch_add(local_popped, Ordering::Relaxed);
        });

        // Let them run
        let start = Instant::now();
        thread::sleep(Duration::from_secs(DURATION_SECS));
        stop.store(true, Ordering::Relaxed);

        for p in producers {
            p.join().unwrap();
        }
        consumer.join().unwrap();

        let elapsed = start.elapsed();
        let total_pushed = pushed.load(Ordering::Relaxed);
        let total_popped = popped.load(Ordering::Relaxed);

        let push_ns_per_op = elapsed.as_nanos() as f64 / total_pushed as f64;
        let pop_ns_per_op = elapsed.as_nanos() as f64 / total_popped as f64;

        println!(
            "  {} producer(s): push {:.1} ns/op, pop {:.1} ns/op, {:.2}M ops/sec",
            num_producers,
            push_ns_per_op,
            pop_ns_per_op,
            total_popped as f64 / elapsed.as_secs_f64() / 1_000_000.0
        );
    }
}

/// Benchmark multi-producer single-consumer throughput
fn bench_multi_producer_single_consumer() {
    println!("\n--- Multi-Producer Single-Consumer ---");

    for num_producers in [1, 2, 4, 8] {
        let group = Arc::new(IoContractGroup::with_capacity(65536 * 2));
        let stop = Arc::new(AtomicBool::new(false));
        let total_pushed = Arc::new(AtomicU64::new(0));
        let total_popped = Arc::new(AtomicU64::new(0));

        let ops_per_producer = BENCH_OPS / num_producers as u64;

        // Spawn producers
        let mut producer_handles = Vec::new();
        for thread_id in 0..num_producers {
            let group = Arc::clone(&group);
            let total_pushed = Arc::clone(&total_pushed);
            let stop = Arc::clone(&stop);

            let handle = thread::spawn(move || {
                set_cpu_affinity(thread_id + 1);
                let waker = Waker::noop();
                let mut pushed = 0u64;

                while pushed < ops_per_producer && !stop.load(Ordering::Relaxed) {
                    let op = Op::with_params(OpType::Nop as u8, 0, 0, 0, std::ptr::null_mut(), 0);
                    match group.push(op, &waker) {
                        Ok(_) => {
                            pushed += 1;
                        }
                        Err(()) => {
                            // Queue full, yield and retry
                            thread::yield_now();
                        }
                    }
                }

                total_pushed.fetch_add(pushed, Ordering::Relaxed);
            });
            producer_handles.push(handle);
        }

        set_cpu_affinity(0);

        // Consumer in main thread
        let start = Instant::now();
        let mut popped = 0u64;
        let mut bias = 0u64;

        while popped < BENCH_OPS {
            if let Some(contract) = group.pop(&mut bias) {
                let id = contract.id();
                group.complete(id, Ok(0));
                group.release_contract(id);
                popped += 1;
            } else {
                // No work, check if producers are done
                if producer_handles.iter().all(|h| h.is_finished()) {
                    break;
                }
                thread::yield_now();
            }
        }

        stop.store(true, Ordering::Relaxed);
        for handle in producer_handles {
            handle.join().unwrap();
        }

        let elapsed = start.elapsed();
        total_popped.fetch_add(popped, Ordering::Relaxed);

        let ops_per_sec = popped as f64 / elapsed.as_secs_f64();
        println!(
            "  {} producer(s): {:.2}M ops/sec ({} ops in {:?})",
            num_producers,
            ops_per_sec / 1_000_000.0,
            popped,
            elapsed
        );
    }
}

/// Benchmark completion notification throughput
fn bench_completion_throughput() {
    println!("\n--- Completion Throughput ---");

    let group = IoContractGroup::with_capacity(65536);
    let waker = Waker::noop();

    // Pre-fill with operations
    let mut ids = Vec::with_capacity(1000);
    for _ in 0..1000 {
        let op = Op::with_params(OpType::Nop as u8, 0, 0, 0, std::ptr::null_mut(), 0);
        let id = group.push(op, &waker).expect("push failed");
        ids.push(id);
    }

    // Pop all
    let mut contracts = Vec::with_capacity(1000);
    let mut bias = 0u64;
    while let Some(contract) = group.pop(&mut bias) {
        contracts.push(contract.id());
    }

    // Benchmark completion
    let start = Instant::now();
    for _ in 0..1000 {
        // Complete all and release
        for &id in &contracts {
            group.complete(id, Ok(42));
            group.release_contract(id);
        }

        // Re-push and re-pop
        ids.clear();
        for _ in 0..1000 {
            let op = Op::with_params(OpType::Nop as u8, 0, 0, 0, std::ptr::null_mut(), 0);
            let id = group.push(op, &waker).expect("push failed");
            ids.push(id);
        }

        contracts.clear();
        while let Some(contract) = group.pop(&mut bias) {
            contracts.push(contract.id());
        }
    }
    let elapsed = start.elapsed();

    let total_ops = 1000 * 1000 * 3; // push + pop + complete
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    println!(
        "  {} cycles (push+pop+complete): {:?}, {:.2}M ops/sec",
        1000,
        elapsed,
        ops_per_sec / 1_000_000.0
    );
}

/// Benchmark high contention scenario
fn bench_high_contention() {
    println!("\n--- High Contention (MPMC-like) ---");

    for num_threads in [2, 4, 8, 14] {
        let group = Arc::new(IoContractGroup::with_config(IoContractGroupConfig {
            capacity: 65536 * 16,
            force_linear_scan: false,
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let total_ops = Arc::new(AtomicU64::new(0));

        let duration = Duration::from_secs(2);

        // Each thread does push, pop, complete cycles
        let mut handles = Vec::new();
        for _ in 0..num_threads {
            let group = Arc::clone(&group);
            let stop = Arc::clone(&stop);
            let total_ops = Arc::clone(&total_ops);

            let handle = thread::spawn(move || {
                let waker = Waker::noop();
                let mut local_ops = 0u64;
                let mut bias = 0u64;

                while !stop.load(Ordering::Relaxed) {
                    // Try to push
                    let op = Op::with_params(OpType::Nop as u8, 0, 0, 0, std::ptr::null_mut(), 0);
                    if group.push(op, &waker).is_ok() {
                        local_ops += 1;
                    }

                    // Try to pop and complete
                    if let Some(contract) = group.pop(&mut bias) {
                        let id = contract.id();
                        group.complete(id, Ok(0));
                        group.release_contract(id);
                        local_ops += 1;
                    }
                }

                total_ops.fetch_add(local_ops, Ordering::Relaxed);
            });
            handles.push(handle);
        }

        thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);

        for handle in handles {
            handle.join().unwrap();
        }

        let ops = total_ops.load(Ordering::Relaxed);
        let ops_per_sec = ops as f64 / duration.as_secs_f64();
        println!(
            "  {} threads: {:.2}M ops/sec ({} total ops)",
            num_threads,
            ops_per_sec / 1_000_000.0,
            ops
        );
    }
}

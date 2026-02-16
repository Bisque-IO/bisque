//! Benchmark for wasmtime sync host call overhead in both directions.
//!
//! This benchmark measures the latency of:
//! - Host → Wasm sync calls (calling a wasm function from sync host code)
//! - Wasm → Host sync calls (calling a sync host function from wasm)
//!
//! Run with: cargo run --example wasmtime_sync --release

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

const ITERATIONS: usize = 10_000_000;
const WARMUP_ITERATIONS: usize = 1_000;

/// Wasm module source in WAT format
const WAT_SOURCE: &str = r#"
(module
    ;; Import a host function that does nothing
    (import "bench" "host_func" (func $host_func))

    ;; Exported function that calls the host function
    (func (export "call_host") (call $host_func))

    ;; Exported function that loops and calls host function N times
    (func (export "loop_call_host") (param $count i32)
        (local $i i32)
        (local.set $i (i32.const 0))
        (loop $loop
            (call $host_func)
            (local.get $i)
            (i32.const 1)
            (i32.add)
            (local.tee $i)
            (local.get $count)
            (i32.lt_u)
            (br_if $loop)
        )
    )

    ;; Simple exported function that returns 42
    (func (export "return_42") (result i32) (i32.const 42))

    ;; Exported function that adds two numbers
    (func (export "compute_sum") (param $a i32) (param $b i32) (result i32)
        (local.get $a)
        (local.get $b)
        (i32.add)
    )
)
"#;

fn main() {
    println!("=== Wasmtime Sync Host Call Benchmark ===\n");
    println!("Iterations per benchmark: {}", ITERATIONS);
    println!("Warmup iterations: {}\n", WARMUP_ITERATIONS);

    let mut config = wasmtime::Config::new();
    config.epoch_interruption(true);
    config.cranelift_opt_level(wasmtime::OptLevel::Speed);

    // Create wasmtime engine
    let engine = wasmtime::Engine::new(&config).unwrap();

    // Parse WAT to WASM binary
    let wasm_bytes = match wat::parse_str(WAT_SOURCE) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("Failed to parse WAT: {}", e);
            return;
        }
    };
    let module = wasmtime::Module::new(&engine, &wasm_bytes).unwrap();

    // Create a counter for host function invocations
    let call_count = Arc::new(Mutex::new(0u64));

    // Create the store
    let mut store = wasmtime::Store::new(&engine, ());
    store.set_epoch_deadline(10);

    // Create linker and define the sync host function
    let mut linker = wasmtime::Linker::new(&engine);
    let call_count_clone = call_count.clone();
    linker
        .func_wrap("bench", "host_func", move || {
            // *call_count_clone.lock().unwrap() += 1;
        })
        .expect("Failed to define host_func");

    // Instantiate the module
    let instance = linker
        .instantiate(&mut store, &module)
        .expect("Failed to instantiate");

    // Get typed function references
    let return_42: wasmtime::TypedFunc<(), i32> = instance
        .get_typed_func(&mut store, "return_42")
        .expect("Failed to get return_42");
    let compute_sum: wasmtime::TypedFunc<(i32, i32), i32> = instance
        .get_typed_func(&mut store, "compute_sum")
        .expect("Failed to get compute_sum");
    let call_host: wasmtime::TypedFunc<(), ()> = instance
        .get_typed_func(&mut store, "call_host")
        .expect("Failed to get call_host");
    let loop_call_host: wasmtime::TypedFunc<i32, ()> = instance
        .get_typed_func(&mut store, "loop_call_host")
        .expect("Failed to get loop_call_host");

    // Warmup
    println!("Warming up...");
    for _ in 0..WARMUP_ITERATIONS {
        return_42.call(&mut store, ()).expect("warmup failed");
        compute_sum
            .call(&mut store, (10, 20))
            .expect("warmup failed");
        call_host.call(&mut store, ()).expect("warmup failed");
        loop_call_host.call(&mut store, 10).expect("warmup failed");
    }
    *call_count.lock().unwrap() = 0;
    println!("Warmup complete.\n");

    // Benchmark 1: Wasm → Host sync call
    println!("Benchmark 1: Wasm → Host sync call");
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        call_host.call(&mut store, ()).expect("call_host failed");
    }
    let elapsed = start.elapsed();
    let nanos_per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    let host_calls = *call_count.lock().unwrap();
    println!("  Total time:         {:?}", elapsed);
    println!("  Host calls made:    {}", host_calls);
    println!("  Per operation:      {:.2} ns/op", nanos_per_op);
    println!(
        "  Ops/sec:            {:.0}",
        ITERATIONS as f64 / elapsed.as_secs_f64()
    );
    println!();

    // Benchmark 2: Host → Wasm sync call (return_42)
    println!("Benchmark 2: Host → Wasm sync call (return_42)");
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        return_42.call(&mut store, ()).expect("return_42 failed");
    }
    let elapsed = start.elapsed();
    let nanos_per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    println!("  Total time:    {:?}", elapsed);
    println!("  Per operation: {:.2} ns/op", nanos_per_op);
    println!(
        "  Ops/sec:       {:.0}",
        ITERATIONS as f64 / elapsed.as_secs_f64()
    );
    println!();

    // Benchmark 3: Host → Wasm sync call with parameters
    println!("Benchmark 3: Host → Wasm sync call (compute_sum with params)");
    let start = Instant::now();
    for i in 0..ITERATIONS {
        compute_sum
            .call(&mut store, (i as i32, (i + 1) as i32))
            .expect("compute_sum failed");
    }
    let elapsed = start.elapsed();
    let nanos_per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    println!("  Total time:    {:?}", elapsed);
    println!("  Per operation: {:.2} ns/op", nanos_per_op);
    println!(
        "  Ops/sec:       {:.0}",
        ITERATIONS as f64 / elapsed.as_secs_f64()
    );
    println!();

    // Benchmark 4: Wasm → Host sync call (wasm-side loop)
    println!("Benchmark 4: Wasm → Host sync call (wasm-side loop)");
    println!(
        "  Calling wasm function once, but it loops {} times internally",
        ITERATIONS
    );
    for _ in 0..1 {
        let start = Instant::now();
        loop_call_host
            .call(&mut store, ITERATIONS as i32)
            .expect("loop_call_host failed");
        let elapsed = start.elapsed();
        let nanos_per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;
        let host_calls = *call_count.lock().unwrap();
        println!("  Total time:         {:?}", elapsed);
        println!("  Host calls made:    {}", host_calls);
        println!("  Per operation:      {:.2} ns/op", nanos_per_op);
        println!(
            "  Ops/sec:            {:.0}",
            ITERATIONS as f64 / elapsed.as_secs_f64()
        );
        println!();
    }

    println!("=== Benchmark Complete ===");
    println!("\nThis benchmark measures synchronous host call overhead.");
    println!("Compare these results with the async benchmark to understand");
    println!("the performance impact of async/await in the wasmtime runtime.");
}

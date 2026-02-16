//! Benchmark for wasmtime async host call overhead in both directions.
//!
//! This benchmark measures the latency of:
//! - Host → Wasm async calls (calling a wasm function from async host code)
//! - Wasm → Host async calls (calling an async host function from wasm)
//!
//! Run with: cargo run --example wasmtime_async --release

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use wasmtime::UpdateDeadline;

const ITERATIONS: usize = 10_000_000;
const WARMUP_ITERATIONS: usize = 1_000;

/// Wasm module source in WAT format
const WAT_SOURCE: &str = r#"
(module
    ;; Import a host function that does nothing (can be async from WASM perspective)
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

#[tokio::main]
async fn main() {
    println!("=== Wasmtime Async Host Call Benchmark ===\n");
    println!("Iterations per benchmark: {}", ITERATIONS);
    println!("Warmup iterations: {}\n", WARMUP_ITERATIONS);

    let mut config = wasmtime::Config::new();
    config.async_support(true);
    config.cranelift_opt_level(wasmtime::OptLevel::Speed);
    config.epoch_interruption(true);
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

    // Create the async store
    let mut store = wasmtime::Store::new(&engine, ());
    store.set_epoch_deadline(10);
    store.epoch_deadline_callback(|_ctx| {
        println!("Epoch deadline reached!");
        Ok(UpdateDeadline::Continue(1))
    });

    // Create linker and define the async host function
    let mut linker = wasmtime::Linker::new(&engine);
    let call_count_clone = call_count.clone();
    linker
        .func_wrap_async("bench", "host_func", move |_caller, (): ()| {
            let call_count_clone = call_count_clone.clone();
            Box::new(async move {
                *call_count_clone.lock().unwrap() += 1;
                // Simulate a minimal async operation
                // tokio::task::yield_now().await;
            })
        })
        .expect("Failed to define host_func");

    // Instantiate the module
    let instance = linker
        .instantiate_async(&mut store, &module)
        .await
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
        return_42
            .call_async(&mut store, ())
            .await
            .expect("warmup failed");
        compute_sum
            .call_async(&mut store, (10, 20))
            .await
            .expect("warmup failed");
        call_host
            .call_async(&mut store, ())
            .await
            .expect("warmup failed");
        loop_call_host
            .call_async(&mut store, 10)
            .await
            .expect("warmup failed");
    }
    *call_count.lock().unwrap() = 0;
    println!("Warmup complete.\n");

    // Benchmark 1: Wasm → Host async call
    println!("Benchmark 1: Wasm → Host async call");
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        call_host
            .call_async(&mut store, ())
            .await
            .expect("call_host failed");
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

    // Benchmark 2: Host → Wasm async call (return_42)
    println!("Benchmark 2: Host → Wasm async call (return_42)");
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        return_42
            .call_async(&mut store, ())
            .await
            .expect("return_42 failed");
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

    // Benchmark 3: Host → Wasm async call with parameters
    println!("Benchmark 3: Host → Wasm async call (compute_sum with params)");
    let start = Instant::now();
    for i in 0..ITERATIONS {
        compute_sum
            .call_async(&mut store, (i as i32, (i + 1) as i32))
            .await
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

    // Benchmark 4: Wasm → Host async call (wasm-side loop)
    println!("Benchmark 4: Wasm → Host async call (wasm-side loop)");
    println!(
        "  Calling wasm function once, but it loops {} times internally",
        ITERATIONS
    );
    let start = Instant::now();
    loop_call_host
        .call_async(&mut store, ITERATIONS as i32)
        .await
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

    println!("=== Benchmark Complete ===");
    println!("\nNote: This benchmark measures async host call overhead.");
    println!("The Wasm → Host benchmark includes a minimal tokio::yield_now() to");
    println!("simulate async work. For pure function call overhead, consider");
    println!("removing the yield_now() or using synchronous benchmarks.");
}

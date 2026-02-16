//! Stress test for the scheduler to reproduce race conditions.
//!
//! This simulates the tcp_echo pattern that can cause hangs:
//! 1. Uses poll_next() (non-blocking) with yield, like tcp_echo
//! 2. Dynamically spawns tasks during execution (nested spawns)
//! 3. External "IO" threads wake tasks via wakers
//! 4. Multiple scheduler threads competing for work
//!
//! Run with: cargo run --example scheduler_stress
//!
//! Configuration via environment variables:
//!   WAKER_THREADS=N   - Number of background waker threads (default: 2)
//!   SCHED_THREADS=N   - Number of scheduler threads (default: 2)
//!   INITIAL_TASKS=N   - Number of initial tasks (default: 8)
//!   SPAWNED_PER=N     - Tasks spawned per initial task (default: 4)
//!   ITERATIONS=N      - Iterations per task (default: 50)
//!   RUNS=N            - Number of test runs (default: 100)
//!   TIMEOUT=N         - Timeout in seconds (default: 5)
//!
//! Examples:
//!   WAKER_THREADS=0 cargo run --example scheduler_stress  # No external wakers
//!   WAKER_THREADS=4 cargo run --example scheduler_stress  # 4 waker threads
//!   WAKER_THREADS=1 SCHED_THREADS=1 cargo run --example scheduler_stress

use atomic_waker::AtomicWaker;
use maniac::scheduler::{BiasState, ContractGroup};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::task::Poll;
use std::thread;
use std::time::{Duration, Instant};

fn get_env_or(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

// Global debug counters
static POLL_NEXT_CALLS: AtomicUsize = AtomicUsize::new(0);
static POLL_NEXT_FOUND: AtomicUsize = AtomicUsize::new(0);
static WAKER_WAKE_CALLS: AtomicUsize = AtomicUsize::new(0);
static WAKER_REGISTER_CALLS: AtomicUsize = AtomicUsize::new(0);

fn reset_counters() {
    POLL_NEXT_CALLS.store(0, Ordering::Relaxed);
    POLL_NEXT_FOUND.store(0, Ordering::Relaxed);
    WAKER_WAKE_CALLS.store(0, Ordering::Relaxed);
    WAKER_REGISTER_CALLS.store(0, Ordering::Relaxed);
}

fn print_counters() {
    eprintln!(
        "  poll_next() calls: {}",
        POLL_NEXT_CALLS.load(Ordering::Relaxed)
    );
    eprintln!(
        "    found work: {}",
        POLL_NEXT_FOUND.load(Ordering::Relaxed)
    );
    eprintln!(
        "  waker.register() calls: {}",
        WAKER_REGISTER_CALLS.load(Ordering::Relaxed)
    );
    eprintln!(
        "  waker.wake() calls: {}",
        WAKER_WAKE_CALLS.load(Ordering::Relaxed)
    );
}

fn main() {
    let num_waker_threads = get_env_or("WAKER_THREADS", 2);
    let num_scheduler_threads = get_env_or("SCHED_THREADS", 2);
    let num_initial_tasks = get_env_or("INITIAL_TASKS", 8);
    let num_spawned_per_task = get_env_or("SPAWNED_PER", 4);
    let iterations_per_task = get_env_or("ITERATIONS", 50);
    let num_runs = get_env_or("RUNS", 100);
    let timeout_secs = get_env_or("TIMEOUT", 5) as u64;

    let config = TestConfig {
        num_waker_threads,
        num_scheduler_threads,
        num_initial_tasks,
        num_spawned_per_task,
        iterations_per_task,
        timeout_secs,
    };

    println!("=== Scheduler Stress Test ===");
    println!("Configuration:");
    println!("  Waker threads:     {}", num_waker_threads);
    println!("  Scheduler threads: {}", num_scheduler_threads);
    println!("  Initial tasks:     {}", num_initial_tasks);
    println!("  Spawned per task:  {}", num_spawned_per_task);
    println!("  Iterations/task:   {}", iterations_per_task);
    println!("  Timeout:           {}s", timeout_secs);
    println!("  Runs:              {}", num_runs);
    println!();

    for run in 1..=num_runs {
        print!("Run {}: ", run);
        reset_counters();
        if !run_test(&config) {
            println!("FAILED (hang detected)");
            std::process::exit(1);
        }
        println!("OK");
    }
    println!("All {} runs passed!", num_runs);
}

struct TestConfig {
    num_waker_threads: usize,
    num_scheduler_threads: usize,
    num_initial_tasks: usize,
    num_spawned_per_task: usize,
    iterations_per_task: usize,
    timeout_secs: u64,
}

// Per-task state tracking
struct TaskState {
    iteration: AtomicUsize,
    state: AtomicU64, // 0=not_started, 1=running, 2=pending, 3=completed
    waker: AtomicWaker,
}

impl TaskState {
    fn new() -> Self {
        Self {
            iteration: AtomicUsize::new(0),
            state: AtomicU64::new(0),
            waker: AtomicWaker::new(),
        }
    }
}

fn run_test(config: &TestConfig) -> bool {
    let total_tasks =
        config.num_initial_tasks + (config.num_initial_tasks * config.num_spawned_per_task);
    let group = Arc::new(ContractGroup::new_blocking());
    let completed = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    // Per-task state for debugging and waker storage
    let task_states: Arc<Vec<TaskState>> =
        Arc::new((0..total_tasks).map(|_| TaskState::new()).collect());

    let next_task_id = Arc::new(AtomicUsize::new(config.num_initial_tasks));
    let iterations_per_task = config.iterations_per_task;
    let num_spawned_per_task = config.num_spawned_per_task;

    // Spawn initial tasks
    for i in 0..config.num_initial_tasks {
        let group_clone = group.clone();
        let completed = completed.clone();
        let next_task_id = next_task_id.clone();
        let task_states = task_states.clone();

        group
            .spawn(
                async move {
                    // Spawn child tasks
                    for _ in 0..num_spawned_per_task {
                        let task_id = next_task_id.fetch_add(1, Ordering::Relaxed);
                        let completed = completed.clone();
                        let task_states = task_states.clone();

                        group_clone
                            .spawn(
                                async move {
                                    run_worker_task(
                                        task_id,
                                        completed,
                                        task_states,
                                        iterations_per_task,
                                    )
                                    .await;
                                },
                                true,
                            )
                            .expect("Failed to spawn child task");
                    }

                    run_worker_task(i, completed, task_states, iterations_per_task).await;
                },
                true,
            )
            .expect("Failed to spawn initial task");
    }

    // Scheduler threads
    let mut handles = vec![];
    for _ in 0..config.num_scheduler_threads {
        let group = group.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut bias = BiasState::new();
            while !stop.load(Ordering::Relaxed) {
                POLL_NEXT_CALLS.fetch_add(1, Ordering::Relaxed);
                if group.poll_next(&mut bias).is_some() {
                    POLL_NEXT_FOUND.fetch_add(1, Ordering::Relaxed);
                } else {
                    thread::yield_now();
                }
            }
        }));
    }

    // Waker threads - simulate IO completion by waking tasks
    // Uses AtomicWaker just like the real IO driver
    for waker_thread_id in 0..config.num_waker_threads {
        let task_states = task_states.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut idx = waker_thread_id; // Start at different offsets
            while !stop.load(Ordering::Relaxed) {
                // Wake one task per iteration, like real IO completion
                let len = task_states.len();
                if len > 0 {
                    idx = (idx + 1) % len;
                    // AtomicWaker::wake() takes the waker and wakes it if present
                    task_states[idx].waker.wake();
                    WAKER_WAKE_CALLS.fetch_add(1, Ordering::Relaxed);
                }
                // Small delay to simulate IO latency
                std::hint::spin_loop();
            }
        }));
    }

    // Wait for completion
    let start = Instant::now();
    let success = loop {
        let done = completed.load(Ordering::Relaxed);
        if done >= total_tasks {
            break true;
        }
        if start.elapsed() > Duration::from_secs(config.timeout_secs) {
            eprintln!("\n=== TIMEOUT: Completed {}/{} ===", done, total_tasks);
            print_counters();

            // Show stuck tasks
            eprintln!("\nStuck tasks:");
            for (i, ts) in task_states.iter().enumerate().take(total_tasks) {
                let iter = ts.iteration.load(Ordering::Relaxed);
                let state = ts.state.load(Ordering::Relaxed);
                if state != 3 {
                    // not completed
                    let state_str = match state {
                        0 => "not_started",
                        1 => "running",
                        2 => "pending",
                        3 => "completed",
                        _ => "unknown",
                    };
                    eprintln!(
                        "  Task {}: state={}, iter={}/{}",
                        i, state_str, iter, iterations_per_task
                    );
                }
            }

            break false;
        }
        thread::sleep(Duration::from_millis(10));
    };

    stop.store(true, Ordering::Relaxed);
    group.stop();
    for handle in handles {
        let _ = handle.join();
    }

    success
}

async fn run_worker_task(
    task_id: usize,
    completed: Arc<AtomicUsize>,
    task_states: Arc<Vec<TaskState>>,
    iterations_per_task: usize,
) {
    task_states[task_id].state.store(1, Ordering::Relaxed); // running

    for iter in 0..iterations_per_task {
        task_states[task_id]
            .iteration
            .store(iter, Ordering::Relaxed);

        let yielded = Arc::new(AtomicBool::new(false));
        let yielded_clone = yielded.clone();
        let task_states_clone = task_states.clone();

        std::future::poll_fn(move |cx| {
            if yielded_clone.swap(true, Ordering::Relaxed) {
                task_states_clone[task_id].state.store(1, Ordering::Relaxed); // back to running
                Poll::Ready(())
            } else {
                task_states_clone[task_id].state.store(2, Ordering::Relaxed); // pending
                // Register waker using AtomicWaker - lock-free!
                task_states_clone[task_id].waker.register(cx.waker());
                WAKER_REGISTER_CALLS.fetch_add(1, Ordering::Relaxed);
                Poll::<()>::Pending
            }
        })
        .await;
    }

    task_states[task_id]
        .iteration
        .store(iterations_per_task, Ordering::Relaxed);
    task_states[task_id].state.store(3, Ordering::Relaxed); // completed
    completed.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

fn run_serialized<F: FnOnce()>(f: F) {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let mutex = LOCK.get_or_init(|| Mutex::new(()));
    let _guard = match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    f();
}

#[test]
// #[cfg(not(miri))]
fn test_preemption_interruption() {
    run_serialized(|| {
        // This test verifies that a thread running a generator loop can be preempted.
        // 1. Spawn a worker thread that initializes preemption and runs a generator.
        // 2. The generator sends its WorkerThreadHandle back to main thread.
        // 3. The generator enters a tight loop.
        // 4. Main thread interrupts the worker.
        // 5. Generator yields, worker thread resumes, marks shutdown, and cleanly finishes.

        let flag = Arc::new(AtomicBool::new(false));
        let should_exit = Arc::new(AtomicBool::new(false));

        let (tx, rx) = std::sync::mpsc::channel();

        let worker_thread = {
            let flag_clone = Arc::clone(&flag);
            let exit_signal = Arc::clone(&should_exit);
            thread::spawn(move || {
                let _preemption_guard = crate::runtime::preemption::init_worker_preemption()
                    .expect("Failed to install preemption handler");

                let mut generator = {
                    let exit_signal = Arc::clone(&exit_signal);
                    let flag_for_handle = Arc::clone(&flag_clone);
                    crate::generator::Gn::<()>::new_scoped(move |mut scope| {
                        let scope_ptr = &mut scope as *mut _ as *mut ();
                        crate::runtime::preemption::set_generator_scope(scope_ptr);

                        // Send our handle to the interrupter
                        #[cfg(unix)]
                        let handle =
                            crate::runtime::preemption::WorkerThreadHandle::current().unwrap();
                        #[cfg(windows)]
                        let handle = crate::runtime::preemption::WorkerThreadHandle::current(
                            &flag_for_handle,
                        )
                        .unwrap();

                        tx.send(handle).unwrap();

                        // Busy loop waiting for preemption
                        let mut i = 0u64;
                        loop {
                            std::hint::black_box(i);
                            i = i.wrapping_add(1);

                            if exit_signal.load(Ordering::Acquire) {
                                crate::runtime::preemption::clear_generator_scope();
                                return crate::generator::done();
                            }
                        }
                    })
                };

                // First resume: blocks until preemption triggers scope.yield_(Preempted).
                use crate::runtime::preemption::GeneratorYieldReason;

                let result = generator.resume();
                let reason = result
                    .and_then(GeneratorYieldReason::from_usize)
                    .expect("generator should yield a structured reason");
                assert_eq!(
                    reason,
                    GeneratorYieldReason::Preempted,
                    "preemption should yield exactly once"
                );

                // Tell the generator to exit and resume it to completion.
                exit_signal.store(true, Ordering::Release);
                let final_result = generator.resume();
                assert!(
                    final_result.is_none(),
                    "generator should finish after exit flag"
                );
            })
        };

        // Receive handle
        let handle = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Worker timed out");

        // Wait a bit for worker to be in loop
        thread::sleep(Duration::from_millis(50));

        // Interrupt
        handle.interrupt().expect("Failed to interrupt worker");

        // Wait for worker to finish (which means it yielded)
        // If preemption fails, this join will hang/timeout.
        worker_thread.join().expect("Worker thread panicked");
    });
}

#[test]
fn test_multiple_generator_preemptions_on_same_thread() {
    run_serialized(|| {
        #[derive(Debug)]
        enum WorkerEvent {
            GeneratorReady(usize),
            GeneratorFinished(usize),
        }

        let preempt_flag = Arc::new(AtomicBool::new(false));
        let (handle_tx, handle_rx) = std::sync::mpsc::channel();
        let (event_tx, event_rx) = std::sync::mpsc::channel();

        let worker = {
            let flag_clone = Arc::clone(&preempt_flag);
            thread::spawn(move || {
                let _guard =
                    crate::runtime::preemption::init_worker_preemption().expect("install handler");

                #[cfg(unix)]
                let handle = crate::runtime::preemption::WorkerThreadHandle::current()
                    .expect("obtain unix handle");
                #[cfg(windows)]
                let handle = crate::runtime::preemption::WorkerThreadHandle::current(&flag_clone)
                    .expect("obtain windows handle");
                handle_tx.send(handle).unwrap();

                use crate::runtime::preemption::GeneratorYieldReason;

                for gen_id in 0..2 {
                    // println!("[Test] Starting generator {}", gen_id);
                    let exit_signal = Arc::new(AtomicBool::new(false));
                    let exit_clone = Arc::clone(&exit_signal);
                    let events = event_tx.clone();

                    let mut generator = crate::generator::Gn::<()>::new_scoped(move |mut scope| {
                        crate::runtime::preemption::set_generator_scope(
                            &mut scope as *mut _ as *mut (),
                        );
                        // println!("[Gen {}] Scope set, notifying ready", gen_id);
                        events
                            .send(WorkerEvent::GeneratorReady(gen_id))
                            .expect("notify ready");

                        let mut spin = 0u64;
                        loop {
                            std::hint::black_box(spin);
                            spin = spin.wrapping_add(1);

                            if exit_clone.load(Ordering::Acquire) {
                                // println!("[Gen {}] Exit signal received", gen_id);
                                crate::runtime::preemption::clear_generator_scope();
                                return crate::generator::done();
                                // return 0;
                            }
                        }
                    });

                    // println!("[Test] Resuming generator {}", gen_id);
                    let reason = generator
                        .resume()
                        .and_then(GeneratorYieldReason::from_usize)
                        .expect("generator should yield");
                    assert_eq!(reason, GeneratorYieldReason::Preempted);
                    // println!("[Test] Generator {} preempted", gen_id);

                    exit_signal.store(true, Ordering::Release);
                    assert!(
                        generator.resume().is_none(),
                        "generator should exit after exit flag"
                    );
                    // println!("[Test] Generator {} finished", gen_id);
                    event_tx
                        .send(WorkerEvent::GeneratorFinished(gen_id))
                        .expect("notify finished");
                }
            })
        };

        let handle = handle_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("handle unavailable");

        for gen_id in 0..2 {
            match event_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("missing ready event")
            {
                WorkerEvent::GeneratorReady(id) => assert_eq!(id, gen_id),
                WorkerEvent::GeneratorFinished(id) => {
                    panic!("generator {id} finished before being preempted")
                }
            }

            handle.interrupt().expect("preemption interrupt failed");

            match event_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("missing finished event")
            {
                WorkerEvent::GeneratorFinished(id) => assert_eq!(id, gen_id),
                WorkerEvent::GeneratorReady(id) => {
                    panic!("unexpected ready event for generator {id}")
                }
            }
        }

        worker.join().expect("worker panicked");
    });
}

#[test]
fn test_preemption_with_mutex() {
    run_serialized(|| {
        // This test ensures that if a generator holds a Mutex when preempted,
        // the lock remains held and valid across the context switch.
        // This verifies that the trampoline/yield mechanism preserves
        // the thread state correctly and doesn't corrupt locks.

        let preempt_flag = Arc::new(AtomicBool::new(false));
        let lock = Arc::new(Mutex::new(0));
        let (handle_tx, handle_rx) = std::sync::mpsc::channel();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();

        let worker = {
            let flag_clone = Arc::clone(&preempt_flag);
            let lock_clone = Arc::clone(&lock);
            thread::spawn(move || {
                let _guard =
                    crate::runtime::preemption::init_worker_preemption().expect("install handler");

                #[cfg(unix)]
                let handle = crate::runtime::preemption::WorkerThreadHandle::current()
                    .expect("obtain unix handle");
                #[cfg(windows)]
                let handle = crate::runtime::preemption::WorkerThreadHandle::current(&flag_clone)
                    .expect("obtain windows handle");
                handle_tx.send(handle).unwrap();

                let exit_signal = Arc::new(AtomicBool::new(false));
                let exit_clone = Arc::clone(&exit_signal);

                let mut generator = crate::generator::Gn::<()>::new_scoped(move |mut scope| {
                    crate::runtime::preemption::set_generator_scope(
                        &mut scope as *mut _ as *mut (),
                    );

                    // Wrap in catch_unwind to ensure clean stack behavior
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        // Lock the mutex inside the generator
                        let _guard = lock_clone.lock().unwrap();
                        ready_tx.send(()).expect("notify ready");

                        // Busy loop while holding the lock
                        let mut spin = 0u64;
                        loop {
                            std::hint::black_box(spin);
                            spin = spin.wrapping_add(1);

                            if exit_clone.load(Ordering::Acquire) {
                                crate::runtime::preemption::clear_generator_scope();
                                return;
                            }
                        }
                    }));

                    crate::generator::done()
                });

                use crate::runtime::preemption::GeneratorYieldReason;

                // Run generator until it signals readiness (locked mutex) and then yields/preempts
                let reason = generator
                    .resume()
                    .and_then(GeneratorYieldReason::from_usize)
                    .expect("generator should yield");

                assert_eq!(
                    reason,
                    GeneratorYieldReason::Preempted,
                    "Should be preempted"
                );

                // Tell the generator to exit and resume it to completion.
                exit_signal.store(true, Ordering::Release);
                assert!(generator.resume().is_none());
            })
        };

        let handle = handle_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("handle unavailable");

        // Wait for generator to lock mutex
        ready_rx.recv().expect("generator didn't start");

        // Verify lock is held (try_lock should fail)
        if let Ok(_) = lock.try_lock() {
            panic!("Mutex should be locked by the generator!");
        }

        // Trigger Preemption
        handle.interrupt().expect("interrupt failed");

        // Now we wait for the worker thread to finish.
        // The worker thread will verify preemption happened, then resume generator.
        // Generator will exit and drop the lock.
        worker.join().expect("worker panicked");

        // Verify lock is released
        match lock.try_lock() {
            Ok(_) => {}
            Err(std::sync::TryLockError::Poisoned(_)) => {
                panic!("Mutex is poisoned! Generator likely panicked.");
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                panic!("Mutex is still locked! Generator did not release it.");
            }
        }
    });
}

#[test]
fn test_generator_thread_migration() {
    run_serialized(|| {
        // This test verifies that a generator can be started on one thread,
        // preempted, moved to another thread, and successfully resumed/finished.

        // Shared state
        let preempt_flag = Arc::new(AtomicBool::new(false));
        let exit_signal = Arc::new(AtomicBool::new(false));

        // Channels for passing the generator
        // We need to wrap Generator in a struct that is Send?
        // Generator is Send if 'static.
        // We'll use a channel of `Generator<'static, (), ()>`.
        let (gen_tx, gen_rx) = std::sync::mpsc::channel();
        let (handle_tx, handle_rx) = std::sync::mpsc::channel();

        // Thread A: The "Source" Thread
        let thread_a = {
            let flag = Arc::clone(&preempt_flag);
            let exit = Arc::clone(&exit_signal);
            thread::spawn(move || {
                let _guard = crate::runtime::preemption::init_worker_preemption().unwrap();

                #[cfg(unix)]
                let handle = crate::runtime::preemption::WorkerThreadHandle::current().unwrap();
                #[cfg(windows)]
                let handle =
                    crate::runtime::preemption::WorkerThreadHandle::current(&flag).unwrap();

                handle_tx.send(handle).unwrap();

                let mut generator = crate::generator::Gn::<()>::new_scoped(move |mut scope| {
                    crate::runtime::preemption::set_generator_scope(
                        &mut scope as *mut _ as *mut (),
                    );

                    let mut spin = 0u64;
                    loop {
                        std::hint::black_box(spin);
                        spin = spin.wrapping_add(1);

                        if exit.load(Ordering::Acquire) {
                            crate::runtime::preemption::clear_generator_scope();
                            return crate::generator::done();
                        }
                    }
                });

                // Start and wait for preemption
                use crate::runtime::preemption::GeneratorYieldReason;
                let reason = generator
                    .resume()
                    .and_then(GeneratorYieldReason::from_usize)
                    .expect("should yield");

                assert_eq!(reason, GeneratorYieldReason::Preempted);

                // Send the generator to Thread B
                // unsafe impl Send for Generator<'static>...
                // The compiler should deduce 'static here because we move Arc (Send + 'static)
                gen_tx.send(generator).unwrap();
            })
        };

        // Interrupt Thread A
        let handle = handle_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("no handle");
        // Wait a bit for loop
        thread::sleep(Duration::from_millis(50));
        handle.interrupt().expect("interrupt failed");

        thread_a.join().expect("thread A panicked");

        // Thread B: The "Destination" Thread
        let thread_b = {
            let exit = Arc::clone(&exit_signal);
            thread::spawn(move || {
                // We don't strictly need preemption enabled on Thread B to resume,
                // but good practice if it might be preempted again.
                // For this test, we just want to finish it.

                let mut generator = gen_rx.recv().unwrap();

                // Signal exit
                exit.store(true, Ordering::Release);

                // Resume
                // This should resume on THIS thread's stack.
                assert!(generator.resume().is_none());
            })
        };

        thread_b.join().expect("thread B panicked");
    });
}

#[test]
fn test_interrupt_long_sleep() {
    run_serialized(|| {
        // Test interrupting a blocking sleep.
        // Note: On Windows, SuspendThread cannot interrupt a kernel Wait (Sleep).
        // So this test effectively verifies platform behavior:
        // - Unix: Signal interrupts Sleep -> Preempted immediately.
        // - Windows: SuspendThread pauses Sleep -> Preempted ONLY after Sleep finishes?
        //   Wait, if we Suspend, the thread stops. The Sleep timer counts.
        //   When Sleep finishes, thread wakes.
        //   THEN it executes Trampoline.
        //   So on Windows, we expect "Preempted after sleep duration".

        let (handle_tx, handle_rx) = std::sync::mpsc::channel();
        let preempt_flag = Arc::new(AtomicBool::new(false));

        let worker = {
            let flag = Arc::clone(&preempt_flag);
            thread::spawn(move || {
                let _guard = crate::runtime::preemption::init_worker_preemption().unwrap();
                #[cfg(unix)]
                let handle = crate::runtime::preemption::WorkerThreadHandle::current().unwrap();
                #[cfg(windows)]
                let handle =
                    crate::runtime::preemption::WorkerThreadHandle::current(&flag).unwrap();
                handle_tx.send(handle).unwrap();

                let mut generator = crate::generator::Gn::<()>::new_scoped(move |mut scope| {
                    crate::runtime::preemption::set_generator_scope(
                        &mut scope as *mut _ as *mut (),
                    );

                    // Sleep for 3 seconds
                    println!("[Gen] Sleeping...");
                    std::thread::sleep(Duration::from_secs(3));
                    println!("[Gen] Woke up!");

                    crate::runtime::preemption::clear_generator_scope();
                    crate::generator::done()
                });

                use crate::runtime::preemption::GeneratorYieldReason;

                let start = std::time::Instant::now();
                let reason = generator
                    .resume()
                    .and_then(GeneratorYieldReason::from_usize);
                let elapsed = start.elapsed();

                if let Some(GeneratorYieldReason::Preempted) = reason {
                    println!("[Worker] Preempted after {:?}", elapsed);

                    // On Unix, this should be fast (< 3s).
                    #[cfg(unix)]
                    assert!(
                        elapsed < Duration::from_secs(2),
                        "Unix sleep should be interrupted"
                    );

                    // On Windows, this will likely be > 3s because we can't interrupt kernel wait.
                    #[cfg(windows)]
                    assert!(
                        elapsed >= Duration::from_secs(3),
                        "Windows sleep cannot be interrupted"
                    );

                    // Resume to finish
                    assert!(generator.resume().is_none());
                } else {
                    panic!(
                        "[Worker] Generator failed to be preempted. Reason: {:?}",
                        reason
                    );
                }
            })
        };

        let handle = handle_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        thread::sleep(Duration::from_millis(500)); // Ensure we are in sleep

        println!("[Test] Interrupting...");
        handle.interrupt().expect("interrupt failed");

        worker.join().unwrap();
    });
}

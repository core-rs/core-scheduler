// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use futures_lite::future::{block_on, poll_once};
use std::sync::mpsc;
use std::time::Duration;

static NEVER: Cancellation = Cancellation::never();
const TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn prepared_work_allocates_only_jobs_and_completion_channels() {
    use crate::{NativeFile, NativeScheduler};
    use core_scheduler::{Blocking, FileReadAt, FileWriteAt};
    use std::task::{Context, Waker};
    use std::time::Instant;

    // Avoid measuring lazy executor parking state or executor-specific wakers.
    fn wait<T>(future: impl Future<Output = T>) -> T {
        let mut future = pin!(future);
        let started = Instant::now();
        loop {
            if let Poll::Ready(output) = future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                return output;
            }
            assert!(started.elapsed() < TIMEOUT, "operation timed out");
            thread::yield_now();
        }
    }

    let scheduler = NativeScheduler::with_options(NativeOptions {
        workers: 1,
        threads: 1,
        capacity: 1,
        ..Default::default()
    })
    .unwrap();
    let mut worker = None;
    // Measure the entire worker loop, including dispatch, completion delivery, and retirement. Thread creation and reporting remain outside measurement.
    scheduler
        .work
        .start_with(|shared, _| {
            worker = Some(thread::spawn(move || {
                // Some platforms allocate native synchronization storage lazily.
                drop(
                    shared
                        .work
                        .wait_timeout(shared.state.lock().unwrap(), Duration::ZERO)
                        .unwrap(),
                );
                allocation_counter::measure(|| work(shared))
            }));
            Ok(())
        })
        .unwrap();
    let token = Cancellation::new();
    let file = NativeFile::from_std(tempfile::tempfile().unwrap());
    let buffer = vec![7; 8];
    let pointer = buffer.as_ptr();

    // Warm queue capacity. This job is also included in the worker's allocation measurement.
    wait(wait(scheduler.spawn_blocking(|| (), &token)).unwrap()).unwrap();
    wait(scheduler.work.drain());

    const REPETITIONS: u64 = 16;
    let allocations = allocation_counter::measure(|| {
        let mut buffer = buffer;
        for _ in 0..REPETITIONS {
            let task = wait(scheduler.spawn_blocking(|| 42, &token)).unwrap();
            assert_eq!(wait(task).unwrap(), 42);
            wait(scheduler.work.drain());

            let (result, returned) = wait(scheduler.file_write_at(&file, buffer, 0, &token));
            assert_eq!(result.unwrap(), 8);
            buffer = returned;
            wait(scheduler.work.drain());

            buffer.fill(0);
            let (result, returned) = wait(scheduler.file_read_at(&file, buffer, 0, &token));
            assert_eq!(result.unwrap(), 8);
            buffer = returned;
            assert_eq!(buffer, [7; 8]);
            assert_eq!(buffer.as_ptr(), pointer);
            wait(scheduler.work.drain());
        }
    });
    wait(scheduler.shutdown());
    let worker_allocations = worker.unwrap().join().unwrap();

    // Native deliberately keeps a boxed job and one completion channel for each operation. Prevent additional allocations, including a separate shared abandonment flag, without requiring native to eliminate these two.
    assert!(
        allocations.count_total <= REPETITIONS * 3 * 2,
        "{allocations:?}"
    );
    assert_eq!(worker_allocations.count_total, 0, "{worker_allocations:?}");
}

#[test]
fn capacity_wait_rechecks_available_or_closed_state_before_sleeping() {
    use std::task::{Context, Waker};

    let pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 1,
        capacity: 1,
        ..Default::default()
    });
    for closed in [false, true] {
        if closed {
            pool.close();
        }
        // Admission can observe a full pool, then retirement or closure can
        // happen before this wait begins. Its final state check must not sleep.
        let mut waiting = pin!(pool.wait_for_capacity(&NEVER));
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        assert_eq!(pool.shared.capacity.total_listeners(), 0);
    }
}

#[test]
fn immediate_admission_and_empty_drain_need_no_listeners_or_allocations() {
    use std::task::{Context, Waker};

    let pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 1,
        capacity: 1,
        ..Default::default()
    });
    // Initialize worker synchronization and queue storage before measuring reuse.
    block_on(block_on(pool.submit(|| ())).unwrap())
        .unwrap()
        .unwrap();
    block_on(pool.drain());
    pool.shared.state.lock().unwrap().queue.reserve(1);
    let cancellation = Cancellation::new();
    let mut admission = pin!(pool.admit(&cancellation, || {
        assert_eq!(pool.shared.capacity.total_listeners(), 0);
        (
            Job {
                run: Box::new(|| {}),
                submitted: false,
            },
            (),
        )
    }));
    let allocations = allocation_counter::measure(|| {
        assert!(matches!(
            admission
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(()))
        ));
    });
    assert_eq!(allocations.count_total, 0);
    block_on(pool.drain());

    let allocations = allocation_counter::measure(|| {
        let mut drain = pin!(pool.drain());
        assert!(
            drain
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
    });
    assert_eq!(allocations.count_total, 0);
}

#[test]
fn pending_capacity_and_drain_waits_do_not_allocate_and_unregister_on_drop() {
    use std::task::{Context, Waker};

    let pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 1,
        capacity: 1,
        ..Default::default()
    });
    let (release, released) = mpsc::channel();
    let running =
        block_on(pool.spawn(move || released.recv_timeout(TIMEOUT).unwrap(), &NEVER)).unwrap();
    let cancellation = Cancellation::new();

    // Measure only waiter polling and destruction on this thread, not worker jobs.
    let allocations = allocation_counter::measure(|| {
        for _ in 0..16 {
            let mut admission = pin!(pool.spawn(|| panic!("unadmitted job ran"), &cancellation));
            let mut drain = pin!(pool.drain());
            for _ in 0..2 {
                assert!(
                    admission
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                assert!(
                    drain
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
            }
            assert_eq!(pool.shared.capacity.total_listeners(), 2);
        }
    });
    assert_eq!(allocations.count_total, 0);
    assert_eq!(pool.shared.capacity.total_listeners(), 0);
    release.send(()).unwrap();
    block_on(running).unwrap();
    block_on(pool.drain());
}

#[test]
fn submitted_jobs_survive_abandonment_and_shutdown_with_bounded_admission() {
    let pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 1,
        capacity: 2,
        ..Default::default()
    });
    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let running = block_on(pool.spawn(
        move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        },
        &NEVER,
    ))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    let (completed, completion) = mpsc::channel();
    let submitted = block_on(pool.submit(move || completed.send(()).unwrap())).unwrap();
    // Dropping a cleanup result cannot abandon execution, even while it remains queued.
    drop(submitted);
    let mut waiting = Box::pin(pool.submit(|| panic!("unadmitted job must not run")));
    assert!(block_on(poll_once(&mut waiting)).is_none());
    let mut drained = Box::pin(pool.drain());
    assert!(block_on(poll_once(&mut drained)).is_none());
    pool.close();
    assert!(matches!(
        block_on(waiting),
        Err(BlockingError::SchedulerClosed)
    ));
    assert!(completion.try_recv().is_err());
    release.send(()).unwrap();
    block_on(running).unwrap();
    block_on(drained);
    completion.recv_timeout(TIMEOUT).unwrap();
}

#[test]
fn draining_waits_for_running_work_and_releases_capacity() {
    let pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 1,
        capacity: 1,
        ..Default::default()
    });
    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let first = block_on(pool.submit(move || {
        started.send(()).unwrap();
        released.recv_timeout(TIMEOUT).unwrap();
    }))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    let mut waiting = Box::pin(pool.submit(|| 42));
    assert!(block_on(poll_once(&mut waiting)).is_none());
    let mut drain = Box::pin(pool.drain());
    assert!(block_on(poll_once(&mut drain)).is_none());
    release.send(()).unwrap();
    assert_eq!(block_on(block_on(waiting).unwrap()).unwrap().unwrap(), 42);
    block_on(first).unwrap().unwrap();
    block_on(drain);
}

struct DropGate {
    started: mpsc::Sender<()>,
    released: mpsc::Receiver<()>,
}

impl Drop for DropGate {
    fn drop(&mut self) {
        self.started.send(()).unwrap();
        self.released.recv_timeout(TIMEOUT).unwrap();
    }
}

#[test]
fn skipped_jobs_release_captures_before_reporting_completion() {
    for shutdown in [false, true] {
        let pool = BlockingPool::new(NativeOptions {
            workers: 1,
            threads: 1,
            capacity: 2,
            ..Default::default()
        });
        let (release_worker, worker_released) = mpsc::channel();
        let (started, start) = mpsc::channel();
        let running = block_on(pool.spawn(
            move || {
                started.send(()).unwrap();
                worker_released.recv_timeout(TIMEOUT).unwrap();
            },
            &NEVER,
        ))
        .unwrap();
        start.recv_timeout(TIMEOUT).unwrap();

        let (started, start) = mpsc::channel();
        let (release_drop, released) = mpsc::channel();
        let captured = DropGate { started, released };
        let cancellation = Cancellation::new();
        let mut queued =
            Box::pin(block_on(pool.spawn(move || drop(captured), &cancellation)).unwrap());
        thread::scope(|scope| {
            let closing = if shutdown {
                Some(scope.spawn(|| pool.close()))
            } else {
                cancellation.cancel();
                release_worker.send(()).unwrap();
                None
            };
            start.recv_timeout(TIMEOUT).unwrap();
            assert!(block_on(poll_once(queued.as_mut())).is_none());
            release_drop.send(()).unwrap();
            assert!(matches!(block_on(queued), Err(TaskError::Cancelled)));
            if let Some(closing) = closing {
                closing.join().unwrap();
                release_worker.send(()).unwrap();
            }
        });
        block_on(running).unwrap();
    }
}

#[test]
fn admission_reports_worker_startup_failure() {
    let mut pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 2,
        capacity: 2,
        ..Default::default()
    });
    pool.fail_start_at = Some(1);
    assert!(
        matches!(block_on(pool.spawn(|| (), &NEVER)), Err(BlockingError::Io(error)) if error.to_string() == "injected thread startup failure")
    );
    assert!(matches!(
        block_on(pool.spawn(|| (), &NEVER)),
        Err(BlockingError::SchedulerClosed)
    ));
}

#[test]
fn partial_startup_failure_closes_admission_and_releases_started_workers() {
    let pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 2,
        capacity: 2,
        ..Default::default()
    });
    let (exited, exit) = mpsc::channel();
    let result = pool.start_with(|shared, identifier| {
        if identifier == 1 {
            return Err(io::Error::other("injected thread startup failure"));
        }
        let exited = exited.clone();
        thread::spawn(move || {
            work(shared);
            exited.send(()).unwrap();
        });
        Ok(())
    });
    assert!(
        matches!(result, Err(BlockingError::Io(error)) if error.to_string() == "injected thread startup failure")
    );
    exit.recv_timeout(TIMEOUT).unwrap();
    assert!(matches!(
        block_on(pool.spawn(|| (), &NEVER)),
        Err(BlockingError::SchedulerClosed)
    ));
    assert!(matches!(
        pool.start_with(|_, _| panic!("startup must not retry")),
        Err(BlockingError::Io(_))
    ));
}

#[test]
fn shutdown_during_startup_cannot_admit_work() {
    let pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 1,
        capacity: 1,
        ..Default::default()
    });
    pool.start_with(|shared, _| {
        pool.close();
        thread::spawn(move || work(shared));
        Ok(())
    })
    .unwrap();
    assert!(matches!(
        block_on(pool.spawn(|| (), &NEVER)),
        Err(BlockingError::SchedulerClosed)
    ));
}

struct Destructor {
    shared: Arc<Shared>,
    dropped: mpsc::Sender<()>,
}

#[test]
fn dropping_a_queued_handle_on_another_thread_skips_execution_and_preserves_the_worker() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 1,
        capacity: 2,
        ..Default::default()
    });
    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let running = block_on(pool.spawn(
        move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        },
        &NEVER,
    ))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();

    let (dropped, drops) = mpsc::channel();
    let captured = Destructor {
        shared: Arc::clone(&pool.shared),
        dropped,
    };
    let executed = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&executed);
    let queued = block_on(pool.spawn(
        move || {
            observed.store(true, Ordering::SeqCst);
            drop(captured);
        },
        &NEVER,
    ))
    .unwrap();
    thread::spawn(move || drop(queued)).join().unwrap();
    assert!(drops.try_recv().is_err());
    release.send(()).unwrap();
    block_on(running).unwrap();
    drops.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(
        block_on(block_on(pool.spawn(|| 7, &NEVER)).unwrap()).unwrap(),
        7
    );
    assert!(!executed.load(Ordering::SeqCst));
    block_on(pool.drain());
}

impl Drop for Destructor {
    fn drop(&mut self) {
        // Taking the lock proves user destructors run outside it.
        drop(self.shared.state.lock().unwrap());
        self.dropped.send(()).unwrap();
        panic!("user destructor panic");
    }
}

#[test]
fn abandoned_result_panics_release_capacity_and_preserve_the_worker() {
    let pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 1,
        capacity: 1,
        ..Default::default()
    });
    let (release, released) = mpsc::channel();
    let (started, start) = mpsc::channel();
    let (dropped, drops) = mpsc::channel();
    let output = Destructor {
        shared: pool.shared.clone(),
        dropped,
    };
    let job = block_on(pool.spawn(
        move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
            output
        },
        &NEVER,
    ))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    drop(job);
    release.send(()).unwrap();
    drops.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(
        block_on(block_on(pool.spawn(|| 7, &NEVER)).unwrap()).unwrap(),
        7
    );
}

#[test]
fn shutdown_catches_capture_destructor_panics_and_cancels_other_queued_jobs() {
    let pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 1,
        capacity: 3,
        ..Default::default()
    });
    let (release, released) = mpsc::channel();
    let (started, start) = mpsc::channel();
    let first = block_on(pool.spawn(
        move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        },
        &NEVER,
    ))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    let (dropped, drops) = mpsc::channel();
    let captured = Destructor {
        shared: pool.shared.clone(),
        dropped,
    };
    let second = block_on(pool.spawn(move || drop(captured), &NEVER)).unwrap();
    let third = block_on(pool.spawn(|| panic!("queued job ran after shutdown"), &NEVER)).unwrap();
    let mut waiting = pin!(pool.spawn(|| (), &NEVER));
    assert!(block_on(poll_once(waiting.as_mut())).is_none());
    pool.close();
    drops.recv_timeout(TIMEOUT).unwrap();
    assert!(matches!(block_on(second), Err(TaskError::Cancelled)));
    assert!(matches!(block_on(third), Err(TaskError::Cancelled)));
    assert!(matches!(
        block_on(waiting),
        Err(BlockingError::SchedulerClosed)
    ));
    release.send(()).unwrap();
    block_on(first).unwrap();
}

#[test]
fn concurrent_first_admissions_share_one_startup() {
    let pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 2,
        capacity: 2,
        ..Default::default()
    });
    thread::scope(|scope| {
        for value in 0..8 {
            let pool = &pool;
            scope.spawn(move || {
                assert_eq!(
                    block_on(block_on(pool.spawn(move || value, &NEVER)).unwrap()).unwrap(),
                    value
                )
            });
        }
    });
}

#[test]
fn io_cancellation_returns_owned_input_and_worker_panics_resume_in_the_waiter() {
    let pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 1,
        capacity: 2,
        ..Default::default()
    });
    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let running = block_on(pool.spawn(
        move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        },
        &NEVER,
    ))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    let token = Cancellation::new();
    let mut waiting = Box::pin(pool.execute(vec![7], &token, |_| -> io::Result<()> {
        panic!("cancelled I/O ran");
    }));
    assert!(block_on(poll_once(&mut waiting)).is_none());
    token.cancel();
    release.send(()).unwrap();
    let (result, buffer) = block_on(waiting);
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    assert_eq!(buffer, vec![7]);
    block_on(running).unwrap();
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        block_on(pool.execute((), &NEVER, |_| -> io::Result<()> {
            std::panic::panic_any(String::from("I/O panic"));
        }))
    }));
    assert_eq!(
        *result.unwrap_err().downcast::<String>().unwrap(),
        "I/O panic"
    );
    assert_eq!(
        block_on(block_on(pool.spawn(|| 42, &NEVER)).unwrap()).unwrap(),
        42
    );
}

#[test]
fn shutdown_drain_waits_until_discarded_capture_destructors_finish() {
    let pool = BlockingPool::new(NativeOptions {
        workers: 1,
        threads: 1,
        capacity: 2,
        ..Default::default()
    });
    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let running = block_on(pool.spawn(
        move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        },
        &NEVER,
    ))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    let (started, start) = mpsc::channel();
    let (release_drop, released) = mpsc::channel();
    let capture = DropGate { started, released };
    let queued = block_on(pool.spawn(move || drop(capture), &NEVER)).unwrap();
    thread::scope(|scope| {
        let closing = scope.spawn(|| pool.close());
        start.recv_timeout(TIMEOUT).unwrap();
        release.send(()).unwrap();
        block_on(running).unwrap();
        let mut drain = Box::pin(pool.drain());
        assert!(block_on(poll_once(&mut drain)).is_none());
        release_drop.send(()).unwrap();
        closing.join().unwrap();
        block_on(drain);
        assert!(matches!(block_on(queued), Err(TaskError::Cancelled)));
    });
}

#[test]
fn panicking_unobserved_payloads_preserve_native_capacity_and_shutdown() {
    const CHILD: &str = "PROTOCOL_SCHEDULER_POOL_PANIC_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "blocking_pool::tests::panicking_unobserved_payloads_preserve_native_capacity_and_shutdown", "--nocapture"])
            .env(CHILD, "1").status().unwrap();
        assert!(status.success());
        return;
    }
    struct Payload;
    impl Drop for Payload {
        fn drop(&mut self) {
            panic!("panic payload destructor");
        }
    }
    struct Capture;
    impl Drop for Capture {
        fn drop(&mut self) {
            std::panic::panic_any(Payload);
        }
    }
    crate::test_support::run(|| {
        // A queued capture is destroyed synchronously during admission close.
        let pool = BlockingPool::new(NativeOptions {
            workers: 1,
            threads: 1,
            capacity: 2,
            ..Default::default()
        });
        let (started, start) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let running = block_on(pool.spawn(
            move || {
                started.send(()).unwrap();
                released.recv_timeout(TIMEOUT).unwrap();
            },
            &NEVER,
        ))
        .unwrap();
        start.recv_timeout(TIMEOUT).unwrap();
        let capture = Capture;
        let queued = block_on(pool.spawn(move || drop(capture), &NEVER)).unwrap();
        pool.close();
        assert!(matches!(block_on(queued), Err(TaskError::Cancelled)));
        release.send(()).unwrap();
        block_on(running).unwrap();
        block_on(pool.drain());

        // Both an abandoned result and an abandoned execution panic can create a second panic payload when destroyed on the worker.
        for panics in [false, true] {
            let pool = BlockingPool::new(NativeOptions {
                workers: 1,
                threads: 1,
                capacity: 1,
                ..Default::default()
            });
            let (started, start) = mpsc::channel();
            let (release, released) = mpsc::channel();
            let task = block_on(pool.spawn(
                move || {
                    started.send(()).unwrap();
                    released.recv_timeout(TIMEOUT).unwrap();
                    if panics {
                        std::panic::panic_any(Capture);
                    }
                    Capture
                },
                &NEVER,
            ))
            .unwrap();
            start.recv_timeout(TIMEOUT).unwrap();
            drop(task);
            release.send(()).unwrap();
            block_on(pool.drain());
            assert_eq!(
                block_on(block_on(pool.spawn(|| 42, &NEVER)).unwrap()).unwrap(),
                42
            );
            pool.close();
            block_on(pool.drain());
        }
    });
}

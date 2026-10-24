// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use core_scheduler_selector::{SelectorDriver, SelectorWorker};
use futures_channel::oneshot;
use futures_lite::future::block_on;
use std::{
    future::poll_fn,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    task::{Poll, Wake, Waker},
    thread::{self, Thread},
    time::Duration,
};

#[derive(Default)]
struct State {
    thread: OnceLock<Thread>,
    queued: Mutex<Vec<oneshot::Sender<()>>>,
    paused: AtomicBool,
    closed: AtomicBool,
    failed: AtomicBool,
    inside_turn: AtomicBool,
    idle: AtomicBool,
    batches: AtomicUsize,
    turns: AtomicUsize,
    published_at: AtomicUsize,
    dropped: AtomicUsize,
    retired: AtomicBool,
    only_idle_completions: AtomicBool,
    interrupts: AtomicUsize,
    completion_interrupts: AtomicUsize,
}
impl Wake for State {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.interrupts.fetch_add(1, Ordering::SeqCst);
        if let Some(thread) = self.thread.get() {
            thread.unpark();
        }
    }
}
impl State {
    fn operation(self: &Arc<Self>) -> oneshot::Receiver<()> {
        let (send, receive) = oneshot::channel();
        self.queued.lock().unwrap().push(send);
        self.wake_by_ref();
        receive
    }
}
struct Driver(Arc<State>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
impl SelectorDriver for Driver {
    type Worker = Self;
    fn enter(self) -> Self {
        self.0.thread.set(thread::current()).unwrap();
        self
    }
}
impl SelectorWorker for Driver {
    fn waker(&self) -> Waker {
        Waker::from(Arc::clone(&self.0))
    }
    fn close(&mut self) {
        self.0.closed.store(true, Ordering::SeqCst);
    }
    fn turn(&mut self, timeout: Option<Duration>) -> io::Result<bool> {
        let turn = self.0.turns.fetch_add(1, Ordering::SeqCst) + 1;
        if self.0.failed.load(Ordering::SeqCst) {
            self.0.retired.store(true, Ordering::SeqCst);
            return Err(io::Error::other("driver failure"));
        }
        if !self.0.paused.load(Ordering::SeqCst)
            && (timeout != Some(Duration::ZERO)
                || !self.0.only_idle_completions.load(Ordering::SeqCst))
        {
            let batch = std::mem::take(&mut *self.0.queued.lock().unwrap());
            if !batch.is_empty() {
                self.0.inside_turn.store(true, Ordering::SeqCst);
                self.0.published_at.store(turn, Ordering::SeqCst);
                let interrupts = self.0.interrupts.load(Ordering::SeqCst);
                for sender in batch {
                    let _ = sender.send(());
                }
                self.0.batches.fetch_add(1, Ordering::SeqCst);
                self.0.completion_interrupts.fetch_add(
                    self.0.interrupts.load(Ordering::SeqCst) - interrupts,
                    Ordering::SeqCst,
                );
                self.0.inside_turn.store(false, Ordering::SeqCst);
                return Ok(false);
            }
            if self.0.closed.load(Ordering::SeqCst) {
                self.0.retired.store(true, Ordering::SeqCst);
                return Ok(true);
            }
        }
        if timeout != Some(Duration::ZERO) {
            self.0.idle.store(true, Ordering::SeqCst);
            if let Some(timeout) = timeout {
                thread::park_timeout(timeout);
            } else {
                thread::park();
            }
            self.0.idle.store(false, Ordering::SeqCst);
        }
        Ok(false)
    }
}
fn construct(states: &[Arc<State>]) -> NativeScheduler {
    NativeScheduler::with_drivers(
        ReadinessSelector::new().unwrap(),
        states
            .iter()
            .map(|state| Driver(Arc::clone(state)))
            .collect(),
        NativeExecutorOptions {
            workers: states.len(),
            timer_capacity: 8,
        },
    )
    .unwrap()
}
fn wait(mut condition: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(
            std::time::Instant::now() < deadline,
            "driver did not progress"
        );
        thread::yield_now();
    }
}

#[test]
fn timer_deadlines_bound_driver_waits_and_survive_driver_retirement() {
    use core_scheduler::{Cancellation, Clock};
    crate::test_support::run(|| {
        let state = Arc::new(State::default());
        let scheduler = Arc::new(construct(&[Arc::clone(&state)]));
        let owned = Arc::clone(&scheduler);
        let task = scheduler
            .spawn(async move {
                let never = Cancellation::never();
                let deadline = owned.now() + Duration::from_millis(10);
                owned.sleep_until(deadline, &never).await.unwrap();
                assert!(owned.now() >= deadline);
            })
            .unwrap();
        block_on(task).unwrap();
        // A failed driver switches back to thread parking, still with timer deadlines.
        state.failed.store(true, Ordering::SeqCst);
        state.wake_by_ref();
        wait(|| state.retired.load(Ordering::SeqCst));
        scheduler
            .block_on(scheduler.sleep(Duration::from_millis(10), &Cancellation::never()))
            .unwrap();
        block_on(scheduler.shutdown());
    });
}

#[test]
fn idle_completions_resume_tasks_without_interrupting_their_own_worker() {
    crate::test_support::run(|| {
        let state = Arc::new(State::default());
        state.only_idle_completions.store(true, Ordering::SeqCst);
        let scheduler = construct(&[Arc::clone(&state)]);
        let observed = Arc::clone(&state);
        let task = scheduler
            .spawn(async move {
                for _ in 0..32 {
                    observed.operation().await.unwrap();
                    assert!(!observed.inside_turn.load(Ordering::SeqCst));
                    assert_eq!(
                        observed.turns.load(Ordering::SeqCst),
                        observed.published_at.load(Ordering::SeqCst),
                        "idle completion must resume its consumer before another driver turn"
                    );
                }
                42
            })
            .unwrap();
        assert_eq!(block_on(task).unwrap(), 42);
        assert_eq!(state.batches.load(Ordering::SeqCst), 32);
        assert_eq!(state.completion_interrupts.load(Ordering::SeqCst), 0);
        block_on(scheduler.shutdown());
        wait(|| state.dropped.load(Ordering::SeqCst) == 1);
        assert!(state.retired.load(Ordering::SeqCst));
    });
}

#[test]
fn remote_task_wakes_still_interrupt_an_idle_single_worker() {
    crate::test_support::run(|| {
        let state = Arc::new(State::default());
        let scheduler = construct(&[Arc::clone(&state)]);
        let (send, receive) = mpsc::channel();
        let ready = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&ready);
        let task = scheduler
            .spawn(poll_fn(move |context| {
                if observed.load(Ordering::Acquire) {
                    Poll::Ready(42)
                } else {
                    send.send(context.waker().clone()).unwrap();
                    Poll::Pending
                }
            }))
            .unwrap();
        let waker = receive.recv_timeout(Duration::from_secs(5)).unwrap();
        wait(|| state.idle.load(Ordering::SeqCst));
        let interrupts = state.interrupts.load(Ordering::SeqCst);
        ready.store(true, Ordering::Release);
        waker.wake();
        assert_eq!(block_on(task).unwrap(), 42);
        assert!(state.interrupts.load(Ordering::SeqCst) > interrupts);
        block_on(scheduler.shutdown());
        wait(|| state.retired.load(Ordering::SeqCst));
    });
}

#[test]
fn completion_batch_is_published_before_any_local_task_resumes() {
    crate::test_support::run(|| {
        let state = Arc::new(State::default());
        state.paused.store(true, Ordering::SeqCst);
        let scheduler = construct(&[Arc::clone(&state)]);
        let (started, ready) = mpsc::channel();
        let tasks: Vec<_> = (0..2)
            .map(|_| {
                let state = Arc::clone(&state);
                let started = started.clone();
                scheduler
                    .spawn(async move {
                        let operation = state.operation();
                        started.send(()).unwrap();
                        operation.await.unwrap();
                        assert!(!state.inside_turn.load(Ordering::SeqCst));
                        assert_eq!(state.batches.load(Ordering::SeqCst), 1);
                    })
                    .unwrap()
            })
            .collect();
        for _ in 0..2 {
            ready.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        state.paused.store(false, Ordering::SeqCst);
        state.wake_by_ref();
        for task in tasks {
            block_on(task).unwrap();
        }
        block_on(scheduler.shutdown());
        wait(|| state.dropped.load(Ordering::SeqCst) == 1);
        assert!(state.retired.load(Ordering::SeqCst));
    });
}

#[test]
fn busy_tasks_do_not_starve_driver_turns() {
    crate::test_support::run(|| {
        let state = Arc::new(State::default());
        let scheduler = construct(&[Arc::clone(&state)]);
        let state2 = Arc::clone(&state);
        let task = scheduler
            .spawn(async move {
                let turns = state2.turns.load(Ordering::SeqCst);
                let mut operation = state2.operation();
                poll_fn(|context| {
                    let result = std::pin::Pin::new(&mut operation).poll(context);
                    context.waker().wake_by_ref();
                    result
                })
                .await
                .unwrap();
                assert!(state2.turns.load(Ordering::SeqCst) > turns);
            })
            .unwrap();
        block_on(task).unwrap();
        block_on(scheduler.shutdown());
        wait(|| state.retired.load(Ordering::SeqCst));
    });
}

#[test]
fn self_wakes_wait_for_the_next_driver_turn() {
    crate::test_support::run(|| {
        let state = Arc::new(State::default());
        let scheduler = construct(&[Arc::clone(&state)]);
        let observed = Arc::clone(&state);
        let task = scheduler
            .spawn(async move {
                let mut previous_turn = None;
                let mut polls = 0;
                poll_fn(|context| {
                    let turn = observed.turns.load(Ordering::SeqCst);
                    if let Some(previous) = previous_turn {
                        assert!(turn > previous, "self-wake extended the current batch");
                    }
                    previous_turn = Some(turn);
                    polls += 1;
                    if polls == 4 {
                        Poll::Ready(())
                    } else {
                        context.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await;
            })
            .unwrap();
        block_on(task).unwrap();
        block_on(scheduler.shutdown());
        wait(|| state.retired.load(Ordering::SeqCst));
    });
}

#[test]
fn tasks_keep_running_after_driver_retirement_or_failure() {
    for fail in [false, true] {
        crate::test_support::run(move || {
            let state = Arc::new(State::default());
            let scheduler = construct(&[Arc::clone(&state)]);
            if fail {
                state.failed.store(true, Ordering::SeqCst);
            } else {
                state.closed.store(true, Ordering::SeqCst);
            }
            state.wake_by_ref();
            wait(|| state.retired.load(Ordering::SeqCst));
            for _ in 0..2 {
                assert_eq!(
                    block_on(scheduler.spawn(async { 42 }).unwrap()).unwrap(),
                    42
                );
            }
            block_on(scheduler.shutdown());
            wait(|| state.dropped.load(Ordering::SeqCst) == 1);
        });
    }
}

#[test]
fn closed_workers_keep_driving_abandoned_operations_until_retirement() {
    crate::test_support::run(|| {
        let state = Arc::new(State::default());
        state.paused.store(true, Ordering::SeqCst);
        let scheduler = construct(&[Arc::clone(&state)]);
        drop(state.operation());
        scheduler.close();
        wait(|| state.closed.load(Ordering::SeqCst));
        assert!(!state.retired.load(Ordering::SeqCst));
        state.paused.store(false, Ordering::SeqCst);
        state.wake_by_ref();
        wait(|| state.dropped.load(Ordering::SeqCst) == 1);
        assert_eq!(state.batches.load(Ordering::SeqCst), 1);
        assert!(state.retired.load(Ordering::SeqCst));
        block_on(scheduler.shutdown());
    });
}

#[test]
fn invalid_configuration_and_partial_startup_drop_every_driver() {
    for options in [
        NativeExecutorOptions {
            workers: 0,
            timer_capacity: 8,
        },
        NativeExecutorOptions {
            workers: 2,
            timer_capacity: 8,
        },
        NativeExecutorOptions {
            workers: 1,
            timer_capacity: 0,
        },
    ] {
        let state = Arc::new(State::default());
        assert!(
            NativeScheduler::with_drivers(
                ReadinessSelector::new().unwrap(),
                vec![Driver(Arc::clone(&state))],
                options
            )
            .is_err()
        );
        assert_eq!(state.dropped.load(Ordering::SeqCst), 1);
    }
    crate::test_support::run(|| {
        let first = Arc::new(State::default());
        let second = Arc::new(State::default());
        let third = Arc::new(State::default());
        let selector = ReadinessSelector::new().unwrap();
        crate::thread::FAIL_AFTER.set(Some(1));
        let result = NativeScheduler::with_drivers(
            selector,
            vec![
                Driver(Arc::clone(&first)),
                Driver(Arc::clone(&second)),
                Driver(Arc::clone(&third)),
            ],
            NativeExecutorOptions {
                workers: 3,
                timer_capacity: 8,
            },
        );
        crate::thread::FAIL_AFTER.set(None);
        assert!(result.is_err());
        wait(|| first.dropped.load(Ordering::SeqCst) == 1);
        assert!(first.retired.load(Ordering::SeqCst));
        assert_eq!(second.dropped.load(Ordering::SeqCst), 1);
        assert_eq!(third.dropped.load(Ordering::SeqCst), 1);
    });
}

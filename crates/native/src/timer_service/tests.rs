// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::test_support;
use core_scheduler_conformance::ClockFixture;
use futures_lite::future::{block_on, poll_once};
use std::time::Duration;

pub(super) fn observe_wait(state: &State, deadline: bool) {
    if let Some(waits) = &state.waits {
        let _ = waits.send(deadline);
    }
}
fn wait_for_driver(waits: &std::sync::mpsc::Receiver<bool>, deadline: bool) {
    while waits.recv_timeout(Duration::from_secs(5)).unwrap() != deadline {}
}

impl ClockFixture for NativeScheduler {
    type Scheduler = Self;
    fn scheduler(&self) -> &Self {
        self
    }
    fn advance(&self, duration: Duration) {
        let wakes = {
            let mut state = self.timers.shared.state.lock().unwrap();
            *state.paused.as_mut().unwrap() += duration;
            let now = state.now();
            let mut ready = Vec::with_capacity(self.timers.capacity);
            state.owner.as_mut().unwrap().poll(now, &mut ready);
            ready
        };
        for waker in wakes {
            drop(waker);
        }
    }
    fn registrations(&self) -> usize {
        self.timers
            .shared
            .state
            .lock()
            .unwrap()
            .owner
            .as_ref()
            .map_or(0, WorkerTimers::len)
    }
}
fn run(test: impl AsyncFnOnce(&NativeScheduler) + Send + 'static) {
    test_support::run(move || {
        let scheduler = test_support::scheduler();
        scheduler.timers.shared.state.lock().unwrap().paused = Some(Instant::now());
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&scheduler))));
        block_on(scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
mod conformance {
    core_scheduler_conformance::clock_tests!(run: super::run);
}

#[test]
fn native_timer_driver_wakes_and_retires_deadlines() {
    test_support::run(|| {
        block_on(async {
            let scheduler = test_support::scheduler();
            let (waits, observed) = std::sync::mpsc::channel();
            scheduler.timers.shared.state.lock().unwrap().waits = Some(waits);
            scheduler.timers.start().unwrap();
            wait_for_driver(&observed, false);
            let never = Cancellation::never();
            let mut distant = Box::pin(scheduler.sleep(Duration::from_secs(3600), &never));
            assert!(poll_once(&mut distant).await.is_none());
            wait_for_driver(&observed, true);
            drop(distant);
            wait_for_driver(&observed, false);
            let deadline = scheduler.now() + Duration::from_millis(50);
            scheduler
                .sleep_until(deadline, &Cancellation::never())
                .await
                .unwrap();
            assert!(scheduler.now() >= deadline);
            scheduler.shutdown().await;
        })
    });
}
#[test]
fn startup_failure_exhaustion_and_shutdown_release_waiters() {
    run(async |scheduler| {
        scheduler.timers.shared.state.lock().unwrap().paused = None;
        let never = Cancellation::never();
        thread::FAIL_AFTER.set(Some(0));
        assert!(
            scheduler
                .sleep(Duration::from_secs(10), &never)
                .await
                .is_err()
        );
        let mut wait = Box::pin(scheduler.sleep(Duration::from_secs(10), &never));
        assert!(poll_once(&mut wait).await.is_none());
        scheduler.close();
        assert_eq!(wait.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    });
}

#[test]
fn full_capacity_can_be_reclaimed_and_closed_clocks_reject_new_waits() {
    test_support::run(|| {
        let scheduler = NativeScheduler::with_options(crate::NativeOptions {
            workers: 1,
            timer_capacity: 1,
            ..Default::default()
        })
        .unwrap();
        scheduler.timers.shared.state.lock().unwrap().paused = Some(Instant::now());
        let never = Cancellation::never();
        let deadline = scheduler.now() + Duration::from_secs(3600);
        let mut waits: Vec<_> = (0..scheduler.timers.capacity)
            .map(|_| Box::pin(scheduler.sleep_until(deadline, &never)))
            .collect();
        for wait in &mut waits {
            assert!(block_on(poll_once(wait)).is_none());
        }
        assert_eq!(
            block_on(scheduler.sleep_until(deadline, &never))
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(waits);
        {
            let mut state = scheduler.timers.shared.state.lock().unwrap();
            let owner = state.owner.as_mut().unwrap();
            assert!(owner.is_empty());
            assert!(owner.next_deadline().is_none());
        }
        let mut next = Box::pin(scheduler.sleep_until(deadline, &never));
        assert!(block_on(poll_once(&mut next)).is_none());
        scheduler.close();
        assert_eq!(
            block_on(next).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        scheduler.timers.shared.state.lock().unwrap().paused = None;
        assert_eq!(
            block_on(scheduler.sleep_until(deadline, &never))
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        block_on(scheduler.shutdown());
    });
}

#[test]
fn reserved_sleep_registration_repoll_and_cancellation_allocate_nothing() {
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Waker},
    };
    let scheduler = test_support::scheduler();
    scheduler.timers.shared.state.lock().unwrap().paused = Some(Instant::now());
    let token = Cancellation::new();
    let deadline = scheduler.now() + Duration::from_secs(3600);
    let allocations = allocation_counter::measure(|| {
        for _ in 0..32 {
            let mut sleep = pin!(scheduler.sleep_until(deadline, &token));
            assert!(
                sleep
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert!(
                sleep
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
    });
    assert_eq!(allocations.count_total, 0, "{allocations:?}");
    assert_eq!(scheduler.registrations(), 0);
    block_on(scheduler.shutdown());
}

#[test]
fn shutdown_waits_for_timer_delivery_outside_queue_locks() {
    use std::{
        sync::Arc,
        task::{Context, Wake, Waker},
    };
    test_support::run(|| {
        struct GatedWake(Arc<test_support::Gate>);
        impl Wake for GatedWake {
            fn wake(self: Arc<Self>) {
                self.0.wait();
            }
        }
        let scheduler = test_support::scheduler();
        let (gate, started, release) = test_support::Gate::new();
        let waker = Waker::from(Arc::new(GatedWake(gate)));
        let never = Cancellation::never();
        let mut sleep = Box::pin(scheduler.sleep(Duration::from_millis(10), &never));
        assert!(
            sleep
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        let mut shutdown = Box::pin(scheduler.shutdown());
        assert!(block_on(poll_once(&mut shutdown)).is_none());
        drop(release);
        block_on(shutdown);
        block_on(sleep).unwrap();
    });
}

#[test]
fn concurrent_shutdown_waits_for_close_wakeup_delivery() {
    use std::{
        sync::Arc,
        task::{Context, Wake, Waker},
    };
    test_support::run(|| {
        struct GatedWake(Arc<test_support::Gate>);
        impl Wake for GatedWake {
            fn wake(self: Arc<Self>) {
                self.0.wait();
            }
        }
        let scheduler = Arc::new(test_support::scheduler());
        scheduler.timers.shared.state.lock().unwrap().paused = Some(Instant::now());
        let (gate, started, release) = test_support::Gate::new();
        let waker = Waker::from(Arc::new(GatedWake(gate)));
        let never = Cancellation::never();
        let mut sleep = Box::pin(scheduler.sleep(Duration::from_secs(3600), &never));
        assert!(
            sleep
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let other = Arc::clone(&scheduler);
        let closing = std::thread::spawn(move || other.close());
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        let mut shutdown = Box::pin(scheduler.shutdown());
        assert!(block_on(poll_once(&mut shutdown)).is_none());
        drop(release);
        closing.join().unwrap();
        block_on(shutdown);
        assert_eq!(
            block_on(sleep).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    });
}

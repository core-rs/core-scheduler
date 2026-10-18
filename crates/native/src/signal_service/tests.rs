// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::test_support;
use core_scheduler::Cancellation;
use core_scheduler_conformance::SignalsFixture;
use futures_lite::future::block_on;
use std::panic::{AssertUnwindSafe, catch_unwind};

struct Fixture {
    scheduler: Arc<NativeScheduler>,
}
impl SignalsFixture for Fixture {
    type Scheduler = NativeScheduler;
    type Signal = i32;
    fn scheduler(&self) -> &Arc<NativeScheduler> {
        &self.scheduler
    }
    fn signals(&self) -> [i32; 2] {
        [libc::SIGUSR1, libc::SIGUSR2]
    }
    fn invalid_signal(&self) -> i32 {
        0
    }
    async fn emit(&self, signal: i32) {
        self.scheduler.signals.shared.dispatch(signal);
    }
}
fn run(test: impl AsyncFnOnce(&Fixture) + Send + 'static) {
    test_support::run(move || {
        let fixture = Fixture {
            scheduler: Arc::new(test_support::scheduler()),
        };
        let result = catch_unwind(AssertUnwindSafe(|| block_on(test(&fixture))));
        block_on(fixture.scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
mod conformance {
    core_scheduler_conformance::signals_tests!(run: super::run);
}
mod dispatch {
    core_scheduler_conformance::signals_dispatch_tests!(run: super::run);
}

#[test]
fn rejects_empty_forbidden_native_and_closed_registrations_and_startup_failure() {
    let scheduler = test_support::scheduler();
    for signals in [
        &[][..],
        &[0],
        &[128],
        &[libc::SIGKILL],
        &[libc::SIGSEGV],
        &[127],
    ] {
        assert!(
            scheduler
                .signal_subscribe(signals, Cancellation::never())
                .is_err()
        );
    }
    thread::FAIL_AFTER.set(Some(0));
    assert!(
        scheduler
            .signal_subscribe(&[libc::SIGUSR1], Cancellation::never())
            .is_err()
    );
    let subscription = scheduler
        .signal_subscribe(&[libc::SIGUSR1], Cancellation::never())
        .unwrap();
    assert!(
        scheduler
            .signal_subscribe(&[127], Cancellation::never())
            .is_err()
    );
    scheduler.signals.shared.state.lock().unwrap().next = usize::MAX;
    assert!(
        scheduler
            .signal_subscribe(&[libc::SIGUSR1], Cancellation::never())
            .is_err()
    );
    scheduler.close();
    assert!(
        scheduler
            .signal_subscribe(&[libc::SIGUSR1], Cancellation::never())
            .is_err()
    );
    block_on(scheduler.shutdown());
    drop(scheduler);
    drop(subscription);
}
#[test]
fn panicking_targets_do_not_poison_dispatch_or_prevent_other_targets() {
    struct Panics;
    impl Notify<i32> for Panics {
        fn notify(&self, _: i32) {
            panic!("signal callback");
        }
    }
    let scheduler = test_support::scheduler();
    let _panics = scheduler
        .signal_subscribe(&[libc::SIGUSR1], Panics)
        .unwrap();
    let token = Cancellation::new();
    let _other = scheduler
        .signal_subscribe(&[libc::SIGUSR1], token.clone())
        .unwrap();
    scheduler.signals.shared.dispatch(libc::SIGUSR1);
    assert!(token.is_cancelled());
    block_on(scheduler.shutdown());
}

#[test]
fn os_signal_bridge_delivers_outside_the_raw_handler() {
    const CHILD: &str = "CORE_NATIVE_SIGNAL_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "signal_service::tests::os_signal_bridge_delivers_outside_the_raw_handler",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    let scheduler = test_support::scheduler();
    let (sender, receiver) = std::sync::mpsc::channel();
    let _subscription = scheduler
        .signal_subscribe(&[libc::SIGUSR1], sender)
        .unwrap();
    // SAFETY: This child process installed its handler before raising the signal; no parallel tests share this process.
    assert_eq!(unsafe { libc::raise(libc::SIGUSR1) }, 0);
    assert_eq!(
        receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap(),
        libc::SIGUSR1
    );

    struct Target {
        gate: Arc<test_support::Gate>,
        dropped: std::sync::mpsc::Sender<()>,
    }
    impl Notify<i32> for Target {
        fn notify(&self, _: i32) {
            self.gate.wait();
        }
    }
    impl Drop for Target {
        fn drop(&mut self) {
            self.dropped.send(()).unwrap();
            panic!("signal target destructor");
        }
    }
    let (gate, entered, release) = test_support::Gate::new();
    let (dropped, destruction) = std::sync::mpsc::channel();
    let target = scheduler
        .signal_subscribe(&[libc::SIGUSR1], Target { gate, dropped })
        .unwrap();
    // SAFETY: The isolated child has already installed this signal handler.
    assert_eq!(unsafe { libc::raise(libc::SIGUSR1) }, 0);
    entered
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    // The active dispatch snapshot now retains the final target reference.
    drop(target);
    drop(release);
    destruction
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    receiver
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(unsafe { libc::raise(libc::SIGUSR1) }, 0);
    assert_eq!(
        receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap(),
        libc::SIGUSR1
    );
    test_support::run(move || block_on(scheduler.shutdown()));
}

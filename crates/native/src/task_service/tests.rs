// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::{NativeScheduler, test_support};
use core_scheduler::{Cancellation, Spawn};
use core_scheduler_conformance::SpawnFixture;
use futures_lite::future::block_on;

impl SpawnFixture for NativeScheduler {
    type Scheduler = Self;
    fn scheduler(&self) -> &Self {
        self
    }
    fn registration_check(&self) -> impl FnOnce() + Send + 'static {
        let shared = Arc::clone(&self.tasks.shared);
        move || assert_eq!(shared.state.lock().unwrap().registered.len(), 1)
    }
    async fn settle(&self) {
        self.tasks.wait(false).await;
    }
    fn close(&self) {
        self.tasks.close();
    }
}
fn run(test: impl AsyncFnOnce(&NativeScheduler) + Send + 'static) {
    test_support::run(move || {
        let scheduler = test_support::scheduler();
        let result = catch_unwind(AssertUnwindSafe(|| block_on(test(&scheduler))));
        block_on(scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
mod conformance {
    core_scheduler_conformance::spawn_tests!(run: super::run);
}
mod dispatch {
    core_scheduler_conformance::spawn_dispatch_tests!(run: super::run);
}

#[test]
fn startup_and_identifier_exhaustion_are_reported_without_polling_inputs() {
    for fail_after in [0, 1] {
        thread::FAIL_AFTER.set(Some(fail_after));
        assert!(
            NativeScheduler::with_options(crate::NativeOptions {
                workers: 2,
                threads: 2,
                capacity: 1,
                ..Default::default()
            })
            .is_err()
        );
    }
    let scheduler = test_support::scheduler();
    scheduler.tasks.shared.state.lock().unwrap().next = usize::MAX;
    assert!(matches!(
        scheduler.spawn(async {}),
        Err(SpawnError::IdentifiersExhausted)
    ));
    block_on(scheduler.shutdown());
}

#[test]
fn tasks_progress_without_occupying_syscall_workers_and_survive_admission_close() {
    test_support::run(|| {
        block_on(async {
            use core_scheduler::Blocking;
            let scheduler = test_support::scheduler();
            let token = Cancellation::new();
            let child = token.clone();
            let task = scheduler
                .spawn(async move {
                    child.cancelled().await;
                    42
                })
                .unwrap();
            scheduler.tasks.wait(false).await;
            let result = scheduler
                .spawn_blocking(|| 73, &Cancellation::never())
                .await
                .unwrap();
            assert_eq!(result.await.unwrap(), 73);
            scheduler.tasks.close();
            token.cancel();
            assert_eq!(task.await.unwrap(), 42);
            scheduler.shutdown().await;
        })
    });
}

#[test]
fn future_destructor_panic_is_reported_and_worker_stays_available() {
    struct Panics;
    impl Future for Panics {
        type Output = ();
        fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }
    impl Drop for Panics {
        fn drop(&mut self) {
            std::panic::panic_any(String::from("future destructor"));
        }
    }
    run(async |scheduler| {
        let Err(TaskError::Panicked(payload)) = scheduler.spawn(Panics).unwrap().await else {
            panic!("lost destructor panic");
        };
        assert_eq!(*payload.downcast::<String>().unwrap(), "future destructor");
        assert_eq!(scheduler.spawn(async { 17 }).unwrap().await.unwrap(), 17);
    });
}

#[test]
fn abandoned_task_destructor_panics_do_not_abort_or_stop_workers() {
    const CHILD: &str = "CORE_NATIVE_TASK_DROP_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "task_service::tests::abandoned_task_destructor_panics_do_not_abort_or_stop_workers",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    struct PanicsOnDrop;
    impl Drop for PanicsOnDrop {
        fn drop(&mut self) {
            panic!("abandoned destructor");
        }
    }
    struct Pending;
    impl Future for Pending {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }
    impl Drop for Pending {
        fn drop(&mut self) {
            // The payload also panics when destroyed: swallowing only the first unwind is insufficient.
            std::panic::panic_any(PanicsOnDrop);
        }
    }
    struct Ready;
    impl Future for Ready {
        type Output = PanicsOnDrop;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(PanicsOnDrop)
        }
    }
    impl Drop for Ready {
        fn drop(&mut self) {
            panic!("completed future destructor");
        }
    }
    run(async |scheduler| {
        let pending = scheduler.spawn(Pending).unwrap();
        scheduler.tasks.wait(false).await;
        drop(pending);
        scheduler.tasks.drain().await;

        let output = scheduler.spawn(async { PanicsOnDrop }).unwrap();
        scheduler.tasks.wait(false).await;
        drop(output);

        let panic_payload = scheduler
            .spawn(async { std::panic::panic_any(PanicsOnDrop) })
            .unwrap();
        scheduler.tasks.wait(false).await;
        drop(panic_payload);

        // Both the completed future and its unconsumed output panic during destruction.
        assert!(matches!(
            scheduler.spawn(Ready).unwrap().await,
            Err(TaskError::Panicked(_))
        ));
        assert_eq!(scheduler.spawn(async { 42 }).unwrap().await.unwrap(), 42);
    });
}

#[test]
fn local_work_can_be_stolen_while_its_worker_is_busy() {
    test_support::run(|| {
        let scheduler = Arc::new(
            NativeScheduler::with_options(crate::NativeOptions {
                workers: 2,
                ..Default::default()
            })
            .unwrap(),
        );
        let inner = Arc::clone(&scheduler);
        let task = scheduler
            .spawn(async move {
                let (done, completion) = std::sync::mpsc::channel();
                let child = inner
                    .spawn(async move {
                        done.send(std::thread::current().id()).unwrap();
                        42
                    })
                    .unwrap();
                let other_thread = completion
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                assert_ne!(other_thread, std::thread::current().id());
                assert_eq!(child.await.unwrap(), 42);
            })
            .unwrap();
        block_on(task).unwrap();
        block_on(scheduler.shutdown());
    });
}

#[test]
fn busy_local_queue_regularly_services_external_submissions() {
    test_support::run(|| {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let scheduler = test_support::scheduler();
        let (started, start) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let observed = Arc::new(AtomicBool::new(false));
        let polls = Arc::new(AtomicUsize::new(0));
        let result = Arc::clone(&observed);
        let count = Arc::clone(&polls);
        let hot = scheduler
            .spawn(std::future::poll_fn(move |context| {
                let poll = count.fetch_add(1, Ordering::Relaxed);
                if poll == 0 {
                    started.send(()).unwrap();
                    released
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                }
                if result.load(Ordering::Acquire) || poll == 1000 {
                    return Poll::Ready(());
                }
                context.waker().wake_by_ref();
                Poll::Pending
            }))
            .unwrap();
        start
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let other = scheduler
            .spawn(async move {
                observed.store(true, Ordering::Release);
            })
            .unwrap();
        release.send(()).unwrap();
        block_on(hot).unwrap();
        assert!(
            polls.load(Ordering::Relaxed) < 1000,
            "external work was starved by local wakes"
        );
        block_on(other).unwrap();
        block_on(scheduler.shutdown());
    });
}

#[test]
fn submissions_from_another_native_worker_use_the_selected_scheduler() {
    test_support::run(|| {
        let first = test_support::scheduler();
        let second = Arc::new(test_support::scheduler());
        let other = Arc::clone(&second);
        let task = first
            .spawn(async move {
                let submitting = std::thread::current().id();
                assert_ne!(
                    other
                        .spawn(async { std::thread::current().id() })
                        .unwrap()
                        .await
                        .unwrap(),
                    submitting
                );
            })
            .unwrap();
        block_on(task).unwrap();
        block_on(first.shutdown());
        block_on(second.shutdown());
    });
}

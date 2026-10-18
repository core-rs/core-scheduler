// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use futures_lite::future::{block_on, poll_once};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::task::Poll;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(10);
fn scheduler(capacity: usize) -> NativeScheduler {
    NativeScheduler::with_options(NativeOptions {
        workers: 1,
        threads: 1,
        capacity,
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn invalid_options_and_closed_services_return_errors_and_buffers() {
    for options in [
        NativeOptions {
            workers: 1,
            threads: 0,
            capacity: 1,
            ..Default::default()
        },
        NativeOptions {
            workers: 1,
            threads: 1,
            capacity: 0,
            ..Default::default()
        },
    ] {
        assert_eq!(
            NativeScheduler::with_options(options).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
    }
    let scheduler = scheduler(1);
    let file = NativeFile::from_std(tempfile::tempfile().unwrap());
    scheduler.close();
    let buffer = vec![1; 4];
    let allocation = buffer.as_ptr();
    let (result, buffer) = block_on(scheduler.file_read(&file, buffer, &Cancellation::never()));
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(buffer.as_ptr(), allocation);
    assert_eq!(
        block_on(scheduler.file_open(
            Path::new("unused"),
            FileOptions::READ,
            &Cancellation::never()
        ))
        .unwrap_err()
        .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        block_on(scheduler.file_close(file)).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        block_on(scheduler.file_close_submit(NativeFile::from_std(tempfile::tempfile().unwrap())))
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert!(matches!(
        block_on(scheduler.spawn_blocking(|| (), &Cancellation::never())),
        Err(BlockingError::SchedulerClosed)
    ));
    block_on(scheduler.shutdown());
}

#[test]
fn startup_failures_preserve_io_ownership_and_report_cleanup_errors() {
    let mut scheduler = scheduler(1);
    scheduler.work.fail_start_at = Some(0);
    let file = NativeFile::from_std(tempfile::tempfile().unwrap());
    let (result, buffer) = block_on(scheduler.file_read(&file, vec![9], &Cancellation::never()));
    assert_eq!(buffer, vec![9]);
    assert_eq!(
        result.unwrap_err().to_string(),
        "injected thread startup failure"
    );
    scheduler.cleanup.fail_start_at = Some(0);
    assert_eq!(
        block_on(scheduler.file_close(file))
            .unwrap_err()
            .to_string(),
        "injected thread startup failure"
    );
    block_on(scheduler.shutdown());
}

#[test]
fn file_errors_and_exclusive_close_are_observable() {
    let scheduler = scheduler(2);
    let directory = tempfile::tempdir().unwrap();
    assert!(
        block_on(scheduler.file_open(
            &directory.path().join("missing"),
            FileOptions::READ,
            &Cancellation::never()
        ))
        .is_err()
    );
    let file = NativeFile::from_std(tempfile::tempfile().unwrap());
    let retained = file.retain();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(
            scheduler.file_close(file)
        )))
        .is_err()
    );
    assert!(retained.metadata().is_ok());
    drop(retained);
    let file = NativeFile::from_std(tempfile::tempfile().unwrap());
    let weak = Arc::downgrade(&file.0);
    drop(scheduler.file_close(file));
    assert_eq!(weak.strong_count(), 0);
    block_on(scheduler.shutdown());
}

#[test]
fn cleanup_makes_progress_independently_and_accepted_close_survives_abandonment() {
    let scheduler = scheduler(1);
    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let running = block_on(scheduler.spawn_blocking(
        move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        },
        &Cancellation::never(),
    ))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    block_on(scheduler.file_close(NativeFile::from_std(tempfile::tempfile().unwrap()))).unwrap();
    release.send(()).unwrap();
    block_on(running).unwrap();

    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let busy = block_on(scheduler.cleanup.submit(move || {
        started.send(()).unwrap();
        released.recv_timeout(TIMEOUT).unwrap();
    }))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    // The cleanup pool is full: a waiter can abandon admission, releasing its file.
    let file = NativeFile::from_std(tempfile::tempfile().unwrap());
    let weak = Arc::downgrade(&file.0);
    let mut waiting = Box::pin(scheduler.file_close(file));
    assert!(block_on(poll_once(&mut waiting)).is_none());
    drop(waiting);
    assert_eq!(weak.strong_count(), 0);
    release.send(()).unwrap();
    block_on(busy).unwrap().unwrap();
    block_on(scheduler.cleanup.drain());

    let scheduler = scheduler_with_two_cleanup_slots();
    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let busy = block_on(scheduler.cleanup.submit(move || {
        started.send(()).unwrap();
        released.recv_timeout(TIMEOUT).unwrap();
    }))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    let mut close =
        Box::pin(scheduler.file_close(NativeFile::from_std(tempfile::tempfile().unwrap())));
    assert!(block_on(poll_once(&mut close)).is_none());
    drop(close);
    let mut shutdown = Box::pin(scheduler.shutdown());
    assert!(block_on(poll_once(&mut shutdown)).is_none());
    release.send(()).unwrap();
    block_on(busy).unwrap().unwrap();
    block_on(shutdown);
}
fn scheduler_with_two_cleanup_slots() -> NativeScheduler {
    scheduler(2)
}

#[test]
fn cleanup_completion_preserves_os_and_worker_errors() {
    for outcome in [
        Ok(Err(io::Error::from(io::ErrorKind::PermissionDenied))),
        Err(TaskError::Panicked(Box::new("panic"))),
    ] {
        let (sender, receiver) = oneshot::channel();
        sender.send(outcome).unwrap();
        assert!(block_on(wait_close(receiver)).is_err());
    }
    let (sender, receiver) = oneshot::channel();
    drop(sender);
    assert!(block_on(wait_close(receiver)).is_err());
}

struct GatedBuffer {
    data: Vec<u8>,
    started: Option<mpsc::Sender<()>>,
    released: mpsc::Receiver<()>,
    dropped: Arc<AtomicUsize>,
    completion: Option<oneshot::Sender<()>>,
}
impl AsRef<[u8]> for GatedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
impl AsMut<[u8]> for GatedBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        if let Some(started) = self.started.take() {
            started.send(()).unwrap();
            self.released.recv_timeout(TIMEOUT).unwrap();
        }
        &mut self.data
    }
}
// SAFETY: The buffer exclusively owns stable heap storage and exposes only its initialized range. Synchronization gates access without changing allocation or length.
unsafe impl IoBuf for GatedBuffer {}
unsafe impl IoBufMut for GatedBuffer {}
impl Drop for GatedBuffer {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
        let _ = self.completion.take().unwrap().send(());
    }
}

#[test]
fn abandoned_io_and_scheduler_drop_retain_worker_access_until_retirement() {
    let scheduler = scheduler(1);
    let file = NativeFile::from_std(tempfile::tempfile().unwrap());
    let retained = Arc::downgrade(&file.0);
    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (completion, completed) = oneshot::channel();
    let dropped = Arc::new(AtomicUsize::new(0));
    let buffer = GatedBuffer {
        data: vec![0; 4],
        started: Some(started),
        released,
        dropped: Arc::clone(&dropped),
        completion: Some(completion),
    };
    let never = Cancellation::never();
    let mut reading = Box::pin(scheduler.file_read(&file, buffer, &never));
    assert!(block_on(poll_once(&mut reading)).is_none());
    start.recv_timeout(TIMEOUT).unwrap();
    drop(reading);
    drop(file);
    assert_eq!(retained.strong_count(), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(scheduler);
    assert_eq!(retained.strong_count(), 1);
    release.send(()).unwrap();
    block_on(completed).unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn running_cancellation_preserves_results_and_abandonment_keeps_capacity_occupied() {
    let scheduler = scheduler(1);
    for abandon in [false, true] {
        let token = Cancellation::new();
        let (started, start) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let running = block_on(scheduler.spawn_blocking(
            move || {
                started.send(()).unwrap();
                released.recv_timeout(TIMEOUT).unwrap();
                37
            },
            &token,
        ))
        .unwrap();
        start.recv_timeout(TIMEOUT).unwrap();
        token.cancel();
        if abandon {
            drop(running);
            let never = Cancellation::never();
            let mut next = Box::pin(scheduler.spawn_blocking(|| 42, &never));
            assert!(block_on(poll_once(&mut next)).is_none());
            release.send(()).unwrap();
            assert_eq!(block_on(block_on(next).unwrap()).unwrap(), 42);
        } else {
            release.send(()).unwrap();
            assert_eq!(block_on(running).unwrap(), 37);
        }
    }
    block_on(scheduler.shutdown());
}

#[test]
fn cancellation_after_io_dispatch_preserves_the_actual_result() {
    let scheduler = scheduler(1);
    let token = Cancellation::new();
    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let mut operation = Box::pin(scheduler.work.execute(vec![3], &token, move |input| {
        started.send(()).unwrap();
        released.recv_timeout(TIMEOUT).unwrap();
        input[0] = 7;
        Ok(11)
    }));
    assert!(block_on(poll_once(&mut operation)).is_none());
    start.recv_timeout(TIMEOUT).unwrap();
    token.cancel();
    release.send(()).unwrap();
    let (result, buffer) = block_on(operation);
    assert_eq!(result.unwrap(), 11);
    assert_eq!(buffer, vec![7]);
    block_on(scheduler.shutdown());
}

#[test]
fn default_scheduler_and_borrowed_non_send_root_future_work() {
    let scheduler = NativeScheduler::new().unwrap();
    assert!(scheduler.worker_count() > 0);
    let value = std::rc::Rc::new(42);
    scheduler.block_on(async {
        assert_eq!(*value, 42);
        let mut first = true;
        std::future::poll_fn(|context| {
            if std::mem::take(&mut first) {
                context.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        let mut first = true;
        std::future::poll_fn(|context| {
            if std::mem::take(&mut first) {
                // Exercise owned wake dispatch as well as wake_by_ref.
                #[allow(clippy::waker_clone_wake)]
                context.waker().clone().wake();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        assert_eq!(scheduler.spawn(async { 73 }).unwrap().await.unwrap(), 73);
        scheduler.shutdown().await;
    });
}

#[test]
fn zero_executor_workers_and_timer_slots_are_rejected() {
    for options in [
        NativeOptions {
            workers: 0,
            ..Default::default()
        },
        NativeOptions {
            timer_capacity: 0,
            ..Default::default()
        },
    ] {
        assert_eq!(
            NativeScheduler::with_options(options).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

#[test]
fn block_on_inside_a_worker_is_rejected_without_stopping_execution() {
    crate::test_support::run(|| {
        let scheduler = Arc::new(scheduler(1));
        let inner = Arc::clone(&scheduler);
        let task = scheduler
            .spawn(async move { inner.block_on(async {}) })
            .unwrap();
        assert!(matches!(block_on(task), Err(TaskError::Panicked(_))));
        assert_eq!(
            block_on(scheduler.spawn(async { 42 }).unwrap()).unwrap(),
            42
        );
        block_on(scheduler.shutdown());
    });
}

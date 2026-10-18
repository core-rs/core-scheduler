// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use core_scheduler::{
    Blocking, Cancellation, FileClose, FileMetadata, FileOpen, FileOptions, FileStat,
};
use core_scheduler_conformance::{blocking, file};
use core_scheduler_native::{NativeOptions, NativeScheduler};
use futures_lite::future::block_on;
use std::{sync::mpsc, time::Duration};

fn run(test: impl FnOnce() + Send + 'static) {
    let (done, completion) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(std::panic::catch_unwind(std::panic::AssertUnwindSafe(test)));
    });
    if let Err(payload) = completion
        .recv_timeout(Duration::from_secs(20))
        .expect("conformance test deadline exceeded")
    {
        std::panic::resume_unwind(payload);
    }
}

struct FileFixture {
    scheduler: NativeScheduler,
    directory: tempfile::TempDir,
    path: std::path::PathBuf,
}
impl core_scheduler_conformance::FileTestFixture for FileFixture {
    type Scheduler = NativeScheduler;
    fn scheduler(&self) -> &NativeScheduler {
        &self.scheduler
    }
    fn path(&self) -> &std::path::Path {
        &self.path
    }
    fn assert_absent(&self) {
        assert!(!self.path.exists());
    }
    fn assert_contents(&self, expected: &[u8]) {
        assert_eq!(std::fs::read(&self.path).unwrap(), expected);
    }
}
fn run_file(test: impl AsyncFnOnce(&FileFixture) + Send + 'static) {
    run(move || {
        let directory = tempfile::tempdir().unwrap();
        let fixture = FileFixture {
            scheduler: NativeScheduler::with_options(NativeOptions {
                workers: 1,
                ..Default::default()
            })
            .unwrap(),
            path: directory.path().join("file"),
            directory,
        };
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&fixture))));
        block_on(fixture.scheduler.shutdown());
        // Keep the temporary directory until every accepted operation has retired, including after a failed assertion.
        drop(fixture.directory);
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
mod files {
    use super::*;
    use core_scheduler_conformance as conformance;
    // Public macros must resolve support paths even when a caller shadows `std`.
    mod std {}
    conformance::file_tests!(run: run_file);
}

#[test]
fn files_move_between_threads_and_compatible_scheduler_instances() {
    run(|| {
        block_on(async {
            let first = NativeScheduler::with_options(NativeOptions {
                workers: 1,
                ..Default::default()
            })
            .unwrap();
            let second = NativeScheduler::with_options(NativeOptions {
                workers: 1,
                threads: 1,
                capacity: 1,
                ..Default::default()
            })
            .unwrap();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("file");
            std::fs::write(&path, b"abcdef").unwrap();
            let file = first
                .file_open(&path, FileOptions::READ, &Cancellation::never())
                .await
                .unwrap();
            std::thread::scope(|scope| {
                scope
                    .spawn(|| block_on(file::read(&second, &file, b"abcdef")))
                    .join()
                    .unwrap();
            });
            let metadata = second
                .file_stat(&file, &Cancellation::never())
                .await
                .unwrap();
            assert_eq!(metadata.size(), Some(6));
            second.file_close(file).await.unwrap();
            first.shutdown().await;
            second.shutdown().await;
        })
    });
}

#[cfg(unix)]
#[test]
fn unix_positioned_operations_preserve_the_shared_cursor() {
    use core_scheduler::FileSeek;
    use std::io::SeekFrom;

    block_on(async {
        let scheduler = NativeScheduler::with_options(NativeOptions {
            workers: 1,
            ..Default::default()
        })
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("file");
        std::fs::write(&path, b"abcdef").unwrap();
        let file = scheduler
            .file_open(
                &path,
                FileOptions {
                    write: true,
                    ..FileOptions::READ
                },
                &Cancellation::never(),
            )
            .await
            .unwrap();
        scheduler
            .file_seek(&file, SeekFrom::Start(3), &Cancellation::never())
            .await
            .unwrap();
        file::read_at(&scheduler, &file, 0, b"abcdef").await;
        file::write_at(&scheduler, &file, 0, b"ABC").await;
        assert_eq!(std::fs::read(&path).unwrap(), b"ABCdef");
        assert_eq!(
            scheduler
                .file_seek(&file, SeekFrom::Current(0), &Cancellation::never())
                .await
                .unwrap(),
            3
        );
        scheduler.file_close(file).await.unwrap();
        scheduler.shutdown().await;
    });
}

struct QueueFixture(NativeScheduler);
struct Hold {
    release: Option<mpsc::Sender<()>>,
    _handle: core_scheduler_native::BlockingTask<()>,
}
impl Drop for Hold {
    fn drop(&mut self) {
        let _ = self.release.take().unwrap().send(());
    }
}
impl blocking::BlockingFixture for QueueFixture {
    type Scheduler = NativeScheduler;
    type Guard = Hold;
    fn scheduler(&self) -> &Self::Scheduler {
        &self.0
    }
    async fn hold_workers(&self) -> Hold {
        let (started, start) = futures_channel::oneshot::channel();
        let (release, released) = mpsc::channel();
        let handle = self
            .0
            .spawn_blocking(
                move || {
                    started.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(10)).unwrap();
                },
                &Cancellation::never(),
            )
            .await
            .unwrap();
        start.await.unwrap();
        Hold {
            release: Some(release),
            _handle: handle,
        }
    }
}

fn run_blocking(test: impl AsyncFnOnce(&NativeScheduler) + Send + 'static) {
    run(move || {
        let scheduler = NativeScheduler::with_options(NativeOptions {
            workers: 1,
            ..Default::default()
        })
        .unwrap();
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&scheduler))));
        block_on(scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
fn run_queue(test: impl AsyncFnOnce(&QueueFixture) + Send + 'static) {
    run(move || {
        let fixture = QueueFixture(
            NativeScheduler::with_options(NativeOptions {
                workers: 1,
                threads: 1,
                capacity: 2,
                ..Default::default()
            })
            .unwrap(),
        );
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&fixture))));
        block_on(fixture.0.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
mod blocking_work {
    use super::*;
    core_scheduler_conformance::blocking_tests!(run: run_blocking);
}
mod blocking_queue {
    use super::*;
    core_scheduler_conformance::blocking_queue_tests!(run: run_queue);
}

#[derive(Clone, Copy)]
enum QueueFault {
    IgnoreQueuedCancellation,
    IgnoreCapacity,
    AdmitOnCancellation,
    DetachOnDrop,
}
struct IncorrectQueue {
    direct: NativeScheduler,
    calls: std::sync::atomic::AtomicUsize,
    fault: QueueFault,
}
impl Blocking for IncorrectQueue {
    type Handle<T>
        = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<T, core_scheduler::TaskError>> + Send>,
    >
    where
        T: Send + 'static;
    async fn spawn_blocking<F, T>(
        &self,
        operation: F,
        cancellation: &Cancellation,
    ) -> Result<Self::Handle<T>, core_scheduler::BlockingError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if (call == 1 && matches!(self.fault, QueueFault::IgnoreCapacity))
            || (call == 1 && matches!(self.fault, QueueFault::AdmitOnCancellation))
        {
            if matches!(self.fault, QueueFault::AdmitOnCancellation) {
                cancellation.cancelled().await;
            }
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation))
                .map_err(core_scheduler::TaskError::Panicked);
            return Ok(Box::pin(std::future::ready(result)));
        }
        let never = Cancellation::never();
        let token = if call == 0 && matches!(self.fault, QueueFault::IgnoreQueuedCancellation) {
            &never
        } else {
            cancellation
        };
        let handle = self.direct.spawn_blocking(operation, token).await?;
        if call == 2 && matches!(self.fault, QueueFault::DetachOnDrop) {
            std::thread::spawn(move || {
                let _ = block_on(handle);
            });
            return Ok(Box::pin(std::future::pending()));
        }
        Ok(Box::pin(handle))
    }
}
struct IncorrectFixture(IncorrectQueue);
impl blocking::BlockingFixture for IncorrectFixture {
    type Scheduler = IncorrectQueue;
    type Guard = Hold;
    fn scheduler(&self) -> &IncorrectQueue {
        &self.0
    }
    async fn hold_workers(&self) -> Hold {
        let (started, start) = futures_channel::oneshot::channel();
        let (release, released) = mpsc::channel();
        let handle = self
            .0
            .direct
            .spawn_blocking(
                move || {
                    started.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(10)).unwrap();
                },
                &Cancellation::never(),
            )
            .await
            .unwrap();
        start.await.unwrap();
        Hold {
            release: Some(release),
            _handle: handle,
        }
    }
}
#[test]
fn queue_conformance_rejects_cancellation_capacity_and_handle_ownership_violations() {
    for fault in [
        QueueFault::IgnoreQueuedCancellation,
        QueueFault::IgnoreCapacity,
        QueueFault::AdmitOnCancellation,
        QueueFault::DetachOnDrop,
    ] {
        let fixture = IncorrectFixture(IncorrectQueue {
            direct: NativeScheduler::with_options(NativeOptions {
                workers: 1,
                threads: 1,
                capacity: 2,
                ..Default::default()
            })
            .unwrap(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            fault,
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            block_on(blocking::queue(&fixture))
        }));
        block_on(fixture.0.direct.shutdown());
        assert!(result.is_err(), "conformance accepted an incorrect backend");
    }
}

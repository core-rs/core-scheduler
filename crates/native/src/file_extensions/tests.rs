// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::test_support;
use core_scheduler_conformance::{FileAdviceFixture, FileAllocateFixture};
use futures_lite::future::{block_on, poll_once};
use std::{
    io::{Seek, SeekFrom, Write},
    sync::{Arc, Mutex, mpsc},
};

struct Fixture {
    scheduler: NativeScheduler,
    file: NativeFile,
    read_only: NativeFile,
    failing: NativeFile,
    path: std::path::PathBuf,
    initial_blocks: u64,
    _directory: tempfile::TempDir,
    started: Mutex<Option<mpsc::Receiver<()>>>,
}
impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("file");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.write_all(b"abcdef").unwrap();
        file.sync_all().unwrap();
        file.seek(SeekFrom::Start(3)).unwrap();
        let read_only = std::fs::File::open(&path).unwrap();
        use std::os::{
            fd::{FromRawFd, OwnedFd},
            unix::fs::MetadataExt,
        };
        let initial_blocks = file.metadata().unwrap().blocks();
        let mut pipes = [-1; 2];
        // SAFETY: pipe initializes both descriptors on success; each is immediately transferred to exactly one owner.
        assert_eq!(unsafe { libc::pipe(pipes.as_mut_ptr()) }, 0);
        let pipe = unsafe { OwnedFd::from_raw_fd(pipes[0]) };
        drop(unsafe { OwnedFd::from_raw_fd(pipes[1]) });
        Self {
            scheduler: test_support::scheduler(),
            file: NativeFile::from_std(file),
            read_only: NativeFile::from_std(read_only),
            failing: NativeFile::from_std(pipe.into()),
            path,
            initial_blocks,
            _directory: directory,
            started: Mutex::new(None),
        }
    }
    fn state(&self, contents: &[u8], cursor: u64) {
        assert_eq!(std::fs::read(&self.path).unwrap(), contents);
        assert_eq!((&*self.file.0).stream_position().unwrap(), cursor);
    }
}
impl FileAllocateFixture for Fixture {
    type Scheduler = NativeScheduler;
    fn scheduler(&self) -> &NativeScheduler {
        &self.scheduler
    }
    fn file(&self) -> &NativeFile {
        &self.file
    }
    fn read_only_file(&self) -> &NativeFile {
        &self.read_only
    }
    fn assert_state(&self, contents: &[u8], cursor: u64) {
        self.state(contents, cursor);
    }
    fn assert_reserved(&self, offset: u64, length: u64) {
        use std::os::unix::fs::MetadataExt;
        assert!(
            self.scheduler
                .file_probe
                .allocations
                .lock()
                .unwrap()
                .iter()
                .any(
                    |&(actual_offset, actual_length, _)| (actual_offset, actual_length)
                        == (offset, length)
                )
        );
        let blocks = self.file.0.metadata().unwrap().blocks();
        if offset >= 65536 {
            assert!(
                blocks > self.initial_blocks,
                "allocation beyond EOF must reserve additional blocks"
            );
        } else {
            assert!(blocks >= self.initial_blocks);
        }
    }
    fn calls(&self) -> usize {
        self.scheduler.file_probe.allocations.lock().unwrap().len()
    }
}
struct Hold {
    probe: Arc<Probe>,
    _release: test_support::Release,
}
impl Drop for Hold {
    fn drop(&mut self) {
        self.probe.gate.lock().unwrap().take();
    }
}
impl FileAdviceFixture for Fixture {
    type Scheduler = NativeScheduler;
    type Guard = Hold;
    fn scheduler(&self) -> &NativeScheduler {
        &self.scheduler
    }
    fn file(&self) -> &NativeFile {
        &self.file
    }
    fn failing_file(&self) -> &NativeFile {
        &self.failing
    }
    fn error_kind(&self) -> io::ErrorKind {
        io::Error::from_raw_os_error(libc::ESPIPE).kind()
    }
    fn assert_state(&self, contents: &[u8], cursor: u64) {
        self.state(contents, cursor);
    }
    fn observed(&self) -> Vec<(u64, u64, AdviceMode)> {
        self.scheduler.file_probe.advice.lock().unwrap().clone()
    }
    fn hold(&self) -> Hold {
        let (gate, started, release) = test_support::Gate::new();
        *self.scheduler.file_probe.gate.lock().unwrap() = Some(gate);
        *self.started.lock().unwrap() = Some(started);
        Hold {
            probe: Arc::clone(&self.scheduler.file_probe),
            _release: release,
        }
    }
    async fn accepted(&self) {
        self.started
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
    }
    fn assert_retained(&self) {
        assert!(Arc::strong_count(&self.file.0) > 1);
    }
    async fn settle(&self) {
        self.scheduler.cleanup.drain().await;
    }
}
fn run(test: impl AsyncFnOnce(&Fixture) + Send + 'static) {
    test_support::run(move || {
        let fixture = Fixture::new();
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&fixture))));
        block_on(fixture.scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
mod allocation {
    core_scheduler_conformance::file_allocate_tests!(run: super::run);
}
mod advice {
    core_scheduler_conformance::file_advice_tests!(run: super::run);
}

#[test]
fn advice_waiting_for_capacity_can_be_abandoned_and_closed_admission_reports_errors() {
    run(async |fixture| {
        // Fill cleanup admission without involving the ordinary I/O workers.
        let (gate, started, release) = test_support::Gate::new();
        let mut jobs = Vec::new();
        for index in 0..fixture.scheduler.cleanup.options.capacity {
            let gate = Arc::clone(&gate);
            jobs.push(
                fixture
                    .scheduler
                    .cleanup
                    .submit(move || {
                        if index == 0 {
                            gate.wait();
                        }
                    })
                    .await
                    .unwrap(),
            );
        }
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let mut pending = Box::pin(fixture.scheduler.file_advise_submit(
            &fixture.file,
            0,
            0,
            AdviceMode::Normal,
        ));
        assert!(poll_once(&mut pending).await.is_none());
        drop(pending);
        drop(release);
        drop(jobs);
        fixture.scheduler.close();
        assert!(
            fixture
                .scheduler
                .file_advise(&fixture.file, 0, 0, AdviceMode::Normal)
                .await
                .is_err()
        );
        assert!(
            fixture
                .scheduler
                .file_advise_submit(&fixture.file, 0, 0, AdviceMode::Normal)
                .await
                .is_err()
        );
    });
}

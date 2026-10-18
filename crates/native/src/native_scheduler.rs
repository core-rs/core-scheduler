// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    BlockingTask, NativeFile, NativeOptions, SystemFileMetadata,
    blocking_pool::{BlockingPool, admission_error},
    file_io,
};
use core_scheduler::{
    Blocking, BlockingError, BufferResult, Cancellation, FileClose, FileOpen, FileOperation,
    FileOptions, FileRead, FileReadAt, FileSeek, FileStat, FileSync, FileTruncate, FileWrite,
    FileWriteAt, IoBuf, IoBufMut, OperationResult, Spawn, SpawnError, SyncMode, TaskError,
};
use futures_channel::oneshot;
use std::{
    io::{self, SeekFrom},
    path::Path,
};

/// Explicit task, clock, file, socket, and blocking-work services, independent of the executor polling their operation futures.
///
/// Native instances accept the same file and socket resources. Tasks use work stealing, sockets use readiness, blocking work and cleanup use independent bounded pools, and timers have a dedicated driver.
pub struct NativeScheduler {
    pub(crate) work: BlockingPool,
    pub(crate) cleanup: BlockingPool,
    pub(crate) closed: Cancellation,
    pub(crate) tasks: crate::task_service::TaskService,
    pub(crate) timers: crate::timer_service::TimerService,
    #[cfg(test)]
    pub(crate) option_probe: std::sync::Arc<crate::socket_options::Probe>,
    #[cfg(test)]
    pub(crate) resolver_probe: std::sync::Arc<crate::resolver::Probe>,
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) file_probe: std::sync::Arc<crate::file_extensions::Probe>,
    #[cfg(unix)]
    pub(crate) signals: crate::signal_service::SignalService,
}

impl NativeScheduler {
    /// Start an executor with default limits.
    pub fn new() -> io::Result<Self> {
        Self::with_options(NativeOptions::default())
    }

    /// Number of configured work-stealing workers.
    pub fn worker_count(&self) -> usize {
        self.tasks.shared.workers.len()
    }

    /// Poll a root future on this thread while workers execute spawned tasks. The root may borrow local state and need not be `Send`.
    ///
    /// # Panics
    ///
    /// Panics on a native task worker, where blocking would prevent task progress. Use `.await` inside tasks.
    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        use event_listener::Listener;
        use std::{
            pin::pin,
            sync::Arc,
            task::{Context, Poll, Wake, Waker},
        };
        struct Notification(event_listener::Event);
        impl Wake for Notification {
            fn wake(self: Arc<Self>) {
                self.0.notify(1);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.notify(1);
            }
        }
        crate::worker::assert_outside_worker();
        let notification = Arc::new(Notification(event_listener::Event::new()));
        let waker = Waker::from(Arc::clone(&notification));
        let mut future = pin!(future);
        loop {
            let listener = notification.0.listen();
            match future.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(output) => return output,
                Poll::Pending => listener.wait(),
            }
        }
    }
    pub(crate) fn check_open(&self) -> io::Result<()> {
        if self.closed.is_cancelled() {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "scheduler is closed",
            ))
        } else {
            Ok(())
        }
    }

    /// Start task polling workers and configure lazy I/O, timer, and signal worker startup. Counts and capacity must be nonzero.
    pub fn with_options(options: NativeOptions) -> io::Result<Self> {
        if options.workers == 0
            || options.threads == 0
            || options.capacity == 0
            || options.timer_capacity == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker counts and capacities must be nonzero",
            ));
        }
        Ok(Self {
            tasks: crate::task_service::TaskService::new(options.workers)?,
            timers: crate::timer_service::TimerService::new(options.timer_capacity),
            #[cfg(test)]
            option_probe: Default::default(),
            #[cfg(test)]
            resolver_probe: Default::default(),
            #[cfg(all(test, target_os = "linux"))]
            file_probe: Default::default(),
            #[cfg(unix)]
            signals: crate::signal_service::SignalService::new(),
            closed: Cancellation::new(),
            work: BlockingPool::new(options),
            cleanup: BlockingPool::new(NativeOptions {
                threads: 1,
                capacity: options.capacity,
                ..Default::default()
            }),
        })
    }

    /// Stop admission. Queued general blocking jobs are discarded; accepted I/O and cleanup retire normally. Running closures cannot be interrupted.
    ///
    /// Dropping the scheduler also closes admission without blocking the dropping thread. Workers retain outstanding resources until access ends.
    pub fn close(&self) {
        self.work.close();
        self.cleanup.close();
        self.closed.cancel();
        self.tasks.close();
        self.timers.close();
        #[cfg(unix)]
        self.signals.close();
    }

    /// Close admission and await retirement of all accepted work and tasks, including abandoned results. Pending timers fail and Unix signal delivery stops.
    ///
    /// Coordinate application cancellation and asynchronous cleanup before calling this. A spawned task or blocking job must not await shutdown of its own scheduler, which would wait for that same task or job. A syscall or closure that never returns can prevent shutdown from completing.
    pub async fn shutdown(&self) {
        self.close();
        self.work.drain().await;
        self.cleanup.drain().await;
        self.tasks.drain().await;
        self.timers.drain().await;
        #[cfg(unix)]
        self.signals.drain().await;
    }

    async fn buffer_operation<B: IoBuf>(
        &self,
        file: &NativeFile,
        buffer: B,
        cancellation: &Cancellation,
        operation: impl FnOnce(&std::fs::File, &mut B) -> io::Result<usize> + Send + 'static,
    ) -> BufferResult<B> {
        let (result, (_, buffer)) = self
            .work
            .execute(
                (file.retain(), buffer),
                cancellation,
                move |(file, buffer)| operation(file, buffer),
            )
            .await;
        (result, buffer)
    }

    async fn submit_close(&self, file: NativeFile) -> io::Result<CloseCompletion> {
        let file = file.into_file();
        self.cleanup
            .submit(move || file_io::close(file))
            .await
            .map_err(|error| io::Error::from(admission_error(error)))
    }
}

impl Drop for NativeScheduler {
    fn drop(&mut self) {
        self.close();
    }
}

impl Spawn for NativeScheduler {
    type Handle<Output: Send + 'static> = crate::NativeTask<Output>;
    fn spawn<F>(&self, future: F) -> Result<Self::Handle<F::Output>, SpawnError>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tasks.spawn(future)
    }
}

impl Blocking for NativeScheduler {
    type Handle<Output>
        = BlockingTask<Output>
    where
        Output: Send + 'static;

    async fn spawn_blocking<F, Output>(
        &self,
        operation: F,
        cancellation: &Cancellation,
    ) -> Result<Self::Handle<Output>, BlockingError>
    where
        F: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        self.work.spawn(operation, cancellation).await
    }
}

impl FileOperation for NativeScheduler {
    type File = NativeFile;
}

impl FileOpen for NativeScheduler {
    async fn file_open(
        &self,
        path: &Path,
        options: FileOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<NativeFile> {
        let path = path.to_owned();
        self.work
            .execute((), cancellation, move |_| {
                std::fs::OpenOptions::new()
                    .read(options.read)
                    .write(options.write)
                    .create(options.create)
                    .create_new(options.create_new)
                    .truncate(options.truncate)
                    .open(path)
                    .map(NativeFile::from_std)
            })
            .await
            .0
    }
}

impl FileRead for NativeScheduler {
    async fn file_read<B: IoBufMut>(
        &self,
        file: &NativeFile,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.buffer_operation(file, buffer, cancellation, |file, buffer| {
            file_io::read(file, buffer.as_mut())
        })
        .await
    }
}

impl FileWrite for NativeScheduler {
    async fn file_write<B: IoBuf>(
        &self,
        file: &NativeFile,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.buffer_operation(file, buffer, cancellation, |file, buffer| {
            file_io::write(file, buffer.as_ref())
        })
        .await
    }
}

impl FileReadAt for NativeScheduler {
    async fn file_read_at<B: IoBufMut>(
        &self,
        file: &NativeFile,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.buffer_operation(file, buffer, cancellation, move |file, buffer| {
            file_io::read_at(file, buffer.as_mut(), offset)
        })
        .await
    }
}

impl FileWriteAt for NativeScheduler {
    async fn file_write_at<B: IoBuf>(
        &self,
        file: &NativeFile,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.buffer_operation(file, buffer, cancellation, move |file, buffer| {
            file_io::write_at(file, buffer.as_ref(), offset)
        })
        .await
    }
}

impl FileSeek for NativeScheduler {
    async fn file_seek(
        &self,
        file: &NativeFile,
        position: SeekFrom,
        cancellation: &Cancellation,
    ) -> OperationResult<u64> {
        self.work
            .execute(file.retain(), cancellation, move |file| {
                file_io::seek(file, position)
            })
            .await
            .0
    }
}

impl FileTruncate for NativeScheduler {
    async fn file_truncate(
        &self,
        file: &NativeFile,
        length: u64,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.work
            .execute(file.retain(), cancellation, move |file| {
                file_io::truncate(file, length)
            })
            .await
            .0
    }
}

impl FileSync for NativeScheduler {
    async fn file_sync(
        &self,
        file: &NativeFile,
        mode: SyncMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.work
            .execute(file.retain(), cancellation, move |file| {
                file_io::sync(file, mode)
            })
            .await
            .0
    }
}

impl FileStat for NativeScheduler {
    type Metadata = SystemFileMetadata;

    async fn file_stat(
        &self,
        file: &NativeFile,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Metadata> {
        self.work
            .execute(file.retain(), cancellation, |file| file_io::stat(file))
            .await
            .0
    }
}

impl FileClose for NativeScheduler {
    async fn file_close(&self, file: NativeFile) -> io::Result<()> {
        wait_close(self.submit_close(file).await?).await
    }

    async fn file_close_submit(&self, file: NativeFile) -> io::Result<()> {
        self.submit_close(file).await.map(drop)
    }
}

type CloseCompletion = oneshot::Receiver<Result<io::Result<()>, TaskError>>;

pub(crate) async fn wait_close(completion: CloseCompletion) -> io::Result<()> {
    completion
        .await
        .map_err(io::Error::other)?
        .map_err(|error| io::Error::other(error.to_string()))?
}

#[cfg(test)]
mod tests;

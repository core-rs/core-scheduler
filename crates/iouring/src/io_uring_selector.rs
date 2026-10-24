// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{IoUringDriver, IoUringOptions};
use core_scheduler::*;
use core_scheduler_selector::{NativeFile, NativeSocket, ReadinessSelector, Selector};
use event_listener::Event as CompletionEvent;
use futures_channel::oneshot;
use io_uring::{IoUring, opcode, squeue, types};
use std::{
    cell::Cell,
    collections::{HashMap, VecDeque},
    fs::File as StdFile,
    future::{Future, poll_fn},
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    pin::pin,
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    task::Poll,
    time::Duration,
};
const CANCEL_TAG: u64 = 1 << 63;
const NOTIFICATION_TAG: u64 = u64::MAX;

// Keep syscall failures deterministic in unit tests while production calls the OS directly.
fn syscall<T>(name: &'static str, operation: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    #[cfg(test)]
    {
        tests::syscall(name, operation)
    }
    #[cfg(not(test))]
    {
        let _ = name;
        operation()
    }
}

fn validate_completion_support(nodrop: bool, rw_cur_pos: bool, ext_arg: bool) -> io::Result<()> {
    if !ext_arg {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring requires extended wait arguments for timed driver turns",
        ));
    }
    if !rw_cur_pos {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring requires shared file cursor support",
        ));
    }
    if nodrop {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring requires reliable completion overflow handling",
        ))
    }
}

fn validate_operation_support(supported: impl Fn(u8) -> bool) -> io::Result<()> {
    for operation in [
        opcode::Read::CODE,
        opcode::Write::CODE,
        opcode::Recv::CODE,
        opcode::Send::CODE,
        opcode::RecvMsg::CODE,
        opcode::SendMsg::CODE,
        opcode::PollAdd::CODE,
        opcode::AsyncCancel::CODE,
    ] {
        if !supported(operation) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "required io_uring operation is unavailable",
            ));
        }
    }
    Ok(())
}

fn discard<T>(value: T) {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value))) {
        // A destructor panic has no receiving caller; its payload can itself panic on drop.
        std::mem::forget(payload);
    }
}

type RawBufferResult<B> = (io::Result<usize>, B);

type RegisteredSocket = Arc<async_io::Async<socket2::Socket>>;

enum Resource {
    Socket(RegisteredSocket),
    // None uses the shared OS cursor; Some is a validated explicit offset.
    File(Arc<StdFile>, Option<u64>),
}

struct Request<B: IoBuf = Vec<u8>> {
    identifier: u64,
    _permit: Permit,
    resource: Option<Resource>,
    buffer: Option<B>,
    pointer: fn(&mut B) -> *mut u8,
    write: bool,
    reply: Cell<Option<oneshot::Sender<RawBufferResult<B>>>>,
    cancelling: Cell<bool>,
}

impl<B: IoBuf> Request<B> {
    fn entry(&mut self) -> squeue::Entry {
        let buffer = self.buffer.as_mut().expect("request retains its buffer");
        let length = buffer.as_ref().len().min(i32::MAX as usize) as u32;
        let pointer = (self.pointer)(buffer);
        let entry = match self
            .resource
            .as_ref()
            .expect("request retains its resource")
        {
            Resource::Socket(socket) => {
                let descriptor = types::Fd(socket.get_ref().as_raw_fd());
                if self.write {
                    opcode::Send::new(descriptor, pointer, length)
                        .flags(libc::MSG_NOSIGNAL)
                        .build()
                } else {
                    opcode::Recv::new(descriptor, pointer, length).build()
                }
            }
            Resource::File(file, offset) => {
                let descriptor = types::Fd(file.as_raw_fd());
                if self.write {
                    opcode::Write::new(descriptor, pointer, length)
                        .offset(offset.unwrap_or(u64::MAX))
                        .build()
                } else {
                    opcode::Read::new(descriptor, pointer, length)
                        .offset(offset.unwrap_or(u64::MAX))
                        .build()
                }
            }
        };
        entry.user_data(self.identifier)
    }

    fn finish(mut self, result: io::Result<usize>) {
        let reply = self
            .reply
            .take()
            .expect("unfinished requests retain their completion sender");
        let buffer = self.buffer.take().expect("request returns its buffer once");
        // Release the retained descriptor before waking a caller that may immediately close it.
        drop(self);
        discard(reply.send((result, buffer)));
    }
}

impl<B: IoBuf> Drop for Request<B> {
    fn drop(&mut self) {
        drop(self.resource.take());
        // A submission can race Shutdown and remain in the incoming queue as the selector exits. Such requests have never reached the kernel and can return their buffers normally. InFlight deliberately suppresses this destructor for requests whose completion is unknown.
        if let Some(reply) = self.reply.take() {
            discard(reply.send((
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "selector closed before submission",
                )),
                self.buffer.take().expect("request returns its buffer once"),
            )));
        }
    }
}

// One erased request allocation lets a selector queue heterogeneous owned buffer types. Its typed completion returns the exact original owner. Only entry preparation (before publication) and original completion may access the request exclusively. In-flight bookkeeping uses shared access so inline UnsafeCell output can remain kernel-accessible without creating an aliasing mutable reference.
pub(crate) trait PendingRequest: Send {
    fn identifier(&self) -> u64;
    fn prepare(&mut self, _ring: &io_uring::Submitter<'_>) -> io::Result<()> {
        Ok(())
    }
    fn ready(&mut self, ring: &io_uring::Submitter<'_>) -> io::Result<bool> {
        self.prepare(ring).map(|()| true)
    }
    fn buffer_pool(&self) -> Option<&Arc<provided_buffers::WorkerBuffers>> {
        None
    }
    fn entry(&mut self) -> squeue::Entry;
    // Buffer accessors may run arbitrary user code, including waking tasks. Such requests must return to the executor after preparation instead of waiting.
    fn entry_may_wake(&self) -> bool {
        true
    }
    fn finish(self: Box<Self>, result: io::Result<usize>);
    fn completion(&self, _result: i32, _flags: u32) {}
    // None retires; Some(true) resubmits now; Some(false) waits for a buffer return.
    fn retry(&self, _result: i32) -> Option<bool> {
        None
    }
    fn retire(self: Box<Self>, _ring: &io_uring::Submitter<'_>, result: io::Result<usize>) {
        self.finish(result);
    }
    fn retire_completion(self: Box<Self>, ring: &io_uring::Submitter<'_>, result: i32) {
        self.retire(
            ring,
            if result < 0 {
                Err(io::Error::from_raw_os_error(-result))
            } else {
                Ok(result as usize)
            },
        );
    }
    // Return true only for the transition which first requests cancellation.
    fn begin_cancellation(&self) -> bool;
    fn take_reply(&self) -> Option<Box<dyn Send>>;
}
impl<B: IoBuf> PendingRequest for Request<B> {
    fn identifier(&self) -> u64 {
        self.identifier
    }
    fn entry(&mut self) -> squeue::Entry {
        Request::entry(self)
    }
    fn finish(self: Box<Self>, result: io::Result<usize>) {
        Request::finish(*self, result);
    }
    fn begin_cancellation(&self) -> bool {
        !self.cancelling.replace(true)
    }
    fn take_reply(&self) -> Option<Box<dyn Send>> {
        self.reply
            .take()
            .map(|reply| Box::new(reply) as Box<dyn Send>)
    }
}

/// Only terminal original completions remove requests from this map. On an unexpected selector failure, leak kernel-accessible resources rather than freeing memory whose completion cannot be established.
#[derive(Default)]
struct InFlight(HashMap<u64, Box<dyn PendingRequest>>);

impl Drop for InFlight {
    fn drop(&mut self) {
        let mut replies = Vec::new();
        for (_, request) in self.0.drain() {
            replies.extend(request.take_reply());
            std::mem::forget(request);
        }
        // Wake abandoned callers only after every in-flight buffer is retained.
        drop(replies);
    }
}

pub(crate) enum Command {
    Submit(Box<dyn PendingRequest>),
    MonitorSubmit(monitor::Submission),
    MonitorCancel(u64, u64),
    FileRelease(registered_file::Release),
    Recycle(Arc<multishot::pool::Pool>, u16),
    PoolReady(Arc<provided_buffers::WorkerBuffers>),
    Rearm(u64),
    Cancel(u64),
    CancelAfterSubmission(u64),
    Shutdown,
}

struct Shutdown {
    finished: AtomicBool,
    event: CompletionEvent,
}

pub(crate) struct SignalCompletion(Arc<Shutdown>);
impl Drop for SignalCompletion {
    fn drop(&mut self) {
        self.0.finished.store(true, Ordering::Release);
        self.0.event.notify(usize::MAX);
    }
}

pub(crate) struct Controller {
    pub(crate) worker: usize,
    capacity: Arc<Capacity>,
    commands: mpsc::Sender<Command>,
    notification: Arc<OwnedFd>,
    next_identifier: AtomicU64,
    buffer_groups: Arc<std::sync::Mutex<Vec<bool>>>,
    files: std::sync::Mutex<registered_file::Files>,
    closed: AtomicBool,
    shutdown: Arc<Shutdown>,
}

impl Controller {
    pub(crate) fn send(&self, command: Command) -> Result<(), mpsc::SendError<Command>> {
        if matches!(
            &command,
            Command::Submit(_) | Command::MonitorSubmit(_) | Command::PoolReady(_)
        ) && self.closed.load(Ordering::Acquire)
        {
            return Err(mpsc::SendError(command));
        }
        let command = match crate::io_uring_worker::enqueue(self, command) {
            Ok(()) => return Ok(()),
            Err(command) => command,
        };
        self.send_remote(command)
    }
    pub(crate) fn send_remote(&self, command: Command) -> Result<(), mpsc::SendError<Command>> {
        let result = self.commands.send(command);
        self.notify();
        result
    }

    fn notify(&self) {
        // A full eventfd already contains a wakeup. Other errors mean the selector has exited; dropping its receivers wakes pending operation futures.
        loop {
            match syscall("notify", || {
                let value = 1u64;
                // SAFETY: The retained eventfd accepts one native-endian u64; this synchronous call cannot retain the pointer.
                let result = unsafe {
                    libc::write(
                        self.notification.as_raw_fd(),
                        (&value as *const u64).cast(),
                        8,
                    )
                };
                if result < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(result as usize)
                }
            }) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                _ => break,
            }
        }
    }

    fn submit(&self, request: Box<dyn PendingRequest>) {
        if let Err(mpsc::SendError(Command::Submit(request))) = self.send(Command::Submit(request))
        {
            request.finish(Err(io::ErrorKind::BrokenPipe.into()));
        }
    }

    // `try_update` is not available on the supported Rust 1.85 toolchain.
    #[allow(deprecated)]
    fn next_identifier(&self) -> io::Result<u64> {
        self.next_identifier
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |identifier| {
                (identifier < CANCEL_TAG - 2).then_some(identifier + 2)
            })
            .map_err(|_| io::Error::other("io_uring operation identifiers exhausted"))
    }

    pub(crate) fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.capacity.close();
            let _ = self.send(Command::Shutdown);
        }
    }
}

impl Drop for Controller {
    fn drop(&mut self) {
        self.close();
    }
}

struct CancelOnDrop {
    controller: Arc<Controller>,
    identifier: u64,
    armed: bool,
}
impl CancelOnDrop {
    fn request(&mut self) {
        if self.armed {
            let _ = self.controller.send(Command::Cancel(self.identifier));
            self.armed = false;
        }
    }
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.request();
    }
}

const CAPACITY_CLOSED: usize = 1 << (usize::BITS - 1);

struct Capacity {
    state: AtomicUsize,
    available: CompletionEvent,
    limit: usize,
}
struct Permit(Arc<Capacity>);
impl Drop for Permit {
    fn drop(&mut self) {
        if self.0.state.fetch_sub(1, Ordering::AcqRel) == self.0.limit {
            // Only the full-to-available transition can release capacity waiters. Closure already wakes every waiter independently.
            self.0.available.notify(usize::MAX);
        }
    }
}
impl Capacity {
    pub(crate) fn close(&self) {
        self.state.fetch_or(CAPACITY_CLOSED, Ordering::AcqRel);
        self.available.notify(usize::MAX);
    }
    async fn acquire(self: &Arc<Self>, cancellation: &Cancellation) -> io::Result<Permit> {
        loop {
            cancellation
                .check()
                .map_err(|error| io::Error::new(io::ErrorKind::Interrupted, error))?;
            {
                let state = self.state.load(Ordering::Acquire);
                if state & CAPACITY_CLOSED != 0 {
                    return Err(io::ErrorKind::BrokenPipe.into());
                }
                if state < self.limit {
                    if self
                        .state
                        .compare_exchange_weak(
                            state,
                            state + 1,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return Ok(Permit(Arc::clone(self)));
                    }
                    continue;
                }
            }
            self.wait_for_capacity(cancellation).await;
        }
    }

    async fn wait_for_capacity(&self, cancellation: &Cancellation) {
        event_listener::listener!(self.available => listener);
        // A permit can be returned or admission closed between the full check and registration. Recheck after listening so that neither transition loses its wakeup.
        let state = self.state.load(Ordering::Acquire);
        if state < self.limit || state & CAPACITY_CLOSED != 0 {
            return;
        }
        let mut cancelled = pin!(cancellation.cancelled());
        let mut listener = pin!(listener);
        poll_fn(|cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(());
            }
            listener.as_mut().poll(cx)
        })
        .await;
    }
}
pub(crate) struct DriverGroup {
    buffer_groups: Arc<std::sync::Mutex<Vec<bool>>>,
    controllers: OnceLock<Vec<Weak<Controller>>>,
    fallback: Arc<ReadinessSelector>,
    closed: AtomicBool,
}
impl DriverGroup {
    pub(crate) fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.fallback.close();
            if let Some(controllers) = self.controllers.get() {
                for controller in controllers.iter().filter_map(Weak::upgrade) {
                    controller.close();
                }
            }
        }
    }
}

/// Owned I/O admission endpoint, independent of the executor polling its futures.
///
/// Construction returns separate drivers and starts no ring thread. Owned file and socket transfers use io_uring; borrowed sockets try nonblocking syscalls and wait for ring readiness. Other capabilities share one readiness fallback. Dropping this endpoint closes admission without blocking.
pub struct IoUringSelector {
    controllers: Vec<Arc<Controller>>,
    group: Arc<DriverGroup>,
    external_next: AtomicUsize,
    pub(crate) fallback: Arc<ReadinessSelector>,
}
impl IoUringSelector {
    /// Create an endpoint and its driver. The driver must be run throughout operation and shutdown. Unsupported kernels or restricted ring setup return errors; ring transfers never silently fall back.
    pub fn new(options: IoUringOptions) -> io::Result<(Self, IoUringDriver)> {
        let (selector, mut drivers) = Self::with_workers(options, 1)?;
        Ok((selector, drivers.pop().expect("one driver was requested")))
    }

    /// Create one ring per worker and one shared readiness fallback. Entries and request capacity apply separately to each ring. Local operations select the entered worker belonging to this selector; external callers use round-robin routing. An operation keeps its chosen ring through admission, completion, and cancellation, even if its task migrates.
    ///
    /// Move the returned drivers into `NativeScheduler::with_drivers`, or enter and drive each from its own event loop. Stopping or failing any driver closes admission across the group; all other drivers must continue retiring their accepted work.
    pub fn with_workers(
        options: IoUringOptions,
        workers: usize,
    ) -> io::Result<(Self, Vec<IoUringDriver>)> {
        if options.entries == 0
            || options.capacity == 0
            || options.capacity >= CAPACITY_CLOSED
            || workers == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "nonzero ring entries, workers, and capacity below isize::MAX + 1 are required",
            ));
        }
        let fallback = Arc::new(ReadinessSelector::with_options(options.fallback)?);
        let group = Arc::new(DriverGroup {
            buffer_groups: Arc::new(std::sync::Mutex::new(Vec::new())),
            controllers: OnceLock::new(),
            fallback: Arc::clone(&fallback),
            closed: AtomicBool::new(false),
        });
        let mut controllers = Vec::with_capacity(workers);
        let mut drivers = Vec::with_capacity(workers);
        for worker in 0..workers {
            let driver = Self::driver(options, Arc::clone(&group), worker)?;
            controllers.push(Arc::clone(&driver.controller));
            drivers.push(driver);
        }
        let _ = group
            .controllers
            .set(controllers.iter().map(Arc::downgrade).collect());
        Ok((
            Self {
                controllers,
                group,
                external_next: AtomicUsize::new(0),
                fallback,
            },
            drivers,
        ))
    }

    fn driver(
        options: IoUringOptions,
        group: Arc<DriverGroup>,
        worker: usize,
    ) -> io::Result<IoUringDriver> {
        let ring = syscall("ring", || IoUring::new(options.entries))?;
        syscall("completion-support", || {
            validate_completion_support(
                ring.params().is_feature_nodrop(),
                ring.params().is_feature_rw_cur_pos(),
                ring.params().is_feature_ext_arg(),
            )
        })?;
        let mut probe = io_uring::Probe::new();
        syscall("probe", || ring.submitter().register_probe(&mut probe))?;
        syscall("operation-support", || {
            validate_operation_support(|operation| probe.is_supported(operation))
        })?;
        let notification = Arc::new(syscall("notification-pair", || {
            // SAFETY: eventfd creates a new descriptor without accessing userspace pointers.
            let descriptor = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
            if descriptor < 0 {
                Err(io::Error::last_os_error())
            } else {
                // SAFETY: A successful eventfd call returns a new, exclusively owned descriptor.
                Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
            }
        })?);
        let receiver = Arc::clone(&notification);
        let (commands, incoming) = mpsc::channel();
        let shutdown = Arc::new(Shutdown {
            finished: AtomicBool::new(false),
            event: CompletionEvent::new(),
        });
        let capacity = Arc::new(Capacity {
            state: AtomicUsize::new(0),
            available: CompletionEvent::new(),
            limit: options.capacity,
        });
        let controller = Arc::new(Controller {
            worker,
            capacity,
            commands,
            notification,
            next_identifier: AtomicU64::new(0),
            buffer_groups: Arc::clone(&group.buffer_groups),
            files: std::sync::Mutex::new(registered_file::Files::new(options.fixed_files)),
            closed: AtomicBool::new(false),
            shutdown: Arc::clone(&shutdown),
        });
        let reactor = Reactor::new(ring, receiver, incoming, Arc::clone(&controller));
        Ok(IoUringDriver {
            reactor: Some(reactor),
            controller,
            group,
            completion: Some(SignalCompletion(shutdown)),
        })
    }

    /// Stop admission and request cancellation of accepted ring work. This does not wait for retirement.
    pub fn close(&self) {
        self.group.close();
    }

    /// Close admission and await fallback and ring retirement. Continue driving the separately owned driver while awaiting this method.
    pub async fn shutdown(&self) {
        self.close();
        self.fallback.shutdown().await;
        for controller in &self.controllers {
            loop {
                event_listener::listener!(controller.shutdown.event => listener);
                if controller.shutdown.finished.load(Ordering::Acquire) {
                    break;
                }
                listener.await;
            }
        }
    }
    fn check_open(&self) -> io::Result<()> {
        if self.group.closed.load(Ordering::Acquire) {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "io_uring selector is closed",
            ))
        } else {
            Ok(())
        }
    }

    fn route(&self) -> Arc<Controller> {
        crate::io_uring_worker::route(&self.group).unwrap_or_else(|| {
            let index = self.external_next.fetch_add(1, Ordering::Relaxed) % self.controllers.len();
            Arc::clone(&self.controllers[index])
        })
    }

    async fn operation<B: IoBuf>(
        &self,
        controller: Arc<Controller>,
        resource: Resource,
        buffer: B,
        write: bool,
        pointer: fn(&mut B) -> *mut u8,
        cancellation: &Cancellation,
    ) -> RawBufferResult<B> {
        match self
            .operation_submit(controller, resource, buffer, write, pointer, cancellation)
            .await
        {
            Ok(completion) => completion.await,
            Err((error, buffer)) => (Err(error), buffer),
        }
    }

    async fn operation_submit<B: IoBuf>(
        &self,
        controller: Arc<Controller>,
        resource: Resource,
        buffer: B,
        write: bool,
        pointer: fn(&mut B) -> *mut u8,
        cancellation: &Cancellation,
    ) -> Result<impl Future<Output = RawBufferResult<B>> + Send, (io::Error, B)> {
        if cancellation.is_cancelled() {
            return Err((
                io::Error::new(io::ErrorKind::Interrupted, core_scheduler::Cancelled),
                buffer,
            ));
        }
        if let Err(error) = self.check_open() {
            return Err((error, buffer));
        }
        if matches!(&resource, Resource::File(_, Some(offset)) if *offset > i64::MAX as u64) {
            return Err((
                io::Error::new(io::ErrorKind::InvalidInput, "file offset exceeds i64::MAX"),
                buffer,
            ));
        }
        let permit = match controller.capacity.acquire(cancellation).await {
            Ok(permit) => permit,
            Err(error) => return Err((error, buffer)),
        };
        let identifier = match controller.next_identifier() {
            Ok(identifier) => identifier,
            Err(error) => return Err((error, buffer)),
        };
        let (reply, result) = oneshot::channel();
        let request = Request {
            identifier,
            _permit: permit,
            resource: Some(resource),
            buffer: Some(buffer),
            pointer,
            write,
            reply: Cell::new(Some(reply)),
            cancelling: Cell::new(false),
        };
        Ok(Self::submit(
            controller,
            Box::new(request),
            result,
            cancellation,
        ))
    }

    fn submit<T: Send>(
        controller: Arc<Controller>,
        request: Box<dyn PendingRequest>,
        result: oneshot::Receiver<T>,
        cancellation: &Cancellation,
    ) -> impl Future<Output = T> + Send {
        let identifier = request.identifier();
        if let Err(mpsc::SendError(Command::Submit(request))) =
            controller.send(Command::Submit(request))
        {
            request.finish(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "io_uring selector has exited",
            )));
        }

        let mut cancel_request = CancelOnDrop {
            controller,
            identifier,
            armed: true,
        };
        async move {
            // A disconnected reply is an unexpected selector failure. Resources whose kernel completion cannot be established are deliberately retained.
            let mut result = pin!(result);
            let mut cancelled = pin!(cancellation.cancelled());
            let result = poll_fn(|context| {
                // Original completion wins the race. Cancellation never releases resources; continue waiting for the original CQE.
                if let Poll::Ready(result) = result.as_mut().poll(context) {
                    return Poll::Ready(result);
                }
                if cancel_request.armed && cancelled.as_mut().poll(context).is_ready() {
                    cancel_request.request();
                }
                Poll::Pending
            })
            .await
            .expect("io_uring selector failed with an operation in flight");
            cancel_request.armed = false;
            drop(cancel_request);
            result
        }
    }
}

fn operation_result<T>(result: io::Result<T>, cancellation: &Cancellation) -> OperationResult<T> {
    match result {
        Err(error)
            if cancellation.is_cancelled()
                && (error.kind() == io::ErrorKind::Interrupted
                    || error.raw_os_error() == Some(libc::ECANCELED)) =>
        {
            Err(core_scheduler::Cancelled.into())
        }
        result => result.map_err(Into::into),
    }
}
impl FileRead for IoUringSelector {
    async fn file_read<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = self
            .operation(
                self.route(),
                Resource::File(retain_file(file), None),
                buffer,
                false,
                |buffer| buffer.as_mut().as_mut_ptr(),
                cancellation,
            )
            .await;
        (operation_result(result, cancellation), buffer)
    }
}

impl FileWrite for IoUringSelector {
    async fn file_write<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = self
            .operation(
                self.route(),
                Resource::File(retain_file(file), None),
                buffer,
                true,
                |buffer| buffer.as_ref().as_ptr().cast_mut(),
                cancellation,
            )
            .await;
        (operation_result(result, cancellation), buffer)
    }
}

impl FileReadAt for IoUringSelector {
    async fn file_read_at<B: IoBufMut>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = self
            .operation(
                self.route(),
                Resource::File(retain_file(file), Some(offset)),
                buffer,
                false,
                |buffer| buffer.as_mut().as_mut_ptr(),
                cancellation,
            )
            .await;
        (operation_result(result, cancellation), buffer)
    }
}

impl FileWriteAt for IoUringSelector {
    async fn file_write_at<B: IoBuf>(
        &self,
        file: &Self::File,
        buffer: B,
        offset: u64,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let (result, buffer) = self
            .operation(
                self.route(),
                Resource::File(retain_file(file), Some(offset)),
                buffer,
                true,
                |buffer| buffer.as_ref().as_ptr().cast_mut(),
                cancellation,
            )
            .await;
        (operation_result(result, cancellation), buffer)
    }
}

fn retain_file(file: &NativeFile) -> Arc<StdFile> {
    // SAFETY: Requests retain this owner until original completion, never change descriptor flags, and release it before waking completion consumers. No native handle escapes this backend.
    unsafe { file.retain_native() }
}
impl Drop for IoUringSelector {
    fn drop(&mut self) {
        self.close();
    }
}
impl Selector for IoUringSelector {
    fn close(&self) {
        self.close();
    }
    async fn shutdown(&self) {
        self.shutdown().await;
    }
}
pub(crate) struct Reactor {
    ring: IoUring,
    started: bool,
    notification: Arc<OwnedFd>,
    incoming: mpsc::Receiver<Command>,
    controller: Arc<Controller>,
    pending: VecDeque<Box<dyn PendingRequest>>,
    dormant: HashMap<u64, Box<dyn PendingRequest>>,
    monitors: monitor::Monitors,
    pending_monitors: VecDeque<monitor::Submission>,
    buffer_pools: Vec<Option<Arc<provided_buffers::WorkerBuffers>>>,
    pool_ready: Vec<u64>,
    in_flight: InFlight,
    cancellations: VecDeque<u64>,
    closing: bool,
    notification_armed: bool,
}

impl Reactor {
    fn new(
        ring: IoUring,
        notification: Arc<OwnedFd>,
        incoming: mpsc::Receiver<Command>,
        controller: Arc<Controller>,
    ) -> Self {
        let monitors = monitor::Monitors::new(controller.capacity.limit);
        let pending_monitors = VecDeque::with_capacity(controller.capacity.limit);
        Self {
            ring,
            monitors,
            pending_monitors,
            buffer_pools: Vec::new(),
            pool_ready: Vec::new(),
            started: false,
            notification,
            incoming,
            controller,
            pending: VecDeque::new(),
            dormant: HashMap::new(),
            in_flight: InFlight::default(),
            cancellations: VecDeque::new(),
            closing: false,
            notification_armed: false,
        }
    }

    pub(crate) fn bind_worker(&mut self) {
        // Deferred and cooperative task work belong to their submitting thread. Enable them only for a never-driven ring after its owner becomes thread-bound; a movable driver must keep the default interrupt behavior.
        if !self.started {
            let entries = self.ring.params().sq_entries();
            let ring = syscall("deferred-ring", || {
                IoUring::builder()
                    .setup_single_issuer()
                    .setup_defer_taskrun()
                    .setup_taskrun_flag()
                    .build(entries)
            })
            .or_else(|_| {
                syscall("cooperative-ring", || {
                    IoUring::builder()
                        .setup_coop_taskrun()
                        .setup_taskrun_flag()
                        .build(entries)
                })
            });
            if let Ok(ring) = ring {
                // Setup already validated this kernel's operations and completion features. No request or notification has reached the old ring, and queued commands remain on the controller. Failure to obtain this optional replacement leaves the original valid ring intact.
                self.ring = ring;
            }
        }
    }

    fn drain_notification(&self) -> io::Result<()> {
        let mut value = 0u64;
        // One successful eventfd read clears the accumulated counter. Bound interrupted retries; a later write or an unread counter remains visible to the next poll.
        for _ in 0..16 {
            match syscall("notification-read", || {
                // SAFETY: The retained eventfd writes exactly one native-endian u64 into this destination; this synchronous call cannot retain the pointer.
                let result = unsafe {
                    libc::read(
                        self.notification.as_raw_fd(),
                        (&mut value as *mut u64).cast(),
                        8,
                    )
                };
                if result < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(result as usize)
                }
            }) {
                Ok(0) => unreachable!(
                    "an eventfd read with a valid eight-byte destination never returns EOF"
                ),
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn collect_completions(&mut self) -> io::Result<bool> {
        let mut completed = false;
        // The CQ iterator snapshots the available batch. Finish publishing every completion in that batch before returning to the executor; application futures are polled only from its ready queue.
        let mut notification_result = None;
        let (submitter, _, mut completions) = self.ring.split();
        for completion in &mut completions {
            completed = true;
            let identifier = completion.user_data();
            if identifier == NOTIFICATION_TAG {
                self.notification_armed = false;
                notification_result = Some(completion.result());
                continue;
            }
            if identifier & CANCEL_TAG != 0 {
                if identifier & monitor::MONITOR_TAG != 0 {
                    self.monitors.complete(
                        identifier & !CANCEL_TAG,
                        completion.result(),
                        false,
                        true,
                    );
                }
                continue;
            }
            if identifier & monitor::MONITOR_TAG != 0 {
                self.monitors.complete(
                    identifier,
                    completion.result(),
                    io_uring::cqueue::more(completion.flags()),
                    false,
                );
                continue;
            }
            let result = completion.result();
            let flags = completion.flags();
            if io_uring::cqueue::more(flags) {
                self.in_flight
                    .0
                    .get(&identifier)
                    .expect("original completion retains its in-flight request")
                    .completion(result, flags);
                continue;
            }
            // Only a final original CQE ends kernel access. A cancellation CQE cannot release buffers or a multishot request.
            let request = self
                .in_flight
                .0
                .remove(&identifier)
                .expect("original completion retains its in-flight request");
            request.completion(result, flags);
            if let Some(ready) = request.retry(result).filter(|_| !self.closing) {
                if ready {
                    self.pending.push_back(request);
                } else {
                    self.dormant.insert(identifier, request);
                }
            } else {
                request.retire_completion(&submitter, result);
            }
        }

        drop(completions);
        if let Some(result) = notification_result {
            if result < 0 {
                return Err(io::Error::from_raw_os_error(-result));
            }
            // Drain before rearming; otherwise the readable counter immediately completes each replacement poll.
            self.drain_notification()?;
        }

        Ok(completed)
    }

    fn cancel(&mut self, identifier: u64) -> bool {
        if let Some(position) = self
            .pending
            .iter()
            .position(|request| request.identifier() == identifier)
        {
            let request = self
                .pending
                .remove(position)
                .expect("pending queue is unchanged since finding this position");
            request.retire(
                &self.ring.submitter(),
                Err(io::ErrorKind::Interrupted.into()),
            );
            true
        } else if let Some(request) = self.dormant.remove(&identifier) {
            request.retire(
                &self.ring.submitter(),
                Err(io::ErrorKind::Interrupted.into()),
            );
            true
        } else if let Some(request) = self.in_flight.0.get(&identifier) {
            if request.begin_cancellation() {
                self.cancellations.push_back(identifier);
            }
            true
        } else {
            false
        }
    }

    fn retire_pending(&mut self, kind: io::ErrorKind) {
        for submission in self.pending_monitors.drain(..) {
            submission.finish(Err(kind.into()));
        }
        for request in self.pending.drain(..) {
            request.retire(&self.ring.submitter(), Err(kind.into()));
        }
        for (_, request) in self.dormant.drain() {
            request.retire(&self.ring.submitter(), Err(kind.into()));
        }
    }

    fn close(&mut self) {
        self.closing = true;
        self.monitors.close(&mut self.cancellations);
        self.retire_pending(io::ErrorKind::Interrupted);
        for request in self.in_flight.0.values() {
            if request.begin_cancellation() {
                self.cancellations.push_back(request.identifier());
            }
        }
    }

    // Returns whether command handling could have woken tasks and whether it reached the batch limit.
    fn process_commands(&mut self) -> (bool, bool) {
        let mut progressed = false;
        // Bound command processing so a busy producer cannot starve completions.
        let mut command_batch_full = true;
        for index in 0..256 {
            // Alternate local and remote preference so neither producer can starve the other.
            let local =
                || crate::io_uring_worker::pop(&self.controller).map(|command| (command, true));
            let remote = || {
                self.incoming
                    .try_recv()
                    .ok()
                    .map(|command| (command, false))
            };
            let command = if index % 2 == 0 {
                local().or_else(remote)
            } else {
                remote().or_else(local)
            };
            let Some((command, local)) = command else {
                command_batch_full = false;
                break;
            };
            // Accepted submissions alone do not wake tasks. Entry preparation below retains the yield for requests which invoke user callbacks.
            progressed |= !matches!(&command, Command::Submit(_) | Command::MonitorSubmit(_) if !self.closing);
            match command {
                Command::MonitorSubmit(submission) if !self.closing => {
                    self.pending_monitors.push_back(submission)
                }
                Command::MonitorSubmit(submission) => {
                    submission.finish(Err(io::ErrorKind::Interrupted.into()))
                }
                Command::MonitorCancel(token, generation) => {
                    if let Some(token) = self.monitors.cancel(token, generation) {
                        self.cancellations.push_back(token);
                    }
                }
                Command::Submit(request) if !self.closing => self.pending.push_back(request),
                Command::Submit(request) => request.finish(Err(io::ErrorKind::Interrupted.into())),
                Command::FileRelease(release) => {
                    release.finish(&self.ring.submitter(), &self.controller);
                }
                Command::Recycle(pool, buffer) => {
                    pool.recycle(buffer);
                    if let Some(request) = self.dormant.remove(&pool.identifier) {
                        self.pending.push_back(request);
                    }
                }
                Command::PoolReady(pool) => {
                    let index = pool.group() as usize;
                    if self.buffer_pools.len() <= index {
                        self.buffer_pools.resize_with(index + 1, || None);
                    }
                    assert!(
                        self.buffer_pools[index].is_none(),
                        "a live pool owns its group ID across all rings"
                    );
                    self.buffer_pools[index] = Some(pool);
                }
                Command::Rearm(identifier) => {
                    if let Some(request) = self.dormant.remove(&identifier) {
                        self.pending.push_back(request);
                    }
                }
                Command::Cancel(identifier) => {
                    if !self.cancel(identifier) {
                        // A migrated waiter can cancel through the other queue before its Submit is processed. Defer once to that opposite FIFO: the original Submit was already published before this Cancel existed. Late cancellations are discarded on the second pass rather than retained as tombstones.
                        let command = Command::CancelAfterSubmission(identifier);
                        if local {
                            let _ = self.controller.commands.send(command);
                        } else {
                            let _ = crate::io_uring_worker::enqueue(&self.controller, command);
                        }
                    }
                }
                Command::CancelAfterSubmission(identifier) => {
                    self.cancel(identifier);
                }
                Command::Shutdown => self.close(),
            }
        }

        (progressed, command_batch_full)
    }

    pub(crate) fn turn(&mut self, timeout: Option<Duration>) -> io::Result<bool> {
        self.started = true;
        let (mut progressed, command_batch_full) = self.process_commands();

        progressed |= self.collect_completions()?;

        // Initialize and refill on the ring owner. Each indexed entry owns only this worker's storage; completed payloads may be retained on any thread.
        for slot in &mut self.buffer_pools {
            let Some(pool) = slot else { continue };
            if pool.service(&self.ring.submitter())? && !self.dormant.is_empty() {
                for (identifier, request) in &self.dormant {
                    if request
                        .buffer_pool()
                        .is_some_and(|known| Arc::ptr_eq(known, pool))
                    {
                        self.pool_ready.push(*identifier);
                    }
                }
            }
            if pool.empty() {
                *slot = None;
            }
        }
        for identifier in self.pool_ready.drain(..) {
            self.pending.push_back(
                self.dormant
                    .remove(&identifier)
                    .expect("a ready pool retains its dormant request"),
            );
        }

        let defer_submission;
        {
            let (submitter, mut submissions, _) = self.ring.split();
            let had_submissions = !submissions.is_empty();
            let mut prepared = false;
            while let Some(&identifier) = self.cancellations.front() {
                let entry = opcode::AsyncCancel::new(identifier)
                    .build()
                    .user_data(identifier | CANCEL_TAG);
                // SAFETY: cancellation entries contain an opaque request token. Monitor owners remain retained through both original and cancellation CQEs.
                if unsafe { submissions.push(&entry) }.is_err() {
                    break;
                }
                self.cancellations.pop_front();
            }
            while !submissions.is_full() {
                let Some(mut request) = self.pending.pop_front() else {
                    break;
                };
                match request.ready(&submitter) {
                    Ok(true) => {}
                    Ok(false) => {
                        self.dormant.insert(request.identifier(), request);
                        continue;
                    }
                    Err(error) => {
                        request.retire(&submitter, Err(error));
                        progressed = true;
                        continue;
                    }
                }
                let identifier = request.identifier();
                let request = self.in_flight.0.entry(identifier).or_insert(request);
                let entry = request.entry();
                progressed |= request.entry_may_wake();
                prepared = true;
                // SAFETY: InFlight owns the buffer allocation and descriptor before publication. Neither is released or accessed until the original CQE, even when cancellation completes first. A failed push does not publish the entry.
                unsafe { submissions.push(&entry) }
                    .expect("submission capacity was checked and the local queue has not changed");
            }
            while !submissions.is_full() {
                let Some(submission) = self.pending_monitors.pop_front() else {
                    break;
                };
                if submission.cancelled() {
                    submission.finish(Err(io::ErrorKind::Interrupted.into()));
                    progressed = true;
                    continue;
                }
                let entry = self.monitors.insert(submission);
                prepared = true;
                // SAFETY: The monitor table retains the stable record and socket before publication. Both original and cancellation CQEs must retire before the record can be reused.
                unsafe { submissions.push(&entry) }
                    .expect("monitor submission capacity was checked");
            }
            // Payloads and cancellations take priority: a repeatedly signalled eventfd must not monopolize a one-entry SQ. If the notification cannot be armed, this turn submits the work and returns before parking.
            if !self.notification_armed && !submissions.is_full() {
                let entry = opcode::PollAdd::new(
                    types::Fd(self.notification.as_raw_fd()),
                    libc::POLLIN as u32,
                )
                .build()
                .user_data(NOTIFICATION_TAG);
                // SAFETY: The reactor and controller retain the eventfd, and PollAdd does not reference userspace memory.
                unsafe { submissions.push(&entry) }
                    .expect("submission capacity was checked and the local queue has not changed");
                self.notification_armed = true;
            }
            // A readiness-only batch can join the next idle wait. Existing SQ entries must be submitted on this turn, even if tasks stay busy.
            defer_submission = prepared
                && !progressed
                && !command_batch_full
                && !had_submissions
                && timeout == Some(Duration::ZERO);
        }

        if self.closing
            && !command_batch_full
            && self.in_flight.0.is_empty()
            && self.pending.is_empty()
            && self.pending_monitors.is_empty()
            && self.monitors.is_empty()
        {
            return Ok(true);
        }

        // Nonblocking turns may stage one readiness-only batch for the next turn. An idle wait submits it and waits in one entry; a busy worker submits it on its next nonblocking turn. Overflow and task work always enter. Short-circuiting preserves the local permit through productive turns. At the actual wait decision, consume it; the armed poll retains remote notifications until their CQE drains the counter.
        let return_to_executor = progressed
            || command_batch_full
            || timeout == Some(Duration::ZERO)
            || !self.pending.is_empty()
            || !self.pending_monitors.is_empty()
            || !self.cancellations.is_empty()
            || !self.notification_armed
            || crate::io_uring_worker::take_notification(&self.controller);
        if return_to_executor {
            let needs_submit = {
                let submissions = self.ring.submission();
                (!submissions.is_empty() && !defer_submission)
                    || submissions.cq_overflow()
                    || submissions.taskrun()
            };
            if needs_submit {
                let count = self.ring.submission().len() as u32;
                match syscall("submit", || {
                    // SAFETY: This exclusive ring owner has published every SQ entry with its retained resources. GETEVENTS runs deferred completion work without blocking when the minimum is zero; no signal-mask pointer is supplied. Request it explicitly because older supported io-uring versions omit it from submit() when only task work is pending.
                    unsafe {
                        self.ring.submitter().enter::<libc::sigset_t>(
                            count,
                            0,
                            io_uring::EnterFlags::GETEVENTS.bits(),
                            None,
                        )
                    }
                }) {
                    Err(error)
                        if error.kind() == io::ErrorKind::Interrupted
                            || matches!(error.raw_os_error(), Some(libc::EBUSY | libc::EAGAIN)) =>
                    {
                        return Ok(false);
                    }
                    result => {
                        result?;
                    }
                }
            }
            return Ok(false);
        }
        let result = syscall("wait", || match timeout {
            None => self.ring.submit_and_wait(1),
            Some(duration) => {
                // Clamp seconds to the signed kernel ABI rather than wrapping very long durations negative.
                let timespec = types::Timespec::new()
                    .sec(duration.as_secs().min(i64::MAX as u64))
                    .nsec(duration.subsec_nanos());
                self.ring
                    .submitter()
                    .submit_with_args(1, &types::SubmitArgs::new().timespec(&timespec))
            }
        });
        if let Err(error) = result {
            if error.kind() != io::ErrorKind::Interrupted
                && !matches!(
                    error.raw_os_error(),
                    Some(libc::ETIME | libc::EBUSY | libc::EAGAIN)
                )
            {
                return Err(error);
            }
        }
        // Publish the completion snapshot produced by this wait before returning. Its wakeups enter the executor's ready queue without another idle turn.
        self.collect_completions()?;
        Ok(false)
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        self.retire_pending(io::ErrorKind::BrokenPipe);
        for pool in self.buffer_pools.iter().flatten() {
            // Only proven-idle registrations can be released. Unknown in-flight payloads remain retained on driver failure.
            let _ = pool.shutdown(&self.ring.submitter());
        }
        // Pop without retaining a queue borrow: returning a buffer or waking its owner can reenter local routing.
        while let Some(command) = crate::io_uring_worker::pop(&self.controller) {
            discard(command);
        }
    }
}

impl std::task::Wake for Controller {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if !crate::io_uring_worker::notify(self) {
            self.notify();
        }
    }
}

mod buffer_pooling;
mod message;
pub(crate) mod monitor;
pub(crate) mod multishot;
mod readiness;
mod registered_file;
mod ring_binding;
mod socket;

#[cfg(test)]
mod tests;

pub(crate) mod file_monitor;

mod file_submission;

mod socket_receiving;

pub(crate) mod provided_buffers;

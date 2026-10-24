// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use futures_lite::future::{block_on, poll_once};
use std::{sync::atomic::AtomicUsize, time::Instant};

use std::cell::RefCell;
type SyscallHook = (&'static str, Box<dyn FnOnce()>);
thread_local! {
    pub(super) static BEFORE_SYSCALL: RefCell<Option<SyscallHook>> = const { RefCell::new(None) };
    static FAILURES: RefCell<VecDeque<(&'static str, Option<io::Error>)>> = const { RefCell::new(VecDeque::new()) };
}

pub(super) fn syscall<T>(
    name: &'static str,
    operation: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    let hook = BEFORE_SYSCALL
        .try_with(|hook| {
            let mut hook = hook.borrow_mut();
            if hook.as_ref().is_some_and(|(expected, _)| *expected == name) {
                hook.take().map(|(_, callback)| callback)
            } else {
                None
            }
        })
        .unwrap_or(None);
    if let Some(hook) = hook {
        hook();
    }
    let failure = FAILURES
        .try_with(|failures| {
            let mut failures = failures.borrow_mut();
            if failures
                .front()
                .is_some_and(|(expected, _)| *expected == name)
            {
                failures.pop_front().and_then(|(_, error)| error)
            } else {
                None
            }
        })
        .unwrap_or(None);
    match failure {
        Some(error) => Err(error),
        None => operation(),
    }
}

pub(super) struct Failures;
impl Failures {
    pub(super) fn new(failures: impl IntoIterator<Item = (&'static str, io::Error)>) -> Self {
        FAILURES.with(|state| {
            *state.borrow_mut() = failures
                .into_iter()
                .map(|(name, error)| (name, Some(error)))
                .collect()
        });
        Self
    }
}
impl Drop for Failures {
    fn drop(&mut self) {
        FAILURES.with(|failures| failures.borrow_mut().clear());
        BEFORE_SYSCALL.with(|hook| hook.borrow_mut().take());
    }
}

pub(super) fn pair(capacity: usize) -> (IoUringSelector, IoUringDriver) {
    IoUringSelector::new(IoUringOptions {
        capacity,
        ..Default::default()
    })
    .unwrap()
}
pub(super) fn file(selector: &IoUringSelector) -> (tempfile::TempDir, NativeFile) {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("file"), b"abcdef").unwrap();
    let file = block_on(selector.file_open(
        &directory.path().join("file"),
        FileOptions {
            write: true,
            ..FileOptions::READ
        },
        &Cancellation::never(),
    ))
    .unwrap();
    (directory, file)
}
fn fifo(selector: &IoUringSelector) -> (tempfile::TempDir, NativeFile) {
    use std::os::unix::ffi::OsStrExt;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fifo");
    let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: The path is a valid terminated string and the fixture owns its parent directory.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    // Opening both ends avoids blocking construction; a read waits for actual bytes.
    let file = block_on(selector.file_open(
        &path,
        FileOptions {
            write: true,
            ..FileOptions::READ
        },
        &Cancellation::never(),
    ))
    .unwrap();
    (directory, file)
}
pub(super) fn drain(selector: &IoUringSelector, driver: &mut IoUringDriver) {
    selector.close();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !driver.turn(Some(Duration::from_millis(10))).unwrap() {
        assert!(
            Instant::now() < deadline,
            "driver did not retire accepted requests"
        );
    }
    block_on(selector.shutdown());
}

#[test]
fn setup_probes_and_errors_are_explicit() {
    assert!(validate_completion_support(true, true, true).is_ok());
    assert_eq!(
        validate_completion_support(false, true, true)
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        validate_completion_support(true, false, true)
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        validate_completion_support(true, true, false)
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert!(validate_operation_support(|_| true).is_ok());
    for missing in [
        opcode::Read::CODE,
        opcode::Write::CODE,
        opcode::Recv::CODE,
        opcode::Send::CODE,
        opcode::RecvMsg::CODE,
        opcode::SendMsg::CODE,
        opcode::PollAdd::CODE,
        opcode::AsyncCancel::CODE,
    ] {
        assert_eq!(
            validate_operation_support(|code| code != missing)
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
    }
    for options in [
        IoUringOptions {
            entries: 0,
            ..Default::default()
        },
        IoUringOptions {
            capacity: 0,
            ..Default::default()
        },
        IoUringOptions {
            capacity: CAPACITY_CLOSED,
            ..Default::default()
        },
        IoUringOptions {
            fallback: core_scheduler_selector::SelectorOptions {
                threads: 0,
                capacity: 1,
            },
            ..Default::default()
        },
    ] {
        assert_eq!(
            IoUringSelector::new(options).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
    }
    assert_eq!(
        IoUringSelector::with_workers(IoUringOptions::default(), 0)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    for name in [
        "ring",
        "probe",
        "completion-support",
        "operation-support",
        "notification-pair",
    ] {
        let _failures = Failures::new([(name, io::ErrorKind::PermissionDenied.into())]);
        assert_eq!(
            IoUringSelector::new(IoUringOptions::default())
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::PermissionDenied,
            "{name}"
        );
    }
}

#[test]
fn lazy_precancelled_empty_invalid_and_closed_operations_preserve_the_buffer() {
    let (selector, mut driver) = pair(1);
    let (_directory, file) = file(&selector);
    let never = Cancellation::never();
    drop(selector.file_read(&file, vec![0; 1], &never));
    assert_eq!(
        selector.controllers[0]
            .capacity
            .state
            .load(Ordering::Acquire)
            & !CAPACITY_CLOSED,
        0
    );
    let cancelled = Cancellation::new();
    cancelled.cancel();
    let (result, buffer) = block_on(selector.file_read(&file, vec![7], &cancelled));
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    assert_eq!(buffer, [7]);
    let (result, buffer) = block_on(selector.file_write_at(&file, vec![9], u64::MAX, &never));
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(buffer, [9]);
    selector.controllers[0]
        .next_identifier
        .store(CANCEL_TAG - 1, Ordering::Relaxed);
    assert_eq!(
        block_on(selector.file_read(&file, vec![0], &never))
            .0
            .unwrap_err()
            .kind(),
        io::ErrorKind::Other
    );
    drain(&selector, &mut driver);
    assert_eq!(
        block_on(selector.file_read(&file, vec![0], &never))
            .0
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert!(matches!(
        block_on(selector.file_read(&file, vec![0], &cancelled)).0,
        Err(OperationError::Cancelled(_))
    ));
    assert!(driver.turn(None).unwrap());
}

#[test]
fn bounded_admission_wakes_for_cancellation_and_closure() {
    let (selector, mut driver) = pair(1);
    let (_directory, file) = fifo(&selector);
    let never = Cancellation::never();
    let mut first = Box::pin(selector.file_read(&file, vec![0], &never));
    assert!(block_on(poll_once(&mut first)).is_none());
    driver.turn(Some(Duration::ZERO)).unwrap();
    let token = Cancellation::new();
    let mut second = Box::pin(selector.file_read(&file, vec![5], &token));
    assert!(block_on(poll_once(&mut second)).is_none());
    token.cancel();
    let (result, buffer) = block_on(second);
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    assert_eq!(buffer, [5]);
    let mut third = Box::pin(selector.file_read(&file, vec![7], &never));
    assert!(block_on(poll_once(&mut third)).is_none());
    selector.close();
    assert_eq!(
        block_on(third).0.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    drain(&selector, &mut driver);
    assert!(block_on(first).0.is_err());
    assert_eq!(
        selector.controllers[0]
            .capacity
            .state
            .load(Ordering::Acquire)
            & !CAPACITY_CLOSED,
        0
    );
}

struct ObservedBuffer {
    bytes: Vec<u8>,
    drops: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for ObservedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for ObservedBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
// SAFETY: Exclusive ownership retains one stable initialized allocation across moves.
unsafe impl IoBuf for ObservedBuffer {}
// SAFETY: Writable access is exclusive and never changes the allocation or initialized range.
unsafe impl IoBufMut for ObservedBuffer {}
impl Drop for ObservedBuffer {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn abandoning_submitted_io_keeps_storage_until_original_retirement() {
    let (selector, mut driver) = pair(1);
    let (_directory, file) = fifo(&selector);
    let drops = Arc::new(AtomicUsize::new(0));
    let buffer = ObservedBuffer {
        bytes: vec![0],
        drops: Arc::clone(&drops),
    };
    let never = Cancellation::never();
    let mut operation = Box::pin(selector.file_read(&file, buffer, &never));
    assert!(block_on(poll_once(&mut operation)).is_none());
    driver.turn(Some(Duration::ZERO)).unwrap();
    assert_eq!(driver.reactor.as_ref().unwrap().in_flight.0.len(), 1);
    drop(operation);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(
        selector.controllers[0]
            .capacity
            .state
            .load(Ordering::Acquire)
            & !CAPACITY_CLOSED,
        1
    );
    drain(&selector, &mut driver);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        selector.controllers[0]
            .capacity
            .state
            .load(Ordering::Acquire)
            & !CAPACITY_CLOSED,
        0
    );
}

#[test]
fn cooperative_cancellation_waits_for_original_completion_and_returns_the_owner() {
    let (selector, mut driver) = pair(1);
    let (_directory, file) = fifo(&selector);
    let cancellation = Cancellation::new();
    let buffer = vec![3];
    let pointer = buffer.as_ptr();
    let mut operation = Box::pin(selector.file_read(&file, buffer, &cancellation));
    assert!(block_on(poll_once(&mut operation)).is_none());
    driver.turn(Some(Duration::ZERO)).unwrap();
    cancellation.cancel();
    assert!(block_on(poll_once(&mut operation)).is_none());
    let deadline = Instant::now() + Duration::from_secs(10);
    let (result, buffer) = loop {
        driver.turn(Some(Duration::from_millis(10))).unwrap();
        if let Some(result) = block_on(poll_once(&mut operation)) {
            break result;
        }
        assert!(Instant::now() < deadline);
    };
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    assert_eq!(pointer, buffer.as_ptr());
    drain(&selector, &mut driver);
}

#[test]
fn dropping_an_undriven_driver_returns_queued_buffers_and_closes_fallback() {
    let (selector, driver) = pair(1);
    let (_directory, file) = file(&selector);
    let never = Cancellation::never();
    let mut operation = Box::pin(selector.file_read(&file, vec![3], &never));
    assert!(block_on(poll_once(&mut operation)).is_none());
    drop(driver);
    let (result, buffer) = block_on(operation);
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(buffer, [3]);
    assert_eq!(
        block_on(selector.file_seek(&file, std::io::SeekFrom::Start(0), &never))
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    block_on(selector.shutdown());
}

#[test]
fn transient_syscall_errors_preserve_progress_and_fatal_errors_stop_the_driver() {
    for (name, error) in [
        ("submit", io::ErrorKind::Interrupted.into()),
        ("submit", io::Error::from_raw_os_error(libc::EBUSY)),
        ("submit", io::Error::from_raw_os_error(libc::EAGAIN)),
        ("wait", io::ErrorKind::Interrupted.into()),
        ("wait", io::Error::from_raw_os_error(libc::ETIME)),
        ("wait", io::Error::from_raw_os_error(libc::EBUSY)),
        ("wait", io::Error::from_raw_os_error(libc::EAGAIN)),
    ] {
        let (selector, mut driver) = pair(1);
        let _failures = Failures::new([(name, error)]);
        // A productive/nonblocking turn publishes without waiting; an idle turn combines publication and waiting in the wait syscall.
        let timeout = if name == "submit" {
            Duration::ZERO
        } else {
            Duration::from_millis(1)
        };
        assert!(!driver.turn(Some(timeout)).unwrap());
        assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
        drain(&selector, &mut driver);
    }
    for name in ["submit", "wait"] {
        let (selector, mut driver) = pair(1);
        let _failures = Failures::new([(name, io::ErrorKind::PermissionDenied.into())]);
        let timeout = if name == "submit" {
            Duration::ZERO
        } else {
            Duration::from_millis(1)
        };
        assert_eq!(
            driver.turn(Some(timeout)).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(driver.turn(None).unwrap());
        block_on(selector.shutdown());
    }
}

#[test]
fn event_loop_wakers_and_notification_errors_do_not_lose_work() {
    let (selector, mut driver) = pair(1);
    let waker = driver.waker();
    waker.wake_by_ref();
    assert!(!driver.turn(None).unwrap());
    #[allow(clippy::waker_clone_wake)]
    waker.clone().wake();
    assert!(!driver.turn(None).unwrap());
    {
        let _failures = Failures::new([("notify", io::ErrorKind::Interrupted.into())]);
        waker.wake_by_ref();
    }
    assert!(!driver.turn(None).unwrap());
    // A full notification eventfd already holds a wakeup.
    waker.wake_by_ref();
    {
        let _failures = Failures::new([("notify", io::ErrorKind::WouldBlock.into())]);
        waker.wake_by_ref();
    }
    assert!(!driver.turn(None).unwrap());
    drain(&selector, &mut driver);
    waker.wake_by_ref();
}

#[test]
fn successful_completion_wins_cancellation_and_releases_the_file_before_waking() {
    let (selector, mut driver) = pair(1);
    let (_directory, file) = file(&selector);
    let token = Cancellation::new();
    let mut operation = Box::pin(selector.file_read_at(&file, vec![0; 6], 0, &token));
    assert!(block_on(poll_once(&mut operation)).is_none());
    let deadline = Instant::now() + Duration::from_secs(10);
    while selector.controllers[0]
        .capacity
        .state
        .load(Ordering::Acquire)
        & !CAPACITY_CLOSED
        != 0
    {
        driver.turn(Some(Duration::from_millis(10))).unwrap();
        assert!(Instant::now() < deadline);
    }
    token.cancel();
    let (result, buffer) = block_on(operation);
    assert_eq!(result.unwrap(), 6);
    assert_eq!(buffer, b"abcdef");
    block_on(selector.file_close(file)).unwrap();
    drain(&selector, &mut driver);
}

pub(super) fn drive<F: Future>(driver: &mut IoUringDriver, future: F) -> F::Output {
    let mut future = pin!(future);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(output) = block_on(poll_once(&mut future)) {
            return output;
        }
        driver.turn(Some(Duration::from_millis(10))).unwrap();
        assert!(Instant::now() < deadline);
    }
}

#[test]
fn empty_operations_keep_native_access_mode_errors() {
    let (selector, mut driver) = pair(2);
    let (directory, file) = file(&selector);
    let never = Cancellation::never();
    assert_eq!(
        drive(
            &mut driver,
            selector.file_read_at(&file, Vec::new(), 0, &never)
        )
        .0
        .unwrap(),
        0
    );
    assert_eq!(
        drive(&mut driver, selector.file_write(&file, Vec::new(), &never))
            .0
            .unwrap(),
        0
    );
    let read_only =
        block_on(selector.file_open(&directory.path().join("file"), FileOptions::READ, &never))
            .unwrap();
    let write_only = block_on(selector.file_open(
        &directory.path().join("file"),
        FileOptions {
            read: false,
            write: true,
            ..Default::default()
        },
        &never,
    ))
    .unwrap();
    assert_eq!(
        drive(
            &mut driver,
            selector.file_write(&read_only, Vec::new(), &never)
        )
        .0
        .unwrap_err()
        .kind(),
        io::Error::from_raw_os_error(libc::EBADF).kind()
    );
    assert_eq!(
        drive(
            &mut driver,
            selector.file_read(&write_only, Vec::new(), &never)
        )
        .0
        .unwrap_err()
        .kind(),
        io::Error::from_raw_os_error(libc::EBADF).kind()
    );
    drain(&selector, &mut driver);
}

#[test]
fn driver_failure_retains_submitted_storage_and_wakes_waiters_with_a_panic() {
    for fatal_turn in [false, true] {
        let (selector, mut driver) = pair(1);
        let (_directory, file) = fifo(&selector);
        let drops = Arc::new(AtomicUsize::new(0));
        let never = Cancellation::never();
        let mut operation = Box::pin(selector.file_read(
            &file,
            ObservedBuffer {
                bytes: vec![0],
                drops: Arc::clone(&drops),
            },
            &never,
        ));
        assert!(block_on(poll_once(&mut operation)).is_none());
        driver.turn(Some(Duration::ZERO)).unwrap();
        assert_eq!(driver.reactor.as_ref().unwrap().in_flight.0.len(), 1);
        if fatal_turn {
            let _failures = Failures::new([("wait", io::ErrorKind::PermissionDenied.into())]);
            // Retire the submission notification and its CQE before the idle wait failure.
            let mut error = None;
            for _ in 0..8 {
                if let Err(failure) = driver.turn(Some(Duration::from_millis(1))) {
                    error = Some(failure);
                    break;
                }
            }
            assert_eq!(error.unwrap().kind(), io::ErrorKind::PermissionDenied);
        }
        drop(driver);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(operation))).is_err()
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            selector.controllers[0]
                .capacity
                .state
                .load(Ordering::Acquire)
                & !CAPACITY_CLOSED,
            1
        );
        // SAFETY: Observe the retained owner count without using its descriptor, and release this temporary alias immediately.
        assert_eq!(Arc::strong_count(&unsafe { file.retain_native() }), 3);
        block_on(selector.shutdown());
    }
}

#[test]
fn abandoned_buffer_destructor_panics_do_not_stop_other_operations() {
    struct PanicsOnDrop(Vec<u8>, Arc<AtomicUsize>);
    impl AsRef<[u8]> for PanicsOnDrop {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }
    impl AsMut<[u8]> for PanicsOnDrop {
        fn as_mut(&mut self) -> &mut [u8] {
            &mut self.0
        }
    }
    // SAFETY: The wrapper retains stable initialized heap storage across moves.
    unsafe impl IoBuf for PanicsOnDrop {}
    // SAFETY: The wrapper exclusively owns its writable storage.
    unsafe impl IoBufMut for PanicsOnDrop {}
    impl Drop for PanicsOnDrop {
        fn drop(&mut self) {
            self.1.fetch_add(1, Ordering::SeqCst);
            panic!("abandoned buffer destructor");
        }
    }
    for submitted in [false, true] {
        let (selector, mut driver) = pair(2);
        let (_directory, file) = file(&selector);
        let never = Cancellation::never();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut operation =
            Box::pin(selector.file_read(&file, PanicsOnDrop(vec![0], Arc::clone(&drops)), &never));
        assert!(block_on(poll_once(&mut operation)).is_none());
        if submitted {
            driver.turn(Some(Duration::ZERO)).unwrap();
        }
        drop(operation);
        let (result, buffer) = drive(
            &mut driver,
            selector.file_read_at(&file, vec![0; 6], 0, &never),
        );
        assert_eq!(result.unwrap(), 6);
        assert_eq!(buffer, b"abcdef");
        drain(&selector, &mut driver);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn close_returns_pending_owners_and_cancellation_tolerates_retired_requests() {
    let (selector, mut driver) = pair(2);
    let (_directory, file) = file(&selector);
    let never = Cancellation::never();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut operation = Box::pin(selector.file_read_at(
        &file,
        ObservedBuffer {
            bytes: vec![0],
            drops: Arc::clone(&drops),
        },
        0,
        &never,
    ));
    assert!(block_on(poll_once(&mut operation)).is_none());
    let deadline = Instant::now() + Duration::from_secs(10);
    while selector.controllers[0]
        .capacity
        .state
        .load(Ordering::Acquire)
        & !CAPACITY_CLOSED
        != 0
    {
        driver.turn(Some(Duration::from_millis(10))).unwrap();
        assert!(Instant::now() < deadline);
    }
    // Completion is stored, but the future has not observed it. Abandonment sends a now-stale cancellation which must be harmless.
    drop(operation);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    driver.turn(Some(Duration::ZERO)).unwrap();
    let mut queued = Box::pin(selector.file_read(&file, vec![5], &never));
    assert!(block_on(poll_once(&mut queued)).is_none());
    selector.close();
    assert!(driver.turn(None).unwrap());
    let (result, buffer) = block_on(queued);
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
    assert_eq!(buffer, [5]);
    block_on(selector.shutdown());
}

#[test]
fn full_submission_queue_defers_cancellations_and_shutdown_tolerates_duplicate_requests() {
    let (selector, mut driver) = IoUringSelector::new(IoUringOptions {
        entries: 1,
        capacity: 4,
        ..Default::default()
    })
    .unwrap();
    let (_directory, file) = fifo(&selector);
    let never = Cancellation::never();
    let mut operations = Vec::new();
    for _ in 0..4 {
        let mut operation = Box::pin(selector.file_read(&file, vec![0], &never));
        assert!(block_on(poll_once(&mut operation)).is_none());
        driver.turn(Some(Duration::ZERO)).unwrap();
        operations.push(operation);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while driver.reactor.as_ref().unwrap().in_flight.0.len() < 4 {
        assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
        assert!(Instant::now() < deadline);
    }
    selector.close();
    // Shutdown marks each request as cancelling; dropping each future also sends its own cancellation to the same owner.
    drop(operations);
    drain(&selector, &mut driver);
    assert_eq!(
        selector.controllers[0]
            .capacity
            .state
            .load(Ordering::Acquire)
            & !CAPACITY_CLOSED,
        0
    );
}

#[test]
fn submission_racing_shutdown_or_driver_exit_returns_unpublished_storage() {
    for driver_exited in [false, true] {
        let (selector, driver) = pair(1);
        let (_directory, file) = file(&selector);
        let never = Cancellation::never();
        // Stage the legitimate interval after admission and request preparation but before publication, so shutdown can deterministically win that race.
        let permit = block_on(selector.controllers[0].capacity.acquire(&never)).unwrap();
        let (reply, result) = oneshot::channel();
        let request = Box::new(Request {
            identifier: selector.controllers[0].next_identifier().unwrap(),
            _permit: permit,
            resource: Some(Resource::File(retain_file(&file), None)),
            buffer: Some(vec![3]),
            pointer: |buffer| buffer.as_mut_ptr(),
            write: false,
            reply: Cell::new(Some(reply)),
            cancelling: Cell::new(false),
        });
        selector.close();
        if driver_exited {
            drop(driver);
            let (result, buffer) = block_on(IoUringSelector::submit(
                Arc::clone(&selector.controllers[0]),
                request,
                result,
                &never,
            ));
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, [3]);
        } else {
            let mut driver = driver;
            let (result, buffer) = block_on(IoUringSelector::submit(
                Arc::clone(&selector.controllers[0]),
                request,
                result,
                &never,
            ));
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer, [3]);
            drain(&selector, &mut driver);
        }
        assert_eq!(
            selector.controllers[0]
                .capacity
                .state
                .load(Ordering::Acquire)
                & !CAPACITY_CLOSED,
            0
        );
        block_on(selector.shutdown());
    }
}

#[test]
fn convenience_driver_loop_reports_the_original_failure() {
    let (selector, driver) = pair(1);
    let _failures = Failures::new([("wait", io::ErrorKind::PermissionDenied.into())]);
    assert_eq!(
        driver.run().unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    block_on(selector.shutdown());
}

fn active(controller: &Controller) -> usize {
    controller.capacity.state.load(Ordering::Acquire) & !CAPACITY_CLOSED
}

#[test]
fn workers_route_locally_and_external_operations_share_ring_capacity_fairly() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    let (selector, drivers) = IoUringSelector::with_workers(
        IoUringOptions {
            capacity: 1,
            ..Default::default()
        },
        2,
    )
    .unwrap();
    let (_directory, file) = file(&selector);
    let never = Cancellation::never();
    let mut first = Box::pin(selector.file_read_at(&file, vec![0], 0, &never));
    let mut second = Box::pin(selector.file_read_at(&file, vec![0], 1, &never));
    assert!(block_on(poll_once(&mut first)).is_none());
    assert!(block_on(poll_once(&mut second)).is_none());
    assert_eq!(active(&selector.controllers[0]), 1);
    assert_eq!(active(&selector.controllers[1]), 1);
    let mut workers: Vec<_> = drivers.into_iter().map(SelectorDriver::enter).collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    while selector
        .controllers
        .iter()
        .any(|controller| active(controller) != 0)
    {
        for worker in &mut workers {
            worker.turn(Some(Duration::ZERO)).unwrap();
        }
        assert!(Instant::now() < deadline);
    }
    assert_eq!(block_on(first).1, b"a");
    assert_eq!(block_on(second).1, b"b");
    assert_eq!(selector.external_next.load(Ordering::Relaxed), 2);
    // The latest entered worker belongs to ring one. Local submission must neither advance external routing nor write the notification eventfd.
    let _failures = Failures::new([("notify", io::ErrorKind::PermissionDenied.into())]);
    let mut local = Box::pin(selector.file_read_at(&file, vec![0], 2, &never));
    assert!(block_on(poll_once(&mut local)).is_none());
    assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
    assert_eq!(selector.external_next.load(Ordering::Relaxed), 2);
    assert_eq!(active(&selector.controllers[0]), 0);
    assert_eq!(active(&selector.controllers[1]), 1);
    while active(&selector.controllers[1]) != 0 {
        workers[1].turn(Some(Duration::from_millis(10))).unwrap();
    }
    assert_eq!(block_on(local).1, b"c");
    selector.close();
    for worker in &mut workers {
        while !worker.turn(Some(Duration::from_millis(10))).unwrap() {}
    }
    block_on(selector.shutdown());
}

#[test]
fn moving_a_waiter_to_another_thread_keeps_cancellation_on_the_original_ring() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    let (selector, mut drivers) =
        IoUringSelector::with_workers(IoUringOptions::default(), 2).unwrap();
    let (_directory, file) = fifo(&selector);
    let second = drivers.pop().unwrap();
    let mut first = drivers.pop().unwrap().enter();
    let cancellation = Cancellation::new();
    let mut operation = Box::pin(selector.file_read(&file, vec![7], &cancellation));
    assert!(block_on(poll_once(&mut operation)).is_none());
    first.turn(Some(Duration::ZERO)).unwrap();
    let cancelled = &cancellation;
    let operation = std::thread::scope(|scope| {
        scope
            .spawn(move || {
                cancelled.cancel();
                assert!(block_on(poll_once(&mut operation)).is_none());
                operation
            })
            .join()
            .unwrap()
    });
    assert_eq!(active(&selector.controllers[0]), 1);
    assert_eq!(
        selector.controllers[1]
            .next_identifier
            .load(Ordering::Relaxed),
        0
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while active(&selector.controllers[0]) != 0 {
        first.turn(Some(Duration::from_millis(10))).unwrap();
        assert!(Instant::now() < deadline);
    }
    let (result, buffer) = block_on(operation);
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    assert_eq!(buffer, [7]);
    let mut second = second.enter();
    first.close();
    while !first.turn(Some(Duration::from_millis(10))).unwrap() {}
    while !second.turn(Some(Duration::from_millis(10))).unwrap() {}
    block_on(selector.shutdown());
}

#[test]
fn nested_selectors_restore_routes_and_never_use_another_selectors_ring() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    let (outer, driver) = pair(2);
    let (_directory, file) = file(&outer);
    let mut outer_worker = driver.enter();
    let (inner, driver) = pair(2);
    let inner_worker = driver.enter();
    assert!(Arc::ptr_eq(&outer.route(), &outer.controllers[0]));
    assert!(Arc::ptr_eq(&inner.route(), &inner.controllers[0]));
    assert_eq!(outer.external_next.load(Ordering::Relaxed), 0);
    assert_eq!(inner.external_next.load(Ordering::Relaxed), 0);
    // Dropping the nested worker restores the outer route and closes only its own selector.
    drop(inner_worker);
    block_on(inner.shutdown());
    let never = Cancellation::never();
    let mut operation = Box::pin(outer.file_read_at(&file, vec![0], 0, &never));
    assert!(block_on(poll_once(&mut operation)).is_none());
    while active(&outer.controllers[0]) != 0 {
        outer_worker.turn(Some(Duration::from_millis(10))).unwrap();
    }
    assert_eq!(block_on(operation).1, b"a");
    outer_worker.close();
    while !outer_worker.turn(Some(Duration::from_millis(10))).unwrap() {}
    block_on(outer.shutdown());
}

#[test]
fn dropping_local_driver_returns_queued_owners_before_shutdown_and_allows_reentrant_wakes() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    use std::task::{Context, Wake, Waker};
    struct Reenter(Arc<Controller>, AtomicUsize);
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {
            self.1.fetch_add(1, Ordering::SeqCst);
            let _ = self.0.send(Command::Cancel(0));
        }
    }
    let (selector, driver) = pair(1);
    let (_directory, file) = file(&selector);
    let worker = driver.enter();
    worker.waker().wake_by_ref();
    let never = Cancellation::never();
    let mut operation = Box::pin(selector.file_read(&file, vec![9], &never));
    let reenter = Arc::new(Reenter(
        Arc::clone(&selector.controllers[0]),
        AtomicUsize::new(0),
    ));
    let waker = Waker::from(Arc::clone(&reenter));
    assert!(
        operation
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(worker);
    assert!(
        selector.controllers[0]
            .shutdown
            .finished
            .load(Ordering::Acquire)
    );
    assert_eq!(active(&selector.controllers[0]), 0);
    assert_eq!(reenter.1.load(Ordering::SeqCst), 1);
    let (result, buffer) = block_on(operation);
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(buffer, [9]);
    block_on(selector.shutdown());
}

#[test]
fn closing_a_full_local_batch_drains_all_accepted_owners() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    for remote_close in [false, true] {
        let (selector, driver) = pair(300);
        let (_directory, file) = fifo(&selector);
        let mut worker = driver.enter();
        let never = Cancellation::never();
        let mut operations = Vec::new();
        for _ in 0..300 {
            let mut operation = Box::pin(selector.file_read(&file, vec![7], &never));
            assert!(block_on(poll_once(&mut operation)).is_none());
            operations.push(operation);
        }
        if remote_close {
            std::thread::scope(|scope| scope.spawn(|| selector.close()).join().unwrap());
        } else {
            selector.close();
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while !worker.turn(Some(Duration::from_millis(10))).unwrap() {
            assert!(Instant::now() < deadline);
        }
        assert_eq!(active(&selector.controllers[0]), 0);
        for operation in operations {
            assert!(block_on(operation).0.is_err());
        }
        block_on(selector.shutdown());
    }
}

#[test]
fn stopping_one_ring_closes_the_group_and_shared_fallback() {
    let (selector, mut drivers) =
        IoUringSelector::with_workers(IoUringOptions::default(), 2).unwrap();
    drop(drivers.pop());
    assert!(
        selector
            .controllers
            .iter()
            .all(|controller| controller.closed.load(Ordering::Acquire))
    );
    let mut driver = drivers.pop().unwrap();
    assert!(driver.turn(None).unwrap());
    block_on(selector.shutdown());
    assert!(matches!(
        block_on(selector.spawn_blocking(|| 42, &Cancellation::never())),
        Err(BlockingError::SchedulerClosed)
    ));
}

#[test]
fn migrated_cancellation_cannot_overtake_a_queued_submission() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    for submit_locally in [false, true] {
        let (selector, driver) = pair(4);
        let (_directory, file) = fifo(&selector);
        let never = Cancellation::never();
        // In either direction place another submission ahead of the target, so alternating queues observes the target cancellation first.
        let mut ahead = Box::pin(selector.file_read(&file, vec![0], &never));
        let mut target = Box::pin(selector.file_read(&file, vec![1], &never));
        let mut worker = if submit_locally {
            let worker = driver.enter();
            assert!(block_on(poll_once(&mut ahead)).is_none());
            assert!(block_on(poll_once(&mut target)).is_none());
            std::thread::scope(|scope| scope.spawn(move || drop(target)).join().unwrap());
            worker
        } else {
            assert!(block_on(poll_once(&mut ahead)).is_none());
            assert!(block_on(poll_once(&mut target)).is_none());
            let worker = driver.enter();
            drop(target);
            worker
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while active(&selector.controllers[0]) != 1 {
            worker.turn(Some(Duration::from_millis(10))).unwrap();
            assert!(
                Instant::now() < deadline,
                "cancellation overtook submission"
            );
        }
        // Only the unrelated read remains; cancelling it must allow ordinary shutdown.
        drop(ahead);
        worker.close();
        while !worker.turn(Some(Duration::from_millis(10))).unwrap() {
            assert!(Instant::now() < deadline);
        }
        block_on(selector.shutdown());
    }
}

#[test]
fn native_workers_use_distinct_rings_without_external_submission_routing() {
    use core_scheduler_native::{NativeExecutorOptions, NativeScheduler};
    let (selector, drivers) = IoUringSelector::with_workers(IoUringOptions::default(), 2).unwrap();
    let (_directory, file) = file(&selector);
    let file = Arc::new(file);
    let scheduler = Arc::new(
        NativeScheduler::with_drivers(
            selector,
            drivers,
            NativeExecutorOptions {
                workers: 2,
                timer_capacity: 8,
            },
        )
        .unwrap(),
    );
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let scheduler2 = Arc::clone(&scheduler);
        let file = Arc::clone(&file);
        let barrier = Arc::clone(&barrier);
        tasks.push(
            scheduler
                .spawn(async move {
                    // Both native threads must be polling before either issues its first local operation.
                    barrier.wait();
                    let never = Cancellation::never();
                    for offset in 0..6 {
                        let (result, buffer) = scheduler2
                            .file_read_at(&file, vec![0], offset, &never)
                            .await;
                        assert_eq!(result.unwrap(), 1);
                        assert_eq!(buffer[0], b'a' + offset as u8);
                    }
                })
                .unwrap(),
        );
    }
    for task in tasks {
        block_on(task).unwrap();
    }
    assert_eq!(
        scheduler.selector().external_next.load(Ordering::Relaxed),
        0
    );
    assert!(
        scheduler
            .selector()
            .controllers
            .iter()
            .all(|controller| controller.next_identifier.load(Ordering::Relaxed) > 0)
    );
    block_on(scheduler.shutdown());
}

#[test]
fn partial_ring_construction_releases_previously_created_drivers() {
    let _failures = Failures;
    FAILURES.with(|failures| {
        *failures.borrow_mut() = VecDeque::from([
            ("ring", None),
            ("ring", Some(io::ErrorKind::PermissionDenied.into())),
        ])
    });
    assert_eq!(
        IoUringSelector::with_workers(IoUringOptions::default(), 3)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
}

#[test]
fn external_admission_remains_bounded_under_contention() {
    let capacity = Arc::new(Capacity {
        state: AtomicUsize::new(0),
        available: CompletionEvent::new(),
        limit: 4,
    });
    std::thread::scope(|scope| {
        let barrier = Arc::new(std::sync::Barrier::new(8));
        for _ in 0..8 {
            let capacity = Arc::clone(&capacity);
            let barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..500 {
                    let permit = block_on(capacity.acquire(&Cancellation::never())).unwrap();
                    assert!(capacity.state.load(Ordering::Acquire) <= capacity.limit);
                    std::thread::yield_now();
                    drop(permit);
                }
            });
        }
    });
    assert_eq!(capacity.state.load(Ordering::Acquire), 0);
    capacity.close();
}

#[test]
fn nonblocking_and_productive_turns_do_not_prepare_an_idle_wait() {
    let (selector, mut driver) = pair(1);
    let (_directory, file) = file(&selector);
    let never = Cancellation::never();
    {
        let _failures = Failures::new([("wait", io::ErrorKind::PermissionDenied.into())]);
        assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
        let mut operation = Box::pin(selector.file_read_at(&file, vec![0; 6], 0, &never));
        assert!(block_on(poll_once(&mut operation)).is_none());
        // Submission and completion both return to the executor without an idle wait. Nonblocking turns may also retire the internal notification CQE.
        assert!(!driver.turn(None).unwrap());
        let deadline = Instant::now() + Duration::from_secs(10);
        let (result, buffer) = loop {
            if let Some(output) = block_on(poll_once(&mut operation)) {
                break output;
            }
            assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
            assert!(Instant::now() < deadline);
        };
        assert_eq!(result.unwrap(), 6);
        assert_eq!(buffer, b"abcdef");
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
    }
    drain(&selector, &mut driver);
}

#[test]
fn nonblocking_turn_preserves_notifications_for_the_next_idle_turn() {
    let (selector, mut driver) = pair(1);
    driver.waker().wake_by_ref();
    assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
    {
        let _failures = Failures::new([("wait", io::ErrorKind::PermissionDenied.into())]);
        // The previously published notification must return us to the executor before rearming or waiting.
        assert!(!driver.turn(None).unwrap());
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
    }
    drain(&selector, &mut driver);
}

#[test]
fn notifications_racing_idle_preparation_remain_observable() {
    // Cover notification before initial publication, initial blocking publication, and a wait whose notification poll is already armed.
    for (name, already_armed) in [("submit", false), ("wait", false), ("wait", true)] {
        let (selector, mut driver) = pair(1);
        if already_armed {
            driver.turn(Some(Duration::ZERO)).unwrap();
        }
        {
            let _failures = Failures;
            let waker = driver.waker();
            BEFORE_SYSCALL.with(|hook| {
                *hook.borrow_mut() = Some((name, Box::new(move || waker.wake_by_ref())));
            });
            let timeout = if name == "submit" {
                Duration::ZERO
            } else {
                Duration::from_secs(1)
            };
            assert!(!driver.turn(Some(timeout)).unwrap());
            assert!(BEFORE_SYSCALL.with(|hook| hook.borrow().is_none()));
            // The notification can yield its own CQE as well as its eventfd value. Both must be retired before sleeping again.
            let _wait_failure = Failures::new([("wait", io::ErrorKind::Interrupted.into())]);
            for _ in 0..4 {
                assert!(!driver.turn(Some(Duration::from_millis(1))).unwrap());
                if FAILURES.with(|failures| failures.borrow().is_empty()) {
                    break;
                }
            }
            assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
        }
        drain(&selector, &mut driver);
    }
}

#[test]
fn capacity_wait_observes_retirement_and_closure_before_registration() {
    let capacity = Arc::new(Capacity {
        state: AtomicUsize::new(0),
        available: CompletionEvent::new(),
        limit: 1,
    });
    let never = Cancellation::never();
    let permit = block_on(capacity.acquire(&never)).unwrap();
    // Stage retirement after admission observed a full pool but before it started listening.
    drop(permit);
    assert!(block_on(poll_once(capacity.wait_for_capacity(&never))).is_some());
    let permit = block_on(capacity.acquire(&never)).unwrap();
    let mut waiting = Box::pin(capacity.wait_for_capacity(&never));
    assert!(block_on(poll_once(&mut waiting)).is_none());
    drop(permit);
    assert!(block_on(poll_once(&mut waiting)).is_some());
    // Closure in the same interval must also bypass waiting, even if the last permit is still held.
    let permit = block_on(capacity.acquire(&never)).unwrap();
    capacity.close();
    assert!(block_on(poll_once(capacity.wait_for_capacity(&never))).is_some());
    assert_eq!(
        block_on(capacity.acquire(&never)).err().unwrap().kind(),
        io::ErrorKind::BrokenPipe
    );
    drop(permit);
}

#[test]
fn pending_submissions_are_flushed_before_preparing_to_sleep() {
    let (selector, mut driver) = IoUringSelector::new(IoUringOptions {
        entries: 1,
        capacity: 4,
        ..Default::default()
    })
    .unwrap();
    let (_directory, file) = fifo(&selector);
    let never = Cancellation::never();
    let mut operations = Vec::new();
    for _ in 0..4 {
        let mut operation = Box::pin(selector.file_read(&file, vec![0], &never));
        assert!(block_on(poll_once(&mut operation)).is_none());
        operations.push(operation);
    }
    {
        let _failures = Failures::new([("wait", io::ErrorKind::PermissionDenied.into())]);
        assert!(!driver.turn(None).unwrap());
        while driver.reactor.as_ref().unwrap().pending.len() > 1 {
            assert!(!driver.turn(None).unwrap());
        }
        assert_eq!(driver.reactor.as_ref().unwrap().in_flight.0.len(), 3);
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
    }
    drop(operations);
    drain(&selector, &mut driver);
}

#[test]
fn owner_wakes_coalesce_without_writes_and_survive_productive_turns() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    let (selector, driver) = pair(1);
    let (_directory, file) = file(&selector);
    let mut worker = driver.enter();
    let waker = worker.waker();
    {
        let _failures = Failures::new([("notify", io::ErrorKind::PermissionDenied.into())]);
        waker.wake_by_ref();
        waker.wake_by_ref();
        #[allow(clippy::waker_clone_wake)]
        waker.clone().wake();
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
    }
    assert!(!worker.turn(Some(Duration::ZERO)).unwrap());
    let never = Cancellation::never();
    let mut operation = Box::pin(selector.file_read_at(&file, vec![0; 6], 0, &never));
    assert!(block_on(poll_once(&mut operation)).is_none());
    // This productive idle turn must retain the local permit, just like the nonblocking turn above.
    assert!(!worker.turn(None).unwrap());
    let deadline = Instant::now() + Duration::from_secs(10);
    while active(&selector.controllers[0]) != 0 {
        assert!(!worker.turn(Some(Duration::ZERO)).unwrap());
        assert!(Instant::now() < deadline);
    }
    let (result, buffer) = block_on(operation);
    assert_eq!(result.unwrap(), 6);
    assert_eq!(buffer, b"abcdef");
    {
        let _failures = Failures::new([("wait", io::ErrorKind::Interrupted.into())]);
        // All three local wakes coalesced into one idle-turn permit.
        assert!(!worker.turn(None).unwrap());
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
        assert!(!worker.turn(Some(Duration::from_millis(1))).unwrap());
        assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
    }
    selector.close();
    while !worker.turn(Some(Duration::from_millis(10))).unwrap() {}
    block_on(selector.shutdown());
}

#[test]
fn local_readiness_submission_can_submit_and_wait_in_one_turn() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    let _hooks = Failures;
    let (selector, driver) = pair(1);
    let (sender, receiver) = block_on(core_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let mut worker = driver.enter();
    assert!(!worker.turn(Some(Duration::ZERO)).unwrap());
    let never = Cancellation::never();
    let mut waiting = Box::pin(selector.socket_wait(&receiver, Interest::Readable, &never));
    assert!(block_on(poll_once(&mut waiting)).is_none());
    BEFORE_SYSCALL.with(|hook| {
        *hook.borrow_mut() = Some((
            "wait",
            Box::new(move || {
                assert_eq!(
                    sender.try_send(b"x", SocketSendFlags::default()).unwrap(),
                    1
                );
            }),
        ));
    });
    // No callback or completion has made a task runnable. This turn publishes the queued PollAdd through the wait itself, without a submit-only enter.
    assert!(!worker.turn(None).unwrap());
    assert!(BEFORE_SYSCALL.with(|hook| hook.borrow().is_none()));
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        worker.turn(Some(Duration::ZERO)).unwrap();
        if let Some(result) = block_on(poll_once(&mut waiting)) {
            result.unwrap();
            break;
        }
        assert!(Instant::now() < deadline);
    }
    selector.close();
    while !worker.turn(Some(Duration::from_millis(10))).unwrap() {}
    block_on(selector.shutdown());
}

#[test]
fn a_full_local_readiness_batch_returns_before_waiting() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    let (selector, driver) = IoUringSelector::new(IoUringOptions {
        entries: 512,
        capacity: 300,
        ..Default::default()
    })
    .unwrap();
    let (_sender, receiver) = block_on(core_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let mut worker = driver.enter();
    worker.turn(Some(Duration::ZERO)).unwrap();
    let never = Cancellation::never();
    let mut waiting = Vec::new();
    for _ in 0..300 {
        let mut operation = Box::pin(selector.socket_wait(&receiver, Interest::Readable, &never));
        assert!(block_on(poll_once(&mut operation)).is_none());
        waiting.push(operation);
    }
    {
        let _failure = Failures::new([("wait", io::ErrorKind::Interrupted.into())]);
        // The first 256 commands fit in the SQ, but 44 local commands remain. Local submissions have no interrupt, so a full batch must not sleep.
        worker.turn(None).unwrap();
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
    }
    selector.close();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !worker.turn(Some(Duration::from_millis(10))).unwrap() {
        assert!(Instant::now() < deadline);
    }
    for operation in waiting {
        assert!(block_on(operation).is_err());
    }
    block_on(selector.shutdown());
}

#[test]
fn readiness_submission_is_deferred_for_at_most_one_nonblocking_turn() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    let (selector, driver) = pair(1);
    let (_sender, receiver) = block_on(core_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let mut worker = driver.enter();
    worker.turn(Some(Duration::ZERO)).unwrap();
    let never = Cancellation::never();
    let mut waiting = Box::pin(selector.socket_wait(&receiver, Interest::Readable, &never));
    assert!(block_on(poll_once(&mut waiting)).is_none());
    {
        let _failure = Failures::new([("submit", io::ErrorKind::Interrupted.into())]);
        worker.turn(Some(Duration::ZERO)).unwrap();
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
        // Busy workers still publish readiness on their next nonblocking turn.
        worker.turn(Some(Duration::ZERO)).unwrap();
        assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
    }
    selector.close();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !worker.turn(Some(Duration::from_millis(10))).unwrap() {
        assert!(Instant::now() < deadline);
    }
    assert!(block_on(waiting).is_err());
    block_on(selector.shutdown());
}

struct WakeOnBufferAccess {
    bytes: Vec<u8>,
    wake: std::task::Waker,
}
impl AsRef<[u8]> for WakeOnBufferAccess {
    fn as_ref(&self) -> &[u8] {
        self.wake.wake_by_ref();
        &self.bytes
    }
}
impl AsMut<[u8]> for WakeOnBufferAccess {
    fn as_mut(&mut self) -> &mut [u8] {
        self.wake.wake_by_ref();
        &mut self.bytes
    }
}
// SAFETY: The buffer exclusively owns one stable initialized Vec allocation.
unsafe impl IoBuf for WakeOnBufferAccess {}
// SAFETY: Mutable access cannot change the allocation or initialized range.
unsafe impl IoBufMut for WakeOnBufferAccess {}

#[test]
fn preparing_user_buffers_returns_to_the_executor_before_waiting() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    struct CountWake(AtomicUsize);
    impl std::task::Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (selector, driver) = pair(1);
    let (_directory, file) = file(&selector);
    let mut worker = driver.enter();
    worker.turn(Some(Duration::ZERO)).unwrap();
    let wakes = Arc::new(CountWake(AtomicUsize::new(0)));
    let buffer = WakeOnBufferAccess {
        bytes: vec![0; 6],
        wake: std::task::Waker::from(Arc::clone(&wakes)),
    };
    let never = Cancellation::never();
    let mut reading = Box::pin(selector.file_read_at(&file, buffer, 0, &never));
    assert!(block_on(poll_once(&mut reading)).is_none());
    {
        let _failure = Failures::new([("wait", io::ErrorKind::Interrupted.into())]);
        // This arbitrary waker need not be the driver's own interrupt. Preparing a payload must give the executor another turn before it can sleep.
        worker.turn(None).unwrap();
        assert_eq!(wakes.0.load(Ordering::SeqCst), 2);
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    let (result, buffer) = loop {
        if let Some(result) = block_on(poll_once(&mut reading)) {
            break result;
        }
        worker.turn(Some(Duration::ZERO)).unwrap();
        assert!(Instant::now() < deadline);
    };
    assert_eq!(result.unwrap(), 6);
    assert_eq!(buffer.bytes, b"abcdef");
    selector.close();
    while !worker.turn(Some(Duration::from_millis(10))).unwrap() {}
    block_on(selector.shutdown());
}

#[test]
fn idle_turn_consumes_local_and_remote_notifications_together() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    let (selector, driver) = pair(1);
    let mut worker = driver.enter();
    let waker = worker.waker();
    waker.wake_by_ref();
    std::thread::spawn(move || waker.wake()).join().unwrap();
    {
        let _failures = Failures::new([("wait", io::ErrorKind::Interrupted.into())]);
        assert!(!worker.turn(None).unwrap());
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
        // A completed eventfd polling request may need one further CQ turn before the wait.
        for _ in 0..4 {
            assert!(!worker.turn(Some(Duration::from_millis(1))).unwrap());
            if FAILURES.with(|failures| failures.borrow().is_empty()) {
                break;
            }
        }
        assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
    }
    selector.close();
    while !worker.turn(Some(Duration::from_millis(10))).unwrap() {}
    block_on(selector.shutdown());
}

#[test]
fn local_notifications_match_the_ring_controller_including_nested_workers() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    let (selector, drivers) = IoUringSelector::with_workers(IoUringOptions::default(), 2).unwrap();
    let mut workers: Vec<_> = drivers.into_iter().map(SelectorDriver::enter).collect();
    for target in [0, 1] {
        workers[target].waker().wake_by_ref();
        let _failures = Failures::new([("wait", io::ErrorKind::Interrupted.into())]);
        assert!(
            !workers[1 - target]
                .turn(Some(Duration::from_millis(1)))
                .unwrap()
        );
        assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
        let _target_failure = Failures::new([("wait", io::ErrorKind::Interrupted.into())]);
        assert!(!workers[target].turn(None).unwrap());
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
    }
    selector.close();
    for worker in &mut workers {
        while !worker.turn(Some(Duration::from_millis(10))).unwrap() {}
    }
    block_on(selector.shutdown());
}

#[test]
fn remote_wakes_interrupt_entered_workers_during_idle_preparation() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    for (name, already_armed) in [("submit", false), ("wait", false), ("wait", true)] {
        let (selector, driver) = pair(1);
        let mut worker = driver.enter();
        if already_armed {
            worker.turn(Some(Duration::ZERO)).unwrap();
        }
        {
            let _failures = Failures;
            let waker = worker.waker();
            BEFORE_SYSCALL.with(|hook| {
                *hook.borrow_mut() = Some((
                    name,
                    Box::new(move || {
                        std::thread::spawn(move || waker.wake()).join().unwrap();
                    }),
                ));
            });
            let timeout = if name == "submit" {
                Duration::ZERO
            } else {
                Duration::from_secs(1)
            };
            assert!(!worker.turn(Some(timeout)).unwrap());
            assert!(BEFORE_SYSCALL.with(|hook| hook.borrow().is_none()));
            // The eventfd polling completion and counter both retire without losing a wake or leaving the next wait permanently ready.
            let _wait_failure = Failures::new([("wait", io::ErrorKind::Interrupted.into())]);
            for _ in 0..4 {
                assert!(!worker.turn(Some(Duration::from_millis(1))).unwrap());
                if FAILURES.with(|failures| failures.borrow().is_empty()) {
                    break;
                }
            }
            assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
        }
        selector.close();
        while !worker.turn(Some(Duration::from_millis(10))).unwrap() {}
        block_on(selector.shutdown());
    }
}

#[test]
fn idle_ring_waits_honor_finite_timeouts_and_rearm_after_notifications() {
    let (selector, mut driver) = pair(1);
    // No request or notification is ready: the kernel times out the direct ring wait.
    let timeout = Duration::from_millis(2);
    let started = Instant::now();
    assert!(!driver.turn(Some(timeout)).unwrap());
    assert!(started.elapsed() >= timeout);
    assert!(driver.reactor.as_ref().unwrap().notification_armed);
    for _ in 0..3 {
        // A wake immediately before entering the kernel must interrupt an unlimited wait.
        {
            let _hooks = Failures;
            let waker = driver.waker();
            BEFORE_SYSCALL.with(|hook| {
                *hook.borrow_mut() = Some((
                    "wait",
                    Box::new(move || {
                        std::thread::spawn(move || waker.wake()).join().unwrap();
                    }),
                ));
            });
            assert!(!driver.turn(None).unwrap());
            assert!(BEFORE_SYSCALL.with(|hook| hook.borrow().is_none()));
        }
        // The wait collected PollAdd and drained its counter; rearm it.
        assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
        let _failure = Failures::new([("wait", io::ErrorKind::Interrupted.into())]);
        for _ in 0..4 {
            assert!(!driver.turn(Some(timeout)).unwrap());
            if FAILURES.with(|failures| failures.borrow().is_empty()) {
                break;
            }
        }
        assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
        assert!(driver.reactor.as_ref().unwrap().notification_armed);
    }
    // Exercise the signed-kernel-ABI clamp without waiting for that duration.
    {
        let _hooks = Failures;
        let waker = driver.waker();
        BEFORE_SYSCALL.with(|hook| {
            *hook.borrow_mut() = Some((
                "wait",
                Box::new(move || {
                    std::thread::spawn(move || waker.wake()).join().unwrap();
                }),
            ));
        });
        assert!(!driver.turn(Some(Duration::MAX)).unwrap());
        assert!(BEFORE_SYSCALL.with(|hook| hook.borrow().is_none()));
    }
    drain(&selector, &mut driver);
}

#[test]
fn remote_notification_retires_before_waiting_with_a_socket_read_pending() {
    let (selector, mut driver) = pair(1);
    let (sender, receiver) = block_on(core_scheduler_conformance::socket::stream_pair(
        &selector,
        SocketDomain::Ipv4,
    ));
    let never = Cancellation::never();
    let mut receiving = Box::pin(selector.socket_wait(&receiver, Interest::Readable, &never));
    assert!(block_on(poll_once(&mut receiving)).is_none());
    let waker = driver.waker();
    std::thread::spawn(move || waker.wake()).join().unwrap();
    {
        let _failures = Failures::new([("wait", io::ErrorKind::Interrupted.into())]);
        // The pending socket never becomes readable. An interrupt CQE must consume its eventfd value before rearming, otherwise every turn creates another immediately-ready interrupt CQE.
        for _ in 0..16 {
            assert!(!driver.turn(Some(Duration::from_millis(1))).unwrap());
            if FAILURES.with(|failures| failures.borrow().is_empty()) {
                break;
            }
        }
        assert!(
            FAILURES.with(|failures| failures.borrow().is_empty()),
            "remote notification kept the driver runnable indefinitely"
        );
        assert_eq!(driver.reactor.as_ref().unwrap().in_flight.0.len(), 1);
        assert!(block_on(poll_once(&mut receiving)).is_none());
    }
    drop(receiving);
    drain(&selector, &mut driver);
    drop(sender);
    drop(receiver);
}

#[test]
fn failed_notification_completion_still_retires_original_completions_in_the_batch() {
    for notification_first in [false, true] {
        let (selector, mut driver) = pair(1);
        let (_directory, file) = file(&selector);
        let never = Cancellation::never();
        let mut operation = Box::pin(selector.file_read_at(&file, vec![0; 6], 0, &never));
        assert!(block_on(poll_once(&mut operation)).is_none());
        let reactor = driver.reactor.as_mut().unwrap();
        let mut request = match reactor.incoming.try_recv().unwrap() {
            Command::Submit(request) => request,
            _ => panic!("expected queued read"),
        };
        let entry = request.entry();
        reactor.in_flight.0.insert(request.identifier(), request);
        // A real invalid PollAdd yields an error CQE. Place it on either side of an original completion to verify that neither CQ ordering strands the owned request.
        let notification = opcode::PollAdd::new(types::Fd(-1), libc::POLLIN as u32)
            .build()
            .user_data(NOTIFICATION_TAG);
        let entries = if notification_first {
            [notification, entry]
        } else {
            [entry, notification]
        };
        for entry in entries {
            // SAFETY: InFlight retains the original request and its storage. The invalid descriptor references no userspace memory.
            unsafe { reactor.ring.submission().push(&entry) }.unwrap();
        }
        reactor.ring.submit_and_wait(2).unwrap();
        assert_eq!(
            driver
                .turn(Some(Duration::ZERO))
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EBADF)
        );
        let (result, buffer) = block_on(operation);
        assert_eq!(result.unwrap(), 6);
        assert_eq!(buffer, b"abcdef");
        assert_eq!(active(&selector.controllers[0]), 0);
        assert!(driver.turn(None).unwrap());
        block_on(selector.shutdown());
    }
}

#[test]
fn full_eventfd_preserves_existing_notifications_without_failing_the_waker() {
    let (selector, mut driver) = pair(1);
    let value = u64::MAX - 1;
    // SAFETY: the controller owns the eventfd and value supplies its required eight-byte input.
    assert_eq!(
        unsafe {
            libc::write(
                selector.controllers[0].notification.as_raw_fd(),
                (&value as *const u64).cast(),
                8,
            )
        },
        8
    );
    // Adding one would exceed the eventfd counter's limit; the existing counter already wakes the worker.
    driver.waker().wake_by_ref();
    assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
    driver
        .reactor
        .as_mut()
        .unwrap()
        .ring
        .submit_and_wait(1)
        .unwrap();
    assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
    assert!(!driver.turn(Some(Duration::from_millis(1))).unwrap());
    drain(&selector, &mut driver);
}

#[test]
fn idle_waits_do_not_read_the_eventfd_without_a_notification_completion() {
    let (selector, mut driver) = pair(1);
    let _failures = Failures::new([("notification-read", io::ErrorKind::Interrupted.into())]);
    for _ in 0..3 {
        assert!(!driver.turn(Some(Duration::from_millis(1))).unwrap());
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
    }
    driver.waker().wake_by_ref();
    // The armed poll now reports the remote notification. Its CQE, rather than an idle-boundary read, drains the counter and retries interruption.
    driver
        .reactor
        .as_mut()
        .unwrap()
        .ring
        .submit_and_wait(1)
        .unwrap();
    assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
    assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
    drain(&selector, &mut driver);
}

#[test]
fn one_notification_read_clears_coalesced_wakes_without_a_second_read() {
    let (selector, mut driver) = pair(1);
    for _ in 0..3 {
        driver.waker().wake_by_ref();
    }
    {
        let _failures = Failures::new([]);
        FAILURES.with(|failures| {
            failures.borrow_mut().extend([
                ("notification-read", None),
                (
                    "notification-read",
                    Some(io::ErrorKind::PermissionDenied.into()),
                ),
            ]);
        });
        driver
            .reactor
            .as_ref()
            .unwrap()
            .drain_notification()
            .unwrap();
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
    }
    // All three notifications were consumed, so the next real read sees an empty eventfd.
    let mut value = 0u64;
    // SAFETY: This synchronous read targets a live eight-byte value and cannot retain its pointer.
    assert_eq!(
        unsafe {
            libc::read(
                selector.controllers[0].notification.as_raw_fd(),
                (&mut value as *mut u64).cast(),
                8,
            )
        },
        -1
    );
    assert_eq!(io::Error::last_os_error().kind(), io::ErrorKind::WouldBlock);
    // A stale notification CQE can find the counter already cleared. It must still permit rearming.
    driver
        .reactor
        .as_ref()
        .unwrap()
        .drain_notification()
        .unwrap();
    drain(&selector, &mut driver);
}

#[test]
fn interrupted_notification_reads_yield_without_losing_the_wake() {
    let (selector, mut driver) = pair(1);
    driver.waker().wake_by_ref();
    {
        let _failures = Failures::new(
            (0..17).map(|_| ("notification-read", io::ErrorKind::Interrupted.into())),
        );
        driver
            .reactor
            .as_ref()
            .unwrap()
            .drain_notification()
            .unwrap();
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
    }
    // The unread counter still completes the next poll after the bounded interrupted retries.
    assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
    driver
        .reactor
        .as_mut()
        .unwrap()
        .ring
        .submit_and_wait(1)
        .unwrap();
    assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
    drain(&selector, &mut driver);
}

#[test]
fn notification_completion_read_failure_stops_the_driver() {
    let (selector, mut driver) = pair(1);
    driver.waker().wake_by_ref();
    assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
    driver
        .reactor
        .as_mut()
        .unwrap()
        .ring
        .submit_and_wait(1)
        .unwrap();
    {
        let _failure =
            Failures::new([("notification-read", io::ErrorKind::PermissionDenied.into())]);
        assert_eq!(
            driver.turn(Some(Duration::ZERO)).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
    }
    assert!(driver.turn(None).unwrap());
    block_on(selector.shutdown());
}

#[test]
fn full_submission_queue_defers_waiting_until_the_notification_can_be_armed() {
    let (selector, mut driver) = IoUringSelector::new(IoUringOptions {
        entries: 1,
        ..Default::default()
    })
    .unwrap();
    // Keep one entry waiting for submission, as after a transient submission failure. Its cancellation tag requires no owned request.
    let entry = opcode::Nop::new().build().user_data(CANCEL_TAG);
    // SAFETY: Nop references no storage or descriptor.
    unsafe {
        driver
            .reactor
            .as_mut()
            .unwrap()
            .ring
            .submission()
            .push(&entry)
    }
    .unwrap();
    {
        let _failure = Failures::new([("wait", io::ErrorKind::PermissionDenied.into())]);
        assert!(!driver.turn(None).unwrap());
        assert!(!driver.reactor.as_ref().unwrap().notification_armed);
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
    }
    drain(&selector, &mut driver);
}

#[test]
fn eventfd_creation_reports_descriptor_exhaustion() {
    const CHILD: &str = "CORE_SCHEDULER_EVENTFD_EXHAUSTION_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let _hooks = Failures;
        let mut previous: libc::rlimit = unsafe { std::mem::zeroed() };
        // SAFETY: the pointer supplies a valid rlimit output. This fixture runs in a separate process so other tests retain their descriptor limits.
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut previous) },
            0
        );
        BEFORE_SYSCALL.with(|hook| {
            *hook.borrow_mut() = Some((
                "notification-pair",
                Box::new(move || {
                    let limit = libc::rlimit {
                        rlim_cur: 0,
                        rlim_max: previous.rlim_max,
                    };
                    // SAFETY: the pointer supplies a valid resource limit; only this child process's soft limit changes.
                    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
                }),
            ));
        });
        let error = IoUringSelector::new(IoUringOptions::default())
            .err()
            .unwrap();
        // SAFETY: restore the original valid limit before finishing the fixture.
        assert_eq!(
            unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &previous) },
            0
        );
        assert_eq!(error.raw_os_error(), Some(libc::EMFILE));
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "io_uring_selector::tests::eventfd_creation_reports_descriptor_exhaustion",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn one_entry_ring_submits_payload_and_cancellation_before_notification() {
    let (selector, mut driver) = IoUringSelector::new(IoUringOptions {
        entries: 1,
        capacity: 1,
        ..Default::default()
    })
    .unwrap();
    let (_directory, file) = fifo(&selector);
    let token = Cancellation::new();
    let mut operation = Box::pin(selector.file_read(&file, vec![0], &token));
    assert!(block_on(poll_once(&mut operation)).is_none());
    assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
    {
        let reactor = driver.reactor.as_ref().unwrap();
        assert_eq!(
            reactor.in_flight.0.len(),
            1,
            "payload must own the sole SQ slot on its first turn"
        );
        assert!(reactor.pending.is_empty());
        assert!(!reactor.notification_armed);
    }
    token.cancel();
    assert!(block_on(poll_once(&mut operation)).is_none());
    assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
    {
        let reactor = driver.reactor.as_ref().unwrap();
        assert!(
            reactor.cancellations.is_empty(),
            "cancellation must own the next available SQ slot"
        );
        assert!(!reactor.notification_armed);
    }
    let (result, buffer) = drive(&mut driver, operation);
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
    assert_eq!(buffer, [0]);
    drain(&selector, &mut driver);
}

#[test]
fn repeated_external_notifications_cannot_starve_a_one_entry_ring_payload() {
    let (selector, mut driver) = IoUringSelector::new(IoUringOptions {
        entries: 1,
        capacity: 1,
        ..Default::default()
    })
    .unwrap();
    let (_directory, file) = file(&selector);
    let never = Cancellation::never();
    let mut operation = Box::pin(selector.file_read_at(&file, vec![0; 6], 0, &never));
    assert!(block_on(poll_once(&mut operation)).is_none());
    assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
    let (wake, requests) = mpsc::channel();
    let (completed, acknowledgements) = mpsc::channel();
    let waker = driver.waker();
    let result = std::thread::scope(|scope| {
        let external = scope.spawn(move || {
            while requests.recv().is_ok() {
                waker.wake_by_ref();
                completed.send(()).unwrap();
            }
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut result = None;
        for _ in 0..64 {
            wake.send(()).unwrap();
            acknowledgements.recv().unwrap();
            // Make each preceding polling request observably ready before the next turn. With notification-first insertion this repeats the interrupt CQ indefinitely while the payload remains pending.
            driver
                .reactor
                .as_mut()
                .unwrap()
                .ring
                .submit_and_wait(1)
                .unwrap();
            assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
            result = block_on(poll_once(&mut operation));
            if result.is_some() || Instant::now() >= deadline {
                break;
            }
        }
        drop(wake);
        external.join().unwrap();
        result.expect("external notifications starved the payload past its completion deadline")
    });
    assert_eq!(result.0.unwrap(), 6);
    assert_eq!(result.1, b"abcdef");
    drain(&selector, &mut driver);
}

#[test]
fn deferred_cancellations_prevent_waiting_while_originals_fill_the_submission_queue() {
    for error in [
        io::ErrorKind::Interrupted.into(),
        io::Error::from_raw_os_error(libc::EBUSY),
    ] {
        let (selector, mut driver) = IoUringSelector::new(IoUringOptions {
            entries: 1,
            ..Default::default()
        })
        .unwrap();
        let (_directory, file) = fifo(&selector);
        let never = Cancellation::never();
        let mut operation = Box::pin(selector.file_read(&file, vec![7], &never));
        assert!(block_on(poll_once(&mut operation)).is_none());
        let request = match driver
            .reactor
            .as_ref()
            .unwrap()
            .incoming
            .try_recv()
            .unwrap()
        {
            Command::Submit(request) => request,
            _ => panic!("expected queued read"),
        };
        // Stage an idle armed notification before inserting the original. Removing this initial remote wake keeps a notification CQ from masking the deferred-cancellation wait decision.
        driver
            .reactor
            .as_ref()
            .unwrap()
            .drain_notification()
            .unwrap();
        assert!(!driver.turn(Some(Duration::from_millis(1))).unwrap());
        assert!(driver.reactor.as_ref().unwrap().notification_armed);
        let identifier = request.identifier();
        driver.reactor.as_mut().unwrap().pending.push_back(request);
        {
            let _failures = Failures::new([
                ("submit", error),
                ("wait", io::ErrorKind::PermissionDenied.into()),
            ]);
            assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
            assert!(driver.reactor.as_mut().unwrap().ring.submission().is_full());
            assert!(driver.reactor.as_mut().unwrap().cancel(identifier));
            assert_eq!(driver.reactor.as_ref().unwrap().cancellations.len(), 1);
            // The sole SQ entry is still the original read, which cannot complete without a writer. Submit it and return so a later turn can publish its cancellation; blocking here would strand the cancellation indefinitely.
            assert!(!driver.turn(Some(Duration::from_millis(1))).unwrap());
            assert_eq!(
                FAILURES.with(|failures| failures.borrow().len()),
                1,
                "deferred cancellation must prevent the wait syscall"
            );
            assert!(
                driver
                    .reactor
                    .as_mut()
                    .unwrap()
                    .ring
                    .submission()
                    .is_empty()
            );
        }
        let (result, buffer) = drive(&mut driver, operation);
        assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
        assert_eq!(buffer, [7]);
        drain(&selector, &mut driver);
    }
}

fn drive_bound_worker<F: Future>(
    worker: &mut impl core_scheduler_selector::SelectorWorker,
    future: F,
) -> F::Output {
    let mut future = pin!(future);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(output) = block_on(poll_once(&mut future)) {
            return output;
        }
        worker.turn(Some(Duration::from_millis(10))).unwrap();
        assert!(
            Instant::now() < deadline,
            "bound worker did not complete accepted I/O"
        );
    }
}

#[test]
fn movable_driver_never_upgrades_and_completes_inflight_io_after_thread_migration() {
    let _failures = Failures::new([]);
    BEFORE_SYSCALL.with(|hook| {
        *hook.borrow_mut() = Some((
            "deferred-ring",
            Box::new(|| {
                panic!("a movable driver must preserve cross-thread ring progress");
            }),
        ));
    });
    let (selector, mut driver) = pair(1);
    let (directory, file) = fifo(&selector);
    let never = Cancellation::never();
    let mut reading = Box::pin(selector.file_read(&file, vec![0], &never));
    assert!(block_on(poll_once(&mut reading)).is_none());
    driver.turn(Some(Duration::ZERO)).unwrap();
    assert_eq!(driver.reactor.as_ref().unwrap().in_flight.0.len(), 1);
    assert!(block_on(poll_once(&mut reading)).is_none());
    std::fs::write(directory.path().join("fifo"), b"x").unwrap();
    let selector_ref = &selector;
    let (mut driver, (result, buffer)) = std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let output = drive(&mut driver, reading);
                // Keep the issuing thread alive through retirement: thread exit can independently cancel that thread's pending kernel requests.
                drain(selector_ref, &mut driver);
                (driver, output)
            })
            .join()
            .unwrap()
    });
    assert_eq!(result.unwrap(), 1);
    assert_eq!(buffer, b"x");
    drop(file);
    assert!(driver.turn(None).unwrap());
    block_on(selector.shutdown());
    BEFORE_SYSCALL.with(|hook| assert!(hook.borrow().is_some()));
}

#[test]
fn fresh_bound_worker_attempts_optional_upgrade_without_losing_queued_io() {
    use core_scheduler_selector::SelectorDriver;
    let (selector, driver) = pair(1);
    let (_directory, file) = file(&selector);
    let never = Cancellation::never();
    let mut reading = Box::pin(selector.file_read_at(&file, vec![0; 3], 1, &never));
    assert!(block_on(poll_once(&mut reading)).is_none());
    let _failures = Failures::new([]);
    let attempted = Arc::new(AtomicBool::new(false));
    BEFORE_SYSCALL.with(|hook| {
        let attempted = Arc::clone(&attempted);
        *hook.borrow_mut() = Some((
            "deferred-ring",
            Box::new(move || {
                attempted.store(true, Ordering::SeqCst);
            }),
        ));
    });
    let mut worker = driver.enter();
    assert!(attempted.load(Ordering::SeqCst));
    let (result, buffer) = drive_bound_worker(&mut worker, reading);
    assert_eq!(result.unwrap(), 3);
    assert_eq!(buffer, b"bcd");
    block_on(selector.file_close(file)).unwrap();
    drive_bound_worker(&mut worker, selector.shutdown());
}

#[test]
fn optional_worker_upgrade_failures_preserve_the_valid_ring_and_queued_io() {
    use core_scheduler_selector::SelectorDriver;
    for failures in [
        vec![("deferred-ring", libc::EINVAL)],
        vec![
            ("deferred-ring", libc::EINVAL),
            ("cooperative-ring", libc::EINVAL),
        ],
        vec![
            ("deferred-ring", libc::EPERM),
            ("cooperative-ring", libc::EPERM),
        ],
    ] {
        let (selector, driver) = pair(1);
        let (_directory, file) = file(&selector);
        let never = Cancellation::never();
        let mut reading = Box::pin(selector.file_read(&file, vec![0; 6], &never));
        assert!(block_on(poll_once(&mut reading)).is_none());
        let _failures = Failures::new(
            failures
                .into_iter()
                .map(|(name, code)| (name, io::Error::from_raw_os_error(code))),
        );
        let mut worker = driver.enter();
        FAILURES.with(|failures| assert!(failures.borrow().is_empty()));
        let (result, buffer) = drive_bound_worker(&mut worker, reading);
        assert_eq!(result.unwrap(), 6);
        assert_eq!(buffer, b"abcdef");
        block_on(selector.file_close(file)).unwrap();
        drive_bound_worker(&mut worker, selector.shutdown());
    }
}

#[test]
fn nonblocking_worker_turns_deliver_io_without_an_idle_wait() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    let (selector, driver) = pair(1);
    let (directory, file) = fifo(&selector);
    let mut worker = driver.enter();
    let never = Cancellation::never();
    let mut reading = pin!(selector.file_read(&file, vec![0; 3], &never));
    assert!(block_on(poll_once(&mut reading)).is_none());
    // Submit while no data is available, then complete from another thread. A busy executor never needs to park to process the resulting task work.
    worker.turn(Some(Duration::ZERO)).unwrap();
    worker.turn(Some(Duration::ZERO)).unwrap();
    std::thread::spawn(move || std::fs::write(directory.path().join("fifo"), b"abc").unwrap())
        .join()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let (result, buffer) = loop {
        worker.turn(Some(Duration::ZERO)).unwrap();
        if let Some(output) = block_on(poll_once(&mut reading)) {
            break output;
        }
        assert!(
            Instant::now() < deadline,
            "nonblocking turns did not process completion work"
        );
        std::thread::yield_now();
    };
    assert_eq!(result.unwrap(), 3);
    assert_eq!(buffer, b"abc");
    drive_bound_worker(&mut worker, selector.shutdown());
}

#[test]
fn binding_an_already_driven_ring_preserves_inflight_io_without_upgrade() {
    use core_scheduler_selector::SelectorDriver;
    let (selector, mut driver) = pair(1);
    let (directory, file) = fifo(&selector);
    let never = Cancellation::never();
    let mut reading = Box::pin(selector.file_read(&file, vec![0], &never));
    assert!(block_on(poll_once(&mut reading)).is_none());
    driver.turn(Some(Duration::ZERO)).unwrap();
    assert_eq!(driver.reactor.as_ref().unwrap().in_flight.0.len(), 1);
    assert!(block_on(poll_once(&mut reading)).is_none());
    let _failures = Failures::new([]);
    BEFORE_SYSCALL.with(|hook| {
        *hook.borrow_mut() = Some((
            "deferred-ring",
            Box::new(|| {
                panic!("an already-driven ring must retain its accepted work");
            }),
        ));
    });
    let mut worker = driver.enter();
    BEFORE_SYSCALL.with(|hook| assert!(hook.borrow().is_some()));
    std::fs::write(directory.path().join("fifo"), b"x").unwrap();
    let (result, buffer) = drive_bound_worker(&mut worker, reading);
    assert_eq!(result.unwrap(), 1);
    assert_eq!(buffer, b"x");
    block_on(selector.file_close(file)).unwrap();
    drive_bound_worker(&mut worker, selector.shutdown());
}

#[test]
fn a_retired_driver_can_enter_a_worker_without_recreating_its_ring() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    let (selector, mut driver) = pair(1);
    drain(&selector, &mut driver);
    assert!(driver.reactor.is_none());
    let _failures = Failures::new([]);
    BEFORE_SYSCALL.with(|hook| {
        *hook.borrow_mut() = Some((
            "deferred-ring",
            Box::new(|| {
                panic!("a retired driver must not create another ring");
            }),
        ));
    });
    let mut worker = driver.enter();
    assert!(worker.turn(None).unwrap());
    BEFORE_SYSCALL.with(|hook| assert!(hook.borrow().is_some()));
    drop(worker);
    block_on(selector.shutdown());
}

#[test]
fn idle_wait_publishes_completions_and_preserves_explicit_wakes() {
    use core_scheduler_selector::{SelectorDriver, SelectorWorker};
    use std::task::Context;
    let _hooks = Failures;
    let (selector, driver) = pair(1);
    let (directory, file) = fifo(&selector);
    let mut worker = driver.enter();
    assert!(!worker.turn(Some(Duration::ZERO)).unwrap());
    let never = Cancellation::never();
    let mut operation = Box::pin(selector.file_read(&file, vec![0; 6], &never));
    let waker = worker.waker();
    let mut context = Context::from_waker(&waker);
    assert!(operation.as_mut().poll(&mut context).is_pending());
    assert!(!worker.turn(Some(Duration::ZERO)).unwrap());
    // Keep the read pending until after the initial completion snapshot, so this turn must wait and publish the completion before returning.
    BEFORE_SYSCALL.with(|hook| {
        *hook.borrow_mut() = Some((
            "wait",
            Box::new(move || std::fs::write(directory.path().join("fifo"), b"abcdef").unwrap()),
        ));
    });
    assert!(!worker.turn(None).unwrap());
    assert!(BEFORE_SYSCALL.with(|hook| hook.borrow().is_none()));
    let std::task::Poll::Ready((result, buffer)) = operation.as_mut().poll(&mut context) else {
        panic!("idle wait did not publish the completed read");
    };
    assert_eq!(result.unwrap(), 6);
    assert_eq!(buffer, b"abcdef");
    {
        let _failure = Failures::new([("wait", io::ErrorKind::Interrupted.into())]);
        // Completion delivery leaves its explicit owner wake for the next idle decision, rather than consuming it before returning to the executor.
        assert!(!worker.turn(None).unwrap());
        assert_eq!(FAILURES.with(|failures| failures.borrow().len()), 1);
        assert!(!worker.turn(None).unwrap());
        assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
    }
    selector.close();
    while !worker.turn(Some(Duration::from_millis(10))).unwrap() {}
    block_on(selector.shutdown());
}

#[test]
fn notification_read_failure_after_an_idle_wait_stops_the_driver() {
    let (selector, mut driver) = pair(1);
    assert!(!driver.turn(Some(Duration::ZERO)).unwrap());
    {
        let _failure =
            Failures::new([("notification-read", io::ErrorKind::PermissionDenied.into())]);
        let waker = driver.waker();
        BEFORE_SYSCALL.with(|hook| {
            *hook.borrow_mut() = Some(("wait", Box::new(move || waker.wake())));
        });
        assert_eq!(
            driver.turn(None).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(BEFORE_SYSCALL.with(|hook| hook.borrow().is_none()));
        assert!(FAILURES.with(|failures| failures.borrow().is_empty()));
    }
    assert!(driver.turn(None).unwrap());
    block_on(selector.shutdown());
}

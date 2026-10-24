// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::Interest;
use core_scheduler_selector::{SelectorDriver, SelectorWorker};
use futures_lite::future::{block_on, poll_once};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    pin::Pin,
    task::Context,
};

fn sockets(selector: &IoUringSelector) -> (NativeSocket, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    (
        selector
            .register_socket(listener.accept().unwrap().0)
            .unwrap(),
        peer,
    )
}

fn options() -> IoUringOptions {
    IoUringOptions {
        entries: 8,
        capacity: 8,
        ..Default::default()
    }
}

fn drive<T>(
    driver: &mut impl SelectorWorker,
    context: &mut Context<'_>,
    mut future: Pin<&mut impl Future<Output = T>>,
) -> T {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Poll::Ready(result) = future.as_mut().poll(context) {
            return result;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "monitor test timed out"
        );
        driver.turn(Some(Duration::ZERO)).unwrap();
    }
}

fn until(driver: &mut impl SelectorWorker, mut condition: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(
            std::time::Instant::now() < deadline,
            "monitor test timed out"
        );
        driver.turn(Some(Duration::ZERO)).unwrap();
    }
}

#[test]
fn repeated_notifications_retain_one_registration_without_allocations() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, mut peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let inspect = reader.retained();
    let never = Cancellation::never();
    let mut byte = [0; 16];
    let mut generation = None;
    for iteration in 0..34 {
        let counts = allocation_counter::measure(|| {
            let mut read = pin!(selector.socket_monitor_recv_poll(
                &mut reader,
                &mut byte,
                SocketRecvFlags::default(),
                &never,
            ));
            assert!(read.as_mut().poll(&mut context).is_pending());
            worker.turn(Some(Duration::ZERO)).unwrap();
            peer.write_all(b"x").unwrap();
            assert_eq!(drive(&mut worker, &mut context, read.as_mut()).unwrap(), 1);
        });
        if iteration >= 2 {
            assert_eq!(counts.count_total, 0);
        }
        assert!(inspect.registration().status.lock().unwrap().pending);
        let current = inspect
            .registration()
            .status
            .lock()
            .unwrap()
            .binding
            .as_ref()
            .unwrap()
            .state
            .generation;
        if let Some(previous) = generation {
            assert_eq!(current, previous);
        }
        generation = Some(current);
    }
    let mut close = pin!(selector.socket_monitor_close(&mut reader));
    drive(&mut worker, &mut context, close.as_mut()).unwrap();
    assert!(!inspect.registration().status.lock().unwrap().pending);
}

#[test]
fn short_stream_reads_clear_readiness_only_on_would_block_and_preserve_eof() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, mut peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let inspect = reader.retained();
    let never = Cancellation::never();
    let mut bytes = [0; 16];
    peer.write_all(b"one").unwrap();
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        3
    );
    assert_eq!(&bytes[..3], b"one");

    assert!(!inspect.registration().is_ready());

    let attempts = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&attempts);
    super::super::tests::BEFORE_SYSCALL.with(|hook| {
        *hook.borrow_mut() = Some((
            "monitor-recv",
            Box::new(move || {
                counted.fetch_add(1, Ordering::Relaxed);
            }),
        ));
    });
    {
        let mut receive = pin!(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &never
        ));
        assert!(receive.as_mut().poll(&mut context).is_pending());
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
        worker.turn(Some(Duration::ZERO)).unwrap();
        peer.write_all(b"two").unwrap();
        until(&mut worker, || inspect.registration().is_ready());
        assert_eq!(
            drive(&mut worker, &mut context, receive.as_mut()).unwrap(),
            3
        );
    }
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
    assert_eq!(&bytes[..3], b"two");
    assert!(inspect.registration().is_ready());

    peer.shutdown(std::net::Shutdown::Write).unwrap();
    {
        let mut receive = pin!(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &never
        ));
        assert_eq!(
            drive(&mut worker, &mut context, receive.as_mut()).unwrap(),
            0
        );
    }
    // EOF is not a short positive read: retain readiness so subsequent reads finish immediately.
    assert!(inspect.registration().is_ready());
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        0
    );
    let mut close = pin!(selector.socket_monitor_close(&mut reader));
    drive(&mut worker, &mut context, close.as_mut()).unwrap();
}

#[test]
fn full_reads_keep_progress_and_short_reads_preserve_racing_notifications() {
    let (selector, _driver) = IoUringSelector::new(options()).unwrap();
    let (socket, mut peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let inspect = reader.retained();
    let never = Cancellation::never();
    peer.write_all(b"abc").unwrap();
    let mut full = [0; 2];
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut full,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        2
    );
    assert_eq!(&full, b"ab");

    // Model a CQ notification delivered concurrently with the successful short syscall.
    let notified = inspect.retained();
    super::super::tests::BEFORE_SYSCALL.with(|hook| {
        *hook.borrow_mut() = Some((
            "monitor-recv",
            Box::new(move || notified.registration().notify()),
        ));
    });
    let mut short = [0; 8];
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut short,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        1
    );
    assert_eq!(short[0], b'c');

    assert!(inspect.registration().is_ready());

    peer.write_all(b"de").unwrap();
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut full,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        2
    );
    assert_eq!(&full, b"de");
    // A full receive leaves the cached readiness available for further draining.
    assert!(inspect.registration().is_ready());
}

#[test]
fn shutdown_readiness_survives_the_last_short_receive() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, mut peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let inspect = reader.retained();
    let never = Cancellation::never();
    {
        let mut waiting = pin!(selector.socket_monitor_wait(&mut reader, &never));
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        peer.write_all(b"last").unwrap();
        peer.shutdown(std::net::Shutdown::Write).unwrap();
        drive(&mut worker, &mut context, waiting.as_mut()).unwrap();
    }
    let mut bytes = [0; 16];
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        4
    );
    assert_eq!(&bytes[..4], b"last");
    assert!(inspect.registration().is_ready());
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        0
    );
    let mut close = pin!(selector.socket_monitor_close(&mut reader));
    drive(&mut worker, &mut context, close.as_mut()).unwrap();
}

#[test]
fn pending_receive_cancellation_leaves_the_stream_usable() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, mut peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let never = Cancellation::never();
    let mut bytes = [0; 8];
    peer.write_all(b"x").unwrap();
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        1
    );
    let cancellation = Cancellation::new();
    {
        let mut receive = pin!(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &cancellation
        ));
        assert!(receive.as_mut().poll(&mut context).is_pending());
        worker.turn(Some(Duration::ZERO)).unwrap();
        cancellation.cancel();
        assert!(matches!(
            receive.as_mut().poll(&mut context),
            Poll::Ready(Err(OperationError::Cancelled(_)))
        ));
    }
    peer.write_all(b"new").unwrap();
    let mut receive = pin!(selector.socket_monitor_recv_poll(
        &mut reader,
        &mut bytes,
        SocketRecvFlags::default(),
        &never
    ));
    assert_eq!(
        drive(&mut worker, &mut context, receive.as_mut()).unwrap(),
        3
    );
}

#[test]
fn peeking_and_empty_receives_keep_syscall_semantics() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, mut peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let inspect = reader.retained();
    let never = Cancellation::never();
    let mut bytes = [0; 8];
    peer.write_all(b"x").unwrap();
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        1
    );
    assert!(!inspect.registration().is_ready());

    peer.write_all(b"y").unwrap();
    // Both operations must try the syscall, even though no readiness was cached.
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut [],
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        0
    );
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags { peek: true },
            &never
        ))
        .unwrap(),
        1
    );
    assert_eq!(bytes[0], b'y');
    assert!(!inspect.registration().is_ready());
    {
        let mut waiting = pin!(selector.socket_monitor_wait(&mut reader, &never));
        drive(&mut worker, &mut context, waiting.as_mut()).unwrap();
    }
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags { peek: true },
            &never
        ))
        .unwrap(),
        1
    );
    assert!(inspect.registration().is_ready());
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        1
    );
    assert_eq!(bytes[0], b'y');
}

#[test]
fn datagram_scalar_receives_do_not_treat_short_messages_as_stream_exhaustion() {
    let (selector, _driver) = IoUringSelector::new(options()).unwrap();
    let never = Cancellation::never();
    let socket =
        block_on(selector.socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never))
            .unwrap();
    block_on(selector.socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)).unwrap();
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    peer.connect(selector.socket_local_address(&socket).unwrap())
        .unwrap();
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);

    // Queue several datagrams before the first syscall. A short message does not mean there are no further messages.
    for payload in [&b"x"[..], b"", b"yz"] {
        peer.send(payload).unwrap();
    }
    let mut bytes = [0; 8];
    for expected in [&b"x"[..], b"", b"yz"] {
        assert_eq!(
            block_on(selector.socket_monitor_recv_poll(
                &mut reader,
                &mut bytes,
                SocketRecvFlags::default(),
                &never
            ))
            .unwrap(),
            expected.len()
        );
        assert_eq!(&bytes[..expected.len()], expected);
    }
}

#[test]
fn unconnected_stream_receives_return_the_os_error_without_waiting() {
    let (selector, _driver) = IoUringSelector::new(options()).unwrap();
    let never = Cancellation::never();
    let socket =
        block_on(selector.socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never))
            .unwrap();
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let mut bytes = [0; 8];
    let result = block_on(poll_once(selector.socket_monitor_recv_poll(
        &mut reader,
        &mut bytes,
        SocketRecvFlags::default(),
        &never,
    )))
    .expect("unconnected receive must not wait for readiness");
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotConnected);
}

#[test]
fn cancelled_and_dropped_waiters_leave_the_registration_active() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, mut peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let inspect = reader.retained();
    let never = Cancellation::never();
    let token = Cancellation::new();
    let mut waiting = Box::pin(selector.socket_monitor_wait(&mut reader, &token));
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    worker.turn(Some(Duration::ZERO)).unwrap();
    token.cancel();
    assert!(matches!(
        block_on(waiting),
        Err(OperationError::Cancelled(_))
    ));
    let generation = inspect
        .registration()
        .status
        .lock()
        .unwrap()
        .binding
        .as_ref()
        .unwrap()
        .state
        .generation;
    let mut pending = Box::pin(selector.socket_monitor_wait(&mut reader, &never));
    assert!(pending.as_mut().poll(&mut context).is_pending());
    drop(pending);
    assert!(inspect.registration().status.lock().unwrap().pending);
    peer.write_all(b"x").unwrap();
    let mut byte = [0];
    let mut read = pin!(selector.socket_monitor_recv_poll(
        &mut reader,
        &mut byte,
        SocketRecvFlags::default(),
        &never
    ));
    drive(&mut worker, &mut context, read.as_mut()).unwrap();
    assert_eq!(
        inspect
            .registration()
            .status
            .lock()
            .unwrap()
            .binding
            .as_ref()
            .unwrap()
            .state
            .generation,
        generation
    );
}

#[test]
fn dropping_the_resource_retires_its_registration() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, _peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let inspect = reader.retained();
    let never = Cancellation::never();
    let mut waiting = Box::pin(selector.socket_monitor_wait(&mut reader, &never));
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    worker.turn(Some(Duration::ZERO)).unwrap();
    drop(waiting);
    drop(reader);
    assert!(inspect.registration().status.lock().unwrap().retiring);
    until(&mut worker, || {
        !inspect.registration().status.lock().unwrap().pending
    });
}

#[test]
fn close_drains_the_registration_and_rejects_subsequent_operations() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, _peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let never = Cancellation::never();
    let mut waiting = Box::pin(selector.socket_monitor_wait(&mut reader, &never));
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    drop(waiting);
    // Cancellation before the submission command is processed needs no kernel cancellation.
    let mut closing = Box::pin(selector.socket_monitor_close(&mut reader));
    drive(&mut worker, &mut context, closing.as_mut()).unwrap();
    drop(closing);
    block_on(selector.socket_monitor_close(&mut reader)).unwrap();
    assert_eq!(
        block_on(selector.socket_monitor_wait(&mut reader, &never))
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn driver_abandonment_notifies_active_queued_and_submission_space_waiters() {
    for turns in [0, 1, 2] {
        let (selector, mut driver) = IoUringSelector::new(IoUringOptions {
            entries: 1,
            capacity: 1,
            ..Default::default()
        })
        .unwrap();
        let (socket, _peer) = sockets(&selector);
        let never = Cancellation::never();
        let mut readers: Vec<_> = (0..3)
            .map(|_| selector.socket_monitor(&socket, Interest::Readable))
            .collect();
        let mut waits: Vec<_> = readers
            .iter_mut()
            .map(|reader| Box::pin(selector.socket_monitor_wait(reader, &never)))
            .collect();
        for wait in &mut waits {
            assert!(block_on(poll_once(wait.as_mut())).is_none());
        }
        for _ in 0..turns {
            driver.turn(Some(Duration::ZERO)).unwrap();
        }
        drop(driver);
        for wait in waits {
            assert_eq!(
                block_on(wait).unwrap_err().kind(),
                io::ErrorKind::BrokenPipe
            );
        }
    }
}

#[test]
fn registrations_do_not_consume_transient_operation_capacity() {
    let (selector, driver) = IoUringSelector::new(IoUringOptions {
        capacity: 1,
        ..options()
    })
    .unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, _peer) = sockets(&selector);
    let never = Cancellation::never();
    let mut readers: Vec<_> = (0..3)
        .map(|_| selector.socket_monitor(&socket, Interest::Readable))
        .collect();
    for reader in &mut readers {
        let mut wait = Box::pin(selector.socket_monitor_wait(reader, &never));
        assert!(wait.as_mut().poll(&mut context).is_pending());
        worker.turn(Some(Duration::ZERO)).unwrap();
    }
    assert_eq!(
        selector.controllers[0]
            .capacity
            .state
            .load(Ordering::Relaxed),
        0
    );
    for reader in &mut readers {
        let mut close = pin!(selector.socket_monitor_close(reader));
        drive(&mut worker, &mut context, close.as_mut()).unwrap();
    }
}

fn staged(selector: &IoUringSelector, storage: &Storage) -> Submission {
    let generation = selector.controllers[0].next_identifier().unwrap();
    let mut status = storage.registration().status.lock().unwrap();
    status.binding = Some(RingBinding::new(
        &selector.controllers[0],
        PollRegistration {
            generation,
            token: storage.as_ptr() as u64 | MONITOR_TAG,
        },
    ));
    status.pending = true;
    Submission::new(
        storage.retained(),
        Arc::clone(&storage.registration().status),
        generation,
    )
}

#[test]
fn terminal_and_cancellation_cqes_retire_in_either_order() {
    for cancel_first in [false, true] {
        let (selector, driver) = IoUringSelector::new(options()).unwrap();
        let (socket, _peer) = sockets(&selector);
        let storage = selector.socket_monitor(&socket, Interest::Readable);
        let submission = staged(&selector, &storage);
        let token = submission.token();
        let generation = submission.retirement.generation;
        let mut monitors = Monitors::new(1);
        monitors.insert(submission);
        assert!(monitors.cancel(token, generation + 2).is_none());
        assert_eq!(monitors.cancel(token, generation), Some(token));
        assert!(monitors.cancel(token, generation).is_none());
        let mut cancellations = VecDeque::new();
        monitors.close(&mut cancellations);
        assert!(cancellations.is_empty());
        monitors.complete(token, libc::POLLIN as i32, true, false);
        assert!(!monitors.is_empty());
        if cancel_first {
            monitors.complete(token, 0, false, true);
            assert!(!monitors.is_empty());
            monitors.complete(token, -libc::ECANCELED, false, false);
        } else {
            monitors.complete(token, libc::POLLIN as i32, false, false);
            assert!(!monitors.is_empty());
            monitors.complete(token, -libc::ENOENT, false, true);
        }
        assert!(monitors.is_empty());
        assert!(monitors.cancel(token, generation).is_none());
        drop(driver);
    }
}

#[test]
fn unsupported_multishot_poll_falls_back_and_terminal_success_can_rearm() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let (socket, _peer) = sockets(&selector);
    let mut storage = selector.socket_monitor(&socket, Interest::Readable);
    let mut monitors = Monitors::new(1);
    let submission = staged(&selector, &storage);
    let token = submission.token();
    monitors.insert(submission);
    monitors.complete(token, -libc::EINVAL, false, false);
    assert!(!storage.registration().multishot.load(Ordering::Relaxed));
    assert!(
        storage
            .registration()
            .status
            .lock()
            .unwrap()
            .error
            .is_none()
    );
    let submission = staged(&selector, &storage);
    monitors.insert(submission);
    monitors.complete(token, libc::POLLIN as i32, false, false);
    assert!(monitors.is_empty());
    block_on(selector.socket_monitor_wait(&mut storage, &Cancellation::never())).unwrap();
    // A terminal failure is surfaced instead of being mistaken for a readiness event.
    storage
        .registration()
        .clear_readiness(storage.registration().readiness.load(Ordering::Acquire));
    let submission = staged(&selector, &storage);
    monitors.insert(submission);
    monitors.complete(token, -libc::EBADF, false, false);
    assert_eq!(
        block_on(selector.socket_monitor_wait(&mut storage, &Cancellation::never()))
            .unwrap_err()
            .kind(),
        io::Error::from_raw_os_error(libc::EBADF).kind()
    );
    drop(driver);
}

#[test]
fn incompatible_closed_selectors_and_exhausted_ids_reject_waits() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let (other, other_driver) = IoUringSelector::new(options()).unwrap();
    let (socket, _peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let cancelled = Cancellation::new();
    cancelled.cancel();
    assert!(matches!(
        block_on(selector.socket_monitor_wait(&mut reader, &cancelled)),
        Err(OperationError::Cancelled(_))
    ));
    assert_eq!(
        block_on(other.socket_monitor_wait(&mut reader, &Cancellation::never()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        block_on(other.socket_monitor_close(&mut reader))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        block_on(selector.socket_monitor_send_poll(
            &mut reader,
            b"x",
            SocketSendFlags::default(),
            &Cancellation::never(),
        ))
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    let mut byte = [0];
    assert_eq!(
        block_on(other.socket_monitor_recv_poll(
            &mut reader,
            &mut byte,
            SocketRecvFlags::default(),
            &Cancellation::never()
        ))
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    selector.controllers[0]
        .next_identifier
        .store(CANCEL_TAG - 2, Ordering::Relaxed);
    assert_eq!(
        block_on(selector.socket_monitor_wait(&mut reader, &Cancellation::never()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::Other
    );
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut byte,
            SocketRecvFlags::default(),
            &Cancellation::never()
        ))
        .unwrap_err()
        .kind(),
        io::ErrorKind::Other
    );
    selector.close();
    assert_eq!(
        selector.socket_type(&socket).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        block_on(selector.socket_monitor_wait(&mut reader, &Cancellation::never()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    drop(driver);
    drop(other_driver);
}

#[test]
fn interrupted_syscalls_retry_and_observe_cancellation_and_closure() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, _peer) = sockets(&selector);
    let mut writer = selector.socket_monitor(&socket, Interest::Writable);
    let never = Cancellation::never();
    let mut attempts = 0;
    let mut operation =
        Box::pin(
            selector.monitor_io(&mut writer, Interest::Writable, &never, |_| {
                attempts += 1;
                match attempts {
                    1 => Err(io::ErrorKind::Interrupted.into()),
                    2 => Err(io::ErrorKind::WouldBlock.into()),
                    _ => Ok(7),
                }
            }),
        );
    assert_eq!(
        drive(&mut worker, &mut context, operation.as_mut()).unwrap(),
        7
    );
    drop(operation);
    let cancellation = Cancellation::new();
    assert!(matches!(
        block_on(
            selector.monitor_io::<()>(&mut writer, Interest::Writable, &cancellation, |_| {
                cancellation.cancel();
                Err(io::ErrorKind::Interrupted.into())
            })
        ),
        Err(OperationError::Cancelled(_))
    ));
    for kind in [io::ErrorKind::Interrupted, io::ErrorKind::WouldBlock] {
        let (selector, _driver) = IoUringSelector::new(options()).unwrap();
        let (socket, _peer) = sockets(&selector);
        let mut writer = selector.socket_monitor(&socket, Interest::Writable);
        assert_eq!(
            block_on(
                selector.monitor_io::<()>(&mut writer, Interest::Writable, &never, |_| {
                    selector.close();
                    Err(kind.into())
                })
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::BrokenPipe
        );
    }
}

#[test]
fn shutdown_overtakes_an_accepted_local_registration() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, _peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    assert!(
        selector.controllers[0]
            .send(Command::MonitorCancel(1, 0))
            .is_ok()
    );
    let never = Cancellation::never();
    let mut waiting = Box::pin(selector.socket_monitor_wait(&mut reader, &never));
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    std::thread::scope(|scope| {
        scope.spawn(|| selector.close()).join().unwrap();
    });
    assert_eq!(
        drive(&mut worker, &mut context, waiting.as_mut())
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    while !worker.turn(Some(Duration::ZERO)).unwrap() {}
}

#[test]
fn cached_readiness_survives_success_and_newer_notifications_survive_would_block() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let (socket, _peer) = sockets(&selector);
    let mut writer = selector.socket_monitor(&socket, Interest::Writable);
    let inspect = writer.retained();
    inspect.registration().notify();
    let before = inspect.registration().readiness.load(Ordering::Acquire);
    let mut attempts = 0;
    assert_eq!(
        block_on(selector.monitor_io(
            &mut writer,
            Interest::Writable,
            &Cancellation::never(),
            |_| {
                attempts += 1;
                if attempts == 1 {
                    // A notification racing the syscall's EAGAIN must not be cleared using the older readiness tick.
                    inspect.registration().notify();
                    Err(io::ErrorKind::WouldBlock.into())
                } else {
                    Ok(5)
                }
            }
        ))
        .unwrap(),
        5
    );
    assert_eq!(attempts, 2);
    assert_ne!(
        inspect.registration().readiness.load(Ordering::Acquire),
        before
    );
    assert!(inspect.registration().is_ready());
    block_on(selector.socket_monitor_wait(&mut writer, &Cancellation::never())).unwrap();
    drop(driver);
}

#[test]
fn readiness_generations_preserve_a_notification_across_wraparound() {
    let (selector, _driver) = IoUringSelector::new(options()).unwrap();
    let (socket, _peer) = sockets(&selector);
    let monitor = selector.socket_monitor(&socket, Interest::Readable);
    let registration = monitor.registration();
    registration.readiness.store(u64::MAX, Ordering::Release);
    registration.cleared.store(u64::MAX, Ordering::Release);
    assert!(!registration.is_ready());
    registration.notify();
    assert!(registration.is_ready());
    registration.clear_readiness(u64::MAX);
    assert!(registration.is_ready());
    registration.clear_readiness(registration.readiness.load(Ordering::Acquire));
    assert!(!registration.is_ready());
}

#[test]
fn one_shot_compatibility_waits_rearm_after_each_terminal_completion() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, mut peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    reader
        .registration()
        .multishot
        .store(false, Ordering::Relaxed);
    let inspect = reader.retained();
    let never = Cancellation::never();
    for _ in 0..2 {
        let mut byte = [0; 8];
        let mut read = pin!(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut byte,
            SocketRecvFlags::default(),
            &never
        ));
        assert!(read.as_mut().poll(&mut context).is_pending());
        peer.write_all(b"x").unwrap();
        until(&mut worker, || {
            !inspect.registration().status.lock().unwrap().pending
        });
        assert_eq!(drive(&mut worker, &mut context, read.as_mut()).unwrap(), 1);
        assert!(!inspect.registration().status.lock().unwrap().pending);
    }
}

#[test]
fn a_later_wait_rebinds_to_its_new_workers_ring_after_retirement() {
    let (selector, mut drivers) = IoUringSelector::with_workers(options(), 2).unwrap();
    let (socket, mut peer) = sockets(&selector);
    let reader = selector.socket_monitor(&socket, Interest::Readable);
    let second_driver = drivers.pop().unwrap();
    let first_driver = drivers.pop().unwrap();
    let (handoff, receive) = mpsc::channel();
    let finished = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let selector = &selector;
        let finished = &finished;
        scope.spawn(move || {
            let mut worker = first_driver.enter();
            let wake = worker.waker();
            let mut context = Context::from_waker(&wake);
            let never = Cancellation::never();
            let mut reader = reader;
            let mut wait = Box::pin(selector.socket_monitor_wait(&mut reader, &never));
            assert!(wait.as_mut().poll(&mut context).is_pending());
            worker.turn(Some(Duration::ZERO)).unwrap();
            drop(wait);
            assert!(handoff.send(reader).is_ok());
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while !finished.load(Ordering::Acquire) {
                assert!(std::time::Instant::now() < deadline);
                worker.turn(Some(Duration::from_millis(1))).unwrap();
            }
        });
        scope.spawn(move || {
            let mut worker = second_driver.enter();
            let wake = worker.waker();
            let mut context = Context::from_waker(&wake);
            let mut reader = receive.recv_timeout(Duration::from_secs(10)).unwrap();
            let inspect = reader.retained();
            let never = Cancellation::never();
            let mut waiting = Box::pin(selector.socket_monitor_wait(&mut reader, &never));
            assert!(waiting.as_mut().poll(&mut context).is_pending());
            until(&mut worker, || {
                if let Poll::Ready(result) = waiting.as_mut().poll(&mut context) {
                    panic!("unexpected completion: {result:?}");
                }
                let status = inspect.registration().status.lock().unwrap();
                status
                    .binding
                    .as_ref()
                    .is_some_and(|binding| binding.matches(&selector.controllers[1]))
            });
            peer.write_all(b"x").unwrap();
            drive(&mut worker, &mut context, waiting.as_mut()).unwrap();
            drop(waiting);
            let mut close = pin!(selector.socket_monitor_close(&mut reader));
            drive(&mut worker, &mut context, close.as_mut()).unwrap();
            finished.store(true, Ordering::Release);
        });
    });
}

fn move_active_monitor_future(receive_payload: bool) {
    let (selector, mut drivers) = IoUringSelector::with_workers(options(), 2).unwrap();
    let (socket, mut peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let never = Cancellation::never();
    let inspect = reader.retained();
    let mut byte = [0];
    let waiting: Pin<Box<dyn Future<Output = OperationResult<usize>> + Send + '_>> =
        if receive_payload {
            Box::pin(selector.socket_monitor_recv_poll(
                &mut reader,
                &mut byte,
                SocketRecvFlags::default(),
                &never,
            ))
        } else {
            Box::pin(async {
                selector
                    .socket_monitor_wait(&mut reader, &never)
                    .await
                    .map(|()| 0)
            })
        };
    let second_driver = drivers.pop().unwrap();
    let first_driver = drivers.pop().unwrap();
    let (handoff, receive) = mpsc::channel();
    let finished = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let selector = &selector;
        let finished = &finished;
        scope.spawn(move || {
            let mut worker = first_driver.enter();
            let wake = worker.waker();
            let mut context = Context::from_waker(&wake);
            let mut waiting = waiting;
            assert!(waiting.as_mut().poll(&mut context).is_pending());
            worker.turn(Some(Duration::ZERO)).unwrap();
            assert!(handoff.send(waiting).is_ok());
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while !finished.load(Ordering::Acquire) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "migrated monitor timed out"
                );
                worker.turn(Some(Duration::from_millis(10))).unwrap();
            }
        });
        scope.spawn(move || {
            let mut worker = second_driver.enter();
            let wake = worker.waker();
            let mut context = Context::from_waker(&wake);
            let mut waiting = receive.recv_timeout(Duration::from_secs(10)).unwrap();
            assert!(waiting.as_mut().poll(&mut context).is_pending());
            {
                let status = inspect.registration().status.lock().unwrap();
                assert!(Arc::ptr_eq(
                    &status.binding.as_ref().unwrap().controller,
                    &selector.controllers[0]
                ));
            }
            peer.write_all(b"x").unwrap();
            assert_eq!(
                drive(&mut worker, &mut context, Pin::new(&mut waiting)).unwrap(),
                usize::from(receive_payload)
            );
            finished.store(true, Ordering::Release);
        });
    });
    if receive_payload {
        assert_eq!(byte, [b'x']);
    }
}

#[test]
fn an_active_monitor_future_can_move_to_another_workers_thread() {
    move_active_monitor_future(false);
}

#[test]
fn an_active_monitor_receive_can_move_to_another_workers_thread() {
    move_active_monitor_future(true);
}

#[test]
fn shutdown_retires_active_and_accepted_queued_monitors() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, _peer) = sockets(&selector);
    let mut first = selector.socket_monitor(&socket, Interest::Readable);
    let mut second = selector.socket_monitor(&socket, Interest::Readable);
    let never = Cancellation::never();
    let mut active = Box::pin(selector.socket_monitor_wait(&mut first, &never));
    assert!(active.as_mut().poll(&mut context).is_pending());
    worker.turn(Some(Duration::ZERO)).unwrap();
    worker.turn(Some(Duration::ZERO)).unwrap();
    let mut queued = Box::pin(selector.socket_monitor_wait(&mut second, &never));
    assert!(queued.as_mut().poll(&mut context).is_pending());
    selector.close();
    assert_eq!(
        drive(&mut worker, &mut context, active.as_mut())
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        drive(&mut worker, &mut context, queued.as_mut())
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    while !worker.turn(Some(Duration::ZERO)).unwrap() {}
}

#[test]
fn dropping_close_leaves_retirement_running_and_can_be_awaited_again() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, _peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let never = Cancellation::never();
    let mut waiting = Box::pin(selector.socket_monitor_wait(&mut reader, &never));
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    worker.turn(Some(Duration::ZERO)).unwrap();
    worker.turn(Some(Duration::ZERO)).unwrap();
    drop(waiting);
    let mut close = Box::pin(selector.socket_monitor_close(&mut reader));
    assert!(close.as_mut().poll(&mut context).is_pending());
    drop(close);
    let mut close = pin!(selector.socket_monitor_close(&mut reader));
    drive(&mut worker, &mut context, close.as_mut()).unwrap();
}

#[test]
fn a_forgotten_wait_does_not_prevent_reuse_or_retirement() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let (socket, _peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let never = Cancellation::never();
    let mut waiting = Box::pin(selector.socket_monitor_wait(&mut reader, &never));
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    worker.turn(Some(Duration::ZERO)).unwrap();
    // Forgetting leaks only the future. A new exclusive borrow can replace its cached waker and reuse the same registration.
    std::mem::forget(waiting);
    {
        let mut waiting = Box::pin(selector.socket_monitor_wait(&mut reader, &never));
        assert!(waiting.as_mut().poll(&mut context).is_pending());
    }
    let mut byte = [0];
    let mut peer = _peer;
    peer.write_all(b"x").unwrap();
    {
        let mut reading = pin!(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut byte,
            SocketRecvFlags::default(),
            &never,
        ));
        assert_eq!(
            drive(&mut worker, &mut context, reading.as_mut()).unwrap(),
            1
        );
    }
    assert_eq!(byte, *b"x");
    let inspect = reader.retained();
    {
        let mut close = pin!(selector.socket_monitor_close(&mut reader));
        drive(&mut worker, &mut context, close.as_mut()).unwrap();
    }
    assert!(!inspect.registration().status.lock().unwrap().pending);
    drop(reader);
    assert!(inspect.registration().closed.load(Ordering::Acquire));
}

#[test]
fn dropping_a_task_retires_its_cached_waker_without_socket_activity() {
    use core_scheduler::Spawn;
    use core_scheduler_native::{NativeExecutorOptions, NativeScheduler};

    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let (socket, _peer) = sockets(&selector);
    let scheduler = Arc::new(
        NativeScheduler::with_drivers(
            selector,
            vec![driver],
            NativeExecutorOptions {
                workers: 1,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let (ready, pending) = std::sync::mpsc::channel();
    let shared = Arc::clone(&scheduler);
    let task = scheduler
        .spawn(async move {
            let mut reader = shared
                .selector()
                .socket_monitor(&socket, Interest::Readable);
            let inspect = reader.downgrade();
            let never = Cancellation::never();
            let mut byte = [0];
            let mut reading = pin!(shared.selector().socket_monitor_recv_poll(
                &mut reader,
                &mut byte,
                SocketRecvFlags::default(),
                &never,
            ));
            let mut ready = Some((ready, inspect));
            poll_fn(|context| {
                let result = reading.as_mut().poll(context);
                if result.is_pending() {
                    if let Some((ready, inspect)) = ready.take() {
                        ready.send(inspect).unwrap();
                    }
                }
                result
            })
            .await
        })
        .unwrap();
    let inspect = pending.recv_timeout(Duration::from_secs(10)).unwrap();
    drop(task);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while inspect.strong_count() != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "cached waker retained the abandoned task's reader"
        );
        std::thread::yield_now();
    }
    block_on(scheduler.shutdown());
}

#[test]
fn abandoning_a_child_wait_does_not_retain_its_parent_task() {
    struct Task {
        future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    }
    // The waker must own this task's future to expose a retained-waker cycle.
    #[allow(clippy::manual_noop_waker)]
    impl std::task::Wake for Task {
        fn wake(self: Arc<Self>) {}
    }

    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let (socket, _peer) = sockets(&selector);
    let selector = Arc::new(selector);
    let shared = Arc::clone(&selector);
    let (created, monitor) = std::sync::mpsc::channel();
    let task = Arc::new(Task {
        future: Mutex::new(Some(Box::pin(async move {
            let mut reader = shared.socket_monitor(&socket, Interest::Readable);
            created.send(reader.downgrade()).unwrap();
            let never = Cancellation::never();
            let mut byte = [0];
            {
                let mut reading = pin!(shared.socket_monitor_recv_poll(
                    &mut reader,
                    &mut byte,
                    SocketRecvFlags::default(),
                    &never,
                ));
                poll_fn(|context| {
                    assert!(reading.as_mut().poll(context).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
            // The reader survives its abandoned child operation. A cached child waker must not make this parent retain itself after its executor releases ownership.
            std::future::pending::<()>().await;
        }))),
    });
    let inspect = Arc::downgrade(&task);
    {
        let wake = Waker::from(Arc::clone(&task));
        let mut context = Context::from_waker(&wake);
        assert!(
            task.future
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .as_mut()
                .poll(&mut context)
                .is_pending()
        );
    }
    let monitor = monitor.recv().unwrap();
    worker.turn(Some(Duration::ZERO)).unwrap();
    drop(task);
    let released = inspect.strong_count() == 0;
    // Break a failed implementation's cycle before asserting so the test does not leak its socket, parent task or registration.
    if let Some(task) = inspect.upgrade() {
        let future = task.future.lock().unwrap().take();
        drop(future);
    }
    until(&mut worker, || monitor.strong_count() == 0);
    assert!(released, "abandoned child waker retained its parent task");
}

#[test]
fn datagram_boundaries_including_empty_messages_reuse_one_registration() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let never = Cancellation::never();
    let socket =
        block_on(selector.socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never))
            .unwrap();
    block_on(selector.socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)).unwrap();
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    peer.connect(selector.socket_local_address(&socket).unwrap())
        .unwrap();
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let inspect = reader.retained();
    let mut generation = None;
    for payload in [&b""[..], b"x", b"yz"] {
        let mut first = [0];
        let mut second = [0; 2];
        let mut buffers = [
            io::IoSliceMut::new(&mut first),
            io::IoSliceMut::new(&mut second),
        ];
        {
            let mut receive = pin!(selector.socket_monitor_recvmsg_poll(
                &mut reader,
                &mut buffers,
                SocketRecvFlags::default(),
                &never
            ));
            assert!(receive.as_mut().poll(&mut context).is_pending());
            worker.turn(Some(Duration::ZERO)).unwrap();
            assert_eq!(peer.send(payload).unwrap(), payload.len());
            assert_eq!(
                drive(&mut worker, &mut context, receive.as_mut())
                    .unwrap()
                    .bytes,
                payload.len()
            );
        }
        assert_eq!(&[first[0], second[0], second[1]][..payload.len()], payload);
        let current = inspect
            .registration()
            .status
            .lock()
            .unwrap()
            .binding
            .as_ref()
            .unwrap()
            .state
            .generation;
        if let Some(previous) = generation {
            assert_eq!(current, previous);
        }
        generation = Some(current);
    }
    let mut close = pin!(selector.socket_monitor_close(&mut reader));
    drive(&mut worker, &mut context, close.as_mut()).unwrap();
}

#[test]
fn a_general_tcp_monitor_keeps_syscall_first_behavior_after_short_reads() {
    let (selector, _driver) = IoUringSelector::new(options()).unwrap();
    let (socket, mut peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let never = Cancellation::never();
    reader.registration().notify();
    peer.write_all(b"a").unwrap();
    let mut buffer = [0; 16];
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut buffer,
            SocketRecvFlags::default(),
            &never,
        ))
        .unwrap(),
        1
    );

    assert!(reader.registration().is_ready());
    // New bytes can be read immediately without driving the selector, even after a short read.
    peer.write_all(b"b").unwrap();
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut buffer,
            SocketRecvFlags::default(),
            &never,
        ))
        .unwrap(),
        1
    );
    assert_eq!(buffer[0], b'b');
}

#[test]
fn a_stream_monitor_on_a_datagram_socket_keeps_message_semantics() {
    let (selector, _driver) = IoUringSelector::new(options()).unwrap();
    let never = Cancellation::never();
    let socket =
        block_on(selector.socket_open(SocketDomain::Ipv4, SocketType::Datagram, None, &never))
            .unwrap();
    block_on(selector.socket_bind(&socket, "127.0.0.1:0".parse().unwrap(), &never)).unwrap();
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    peer.connect(selector.socket_local_address(&socket).unwrap())
        .unwrap();
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);

    reader.registration().notify();
    for payload in [&b"x"[..], b"", b"yz"] {
        peer.send(payload).unwrap();
    }
    let mut buffer = [0; 16];
    for expected in [&b"x"[..], b"", b"yz"] {
        assert_eq!(
            block_on(selector.socket_monitor_recv_poll(
                &mut reader,
                &mut buffer,
                SocketRecvFlags::default(),
                &never,
            ))
            .unwrap(),
            expected.len()
        );
        assert_eq!(&buffer[..expected.len()], expected);
        assert!(reader.registration().is_ready());
    }
}

fn urgent_tcp_boundary(monitor: impl Fn(&IoUringSelector, &NativeSocket) -> Storage) {
    for (armed, inline, consumed) in [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (true, true, false),
        (false, false, true),
        (true, false, true),
    ] {
        let (selector, driver) = IoUringSelector::new(options()).unwrap();
        let mut worker = driver.enter();
        let wake = worker.waker();
        let mut context = Context::from_waker(&wake);
        let (socket, mut peer) = sockets(&selector);
        socket::retain_socket(&socket)
            .get_ref()
            .set_out_of_band_inline(inline)
            .unwrap();
        peer.write_all(b"before").unwrap();
        assert_eq!(
            socket2::SockRef::from(&peer)
                .send_with_flags(b"!", libc::MSG_OOB)
                .unwrap(),
            1
        );
        peer.write_all(b"after").unwrap();
        std::thread::sleep(Duration::from_millis(10));
        if consumed {
            // Consuming OOB removes POLLPRI but leaves the ordinary receive's short-read boundary.
            let mut urgent = [std::mem::MaybeUninit::uninit(); 1];
            assert_eq!(
                socket::retain_socket(&socket)
                    .get_ref()
                    .recv_out_of_band(&mut urgent)
                    .unwrap(),
                1
            );
        }
        let mut reader = monitor(&selector, &socket);
        let inspect = reader.retained();
        let never = Cancellation::never();
        if armed {
            let mut waiting = pin!(selector.socket_monitor_wait(&mut reader, &never));
            drive(&mut worker, &mut context, waiting.as_mut()).unwrap();
        }
        let mut buffer = [0; 64];
        let count = block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut buffer,
            SocketRecvFlags::default(),
            &never,
        ))
        .unwrap();
        assert_eq!(&buffer[..count], b"before");
        let count = {
            let mut receive = pin!(selector.socket_monitor_recv_poll(
                &mut reader,
                &mut buffer,
                SocketRecvFlags::default(),
                &never,
            ));
            let deadline = std::time::Instant::now() + Duration::from_millis(100);
            loop {
                if let Poll::Ready(result) = receive.as_mut().poll(&mut context) {
                    break result.unwrap();
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "trailing normal TCP bytes stalled at an urgent mark"
                );
                worker.turn(Some(Duration::ZERO)).unwrap();
            }
        };
        assert_eq!(
            &buffer[..count],
            if inline {
                &b"!after"[..]
            } else {
                &b"after"[..]
            }
        );
        {
            // Draining the ordinary bytes still clears readiness on WouldBlock, without spinning.
            let mut receive = pin!(selector.socket_monitor_recv_poll(
                &mut reader,
                &mut buffer,
                SocketRecvFlags::default(),
                &never,
            ));
            assert!(receive.as_mut().poll(&mut context).is_pending());
            assert!(!inspect.registration().is_ready());
        }
        let mut close = pin!(selector.socket_monitor_close(&mut reader));
        drive(&mut worker, &mut context, close.as_mut()).unwrap();
    }
}

#[test]
fn urgent_tcp_boundary_keeps_trailing_bytes_readable() {
    urgent_tcp_boundary(|selector, socket| selector.socket_monitor(socket, Interest::Readable));
}

#[test]
fn completion_releases_backend_storage_before_waking() {
    struct ObserveRelease {
        owner: std::sync::Weak<State>,
        released: AtomicBool,
        woken: AtomicBool,
    }
    impl std::task::Wake for ObserveRelease {
        fn wake(self: Arc<Self>) {
            self.released
                .store(self.owner.upgrade().is_none(), Ordering::Release);
            self.woken.store(true, Ordering::Release);
        }
    }
    for finish in [false, true] {
        let (selector, _driver) = IoUringSelector::new(options()).unwrap();
        let (socket, _peer) = sockets(&selector);
        let storage = selector.socket_monitor(&socket, Interest::Readable);
        let observer = Arc::new(ObserveRelease {
            owner: storage.downgrade(),
            released: AtomicBool::new(false),
            woken: AtomicBool::new(false),
        });
        let submission = staged(&selector, &storage);
        storage.registration().status.lock().unwrap().waker =
            Some(Waker::from(Arc::clone(&observer)));
        drop(storage);
        if finish {
            submission.finish(Ok(()));
        } else {
            drop(submission);
        }
        assert!(observer.woken.load(Ordering::Acquire));
        assert!(
            observer.released.load(Ordering::Acquire),
            "completion woke its caller before releasing the monitor's socket owner"
        );
        // The only remaining socket reference belongs to the caller, so immediate exclusive close is valid.
        let native = socket::retain_socket(&socket);
        drop(socket);
        assert!(Arc::try_unwrap(native).is_ok());
    }
}

#[test]
fn a_close_caller_can_release_its_socket_before_the_completion_wake_returns() {
    struct PauseWake {
        entered: std::sync::mpsc::Sender<()>,
        resume: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl std::task::Wake for PauseWake {
        fn wake(self: Arc<Self>) {
            self.entered.send(()).unwrap();
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
        }
    }
    let (selector, _driver) = IoUringSelector::new(options()).unwrap();
    let (socket, _peer) = sockets(&selector);
    let mut monitor = selector.socket_monitor(&socket, Interest::Readable);
    let submission = staged(&selector, &monitor);
    let (entered, waking) = std::sync::mpsc::channel();
    let (resume, resumed) = std::sync::mpsc::channel();
    let waker = Waker::from(Arc::new(PauseWake {
        entered,
        resume: Mutex::new(resumed),
    }));
    let mut context = Context::from_waker(&waker);
    let mut thread = None;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut close = Box::pin(selector.socket_monitor_close(&mut monitor));
        assert!(close.as_mut().poll(&mut context).is_pending());
        thread = Some(std::thread::spawn(move || submission.finish(Ok(()))));
        // Hold the completing driver inside the wake callback while independently repolling close.
        waking.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(matches!(
            close.as_mut().poll(&mut context),
            Poll::Ready(Ok(()))
        ));
        drop(close);
        drop(monitor);
        block_on(selector.socket_close(socket)).unwrap();
    }));
    let _ = resume.send(());
    thread.unwrap().join().unwrap();
    result.unwrap();
}

#[test]
fn repolling_a_pending_receive_can_read_before_the_driver_reports_readiness() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let waker = worker.waker();
    let mut context = Context::from_waker(&waker);
    let (socket, mut peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let never = Cancellation::never();
    let mut bytes = [0; 8];
    {
        let mut receive = pin!(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &never,
        ));
        assert!(receive.as_mut().poll(&mut context).is_pending());
        // The registration is queued but the driver has not published any readiness. An independent wake must still be able to make synchronous progress.
        peer.write_all(b"ready").unwrap();
        assert!(matches!(
            receive.as_mut().poll(&mut context),
            Poll::Ready(Ok(5))
        ));
    }
    assert_eq!(&bytes[..5], b"ready");
    // Successful completion released the waiter claim, so the next operation can wait normally.
    {
        let mut receive = pin!(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &never,
        ));
        assert!(receive.as_mut().poll(&mut context).is_pending());
        peer.write_all(b"again").unwrap();
        assert_eq!(
            drive(&mut worker, &mut context, receive.as_mut()).unwrap(),
            5
        );
    }
    assert_eq!(&bytes[..5], b"again");
    let mut close = pin!(selector.socket_monitor_close(&mut reader));
    drive(&mut worker, &mut context, close.as_mut()).unwrap();
}

#[test]
fn cancelling_a_pending_receive_preserves_bytes_that_are_already_available() {
    let (selector, driver) = IoUringSelector::new(options()).unwrap();
    let mut worker = driver.enter();
    let waker = worker.waker();
    let mut context = Context::from_waker(&waker);
    let (socket, mut peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let cancellation = Cancellation::new();
    let mut bytes = [0; 8];
    {
        let mut receive = pin!(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &cancellation,
        ));
        assert!(receive.as_mut().poll(&mut context).is_pending());
        peer.write_all(b"ready").unwrap();
        cancellation.cancel();
        assert!(matches!(
            receive.as_mut().poll(&mut context),
            Poll::Ready(Err(OperationError::Cancelled(_)))
        ));
    }
    assert_eq!(
        block_on(selector.socket_monitor_recv_poll(
            &mut reader,
            &mut bytes,
            SocketRecvFlags::default(),
            &Cancellation::never(),
        ))
        .unwrap(),
        5
    );
    assert_eq!(&bytes[..5], b"ready");
    let mut close = pin!(selector.socket_monitor_close(&mut reader));
    drive(&mut worker, &mut context, close.as_mut()).unwrap();
}

#[test]
fn cancellation_racing_would_block_does_not_wait_for_readiness() {
    let (selector, _driver) = IoUringSelector::new(options()).unwrap();
    let (socket, _peer) = sockets(&selector);
    let mut reader = selector.socket_monitor(&socket, Interest::Readable);
    let cancellation = Cancellation::new();
    let signal = cancellation.clone();
    super::super::tests::BEFORE_SYSCALL.with(|hook| {
        *hook.borrow_mut() = Some((
            "monitor-recv",
            Box::new(move || {
                signal.cancel();
            }),
        ));
    });
    let mut byte = [0];
    let result = block_on(poll_once(selector.socket_monitor_recv_poll(
        &mut reader,
        &mut byte,
        SocketRecvFlags::default(),
        &cancellation,
    )));
    assert!(matches!(result, Some(Err(OperationError::Cancelled(_)))));
    // Cancellation released the claim even though it arrived between preflight and listener registration.
    assert!(
        block_on(poll_once(
            selector.socket_monitor_wait(&mut reader, &Cancellation::never())
        ))
        .is_none()
    );
}

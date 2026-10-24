// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use core_scheduler_selector::{SelectorDriver, SelectorWorker};
use futures_lite::future::{block_on, poll_once};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    pin::Pin,
    sync::atomic::AtomicUsize,
    task::Context,
    time::Instant,
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

fn drive<F: Future>(
    worker: &mut impl SelectorWorker,
    context: &mut Context<'_>,
    mut future: Pin<&mut F>,
) -> F::Output {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Poll::Ready(result) = future.as_mut().poll(context) {
            return result;
        }
        assert!(Instant::now() < deadline);
        worker.turn(Some(Duration::ZERO)).unwrap();
    }
}

#[test]
fn repeated_owned_reads_reuse_request_and_completion_storage_without_allocations() {
    let (selector, driver) = IoUringSelector::new(IoUringOptions {
        entries: 8,
        capacity: 8,
        ..Default::default()
    })
    .unwrap();
    let (socket, mut peer) = sockets(&selector);
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let never = Cancellation::never();
    let mut reader = selector.socket_stream_reader(&socket).unwrap();
    let mut buffer = Some(vec![0; 64]);
    for iteration in 0..34 {
        let counts = allocation_counter::measure(|| {
            peer.write_all(b"x").unwrap();
            let mut reading = pin!(reader.read_owned(buffer.take().unwrap(), &never));
            let (result, returned) = drive(&mut worker, &mut context, reading.as_mut());
            assert_eq!(result.unwrap(), 1);
            assert_eq!(returned[0], b'x');
            buffer = Some(returned);
        });
        if iteration >= 2 {
            assert_eq!(counts.count_total, 0);
        }
    }
    drive(&mut worker, &mut context, pin!(reader.close())).unwrap();
}

#[test]
fn abandoning_receive_retires_its_owner_and_allows_a_different_buffer_type() {
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let (socket, mut peer) = sockets(&selector);
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let never = Cancellation::never();
    let mut reader = selector.socket_stream_reader(&socket).unwrap();
    {
        let mut reading = pin!(reader.read_owned(vec![0; 16], &never));
        assert!(reading.as_mut().poll(&mut context).is_pending());
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while selector.controllers[0]
        .capacity
        .state
        .load(Ordering::Acquire)
        != 0
    {
        assert!(Instant::now() < deadline);
        worker.turn(Some(Duration::ZERO)).unwrap();
    }
    peer.write_all(b"ready").unwrap();
    let (result, buffer) = drive(
        &mut worker,
        &mut context,
        pin!(reader.read_owned(vec![0; 16].into_boxed_slice(), &never)),
    );
    assert_eq!(result.unwrap(), 5);
    assert_eq!(&buffer[..5], b"ready");
    peer.write_all(b"again").unwrap();
    let (result, buffer) = drive(
        &mut worker,
        &mut context,
        pin!(reader.read_owned(vec![0; 16], &never)),
    );
    assert_eq!(result.unwrap(), 5);
    assert_eq!(&buffer[..5], b"again");
    drive(&mut worker, &mut context, pin!(reader.close())).unwrap();
}

#[test]
fn cancellation_retires_the_original_receive_and_returns_its_buffer() {
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let (socket, mut peer) = sockets(&selector);
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let cancellation = Cancellation::new();
    let mut reader = selector.socket_stream_reader(&socket).unwrap();
    {
        let mut reading = pin!(reader.read_owned(vec![7; 16], &cancellation));
        assert!(reading.as_mut().poll(&mut context).is_pending());
        worker.turn(Some(Duration::ZERO)).unwrap();
        cancellation.cancel();
        let (result, buffer) = drive(&mut worker, &mut context, reading.as_mut());
        assert!(matches!(result, Err(OperationError::Cancelled(_))));
        assert_eq!(buffer, [7; 16]);
    }
    peer.write_all(b"next").unwrap();
    let (result, buffer) = drive(
        &mut worker,
        &mut context,
        pin!(reader.read_owned(vec![0; 16], &Cancellation::never())),
    );
    assert_eq!(result.unwrap(), 4);
    assert_eq!(&buffer[..4], b"next");
    drive(&mut worker, &mut context, pin!(reader.close())).unwrap();
}

#[test]
fn unpublished_receive_is_rejected_when_its_driver_disappears() {
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let (socket, _peer) = sockets(&selector);
    let never = Cancellation::never();
    let mut reader = selector.socket_stream_reader(&socket).unwrap();
    let mut reading = Box::pin(reader.read_owned(vec![7; 16], &never));
    assert!(block_on(poll_once(&mut reading)).is_none());
    drop(driver);
    let (result, buffer) = block_on(reading);
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(buffer, [7; 16]);
    block_on(reader.close()).unwrap();
    block_on(selector.shutdown());
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
// SAFETY: Moving this owner retains the same exclusive Vec allocation and initialized range.
unsafe impl IoBuf for ObservedBuffer {}
// SAFETY: Only this owner exposes mutable access to its initialized range.
unsafe impl IoBufMut for ObservedBuffer {}
impl Drop for ObservedBuffer {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn unknown_retirement_retains_the_buffer_and_panics_instead_of_returning_it() {
    let (selector, mut driver) = IoUringSelector::new(Default::default()).unwrap();
    let (socket, _peer) = sockets(&selector);
    let drops = Arc::new(AtomicUsize::new(0));
    let never = Cancellation::never();
    let mut reader = selector.socket_stream_reader(&socket).unwrap();
    let mut reading = Box::pin(reader.read_owned(
        ObservedBuffer {
            bytes: vec![0; 16],
            drops: Arc::clone(&drops),
        },
        &never,
    ));
    assert!(block_on(poll_once(&mut reading)).is_none());
    driver.turn(Some(Duration::ZERO)).unwrap();
    assert_eq!(driver.reactor.as_ref().unwrap().in_flight.0.len(), 1);
    drop(driver);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(reading))).is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    block_on(selector.shutdown());
}

#[test]
fn forgetting_a_receive_and_dropping_its_reader_still_retains_the_owner_until_completion() {
    let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
    let (socket, mut peer) = sockets(&selector);
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let drops = Arc::new(AtomicUsize::new(0));
    let never = Cancellation::never();
    let mut reader = selector.socket_stream_reader(&socket).unwrap();
    let mut reading = Box::pin(reader.read_owned(
        ObservedBuffer {
            bytes: vec![0; 16],
            drops: Arc::clone(&drops),
        },
        &never,
    ));
    assert!(reading.as_mut().poll(&mut context).is_pending());
    std::mem::forget(reading);
    drop(reader);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    peer.write_all(b"retire").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while drops.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline);
        worker.turn(Some(Duration::ZERO)).unwrap();
    }
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn cancellation_while_waiting_for_capacity_returns_the_unsubmitted_buffer() {
    for cancel in [false, true] {
        let (selector, driver) = super::super::tests::pair(1);
        let (first, mut first_peer) = sockets(&selector);
        let (second, mut second_peer) = sockets(&selector);
        let mut worker = driver.enter();
        let wake = worker.waker();
        let mut context = Context::from_waker(&wake);
        let never = Cancellation::never();
        let cancellation = Cancellation::new();
        let mut first_reader = selector.socket_stream_reader(&first).unwrap();
        let mut second_reader = selector.socket_stream_reader(&second).unwrap();
        {
            let mut first_read = pin!(first_reader.read_owned(vec![0; 16], &never));
            assert!(first_read.as_mut().poll(&mut context).is_pending());
            let mut second_read = pin!(second_reader.read_owned(vec![7; 16], &cancellation));
            assert!(second_read.as_mut().poll(&mut context).is_pending());
            if cancel {
                cancellation.cancel();
            } else {
                first_peer.write_all(b"first").unwrap();
                assert_eq!(
                    drive(&mut worker, &mut context, first_read.as_mut())
                        .0
                        .unwrap(),
                    5
                );
                second_peer.write_all(b"second").unwrap();
            }
            let (result, buffer) = drive(&mut worker, &mut context, second_read.as_mut());
            if cancel {
                assert!(matches!(result, Err(OperationError::Cancelled(_))));
                assert_eq!(buffer, [7; 16]);
            } else {
                assert_eq!(result.unwrap(), 6);
                assert_eq!(&buffer[..6], b"second");
            }
        }
        drive(&mut worker, &mut context, pin!(first_reader.close())).unwrap();
        drive(&mut worker, &mut context, pin!(second_reader.close())).unwrap();
    }
}

#[test]
fn early_failures_return_the_original_owner_without_publishing_a_request() {
    for failure in ["cancelled", "closed", "controller", "identifier"] {
        let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
        let (socket, _peer) = sockets(&selector);
        let mut worker = driver.enter();
        let wake = worker.waker();
        let mut context = Context::from_waker(&wake);
        let cancellation = Cancellation::new();
        let mut reader = selector.socket_stream_reader(&socket).unwrap();
        let expected = match failure {
            "cancelled" => {
                cancellation.cancel();
                io::ErrorKind::Interrupted
            }
            "closed" => {
                selector.close();
                io::ErrorKind::BrokenPipe
            }
            "controller" => {
                selector.controllers[0].close();
                io::ErrorKind::BrokenPipe
            }
            "identifier" => {
                selector.controllers[0]
                    .next_identifier
                    .store(CANCEL_TAG - 2, Ordering::Relaxed);
                io::ErrorKind::Other
            }
            _ => unreachable!("the test enumerates known failure cases"),
        };
        let buffer = vec![7; 16];
        let pointer = buffer.as_ptr();
        let (result, buffer) = drive(
            &mut worker,
            &mut context,
            pin!(reader.read_owned(buffer, &cancellation)),
        );
        assert_eq!(result.unwrap_err().kind(), expected);
        assert_eq!(buffer.as_ptr(), pointer);
        assert_eq!(
            selector.controllers[0]
                .capacity
                .state
                .load(Ordering::Acquire)
                & !CAPACITY_CLOSED,
            0
        );
        drive(&mut worker, &mut context, pin!(reader.close())).unwrap();
    }
}

#[test]
fn cancellation_during_receive_retry_returns_the_original_owner() {
    for error in [io::ErrorKind::Interrupted, io::ErrorKind::WouldBlock] {
        let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
        let (socket, _peer) = sockets(&selector);
        let mut worker = driver.enter();
        let wake = worker.waker();
        let mut context = Context::from_waker(&wake);
        let cancellation = Cancellation::new();
        let mut reader = selector.socket_stream_reader(&socket).unwrap();
        {
            let mut reading = pin!(reader.read_owned(vec![7; 16], &cancellation));
            assert!(reading.as_mut().poll(&mut context).is_pending());
            let Some(Command::Submit(request)) =
                crate::io_uring_worker::pop(&selector.controllers[0])
            else {
                panic!("expected accepted receive");
            };
            request.finish(Err(error.into()));
            if error == io::ErrorKind::WouldBlock {
                // Enter the readiness retry before cancellation to exercise its separate retirement path.
                assert!(reading.as_mut().poll(&mut context).is_pending());
            }
            cancellation.cancel();
            let (result, buffer) = drive(&mut worker, &mut context, reading.as_mut());
            assert!(matches!(result, Err(OperationError::Cancelled(_))));
            assert_eq!(buffer, [7; 16]);
        }
        drive(&mut worker, &mut context, pin!(reader.close())).unwrap();
    }
}

#[test]
fn scalar_dispatch_retains_capacity_waiting_closure_and_cancellation_semantics() {
    let (selector, driver) = super::super::tests::pair(1);
    let (socket, mut peer) = sockets(&selector);
    let mut worker = driver.enter();
    let wake = worker.waker();
    let mut context = Context::from_waker(&wake);
    let never = Cancellation::never();
    {
        peer.write_all(b"a").unwrap();
        let mut first = pin!(selector.socket_read(&socket, vec![0; 16], &never));
        let mut second = pin!(selector.socket_read(&socket, vec![0; 16], &never));
        assert!(first.as_mut().poll(&mut context).is_pending());
        assert!(second.as_mut().poll(&mut context).is_pending());
        assert_eq!(
            drive(&mut worker, &mut context, first.as_mut()).0.unwrap(),
            1
        );
        peer.write_all(b"b").unwrap();
        assert_eq!(
            drive(&mut worker, &mut context, second.as_mut()).0.unwrap(),
            1
        );
    }
    {
        let cancellation = Cancellation::new();
        let mut read = pin!(selector.socket_read(&socket, vec![0; 16], &cancellation));
        assert!(read.as_mut().poll(&mut context).is_pending());
        worker.turn(Some(Duration::ZERO)).unwrap();
        cancellation.cancel();
        assert!(read.as_mut().poll(&mut context).is_pending());
        assert!(matches!(
            drive(&mut worker, &mut context, read.as_mut()).0,
            Err(OperationError::Cancelled(_))
        ));
    }
    selector.controllers[0].close();
    assert_eq!(
        block_on(selector.socket_read(&socket, vec![0; 16], &never))
            .0
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn interrupted_and_would_block_receives_retry_without_losing_the_owner() {
    for error in [io::ErrorKind::Interrupted, io::ErrorKind::WouldBlock] {
        let (selector, driver) = IoUringSelector::new(Default::default()).unwrap();
        let (socket, mut peer) = sockets(&selector);
        let mut worker = driver.enter();
        let wake = worker.waker();
        let mut context = Context::from_waker(&wake);
        let never = Cancellation::never();
        let mut reader = selector.socket_stream_reader(&socket).unwrap();
        {
            let mut reading = pin!(reader.read_owned(vec![7; 16], &never));
            assert!(reading.as_mut().poll(&mut context).is_pending());
            // An original completion can report either retryable OS error. Retire this unsubmitted request directly to make those otherwise kernel-dependent paths deterministic.
            let Some(Command::Submit(request)) =
                crate::io_uring_worker::pop(&selector.controllers[0])
            else {
                panic!("expected accepted receive");
            };
            request.finish(Err(error.into()));
            assert!(reading.as_mut().poll(&mut context).is_pending());
            peer.write_all(b"retry").unwrap();
            let (result, buffer) = drive(&mut worker, &mut context, reading.as_mut());
            assert_eq!(result.unwrap(), 5);
            assert_eq!(&buffer[..5], b"retry");
            assert_eq!(&buffer[5..], [7; 11]);
        }
        drive(&mut worker, &mut context, pin!(reader.close())).unwrap();
    }
}

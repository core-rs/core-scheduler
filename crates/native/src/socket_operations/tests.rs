// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use futures_lite::future::{block_on, poll_once};
use std::{
    sync::{Arc, mpsc},
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(10);
fn scheduler(capacity: usize) -> NativeScheduler {
    NativeScheduler::with_options(crate::NativeOptions {
        workers: 1,

        threads: 1,
        capacity,
        ..Default::default()
    })
    .unwrap()
}
fn socket() -> NativeSocket {
    NativeSocket::new(socket_io::open(SocketDomain::Ipv4, SocketType::Datagram, None).unwrap())
        .unwrap()
}

#[test]
fn socket_cleanup_is_independent_bounded_and_preserves_closed_admission_errors() {
    let scheduler = scheduler(1);
    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let busy = block_on(scheduler.spawn_blocking(
        move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        },
        &Cancellation::never(),
    ))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    block_on(scheduler.socket_close(socket())).unwrap();
    release.send(()).unwrap();
    block_on(busy).unwrap();

    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let busy = block_on(scheduler.cleanup.submit(move || {
        started.send(()).unwrap();
        released.recv_timeout(TIMEOUT).unwrap();
    }))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    let socket = socket();
    let weak = Arc::downgrade(&socket.inner);
    let mut close = Box::pin(scheduler.socket_close(socket));
    assert!(block_on(poll_once(&mut close)).is_none());
    drop(close);
    assert_eq!(weak.strong_count(), 0);
    release.send(()).unwrap();
    block_on(busy).unwrap().unwrap();
    block_on(scheduler.shutdown());
    for submit in [false, true] {
        let socket = super::tests::socket();
        let error = if submit {
            block_on(scheduler.socket_close_submit(socket))
        } else {
            block_on(scheduler.socket_close(socket))
        }
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        let socket = super::tests::socket();
        let error = if submit {
            block_on(scheduler.socket_shutdown_submit(&socket, Shutdown::Both))
        } else {
            block_on(scheduler.socket_shutdown(&socket, Shutdown::Both))
        }
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }
}

#[test]
fn accepted_shutdown_and_close_survive_abandonment_and_drain_during_shutdown() {
    for submit in [false, true] {
        let scheduler = scheduler(2);
        let (client, server) = block_on(core_scheduler_conformance::socket::stream_pair(
            &scheduler,
            SocketDomain::Ipv4,
        ));
        let (started, start) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let busy = block_on(scheduler.cleanup.submit(move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        }))
        .unwrap();
        start.recv_timeout(TIMEOUT).unwrap();
        if submit {
            block_on(scheduler.socket_shutdown_submit(&client, Shutdown::Write)).unwrap();
        } else {
            let mut shutdown = Box::pin(scheduler.socket_shutdown(&client, Shutdown::Write));
            assert!(block_on(poll_once(&mut shutdown)).is_none());
            drop(shutdown);
        }
        let weak = Arc::downgrade(&client.inner);
        drop(client);
        assert_eq!(weak.strong_count(), 1);
        release.send(()).unwrap();
        block_on(busy).unwrap().unwrap();
        block_on(scheduler.cleanup.drain());
        assert_eq!(weak.strong_count(), 0);
        let (result, _) =
            block_on(scheduler.socket_read(&server, vec![0; 1], &Cancellation::never()));
        assert_eq!(result.unwrap(), 0);
        let (started, start) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let busy = block_on(scheduler.cleanup.submit(move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        }))
        .unwrap();
        start.recv_timeout(TIMEOUT).unwrap();
        let mut close = Box::pin(scheduler.socket_close(server));
        assert!(block_on(poll_once(&mut close)).is_none());
        drop(close);
        let mut shutdown = Box::pin(scheduler.shutdown());
        assert!(block_on(poll_once(&mut shutdown)).is_none());
        release.send(()).unwrap();
        block_on(busy).unwrap().unwrap();
        block_on(shutdown);
    }
}

#[test]
fn exclusive_close_rejects_retained_access_and_unpolled_close_drops_normally() {
    let scheduler = scheduler(1);
    let socket = socket();
    let retained = socket.retain();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(
            scheduler.socket_close(socket)
        )))
        .is_err()
    );
    retained
        .get_ref()
        .bind(&"127.0.0.1:0".parse::<SocketAddress>().unwrap().into())
        .unwrap();
    let socket = super::tests::socket();
    let weak = Arc::downgrade(&socket.inner);
    drop(scheduler.socket_close(socket));
    assert_eq!(weak.strong_count(), 0);
    block_on(scheduler.shutdown());
}

#[test]
#[cfg(unix)]
fn vectored_sends_preserve_short_counts_without_implicitly_advancing_the_owner() {
    let scheduler = scheduler(1);
    let (client, server) = block_on(core_scheduler_conformance::socket::stream_pair(
        &scheduler,
        SocketDomain::Ipv4,
    ));
    client.get_ref().set_send_buffer_size(4096).unwrap();
    server.get_ref().set_recv_buffer_size(4096).unwrap();
    block_on(core_scheduler_conformance::socket_messages::partial_send(
        &scheduler,
        &client,
        [vec![1; 2 * 1024 * 1024], vec![2; 2 * 1024 * 1024]],
    ));
    block_on(scheduler.socket_close(client)).unwrap();
    block_on(scheduler.socket_close(server)).unwrap();
    block_on(scheduler.shutdown());
}

#[test]
fn closure_wakes_pending_io_and_preserves_owned_inputs() {
    crate::test_support::run(|| {
        block_on(async {
            let scheduler = scheduler(1);
            let (client, server) =
                core_scheduler_conformance::socket::stream_pair(&scheduler, SocketDomain::Ipv4)
                    .await;
            let never = Cancellation::never();
            let buffer = vec![0; 8];
            let allocation = buffer.as_ptr();
            let mut reading = Box::pin(scheduler.socket_read(&server, buffer, &never));
            assert!(poll_once(&mut reading).await.is_none());
            // Readiness does not occupy the bounded native-work pool.
            assert_eq!(
                scheduler
                    .spawn_blocking(|| 42, &never)
                    .await
                    .unwrap()
                    .await
                    .unwrap(),
                42
            );
            scheduler.close();
            let (result, buffer) = reading.await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(buffer.as_ptr(), allocation);
            assert_eq!(
                scheduler.socket_local_address(&client).unwrap_err().kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                scheduler.socket_peer_address(&server).unwrap_err().kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                scheduler
                    .socket_get_option(&client, TcpNoDelay, &never)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                scheduler
                    .socket_set_option(&client, TcpNoDelay, true, &never)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            assert_eq!(
                scheduler
                    .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
                    .await
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
            scheduler.shutdown().await;
        })
    });
}

#[test]
fn native_cleanup_failure_is_reported_by_each_cleanup_operation() {
    for operation in 0..4 {
        let mut scheduler = scheduler(1);
        let (client, server) = block_on(core_scheduler_conformance::socket::stream_pair(
            &scheduler,
            SocketDomain::Ipv4,
        ));
        scheduler.cleanup = crate::blocking_pool::BlockingPool::new(crate::NativeOptions {
            workers: 1,

            threads: 1,
            capacity: 1,
            ..Default::default()
        });
        scheduler.cleanup.fail_start_at = Some(0);
        let error = match operation {
            0 => block_on(scheduler.socket_close(client)),
            1 => block_on(scheduler.socket_close_submit(client)),
            2 => block_on(scheduler.socket_shutdown(&client, Shutdown::Write)),
            _ => block_on(scheduler.socket_shutdown_submit(&client, Shutdown::Write)),
        }
        .unwrap_err();
        assert_eq!(error.to_string(), "injected thread startup failure");
        drop(server);
        block_on(scheduler.shutdown());
    }
}

#[test]
fn immediate_cancellation_and_closure_prevent_readiness_submission() {
    let scheduler = scheduler(1);
    let cancelled = Cancellation::new();
    cancelled.cancel();
    let never = Cancellation::never();
    assert!(matches!(
        block_on(scheduler.socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &cancelled)),
        Err(OperationError::Cancelled(_))
    ));
    assert!(matches!(
        block_on(scheduler.cancellable(
            &cancelled,
            std::future::poll_fn(|_| -> std::task::Poll<io::Result<()>> {
                panic!("cancelled operation ran")
            })
        )),
        Err(OperationError::Cancelled(_))
    ));
    scheduler.close();
    assert_eq!(
        block_on(scheduler.cancellable(
            &never,
            std::future::poll_fn(|_| -> std::task::Poll<io::Result<()>> {
                panic!("closed operation ran")
            })
        ))
        .unwrap_err()
        .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn invalid_socket_protocol_is_reported() {
    let scheduler = scheduler(1);
    assert!(
        block_on(scheduler.socket_open(
            SocketDomain::Ipv4,
            SocketType::Datagram,
            Some(SocketProtocol::Tcp),
            &Cancellation::never()
        ))
        .is_err()
    );
    block_on(scheduler.shutdown());
}

#[test]
fn prepared_message_io_reuses_vectors_and_payload_without_fresh_allocations() {
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };
    fn wait<T>(future: impl Future<Output = T>) -> T {
        let mut future = pin!(future);
        let started = std::time::Instant::now();
        loop {
            if let Poll::Ready(output) = future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                return output;
            }
            assert!(started.elapsed() < TIMEOUT, "message I/O timed out");
            std::thread::yield_now();
        }
    }
    let scheduler = scheduler(1);
    let (client, server) = block_on(core_scheduler_conformance::socket::stream_pair(
        &scheduler,
        SocketDomain::Ipv4,
    ));
    let token = Cancellation::new();
    let message = SocketMessage {
        buffers: IoBuffers::new([vec![7; 8]]),
        address: None,
    };
    let buffers = IoBuffersMut::new([vec![0; 8]]);
    let sent_pointer = message.buffers.buffer(0).as_ptr();
    let received_pointer = buffers.buffer(0).as_ptr();
    let mut message = Some(message);
    let mut buffers = Some(buffers);
    for _ in 0..16 {
        // Readiness registration belongs to the reactor and can allocate on some platforms (for example kqueue changes in polling). Measure the ready syscall path separately.
        wait(client.io(Interest::Writable, |socket| {
            socket_io::wait(socket, Interest::Writable)
        }))
        .unwrap();
        let allocations = allocation_counter::measure(|| {
            let sent = wait(scheduler.socket_sendmsg(
                &client,
                message.take().unwrap(),
                SocketSendFlags::default(),
                &token,
            ));
            assert_eq!(sent.0.unwrap(), 8);
            message = Some(sent.1);
        });
        assert_eq!(allocations.count_total, 0, "{allocations:?}");
        wait(server.io(Interest::Readable, |socket| {
            socket_io::wait(socket, Interest::Readable)
        }))
        .unwrap();
        let allocations = allocation_counter::measure(|| {
            let received = wait(scheduler.socket_recvmsg(
                &server,
                buffers.take().unwrap(),
                SocketRecvFlags::default(),
                &token,
            ));
            assert_eq!(received.0.unwrap().bytes, 8);
            assert_eq!(received.1.buffer(0), &[7; 8]);
            buffers = Some(received.1);
        });
        assert_eq!(allocations.count_total, 0, "{allocations:?}");
    }
    assert_eq!(message.unwrap().buffers.buffer(0).as_ptr(), sent_pointer);
    assert_eq!(buffers.unwrap().buffer(0).as_ptr(), received_pointer);
    block_on(scheduler.shutdown());
}

#[test]
fn closed_socket_io_returns_the_owner_without_attempting_a_syscall() {
    let scheduler = scheduler(1);
    let socket = socket();
    let input = vec![7; 4];
    let pointer = input.as_ptr();
    scheduler.close();
    let (result, input) = block_on(scheduler.socket_read(&socket, input, &Cancellation::never()));
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(input.as_ptr(), pointer);
    block_on(scheduler.shutdown());
}

#[test]
fn borrowed_operations_reject_closed_schedulers_and_preserve_storage_on_errors() {
    use std::io::{IoSlice, IoSliceMut};
    let scheduler = scheduler(1);
    let socket = socket();
    let never = Cancellation::never();
    let mut target = [0x5a; 4];
    // An unconnected datagram send must report the immediate native error.
    assert!(matches!(
        block_on(scheduler.socket_send_poll(&socket, b"x", SocketSendFlags::default(), &never)),
        Err(OperationError::Io(_))
    ));
    assert!(matches!(
        block_on(scheduler.socket_sendmsg_poll(
            &socket,
            &[IoSlice::new(b"x")],
            None,
            SocketSendFlags::default(),
            &never
        )),
        Err(OperationError::Io(_))
    ));
    let address: SocketAddress = "127.0.0.1:0".parse().unwrap();
    socket.get_ref().bind(&address.into()).unwrap();
    let mut pending = Box::pin(scheduler.socket_recv_poll(
        &socket,
        &mut target,
        SocketRecvFlags::default(),
        &never,
    ));
    assert!(block_on(poll_once(&mut pending)).is_none());
    scheduler.close();
    assert_eq!(
        block_on(pending).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        block_on(scheduler.socket_recv_poll(
            &socket,
            &mut target,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap_err()
        .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        block_on(scheduler.socket_recvmsg_poll(
            &socket,
            &mut [IoSliceMut::new(&mut target)],
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap_err()
        .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        block_on(scheduler.socket_send_poll(&socket, b"x", SocketSendFlags::default(), &never))
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        block_on(scheduler.socket_sendmsg_poll(
            &socket,
            &[IoSlice::new(b"x")],
            None,
            SocketSendFlags::default(),
            &never
        ))
        .unwrap_err()
        .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(target, [0x5a; 4]);
    block_on(scheduler.shutdown());
}

#[cfg(unix)]
#[test]
fn borrowed_sends_preserve_short_counts_and_cancel_when_the_send_queue_is_full() {
    use std::io::IoSlice;
    for message in [false, true] {
        let scheduler = scheduler(1);
        let (client, server) = block_on(core_scheduler_conformance::socket::stream_pair(
            &scheduler,
            SocketDomain::Ipv4,
        ));
        let client_socket = client.get_ref();
        let server_socket = server.get_ref();
        client_socket.set_send_buffer_size(4096).unwrap();
        server_socket.set_recv_buffer_size(4096).unwrap();
        let payload = vec![7; 4 * 1024 * 1024];
        let never = Cancellation::never();
        let count = if message {
            block_on(scheduler.socket_sendmsg_poll(
                &client,
                &[IoSlice::new(&payload[..100]), IoSlice::new(&payload[100..])],
                None,
                SocketSendFlags::default(),
                &never,
            ))
            .unwrap()
        } else {
            block_on(scheduler.socket_send_poll(
                &client,
                &payload,
                SocketSendFlags::default(),
                &never,
            ))
            .unwrap()
        };
        assert!(
            count > 0 && count < payload.len(),
            "fixture must force a short send"
        );
        // Saturate both windows and the sender's queue before polling the borrowed future. The peer deliberately never reads.
        let deadline = std::time::Instant::now() + TIMEOUT;
        let token = Cancellation::new();
        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "send queue did not fill"
            );
            let result = socket_io::send_poll(client_socket, &payload, SocketSendFlags::default());
            if matches!(result, Err(ref error) if error.kind() == io::ErrorKind::WouldBlock) {
                let vectors = [IoSlice::new(&payload)];
                let cancelled = if message {
                    cancel_pending_send(
                        scheduler.socket_sendmsg_poll(
                            &client,
                            &vectors,
                            None,
                            SocketSendFlags::default(),
                            &token,
                        ),
                        &token,
                    )
                } else {
                    cancel_pending_send(
                        scheduler.socket_send_poll(
                            &client,
                            &payload,
                            SocketSendFlags::default(),
                            &token,
                        ),
                        &token,
                    )
                };
                if cancelled {
                    break;
                }
                continue;
            }
            assert!(result.unwrap() > 0);
        }
        assert!(payload.iter().all(|byte| *byte == 7));
        block_on(scheduler.shutdown());
    }
}

#[cfg(unix)]
fn cancel_pending_send(
    future: impl std::future::Future<Output = OperationResult<usize>>,
    token: &Cancellation,
) -> bool {
    let mut sending = std::pin::pin!(future);
    if let Some(result) = block_on(poll_once(&mut sending)) {
        assert!(result.unwrap() > 0);
        false
    } else {
        token.cancel();
        assert!(matches!(
            block_on(sending),
            Err(OperationError::Cancelled(_))
        ));
        true
    }
}

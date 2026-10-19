// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use futures_lite::future::{block_on, poll_once};
use std::{
    sync::{Arc, mpsc},
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(10);
fn scheduler(capacity: usize) -> DirectScheduler {
    DirectScheduler::new(crate::DirectOptions {
        threads: 1,
        capacity,
    })
    .unwrap()
}
fn socket() -> DirectSocket {
    DirectSocket::new(socket_io::open(SocketDomain::Ipv4, SocketType::Datagram, None).unwrap())
}

#[test]
fn pending_socket_attempts_release_capacity_and_observe_cancellation_and_scheduler_close() {
    let scheduler = scheduler(1);
    let socket = socket();
    let token = Cancellation::new();
    let (started, start) = mpsc::channel();
    let buffer = vec![1; 4];
    let allocation = buffer.as_ptr();
    let mut pending = Box::pin(
        scheduler.socket_operation(&socket, buffer, &token, move |_, _| {
            let _ = started.send(());
            Err::<usize, _>(io::ErrorKind::WouldBlock.into())
        }),
    );
    assert!(block_on(poll_once(&mut pending)).is_none());
    start.recv_timeout(TIMEOUT).unwrap();
    block_on(scheduler.work.drain());
    assert!(block_on(poll_once(&mut pending)).is_none());
    // The pending socket occupies neither the sole worker nor the sole admission slot.
    let work = block_on(scheduler.spawn_blocking(|| 42, &Cancellation::never())).unwrap();
    assert_eq!(block_on(work).unwrap(), 42);
    token.cancel();
    let (result, returned) = block_on(pending);
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
    assert_eq!(returned.as_ptr(), allocation);
    // Result delivery precedes worker-slot retirement. Start the next isolated assertion with admission available.
    block_on(scheduler.work.drain());

    let never = Cancellation::never();
    let (started, start) = mpsc::channel();
    let mut pending =
        Box::pin(
            scheduler.socket_operation(&socket, vec![2], &never, move |_, _| {
                let _ = started.send(());
                Err::<usize, _>(io::ErrorKind::WouldBlock.into())
            }),
        );
    assert!(block_on(poll_once(&mut pending)).is_none());
    start.recv_timeout(TIMEOUT).unwrap();
    block_on(scheduler.work.drain());
    assert!(block_on(poll_once(&mut pending)).is_none());
    block_on(scheduler.shutdown());
    let (result, returned) = block_on(pending);
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(returned, vec![2]);
}

#[test]
fn interrupted_attempts_retry_and_retry_service_errors_return_input() {
    let mut scheduler = scheduler(1);
    let socket = socket();
    let mut calls = 0;
    let (result, buffer) = block_on(scheduler.socket_operation(
        &socket,
        vec![3],
        &Cancellation::never(),
        move |_, _| {
            calls += 1;
            if calls == 1 {
                Err(io::ErrorKind::Interrupted.into())
            } else {
                Ok(11)
            }
        },
    ));
    assert_eq!(result.unwrap(), 11);
    assert_eq!(buffer, vec![3]);
    // A fresh scheduler has not initialized its retry worker yet.
    block_on(scheduler.shutdown());
    scheduler = super::tests::scheduler(1);
    scheduler.socket_retry.fail_start = true;
    let (result, buffer) = block_on(scheduler.socket_operation(
        &socket,
        vec![5],
        &Cancellation::never(),
        |_, _| Err::<usize, _>(io::ErrorKind::WouldBlock.into()),
    ));
    assert_eq!(
        result.unwrap_err().to_string(),
        "injected socket retry startup failure"
    );
    assert_eq!(buffer, vec![5]);
    block_on(scheduler.shutdown());
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
    let weak = Arc::downgrade(&socket.0);
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
        let weak = Arc::downgrade(&client.0);
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
        .bind(&"127.0.0.1:0".parse::<SocketAddress>().unwrap().into())
        .unwrap();
    let socket = super::tests::socket();
    let weak = Arc::downgrade(&socket.0);
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
    client.0.set_send_buffer_size(4096).unwrap();
    server.0.set_recv_buffer_size(4096).unwrap();
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
fn borrowed_operations_reject_closed_schedulers_and_preserve_storage_on_errors() {
    use std::io::{IoSlice, IoSliceMut};
    let scheduler = scheduler(1);
    let socket = socket();
    let address: SocketAddress = "127.0.0.1:0".parse().unwrap();
    socket.0.bind(&address.into()).unwrap();
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
        let client_socket = &client.0;
        let server_socket = &server.0;
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
        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "send queue did not fill"
            );
            let result = socket_io::send_poll(client_socket, &payload, SocketSendFlags::default());
            if matches!(result, Err(ref error) if error.kind() == io::ErrorKind::WouldBlock) {
                let token = Cancellation::new();
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
        match block_on(sending) {
            Err(OperationError::Cancelled(_)) => true,
            result => {
                // TCP can free sender capacity between polls without an application read. Progress wins cancellation; keep filling until cancellation is observed.
                assert!(result.unwrap() > 0);
                false
            }
        }
    }
}

#[test]
fn borrowed_calls_make_progress_without_a_blocking_pool_slot() {
    let scheduler = scheduler(1);
    let (client, server) = block_on(core_scheduler_conformance::socket::stream_pair(
        &scheduler,
        SocketDomain::Ipv4,
    ));
    let (started, start) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let never = Cancellation::never();
    let busy = block_on(scheduler.spawn_blocking(
        move || {
            started.send(()).unwrap();
            released.recv_timeout(TIMEOUT).unwrap();
        },
        &never,
    ))
    .unwrap();
    start.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(
        block_on(scheduler.socket_send_poll(&client, b"x", SocketSendFlags::default(), &never))
            .unwrap(),
        1
    );
    let mut target = [0; 1];
    assert_eq!(
        block_on(scheduler.socket_recv_poll(
            &server,
            &mut target,
            SocketRecvFlags::default(),
            &never
        ))
        .unwrap(),
        1
    );
    assert_eq!(target, *b"x");
    release.send(()).unwrap();
    block_on(busy).unwrap();
    block_on(scheduler.shutdown());
}

#[test]
fn borrowed_retry_handles_interruption_startup_errors_and_successful_cancellation_races() {
    let scheduler = scheduler(1);
    let socket = socket();
    let never = Cancellation::never();
    let mut calls = 0;
    let count = block_on(scheduler.socket_poll_operation(&socket, &never, |_| {
        calls += 1;
        if calls == 1 {
            Err(io::ErrorKind::Interrupted.into())
        } else {
            Ok(7)
        }
    }))
    .unwrap();
    assert_eq!(count, 7);
    assert_eq!(calls, 2);
    let token = Cancellation::new();
    assert_eq!(
        block_on(scheduler.socket_poll_operation(&socket, &token, |_| {
            token.cancel();
            Ok(11)
        }))
        .unwrap(),
        11
    );
    block_on(scheduler.shutdown());
    let mut scheduler = self::scheduler(1);
    scheduler.socket_retry.fail_start = true;
    assert_eq!(
        block_on(
            scheduler.socket_poll_operation::<()>(&socket, &never, |_| Err(
                io::ErrorKind::WouldBlock.into()
            ))
        )
        .unwrap_err()
        .to_string(),
        "injected socket retry startup failure"
    );
    block_on(scheduler.shutdown());
}

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use std::time::Duration;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn socket() -> socket2::Socket {
    socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap()
}

#[test]
fn explicit_close_rejects_retained_socket_ownership() {
    let runtime = runtime();
    let descriptor = TokioSocket::new(runtime.handle(), socket()).unwrap();
    let retained = descriptor.retain();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| descriptor.into_close()));
    assert!(result.is_err());
    assert!(retained.get_ref().set_nonblocking(true).is_ok());
    retained.into_close()().unwrap();
}

#[test]
fn nonblocking_configuration_errors_are_returned() {
    let runtime = runtime();
    let error = TokioSocket::new_with(
        runtime.handle(),
        socket(),
        |_| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
        |_, _| unreachable!("socket registration follows nonblocking configuration"),
    )
    .err()
    .unwrap();

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
}

#[test]
fn registration_errors_are_returned() {
    let runtime = runtime();
    let error = TokioSocket::new_with(
        runtime.handle(),
        socket(),
        |_| Ok(()),
        |_, _| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
    )
    .err()
    .unwrap();

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
}

#[test]
fn invalid_accept_returns_without_waiting_for_readiness() {
    let runtime = runtime();
    let socket = runtime.block_on(async { TokioSocket::new(runtime.handle(), socket()).unwrap() });

    let result = runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(1),
            socket.io(Interest::Readable, |socket| socket.accept().map(|_| ())),
        )
        .await
    });

    assert!(
        result
            .expect("invalid accept waited for readiness")
            .is_err()
    );
}
#[test]
fn transient_would_block_retries_with_the_requested_readiness_direction() {
    use core_scheduler::{Cancellation, SocketDomain};
    crate::test_support::run(|| {
        futures_lite::future::block_on(async {
            let scheduler = crate::test_support::scheduler();
            let (client, server) =
                core_scheduler_conformance::socket::stream_pair(&scheduler, SocketDomain::Ipv4)
                    .await;
            // Model a socket becoming ready between the initial syscall and readiness registration. Both reactor directions must retry the actual syscall.
            let mut writes = 0;
            let sent = client
                .io(Interest::Writable, |socket| {
                    writes += 1;
                    if writes == 1 {
                        return Err(io::ErrorKind::WouldBlock.into());
                    }
                    crate::socket_io::send(socket, b"x")
                })
                .await
                .unwrap();
            assert_eq!(sent, 1);
            assert!(writes >= 2);
            let mut reads = 0;
            let mut buffer = [0];
            let received = server
                .io(Interest::Readable, |socket| {
                    reads += 1;
                    if reads == 1 {
                        return Err(io::ErrorKind::WouldBlock.into());
                    }
                    crate::socket_io::recv(socket, &mut buffer)
                })
                .await
                .unwrap();
            assert_eq!(received, 1);
            assert_eq!(&buffer, b"x");
            assert!(reads >= 2);
            // Confirm normal public operations still use the same registration.
            use core_scheduler::{SocketClose, SocketWait};
            scheduler
                .socket_wait(&client, Interest::Writable, &Cancellation::never())
                .await
                .unwrap();
            scheduler.socket_close(client).await.unwrap();
            scheduler.socket_close(server).await.unwrap();
            scheduler.shutdown().await;
        })
    });
}

#[cfg(unix)]
#[test]
fn registration_is_deferred_until_waiting_and_reused_after_cancellation() {
    use core_scheduler::*;
    use futures_lite::future::{block_on, poll_once};
    use std::io::Write;
    crate::test_support::run(|| {
        block_on(async {
            let scheduler = crate::test_support::scheduler();
            let never = Cancellation::never();
            let socket = scheduler
                .socket_open(SocketDomain::Ipv4, SocketType::Stream, None, &never)
                .await
                .unwrap();
            assert!(socket.inner.registration.get().is_none());
            assert_eq!(Arc::strong_count(&socket.inner.socket), 1);
            // Linux reports hangup on an unconnected stream. Observe native readiness without letting Tokio cache those permanent closed bits.
            #[cfg(target_os = "linux")]
            for interest in [Interest::Readable, Interest::Writable] {
                scheduler
                    .socket_wait(&socket, interest, &never)
                    .await
                    .unwrap();
                assert!(socket.inner.registration.get().is_none());
            }
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            scheduler
                .socket_connect(&socket, listener.local_addr().unwrap().into(), &never)
                .await
                .unwrap();
            let (mut peer, _) = listener.accept().unwrap();
            let token = Cancellation::new();
            let mut target = [0x5a];
            let mut receiving = Box::pin(scheduler.socket_recv_poll(
                &socket,
                &mut target,
                SocketRecvFlags::default(),
                &token,
            ));
            assert!(poll_once(&mut receiving).await.is_none());
            let registration = socket.inner.registration.get().unwrap() as *const _;
            assert_eq!(Arc::strong_count(&socket.inner.socket), 2);
            token.cancel();
            assert!(matches!(receiving.await, Err(OperationError::Cancelled(_))));
            assert_eq!(target, [0x5a]);
            peer.write_all(b"x").unwrap();
            assert_eq!(
                scheduler
                    .socket_recv_poll(&socket, &mut target, SocketRecvFlags::default(), &never)
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(target, *b"x");
            assert_eq!(
                socket.inner.registration.get().unwrap() as *const _,
                registration
            );
            let weak = Arc::downgrade(&socket.inner.socket);
            scheduler.socket_close(socket).await.unwrap();
            assert_eq!(weak.strong_count(), 0);
            scheduler.shutdown().await;
        });
    });
}

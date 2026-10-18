// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
#[test]
fn failed_registration_returns_the_original_error() {
    let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
    let error = NativeSocket::new_with(socket, |_| Err(io::ErrorKind::PermissionDenied.into()))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
}

#[test]
fn transient_would_block_retries_through_writable_readiness() {
    crate::test_support::run(|| {
        let socket = NativeSocket::new(
            socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::DGRAM, None).unwrap(),
        )
        .unwrap();
        socket
            .get_ref()
            .bind(
                &"127.0.0.1:0"
                    .parse::<std::net::SocketAddr>()
                    .unwrap()
                    .into(),
            )
            .unwrap();
        let mut attempts = 0;
        let result = futures_lite::future::block_on(socket.io(Interest::Writable, |socket| {
            attempts += 1;
            if attempts <= 2 {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            socket.local_addr()
        }))
        .unwrap();
        assert!(attempts >= 3);
        assert_eq!(result.as_socket().unwrap(), socket.local_addr().unwrap());
    });
}

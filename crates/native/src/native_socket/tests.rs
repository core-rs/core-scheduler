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

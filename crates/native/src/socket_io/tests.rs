// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use core_scheduler::{IoBuffers, IoBuffersMut};
use std::net::TcpListener;
use std::time::{Duration, Instant};

thread_local! {
    pub(super) static WAIT_ERROR: std::cell::Cell<Option<io::ErrorKind>> = const { std::cell::Cell::new(None) };
}

fn retry<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match operation() {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "native socket operation did not progress"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            result => return result,
        }
    }
}

#[test]
fn native_parameters_addresses_and_errors_are_preserved() {
    for (kind, protocol) in [
        (SocketType::Raw, Some(SocketProtocol::Other(-1))),
        (SocketType::Other(-1), None),
    ] {
        assert!(open(SocketDomain::Ipv4, kind, protocol).is_err());
    }
    let address = "127.0.0.1:1234".parse::<SocketAddr>().unwrap();
    assert_eq!(super::address(address.into()), address);
    assert!(result_count(-1).is_err());
    assert!(wait_result(-1, 0).is_err());
    assert_eq!(
        wait_result(0, 0).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    wait_result(1, 1).unwrap();
    assert!(connect_pending(&io::Error::from(io::ErrorKind::WouldBlock)));
    assert!(!connect_pending(&io::Error::from(
        io::ErrorKind::PermissionDenied
    )));
    assert_eq!(
        connected(
            Err(io::ErrorKind::PermissionDenied.into()),
            || unreachable!("peer lookup follows SO_ERROR")
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        connected(
            Ok(Some(io::ErrorKind::ConnectionRefused.into())),
            || unreachable!("peer lookup follows SO_ERROR")
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::ConnectionRefused
    );
    assert_eq!(
        connected(Ok(None), || Err(io::ErrorKind::NotConnected.into()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        connected(Ok(None), || Err(io::ErrorKind::PermissionDenied.into()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    connected(Ok(None), || Ok(address.into())).unwrap();
}

#[test]
fn native_connect_preserves_immediate_and_deferred_results() {
    let socket = open(SocketDomain::Ipv4, SocketType::Datagram, None).unwrap();
    connect(&socket, "127.0.0.1:9".parse().unwrap(), &mut false).unwrap();
    connect(&socket, "127.0.0.1:9".parse().unwrap(), &mut true).unwrap();
    WAIT_ERROR.with(|slot| slot.set(Some(io::ErrorKind::WouldBlock)));
    assert_eq!(
        connect(&socket, "127.0.0.1:9".parse().unwrap(), &mut true)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    let socket = open(SocketDomain::Ipv4, SocketType::Stream, None).unwrap();
    assert!(connect(&socket, "[::1]:9".parse().unwrap(), &mut false).is_err());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let refused = listener.local_addr().unwrap();
    drop(listener);
    let socket = open(SocketDomain::Ipv4, SocketType::Stream, None).unwrap();
    let mut started = false;
    assert!(retry(|| connect(&socket, refused, &mut started)).is_err());
}

#[test]
fn message_validation_rejects_oversized_counts_lengths_and_overflow() {
    for (count, lengths) in [
        (i32::MAX as usize + 1, vec![]),
        (1, vec![i32::MAX as usize + 1]),
        (2, vec![usize::MAX, 1]),
    ] {
        assert_eq!(
            validate_lengths(count, lengths.into_iter())
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    validate_lengths(0, [].into_iter()).unwrap();
    validate_lengths(1, [i32::MAX as usize].into_iter()).unwrap();
}

struct EmptyBuffers(usize);
// SAFETY: This virtual collection exposes stable, empty byte ranges without any payload storage. Its count is immutable while owned by the backend.
unsafe impl IoBufSequence for EmptyBuffers {
    fn buffer_count(&self) -> usize {
        self.0
    }
    fn buffer(&self, _: usize) -> &[u8] {
        &[]
    }
}
// SAFETY: Empty ranges contain no bytes that can overlap or be concurrently accessed.
unsafe impl IoBufSequenceMut for EmptyBuffers {
    fn buffer_mut(&mut self, _: usize) -> &mut [u8] {
        &mut []
    }
}

#[test]
fn invalid_vector_counts_fail_before_iterating_or_performing_native_io() {
    let socket = open(SocketDomain::Ipv4, SocketType::Datagram, None).unwrap();
    let mut buffers = EmptyBuffers(i32::MAX as usize + 1);
    let mut vectors = IoVectors::new();
    assert_eq!(
        vectors
            .send(&socket, &buffers, None, SocketSendFlags::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        vectors
            .recv(&socket, &mut buffers, SocketRecvFlags::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    // Native WouldBlock also propagates without changing the caller's initialized ranges.
    socket
        .bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into())
        .unwrap();
    let mut target = [vec![0x5a; 2]];
    assert_eq!(
        vectors
            .recv(&socket, &mut target, SocketRecvFlags::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(target, [vec![0x5a; 2]]);
    assert!(
        vectors
            .send(&socket, &[vec![1]], None, SocketSendFlags::default())
            .is_err()
    );
}

#[test]
fn prepared_messages_bypass_scratch_and_native_attempts_need_no_vector_allocation() {
    let sender = open(SocketDomain::Ipv4, SocketType::Datagram, None).unwrap();
    let receiver = open(SocketDomain::Ipv4, SocketType::Datagram, None).unwrap();
    receiver
        .bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into())
        .unwrap();
    let destination = super::address(receiver.local_addr().unwrap());
    let source = IoBuffers::new([b"a".to_vec(), b"bc".to_vec()]);
    let mut target = IoBuffersMut::new([vec![0; 1], vec![0; 2]]);
    let mut vectors = IoVectors::new();
    let prepared = source.io_slices().unwrap().as_ptr();
    assert_eq!(
        vectors
            .sending(&source)
            .unwrap()
            .0
            .cast::<std::io::IoSlice<'_>>(),
        prepared
    );
    let prepared = target.io_slices_mut().unwrap().as_ptr() as usize;
    assert_eq!(vectors.receiving(&mut target).unwrap().0 as usize, prepared);
    let allocations = allocation_counter::measure(|| {
        assert_eq!(
            vectors
                .send(
                    &sender,
                    &source,
                    Some(destination),
                    SocketSendFlags::default()
                )
                .unwrap(),
            3
        );
        let result =
            retry(|| vectors.recv(&receiver, &mut target, SocketRecvFlags::default())).unwrap();
        assert_eq!(result.bytes, 3);
    });
    assert_eq!(
        allocations.count_total, 0,
        "prepared native message attempts allocated"
    );
    assert!(vectors.scratch.is_empty());
    assert_eq!(target.into_inner(), [b"a".to_vec(), b"bc".to_vec()]);

    let source = [b"x".to_vec(), b"yz".to_vec()];
    let mut target = [vec![0; 1], vec![0; 2]];
    // Warm fallback capacity, then verify it survives rebuilding pointers.
    vectors.sending(&source).unwrap();
    vectors.receiving(&mut target).unwrap();
    let allocations = allocation_counter::measure(|| {
        vectors.sending(&source).unwrap();
        vectors.receiving(&mut target).unwrap();
    });
    assert_eq!(allocations.count_total, 0);
}

#[test]
fn zero_capacity_message_receives_use_a_real_native_vector_and_report_os_results() {
    let sender = open(SocketDomain::Ipv4, SocketType::Datagram, None).unwrap();
    let receiver = open(SocketDomain::Ipv4, SocketType::Datagram, None).unwrap();
    receiver
        .bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into())
        .unwrap();
    let destination = super::address(receiver.local_addr().unwrap());
    let mut vectors = IoVectors::new();
    vectors
        .send(
            &sender,
            &Vec::<Vec<u8>>::new(),
            Some(destination),
            SocketSendFlags::default(),
        )
        .unwrap();
    // Zero-capacity datagram receive consumption is platform-specific. Its count must still be zero.
    let result = retry(|| {
        vectors.recv(
            &receiver,
            &mut Vec::<Vec<u8>>::new(),
            SocketRecvFlags::default(),
        )
    })
    .unwrap();
    assert_eq!(result.bytes, 0);
}

#[cfg(target_os = "linux")]
#[test]
fn missing_control_storage_reports_discarded_packet_metadata() {
    let sender = open(SocketDomain::Ipv6, SocketType::Datagram, None).unwrap();
    let receiver = open(SocketDomain::Ipv6, SocketType::Datagram, None).unwrap();
    let enabled: libc::c_int = 1;
    // SAFETY: The live IP socket reads one initialized option value synchronously and does not retain its address.
    assert_eq!(
        unsafe {
            libc::setsockopt(
                receiver.as_raw_fd(),
                libc::IPPROTO_IPV6,
                libc::IPV6_RECVPKTINFO,
                (&enabled as *const libc::c_int).cast(),
                std::mem::size_of_val(&enabled) as _,
            )
        },
        0
    );
    receiver
        .bind(&"[::1]:0".parse::<SocketAddr>().unwrap().into())
        .unwrap();
    sender
        .send_to(b"x", &receiver.local_addr().unwrap())
        .unwrap();
    let result =
        retry(|| IoVectors::new().recv(&receiver, &mut [vec![0; 4]], SocketRecvFlags::default()))
            .unwrap();
    assert_eq!(result.bytes, 1);
    assert!(result.control_truncated);
    assert!(!result.truncated);
}

#[cfg(windows)]
#[test]
fn message_receive_preserves_shutdown_errors() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let socket =
        Socket::from(std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap());
    let (_peer, _) = listener.accept().unwrap();
    socket.set_nonblocking(true).unwrap();
    socket.shutdown(std::net::Shutdown::Read).unwrap();
    let error = IoVectors::new()
        .recv(&socket, &mut [vec![0; 1]], SocketRecvFlags::default())
        .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(WSAESHUTDOWN));
}

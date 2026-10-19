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

#[test]
fn borrowed_native_io_enforces_nonblocking_without_allocating_vectors() {
    let sender = open(SocketDomain::Ipv4, SocketType::Datagram, None).unwrap();
    let receiver = open(SocketDomain::Ipv4, SocketType::Datagram, None).unwrap();
    receiver
        .bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into())
        .unwrap();
    let destination = super::address(receiver.local_addr().unwrap());
    sender.connect(&destination.into()).unwrap();
    // Exercise initially blocking sockets. Per-call flags suffice on Unix; Windows must enable FIONBIO before attempting I/O.
    sender.set_nonblocking(false).unwrap();
    receiver.set_nonblocking(false).unwrap();
    let mut target = [0x5a; 4];
    assert_eq!(
        recv_poll(&receiver, &mut target, SocketRecvFlags::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    receiver.set_nonblocking(false).unwrap();
    assert_eq!(
        recvmsg_poll(
            &receiver,
            &mut [IoSliceMut::new(&mut target)],
            SocketRecvFlags::default()
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::WouldBlock
    );
    let source = *b"abc";
    let allocations = allocation_counter::measure(|| {
        assert_eq!(
            send_poll(&sender, &source, SocketSendFlags { dont_route: true }).unwrap(),
            3
        );
        let count =
            retry(|| recv_poll(&receiver, &mut target, SocketRecvFlags { peek: true })).unwrap();
        assert_eq!(count, 3);
        assert_eq!(target, [b'a', b'b', b'c', 0x5a]);
        let mut first = [0; 1];
        let mut second = [0x5a; 3];
        let result = retry(|| {
            recvmsg_poll(
                &receiver,
                &mut [IoSliceMut::new(&mut first), IoSliceMut::new(&mut second)],
                SocketRecvFlags::default(),
            )
        })
        .unwrap();
        assert_eq!(result.bytes, 3);
        assert_eq!(first, *b"a");
        assert_eq!(second, [b'b', b'c', 0x5a]);
        assert_eq!(
            sendmsg_poll(
                &sender,
                &[IoSlice::new(&source[..1]), IoSlice::new(&source[1..])],
                None,
                SocketSendFlags::default()
            )
            .unwrap(),
            3
        );
        assert_eq!(
            retry(|| recv_poll(&receiver, &mut target, SocketRecvFlags::default())).unwrap(),
            3
        );
    });
    assert_eq!(
        allocations.count_total, 0,
        "borrowed native calls allocated"
    );
    assert_eq!(source, *b"abc");
}

#[cfg(windows)]
thread_local! {
    pub(super) static NONBLOCKING_ERROR: std::cell::Cell<Option<io::ErrorKind>> = const { std::cell::Cell::new(None) };
}

#[cfg(windows)]
#[test]
fn every_borrowed_attempt_propagates_nonblocking_setup_failure_before_io() {
    let sender = open(SocketDomain::Ipv4, SocketType::Datagram, None).unwrap();
    let receiver = open(SocketDomain::Ipv4, SocketType::Datagram, None).unwrap();
    receiver
        .bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into())
        .unwrap();
    let address = super::address(receiver.local_addr().unwrap());
    sender.connect(&address.into()).unwrap();
    let mut target = [0x5a; 1];
    for operation in 0..4 {
        NONBLOCKING_ERROR.with(|slot| slot.set(Some(io::ErrorKind::PermissionDenied)));
        let error = match operation {
            0 => send_poll(&sender, b"x", SocketSendFlags::default()).unwrap_err(),
            1 => sendmsg_poll(
                &sender,
                &[IoSlice::new(b"x")],
                None,
                SocketSendFlags::default(),
            )
            .unwrap_err(),
            2 => recv_poll(&receiver, &mut target, SocketRecvFlags::default()).unwrap_err(),
            _ => recvmsg_poll(
                &receiver,
                &mut [IoSliceMut::new(&mut target)],
                SocketRecvFlags::default(),
            )
            .unwrap_err(),
        };
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(target, [0x5a]);
    }
    assert_eq!(
        recv_poll(&receiver, &mut target, SocketRecvFlags::default())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
}

// A zero-initialized virtual mapping exercises real slice lengths beyond the portable native limit without touching or allocating physical pages for the payload.
struct LargeBuffer {
    pointer: *mut u8,
    length: usize,
}
impl LargeBuffer {
    fn new() -> Self {
        let length = i32::MAX as usize + 1;
        #[cfg(unix)]
        // SAFETY: Requests a fresh, private, readable/writable anonymous mapping, initialized to zero by the OS. No pages are touched by this test.
        let pointer = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                length,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                -1,
                0,
            )
        };
        #[cfg(unix)]
        assert_ne!(pointer, libc::MAP_FAILED);
        #[cfg(windows)]
        // SAFETY: A fresh committed reservation supplies readable/writable zero-initialized pages on demand. Physical pages are not allocated until accessed.
        let pointer = unsafe {
            windows_sys::Win32::System::Memory::VirtualAlloc(
                std::ptr::null(),
                length,
                windows_sys::Win32::System::Memory::MEM_RESERVE
                    | windows_sys::Win32::System::Memory::MEM_COMMIT,
                windows_sys::Win32::System::Memory::PAGE_READWRITE,
            )
        };
        assert!(!pointer.is_null());
        Self {
            pointer: pointer.cast(),
            length,
        }
    }
    fn bytes(&mut self) -> &mut [u8] {
        // SAFETY: This exclusive owner retains the entire zero-initialized, readable/writable mapping. Its length fits isize on our supported 64-bit targets.
        unsafe { std::slice::from_raw_parts_mut(self.pointer, self.length) }
    }
}
impl Drop for LargeBuffer {
    fn drop(&mut self) {
        #[cfg(unix)]
        // SAFETY: This is exactly the still-owned live mapping and its original length; no borrowed slices remain.
        assert_eq!(unsafe { libc::munmap(self.pointer.cast(), self.length) }, 0);
        #[cfg(windows)]
        // SAFETY: Release the original reservation, with no borrowed slices remaining.
        assert_ne!(
            unsafe {
                windows_sys::Win32::System::Memory::VirtualFree(
                    self.pointer.cast(),
                    0,
                    windows_sys::Win32::System::Memory::MEM_RELEASE,
                )
            },
            0
        );
    }
}

#[test]
fn borrowed_message_lengths_are_validated_before_native_access() {
    let socket = open(SocketDomain::Ipv4, SocketType::Datagram, None).unwrap();
    let mut mapping = LargeBuffer::new();
    assert_eq!(
        sendmsg_poll(
            &socket,
            &[IoSlice::new(mapping.bytes())],
            None,
            SocketSendFlags::default()
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        recvmsg_poll(
            &socket,
            &mut [IoSliceMut::new(mapping.bytes())],
            SocketRecvFlags::default()
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
}

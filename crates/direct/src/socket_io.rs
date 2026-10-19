// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Synchronous, nonblocking IP syscalls. No native pointer escapes a call.

use core_scheduler::{
    Interest, IoBufSequence, IoBufSequenceMut, SocketDomain, SocketProtocol, SocketRecvFlags,
    SocketRecvResult, SocketSendFlags, SocketType,
};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
#[cfg(windows)]
use std::ptr;
use std::{
    io::{self, IoSlice, IoSliceMut},
    net::SocketAddr,
};

#[cfg(unix)]
use std::os::fd::{AsRawFd, IntoRawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, IntoRawSocket};
#[cfg(windows)]
use windows_sys::Win32::Networking::WinSock::*;

pub(crate) fn open(
    domain: SocketDomain,
    kind: SocketType,
    protocol: Option<SocketProtocol>,
) -> io::Result<Socket> {
    let domain = match domain {
        SocketDomain::Ipv4 => Domain::IPV4,
        SocketDomain::Ipv6 => Domain::IPV6,
    };
    let kind = match kind {
        SocketType::Stream => Type::STREAM,
        SocketType::Datagram => Type::DGRAM,
        SocketType::Raw => Type::RAW,
        SocketType::Other(value) => Type::from(value),
    };
    let protocol = protocol.map(|protocol| match protocol {
        SocketProtocol::Tcp => Protocol::TCP,
        SocketProtocol::Udp => Protocol::UDP,
        SocketProtocol::Other(value) => Protocol::from(value),
    });
    Socket::new(domain, kind, protocol).and_then(nonblocking)
}

pub(crate) fn nonblocking(socket: Socket) -> io::Result<Socket> {
    socket.set_nonblocking(true).map(|()| socket)
}

pub(crate) fn address(address: SockAddr) -> SocketAddr {
    address.as_socket().expect("direct only creates IP sockets")
}

pub(crate) fn close(socket: Socket) -> io::Result<()> {
    #[cfg(unix)]
    // SAFETY: Exclusive ownership was established before cleanup admission. Transfer the descriptor to exactly one close attempt, including on EINTR.
    let result = unsafe { libc::close(socket.into_raw_fd()) };
    #[cfg(windows)]
    // SAFETY: This backend exposes no linger option or native aliases. The nonblocking socket is exclusively owned and transferred to exactly one closesocket call.
    let result = unsafe { closesocket(socket.into_raw_socket() as _) };
    result_count(result as isize).map(drop)
}

pub(crate) fn recv(socket: &Socket, bytes: &mut [u8]) -> io::Result<usize> {
    // SAFETY: The exclusive initialized byte range lives through this synchronous call. Socket::recv only writes initialized bytes and never extends the range.
    let bytes = unsafe { std::slice::from_raw_parts_mut(bytes.as_mut_ptr().cast(), bytes.len()) };
    socket.recv(bytes)
}

pub(crate) fn send(socket: &Socket, bytes: &[u8]) -> io::Result<usize> {
    socket.send_with_flags(bytes, send_flags(SocketSendFlags::default()))
}

pub(crate) fn connect_pending(error: &io::Error) -> bool {
    #[cfg(unix)]
    let pending = error.raw_os_error() == Some(libc::EINPROGRESS);
    #[cfg(windows)]
    let pending = error.raw_os_error() == Some(WSAEWOULDBLOCK);
    pending || error.kind() == io::ErrorKind::WouldBlock
}

pub(crate) fn connect(socket: &Socket, address: SocketAddr, started: &mut bool) -> io::Result<()> {
    if !*started {
        *started = true;
        match socket.connect(&address.into()) {
            Err(error) if connect_pending(&error) => (),
            result => return result,
        }
    }
    wait(socket, Interest::Writable)?;
    connected(socket.take_error(), || socket.peer_addr())
}

fn connected(
    error: io::Result<Option<io::Error>>,
    peer: impl FnOnce() -> io::Result<SockAddr>,
) -> io::Result<()> {
    if let Some(error) = error? {
        return Err(error);
    }
    // Readiness can be spurious; a successful SO_ERROR check alone does not establish connection.
    match peer() {
        Err(error) if error.kind() == io::ErrorKind::NotConnected => {
            Err(io::ErrorKind::WouldBlock.into())
        }
        result => result.map(drop),
    }
}

pub(crate) fn wait(socket: &Socket, interest: Interest) -> io::Result<()> {
    // A per-thread, one-shot test shim makes connect readiness failures deterministic without depending on network timing.
    #[cfg(test)]
    if let Some(error) = tests::WAIT_ERROR.with(|slot| slot.take()) {
        return Err(error.into());
    }
    #[cfg(unix)]
    let (result, events) = {
        let mut descriptor = libc::pollfd {
            fd: socket.as_raw_fd(),
            events: match interest {
                Interest::Readable => libc::POLLIN,
                Interest::Writable => libc::POLLOUT,
            },
            revents: 0,
        };
        // SAFETY: One initialized pollfd, retained through a zero-timeout poll; no pointer is retained.
        let result = unsafe { libc::poll(&mut descriptor, 1, 0) };
        (result, descriptor.revents)
    };
    #[cfg(windows)]
    let (result, events) = {
        let mut descriptor = WSAPOLLFD {
            fd: socket.as_raw_socket() as _,
            events: match interest {
                Interest::Readable => POLLRDNORM,
                Interest::Writable => POLLWRNORM,
            },
            revents: 0,
        };
        // SAFETY: One initialized poll entry lives through this synchronous zero-timeout call.
        let result = unsafe { WSAPoll(&mut descriptor, 1, 0) };
        (result, descriptor.revents)
    };
    wait_result(result, events)
}

fn wait_result(result: i32, events: i16) -> io::Result<()> {
    result_count(result as isize)?;
    if events == 0 {
        Err(io::ErrorKind::WouldBlock.into())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
type NativeVector = libc::iovec;
#[cfg(windows)]
type NativeVector = WSABUF;

/// Scratch capacity is reused between attempts; raw pointers are rebuilt before every syscall and never used after it returns. Prepared packages bypass scratch entirely.
pub(crate) struct IoVectors {
    scratch: Vec<NativeVector>,
    empty: NativeVector,
}

// SAFETY: Scratch contains no independently usable pointers. Its pointers are only dereferenced synchronously while the operation exclusively retains the sequence from which they were rebuilt.
unsafe impl Send for IoVectors {}

impl IoVectors {
    pub(crate) fn new() -> Self {
        Self {
            scratch: Vec::new(),
            empty: native_vector(&[]),
        }
    }

    fn empty(&self) -> (*const NativeVector, usize) {
        (&self.empty, 1)
    }

    fn sending<B: IoBufSequence>(
        &mut self,
        buffers: &B,
    ) -> io::Result<(*const NativeVector, usize)> {
        validate(buffers)?;
        if buffers.buffer_count() == 0 {
            return Ok(self.empty());
        }
        if let Some(vectors) = buffers.io_slices() {
            // IoSlice is ABI-compatible with iovec/WSABUF. The retained sequence keeps this array and its bytes alive through the synchronous call.
            return Ok((vectors.as_ptr().cast(), vectors.len()));
        }
        self.scratch.clear();
        self.scratch
            .extend((0..buffers.buffer_count()).map(|index| native_vector(buffers.buffer(index))));
        Ok((self.scratch.as_ptr(), self.scratch.len()))
    }

    fn receiving<B: IoBufSequenceMut>(
        &mut self,
        buffers: &mut B,
    ) -> io::Result<(*const NativeVector, usize)> {
        validate(buffers)?;
        let count = buffers.buffer_count();
        if count == 0 {
            return Ok(self.empty());
        }
        if let Some(vectors) = buffers.io_slices_mut() {
            // IoSliceMut supplies native layout. The syscall only reads the vector array and writes the exclusively borrowed payload; it never changes vector entries.
            return Ok((vectors.as_ptr().cast(), vectors.len()));
        }
        self.scratch.clear();
        for index in 0..count {
            let bytes = buffers.buffer_mut(index);
            #[cfg(unix)]
            self.scratch.push(libc::iovec {
                iov_base: bytes.as_mut_ptr().cast(),
                iov_len: bytes.len(),
            });
            #[cfg(windows)]
            self.scratch.push(WSABUF {
                buf: bytes.as_mut_ptr(),
                len: bytes.len() as u32,
            });
        }
        Ok((self.scratch.as_ptr(), self.scratch.len()))
    }

    pub(crate) fn send<B: IoBufSequence>(
        &mut self,
        socket: &Socket,
        buffers: &B,
        address: Option<SocketAddr>,
        flags: SocketSendFlags,
    ) -> io::Result<usize> {
        let (vectors, count) = self.sending(buffers)?;
        // SAFETY: The sequence retains the vector array and all payload ranges until this synchronous call returns.
        unsafe { send_vectors(socket, vectors, count, address, send_flags(flags)) }
    }

    pub(crate) fn recv<B: IoBufSequenceMut>(
        &mut self,
        socket: &Socket,
        buffers: &mut B,
        flags: SocketRecvFlags,
    ) -> io::Result<SocketRecvResult> {
        let (vectors, count) = self.receiving(buffers)?;
        // SAFETY: The sequence retains the vector array and all payload ranges until this synchronous call returns.
        unsafe { recv_vectors(socket, vectors, count, recv_flags(flags)) }
    }
}

// SAFETY: The caller must retain the I/O-vector array and every payload range through this synchronous call; receive ranges must be exclusive. No pointers are retained after return.
unsafe fn send_vectors(
    socket: &Socket,
    vectors: *const NativeVector,
    count: usize,
    address: Option<SocketAddr>,
    flags: i32,
) -> io::Result<usize> {
    let address = address.map(SockAddr::from);
    #[cfg(unix)]
    {
        // SAFETY: Zero initializes valid msghdr metadata. Payload and vectors remain alive and immobile until the syscall returns; no control or zero-copy flags extend access.
        let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
        header.msg_iov = vectors.cast_mut();
        header.msg_iovlen = count as _;
        if let Some(address) = &address {
            header.msg_name = address.as_ptr().cast_mut().cast();
            header.msg_namelen = address.len();
        }
        // SAFETY: Header, address, vector array, and all initialized byte ranges are retained for this synchronous nonblocking send.
        result_count(unsafe { libc::sendmsg(socket.as_raw_fd(), &header, flags) })
    }
    #[cfg(windows)]
    {
        let mut bytes = 0;
        let (address, length) = address.as_ref().map_or((ptr::null(), 0), |address| {
            (address.as_ptr().cast(), address.len())
        });
        // SAFETY: All pointers are valid through this non-overlapped call. Winsock reads vector entries without modifying them; null overlapped/completion arguments prohibit deferred buffer access.
        result_count(unsafe {
            WSASendTo(
                socket.as_raw_socket() as _,
                vectors,
                count as u32,
                &mut bytes,
                flags as u32,
                address,
                length,
                ptr::null_mut(),
                None,
            )
        } as isize)?;
        Ok(bytes as usize)
    }
}

// SAFETY: The caller must retain the I/O-vector array and every payload range through this synchronous call; receive ranges must be exclusive. No pointers are retained after return.
unsafe fn recv_vectors(
    socket: &Socket,
    vectors: *const NativeVector,
    count: usize,
    flags: i32,
) -> io::Result<SocketRecvResult> {
    // SAFETY: try_init provides address storage and length initialized to capacity. The syscall fills only this storage, writes initialized payload within supplied ranges, and retains no pointers after return.
    let ((bytes, truncated, control_truncated), address) = unsafe {
        SockAddr::try_init(|address, length| {
            #[cfg(unix)]
            {
                let mut header: libc::msghdr = std::mem::zeroed();
                header.msg_iov = vectors.cast_mut();
                header.msg_iovlen = count as _;
                header.msg_name = address.cast();
                header.msg_namelen = *length;
                let bytes = result_count(libc::recvmsg(socket.as_raw_fd(), &mut header, flags))?;
                *length = header.msg_namelen;
                Ok((
                    bytes,
                    header.msg_flags & libc::MSG_TRUNC != 0,
                    header.msg_flags & libc::MSG_CTRUNC != 0,
                ))
            }
            #[cfg(windows)]
            {
                let mut bytes = 0;
                let mut flags = flags as u32;
                let result = WSARecvFrom(
                    socket.as_raw_socket() as _,
                    vectors,
                    count as u32,
                    &mut bytes,
                    &mut flags,
                    address.cast(),
                    length,
                    ptr::null_mut(),
                    None,
                );
                match result_count(result as isize) {
                    Ok(_) => Ok((bytes as usize, false, false)),
                    Err(error) if error.raw_os_error() == Some(WSAEMSGSIZE) => {
                        Ok((bytes as usize, true, false))
                    }
                    Err(error) => Err(error),
                }
            }
        })
    }?;
    Ok(SocketRecvResult {
        bytes,
        address: address.as_socket(),
        truncated,
        control_truncated,
    })
}

#[cfg(unix)]
const POLL_FLAGS: i32 = libc::MSG_DONTWAIT;
#[cfg(windows)]
const POLL_FLAGS: i32 = 0;

// Windows mode changes persist; aliases must not concurrently reset them. Unix instead supplies MSG_DONTWAIT directly to each syscall.
#[cfg(windows)]
fn poll_nonblocking(socket: &Socket) -> io::Result<()> {
    #[cfg(not(test))]
    let result = socket.set_nonblocking(true);
    #[cfg(test)]
    let result = tests::NONBLOCKING_ERROR.with(|slot| match slot.take() {
        Some(error) => Err(io::Error::from(error)),
        None => socket.set_nonblocking(true),
    });
    result
}

pub(crate) fn recv_poll(
    socket: &Socket,
    bytes: &mut [u8],
    flags: SocketRecvFlags,
) -> io::Result<usize> {
    #[cfg(windows)]
    poll_nonblocking(socket)?;
    let flags = POLL_FLAGS | recv_flags(flags);
    // SAFETY: This initialized, exclusive byte range remains alive until the synchronous receive returns.
    let bytes = unsafe { std::slice::from_raw_parts_mut(bytes.as_mut_ptr().cast(), bytes.len()) };
    socket.recv_with_flags(bytes, flags)
}

pub(crate) fn send_poll(
    socket: &Socket,
    bytes: &[u8],
    flags: SocketSendFlags,
) -> io::Result<usize> {
    #[cfg(windows)]
    poll_nonblocking(socket)?;
    socket.send_with_flags(bytes, POLL_FLAGS | send_flags(flags))
}

pub(crate) fn sendmsg_poll(
    socket: &Socket,
    buffers: &[IoSlice<'_>],
    address: Option<SocketAddr>,
    flags: SocketSendFlags,
) -> io::Result<usize> {
    validate_lengths(buffers.len(), buffers.iter().map(|buffer| buffer.len()))?;
    let empty = native_vector(&[]);
    let (vectors, count) = if buffers.is_empty() {
        (&empty as *const NativeVector, 1)
    } else {
        (buffers.as_ptr().cast(), buffers.len())
    };
    #[cfg(windows)]
    poll_nonblocking(socket)?;
    let flags = POLL_FLAGS | send_flags(flags);
    // SAFETY: IoSlice has the native iovec/WSABUF layout. The borrowed array and its initialized payload remain live throughout this synchronous call, including the empty-vector fallback.
    unsafe { send_vectors(socket, vectors, count, address, flags) }
}

pub(crate) fn recvmsg_poll(
    socket: &Socket,
    buffers: &mut [IoSliceMut<'_>],
    flags: SocketRecvFlags,
) -> io::Result<SocketRecvResult> {
    validate_lengths(buffers.len(), buffers.iter().map(|buffer| buffer.len()))?;
    let empty = native_vector(&[]);
    let (vectors, count) = if buffers.is_empty() {
        (&empty as *const NativeVector, 1)
    } else {
        (buffers.as_ptr().cast(), buffers.len())
    };
    #[cfg(windows)]
    poll_nonblocking(socket)?;
    let flags = POLL_FLAGS | recv_flags(flags);
    // SAFETY: IoSliceMut has the native layout. The array and exclusive payload ranges remain live throughout this synchronous call; vector entries are read only.
    unsafe { recv_vectors(socket, vectors, count, flags) }
}

fn native_vector(bytes: &[u8]) -> NativeVector {
    #[cfg(unix)]
    return libc::iovec {
        iov_base: bytes.as_ptr().cast_mut().cast(),
        iov_len: bytes.len(),
    };
    #[cfg(windows)]
    WSABUF {
        buf: bytes.as_ptr().cast_mut(),
        len: bytes.len() as u32,
    }
}

fn validate<B: IoBufSequence>(buffers: &B) -> io::Result<()> {
    validate_lengths(
        buffers.buffer_count(),
        (0..buffers.buffer_count()).map(|index| buffers.buffer(index).len()),
    )
}

fn validate_lengths(count: usize, mut lengths: impl Iterator<Item = usize>) -> io::Result<()> {
    if count > i32::MAX as usize
        || lengths
            .try_fold(0usize, usize::checked_add)
            .is_none_or(|length| length > i32::MAX as usize)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "message exceeds native length limits",
        ));
    }
    Ok(())
}

fn send_flags(flags: SocketSendFlags) -> i32 {
    #[cfg(unix)]
    let (base, dont_route) = (libc::MSG_NOSIGNAL, libc::MSG_DONTROUTE);
    #[cfg(windows)]
    let (base, dont_route) = (0, MSG_DONTROUTE);
    base | if flags.dont_route { dont_route } else { 0 }
}

fn recv_flags(flags: SocketRecvFlags) -> i32 {
    #[cfg(unix)]
    let peek = libc::MSG_PEEK;
    #[cfg(windows)]
    let peek = MSG_PEEK;
    if flags.peek { peek } else { 0 }
}

fn result_count(result: isize) -> io::Result<usize> {
    if result >= 0 {
        return Ok(result as usize);
    }
    #[cfg(unix)]
    let error = io::Error::last_os_error();
    #[cfg(windows)]
    // SAFETY: Reads the calling thread's most recent Winsock error.
    let error = io::Error::from_raw_os_error(unsafe { WSAGetLastError() });
    Err(error)
}

#[cfg(test)]
mod tests;

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    DirectScheduler, DirectSocket,
    blocking_pool::admission_error,
    direct_scheduler::wait_close,
    socket_io::{self, IoVectors},
};
use core_scheduler::*;
use socket2::Socket;
use std::{io, net::Shutdown};

impl DirectScheduler {
    // Borrowed payload stays in the polling future. Only the retry pulse outlives a synchronous attempt; nothing is dispatched to the worker pool.
    async fn socket_poll_operation<Output>(
        &self,
        socket: &DirectSocket,
        cancellation: &Cancellation,
        mut operation: impl FnMut(&Socket) -> io::Result<Output> + Send,
    ) -> OperationResult<Output> {
        cancellation.check()?;
        loop {
            self.socket_retry.check_open()?;
            match operation(&socket.0) {
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        || error.kind() == io::ErrorKind::Interrupted =>
                {
                    self.socket_retry.wait(cancellation).await?;
                }
                result => return result.map_err(Into::into),
            }
        }
    }

    /// Each attempt owns its entire input in the worker. Between attempts, no worker or kernel can access it, so abandonment is immediately safe. Actual syscall results win cancellation.
    async fn socket_operation<Input: Send + 'static, Output: Send + 'static>(
        &self,
        socket: &DirectSocket,
        input: Input,
        cancellation: &Cancellation,
        operation: impl FnMut(&Socket, &mut Input) -> io::Result<Output> + Send + 'static,
    ) -> (OperationResult<Output>, Input) {
        let mut state = (socket.retain(), input, operation);
        loop {
            let (result, returned) = self
                .work
                .execute(state, cancellation, |(socket, input, operation)| {
                    operation(socket, input)
                })
                .await;
            state = returned;
            match result {
                Err(OperationError::Io(error))
                    if error.kind() == io::ErrorKind::WouldBlock
                        || error.kind() == io::ErrorKind::Interrupted =>
                {
                    if let Err(error) = self.socket_retry.wait(cancellation).await {
                        return (Err(error), state.1);
                    }
                }
                result => return (result, state.1),
            }
        }
    }
}

impl SocketOperation for DirectScheduler {
    type Socket = DirectSocket;
}

impl SocketOpen for DirectScheduler {
    async fn socket_open(
        &self,
        domain: SocketDomain,
        kind: SocketType,
        protocol: Option<SocketProtocol>,
        cancellation: &Cancellation,
    ) -> OperationResult<DirectSocket> {
        self.work
            .execute((), cancellation, move |_| {
                socket_io::open(domain, kind, protocol).map(DirectSocket::new)
            })
            .await
            .0
    }
}

impl SocketBind for DirectScheduler {
    async fn socket_bind(
        &self,
        socket: &DirectSocket,
        address: SocketAddress,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.socket_operation(socket, (), cancellation, move |socket, _| {
            socket.bind(&address.into())
        })
        .await
        .0
    }
}

impl SocketListen for DirectScheduler {
    async fn socket_listen(
        &self,
        socket: &DirectSocket,
        backlog: i32,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.socket_operation(socket, (), cancellation, move |socket, _| {
            socket.listen(backlog)
        })
        .await
        .0
    }
}

impl SocketLocalAddress for DirectScheduler {
    fn socket_local_address(&self, socket: &DirectSocket) -> io::Result<SocketAddress> {
        socket.0.local_addr().map(socket_io::address)
    }
}

impl SocketPeerAddress for DirectScheduler {
    fn socket_peer_address(&self, socket: &DirectSocket) -> io::Result<SocketAddress> {
        socket.0.peer_addr().map(socket_io::address)
    }
}

impl SocketConnect for DirectScheduler {
    async fn socket_connect(
        &self,
        socket: &DirectSocket,
        address: SocketAddress,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.socket_operation(socket, false, cancellation, move |socket, started| {
            socket_io::connect(socket, address, started)
        })
        .await
        .0
    }
}

impl SocketAccept for DirectScheduler {
    async fn socket_accept(
        &self,
        socket: &DirectSocket,
        cancellation: &Cancellation,
    ) -> OperationResult<(DirectSocket, SocketAddress)> {
        self.socket_operation(socket, (), cancellation, |socket, _| {
            socket.accept().and_then(|(socket, address)| {
                socket_io::nonblocking(socket)
                    .map(|socket| (DirectSocket::new(socket), socket_io::address(address)))
            })
        })
        .await
        .0
    }
}

impl SocketRead for DirectScheduler {
    async fn socket_read<B: IoBufMut>(
        &self,
        socket: &DirectSocket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.socket_operation(socket, buffer, cancellation, |socket, buffer| {
            socket_io::recv(socket, buffer.as_mut())
        })
        .await
    }
}

impl SocketWrite for DirectScheduler {
    async fn socket_write<B: IoBuf>(
        &self,
        socket: &DirectSocket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.socket_operation(socket, buffer, cancellation, |socket, buffer| {
            socket_io::send(socket, buffer.as_ref())
        })
        .await
    }
}

impl SocketRecvPoll for DirectScheduler {
    async fn socket_recv_poll(
        &self,
        socket: &DirectSocket,
        buffer: &mut [u8],
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.socket_poll_operation(socket, cancellation, |socket| {
            socket_io::recv_poll(socket, buffer, flags)
        })
        .await
    }
}

impl SocketSendPoll for DirectScheduler {
    async fn socket_send_poll(
        &self,
        socket: &DirectSocket,
        buffer: &[u8],
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.socket_poll_operation(socket, cancellation, |socket| {
            socket_io::send_poll(socket, buffer, flags)
        })
        .await
    }
}

impl SocketRecvMsgPoll for DirectScheduler {
    async fn socket_recvmsg_poll(
        &self,
        socket: &DirectSocket,
        buffer: &mut [std::io::IoSliceMut<'_>],
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<SocketRecvResult> {
        self.socket_poll_operation(socket, cancellation, |socket| {
            socket_io::recvmsg_poll(socket, buffer, flags)
        })
        .await
    }
}

impl SocketSendMsgPoll for DirectScheduler {
    async fn socket_sendmsg_poll(
        &self,
        socket: &DirectSocket,
        buffer: &[std::io::IoSlice<'_>],
        address: Option<SocketAddress>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        self.socket_poll_operation(socket, cancellation, |socket| {
            socket_io::sendmsg_poll(socket, buffer, address, flags)
        })
        .await
    }
}

impl SocketWait for DirectScheduler {
    async fn socket_wait(
        &self,
        socket: &DirectSocket,
        interest: Interest,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.socket_operation(socket, (), cancellation, move |socket, _| {
            socket_io::wait(socket, interest)
        })
        .await
        .0
    }
}

impl SocketSendMsg for DirectScheduler {
    async fn socket_sendmsg<B: IoBufSequence>(
        &self,
        socket: &DirectSocket,
        message: SocketMessage<B>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<usize>, SocketMessage<B>) {
        let mut vectors = IoVectors::new();
        self.socket_operation(socket, message, cancellation, move |socket, message| {
            vectors.send(socket, &message.buffers, message.address, flags)
        })
        .await
    }
}

impl SocketRecvMsg for DirectScheduler {
    async fn socket_recvmsg<B: IoBufSequenceMut>(
        &self,
        socket: &DirectSocket,
        buffers: B,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<SocketRecvResult>, B) {
        let mut vectors = IoVectors::new();
        self.socket_operation(socket, buffers, cancellation, move |socket, buffers| {
            vectors.recv(socket, buffers, flags)
        })
        .await
    }
}

impl SocketClose for DirectScheduler {
    async fn socket_close(&self, socket: DirectSocket) -> io::Result<()> {
        let socket = socket.into_socket();
        let completion = self
            .cleanup
            .submit(move || socket_io::close(socket))
            .await
            .map_err(|error| io::Error::from(admission_error(error)))?;
        wait_close(completion).await
    }

    async fn socket_close_submit(&self, socket: DirectSocket) -> io::Result<()> {
        let socket = socket.into_socket();
        self.cleanup
            .submit(move || socket_io::close(socket))
            .await
            .map(drop)
            .map_err(|error| io::Error::from(admission_error(error)))
    }
}

impl SocketShutdown for DirectScheduler {
    async fn socket_shutdown(&self, socket: &DirectSocket, how: Shutdown) -> io::Result<()> {
        let socket = socket.retain();
        let completion = self
            .cleanup
            .submit(move || socket.shutdown(how))
            .await
            .map_err(|error| io::Error::from(admission_error(error)))?;
        wait_close(completion).await
    }

    async fn socket_shutdown_submit(&self, socket: &DirectSocket, how: Shutdown) -> io::Result<()> {
        let socket = socket.retain();
        self.cleanup
            .submit(move || socket.shutdown(how))
            .await
            .map(drop)
            .map_err(|error| io::Error::from(admission_error(error)))
    }
}

#[cfg(test)]
mod tests;

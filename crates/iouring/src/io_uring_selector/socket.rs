// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::Interest;

pub(super) fn retain_socket(socket: &impl SocketSource) -> RegisteredSocket {
    socket.retain_socket()
}

// O_NONBLOCK sockets can complete with EAGAIN. Retire that attempt before waiting for readiness and retrying on the operation's original ring. No borrowed payload reaches the readiness service.
pub(super) async fn retry_operation<B, T, O, F>(
    selector: &IoUringSelector,
    controller: &Arc<Controller>,
    socket: &impl SocketSource,
    mut buffers: B,
    mut operation: O,
    interest: Interest,
    cancellation: &Cancellation,
) -> (io::Result<T>, B)
where
    B: Send + 'static,
    O: FnMut(B) -> F,
    F: Future<Output = (io::Result<T>, B)>,
{
    loop {
        if let Err(error) = cancellation.check() {
            return (
                Err(io::Error::new(io::ErrorKind::Interrupted, error)),
                buffers,
            );
        }
        let (result, returned) = operation(buffers).await;
        buffers = returned;
        match result {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                let (result, returned) = selector
                    .wait_ready_owned(
                        Arc::clone(controller),
                        socket,
                        buffers,
                        interest,
                        cancellation,
                    )
                    .await;
                buffers = returned;
                if let Err(error) = result {
                    return (Err(error.into()), buffers);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return (result, buffers),
        }
    }
}

impl IoUringSelector {
    #[cfg(test)]
    pub(crate) async fn socket_read<B: IoBufMut>(
        &self,
        socket: &impl SocketSource,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let controller = self.route();
        let (result, buffer) = retry_operation(
            self,
            &controller,
            socket,
            buffer,
            |buffer| {
                self.operation(
                    Arc::clone(&controller),
                    retain_socket(socket),
                    buffer,
                    false,
                    |buffer| buffer.as_mut().as_mut_ptr(),
                    cancellation,
                )
            },
            Interest::Readable,
            cancellation,
        )
        .await;
        (operation_result(result, cancellation), buffer)
    }
}

impl IoUringSelector {
    pub(crate) async fn socket_write_owned<B: IoBuf>(
        &self,
        socket: &RegisteredSocket,
        buffer: B,
        operations: &mut crate::operation_state::OperationState,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        if let Err(error) = self.check_open() {
            return (Err(error.into()), buffer);
        }
        if let Err(error) = operations.check_idle() {
            return (Err(error), buffer);
        }
        // A ready socket can finish synchronously without publishing a request. No access to the owned buffer survives this nonblocking syscall.
        match syscall("socket-write", || {
            crate::retained_socket::send(socket, buffer.as_ref(), SocketSendFlags::default())
        }) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            result => return (operation_result(result, cancellation), buffer),
        }
        // The idle check and synchronous probe retain exclusive lane access without suspension; concurrent retirement can only leave the lane idle.
        let buffer = match operations.begin(buffer) {
            Ok(buffer) => buffer,
            Err(_) => unreachable!("exclusive idle lane cannot acquire another operation"),
        };
        let controller = self.route();
        let (result, buffer) = retry_operation(
            self,
            &controller,
            socket,
            buffer,
            |buffer| {
                self.operation(
                    Arc::clone(&controller),
                    retain_socket(socket),
                    buffer,
                    true,
                    |buffer| buffer.as_ref().as_ptr().cast_mut(),
                    cancellation,
                )
            },
            Interest::Writable,
            cancellation,
        )
        .await;
        (operation_result(result, cancellation), buffer.into_inner())
    }
}

#[cfg(test)]
impl IoUringSelector {
    pub(super) async fn socket_write<B: IoBuf>(
        &self,
        socket: &impl SocketSource,
        buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        self.socket_write_owned(
            &socket.retain_socket(),
            buffer,
            &mut Default::default(),
            cancellation,
        )
        .await
    }
}

#[cfg(test)]
mod tests;

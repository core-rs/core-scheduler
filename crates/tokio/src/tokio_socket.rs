// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use core_scheduler::Interest;
use std::{io, net::SocketAddr, sync::Arc};
use tokio::runtime::{Handle, Id};

#[cfg(unix)]
#[derive(Debug)]
pub(crate) struct RegisteredSocket {
    socket: Arc<socket2::Socket>,
    runtime: Handle,
    registration: tokio::sync::OnceCell<tokio::io::unix::AsyncFd<Arc<socket2::Socket>>>,
}

#[cfg(unix)]
impl RegisteredSocket {
    fn new(runtime: &Handle, socket: socket2::Socket) -> Self {
        Self {
            socket: Arc::new(socket),
            runtime: runtime.clone(),
            registration: tokio::sync::OnceCell::new(),
        }
    }

    fn get_ref(&self) -> &socket2::Socket {
        &self.socket
    }

    fn into_inner(self) -> socket2::Socket {
        drop(self.registration);
        Arc::try_unwrap(self.socket).expect("socket registration releases its private socket owner")
    }

    async fn registration(&self) -> io::Result<&tokio::io::unix::AsyncFd<Arc<socket2::Socket>>> {
        self.registration
            .get_or_try_init(|| async {
                let _scope = self.runtime.enter();
                // SAFETY: Both this wrapper and the registration own the same socket. No operation can close or replace its descriptor while registered, including if the wrapper or a pending future is forgotten.
                unsafe { tokio::io::unix::AsyncFd::register(Arc::clone(&self.socket)) }
                    .map_err(io::Error::from)
            })
            .await
    }
}

#[cfg(windows)]
type RegisteredSocket = async_io::Async<socket2::Socket>;

/// A nonblocking socket associated with the adapter's Tokio runtime.
///
/// A descriptor can represent an unconnected, bound, listening, connected, or datagram socket. Unix readiness registration is deferred until an operation needs to wait, so an unconnected socket cannot retain stale closed readiness after connecting or listening. It is deliberately not `Clone`; operation futures borrow it, and accepted shutdown jobs privately retain its registration until completion. Explicit close consumes exclusive ownership.
pub struct TokioSocket {
    pub(crate) inner: Arc<RegisteredSocket>,
    runtime: Id,
}

impl TokioSocket {
    pub(super) fn new(runtime: &Handle, socket: socket2::Socket) -> io::Result<Self> {
        Self::new_with(
            runtime,
            socket,
            |socket| socket.set_nonblocking(true),
            |runtime, socket| {
                #[cfg(unix)]
                {
                    Ok(RegisteredSocket::new(runtime, socket))
                }

                #[cfg(windows)]
                {
                    let _ = runtime;
                    RegisteredSocket::new(socket)
                }
            },
        )
    }

    fn new_with(
        runtime: &Handle,
        socket: socket2::Socket,
        set_nonblocking: impl FnOnce(&socket2::Socket) -> io::Result<()>,
        register: impl FnOnce(&Handle, socket2::Socket) -> io::Result<RegisteredSocket>,
    ) -> io::Result<Self> {
        set_nonblocking(&socket)?;
        let inner = register(runtime, socket)?;

        Ok(Self {
            inner: Arc::new(inner),
            runtime: runtime.id(),
        })
    }

    pub(super) fn retain(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            runtime: self.runtime,
        }
    }

    pub(super) fn into_close(self) -> impl FnOnce() -> io::Result<()> + Send + 'static {
        let inner = Arc::try_unwrap(self.inner)
            .expect("cannot close a socket with outstanding operations; await them first");
        move || {
            #[cfg(unix)]
            {
                crate::socket_io::close(inner.into_inner())
            }
            #[cfg(windows)]
            {
                inner.into_inner().and_then(crate::socket_io::close)
            }
        }
    }

    pub(super) fn runtime(&self) -> Id {
        self.runtime
    }

    pub(super) fn get_ref(&self) -> &socket2::Socket {
        #[cfg(unix)]
        {
            self.inner.get_ref()
        }

        #[cfg(windows)]
        {
            self.inner.get_ref()
        }
    }

    pub(super) async fn io<Output, Operation>(
        &self,
        interest: Interest,
        mut operation: Operation,
    ) -> io::Result<Output>
    where
        Operation: FnMut(&socket2::Socket) -> io::Result<Output> + Send,
    {
        // Try the syscall before registration or waiting. In particular, Linux can report EPOLLHUP for an unconnected TCP socket; Tokio retains closed readiness permanently, which would spin after the socket later connects and a syscall returns WouldBlock.
        let result = operation(self.get_ref());
        if !matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock) {
            return result;
        }

        #[cfg(unix)]
        {
            let interest = match interest {
                Interest::Readable => tokio::io::Interest::READABLE,
                Interest::Writable => tokio::io::Interest::WRITABLE,
            };
            self.inner
                .registration()
                .await?
                .async_io(interest, |_| operation(self.inner.get_ref()))
                .await
        }

        #[cfg(windows)]
        {
            match interest {
                Interest::Readable => self.inner.read_with(|socket| operation(socket)).await,
                Interest::Writable => self.inner.write_with(|socket| operation(socket)).await,
            }
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.get_ref().local_addr().map(crate::socket_io::address)
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.get_ref().peer_addr().map(crate::socket_io::address)
    }
}

#[cfg(test)]
mod tests;

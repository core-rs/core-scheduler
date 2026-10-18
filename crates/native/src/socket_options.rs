// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{NativeScheduler, NativeSocket};
use core_scheduler::{
    Cancellation, OperationResult, SocketGetOption, SocketKeepalive, SocketOption,
    SocketRegistration, SocketReuseAddress, SocketSetOption, TcpNoDelay,
};
use socket2::Socket;
use std::{
    io,
    net::{TcpListener, TcpStream},
};

// Only identifiers with a native mapping are implemented by the Native adapter; consumers still use portable option types.
trait NativeOption: SocketOption {
    fn get(self, socket: &Socket) -> io::Result<Self::Value>;
    fn set(self, socket: &Socket, value: Self::Value) -> io::Result<()>;
}
macro_rules! option {
    ($option:ty, $get:ident, $set:ident) => {
        impl NativeOption for $option {
            fn get(self, socket: &Socket) -> io::Result<bool> {
                socket.$get()
            }
            fn set(self, socket: &Socket, value: bool) -> io::Result<()> {
                socket.$set(value)
            }
        }
    };
}
option!(SocketReuseAddress, reuse_address, set_reuse_address);
option!(SocketKeepalive, keepalive, set_keepalive);
option!(TcpNoDelay, tcp_nodelay, set_tcp_nodelay);

impl<O: NativeOption + 'static> SocketGetOption<O> for NativeScheduler {
    async fn socket_get_option(
        &self,
        socket: &NativeSocket,
        option: O,
        cancellation: &Cancellation,
    ) -> OperationResult<O::Value> {
        cancellation.check()?;
        self.check_open()?;
        #[cfg(test)]
        self.option_probe.attempt(socket.get_ref())?;
        Ok(option.get(socket.get_ref())?)
    }
}
impl<O: NativeOption + 'static> SocketSetOption<O> for NativeScheduler {
    async fn socket_set_option(
        &self,
        socket: &NativeSocket,
        option: O,
        value: O::Value,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        self.check_open()?;
        #[cfg(test)]
        self.option_probe.attempt(socket.get_ref())?;
        Ok(option.set(socket.get_ref(), value)?)
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct Probe {
    pub(crate) calls: std::sync::atomic::AtomicUsize,
    pub(crate) failing: std::sync::atomic::AtomicUsize,
}
#[cfg(test)]
impl Probe {
    fn attempt(&self, socket: &Socket) -> io::Result<()> {
        use std::sync::atomic::Ordering;
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.failing.load(Ordering::SeqCst) == socket as *const Socket as usize {
            Err(io::ErrorKind::PermissionDenied.into())
        } else {
            Ok(())
        }
    }
}

impl NativeScheduler {
    fn register_tcp(&self, socket: Socket) -> io::Result<NativeSocket> {
        self.check_open()?;
        NativeSocket::new(socket)
    }
}
impl SocketRegistration for NativeScheduler {
    fn register_socket(&self, socket: TcpStream) -> io::Result<NativeSocket> {
        self.register_tcp(socket.into())
    }
    fn register_listener(&self, listener: TcpListener) -> io::Result<NativeSocket> {
        self.register_tcp(listener.into())
    }
}

#[cfg(test)]
mod tests;

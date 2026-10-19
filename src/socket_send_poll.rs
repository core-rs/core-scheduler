// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, OperationResult, SocketOperation, SocketSendFlags};
use std::future::Future;

/// Send once from borrowed initialized storage using nonblocking polling.
///
/// This optional capability follows the borrowed-buffer lifecycle of [`SocketRecvPoll`](crate::SocketRecvPoll): every buffer access finishes before a poll returns, and each I/O attempt is nonblocking. Windows implementations may leave the socket in nonblocking mode. No backend or kernel access may outlive that poll. Futures are lazy, pre-cancellation prevents I/O, successful progress wins concurrent cancellation, and pending operations register wakeups for readiness and cancellation. Stream callers handle short transfers; datagram callers submit a complete message per operation.
pub trait SocketSendPoll: SocketOperation {
    fn socket_send_poll(
        &self,
        socket: &Self::Socket,
        buffer: &[u8],
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send;
}

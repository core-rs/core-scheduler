// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, OperationResult, SocketOperation, SocketRecvFlags};
use std::future::Future;

/// Receive once into borrowed initialized storage using nonblocking polling.
///
/// The `_poll` suffix describes readiness-driven I/O; callers still `.await` the returned future. This optional capability accepts stack storage and ordinary borrowed slices. Buffer access must finish before each poll returns, including `Pending`; neither the backend nor the kernel may retain access between polls. Dropping or forgetting the future leaves no outstanding buffer access. Readiness and cancellation registrations may remain, but must not reference the bytes. Implementations must not dispatch borrowed storage to independent workers or completion requests.
///
/// Every I/O attempt must be nonblocking regardless of the socket's initial mode. Unix backends can use `MSG_DONTWAIT`; Windows backends may set `FIONBIO` before each attempt and leave nonblocking mode enabled. External aliases must not concurrently change blocking mode. Semantic flags do not control this requirement.
///
/// Futures are lazy, pre-cancellation prevents I/O, and successful progress wins concurrent cancellation. Pending operations arrange wakeups for readiness and cancellation. Receives preserve short transfers and do not resize storage. For datagrams, an undersized buffer can truncate and discard the remainder of a message.
///
/// ```
/// use core_scheduler::{Cancellation, OperationResult, SocketRecvPoll, SocketRecvFlags};
/// async fn read_header<S: SocketRecvPoll>(scheduler: &S, socket: &S::Socket) -> OperationResult<usize> {
///     let mut header = [0; 64];
///     scheduler.socket_recv_poll(socket, &mut header, SocketRecvFlags::default(), &Cancellation::never()).await
/// }
/// ```
pub trait SocketRecvPoll: SocketOperation {
    fn socket_recv_poll(
        &self,
        socket: &Self::Socket,
        buffer: &mut [u8],
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send;
}

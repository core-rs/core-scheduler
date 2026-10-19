// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{Cancellation, OperationResult, SocketAddress, SocketOperation, SocketSendFlags};
use std::{future::Future, io::IoSlice};

/// Send one scatter/gather message from borrowed I/O vectors using nonblocking polling. `None` for the address selects the connected peer.
///
/// Both the I/O-vector array and its payload may use stack storage. Every access finishes before a poll returns, following [`SocketRecvPoll`](crate::SocketRecvPoll). Futures are lazy, pre-cancellation prevents I/O, and successful progress wins concurrent cancellation. Pending operations register wakeups for readiness and cancellation. No payload copying or vector allocation is required by this interface. Short stream transfers and datagram boundaries are preserved; vectors are never advanced implicitly.
pub trait SocketSendMsgPoll: SocketOperation {
    fn socket_sendmsg_poll(
        &self,
        socket: &Self::Socket,
        buffers: &[IoSlice<'_>],
        address: Option<SocketAddress>,
        flags: SocketSendFlags,
        cancellation: &Cancellation,
    ) -> impl Future<Output = OperationResult<usize>> + Send;
}

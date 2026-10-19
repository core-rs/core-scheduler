// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate borrowed socket tests. The runner supplies `(scheduler, sender, receiver)` as references to a fresh bound, unconnected datagram pair. Requires `SocketRecvMsgPoll`, `SocketSendMsgPoll`, and `SocketLocalAddress`.
///
/// Invoke inside a dedicated module. The runner must create a fresh isolated fixture, await the closure, clean up, enforce a deadline, and propagate panics. Async closures require Rust 1.85 or later.
#[macro_export]
macro_rules! socket_datagram_poll_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            addresses_peek_and_truncation => |scheduler, sender, receiver| {
                $crate::socket_poll::datagram_truncation(scheduler, sender, receiver).await;
            },
            empty_messages => |scheduler, sender, receiver| {
                $crate::socket_poll::datagram_empty(scheduler, sender, receiver).await;
            },
            pending_cancellation => |scheduler, sender, receiver| {
                let _ = sender;
                $crate::socket_poll::message_cancellation(scheduler, receiver).await;
            },
        }
    };
}

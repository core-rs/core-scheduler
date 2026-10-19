// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Generate borrowed socket tests. The runner supplies `(scheduler, sender, receiver)` as references to a fresh connected stream pair. Requires `SocketRecvMsgPoll` and `SocketSendMsgPoll`.
///
/// Invoke inside a dedicated module. The runner must create a fresh isolated fixture, await the closure, clean up, enforce a deadline, and propagate panics. Async closures require Rust 1.85 or later.
#[macro_export]
macro_rules! socket_message_poll_tests {
    (run: $run:path $(,)?) => {
        $crate::__conformance_test_cases! { $run;
            pre_cancelled => |scheduler, sender, receiver| {
                $crate::socket_poll::message_pre_cancelled(scheduler, sender, receiver).await;
            },
            stack_transfers => |scheduler, sender, receiver| {
                $crate::socket_poll::message_transfer(scheduler, sender, receiver).await;
            },
            pending_cancellation => |scheduler, sender, receiver| {
                let _ = sender;
                $crate::socket_poll::message_cancellation(scheduler, receiver).await;
            },
            abandoned_receives => |scheduler, sender, receiver| {
                $crate::socket_poll::message_abandonment(scheduler, sender, receiver).await;
            },
            readiness => |scheduler, sender, receiver| {
                $crate::socket_poll::message_readiness(scheduler, sender, receiver).await;
            },
        }
    };
}

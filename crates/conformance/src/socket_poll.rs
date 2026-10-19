// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! Borrowed nonblocking I/O assertions. Fixtures provide fresh sockets and drive readiness while enforcing test deadlines.

use crate::probe::WakeProbe;
use core_scheduler::*;
use futures_lite::future::poll_once;
use std::{
    future::Future,
    io::{IoSlice, IoSliceMut},
    pin::pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Waker},
};

fn cancelled<T>(result: OperationResult<T>) {
    assert!(matches!(result, Err(OperationError::Cancelled(_))));
}

/// Pre-cancelled scalar operations leave storage and the receive queue unchanged. Unpolled operations perform no I/O.
pub async fn scalar_pre_cancelled<S: SocketRecvPoll + SocketSendPoll>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let token = Cancellation::new();
    token.cancel();
    let never = Cancellation::never();
    let mut target = [0x5a; 1];
    drop(scheduler.socket_send_poll(sender, b"unpolled", SocketSendFlags::default(), &never));
    cancelled(
        scheduler
            .socket_send_poll(sender, b"cancelled", SocketSendFlags::default(), &token)
            .await,
    );
    assert_eq!(
        scheduler
            .socket_send_poll(sender, &[0xff], SocketSendFlags::default(), &never)
            .await
            .unwrap(),
        1
    );
    drop(scheduler.socket_recv_poll(receiver, &mut target, SocketRecvFlags::default(), &never));
    cancelled(
        scheduler
            .socket_recv_poll(receiver, &mut target, SocketRecvFlags::default(), &token)
            .await,
    );
    assert_eq!(target, [0x5a]);
    assert_eq!(
        scheduler
            .socket_recv_poll(receiver, &mut target, SocketRecvFlags::default(), &never)
            .await
            .unwrap(),
        1
    );
    assert_eq!(target, [0xff]);
}

/// Stack buffers support short reads and semantic peek flags without losing stream data.
pub async fn scalar_transfer<S: SocketRecvPoll + SocketSendPoll>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let source = *b"abcdef";
    let mut sent = 0;
    while sent < source.len() {
        let count = scheduler
            .socket_send_poll(sender, &source[sent..], SocketSendFlags::default(), &never)
            .await
            .unwrap();
        assert!(count > 0 && count <= source.len() - sent);
        sent += count;
    }
    let mut peek = [0; 1];
    assert_eq!(
        scheduler
            .socket_recv_poll(receiver, &mut peek, SocketRecvFlags { peek: true }, &never)
            .await
            .unwrap(),
        1
    );
    assert_eq!(peek, [b'a']);
    let mut target = [0; 6];
    let mut received = 0;
    while received < target.len() {
        let mut chunk = [0; 2];
        let count = scheduler
            .socket_recv_poll(
                receiver,
                &mut chunk[..(6 - received).min(2)],
                SocketRecvFlags::default(),
                &never,
            )
            .await
            .unwrap();
        assert!(count > 0 && count <= 2);
        target[received..received + count].copy_from_slice(&chunk[..count]);
        received += count;
    }
    assert_eq!(target, source);
}

/// Quiet scalar receives must register a cancellation wakeup and leave initialized storage unchanged.
pub async fn scalar_cancellation<S: SocketRecvPoll>(scheduler: &S, receiver: &S::Socket) {
    let token = Cancellation::new();
    let mut target = [0x5a; 8];
    {
        let mut receiving = pin!(scheduler.socket_recv_poll(
            receiver,
            &mut target,
            SocketRecvFlags::default(),
            &token
        ));
        let probe = Arc::new(WakeProbe::default());
        let waker = Waker::from(Arc::clone(&probe));
        assert!(
            receiving
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        token.cancel();
        assert!(
            probe.0.load(Ordering::SeqCst) > 0,
            "cancellation did not wake the receiver"
        );
        cancelled(receiving.await);
    }
    assert_eq!(target, [0x5a; 8]);
}

/// Dropping or forgetting a pending receive releases the borrow safely. Subsequent I/O must neither access its old stack bytes nor consume data on its behalf.
pub async fn scalar_abandonment<S: SocketRecvPoll + SocketSendPoll>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    for forget in [false, true] {
        let mut old = [0x5a; 8];
        let mut receiving = Box::pin(scheduler.socket_recv_poll(
            receiver,
            &mut old,
            SocketRecvFlags::default(),
            &never,
        ));
        assert!(poll_once(&mut receiving).await.is_none());
        if forget {
            std::mem::forget(receiving);
        } else {
            drop(receiving);
        }
        old.fill(0xa5);
        assert_eq!(
            scheduler
                .socket_send_poll(sender, b"x", SocketSendFlags::default(), &never)
                .await
                .unwrap(),
            1
        );
        let mut target = [0; 1];
        assert_eq!(
            scheduler
                .socket_recv_poll(receiver, &mut target, SocketRecvFlags::default(), &never)
                .await
                .unwrap(),
            1
        );
        assert_eq!(target, *b"x");
        assert_eq!(old, [0xa5; 8]);
    }
}

/// A pending receive wakes when data arrives; write shutdown produces EOF without modifying storage.
pub async fn scalar_readiness_and_eof<S: SocketRecvPoll + SocketSendPoll + SocketShutdown>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let mut target = [0x5a; 2];
    {
        let mut receiving = pin!(scheduler.socket_recv_poll(
            receiver,
            &mut target,
            SocketRecvFlags::default(),
            &never
        ));
        assert!(poll_once(&mut receiving).await.is_none());
        assert_eq!(
            scheduler
                .socket_send_poll(sender, b"x", SocketSendFlags::default(), &never)
                .await
                .unwrap(),
            1
        );
        assert_eq!(receiving.await.unwrap(), 1);
    }
    assert_eq!(target, [b'x', 0x5a]);
    scheduler
        .socket_shutdown(sender, std::net::Shutdown::Write)
        .await
        .unwrap();
    assert_eq!(
        scheduler
            .socket_recv_poll(receiver, &mut target, SocketRecvFlags::default(), &never)
            .await
            .unwrap(),
        0
    );
    assert_eq!(target, [b'x', 0x5a]);
}

/// Pre-cancelled and unpolled vector operations preserve both the queue and initialized stack storage.
pub async fn message_pre_cancelled<S: SocketRecvMsgPoll + SocketSendMsgPoll>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let token = Cancellation::new();
    token.cancel();
    let never = Cancellation::never();
    let source = [IoSlice::new(b"unpolled")];
    drop(scheduler.socket_sendmsg_poll(sender, &source, None, SocketSendFlags::default(), &never));
    cancelled(
        scheduler
            .socket_sendmsg_poll(sender, &source, None, SocketSendFlags::default(), &token)
            .await,
    );
    assert_eq!(
        scheduler
            .socket_sendmsg_poll(
                sender,
                &[IoSlice::new(&[0xff])],
                None,
                SocketSendFlags::default(),
                &never
            )
            .await
            .unwrap(),
        1
    );
    let mut target = [0x5a; 2];
    {
        let mut vectors = [IoSliceMut::new(&mut target)];
        drop(scheduler.socket_recvmsg_poll(
            receiver,
            &mut vectors,
            SocketRecvFlags::default(),
            &never,
        ));
        cancelled(
            scheduler
                .socket_recvmsg_poll(receiver, &mut vectors, SocketRecvFlags::default(), &token)
                .await,
        );
    }
    assert_eq!(target, [0x5a; 2]);
    assert_eq!(
        scheduler
            .socket_recvmsg_poll(
                receiver,
                &mut [IoSliceMut::new(&mut target)],
                SocketRecvFlags::default(),
                &never
            )
            .await
            .unwrap()
            .bytes,
        1
    );
    assert_eq!(target, [0xff, 0x5a]);
}

/// Stack vectors and payloads reach stream I/O with explicit advancement after short transfers. Neither send nor receive advances the caller's vector entries.
pub async fn message_transfer<S: SocketRecvMsgPoll + SocketSendMsgPoll>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let header = *b"head";
    let body = *b"body";
    let mut source = [IoSlice::new(&header), IoSlice::new(&body)];
    let mut pending = &mut source[..];
    while !pending.is_empty() {
        let before: usize = pending.iter().map(|buffer| buffer.len()).sum();
        let count = scheduler
            .socket_sendmsg_poll(sender, pending, None, SocketSendFlags::default(), &never)
            .await
            .unwrap();
        assert!(count > 0 && count <= before);
        assert_eq!(
            pending.iter().map(|buffer| buffer.len()).sum::<usize>(),
            before
        );
        IoSlice::advance_slices(&mut pending, count);
    }
    let mut first = [0x5a; 3];
    let mut second = [0x5a; 5];
    {
        let mut vectors = [IoSliceMut::new(&mut first), IoSliceMut::new(&mut second)];
        let mut pending = &mut vectors[..];
        let mut received = 0;
        while !pending.is_empty() {
            let before: usize = pending.iter().map(|buffer| buffer.len()).sum();
            let result = scheduler
                .socket_recvmsg_poll(receiver, pending, SocketRecvFlags::default(), &never)
                .await
                .unwrap();
            assert!(result.bytes > 0 && result.bytes <= before);
            assert_eq!(
                pending.iter().map(|buffer| buffer.len()).sum::<usize>(),
                before
            );
            received += result.bytes;
            IoSliceMut::advance_slices(&mut pending, result.bytes);
        }
        assert_eq!(received, 8);
    }
    assert_eq!(first, *b"hea");
    assert_eq!(second, *b"dbody");
}

/// A quiet vector receive registers cancellation and readiness wakeups; its stack vectors remain unchanged on cancellation.
pub async fn message_cancellation<S: SocketRecvMsgPoll>(scheduler: &S, receiver: &S::Socket) {
    let token = Cancellation::new();
    let mut target = [0x5a; 8];
    {
        let mut vectors = [IoSliceMut::new(&mut target)];
        let pointer = vectors[0].as_ptr();
        {
            let mut receiving = pin!(scheduler.socket_recvmsg_poll(
                receiver,
                &mut vectors,
                SocketRecvFlags::default(),
                &token
            ));
            let probe = Arc::new(WakeProbe::default());
            let waker = Waker::from(Arc::clone(&probe));
            assert!(
                receiving
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            token.cancel();
            assert!(probe.0.load(Ordering::SeqCst) > 0);
            cancelled(receiving.await);
        }
        assert_eq!(vectors[0].as_ptr(), pointer);
        assert_eq!(vectors[0].len(), 8);
    }
    assert_eq!(target, [0x5a; 8]);
}

/// Neither dropping nor forgetting a pending vector receive leaves access to its stack vectors or payload.
pub async fn message_abandonment<S: SocketRecvMsgPoll + SocketSendMsgPoll>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    for forget in [false, true] {
        let mut old = [0x5a; 8];
        {
            let mut vectors = [IoSliceMut::new(&mut old)];
            let mut receiving = Box::pin(scheduler.socket_recvmsg_poll(
                receiver,
                &mut vectors,
                SocketRecvFlags::default(),
                &never,
            ));
            assert!(poll_once(&mut receiving).await.is_none());
            if forget {
                std::mem::forget(receiving);
            } else {
                drop(receiving);
            }
        }
        old.fill(0xa5);
        assert_eq!(
            scheduler
                .socket_sendmsg_poll(
                    sender,
                    &[IoSlice::new(b"x")],
                    None,
                    SocketSendFlags::default(),
                    &never
                )
                .await
                .unwrap(),
            1
        );
        let mut target = [0; 1];
        assert_eq!(
            scheduler
                .socket_recvmsg_poll(
                    receiver,
                    &mut [IoSliceMut::new(&mut target)],
                    SocketRecvFlags::default(),
                    &never
                )
                .await
                .unwrap()
                .bytes,
            1
        );
        assert_eq!(target, *b"x");
        assert_eq!(old, [0xa5; 8]);
    }
}

/// A pending vector receive resumes on readiness and preserves initialized bytes beyond its returned count.
pub async fn message_readiness<S: SocketRecvMsgPoll + SocketSendMsgPoll>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let mut first = [0x5a; 1];
    let mut second = [0x5a; 2];
    {
        let mut vectors = [IoSliceMut::new(&mut first), IoSliceMut::new(&mut second)];
        let mut receiving = pin!(scheduler.socket_recvmsg_poll(
            receiver,
            &mut vectors,
            SocketRecvFlags::default(),
            &never
        ));
        assert!(poll_once(&mut receiving).await.is_none());
        assert_eq!(
            scheduler
                .socket_sendmsg_poll(
                    sender,
                    &[IoSlice::new(b"x")],
                    None,
                    SocketSendFlags::default(),
                    &never
                )
                .await
                .unwrap(),
            1
        );
        assert_eq!(receiving.await.unwrap().bytes, 1);
    }
    assert_eq!(first, *b"x");
    assert_eq!(second, [0x5a; 2]);
}

/// Addressed borrowed messages preserve peek, truncation, discard, source metadata, and datagram boundaries.
pub async fn datagram_truncation<S: SocketRecvMsgPoll + SocketSendMsgPoll + SocketLocalAddress>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let destination = scheduler.socket_local_address(receiver).unwrap();
    let source = scheduler.socket_local_address(sender).unwrap();
    let header = *b"abc";
    let body = *b"def";
    assert_eq!(
        scheduler
            .socket_sendmsg_poll(
                sender,
                &[IoSlice::new(&header), IoSlice::new(&body)],
                Some(destination),
                SocketSendFlags { dont_route: true },
                &never
            )
            .await
            .unwrap(),
        6
    );
    let mut first = [0; 2];
    let mut second = [0; 2];
    let result = scheduler
        .socket_recvmsg_poll(
            receiver,
            &mut [IoSliceMut::new(&mut first), IoSliceMut::new(&mut second)],
            SocketRecvFlags { peek: true },
            &never,
        )
        .await
        .unwrap();
    assert_eq!(result.bytes, 4);
    assert_eq!(result.address, Some(source));
    assert!(result.truncated);
    assert_eq!(first, *b"ab");
    assert_eq!(second, *b"cd");
    let mut target = [0; 3];
    let result = scheduler
        .socket_recvmsg_poll(
            receiver,
            &mut [IoSliceMut::new(&mut target)],
            SocketRecvFlags::default(),
            &never,
        )
        .await
        .unwrap();
    assert_eq!(result.bytes, 3);
    assert!(result.truncated);
    assert_eq!(target, *b"abc");
    assert_eq!(
        scheduler
            .socket_sendmsg_poll(
                sender,
                &[IoSlice::new(b"next")],
                Some(destination),
                SocketSendFlags::default(),
                &never
            )
            .await
            .unwrap(),
        4
    );
    let mut target = [0x5a; 8];
    let result = scheduler
        .socket_recvmsg_poll(
            receiver,
            &mut [IoSliceMut::new(&mut target)],
            SocketRecvFlags::default(),
            &never,
        )
        .await
        .unwrap();
    assert_eq!(result.bytes, 4);
    assert_eq!(result.address, Some(source));
    assert!(!result.truncated && !result.control_truncated);
    assert_eq!(&target[..4], b"next");
    assert_eq!(&target[4..], &[0x5a; 4]);
}

/// Empty vector arrays still represent real empty datagrams and provide a usable zero-capacity receive.
pub async fn datagram_empty<S: SocketRecvMsgPoll + SocketSendMsgPoll + SocketLocalAddress>(
    scheduler: &S,
    sender: &S::Socket,
    receiver: &S::Socket,
) {
    let never = Cancellation::never();
    let destination = scheduler.socket_local_address(receiver).unwrap();
    let source = scheduler.socket_local_address(sender).unwrap();
    assert_eq!(
        scheduler
            .socket_sendmsg_poll(
                sender,
                &[],
                Some(destination),
                SocketSendFlags::default(),
                &never
            )
            .await
            .unwrap(),
        0
    );
    let result = scheduler
        .socket_recvmsg_poll(receiver, &mut [], SocketRecvFlags::default(), &never)
        .await
        .unwrap();
    assert_eq!(result.bytes, 0);
    // Zero-capacity receives need not return sender metadata on every platform.
    assert!(!result.truncated);
    assert_eq!(
        scheduler
            .socket_sendmsg_poll(
                sender,
                &[IoSlice::new(&[])],
                Some(destination),
                SocketSendFlags::default(),
                &never
            )
            .await
            .unwrap(),
        0
    );
    let mut target = [0x5a; 1];
    let result = scheduler
        .socket_recvmsg_poll(
            receiver,
            &mut [IoSliceMut::new(&mut target)],
            SocketRecvFlags::default(),
            &never,
        )
        .await
        .unwrap();
    assert_eq!(result.bytes, 0);
    assert_eq!(result.address, Some(source));
    assert_eq!(target, [0x5a]);
}

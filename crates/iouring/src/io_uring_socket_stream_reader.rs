// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::Interest;
use crate::{IoUringBufferPool, IoUringSelector, MonitorStorage};
use core_scheduler::{
    BufferPool, BufferPoolOptions, BufferPooling, Cancellation, OperationResult, ReceiveBuffer,
    SocketRecvFlags, SocketStreamReader,
};
use core_scheduler_selector::NativeSocket;
use std::{
    future::{Future, poll_fn},
    io::{self, IoSliceMut},
    pin::pin,
    task::Poll,
};

/// A scheduler-bound stream reader with reusable registration and lazily allocated receive storage. Normally obtained through [`SocketStreamReading`](core_scheduler::SocketStreamReading).
pub(crate) struct IoUringSocketStreamReader<'a> {
    scheduler: &'a IoUringSelector,
    operations: crate::operation_state::OperationState,
    owned: Option<crate::io_uring_selector::socket_read::State>,
    monitor: MonitorStorage,
    pool: Option<IoUringBufferPool>,
    ahead: crate::receive_ahead::ReceiveAhead,
    eof: bool,
    closed: bool,
}

impl<'a> IoUringSocketStreamReader<'a> {
    /// Construct the default backend composition without submitting payload I/O. Applications normally use the scheduler factory.
    pub(crate) fn new(scheduler: &'a IoUringSelector, socket: &NativeSocket) -> io::Result<Self> {
        if scheduler.socket_type(socket)? != core_scheduler::SocketType::Stream {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream readers require a stream socket",
            ));
        }
        Ok(Self {
            scheduler,
            operations: Default::default(),
            owned: None,
            monitor: scheduler.socket_monitor(socket, Interest::Readable),
            pool: None,
            ahead: Default::default(),
            eof: false,
            closed: false,
        })
    }

    /// Select shared storage for subsequent owned reads. Already returned chunks retain their original pool. Borrowed reads require no pool; otherwise the first owned read creates two reusable 64 KiB slots.
    pub(crate) fn with_pool(mut self, pool: IoUringBufferPool) -> Self {
        self.pool = Some(pool);
        self
    }

    fn check_open(&self) -> OperationResult<()> {
        if self.closed {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "stream reader is closed").into())
        } else {
            Ok(())
        }
    }
}

impl SocketStreamReader for IoUringSocketStreamReader<'_> {
    type Buffer = <IoUringBufferPool as BufferPool>::Buffer;

    async fn read(&mut self, cancellation: &Cancellation) -> OperationResult<Option<Self::Buffer>> {
        cancellation.check()?;
        if self.closed || self.eof {
            return Ok(None);
        }
        self.operations.check_idle()?;
        if self.pool.is_none() {
            self.pool = Some(
                self.scheduler
                    .buffer_pool(BufferPoolOptions::new(2, 64 * 1024))?,
            );
        }
        if self
            .pool
            .as_ref()
            .is_some_and(IoUringBufferPool::is_provided)
        {
            let message = self
                .ahead
                .read(
                    self.scheduler,
                    self.monitor.socket(),
                    self.pool.as_ref().unwrap(),
                    cancellation,
                )
                .await?;
            if message.is_none() {
                self.eof = true;
            }
            return Ok(message.map(core_scheduler::SocketRecvMessage::into_payload));
        }
        let pool = self
            .pool
            .as_ref()
            .expect("owned reads initialize their pool");
        loop {
            let mut buffer = pool.acquire(cancellation).await?;
            let result = {
                let mut reading = pin!(self.scheduler.socket_monitor_recv_poll(
                    &mut self.monitor,
                    buffer.as_mut(),
                    SocketRecvFlags::default(),
                    cancellation
                ));
                poll_fn(|context| Poll::Ready(reading.as_mut().poll(context))).await
            };
            match result {
                Poll::Ready(result) => {
                    let count = result?;
                    if count == 0 {
                        self.eof = true;
                        return Ok(None);
                    }
                    buffer.truncate(count);
                    return Ok(Some(buffer));
                }
                Poll::Pending => drop(buffer),
            }
            self.scheduler
                .socket_monitor_wait(&mut self.monitor, cancellation)
                .await?;
        }
    }

    async fn read_into(
        &mut self,
        buffer: &mut [u8],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        if self.ahead.active() {
            return self
                .read_vectored_with_flags(
                    &mut [IoSliceMut::new(buffer)],
                    SocketRecvFlags::default(),
                    cancellation,
                )
                .await
                .map(|result| result.bytes);
        }

        self.check_open()?;
        self.operations.check_idle()?;
        if self.eof || buffer.is_empty() {
            cancellation.check()?;
            return Ok(0);
        }
        let count = self
            .scheduler
            .socket_monitor_recv_poll(
                &mut self.monitor,
                buffer,
                SocketRecvFlags::default(),
                cancellation,
            )
            .await?;
        if count == 0 {
            self.eof = true;
        }
        Ok(count)
    }

    async fn read_vectored(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        cancellation: &Cancellation,
    ) -> OperationResult<usize> {
        if self.ahead.active() {
            return self
                .read_vectored_with_flags(buffers, SocketRecvFlags::default(), cancellation)
                .await
                .map(|result| result.bytes);
        }

        self.check_open()?;
        self.operations.check_idle()?;
        if self.eof || buffers.iter().all(|buffer| buffer.is_empty()) {
            cancellation.check()?;
            return Ok(0);
        }
        let result = self
            .scheduler
            .socket_monitor_recvmsg_poll(
                &mut self.monitor,
                buffers,
                SocketRecvFlags::default(),
                cancellation,
            )
            .await?;
        if result.bytes == 0 {
            self.eof = true;
        }
        Ok(result.bytes)
    }

    async fn read_vectored_with_flags(
        &mut self,
        buffers: &mut [IoSliceMut<'_>],
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> OperationResult<core_scheduler::SocketRecvResult> {
        if self.ahead.active() {
            self.check_open()?;
            // Receive-ahead acquires this lane only after raw operations retire; all later owned reads stay with that receiver.
            if self.eof || buffers.iter().all(|buffer| buffer.is_empty()) {
                cancellation.check()?;
                return Ok(core_scheduler::SocketRecvResult {
                    bytes: 0,
                    address: None,
                    truncated: false,
                    control_truncated: false,
                });
            }
            return self
                .ahead
                .read_vectored(buffers, flags, true, cancellation)
                .await;
        }

        self.check_open()?;
        self.operations.check_idle()?;
        if self.eof || buffers.iter().all(|buffer| buffer.is_empty()) {
            cancellation.check()?;
            return Ok(core_scheduler::SocketRecvResult {
                bytes: 0,
                address: None,
                truncated: false,
                control_truncated: false,
            });
        }
        self.scheduler
            .socket_monitor_recvmsg_poll(&mut self.monitor, buffers, flags, cancellation)
            .await
    }
    async fn read_owned_vectored<Buffers: core_scheduler::IoBufSequenceMut>(
        &mut self,
        buffers: Buffers,
        flags: SocketRecvFlags,
        cancellation: &Cancellation,
    ) -> (OperationResult<core_scheduler::SocketRecvResult>, Buffers) {
        if self.ahead.active() {
            if let Err(error) = self.check_open() {
                return (Err(error), buffers);
            }
            return self
                .ahead
                .read_owned(buffers, flags, true, cancellation)
                .await;
        }

        if let Err(error) = self.check_open() {
            return (Err(error), buffers);
        }
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffers);
        }
        if self.eof || (0..buffers.buffer_count()).all(|index| buffers.buffer(index).is_empty()) {
            return (
                Ok(core_scheduler::SocketRecvResult {
                    bytes: 0,
                    address: None,
                    truncated: false,
                    control_truncated: false,
                }),
                buffers,
            );
        }
        let buffers = match self.operations.begin(buffers) {
            Ok(buffers) => buffers,
            Err((error, buffers)) => return (Err(error), buffers),
        };
        let (result, buffers) = self
            .scheduler
            .socket_recvmsg(self.monitor.socket(), buffers, flags, cancellation)
            .await;
        (result, buffers.into_inner())
    }
    async fn read_owned<B: core_scheduler::IoBufMut>(
        &mut self,
        buffer: B,
        cancellation: &Cancellation,
    ) -> core_scheduler::BufferResult<B> {
        if self.ahead.active() {
            let mut buffer = buffer;
            let result = self.read_into(buffer.as_mut(), cancellation).await;
            return (result, buffer);
        }

        if let Err(error) = self.check_open() {
            return (Err(error), buffer);
        }
        if self.eof || buffer.as_ref().is_empty() {
            return (cancellation.check().map(|()| 0).map_err(Into::into), buffer);
        }
        if let Err(error) = cancellation.check() {
            return (Err(error.into()), buffer);
        }
        let buffer = match self.operations.begin(buffer) {
            Ok(buffer) => buffer,
            Err((error, buffer)) => return (Err(error), buffer),
        };
        let (result, buffer) = self
            .owned
            .get_or_insert_with(Default::default)
            .read(self.scheduler, self.monitor.socket(), buffer, cancellation)
            .await;
        if matches!(result, Ok(0)) {
            self.eof = true;
        }
        (result, buffer.into_inner())
    }
    async fn close(&mut self) -> io::Result<()> {
        self.closed = true;
        self.operations.close().await;
        let ahead = self.ahead.close().await;
        self.scheduler
            .socket_monitor_close(&mut self.monitor)
            .await
            .and(ahead)
    }
}

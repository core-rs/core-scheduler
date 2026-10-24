// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use std::{any::Any, sync::Mutex};

pub(crate) struct State {
    shared: Arc<Shared>,
    cached: Option<Box<dyn Any + Send>>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            shared: Arc::new(Shared {
                slot: Mutex::new(Slot {
                    result: None,
                    collecting: false,
                }),
                ready: AtomicBool::new(false),
                waker: atomic_waker::AtomicWaker::new(),
            }),
            cached: None,
        }
    }
}

enum Completion {
    Request(io::Result<usize>, Box<dyn Any + Send>),
    Rejected(Box<dyn Any + Send>),
    Failed,
}
struct Slot {
    result: Option<Completion>,
    collecting: bool,
}
struct Shared {
    slot: Mutex<Slot>,
    ready: AtomicBool,
    waker: atomic_waker::AtomicWaker,
}
impl Shared {
    fn publish(&self, result: Completion) {
        let abandoned = {
            let mut slot = self.slot.lock().unwrap();
            let abandoned = if slot.collecting {
                slot.result = Some(result);
                None
            } else {
                Some(result)
            };
            // Dropping a returned owner can retire its lane and permit immediate reuse. Publish readiness before releasing that owner or allowing collection to take it.
            self.ready.store(true, Ordering::Release);
            abandoned
        };
        self.waker.wake();
        discard(abandoned);
    }
}
impl Drop for Shared {
    fn drop(&mut self) {
        discard(
            self.slot
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .result
                .take(),
        );
    }
}

struct CompletionWait<'a> {
    cancel: CancelOnDrop,
    shared: Option<&'a Shared>,
}
impl Drop for CompletionWait<'_> {
    fn drop(&mut self) {
        let Some(shared) = self.shared.take() else {
            return;
        };
        let abandoned = {
            let mut slot = shared.slot.lock().unwrap();
            slot.collecting = false;
            slot.result.take()
        };
        shared.waker.take();
        discard(abandoned);
    }
}

struct ReadRequest<B: IoBufMut> {
    identifier: u64,
    permit: Option<Permit>,
    socket: Option<RegisteredSocket>,
    buffer: Option<B>,
    reply: Cell<Option<Arc<Shared>>>,
    cancelling: Cell<bool>,
}
impl<B: IoBufMut> PendingRequest for ReadRequest<B> {
    fn identifier(&self) -> u64 {
        self.identifier
    }
    fn entry(&mut self) -> squeue::Entry {
        let buffer = self.buffer.as_mut().unwrap();
        let length = buffer.as_ref().len().min(i32::MAX as usize) as u32;
        opcode::Recv::new(
            types::Fd(self.socket.as_ref().unwrap().get_ref().as_raw_fd()),
            buffer.as_mut().as_mut_ptr(),
            length,
        )
        .build()
        .user_data(self.identifier)
    }
    fn finish(mut self: Box<Self>, result: io::Result<usize>) {
        let shared = self.reply.take().unwrap();
        drop(self.socket.take());
        drop(self.permit.take());
        shared.publish(Completion::Request(result, self));
    }
    fn begin_cancellation(&self) -> bool {
        !self.cancelling.replace(true)
    }
    fn take_reply(&self) -> Option<Box<dyn Send>> {
        self.reply
            .take()
            .map(|shared| Box::new(Failure(shared)) as Box<dyn Send>)
    }
}
impl<B: IoBufMut> Drop for ReadRequest<B> {
    fn drop(&mut self) {
        drop(self.socket.take());
        drop(self.permit.take());
        if let Some(shared) = self.reply.take() {
            shared.publish(Completion::Rejected(Box::new(self.buffer.take().unwrap())));
        }
    }
}
struct Failure(Arc<Shared>);
impl Drop for Failure {
    fn drop(&mut self) {
        self.0.publish(Completion::Failed);
    }
}

impl State {
    async fn attempt<B: IoBufMut>(
        &mut self,
        selector: &IoUringSelector,
        controller: Arc<Controller>,
        socket: RegisteredSocket,
        buffer: B,
        cancellation: &Cancellation,
    ) -> RawBufferResult<B> {
        if let Err(error) = selector.check_open() {
            return (Err(error), buffer);
        }
        let permit = match controller.capacity.try_acquire(cancellation) {
            Ok(Some(permit)) => permit,
            Ok(None) => match controller.capacity.acquire(cancellation).await {
                Ok(permit) => permit,
                Err(error) => return (Err(error), buffer),
            },
            Err(error) => return (Err(error), buffer),
        };
        let identifier = match controller.next_identifier() {
            Ok(identifier) => identifier,
            Err(error) => return (Err(error), buffer),
        };
        let mut request = self
            .cached
            .take()
            .and_then(|cached| cached.downcast::<ReadRequest<B>>().ok())
            .unwrap_or_else(|| {
                Box::new(ReadRequest {
                    identifier,
                    permit: None,
                    socket: None,
                    buffer: None,
                    reply: Cell::new(None),
                    cancelling: Cell::new(false),
                })
            });
        request.identifier = identifier;
        request.permit = Some(permit);
        request.socket = Some(socket);
        request.buffer = Some(buffer);
        request.reply.set(Some(Arc::clone(&self.shared)));
        request.cancelling.set(false);
        self.shared.ready.store(false, Ordering::Relaxed);
        self.shared.slot.lock().unwrap().collecting = true;
        let mut completion = CompletionWait {
            cancel: CancelOnDrop {
                controller,
                identifier,
                armed: true,
            },
            shared: Some(&self.shared),
        };
        completion.cancel.controller.submit(request);
        let mut cancelled = pin!(cancellation.cancelled());
        poll_fn(|context| {
            if self.shared.ready.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            self.shared.waker.register(context.waker());
            if self.shared.ready.load(Ordering::Acquire) {
                self.shared.waker.take();
                return Poll::Ready(());
            }
            if completion.cancel.armed && cancelled.as_mut().poll(context).is_ready() {
                completion.cancel.request();
            }
            Poll::Pending
        })
        .await;
        completion.cancel.armed = false;
        let result = {
            let mut slot = self.shared.slot.lock().unwrap();
            slot.collecting = false;
            slot.result.take().unwrap()
        };
        completion.shared = None;
        drop(completion);
        match result {
            Completion::Request(result, cached) => {
                let mut request = cached.downcast::<ReadRequest<B>>().ok().unwrap();
                let buffer = request.buffer.take().unwrap();
                self.cached = Some(request);
                (result, buffer)
            }
            Completion::Rejected(buffer) => (
                Err(io::ErrorKind::BrokenPipe.into()),
                *buffer.downcast::<B>().ok().unwrap(),
            ),
            Completion::Failed => panic!("io_uring selector failed with a read in flight"),
        }
    }

    pub(crate) async fn read<B: IoBufMut>(
        &mut self,
        selector: &IoUringSelector,
        socket: &impl SocketSource,
        mut buffer: B,
        cancellation: &Cancellation,
    ) -> BufferResult<B> {
        let controller = selector.route();
        loop {
            if let Err(error) = cancellation.check() {
                return (Err(error.into()), buffer);
            }
            let (result, returned) = self
                .attempt(
                    selector,
                    Arc::clone(&controller),
                    socket.retain_socket(),
                    buffer,
                    cancellation,
                )
                .await;
            buffer = returned;
            match result {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    let (result, returned) = selector
                        .wait_ready_owned(
                            Arc::clone(&controller),
                            socket,
                            buffer,
                            crate::Interest::Readable,
                            cancellation,
                        )
                        .await;
                    buffer = returned;
                    if let Err(error) = result {
                        return (Err(error), buffer);
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => return (operation_result(result, cancellation), buffer),
            }
        }
    }
}

#[cfg(test)]
mod tests;

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    NativeTask, thread,
    worker::{self, WorkerState},
};
use core_scheduler::{SpawnError, TaskError};
use crossbeam_deque::{Injector, Worker};
use event_listener::Event;
use std::{
    collections::HashSet,
    future::Future,
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

pub(crate) const UNASSIGNED_WORKER: usize = usize::MAX;
pub(crate) struct TaskState {
    pub(crate) worker: AtomicUsize,
}
pub(crate) type Runnable = async_task::Runnable<TaskState>;
pub(crate) struct State {
    closed: bool,
    pub(crate) next: usize,
    pub(crate) registered: HashSet<usize>,
}
pub(crate) struct Shared {
    pub(crate) state: Mutex<State>,
    pub(crate) ready: Injector<Runnable>,
    pub(crate) workers: Vec<WorkerState>,
    closed: AtomicBool,
    pub(crate) idle_workers: AtomicUsize,
    remaining: AtomicUsize,
    outstanding: AtomicUsize,
    changed: Event,
}
pub(crate) struct TaskService {
    pub(crate) shared: Arc<Shared>,
}
struct Registration {
    shared: Arc<Shared>,
    id: usize,
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.shared
            .state
            .lock()
            .unwrap()
            .registered
            .remove(&self.id);
        self.shared.remaining.fetch_sub(1, Ordering::AcqRel);
        self.shared.changed.notify(usize::MAX);
        if self.shared.closed.load(Ordering::Acquire) {
            self.shared.wake_all();
        }
    }
}

// async-task aborts if its future or stored output destructor unwinds. Keep both user-owned values behind guards, including when the task is abandoned.
struct TaskFuture<F: Future> {
    future: Option<Pin<Box<F>>>,
    _registration: Registration,
}
impl<F: Future> Unpin for TaskFuture<F> {}
impl<F: Future> Future for TaskFuture<F> {
    type Output = TaskOutcome<F::Output>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut result = match catch_unwind(AssertUnwindSafe(|| {
            this.future.as_mut().unwrap().as_mut().poll(context)
        })) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(output)) => Ok(output),
            Err(payload) => Err(TaskError::Panicked(payload)),
        };
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(this.future.take()))) {
            crate::panic::discard(result);
            result = Err(TaskError::Panicked(payload));
        }
        Poll::Ready(TaskOutcome(Some(result)))
    }
}
impl<F: Future> Drop for TaskFuture<F> {
    fn drop(&mut self) {
        crate::panic::discard(self.future.take());
    }
}
pub(crate) struct TaskOutcome<T>(pub(crate) Option<Result<T, TaskError>>);
impl<T> Drop for TaskOutcome<T> {
    fn drop(&mut self) {
        crate::panic::discard(self.0.take());
    }
}

impl Shared {
    pub(crate) fn new(queues: &[Worker<Runnable>]) -> Self {
        Self {
            state: Mutex::new(State {
                closed: false,
                next: 0,
                registered: HashSet::new(),
            }),
            ready: Injector::new(),
            workers: queues
                .iter()
                .map(|queue| WorkerState::new(queue.stealer()))
                .collect(),
            closed: AtomicBool::new(false),
            idle_workers: AtomicUsize::new(0),
            remaining: AtomicUsize::new(0),
            outstanding: AtomicUsize::new(0),
            changed: Event::new(),
        }
    }

    pub(crate) fn finished(&self) -> bool {
        self.closed.load(Ordering::Acquire)
            && self.remaining.load(Ordering::Acquire) == 0
            && self.outstanding.load(Ordering::Acquire) == 0
    }
    pub(crate) fn wake_all(&self) {
        for worker in &self.workers {
            worker.wake();
        }
    }
    pub(crate) fn wake_idle(&self, preferred: usize) {
        std::sync::atomic::fence(Ordering::SeqCst);
        if self.idle_workers.load(Ordering::SeqCst) == 0 {
            return;
        }
        if let Some(worker) = self.workers.get(preferred) {
            if worker.wake() {
                return;
            }
        }
        for worker in &self.workers {
            if worker.wake() {
                return;
            }
        }
    }
    pub(crate) fn retire_runnable(&self) {
        self.outstanding.fetch_sub(1, Ordering::AcqRel);
        self.changed.notify(usize::MAX);
        if self.finished() {
            self.wake_all();
        }
    }
}
impl TaskService {
    pub(crate) fn new(threads: usize) -> io::Result<Self> {
        let queues: Vec<_> = (0..threads).map(|_| Worker::new_fifo()).collect();
        let service = Self {
            shared: Arc::new(Shared::new(&queues)),
        };
        for (identifier, queue) in queues.into_iter().enumerate() {
            let shared = Arc::clone(&service.shared);
            thread::start("core-native-task", move || {
                worker::run(shared, identifier, queue)
            })?;
        }
        Ok(service)
    }
    pub(crate) fn spawn<F>(&self, future: F) -> Result<NativeTask<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let mut state = self.shared.state.lock().unwrap();
        if state.closed {
            drop(state);
            return Err(SpawnError::SchedulerClosed);
        }
        let Some(next) = state.next.checked_add(1) else {
            drop(state);
            return Err(SpawnError::IdentifiersExhausted);
        };
        let id = state.next;
        state.next = next;
        state.registered.insert(id);
        self.shared.remaining.fetch_add(1, Ordering::Release);
        let registration = Registration {
            shared: Arc::clone(&self.shared),
            id,
        };
        let shared = Arc::clone(&self.shared);
        let (runnable, task) = async_task::Builder::new()
            .metadata(TaskState {
                worker: AtomicUsize::new(UNASSIGNED_WORKER),
            })
            .spawn(
                move |_| TaskFuture {
                    future: Some(Box::pin(future)),
                    _registration: registration,
                },
                move |runnable| {
                    shared.outstanding.fetch_add(1, Ordering::AcqRel);
                    worker::schedule(&shared, runnable);
                },
            );
        drop(state);
        runnable.schedule();
        Ok(NativeTask(task))
    }
    pub(crate) fn close(&self) {
        self.shared.state.lock().unwrap().closed = true;
        self.shared.closed.store(true, Ordering::Release);
        self.shared.wake_all();
    }
    pub(crate) async fn drain(&self) {
        self.wait(true).await;
    }
    pub(crate) async fn wait(&self, all: bool) {
        loop {
            event_listener::listener!(self.shared.changed => listener);
            if self.shared.outstanding.load(Ordering::Acquire) == 0
                && (!all || self.shared.remaining.load(Ordering::Acquire) == 0)
            {
                return;
            }
            listener.await;
        }
    }
}
impl Drop for TaskService {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests;

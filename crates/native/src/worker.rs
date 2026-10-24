// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::task_service::Runnable;
use crate::task_service::Shared;
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::thread::{self, Thread};
use std::{task::Waker, time::Duration};

pub(crate) struct WorkerState {
    pub(crate) incoming: Injector<Runnable>,
    pub(crate) timers: crossbeam_queue::SegQueue<crate::worker_timers::Command>,
    stealers: [Stealer<Runnable>; 2],
    sleeping: AtomicBool,
    thread: OnceLock<Thread>,
    driver_waker: OnceLock<Waker>,
}

impl WorkerState {
    pub(crate) fn new(stealers: [Stealer<Runnable>; 2]) -> Self {
        Self {
            incoming: Injector::new(),
            timers: crossbeam_queue::SegQueue::new(),
            stealers,
            sleeping: AtomicBool::new(false),
            thread: OnceLock::new(),
            driver_waker: OnceLock::new(),
        }
    }

    pub(crate) fn wake(&self) -> bool {
        if self.sleeping.swap(false, Ordering::SeqCst) {
            let target = self
                .thread
                .get()
                .expect("sleeping worker has registered its thread");
            // A completion published by this worker makes its driver turn return. Interrupt only another thread; a self-notification would leave a stale permit for the next idle turn.
            if target.id() != thread::current().id() {
                target.unpark();
                if let Some(waker) = self.driver_waker.get() {
                    waker.wake_by_ref();
                }
            }
            true
        } else {
            false
        }
    }
}

struct LocalWorker {
    identifier: usize,
    scheduler: Weak<Shared>,
    ready: Worker<Runnable>,
    current: Worker<Runnable>,
    timers: core_scheduler_timers::local::TimerQueue<Waker>,
}

thread_local! {
    static CURRENT_WORKER: RefCell<Option<LocalWorker>> = const { RefCell::new(None) };
}

pub(crate) fn is_worker() -> bool {
    CURRENT_WORKER.with(|current| current.borrow().is_some())
}

pub(crate) fn assert_outside_worker() {
    assert!(
        !is_worker(),
        "cannot synchronously wait on a native scheduler worker; use .await"
    );
}

// Callbacks never execute user code while borrowing the worker. Return displaced wakers and acknowledgments for delivery after this borrow ends.
pub(crate) fn timers<T>(
    shared: &Arc<Shared>,
    owner: Option<usize>,
    apply: impl FnOnce(usize, &mut core_scheduler_timers::local::TimerQueue<Waker>) -> T,
) -> Option<T> {
    CURRENT_WORKER.with(|current| {
        let mut current = current.borrow_mut();
        let local = current.as_mut()?;
        if !std::ptr::eq(local.scheduler.as_ptr(), Arc::as_ptr(shared))
            || owner.is_some_and(|owner| owner != local.identifier)
        {
            return None;
        }
        Some(apply(local.identifier, &mut local.timers))
    })
}

pub(crate) fn schedule(shared: &Arc<Shared>, runnable: Runnable) {
    let preferred = runnable.metadata().worker.load(Ordering::Relaxed);
    let remote = CURRENT_WORKER.with(|current| {
        let current = current.borrow();
        if let Some(worker) = current.as_ref() {
            if std::ptr::eq(worker.scheduler.as_ptr(), Arc::as_ptr(shared))
                && preferred == worker.identifier
            {
                worker.ready.push(runnable);
                return None;
            }
        }
        Some(runnable)
    });
    if let Some(runnable) = remote {
        shared.workers[preferred].incoming.push(runnable);
    }
    // Publication precedes the sleeping flag exchange. A worker publishes its sleeping flag before rechecking queues, so a racing wake is retained.
    shared.wake_idle(preferred);
}

fn search(shared: &Shared, local: &LocalWorker, iteration: usize) -> Steal<Runnable> {
    let incoming = &shared.workers[local.identifier].incoming;
    let mut retry = false;
    // Admit external wakes at each batch boundary, even while local tasks keep waking themselves.
    if !incoming.is_empty() {
        let _ = take_stolen(incoming.steal_batch(&local.ready), &mut retry);
    }
    if let Some(runnable) = local.ready.pop() {
        return Steal::Success(runnable);
    }
    // Only steal after exhausting this worker's own work. Rotate the first victim so idle workers do not always contend on worker zero.
    for offset in 0..shared.workers.len() {
        let victim = local
            .identifier
            .wrapping_add(iteration)
            .wrapping_add(offset)
            % shared.workers.len();
        if victim == local.identifier {
            continue;
        }
        let worker = &shared.workers[victim];
        // Both generations remain stealable while their owner swaps their roles.
        for stealer in &worker.stealers {
            if let Some(runnable) =
                take_stolen(stealer.steal_batch_and_pop(&local.ready), &mut retry)
            {
                return Steal::Success(runnable);
            }
        }
        // Remote wakes remain stealable while the previous worker is busy.
        if let Some(runnable) = take_stolen(
            worker.incoming.steal_batch_and_pop(&local.ready),
            &mut retry,
        ) {
            return Steal::Success(runnable);
        }
    }
    search_result(retry)
}

fn search_result(retry: bool) -> Steal<Runnable> {
    if retry { Steal::Retry } else { Steal::Empty }
}

fn take_stolen<Value>(result: Steal<Value>, retry: &mut bool) -> Option<Value> {
    match result {
        Steal::Success(value) => Some(value),
        Steal::Retry => {
            *retry = true;
            None
        }
        Steal::Empty => None,
    }
}

fn yield_on_retry(result: Steal<Runnable>) -> Steal<Runnable> {
    if matches!(result, Steal::Retry) {
        thread::yield_now();
    }
    result
}

fn finish_idle_search(
    worker: &WorkerState,
    idle_workers: &std::sync::atomic::AtomicUsize,
    result: Steal<Runnable>,
    should_park: impl FnOnce() -> bool,
    park: impl FnOnce(),
) -> Option<Runnable> {
    let runnable = match result {
        Steal::Success(runnable) => Some(runnable),
        Steal::Retry => None,
        Steal::Empty => {
            if should_park() {
                park();
            }
            None
        }
    };
    worker.sleeping.store(false, Ordering::SeqCst);
    idle_workers.fetch_sub(1, Ordering::SeqCst);
    runnable
}

fn find_work(shared: &Shared, iteration: usize) -> Steal<Runnable> {
    CURRENT_WORKER.with(|current| {
        search(
            shared,
            current
                .borrow()
                .as_ref()
                .expect("worker context is installed"),
            iteration,
        )
    })
}

fn drive(
    driver: &mut Option<&mut dyn core_scheduler_selector::SelectorWorker>,
    timeout: Option<Duration>,
) {
    if let Some(active) = driver.as_mut() {
        if !matches!(active.turn(timeout), Ok(false)) {
            *driver = None;
        }
    } else {
        match timeout {
            Some(timeout) if !timeout.is_zero() => thread::park_timeout(timeout),
            Some(_) => {}
            None => thread::park(),
        }
    }
}

pub(crate) fn run(shared: Arc<Shared>, identifier: usize, queues: [Worker<Runnable>; 2]) {
    run_loop(shared, identifier, queues, None);
}

pub(crate) fn run_driven<D: core_scheduler_selector::SelectorDriver>(
    shared: Arc<Shared>,
    identifier: usize,
    queues: [Worker<Runnable>; 2],
    driver: D,
) {
    use core_scheduler_selector::SelectorWorker;
    let mut driver = driver.enter();
    shared.workers[identifier]
        .driver_waker
        .set(driver.waker())
        .expect("worker driver registered once");
    run_loop(shared, identifier, queues, Some(&mut driver));
}

fn run_loop(
    shared: Arc<Shared>,
    identifier: usize,
    [ready, current]: [Worker<Runnable>; 2],
    mut driver: Option<&mut dyn core_scheduler_selector::SelectorWorker>,
) {
    let worker = &shared.workers[identifier];
    worker
        .thread
        .set(thread::current())
        .expect("worker thread registered once");
    CURRENT_WORKER.with(|context| {
        context.replace(Some(LocalWorker {
            identifier,
            scheduler: Arc::downgrade(&shared),
            ready,
            current,
            timers: core_scheduler_timers::local::TimerQueue::with_capacity(shared.timer_capacity),
        }))
    });
    let mut iteration = 0usize;
    loop {
        iteration = iteration.wrapping_add(1);
        crate::worker_timers::turn(&shared, identifier);
        if shared.finished() {
            // Closure may have followed the open timer pass above. Notify local waiters before retiring this worker, even when no task futures remain.
            crate::worker_timers::turn(&shared, identifier);
            if let Some(active) = driver.as_mut() {
                active.close();
                drive(&mut driver, None);
                continue;
            }
            break;
        }
        let runnable = match find_work(&shared, iteration) {
            Steal::Empty => {
                worker.sleeping.store(true, Ordering::SeqCst);
                shared.idle_workers.fetch_add(1, Ordering::SeqCst);
                // Pair with the enqueuer's fence: either this search observes its queued work, or the enqueuer observes an idle worker.
                std::sync::atomic::fence(Ordering::SeqCst);
                // Recheck after publishing the sleeping flag. An unpark token remains available if work arrives immediately before park().
                finish_idle_search(
                    worker,
                    &shared.idle_workers,
                    find_work(&shared, iteration),
                    || !shared.finished(),
                    || {
                        drive(
                            &mut driver,
                            crate::worker_timers::timeout(&shared, identifier),
                        )
                    },
                )
            }
            result => yield_on_retry(result).success(),
        };
        let Some(runnable) = runnable else {
            continue;
        };
        run_ready_batch(&shared, runnable);
        // Publish the available completion batch before starting the next task batch.
        drive(&mut driver, Some(Duration::ZERO));
    }
    CURRENT_WORKER.with(|current| current.take());
}

#[inline(always)]
fn run_ready_batch(shared: &Shared, first: Runnable) {
    // Freeze the batch. Wakes during task polling go into the now-empty next queue.
    let (identifier, has_more) = CURRENT_WORKER.with(|current| {
        let mut current = current.borrow_mut();
        let local = current.as_mut().expect("worker context is installed");
        std::mem::swap(&mut local.ready, &mut local.current);
        (local.identifier, !local.current.is_empty())
    });
    if has_more {
        shared.wake_idle(usize::MAX);
    }
    let mut runnable = Some(first);
    while let Some(task) = runnable {
        task.metadata().worker.store(identifier, Ordering::Relaxed);
        // Contain task-machinery panics as well as the guarded future's panics.
        crate::panic::contain(|| {
            task.run();
        });
        shared.retire_runnable();
        runnable = CURRENT_WORKER.with(|current| {
            current
                .borrow()
                .as_ref()
                .expect("worker context is installed")
                .current
                .pop()
        });
    }
}

#[cfg(test)]
mod tests;

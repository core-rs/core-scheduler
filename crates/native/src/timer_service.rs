// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{NativeScheduler, thread};
use core_scheduler::{Cancellation, Clock, OperationResult};
use core_scheduler_timers::sharded::{
    SleepError, SleepWake, TimerAssist, TimerWaiter, WorkerTimers,
};
use event_listener::Event;
use std::{
    future::{Future, poll_fn},
    io,
    pin::pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
    time::Instant,
};

struct State {
    owner: Option<WorkerTimers<SleepWake>>,
    #[cfg(test)]
    paused: Option<Instant>,
    #[cfg(test)]
    waits: Option<std::sync::mpsc::Sender<bool>>,
}
impl State {
    fn now(&self) -> Instant {
        #[cfg(test)]
        if let Some(now) = self.paused {
            return now;
        }
        Instant::now()
    }
}
struct Shared {
    state: Mutex<State>,
    running: AtomicBool,
    closing: AtomicBool,
    changed: Event,
}
pub(crate) struct TimerService {
    shared: Arc<Shared>,
    clock: TimerAssist<SleepWake>,
    started: Mutex<bool>,
    capacity: usize,
}
impl TimerService {
    pub(crate) fn new(capacity: usize) -> Self {
        let owner = WorkerTimers::with_capacity(capacity);
        Self {
            clock: owner.assistance(),
            capacity: owner.capacity(),
            started: Mutex::new(false),
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    owner: Some(owner),
                    #[cfg(test)]
                    paused: None,
                    #[cfg(test)]
                    waits: None,
                }),
                running: AtomicBool::new(false),
                closing: AtomicBool::new(false),
                changed: Event::new(),
            }),
        }
    }
    fn start(&self) -> io::Result<()> {
        // Paused clocks use the same queue, driven explicitly by the conformance fixture.
        #[cfg(test)]
        if self.shared.state.lock().unwrap().paused.is_some() {
            return Ok(());
        }
        let mut started = self.started.lock().unwrap();
        if self.shared.state.lock().unwrap().owner.is_none() {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "clock is closed"));
        }
        if !*started {
            let shared = Arc::clone(&self.shared);
            let clock = self.clock.clone();
            let capacity = self.capacity;
            shared.running.store(true, Ordering::Release);
            if let Err(error) =
                thread::start("core-native-timer", move || work(shared, clock, capacity))
            {
                self.shared.running.store(false, Ordering::Release);
                self.shared.changed.notify(usize::MAX);
                return Err(error);
            }
            *started = true;
        }
        Ok(())
    }
    pub(crate) fn now(&self) -> Instant {
        self.shared.state.lock().unwrap().now()
    }
    pub(crate) async fn sleep(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        if self.now() >= deadline {
            return Ok(());
        }
        self.start()?;
        let mut sleep = pin!(self.clock.sleep_until(deadline));
        let mut cancelled = pin!(cancellation.cancelled());
        poll_fn(|context| {
            // Expiry observed after the first cancellation check wins concurrent cancellation.
            if self.now() >= deadline {
                return Poll::Ready(Ok(()));
            }
            if cancelled.as_mut().poll(context).is_ready() {
                return Poll::Ready(Err(core_scheduler::Cancelled.into()));
            }
            sleep.as_mut().poll(context).map(|result| {
                result.map_err(|error| {
                    io::Error::new(
                        match error {
                            SleepError::Full => io::ErrorKind::WouldBlock,
                            SleepError::Closed => io::ErrorKind::BrokenPipe,
                        },
                        error,
                    )
                    .into()
                })
            })
        })
        .await
    }
    pub(crate) fn close(&self) {
        let owner = {
            let mut state = self.shared.state.lock().unwrap();
            let owner = state.owner.take();
            if owner.is_some() {
                self.shared.closing.store(true, Ordering::Release);
            }
            owner
        };
        if let Some(owner) = owner {
            // Queue closure publishes the wakeup before the driver can park. Wakers run outside both locks. Concurrent shutdowns also wait for this delivery.
            crate::panic::discard(owner);
            self.shared.closing.store(false, Ordering::Release);
            self.shared.changed.notify(usize::MAX);
        }
    }
    pub(crate) async fn drain(&self) {
        // Serialize against lazy startup before inspecting its running flag.
        drop(self.started.lock().unwrap());
        loop {
            event_listener::listener!(self.shared.changed => listener);
            if !self.shared.running.load(Ordering::Acquire)
                && !self.shared.closing.load(Ordering::Acquire)
            {
                return;
            }
            listener.await;
        }
    }
}
impl Drop for TimerService {
    fn drop(&mut self) {
        self.close();
    }
}
fn work(shared: Arc<Shared>, clock: TimerAssist<SleepWake>, capacity: usize) {
    let waiter = TimerWaiter::new(&[clock]);
    let mut ready = Vec::with_capacity(capacity);
    loop {
        {
            let mut state = shared.state.lock().unwrap();
            let now = state.now();
            let Some(owner) = state.owner.as_mut() else {
                break;
            };
            owner.poll(now, &mut ready);
            #[cfg(test)]
            {
                let deadline =
                    !ready.is_empty() || state.owner.as_mut().unwrap().next_deadline().is_some();
                tests::observe_wait(&state, deadline);
            }
        }
        if ready.is_empty() {
            waiter.park();
        } else {
            for wake in ready.drain(..) {
                crate::panic::discard(wake);
            }
        }
    }
    shared.running.store(false, Ordering::Release);
    shared.changed.notify(usize::MAX);
}
impl Clock for NativeScheduler {
    fn now(&self) -> Instant {
        self.timers.now()
    }
    async fn sleep_until(
        &self,
        deadline: Instant,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        self.timers.sleep(deadline, cancellation).await
    }
}
#[cfg(test)]
mod tests;

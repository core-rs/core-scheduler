// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

//! A scheduler-local retry pulse, not a reactor. Waiting sockets occupy no worker or admission slot.

use core_scheduler::{Cancellation, OperationResult};
use event_listener::Event;
use std::future::{Future, poll_fn};
use std::io;
use std::pin::pin;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::task::Poll;
use std::thread;
use std::time::Duration;

struct Shared {
    closed: Mutex<bool>,
    work: Condvar,
    retry: Event,
    #[cfg(test)]
    idle: Event,
}

pub(crate) struct SocketRetry {
    shared: Arc<Shared>,
    started: OnceLock<io::Result<()>>,
    #[cfg(test)]
    pub(crate) fail_start: bool,
}

impl SocketRetry {
    pub(crate) fn new() -> Self {
        let retry = Event::new();
        retry.notify(0);
        Self {
            shared: Arc::new(Shared {
                closed: Mutex::new(false),
                work: Condvar::new(),
                retry,
                #[cfg(test)]
                idle: Event::new(),
            }),
            started: OnceLock::new(),
            #[cfg(test)]
            fail_start: false,
        }
    }

    pub(crate) fn check_open(&self) -> io::Result<()> {
        if *self.shared.closed.lock().unwrap() {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "scheduler is closed",
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) async fn wait(&self, cancellation: &Cancellation) -> OperationResult<()> {
        event_listener::listener!(self.shared.retry => retry);
        {
            let closed = self.shared.closed.lock().unwrap();
            cancellation.check()?;
            if *closed {
                return Err(
                    io::Error::new(io::ErrorKind::BrokenPipe, "scheduler is closed").into(),
                );
            }
        }
        self.started
            .get_or_init(|| {
                #[cfg(test)]
                if self.fail_start {
                    return Err(io::Error::other("injected socket retry startup failure"));
                }
                let shared = Arc::clone(&self.shared);
                thread::Builder::new()
                    .name("protocol-direct-socket-retry".into())
                    .spawn(move || work(shared))
                    .map(drop)
            })
            .as_ref()
            .map_err(|error| io::Error::new(error.kind(), error.to_string()))?;
        // Registration precedes notification. The worker checks listeners under this same lock before sleeping indefinitely.
        {
            let _closed = self.shared.closed.lock().unwrap();
            self.shared.work.notify_one();
        }
        let mut cancelled = pin!(cancellation.cancelled());
        let mut retry = pin!(retry);
        poll_fn(|context| {
            if cancelled.as_mut().poll(context).is_ready() {
                return Poll::Ready(Err(core_scheduler::Cancelled.into()));
            }
            retry.as_mut().poll(context).map(|()| Ok(()))
        })
        .await
    }

    pub(crate) fn close(&self) {
        *self.shared.closed.lock().unwrap() = true;
        self.shared.work.notify_all();
        self.shared.retry.notify(usize::MAX);
    }
}

impl Drop for SocketRetry {
    fn drop(&mut self) {
        self.close();
    }
}

fn work(shared: Arc<Shared>) {
    let mut closed = shared.closed.lock().unwrap();
    while !*closed {
        if shared.retry.total_listeners() == 0 {
            #[cfg(test)]
            shared.idle.notify(usize::MAX);
            closed = shared.work.wait(closed).unwrap();
        } else {
            closed = shared
                .work
                .wait_timeout(closed, Duration::from_millis(2))
                .unwrap()
                .0;
            // Wake executor code outside backend locks. Retrying early after a new waiter is harmless.
            drop(closed);
            shared.retry.notify(usize::MAX);
            closed = shared.closed.lock().unwrap();
        }
    }
}

#[cfg(test)]
mod tests;

// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    IoUringDriver,
    io_uring_selector::{Command, Controller, DriverGroup, multishot::pool::Pool},
};
use core_scheduler_selector::SelectorWorker;
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    io,
    rc::Rc,
    sync::Arc,
    task::Waker,
    time::Duration,
};

struct LocalRoute {
    group: Arc<DriverGroup>,
    controller: Arc<Controller>,
    commands: RefCell<VecDeque<Command>>,
    notified: Cell<bool>,
}
impl LocalRoute {
    fn recycle(&self, pool: &Pool, buffer: u16) {
        // This thread exclusively owns the provided ring. Returning a lease needs no queued work while its receive remains armed.
        pool.recycle(buffer);
        if pool.needs_rearm() {
            self.commands
                .borrow_mut()
                .push_back(Command::Rearm(pool.identifier));
        }
    }
}

thread_local! {
    static ROUTES: RefCell<Vec<Rc<LocalRoute>>> = const { RefCell::new(Vec::new()) };
}

/// A thread-bound ring driver with a local submission queue.
///
/// Created by `SelectorDriver::enter`. This owner cannot move between threads. Local routing applies only to the explicitly supplied selector; tasks and operation futures retain no thread-local references. Nested workers restore their previous routing when dropped.
pub struct IoUringWorker {
    driver: IoUringDriver,
    route: Rc<LocalRoute>,
}
impl IoUringWorker {
    pub(crate) fn new(mut driver: IoUringDriver) -> Self {
        if let Some(reactor) = driver.reactor.as_mut() {
            reactor.bind_worker();
        }
        let route = Rc::new(LocalRoute {
            group: Arc::clone(&driver.group),
            controller: Arc::clone(&driver.controller),
            commands: RefCell::new(VecDeque::new()),
            notified: Cell::new(false),
        });
        ROUTES.with(|routes| routes.borrow_mut().push(Rc::clone(&route)));
        Self { driver, route }
    }
}
impl SelectorWorker for IoUringWorker {
    fn waker(&self) -> Waker {
        self.driver.waker()
    }
    fn close(&mut self) {
        self.driver.group.close();
    }
    fn turn(&mut self, timeout: Option<Duration>) -> io::Result<bool> {
        self.driver.turn(timeout)
    }
}
impl Drop for IoUringWorker {
    fn drop(&mut self) {
        // Stop while the local route remains installed, so queued owners retire before the driver's shutdown notification. Reactor teardown invokes callbacks without retaining TLS or queue borrows.
        self.driver.stop();
        ROUTES.with(|routes| {
            routes
                .borrow_mut()
                .retain(|route| !Rc::ptr_eq(route, &self.route))
        });
    }
}

pub(crate) fn route_index(group: &Arc<DriverGroup>) -> Option<usize> {
    ROUTES.with(|routes| {
        routes
            .borrow()
            .iter()
            .rev()
            .find(|route| Arc::ptr_eq(&route.group, group))
            .map(|route| route.controller.worker)
    })
}
pub(crate) fn enqueue(controller: &Controller, command: Command) -> Result<(), Command> {
    ROUTES.with(|routes| {
        let routes = routes.borrow();
        if let Some(route) = routes
            .iter()
            .rev()
            .find(|route| std::ptr::eq(Arc::as_ptr(&route.controller), controller))
        {
            route.commands.borrow_mut().push_back(command);
            Ok(())
        } else {
            Err(command)
        }
    })
}
pub(crate) fn recycle(pool: &Pool, buffer: u16) -> bool {
    ROUTES
        .try_with(|routes| {
            let routes = routes.borrow();
            if let Some(route) = routes.iter().rev().find(|route| {
                std::ptr::eq(Arc::as_ptr(&route.controller), pool.controller.as_ptr())
            }) {
                route.recycle(pool, buffer);
                true
            } else {
                false
            }
        })
        .unwrap_or(false)
}

pub(crate) fn pop(controller: &Controller) -> Option<Command> {
    ROUTES.with(|routes| {
        routes
            .borrow()
            .iter()
            .rev()
            .find(|route| std::ptr::eq(Arc::as_ptr(&route.controller), controller))
            .and_then(|route| route.commands.borrow_mut().pop_front())
    })
}

pub(crate) fn notify(controller: &Controller) -> bool {
    ROUTES.with(|routes| {
        let routes = routes.borrow();
        if let Some(route) = routes
            .iter()
            .rev()
            .find(|route| std::ptr::eq(Arc::as_ptr(&route.controller), controller))
        {
            route.notified.set(true);
            true
        } else {
            false
        }
    })
}
pub(crate) fn take_notification(controller: &Controller) -> bool {
    ROUTES.with(|routes| {
        routes
            .borrow()
            .iter()
            .rev()
            .find(|route| std::ptr::eq(Arc::as_ptr(&route.controller), controller))
            .is_some_and(|route| route.notified.replace(false))
    })
}

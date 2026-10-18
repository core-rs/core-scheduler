// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::test_support;
use core_scheduler::{SocketDomain, SocketOpen, SocketType};
use core_scheduler_conformance::{SocketOptionFixture, SocketRegistrationFixture};
use futures_lite::future::block_on;
use std::sync::atomic::Ordering;

struct Fixture<O> {
    scheduler: NativeScheduler,
    socket: NativeSocket,
    failing: NativeSocket,
    option: O,
    get: fn(&Socket) -> io::Result<bool>,
    set: fn(&Socket, bool) -> io::Result<()>,
}
impl<O: NativeOption<Value = bool> + Copy> SocketOptionFixture for Fixture<O> {
    type Scheduler = NativeScheduler;
    type Option = O;
    fn scheduler(&self) -> &NativeScheduler {
        &self.scheduler
    }
    fn socket(&self) -> &NativeSocket {
        &self.socket
    }
    fn failing_socket(&self) -> &NativeSocket {
        &self.failing
    }
    fn error_kind(&self) -> io::ErrorKind {
        io::ErrorKind::PermissionDenied
    }
    fn option(&self) -> O {
        self.option
    }
    fn values(&self) -> [bool; 2] {
        [false, true]
    }
    fn get_native(&self) -> bool {
        (self.get)(self.socket.get_ref()).unwrap()
    }
    fn set_native(&self, value: bool) {
        (self.set)(self.socket.get_ref(), value).unwrap();
    }
    fn calls(&self) -> usize {
        self.scheduler.option_probe.calls.load(Ordering::SeqCst)
    }
}
fn run_option<O: NativeOption<Value = bool> + Copy + Send + 'static>(
    option: O,
    get: fn(&Socket) -> io::Result<bool>,
    set: fn(&Socket, bool) -> io::Result<()>,
    test: impl AsyncFnOnce(&Fixture<O>) + Send + 'static,
) {
    test_support::run(move || {
        let scheduler = test_support::scheduler();
        let socket = NativeSocket::new(
            crate::socket_io::open(SocketDomain::Ipv4, SocketType::Stream, None).unwrap(),
        )
        .unwrap();
        let failing = NativeSocket::new(
            crate::socket_io::open(SocketDomain::Ipv4, SocketType::Stream, None).unwrap(),
        )
        .unwrap();
        scheduler.option_probe.failing.store(
            failing.get_ref() as *const Socket as usize,
            Ordering::SeqCst,
        );
        let fixture = Fixture {
            scheduler,
            socket,
            failing,
            option,
            get,
            set,
        };
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&fixture))));
        block_on(fixture.scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
fn run_nodelay(test: impl AsyncFnOnce(&Fixture<TcpNoDelay>) + Send + 'static) {
    run_option(
        TcpNoDelay,
        Socket::tcp_nodelay,
        Socket::set_tcp_nodelay,
        test,
    );
}
fn run_keepalive(test: impl AsyncFnOnce(&Fixture<SocketKeepalive>) + Send + 'static) {
    run_option(
        SocketKeepalive,
        Socket::keepalive,
        Socket::set_keepalive,
        test,
    );
}
fn run_reuse(test: impl AsyncFnOnce(&Fixture<SocketReuseAddress>) + Send + 'static) {
    run_option(
        SocketReuseAddress,
        Socket::reuse_address,
        Socket::set_reuse_address,
        test,
    );
}
mod nodelay_get {
    core_scheduler_conformance::socket_get_option_tests!(run: super::run_nodelay);
}
mod nodelay_set {
    core_scheduler_conformance::socket_set_option_tests!(run: super::run_nodelay);
}
mod keepalive_get {
    core_scheduler_conformance::socket_get_option_tests!(run: super::run_keepalive);
}
mod keepalive_set {
    core_scheduler_conformance::socket_set_option_tests!(run: super::run_keepalive);
}
mod reuse_get {
    core_scheduler_conformance::socket_get_option_tests!(run: super::run_reuse);
}
mod reuse_set {
    core_scheduler_conformance::socket_set_option_tests!(run: super::run_reuse);
}

struct RegistrationFixture {
    scheduler: NativeScheduler,
    domain: SocketDomain,
}
impl SocketRegistrationFixture for RegistrationFixture {
    type Scheduler = NativeScheduler;
    fn scheduler(&self) -> &NativeScheduler {
        &self.scheduler
    }
    fn domain(&self) -> SocketDomain {
        self.domain
    }
}
fn run_registration(
    domain: SocketDomain,
    test: impl AsyncFnOnce(&RegistrationFixture) + Send + 'static,
) {
    test_support::run(move || {
        let fixture = RegistrationFixture {
            scheduler: test_support::scheduler(),
            domain,
        };
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&fixture))));
        block_on(fixture.scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
fn run_ipv4(test: impl AsyncFnOnce(&RegistrationFixture) + Send + 'static) {
    run_registration(SocketDomain::Ipv4, test);
}
fn run_ipv6(test: impl AsyncFnOnce(&RegistrationFixture) + Send + 'static) {
    run_registration(SocketDomain::Ipv6, test);
}
mod ipv4 {
    core_scheduler_conformance::socket_registration_tests!(run: super::run_ipv4);
}
mod ipv6 {
    core_scheduler_conformance::socket_registration_tests!(run: super::run_ipv6);
}

#[test]
fn imported_resources_are_nonblocking_and_closed_admission_is_rejected() {
    let scheduler = test_support::scheduler();
    let native = TcpListener::bind("127.0.0.1:0").unwrap();
    let listener = scheduler.register_listener(native).unwrap();
    assert_eq!(
        listener.get_ref().accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    scheduler.close();
    assert!(
        scheduler
            .register_listener(TcpListener::bind("127.0.0.1:0").unwrap())
            .is_err()
    );
    block_on(scheduler.shutdown());
}
#[test]
fn actual_option_syscall_errors_are_preserved() {
    test_support::run(|| {
        block_on(async {
            let scheduler = test_support::scheduler();
            let socket = scheduler
                .socket_open(
                    SocketDomain::Ipv4,
                    SocketType::Datagram,
                    None,
                    &Cancellation::never(),
                )
                .await
                .unwrap();
            let expected = socket.get_ref().tcp_nodelay().unwrap_err();
            let error = scheduler
                .socket_get_option(&socket, TcpNoDelay, &Cancellation::never())
                .await
                .unwrap_err();
            let core_scheduler::OperationError::Io(error) = error else {
                panic!("lost syscall error")
            };
            assert_eq!(error.raw_os_error(), expected.raw_os_error());
            let expected = socket.get_ref().set_tcp_nodelay(true).unwrap_err();
            let error = scheduler
                .socket_set_option(&socket, TcpNoDelay, true, &Cancellation::never())
                .await
                .unwrap_err();
            let core_scheduler::OperationError::Io(error) = error else {
                panic!("lost syscall error");
            };
            assert_eq!(error.raw_os_error(), expected.raw_os_error());
            scheduler.shutdown().await;
        })
    });
}

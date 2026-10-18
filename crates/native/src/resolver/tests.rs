// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::*;
use crate::test_support;
use core_scheduler::{
    AddressInfo, AddressInfoCollection, AddressInfoView, AddressResolveError,
    AddressResolveErrorKind,
};
use core_scheduler_conformance::AddressResolutionFixture;
use futures_lite::future::{block_on, poll_once};

struct Fixture {
    scheduler: NativeScheduler,
    expected: Vec<AddressInfo>,
}
impl AddressResolutionFixture for Fixture {
    type Scheduler = NativeScheduler;
    fn scheduler(&self) -> &NativeScheduler {
        &self.scheduler
    }
    fn name(&self) -> &str {
        "conformance.test"
    }
    fn service_name(&self) -> &str {
        "conformance-http"
    }
    fn expected(&self) -> &[AddressInfo] {
        &self.expected
    }
    fn failure_name(&self) -> &str {
        "failure.test"
    }
    fn assert_error(&self, error: &AddressResolveError) {
        assert_eq!(
            error.kind,
            AddressResolveErrorKind::InvalidHints,
            "{error:?}"
        );
        assert!(error.code.is_some());
        assert!(!error.message.is_empty());
    }
    fn lookups(&self) -> usize {
        self.scheduler.resolver_probe.requests.lock().unwrap().len()
    }
    fn assert_hints(&self, hints: AddressHints) {
        let actual = *self
            .scheduler
            .resolver_probe
            .requests
            .lock()
            .unwrap()
            .last()
            .unwrap();
        // Inspect the actual native argument record, including flags; do not merely compare the trait-method arguments.
        assert_eq!(
            actual[0],
            if hints.domain.is_some() {
                sys::AF_INET as i32
            } else {
                sys::AF_UNSPEC as i32
            }
        );
        assert_eq!(
            actual[1],
            if hints.socket_type.is_some() {
                sys::SOCK_DGRAM as i32
            } else {
                0
            }
        );
        assert_eq!(
            actual[2],
            if hints.protocol.is_some() {
                sys::IPPROTO_UDP as i32
            } else {
                0
            }
        );
        assert_eq!(actual[3] & sys::AI_PASSIVE as i32 != 0, hints.passive);
        assert_eq!(
            actual[3] & sys::AI_NUMERICHOST as i32 != 0,
            hints.numeric_host
        );
        assert_eq!(
            actual[3] & sys::AI_ADDRCONFIG as i32 != 0,
            hints.address_configured
        );
        assert_ne!(actual[3] & sys::AI_NUMERICSERV as i32, 0);
    }
}
fn expected() -> Vec<AddressInfo> {
    // Independently query the same numeric native fixture, then decode parameters without SystemAddressInfo accessors.
    #[cfg(windows)]
    let _winsock = platform::Winsock::new().unwrap();
    let name = platform::string("127.0.0.1");
    let service = platform::string("80");
    let mut head = ptr::null_mut();
    // SAFETY: Strings and writable head storage live through this synchronous numeric lookup; no DNS is involved.
    assert_eq!(
        unsafe { platform::getaddrinfo(name.as_ptr(), service.as_ptr(), ptr::null(), &mut head) },
        0
    );
    let mut result = Vec::new();
    let mut current = head;
    while let Some(entry) = unsafe { current.as_ref() } {
        result.push(AddressInfo {
            address: "127.0.0.1:80".parse().unwrap(),
            socket_type: match entry.ai_socktype {
                value if value == sys::SOCK_STREAM as i32 => SocketType::Stream,
                value if value == sys::SOCK_DGRAM as i32 => SocketType::Datagram,
                value if value == sys::SOCK_RAW as i32 => SocketType::Raw,
                other => SocketType::Other(other),
            },
            protocol: match entry.ai_protocol {
                0 => None,
                value if value == sys::IPPROTO_TCP as i32 => Some(SocketProtocol::Tcp),
                value if value == sys::IPPROTO_UDP as i32 => Some(SocketProtocol::Udp),
                other => Some(SocketProtocol::Other(other)),
            },
        });
        current = entry.ai_next;
    }
    if !head.is_null() {
        unsafe { platform::freeaddrinfo(head) };
    }
    result
}
fn run(test: impl AsyncFnOnce(&Fixture) + Send + 'static) {
    test_support::run(move || {
        let fixture = Fixture {
            scheduler: test_support::scheduler(),
            expected: expected(),
        };
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(test(&fixture))));
        block_on(fixture.scheduler.shutdown());
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    });
}
mod conformance {
    core_scheduler_conformance::address_resolution_tests!(run: super::run);
}
mod ipv4 {
    core_scheduler_conformance::address_resolution_ip_tests!(run: super::run, domain: core_scheduler::SocketDomain::Ipv4);
}
mod ipv6 {
    core_scheduler_conformance::address_resolution_ip_tests!(run: super::run, domain: core_scheduler::SocketDomain::Ipv6);
}

#[test]
fn running_lookup_retains_bounded_capacity_after_cancellation_and_abandonment() {
    test_support::run(|| {
        block_on(async {
            let scheduler = test_support::scheduler();
            let (gate, started, release) = test_support::Gate::new();
            *scheduler.resolver_probe.gate.lock().unwrap() = Some(gate);
            let token = Cancellation::new();
            let mut operation = Box::pin(scheduler.address_resolve(
                Some("127.0.0.1"),
                AddressService::Port(80),
                AddressHints::default(),
                &token,
            ));
            assert!(poll_once(&mut operation).await.is_none());
            started
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            token.cancel();
            assert!(poll_once(&mut operation).await.is_none());
            drop(operation);
            assert!(poll_once(scheduler.work.drain()).await.is_none());
            drop(release);
            scheduler.shutdown().await;
        })
    });
}
#[test]
fn started_lookup_preserves_success_and_native_owner_moves_without_allocating_views() {
    test_support::run(|| {
        block_on(async {
            let scheduler = test_support::scheduler();
            let (gate, started, release) = test_support::Gate::new();
            *scheduler.resolver_probe.gate.lock().unwrap() = Some(gate);
            let token = Cancellation::new();
            let hints = AddressHints {
                domain: Some(SocketDomain::Ipv6),
                socket_type: Some(SocketType::Stream),
                protocol: Some(SocketProtocol::Tcp),
                numeric_host: true,
                ..AddressHints::default()
            };
            let mut operation = Box::pin(scheduler.address_resolve(
                Some("fe80::1%7"),
                AddressService::Port(1234),
                hints,
                &token,
            ));
            assert!(poll_once(&mut operation).await.is_none());
            started
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            token.cancel();
            drop(release);
            let result = operation.await.unwrap();
            let allocations = allocation_counter::measure(|| {
                let entry = result.entries().next().unwrap();
                assert_eq!(
                    entry.address(),
                    "[fe80::1%7]:1234".parse::<std::net::SocketAddr>().unwrap()
                );
                assert_eq!(entry.socket_type(), SocketType::Stream);
                assert_eq!(entry.protocol(), Some(SocketProtocol::Tcp));
            });
            assert_eq!(allocations.count_total, 0);
            std::thread::spawn(move || drop(result)).join().unwrap();
            scheduler.shutdown().await;
        })
    });
}

#[test]
fn native_address_validation_and_unusual_socket_parameters_are_preserved() {
    let probe = Probe::default();
    for hints in [
        AddressHints {
            socket_type: Some(SocketType::Raw),
            ..AddressHints::default()
        },
        AddressHints {
            socket_type: Some(SocketType::Other(-1)),
            protocol: Some(SocketProtocol::Other(123)),
            ..AddressHints::default()
        },
    ] {
        let _ = lookup(Some("127.0.0.1"), "0", hints, true, &probe);
    }
    let mut result = lookup(
        Some("127.0.0.1"),
        "80",
        AddressHints::default(),
        true,
        &probe,
    )
    .unwrap();
    let head = result.head;
    assert!(!head.is_null());
    // SAFETY: The owned list has no outstanding views. Only scalar creation parameters are changed; allocation links and pointers remain untouched for native cleanup.
    unsafe {
        (*head).ai_socktype = 123;
        (*head).ai_protocol = 456;
    }
    let first = result.entries().next().unwrap();
    assert_eq!(first.socket_type(), SocketType::Other(123));
    assert_eq!(first.protocol(), Some(SocketProtocol::Other(456)));
    drop(first);
    unsafe {
        (*head).ai_protocol = 0;
    }
    assert_eq!(result.entries().next().unwrap().protocol(), None);
    unsafe {
        (*head).ai_socktype = sys::SOCK_RAW as i32;
    }
    assert_eq!(
        result.entries().next().unwrap().socket_type(),
        SocketType::Raw
    );
    let mut native: AddrInfo = unsafe { std::mem::zeroed() };
    assert!(address(&native).is_err());
    native.ai_family = sys::AF_INET as i32;
    assert!(address(&native).is_err());
    let mut storage = socket2::SockAddrStorage::zeroed();
    let ipv4 = socket2::SockAddr::from("127.0.0.1:80".parse::<std::net::SocketAddr>().unwrap());
    // SAFETY: The source is a complete IPv4 address and destination is fully initialized storage large enough for either family.
    unsafe {
        ptr::copy_nonoverlapping(
            ipv4.as_ptr().cast::<u8>(),
            (&mut storage as *mut socket2::SockAddrStorage).cast::<u8>(),
            ipv4.len() as usize,
        );
    }
    native.ai_addr = (&mut storage as *mut socket2::SockAddrStorage).cast();
    native.ai_addrlen = std::mem::size_of::<platform::SockAddrIn6>() as _;
    native.ai_family = sys::AF_INET6 as i32;
    assert!(address(&native).is_err());
    let native_head = result.head;
    result.head = ptr::null_mut();
    drop(result);
    // Reject a non-IP native result through the owning validation path. Only the scalar family is changed; allocation pointers and list links remain valid for the matching native deallocator.
    unsafe {
        (*native_head).ai_family = sys::AF_UNSPEC as i32;
    }
    let error = unsafe {
        SystemAddresses::from_native(
            native_head,
            #[cfg(windows)]
            platform::Winsock::new().unwrap(),
        )
    }
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[cfg(unix)]
#[test]
fn native_resolver_error_codes_keep_categories_and_system_errors() {
    use AddressResolveErrorKind as Kind;
    for (code, kind) in [
        (sys::EAI_AGAIN, Kind::TemporaryFailure),
        (sys::EAI_NONAME, Kind::NameNotFound),
        (sys::EAI_SERVICE, Kind::ServiceUnavailable),
        (sys::EAI_BADFLAGS, Kind::InvalidHints),
        (sys::EAI_FAMILY, Kind::InvalidHints),
        (sys::EAI_SOCKTYPE, Kind::InvalidHints),
        (sys::EAI_FAIL, Kind::PermanentFailure),
        (sys::EAI_MEMORY, Kind::OutOfMemory),
        (sys::EAI_SYSTEM, Kind::System),
        (i32::MAX, Kind::Other),
    ] {
        let error = platform::error(code);
        assert_eq!(error.kind, kind);
        assert_eq!(error.code, Some(code));
        assert_eq!(error.system_error.is_some(), code == sys::EAI_SYSTEM);
    }
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    assert_eq!(platform::error(sys::EAI_NODATA).kind, Kind::NameNotFound);
    #[cfg(any(target_os = "freebsd", target_vendor = "apple"))]
    for code in [platform::EAI_BADHINTS, platform::EAI_PROTOCOL] {
        assert_eq!(platform::error(code).kind, Kind::InvalidHints);
    }
}

#[cfg(windows)]
#[test]
fn winsock_startup_and_resolver_errors_are_preserved() {
    use AddressResolveErrorKind as Kind;
    platform::STARTUP_ERROR.set(Some(sys::WSASYSNOTREADY));
    assert_eq!(
        lookup(
            Some("127.0.0.1"),
            "80",
            AddressHints::default(),
            true,
            &Probe::default()
        )
        .unwrap_err()
        .raw_os_error(),
        Some(sys::WSASYSNOTREADY)
    );
    for (code, kind) in [
        (sys::WSATRY_AGAIN, Kind::TemporaryFailure),
        (sys::WSAHOST_NOT_FOUND, Kind::NameNotFound),
        (sys::WSANO_DATA, Kind::NameNotFound),
        (sys::WSATYPE_NOT_FOUND, Kind::ServiceUnavailable),
        (sys::WSAEINVAL, Kind::InvalidHints),
        (sys::WSAEAFNOSUPPORT, Kind::InvalidHints),
        (sys::WSAESOCKTNOSUPPORT, Kind::InvalidHints),
        (sys::WSANO_RECOVERY, Kind::PermanentFailure),
        (sys::WSA_NOT_ENOUGH_MEMORY, Kind::OutOfMemory),
        (i32::MAX, Kind::Other),
    ] {
        assert_eq!(platform::error(code).kind, kind);
    }
}

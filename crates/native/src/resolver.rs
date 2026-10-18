// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![allow(clippy::unnecessary_cast)]

use crate::{NativeScheduler, SystemAddresses};
use core_scheduler::{
    AddressHints, AddressResolution, AddressService, Cancellation, OperationResult, SocketDomain,
    SocketProtocol, SocketType,
};
use std::{io, ptr};

#[cfg(unix)]
#[path = "resolver/unix.rs"]
pub(crate) mod platform;
#[cfg(windows)]
#[path = "resolver/windows.rs"]
pub(crate) mod platform;
pub(crate) use platform::{AddrInfo, sys};

impl AddressResolution for NativeScheduler {
    type Addresses = SystemAddresses;
    async fn address_resolve(
        &self,
        name: Option<&str>,
        service: AddressService<'_>,
        hints: AddressHints,
        cancellation: &Cancellation,
    ) -> OperationResult<Self::Addresses> {
        cancellation.check()?;
        fn validate(name: &str) -> io::Result<()> {
            if name.is_empty() || name.contains('\0') {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "empty or NUL-containing resolver input",
                ))
            } else {
                Ok(())
            }
        }
        if let Some(name) = name {
            validate(name)?;
        }
        let (service, numeric_service) = match service {
            AddressService::Port(port) => (port.to_string(), true),
            AddressService::Name(name) => {
                validate(name)?;
                (name.to_owned(), false)
            }
        };
        let name = name.map(str::to_owned);
        #[cfg(test)]
        let probe = std::sync::Arc::clone(&self.resolver_probe);
        self.work
            .execute((), cancellation, move |_| {
                lookup(
                    name.as_deref(),
                    &service,
                    hints,
                    numeric_service,
                    #[cfg(test)]
                    &probe,
                )
            })
            .await
            .0
    }
}

pub(crate) fn lookup(
    name: Option<&str>,
    service: &str,
    hints: AddressHints,
    numeric_service: bool,
    #[cfg(test)] probe: &Probe,
) -> io::Result<SystemAddresses> {
    #[cfg(windows)]
    let winsock = platform::Winsock::new()?;
    #[cfg(test)]
    let failure = name == Some("failure.test");
    #[cfg(test)]
    let name = match name {
        Some("conformance.test") if !hints.numeric_host => Some("127.0.0.1"),
        Some("failure.test") => Some("127.0.0.1"),
        name => name,
    };
    #[cfg(test)]
    let service = if service == "conformance-http" {
        "80"
    } else {
        service
    };
    let name = name.map(platform::string);
    let service = platform::string(service);
    // SAFETY: A zeroed hints record has null output fields. Populated fields are native numeric parameters.
    let mut native: AddrInfo = unsafe { std::mem::zeroed() };
    native.ai_family = match hints.domain {
        None => sys::AF_UNSPEC as i32,
        Some(SocketDomain::Ipv4) => sys::AF_INET as i32,
        Some(SocketDomain::Ipv6) => sys::AF_INET6 as i32,
    };
    native.ai_socktype = hints.socket_type.map_or(0, |kind| match kind {
        SocketType::Stream => sys::SOCK_STREAM as i32,
        SocketType::Datagram => sys::SOCK_DGRAM as i32,
        SocketType::Raw => sys::SOCK_RAW as i32,
        SocketType::Other(value) => value,
    });
    native.ai_protocol = hints.protocol.map_or(0, |protocol| match protocol {
        SocketProtocol::Tcp => sys::IPPROTO_TCP as i32,
        SocketProtocol::Udp => sys::IPPROTO_UDP as i32,
        SocketProtocol::Other(value) => value,
    });
    #[cfg(test)]
    if failure {
        native.ai_socktype = -1;
    }
    native.ai_flags = flags(hints)
        | if numeric_service {
            sys::AI_NUMERICSERV as i32
        } else {
            0
        };
    #[cfg(test)]
    {
        probe.requests.lock().unwrap().push([
            native.ai_family,
            native.ai_socktype,
            native.ai_protocol,
            native.ai_flags,
        ]);
        let gate = probe.gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.wait();
        }
    }
    let mut head = ptr::null_mut();
    // SAFETY: Inputs are terminated and live through this synchronous call; the output list is immediately owned below.
    let code = unsafe {
        platform::getaddrinfo(
            name.as_ref().map_or(ptr::null(), |name| name.as_ptr()),
            service.as_ptr(),
            &native,
            &mut head,
        )
    };
    if code != 0 {
        return Err(platform::error(code).into());
    }
    // SAFETY: head is the unchanged successful native list. Ownership includes the matching deallocator and, on Windows, Winsock initialization.
    unsafe {
        SystemAddresses::from_native(
            head,
            #[cfg(windows)]
            winsock,
        )
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct Probe {
    pub(crate) requests: std::sync::Mutex<Vec<[i32; 4]>>,
    pub(crate) gate: std::sync::Mutex<Option<std::sync::Arc<crate::test_support::Gate>>>,
}
pub(crate) fn flags(hints: AddressHints) -> i32 {
    (i32::from(hints.passive) * sys::AI_PASSIVE as i32)
        | (i32::from(hints.numeric_host) * sys::AI_NUMERICHOST as i32)
        | (i32::from(hints.address_configured) * sys::AI_ADDRCONFIG as i32)
}

pub(crate) fn address(entry: &AddrInfo) -> io::Result<std::net::SocketAddr> {
    let minimum = match entry.ai_family {
        family if family == sys::AF_INET as i32 => std::mem::size_of::<platform::SockAddrIn>(),
        family if family == sys::AF_INET6 as i32 => std::mem::size_of::<platform::SockAddrIn6>(),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "resolver returned a non-IP address",
            ));
        }
    };
    let length = entry.ai_addrlen as usize;
    if entry.ai_addr.is_null()
        || length < minimum
        || length > std::mem::size_of::<socket2::SockAddrStorage>()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid resolver socket address",
        ));
    }
    let mut storage = socket2::SockAddrStorage::zeroed();
    // SAFETY: Native records own readable ai_addrlen bytes. Bounds above ensure stack storage is large enough; copying does not require source alignment.
    let address = unsafe {
        ptr::copy_nonoverlapping(
            entry.ai_addr.cast::<u8>(),
            (&mut storage as *mut socket2::SockAddrStorage).cast::<u8>(),
            length,
        );
        socket2::SockAddr::new(storage, length as _)
    };
    address
        .as_socket()
        .filter(|address| match address {
            std::net::SocketAddr::V4(_) => entry.ai_family == sys::AF_INET as i32,
            std::net::SocketAddr::V6(_) => entry.ai_family == sys::AF_INET6 as i32,
        })
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "resolver socket family mismatch",
            )
        })
}

#[cfg(test)]
mod tests;

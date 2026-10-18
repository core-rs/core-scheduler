// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![allow(clippy::unnecessary_cast)]
use crate::resolver::{self, AddrInfo, sys};
use core_scheduler::{AddressInfoView, SocketAddress, SocketProtocol, SocketType};

/// A borrowed entry in an immutable, validated [`SystemAddresses`](crate::SystemAddresses) native list.
#[repr(transparent)]
pub struct SystemAddressInfo(pub(crate) AddrInfo);
// SAFETY: Entries are immutable and only borrowed from a live SystemAddresses owner. Native pointers never escape through this API.
unsafe impl Send for SystemAddressInfo {}
unsafe impl Sync for SystemAddressInfo {}
impl AddressInfoView for SystemAddressInfo {
    fn address(&self) -> SocketAddress {
        resolver::address(&self.0).expect("native addresses were validated before exposing views")
    }
    fn socket_type(&self) -> SocketType {
        match self.0.ai_socktype {
            value if value == sys::SOCK_STREAM as i32 => SocketType::Stream,
            value if value == sys::SOCK_DGRAM as i32 => SocketType::Datagram,
            value if value == sys::SOCK_RAW as i32 => SocketType::Raw,
            other => SocketType::Other(other),
        }
    }
    fn protocol(&self) -> Option<SocketProtocol> {
        match self.0.ai_protocol {
            0 => None,
            value if value == sys::IPPROTO_TCP as i32 => Some(SocketProtocol::Tcp),
            value if value == sys::IPPROTO_UDP as i32 => Some(SocketProtocol::Udp),
            other => Some(SocketProtocol::Other(other)),
        }
    }
}

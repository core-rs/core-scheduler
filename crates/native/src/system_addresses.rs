// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    SystemAddressInfo,
    resolver::{self, AddrInfo, platform},
};
use core_scheduler::{AddressInfoCollection, AddressInfoView};
use std::io;

/// Owns an unchanged native resolver list. Borrowed views decode fields on demand; no copied entry array or per-entry allocation is required.
#[derive(Debug)]
pub struct SystemAddresses {
    pub(crate) head: *mut AddrInfo,
    #[cfg(windows)]
    _winsock: platform::Winsock,
}
// SAFETY: The exclusive owner retains an immutable native list, whose matching deallocator can run on any thread. No native access survives lookup completion.
unsafe impl Send for SystemAddresses {}
unsafe impl Sync for SystemAddresses {}
impl SystemAddresses {
    // SAFETY: head must be an exclusively owned successful getaddrinfo list, with readable records and address storage, freed only by this owner.
    pub(crate) unsafe fn from_native(
        head: *mut AddrInfo,
        #[cfg(windows)] winsock: platform::Winsock,
    ) -> io::Result<Self> {
        let result = Self {
            head,
            #[cfg(windows)]
            _winsock: winsock,
        };
        let mut current = head;
        while let Some(entry) = unsafe { current.as_ref() } {
            resolver::address(entry)?;
            current = entry.ai_next;
        }
        Ok(result)
    }
}
impl AddressInfoCollection for SystemAddresses {
    fn entries(&self) -> impl Iterator<Item: AddressInfoView + Send> + Send {
        // SAFETY: Transparent views borrow immutable validated entries from this owner's retained list.
        let first = unsafe { self.head.cast::<SystemAddressInfo>().as_ref() };
        std::iter::successors(first, |entry| unsafe {
            entry.0.ai_next.cast::<SystemAddressInfo>().as_ref()
        })
    }
}
impl Drop for SystemAddresses {
    fn drop(&mut self) {
        if !self.head.is_null() {
            // SAFETY: This unchanged list has exactly one owner and all borrowed views end before its drop.
            unsafe { platform::freeaddrinfo(self.head) };
        }
    }
}

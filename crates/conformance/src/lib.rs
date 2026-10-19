// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![doc = include_str!("../readme.md")]

pub mod address_resolution;
pub mod blocking;
pub mod clock;
pub mod file;
pub mod file_advice;
pub mod file_allocate;
pub mod signals;
pub mod socket;
pub mod socket_messages;
pub mod socket_options;
pub mod socket_poll;
pub mod socket_registration;
pub mod spawn;

mod address_resolution_fixture;
mod address_resolution_ip_tests;
mod address_resolution_tests;
mod blocking_queue_tests;
mod blocking_tests;
mod clock_fixture;
mod clock_tests;
mod file_advice_fixture;
mod file_advice_tests;
mod file_allocate_fixture;
mod file_allocate_tests;
mod file_test_fixture;
mod file_tests;
mod probe;
mod signals_dispatch_tests;
mod signals_fixture;
mod signals_tests;
mod socket_datagram_poll_tests;
mod socket_datagram_tests;
mod socket_get_option_tests;
mod socket_io_fixture;
mod socket_io_guard;
mod socket_io_tests;
mod socket_message_poll_tests;
mod socket_message_tests;
mod socket_option_fixture;
mod socket_poll_tests;
mod socket_registration_fixture;
mod socket_registration_tests;
mod socket_set_option_tests;
mod socket_tests;
mod spawn_dispatch_tests;
mod spawn_fixture;
mod spawn_tests;
mod test_cases;

pub use address_resolution_fixture::AddressResolutionFixture;
pub use clock_fixture::ClockFixture;
pub use file_advice_fixture::FileAdviceFixture;
pub use file_allocate_fixture::FileAllocateFixture;
pub use file_test_fixture::FileTestFixture;
pub use signals_fixture::SignalsFixture;
pub use socket_io_fixture::SocketIoFixture;
pub use socket_io_guard::SocketIoGuard;
pub use socket_option_fixture::SocketOptionFixture;
pub use socket_registration_fixture::SocketRegistrationFixture;
pub use spawn_fixture::SpawnFixture;

// Macro expansions must resolve their support dependency through this crate even when dependencies are renamed by a backend.
#[doc(hidden)]
pub use core_scheduler as __core_scheduler;

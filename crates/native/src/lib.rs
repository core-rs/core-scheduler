// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

#![doc = include_str!("../readme.md")]
#![deny(unsafe_op_in_unsafe_fn)]

#[cfg(not(any(unix, windows)))]
compile_error!("core-scheduler-native currently supports Unix and Windows");

mod blocking_pool;
mod blocking_task;
#[cfg(target_os = "linux")]
mod file_extensions;
mod file_io;
mod native_file;
mod native_options;
mod native_scheduler;
mod native_socket;
#[cfg(unix)]
mod native_subscription;
mod native_task;
mod panic;
mod resolver;
#[cfg(unix)]
mod signal_service;
mod socket_io;
mod socket_operations;
mod socket_options;
mod system_address_info;
mod system_addresses;
mod system_file_metadata;
mod task_service;
#[cfg(test)]
mod test_support;
mod thread;
mod timer_service;
mod worker;

pub use blocking_task::BlockingTask;
pub use native_file::NativeFile;
pub use native_options::NativeOptions;
pub use native_scheduler::NativeScheduler;
pub use native_socket::NativeSocket;
#[cfg(unix)]
pub use native_subscription::NativeSubscription;
pub use native_task::NativeTask;
pub use system_address_info::SystemAddressInfo;
pub use system_addresses::SystemAddresses;
pub use system_file_metadata::SystemFileMetadata;

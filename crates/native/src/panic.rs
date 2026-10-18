// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::panic::{AssertUnwindSafe, catch_unwind};

pub(crate) fn contain(action: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(action)) {
        // A user panic payload may itself panic on drop. On this exceptional path there is no caller to receive it, so leak the payload rather than unwind through an executor or kill a service thread.
        std::mem::forget(payload);
    }
}
pub(crate) fn discard<T>(value: T) {
    contain(|| drop(value));
}

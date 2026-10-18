// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{io, thread};

#[cfg(test)]
thread_local! {
    pub(crate) static FAIL_AFTER: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

pub(crate) fn start(name: &str, run: impl FnOnce() + Send + 'static) -> io::Result<()> {
    #[cfg(test)]
    if let Some(remaining) = FAIL_AFTER.get() {
        FAIL_AFTER.set(remaining.checked_sub(1));
        if remaining == 0 {
            return Err(io::Error::other("injected thread startup failure"));
        }
    }
    thread::Builder::new()
        .name(name.into())
        .spawn(run)
        .map(drop)
}

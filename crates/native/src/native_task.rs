// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use core_scheduler::TaskError;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// An owning task handle. Dropping it requests destruction after any active poll returns; it never detaches execution.
#[must_use = "await the task or retain its owning handle"]
pub struct NativeTask<T>(
    pub(crate) async_task::Task<crate::task_service::TaskOutcome<T>, crate::task_service::TaskState>,
);

impl<T> Future for NativeTask<T> {
    type Output = Result<T, TaskError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(context)
            .map(|mut outcome| outcome.0.take().unwrap())
    }
}

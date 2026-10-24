// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use atomic_waker::AtomicWaker;
use core_scheduler::{IoBuf, IoBufMut, IoBufSequence, IoBufSequenceMut, OperationError};
use std::{
    future::poll_fn,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};

/// Lazily allocated retirement state shared by a bound resource and its owned operation.
#[derive(Default)]
pub(crate) struct OperationState {
    activity: Option<Arc<Activity>>,
}

struct Activity {
    running: AtomicBool,
    // Close borrows its lane mutably, so at most one retirement waiter can register.
    retired: AtomicWaker,
}

// Field order releases the owner before publishing retirement, including during unwinding.
pub(crate) struct OperationGuard<T> {
    value: Option<T>,
    _retirement: Retirement,
}
struct Retirement(Arc<Activity>);
struct RetirementWait<'a>(&'a AtomicWaker);
impl Drop for RetirementWait<'_> {
    fn drop(&mut self) {
        self.0.take();
    }
}
impl Drop for Retirement {
    fn drop(&mut self) {
        self.0.running.store(false, Ordering::Release);
        self.0.retired.wake();
    }
}
impl<T> OperationGuard<T> {
    pub(crate) fn value_mut(&mut self) -> &mut T {
        self.value.as_mut().unwrap()
    }
    pub(crate) fn into_inner(mut self) -> T {
        let value = self.value.take().unwrap();
        drop(self);
        value
    }
}
impl<T: AsRef<[u8]>> AsRef<[u8]> for OperationGuard<T> {
    fn as_ref(&self) -> &[u8] {
        self.value.as_ref().unwrap().as_ref()
    }
}
impl<T: AsMut<[u8]>> AsMut<[u8]> for OperationGuard<T> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.value_mut().as_mut()
    }
}
// SAFETY: The guard retains the complete stable owner without changing its exposed allocation or range.
unsafe impl<T: IoBuf> IoBuf for OperationGuard<T> {}
// SAFETY: Only the guard owns mutable access to the retained buffer.
unsafe impl<T: IoBufMut> IoBufMut for OperationGuard<T> {}
// SAFETY: Every range and prepared I/O vector is forwarded unchanged to the exclusively retained owner.
unsafe impl<T: IoBufSequence> IoBufSequence for OperationGuard<T> {
    fn buffer_count(&self) -> usize {
        self.value.as_ref().unwrap().buffer_count()
    }
    fn buffer(&self, index: usize) -> &[u8] {
        self.value.as_ref().unwrap().buffer(index)
    }
    fn io_slices(&self) -> Option<&[std::io::IoSlice<'_>]> {
        self.value.as_ref().unwrap().io_slices()
    }
}
// SAFETY: The guard exclusively owns the underlying sequence and preserves its writable ranges.
unsafe impl<T: IoBufSequenceMut> IoBufSequenceMut for OperationGuard<T> {
    fn buffer_mut(&mut self, index: usize) -> &mut [u8] {
        self.value_mut().buffer_mut(index)
    }
    fn io_slices_mut(&mut self) -> Option<&[std::io::IoSliceMut<'_>]> {
        self.value_mut().io_slices_mut()
    }
}
impl OperationState {
    pub(crate) fn check_idle(&self) -> Result<(), OperationError> {
        if self
            .activity
            .as_ref()
            .is_some_and(|activity| activity.running.load(Ordering::Acquire))
        {
            Err(io::Error::from(io::ErrorKind::WouldBlock).into())
        } else {
            Ok(())
        }
    }
    pub(crate) fn begin<T>(&mut self, value: T) -> Result<OperationGuard<T>, (OperationError, T)> {
        let activity = self.activity.get_or_insert_with(|| {
            Arc::new(Activity {
                running: AtomicBool::new(false),
                retired: AtomicWaker::new(),
            })
        });
        if activity.running.swap(true, Ordering::AcqRel) {
            return Err((io::Error::from(io::ErrorKind::WouldBlock).into(), value));
        }
        Ok(OperationGuard {
            value: Some(value),
            _retirement: Retirement(Arc::clone(activity)),
        })
    }
    pub(crate) async fn close(&mut self) {
        if let Some(activity) = &self.activity {
            let _wait = RetirementWait(&activity.retired);
            poll_fn(|context| {
                activity.retired.register(context.waker());
                if !activity.running.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }
    }
}

#[cfg(test)]
mod tests;

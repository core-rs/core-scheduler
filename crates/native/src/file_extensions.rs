// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::{
    NativeFile, NativeScheduler, blocking_pool::admission_error, native_scheduler::wait_close,
};
use core_scheduler::{
    AdviceMode, AllocationMode, Cancellation, FileAdvice, FileAllocate, OperationResult,
};
use std::{io, os::fd::AsRawFd};

fn range(offset: u64, length: u64) -> io::Result<()> {
    if offset
        .checked_add(length)
        .is_none_or(|end| end > i64::MAX as u64)
    {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "offset and range end must fit in i64",
        ))
    } else {
        Ok(())
    }
}

impl FileAllocate for NativeScheduler {
    async fn file_allocate(
        &self,
        file: &NativeFile,
        offset: u64,
        length: u64,
        mode: AllocationMode,
        cancellation: &Cancellation,
    ) -> OperationResult<()> {
        cancellation.check()?;
        range(offset, length)?;
        if length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "allocation length must be positive",
            )
            .into());
        }
        #[cfg(test)]
        let probe = std::sync::Arc::clone(&self.file_probe);
        self.work
            .execute(file.retain(), cancellation, move |file| {
                #[cfg(test)]
                probe
                    .allocations
                    .lock()
                    .unwrap()
                    .push((offset, length, mode));
                allocate(file, offset, length, mode)
            })
            .await
            .0
    }
}

fn allocate(
    file: &std::fs::File,
    offset: u64,
    length: u64,
    mode: AllocationMode,
) -> io::Result<()> {
    let flags = match mode {
        AllocationMode::Extend => 0,
        AllocationMode::KeepSize => libc::FALLOC_FL_KEEP_SIZE,
    };
    // SAFETY: The retained file outlives the synchronous syscall; validation established positive signed 64-bit length and range.
    let result =
        unsafe { libc::fallocate64(file.as_raw_fd(), flags, offset as i64, length as i64) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

impl FileAdvice for NativeScheduler {
    async fn file_advise(
        &self,
        file: &NativeFile,
        offset: u64,
        length: u64,
        advice: AdviceMode,
    ) -> io::Result<()> {
        wait_close(self.submit_advice(file, offset, length, advice).await?).await
    }
    async fn file_advise_submit(
        &self,
        file: &NativeFile,
        offset: u64,
        length: u64,
        advice: AdviceMode,
    ) -> io::Result<()> {
        self.submit_advice(file, offset, length, advice)
            .await
            .map(drop)
    }
}
impl NativeScheduler {
    async fn submit_advice(
        &self,
        file: &NativeFile,
        offset: u64,
        length: u64,
        advice: AdviceMode,
    ) -> io::Result<
        futures_channel::oneshot::Receiver<Result<io::Result<()>, core_scheduler::TaskError>>,
    > {
        range(offset, length)?;
        let file = file.retain();
        #[cfg(test)]
        let probe = std::sync::Arc::clone(&self.file_probe);
        self.cleanup
            .submit(move || {
                #[cfg(test)]
                {
                    let gate = probe.gate.lock().unwrap().clone();
                    if let Some(gate) = gate {
                        gate.wait();
                    }
                    probe.advice.lock().unwrap().push((offset, length, advice));
                }
                let result = advise(&file, offset, length, advice);
                drop(file);
                result
            })
            .await
            .map_err(|error| io::Error::from(admission_error(error)))
    }
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct Probe {
    pub(crate) allocations: std::sync::Mutex<Vec<(u64, u64, AllocationMode)>>,
    pub(crate) advice: std::sync::Mutex<Vec<(u64, u64, AdviceMode)>>,
    pub(crate) gate: std::sync::Mutex<Option<std::sync::Arc<crate::test_support::Gate>>>,
}
fn advise(file: &std::fs::File, offset: u64, length: u64, advice: AdviceMode) -> io::Result<()> {
    let advice = match advice {
        AdviceMode::Normal => libc::POSIX_FADV_NORMAL,
        AdviceMode::Sequential => libc::POSIX_FADV_SEQUENTIAL,
        AdviceMode::Random => libc::POSIX_FADV_RANDOM,
        AdviceMode::WillNeed => libc::POSIX_FADV_WILLNEED,
        AdviceMode::DontNeed => libc::POSIX_FADV_DONTNEED,
        AdviceMode::NoReuse => libc::POSIX_FADV_NOREUSE,
    };
    // SAFETY: The retained descriptor and validated range remain valid through the synchronous call. This function returns an errno value directly.
    let result =
        unsafe { libc::posix_fadvise64(file.as_raw_fd(), offset as i64, length as i64, advice) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(result))
    }
}

#[cfg(test)]
mod tests;

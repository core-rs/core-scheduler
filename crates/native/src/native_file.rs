// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::{fs::File, sync::Arc};

/// An owned file for native scheduler instances. Operations borrow it; explicit close consumes it. It is deliberately not `Clone`.
///
/// The backend privately retains the file while an operation uses it. Dropping an operation future does not establish completion; await the operation before explicitly closing the file. Cancellation-aware work can first receive a cooperative cancellation request; accepted submission-only work retains the descriptor until it finishes. Ordinary handle destruction releases ownership without waiting or reporting close errors.
///
/// Open files through [`FileOpen::file_open`](core_scheduler::FileOpen::file_open). Native file import and access are deliberately unavailable: externally configured append flags or descriptor aliases would bypass positioned-write and exclusive-close guarantees.
///
/// ```compile_fail
/// use core_scheduler_native::NativeFile;
/// fn duplicate(file: &NativeFile) -> NativeFile {
///     file.clone() // Cloning this reference cannot produce another owned handle.
/// }
/// ```
///
/// ```compile_fail
/// use core_scheduler_native::NativeFile;
/// let append_file = std::fs::OpenOptions::new().append(true).open("log").unwrap();
/// let file = NativeFile::from(append_file);
/// ```
///
/// ```compile_fail
/// use core_scheduler_native::NativeFile;
/// fn native(file: &NativeFile) -> &std::fs::File {
///     file.as_ref()
/// }
/// ```
#[derive(Debug)]
pub struct NativeFile(pub(crate) Arc<File>);

impl NativeFile {
    // Only files opened with backend-controlled options may enter this wrapper.
    pub(crate) fn from_std(file: File) -> Self {
        Self(Arc::new(file))
    }
    pub(crate) fn retain(&self) -> Arc<File> {
        Arc::clone(&self.0)
    }

    pub(crate) fn into_file(self) -> File {
        Arc::try_unwrap(self.0)
            .expect("cannot close a file with outstanding operations; cancel and await them first")
    }
}

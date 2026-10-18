// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

/// Independent limits for executor workers, blocking work, and timers. Cleanup has its own worker and bounded queue.
#[derive(Clone, Copy, Debug)]
pub struct NativeOptions {
    /// Number of work-stealing task workers. Must be nonzero.
    pub workers: usize,
    /// Maximum simultaneous synchronous operations. Must be nonzero.
    pub threads: usize,
    /// Maximum admitted blocking jobs, including queued and running work. Must be nonzero.
    pub capacity: usize,
    /// Reserve at least this many simultaneous timer registrations. Must be nonzero. A full queue returns `WouldBlock`.
    pub timer_capacity: usize,
}
impl Default for NativeOptions {
    fn default() -> Self {
        Self {
            workers: std::thread::available_parallelism().map_or(1, usize::from),
            threads: 4,
            capacity: 64,
            timer_capacity: 1024,
        }
    }
}

# Releases

## Unreleased

- Defer Unix Tokio socket readiness registration until I/O needs to wait, avoiding stale unconnected TCP hangup events which could make a later full-queue send spin indefinitely.
- Add optional borrowed recv/send and message `_poll` capabilities with stack-buffer support, explicit cancellation, and shared conformance suites in Direct, Tokio, and Native. Windows attempts explicitly enable nonblocking mode.

- Add `core-scheduler-native`, porting Socketry's work-stealing executor and readiness I/O with owned operations, explicit cancellation, bounded blocking services, and asynchronous shutdown.
- Extract `core-scheduler-timers` with reserved timer storage, constant-time cancellation, batched expiry, and reusable sleep registrations.

- Rename `protocol-scheduler` and its companion crates to the `core-scheduler` family; Rust imports now use `core_scheduler`, `core_scheduler_conformance`, `core_scheduler_direct`, and `core_scheduler_tokio`.
- Contain native-worker and queued-capture panic payloads whose destructors panic, preserving worker capacity and shutdown progress in Direct and Tokio.
- Add `core-scheduler-tokio` with explicit runtime selection, owning tasks, clocks, socket/file operations, native resolution, bounded blocking work, and platform-supported signals and filesystem extensions.

- Implement Direct task execution, clocks, native address resolution with borrowed result views, typed socket options, TCP registration, Unix signals, and Linux file allocation/advice, with shared conformance suites and native lifecycle tests.

- Add shared conformance suites for clocks, tasks, address resolution, signals, typed socket options, TCP registration, file allocation, and advice, with controlled fixtures and rejection tests.
- Rename `Signals::signal_notify` to `signal_subscribe` and its associated `Registration` type to `Subscription`.

- Generate individually reported conformance suites from shared capability-specific case lists, with backend-owned runners and isolated fixtures. Direct now uses these suites; existing async assertion helpers remain available.

- Add reusable socket/message conformance tests and direct IPv4/IPv6 socket support, including prepared scatter/gather I/O, cancellation/retirement fixtures, nonblocking retry without occupied worker slots, and independent cleanup.

- Add owned `IoBuffers` and `IoBuffersMut` packages with reusable native I/O vectors and explicit advancement after partial transfers. Sequence traits now expose indexed byte ranges and optional prepared vectors instead of mutable source-owner slices.

- Reuse direct blocking-task completion state for handle abandonment, removing a separate shared allocation per blocking submission.
- Accept owned buffer sequences for message I/O and backend-associated address resolution results with borrowed entry views, allowing arrays, pooled collections, and native resolver lists without mandatory copied vectors.
- Keep cancellation and direct capacity listeners inline in waiting futures, avoiding per-wait allocations and unnecessary capacity registration during immediate admission.
- Extract runtime-independent scheduler capabilities and shared types from Socketry.
- Make operation context explicit, retain implementation-associated resources and owning task handles, and preserve owned-buffer and cooperative-cancellation contracts.
- Add `core-scheduler-conformance` with reusable file and blocking-work assertions.
- Add `core-scheduler-direct` with bounded file/blocking workers, independent cleanup, and explicit shutdown.

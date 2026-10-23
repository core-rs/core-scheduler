# Releases

## Unreleased

- Extend the echo benchmark with Native readiness and Monoio io_uring/epoll servers, using the same validated client, buffer sizes, and CPU placement.

- Add installed I/O usage guidance and runnable, cross-backend recipes for reused borrowed stream storage, retained frames, bounded packet handoff, and overlapped file copies. Benchmark the same loops and demonstrate monitor cleanup on errors and cancellation.

- Consolidate portable receives around explicit buffer pools, retaining multishot controls in the io_uring backend. Monitor and receiver close methods now take no cancellation token and return `io::Result<()>`; await close and drop retained handles before exclusively closing a resource. See the [interface reference](context/interface.md) for capability and lifecycle semantics.

- Expose backend-associated receive messages with borrowed address views, payload access, and owned payload extraction. Keep writable payload access optional for immutable receivers, and retain io_uring native address metadata until conversion is requested. Specify ownership and lifetime semantics without imposing copying or allocation costs.

- Name owned receive results `SocketRecvMessage` across polling and multishot backends. Return pooled socket buffers before readiness waits so idle connections cannot reserve the shared budget, reuse Tokio's registered readiness without extra native probes, and document cancellation, explicit retirement, and future pool growth and caching boundaries.

- Distribute new Native tasks round-robin across worker queues before their first poll, reusing the admission lock and removing the shared unassigned-task queue. Preserve worker-local wakes and idle stealing without adding per-operation load counters.

- Reuse persistent slab ownership claims across heap buffer checkout, freezing, and return. Notify only while capacity waiters are registered, preserving the public buffer interface, stable storage, and cross-thread teardown safety.

- Prototype backend-selected buffer pools with writable completed leases and retained immutable views. Ordinary shared pools use portable monitor receives; the concrete io_uring `buffer_pool_per_worker` experiment allocates every worker's contiguous slab and provided-ring metadata up front. Keep worker policy out of the portable capability and make the backend's receiver strategy opaque. Use one selector-wide group ID across rings, return leases to their home slab, and remove cross-worker reclamation. Worker receive-ahead requires Linux 6.0 or later; its locality and throughput remain experimental.

- Replace the shared receive-buffer pool's free-list mutex with atomic availability words, and wake a distinct capacity waiter for every returned slot. Avoid listener registration for queued io_uring multishot messages and redundant ownership increments on local buffer returns; allow retained leases to be released safely during thread-local teardown. Correct buffer benchmarks to execute Native and Tokio I/O on their task workers and add burst and pool-contention measurements.

- Prototype bounded shared `BufferPool` storage, unique writable receive leases, retained immutable views, monitor-based file/socket receivers, and separate file submission/completion waits across Direct, Tokio, Native readiness, and io_uring. Add owned write-all helpers and matched file/TCP/UDP buffer benchmarks; kernel buffer registration remains a separate optimization.

- Prototype sequential `FileMonitor` resources with explicit cancellation and retirement across Direct, Tokio, Native readiness, and io_uring. io_uring reuses typed request storage and caches an optional fixed-file registration for the current ring, releasing and replacing it when a subsequent operation changes workers, with shared ring-binding conventions for file and socket monitors; add positioned-file benchmarks comparing ordinary and monitored operations.

- Prototype optional `Timers` resources in Native, Direct, and Tokio, constructed through `scheduler.timer()` and reused with `timer_wait_until`. Add a timer benchmark comparing refresh, expiry, and allocation costs; the reusable capability and its retention policy remain experimental.

- Keep Direct timer waker callbacks outside the registry lock, so reentrant callbacks cannot deadlock the clock.

- Add reusable directional `SocketMonitor` and `SocketStreamMonitor` state, with shared operations, caller-owned connection storage, and explicit cancellation and retirement. io_uring retains persistent poll registrations and caches readiness; stream monitors provide a separate specialization point while preserving readiness until `WouldBlock`. Other backends reuse their existing polling operations.

- Add portable `SocketRecvMsgMultishot` and explicitly advanced receivers, with a fixed-buffer polling implementation for Direct, Readiness, and Tokio, and Native forwarding. Add io_uring `socket_recvmsg_multishot` with bounded provided-buffer pools, owned receive leases, automatic rearming after exhaustion, and explicit retirement. Add a matched echo benchmark against Tokio and Native readiness I/O.

- Drive Native timers from an exclusively owned queue on each task worker, using the next deadline to bound its I/O wait. Remove the dedicated timer thread, retain original timer ownership across task migration, and configure reserved timer capacity per worker.

- Stage io_uring readiness submissions for at most one nonblocking turn so idle workers can submit and wait together. Publish completions immediately after waiting, while preserving callback-induced wakes and bounded command processing.

- Skip empty Native admission-queue steals and unchanged timer-queue flushes, reducing idle-path synchronization and timer bookkeeping while preserving remote wakeups.

- Process Native tasks in alternating, stealable ready batches with an I/O driver turn between batches. Avoid redundant worker self-interrupts while preserving remote wakeups and shutdown progress.

- Simplify Native task admission and worker parking without a task-ID registry or duplicate search-result types. Configure Tokio sockets once on construction and clear io_uring notification counters with one successful read.

- Avoid speculative eventfd reads before io_uring idle waits; the armed ring poll preserves remote wakeups, and notification completions drain the counter before rearming.

- Avoid redundant speculative Tokio receive attempts by waiting on cached readiness for established nonempty scalar stream reads. Lifecycle operations, empty reads, and datagrams retain syscall-first behavior; short reads preserve readiness until `WouldBlock`.

- Use cooperative io_uring completion processing for fresh thread-bound workers where supported, retaining the original ring for movable drivers and when the optional setup fails.

- Wait directly on io_uring with eventfd interrupts, retaining owner-thread wakes in memory to avoid kernel self-interrupts. Complete ready scalar socket writes synchronously and combine idle submission with waiting. Ring setup now requires extended wait arguments for timed driver turns, available since Linux 5.11.

- Reserve Tokio's lightweight socket readiness polling path per direction, retaining concurrent waiter and cancellation support.

- Notify io_uring capacity waiters only when capacity becomes available, and native task-drain waiters only when outstanding runnables reach zero.

- Make Tokio scheduler closure admission-only for sockets and clocks. Accepted operations complete or respond to their explicit cancellation token, avoiding per-operation shutdown listeners; applications coordinate cancellation before draining tasks.

- Run io_uring borrowed socket operations as direct nonblocking syscalls followed by per-worker ring readiness waits, avoiding the separate readiness reactor on that path.

- Avoid io_uring idle-wait setup after useful progress or nonblocking turns, preserve wakeups during sleep preparation, and register capacity listeners only when admission is full.

- Add a reproducible TCP echo benchmark comparing pure Tokio, the Tokio adapter, and Native with io_uring, including owned-buffer and borrowed readiness paths, with repeated throughput and round-trip latency measurements.

- Separate native I/O into `core-scheduler-selector` and compose `NativeScheduler<S>` with independently selected capabilities, preserving the default readiness constructor.
- Add `core-scheduler-iouring` with completion file and owned socket transfers, per-worker rings driven by Native, local submission queues, and independent event-loop or thread driving.
- Publish completion batches to ready queues before resuming local tasks, and retain ring affinity for cancellation after task migration.
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

# Core Scheduler Conformance

Reusable, capability-specific tests for implementations of `core-scheduler`. Add this crate as a **development dependency** of a backend. Generated suites report ordinary named Rust tests, while async assertion helpers remain available for custom fixtures and smaller capability sets.

## Generated suites

Invoke a suite inside a dedicated module and supply a runner function:

```rust,ignore
mod blocking {
    core_scheduler_conformance::blocking_tests!(run: super::run_blocking);
}

mod ipv4 {
    core_scheduler_conformance::socket_tests!(run: super::run_ipv4_stream);
}
```

The conformance crate owns each suite's case list. Updating this dependency automatically includes new cases in every backend using that suite; backends do not maintain copies of test names. Each expansion produces separate `#[test]` functions such as `blocking::pre_cancelled_submission` and `ipv4::pending_read_cancellation`. Normal `cargo test` listing, filtering, parallel execution, and failure reporting work without a custom test harness. A failed assertion does not prevent sibling cases from running.

A runner receives one async closure and creates a **fresh isolated fixture for each invocation**. For a blocking implementation named `Backend`, its signature is `fn run_blocking(test: impl AsyncFnOnce(&Backend))`. Stream and datagram runners accept `impl AsyncFnOnce(&Backend, &BackendSocket, &BackendSocket)`, supplying the scheduler, sender, and receiver by reference. Fixture runners accept `impl AsyncFnOnce(&Fixture)`. Async closures require Rust 1.85, matching the workspace minimum. Runners may add `Send + 'static` when moving the closure to a deadline-controlled thread; operation futures need not be boxed.

| Macro | Runner supplies | Required capabilities or fixture |
| --- | --- | --- |
| `blocking_tests!` | Scheduler | `Blocking` |
| `blocking_queue_tests!` | Fixture | `blocking::BlockingFixture` |
| `file_tests!` | Fixture | `FileTestFixture` with the complete `FileScheduler` profile |
| `socket_tests!` | Scheduler and fresh connected stream pair | `SocketRead`, `SocketWrite`, `SocketWait`, `SocketShutdown` |
| `socket_message_tests!` | Scheduler and fresh connected stream pair | `SocketSendMsg`, `SocketRecvMsg` |
| `socket_datagram_tests!` | Scheduler and fresh bound, unconnected datagram pair | `SocketSendMsg`, `SocketRecvMsg`, `SocketLocalAddress` |
| `socket_poll_tests!` | Scheduler and fresh connected stream pair | `SocketRecvPoll`, `SocketSendPoll`, `SocketShutdown` |
| `socket_message_poll_tests!` | Scheduler and fresh connected stream pair | `SocketRecvMsgPoll`, `SocketSendMsgPoll` |
| `socket_datagram_poll_tests!` | Scheduler and fresh bound, unconnected datagram pair | `SocketRecvMsgPoll`, `SocketSendMsgPoll`, `SocketLocalAddress` |
| `socket_io_tests!` | Fixture | `SocketIoFixture` |
| `clock_tests!` | Fixture | `ClockFixture` |
| `spawn_tests!` | Fixture | `SpawnFixture` |
| `spawn_dispatch_tests!` | Fixture | `SpawnFixture + Sync`, with a background executor driver |
| `address_resolution_tests!` | Fixture | `AddressResolutionFixture` |
| `address_resolution_ip_tests!` | Fixture and `domain: SocketDomain` macro argument | `AddressResolutionFixture`, one supported family per module |
| `signals_tests!` | Fixture | `SignalsFixture` |
| `signals_dispatch_tests!` | Fixture | `SignalsFixture + Sync`, with a background dispatch driver |
| `socket_get_option_tests!` | Fixture | `SocketOptionFixture` and `SocketGetOption<Fixture::Option>` |
| `socket_set_option_tests!` | Fixture | `SocketOptionFixture` and `SocketSetOption<Fixture::Option>` |
| `socket_registration_tests!` | Fixture | `SocketRegistrationFixture`, plus accept, scalar I/O, close, and endpoint lookup |
| `file_allocate_tests!` | Fixture | `FileAllocateFixture` |
| `file_advice_tests!` | Fixture | `FileAdviceFixture` |

For example, a resolver fixture can select a numeric family independently of its general resolver suite:

```rust,ignore
mod resolution {
    core_scheduler_conformance::address_resolution_tests!(run: super::run_resolver);
}
mod resolution_ipv4 {
    core_scheduler_conformance::address_resolution_ip_tests!(
        run: super::run_resolver,
        domain: core_scheduler::SocketDomain::Ipv4,
    );
}
```

Run socket suites separately for each supported address family and the I/O lifecycle suite for each supported operation/input representation. Use individual assertion helpers for capability combinations smaller than a suite. Unsupported capabilities must not be presented as passing tests.

The runner drives its executor and services, awaits the supplied closure exactly once, enforces a deadline covering setup, assertions, and cleanup, and propagates assertion panics to the generated test. Cleanup must also run after assertion failure: release held execution boundaries, let accepted operations retire, and await backend shutdown before destroying temporary storage. Do not catch and discard a panic. The [Direct file/blocking runners](../direct/tests/conformance.rs), [socket runners](../direct/tests/sockets.rs), and [Tokio runtime runners](../tokio/tests/runtime_conformance.rs) provide complete integrations.

## Assertions and fixtures

File helpers accept backend-created resources and paths and require only the capabilities they exercise. `FileTestFixture` additionally supplies independent backing-store observations, so the complete file suite checks contents without relying on the same scheduler implementation to verify its own writes. Each file case starts with an absent path, creates and seeds its own regular file, and closes it after its assertions.

Socket assertions cover separate creation/binding/listening/connection/acceptance, scalar and prepared message I/O, short transfers, EOF, readiness, IPv4/IPv6 addresses, datagram boundaries, peek, truncation, empty datagrams, cancellation, and ownership. `socket::stream_pair` creates an isolated loopback fixture; individual I/O assertions also accept backend-created sockets. Borrowed polling suites additionally exercise stack payloads and vectors, explicit advancement, cancellation wakeups, and safe reuse of storage after dropping or forgetting pending receives. `SocketIoFixture` and `SocketIoGuard` expose a backend-controlled access boundary for deterministic completion/cancellation races and buffer retention after waiter abandonment. Fixtures must release blocked access on destruction. Blocking queue fixtures hold execution at a known boundary to check cancellation, admission, and handle abandonment without assuming global FIFO order.

Tests panic on contract violations. Existing aggregate helpers remain available. No test assumes one runtime, task order, concrete descriptor, or prompt cancellation of a running syscall. Scheduler shutdown, capacity/progress policy, platform-specific behavior, and unsupported capabilities still need backend-specific tests. Fixtures and assertions live here; backend-specific internal checks belong in backend crates. Future adapters can use the same suites without depending on direct.

## Remaining capability suites

`ClockFixture` provides a controlled clock domain with independently observed timer registrations. Time advances through its test driver, without real sleeps. Cases cover lazy construction, registration release on drop/completion/cancellation, absolute deadlines and wakeups, relative waits starting at first poll, pre-cancellation before elapsed deadlines and overflow, and expiry winning cancellation observed at the same poll. A production backend can expose its paused clock or a test driver at the same timer boundary.

`SpawnFixture` supplies its executor barrier, admission close, and an independent registration check. Cases cover registration before first poll, result and capture ownership, handle transfer across threads, panic payloads, cooperative cancellation and completion wakeups, abandoning pending and completed tasks, and rejecting closed-service submissions without polling them. The separate dispatch suite holds a task in an active poll and drops its handle on another thread, checking retention during the poll and destruction after handle drop and backend quiescence. Handle drop may wait for the poll; the test does not assume the drop thread requests abandonment before the poll is released. Its fixture must support a scoped background driver; the basic suite adds no `Send` or `Sync` bound to `Spawn`.

`AddressResolutionFixture` configures a nonnumeric success name, a named service, an ordered answer list, and a structured failure. Results preserve order, duplicates, scoped/native fields, borrowed views, and ownership across threads and subsequent lookups. Validation and pre-cancellation are checked without native lookup submissions. Numeric family suites test TCP/UDP hints, numeric ports, and absent-name loopback/wildcard behavior. Boundary observations verify all hint flags, including address configuration, without imposing a particular host network configuration. Backends must still exercise their implementation-specific queued/running lookup retirement and native-list cleanup boundaries.

`SignalsFixture` controls isolated delivery and subscription retirement. Cases cover signal selection, independent targets and subscriptions, cancellation targets, setup failure, and callbacks subscribing/unsubscribing reentrantly. The separate dispatch suite drops a subscription while its callback is held, checking that unsubscribe returns without joining and that the target survives until dispatch ends. Its scoped background driver requires a `Sync` fixture, without strengthening the production `Signals` contract. Test guards release held callbacks on assertion failure; timeout waits are watchdogs, not timing-based correctness assertions. Real signal backends additionally test their OS-handler-to-dispatch bridge outside these portable fixtures.

Socket-option fixtures independently observe native state and syscall attempts, with separate getter-only and setter-only suites. They check lazy/pre-cancelled operations, native values, setter effects, and preserved errors. TCP-registration tests use standard-library loopback resources, preserve stream/listener endpoints and queued connections, exchange traffic in both directions, and observe EOF. Run registration once per supported family; platform-specific native handle/mode failure injection remains a backend responsibility.

Allocation fixtures start with known contents and cursor state, and must observe real reservation at the allocation boundary. Cases check extending and keeping size, no shrinking or payload/cursor changes, zero-filled exposed bytes, laziness/cancellation, invalid ranges, and read-only failures. Advice fixtures observe actual call arguments because cache residency and eviction are not promised. Cases check all supported modes, zero-length-to-EOF ranges, lazy admission, validation and execution errors, completion versus submission, and accepted work surviving waiter abandonment. Select a filesystem/profile supporting the tested modes; unsupported capabilities are omitted rather than counted as passing.

The conformance crate's integration tests run all generated suites against controlled test implementations and reject deliberately incorrect variants. Socket extensions also exercise native standard-library TCP resources. These tests validate the reusable assertions, not every behavior of a production runtime or filesystem. Direct and Tokio run these suites against their real implementations where each capability is supported, with Unix signal and Linux filesystem suites selected explicitly. Native boundary observations and execution gates supplement the shared assertions; the controlled test implementations remain tests of the conformance library itself. Complete measured source-region coverage is a separate requirement from contract coverage, which grows as further edge cases and backend-specific tests are added.

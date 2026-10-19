# Core Scheduler

Explicit scheduler capabilities for reusable Rust protocol libraries. Applications choose an execution environment; libraries ask for the operations they need.

The root crate defines interfaces and runtime-independent support. It contains no executor, reactor, operating-system I/O implementation, or runtime adapter. The initial API is extracted from [Socketry](https://github.com/core-rs/socketry-rust), and is not yet published to crates.io.

The workspace also contains five companion crates, released together with the interface:

- [`core-scheduler-conformance`](crates/conformance/readme.md): reusable capability-specific assertions for backend development dependencies.
- [`core-scheduler-direct`](crates/direct/readme.md): reference implementations of tasks, clocks, native address resolution, file/socket operations, and platform-supported signals and filesystem extensions, with explicit service ownership and bounded syscall admission.
- [`core-scheduler-tokio`](crates/tokio/readme.md): an adapter for an explicitly selected Tokio runtime, preserving owning task handles, owned I/O, cancellation, and bounded native-work admission.

- [`core-scheduler-native`](crates/native/readme.md): Socketry's work-stealing executor and native readiness I/O, with Core ownership and shutdown semantics.
- [`core-scheduler-timers`](crates/timers/readme.md): runtime-independent batched timers, reusable sleep registrations, and deadline notifications.

## Explicit operation context

Keep resources and protocol state in an object, and pass the scheduler and cancellation to each operation. The resulting future borrows that operation's execution context:

```rust
use core_scheduler::{BufferResult, Cancellation, SocketRead};

struct Body<Socket> {
    socket: Socket,
}

impl<Socket> Body<Socket> {
    async fn read<S>(
        &self,
        scheduler: &S,
        cancellation: &Cancellation,
        buffer: Vec<u8>,
    ) -> BufferResult
    where
        S: SocketRead<Socket = Socket>,
    {
        scheduler.socket_read(&self.socket, buffer, cancellation).await
    }
}
```

The body retains no scheduler. Its next operation can use a different compatible scheduler. Associated resource types and backend registration rules determine compatibility; passing a new scheduler does not rebind an outstanding future or an incompatible resource.

## Capabilities

Require individual operations, such as `FileRead`, `SocketRead + SocketWrite`, or `Clock`. `FileScheduler` and `SocketScheduler` aggregate their core operations for consumers that need the complete capability set.

| Capability | Contract |
| --- | --- |
| File operations | Open, close, metadata, cursor and positioned I/O, truncation, and synchronization, using an associated file type. |
| Socket operations | Open, bind, listen, accept, connect, close, shutdown, readiness, and I/O, using an associated socket type. |
| `Clock` | Monotonic observations and cancellable relative or absolute waits. |
| `Spawn` | Primitive task dispatch with implementation-associated, owning completion handles. |
| `Blocking` | Optional bounded admission of owned synchronous work. |
| `Signals` | Optional notification subscriptions with implementation-associated guards and signal identifiers. |
| Borrowed socket I/O | Optional `SocketRecvPoll`, `SocketSendPoll`, `SocketRecvMsgPoll`, and `SocketSendMsgPoll`, accepting stack buffers and I/O vectors. |
| Extension capabilities | Typed socket options, scatter/gather messages, address resolution, standard TCP resource registration, file advice, and allocation. |

Operation futures use static dispatch and are `Send`. File and socket resources are `Send + Sync`; spawned futures and outputs are `Send + 'static`. Local-only execution is outside this initial contract. Independent capabilities can be implemented separately without runtime-selection features.

## Cancellation, ownership, and completion

`Cancellation` is a runtime-independent cooperative signal. Clones share state, children inherit ancestor cancellation, and dropping a signal does not cancel it. Cancellation requests and completed cleanup are separate events.

Owned I/O buffers keep a stable, initialized allocation. Operations return the original owner with success, error, or cancellation. A backend must retain buffers and resources until access ends, including when a caller drops the future. Reads and writes can be partial; there is no implicit read-exact or write-all behavior.

Borrowed socket `_poll` operations are also ordinary async methods: callers `.await` them with explicit cancellation. Each payload access finishes before a poll returns; pending readiness waits retain no payload pointers. Their buffers and `IoSlice` arrays can therefore live on the stack, and dropping or forgetting a pending future leaves no outstanding buffer access. Windows implementations can enable nonblocking mode and leave it enabled; aliases must not concurrently restore blocking mode. Owned operations remain available for completion-based dispatch.

Operation futures are lazy unless explicitly documented otherwise. Pre-cancellation prevents cancellable operation submission. Successful I/O progress wins a concurrent cancellation request; abandoning an operation cannot roll back its effects. Cleanup operations intentionally take no cancellation token so they can finish during graceful shutdown. `Spawn::spawn` and signal subscription register synchronously.

A task handle owns its task: moving transfers ownership, awaiting observes completion, and dropping requests destruction after any active poll returns. Runtime adapters must preserve this contract, including when their host runtime normally detaches on drop. The interface exposes no forced-abort method or implicit task hierarchy.

See the [design and behavioral contracts](context/design.md) for resource compatibility, shutdown, cancellation boundaries, and extraction decisions.

## Development

Install the launcher with `cargo install bake --locked`. The private `bake/` workspace member supplies the shared Core development tasks:

```sh
cargo bake --locked test:coverage --all-targets true
```

The tests cover runtime-independent support, generic composition, reusable conformance assertions, and the Direct, Tokio, and Native backends.

## Contributing

Please open an issue or pull request on [GitHub](https://github.com/core-rs/core-scheduler).

### Agent Context

Run `cargo bake agent:context:install` to install shared context and skills. Use `cargo bake agent:context:list` and `cargo bake agent:context:show` to discover guidance. Follow `agents.md` and the relevant skills under `.agents/skills/`.

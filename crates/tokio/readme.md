# Core Scheduler Tokio

`core-scheduler-tokio` adapts an explicitly selected Tokio runtime to the `core-scheduler` capabilities. It supplies owning tasks, clocks, native address resolution, files, IP sockets, scatter/gather I/O, and bounded blocking work. Unix additionally supports signals; Linux supports file allocation and advice.

```rust
use core_scheduler::{Cancellation, FileClose, FileOpen, FileOptions, FileRead};
use core_scheduler_tokio::TokioScheduler;

# async fn example(path: &std::path::Path, runtime: tokio::runtime::Handle) -> Result<(), Box<dyn std::error::Error>> {
let scheduler = TokioScheduler::new(runtime);
let cancellation = Cancellation::new();
let file = scheduler.file_open(path, FileOptions::READ, &cancellation).await?;
let (result, buffer) = scheduler.file_read(&file, vec![0; 4096], &cancellation).await;
assert!(result? <= buffer.len());
scheduler.file_close(file).await?;
scheduler.shutdown().await;
# Ok(()) }
```

The application owns the Tokio runtime and keeps it alive and driven, with I/O and time enabled. Both current-thread and multithreaded runtimes are supported. Operation futures may be polled by another executor or thread while the selected runtime continues driving its services. Construction takes a runtime handle explicitly; the adapter does not discover an implicit current scheduler or create a runtime. `TokioOptions` configures the dedicated native-work and cleanup pools.

`TokioTask` owns execution lifetime. Retain its handle while a background task is wanted; move the handle to transfer ownership. Dropping it requests abortion after any active poll returns. Normal shutdown uses explicit cooperative cancellation followed by awaiting the task, so it can perform asynchronous cleanup. Task futures and unconsumed outputs have destructor-panic guards; an unobserved panic payload may be leaked if necessary to contain a payload destructor that also panics.

`close()` stops new admission, discards queued general blocking jobs, wakes pending socket and clock operations, and stops signal delivery. Existing task handles remain valid, and accepted native I/O and cleanup retire normally. `shutdown().await` additionally waits for tasks, native work, and cleanup to retire. Coordinate application cancellation and await task handles before shutdown. A task cannot await shutdown of the scheduler that must drain it. A pending task or synchronous call which never returns can prevent shutdown. Dropping the adapter closes admission without joining workers or waiting for active work. Queued capture and signal target destructors can run on the dropping thread. The adapter never shuts down the application's Tokio runtime.

`TokioSocket` retains its readiness registration and runtime identity. Adapter instances selecting the same runtime can share resources between completed operations; an incompatible runtime is rejected before submission. Unix sockets use Tokio's `AsyncFd` reactor, registering only when a native attempt first returns `WouldBlock`. This avoids stale hangup readiness from unconnected TCP sockets; registration failures are reported by the operation which needs to wait. Windows generic sockets use the existing Socketry adapter's `async-io` readiness bridge because Tokio's Unix-style generic registration is unavailable there. The Windows bridge has its own reactor; task execution and clocks still use the selected Tokio runtime. Standard-library TCP imports preserve endpoints and connections while selecting nonblocking mode; external aliases must not change that mode.

Socket syscalls execute synchronously in readiness callbacks and retain no payload pointers after returning. Pending futures own their buffers. Prepared `IoBuffers` and `IoBuffersMut` supply native I/O vectors directly; other sequences reuse scratch capacity across attempts. Completion wins concurrent cancellation. Dropping a pending readiness operation safely releases its input because no outstanding kernel operation retains it. Socket shutdown and explicit close use independent bounded cleanup admission; accepted cleanup survives waiter abandonment.

`TokioFile` privately retains a native file during worker execution, with no runtime affinity. All adapter instances accept the same file type. Native file import and access are unavailable so append flags and descriptor aliases cannot bypass positioned-write and exclusive-close guarantees. File and resolver calls use dedicated bounded workers, rather than copying owned buffers into Tokio file storage. Bounds include queued and running work. Started calls retain their inputs and actual results through retirement, including after waiter abandonment. Await all outstanding uses before explicit close; close requires exclusive ownership and reports one native close attempt without retrying.

Resolver results retain the native `addrinfo` list and matching deallocator, with allocation-free borrowed accessors. Unix signals use a scheduler-owned dispatch thread outside raw handlers and registry locks. Subscription drop stops new notifications without joining callbacks; callback and dispatch-owned target destructor panics are contained. Signal dispositions follow `signal-hook`'s process-wide policy and are not restored on unsubscribe. Linux allocation/advice implement the interface's native semantic contracts; other platforms omit those optional traits.

The companion `core-scheduler-conformance` crate is a development dependency. Shared suites run against real adapter operations, with additional tests for runtime affinity, external polling, ownership, bounded admission, shutdown, native errors, and destructor panics.

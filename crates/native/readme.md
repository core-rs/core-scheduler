# Core Scheduler Native

A work-stealing executor and native readiness backend for Core's explicit scheduler capabilities. Applications select this implementation; libraries depend only on `core-scheduler`.

Tasks run on worker-local queues with remote inboxes and work stealing. Nonblocking sockets use `async-io` readiness. Files, address resolution, and blocking closures use a separate bounded pool; accepted cleanup has an independent pool. Monotonic sleeps use reserved timer storage from `core-scheduler-timers`.

```rust
use core_scheduler::{Cancellation, Clock, Spawn};
use core_scheduler_native::NativeScheduler;
use std::time::Duration;

let scheduler = NativeScheduler::new()?;
scheduler.block_on(async {
    let task = scheduler.spawn(async { 42 }).unwrap();
    assert_eq!(task.await.unwrap(), 42);
    scheduler.sleep(Duration::from_millis(1), &Cancellation::never()).await.unwrap();
    scheduler.shutdown().await;
});
# Ok::<(), std::io::Error>(())
```

Operation futures receive the scheduler and cancellation explicitly. Task handles own execution: dropping a handle requests destruction after any active poll returns. Close stops admission and wakes socket and timer waits, while preserving live task owners. Coordinate cancellation and await tasks before `shutdown().await`, which drains accepted work. Dropping the scheduler closes it without blocking the dropping thread. A task, syscall, or closure that never returns can prevent graceful shutdown.

Sockets have no scheduler-instance affinity. Finish outstanding operations before exclusive close. TCP resources imported from the standard library must remain nonblocking through any external aliases.

`NativeOptions` configures executor workers, blocking workers, admitted blocking jobs, and timer slots. A full timer queue returns `WouldBlock`; a closed scheduler returns `BrokenPipe` for pending clock and socket operations.

The executor and readiness design come from [Socketry](https://github.com/core-rs/socketry-rust/tree/7a70586d5d37fb640d696d0c03a05b2ee3cbe06b/crates/executor). This adapter follows Core's current ownership, cancellation, and asynchronous shutdown contracts. It does not include Socketry's io_uring backend.

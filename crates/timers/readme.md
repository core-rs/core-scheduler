# Core Scheduler Timers

Runtime-independent monotonic timer queues with batched expiry and constant-time cancellation. Capacity can be reserved during setup so registration and cancellation reuse existing storage.

`Timers<Value>` stores values until their deadlines. Dropping a timer handle does not cancel it; use `TimerHandle::cancel` explicitly. Collect expired values and process them outside queue operations.

The `sharded` feature provides worker-owned queues, assistance views, deadline notifications, and lazy sleep futures. The optional `driver` feature provides a bounded producer inbox. Neither feature creates threads or selects an executor.

Extracted from [Socketry](https://github.com/core-rs/socketry-rust/tree/7a70586d5d37fb640d696d0c03a05b2ee3cbe06b/crates/timers), retaining its MIT attribution.

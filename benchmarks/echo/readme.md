# `core-scheduler-echo-benchmark`

The portable multishot API has been consolidated into pooled receives. Historical results below retain their original meanings and can be reproduced at their recorded commits. Current `multi`/`multi-retain` buffer modes and `iouring-multishot` echo mode use the concrete io_uring constructor. The old portable polling control and `tokio-adapter-multishot` echo mode are retired; use pooled receivers for current portable comparisons.

Compare pure Tokio, the Core Tokio adapter, Native readiness/io_uring, and Monoio using the same persistent-connection TCP echo client. The primary comparison uses Core's reusable stream monitors; owned io_uring I/O is retained as a completion-based control. Monoio uses its owned-buffer API with explicitly selected io_uring and epoll drivers. The private workspace package adds no dependencies to the public scheduler interface.

The [initial Aiko results](results.md) include throughput, latency, variation across repetitions, and raw samples.

The [current comparison with Tokio and Monoio](monoio-comparison.md) measures the recommended persistent monitor paths, owned io_uring, and both Monoio drivers on Aiko, with client-placement checks and complete raw results.

The [performance investigation](profiling.md) compares compiler settings, identifies CPU and syscall costs, and measures isolated optimization prototypes.

The [io_uring performance improvements](performance.md) describe the implemented fast paths, their relationship to `io-event`, and repeated before/after measurements.

The historical [borrowed socket comparison](poll-performance.md) measures both adapters' `_poll` paths alongside their owned operations and pure Tokio, and identifies opportunities in the native readiness integration.

The [worker-ring readiness comparison](ring-readiness-performance.md) measures the replacement of the shared readiness fallback with direct nonblocking syscalls and per-worker ring waits.

The [fresh socket-path profiles](readiness-profiling.md) combine optimized-build controls, userspace/kernel sampling, and syscall counters to distinguish idle-wait costs from request and worker bookkeeping.

The [Tokio admission-only comparison](tokio-admission-performance.md) measures removing per-operation scheduler-shutdown listeners, with before/after pure Tokio controls and both adapter buffer interfaces.

The [socket readiness comparison](socket-performance.md) measures reserved Tokio polling waiters and capacity/drain notification reductions with matching before/after benchmark code. It also links the rejected direct-ring interrupt experiments and io-event source review.

The [owner-wake comparison with pure Tokio](tokio-comparison.md) measures the integrated owner-wake changes against pure Tokio and both adapter buffer paths using the same optimized executable for all five backends.

The [readiness and completion comparison](readiness-performance.md) measures cached Tokio readiness and cooperative io_uring task work with matched optimized before/after builds. It also records the rejected short-read shortcut and TCP urgent-data regression.

The [idle notification follow-up](idle-notification-performance.md) measures removing speculative eventfd reads before ring waits and records the other request, wakeup, and compiler experiments.

The [single-worker research](single-worker-research.md) profiles the merged alternating-queue executor, compares matching Tokio read policies, and records the remaining Native readiness costs and rejected ring hints.

The [worker timer research](worker-timers-research.md) tests using timer deadlines in worker I/O waits, distinguishes timer precision from integration costs, and measures the CPU cost of sharing one queue between workers.

The [production worker-local timer integration](worker-local-timers.md) removes the dedicated timer thread, preserves timer ownership across migration, and records matched echo and timer measurements.

The [timer wake-delivery research](timer-ready-research.md) compares immediate delivery with an additional stealable ready action, using the archived [worker-local timer prototype](local-timers-research.md).

The [worker hot-path measurements](worker-hotpath-research.md) and [worker-loop follow-up](worker-loop-followup.md) record the empty admission-queue and unchanged timer-flush improvements, plus the rejected combined-wait and completion-collection experiments.

The [task-admission comparison](task-admission.md) measures round-robin initial worker placement with unchanged idle stealing, including matched echo controls and persistent receiver ring assignments.

The [readiness submission research](readiness-submission-research.md) measures bounded staging of readiness requests and immediate completion publication after ring waits, including the rejected worker-order change and the retained combined variant.

The [multishot receive comparison](multishot-performance.md) measures persistent io_uring `recvmsg` with provided-buffer leases against pure Tokio and Native `_poll`.

Build and run the comparison on Linux:

```sh
python3 benchmarks/echo/run.py --server-cpus 0-3 --client-cpus 4-7 --output /tmp/echo-results.json
```

Check `lscpu -e=CPU,CORE,SOCKET,NODE,CACHE` first: those CPU numbers must refer to separate physical cores, preferably sharing one last-level cache. The runner uses the first `workers` entries from the server CPU set. CPU affinity is optional; omit both options on platforms without `taskset`. io_uring requires Linux; use `--backends tokio tokio-adapter tokio-adapter-poll` elsewhere.

On Linux, the default backends are `tokio`, `tokio-adapter-monitor`, `native-monitor`, `iouring-monitor`, `iouring`, `monoio`, and `monoio-legacy`. Elsewhere the defaults are the two Tokio cases. The default matrix compares 1 and 4 server workers, 1 and 32 connections, and 64-byte and 4 KiB payloads. Each case runs three times with a one-second warmup and three-second measurement. Case and backend order are shuffled with a fixed seed. Adjust `--workers`, `--connections`, `--bytes`, `--repeats`, `--warmup`, and `--seconds` for longer or different workloads. The client has four Tokio workers by default (`--client-workers`).

The runner builds an optimized binary using the checked-in lockfile, then starts separate server and client processes for every sample. Pass `--client-binary /path/to/baseline` to keep an existing client executable fixed across server changes. Raw results record both binary hashes. Servers bind an ephemeral IPv4 loopback port, accept the requested number of connections, and exit after all clients close. Both peers enable `TCP_NODELAY`. Server buffers are 64 KiB and allocated once per connection; writes handle short transfers.

| Backend | Executor | Socket payload I/O |
| --- | --- | --- |
| `tokio` | Tokio | `TcpStream::read` / `write_all` with borrowed buffers |
| `tokio-adapter` | Tokio | Owned `socket_read` / `socket_write`, implemented with readiness |
| `tokio-adapter-poll` | Tokio | Borrowed `socket_recv_poll` / `socket_send_poll`, implemented with readiness |
| `iouring` | Native | Owned reads submit io_uring `Recv`; ready writes complete synchronously, otherwise submit `Send` |
| `iouring-poll` | Native | Borrowed `socket_recv_poll` / `socket_send_poll`, using direct syscalls and ring readiness waits |
| `native-monitor` | Native | Borrowed I/O through persistent stream monitors using the portable readiness selector |
| `monoio` | One local Monoio runtime per worker | Owned `AsyncReadRent` / `AsyncWriteRentExt` with io_uring, no fallback |
| `monoio-legacy` | One local Monoio runtime per worker | The same owned API with Monoio’s epoll driver |
| `tokio-adapter-monitor` | Tokio | Borrowed I/O through separate reusable `SocketStreamMonitor` reader/writer state |
| `iouring-monitor` | Native | Borrowed I/O with persistent directional ring polls and cached stream readiness |
| `iouring-multishot` | Native | Persistent `socket_recvmsg_multishot` with provided-buffer leases; borrowed sends |

Monoio is the published `0.2.4` release, locked alongside the other dependencies. It uses one local runtime per worker and a 256-entry ring for each io_uring runtime. Setup accepts the predetermined connection count and distributes sockets round-robin before registering them on their owning runtime. Connections never move between Monoio workers; Core and Tokio may schedule work across workers. Workers without connections finish immediately. These are runtime-model differences, not only interface-dispatch costs. The process affinity mask is shared by each server's worker threads; this benchmark does not pin individual Monoio threads. Setup, connection registration, and accept policy are outside the measured workload after warmup. Socket mode matches Monoio's own accept path: nonblocking for epoll and blocking for io_uring. Both use one initialized 64 KiB allocation per connection and complete partial writes before reusing it.

The adapters share one generic owned echo loop and one generic borrowed echo loop. The borrowed loop uses default send/receive flags and keeps the buffer in its connection future. Ready calls can finish in the same task poll; calls returning `WouldBlock` wait for readiness. The io_uring configurations retain one ring per worker, 256 SQ entries per ring, and admission capacity of at least twice the connection count, with a minimum of 256. `iouring-poll` does not submit payload transfers to those rings; it submits `PollAdd` readiness waits only when a syscall would block. Listener handling uses the readiness fallback. Comparing owned and borrowed modes therefore measures a change of I/O mechanism for the native backend, while both Tokio adapter modes already use readiness. The historical borrowed-socket report records the earlier shared-readiness fallback implementation.

The multishot case uses eight 64 KiB payload buffers per connection, plus receive metadata, rather than the other cases' single 64 KiB buffer. It echoes directly from each completed lease, then returns it to the original ring. Kernel rearming avoids a new receive submission per message. Exhausting the bounded pool pauses reception until a lease returns. Its persistent receiver consumes one admission slot, so the capacity above leaves room for sends that need to wait.

The client connects and allocates buffers and histograms before warmup. All connections use one shared measurement window. Each sends one complete payload, waits for its full echo, verifies the bytes, and then sends the next request. Only round trips entirely inside the measurement window are counted. Neither connection setup nor warmup contributes to the reported throughput or latency histogram. One request means one round trip; payload MiB/s counts bytes in one direction, so aggregate TCP payload traffic is twice that figure.

Latency is round-trip latency as seen by this closed-loop client, with three significant digits in the HDR histogram. It includes client scheduling and loopback networking. This is not an open-loop arrival-rate test, does not correct for coordinated omission, and does not measure connection churn or pipelining. The comparison measures these concrete implementations and ownership paths, not just trait-dispatch overhead. A client bottleneck can limit every server; verify client headroom before treating throughput as server capacity. Loopback results do not predict physical-network performance.

Raw JSON preserves every repetition, configuration, CPU topology, source revision, toolchain, and load averages. Compare medians and variation across repetitions rather than selecting the fastest run. Keep frequency policy and affinity consistent; the runner does not change machine-wide settings.

To run the binaries directly:

```sh
cargo build --release --locked -p core-scheduler-echo-benchmark
# Prints an address; remains running until the four clients close:
target/release/core-scheduler-echo-benchmark server iouring 4 4
# Substitute the printed address:
target/release/core-scheduler-echo-benchmark client 127.0.0.1:PORT 4 4 64 1 3
```

Smoke tests exercise each available backend, payload validation, invalid inputs, and empty measurement windows. A controlled socket fixture verifies partial writes, zero-progress errors, and buffer reuse:

```sh
cargo test --locked -p core-scheduler-echo-benchmark
```

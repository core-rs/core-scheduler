# Native socket overhead

This investigation separates receive policy from scheduler overhead, removes unnecessary readiness synchronization and routing ownership, and reuses owned scalar receive requests. The public reader/writer interface is unchanged. Borrowed operations still retain no payload pointer between polls, and owned requests retain their buffer through original completion.

## What accounts for the gap

The [matched Tokio control](results/tokio-parity/diagnostics/tokio-policy-confirmation.json) measures 717,387 echoes/s for ordinary Tokio, 666,068 for pure Tokio retaining readiness until `WouldBlock`, and 656,760 for Core's Tokio adapter. That puts the Tokio adapter 1.4% behind the control with the same conservative policy, versus 8.5% behind ordinary Tokio. Five randomized repetitions use one worker, 32 connections, and 64-byte echoes.

Tokio's ordinary TCP read clears readiness after a short successful read in this dependency version. Core preserves readiness until `WouldBlock`: a short read at an urgent-data boundary can leave ordinary bytes queued, including after another alias consumes the urgent byte. The [existing regression](readiness-performance.md) demonstrates that distinction. The [control patch](results/tokio-parity/diagnostics/tokio-conservative.patch) also changes readiness APIs, `ReadBuf` handling, and cooperative budgeting, so its entire difference must not be attributed to one syscall.

Separate [instrumented counters](results/tokio-parity/diagnostics/normalized-stats.json) confirm approximately one receive syscall per echo in ordinary Tokio and two in both the conservative control and adapter. Instrumented throughput is not capacity evidence.

Fresh Native profiles identified waiter claim/cleanup, readiness updates, routing, and owned request lifecycle work. These are userspace samples, not isolated costs. In particular, route selection accounts for 10.69% of one [userspace profile](results/tokio-parity/diagnostics/perf-iouring-monitor-w1-c32-b64-r1.self.txt), while that process consumes much more system CPU than user CPU. The [borrowed-route screen](results/tokio-parity/diagnostics/route-screen.json) establishes no substantial independent throughput improvement.

## Selected changes

- Readiness notifications and consumed generations use ordinary atomic loads/stores under their existing ownership rules. The exclusive resource borrow replaces a separate atomic waiter claim. A cleanup guard still releases an abandoned wait's waker; normal notification-driven completion avoids its cleanup mutex. An arbitrary Arc-owned task regression checks that abandoning a child read cannot retain its parent.
- Waits borrow their chosen controller from the selector's immutable controller vector. TLS supplies only an index for the matching selector. An accepted wait keeps that reference across task migration; backend registrations retain their own ownership through retirement.
- Bound scalar owned stream reads lazily reuse typed request and completion storage, following the existing file-resource pattern. The empty cached request retains no socket, permit, or buffer. Changing buffer-owner type replaces it; cancellation and abandonment preserve original-completion ownership. A real receive test requires zero fresh allocations after warmup with existing capacity and no transient readiness retry.

The [owned-read screen](results/owned-receive-cache/readme.md) isolates request reuse from smaller admission/retirement changes: it improves the one-worker/32-connection owned path by 6.6% against merged main, and 5.9% against those smaller changes alone. Its remaining gap to ordinary Tokio is 8.6%. These are completion-based receives with the caller's retained owner, rather than borrowed-buffer substitutions.

## Final matched comparison

The [final matrix](results/tokio-parity/final-matrix.json) contains 144 randomized samples: eight worker/connection/payload cases, six variants, and three repetitions. The [selected source and binary hash](results/tokio-parity/final-source.txt) identify the production snapshot. Both Native paths retain the original post-batch worker loop. Rates below are median validated echoes per second; percentage changes compare those medians.

### Borrowed stream receives

`iouring-monitor` is the Native stream reader using caller-retained storage through `recv_poll`. It does not allocate or use the owned receive cache.

| Workers | Connections | Bytes | Baseline | Selected | Change | Ordinary Tokio | Selected vs Tokio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 64 | 271,188 | 270,045 | -0.42% | 278,547 | -3.05% |
| 1 | 32 | 64 | 626,718 | 634,075 | +1.17% | 707,582 | -10.39% |
| 4 | 1 | 64 | 257,786 | 262,369 | +1.78% | 271,498 | -3.36% |
| 4 | 32 | 64 | 1,747,350 | 1,746,908 | -0.03% | 1,775,382 | -1.60% |
| 1 | 1 | 4096 | 240,666 | 238,439 | -0.93% | 243,216 | -1.96% |
| 1 | 32 | 4096 | 568,734 | 573,963 | +0.92% | 625,276 | -8.21% |
| 4 | 1 | 4096 | 240,974 | 240,619 | -0.15% | 238,708 | +0.80% |
| 4 | 32 | 4096 | 1,612,974 | 1,602,446 | -0.65% | 1,615,897 | -0.83% |

These bookkeeping changes leave borrowed throughput close to baseline: -0.93% to +1.78%. Small deltas are not established independent improvements. Against ordinary Tokio, the busy single-worker cases still trail by 8.2–10.4%; the remaining six cases range from 3.4% behind to 0.8% ahead. Against conservative Tokio in this matrix, the busy single-worker gap is 4.1–4.4%, while all other borrowed cases are within 4.0%.

### Owned scalar stream receives

`iouring` transfers the buffer owner to the completion-based reader. Submitted requests retain that owner through CQE retirement; substituting the borrowed path would change what this benchmark measures. The public owned-buffer contract also permits synchronous completion before submission.

| Workers | Connections | Bytes | Baseline | Selected | Change | Ordinary Tokio | Selected vs Tokio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 64 | 227,685 | 243,766 | +7.06% | 278,547 | -12.49% |
| 1 | 32 | 64 | 607,193 | 647,347 | +6.61% | 707,582 | -8.51% |
| 4 | 1 | 64 | 203,730 | 233,417 | +14.57% | 271,498 | -14.03% |
| 4 | 32 | 64 | 1,748,692 | 1,718,501 | -1.73% | 1,775,382 | -3.20% |
| 1 | 1 | 4096 | 231,750 | 236,695 | +2.13% | 243,216 | -2.68% |
| 1 | 32 | 4096 | 549,722 | 586,912 | +6.77% | 625,276 | -6.14% |
| 4 | 1 | 4096 | 234,966 | 238,514 | +1.51% | 238,708 | -0.08% |
| 4 | 32 | 4096 | 1,585,532 | 1,574,983 | -0.67% | 1,615,897 | -2.53% |

Request reuse provides the clearest gain: +6.6–6.8% at one worker/32 connections, +7.1% for one worker/one 64-byte connection, and +14.6% for four workers/one 64-byte connection. The four-worker/32-connection changes are -1.7% and -0.7%; there is no universal throughput improvement.

The busy single-worker owned path is now 2.1–2.2% behind the conservative Tokio control, while remaining 6.1–8.5% behind ordinary Tokio. For one 64-byte connection it is still 12.5–14.0% behind ordinary Tokio. Completion-based ownership has a different submission/retirement path from borrowed synchronous syscalls; this patch removes repeated allocation but does not remove that distinction or establish complete parity. At four workers/32 connections, both Native paths are within 3.2% of ordinary Tokio.

Owned P99 improves from 56.959 to 53.631 µs for one worker/32 connections/64 bytes, and from 62.911 to 59.423 µs for the corresponding 4 KiB case. Other P99 changes vary; the JSON preserves every latency and throughput sample rather than deriving latency from throughput. Three repetitions do not establish a tight confidence interval.

The [receive-policy and ring investigation](native-short-read.md) continues this comparison with a Native short-read diagnostic, syscall counts, CPU profiles, and controlled ring task-work experiments.

## Rejected alternatives

Explicit `inline(always)` annotations produced a byte-identical Native `.text` section in the [compiler screen](results/tokio-parity/diagnostics/native-compiler-screen.json). Thin LTO changed Native throughput by only +0.65% in that short screen. Additional Tokio adapter prototypes produced no useful measured gain. No profile or Tokio production change is selected.

Deferred io\_uring task work improves the busy one-worker path but retains a low-load regression. Combining it with the rejected pre-batch loop ordering gives 679,917 versus 633,766 echoes/s at one worker/32 connections, but 235,453 versus 262,632 at four workers/one connection in the [three-repeat screen](results/tokio-parity/diagnostics/deferred-before-screen.json). The latter is a 10.3% regression. The implementation therefore keeps cooperative task work; the deferred-ring research remains separate in [#35](https://github.com/core-rs/core-scheduler/pull/35).

The [loop-order controls](results/tokio-parity/diagnostics/loop-order/readme.md) also reject moving the driver turn before each ready batch. That order helped some borrowed-read screens but regressed four-worker/single-connection owned reads. Restoring the original post-batch order recovers 16.7% in the isolated control. Skipping a redundant turn after idle completion recovers only 1.0%, so it does not explain the larger effect. Owned request preparation conservatively returns to the executor because buffer accessors may wake arbitrary tasks; moving it into the idle path adds an idle handshake around that productive work. The selected patch keeps the original worker loop and adds no scheduling state. Restoring the lane's general Event instead of AtomicWaker has no established throughput benefit in its separate control.

## Reproduction and limits

The merged baseline is `1840258bbf9cf10c08fefa26fbff54a747f18d08`. The selected optimized production snapshot is `22b246853c609264f9112554075bf49eb2cc9af2`; later changes make a separate write-benchmark boundary test deterministic and add reporting; the measured echo code is unchanged. The earlier `49202e5` snapshot includes the rejected pre-batch loop order. Build with `CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --release --locked -p core-scheduler-echo-benchmark`, without LTO or CPU overrides.

Measurements run on Aiko's Ryzen 9 9950X3D, Linux 7.2.8, Rust 1.99.0/LLVM 23.1.1. Servers use CPU 0 or CPUs 0–3; the unchanged four-worker validating client uses CPUs 4–7, on separate physical cores in the same L3 group. Every build, profile, test, and timed run holds `/tmp/core-scheduler-perf.lock`. The runner randomizes variants and cases, using one second warmup and three measured seconds, and retains executable hashes and every sample. The client hash is `c72f7aea7151fb83020b096cfdaccc46a70b0eeff6bbb0a0e4ea4157d1aeb071`.

This is a closed-loop loopback workload with one outstanding echo per connection, reused 64 KiB storage, TCP\_NODELAY, complete partial writes, and validated payloads. It does not establish physical-network performance or saturation. Unrelated game/Wine processes were observed elsewhere on Aiko during the routing screen, with broad CPU affinity; their configuration was not changed. Small differences require matched controls and should not be treated as established gains.

The [comparison runner](results/tokio-parity/compare.py) accepts frozen `--before`, `--after`, `--client`, and optional conservative Tokio `--control` binaries, plus backend/case/repetition settings. The [diagnostic source snapshots](results/tokio-parity/diagnostics) preserve the separately screened variants; those intermediate variants are not all retained production changes. In particular, the early exclusive-wait prototype omitted abandoned-waker cleanup, which the combined implementation restores.

# Echo comparison with Tokio and Monoio

Core's Tokio and Native io_uring monitor paths are close to pure Tokio across this fixed echo matrix, but the busy single-worker cases still miss a 5% throughput target. Monoio is broadly comparable to Tokio. The portable Native readiness selector remains substantially slower than the other fast paths.

These are closed-loop loopback workload measurements, including the shared client and networking. They are not independent measurements of server saturation or physical-network capacity. Three repetitions reveal repeatability for this run, not statistical equivalence across machines or workloads.

## Summary

The normalized result is the geometric mean of each backend's median round trips per second divided by the matched Tokio median across eight configurations. Each configuration has equal weight; this is not an average of raw throughput or a production traffic mix.

| Backend | Throughput relative to Tokio | Difference |
| --- | ---: | ---: |
| Tokio | 1.000× | +0.0% |
| Core Tokio monitors | 0.965× | -3.5% |
| Core Native readiness monitors | 0.685× | -31.5% |
| Core Native io_uring monitors | 0.960× | -4.0% |
| Core Native owned io_uring | 0.904× | -9.6% |
| Monoio io_uring | 0.985× | -1.5% |
| Monoio epoll | 0.991× | -0.9% |

For 64-byte requests, representative median round trips per second are:

| Backend | 1 worker, 1 connection | 1 worker, 32 connections | 4 workers, 32 connections |
| --- | ---: | ---: | ---: |
| Tokio | 278,179 | 707,544 | 1,779,701 |
| Core Tokio monitors | 274,362 | 648,115 | 1,677,153 |
| Core Native readiness monitors | 170,329 | 416,147 | 1,384,362 |
| Core Native io_uring monitors | 269,579 | 627,743 | 1,741,807 |
| Core Native owned io_uring | 224,936 | 620,795 | 1,725,868 |
| Monoio io_uring | 269,965 | 713,297 | 1,696,496 |
| Monoio epoll | 275,290 | 686,538 | 1,774,294 |

The important gaps are concentrated rather than uniform:

- With one worker and 32 connections, the Core Tokio monitor adapter trails Tokio by 8.4% at 64 bytes and 7.1% at 4 KiB. Core Native io_uring monitors trail by 11.3% and 9.3%. Those repetitions do not overlap the Tokio throughput range.
- With four workers and 32 connections, Core Native io_uring monitors are 2.1% behind Tokio at 64 bytes and 1.3% behind at 4 KiB. The Tokio adapter is 5.8% and 3.8% behind.
- Single-connection cases for Core's Tokio and Native io_uring monitors range from 3.2% behind to 0.2% ahead of Tokio. Differences this small should not be treated as a definitive ranking.
- Monoio io_uring is 0.8–1.3% ahead with one worker and 32 connections, and 4.7–4.8% behind with four workers and 32 connections. Monoio epoll is within 3% across this matrix.
- Core Native readiness monitors trail by 22–41%. Core's owned io_uring path is also noticeably slower for a single connection sending 64-byte requests, despite being close for 4 KiB.

The next focused investigation should be the one-worker/32-connection case: the Tokio adapter and Native io_uring monitor gaps repeat at both sizes, while Monoio's local io_uring runtime keeps up. This benchmark does not identify the cause. CPU profiles and scheduler/syscall counters are needed to distinguish monitor bookkeeping, task scheduling, and I/O policy before changing the implementation.

## Method and provenance

Measured on Aiko on 2026-10-23 UTC using the clean benchmark commit [`cf67e7c`](https://github.com/core-rs/core-scheduler/commit/cf67e7c81b5ba4a72f5c045e29c0251f27c54e14), based on merged #31 (`eba10b5`). Later commits add diagnostics and this report without changing the measured Rust code. [Raw primary results](results/monoio/aiko.json) contain every sample, source revision, binary hashes, CPU topology, load, and command arguments.

- AMD Ryzen 9 9950X3D, Linux `7.2.8-arch1-2`, Rust/Cargo `1.99.0`.
- `cargo build --release --locked`: standard optimized release profile, no custom LTO or CPU flags. One executable supplies every server and the common Tokio client.
- Published Monoio `0.2.4` and Tokio `1.53.3`, fixed by `Cargo.lock`.
- Seven backends × two worker counts (1, 4) × two connection counts (1, 32) × two payload sizes (64 bytes, 4 KiB) × three repetitions = 168 samples. Shuffled order with a fixed seed.
- One-second warmup followed by a shared three-second measurement window. Setup, buffer allocation, and cleanup are outside that window. Only complete round trips within it count.
- Server process affinity uses the first `workers` CPUs from 0–3. The four-worker client uses CPUs 4–7. These are distinct physical cores within the same L3 group, without SMT sibling sharing. Individual worker threads are not pinned within their process masks.
- IPv4 loopback, `TCP_NODELAY`, persistent connections, one outstanding request per connection, content validation, and complete partial-write handling. Each server connection reuses one initialized 64 KiB buffer.
- Round-trip latency includes client scheduling. Histograms are closed-loop and do not correct coordinated omission. Throughput is round trips per second; byte throughput in the JSON counts payload in one direction.

The server and client SHA-256 is `c72f7aea7151fb83020b096cfdaccc46a70b0eeff6bbb0a0e4ea4157d1aeb071`. The supplemental checks use that same executable.

```sh
python3 benchmarks/echo/run.py --server-cpus 0-3 --client-cpus 4-7 --repeats 3 --warmup 1 --seconds 3 --output /tmp/core-scheduler-monoio-aiko.json
```

## What is being compared

Core's recommended borrowed paths reuse separate readable and writable stream monitors. The Native io_uring monitor path uses direct nonblocking payload syscalls with persistent ring readiness; it does not send every payload through an io_uring submission. The owned io_uring control submits receives and can complete ready sends synchronously. The Tokio adapter also uses readiness, so its comparison is narrower, but still includes monitor and operation semantics beyond trait dispatch. The portable Native readiness case uses the readiness selector rather than ring readiness.

Monoio uses its owned-buffer read/write API with explicit io_uring and legacy epoll drivers; driver failure does not silently select a fallback. Its [runtime model](https://github.com/monoio-rs/monoio) keeps connection tasks local to each runtime. This benchmark distributes accepted connections round-robin during setup and runs one local Monoio runtime per server worker, with 256 SQ entries for each io_uring runtime. Connections never migrate between Monoio workers; Core and Tokio can schedule tasks across workers. Workers with no connections exit, so in Monoio's four-worker/one-connection case only one worker remains active. Monoio's socket mode follows its own accept implementation: blocking sockets for io_uring, nonblocking sockets for epoll. Both complete partial writes and recover the same buffer owner for the next read.

This compares practical implementations of the same echo behavior, including their ownership and scheduling models. It does not isolate only the cost of a trait, the kernel interface, or the executor. The [runner documentation](readme.md) describes all available modes.

## Client capacity and CPU placement

Two supplemental probes use the identical binary, four server workers on CPUs 0–3, 32 connections, both payload sizes, one-second warmup, and a three-second measured window. Each configuration runs twice in shuffled order. [The diagnostic script](results/monoio/headroom.py) records full-process CPU time with `wait4`, including all threads. CPU time includes setup, warmup, and cleanup; the elapsed-time estimate includes polling/reaping delay. It is approximate whole-run utilization, not a synchronized timed-region profile or per-thread saturation measurement.

The [first probe](results/monoio/aiko-mixed-cache-headroom.json) compares four client workers on CPUs 4–7 with eight on CPUs 4–11 across all seven backends (56 samples). Four-client measurements broadly reproduce the primary suite, with about 2.0–2.4 client cores used on average. Eight clients cross into the second L3 group and sometimes collapse throughput across every backend. That experiment changes both client parallelism and placement, so it cannot establish a shortage of client CPU or a server capacity limit. The collapse's specific cause has not been profiled.

The [control probe](results/monoio/aiko-same-cache-headroom.json) keeps both client sizes within the second L3 group: four on CPUs 8–11, eight on CPUs 8–15 (24 samples). Server CPUs remain 0–3. It avoids the mixed-client-cache configuration but still measures a different server/client placement from the primary suite. Median results are:

| Backend | Payload | 4 clients, round trips/s | 8 clients, round trips/s | Change | Client CPU cores, 4 → 8 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Tokio | 64 B | 943,854 | 991,222 | +5.0% | 2.02 → 2.69 |
| Tokio | 4,096 B | 823,183 | 844,142 | +2.5% | 2.11 → 2.86 |
| Core Native io_uring monitors | 64 B | 981,212 | 1,179,829 | +20.2% | 2.02 → 3.07 |
| Core Native io_uring monitors | 4,096 B | 843,785 | 983,211 | +16.5% | 2.13 → 3.24 |
| Monoio io_uring | 64 B | 923,824 | 1,176,527 | +27.4% | 1.91 → 3.03 |
| Monoio io_uring | 4,096 B | 813,288 | 1,064,712 | +30.9% | 2.06 → 3.41 |

More client workers help some of these cross-cache workloads, but none recovers the primary same-cache throughput. Backend rankings also change. This demonstrates that the complete workload is sensitive to client parallelism and placement; average CPU utilization below four cores does not rule out client scheduling, individual-thread, kernel, or synchronization limits. Use the primary results as a controlled comparison at its stated placement, not as maximum server throughput. A capacity study should vary connection count and offered load, verify per-thread/CPU headroom, and use a separate physical client where appropriate.

To reproduce the diagnostics on the recorded Aiko topology:

```sh
python3 benchmarks/echo/results/monoio/headroom.py --binary target/release/core-scheduler-echo-benchmark --client-cpu-start 4 --output /tmp/mixed-cache-headroom.json
python3 benchmarks/echo/results/monoio/headroom.py --binary target/release/core-scheduler-echo-benchmark --client-cpu-start 8 --backends tokio iouring-monitor monoio --output /tmp/same-cache-headroom.json
```

These CPU lists are machine-specific. Check physical cores and L3 groups before reusing them elsewhere. Both supplemental files record the primary binary hash, per-sample CPU masks, topology, load, and process counters.

## Full primary results

Each row gives the median and observed minimum–maximum throughput across three repetitions. The p99 column is the median of each run's p99 round-trip latency, not a percentile of a merged histogram. Rounding is only for display; ratios use unrounded values.

### 1 worker(s), 1 connection(s), 64-byte payload

| Backend | Median round trips/s | Min–max | Versus Tokio | Median p99, µs |
| --- | ---: | ---: | ---: | ---: |
| Tokio | 278,179 | 277,893–279,245 | +0.0% | 4.20 |
| Core Tokio monitors | 274,362 | 273,573–275,843 | -1.4% | 4.38 |
| Core Native readiness monitors | 170,329 | 162,917–174,795 | -38.8% | 6.95 |
| Core Native io_uring monitors | 269,579 | 267,653–273,511 | -3.1% | 4.32 |
| Core Native owned io_uring | 224,936 | 222,483–244,217 | -19.1% | 9.34 |
| Monoio io_uring | 269,965 | 268,820–272,828 | -3.0% | 4.36 |
| Monoio epoll | 275,290 | 271,419–278,381 | -1.0% | 5.47 |

### 1 worker(s), 1 connection(s), 4,096-byte payload

| Backend | Median round trips/s | Min–max | Versus Tokio | Median p99, µs |
| --- | ---: | ---: | ---: | ---: |
| Tokio | 239,212 | 238,400–244,795 | +0.0% | 5.21 |
| Core Tokio monitors | 239,570 | 238,690–242,107 | +0.1% | 5.32 |
| Core Native readiness monitors | 178,775 | 163,208–179,630 | -25.3% | 6.66 |
| Core Native io_uring monitors | 239,782 | 236,542–245,230 | +0.2% | 5.26 |
| Core Native owned io_uring | 240,782 | 239,458–241,666 | +0.7% | 5.27 |
| Monoio io_uring | 240,022 | 236,200–240,613 | +0.3% | 5.33 |
| Monoio epoll | 244,824 | 244,401–251,748 | +2.3% | 5.22 |

### 1 worker(s), 32 connection(s), 64-byte payload

| Backend | Median round trips/s | Min–max | Versus Tokio | Median p99, µs |
| --- | ---: | ---: | ---: | ---: |
| Tokio | 707,544 | 705,877–709,470 | +0.0% | 52.83 |
| Core Tokio monitors | 648,115 | 645,402–650,960 | -8.4% | 56.93 |
| Core Native readiness monitors | 416,147 | 414,903–417,501 | -41.2% | 114.88 |
| Core Native io_uring monitors | 627,743 | 627,420–631,962 | -11.3% | 55.20 |
| Core Native owned io_uring | 620,795 | 619,349–630,527 | -12.3% | 55.30 |
| Monoio io_uring | 713,297 | 710,813–721,070 | +0.8% | 49.02 |
| Monoio epoll | 686,538 | 685,079–694,895 | -3.0% | 51.68 |

### 1 worker(s), 32 connection(s), 4,096-byte payload

| Backend | Median round trips/s | Min–max | Versus Tokio | Median p99, µs |
| --- | ---: | ---: | ---: | ---: |
| Tokio | 625,169 | 622,175–625,318 | +0.0% | 59.49 |
| Core Tokio monitors | 580,614 | 578,882–581,232 | -7.1% | 63.36 |
| Core Native readiness monitors | 372,021 | 371,257–373,858 | -40.5% | 129.15 |
| Core Native io_uring monitors | 567,075 | 563,344–567,594 | -9.3% | 61.02 |
| Core Native owned io_uring | 560,520 | 558,189–563,231 | -10.3% | 61.38 |
| Monoio io_uring | 633,441 | 632,488–633,828 | +1.3% | 55.30 |
| Monoio epoll | 610,298 | 603,991–611,772 | -2.4% | 57.82 |

### 4 worker(s), 1 connection(s), 64-byte payload

| Backend | Median round trips/s | Min–max | Versus Tokio | Median p99, µs |
| --- | ---: | ---: | ---: | ---: |
| Tokio | 274,781 | 273,998–279,044 | +0.0% | 4.34 |
| Core Tokio monitors | 275,154 | 273,995–277,302 | +0.1% | 4.38 |
| Core Native readiness monitors | 193,491 | 172,605–201,115 | -29.6% | 6.19 |
| Core Native io_uring monitors | 265,955 | 262,360–266,982 | -3.2% | 5.56 |
| Core Native owned io_uring | 206,165 | 203,839–208,603 | -25.0% | 7.25 |
| Monoio io_uring | 273,710 | 271,706–275,341 | -0.4% | 4.36 |
| Monoio epoll | 267,532 | 266,689–270,343 | -2.6% | 5.50 |

### 4 worker(s), 1 connection(s), 4,096-byte payload

| Backend | Median round trips/s | Min–max | Versus Tokio | Median p99, µs |
| --- | ---: | ---: | ---: | ---: |
| Tokio | 244,679 | 236,508–246,110 | +0.0% | 5.27 |
| Core Tokio monitors | 241,149 | 239,626–245,078 | -1.4% | 5.45 |
| Core Native readiness monitors | 180,424 | 178,391–181,951 | -26.3% | 7.06 |
| Core Native io_uring monitors | 241,580 | 240,666–243,801 | -1.3% | 5.29 |
| Core Native owned io_uring | 240,667 | 239,099–240,746 | -1.6% | 5.29 |
| Monoio io_uring | 241,432 | 236,395–241,564 | -1.3% | 5.37 |
| Monoio epoll | 247,848 | 247,366–248,863 | +1.3% | 5.20 |

### 4 worker(s), 32 connection(s), 64-byte payload

| Backend | Median round trips/s | Min–max | Versus Tokio | Median p99, µs |
| --- | ---: | ---: | ---: | ---: |
| Tokio | 1,779,701 | 1,728,643–1,802,885 | +0.0% | 51.58 |
| Core Tokio monitors | 1,677,153 | 1,667,394–1,712,764 | -5.8% | 52.38 |
| Core Native readiness monitors | 1,384,362 | 1,366,466–1,435,781 | -22.2% | 56.13 |
| Core Native io_uring monitors | 1,741,807 | 1,741,031–1,769,506 | -2.1% | 51.87 |
| Core Native owned io_uring | 1,725,868 | 1,724,204–1,735,097 | -3.0% | 53.60 |
| Monoio io_uring | 1,696,496 | 1,691,036–1,705,435 | -4.7% | 57.41 |
| Monoio epoll | 1,774,294 | 1,768,138–1,777,546 | -0.3% | 54.08 |

### 4 worker(s), 32 connection(s), 4,096-byte payload

| Backend | Median round trips/s | Min–max | Versus Tokio | Median p99, µs |
| --- | ---: | ---: | ---: | ---: |
| Tokio | 1,635,762 | 1,603,081–1,637,225 | +0.0% | 59.42 |
| Core Tokio monitors | 1,572,922 | 1,552,334–1,573,855 | -3.8% | 59.33 |
| Core Native readiness monitors | 1,223,560 | 1,210,251–1,224,765 | -25.2% | 65.22 |
| Core Native io_uring monitors | 1,613,988 | 1,608,622–1,636,127 | -1.3% | 60.29 |
| Core Native owned io_uring | 1,595,434 | 1,554,478–1,616,652 | -2.5% | 63.13 |
| Monoio io_uring | 1,557,543 | 1,521,602–1,561,369 | -4.8% | 66.56 |
| Monoio epoll | 1,618,031 | 1,613,753–1,622,955 | -1.1% | 61.92 |

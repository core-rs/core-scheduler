# Native receive policy and ring overhead

The busy single-worker TCP echo gap has at least two measurable components: receive policy and io\_uring completion work. Matching Tokio's scalar receive policy reduces the gap but leaves about 5–6%. Changing the ring's task-work policy then brings that diagnostic busy path close to Tokio. Neither prototype is selected for production: the receive policy fails the urgent-data check below, and deferred task work regresses low-load throughput.

These experiments isolate changes from the merged production implementation. They change neither the public reader/writer interface nor its ownership requirements. The receive-policy prototype affects only borrowed scalar stream reads; owned, vectored, and packet receives keep their existing behavior.

## Policy under test

The production Native path tries a nonblocking receive on every poll and preserves successful-read readiness until `WouldBlock`. The [prototype patch](results/native-short-read/source.patch) instead consults cached readiness before subsequent established-stream reads and clears the notification captured before a positive short receive. Newer notifications survive. The first read retains its immediate syscall behavior, and empty reads, EOF handling, cancellation, writes, routing, ring configuration, and the worker loop keep their existing behavior.

Clearing readiness alone would leave the next call's speculative receive in place. The experiment therefore tests both readiness-first gating and short-read clearing. It reproduces Tokio's relevant scalar policy, rather than substituting Tokio's executor or epoll reactor into Native. Code generation, resource state, and readiness gating also change, so the measured effect cannot be attributed exclusively to one avoided syscall.

## Short-read comparison

The [120 raw samples](results/native-short-read/matrix.json) retain all latency measurements, executable hashes, and the randomized order. The [source record](results/native-short-read/source.txt) identifies the two changed implementation files. Rates below are medians of five repetitions.

| Workers | Connections | Bytes | Current Native | Short-read Native | Tokio | Native change | Short-read vs Tokio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 64 | 274,338 | 272,011 | 273,306 | -0.85% | -0.47% |
| 1 | 32 | 64 | 634,040 | 669,624 | 707,304 | +5.61% | -5.33% |
| 4 | 1 | 64 | 259,864 | 269,052 | 272,202 | +3.54% | -1.16% |
| 4 | 32 | 64 | 1,729,365 | 1,744,055 | 1,755,751 | +0.85% | -0.67% |
| 1 | 1 | 4096 | 238,646 | 233,494 | 238,246 | -2.16% | -1.99% |
| 1 | 32 | 4096 | 572,545 | 599,377 | 635,167 | +4.69% | -5.63% |
| 4 | 1 | 4096 | 239,705 | 238,110 | 239,057 | -0.67% | -0.40% |
| 4 | 32 | 4096 | 1,578,878 | 1,585,408 | 1,602,510 | +0.41% | -1.07% |

The busy single-worker path improves 4.7–5.6%, but still trails Tokio by 5.3–5.6%. The other six cases are within 2.0% of Tokio in this matrix. This is not a universal improvement: one worker/one 4 KiB connection is 2.2% slower than current Native. Small differences require caution, and the complete sample ranges remain available.

## Correctness check

The same [bound-reader urgent-data test](results/native-short-read/urgent-data.rs) passes on the current implementation and fails on the prototype: the next read stalls for 100 ms while a direct receive retrieves the already queued ordinary suffix. The [baseline log](results/native-short-read/baseline-urgent.log) and [prototype log](results/native-short-read/candidate-urgent.log) preserve both outcomes. The prototype therefore remains a diagnostic patch, rather than a production change.

## Receive syscall check

A separate [counter run](results/native-short-read/counts.json) measures about 2.000 receives per echo for current Native and 1.000 for both short-read Native and ordinary Tokio. The [counter runner](results/native-short-read/counts.py) uses zero warmup so counted requests and syscalls cover the same interval, apart from startup and EOF. Syscall tracepoints require elevated perf access on Aiko; this run uses `sudo -n perf`. Instrumented throughput is not capacity evidence.

## Residual CPU cost

The [matched CPU and syscall counters](results/native-short-read/profiles/stats.json) use three randomized repetitions per variant, with no warmup and three seconds measured. They confirm that short-read Native makes fewer event-wait syscalls (about 0.021 io\_uring entries per echo) than Tokio (about 0.042 epoll waits). Excess syscall count is therefore not an explanation for the remaining gap in this workload.

Most of the additional measured cycles occur in kernel execution: median short-read Native kernel cycles are about 7,448 per echo, versus 7,227 for Tokio; userspace medians are about 775 versus 753. The [Native profile](results/native-short-read/profiles/short-read.self.txt) identifies `tctx_task_work_run`, `task_work_run`, and `io_poll_task_func` as additional kernel work. Samples identify candidates rather than isolated costs; the [profiling runner](results/native-short-read/profile.py) preserves the measurement setup.

## Ring task-work controls

The production thread-bound ring requests `COOP_TASKRUN | TASKRUN_FLAG`. Cooperative task work can run on return from ordinary syscalls, including socket sends and receives. `DEFER_TASKRUN` instead moves that work to explicit io\_uring entries carrying `GETEVENTS`; it requires `SINGLE_ISSUER` and a thread-bound owner. See the [kernel setup contract](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html). This changes when completion processing happens, rather than removing its work.

The [first ring control](results/native-short-read/deferred/matrix.json) retains the same short-read prototype and changes only the requested setup flags to `SINGLE_ISSUER | DEFER_TASKRUN | TASKRUN_FLAG`. Five repetitions of four 64-byte cases give:

| Workers | Connections | Cooperative | Deferred | Tokio | Deferred change | Deferred vs Tokio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 272,749 | 268,312 | 273,948 | -1.63% | -2.06% |
| 1 | 32 | 667,158 | 708,832 | 712,281 | +6.25% | -0.48% |
| 4 | 1 | 269,945 | 250,422 | 273,019 | -7.23% | -8.28% |
| 4 | 32 | 1,761,579 | 1,762,592 | 1,758,979 | +0.06% | +0.21% |

The [source record](results/native-short-read/deferred/source.txt) preserves that control. This supports ring task-work policy as a substantial part of the residual busy-worker cost, but does not establish a universal default or isolate `SINGLE_ISSUER` from deferral.

The resolved io-uring 0.7.15 provider's `submit()` already sets `GETEVENTS` when `SQ_TASKRUN` is set. There is no missing unsafe-enter workaround here. A separate [publication control](results/native-short-read/deferred-publish/matrix.json) adds CQ collection immediately after successful nonblocking submission, without changing the worker loop. It changes medians by +0.28%, +0.30%, -0.42%, and -0.32% for the same four cases. It does not resolve the low-load regression. The [source record](results/native-short-read/deferred-publish/source.txt) preserves the exact change.

## Controls retaining the production receive policy

The [160-sample safe-path matrix](results/native-short-read/safe-deferred/matrix.json) compares current Native, Native with post-submit CQ collection, Native with that same collection plus deferred/single-issuer setup, and raw Tokio. All Native variants retain the production receive policy. Five repetitions cover both payload sizes. The [deferred source record](results/native-short-read/safe-deferred/source.txt) and [cooperative publication source record](results/native-short-read/safe-publish/source.txt) preserve the changes. Both pass the urgent-data fixture ([deferred](results/native-short-read/safe-deferred/urgent.log), [cooperative publication](results/native-short-read/safe-publish/urgent.log)).

| Workers | Connections | Bytes | Current Native | Cooperative + collection | Deferred + collection | Tokio | Deferred vs current | Deferred vs Tokio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 64 | 274,241 | 272,700 | 265,220 | 273,356 | -3.29% | -2.98% |
| 1 | 32 | 64 | 626,121 | 632,249 | 670,403 | 712,618 | +7.07% | -5.92% |
| 4 | 1 | 64 | 261,108 | 246,926 | 236,151 | 275,881 | -9.56% | -14.40% |
| 4 | 32 | 64 | 1,758,922 | 1,747,452 | 1,763,885 | 1,760,239 | +0.28% | +0.21% |
| 1 | 1 | 4096 | 240,498 | 240,035 | 246,049 | 241,137 | +2.31% | +2.04% |
| 1 | 32 | 4096 | 576,428 | 573,701 | 607,273 | 636,223 | +5.35% | -4.55% |
| 4 | 1 | 4096 | 239,243 | 240,486 | 248,283 | 240,051 | +3.78% | +3.43% |
| 4 | 32 | 4096 | 1,599,109 | 1,589,241 | 1,612,204 | 1,619,920 | +0.82% | -0.48% |

The combined safe variant improves busy single-worker throughput by 5.4–7.1%, while retaining a 4.6–5.9% gap to Tokio. It regresses 64-byte single-connection throughput by 3.3% with one worker and 9.6% with four. Post-submit collection alone also regresses the latter case by 5.4%, so collection timing interacts with the receive path. The 4 KiB single-connection cases improve instead. These results reject both changes as unconditional defaults; the residual low-load effect cannot be reduced to a constant per-entry cost.

## Kernel batching evidence

The [follow-up counters](results/native-short-read/safe-deferred/profiles/stats.json) separate hardware-only CPU measurements from work/syscall tracepoints. Each mode uses three randomized repetitions of one worker/32 connections/64 bytes, with no warmup so request totals and counters cover the same interval. The [runner](results/native-short-read/ring-profile.py) retains both modes and separate sampled profiles. Startup/EOF contribute a small fixed count; traced throughput is not a capacity result.

| Median cost per echo | Current Native | Safe deferred + collection | Raw Tokio |
| --- | ---: | ---: | ---: |
| Userspace cycles | 872 | 915 | 729 |
| Kernel cycles | 7,592 | 7,022 | 6,893 |
| Receive syscalls | 2.00005 | 2.00005 | 1.00003 |
| Send syscalls | 1.00002 | 1.00002 | 1.00002 |
| io\_uring enters | 0.02261 | 0.05114 | 0 |
| epoll waits | 0.00002 | 0.00001 | 0.04311 |
| Cooperative task-work runs | 0.94449 | 0 | 0 |
| Deferred local-work runs | 0 | 0.05114 | 0 |
| io\_uring completions | 1.00009 | 1.00007 | 0 |

The current ring processes almost one task-work batch per echo. Deferred processing runs about 18.5 times fewer batches while delivering the same completion count. Its hardware-only kernel cost falls about 570 cycles/echo (7.5%), despite more explicit ring entries; userspace cost increases about 43 cycles/echo. This supports fragmented kernel completion processing as a substantial source of busy-path cost. Fewer explicit event-wait syscalls alone do not establish a cheaper reactor.

The [baseline setup trace](results/native-short-read/safe-deferred/profiles/baseline.setup.txt) records the worker's accepted `0x300` flags (`COOP_TASKRUN | TASKRUN_FLAG`). The [deferred setup trace](results/native-short-read/safe-deferred/profiles/deferred.setup.txt) records `0x3200` (`SINGLE_ISSUER | DEFER_TASKRUN | TASKRUN_FLAG`). These representative one-worker runs verify successful optional replacement rather than inferring it solely from requested source flags. Both first create an unbound default ring with flags zero. A separate [four-worker setup runner](results/native-short-read/ring-setup.py) verifies accepted flags for every worker: [cooperative](results/native-short-read/setups-four-workers/baseline.txt), [deferred](results/native-short-read/setups-four-workers/deferred.txt), and [single-issuer cooperative](results/native-short-read/setups-four-workers/single.txt). The last control accepts `0x1300`, so its small measured effect is not an unsupported-flag fallback.

A [separate single-issuer control](results/native-short-read/single-issuer/matrix.json) adds only `SINGLE_ISSUER` to the production cooperative ring. Its five-repeat busy single-worker median changes by +0.51%, while the four-worker/one-connection median changes by -3.45%. This establishes only a small single-issuer effect in its tested production-policy configuration. The earlier deferred control uses the short-read prototype, while the safe deferred variant also adds CQ publication; these controls do not exclude interactions between setup flags, receive policy, and publication timing. The [source record](results/native-short-read/single-issuer/source.txt) keeps that smaller intervention separate from CQ publication.

## Low-load and registration controls

A [no-stealing control](results/native-short-read/no-steal-deferred/matrix.json) keeps task placement on each last worker and compares cooperative versus deferred rings without post-submit CQ collection. At four workers/one 64-byte connection, cooperative Native reaches 271,607 echoes/s, deferred Native 254,670, and Tokio 272,724. Deferral remains 6.24% slower than cooperative processing even when task migration is impossible. The corresponding 4 KiB case improves from 240,203 to 245,533; four workers/32 connections improves from 1,737,933 to 1,766,141. The [cooperative](results/native-short-read/no-steal-cooperative/source.txt) and [deferred](results/native-short-read/no-steal-deferred/source.txt) source records preserve this deliberately non-production scheduling diagnostic.

The [four-worker/one-connection counters](results/native-short-read/safe-deferred/profiles-low-load/stats.json), collected separately with normal work stealing, find about one task-work run and one ring entry per echo for both policies. This workload offers almost no kernel batching opportunity. Submission counts are only about 0.00005–0.00006 per echo, so these traced runs do not exhibit sustained monitor cancellation/rebinding churn. Tracing changes timing; these counts do not establish the migration rate of the uninstrumented capacity runs. Deferred kernel cycles are lower here too, but lower active CPU cost does not establish shorter round-trip latency when the worker parks between requests.

A [lazy-registration screen](results/native-short-read/lazy-registration/matrix.json) checks another kernel-cost hypothesis: io\_uring sockets also retain dormant async-io epoll registrations. It patches an isolated async-io 2.6.0 copy to register only on first readiness interest, with matching modify/add and delete behavior. It changes no ring flags, receive policy, or worker code. The [source record](results/native-short-read/lazy-registration/source.txt) includes the dependency patch and lockfile change. This is a diagnostic vendor patch, not a production change or a generally validated async-io implementation.

Across three repetitions, lazy registration changes the 64-byte medians by -0.89% (one worker/one connection), +0.14% (one/32), +6.11% (four/one), and +1.26% (four/32). It does not reproduce the busy single-worker gain and is not the main explanation for that gap. The four-worker/one-connection baseline ranges from 243,945 to 267,584, so that latency-oriented screen needs a larger matched follow-up before selecting a registration redesign.

A [forced-poll screen](results/native-short-read/deferred-poll/matrix.json) adds a nonblocking `GETEVENTS` entry when a zero-timeout turn has published no completion and the SQ is empty. It does not override readiness submission staging or make a second entry after another submit. It tests whether avoiding an extra idle pass helps deferred completion timing. Across three repetitions, its four 64-byte median changes are -2.47%, -0.33%, -1.51%, +0.36%, respectively. This rejects the extra unconditional empty-SQ poll as a general remedy. The [source record](results/native-short-read/deferred-poll/source.txt) preserves the diagnostic, including its explicit zero-minimum enter.

## io-event boundary-flush controls

io-event's [ring setup](https://github.com/socketry/io-event/blob/c8550a85e340941491ee4c09b73d54c5310fed1e/ext/io/event/selector/uring.c#L327) requests `SINGLE_ISSUER | DEFER_TASKRUN | TASKRUN_FLAG`, alongside `SUBMIT_ALL`. Its [selector boundary](https://github.com/socketry/io-event/blob/c8550a85e340941491ee4c09b73d54c5310fed1e/ext/io/event/selector/uring.c#L1830) flushes pending submissions, conditionally calls `io_uring_get_events` when `SQ_TASKRUN` is set, runs ready fibers, and processes completions before deciding whether to block. Native's existing provider already supplies `GETEVENTS` when `submit()` observes that flag. These controls test whether changing its placement resolves the deferred ring's low-load penalty.

Three separate randomized screens retain the production receive policy and use conditional kernel entries rather than unconditional empty-ring polling:

- [Early flush](results/native-short-read/io-event/early.json): enable deferred/single-issuer setup and service `SQ_TASKRUN` before the initial CQ snapshot, while preserving Native's readiness-submission staging. Its [source record](results/native-short-read/io-event/early-source.txt) retains the exact intervention. The fourth variant, `deferred-iouring-monitor`, is the earlier safe deferred/post-submit-collection executable.
- [No readiness staging](results/native-short-read/io-event/unstaged.json): retain early flushing and submit newly prepared readiness entries on their first turn. The [source record](results/native-short-read/io-event/unstaged-source.txt) captures that additional change. Here the fourth variant is the preceding early-flush executable.
- [Submission first](results/native-short-read/io-event/submission-first.json): attempt submission of queued SQEs, conditionally service any remaining `SQ_TASKRUN`, and collect completions before the idle decision. The [source record](results/native-short-read/io-event/submission-first-source.txt) preserves this closer ordering control. It retains Native's initial CQ retirement pass and one bounded submission attempt; io-event instead loops over submissions. It can also split submission and a subsequent blocking wait into separate kernel entries. Here the fourth variant is the no-staging executable.

Each screen has 48 samples: three repetitions of four variants, one/four workers, one/32 connections, and 64-byte payloads only. These are separate matrices, so compare each prototype with its own interleaved baseline. All use the same frozen validating client, release optimizations with debug symbols, CPU placement, one-second warmup, and three-second measurements. Builds, tests, traces, and timings hold the existing exclusive performance lock. The [early](results/native-short-read/io-event/early-build.log), [unstaged](results/native-short-read/io-event/unstaged-build.log), and [submission-first](results/native-short-read/io-event/submission-first-build.log) build logs record optimized profiles.

Submission-first medians, in echoes/s:

| Workers | Connections | Current Native | Submission first | Tokio | Change vs current | Change vs Tokio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 270,910 | 264,232 | 275,447 | -2.47% | -4.07% |
| 1 | 32 | 635,066 | 675,560 | 707,495 | +6.38% | -4.51% |
| 4 | 1 | 262,487 | 232,769 | 273,819 | -11.32% | -14.99% |
| 4 | 32 | 1,764,860 | 1,757,681 | 1,739,872 | -0.41% | +1.02% |

Early flushing changes the corresponding four cases by -0.02%, +6.42%, -12.64%, and -0.12%. Removing readiness staging changes them by -5.17%, +7.51%, -10.61%, and +1.53%. All three controls retain the busy single-worker gain and the substantial four-worker/one-connection regression. Moving the conditional flush or removing the extra staging turn does not resolve that tradeoff. These small screens reject the tested combinations as general defaults; they do not isolate the cost of every individual change or establish the best policy for other workloads.

Separate setup traces verify that all four bound workers actually accept `0x3200` for [early flush](results/native-short-read/io-event/early-setup.txt) and [submission first](results/native-short-read/io-event/submission-first-setup.txt), against the [baseline's `0x300`](results/native-short-read/io-event/baseline-setup.txt). They use the existing [setup runner](results/native-short-read/ring-setup.py), with `single` naming the early-flush executable and `deferred` naming submission first.

The early prototype's [regression patch](results/native-short-read/io-event/early-tests.patch) checks publication of deferred notification work within one nonblocking turn and transient/fatal flush errors. Its [successful crate test log](results/native-short-read/io-event/early-tests.log) covers 234 unit tests, 290 integration tests including the urgent-data fixture, and one doctest. An [initial parallel attempt](results/native-short-read/io-event/early-tests-parallel.log) failed one ring allocation with `ENOMEM`; the complete rerun used four test threads. Submission-first independently passes the same 290 [public integration tests](results/native-short-read/io-event/submission-first-integration-tests.log), including urgent-data handling, and [library Clippy](results/native-short-read/io-event/submission-first-clippy.log). The early prototype's internal error-injection tests are specific to its ordering and are not claimed as submission-first validation. These remain archived experiments; production runtime code is unchanged.

## Local-wake policy screen

A [60-sample screen](results/native-short-read/local-wakes/matrix.json) changes only native wake escalation. Local notifications outside task polling wait for the ready-batch boundary before waking idle helpers; application-poll and remote wakes retain immediate escalation. Task stealing remains enabled. The [source record](results/native-short-read/local-wakes/source.txt) preserves the runtime intervention and its polling-phase flag.

| Workers | Connections | Borrowed current | Borrowed candidate | Change | Owned current | Owned candidate | Change |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 271,492 | 275,787 | +1.58% | 230,818 | 237,022 | +2.69% |
| 1 | 32 | 637,185 | 638,246 | +0.17% | 650,598 | 654,407 | +0.59% |
| 4 | 1 | 262,517 | 260,394 | -0.81% | 233,011 | 230,731 | -0.98% |
| 4 | 32 | 1,777,493 | 1,762,114 | -0.87% | 1,739,694 | 1,732,939 | -0.39% |

The 64-byte/three-repeat screen gives at most 2.7% improvement for one worker/one owned connection, essentially unchanged busy single-worker results, and small losses with four workers. It does not justify adding polling-phase state or changing wake semantics. The production worker behavior remains the reference for this investigation; this prototype is archived, not selected. Suppression would also cover callbacks executed during driver/timer work, so selecting such a policy would require considering callback progress as well as ordinary task polls.

## Method

The initial comparison runs the current Native baseline, the short-read prototype, and ordinary Tokio on Aiko using the same frozen validating client. Five randomized repetitions cover one/four workers, one/32 connections, and 64-byte/4 KiB payloads: 120 samples in total. Each sample has one second warmup and three measured seconds. Servers use CPU 0 or CPUs 0–3; the four-worker client uses separate physical CPUs 4–7 in the same L3 group. Builds, timings, correctness checks, and counters hold `/tmp/core-scheduler-perf.lock` exclusively.

Both Native variants use release optimizations with debug symbols, without LTO or target-CPU overrides. Runtime source is `22b246853c609264f9112554075bf49eb2cc9af2`, identical to the runtime in PR #38's `faccc3b3bf2d781d212d2c3c7cf91847c6a4de5d`; subsequent changes in that PR only affect benchmark test determinism and reports. The baseline is the frozen `final-postbatch` executable from the [preceding comparison](tokio-parity.md). The comparison runner is [the same frozen-binary runner](results/tokio-parity/compare.py).

This is persistent loopback TCP echo with TCP\_NODELAY, one outstanding request per connection, a retained 64 KiB receive buffer, complete partial writes, and validated payloads. It measures these concrete closed-loop paths, rather than general server capacity or physical-network performance. Later matrices retain the same frozen-client setup and identify their own variants and three- or five-repeat counts. Hardware-counter and trace runs use separate zero-warmup settings described above. These short repetitions do not establish a tight confidence interval.

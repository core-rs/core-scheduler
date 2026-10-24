# Reusing owned socket receive requests

The bound io_uring stream reader now reuses its scalar receive request and completion storage. Repeated `read_owned` calls with the same buffer-owner type allocate nothing after warmup when backend capacity exists and no transient readiness retry is required. The buffer remains owned by the request through original completion. No public interface changed.

The cache follows the existing file-request ownership pattern: completion releases the socket and admission permit before publishing the returned request, collection removes its buffer and caches the empty box, and abandonment discards a retired owner. A different owner type replaces the cached box. Unknown kernel retirement still retains the request and causes its waiting future to panic. Borrowed reads initialize no cache.

`screen.json` records three randomized repetitions per variant and case on Aiko, with one second warmup and three seconds measurement. All servers use the ordinary optimized release profile with debug information, without LTO or target CPU overrides. Server workers use CPUs 0–3; the fixed four-worker validating client uses CPUs 4–7. Builds and timing runs exclusively hold `/tmp/core-scheduler-perf.lock`.

| Server | 1 worker, 1 connection | 1 worker, 32 connections |
| --- | ---: | ---: |
| Native owned, merged baseline | 229,620 | 614,136 |
| Native owned, smaller admission/retirement path | 232,529 | 617,749 |
| Native owned, reusable receive request | 239,715 | 654,384 |
| Pure Tokio | 275,231 | 715,561 |

Values are median validated requests per second with 64-byte messages. The cache improves the baseline by 4.4% with one connection and 6.6% with 32 connections; relative to the smaller admission/retirement changes, it improves throughput by 3.1% and 5.9%. The one-connection samples vary more, so their small gains deserve less weight. The remaining owned-path gap to Tokio is 12.9% and 8.6%, respectively. These measurements do not substitute borrowed I/O for completion-based receives.

The baseline is `1840258bbf9cf10c08fefa26fbff54a747f18d08`. `source.sha256` identifies every changed production file used by the cached binary; the JSON records frozen binary and client hashes. Source is synchronized before each build and explicitly touched inside the build lock: preserving an older local modification time can otherwise make Cargo incorrectly reuse a prior remote artifact.

Two smaller experiments informed this change. `prepared-sqe.json` moved scalar SQE preparation to the submitting task and allowed callback-free submit/wait batching, but gave no gain: −0.1% with one connection and −1.5% with 32. It was rejected. `retirement.json` replaces the lane's general event listener with one cleanup-guarded atomic waker and avoids unboxing a scalar request at completion; it was approximately flat with 32 connections and noisy with one. The cache comparison measures those smaller changes separately rather than attributing their effect to reuse.

The allocation regression runs real bound-reader receives with a reused vector and entered worker, requiring zero allocations after two warmup iterations. Safety tests cover cancellation, capacity waiting, retryable original completions, queued rejection, driver failure with unknown retirement, buffer-type changes, abandoned operations, forgetting a future and dropping its reader, and cleanup of dropped close waiters. The final combined executor comparison should also verify borrowed controls and several workers.

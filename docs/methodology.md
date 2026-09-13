# Methodology

## Evidence classification

The layered report classifies evidence before comparing it:

| Layer | Completion question | Allowed comparison |
|---|---|---|
| Consensus in memory | Did the same leader-side application operation complete? | Competitors only after completion boundaries align |
| Durable replication | Did the same Rafter durability batch complete? | Rafter storage backends; competitor claims require an equivalent contract |
| Complete durable service | Did the client receive a result after Raft commitment and durable application? | Same workload, host, timing mode, and semantic contract |
| Failure and sustained operation | What remained responsive and recovered during a controlled fault or soak? | Correctness checks stay separate from performance results |

Normalized records retain the layer, engine/version, named configuration,
workload, completion boundary, environment, repetitions, metric definition, and
source cases. Comparisons fail closed when payload, concurrency, rate, fault
scenario, completion contract, environment, or timing/diagnostic mode differs.
Incomplete evidence suppresses the headline rather than dropping failed cases.
Differences within an a priori ±3% band are described as roughly level.

Report generation is deterministic. An LLM may later rewrite already-verified
prose for another audience, but it must never select cases, calculate metrics,
or produce the authoritative JSON.

## In-memory suite

Three voters in one process, in-memory stores, 512-byte values for serial and
pipelined workloads, and a separate large-payload probe. Rafter and raft-rs use
an explicit message pump; OpenRaft runs on a single-thread Tokio runtime.
Each replica executes the same tiny reference application operation for every
normal payload. Completion is recorded after that operation executes on the
leader: Rafter consumes `Apply`, raft-rs consumes the committed ready entry,
and OpenRaft returns from `client_write()`. The operation only accounts for
applied payload bytes, keeping protocol/runtime and scheduler overhead visible.
The suite requires seven isolated repetitions before publishing a leaderboard.

Multi-write workloads are submission bursts, not continuously replenished
windows. The report labels them accordingly; a rolling-window workload remains
separate future evidence.

The sources were imported from Rafter; [UPSTREAM.json](../microbench/UPSTREAM.json)
records their origin. They are maintained in this repo so changing the Rafter
library revision does not silently change the workload. Rafter-only mode also
runs read, codec, service, and MultiRaft probes. These do not have competitor
comparisons. Raw process reports are retained; tables use medians of per-run
results, including median per-run p99 rather than a pooled percentile.

## Durable-service suite

Contract: `durable-log+durable-application-v1/logged-reads`.
Three processes use real TCP and separate data directories on one host.
Success waits for Raft commitment and a synced application journal. Put, Get,
and compare-and-swap all use the log. Retried commands preserve session and
sequence identity; duplicate commands return the recorded result, stale
sequences and changed contents under the same identity are rejected.

Rafter uses native file stores. raft-rs and OpenRaft use benchmark journals.
Storage, batching, codecs, and scheduling differ between integrations. The
shared application journal records apply index and retry identity, synchronizes
new files/directories, truncates incomplete trailing records on replay, and
rejects complete records with invalid checksums.

Rafter's ordered mode executes that journal through the exact pinned
`rafter_runtime::application::ApplicationWorker`; the harness supplies only
its store adapter, diagnostics, query fencing, and client-result mapping. The
build receipt names this public worker, and ordered evidence is refused when
the receipt does not. Retained credits and ready-only batch limits therefore
exercise the same public mechanism as Rafter's independent package consumer,
not a benchmark-only durability thread.

The OpenRaft baseline synchronizes each log append before invoking OpenRaft's
flush callback. The separately named async control stages appended entries so
they are immediately readable, publishes them through one bounded ordered
journal worker, and invokes the callback only after synchronization. Vote,
committed-index, and truncation writes use the same ordered worker and wait for
a durability fence. Both controls keep identical application durability and
client-completion contracts; reports never combine their aggregates or choose
between them implicitly.

Rafter's optional durable-completion-priority experiment does not relax its
one-operation persistence ownership. It may reorder only same-term leader
`AppendEntriesResponse` inputs ahead of newly collected proposal batches that
exceed the configured speculative limit and at most one client batch of ready,
unsubmitted execute requests, then uses the ordinary synchronous ACK fence and
proposal pipeline. Speculative-sized batches retain their original order. The
no-wait scan stops at reads, status requests, wake boundaries, or unsafe Raft
input and restores skipped execute requests in their original order. It may
also release an older application completion that is already durable before
waiting for a newer Raft persistence operation. A client response still
requires that client's own durable application completion. Deterministic actor
tests deliberately exercise
the bounded lookahead, its scan limit, and unsafe boundaries during the locked
Rust build. Per-case receipts separately record which paths happened under the
measured workload. Missing workload observation is reported as incomplete
feature coverage; it does not invalidate otherwise intact evidence or finite
correctness checks. Legacy schema-1 receipts remain replayable without claiming
lookahead coverage.

| Metric | Meaning |
|---|---|
| Successes/s | Successful logical operations completed inside the measurement window |
| Arrival latency | Scheduled arrival to completion, including client waiting |
| Execution latency | Client request start to completion, including network and retries |
| Client-start delay | Scheduled arrival to client request start |
| Unsent | Scheduled work that could not enter the generator's bounded queue |
| Unknown | Dispatched work whose result could not be confirmed before its deadline |

Latency distributions are measured separately; subtracting percentiles does not
recover a server-side percentile. Unsent work was never accepted by a server.
A late completion is retained but does not inflate in-window throughput.

Fixed offered-load curves also evaluate a declared useful-capacity objective.
Every repetition must achieve at least 99% of the offered rate, keep arrival
p99 at or below 20 ms and p99.9 at or below 50 ms, and record zero errors,
unknown outcomes, and unsent requests. The decision uses the worst repetition,
not only its median. Client-start waiting and completions after the measurement
window remain visible as queue-pressure evidence. A highest qualifying point at
the top of the tested curve is reported as a lower bound, never as maximum
capacity.

For current measurements, scheduled dispatch and worker-start waiting retain
their raw fixed-memory histograms. Their counts, bucket-constrained maxima, and
summary percentiles are independently replayed. Dispatch p99 and p99.9 are
displayed separately from start-wait and execution latency: unsent work shows
admission loss, while scheduler lateness shows whether the generator itself
emitted arrivals late. Older evidence that predates these histograms reports
them as not measured; mixed availability within one aggregate fails closed.
Neither is silently reclassified as successful demand.

Durable suites publish four independent verdicts. Evidence integrity covers
expected cases, seals, recorded identities, receipts, and accounting.
Correctness covers the finite history, restart, and smoke checks. Feature
coverage states whether selected optimization paths occurred in the measured
workload. The service objective evaluates achieved rate, latency, and complete
loss accounting. A report retains measurements when feature coverage is
incomplete or the service objective fails; it never converts either condition
into a passing performance claim. Historical suite-level activity failures stay
visible under their original message while being classified as feature coverage.

Every case checks a complete 64-operation Put/Get/CAS history and acknowledged
canaries across simultaneous process restarts, before and after load. These
checks are finite; they do not prove every measured write survived a power cut.
Leader-loss kills and restarts the leader. Follower-catchup pauses and resumes a
follower with its retained log. The optional `snapshot-catchup` smoke kills one
follower before measured traffic, requires the leader to publish a newer Raft
snapshot while serving writes, restarts the same follower data directory, and
requires an application-snapshot install at that exact or a later boundary.
Its receipt replays the kill/restart actions, local compaction counters, snapshot
boundary, application boundary, and restart canaries. This is functional smoke
evidence, not fault-performance or power-loss evidence. Default timing cases do
not enable snapshots. The application journal remains append-only even when the
Raft WAL is reclaimed. Dynamic membership and TLS are not exercised.

## Reproduction

Both suites record the resolved Rafter commit, lockfile, binary hashes, workload,
compiler, host, and raw results. Repetitions use a deterministic cyclic engine
rotation. The fixed-machine capacity and speculative-threshold commands set the
repetition count equal to their arm count, so every arm occupies every execution
position once at each rate. Current paired suites expand that declaration into
an exact pre-timing execution plan containing every case name, arm position,
seed, rate, mode, and workload option. Report generation independently derives
the plan and compares every retained case manifest with it. Generic paired runs
record their repetition count and exact ordering but need not form a complete
position-balanced block. Failed cases remain visible and fail the job. The
durable report excludes failed cases from aggregates; always inspect errors,
unknowns, and unsent work alongside speed. Checksums detect artifact changes;
they are not signatures.

Fixed-machine commands additionally require the exact benchmark repository
commit. Before creating an output directory, the runner verifies that `HEAD`
matches it and refuses tracked or untracked changes. The suite manifest retains
that clean repository identity; the selected Rafter revision and source digest
remain separate because dependency selection is part of the run setup. Both
selected builds are archived with their own source digests before the tracked
dependency selection is restored. The working build receipt is then invalidated;
cases use the archived receipt and binary set, and the next suite starts from the
same clean benchmark commit. Same-exact-SHA arms with identical compile-time
features are built and tested once; both independently archived arm directories
then contain the identical binary hashes and receipt. Runtime-only scheduling
options remain bound by the execution plan and individual case manifests.

Durable cases also sample Linux PSI, CPU and cgroup throttling counters, load,
per-process I/O, filesystem capacity, and raw `/proc/diskstats` block-device
latency/utilization counters during the measured load. These cumulative counters
help distinguish storage saturation from other host pressure; they do not by
themselves identify the workload responsible for a stall. The case manifest
records the filesystem source so block-device deltas can be interpreted against
the selected data path. Current cases derive a sealed summary from the raw
samples and require at least 95% duration coverage with bounded sampling gaps.
The verifier independently replays that receipt. The layered report surfaces
the maximum observed pressure and throttling signals, but they remain diagnostic:
workload and unrelated activity are inseparable, and no result is removed,
accepted, or ranked from those counters.

Before a fixed machine becomes an evidence source, `machine-profile` records a
predeclared idle observation on the selected data volume. It requires at least
20 GiB available; CPU busy time no greater than 5%; CPU iowait no greater than
2%; steal and cgroup throttling no greater than 1%; CPU and I/O PSI `some` no
greater than 2%; I/O PSI `full` and memory PSI `some` no greater than 0.5%; and
memory PSI `full` no greater than 0.1%. An exposed hardware-throttle counter
must not advance. It also verifies 16 complete file write/sync/rename/directory-
sync/delete sequences used by the durable adapters and records their latency
distribution without applying a storage-speed threshold. CPU frequency policy,
clocksource, NUMA shape, filesystem identity, and block topology are retained
as interpretation context. Missing required counters produce `not measured`,
never a silent pass; hardware counters remain explicitly optional because many
Linux hosts do not expose them. Passing this idle check does not qualify
pressure during the benchmark; the per-case samples remain the authority for
the measured interval.

The `capacity` command captures this profile after both exact same-SHA Rafter
arms and the controls are built, immediately before timing. It binds the sealed
profile digest and selected data root into the suite manifest and refuses to
run any timed case unless the profile passes. There is no record-only override
on this path. Builds therefore cannot be mistaken for benchmark-time pressure,
and an older profile cannot be substituted for the one associated with the
suite. Replayed per-case measured-load context and loss accounting remain
authoritative during the curve.

The `reclamation-load` command applies the same fixed-machine gate to an internal
same-SHA Rafter comparison. All arms use WAL, pipeline mode, peer cap 32,
speculative threshold 1, inflight append bound 8, and completion-priority
scheduling. Only snapshot/checkpoint interval changes: the control disables
snapshots and the candidates use 10,000 and 100,000 applied entries. Three
repetitions form one complete position-balanced block at 3,000 writes/s and
saturation. Those intervals govern maintenance frequency; they are not claims
about a 10,000- or 100,000-entry retained live suffix.

The objective is declared in code before results exist. Fixed-load cases must
achieve at least 99% of offered work with zero errors, unknown outcomes, or
unsent requests. Each snapshot candidate must execute measured-load compaction
in every repetition, retain at least 95% of its same-seed control's saturated
throughput, remain within the larger of a 10% or 1 ms p99 regression and the
larger of a 10% or 2 ms p99.9 regression at equal offered load, and end with
fewer allocated managed Raft bytes than the no-snapshot control. Managed Raft
bytes cover WAL and snapshot data and metadata. After final restart, a candidate
must still use fewer allocated managed Raft bytes than its same-seed no-snapshot
control and retain exactly one selected snapshot envelope and manifest on each node.
A candidate must also retain no temporary snapshot artifacts after that final
owner-thread barrier. A failed row stays in the report and fails the suite;
queue growth is not hidden by weakening the objective.

Rafter WAL cases retain controller filesystem inventories at three boundaries:
before measurement, after measurement, and after the final process restart.
Each filesystem inventory follows a complete-status owner-thread barrier.
Logical size and allocated blocks are accounted by node and separated into Raft
WAL data, Raft-WAL publication metadata, Raft snapshot data, Raft snapshot
metadata, recognized temporary snapshot artifacts, application journal, and
other files. Temporary bytes remain in the managed Raft total but are not
counted as retained snapshot generations. Derived totals and deltas are replayed
from the sealed receipt. The paired runner then deletes the live stores, so
these receipts are controller observations rather than independently retained
database files. Measurement-window compaction counts and duration histograms
are subtracted from pre/post node-status watermarks; reported maxima are log2
upper bounds rather than warmup-contaminated exact timings. Final process
restart timing is monotonic same-host controller time.
Application-journal reclamation remains unmeasured and no bound is inferred
from managed Raft cleanup.

Keep default pins checked in. A requested Rafter ref is resolved once and written
as an exact commit into both manifests, then Cargo updates their locks while
retaining existing compatible dependencies. Builds use `--locked`. The selected
locks are uploaded, including any transitive dependency changes the ref requires.
Re-run the controls on the same hardware when testing a new Rafter revision.
Process samples retain host CPU, cgroup throttling, load average, and Linux
pressure counters when the operating system exposes them. They are diagnostic
context, not an automatic rule for deleting or accepting a run; anomalous and
failed evidence remains visible.

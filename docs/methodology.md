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
`AppendEntriesResponse` inputs ahead of newly collected proposals, then uses the
ordinary synchronous ACK fence and proposal pipeline. It may also release an
older application completion that is already durable before waiting for a
newer Raft persistence operation. A client response still requires that
client's own durable application completion. Per-case receipts record both
paths, and a paired suite fails if the selected experiment never executes.

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

Every case checks a complete 64-operation Put/Get/CAS history and acknowledged
canaries across simultaneous process restarts, before and after load. These
checks are finite; they do not prove every measured write survived a power cut.
Leader-loss kills and restarts the leader. Follower-catchup pauses and resumes a
follower with its retained log. No snapshots, compaction, dynamic membership,
or TLS are exercised. Use trusted networks and finite runs; storage grows.

## Reproduction

Both suites record the resolved Rafter commit, lockfile, binary hashes, workload,
compiler, host, and raw results. Repetitions rotate engine order. Failed cases
remain visible and fail the job. The durable report excludes failed cases from
aggregates; always inspect errors, unknowns, and unsent work alongside speed.
Checksums detect artifact changes; they are not signatures.

Keep default pins checked in. A requested Rafter ref is resolved once and written
as an exact commit into both manifests, then Cargo updates their locks while
retaining existing compatible dependencies. Builds use `--locked`. The selected
locks are uploaded, including any transitive dependency changes the ref requires.
Re-run the controls on the same hardware when testing a new Rafter revision.

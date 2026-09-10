# Methodology

## In-memory suite

Three voters in one process, in-memory stores, 512-byte values for serial and
pipelined workloads, and a separate large-payload probe. Rafter and raft-rs use
an explicit message pump; OpenRaft runs on a single-thread Tokio runtime.
Each binary states its completion boundary in the raw JSON and report. The
suite measures protocol/runtime work without TCP or disk persistence.

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

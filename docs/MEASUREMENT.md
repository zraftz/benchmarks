# Reading the measurements

## Counts before percentiles

A complete report accounts for every offered logical operation:

```text
offered = attempted + not_issued
attempted = ok + unknown + errors
network_attempts >= logical attempts that reached the network
```

`not_issued` is a load-generator admission failure, not a successful request or
a server rejection. `unknown` means the client could not establish a final
outcome. Inspect both before quoting successful-request latency. A fast
successful subset is not evidence that the whole offered workload was served.

The generator keeps successful and all-dispatched histograms separately. It
also records worker and dispatch lateness. Scheduled arrivals are not shifted
forward to hide a slow server or delayed generator. Closed loop is explicitly
labeled and makes no fixed-arrival-rate claim.

The fixed-memory logarithmic histogram has 4,096 bins and reports upper-bound
bin values. At larger durations bin width is approximately 1.563% or less;
very small values are exact. Counts are retained, not sampled or silently
truncated. This is the harness's histogram format, not an HdrHistogram export.
Per-second data records completion counts and maximum successful latency,
not a complete percentile histogram for each second.

## Repetitions

The runner rotates implementations by repetition, with the same configured
seed per repetition. Actual scheduling and per-worker assignment are not fully
deterministic. Warm-up and measurement use separate sessions and share the
configured workload key namespace. A seed does not imply identical wire traces.

Reports aggregate only matched settings and qualifying non-smoke runs. The
aggregate p99 is the median of individual p99 values, **not** a percentile from
merged observations. The current report shows min/median/max throughput; it
does not compute confidence intervals or automatically choose a winning SLO.
Zero offered rate means closed loop.

## Environment and evidence

Manifests record source digests, binary digests, dependency pins, compiler/build
receipts, cgroup limits where available, host/platform details, filesystem
mount details, and block-device information. Linux process samples contain RSS,
CPU ticks, and I/O counters. They are not allocator or per-Raft-group measurements.
Local shared-device contention is part of a local embedding result.

Each case has an immutable checksum inventory. A hash detects changes relative
to that inventory; it does not establish who generated the measurements. Keep
build logs, result archives, machine allocation details, and review notes when
publishing. Do not mix runs from different host configurations simply because
the implementation names match.

## Failure runs

The controller records requested and completed failure actions with timestamps.
The load generator reports its start time and per-second completions. Use these
to inspect service interruption and recovery. Exact first-success recovery,
recovery-to-SLO, and snapshot-transfer telemetry are future report features.
The included follower scenario is a temporary process pause with retained logs.
No claim about disk power-loss survival follows from these process tests.

## Storage growth

No automatic compaction occurs. File and in-memory indexes grow with retained
logs, even if the keyspace is bounded. Scope initial runs to a finite duration,
check free disk/memory first, and inspect before/after counters. Sustained
steady-state claims across many compaction cycles require a future snapshot
and compaction-capable adapter lane.

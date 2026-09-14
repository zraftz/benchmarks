# Raft benchmarks

Current qualified evidence: [one-minute summary](reports/qualified-34678152454-wal-34741175806/summary.md) ·
[static HTML](reports/qualified-34678152454-wal-34741175806/report.html) ·
[normalized JSON](reports/qualified-34678152454-wal-34741175806/summary.json)

Qualified in-memory evidence: [summary](reports/qualified-in-memory-34654362991/summary.md) ·
[HTML](reports/qualified-in-memory-34654362991/report.html) ·
[JSON](reports/qualified-in-memory-34654362991/summary.json)

Compare Rafter, raft-rs, and OpenRaft with two suites:

| Suite | Measures | Output |
|---|---|---|
| **In memory** | Serial, pipelined, and large-payload replication in one process | HTML comparison, JSON, raw repetitions |
| **Durable service** | Three processes over TCP, synced storage, client load, and restart checks | HTML comparison, measurements, logs, checksums |

## Run on GitHub

Open **Actions → Baseline benchmark → Run workflow**. Choose the suite and enter
any Rafter branch, tag, or full commit SHA. Leave it blank to use the checked-in
pin. Both suites resolve the same SHA and retain their exact dependency locks.

```sh
gh workflow run baseline.yml --repo zraftz/benchmarks --ref perf/raft-performance \
  -f suite=both -f rafter_ref=perf/raft-performance
```

Download the `benchmarks-<run-id>` artifact. Open `results/microbench/report.html`
and/or `results/baseline/report.html`. Raw measurements and source/build identity
are alongside each report. Shared GitHub runners provide an initial reference;
rerun all engines together when comparing changes.

The baseline workflow's **reclamation** suite runs the predeclared same-code
service comparison on the hosted runner. It binds the suite to the workflow SHA
and one exact Rafter SHA, and retains the report even when an objective fails:

```sh
gh workflow run baseline.yml --repo zraftz/benchmarks \
  --ref perf/raft-performance -f suite=reclamation \
  -f rafter_ref=<exact-40-character-sha>
```

This hosted path deliberately does not claim the fixed-machine qualification.
Its per-case pressure receipts remain visible, and performance is observational
unless the recorded host evidence is suitable. Use `./raft-bench
reclamation-load` on the fixed machine for the qualified result.

Selected runs also produce `results/summary/`:

| File | Use |
|---|---|
| `report.html` | One-page implementation-performance report |
| `summary.md` | GitHub summary, PR, or README excerpt |
| `summary.json` | Normalized metrics and exact source-case references |

The page asks four questions: consensus in memory, durable replication,
complete durable service, and failure/sustained operation. Missing or
incompatible evidence stays visibly unmeasured. The generator is deterministic
and does not use an LLM or require an API key.

The durable baseline runs 27 cases: three engines × three load levels × three
repetitions, with 60 seconds measured per case. Allow about 40–60 minutes.
`rafter-only` runs the extra in-memory read, codec, service, and MultiRaft probes.

## Run locally

Needs Rust 1.88.0, Python 3.11+, Go 1.22+, Git, `protoc`, and a C compiler.
Linux is recommended. Data storage must support file and directory syncs.
Verify the exact compiler and tool prerequisites before investing in a long run:

```sh
./raft-bench doctor
```

The doctor makes no installations and exits unsuccessfully when the pinned
Rust/Cargo version, minimum Python/Go versions, or required unversioned tools
are unavailable.

Before using a new fixed machine for evidence, characterize the exact data
volume while the host is idle:

```sh
./raft-bench machine-profile \
  --data-root /path/to/benchmark-disk \
  --output results/machine-profile
```

The predeclared 15-second objective requires at least 20 GiB available and
bounds idle CPU use, iowait, steal, cgroup throttling, CPU/I/O/memory pressure,
and any exposed hardware-throttle counter. The receipt records CPU frequency
policy, clock, NUMA, filesystem, and block topology, then measures 16 complete
write/sync/rename/directory-sync/delete cycles on the selected volume. Storage
latencies are diagnostic, not a gate. Missing required Linux counters do not
pass; unavailable hardware-throttle counters remain explicit but optional.
`--record-only` retains an incomplete or stressed-host observation without
relabeling it as qualified.

For the predeclared useful-capacity curve, use the exact 40-character Rafter
commit. This command builds both same-code Rafter controls first, then captures
and seals a fresh idle/storage profile on the selected volume. Timing does not
start unless that profile passes. The suite binds the profile seal and runs
FIFO Rafter, completion-priority Rafter, and both OpenRaft storage controls at
zero added loopback delay:

```sh
./raft-bench capacity \
  --rafter-sha <exact-40-character-sha> \
  --benchmark-sha <exact-benchmark-commit> \
  --data-root /path/to/benchmark-disk \
  --output results/capacity-<sha>
```

The default rates are `1000,2000,3000,4000,6000,8000,10000,12000`; only
1,000/s gets the separate diagnostic pass. Four timing repetitions use a full
cyclic arm rotation, so each of the four implementations occupies every
execution position once at each rate. The suite records the exact case sequence,
arm position, seed, rate, mode, and workload options before timing; report
generation replays every case against that plan. The default is 132 cases and
154 declared warmup/measurement minutes; builds, finite qualification histories,
recovery checks, and process overhead are outside that estimate. A failed
machine profile is retained with the aborted suite and cannot be overridden by
this command. Use a new output directory after correcting the host condition.

```sh
./raft-bench microbench --runs 7

./raft-bench build --rafter-ref perf/raft-performance
./raft-bench run --rates 0,100,1000 --runs 3 --data-root /path/to/benchmark-disk
```

`--rafter-ref` is also accepted by `microbench`. Selection updates both local
manifests and lockfiles; omitted refs use the current selection. Use
`./raft-bench select-rafter <SHA>` to select once before running both suites.
Rafter source is fetched as a dependency; its checkout is never edited.
Paired suites archive both builds, run their functional smokes, then restore the
tracked dependency selection and invalidate the working build receipt. This
leaves the exact benchmark checkout clean for the next fixed-machine suite.
When both arms select the same exact revision and compile-time features, the
runner tests and builds it once, then archives that identical verified binary
and receipt for both runtime configurations.
Pipeline experiments may vary the bounded per-follower replication window with
`--max-inflight-appends`; the default remains Rafter's eight append batches.
The WAL pipeline can also run the finite live-reclamation smoke directly:

```sh
./raft-bench build --rafter-hard-state wal --pipelined-durability
./raft-bench run --smoke --implementations rafter --rates 0 \
  --pipelined-durability --peer-batch-size 32 \
  --snapshot-interval-entries 8 --scenario snapshot-catchup \
  --application-checkpoint-bytes 1
```

That scenario kills a follower before load, requires a newer leader snapshot,
then proves snapshot installation and acknowledged canaries after restart. It
does not qualify reclamation latency. After the matching Raft snapshot and WAL
compaction are durable, snapshot-enabled Rafter checkpoints the benchmark
application journal to that applied boundary when its physical history exceeds
64 MiB per node. Smaller durable journals remain append-only until the bound is
crossed. Default cases that do not enable snapshots remain append-only.

On a fixed machine, qualify the service cost and physical effect of reclamation
without editing the suite after observing results:

```sh
./raft-bench reclamation-load \
  --rafter-sha <exact-40-character-sha> \
  --benchmark-sha <exact-benchmark-commit> \
  --data-root /path/to/benchmark-disk \
  --output results/reclamation-load-<sha>
```

This is a same-binary Rafter comparison: reclamation disabled versus checkpoint
intervals of 10,000 and 100,000 applied entries, at 3,000 writes/s and
saturation. Three repetitions rotate the three arms through every execution
position. The default is 24 cases and 28 declared warmup/measurement minutes,
excluding qualification and process overhead. The command first requires a
fresh fixed-machine profile, then emits `reclamation-under-load.json` and a
concise Markdown report. It fails unless
every snapshot arm compacts during every measured interval, preserves complete
request accounting, stays within the predeclared throughput/tail budgets, and
uses fewer allocated managed Raft bytes than its same-seed no-snapshot control
both after load and after the mandatory restart. Managed Raft bytes include WAL
and snapshot data and metadata. After final restart, the receipt must also show
exactly one selected snapshot envelope and manifest on each node and no
temporary snapshot artifacts. These values are checkpoint intervals, not
retained-suffix sizes.

Every current Rafter WAL case records controller-observed logical and allocated
bytes before measurement, after measurement, and after final restart. Raft WAL,
its manifest, Raft snapshot envelopes, manifests and temporary artifacts, and
the application journal are reported separately. Reclamation runs compare the
checkpointed application journal with the append-only no-snapshot control and
require one authoritative journal with no checkpoint temporary after restart.
Temporary bytes remain part of the managed Raft total but do not count as
retained snapshot generations. The live node files are removed after
verification, so the sealed receipt is a controller `stat` observation rather
than a retained copy of the database.

Results live under `results/`. Verify a case or an in-memory suite with:

```sh
./raft-bench verify <result-directory>
```

Generate or regenerate a layered report from immutable evidence with:

```sh
./raft-bench summary \
  --durable-suite /path/to/results/paired \
  --storage-suite /path/to/hard-state-evidence \
  --wal-reclamation-suite /path/to/wal-reclamation-evidence \
  --history-suite /path/to/prior/results/paired \
  --output /path/to/new-report
```

The WAL reclamation importer expects the exact 10,000- and 100,000-entry
component receipts plus their independently hashed summaries. It replays
physical-file, accounting, phase, and reopen invariants. The report labels
runner timings as diagnostics and does not promote this component check into
complete-service or competitor performance evidence.

When a paired suite contains several candidate batch caps, pass
`--headline-variant candidate-b32` (or another explicit variant). The report
never selects the most attractive candidate automatically. Paired runs may
retain both `openraft` and `openraft-async`; pass `--headline-control-variant`
to choose one explicitly. With more than one control, no headline is selected
implicitly. The baseline workflow exposes the same choices as
`headline_rafter_variant` and `headline_openraft_control`, so a multi-variant
run can publish a predeclared headline without choosing after seeing results.

## Compare changes

All active performance work uses `perf/raft-performance`. In paired CI, give
prior and candidate Rafter refs, choose each embedding mode, and keep batch caps
fixed to isolate a change. Runs alternate both revisions with OpenRaft on one
runner. Timing and diagnostic cases stay separate in `results/paired/`.

| Option | Purpose |
|---|---|
| Build `--peer-group-commit`, run `--peer-batch-size 32` | Drain ready peer events into one durable batch |
| Build/run `--ordered-apply` | Persist application work through Rafter's public bounded ordered worker |
| Run `--peer-message-stream --implementations rafter,raft-rs` | Send peer messages without empty transport replies |
| Build `--rafter-hard-state journal` or `wal` | Select supported native storage; `replace` is the default |
| Build/run `--pipelined-durability` | Rafter only: overlap eligible replication and local persistence |
| Run `--max-speculative-proposals N` | Bound ready proposal batches eligible for that overlap; default 1 |
| Run `--durable-completion-priority` | Experimental Rafter pipeline scheduling: handle safe ready replication ACKs before proposal batches larger than the speculative limit or one bounded ready prefix of unsubmitted writes, and release ready durable application completions before the next Raft persistence wait |
| Run `--openraft-async-flush --implementations openraft` | Exercise OpenRaft's callback-driven ordered log flusher |

Options require a Rafter revision supporting the corresponding API. Pipeline
mode also enables ordered apply and message transport. Paired CI offers
`inline`, `worker`, `messages`, and `pipeline` independently for each revision;
message modes run with both zero and 2 ms added loopback egress delay unless
`network_delays_ms` explicitly selects a smaller or different delay set.

Set `prior_rafter_ref` to enable paired CI. Choose `prior_hard_state` and
`rafter_hard_state` independently; keep both at `wal` to isolate pipelining.
Each run uses fresh directories; the WAL has no implicit format migration.
Build receipts and node stats record the choices. Use `--diagnostics` separately for stage counters,
empty-append reasons, and bounded sampled operation timelines; timing runs leave them disabled.
Each timeline names the adapter's first observable Raft commit callback and the later application
dispatch, start, durable completion, and client completion points. These are callback timestamps,
not kernel storage-commit timestamps. Diagnostics also retain owner persistence blocking,
replication-ack queue delay, proposal-batch distribution, and owner-observed full replication-window
time. Per-process samples retain host CPU, cgroup throttling, load, and Linux pressure counters when
available. Each new case derives and seals a measured-load summary from those raw samples;
verification replays both its sampling coverage and counter deltas. The layered report surfaces the
worst observed host signals without using them to remove, accept, or rank a result. Paired CI can sweep
speculative limits and restrict timing and diagnostic rates independently.
It can also retain synchronous and asynchronous OpenRaft storage controls as
separately named arms.

Current load-generator receipts retain raw fixed-memory histograms for scheduled
dispatch lateness and worker-start waiting. Verification replays their counts,
bucket-constrained maxima, and published summaries. The capacity table shows
dispatch p99 separately from worker-start and execution latency, so delayed load
generation is not guessed from unsent requests.

`--durable-completion-priority` is an opt-in measurement candidate, not the
default. Its receipt records prioritized peer batches and client completions
released before a later Raft persistence wait. Current receipts also count the
ready execute inputs bypassed by the zero-wait lookahead; they do not count
reads, status requests, wake boundaries, or unsafe Raft input because the scan
stops and restores the original order there. The scan retains at most one
configured client batch and does not delay proposal batches that remain within
the configured speculative limit. The paired runner requires current selected
variants to exercise that lookahead somewhere in the suite, while legacy
schema-1 receipts remain replayable and low-load cases may report no opportunity.

For an offered-load envelope, set `paired_rates` to a fixed curve such as
`1000,2000,3000,4000,6000,8000,10000,12000`, keep one Rafter batch cap and
speculative threshold, restrict diagnostics to one selected rate, and set
`network_delays_ms=0` when the first curve should isolate no-delay capacity. The
layered report declares useful capacity only when every repetition achieves at
least 99% of offered load, p99 is at most 20 ms, p99.9 is at most 50 ms, and
errors, unknown outcomes, and unsent requests are all zero. The detailed table
retains every tested point and client-start waiting time.

On a fixed machine, run the speculative-limit experiment without editing the
suite or choosing thresholds after measurement:

```sh
./raft-bench pipeline-thresholds \
  --rafter-sha <exact-40-character-sha> \
  --benchmark-sha <exact-benchmark-commit> \
  --data-root /path/to/benchmark-disk \
  --output results/pipeline-thresholds-<sha>
```

This holds the exact Rafter source, WAL, peer batch cap, append window,
completion-priority policy, topology, and network delay constant while testing
limits `1/2/4/8` at saturation and 1,000 writes/s. The prior arm is the same
exact SHA at limit 1, and diagnostics retain proposal geometry and pipeline
activity for every threshold. Six timing repetitions use a full cyclic arm
rotation, so each of the six implementations occupies every execution position
once at each rate. The default is 84 cases and 98 declared warmup/measurement
minutes, excluding qualification and process overhead. Machine qualification
runs before timing.

## Development

```sh
./scripts/check
./raft-bench build
./scripts/smoke
```

[Methodology](docs/methodology.md) · [Contributing](CONTRIBUTING.md) ·
[Manual multi-host setup](deploy/README.md)

Maintained by the Rafter author. Results describe these service integrations;
they are not upstream endorsements. Apache-2.0; see [NOTICE](NOTICE).

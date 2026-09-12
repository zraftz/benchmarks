# Raft benchmarks

Current qualified evidence: [one-minute summary](reports/qualified-34628543562/summary.md) ·
[static HTML](reports/qualified-34628543562/report.html) ·
[normalized JSON](reports/qualified-34628543562/summary.json)

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

```sh
./raft-bench microbench --runs 7

./raft-bench build --rafter-ref perf/raft-performance
./raft-bench run --rates 0,100,1000 --runs 3 --data-root /path/to/benchmark-disk
```

`--rafter-ref` is also accepted by `microbench`. Selection updates both local
manifests and lockfiles; omitted refs use the current selection. Use
`./raft-bench select-rafter <SHA>` to select once before running both suites.
Rafter source is fetched as a dependency; its checkout is never edited.
Pipeline experiments may vary the bounded per-follower replication window with
`--max-inflight-appends`; the default remains Rafter's eight append batches.

Results live under `results/`. Verify a case or an in-memory suite with:

```sh
./raft-bench verify <result-directory>
```

Generate or regenerate a layered report from immutable evidence with:

```sh
./raft-bench summary \
  --durable-suite /path/to/results/paired \
  --storage-suite /path/to/hard-state-evidence \
  --history-suite /path/to/prior/results/paired \
  --output /path/to/new-report
```

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
| Build/run `--ordered-apply` | Persist application work on one ordered worker |
| Run `--peer-message-stream --implementations rafter,raft-rs` | Send peer messages without empty transport replies |
| Build `--rafter-hard-state journal` or `wal` | Select supported native storage; `replace` is the default |
| Build/run `--pipelined-durability` | Rafter only: overlap eligible replication and local persistence |
| Run `--max-speculative-proposals N` | Bound ready proposal batches eligible for that overlap; default 1 |
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
time. Paired CI can sweep speculative limits and restrict timing and diagnostic rates independently.
It can also retain synchronous and asynchronous OpenRaft storage controls as
separately named arms.

For an offered-load envelope, set `paired_rates` to a fixed curve such as
`1000,2000,3000,4000,6000,8000,10000,12000`, keep one Rafter batch cap and
speculative threshold, restrict diagnostics to one selected rate, and set
`network_delays_ms=0` when the first curve should isolate no-delay capacity. The
layered report declares useful capacity only when every repetition achieves at
least 99% of offered load, p99 is at most 20 ms, p99.9 is at most 50 ms, and
errors, unknown outcomes, and unsent requests are all zero. The detailed table
retains every tested point and client-start waiting time.

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

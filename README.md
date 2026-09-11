# Raft benchmarks

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

The durable baseline runs 27 cases: three engines × three load levels × three
repetitions, with 60 seconds measured per case. Allow about 40–60 minutes.
`rafter-only` runs the extra in-memory read, codec, service, and MultiRaft probes.

## Run locally

Needs Rust 1.88.0, Python 3.11+, Go 1.22+, Git, `protoc`, and a C compiler.
Linux is recommended. Data storage must support file and directory syncs.

```sh
./raft-bench microbench --runs 3

./raft-bench build --rafter-ref perf/raft-performance
./raft-bench run --rates 0,100,1000 --runs 3 --data-root /path/to/benchmark-disk
```

`--rafter-ref` is also accepted by `microbench`. Selection updates both local
manifests and lockfiles; omitted refs use the current selection. Use
`./raft-bench select-rafter <SHA>` to select once before running both suites.
Rafter source is fetched as a dependency; its checkout is never edited.

Results live under `results/`. Verify a case or an in-memory suite with:

```sh
./raft-bench verify <result-directory>
```

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

Options require a Rafter revision supporting the corresponding API. Pipeline
mode also enables ordered apply and message transport. Paired CI offers
`inline`, `worker`, `messages`, and `pipeline` independently for each revision;
message modes run with both zero and 2 ms added loopback egress delay.

Set `prior_rafter_ref` to enable paired CI. Choose `prior_hard_state` and
`rafter_hard_state` independently; keep both at `wal` to isolate pipelining.
Each run uses fresh directories; the WAL has no implicit format migration.
Build receipts and node stats record the choices. Use `--diagnostics` separately for stage counters and
empty-append reasons; timing runs leave them disabled.

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

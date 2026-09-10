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
gh workflow run baseline.yml --repo zraftz/benchmarks \
  -f suite=both -f rafter_ref=perf/durable-crc32
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

./raft-bench build --rafter-ref perf/durable-crc32
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

To test the opt-in hard-state journal on a supporting Rafter branch:

```sh
./raft-bench build --rafter-ref perf/hard-state-journal --rafter-hard-state journal
```

The CI job exposes the same `rafter_hard_state` selector. Build receipts and
node stats record the backend; `replace` remains the default.

For a paired durable experiment, enable `paired` in the workflow and provide
both Rafter refs. It alternates the prior revision, candidate batch caps
(8/16/32/64), and OpenRaft on one runner. Results are in `results/paired/`.
Diagnostic cases run separately; their timings are never pooled with the sweep.

Locally, build a supporting revision with `--peer-group-commit`, then run with
`--peer-batch-size 32`. Add `--diagnostics` only for a separate instrumented run.

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

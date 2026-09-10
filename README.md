# raft-bench

A standalone comparison lab for replicated key/value services built with Rafter,
raft-rs, and OpenRaft. Maintained from the Rafter author's perspective; not an
independently governed benchmark or an upstream endorsement.

This repository contains a benchmark harness, not a published performance
result. Run the locked build and real-adapter smoke gates before collecting
measurements. See [VALIDATION.md](VALIDATION.md) for the checks performed and
remaining limits.

## Two suites, deliberately separate

| Suite | What it measures | How it gets here |
|---|---|---|
| Historical protocol comparison | Original in-memory Rafter / raft-rs / OpenRaft workloads | `import-protocol` extracts the original source and results from an exact Rafter commit |
| Durable replicated service | Client-visible Put, Get, and CAS over real TCP, synced consensus storage, and synced application journals | New adapter, workload, supervision, checking, and reporting source is included in this repository |

The default service runner launches **three processes on one host**, with an
external native load-generator process. It is not an in-process message pump,
but it is also not a three-physical-host experiment. Standalone node binaries
and an example inventory support manual multi-host deployment.

### Included

- Rust adapters for Rafter, raft-rs, and OpenRaft, with a common application model
  and client/peer framing. Membership is a static three-voter group.
- A Go load generator: closed-loop or scheduled-rate arrivals, persistent
  connections, same-identity retries, explicit unknown/not-issued accounting,
  fixed-memory histograms, and optional complete operation histories.
- A Python runner: build receipts, version pins, rotated execution order,
  immutable run directories, process resource samples, failure injection,
  restart canaries, independent finite-history checking, and static HTML reports.
- Exact-commit historical benchmark importer, tooling tests, Rust unit-test
  source, CI configuration, migration notes, and a manual three-host guide.

### Not implemented or not established yet

No TLS, optimized ReadIndex/lease reads, snapshot/compaction workload, membership
changes, multi-Raft workload, automatic remote provisioning, etcd-raft adapter,
or HashiCorp adapter. Upstream adapter review and production performance
qualification remain future work.
Unsupported snapshot operations fail explicitly rather than pretending to work.

## Start here

Use Linux for the full runner. Python **3.11+**, Go **1.22+**, Rust **1.88.0**,
Git, `protoc`, and a C compiler/linker are required. A Rustup installation reads
`rust-toolchain.toml`. Initial dependency/source downloads require network access.
The load generator itself uses no third-party Go modules. Data filesystems must
support file and directory syncs. Use disk-backed storage for performance runs;
tmpfs is suitable only for functional smoke checks.

```sh
./raft-bench doctor
./raft-bench build
./scripts/check
./scripts/smoke results/local-smoke
```

`build` requires the checked-in `Cargo.lock`, runs Rust workspace tests, builds
the adapters in release mode, runs Go's race tests, and stages all four binaries
in `dist/` with a build receipt. Every Cargo build uses `--locked`. Missing
lockfiles and failed builds stop qualification; source or binary changes require
a rebuild. `CARGO_TARGET_DIR` and Cargo's configured target directory are
supported; the runner uses the staged copies in `dist/`.

To keep caches, temporary files, and results on a separate development volume,
put the checkout there and set these before running the commands above:

```sh
mkdir -p .cache/tmp .cache/cargo .cache/go-build .cache/go-mod
export TMPDIR="$PWD/.cache/tmp"
export CARGO_HOME="$PWD/.cache/cargo"
export CARGO_TARGET_DIR="$PWD/target"
export GOCACHE="$PWD/.cache/go-build"
export GOMODCACHE="$PWD/.cache/go-mod"
```

CI uses Rust 1.88.0, Go 1.26.7, and Python 3.11 on Ubuntu 24.04. Go 1.22+
and Python 3.11+ are the source minimums; use the CI versions to reproduce its
checks. No third-party Python or Go packages are required.

A smoke run limits the measured interval and concurrency, performs finite
qualification and process-restart checks, and marks its report **SMOKE ONLY**.
The full run will stop or retain a failed case when a build, correctness,
recovery, or accounting check fails. It never substitutes a test double for an
unavailable adapter.

### Historical in-memory suite

```sh
./raft-bench import-protocol
./raft-bench protocol --mode full --runs 6
```

The original benchmark files are **not duplicated in this repository**. The
importer fetches them from:

```text
zsumz/rafter @ 518aefd767dc0ef2c1f1ccc5f970c96456cfd109
```

It validates Git blob identities, preserves the original methodology and result
files, and replaces local Rafter paths with that exact Git revision. Original
and regenerated lockfiles are kept separately. This import path could not be
executed here; its rewrite and integrity helpers have unit tests. Historical
results do not appear in the durable-service report. See
[protocol/README.md](protocol/README.md).

## Run an evidence-oriented local experiment

Select the actual storage volume, rather than relying on a temporary directory:

```sh
./raft-bench run \
  --implementations rafter,raft-rs,openraft \
  --scenario durable-kv \
  --rates 100,1000,5000 \
  --duration 60 --warmup 10 --runs 6 \
  --concurrency 64 --payload 512 \
  --data-root /path/to/benchmark-ssd
```

Rates are offered application operations per second. `--rates 0` selects a
bounded-concurrency closed loop. Each rate is a separate experiment, not a
promise that the system sustains it. Repetitions rotate implementation order;
six repetitions balance positions across three adapters. Data directories and
result directories are newly allocated for each case and are not deleted by
the runner. Plan disk space: logs are retained without compaction.

Evidence mode requires a measured interval of at least 60 seconds, at least
5 seconds of warm-up, three repetitions, and an explicit data root. These gates
reduce accidental smoke-scoreboard publication; they do **not** certify hardware
isolation, fairness, steady state, or correctness.

Other included scenarios:

```sh
./raft-bench run --scenario leader-loss --data-root /path/to/benchmark-ssd
./raft-bench run --scenario follower-catchup --data-root /path/to/benchmark-ssd
./raft-bench run --rates 1000 --read-percent 80 --cas-percent 5 \
  --data-root /path/to/benchmark-ssd
```

Leader loss sends SIGKILL to the currently observed leader at approximately
one third of the measured interval, then restarts that process with its original
data at two thirds. Follower catch-up pauses and resumes a follower on the same
schedule. It exercises retained-log catch-up, **not snapshot transfer**. Election
and workload timestamps are retained; the report does not yet calculate a
precise recovery-to-SLO metric.

## What success means

All three adapters target the same client contract: successful execution means
Raft commitment plus application of the command through a separately synced
application journal. A retry uses the same client/session and sequence identity.
The replicated model rejects stale sequences or identity reuse with different
contents. One logical operation may have multiple network attempts and log
proposals; it is counted once at the client.

**Get is a logged operation in this first suite.** It shares the durable,
replicated application path rather than silently comparing one implementation's
lease read with another's quorum read. This is intentionally not a benchmark
of the libraries' fastest available read APIs.

The adapters are not identical internally: Rafter uses its native file stores;
raft-rs and OpenRaft use declared benchmark journals. Peer codecs, callback
batching, scheduler models, and election behavior also differ. Results describe
these complete embeddings, not a pure consensus-kernel ranking. See the
[contract](BENCHMARK_CONTRACT.md) and [adapter notes](docs/ADAPTERS.md).

## Inspect a run

A suite contains `report.html`, `suite.json`, `completion.json`, and one directory
per implementation/rate/repetition. A completed case contains:

```text
manifest.json                  # Source/binary identity, options, host, pins
measurement.json               # Counts, histograms, per-second completion data
qualification-history.jsonl    # Small complete Put/Get/CAS history
qualification.json             # Finite history verdict
preflight-recovery.json        # Initial acknowledged-canary restart check
recovery.json                  # Pre/post-load canary restart check
fault.json                     # Fault actions and controller timestamps
process-samples.jsonl          # Linux node/load-generator CPU, RSS, I/O samples
before.json / after.json       # Adapter and application counters
cluster/                       # Node configs and process logs
SHA256SUMS.json                 # Case artifact inventory and integrity hashes
```

```sh
./raft-bench verify results/runs/<suite>/<case>
./raft-bench report results/runs/<suite> --output results/review.html
```

Verification recomputes file hashes, checks count/histogram accounting, reruns
the finite history checker, and requires recorded restart checks to pass. The
checksum file is not a signature or independent attestation. The full measured
history is intentionally not retained by default; restart canaries do not
establish that every measured write survived.

## Tests and development

```sh
./scripts/check          # formatting, Go vet/race tests, Python tests, Clippy, Rust tests
./scripts/check tooling  # Go and Python only
./scripts/check rust     # Rust only
./raft-bench build       # locked release build plus test/build receipt
./scripts/smoke          # 12 cases across all adapters and three scenarios
```

The smoke script covers both closed-loop and scheduled-rate durable KV, leader
loss, and follower pause/resume. It retains all cases and exits unsuccessfully
if any case fails. Every case also checks a finite Put/Get/CAS history and
acknowledged canaries across simultaneous process restarts.

The Python integration fixture is explicitly named `test-fixture`, shares a
SQLite database, and is **not a consensus implementation**. The public CLI
allowlist rejects it. Its tests check supervision, sockets, accounting,
verification, and failure-path cleanup only.

[CI](.github/workflows/ci.yml) runs on pull requests, pushes to `main`, and manual
dispatch. It uses pinned action commits, read-only repository permissions,
timeouts, and uploads build receipts and smoke evidence even on failure. It
checks correctness and tool execution without an absolute-throughput threshold.
See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow.

## Run a baseline on GitHub

The manual **Baseline benchmark** workflow runs all three engines at closed-loop,
100/s, and 1000/s load, with three repetitions of each configuration. Each case
has 10 seconds of warmup and 60 seconds of measurement. The default scenario is
`durable-kv`; leader loss and follower catchup can be selected separately.

```sh
gh workflow run baseline.yml --repo zraftz/benchmarks --ref main -f scenario=durable-kv
```

Download the workflow's `baseline-<scenario>-<run-id>` artifact and open
`results/baseline/report.html`. It includes raw evidence, logs, and build identity.
The run takes at least 32 minutes plus build and qualification time. These shared
runner measurements are an initial reference; use controlled hardware and review
before publishing a general performance ranking. CI's shorter smoke suite runs
separately on pushes and pull requests.

## Reading order

[Contract](BENCHMARK_CONTRACT.md) → [adapters](docs/ADAPTERS.md) →
[measurement](docs/MEASUREMENT.md) → [three hosts](deploy/README.md) →
[migration](docs/MIGRATION.md) → [roadmap](docs/ROADMAP.md).

Apache-2.0 for the new harness. Imported source retains its upstream notices;
dependencies retain their own licenses. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

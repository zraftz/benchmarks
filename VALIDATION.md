# Validation status — 2026-09-10

Ready for an initial repository and CI run. Locked builds, development checks,
and all 24 real-adapter smoke cases passed locally: 12 on native disk-backed
storage and 12 in Linux using tmpfs. GitHub-hosted CI has not run.

## Passed locally

Native host: macOS 15.1.1, ARM64, Rust 1.88.0, Python 3.14.6. Linux container:
Debian 12 ARM64 (`rust:1.88.0-bookworm`), Rust 1.88.0, Go 1.26.7, Python 3.11.2,
and protoc 29.3. Source and retained evidence live on zdev; disposable reruns
used the user-authorized temporary workspace.

| Check | Result |
|---|---|
| Dependency resolution | Checked-in Cargo.lock; subsequent builds used `--locked` |
| Rust workspace tests | 5 passed on each platform; all three adapter targets compiled |
| Rust formatting and strict Clippy | Passed on each platform, including all targets and `-D warnings` |
| Release builds and receipts | All three adapters and the Go load generator staged; source, lockfile, and binary hashes recorded |
| External Cargo target directory | Linux release build staged binaries successfully from a target directory outside the source tree |
| Python tests | 31 passed on each platform, including four process/fault integrations and three build regressions |
| Go vet and race tests | 6 tests passed on each platform; native Go 1.24.1 and 1.26.7, Linux Go 1.26.7 |
| Native real-adapter smoke | 12 passed on disk-backed APFS temporary storage |
| Linux real-adapter smoke | 12 passed using tmpfs for functional process/restart coverage |
| Evidence verification | All 24 retained case inventories, checksums, accounting, histories, and recovery records independently rechecked |
| Workflow and shell lint | actionlint and ShellCheck passed |
| Repository packaging | Ignore inventory, executable modes, whitespace, and local documentation links checked |

The 12-case suite covers all three implementations: durable-kv at closed-loop
and 100 requests/second, leader loss, and follower catchup. Smoke results establish
bounded functional coverage; they are not comparative performance results or
power-loss durability evidence. Python integration tests use the explicitly
non-Raft SQLite fixture and establish tooling behavior.

The first Rust compilation required an explicit OpenRaft snapshot data type.
Strict Clippy also identified a pending-command type that needed an alias.
Both were fixed without changing the benchmark contract.

## Storage findings and retained evidence

The original zdev smoke run failed all 12 cases during qualification with unknown
operation outcomes. The host showed prolonged filesystem stalls, including a
compiler blocked in a file write and a startup probe exceeding its 30-second
deadline, despite more than 400 GiB free. The same staged native binaries then
passed all 12 cases on internal temporary storage. This supports storage/host
behavior as a contributor to the original failures. Qualification criteria and
required sync operations were unchanged.

Docker's macOS bind mount rejected directory `fsync` with `EBADF`. Linux checks
therefore used tmpfs for test data. A first smoke attempt exhausted a 256 MiB
tmpfs; rerunning the unchanged suite with 2 GiB passed. Linux results cover
process failures and restart recovery, not physical-storage durability.

Evidence is retained in ignored directories under the zdev working copy:

- `results/tmp-qualification/`: 12 successful native cases and run summary.
- `results/linux-qualification/`: 12 successful Linux cases, checks/build log,
  build receipt, smoke log, summary, and prior container diagnostics.
- `results/repository-readiness/`: original zdev failures and node logs.
- `.cache/`: native tooling/build logs; `dist/`: native binaries and build receipt.

Temporary disk use and configured tmpfs capacity stayed within the authorized
4 GB allowance. The disposable temporary workspace and test containers have
been removed; copied evidence was verified before cleanup.

## CI and remaining scope

The workflow targets Ubuntu 24.04, Rust 1.88.0, Go 1.26.7, and Python 3.11. It
runs formatting, lint, unit/integration tests, a locked release build, and the
12-case real-adapter suite, retaining evidence on failure. The local Linux run
used Debian ARM64; it does not replace the first GitHub-hosted workflow run.

At this local-validation checkpoint, no remote had been created or pushed.
Historical protocol import/reruns,
multi-host qualification, power-loss testing, upstream adapter review, and
performance publication remain outside this validation.

The original delivery's logs and report are preserved in
[validation/source-archive.md](validation/source-archive.md). Its
[source manifest](validation/source-archive-manifest.json) describes the original
archive, not the current source tree.

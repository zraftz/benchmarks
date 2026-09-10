# Validation performed for this delivery

## Passed in this environment

**28 Python tests and 6 Go tests** passed. The Go tests ran with the race detector.

| Check | Result | Retained log |
|---|---|---|
| Python history-checker and evidence/import-helper unit tests | 24 passed | [python-unit-tests.log](python-unit-tests.log) |
| Closed-loop tooling integration | Passed | [integration-closed-loop.log](integration-closed-loop.log) |
| Scheduled-rate tooling integration | Passed | [integration-scheduled-rate.log](integration-scheduled-rate.log) |
| Leader-process-loss tooling integration | Passed | [integration-leader-loss.log](integration-leader-loss.log) |
| Follower-pause/resume tooling integration | Passed | [integration-follower-catchup.log](integration-follower-catchup.log) |
| Go histogram, framing, and node-parser tests with `-race` | 6 passed | [go-race-tests.log](go-race-tests.log) |

The four integration tests use the real Go load generator and Python supervisor
against an explicitly non-Raft shared-SQLite test fixture. They exercise sockets,
process lifecycle/fault actions, client accounting, finite-history checking,
canary verification, artifact sealing, and cleanup. They do **not** test Raft
consensus, network replication, or the Rust adapter implementations. Fixture
throughput is not included as a benchmark result.

TOML parsing, Python syntax, shell syntax, CLI help, local documentation links,
example configuration generation, and ZIP integrity are checked during packaging.
The source manifest records SHA-256 digests of the packaged files; it is not a
signature or independent certification.

## Not executed

The environment has Python, Go, and Git, but no Rust compiler, Cargo, or protoc;
see [doctor.json](doctor.json). Direct outbound network access was
also unavailable. Consequently none of the following was completed here:

- Root Cargo.lock resolution, Rust compilation, Rust tests, Clippy, or rustfmt.
- The exact-source historical importer end to end, or original protocol reruns.
- A cluster using any of the three real Rust adapters, including their
  qualification, durable-write, failover, or restart checks.
- Multi-host performance runs, power-loss injection, or GitHub Actions execution.

The Rust adapters are written source, not validated binaries. API or integration
errors may remain. **Do not publish comparative performance claims from this
archive until `./raft-bench build` and the real-adapter smoke scenarios pass.**
Retain and review their results; do not weaken the checks just to obtain a score.
The bundled CI is configured to run those gates but has not run for this delivery.

## Exact commands used for the executed tests

```sh
PYTHONPATH=.:tests python3 -m unittest test_checker test_evidence -v
PYTHONPATH=.:tests python3 -m unittest test_integration.IntegrationTests.test_closed_loop_end_to_end -v
PYTHONPATH=.:tests python3 -m unittest test_integration.IntegrationTests.test_scheduled_rate_end_to_end -v
PYTHONPATH=.:tests python3 -m unittest test_integration.IntegrationTests.test_leader_process_loss -v
PYTHONPATH=.:tests python3 -m unittest test_integration.IntegrationTests.test_follower_pause_and_resume -v
(cd loadgen && GOTOOLCHAIN=local GOPROXY=off go test -race -count=1 -v ./...)
```

The integration cases were run separately to fit the execution environment's
per-command limit. Compiled binaries, build caches, fixture databases, and smoke
measurement artifacts are excluded from the source ZIP.

# Rafter implementation performance

Where Rafter demonstrably leads, where evidence is mixed, and what remains unmeasured.

## Consensus in memory

*How quickly can the implementation replicate and commit without disks or sockets?*

**Comparison pending qualification.**

Three voters in one process, in-memory stores, no disk synchronization or TCP.

- The suite exists, but no in-memory evidence set was selected for this report.

## Durable replication

*How efficiently does Rafter make consensus state survive a restart?*

**No component evidence selected.**

Not yet measured: equivalent OpenRaft persistence comparison; WAL reclamation under complete service traffic; Combined WAL physical-reclamation component qualification.

## Complete durable service

*How many durable writes can an application complete, and how long do clients wait?*

**Rafter pipeline + completion priority completed 3.57× as many durable writes per second as the tested OpenRaft async flusher integration in the no-delay condition.**

Benchmark source: `ff5c1253dcae6734b080731ddbea41c7e8818cdd` (clean at preflight).

| Verdict | Status | Detail |
| --- | --- | --- |
| Evidence integrity | passed | expected cases, seals, identities, receipts, and accounting |
| Correctness checks | passed | finite history, restart, and smoke checks; not exhaustive proof |
| Environment qualification | not measured | sealed idle/storage profile immediately after builds; per-case pressure remains separate |
| Feature coverage | passed | candidate-b32-commit-first: completion-priority and bounded-lookahead activity were both observed |
| Service objective | passed | At 0 ms added egress, Rafter met the declared service objective through at least 1,000 writes/s; the selected OpenRaft control met it through at least 1,000 writes/s. |

Measured-load host context (72/72 timing cases): maximum CPU busy 65.771%, iowait 21.533%, steal 0.000%, cgroup throttling 0.000%, I/O PSI some 31.298%, and hardware-throttle counter delta not exposed. Workload and unrelated host activity are inseparable in these counters; they never remove, accept, or rank a result.

| Added loopback egress | Rafter throughput | OpenRaft throughput | Advantage |
| ---: | ---: | ---: | ---: |
| 0 ms | **12,060 writes/s** | 3,382 writes/s | **3.57×** |
| 2 ms | **5,433 writes/s** | 2,166 writes/s | **2.51×** |

| Added egress | Offered rate | Rafter p99 | OpenRaft p99 | Rafter p99.9 | OpenRaft p99.9 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 ms | saturation | 13.107 ms | 21.234 ms | 19.137 ms | 27.001 ms |
| 0 ms | 100/s | 2.359 ms | 2.753 ms | 2.687 ms | 6.291 ms |
| 0 ms | 1,000/s | 2.785 ms | 14.680 ms | 7.471 ms | 18.088 ms |
| 2 ms | saturation | 23.331 ms | 31.195 ms | 25.690 ms | 32.768 ms |
| 2 ms | 100/s | 13.369 ms | 13.894 ms | 14.287 ms | 14.680 ms |
| 2 ms | 1,000/s | 11.010 ms | 22.807 ms | 14.025 ms | 23.331 ms |

Rafter accounting: 0 errors · 0 unknown · 0 unsent
OpenRaft accounting: 0 errors · 0 unknown · 0 unsent

Rafter pipeline + completion priority versus same-code Rafter pipeline FIFO throughput — 0 ms: +45.1% · 2 ms: +1.9%

| Added egress | Offered rate | Rafter pipeline + completion priority p99 | Rafter pipeline FIFO p99 | Rafter pipeline + completion priority p99.9 | Rafter pipeline FIFO p99.9 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 ms | saturation | 13.107 ms | 25.166 ms | 19.137 ms | 31.457 ms |
| 0 ms | 100/s | 2.359 ms | 2.392 ms | 2.687 ms | 5.767 ms |
| 0 ms | 1,000/s | 2.785 ms | 2.720 ms | 7.471 ms | 8.061 ms |
| 2 ms | saturation | 23.331 ms | 24.117 ms | 25.690 ms | 27.525 ms |
| 2 ms | 100/s | 13.369 ms | 13.369 ms | 14.287 ms | 14.287 ms |
| 2 ms | 1,000/s | 11.010 ms | 11.141 ms | 14.025 ms | 16.515 ms |

Useful-capacity objective: every repetition achieves at least 99% of offered load, p99 ≤ 20 ms, p99.9 ≤ 50 ms, and zero errors, unknown outcomes, or unsent requests.

**At 0 ms added egress, Rafter met the declared service objective through at least 1,000 writes/s; the selected OpenRaft control met it through at least 1,000 writes/s.**

| Added egress | Configuration | Highest qualifying rate | Tested through |
| ---: | --- | ---: | ---: |
| 0 ms | Rafter pipeline + completion priority | at least 1,000/s | 1,000/s |
| 0 ms | Rafter pipeline FIFO | at least 1,000/s | 1,000/s |
| 0 ms | OpenRaft async flusher | at least 1,000/s | 1,000/s |
| 0 ms | OpenRaft synchronous | at least 1,000/s | 1,000/s |
| 2 ms | Rafter pipeline + completion priority | at least 1,000/s | 1,000/s |
| 2 ms | Rafter pipeline FIFO | at least 1,000/s | 1,000/s |
| 2 ms | OpenRaft async flusher | 100/s | 1,000/s |
| 2 ms | OpenRaft synchronous | at least 1,000/s | 1,000/s |

| Added egress | Offered | Engine | Min achieved | Arrival p99/p99.9 | Execution p99/p99.9 | Dispatch p99/p99.9 | Start-wait p99 | Post-window | Loss | Meets objective |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| 0 ms | 100/s | Rafter | 100/s (100.0%) | 2.359 ms / 2.753 ms | 1.475 ms / 2.048 ms | 1.065 ms / 1.073 ms | 1.081 ms | 0 | 0 | yes |
| 0 ms | 100/s | OpenRaft | 100/s (100.0%) | 2.785 ms / 6.619 ms | 1.917 ms / 6.160 ms | 1.065 ms / 1.081 ms | 1.081 ms | 0 | 0 | yes |
| 0 ms | 1,000/s | Rafter | 1,000/s (100.0%) | 2.785 ms / 7.864 ms | 1.966 ms / 7.471 ms | 1.024 ms / 1.081 ms | 1.040 ms | 4 | 0 | yes |
| 0 ms | 1,000/s | OpenRaft | 1,000/s (100.0%) | 17.564 ms / 20.447 ms | 17.039 ms / 20.185 ms | 1.040 ms / 1.081 ms | 1.065 ms | 12 | 0 | yes |
| 2 ms | 100/s | Rafter | 100/s (100.0%) | 13.369 ms / 14.418 ms | 13.238 ms / 13.631 ms | 1.065 ms / 1.073 ms | 1.081 ms | 3 | 0 | yes |
| 2 ms | 100/s | OpenRaft | 100/s (100.0%) | 14.025 ms / 14.680 ms | 13.500 ms / 14.025 ms | 1.065 ms / 1.081 ms | 1.081 ms | 3 | 0 | yes |
| 2 ms | 1,000/s | Rafter | 1,000/s (100.0%) | 11.141 ms / 18.874 ms | 10.355 ms / 18.612 ms | 1.065 ms / 1.114 ms | 1.081 ms | 29 | 0 | yes |
| 2 ms | 1,000/s | OpenRaft | 1,000/s (100.0%) | 22.807 ms / 27.001 ms | 22.282 ms / 26.477 ms | 1.065 ms / 1.081 ms | 1.081 ms | 54 | 0 | no |

three processes on one host; real TCP, not three physical hosts; 64 clients; 512-byte writes; medians of 3 repetitions. Success requires Raft commitment and durable application completion. Added delay affects client and peer egress. Storage, codec, and scheduling choices differ between integrations. Selected identities: Rafter 88d43848e460; OpenRaft async flusher 0.9.24.

Evidence: [Detailed service results](https://github.com/zraftz/benchmarks/actions/runs/35386922113)

## Failure and sustained operation

*Does performance remain useful during failures and over long runs?*

**Complete-service snapshot catch-up smoke passed; Finite recovery checks passed; comparative fault and sustained-operation performance remain unmeasured.**

- restart and acknowledged-canary checks: 92 cases passed — finite histories and canaries, not every measured write or power loss
- linearizable qualification histories: 92 cases passed — 64-operation per-case qualification histories
- Rafter recovery smoke: durable-kv passed, follower-catchup passed, leader-loss passed, snapshot-catchup passed — smoke correctness only; not comparative performance
- complete-service snapshot catch-up smoke: 1 case passed — live writes, Raft snapshot and WAL reclamation, lagging-follower snapshot installation, and restart canaries; functional smoke only, not performance

Not yet measured: comparative leader-loss performance; comparative slow-follower performance; storage-stall recovery performance; long-duration snapshot/compaction and WAL-reclamation soak performance.

Evidence: [Detailed service results](https://github.com/zraftz/benchmarks/actions/runs/35386922113)

---

This is implementation performance, not a claim of a faster Raft algorithm. The report is generated deterministically from normalized evidence; no LLM writes or selects results.

# Rafter implementation performance

Where Rafter demonstrably leads, where evidence is mixed, and what remains unmeasured.

## Consensus in memory

*How quickly can the implementation replicate and commit without disks or sockets?*

**Rafter leads 12 of 12 qualified throughput and p99 comparisons with no measured regressions.**

| Workload | In flight | Rafter | raft-rs | OpenRaft | Rafter p99 | raft-rs p99 | OpenRaft p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| large_payload | 16 | 4,170/s | 1,009/s | 764/s | 4,203.3 µs | 15,756.0 µs | 22,794.5 µs |
| pipelined | 64 | 889,845/s | 318,128/s | 269,262/s | 184.3 µs | 217.4 µs | 264.1 µs |
| serial | 1 | 347,506/s | 196,551/s | 44,552/s | 8.3 µs | 11.3 µs | 41.3 µs |

Three voters in one process, in-memory stores, no disk synchronization or TCP. Workloads use submission bursts, not a continuously replenished concurrency window.

- All adapters execute the same leader-side reference application operation; 7 repetitions.
- Selected identities: rafter 3f2cdc956eaa; raft-rs 0.7.0 (crate `raft`, prost-codec); openraft 0.9.24 (features: storage-v2; current-thread tokio).

Evidence: [In-memory evidence](https://github.com/zraftz/benchmarks/actions/runs/34654362991)

## Durable replication

*How efficiently does Rafter make consensus state survive a restart?*

**The append-only journal improved throughput 40.9–61.3% over Rafter's file-replacement backend.**

| Batch | File replacement | Append-only journal | Throughput change | Replacement p99 | Journal p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 324/s | 522/s | +61.3% | 6.086 ms | 3.846 ms |
| 8 | 2,496/s | 3,815/s | +52.9% | 6.952 ms | 4.410 ms |
| 32 | 9,490/s | 14,529/s | +53.1% | 6.281 ms | 4.506 ms |
| 128 | 33,176/s | 46,740/s | +40.9% | 7.283 ms | 5.505 ms |

Separate hard-state syscall probe: file replacement 2.0, journal 1.0 sync calls/publication. This is not synchronization calls per committed entry.

WAL physical-reclamation component qualification:

| Retained suffix | Managed WAL | Files | Exact recovery | Diagnostic pause p99 | Diagnostic reopen |
| ---: | ---: | ---: | :---: | ---: | ---: |
| 512 entries | 50,364 bytes | 3 | passed | 3.858 ms | 0.475 ms |
| 10,000 entries | 980,188 bytes | 3 | passed | 7.035 ms | 2.055 ms |
| 100,000 entries | 9,800,188 bytes | 3 | passed | 197.255 ms | 17.338 ms |

Three repeated component cycles at each retained suffix. Physical bounds and exact reopen are qualified; hosted-runner pause and reopen times are diagnostic only.

One identical Rafter binary, 256-byte proposals, three repetitions; batch size is explicit in every row. Selected Rafter revision: a0329e8c45b9.

Not yet measured: equivalent OpenRaft persistence comparison; WAL reclamation under complete service traffic.

Evidence: [Storage evidence](https://github.com/zsumz/rafter/actions/runs/34907033375) · [WAL reclamation evidence](https://github.com/zsumz/rafter/actions/runs/34907033422)

## Complete durable service

*How many durable writes can an application complete, and how long do clients wait?*

**Rafter pipeline + completion priority completed 3.96× as many durable writes per second as the tested OpenRaft async flusher integration in the no-delay condition.**

Benchmark source: `e446d14b480d4dcfc1a8a8057a898c36af8d3a23` (clean at preflight).

| Verdict | Status | Detail |
| --- | --- | --- |
| Evidence integrity | passed | expected cases, seals, identities, receipts, and accounting |
| Correctness checks | passed | finite history, restart, and smoke checks; not exhaustive proof |
| Environment qualification | not measured | sealed idle/storage profile immediately after builds; per-case pressure remains separate |
| Feature coverage | passed | candidate-b32-commit-first: completion-priority and bounded-lookahead activity were both observed |
| Service objective | passed | At 0 ms added egress, Rafter met the declared service objective through at least 1,000 writes/s; the selected OpenRaft control met it through at least 1,000 writes/s. |

Measured-load host context (72/72 timing cases): maximum CPU busy 52.450%, iowait 28.066%, steal 0.000%, cgroup throttling 0.000%, I/O PSI some 32.658%, and hardware-throttle counter delta not exposed. Workload and unrelated host activity are inseparable in these counters; they never remove, accept, or rank a result.

| Added loopback egress | Rafter throughput | OpenRaft throughput | Advantage |
| ---: | ---: | ---: | ---: |
| 0 ms | **10,263 writes/s** | 2,590 writes/s | **3.96×** | 
| 2 ms | **5,042 writes/s** | 1,827 writes/s | **2.76×** | 

| Added egress | Offered rate | Rafter p99 | OpenRaft p99 | Rafter p99.9 | OpenRaft p99.9 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 ms | saturation | 10.748 ms | 29.360 ms | 13.369 ms | 30.933 ms |
| 0 ms | 100/s | 3.211 ms | 3.801 ms | 6.160 ms | 6.488 ms |
| 0 ms | 1,000/s | 4.063 ms | 9.568 ms | 6.816 ms | 13.763 ms |
| 2 ms | saturation | 16.253 ms | 39.846 ms | 18.350 ms | 42.992 ms |
| 2 ms | 100/s | 13.894 ms | 14.549 ms | 14.680 ms | 17.302 ms |
| 2 ms | 1,000/s | 12.583 ms | 24.117 ms | 14.811 ms | 53.477 ms |

Rafter accounting: 0 errors · 0 unknown · 0 unsent
OpenRaft accounting: 0 errors · 0 unknown · 0 unsent

Rafter pipeline + completion priority versus same-code Rafter pipeline FIFO throughput — 0 ms: +29.3% · 2 ms: +5.9%

| Added egress | Offered rate | Rafter pipeline + completion priority p99 | Rafter pipeline FIFO p99 | Rafter pipeline + completion priority p99.9 | Rafter pipeline FIFO p99.9 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 ms | saturation | 10.748 ms | 16.384 ms | 13.369 ms | 21.234 ms |
| 0 ms | 100/s | 3.211 ms | 3.211 ms | 6.160 ms | 5.898 ms |
| 0 ms | 1,000/s | 4.063 ms | 5.046 ms | 6.816 ms | 17.302 ms |
| 2 ms | saturation | 16.253 ms | 18.350 ms | 18.350 ms | 22.807 ms |
| 2 ms | 100/s | 13.894 ms | 13.763 ms | 14.680 ms | 14.680 ms |
| 2 ms | 1,000/s | 12.583 ms | 13.107 ms | 14.811 ms | 14.942 ms |

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
| 2 ms | OpenRaft synchronous | 100/s | 1,000/s |

| Added egress | Offered | Engine | Min achieved | Arrival p99/p99.9 | Execution p99/p99.9 | Dispatch p99/p99.9 | Start-wait p99 | Post-window | Loss | Meets objective |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| 0 ms | 100/s | Rafter | 100/s (100.0%) | 3.211 ms / 6.160 ms | 2.687 ms / 5.571 ms | 1.081 ms / 1.095 ms | 1.081 ms | 0 | 0 | yes |
| 0 ms | 100/s | OpenRaft | 100/s (100.0%) | 3.834 ms / 6.685 ms | 3.146 ms / 5.964 ms | 1.081 ms / 1.081 ms | 1.081 ms | 0 | 0 | yes |
| 0 ms | 1,000/s | Rafter | 1,000/s (100.0%) | 4.260 ms / 10.486 ms | 3.670 ms / 9.961 ms | 1.065 ms / 1.098 ms | 1.081 ms | 5 | 0 | yes |
| 0 ms | 1,000/s | OpenRaft | 1,000/s (100.0%) | 9.830 ms / 14.025 ms | 9.306 ms / 13.500 ms | 1.081 ms / 1.163 ms | 1.114 ms | 8 | 0 | yes |
| 2 ms | 100/s | Rafter | 100/s (100.0%) | 13.894 ms / 14.811 ms | 13.500 ms / 14.025 ms | 1.081 ms / 1.098 ms | 1.081 ms | 3 | 0 | yes |
| 2 ms | 100/s | OpenRaft | 100/s (100.0%) | 14.680 ms / 18.874 ms | 14.025 ms / 18.350 ms | 1.081 ms / 1.098 ms | 1.081 ms | 3 | 0 | yes |
| 2 ms | 1,000/s | Rafter | 1,000/s (100.0%) | 12.976 ms / 15.204 ms | 12.452 ms / 14.549 ms | 1.081 ms / 1.147 ms | 1.114 ms | 31 | 0 | yes |
| 2 ms | 1,000/s | OpenRaft | 1,000/s (100.0%) | 24.379 ms / 54.002 ms | 23.855 ms / 54.002 ms | 1.065 ms / 1.098 ms | 1.098 ms | 53 | 0 | no |

three processes on one host; real TCP, not three physical hosts; 64 clients; 512-byte writes; medians of 3 repetitions. Success requires Raft commitment and durable application completion. Added delay affects client and peer egress. Storage, codec, and scheduling choices differ between integrations. Selected identities: Rafter 8c3dc15df070; OpenRaft async flusher 0.9.24.

Evidence: [Detailed service results](https://github.com/zraftz/benchmarks/actions/runs/34904893568)

## Failure and sustained operation

*Does performance remain useful during failures and over long runs?*

**Complete-service snapshot catch-up smoke passed; Finite recovery checks passed; WAL physical-reclamation component checks passed; comparative fault and sustained-operation performance remain unmeasured.**

- restart and acknowledged-canary checks: 92 cases passed — finite histories and canaries, not every measured write or power loss
- linearizable qualification histories: 92 cases passed — 64-operation per-case qualification histories
- Rafter recovery smoke: durable-kv passed, follower-catchup passed, leader-loss passed, snapshot-catchup passed — smoke correctness only; not comparative performance
- complete-service snapshot catch-up smoke: 1 case passed — live writes, Raft snapshot and WAL reclamation, lagging-follower snapshot installation, and restart canaries; functional smoke only, not performance
- WAL physical-reclamation component: 3 cases passed — 512, 10k, and 100k retained suffixes across repeated writes, snapshots, compaction, physical cleanup, and exact reopen; not complete-service traffic or latency evidence

Visible history:

- Run 34599945967: 3 affected cases, 681 unsent, 0 unknown, 0 errors. Individual cases remain in `summary.json`.
- Run 34673142049: 24 affected cases, 41998 unsent, 0 unknown, 0 errors. Individual cases remain in `summary.json`.

Not yet measured: comparative leader-loss performance; comparative slow-follower performance; storage-stall recovery performance; long-duration snapshot/compaction and WAL-reclamation soak performance.

Evidence: [Detailed service results](https://github.com/zraftz/benchmarks/actions/runs/34904893568) · [Historical run 1](https://github.com/zraftz/benchmarks/actions/runs/34599945967) · [Historical run 2](https://github.com/zraftz/benchmarks/actions/runs/34673142049) · [WAL reclamation evidence](https://github.com/zsumz/rafter/actions/runs/34907033422)

---

This is implementation performance, not a claim of a faster Raft algorithm. The report is generated deterministically from normalized evidence; no LLM writes or selects results.

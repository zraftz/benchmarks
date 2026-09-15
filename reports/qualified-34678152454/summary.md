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

**The append-only journal improved throughput 45.3–61.4% over Rafter's file-replacement backend.**

| Batch | File replacement | Append-only journal | Throughput change | Replacement p99 | Journal p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 358/s | 578/s | +61.4% | 4.275 ms | 2.692 ms |
| 8 | 2,778/s | 4,335/s | +56.1% | 4.111 ms | 2.679 ms |
| 32 | 10,276/s | 15,839/s | +54.1% | 4.066 ms | 2.918 ms |
| 128 | 36,866/s | 53,563/s | +45.3% | 4.774 ms | 3.641 ms |

Separate hard-state syscall probe: file replacement 2.0, journal 1.0 sync calls/publication. This is not synchronization calls per committed entry.

One identical Rafter binary, 256-byte proposals, three repetitions; batch size is explicit in every row. Selected Rafter revision: 8b3f7e71cb23.

Not yet measured: Combined WAL component comparison.

Evidence: [Storage evidence](https://github.com/zsumz/rafter/actions/runs/34670447563)

## Complete durable service

*How many durable writes can an application complete, and how long do clients wait?*

**Rafter pipeline + completion priority completed 3.52× as many durable writes per second as the tested OpenRaft synchronous integration in the no-delay condition.**

| Added loopback egress | Rafter throughput | OpenRaft throughput | Advantage |
| ---: | ---: | ---: | ---: |
| 0 ms | **9,005 writes/s** | 2,559 writes/s | **3.52×** | 
| 2 ms | **4,810 writes/s** | 1,884 writes/s | **2.55×** | 

| Added egress | Offered rate | Rafter p99 | OpenRaft p99 | Rafter p99.9 | OpenRaft p99.9 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 ms | saturation | 12.845 ms | 32.506 ms | 16.515 ms | 41.943 ms |
| 0 ms | 100/s | 4.719 ms | 5.308 ms | 6.423 ms | 7.930 ms |
| 0 ms | 1,000/s | 6.291 ms | 9.175 ms | 14.025 ms | 18.088 ms |
| 2 ms | saturation | 18.088 ms | 44.564 ms | 20.709 ms | 52.429 ms |
| 2 ms | 100/s | 14.025 ms | 14.942 ms | 15.204 ms | 17.564 ms |
| 2 ms | 1,000/s | 14.942 ms | 22.544 ms | 18.612 ms | 36.700 ms |

Rafter accounting: 0 errors · 0 unknown · 0 unsent
OpenRaft accounting: 0 errors · 0 unknown · 0 unsent

Rafter pipeline + completion priority versus same-code Rafter pipeline FIFO throughput — 0 ms: +17.5% · 2 ms: +2.7%

| Added egress | Offered rate | Rafter pipeline + completion priority p99 | Rafter pipeline FIFO p99 | Rafter pipeline + completion priority p99.9 | Rafter pipeline FIFO p99.9 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 ms | saturation | 12.845 ms | 15.335 ms | 16.515 ms | 19.923 ms |
| 0 ms | 100/s | 4.719 ms | 4.588 ms | 6.423 ms | 6.029 ms |
| 0 ms | 1,000/s | 6.291 ms | 6.750 ms | 14.025 ms | 12.321 ms |
| 2 ms | saturation | 18.088 ms | 19.137 ms | 20.709 ms | 22.282 ms |
| 2 ms | 100/s | 14.025 ms | 14.156 ms | 15.204 ms | 16.122 ms |
| 2 ms | 1,000/s | 14.942 ms | 15.073 ms | 18.612 ms | 21.234 ms |

Useful-capacity objective: every repetition achieves at least 99% of offered load, p99 ≤ 20 ms, p99.9 ≤ 50 ms, and zero errors, unknown outcomes, or unsent requests.

**At 0 ms added egress, Rafter met the declared service objective through at least 1,000 writes/s; the selected OpenRaft control met it through at least 1,000 writes/s.**

| Added egress | Configuration | Highest qualifying rate | Tested through |
| ---: | --- | ---: | ---: |
| 0 ms | Rafter pipeline + completion priority | at least 1,000/s | 1,000/s |
| 0 ms | Rafter pipeline FIFO | at least 1,000/s | 1,000/s |
| 0 ms | OpenRaft async flusher | at least 1,000/s | 1,000/s |
| 0 ms | OpenRaft synchronous | at least 1,000/s | 1,000/s |
| 2 ms | Rafter pipeline + completion priority | at least 1,000/s | 1,000/s |
| 2 ms | Rafter pipeline FIFO | 100/s | 1,000/s |
| 2 ms | OpenRaft async flusher | 100/s | 1,000/s |
| 2 ms | OpenRaft synchronous | 100/s | 1,000/s |

| Added egress | Offered | Engine | Min achieved | Arrival p99/p99.9 | Execution p99/p99.9 | Start-wait p99 | Post-window | Loss | Meets objective |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| 0 ms | 100/s | Rafter | 100/s (100.0%) | 4.915 ms / 6.750 ms | 4.391 ms / 6.029 ms | 1.130 ms | 0 | 0 | yes |
| 0 ms | 100/s | OpenRaft | 100/s (100.0%) | 5.374 ms / 8.192 ms | 4.653 ms / 7.406 ms | 1.081 ms | 0 | 0 | yes |
| 0 ms | 1,000/s | Rafter | 1,000/s (100.0%) | 8.913 ms / 33.030 ms | 8.520 ms / 32.768 ms | 1.081 ms | 8 | 0 | yes |
| 0 ms | 1,000/s | OpenRaft | 1,000/s (100.0%) | 9.306 ms / 20.447 ms | 8.782 ms / 19.923 ms | 1.114 ms | 16 | 0 | yes |
| 2 ms | 100/s | Rafter | 100/s (100.0%) | 14.156 ms / 16.777 ms | 13.631 ms / 15.729 ms | 1.081 ms | 3 | 0 | yes |
| 2 ms | 100/s | OpenRaft | 100/s (100.0%) | 15.204 ms / 18.088 ms | 14.549 ms / 17.039 ms | 1.098 ms | 3 | 0 | yes |
| 2 ms | 1,000/s | Rafter | 1,000/s (100.0%) | 15.073 ms / 19.137 ms | 14.680 ms / 18.874 ms | 1.114 ms | 30 | 0 | yes |
| 2 ms | 1,000/s | OpenRaft | 1,000/s (100.0%) | 22.807 ms / 47.186 ms | 22.282 ms / 46.137 ms | 1.098 ms | 47 | 0 | no |

three processes on one host; real TCP, not three physical hosts; 64 clients; 512-byte writes; medians of 3 repetitions. Success requires Raft commitment and durable application completion. Added delay affects client and peer egress. Storage, codec, and scheduling choices differ between integrations. Selected identities: Rafter 8b3f7e71cb23; OpenRaft synchronous 0.9.24.

Evidence: [Detailed service results](https://github.com/zraftz/benchmarks/actions/runs/34678152454)

## Failure and sustained operation

*Does performance remain useful during failures and over long runs?*

**Finite recovery checks passed; comparative fault and sustained-operation performance remain unmeasured.**

- restart and acknowledged-canary checks: 91 cases passed — finite histories and canaries, not every measured write or power loss
- linearizable qualification histories: 91 cases passed — 64-operation per-case qualification histories
- Rafter recovery smoke: durable-kv passed, follower-catchup passed, leader-loss passed — smoke correctness only; not comparative performance

Visible history:

- Run 34599945967: 3 affected cases, 681 unsent, 0 unknown, 0 errors. Individual cases remain in `summary.json`.
- Run 34673142049: 24 affected cases, 41998 unsent, 0 unknown, 0 errors. Individual cases remain in `summary.json`.

Not yet measured: comparative leader-loss performance; comparative slow-follower performance; storage-stall recovery performance; snapshot/compaction and WAL-reclamation soak performance.

Evidence: [Detailed service results](https://github.com/zraftz/benchmarks/actions/runs/34678152454) · [Historical run 1](https://github.com/zraftz/benchmarks/actions/runs/34599945967) · [Historical run 2](https://github.com/zraftz/benchmarks/actions/runs/34673142049)

---

This is implementation performance, not a claim of a faster Raft algorithm. The report is generated deterministically from normalized evidence; no LLM writes or selects results.

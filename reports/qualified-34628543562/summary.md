# Rafter implementation performance

Where Rafter demonstrably leads, where evidence is mixed, and what remains unmeasured.

## Consensus in memory

*How quickly can the implementation replicate and commit without disks or sockets?*

**Comparison pending qualification.**

Three voters in one process, in-memory stores, no disk synchronization or TCP.

- The suite exists, but no in-memory evidence set was selected for this report.

## Durable replication

*How efficiently does Rafter make consensus state survive a restart?*

**The append-only journal improved throughput 42.5–61.3% over Rafter's file-replacement backend.**

| Batch | File replacement | Append-only journal | Throughput change | Replacement p99 | Journal p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 354/s | 571/s | +61.3% | 4.140 ms | 2.813 ms |
| 8 | 2,785/s | 4,277/s | +53.6% | 4.267 ms | 3.054 ms |
| 32 | 10,562/s | 15,841/s | +50.0% | 4.469 ms | 3.287 ms |
| 128 | 37,011/s | 52,739/s | +42.5% | 4.794 ms | 3.699 ms |

Separate hard-state syscall probe: file replacement 2.0, journal 1.0 sync calls/publication. This is not synchronization calls per committed entry.

One identical Rafter binary, 256-byte proposals, three repetitions; batch size is explicit in every row.

Not yet measured: Combined WAL component comparison.

Evidence: [Storage evidence](https://github.com/zsumz/rafter/actions/runs/34599862151)

## Complete durable service

*How many durable writes can an application complete, and how long do clients wait?*

**Rafter pipeline completed 2.75× as many durable writes per second as the tested OpenRaft integration in the no-delay condition. At no-delay saturation, p99 was 15.8% higher and p99.9 was 37.3% higher while completing that greater load.**

| Added loopback egress | Rafter throughput | OpenRaft throughput | Advantage |
| ---: | ---: | ---: | ---: |
| 0 ms | **10,314 writes/s** | 3,744 writes/s | **2.75×** | 
| 2 ms | **5,532 writes/s** | 2,484 writes/s | **2.23×** | 

| Added egress | Offered rate | Rafter p99 | OpenRaft p99 | Rafter p99.9 | OpenRaft p99.9 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 ms | saturation | 23.069 ms | 19.923 ms | 29.884 ms | 21.758 ms |
| 0 ms | 100/s | 2.490 ms | 2.785 ms | 3.178 ms | 3.244 ms |
| 0 ms | 1,000/s | 2.687 ms | 18.874 ms | 4.456 ms | 22.020 ms |
| 2 ms | saturation | 25.952 ms | 33.030 ms | 30.671 ms | 38.273 ms |
| 2 ms | 100/s | 13.500 ms | 14.025 ms | 14.549 ms | 14.811 ms |
| 2 ms | 1,000/s | 11.141 ms | 18.874 ms | 13.631 ms | 20.185 ms |

Rafter accounting: 0 errors · 0 unknown · 0 unsent
OpenRaft accounting: 0 errors · 0 unknown · 4 unsent

Pipeline versus same-code synchronous Rafter throughput — 0 ms: +17.6% · 2 ms: +3.8%

| Added egress | Offered rate | Pipeline p99 | Synchronous p99 | Pipeline p99.9 | Synchronous p99.9 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 ms | saturation | 23.069 ms | 28.312 ms | 29.884 ms | 34.603 ms |
| 0 ms | 100/s | 2.490 ms | 2.818 ms | 3.178 ms | 3.441 ms |
| 0 ms | 1,000/s | 2.687 ms | 2.753 ms | 4.456 ms | 5.046 ms |
| 2 ms | saturation | 25.952 ms | 29.360 ms | 30.671 ms | 32.506 ms |
| 2 ms | 100/s | 13.500 ms | 13.763 ms | 14.549 ms | 14.942 ms |
| 2 ms | 1,000/s | 11.141 ms | 11.141 ms | 13.631 ms | 13.763 ms |

three processes on one host; real TCP, not three physical hosts; 64 clients; 512-byte writes; medians of 3 repetitions. Success requires Raft commitment and durable application completion. Added delay affects client and peer egress. Storage, codec, and scheduling choices differ between integrations.

Evidence: [Detailed service results](https://github.com/zraftz/benchmarks/actions/runs/34628543562)

## Failure and sustained operation

*Does performance remain useful during failures and over long runs?*

**Finite recovery checks passed; comparative fault and sustained-operation performance remain unmeasured.**

- restart and acknowledged-canary checks: 75 cases passed — finite histories and canaries, not every measured write or power loss
- linearizable qualification histories: 75 cases passed — 64-operation per-case qualification histories
- Rafter recovery smoke: durable-kv passed, follower-catchup passed, leader-loss passed — smoke correctness only; not comparative performance

Visible history:

- Run 34599945967, `008-n0-openraft-r1-q1000-timing`: 14 unsent, 0 unknown, 0 errors.
- Run 34599945967, `018-n0-openraft-r2-q1000-timing`: 100 unsent, 0 unknown, 0 errors.
- Run 34599945967, `025-n0-candidate-b32-r3-q1000-timing`: 567 unsent, 0 unknown, 0 errors.

Not yet measured: comparative leader-loss performance; comparative slow-follower performance; storage-stall recovery performance; snapshot/compaction soak and physical reclamation.

Evidence: [Detailed service results](https://github.com/zraftz/benchmarks/actions/runs/34628543562) · [Historical run 1](https://github.com/zraftz/benchmarks/actions/runs/34599945967)

---

This is implementation performance, not a claim of a faster Raft algorithm. The report is generated deterministically from normalized evidence; no LLM writes or selects results.

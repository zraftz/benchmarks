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

Evidence: [In-memory evidence](https://github.com/zraftz/benchmarks/actions/runs/34654362991)

## Durable replication

*How efficiently does Rafter make consensus state survive a restart?*

**No component evidence selected.**

Not yet measured: Combined WAL component comparison.

## Complete durable service

*How many durable writes can an application complete, and how long do clients wait?*

**No durable-service evidence selected.**

## Failure and sustained operation

*Does performance remain useful during failures and over long runs?*

**Comparative fault performance has not been measured.**

Not yet measured: comparative leader-loss performance; comparative slow-follower performance; storage-stall recovery performance; snapshot/compaction soak and physical reclamation.

---

This is implementation performance, not a claim of a faster Raft algorithm. The report is generated deterministically from normalized evidence; no LLM writes or selects results.

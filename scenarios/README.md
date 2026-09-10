# Implemented scenario names

These are CLI selections, not an unimplemented configuration DSL.

| `--scenario` | Baseline | Timed action |
|---|---|---|
| `durable-kv` | Three durable nodes, configurable Put/Get/CAS mix | No fault |
| `leader-loss` | Same contract and workload | SIGKILL observed leader at ~1/3, restart same data at ~2/3 |
| `follower-catchup` | Same contract and workload | SIGSTOP one follower at ~1/3, SIGCONT at ~2/3 |

Each case independently performs history qualification and simultaneous-process
restart canaries before and after measurement. `--smoke` reduces duration and
concurrency; it does not turn those finite checks into a throughput claim.
Read mix is controlled with `--read-percent`; CAS mix with `--cas-percent`.
The remaining share is Put. The built-in CAS workload is create-if-absent;
the application protocol supports arbitrary expected values.

Load beyond capacity can be explored using `--rates`, with not-issued, unknown,
and queueing evidence retained. There is not yet a dedicated overload-then-drain
scenario or automatic maximum-sustainable-rate search.

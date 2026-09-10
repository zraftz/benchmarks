# Next gates

The sequence matters more than adding names to a scoreboard.

## Gate 1 — real build and adapter qualification

Keep the locked build and real-adapter smoke checks passing in CI. Add
storage-boundary crash tests and stronger session/retry qualification. Obtain
upstream review of the incumbent adapters before performance publication.

## Gate 2 — comparable sustained workloads

Implement snapshot/compaction end to end, including application state and
applied-index recovery. Measure enough cycles to distinguish bounded steady
state from retained-log growth. Add ReadIndex and explicitly separate lease
reads, matching semantics. Add consistent per-entry, per-flush, and batching
counters across implementations. Separate conservative and tuned baselines.

## Gate 3 — physical-host evidence

Automate a reviewed inventory/deployment path, qualification on remote clusters,
controlled fault actions, dedicated resource allocation, generator saturation
checks, and latency-versus-offered-load graphs. Add recovery-to-SLO measurement.
Keep plain TCP and TLS in separately matched lanes. Process-crash testing does
not replace power-loss fault injection.

## Gate 4 — wider coverage

Add etcd-io/raft and HashiCorp Raft with their own idiomatic durable embeddings.
Keep the etcd server as a separate product-level comparison. Add multi-Raft
idle cost, hot-group interference, fairness, and snapshot catch-up workloads.
These adapters and scenarios are not implemented in this repository.

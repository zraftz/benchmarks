# Adapter notes and review checklist

All adapters share the client model, client framing, admission limits, and
persistent application-journal format. They are intentionally explicit about
what they do **not** share. The first review should establish correctness;
upstream-aware tuning comes after qualification passes.

| Adapter | Pinned implementation | Consensus persistence | Peer codec / scheduling |
|---|---|---|---|
| Rafter | Git `518aefd767dc0ef2c1f1ccc5f970c96456cfd109` | Native file stores and `DurableRaftNode` | Native Rafter codec; single-owner actor |
| raft-rs | `raft = 0.7.0`, prost-codec | Synced fixture journal replayed into `MemStorage` | Prost peer messages; single-owner actor |
| OpenRaft | `openraft = 0.9.24`, storage-v2 + serde | Synced fixture journal and in-memory index | JSON serialization of native RPCs; public async API |

The exact Rust dependency manifests are authoritative. `implementations.lock.json`
is a readable inventory, not a substitute for the checked-in Cargo.lock.
OpenRaft is deliberately kept at the historical harness's version in this first
extraction; this is not a claim it is the latest release.

## Rafter

The adapter uses the public durable runtime and native file stores. On restart,
`recover_with_storage_and_snapshot_store_applied_through` is given the recovered
application index. Its recovery outputs are applied explicitly before serving
work. Ordinary commands use `step_batch`. Peer messages are serialized through
`rafter-codec`; the actor does not bypass file persistence.

The application apply index is tracked separately from the core's dispatch
index. No snapshot triggers or membership changes are issued. The initial
staggered election timeout is `50 + 7*(node_id-1)` ticks, with 20 ms ticks.
Tick timing and runtime overload behavior need validation on the target host.

## raft-rs

The adapter owns the Ready loop. It journals entries and hard state, syncs,
updates the in-memory storage view, and only then releases the Ready messages.
Committed entries are durably applied before the actor processes another input.
LightReady commit progress can be reconstructed conservatively from the durable
application index on restart.

This is a simple declared backend, **not TiKV's storage engine or integration**.
Its conservative persist-before-send ordering can forgo supported overlap.
Election ticks use the same configured bases as Rafter, but raft-rs applies its
own randomized election interval. Pre-vote and check-quorum are enabled; the
append budget is 512 KiB. Review defaults and batching before comparing peaks.

## OpenRaft

The adapter uses public `Raft::client_write` for all operations. Log append
callbacks fire only after the fixture journal sync. Committed membership and
last-applied metadata are journaled atomically with application progress. The
first valid log index can be zero and is not confused with “nothing applied.”

Fresh membership is initialized explicitly by the supervisor; restarts do not
reinitialize the cluster. Automatic snapshots are disabled, and unsupported
snapshot/purge calls return errors. Four Tokio workers are configured. Fixture
file operations are blocking inside storage callbacks; this is a conservative
baseline, not an optimized production OpenRaft integration.

## Before publishing a comparison

1. Build with the committed resolved lockfile and run all real-adapter smoke
   scenarios; retain failed cases and inspect every error.
2. Review persistence ordering, restart replay, retry identity, and application
   completion boundaries with each implementation's maintainers or an expert.
3. Separate this conservative baseline from any tuned lane. Record serialization,
   flush grouping, overlap, runtime threads, resource limits, and election settings.
4. Use dedicated hosts, audit the generator headroom, and include losses and
   negative results. Do not promote “our adapter is faster” into an unsupported
   claim about every embedding of the underlying library.

Sources inspected while writing the adapters:

- https://github.com/zsumz/rafter/tree/518aefd767dc0ef2c1f1ccc5f970c96456cfd109
- https://github.com/zsumz/rafter/blob/518aefd767dc0ef2c1f1ccc5f970c96456cfd109/bench-compare/src/bin/bench-raft-rs.rs
- https://github.com/zsumz/rafter/blob/518aefd767dc0ef2c1f1ccc5f970c96456cfd109/bench-compare/src/bin/bench-openraft.rs
- https://github.com/databendlabs/openraft/tree/v0.9.24
- https://docs.rs/raft/0.7.0/raft/raw_node/struct.RawNode.html

This source review is not a substitute for a successful Rust build or cluster test.

# Durable-service contract v1

Identifier: `durable-log+durable-application-v1/logged-reads`.

This is an adapter target backed by finite checks, not a proof that the adapter
implementations satisfy it under every schedule. Rust compilation and real-node
qualification remain required for this archive.

## Topology and persistence

Three static voters; one node process per voter. Each has an exclusive data
folder containing its persistent consensus state and a persistent application
journal. Reusing a folder for a different cluster, voter mapping, node ID, or
implementation is rejected. The default runner uses real loopback TCP and
local files; three folders on one device are not three independent disks.

Consensus storage must obey its library's persistence ordering. Client success
requires commitment and a completed application-journal `sync_data` before the
result is released. The application journal records command identity and apply
index together, reconstructing retry state and application state on restart.
This deliberately includes a second durability boundary beyond the Raft log.
The application journal is **not** presented as a production database backend.

The fixture journal frames JSON records with length and CRC32. New file and
directory creation is synchronized. An incomplete trailing frame is truncated
on replay; a fully present frame with a bad checksum is an error. These rules
are not a claim about device power-loss protection or arbitrary corruption.

## Operations

`Put(key, value)` overwrites one value and returns the resulting value.
`Get(key)` returns the current value or null. `CompareAndSwap(key, expected,
replacement)` compares the current value (including absent/null), conditionally
writes, and returns `swapped` plus the resulting value. There are no multi-key
transactions. Every operation, including Get, is proposed through the log.

One session issues at most one logical operation at a time. Commands carry a
nonempty session identity and a strictly increasing positive sequence number.
Exact retries of the current sequence return its recorded outcome; an older
sequence returns `stale_sequence`; changing the contents under an existing
identity returns `identity_conflict`. Deduplication retains the latest command
and outcome per session, not an unbounded history of each session. Sessions
are not expired in this finite benchmark.

A logical retry can be proposed more than once. The model does not apply the
same accepted command twice. This is a scoped retry contract, not an unqualified
“exactly once” claim for arbitrary external effects.

## Transport and completion

Frames are a big-endian 32-bit byte length followed by JSON on the client link.
Peer links use the same envelope plus an eight-byte sender ID and the adapter's
peer codec. Persistent connections are used. Frames are size limited. Peer IDs
are checked against configured membership, but plaintext identities are **not
authentication**. These binaries belong on isolated trusted benchmark networks.

Client responses distinguish `ok`, `not_leader`, `overloaded`, `unknown`, and
`error`. Only `ok` with a result and no application error is a successful
operation. After an ambiguous transport/API failure, the same command identity
is retried until its client deadline. An exhausted deadline is `unknown`; it
must not be counted as proof of non-commitment.

## Measurement boundaries

Closed loop measures submission-to-result with bounded outstanding requests.
Scheduled rate measures intended-arrival-to-result, including generator/worker
queueing. Requests that cannot enter the bounded generator queue remain
`not_issued` offered work. Network attempts are reported separately from logical
operations. Successful throughput counts successes within the measured window;
draining requests after the window does not inflate its throughput.

Application value bytes, encoded command bytes, Raft entries, and on-disk journal
bytes are different quantities. The current standard report uses logical
operations and value size. Native counters and sampled I/O are retained; it does
not yet normalize Raft entries or flushes across all adapters.

## Qualification

A 64-operation complete history over four keys checks Put/Get/CAS semantics and
real-time ordering. Unknown outcomes, checker timeouts, and search limits return
inconclusive rather than pass. Verification replays this history independently
of the saved verdict. The checker partitions by key only after validating the
single-outstanding-operation session contract.

Before measurement, 12 acknowledged canaries are checked after killing and
restarting all node processes. After measurement, another 12 are written and
all 24 are checked through logged reads after a second restart. These are finite
canary checks; they do not verify every measured write. A process kill leaves
kernel/device caches alive and is not a simulated power cut.

## Unsupported work

No snapshot/compaction, membership changes, multi-Raft, TLS, lease reads, or
ReadIndex benchmark is silently substituted. Logs retain the complete run.
The follower-catch-up scenario therefore means log replay/replication after a
pause, not snapshot installation. Storage exhaustion and oversized runs fail;
they do not imply an implementation cannot support the missing feature.

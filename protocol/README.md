# Retained protocol comparison

`./raft-bench import-protocol` extracts the original `bench-compare` package and
runner from Rafter commit `518aefd767dc0ef2c1f1ccc5f970c96456cfd109`.
All original sources and checked-in result files are preserved. Fifteen known
Git blob IDs are independently checked before import succeeds. An import
receipt hashes every extracted file.

**The upstream source blobs are fetched at setup, not vendored in this repository.**
This is deliberate: there is one source of truth for the preserved harness, and
no vendored copy of the Rafter library. Setup requires network access, Git and
Cargo. The new durable adapters in `adapters/` are included in this repository.

Only eight Rafter Cargo dependency locations are changed to exact Git pins;
the original Cargo manifest and lockfile are retained beside the extraction.
The new lockfile is generated rather than invented. No workload, timer,
allocation instrumentation or aggregation code is rewritten.

```sh
./raft-bench import-protocol
./raft-bench protocol --runs 6
./raft-bench protocol --mode rafter-only --runs 4
```

New results go in immutable `protocol/runs/<id>` directories. The old result
files inside the extracted package are historical input evidence, not results
of the newly built adapter. The wrapper never overwrites them.

The original harness reports different completion boundaries for different
libraries and includes runtime/scheduler overhead differently. Those caveats
remain. In-memory protocol numbers must not be combined with durable service
numbers or described as networked, fsync-matched results.

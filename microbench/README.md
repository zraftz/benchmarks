# In-memory benchmarks

```sh
./raft-bench microbench --runs 7
./raft-bench microbench --mode rafter-only --rafter-ref <branch-or-SHA>
```

`full` compares serial, pipelined, and large-payload workloads across all three
engines. `rafter-only` adds read, codec, service, and MultiRaft probes.

The Rust benchmark sources are maintained here; Rafter itself is a pinned Git
dependency. [UPSTREAM.json](UPSTREAM.json) records the original source revision
and blobs. Raw version strings come from the imported harness; the enclosing
report records the actual selected Rafter commit.

# Contributing

Run `./scripts/check`, `./raft-bench build`, and `./scripts/smoke` before pushing.
CI runs the development checks, real-cluster smoke tests, and both in-memory
modes. Longer runs use the manual **Baseline benchmark** workflow.

Keep both Cargo.lock files checked in. `select-rafter` updates the two workspaces
for local experiments; review the diff before committing a new default pin.
Keep workloads and competitor pins constant while testing a Rafter change.

Builds and results are ignored. CI retains reports, raw results, and resolved
locks as artifacts. To keep local work on a development volume, put the checkout
there and set cache paths before building:

```sh
mkdir -p .cache/tmp .cache/cargo .cache/go-build .cache/go-mod
export TMPDIR="$PWD/.cache/tmp" CARGO_HOME="$PWD/.cache/cargo"
export CARGO_TARGET_DIR="$PWD/target"
export GOCACHE="$PWD/.cache/go-build" GOMODCACHE="$PWD/.cache/go-mod"
```

Use `cargo fmt --all`, `cargo fmt --manifest-path microbench/Cargo.toml`, and
`gofmt -w loadgen/*.go` for formatting. Keep tests separate from implementation.

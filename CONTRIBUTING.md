# Development

Install the tools listed in [README.md](README.md), then run:

```sh
./scripts/check
./raft-bench build
./scripts/smoke
```

Use `cargo fmt --all` and `(cd loadgen && gofmt -w .)` to format changes.
`scripts/test.sh` remains an alias for `scripts/check`. Python integration tests
use a named test fixture; the smoke script always runs the three real adapters.

Keep Cargo.lock checked in. Dependency updates are deliberate: review the
manifest/lockfile diff, update `implementations.lock.json` when implementation
pins change, and rerun the checks above. Do not edit pins merely to improve a
comparison result. The historical importer has its own separate lockfile.

Build products, caches, imported upstream files, and benchmark results are
ignored. Retain experimental evidence outside Git or as CI artifacts. The files
under `validation/` document the original source delivery; they are historical,
not evidence for later changes. Current validation is summarized in
[VALIDATION.md](VALIDATION.md).

Before pushing a new repository, run the full checks and include the root
lockfile. Enable GitHub Actions and require both `Python and Go tooling` and
`Rust and real Raft clusters` in branch protection. Shared CI runners validate
behavior; performance publication needs the review and dedicated-host work in
[the roadmap](docs/ROADMAP.md).

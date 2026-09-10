# Moving the comparison out of zsumz/rafter

This repository does not mutate the upstream repository. The source can be initialized as a
new repository; no remote repository is created by the harness.

## First migration

1. Build and qualify this source, then run `./raft-bench import-protocol`.
   Review `protocol/upstream/IMPORT.json` and the original lockfile/source
   backups. The importer extracts exact Git objects, not whatever is at main.
2. Run both historical modes and compare workload definitions and deterministic
   counters with the old suite. Timing need not match across different hosts.
3. Keep the root Cargo.lock checked in and retain imported source/provenance according
   to the new repository's retention policy. `protocol/upstream/` is ignored by
   default to keep the initial checkout small; force-add its source (not
   targets) or retain it as an immutable release artifact if you want vendoring.
4. Link Rafter's README benchmark section to this repository. Keep the old
   historical results discoverable; do not silently replace them with a new
   version's numbers.

## Keep in Rafter

Protocol/storage correctness tests, TLA+/invariant verification, reference
consumer acceptance tests, deterministic regression counters, and adversarial
transport memory checks should remain authoritative next to the implementation.
The original `bench-compare` package contains both comparisons and Rafter-only
fixtures. Importing it here does not authorize deleting those upstream guards.

In particular, the original benchmark workflow also contains Rafter-specific
verification jobs. Split the comparison jobs deliberately instead of moving
that entire workflow or deleting it wholesale. Preserve
`check-transport-receive-memory` coverage independently of public comparisons.

## Dependencies

All canonical Rafter dependencies in the extracted package are rewritten to the
same exact Git revision. The original local-path Cargo.lock is retained; a new
lockfile is generated because Git dependencies have different source identities.
No local-checkout override mode is currently exposed by the new CLI. A future
one must replace the entire Rafter graph and record dirty state, not override
only the top-level crate.

## Publication

Create the new GitHub repository and push the reviewed source normally. Run its
CI before linking a performance claim. This delivery did not create a remote
repository, push a branch, remove upstream code, or run GitHub Actions.

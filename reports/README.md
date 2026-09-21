# Performance reports

Each directory is an immutable presentation of selected benchmark evidence.
`report.html`, `summary.md`, and `summary.json` come from the same normalized
object. The JSON names the benchmark run and every source case; generator
identity remains separate from benchmarked source identity.

Generate a new directory with `./raft-bench summary`. Do not edit generated
numbers by hand or replace a prior report in place.

- [Qualified service run 35386922113](qualified-35386922113/summary.md) — current durable-service evidence; benchmark `ff5c1253`, Rafter `88d43848`
- [Qualified layered run 34904893568](qualified-34904893568-wal-34907033422/summary.md) — previous service, storage, and WAL-reclamation evidence
- [Qualified layered run 34678152454](qualified-34678152454-wal-34741175806/summary.md) — previous combined evidence
- [Qualified service run 34678152454](qualified-34678152454/summary.md) — previous completion-priority service evidence
- [Qualified in-memory run 34654362991](qualified-in-memory-34654362991/summary.md) — aligned leader-application completion boundary
- [Qualified service run 34628543562](qualified-34628543562/summary.md) — previous exact-main evidence
- [Qualified service run 34609743659](qualified-34609743659/summary.md) — previous evidence

# Results

No new comparative performance results are bundled. Tooling validation logs are
under `validation/`; those are not measurements of Rafter or its competitors.

Local suites are created under `results/runs/<unique-id>` unless `--output` is
specified. Historical in-memory results are imported separately into
`protocol/upstream/bench-compare/results` and are excluded from service reports.

Run directories are immutable per invocation. Re-reporting requires a new output
filename. Raw storage directories live under the explicitly selected data root
and are intentionally retained, not packed into the report or deleted for you.

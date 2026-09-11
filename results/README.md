# Results

Generated reports and raw evidence are ignored by Git. Local commands print their
output paths; GitHub workflows upload them as downloadable artifacts.

Durable runs default to `results/runs/<id>`. In-memory runs default to
`results/microbench/<id>`. Existing result directories are never overwritten.
Data directories are retained separately under the selected data root.

`summary/` contains the concise layered report when at least one primary suite
was produced. Its HTML, Markdown, and JSON are generated from the same normalized
object. Detailed suite reports and individual sealed cases remain alongside it.

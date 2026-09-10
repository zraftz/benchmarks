"""Static HTML, no external scripts, trackers, CDNs or client-side dependencies."""
from __future__ import annotations
from pathlib import Path
from statistics import median
import html
import json
from .evidence import verify


def render(suite: Path, destination: Path) -> None:
    rows = []
    grouped = {}
    for path in sorted(suite.glob("*/manifest.json")):
        case = path.parent
        manifest = json.loads(path.read_text())
        if not (case / "measurement.json").exists():
            rows.append(f"<tr><td>{html.escape(case.name)}</td><td colspan='7'>FAILED — inspect retained logs</td></tr>")
            continue
        result = json.loads((case / "measurement.json").read_text())
        check = verify(case)
        label = "CHECK FAILED" if check["status"] != "passed" else ("SMOKE ONLY" if manifest["smoke"] else "embedding baseline")
        fields = [manifest["implementation"], manifest["scenario"], result["config"]["rate"],
                  round(result["successful_ops_per_second"], 2), round(result["success_latency"]["p99_ms"], 3),
                  result["unknown"], result["not_issued"], label]
        rows.append("<tr>" + "".join(f"<td>{html.escape(str(f))}</td>" for f in fields) + "</tr>")
        if check["status"] == "passed" and not manifest["smoke"] and manifest["implementation"] in ("rafter", "raft-rs", "openraft"):
            key = (manifest["implementation"], manifest["scenario"], result["config"]["rate"], result["config"]["payload_bytes"], result["config"]["concurrency"],result["config"]["read_percent"],result["config"]["cas_percent"])
            grouped.setdefault(key, []).append(result)
    aggregates = []
    for key, values in sorted(grouped.items()):
        rates = [r["successful_ops_per_second"] for r in values]
        tails = [r["success_latency"]["p99_ms"] for r in values]
        aggregates.append(f"<tr><td>{html.escape(str(key))}</td><td>{len(values)}</td><td>{min(rates):.1f}</td><td>{median(rates):.1f}</td><td>{max(rates):.1f}</td><td>{median(tails):.3f}</td></tr>")
    page = """<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>raft-bench evidence</title><style>
body{font:16px/1.55 system-ui,sans-serif;max-width:1240px;margin:48px auto;padding:0 24px;color:#202020;background:#fff}
h1{font-size:36px;letter-spacing:-1px}p{max-width:900px}table{border-collapse:collapse;width:100%;font-size:14px;margin:24px 0}
th,td{text-align:left;padding:12px;border-bottom:1px solid #ddd}th{background:#f5f5f5}.table{overflow:auto}.note{border-left:3px solid #555;padding-left:18px}
</style><h1>raft-bench</h1><p>Durable replicated key/value service: real TCP, file-backed consensus storage, durable application journals.</p>
<p class="note">These results measure declared benchmark embeddings, not isolated libraries. The Rafter adapter uses its native stores; the other adapters use fixture journals. Reads are logged. No TLS, compaction, snapshots, dynamic membership, or upstream-reviewed tuning. A local run uses three processes on one host, not three physical hosts.</p>
<h2>Individual runs</h2><div class="table"><table><thead><tr><th>Implementation</th><th>Scenario</th><th>Offered rate</th><th>Successes/s</th><th>Success p99 ms</th><th>Unknown</th><th>Not issued</th><th>Evidence class</th></tr></thead><tbody>""" + "".join(rows) + """</tbody></table></div>
<p>Offered rate 0 means closed loop. Success latency includes scheduled waiting in open-loop runs. Always inspect unknowns, errors, not-issued counts, generator lateness, and the all-dispatched histogram alongside successful-request percentiles.</p>
<h2>Matched-configuration aggregates</h2><p>Key: implementation, scenario, offered rate, value bytes, concurrency, read %, CAS %. Median p99 means median of per-run p99 values, not a pooled percentile. One run is not repeatability evidence.</p>
<div class="table"><table><thead><tr><th>Configuration</th><th>Runs</th><th>Min ops/s</th><th>Median ops/s</th><th>Max ops/s</th><th>Median p99 ms</th></tr></thead><tbody>""" + "".join(aggregates) + """</tbody></table></div>
<p>Qualification is a finite complete-history check plus process-restart canaries. It is not a proof of Raft correctness or power-loss safety. Archived protocol results are intentionally excluded.</p></html>"""
    with destination.open("x") as stream:
        stream.write(page)

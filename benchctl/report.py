"""Compact durable-service reports with separate waiting and execution latency."""
from pathlib import Path
from statistics import median
import html
import json

from .evidence import verify


def cell(value):
    return f"<td>{html.escape(str(value))}</td>"


def latency(result, name):
    value = result.get(name, {}).get("p99_ms")
    return "—" if value is None else f"{value:.2f}"


def median_latency(results, name):
    values = [r.get(name, {}).get("p99_ms") for r in results]
    return "—" if any(v is None for v in values) else f"{median(values):.2f}"


def render(suite: Path, destination: Path) -> None:
    individual, groups, revisions = [], {}, set()
    for path in sorted(suite.glob("*/manifest.json")):
        case = path.parent
        manifest = json.loads(path.read_text())
        revision = manifest["implementation_pins"]["rafter"]["rev"]
        revisions.add(revision)
        if not (case / "measurement.json").exists():
            individual.append(f"<tr>{cell(case.name)}<td colspan='10'>FAILED — inspect case logs</td></tr>")
            continue
        r = json.loads((case / "measurement.json").read_text())
        checked = verify(case)
        label = "CHECK FAILED" if checked["status"] != "passed" else ("SMOKE ONLY" if manifest["smoke"] else "Passed")
        individual.append("<tr>" + "".join(cell(v) for v in (
            case.name, manifest["scenario"], r["config"]["rate"], f'{r["successful_ops_per_second"]:.1f}',
            latency(r, "success_latency"), latency(r, "success_execution_latency"),
            latency(r, "worker_start_lateness"), r["errors"], r["unknown"], r["not_issued"], label)) + "</tr>")
        if checked["status"] == "passed" and not manifest["smoke"]:
            variant = json.dumps({"backend": (manifest.get("build_receipt") or {}).get("rafter_hard_state_backend", "replace"),
                "peer_batch_size": manifest["options"].get("peer_batch_size", 1),
                "batch_size": manifest["options"].get("batch_size", 64),
                "network_delay_ms": manifest["options"].get("network_delay_ms", 0),
                "ordered_apply": manifest["options"].get("ordered_apply", False),
                "peer_message_stream": manifest["options"].get("peer_message_stream", False),
                "pipelined_durability": manifest["options"].get("pipelined_durability", False),
                "combine_peer_proposals": manifest["options"].get("combine_peer_proposals", False),
                "openraft_async_flush": manifest["options"].get("openraft_async_flush", False),
                "peer_group_commit": (manifest.get("build_receipt") or {}).get("peer_group_commit", False),
                "diagnostics": manifest["options"].get("diagnostics", False)}, sort_keys=True)
            key = (manifest["implementation"], revision, variant, manifest["scenario"], r["config"]["rate"],
                   r["config"]["payload_bytes"], r["config"]["concurrency"],
                   r["config"]["read_percent"], r["config"]["cas_percent"])
            groups.setdefault(key, []).append(r)
    aggregates = []
    for key, values in sorted(groups.items()):
        engine, revision, variant, scenario, rate, payload, concurrency, reads, cas = key
        rates = [v["successful_ops_per_second"] for v in values]
        settings = json.loads(variant)
        mode = "diagnostics" if settings["diagnostics"] else "timing"
        apply = "worker" if settings["ordered_apply"] else "inline/native"
        transport = "messages" if settings["peer_message_stream"] else "RPC"
        if engine == "openraft":
            persistence = ("callback-driven async log flush" if settings["openraft_async_flush"]
                           else "synchronous log flush")
        else:
            persistence = "pipelined" if settings["pipelined_durability"] else "synchronous"
        combine = (("combined ack+proposal" if settings["combine_peer_proposals"] else "separate ack/proposal")
                   + " · " if engine == "rafter" else "")
        detail = f"{persistence} · {settings['backend']} · peers ≤{settings['peer_batch_size']} · {combine}{apply} · {transport} · netem {settings['network_delay_ms']}ms · {mode}"
        config = f"{scenario} · {detail} · {payload} bytes · {concurrency} clients · {reads}% reads · {cas}% CAS"
        fields = (engine, revision[:12], config, rate, len(values), f"{median(rates):.1f}",
                  f"{min(rates):.1f}–{max(rates):.1f}", median_latency(values, "success_latency"),
                  median_latency(values, "success_execution_latency"), median_latency(values, "worker_start_lateness"),
                  sum(r["errors"] for r in values), sum(r["unknown"] for r in values), sum(r["not_issued"] for r in values))
        aggregates.append("<tr>" + "".join(cell(v) for v in fields) + "</tr>")
    pins = ", ".join(html.escape(r) for r in sorted(revisions))
    page = '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Durable service benchmarks</title><style>
body{font:16px/1.5 system-ui,sans-serif;margin:48px auto;padding:0 24px;color:#202020;max-width:1600px}
h1{font-size:32px}table{border-collapse:collapse;width:100%;font-size:14px}th,td{text-align:left;padding:12px;border-bottom:1px solid #ddd}th{background:#f5f5f5}.table{overflow:auto}code{overflow-wrap:anywhere}details{margin:24px 0}p{max-width:1000px}
</style><h1>Durable service benchmarks</h1><p>Three processes, TCP, synced consensus and application storage. Rafter revision: <code>''' + pins + '''</code></p>
<h2>Repeated runs</h2><div class="table"><table><thead><tr><th>Engine</th><th>Rafter SHA</th><th>Configuration</th><th>Offered/s</th><th>Runs</th><th>Median ops/s</th><th>Min–max</th><th>Arrival p99 ms</th><th>Execution p99 ms</th><th>Client-start delay p99 ms</th><th>Errors</th><th>Unknown</th><th>Unsent</th></tr></thead><tbody>''' + "".join(aggregates) + '''</tbody></table></div>
<p>Rate 0 means closed loop. Latency columns are medians of per-run p99 values. Arrival latency includes waiting; execution starts when the client begins its request. Client-start delay includes all dispatched operations. Older reports without execution measurements show —.</p>
<details><summary>Individual runs and checks</summary><div class="table"><table><thead><tr><th>Case</th><th>Scenario</th><th>Offered/s</th><th>Successes/s</th><th>Arrival p99 ms</th><th>Execution p99 ms</th><th>Client-start delay p99 ms</th><th>Errors</th><th>Unknown</th><th>Unsent</th><th>Checks</th></tr></thead><tbody>''' + "".join(individual) + '''</tbody></table></div></details>
<p>Unsent requests never entered the client queue. Unknown requests were dispatched but their outcome could not be confirmed. Inspect both alongside throughput. Checks cover finite histories and restart canaries. Storage and integration choices differ; shared-runner results are an initial reference.</p></html>'''
    with destination.open("x") as stream:
        stream.write(page)

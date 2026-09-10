"""Small comparison tables; original per-process reports remain the evidence."""
import html
import json
import math
from pathlib import Path
from statistics import median


def summarize(runs: list[dict]) -> list[dict]:
    groups = {}
    expected = None
    for run in runs:
        keys = set()
        for report in run["results"]:
            for workload in report["workloads"]:
                key = (report["library"], workload["name"])
                if key in keys:
                    raise ValueError(f"duplicate microbenchmark workload: {key}")
                keys.add(key)
                values = (workload["proposals_per_s"], workload["elapsed_ms"],
                          workload["commit_latency_us"]["p50"], workload["commit_latency_us"]["p99"])
                if any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in values):
                    raise ValueError("invalid microbenchmark measurement")
                if workload["proposals"] <= 0 or workload["elapsed_ms"] <= 0:
                    raise ValueError("empty microbenchmark workload")
                groups.setdefault(key, []).append((report, workload))
        if not keys or (expected is not None and keys != expected):
            raise ValueError("microbenchmark workload set changed between repetitions")
        expected = keys
    if not groups:
        raise ValueError("no microbenchmark results")
    rows = []
    for (library, workload), samples in sorted(groups.items()):
        for field in ("proposals", "payload_bytes", "max_in_flight"):
            if len({s[field] for _, s in samples}) != 1:
                raise ValueError(f"microbenchmark configuration changed: {field}")
        definitions = {r["commit_latency_definition"] for r, _ in samples}
        if len(definitions) != 1:
            raise ValueError("microbenchmark completion boundary changed")
        rates = [w["proposals_per_s"] for _, w in samples]
        rows.append({"library": library, "workload": workload, "runs": len(samples),
                     "proposals_per_s": {"min": min(rates), "median": median(rates), "max": max(rates)},
                     "p50_us": median(w["commit_latency_us"]["p50"] for _, w in samples),
                     "p99_us": median(w["commit_latency_us"]["p99"] for _, w in samples),
                     "completion": definitions.pop(),
                     **{k: samples[0][1][k] for k in ("proposals", "payload_bytes", "max_in_flight")}})
    return rows


def render(directory: Path, summary: dict) -> None:
    rows = []
    for row in summary["results"]:
        rates = row["proposals_per_s"]
        fields = [row["library"], row["workload"], row["runs"], f'{rates["median"]:,.0f}',
                  f'{rates["min"]:,.0f}–{rates["max"]:,.0f}', f'{row["p99_us"]:,.2f}']
        rows.append("<tr>" + "".join(f"<td>{html.escape(str(v))}</td>" for v in fields) + "</tr>")
    revision = html.escape(summary["rafter"]["rev"])
    definitions = "".join(f'<li>{html.escape(r["library"])}: {html.escape(r["completion"])}</li>'
                          for r in summary["results"] if r["workload"] == "serial")
    page = f'''<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>In-memory benchmarks</title>
<style>body{{font:16px/1.5 system-ui;max-width:1100px;margin:48px auto;padding:0 24px}}table{{border-collapse:collapse;width:100%}}td,th{{padding:12px;text-align:left;border-bottom:1px solid #ddd}}code{{overflow-wrap:anywhere}}</style>
<h1>In-memory benchmarks</h1><p>Rafter <code>{revision}</code></p>
<p>Three voters in one process. Memory storage; no disk syncs or network sockets. Values are medians across process runs.</p>
<table><tr><th>Engine</th><th>Workload</th><th>Runs</th><th>Proposals/s</th><th>Min–max</th><th>p99 µs</th></tr>{''.join(rows)}</table>
<ul>{definitions}</ul><p><a href="report.json">JSON results</a> · <a href="provenance.json">Build and host</a></p></html>'''
    (directory / "report.html").write_text(page)


def verify_report(directory: Path) -> None:
    provenance = json.loads((directory / "provenance.json").read_text())
    if provenance["kind"] != "in-memory" or (directory / "failure.json").exists():
        raise ValueError("microbenchmark run failed or has an unknown kind")
    if json.loads((directory / "completion.json").read_text())["status"] != "passed":
        raise ValueError("microbenchmark run is incomplete")
    report = json.loads((directory / "report.json").read_text())
    if report["rafter"] != provenance["rafter"] or len(report["runs"]) != provenance["runs"]:
        raise ValueError("microbenchmark provenance mismatch")
    from .microbench import MODES
    binaries = MODES[provenance["mode"]]
    if set(provenance["binaries"]) != set(binaries):
        raise ValueError("microbenchmark binary set does not match its mode")
    replay = []
    for index, run in enumerate(report["runs"]):
        order = list(binaries[index % len(binaries):] + binaries[:index % len(binaries)])
        if type(run["run"]) is not int or run["run"] != index + 1 or run["execution_order"] != order:
            raise ValueError("microbenchmark repetition order changed")
        reports = []
        for binary in run["execution_order"]:
            if binary not in provenance["binaries"]:
                raise ValueError("unknown benchmark binary")
            reports.append(json.loads((directory / f'run-{run["run"]}-{binary}.json').read_text()))
        replay.append({**run, "results": reports})
    if report["results"] != summarize(replay) or report["runs"] != replay:
        raise ValueError("microbenchmark aggregate does not match raw reports")

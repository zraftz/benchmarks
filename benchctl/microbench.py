"""Run the repository-owned in-memory suite against the selected libraries."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid

from .evidence import ROOT, capture, digest, host_info, seal, source_digest, write_json
from .micro_report import render, summarize
from .selection import check_resolution

MODES = {
    "full": ("bench-rafter", "bench-raft-rs", "bench-openraft"),
    "rafter-only": ("bench-rafter", "bench-rafter-service", "bench-rafter-codec", "bench-rafter-multiraft"),
}


def run_microbench(mode: str, runs: int, output: Path | None = None) -> Path:
    if mode not in MODES or not 1 <= runs <= 100:
        raise ValueError("microbenchmark mode must be full or rafter-only; runs must be 1..100")
    if os.environ.get("CARGO_BUILD_TARGET"):
        raise ValueError("microbenchmarks need a native build; unset CARGO_BUILD_TARGET")
    directory = output.resolve() if output else ROOT / "results/microbench" / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8])
    directory.mkdir(parents=True, exist_ok=False)
    pins = json.loads((ROOT / "implementations.lock.json").read_text())
    check_resolution(ROOT, pins["rafter"]["rev"])
    manifest = ROOT / "microbench/Cargo.toml"
    binaries = MODES[mode]
    provenance = {"schema": 1, "kind": "in-memory", "rafter": pins["rafter"],
                  "mode": mode, "runs": runs, "source_digest": source_digest(),
                  "upstream": json.loads((ROOT / "microbench/UPSTREAM.json").read_text()),
                  "host": host_info(directory), "compiler": capture(["rustc", "-Vv"])}
    raw = []
    try:
        command = ["cargo", "build", "--release", "--locked", "--manifest-path", str(manifest)]
        if mode == "rafter-only":
            command.append("--no-default-features")
        for binary in binaries:
            command += ["--bin", binary]
        build_env = dict(os.environ)
        build_env["RAFTER_BENCH_REV"] = pins["rafter"]["rev"]
        with (directory / "build.log").open("x") as log:
            subprocess.run(command, cwd=ROOT, env=build_env, stdout=log,
                           stderr=subprocess.STDOUT, check=True)
        metadata = json.loads(subprocess.check_output(
            ["cargo", "metadata", "--no-deps", "--locked", "--format-version", "1", "--manifest-path", str(manifest)], cwd=ROOT))
        release = Path(metadata["target_directory"]) / "release"
        staged = ROOT / "dist/microbench"
        staged.mkdir(parents=True, exist_ok=True)
        for binary in binaries:
            shutil.copy2(release / binary, staged / binary)
        provenance["binaries"] = {name: digest(staged / name) for name in binaries}
        provenance["cargo_lock_sha256"] = digest(ROOT / "microbench/Cargo.lock")
        shutil.copy2(ROOT / "microbench/Cargo.lock", directory / "Cargo.lock")
        write_json(directory / "provenance.json", provenance)
        for repeat in range(runs):
            order = binaries[repeat % len(binaries):] + binaries[:repeat % len(binaries)]
            reports = []
            for binary in order:
                print(f"Microbench {repeat + 1}/{runs}: {binary}", flush=True)
                env = dict(os.environ)
                # Parent-shell settings must not silently change the workload set.
                env.pop("BENCH_RAFTER_EXTRA_WORKLOADS", None)
                if mode == "rafter-only" and binary == "bench-rafter":
                    env["BENCH_RAFTER_EXTRA_WORKLOADS"] = "1"
                name = f"run-{repeat + 1}-{binary}"
                with (directory / f"{name}.json").open("x") as out, (directory / f"{name}.log").open("x") as log:
                    subprocess.run([str(staged / binary)], cwd=ROOT, env=env, stdout=out,
                                   stderr=log, check=True, timeout=600)
                reports.append(json.loads((directory / f"{name}.json").read_text()))
            raw.append({"run": repeat + 1, "execution_order": list(order), "results": reports})
        summary = {"schema": 1, "kind": "in-memory", "rafter": pins["rafter"],
                   "aggregation": "median of isolated process runs", "results": summarize(raw), "runs": raw}
        write_json(directory / "report.json", summary)
        render(directory, summary)
        write_json(directory / "completion.json", {"status": "passed"})
    except Exception as exc:
        if not (directory / "provenance.json").exists():
            write_json(directory / "provenance.json", provenance)
        write_json(directory / "failure.json", {"status": "failed", "error": str(exc)})
        raise
    finally:
        seal(directory)
    print(f"Microbenchmark report: {directory / 'report.html'}")
    return directory

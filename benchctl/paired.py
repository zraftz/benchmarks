"""Same-host prior/candidate/control runs with independently hashed build receipts."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import shutil
from types import SimpleNamespace
import uuid

from .build import build
from .evidence import ROOT, digest, verify, write_json
from .report import render
from .runner import run_case
from .selection import select_rafter


def archive_build(destination: Path) -> dict:
    receipt = json.loads((ROOT / "dist/build.json").read_text())
    destination.mkdir(parents=True, exist_ok=False)
    for engine, expected in receipt["binaries"].items():
        binary = ROOT / "dist" / f"raft-bench-{engine}"
        if digest(binary) != expected:
            raise RuntimeError("cannot archive a binary that differs from its build receipt")
        shutil.copy2(binary, destination / binary.name)
    write_json(destination / "build.json", receipt)
    return receipt


def ordered_arms(arms: list, repeat: int) -> list:
    rotated = arms[repeat % len(arms):] + arms[:repeat % len(arms)]
    return rotated if repeat % 2 == 0 else list(reversed(rotated))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prior", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--peer-batch-sizes", default="8,16,32,64")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    args = parser.parse_args()
    caps = [int(value) for value in args.peer_batch_sizes.split(",")]
    if not caps or len(set(caps)) != len(caps) or any(cap < 1 or cap > 64 for cap in caps):
        raise ValueError("distinct peer batch sizes in 1..64 are required")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    work = ROOT / ".cache/paired" / uuid.uuid4().hex
    data = args.data_root.resolve() / work.name
    select_rafter(args.prior)
    build("journal")
    prior = archive_build(work / "prior")
    write_json(output / "prior-build.json", prior)
    select_rafter(args.candidate)
    build("journal", peer_group_commit=True)
    candidate = archive_build(work / "candidate")
    write_json(output / "candidate-build.json", candidate)
    if prior["loadgen"] != candidate["loadgen"] or digest(ROOT / "dist/raft-bench-load") != candidate["loadgen"]:
        raise RuntimeError("paired runs require the identical load generator")
    for engine in ("raft-rs", "openraft"):
        if prior["implementations"][engine] != candidate["implementations"][engine]:
            raise RuntimeError("control implementation changed while selecting Rafter")
    arms = [("prior", "rafter", 1, "prior"), ("openraft", "openraft", 1, "candidate")]
    arms += [(f"candidate-b{cap}", "rafter", cap, "candidate") for cap in caps]
    write_json(output / "suite.json", {"schema": 1, "arms": arms, "rates": [0, 100, 1000], "runs": 3,
        "order": "rotate and reverse arms by repetition; all cases sequential on one host",
        "diagnostics": "separate cases after timing runs; never pooled", "data_root": str(data)})
    failures = []
    ordinal = 0
    for diagnostic in (False, True):
        diagnostic_arms = [arms[0], arms[1], next((arm for arm in arms[2:] if arm[2] == 32), arms[-1])]
        for repeat in range(1 if diagnostic else 3):
            for rate in ([0, 1000] if diagnostic else [0, 100, 1000]):
                for label, engine, cap, binary_set in ordered_arms(diagnostic_arms if diagnostic else arms, repeat):
                    ordinal += 1
                    name = f"{ordinal:03d}-{label}-r{repeat+1}-q{rate}-{'trace' if diagnostic else 'timing'}"
                    options = SimpleNamespace(scenario="durable-kv", smoke=False, batch_size=64,
                        peer_batch_size=cap, diagnostics=diagnostic, duration=60, warmup=10,
                        concurrency=64, payload=512, rate=rate, keyspace=10000, seed=repeat+1,
                        read_percent=0, cas_percent=0, timeout=2, variant=label)
                    print(f"Running {name}", flush=True)
                    try:
                        run_case(engine, output / name, data / name, options,
                            command=[str(work / binary_set / f"raft-bench-{engine}")],
                            build_receipt=prior if binary_set == "prior" else candidate)
                        checked = verify(output / name)
                        if checked["status"] != "passed":
                            raise RuntimeError(f"evidence verification failed: {checked}")
                    except Exception as error:
                        failures.append({"case": name, "error": str(error)})
                        print(f"FAILED {name}: {error}", flush=True)
    write_json(output / "completion.json", {"failures": failures})
    render(output, output / "report.html")
    if failures:
        raise RuntimeError(f"{len(failures)} cases failed; evidence retained")


if __name__ == "__main__":
    main()

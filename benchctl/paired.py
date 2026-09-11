"""Same-host prior/candidate/control runs with independently hashed build receipts."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace
import uuid

from .build import build
from .evidence import ROOT, digest, verify, write_json
from .report import render
from .runner import run_case
from .selection import select_rafter
from .network import loopback_delay


MODES = ("inline", "worker", "messages", "pipeline")


def mode_flags(mode: str, engine: str = "rafter") -> dict[str, bool]:
    if mode not in MODES:
        raise ValueError("unsupported embedding mode")
    return {"ordered_apply": mode != "inline" and engine == "rafter",
            "peer_message_stream": mode in ("messages", "pipeline") and engine == "rafter",
            "pipelined_durability": mode == "pipeline" and engine == "rafter"}


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
    parser.add_argument("--prior-hard-state", choices=("replace", "journal", "wal"), default="journal")
    parser.add_argument("--candidate-hard-state", choices=("replace", "journal", "wal"), default="journal")
    parser.add_argument("--peer-batch-sizes", default="8,16,32,64")
    parser.add_argument("--network-delays-ms", default="0", help="explicit Linux loopback netem delays; 0 leaves networking unchanged")
    parser.add_argument("--prior-mode", choices=MODES, default="inline")
    parser.add_argument("--candidate-mode", choices=MODES, default="inline")
    parser.add_argument("--prior-peer-batch-size", type=int, default=1)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    args = parser.parse_args()
    caps = [int(value) for value in args.peer_batch_sizes.split(",")]
    if not caps or len(set(caps)) != len(caps) or any(cap < 1 or cap > 64 for cap in caps):
        raise ValueError("distinct peer batch sizes in 1..64 are required")
    if not 1 <= args.prior_peer_batch_size <= 64:
        raise ValueError("prior peer batch size must be 1..64")
    delays = [int(value) for value in args.network_delays_ms.split(",")]
    if not delays or len(set(delays)) != len(delays) or any(value < 0 or value > 100 for value in delays):
        raise ValueError("distinct network delays in 0..100 are required")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    work = ROOT / ".cache/paired" / uuid.uuid4().hex
    data = args.data_root.resolve() / work.name
    select_rafter(args.prior)
    build(args.prior_hard_state, peer_group_commit=args.prior_peer_batch_size > 1, ordered_apply=args.prior_mode != "inline", pipelined_durability=args.prior_mode == "pipeline")
    prior = archive_build(work / "prior")
    write_json(output / "prior-build.json", prior)
    select_rafter(args.candidate)
    build(args.candidate_hard_state, peer_group_commit=True, ordered_apply=args.candidate_mode != "inline", pipelined_durability=args.candidate_mode == "pipeline")
    candidate = archive_build(work / "candidate")
    write_json(output / "candidate-build.json", candidate)
    if args.candidate_mode != "inline":
        scenarios = ("durable-kv", "leader-loss", "follower-catchup") if args.candidate_mode in ("messages", "pipeline") else ("durable-kv",)
        for scenario in scenarios:
            flags = ["--peer-message-stream"] if args.candidate_mode in ("messages", "pipeline") else []
            engines = "rafter,raft-rs"
            if args.candidate_mode == "pipeline":
                flags.append("--pipelined-durability")
                engines = "rafter"
            subprocess.run([sys.executable, str(ROOT / "raft-bench"), "run", "--smoke",
                "--implementations", engines, "--ordered-apply", "--peer-batch-size", "32",
                "--scenario", scenario, *flags,
                "--output", str(output / f"worker-smoke-{scenario}"), "--data-root", str(data / "smoke")],
                cwd=ROOT, check=True)
    if prior["loadgen"] != candidate["loadgen"] or digest(ROOT / "dist/raft-bench-load") != candidate["loadgen"]:
        raise RuntimeError("paired runs require the identical load generator")
    for engine in ("raft-rs", "openraft"):
        if prior["implementations"][engine] != candidate["implementations"][engine]:
            raise RuntimeError("control implementation changed while selecting Rafter")
    arms = [("prior", "rafter", args.prior_peer_batch_size, "prior"), ("openraft", "openraft", 1, "candidate")]
    arms += [(f"candidate-b{cap}", "rafter", cap, "candidate") for cap in caps]
    write_json(output / "suite.json", {"schema": 1, "arms": arms, "rates": [0, 100, 1000], "runs": 3,
        "order": "rotate and reverse arms by repetition; all cases sequential on one host",
        "diagnostics": "separate cases after timing runs; never pooled", "network_delays_ms": delays, "prior_mode": args.prior_mode, "candidate_mode": args.candidate_mode, "prior_hard_state": args.prior_hard_state, "candidate_hard_state": args.candidate_hard_state, "data_root": str(data)})
    failures = []
    ordinal = 0
    for delay in delays:
        with loopback_delay(delay) as network:
            write_json(output / f"network-{delay}ms.json", network)
            for diagnostic in (False, True):
                diagnostic_arms = [arms[0], arms[1], next((arm for arm in arms[2:] if arm[2] == 32), arms[-1])]
                for repeat in range(1 if diagnostic else 3):
                    for rate in (0, 100, 1000):
                        for label, engine, cap, binary_set in ordered_arms(diagnostic_arms if diagnostic else arms, repeat):
                            ordinal += 1
                            name = f"{ordinal:03d}-n{delay}-{label}-r{repeat+1}-q{rate}-{'trace' if diagnostic else 'timing'}"
                            options = SimpleNamespace(network_delay_ms=delay, scenario="durable-kv", smoke=False, batch_size=64,
                                peer_batch_size=cap, diagnostics=diagnostic, duration=60, warmup=10,
                                **mode_flags(args.prior_mode if binary_set == "prior" else args.candidate_mode, engine),
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
                                # Completed evidence retains configs/logs/checksums; only the generated
                                # live node stores are reclaimed, so a long sweep fits runner storage.
                                shutil.rmtree(data / name)
                            except Exception as error:
                                failures.append({"case": name, "error": str(error)})
                                print(f"FAILED {name}: {error}", flush=True)
    write_json(output / "completion.json", {"failures": failures})
    render(output, output / "report.html")
    if failures:
        raise RuntimeError(f"{len(failures)} cases failed; evidence retained")


if __name__ == "__main__":
    main()

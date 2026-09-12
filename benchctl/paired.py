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
OPENRAFT_CONTROLS = ("synchronous", "async")


def mode_flags(mode: str, engine: str = "rafter") -> dict[str, bool]:
    if mode not in MODES:
        raise ValueError("unsupported embedding mode")
    return {"ordered_apply": mode != "inline" and engine == "rafter",
            "peer_message_stream": mode in ("messages", "pipeline") and engine == "rafter",
            "pipelined_durability": mode == "pipeline" and engine == "rafter"}


def openraft_arms(controls: list[str]) -> list[tuple[str, str, int, str, int, bool, int]]:
    if not controls or len(set(controls)) != len(controls) or any(
            control not in OPENRAFT_CONTROLS for control in controls):
        raise ValueError("distinct OpenRaft controls drawn from synchronous,async are required")
    labels = {"synchronous": "openraft", "async": "openraft-async"}
    return [(labels[control], "openraft", 1, "candidate", 1, False, 8) for control in controls]


def candidate_arms(caps: list[int], thresholds: list[int], windows: list[int],
                   combine: bool, durable_completion_priority: bool = False,
                   ) -> list[tuple[str, str, int, str, int, bool, int]]:
    def label(cap: int, threshold: int, window: int) -> str:
        result = f"candidate-b{cap}" if thresholds == [1] else f"candidate-b{cap}-s{threshold}"
        if combine:
            result += "-combined"
        if durable_completion_priority:
            result += "-commit-first"
        if windows != [8]:
            result += f"-w{window}"
        return result

    return [(label(cap, threshold, window), "rafter", cap, "candidate", threshold, combine, window)
            for cap in caps for threshold in thresholds for window in windows]


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


def combined_activation_failures(output: Path, arms: list[tuple]) -> list[dict]:
    required = {arm[0] for arm in arms if arm[5]}
    observed = set()
    for manifest_path in output.glob("*/manifest.json"):
        case = manifest_path.parent
        if (case / "failure.json").exists() or not (case / "outcome.json").exists():
            continue
        manifest = json.loads(manifest_path.read_text())
        variant = manifest.get("options", {}).get("variant")
        if variant not in required or not (case / "pipeline-activity.json").exists():
            continue
        receipt = json.loads((case / "pipeline-activity.json").read_text())
        combined = receipt.get("combined_peer_proposal_batches_by_node")
        if isinstance(combined, dict) and sum(combined.values()) > 0:
            observed.add(variant)
    return [{"case": f"suite:{variant}",
             "error": "selected combined peer/proposal mode executed no combined steps in any completed case"}
            for variant in sorted(required - observed)]


def completion_priority_activation_failures(output: Path, variants: set[str]) -> list[dict]:
    observed = set()
    for manifest_path in output.glob("*/manifest.json"):
        case = manifest_path.parent
        if (case / "failure.json").exists() or not (case / "outcome.json").exists():
            continue
        manifest = json.loads(manifest_path.read_text())
        variant = manifest.get("options", {}).get("variant")
        receipt_path = case / "completion-priority-activity.json"
        if variant in variants and receipt_path.exists() and json.loads(
                receipt_path.read_text()).get("observed_during_load") is True:
            observed.add(variant)
    return [{"case": f"suite:{variant}",
             "error": "selected durable completion priority executed no prioritized work in any completed case"}
            for variant in sorted(variants - observed)]


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
    parser.add_argument("--prior-max-speculative-proposals", type=int, default=1)
    parser.add_argument("--candidate-max-speculative-proposals", default="1")
    parser.add_argument("--prior-max-inflight-appends", type=int, default=8)
    parser.add_argument("--candidate-max-inflight-appends", default="8")
    parser.add_argument("--prior-combine-peer-proposals", action="store_true")
    parser.add_argument("--candidate-combine-peer-proposals", action="store_true")
    parser.add_argument("--prior-durable-completion-priority", action="store_true")
    parser.add_argument("--candidate-durable-completion-priority", action="store_true")
    parser.add_argument("--rates", default="0,100,1000")
    parser.add_argument("--diagnostic-rates", help="defaults to all timing rates")
    parser.add_argument("--openraft-controls", default="synchronous",
                        help="comma-separated OpenRaft controls: synchronous,async")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    args = parser.parse_args()
    caps = [int(value) for value in args.peer_batch_sizes.split(",")]
    if not caps or len(set(caps)) != len(caps) or any(cap < 1 or cap > 64 for cap in caps):
        raise ValueError("distinct peer batch sizes in 1..64 are required")
    if not 1 <= args.prior_peer_batch_size <= 64:
        raise ValueError("prior peer batch size must be 1..64")
    thresholds = [int(value) for value in args.candidate_max_speculative_proposals.split(",")]
    if (not thresholds or len(set(thresholds)) != len(thresholds)
            or any(value < 1 or value > 64 for value in thresholds)):
        raise ValueError("distinct candidate speculative proposal limits in 1..64 are required")
    if not 1 <= args.prior_max_speculative_proposals <= 64:
        raise ValueError("prior speculative proposal limit must be 1..64")
    windows = [int(value) for value in args.candidate_max_inflight_appends.split(",")]
    if (not windows or len(set(windows)) != len(windows)
            or any(value < 1 or value > 64 for value in windows)):
        raise ValueError("distinct candidate inflight append limits in 1..64 are required")
    if not 1 <= args.prior_max_inflight_appends <= 64:
        raise ValueError("prior inflight append limit must be 1..64")
    if args.prior_combine_peer_proposals and args.prior_mode != "pipeline":
        raise ValueError("prior combined peer/proposal steps require pipeline mode")
    if args.candidate_combine_peer_proposals and args.candidate_mode != "pipeline":
        raise ValueError("candidate combined peer/proposal steps require pipeline mode")
    if args.prior_durable_completion_priority and args.prior_mode != "pipeline":
        raise ValueError("prior durable completion priority requires pipeline mode")
    if args.candidate_durable_completion_priority and args.candidate_mode != "pipeline":
        raise ValueError("candidate durable completion priority requires pipeline mode")
    if args.prior_durable_completion_priority and args.prior_combine_peer_proposals:
        raise ValueError("prior arm must select only one mixed-input pipeline policy")
    if args.candidate_durable_completion_priority and args.candidate_combine_peer_proposals:
        raise ValueError("candidate arms must select only one mixed-input pipeline policy")
    rates = [int(value) for value in args.rates.split(",")]
    if not rates or len(set(rates)) != len(rates) or any(value < 0 or value > 10_000_000 for value in rates):
        raise ValueError("distinct offered rates in 0..10000000 are required")
    diagnostic_rates = ([int(value) for value in args.diagnostic_rates.split(",")]
                        if args.diagnostic_rates else rates)
    if (not diagnostic_rates or len(set(diagnostic_rates)) != len(diagnostic_rates)
            or any(value not in rates for value in diagnostic_rates)):
        raise ValueError("diagnostic rates must be distinct members of the timing rates")
    controls = args.openraft_controls.split(",")
    control_arms = openraft_arms(controls)
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
                flags.extend(["--pipelined-durability", "--max-speculative-proposals", str(thresholds[0]),
                              "--max-inflight-appends", str(windows[0])])
                if args.candidate_combine_peer_proposals:
                    flags.append("--combine-peer-proposals")
                if args.candidate_durable_completion_priority:
                    flags.append("--durable-completion-priority")
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
    arms = [("prior", "rafter", args.prior_peer_batch_size, "prior",
             args.prior_max_speculative_proposals, args.prior_combine_peer_proposals,
             args.prior_max_inflight_appends),
            *control_arms]
    candidates = candidate_arms(caps, thresholds, windows, args.candidate_combine_peer_proposals,
                                args.candidate_durable_completion_priority)
    arms += candidates
    diagnostic_arms = (arms if len(thresholds) > 1 or len(windows) > 1 else
                       [arms[0], *control_arms,
                        next((arm for arm in candidates if arm[2] == 32), candidates[-1])])
    write_json(output / "suite.json", {"schema": 2, "arms": arms, "rates": rates, "runs": 3,
        "order": "rotate and reverse arms by repetition; all cases sequential on one host",
        "diagnostics": "separate cases after timing runs; never pooled",
        "diagnostic_arms": [arm[0] for arm in diagnostic_arms], "diagnostic_rates": diagnostic_rates,
        "network_delays_ms": delays, "prior_mode": args.prior_mode, "candidate_mode": args.candidate_mode,
        "prior_hard_state": args.prior_hard_state, "candidate_hard_state": args.candidate_hard_state,
        "openraft_controls": controls,
        "prior_max_speculative_proposals": args.prior_max_speculative_proposals,
        "candidate_max_speculative_proposals": thresholds,
        "prior_max_inflight_appends": args.prior_max_inflight_appends,
        "candidate_max_inflight_appends": windows,
        "prior_combine_peer_proposals": args.prior_combine_peer_proposals,
        "candidate_combine_peer_proposals": args.candidate_combine_peer_proposals,
        "prior_durable_completion_priority": args.prior_durable_completion_priority,
        "candidate_durable_completion_priority": args.candidate_durable_completion_priority,
        "data_root": str(data)})
    failures = []
    ordinal = 0
    for delay in delays:
        with loopback_delay(delay) as network:
            write_json(output / f"network-{delay}ms.json", network)
            for diagnostic in (False, True):
                for repeat in range(1 if diagnostic else 3):
                    for rate in diagnostic_rates if diagnostic else rates:
                        for arm_label, engine, cap, binary_set, threshold, combine, window in ordered_arms(diagnostic_arms if diagnostic else arms, repeat):
                            ordinal += 1
                            name = f"{ordinal:03d}-n{delay}-{arm_label}-r{repeat+1}-q{rate}-{'trace' if diagnostic else 'timing'}"
                            options = SimpleNamespace(network_delay_ms=delay, scenario="durable-kv", smoke=False, batch_size=64,
                                peer_batch_size=cap, diagnostics=diagnostic, duration=60, warmup=10,
                                **mode_flags(args.prior_mode if binary_set == "prior" else args.candidate_mode, engine),
                                openraft_async_flush=arm_label == "openraft-async",
                                max_speculative_proposals=threshold,
                                max_inflight_appends=window,
                                combine_peer_proposals=combine,
                                durable_completion_priority=(engine == "rafter" and (
                                    args.prior_durable_completion_priority if binary_set == "prior"
                                    else args.candidate_durable_completion_priority)),
                                concurrency=64, payload=512, rate=rate, keyspace=10000, seed=repeat+1,
                                read_percent=0, cas_percent=0, timeout=2, variant=arm_label)
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
    failures.extend(combined_activation_failures(output, arms))
    priority_variants = set()
    if args.prior_durable_completion_priority:
        priority_variants.add("prior")
    if args.candidate_durable_completion_priority:
        priority_variants.update(arm[0] for arm in candidates)
    failures.extend(completion_priority_activation_failures(output, priority_variants))
    write_json(output / "completion.json", {"failures": failures})
    render(output, output / "report.html")
    if failures:
        raise RuntimeError(f"{len(failures)} cases failed; evidence retained")


if __name__ == "__main__":
    main()

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
from .evidence import ROOT, digest, require_clean_repository, verify, write_json
from .feature_coverage import combined_step_checks, completion_priority_checks, verdict as coverage_verdict
from .report import render
from .runner import run_case
from .selection import capture_rafter_selection, restore_rafter_selection, select_rafter
from .network import loopback_delay
from .suite_plan import MODES, execution_plan, load_window_plan, mode_flags, ordered_arms


OPENRAFT_CONTROLS = ("synchronous", "async")


def qualify_machine(data_root: Path, output: Path) -> dict:
    """Capture the idle host after builds and bind its passing seal to this suite."""
    from .machine import capture_profile

    profile = output / "machine-profile"
    verdict = capture_profile(data_root.resolve(), profile)
    checked = verify(profile)
    status = "passed" if verdict["status"] == "passed" and checked["status"] == "passed" else "failed"
    receipt = {
        "schema": 1,
        "status": status,
        "profile": profile.name,
        "seal_sha256": digest(profile / "SHA256SUMS.json"),
        "data_root": str(data_root.resolve()),
        "verification": checked["verdicts"],
    }
    write_json(output / "machine-qualification.json", receipt)
    if status != "passed":
        raise RuntimeError(
            "fixed-machine idle objective did not pass; sealed evidence was retained"
        )
    return receipt


def openraft_arms(controls: list[str]) -> list[tuple[str, str, int, str, int, bool, int]]:
    if not controls or len(set(controls)) != len(controls) or any(
            control not in OPENRAFT_CONTROLS for control in controls):
        raise ValueError("distinct OpenRaft controls drawn from synchronous,async are required")
    labels = {"synchronous": "openraft", "async": "openraft-async"}
    return [(labels[control], "openraft", 1, "candidate", 1, False, 8) for control in controls]


def candidate_arms(caps: list[int], thresholds: list[int], windows: list[int],
                   combine: bool, durable_completion_priority: bool = False,
                   snapshot_intervals: list[int] | None = None) -> list[tuple]:
    snapshot_aware = snapshot_intervals is not None
    intervals = snapshot_intervals or [0]

    def label(cap: int, threshold: int, window: int, interval: int) -> str:
        result = f"candidate-b{cap}" if thresholds == [1] else f"candidate-b{cap}-s{threshold}"
        if combine:
            result += "-combined"
        if durable_completion_priority:
            result += "-commit-first"
        if windows != [8]:
            result += f"-w{window}"
        if snapshot_aware:
            result += f"-snapshot{interval}"
        return result

    arms = []
    for cap in caps:
        for threshold in thresholds:
            for window in windows:
                for interval in intervals:
                    arm = (label(cap, threshold, window, interval), "rafter", cap,
                           "candidate", threshold, combine, window)
                    arms.append((*arm, interval) if snapshot_aware else arm)
    return arms


def worker_smoke_scenarios(candidate_mode: str, hard_state: str) -> list[str]:
    scenarios = (["durable-kv", "leader-loss", "follower-catchup"]
                 if candidate_mode in ("messages", "pipeline")
                 else ["durable-kv"])
    if candidate_mode == "pipeline" and hard_state == "wal":
        scenarios.append("snapshot-catchup")
    return scenarios


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


def combined_activation_failures(output: Path, arms: list[tuple]) -> list[dict]:
    required = {arm[0] for arm in arms if arm[5]}
    return [{"case": f"suite:{check['variant']}", "error": check["detail"]}
            for check in combined_step_checks(output, required) if check["status"] != "passed"]


def completion_priority_activation_failures(output: Path, variants: set[str]) -> list[dict]:
    return [{"case": f"suite:{check['variant']}", "error": check["detail"]}
            for check in completion_priority_checks(output, variants)
            if check["status"] != "passed"]


def prepare_builds_and_smokes(
    args,
    output: Path,
    work: Path,
    data: Path,
    thresholds: list[int],
    windows: list[int],
) -> tuple[dict, dict]:
    """Build selected revisions, run functional smokes, then restore source pins."""
    original = capture_rafter_selection()
    try:
        select_rafter(args.prior)
        build(
            args.prior_hard_state,
            peer_group_commit=args.prior_peer_batch_size > 1,
            ordered_apply=args.prior_mode != "inline",
            pipelined_durability=args.prior_mode == "pipeline",
        )
        prior = archive_build(work / "prior")
        write_json(output / "prior-build.json", prior)
        select_rafter(args.candidate)
        build(
            args.candidate_hard_state,
            peer_group_commit=True,
            ordered_apply=args.candidate_mode != "inline",
            pipelined_durability=args.candidate_mode == "pipeline",
        )
        candidate = archive_build(work / "candidate")
        write_json(output / "candidate-build.json", candidate)
        if args.candidate_mode != "inline":
            scenarios = worker_smoke_scenarios(
                args.candidate_mode, args.candidate_hard_state
            )
            for scenario in scenarios:
                flags = (
                    ["--peer-message-stream"]
                    if args.candidate_mode in ("messages", "pipeline")
                    else []
                )
                engines = "rafter,raft-rs"
                if args.candidate_mode == "pipeline":
                    flags.extend([
                        "--pipelined-durability",
                        "--max-speculative-proposals",
                        str(thresholds[0]),
                        "--max-inflight-appends",
                        str(windows[0]),
                    ])
                    if args.candidate_combine_peer_proposals:
                        flags.append("--combine-peer-proposals")
                    if args.candidate_durable_completion_priority:
                        flags.append("--durable-completion-priority")
                    if scenario == "snapshot-catchup":
                        flags.extend(["--snapshot-interval-entries", "8"])
                    engines = "rafter"
                subprocess.run(
                    [
                        sys.executable,
                        str(ROOT / "raft-bench"),
                        "run",
                        "--smoke",
                        "--implementations",
                        engines,
                        "--ordered-apply",
                        "--peer-batch-size",
                        "32",
                        "--scenario",
                        scenario,
                        *flags,
                        "--output",
                        str(output / f"worker-smoke-{scenario}"),
                        "--data-root",
                        str(data / "smoke"),
                    ],
                    cwd=ROOT,
                    check=True,
                )
        return prior, candidate
    finally:
        restore_rafter_selection(original)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prior", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument(
        "--benchmark-sha",
        help="exact clean benchmark repository revision required by fixed-machine runs",
    )
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
    parser.add_argument("--prior-snapshot-interval-entries", type=int, default=0)
    parser.add_argument("--candidate-snapshot-interval-entries",
                        help="comma-separated Rafter snapshot/checkpoint intervals")
    parser.add_argument("--rates", default="0,100,1000")
    parser.add_argument("--diagnostic-rates", help="defaults to all timing rates")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--openraft-controls", default="synchronous",
                        help="comma-separated OpenRaft controls: synchronous,async; use none for an internal Rafter comparison")
    parser.add_argument("--suite-kind", choices=("paired-durable-service", "reclamation-under-load"),
                        default="paired-durable-service")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument(
        "--qualify-machine",
        action="store_true",
        help="after builds, require a fresh passing idle/storage profile before timing",
    )
    args = parser.parse_args()
    if args.qualify_machine and not args.benchmark_sha:
        raise ValueError("--qualify-machine requires --benchmark-sha")
    benchmark_repository = (
        require_clean_repository(args.benchmark_sha) if args.benchmark_sha else None
    )
    if not 3 <= args.runs <= 10:
        raise ValueError("paired evidence requires 3..10 repetitions")
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
    snapshot_aware = args.candidate_snapshot_interval_entries is not None
    snapshot_intervals = (
        [int(value) for value in args.candidate_snapshot_interval_entries.split(",")]
        if snapshot_aware else [0]
    )
    if (
        not snapshot_intervals
        or len(set(snapshot_intervals)) != len(snapshot_intervals)
        or any(value < 0 or value > 1_000_000_000 for value in snapshot_intervals)
    ):
        raise ValueError("distinct candidate snapshot intervals in 0..1000000000 are required")
    if not 0 <= args.prior_snapshot_interval_entries <= 1_000_000_000:
        raise ValueError("prior snapshot interval must be in 0..1000000000")
    if args.prior_snapshot_interval_entries and (
        args.prior_mode != "pipeline" or args.prior_hard_state != "wal"
    ):
        raise ValueError("prior snapshot compaction requires pipeline mode with WAL")
    if any(snapshot_intervals) and (
        args.candidate_mode != "pipeline" or args.candidate_hard_state != "wal"
    ):
        raise ValueError("candidate snapshot compaction requires pipeline mode with WAL")
    if args.suite_kind == "reclamation-under-load" and (
        not snapshot_aware
        or not snapshot_intervals
        or any(interval == 0 for interval in snapshot_intervals)
        or args.prior_snapshot_interval_entries != 0
    ):
        raise ValueError("reclamation-under-load requires a no-snapshot prior and nonzero candidate intervals")
    if args.suite_kind == "reclamation-under-load":
        exact = lambda value: (
            len(value) == 40
            and all(character in "0123456789abcdef" for character in value.lower())
        )
        if not exact(args.prior) or args.prior.lower() != args.candidate.lower():
            raise ValueError("reclamation-under-load requires the same exact Rafter SHA in both arms")
        if (
            args.prior_hard_state != "wal"
            or args.candidate_hard_state != "wal"
            or args.prior_mode != "pipeline"
            or args.candidate_mode != "pipeline"
            or caps != [args.prior_peer_batch_size]
            or thresholds != [args.prior_max_speculative_proposals]
            or windows != [args.prior_max_inflight_appends]
            or args.prior_combine_peer_proposals != args.candidate_combine_peer_proposals
            or args.prior_durable_completion_priority
            != args.candidate_durable_completion_priority
        ):
            raise ValueError(
                "reclamation-under-load arms may differ only by snapshot interval and build label"
            )
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
    controls = [] if args.openraft_controls == "none" else args.openraft_controls.split(",")
    control_arms = [] if not controls else openraft_arms(controls)
    if args.suite_kind == "reclamation-under-load" and controls:
        raise ValueError("reclamation-under-load is a same-code internal Rafter comparison")
    delays = [int(value) for value in args.network_delays_ms.split(",")]
    if not delays or len(set(delays)) != len(delays) or any(value < 0 or value > 100 for value in delays):
        raise ValueError("distinct network delays in 0..100 are required")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    work = ROOT / ".cache/paired" / uuid.uuid4().hex
    data = args.data_root.resolve() / work.name
    prior, candidate = prepare_builds_and_smokes(
        args, output, work, data, thresholds, windows
    )
    if prior["loadgen"] != candidate["loadgen"] or digest(ROOT / "dist/raft-bench-load") != candidate["loadgen"]:
        raise RuntimeError("paired runs require the identical load generator")
    for engine in ("raft-rs", "openraft"):
        if prior["implementations"][engine] != candidate["implementations"][engine]:
            raise RuntimeError("control implementation changed while selecting Rafter")
    machine_qualification = (
        qualify_machine(args.data_root, output) if args.qualify_machine else None
    )
    prior_arm = ("prior", "rafter", args.prior_peer_batch_size, "prior",
                 args.prior_max_speculative_proposals, args.prior_combine_peer_proposals,
                 args.prior_max_inflight_appends)
    if snapshot_aware or args.prior_snapshot_interval_entries:
        prior_arm = (*prior_arm, args.prior_snapshot_interval_entries)
    arms = [prior_arm, *control_arms]
    candidates = candidate_arms(caps, thresholds, windows, args.candidate_combine_peer_proposals,
                                args.candidate_durable_completion_priority,
                                snapshot_intervals if snapshot_aware else None)
    arms += candidates
    if args.suite_kind == "reclamation-under-load" and args.runs % len(arms):
        raise ValueError(
            "reclamation-under-load repetitions must be a multiple of the arm count for position balance"
        )
    diagnostic_arms = (arms if len(thresholds) > 1 or len(windows) > 1 or len(snapshot_intervals) > 1 else
                       [arms[0], *control_arms,
                        next((arm for arm in candidates if arm[2] == 32), candidates[-1])])
    suite = {"schema": 5,
        "kind": args.suite_kind, "arms": arms, "rates": rates, "runs": args.runs,
        "order": "cyclic arm rotation by repetition; all cases sequential on one host",
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
        "prior_snapshot_interval_entries": args.prior_snapshot_interval_entries,
        "candidate_snapshot_interval_entries": snapshot_intervals,
        "data_root": str(data), "machine_qualification": machine_qualification,
        "benchmark_repository": benchmark_repository}
    plan = execution_plan(suite)
    suite["execution_plan"] = plan
    suite["load_window_plan"] = load_window_plan(plan)
    write_json(output / "suite.json", suite)
    planned = suite["load_window_plan"]
    print(
        f"Plan: {planned['cases']} cases; "
        f"{planned['declared_load_window_seconds'] / 60:.1f} declared load-window minutes "
        "plus qualification, recovery, build, and process overhead",
        flush=True,
    )
    failures = []
    by_delay = {delay: [] for delay in delays}
    for planned in plan:
        by_delay[planned["options"]["network_delay_ms"]].append(planned)
    for delay, delay_plan in by_delay.items():
        with loopback_delay(delay) as network:
            write_json(output / f"network-{delay}ms.json", network)
            for planned in delay_plan:
                name = planned["name"]
                engine = planned["implementation"]
                binary_set = planned["binary_set"]
                options = SimpleNamespace(**planned["options"])
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
    priority_variants = set()
    if args.prior_durable_completion_priority:
        priority_variants.add("prior")
    if args.candidate_durable_completion_priority:
        priority_variants.update(arm[0] for arm in candidates)
    combined_variants = {arm[0] for arm in arms if arm[5]}
    feature_coverage = coverage_verdict(
        output,
        completion_priority=priority_variants,
        combined=combined_variants,
    )
    write_json(output / "completion.json", {
        "schema": 2,
        "failures": failures,
        "feature_coverage": feature_coverage,
    })
    render(output, output / "report.html")
    assessment = None
    if args.suite_kind == "reclamation-under-load":
        from .reclamation_load import assess, markdown
        from .results import load_durable_suite

        assessment = assess(load_durable_suite(output, require_derived=False))
        write_json(output / "reclamation-under-load.json", assessment)
        (output / "reclamation-under-load.md").write_text(markdown(assessment))
    if failures:
        raise RuntimeError(f"{len(failures)} cases failed; evidence retained")
    if assessment is not None and assessment["status"] != "passed":
        raise RuntimeError("reclamation-under-load objective did not pass; evidence retained")


if __name__ == "__main__":
    main()

from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid
from .evidence import ROOT, digest, source_digest, verify, write_json
from .build import IMPLEMENTATIONS, build


def run(options) -> None:
    from .runner import run_case
    from .report import render
    if os.name != "posix":
        raise ValueError("local process/fault runner requires POSIX (Linux recommended)")
    implementations = options.implementations.split(",")
    if not implementations or len(set(implementations)) != len(implementations) or any(i not in IMPLEMENTATIONS for i in implementations):
        raise ValueError("implementations must be distinct members of rafter,raft-rs,openraft")
    rates = [float(r) for r in options.rates.split(",")]
    if not rates or any(not math.isfinite(r) or r < 0 or r > 1e7 for r in rates):
        raise ValueError("rates must be finite numbers from 0 to 10000000")
    if options.smoke:
        options.duration = min(options.duration, 3)
        options.warmup = min(options.warmup, .5)
        options.concurrency = min(options.concurrency, 4)
        options.runs = 1
    elif options.duration < 60 or options.warmup < 5 or options.runs < 3:
        raise ValueError("evidence mode requires duration >=60s, warmup >=5s, runs >=3; use --smoke for short tests")
    if options.duration <= 0 or options.duration > 3600 or options.warmup < 0 or options.warmup > 600 or options.runs < 1 or options.runs > 100:
        raise ValueError("invalid duration, warmup or run count")
    if not 1 <= options.concurrency <= 1024 or not 1 <= options.payload <= 65536 or not 1 <= options.keyspace <= 1_000_000:
        raise ValueError("invalid workload sizes")
    if not 1 <= options.batch_size <= 64 or not 0 < options.timeout <= 60 or options.read_percent < 0 or options.cas_percent < 0 or options.read_percent + options.cas_percent > 100:
        raise ValueError("invalid batch size, timeout or mix")
    if not options.data_root and not options.smoke:
        raise ValueError("--data-root is required for evidence runs; select the intended storage volume explicitly")
    receipt_path = ROOT / "dist/build.json"
    if not receipt_path.exists():
        raise RuntimeError("no verified local build; run ./raft-bench build first")
    receipt = json.loads(receipt_path.read_text())
    if receipt.get("source_digest") != source_digest():
        raise RuntimeError("sources changed since build receipt; rebuild")
    for implementation in implementations:
        binary = ROOT / "dist" / f"raft-bench-{implementation}"
        if not binary.exists() or digest(binary) != receipt["binaries"][implementation]:
            raise RuntimeError("adapter binary does not match build receipt")
    if digest(ROOT / "dist/raft-bench-load") != receipt["loadgen"]:
        raise RuntimeError("load generator does not match build receipt")
    suite_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    suite = options.output.resolve() if options.output else ROOT / "results/runs" / suite_id
    suite.mkdir(parents=True, exist_ok=False)
    data_root = (options.data_root or ROOT / ".cache/smoke-data").resolve() / suite_id
    data_root.mkdir(parents=True, exist_ok=False)
    write_json(suite / "suite.json", {"schema": 1, "id": suite_id, "implementations": implementations,
        "rates": rates, "runs": options.runs, "order": "cyclic rotation by repetition", "smoke": options.smoke,
        "source_digest": source_digest(), "data_root": str(data_root)})
    failures = []
    ordinal = 0
    for repeat in range(options.runs):
        ordered = implementations[repeat % len(implementations):] + implementations[:repeat % len(implementations)]
        for rate in rates:
            for implementation in ordered:
                ordinal += 1
                options.rate = rate
                options.seed = options.seed_base + repeat
                name = f"{ordinal:03d}-{implementation}-r{repeat + 1}-q{rate:g}"
                print(f"Running {name}", flush=True)
                try:
                    run_case(implementation, suite / name, data_root / name, options)
                except Exception as exc:
                    failures.append({"case": name, "error": str(exc)})
                    print(f"FAILED {name}: {exc}", file=sys.stderr)
    write_json(suite / "completion.json", {"failures": failures})
    render(suite, suite / "report.html")
    print(f"Results: {suite}\nReport: {suite / 'report.html'}")
    if failures:
        raise RuntimeError(f"{len(failures)} case(s) failed; artifacts were retained")


def node_configs(inventory_path: Path, destination: Path) -> None:
    inventory = json.loads(inventory_path.read_text())
    nodes = inventory["nodes"]
    if len(nodes) != 3 or {n["id"] for n in nodes} != {1, 2, 3}:
        raise ValueError("inventory must contain exactly node IDs 1,2,3")
    destination.mkdir(parents=True, exist_ok=False)
    peers = {n["id"]: n["peer"] for n in nodes}
    for n in nodes:
        write_json(destination / f"node-{n['id']}.json", {"id": n["id"], "cluster": inventory["cluster"],
            "client": n["client"], "peer": n["peer"], "peers": peers, "data_dir": n["data_dir"],
            "tick_ms": 20, "capacity": 4096, "batch_size": 64})
    print(f"Node configs: {destination}. These commands do not provision or modify remote hosts.")


def main() -> None:
    parser = argparse.ArgumentParser(description="raft-bench: protocol diagnostics and durable networked embedding baselines")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("doctor", help="show required tools; makes no installations")
    sub.add_parser("build", help="test and build all three Rust adapters and native load generator with the locked dependencies")
    sub.add_parser("import-protocol", help="extract exact historical sources from the pinned upstream commit")
    p = sub.add_parser("protocol", help="run the retained in-memory comparison, not the durable-service suite")
    p.add_argument("--mode", choices=("full", "rafter-only"), default="full")
    p.add_argument("--runs", type=int, default=5)
    p = sub.add_parser("run")
    p.add_argument("--implementations", default=",".join(IMPLEMENTATIONS))
    p.add_argument("--scenario", choices=("durable-kv", "leader-loss", "follower-catchup"), default="durable-kv")
    p.add_argument("--smoke", action="store_true")
    p.add_argument("--duration", type=float, default=60)
    p.add_argument("--warmup", type=float, default=10)
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--concurrency", type=int, default=64)
    p.add_argument("--payload", type=int, default=512)
    p.add_argument("--keyspace", type=int, default=10000)
    p.add_argument("--rates", default="0")
    p.add_argument("--read-percent", type=int, default=0)
    p.add_argument("--cas-percent", type=int, default=0)
    p.add_argument("--batch-size", type=int, default=64)
    p.add_argument("--timeout", type=float, default=2)
    p.add_argument("--seed-base", type=int, default=1)
    p.add_argument("--data-root", type=Path)
    p.add_argument("--output", type=Path)
    p = sub.add_parser("verify")
    p.add_argument("case", type=Path)
    p = sub.add_parser("report")
    p.add_argument("suite", type=Path)
    p.add_argument("--output", type=Path, required=True)
    p = sub.add_parser("node-configs")
    p.add_argument("--inventory", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p = sub.add_parser("initialize", help="explicitly initialize a new OpenRaft test cluster; never use on existing data")
    p.add_argument("--address", required=True)
    args = parser.parse_args()
    try:
        if args.command == "doctor":
            print(json.dumps({t: shutil.which(t) for t in ("cargo", "rustc", "go", "protoc", "git", "python3")}, indent=2))
        elif args.command == "build":
            build()
        elif args.command in ("import-protocol", "protocol"):
            from .legacy import import_protocol, run_protocol
            if args.command == "import-protocol":
                import_protocol()
            else:
                run_protocol(args.mode, args.runs)
        elif args.command == "run":
            run(args)
        elif args.command == "verify":
            result = verify(args.case.resolve())
            print(json.dumps(result, indent=2))
            if result["status"] != "passed":
                raise RuntimeError("verification failed")
        elif args.command == "report":
            from .report import render
            render(args.suite, args.output)
        elif args.command == "node-configs":
            node_configs(args.inventory, args.output)
        elif args.command == "initialize":
            from .protocol import request
            result = request(args.address, {"op": "initialize"}, timeout=15)
            print(json.dumps(result, indent=2))
            if result.get("status") != "ok":
                raise RuntimeError("initialization refused")
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError, KeyError) as exc:
        parser.exit(1, f"error: {exc}\n")

if __name__ == "__main__":
    main()

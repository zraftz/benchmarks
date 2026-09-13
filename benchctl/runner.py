"""Local multiprocess benchmark. Cross-host traffic uses the same Go generator.

No test double can enter the public CLI implementation matrix. All performance
measurements here are newly produced by actual child binaries, never copied
from historical protocol results.
"""
from __future__ import annotations
from pathlib import Path
import json
import os
import subprocess
import time
import uuid
from . import protocol
from .checker import check_history, read_history
from .cluster import Cluster
from .evidence import (ROOT, capture, digest, host_info, proc_sample, seal, source_digest,
                       system_sample, validate_result, write_json)
from .machine import LOAD_ENVIRONMENT_SCHEMA, load_environment_receipt


def load_command(nodes: dict[int, str], directory: Path, name: str, *, duration: float,
                 concurrency: int, payload: int, rate: float, keyspace: int, seed: int,
                 reads: int = 0, cas: int = 0, operations: int = 0, history: bool = False,
                 namespace: str = "bench", timeout: float = 2) -> list[str]:
    args = [str(ROOT / "dist/raft-bench-load"), "--nodes", ",".join(f"{i}={a}" for i, a in nodes.items()),
        "--session", uuid.uuid4().hex, "--namespace", namespace,
        "--output", str(directory / f"{name}.json"), "--duration", f"{duration}s",
        "--timeout", f"{timeout}s", "--concurrency", str(concurrency), "--payload", str(payload),
        "--rate", str(rate), "--keyspace", str(keyspace), "--seed", str(seed),
        "--read-percent", str(reads), "--cas-percent", str(cas), "--operations", str(operations)]
    if history:
        args += ["--history", str(directory / f"{name}.jsonl")]
    return args


def checked_load(args: list[str], log: Path, timeout: float = 60) -> None:
    with log.open("xb") as stream:
        subprocess.run(args, stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=timeout)


def canaries(nodes: dict[int, str], session: str) -> list[dict]:
    commands = []
    for i in range(12):
        c = {"client": f"{session}-writer", "sequence": i + 1, "kind": "put",
             "key": f"audit/{session}/{i}", "value": f"acknowledged-{session}-{i}", "expected": None}
        result = protocol.execute(nodes, c)
        if result.get("value") != c["value"]:
            raise RuntimeError("canary write result does not match")
        commands.append(c)
    return commands


def confirm_canaries(nodes: dict[int, str], commands: list[dict], session: str) -> dict:
    checked = []
    for i, original in enumerate(commands):
        c = {"client": f"{session}-reader", "sequence": i + 1, "kind": "get",
             "key": original["key"], "value": "", "expected": None}
        result = protocol.execute(nodes, c)
        if result.get("value") != original["value"]:
            raise RuntimeError(f"acknowledged canary missing after restart: {original['key']}")
        checked.append(original["key"])
    return {"status": "passed", "checked_keys": checked,
            "scope": "12 acknowledged canaries survived simultaneous process kill and restart; not power-loss testing"}


def run_case(implementation: str, directory: Path, data: Path, options, *, command: list[str] | None = None, build_receipt: dict | None = None) -> None:
    directory.mkdir(parents=True, exist_ok=False)
    data.mkdir(parents=True, exist_ok=True)
    command = command or [str(ROOT / "dist" / f"raft-bench-{implementation}")]
    receipt = build_receipt if build_receipt is not None else (json.loads((ROOT / "dist/build.json").read_text()) if (ROOT / "dist/build.json").exists() else None)
    if receipt and digest(Path(command[0])) != receipt["binaries"][implementation]:
        raise RuntimeError("case binary differs from the supplied build receipt")
    case_id = uuid.uuid4().hex
    sample_interval_seconds = 0.1 if options.smoke else 1.0
    manifest = {"schema": 1, "implementation": implementation, "case_id": case_id,
        "scenario": options.scenario, "topology": "three processes on one host; real TCP, not three physical hosts",
        "smoke": options.smoke, "controller_start_unix_ns": time.time_ns(),
        "load_environment_observation": {
            "schema": LOAD_ENVIRONMENT_SCHEMA,
            "expected_duration_seconds": options.duration,
            "nominal_sample_interval_seconds": sample_interval_seconds,
        },
        "options": {k: str(v) if isinstance(v, Path) else v for k, v in vars(options).items()},
        "host": host_info(data), "source_digest": source_digest(),
        "implementation_pins": receipt["implementations"] if receipt else json.loads((ROOT / "implementations.lock.json").read_text()),
        "binary_sha256": digest(Path(command[0])), "loadgen_sha256": digest(ROOT / "dist/raft-bench-load"),
        "build_receipt": receipt,
        "limitations": ["plaintext transport", "logged reads", "static three-voter group", "retained logs; no snapshots",
                        "adapter storage/codec costs differ and are disclosed", "not upstream-reviewed tuning"]}
    write_json(directory / "manifest.json", manifest)
    cluster = Cluster(implementation, command, directory / "cluster", data, case_id, options.batch_size,
        getattr(options, "peer_batch_size", 1), getattr(options, "diagnostics", False),
        getattr(options, "ordered_apply", False), getattr(options, "peer_message_stream", False),
        getattr(options, "pipelined_durability", False),
        getattr(options, "max_speculative_proposals", 1),
        getattr(options, "combine_peer_proposals", False),
        getattr(options, "max_inflight_appends", 8),
        getattr(options, "durable_completion_priority", False),
        getattr(options, "openraft_async_flush", False))
    load = None
    try:
        cluster.start()
        qualify = load_command(cluster.nodes, directory, "qualification-history", duration=20, concurrency=4,
            payload=32, rate=0, keyspace=4, seed=1, reads=40, cas=20, operations=64, history=True,
            namespace=f"qualification/{case_id}", timeout=5)
        checked_load(qualify, directory / "qualification.log")
        verdict = check_history(read_history(directory / "qualification-history.jsonl"))
        write_json(directory / "qualification.json", verdict)
        if verdict["status"] != "passed":
            raise RuntimeError(f"qualification did not pass: {verdict}")
        pre = canaries(cluster.nodes, case_id + "pre")
        # Qualification includes a real simultaneous process kill before measurement.
        cluster.stop_all()
        cluster.start(initialize=False)
        pre_recovery = confirm_canaries(cluster.nodes, pre, case_id + "pre")
        write_json(directory / "preflight-recovery.json", pre_recovery)
        if options.warmup > 0:
            args = load_command(cluster.nodes, directory, "warmup", duration=options.warmup,
                concurrency=options.concurrency, payload=options.payload, rate=options.rate,
                keyspace=options.keyspace, seed=options.seed, reads=options.read_percent,
                cas=options.cas_percent, namespace="bench", timeout=options.timeout)
            checked_load(args, directory / "warmup.log", options.warmup + options.timeout * 3 + 30)
        before = protocol.statuses(cluster.nodes)
        write_json(directory / "before.json", before)
        args = load_command(cluster.nodes, directory, "measurement", duration=options.duration,
            concurrency=options.concurrency, payload=options.payload, rate=options.rate,
            keyspace=options.keyspace, seed=options.seed, reads=options.read_percent,
            cas=options.cas_percent, namespace="bench", timeout=options.timeout)
        write_json(directory / "load-command.json", {"argv": args})
        with (directory / "load.log").open("xb") as output, (directory / "process-samples.jsonl").open("x") as samples:
            start = time.monotonic()
            load = subprocess.Popen(args, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            fault_node = None
            injected = False
            restored = False
            fault = {"scenario": options.scenario, "events": [], "timing_resolution": "controller timestamps; generator timeline is 1-second buckets"}
            while load.poll() is None:
                elapsed = time.monotonic() - start
                if elapsed > options.duration + options.timeout * 4 + 30:
                    raise TimeoutError("load generator exceeded run deadline")
                if not injected and elapsed >= options.duration / 3 and options.scenario != "durable-kv":
                    current = protocol.leader(cluster.nodes)
                    if options.scenario == "leader-loss":
                        fault_node = current
                        cluster.stop_node(fault_node)
                        action = "SIGKILL leader"
                    else:
                        fault_node = next(i for i in cluster.nodes if i != current)
                        cluster.pause(fault_node)
                        action = "SIGSTOP follower"
                    fault["events"].append({"unix_ns": time.time_ns(), "controller_seconds": elapsed, "node": fault_node, "action": action})
                    injected = True
                if injected and not restored and elapsed >= 2 * options.duration / 3:
                    if options.scenario == "leader-loss":
                        cluster.start_node(fault_node)
                        action = "restart same data directory"
                    else:
                        cluster.resume(fault_node)
                        action = "SIGCONT follower"
                    fault["events"].append({"unix_ns": time.time_ns(), "controller_seconds": elapsed, "node": fault_node, "action": action})
                    restored = True
                observed = {"controller_seconds": elapsed,
                    "monotonic_ns": time.monotonic_ns(),
                    "nodes": {i: proc_sample(p.pid) for i, p in cluster.processes.items()},
                    "load_generator": proc_sample(load.pid),
                    "system": system_sample(data_path=data)}
                samples.write(json.dumps(observed) + "\n")
                samples.flush()
                time.sleep(sample_interval_seconds)
            elapsed = time.monotonic() - start
            observed = {"controller_seconds": elapsed,
                "monotonic_ns": time.monotonic_ns(),
                "nodes": {i: proc_sample(p.pid) for i, p in cluster.processes.items()},
                "load_generator": proc_sample(load.pid),
                "system": system_sample(data_path=data)}
            samples.write(json.dumps(observed) + "\n")
            samples.flush()
            if load.returncode:
                raise RuntimeError(f"load generator exited {load.returncode}; inspect load.log")
        write_json(
            directory / "load-environment.json",
            load_environment_receipt(
                [
                    json.loads(line)
                    for line in (directory / "process-samples.jsonl").read_text().splitlines()
                ],
                expected_duration_seconds=options.duration,
                nominal_sample_interval_seconds=sample_interval_seconds,
            ),
        )
        if injected and not restored:
            if options.scenario == "leader-loss":
                cluster.start_node(fault_node)
            else:
                cluster.resume(fault_node)
        if options.scenario != "durable-kv" and not injected:
            raise RuntimeError("requested fault was not injected; refusing to label this a fault run")
        write_json(directory / "fault.json", fault)
        result = json.loads((directory / "measurement.json").read_text())
        invalid = validate_result(result)
        if invalid:
            raise RuntimeError(f"measurement accounting failed: {invalid}")
        cluster.wait_ready()
        protocol.leader(cluster.nodes)
        after_load = None
        if getattr(options, "diagnostics", False) or (
                getattr(options, "pipelined_durability", False) and options.scenario == "durable-kv"):
            after_load = protocol.statuses(cluster.nodes)
        if getattr(options, "diagnostics", False):
            from .timelines import extract
            write_json(directory / "diagnostics-after-load.json", after_load)
            write_json(directory / "operation-timelines.json", extract(before, after_load))
            if implementation == "rafter":
                from .replication_windows import configuration_receipt
                write_json(directory / "replication-window-config.json", configuration_receipt(
                    before, after_load, getattr(options, "max_inflight_appends", 8)))
        if getattr(options, "pipelined_durability", False) and options.scenario == "durable-kv":
            from .pipeline import activity
            write_json(directory / "persistence-after-load.json", after_load)
            write_json(directory / "pipeline-activity.json", activity(
                before, after_load,
                require_combined=getattr(options, "combine_peer_proposals", False)))
            if getattr(options, "durable_completion_priority", False):
                from .completion_priority import activity as completion_priority_activity
                write_json(directory / "completion-priority-activity.json",
                           completion_priority_activity(before, after_load))
        post = canaries(cluster.nodes, case_id + "post")
        write_json(directory / "after.json", protocol.statuses(cluster.nodes))
        cluster.stop_all()
        cluster.start(initialize=False)
        recovery = confirm_canaries(cluster.nodes, pre + post, case_id + "final")
        recovery["scope"] = "24 pre/post-load acknowledged canaries survived process restarts; finite check, not all measured writes or power loss"
        write_json(directory / "recovery.json", recovery)
        write_json(directory / "outcome.json", {"status": "completed", "smoke": options.smoke,
            "evidence_class": "smoke-only" if options.smoke else "embedding-baseline",
            "completed_unix_ns": time.time_ns()})
    except BaseException as exc:
        if not (directory / "failure.json").exists():
            write_json(directory / "failure.json", {"status": "failed", "error": str(exc), "type": type(exc).__name__})
        raise
    finally:
        if load is not None and load.poll() is None:
            load.kill()
            load.wait(timeout=5)
        cluster.close()
        seal(directory)

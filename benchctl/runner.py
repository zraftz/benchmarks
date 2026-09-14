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
from .evidence import (ROOT, capture, digest, host_info, seal, source_digest,
                       validate_result, write_json)
from .load_sampling import LoadSampler
from .machine import LOAD_ENVIRONMENT_SCHEMA, load_environment_receipt
from .storage_footprint import capture as capture_storage_footprint
from .storage_footprint import OBSERVATION as STORAGE_FOOTPRINT_OBSERVATION
from .storage_footprint import receipt as storage_footprint_receipt


def qualification_timeout_seconds(options) -> int:
    """Keep tiny-interval snapshot smokes functional without changing timed loads."""
    if options.smoke and getattr(options, "snapshot_interval_entries", 0):
        return 15
    return 5


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
    # Smoke cases are functional evidence, not higher-resolution performance
    # measurements. Use the normal cadence so a slow host-counter probe does
    # not turn its own collection cost into a spurious coverage gap.
    sample_interval_seconds = 1.0
    observe_storage_footprint = bool(
        implementation == "rafter"
        and receipt
        and receipt.get("rafter_hard_state_backend") == "wal"
    )
    manifest = {"schema": 1, "implementation": implementation, "case_id": case_id,
        "scenario": options.scenario, "topology": "three processes on one host; real TCP, not three physical hosts",
        "smoke": options.smoke, "controller_start_unix_ns": time.time_ns(),
        "load_environment_observation": {
            "schema": LOAD_ENVIRONMENT_SCHEMA,
            "expected_duration_seconds": options.duration,
            "nominal_sample_interval_seconds": sample_interval_seconds,
        },
        "storage_footprint_observation": (
            STORAGE_FOOTPRINT_OBSERVATION if observe_storage_footprint else None
        ),
        "options": {k: str(v) if isinstance(v, Path) else v for k, v in vars(options).items()},
        "host": host_info(data),
        "source_digest": receipt.get("source_digest") if receipt else source_digest(),
        "implementation_pins": receipt["implementations"] if receipt else json.loads((ROOT / "implementations.lock.json").read_text()),
        "binary_sha256": digest(Path(command[0])), "loadgen_sha256": digest(ROOT / "dist/raft-bench-load"),
        "build_receipt": receipt,
        "limitations": ["plaintext transport", "logged reads", "static three-voter group",
                        ("application journal remains append-only; Raft snapshots and WAL reclamation active"
                         if getattr(options, "snapshot_interval_entries", 0)
                         else "retained logs; no snapshots"),
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
        getattr(options, "openraft_async_flush", False),
        getattr(options, "snapshot_interval_entries", 0))
    load = None
    try:
        cluster.start()
        qualify = load_command(cluster.nodes, directory, "qualification-history", duration=20, concurrency=4,
            payload=32, rate=0, keyspace=4, seed=1, reads=40, cas=20, operations=64, history=True,
            namespace=f"qualification/{case_id}", timeout=qualification_timeout_seconds(options))
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
        before = protocol.complete_statuses(cluster.nodes)
        write_json(directory / "before.json", before)
        footprint_before = (
            capture_storage_footprint(data) if observe_storage_footprint else None
        )
        args = load_command(cluster.nodes, directory, "measurement", duration=options.duration,
            concurrency=options.concurrency, payload=options.payload, rate=options.rate,
            keyspace=options.keyspace, seed=options.seed, reads=options.read_percent,
            cas=options.cas_percent, namespace="bench", timeout=options.timeout)
        write_json(directory / "load-command.json", {"argv": args})
        with (directory / "load.log").open("xb") as output, (directory / "process-samples.jsonl").open("x") as samples:
            start = time.monotonic()
            load = subprocess.Popen(args, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            sampler = LoadSampler(
                samples,
                start=start,
                interval_seconds=sample_interval_seconds,
                processes=cluster.process_snapshot,
                load_pid=load.pid,
                data_path=data,
            )
            sampler.start()
            fault_node = None
            stopped_application_index = None
            injected = False
            restored = False
            fault = {"scenario": options.scenario, "events": [], "timing_resolution": "controller timestamps; generator timeline is 1-second buckets"}
            try:
                while load.poll() is None:
                    elapsed = time.monotonic() - start
                    if elapsed > options.duration + options.timeout * 4 + 30:
                        raise TimeoutError("load generator exceeded run deadline")
                    inject_at = 0 if options.scenario == "snapshot-catchup" else options.duration / 3
                    if not injected and elapsed >= inject_at and options.scenario != "durable-kv":
                        current = protocol.leader(cluster.nodes)
                        if options.scenario == "leader-loss":
                            fault_node = current
                            cluster.stop_node(fault_node)
                            action = "SIGKILL leader"
                        elif options.scenario == "snapshot-catchup":
                            fault_node = next(i for i in cluster.nodes if i != current)
                            stopped = protocol.request(cluster.nodes[fault_node], {"op": "status"})
                            stopped_application_index = stopped["info"]["application"]["applied_index"]
                            cluster.stop_node(fault_node)
                            action = "SIGKILL follower"
                        else:
                            fault_node = next(i for i in cluster.nodes if i != current)
                            cluster.pause(fault_node)
                            action = "SIGSTOP follower"
                        fault["events"].append({"unix_ns": time.time_ns(), "controller_seconds": elapsed, "node": fault_node, "action": action})
                        injected = True
                    if (injected and not restored and options.scenario != "snapshot-catchup"
                            and elapsed >= 2 * options.duration / 3):
                        if options.scenario in ("leader-loss", "snapshot-catchup"):
                            cluster.start_node(fault_node)
                            action = "restart same data directory"
                        else:
                            cluster.resume(fault_node)
                            action = "SIGCONT follower"
                        fault["events"].append({"unix_ns": time.time_ns(), "controller_seconds": elapsed, "node": fault_node, "action": action})
                        restored = True
                    time.sleep(min(sample_interval_seconds, 0.1))
                if load.returncode:
                    raise RuntimeError(f"load generator exited {load.returncode}; inspect load.log")
            finally:
                sampler.stop()
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
        if options.scenario == "snapshot-catchup" and injected and not restored:
            current = protocol.leader(cluster.nodes)
            leader_status = protocol.request(cluster.nodes[current], {"op": "status"})
            required_snapshot_index = leader_status["info"]["snapshot_compaction"]["current_index"]
            if required_snapshot_index <= stopped_application_index:
                raise RuntimeError(
                    "leader did not publish a snapshot beyond the stopped follower floor"
                )
            cluster.start_node(fault_node)
            action = "restart same data directory"
            fault["events"].append({"unix_ns": time.time_ns(),
                "controller_seconds": time.monotonic() - start,
                "node": fault_node, "action": action})
            fault["snapshot_catchup"] = {
                "stopped_application_index": stopped_application_index,
                "required_snapshot_index": required_snapshot_index,
            }
            deadline = time.monotonic() + 30
            while True:
                try:
                    follower = protocol.request(
                        cluster.nodes[fault_node], {"op": "status"}, timeout=1
                    )
                except OSError:
                    follower = {}
                info = follower.get("info", {})
                snapshot = info.get("snapshot_compaction", {})
                application = info.get("application", {})
                if (snapshot.get("application_installs", 0) > 0
                        and snapshot.get("current_index", 0) >= required_snapshot_index
                        and application.get("applied_index", 0) >= required_snapshot_index):
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("restarted follower did not install the required snapshot")
                time.sleep(.05)
            restored = True
        if injected and not restored:
            if options.scenario in ("leader-loss", "snapshot-catchup"):
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
        if (getattr(options, "diagnostics", False)
                or (getattr(options, "pipelined_durability", False)
                    and options.scenario == "durable-kv")
                or getattr(options, "snapshot_interval_entries", 0)
                or observe_storage_footprint):
            after_load = protocol.complete_statuses(cluster.nodes)
        if observe_storage_footprint:
            write_json(directory / "after-measurement-status.json", after_load)
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
        if getattr(options, "snapshot_interval_entries", 0):
            from .reclamation_service import activity as reclamation_activity
            write_json(directory / "snapshot-after-scenario.json", after_load)
            reclamation = reclamation_activity(
                before, after_load, options.snapshot_interval_entries, options.scenario,
                restarted_node=fault_node if options.scenario == "snapshot-catchup" else None,
                stopped_application_index=(
                    fault.get("snapshot_catchup", {}).get("stopped_application_index")
                ),
                required_snapshot_index=(
                    fault.get("snapshot_catchup", {}).get("required_snapshot_index")
                ),
                native_snapshot_stages=getattr(options, "diagnostics", False),
                require_native_snapshot_stage_activity=getattr(
                    options, "diagnostics", False
                ),
            )
            write_json(directory / "snapshot-compaction-activity.json", reclamation)
            if reclamation["status"] != "passed":
                raise RuntimeError(
                    f"live snapshot/reclamation activity did not pass: {reclamation['failures']}"
                )
        footprint_after_load = (
            capture_storage_footprint(data) if observe_storage_footprint else None
        )
        post = canaries(cluster.nodes, case_id + "post")
        write_json(directory / "after.json", protocol.complete_statuses(cluster.nodes))
        cluster.stop_all()
        restart_started = time.monotonic_ns()
        cluster.start(initialize=False)
        process_restart_ns = time.monotonic_ns() - restart_started
        confirmation_started = time.monotonic_ns()
        recovery = confirm_canaries(cluster.nodes, pre + post, case_id + "final")
        recovery["timing"] = {
            "process_restart_ns": process_restart_ns,
            "canary_confirmation_ns": time.monotonic_ns() - confirmation_started,
            "scope": (
                "controller monotonic time; same-host process restart and finite canary reads, "
                "not power-loss recovery"
            ),
        }
        recovery["scope"] = "24 pre/post-load acknowledged canaries survived process restarts; finite check, not all measured writes or power loss"
        write_json(directory / "recovery.json", recovery)
        if observe_storage_footprint:
            # A status response is an owner-thread barrier. With no further client
            # writes, all snapshot maintenance triggered by the final canaries has
            # completed before the filesystem inventory is observed.
            write_json(
                directory / "after-final-restart.json",
                protocol.complete_statuses(cluster.nodes),
            )
            write_json(
                directory / "storage-footprint.json",
                storage_footprint_receipt(
                    footprint_before,
                    footprint_after_load,
                    capture_storage_footprint(data),
                ),
            )
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

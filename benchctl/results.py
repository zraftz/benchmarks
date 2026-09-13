"""Normalize benchmark artifacts without changing their evidence boundaries."""
from __future__ import annotations

from collections import defaultdict
import json
import math
from pathlib import Path
import re
from statistics import median
from typing import Any

from .evidence import CONTRACT, digest, verify
from .feature_coverage import verdict as feature_coverage_verdict
from .suite_plan import execution_plan


def _read(path: Path) -> Any:
    return json.loads(path.read_text())


def _is_feature_coverage_failure(item: dict) -> bool:
    error = item.get("error", "")
    return item.get("case", "").startswith("suite:") and (
        "completion priority" in error
        or "completion-priority" in error
        or "combined peer/proposal" in error
    )


def _mode(options: dict) -> str:
    if options.get("pipelined_durability"):
        return "pipeline"
    if options.get("peer_message_stream"):
        return "messages"
    if options.get("ordered_apply"):
        return "worker"
    return "inline"


def _cpu_model(host: dict) -> str | None:
    text = host.get("cpu", {}).get("stdout", "")
    for line in text.splitlines():
        if line.strip().startswith("Model name:"):
            return line.split(":", 1)[1].strip()
    return None


def _filesystem(host: dict) -> dict:
    try:
        rows = json.loads(host.get("filesystem", {}).get("stdout", "{}"))["filesystems"]
        row = rows[0]
        return {key: row.get(key) for key in ("source", "fstype", "options")}
    except (json.JSONDecodeError, KeyError, IndexError, TypeError):
        return {"source": None, "fstype": None, "options": None}


def _environment(manifest: dict) -> dict:
    host = manifest.get("host", {})
    return {
        "topology": manifest.get("topology"),
        "platform": host.get("platform"),
        "machine": host.get("machine"),
        "logical_cpus": host.get("logical_cpus"),
        "cpu_model": _cpu_model(host),
        "filesystem": _filesystem(host),
        "filesystem_space": host.get("filesystem_space"),
        "benchmark_source_digest": manifest.get("source_digest"),
        "load_generator_sha256": manifest.get("loadgen_sha256"),
    }


def _engine(manifest: dict) -> dict:
    name = manifest["implementation"]
    pin = manifest["implementation_pins"].get(name, {})
    version = pin.get("rev") or pin.get("version") or "unresolved"
    return {"name": name, "version": version}


def _display_name(manifest: dict) -> str:
    engine = manifest["implementation"]
    if engine != "rafter":
        if engine == "openraft":
            return ("OpenRaft async flusher" if manifest.get("options", {}).get("openraft_async_flush")
                    else "OpenRaft synchronous")
        return {"raft-rs": "raft-rs"}.get(engine, engine)
    mode = _mode(manifest.get("options", {}))
    if mode == "pipeline":
        priority = manifest.get("options", {}).get("durable_completion_priority", False)
        return "Rafter pipeline + completion priority" if priority else "Rafter pipeline FIFO"
    return {
        "messages": "Rafter synchronous messages",
        "worker": "Rafter ordered worker",
        "inline": "Rafter inline",
    }[mode]


def _workload(manifest: dict, measurement: dict | None) -> dict:
    options = manifest.get("options", {})
    config = (measurement or {}).get("config", {})
    return {
        "scenario": manifest.get("scenario"),
        "offered_per_second": config.get("rate", options.get("rate")),
        "payload_bytes": config.get("payload_bytes", options.get("payload")),
        "concurrency": config.get("concurrency", options.get("concurrency")),
        "read_percent": config.get("read_percent", options.get("read_percent")),
        "cas_percent": config.get("cas_percent", options.get("cas_percent")),
        "duration_seconds": config.get("duration_seconds", options.get("duration")),
        "warmup_seconds": options.get("warmup"),
        "network_delay_ms": options.get("network_delay_ms", 0),
    }


def _normalize_case(case: Path, root: Path) -> dict:
    manifest = _read(case / "manifest.json")
    measurement = _read(case / "measurement.json") if (case / "measurement.json").exists() else None
    checked = verify(case)
    options = manifest.get("options", {})
    errors = list(checked.get("errors", []))
    if measurement is not None and measurement.get("contract") != CONTRACT:
        errors.append("unsupported service completion boundary")
    qualification = "passed" if checked["status"] == "passed" and not errors else "failed"
    metrics = None
    accounting = None
    if measurement is not None:
        metrics = {
            "throughput_ops_s": measurement.get("successful_ops_per_second"),
            "client_p99_ms": measurement.get("success_latency", {}).get("p99_ms"),
            "client_p999_ms": measurement.get("success_latency", {}).get("p999_ms"),
            "execution_p99_ms": measurement.get("success_execution_latency", {}).get("p99_ms"),
            "execution_p999_ms": measurement.get("success_execution_latency", {}).get("p999_ms"),
            "client_start_p99_ms": measurement.get("worker_start_lateness", {}).get("p99_ms"),
            "scheduler_dispatch_p99_ms": measurement.get("scheduler_lateness", {}).get("p99_ms"),
            "scheduler_dispatch_p999_ms": measurement.get("scheduler_lateness", {}).get("p999_ms"),
        }
        accounting = {key: measurement.get(key) for key in
                      ("offered", "attempted", "ok", "completed_in_window", "errors", "unknown", "not_issued")}
    receipt = manifest.get("build_receipt") or {}
    load_environment = (
        _read(case / "load-environment.json")
        if (case / "load-environment.json").exists()
        else None
    )
    return {
        "layer": "complete_service",
        "source_case": case.relative_to(root).as_posix(),
        "engine": _engine(manifest),
        "display_name": _display_name(manifest),
        "configuration": {
            "variant": options.get("variant", manifest["implementation"]),
            "mode": _mode(options) if manifest["implementation"] == "rafter" else "public_api",
            "hard_state": receipt.get("rafter_hard_state_backend") if manifest["implementation"] == "rafter" else "adapter_journal",
            "peer_batch_size": options.get("peer_batch_size", 1),
            "client_batch_size": options.get("batch_size"),
            "max_speculative_proposals": options.get("max_speculative_proposals", 1),
            "max_inflight_appends": options.get("max_inflight_appends", 8),
            "combine_peer_proposals": bool(options.get("combine_peer_proposals", False)),
            "durable_completion_priority": bool(options.get("durable_completion_priority", False)),
            "openraft_async_flush": bool(options.get("openraft_async_flush", False)),
        },
        "workload": _workload(manifest, measurement),
        "completion_boundary": (measurement or {}).get("contract", CONTRACT),
        "environment": _environment(manifest),
        "measurement_mode": "diagnostic" if options.get("diagnostics") else "timing",
        "metric_definitions": {
            "throughput_ops_s": "successful logical operations completed inside the measurement window per second",
            "client_p99_ms": "scheduled arrival to successful client completion; upper-bound histogram p99",
            "client_p999_ms": "scheduled arrival to successful client completion; upper-bound histogram p99.9",
            "execution_p99_ms": "client request start to successful completion; upper-bound histogram p99",
            "execution_p999_ms": "client request start to successful completion; upper-bound histogram p99.9",
            "client_start_p99_ms": "scheduled arrival to client request start; upper-bound histogram p99",
            "scheduler_dispatch_p99_ms": "scheduled arrival to load-generator dispatch attempt; upper-bound histogram p99",
            "scheduler_dispatch_p999_ms": "scheduled arrival to load-generator dispatch attempt; upper-bound histogram p99.9",
            "aggregate": "median of per-repetition values; percentiles are not pooled",
        },
        "repetition": options.get("seed"),
        "smoke": bool(manifest.get("smoke")),
        "qualification": qualification,
        "qualification_errors": errors,
        "verdicts": checked.get("verdicts", {
            "evidence_integrity": {"status": qualification, "errors": errors},
            "correctness_checks": {"status": qualification, "errors": errors},
        }),
        "metrics": metrics,
        "accounting": accounting,
        "load_environment": load_environment,
        "recovery": _read(case / "recovery.json") if (case / "recovery.json").exists() else None,
        "history_check": _read(case / "qualification.json") if (case / "qualification.json").exists() else None,
    }


def _expected_durable_cases(suite: dict) -> int:
    if "arms" in suite:
        delays = len(suite.get("network_delays_ms", [0]))
        timing = delays * len(suite["arms"]) * len(suite["rates"]) * suite["runs"]
        diagnostic_arms = len(suite.get("diagnostic_arms", [])) or min(3, len(suite["arms"]))
        diagnostic_rates = len(suite.get("diagnostic_rates", suite["rates"]))
        diagnostics = delays * diagnostic_arms * diagnostic_rates
        return timing + diagnostics
    return len(suite["implementations"]) * len(suite["rates"]) * suite["runs"]


def _execution_plan_errors(directory: Path, suite: dict) -> list[str]:
    recorded = suite.get("execution_plan")
    if suite.get("schema", 1) < 3:
        return ["unexpected execution plan on legacy suite"] if recorded is not None else []
    errors = []
    try:
        expected = execution_plan(suite)
    except (KeyError, TypeError, ValueError) as error:
        return [f"execution plan could not be derived: {error}"]
    if recorded != expected:
        errors.append("recorded execution plan differs from suite declaration")
    manifests = {path.parent.name: path for path in directory.glob("*/manifest.json")}
    expected_names = {item["name"] for item in expected}
    if set(manifests) != expected_names:
        errors.append("primary case inventory differs from execution plan")
        return errors
    for item in expected:
        manifest = _read(manifests[item["name"]])
        if manifest.get("implementation") != item["implementation"]:
            errors.append(f"case implementation differs from plan: {item['name']}")
        if manifest.get("scenario") != "durable-kv" or manifest.get("smoke") is not False:
            errors.append(f"case classification differs from plan: {item['name']}")
        if manifest.get("options") != item["options"]:
            errors.append(f"case options differ from plan: {item['name']}")
    return errors


def _load_machine_qualification(directory: Path, suite: dict) -> tuple[dict | None, list[str]]:
    declared = suite.get("machine_qualification")
    receipt_path = directory / "machine-qualification.json"
    if declared is None:
        return (None, ["undeclared machine qualification receipt"] if receipt_path.exists() else [])
    errors = []
    receipt = None
    try:
        receipt = _read(receipt_path)
        if receipt != declared:
            errors.append("suite machine qualification differs from its receipt")
        if not isinstance(receipt, dict):
            return receipt, errors + ["machine qualification receipt is not an object"]
        expected_keys = {"schema", "status", "profile", "seal_sha256", "data_root", "verification"}
        if set(receipt) != expected_keys or receipt.get("schema") != 1:
            errors.append("unsupported machine qualification receipt")
        profile_name = receipt.get("profile")
        if profile_name != "machine-profile":
            errors.append("machine profile path is not the fixed suite-local directory")
        else:
            profile = directory / profile_name
            checked = verify(profile)
            if not isinstance(checked, dict):
                return receipt, errors + ["machine profile replay verdict is not an object"]
            if checked.get("status") != "passed" or receipt.get("status") != "passed":
                errors.append("machine qualification did not pass replay")
            if receipt.get("verification") != checked.get("verdicts"):
                errors.append("machine qualification verification differs from replay")
            if receipt.get("seal_sha256") != digest(profile / "SHA256SUMS.json"):
                errors.append("machine profile seal digest differs")
        data_root = suite.get("data_root")
        if not isinstance(data_root, str) or receipt.get("data_root") != str(Path(data_root).parent):
            errors.append("machine qualification data root differs from case storage root")
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        errors.append(f"machine qualification could not be replayed: {error}")
    return receipt, errors


def load_durable_suite(directory: Path) -> dict:
    directory = directory.resolve()
    suite = _read(directory / "suite.json")
    completion = _read(directory / "completion.json")
    cases = [_normalize_case(path.parent, directory) for path in sorted(directory.glob("*/manifest.json"))]
    legacy_coverage_messages = []
    runner_failures = []
    for item in completion.get("failures", []):
        if _is_feature_coverage_failure(item):
            legacy_coverage_messages.append(item)
        else:
            runner_failures.append(item)
    integrity_reasons = [f"runner failure: {item}" for item in runner_failures]
    correctness_reasons = []
    integrity_reasons.extend(_execution_plan_errors(directory, suite))
    machine_qualification, machine_errors = _load_machine_qualification(directory, suite)
    integrity_reasons.extend(machine_errors)
    expected = _expected_durable_cases(suite)
    if len(cases) != expected:
        integrity_reasons.append(f"expected {expected} primary cases, found {len(cases)}")
    priority_variants = {case["configuration"]["variant"] for case in cases
                         if case["configuration"]["durable_completion_priority"]}
    combined_variants = {case["configuration"]["variant"] for case in cases
                         if case["configuration"]["combine_peer_proposals"]}
    feature_coverage = feature_coverage_verdict(
        directory, completion_priority=priority_variants, combined=combined_variants
    )
    if legacy_coverage_messages:
        feature_coverage["historical_runner_messages"] = legacy_coverage_messages
    recorded_coverage = completion.get("feature_coverage")
    if recorded_coverage is not None and recorded_coverage != feature_coverage:
        integrity_reasons.append("recorded feature-coverage verdict differs from retained receipts")
    smokes = []
    for smoke_suite in sorted(directory.glob("worker-smoke-*")):
        smoke_completion = _read(smoke_suite / "completion.json")
        smoke_cases = [_normalize_case(path.parent, directory)
                       for path in sorted(smoke_suite.glob("*/manifest.json"))]
        status = "passed" if not smoke_completion.get("failures") and smoke_cases and all(
            case["qualification"] == "passed" for case in smoke_cases) else "failed"
        smokes.append({
            "scenario": smoke_suite.name.removeprefix("worker-smoke-"),
            "status": status,
            "source_cases": [case["source_case"] for case in smoke_cases],
            "failures": smoke_completion.get("failures", []),
        })
        cases.extend(smoke_cases)
    integrity_failed = [case["source_case"] for case in cases
                        if case["verdicts"]["evidence_integrity"]["status"] != "passed"]
    if integrity_failed:
        integrity_reasons.append(f"evidence-integrity failures: {', '.join(integrity_failed)}")
    correctness_failed = [case["source_case"] for case in cases
                          if case["verdicts"]["correctness_checks"]["status"] != "passed"]
    if correctness_failed:
        correctness_reasons.append(f"correctness-check failures: {', '.join(correctness_failed)}")
    failed_smokes = [smoke["scenario"] for smoke in smokes if smoke["status"] != "passed"]
    if failed_smokes:
        correctness_reasons.append(f"failed recovery smoke: {', '.join(failed_smokes)}")
    reasons = integrity_reasons + correctness_reasons
    return {
        "kind": "durable_service",
        "directory": str(directory),
        "suite": suite,
        "qualification": "passed" if not reasons else "failed",
        "qualification_errors": reasons,
        "verdicts": {
            "evidence_integrity": {
                "status": "passed" if not integrity_reasons else "failed",
                "errors": integrity_reasons,
                "scope": "expected cases, seals, identities, receipts, and accounting",
            },
            "correctness_checks": {
                "status": "passed" if not correctness_reasons else "failed",
                "errors": correctness_reasons,
                "scope": "finite history, restart, and smoke checks; not exhaustive proof",
            },
            "feature_coverage": feature_coverage,
            "environment_qualification": {
                "status": ("not measured" if machine_qualification is None else
                           "failed" if machine_errors else "passed"),
                "errors": machine_errors,
                "scope": "sealed idle/storage profile immediately after builds; per-case pressure remains separate",
            },
        },
        "cases": cases,
        "smokes": smokes,
        "machine_qualification": machine_qualification,
    }


def _median_metric(samples: list[dict], name: str) -> dict:
    values = [sample["metrics"][name] for sample in samples]
    if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in values):
        raise ValueError(f"invalid durable metric: {name}")
    return {"median": median(values), "min": min(values), "max": max(values)}


def aggregate_durable(data: dict) -> list[dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for case in data["cases"]:
        if case["smoke"] or case["measurement_mode"] != "timing":
            continue
        identity = {key: case[key] for key in (
            "layer", "engine", "display_name", "configuration", "workload",
            "completion_boundary", "environment", "measurement_mode", "metric_definitions")}
        groups[json.dumps(identity, sort_keys=True)].append(case)
    expected = data["suite"]["runs"]
    rows = []
    for encoded, samples in sorted(groups.items()):
        identity = json.loads(encoded)
        reasons = []
        if data["qualification"] != "passed":
            reasons.extend(data["qualification_errors"])
        if len(samples) != expected:
            reasons.append(f"expected {expected} repetitions, found {len(samples)}")
        if any(sample["qualification"] != "passed" or sample["metrics"] is None for sample in samples):
            reasons.append("one or more source cases failed verification")
        row = {
            **identity,
            "repetitions": len(samples),
            "expected_repetitions": expected,
            "qualification": "failed" if reasons else "passed",
            "qualification_errors": reasons,
            "source_cases": [sample["source_case"] for sample in samples],
            "metrics": {},
            "accounting": {},
        }
        if not reasons:
            row["metrics"] = {name: _median_metric(samples, name) for name in
                              ("throughput_ops_s", "client_p99_ms", "client_p999_ms",
                               "execution_p99_ms", "execution_p999_ms", "client_start_p99_ms",
                               "scheduler_dispatch_p99_ms", "scheduler_dispatch_p999_ms")}
            row["accounting"] = {key: sum(sample["accounting"][key] for sample in samples)
                                 for key in ("offered", "attempted", "ok", "completed_in_window",
                                             "errors", "unknown", "not_issued")}
        rows.append(row)
    return rows


def load_microbench(directory: Path) -> dict:
    directory = directory.resolve()
    checked = verify(directory)
    provenance = _read(directory / "provenance.json")
    if not (directory / "report.json").exists():
        return {"kind": "consensus_in_memory", "directory": str(directory),
                "qualification": "failed", "qualification_errors": checked.get("errors", []),
                "records": [], "source_cases": [name for name in ("provenance.json", "failure.json")
                                                  if (directory / name).exists()]}
    report = _read(directory / "report.json")
    records = []
    for row in report["results"]:
        sources = []
        reported_versions = set()
        for run in report["runs"]:
            for binary, raw in zip(run["execution_order"], run["results"], strict=True):
                if raw["library"] == row["library"] and any(
                        workload["name"] == row["workload"] for workload in raw["workloads"]):
                    sources.append(f"run-{run['run']}-{binary}.json")
                    reported_versions.add(raw.get("version", "unresolved"))
        version = report["rafter"]["rev"] if row["library"] == "rafter" else ", ".join(sorted(reported_versions))
        records.append({
            "layer": "consensus_in_memory",
            "engine": {"name": row["library"], "version": version},
            "display_name": {"rafter": "Rafter", "openraft": "OpenRaft", "raft-rs": "raft-rs"}.get(row["library"], row["library"]),
            "configuration": {"storage": "memory", "transport": "in_process"},
            "workload": {key: row[key] for key in ("workload", "proposals", "payload_bytes", "max_in_flight")},
            "completion_boundary": row["completion"],
            "environment": {
                "topology": "three voters in one process",
                "platform": provenance.get("host", {}).get("platform"),
                "machine": provenance.get("host", {}).get("machine"),
                "logical_cpus": provenance.get("host", {}).get("logical_cpus"),
                "benchmark_source_digest": provenance.get("source_digest"),
            },
            "measurement_mode": "timing",
            "metric_definitions": {
                "throughput_ops_s": "proposals completed per elapsed benchmark second",
                "client_p99_us": "submission to shared leader-side reference application completion; median per-run p99",
                "aggregate": "median and range across isolated process repetitions; percentiles are not pooled",
            },
            "repetitions": row["runs"],
            "qualification": checked["status"],
            "qualification_errors": checked.get("errors", []),
            "metrics": {
                "throughput_ops_s": row["proposals_per_s"],
                "client_p99_us": {"median": row["p99_us"]},
            },
            "source_cases": sources,
        })
    return {"kind": "consensus_in_memory", "directory": str(directory),
            "qualification": checked["status"], "records": records}


def load_storage_comparison(directory: Path) -> dict:
    """Recompute the paired storage aggregate from every retained raw repetition."""
    directory = directory.resolve()
    metadata = _read(directory / "metadata.json")
    recorded = _read(directory / "results.json")
    arms = ("baseline", "candidate")
    workloads = tuple(key for key in recorded["results"]["baseline"] if key.startswith("batch-"))
    expected_stems = {f"run-{run}-{workload}-{arm}" for run in range(1, metadata["runs"] + 1)
                      for workload in workloads + ("snapshot",) for arm in arms}
    reasons = []
    execution_order = _read(directory / "execution-order.json")
    if len(execution_order) != len(expected_stems) or set(execution_order) != expected_stems:
        reasons.append("execution order does not cover the expected cases exactly once")
    for stem in expected_stems:
        raw_path = directory / f"{stem}.json"
        execution_path = directory / f"{stem}.execution.json"
        if not raw_path.is_file() or not execution_path.is_file():
            reasons.append(f"missing storage source receipt: {stem}")
            continue
        execution = _read(execution_path)
        if execution.get("exit_code") != 0 or execution.get("timed_out") is not False:
            reasons.append(f"storage source execution failed: {stem}")
    if metadata["binaries"]["baseline"]["sha256"] != metadata["binaries"]["candidate"]["sha256"]:
        reasons.append("storage comparison did not use one identical binary")
    records = []
    for workload in sorted(workloads, key=lambda item: int(item.split("-")[1])):
        batch_size = int(workload.split("-")[1])
        for arm in arms:
            samples = []
            sources = []
            for run in range(1, metadata["runs"] + 1):
                path = directory / f"run-{run}-{workload}-{arm}.json"
                if not path.is_file():
                    continue
                raw = _read(path)
                sample = raw["workloads"][0]
                sources.append(path.name)
                if raw.get("harness") != "rafter-bench-cluster" or sample.get("batch_size") != batch_size:
                    reasons.append(f"invalid storage source case: {path.name}")
                if raw.get("hard_state") != metadata["hard_state"][arm]:
                    reasons.append(f"storage backend mismatch: {path.name}")
                samples.append(sample)
            if len(samples) != metadata["runs"]:
                reasons.append(f"missing storage repetitions: {arm}/{workload}")
                continue
            aggregate = recorded["results"][arm][workload]
            throughput = median(sample["proposals_per_s"] for sample in samples)
            batch_p99 = median(sample["batch_completion_latency_ms"]["p99"] for sample in samples)
            if not math.isclose(throughput, aggregate["median"]["proposals_per_s"], rel_tol=0, abs_tol=1e-9):
                reasons.append(f"throughput aggregate mismatch: {arm}/{workload}")
            if not math.isclose(batch_p99, aggregate["median"]["batch_completion_latency_ms"]["p99"], rel_tol=0, abs_tol=1e-9):
                reasons.append(f"batch p99 aggregate mismatch: {arm}/{workload}")
            records.append({
                "layer": "durable_replication",
                "engine": {"name": "rafter", "version": metadata["commit"]},
                "display_name": {"replace": "File replacement", "journal": "Append-only journal", "wal": "Combined WAL"}[metadata["hard_state"][arm]],
                "configuration": {"hard_state": metadata["hard_state"][arm], "arm": arm},
                "workload": {"kind": "proposal_batches", "batch_size": batch_size,
                             "payload_bytes": samples[0]["payload_bytes"], "batches": metadata["batches"]},
                "completion_boundary": "rafter-bench-cluster batch completion after durable Raft state publication",
                "environment": {"machine": metadata["machine"], "cpu_affinity": metadata["cpu_affinity"]},
                "measurement_mode": "timing",
                "metric_definitions": {
                    "throughput_ops_s": "durable proposal entries completed per elapsed benchmark second",
                    "batch_completion_p99_ms": "proposal-batch submission to completion; median per-run p99",
                    "aggregate": "median and range across paired repetitions; percentiles are not pooled",
                },
                "repetitions": len(samples),
                "qualification": "passed",
                "qualification_errors": [],
                "metrics": {
                    "throughput_ops_s": {"median": throughput,
                                         "min": min(sample["proposals_per_s"] for sample in samples),
                                         "max": max(sample["proposals_per_s"] for sample in samples)},
                    "batch_completion_p99_ms": {"median": batch_p99,
                                                "min": min(sample["batch_completion_latency_ms"]["p99"] for sample in samples),
                                                "max": max(sample["batch_completion_latency_ms"]["p99"] for sample in samples)},
                },
                "source_cases": sources,
            })
    probes = {}
    for backend in set(metadata["hard_state"].values()):
        path = directory / f"{backend}-syscalls.json"
        if path.exists():
            probe = _read(path)
            writes = probe.get("writes", 0)
            probes[backend] = {
                "hard_state_publications": writes,
                "sync_calls": probe.get("fdatasync", 0) + probe.get("fsync", 0),
                "sync_calls_per_publication": ((probe.get("fdatasync", 0) + probe.get("fsync", 0)) / writes) if writes else None,
                "scope": "fixed hard-state syscall probe; not synchronization calls per committed entry",
                "source_case": path.name,
            }
    if reasons:
        for record in records:
            record["qualification"] = "failed"
            record["qualification_errors"] = reasons
    return {
        "kind": "durable_replication",
        "directory": str(directory),
        "qualification": "failed" if reasons else "passed",
        "qualification_errors": reasons,
        "records": records,
        "sync_probes": probes,
    }


_WAL_RECLAMATION_RETAINED = (10_000, 100_000)
_WAL_RECLAMATION_PHASES = {
    "wal_reclamation",
    "wal_checkpoint_prepare",
    "wal_manifest_publish",
    "wal_cleanup",
}
_WAL_RECLAMATION_GENERATION = re.compile(
    r"^raft-wal-(checkpoint|segment)-(\d{20})\.(rfwc|rfwb)$"
)


def _require_wal(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _check_wal_latency(value: dict, expected_count: int, name: str) -> None:
    _require_wal(
        isinstance(value, dict) and set(value) == {"count", "p50_ns", "p99_ns", "max_ns"},
        f"{name} latency shape changed",
    )
    _require_wal(value["count"] == expected_count, f"{name} latency count disagrees")
    _require_wal(
        0 <= value["p50_ns"] <= value["p99_ns"] <= value["max_ns"],
        f"{name} latency ordering disagrees",
    )


def _check_wal_inventory(value: dict, generation: int, expected_bytes: int, name: str) -> None:
    _require_wal(
        isinstance(value, dict) and set(value) == {"file_count", "bytes", "names"},
        f"{name} inventory shape changed",
    )
    _require_wal(
        value["file_count"] == 3 and len(value["names"]) == 3,
        f"{name} did not retain exactly three managed WAL files",
    )
    _require_wal(value["bytes"] == expected_bytes and expected_bytes > 0,
                 f"{name} managed WAL bytes disagree")
    _require_wal(value["names"] == sorted(value["names"]), f"{name} names are not sorted")
    _require_wal(value["names"].count("raft-wal-current") == 1,
                 f"{name} has no unique authoritative manifest")
    generations = []
    kinds = []
    for filename in value["names"]:
        if filename == "raft-wal-current":
            continue
        match = _WAL_RECLAMATION_GENERATION.fullmatch(filename)
        _require_wal(match is not None, f"{name} has an unexpected managed filename")
        kinds.append(match.group(1))
        generations.append(int(match.group(2)))
    _require_wal(sorted(kinds) == ["checkpoint", "segment"],
                 f"{name} does not contain one checkpoint and one segment")
    _require_wal(generations == [generation, generation],
                 f"{name} files do not select generation {generation}")


def _normalize_wal_reclamation_receipt(raw: dict, retained_entries: int,
                                       source_cases: list[str]) -> tuple[dict, dict]:
    _require_wal(set(raw) == {
        "schema", "layer", "comparison_kind", "source_sha", "completion_boundary",
        "config", "rounds", "aggregate", "phase_metrics", "final",
    }, "top-level receipt shape changed")
    _require_wal(raw["schema"] == 1, "unsupported reclamation receipt schema")
    _require_wal(raw["layer"] == "durable_replication", "wrong evidence layer")
    _require_wal(raw["comparison_kind"] == "rafter_component_qualification",
                 "reclamation evidence was mislabeled as a competitor comparison")
    _require_wal(re.fullmatch(r"[0-9a-f]{40}", raw["source_sha"]) is not None,
                 "source identity is not a lowercase full SHA")
    config = raw["config"]
    expected_config = {
        "retained_entries": retained_entries,
        "rounds": 3,
        "batches_per_round": 64,
        "batch_size": 8,
        "payload_bytes": 64,
    }
    _require_wal(config == expected_config, "reclamation qualification configuration changed")
    rounds = raw["rounds"]
    _require_wal(isinstance(rounds, list) and len(rounds) == config["rounds"],
                 "round count disagrees")
    expected_appended = config["batches_per_round"] * config["batch_size"]
    aggregate = raw["aggregate"]
    _require_wal(set(aggregate) == {
        "write_latency", "reclamation_pause", "reopen_ns", "managed_wal_bytes",
        "managed_wal_files",
    }, "aggregate shape changed")
    expected_bytes = aggregate["managed_wal_bytes"]
    pauses = []
    for number, value in enumerate(rounds, 1):
        _require_wal(set(value) == {
            "round", "appended_entries", "compacted_through", "snapshot_ns",
            "reclamation_pause_ns", "write_latency", "managed_wal",
        }, f"round {number} shape changed")
        _require_wal(value["round"] == number, f"round {number} sequence disagrees")
        _require_wal(value["appended_entries"] == expected_appended,
                     f"round {number} append accounting disagrees")
        _require_wal(value["compacted_through"] == number * expected_appended,
                     f"round {number} compaction boundary disagrees")
        _require_wal(value["snapshot_ns"] > 0 and value["reclamation_pause_ns"] > 0,
                     f"round {number} timing is absent")
        _check_wal_latency(value["write_latency"], config["batches_per_round"],
                           f"round {number} writes")
        _check_wal_inventory(value["managed_wal"], number, expected_bytes, f"round {number}")
        pauses.append(value["reclamation_pause_ns"])
    _check_wal_latency(
        aggregate["write_latency"],
        config["rounds"] * config["batches_per_round"],
        "aggregate writes",
    )
    _check_wal_latency(aggregate["reclamation_pause"], config["rounds"],
                       "aggregate reclamation")
    _require_wal(aggregate["reopen_ns"] > 0, "reopen timing is absent")
    _require_wal(aggregate["managed_wal_files"] == 3 and expected_bytes > 0,
                 "aggregate physical WAL bound disagrees")

    metrics = raw["phase_metrics"]
    _require_wal(
        isinstance(metrics, list) and len(metrics) == 4
        and {metric.get("name") for metric in metrics} == _WAL_RECLAMATION_PHASES,
        "reclamation phase inventory disagrees",
    )
    by_name = {}
    for metric in metrics:
        _require_wal(set(metric) == {"name", "calls", "total_ns", "max_ns", "buckets"},
                     "phase metric shape changed")
        _require_wal(metric["calls"] == config["rounds"],
                     f"{metric['name']} call count disagrees")
        _require_wal(0 < metric["max_ns"] <= metric["total_ns"],
                     f"{metric['name']} timing is incoherent")
        _require_wal(sum(metric["buckets"]) == metric["calls"],
                     f"{metric['name']} histogram accounting disagrees")
        by_name[metric["name"]] = metric
    overall = by_name["wal_reclamation"]["total_ns"]
    _require_wal(
        sum(by_name[name]["total_ns"] for name in
            _WAL_RECLAMATION_PHASES - {"wal_reclamation"}) <= overall,
        "nested reclamation phases exceed the overall timer",
    )
    _require_wal(overall <= sum(pauses),
                 "reclamation telemetry exceeds synchronous pauses")

    final = raw["final"]
    _require_wal(set(final) == {
        "hard_commit", "compacted_through", "retained_entries", "next_index",
        "recovery_verified", "physical_bound_verified", "managed_wal",
    }, "final verification shape changed")
    expected_total = retained_entries + config["rounds"] * expected_appended
    _require_wal(final["hard_commit"] == expected_total
                 and final["next_index"] == expected_total + 1,
                 "final hard state or log boundary disagrees")
    _require_wal(final["compacted_through"] == config["rounds"] * expected_appended,
                 "final compaction boundary disagrees")
    _require_wal(final["retained_entries"] == retained_entries,
                 "final retained suffix length disagrees")
    _require_wal(final["recovery_verified"] is True
                 and final["physical_bound_verified"] is True,
                 "final recovery or physical bound was not verified")
    _check_wal_inventory(final["managed_wal"], config["rounds"], expected_bytes, "reopen")

    derived_summary = {
        "schema": 1,
        "source_sha": raw["source_sha"],
        "retained_entries": retained_entries,
        "rounds": config["rounds"],
        "managed_wal_bytes": expected_bytes,
        "reclamation_pause_p99_ns": aggregate["reclamation_pause"]["p99_ns"],
        "reopen_ns": aggregate["reopen_ns"],
        "qualified": True,
    }
    record = {
        "layer": "durable_replication",
        "comparison_kind": "rafter_component_qualification",
        "engine": {"name": "rafter", "version": raw["source_sha"]},
        "display_name": "Combined WAL physical reclamation",
        "configuration": dict(config),
        "workload": {
            "kind": "repeated_write_snapshot_compaction_reopen",
            "appended_entries_per_round": expected_appended,
        },
        "completion_boundary": raw["completion_boundary"],
        "environment": {
            "qualification": "not recorded by component receipt",
            "timings_publishable": False,
        },
        "measurement_mode": "diagnostic",
        "metric_definitions": {
            "managed_wal_bytes": "bytes in the authoritative manifest, checkpoint, and segment",
            "reclamation_pause_p99_ms": "diagnostic p99 across three synchronous component cycles",
            "reopen_ms": "diagnostic exact-suffix reopen duration",
        },
        "qualification": "passed",
        "qualification_errors": [],
        "metrics": {
            "managed_wal_bytes": expected_bytes,
            "managed_wal_files": aggregate["managed_wal_files"],
            "reclamation_pause_p99_ms": aggregate["reclamation_pause"]["p99_ns"] / 1_000_000,
            "reopen_ms": aggregate["reopen_ns"] / 1_000_000,
        },
        "verification": {
            "recovery_verified": True,
            "physical_bound_verified": True,
            "final_hard_commit": final["hard_commit"],
            "final_compacted_through": final["compacted_through"],
            "final_next_index": final["next_index"],
        },
        "source_cases": source_cases,
    }
    return record, derived_summary


def load_wal_reclamation(directory: Path) -> dict:
    """Replay the exact 10k/100k component receipts without publishing runner timing."""
    original = directory.absolute()
    reasons = []
    if original.is_symlink() or not original.is_dir():
        return {
            "kind": "wal_reclamation",
            "directory": str(original),
            "qualification": "failed",
            "qualification_errors": ["WAL reclamation evidence is not a plain directory"],
            "records": [],
        }
    directory = original.resolve()
    expected_names = {
        f"retained-{retained}{suffix}"
        for retained in _WAL_RECLAMATION_RETAINED
        for suffix in (".json", "-summary.json")
    }
    entries = list(directory.iterdir())
    if any(path.is_symlink() for path in entries):
        reasons.append("symlink in WAL reclamation evidence")
    actual_names = {path.name for path in entries}
    if actual_names != expected_names:
        reasons.append("WAL reclamation evidence file set changed from the exact 10k/100k receipts")

    records = []
    source_shas = set()
    for retained in _WAL_RECLAMATION_RETAINED:
        receipt_path = directory / f"retained-{retained}.json"
        summary_path = directory / f"retained-{retained}-summary.json"
        if not receipt_path.is_file() or not summary_path.is_file():
            continue
        try:
            raw = _read(receipt_path)
            record, expected_summary = _normalize_wal_reclamation_receipt(
                raw,
                retained,
                [receipt_path.name, summary_path.name],
            )
            expected_summary["receipt_sha256"] = digest(receipt_path)
            recorded_summary = _read(summary_path)
            _require_wal(recorded_summary == expected_summary,
                         f"retained-{retained} verifier summary disagrees")
            records.append(record)
            source_shas.add(record["engine"]["version"])
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            reasons.append(f"retained-{retained}: {error}")
    if len(source_shas) > 1:
        reasons.append("WAL reclamation receipts use different Rafter revisions")
    if len(records) != len(_WAL_RECLAMATION_RETAINED):
        reasons.append("WAL reclamation evidence did not normalize both retained-suffix shapes")
    if reasons:
        for record in records:
            record["qualification"] = "failed"
            record["qualification_errors"] = reasons
    return {
        "kind": "wal_reclamation",
        "directory": str(directory),
        "qualification": "failed" if reasons else "passed",
        "qualification_errors": reasons,
        "source_sha": next(iter(source_shas)) if len(source_shas) == 1 else None,
        "records": records,
    }

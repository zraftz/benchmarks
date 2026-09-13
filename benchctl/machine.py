"""Predeclared idle-host qualification for fixed-machine benchmark runs."""
from __future__ import annotations

from pathlib import Path
from itertools import pairwise
import json
import math
import os
import time
import uuid
from typing import Any

from .evidence import host_info, seal, source_digest, system_sample, write_json


OBJECTIVE = {
    "duration_seconds": 15.0,
    "sample_interval_seconds": 1.0,
    "minimum_available_bytes": 20 * 1024**3,
    "maximum_cpu_busy_percent": 5.0,
    "maximum_cpu_iowait_percent": 2.0,
    "maximum_cpu_steal_percent": 1.0,
    "maximum_cgroup_throttled_percent": 1.0,
    "maximum_cpu_pressure_some_percent": 2.0,
    "maximum_io_pressure_some_percent": 2.0,
    "maximum_io_pressure_full_percent": 0.5,
    "maximum_memory_pressure_some_percent": 0.5,
    "maximum_memory_pressure_full_percent": 0.1,
    "maximum_hardware_throttle_counter_delta": 0.0,
}
STORAGE_PROBE_SAMPLES = 16
LOAD_ENVIRONMENT_SCHEMA = 1


def _deltas(before: dict[str, Any] | None, after: dict[str, Any] | None) -> dict[str, int] | None:
    if not isinstance(before, dict) or not isinstance(after, dict):
        return None
    result = {}
    for name in before.keys() & after.keys():
        if type(before[name]) is int and type(after[name]) is int:
            delta = after[name] - before[name]
            if delta < 0:
                return None
            result[name] = delta
    return result or None


def _percent(value: int, total: int) -> float | None:
    return 100.0 * value / total if total > 0 else None


def summarize_idle(samples: list[dict[str, Any]], *, schema: int = 2) -> dict[str, Any]:
    if schema not in (1, 2):
        raise ValueError("unsupported machine profile schema")
    if len(samples) < 2:
        raise ValueError("machine profile requires at least two samples")
    elapsed_ns = samples[-1]["monotonic_ns"] - samples[0]["monotonic_ns"]
    if type(elapsed_ns) is not int or elapsed_ns <= 0:
        raise ValueError("machine profile timestamps did not advance")
    elapsed_seconds = elapsed_ns / 1_000_000_000
    gaps = [
        (right["monotonic_ns"] - left["monotonic_ns"]) / 1_000_000_000
        for left, right in pairwise(samples)
    ]
    if any(not math.isfinite(gap) or gap <= 0 for gap in gaps):
        raise ValueError("machine profile sample timestamps are not increasing")
    before = samples[0]["system"]
    after = samples[-1]["system"]

    cpu = _deltas(before.get("cpu_ticks"), after.get("cpu_ticks"))
    cpu_total = None if cpu is None else sum(
        value for name, value in cpu.items() if name not in ("guest", "guest_nice")
    )
    cpu_summary = None if cpu_total is None else {
        "ticks": cpu,
        "total_ticks": cpu_total,
        "busy_percent": _percent(
            cpu_total - cpu.get("idle", 0) - cpu.get("iowait", 0), cpu_total
        ),
        "iowait_percent": _percent(cpu.get("iowait", 0), cpu_total),
        "steal_percent": _percent(cpu.get("steal", 0), cpu_total),
    }

    cgroup = _deltas(before.get("cgroup_cpu"), after.get("cgroup_cpu"))
    cgroup_summary = None if cgroup is None else {
        "counters": cgroup,
        "throttled_percent_of_wall": 100.0
        * cgroup.get("throttled_usec", 0)
        / (elapsed_seconds * 1_000_000),
    }

    pressure = {}
    for resource in ("cpu", "io", "memory"):
        resource_before = before.get("pressure", {}).get(resource)
        resource_after = after.get("pressure", {}).get(resource)
        if not isinstance(resource_before, dict) or not isinstance(resource_after, dict):
            pressure[resource] = None
            continue
        rows = {}
        for kind in resource_before.keys() & resource_after.keys():
            start = resource_before[kind].get("total")
            finish = resource_after[kind].get("total")
            if type(start) is not int or type(finish) is not int or finish < start:
                continue
            delta = finish - start
            rows[kind] = {
                "stalled_microseconds": delta,
                "percent_of_wall": 100.0 * delta / (elapsed_seconds * 1_000_000),
            }
        pressure[resource] = rows or None

    devices = {}
    before_devices = before.get("block_devices")
    after_devices = after.get("block_devices")
    if isinstance(before_devices, dict) and isinstance(after_devices, dict):
        for name in before_devices.keys() & after_devices.keys():
            delta = _deltas(before_devices[name], after_devices[name])
            if delta is not None:
                devices[name] = delta
    else:
        devices = None

    hardware_throttles = _deltas(
        before.get("hardware_throttle_counts"),
        after.get("hardware_throttle_counts"),
    )
    hardware_throttling = None if hardware_throttles is None else {
        "counters": hardware_throttles,
        "changed_counters": sum(value > 0 for value in hardware_throttles.values()),
        "maximum_counter_delta": max(hardware_throttles.values(), default=0),
    }

    spaces = [sample["system"].get("filesystem_space") for sample in samples]
    available = [space["available_bytes"] for space in spaces if isinstance(space, dict)]
    filesystem = None if not available else {
        "minimum_available_bytes": min(available),
        "ending_available_bytes": available[-1],
    }
    supported = {
        "cpu_ticks": cpu_summary is not None,
        "cgroup_cpu": cgroup_summary is not None,
        "linux_pressure": all(pressure.get(name) is not None for name in ("cpu", "io", "memory")),
        "block_devices": devices is not None,
        "filesystem_space": filesystem is not None,
    }
    result = {
        "schema": schema,
        "sample_count": len(samples),
        "elapsed_seconds": elapsed_seconds,
        "maximum_sample_gap_seconds": max(gaps),
        "supported": supported,
        "cpu": cpu_summary,
        "cgroup_cpu": cgroup_summary,
        "pressure": pressure,
        "block_devices": devices,
        "filesystem": filesystem,
        "limitations": [
            "idle counters characterize this observation window, not future benchmark load",
            "host counters do not identify which process caused a stall",
            "raw block-device counters are retained without guessing filesystem device ancestry",
        ],
    }
    if schema >= 2:
        supported["hardware_throttle_counters"] = hardware_throttling is not None
        result["hardware_throttling"] = hardware_throttling
        result["limitations"].append(
            "hardware throttle counters are optional and may repeat one package event per CPU"
        )
    return result


def assess_idle(summary: dict[str, Any], objective: dict[str, float] = OBJECTIVE) -> dict[str, Any]:
    missing = []
    failures = []

    def check(path: str, value: float | None, maximum: float) -> None:
        if value is None or not math.isfinite(value):
            missing.append(path)
        elif value > maximum:
            failures.append(f"{path} {value:.3f} exceeds {maximum:.3f}")

    cpu = summary.get("cpu") or {}
    cgroup = summary.get("cgroup_cpu") or {}
    cpu_pressure = (summary.get("pressure") or {}).get("cpu") or {}
    io_pressure = (summary.get("pressure") or {}).get("io") or {}
    memory_pressure = (summary.get("pressure") or {}).get("memory") or {}
    filesystem = summary.get("filesystem") or {}
    elapsed = summary.get("elapsed_seconds")
    if not isinstance(elapsed, (int, float)) or not math.isfinite(elapsed):
        missing.append("elapsed_seconds")
    elif elapsed < objective["duration_seconds"]:
        failures.append(
            f"elapsed_seconds {elapsed:.3f} is below {objective['duration_seconds']:.3f}"
        )
    maximum_gap = summary.get("maximum_sample_gap_seconds")
    allowed_gap = objective["sample_interval_seconds"] * 1.5
    if not isinstance(maximum_gap, (int, float)) or not math.isfinite(maximum_gap):
        missing.append("maximum_sample_gap_seconds")
    elif maximum_gap > allowed_gap:
        failures.append(
            f"maximum_sample_gap_seconds {maximum_gap:.3f} exceeds {allowed_gap:.3f}"
        )
    if summary.get("schema", 1) >= 2:
        check(
            "cpu.busy_percent",
            cpu.get("busy_percent"),
            objective["maximum_cpu_busy_percent"],
        )
        check(
            "pressure.cpu.some.percent_of_wall",
            (cpu_pressure.get("some") or {}).get("percent_of_wall"),
            objective["maximum_cpu_pressure_some_percent"],
        )
        check(
            "pressure.memory.some.percent_of_wall",
            (memory_pressure.get("some") or {}).get("percent_of_wall"),
            objective["maximum_memory_pressure_some_percent"],
        )
        check(
            "pressure.memory.full.percent_of_wall",
            (memory_pressure.get("full") or {}).get("percent_of_wall"),
            objective["maximum_memory_pressure_full_percent"],
        )
        hardware = summary.get("hardware_throttling")
        if isinstance(hardware, dict):
            check(
                "hardware_throttling.maximum_counter_delta",
                hardware.get("maximum_counter_delta"),
                objective["maximum_hardware_throttle_counter_delta"],
            )
    check("cpu.iowait_percent", cpu.get("iowait_percent"), objective["maximum_cpu_iowait_percent"])
    check("cpu.steal_percent", cpu.get("steal_percent"), objective["maximum_cpu_steal_percent"])
    check(
        "cgroup_cpu.throttled_percent_of_wall",
        cgroup.get("throttled_percent_of_wall"),
        objective["maximum_cgroup_throttled_percent"],
    )
    check(
        "pressure.io.some.percent_of_wall",
        (io_pressure.get("some") or {}).get("percent_of_wall"),
        objective["maximum_io_pressure_some_percent"],
    )
    check(
        "pressure.io.full.percent_of_wall",
        (io_pressure.get("full") or {}).get("percent_of_wall"),
        objective["maximum_io_pressure_full_percent"],
    )
    available = filesystem.get("minimum_available_bytes")
    if type(available) is not int:
        missing.append("filesystem.minimum_available_bytes")
    elif available < objective["minimum_available_bytes"]:
        failures.append(
            f"filesystem.minimum_available_bytes {available} is below "
            f"{objective['minimum_available_bytes']}"
        )
    status = "failed" if failures else "not measured" if missing else "passed"
    return {
        "status": status,
        "objective": dict(objective),
        "failures": failures,
        "missing": missing,
        "scope": "predeclared idle-host observation only; benchmark load is assessed separately",
    }


def load_environment_receipt(
    samples: list[dict[str, Any]],
    *,
    expected_duration_seconds: float,
    nominal_sample_interval_seconds: float,
) -> dict[str, Any]:
    """Summarize measured-load context without turning resource use into a winner filter."""
    for name, value in (
        ("expected_duration_seconds", expected_duration_seconds),
        ("nominal_sample_interval_seconds", nominal_sample_interval_seconds),
    ):
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be finite and positive")
    summary = summarize_idle(samples, schema=2)
    minimum_elapsed = expected_duration_seconds * 0.95
    maximum_gap = max(0.5, nominal_sample_interval_seconds * 3)
    failures = []
    if summary["elapsed_seconds"] < minimum_elapsed:
        failures.append(
            f"elapsed_seconds {summary['elapsed_seconds']:.3f} is below {minimum_elapsed:.3f}"
        )
    if summary["maximum_sample_gap_seconds"] > maximum_gap:
        failures.append(
            "maximum_sample_gap_seconds "
            f"{summary['maximum_sample_gap_seconds']:.3f} exceeds {maximum_gap:.3f}"
        )
    return {
        "schema": LOAD_ENVIRONMENT_SCHEMA,
        "kind": "measured-load-environment-observation",
        "expected_duration_seconds": expected_duration_seconds,
        "nominal_sample_interval_seconds": nominal_sample_interval_seconds,
        "coverage": {
            "status": "failed" if failures else "passed",
            "minimum_elapsed_seconds": minimum_elapsed,
            "maximum_sample_gap_seconds": maximum_gap,
            "failures": failures,
        },
        "summary": summary,
        "interpretation": (
            "workload and unrelated host activity are both included; these diagnostics never "
            "remove, accept, or rank a performance result"
        ),
    }


def _storage_timing_summary(samples: list[dict[str, int]]) -> dict[str, dict[str, int]]:
    fields = ("write_sync_ns", "rename_directory_sync_ns", "delete_directory_sync_ns")
    summary = {}
    for field in fields:
        values = sorted(sample[field] for sample in samples)
        summary[field] = {
            "minimum_ns": values[0],
            "median_ns": values[(len(values) - 1) // 2],
            "p99_ns": values[math.ceil(len(values) * 0.99) - 1],
            "maximum_ns": values[-1],
        }
    return summary


def storage_probe_errors(receipt: dict[str, Any]) -> list[str]:
    if receipt.get("status") != "passed":
        return ["machine-profile storage publication probe did not pass"]
    if receipt.get("schema", 1) == 1:
        return []
    if receipt.get("schema") != 2:
        return ["machine-profile storage publication probe has unsupported schema"]
    samples = receipt.get("samples")
    if (
        not isinstance(samples, list)
        or receipt.get("sample_count") != STORAGE_PROBE_SAMPLES
        or len(samples) != STORAGE_PROBE_SAMPLES
    ):
        return ["machine-profile storage publication sample accounting differs"]
    if receipt.get("bytes") != 4096:
        return ["machine-profile storage publication payload size differs"]
    required = ("write_sync_ns", "rename_directory_sync_ns", "delete_directory_sync_ns")
    if not samples or any(
        type(sample) is not dict
        or any(type(sample.get(field)) is not int or sample[field] < 0 for field in required)
        for sample in samples
    ):
        return ["machine-profile storage publication timings are invalid"]
    if receipt.get("summary") != _storage_timing_summary(samples):
        return ["machine-profile storage publication summary differs from raw samples"]
    if receipt.get("timings") != samples[-1]:
        return ["machine-profile storage publication compatibility timing differs"]
    return []


def _storage_probe_once(data_root: Path, token: str, payload: bytes) -> dict[str, int]:
    temporary = data_root / f".raft-bench-probe-{token}.tmp"
    published = data_root / f".raft-bench-probe-{token}.published"
    timings = {}
    try:
        started = time.monotonic_ns()
        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        timings["write_sync_ns"] = time.monotonic_ns() - started
        started = time.monotonic_ns()
        os.replace(temporary, published)
        directory = os.open(data_root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        timings["rename_directory_sync_ns"] = time.monotonic_ns() - started
        if published.read_bytes() != payload:
            raise OSError("storage probe payload changed after publication")
        started = time.monotonic_ns()
        published.unlink()
        directory = os.open(data_root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        timings["delete_directory_sync_ns"] = time.monotonic_ns() - started
        return timings
    finally:
        temporary.unlink(missing_ok=True)
        published.unlink(missing_ok=True)


def storage_probe(data_root: Path) -> dict[str, Any]:
    token = uuid.uuid4().hex
    payload = bytes(range(256)) * 16
    samples = [
        _storage_probe_once(data_root, f"{token}-{index}", payload)
        for index in range(STORAGE_PROBE_SAMPLES)
    ]
    return {
        "schema": 2,
        "status": "passed",
        "bytes": len(payload),
        "sample_count": len(samples),
        "timings": samples[-1],
        "samples": samples,
        "summary": _storage_timing_summary(samples),
    }


def capture_profile(
    data_root: Path,
    output: Path,
    *,
    duration_seconds: float = OBJECTIVE["duration_seconds"],
    interval_seconds: float = OBJECTIVE["sample_interval_seconds"],
) -> dict[str, Any]:
    if not 2 <= duration_seconds <= 300 or not 0.1 <= interval_seconds <= 10:
        raise ValueError("machine profile requires duration 2..300s and interval 0.1..10s")
    data_root = data_root.resolve()
    data_root.mkdir(parents=True, exist_ok=True)
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / "machine-profile.json", {
        "schema": 2,
        "kind": "fixed-machine-idle",
        "data_root": str(data_root),
        "observation": {
            "duration_seconds": duration_seconds,
            "sample_interval_seconds": interval_seconds,
        },
        "qualification_objective": dict(OBJECTIVE),
        "source_digest": source_digest(),
    })
    write_json(output / "host.json", host_info(data_root))
    write_json(output / "storage-probe.json", storage_probe(data_root))
    samples = []
    deadline = time.monotonic() + duration_seconds
    with (output / "idle-samples.jsonl").open("x") as stream:
        while True:
            sample = {
                "monotonic_ns": time.monotonic_ns(),
                "unix_ns": time.time_ns(),
                "system": system_sample(data_path=data_root),
            }
            samples.append(sample)
            stream.write(json.dumps(sample, sort_keys=True, allow_nan=False) + "\n")
            stream.flush()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(interval_seconds, remaining))
        os.fsync(stream.fileno())
    summary = summarize_idle(samples, schema=2)
    verdict = assess_idle(summary)
    write_json(output / "summary.json", summary)
    write_json(output / "verdict.json", verdict)
    seal(output)
    return verdict

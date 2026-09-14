"""Immutable receipts, provenance, artifact integrity and accounting validation."""
from __future__ import annotations
from pathlib import Path
import hashlib
import json
import math
import os
import platform
import subprocess
import sys
from typing import Any
ROOT = Path(__file__).resolve().parents[1]
CONTRACT = "durable-log+durable-application-v1/logged-reads"
HISTOGRAM_QUANTIZATION = "64 subdivisions per power of two; upper-bound percentiles"


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as stream:
        json.dump(data, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def capture(command: list[str], *, cwd: Path = ROOT) -> dict[str, Any]:
    try:
        p = subprocess.run(command, cwd=cwd, capture_output=True, text=True, timeout=10, check=False)
        return {"command": command, "returncode": p.returncode, "stdout": p.stdout.strip(), "stderr": p.stderr.strip()}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"command": command, "unavailable": str(exc)}


def require_clean_repository(expected_sha: str, *, root: Path | None = None) -> dict[str, str]:
    """Bind a fixed-machine run to one clean, exact benchmark revision."""
    expected = expected_sha.lower()
    if len(expected) != 40 or any(character not in "0123456789abcdef" for character in expected):
        raise ValueError("benchmark SHA must be an exact 40-character hexadecimal commit")
    repository = (root or ROOT).resolve()
    head = capture(["git", "rev-parse", "HEAD"], cwd=repository)
    status = capture(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=repository,
    )
    if head.get("returncode") != 0 or status.get("returncode") != 0:
        raise RuntimeError("cannot verify benchmark repository revision and cleanliness")
    observed = head["stdout"].lower()
    if observed != expected:
        raise RuntimeError(
            f"benchmark repository HEAD is {observed}, expected {expected}"
        )
    if status["stdout"]:
        raise RuntimeError("benchmark repository has tracked or untracked changes")
    return {"commit": observed, "status": "clean"}


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text().strip()
    except OSError:
        return None


def _cpu_frequency_policies(sysfs_root: Path = Path("/sys")) -> dict[str, Any] | None:
    fields = (
        "scaling_driver",
        "scaling_governor",
        "energy_performance_preference",
        "scaling_min_freq",
        "scaling_max_freq",
    )
    policies = {}
    for policy in sorted((sysfs_root / "devices/system/cpu/cpufreq").glob("policy*")):
        values = {field: _read_text(policy / field) for field in fields}
        if any(value is not None for value in values.values()):
            policies[policy.name] = values
    return policies or None


def _cpu_controls(sysfs_root: Path = Path("/sys")) -> dict[str, str | None]:
    paths = {
        "clocksource": "devices/system/clocksource/clocksource0/current_clocksource",
        "available_clocksources": "devices/system/clocksource/clocksource0/available_clocksource",
        "cpufreq_boost": "devices/system/cpu/cpufreq/boost",
        "intel_pstate_no_turbo": "devices/system/cpu/intel_pstate/no_turbo",
        "numa_online": "devices/system/node/online",
        "transparent_hugepages": "kernel/mm/transparent_hugepage/enabled",
    }
    return {name: _read_text(sysfs_root / relative) for name, relative in paths.items()}


def _hardware_throttle_counts(sysfs_root: Path) -> dict[str, int] | None:
    root = sysfs_root / "devices/system/cpu"
    counters = {}
    for path in sorted(root.glob("cpu[0-9]*/thermal_throttle/*_throttle_count")):
        try:
            counters[path.relative_to(root).as_posix()] = int(path.read_text().strip())
        except (OSError, ValueError):
            continue
    return counters or None


def source_digest() -> str:
    """Hash code/manifests, not build outputs or results. Git need not be installed."""
    h = hashlib.sha256()
    paths = [p for directory in ("adapters", "crates", "loadgen", "benchctl", "scripts", "tests")
             for p in (ROOT / directory).rglob("*") if p.is_file() and "__pycache__" not in p.parts]
    paths += [ROOT / name for name in ("Cargo.toml", "Cargo.lock", ".cargo/config.toml", "rust-toolchain.toml", "implementations.lock.json", "raft-bench") if (ROOT / name).exists()]
    paths += [p for p in (ROOT / "microbench").rglob("*")
              if p.is_file() and "target" not in p.parts and p.suffix in (".rs", ".toml", ".lock", ".json")]
    for p in sorted(paths):
        h.update(p.relative_to(ROOT).as_posix().encode() + b"\0" + bytes.fromhex(digest(p)))
    return h.hexdigest()


def filesystem_space(data_dir: Path) -> dict[str, int] | None:
    try:
        stats = os.statvfs(data_dir)
        fragment_size = stats.f_frsize or stats.f_bsize
        return {
            "total_bytes": stats.f_blocks * fragment_size,
            "free_bytes": stats.f_bfree * fragment_size,
            "available_bytes": stats.f_bavail * fragment_size,
        }
    except OSError:
        return None


def host_info(data_dir: Path) -> dict[str, Any]:
    result = {"platform": platform.platform(), "machine": platform.machine(), "python": sys.version,
              "logical_cpus": os.cpu_count(), "data_path": str(data_dir.resolve()),
              "uname": capture(["uname", "-a"]), "cpu": capture(["lscpu"]),
              "filesystem": capture(["findmnt", "-T", str(data_dir), "-J", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS,MAJ:MIN"]),
              "filesystem_space": filesystem_space(data_dir),
              "storage": capture(["lsblk", "-J", "-o", "NAME,TYPE,SIZE,ROTA,MODEL"]),
              "storage_topology": capture(["lsblk", "-J", "-b", "-o",
                                           "NAME,KNAME,PKNAME,MAJ:MIN,TYPE,SIZE,ROTA,MODEL,TRAN,SCHED,MOUNTPOINTS"]),
              "cpu_frequency_policies": _cpu_frequency_policies(),
              "cpu_controls": _cpu_controls(),
              "kernel_command_line": _read_text(Path("/proc/cmdline")),
              "swaps": _read_text(Path("/proc/swaps")),
              "git": capture(["git", "rev-parse", "HEAD"]), "git_status": capture(["git", "status", "--porcelain"])}
    for name, file in (("cpu_max", "/sys/fs/cgroup/cpu.max"), ("memory_max", "/sys/fs/cgroup/memory.max")):
        try:
            result[name] = Path(file).read_text().strip()
        except OSError:
            result[name] = None
    return result


def proc_sample(pid: int) -> dict[str, Any]:
    """Linux process samples, not heap measurements or a claim of total cluster RSS."""
    try:
        status = Path(f"/proc/{pid}/status").read_text()
        values = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        io = Path(f"/proc/{pid}/io").read_text()
        return {"pid": pid, "rss_kib": int(values.get("VmRSS", "0 kB").split()[0]),
                "cpu_ticks": int(stat[11]) + int(stat[12]), "ticks_per_second": os.sysconf("SC_CLK_TCK"),
                "io": dict(line.split(":", 1) for line in io.splitlines())}
    except (OSError, KeyError, ValueError, IndexError):
        return {"pid": pid, "unavailable": True}


def system_sample(*, proc_root: Path = Path("/proc"),
                  cgroup_root: Path = Path("/sys/fs/cgroup"),
                  sysfs_root: Path = Path("/sys"),
                  data_path: Path | None = None) -> dict[str, Any]:
    """Linux host-pressure counters; cumulative fields are compared between samples."""
    result: dict[str, Any] = {}
    try:
        fields = (proc_root / "stat").read_text().splitlines()[0].split()
        names = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal",
                 "guest", "guest_nice")
        if fields[0] != "cpu":
            raise ValueError("missing aggregate cpu row")
        result["cpu_ticks"] = {name: int(value) for name, value in zip(names, fields[1:])}
    except (OSError, ValueError, IndexError):
        result["cpu_ticks"] = None
    try:
        result["cgroup_cpu"] = {
            key: int(value)
            for key, value in (
                line.split() for line in (cgroup_root / "cpu.stat").read_text().splitlines()
            )
        }
    except (OSError, ValueError):
        result["cgroup_cpu"] = None
    pressure = {}
    for resource in ("cpu", "io", "memory"):
        try:
            rows = {}
            for line in (proc_root / "pressure" / resource).read_text().splitlines():
                kind, *fields = line.split()
                values = {}
                for field in fields:
                    key, value = field.split("=", 1)
                    values[key] = int(value) if key == "total" else float(value)
                rows[kind] = values
            pressure[resource] = rows
        except (OSError, ValueError):
            pressure[resource] = None
    result["pressure"] = pressure
    try:
        one, five, fifteen, runnable, last_pid = (proc_root / "loadavg").read_text().split()
        result["load_average"] = {
            "one": float(one),
            "five": float(five),
            "fifteen": float(fifteen),
            "runnable": runnable,
            "last_pid": int(last_pid),
        }
    except (OSError, ValueError):
        result["load_average"] = None
    try:
        devices = {}
        for line in (proc_root / "diskstats").read_text().splitlines():
            fields = line.split()
            if len(fields) < 14:
                raise ValueError("incomplete diskstats row")
            values = [int(value) for value in fields[:2] + fields[3:]]
            device = {
                "major": values[0],
                "minor": values[1],
                "reads_completed": values[2],
                "reads_merged": values[3],
                "sectors_read": values[4],
                "read_ms": values[5],
                "writes_completed": values[6],
                "writes_merged": values[7],
                "sectors_written": values[8],
                "write_ms": values[9],
                "io_in_progress": values[10],
                "io_ms": values[11],
                "weighted_io_ms": values[12],
            }
            optional = (
                "discards_completed", "discards_merged", "sectors_discarded",
                "discard_ms", "flushes_completed", "flush_ms",
            )
            device.update(zip(optional, values[13:19]))
            devices[fields[2]] = device
        result["block_devices"] = devices
    except (OSError, ValueError):
        result["block_devices"] = None
    result["hardware_throttle_counts"] = _hardware_throttle_counts(sysfs_root)
    result["filesystem_space"] = filesystem_space(data_path) if data_path is not None else None
    return result


def _histogram_percentile(histogram: dict[str, Any], percentile: float) -> int:
    count = histogram["count"]
    if count == 0:
        return 0
    rank = max(1, math.ceil(count * percentile))
    observed = 0
    for index, value in enumerate(histogram["bins"]):
        observed += value
        if observed < rank:
            continue
        exponent, offset = divmod(index, 64)
        if exponent == 0:
            return 1
        base = 1 << (exponent - 1)
        width = max(1, base >> 6)
        upper = base + (offset + 1) * width - 1
        return min(upper, histogram["maximum_ns"])
    raise ValueError("histogram count exceeds bins")


def histogram_summary(histogram: dict[str, Any]) -> dict[str, Any]:
    return {
        "count": histogram["count"],
        "p50_ms": _histogram_percentile(histogram, 0.50) / 1e6,
        "p95_ms": _histogram_percentile(histogram, 0.95) / 1e6,
        "p99_ms": _histogram_percentile(histogram, 0.99) / 1e6,
        "p999_ms": _histogram_percentile(histogram, 0.999) / 1e6,
        "max_ms": histogram["maximum_ns"] / 1e6,
        "quantization": HISTOGRAM_QUANTIZATION,
    }


def _histogram_errors(histogram: Any, expected_count: int, name: str) -> list[str]:
    if not isinstance(histogram, dict):
        return [f"histogram is not an object: {name}"]
    bins = histogram.get("bins")
    count = histogram.get("count")
    maximum = histogram.get("maximum_ns")
    if (
        not isinstance(bins, list)
        or len(bins) != 4096
        or any(type(value) is not int or value < 0 for value in bins)
        or type(count) is not int
        or count < 0
        or sum(bins) != count
        or count != expected_count
        or type(maximum) is not int
        or maximum < 0
    ):
        return [f"histogram accounting mismatch: {name}"]
    if count == 0:
        return [] if maximum == 0 else [f"histogram maximum differs from bins: {name}"]
    highest = max(index for index, value in enumerate(bins) if value)
    exponent, offset = divmod(highest, 64)
    if exponent == 0:
        return [f"histogram contains an unreachable bin: {name}"]
    base = 1 << (exponent - 1)
    width = max(1, base >> 6)
    lower = base + offset * width
    upper = base + (offset + 1) * width - 1
    if not lower <= maximum <= upper:
        return [f"histogram maximum differs from bins: {name}"]
    return []


def validate_result(r: dict[str, Any]) -> list[str]:
    errors = []
    required = ("offered", "attempted", "not_issued", "ok", "unknown", "errors", "completed_in_window", "network_attempts")
    for key in required:
        if type(r.get(key)) is not int or r[key] < 0:
            errors.append(f"invalid nonnegative count: {key}")
    if errors:
        return errors
    if r["offered"] != r["attempted"] + r["not_issued"]:
        errors.append("offered != attempted + not_issued")
    if r["attempted"] != r["ok"] + r["unknown"] + r["errors"]:
        errors.append("attempted != ok + unknown + errors")
    if r["completed_in_window"] > r["ok"]:
        errors.append("in-window completions exceed successes")
    if r.get("contract") != CONTRACT:
        errors.append("unsupported semantic contract")
    schema = r.get("schema", 1)
    if type(schema) is not int or schema < 1 or schema > 3:
        errors.append("unsupported measurement schema")
        return errors
    histograms = [
        ("success_histogram", r["ok"], "success_latency"),
        ("all_histogram", r["attempted"], "all_dispatched_latency"),
    ]
    if schema >= 2:
        histograms += [
            ("success_execution_histogram", r["ok"], "success_execution_latency"),
            ("all_execution_histogram", r["attempted"], "all_dispatched_execution_latency"),
        ]
    if schema >= 3:
        config = r.get("config")
        rate = config.get("rate") if isinstance(config, dict) else None
        if type(rate) not in (int, float) or not math.isfinite(rate) or rate < 0:
            errors.append("invalid scheduled rate for measurement schema 3")
            scheduler_count = -1
        else:
            scheduler_count = r["offered"] if rate > 0 else 0
        histograms += [
            ("worker_start_lateness_histogram", r["attempted"], "worker_start_lateness"),
            ("scheduler_lateness_histogram", scheduler_count, "scheduler_lateness"),
        ]
    for name, count, summary_name in histograms:
        histogram = r.get(name)
        histogram_errors = _histogram_errors(histogram, count, name)
        errors.extend(histogram_errors)
        if schema >= 3 and not histogram_errors:
            if r.get(summary_name) != histogram_summary(histogram):
                errors.append(f"histogram summary mismatch: {summary_name}")
    return errors


def seal(directory: Path) -> None:
    files = {}
    for p in sorted(directory.rglob("*")):
        if p.is_symlink():
            raise ValueError("symlink in run evidence")
        if p.is_file() and p.name != "SHA256SUMS.json":
            files[p.relative_to(directory).as_posix()] = digest(p)
    write_json(directory / "SHA256SUMS.json", {"schema": 1, "files": files})


def runtime_configuration_errors(directory: Path, manifest: dict) -> list[str]:
    """Recompute runtime receipts while retaining compatibility with old evidence."""
    options = manifest.get("options", {})
    receipt_path = directory / "replication-window-config.json"
    if manifest.get("implementation") != "rafter" or not options.get("diagnostics"):
        return ["unexpected replication window activation receipt"] if receipt_path.exists() else []
    declared = "max_inflight_appends" in options
    if not declared:
        return ["replication window receipt has no declared bound"] if receipt_path.exists() else []
    if not receipt_path.exists():
        return ["replication window activation receipt is missing"]
    from .replication_windows import configuration_receipt
    actual = configuration_receipt(
        json.loads((directory / "before.json").read_text()),
        json.loads((directory / "diagnostics-after-load.json").read_text()),
        options["max_inflight_appends"],
    )
    recorded = json.loads(receipt_path.read_text())
    return [] if actual == recorded else ["replication window activation receipt differs from runtime status"]


def load_environment_errors(directory: Path, manifest: dict) -> list[str]:
    """Replay declared measured-load context while retaining old case compatibility."""
    receipt_path = directory / "load-environment.json"
    declared = manifest.get("load_environment_observation")
    if declared is None:
        return ["unexpected measured-load environment receipt"] if receipt_path.exists() else []
    expected_keys = {
        "schema",
        "expected_duration_seconds",
        "nominal_sample_interval_seconds",
    }
    if (
        not isinstance(declared, dict)
        or set(declared) != expected_keys
        or declared.get("schema") != 1
    ):
        return ["unsupported measured-load environment declaration"]
    if not receipt_path.exists():
        return ["measured-load environment receipt is missing"]
    from .machine import load_environment_receipt

    samples = [
        json.loads(line)
        for line in (directory / "process-samples.jsonl").read_text().splitlines()
    ]
    observed = load_environment_receipt(
        samples,
        expected_duration_seconds=declared["expected_duration_seconds"],
        nominal_sample_interval_seconds=declared["nominal_sample_interval_seconds"],
    )
    recorded = json.loads(receipt_path.read_text())
    errors = []
    if observed != recorded:
        errors.append("measured-load environment receipt differs from raw samples")
    if observed["coverage"]["status"] != "passed":
        errors.append("measured-load environment sampling coverage did not pass")
    return errors


def snapshot_reclamation_errors(directory: Path, manifest: dict) -> list[str]:
    """Replay optional live snapshot/reclamation evidence from sealed status files."""
    options = manifest.get("options", {})
    interval = options.get("snapshot_interval_entries", 0)
    receipt_path = directory / "snapshot-compaction-activity.json"
    after_path = directory / "snapshot-after-scenario.json"
    if not interval:
        unexpected = [path.name for path in (receipt_path, after_path) if path.exists()]
        return [f"unexpected live snapshot/reclamation evidence: {name}" for name in unexpected]
    if type(interval) is not int or interval < 1 or interval > 1_000_000_000:
        return ["invalid declared snapshot interval"]
    missing = [path.name for path in (receipt_path, after_path) if not path.exists()]
    if missing:
        return [f"missing live snapshot/reclamation evidence: {name}" for name in missing]
    from .reclamation_service import activity

    errors = []
    restarted_node = None
    stopped_application_index = None
    required_snapshot_index = None
    if manifest.get("scenario") == "snapshot-catchup":
        fault = json.loads((directory / "fault.json").read_text())
        events = fault.get("events", [])
        if not isinstance(events, list) or len(events) != 2:
            errors.append("snapshot catch-up requires exactly one follower kill and restart")
        elif not all(isinstance(event, dict) for event in events):
            errors.append("snapshot catch-up fault events are invalid")
        else:
            restarted_node = events[0].get("node")
            if (
                type(restarted_node) is not int
                or restarted_node not in (1, 2, 3)
                or events[0].get("action") != "SIGKILL follower"
                or events[1].get("node") != restarted_node
                or events[1].get("action") != "restart same data directory"
            ):
                errors.append("snapshot catch-up fault actions differ from the declared scenario")
        catchup = fault.get("snapshot_catchup", {})
        stopped_application_index = catchup.get("stopped_application_index")
        required_snapshot_index = catchup.get("required_snapshot_index")

    recorded = json.loads(receipt_path.read_text())
    diagnostics = bool(options.get("diagnostics", False))
    actual = activity(
        json.loads((directory / "before.json").read_text()),
        json.loads(after_path.read_text()),
        interval,
        manifest.get("scenario"),
        restarted_node=restarted_node,
        stopped_application_index=stopped_application_index,
        required_snapshot_index=required_snapshot_index,
        native_snapshot_stages=diagnostics or recorded.get("schema") == 3,
        require_native_snapshot_stage_activity=diagnostics,
    )
    if actual != recorded:
        errors.append("live snapshot/reclamation receipt differs from runtime status")
    if actual["status"] != "passed":
        errors.append("live snapshot/reclamation activity did not pass")
    return errors


def storage_footprint_errors(directory: Path, manifest: dict) -> list[str]:
    """Validate controller-observed storage receipts when explicitly declared."""
    declared = manifest.get("storage_footprint_observation")
    receipt_path = directory / "storage-footprint.json"
    if declared is None:
        return ["unexpected storage footprint receipt"] if receipt_path.exists() else []
    from .storage_footprint import OBSERVATION, errors

    expected = OBSERVATION
    if declared != expected:
        return ["unsupported storage footprint observation declaration"]
    if not receipt_path.exists():
        return ["declared storage footprint receipt is missing"]
    failures = []
    for boundary, name in expected["status_barriers"].items():
        barrier_path = directory / name
        if not barrier_path.exists():
            failures.append(f"storage footprint {boundary} status barrier is missing")
            continue
        try:
            statuses = json.loads(barrier_path.read_text())
        except (OSError, json.JSONDecodeError):
            failures.append(f"storage footprint {boundary} status barrier is invalid")
            continue
        if not isinstance(statuses, dict) or set(statuses) != {"1", "2", "3"}:
            failures.append(f"storage footprint {boundary} status barrier is incomplete")
            continue
        if any(
            not isinstance(status, dict)
            or status.get("status") != "ok"
            or not isinstance(status.get("info"), dict)
            or status["info"].get("node_id") != int(node)
            for node, status in statuses.items()
        ):
            failures.append(f"storage footprint {boundary} status barrier is incoherent")
    try:
        value = json.loads(receipt_path.read_text())
    except (OSError, json.JSONDecodeError):
        failures.append("storage footprint receipt is invalid")
        return failures
    return failures + errors(value)


def verify(directory: Path) -> dict[str, Any]:
    integrity_errors = []
    correctness_errors = []
    active_errors = integrity_errors
    try:
        index = json.loads((directory / "SHA256SUMS.json").read_text())["files"]
        actual = {p.relative_to(directory).as_posix() for p in directory.rglob("*") if p.is_file() and p.name != "SHA256SUMS.json"}
        if actual != set(index):
            integrity_errors.append("artifact inventory changed")
        for relative, sha in index.items():
            p = directory / relative
            if Path(relative).is_absolute() or ".." in Path(relative).parts or p.is_symlink() or not p.is_file():
                integrity_errors.append(f"invalid artifact path: {relative}")
            elif digest(p) != sha:
                integrity_errors.append(f"checksum mismatch: {relative}")
        if (directory / "machine-profile.json").exists():
            from .machine import assess_idle, storage_probe_errors, summarize_idle
            manifest = json.loads((directory / "machine-profile.json").read_text())
            profile_schema = manifest.get("schema")
            if profile_schema not in (1, 2) or manifest.get("kind") != "fixed-machine-idle":
                integrity_errors.append("unsupported machine-profile manifest")
            samples = [json.loads(line) for line in
                       (directory / "idle-samples.jsonl").read_text().splitlines()]
            observed_summary = summarize_idle(
                samples, schema=profile_schema if profile_schema in (1, 2) else 2
            )
            recorded_summary = json.loads((directory / "summary.json").read_text())
            if observed_summary != recorded_summary:
                integrity_errors.append("machine-profile summary differs from raw samples")
            observed_verdict = assess_idle(
                observed_summary, manifest.get("qualification_objective", {})
            )
            recorded_verdict = json.loads((directory / "verdict.json").read_text())
            if observed_verdict != recorded_verdict:
                integrity_errors.append("machine-profile verdict differs from objective")
            storage = json.loads((directory / "storage-probe.json").read_text())
            integrity_errors.extend(storage_probe_errors(storage))
            environment_status = recorded_verdict.get("status", "failed")
            failed = bool(integrity_errors) or environment_status != "passed"
            return {
                "status": "failed" if failed else "passed",
                "errors": integrity_errors + recorded_verdict.get("failures", [])
                + [f"missing: {name}" for name in recorded_verdict.get("missing", [])],
                "verdicts": {
                    "evidence_integrity": {
                        "status": "failed" if integrity_errors else "passed",
                        "errors": integrity_errors,
                    },
                    "environment_qualification": recorded_verdict,
                },
            }
        if (directory / "provenance.json").exists():
            from .micro_report import verify_report
            verify_report(directory)
            return {
                "status": "failed" if integrity_errors else "passed",
                "errors": integrity_errors,
                "verdicts": {
                    "evidence_integrity": {
                        "status": "failed" if integrity_errors else "passed",
                        "errors": integrity_errors,
                    },
                    "correctness_checks": {"status": "not measured", "errors": []},
                },
            }
        manifest = json.loads((directory / "manifest.json").read_text())
        integrity_errors.extend(load_environment_errors(directory, manifest))
        integrity_errors.extend(runtime_configuration_errors(directory, manifest))
        integrity_errors.extend(snapshot_reclamation_errors(directory, manifest))
        integrity_errors.extend(storage_footprint_errors(directory, manifest))
        if manifest.get("options", {}).get("diagnostics"):
            from .timelines import extract, recorded_extract_matches
            actual_timelines = extract(
                json.loads((directory / "before.json").read_text()),
                json.loads((directory / "diagnostics-after-load.json").read_text()),
            )
            recorded_timelines = json.loads((directory / "operation-timelines.json").read_text())
            if not recorded_extract_matches(actual_timelines, recorded_timelines):
                integrity_errors.append("operation timeline extraction differs from status watermarks")
        if manifest.get("options", {}).get("pipelined_durability") and manifest.get("scenario") == "durable-kv":
            from .pipeline import activity, recorded_activity_matches
            actual_activity = activity(
                json.loads((directory / "before.json").read_text()),
                json.loads((directory / "persistence-after-load.json").read_text()),
                require_combined=bool(manifest.get("options", {}).get("combine_peer_proposals")))
            recorded_activity = json.loads((directory / "pipeline-activity.json").read_text())
            if not recorded_activity_matches(actual_activity, recorded_activity):
                integrity_errors.append("pipeline activity verdict differs from recorded counters")
        completion_priority = manifest.get("options", {}).get("durable_completion_priority")
        if completion_priority and manifest.get("scenario") == "durable-kv":
            from .completion_priority import activity as completion_priority_activity
            actual_priority = completion_priority_activity(
                json.loads((directory / "before.json").read_text()),
                json.loads((directory / "persistence-after-load.json").read_text()),
            )
            recorded_priority = json.loads(
                (directory / "completion-priority-activity.json").read_text()
            )
            if actual_priority != recorded_priority:
                integrity_errors.append("durable completion priority verdict differs from recorded counters")
        elif (directory / "completion-priority-activity.json").exists():
            integrity_errors.append("unexpected durable completion priority activity receipt")
        result = json.loads((directory / "measurement.json").read_text())
        integrity_errors.extend(validate_result(result))
        active_errors = correctness_errors
        qualification = json.loads((directory / "qualification.json").read_text())
        recovery = json.loads((directory / "recovery.json").read_text())
        from .checker import check_history, read_history
        replay = check_history(read_history(directory / "qualification-history.jsonl"))
        if replay.get("status") != "passed":
            correctness_errors.append("independent history recheck did not pass")
        if qualification.get("status") != "passed":
            correctness_errors.append("history qualification did not pass")
        if recovery.get("status") != "passed":
            correctness_errors.append("restart recovery check did not pass")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        active_errors.append(str(exc))
    errors = integrity_errors + correctness_errors
    return {"status": "passed" if not errors else "failed", "errors": errors,
            "verdicts": {
                "evidence_integrity": {
                    "status": "passed" if not integrity_errors else "failed",
                    "errors": integrity_errors,
                },
                "correctness_checks": {
                    "status": "passed" if not correctness_errors else "failed",
                    "errors": correctness_errors,
                },
            },
            "scope": "artifact integrity/accounting plus recorded finite checks; not independent certification"}

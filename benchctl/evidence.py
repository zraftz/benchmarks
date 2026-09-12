"""Immutable receipts, provenance, artifact integrity and accounting validation."""
from __future__ import annotations
from pathlib import Path
import hashlib
import json
import os
import platform
import subprocess
import sys
from typing import Any
ROOT = Path(__file__).resolve().parents[1]
CONTRACT = "durable-log+durable-application-v1/logged-reads"


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


def host_info(data_dir: Path) -> dict[str, Any]:
    result = {"platform": platform.platform(), "machine": platform.machine(), "python": sys.version,
              "logical_cpus": os.cpu_count(), "data_path": str(data_dir.resolve()),
              "uname": capture(["uname", "-a"]), "cpu": capture(["lscpu"]),
              "filesystem": capture(["findmnt", "-T", str(data_dir), "-J", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"]),
              "storage": capture(["lsblk", "-J", "-o", "NAME,TYPE,SIZE,ROTA,MODEL"]),
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
    histograms = [("success_histogram", r["ok"]), ("all_histogram", r["attempted"])]
    if r.get("schema", 1) >= 2:
        histograms += [("success_execution_histogram", r["ok"]), ("all_execution_histogram", r["attempted"])]
    for name, count in histograms:
        hist = r.get(name, {})
        bins = hist.get("bins", [])
        if len(bins) != 4096 or any(type(n) is not int or n < 0 for n in bins) or sum(bins) != count or hist.get("count") != count:
            errors.append(f"histogram accounting mismatch: {name}")
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


def verify(directory: Path) -> dict[str, Any]:
    errors = []
    try:
        index = json.loads((directory / "SHA256SUMS.json").read_text())["files"]
        actual = {p.relative_to(directory).as_posix() for p in directory.rglob("*") if p.is_file() and p.name != "SHA256SUMS.json"}
        if actual != set(index):
            errors.append("artifact inventory changed")
        for relative, sha in index.items():
            p = directory / relative
            if Path(relative).is_absolute() or ".." in Path(relative).parts or p.is_symlink() or not p.is_file():
                errors.append(f"invalid artifact path: {relative}")
            elif digest(p) != sha:
                errors.append(f"checksum mismatch: {relative}")
        if (directory / "provenance.json").exists():
            from .micro_report import verify_report
            verify_report(directory)
            return {"status": "failed" if errors else "passed", "errors": errors}
        manifest = json.loads((directory / "manifest.json").read_text())
        errors.extend(runtime_configuration_errors(directory, manifest))
        if manifest.get("options", {}).get("diagnostics"):
            from .timelines import extract, recorded_extract_matches
            actual_timelines = extract(
                json.loads((directory / "before.json").read_text()),
                json.loads((directory / "diagnostics-after-load.json").read_text()),
            )
            recorded_timelines = json.loads((directory / "operation-timelines.json").read_text())
            if not recorded_extract_matches(actual_timelines, recorded_timelines):
                errors.append("operation timeline extraction differs from status watermarks")
        if manifest.get("options", {}).get("pipelined_durability") and manifest.get("scenario") == "durable-kv":
            from .pipeline import activity, recorded_activity_matches
            actual_activity = activity(
                json.loads((directory / "before.json").read_text()),
                json.loads((directory / "persistence-after-load.json").read_text()),
                require_combined=bool(manifest.get("options", {}).get("combine_peer_proposals")))
            recorded_activity = json.loads((directory / "pipeline-activity.json").read_text())
            if not recorded_activity_matches(actual_activity, recorded_activity):
                errors.append("pipeline activity verdict differs from recorded counters")
        result = json.loads((directory / "measurement.json").read_text())
        errors.extend(validate_result(result))
        qualification = json.loads((directory / "qualification.json").read_text())
        recovery = json.loads((directory / "recovery.json").read_text())
        from .checker import check_history, read_history
        replay = check_history(read_history(directory / "qualification-history.jsonl"))
        if replay.get("status") != "passed":
            errors.append("independent history recheck did not pass")
        if qualification.get("status") != "passed":
            errors.append("history qualification did not pass")
        if recovery.get("status") != "passed":
            errors.append("restart recovery check did not pass")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        errors.append(str(exc))
    return {"status": "passed" if not errors else "failed", "errors": errors,
            "scope": "artifact integrity/accounting plus recorded finite checks; not independent certification"}

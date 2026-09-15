"""Controller-observed storage footprints for retained service evidence."""
from __future__ import annotations

from pathlib import Path
from typing import Any


CATEGORIES = (
    "raft_wal_data",
    "raft_wal_metadata",
    "raft_snapshot_data",
    "raft_snapshot_metadata",
    "raft_snapshot_temporary",
    "application_journal",
    "other",
)

MANAGED_RAFT_CATEGORIES = CATEGORIES[:5]
OBSERVATION = {
    "schema": 1,
    "checkpoints": [
        "before_measurement",
        "after_measurement",
        "after_final_restart",
    ],
    "status_barriers": {
        "before_measurement": "before.json",
        "after_measurement": "after-measurement-status.json",
        "after_final_restart": "after-final-restart.json",
    },
    "sizes": ["logical_bytes", "allocated_bytes"],
}


def _category(relative: Path) -> str:
    parts = relative.parts
    name = relative.name
    if len(parts) == 2 and name == "application.wal":
        return "application_journal"
    if len(parts) >= 3 and parts[1] == "raft":
        if name.endswith((".rfwb", ".rfwc")):
            return "raft_wal_data"
        if name == "raft-wal-current":
            return "raft_wal_metadata"
        if parts[2] == "snapshots":
            if name.startswith(".") and name.endswith(".tmp"):
                return "raft_snapshot_temporary"
            if name.endswith(".rfsn") or name == "pending.snapshot-transfer.body":
                return "raft_snapshot_data"
            if (
                name in {"current.snapshot", "pending.snapshot-transfer"}
            ):
                return "raft_snapshot_metadata"
    return "other"


def _empty_totals() -> dict[str, dict[str, int]]:
    return {
        category: {"files": 0, "logical_bytes": 0, "allocated_bytes": 0}
        for category in CATEGORIES
    }


def capture(root: Path) -> dict[str, Any]:
    """Capture regular-file sizes without retaining or hashing live node data."""
    if not root.is_dir():
        raise ValueError("storage footprint root is not a directory")
    nodes: dict[str, dict[str, Any]] = {}
    for node in (1, 2, 3):
        directory = root / f"node-{node}"
        if not directory.is_dir() or directory.is_symlink():
            raise ValueError(f"storage footprint is missing node-{node}")
        files = []
        totals = _empty_totals()
        for path in sorted(directory.rglob("*")):
            if path.is_symlink():
                raise ValueError("symlink in live storage footprint")
            if not path.is_file():
                continue
            relative = path.relative_to(root)
            stat = path.stat()
            category = _category(relative)
            logical = stat.st_size
            allocated = getattr(stat, "st_blocks", 0) * 512
            files.append({
                "path": relative.as_posix(),
                "category": category,
                "logical_bytes": logical,
                "allocated_bytes": allocated,
            })
            totals[category]["files"] += 1
            totals[category]["logical_bytes"] += logical
            totals[category]["allocated_bytes"] += allocated
        nodes[str(node)] = {"files": files, "totals": totals}
    return {
        "nodes": nodes,
        "totals": _sum_totals(node["totals"] for node in nodes.values()),
    }


def _sum_totals(values) -> dict[str, dict[str, int]]:
    result = _empty_totals()
    for value in values:
        for category in CATEGORIES:
            for field in ("files", "logical_bytes", "allocated_bytes"):
                result[category][field] += value[category][field]
    return result


def receipt(before: dict, after_load: dict, after_restart: dict) -> dict[str, Any]:
    snapshots = {
        "before_measurement": before,
        "after_measurement": after_load,
        "after_final_restart": after_restart,
    }
    deltas = {}
    for destination, left, right in (
        ("measurement", before, after_load),
        ("final_restart", after_load, after_restart),
    ):
        deltas[destination] = {
            category: {
                field: right["totals"][category][field] - left["totals"][category][field]
                for field in ("files", "logical_bytes", "allocated_bytes")
            }
            for category in CATEGORIES
        }
    return {
        "schema": 1,
        "scope": (
            "controller stat observations of logical and allocated regular-file bytes; "
            "each boundary follows an owner-thread status barrier; live files are not "
            "retained in the evidence archive"
        ),
        "snapshots": snapshots,
        "deltas": deltas,
    }


def errors(value: Any) -> list[str]:
    failures = []
    if not isinstance(value, dict) or value.get("schema") != 1:
        return ["unsupported storage footprint receipt"]
    snapshots = value.get("snapshots")
    expected_snapshots = {
        "before_measurement",
        "after_measurement",
        "after_final_restart",
    }
    if not isinstance(snapshots, dict) or set(snapshots) != expected_snapshots:
        return ["storage footprint checkpoint inventory differs"]
    for checkpoint, snapshot in snapshots.items():
        failures.extend(_snapshot_errors(snapshot, checkpoint))
    if failures:
        return failures
    expected = receipt(
        snapshots["before_measurement"],
        snapshots["after_measurement"],
        snapshots["after_final_restart"],
    )
    if value != expected:
        failures.append("storage footprint derived totals or deltas differ")
    return failures


def _snapshot_errors(snapshot: Any, checkpoint: str) -> list[str]:
    failures = []
    if not isinstance(snapshot, dict) or set(snapshot) != {"nodes", "totals"}:
        return [f"invalid storage footprint snapshot: {checkpoint}"]
    nodes = snapshot.get("nodes")
    if not isinstance(nodes, dict) or set(nodes) != {"1", "2", "3"}:
        return [f"storage footprint node inventory differs: {checkpoint}"]
    for node, observed in nodes.items():
        if not isinstance(observed, dict) or set(observed) != {"files", "totals"}:
            failures.append(f"invalid storage footprint node {node}: {checkpoint}")
            continue
        files = observed["files"]
        if not isinstance(files, list):
            failures.append(f"invalid storage footprint files for node {node}: {checkpoint}")
            continue
        expected_prefix = f"node-{node}/"
        seen = set()
        totals = _empty_totals()
        for item in files:
            if not isinstance(item, dict) or set(item) != {
                "path", "category", "logical_bytes", "allocated_bytes"
            }:
                failures.append(f"invalid storage footprint file row for node {node}: {checkpoint}")
                continue
            path = item["path"]
            relative = Path(path) if isinstance(path, str) else Path("/")
            if (
                not isinstance(path, str)
                or not path.startswith(expected_prefix)
                or relative.is_absolute()
                or ".." in relative.parts
                or path in seen
            ):
                failures.append(f"invalid storage footprint path for node {node}: {checkpoint}")
                continue
            seen.add(path)
            category = item["category"]
            if category not in CATEGORIES or category != _category(relative):
                failures.append(f"invalid storage footprint category for node {node}: {checkpoint}")
                continue
            logical = item["logical_bytes"]
            allocated = item["allocated_bytes"]
            if (
                type(logical) is not int
                or logical < 0
                or type(allocated) is not int
                or allocated < 0
            ):
                failures.append(f"invalid storage footprint size for node {node}: {checkpoint}")
                continue
            totals[category]["files"] += 1
            totals[category]["logical_bytes"] += logical
            totals[category]["allocated_bytes"] += allocated
        if observed["totals"] != totals:
            failures.append(f"storage footprint node totals differ for node {node}: {checkpoint}")
    if failures:
        return failures
    expected_totals = _sum_totals(node["totals"] for node in nodes.values())
    if snapshot["totals"] != expected_totals:
        failures.append(f"storage footprint aggregate totals differ: {checkpoint}")
    return failures

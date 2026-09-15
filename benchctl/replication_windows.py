"""Prove that a requested Rafter replication-window bound is active."""
from __future__ import annotations
from typing import Any


def _positive_int(value: Any, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"invalid replication-window value: {name}")
    return value


def _snapshot(snapshot: dict, name: str, expected: int) -> dict:
    if not isinstance(snapshot, dict) or not snapshot:
        raise ValueError(f"{name} status snapshot has no nodes")
    node_ids = {_positive_int(int(node), "status node_id") for node in snapshot}
    leaders = []
    for node, status in sorted(snapshot.items(), key=lambda item: int(item[0])):
        if not isinstance(status, dict) or status.get("status") != "ok":
            raise ValueError(f"{name} node {node} has no successful status")
        info = status.get("info")
        if not isinstance(info, dict) or info.get("implementation") != "rafter":
            raise ValueError(f"{name} node {node} is not a Rafter status")
        node_id = _positive_int(info.get("node_id"), "node_id")
        if node_id != int(node):
            raise ValueError(f"{name} node {node} reports a different node identity")
        engine = info.get("engine")
        if not isinstance(engine, dict):
            raise ValueError(f"{name} node {node} has malformed engine diagnostics")
        replication = engine.get("replication_windows")
        if not isinstance(replication, dict):
            raise ValueError(f"{name} node {node} has malformed replication-window diagnostics")
        windows = replication.get("windows")
        if info.get("leader") is not True:
            if windows != []:
                raise ValueError(f"{name} non-leader {node} reports replication windows")
            continue
        if not isinstance(windows, list) or not windows:
            raise ValueError(f"{name} leader {node} has no replication windows")
        observed = []
        follower_ids = set()
        for window in windows:
            if not isinstance(window, dict):
                raise ValueError(f"{name} leader {node} has a malformed replication window")
            follower = _positive_int(window.get("follower_id"), "follower_id")
            maximum = _positive_int(
                window.get("max_in_flight_batches"), "max_in_flight_batches"
            )
            if follower in follower_ids:
                raise ValueError(f"{name} leader {node} has duplicate follower windows")
            if maximum != expected:
                raise ValueError(
                    f"{name} leader {node} reports replication window {maximum}, expected {expected}"
                )
            follower_ids.add(follower)
            observed.append({
                "follower_id": follower,
                "max_in_flight_batches": maximum,
            })
        if follower_ids != node_ids - {node_id}:
            raise ValueError(f"{name} leader {node} replication-window membership differs")
        leaders.append({"node_id": node_id, "follower_windows": observed})
    if not leaders:
        raise ValueError(f"{name} status snapshot has no observed leader")
    return {"leaders": leaders}


def configuration_receipt(before: dict, after_load: dict, expected: int) -> dict:
    """Return a sealed receipt only when each observed leader reports the requested bound."""
    expected = _positive_int(expected, "requested_max_in_flight_batches")
    if expected > 64:
        raise ValueError("requested replication-window bound exceeds 64")
    return {
        "schema": 1,
        "status": "passed",
        "requested_max_in_flight_batches": expected,
        "snapshots": {
            "before": _snapshot(before, "before", expected),
            "after_load": _snapshot(after_load, "after_load", expected),
        },
        "scope": "runtime leader diagnostics before and after measured load",
    }

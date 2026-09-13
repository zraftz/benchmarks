"""Replay live service snapshot and WAL-reclamation activity from node status."""
from __future__ import annotations

from typing import Any


def _nodes(statuses: dict) -> dict[str, dict]:
    return {str(node): reply for node, reply in statuses.items()}


def _histogram(values: Any, completed: int, node: str, boundary: str) -> list[int]:
    if (
        not isinstance(values, list)
        or len(values) != 64
        or any(type(value) is not int or value < 0 for value in values)
        or sum(values) != completed
    ):
        raise ValueError(f"node {node} {boundary} compaction histogram is invalid")
    return values


def _upper_bound_ns(buckets: list[int]) -> int | None:
    populated = [index for index, count in enumerate(buckets) if count]
    if not populated:
        return None
    highest = populated[-1]
    return (1 << (highest + 1)) - 1 if highest < 63 else (1 << 64) - 1


def activity(
    before: dict,
    after: dict,
    interval: int,
    scenario: str,
    *,
    restarted_node: int | None = None,
    stopped_application_index: int | None = None,
    required_snapshot_index: int | None = None,
) -> dict[str, Any]:
    failures = []
    before_nodes = _nodes(before)
    after_nodes = _nodes(after)
    if set(before_nodes) != {"1", "2", "3"} or set(after_nodes) != {"1", "2", "3"}:
        failures.append("snapshot activity requires status from all three nodes")

    nodes = {}
    histogram_nodes = 0
    for node in ("1", "2", "3"):
        try:
            before_info = before_nodes[node]["info"]
            after_info = after_nodes[node]["info"]
            old = before_info["snapshot_compaction"]
            new = after_info["snapshot_compaction"]
            old_completed = old["completed"]
            new_completed = new["completed"]
            old_installs = old["application_installs"]
            new_installs = new["application_installs"]
            old_total_ns = old["total_ns"]
            new_total_ns = new["total_ns"]
            current_index = new["current_index"]
            applied_index = after_info["application"]["applied_index"]
            commit_index = after_info["engine"]["commit_index"]
            values = (
                old_completed,
                new_completed,
                old_installs,
                new_installs,
                current_index,
                applied_index,
                commit_index,
                old_total_ns,
                new_total_ns,
                new["max_ns"],
                new["latest_payload_bytes"],
            )
            if any(type(value) is not int or value < 0 for value in values):
                raise ValueError("snapshot counters must be nonnegative integers")
            if old["interval_entries"] != interval or new["interval_entries"] != interval:
                failures.append(f"node {node} snapshot interval differs from the case")
            restarted = restarted_node is not None and str(restarted_node) == node
            if not restarted and (
                new_completed < old_completed
                or new_installs < old_installs
                or new_total_ns < old_total_ns
            ):
                failures.append(f"node {node} snapshot counters regressed")
            if not 0 < current_index <= applied_index <= commit_index:
                failures.append(f"node {node} snapshot/application/commit boundaries are incoherent")
            completed = new_completed if restarted else new_completed - old_completed
            installs = new_installs if restarted else new_installs - old_installs
            total_ns = new_total_ns if restarted else new_total_ns - old_total_ns
            old_buckets = old.get("buckets_log2")
            new_buckets = new.get("buckets_log2")
            measurement = None
            if old_buckets is not None or new_buckets is not None:
                if old_buckets is None or new_buckets is None:
                    failures.append(f"node {node} compaction histogram changed availability")
                else:
                    first = _histogram(old_buckets, old_completed, node, "before")
                    last = _histogram(new_buckets, new_completed, node, "after")
                    buckets = last if restarted else [
                        right - left for left, right in zip(first, last, strict=True)
                    ]
                    if any(value < 0 for value in buckets) or sum(buckets) != completed:
                        failures.append(f"node {node} compaction histogram regressed")
                    else:
                        histogram_nodes += 1
                        measurement = {
                            "samples": completed,
                            "total_ns": total_ns,
                            "mean_ns": total_ns / completed if completed else None,
                            "max_upper_bound_ns": _upper_bound_ns(buckets),
                            "buckets_log2": buckets,
                        }
            if completed and (new["max_ns"] == 0 or new["latest_payload_bytes"] == 0):
                failures.append(f"node {node} completed snapshot lacks timing or payload evidence")
            nodes[node] = {
                "compactions_since_before_status": completed,
                "application_installs_since_before_status": installs,
                "current_snapshot_index": current_index,
                "application_applied_index": applied_index,
                "raft_commit_index": commit_index,
                "max_compaction_ns": new["max_ns"],
                "latest_payload_bytes": new["latest_payload_bytes"],
            }
            if measurement is not None:
                nodes[node]["measurement_compaction"] = measurement
        except (KeyError, TypeError, ValueError) as error:
            failures.append(f"node {node} snapshot activity is invalid: {error}")

    if histogram_nodes not in (0, 3):
        failures.append("snapshot compaction histograms are not available on all three nodes")

    compactions = sum(node["compactions_since_before_status"] for node in nodes.values())
    installs = sum(node["application_installs_since_before_status"] for node in nodes.values())
    if compactions == 0:
        failures.append("no live snapshot compaction completed since the before status")
    require_transfer = scenario == "snapshot-catchup"
    if require_transfer and installs == 0:
        failures.append("the lagging-follower scenario did not install an application snapshot")
    if require_transfer:
        if (type(stopped_application_index) is not int
                or type(required_snapshot_index) is not int
                or required_snapshot_index <= stopped_application_index):
            failures.append("snapshot catch-up boundary is missing or did not advance")
        elif str(restarted_node) not in nodes:
            failures.append("snapshot catch-up restarted node is missing")
        else:
            restarted = nodes[str(restarted_node)]
            if (restarted["current_snapshot_index"] < required_snapshot_index
                    or restarted["application_applied_index"] < required_snapshot_index):
                failures.append("restarted follower did not reach the required snapshot boundary")
    return {
        "schema": 2 if histogram_nodes == 3 else 1,
        "status": "passed" if not failures else "failed",
        "interval_entries": interval,
        "scenario": scenario,
        "restarted_node": restarted_node,
        "stopped_application_index": stopped_application_index,
        "required_snapshot_index": required_snapshot_index,
        "requirements": {
            "live_compaction": True,
            "snapshot_transfer_and_application_install": require_transfer,
        },
        "totals": {
            "compactions_since_before_status": compactions,
            "application_installs_since_before_status": installs,
        },
        "nodes": nodes,
        "failures": failures,
    }

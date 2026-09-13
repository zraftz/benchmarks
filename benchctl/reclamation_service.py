"""Replay live service snapshot and WAL-reclamation activity from node status."""
from __future__ import annotations

from typing import Any


def _nodes(statuses: dict) -> dict[str, dict]:
    return {str(node): reply for node, reply in statuses.items()}


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
                new["max_ns"],
                new["latest_payload_bytes"],
            )
            if any(type(value) is not int or value < 0 for value in values):
                raise ValueError("snapshot counters must be nonnegative integers")
            if old["interval_entries"] != interval or new["interval_entries"] != interval:
                failures.append(f"node {node} snapshot interval differs from the case")
            restarted = restarted_node is not None and str(restarted_node) == node
            if not restarted and (new_completed < old_completed or new_installs < old_installs):
                failures.append(f"node {node} snapshot counters regressed")
            if not 0 < current_index <= applied_index <= commit_index:
                failures.append(f"node {node} snapshot/application/commit boundaries are incoherent")
            completed = new_completed if restarted else new_completed - old_completed
            installs = new_installs if restarted else new_installs - old_installs
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
        except (KeyError, TypeError, ValueError) as error:
            failures.append(f"node {node} snapshot activity is invalid: {error}")

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
        "schema": 1,
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

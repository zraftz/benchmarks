"""Validate and isolate bounded operation timelines from diagnostic node status."""
from __future__ import annotations
from typing import Any


STAGES = (
    "client_ingress",
    "owner_admitted",
    "proposal_submitted",
    "raft_commit_observed",
    "application_dispatched",
    "application_started",
    "application_durable",
    "client_completion",
)
COMMIT_BOUNDARIES = ("engine_apply_effect", "state_machine_apply_callback")
COUNTERS = (
    "operations_seen",
    "operations_sampled",
    "completed_seen",
    "abandoned",
    "skipped_in_flight",
    "discarded_incomplete",
    "evicted_commit_marks",
    "in_flight",
    "commit_marks",
)
LIMITS = ("sample_every", "retained_limit", "in_flight_limit", "commit_mark_limit")


def _nonnegative(value: Any, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"invalid timeline count: {name}")
    return value


def _nodes(snapshot: dict) -> dict[str, dict]:
    if not isinstance(snapshot, dict):
        raise ValueError("timeline status snapshot is not an object")
    nodes = {str(node): status for node, status in snapshot.items()}
    if not nodes:
        raise ValueError("timeline status snapshot has no nodes")
    return nodes


def _state(status: dict, node: str) -> dict:
    if not isinstance(status, dict) or status.get("status") != "ok":
        raise ValueError(f"node {node} has no successful timeline status")
    try:
        diagnostics = status["info"]["diagnostics"]
        state = diagnostics["operation_timelines"]
    except (KeyError, TypeError) as exc:
        raise ValueError(f"node {node} has no operation timelines") from exc
    if not isinstance(diagnostics, dict) or not isinstance(state, dict):
        raise ValueError(f"node {node} operation timelines are malformed")
    if diagnostics.get("enabled") is not True or state.get("schema") != 1:
        raise ValueError(f"node {node} operation timelines are disabled or unsupported")
    for name in COUNTERS:
        _nonnegative(state.get(name), f"node {node} {name}")
    for name in LIMITS:
        if _nonnegative(state.get(name), f"node {node} {name}") == 0:
            raise ValueError(f"node {node} timeline limit is zero: {name}")
    seen = state["operations_seen"]
    expected_sampled = 0 if seen == 0 else 1 + (seen - 1) // state["sample_every"]
    if state["operations_sampled"] != expected_sampled:
        raise ValueError(f"node {node} sampled-operation accounting mismatch")
    accounted = (
        state["completed_seen"]
        + state["abandoned"]
        + state["skipped_in_flight"]
        + state["discarded_incomplete"]
        + state["in_flight"]
    )
    if state["operations_sampled"] != accounted:
        raise ValueError(f"node {node} timeline lifecycle accounting mismatch")
    if state["in_flight"] > state["in_flight_limit"]:
        raise ValueError(f"node {node} timeline in-flight bound exceeded")
    if state["commit_marks"] > state["commit_mark_limit"]:
        raise ValueError(f"node {node} commit-mark bound exceeded")
    retained = state.get("retained")
    if not isinstance(retained, list) or len(retained) > state["retained_limit"]:
        raise ValueError(f"node {node} retained timeline bound exceeded")
    if len(retained) != min(state["completed_seen"], state["retained_limit"]):
        raise ValueError(f"node {node} retained timeline accounting mismatch")
    ordinals = set()
    for timeline in retained:
        if not isinstance(timeline, dict):
            raise ValueError(f"node {node} retained timeline is not an object")
        ordinal = _nonnegative(timeline.get("sample_ordinal"), "sample_ordinal")
        index = _nonnegative(timeline.get("log_index"), "log_index")
        if ordinal == 0 or ordinal > seen or (ordinal - 1) % state["sample_every"]:
            raise ValueError(f"node {node} retained sample ordinal is invalid")
        if ordinal in ordinals:
            raise ValueError(f"node {node} retained sample ordinal is duplicated")
        if index == 0:
            raise ValueError(f"node {node} retained log index is zero")
        ordinals.add(ordinal)
        if timeline.get("commit_boundary") not in COMMIT_BOUNDARIES:
            raise ValueError(f"node {node} has an unsupported commit boundary")
        points = timeline.get("points_ns")
        if not isinstance(points, dict) or set(points) != set(STAGES):
            raise ValueError(f"node {node} timeline stage inventory differs")
        values = [_nonnegative(points.get(stage), stage) for stage in STAGES]
        if values[0] != 0 or values != sorted(values):
            raise ValueError(f"node {node} timeline stages are not monotonic")
    return state


def extract(before: dict, after: dict) -> dict:
    """Return only samples admitted after the pre-measurement status watermark."""
    before_nodes = _nodes(before)
    after_nodes = _nodes(after)
    if set(before_nodes) != set(after_nodes):
        raise ValueError("timeline node inventory changed during measurement")
    nodes = {}
    retained_total = 0
    for node in sorted(before_nodes, key=lambda value: int(value)):
        earlier = _state(before_nodes[node], node)
        later = _state(after_nodes[node], node)
        if any(earlier[name] != later[name] for name in LIMITS):
            raise ValueError(f"node {node} timeline configuration changed")
        watermark = earlier["operations_seen"]
        if later["operations_seen"] < watermark:
            raise ValueError(f"node {node} operation timeline counter regressed")
        retained = [
            timeline
            for timeline in later["retained"]
            if timeline["sample_ordinal"] > watermark
        ]
        retained.sort(key=lambda timeline: timeline["sample_ordinal"])
        retained_total += len(retained)
        nodes[node] = {
            "before_operations_seen": watermark,
            "after_operations_seen": later["operations_seen"],
            "measurement_operations_seen": later["operations_seen"] - watermark,
            "retained_after_total": len(later["retained"]),
            "retained_measurement_timelines": retained,
        }
    return {
        "schema": 1,
        "scope": "bounded samples admitted after the pre-measurement watermark; callback timestamps, not kernel commit timestamps",
        "commit_boundaries": {
            "engine_apply_effect": "first committed apply effect observed by the shared Rafter or raft-rs owner",
            "state_machine_apply_callback": "entry to OpenRaft's committed state-machine apply callback",
        },
        "stages": list(STAGES),
        "retained_measurement_timelines": retained_total,
        "nodes": nodes,
    }

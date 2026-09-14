"""Replay live service snapshot and WAL-reclamation activity from node status."""
from __future__ import annotations

from typing import Any


NATIVE_SNAPSHOT_BASE_STAGES = (
    "snapshot_publication",
    "snapshot_data_write",
    "snapshot_data_sync",
    "snapshot_file_publish",
    "snapshot_manifest_publish",
    "snapshot_prune",
)
NATIVE_SNAPSHOT_KERNEL_STAGES = (
    "snapshot_kernel_prepare",
    "snapshot_kernel_commit",
)
NATIVE_SNAPSHOT_STAGES = NATIVE_SNAPSHOT_BASE_STAGES + NATIVE_SNAPSHOT_KERNEL_STAGES
APPLICATION_CHECKPOINT_STAGES = (
    "journal_checkpoint_write_ns",
    "journal_checkpoint_sync_ns",
    "journal_checkpoint_publish_ns",
)


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


def _native_histogram(values: Any, calls: int, node: str, stage: str) -> list[int]:
    if (
        not isinstance(values, list)
        or len(values) > 64
        or any(type(value) is not int or value < 0 for value in values)
        or sum(values) != calls
    ):
        raise ValueError(f"node {node} {stage} native histogram is invalid")
    return values + [0] * (64 - len(values))


def _native_snapshot_delta(
    before_info: dict, after_info: dict, restarted: bool, node: str
) -> dict[str, dict[str, Any]] | None:
    old = before_info.get("engine", {}).get("persistence_diagnostics")
    new = after_info.get("engine", {}).get("persistence_diagnostics")
    if old is None and new is None:
        return None
    if not isinstance(old, dict) or not isinstance(new, dict):
        raise ValueError(f"node {node} native snapshot stage availability changed")
    base_present = [stage in old or stage in new for stage in NATIVE_SNAPSHOT_BASE_STAGES]
    if not any(base_present):
        return None
    if not all(stage in old and stage in new for stage in NATIVE_SNAPSHOT_BASE_STAGES):
        raise ValueError(f"node {node} native snapshot stages are incomplete")
    kernel_present = [stage in old or stage in new for stage in NATIVE_SNAPSHOT_KERNEL_STAGES]
    if any(kernel_present) and not all(
        stage in old and stage in new for stage in NATIVE_SNAPSHOT_KERNEL_STAGES
    ):
        raise ValueError(f"node {node} native snapshot kernel stages are incomplete")
    stages = (
        NATIVE_SNAPSHOT_STAGES
        if all(kernel_present)
        else NATIVE_SNAPSHOT_BASE_STAGES
    )

    result = {}
    for stage in stages:
        first = old[stage]
        last = new[stage]
        if not isinstance(first, dict) or not isinstance(last, dict):
            raise ValueError(f"node {node} {stage} native metric is invalid")
        expected = {"calls", "total_ns", "max_ns", "buckets_ns_log2"}
        if set(first) != expected or set(last) != expected:
            raise ValueError(f"node {node} {stage} native metric shape changed")
        for boundary, metric in (("before", first), ("after", last)):
            if any(
                type(metric[name]) is not int or metric[name] < 0
                for name in ("calls", "total_ns", "max_ns")
            ):
                raise ValueError(
                    f"node {node} {stage} {boundary} native counters are invalid"
                )
        first_buckets = _native_histogram(
            first["buckets_ns_log2"], first["calls"], node, stage
        )
        last_buckets = _native_histogram(
            last["buckets_ns_log2"], last["calls"], node, stage
        )
        calls = last["calls"] if restarted else last["calls"] - first["calls"]
        total_ns = last["total_ns"] if restarted else last["total_ns"] - first["total_ns"]
        buckets = (
            last_buckets
            if restarted
            else [right - left for left, right in zip(first_buckets, last_buckets, strict=True)]
        )
        if calls < 0 or total_ns < 0 or any(value < 0 for value in buckets):
            raise ValueError(f"node {node} {stage} native counters regressed")
        if sum(buckets) != calls:
            raise ValueError(f"node {node} {stage} native histogram delta disagrees")
        result[stage] = {
            "samples": calls,
            "total_ns": total_ns,
            "mean_ns": total_ns / calls if calls else None,
            "max_upper_bound_ns": _upper_bound_ns(buckets),
            "buckets_ns_log2": buckets,
        }
    return result


def _aggregate_native_stages(nodes: dict[str, dict]) -> dict[str, dict[str, Any]]:
    result = {}
    stage_sets = {
        tuple(node["native_snapshot_stages"])
        for node in nodes.values()
    }
    if len(stage_sets) != 1:
        raise ValueError("native snapshot stage availability differs between nodes")
    present = set(next(iter(stage_sets)))
    for stage in (stage for stage in NATIVE_SNAPSHOT_STAGES if stage in present):
        metrics = [node["native_snapshot_stages"][stage] for node in nodes.values()]
        buckets = [
            sum(metric["buckets_ns_log2"][index] for metric in metrics)
            for index in range(64)
        ]
        samples = sum(metric["samples"] for metric in metrics)
        total_ns = sum(metric["total_ns"] for metric in metrics)
        result[stage] = {
            "samples": samples,
            "total_ns": total_ns,
            "mean_ns": total_ns / samples if samples else None,
            "max_upper_bound_ns": _upper_bound_ns(buckets),
            "buckets_ns_log2": buckets,
        }
    return result


def _application_metric(info: dict, stage: str, node: str) -> dict[str, Any]:
    try:
        metrics = info["application"]["diagnostics"]["metrics"]
    except (KeyError, TypeError) as error:
        raise ValueError(f"node {node} application diagnostics are unavailable") from error
    if not isinstance(metrics, dict):
        raise ValueError(f"node {node} application diagnostics are invalid")
    metric = metrics.get(stage)
    if metric is None:
        return {"samples": 0, "total": 0, "max": 0, "buckets_log2": [0] * 64}
    if not isinstance(metric, dict) or set(metric) != {
        "samples", "total", "max", "buckets_log2"
    }:
        raise ValueError(f"node {node} {stage} application metric shape changed")
    samples = metric["samples"]
    total = metric["total"]
    maximum = metric["max"]
    buckets = metric["buckets_log2"]
    if any(type(value) is not int or value < 0 for value in (samples, total, maximum)):
        raise ValueError(f"node {node} {stage} application counters are invalid")
    buckets = _native_histogram(buckets, samples, node, stage)
    return {"samples": samples, "total": total, "max": maximum, "buckets_log2": buckets}


def _application_checkpoint_delta(
    before_info: dict, after_info: dict, restarted: bool, node: str
) -> dict[str, dict[str, Any]]:
    result = {}
    for stage in APPLICATION_CHECKPOINT_STAGES:
        first = _application_metric(before_info, stage, node)
        last = _application_metric(after_info, stage, node)
        samples = last["samples"] if restarted else last["samples"] - first["samples"]
        total_ns = last["total"] if restarted else last["total"] - first["total"]
        buckets = (
            last["buckets_log2"]
            if restarted
            else [
                right - left
                for left, right in zip(
                    first["buckets_log2"], last["buckets_log2"], strict=True
                )
            ]
        )
        if samples < 0 or total_ns < 0 or any(value < 0 for value in buckets):
            raise ValueError(f"node {node} {stage} application counters regressed")
        if sum(buckets) != samples:
            raise ValueError(f"node {node} {stage} application histogram delta disagrees")
        result[stage] = {
            "samples": samples,
            "total_ns": total_ns,
            "mean_ns": total_ns / samples if samples else None,
            "max_upper_bound_ns": _upper_bound_ns(buckets),
            "buckets_ns_log2": buckets,
        }
    return result


def _aggregate_application_stages(nodes: dict[str, dict]) -> dict[str, dict[str, Any]]:
    result = {}
    for stage in APPLICATION_CHECKPOINT_STAGES:
        metrics = [node["application_checkpoint_stages"][stage] for node in nodes.values()]
        buckets = [
            sum(metric["buckets_ns_log2"][index] for metric in metrics)
            for index in range(64)
        ]
        samples = sum(metric["samples"] for metric in metrics)
        total_ns = sum(metric["total_ns"] for metric in metrics)
        result[stage] = {
            "samples": samples,
            "total_ns": total_ns,
            "mean_ns": total_ns / samples if samples else None,
            "max_upper_bound_ns": _upper_bound_ns(buckets),
            "buckets_ns_log2": buckets,
        }
    return result


def _retirement_delta(
    before_info: dict, after_info: dict, restarted: bool, node: str
) -> dict[str, int]:
    first = before_info.get("engine", {}).get("log_retirement")
    last = after_info.get("engine", {}).get("log_retirement")
    expected = {
        "enabled",
        "max_inflight_entries",
        "max_inflight_payload_bytes",
        "accepted_batches",
        "accepted_entries",
        "inline_fallback_batches",
        "inflight_entries",
        "inflight_payload_bytes",
    }
    if not isinstance(first, dict) or not isinstance(last, dict):
        raise ValueError(f"node {node} retired-log worker status is unavailable")
    if set(first) != expected or set(last) != expected:
        raise ValueError(f"node {node} retired-log worker status shape changed")
    if first["enabled"] is not True or last["enabled"] is not True:
        raise ValueError(f"node {node} retired-log worker is not enabled")
    numeric = expected - {"enabled"}
    if any(type(value[name]) is not int or value[name] < 0
           for value in (first, last) for name in numeric):
        raise ValueError(f"node {node} retired-log worker counters are invalid")
    for limit, inflight in (
        ("max_inflight_entries", "inflight_entries"),
        ("max_inflight_payload_bytes", "inflight_payload_bytes"),
    ):
        if first[limit] == 0 or first[limit] != last[limit]:
            raise ValueError(f"node {node} retired-log worker limit changed")
        if first[inflight] > first[limit] or last[inflight] > last[limit]:
            raise ValueError(f"node {node} retired-log worker exceeded its credits")
    deltas = {}
    for name in ("accepted_batches", "accepted_entries", "inline_fallback_batches"):
        delta = last[name] if restarted else last[name] - first[name]
        if delta < 0:
            raise ValueError(f"node {node} retired-log worker counters regressed")
        deltas[name] = delta
    return {
        **deltas,
        "max_inflight_entries": last["max_inflight_entries"],
        "max_inflight_payload_bytes": last["max_inflight_payload_bytes"],
        "inflight_entries": last["inflight_entries"],
        "inflight_payload_bytes": last["inflight_payload_bytes"],
    }


def activity(
    before: dict,
    after: dict,
    interval: int,
    scenario: str,
    *,
    restarted_node: int | None = None,
    stopped_application_index: int | None = None,
    required_snapshot_index: int | None = None,
    native_snapshot_stages: bool = False,
    require_native_snapshot_stage_activity: bool = False,
    application_checkpoint_stages: bool = False,
    require_application_checkpoint_stage_activity: bool = False,
    retired_log_worker: bool = False,
    require_retired_log_worker_activity: bool = False,
) -> dict[str, Any]:
    failures = []
    before_nodes = _nodes(before)
    after_nodes = _nodes(after)
    if set(before_nodes) != {"1", "2", "3"} or set(after_nodes) != {"1", "2", "3"}:
        failures.append("snapshot activity requires status from all three nodes")

    nodes = {}
    histogram_nodes = 0
    native_stage_nodes = 0
    application_stage_nodes = 0
    retirement_worker_nodes = 0
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
            if native_snapshot_stages:
                native_stages = _native_snapshot_delta(before_info, after_info, restarted, node)
                if native_stages is not None:
                    native_stage_nodes += 1
                    nodes[node]["native_snapshot_stages"] = native_stages
            if application_checkpoint_stages:
                application_stages = _application_checkpoint_delta(
                    before_info, after_info, restarted, node
                )
                application_stage_nodes += 1
                nodes[node]["application_checkpoint_stages"] = application_stages
            if retired_log_worker:
                retirement = _retirement_delta(before_info, after_info, restarted, node)
                retirement_worker_nodes += 1
                nodes[node]["log_retirement"] = retirement
        except (KeyError, TypeError, ValueError) as error:
            failures.append(f"node {node} snapshot activity is invalid: {error}")

    if histogram_nodes not in (0, 3):
        failures.append("snapshot compaction histograms are not available on all three nodes")
    if require_native_snapshot_stage_activity and not native_snapshot_stages:
        failures.append("required native snapshot stages were not selected")
    if native_snapshot_stages and native_stage_nodes != 3:
        failures.append("native snapshot stages are not available on all three nodes")
    if require_application_checkpoint_stage_activity and not application_checkpoint_stages:
        failures.append("required application checkpoint stages were not selected")
    if application_checkpoint_stages and application_stage_nodes != 3:
        failures.append("application checkpoint stages are not available on all three nodes")
    if require_retired_log_worker_activity and not retired_log_worker:
        failures.append("required retired-log worker observation was not selected")
    if retired_log_worker and retirement_worker_nodes != 3:
        failures.append("retired-log worker status is not available on all three nodes")

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
    totals = {
        "compactions_since_before_status": compactions,
        "application_installs_since_before_status": installs,
    }
    if native_stage_nodes == 3:
        try:
            native_totals = _aggregate_native_stages(nodes)
        except ValueError as error:
            failures.append(str(error))
        else:
            totals["native_snapshot_stages"] = native_totals
            missing = [
                stage for stage, metric in native_totals.items() if metric["samples"] == 0
            ]
            if require_native_snapshot_stage_activity and missing:
                failures.append(
                    "no native snapshot stage activity observed: " + ", ".join(missing)
                )
    if application_stage_nodes == 3:
        application_totals = _aggregate_application_stages(nodes)
        totals["application_checkpoint_stages"] = application_totals
        missing = [
            stage for stage, metric in application_totals.items() if metric["samples"] == 0
        ]
        if require_application_checkpoint_stage_activity and missing:
            failures.append(
                "no application checkpoint stage activity observed: " + ", ".join(missing)
            )
    if retirement_worker_nodes == 3:
        retirement_totals = {
            name: sum(node["log_retirement"][name] for node in nodes.values())
            for name in ("accepted_batches", "accepted_entries", "inline_fallback_batches")
        }
        totals["log_retirement"] = retirement_totals
        if require_retired_log_worker_activity and retirement_totals["accepted_batches"] == 0:
            failures.append("no retired log prefix was accepted by the bounded worker")
        if require_retired_log_worker_activity and retirement_totals["accepted_entries"] == 0:
            failures.append("bounded retired-log work contained no entries")
        if require_retired_log_worker_activity and retirement_totals["inline_fallback_batches"]:
            failures.append("retired log prefixes fell back to owner-thread destruction")
    return {
        "schema": (
            5
            if (
                native_snapshot_stages
                and application_checkpoint_stages
                and histogram_nodes == native_stage_nodes == application_stage_nodes == 3
                and all(
                    stage in totals.get("native_snapshot_stages", {})
                    for stage in NATIVE_SNAPSHOT_KERNEL_STAGES
                )
            )
            else 4
            if (
                native_snapshot_stages
                and application_checkpoint_stages
                and histogram_nodes == native_stage_nodes == application_stage_nodes == 3
            )
            else (
                3
                if native_snapshot_stages and histogram_nodes == native_stage_nodes == 3
                else (2 if histogram_nodes == 3 else 1)
            )
        ),
        "status": "passed" if not failures else "failed",
        "interval_entries": interval,
        "scenario": scenario,
        "restarted_node": restarted_node,
        "stopped_application_index": stopped_application_index,
        "required_snapshot_index": required_snapshot_index,
        "requirements": {
            "live_compaction": True,
            "snapshot_transfer_and_application_install": require_transfer,
            **(
                {"native_snapshot_stage_activity": True}
                if require_native_snapshot_stage_activity
                else {}
            ),
            **(
                {"application_checkpoint_stage_activity": True}
                if require_application_checkpoint_stage_activity
                else {}
            ),
            **(
                {
                    "retired_log_worker_activity": True,
                    "no_inline_retirement_fallback": True,
                }
                if require_retired_log_worker_activity
                else {}
            ),
        },
        "totals": totals,
        "nodes": nodes,
        "failures": failures,
    }

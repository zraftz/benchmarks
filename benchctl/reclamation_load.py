"""Predeclared assessment for same-code WAL reclamation under service load."""
from __future__ import annotations

from collections import defaultdict
from statistics import median
from typing import Any

from .reclamation_service import (
    APPLICATION_CHECKPOINT_STAGES,
    NATIVE_SNAPSHOT_BASE_STAGES,
    NATIVE_SNAPSHOT_STAGES,
    NATIVE_WAL_RECLAMATION_STAGES,
)
from .storage_footprint import MANAGED_RAFT_CATEGORIES


OBJECTIVE = {
    "minimum_fixed_load_achieved_percent": 99.0,
    "minimum_saturated_throughput_ratio": 0.95,
    "maximum_equal_load_p99_regression_ratio": 1.10,
    "maximum_equal_load_p99_absolute_regression_ms": 1.0,
    "maximum_equal_load_p999_regression_ratio": 1.10,
    "maximum_equal_load_p999_absolute_regression_ms": 2.0,
    "loss_counts": {"errors": 0, "unknown": 0, "not_issued": 0},
    "reclamation_activity": "at least one measured-load compaction per snapshot arm and repetition",
    "managed_raft_allocation": (
        "post-load and post-restart WAL plus snapshot allocated bytes below the same-seed "
        "no-snapshot control"
    ),
    "snapshot_artifact_bound": (
        "after final restart, exactly one selected snapshot envelope and manifest per node, "
        "with no temporary snapshot artifacts"
    ),
    "application_journal": (
        "post-load and post-restart allocated bytes below the same-seed no-snapshot control; "
        "one authoritative journal and no checkpoint temporary per node after restart"
    ),
}


def _timing_cases(data: dict) -> list[dict]:
    return [
        case
        for case in data.get("cases", [])
        if not case.get("smoke") and case.get("measurement_mode") == "timing"
    ]


def _category_value(case: dict, checkpoint: str, category: str, field: str) -> int:
    try:
        value = case["storage_footprint"]["snapshots"][checkpoint]["totals"][category][
            field
        ]
    except (KeyError, TypeError) as error:
        raise ValueError(f"missing {checkpoint} {category} {field}") from error
    if type(value) is not int or value < 0:
        raise ValueError(f"invalid {checkpoint} {category} {field}")
    return value


def _allocated(case: dict, category: str, checkpoint: str = "after_measurement") -> int:
    return _category_value(case, checkpoint, category, "allocated_bytes")


def _legacy_wal_allocated(case: dict, checkpoint: str = "after_measurement") -> int:
    """Count the initial RFWB generation stored at the legacy pathname.

    Before its first reclamation, the combined WAL deliberately occupies
    ``raft/hard-state``. The generic footprint receipt classifies that
    backend-neutral pathname as ``other``; this assessment knows the selected
    backend is WAL and must include the file in the no-reclamation control.
    """
    if case.get("configuration", {}).get("hard_state") != "wal":
        return 0
    try:
        nodes = case["storage_footprint"]["snapshots"][checkpoint]["nodes"]
    except (KeyError, TypeError) as error:
        raise ValueError(f"missing {checkpoint} storage node inventory") from error
    if not isinstance(nodes, dict) or set(nodes) != {"1", "2", "3"}:
        raise ValueError(f"invalid {checkpoint} storage node inventory")
    allocated = 0
    for node, observed in nodes.items():
        try:
            files = observed["files"]
        except (KeyError, TypeError) as error:
            raise ValueError(f"missing {checkpoint} storage files for node {node}") from error
        if not isinstance(files, list):
            raise ValueError(f"invalid {checkpoint} storage files for node {node}")
        matches = [
            item
            for item in files
            if isinstance(item, dict)
            and item.get("path") == f"node-{node}/raft/hard-state"
        ]
        if len(matches) > 1:
            raise ValueError(f"duplicate {checkpoint} initial WAL for node {node}")
        if matches:
            item = matches[0]
            value = item.get("allocated_bytes")
            category = item.get("category")
            if type(value) is not int or value < 0:
                raise ValueError(f"invalid {checkpoint} initial WAL allocation for node {node}")
            # A future footprint schema may classify this path directly. Avoid
            # counting it twice when that happens.
            if category != "raft_wal_data":
                allocated += value
    return allocated


def _wal_allocated(case: dict, checkpoint: str = "after_measurement") -> int:
    return _allocated(case, "raft_wal_data", checkpoint) + _legacy_wal_allocated(
        case, checkpoint
    )


def _managed_raft_allocated(case: dict, checkpoint: str = "after_measurement") -> int:
    categorized = sum(
        _allocated(case, category, checkpoint) for category in MANAGED_RAFT_CATEGORIES
    )
    return categorized + _legacy_wal_allocated(case, checkpoint)


def _final_snapshot_artifacts(case: dict) -> dict[str, dict[str, int]]:
    try:
        nodes = case["storage_footprint"]["snapshots"]["after_final_restart"]["nodes"]
    except (KeyError, TypeError) as error:
        raise ValueError("missing after_final_restart node inventory") from error
    if not isinstance(nodes, dict) or set(nodes) != {"1", "2", "3"}:
        raise ValueError("invalid after_final_restart node inventory")
    result = {}
    for node, observed in nodes.items():
        try:
            totals = observed["totals"]
            data = totals["raft_snapshot_data"]["files"]
            metadata = totals["raft_snapshot_metadata"]["files"]
            temporary = totals["raft_snapshot_temporary"]["files"]
        except (KeyError, TypeError) as error:
            raise ValueError(
                f"missing after_final_restart snapshot artifacts for node {node}"
            ) from error
        if type(data) is not int or data < 0:
            raise ValueError(
                f"invalid after_final_restart snapshot data files for node {node}"
            )
        if type(metadata) is not int or metadata < 0:
            raise ValueError(
                f"invalid after_final_restart snapshot metadata files for node {node}"
            )
        if type(temporary) is not int or temporary < 0:
            raise ValueError(
                f"invalid after_final_restart temporary snapshot files for node {node}"
            )
        result[node] = {
            "data_files": data,
            "metadata_files": metadata,
            "temporary_files": temporary,
        }
    return result


def _final_application_artifacts(case: dict) -> dict[str, dict[str, int]]:
    try:
        nodes = case["storage_footprint"]["snapshots"]["after_final_restart"]["nodes"]
    except (KeyError, TypeError) as error:
        raise ValueError("missing after_final_restart node inventory") from error
    if not isinstance(nodes, dict) or set(nodes) != {"1", "2", "3"}:
        raise ValueError("invalid after_final_restart storage node inventory")
    result = {}
    for node, observed in nodes.items():
        try:
            files = observed["files"]
        except (KeyError, TypeError) as error:
            raise ValueError(
                f"missing after_final_restart storage files for node {node}"
            ) from error
        if not isinstance(files, list):
            raise ValueError(f"invalid after_final_restart storage files for node {node}")
        paths = [item.get("path") for item in files if isinstance(item, dict)]
        result[node] = {
            "journal_files": paths.count(f"node-{node}/application.wal"),
            "temporary_files": paths.count(f"node-{node}/application.wal.checkpoint.tmp"),
        }
    return result


def _restart_ns(case: dict) -> int:
    try:
        value = case["recovery"]["timing"]["process_restart_ns"]
    except (KeyError, TypeError) as error:
        raise ValueError("missing final process restart timing") from error
    if type(value) is not int or value <= 0:
        raise ValueError("invalid final process restart timing")
    return value


def _compaction(case: dict) -> tuple[int, int]:
    receipt = case.get("snapshot_reclamation")
    if not isinstance(receipt, dict):
        raise ValueError("missing live reclamation activity")
    samples = 0
    maximum = 0
    for node in receipt.get("nodes", {}).values():
        measurement = node.get("measurement_compaction", {})
        count = measurement.get("samples")
        upper = measurement.get("max_upper_bound_ns")
        if type(count) is not int or count < 0:
            raise ValueError("invalid measured-load compaction count")
        if upper is not None and (type(upper) is not int or upper <= 0):
            raise ValueError("invalid measured-load compaction maximum")
        samples += count
        maximum = max(maximum, upper or 0)
    if samples == 0:
        raise ValueError("no measured-load compaction was observed")
    return samples, maximum


def _native_snapshot_stages(case: dict) -> dict[str, dict[str, int | float | None]]:
    receipt = case.get("snapshot_reclamation")
    if not isinstance(receipt, dict):
        raise ValueError("missing live reclamation activity")
    totals = receipt.get("totals")
    if not isinstance(totals, dict):
        raise ValueError("live reclamation totals are invalid")
    raw = totals.get("native_snapshot_stages")
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("native snapshot stage set changed")
    allowed_stage_sets = (
        set(NATIVE_SNAPSHOT_BASE_STAGES),
        set(NATIVE_SNAPSHOT_STAGES),
        set(NATIVE_SNAPSHOT_STAGES + NATIVE_WAL_RECLAMATION_STAGES),
    )
    if set(raw) not in allowed_stage_sets:
        raise ValueError("native snapshot stage set changed")
    stages = tuple(
        stage
        for stage in NATIVE_SNAPSHOT_STAGES + NATIVE_WAL_RECLAMATION_STAGES
        if stage in raw
    )
    if receipt.get("schema") in (5, 6) and set(stages) != set(
        NATIVE_SNAPSHOT_STAGES
    ):
        raise ValueError("schema 5 or 6 native snapshot kernel stages are missing")
    if receipt.get("schema") == 7 and set(stages) != allowed_stage_sets[-1]:
        raise ValueError("schema 7 native WAL reclamation stages are missing")
    result = {}
    for stage in stages:
        metric = raw[stage]
        if not isinstance(metric, dict):
            raise ValueError(f"invalid {stage} metric")
        samples = metric.get("samples")
        total_ns = metric.get("total_ns")
        maximum = metric.get("max_upper_bound_ns")
        if type(samples) is not int or samples < 0:
            raise ValueError(f"invalid {stage} sample count")
        if type(total_ns) is not int or total_ns < 0:
            raise ValueError(f"invalid {stage} total")
        if maximum is not None and (type(maximum) is not int or maximum <= 0):
            raise ValueError(f"invalid {stage} maximum upper bound")
        if (samples == 0) != (maximum is None):
            raise ValueError(f"incoherent {stage} samples and maximum")
        result[stage] = {
            "samples": samples,
            "total_ns": total_ns,
            "mean_ns": total_ns / samples if samples else None,
            "max_upper_bound_ns": maximum,
        }
    return result


def _application_checkpoint_stages(
    case: dict,
) -> dict[str, dict[str, int | float | None]]:
    receipt = case.get("snapshot_reclamation")
    if not isinstance(receipt, dict):
        raise ValueError("missing live reclamation activity")
    totals = receipt.get("totals")
    if not isinstance(totals, dict):
        raise ValueError("live reclamation totals are invalid")
    raw = totals.get("application_checkpoint_stages")
    if raw is None:
        if receipt.get("schema") in (4, 5, 6, 7):
            raise ValueError(
                "application checkpoint stages are missing from schema 4, 5, 6, or 7 receipt"
            )
        return {}
    if not isinstance(raw, dict) or set(raw) != set(APPLICATION_CHECKPOINT_STAGES):
        raise ValueError("application checkpoint stage set changed")
    result = {}
    for stage in APPLICATION_CHECKPOINT_STAGES:
        metric = raw[stage]
        if not isinstance(metric, dict):
            raise ValueError(f"invalid {stage} metric")
        samples = metric.get("samples")
        total_ns = metric.get("total_ns")
        maximum = metric.get("max_upper_bound_ns")
        if type(samples) is not int or samples < 0:
            raise ValueError(f"invalid {stage} sample count")
        if type(total_ns) is not int or total_ns < 0:
            raise ValueError(f"invalid {stage} total")
        if maximum is not None and (type(maximum) is not int or maximum <= 0):
            raise ValueError(f"invalid {stage} maximum upper bound")
        if (samples == 0) != (maximum is None):
            raise ValueError(f"incoherent {stage} samples and maximum")
        result[stage] = {
            "samples": samples,
            "total_ns": total_ns,
            "mean_ns": total_ns / samples if samples else None,
            "max_upper_bound_ns": maximum,
        }
    return result


def _application_snapshot_encode(
    case: dict,
) -> dict[str, int | float | None]:
    receipt = case.get("snapshot_reclamation")
    if not isinstance(receipt, dict):
        raise ValueError("missing live reclamation activity")
    totals = receipt.get("totals")
    if not isinstance(totals, dict):
        raise ValueError("live reclamation totals are invalid")
    metric = totals.get("application_snapshot_encode")
    if metric is None:
        if receipt.get("schema") in (6, 7):
            raise ValueError(
                "application snapshot encode timing is missing from schema 6 or 7 receipt"
            )
        return {}
    if not isinstance(metric, dict):
        raise ValueError("invalid application snapshot encode metric")
    samples = metric.get("samples")
    total_ns = metric.get("total_ns")
    maximum = metric.get("max_upper_bound_ns")
    if type(samples) is not int or samples < 0:
        raise ValueError("invalid application snapshot encode sample count")
    if type(total_ns) is not int or total_ns < 0:
        raise ValueError("invalid application snapshot encode total")
    if maximum is not None and (type(maximum) is not int or maximum <= 0):
        raise ValueError("invalid application snapshot encode maximum upper bound")
    if (samples == 0) != (maximum is None):
        raise ValueError("incoherent application snapshot encode samples and maximum")
    return {
        "samples": samples,
        "total_ns": total_ns,
        "mean_ns": total_ns / samples if samples else None,
        "max_upper_bound_ns": maximum,
    }


def _tail_limit(control: float, *, ratio: float, absolute_ms: float) -> float:
    return control + max(control * (ratio - 1.0), absolute_ms)


def assess(data: dict) -> dict[str, Any]:
    failures: dict[str, list[str]] = {
        "evidence_and_correctness": [],
        "service_objective": [],
        "reclamation_activity": [],
        "physical_reclamation": [],
    }
    if data.get("suite", {}).get("kind") != "reclamation-under-load":
        failures["evidence_and_correctness"].append(
            "suite is not declared as reclamation-under-load"
        )
    if data.get("qualification") != "passed":
        failures["evidence_and_correctness"].extend(data.get("qualification_errors", []))

    cases = _timing_cases(data)
    diagnostic_grouped: dict[tuple[str, float], list[dict]] = defaultdict(list)
    for case in data.get("cases", []):
        if case.get("smoke") or case.get("measurement_mode") != "diagnostic":
            continue
        diagnostic_grouped[(
            case["configuration"]["variant"],
            case["workload"]["offered_per_second"],
        )].append(case)
    grouped: dict[tuple[str, float, int], list[dict]] = defaultdict(list)
    for case in cases:
        key = (
            case["configuration"]["variant"],
            case["workload"]["offered_per_second"],
            case["repetition"],
        )
        grouped[key].append(case)
    duplicates = [key for key, values in grouped.items() if len(values) != 1]
    if duplicates:
        failures["evidence_and_correctness"].append(
            "timing cases are not unique by variant, rate, and repetition"
        )

    suite_arms = data.get("suite", {}).get("arms", [])
    prior_labels = [arm[0] for arm in suite_arms if len(arm) >= 4 and arm[3] == "prior"]
    candidates = [arm for arm in suite_arms if len(arm) == 8 and arm[3] == "candidate"]
    if len(prior_labels) != 1:
        failures["evidence_and_correctness"].append(
            "reclamation comparison requires exactly one no-snapshot prior"
        )
    if not candidates:
        failures["evidence_and_correctness"].append(
            "reclamation comparison has no snapshot-aware candidate"
        )
    prior = prior_labels[0] if len(prior_labels) == 1 else "prior"
    if len(prior_labels) == 1:
        prior_arm = next(arm for arm in suite_arms if arm[0] == prior)
        if len(prior_arm) != 8 or prior_arm[7] != 0:
            failures["evidence_and_correctness"].append(
                "reclamation comparison prior is not explicitly snapshot-disabled"
            )
    rates = data.get("suite", {}).get("rates", [])
    runs = data.get("suite", {}).get("runs", 0)
    rows = []

    for arm in candidates:
        variant = arm[0]
        interval = arm[7]
        if type(interval) is not int or interval <= 0:
            failures["evidence_and_correctness"].append(
                f"candidate {variant} has no positive snapshot interval"
            )
            continue
        for rate in rates:
            paired = []
            for repetition in range(1, runs + 1):
                control_values = grouped.get((prior, rate, repetition), [])
                candidate_values = grouped.get((variant, rate, repetition), [])
                if len(control_values) != 1 or len(candidate_values) != 1:
                    failures["evidence_and_correctness"].append(
                        f"missing unique same-seed pair for {variant} at {rate}/s repetition {repetition}"
                    )
                    continue
                control = control_values[0]
                candidate = candidate_values[0]
                pair_failures = []
                control_configuration = {
                    key: value
                    for key, value in control.get("configuration", {}).items()
                    if key not in ("variant", "snapshot_interval_entries")
                }
                candidate_configuration = {
                    key: value
                    for key, value in candidate.get("configuration", {}).items()
                    if key not in ("variant", "snapshot_interval_entries")
                }
                if (
                    control.get("engine") != candidate.get("engine")
                    or control_configuration != candidate_configuration
                ):
                    failures["evidence_and_correctness"].append(
                        f"{variant} at {rate}/s repetition {repetition} differs from the control beyond snapshot interval"
                    )
                for label, case in ((prior, control), (variant, candidate)):
                    if case.get("qualification") != "passed" or case.get("metrics") is None:
                        pair_failures.append(f"{label} source case did not pass")
                        continue
                    accounting = case["accounting"]
                    for name, description in (
                        ("errors", "errors"),
                        ("unknown", "unknown outcomes"),
                        ("not_issued", "unsent requests"),
                    ):
                        count = accounting[name]
                        if count:
                            pair_failures.append(f"{label} had {count} {description}")
                    if rate > 0:
                        achieved = case["metrics"]["throughput_ops_s"] / rate * 100.0
                        if achieved < OBJECTIVE["minimum_fixed_load_achieved_percent"]:
                            pair_failures.append(f"{label} achieved only {achieved:.3f}% of offered load")
                try:
                    compactions, compaction_max = _compaction(candidate)
                except ValueError as error:
                    compactions, compaction_max = 0, 0
                    failures["reclamation_activity"].append(
                        f"{variant} at {rate}/s repetition {repetition}: {error}"
                    )
                try:
                    control_wal = _wal_allocated(control)
                    candidate_wal = _wal_allocated(candidate)
                    control_snapshot = _allocated(control, "raft_snapshot_data")
                    candidate_snapshot = _allocated(candidate, "raft_snapshot_data")
                    control_managed = _managed_raft_allocated(control)
                    candidate_managed = _managed_raft_allocated(candidate)
                    control_final_managed = _managed_raft_allocated(
                        control, "after_final_restart"
                    )
                    candidate_final_managed = _managed_raft_allocated(
                        candidate, "after_final_restart"
                    )
                    control_application = _allocated(control, "application_journal")
                    candidate_application = _allocated(candidate, "application_journal")
                    control_final_application = _allocated(
                        control, "application_journal", "after_final_restart"
                    )
                    candidate_final_application = _allocated(
                        candidate, "application_journal", "after_final_restart"
                    )
                    snapshot_artifacts_by_node = _final_snapshot_artifacts(candidate)
                    application_artifacts_by_node = _final_application_artifacts(candidate)
                    snapshot_data_files = sum(
                        item["data_files"]
                        for item in snapshot_artifacts_by_node.values()
                    )
                    snapshot_metadata_files = sum(
                        item["metadata_files"]
                        for item in snapshot_artifacts_by_node.values()
                    )
                    snapshot_temporary_files = sum(
                        item["temporary_files"]
                        for item in snapshot_artifacts_by_node.values()
                    )
                    if candidate_managed >= control_managed:
                        failures["physical_reclamation"].append(
                            f"{variant} at {rate}/s repetition {repetition} retained "
                            f"{candidate_managed} managed Raft bytes versus {control_managed} "
                            "without reclamation"
                        )
                    if candidate_final_managed >= control_final_managed:
                        failures["physical_reclamation"].append(
                            f"{variant} at {rate}/s repetition {repetition} retained "
                            f"{candidate_final_managed} managed Raft bytes after final restart "
                            f"versus {control_final_managed} without reclamation"
                        )
                    if candidate_application >= control_application:
                        failures["physical_reclamation"].append(
                            f"{variant} at {rate}/s repetition {repetition} retained "
                            f"{candidate_application} application-journal bytes versus "
                            f"{control_application} without checkpointing"
                        )
                    if candidate_final_application >= control_final_application:
                        failures["physical_reclamation"].append(
                            f"{variant} at {rate}/s repetition {repetition} retained "
                            f"{candidate_final_application} application-journal bytes after "
                            f"restart versus {control_final_application} without checkpointing"
                        )
                    for node, artifacts in snapshot_artifacts_by_node.items():
                        if artifacts["data_files"] != 1:
                            failures["physical_reclamation"].append(
                                f"{variant} at {rate}/s repetition {repetition} retained "
                                f"{artifacts['data_files']} snapshot data files on node {node} "
                                "after final restart"
                            )
                        if artifacts["metadata_files"] != 1:
                            failures["physical_reclamation"].append(
                                f"{variant} at {rate}/s repetition {repetition} retained "
                                f"{artifacts['metadata_files']} snapshot metadata files on node "
                                f"{node} after final restart"
                            )
                        if artifacts["temporary_files"] != 0:
                            failures["physical_reclamation"].append(
                                f"{variant} at {rate}/s repetition {repetition} retained "
                                f"{artifacts['temporary_files']} temporary snapshot files on node "
                                f"{node} after final restart"
                            )
                    for node, artifacts in application_artifacts_by_node.items():
                        if artifacts["journal_files"] != 1:
                            failures["physical_reclamation"].append(
                                f"{variant} at {rate}/s repetition {repetition} retained "
                                f"{artifacts['journal_files']} application journals on node {node} "
                                "after final restart"
                            )
                        if artifacts["temporary_files"] != 0:
                            failures["physical_reclamation"].append(
                                f"{variant} at {rate}/s repetition {repetition} retained "
                                f"{artifacts['temporary_files']} application checkpoint temporary "
                                f"files on node {node} after final restart"
                            )
                except ValueError as error:
                    control_wal = candidate_wal = 0
                    control_snapshot = candidate_snapshot = 0
                    control_managed = candidate_managed = 0
                    control_final_managed = candidate_final_managed = 0
                    control_application = candidate_application = 0
                    control_final_application = candidate_final_application = 0
                    snapshot_data_files = snapshot_metadata_files = 0
                    snapshot_temporary_files = 0
                    snapshot_artifacts_by_node = {}
                    application_artifacts_by_node = {}
                    failures["physical_reclamation"].append(
                        f"{variant} at {rate}/s repetition {repetition}: {error}"
                    )
                try:
                    restart_ns = _restart_ns(candidate)
                except ValueError as error:
                    restart_ns = 0
                    failures["evidence_and_correctness"].append(
                        f"{variant} at {rate}/s repetition {repetition}: {error}"
                    )

                control_metrics = control.get("metrics") or {}
                candidate_metrics = candidate.get("metrics") or {}
                comparable_metrics = all(
                    type(metrics.get(name)) in (int, float)
                    for metrics in (control_metrics, candidate_metrics)
                    for name in ("throughput_ops_s", "client_p99_ms", "client_p999_ms")
                )
                throughput_ratio = (
                    candidate_metrics["throughput_ops_s"]
                    / control_metrics["throughput_ops_s"]
                    if comparable_metrics and control_metrics["throughput_ops_s"] > 0
                    else 0.0
                )
                p99_regression = (
                    candidate_metrics["client_p99_ms"] - control_metrics["client_p99_ms"]
                    if comparable_metrics else 0.0
                )
                p999_regression = (
                    candidate_metrics["client_p999_ms"] - control_metrics["client_p999_ms"]
                    if comparable_metrics else 0.0
                )
                if rate == 0 and not pair_failures and (
                    throughput_ratio < OBJECTIVE["minimum_saturated_throughput_ratio"]
                ):
                    pair_failures.append(
                        f"{variant} retained only {throughput_ratio:.3f} of saturated throughput"
                    )
                if rate > 0 and not pair_failures:
                    control_p99 = control["metrics"]["client_p99_ms"]
                    control_p999 = control["metrics"]["client_p999_ms"]
                    if candidate["metrics"]["client_p99_ms"] > _tail_limit(
                        control_p99,
                        ratio=OBJECTIVE["maximum_equal_load_p99_regression_ratio"],
                        absolute_ms=OBJECTIVE["maximum_equal_load_p99_absolute_regression_ms"],
                    ):
                        pair_failures.append(
                            f"{variant} p99 exceeded the equal-load regression budget"
                        )
                    if candidate["metrics"]["client_p999_ms"] > _tail_limit(
                        control_p999,
                        ratio=OBJECTIVE["maximum_equal_load_p999_regression_ratio"],
                        absolute_ms=OBJECTIVE["maximum_equal_load_p999_absolute_regression_ms"],
                    ):
                        pair_failures.append(
                            f"{variant} p99.9 exceeded the equal-load regression budget"
                        )
                failures["service_objective"].extend(
                    f"{variant} at {rate}/s repetition {repetition}: {failure}"
                    for failure in pair_failures
                )
                paired.append({
                    "repetition": repetition,
                    "throughput_ratio": throughput_ratio,
                    "candidate_throughput_ops_s": candidate_metrics.get("throughput_ops_s"),
                    "control_throughput_ops_s": control_metrics.get("throughput_ops_s"),
                    "p99_regression_ms": p99_regression,
                    "p999_regression_ms": p999_regression,
                    "measured_compactions": compactions,
                    "compaction_max_upper_bound_ns": compaction_max,
                    "candidate_raft_wal_allocated_bytes": candidate_wal,
                    "control_raft_wal_allocated_bytes": control_wal,
                    "candidate_raft_snapshot_allocated_bytes": candidate_snapshot,
                    "control_raft_snapshot_allocated_bytes": control_snapshot,
                    "candidate_managed_raft_allocated_bytes": candidate_managed,
                    "control_managed_raft_allocated_bytes": control_managed,
                    "candidate_final_managed_raft_allocated_bytes": candidate_final_managed,
                    "control_final_managed_raft_allocated_bytes": control_final_managed,
                    "candidate_final_snapshot_data_files": snapshot_data_files,
                    "candidate_final_snapshot_metadata_files": snapshot_metadata_files,
                    "candidate_final_snapshot_temporary_files": snapshot_temporary_files,
                    "candidate_final_snapshot_artifacts_by_node": (
                        snapshot_artifacts_by_node
                    ),
                    "candidate_application_journal_allocated_bytes": candidate_application,
                    "control_application_journal_allocated_bytes": control_application,
                    "candidate_final_application_journal_allocated_bytes": (
                        candidate_final_application
                    ),
                    "control_final_application_journal_allocated_bytes": (
                        control_final_application
                    ),
                    "candidate_final_application_artifacts_by_node": (
                        application_artifacts_by_node
                    ),
                    "candidate_process_restart_ns": restart_ns,
                })
            if paired:
                native_snapshot_stages = {}
                application_checkpoint_stages = {}
                application_snapshot_encode = {}
                diagnostic_cases = diagnostic_grouped.get((variant, rate), [])
                if diagnostic_grouped and len(diagnostic_cases) != 1:
                    failures["reclamation_activity"].append(
                        f"{variant} at {rate}/s has {len(diagnostic_cases)} diagnostic cases"
                    )
                elif diagnostic_cases:
                    try:
                        native_snapshot_stages = _native_snapshot_stages(diagnostic_cases[0])
                        application_checkpoint_stages = _application_checkpoint_stages(
                            diagnostic_cases[0]
                        )
                        application_snapshot_encode = _application_snapshot_encode(
                            diagnostic_cases[0]
                        )
                    except ValueError as error:
                        failures["reclamation_activity"].append(
                            f"{variant} at {rate}/s diagnostic: {error}"
                        )
                row = {
                    "variant": variant,
                    "snapshot_interval_entries": interval,
                    "offered_per_second": rate,
                    "repetitions": paired,
                    "median_throughput_ratio": median(item["throughput_ratio"] for item in paired),
                    "worst_p99_regression_ms": max(item["p99_regression_ms"] for item in paired),
                    "worst_p999_regression_ms": max(item["p999_regression_ms"] for item in paired),
                    "measured_compactions": sum(item["measured_compactions"] for item in paired),
                    "max_compaction_upper_bound_ns": max(
                        item["compaction_max_upper_bound_ns"] for item in paired
                    ),
                    "median_candidate_raft_wal_allocated_bytes": median(
                        item["candidate_raft_wal_allocated_bytes"] for item in paired
                    ),
                    "median_control_raft_wal_allocated_bytes": median(
                        item["control_raft_wal_allocated_bytes"] for item in paired
                    ),
                    "median_candidate_raft_snapshot_allocated_bytes": median(
                        item["candidate_raft_snapshot_allocated_bytes"] for item in paired
                    ),
                    "median_candidate_managed_raft_allocated_bytes": median(
                        item["candidate_managed_raft_allocated_bytes"] for item in paired
                    ),
                    "median_control_managed_raft_allocated_bytes": median(
                        item["control_managed_raft_allocated_bytes"] for item in paired
                    ),
                    "median_candidate_final_managed_raft_allocated_bytes": median(
                        item["candidate_final_managed_raft_allocated_bytes"]
                        for item in paired
                    ),
                    "median_control_final_managed_raft_allocated_bytes": median(
                        item["control_final_managed_raft_allocated_bytes"]
                        for item in paired
                    ),
                    "max_candidate_final_snapshot_data_files": max(
                        item["candidate_final_snapshot_data_files"] for item in paired
                    ),
                    "max_candidate_final_snapshot_metadata_files": max(
                        item["candidate_final_snapshot_metadata_files"] for item in paired
                    ),
                    "max_candidate_final_snapshot_temporary_files": max(
                        item["candidate_final_snapshot_temporary_files"] for item in paired
                    ),
                    "median_candidate_application_journal_allocated_bytes": median(
                        item["candidate_application_journal_allocated_bytes"] for item in paired
                    ),
                    "median_control_application_journal_allocated_bytes": median(
                        item["control_application_journal_allocated_bytes"] for item in paired
                    ),
                    "median_candidate_final_application_journal_allocated_bytes": median(
                        item["candidate_final_application_journal_allocated_bytes"]
                        for item in paired
                    ),
                    "median_control_final_application_journal_allocated_bytes": median(
                        item["control_final_application_journal_allocated_bytes"]
                        for item in paired
                    ),
                    "max_candidate_process_restart_ns": max(
                        item["candidate_process_restart_ns"] for item in paired
                    ),
                }
                if native_snapshot_stages:
                    row["native_snapshot_stages"] = native_snapshot_stages
                if application_checkpoint_stages:
                    row["application_checkpoint_stages"] = application_checkpoint_stages
                if application_snapshot_encode:
                    row["application_snapshot_encode"] = application_snapshot_encode
                rows.append(row)

    verdicts = {
        name: {"status": "passed" if not errors else "failed", "errors": errors}
        for name, errors in failures.items()
    }
    return {
        "schema": 1,
        "kind": "reclamation-under-load",
        "status": "passed" if all(not errors for errors in failures.values()) else "failed",
        "objective": OBJECTIVE,
        "verdicts": verdicts,
        "comparisons": rows,
        "scope": (
            "same exact Rafter revision and durable service configuration; only the "
            "application-snapshot/WAL-reclamation interval changes. Snapshot intervals are not "
            "retained-log suffix sizes. Managed Raft bytes include WAL data and metadata plus "
            "snapshot data and metadata. Reported maintenance maxima cover Raft snapshot "
            "publication, WAL compaction, bounded retired-log handoff, snapshot pruning, and "
            "bounded size-triggered application-journal checkpointing after application snapshot encoding; "
            "application snapshot encoding is reported separately, and accepted background log "
            "destruction is outside those maxima."
        ),
    }


def markdown(value: dict) -> str:
    lines = [
        "# Raft storage reclamation under load",
        "",
        f"**Verdict: {value['status']}.**",
        "",
        value["scope"],
        "",
        "| Snapshot interval | Offered/s | Throughput retained | Worst p99 delta | "
        "Worst p99.9 delta | Compactions | Raft maintenance max upper bound | "
        "Managed Raft after load | No-reclamation after load | Managed Raft after restart | "
        "No-reclamation after restart | App journal | No-checkpoint app journal | Snapshot files | Max restart |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in value["comparisons"]:
        rate = "saturation" if row["offered_per_second"] == 0 else f"{row['offered_per_second']:,}"
        lines.append(
            f"| {row['snapshot_interval_entries']:,} | {rate} | "
            f"{row['median_throughput_ratio'] * 100:.1f}% | "
            f"{row['worst_p99_regression_ms']:+.3f} ms | "
            f"{row['worst_p999_regression_ms']:+.3f} ms | "
            f"{row['measured_compactions']:,} | "
            f"{row['max_compaction_upper_bound_ns'] / 1e6:.3f} ms | "
            f"{row['median_candidate_managed_raft_allocated_bytes'] / 2**20:.2f} MiB | "
            f"{row['median_control_managed_raft_allocated_bytes'] / 2**20:.2f} MiB | "
            f"{row['median_candidate_final_managed_raft_allocated_bytes'] / 2**20:.2f} MiB | "
            f"{row['median_control_final_managed_raft_allocated_bytes'] / 2**20:.2f} MiB | "
            f"{row['median_candidate_application_journal_allocated_bytes'] / 2**20:.2f} MiB | "
            f"{row['median_control_application_journal_allocated_bytes'] / 2**20:.2f} MiB | "
            f"{row['max_candidate_final_snapshot_data_files']} data / "
            f"{row['max_candidate_final_snapshot_metadata_files']} metadata / "
            f"{row['max_candidate_final_snapshot_temporary_files']} temporary | "
            f"{row['max_candidate_process_restart_ns'] / 1e9:.3f} s |"
        )
    staged = [row for row in value["comparisons"] if row.get("native_snapshot_stages")]
    if staged:
        lines += [
            "",
            "## Native snapshot and WAL stage maxima",
            "",
            "Log2 upper bounds across all three nodes in the separate diagnostic case; "
            "not timing-run percentiles.",
            "",
            "| Snapshot interval | Offered/s | Publication | Source + write | Data sync | "
            "File publish | Manifest publish | Prune | Kernel prepare | Kernel commit | "
            "WAL reclaim |",
            "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | "
            "---: | ---: |",
        ]
        for row in staged:
            rate = (
                "saturation"
                if row["offered_per_second"] == 0
                else f"{row['offered_per_second']:,}"
            )

            def maximum(stage: str) -> str:
                value_ns = row["native_snapshot_stages"].get(stage, {}).get(
                    "max_upper_bound_ns"
                )
                return "not observed" if value_ns is None else f"{value_ns / 1e6:.3f} ms"

            lines.append(
                f"| {row['snapshot_interval_entries']:,} | {rate} | "
                f"{maximum('snapshot_publication')} | "
                f"{maximum('snapshot_data_write')} | "
                f"{maximum('snapshot_data_sync')} | "
                f"{maximum('snapshot_file_publish')} | "
                f"{maximum('snapshot_manifest_publish')} | "
                f"{maximum('snapshot_prune')} | "
                f"{maximum('snapshot_kernel_prepare')} | "
                f"{maximum('snapshot_kernel_commit')} | "
                f"{maximum('wal_reclamation')} |"
            )
    application_staged = [
        row
        for row in value["comparisons"]
        if row.get("application_checkpoint_stages") or row.get("application_snapshot_encode")
    ]
    if application_staged:
        lines += [
            "",
            "## Application snapshot and checkpoint stage maxima",
            "",
            "Log2 upper bounds across all three nodes in the separate diagnostic case; "
            "not timing-run percentiles.",
            "",
            "| Snapshot interval | Offered/s | Encodes | WAL reclaims | Checkpoints | "
            "Encode max | Write max | Sync max | Publish max |",
            "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
        for row in application_staged:
            rate = (
                "saturation"
                if row["offered_per_second"] == 0
                else f"{row['offered_per_second']:,}"
            )

            def application_maximum(stage: str) -> str:
                value_ns = (
                    row.get("application_checkpoint_stages", {})
                    .get(stage, {})
                    .get("max_upper_bound_ns")
                )
                return "not observed" if value_ns is None else f"{value_ns / 1e6:.3f} ms"

            encode_ns = row.get("application_snapshot_encode", {}).get(
                "max_upper_bound_ns"
            )
            encode = "not observed" if encode_ns is None else f"{encode_ns / 1e6:.3f} ms"
            encodes = row.get("application_snapshot_encode", {}).get("samples", 0)
            checkpoints = (
                row.get("application_checkpoint_stages", {})
                .get("journal_checkpoint_sync_ns", {})
                .get("samples", 0)
            )
            wal_reclaims = (
                row.get("native_snapshot_stages", {})
                .get("wal_reclamation", {})
                .get("samples", 0)
            )

            lines.append(
                f"| {row['snapshot_interval_entries']:,} | {rate} | "
                f"{encodes:,} | {wal_reclaims:,} | {checkpoints:,} | {encode} | "
                f"{application_maximum('journal_checkpoint_write_ns')} | "
                f"{application_maximum('journal_checkpoint_sync_ns')} | "
                f"{application_maximum('journal_checkpoint_publish_ns')} |"
            )
    lines += [
        "",
        "The selected candidate atomically checkpoints an oversized application journal after "
        "a corresponding Raft snapshot is durable; smaller durable histories remain append-only "
        "until the 64 MiB per-node bound is crossed. The physical-reclamation verdict assesses the "
        "observed bound. Maintenance maxima are log2 upper bounds between the pre-load and "
        "post-load status watermarks. The adapter timer covers Raft snapshot publication, WAL "
        "compaction, bounded retired-log handoff, snapshot pruning, and the application-journal "
        "checkpoint; application snapshot encoding is reported separately, and accepted "
        "background log destruction finishes independently.",
        "",
    ]
    for name, verdict in value["verdicts"].items():
        lines.append(f"- {name.replace('_', ' ')}: {verdict['status']}")
        lines.extend(f"  - {error}" for error in verdict["errors"])
    return "\n".join(lines) + "\n"

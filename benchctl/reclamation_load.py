"""Predeclared assessment for same-code WAL reclamation under service load."""
from __future__ import annotations

from collections import defaultdict
from statistics import median
from typing import Any

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
        "post-load WAL plus snapshot allocated bytes below the same-seed no-snapshot control"
    ),
    "snapshot_artifact_bound": (
        "after final restart, exactly one selected snapshot envelope and manifest per node, "
        "with no temporary snapshot artifacts"
    ),
    "application_journal": "reported separately; no physical-reclamation claim",
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


def _allocated(case: dict, category: str) -> int:
    return _category_value(case, "after_measurement", category, "allocated_bytes")


def _managed_raft_allocated(case: dict) -> int:
    return sum(_allocated(case, category) for category in MANAGED_RAFT_CATEGORIES)


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
                    loss = sum(accounting[name] for name in ("errors", "unknown", "not_issued"))
                    if loss:
                        pair_failures.append(f"{label} had {loss} errors, unknown outcomes, or unsent requests")
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
                    control_wal = _allocated(control, "raft_wal_data")
                    candidate_wal = _allocated(candidate, "raft_wal_data")
                    control_snapshot = _allocated(control, "raft_snapshot_data")
                    candidate_snapshot = _allocated(candidate, "raft_snapshot_data")
                    control_managed = _managed_raft_allocated(control)
                    candidate_managed = _managed_raft_allocated(candidate)
                    application_bytes = _allocated(candidate, "application_journal")
                    snapshot_artifacts_by_node = _final_snapshot_artifacts(candidate)
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
                except ValueError as error:
                    control_wal = candidate_wal = 0
                    control_snapshot = candidate_snapshot = 0
                    control_managed = candidate_managed = 0
                    application_bytes = 0
                    snapshot_data_files = snapshot_metadata_files = 0
                    snapshot_temporary_files = 0
                    snapshot_artifacts_by_node = {}
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

                throughput_ratio = (
                    candidate["metrics"]["throughput_ops_s"]
                    / control["metrics"]["throughput_ops_s"]
                    if not pair_failures and control["metrics"]["throughput_ops_s"] > 0
                    else 0.0
                )
                p99_regression = (
                    candidate["metrics"]["client_p99_ms"]
                    - control["metrics"]["client_p99_ms"]
                    if not pair_failures else 0.0
                )
                p999_regression = (
                    candidate["metrics"]["client_p999_ms"]
                    - control["metrics"]["client_p999_ms"]
                    if not pair_failures else 0.0
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
                candidate_metrics = candidate.get("metrics") or {}
                control_metrics = control.get("metrics") or {}
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
                    "candidate_final_snapshot_data_files": snapshot_data_files,
                    "candidate_final_snapshot_metadata_files": snapshot_metadata_files,
                    "candidate_final_snapshot_temporary_files": snapshot_temporary_files,
                    "candidate_final_snapshot_artifacts_by_node": (
                        snapshot_artifacts_by_node
                    ),
                    "candidate_application_journal_allocated_bytes": application_bytes,
                    "candidate_process_restart_ns": restart_ns,
                })
            if paired:
                rows.append({
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
                    "max_candidate_process_restart_ns": max(
                        item["candidate_process_restart_ns"] for item in paired
                    ),
                })

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
            "snapshot data and metadata. Application-journal physical reclamation is not tested."
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
        "Worst p99.9 delta | Compactions | Compaction max upper bound | "
        "Managed Raft after load | No-reclamation Raft | Snapshot files | Max restart |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
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
            f"{row['max_candidate_final_snapshot_data_files']} data / "
            f"{row['max_candidate_final_snapshot_metadata_files']} metadata / "
            f"{row['max_candidate_final_snapshot_temporary_files']} temporary | "
            f"{row['max_candidate_process_restart_ns'] / 1e9:.3f} s |"
        )
    lines += [
        "",
        "The application journal is reported separately and remains append-only. Compaction "
        "maxima are log2 upper bounds for pauses observed between the pre-load and post-load "
        "status watermarks.",
        "",
    ]
    for name, verdict in value["verdicts"].items():
        lines.append(f"- {name.replace('_', ' ')}: {verdict['status']}")
        lines.extend(f"  - {error}" for error in verdict["errors"])
    return "\n".join(lines) + "\n"

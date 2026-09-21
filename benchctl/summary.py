"""One concise, layered performance report from normalized evidence."""
from __future__ import annotations

import html
import json
import math
import os
from pathlib import Path
import re
from typing import Iterable

from .comparisons import NotComparable, compare
from .evidence import source_digest, write_json
from .results import (aggregate_durable, load_durable_suite, load_microbench,
                      load_storage_comparison, load_wal_reclamation)


MIN_MEMORY_REPETITIONS = 7
SERVICE_OBJECTIVE = {
    "minimum_achieved_percent": 99.0,
    "maximum_p99_ms": 20.0,
    "maximum_p999_ms": 50.0,
    "require_zero_errors_unknown_and_unsent": True,
    "evaluation": "Every repetition must meet the rate and tail limits; accounting is summed without survivor filtering.",
}
SNAPSHOT_STAGE_COLUMNS = (
    ("snapshot_publication", "Publication"),
    ("snapshot_data_write", "Source + write"),
    ("snapshot_data_sync", "Data sync"),
    ("snapshot_file_publish", "File publish"),
    ("snapshot_manifest_publish", "Manifest publish"),
    ("snapshot_prune", "Prune"),
    ("snapshot_kernel_prepare", "Kernel prepare"),
    ("snapshot_kernel_commit", "Kernel commit"),
    ("wal_reclamation", "WAL reclaim"),
)
APPLICATION_CHECKPOINT_STAGE_COLUMNS = (
    ("journal_checkpoint_write_ns", "Write max"),
    ("journal_checkpoint_sync_ns", "Sync max"),
    ("journal_checkpoint_publish_ns", "Publish max"),
)


def _brief_version(value: str) -> str:
    return value[:12] if re.fullmatch(r"[0-9a-f]{40}", value) else value


def _source_refs(evidence: str, cases: Iterable[str]) -> list[dict]:
    return [{"evidence": evidence, "case": case} for case in cases]


def _snapshot_stage_maximum(row: dict, stage: str) -> str:
    value = row.get("native_snapshot_stages", {}).get(stage, {}).get(
        "max_upper_bound_ns"
    )
    return "not observed" if value is None else f"{value / 1e6:.3f} ms"


def _application_stage_maximum(row: dict, stage: str) -> str:
    value = row.get("application_checkpoint_stages", {}).get(stage, {}).get(
        "max_upper_bound_ns"
    )
    return "not observed" if value is None else f"{value / 1e6:.3f} ms"


def _application_encode_maximum(row: dict) -> str:
    value = row.get("application_snapshot_encode", {}).get("max_upper_bound_ns")
    return "not observed" if value is None else f"{value / 1e6:.3f} ms"


def _application_maintenance_counts(row: dict) -> str:
    encodes = row.get("application_snapshot_encode", {}).get("samples", 0)
    wal_reclaims = (
        row.get("native_snapshot_stages", {})
        .get("wal_reclamation", {})
        .get("samples", 0)
    )
    checkpoints = (
        row.get("application_checkpoint_stages", {})
        .get("journal_checkpoint_sync_ns", {})
        .get("samples", 0)
    )
    return f"{encodes:,} / {wal_reclaims:,} / {checkpoints:,}"


def _consensus_section(micro: dict | None) -> dict:
    section = {
        "id": "consensus-in-memory",
        "title": "Consensus in memory",
        "question": "How quickly can the implementation replicate and commit without disks or sockets?",
        "status": "pending qualification",
        "result": "Comparison pending qualification.",
        "conditions": "Three voters in one process, in-memory stores, no disk synchronization or TCP.",
        "details": [],
        "rows": [],
        "source_cases": [],
    }
    if micro is None:
        section["details"].append("The suite exists, but no in-memory evidence set was selected for this report.")
        return section
    if not micro["records"]:
        section["details"].append("The selected in-memory evidence is incomplete; no measurements were available.")
        section["qualification_errors"] = micro.get("qualification_errors", [])
        return section
    boundaries = sorted({record["completion_boundary"] for record in micro["records"]})
    repetitions = sorted({record["repetitions"] for record in micro["records"]})
    section["completion_boundaries"] = boundaries
    section["source_cases"] = _source_refs("consensus_in_memory", sorted({
        case for record in micro["records"] for case in record["source_cases"]
    }))
    if micro["qualification"] != "passed":
        section["details"].append("The selected evidence failed artifact verification.")
        return section
    if len(boundaries) != 1:
        section["details"].append(
            "Adapters record different leader-side completion boundaries, so no shared leaderboard is published."
        )
        return section
    if not repetitions or min(repetitions) < MIN_MEMORY_REPETITIONS:
        section["details"].append(
            f"Selected evidence has {', '.join(map(str, repetitions))} repetitions; "
            f"qualification requires at least {MIN_MEMORY_REPETITIONS}."
        )
        return section

    grouped = {}
    for record in micro["records"]:
        grouped.setdefault(record["workload"]["workload"], {})[record["engine"]["name"]] = record
    expected = {"rafter", "raft-rs", "openraft"}
    if not grouped or any(set(engines) != expected for engines in grouped.values()):
        section["details"].append("Every workload needs Rafter, raft-rs, and OpenRaft measurements.")
        return section

    interpretations = []
    rows = []
    try:
        for workload, engines in sorted(grouped.items()):
            rafter = engines["rafter"]
            raft_rs = engines["raft-rs"]
            openraft = engines["openraft"]
            throughput_raft_rs = compare(
                rafter, raft_rs, "throughput_ops_s", higher_is_better=True
            )
            throughput_openraft = compare(
                rafter, openraft, "throughput_ops_s", higher_is_better=True
            )
            p99_raft_rs = compare(rafter, raft_rs, "client_p99_us", higher_is_better=False)
            p99_openraft = compare(rafter, openraft, "client_p99_us", higher_is_better=False)
            comparisons = (throughput_raft_rs, throughput_openraft, p99_raft_rs, p99_openraft)
            interpretations.extend(item["label"] for item in comparisons)
            rows.append({
                "workload": workload,
                "payload_bytes": rafter["workload"]["payload_bytes"],
                "max_in_flight": rafter["workload"]["max_in_flight"],
                "repetitions": rafter["repetitions"],
                "rafter_ops_s": rafter["metrics"]["throughput_ops_s"]["median"],
                "raft_rs_ops_s": raft_rs["metrics"]["throughput_ops_s"]["median"],
                "openraft_ops_s": openraft["metrics"]["throughput_ops_s"]["median"],
                "rafter_p99_us": rafter["metrics"]["client_p99_us"]["median"],
                "raft_rs_p99_us": raft_rs["metrics"]["client_p99_us"]["median"],
                "openraft_p99_us": openraft["metrics"]["client_p99_us"]["median"],
                "throughput_vs_raft_rs": throughput_raft_rs,
                "throughput_vs_openraft": throughput_openraft,
                "p99_vs_raft_rs": p99_raft_rs,
                "p99_vs_openraft": p99_openraft,
            })
    except NotComparable as error:
        section["status"] = "not comparable"
        section["result"] = "The selected in-memory measurements are not comparable."
        section["details"].append(str(error))
        return section

    leads = interpretations.count("measured lead")
    regressions = interpretations.count("measured regression")
    section["rows"] = rows
    if regressions == 0:
        section["status"] = "measured lead" if leads else "roughly level"
    elif leads or regressions < len(interpretations):
        section["status"] = "mixed result"
    else:
        section["status"] = "measured regression"
    section["result"] = (
        f"Rafter leads {leads} of {len(interpretations)} qualified throughput and p99 comparisons"
        + (" with no measured regressions." if regressions == 0 else f"; {regressions} are regressions.")
    )
    section["conditions"] += (
        " Workloads use submission bursts, not a continuously replenished concurrency window."
    )
    section["details"].append(
        f"All adapters execute the same leader-side reference application operation; {min(repetitions)} repetitions."
    )
    identities = {
        record["engine"]["name"]: _brief_version(record["engine"]["version"])
        for record in micro["records"]
    }
    section["details"].append(
        "Selected identities: "
        + "; ".join(f"{name} {identities[name]}" for name in ("rafter", "raft-rs", "openraft"))
        + "."
    )
    return section


def _storage_section(storage: dict | None, reclamation: dict | None) -> dict:
    section = {
        "id": "durable-replication",
        "title": "Durable replication",
        "question": "How efficiently does Rafter make consensus state survive a restart?",
        "status": "not measured",
        "result": "No component evidence selected.",
        "conditions": None,
        "rows": [],
        "sync_probes": {},
        "wal_reclamation": None,
        "source_cases": [],
        "not_measured": [
            "equivalent OpenRaft persistence comparison",
            "WAL reclamation under complete service traffic",
            "Combined WAL physical-reclamation component qualification",
        ],
        "comparison_kind": "internal optimization",
    }
    if storage is None and reclamation is None:
        return section
    if storage is not None:
        if storage["qualification"] != "passed":
            section.update(
                status="mixed result",
                result="The selected storage evidence failed normalization checks.",
            )
            section["qualification_errors"] = storage["qualification_errors"]
        else:
            by_workload = {}
            for record in storage["records"]:
                by_workload.setdefault(record["workload"]["batch_size"], {})[
                    record["configuration"]["arm"]
                ] = record
            rows = []
            throughput_changes = []
            p99_advantages = []
            source_cases = []
            for batch_size, pair in sorted(by_workload.items()):
                baseline, candidate = pair["baseline"], pair["candidate"]
                throughput = compare(candidate, baseline, "throughput_ops_s", higher_is_better=True)
                p99 = compare(candidate, baseline, "batch_completion_p99_ms", higher_is_better=False)
                throughput_changes.append(throughput["advantage_percent"])
                p99_advantages.append(p99["advantage_percent"])
                source_cases.extend(candidate["source_cases"] + baseline["source_cases"])
                rows.append({
                    "batch_size": batch_size,
                    "file_replacement_ops_s": baseline["metrics"]["throughput_ops_s"]["median"],
                    "append_only_journal_ops_s": candidate["metrics"]["throughput_ops_s"]["median"],
                    "throughput_change_percent": throughput["advantage_percent"],
                    "file_replacement_batch_p99_ms": baseline["metrics"]["batch_completion_p99_ms"]["median"],
                    "append_only_journal_batch_p99_ms": candidate["metrics"]["batch_completion_p99_ms"]["median"],
                    "batch_p99_improvement_percent": p99["advantage_percent"],
                    "interpretation": throughput["label"],
                })
            section.update(
                status="measured lead",
                result=(f"The append-only journal improved throughput {min(throughput_changes):.1f}–"
                        f"{max(throughput_changes):.1f}% over Rafter's file-replacement backend."),
                conditions=("One identical Rafter binary, 256-byte proposals, three repetitions; "
                            "batch size is explicit in every row. Selected Rafter revision: "
                            f"{_brief_version(storage['records'][0]['engine']['version'])}."),
                rows=rows,
                sync_probes=storage["sync_probes"],
                batch_p99_improvement_percent={"min": min(p99_advantages),
                                               "max": max(p99_advantages)},
                source_cases=_source_refs("durable_replication", source_cases),
            )
    if reclamation is not None:
        if reclamation["qualification"] == "passed":
            records = sorted(reclamation["records"],
                             key=lambda record: record["configuration"]["retained_entries"])
            section["wal_reclamation"] = {
                "status": "passed",
                "source_revision": _brief_version(records[0]["engine"]["version"]),
                "rows": [{
                    "retained_entries": record["configuration"]["retained_entries"],
                    "managed_wal_bytes": record["metrics"]["managed_wal_bytes"],
                    "managed_wal_files": record["metrics"]["managed_wal_files"],
                    "reclamation_pause_p99_ms": record["metrics"]["reclamation_pause_p99_ms"],
                    "reopen_ms": record["metrics"]["reopen_ms"],
                    "recovery_verified": record["verification"]["recovery_verified"],
                    "physical_bound_verified": record["verification"]["physical_bound_verified"],
                } for record in records],
                "conditions": (
                    "Three repeated component cycles at each retained suffix. Physical bounds and "
                    "exact reopen are qualified; hosted-runner pause and reopen times are diagnostic only."
                ),
            }
            section["not_measured"].remove(
                "Combined WAL physical-reclamation component qualification"
            )
            section["source_cases"] += _source_refs(
                "wal_reclamation",
                [case for record in records for case in record["source_cases"]],
            )
            if storage is None:
                section.update(
                    status="mixed result",
                    result=("Combined WAL physical reclamation passed its 512/10k/100k retained-suffix "
                            "component qualification; comparative persistence performance was not selected."),
                    conditions=("Selected Rafter revision: "
                                f"{_brief_version(records[0]['engine']['version'])}."),
                )
        else:
            section["status"] = "mixed result"
            section["wal_reclamation"] = {
                "status": "failed",
                "qualification_errors": reclamation["qualification_errors"],
                "rows": [],
            }
            if storage is None:
                section["result"] = "The selected WAL reclamation evidence failed normalization checks."
    return section


def _one(rows: list[dict], *, variant: str, rate: float, delay: int) -> dict:
    matches = [row for row in rows if row["configuration"]["variant"] == variant
               and row["workload"]["offered_per_second"] == rate
               and row["workload"]["network_delay_ms"] == delay]
    if len(matches) != 1:
        raise NotComparable(f"expected one {variant} result at rate {rate}, delay {delay}; found {len(matches)}")
    return matches[0]


def _candidate_variants(durable: dict, rows: list[dict]) -> tuple[list[str], list[str]]:
    suite = durable["suite"]
    if "arms" not in suite:
        rafter = sorted({row["configuration"]["variant"] for row in rows if row["engine"]["name"] == "rafter"})
        return rafter, []
    candidates = [arm[0] for arm in suite["arms"] if arm[1] == "rafter" and arm[3] == "candidate"]
    priors = [arm[0] for arm in suite["arms"] if arm[1] == "rafter" and arm[3] == "prior"]
    return candidates, priors


def _capacity_point(row: dict) -> dict:
    offered = row["workload"]["offered_per_second"]
    minimum_achieved = row["metrics"]["throughput_ops_s"]["min"]
    accounting = row["accounting"]
    loss = sum(accounting[key] for key in ("errors", "unknown", "not_issued"))
    achieved_percent = minimum_achieved / offered * 100.0
    p99 = row["metrics"]["client_p99_ms"]["max"]
    p999 = row["metrics"]["client_p999_ms"]["max"]
    return {
        "offered_per_second": offered,
        "minimum_achieved_ops_s": minimum_achieved,
        "minimum_achieved_percent": achieved_percent,
        "worst_repetition_p99_ms": p99,
        "worst_repetition_p999_ms": p999,
        "worst_repetition_execution_p99_ms": row["metrics"]["execution_p99_ms"]["max"],
        "worst_repetition_execution_p999_ms": row["metrics"]["execution_p999_ms"]["max"],
        "worst_repetition_client_start_p99_ms": row["metrics"]["client_start_p99_ms"]["max"],
        "worst_repetition_scheduler_dispatch_p99_ms": row["metrics"][
            "scheduler_dispatch_p99_ms"
        ]["max"],
        "worst_repetition_scheduler_dispatch_p999_ms": row["metrics"][
            "scheduler_dispatch_p999_ms"
        ]["max"],
        "errors": accounting["errors"],
        "unknown": accounting["unknown"],
        "unsent": accounting["not_issued"],
        "completed_after_window": accounting["ok"] - accounting["completed_in_window"],
        "qualifies": (achieved_percent >= SERVICE_OBJECTIVE["minimum_achieved_percent"]
                      and p99 <= SERVICE_OBJECTIVE["maximum_p99_ms"]
                      and p999 <= SERVICE_OBJECTIVE["maximum_p999_ms"]
                      and loss == 0),
    }


def _highest_contiguous_qualifying(points: list[tuple[float, dict]]) -> float | None:
    highest = None
    for rate, point in points:
        if not point["qualifies"]:
            break
        highest = rate
    return highest


def _variant_capacity_boundaries(rows: list[dict]) -> tuple[list[dict], list[str]]:
    grouped = {}
    for row in rows:
        rate = row["workload"]["offered_per_second"]
        if rate <= 0:
            continue
        key = (row["workload"]["network_delay_ms"], row["configuration"]["variant"])
        grouped.setdefault(key, []).append(row)

    boundaries = []
    sources = []
    for (delay, variant), variant_rows in sorted(
        grouped.items(),
        key=lambda item: (
            item[0][0],
            0 if item[1][0]["engine"]["name"] == "rafter" else 1,
            item[1][0]["display_name"],
            item[0][1],
        ),
    ):
        rates = sorted(variant_rows, key=lambda row: row["workload"]["offered_per_second"])
        points = [(row["workload"]["offered_per_second"], _capacity_point(row)) for row in rates]
        highest = _highest_contiguous_qualifying(points)
        tested_max = points[-1][0]
        example = rates[0]
        sources.extend(case for row in rates for case in row["source_cases"])
        boundaries.append({
            "network_delay_ms": delay,
            "variant": variant,
            "display_name": example["display_name"],
            "engine": example["engine"]["name"],
            "highest_qualifying_rate": highest,
            "maximum_tested_rate": tested_max,
            "reached_tested_ceiling": highest == tested_max,
        })
    return boundaries, sources


def _capacity_summary(rows: list[dict], candidate: str, control: str) -> dict:
    candidate_delays = {row["workload"]["network_delay_ms"] for row in rows
                        if row["configuration"]["variant"] == candidate}
    control_delays = {row["workload"]["network_delay_ms"] for row in rows
                      if row["configuration"]["variant"] == control}
    variant_boundaries, variant_sources = _variant_capacity_boundaries(rows)
    curves = []
    boundaries = []
    sources = []
    for delay in sorted(candidate_delays & control_delays):
        candidate_rates = {row["workload"]["offered_per_second"] for row in rows
                           if row["configuration"]["variant"] == candidate
                           and row["workload"]["network_delay_ms"] == delay
                           and row["workload"]["offered_per_second"] > 0}
        control_rates = {row["workload"]["offered_per_second"] for row in rows
                         if row["configuration"]["variant"] == control
                         and row["workload"]["network_delay_ms"] == delay
                         and row["workload"]["offered_per_second"] > 0}
        common_rates = sorted(candidate_rates & control_rates)
        if not common_rates:
            continue
        delay_rows = []
        for rate in common_rates:
            candidate_row = _one(rows, variant=candidate, rate=rate, delay=delay)
            control_row = _one(rows, variant=control, rate=rate, delay=delay)
            sources.extend(candidate_row["source_cases"] + control_row["source_cases"])
            delay_rows.append({
                "network_delay_ms": delay,
                "offered_per_second": rate,
                "rafter": _capacity_point(candidate_row),
                "openraft": _capacity_point(control_row),
            })
        candidate_max = _highest_contiguous_qualifying(
            [(row["offered_per_second"], row["rafter"]) for row in delay_rows]
        )
        control_max = _highest_contiguous_qualifying(
            [(row["offered_per_second"], row["openraft"]) for row in delay_rows]
        )
        tested_max = common_rates[-1]
        boundaries.append({
            "network_delay_ms": delay,
            "maximum_tested_rate": tested_max,
            "rafter_highest_qualifying_rate": candidate_max,
            "rafter_reached_tested_ceiling": candidate_max == tested_max,
            "openraft_highest_qualifying_rate": control_max,
            "openraft_reached_tested_ceiling": control_max == tested_max,
        })
        curves.extend(delay_rows)
    if not boundaries:
        return {"status": "not measured", "result": "No common fixed-rate capacity curve was selected.",
                "objective": SERVICE_OBJECTIVE, "boundaries": [], "variant_boundaries": variant_boundaries,
                "rows": [], "source_cases": _source_refs("durable_service", dict.fromkeys(variant_sources))}
    comparisons = []
    for boundary in boundaries:
        candidate_value = boundary["rafter_highest_qualifying_rate"] or 0
        control_value = boundary["openraft_highest_qualifying_rate"] or 0
        comparisons.append((candidate_value > control_value) - (candidate_value < control_value))
    status = ("mixed result" if any(value < 0 for value in comparisons)
              else "measured lead" if any(value > 0 for value in comparisons)
              else "roughly level")
    no_delay = next((boundary for boundary in boundaries if boundary["network_delay_ms"] == 0), boundaries[0])

    def describe(prefix: str) -> str:
        value = no_delay[f"{prefix}_highest_qualifying_rate"]
        if value is None:
            return "no tested rate"
        qualifier = "at least " if no_delay[f"{prefix}_reached_tested_ceiling"] else ""
        return f"{qualifier}{value:,.0f} writes/s"

    return {
        "status": status,
        "result": (f"At {no_delay['network_delay_ms']} ms added egress, Rafter met the declared service objective through "
                   f"{describe('rafter')}; the selected OpenRaft control met it through {describe('openraft')}."),
        "objective": SERVICE_OBJECTIVE,
        "boundaries": boundaries,
        "variant_boundaries": variant_boundaries,
        "rows": curves,
        "source_cases": _source_refs("durable_service", dict.fromkeys(sources + variant_sources)),
    }


def _measured_load_context(durable: dict) -> dict:
    timing_cases = [
        case
        for case in durable.get("cases", [])
        if not case.get("smoke") and case.get("measurement_mode") == "timing"
    ]
    observed = [case for case in timing_cases if case.get("load_environment") is not None]
    if not observed:
        return {
            "status": "not measured",
            "observed_cases": 0,
            "expected_cases": len(timing_cases),
            "metrics": {},
            "source_cases": [],
            "scope": "no replayed measured-load environment receipts",
        }

    def values(path: tuple[str, ...]) -> list[float]:
        result = []
        for case in observed:
            value = case["load_environment"].get("summary", {})
            for name in path:
                value = value.get(name) if isinstance(value, dict) else None
            if type(value) in (int, float) and math.isfinite(value):
                result.append(value)
        return result

    def maximum(path: tuple[str, ...]) -> float | None:
        available = values(path)
        return max(available) if available else None

    available_bytes = values(("filesystem", "minimum_available_bytes"))
    hardware = values(("hardware_throttling", "maximum_counter_delta"))
    return {
        "status": "observed" if len(observed) == len(timing_cases) else "partial",
        "observed_cases": len(observed),
        "expected_cases": len(timing_cases),
        "metrics": {
            "maximum_cpu_busy_percent": maximum(("cpu", "busy_percent")),
            "maximum_cpu_iowait_percent": maximum(("cpu", "iowait_percent")),
            "maximum_cpu_steal_percent": maximum(("cpu", "steal_percent")),
            "maximum_cgroup_throttled_percent": maximum(
                ("cgroup_cpu", "throttled_percent_of_wall")
            ),
            "maximum_io_pressure_some_percent": maximum(
                ("pressure", "io", "some", "percent_of_wall")
            ),
            "maximum_io_pressure_full_percent": maximum(
                ("pressure", "io", "full", "percent_of_wall")
            ),
            "maximum_memory_pressure_full_percent": maximum(
                ("pressure", "memory", "full", "percent_of_wall")
            ),
            "maximum_hardware_throttle_counter_delta": max(hardware) if hardware else None,
            "hardware_counter_cases": len(hardware),
            "minimum_available_bytes": min(available_bytes) if available_bytes else None,
            "maximum_sample_gap_seconds": maximum(("maximum_sample_gap_seconds",)),
        },
        "source_cases": _source_refs(
            "durable_service", [case["source_case"] for case in observed]
        ),
        "scope": (
            "Workload and unrelated host activity are inseparable in these counters; "
            "they never remove, accept, or rank a result"
        ),
    }


def _service_section(durable: dict | None, headline_variant: str | None,
                     headline_control_variant: str | None) -> dict:
    not_measured_verdict = {"status": "not measured", "detail": "No durable-service evidence selected."}
    section = {
        "id": "complete-durable-service",
        "title": "Complete durable service",
        "question": "How many durable writes can an application complete, and how long do clients wait?",
        "status": "not measured",
        "result": "No durable-service evidence selected.",
        "conditions": None,
        "rows": [],
        "internal_comparisons": [],
        "capacity": {"status": "not measured", "result": "No common fixed-rate capacity curve was selected.",
                     "objective": SERVICE_OBJECTIVE, "boundaries": [], "variant_boundaries": [],
                     "rows": [], "source_cases": []},
        "measured_load_context": {
            "status": "not measured", "observed_cases": 0, "expected_cases": 0,
            "metrics": {}, "source_cases": [],
            "scope": "no durable-service evidence selected",
        },
        "source_cases": [],
        "benchmark_repository": None,
        "comparison_kind": "competitor comparison",
        "verdicts": {
            "evidence_integrity": dict(not_measured_verdict),
            "correctness_checks": dict(not_measured_verdict),
            "feature_coverage": dict(not_measured_verdict),
            "environment_qualification": dict(not_measured_verdict),
            "service_objective": dict(not_measured_verdict),
        },
    }
    if durable is None:
        return section
    section["benchmark_repository"] = durable.get("suite", {}).get(
        "benchmark_repository"
    )
    section["measured_load_context"] = _measured_load_context(durable)
    recorded_verdicts = durable.get("verdicts", {})
    for name in ("evidence_integrity", "correctness_checks", "feature_coverage",
                 "environment_qualification"):
        if name in recorded_verdicts:
            section["verdicts"][name] = recorded_verdicts[name]
    if durable.get("suite", {}).get("kind") == "reclamation-under-load":
        assessment = durable.get("reclamation_under_load")
        section["comparison_kind"] = "internal optimization"
        section["source_cases"] = _source_refs(
            "durable_service", [case["source_case"] for case in durable.get("cases", [])]
        )
        if not isinstance(assessment, dict) or assessment.get("kind") != "reclamation-under-load":
            section.update(
                status="mixed result",
                result=("The selected reclamation-under-load evidence is missing its replayed "
                        "assessment; no service or competitor headline was computed."),
            )
            section["verdicts"]["service_objective"] = {
                "status": "failed",
                "detail": "The reclamation-under-load assessment is missing or invalid.",
            }
            return section
        objective = assessment.get("verdicts", {}).get("service_objective", {})
        objective_status = objective.get("status", "failed")
        objective_errors = objective.get("errors", [])
        section["verdicts"]["service_objective"] = {
            "status": objective_status,
            "detail": ("The reclamation-under-load service objective passed."
                       if objective_status == "passed" else
                       "; ".join(objective_errors) or
                       "The reclamation-under-load service objective failed."),
        }
        evidence_passed = durable["qualification"] == "passed"
        assessment_passed = assessment.get("status") == "passed"
        section.update(
            status="internal qualification" if evidence_passed and assessment_passed else "mixed result",
            result=(
                "WAL reclamation met its same-code service, activity, physical-bound, and restart objectives; "
                "no competitor headline was computed."
                if evidence_passed and assessment_passed else
                "WAL reclamation did not meet every same-code under-load objective; no competitor headline was computed."
            ),
            conditions=assessment.get("scope"),
        )
        return section
    rows = aggregate_durable(durable)
    if durable["qualification"] != "passed" or any(row["qualification"] != "passed" for row in rows):
        section.update(status="mixed result", result="The selected service evidence is incomplete; no headline was computed.")
        section["qualification_errors"] = durable["qualification_errors"] + [
            error for row in rows for error in row["qualification_errors"]
        ]
        return section
    candidates, priors = _candidate_variants(durable, rows)
    selected = headline_variant or (candidates[0] if len(candidates) == 1 else None)
    if selected not in candidates:
        section.update(
            status="not comparable",
            result=("Select one Rafter configuration for the headline. Available variants: "
                    + ", ".join(candidates)),
        )
        return section
    openraft_variants = sorted({row["configuration"]["variant"] for row in rows
                                if row["engine"]["name"] == "openraft"})
    control = headline_control_variant or (openraft_variants[0] if len(openraft_variants) == 1 else None)
    if control not in openraft_variants:
        section.update(
            status="not comparable",
            result=("Select one OpenRaft control for the headline. Available variants: "
                    + ", ".join(openraft_variants)),
        )
        return section
    capacity = _capacity_summary(rows, selected, control)
    section["capacity"] = capacity
    candidate_boundaries = [
        boundary for boundary in capacity["variant_boundaries"]
        if boundary["variant"] == selected
    ]
    if candidate_boundaries:
        objective_passed = all(
            boundary["highest_qualifying_rate"] is not None
            for boundary in candidate_boundaries
        )
        section["verdicts"]["service_objective"] = {
            "status": "passed" if objective_passed else "failed",
            "detail": capacity["result"],
        }
    delays = sorted({row["workload"]["network_delay_ms"] for row in rows
                     if row["configuration"]["variant"] == selected})
    required_rates = {0, 100, 1000}
    available = {(row["configuration"]["variant"], row["workload"]["network_delay_ms"],
                  row["workload"]["offered_per_second"]) for row in rows}
    standard_complete = all((variant, delay, rate) in available
                            for variant in (selected, control) for delay in delays for rate in required_rates)
    candidate_rows = [row for row in rows if row["configuration"]["variant"] == selected]
    control_rows = [row for row in rows if row["configuration"]["variant"] == control]
    example = candidate_rows[0]
    openraft = control_rows[0]
    selected_configuration = {"name": example["display_name"], "variant": selected,
                              "rafter_version": example["engine"]["version"],
                              "openraft_control": openraft["display_name"],
                              "openraft_variant": control,
                              "openraft_version": openraft["engine"]["version"]}
    selected_identities = (
        f"Selected identities: Rafter {_brief_version(example['engine']['version'])}; "
        f"{openraft['display_name']} {_brief_version(openraft['engine']['version'])}."
    )
    machine_condition = (
        " Fixed-machine idle/storage profile passed sealed replay immediately after builds."
        if durable.get("machine_qualification") is not None else ""
    )
    if not standard_complete:
        if capacity["variant_boundaries"]:
            status = ("mixed result" if section["verdicts"]["feature_coverage"]["status"] == "incomplete"
                      else capacity["status"])
            section.update(status=status, result=capacity["result"],
                           conditions=(f"{example['environment']['topology']}; {example['workload']['concurrency']} clients; "
                                       f"{example['workload']['payload_bytes']}-byte writes; {example['repetitions']} repetitions per point. "
                                       "Each capacity decision uses the worst repetition and complete loss accounting. "
                                       + selected_identities + machine_condition),
                           selected_configuration=selected_configuration,
                           source_cases=capacity["source_cases"])
        else:
            section.update(status="not comparable",
                           result="The standard service points are incomplete and no fixed-rate capacity curve is available.")
        return section
    result_rows = []
    sources = []
    try:
        for delay in delays:
            candidate_saturated = _one(rows, variant=selected, rate=0, delay=delay)
            control_saturated = _one(rows, variant=control, rate=0, delay=delay)
            candidate_low = _one(rows, variant=selected, rate=100, delay=delay)
            control_low = _one(rows, variant=control, rate=100, delay=delay)
            candidate_1000 = _one(rows, variant=selected, rate=1000, delay=delay)
            control_1000 = _one(rows, variant=control, rate=1000, delay=delay)
            throughput = compare(candidate_saturated, control_saturated, "throughput_ops_s", higher_is_better=True)
            saturated_p99 = compare(candidate_saturated, control_saturated, "client_p99_ms", higher_is_better=False)
            saturated_p999 = compare(candidate_saturated, control_saturated, "client_p999_ms", higher_is_better=False)
            low = compare(candidate_low, control_low, "client_p99_ms", higher_is_better=False)
            low_p999 = compare(candidate_low, control_low, "client_p999_ms", higher_is_better=False)
            at_1000 = compare(candidate_1000, control_1000, "client_p99_ms", higher_is_better=False)
            at_1000_p999 = compare(candidate_1000, control_1000, "client_p999_ms", higher_is_better=False)
            selected_rows = (candidate_saturated, control_saturated, candidate_low, control_low,
                             candidate_1000, control_1000)
            sources.extend(case for row in selected_rows for case in row["source_cases"])
            result_rows.append({
                "network_delay_ms": delay,
                "rafter_throughput_ops_s": candidate_saturated["metrics"]["throughput_ops_s"]["median"],
                "openraft_throughput_ops_s": control_saturated["metrics"]["throughput_ops_s"]["median"],
                "throughput_ratio": throughput["ratio"],
                "throughput_interpretation": throughput["label"],
                "rafter_saturated_p99_ms": candidate_saturated["metrics"]["client_p99_ms"]["median"],
                "openraft_saturated_p99_ms": control_saturated["metrics"]["client_p99_ms"]["median"],
                "saturated_p99_improvement_percent": saturated_p99["advantage_percent"],
                "saturated_p99_interpretation": saturated_p99["label"],
                "rafter_saturated_p999_ms": candidate_saturated["metrics"]["client_p999_ms"]["median"],
                "openraft_saturated_p999_ms": control_saturated["metrics"]["client_p999_ms"]["median"],
                "saturated_p999_improvement_percent": saturated_p999["advantage_percent"],
                "saturated_p999_interpretation": saturated_p999["label"],
                "rafter_p99_at_100_ms": candidate_low["metrics"]["client_p99_ms"]["median"],
                "openraft_p99_at_100_ms": control_low["metrics"]["client_p99_ms"]["median"],
                "p99_at_100_improvement_percent": low["advantage_percent"],
                "p99_at_100_interpretation": low["label"],
                "rafter_p999_at_100_ms": candidate_low["metrics"]["client_p999_ms"]["median"],
                "openraft_p999_at_100_ms": control_low["metrics"]["client_p999_ms"]["median"],
                "p999_at_100_improvement_percent": low_p999["advantage_percent"],
                "p999_at_100_interpretation": low_p999["label"],
                "rafter_p99_at_1000_ms": candidate_1000["metrics"]["client_p99_ms"]["median"],
                "openraft_p99_at_1000_ms": control_1000["metrics"]["client_p99_ms"]["median"],
                "p99_at_1000_improvement_percent": at_1000["advantage_percent"],
                "p99_at_1000_interpretation": at_1000["label"],
                "rafter_p999_at_1000_ms": candidate_1000["metrics"]["client_p999_ms"]["median"],
                "openraft_p999_at_1000_ms": control_1000["metrics"]["client_p999_ms"]["median"],
                "p999_at_1000_improvement_percent": at_1000_p999["advantage_percent"],
                "p999_at_1000_interpretation": at_1000_p999["label"],
            })
    except NotComparable as error:
        section.update(status="not comparable", result=str(error))
        return section
    candidate_accounting = {key: sum(row["accounting"][key] for row in candidate_rows)
                            for key in ("errors", "unknown", "not_issued")}
    control_accounting = {key: sum(row["accounting"][key] for row in control_rows)
                          for key in ("errors", "unknown", "not_issued")}
    internal = []
    if len(priors) == 1:
        prior = priors[0]
        for delay in delays:
            candidate = _one(rows, variant=selected, rate=0, delay=delay)
            baseline = _one(rows, variant=prior, rate=0, delay=delay)
            same_source = candidate["engine"] == baseline["engine"]
            same_storage = candidate["configuration"]["hard_state"] == baseline["configuration"]["hard_state"]
            if same_source and same_storage:
                change = compare(candidate, baseline, "throughput_ops_s", higher_is_better=True)
                latency_rows = []
                internal_sources = candidate["source_cases"] + baseline["source_cases"]
                for rate in (0, 100, 1000):
                    candidate_rate = _one(rows, variant=selected, rate=rate, delay=delay)
                    baseline_rate = _one(rows, variant=prior, rate=rate, delay=delay)
                    p99 = compare(candidate_rate, baseline_rate, "client_p99_ms", higher_is_better=False)
                    p999 = compare(candidate_rate, baseline_rate, "client_p999_ms", higher_is_better=False)
                    latency_rows.append({
                        "offered_per_second": rate,
                        "candidate_p99_ms": candidate_rate["metrics"]["client_p99_ms"]["median"],
                        "prior_p99_ms": baseline_rate["metrics"]["client_p99_ms"]["median"],
                        "p99_improvement_percent": p99["advantage_percent"],
                        "p99_interpretation": p99["label"],
                        "candidate_p999_ms": candidate_rate["metrics"]["client_p999_ms"]["median"],
                        "prior_p999_ms": baseline_rate["metrics"]["client_p999_ms"]["median"],
                        "p999_improvement_percent": p999["advantage_percent"],
                        "p999_interpretation": p999["label"],
                    })
                    if rate != 0:
                        internal_sources.extend(candidate_rate["source_cases"] + baseline_rate["source_cases"])
                internal.append({"network_delay_ms": delay, "prior_variant": prior,
                                 "candidate_name": candidate["display_name"],
                                 "prior_name": baseline["display_name"],
                                 "throughput_change_percent": change["advantage_percent"],
                                 "interpretation": change["label"],
                                 "latencies": latency_rows,
                                 "source_cases": _source_refs("durable_service", internal_sources)})
    interpretations = [row[key] for row in result_rows for key in
                       ("throughput_interpretation", "saturated_p99_interpretation",
                        "saturated_p999_interpretation", "p99_at_100_interpretation",
                        "p999_at_100_interpretation", "p99_at_1000_interpretation",
                        "p999_at_1000_interpretation")]
    tail_interpretations = [row[key] for row in result_rows for key in
                            ("saturated_p99_interpretation", "saturated_p999_interpretation",
                             "p99_at_100_interpretation", "p999_at_100_interpretation",
                             "p99_at_1000_interpretation", "p999_at_1000_interpretation")]
    if any(candidate_accounting.values()) or "measured regression" in interpretations:
        status = "mixed result"
    elif all(row["throughput_interpretation"] == "roughly level" for row in result_rows):
        status = "roughly level"
    else:
        status = "measured lead"
    no_delay = next((row for row in result_rows if row["network_delay_ms"] == 0), result_rows[0])
    saturated_regressions = []
    for label, key in (("p99", "saturated_p99_improvement_percent"),
                       ("p99.9", "saturated_p999_improvement_percent")):
        if no_delay[f"saturated_{label.replace('.', '')}_interpretation"] == "measured regression":
            saturated_regressions.append(f"{label} was {abs(no_delay[key]):.1f}% higher")
    tail_qualification = (" At no-delay saturation, " + " and ".join(saturated_regressions)
                          + " while completing that greater load.") if saturated_regressions else ""
    candidate_loss = sum(candidate_accounting.values())
    if candidate_loss:
        result = (
            f"{example['display_name']} is not qualified: the selected cases include "
            f"{candidate_accounting['errors']} errors, {candidate_accounting['unknown']} unknown outcomes, "
            f"and {candidate_accounting['not_issued']} unsent requests."
        )
    else:
        result = (
            f"{example['display_name']} completed {no_delay['throughput_ratio']:.2f}× as many "
            f"durable writes per second as the tested {openraft['display_name']} integration in the no-delay condition."
            + tail_qualification
        )
        tail_regressions = tail_interpretations.count("measured regression")
        if tail_regressions:
            result += (
                f" Across the displayed loads, {tail_regressions} of {len(tail_interpretations)} "
                "p99 and p99.9 comparisons were regressions."
            )
    section.update(
        status=status,
        result=result,
        headline_qualified=(candidate_loss == 0 and "measured regression" not in interpretations
                            and section["verdicts"]["feature_coverage"]["status"]
                            in ("passed", "not selected", "not measured")),
        conditions=(f"{example['environment']['topology']}; {example['workload']['concurrency']} clients; "
                    f"{example['workload']['payload_bytes']}-byte writes; medians of {example['repetitions']} repetitions. "
                    "Success requires Raft commitment and durable application completion. Added delay affects client and peer egress. "
                    "Storage, codec, and scheduling choices differ between integrations. "
                    + selected_identities + machine_condition),
        selected_configuration=selected_configuration,
        rows=result_rows,
        accounting={"rafter": candidate_accounting, "openraft": control_accounting},
        internal_comparisons=internal,
        source_cases=_source_refs("durable_service", sources) + capacity["source_cases"],
    )
    return section


def _history_anomalies(histories: list[dict]) -> list[dict]:
    anomalies = []
    for index, history in enumerate(histories):
        run_match = re.search(r"(\d{11})", history["directory"])
        run_id = run_match.group(1) if run_match else Path(history["directory"]).name
        for case in history["cases"]:
            accounting = case.get("accounting")
            if case["measurement_mode"] != "timing" or not accounting:
                continue
            if any(accounting.get(key, 0) for key in ("errors", "unknown", "not_issued")):
                anomalies.append({
                    "evidence": f"history_{index + 1}",
                    "run": run_id,
                    "case": case["source_case"],
                    "engine": case["display_name"],
                    "workload": case["workload"],
                    "errors": accounting["errors"],
                    "unknown": accounting["unknown"],
                    "unsent": accounting["not_issued"],
                })
    return anomalies


def _failure_section(durable: dict | None, anomalies: list[dict],
                     reclamation: dict | None) -> dict:
    section = {
        "id": "failure-and-sustained-operation",
        "title": "Failure and sustained operation",
        "question": "Does performance remain useful during failures and over long runs?",
        "status": "not measured",
        "result": "Comparative fault performance has not been measured.",
        "checks": [],
        "history": anomalies,
        "reclamation_under_load": None,
        "not_measured": ["comparative leader-loss performance", "comparative slow-follower performance",
                         "storage-stall recovery performance",
                         "long-duration snapshot/compaction and WAL-reclamation soak performance"],
        "source_cases": [],
    }
    if durable is None and reclamation is None:
        return section
    checks = []
    source_cases = [{"evidence": item["evidence"], "case": item["case"]} for item in anomalies]
    result_parts = []
    if durable is not None:
        recovered = [case for case in durable["cases"]
                     if (case.get("recovery") or {}).get("status") == "passed"]
        histories = [case for case in durable["cases"]
                     if (case.get("history_check") or {}).get("status") == "passed"]
        smokes = [{"scenario": smoke["scenario"], "status": smoke["status"]}
                  for smoke in durable["smokes"]]
        snapshot_smokes = [
            smoke for smoke in durable["smokes"]
            if smoke["scenario"] == "snapshot-catchup" and smoke["status"] == "passed"
        ]
        checks += [
            {"name": "restart and acknowledged-canary checks", "passed_cases": len(recovered),
             "scope": "finite histories and canaries, not every measured write or power loss"},
            {"name": "linearizable qualification histories", "passed_cases": len(histories),
             "scope": "64-operation per-case qualification histories"},
            {"name": "Rafter recovery smoke", "scenarios": smokes,
             "scope": "smoke correctness only; not comparative performance"},
        ]
        if snapshot_smokes:
            snapshot_cases = [
                case for case in durable["cases"]
                if case.get("smoke")
                and case["workload"].get("scenario") == "snapshot-catchup"
                and (case.get("snapshot_reclamation") or {}).get("status") == "passed"
            ]
            checks.append({
                "name": "complete-service snapshot catch-up smoke",
                "passed_cases": len(snapshot_cases),
                "scope": (
                    "live writes, Raft snapshot and WAL reclamation, lagging-follower snapshot "
                    "installation, and restart canaries; functional smoke only, not performance"
                ),
            })
            result_parts.append("Complete-service snapshot catch-up smoke passed")
        source_cases = (_source_refs("durable_service",
                                    [case["source_case"] for case in durable["cases"]])
                        + source_cases)
        result_parts.append("Finite recovery checks passed")
        if durable.get("suite", {}).get("kind") == "reclamation-under-load":
            assessment = durable.get("reclamation_under_load")
            if isinstance(assessment, dict) and assessment.get("kind") == "reclamation-under-load":
                section["reclamation_under_load"] = assessment
                if assessment.get("status") == "passed":
                    result_parts.append(
                        "WAL reclamation under complete-service traffic met its predeclared objectives"
                    )
                else:
                    result_parts.append(
                        "WAL reclamation under complete-service traffic missed at least one predeclared objective"
                    )
            else:
                result_parts.append(
                    "WAL reclamation under complete-service traffic is missing its replayed assessment"
                )
    if reclamation is not None:
        if reclamation["qualification"] == "passed":
            checks.append({
                "name": "WAL physical-reclamation component",
                "passed_cases": len(reclamation["records"]),
                "scope": (
                    "512, 10k, and 100k retained suffixes across repeated writes, snapshots, compaction, "
                    "physical cleanup, and exact reopen; not complete-service traffic or latency evidence"
                ),
            })
            source_cases += _source_refs(
                "wal_reclamation",
                [case for record in reclamation["records"] for case in record["source_cases"]],
            )
            result_parts.append("WAL physical-reclamation component checks passed")
        else:
            checks.append({
                "name": "WAL physical-reclamation component",
                "passed_cases": 0,
                "scope": "normalization failed: " + "; ".join(reclamation["qualification_errors"]),
            })
            result_parts.append("WAL physical-reclamation evidence failed normalization")
    section.update(
        status="mixed result",
        result=("; ".join(result_parts)
                + "; comparative fault and sustained-operation performance remain unmeasured."),
        checks=checks,
        source_cases=source_cases,
    )
    return section


def build_summary(*, durable: dict | None = None, storage: dict | None = None,
                  reclamation: dict | None = None, micro: dict | None = None,
                  histories: list[dict] | None = None,
                  headline_variant: str | None = None,
                  headline_control_variant: str | None = None) -> dict:
    histories = histories or []
    anomalies = _history_anomalies(histories)
    service = _service_section(durable, headline_variant, headline_control_variant)
    storage_section = _storage_section(storage, reclamation)
    if (
        durable is not None
        and durable.get("suite", {}).get("kind") == "reclamation-under-load"
        and durable.get("reclamation_under_load") is not None
    ):
        storage_section["not_measured"].remove(
            "WAL reclamation under complete service traffic"
        )
    sections = [
        _consensus_section(micro),
        storage_section,
        service,
        _failure_section(durable, anomalies, reclamation),
    ]
    durable_replication_inputs = [item for item in (storage, reclamation) if item is not None]
    durable_replication_records = [
        record for item in durable_replication_inputs for record in item["records"]
    ]
    durable_replication_status = (
        "not measured" if not durable_replication_inputs else
        "failed" if any(item["qualification"] != "passed"
                        for item in durable_replication_inputs) else "passed"
    )
    normalized_results = {
        "consensus_in_memory": micro["records"] if micro else [],
        "durable_replication": durable_replication_records,
        "complete_durable_service": aggregate_durable(durable) if durable else [],
    }
    evidence_status = {
        "consensus_in_memory": micro["qualification"] if micro else "not measured",
        "durable_replication": durable_replication_status,
        "complete_durable_service": durable["qualification"] if durable else "not measured",
    }
    return {
        "schema": 2,
        "title": "Rafter implementation performance",
        "principle": "Where Rafter demonstrably leads, where evidence is mixed, and what remains unmeasured.",
        "interpretation": {"roughly_level_percent": 3.0,
                           "rule": "Differences within ±3% are roughly level; comparisons fail closed across incompatible evidence."},
        "report_generator": {"source_digest": source_digest(), "uses_llm": False},
        "evidence_status": evidence_status,
        "durable_service_verdicts": service["verdicts"],
        "normalized_results": normalized_results,
        "sections": sections,
    }


def _fmt_ops(value: float) -> str:
    return f"{value:,.0f}"


def _fmt_ms(value: float | None) -> str:
    return "not measured" if value is None else f"{value:.3f} ms"


def _fmt_us(value: float) -> str:
    return f"{value:,.1f} µs"


def _fmt_mib(value: float) -> str:
    return f"{value / 2**20:,.2f} MiB"


def _fmt_capacity(value: float | None, reached_ceiling: bool) -> str:
    if value is None:
        return "none"
    prefix = "at least " if reached_ceiling else ""
    return f"{prefix}{_fmt_ops(value)}/s"


def _measured_load_context_text(context: dict) -> str:
    metrics = context["metrics"]

    def percent(name: str) -> str:
        value = metrics.get(name)
        return "not exposed" if value is None else f"{value:.3f}%"

    hardware = metrics.get("maximum_hardware_throttle_counter_delta")
    hardware_text = (
        "not exposed"
        if hardware is None
        else (
            f"{hardware:,.0f} "
            f"({metrics['hardware_counter_cases']}/{context['observed_cases']} cases exposed)"
        )
    )
    return (
        f"Measured-load host context ({context['observed_cases']}/{context['expected_cases']} "
        f"timing cases): maximum CPU busy {percent('maximum_cpu_busy_percent')}, iowait "
        f"{percent('maximum_cpu_iowait_percent')}, steal "
        f"{percent('maximum_cpu_steal_percent')}, cgroup throttling "
        f"{percent('maximum_cgroup_throttled_percent')}, I/O PSI some "
        f"{percent('maximum_io_pressure_some_percent')}, and hardware-throttle counter "
        f"delta {hardware_text}. {context['scope']}."
    )


def _verdict_detail(verdict: dict) -> str:
    if verdict.get("detail"):
        return verdict["detail"]
    checks = verdict.get("checks", [])
    if checks:
        incomplete = [check for check in checks if check.get("status") != "passed"]
        selected = incomplete or checks
        details = []
        for check in selected:
            variant = check.get("variant")
            prefix = f"{variant}: " if isinstance(variant, str) and variant else ""
            details.append(prefix + check["detail"])
        return "; ".join(dict.fromkeys(details))
    errors = verdict.get("errors", [])
    if errors:
        return "; ".join(str(error) for error in errors)
    return verdict.get("scope", "No additional detail.")


def _history_groups(history: list[dict]) -> list[dict]:
    """Keep the reader summary short while the normalized JSON retains every case."""
    groups: dict[tuple[str, str], dict] = {}
    for item in history:
        key = (item["evidence"], item["run"])
        group = groups.setdefault(key, {
            "evidence": item["evidence"],
            "run": item["run"],
            "cases": 0,
            "unsent": 0,
            "unknown": 0,
            "errors": 0,
        })
        group["cases"] += 1
        for field in ("unsent", "unknown", "errors"):
            group[field] += item[field]
    return list(groups.values())


def render_markdown(summary: dict, evidence: dict) -> str:
    sections = {section["id"]: section for section in summary["sections"]}
    lines = [f"# {summary['title']}", "", summary["principle"], ""]
    for section_id in ("consensus-in-memory", "durable-replication", "complete-durable-service", "failure-and-sustained-operation"):
        section = sections[section_id]
        lines += [f"## {section['title']}", "", f"*{section['question']}*", "", f"**{section['result']}**", ""]
        if section_id == "complete-durable-service":
            repository = section.get("benchmark_repository")
            if repository:
                lines += [
                    f"Benchmark source: `{repository['commit']}` ({repository['status']} at preflight).",
                    "",
                ]
            labels = {
                "evidence_integrity": "Evidence integrity",
                "correctness_checks": "Correctness checks",
                "environment_qualification": "Environment qualification",
                "feature_coverage": "Feature coverage",
                "service_objective": "Service objective",
            }
            lines += ["| Verdict | Status | Detail |", "| --- | --- | --- |"]
            for name, label in labels.items():
                verdict = section["verdicts"][name]
                lines.append(f"| {label} | {verdict['status']} | {_verdict_detail(verdict)} |")
            lines.append("")
            context = section["measured_load_context"]
            if context["status"] != "not measured":
                lines += [_measured_load_context_text(context), ""]
        if section_id == "consensus-in-memory" and section["rows"]:
            lines += ["| Workload | In flight | Rafter | raft-rs | OpenRaft | Rafter p99 | raft-rs p99 | OpenRaft p99 |",
                      "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
            for row in section["rows"]:
                lines.append(f"| {row['workload']} | {row['max_in_flight']} | "
                             f"{_fmt_ops(row['rafter_ops_s'])}/s | {_fmt_ops(row['raft_rs_ops_s'])}/s | "
                             f"{_fmt_ops(row['openraft_ops_s'])}/s | {_fmt_us(row['rafter_p99_us'])} | "
                             f"{_fmt_us(row['raft_rs_p99_us'])} | {_fmt_us(row['openraft_p99_us'])} |")
            lines.append("")
        elif section_id == "durable-replication":
            if section["rows"]:
                lines += ["| Batch | File replacement | Append-only journal | Throughput change | Replacement p99 | Journal p99 |",
                          "| ---: | ---: | ---: | ---: | ---: | ---: |"]
                for row in section["rows"]:
                    lines.append(f"| {row['batch_size']} | {_fmt_ops(row['file_replacement_ops_s'])}/s | "
                                 f"{_fmt_ops(row['append_only_journal_ops_s'])}/s | +{row['throughput_change_percent']:.1f}% | "
                                 f"{_fmt_ms(row['file_replacement_batch_p99_ms'])} | {_fmt_ms(row['append_only_journal_batch_p99_ms'])} |")
                lines.append("")
                if section["sync_probes"]:
                    probes = section["sync_probes"]
                    lines += [("Separate hard-state syscall probe: "
                               f"file replacement {probes['replace']['sync_calls_per_publication']:.1f}, "
                               f"journal {probes['journal']['sync_calls_per_publication']:.1f} sync calls/publication. "
                               "This is not synchronization calls per committed entry."), ""]
            reclamation = section["wal_reclamation"]
            if reclamation and reclamation["status"] == "passed":
                lines += ["WAL physical-reclamation component qualification:", "",
                          "| Retained suffix | Managed WAL | Files | Exact recovery | Diagnostic pause p99 | Diagnostic reopen |",
                          "| ---: | ---: | ---: | :---: | ---: | ---: |"]
                for row in reclamation["rows"]:
                    lines.append(
                        f"| {row['retained_entries']:,} entries | {row['managed_wal_bytes']:,} bytes | "
                        f"{row['managed_wal_files']} | "
                        f"{'passed' if row['recovery_verified'] and row['physical_bound_verified'] else 'failed'} | "
                        f"{_fmt_ms(row['reclamation_pause_p99_ms'])} | {_fmt_ms(row['reopen_ms'])} |"
                    )
                lines += ["", reclamation["conditions"], ""]
            elif reclamation:
                lines += ["WAL reclamation evidence failed normalization: "
                          + "; ".join(reclamation["qualification_errors"]), ""]
        elif section_id == "complete-durable-service" and section["rows"]:
            lines += ["| Added loopback egress | Rafter throughput | OpenRaft throughput | Advantage |",
                      "| ---: | ---: | ---: | ---: |"]
            for row in section["rows"]:
                lines.append(f"| {row['network_delay_ms']} ms | **{_fmt_ops(row['rafter_throughput_ops_s'])} writes/s** | "
                             f"{_fmt_ops(row['openraft_throughput_ops_s'])} writes/s | **{row['throughput_ratio']:.2f}×** |"
                             )
            lines += ["", "| Added egress | Offered rate | Rafter p99 | OpenRaft p99 | Rafter p99.9 | OpenRaft p99.9 |",
                      "| ---: | ---: | ---: | ---: | ---: | ---: |"]
            for row in section["rows"]:
                latency_rows = (
                    ("saturation", row["rafter_saturated_p99_ms"], row["openraft_saturated_p99_ms"],
                     row["rafter_saturated_p999_ms"], row["openraft_saturated_p999_ms"]),
                    ("100/s", row["rafter_p99_at_100_ms"], row["openraft_p99_at_100_ms"],
                     row["rafter_p999_at_100_ms"], row["openraft_p999_at_100_ms"]),
                    ("1,000/s", row["rafter_p99_at_1000_ms"], row["openraft_p99_at_1000_ms"],
                     row["rafter_p999_at_1000_ms"], row["openraft_p999_at_1000_ms"]),
                )
                for rate, rafter_p99, openraft_p99, rafter_p999, openraft_p999 in latency_rows:
                    lines.append(f"| {row['network_delay_ms']} ms | {rate} | {_fmt_ms(rafter_p99)} | "
                                 f"{_fmt_ms(openraft_p99)} | {_fmt_ms(rafter_p999)} | {_fmt_ms(openraft_p999)} |")
            lines.append("")
            accounting = section["accounting"]["rafter"]
            control_accounting = section["accounting"]["openraft"]
            lines += [f"Rafter accounting: {accounting['errors']} errors · {accounting['unknown']} unknown · {accounting['not_issued']} unsent",
                      f"OpenRaft accounting: {control_accounting['errors']} errors · {control_accounting['unknown']} unknown · {control_accounting['not_issued']} unsent", ""]
            if section["internal_comparisons"]:
                comparison = section["internal_comparisons"][0]
                candidate_name = comparison["candidate_name"]
                prior_name = comparison["prior_name"]
                changes = " · ".join(f"{item['network_delay_ms']} ms: {item['throughput_change_percent']:+.1f}%"
                                     for item in section["internal_comparisons"])
                lines += [f"{candidate_name} versus same-code {prior_name} throughput — {changes}", "",
                          f"| Added egress | Offered rate | {candidate_name} p99 | {prior_name} p99 | {candidate_name} p99.9 | {prior_name} p99.9 |",
                          "| ---: | ---: | ---: | ---: | ---: | ---: |"]
                for item in section["internal_comparisons"]:
                    for latency in item["latencies"]:
                        rate = "saturation" if latency["offered_per_second"] == 0 else f"{latency['offered_per_second']:,}/s"
                        lines.append(f"| {item['network_delay_ms']} ms | {rate} | {_fmt_ms(latency['candidate_p99_ms'])} | "
                                     f"{_fmt_ms(latency['prior_p99_ms'])} | {_fmt_ms(latency['candidate_p999_ms'])} | "
                                     f"{_fmt_ms(latency['prior_p999_ms'])} |")
                lines.append("")
        elif section_id == "failure-and-sustained-operation" and section["checks"]:
            for check in section["checks"]:
                if "passed_cases" in check:
                    count = check["passed_cases"]
                    noun = "case" if count == 1 else "cases"
                    lines.append(f"- {check['name']}: {count} {noun} passed — {check['scope']}")
                else:
                    scenarios = ", ".join(f"{item['scenario']} {item['status']}" for item in check["scenarios"])
                    lines.append(f"- {check['name']}: {scenarios} — {check['scope']}")
            lines.append("")
        if section_id == "failure-and-sustained-operation" and section.get("reclamation_under_load"):
            under_load = section["reclamation_under_load"]
            lines += [
                f"WAL reclamation under load: **{under_load['status']}**",
                "",
                under_load["scope"],
                "",
                "| Snapshot interval | Offered | Throughput retained | Worst p99 / p99.9 delta | "
                "Compactions / Raft maintenance max | Managed Raft after load | Managed Raft after restart | Max restart |",
                "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
            for row in under_load["comparisons"]:
                rate = ("saturation" if row["offered_per_second"] == 0 else
                        f"{row['offered_per_second']:,}/s")
                lines.append(
                    f"| {row['snapshot_interval_entries']:,} | {rate} | "
                    f"{row['median_throughput_ratio'] * 100:.1f}% | "
                    f"{row['worst_p99_regression_ms']:+.3f} / "
                    f"{row['worst_p999_regression_ms']:+.3f} ms | "
                    f"{row['measured_compactions']:,} / "
                    f"{row['max_compaction_upper_bound_ns'] / 1e6:.3f} ms | "
                    f"{_fmt_mib(row['median_candidate_managed_raft_allocated_bytes'])} / "
                    f"{_fmt_mib(row['median_control_managed_raft_allocated_bytes'])} | "
                    f"{_fmt_mib(row['median_candidate_final_managed_raft_allocated_bytes'])} / "
                    f"{_fmt_mib(row['median_control_final_managed_raft_allocated_bytes'])} | "
                    f"{row['max_candidate_process_restart_ns'] / 1e9:.3f} s |"
                )
            lines.append("")
            staged = [
                row
                for row in under_load["comparisons"]
                if row.get("native_snapshot_stages")
            ]
            if staged:
                lines += [
                    "Snapshot publication and WAL reclamation stage maxima from separate "
                    "diagnostic cases "
                    "(log2 upper bounds, not timing-run percentiles):",
                    "",
                    "| Snapshot interval | Offered | "
                    + " | ".join(label for _, label in SNAPSHOT_STAGE_COLUMNS)
                    + " |",
                    "| ---: | ---: | "
                    + " | ".join("---:" for _ in SNAPSHOT_STAGE_COLUMNS)
                    + " |",
                ]
                for row in staged:
                    rate = (
                        "saturation"
                        if row["offered_per_second"] == 0
                        else f"{row['offered_per_second']:,}/s"
                    )
                    maxima = " | ".join(
                        _snapshot_stage_maximum(row, stage)
                        for stage, _ in SNAPSHOT_STAGE_COLUMNS
                    )
                    lines.append(
                        f"| {row['snapshot_interval_entries']:,} | {rate} | {maxima} |"
                    )
                lines.append("")
            application_staged = [
                row
                for row in under_load["comparisons"]
                if row.get("application_snapshot_encode")
                or row.get("application_checkpoint_stages")
            ]
            if application_staged:
                lines += [
                    "Application maintenance from separate diagnostic cases "
                    "(counts and log2 upper bounds, not timing-run percentiles):",
                    "",
                    "| Snapshot interval | Offered | Snapshot encodes / WAL reclaims / "
                    "journal checkpoints | "
                    "Encode max | "
                    + " | ".join(
                        label for _, label in APPLICATION_CHECKPOINT_STAGE_COLUMNS
                    )
                    + " |",
                    "| ---: | ---: | ---: | ---: | "
                    + " | ".join("---:" for _ in APPLICATION_CHECKPOINT_STAGE_COLUMNS)
                    + " |",
                ]
                for row in application_staged:
                    rate = (
                        "saturation"
                        if row["offered_per_second"] == 0
                        else f"{row['offered_per_second']:,}/s"
                    )
                    maxima = " | ".join(
                        _application_stage_maximum(row, stage)
                        for stage, _ in APPLICATION_CHECKPOINT_STAGE_COLUMNS
                    )
                    lines.append(
                        f"| {row['snapshot_interval_entries']:,} | {rate} | "
                        f"{_application_maintenance_counts(row)} | "
                        f"{_application_encode_maximum(row)} | {maxima} |"
                    )
                lines.append("")
            for name, verdict in under_load["verdicts"].items():
                lines.append(f"- {name.replace('_', ' ')}: {verdict['status']}")
                lines.extend(f"  - {error}" for error in verdict.get("errors", []))
            lines.append("")
        if section_id == "complete-durable-service" and section["capacity"]["variant_boundaries"]:
            objective = section["capacity"]["objective"]
            lines += [f"Useful-capacity objective: every repetition achieves at least "
                      f"{objective['minimum_achieved_percent']:.0f}% of offered load, p99 ≤ "
                      f"{objective['maximum_p99_ms']:.0f} ms, p99.9 ≤ {objective['maximum_p999_ms']:.0f} ms, "
                      "and zero errors, unknown outcomes, or unsent requests.", "",
                      f"**{section['capacity']['result']}**", "",
                      "| Added egress | Configuration | Highest qualifying rate | Tested through |",
                      "| ---: | --- | ---: | ---: |"]
            for boundary in section["capacity"]["variant_boundaries"]:
                lines.append(
                    f"| {boundary['network_delay_ms']} ms | {boundary['display_name']} | "
                    f"{_fmt_capacity(boundary['highest_qualifying_rate'], boundary['reached_tested_ceiling'])} | "
                    f"{_fmt_ops(boundary['maximum_tested_rate'])}/s |"
                )
            lines.append("")
        if section_id == "complete-durable-service" and section["capacity"]["rows"]:
            lines += ["| Added egress | Offered | Engine | Min achieved | Arrival p99/p99.9 | Execution p99/p99.9 | Dispatch p99/p99.9 | Start-wait p99 | Post-window | Loss | Meets objective |",
                      "| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: |"]
            for row in section["capacity"]["rows"]:
                for engine, label in (("rafter", "Rafter"), ("openraft", "OpenRaft")):
                    point = row[engine]
                    loss = point["errors"] + point["unknown"] + point["unsent"]
                    lines.append(f"| {row['network_delay_ms']} ms | {_fmt_ops(row['offered_per_second'])}/s | {label} | "
                                 f"{_fmt_ops(point['minimum_achieved_ops_s'])}/s ({point['minimum_achieved_percent']:.1f}%) | "
                                 f"{_fmt_ms(point['worst_repetition_p99_ms'])} / {_fmt_ms(point['worst_repetition_p999_ms'])} | "
                                 f"{_fmt_ms(point['worst_repetition_execution_p99_ms'])} / {_fmt_ms(point['worst_repetition_execution_p999_ms'])} | "
                                 f"{_fmt_ms(point['worst_repetition_scheduler_dispatch_p99_ms'])} / "
                                 f"{_fmt_ms(point['worst_repetition_scheduler_dispatch_p999_ms'])} | "
                                 f"{_fmt_ms(point['worst_repetition_client_start_p99_ms'])} | "
                                 f"{point['completed_after_window']} | {loss} | "
                                 f"{'yes' if point['qualifies'] else 'no'} |")
            lines.append("")
        if section.get("conditions"):
            lines += [section["conditions"], ""]
        if section.get("details"):
            lines += [*(f"- {detail}" for detail in section["details"]), ""]
        if section.get("history"):
            lines += ["Visible history:", ""]
            for group in _history_groups(section["history"]):
                noun = "case" if group["cases"] == 1 else "cases"
                lines.append(f"- Run {group['run']}: {group['cases']} affected {noun}, "
                             f"{group['unsent']} unsent, {group['unknown']} unknown, "
                             f"{group['errors']} errors. Individual cases remain in `summary.json`.")
            lines.append("")
        if section.get("not_measured"):
            lines += ["Not yet measured: " + "; ".join(section["not_measured"]) + ".", ""]
        evidence_ids = sorted({ref["evidence"] for ref in section.get("source_cases", [])})
        links = [f"[{evidence[item]['label']}]({evidence[item]['href']})" for item in evidence_ids if item in evidence]
        if links:
            lines += ["Evidence: " + " · ".join(links), ""]
    lines += ["---", "", "This is implementation performance, not a claim of a faster Raft algorithm. "
              "The report is generated deterministically from normalized evidence; no LLM writes or selects results.", ""]
    return "\n".join(lines)


def render_html(summary: dict, evidence: dict) -> str:
    markdown = render_markdown(summary, evidence)
    sections = {section["id"]: section for section in summary["sections"]}
    cards = []
    for section_id in ("consensus-in-memory", "durable-replication", "complete-durable-service", "failure-and-sustained-operation"):
        section = sections[section_id]
        content = [f"<p class='question'>{html.escape(section['question'])}</p>",
                   f"<p class='result'>{html.escape(section['result'])}</p>"]
        if section_id == "complete-durable-service":
            repository = section.get("benchmark_repository")
            if repository:
                content.append(
                    "<p class='conditions'>Benchmark source: "
                    f"<code>{html.escape(repository['commit'])}</code> "
                    f"({html.escape(repository['status'])} at preflight).</p>"
                )
            labels = {
                "evidence_integrity": "Evidence integrity",
                "correctness_checks": "Correctness checks",
                "environment_qualification": "Environment qualification",
                "feature_coverage": "Feature coverage",
                "service_objective": "Service objective",
            }
            verdict_body = "".join(
                f"<tr><td>{html.escape(label)}</td><td>{html.escape(section['verdicts'][name]['status'])}</td>"
                f"<td>{html.escape(_verdict_detail(section['verdicts'][name]))}</td></tr>"
                for name, label in labels.items()
            )
            content.append("<div class='table'><table><thead><tr><th>Verdict</th><th>Status</th><th>Detail</th>"
                           f"</tr></thead><tbody>{verdict_body}</tbody></table></div>")
            context = section["measured_load_context"]
            if context["status"] != "not measured":
                content.append(
                    f"<p class='conditions'>{html.escape(_measured_load_context_text(context))}</p>"
                )
        if section_id == "consensus-in-memory" and section["rows"]:
            body = "".join(
                f"<tr><td>{html.escape(row['workload'])}</td><td>{row['max_in_flight']}</td>"
                f"<td>{_fmt_ops(row['rafter_ops_s'])}/s</td><td>{_fmt_ops(row['raft_rs_ops_s'])}/s</td>"
                f"<td>{_fmt_ops(row['openraft_ops_s'])}/s</td><td>{_fmt_us(row['rafter_p99_us'])}</td>"
                f"<td>{_fmt_us(row['raft_rs_p99_us'])}</td><td>{_fmt_us(row['openraft_p99_us'])}</td></tr>"
                for row in section["rows"]
            )
            content.append("<div class='table'><table><thead><tr><th>Workload</th><th>In flight</th>"
                           "<th>Rafter</th><th>raft-rs</th><th>OpenRaft</th><th>Rafter p99</th>"
                           f"<th>raft-rs p99</th><th>OpenRaft p99</th></tr></thead><tbody>{body}</tbody></table></div>")
        elif section_id == "durable-replication":
            if section["rows"]:
                body = "".join(
                    f"<tr><td>{row['batch_size']}</td><td>{_fmt_ops(row['file_replacement_ops_s'])}/s</td>"
                    f"<td>{_fmt_ops(row['append_only_journal_ops_s'])}/s</td><td>+{row['throughput_change_percent']:.1f}%</td>"
                    f"<td>{_fmt_ms(row['file_replacement_batch_p99_ms'])}</td>"
                    f"<td>{_fmt_ms(row['append_only_journal_batch_p99_ms'])}</td></tr>" for row in section["rows"])
                content.append("<div class='table'><table><thead><tr><th>Batch</th><th>File replacement</th>"
                               "<th>Append-only journal</th><th>Throughput change</th><th>Replacement p99</th><th>Journal p99</th>"
                               f"</tr></thead><tbody>{body}</tbody></table></div>")
                if section["sync_probes"]:
                    probes = section["sync_probes"]
                    content.append("<p>Separate hard-state syscall probe: "
                                   f"file replacement {probes['replace']['sync_calls_per_publication']:.1f}, "
                                   f"journal {probes['journal']['sync_calls_per_publication']:.1f} sync calls/publication. "
                                   "This is not synchronization calls per committed entry.</p>")
            reclamation = section["wal_reclamation"]
            if reclamation and reclamation["status"] == "passed":
                body = "".join(
                    f"<tr><td>{row['retained_entries']:,} entries</td>"
                    f"<td>{row['managed_wal_bytes']:,} bytes</td><td>{row['managed_wal_files']}</td>"
                    f"<td>{'passed' if row['recovery_verified'] and row['physical_bound_verified'] else 'failed'}</td>"
                    f"<td>{_fmt_ms(row['reclamation_pause_p99_ms'])}</td>"
                    f"<td>{_fmt_ms(row['reopen_ms'])}</td></tr>"
                    for row in reclamation["rows"]
                )
                content.append("<p><strong>WAL physical-reclamation component qualification</strong></p>"
                               "<div class='table'><table><thead><tr><th>Retained suffix</th>"
                               "<th>Managed WAL</th><th>Files</th><th>Exact recovery</th>"
                               "<th>Diagnostic pause p99</th><th>Diagnostic reopen</th>"
                               f"</tr></thead><tbody>{body}</tbody></table></div>"
                               f"<p>{html.escape(reclamation['conditions'])}</p>")
            elif reclamation:
                content.append("<p>WAL reclamation evidence failed normalization: "
                               + html.escape("; ".join(reclamation["qualification_errors"])) + "</p>")
        elif section_id == "complete-durable-service" and section["rows"]:
            throughput_body = "".join(
                f"<tr><td>{row['network_delay_ms']} ms</td><td><strong>{_fmt_ops(row['rafter_throughput_ops_s'])}/s</strong></td>"
                f"<td>{_fmt_ops(row['openraft_throughput_ops_s'])}/s</td><td><strong>{row['throughput_ratio']:.2f}×</strong></td>"
                "</tr>"
                for row in section["rows"])
            content.append("<div class='table'><table><thead><tr><th>Added egress</th><th>Rafter throughput</th>"
                           f"<th>OpenRaft throughput</th><th>Advantage</th></tr></thead><tbody>{throughput_body}</tbody></table></div>")
            latency_body = ""
            for row in section["rows"]:
                latency_rows = (
                    ("saturation", row["rafter_saturated_p99_ms"], row["openraft_saturated_p99_ms"],
                     row["rafter_saturated_p999_ms"], row["openraft_saturated_p999_ms"]),
                    ("100/s", row["rafter_p99_at_100_ms"], row["openraft_p99_at_100_ms"],
                     row["rafter_p999_at_100_ms"], row["openraft_p999_at_100_ms"]),
                    ("1,000/s", row["rafter_p99_at_1000_ms"], row["openraft_p99_at_1000_ms"],
                     row["rafter_p999_at_1000_ms"], row["openraft_p999_at_1000_ms"]),
                )
                latency_body += "".join(
                    f"<tr><td>{row['network_delay_ms']} ms</td><td>{rate}</td><td>{_fmt_ms(rafter_p99)}</td>"
                    f"<td>{_fmt_ms(openraft_p99)}</td><td>{_fmt_ms(rafter_p999)}</td><td>{_fmt_ms(openraft_p999)}</td></tr>"
                    for rate, rafter_p99, openraft_p99, rafter_p999, openraft_p999 in latency_rows)
            content.append("<div class='table'><table><thead><tr><th>Added egress</th><th>Offered rate</th>"
                           "<th>Rafter p99</th><th>OpenRaft p99</th><th>Rafter p99.9</th><th>OpenRaft p99.9</th>"
                           f"</tr></thead><tbody>{latency_body}</tbody></table></div>")
            accounting = section["accounting"]["rafter"]
            control_accounting = section["accounting"]["openraft"]
            content.append(f"<p>Rafter accounting: {accounting['errors']} errors · {accounting['unknown']} unknown · {accounting['not_issued']} unsent<br>"
                           f"OpenRaft accounting: {control_accounting['errors']} errors · {control_accounting['unknown']} unknown · {control_accounting['not_issued']} unsent</p>")
            if section["internal_comparisons"]:
                comparison = section["internal_comparisons"][0]
                candidate_name = comparison["candidate_name"]
                prior_name = comparison["prior_name"]
                changes = " · ".join(f"{item['network_delay_ms']} ms: {item['throughput_change_percent']:+.1f}%"
                                     for item in section["internal_comparisons"])
                content.append(f"<p>{html.escape(candidate_name)} versus same-code "
                               f"{html.escape(prior_name)} throughput — {html.escape(changes)}</p>")
                internal_body = ""
                for item in section["internal_comparisons"]:
                    internal_body += "".join(
                        f"<tr><td>{item['network_delay_ms']} ms</td><td>{'saturation' if latency['offered_per_second'] == 0 else format(latency['offered_per_second'], ',') + '/s'}</td>"
                        f"<td>{_fmt_ms(latency['candidate_p99_ms'])}</td><td>{_fmt_ms(latency['prior_p99_ms'])}</td>"
                        f"<td>{_fmt_ms(latency['candidate_p999_ms'])}</td><td>{_fmt_ms(latency['prior_p999_ms'])}</td></tr>"
                        for latency in item["latencies"])
                content.append("<div class='table'><table><thead><tr><th>Added egress</th><th>Offered rate</th>"
                               f"<th>{html.escape(candidate_name)} p99</th><th>{html.escape(prior_name)} p99</th>"
                               f"<th>{html.escape(candidate_name)} p99.9</th><th>{html.escape(prior_name)} p99.9</th>"
                               f"</tr></thead><tbody>{internal_body}</tbody></table></div>")
        elif section_id == "failure-and-sustained-operation" and section["checks"]:
            items = []
            for check in section["checks"]:
                if "passed_cases" in check:
                    count = check["passed_cases"]
                    noun = "case" if count == 1 else "cases"
                    value = f"{count} {noun} passed"
                else:
                    value = ", ".join(
                        f"{item['scenario']} {item['status']}" for item in check["scenarios"]
                    )
                items.append(f"<li><strong>{html.escape(check['name'])}:</strong> {html.escape(value)} — {html.escape(check['scope'])}</li>")
            content.append("<ul>" + "".join(items) + "</ul>")
        if section_id == "failure-and-sustained-operation" and section.get("reclamation_under_load"):
            under_load = section["reclamation_under_load"]
            content.append(
                f"<p>WAL reclamation under load: <strong>{html.escape(under_load['status'])}</strong></p>"
                f"<p class='conditions'>{html.escape(under_load['scope'])}</p>"
            )
            reclamation_body = ""
            for row in under_load["comparisons"]:
                rate = ("saturation" if row["offered_per_second"] == 0 else
                        f"{row['offered_per_second']:,}/s")
                reclamation_body += (
                    f"<tr><td>{row['snapshot_interval_entries']:,}</td><td>{rate}</td>"
                    f"<td>{row['median_throughput_ratio'] * 100:.1f}%</td>"
                    f"<td>{row['worst_p99_regression_ms']:+.3f} / "
                    f"{row['worst_p999_regression_ms']:+.3f} ms</td>"
                    f"<td>{row['measured_compactions']:,} / "
                    f"{row['max_compaction_upper_bound_ns'] / 1e6:.3f} ms</td>"
                    f"<td>{_fmt_mib(row['median_candidate_managed_raft_allocated_bytes'])} / "
                    f"{_fmt_mib(row['median_control_managed_raft_allocated_bytes'])}</td>"
                    f"<td>{_fmt_mib(row['median_candidate_final_managed_raft_allocated_bytes'])} / "
                    f"{_fmt_mib(row['median_control_final_managed_raft_allocated_bytes'])}</td>"
                    f"<td>{row['max_candidate_process_restart_ns'] / 1e9:.3f} s</td></tr>"
                )
            content.append(
                "<div class='table'><table><thead><tr><th>Snapshot interval</th><th>Offered</th>"
                "<th>Throughput retained</th><th>Worst p99 / p99.9 delta</th>"
                "<th>Compactions / Raft maintenance max</th><th>Managed Raft after load</th>"
                "<th>Managed Raft after restart</th><th>Max restart</th>"
                f"</tr></thead><tbody>{reclamation_body}</tbody></table></div>"
            )
            staged = [
                row
                for row in under_load["comparisons"]
                if row.get("native_snapshot_stages")
            ]
            if staged:
                stage_body = ""
                for row in staged:
                    rate = (
                        "saturation"
                        if row["offered_per_second"] == 0
                        else f"{row['offered_per_second']:,}/s"
                    )
                    values = "".join(
                        f"<td>{_snapshot_stage_maximum(row, stage)}</td>"
                        for stage, _ in SNAPSHOT_STAGE_COLUMNS
                    )
                    stage_body += (
                        f"<tr><td>{row['snapshot_interval_entries']:,}</td>"
                        f"<td>{rate}</td>{values}</tr>"
                    )
                headings = "".join(
                    f"<th>{html.escape(label)}</th>"
                    for _, label in SNAPSHOT_STAGE_COLUMNS
                )
                content.append(
                    "<p>Snapshot publication and WAL reclamation stage maxima from separate "
                    "diagnostic cases "
                    "(log2 upper bounds, not timing-run percentiles):</p>"
                    "<div class='table'><table><thead><tr><th>Snapshot interval</th>"
                    f"<th>Offered</th>{headings}</tr></thead>"
                    f"<tbody>{stage_body}</tbody></table></div>"
                )
            application_staged = [
                row
                for row in under_load["comparisons"]
                if row.get("application_snapshot_encode")
                or row.get("application_checkpoint_stages")
            ]
            if application_staged:
                application_body = ""
                for row in application_staged:
                    rate = (
                        "saturation"
                        if row["offered_per_second"] == 0
                        else f"{row['offered_per_second']:,}/s"
                    )
                    maxima = "".join(
                        f"<td>{_application_stage_maximum(row, stage)}</td>"
                        for stage, _ in APPLICATION_CHECKPOINT_STAGE_COLUMNS
                    )
                    application_body += (
                        f"<tr><td>{row['snapshot_interval_entries']:,}</td>"
                        f"<td>{rate}</td>"
                        f"<td>{_application_maintenance_counts(row)}</td>"
                        f"<td>{_application_encode_maximum(row)}</td>{maxima}</tr>"
                    )
                headings = "".join(
                    f"<th>{html.escape(label)}</th>"
                    for _, label in APPLICATION_CHECKPOINT_STAGE_COLUMNS
                )
                content.append(
                    "<p>Application maintenance from separate diagnostic cases "
                    "(counts and log2 upper bounds, not timing-run percentiles):</p>"
                    "<div class='table'><table><thead><tr><th>Snapshot interval</th>"
                    "<th>Offered</th><th>Snapshot encodes / WAL reclaims / "
                    "journal checkpoints</th>"
                    f"<th>Encode max</th>{headings}</tr></thead>"
                    f"<tbody>{application_body}</tbody></table></div>"
                )
            verdicts = "".join(
                f"<li><strong>{html.escape(name.replace('_', ' '))}:</strong> "
                f"{html.escape(verdict['status'])}"
                + ("<ul>" + "".join(
                    f"<li>{html.escape(str(error))}</li>" for error in verdict.get("errors", [])
                ) + "</ul>" if verdict.get("errors") else "")
                + "</li>"
                for name, verdict in under_load["verdicts"].items()
            )
            content.append(f"<ul>{verdicts}</ul>")
        if section_id == "complete-durable-service" and section["capacity"]["variant_boundaries"]:
            objective = section["capacity"]["objective"]
            content.append(f"<p>Useful-capacity objective: every repetition achieves at least "
                           f"{objective['minimum_achieved_percent']:.0f}% of offered load, p99 ≤ "
                           f"{objective['maximum_p99_ms']:.0f} ms, p99.9 ≤ {objective['maximum_p999_ms']:.0f} ms, "
                           "and zero errors, unknown outcomes, or unsent requests.</p>")
            content.append(f"<p><strong>{html.escape(section['capacity']['result'])}</strong></p>")
            boundary_body = "".join(
                f"<tr><td>{boundary['network_delay_ms']} ms</td>"
                f"<td>{html.escape(boundary['display_name'])}</td>"
                f"<td>{_fmt_capacity(boundary['highest_qualifying_rate'], boundary['reached_tested_ceiling'])}</td>"
                f"<td>{_fmt_ops(boundary['maximum_tested_rate'])}/s</td></tr>"
                for boundary in section["capacity"]["variant_boundaries"]
            )
            content.append("<div class='table'><table><thead><tr><th>Added egress</th><th>Configuration</th>"
                           "<th>Highest qualifying rate</th><th>Tested through</th>"
                           f"</tr></thead><tbody>{boundary_body}</tbody></table></div>")
        if section_id == "complete-durable-service" and section["capacity"]["rows"]:
            capacity_body = ""
            for row in section["capacity"]["rows"]:
                for engine, label in (("rafter", "Rafter"), ("openraft", "OpenRaft")):
                    point = row[engine]
                    loss = point["errors"] + point["unknown"] + point["unsent"]
                    capacity_body += (f"<tr><td>{row['network_delay_ms']} ms</td><td>{_fmt_ops(row['offered_per_second'])}/s</td>"
                                      f"<td>{label}</td><td>{_fmt_ops(point['minimum_achieved_ops_s'])}/s "
                                      f"({point['minimum_achieved_percent']:.1f}%)</td>"
                                      f"<td>{_fmt_ms(point['worst_repetition_p99_ms'])} / "
                                      f"{_fmt_ms(point['worst_repetition_p999_ms'])}</td>"
                                      f"<td>{_fmt_ms(point['worst_repetition_execution_p99_ms'])} / "
                                      f"{_fmt_ms(point['worst_repetition_execution_p999_ms'])}</td>"
                                      f"<td>{_fmt_ms(point['worst_repetition_scheduler_dispatch_p99_ms'])} / "
                                      f"{_fmt_ms(point['worst_repetition_scheduler_dispatch_p999_ms'])}</td>"
                                      f"<td>{_fmt_ms(point['worst_repetition_client_start_p99_ms'])}</td>"
                                      f"<td>{point['completed_after_window']}</td><td>{loss}</td>"
                                      f"<td>{'yes' if point['qualifies'] else 'no'}</td></tr>")
            content.append("<div class='table'><table><thead><tr><th>Added egress</th><th>Offered</th><th>Engine</th>"
                           "<th>Min achieved</th><th>Arrival p99/p99.9</th><th>Execution p99/p99.9</th>"
                           "<th>Dispatch p99/p99.9</th><th>Start-wait p99</th><th>Post-window</th>"
                           f"<th>Loss</th><th>Meets objective</th></tr></thead><tbody>{capacity_body}</tbody></table></div>")
        if section.get("conditions"):
            content.append(f"<p class='conditions'>{html.escape(section['conditions'])}</p>")
        if section.get("details"):
            content.append("<ul>" + "".join(f"<li>{html.escape(item)}</li>" for item in section["details"]) + "</ul>")
        if section.get("history"):
            items = ""
            for group in _history_groups(section["history"]):
                noun = "case" if group["cases"] == 1 else "cases"
                items += (f"<li>Run {html.escape(group['run'])}: {group['cases']} affected {noun}, "
                          f"{group['unsent']} unsent, {group['unknown']} unknown, "
                          f"{group['errors']} errors. Individual cases remain in <code>summary.json</code>.</li>")
            content.append("<details><summary>Visible history</summary><ul>" + items + "</ul></details>")
        if section.get("not_measured"):
            content.append("<p><strong>Not yet measured:</strong> "
                           + html.escape("; ".join(section["not_measured"])) + ".</p>")
        evidence_ids = sorted({ref["evidence"] for ref in section.get("source_cases", [])})
        links = [f"<a href='{html.escape(evidence[item]['href'], quote=True)}'>{html.escape(evidence[item]['label'])}</a>"
                 for item in evidence_ids if item in evidence]
        if links:
            content.append("<p class='links'>" + " · ".join(links) + "</p>")
        cards.append(f"<section id='{section_id}'><div class='status'>{html.escape(section['status'])}</div>"
                     f"<h2>{html.escape(section['title'])}</h2>{''.join(content)}</section>")
    return f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(summary['title'])}</title><style>
:root{{--ink:#142019;--muted:#5d6861;--line:#dbe2dd;--paper:#fbfcfb;--accent:#176b45}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--paper);color:var(--ink);font:16px/1.5 system-ui,sans-serif}}
main{{max-width:1060px;margin:56px auto;padding:0 24px}}h1{{font-size:clamp(2rem,5vw,3.4rem);line-height:1.05;margin-bottom:12px}}
.lede{{font-size:1.15rem;color:var(--muted);max-width:760px;margin-bottom:40px}}section{{background:white;border:1px solid var(--line);border-radius:14px;padding:28px;margin:18px 0}}
h2{{margin:2px 0 8px;font-size:1.5rem}}.status{{float:right;color:var(--accent);font-size:.78rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em}}
.question{{color:var(--muted);margin:0 0 14px}}.result{{font-size:1.2rem;font-weight:700;max-width:820px}}.conditions{{color:var(--muted);font-size:.92rem;max-width:900px}}
.table{{overflow-x:auto}}table{{border-collapse:collapse;width:100%;font-size:.9rem}}th,td{{text-align:right;padding:10px 12px;border-bottom:1px solid var(--line);white-space:nowrap}}th:first-child,td:first-child{{text-align:left}}
a{{color:var(--accent)}}code{{overflow-wrap:anywhere}}footer{{color:var(--muted);font-size:.9rem;margin:32px 4px}}@media(max-width:600px){{main{{margin:28px auto}}section{{padding:20px}}.status{{float:none;margin-bottom:8px}}}}
</style></head><body><main><h1>{html.escape(summary['title'])}</h1><p class="lede">{html.escape(summary['principle'])}</p>{''.join(cards)}
<footer>This is implementation performance, not a claim of a faster Raft algorithm. Generated deterministically from normalized evidence; no LLM writes or selects results. <a href="summary.md">Markdown</a> · <a href="summary.json">JSON</a></footer>
<!-- Markdown rendering is generated from the same summary object; length={len(markdown)} --></main></body></html>'''


def _evidence_entry(label: str, path: Path, destination: Path, url: str | None) -> dict:
    details = path / "report.html" if (path / "report.html").exists() else path
    parts = path.parts
    artifact_path = Path(*parts[parts.index("results"):]).as_posix() if "results" in parts else "."
    return {"label": label, "href": url or os.path.relpath(details, destination),
            "artifact_path": artifact_path,
            "local_root": None if url else os.path.relpath(path, destination)}


def generate(*, destination: Path, durable_path: Path | None = None,
             storage_path: Path | None = None, reclamation_path: Path | None = None,
             micro_path: Path | None = None,
             history_paths: list[Path] | None = None, headline_variant: str | None = None,
             headline_control_variant: str | None = None,
             durable_url: str | None = None, storage_url: str | None = None,
             reclamation_url: str | None = None,
             micro_url: str | None = None, history_urls: list[str] | None = None) -> dict:
    history_paths = history_paths or []
    history_urls = history_urls or []
    if history_urls and len(history_urls) != len(history_paths):
        raise ValueError("provide one --history-url for every --history-suite")
    durable = load_durable_suite(durable_path) if durable_path else None
    storage = load_storage_comparison(storage_path) if storage_path else None
    reclamation = load_wal_reclamation(reclamation_path) if reclamation_path else None
    micro = load_microbench(micro_path) if micro_path else None
    histories = [load_durable_suite(path) for path in history_paths]
    summary = build_summary(durable=durable, storage=storage, reclamation=reclamation, micro=micro,
                            histories=histories, headline_variant=headline_variant,
                            headline_control_variant=headline_control_variant)
    destination = destination.resolve()
    destination.mkdir(parents=True, exist_ok=False)
    evidence = {}
    if durable_path:
        evidence["durable_service"] = _evidence_entry("Detailed service results", durable_path.resolve(), destination, durable_url)
    if storage_path:
        evidence["durable_replication"] = _evidence_entry("Storage evidence", storage_path.resolve(), destination, storage_url)
    if reclamation_path:
        evidence["wal_reclamation"] = _evidence_entry(
            "WAL reclamation evidence", reclamation_path.resolve(), destination, reclamation_url
        )
    if micro_path:
        evidence["consensus_in_memory"] = _evidence_entry("In-memory evidence", micro_path.resolve(), destination, micro_url)
    for index, path in enumerate(history_paths):
        url = history_urls[index] if history_urls else None
        evidence[f"history_{index + 1}"] = _evidence_entry(f"Historical run {index + 1}", path.resolve(), destination, url)
    summary["evidence"] = evidence
    write_json(destination / "summary.json", summary)
    with (destination / "summary.md").open("x") as stream:
        stream.write(render_markdown(summary, evidence))
    with (destination / "report.html").open("x") as stream:
        stream.write(render_html(summary, evidence))
    return summary

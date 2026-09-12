"""Separate workload feature observation from evidence and correctness verdicts."""
from __future__ import annotations

import json
from pathlib import Path


def _completed_cases(output: Path, variants: set[str]):
    for manifest_path in output.glob("*/manifest.json"):
        case = manifest_path.parent
        if (case / "failure.json").exists() or not (case / "outcome.json").exists():
            continue
        manifest = json.loads(manifest_path.read_text())
        variant = manifest.get("options", {}).get("variant")
        if variant in variants:
            yield variant, case


def completion_priority_checks(output: Path, variants: set[str]) -> list[dict]:
    observed = {variant: False for variant in variants}
    lookahead = {variant: False for variant in variants}
    lookahead_recorded = {variant: False for variant in variants}
    receipts = {variant: 0 for variant in variants}
    for variant, case in _completed_cases(output, variants):
        receipt_path = case / "completion-priority-activity.json"
        if not receipt_path.exists():
            continue
        receipt = json.loads(receipt_path.read_text())
        receipts[variant] += 1
        observed[variant] |= receipt.get("observed_during_load") is True
        lookahead_recorded[variant] |= receipt.get("schema") == 2
        lookahead[variant] |= receipt.get("observed_bounded_lookahead_during_load") is True
    checks = []
    for variant in sorted(variants):
        core_status = "passed" if observed[variant] else "not observed"
        lookahead_status = ("passed" if lookahead[variant]
                            else "not observed" if lookahead_recorded[variant]
                            else "not recorded")
        passed = observed[variant] and (
            not lookahead_recorded[variant] or lookahead[variant]
        )
        if not observed[variant]:
            detail = "durable completion-priority activity was not observed in any completed case"
        elif lookahead_recorded[variant] and not lookahead[variant]:
            detail = "bounded completion-priority lookahead was not observed in any completed case"
        elif not lookahead_recorded[variant]:
            detail = "completion-priority activity was observed; legacy receipts did not record bounded lookahead"
        else:
            detail = "completion-priority and bounded-lookahead activity were both observed"
        checks.append({
            "feature": "durable_completion_priority",
            "variant": variant,
            "status": "passed" if passed else "incomplete",
            "observations": {
                "priority_paths": core_status,
                "bounded_lookahead": lookahead_status,
                "receipts": receipts[variant],
            },
            "detail": detail,
        })
    return checks


def combined_step_checks(output: Path, variants: set[str]) -> list[dict]:
    observed = {variant: False for variant in variants}
    receipts = {variant: 0 for variant in variants}
    for variant, case in _completed_cases(output, variants):
        receipt_path = case / "pipeline-activity.json"
        if not receipt_path.exists():
            continue
        receipt = json.loads(receipt_path.read_text())
        receipts[variant] += 1
        combined = receipt.get("combined_peer_proposal_batches_by_node")
        observed[variant] |= isinstance(combined, dict) and sum(combined.values()) > 0
    return [{
        "feature": "combined_peer_proposals",
        "variant": variant,
        "status": "passed" if observed[variant] else "incomplete",
        "observations": {
            "combined_steps": "passed" if observed[variant] else "not observed",
            "receipts": receipts[variant],
        },
        "detail": ("combined peer/proposal steps were observed"
                   if observed[variant]
                   else "selected combined peer/proposal mode executed no combined steps in any completed case"),
    } for variant in sorted(variants)]


def verdict(output: Path, *, completion_priority: set[str], combined: set[str]) -> dict:
    checks = completion_priority_checks(output, completion_priority)
    checks.extend(combined_step_checks(output, combined))
    if not checks:
        status = "not selected"
    elif all(check["status"] == "passed" for check in checks):
        status = "passed"
    else:
        status = "incomplete"
    return {
        "status": status,
        "checks": checks,
        "scope": "workload observation only; deterministic code-path tests run in the locked Rust build",
    }

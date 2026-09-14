"""Deterministic execution plans for sequential paired durable-service suites."""
from __future__ import annotations
from .cluster import DEFAULT_APPLICATION_CHECKPOINT_BYTES


MODES = ("inline", "worker", "messages", "pipeline")


def mode_flags(mode: str, engine: str = "rafter") -> dict[str, bool]:
    if mode not in MODES:
        raise ValueError("unsupported embedding mode")
    return {
        "ordered_apply": mode != "inline" and engine == "rafter",
        "peer_message_stream": mode in ("messages", "pipeline") and engine == "rafter",
        "pipelined_durability": mode == "pipeline" and engine == "rafter",
    }


def ordered_arms(arms: list, repeat: int) -> list:
    return arms[repeat % len(arms):] + arms[:repeat % len(arms)]


def execution_plan(suite: dict) -> list[dict]:
    """Expand a suite declaration into the exact case order and option set."""
    arms = [tuple(arm) for arm in suite["arms"]]
    if not arms or any(len(arm) not in (7, 8) for arm in arms):
        raise ValueError("paired arms must contain seven legacy fields or eight snapshot-aware fields")
    labels = [arm[0] for arm in arms]
    if len(set(labels)) != len(labels):
        raise ValueError("paired arm labels must be distinct")
    by_label = dict(zip(labels, arms))
    diagnostic_labels = suite["diagnostic_arms"]
    if not diagnostic_labels or len(set(diagnostic_labels)) != len(diagnostic_labels):
        raise ValueError("diagnostic arm labels must be distinct and nonempty")
    try:
        diagnostic_arms = [by_label[label] for label in diagnostic_labels]
    except KeyError as error:
        raise ValueError(f"unknown diagnostic arm: {error.args[0]}") from error

    plan = []
    ordinal = 0
    for delay in suite["network_delays_ms"]:
        for diagnostic in (False, True):
            selected_arms = diagnostic_arms if diagnostic else arms
            rates = suite["diagnostic_rates"] if diagnostic else suite["rates"]
            repeats = 1 if diagnostic else suite["runs"]
            for repeat in range(repeats):
                for rate in rates:
                    for position, arm in enumerate(ordered_arms(selected_arms, repeat), start=1):
                        label, engine, cap, binary_set, threshold, combine, window = arm[:7]
                        snapshot_interval = arm[7] if len(arm) == 8 else 0
                        mode = suite[
                            "prior_mode" if binary_set == "prior" else "candidate_mode"
                        ]
                        flags = mode_flags(mode, engine)
                        priority = engine == "rafter" and suite[
                            "prior_durable_completion_priority"
                            if binary_set == "prior"
                            else "candidate_durable_completion_priority"
                        ]
                        ordinal += 1
                        measurement_mode = "diagnostic" if diagnostic else "timing"
                        name = (
                            f"{ordinal:03d}-n{delay}-{label}-r{repeat + 1}-q{rate}-"
                            f"{'trace' if diagnostic else 'timing'}"
                        )
                        options = {
                            "network_delay_ms": delay,
                            "scenario": "durable-kv",
                            "smoke": False,
                            "batch_size": 64,
                            "peer_batch_size": cap,
                            "diagnostics": diagnostic,
                            "duration": 60,
                            "warmup": 10,
                            **flags,
                            "openraft_async_flush": label == "openraft-async",
                            "max_speculative_proposals": threshold,
                            "max_inflight_appends": window,
                            "combine_peer_proposals": combine,
                            "durable_completion_priority": priority,
                            "snapshot_interval_entries": snapshot_interval,
                            "application_checkpoint_bytes": DEFAULT_APPLICATION_CHECKPOINT_BYTES,
                            "concurrency": 64,
                            "payload": 512,
                            "rate": rate,
                            "keyspace": 10000,
                            "seed": repeat + 1,
                            "read_percent": 0,
                            "cas_percent": 0,
                            "timeout": 2,
                            "variant": label,
                        }
                        plan.append({
                            "ordinal": ordinal,
                            "name": name,
                            "measurement_mode": measurement_mode,
                            "repetition": repeat + 1,
                            "arm_position": position,
                            "arm": label,
                            "implementation": engine,
                            "binary_set": binary_set,
                            "options": options,
                        })
    return plan


def load_window_plan(plan: list[dict]) -> dict:
    """Summarize declared load windows without pretending to predict wall time."""
    timing = sum(item["measurement_mode"] == "timing" for item in plan)
    diagnostic = sum(item["measurement_mode"] == "diagnostic" for item in plan)
    warmup = sum(item["options"]["warmup"] for item in plan)
    measurement = sum(item["options"]["duration"] for item in plan)
    return {
        "schema": 1,
        "cases": len(plan),
        "timing_cases": timing,
        "diagnostic_cases": diagnostic,
        "warmup_seconds": warmup,
        "measurement_seconds": measurement,
        "declared_load_window_seconds": warmup + measurement,
        "scope": (
            "declared warmup and measurement windows only; excludes builds, "
            "finite qualification histories, recovery checks, and process overhead"
        ),
    }

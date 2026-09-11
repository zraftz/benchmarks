"""Fail-closed comparison rules for normalized benchmark results."""
from __future__ import annotations

import json
import math
from typing import Any


EQUIVALENCE_PERCENT = 3.0
COMPARABLE_FIELDS = (
    "layer",
    "workload",
    "completion_boundary",
    "environment",
    "measurement_mode",
    "metric_definitions",
)


class NotComparable(ValueError):
    """Raised when two results do not measure the same observable workload."""


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def incompatibilities(left: dict, right: dict) -> list[str]:
    reasons = []
    for field in COMPARABLE_FIELDS:
        if _canonical(left.get(field)) != _canonical(right.get(field)):
            reasons.append(field)
    if left.get("qualification") != "passed" or right.get("qualification") != "passed":
        reasons.append("qualification")
    return reasons


def compare(left: dict, right: dict, metric: str, *, higher_is_better: bool) -> dict:
    """Compare one metric after checking every comparability boundary."""
    reasons = incompatibilities(left, right)
    if reasons:
        raise NotComparable("not comparable: " + ", ".join(reasons))
    left_value = left.get("metrics", {}).get(metric, {}).get("median")
    right_value = right.get("metrics", {}).get(metric, {}).get("median")
    if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0
           for value in (left_value, right_value)) or right_value == 0:
        raise NotComparable(f"not comparable: invalid {metric}")
    change = (left_value / right_value - 1.0) * 100.0
    advantage = change if higher_is_better else -change
    if advantage > EQUIVALENCE_PERCENT:
        label = "measured lead"
    elif advantage < -EQUIVALENCE_PERCENT:
        label = "measured regression"
    else:
        label = "roughly level"
    return {
        "metric": metric,
        "left": left_value,
        "right": right_value,
        "ratio": left_value / right_value,
        "change_percent": change,
        "advantage_percent": advantage,
        "higher_is_better": higher_is_better,
        "equivalence_percent": EQUIVALENCE_PERCENT,
        "label": label,
    }

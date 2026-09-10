"""Bounded exhaustive per-key linearizability checker for *complete* KV histories.

It checks Put/Get/CAS outcomes and real-time precedence. Unknown operations are
NOT dropped to manufacture a pass: v1 returns `inconclusive` for incomplete
histories. Fault runs have a separate acknowledged-canary recovery check.

The common model also has session semantics. Before partitioning by key, this
checker requires unique identities and sequential, nonoverlapping operations
per client, so deduplication cannot introduce a hidden cross-key dependency.
"""
from __future__ import annotations
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
import json
import time
from typing import Any


def transition(value: str | None, command: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    kind = command["kind"]
    swapped = None
    if kind == "put":
        value = command["value"]
    elif kind == "cas":
        swapped = value == command.get("expected")
        if swapped:
            value = command["value"]
    elif kind != "get":
        raise ValueError(f"unknown command: {kind}")
    return value, {"value": value, "swapped": swapped, "error": None}


def check_history(events: list[dict[str, Any]], *, timeout: float = 5.0,
                  max_states: int = 200_000, initial: dict[str, str] | None = None) -> dict[str, Any]:
    if not events:
        return {"status": "inconclusive", "reason": "empty history"}
    start = time.monotonic()
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    clients: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen = set()
    try:
        for event in events:
            c, response = event["command"], event["reply"]
            if response.get("status") != "ok" or not isinstance(response.get("result"), dict):
                return {"status": "inconclusive", "reason": "incomplete or unsuccessful operation"}
            if response["result"].get("error"):
                return {"status": "inconclusive", "reason": "session/application error"}
            if not isinstance(event["start_ns"], int) or not isinstance(event["end_ns"], int) or event["start_ns"] < 0 or event["end_ns"] < event["start_ns"]:
                raise ValueError("invalid invocation/response interval")
            identity = c["client"], c["sequence"]
            if identity in seen or not isinstance(c["sequence"], int) or c["sequence"] < 1:
                raise ValueError("duplicate/invalid command identity")
            seen.add(identity)
            transition(None, c)  # Validate supported model.
            groups[c["key"]].append(event)
            clients[c["client"]].append(event)
        for stream in clients.values():
            stream.sort(key=lambda e: e["start_ns"])
            for before, after in zip(stream, stream[1:]):
                if before["end_ns"] > after["start_ns"] or before["command"]["sequence"] >= after["command"]["sequence"]:
                    raise ValueError("client sessions must be nonoverlapping with increasing sequences")
    except (KeyError, TypeError, ValueError) as exc:
        return {"status": "invalid", "reason": str(exc)}
    visited = 0
    for key, operations in groups.items():
        if len(operations) > 250:
            return {"status": "inconclusive", "reason": "qualification key exceeds 250-operation limit"}
        n = len(operations)
        predecessors = []
        for candidate in operations:
            mask = 0
            for i, other in enumerate(operations):
                if other is not candidate and other["end_ns"] < candidate["start_ns"]:
                    mask |= 1 << i
            predecessors.append(mask)
        full = (1 << n) - 1

        @lru_cache(maxsize=max_states)
        def visit(done: int, value: str | None) -> bool:
            nonlocal visited
            visited += 1
            if visited > max_states or time.monotonic() - start > timeout:
                raise TimeoutError("checker resource limit")
            if done == full:
                return True
            for i, event in enumerate(operations):
                bit = 1 << i
                if done & bit or predecessors[i] & ~done:
                    continue
                new_value, expected = transition(value, event["command"])
                observed = event["reply"]["result"]
                if all(observed.get(k) == v for k, v in expected.items()) and visit(done | bit, new_value):
                    return True
            return False

        try:
            passed = visit(0, (initial or {}).get(key))
        except (TimeoutError, RecursionError) as exc:
            return {"status": "inconclusive", "reason": str(exc), "states": visited}
        if not passed:
            return {"status": "failed", "key": key, "states": visited,
                    "reason": "no legal sequential ordering respects response values and real time"}
    return {"status": "passed", "operations": len(events), "keys": len(groups), "states": visited,
            "scope": "complete qualification history only; not a proof of all executions"}


def read_history(path: Path) -> list[dict[str, Any]]:
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError("qualification history exceeds 16 MiB")
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]

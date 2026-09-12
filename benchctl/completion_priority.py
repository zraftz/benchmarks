"""Verify bounded owner scheduling activity without claiming a latency boundary."""
from __future__ import annotations


FIELDS = (
    "prioritized_peer_batches",
    "prioritized_peer_events",
    "pre_persistence_client_completions",
)


def activity(before: dict, after: dict) -> dict:
    def counters(snapshot: dict) -> dict[str, dict[str, int]]:
        if {str(node) for node in snapshot} != {"1", "2", "3"}:
            raise ValueError("durable completion priority requires all three node statuses")
        result = {}
        for node, status in snapshot.items():
            receipt = status.get("info", {}).get("durable_completion_priority", {})
            if status.get("status") != "ok" or receipt.get("enabled") is not True:
                raise ValueError("durable completion priority requires enabled counters from each node")
            values = {name: receipt.get(name) for name in FIELDS}
            if any(type(value) is not int or value < 0 for value in values.values()):
                raise ValueError("durable completion priority has an invalid counter")
            result[str(node)] = values
        return result

    earlier, later = counters(before), counters(after)
    deltas = {
        name: {node: later[node][name] - earlier[node][name] for node in earlier}
        for name in FIELDS
    }
    if any(value < 0 for by_node in deltas.values() for value in by_node.values()):
        raise ValueError("durable completion priority counters reset during the load")
    return {
        "schema": 1,
        "status": "passed",
        "observed_during_load": any(
            value > 0 for by_node in deltas.values() for value in by_node.values()
        ),
        **{f"{name}_by_node": values for name, values in deltas.items()},
        "scope": "load and generator drain, before post-load canaries; not an exact measurement-window count",
    }

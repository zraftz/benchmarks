"""Verify bounded owner scheduling activity without claiming a latency boundary."""
from __future__ import annotations


CORE_FIELDS = (
    "prioritized_peer_batches",
    "prioritized_peer_events",
    "pre_persistence_client_completions",
)
LOOKAHEAD_FIELD = "prioritized_client_inputs_bypassed"


def activity(before: dict, after: dict) -> dict:
    def counters(snapshot: dict) -> tuple[dict[str, dict[str, int]], bool]:
        if {str(node) for node in snapshot} != {"1", "2", "3"}:
            raise ValueError("durable completion priority requires all three node statuses")
        result = {}
        lookahead_presence = set()
        for node, status in snapshot.items():
            receipt = status.get("info", {}).get("durable_completion_priority", {})
            if status.get("status") != "ok" or receipt.get("enabled") is not True:
                raise ValueError("durable completion priority requires enabled counters from each node")
            lookahead_presence.add(LOOKAHEAD_FIELD in receipt)
            fields = CORE_FIELDS + ((LOOKAHEAD_FIELD,) if LOOKAHEAD_FIELD in receipt else ())
            values = {name: receipt.get(name) for name in fields}
            if any(type(value) is not int or value < 0 for value in values.values()):
                raise ValueError("durable completion priority has an invalid counter")
            result[str(node)] = values
        if len(lookahead_presence) != 1:
            raise ValueError("durable completion priority has inconsistent lookahead counters")
        return result, lookahead_presence == {True}

    (earlier, earlier_has_lookahead), (later, later_has_lookahead) = (
        counters(before), counters(after)
    )
    if earlier_has_lookahead != later_has_lookahead:
        raise ValueError("durable completion priority lookahead counter changed availability")
    fields = CORE_FIELDS + ((LOOKAHEAD_FIELD,) if earlier_has_lookahead else ())
    deltas = {
        name: {node: later[node][name] - earlier[node][name] for node in earlier}
        for name in fields
    }
    if any(value < 0 for by_node in deltas.values() for value in by_node.values()):
        raise ValueError("durable completion priority counters reset during the load")
    result = {
        "schema": 2 if earlier_has_lookahead else 1,
        "status": "passed",
        "observed_during_load": any(
            value > 0 for by_node in deltas.values() for value in by_node.values()
        ),
        **{f"{name}_by_node": values for name, values in deltas.items()},
        "scope": "load and generator drain, before post-load canaries; not an exact measurement-window count",
    }
    if earlier_has_lookahead:
        result["observed_bounded_lookahead_during_load"] = any(
            value > 0 for value in deltas[LOOKAHEAD_FIELD].values()
        )
    return result

"""Prove that a selected pipeline actually executed during a steady-state load."""
from __future__ import annotations


def activity(before: dict, after: dict, *, require_combined: bool = False) -> dict:
    def counters(snapshot):
        result = {}
        if {str(node) for node in snapshot} != {"1", "2", "3"}:
            raise ValueError("pipeline activity requires all three node statuses")
        for node, status in snapshot.items():
            pipeline = status.get("info", {}).get("engine", {}).get("persistence_pipeline", {})
            count = pipeline.get("completed_operations")
            if status.get("status") != "ok" or pipeline.get("enabled") is not True or type(count) is not int or count < 0:
                raise ValueError("pipeline activity requires enabled counters from each node")
            optional = {}
            for name in ("submitted_operations", "synchronous_proposal_batches", "synchronous_proposals",
                         "speculative_proposal_batches", "speculative_proposals",
                         "combined_peer_proposal_batches", "combined_peer_events",
                         "combined_peer_proposals"):
                value = pipeline.get(name)
                if value is not None and (type(value) is not int or value < 0):
                    raise ValueError(f"pipeline activity has an invalid {name} counter")
                optional[name] = value
            threshold = pipeline.get("max_speculative_proposals")
            if threshold is not None and (type(threshold) is not int or not 1 <= threshold <= 64):
                raise ValueError("pipeline activity has an invalid speculative proposal limit")
            sizes = pipeline.get("proposal_batch_sizes")
            if sizes is not None:
                if (not isinstance(sizes, dict) or any(not key.isdigit() or int(key) < 1 or int(key) > 64
                        or type(value) is not int or value < 0 for key, value in sizes.items())):
                    raise ValueError("pipeline activity has invalid proposal batch counters")
            result[str(node)] = {"completed_operations": count, "max_speculative_proposals": threshold,
                                 "proposal_batch_sizes": sizes, **optional}
        return result
    earlier, later = counters(before), counters(after)
    delta = {node: later[node]["completed_operations"] - values["completed_operations"]
             for node, values in earlier.items()}
    if any(count < 0 for count in delta.values()):
        raise ValueError("pipeline counters reset during the steady-state load")
    if sum(delta.values()) == 0:
        raise ValueError("selected pipeline completed no persistence operations during the load")
    result = {"schema": 2, "status": "passed", "completed_by_node": delta,
            "scope": "load and generator drain, before post-load canaries; not an exact measurement-window count"}
    optional_names = ("submitted_operations", "synchronous_proposal_batches", "synchronous_proposals",
                      "speculative_proposal_batches", "speculative_proposals",
                      "combined_peer_proposal_batches", "combined_peer_events",
                      "combined_peer_proposals")
    for name in optional_names:
        presence = [values[name] is not None for values in (*earlier.values(), *later.values())]
        if any(presence) and not all(presence):
            raise ValueError(f"pipeline {name} counter availability changed")
        if all(presence):
            values = {node: later[node][name] - earlier[node][name] for node in earlier}
            if any(value < 0 for value in values.values()):
                raise ValueError(f"pipeline {name} counter regressed")
            result[f"{name}_by_node"] = values
    combined = result.get("combined_peer_proposal_batches_by_node")
    if require_combined and (combined is None or sum(combined.values()) == 0):
        raise ValueError("selected combined peer/proposal mode executed no combined steps")
    threshold_presence = [values["max_speculative_proposals"] is not None
                          for values in (*earlier.values(), *later.values())]
    if any(threshold_presence) and not all(threshold_presence):
        raise ValueError("pipeline speculative proposal limit availability changed")
    if all(threshold_presence):
        thresholds = {node: later[node]["max_speculative_proposals"] for node in earlier}
        if any(earlier[node]["max_speculative_proposals"] != value for node, value in thresholds.items()):
            raise ValueError("pipeline speculative proposal limit changed during load")
        result["max_speculative_proposals_by_node"] = thresholds
    size_presence = [values["proposal_batch_sizes"] is not None
                     for values in (*earlier.values(), *later.values())]
    if any(size_presence) and not all(size_presence):
        raise ValueError("pipeline proposal batch counter availability changed")
    if all(size_presence):
        sizes_by_node = {}
        for node in earlier:
            keys = set(earlier[node]["proposal_batch_sizes"]) | set(later[node]["proposal_batch_sizes"])
            sizes_by_node[node] = {
                key: later[node]["proposal_batch_sizes"].get(key, 0)
                - earlier[node]["proposal_batch_sizes"].get(key, 0)
                for key in sorted(keys, key=int)
            }
            if any(value < 0 for value in sizes_by_node[node].values()):
                raise ValueError("pipeline proposal batch counter regressed")
        result["proposal_batch_sizes_by_node"] = sizes_by_node
    return result


def recorded_activity_matches(actual: dict, recorded: dict) -> bool:
    """Compare current, transitional, and legacy sealed activity verdicts."""
    if recorded.get("schema") == 2:
        return actual == recorded
    if "schema" in recorded:
        return False
    compatible = dict(actual)
    compatible.pop("schema", None)
    legacy_fields = {"status", "completed_by_node", "scope"}
    if set(recorded) == legacy_fields:
        compatible = {key: compatible[key] for key in legacy_fields}
    return compatible == recorded

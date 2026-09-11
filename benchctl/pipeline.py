"""Prove that a selected pipeline actually executed during a steady-state load."""
from __future__ import annotations


def activity(before: dict, after: dict) -> dict:
    def counters(snapshot):
        result = {}
        if {str(node) for node in snapshot} != {"1", "2", "3"}:
            raise ValueError("pipeline activity requires all three node statuses")
        for node, status in snapshot.items():
            pipeline = status.get("info", {}).get("engine", {}).get("persistence_pipeline", {})
            count = pipeline.get("completed_operations")
            if status.get("status") != "ok" or pipeline.get("enabled") is not True or type(count) is not int or count < 0:
                raise ValueError("pipeline activity requires enabled counters from each node")
            result[str(node)] = count
        return result
    earlier, later = counters(before), counters(after)
    delta = {node: later[node] - count for node, count in earlier.items()}
    if any(count < 0 for count in delta.values()):
        raise ValueError("pipeline counters reset during the steady-state load")
    if sum(delta.values()) == 0:
        raise ValueError("selected pipeline completed no persistence operations during the load")
    return {"status": "passed", "completed_by_node": delta,
            "scope": "load and generator drain, before post-load canaries; not an exact measurement-window count"}

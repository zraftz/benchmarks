import copy
import unittest
from benchctl.pipeline import activity


def snapshot(counts):
    return {str(i + 1): {"status": "ok", "info": {"engine": {"persistence_pipeline": {
        "enabled": True, "completed_operations": count}}}} for i, count in enumerate(counts)}


def instrumented(counts, speculative, synchronous, sizes, threshold=2):
    result = snapshot(counts)
    for node, status in result.items():
        pipeline = status["info"]["engine"]["persistence_pipeline"]
        pipeline.update({"submitted_operations": counts[int(node) - 1],
                         "synchronous_proposal_batches": synchronous[int(node) - 1],
                         "synchronous_proposals": synchronous[int(node) - 1],
                         "speculative_proposal_batches": speculative[int(node) - 1],
                         "speculative_proposals": speculative[int(node) - 1],
                         "proposal_batch_sizes": sizes[int(node) - 1],
                         "max_speculative_proposals": threshold})
    return result


class PipelineActivityTests(unittest.TestCase):
    def test_counts_are_deltas_across_nodes_without_claiming_latency(self):
        result = activity(snapshot([7, 0, 0]), snapshot([12, 3, 0]))
        self.assertEqual(result["completed_by_node"], {"1": 5, "2": 3, "3": 0})
        self.assertEqual(result["status"], "passed")

    def test_fallback_or_restart_cannot_masquerade_as_exercised_pipeline(self):
        before = snapshot([7, 0, 0])
        for after in (snapshot([7, 0, 0]), snapshot([0, 2, 0])):
            with self.subTest(after=after), self.assertRaises(ValueError):
                activity(before, after)

    def test_missing_disabled_or_malformed_counters_fail_closed(self):
        before = snapshot([0, 0, 0])
        after = snapshot([1, 0, 0])
        cases = [dict(list(after.items())[:2])]
        for value in (False, None):
            changed = copy.deepcopy(after)
            changed["2"]["info"]["engine"]["persistence_pipeline"]["enabled"] = value
            cases.append(changed)
        for value in (-1, True, "1", None):
            changed = copy.deepcopy(after)
            changed["1"]["info"]["engine"]["persistence_pipeline"]["completed_operations"] = value
            cases.append(changed)
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValueError):
                activity(before, case)

    def test_threshold_and_batch_geometry_are_counter_deltas(self):
        before = instrumented([4, 0, 0], [3, 0, 0], [1, 0, 0], [{"1": 3, "4": 1}, {}, {}])
        after = instrumented([9, 0, 0], [7, 0, 0], [2, 0, 0], [{"1": 7, "4": 2}, {}, {}])
        result = activity(before, after)
        self.assertEqual(result["max_speculative_proposals_by_node"], {"1": 2, "2": 2, "3": 2})
        self.assertEqual(result["speculative_proposal_batches_by_node"]["1"], 4)
        self.assertEqual(result["proposal_batch_sizes_by_node"]["1"], {"1": 4, "4": 1})

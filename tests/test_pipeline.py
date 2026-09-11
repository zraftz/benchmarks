import copy
import unittest
from benchctl.pipeline import activity


def snapshot(counts):
    return {str(i + 1): {"status": "ok", "info": {"engine": {"persistence_pipeline": {
        "enabled": True, "completed_operations": count}}}} for i, count in enumerate(counts)}


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

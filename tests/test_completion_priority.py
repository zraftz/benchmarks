import copy
import unittest

from benchctl.completion_priority import activity


def snapshot(peer_batches: int, peer_events: int, completions: int) -> dict:
    result = {}
    for node in (1, 2, 3):
        result[str(node)] = {
            "status": "ok",
            "info": {
                "durable_completion_priority": {
                    "enabled": True,
                    "prioritized_peer_batches": peer_batches if node == 1 else 0,
                    "prioritized_peer_events": peer_events if node == 1 else 0,
                    "pre_persistence_client_completions": completions if node == 1 else 0,
                }
            },
        }
    return result


class CompletionPriorityTests(unittest.TestCase):
    def test_records_counter_deltas_and_observed_activity(self):
        result = activity(snapshot(2, 3, 4), snapshot(7, 11, 10))
        self.assertTrue(result["observed_during_load"])
        self.assertEqual(result["prioritized_peer_batches_by_node"]["1"], 5)
        self.assertEqual(result["prioritized_peer_events_by_node"]["1"], 8)
        self.assertEqual(result["pre_persistence_client_completions_by_node"]["1"], 6)

    def test_no_available_work_is_recorded_without_fabricating_activity(self):
        result = activity(snapshot(2, 3, 4), snapshot(2, 3, 4))
        self.assertFalse(result["observed_during_load"])

    def test_missing_disabled_malformed_or_reset_counters_fail_closed(self):
        before = snapshot(2, 3, 4)
        cases = []
        missing = copy.deepcopy(before)
        del missing["3"]
        cases.append(missing)
        disabled = copy.deepcopy(before)
        disabled["2"]["info"]["durable_completion_priority"]["enabled"] = False
        cases.append(disabled)
        malformed = copy.deepcopy(before)
        malformed["1"]["info"]["durable_completion_priority"]["prioritized_peer_batches"] = True
        cases.append(malformed)
        reset = snapshot(1, 3, 4)
        cases.append(reset)
        for after in cases:
            with self.subTest(after=after), self.assertRaises(ValueError):
                activity(before, after)

import copy
import unittest

from benchctl.comparisons import NotComparable, compare


def result(value=100.0):
    return {
        "layer": "complete_service",
        "workload": {"payload_bytes": 512, "concurrency": 64, "rate": 0},
        "completion_boundary": "durable-log+durable-application-v1/logged-reads",
        "environment": {"host": "same"},
        "measurement_mode": "timing",
        "metric_definitions": {"throughput_ops_s": "successful completions per second"},
        "qualification": "passed",
        "metrics": {"throughput_ops_s": {"median": value}},
    }


class ComparisonTests(unittest.TestCase):
    def test_fixed_equivalence_band_and_direction(self):
        self.assertEqual(compare(result(102), result(100), "throughput_ops_s", higher_is_better=True)["label"], "roughly level")
        self.assertEqual(compare(result(104), result(100), "throughput_ops_s", higher_is_better=True)["label"], "measured lead")
        self.assertEqual(compare(result(90), result(100), "throughput_ops_s", higher_is_better=False)["label"], "measured lead")

    def test_each_evidence_boundary_is_fail_closed(self):
        for field, value in (
            ("layer", "durable_replication"),
            ("workload", {"payload_bytes": 1024, "concurrency": 64, "rate": 0}),
            ("completion_boundary", "leader commit callback"),
            ("environment", {"host": "other"}),
            ("measurement_mode", "diagnostic"),
            ("metric_definitions", {"throughput_ops_s": "attempts per second"}),
            ("qualification", "failed"),
        ):
            changed = copy.deepcopy(result())
            changed[field] = value
            with self.subTest(field=field), self.assertRaises(NotComparable):
                compare(result(), changed, "throughput_ops_s", higher_is_better=True)

    def test_invalid_metric_is_not_comparable(self):
        for value in (float("nan"), -1, None, 0):
            with self.subTest(value=value), self.assertRaises(NotComparable):
                compare(result(), result(value), "throughput_ops_s", higher_is_better=True)

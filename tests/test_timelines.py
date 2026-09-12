import copy
import unittest

from benchctl.timelines import STAGES, extract, recorded_extract_matches
from benchctl.replication_windows import configuration_receipt


def timeline(ordinal=65, boundary="engine_apply_effect"):
    return {
        "sample_ordinal": ordinal,
        "log_index": 44,
        "commit_boundary": boundary,
        "points_ns": {stage: index * 10 for index, stage in enumerate(STAGES)},
    }


def snapshot(seen, retained, *, sample_every=64):
    sampled = 0 if seen == 0 else 1 + (seen - 1) // sample_every
    state = {
        "schema": 1,
        "sample_every": sample_every,
        "retained_limit": 256,
        "in_flight_limit": 256,
        "commit_mark_limit": 8192,
        "operations_seen": seen,
        "operations_sampled": sampled,
        "completed_seen": sampled,
        "abandoned": 0,
        "skipped_in_flight": 0,
        "discarded_incomplete": 0,
        "evicted_commit_marks": 0,
        "in_flight": 0,
        "commit_marks": 0,
        "retained": retained,
    }
    return {
        str(node): {
            "status": "ok",
            "info": {
                "diagnostics": {
                    "enabled": True,
                    "operation_timelines": copy.deepcopy(state),
                }
            },
        }
        for node in range(1, 4)
    }


def add_diagnostics(snapshot_value, *, samples, total, observed, full):
    for status in snapshot_value.values():
        status["info"]["diagnostics"]["metrics"] = {
            "owner_persistence_blocked_ns": {
                "samples": samples,
                "total": total,
                "max": total,
                "buckets_log2": [samples],
            }
        }
        status["info"]["engine"] = {
            "replication_windows": {
                "definition": "owner-observed",
                "observations": samples,
                "observed_ns": observed,
                "any_full_ns": full,
                "full_follower_ns": full * 2,
                "currently_full_followers": 0,
                "windows": [],
            }
        }
    return snapshot_value


def add_runtime_windows(snapshot_value, maximum=8):
    for node, status in snapshot_value.items():
        status["info"]["implementation"] = "rafter"
        status["info"]["node_id"] = int(node)
        status["info"]["leader"] = node == "1"
        status["info"].setdefault("engine", {})["replication_windows"] = {
            "windows": ([
                {"follower_id": 2, "max_in_flight_batches": maximum},
                {"follower_id": 3, "max_in_flight_batches": maximum},
            ] if node == "1" else [])
        }
    return snapshot_value


class TimelineTests(unittest.TestCase):
    def test_replication_window_configuration_receipt_proves_runtime_bound(self):
        before = add_runtime_windows(snapshot(64, [timeline(1)]), 4)
        after = add_runtime_windows(snapshot(128, [timeline(1), timeline(65)]), 4)
        receipt = configuration_receipt(before, after, 4)
        self.assertEqual(receipt["status"], "passed")
        self.assertEqual(receipt["requested_max_in_flight_batches"], 4)
        self.assertEqual(receipt["snapshots"]["before"]["leaders"][0]["node_id"], 1)

    def test_replication_window_configuration_rejects_inactive_or_missing_bound(self):
        before = add_runtime_windows(snapshot(64, [timeline(1)]), 8)
        after = add_runtime_windows(snapshot(128, [timeline(1), timeline(65)]), 4)
        with self.assertRaisesRegex(ValueError, "reports replication window 8, expected 4"):
            configuration_receipt(before, after, 4)
        before["1"]["info"]["engine"]["replication_windows"]["windows"] = []
        with self.assertRaisesRegex(ValueError, "has no replication windows"):
            configuration_receipt(before, after, 8)

    def test_extract_uses_pre_measurement_watermark(self):
        before = snapshot(64, [timeline(1)])
        after = snapshot(128, [timeline(1), timeline(65)])
        result = extract(before, after)
        self.assertEqual(result["retained_measurement_timelines"], 3)
        self.assertEqual(
            result["nodes"]["1"]["retained_measurement_timelines"],
            [timeline(65)],
        )
        self.assertEqual(result["nodes"]["1"]["measurement_operations_seen"], 64)

    def test_rejects_nonmonotonic_or_unknown_boundaries(self):
        before = snapshot(64, [timeline(1)])
        for changed in (timeline(65, "kernel_commit"), timeline(65)):
            if changed["commit_boundary"] == "engine_apply_effect":
                changed["points_ns"]["application_started"] = 1
            after = snapshot(128, [timeline(1), changed])
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                extract(before, after)

    def test_rejects_counter_or_configuration_drift(self):
        before = snapshot(64, [timeline(1)])
        after = snapshot(128, [timeline(1), timeline(65)])
        after["1"]["info"]["diagnostics"]["operation_timelines"]["operations_sampled"] += 1
        with self.assertRaises(ValueError):
            extract(before, after)
        after = snapshot(128, [timeline(1), timeline(65)], sample_every=32)
        with self.assertRaises(ValueError):
            extract(before, after)

    def test_rejects_malformed_or_incomplete_retained_state(self):
        before = snapshot(64, [timeline(1)])
        after = snapshot(128, [timeline(1), timeline(65)])
        after["1"]["info"]["diagnostics"]["operation_timelines"] = []
        with self.assertRaises(ValueError):
            extract(before, after)
        after = snapshot(128, [timeline(1)])
        with self.assertRaises(ValueError):
            extract(before, after)

    def test_extracts_precommit_and_replication_window_deltas(self):
        before = add_diagnostics(snapshot(64, [timeline(1)]), samples=2, total=20,
                                 observed=100, full=25)
        after = add_diagnostics(snapshot(128, [timeline(1), timeline(65)]), samples=5, total=80,
                                observed=300, full=125)
        result = extract(before, after)
        node = result["nodes"]["1"]
        metric = node["measurement_metrics"]["owner_persistence_blocked_ns"]
        self.assertEqual(metric["samples"], 3)
        self.assertEqual(metric["mean_ns"], 20)
        windows = node["measurement_replication_windows"]
        self.assertEqual(windows["observed_ns"], 200)
        self.assertEqual(windows["any_full_fraction"], .5)
        self.assertEqual(windows["mean_full_followers"], 1.0)

    def test_sealed_extraction_compatibility_is_explicit_and_fail_closed(self):
        current = extract(snapshot(64, [timeline(1)]), snapshot(128, [timeline(1), timeline(65)]))
        transitional = copy.deepcopy(current)
        transitional["schema"] = 1
        legacy = copy.deepcopy(transitional)
        for node in legacy["nodes"].values():
            node.pop("measurement_metrics")
            node.pop("measurement_replication_windows")
        self.assertTrue(recorded_extract_matches(current, current))
        self.assertTrue(recorded_extract_matches(current, transitional))
        self.assertTrue(recorded_extract_matches(current, legacy))
        malformed = copy.deepcopy(transitional)
        malformed["nodes"]["1"].pop("measurement_metrics")
        self.assertFalse(recorded_extract_matches(current, malformed))
        self.assertFalse(recorded_extract_matches(current, {**current, "schema": 3}))

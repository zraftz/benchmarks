import copy
import unittest

from benchctl.timelines import STAGES, extract


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


class TimelineTests(unittest.TestCase):
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

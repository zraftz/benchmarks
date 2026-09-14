import json
import tempfile
import unittest
from pathlib import Path

from benchctl.evidence import snapshot_reclamation_errors
from benchctl.reclamation_service import NATIVE_SNAPSHOT_STAGES, activity


def statuses(*, completed: int, installs: tuple[int, int, int], index: int) -> dict:
    return {
        str(node): {
            "info": {
                "application": {"applied_index": index + 10},
                "engine": {"commit_index": index + 10},
                "snapshot_compaction": {
                    "interval_entries": 64,
                    "completed": completed,
                    "current_index": index,
                    "total_ns": completed * 100,
                    "max_ns": 100 if completed else 0,
                    "latest_payload_bytes": 512 if completed else 0,
                    "application_installs": installs[node - 1],
                },
            }
        }
        for node in (1, 2, 3)
    }


def add_native_snapshot_metrics(value: dict, bucket_counts: dict[int, int]) -> None:
    calls = sum(bucket_counts.values())
    for status in value.values():
        metrics = {}
        for stage in NATIVE_SNAPSHOT_STAGES:
            buckets = [0] * 64 if calls else []
            for bucket, count in bucket_counts.items():
                buckets[bucket] = count
            metrics[stage] = {
                "calls": calls,
                "total_ns": calls * 100,
                "max_ns": 100 if calls else 0,
                "buckets_ns_log2": buckets,
            }
        status["info"]["engine"]["persistence_diagnostics"] = metrics


class ReclamationServiceTests(unittest.TestCase):
    def test_follower_catchup_requires_compaction_and_application_install(self):
        before = statuses(completed=1, installs=(0, 0, 0), index=64)
        after = statuses(completed=2, installs=(0, 0, 1), index=128)
        receipt = activity(
            before,
            after,
            64,
            "snapshot-catchup",
            restarted_node=3,
            stopped_application_index=100,
            required_snapshot_index=128,
        )
        self.assertEqual(receipt["status"], "passed")
        self.assertEqual(receipt["totals"]["compactions_since_before_status"], 4)
        self.assertEqual(receipt["totals"]["application_installs_since_before_status"], 1)

        after["3"]["info"]["snapshot_compaction"]["application_installs"] = 0
        failed = activity(
            before,
            after,
            64,
            "snapshot-catchup",
            restarted_node=3,
            stopped_application_index=100,
            required_snapshot_index=128,
        )
        self.assertEqual(failed["status"], "failed")
        self.assertIn(
            "the lagging-follower scenario did not install an application snapshot",
            failed["failures"],
        )

    def test_verifier_replays_declared_receipt_and_rejects_tampering(self):
        before = statuses(completed=1, installs=(0, 0, 0), index=64)
        after = statuses(completed=2, installs=(0, 0, 1), index=128)
        manifest = {
            "scenario": "snapshot-catchup",
            "options": {"snapshot_interval_entries": 64},
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "before.json").write_text(json.dumps(before))
            (root / "snapshot-after-scenario.json").write_text(json.dumps(after))
            (root / "fault.json").write_text(json.dumps({
                "events": [
                    {"node": 3, "action": "SIGKILL follower"},
                    {"node": 3, "action": "restart same data directory"},
                ],
                "snapshot_catchup": {
                    "stopped_application_index": 100,
                    "required_snapshot_index": 128,
                },
            }))
            receipt = activity(
                before,
                after,
                64,
                "snapshot-catchup",
                restarted_node=3,
                stopped_application_index=100,
                required_snapshot_index=128,
            )
            (root / "snapshot-compaction-activity.json").write_text(json.dumps(receipt))
            self.assertEqual(snapshot_reclamation_errors(root, manifest), [])

            receipt["totals"]["compactions_since_before_status"] += 1
            (root / "snapshot-compaction-activity.json").write_text(json.dumps(receipt))
            self.assertEqual(
                snapshot_reclamation_errors(root, manifest),
                ["live snapshot/reclamation receipt differs from runtime status"],
            )

    def test_verifier_rejects_fault_action_drift(self):
        before = statuses(completed=1, installs=(0, 0, 0), index=64)
        after = statuses(completed=2, installs=(0, 0, 1), index=128)
        manifest = {
            "scenario": "snapshot-catchup",
            "options": {"snapshot_interval_entries": 64},
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "before.json").write_text(json.dumps(before))
            (root / "snapshot-after-scenario.json").write_text(json.dumps(after))
            (root / "fault.json").write_text(json.dumps({
                "events": [
                    {"node": 3, "action": "SIGSTOP follower"},
                    {"node": 3, "action": "restart same data directory"},
                ],
                "snapshot_catchup": {
                    "stopped_application_index": 100,
                    "required_snapshot_index": 128,
                },
            }))
            receipt = activity(
                before,
                after,
                64,
                "snapshot-catchup",
                restarted_node=3,
                stopped_application_index=100,
                required_snapshot_index=128,
            )
            (root / "snapshot-compaction-activity.json").write_text(json.dumps(receipt))
            self.assertEqual(
                snapshot_reclamation_errors(root, manifest),
                ["snapshot catch-up fault actions differ from the declared scenario"],
            )

    def test_current_status_reports_measurement_window_compaction_histogram(self):
        before = statuses(completed=1, installs=(0, 0, 0), index=64)
        after = statuses(completed=3, installs=(0, 0, 0), index=128)
        for status in before.values():
            buckets = [0] * 64
            buckets[6] = 1
            status["info"]["snapshot_compaction"]["buckets_log2"] = buckets
        for status in after.values():
            buckets = [0] * 64
            buckets[6] = 1
            buckets[7] = 2
            status["info"]["snapshot_compaction"]["buckets_log2"] = buckets
        receipt = activity(before, after, 64, "durable-kv")
        self.assertEqual(receipt["schema"], 2)
        self.assertEqual(
            receipt["nodes"]["1"]["measurement_compaction"],
            {
                "samples": 2,
                "total_ns": 200,
                "mean_ns": 100.0,
                "max_upper_bound_ns": 255,
                "buckets_log2": [0] * 7 + [2] + [0] * 56,
            },
        )

    def test_current_status_reports_native_snapshot_stage_deltas(self):
        before = statuses(completed=1, installs=(0, 0, 0), index=64)
        after = statuses(completed=3, installs=(0, 0, 0), index=128)
        for status in before.values():
            buckets = [0] * 64
            buckets[6] = 1
            status["info"]["snapshot_compaction"]["buckets_log2"] = buckets
        for status in after.values():
            buckets = [0] * 64
            buckets[6] = 1
            buckets[7] = 2
            status["info"]["snapshot_compaction"]["buckets_log2"] = buckets
        add_native_snapshot_metrics(before, {6: 1})
        add_native_snapshot_metrics(after, {6: 1, 7: 2})

        receipt = activity(before, after, 64, "durable-kv")

        self.assertEqual(receipt["schema"], 3)
        publication = receipt["totals"]["native_snapshot_stages"][
            "snapshot_publication"
        ]
        self.assertEqual(publication["samples"], 6)
        self.assertEqual(publication["total_ns"], 600)
        self.assertEqual(publication["mean_ns"], 100.0)
        self.assertEqual(publication["max_upper_bound_ns"], 255)
        self.assertEqual(publication["buckets_ns_log2"], [0] * 7 + [6] + [0] * 56)

    def test_native_snapshot_stage_availability_cannot_change_mid_case(self):
        before = statuses(completed=1, installs=(0, 0, 0), index=64)
        after = statuses(completed=2, installs=(0, 0, 0), index=128)
        add_native_snapshot_metrics(after, {7: 1})

        receipt = activity(before, after, 64, "durable-kv")

        self.assertEqual(receipt["status"], "failed")
        self.assertTrue(any(
            "native snapshot stage availability changed" in failure
            for failure in receipt["failures"]
        ))


if __name__ == "__main__":
    unittest.main()

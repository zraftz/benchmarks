import json
import tempfile
import unittest
from pathlib import Path

from benchctl.evidence import snapshot_reclamation_errors
from benchctl.reclamation_service import activity


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


if __name__ == "__main__":
    unittest.main()

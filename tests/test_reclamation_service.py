import json
import tempfile
import unittest
from pathlib import Path

from benchctl.evidence import snapshot_reclamation_errors
from benchctl.reclamation_service import (
    APPLICATION_CHECKPOINT_STAGES,
    NATIVE_SNAPSHOT_STAGES,
    NATIVE_WAL_RECLAMATION_STAGES,
    activity,
)


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


def add_wal_reclamation_metrics(value: dict, bucket_counts: dict[int, int]) -> None:
    calls = sum(bucket_counts.values())
    for status in value.values():
        metrics = status["info"]["engine"]["persistence_diagnostics"]
        for stage in NATIVE_WAL_RECLAMATION_STAGES:
            buckets = [0] * 64 if calls else []
            for bucket, count in bucket_counts.items():
                buckets[bucket] = count
            metrics[stage] = {
                "calls": calls,
                "total_ns": calls * 100,
                "max_ns": 100 if calls else 0,
                "buckets_ns_log2": buckets,
            }


def add_application_checkpoint_metrics(value: dict, bucket_counts: dict[int, int]) -> None:
    samples = sum(bucket_counts.values())
    for status in value.values():
        metrics = {}
        for stage in APPLICATION_CHECKPOINT_STAGES:
            buckets = [0] * 64 if samples else []
            for bucket, count in bucket_counts.items():
                buckets[bucket] = count
            metrics[stage] = {
                "samples": samples,
                "total": samples * 100,
                "max": 100 if samples else 0,
                "buckets_log2": buckets,
            }
        status["info"]["application"]["diagnostics"] = {"metrics": metrics}


def add_application_snapshot_encode_metrics(
    value: dict, bucket_counts: dict[int, int]
) -> None:
    samples = sum(bucket_counts.values())
    for status in value.values():
        buckets = [0] * 64 if samples else []
        for bucket, count in bucket_counts.items():
            buckets[bucket] = count
        status["info"]["diagnostics"] = {
            "metrics": {
                "application_snapshot_encode_ns": {
                    "samples": samples,
                    "total": samples * 100,
                    "max": 100 if samples else 0,
                    "buckets_log2": buckets,
                }
            }
        }


def add_log_retirement(
    value: dict, accepted_batches: int, accepted_entries: int, fallbacks: int = 0
) -> None:
    for status in value.values():
        status["info"]["engine"]["log_retirement"] = {
            "enabled": True,
            "max_inflight_entries": 262_144,
            "max_inflight_payload_bytes": 128 * 1024 * 1024,
            "accepted_batches": accepted_batches,
            "accepted_entries": accepted_entries,
            "inline_fallback_batches": fallbacks,
            "inflight_entries": 0,
            "inflight_payload_bytes": 0,
        }


class ReclamationServiceTests(unittest.TestCase):
    def test_zero_snapshot_index_is_valid_before_feature_activity(self):
        before = statuses(completed=0, installs=(0, 0, 0), index=0)
        after = statuses(completed=0, installs=(0, 0, 0), index=0)

        receipt = activity(before, after, 64, "durable-kv")

        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(
            receipt["failures"],
            ["no live snapshot compaction completed since the before status"],
        )
        self.assertTrue(all(
            node["current_snapshot_index"] == 0
            for node in receipt["nodes"].values()
        ))

    def test_verifier_keeps_missing_activity_out_of_evidence_integrity(self):
        before = statuses(completed=0, installs=(0, 0, 0), index=0)
        after = statuses(completed=0, installs=(0, 0, 0), index=0)
        receipt = activity(before, after, 64, "durable-kv")
        manifest = {
            "scenario": "durable-kv",
            "options": {"snapshot_interval_entries": 64},
        }

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "before.json").write_text(json.dumps(before))
            (root / "snapshot-after-scenario.json").write_text(json.dumps(after))
            (root / "snapshot-compaction-activity.json").write_text(
                json.dumps(receipt)
            )

            self.assertEqual(snapshot_reclamation_errors(root, manifest), [])

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
        for status in before.values():
            buckets = [0] * 64
            buckets[6] = 1
            status["info"]["snapshot_compaction"]["buckets_log2"] = buckets
        for status in after.values():
            buckets = [0] * 64
            buckets[6] = 1
            buckets[7] = 1
            status["info"]["snapshot_compaction"]["buckets_log2"] = buckets
        add_native_snapshot_metrics(before, {6: 1})
        add_native_snapshot_metrics(after, {6: 1, 7: 1})
        add_application_checkpoint_metrics(before, {6: 1})
        add_application_checkpoint_metrics(after, {6: 1, 7: 1})
        add_log_retirement(before, 1, 64)
        add_log_retirement(after, 2, 128)
        manifest = {
            "scenario": "snapshot-catchup",
            "options": {"snapshot_interval_entries": 64},
            "native_snapshot_stage_observation": {"schema": 1, "required": True},
            "application_checkpoint_stage_observation": {
                "schema": 1,
                "required": True,
            },
            "retired_log_worker_observation": {"schema": 1, "required": True},
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
                native_snapshot_stages=True,
                require_native_snapshot_stage_activity=True,
                application_checkpoint_stages=True,
                require_application_checkpoint_stage_activity=True,
                retired_log_worker=True,
                require_retired_log_worker_activity=True,
            )
            (root / "snapshot-compaction-activity.json").write_text(json.dumps(receipt))
            self.assertEqual(snapshot_reclamation_errors(root, manifest), [])

            receipt["totals"]["compactions_since_before_status"] += 1
            (root / "snapshot-compaction-activity.json").write_text(json.dumps(receipt))
            self.assertEqual(
                snapshot_reclamation_errors(root, manifest),
                ["live snapshot/reclamation receipt differs from runtime status"],
            )

    def test_retired_log_worker_activity_is_bounded_and_has_no_inline_fallback(self):
        before = statuses(completed=1, installs=(0, 0, 0), index=64)
        after = statuses(completed=3, installs=(0, 0, 0), index=128)
        add_log_retirement(before, 1, 64)
        add_log_retirement(after, 3, 192)

        receipt = activity(
            before,
            after,
            64,
            "durable-kv",
            retired_log_worker=True,
            require_retired_log_worker_activity=True,
        )

        self.assertEqual(receipt["status"], "passed")
        self.assertEqual(
            receipt["totals"]["log_retirement"],
            {
                "accepted_batches": 6,
                "accepted_entries": 384,
                "inline_fallback_batches": 0,
            },
        )

        add_log_retirement(after, 3, 192, fallbacks=1)
        failed = activity(
            before,
            after,
            64,
            "durable-kv",
            retired_log_worker=True,
            require_retired_log_worker_activity=True,
        )
        self.assertEqual(failed["status"], "failed")
        self.assertIn(
            "retired log prefixes fell back to owner-thread destruction",
            failed["failures"],
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

    def test_verifier_preserves_old_diagnostic_contract_but_enforces_new_declaration(self):
        before = statuses(completed=1, installs=(0, 0, 0), index=64)
        after = statuses(completed=2, installs=(0, 0, 0), index=128)
        for value, bucket_counts in ((before, {6: 1}), (after, {6: 1, 7: 1})):
            for status in value.values():
                buckets = [0] * 64
                for bucket, count in bucket_counts.items():
                    buckets[bucket] = count
                status["info"]["snapshot_compaction"]["buckets_log2"] = buckets
        manifest = {
            "scenario": "durable-kv",
            "options": {"snapshot_interval_entries": 64, "diagnostics": True},
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "before.json").write_text(json.dumps(before))
            (root / "snapshot-after-scenario.json").write_text(json.dumps(after))
            receipt = activity(before, after, 64, "durable-kv")
            (root / "snapshot-compaction-activity.json").write_text(json.dumps(receipt))

            self.assertEqual(snapshot_reclamation_errors(root, manifest), [])

            manifest["native_snapshot_stage_observation"] = {
                "schema": 1,
                "required": True,
            }
            failures = snapshot_reclamation_errors(root, manifest)
            self.assertIn(
                "live snapshot/reclamation receipt differs from runtime status", failures
            )
            self.assertNotIn("live snapshot/reclamation activity did not pass", failures)

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
        add_application_checkpoint_metrics(before, {6: 1})
        add_application_checkpoint_metrics(after, {6: 1, 7: 2})
        add_application_snapshot_encode_metrics(before, {6: 1})
        add_application_snapshot_encode_metrics(after, {6: 1, 7: 2})

        receipt = activity(
            before,
            after,
            64,
            "durable-kv",
            native_snapshot_stages=True,
            require_native_snapshot_stage_activity=True,
            application_checkpoint_stages=True,
            require_application_checkpoint_stage_activity=True,
        )

        self.assertEqual(receipt["schema"], 6)
        publication = receipt["totals"]["native_snapshot_stages"][
            "snapshot_publication"
        ]
        self.assertEqual(publication["samples"], 6)
        self.assertEqual(publication["total_ns"], 600)
        self.assertEqual(publication["mean_ns"], 100.0)
        self.assertEqual(publication["max_upper_bound_ns"], 255)
        self.assertEqual(publication["buckets_ns_log2"], [0] * 7 + [6] + [0] * 56)
        kernel_commit = receipt["totals"]["native_snapshot_stages"][
            "snapshot_kernel_commit"
        ]
        self.assertEqual(kernel_commit["samples"], 6)
        application_sync = receipt["totals"]["application_checkpoint_stages"][
            "journal_checkpoint_sync_ns"
        ]
        self.assertEqual(application_sync["samples"], 6)
        self.assertEqual(application_sync["total_ns"], 600)
        self.assertEqual(application_sync["mean_ns"], 100.0)
        self.assertEqual(application_sync["max_upper_bound_ns"], 255)
        application_encode = receipt["totals"]["application_snapshot_encode"]
        self.assertEqual(application_encode["samples"], 6)
        self.assertEqual(application_encode["total_ns"], 600)
        self.assertEqual(application_encode["mean_ns"], 100.0)
        self.assertEqual(application_encode["max_upper_bound_ns"], 255)

    def test_schema_seven_observes_zero_wal_reclaims_without_requiring_one(self):
        before = statuses(completed=1, installs=(0, 0, 0), index=64)
        after = statuses(completed=2, installs=(0, 0, 0), index=128)
        for status in before.values():
            buckets = [0] * 64
            buckets[6] = 1
            status["info"]["snapshot_compaction"]["buckets_log2"] = buckets
        for status in after.values():
            buckets = [0] * 64
            buckets[6] = 1
            buckets[7] = 1
            status["info"]["snapshot_compaction"]["buckets_log2"] = buckets
        add_native_snapshot_metrics(before, {6: 1})
        add_native_snapshot_metrics(after, {6: 1, 7: 1})
        add_wal_reclamation_metrics(before, {})
        add_wal_reclamation_metrics(after, {})
        add_application_checkpoint_metrics(before, {6: 1})
        add_application_checkpoint_metrics(after, {6: 1, 7: 1})
        add_application_snapshot_encode_metrics(before, {6: 1})
        add_application_snapshot_encode_metrics(after, {6: 1, 7: 1})

        receipt = activity(
            before,
            after,
            64,
            "durable-kv",
            native_snapshot_stages=True,
            require_native_snapshot_stage_activity=True,
            wal_reclamation_stages=True,
            application_checkpoint_stages=True,
            require_application_checkpoint_stage_activity=True,
        )

        self.assertEqual(receipt["schema"], 7)
        self.assertEqual(receipt["status"], "passed")
        self.assertTrue(
            receipt["requirements"]["wal_reclamation_stage_observation"]
        )
        self.assertEqual(
            receipt["totals"]["native_snapshot_stages"]["wal_reclamation"][
                "samples"
            ],
            0,
        )

        manifest = {
            "scenario": "durable-kv",
            "options": {"snapshot_interval_entries": 64},
            "native_snapshot_stage_observation": {"schema": 2, "required": True},
            "application_checkpoint_stage_observation": {
                "schema": 1,
                "required": True,
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "before.json").write_text(json.dumps(before))
            (root / "snapshot-after-scenario.json").write_text(json.dumps(after))
            (root / "snapshot-compaction-activity.json").write_text(
                json.dumps(receipt)
            )
            self.assertEqual(snapshot_reclamation_errors(root, manifest), [])

    def test_native_snapshot_stage_availability_cannot_change_mid_case(self):
        before = statuses(completed=1, installs=(0, 0, 0), index=64)
        after = statuses(completed=2, installs=(0, 0, 0), index=128)
        add_native_snapshot_metrics(after, {7: 1})

        receipt = activity(
            before,
            after,
            64,
            "durable-kv",
            native_snapshot_stages=True,
            require_native_snapshot_stage_activity=True,
        )

        self.assertEqual(receipt["status"], "failed")
        self.assertTrue(any(
            "native snapshot stage availability changed" in failure
            for failure in receipt["failures"]
        ))

    def test_application_snapshot_encode_count_must_match_compactions(self):
        before = statuses(completed=1, installs=(0, 0, 0), index=64)
        after = statuses(completed=2, installs=(0, 0, 0), index=128)
        add_application_snapshot_encode_metrics(before, {6: 1})
        add_application_snapshot_encode_metrics(after, {6: 1, 7: 2})

        receipt = activity(before, after, 64, "durable-kv")

        self.assertEqual(receipt["status"], "failed")
        self.assertTrue(any(
            "application snapshot encode count differs from completed compactions" in failure
            for failure in receipt["failures"]
        ))

    def test_diagnostic_native_snapshot_stages_must_execute(self):
        before = statuses(completed=1, installs=(0, 0, 0), index=64)
        after = statuses(completed=2, installs=(0, 0, 0), index=128)
        add_native_snapshot_metrics(before, {})
        add_native_snapshot_metrics(after, {})

        receipt = activity(
            before,
            after,
            64,
            "durable-kv",
            native_snapshot_stages=True,
            require_native_snapshot_stage_activity=True,
        )

        self.assertEqual(receipt["status"], "failed")
        self.assertIn(
            "no native snapshot stage activity observed: "
            + ", ".join(NATIVE_SNAPSHOT_STAGES),
            receipt["failures"],
        )


if __name__ == "__main__":
    unittest.main()

import copy
import unittest

from benchctl.reclamation_load import assess, markdown
from benchctl.reclamation_service import (
    APPLICATION_CHECKPOINT_STAGES,
    NATIVE_SNAPSHOT_BASE_STAGES,
    NATIVE_SNAPSHOT_STAGES,
)


def footprint(
    raft_bytes: int,
    application_bytes: int,
    *,
    candidate: bool,
    legacy_wal_bytes: int = 0,
) -> dict:
    categories = {
        "raft_wal_data": {"allocated_bytes": raft_bytes},
        "raft_wal_metadata": {"allocated_bytes": 10},
        "raft_snapshot_data": {
            "allocated_bytes": 20 if candidate else 0,
            "files": 3 if candidate else 0,
        },
        "raft_snapshot_metadata": {
            "allocated_bytes": 10 if candidate else 0,
            "files": 3 if candidate else 0,
        },
        "raft_snapshot_temporary": {"allocated_bytes": 0},
        "application_journal": {"allocated_bytes": application_bytes},
    }
    nodes = {
        str(node): {
            "files": ([{
                "path": f"node-{node}/application.wal",
                "category": "application_journal",
                "allocated_bytes": application_bytes // 3,
            }] + ([{
                "path": f"node-{node}/raft/hard-state",
                "category": "other",
                "allocated_bytes": legacy_wal_bytes,
            }] if legacy_wal_bytes else [])),
            "totals": {
                "raft_snapshot_data": {"files": 1 if candidate else 0},
                "raft_snapshot_metadata": {"files": 1 if candidate else 0},
                "raft_snapshot_temporary": {"files": 0},
            }
        }
        for node in (1, 2, 3)
    }
    return {
        "snapshots": {
            "after_measurement": {
                "totals": categories,
                "nodes": copy.deepcopy(nodes),
            },
            "after_final_restart": {
                "totals": copy.deepcopy(categories),
                "nodes": nodes,
            },
        }
    }


def case(variant: str, rate: int, repetition: int, *, candidate: bool) -> dict:
    throughput = (9700 if candidate else 10000) if rate == 0 else (2995 if candidate else 3000)
    return {
        "smoke": False,
        "measurement_mode": "timing",
        "qualification": "passed",
        "configuration": {"variant": variant, "hard_state": "wal"},
        "workload": {"offered_per_second": rate},
        "repetition": repetition,
        "metrics": {
            "throughput_ops_s": throughput,
            "client_p99_ms": 5.5 if candidate else 5.0,
            "client_p999_ms": 11.0 if candidate else 10.0,
        },
        "accounting": {"errors": 0, "unknown": 0, "not_issued": 0},
        "storage_footprint": footprint(
            100 if candidate else 1000, 200 if candidate else 2000, candidate=candidate
        ),
        "recovery": {"timing": {"process_restart_ns": 250_000_000}},
        "snapshot_reclamation": ({
            "nodes": {
                str(node): {
                    "measurement_compaction": {
                        "samples": 1,
                        "max_upper_bound_ns": 1_048_575,
                    }
                }
                for node in (1, 2, 3)
            }
        } if candidate else None),
    }


def suite() -> dict:
    cases = []
    for rate in (3000, 0):
        for repetition in (1, 2, 3):
            cases.append(case("prior", rate, repetition, candidate=False))
            cases.append(case("snap10k", rate, repetition, candidate=True))
    return {
        "qualification": "passed",
        "qualification_errors": [],
        "suite": {
            "kind": "reclamation-under-load",
            "arms": [
                ["prior", "rafter", 32, "prior", 1, False, 8, 0],
                ["snap10k", "rafter", 32, "candidate", 1, False, 8, 10000],
            ],
            "rates": [3000, 0],
            "runs": 3,
        },
        "cases": cases,
    }


class ReclamationLoadTests(unittest.TestCase):
    def test_passing_assessment_preserves_same_seed_service_and_storage_evidence(self):
        value = assess(suite())
        self.assertEqual(value["status"], "passed")
        self.assertEqual(len(value["comparisons"]), 2)
        saturated = next(
            row for row in value["comparisons"] if row["offered_per_second"] == 0
        )
        self.assertAlmostEqual(saturated["median_throughput_ratio"], 0.97)
        self.assertEqual(saturated["measured_compactions"], 9)
        self.assertNotIn("native_snapshot_stages", saturated)
        self.assertIn("atomic application-journal checkpointing", value["scope"])
        self.assertIn("Verdict: passed", markdown(value))

    def test_native_snapshot_stage_maxima_are_imported_and_rendered(self):
        data = suite()
        for rate in (3000, 0):
            item = case("snap10k", rate, 1, candidate=True)
            item["measurement_mode"] = "diagnostic"
            item["snapshot_reclamation"]["totals"] = {
                "native_snapshot_stages": {
                    stage: {
                        "samples": 1,
                        "total_ns": 100,
                        "max_upper_bound_ns": 127,
                    }
                    for stage in NATIVE_SNAPSHOT_STAGES
                },
                "application_checkpoint_stages": {
                    stage: {
                        "samples": 1,
                        "total_ns": 200,
                        "max_upper_bound_ns": 255,
                    }
                    for stage in APPLICATION_CHECKPOINT_STAGES
                },
            }
            item["snapshot_reclamation"]["schema"] = 5
            data["cases"].append(item)

        value = assess(data)

        self.assertEqual(value["status"], "passed")
        saturated = next(
            row for row in value["comparisons"] if row["offered_per_second"] == 0
        )
        publication = saturated["native_snapshot_stages"]["snapshot_publication"]
        self.assertEqual(publication["samples"], 1)
        self.assertEqual(publication["total_ns"], 100)
        self.assertEqual(publication["max_upper_bound_ns"], 127)
        self.assertIn("Native snapshot stage maxima", markdown(value))
        kernel_commit = saturated["native_snapshot_stages"][
            "snapshot_kernel_commit"
        ]
        self.assertEqual(kernel_commit["samples"], 1)
        application_sync = saturated["application_checkpoint_stages"][
            "journal_checkpoint_sync_ns"
        ]
        self.assertEqual(application_sync["samples"], 1)
        self.assertEqual(application_sync["max_upper_bound_ns"], 255)
        self.assertIn("Application checkpoint stage maxima", markdown(value))

    def test_schema_four_native_stages_remain_replayable(self):
        data = suite()
        for rate in (3000, 0):
            item = case("snap10k", rate, 1, candidate=True)
            item["measurement_mode"] = "diagnostic"
            item["snapshot_reclamation"]["schema"] = 4
            item["snapshot_reclamation"]["totals"] = {
                "native_snapshot_stages": {
                    stage: {
                        "samples": 1,
                        "total_ns": 100,
                        "max_upper_bound_ns": 127,
                    }
                    for stage in NATIVE_SNAPSHOT_BASE_STAGES
                },
                "application_checkpoint_stages": {
                    stage: {
                        "samples": 1,
                        "total_ns": 200,
                        "max_upper_bound_ns": 255,
                    }
                    for stage in APPLICATION_CHECKPOINT_STAGES
                },
            }
            data["cases"].append(item)

        value = assess(data)

        self.assertEqual(value["status"], "passed")
        saturated = next(
            row for row in value["comparisons"] if row["offered_per_second"] == 0
        )
        self.assertNotIn(
            "snapshot_kernel_commit", saturated["native_snapshot_stages"]
        )
        self.assertIn("not observed", markdown(value))

    def test_tail_regression_and_missing_compaction_fail_separate_verdicts(self):
        data = copy.deepcopy(suite())
        candidate = next(
            item
            for item in data["cases"]
            if item["configuration"]["variant"] == "snap10k"
            and item["workload"]["offered_per_second"] == 3000
            and item["repetition"] == 1
        )
        candidate["metrics"]["client_p99_ms"] = 7.0
        candidate["snapshot_reclamation"]["nodes"] = {}
        value = assess(data)
        self.assertEqual(value["status"], "failed")
        self.assertEqual(value["verdicts"]["service_objective"]["status"], "failed")
        self.assertEqual(value["verdicts"]["reclamation_activity"]["status"], "failed")

    def test_application_growth_fails_physical_reclamation(self):
        data = suite()
        for item in data["cases"]:
            if item["configuration"]["variant"] == "snap10k":
                item["storage_footprint"]["snapshots"]["after_measurement"]["totals"][
                    "application_journal"
                ]["allocated_bytes"] = 10_000_000
        value = assess(data)
        self.assertEqual(value["verdicts"]["physical_reclamation"]["status"], "failed")
        self.assertTrue(all(
            row["median_candidate_application_journal_allocated_bytes"] == 10_000_000
            for row in value["comparisons"]
        ))
        self.assertTrue(any(
            "application-journal bytes" in error
            for error in value["verdicts"]["physical_reclamation"]["errors"]
        ))

    def test_initial_wal_path_is_included_in_no_reclamation_control(self):
        data = suite()
        for item in data["cases"]:
            if item["configuration"]["variant"] == "prior":
                item["storage_footprint"] = footprint(
                    0, 2000, candidate=False, legacy_wal_bytes=1000
                )
        value = assess(data)
        self.assertEqual(value["verdicts"]["physical_reclamation"]["status"], "passed")
        self.assertTrue(all(
            row["median_control_managed_raft_allocated_bytes"] == 3010
            for row in value["comparisons"]
        ))

    def test_failed_service_pair_retains_observed_metrics(self):
        data = suite()
        candidate = next(
            item
            for item in data["cases"]
            if item["configuration"]["variant"] == "snap10k"
            and item["workload"]["offered_per_second"] == 3000
            and item["repetition"] == 1
        )
        candidate["accounting"]["not_issued"] = 1
        value = assess(data)
        fixed = next(
            row for row in value["comparisons"] if row["offered_per_second"] == 3000
        )
        self.assertEqual(value["verdicts"]["service_objective"]["status"], "failed")
        self.assertAlmostEqual(fixed["repetitions"][0]["throughput_ratio"], 2995 / 3000)
        self.assertEqual(fixed["repetitions"][0]["p99_regression_ms"], 0.5)
        self.assertTrue(any(
            "snap10k had 1 unsent requests" in error
            for error in value["verdicts"]["service_objective"]["errors"]
        ))
        self.assertFalse(any(
            "unknown outcomes" in error
            for error in value["verdicts"]["service_objective"]["errors"]
        ))

    def test_accumulated_snapshot_envelopes_fail_physical_reclamation(self):
        data = suite()
        candidate = next(
            item
            for item in data["cases"]
            if item["configuration"]["variant"] == "snap10k"
        )
        candidate["storage_footprint"]["snapshots"]["after_final_restart"]["nodes"][
            "1"
        ]["totals"]["raft_snapshot_data"]["files"] = 2
        value = assess(data)
        self.assertEqual(value["verdicts"]["physical_reclamation"]["status"], "failed")
        self.assertTrue(any(
            "2 snapshot data files on node 1" in error
            for error in value["verdicts"]["physical_reclamation"]["errors"]
        ))

    def test_temporary_snapshot_after_final_barrier_fails_physical_reclamation(self):
        data = suite()
        candidate = next(
            item
            for item in data["cases"]
            if item["configuration"]["variant"] == "snap10k"
        )
        candidate["storage_footprint"]["snapshots"]["after_final_restart"]["nodes"][
            "2"
        ]["totals"]["raft_snapshot_temporary"]["files"] = 1
        value = assess(data)
        self.assertEqual(value["verdicts"]["physical_reclamation"]["status"], "failed")
        self.assertTrue(any(
            "1 temporary snapshot files on node 2" in error
            for error in value["verdicts"]["physical_reclamation"]["errors"]
        ))

    def test_temporary_application_checkpoint_after_restart_fails_reclamation(self):
        data = suite()
        candidate = next(
            item
            for item in data["cases"]
            if item["configuration"]["variant"] == "snap10k"
        )
        candidate["storage_footprint"]["snapshots"]["after_final_restart"]["nodes"][
            "2"
        ]["files"].append({
            "path": "node-2/application.wal.checkpoint.tmp",
            "category": "other",
            "allocated_bytes": 4096,
        })
        value = assess(data)
        self.assertEqual(value["verdicts"]["physical_reclamation"]["status"], "failed")
        self.assertTrue(any(
            "application checkpoint temporary files on node 2" in error
            for error in value["verdicts"]["physical_reclamation"]["errors"]
        ))

    def test_post_restart_managed_growth_cannot_hide_behind_post_load_reclamation(self):
        data = suite()
        candidate = next(
            item
            for item in data["cases"]
            if item["configuration"]["variant"] == "snap10k"
        )
        candidate["storage_footprint"]["snapshots"]["after_final_restart"]["totals"][
            "raft_wal_data"
        ]["allocated_bytes"] = 2_000
        value = assess(data)
        self.assertEqual(value["verdicts"]["physical_reclamation"]["status"], "failed")
        self.assertTrue(any(
            "managed Raft bytes after final restart" in error
            for error in value["verdicts"]["physical_reclamation"]["errors"]
        ))


if __name__ == "__main__":
    unittest.main()

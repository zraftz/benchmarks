from pathlib import Path
from types import SimpleNamespace
import json
import tempfile
import unittest
from unittest.mock import patch
from benchctl.paired import (archive_build, candidate_arms, combined_activation_failures,
                             completion_priority_activation_failures, openraft_arms,
                             ordered_arms, mode_flags, prepare_builds_and_smokes, qualify_machine,
                             worker_smoke_scenarios)
from benchctl.evidence import digest
from benchctl.feature_coverage import verdict as feature_coverage_verdict
from benchctl.results import _execution_plan_errors, _expected_durable_cases
from benchctl.suite_plan import execution_plan, load_window_plan


class PairedTests(unittest.TestCase):
    def test_hosted_paired_run_binds_exact_workflow_sha(self):
        workflow = (
            Path(__file__).parents[1] / ".github/workflows/baseline.yml"
        ).read_text()
        self.assertIn('--benchmark-sha "$GITHUB_SHA"', workflow)

    def test_hosted_reclamation_run_is_same_code_and_bound_to_workflow_sha(self):
        workflow = (
            Path(__file__).parents[1] / ".github/workflows/baseline.yml"
        ).read_text()
        self.assertIn("group: baseline-benchmark", workflow)
        self.assertIn('--benchmark-sha "$GITHUB_SHA"', workflow)
        self.assertIn("--suite-kind reclamation-under-load", workflow)
        self.assertIn("--candidate-snapshot-interval-entries", workflow)
        self.assertIn('PRIOR_REF="$CANDIDATE_REF"', workflow)
        self.assertIn("OPENRAFT_CONTROLS=none", workflow)
        self.assertIn("inputs.suite == 'microbench'", workflow)
        self.assertNotIn("--qualify-machine", workflow)

    def test_hosted_layered_report_is_published_to_the_run_summary(self):
        workflow = (
            Path(__file__).parents[1] / ".github/workflows/baseline.yml"
        ).read_text()
        self.assertIn(
            'cat results/summary/summary.md >> "$GITHUB_STEP_SUMMARY"', workflow
        )

    def test_build_preparation_restores_selection_after_failure(self):
        args = SimpleNamespace(
            prior="a" * 40,
            candidate="b" * 40,
            prior_hard_state="wal",
            candidate_hard_state="wal",
            prior_peer_batch_size=32,
            prior_mode="pipeline",
            candidate_mode="inline",
            candidate_combine_peer_proposals=False,
            candidate_durable_completion_priority=False,
        )
        captured = {"selection": b"original"}
        with tempfile.TemporaryDirectory() as temp, \
                patch("benchctl.paired.capture_rafter_selection", return_value=captured), \
                patch("benchctl.paired.restore_rafter_selection") as restore, \
                patch("benchctl.paired.select_rafter", side_effect=RuntimeError("fetch failed")):
            root = Path(temp)
            with self.assertRaisesRegex(RuntimeError, "fetch failed"):
                prepare_builds_and_smokes(
                    args, root / "output", root / "work", root / "data", [1], [8]
                )
        restore.assert_called_once_with(captured)

    def test_build_preparation_returns_archives_then_restores_selection(self):
        args = SimpleNamespace(
            prior="a" * 40,
            candidate="b" * 40,
            prior_hard_state="journal",
            candidate_hard_state="wal",
            prior_peer_batch_size=1,
            prior_mode="inline",
            candidate_mode="inline",
            candidate_combine_peer_proposals=False,
            candidate_durable_completion_priority=False,
        )
        captured = {"selection": b"original"}
        prior = {"source_digest": "prior"}
        candidate = {"source_digest": "candidate"}
        with tempfile.TemporaryDirectory() as temp, \
                patch("benchctl.paired.capture_rafter_selection", return_value=captured), \
                patch("benchctl.paired.restore_rafter_selection") as restore, \
                patch("benchctl.paired.select_rafter") as select, \
                patch("benchctl.paired.build") as build, \
                patch("benchctl.paired.archive_build", side_effect=[prior, candidate]):
            root = Path(temp)
            output = root / "output"
            output.mkdir()
            self.assertEqual(
                prepare_builds_and_smokes(
                    args, output, root / "work", root / "data", [1], [8]
                ),
                (prior, candidate),
            )
        self.assertEqual([call.args[0] for call in select.call_args_list], [args.prior, args.candidate])
        self.assertEqual(build.call_count, 2)
        restore.assert_called_once_with(captured)

    def test_same_exact_source_and_features_archive_one_verified_build_twice(self):
        sha = "a" * 40
        args = SimpleNamespace(
            prior=sha,
            candidate=sha,
            prior_hard_state="wal",
            candidate_hard_state="wal",
            prior_peer_batch_size=32,
            prior_mode="inline",
            candidate_mode="inline",
            candidate_combine_peer_proposals=False,
            candidate_durable_completion_priority=False,
        )
        receipt = {"source_digest": "shared"}
        with tempfile.TemporaryDirectory() as temp, \
                patch("benchctl.paired.capture_rafter_selection", return_value={}), \
                patch("benchctl.paired.restore_rafter_selection"), \
                patch("benchctl.paired.select_rafter") as select, \
                patch("benchctl.paired.build") as build, \
                patch("benchctl.paired.archive_build", return_value=receipt) as archive:
            root = Path(temp)
            output = root / "output"
            output.mkdir()
            self.assertEqual(
                prepare_builds_and_smokes(
                    args, output, root / "work", root / "data", [1], [8]
                ),
                (receipt, receipt),
            )
        select.assert_called_once_with(sha)
        build.assert_called_once()
        self.assertEqual(
            [call.args[0].name for call in archive.call_args_list],
            ["prior", "candidate"],
        )

    def test_only_the_wal_pipeline_adds_snapshot_catchup_smoke(self):
        base = ["durable-kv", "leader-loss", "follower-catchup"]
        self.assertEqual(worker_smoke_scenarios("messages", "wal"), base)
        self.assertEqual(worker_smoke_scenarios("pipeline", "journal"), base)
        self.assertEqual(
            worker_smoke_scenarios("pipeline", "wal"),
            [*base, "snapshot-catchup"],
        )

    def test_order_is_balanced_and_each_arm_runs_once(self):
        arms = list("abcdef")
        orders = [ordered_arms(arms, repeat) for repeat in range(len(arms))]
        self.assertEqual(orders[0], list("abcdef"))
        self.assertEqual(orders[1], list("bcdefa"))
        self.assertTrue(all(sorted(order) == arms for order in orders))
        for arm in arms:
            self.assertEqual(sorted(order.index(arm) for order in orders), list(range(len(arms))))

    def test_execution_plan_binds_full_position_balanced_case_options(self):
        arms = [
            ["prior", "rafter", 32, "prior", 1, False, 8],
            ["openraft", "openraft", 1, "candidate", 1, False, 8],
            ["candidate", "rafter", 32, "candidate", 4, False, 8],
        ]
        suite = {
            "arms": arms,
            "rates": [1000],
            "runs": 3,
            "network_delays_ms": [0],
            "diagnostic_arms": ["candidate"],
            "diagnostic_rates": [1000],
            "prior_mode": "pipeline",
            "candidate_mode": "pipeline",
            "prior_durable_completion_priority": False,
            "candidate_durable_completion_priority": True,
        }
        plan = execution_plan(suite)
        timing = [item for item in plan if item["measurement_mode"] == "timing"]
        self.assertEqual(len(timing), 9)
        for arm in ("prior", "openraft", "candidate"):
            positions = [item["arm_position"] for item in timing if item["arm"] == arm]
            self.assertEqual(sorted(positions), [1, 2, 3])
        candidate = next(item for item in timing if item["arm"] == "candidate")
        self.assertEqual(candidate["options"]["rate"], 1000)
        self.assertEqual(candidate["options"]["max_speculative_proposals"], 4)
        self.assertTrue(candidate["options"]["durable_completion_priority"])
        self.assertEqual(plan[-1]["measurement_mode"], "diagnostic")
        self.assertEqual(plan[-1]["options"]["seed"], 1)
        self.assertEqual(plan[-1]["options"]["snapshot_interval_entries"], 0)

    def test_snapshot_aware_plan_binds_each_interval_without_pooling(self):
        suite = {
            "schema": 4,
            "arms": [
                ["prior", "rafter", 32, "prior", 1, False, 8, 0],
                ["snap10k", "rafter", 32, "candidate", 1, False, 8, 10000],
                ["snap100k", "rafter", 32, "candidate", 1, False, 8, 100000],
            ],
            "rates": [3000],
            "runs": 3,
            "network_delays_ms": [0],
            "diagnostic_arms": ["prior", "snap10k", "snap100k"],
            "diagnostic_rates": [3000],
            "prior_mode": "pipeline",
            "candidate_mode": "pipeline",
            "prior_durable_completion_priority": True,
            "candidate_durable_completion_priority": True,
        }
        plan = execution_plan(suite)
        timing = [item for item in plan if item["measurement_mode"] == "timing"]
        self.assertEqual(len(timing), 9)
        self.assertEqual(
            {item["arm"]: item["options"]["snapshot_interval_entries"] for item in timing},
            {"prior": 0, "snap10k": 10000, "snap100k": 100000},
        )

    def test_execution_plan_replay_rejects_case_or_recorded_plan_drift(self):
        suite = {
            "schema": 3,
            "arms": [["candidate", "rafter", 32, "candidate", 1, False, 8]],
            "rates": [1000],
            "runs": 3,
            "network_delays_ms": [0],
            "diagnostic_arms": ["candidate"],
            "diagnostic_rates": [1000],
            "prior_mode": "pipeline",
            "candidate_mode": "pipeline",
            "prior_durable_completion_priority": False,
            "candidate_durable_completion_priority": True,
        }
        suite["execution_plan"] = execution_plan(suite)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for item in suite["execution_plan"]:
                case = root / item["name"]
                case.mkdir()
                (case / "manifest.json").write_text(json.dumps({
                    "implementation": item["implementation"],
                    "scenario": "durable-kv",
                    "smoke": False,
                    "options": item["options"],
                }))
            self.assertEqual(_execution_plan_errors(root, suite), [])

            changed = suite["execution_plan"][0]
            manifest_path = root / changed["name"] / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["options"]["seed"] = 99
            manifest_path.write_text(json.dumps(manifest))
            self.assertIn(
                f"case options differ from plan: {changed['name']}",
                _execution_plan_errors(root, suite),
            )

            manifest["options"]["seed"] = changed["options"]["seed"]
            manifest_path.write_text(json.dumps(manifest))
            suite["execution_plan"][0]["arm_position"] = 99
            self.assertIn(
                "recorded execution plan differs from suite declaration",
                _execution_plan_errors(root, suite),
            )
            self.assertEqual(
                _execution_plan_errors(root, {"schema": 6}),
                ["unsupported durable suite schema"],
            )

    def test_schema_five_replays_clean_benchmark_repository_identity(self):
        commit = "a" * 40
        suite = {
            "schema": 5,
            "arms": [["candidate", "rafter", 32, "candidate", 1, False, 8]],
            "rates": [1000],
            "runs": 3,
            "network_delays_ms": [0],
            "diagnostic_arms": ["candidate"],
            "diagnostic_rates": [1000],
            "prior_mode": "pipeline",
            "candidate_mode": "pipeline",
            "prior_durable_completion_priority": False,
            "candidate_durable_completion_priority": True,
            "benchmark_repository": {"commit": commit, "status": "clean"},
        }
        suite["execution_plan"] = execution_plan(suite)
        suite["load_window_plan"] = load_window_plan(suite["execution_plan"])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for item in suite["execution_plan"]:
                case = root / item["name"]
                case.mkdir()
                (case / "manifest.json").write_text(json.dumps({
                    "implementation": item["implementation"],
                    "scenario": "durable-kv",
                    "smoke": False,
                    "options": item["options"],
                    "host": {"git": {"returncode": 0, "stdout": commit}},
                }))
            self.assertEqual(_execution_plan_errors(root, suite), [])
            suite["benchmark_repository"]["status"] = "dirty"
            self.assertIn(
                "benchmark repository identity is not an exact clean commit",
                _execution_plan_errors(root, suite),
            )
            suite["benchmark_repository"]["status"] = "clean"
            suite["load_window_plan"]["cases"] += 1
            self.assertIn(
                "recorded load-window plan differs from execution plan",
                _execution_plan_errors(root, suite),
            )
            suite["load_window_plan"] = load_window_plan(suite["execution_plan"])
            suite["benchmark_repository"] = {"commit": "b" * 40, "status": "clean"}
            self.assertTrue(any(
                error.startswith("case benchmark repository differs from suite:")
                for error in _execution_plan_errors(root, suite)
            ))

    def test_load_window_plan_counts_declared_windows_without_overhead_claim(self):
        plan = [
            {"measurement_mode": "timing", "options": {"warmup": 10, "duration": 60}},
            {"measurement_mode": "diagnostic", "options": {"warmup": 10, "duration": 60}},
        ]
        summary = load_window_plan(plan)
        self.assertEqual(summary["cases"], 2)
        self.assertEqual(summary["timing_cases"], 1)
        self.assertEqual(summary["diagnostic_cases"], 1)
        self.assertEqual(summary["declared_load_window_seconds"], 140)
        self.assertIn("excludes builds", summary["scope"])

    def test_archive_preserves_independent_receipt_and_rejects_changed_binary(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            dist = root / "dist"
            dist.mkdir()
            binary = dist / "raft-bench-rafter"
            binary.write_bytes(b"compiled prior revision")
            receipt = {"binaries": {"rafter": digest(binary)}, "implementations": {"rafter": {"rev": "a" * 40}}}
            (dist / "build.json").write_text(json.dumps(receipt))
            with patch("benchctl.paired.ROOT", root):
                self.assertEqual(archive_build(root / "prior"), receipt)
                binary.write_bytes(b"different revision")
                with self.assertRaisesRegex(RuntimeError, "differs"):
                    archive_build(root / "invalid")
            self.assertEqual((root / "prior/raft-bench-rafter").read_bytes(), b"compiled prior revision")
            self.assertEqual(json.loads((root / "prior/build.json").read_text()), receipt)

    def test_pipeline_mode_retains_worker_transport_and_control_isolation(self):
        self.assertEqual(mode_flags("pipeline"), {"ordered_apply": True, "peer_message_stream": True, "pipelined_durability": True})
        self.assertEqual(mode_flags("messages"), {"ordered_apply": True, "peer_message_stream": True, "pipelined_durability": False})
        for mode in ("inline", "worker", "messages", "pipeline"):
            self.assertFalse(any(mode_flags(mode, "openraft").values()))
        with self.assertRaisesRegex(ValueError, "unsupported"):
            mode_flags("unknown")

    def test_threshold_sweep_case_count_uses_declared_diagnostic_subset(self):
        suite = {"arms": [["prior"], ["openraft"], ["s1"], ["s2"], ["s4"], ["s8"]],
                 "rates": [0, 1000], "runs": 3, "network_delays_ms": [0, 2],
                 "diagnostic_arms": ["prior", "openraft", "s1", "s2", "s4", "s8"],
                 "diagnostic_rates": [0]}
        self.assertEqual(_expected_durable_cases(suite), 84)

    def test_openraft_controls_are_separate_named_arms(self):
        self.assertEqual(openraft_arms(["synchronous", "async"]), [
            ("openraft", "openraft", 1, "candidate", 1, False, 8),
            ("openraft-async", "openraft", 1, "candidate", 1, False, 8),
        ])
        with self.assertRaisesRegex(ValueError, "distinct OpenRaft controls"):
            openraft_arms(["async", "async"])

    def test_replication_window_sweep_has_distinct_named_arms(self):
        self.assertEqual(candidate_arms([32], [4], [8, 16, 32], True), [
            ("candidate-b32-s4-combined-w8", "rafter", 32, "candidate", 4, True, 8),
            ("candidate-b32-s4-combined-w16", "rafter", 32, "candidate", 4, True, 16),
            ("candidate-b32-s4-combined-w32", "rafter", 32, "candidate", 4, True, 32),
        ])
        self.assertEqual(candidate_arms([32], [1], [8], False, True), [
            ("candidate-b32-commit-first", "rafter", 32, "candidate", 1, False, 8),
        ])

    def test_combined_activity_is_required_across_suite_not_every_case(self):
        arms = [("candidate-combined", "rafter", 32, "candidate", 4, True, 8)]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            quiet = root / "001-candidate-combined-r1-q100-timing"
            busy = root / "002-candidate-combined-r1-q0-timing"
            for case, batches in ((quiet, 0), (busy, 7)):
                case.mkdir()
                (case / "manifest.json").write_text(json.dumps({
                    "options": {"variant": "candidate-combined"},
                }))
                (case / "outcome.json").write_text(json.dumps({"status": "completed"}))
                (case / "pipeline-activity.json").write_text(json.dumps({
                    "combined_peer_proposal_batches_by_node": {"1": batches, "2": 0, "3": 0},
                }))
            self.assertEqual(combined_activation_failures(root, arms), [])
            (busy / "failure.json").write_text(json.dumps({"status": "failed"}))
            self.assertEqual(combined_activation_failures(root, arms), [{
                "case": "suite:candidate-combined",
                "error": "selected combined peer/proposal mode executed no combined steps in any completed case",
            }])

    def test_completion_priority_is_required_across_suite_not_every_case(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for ordinal, observed in ((1, False), (2, True)):
                case = root / f"00{ordinal}-candidate-commit-first-r1-q0-timing"
                case.mkdir()
                (case / "manifest.json").write_text(json.dumps({
                    "options": {"variant": "candidate-commit-first"},
                }))
                (case / "outcome.json").write_text(json.dumps({"status": "completed"}))
                (case / "completion-priority-activity.json").write_text(json.dumps({
                    "observed_during_load": observed,
                }))
            variants = {"candidate-commit-first"}
            self.assertEqual(completion_priority_activation_failures(root, variants), [])
            (root / "002-candidate-commit-first-r1-q0-timing/failure.json").write_text(
                json.dumps({"status": "failed"})
            )
            self.assertEqual(completion_priority_activation_failures(root, variants), [{
                "case": "suite:candidate-commit-first",
                "error": "durable completion-priority activity was not observed in any completed case",
            }])

    def test_current_completion_priority_requires_bounded_lookahead_activity(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case = root / "001-candidate-commit-first-r1-q0-timing"
            case.mkdir()
            (case / "manifest.json").write_text(json.dumps({
                "options": {"variant": "candidate-commit-first"},
            }))
            (case / "outcome.json").write_text(json.dumps({"status": "completed"}))
            receipt = {
                "schema": 2,
                "observed_during_load": True,
                "observed_bounded_lookahead_during_load": False,
            }
            (case / "completion-priority-activity.json").write_text(json.dumps(receipt))
            variants = {"candidate-commit-first"}
            self.assertEqual(completion_priority_activation_failures(root, variants), [{
                "case": "suite:candidate-commit-first",
                "error": "bounded completion-priority lookahead was not observed in any completed case",
            }])
            coverage = feature_coverage_verdict(
                root, completion_priority=variants, combined=set()
            )
            self.assertEqual(coverage["status"], "incomplete")
            self.assertEqual(
                coverage["checks"][0]["observations"],
                {"priority_paths": "passed", "bounded_lookahead": "not observed", "receipts": 1},
            )
            receipt["observed_bounded_lookahead_during_load"] = True
            (case / "completion-priority-activity.json").write_text(json.dumps(receipt))
            self.assertEqual(completion_priority_activation_failures(root, variants), [])

    def test_machine_qualification_binds_the_passing_profile_seal(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            profile = root / "evidence/machine-profile"
            profile.mkdir(parents=True)
            (profile / "SHA256SUMS.json").write_text("sealed profile\n")
            checked = {"status": "passed", "verdicts": {"evidence_integrity": {"status": "passed"}}}
            with patch("benchctl.machine.capture_profile", return_value={"status": "passed"}), \
                    patch("benchctl.paired.verify", return_value=checked):
                receipt = qualify_machine(root / "data", root / "evidence")
            self.assertEqual(receipt["status"], "passed")
            self.assertEqual(receipt["seal_sha256"], digest(profile / "SHA256SUMS.json"))
            self.assertEqual(
                json.loads((root / "evidence/machine-qualification.json").read_text()),
                receipt,
            )

    def test_machine_qualification_retains_and_refuses_a_failed_profile(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            profile = root / "evidence/machine-profile"
            profile.mkdir(parents=True)
            (profile / "SHA256SUMS.json").write_text("failed profile\n")
            checked = {"status": "passed", "verdicts": {"evidence_integrity": {"status": "passed"}}}
            with patch("benchctl.machine.capture_profile", return_value={"status": "failed"}), \
                    patch("benchctl.paired.verify", return_value=checked):
                with self.assertRaisesRegex(RuntimeError, "did not pass"):
                    qualify_machine(root / "data", root / "evidence")
            receipt = json.loads(
                (root / "evidence/machine-qualification.json").read_text()
            )
            self.assertEqual(receipt["status"], "failed")

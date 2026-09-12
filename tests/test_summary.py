import copy
import json
from pathlib import Path
import tempfile
import unittest

from benchctl.evidence import seal
from benchctl.results import _display_name, load_microbench, load_storage_comparison
from benchctl.summary import build_summary, render_html, render_markdown


ENVIRONMENT = {"topology": "three processes on one host", "runner": "same"}
COMPLETION = "durable-log+durable-application-v1/logged-reads"
MEMORY_COMPLETION = "proposal submitted -> reference application operation completes on leader"


def memory_record(engine, throughput, p99, *, completion=MEMORY_COMPLETION, repetitions=7):
    return {
        "layer": "consensus_in_memory",
        "engine": {"name": engine, "version": "selected"},
        "display_name": engine,
        "configuration": {"storage": "memory", "transport": "in_process"},
        "workload": {"workload": "serial", "proposals": 2000,
                     "payload_bytes": 512, "max_in_flight": 1},
        "completion_boundary": completion,
        "environment": {"topology": "three voters in one process", "machine": "same"},
        "measurement_mode": "timing",
        "metric_definitions": {"throughput_ops_s": "ops", "client_p99_us": "p99",
                               "aggregate": "median"},
        "repetitions": repetitions,
        "qualification": "passed",
        "qualification_errors": [],
        "metrics": {"throughput_ops_s": {"median": throughput},
                    "client_p99_us": {"median": p99}},
        "source_cases": [f"run-{run}-{engine}.json" for run in range(1, repetitions + 1)],
    }


def service_case(variant, engine, delay, rate, throughput, p99, *, p999=None, unsent=0):
    p999 = p99 * 1.25 if p999 is None else p999
    mode = "pipeline" if variant.startswith("candidate") else ("messages" if variant == "prior" else "public_api")
    return {
        "layer": "complete_service",
        "source_case": f"n{delay}-{variant}-q{rate}",
        "engine": {"name": engine, "version": "a" * 40 if engine == "rafter" else "0.9.24"},
        "display_name": "Rafter pipeline FIFO" if variant.startswith("candidate") else ("Rafter synchronous messages" if variant == "prior" else "OpenRaft"),
        "configuration": {"variant": variant, "mode": mode,
                          "hard_state": "wal" if engine == "rafter" else "adapter_journal",
                          "peer_batch_size": 32 if engine == "rafter" else 1, "client_batch_size": 64},
        "workload": {"scenario": "durable-kv", "offered_per_second": rate,
                     "payload_bytes": 512, "concurrency": 64, "read_percent": 0,
                     "cas_percent": 0, "duration_seconds": 60, "warmup_seconds": 10,
                     "network_delay_ms": delay},
        "completion_boundary": COMPLETION,
        "environment": ENVIRONMENT,
        "measurement_mode": "timing",
        "metric_definitions": {"throughput_ops_s": "ops", "client_p99_ms": "p99",
                               "client_p999_ms": "p99.9", "execution_p99_ms": "execution p99",
                               "execution_p999_ms": "execution p99.9", "client_start_p99_ms": "start p99",
                               "aggregate": "median"},
        "repetition": 1,
        "smoke": False,
        "qualification": "passed",
        "qualification_errors": [],
        "metrics": {"throughput_ops_s": throughput, "client_p99_ms": p99,
                    "client_p999_ms": p999, "execution_p99_ms": p99,
                    "execution_p999_ms": p999, "client_start_p99_ms": 0.1},
        "accounting": {"offered": 1000, "attempted": 1000 - unsent, "ok": 1000 - unsent,
                       "completed_in_window": 1000 - unsent,
                       "errors": 0, "unknown": 0, "not_issued": unsent},
        "recovery": {"status": "passed"},
        "history_check": {"status": "passed"},
    }


def durable_data():
    values = {
        0: {"candidate-b32": (8025, {0: 15.466, 100: 5.308, 1000: 12.714}),
            "prior": (7906, {0: 15.729, 100: 5.505, 1000: 16.384}),
            "openraft": (2116, {0: 41.943, 100: 5.374, 1000: 18.088})},
        2: {"candidate-b32": (4689, {0: 20.972, 100: 14.942, 1000: 20.972}),
            "prior": (4554, {0: 21.758, 100: 15.073, 1000: 19.137}),
            "openraft": (1666, {0: 53.477, 100: 16.384, 1000: 30.409})},
    }
    cases = []
    for delay, variants in values.items():
        for variant, (saturated, latencies) in variants.items():
            engine = "openraft" if variant == "openraft" else "rafter"
            for rate in (0, 100, 1000):
                throughput = saturated if rate == 0 else float(rate)
                unsent = 9 if delay == 2 and variant == "openraft" and rate == 0 else 0
                cases.append(service_case(variant, engine, delay, rate, throughput, latencies[rate], unsent=unsent))
    return {
        "kind": "durable_service",
        "directory": "/evidence/34609743659",
        "suite": {"runs": 1, "arms": [["prior", "rafter", 32, "prior"],
                                            ["openraft", "openraft", 1, "candidate"],
                                            ["candidate-b32", "rafter", 32, "candidate"]]},
        "qualification": "passed",
        "qualification_errors": [],
        "cases": cases,
        "smokes": [{"scenario": name, "status": "passed", "source_cases": [], "failures": []}
                   for name in ("durable-kv", "leader-loss", "follower-catchup")],
    }


def storage_record(arm, backend, batch, throughput, p99):
    return {
        "layer": "durable_replication",
        "engine": {"name": "rafter", "version": "a" * 40},
        "display_name": backend,
        "configuration": {"hard_state": backend, "arm": arm},
        "workload": {"kind": "proposal_batches", "batch_size": batch, "payload_bytes": 256, "batches": 1000},
        "completion_boundary": "durable batch completion",
        "environment": {"host": "same"},
        "measurement_mode": "timing",
        "metric_definitions": {"throughput_ops_s": "ops", "batch_completion_p99_ms": "p99",
                               "aggregate": "median"},
        "repetitions": 3,
        "qualification": "passed",
        "qualification_errors": [],
        "metrics": {"throughput_ops_s": {"median": throughput},
                    "batch_completion_p99_ms": {"median": p99}},
        "source_cases": [f"{arm}-{batch}-{run}" for run in range(3)],
    }


class SummaryTests(unittest.TestCase):
    def test_qualified_in_memory_evidence_publishes_one_fair_leaderboard(self):
        micro = {"qualification": "passed", "records": [
            memory_record("rafter", 120, 1.0),
            memory_record("raft-rs", 100, 1.5),
            memory_record("openraft", 80, 2.0),
        ]}
        section = build_summary(micro=micro)["sections"][0]
        self.assertEqual(section["status"], "measured lead")
        self.assertEqual(len(section["rows"]), 1)
        self.assertEqual(section["rows"][0]["throughput_vs_openraft"]["label"], "measured lead")
        self.assertIn("4 of 4", section["result"])
        rendered = render_markdown(build_summary(micro=micro), {})
        self.assertIn("reference application operation", rendered)
        self.assertIn("Selected identities: rafter selected; raft-rs selected; openraft selected.", rendered)
        self.assertIn("120/s", rendered)

    def test_in_memory_evidence_fails_closed_on_boundary_or_repetition_mismatch(self):
        records = [
            memory_record("rafter", 120, 1.0),
            memory_record("raft-rs", 100, 1.5),
            memory_record("openraft", 80, 2.0, completion="different"),
        ]
        section = build_summary(micro={"qualification": "passed", "records": records})["sections"][0]
        self.assertEqual(section["status"], "pending qualification")
        self.assertFalse(section["rows"])
        for record in records:
            record["completion_boundary"] = MEMORY_COMPLETION
            record["repetitions"] = 6
        section = build_summary(micro={"qualification": "passed", "records": records})["sections"][0]
        self.assertEqual(section["status"], "pending qualification")
        self.assertIn("requires at least 7", section["details"][0])

    def test_service_headline_is_one_named_variant_and_outputs_agree(self):
        storage = {"qualification": "passed", "qualification_errors": [], "sync_probes": {}, "records": [
            storage_record("baseline", "replace", 1, 354.102, 4.140),
            storage_record("candidate", "journal", 1, 571.297, 2.813),
            storage_record("baseline", "replace", 128, 37010.738, 4.794),
            storage_record("candidate", "journal", 128, 52738.518, 3.699),
        ]}
        history = copy.deepcopy(durable_data())
        history["directory"] = "/evidence/34599945967"
        history["cases"][2]["accounting"]["not_issued"] = 567
        summary = build_summary(durable=durable_data(), storage=storage, histories=[history])
        sections = {section["id"]: section for section in summary["sections"]}
        service = sections["complete-durable-service"]
        self.assertTrue(service["headline_qualified"])
        self.assertEqual(service["selected_configuration"]["name"], "Rafter pipeline FIFO")
        self.assertAlmostEqual(service["rows"][0]["throughput_ratio"], 8025 / 2116)
        self.assertAlmostEqual(service["rows"][0]["rafter_p999_at_1000_ms"], 12.714 * 1.25)
        self.assertEqual(service["accounting"]["rafter"], {"errors": 0, "unknown": 0, "not_issued": 0})
        self.assertEqual(service["accounting"]["openraft"]["not_issued"], 9)
        self.assertEqual(len(summary["normalized_results"]["complete_durable_service"]), 18)
        self.assertEqual(sections["consensus-in-memory"]["status"], "pending qualification")
        self.assertEqual(sections["failure-and-sustained-operation"]["history"][0]["unsent"], 567)
        evidence = {"durable_service": {"label": "Service", "href": "service"},
                    "durable_replication": {"label": "Storage", "href": "storage"},
                    "history_1": {"label": "History", "href": "history"}}
        markdown = render_markdown(summary, evidence)
        page = render_html(summary, evidence)
        for rendered in (markdown, page):
            self.assertIn("8,025", rendered)
            self.assertIn("3.79", rendered)
            self.assertIn("p99.9", rendered)
            self.assertIn("567 unsent", rendered)
            self.assertIn("Selected identities: Rafter aaaaaaaaaaaa; OpenRaft 0.9.24.", rendered)
        self.assertIn('"rafter_throughput_ops_s": 8025', json.dumps(summary, sort_keys=True))

    def test_multiple_candidates_require_explicit_selection(self):
        data = durable_data()
        extras = []
        for case in data["cases"]:
            if case["configuration"]["variant"] == "candidate-b32":
                other = copy.deepcopy(case)
                other["configuration"]["variant"] = "candidate-b64"
                other["source_case"] += "-b64"
                extras.append(other)
        data["cases"].extend(extras)
        data["suite"]["arms"].append(["candidate-b64", "rafter", 64, "candidate"])
        summary = build_summary(durable=data)
        section = summary["sections"][2]
        self.assertEqual(section["status"], "not comparable")
        selected = build_summary(durable=data, headline_variant="candidate-b32")["sections"][2]
        self.assertEqual(selected["status"], "measured lead")

    def test_multiple_openraft_controls_require_explicit_selection(self):
        data = durable_data()
        extras = []
        for case in data["cases"]:
            if case["configuration"]["variant"] == "openraft":
                other = copy.deepcopy(case)
                other["configuration"]["variant"] = "openraft-async"
                other["configuration"]["openraft_async_flush"] = True
                other["display_name"] = "OpenRaft async flusher"
                other["source_case"] += "-async"
                extras.append(other)
        data["cases"].extend(extras)
        data["suite"]["arms"].append(["openraft-async", "openraft", 1, "candidate"])
        section = build_summary(durable=data)["sections"][2]
        self.assertEqual(section["status"], "not comparable")
        selected = build_summary(
            durable=data, headline_control_variant="openraft-async"
        )["sections"][2]
        self.assertEqual(selected["status"], "measured lead")
        self.assertEqual(selected["selected_configuration"]["openraft_variant"], "openraft-async")

    def test_pipeline_names_distinguish_fifo_from_completion_priority(self):
        manifest = {"implementation": "rafter", "options": {"pipelined_durability": True}}
        self.assertEqual(_display_name(manifest), "Rafter pipeline FIFO")
        manifest["options"]["durable_completion_priority"] = True
        self.assertEqual(_display_name(manifest), "Rafter pipeline + completion priority")

        data = durable_data()
        for case in data["cases"]:
            if case["configuration"]["variant"] == "candidate-b32":
                case["display_name"] = "Rafter pipeline + completion priority"
            elif case["configuration"]["variant"] == "prior":
                case["configuration"]["mode"] = "pipeline"
                case["display_name"] = "Rafter pipeline FIFO"
        summary = build_summary(durable=data)
        internal = summary["sections"][2]["internal_comparisons"][0]
        self.assertEqual(internal["candidate_name"], "Rafter pipeline + completion priority")
        self.assertEqual(internal["prior_name"], "Rafter pipeline FIFO")
        for rendered in (render_markdown(summary, {}), render_html(summary, {})):
            self.assertIn(
                "Rafter pipeline + completion priority versus same-code Rafter pipeline FIFO",
                rendered,
            )
            self.assertNotIn("same-code synchronous Rafter", rendered)

    def test_capacity_curve_uses_declared_objective(self):
        data = durable_data()
        data["cases"] = []
        for rate in (2000, 4000):
            data["cases"].append(service_case(
                "candidate-b32", "rafter", 0, rate,
                1995 if rate == 2000 else 3980, 8 if rate == 2000 else 18,
                p999=15 if rate == 2000 else 40,
            ))
            data["cases"].append(service_case(
                "openraft", "openraft", 0, rate,
                1990 if rate == 2000 else 3900, 12 if rate == 2000 else 25,
                p999=30 if rate == 2000 else 60,
            ))
        summary = build_summary(durable=data)
        section = summary["sections"][2]
        self.assertEqual(section["status"], "measured lead")
        self.assertEqual(section["rows"], [])
        self.assertEqual(section["capacity"]["boundaries"], [{
            "network_delay_ms": 0,
            "maximum_tested_rate": 4000,
            "rafter_highest_qualifying_rate": 4000,
            "rafter_reached_tested_ceiling": True,
            "openraft_highest_qualifying_rate": 2000,
            "openraft_reached_tested_ceiling": False,
        }])
        rendered = render_markdown(summary, {})
        self.assertIn("Useful-capacity objective", rendered)
        self.assertIn("Execution p99/p99.9", rendered)
        self.assertIn("Useful-capacity objective", render_html(summary, {}))

    def test_capacity_report_retains_every_named_variant_boundary(self):
        data = durable_data()
        data["cases"] = []
        for rate in (1000, 2000):
            data["cases"].append(service_case(
                "candidate-b32", "rafter", 0, rate, rate,
                8 if rate == 1000 else 21, p999=15 if rate == 1000 else 45,
            ))
            data["cases"].append(service_case(
                "prior", "rafter", 0, rate, rate, 10, p999=18,
            ))
            data["cases"].append(service_case(
                "openraft", "openraft", 0, rate, rate,
                12 if rate == 1000 else 25, p999=30 if rate == 1000 else 60,
            ))
        summary = build_summary(durable=data)
        capacity = summary["sections"][2]["capacity"]
        boundaries = {row["variant"]: row for row in capacity["variant_boundaries"]}
        self.assertEqual(boundaries["prior"]["highest_qualifying_rate"], 2000)
        self.assertEqual(boundaries["candidate-b32"]["highest_qualifying_rate"], 1000)
        self.assertEqual(boundaries["openraft"]["highest_qualifying_rate"], 1000)
        for rendered in (render_markdown(summary, {}), render_html(summary, {})):
            self.assertIn("Rafter synchronous messages", rendered)
            self.assertIn("at least 2,000/s", rendered)

    def test_capacity_does_not_skip_a_failed_lower_rate(self):
        data = durable_data()
        data["cases"] = []
        for rate, p99 in ((1000, 10), (2000, 21), (3000, 10)):
            data["cases"].append(service_case(
                "candidate-b32", "rafter", 0, rate, rate, p99, p999=30,
            ))
            data["cases"].append(service_case(
                "openraft", "openraft", 0, rate, rate, 10, p999=30,
            ))
        capacity = build_summary(durable=data)["sections"][2]["capacity"]
        candidate = next(
            boundary for boundary in capacity["variant_boundaries"]
            if boundary["variant"] == "candidate-b32"
        )
        self.assertEqual(candidate["highest_qualifying_rate"], 1000)
        self.assertTrue(next(
            row for row in capacity["rows"] if row["offered_per_second"] == 3000
        )["rafter"]["qualifies"])

    def test_incomplete_suite_suppresses_headline(self):
        data = durable_data()
        data["qualification"] = "failed"
        data["qualification_errors"] = ["one missing case"]
        section = build_summary(durable=data)["sections"][2]
        self.assertEqual(section["status"], "mixed result")
        self.assertEqual(section["rows"], [])

    def test_incomplete_case_does_not_crash_failure_summary(self):
        data = durable_data()
        data["qualification"] = "failed"
        data["qualification_errors"] = ["one incomplete case"]
        data["cases"][0]["recovery"] = None
        data["cases"][0]["history_check"] = None
        section = build_summary(durable=data)["sections"][3]
        self.assertEqual(section["status"], "mixed result")
        self.assertEqual(section["checks"][0]["passed_cases"], len(data["cases"]) - 1)

    def test_candidate_accounting_loss_labels_result_mixed(self):
        data = durable_data()
        candidate = next(case for case in data["cases"]
                         if case["configuration"]["variant"] == "candidate-b32")
        candidate["accounting"]["not_issued"] = 1
        section = build_summary(durable=data)["sections"][2]
        self.assertEqual(section["status"], "mixed result")
        self.assertFalse(section["headline_qualified"])
        self.assertIn("is not qualified", section["result"])
        self.assertIn("1 unsent requests", section["result"])
        self.assertNotIn("3.79×", section["result"])
        self.assertEqual(section["accounting"]["rafter"]["not_issued"], 1)

    def test_saturated_tail_regression_labels_result_mixed(self):
        data = durable_data()
        candidate = next(case for case in data["cases"]
                         if case["configuration"]["variant"] == "candidate-b32"
                         and case["workload"]["network_delay_ms"] == 0
                         and case["workload"]["offered_per_second"] == 0)
        control = next(case for case in data["cases"]
                       if case["configuration"]["variant"] == "openraft"
                       and case["workload"]["network_delay_ms"] == 0
                       and case["workload"]["offered_per_second"] == 0)
        candidate["metrics"]["client_p99_ms"] = control["metrics"]["client_p99_ms"] * 1.2
        section = build_summary(durable=data)["sections"][2]
        self.assertEqual(section["status"], "mixed result")
        self.assertFalse(section["headline_qualified"])
        self.assertEqual(section["rows"][0]["saturated_p99_interpretation"], "measured regression")
        self.assertIn("1 of 12", section["result"])

    def test_storage_loader_recomputes_raw_receipts(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = {"runs": 3, "batches": 1000, "commit": "a" * 40,
                        "machine": {"machine": "x86_64"}, "cpu_affinity": [0],
                        "binaries": {"baseline": {"sha256": "same"}, "candidate": {"sha256": "same"}},
                        "hard_state": {"baseline": "replace", "candidate": "journal"}}
            (root / "metadata.json").write_text(json.dumps(metadata))
            stems = []
            samples = {"baseline": [10.0, 20.0, 30.0], "candidate": [20.0, 40.0, 60.0]}
            for run in range(1, 4):
                for workload in ("batch-1", "snapshot"):
                    for arm in ("baseline", "candidate"):
                        stem = f"run-{run}-{workload}-{arm}"
                        stems.append(stem)
                        raw = ({"harness": "rafter-bench-cluster", "hard_state": metadata["hard_state"][arm],
                                "workloads": [{"name": "proposals", "batch_size": 1, "payload_bytes": 256,
                                               "proposals_per_s": samples[arm][run - 1],
                                               "batch_completion_latency_ms": {"p99": samples[arm][run - 1] / 10}}]}
                               if workload == "batch-1" else
                               {"harness": "rafter-bench-cluster", "hard_state": metadata["hard_state"][arm],
                                "workloads": [{"name": "snapshot_transfer"}]})
                        (root / f"{stem}.json").write_text(json.dumps(raw))
                        (root / f"{stem}.execution.json").write_text(json.dumps({"exit_code": 0, "timed_out": False}))
            (root / "execution-order.json").write_text(json.dumps(stems))
            aggregate = {arm: {"batch-1": {"median": {"proposals_per_s": samples[arm][1],
                "batch_completion_latency_ms": {"p99": samples[arm][1] / 10}}}} for arm in samples}
            (root / "results.json").write_text(json.dumps({"results": aggregate}))
            loaded = load_storage_comparison(root)
            self.assertEqual(loaded["qualification"], "passed")
            self.assertEqual(loaded["records"][1]["metrics"]["throughput_ops_s"]["median"], 40.0)
            (root / "run-1-batch-1-candidate.execution.json").write_text(json.dumps({"exit_code": 1, "timed_out": False}))
            self.assertEqual(load_storage_comparison(root)["qualification"], "failed")

    def test_failed_microbench_is_retained_without_a_leaderboard(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "provenance.json").write_text(json.dumps({"kind": "in-memory"}))
            (root / "failure.json").write_text(json.dumps({"status": "failed", "error": "compiler failed"}))
            seal(root)
            loaded = load_microbench(root)
            self.assertEqual(loaded["qualification"], "failed")
            self.assertEqual(loaded["records"], [])
            section = build_summary(micro=loaded)["sections"][0]
            self.assertEqual(section["status"], "pending qualification")
            self.assertIn("incomplete", section["details"][0])


if __name__ == "__main__":
    unittest.main()

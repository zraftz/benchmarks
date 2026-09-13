import copy
import json
from pathlib import Path
import tempfile
import unittest
from benchctl.evidence import (CONTRACT, histogram_summary, load_environment_errors,
                               runtime_configuration_errors, seal,
                               filesystem_space, system_sample, validate_result,
                               verify, write_json)
from benchctl.machine import load_environment_receipt
from benchctl.replication_windows import configuration_receipt
from tests.test_timelines import add_runtime_windows, snapshot, timeline


def result():
    bins=[0]*4096;bins[64]=2
    h={"bins":bins,"count":2,"maximum_ns":1}
    return {"contract":CONTRACT,"offered":3,"attempted":2,"not_issued":1,"ok":2,"unknown":0,"errors":0,
            "completed_in_window":2,"network_attempts":2,"success_histogram":h,"all_histogram":copy.deepcopy(h)}

class EvidenceTests(unittest.TestCase):
    def test_system_sample_records_pressure_and_throttling_counters(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proc = root / "proc"
            cgroup = root / "cgroup"
            sysfs = root / "sys"
            (proc / "pressure").mkdir(parents=True)
            cgroup.mkdir()
            throttle = sysfs / "devices/system/cpu/cpu0/thermal_throttle"
            throttle.mkdir(parents=True)
            (throttle / "core_throttle_count").write_text("7\n")
            (proc / "stat").write_text("cpu 1 2 3 4 5 6 7 8 9 10\n")
            (proc / "loadavg").write_text("0.10 0.20 0.30 2/100 4321\n")
            (proc / "diskstats").write_text(
                "8 0 nvme0n1 10 1 20 30 40 2 50 60 0 70 80 3 4 5 6 7 8\n"
            )
            (cgroup / "cpu.stat").write_text("usage_usec 100\nnr_throttled 3\nthrottled_usec 40\n")
            for resource in ("cpu", "io", "memory"):
                (proc / "pressure" / resource).write_text(
                    "some avg10=1.25 avg60=0.50 avg300=0.10 total=1234\n"
                    "full avg10=0.25 avg60=0.05 avg300=0.01 total=234\n"
                )
            sample = system_sample(
                proc_root=proc,
                cgroup_root=cgroup,
                sysfs_root=sysfs,
                data_path=root,
            )
            self.assertEqual(sample["cpu_ticks"]["steal"], 8)
            self.assertEqual(sample["cgroup_cpu"]["throttled_usec"], 40)
            self.assertEqual(sample["pressure"]["io"]["some"]["total"], 1234)
            self.assertEqual(sample["load_average"]["runnable"], "2/100")
            self.assertEqual(sample["block_devices"]["nvme0n1"]["read_ms"], 30)
            self.assertEqual(sample["block_devices"]["nvme0n1"]["write_ms"], 60)
            self.assertEqual(sample["block_devices"]["nvme0n1"]["io_ms"], 70)
            self.assertEqual(sample["block_devices"]["nvme0n1"]["flush_ms"], 8)
            self.assertEqual(
                sample["hardware_throttle_counts"]["cpu0/thermal_throttle/core_throttle_count"],
                7,
            )
            self.assertGreater(sample["filesystem_space"]["available_bytes"], 0)

    def test_filesystem_space_records_capacity_for_data_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            space = filesystem_space(Path(tmp))
            self.assertGreater(space["total_bytes"], 0)
            self.assertGreater(space["free_bytes"], 0)
            self.assertGreater(space["available_bytes"], 0)

    def test_measured_load_environment_receipt_replays_raw_samples(self):
        samples = [
            {
                "controller_seconds": second,
                "monotonic_ns": (second + 1) * 1_000_000_000,
                "system": {"pressure": {}, "filesystem_space": None},
            }
            for second in range(3)
        ]
        declared = {
            "schema": 1,
            "expected_duration_seconds": 2,
            "nominal_sample_interval_seconds": 1,
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "process-samples.jsonl").write_text(
                "".join(json.dumps(item) + "\n" for item in samples)
            )
            receipt = load_environment_receipt(
                samples,
                expected_duration_seconds=2,
                nominal_sample_interval_seconds=1,
            )
            write_json(root / "load-environment.json", receipt)
            self.assertEqual(
                load_environment_errors(root, {"load_environment_observation": declared}),
                [],
            )

            incomplete = dict(declared)
            incomplete["expected_duration_seconds"] = 4
            receipt = load_environment_receipt(
                samples,
                expected_duration_seconds=4,
                nominal_sample_interval_seconds=1,
            )
            (root / "load-environment.json").write_text(json.dumps(receipt))
            self.assertEqual(
                load_environment_errors(
                    root, {"load_environment_observation": incomplete}
                ),
                ["measured-load environment sampling coverage did not pass"],
            )

            receipt = load_environment_receipt(
                samples,
                expected_duration_seconds=2,
                nominal_sample_interval_seconds=1,
            )
            receipt["summary"]["sample_count"] += 1
            (root / "load-environment.json").write_text(json.dumps(receipt))
            self.assertEqual(
                load_environment_errors(root, {"load_environment_observation": declared}),
                ["measured-load environment receipt differs from raw samples"],
            )

    def test_old_cases_reject_undeclared_measured_load_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertEqual(load_environment_errors(root, {}), [])
            write_json(root / "load-environment.json", {})
            self.assertEqual(
                load_environment_errors(root, {}),
                ["unexpected measured-load environment receipt"],
            )

    def test_fault_smoke_does_not_require_durable_kv_priority_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            write_json(p / "manifest.json", {
                "implementation": "rafter",
                "scenario": "leader-loss",
                "options": {
                    "durable_completion_priority": True,
                    "pipelined_durability": True,
                },
            })
            write_json(p / "measurement.json", result())
            write_json(p / "qualification.json", {"status": "passed"})
            write_json(p / "recovery.json", {"status": "passed"})
            event = {
                "command": {
                    "client": "client-1",
                    "sequence": 1,
                    "kind": "put",
                    "key": "key-1",
                    "value": "value-1",
                },
                "reply": {
                    "status": "ok",
                    "result": {"value": "value-1", "swapped": None, "error": None},
                },
                "start_ns": 1,
                "end_ns": 2,
            }
            (p / "qualification-history.jsonl").write_text(json.dumps(event) + "\n")
            seal(p)
            checked = verify(p)
            self.assertEqual(checked["status"], "passed")
            self.assertEqual(checked["verdicts"]["evidence_integrity"]["status"], "passed")
            self.assertEqual(checked["verdicts"]["correctness_checks"]["status"], "passed")

    def test_new_rafter_diagnostics_require_matching_runtime_configuration(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            before = add_runtime_windows(snapshot(64, [timeline(1)]), 4)
            after = add_runtime_windows(snapshot(128, [timeline(1), timeline(65)]), 4)
            write_json(p / "before.json", before)
            write_json(p / "diagnostics-after-load.json", after)
            manifest = {"implementation": "rafter", "options": {
                "diagnostics": True, "max_inflight_appends": 4}}
            self.assertEqual(runtime_configuration_errors(p, manifest),
                             ["replication window activation receipt is missing"])
            write_json(p / "replication-window-config.json",
                       configuration_receipt(before, after, 4))
            self.assertEqual(runtime_configuration_errors(p, manifest), [])

    def test_old_rafter_evidence_does_not_require_new_runtime_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest = {"implementation": "rafter", "options": {"diagnostics": True}}
            self.assertEqual(runtime_configuration_errors(Path(tmp), manifest), [])

    def test_accounting(self):self.assertEqual(validate_result(result()),[])
    def test_omitted_offered_work(self):
        r=result();r["offered"]+=1;self.assertTrue(validate_result(r))
    def test_missing_latency_sample(self):
        r=result();r["success_histogram"]["count"]=1;self.assertTrue(validate_result(r))
    def test_in_window_count(self):
        r=result();r["completed_in_window"]=3;self.assertTrue(validate_result(r))
    def test_contract(self):
        r=result();r["contract"]="memory-only";self.assertTrue(validate_result(r))
    def test_boolean_count_is_invalid(self):
        r=result();r["ok"]=True;self.assertTrue(validate_result(r))
    def test_no_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/"r.json";write_json(p,{})
            with self.assertRaises(FileExistsError):write_json(p,{})
    def test_tamper_detected(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp);write_json(p/"measurement.json",result());write_json(p/"qualification.json",{"status":"passed"});write_json(p/"recovery.json",{"status":"passed"})
            seal(p);(p/"measurement.json").write_text("{}")
            checked = verify(p)
            self.assertEqual(checked["status"],"failed")
            self.assertEqual(checked["verdicts"]["evidence_integrity"]["status"], "failed")
            self.assertTrue(any("checksum" in e for e in checked["errors"]))
    def test_symlink_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp);(p/"link").symlink_to("/etc/passwd")
            with self.assertRaises(ValueError):seal(p)
    def test_execution_histograms_required_for_new_schema(self):
        r=result();r["schema"]=2
        self.assertTrue(validate_result(r))
        r["success_execution_histogram"]=copy.deepcopy(r["success_histogram"])
        r["all_execution_histogram"]=copy.deepcopy(r["all_histogram"])
        self.assertEqual(validate_result(r), [])
        r["all_execution_histogram"]["count"] = 1
        self.assertTrue(validate_result(r))

    def test_schema_three_replays_scheduler_and_worker_histograms(self):
        r = result()
        r["schema"] = 3
        r["config"] = {"rate": 100}
        for histogram_name in (
            "success_execution_histogram",
            "all_execution_histogram",
            "worker_start_lateness_histogram",
        ):
            r[histogram_name] = copy.deepcopy(r["success_histogram"])
        scheduler = copy.deepcopy(r["success_histogram"])
        scheduler["bins"][64] = 3
        scheduler["count"] = 3
        r["scheduler_lateness_histogram"] = scheduler
        for histogram_name, summary_name in (
            ("success_histogram", "success_latency"),
            ("all_histogram", "all_dispatched_latency"),
            ("success_execution_histogram", "success_execution_latency"),
            ("all_execution_histogram", "all_dispatched_execution_latency"),
            ("worker_start_lateness_histogram", "worker_start_lateness"),
            ("scheduler_lateness_histogram", "scheduler_lateness"),
        ):
            r[summary_name] = histogram_summary(r[histogram_name])
        self.assertEqual(validate_result(r), [])

        r["scheduler_lateness"]["p99_ms"] += 1
        self.assertIn(
            "histogram summary mismatch: scheduler_lateness",
            validate_result(r),
        )
        r["scheduler_lateness"] = histogram_summary(scheduler)
        r["scheduler_lateness_histogram"]["maximum_ns"] = 2
        self.assertIn(
            "histogram maximum differs from bins: scheduler_lateness_histogram",
            validate_result(r),
        )

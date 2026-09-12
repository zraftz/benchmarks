import copy
import json
from pathlib import Path
import tempfile
import unittest
from benchctl.evidence import (CONTRACT, runtime_configuration_errors, seal,
                               system_sample, validate_result, verify, write_json)
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
            (proc / "pressure").mkdir(parents=True)
            cgroup.mkdir()
            (proc / "stat").write_text("cpu 1 2 3 4 5 6 7 8 9 10\n")
            (proc / "loadavg").write_text("0.10 0.20 0.30 2/100 4321\n")
            (cgroup / "cpu.stat").write_text("usage_usec 100\nnr_throttled 3\nthrottled_usec 40\n")
            for resource in ("cpu", "io", "memory"):
                (proc / "pressure" / resource).write_text(
                    "some avg10=1.25 avg60=0.50 avg300=0.10 total=1234\n"
                    "full avg10=0.25 avg60=0.05 avg300=0.01 total=234\n"
                )
            sample = system_sample(proc_root=proc, cgroup_root=cgroup)
            self.assertEqual(sample["cpu_ticks"]["steal"], 8)
            self.assertEqual(sample["cgroup_cpu"]["throttled_usec"], 40)
            self.assertEqual(sample["pressure"]["io"]["some"]["total"], 1234)
            self.assertEqual(sample["load_average"]["runnable"], "2/100")

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
            self.assertEqual(verify(p)["status"], "passed")

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
            self.assertEqual(verify(p)["status"],"failed")
            self.assertTrue(any("checksum" in e for e in verify(p)["errors"]))
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

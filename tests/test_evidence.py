import copy
import json
from pathlib import Path
import tempfile
import unittest
from benchctl.evidence import (CONTRACT, runtime_configuration_errors, seal,
                               validate_result, verify, write_json)
from benchctl.replication_windows import configuration_receipt
from tests.test_timelines import add_runtime_windows, snapshot, timeline


def result():
    bins=[0]*4096;bins[64]=2
    h={"bins":bins,"count":2,"maximum_ns":1}
    return {"contract":CONTRACT,"offered":3,"attempted":2,"not_issued":1,"ok":2,"unknown":0,"errors":0,
            "completed_in_window":2,"network_attempts":2,"success_histogram":h,"all_histogram":copy.deepcopy(h)}

class EvidenceTests(unittest.TestCase):
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

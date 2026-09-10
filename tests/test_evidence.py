import copy
import json
from pathlib import Path
import tempfile
import unittest
from benchctl.evidence import CONTRACT, seal, validate_result, verify, write_json
from benchctl.legacy import blob_sha, rewrite_dependencies


def result():
    bins=[0]*4096;bins[64]=2
    h={"bins":bins,"count":2,"maximum_ns":1}
    return {"contract":CONTRACT,"offered":3,"attempted":2,"not_issued":1,"ok":2,"unknown":0,"errors":0,
            "completed_in_window":2,"network_attempts":2,"success_histogram":h,"all_histogram":copy.deepcopy(h)}

class EvidenceTests(unittest.TestCase):
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
    def test_dependency_boundary(self):
        source="\n".join(f'rafter-{i} = {{ path = "../crates/rafter-{i}" }}' for i in range(8))
        rewritten=rewrite_dependencies(source)
        self.assertEqual(rewritten.count('rev = "518a'),8)
        self.assertNotIn("../crates",rewritten)
    def test_changed_boundary_is_not_silently_accepted(self):
        with self.assertRaises(ValueError):rewrite_dependencies('rafter = { path = "../crates/rafter" }')
    def test_git_blob_hash(self):
        self.assertEqual(blob_sha(b""),"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391")

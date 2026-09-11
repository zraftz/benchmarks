"""End-to-end TOOLING tests against an explicitly non-Raft shared-SQLite double."""
from pathlib import Path
import json
import os
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from benchctl.evidence import ROOT, digest, verify
from benchctl.runner import run_case

class IntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.name != "posix":
            raise unittest.SkipTest("POSIX process tests")
        binary=ROOT/"dist/raft-bench-load"
        binary.parent.mkdir(exist_ok=True)
        subprocess.run(["go","build","-trimpath","-o",str(binary),"."],cwd=ROOT/"loadgen",check=True)
    def case(self,scenario,rate):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            options=SimpleNamespace(scenario=scenario,smoke=True,batch_size=64,duration=2.4,warmup=.1,
                concurrency=4,payload=128,rate=rate,keyspace=32,seed=7,read_percent=20,cas_percent=10,timeout=1.5)
            run_case("test-fixture",root/"case",root/"data",options,
                command=[sys.executable,str(ROOT/"tests/fake_node.py")],
                build_receipt={"binaries": {"test-fixture": digest(Path(sys.executable))},
                    "implementations": {"test-fixture": {"purpose": "tooling test only"}}})
            check=verify(root/"case")
            self.assertEqual(check["status"],"passed",check)
            r=json.loads((root/"case/measurement.json").read_text())
            self.assertGreater(r["ok"],0)
            self.assertEqual(r["schema"],2)
            self.assertEqual(r["success_execution_histogram"]["count"],r["ok"])
            self.assertEqual(r["all_execution_histogram"]["count"],r["attempted"])
            self.assertEqual(r["offered"],r["attempted"]+r["not_issued"])
            m=json.loads((root/"case/manifest.json").read_text())
            self.assertEqual(m["implementation"],"test-fixture")
            self.assertTrue(m["smoke"])
            fault=json.loads((root/"case/fault.json").read_text())
            self.assertEqual(len(fault["events"]),0 if scenario=="durable-kv" else 2)
    def test_closed_loop_end_to_end(self):self.case("durable-kv",0)
    def test_scheduled_rate_end_to_end(self):self.case("durable-kv",100)
    def test_leader_process_loss(self):self.case("leader-loss",100)
    def test_follower_pause_and_resume(self):self.case("follower-catchup",100)

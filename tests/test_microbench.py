import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from benchctl.microbench import run_microbench
from benchctl.micro_report import summarize


def sample(rate):
    return {"run": 1, "results": [{"library": "rafter", "commit_latency_definition": "proposal to apply",
            "workloads": [{"name": "serial", "proposals": 100, "payload_bytes": 512, "max_in_flight": 1,
                           "elapsed_ms": 1000 / rate, "proposals_per_s": rate,
                           "commit_latency_us": {"p50": 1, "p99": 2}}]}]}


class MicrobenchTests(unittest.TestCase):
    def test_aggregates_medians_and_retains_range(self):
        row = summarize([sample(20), sample(10), sample(90)])[0]
        self.assertEqual(row["proposals_per_s"], {"min": 10, "median": 20, "max": 90})
        self.assertEqual(row["runs"], 3)

    def test_changed_workload_or_configuration_is_rejected(self):
        for field, value in (("name", "other"), ("payload_bytes", 1024), ("proposals_per_s", float("nan"))):
            changed = copy.deepcopy(sample(10))
            changed["results"][0]["workloads"][0][field] = value
            with self.assertRaises(ValueError):
                summarize([sample(10), changed])

    def test_failed_build_retains_identity_and_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); (root / "microbench").mkdir()
            (root / "microbench/UPSTREAM.json").write_text("{}")
            (root / "implementations.lock.json").write_text(json.dumps({"rafter": {"rev": "a" * 40}}))
            build_environment = {}
            def fail(command, **kwargs):
                build_environment.update(kwargs["env"])
                kwargs["stdout"].write("compiler failed")
                raise subprocess.CalledProcessError(1, command)
            out = root / "result"
            with patch("benchctl.microbench.ROOT", root), patch("benchctl.microbench.check_resolution"), \
                 patch("benchctl.microbench.source_digest", return_value="code"), patch("benchctl.microbench.host_info", return_value={}), \
                 patch("benchctl.microbench.capture", return_value={}), patch("benchctl.microbench.subprocess.run", side_effect=fail):
                with self.assertRaises(subprocess.CalledProcessError):
                    run_microbench("full", 1, out)
            self.assertIn("compiler failed", (out / "build.log").read_text())
            self.assertEqual(json.loads((out / "provenance.json").read_text())["rafter"]["rev"], "a" * 40)
            self.assertEqual(build_environment["RAFTER_BENCH_REV"], "a" * 40)
            self.assertTrue((out / "failure.json").exists())
            self.assertTrue((out / "SHA256SUMS.json").exists())
            self.assertFalse((out / "report.html").exists())

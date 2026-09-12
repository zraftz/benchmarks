import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchctl.report import render


def write_case(root: Path, name: str, threshold: int, window: int) -> None:
    case = root / name
    case.mkdir()
    manifest = {
        "implementation": "rafter",
        "implementation_pins": {"rafter": {"rev": "a" * 40}},
        "scenario": "durable-kv",
        "smoke": False,
        "build_receipt": {"rafter_hard_state_backend": "wal", "peer_group_commit": True},
        "options": {
            "variant": name,
            "batch_size": 64,
            "peer_batch_size": 32,
            "max_speculative_proposals": threshold,
            "max_inflight_appends": window,
            "network_delay_ms": 0,
            "ordered_apply": True,
            "peer_message_stream": True,
            "pipelined_durability": True,
            "combine_peer_proposals": True,
            "diagnostics": False,
        },
    }
    measurement = {
        "config": {"rate": 1000, "payload_bytes": 512, "concurrency": 64,
                   "read_percent": 0, "cas_percent": 0},
        "successful_ops_per_second": 1000,
        "success_latency": {"p99_ms": 2},
        "success_execution_latency": {"p99_ms": 1},
        "worker_start_lateness": {"p99_ms": 1},
        "errors": 0,
        "unknown": 0,
        "not_issued": 0,
    }
    (case / "manifest.json").write_text(json.dumps(manifest))
    (case / "measurement.json").write_text(json.dumps(measurement))


class ReportTests(unittest.TestCase):
    def test_thresholds_and_windows_are_never_pooled(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_case(root, "candidate-s4-w4", 4, 4)
            write_case(root, "candidate-s8-w16", 8, 16)
            destination = root / "report.html"
            with patch("benchctl.report.verify", return_value={"status": "passed"}):
                render(root, destination)
            report = destination.read_text()
            self.assertIn("candidate-s4-w4", report)
            self.assertIn("speculative ≤4", report)
            self.assertIn("append window ≤4", report)
            self.assertIn("candidate-s8-w16", report)
            self.assertIn("speculative ≤8", report)
            self.assertIn("append window ≤16", report)


if __name__ == "__main__":
    unittest.main()

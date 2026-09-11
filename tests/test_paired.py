from pathlib import Path
import json
import tempfile
import unittest
from unittest.mock import patch
from benchctl.paired import archive_build, ordered_arms, mode_flags
from benchctl.evidence import digest


class PairedTests(unittest.TestCase):
    def test_order_is_balanced_and_each_arm_runs_once(self):
        arms = list("abcdef")
        orders = [ordered_arms(arms, repeat) for repeat in range(3)]
        self.assertEqual(orders, [list("abcdef"), list("afedcb"), list("cdefab")])
        self.assertTrue(all(sorted(order) == arms for order in orders))

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

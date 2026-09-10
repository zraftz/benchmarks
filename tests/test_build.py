import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from benchctl.build import IMPLEMENTATIONS, build
from benchctl.evidence import digest


class BuildTests(unittest.TestCase):
    def test_missing_lockfile_refuses_resolution_and_invalidates_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "dist").mkdir()
            receipt = root / "dist/build.json"
            receipt.write_text("old receipt")
            with patch("benchctl.build.ROOT", root), patch("benchctl.build.subprocess.run") as run:
                with self.assertRaisesRegex(RuntimeError, "Cargo.lock is missing"):
                    build()
            run.assert_not_called()
            self.assertFalse(receipt.exists())
            self.assertFalse((root / "Cargo.lock").exists())

    def test_failed_build_invalidates_old_receipt_and_preserves_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Cargo.lock").write_text("locked")
            (root / "implementations.lock.json").write_text(json.dumps({"rafter": {"rev": "a" * 40}}))
            (root / "dist").mkdir()
            (root / "dist/build.json").write_text("old receipt")
            def fail(command, **kwargs):
                kwargs["stdout"].write("compiler failure")
                raise subprocess.CalledProcessError(1, command)
            with patch("benchctl.build.ROOT", root), patch("benchctl.build.shutil.which", return_value="tool"), \
                 patch("benchctl.build.check_resolution"), \
                 patch("benchctl.build.subprocess.run", side_effect=fail):
                with self.assertRaisesRegex(RuntimeError, "dist/rust-tests.log"):
                    build()
            self.assertFalse((root / "dist/build.json").exists())
            self.assertEqual((root / "dist/rust-tests.log").read_text(), "compiler failure")

    def test_cargo_output_directory_is_staged_and_hashed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "source"
            root.mkdir()
            (root / "Cargo.lock").write_text("locked")
            (root / "implementations.lock.json").write_text(json.dumps({"rafter": {"rev": "a" * 40}}))
            target = Path(tmp) / "external target"
            (target / "release").mkdir(parents=True)
            for name in IMPLEMENTATIONS:
                binary = target / "release" / f"raft-bench-{name}"
                binary.write_text(name)
                binary.chmod(0o755)
            def run(command, **kwargs):
                if command[:2] == ["go", "build"]:
                    (root / "dist/raft-bench-load").write_text("loadgen")
            with patch("benchctl.build.ROOT", root), patch("benchctl.build.shutil.which", return_value="tool"), \
                 patch("benchctl.build.check_resolution"), \
                 patch("benchctl.build.subprocess.run", side_effect=run), \
                 patch("benchctl.build.subprocess.check_output", return_value=json.dumps({"target_directory": str(target)})), \
                 patch("benchctl.build.capture", return_value={}), patch("benchctl.build.source_digest", return_value="source"):
                build()
            receipt = json.loads((root / "dist/build.json").read_text())
            for name in IMPLEMENTATIONS:
                binary = root / "dist" / f"raft-bench-{name}"
                self.assertEqual(binary.read_text(), name)
                self.assertTrue(binary.stat().st_mode & 0o111)
                self.assertEqual(receipt["binaries"][name], digest(binary))
            self.assertEqual(receipt["cargo_lock_sha256"], digest(root / "Cargo.lock"))

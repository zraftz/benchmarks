import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from benchctl.selection import (
    LOCKS,
    MANIFESTS,
    REPOSITORY,
    check_resolution,
    capture_rafter_selection,
    resolve_ref,
    rewrite_lock_sources,
    restore_rafter_selection,
    select_rafter,
)

OLD, NEW = "a" * 40, "b" * 40


def fixture(root):
    for folder in ("adapters/rafter", "crates", "microbench", ".cargo", "dist/microbench"):
        (root / folder).mkdir(parents=True, exist_ok=True)
    (root / "Cargo.toml").write_text("[workspace]\n")
    (root / "rust-toolchain.toml").write_text('[toolchain]\nchannel="1.88.0"\n')
    (root / "implementations.lock.json").write_text(json.dumps({"rafter": {"git": REPOSITORY, "rev": OLD}}))
    packages = ("rafter", "rafter-runtime", "rafter-storage", "rafter-codec")
    for name in MANIFESTS:
        manifest = root / name
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text("[dependencies]\n" + "".join(
            f'{p} = {{ git = "{REPOSITORY}", rev = "{OLD}" }}\n' for p in packages))
    lock = "".join(f'[[package]]\nname="{p}"\nversion="0.1.0"\nsource="git+{REPOSITORY}?rev={OLD}#{OLD}"\n' for p in packages)
    lock += '[[package]]\nname="raft"\nversion="0.7.0"\n[[package]]\nname="openraft"\nversion="0.9.24"\n'
    for name in LOCKS:
        (root / name).write_text(lock)
    (root / ".cache").mkdir()
    for name in ("dist/build.json", "dist/microbench/build.json"):
        (root / name).write_text("old receipt")


class SelectionTests(unittest.TestCase):
    def test_captured_selection_restores_exact_files_and_invalidates_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture(root)
            captured = capture_rafter_selection(root=root)
            for name in captured:
                (root / name).write_bytes(b"changed")
            restore_rafter_selection(captured, root=root)
            self.assertEqual(
                capture_rafter_selection(root=root),
                captured,
            )
            self.assertFalse((root / "dist/build.json").exists())
            self.assertFalse((root / "dist/microbench/build.json").exists())
            with self.assertRaisesRegex(ValueError, "unexpected file inventory"):
                restore_rafter_selection({}, root=root)

    def test_bad_refs_are_rejected_before_fetch(self):
        for ref in ("--upload-pack=evil", "main;id", "../main", "main~1", "", "x\ny"):
            with patch("benchctl.selection.subprocess.run") as run:
                with self.assertRaises(ValueError):
                    resolve_ref(ref, Path("unused"))
                run.assert_not_called()

    def test_branch_and_exact_commit_resolve_with_real_git(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, cache = Path(tmp) / "source", Path(tmp) / "cache.git"
            subprocess.run(["git", "init", "-q", "-b", "perf/test", str(source)], check=True)
            (source / "file").write_text("one")
            subprocess.run(["git", "-C", str(source), "add", "."], check=True)
            subprocess.run(["git", "-C", str(source), "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid",
                            "-c", "commit.gpgsign=false", "commit", "-qm", "fixture"], check=True)
            sha = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
            with patch("benchctl.selection.REPOSITORY", str(source)):
                self.assertEqual(resolve_ref("perf/test", cache), sha)
                self.assertEqual(resolve_ref(sha, cache), sha)

    def test_selection_updates_both_locks_and_invalidates_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); fixture(root)
            commands = []
            def update(command, **_kwargs):
                commands.append(command)
            with patch("benchctl.selection.ROOT", root), patch("benchctl.selection.resolve_ref", return_value=NEW) as resolve, \
                 patch("benchctl.selection.subprocess.run", side_effect=update):
                selected = select_rafter("perf/test")
            resolve.assert_called_once()
            self.assertEqual(selected["rev"], NEW)
            self.assertEqual(selected["requested_ref"], "perf/test")
            self.assertEqual(len(commands), 2)
            for command in commands:
                self.assertEqual(command[1:5], ["metadata", "--locked", "--format-version", "1"])
                self.assertNotIn("--workspace", command)
            check_resolution(root, NEW)
            self.assertFalse((root / "dist/build.json").exists())
            self.assertFalse((root / "dist/microbench/build.json").exists())

    def test_changed_dependency_graph_uses_targeted_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); fixture(root)
            commands = []

            def update(command, **_kwargs):
                commands.append(command)
                if command[1] == "metadata":
                    raise subprocess.CalledProcessError(101, command)

            with patch("benchctl.selection.ROOT", root), \
                 patch("benchctl.selection.resolve_ref", return_value=NEW), \
                 patch("benchctl.selection.subprocess.run", side_effect=update):
                select_rafter("perf/test")
            self.assertEqual([command[1] for command in commands],
                             ["metadata", "update", "metadata", "update"])
            for command in commands[1::2]:
                self.assertEqual(command[1:6], ["update", "-p", "rafter", "--precise", NEW])
                self.assertNotIn("--workspace", command)

    def test_lock_rewrite_changes_only_exact_rafter_sources(self):
        content = (
            f'source="git+{REPOSITORY}?rev={OLD}#{OLD}"\n'
            f'checksum="{OLD}"\n'
        )
        self.assertEqual(
            rewrite_lock_sources(content, OLD, NEW),
            f'source="git+{REPOSITORY}?rev={NEW}#{NEW}"\nchecksum="{OLD}"\n',
        )

    def test_failed_resolution_keeps_checkout_and_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); fixture(root)
            originals = {name: (root / name).read_bytes() for name in (*MANIFESTS, *LOCKS, "implementations.lock.json", "dist/build.json")}
            with patch("benchctl.selection.ROOT", root), patch("benchctl.selection.resolve_ref", return_value=NEW), \
                 patch("benchctl.selection.subprocess.run", side_effect=subprocess.CalledProcessError(1, "cargo")):
                with self.assertRaises(subprocess.CalledProcessError):
                    select_rafter("bad-api")
            self.assertEqual(originals, {name: (root / name).read_bytes() for name in originals})

    def test_mixed_revision_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); fixture(root)
            p = root / "microbench/Cargo.lock"
            p.write_text(p.read_text().replace(OLD, NEW, 1))
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                check_resolution(root, OLD)

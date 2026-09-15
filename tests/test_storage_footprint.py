import tempfile
import unittest
from pathlib import Path

from benchctl.evidence import storage_footprint_errors, write_json
from benchctl.storage_footprint import OBSERVATION, capture, errors, receipt


class StorageFootprintTests(unittest.TestCase):
    def make_tree(self, root: Path, suffix: bytes = b"") -> None:
        for node in (1, 2, 3):
            directory = root / f"node-{node}"
            (directory / "raft/snapshots").mkdir(parents=True)
            (directory / "application.wal").write_bytes(b"application" + suffix)
            (directory / "raft/raft-wal-current").write_bytes(b"manifest")
            (directory / f"raft/raft-wal-segment-{node:020d}.rfwb").write_bytes(
                b"segment" + suffix
            )
            (directory / "raft/snapshots/current.snapshot").write_bytes(b"snapshot manifest")
            (directory / f"raft/snapshots/snapshot-{node}-1-1-1.rfsn").write_bytes(
                b"snapshot data" + suffix
            )
            (directory / f"raft/snapshots/.snapshot-{node}.tmp").write_bytes(b"")
            (directory / "IDENTITY.json").write_bytes(b"identity")

    def test_capture_separates_raft_wal_and_append_only_application_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.make_tree(root)
            observed = capture(root)
        self.assertEqual(observed["totals"]["raft_wal_data"]["files"], 3)
        self.assertEqual(observed["totals"]["raft_wal_metadata"]["files"], 3)
        self.assertEqual(observed["totals"]["raft_snapshot_data"]["files"], 3)
        self.assertEqual(observed["totals"]["raft_snapshot_metadata"]["files"], 3)
        self.assertEqual(observed["totals"]["raft_snapshot_temporary"]["files"], 3)
        self.assertEqual(observed["totals"]["application_journal"]["files"], 3)
        self.assertEqual(observed["totals"]["other"]["files"], 3)

    def test_receipt_recomputes_totals_and_rejects_tampering(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.make_tree(root)
            before = capture(root)
            for node in (1, 2, 3):
                with (root / f"node-{node}/application.wal").open("ab") as stream:
                    stream.write(b"growth")
            after = capture(root)
            value = receipt(before, after, after)
        self.assertEqual(errors(value), [])
        value["deltas"]["measurement"]["application_journal"]["logical_bytes"] += 1
        self.assertEqual(
            errors(value), ["storage footprint derived totals or deltas differ"]
        )

    def test_capture_rejects_symlinked_live_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.make_tree(root)
            (root / "node-1/raft/link").symlink_to(root / "node-2/raft")
            with self.assertRaisesRegex(ValueError, "symlink"):
                capture(root)

    def test_declared_receipt_requires_three_coherent_status_barriers(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            live = root / "live"
            case = root / "case"
            live.mkdir()
            case.mkdir()
            self.make_tree(live)
            observed = capture(live)
            write_json(case / "storage-footprint.json", receipt(observed, observed, observed))
            statuses = {
                str(node): {"status": "ok", "info": {"node_id": node}}
                for node in (1, 2, 3)
            }
            for name in OBSERVATION["status_barriers"].values():
                write_json(case / name, statuses)
            manifest = {"storage_footprint_observation": OBSERVATION}
            self.assertEqual(storage_footprint_errors(case, manifest), [])
            malformed = dict(statuses)
            malformed["2"] = {"status": "ok", "info": []}
            (case / "after-final-restart.json").unlink()
            write_json(case / "after-final-restart.json", malformed)
            self.assertEqual(
                storage_footprint_errors(case, manifest),
                ["storage footprint after_final_restart status barrier is incoherent"],
            )
            (case / "after-final-restart.json").unlink()
            self.assertEqual(
                storage_footprint_errors(case, manifest),
                ["storage footprint after_final_restart status barrier is missing"],
            )


if __name__ == "__main__":
    unittest.main()

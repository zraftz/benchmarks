from pathlib import Path
import json
import tempfile
import unittest

from benchctl.evidence import seal, verify, write_json
from benchctl.machine import OBJECTIVE, assess_idle, storage_probe, summarize_idle


def sample(at: int, *, iowait: int, steal: int, io_some: int, io_full: int,
           throttled: int, available: int) -> dict:
    return {
        "monotonic_ns": at,
        "system": {
            "cpu_ticks": {
                "user": at // 10_000_000,
                "nice": 0,
                "system": 0,
                "idle": at // 20_000_000,
                "iowait": iowait,
                "irq": 0,
                "softirq": 0,
                "steal": steal,
                "guest": 0,
                "guest_nice": 0,
            },
            "cgroup_cpu": {"usage_usec": at // 1000, "throttled_usec": throttled},
            "pressure": {
                "cpu": {"some": {"total": 0}, "full": {"total": 0}},
                "io": {"some": {"total": io_some}, "full": {"total": io_full}},
                "memory": {"some": {"total": 0}, "full": {"total": 0}},
            },
            "block_devices": {
                "nvme0n1": {"major": 259, "minor": 0, "writes_completed": at // 1_000_000_000}
            },
            "filesystem_space": {"available_bytes": available},
        },
    }


def quiet_samples() -> list[dict]:
    available = 30 * 1024**3
    return [
        sample((second + 1) * 1_000_000_000, iowait=10 + second, steal=2,
               io_some=1_000 + second * 1_000, io_full=100,
               throttled=10, available=available)
        for second in range(16)
    ]


class MachineProfileTests(unittest.TestCase):
    def test_idle_summary_and_predeclared_objective_pass(self):
        samples = quiet_samples()
        summary = summarize_idle(samples)
        self.assertEqual(summary["sample_count"], 16)
        self.assertEqual(summary["elapsed_seconds"], 15)
        self.assertEqual(summary["maximum_sample_gap_seconds"], 1)
        self.assertEqual(summary["block_devices"]["nvme0n1"]["writes_completed"], 15)
        self.assertEqual(assess_idle(summary)["status"], "passed")

    def test_pressure_and_space_fail_without_hiding_measurements(self):
        samples = [
            sample(1_000_000_000, iowait=0, steal=0, io_some=0, io_full=0,
                   throttled=0, available=30 * 1024**3),
            sample(11_000_000_000, iowait=100, steal=0, io_some=5_000_000,
                   io_full=1_000_000, throttled=5_000_000,
                   available=OBJECTIVE["minimum_available_bytes"] - 1),
        ]
        summary = summarize_idle(samples)
        verdict = assess_idle(summary)
        self.assertEqual(verdict["status"], "failed")
        self.assertGreaterEqual(len(verdict["failures"]), 4)
        self.assertEqual(verdict["missing"], [])

    def test_missing_linux_counters_never_passes(self):
        samples = [
            {"monotonic_ns": (second + 1) * 1_000_000_000,
             "system": {"pressure": {}, "filesystem_space": None}}
            for second in range(16)
        ]
        verdict = assess_idle(summarize_idle(samples))
        self.assertEqual(verdict["status"], "not measured")
        self.assertTrue(verdict["missing"])

    def test_storage_probe_exercises_and_cleans_publication_protocol(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            receipt = storage_probe(root)
            self.assertEqual(receipt["status"], "passed")
            self.assertEqual(receipt["bytes"], 4096)
            self.assertEqual(list(root.iterdir()), [])

    def test_sealed_profile_recomputes_from_raw_samples_and_objective(self):
        samples = quiet_samples()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "machine-profile.json", {
                "schema": 1,
                "kind": "fixed-machine-idle",
                "qualification_objective": OBJECTIVE,
                "source_digest": "0" * 64,
            })
            write_json(root / "host.json", {})
            write_json(root / "storage-probe.json", {"status": "passed"})
            (root / "idle-samples.jsonl").write_text(
                "".join(json.dumps(item, sort_keys=True) + "\n" for item in samples)
            )
            summary = summarize_idle(samples)
            write_json(root / "summary.json", summary)
            write_json(root / "verdict.json", assess_idle(summary))
            seal(root)
            checked = verify(root)
            self.assertEqual(checked["status"], "passed")
            self.assertEqual(
                checked["verdicts"]["environment_qualification"]["status"], "passed"
            )


if __name__ == "__main__":
    unittest.main()

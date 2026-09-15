from pathlib import Path
import json
import tempfile
import unittest

from benchctl.evidence import digest, seal, verify, write_json
from benchctl.machine import (
    OBJECTIVE,
    STORAGE_PROBE_SAMPLES,
    assess_idle,
    load_environment_receipt,
    storage_probe,
    storage_probe_errors,
    summarize_idle,
)
from benchctl.results import _load_machine_qualification
from unittest.mock import patch


def sample(
    at: int,
    *,
    iowait: int,
    steal: int,
    io_some: int,
    io_full: int,
    throttled: int,
    available: int,
    cpu_some: int = 0,
    memory_some: int = 0,
    memory_full: int = 0,
    hardware_throttles: int = 0,
) -> dict:
    return {
        "monotonic_ns": at,
        "system": {
            "cpu_ticks": {
                "user": at // 1_000_000_000,
                "nice": 0,
                "system": 0,
                "idle": at // 10_000_000,
                "iowait": iowait,
                "irq": 0,
                "softirq": 0,
                "steal": steal,
                "guest": 0,
                "guest_nice": 0,
            },
            "cgroup_cpu": {"usage_usec": at // 1000, "throttled_usec": throttled},
            "pressure": {
                "cpu": {"some": {"total": cpu_some}, "full": {"total": 0}},
                "io": {"some": {"total": io_some}, "full": {"total": io_full}},
                "memory": {
                    "some": {"total": memory_some},
                    "full": {"total": memory_full},
                },
            },
            "block_devices": {
                "nvme0n1": {"major": 259, "minor": 0, "writes_completed": at // 1_000_000_000}
            },
            "hardware_throttle_counts": {"cpu0/core_throttle_count": hardware_throttles},
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
        self.assertLess(summary["cpu"]["busy_percent"], 5)
        self.assertEqual(summary["hardware_throttling"]["maximum_counter_delta"], 0)
        self.assertEqual(assess_idle(summary)["status"], "passed")

    def test_pressure_and_space_fail_without_hiding_measurements(self):
        samples = [
            sample(1_000_000_000, iowait=0, steal=0, io_some=0, io_full=0,
                   throttled=0, available=30 * 1024**3),
            sample(11_000_000_000, iowait=100, steal=0, io_some=5_000_000,
                   io_full=1_000_000, throttled=5_000_000,
                   available=OBJECTIVE["minimum_available_bytes"] - 1,
                   cpu_some=5_000_000, memory_some=2_000_000,
                   memory_full=1_000_000, hardware_throttles=1),
        ]
        summary = summarize_idle(samples)
        verdict = assess_idle(summary)
        self.assertEqual(verdict["status"], "failed")
        self.assertGreaterEqual(len(verdict["failures"]), 8)
        self.assertEqual(verdict["missing"], [])

    def test_busy_cpu_fails_even_without_iowait_or_pressure(self):
        samples = quiet_samples()
        samples[-1]["system"]["cpu_ticks"]["user"] = 500
        verdict = assess_idle(summarize_idle(samples))
        self.assertEqual(verdict["status"], "failed")
        self.assertTrue(any("cpu.busy_percent" in item for item in verdict["failures"]))

    def test_missing_linux_counters_never_passes(self):
        samples = [
            {"monotonic_ns": (second + 1) * 1_000_000_000,
             "system": {"pressure": {}, "filesystem_space": None}}
            for second in range(16)
        ]
        verdict = assess_idle(summarize_idle(samples))
        self.assertEqual(verdict["status"], "not measured")
        self.assertTrue(verdict["missing"])

    def test_measured_load_receipt_checks_coverage_without_ranking_pressure(self):
        samples = quiet_samples()
        samples[-1]["system"]["pressure"]["io"]["some"]["total"] = 12_000_000
        receipt = load_environment_receipt(
            samples,
            expected_duration_seconds=15,
            nominal_sample_interval_seconds=1,
        )
        self.assertEqual(receipt["coverage"]["status"], "passed")
        self.assertGreater(
            receipt["summary"]["pressure"]["io"]["some"]["percent_of_wall"],
            50,
        )
        self.assertIn("never remove, accept, or rank", receipt["interpretation"])

        receipt = load_environment_receipt(
            samples[:10],
            expected_duration_seconds=15,
            nominal_sample_interval_seconds=1,
        )
        self.assertEqual(receipt["coverage"]["status"], "failed")
        self.assertTrue(receipt["coverage"]["failures"])

    def test_storage_probe_exercises_and_cleans_publication_protocol(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            receipt = storage_probe(root)
            self.assertEqual(receipt["status"], "passed")
            self.assertEqual(receipt["schema"], 2)
            self.assertEqual(receipt["bytes"], 4096)
            self.assertEqual(receipt["sample_count"], STORAGE_PROBE_SAMPLES)
            self.assertEqual(storage_probe_errors(receipt), [])
            self.assertEqual(list(root.iterdir()), [])

            receipt["summary"]["write_sync_ns"]["p99_ns"] += 1
            self.assertEqual(
                storage_probe_errors(receipt),
                ["machine-profile storage publication summary differs from raw samples"],
            )

    def test_storage_probe_receipt_rejects_changed_protocol_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            receipt = storage_probe(Path(tmp))

        receipt["sample_count"] -= 1
        self.assertEqual(
            storage_probe_errors(receipt),
            ["machine-profile storage publication sample accounting differs"],
        )
        receipt["sample_count"] = STORAGE_PROBE_SAMPLES
        receipt["bytes"] -= 1
        self.assertEqual(
            storage_probe_errors(receipt),
            ["machine-profile storage publication payload size differs"],
        )
        receipt["bytes"] = 4096
        receipt["timings"] = dict(receipt["timings"])
        receipt["timings"]["write_sync_ns"] += 1
        self.assertEqual(
            storage_probe_errors(receipt),
            ["machine-profile storage publication compatibility timing differs"],
        )

    def test_sealed_profile_recomputes_from_raw_samples_and_objective(self):
        samples = quiet_samples()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / "machine-profile.json", {
                "schema": 2,
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

    def test_schema_one_profiles_replay_without_new_fields(self):
        samples = quiet_samples()
        summary = summarize_idle(samples, schema=1)
        self.assertNotIn("hardware_throttling", summary)
        legacy_objective = {
            name: value
            for name, value in OBJECTIVE.items()
            if name
            not in {
                "maximum_cpu_busy_percent",
                "maximum_cpu_pressure_some_percent",
                "maximum_memory_pressure_some_percent",
                "maximum_memory_pressure_full_percent",
                "maximum_hardware_throttle_counter_delta",
            }
        }
        self.assertEqual(assess_idle(summary, legacy_objective)["status"], "passed")

    def test_suite_machine_qualification_replays_identity_and_seal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            profile = root / "machine-profile"
            profile.mkdir()
            (profile / "SHA256SUMS.json").write_text("profile seal\n")
            checked = {"status": "passed", "verdicts": {"environment_qualification": {"status": "passed"}}}
            receipt = {
                "schema": 1,
                "status": "passed",
                "profile": "machine-profile",
                "seal_sha256": digest(profile / "SHA256SUMS.json"),
                "data_root": "/benchmark-data",
                "verification": checked["verdicts"],
            }
            write_json(root / "machine-qualification.json", receipt)
            suite = {"data_root": "/benchmark-data/case-data", "machine_qualification": receipt}
            with patch("benchctl.results.verify", return_value=checked):
                observed, errors = _load_machine_qualification(root, suite)
            self.assertEqual(observed, receipt)
            self.assertEqual(errors, [])

            changed = dict(receipt, seal_sha256="0" * 64)
            write_json(root / "replacement.json", changed)
            suite["machine_qualification"] = changed
            (root / "machine-qualification.json").unlink()
            (root / "replacement.json").rename(root / "machine-qualification.json")
            with patch("benchctl.results.verify", return_value=checked):
                _, errors = _load_machine_qualification(root, suite)
            self.assertIn("machine profile seal digest differs", errors)


if __name__ == "__main__":
    unittest.main()

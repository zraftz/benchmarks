from pathlib import Path
from types import SimpleNamespace
import sys
import unittest

from benchctl.cli import (
    CAPACITY_RUNS,
    CAPACITY_RATES,
    PIPELINE_THRESHOLD_RUNS,
    PIPELINE_THRESHOLD_RATES,
    capacity_command,
    pipeline_threshold_command,
)


class CapacityCommandTests(unittest.TestCase):
    def options(self, sha="a" * 40):
        return SimpleNamespace(
            rafter_sha=sha,
            data_root=Path("/benchmark-data"),
            output=Path("/evidence/capacity"),
            rates=CAPACITY_RATES,
            diagnostic_rates="1000",
        )

    def test_predeclared_curve_uses_same_exact_sha_and_qualified_machine(self):
        command = capacity_command(self.options())
        self.assertEqual(command[:3], [sys.executable, "-m", "benchctl.paired"])
        self.assertEqual(command[command.index("--prior") + 1], "a" * 40)
        self.assertEqual(command[command.index("--candidate") + 1], "a" * 40)
        self.assertEqual(command[command.index("--rates") + 1], CAPACITY_RATES)
        self.assertEqual(int(command[command.index("--runs") + 1]), CAPACITY_RUNS)
        self.assertIn("--candidate-durable-completion-priority", command)
        self.assertIn("--qualify-machine", command)
        self.assertEqual(command[command.index("--network-delays-ms") + 1], "0")
        self.assertEqual(
            command[command.index("--openraft-controls") + 1], "synchronous,async"
        )

    def test_capacity_refuses_a_moving_or_abbreviated_ref(self):
        for value in ("main", "abc123", "g" * 40):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "exact 40-character"):
                    capacity_command(self.options(value))

    def test_threshold_sweep_changes_only_the_speculative_limit(self):
        options = self.options()
        options.rates = PIPELINE_THRESHOLD_RATES
        options.diagnostic_rates = PIPELINE_THRESHOLD_RATES
        command = pipeline_threshold_command(options)
        self.assertEqual(command[command.index("--prior") + 1], "a" * 40)
        self.assertEqual(command[command.index("--candidate") + 1], "a" * 40)
        self.assertEqual(
            command[command.index("--candidate-max-speculative-proposals") + 1],
            "1,2,4,8",
        )
        self.assertEqual(
            command[command.index("--prior-max-speculative-proposals") + 1], "1"
        )
        self.assertIn("--prior-durable-completion-priority", command)
        self.assertIn("--candidate-durable-completion-priority", command)
        self.assertEqual(command[command.index("--rates") + 1], "0,1000")
        self.assertEqual(
            int(command[command.index("--runs") + 1]), PIPELINE_THRESHOLD_RUNS
        )
        self.assertEqual(command[command.index("--diagnostic-rates") + 1], "0,1000")
        self.assertEqual(command[command.index("--network-delays-ms") + 1], "0")
        self.assertIn("--qualify-machine", command)

    def test_threshold_sweep_refuses_a_moving_or_abbreviated_ref(self):
        for value in ("main", "abc123", "g" * 40):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "exact 40-character"):
                    pipeline_threshold_command(self.options(value))


if __name__ == "__main__":
    unittest.main()

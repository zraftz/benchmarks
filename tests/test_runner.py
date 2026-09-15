import unittest
from types import SimpleNamespace

from benchctl.runner import qualification_timeout_seconds


class RunnerTests(unittest.TestCase):
    def test_tiny_interval_snapshot_smoke_has_a_functional_qualification_deadline(self):
        options = SimpleNamespace(smoke=True, snapshot_interval_entries=8)
        self.assertEqual(qualification_timeout_seconds(options), 15)

    def test_timing_and_non_snapshot_smokes_keep_the_original_deadline(self):
        self.assertEqual(
            qualification_timeout_seconds(
                SimpleNamespace(smoke=False, snapshot_interval_entries=8)
            ),
            5,
        )
        self.assertEqual(
            qualification_timeout_seconds(
                SimpleNamespace(smoke=True, snapshot_interval_entries=0)
            ),
            5,
        )


if __name__ == "__main__":
    unittest.main()

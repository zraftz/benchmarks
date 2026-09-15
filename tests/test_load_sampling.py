import io
import json
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from benchctl.load_sampling import LoadSampler


class LoadSamplingTests(unittest.TestCase):
    def test_sampling_continues_while_fault_controller_is_blocked(self):
        stream = io.StringIO()
        processes = {1: SimpleNamespace(pid=11), 2: SimpleNamespace(pid=12)}
        start = time.monotonic()
        sampler = LoadSampler(
            stream,
            start=start,
            interval_seconds=0.01,
            processes=lambda: dict(processes),
            load_pid=99,
            data_path=Path("/tmp"),
            process_sample=lambda pid: {"pid": pid},
            host_sample=lambda **_: {"ok": True},
        )
        sampler.start()
        time.sleep(0.065)
        processes[2] = SimpleNamespace(pid=22)
        time.sleep(0.025)
        sampler.stop()

        samples = [json.loads(line) for line in stream.getvalue().splitlines()]
        self.assertGreaterEqual(len(samples), 8)
        self.assertTrue(all(sample["load_generator"] == {"pid": 99} for sample in samples))
        self.assertTrue(any(sample["nodes"]["2"] == {"pid": 22} for sample in samples))
        times = [sample["monotonic_ns"] for sample in samples]
        self.assertEqual(times, sorted(times))


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import patch
from benchctl.network import loopback_delay


class NetworkTests(unittest.TestCase):
    def test_zero_delay_does_not_modify_network(self):
        with patch("benchctl.network.subprocess.run") as run:
            with loopback_delay(0) as evidence:
                self.assertEqual(evidence["delay_ms_each_egress"], 0)
            run.assert_not_called()

    def test_existing_qdisc_is_never_replaced(self):
        with patch("benchctl.network.sys.platform", "linux"), \
             patch("benchctl.network.subprocess.check_output", return_value='[{"kind":"fq_codel"}]'), \
             patch("benchctl.network.subprocess.run") as run:
            with self.assertRaisesRegex(RuntimeError, "refusing"):
                with loopback_delay(2):
                    self.fail("must not enter experiment")
            run.assert_not_called()

    def test_failure_restores_network_after_recording_configuration(self):
        outputs = ['[{"kind":"noqueue"}]', '[{"kind":"netem","handle":"1:"}]', 'round-trip min/avg/max = 4/4/4 ms']
        with patch("benchctl.network.sys.platform", "linux"), \
             patch("benchctl.network.subprocess.check_output", side_effect=outputs), \
             patch("benchctl.network.subprocess.run") as run:
            with self.assertRaisesRegex(RuntimeError, "case failed"):
                with loopback_delay(2) as evidence:
                    self.assertEqual(evidence["qdisc"][0]["kind"], "netem")
                    raise RuntimeError("case failed")
            self.assertEqual(run.call_args_list[0].args[0],
                ["sudo", "-n", "tc", "qdisc", "add", "dev", "lo", "root", "handle", "1:", "netem", "delay", "2ms"])
            self.assertEqual(run.call_args_list[-1].args[0],
                ["sudo", "-n", "tc", "qdisc", "del", "dev", "lo", "root"])

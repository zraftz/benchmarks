import unittest
from unittest.mock import patch

from benchctl import protocol


class ProtocolTests(unittest.TestCase):
    def test_complete_statuses_retries_a_partial_observation(self):
        nodes = {1: "node-1", 2: "node-2", 3: "node-3"}
        complete = {
            node: {"status": "ok", "info": {"node_id": node}}
            for node in nodes
        }
        with patch.object(protocol, "statuses", side_effect=[{1: complete[1]}, complete]) as call:
            self.assertEqual(protocol.complete_statuses(nodes, timeout=1), complete)
        self.assertEqual(call.call_count, 2)


if __name__ == "__main__":
    unittest.main()

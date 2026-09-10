import unittest
from benchctl.checker import check_history

def event(client, sequence, kind, start, end, value=None, expected=None, observed=None, swapped=None, key="k"):
    return {"command": {"client": client, "sequence": sequence, "kind": kind, "key": key,
            "value": value or "", "expected": expected}, "start_ns": start, "end_ns": end,
            "reply": {"status": "ok", "result": {"value": observed, "swapped": swapped, "error": None}}}

class CheckerTests(unittest.TestCase):
    def test_sequential_put_read(self):
        h = [event("a",1,"put",0,10,"v",observed="v"), event("b",1,"get",11,20,observed="v")]
        self.assertEqual(check_history(h)["status"], "passed")
    def test_stale_read_fails(self):
        h = [event("a",1,"put",0,10,"v",observed="v"), event("b",1,"get",11,20,observed=None)]
        self.assertEqual(check_history(h)["status"], "failed")
    def test_overlapping_read_can_come_first(self):
        h = [event("a",1,"put",0,10,"v",observed="v"), event("b",1,"get",1,9,observed=None)]
        self.assertEqual(check_history(h)["status"], "passed")
    def test_cas(self):
        h = [event("a",1,"cas",0,10,"v",observed="v",swapped=True), event("b",1,"cas",11,20,"z",observed="v",swapped=False)]
        self.assertEqual(check_history(h)["status"], "passed")
    def test_unknown_is_not_a_pass(self):
        e = event("a",1,"put",0,10,"v",observed="v"); e["reply"]["status"]="unknown"
        self.assertEqual(check_history([e])["status"], "inconclusive")
    def test_duplicate_identity(self):
        e = event("a",1,"put",0,10,"v",observed="v")
        self.assertEqual(check_history([e,e])["status"], "invalid")
    def test_session_overlap(self):
        h=[event("a",1,"put",0,10,"v",observed="v"),event("a",2,"get",1,20,observed="v")]
        self.assertEqual(check_history(h)["status"], "invalid")
    def test_empty(self):
        self.assertEqual(check_history([])["status"], "inconclusive")
    def test_resource_limit(self):
        e=event("a",1,"put",0,10,"v",observed="v")
        self.assertEqual(check_history([e],max_states=0)["status"], "inconclusive")
    def test_initial_state(self):
        self.assertEqual(check_history([event("a",1,"get",0,10,observed="v")], initial={"k":"v"})["status"], "passed")
    def test_invalid_interval(self):
        self.assertEqual(check_history([event("a",1,"get",10,1)])["status"], "invalid")
    def test_independent_keys(self):
        h=[event("a",1,"put",0,10,"x",observed="x",key="x"), event("b",1,"put",0,10,"y",observed="y",key="y")]
        self.assertEqual(check_history(h)["keys"],2)

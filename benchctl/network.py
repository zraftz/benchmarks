"""Explicit, temporary loopback delay for an otherwise idle Linux benchmark runner."""
from contextlib import contextmanager
import json
import subprocess
import sys


@contextmanager
def loopback_delay(milliseconds: int):
    if milliseconds == 0:
        yield {"delay_ms_each_egress": 0, "scope": "unmodified loopback"}
        return
    if sys.platform != "linux" or not 1 <= milliseconds <= 100:
        raise ValueError("nonzero loopback delay requires Linux and 1..100 milliseconds")
    prior = json.loads(subprocess.check_output(["tc", "-j", "qdisc", "show", "dev", "lo"]))
    if any(item.get("kind") != "noqueue" for item in prior):
        raise RuntimeError("refusing to replace existing loopback traffic control")
    subprocess.run(["sudo", "-n", "tc", "qdisc", "add", "dev", "lo", "root", "handle", "1:",
        "netem", "delay", f"{milliseconds}ms"], check=True)
    try:
        configured = json.loads(subprocess.check_output(["tc", "-j", "qdisc", "show", "dev", "lo"]))
        ping = subprocess.check_output(["ping", "-n", "-c", "5", "-i", "0.2", "127.0.0.1"], text=True)
        yield {"delay_ms_each_egress": milliseconds, "scope": "all loopback TCP, including clients and peers",
               "qdisc": configured, "ping": ping}
    finally:
        subprocess.run(["sudo", "-n", "tc", "qdisc", "del", "dev", "lo", "root"], check=True)

"""Manage only child processes launched by this run; never kill by name/port."""
from __future__ import annotations
from pathlib import Path
import json
import os
import signal
import socket
import subprocess
import threading
import time
from typing import Any
from . import protocol
from .evidence import write_json, CONTRACT


class Cluster:
    def __init__(self, implementation: str, command: list[str], directory: Path,
                 data: Path, cluster_id: str, batch_size: int = 64, peer_batch_size: int = 1,
                 diagnostics: bool = False, ordered_apply: bool = False,
                 peer_message_stream: bool = False, pipelined_durability: bool = False,
                 max_speculative_proposals: int = 1, combine_peer_proposals: bool = False,
                 max_inflight_appends: int = 8, durable_completion_priority: bool = False,
                 openraft_async_flush: bool = False, snapshot_interval_entries: int = 0):
        self.implementation, self.command = implementation, command
        self.directory, self.data = directory, data
        self.processes: dict[int, subprocess.Popen] = {}
        self._processes_lock = threading.Lock()
        self.logs: list[Any] = []
        self.paused: set[int] = set()
        self.configs: dict[int, Path] = {}
        self.nodes: dict[int, str] = {}
        directory.mkdir(parents=True, exist_ok=True)
        data.mkdir(parents=True, exist_ok=True)
        reserved = []
        try:
            for _ in range(6):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                reserved.append(sock)
            ports = [s.getsockname()[1] for s in reserved]
            peers = {i: f"127.0.0.1:{ports[i + 2]}" for i in (1, 2, 3)}
            for node in (1, 2, 3):
                self.nodes[node] = f"127.0.0.1:{ports[node - 1]}"
                self.configs[node] = directory / f"node-{node}.json"
                write_json(self.configs[node], {"id": node, "cluster": cluster_id,
                    "client": self.nodes[node], "peer": peers[node], "peers": peers,
                    "data_dir": str((data / f"node-{node}").resolve()),
                    "tick_ms": 20, "capacity": 4096, "batch_size": batch_size,
                    "peer_batch_size": peer_batch_size, "diagnostics": diagnostics, "ordered_apply": ordered_apply,
                    "peer_message_stream": peer_message_stream, "pipelined_durability": pipelined_durability,
                    "max_speculative_proposals": max_speculative_proposals,
                    "combine_peer_proposals": combine_peer_proposals,
                    "max_inflight_appends": max_inflight_appends,
                    "durable_completion_priority": durable_completion_priority,
                    "openraft_async_flush": openraft_async_flush,
                    "snapshot_interval_entries": snapshot_interval_entries})
        finally:
            for sock in reserved:
                sock.close()
        # Port reservations cannot be handed to these adapters. A concurrent bind
        # can still win this small race; startup fails rather than attaching to it.

    def start_node(self, node: int) -> None:
        with self._processes_lock:
            current = self.processes.get(node)
        if current is not None and current.poll() is None:
            raise RuntimeError("node already running")
        generation = len(list(self.directory.glob(f"node-{node}-*.log")))
        log = (self.directory / f"node-{node}-{generation}.log").open("xb")
        self.logs.append(log)
        process = subprocess.Popen(self.command + ["--config", str(self.configs[node])],
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        with self._processes_lock:
            self.processes[node] = process

    def process_snapshot(self) -> dict[int, subprocess.Popen]:
        with self._processes_lock:
            return dict(self.processes)

    def start(self, *, initialize: bool = True) -> None:
        for node in self.nodes:
            self.start_node(node)
        self.wait_ready()
        if initialize and self.implementation == "openraft":
            result = protocol.request(self.nodes[1], {"op": "initialize"}, timeout=15)
            if result.get("status") != "ok":
                raise RuntimeError(f"OpenRaft initialization failed: {result}")
        protocol.leader(self.nodes)

    def wait_ready(self) -> None:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            for node, process in self.processes.items():
                if process.poll() is not None:
                    raise RuntimeError(f"node {node} exited with {process.returncode}; inspect {self.directory}")
            observed = protocol.statuses(self.nodes)
            if len(observed) == 3:
                for node, status in observed.items():
                    info = status.get("info", {})
                    if status.get("status") != "ok" or info.get("implementation") != self.implementation or info.get("node_id") != node or info.get("contract") != CONTRACT:
                        raise RuntimeError("unexpected node identity/semantic contract on reserved port")
                return
            time.sleep(.05)
        raise TimeoutError("cluster did not become ready")

    def stop_node(self, node: int, *, kill: bool = True) -> None:
        process = self.processes[node]
        if process.poll() is not None:
            return
        if node in self.paused:
            process.send_signal(signal.SIGCONT)
            self.paused.discard(node)
        if kill:
            process.kill()
        else:
            process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    def stop_all(self) -> None:
        # Issue all kills before waiting: no orderly application shutdown or drain.
        for node, process in self.processes.items():
            if process.poll() is None:
                if node in self.paused:
                    process.send_signal(signal.SIGCONT)
                    self.paused.discard(node)
                process.kill()
        for process in self.processes.values():
            process.wait(timeout=5)

    def pause(self, node: int) -> None:
        process = self.processes[node]
        if process.poll() is not None:
            raise RuntimeError("cannot pause exited node")
        process.send_signal(signal.SIGSTOP)
        self.paused.add(node)

    def resume(self, node: int) -> None:
        if node in self.paused:
            self.processes[node].send_signal(signal.SIGCONT)
            self.paused.remove(node)

    def close(self) -> None:
        try:
            for node in list(self.processes):
                self.stop_node(node, kill=False)
        finally:
            for log in self.logs:
                log.close()

"""Independent measured-load sampling, isolated from fault-controller stalls."""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Callable, TextIO

from .evidence import proc_sample, system_sample


class LoadSampler:
    def __init__(
        self,
        stream: TextIO,
        *,
        start: float,
        interval_seconds: float,
        processes: Callable[[], dict[int, Any]],
        load_pid: int,
        data_path: Path,
        process_sample: Callable[[int], dict] = proc_sample,
        host_sample: Callable[..., dict] = system_sample,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("load sampling interval must be positive")
        self._stream = stream
        self._start = start
        self._interval = interval_seconds
        self._processes = processes
        self._load_pid = load_pid
        self._data_path = data_path
        self._process_sample = process_sample
        self._host_sample = host_sample
        self._stop = threading.Event()
        self._error: BaseException | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="raft-bench-load-sampler",
            daemon=False,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=30)
        if self._thread.is_alive():
            raise TimeoutError("measured-load sampler did not stop")
        if self._error is not None:
            raise RuntimeError("measured-load sampler failed") from self._error

    def _sample(self) -> None:
        observed = {
            "controller_seconds": time.monotonic() - self._start,
            "monotonic_ns": time.monotonic_ns(),
            "nodes": {
                node: self._process_sample(process.pid)
                for node, process in self._processes().items()
            },
            "load_generator": self._process_sample(self._load_pid),
            "system": self._host_sample(data_path=self._data_path),
        }
        self._stream.write(json.dumps(observed) + "\n")
        self._stream.flush()

    def _run(self) -> None:
        try:
            deadline = time.monotonic()
            while not self._stop.is_set():
                self._sample()
                deadline += self._interval
                self._stop.wait(max(0.0, deadline - time.monotonic()))
            self._sample()
        except BaseException as error:
            self._error = error

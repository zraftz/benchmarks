"""Fail-closed toolchain preflight for benchmark hosts."""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tomllib

from .evidence import ROOT


def _version(text: str, pattern: str) -> tuple[int, ...] | None:
    match = re.search(pattern, text)
    return tuple(int(value) for value in match.groups()) if match else None


def requirements() -> dict[str, object]:
    toolchain = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())
    rust = toolchain["toolchain"]["channel"]
    go_match = re.search(r"^go\s+(\d+)\.(\d+)", (ROOT / "loadgen/go.mod").read_text(), re.MULTILINE)
    if go_match is None:
        raise ValueError("loadgen/go.mod does not declare a Go version")
    return {
        "rustc": rust,
        "cargo": rust,
        "go": tuple(int(value) for value in go_match.groups()),
        "python3": (3, 11),
    }


def assess(observed: dict[str, dict], required: dict[str, object]) -> dict:
    checks = {}
    for name in ("rustc", "cargo", "go", "python3", "protoc", "git", "cc"):
        item = observed.get(name, {})
        status = "passed"
        detail = "available"
        if not item.get("path") or item.get("returncode") != 0:
            status, detail = "failed", "missing or not executable"
        elif name in ("rustc", "cargo"):
            actual = _version(item.get("version", ""), rf"{name}\s+(\d+)\.(\d+)\.(\d+)")
            expected = tuple(int(value) for value in str(required[name]).split("."))
            if actual != expected:
                status, detail = "failed", f"requires exactly {required[name]}"
            else:
                detail = f"exact {required[name]}"
        elif name == "go":
            actual = _version(item.get("version", ""), r"go version go(\d+)\.(\d+)(?:\.\d+)?")
            minimum = required[name]
            if actual is None or actual < minimum:
                status, detail = "failed", f"requires at least {minimum[0]}.{minimum[1]}"
            else:
                detail = f"at least {minimum[0]}.{minimum[1]}"
        elif name == "python3":
            actual = _version(item.get("version", ""), r"Python\s+(\d+)\.(\d+)(?:\.\d+)?")
            minimum = required[name]
            if actual is None or actual < minimum:
                status, detail = "failed", f"requires at least {minimum[0]}.{minimum[1]}"
            else:
                detail = f"at least {minimum[0]}.{minimum[1]}"
        checks[name] = {**item, "status": status, "requirement": detail}
    return {
        "schema": 1,
        "status": "passed" if all(check["status"] == "passed" for check in checks.values()) else "failed",
        "tools": checks,
    }


def inspect() -> dict:
    commands = {
        "rustc": ["rustc", "--version"],
        "cargo": ["cargo", "--version"],
        "go": ["go", "version"],
        "python3": [sys.executable, "--version"],
        "protoc": ["protoc", "--version"],
        "git": ["git", "--version"],
        "cc": ["cc", "--version"],
    }
    observed = {}
    for name, command in commands.items():
        path = sys.executable if name == "python3" else shutil.which(command[0])
        if path is None:
            observed[name] = {"path": None, "version": None, "returncode": None}
            continue
        completed = subprocess.run(
            [path, *command[1:]], capture_output=True, text=True, timeout=10, check=False
        )
        output = (completed.stdout or completed.stderr).strip().splitlines()
        observed[name] = {
            "path": path,
            "version": output[0] if output else None,
            "returncode": completed.returncode,
        }
    return assess(observed, requirements())

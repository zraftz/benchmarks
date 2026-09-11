"""Locked builds and receipts for the exact binaries run by the supervisor."""
from pathlib import Path
import json
import os
import shutil
import subprocess
import uuid

from .evidence import ROOT, capture, digest, source_digest, write_json
from .selection import check_resolution

IMPLEMENTATIONS = ("rafter", "raft-rs", "openraft")


def build(rafter_hard_state: str = "replace", peer_group_commit: bool = False, ordered_apply: bool = False) -> None:
    dist = ROOT / "dist"
    dist.mkdir(exist_ok=True)
    # A failed rebuild must never leave an earlier receipt usable.
    (dist / "build.json").unlink(missing_ok=True)
    if rafter_hard_state not in ("replace", "journal"):
        raise ValueError("rafter hard state must be replace or journal")
    selected = ["raft-bench-rafter/journal-hard-state"] if rafter_hard_state == "journal" else []
    if peer_group_commit:
        selected.append("raft-bench-rafter/peer-group-commit")
    if ordered_apply:
        selected.append("raft-bench-rafter/ordered-apply")
    features = ["--features", ",".join(selected)] if selected else []
    if not (ROOT / "Cargo.lock").is_file():
        raise RuntimeError("Cargo.lock is missing; restore the checked-in lockfile before building")
    pins = json.loads((ROOT / "implementations.lock.json").read_text())
    check_resolution(ROOT, pins["rafter"]["rev"])
    for tool in ("cargo", "rustc", "go", "protoc", "git"):
        if not shutil.which(tool):
            raise RuntimeError(f"{tool} is missing; see README prerequisites")
    with (dist / "rust-tests.log").open("w") as log:
        try:
            subprocess.run(["cargo", "test", "--workspace", "--locked", *features], cwd=ROOT,
                           stdout=log, stderr=subprocess.STDOUT, check=True)
        except subprocess.CalledProcessError as exc:
            raise RuntimeError("Rust tests failed; inspect dist/rust-tests.log") from exc
    subprocess.run(["cargo", "build", "--workspace", "--release", "--locked", *features], cwd=ROOT, check=True)
    # Cargo resolves config files and CARGO_TARGET_DIR; do not guess its output path.
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"], cwd=ROOT))
    release = Path(metadata["target_directory"]) / "release"
    for name in IMPLEMENTATIONS:
        shutil.copy2(release / f"raft-bench-{name}", dist / f"raft-bench-{name}")
    subprocess.run(["go", "test", "-race", "./..."], cwd=ROOT / "loadgen", check=True)
    subprocess.run(["go", "build", "-trimpath", "-o", str(dist / "raft-bench-load"), "."],
                   cwd=ROOT / "loadgen", check=True)
    record = {
        "schema": 1, "peer_group_commit": peer_group_commit, "ordered_apply": ordered_apply, "rafter_hard_state_backend": rafter_hard_state, "source_digest": source_digest(), "implementations": pins,
        "cargo_lock_sha256": digest(ROOT / "Cargo.lock"), "rust_tests_passed": True,
        "rustc": capture(["rustc", "-Vv"]), "cargo": capture(["cargo", "-V"]),
        "go": capture(["go", "version"]), "protoc": capture(["protoc", "--version"]),
        "binaries": {name: digest(dist / f"raft-bench-{name}") for name in IMPLEMENTATIONS},
        "loadgen": digest(dist / "raft-bench-load"),
        "environment": {key: os.environ.get(key) for key in (
            "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "CC", "CFLAGS",
            "CARGO_TARGET_DIR", "GOFLAGS", "GOMAXPROCS")},
    }
    temp = dist / f"build-{uuid.uuid4().hex}.json"
    write_json(temp, record)
    os.replace(temp, dist / "build.json")
    print("Build and Rust unit-test receipt: dist/build.json")

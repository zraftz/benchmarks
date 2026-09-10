"""Lossless, SHA-verified extraction of the original protocol benchmark sources.

The ZIP does not contain a second copy of Rafter. This importer reads exact Git
blobs, preserves the original code and result files, then changes only Rafter
Cargo dependency locations. It never edits the user's Rafter repository.
"""
from __future__ import annotations
from pathlib import Path
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import uuid
from .evidence import ROOT, capture, digest, host_info, write_json

REPOSITORY = "https://github.com/zsumz/rafter"
REVISION = "518aefd767dc0ef2c1f1ccc5f970c96456cfd109"
EXPECTED = {
    "bench-compare/Cargo.toml": "183a967ddb1bb847c07e7d7d5c832f995188405b",
    "bench-compare/Cargo.lock": "a9dacd9f8c826f7ef360047f66efc6aa6e7d5fd7",
    "bench-compare/METHODOLOGY.md": "bf1de9e4bf0b0de904b6ff6fd8230cb56036f446",
    "bench-compare/src/lib.rs": "8e47674cbe05bca8cfe188d8cf4a14c92a4c0c83",
    "bench-compare/src/bin/bench-rafter.rs": "3bcb5b71251cf0ab2a3ca98bd15565a96cd9b338",
    "bench-compare/src/bin/bench-raft-rs.rs": "1f30b7a13427346504119d55dc88a93943e7210c",
    "bench-compare/src/bin/bench-openraft.rs": "a264ebc664c215586765fb4108a2e19d16dfcb1c",
    "bench-compare/src/bin/bench-rafter-codec.rs": "3362893f22295cb75c53e25129cc965bf55c0aa0",
    "bench-compare/src/bin/bench-rafter-multiraft.rs": "1fa9e24f836348aad968f25e9c41a4cd89b3aa92",
    "bench-compare/src/bin/bench-rafter-profile.rs": "ba1feb37406338dee5a3dc27cd2f511f9136b125",
    "bench-compare/src/bin/bench-rafter-service.rs": "20fc2dcf6517f2915f4099db7b008ce1baef7e62",
    "bench-compare/src/bin/check-transport-receive-memory.rs": "3a3818da392623052a823292b2c88c5cda6c7482",
    "bench-compare/results/latest.json": "9a447d4d24730f616cea63693a5faf3f9df0bfb4",
    "bench-compare/results/rafter-only.json": "02fa53ba354f53806d76641089bf792a948a8954",
    "scripts/bench-compare.sh": "f9f758f2f1074f0ebd199f35eabdce29b898503d",
}


def blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def rewrite_dependencies(text: str) -> str:
    pattern = r'path\s*=\s*"\.\./crates/[^"\n]+"'
    changed, count = re.subn(pattern, f'git = "{REPOSITORY}", rev = "{REVISION}"', text)
    if count != 8:
        raise ValueError(f"expected eight Rafter dependencies, found {count}; review source changes")
    return changed


def verify_import(directory: Path) -> None:
    receipt = json.loads((directory / "IMPORT.json").read_text())
    for relative, sha in receipt["extracted_sha256"].items():
        p = directory / relative
        if p.is_symlink() or not p.is_file() or digest(p) != sha:
            raise ValueError(f"imported source changed: {relative}; refusing historical provenance claim")


def import_protocol() -> Path:
    destination = ROOT / "protocol/upstream"
    if destination.exists():
        verify_import(destination)
        return destination
    for tool in ("git", "cargo"):
        if not shutil.which(tool):
            raise RuntimeError(f"{tool} is needed for the exact-source import")
    cache = ROOT / ".cache/rafter-import.git"
    cache.parent.mkdir(parents=True, exist_ok=True)
    if not cache.exists():
        subprocess.run(["git", "init", "--bare", str(cache)], check=True)
    def git(*args: str) -> bytes:
        return subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "--git-dir", str(cache), *args],
                              check=True, capture_output=True).stdout
    git("fetch", "--depth=1", REPOSITORY, REVISION)
    actual = git("rev-parse", "FETCH_HEAD").decode().strip()
    if actual != REVISION:
        raise ValueError("fetched revision does not match pin")
    listed = git("ls-tree", "-r", "--name-only", REVISION, "bench-compare").decode().splitlines()
    paths = sorted(set(listed + ["scripts/bench-compare.sh", "LICENSE", "NOTICE"]))
    with tempfile.TemporaryDirectory(prefix="protocol-import-", dir=ROOT / ".cache") as temporary:
        staging = Path(temporary) / "upstream"
        staging.mkdir()
        originals = {}
        for relative in paths:
            if Path(relative).is_absolute() or ".." in Path(relative).parts:
                raise ValueError("unsafe upstream path")
            data = git("show", f"{REVISION}:{relative}")
            sha = blob_sha(data)
            if relative in EXPECTED and sha != EXPECTED[relative]:
                raise ValueError(f"upstream blob mismatch: {relative}")
            p = staging / relative
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
            originals[relative] = sha
        if set(EXPECTED) - set(originals):
            raise ValueError("upstream extraction is missing an expected artifact")
        manifest = staging / "bench-compare/Cargo.toml"
        original = manifest.read_text()
        (staging / "Cargo.toml.original").write_text(original)
        manifest.write_text(rewrite_dependencies(original))
        original_lock = staging / "bench-compare/Cargo.lock"
        shutil.copyfile(original_lock, staging / "Cargo.lock.original")
        # Rafter path packages become Git source packages. Resolve that source-ID
        # change explicitly; retain the old lock and record both. No invented lockfile.
        subprocess.run(["cargo", "generate-lockfile", "--manifest-path", str(manifest)], cwd=ROOT, check=True)
        receipt = {"schema": 1, "repository": REPOSITORY, "revision": REVISION,
                   "original_git_blobs": originals,
                   "changes": ["eight Cargo path dependencies replaced with exact Git revision", "Cargo.lock regenerated for Git source IDs; original retained"],
                   "extracted_sha256": {p.relative_to(staging).as_posix(): digest(p) for p in staging.rglob("*") if p.is_file()}}
        write_json(staging / "IMPORT.json", receipt)
        staging.rename(destination)
    print(f"Imported exact protocol sources: {destination}")
    return destination


def run_protocol(mode: str, runs: int) -> None:
    if not 1 <= runs <= 100:
        raise ValueError("runs must be 1..100")
    source = import_protocol()
    directory = ROOT / "protocol/runs" / uuid.uuid4().hex
    directory.mkdir(parents=True, exist_ok=False)
    environment = dict(os.environ, BENCH_COMPARE_MODE=mode, BENCH_COMPARE_RUNS=str(runs), OUT=str(directory / "report.json"))
    environment.pop("CARGO_TARGET_DIR", None)
    if environment.get("CARGO_BUILD_TARGET"):
        raise ValueError("legacy runner expects a native build; unset CARGO_BUILD_TARGET")
    with (directory / "run.log").open("xb") as log:
        subprocess.run(["bash", str(source / "scripts/bench-compare.sh")], cwd=source,
                       env=environment, stdout=log, stderr=subprocess.STDOUT, check=True)
    write_json(directory / "provenance.json", {"kind": "historical-protocol-harness",
        "import": json.loads((source / "IMPORT.json").read_text()), "host": host_info(directory),
        "compiler": capture(["rustc", "-Vv"]), "mode": mode, "runs": runs,
        "warning": "Legacy report version strings and completion boundaries are retained; Rafter source is the exact revision in IMPORT.json, not current HEAD. No durability or network-performance claim."})
    print(f"Protocol report: {directory}. Historical input results were not overwritten.")

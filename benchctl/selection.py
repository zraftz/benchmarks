"""Resolve a Rafter ref once and update both locked benchmark workspaces."""
from pathlib import Path
import json
import re
import shutil
import subprocess
import tempfile
import tomllib

from .evidence import ROOT

REPOSITORY = "https://github.com/zsumz/rafter"
MANIFESTS = ("adapters/rafter/Cargo.toml", "crates/common/Cargo.toml", "microbench/Cargo.toml")
LOCKS = ("Cargo.lock", "microbench/Cargo.lock")


def resolve_ref(ref: str, cache: Path) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,199}", ref) or ".." in ref or "//" in ref:
        raise ValueError("Rafter ref must be a branch, tag, or full commit SHA")
    cache.parent.mkdir(parents=True, exist_ok=True)
    if not cache.exists():
        subprocess.run(["git", "init", "--bare", str(cache)], check=True, capture_output=True)
    git = ["git", "-c", "core.hooksPath=/dev/null", "--git-dir", str(cache)]
    subprocess.run([*git, "fetch", "--depth=1", "--no-tags", REPOSITORY, ref], check=True, timeout=180)
    sha = subprocess.check_output([*git, "rev-parse", "FETCH_HEAD^{commit}"], text=True).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("Rafter ref did not resolve to a full commit SHA")
    if re.fullmatch(r"[0-9a-fA-F]{40}", ref) and sha != ref.lower():
        raise ValueError("fetched Rafter commit does not match the requested SHA")
    return sha


def rewrite_manifest(content: str, sha: str) -> str:
    lines = []
    count = 0
    for line in content.splitlines(keepends=True):
        if re.match(r"^rafter(?:-[\w-]+)?\s*=", line):
            if f'git = "{REPOSITORY}"' not in line:
                raise ValueError("unexpected Rafter dependency source")
            line, changed = re.subn(r'rev = "[0-9a-f]{40}"', f'rev = "{sha}"', line)
            if changed != 1:
                raise ValueError("Rafter dependencies must use exact commit pins")
            count += 1
        lines.append(line)
    if count == 0:
        raise ValueError("manifest has no Rafter dependencies")
    return "".join(lines)


def check_resolution(root: Path, sha: str) -> None:
    for name in MANIFESTS:
        manifest = tomllib.loads((root / name).read_text())
        for package, spec in manifest["dependencies"].items():
            if package == "rafter" or package.startswith("rafter-"):
                if spec.get("git") != REPOSITORY or spec.get("rev") != sha:
                    raise ValueError(f"Rafter manifest identity mismatch: {name}: {package}")
    for name in LOCKS:
        packages = tomllib.loads((root / name).read_text())["package"]
        rafter_packages = {p["name"] for p in packages if p["name"] == "rafter" or p["name"].startswith("rafter-")}
        if not {"rafter", "rafter-runtime", "rafter-storage", "rafter-codec"} <= rafter_packages:
            raise ValueError(f"Rafter packages missing from {name}")
        for package in packages:
            if package["name"] == "rafter" or package["name"].startswith("rafter-"):
                expected = f"git+{REPOSITORY}?rev={sha}#{sha}"
                if package.get("source") != expected:
                    raise ValueError(f"Rafter lock identity mismatch: {name}: {package['name']}")
        for package, version in (("raft", "0.7.0"), ("openraft", "0.9.24")):
            if {p["version"] for p in packages if p["name"] == package} != {version}:
                raise ValueError(f"comparison dependency changed: {package}")


def select_rafter(ref: str) -> dict:
    sha = resolve_ref(ref, ROOT / ".cache/rafter-refs.git")
    # Resolve in a disposable copy. A bad ref/API/dependency cannot leave half
    # the checkout on one Rafter revision and half on another.
    with tempfile.TemporaryDirectory(prefix="rafter-selection-", dir=ROOT / ".cache") as tmp:
        staging = Path(tmp)
        for folder in ("adapters", "crates", "microbench", ".cargo"):
            shutil.copytree(ROOT / folder, staging / folder,
                            ignore=shutil.ignore_patterns("target", "__pycache__"))
        for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml"):
            shutil.copy2(ROOT / name, staging / name)
        for name in MANIFESTS:
            path = staging / name
            path.write_text(rewrite_manifest(path.read_text(), sha))
        for name in ("Cargo.toml", "microbench/Cargo.toml"):
            subprocess.run(["cargo", "update", "--workspace", "--manifest-path", str(staging / name)],
                           cwd=staging, check=True)
        check_resolution(staging, sha)
        pins = json.loads((ROOT / "implementations.lock.json").read_text())
        pins["rafter"] = {"git": REPOSITORY, "rev": sha, "requested_ref": ref}
        # Invalidate receipts before publishing any source/lock changes.
        for receipt in ("dist/build.json", "dist/microbench/build.json"):
            (ROOT / receipt).unlink(missing_ok=True)
        for name in (*MANIFESTS, *LOCKS):
            shutil.copy2(staging / name, ROOT / name)
        (ROOT / "implementations.lock.json").write_text(json.dumps(pins, indent=2) + "\n")
    print(f"Rafter: {ref} -> {sha}", flush=True)
    return pins["rafter"]

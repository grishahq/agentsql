"""Build and verify release artifacts without publishing anything.

Run after `python -m pip install '.[dev]'`: `python tools/verify_release.py`.
The build uses the already installed build backend. A fresh consumer environment
installs only the generated wheel, offline and with no runtime dependencies.
Tests and examples then run from the unpacked source distribution outside this
checkout. The final artifacts, checksums, and JSON evidence are saved in dist/.
"""

from __future__ import annotations

import argparse
from email.parser import BytesParser
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
import venv
import zipfile


ROOT = Path(__file__).resolve().parents[1]


def ensure(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def run(label: str, command: list[str], directory: Path, env: dict[str, str],
        checks: list[dict], timeout: int = 180) -> str:
    started = time.monotonic()
    result = subprocess.run(command, cwd=directory, env=env, capture_output=True,
                            text=True, timeout=timeout)
    elapsed = round(time.monotonic() - started, 3)
    if result.returncode:
        checks.append({"check": label, "status": "FAIL", "seconds": elapsed})
        raise RuntimeError(
            f"{label} exited {result.returncode}\n{result.stdout}\n{result.stderr}"
        )
    entry = {"check": label, "status": "PASS", "seconds": elapsed}
    test_count = re.search(r"Ran (\d+) tests?", result.stderr)
    if test_count:
        entry["tests"] = int(test_count.group(1))
    checks.append(entry)
    print(f"PASS {label} ({elapsed}s)", flush=True)
    return result.stdout


def unpack_source(source: Path, directory: Path) -> Path:
    # Only regular files/directories from this just-built archive are accepted.
    # Avoid platform/version-dependent extractall defaults or tar link handling.
    with tarfile.open(source, "r:gz") as archive:
        members = archive.getmembers()
        roots = {PurePosixPath(item.name).parts[0] for item in members}
        ensure(len(roots) == 1, "The source archive must have one top-level directory")
        for member in members:
            path = PurePosixPath(member.name)
            ensure(not path.is_absolute() and ".." not in path.parts,
                   f"Unsafe source archive path: {member.name}")
            destination = directory.joinpath(*path.parts)
            if member.isdir():
                destination.mkdir(parents=True, exist_ok=True)
            else:
                ensure(member.isfile(), f"Unsupported source archive member: {member.name}")
                destination.parent.mkdir(parents=True, exist_ok=True)
                contents = archive.extractfile(member)
                ensure(contents is not None, f"Missing source archive member: {member.name}")
                with contents:
                    destination.write_bytes(contents.read())
    return directory / roots.pop()


def readme_blocks(readme: Path, language: str) -> list[str]:
    return re.findall(rf"^```{re.escape(language)}\n(.*?)^```", readme.read_text(encoding="utf-8"),
                      flags=re.MULTILINE | re.DOTALL)


def verify(directory: Path, env: dict[str, str], checks: list[dict]) -> dict:
    artifacts = directory / "artifacts"
    artifacts.mkdir()
    run("Build wheel and source distribution", [sys.executable, "-m", "build", "--no-isolation",
        "--outdir", str(artifacts), str(ROOT)], directory, env, checks)
    wheels = list(artifacts.glob("*.whl"))
    sources = list(artifacts.glob("*.tar.gz"))
    ensure(len(wheels) == len(sources) == 1, "Expected exactly one wheel and one source archive")
    wheel, source = wheels[0], sources[0]
    run("Validate distribution metadata", [sys.executable, "-m", "twine", "check", "--strict",
        str(wheel), str(source)], directory, env, checks)

    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        expected_modules = {path.relative_to(ROOT / "src").as_posix()
                            for path in (ROOT / "src" / "agentsql").rglob("*.py")}
        ensure(bool(expected_modules), "AgentSQL source package is missing")
        ensure(expected_modules.issubset(names), "Wheel is missing a Python module")
        importable_modules = sorted(
            path.removesuffix(".py").replace("/", ".")
            for path in expected_modules
            if not path.endswith(("/__init__.py", "/__main__.py"))
        )
        ensure("agentsql/py.typed" in names, "Wheel is missing its typing marker")
        ensure(any(name.endswith("/licenses/LICENSE") for name in names), "Wheel is missing LICENSE")
        metadata_names = [name for name in names if name.endswith(".dist-info/METADATA")]
        ensure(len(metadata_names) == 1, "Expected exactly one package metadata record")
        metadata = BytesParser().parsebytes(archive.read(metadata_names[0]))
        ensure(metadata["Name"] == "agentsql", "Wheel has an unexpected distribution name")
        entrypoint_names = [name for name in names if name.endswith(".dist-info/entry_points.txt")]
        ensure(len(entrypoint_names) == 1, "Wheel must expose its public CLI entry point")
        ensure("agentsql = agentsql.cli:main" in archive.read(entrypoint_names[0]).decode("utf-8"),
               "Wheel has an unexpected CLI entry point")
        runtime = [item for item in metadata.get_all("Requires-Dist", []) if "extra ==" not in item]
        ensure(not runtime, f"Unexpected runtime dependencies: {runtime}")
        version = metadata["Version"]
    checks.append({"check": "Wheel license, typing marker, and stdlib-only runtime", "status": "PASS"})

    source_root = unpack_source(source, directory / "source")
    required = ["README.md", "LICENSE", "pyproject.toml", "src/agentsql/py.typed",
                "docs/integration.md", "docs/platform.md", "tools/verify_release.py", "examples/demo.py",
                "examples/project_integration.py", "examples/scenarios.py", "examples/platform_demo.py",
                "tests/test_workspace.py", "tests/test_platform_demo.py",
                "docs/assets/agentsql-hero.png", "docs/assets/agentsql-hero-prompt.txt",
                ".github/workflows/release.yml"]
    for name in required:
        ensure((source_root / name).is_file(), f"Source distribution is missing {name}")
    ensure((source_root / "docs/assets/agentsql-hero.png").read_bytes().startswith(
        b"\x89PNG\r\n\x1a\n"), "Source archive launch image is not a PNG")
    for folder, suffix in [("tests", ".py"), ("examples", ".py"), ("docs", ".md")]:
        expected = {path.relative_to(ROOT) for path in (ROOT / folder).rglob(f"*{suffix}")}
        actual = {path.relative_to(source_root) for path in (source_root / folder).rglob(f"*{suffix}")}
        ensure(expected == actual, f"Source distribution has an incomplete {folder} directory")
    checks.append({"check": "Source archive includes tests, documentation, and examples", "status": "PASS"})

    consumer = directory / "consumer"
    consumer.mkdir()
    environment = directory / "consumer-env"
    venv.EnvBuilder(with_pip=True).create(environment)
    python = environment / "bin" / "python"
    cli = environment / "bin" / "agentsql"
    consumer_env = dict(env, PATH=str(environment / "bin") + os.pathsep + env.get("PATH", ""))
    run("Install wheel offline in a clean environment", [str(python), "-m", "pip", "install",
        "--no-index", "--no-deps", "--no-cache-dir", str(wheel)], consumer, consumer_env, checks)
    probe = (
        "import importlib, importlib.metadata, pathlib, agentsql; "
        f"assert agentsql.__version__ == {version!r}; "
        f"assert importlib.metadata.version('agentsql') == {version!r}; "
        f"assert pathlib.Path(agentsql.__file__).is_relative_to({str(environment)!r}); "
        f"[importlib.import_module(name) for name in {importable_modules!r}]; "
        "print(agentsql.__version__)"
    )
    run("Validate installed import and version", [str(python), "-I", "-c", probe], consumer,
        consumer_env, checks)
    checks[-1]["modules"] = importable_modules
    run("Installed CLI demo", [str(cli), "demo"], consumer, consumer_env, checks)
    run("Installed module demo", [str(python), "-I", "-m", "agentsql", "demo"],
        consumer, consumer_env, checks)

    blocks = readme_blocks(source_root / "README.md", "python")
    ensure(bool(blocks), "README must include a runnable SDK quickstart")
    quickstart = consumer / "readme_quickstart.py"
    quickstart.write_text(blocks[0], encoding="utf-8")
    run("Exact README SDK quickstart", [str(python), "-I", str(quickstart)], consumer,
        consumer_env, checks)

    readme = (source_root / "README.md").read_text(encoding="utf-8")
    cli_section = readme.split("## CLI quickstart", 1)
    ensure(len(cli_section) == 2, "README must include the CLI quickstart section")
    cli_blocks = re.findall(r"^```sh\n(.*?)^```", cli_section[1], flags=re.MULTILINE | re.DOTALL)
    ensure(bool(cli_blocks), "README CLI quickstart is missing its commands")
    command_file = consumer / "readme_cli.sh"
    command_file.write_text(cli_blocks[0], encoding="utf-8")
    inputs = consumer / "inputs"
    inputs.mkdir()
    (inputs / "prices.csv").write_text("item,price\nwidget,10\n", encoding="utf-8")
    run("Exact README CLI quickstart", ["sh", "-eu", str(command_file)], consumer,
        consumer_env, checks)

    for name in ("demo", "project_integration"):
        run(f"Source archive {name} example", [str(python), str(source_root / "examples" / f"{name}.py")],
            consumer, consumer_env, checks)
    output = run("Source archive scenario suite", [str(python), str(source_root / "examples" /
        "scenarios.py"), "--json"], consumer, consumer_env, checks)
    scenarios = json.loads(output)
    ensure(scenarios["passed"] == scenarios["total"] and scenarios["total"] > 0,
           "Scenario suite reported a failure")
    checks[-1]["scenarios"] = scenarios["passed"]
    output = run("Source archive platform demonstration", [str(python), str(source_root /
        "examples" / "platform_demo.py"), "--directory", str(consumer / "platform-demo")],
        consumer, consumer_env, checks)
    platform_demo = json.loads(output)
    ensure(platform_demo["passed"] == len(platform_demo["outcomes"]) >= 7,
           "Platform demonstration did not verify all expected outcome groups")
    checks[-1]["outcome_groups"] = platform_demo["passed"]
    run("Source archive correctness suite against installed wheel", [str(python), "-m", "unittest",
        "discover", "-s", str(source_root / "tests"), "-v"], consumer, consumer_env, checks,
        timeout=300)

    return {"version": version, "artifacts": [
        {"file": artifact.name, "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
         "bytes": artifact.stat().st_size} for artifact in (wheel, source)]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist-dir", type=Path, default=ROOT / "dist",
                        help="Destination for verified artifacts and evidence (default: dist/)")
    args = parser.parse_args()
    destination = args.dist_dir.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    env = {key: value for key, value in os.environ.items()
           if key not in {"PYTHONPATH", "PYTHONHOME"} and not key.startswith("COVERAGE_")}
    env.update(PYTHONNOUSERSITE="1", PIP_DISABLE_PIP_VERSION_CHECK="1")
    checks: list[dict] = []
    report = {"status": "FAIL", "python": platform.python_version(), "platform": platform.platform(),
              "sqlite": sqlite3.sqlite_version, "checks": checks}
    try:
        try:
            report["tool_versions"] = {
                name: importlib.metadata.version(name)
                for name in ("build", "setuptools", "wheel", "twine")
            }
        except importlib.metadata.PackageNotFoundError as error:
            raise RuntimeError("Install development tooling first: python -m pip install '.[dev]'") from error
        with tempfile.TemporaryDirectory(prefix="agentsql-release-") as temporary:
            root = Path(temporary).resolve()
            ensure(not root.is_relative_to(ROOT), "Verification must run outside the checkout")
            report.update(verify(root, env, checks))
            for artifact in report["artifacts"]:
                shutil.copy2(root / "artifacts" / artifact["file"], destination / artifact["file"])
        report["status"] = "PASS"
        checksum_text = "".join(f'{item["sha256"]}  {item["file"]}\n' for item in report["artifacts"])
        (destination / "SHA256SUMS").write_text(checksum_text, encoding="utf-8")
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        report["error"] = str(error)
        print(f"FAIL {error}", file=sys.stderr)
    evidence = destination / "release-verification.json"
    evidence.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Release verification: {report['status']}. Evidence: {evidence}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

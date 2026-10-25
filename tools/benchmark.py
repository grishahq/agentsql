"""Measure one local workload; results are evidence, not capacity guarantees."""

import argparse
import json
import math
from pathlib import Path
import platform
import resource
import sqlite3
import statistics
import tempfile
import time

from agentsql import Workspace


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def benchmark(files, revisions, file_bytes):
    with tempfile.TemporaryDirectory(prefix="agentsql-benchmark-") as directory:
        root = Path(directory)
        inputs = root / "inputs"
        inputs.mkdir()
        for index in range(files):
            (inputs / f"input-{index:06d}.txt").write_bytes(b"x" * file_bytes)
        started = time.perf_counter()
        workspace = Workspace.create(root / "workspace.sqlite3", inputs)
        capture_seconds = time.perf_counter() - started
        elapsed = []
        for generation in range(revisions):
            started = time.perf_counter()
            worker = workspace.branch(f"worker-{generation}")
            path = f"input-{generation % files:06d}.txt"
            original = worker.read_bytes(path)
            worker.write_bytes(path, original + b"!")
            worker.write_text("report.txt", f"Generation {generation}")
            proposal = worker.seal()
            worker.accept(expected_proposal=proposal)
            elapsed.append((time.perf_counter() - started) * 1000)
        started = time.perf_counter()
        backup = workspace.backup(root / "backup.sqlite3")
        backup_seconds = time.perf_counter() - started
        started = time.perf_counter()
        report = Workspace.open(backup).check_integrity()
        integrity_seconds = time.perf_counter() - started
        if not report["ok"] or report["head_revision"] != revisions + 1:
            raise RuntimeError(report)
        snapshot = workspace.snapshot()
        if snapshot.read_text("report.txt") != f"Generation {revisions - 1}":
            raise RuntimeError("Final result does not match workload")
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return {
            "measurement": "One local run; not a capacity or latency guarantee",
            "python": platform.python_version(), "platform": platform.platform(),
            "sqlite": sqlite3.sqlite_version, "files": files, "initial_file_bytes": file_bytes,
            "accepted_change_sets": revisions, "capture_seconds": round(capture_seconds, 4),
            "worker_seal_accept_ms_median": round(statistics.median(elapsed), 3),
            "worker_seal_accept_ms_p95": round(sorted(elapsed)[math.ceil(.95 * len(elapsed)) - 1], 3),
            "backup_seconds": round(backup_seconds, 4), "integrity_seconds": round(integrity_seconds, 4),
            "database_bytes": workspace.db_path.stat().st_size, "backup_bytes": backup.stat().st_size,
            "peak_process_rss_bytes": rss if platform.system() == "Darwin" else rss * 1024,
            "integrity_ok": report["ok"],
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--files", type=positive, default=1000)
    parser.add_argument("--revisions", type=positive, default=50)
    parser.add_argument("--file-bytes", type=positive, default=512)
    args = parser.parse_args()
    print(json.dumps(benchmark(args.files, args.revisions, args.file_bytes), indent=2, sort_keys=True))

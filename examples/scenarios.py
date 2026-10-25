"""Exercise installed agentsql with deterministic simulated agent workers.

Run: python examples/scenarios.py [--json] [--directory NEW_OR_EMPTY_DIRECTORY]
No language model, network service, or API key is used. A concurrent winner may
vary, but every expected outcome is deterministic and checked against real state.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing
from pathlib import Path
import tempfile
import sys

from agentsql import ConflictError, Workspace


def check(condition, message):
    # Keep validation active even when Python is invoked with -O.
    if not condition:
        raise AssertionError(message)


def create(root, files=None):
    root.mkdir()
    source = root / "source"
    source.mkdir()
    for name, contents in (files or {"input.txt": "original"}).items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")
    return Workspace.create(root / "workspace.sqlite3", source)


def publish(workspace, name, files):
    worker = workspace.branch(name)
    for path, contents in files.items():
        worker.write_text(path, contents)
    return worker.accept(message=f"Simulated worker {name}")


def reject(workspace, worker, kind, path):
    before = workspace.history()
    try:
        worker.accept()
    except ConflictError as error:
        evidence = error.as_dict()
    else:
        raise AssertionError("A stale or conflicting proposal was accepted")
    check(any(item["kind"] == kind and item["path"] == path
              for item in evidence["conflicts"]), evidence)
    check(workspace.history() == before, "Rejected publication changed history")
    check(worker.inspect()["status"] == "open", "Rejected branch was closed")
    return evidence


def stale_input_recompute(root):
    workspace = create(root, {"prices.csv": "item,price\nwidget,100\n"})

    def prepare(worker):
        price = int(worker.read_text("prices.csv").splitlines()[1].split(",")[1])
        worker.write_text("proposal.md", f"Total for three widgets: ${3 * price}\n")

    first = workspace.branch("worker-a")
    prepare(first)
    publish(workspace, "worker-b", {"prices.csv": "item,price\nwidget,120\n"})
    conflict = reject(workspace, first, "stale_read", "prices.csv")
    check(not workspace.snapshot().exists("proposal.md"), "Stale output leaked")
    refreshed = workspace.branch("worker-a-recomputed")
    prepare(refreshed)
    accepted = refreshed.accept()
    proposal = workspace.snapshot().read_text("proposal.md")
    check(proposal == "Total for three widgets: $360\n", proposal)
    return {"conflict": conflict, "accepted": accepted, "proposal": proposal}


def independent_branches(root):
    workspace = create(root)
    workers = [workspace.branch(name) for name in ("worker-a", "worker-b")]
    for worker, output in zip(workers, ("a.txt", "b.txt")):
        worker.write_text(output, worker.read_text("input.txt") + worker.name)
    accepted = [worker.accept() for worker in workers]
    check([item["revision"] for item in accepted] == [2, 3], accepted)
    check(workspace.snapshot().list_files() == ["a.txt", "b.txt", "input.txt"],
          "Independent branch output missing")
    return {"accepted": accepted, "shared_input_version": workspace.snapshot().version("input.txt")}


def _publisher(database, name, barrier, queue):
    try:
        worker = Workspace.open(database).get_branch(name)
        barrier.wait(timeout=20)
        try:
            queue.put({"status": "accepted", "worker": name, "result": worker.accept()})
        except ConflictError as error:
            queue.put({"status": "requires_recheck", "worker": name, "result": error.as_dict()})
    except Exception as error:
        queue.put({"status": "error", "worker": name, "error": repr(error)})
        raise


def concurrent_writers(root):
    workspace = create(root)
    for name in ("worker-a", "worker-b"):
        worker = workspace.branch(name)
        worker.write_text("input.txt", name)
        worker.write_text(f"{name}-only.txt", name)
    context = multiprocessing.get_context("spawn")
    barrier, queue = context.Barrier(3), context.Queue()
    processes = [context.Process(target=_publisher,
                 args=(str(workspace.db_path), name, barrier, queue))
                 for name in ("worker-a", "worker-b")]
    try:
        for process in processes:
            process.start()
        barrier.wait(timeout=20)
        outcomes = [queue.get(timeout=25) for _ in processes]
        for process in processes:
            process.join(timeout=20)
            check(process.exitcode == 0, f"Publisher process exit: {process.exitcode}")
        check(sorted(item["status"] for item in outcomes) == ["accepted", "requires_recheck"], outcomes)
        winner = next(item["worker"] for item in outcomes if item["status"] == "accepted")
        loser = next(item["worker"] for item in outcomes if item["status"] != "accepted")
        current = workspace.snapshot()
        check(current.read_text("input.txt") == winner, "Winner's bytes missing")
        check(current.exists(f"{winner}-only.txt"), "Winner's full change set missing")
        check(not current.exists(f"{loser}-only.txt"), "Loser's uncontested output leaked")
        check(len(workspace.history()) == 2, "Concurrent publishers created extra revisions")
        return {"processes": 2, "outcomes": sorted(outcomes, key=lambda item: item["worker"])}
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
        queue.close()
        queue.join_thread()


def whole_change_set(root):
    workspace = create(root)
    worker = workspace.branch("two-output-worker")
    worker.write_text("input.txt", "replacement")
    worker.write_text("uncontested.md", "Do not publish part of this proposal")
    publish(workspace, "competing-worker", {"input.txt": "accepted competitor"})
    conflict = reject(workspace, worker, "conflicting_write", "input.txt")
    current = workspace.snapshot()
    check(not current.exists("uncontested.md"), "Partial change set was published")
    check(current.read_text("input.txt") == "accepted competitor", "Winner was overwritten")
    return {"conflict": conflict, "uncontested_output_published": current.exists("uncontested.md")}


def coherent_snapshots(root):
    workspace = create(root, {"left.txt": "old", "right.txt": "old"})
    snapshot = workspace.snapshot()
    branch = workspace.branch("reader")
    left_before = snapshot.read_text("left.txt")
    branch.read_text("left.txt")
    publish(workspace, "pair-update", {"left.txt": "new", "right.txt": "new"})
    old_pair = [left_before, snapshot.read_text("right.txt")]
    current = workspace.snapshot()
    new_pair = [current.read_text("left.txt"), current.read_text("right.txt")]
    check(old_pair == ["old", "old"] and new_pair == ["new", "new"], "Mixed generations")
    check(branch.read_text("right.txt") == "old", "Branch input snapshot moved")
    return {"old_revision": snapshot.revision, "old_pair": old_pair,
            "new_revision": current.revision, "new_pair": new_pair}


def missing_input_appears(root):
    workspace = create(root)
    worker = workspace.branch("optional-input-reader")
    check(not worker.exists("optional.txt"), "Optional input unexpectedly exists")
    worker.write_text("result.txt", "Computed without optional input")
    publish(workspace, "input-creator", {"optional.txt": "new context"})
    conflict = reject(workspace, worker, "stale_read", "optional.txt")
    check(not workspace.snapshot().exists("result.txt"), "Stale output leaked")
    return {"tracked_reads": worker.inspect()["reads"], "conflict": conflict}


def listing_membership(root):
    workspace = create(root)
    worker = workspace.branch("inventory-worker")
    listed = worker.list_files()
    worker.write_text("inventory.txt", "\n".join(listed))
    publish(workspace, "new-input-worker", {"extra.txt": "additional input"})
    conflict = reject(workspace, worker, "stale_listing", "*")
    check(not workspace.snapshot().exists("inventory.txt"), "Stale inventory leaked")
    return {"original_listing": listed, "current_listing": workspace.snapshot().list_files(),
            "conflict": conflict}


def persistence_reopen(root):
    workspace = create(root)
    worker = workspace.branch("durable-worker")
    worker.write_text("result.txt", worker.read_text("input.txt").upper())
    before = worker.inspect()
    reopened = Workspace.open(workspace.db_path)
    check(reopened.get_branch("durable-worker").inspect() == before, "Staged state was lost")
    accepted = reopened.get_branch("durable-worker").accept("Accepted after reopening")
    final = Workspace.open(workspace.db_path)
    check(final.snapshot().read_text("result.txt") == "ORIGINAL", "Published bytes were lost")
    check(final.get_branch("durable-worker").inspect()["status"] == "accepted", "Status lost")
    check(final.history()[-1]["message"] == "Accepted after reopening", "History was lost")
    return {"tracked_reads_after_reopen": before["reads"], "accepted": accepted,
            "history_revisions": [item["revision"] for item in final.history()]}


def untracked_input_boundary(root):
    workspace = create(root)
    worker = workspace.branch("external-file-reader")
    content = (root / "source" / "input.txt").read_text(encoding="utf-8")
    worker.write_text("result.txt", content)
    publish(workspace, "input-updater", {"input.txt": "new workspace input"})
    accepted = worker.accept()
    reads = worker.inspect()["reads"]
    check(reads == [], "SDK claimed to observe an external file read")
    check(workspace.snapshot().read_text("result.txt") == "original", "Unexpected output")
    return {"accepted": accepted, "tracked_reads": reads, "detection_expected": False,
            "boundary": "External file reads bypass the SDK; use branch.read_text/read_bytes to track inputs."}


def source_capture_isolation(root):
    workspace = create(root)
    source_file = root / "source" / "input.txt"
    source_file.write_text("outside mutation", encoding="utf-8")
    check(workspace.snapshot().read_text("input.txt") == "original", "Capture was mutable")
    publish(workspace, "workspace-updater", {"input.txt": "workspace mutation"})
    check(source_file.read_text(encoding="utf-8") == "outside mutation", "Source was overwritten")
    return {"source": source_file.read_text(encoding="utf-8"),
            "initial_capture": workspace.snapshot(1).read_text("input.txt"),
            "current_workspace": workspace.snapshot().read_text("input.txt")}


def single_worker_alternatives(root):
    workspace = create(root)
    fast, detailed = workspace.branch("fast-approach"), workspace.branch("detailed-approach")
    for worker in (fast, detailed):
        worker.write_text("answer.md", f"{worker.name}: {worker.read_text('input.txt')}")
    reviews = {worker.name: worker.diff() for worker in (fast, detailed)}
    check(all("answer.md" in diff for diff in reviews.values()), "Alternative diff missing")
    accepted = detailed.accept("Selected detailed approach")
    conflict = reject(workspace, fast, "conflicting_write", "answer.md")
    check(workspace.snapshot().read_text("answer.md") == "detailed-approach: original",
          "Unselected alternative overwrote chosen result")
    return {"reviewed_diffs": reviews, "chosen": accepted, "unselected_conflict": conflict}


SCENARIOS = (stale_input_recompute, independent_branches, concurrent_writers,
             whole_change_set, coherent_snapshots, missing_input_appears,
             listing_membership, persistence_reopen, untracked_input_boundary,
             source_capture_isolation, single_worker_alternatives)


def run(directory):
    results = []
    for scenario in SCENARIOS:
        try:
            evidence = scenario(directory / scenario.__name__)
            results.append({"scenario": scenario.__name__, "status": "PASS", "evidence": evidence})
        except Exception as error:
            results.append({"scenario": scenario.__name__, "status": "FAIL", "error": repr(error)})
    return {"actors": "Simulated agent workers; deterministic local code, no models or API keys",
            "passed": sum(item["status"] == "PASS" for item in results),
            "total": len(results), "results": results}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print machine-readable scenario evidence")
    parser.add_argument("--directory", type=Path, help="Retain workspaces in a new or empty directory")
    args = parser.parse_args(argv)
    try:
        if args.directory is None:
            with tempfile.TemporaryDirectory(prefix="agentsql-scenarios-") as temporary:
                report = run(Path(temporary))
            report["retained_directory"] = None
        else:
            destination = args.directory.expanduser().absolute()
            if destination.is_symlink() or (destination.exists() and
                    (not destination.is_dir() or any(destination.iterdir()))):
                raise ValueError("The scenario directory must be new or empty and not a symlink")
            destination.mkdir(parents=True, exist_ok=True)
            report = run(destination)
            report["retained_directory"] = str(destination)
    except (OSError, ValueError) as error:
        if args.json:
            print(json.dumps({"error": str(error)}))
        else:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(report["actors"])
        for result in report["results"]:
            print(f"{result['status']} {result['scenario']}: "
                  + json.dumps(result.get("evidence", result.get("error")), sort_keys=True))
        print(f"{report['passed']}/{report['total']} scenarios passed")
        if report["retained_directory"]:
            print(f"Workspaces retained at {report['retained_directory']}")
    return 0 if report["passed"] == report["total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

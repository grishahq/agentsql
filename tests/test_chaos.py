"""Bounded process contention and real process-death transaction checks.

The crash gates are installed only by the test child. They pause an actual
SQLite transaction or pause immediately after its COMMIT; no production code
is mocked and no timing-dependent sleep decides where the process is killed.
"""

from __future__ import annotations

from contextlib import closing
import multiprocessing
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

from agentsql import ConflictError, Workspace


def _accept_at_barrier(database, name, barrier, connection):
    try:
        branch = Workspace.open(database).get_branch(name)
        barrier.wait(timeout=15)
        try:
            connection.send(("accepted", name, branch.accept(f"Publish {name}")))
        except ConflictError as exc:
            connection.send(("conflict", name, exc.as_dict()))
    except BaseException as exc:
        connection.send(("error", name, type(exc).__name__, str(exc)))
    finally:
        connection.close()


def _accept_with_crash_gate(database, name, phase, connection, release):
    """Pause exactly inside publication, or after durable commit before return."""
    try:
        branch = Workspace.open(database).get_branch(name)
        real_connect = sqlite3.connect

        def gate():
            connection.send(("at_gate", phase))
            if not release.wait(30):
                raise RuntimeError("Parent did not terminate or release the child")
            return 0

        class GatedConnection(sqlite3.Connection):
            def commit(self):
                super().commit()
                if phase == "after_commit":
                    gate()

        def connect(*args, **kwargs):
            kwargs["factory"] = GatedConnection
            db = real_connect(*args, **kwargs)
            if phase == "during_entries":
                db.create_function("publication_gate", 0, gate)
                db.execute("""
                    CREATE TEMP TRIGGER crash_publication
                    AFTER INSERT ON main.entries
                    WHEN NEW.path = 'output-one.txt' AND NEW.revision > 1
                    BEGIN SELECT publication_gate(); END
                """)
            return db

        with patch("agentsql.core.sqlite3.connect", side_effect=connect):
            result = branch.accept("Crash recovery probe")
        connection.send(("returned", result))
    except BaseException as exc:
        connection.send(("error", type(exc).__name__, str(exc)))
    finally:
        connection.close()


class ProcessContractsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        source = self.root / "inputs"
        source.mkdir()
        (source / "left.txt").write_text("initial left", encoding="utf-8")
        (source / "right.txt").write_text("initial right", encoding="utf-8")
        self.database = self.root / "workspace.sqlite3"
        self.workspace = Workspace.create(self.database, source)
        self.context = multiprocessing.get_context("spawn")

    def _assert_database_integrity(self):
        with closing(sqlite3.connect(self.database)) as db:
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchall(), [("ok",)])
            self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])

    def _run_publishers(self, names):
        barrier = self.context.Barrier(len(names) + 1)
        workers = []
        for name in names:
            parent, child = self.context.Pipe(duplex=False)
            process = self.context.Process(
                target=_accept_at_barrier,
                args=(str(self.database), name, barrier, child),
            )
            workers.append((process, parent, child))
        try:
            for process, _, child in workers:
                process.start()
                child.close()
            barrier.wait(timeout=15)
            deadline = time.monotonic() + 15
            outcomes = []
            for process, parent, _ in workers:
                self.assertTrue(parent.poll(max(0, deadline - time.monotonic())),
                                f"Publisher {process.pid} never returned")
                outcomes.append(parent.recv())
            for process, _, _ in workers:
                process.join(timeout=max(0, deadline - time.monotonic()))
                self.assertEqual(process.exitcode, 0)
            self.assertFalse(any(item[0] == "error" for item in outcomes), outcomes)
            return outcomes
        finally:
            for process, parent, child in workers:
                if process.is_alive():
                    process.terminate()
                if process.pid is not None:
                    process.join(timeout=5)
                parent.close()
                child.close()

    def test_eight_processes_can_publish_independent_outputs_from_same_inputs(self):
        names = [f"independent-{number}" for number in range(8)]
        for name in names:
            branch = self.workspace.branch(name)
            self.assertEqual(branch.read_text("left.txt"), "initial left")
            branch.write_text(f"outputs/{name}.txt", name)
        pinned = self.workspace.snapshot()
        outcomes = self._run_publishers(names)
        self.assertEqual([item[0] for item in outcomes], ["accepted"] * 8)
        self.assertEqual(sorted(item[2]["revision"] for item in outcomes), list(range(2, 10)))
        current = Workspace.open(self.database).snapshot()
        for name in names:
            self.assertEqual(current.read_text(f"outputs/{name}.txt"), name)
            self.assertFalse(pinned.exists(f"outputs/{name}.txt"))
        history = self.workspace.history()
        self.assertEqual([item["revision"] for item in history], list(range(1, 10)))
        self.assertEqual([item["parent_revision"] for item in history], [None] + list(range(1, 9)))
        self._assert_database_integrity()

    def test_eight_competing_processes_publish_exactly_one_whole_change_set(self):
        names = [f"competitor-{number}" for number in range(8)]
        for name in names:
            branch = self.workspace.branch(name)
            branch.write_text("left.txt", name)
            branch.write_text(f"outputs/{name}.txt", name)
        outcomes = self._run_publishers(names)
        accepted = [item for item in outcomes if item[0] == "accepted"]
        conflicts = [item for item in outcomes if item[0] == "conflict"]
        self.assertEqual(len(accepted), 1, outcomes)
        self.assertEqual(len(conflicts), 7, outcomes)
        winner = accepted[0][1]
        snapshot = self.workspace.snapshot()
        self.assertEqual(snapshot.revision, 2)
        self.assertEqual(snapshot.read_text("left.txt"), winner)
        self.assertEqual(snapshot.read_text("right.txt"), "initial right")
        self.assertEqual(snapshot.list_files(), ["left.txt", f"outputs/{winner}.txt", "right.txt"])
        self.assertEqual(len(self.workspace.history()), 2)
        for _, _, evidence in conflicts:
            self.assertEqual(evidence["current_revision"], 2)
            self.assertEqual(evidence["conflicts"], [{
                "kind": "conflicting_write", "path": "left.txt", "expected": 1, "actual": 2,
            }])
        self._assert_database_integrity()

    def test_cross_read_write_skew_rejects_one_of_two_disjoint_writers(self):
        a = self.workspace.branch("left-worker")
        b = self.workspace.branch("right-worker")
        a.read_text("right.txt")
        b.read_text("left.txt")
        a.write_text("left.txt", "left derived from initial right")
        b.write_text("right.txt", "right derived from initial left")
        outcomes = self._run_publishers([a.name, b.name])
        self.assertEqual(sorted(item[0] for item in outcomes), ["accepted", "conflict"])
        winner = next(item[1] for item in outcomes if item[0] == "accepted")
        evidence = next(item[2] for item in outcomes if item[0] == "conflict")
        changed = "left.txt" if winner == a.name else "right.txt"
        unchanged = "right.txt" if winner == a.name else "left.txt"
        self.assertEqual(evidence["conflicts"], [{
            "kind": "stale_read", "path": changed, "expected": 1, "actual": 2,
        }])
        snapshot = self.workspace.snapshot()
        self.assertEqual(snapshot.read_text(unchanged), f"initial {unchanged.removesuffix('.txt')}")
        self.assertEqual(len(self.workspace.history()), 2)
        self._assert_database_integrity()

    def _kill_at_gate(self, phase):
        parent, child = self.context.Pipe(duplex=False)
        release = self.context.Event()
        process = self.context.Process(
            target=_accept_with_crash_gate,
            args=(str(self.database), "crash-worker", phase, child, release),
        )
        try:
            process.start()
            child.close()
            self.assertTrue(parent.poll(15), "Child never reached the crash gate")
            self.assertEqual(parent.recv(), ("at_gate", phase))
            # Readers must observe either the old commit or the full new one,
            # even while the child is alive and blocked inside publication.
            observed = Workspace.open(self.database)
            if phase == "during_entries":
                self.assertEqual(observed.snapshot().revision, 1)
                self.assertEqual(len(observed.history()), 1)
                self.assertFalse(observed.snapshot().exists("output-one.txt"))
            else:
                self.assertEqual(observed.snapshot().revision, 2)
                self.assertEqual(observed.snapshot().read_text("output-two.txt"), "two")
            process.terminate()
            process.join(timeout=5)
            self.assertIsNotNone(process.exitcode)
            self.assertNotEqual(process.exitcode, 0)
        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            parent.close()
            child.close()

    def _prepare_crash_branch(self):
        branch = self.workspace.branch("crash-worker")
        branch.read_text("left.txt")
        branch.write_text("left.txt", "updated left")
        branch.write_text("output-one.txt", "one")
        branch.write_text("output-two.txt", "two")
        branch.delete("right.txt")
        return branch

    def test_killed_mid_publication_rolls_back_every_shared_change_and_can_retry(self):
        branch = self._prepare_crash_branch()
        before_history = self.workspace.history()
        before_proposal = branch.inspect()
        self._kill_at_gate("during_entries")
        reopened = Workspace.open(self.database)
        self.assertEqual(reopened.history(), before_history)
        self.assertEqual(reopened.get_branch(branch.name).inspect(), before_proposal)
        snapshot = reopened.snapshot()
        self.assertEqual(snapshot.revision, 1)
        self.assertEqual(snapshot.list_files(), ["left.txt", "right.txt"])
        self.assertEqual(snapshot.read_text("left.txt"), "initial left")
        self.assertEqual(snapshot.read_text("right.txt"), "initial right")
        self._assert_database_integrity()
        result = reopened.get_branch(branch.name).accept("Retry after worker death")
        self.assertEqual(result["revision"], 2)
        self.assertEqual(reopened.snapshot().read_text("output-two.txt"), "two")
        self.assertFalse(reopened.snapshot().exists("right.txt"))

    def test_killed_after_commit_before_response_retains_complete_published_result(self):
        branch = self._prepare_crash_branch()
        self._kill_at_gate("after_commit")
        reopened = Workspace.open(self.database)
        snapshot = reopened.snapshot()
        self.assertEqual(snapshot.revision, 2)
        self.assertEqual(snapshot.list_files(), ["left.txt", "output-one.txt", "output-two.txt"])
        self.assertEqual(snapshot.read_text("left.txt"), "updated left")
        self.assertEqual(snapshot.read_text("output-one.txt"), "one")
        self.assertEqual(snapshot.read_text("output-two.txt"), "two")
        self.assertFalse(snapshot.exists("right.txt"))
        self.assertEqual(snapshot.version("right.txt"), 2)
        proposal = reopened.get_branch(branch.name).inspect()
        self.assertEqual(proposal["status"], "accepted")
        self.assertEqual(proposal["accepted_revision"], 2)
        history = reopened.history()
        self.assertEqual(len(history), 2)
        self.assertEqual(history[-1]["message"], "Crash recovery probe")
        self.assertEqual(history[-1]["paths"], ["left.txt", "output-one.txt", "output-two.txt", "right.txt"])
        self._assert_database_integrity()


if __name__ == "__main__":
    unittest.main()

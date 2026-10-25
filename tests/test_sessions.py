"""Named cooperative sessions, provenance-preserving forks and schema upgrades."""

from contextlib import closing
import hashlib
import json
import multiprocessing
from pathlib import Path
import sqlite3
import tempfile
import unittest

from agentsql import BranchClosedError, ConflictError, Workspace, WorkspaceError


def _session_worker(database, barrier, number, queue):
    try:
        workspace = Workspace.open(database)
        barrier.wait(timeout=15)
        branch = workspace.session("shared-workers")
        branch.fs.write_file(f"worker-{number}.txt", str(number))
        queue.put(("ok", branch.base_revision))
    except BaseException as error:
        queue.put(("error", repr(error)))


class SessionAndForkTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        source = self.root / "source"
        source.mkdir()
        (source / "inputs").mkdir()
        (source / "inputs/prices.csv").write_text("coffee,10\n", encoding="utf-8")
        self.database = self.root / "state.db"
        self.workspace = Workspace.create(self.database, source)

    def test_named_session_reopens_shared_staged_state(self):
        first = self.workspace.session("team")
        first.fs.write_file("draft.md", "shared pending work")
        reopened = Workspace.open(self.database).session("team")
        self.assertEqual(reopened.fs.read_file("draft.md"), "shared pending work")
        self.assertEqual(first.base_revision, reopened.base_revision)
        reopened.kv.set("stage", "review")
        self.assertEqual(first.kv.get("stage"), "review")
        self.assertEqual(len(self.workspace.branches()), 1)

    def test_session_does_not_refresh_an_existing_base(self):
        first = self.workspace.session("team")
        update = self.workspace.branch("update")
        update.fs.write_file("inputs/prices.csv", "coffee,12\n")
        update.accept()
        self.assertEqual(self.workspace.session("team").base_revision, first.base_revision)
        self.assertEqual(first.fs.read_file("inputs/prices.csv"), "coffee,10\n")

    def test_sealed_and_accepted_sessions_remain_closed_on_reopen(self):
        branch = self.workspace.session("team")
        branch.fs.write_file("draft.md", "done")
        token = branch.seal()
        reopened = self.workspace.session("team")
        self.assertEqual(reopened.inspect()["proposal_id"], token)
        with self.assertRaises(BranchClosedError):
            reopened.fs.write_file("draft.md", "changed")
        branch.accept(expected_proposal=token)
        with self.assertRaises(BranchClosedError):
            self.workspace.session("team").fs.read_file("draft.md")

    def test_session_creation_is_atomic_across_processes(self):
        context = multiprocessing.get_context("spawn")
        count = 6
        barrier, queue = context.Barrier(count), context.Queue()
        processes = [context.Process(target=_session_worker,
            args=(str(self.database), barrier, number, queue)) for number in range(count)]
        for process in processes:
            process.start()
        try:
            results = [queue.get(timeout=30) for _ in processes]
            self.assertEqual(results, [("ok", 1)] * count)
        finally:
            for process in processes:
                process.join(timeout=10)
                if process.is_alive():
                    process.kill()
                    process.join()
        self.assertTrue(all(process.exitcode == 0 for process in processes))
        self.assertEqual(len(self.workspace.branches()), 1)
        self.workspace.session("shared-workers").accept()
        self.assertEqual(self.workspace.snapshot().list_files(),
                         ["inputs/prices.csv"] + [f"worker-{number}.txt" for number in range(count)])
        self.assertTrue(self.workspace.check_integrity()["ok"])

    def test_open_fork_copies_files_directories_state_and_all_provenance(self):
        setup = self.workspace.branch("config")
        setup.kv.set("config/discount", 0.1)
        setup.kv.set("config/obsolete", True)
        setup.accept()
        source = self.workspace.branch("source")
        source.fs.read_file("inputs/prices.csv")
        source.fs.readdir("inputs")
        source.fs.exists("not-present")
        source.kv.get("config/discount")
        source.kv.get("config/missing")
        source.kv.list("config/")
        source.fs.mkdir("outputs/empty", parents=True)
        source.fs.write_file("outputs/report.md", "draft")
        source.kv.set("config/discount", 0.2)
        source.kv.delete("config/obsolete")
        source.tools.record("external-effect", result="belongs to source")
        before = source.inspect()
        fork = self.workspace.fork("source", "alternative")
        after = fork.inspect()
        for field in ("base_revision", "reads", "listing_hash", "changes", "directories",
                      "directory_reads", "kv_reads", "kv_changes", "kv_list_reads"):
            self.assertEqual(before[field], after[field], field)
        self.assertFalse(after["sealed"])
        self.assertEqual(fork.fs.read_file("outputs/report.md"), "draft")
        self.assertTrue(fork.fs.exists("outputs/empty"))
        self.assertEqual(fork.kv.get("config/discount"), 0.2)
        self.assertFalse(fork.kv.exists("config/obsolete"))
        self.assertEqual(fork.tools.list(), [])
        self.assertEqual(len(source.tools.list()), 1)
        fork.fs.write_file("outputs/report.md", "alternative")
        self.assertEqual(source.fs.read_file("outputs/report.md"), "draft")
        source.fs.write_file("source-only", "later")
        self.assertFalse(fork.fs.exists("source-only"))
        self.assertTrue(self.workspace.check_integrity()["ok"])

    def test_stale_dependency_remains_stale_after_fork(self):
        source = self.workspace.branch("source")
        source.fs.read_file("inputs/prices.csv")
        source.fs.write_file("report.md", "old prices")
        update = self.workspace.branch("update")
        update.fs.write_file("inputs/prices.csv", "coffee,12\n")
        update.accept()
        fork = self.workspace.fork("source", "fork")
        with self.assertRaises(ConflictError) as caught:
            fork.accept()
        self.assertTrue(any(row["path"] == "inputs/prices.csv"
                            for row in caught.exception.conflicts))
        self.assertFalse(self.workspace.snapshot().exists("report.md"))

    def test_sealed_fork_is_editable_and_does_not_inherit_review_identity(self):
        source = self.workspace.branch("source")
        source.fs.write_file("report.md", "reviewed")
        token = source.seal()
        fork = self.workspace.fork("source", "alternative")
        self.assertFalse(fork.inspect()["sealed"])
        fork.fs.write_file("report.md", "new approach")
        new_token = fork.seal()
        self.assertNotEqual(token, new_token)
        self.assertEqual(source.inspect()["proposal_id"], token)
        fork.accept(expected_proposal=new_token)
        self.assertEqual(self.workspace.snapshot().fs.read_file("report.md"), "new approach")

    def test_accepted_fork_bases_on_source_accepted_revision_and_starts_clean(self):
        source = self.workspace.branch("source")
        source.fs.read_file("inputs/prices.csv")
        source.fs.mkdir("empty")
        source.fs.write_file("report.md", "published")
        source.kv.set("state", {"done": True})
        accepted = source.accept()
        update = self.workspace.branch("later")
        update.fs.write_file("new-head-only", "later")
        update.accept()
        fork = self.workspace.fork("source", "accepted-fork")
        self.assertEqual(fork.base_revision, accepted["revision"])
        self.assertEqual(fork.fs.read_file("report.md"), "published")
        self.assertTrue(fork.fs.exists("empty"))
        self.assertEqual(fork.kv.get("state"), {"done": True})
        self.assertFalse(self.workspace.snapshot(fork.base_revision).fs.exists("new-head-only"))
        state = fork.inspect()
        self.assertEqual(state["changes"], [])
        self.assertEqual(state["directories"], [])
        self.assertEqual(state["kv_changes"], [])
        self.assertFalse(state["tracks_listing"])
        fork.fs.write_file("next-report", "okay")
        fork.accept()
        self.assertTrue(self.workspace.snapshot().exists("new-head-only"))

    def test_duplicate_or_missing_fork_does_not_mutate_any_branch(self):
        source = self.workspace.branch("source")
        source.fs.write_file("pending", "keep")
        before = source.inspect()
        for original, name in (("missing", "new"), ("source", "source")):
            with self.subTest(original=original), self.assertRaises(WorkspaceError):
                self.workspace.fork(original, name)
        self.assertEqual(source.inspect(), before)
        self.assertEqual([branch["name"] for branch in self.workspace.branches()], ["source"])

    def make_schema_two(self, *, nested_output=False):
        branch = self.workspace.branch("legacy")
        branch.read_text("inputs/prices.csv")
        branch.list_files()
        output = "new/reports/result.md" if nested_output else "result.md"
        branch.write_text(output, "legacy proposal")
        with closing(sqlite3.connect(self.database)) as db:
            # Reconstruct v0.2 metadata and its exact legacy proposal digest.
            listing = hashlib.sha256(json.dumps(["inputs/prices.csv"], ensure_ascii=True).encode("ascii")).hexdigest()
            db.execute("UPDATE branches SET listing_hash=? WHERE name='legacy'", (listing,))
            payload = {"branch": "legacy", "base_revision": 1, "listing_hash": listing,
                       "reads": db.execute("SELECT path,version FROM reads WHERE branch='legacy' ORDER BY path").fetchall(),
                       "changes": db.execute("SELECT path,hash FROM changes WHERE branch='legacy' ORDER BY path").fetchall()}
            token = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=True,
                separators=(",", ":")).encode("ascii")).hexdigest()
            db.execute("INSERT INTO proposals(branch,proposal_id) VALUES ('legacy',?)", (token,))
            for table in ("tool_events", "tool_calls", "dir_changes", "dir_reads", "dirs",
                          "kv_changes", "kv_reads", "kv_list_reads", "kv_entries"):
                db.execute(f"DROP TABLE {table}")
            db.execute("ALTER TABLE branches DROP COLUMN listing_format")
            db.execute("ALTER TABLE proposals DROP COLUMN format")
            db.execute("DELETE FROM meta WHERE key='v3_start_revision'")
            db.execute("UPDATE meta SET value='2' WHERE key='schema_version'")
            db.commit()
        return token, output

    def test_schema_two_sealed_proposal_keeps_review_identity_after_upgrade(self):
        token, output = self.make_schema_two()
        reopened = Workspace.open(self.database)
        branch = reopened.get_branch("legacy")
        self.assertEqual(branch.inspect()["proposal_id"], token)
        self.assertTrue(reopened.check_integrity()["ok"], reopened.check_integrity())
        branch.accept(expected_proposal=token)
        self.assertEqual(reopened.snapshot().fs.read_file(output), "legacy proposal")
        self.assertTrue(reopened.check_integrity()["ok"], reopened.check_integrity())

    def test_schema_two_pending_nested_output_can_be_accepted_after_upgrade(self):
        token, output = self.make_schema_two(nested_output=True)
        reopened = Workspace.open(self.database)
        branch = reopened.get_branch("legacy")
        self.assertEqual(branch.inspect()["proposal_id"], token)
        branch.accept(expected_proposal=token)
        self.assertEqual(reopened.snapshot().fs.read_file(output), "legacy proposal")
        self.assertTrue(reopened.snapshot().fs.exists("new/reports"))
        self.assertTrue(reopened.check_integrity()["ok"], reopened.check_integrity())

    def test_schema_upgrade_is_persistent_and_does_not_repeat(self):
        token, _ = self.make_schema_two()
        first = Workspace.open(self.database)
        second = Workspace.open(self.database)
        self.assertEqual(first.get_branch("legacy").inspect(), second.get_branch("legacy").inspect())
        self.assertEqual(second.get_branch("legacy").inspect()["proposal_id"], token)
        with closing(sqlite3.connect(self.database)) as db:
            self.assertEqual(db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0], "3")


if __name__ == "__main__":
    unittest.main()

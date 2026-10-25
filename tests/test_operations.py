"""Storage operations, schema upgrades, and immutable proposal review contracts."""

from contextlib import closing
import multiprocessing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from agentsql import (
    BranchClosedError,
    ConflictError,
    ProposalMismatchError,
    Workspace,
    WorkspaceBusyError,
    WorkspaceError,
)
from agentsql import core


def _backup_writer(database, started, results):
    try:
        workspace = Workspace.open(database)
        started.set()
        for generation in range(1, 13):
            worker = workspace.branch(f"pair-{generation}")
            worker.write_text("left.txt", str(generation))
            worker.write_text("right.txt", str(generation))
            worker.accept()
        results.put("ok")
    except Exception as error:
        results.put(repr(error))


class OperationsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        source = self.root / "inputs"
        source.mkdir()
        (source / "left.txt").write_text("0", encoding="utf-8")
        (source / "right.txt").write_text("0", encoding="utf-8")
        self.database = self.root / "workspace.sqlite3"
        self.workspace = Workspace.create(self.database, source)

    def proposal(self, name="worker"):
        worker = self.workspace.branch(name)
        worker.write_text("answer.md", "Answer: " + worker.read_text("left.txt"))
        return worker

    def legacy(self):
        with closing(sqlite3.connect(self.database)) as db:
            for table in ("tool_events", "tool_calls", "dir_changes", "dir_reads", "dirs", "kv_changes", "kv_reads", "kv_list_reads", "kv_entries"):
                db.execute(f"DROP TABLE {table}")
            db.execute("ALTER TABLE branches DROP COLUMN listing_format")
            db.execute("DELETE FROM meta WHERE key='v3_start_revision'")
            db.execute("DROP TABLE proposals")
            db.execute("UPDATE meta SET value='1' WHERE key='schema_version'")
            db.commit()

    def test_seal_survives_reopen_and_publishes_exact_reviewed_result(self):
        worker = self.proposal()
        identifier = worker.seal()
        self.assertEqual(identifier, worker.seal())
        self.assertEqual(len(identifier), 64)
        inspected = worker.inspect()
        self.assertTrue(inspected["sealed"])
        self.assertEqual(inspected["proposal_id"], identifier)
        reviewed_diff = worker.diff()
        reopened = Workspace.open(self.database).get_branch(worker.name)
        self.assertEqual(reopened.inspect(), inspected)
        self.assertEqual(reopened.diff(), reviewed_diff)
        accepted = reopened.accept(expected_proposal=identifier)
        self.assertEqual(accepted["revision"], 2)
        self.assertEqual(self.workspace.snapshot(2).read_text("answer.md"), "Answer: 0")
        self.assertTrue(self.workspace.check_integrity()["ok"])

    def test_sealing_prevents_every_operation_that_can_mutate_provenance(self):
        worker = self.proposal()
        worker.seal()
        before = worker.inspect()
        for operation in (
            lambda: worker.read_bytes("left.txt"), lambda: worker.exists("missing.txt"),
            worker.list_files, lambda: worker.write_text("answer.md", "replacement"),
            lambda: worker.delete("left.txt"),
        ):
            with self.subTest(operation=operation), self.assertRaises(BranchClosedError):
                operation()
            self.assertEqual(worker.inspect(), before)
        self.assertIn("answer.md", worker.diff())

    def test_mismatched_or_unsealed_review_identifier_never_publishes(self):
        worker = self.proposal()
        for token in ("unreviewed", worker.seal() + "wrong"):
            with self.assertRaises(ProposalMismatchError) as caught:
                worker.accept(expected_proposal=token)
            self.assertEqual(caught.exception.as_dict()["error"], "proposal_mismatch")
            self.assertFalse(self.workspace.snapshot().exists("answer.md"))
        unsealed = self.proposal("unsealed")
        with self.assertRaises(ProposalMismatchError) as caught:
            unsealed.accept(expected_proposal="unreviewed")
        self.assertIsNone(caught.exception.actual)

    def test_stale_sealed_proposal_remains_frozen_after_rejection(self):
        worker = self.proposal()
        token = worker.seal()
        update = self.workspace.branch("update")
        update.write_text("left.txt", "1")
        update.accept()
        before = self.workspace.history()
        with self.assertRaises(ConflictError):
            worker.accept(expected_proposal=token)
        self.assertEqual(self.workspace.history(), before)
        self.assertEqual(worker.inspect()["proposal_id"], token)
        with self.assertRaises(BranchClosedError):
            worker.write_text("answer.md", "ignore conflict")

    def test_empty_or_accepted_branches_cannot_be_sealed(self):
        empty = self.workspace.branch("empty")
        with self.assertRaises(WorkspaceError):
            empty.seal()
        worker = self.proposal()
        worker.accept()
        with self.assertRaises(BranchClosedError):
            worker.seal()

    def test_backup_retains_history_pending_reads_and_sealed_proposal(self):
        worker = self.proposal()
        token = worker.seal()
        destination = self.workspace.backup(self.root / "backup.sqlite3")
        backup = Workspace.open(destination)
        self.assertEqual(backup.history(), self.workspace.history())
        self.assertEqual(backup.get_branch("worker").inspect(), worker.inspect())
        self.assertTrue(backup.check_integrity()["ok"])
        backup.get_branch("worker").accept(expected_proposal=token)
        self.assertTrue(backup.snapshot().exists("answer.md"))
        self.assertFalse(self.workspace.snapshot().exists("answer.md"))

    def test_backup_never_replaces_existing_files_or_symlinks(self):
        destination = self.root / "keep.txt"
        destination.write_text("keep", encoding="utf-8")
        symlink = self.root / "link.sqlite3"
        symlink.symlink_to(destination)
        for path in (destination, symlink, self.database):
            with self.subTest(path=path), self.assertRaises(WorkspaceError):
                self.workspace.backup(path)
        self.assertEqual(destination.read_text(), "keep")
        self.assertTrue(self.workspace.check_integrity()["ok"])

    def test_backup_during_publication_contains_one_coherent_snapshot(self):
        context = multiprocessing.get_context("spawn")
        started, results = context.Event(), context.Queue()
        process = context.Process(target=_backup_writer, args=(str(self.database), started, results))
        try:
            process.start()
            self.assertTrue(started.wait(10))
            backup = Workspace.open(self.workspace.backup(self.root / "concurrent.sqlite3"))
            snapshot = backup.snapshot()
            self.assertEqual(snapshot.read_text("left.txt"), snapshot.read_text("right.txt"))
            self.assertTrue(backup.check_integrity()["ok"])
            self.assertEqual(results.get(timeout=15), "ok")
            process.join(10)
            self.assertEqual(process.exitcode, 0)
        finally:
            if process.is_alive():
                process.terminate()
                process.join(5)
            results.close()
            results.join_thread()

    def test_backup_failure_removes_only_its_private_temporary_files(self):
        destination = self.root / "failed.sqlite3"
        with patch("agentsql.core._publish_database", side_effect=OSError("injected I/O failure")):
            with self.assertRaisesRegex(OSError, "injected I/O failure"):
                self.workspace.backup(destination)
        self.assertFalse(destination.exists())
        self.assertEqual(list(self.root.glob(".failed.sqlite3-*")), [])
        self.assertTrue(self.workspace.check_integrity()["ok"])

    def test_schema_one_migration_preserves_existing_pending_work_and_history(self):
        worker = self.proposal()
        before = worker.inspect()
        history = self.workspace.history()
        self.legacy()
        reopened = Workspace.open(self.database)
        self.assertEqual(reopened.get_branch(worker.name).inspect(), before)
        self.assertEqual(reopened.history(), history)
        token = reopened.get_branch(worker.name).seal()
        reopened.get_branch(worker.name).accept(expected_proposal=token)
        self.assertTrue(reopened.check_integrity()["ok"])

    def test_schema_migration_failure_is_atomic_and_retryable(self):
        self.legacy()
        with closing(sqlite3.connect(self.database)) as db:
            db.execute("CREATE TRIGGER prevent_upgrade BEFORE UPDATE ON meta "
                       "WHEN OLD.key='schema_version' BEGIN SELECT RAISE(ABORT,'upgrade fault'); END")
            db.commit()
        with self.assertRaisesRegex(WorkspaceError, "upgrade fault"):
            Workspace.open(self.database)
        with closing(sqlite3.connect(self.database)) as db:
            self.assertEqual(db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0], "1")
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='proposals'").fetchone())
            db.execute("DROP TRIGGER prevent_upgrade")
            db.commit()
        self.assertTrue(Workspace.open(self.database).check_integrity()["ok"])

    def test_unknown_schema_is_rejected_without_modification(self):
        with closing(sqlite3.connect(self.database)) as db:
            db.execute("UPDATE meta SET value='999' WHERE key='schema_version'")
            db.commit()
        with self.assertRaisesRegex(WorkspaceError, "Unsupported workspace schema"):
            Workspace.open(self.database)
        with closing(sqlite3.connect(self.database)) as db:
            self.assertEqual(db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0], "999")

    def test_lock_timeout_is_structured_and_failed_write_can_be_retried(self):
        worker = Workspace.open(self.database, timeout=0).branch("nonblocking")
        with closing(sqlite3.connect(self.database)) as blocker:
            blocker.execute("BEGIN IMMEDIATE")
            with self.assertRaises(WorkspaceBusyError) as caught:
                worker.write_text("answer.md", "write")
            self.assertTrue(caught.exception.as_dict()["retryable"])
            blocker.rollback()
        self.assertEqual(worker.inspect()["changes"], [])
        worker.write_text("answer.md", "write")
        worker.accept()

    def test_invalid_timeouts_revision_types_and_unicode_names_fail_cleanly(self):
        for value in (-1, float("inf"), float("nan"), True, "1", 1e300, 10**400):
            with self.subTest(timeout=value), self.assertRaises(ValueError):
                Workspace.open(self.database, timeout=value)
        with self.assertRaises(WorkspaceError):
            self.workspace.snapshot(True)
        with self.assertRaisesRegex(WorkspaceError, "UTF-8"):
            self.workspace.branch("bad-\ud800")

    def test_integrity_detects_modified_blob_and_proposal_without_repairing(self):
        worker = self.proposal()
        worker.seal()
        with closing(sqlite3.connect(self.database)) as db:
            db.execute("UPDATE blobs SET data=? WHERE hash=(SELECT hash FROM changes LIMIT 1)", (b"tampered",))
            db.execute("UPDATE proposals SET proposal_id='tampered'")
            db.commit()
        report = self.workspace.check_integrity()
        self.assertFalse(report["ok"])
        self.assertTrue(any("Blob digest mismatch" in issue for issue in report["issues"]))
        self.assertTrue(any("Sealed proposal mismatch" in issue for issue in report["issues"]))

    def test_integrity_detects_disappearing_inherited_file(self):
        worker = self.proposal()
        revision = worker.accept()["revision"]
        with closing(sqlite3.connect(self.database)) as db:
            db.execute("DELETE FROM entries WHERE revision=? AND path='right.txt'", (revision,))
            db.commit()
        report = self.workspace.check_integrity()
        self.assertFalse(report["ok"])
        self.assertTrue(any("Inherited file mismatch" in issue for issue in report["issues"]))

    def test_integrity_detects_accepted_output_not_matching_recorded_proposal(self):
        worker = self.proposal()
        worker.accept()
        with closing(sqlite3.connect(self.database)) as db:
            db.execute("UPDATE changes SET hash=(SELECT hash FROM entries WHERE revision=1 LIMIT 1)")
            db.commit()
        report = self.workspace.check_integrity()
        self.assertFalse(report["ok"])
        self.assertTrue(any("Accepted change mismatch" in issue for issue in report["issues"]))

    def test_sqlite_full_during_publication_leaves_every_output_unpublished(self):
        worker = self.workspace.branch("large-change-set")
        for number in range(300):
            worker.write_text(f"long-directory-{number}/output-{number}.txt", "data")
        before = worker.inspect()
        connect = sqlite3.connect

        def page_limited_connect(*args, **kwargs):
            db = connect(*args, **kwargs)
            pages = db.execute("PRAGMA page_count").fetchone()[0]
            db.execute(f"PRAGMA max_page_count={pages}")
            return db

        with patch("agentsql.core.sqlite3.connect", side_effect=page_limited_connect):
            with self.assertRaisesRegex(WorkspaceError, "full"):
                worker.accept()
        self.assertEqual(worker.inspect(), before)
        self.assertEqual(self.workspace.snapshot().list_files(), ["left.txt", "right.txt"])
        self.assertEqual(len(self.workspace.history()), 1)
        self.assertTrue(self.workspace.check_integrity()["ok"])
        worker.accept()
        self.assertEqual(len(self.workspace.snapshot().list_files()), 302)

    def test_creation_is_published_complete_and_never_replaces_a_racing_file(self):
        source = self.root / "new-inputs"
        source.mkdir()
        (source / "file.txt").write_text("complete", encoding="utf-8")
        target = self.root / "raced.sqlite3"
        publish = core._publish_database

        def competing_file(temporary, destination):
            self.assertFalse(destination.exists())
            self.assertEqual(Workspace.open(temporary).snapshot().read_text("file.txt"), "complete")
            destination.write_bytes(b"winner")
            publish(temporary, destination)

        with patch("agentsql.core._publish_database", side_effect=competing_file):
            with self.assertRaises(FileExistsError):
                Workspace.create(target, source)
        self.assertEqual(target.read_bytes(), b"winner")
        self.assertEqual(list(self.root.glob(".raced.sqlite3-*")), [])


if __name__ == "__main__":
    unittest.main()

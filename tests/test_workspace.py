"""Public SDK contracts and controlled faults for capture and publication."""

from __future__ import annotations

from contextlib import closing
import multiprocessing
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from agentsql import (
    BranchClosedError,
    ConflictError,
    InvalidPathError,
    Workspace,
    WorkspaceError,
)


def _accept_worker(database, branch_name, barrier, results):
    """Top-level target so the race also runs under multiprocessing spawn."""
    try:
        branch = Workspace.open(database).get_branch(branch_name)
        barrier.wait(timeout=15)
        result = branch.accept(message=f"Publish {branch_name}")
        results.put(("accepted", branch_name, result))
    except ConflictError as error:
        results.put(("conflict", branch_name, error.as_dict()))
    except BaseException as error:
        results.put(("error", branch_name, repr(error)))


def _snapshot_writer(database, barrier, results):
    try:
        workspace = Workspace.open(database)
        barrier.wait(timeout=15)
        for number in range(1, 31):
            branch = workspace.branch(f"generation-{number}")
            branch.write_text("left.txt", str(number))
            branch.write_text("right.txt", str(number))
            branch.accept()
        results.put(("complete",))
    except BaseException as error:
        results.put(("error", repr(error)))


class WorkspaceContracts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "inputs"
        self.source.mkdir()
        (self.source / "prices.csv").write_text("item,price\ncoffee,10\n", encoding="utf-8")
        (self.source / "notes.txt").write_text("original notes\n", encoding="utf-8")
        self.database = self.root / "workspace.sqlite3"
        self.workspace = Workspace.create(self.database, self.source)

    def publish(self, name, path, content):
        branch = self.workspace.branch(name)
        branch.write_text(path, content)
        return branch.accept(message=name)

    def assert_conflict(self, branch, kind, path=None):
        with self.assertRaises(ConflictError) as caught:
            branch.accept()
        evidence = caught.exception.as_dict()
        self.assertEqual(evidence["error"], "requires_recheck")
        candidates = [item for item in evidence["conflicts"] if item["kind"] == kind]
        if path is not None:
            candidates = [item for item in candidates if item["path"] == path]
        self.assertTrue(candidates, evidence)
        for item in candidates:
            self.assertIn("path", item)
            self.assertIn("expected", item)
            self.assertIn("actual", item)
            self.assertNotEqual(item["expected"], item["actual"])
        return evidence

    def test_successful_publication_preserves_source_and_records_history(self):
        before = self.workspace.history()
        branch = self.workspace.branch("worker")
        self.assertEqual(branch.read_text("prices.csv"), "item,price\ncoffee,10\n")
        branch.write_text("proposal.md", "Coffee costs 10.\n")
        self.assertFalse(self.workspace.snapshot().exists("proposal.md"))
        result = branch.accept(message="Publish checked proposal")
        self.assertEqual(result["branch"], "worker")
        self.assertIn("proposal.md", result["paths"])
        self.assertEqual(self.workspace.snapshot().read_text("proposal.md"), "Coffee costs 10.\n")
        self.assertEqual(self.workspace.snapshot().version("proposal.md"), result["revision"])
        self.assertEqual(len(self.workspace.history()), len(before) + 1)
        self.assertFalse((self.source / "proposal.md").exists())

    def test_stale_read_of_different_path_blocks_proposal_then_recompute_succeeds(self):
        original = self.workspace.snapshot().version("prices.csv")
        worker_a = self.workspace.branch("worker-a")
        worker_a.read_text("prices.csv")
        worker_a.write_text("proposal.md", "Coffee costs 10.\n")
        update = self.publish("worker-b", "prices.csv", "item,price\ncoffee,12\n")
        before = self.workspace.history()
        evidence = self.assert_conflict(worker_a, "stale_read", "prices.csv")
        detail = next(item for item in evidence["conflicts"] if item["path"] == "prices.csv")
        self.assertEqual(detail["expected"], original)
        self.assertEqual(detail["actual"], update["revision"])
        self.assertEqual(self.workspace.history(), before)
        self.assertFalse(self.workspace.snapshot().exists("proposal.md"))
        refreshed = self.workspace.branch("worker-a-recomputed")
        self.assertIn(",12", refreshed.read_text("prices.csv"))
        refreshed.write_text("proposal.md", "Coffee costs 12.\n")
        refreshed.accept()
        self.assertEqual(self.workspace.snapshot().read_text("proposal.md"), "Coffee costs 12.\n")

    def test_blind_writers_cannot_overwrite_an_accepted_competitor(self):
        first = self.workspace.branch("first")
        second = self.workspace.branch("second")
        first.write_text("notes.txt", "first result")
        second.write_text("notes.txt", "second result")
        first.accept()
        before = self.workspace.history()
        self.assert_conflict(second, "conflicting_write", "notes.txt")
        self.assertEqual(self.workspace.snapshot().read_text("notes.txt"), "first result")
        self.assertEqual(self.workspace.history(), before)

    def test_independent_writers_can_publish_from_the_same_base(self):
        first = self.workspace.branch("first")
        second = self.workspace.branch("second")
        first.write_text("first.txt", "first result")
        second.write_text("second.txt", "second result")
        first.accept()
        second.accept()
        snapshot = self.workspace.snapshot()
        self.assertEqual(snapshot.read_text("first.txt"), "first result")
        self.assertEqual(snapshot.read_text("second.txt"), "second result")

    def test_whole_change_set_is_rejected_without_publishing_uncontested_output(self):
        proposal = self.workspace.branch("proposal")
        proposal.write_text("notes.txt", "contested replacement")
        proposal.write_text("uncontested.txt", "must not leak")
        self.publish("competitor", "notes.txt", "accepted replacement")
        before = self.workspace.history()
        self.assert_conflict(proposal, "conflicting_write", "notes.txt")
        snapshot = self.workspace.snapshot()
        self.assertEqual(snapshot.read_text("notes.txt"), "accepted replacement")
        self.assertFalse(snapshot.exists("uncontested.txt"))
        self.assertEqual(self.workspace.history(), before)

    def test_database_failure_mid_publication_rolls_back_and_allows_retry(self):
        branch = self.workspace.branch("retry-after-failure")
        branch.read_text("prices.csv")
        branch.write_text("a-first.txt", "first pending output")
        branch.write_text("z-failure.txt", "second pending output")
        before_history = self.workspace.history()
        before_branch = branch.inspect()
        before_revision = self.workspace.snapshot().revision
        # This storage-level fault occurs after the revision and first pending
        # entry have been inserted. It exercises rollback, not conflict checks.
        with closing(sqlite3.connect(self.database)) as database:
            database.execute("""
                CREATE TRIGGER inject_publication_failure
                BEFORE INSERT ON entries
                WHEN NEW.path = 'z-failure.txt'
                BEGIN
                    SELECT RAISE(ABORT, 'injected publication failure');
                END
            """)
            database.commit()
        with self.assertRaisesRegex(WorkspaceError, "injected publication failure"):
            branch.accept()
        reopened = Workspace.open(self.database)
        self.assertEqual(reopened.history(), before_history)
        self.assertEqual(reopened.snapshot().revision, before_revision)
        self.assertEqual(reopened.get_branch(branch.name).inspect(), before_branch)
        self.assertFalse(reopened.snapshot().exists("a-first.txt"))
        self.assertFalse(reopened.snapshot().exists("z-failure.txt"))
        with closing(sqlite3.connect(self.database)) as database:
            database.execute("DROP TRIGGER inject_publication_failure")
            database.commit()
        reopened.get_branch(branch.name).accept()
        self.assertEqual(reopened.snapshot().read_text("a-first.txt"), "first pending output")
        self.assertEqual(reopened.snapshot().read_text("z-failure.txt"), "second pending output")
        self.assertEqual(len(reopened.history()), len(before_history) + 1)

    def test_selective_publication_is_not_exposed(self):
        branch = self.workspace.branch("whole-set")
        branch.write_text("one.txt", "one")
        branch.write_text("two.txt", "two")
        with self.assertRaises(TypeError):
            branch.accept(paths=["one.txt"])
        self.assertFalse(self.workspace.snapshot().exists("one.txt"))
        self.assertFalse(self.workspace.snapshot().exists("two.txt"))
        branch.accept()
        self.assertTrue(self.workspace.snapshot().exists("one.txt"))
        self.assertTrue(self.workspace.snapshot().exists("two.txt"))

    def test_branches_keep_their_immutable_base(self):
        branch = self.workspace.branch("slow-worker")
        self.publish("fast-worker", "notes.txt", "new shared state")
        self.assertEqual(branch.read_text("notes.txt"), "original notes\n")
        self.assertEqual(self.workspace.snapshot(branch.base_revision).read_text("notes.txt"), "original notes\n")
        branch.write_text("result.txt", "derived from original")
        self.assert_conflict(branch, "stale_read", "notes.txt")

    def test_snapshot_stays_coherent_after_later_publications(self):
        snapshot = self.workspace.snapshot()
        initial_version = snapshot.version("prices.csv")
        branch = self.workspace.branch("update-both")
        branch.write_text("prices.csv", "item,price\ncoffee,20\n")
        branch.write_text("notes.txt", "updated notes")
        branch.accept()
        self.assertEqual(snapshot.read_text("prices.csv"), "item,price\ncoffee,10\n")
        self.assertEqual(snapshot.read_text("notes.txt"), "original notes\n")
        self.assertEqual(snapshot.version("prices.csv"), initial_version)
        current = self.workspace.snapshot()
        self.assertEqual(current.read_text("prices.csv"), "item,price\ncoffee,20\n")
        self.assertEqual(current.read_text("notes.txt"), "updated notes")

    def test_binary_contents_and_unicode_paths_round_trip(self):
        branch = self.workspace.branch("binary")
        content = bytes(range(256))
        branch.write_bytes("data/café.bin", content)
        self.assertEqual(branch.read_bytes("data/café.bin"), content)
        branch.accept()
        self.assertEqual(self.workspace.snapshot().read_bytes("data/café.bin"), content)

    def test_persistence_reopen_preserves_pending_reads_and_accepted_history(self):
        branch = self.workspace.branch("pending")
        branch.read_text("prices.csv")
        branch.write_text("proposal.md", "pending proposal")
        self.publish("accepted", "notes.txt", "persisted")
        history = self.workspace.history()
        reopened = Workspace.open(self.database)
        self.assertEqual(reopened.history(), history)
        self.assertEqual(reopened.snapshot().read_text("notes.txt"), "persisted")
        self.assertEqual(reopened.get_branch("pending").read_text("proposal.md"), "pending proposal")
        updater = reopened.branch("later-update")
        updater.write_text("prices.csv", "changed")
        updater.accept()
        self.assert_conflict(reopened.get_branch("pending"), "stale_read", "prices.csv")

    def test_accepted_branch_cannot_be_modified_or_published_again(self):
        branch = self.workspace.branch("finished")
        branch.write_text("result.txt", "accepted")
        branch.accept()
        before = self.workspace.history()
        for operation in (
            lambda: branch.write_text("result.txt", "replacement"),
            lambda: branch.write_bytes("other.bin", b"replacement"),
            lambda: branch.delete("result.txt"),
            lambda: branch.accept(),
        ):
            with self.subTest(operation=operation):
                with self.assertRaises(BranchClosedError):
                    operation()
        self.assertEqual(self.workspace.history(), before)
        self.assertEqual(self.workspace.snapshot().read_text("result.txt"), "accepted")

    def test_missing_read_dependency_is_tracked_even_when_read_raises(self):
        branch = self.workspace.branch("expects-absent")
        with self.assertRaises(FileNotFoundError):
            branch.read_text("optional.txt")
        branch.write_text("decision.txt", "No optional input was available")
        self.publish("creates-input", "optional.txt", "new information")
        self.assert_conflict(branch, "stale_read", "optional.txt")

    def test_exists_tracks_both_presence_and_absence(self):
        for path, initial in (("prices.csv", True), ("missing.txt", False)):
            with self.subTest(path=path):
                branch = self.workspace.branch(f"check-{initial}")
                self.assertEqual(branch.exists(path), initial)
                branch.write_text(f"decision-{initial}.txt", "decision")
                self.publish(f"mutate-{initial}", path, "changed")
                self.assert_conflict(branch, "stale_read", path)

    def test_deleted_input_invalidates_a_reader(self):
        reader = self.workspace.branch("reader")
        reader.read_text("prices.csv")
        reader.write_text("result.txt", "derived")
        remover = self.workspace.branch("remover")
        remover.delete("prices.csv")
        removal = remover.accept()
        current = self.workspace.snapshot()
        self.assertFalse(current.exists("prices.csv"))
        self.assertEqual(current.version("prices.csv"), removal["revision"])
        self.assert_conflict(reader, "stale_read", "prices.csv")

    def test_same_bytes_restored_after_change_still_have_a_new_version(self):
        reader = self.workspace.branch("reader")
        original = reader.read_text("notes.txt")
        reader.write_text("result.txt", "derived")
        self.publish("change", "notes.txt", "temporary change")
        self.publish("restore-content", "notes.txt", original)
        self.assertEqual(self.workspace.snapshot().read_text("notes.txt"), original)
        self.assert_conflict(reader, "stale_read", "notes.txt")

    def test_explicit_same_bytes_write_advances_version_and_invalidates_reader(self):
        reader = self.workspace.branch("reader")
        original = reader.read_text("notes.txt")
        old_version = self.workspace.snapshot().version("notes.txt")
        reader.write_text("result.txt", "derived")
        self.publish("explicit-write", "notes.txt", original)
        self.assertGreater(self.workspace.snapshot().version("notes.txt"), old_version)
        self.assert_conflict(reader, "stale_read", "notes.txt")

    def test_create_then_delete_still_invalidates_original_absence(self):
        reader = self.workspace.branch("reader")
        self.assertFalse(reader.exists("optional.txt"))
        reader.write_text("result.txt", "derived from absence")
        self.publish("create", "optional.txt", "temporary input")
        remover = self.workspace.branch("remove")
        remover.delete("optional.txt")
        remover.accept()
        self.assertFalse(self.workspace.snapshot().exists("optional.txt"))
        self.assertGreater(self.workspace.snapshot().version("optional.txt"), 0)
        self.assert_conflict(reader, "stale_read", "optional.txt")

    def test_reading_deleted_input_tracks_tombstone_against_recreation(self):
        remover = self.workspace.branch("remove")
        remover.delete("notes.txt")
        removal = remover.accept()
        reader = self.workspace.branch("reader")
        with self.assertRaises(FileNotFoundError):
            reader.read_bytes("notes.txt")
        reader.write_text("result.txt", "derived from deleted input")
        self.publish("recreate", "notes.txt", "back again")
        evidence = self.assert_conflict(reader, "stale_read", "notes.txt")
        conflict = next(item for item in evidence["conflicts"] if item["path"] == "notes.txt")
        self.assertEqual(conflict["expected"], removal["revision"])

    def test_listing_tracks_membership_changes(self):
        reader = self.workspace.branch("inventory")
        self.assertEqual(set(reader.list_files()), {"notes.txt", "prices.csv"})
        reader.write_text("inventory.txt", "two source files")
        self.publish("new-file", "extra.txt", "additional input")
        self.assert_conflict(reader, "stale_listing")
        self.assertFalse(self.workspace.snapshot().exists("inventory.txt"))

    def test_listing_tracks_removals(self):
        reader = self.workspace.branch("inventory")
        reader.list_files()
        reader.write_text("inventory.txt", "two source files")
        remover = self.workspace.branch("remove")
        remover.delete("notes.txt")
        remover.accept()
        self.assert_conflict(reader, "stale_listing")

    def test_listing_does_not_claim_to_track_unread_file_contents(self):
        reader = self.workspace.branch("inventory")
        reader.list_files()
        reader.write_text("inventory.txt", "two source files")
        self.publish("content-only", "notes.txt", "changed contents, same name")
        reader.accept()
        self.assertEqual(self.workspace.snapshot().read_text("inventory.txt"), "two source files")

    def test_listing_includes_pending_additions_and_deletions(self):
        branch = self.workspace.branch("edit-inventory")
        branch.delete("notes.txt")
        branch.write_text("new.txt", "new")
        self.assertEqual(set(branch.list_files()), {"new.txt", "prices.csv"})
        branch.accept()
        self.assertEqual(set(self.workspace.snapshot().list_files()), {"new.txt", "prices.csv"})

    def test_reading_own_pending_write_does_not_report_a_base_input_read(self):
        branch = self.workspace.branch("own-output")
        branch.write_text("generated.txt", "own generated content")
        self.assertEqual(branch.read_text("generated.txt"), "own generated content")
        self.publish("competing-output", "generated.txt", "other content")
        evidence = self.assert_conflict(branch, "conflicting_write", "generated.txt")
        self.assertFalse(any(item["kind"] == "stale_read" for item in evidence["conflicts"]))

    def test_external_reads_are_not_falsely_claimed_as_tracked(self):
        branch = self.workspace.branch("untracked")
        external_content = (self.source / "prices.csv").read_text(encoding="utf-8")
        branch.write_text("proposal.md", external_content)
        self.publish("update-price", "prices.csv", "changed")
        branch.accept()
        self.assertEqual(self.workspace.snapshot().read_text("proposal.md"), external_content)

    def test_source_changes_after_capture_do_not_mutate_workspace(self):
        (self.source / "prices.csv").write_text("outside mutation", encoding="utf-8")
        (self.source / "notes.txt").unlink()
        self.assertEqual(self.workspace.snapshot().read_text("prices.csv"), "item,price\ncoffee,10\n")
        self.assertEqual(self.workspace.snapshot().read_text("notes.txt"), "original notes\n")

    def test_observed_source_change_during_capture_leaves_no_database(self):
        source = self.root / "changing-source"
        source.mkdir()
        input_file = source / "input.txt"
        input_file.write_text("original", encoding="utf-8")
        target = self.root / "failed-capture.sqlite3"
        root_info = source.stat()
        real_fstat = os.fstat
        root_checks = 0
        changed_content = "changed after the first content capture"

        def mutate_between_passes(descriptor):
            nonlocal root_checks
            result = real_fstat(descriptor)
            if (result.st_dev, result.st_ino) == (root_info.st_dev, root_info.st_ino):
                root_checks += 1
                # The second root fstat is the end of the initial capture
                # walk. Mutate the actual file before the validation walk,
                # while returning genuine metadata from every fstat call.
                if root_checks == 2:
                    input_file.write_text(changed_content, encoding="utf-8")
            return result

        with patch("agentsql.core.os.fstat", side_effect=mutate_between_passes):
            with self.assertRaisesRegex(WorkspaceError, "Source changed during capture"):
                Workspace.create(target, source)
        self.assertGreaterEqual(root_checks, 2)
        self.assertEqual(input_file.read_text(encoding="utf-8"), changed_content)
        for path in (target, Path(str(target) + "-wal"), Path(str(target) + "-shm")):
            self.assertFalse(path.exists(), f"Failed capture left database artifact: {path}")
        retried = Workspace.create(target, source)
        self.assertEqual(retried.snapshot().read_text("input.txt"), changed_content)

    def test_read_only_branch_cannot_create_a_history_entry(self):
        reader = self.workspace.branch("read-only")
        reader.read_text("notes.txt")
        before = self.workspace.history()
        with self.assertRaises(WorkspaceError):
            reader.accept()
        self.assertEqual(self.workspace.history(), before)

    def test_database_inside_capture_source_is_rejected(self):
        with self.assertRaises((WorkspaceError, ValueError)):
            Workspace.create(self.source / "self-capture.sqlite3", self.source)
        self.assertFalse((self.source / "self-capture.sqlite3").exists())

    def test_inspection_and_diff_show_pending_changes_without_publishing(self):
        branch = self.workspace.branch("review")
        branch.write_text("notes.txt", "review this replacement\n")
        branch.write_text("new.txt", "new text\n")
        self.assertIsInstance(branch.inspect(), dict)
        difference = branch.diff()
        self.assertIn("notes.txt", difference)
        self.assertIn("original notes", difference)
        self.assertIn("review this replacement", difference)
        self.assertIn("new.txt", difference)
        self.assertEqual(self.workspace.snapshot().read_text("notes.txt"), "original notes\n")
        self.assertFalse(self.workspace.snapshot().exists("new.txt"))

    def test_invalid_paths_are_rejected_by_read_write_and_snapshot(self):
        branch = self.workspace.branch("paths")
        snapshot = self.workspace.snapshot()
        invalid_paths = ("", ".", "..", "../escape", "/absolute", "a/../b", "a/./b", "a//b", "a/", "a\\b", "C:/file", "dir/name:stream", "nul\x00path")
        for path in invalid_paths:
            for operation in (
                lambda value: branch.read_bytes(value),
                lambda value: branch.write_bytes(value, b"bad"),
                lambda value: branch.delete(value),
                lambda value: branch.exists(value),
                lambda value: snapshot.read_bytes(value),
                lambda value: snapshot.version(value),
            ):
                with self.subTest(path=path, operation=operation):
                    with self.assertRaises(InvalidPathError):
                        operation(path)
        self.assertFalse((self.root / "escape").exists())

    def test_import_rejects_symlinks_to_files_and_directories(self):
        for target in (self.source / "prices.csv", self.source):
            with self.subTest(target=target):
                capture = self.root / f"symlink-source-{target.name}"
                capture.mkdir()
                (capture / "link").symlink_to(target, target_is_directory=target.is_dir())
                with self.assertRaises((WorkspaceError, ValueError)):
                    Workspace.create(self.root / f"symlink-{target.name}.sqlite3", capture)

    @unittest.skipUnless(hasattr(os, "mkfifo"), "Named pipes are unavailable on this platform")
    def test_import_rejects_special_files_without_opening_them(self):
        source = self.root / "special-source"
        source.mkdir()
        os.mkfifo(source / "pipe")
        with self.assertRaises((WorkspaceError, ValueError)):
            Workspace.create(self.root / "special.sqlite3", source)

    def test_publish_rejects_cross_branch_file_directory_collision(self):
        parent = self.workspace.branch("parent-file")
        nested = self.workspace.branch("nested-file")
        parent.write_text("report", "a file")
        nested.write_text("report/chapter.txt", "a nested file")
        parent.accept()
        before = self.workspace.history()
        with self.assertRaises(WorkspaceError):
            nested.accept()
        snapshot = self.workspace.snapshot()
        self.assertEqual(snapshot.read_text("report"), "a file")
        self.assertFalse(snapshot.exists("report/chapter.txt"))
        self.assertEqual(self.workspace.history(), before)

    def test_spawned_competing_publishers_have_exactly_one_winner(self):
        for name in ("writer-a", "writer-b"):
            branch = self.workspace.branch(name)
            branch.write_text("notes.txt", name)
            branch.write_text(f"{name}-only.txt", name)
        before = len(self.workspace.history())
        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(3)
        results = context.Queue()
        processes = [context.Process(target=_accept_worker, args=(str(self.database), name, barrier, results)) for name in ("writer-a", "writer-b")]
        try:
            for process in processes:
                process.start()
            barrier.wait(timeout=15)
            outcomes = [results.get(timeout=20) for _ in processes]
            for process in processes:
                process.join(timeout=15)
                self.assertEqual(process.exitcode, 0)
            self.assertEqual(sorted(outcome[0] for outcome in outcomes), ["accepted", "conflict"], outcomes)
            winner = next(outcome[1] for outcome in outcomes if outcome[0] == "accepted")
            loser = next(outcome[1] for outcome in outcomes if outcome[0] == "conflict")
            snapshot = self.workspace.snapshot()
            self.assertEqual(snapshot.read_text("notes.txt"), winner)
            self.assertTrue(snapshot.exists(f"{winner}-only.txt"))
            self.assertFalse(snapshot.exists(f"{loser}-only.txt"))
            self.assertEqual(len(self.workspace.history()), before + 1)
        finally:
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
            results.close()
            results.join_thread()

    def test_snapshots_are_coherent_during_another_process_publications(self):
        seed = self.workspace.branch("seed-pair")
        seed.write_text("left.txt", "0")
        seed.write_text("right.txt", "0")
        seed.accept()
        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(2)
        results = context.Queue()
        process = context.Process(target=_snapshot_writer, args=(str(self.database), barrier, results))
        try:
            process.start()
            barrier.wait(timeout=15)
            observations = 0
            while process.is_alive() or observations < 60:
                snapshot = self.workspace.snapshot()
                left = snapshot.read_text("left.txt")
                right = snapshot.read_text("right.txt")
                self.assertEqual(left, right, "A snapshot mixed two published generations")
                observations += 1
                if observations > 10000:
                    self.fail("Publisher failed to finish within 10000 snapshot reads")
            outcome = results.get(timeout=10)
            self.assertEqual(outcome, ("complete",))
            process.join(timeout=10)
            self.assertEqual(process.exitcode, 0)
            self.assertEqual(self.workspace.snapshot().read_text("left.txt"), "30")
        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            results.close()
            results.join_thread()


if __name__ == "__main__":
    unittest.main()

"""Versioned file-tree contracts: directories, atomic edits, and tracked inputs."""

from __future__ import annotations

import hashlib
import multiprocessing
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from agentsql import BranchClosedError, ConflictError, InvalidPathError, Workspace, WorkspaceError
from agentsql import files as file_module


def _replace_branch_tree(database, barrier, results):
    try:
        branch = Workspace.open(database).get_branch("tree-race")
        barrier.wait(timeout=15)
        for generation in range(20):
            data = str(generation).encode("ascii")
            branch.fs.apply_files({"left": data, "right": data}, [f"epoch-{generation}"])
        results.put(("complete",))
    except BaseException as error:
        results.put(("error", repr(error)))


class FileSystemContracts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        source = self.root / "input"
        source.mkdir()
        (source / "reports").mkdir()
        (source / "reports" / "empty").mkdir()
        (source / "reports" / "first.txt").write_text("first", encoding="utf-8")
        (source / "binary.dat").write_bytes(b"\x00\xff\xfe")
        self.workspace = Workspace.create(self.root / "workspace.db", source)

    def test_capture_preserves_empty_directories_and_reports_metadata(self):
        fs = self.workspace.snapshot().fs
        self.assertEqual(fs.readdir("/"), [
            {"name": "binary.dat", "path": "binary.dat", "type": "file"},
            {"name": "reports", "path": "reports", "type": "dir"},
        ])
        self.assertEqual(fs.readdir("reports"), [
            {"name": "empty", "path": "reports/empty", "type": "dir"},
            {"name": "first.txt", "path": "reports/first.txt", "type": "file"},
        ])
        self.assertEqual(fs.readdir("reports/empty"), [])
        self.assertEqual(fs.read_file("reports/first.txt"), "first")
        self.assertEqual(fs.read_file("binary.dat", encoding=None), b"\x00\xff\xfe")
        with self.assertRaises(UnicodeDecodeError):
            fs.read_file("binary.dat")
        self.assertEqual(fs.stat("reports/empty"), {
            "path": "reports/empty", "type": "dir", "size": 0, "version": 1, "staged": False,
        })
        self.assertEqual(fs.stat("binary.dat"), {
            "path": "binary.dat", "type": "file", "size": 3, "version": 1,
            "sha256": hashlib.sha256(b"\x00\xff\xfe").hexdigest(), "staged": False,
        })
        self.assertTrue(fs.exists(""))
        self.assertTrue(fs.exists("/"))
        self.assertEqual(fs.stat("/")["path"], "")
        self.assertFalse(fs.exists("missing"))

    def test_write_auto_parents_and_directory_only_proposals_publish(self):
        branch = self.workspace.branch("write")
        branch.fs.write_file("new/nested/text.txt", "café")
        branch.fs.write_file("new/binary", b"\x00\xff")
        self.assertEqual(branch.fs.stat("new")["version"], None)
        self.assertTrue(branch.fs.stat("new/nested/text.txt")["staged"])
        self.assertFalse(self.workspace.snapshot().fs.exists("new"))
        proposal = branch.seal()
        result = branch.accept(expected_proposal=proposal)
        self.assertEqual(self.workspace.snapshot().fs.stat("new")["version"], result["revision"])
        self.assertEqual(self.workspace.snapshot().fs.read_file("new/nested/text.txt"), "café")
        empty = self.workspace.branch("empty-dir")
        empty.fs.mkdir("only-empty")
        proposal = empty.seal()
        result = empty.accept(expected_proposal=proposal)
        self.assertEqual(self.workspace.snapshot().fs.stat("only-empty")["version"], result["revision"])
        self.assertTrue(self.workspace.check_integrity()["ok"])

    def test_independent_writers_can_share_a_new_implicit_parent_directory(self):
        left, right = self.workspace.branch("left"), self.workspace.branch("right")
        left.fs.write_file("new-shared-parent/left.txt", "left")
        right.fs.write_file("new-shared-parent/right.txt", "right")
        created_revision = left.accept()["revision"]
        right.accept()
        snapshot = self.workspace.snapshot()
        self.assertEqual(snapshot.fs.read_file("new-shared-parent/left.txt"), "left")
        self.assertEqual(snapshot.fs.read_file("new-shared-parent/right.txt"), "right")
        self.assertEqual(snapshot.fs.stat("new-shared-parent")["version"], created_revision)
        self.assertTrue(self.workspace.check_integrity()["ok"])

    def test_mkdir_validates_parents_and_exist_ok(self):
        branch = self.workspace.branch("directories")
        with self.assertRaises(FileNotFoundError):
            branch.fs.mkdir("missing/child")
        with self.assertRaises(NotADirectoryError):
            branch.fs.mkdir("binary.dat/child", parents=True)
        with self.assertRaises(FileExistsError):
            branch.fs.mkdir("binary.dat", exist_ok=True)
        with self.assertRaises(FileExistsError):
            branch.fs.mkdir("reports")
        branch.fs.mkdir("reports", exist_ok=True)
        branch.fs.mkdir("/", exist_ok=True)
        with self.assertRaises(FileExistsError):
            branch.fs.mkdir("/")
        branch.fs.mkdir("nested/a/b", parents=True)
        self.assertTrue(branch.fs.exists("nested/a/b"))
        branch.accept()
        self.assertEqual(self.workspace.snapshot().fs.readdir("nested/a"), [
            {"name": "b", "path": "nested/a/b", "type": "dir"},
        ])

    def test_remove_directory_is_atomic_and_preserves_unrelated_objects(self):
        branch = self.workspace.branch("remove")
        before = branch.fs.capture_files()
        with self.assertRaises(OSError):
            branch.fs.remove("reports")
        self.assertEqual(branch.fs.capture_files(), before)
        branch.fs.remove("reports", recursive=True)
        self.assertFalse(branch.fs.exists("reports"))
        self.assertTrue(branch.fs.exists("binary.dat"))
        self.assertTrue(self.workspace.snapshot().fs.exists("reports/first.txt"))
        branch.accept()
        self.assertFalse(self.workspace.snapshot().fs.exists("reports"))
        self.assertEqual(self.workspace.snapshot().fs.readdir(), [
            {"name": "binary.dat", "path": "binary.dat", "type": "file"},
        ])
        self.assertTrue(self.workspace.check_integrity()["ok"])

    def test_remove_file_and_empty_directory(self):
        branch = self.workspace.branch("remove-one")
        branch.fs.remove("reports/empty")
        branch.fs.remove("binary.dat")
        with self.assertRaises(FileNotFoundError):
            branch.fs.remove("binary.dat")
        branch.accept()
        self.assertEqual(self.workspace.snapshot().fs.readdir("reports"), [
            {"name": "first.txt", "path": "reports/first.txt", "type": "file"},
        ])

    def test_rename_directory_preserves_bytes_and_empty_children(self):
        branch = self.workspace.branch("rename")
        branch.fs.rename("reports", "archive")
        self.assertFalse(branch.fs.exists("reports"))
        self.assertTrue(branch.fs.exists("archive/empty"))
        self.assertEqual(branch.fs.read_file("archive/first.txt"), "first")
        self.assertTrue(self.workspace.snapshot().fs.exists("reports"))
        branch.accept()
        snapshot = self.workspace.snapshot()
        self.assertEqual(snapshot.fs.read_file("archive/first.txt"), "first")
        self.assertFalse(snapshot.fs.exists("reports"))
        self.assertTrue(snapshot.fs.exists("archive/empty"))
        self.assertTrue(self.workspace.check_integrity()["ok"])

    def test_rename_file_replacement_requires_explicit_flag(self):
        branch = self.workspace.branch("replace-file")
        with self.assertRaises(FileExistsError):
            branch.fs.rename("binary.dat", "reports/first.txt")
        with self.assertRaises(FileNotFoundError):
            branch.fs.rename("binary.dat", "missing/file")
        branch.fs.rename("binary.dat", "reports/first.txt", replace=True)
        self.assertEqual(branch.fs.read_file("reports/first.txt", encoding=None), b"\x00\xff\xfe")
        self.assertFalse(branch.fs.exists("binary.dat"))
        branch.accept()
        self.assertTrue(self.workspace.check_integrity()["ok"])

    def test_rename_rejects_invalid_type_changes_and_subtree_moves(self):
        branch = self.workspace.branch("invalid-moves")
        before = branch.fs.capture_files()
        for source, target, error in [
            ("reports", "binary.dat", NotADirectoryError),
            ("binary.dat", "reports", IsADirectoryError),
            ("reports", "reports/child", InvalidPathError),
            ("reports/empty", "reports", OSError),
            ("reports/first.txt", "binary.dat/child", NotADirectoryError),
            ("missing", "new", FileNotFoundError),
        ]:
            with self.subTest(source=source, target=target), self.assertRaises(error):
                branch.fs.rename(source, target, replace=True)
        self.assertEqual(branch.fs.capture_files(), before)
        branch.fs.rename("reports/first.txt", "reports/first.txt")
        branch.fs.mkdir("other")
        branch.fs.rename("reports/empty", "other", replace=True)
        self.assertFalse(branch.fs.exists("reports/empty"))
        branch.accept()

    def test_failed_read_records_absence_and_directory_type_dependencies(self):
        for directory in (False, True):
            with self.subTest(directory=directory):
                worker = self.workspace.branch(f"missing-reader-{directory}")
                target = f"appears-{directory}"
                with self.assertRaises(FileNotFoundError):
                    worker.fs.read_file(target)
                worker.fs.write_file(f"out-{directory}", "assumed absent")
                writer = self.workspace.branch(f"missing-writer-{directory}")
                if directory:
                    writer.fs.mkdir(target)
                else:
                    writer.fs.write_file(target, "new")
                writer.accept()
                head = self.workspace.snapshot().revision
                with self.assertRaises(ConflictError) as caught:
                    worker.accept()
                self.assertTrue(any(c["path"] == target for c in caught.exception.conflicts))
                self.assertEqual(self.workspace.snapshot().revision, head)
                self.assertFalse(self.workspace.snapshot().fs.exists(f"out-{directory}"))

    def test_failed_remove_and_rename_record_missing_source_observations(self):
        for operation in ("remove", "rename"):
            for directory in (False, True):
                with self.subTest(operation=operation, directory=directory):
                    name = f"{operation}-{directory}"
                    missing = f"missing-{name}"
                    worker = self.workspace.branch(f"observer-{name}")
                    with self.assertRaises(FileNotFoundError):
                        if operation == "remove":
                            worker.fs.remove(missing)
                        else:
                            worker.fs.rename(missing, "unused-destination")
                    self.assertEqual(worker.inspect()["changes"], [])
                    worker.fs.write_file(f"conclusion-{name}", "source did not exist")
                    other = self.workspace.branch(f"creator-{name}")
                    if directory:
                        other.fs.mkdir(missing)
                    else:
                        other.fs.write_file(missing, "appeared")
                    other.accept()
                    with self.assertRaises(ConflictError):
                        worker.accept()
                    self.assertFalse(self.workspace.snapshot().fs.exists(f"conclusion-{name}"))

    def test_failed_mkdir_records_missing_parent_or_existing_target(self):
        worker = self.workspace.branch("failed-mkdir")
        with self.assertRaises(FileNotFoundError):
            worker.fs.mkdir("missing-parent/child")
        with self.assertRaises(FileExistsError):
            worker.fs.mkdir("binary.dat")
        self.assertEqual(worker.inspect()["directories"], [])
        worker.fs.write_file("conclusion", "parent missing and target occupied")
        creator = self.workspace.branch("supply-parent")
        creator.fs.mkdir("missing-parent")
        creator.accept()
        with self.assertRaises(ConflictError):
            worker.accept()

    def test_failed_nonempty_directory_removal_records_membership_observation(self):
        worker = self.workspace.branch("nonempty-observer")
        with self.assertRaises(OSError):
            worker.fs.remove("reports")
        worker.fs.write_file("conclusion", "reports was not empty")
        editor = self.workspace.branch("empty-the-directory")
        editor.fs.remove("reports/first.txt")
        editor.fs.remove("reports/empty")
        editor.accept()
        with self.assertRaises(ConflictError):
            worker.accept()

    def test_directory_listing_detects_new_empty_directory(self):
        reader = self.workspace.branch("listing-reader")
        reader.fs.readdir("reports")
        reader.fs.write_file("inventory.txt", "empty, first")
        writer = self.workspace.branch("listing-writer")
        writer.fs.mkdir("reports/new-empty")
        writer.accept()
        with self.assertRaises(ConflictError) as caught:
            reader.accept()
        self.assertTrue(any(item["kind"] == "stale_listing" for item in caught.exception.conflicts))

    def test_directory_stat_does_not_depend_on_child_file_content(self):
        reader = self.workspace.branch("stat-reader")
        reader.fs.stat("reports")
        reader.fs.write_file("directory.txt", "reports is a directory")
        writer = self.workspace.branch("content-writer")
        writer.fs.write_file("reports/first.txt", "updated")
        writer.accept()
        reader.accept()
        self.assertEqual(self.workspace.snapshot().fs.read_file("reports/first.txt"), "updated")

    def test_directory_rename_rejects_changed_input_file(self):
        mover = self.workspace.branch("mover")
        mover.fs.rename("reports", "archive")
        writer = self.workspace.branch("source-editor")
        writer.fs.write_file("reports/first.txt", "new content")
        writer.accept()
        with self.assertRaises(ConflictError):
            mover.accept()
        self.assertEqual(self.workspace.snapshot().fs.read_file("reports/first.txt"), "new content")
        self.assertFalse(self.workspace.snapshot().fs.exists("archive"))

    def test_recursive_remove_rejects_new_child_without_losing_it(self):
        remover = self.workspace.branch("remover")
        remover.fs.remove("reports", recursive=True)
        writer = self.workspace.branch("new-child")
        writer.fs.write_file("reports/new.txt", "keep")
        writer.accept()
        with self.assertRaises(ConflictError):
            remover.accept()
        self.assertEqual(self.workspace.snapshot().fs.read_file("reports/new.txt"), "keep")

    def test_capture_and_apply_preserve_full_tree_and_skip_identical_writes(self):
        worker = self.workspace.branch("capture")
        capture = worker.fs.capture_files()
        self.assertEqual(capture["directories"], ["reports", "reports/empty"])
        capture["files"]["result.txt"] = b"done"
        worker.fs.apply_files(capture["files"], capture["directories"], expected_token=capture["token"])
        self.assertEqual([change["path"] for change in worker.inspect()["changes"]], ["result.txt"])
        worker.accept()
        self.assertEqual(self.workspace.snapshot().fs.read_file("result.txt"), "done")
        self.assertTrue(self.workspace.snapshot().fs.exists("reports/empty"))

    def test_apply_is_full_replacement_and_infers_required_parent_directories(self):
        worker = self.workspace.branch("replace-all")
        worker.fs.apply_files({"new/tree/output": b"new"}, ["new/empty"])
        self.assertFalse(worker.fs.exists("reports"))
        self.assertTrue(worker.fs.exists("new/empty"))
        self.assertTrue(self.workspace.snapshot().fs.exists("reports"))
        worker.accept()
        self.assertEqual(self.workspace.snapshot().list_files(), ["new/tree/output"])
        self.assertTrue(self.workspace.check_integrity()["ok"])

    def test_capture_tracks_unchanged_inputs_for_external_output_import(self):
        worker = self.workspace.branch("external-worker")
        capture = worker.fs.capture_files()
        capture["files"]["result"] = b"computed from initial input"
        worker.fs.apply_files(capture["files"], capture["directories"], expected_token=capture["token"])
        editor = self.workspace.branch("external-editor")
        editor.fs.write_file("reports/first.txt", "changed")
        editor.accept()
        with self.assertRaises(ConflictError):
            worker.accept()

    def test_apply_refuses_branch_content_or_directory_changes_since_capture(self):
        for directory in (False, True):
            with self.subTest(directory=directory):
                worker = self.workspace.branch(f"token-{directory}")
                capture = worker.fs.capture_files()
                if directory:
                    worker.fs.mkdir("staged-empty")
                else:
                    worker.fs.write_file("reports/first.txt", "concurrent staging")
                before = worker.inspect()
                visible = worker.fs.capture_files()
                with self.assertRaisesRegex(WorkspaceError, "changed after capture"):
                    worker.fs.apply_files(capture["files"], capture["directories"],
                                          expected_token=capture["token"])
                self.assertEqual(worker.inspect(), before)
                self.assertEqual(worker.fs.capture_files(), visible)

    def test_invalid_apply_input_leaves_branch_unchanged(self):
        worker = self.workspace.branch("bad-import")
        before = worker.fs.capture_files()
        for files, directories, error in [
            ({"../escape": b"x"}, [], InvalidPathError),
            ({"file": b"x", "file/child": b"y"}, [], InvalidPathError),
            ({"file": b"x"}, ["file"], InvalidPathError),
            ({"text": "not bytes"}, [], TypeError),
            ({"okay": b"x"}, ["/absolute"], InvalidPathError),
            ({"okay": b"x"}, "directory", TypeError),
        ]:
            with self.subTest(files=files, directories=directories), self.assertRaises(error):
                worker.fs.apply_files(files, directories)
            self.assertEqual(worker.fs.capture_files(), before)

    def test_tree_edit_storage_failure_rolls_back_all_staged_edits(self):
        worker = self.workspace.branch("fault")
        before = worker.fs.capture_files()
        original = file_module._file_change
        calls = 0

        def fail_second(*args):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("simulated import failure")
            return original(*args)

        with patch.object(file_module, "_file_change", side_effect=fail_second):
            with self.assertRaisesRegex(RuntimeError, "simulated import failure"):
                worker.fs.apply_files({"output-a": b"a", "output-b": b"b"})
        self.assertEqual(worker.fs.capture_files(), before)
        self.assertEqual(worker.inspect()["changes"], [])

    def test_whole_tree_reads_are_coherent_across_another_process_staging_outputs(self):
        worker = self.workspace.branch("tree-race")
        context = multiprocessing.get_context("spawn")
        barrier, results = context.Barrier(2), context.Queue()
        process = context.Process(target=_replace_branch_tree,
                                  args=(self.workspace.db_path, barrier, results))
        process.start()
        try:
            barrier.wait(timeout=15)
            for _ in range(30):
                capture = worker.fs.capture_files()
                if "left" in capture["files"]:
                    self.assertEqual(capture["files"]["left"], capture["files"]["right"])
                    generation = capture["files"]["left"].decode("ascii")
                    self.assertEqual(capture["directories"], [f"epoch-{generation}"])
            self.assertEqual(results.get(timeout=20), ("complete",))
            process.join(timeout=15)
            self.assertEqual(process.exitcode, 0)
        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=15)
            results.close()
            results.join_thread()
        worker.accept()
        self.assertEqual(self.workspace.snapshot().fs.read_file("left"), "19")

    def test_pending_directory_moves_survive_reopen_and_sealed_identity(self):
        worker = self.workspace.branch("reopen")
        worker.fs.rename("reports", "archive")
        worker.fs.mkdir("new-empty")
        capture = worker.fs.capture_files()
        proposal = worker.seal()
        reopened = Workspace.open(self.workspace.db_path)
        pending = reopened.get_branch("reopen")
        self.assertEqual(pending.inspect()["proposal_id"], proposal)
        result = pending.accept(expected_proposal=proposal)
        self.assertEqual(reopened.snapshot().fs.capture_files()["files"], capture["files"])
        self.assertEqual(reopened.snapshot().fs.capture_files()["directories"], capture["directories"])
        self.assertEqual(reopened.snapshot().fs.stat("archive")["version"], result["revision"])
        self.assertTrue(reopened.snapshot(1).fs.exists("reports/empty"))
        self.assertFalse(reopened.snapshot(1).fs.exists("new-empty"))

    def test_recreated_directory_has_new_version_and_invalidates_old_stat(self):
        reader = self.workspace.branch("old-stat")
        reader.fs.stat("reports/empty")
        reader.fs.write_file("answer", "the directory existed")
        remover = self.workspace.branch("delete-directory")
        remover.fs.remove("reports/empty")
        remover.accept()
        creator = self.workspace.branch("recreate-directory")
        creator.fs.mkdir("reports/empty")
        revision = creator.accept()["revision"]
        self.assertEqual(self.workspace.snapshot().fs.stat("reports/empty")["version"], revision)
        with self.assertRaises(ConflictError):
            reader.accept()

    def test_snapshot_is_immutable_and_closed_branches_cannot_track_more_inputs(self):
        snapshot = self.workspace.snapshot()
        for operation in [
            lambda: snapshot.fs.write_file("new", "x"),
            lambda: snapshot.fs.mkdir("new"),
            lambda: snapshot.fs.remove("binary.dat"),
            lambda: snapshot.fs.rename("reports", "archive"),
            lambda: snapshot.fs.apply_files({}),
        ]:
            with self.assertRaises(WorkspaceError):
                operation()
        worker = self.workspace.branch("sealed")
        worker.fs.write_file("output", "complete")
        proposal = worker.seal()
        for operation in [
            lambda: worker.fs.read_file("binary.dat", encoding=None),
            lambda: worker.fs.exists("reports"),
            lambda: worker.fs.stat("reports"),
            lambda: worker.fs.readdir(),
            lambda: worker.fs.capture_files(),
            lambda: worker.fs.write_file("new", "x"),
            lambda: worker.fs.mkdir("new"),
            lambda: worker.fs.remove("binary.dat"),
            lambda: worker.fs.rename("reports", "archive"),
            lambda: worker.fs.apply_files({}),
        ]:
            with self.assertRaises(BranchClosedError):
                operation()
        worker.accept(expected_proposal=proposal)
        with self.assertRaises(BranchClosedError):
            worker.fs.capture_files()

    def test_paths_root_and_type_errors(self):
        worker = self.workspace.branch("paths")
        for bad in ("/absolute", "a/../b", "a//b", "a\\b", "C:file", "a\x00b", "a/"):
            for operation in (
                lambda p: worker.fs.read_file(p), lambda p: worker.fs.stat(p),
                lambda p: worker.fs.readdir(p), lambda p: worker.fs.exists(p),
                lambda p: worker.fs.mkdir(p), lambda p: worker.fs.write_file(p, "x"),
                lambda p: worker.fs.remove(p), lambda p: worker.fs.rename("reports", p),
            ):
                with self.subTest(path=bad), self.assertRaises(InvalidPathError):
                    operation(bad)
        for root in ("", "/"):
            with self.assertRaises(IsADirectoryError):
                worker.fs.read_file(root)
            with self.assertRaises(IsADirectoryError):
                worker.fs.write_file(root, "x")
            with self.assertRaises(InvalidPathError):
                worker.fs.remove(root, recursive=True)
            with self.assertRaises(InvalidPathError):
                worker.fs.rename(root, "new")
        with self.assertRaises(NotADirectoryError):
            worker.fs.readdir("binary.dat")
        with self.assertRaises(FileNotFoundError):
            worker.fs.readdir("missing")
        with self.assertRaises(IsADirectoryError):
            worker.fs.read_file("reports")
        with self.assertRaises(FileNotFoundError):
            worker.fs.stat("missing")
        with self.assertRaises(TypeError):
            worker.fs.write_file("new", bytearray(b"x"))
        with self.assertRaises(InvalidPathError):
            worker.fs.write_file("reports", "cannot replace a directory")


if __name__ == "__main__":
    unittest.main()

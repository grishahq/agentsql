"""Never combine a newly published database with another database's sidecars."""

from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from agentsql import Workspace, WorkspaceError
from agentsql import core


class DestinationSidecarTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.source = self.root / "new-inputs"
        self.source.mkdir()
        (self.source / "input.txt").write_text("NEW", encoding="utf-8")
        self.workspace = Workspace.create(self.root / "source.sqlite3", self.source)

    def _old_database_with_orphaned_wal(self, target):
        """Keep a real WAL alive after an external deletion of its main file."""
        source = self.root / f"old-{target.stem}"
        source.mkdir()
        (source / "input.txt").write_text("OLD", encoding="utf-8")
        old = Workspace.create(target, source)
        holder = sqlite3.connect(target)
        self.addCleanup(holder.close)
        holder.execute("BEGIN")
        holder.execute("SELECT * FROM meta").fetchall()
        branch = old.branch("old-publisher")
        branch.write_text("input.txt", "OLD WAL")
        branch.accept()
        sidecars = {Path(str(target) + suffix): Path(str(target) + suffix).read_bytes()
                    for suffix in ("-wal", "-shm")}
        self.assertGreater(len(sidecars[Path(str(target) + "-wal")]), 32)
        # This models a manually removed main file after a failed restore or
        # interrupted cleanup. Reusing its basename must never adopt its WAL.
        target.unlink()
        return sidecars

    def _assert_rejected_without_touching_sidecars(self, target, sidecars, operation):
        with self.assertRaises(WorkspaceError):
            operation()
        self.assertFalse(target.exists())
        for path, contents in sidecars.items():
            self.assertTrue(path.exists())
            self.assertEqual(path.read_bytes(), contents)
        self.assertEqual(list(self.root.glob(f".{target.name}-*")), [])

    def test_create_refuses_real_orphaned_wal_instead_of_reading_old_workspace(self):
        target = self.root / "new.sqlite3"
        sidecars = self._old_database_with_orphaned_wal(target)
        self._assert_rejected_without_touching_sidecars(
            target, sidecars, lambda: Workspace.create(target, self.source))

    def test_backup_refuses_real_orphaned_destination_wal(self):
        target = self.root / "backup.sqlite3"
        sidecars = self._old_database_with_orphaned_wal(target)
        self._assert_rejected_without_touching_sidecars(
            target, sidecars, lambda: self.workspace.backup(target))
        self.assertEqual(self.workspace.snapshot().read_text("input.txt"), "NEW")

    def test_create_and_backup_refuse_every_existing_and_broken_symlink_sidecar(self):
        for action in ("create", "backup"):
            for suffix in ("-wal", "-shm", "-journal"):
                for kind in ("regular", "broken-symlink"):
                    with self.subTest(action=action, suffix=suffix, kind=kind):
                        target = self.root / f"{action}-{suffix[1:]}-{kind}.sqlite3"
                        sidecar = Path(str(target) + suffix)
                        if kind == "regular":
                            sidecar.write_bytes(b"existing sidecar must survive")
                        else:
                            sidecar.symlink_to(self.root / "missing-sidecar-target")
                        operation = (lambda: Workspace.create(target, self.source)) if action == "create" else (
                            lambda: self.workspace.backup(target))
                        with self.assertRaises(WorkspaceError):
                            operation()
                        self.assertFalse(target.exists())
                        if kind == "regular":
                            self.assertEqual(sidecar.read_bytes(), b"existing sidecar must survive")
                        else:
                            self.assertTrue(sidecar.is_symlink())
                            self.assertEqual(sidecar.readlink(), self.root / "missing-sidecar-target")
                        self.assertEqual(list(self.root.glob(f".{target.name}-*")), [])

    def test_create_and_backup_recheck_sidecars_immediately_before_publication(self):
        publish = core._publish_database
        for action in ("create", "backup"):
            for suffix in ("-wal", "-shm", "-journal"):
                with self.subTest(action=action, suffix=suffix):
                    target = self.root / f"raced-{action}-{suffix[1:]}.sqlite3"
                    sidecar = Path(str(target) + suffix)

                    def race(temporary, destination):
                        self.assertEqual(destination, target)
                        self.assertFalse(destination.exists())
                        self.assertTrue(temporary.exists())
                        sidecar.write_bytes(b"racing owner's sidecar")
                        publish(temporary, destination)

                    with patch("agentsql.core._publish_database", side_effect=race):
                        operation = (lambda: Workspace.create(target, self.source)) if action == "create" else (
                            lambda: self.workspace.backup(target))
                        self._assert_rejected_without_touching_sidecars(
                            target, {sidecar: b"racing owner's sidecar"}, operation)
        self.assertTrue(self.workspace.check_integrity()["ok"])


if __name__ == "__main__":
    unittest.main()

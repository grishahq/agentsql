"""Logical corruption of extended state should produce evidence, not a crash."""

from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest

from agentsql import Workspace, WorkspaceError


class PlatformIntegrityTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "workspace.sqlite3"
        self.workspace = Workspace.create(self.path)

    def test_malformed_terminal_audit_fields_are_reported_without_type_error(self):
        identifier = self.workspace.tools.record("tool", result={"ok": True})
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("UPDATE tool_calls SET duration_ms=NULL,parameters='invalid-json' WHERE id=?", (identifier,))
            db.commit()
        report = self.workspace.check_integrity()
        self.assertFalse(report["ok"])
        self.assertTrue(any("Invalid tool completion" in item for item in report["issues"]))
        self.assertTrue(any("Invalid tool parameters" in item for item in report["issues"]))

    def test_schema_three_requires_a_valid_migration_boundary(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("DELETE FROM meta WHERE key='v3_start_revision'")
            db.commit()
        with self.assertRaisesRegex(WorkspaceError, "migration boundary"):
            Workspace.open(self.path)
        with self.assertRaisesRegex(WorkspaceError, "migration boundary"):
            self.workspace.check_integrity()

    def test_state_and_directory_history_tampering_is_detected(self):
        worker = self.workspace.branch("worker")
        worker.fs.mkdir("reports")
        worker.kv.set("task", {"stage": "accepted"})
        worker.accept(expected_proposal=worker.seal())
        self.assertTrue(self.workspace.check_integrity()["ok"])
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("UPDATE kv_changes SET value='null'")
            db.execute("UPDATE dirs SET present=0 WHERE revision=2")
            db.commit()
        report = self.workspace.check_integrity()
        self.assertFalse(report["ok"])
        self.assertTrue(any("kv_entries transition" in item for item in report["issues"]))
        self.assertTrue(any("dirs transition" in item for item in report["issues"]))


if __name__ == "__main__":
    unittest.main()

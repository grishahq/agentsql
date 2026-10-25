"""Validate the installed CLI contract at process and stream boundaries."""

from __future__ import annotations

from contextlib import closing
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from agentsql import Workspace
from agentsql.cli import main


class OperationsCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="agentsql-operations-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "inputs"
        self.source.mkdir()
        (self.source / "input.txt").write_text("Original input\n", encoding="utf-8")
        self.database = self.root / "workspace.sqlite3"
        self.workspace = Workspace.create(self.database, self.source)

    def command(self, *args: str, data: bytes | None = None, expected: int = 0):
        result = subprocess.run(
            [sys.executable, "-m", "agentsql", "-w", str(self.database), *args],
            input=data, capture_output=True, timeout=15,
        )
        self.assertEqual(result.returncode, expected, result.stderr.decode("utf-8", "replace"))
        self.assertNotIn(b"Traceback", result.stderr)
        return result

    def test_sealed_review_and_expected_token_survive_cli_processes(self):
        self.command("branch", "writer")
        self.command("read", "writer", "input.txt")
        self.command("write", "writer", "result.md", "--text", "Reviewed output")
        proposal = json.loads(self.command("seal", "writer").stdout)
        self.assertEqual(proposal["branch"], "writer")
        self.assertTrue(proposal["proposal_id"])
        self.assertEqual(json.loads(self.command("seal", "writer").stdout), proposal)
        reviewed = json.loads(self.command("inspect", "writer").stdout)
        self.assertTrue(reviewed["sealed"])
        self.assertEqual(reviewed["proposal_id"], proposal["proposal_id"])
        self.assertIn(b"Reviewed output", self.command("diff", "writer").stdout)

        for args in [
            ("write", "writer", "result.md", "--text", "Unreviewed output"),
            ("read", "writer", "input.txt"),
            ("delete", "writer", "input.txt"),
            ("list", "--branch", "writer"),
        ]:
            self.assertIn("error", json.loads(self.command(*args, expected=1).stderr))
        self.assertEqual(json.loads(self.command("inspect", "writer").stdout), reviewed)

        mismatch = self.command("accept", "writer", "--proposal", "wrong-token", expected=1)
        self.assertEqual(json.loads(mismatch.stderr)["error"], "proposal_mismatch")
        self.assertEqual(len(self.workspace.history()), 1)
        accepted = json.loads(self.command(
            "accept", "writer", "--proposal", proposal["proposal_id"], "-m", "Reviewed result",
        ).stdout)
        self.assertEqual(accepted["revision"], 2)
        self.assertEqual(self.workspace.snapshot().read_text("result.md"), "Reviewed output")

    def test_expected_proposal_requires_a_seal(self):
        worker = self.workspace.branch("worker")
        worker.write_text("result.txt", "result")
        rejected = self.command("accept", "worker", "--proposal", "unsealed", expected=1)
        self.assertEqual(json.loads(rejected.stderr)["error"], "proposal_mismatch")
        self.assertEqual(len(self.workspace.history()), 1)
        self.assertFalse(self.workspace.snapshot().exists("result.txt"))

    def test_sealed_stale_dependency_rejects_and_keeps_review_evidence(self):
        worker = self.workspace.branch("worker")
        worker.read_text("input.txt")
        worker.write_text("result.txt", "Based on original input")
        proposal_id = worker.seal()
        updater = self.workspace.branch("update")
        updater.write_text("input.txt", "Changed input")
        updater.accept()
        rejected = self.command("accept", "worker", "--proposal", proposal_id, expected=2)
        self.assertEqual(json.loads(rejected.stderr)["error"], "requires_recheck")
        inspected = json.loads(self.command("inspect", "worker").stdout)
        self.assertEqual(inspected["proposal_id"], proposal_id)
        self.assertTrue(inspected["sealed"])
        self.assertFalse(self.workspace.snapshot().exists("result.txt"))
        self.assertEqual(len(self.workspace.history()), 2)

    def test_backup_is_reopenable_and_independent_including_pending_review(self):
        worker = self.workspace.branch("pending")
        worker.read_text("input.txt")
        worker.write_text("result.txt", "Prepared")
        proposal_id = worker.seal()
        destination = self.root / "backup.sqlite3"
        report = json.loads(self.command("backup", str(destination)).stdout)
        self.assertEqual(Path(report["backup"]), destination.resolve())
        backup = Workspace.open(destination)
        self.assertEqual(backup.history(), self.workspace.history())
        self.assertEqual(backup.get_branch("pending").inspect()["proposal_id"], proposal_id)
        accepted = worker.accept(expected_proposal=proposal_id)
        self.assertEqual(accepted["revision"], 2)
        self.assertEqual(len(backup.history()), 1)
        self.assertFalse(backup.snapshot().exists("result.txt"))
        checked = subprocess.run(
            [sys.executable, "-m", "agentsql", "-w", str(destination), "check"],
            capture_output=True, timeout=15,
        )
        self.assertEqual(checked.returncode, 0, checked.stderr)
        self.assertTrue(json.loads(checked.stdout)["ok"])

    def test_backup_never_overwrites_an_existing_file_or_symlink(self):
        existing = self.root / "retained.txt"
        existing.write_bytes(b"Keep this file")
        symlink = self.root / "linked.sqlite3"
        symlink.symlink_to(existing)
        for destination in [existing, symlink, self.root]:
            result = self.command("backup", str(destination), expected=1)
            self.assertIn("error", json.loads(result.stderr))
            self.assertEqual(existing.read_bytes(), b"Keep this file")
        self.assertTrue(symlink.is_symlink())
        self.assertEqual(len(self.workspace.history()), 1)

    def test_check_detects_blob_corruption_as_a_failed_structured_report(self):
        self.assertTrue(json.loads(self.command("check").stdout)["ok"])
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute("UPDATE blobs SET data = ?", (b"Corrupted data",))
            connection.commit()
        result = self.command("check", expected=1)
        self.assertFalse(json.loads(result.stdout)["ok"])
        self.assertEqual(result.stderr, b"")

    def test_lock_timeout_is_structured_and_failed_write_changes_nothing(self):
        connection = sqlite3.connect(self.database)
        self.addCleanup(connection.close)
        connection.execute("BEGIN IMMEDIATE")
        result = self.command("--timeout", "0", "branch", "blocked", expected=1)
        report = json.loads(result.stderr)
        self.assertEqual(report["error"], "workspace_busy")
        self.assertTrue(report["retryable"])
        connection.rollback()
        self.assertEqual(self.workspace.branches(), [])
        self.command("--timeout", "0.1", "branch", "unblocked")

    def test_invalid_timeout_and_usage_are_json_without_opening_database(self):
        for value in ["nan", "inf", "-inf", "-1", "1e300", "2147483.648", "invalid"]:
            rejected = self.command(f"--timeout={value}", "check", expected=2)
            self.assertEqual(json.loads(rejected.stderr)["error"], "UsageError")
        for arguments in [(), ("accept",), ("unknown-command",), ("check", "--unknown")]:
            rejected = self.command(*arguments, expected=2)
            self.assertEqual(json.loads(rejected.stderr)["error"], "UsageError")
        self.assertEqual(len(self.workspace.history()), 1)

    def test_binary_file_and_stdin_are_returned_byte_for_byte(self):
        self.workspace.branch("binary")
        payload = bytes(range(256)) * 8192 + b"no trailing newline"
        source = self.root / "données.bin"
        source.write_bytes(payload)
        self.command("write", "binary", "output.bin", "--from-file", str(source))
        self.assertEqual(self.command("read", "binary", "output.bin").stdout, payload)
        self.command("write", "binary", "stdin.bin", data=payload)
        self.command("accept", "binary")
        self.assertEqual(self.command("show", "stdin.bin").stdout, payload)
        self.assertEqual(self.command("show", "output.bin").stdout, payload)

    def test_missing_and_directory_input_files_fail_before_staging(self):
        self.workspace.branch("worker")
        for source in [self.root / "missing.bin", self.root]:
            result = self.command("write", "worker", "out.bin", "--from-file", str(source), expected=1)
            self.assertIn("error", json.loads(result.stderr))
        self.assertEqual(self.workspace.get_branch("worker").inspect()["changes"], [])

    def test_invalid_unicode_text_is_a_structured_error(self):
        self.workspace.branch("worker")
        diagnostics = io.StringIO()
        with patch("sys.stderr", diagnostics):
            status = main(["-w", str(self.database), "write", "worker", "out.txt", "--text", "\udcff"])
        self.assertEqual(status, 1)
        self.assertEqual(json.loads(diagnostics.getvalue())["error"], "UnicodeEncodeError")
        self.assertEqual(self.workspace.get_branch("worker").inspect()["changes"], [])

    def test_non_utf8_terminal_does_not_corrupt_json_errors_or_file_bytes(self):
        worker = self.workspace.branch("données")
        worker.write_text("résultat.md", "Résumé")
        environment = dict(os.environ, PYTHONIOENCODING="ascii")
        command = [sys.executable, "-m", "agentsql", "-w", str(self.database)]
        inspected = subprocess.run(command + ["inspect", worker.name], env=environment,
                                   capture_output=True, timeout=15)
        self.assertEqual(inspected.returncode, 0, inspected.stderr)
        self.assertEqual(json.loads(inspected.stdout)["name"], worker.name)
        failed_diff = subprocess.run(command + ["diff", worker.name], env=environment,
                                    capture_output=True, timeout=15)
        self.assertEqual(failed_diff.returncode, 1)
        self.assertEqual(json.loads(failed_diff.stderr)["error"], "UnicodeEncodeError")
        raw = subprocess.run(command + ["read", worker.name, "résultat.md"], env=environment,
                             capture_output=True, timeout=15)
        self.assertEqual(raw.returncode, 0, raw.stderr)
        self.assertEqual(raw.stdout, "Résumé".encode("utf-8"))

    def test_interrupt_returns_standard_status_and_json(self):
        diagnostics = io.StringIO()
        with patch("agentsql.cli.Workspace.open", side_effect=KeyboardInterrupt), patch("sys.stderr", diagnostics):
            status = main(["-w", str(self.database), "check"])
        self.assertEqual(status, 130)
        self.assertEqual(json.loads(diagnostics.getvalue())["error"], "KeyboardInterrupt")

    @unittest.skipUnless(os.name == "posix", "POSIX closed-pipe behavior")
    def test_closed_stdout_pipe_has_no_shutdown_traceback(self):
        worker = self.workspace.branch("large-output")
        worker.write_bytes("large.bin", b"x" * 1048576)
        worker.accept()
        # Large raw output fails in write(); short JSON fails only when flushed.
        for arguments in [("show", "large.bin"), ("branches",)]:
            with self.subTest(command=arguments):
                read_end, write_end = os.pipe()
                os.close(read_end)
                try:
                    result = subprocess.run(
                        [sys.executable, "-m", "agentsql", "-w", str(self.database), *arguments],
                        stdout=write_end, stderr=subprocess.PIPE, timeout=15,
                    )
                finally:
                    os.close(write_end)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stderr, b"")


if __name__ == "__main__":
    unittest.main()

"""Verify the complete platform demo as an application outside the checkout."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from agentsql import Workspace


EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "platform_demo.py"


class PlatformDemoTests(unittest.TestCase):
    def test_external_application_retains_seven_verified_outcomes_and_checkpoint(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            destination = root / "evidence"
            result = subprocess.run(
                [sys.executable, str(EXAMPLE), "--directory", str(destination)],
                cwd=root, capture_output=True, text=True, timeout=45,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(report["passed"], 7)
            self.assertEqual(len(report["outcomes"]), report["passed"])
            self.assertIn("simulated", report["actors"])
            self.assertEqual(json.loads((destination / "results.json").read_text()), report)
            self.assertTrue(any(item.get("resource") == "kv" and item["kind"] == "stale_read"
                                for item in report["stale_rejection"]["conflicts"]))

            workspace = Workspace.open(destination / "workspace.sqlite3")
            current = workspace.snapshot()
            self.assertEqual(report["shared_revision"], current.revision)
            self.assertEqual(current.kv.get("quotes/total"), 360)
            self.assertEqual(current.kv.get("workflow/mcp"), "ready")
            self.assertEqual(current.fs.stat("empty-results")["type"], "dir")
            self.assertIn("$360", current.fs.read_file("reports/proposal.md"))
            self.assertIn("$360", current.fs.read_file("reports/command-output.txt"))
            self.assertEqual(current.fs.read_file("reports/mcp-note.md"), "Prepared through MCP\n")
            self.assertEqual(current.fs.read_file("reports/variant.md"), "Approach one\n")
            self.assertTrue((destination / "simulated-notification.txt").is_file())
            self.assertEqual(workspace.get_branch("simulated-agent-a").inspect()["status"], "open")
            self.assertTrue(workspace.get_branch("simulated-agent-a").inspect()["sealed"])
            self.assertEqual(workspace.get_branch("variant-right").inspect()["status"], "open")
            checkpoint = Workspace.open(destination / "checkpoint.sqlite3")
            self.assertEqual(checkpoint.history(), workspace.history())
            self.assertEqual(checkpoint.tools.list(), workspace.tools.list())
            self.assertEqual(checkpoint.branches(), workspace.branches())
            self.assertTrue(checkpoint.check_integrity()["ok"])
            self.assertTrue(workspace.check_integrity()["ok"])

    def test_default_temporary_demo_works_from_an_external_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = subprocess.run(
                [sys.executable, str(EXAMPLE)], cwd=temporary,
                capture_output=True, text=True, timeout=45,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(json.loads(result.stdout)["passed"], 7)
            self.assertEqual(list(Path(temporary).iterdir()), [])

    def test_nonempty_destination_fails_without_creating_a_database(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            marker = root / "keep.txt"
            marker.write_text("existing data", encoding="utf-8")
            result = subprocess.run(
                [sys.executable, str(EXAMPLE), "--directory", str(root)],
                cwd=root, capture_output=True, text=True, timeout=15,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("new or empty", result.stderr)
            self.assertEqual(marker.read_text(encoding="utf-8"), "existing data")
            self.assertEqual(list(root.iterdir()), [marker])


if __name__ == "__main__":
    unittest.main()

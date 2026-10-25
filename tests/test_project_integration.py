"""Validate the reusable integration example as an application consumer."""

import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from agentsql import ConflictError, Workspace


EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "project_integration.py"


class ProjectIntegrationTests(unittest.TestCase):
    def test_external_application_recomputes_and_exports_exact_accepted_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "application"
            command = [sys.executable, str(EXAMPLE), "--directory", str(output)]
            run = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=20)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertIn("requires_recheck", run.stdout)
            self.assertIn("Integration passed", run.stdout)
            workspace = Workspace.open(output / "workspace.sqlite3")
            accepted = workspace.snapshot(3)
            self.assertIn("$360.00", accepted.read_text("proposal.md"))
            self.assertEqual((output / "accepted-proposal.md").read_bytes(), accepted.read_bytes("proposal.md"))
            self.assertIn("100.00", (output / "inputs" / "prices.csv").read_text())
            self.assertEqual(workspace.get_branch("purchase-001-attempt-1").inspect()["status"], "open")
            history = workspace.history()
            repeated = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=20)
            self.assertNotEqual(repeated.returncode, 0)
            self.assertEqual(workspace.history(), history)

    def test_continuously_changing_inputs_exhaust_retry_budget_without_publication(self):
        spec = importlib.util.spec_from_file_location("project_integration_example", EXAMPLE)
        example = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(example)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "inputs"
            source.mkdir()
            (source / "prices.csv").write_text("100", encoding="utf-8")
            database = root / "workspace.sqlite3"
            workspace = Workspace.create(database, source)
            calls = []

            def generate_with_concurrent_price_update(prices):
                calls.append(prices)
                updater = workspace.branch(f"price-update-{len(calls)}")
                updater.write_text("prices.csv", str(int(prices) + 1))
                updater.accept()
                return f"Proposal based on {prices}"

            first = example.prepare_attempt(database, "task-attempt-1", generate_with_concurrent_price_update)
            with self.assertRaises(ConflictError):
                example.publish_with_recompute(
                    database, "task", first, generate_with_concurrent_price_update,
                    max_attempts=3, emit=lambda message: None,
                )
            self.assertEqual(calls, ["100", "101", "102"])
            self.assertEqual(workspace.snapshot().read_text("prices.csv"), "103")
            self.assertFalse(workspace.snapshot().exists("proposal.md"))
            self.assertEqual(len(workspace.history()), 4)


if __name__ == "__main__":
    unittest.main()

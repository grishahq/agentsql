"""Exercise the public CLI as separate processes, including structured rejection."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from agentsql import Workspace


class CliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="agentsql-cli-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        (self.source / "prices.csv").write_text("item,price\nwidget,100\n", encoding="utf-8")
        self.database = self.root / "workspace.sqlite3"

    def cli(self, *arguments: str, data: bytes | None = None, expected: int = 0):
        result = subprocess.run(
            [sys.executable, "-m", "agentsql", "-w", str(self.database), *arguments],
            input=data, capture_output=True, timeout=15,
        )
        self.assertEqual(result.returncode, expected, result.stderr.decode("utf-8", errors="replace"))
        return result

    def create(self) -> None:
        self.assertTrue(json.loads(self.cli("create", str(self.source)).stdout)["created"])

    def test_module_help_uses_public_product_name(self) -> None:
        help_text = self.cli("--help").stdout.decode("utf-8")
        self.assertTrue(help_text.startswith("usage: agentsql "), help_text)
        self.assertIn("AgentSQL: persistent agent state", help_text)

    def test_binary_stdin_round_trip_and_historical_show(self) -> None:
        self.create()
        self.cli("branch", "worker")
        payload = b"\x00\xffbinary\nno-final-newline"
        self.cli("write", "worker", "nested/output.bin", data=payload)
        self.assertEqual(self.cli("read", "worker", "nested/output.bin").stdout, payload)
        accepted = json.loads(self.cli("accept", "worker", "-m", "Binary output").stdout)
        self.assertEqual(accepted["revision"], 2)
        self.assertEqual(self.cli("show", "nested/output.bin").stdout, payload)
        self.assertEqual(
            self.cli("show", "prices.csv", "--revision", "1").stdout,
            b"item,price\nwidget,100\n",
        )
        self.assertEqual(len(json.loads(self.cli("history").stdout)), 2)

    def test_conflict_exit_code_evidence_and_atomic_rejection(self) -> None:
        self.create()
        self.cli("branch", "agent-a")
        self.cli("read", "agent-a", "prices.csv")
        self.cli("write", "agent-a", "proposal.md", "--text", "Uses the old price")
        self.cli("branch", "agent-b")
        self.cli("write", "agent-b", "prices.csv", "--text", "item,price\nwidget,120\n")
        self.cli("accept", "agent-b")
        rejected = self.cli("accept", "agent-a", expected=2)
        self.assertEqual(rejected.stdout, b"")
        evidence = json.loads(rejected.stderr)
        self.assertEqual(evidence["error"], "requires_recheck")
        self.assertIn(
            {"kind": "stale_read", "path": "prices.csv", "expected": 1, "actual": 2},
            evidence["conflicts"],
        )
        shared = Workspace.open(self.database)
        self.assertFalse(shared.snapshot().exists("proposal.md"))
        self.assertEqual(len(shared.history()), 2)
        self.assertIn("proposal.md", self.cli("diff", "agent-a").stdout.decode("utf-8"))

    def test_invalid_path_returns_json_without_publication(self) -> None:
        self.create()
        self.cli("branch", "worker")
        rejected = self.cli("write", "worker", "../escape", "--text", "unsafe", expected=1)
        self.assertEqual(json.loads(rejected.stderr)["error"], "InvalidPathError")
        self.assertEqual(json.loads(self.cli("inspect", "worker").stdout)["changes"], [])

    def test_demo_recomputes_and_preserves_retained_results(self) -> None:
        directory = self.root / "demo"
        result = self.cli("demo", "--directory", str(directory))
        self.assertIn(b"Simulated agent workers", result.stdout)
        self.assertIn(b"Demo passed", result.stdout)
        shared = Workspace.open(directory / "workspace.sqlite3")
        self.assertIn("$360.00", shared.snapshot().read_text("proposal.md"))
        self.assertEqual(len(shared.history()), 3)
        rejected = self.cli("demo", "--directory", str(directory), expected=1)
        self.assertEqual(json.loads(rejected.stderr)["error"], "ValueError")
        self.assertEqual(len(Workspace.open(directory / "workspace.sqlite3").history()), 3)


if __name__ == "__main__":
    unittest.main()

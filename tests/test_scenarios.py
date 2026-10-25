"""The scenario runner must work as an installed-package consumer outside the repo."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "examples" / "scenarios.py"


class ScenarioRunnerTests(unittest.TestCase):
    def test_json_report_and_retained_workspaces_from_external_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "evidence"
            result = subprocess.run(
                [sys.executable, str(SCRIPT), "--json", "--directory", str(destination)],
                cwd=temporary, capture_output=True, text=True, timeout=90,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(report["passed"], 11)
            self.assertEqual(report["total"], 11)
            self.assertIn("Simulated", report["actors"])
            for scenario in report["results"]:
                self.assertEqual(scenario["status"], "PASS", scenario)
                database = destination / scenario["scenario"] / "workspace.sqlite3"
                self.assertTrue(database.is_file(), str(database))
            boundary = next(item for item in report["results"]
                            if item["scenario"] == "untracked_input_boundary")
            self.assertFalse(boundary["evidence"]["detection_expected"])

    def test_nonempty_destination_fails_without_modifying_existing_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / "keep.txt"
            marker.write_text("keep me", encoding="utf-8")
            result = subprocess.run(
                [sys.executable, str(SCRIPT), "--json", "--directory", temporary],
                cwd=temporary, capture_output=True, text=True, timeout=15,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("new or empty", json.loads(result.stdout)["error"])
            self.assertEqual(marker.read_text(encoding="utf-8"), "keep me")
            self.assertEqual(list(Path(temporary).iterdir()), [marker])


if __name__ == "__main__":
    unittest.main()

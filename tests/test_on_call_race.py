"""Business-state oracles and lifecycle failures for the controlled race lab."""

from __future__ import annotations

import importlib
import json
import multiprocessing
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "on_call_race.py"
# Spawned targets must be importable in both this process and fresh children.
sys.path.insert(0, str(EXAMPLE.parent))
lab = importlib.import_module("on_call_race")


class OnCallRaceTests(unittest.TestCase):
    def cli(self, directory, *arguments):
        return subprocess.run([sys.executable, str(EXAMPLE), *arguments], cwd=directory,
                              capture_output=True, text=True, timeout=30)

    def test_external_cli_verifies_both_orders_and_actual_business_outcomes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "report.json"
            result = self.cli(root, "--rounds", "2", "--output", str(output))
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(json.loads(output.read_text()), report)
            self.assertEqual(report["schema_version"], 1)
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(len(report["trials"]), 8)
            self.assertNotIn(str(root), result.stdout)
            expected = {
                "snapshot_reads_with_tool_log": (4, 0, 0, 2),
                "tracked_branch_reads": (2, 0, 2, 0),
                "tracked_plus_recompute": (2, 2, 2, 0),
                "independent_teams": (4, 0, 0, 0),
            }
            for case, (approvals, denials, rechecks, violations) in expected.items():
                summary = report["summary"][case]
                self.assertEqual(summary["accepted_approvals"], approvals)
                self.assertEqual(summary["accepted_denials"], denials)
                self.assertEqual(summary["accepted_proposals"], approvals + denials)
                self.assertEqual(summary["rechecks"], rechecks)
                self.assertEqual(summary["coverage_violations"], violations)
                self.assertEqual(summary["incomplete_publications"], 0)
                self.assertEqual(summary["acceptance_orders"], {"alice_first": 1, "bob_first": 1})
            for trial in report["trials"]:
                first, second = trial["workers"]
                self.assertEqual([first["actor"], second["actor"]], trial["acceptance_order"])
                self.assertEqual(first["publication"], "accepted")
                oracle = trial["oracle"]
                self.assertTrue(oracle["seed_state"]["coverage_valid"])
                self.assertTrue(oracle["accepted_revisions"][0]["coverage_valid"])
                self.assertTrue(oracle["rejected_proposals_absent_after_race"])
                if trial["case"] == "snapshot_reads_with_tool_log":
                    self.assertFalse(oracle["final_coverage_valid"])
                    self.assertEqual(first["kv_reads"], [])
                    self.assertEqual(second["kv_reads"], [])
                else:
                    self.assertTrue(oracle["final_coverage_valid"])
                    self.assertEqual(len(first["kv_reads"]), 2)
                    self.assertEqual(len(second["kv_reads"]), 2)
                if trial["case"] == "tracked_plus_recompute":
                    self.assertEqual(trial["recompute"]["actor"], second["actor"])
                    self.assertEqual(trial["recompute"]["decision"], "denied")
                    self.assertEqual(trial["recompute"]["publication"], "accepted")
                    self.assertEqual(len(oracle["accepted_revisions"]), 2)

    def test_oracle_rejects_canonical_state_and_partial_artifact_corruption(self):
        for corruption in ("unexplained_roster", "file_without_state", "mismatched_artifacts"):
            with self.subTest(corruption=corruption), tempfile.TemporaryDirectory() as temporary:
                workspace, specs = lab._seed(Path(temporary))
                bad = workspace.branch("invalid-business-result")
                if corruption == "unexplained_roster":
                    bad.kv.set(specs[0]["own_key"], False)
                else:
                    bad.fs.write_file("decisions/alice.json", '{"decision":"approved"}')
                    if corruption == "mismatched_artifacts":
                        bad.kv.set("decisions/alice", {"decision": "denied"})
                bad.accept(expected_proposal=bad.seal())
                with self.assertRaises(lab.LabError):
                    lab._observe_revision(workspace.snapshot(), specs)

    def test_failed_worker_aborts_experiment_and_cleans_up_processes(self):
        before = {process.pid for process in multiprocessing.active_children()}
        with tempfile.TemporaryDirectory() as temporary:
            workspace, specs = lab._seed(Path(temporary))
            specs[0]["peer_key"] = "roster/missing/person"
            with self.assertRaises(lab.LabError):
                lab._publish_parallel(workspace, specs, "tracked_branch_reads", "alice")
            self.assertEqual(len(workspace.history()), 2)
        self.assertEqual({process.pid for process in multiprocessing.active_children()}, before)

    def test_cli_refuses_existing_paths_and_invalid_rounds_before_running(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "keep.json"
            output.write_text("keep", encoding="utf-8")
            dangling = root / "dangling.json"
            dangling.symlink_to(root / "missing.json")
            for path in (output, dangling):
                result = self.cli(root, "--rounds", "100", "--output", str(path))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("never overwritten", result.stderr)
            self.assertEqual(output.read_text(), "keep")
            self.assertTrue(dangling.is_symlink())
            for rounds in ("0", "101", "invalid"):
                result = self.cli(root, "--rounds", rounds)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("between 1 and 100", result.stderr)


if __name__ == "__main__":
    unittest.main()

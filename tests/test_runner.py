"""Materialized process execution, bounded output and atomic import contracts."""

from __future__ import annotations

import base64
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from agentsql import ConflictError, Workspace, WorkspaceBusyError
from agentsql.audit import ToolLog
from agentsql.runner import MAX_OUTPUT_BYTES, _run_process, _stop_process, run_command


class CommandRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        (self.source / "prices.csv").write_text("coffee,10\n", encoding="utf-8")
        (self.source / "notes.txt").write_text("original\n", encoding="utf-8")
        self.workspace = Workspace.create(self.root / "state.db", self.source)
        self.branch = self.workspace.branch("worker")

    def run_python(self, script, *, timeout=10, env=None):
        return run_command(self.workspace, "worker", [sys.executable, "-c", script],
                           timeout=timeout, env=env)

    def test_real_file_operations_are_imported_without_touching_original_directory(self):
        result = self.run_python("from pathlib import Path; "
            "data=Path('prices.csv').read_text(); "
            "Path('result.md').write_text('report: '+data); "
            "Path('notes.txt').unlink(); "
            "Path('empty/nested').mkdir(parents=True); print('done')")
        self.assertTrue(result["imported"], result)
        self.assertEqual(result["returncode"], 0)
        self.assertEqual(result["stdout"], "done\n")
        self.assertEqual(result["changed_files"], ["notes.txt", "result.md"])
        self.assertEqual(self.branch.fs.read_file("result.md"), "report: coffee,10\n")
        self.assertTrue(self.branch.fs.exists("empty/nested"))
        self.assertFalse(self.branch.fs.exists("notes.txt"))
        self.assertEqual((self.source / "notes.txt").read_text(), "original\n")
        self.assertFalse((self.source / "result.md").exists())
        proposal = self.branch.seal()
        self.branch.accept(expected_proposal=proposal)
        self.assertEqual(self.workspace.snapshot().fs.read_file("result.md"), "report: coffee,10\n")

    def test_all_exported_inputs_are_tracked_even_if_command_does_not_read_them(self):
        result = self.run_python("from pathlib import Path; Path('report.md').write_text('constant')")
        self.assertTrue(result["imported"], result)
        self.assertTrue({"notes.txt", "prices.csv"}.issubset(
            {item["path"] for item in self.branch.inspect()["reads"]}))
        other = self.workspace.branch("prices")
        other.fs.write_file("prices.csv", "coffee,12\n")
        other.accept()
        with self.assertRaises(ConflictError):
            self.branch.accept()

    def test_noop_run_does_not_create_file_changes(self):
        result = self.run_python("print('only reads')")
        self.assertTrue(result["imported"], result)
        self.assertEqual(result["changed_files"], [])
        self.assertEqual(self.branch.inspect()["changes"], [])

    def test_failed_command_preserves_existing_pending_outputs(self):
        self.branch.fs.write_file("pending.txt", "keep")
        before = self.branch.inspect()["changes"]
        result = self.run_python("from pathlib import Path; import sys; "
            "Path('pending.txt').write_text('overwrite'); Path('bad').write_text('partial'); "
            "print('error',file=sys.stderr); sys.exit(7)")
        self.assertFalse(result["imported"])
        self.assertEqual(result["returncode"], 7)
        self.assertEqual(result["error"], "command_failed")
        self.assertEqual(result["stderr"], "error\n")
        self.assertEqual(self.branch.inspect()["changes"], before)
        self.assertEqual(self.branch.fs.read_file("pending.txt"), "keep")

    def test_timeout_discards_partial_output_and_kills_background_children(self):
        marker = self.root / "escaped-child-marker"
        script = ("from pathlib import Path; import subprocess,sys,time; "
                  "Path('partial').write_text('do not import'); "
                  "subprocess.Popen([sys.executable,'-c',"
                  f"\"import time; from pathlib import Path; time.sleep(1); Path({str(marker)!r}).touch()\"]); "
                  "time.sleep(10)")
        result = self.run_python(script, timeout=0.2)
        self.assertTrue(result.get("timed_out"), result)
        self.assertFalse(result["imported"])
        self.assertEqual(result["error"], "command_timeout")
        self.assertFalse(self.branch.fs.exists("partial"))
        # The captured child PID/process group is killed before return. Waiting
        # past its delay verifies that no surviving writer creates the marker.
        import time
        time.sleep(1.1)
        self.assertFalse(marker.exists())

    def test_stdout_stderr_are_bounded_and_binary_preserved(self):
        result = self.run_python("import os; "
            f"os.write(1,b'x'*{MAX_OUTPUT_BYTES * 4}); "
            "os.write(2,b'\\xff\\x00stderr')")
        self.assertTrue(result["imported"], result)
        self.assertEqual(len(result["stdout"]), MAX_OUTPUT_BYTES)
        self.assertTrue(result["stdout_truncated"])
        self.assertEqual(base64.b64decode(result["stderr_base64"]), b"\xff\0stderr")
        self.assertFalse(result["stderr_truncated"])

    def test_selector_creation_failure_does_not_launch_a_child(self):
        with patch("agentsql.runner.selectors.DefaultSelector", side_effect=OSError("selector unavailable")), \
                patch("agentsql.runner.subprocess.Popen") as spawn:
            result = self.run_python("import time; time.sleep(60)")
        spawn.assert_not_called()
        self.assertFalse(result["imported"])
        self.assertEqual(result["error"]["message"], "selector unavailable")
        self.assertEqual(self.branch.inspect()["changes"], [])

    def test_partial_selector_registration_failure_reaps_child_and_closes_resources(self):
        selector = selectors.DefaultSelector()
        register = selector.register
        spawn = subprocess.Popen
        children = []

        def track_spawn(*args, **kwargs):
            child = spawn(*args, **kwargs)
            children.append(child)
            return child

        def fail_second_registration(*args, **kwargs):
            if selector.get_map():
                raise OSError("second registration failed")
            return register(*args, **kwargs)

        with patch("agentsql.runner.selectors.DefaultSelector", return_value=selector), \
                patch.object(selector, "register", side_effect=fail_second_registration), \
                patch("agentsql.runner.subprocess.Popen", side_effect=track_spawn), \
                self.assertRaisesRegex(OSError, "second registration failed"):
            _run_process([sys.executable, "-c", "import time; time.sleep(60)"], self.root, 10, None)
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].returncode)
        self.assertTrue(children[0].stdout.closed)
        self.assertTrue(children[0].stderr.closed)
        self.assertIsNone(selector.get_map())

    def test_nonblocking_setup_failure_reaps_child_and_closes_resources(self):
        selector = selectors.DefaultSelector()
        spawn = subprocess.Popen
        children = []

        def track_spawn(*args, **kwargs):
            child = spawn(*args, **kwargs)
            children.append(child)
            return child

        with patch("agentsql.runner.selectors.DefaultSelector", return_value=selector), \
                patch("agentsql.runner.os.set_blocking", side_effect=OSError("setup failed")), \
                patch("agentsql.runner.subprocess.Popen", side_effect=track_spawn), \
                self.assertRaisesRegex(OSError, "setup failed"):
            _run_process([sys.executable, "-c", "import time; time.sleep(60)"], self.root, 10, None)
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].returncode)
        self.assertTrue(children[0].stdout.closed)
        self.assertTrue(children[0].stderr.closed)
        self.assertIsNone(selector.get_map())

    def test_timeout_signals_group_once_then_reaps_and_closes_resources(self):
        selector = selectors.DefaultSelector()
        spawn = subprocess.Popen
        killpg = os.killpg
        children = []

        def track_spawn(*args, **kwargs):
            child = spawn(*args, **kwargs)
            children.append(child)
            return child

        def kill_once(group, sig):
            if terminate.call_count > 1:
                # Darwin returns EPERM when a prior SIGKILL leaves only
                # unreaped zombies in the group. Do not send that second signal.
                raise PermissionError(1, "Operation not permitted")
            return killpg(group, sig)

        with patch("agentsql.runner.selectors.DefaultSelector", return_value=selector), \
                patch("agentsql.runner.subprocess.Popen", side_effect=track_spawn), \
                patch("agentsql.runner.os.killpg", side_effect=kill_once) as terminate:
            result = _run_process([sys.executable, "-c", "import time; time.sleep(60)"],
                                  self.root, 0.05, None)
        self.assertTrue(result["timed_out"], result)
        self.assertEqual(result["returncode"], -signal.SIGKILL)
        terminate.assert_called_once_with(children[0].pid, signal.SIGKILL)
        self.assertTrue(children[0].stdout.closed)
        self.assertTrue(children[0].stderr.closed)
        self.assertIsNone(selector.get_map())

    def test_signal_failure_is_not_retried_and_still_reaps_child(self):
        spawn = subprocess.Popen
        killpg = os.killpg
        children = []

        def track_spawn(*args, **kwargs):
            child = spawn(*args, **kwargs)
            children.append(child)
            return child

        def fail_after_killing(group, sig):
            # Inject a signaling error while allowing this real child to exit,
            # so the test can verify reaping without leaking a live process.
            killpg(group, sig)
            raise PermissionError(1, "injected signal failure")

        with patch("agentsql.runner.subprocess.Popen", side_effect=track_spawn), \
                patch("agentsql.runner.os.killpg", side_effect=fail_after_killing) as terminate, \
                self.assertRaisesRegex(PermissionError, "injected signal failure"):
            _run_process([sys.executable, "-c", "import time; time.sleep(60)"],
                         self.root, 0.05, None)
        self.assertEqual(terminate.call_count, 1)
        self.assertIsNotNone(children[0].returncode)
        self.assertTrue(children[0].stdout.closed)
        self.assertTrue(children[0].stderr.closed)

    def test_setup_error_remains_primary_if_cleanup_signal_fails(self):
        spawn = subprocess.Popen
        killpg = os.killpg
        children = []

        def track_spawn(*args, **kwargs):
            child = spawn(*args, **kwargs)
            children.append(child)
            return child

        def fail_after_killing(group, sig):
            killpg(group, sig)
            raise PermissionError(1, "injected cleanup failure")

        with patch("agentsql.runner.subprocess.Popen", side_effect=track_spawn), \
                patch("agentsql.runner.os.set_blocking", side_effect=OSError("original setup failure")), \
                patch("agentsql.runner.os.killpg", side_effect=fail_after_killing) as terminate, \
                self.assertRaisesRegex(OSError, "original setup failure") as caught:
            _run_process([sys.executable, "-c", "import time; time.sleep(60)"],
                         self.root, 10, None)
        self.assertIsInstance(caught.exception.__cause__, PermissionError)
        self.assertEqual(terminate.call_count, 1)
        self.assertIsNotNone(children[0].returncode)
        self.assertTrue(children[0].stdout.closed)
        self.assertTrue(children[0].stderr.closed)

    def test_denied_group_signal_stops_owned_leader_without_hiding_error(self):
        spawn = subprocess.Popen
        children = []

        def track_spawn(*args, **kwargs):
            child = spawn(*args, **kwargs)
            children.append(child)
            return child

        with patch("agentsql.runner.subprocess.Popen", side_effect=track_spawn), \
                patch("agentsql.runner.os.killpg", side_effect=PermissionError(1, "group denied")) as terminate, \
                self.assertRaisesRegex(PermissionError, "group denied"):
            _run_process([sys.executable, "-c", "import time; time.sleep(60)"],
                         self.root, 0.05, None)
        self.assertEqual(terminate.call_count, 1)
        self.assertIsNotNone(children[0].returncode)
        self.assertTrue(children[0].stdout.closed)
        self.assertTrue(children[0].stderr.closed)

    def test_denied_group_and_leader_signals_do_not_wait_without_a_bound(self):
        child = Mock(spec=subprocess.Popen)
        child.pid = 12345
        child.kill.side_effect = PermissionError(1, "leader denied")
        child.wait.side_effect = subprocess.TimeoutExpired("inaccessible command", 1)
        with patch("agentsql.runner.os.killpg", side_effect=PermissionError(1, "group denied")) as terminate, \
                self.assertRaisesRegex(PermissionError, "group denied"):
            _stop_process(child)
        terminate.assert_called_once_with(child.pid, signal.SIGKILL)
        child.kill.assert_called_once_with()
        child.wait.assert_called_once()
        self.assertGreater(child.wait.call_args.kwargs["timeout"], 0)
        self.assertLessEqual(child.wait.call_args.kwargs["timeout"], 1)

    def test_symlink_output_rejects_entire_import(self):
        result = self.run_python("from pathlib import Path; "
            "Path('good').write_text('partial'); Path('unsafe').symlink_to('/etc/passwd')")
        self.assertFalse(result["imported"], result)
        self.assertIn("symlink", result["error"]["message"])
        self.assertEqual(self.branch.inspect()["changes"], [])

    def test_special_output_file_rejects_entire_import_without_blocking(self):
        result = self.run_python("import os; from pathlib import Path; "
            "Path('good').write_text('partial'); os.mkfifo('fifo')")
        self.assertFalse(result["imported"], result)
        self.assertIn("special file", result["error"]["message"])
        self.assertEqual(self.branch.inspect()["changes"], [])

    def test_same_branch_concurrent_mutation_invalidates_import_atomically(self):
        database = str(self.workspace.db_path)
        script = ("from pathlib import Path; from agentsql import Workspace; "
                  f"Workspace.open({database!r}).get_branch('worker').fs.write_file('parallel.txt','other'); "
                  "Path('result.md').write_text('must not overwrite branch')")
        result = self.run_python(script)
        self.assertFalse(result["imported"], result)
        self.assertEqual(self.branch.fs.read_file("parallel.txt"), "other")
        self.assertFalse(self.branch.fs.exists("result.md"))

    def test_sealing_during_execution_preserves_reviewed_proposal(self):
        self.branch.fs.write_file("reviewed.md", "original")
        script = ("from pathlib import Path; from agentsql import Workspace; "
                  f"Workspace.open({str(self.workspace.db_path)!r}).get_branch('worker').seal(); "
                  "Path('reviewed.md').write_text('changed during command')")
        result = self.run_python(script)
        self.assertFalse(result["imported"], result)
        state = self.branch.inspect()
        self.assertTrue(state["sealed"])
        self.branch.accept(expected_proposal=state["proposal_id"])
        self.assertEqual(self.workspace.snapshot().fs.read_file("reviewed.md"), "original")

    def test_no_shell_interpolation_and_environment_can_be_replaced(self):
        marker = self.root / "shell-marker"
        literal = f"$(touch {marker}); echo expanded"
        result = run_command(self.workspace, "worker", [sys.executable, "-c",
            "import os,sys; print(sys.argv[1]); print(os.environ['RUNNER_TEST'])", literal],
            env={"RUNNER_TEST": "chosen"})
        self.assertTrue(result["imported"], result)
        self.assertEqual(result["stdout"], literal + "\nchosen\n")
        self.assertFalse(marker.exists())

    def test_missing_executable_returns_structured_failure(self):
        result = run_command(self.workspace, "worker", ["/does-not-exist-agentsql"])
        self.assertFalse(result["imported"])
        self.assertEqual(result["error"]["error"], "FileNotFoundError")
        self.assertEqual(self.branch.inspect()["changes"], [])

    def test_materialized_tree_is_verified_before_command_execution(self):
        self.branch.fs.write_file("pending", "keep")
        before = self.branch.inspect()["changes"]
        with patch("agentsql.runner._scan_tree", return_value=({}, [])), \
                patch("agentsql.runner._run_process") as execute:
            result = self.run_python("print('must not execute')")
        self.assertFalse(result["imported"], result)
        self.assertIn("represented exactly", result["error"]["message"])
        execute.assert_not_called()
        self.assertEqual(self.branch.inspect()["changes"], before)
        self.assertEqual(self.branch.fs.read_file("pending"), "keep")

    def test_materialized_directory_membership_is_verified_before_execution(self):
        self.branch.fs.mkdir("empty")
        before = self.branch.inspect()["directories"]
        actual = {"notes.txt": b"original\n", "prices.csv": b"coffee,10\n"}
        with patch("agentsql.runner._scan_tree", return_value=(actual, [])), \
                patch("agentsql.runner._run_process") as execute:
            result = self.run_python("print('must not execute')")
        self.assertFalse(result["imported"], result)
        execute.assert_not_called()
        self.assertEqual(self.branch.inspect()["directories"], before)

    def _require_case_aliases(self):
        probe = self.root / "case-probe"
        probe.mkdir()
        (probe / "REPORT").write_bytes(b"case probe")
        if not (probe / "report").exists():
            self.skipTest("Host filesystem distinguishes case variants")

    def test_case_aliased_files_are_rejected_without_execution_or_data_loss(self):
        self._require_case_aliases()
        self.branch.fs.write_file("REPORT.txt", b"UPPER")
        self.branch.fs.write_file("report.txt", b"lower")
        before = self.branch.inspect()["changes"]
        marker = self.root / "must-not-execute"
        result = self.run_python(f"from pathlib import Path; Path({str(marker)!r}).touch()")
        self.assertFalse(result["imported"], result)
        self.assertFalse(marker.exists())
        self.assertNotIn("returncode", result)
        self.assertEqual(self.branch.inspect()["changes"], before)
        self.assertEqual(self.branch.fs.read_file("REPORT.txt", encoding=None), b"UPPER")
        self.assertEqual(self.branch.fs.read_file("report.txt", encoding=None), b"lower")

    def test_case_aliased_directories_are_rejected_before_execution(self):
        self._require_case_aliases()
        self.branch.fs.mkdir("Reports")
        self.branch.fs.mkdir("reports")
        self.branch.fs.write_file("Reports/first", "first")
        self.branch.fs.write_file("reports/second", "second")
        before_files = self.branch.inspect()["changes"]
        before_dirs = self.branch.inspect()["directories"]
        marker = self.root / "must-not-execute"
        result = self.run_python(f"from pathlib import Path; Path({str(marker)!r}).touch()")
        self.assertFalse(result["imported"], result)
        self.assertFalse(marker.exists())
        self.assertNotIn("returncode", result)
        self.assertEqual(self.branch.inspect()["changes"], before_files)
        self.assertEqual(self.branch.inspect()["directories"], before_dirs)

    def test_case_aliased_empty_directories_are_rejected_before_execution(self):
        self._require_case_aliases()
        self.branch.fs.mkdir("Reports")
        self.branch.fs.mkdir("reports")
        before = self.branch.inspect()["directories"]
        marker = self.root / "must-not-execute"
        result = self.run_python(f"from pathlib import Path; Path({str(marker)!r}).touch()")
        self.assertFalse(result["imported"], result)
        self.assertIn("represented exactly", result["error"]["message"])
        self.assertFalse(marker.exists())
        self.assertEqual(self.branch.inspect()["directories"], before)

    def test_unicode_aliased_files_are_rejected_before_execution(self):
        composed, decomposed = "caf\u00e9.txt", "cafe\u0301.txt"
        probe = self.root / "unicode-probe"
        probe.mkdir()
        (probe / composed).write_bytes(b"unicode probe")
        if not (probe / decomposed).exists():
            self.skipTest("Host filesystem distinguishes Unicode normalization variants")
        self.branch.fs.write_file(composed, b"composed")
        self.branch.fs.write_file(decomposed, b"decomposed")
        before = self.branch.inspect()["changes"]
        marker = self.root / "must-not-execute"
        result = self.run_python(f"from pathlib import Path; Path({str(marker)!r}).touch()")
        self.assertFalse(result["imported"], result)
        self.assertFalse(marker.exists())
        self.assertNotIn("returncode", result)
        self.assertEqual(self.branch.inspect()["changes"], before)
        self.assertEqual(self.branch.fs.read_file(composed, encoding=None), b"composed")
        self.assertEqual(self.branch.fs.read_file(decomposed, encoding=None), b"decomposed")

    def test_external_effect_is_not_rolled_back_on_command_failure(self):
        # Deliberately demonstrate the documented host-access boundary with a
        # harmless file in this test's own temporary directory.
        outside = self.root / "external-effect.txt"
        result = self.run_python("from pathlib import Path; import sys; "
            f"Path({str(outside)!r}).write_text('external effect'); "
            "Path('partial').write_text('discard'); sys.exit(2)")
        self.assertFalse(result["imported"])
        self.assertEqual(outside.read_text(), "external effect")
        self.assertFalse(self.branch.fs.exists("partial"))

    def test_runner_attempts_have_terminal_audit_events(self):
        successful = self.run_python("print('success')")
        failed = self.run_python("import sys; sys.exit(2)")
        self.assertEqual(self.branch.tools.get(successful["tool_call_id"])["status"], "succeeded")
        self.assertEqual(self.branch.tools.get(failed["tool_call_id"])["status"], "failed")
        events = self.branch.tools.events(successful["tool_call_id"])
        self.assertEqual([event["kind"] for event in events], ["started", "succeeded"])

    def test_audit_completion_failure_preserves_successfully_imported_files(self):
        with patch.object(ToolLog, "success", side_effect=WorkspaceBusyError("audit locked")), \
                patch.object(ToolLog, "error") as failed_audit:
            result = self.run_python("from pathlib import Path; Path('result.md').write_text('imported')")
        self.assertTrue(result["imported"], result)
        self.assertNotIn("error", result)
        self.assertEqual(result["audit_error"]["error"], "workspace_busy")
        self.assertEqual(self.branch.fs.read_file("result.md"), "imported")
        self.assertEqual(self.branch.tools.get(result["tool_call_id"])["status"], "pending")
        failed_audit.assert_not_called()

    def test_audit_error_failure_does_not_mask_original_command_failure(self):
        with patch.object(ToolLog, "error", side_effect=WorkspaceBusyError("audit locked")):
            result = self.run_python("from pathlib import Path; import sys; Path('partial').touch(); sys.exit(7)")
        self.assertFalse(result["imported"])
        self.assertEqual(result["returncode"], 7)
        self.assertEqual(result["error"], "command_failed")
        self.assertEqual(result["audit_error"]["error"], "workspace_busy")
        self.assertFalse(self.branch.fs.exists("partial"))

    def test_audit_error_failure_preserves_original_filesystem_error(self):
        self.branch.fs.write_file("reviewed", "done")
        self.branch.seal()
        with patch.object(ToolLog, "error", side_effect=WorkspaceBusyError("audit locked")):
            result = self.run_python("print('not executed')")
        self.assertFalse(result["imported"])
        self.assertEqual(result["error"]["error"], "BranchClosedError")
        self.assertEqual(result["audit_error"]["error"], "workspace_busy")

    def test_audit_cleanup_failure_does_not_mask_original_interruption(self):
        with patch("agentsql.files.FileSystem.capture_files", side_effect=KeyboardInterrupt("original")), \
                patch.object(ToolLog, "error", side_effect=WorkspaceBusyError("audit locked")), \
                self.assertRaisesRegex(KeyboardInterrupt, "original"):
            self.run_python("print('not executed')")

    def test_interrupted_audit_completion_does_not_report_execution_rollback(self):
        with patch.object(ToolLog, "success", side_effect=KeyboardInterrupt("audit interrupted")), \
                patch.object(ToolLog, "error") as failed_audit, \
                self.assertRaisesRegex(KeyboardInterrupt, "audit interrupted"):
            self.run_python("from pathlib import Path; Path('result.md').write_text('imported')")
        self.assertEqual(self.branch.fs.read_file("result.md"), "imported")
        failed_audit.assert_not_called()

    def test_sealed_branch_is_not_executed_or_modified(self):
        self.branch.fs.write_file("reviewed", "done")
        proposal = self.branch.seal()
        result = self.run_python("from pathlib import Path; Path('unexpected').touch()")
        self.assertFalse(result["imported"])
        self.assertNotIn("returncode", result)
        self.assertEqual(self.branch.inspect()["proposal_id"], proposal)

    def test_tree_size_limits_leave_pending_changes_unchanged(self):
        with patch("agentsql.runner.MAX_FILE_BYTES", 10):
            result = self.run_python("from pathlib import Path; Path('big').write_bytes(b'x'*11)")
        self.assertFalse(result["imported"], result)
        self.assertEqual(self.branch.inspect()["changes"], [])

    def test_invalid_options_fail_before_running_anything(self):
        for command in ([], "echo hello", [""], [1], ["echo", "\0"]):
            with self.subTest(command=command), self.assertRaises(ValueError):
                run_command(self.workspace, "worker", command)
        for timeout in (0, -1, True, float("nan"), float("inf"), 10 ** 400, -(10 ** 400),
                        2_147_483.648):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                self.run_python("print('never')", timeout=timeout)
        with self.assertRaises(ValueError):
            self.run_python("print('never')", env={"BAD=KEY": "x"})


if __name__ == "__main__":
    unittest.main()

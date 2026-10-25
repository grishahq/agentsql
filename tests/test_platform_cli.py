"""Real CLI subprocess workflows across the expanded workspace platform."""

import base64
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from agentsql import Workspace


class PlatformCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = self.root / "state.db"
        self.command("init")

    def command(self, *arguments, data=None, expected=0, database=None):
        completed = subprocess.run([sys.executable, "-m", "agentsql", "-w",
            str(database or self.database), *arguments], input=data, capture_output=True,
            cwd=self.root, timeout=20)
        self.assertEqual(completed.returncode, expected, completed.stderr.decode("utf-8", "replace"))
        self.assertNotIn(b"Traceback", completed.stderr)
        return completed

    def json_command(self, *arguments, **options):
        return json.loads(self.command(*arguments, **options).stdout)

    def test_empty_init_named_resume_and_fork_persist_across_processes(self):
        initial = self.json_command("check")
        self.assertTrue(initial["ok"])
        self.assertEqual(initial["schema_version"], "3")
        self.assertEqual(self.json_command("list"), {"files": []})
        self.json_command("session", "team")
        self.json_command("fs", "--branch", "team", "write", "pending.txt", "--text", "pending")
        resumed = self.json_command("session", "team")
        self.assertEqual(resumed["changes"][0]["path"], "pending.txt")
        self.assertEqual(len(self.json_command("branches")), 1)
        self.json_command("fork", "team", "alternative")
        self.assertEqual(self.command("fs", "--branch", "alternative", "read", "pending.txt").stdout, b"pending")
        self.json_command("fs", "--branch", "alternative", "write", "pending.txt", "--text", "alternative")
        self.assertEqual(self.command("fs", "--branch", "team", "read", "pending.txt").stdout, b"pending")

    def test_directory_rename_file_and_kv_sealed_acceptance(self):
        self.json_command("session", "worker")
        self.json_command("fs", "--branch", "worker", "mkdir", "drafts/empty", "--parents")
        self.json_command("fs", "--branch", "worker", "write", "drafts/result.md", "--text", "reviewed")
        self.json_command("fs", "--branch", "worker", "rename", "drafts", "reports")
        listing = self.json_command("fs", "--branch", "worker", "readdir", "reports")
        self.assertEqual({entry["name"] for entry in listing}, {"empty", "result.md"})
        self.json_command("kv", "--branch", "worker", "set", "job/result", '{"approved":true,"items":[1,null]}')
        token = self.json_command("seal", "worker")["proposal_id"]
        refused = self.command("kv", "--branch", "worker", "set", "job/result", "false", expected=1)
        self.assertIn("sealed", json.loads(refused.stderr)["message"])
        self.assertIn(b"reviewed", self.command("diff", "worker").stdout)
        self.json_command("accept", "worker", "--proposal", token)
        self.assertEqual(self.command("fs", "read", "reports/result.md").stdout, b"reviewed")
        self.assertEqual(self.json_command("fs", "stat", "reports/empty")["type"], "dir")
        self.assertEqual(self.json_command("kv", "get", "job/result"), {"approved": True, "items": [1, None]})
        self.assertEqual(self.json_command("history")[-1]["keys"], ["job/result"])
        self.assertTrue(self.json_command("check")["ok"])

    def test_binary_file_input_and_export_are_exact_bytes(self):
        self.json_command("branch", "binary")
        data = bytes(range(256)) * 5 + b"\0\xff\n"
        source = self.root / "binary-source"
        source.write_bytes(data)
        self.json_command("fs", "--branch", "binary", "write", "input.bin", "--from-file", str(source))
        self.assertEqual(self.command("fs", "--branch", "binary", "read", "input.bin").stdout, data)
        self.json_command("fs", "--branch", "binary", "write", "stdin.bin", data=data)
        self.json_command("accept", "binary")
        self.assertEqual(self.command("show", "input.bin").stdout, data)
        self.assertEqual(self.command("fs", "read", "stdin.bin").stdout, data)

    def test_tool_call_timeline_and_checkpoint_reopen(self):
        self.json_command("session", "worker")
        call_id = self.json_command("tools", "--branch", "worker", "start", "fetch",
                                    "--parameters", '{"url":"example"}')["id"]
        self.assertEqual(self.json_command("tools", "--branch", "worker", "get", str(call_id))["status"], "pending")
        self.json_command("tools", "--branch", "worker", "success", str(call_id), "--result", '{"status":200}')
        timeline = self.json_command("timeline", "--branch", "worker")
        self.assertEqual(len(timeline), 1)
        self.assertEqual(timeline[0]["result"], {"status": 200})
        events = self.json_command("tools", "--branch", "worker", "events", "--call-id", str(call_id))
        self.assertEqual([event["kind"] for event in events], ["started", "succeeded"])
        self.json_command("fs", "--branch", "worker", "write", "pending", "--text", "checkpoint")
        self.json_command("kv", "--branch", "worker", "set", "pending-state", "[1,2]")
        backup = self.root / "checkpoint.db"
        self.json_command("checkpoint", str(backup))
        self.assertEqual(self.json_command("timeline", database=backup), timeline)
        self.assertEqual(self.command("fs", "--branch", "worker", "read", "pending", database=backup).stdout, b"checkpoint")
        self.assertEqual(self.json_command("kv", "--branch", "worker", "get", "pending-state", database=backup), [1, 2])
        self.assertTrue(self.json_command("check", database=backup)["ok"])

    def test_run_imports_regular_command_output_and_bounds_stdout(self):
        self.json_command("session", "worker")
        self.json_command("fs", "--branch", "worker", "write", "input", "--text", "source")
        script = ("from pathlib import Path; import os; "
                  "Path('output').write_bytes(Path('input').read_bytes()+b' result'); "
                  "os.write(1,b'x'*200000)")
        result = self.json_command("run", "--command-timeout", "10", "worker", "--",
                                   sys.executable, "-c", script)
        self.assertTrue(result["imported"], result)
        self.assertTrue(result["stdout_truncated"])
        self.assertEqual(len(base64.b64decode(result["stdout_base64"])), 128 * 1024)
        self.assertEqual(result["changed_files"], ["output"])
        self.assertEqual(self.command("fs", "--branch", "worker", "read", "output").stdout, b"source result")
        self.assertEqual(self.json_command("tools", "--branch", "worker", "stats")[0]["succeeded"], 1)
        self.json_command("accept", "worker")
        self.assertEqual(self.command("show", "output").stdout, b"source result")

    def test_failed_run_has_json_result_and_does_not_import(self):
        self.json_command("session", "worker")
        script = "from pathlib import Path; import sys; Path('partial').touch(); sys.exit(4)"
        result = self.json_command("run", "worker", "--", sys.executable, "-c", script, expected=1)
        self.assertEqual(result["returncode"], 4)
        self.assertFalse(result["imported"])
        self.assertEqual(self.json_command("inspect", "worker")["changes"], [])

    def test_cli_mcp_handshake_discovery_and_staging(self):
        self.json_command("session", "mcp-agent")
        messages = [
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": "2025-11-25", "capabilities": {},
                "clientInfo": {"name": "cli-test", "version": "1"}}},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
                "name": "write_file", "arguments": {"path": "from-mcp.md", "content": "tool output"}}},
            {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {
                "name": "kv_set", "arguments": {"key": "worker/status", "value": "ready"}}},
            {"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "seal"}},
        ]
        completed = self.command("serve", "--branch", "mcp-agent",
            data=b"".join(json.dumps(message).encode() + b"\n" for message in messages))
        self.assertEqual(completed.stderr, b"")
        replies = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual([reply["id"] for reply in replies], [1, 2, 3, 4, 5])
        tool_names = {tool["name"] for tool in replies[1]["result"]["tools"]}
        self.assertNotIn("accept", tool_names)
        self.assertIn("read_file", tool_names)
        self.assertFalse(replies[2]["result"]["isError"])
        proposal = replies[-1]["result"]["structuredContent"]["proposal_id"]
        self.json_command("accept", "mcp-agent", "--proposal", proposal)
        self.assertEqual(self.command("show", "from-mcp.md").stdout, b"tool output")
        self.assertEqual(self.json_command("kv", "get", "worker/status"), "ready")

    def test_cli_mcp_startup_errors_use_only_stderr(self):
        completed = self.command("serve", "--branch", "not-created", expected=1)
        self.assertEqual(completed.stdout, b"")
        self.assertIn("error", json.loads(completed.stderr))

    def test_stale_kv_input_rejects_file_result_through_cli(self):
        self.json_command("branch", "setup")
        self.json_command("kv", "--branch", "setup", "set", "price", "100")
        self.json_command("accept", "setup")
        self.json_command("branch", "proposal")
        self.assertEqual(self.json_command("kv", "--branch", "proposal", "get", "price"), 100)
        self.json_command("write", "proposal", "proposal.md", "--text", "total 300")
        self.json_command("branch", "update")
        self.json_command("kv", "--branch", "update", "set", "price", "120")
        self.json_command("accept", "update")
        rejected = self.command("accept", "proposal", expected=2)
        evidence = json.loads(rejected.stderr)
        self.assertEqual(evidence["error"], "requires_recheck")
        self.assertTrue(any(item.get("resource") == "kv" for item in evidence["conflicts"]))
        self.assertFalse(Workspace.open(self.database).snapshot().exists("proposal.md"))

    def test_edits_require_branch_and_invalid_json_does_not_stage_state(self):
        for arguments in (("fs", "write", "file", "--text", "bad"),
                          ("fs", "mkdir", "dir"), ("kv", "set", "key", "1")):
            with self.subTest(arguments=arguments):
                self.assertIn("error", json.loads(self.command(*arguments, expected=2).stderr))
        self.json_command("branch", "worker")
        for value in ("not-json", "NaN", '{"x":Infinity}'):
            with self.subTest(value=value):
                self.assertIn("error", json.loads(self.command("kv", "--branch", "worker", "set", "bad", value, expected=1).stderr))
        self.assertEqual(self.json_command("inspect", "worker")["kv_changes"], [])


if __name__ == "__main__":
    unittest.main()

"""MCP framing, lifecycle, branch scope and actual tool contracts."""

from __future__ import annotations

import base64
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from agentsql import ConflictError, Workspace
from agentsql.server import serve


def _request(identifier, method, params=None):
    result = {"jsonrpc": "2.0", "id": identifier, "method": method}
    if params is not None:
        result["params"] = params
    return result


def _initialize(version="2025-11-25"):
    return _request(1, "initialize", {"protocolVersion": version, "capabilities": {},
                                     "clientInfo": {"name": "test-client", "version": "1"}})


_READY = {"jsonrpc": "2.0", "method": "notifications/initialized"}


class MCPServerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        source = self.root / "source"
        source.mkdir()
        (source / "prices.csv").write_text("coffee,10\n", encoding="utf-8")
        self.database = self.root / "state.db"
        self.workspace = Workspace.create(self.database, source)
        self.branch = self.workspace.branch("worker")

    def exchange(self, *messages, raw=None):
        data = raw if raw is not None else b"".join(
            json.dumps(message).encode() + b"\n" for message in messages)
        output = io.BytesIO()
        serve(self.workspace, "worker", input_stream=io.BytesIO(data), output=output)
        return [json.loads(line) for line in output.getvalue().splitlines()]

    def call(self, name, arguments=None):
        return self.exchange(_initialize(), _READY,
            _request(2, "tools/call", {"name": name, "arguments": arguments or {}}))[-1]

    def test_lifecycle_and_only_branch_tools_are_discoverable(self):
        responses = self.exchange(_initialize(), _READY, _request(2, "tools/list"))
        self.assertEqual(len(responses), 2)
        self.assertEqual(responses[0]["result"]["protocolVersion"], "2025-11-25")
        self.assertEqual(responses[0]["result"]["serverInfo"]["name"], "agentsql")
        tools = {item["name"]: item for item in responses[1]["result"]["tools"]}
        self.assertEqual(set(tools), {"read_file", "write_file", "readdir", "mkdir", "remove",
                         "rename", "stat", "kv_get", "kv_set", "kv_delete", "kv_list",
                         "inspect", "diff", "seal"})
        self.assertTrue(tools["inspect"]["annotations"]["readOnlyHint"])
        self.assertFalse(tools["read_file"]["annotations"]["readOnlyHint"])
        self.assertNotIn("accept", tools)
        self.assertNotIn("run_command", tools)

    def test_protocol_version_negotiation(self):
        for version in ("2025-06-18", "2024-11-05", "unknown"):
            with self.subTest(version=version):
                response = self.exchange(_initialize(version))[0]
                self.assertEqual(response["result"]["protocolVersion"],
                                 "2025-11-25" if version == "unknown" else version)

    def test_initialization_is_required_and_cannot_repeat(self):
        responses = self.exchange(_request(0, "tools/list"), _initialize(),
                                  _request(2, "tools/list"), _READY, _initialize())
        self.assertEqual(responses[0]["error"]["code"], -32002)
        self.assertEqual(responses[2]["error"]["code"], -32002)
        self.assertEqual(responses[3]["error"]["code"], -32600)

    def test_ping_and_notifications_have_correct_response_behavior(self):
        responses = self.exchange(_request(0, "ping"), _initialize(), _READY,
            {"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 10}},
            {"jsonrpc": "2.0", "method": "tools/call", "params": {
                "name": "write_file", "arguments": {"path": "bad", "content": "ignored"}}},
            _request(2, "ping"))
        self.assertEqual([r["id"] for r in responses], [0, 1, 2])
        self.assertEqual(self.branch.inspect()["changes"], [])

    def test_read_tracks_dependency_and_old_proposal_is_rejected(self):
        response = self.call("read_file", {"path": "prices.csv"})
        self.assertEqual(response["result"]["structuredContent"]["content"], "coffee,10\n")
        self.call("write_file", {"path": "report.md", "content": "price is 10"})
        self.assertIn("prices.csv", [row["path"] for row in self.branch.inspect()["reads"]])
        other = self.workspace.branch("price-update")
        other.fs.write_file("prices.csv", "coffee,12\n")
        other.accept()
        with self.assertRaises(ConflictError):
            self.branch.accept()

    def test_binary_roundtrip_and_explicit_utf8_error(self):
        content = b"\0\xff\x80binary"
        response = self.call("write_file", {"path": "file.bin", "encoding": "base64",
                                           "content": base64.b64encode(content).decode()})
        self.assertFalse(response["result"]["isError"])
        response = self.call("read_file", {"path": "file.bin", "encoding": "base64"})
        self.assertEqual(base64.b64decode(response["result"]["structuredContent"]["content"]), content)
        self.assertTrue(self.call("read_file", {"path": "file.bin"})["result"]["isError"])

    def test_file_directory_and_kv_tools(self):
        self.assertFalse(self.call("mkdir", {"path": "reports/empty", "parents": True})["result"]["isError"])
        self.call("write_file", {"path": "reports/a.txt", "content": "hello"})
        entries = self.call("readdir", {"path": "reports"})["result"]["structuredContent"]["entries"]
        self.assertEqual({row["name"] for row in entries}, {"empty", "a.txt"})
        self.call("rename", {"source": "reports/a.txt", "destination": "reports/b.txt"})
        result = self.call("stat", {"path": "reports/b.txt"})["result"]["structuredContent"]
        self.assertEqual(result["size"], 5)
        self.call("kv_set", {"key": "job/status", "value": {"step": 2}})
        self.assertEqual(self.call("kv_get", {"key": "job/status"})["result"]["structuredContent"]["value"], {"step": 2})
        result = self.call("kv_list", {"prefix": "job/"})["result"]["structuredContent"]["entries"]
        self.assertEqual(len(result), 1)
        self.assertFalse(self.call("kv_delete", {"key": "job/status"})["result"]["isError"])
        self.assertFalse(self.call("remove", {"path": "reports", "recursive": True})["result"]["isError"])

    def test_seal_prevents_further_worker_changes(self):
        self.call("write_file", {"path": "result.md", "content": "done"})
        response = self.call("seal")
        self.assertTrue(response["result"]["structuredContent"]["proposal_id"])
        self.assertTrue(self.call("write_file", {"path": "result.md", "content": "changed"})["result"]["isError"])
        self.assertFalse(self.call("inspect")["result"]["isError"])
        self.assertFalse(self.call("diff")["result"]["isError"])

    def test_errors_are_structured_and_session_stays_usable(self):
        for arguments in ({"path": "../escape", "content": "no"},
                          {"path": "x", "content": "bad!", "encoding": "base64"}):
            with self.subTest(arguments=arguments):
                response = self.call("write_file", arguments)
                self.assertTrue(response["result"]["isError"])
                self.assertIn("error", response["result"]["structuredContent"])
        responses = self.exchange(_initialize(), _READY,
            _request(2, "tools/call", {"name": "read_file", "arguments": {"path": "missing"}}),
            _request(3, "ping"))
        self.assertTrue(responses[1]["result"]["isError"])
        self.assertEqual(responses[2]["result"], {})

    def test_unknown_tools_arguments_and_wrong_types_are_protocol_errors(self):
        for name, arguments in (("accept", {}), ("read_file", {}),
                                ("read_file", {"path": "x", "branch": "other"}),
                                ("read_file", {"path": 1}),
                                ("mkdir", {"path": "x", "parents": 1}),
                                ("read_file", {"path": "x", "encoding": "pickle"})):
            with self.subTest(name=name, arguments=arguments):
                self.assertEqual(self.call(name, arguments)["error"]["code"], -32602)

    def test_invalid_json_and_batches_are_rejected_then_recover(self):
        raw = b'not json\n[]\n{"jsonrpc":"2.0","id":true,"method":"ping"}\n'
        raw += b'{"jsonrpc":"2.0","id":1,"method":"ping","params":{"x":NaN}}\n'
        raw += json.dumps(_request(9, "ping")).encode() + b"\n"
        responses = self.exchange(raw=raw)
        self.assertEqual([response["error"]["code"] for response in responses[:-1]],
                         [-32700, -32600, -32600, -32700])
        self.assertEqual(responses[-1]["result"], {})

    def test_oversized_frame_is_drained_without_losing_next_request(self):
        with patch("agentsql.server.MAX_MESSAGE_BYTES", 256):
            raw = b'"' + b"x" * 1000 + b'"\n' + json.dumps(_request(9, "ping")).encode() + b"\n"
            responses = self.exchange(raw=raw)
        self.assertEqual(responses[0]["error"]["code"], -32700)
        self.assertEqual(responses[1]["result"], {})

    def test_mcp_file_limit_does_not_stage_partial_write(self):
        with patch("agentsql.server.MAX_FILE_BYTES", 3):
            response = self.call("write_file", {"path": "big", "content": "1234"})
            self.assertTrue(response["result"]["isError"])
        self.assertEqual(self.branch.inspect()["changes"], [])

    def test_duplicate_keys_and_excessive_nesting_are_rejected(self):
        duplicate = b'{"jsonrpc":"2.0","id":1,"id":2,"method":"ping"}\n'
        nested = b'{"jsonrpc":"2.0","id":3,"method":"ping","params":{"x":'
        nested += b"[" * 100 + b"0" + b"]" * 100 + b"}}\n"
        responses = self.exchange(raw=duplicate + nested)
        self.assertEqual([item["error"]["code"] for item in responses], [-32700, -32700])

    def test_tool_audit_records_success_and_execution_error(self):
        self.call("read_file", {"path": "prices.csv"})
        self.call("read_file", {"path": "missing"})
        calls = self.branch.tools.list(name="mcp.read_file")
        self.assertEqual([item["status"] for item in calls], ["succeeded", "failed"])
        self.assertEqual(calls[0]["parameters"], {"path": "prices.csv"})

    def test_older_clients_receive_compatible_tool_result_fields(self):
        response = self.exchange(_initialize("2024-11-05"), _READY,
            _request(2, "tools/call", {"name": "inspect"}))[-1]
        self.assertNotIn("structuredContent", response["result"])
        self.assertEqual(response["result"]["content"][0]["type"], "text")

    def test_real_stdio_process_has_only_protocol_output(self):
        # Launch the public server function in an independent Python process.
        script = ("from agentsql import Workspace; "
                  "from agentsql.server import serve; "
                  "import sys; serve(Workspace.open(sys.argv[1]), 'worker')")
        requests = (_initialize(), _READY, _request(2, "tools/call", {
            "name": "write_file", "arguments": {"path": "child.txt", "content": "child"}}),
            _request(3, "tools/list"))
        completed = subprocess.run([sys.executable, "-c", script, str(self.database)],
            input=b"".join(json.dumps(item).encode() + b"\n" for item in requests),
            capture_output=True, timeout=20)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        responses = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual([item["id"] for item in responses], [1, 2, 3])
        self.assertEqual(self.branch.fs.read_file("child.txt"), "child")


if __name__ == "__main__":
    unittest.main()

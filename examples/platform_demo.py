"""Exercise files, state, audit, sessions, forks, a real subprocess, and MCP.

All agent workers are simulated. No model, network, or API key is used.
Run after installation: python examples/platform_demo.py
Preserve evidence: python examples/platform_demo.py --directory ./platform-output
"""

from __future__ import annotations

import argparse
import io
import json
from pathlib import Path
import sys
import tempfile

from agentsql import ConflictError, Workspace
from agentsql.runner import run_command
from agentsql.server import serve


def run(directory: Path) -> dict:
    """Run deterministic checks and retain a database, checkpoint, and summary."""
    directory.mkdir(parents=True, exist_ok=True)
    if any(directory.iterdir()):
        raise ValueError("The demo directory must be new or empty")
    database = directory / "workspace.sqlite3"
    workspace = Workspace.create(database)
    outcomes = []

    # A named session deliberately shares staged work between cooperating handles.
    setup = workspace.session("setup")
    setup.fs.mkdir("drafts")
    setup.fs.mkdir("inputs")
    setup.fs.mkdir("empty-results")
    setup.fs.write_file("drafts/item.txt", "widget")
    setup.fs.rename("drafts/item.txt", "inputs/item.txt")
    setup.kv.set("pricing/unit", 100)
    same_session = Workspace.open(database).session("setup")
    assert same_session.fs.read_file("inputs/item.txt") == "widget"
    assert same_session.kv.get("pricing/unit") == 100
    same_session.accept("Initialize files, empty directories, and JSON state together")
    assert workspace.snapshot().fs.stat("empty-results")["type"] == "dir"
    outcomes.append("Named session shares staged files and JSON state; rename and empty directories persist")

    # Agent A observes versioned state and writes different file and state outputs.
    agent_a = workspace.branch("simulated-agent-a")
    item = agent_a.fs.read_file("inputs/item.txt")
    total = agent_a.kv.get("pricing/unit") * 3
    agent_a.fs.write_file("reports/proposal.md", f"Buy 3 {item} units. Total: ${total}.\n")
    agent_a.kv.set("quotes/total", total)
    external_effect = directory / "simulated-notification.txt"
    with agent_a.tools.track("simulated_notification", {"destination": "local demo file"}) as call:
        # This actual local effect is outside the versioned database. It represents
        # an external action; no email or network service is contacted.
        external_effect.write_text("Simulated notification was written\n", encoding="utf-8")
        call.set_result({"written": True})
    reviewed = agent_a.seal()

    agent_b = workspace.branch("simulated-agent-b")
    agent_b.kv.set("pricing/unit", 120)
    agent_b.accept("Update prices while A's report is pending")
    try:
        agent_a.accept(expected_proposal=reviewed)
    except ConflictError as conflict:
        assert any(item.get("resource") == "kv" and item["kind"] == "stale_read"
                   for item in conflict.conflicts)
        rejection = conflict.as_dict()
    else:
        raise AssertionError("The stale proposal was incorrectly accepted")
    assert not workspace.snapshot().fs.exists("reports/proposal.md")
    assert not workspace.snapshot().kv.exists("quotes/total")
    assert external_effect.exists()
    assert agent_a.tools.get(call.id)["status"] == "succeeded"
    outcomes.append("Changed KV input rejects the complete file/state proposal; audit and external effect remain")

    fresh = workspace.branch("simulated-agent-a-refreshed")
    total = fresh.kv.get("pricing/unit") * 3
    fresh.fs.write_file("reports/proposal.md", f"Buy 3 widget units. Total: ${total}.\n")
    fresh.kv.set("quotes/total", total)
    fresh.accept(expected_proposal=fresh.seal())
    assert workspace.snapshot().kv.get("quotes/total") == 360
    outcomes.append("Recomputed report and JSON total are accepted together at $360")

    # Forking an accepted branch starts at that accepted revision, even if head
    # subsequently advances. Staged variants stay independent until acceptance.
    left = workspace.fork(fresh.name, "variant-left")
    right = workspace.fork(fresh.name, "variant-right")
    left.fs.write_file("reports/variant.md", "Approach one\n")
    right.fs.write_file("reports/variant.md", "Approach two\n")
    assert left.fs.read_file("reports/variant.md") != right.fs.read_file("reports/variant.md")
    left.accept("Select approach one")
    assert right.base_revision < workspace.snapshot().revision
    assert right.kv.get("quotes/total") == 360
    try:
        right.accept()
    except ConflictError:
        pass
    else:
        raise AssertionError("The competing variant overwrote the selected one")
    outcomes.append("Forked alternatives retain their base snapshot; selected output cannot be overwritten")

    # Run ordinary filesystem code against a materialized tree. This is a real
    # subprocess, with normal host permissions; it is not a security sandbox.
    command_branch = workspace.branch("command-worker")
    process = run_command(workspace, command_branch.name, [sys.executable, "-c", (
        "from pathlib import Path; "
        "text = Path('reports/proposal.md').read_text(encoding='utf-8'); "
        "Path('reports/command-output.txt').write_text(text.upper(), encoding='utf-8'); "
        "print('simulated command worker completed')"
    )])
    assert process["imported"], process
    assert process["returncode"] == 0
    assert not workspace.snapshot().fs.exists("reports/command-output.txt")
    command_branch.accept(expected_proposal=command_branch.seal())
    assert "$360" in workspace.snapshot().fs.read_file("reports/command-output.txt")
    outcomes.append("Real subprocess output is staged and audited, then accepted explicitly")

    # Exercise the real MCP request handler with deterministic stdio frames.
    # A host application uses an actual stdio transport with the same protocol.
    mcp_branch = workspace.branch("mcp-worker")
    requests = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {"name": "deterministic-platform-demo", "version": "1"}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
            "name": "read_file", "arguments": {"path": "reports/proposal.md"}}},
        {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {
            "name": "write_file", "arguments": {
                "path": "reports/mcp-note.md", "content": "Prepared through MCP\n"}}},
        {"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {
            "name": "kv_set", "arguments": {"key": "workflow/mcp", "value": "ready"}}},
        {"jsonrpc": "2.0", "id": 6, "method": "tools/call", "params": {
            "name": "seal", "arguments": {}}},
    ]
    request_bytes = b"".join(json.dumps(item).encode("utf-8") + b"\n" for item in requests)
    output = io.BytesIO()
    serve(workspace, mcp_branch.name, input_stream=io.BytesIO(request_bytes), output=output)
    responses = [json.loads(line) for line in output.getvalue().splitlines()]
    assert len(responses) == 6
    assert all("error" not in item and not item["result"].get("isError") for item in responses)
    advertised = responses[1]["result"]["tools"]
    assert "accept" not in {item["name"] for item in advertised}
    mcp_token = responses[-1]["result"]["structuredContent"]["proposal_id"]
    assert not workspace.snapshot().fs.exists("reports/mcp-note.md")
    mcp_branch.accept(expected_proposal=mcp_token)
    assert workspace.snapshot().kv.get("workflow/mcp") == "ready"
    outcomes.append("MCP tools stage and seal a file/state proposal; the coordinator accepts it separately")

    checkpoint = workspace.backup(directory / "checkpoint.sqlite3")
    restored = Workspace.open(checkpoint)
    assert restored.history() == workspace.history()
    assert restored.tools.list() == workspace.tools.list()
    assert restored.check_integrity()["ok"]
    assert workspace.check_integrity()["ok"]
    outcomes.append("Online checkpoint retains accepted history, pending alternatives, and tool evidence")
    result = {"actors": "simulated agent workers", "passed": len(outcomes), "outcomes": outcomes,
              "stale_rejection": rejection, "shared_revision": workspace.snapshot().revision,
              "files": workspace.snapshot().list_files(),
              "tool_events": [{"call_id": row["call_id"], "kind": row["kind"]}
                              for row in workspace.tools.events()]}
    (directory / "results.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, help="Retain evidence in a new or empty directory")
    arguments = parser.parse_args()
    if arguments.directory is None:
        with tempfile.TemporaryDirectory(prefix="agentsql-platform-") as temporary:
            result = run(Path(temporary))
    else:
        result = run(arguments.directory)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

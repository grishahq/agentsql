# AgentSQL: files, state, and tool history

AgentSQL 0.3.0a1 provides a local storage layer for cooperating agent workers:
versioned files and directories, JSON state, explicit tool history, named sessions,
isolated branches, subprocess staging, and branch-scoped MCP tools. Accepting a
proposal checks its tracked inputs and publishes all its file and state changes in
one SQLite transaction.

The immediate users are developers who coordinate document generators, research
workers, data processors, or several attempts by one agent on a single machine.
For example, a proposal can read its product description from a file and its price
from JSON state. If another worker updates that price, acceptance requires a
recheck even when the proposal writes a different file.

## Run the complete demonstration

From the repository root, after installing the project in your virtual environment:

```sh
python -m pip install .
python examples/platform_demo.py
```

To keep the SQLite workspace, checkpoint, structured conflict evidence, and summary:

```sh
python examples/platform_demo.py --directory ./platform-output
```

The directory must be new or empty. All agent workers are simulated; there is no
model or network dependency. The filesystem operations, subprocess, database
transactions, audit history, and MCP request handler are real. The MCP portion
feeds the server stdio frames in memory; it does not claim validation against
every MCP host application.

The demonstration checks seven groups of outcomes: session sharing with
directory/file operations; stale state rejection while preserving tool evidence;
recomputation; isolated forks; subprocess staging; MCP preparation with separate
acceptance; and online checkpoint persistence.

## Embed the SDK

```python
from agentsql import Workspace

workspace = Workspace.create("team.sqlite3")  # Empty workspace.
worker = workspace.session("research-1")       # Create or reopen staged work.
worker.fs.mkdir("reports", exist_ok=True)
worker.fs.write_file("reports/draft.md", "Draft based on current inputs\n")
worker.kv.set("task/status", {"stage": "draft", "attempt": 1})

# This records a reported operation; replace the body with application work.
with worker.tools.track("generate_document", {"worker": "research-1"}) as call:
    call.set_result({"path": "reports/draft.md"})

reviewed = worker.seal()
print(worker.diff())
print(worker.inspect())
worker.accept("Publish reviewed document and task state", expected_proposal=reviewed)
```

For existing files, use `Workspace.create("team.sqlite3", "./inputs")`. Place the
database outside the input tree and pause source writers during import. The
import detects observed races; it is not an operating-system snapshot. Acceptance
changes the database's shared revision, rather than rewriting the original tree.

| Surface | Main operations | Behavior |
| --- | --- | --- |
| `branch.fs` | `read_file`, `write_file`, `mkdir`, `readdir`, `stat`, `exists`, `remove`, `rename` | Tracked branch operations; files, empty directories, and atomic directory rename |
| `snapshot.fs` | Read, list, stat, exists | Immutable historical view; no branch dependencies |
| `branch.kv` | `get`, `set`, `exists`, `version`, `delete`, `list(prefix)` | JSON state accepted together with files |
| `snapshot.kv` | Read, exists, version, list | Immutable historical JSON state |
| `branch.tools` | `start`, `success`, `error`, `record`, `track`, `get`, `list`, `stats`, `events` | Durable, explicitly reported tool calls and lifecycle events |
| `workspace.tools` | Same audit queries and reporting | Workspace-wide tool history |
| `workspace` | `branch`, `session`, `fork`, `snapshot`, `history`, `backup`, `check_integrity` | Isolation, coordination, and operational inspection |

Use nonempty relative paths such as `reports/draft.md`. Directory inspection also
accepts the root as `""` or `"/"`. Other absolute paths, traversal, control
characters, and symlink capture are rejected. `read_file(..., encoding=None)`
returns bytes. Parent directories are created for writes; rename requires its
destination parent to exist. File stats expose type, byte size, content digest,
version, and staged status, rather than POSIX ownership or mode bits.

State accepts finite JSON values with string object keys. Returned values are
independent decoded copies. JSON `null` is a present value; use `exists()` to
distinguish it from absence. Never-seen keys have version `0`, deletion tombstones
retain their last write revision, and staged writes have version `None`.
`kv.list(prefix)` returns records with `key`, `value`, and `version`; it tracks both
prefix membership and versions of returned values. `%`, `_`, and backslashes in
prefixes are literal. A caught missing-key deletion still records the observed
base absence, so later creation can invalidate a dependent result.

## Sessions, isolated workers, and forks

```python
# A new independent worker uses the current accepted revision.
worker = workspace.branch("agent-b")

# Cooperative handles use the same pending writes. Choose this intentionally.
shared = workspace.session("cooperative-task")
shared_again = Workspace.open("team.sqlite3").session("cooperative-task")

# Copy a pending branch and its dependencies into an editable alternative.
alternative = workspace.fork("cooperative-task", "try-another-approach")

# Explicit historical input snapshot; acceptance still validates current state.
historical = workspace.branch("historical-attempt", revision=1)
```

A branch keeps its fixed base revision. Accepting another branch does not refresh
its reads. Recompute from a new branch after a conflict; editing an old result or
forking an old pending branch does not remove its stale dependencies.

Forking a pending branch copies staged files, directories, JSON state, and tracked
dependencies. It does not copy a seal or tool calls: an external action that
already happened belongs to the original branch. Forking an accepted branch
starts at that branch's accepted revision, which may be older than today's shared
head. Named sessions reopen the same branch, including its sealed or accepted
status; use a fresh name or fork when further edits require an open branch.

Use one branch per worker for isolation. A shared named session provides
cooperative staged state; it does not give each participant a private proposal or
prevent one participant from changing another participant's pending writes.

## Run existing command-line workers

```python
import sys
from agentsql.runner import run_command

worker = workspace.branch("script-worker")
result = run_command(workspace, worker.name, [
    sys.executable, "-c",
    "from pathlib import Path; Path('result.txt').write_text('prepared')",
], timeout=30)
if result["imported"]:
    worker.accept(expected_proposal=worker.seal())
```

Equivalent CLI:

```sh
agentsql -w team.sqlite3 branch script-worker-cli
agentsql -w team.sqlite3 run --command-timeout 30 script-worker-cli -- \
  python -c "from pathlib import Path; Path('result.txt').write_text('prepared')"
agentsql -w team.sqlite3 diff script-worker-cli
agentsql -w team.sqlite3 accept script-worker-cli
```

Put runner options before the branch name and command arguments after `--`.
The runner materializes the branch in a temporary working directory and stages
successful regular-file and directory changes back. It tracks every exported
file and the full directory listing conservatively, including files the command
never reads. It checks the branch content token before importing to avoid
overwriting newer staged work. Nonzero exit, timeout, unsafe output trees, or a
changed branch leave pending file writes unchanged. Audit evidence and tracked
inputs remain. Publication is a separate operation.
Before launch, an exact file/directory roundtrip check rejects workspace paths
that the host filesystem aliases or normalizes, such as case-colliding names on
a case-insensitive disk. Rejected exports never execute the command.

File import and completion of the audit record are separate transactions. If
`imported` is true with an `audit_error`, file changes are already staged while
the audit outcome may still be pending. Inspect the branch and tool call before
retrying; do not rerun an external effect just to repair its log. The runner
preserves the primary command failure if recording that failure also fails.

**This runner is not a security sandbox.** Commands have the host user's
permissions and network access. They can read absolute paths, change files
outside the temporary directory, or call APIs; those actions are not intercepted
or undone. The default child environment is inherited; SDK `env=` replaces it.
Run trusted, cooperating programs. Killing a process group handles normal child
cleanup but is not a containment boundary for a process that deliberately escapes
the group.

Current runner bounds are 10,000 tree entries, 64 MiB per file, 256 MiB total
content, and 128 KiB retained per stdout/stderr stream. The remainder of each
stream is drained and flagged as truncated. The workspace still retains file
content in memory during capture/import; it is intended for small workspaces.
Runner timeouts must be positive and no greater than 2,147,483.647 seconds.

## Connect an MCP-capable agent

Prepare the branch once, then configure a host that supports local stdio MCP
servers:

```sh
agentsql -w /absolute/path/team.sqlite3 session mcp-worker
agentsql -w /absolute/path/team.sqlite3 serve --branch mcp-worker
```

An illustrative host configuration is:

```json
{
  "mcpServers": {
    "agentsql": {
      "command": "/absolute/path/venv/bin/agentsql",
      "args": [
        "-w", "/absolute/path/team.sqlite3", "serve", "--branch", "mcp-worker"
      ]
    }
  }
}
```

The surrounding configuration format varies by host. The executable and arguments
above are the actual server entry point. Assign each isolated worker its own
branch and server configuration. Tools cover files/directories, JSON state,
inspection, diff, and sealing. Each call is audited. The stdio server supports the
MCP tools lifecycle and newline-delimited JSON-RPC; it has no HTTP transport or
authentication layer. It does not expose command execution or acceptance. The
coordinator inspects the sealed branch and accepts it separately through the SDK
or CLI.

The server accepts protocol revisions `2025-11-25`, `2025-06-18`, and `2024-11-05`;
the older revision receives text tool results without newer structured-content
fields. Message size is capped at 8 MiB, and individual file tools at 4 MiB. These
are transport limits; SDK operations have a separate capture-memory boundary.
Protocol references: [MCP lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle),
[stdio transport](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports),
and [tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools).

## Audit and recovery boundaries

Tool logging stores exactly what callers report. It does not automatically observe
arbitrary SDK users' network calls, shell operations, model invocations, or reads
outside the workspace. The MCP server and subprocess runner report their own
operations. Redact secrets from names, parameters, results, and error messages
before recording them; this project has no automatic secret scanner or encryption
layer.

`start()` commits before execution. Completion and its lifecycle event commit
together, exactly once. An interrupted worker can leave a pending call even if an
external side effect already happened. Check the external system before retrying;
pending does not mean safe to repeat. A failed call can also have partial effects.
Rejecting a branch, restoring a checkpoint, or selecting another fork never undoes
an email, API call, or host file write.

Use `workspace.backup(destination)` or CLI `checkpoint DESTINATION` for a coherent
online checkpoint including pending work and audit history. Open the checkpoint at
a separate path for inspection/recovery. A checkpoint restores database state,
rather than an OS process. There is no deterministic LLM replay guarantee.

## Optional runtime integrations

FUSE/NFS mounting would let ordinary programs access the database-backed tree
through a live host filesystem path. It is useful when an integration needs
ongoing filesystem access without routing operations through the SDK or MCP.
The existing runner already supports batch commands through materialization and
checked import. A native mount is an optional integration, not a requirement for
the current SDK release.

An OS sandbox addresses a different problem: restricting what a program can do
to the host. Version checks and atomic acceptance provide no such isolation.
Untrusted programs require a suitable external execution boundary; implementing
our own sandbox is a separate product decision. The current runner is for
trusted, cooperating programs and keeps its host-access boundary explicit.

## Relationship to AgentFS

These projects share substantial foundations. AgentFS already provides file,
JSON-state, tool-audit, session, overlay, and diff workflows. This release covers
the common storage surfaces in our own implementation and adds version-checked
acceptance across files and JSON state. It does not claim complete AgentFS parity.

| Capability | AgentFS primary documentation | AgentSQL 0.3.0a1 |
| --- | --- | --- |
| File operations, JSON state, tool-call history | SDK surfaces | Implemented; synchronous Python API with different return shapes |
| Named sessions and isolated work | Shared sessions and overlays | Named cooperative branches, isolated branches, provenance-preserving forks |
| Inspect changes | Diff and audit workflows | Diff, read dependencies, sealed proposal IDs, accepted history |
| Apply reviewed work | Session guide describes manual/scripted application | Atomic whole-proposal acceptance, rejecting stale tracked file or state inputs |
| Ordinary command execution | AgentFS runtime/overlay workflow | Temporary materialization plus conservative import; no syscall interception |
| Host filesystem mount | Linux FUSE / macOS NFS in the repository documentation | Not implemented |
| SDK languages and storage format | Python, TypeScript, Rust and AgentFS specification | Python SDK plus CLI/MCP; independent database schema |

This is an independent API and database format, not an AgentFS-compatible driver,
drop-in replacement, or a reader for AgentFS database files. Basic path names and
SDK method names deliberately feel familiar; similar names do not guarantee the
same semantics. Our `kv.list()` includes values and versions, and our `readdir()`
returns child metadata records rather than AgentFS Python's documented list of
names. Our runner's host access differs materially from a mounted/runtime overlay.

There are no hard links, symbolic links, mode bits, ownership metadata, device
nodes, live POSIX mounts, cloud synchronization, distributed locking, remote
execution service, or built-in encryption. SQLite handles cooperating processes
on one host with suitable local storage. Neither automatic semantic memory merging
nor universal agent-framework compatibility is promised.

Comparison checked against primary sources on 2026-10-25:
[AgentFS repository](https://github.com/tursodatabase/agentfs),
[Python SDK](https://docs.turso.tech/agentfs/sdk/python), and
[shared sessions guide](https://docs.turso.tech/agentfs/guides/sessions).

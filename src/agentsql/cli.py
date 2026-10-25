"""Small local CLI. Reads emit exact bytes; metadata and errors emit JSON."""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
from typing import NoReturn, Sequence

from .core import ConflictError, Workspace, WorkspaceError


class _UsageError(Exception):
    """Argument errors are structured like operational errors for CLI callers."""


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise _UsageError(message)


def _timeout(value: str) -> float:
    try:
        seconds = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("timeout must be finite seconds between 0 and 2147483.647") from error
    if not math.isfinite(seconds) or not 0 <= seconds <= 2_147_483.647:
        raise argparse.ArgumentTypeError("timeout must be finite seconds between 0 and 2147483.647")
    return seconds


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(
        prog="agentsql",
        description="AgentSQL: persistent agent state with tracked input checks.",
    )
    parser.add_argument(
        "-w", "--workspace", default="workspace.sqlite3", metavar="DATABASE",
        help="SQLite workspace path (default: workspace.sqlite3); place before the command",
    )
    parser.add_argument(
        "--timeout", type=_timeout, default=30.0, metavar="SECONDS",
        help="Lock wait, 0..2147483.647 seconds (default: 30); place before command",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    create = commands.add_parser("create", aliases=["init"], help="Create an empty workspace or capture files")
    create.add_argument("source", nargs="?", help="Optional source directory; pause writers during capture")

    branch = commands.add_parser("branch", help="Create an isolated branch from shared state")
    branch.add_argument("name")
    branch.add_argument("--revision", type=int, help="Branch from an accepted historical revision")
    commands.add_parser("branches", help="List branches and their status")
    session = commands.add_parser("session", help="Create or reopen a named cooperative branch")
    session.add_argument("name")
    fork = commands.add_parser("fork", help="Fork staged files and state while retaining their dependencies")
    fork.add_argument("source")
    fork.add_argument("name")

    read = commands.add_parser("read", help="Read branch bytes and track the input version")
    read.add_argument("branch")
    read.add_argument("path")

    write = commands.add_parser("write", help="Stage bytes on a branch (stdin by default)")
    write.add_argument("branch")
    write.add_argument("path")
    content = write.add_mutually_exclusive_group()
    content.add_argument("--from-file", metavar="FILE", help="Read bytes from a local file")
    content.add_argument("--text", help="Write the given text as UTF-8")

    delete = commands.add_parser("delete", help="Stage deletion of a branch file")
    delete.add_argument("branch")
    delete.add_argument("path")

    listing = commands.add_parser("list", help="List snapshot or branch files")
    selection = listing.add_mutually_exclusive_group()
    selection.add_argument("--branch", help="List a branch and track its directory membership")
    selection.add_argument("--revision", type=int, help="Read a historical shared snapshot")

    seal = commands.add_parser("seal", help="Freeze a complete proposal before review")
    seal.add_argument("branch")
    inspect = commands.add_parser("inspect", help="Show tracked inputs and the whole change set")
    inspect.add_argument("branch")
    diff = commands.add_parser("diff", help="Show proposed changes against the branch base")
    diff.add_argument("branch")

    accept = commands.add_parser("accept", help="Atomically validate and publish a whole branch")
    accept.add_argument("branch")
    accept.add_argument("-m", "--message", default="", help="Description for accepted history")
    accept.add_argument("--proposal", metavar="ID", help="Require this sealed proposal ID")

    commands.add_parser("history", help="Show the history of accepted workspace revisions")
    show = commands.add_parser("show", help="Read shared snapshot bytes without branch tracking")
    show.add_argument("path")
    show.add_argument("--revision", type=int, help="Read a historical shared snapshot")
    backup = commands.add_parser("backup", aliases=["checkpoint"], help="Create a consistent database checkpoint at a new path")
    backup.add_argument("destination", metavar="DESTINATION")
    commands.add_parser("check", help="Check SQLite, references, and stored content integrity")

    fs = commands.add_parser("fs", help="Operate on versioned files and directories")
    fs_selection = fs.add_mutually_exclusive_group()
    fs_selection.add_argument("--branch")
    fs_selection.add_argument("--revision", type=int)
    fs_commands = fs.add_subparsers(dest="fs_command", required=True)
    for name in ("read", "stat"):
        operation = fs_commands.add_parser(name)
        operation.add_argument("path")
    readdir = fs_commands.add_parser("readdir")
    readdir.add_argument("path", nargs="?", default="")
    fs_write = fs_commands.add_parser("write")
    fs_write.add_argument("path")
    fs_input = fs_write.add_mutually_exclusive_group()
    fs_input.add_argument("--text")
    fs_input.add_argument("--from-file")
    mkdir = fs_commands.add_parser("mkdir")
    mkdir.add_argument("path")
    mkdir.add_argument("--parents", action="store_true")
    mkdir.add_argument("--exist-ok", action="store_true")
    remove = fs_commands.add_parser("remove")
    remove.add_argument("path")
    remove.add_argument("--recursive", action="store_true")
    rename = fs_commands.add_parser("rename")
    rename.add_argument("source")
    rename.add_argument("destination")
    rename.add_argument("--replace", action="store_true")

    kv = commands.add_parser("kv", help="Read and stage versioned JSON state")
    kv_selection = kv.add_mutually_exclusive_group()
    kv_selection.add_argument("--branch")
    kv_selection.add_argument("--revision", type=int)
    kv_commands = kv.add_subparsers(dest="kv_command", required=True)
    for name in ("get", "delete"):
        operation = kv_commands.add_parser(name)
        operation.add_argument("key")
    kv_set = kv_commands.add_parser("set")
    kv_set.add_argument("key")
    kv_set.add_argument("value", help="A JSON value")
    kv_list = kv_commands.add_parser("list")
    kv_list.add_argument("--prefix", default="")

    tools = commands.add_parser("tools", help="Record and inspect explicit tool-call history")
    tools.add_argument("--branch")
    tool_commands = tools.add_subparsers(dest="tool_command", required=True)
    start = tool_commands.add_parser("start")
    start.add_argument("name")
    start.add_argument("--parameters", default="null")
    success = tool_commands.add_parser("success")
    success.add_argument("id", type=int)
    success.add_argument("--result", default="null")
    failure = tool_commands.add_parser("error")
    failure.add_argument("id", type=int)
    failure.add_argument("message")
    get = tool_commands.add_parser("get")
    get.add_argument("id", type=int)
    listing = tool_commands.add_parser("list")
    listing.add_argument("--name")
    listing.add_argument("--status", choices=("pending", "succeeded", "failed"))
    listing.add_argument("--limit", type=int, default=100)
    listing.add_argument("--after", type=int, default=0)
    tool_commands.add_parser("stats")
    events = tool_commands.add_parser("events")
    events.add_argument("--call-id", type=int)
    events.add_argument("--after", type=int, default=0)
    events.add_argument("--limit", type=int, default=100)
    timeline = commands.add_parser("timeline", help="Show explicitly recorded tool calls")
    timeline.add_argument("--branch")
    timeline.add_argument("--limit", type=int, default=100)
    serve = commands.add_parser("serve", help="Expose a branch through an MCP stdio server")
    serve.add_argument("--branch", required=True)
    run = commands.add_parser("run", help="Run a command in a materialized branch; this is not a security sandbox")
    run.add_argument("branch")
    run.add_argument("--command-timeout", type=_timeout, default=60.0, help="Process timeout; place before the branch name")
    run.add_argument("argv", nargs=argparse.REMAINDER)

    demo = commands.add_parser("demo", help="Run deterministic simulated agent workers; no API key")
    demo.add_argument(
        "--directory", type=Path,
        help="Keep the demo in a new or empty directory (default: temporary storage)",
    )
    return parser


def _json(value: object, *, error: bool = False) -> None:
    # ASCII JSON also handles undecodable filesystem names represented as
    # surrogate escapes, and works with a caller's non-UTF-8 terminal encoding.
    print(json.dumps(value, indent=2, sort_keys=True), file=sys.stderr if error else sys.stdout)


def _execute(args: argparse.Namespace) -> int:
    if args.command == "demo":
        from .demo import run_demo

        run_demo(args.directory)
        return 0
    if args.command in ("create", "init"):
        Workspace.create(args.workspace, args.source, timeout=args.timeout)
        _json({"created": True, "workspace": str(Path(args.workspace).expanduser().resolve())})
        return 0

    workspace = Workspace.open(args.workspace, timeout=args.timeout)
    if args.command == "branch":
        _json(workspace.branch(args.name, revision=args.revision).inspect())
    elif args.command == "session":
        _json(workspace.session(args.name).inspect())
    elif args.command == "fork":
        _json(workspace.fork(args.source, args.name).inspect())
    elif args.command == "branches":
        _json(workspace.branches())
    elif args.command == "read":
        sys.stdout.buffer.write(workspace.get_branch(args.branch).read_bytes(args.path))
    elif args.command == "write":
        if args.from_file is not None:
            data = Path(args.from_file).read_bytes()
        elif args.text is not None:
            data = args.text.encode("utf-8")
        else:
            data = sys.stdin.buffer.read()
        workspace.get_branch(args.branch).write_bytes(args.path, data)
        _json({"branch": args.branch, "path": args.path, "staged": "write", "bytes": len(data)})
    elif args.command == "delete":
        workspace.get_branch(args.branch).delete(args.path)
        _json({"branch": args.branch, "path": args.path, "staged": "delete"})
    elif args.command == "list":
        if args.branch is not None:
            files = workspace.get_branch(args.branch).list_files()
        else:
            files = workspace.snapshot(args.revision).list_files()
        _json({"files": files})
    elif args.command == "seal":
        proposal_id = workspace.get_branch(args.branch).seal()
        _json({"branch": args.branch, "proposal_id": proposal_id})
    elif args.command == "inspect":
        _json(workspace.get_branch(args.branch).inspect())
    elif args.command == "diff":
        sys.stdout.write(workspace.get_branch(args.branch).diff())
    elif args.command == "accept":
        _json(workspace.get_branch(args.branch).accept(
            message=args.message, expected_proposal=args.proposal,
        ))
    elif args.command == "history":
        _json(workspace.history())
    elif args.command == "show":
        sys.stdout.buffer.write(workspace.snapshot(args.revision).read_bytes(args.path))
    elif args.command in ("backup", "checkpoint"):
        destination = workspace.backup(args.destination)
        _json({"backup": str(destination)})
    elif args.command == "check":
        result = workspace.check_integrity()
        _json(result)
        return 0 if result["ok"] else 1
    elif args.command == "fs":
        mutating = args.fs_command in ("write", "mkdir", "remove", "rename")
        if mutating and args.branch is None:
            raise _UsageError("File edits require --branch")
        owner = workspace.get_branch(args.branch) if args.branch is not None else workspace.snapshot(args.revision)
        filesystem = owner.fs
        if args.fs_command == "read":
            sys.stdout.buffer.write(filesystem.read_file(args.path, encoding=None))
        elif args.fs_command == "stat":
            _json(filesystem.stat(args.path))
        elif args.fs_command == "readdir":
            _json(filesystem.readdir(args.path))
        elif args.fs_command == "write":
            data = Path(args.from_file).read_bytes() if args.from_file is not None else (
                args.text.encode("utf-8") if args.text is not None else sys.stdin.buffer.read())
            filesystem.write_file(args.path, data)
            _json({"path": args.path, "staged": "write", "bytes": len(data)})
        elif args.fs_command == "mkdir":
            filesystem.mkdir(args.path, parents=args.parents, exist_ok=args.exist_ok)
            _json({"path": args.path, "staged": "mkdir"})
        elif args.fs_command == "remove":
            filesystem.remove(args.path, recursive=args.recursive)
            _json({"path": args.path, "staged": "remove"})
        elif args.fs_command == "rename":
            filesystem.rename(args.source, args.destination, replace=args.replace)
            _json({"source": args.source, "destination": args.destination, "staged": "rename"})
    elif args.command == "kv":
        if args.kv_command in ("set", "delete") and args.branch is None:
            raise _UsageError("State edits require --branch")
        state_owner = workspace.get_branch(args.branch) if args.branch is not None else workspace.snapshot(args.revision)
        if args.kv_command == "get":
            _json(state_owner.kv.get(args.key))
        elif args.kv_command == "list":
            _json(state_owner.kv.list(args.prefix))
        elif args.kv_command == "set":
            state_owner.kv.set(args.key, json.loads(args.value))
            _json({"key": args.key, "staged": "set"})
        elif args.kv_command == "delete":
            state_owner.kv.delete(args.key)
            _json({"key": args.key, "staged": "delete"})
    elif args.command in ("tools", "timeline"):
        log = workspace.tools if args.branch is None else workspace.get_branch(args.branch).tools
        if args.command == "timeline":
            _json(log.list(limit=args.limit))
        elif args.tool_command == "start":
            _json({"id": log.start(args.name, json.loads(args.parameters))})
        elif args.tool_command == "success":
            _json(log.success(args.id, json.loads(args.result)))
        elif args.tool_command == "error":
            _json(log.error(args.id, args.message))
        elif args.tool_command == "get":
            _json(log.get(args.id))
        elif args.tool_command == "list":
            _json(log.list(name=args.name, status=args.status, limit=args.limit, after=args.after))
        elif args.tool_command == "stats":
            _json(log.stats())
        elif args.tool_command == "events":
            _json(log.events(call_id=args.call_id, after=args.after, limit=args.limit))
    elif args.command == "serve":
        from .server import serve
        serve(workspace, args.branch)
    elif args.command == "run":
        from .runner import run_command
        command = args.argv[1:] if args.argv and args.argv[0] == "--" else args.argv
        result = run_command(workspace, args.branch, command, timeout=args.command_timeout)
        _json(result)
        return 0 if result.get("imported") else 1
    return 0


def _failure(error: Exception | BaseException, status: int) -> int:
    try:
        if isinstance(error, WorkspaceError) and callable(getattr(error, "as_dict", None)):
            payload = error.as_dict()
        else:
            message = "Operation interrupted" if isinstance(error, KeyboardInterrupt) else str(error)
            payload = {"error": type(error).__name__.lstrip("_"), "message": message}
        _json(payload, error=True)
        sys.stderr.flush()
    except (OSError, UnicodeError):
        # A closed diagnostic pipe must not turn a handled error into a traceback.
        pass
    return status


def _broken_pipe() -> int:
    # Flush in main catches deferred pipe failures before interpreter shutdown.
    # Redirect the real descriptor so Python does not retry that failed flush.
    try:
        descriptor = sys.stdout.fileno()
        with open(os.devnull, "wb") as sink:
            os.dup2(sink.fileno(), descriptor)
    except (AttributeError, OSError, ValueError):
        pass
    return 1


def main(argv: Sequence[str] | None = None) -> int:
    """Return 0 success, 1 operation error, 2 stale/usage error, 130 interrupt.

    Once publication commits, a lost stdout response or interruption is not
    evidence of rollback. Reopen and inspect history before retrying acceptance.
    """
    try:
        args = _parser().parse_args(argv)
        status = _execute(args)
        sys.stdout.flush()
        return status
    except BrokenPipeError:
        return _broken_pipe()
    except _UsageError as error:
        return _failure(error, 2)
    except ConflictError as error:
        return _failure(error, 2)
    except KeyboardInterrupt as error:
        return _failure(error, 130)
    except (WorkspaceError, OSError, ValueError, UnicodeError, sqlite3.Error, KeyError) as error:
        return _failure(error, 1)


if __name__ == "__main__":
    raise SystemExit(main())

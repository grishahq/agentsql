"""A small branch-scoped MCP server over newline-delimited JSON-RPC stdio.

Only workspace tools are exposed. Publication and process execution deliberately
remain coordinator operations; this server does not expose host filesystem paths.
"""

from __future__ import annotations

import base64
import binascii
import json
import sys
from typing import Any, BinaryIO

from . import __version__
from .core import Branch, Workspace, WorkspaceError

PROTOCOL_VERSIONS = ("2025-11-25", "2025-06-18", "2024-11-05")
MAX_MESSAGE_BYTES = 8 * 1024 * 1024
MAX_FILE_BYTES = 4 * 1024 * 1024


class _ProtocolError(Exception):
    def __init__(self, code: int, message: str):
        self.code, self.message = code, message
        super().__init__(message)


def _tool(name: str, description: str, properties: dict | None = None,
          required: tuple[str, ...] = (), *, readonly: bool = False,
          destructive: bool = False) -> dict:
    return {
        "name": name, "description": description,
        "inputSchema": {"type": "object", "properties": properties or {},
                        "required": list(required), "additionalProperties": False},
        "annotations": {"readOnlyHint": readonly, "destructiveHint": destructive,
                        "openWorldHint": False},
    }


_STRING = {"type": "string"}
_BOOL = {"type": "boolean"}
_ENCODING = {"type": "string", "enum": ["utf-8", "base64"]}
_TOOLS = [
    _tool("read_file", "Read a file and track its input version. Max 4 MiB.",
          {"path": _STRING, "encoding": _ENCODING}, ("path",)),
    _tool("write_file", "Stage a UTF-8 or base64 encoded file. Max 4 MiB.",
          {"path": _STRING, "content": _STRING, "encoding": _ENCODING}, ("path", "content")),
    _tool("readdir", "List direct children of a directory; track the observed listing.",
          {"path": _STRING}),
    _tool("mkdir", "Stage an empty directory.",
          {"path": _STRING, "parents": _BOOL, "exist_ok": _BOOL}, ("path",)),
    _tool("remove", "Stage removal of a file or directory.",
          {"path": _STRING, "recursive": _BOOL}, ("path",), destructive=True),
    _tool("rename", "Stage an atomic file or directory rename.",
          {"source": _STRING, "destination": _STRING, "replace": _BOOL},
          ("source", "destination"), destructive=True),
    _tool("stat", "Inspect a workspace entry; track its version.",
          {"path": _STRING}, ("path",)),
    _tool("kv_get", "Read a JSON value and track its input version.",
          {"key": _STRING}, ("key",)),
    _tool("kv_set", "Stage a JSON key-value entry.",
          {"key": _STRING, "value": {}}, ("key", "value")),
    _tool("kv_delete", "Stage deletion of a JSON key-value entry.",
          {"key": _STRING}, ("key",), destructive=True),
    _tool("kv_list", "List JSON entries with an optional key prefix; track dependencies.",
          {"prefix": _STRING}),
    _tool("inspect", "Inspect this branch's changes and tracked dependencies.", readonly=True),
    _tool("diff", "Show this branch's proposed file changes.", readonly=True),
    _tool("seal", "Freeze this proposal for coordinator review. Further reads/writes fail."),
]
_TOOL_BY_NAME = {tool["name"]: tool for tool in _TOOLS}


def _validate_arguments(name: str, arguments: Any) -> dict:
    if name not in _TOOL_BY_NAME:
        raise _ProtocolError(-32602, f"Unknown tool: {name}")
    if not isinstance(arguments, dict):
        raise _ProtocolError(-32602, "Tool arguments must be an object")
    schema = _TOOL_BY_NAME[name]["inputSchema"]
    if set(arguments) - set(schema["properties"]):
        raise _ProtocolError(-32602, "Unknown tool argument")
    if set(schema["required"]) - set(arguments):
        raise _ProtocolError(-32602, "Missing required tool argument")
    for key, value in arguments.items():
        field = schema["properties"][key]
        expected = field.get("type")
        if expected == "string" and not isinstance(value, str):
            raise _ProtocolError(-32602, f"{key} must be a string")
        if expected == "boolean" and not isinstance(value, bool):
            raise _ProtocolError(-32602, f"{key} must be a boolean")
        if "enum" in field and value not in field["enum"]:
            raise _ProtocolError(-32602, f"Unsupported {key}")
    return arguments


def _execute(branch: Branch, name: str, arguments: dict) -> dict:
    fs = branch.fs
    if name == "read_file":
        if fs.stat(arguments["path"])["size"] > MAX_FILE_BYTES:
            raise ValueError("File exceeds the 4 MiB MCP tool limit; use the SDK")
        data = fs.read_file(arguments["path"], encoding=None)
        if len(data) > MAX_FILE_BYTES:
            raise ValueError("File exceeds the 4 MiB MCP tool limit; use the SDK")
        encoding = arguments.get("encoding", "utf-8")
        content = (data.decode("utf-8") if encoding == "utf-8"
                   else base64.b64encode(data).decode("ascii"))
        return {"path": arguments["path"], "encoding": encoding, "content": content}
    if name == "write_file":
        if arguments.get("encoding", "utf-8") == "base64":
            data = base64.b64decode(arguments["content"], validate=True)
        else:
            data = arguments["content"].encode("utf-8")
        if len(data) > MAX_FILE_BYTES:
            raise ValueError("File exceeds the 4 MiB MCP tool limit; use the SDK")
        fs.write_file(arguments["path"], data)
        return {"path": arguments["path"], "size": len(data)}
    if name == "readdir":
        return {"entries": fs.readdir(arguments.get("path", ""))}
    if name == "mkdir":
        fs.mkdir(arguments["path"], parents=arguments.get("parents", False),
                 exist_ok=arguments.get("exist_ok", False))
        return {"path": arguments["path"]}
    if name == "remove":
        fs.remove(arguments["path"], recursive=arguments.get("recursive", False))
        return {"path": arguments["path"]}
    if name == "rename":
        fs.rename(arguments["source"], arguments["destination"],
                  replace=arguments.get("replace", False))
        return {"source": arguments["source"], "destination": arguments["destination"]}
    if name == "stat":
        return fs.stat(arguments["path"])
    if name == "kv_get":
        return {"key": arguments["key"], "value": branch.kv.get(arguments["key"])}
    if name == "kv_set":
        branch.kv.set(arguments["key"], arguments["value"])
        return {"key": arguments["key"]}
    if name == "kv_delete":
        branch.kv.delete(arguments["key"])
        return {"key": arguments["key"]}
    if name == "kv_list":
        return {"entries": branch.kv.list(arguments.get("prefix", ""))}
    if name == "inspect":
        return branch.inspect()
    if name == "diff":
        return {"diff": branch.diff()}
    if name == "seal":
        return {"proposal_id": branch.seal()}
    raise _ProtocolError(-32602, f"Unknown tool: {name}")


def _error(identifier: Any, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": identifier, "error": {"code": code, "message": message}}


def _tool_result(result: dict, *, error: bool = False) -> dict:
    return {"content": [{"type": "text", "text": json.dumps(result, ensure_ascii=True)}],
            "structuredContent": result, "isError": error}


def _reject_constant(value: str) -> None:
    raise ValueError(f"Non-finite JSON constant: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict:
    value: dict = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate JSON object key")
        value[key] = item
    return value


def _check_depth(value: Any) -> None:
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > 64:
            raise ValueError("JSON nesting exceeds the 64 level limit")
        if isinstance(item, dict):
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)


def serve(workspace: Workspace, branch_name: str, *, input_stream: BinaryIO | None = None,
          output: BinaryIO | None = None) -> None:
    """Serve one existing branch until EOF. This is a local stdio-only MCP server.

    Requests and responses are capped at 8 MiB each. Oversized frames are drained
    through their newline so the following request remains readable. A coordinator
    must accept results separately after reviewing the sealed proposal.
    """
    branch = workspace.get_branch(branch_name)
    source = input_stream if input_stream is not None else sys.stdin.buffer
    sink = output if output is not None else sys.stdout.buffer
    initialized = False
    ready = False
    protocol_version = PROTOCOL_VERSIONS[0]

    def send(response: dict) -> None:
        encoded = json.dumps(response, ensure_ascii=True, allow_nan=False).encode("utf-8")
        if len(encoded) > MAX_MESSAGE_BYTES:
            encoded = json.dumps(_error(response.get("id"), -32001,
                "Response exceeds the 8 MiB limit; use the SDK")).encode("utf-8")
        sink.write(encoded + b"\n")
        sink.flush()

    while True:
        line = source.readline(MAX_MESSAGE_BYTES + 1)
        if not line:
            return
        if len(line) > MAX_MESSAGE_BYTES:
            while not line.endswith(b"\n"):
                line = source.readline(MAX_MESSAGE_BYTES + 1)
                if not line:
                    break
            send(_error(None, -32700, "Message exceeds the 8 MiB limit"))
            continue
        try:
            request = json.loads(line, parse_constant=_reject_constant,
                                 object_pairs_hook=_unique_object)
            _check_depth(request)
        except (ValueError, UnicodeError, RecursionError):
            send(_error(None, -32700, "Invalid UTF-8 JSON"))
            continue
        if not isinstance(request, dict):
            send(_error(None, -32600, "Requests must be JSON-RPC objects; batches are unsupported"))
            continue
        identifier = request.get("id")
        if (request.get("jsonrpc") != "2.0" or not isinstance(request.get("method"), str)
                or ("id" in request and (isinstance(identifier, bool)
                    or not isinstance(identifier, (int, str))))):
            send(_error(None, -32600, "Invalid JSON-RPC request"))
            continue
        method = request["method"]
        # Requests without IDs are notifications and MUST NOT produce responses.
        # Never execute an accidental mutating tool call sent as a notification.
        if "id" not in request:
            if method == "notifications/initialized" and initialized:
                ready = True
            continue
        result: dict
        try:
            params = request.get("params", {})
            if not isinstance(params, dict):
                raise _ProtocolError(-32602, "params must be an object")
            if method == "initialize":
                if initialized:
                    raise _ProtocolError(-32600, "Connection is already initialized")
                if (not isinstance(params.get("protocolVersion"), str)
                        or not isinstance(params.get("capabilities"), dict)
                        or not isinstance(params.get("clientInfo"), dict)
                        or not isinstance(params["clientInfo"].get("name"), str)
                        or not isinstance(params["clientInfo"].get("version"), str)):
                    raise _ProtocolError(-32602, "Invalid initialize parameters")
                requested = params["protocolVersion"]
                version = requested if requested in PROTOCOL_VERSIONS else PROTOCOL_VERSIONS[0]
                protocol_version = version
                result = {"protocolVersion": version, "capabilities": {"tools": {}},
                          "serverInfo": {"name": "agentsql", "version": __version__},
                          "instructions": "Tools operate on one isolated branch. Reads track input "
                          "versions. Seal the proposal for coordinator review; publication is external."}
                initialized = True
            elif method == "ping":
                result = {}
            elif not ready:
                raise _ProtocolError(-32002, "Initialize and send notifications/initialized first")
            elif method == "tools/list":
                if set(params) - {"_meta"}:
                    raise _ProtocolError(-32602, "This tool list has no pagination cursor")
                tools = _TOOLS
                if protocol_version == "2024-11-05":
                    tools = [{key: value for key, value in item.items() if key != "annotations"}
                             for item in _TOOLS]
                result = {"tools": tools}
            elif method == "tools/call":
                if set(params) - {"name", "arguments", "_meta"} or not isinstance(params.get("name"), str):
                    raise _ProtocolError(-32602, "Invalid tool call parameters")
                arguments = _validate_arguments(params["name"], params.get("arguments", {}))
                call_id = None
                try:
                    call_id = branch.tools.start(f"mcp.{params['name']}", arguments)
                    value = _execute(branch, params["name"], arguments)
                    branch.tools.success(call_id, value)
                    result = _tool_result(value)
                except (WorkspaceError, OSError, ValueError, TypeError, binascii.Error) as exc:
                    value = (exc.as_dict() if isinstance(exc, WorkspaceError)
                             else {"error": type(exc).__name__, "message": str(exc)})
                    if call_id is not None:
                        try:
                            branch.tools.error(call_id, str(exc))
                        except WorkspaceError:
                            pass  # Preserve the original actionable tool failure.
                    result = _tool_result(value, error=True)
                if protocol_version == "2024-11-05":
                    result.pop("structuredContent", None)
            else:
                raise _ProtocolError(-32601, f"Unknown method: {method}")
            send({"jsonrpc": "2.0", "id": identifier, "result": result})
        except _ProtocolError as exc:
            send(_error(identifier, exc.code, exc.message))

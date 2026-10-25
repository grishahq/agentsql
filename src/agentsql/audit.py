"""Explicit, durable tool-call history, independent of workspace acceptance.

Only calls reported through this API are observed. A pending row means that the
outcome is unknown: an interrupted worker might already have caused an external
effect. Rejecting a branch or restoring a database cannot undo that effect.
Parameters and results are caller-controlled; redact secrets before recording.
"""

from __future__ import annotations

from contextlib import contextmanager
import json
import math
import sqlite3
import time
from typing import TYPE_CHECKING, Iterator, List

from .core import WorkspaceError, _branch

if TYPE_CHECKING:
    from .core import Workspace


def _text(value: object, label: str, *, name: bool = False) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a string")
    if name and (not value.strip() or len(value) > 200
                 or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        raise ValueError(f"{label} must contain 1–200 characters without control characters")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise ValueError(f"{label} must be valid UTF-8") from exc
    return value


def _json(value: object) -> str | None:
    """Reject coercions such as non-string object keys and non-finite numbers."""
    def validate(item: object) -> None:
        if item is None or type(item) in (bool, int):
            return
        if type(item) is float and math.isfinite(item):
            return
        if type(item) is str:
            _text(item, "JSON strings")
            return
        if type(item) is list:
            for child in item:
                validate(child)
            return
        if type(item) is dict:
            for key, child in item.items():
                if not isinstance(key, str):
                    raise ValueError("JSON object keys must be strings")
                _text(key, "JSON object keys")
                validate(child)
            return
        raise ValueError("Parameters and results must contain only finite JSON values")

    try:
        validate(value)
        return None if value is None else json.dumps(
            value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, RecursionError, OverflowError) as exc:
        raise ValueError("Parameters and results must be valid JSON values") from exc


def _number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError(f"{label} must be a finite nonnegative timestamp")
    try:
        result = float(value)
    except OverflowError as exc:
        raise ValueError(f"{label} must be a finite nonnegative timestamp") from exc
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{label} must be a finite nonnegative timestamp")
    return result


def _integer(value: object, label: str, minimum: int = 0, maximum: int | None = None) -> int:
    if (isinstance(value, bool) or not isinstance(value, int) or value < minimum
            or (maximum is not None and value > maximum) or value > 9_223_372_036_854_775_807):
        raise ValueError(f"Invalid {label}")
    return value


def _decode(row: sqlite3.Row) -> dict:
    result = dict(row)
    for key in ("parameters", "result"):
        result[key] = None if result[key] is None else json.loads(result[key])
    return result


class ToolCall:
    """Context-manager handle. Results are copied when set, before completion."""

    def __init__(self, identifier: int):
        self._id = identifier
        self._result: str | None = None

    @property
    def id(self) -> int:
        return self._id

    def set_result(self, value: object) -> None:
        self._result = _json(value)


class ToolLog:
    """Append-only call lifecycle with optional branch-scoped access.

    Branch-scoped logs permit reporting effects after sealing or acceptance.
    Their calls cannot be read or completed through another branch's log.
    The workspace-level log can inspect and complete any call.
    """

    def __init__(self, workspace: Workspace, branch: str | None = None):
        self._workspace = workspace
        self._branch = branch
        if branch is not None:
            with workspace._transaction() as db:
                _branch(db, branch)

    def _scope(self, db: sqlite3.Connection) -> tuple[str, tuple]:
        if self._branch is None:
            return "", ()
        _branch(db, self._branch)
        return " AND branch=?", (self._branch,)

    def _call(self, db: sqlite3.Connection, identifier: int) -> sqlite3.Row:
        scope, arguments = self._scope(db)
        row = db.execute("SELECT * FROM tool_calls WHERE id=?" + scope,
                         (identifier, *arguments)).fetchone()
        if row is None:
            raise WorkspaceError(f"Unknown tool call: {identifier}")
        return row

    def start(self, name: str, parameters: object = None) -> int:
        """Commit intent before running a tool; no transaction spans its execution."""
        name = _text(name, "Tool name", name=True)
        encoded = _json(parameters)
        at = _number(time.time(), "started_at")
        with self._workspace._transaction(write=True) as db:
            self._scope(db)
            cursor = db.execute(
                "INSERT INTO tool_calls(branch,name,parameters,status,started_at) "
                "VALUES (?,?,?,'pending',?)", (self._branch, name, encoded, at))
            assert cursor.lastrowid is not None
            identifier = cursor.lastrowid
            db.execute("INSERT INTO tool_events(call_id,kind,at) VALUES (?,'started',?)",
                       (identifier, at))
        return identifier

    def _finish(self, identifier: int, *, result: object = None,
                error: str | None = None) -> dict:
        identifier = _integer(identifier, "call ID", 1)
        encoded = _json(result)
        if error is not None:
            error = _text(error, "Tool error")
        status = "succeeded" if error is None else "failed"
        with self._workspace._transaction(write=True) as db:
            row = self._call(db, identifier)
            if row["status"] != "pending":
                raise WorkspaceError(f"Tool call {identifier} is already {row['status']}")
            # A wall-clock correction cannot produce a negative duration.
            completed = max(_number(time.time(), "completed_at"), row["started_at"])
            duration = (completed - row["started_at"]) * 1000
            db.execute("UPDATE tool_calls SET status=?,result=?,error=?,completed_at=?,duration_ms=? "
                       "WHERE id=?", (status, encoded, error, completed, duration, identifier))
            db.execute("INSERT INTO tool_events(call_id,kind,at) VALUES (?,?,?)",
                       (identifier, status, completed))
            result_row = _decode(self._call(db, identifier))
        return result_row

    def success(self, identifier: int, result: object = None) -> dict:
        return self._finish(identifier, result=result)

    def error(self, identifier: int, error: str) -> dict:
        return self._finish(identifier, error=_text(error, "Tool error"))

    def record(self, name: str, *, parameters: object = None, result: object = None,
               error: str | None = None, started_at: float | None = None,
               completed_at: float | None = None) -> int:
        """Atomically record an already completed call with its two lifecycle events."""
        name = _text(name, "Tool name", name=True)
        parameters_json, result_json = _json(parameters), _json(result)
        if error is not None:
            error = _text(error, "Tool error")
            if result is not None:
                raise ValueError("A failed tool call cannot also have a result")
        start = _number(time.time() if started_at is None else started_at, "started_at")
        end = _number(max(time.time(), start) if completed_at is None else completed_at,
                      "completed_at")
        duration = (end - start) * 1000
        if end < start or not math.isfinite(duration):
            raise ValueError("completed_at must follow started_at with a finite duration")
        status = "succeeded" if error is None else "failed"
        with self._workspace._transaction(write=True) as db:
            self._scope(db)
            cursor = db.execute(
                "INSERT INTO tool_calls(branch,name,parameters,status,result,error,started_at,"
                "completed_at,duration_ms) VALUES (?,?,?,?,?,?,?,?,?)",
                (self._branch, name, parameters_json, status, result_json, error, start, end, duration))
            assert cursor.lastrowid is not None
            identifier = cursor.lastrowid
            db.executemany("INSERT INTO tool_events(call_id,kind,at) VALUES (?,?,?)",
                           [(identifier, "started", start), (identifier, status, end)])
        return identifier

    def get(self, identifier: int) -> dict:
        identifier = _integer(identifier, "call ID", 1)
        with self._workspace._transaction() as db:
            return _decode(self._call(db, identifier))

    def list(self, *, name: str | None = None, status: str | None = None,
             limit: int = 100, after: int = 0) -> list[dict]:
        limit, after = _integer(limit, "limit", 1, 1000), _integer(after, "after")
        if name is not None:
            name = _text(name, "Tool name", name=True)
        if status is not None and status not in ("pending", "succeeded", "failed"):
            raise ValueError("Tool status must be pending, succeeded, or failed")
        with self._workspace._transaction() as db:
            scope, arguments = self._scope(db)
            parameters: List = [after, *arguments]
            for key, value in (("name", name), ("status", status)):
                if value is not None:
                    scope += f" AND {key}=?"
                    parameters.append(value)
            return [_decode(row) for row in db.execute(
                "SELECT * FROM tool_calls WHERE id>?" + scope + " ORDER BY id LIMIT ?",
                (*parameters, limit))]

    def stats(self) -> List[dict]:
        with self._workspace._transaction() as db:
            scope, arguments = self._scope(db)
            return [dict(row) for row in db.execute(
                "SELECT name,COUNT(*) AS total,SUM(status='pending') AS pending,"
                "SUM(status='succeeded') AS succeeded,SUM(status='failed') AS failed,"
                "AVG(duration_ms) AS avg_duration_ms FROM tool_calls WHERE 1=1"
                + scope + " GROUP BY name ORDER BY name", arguments)]

    def events(self, call_id: int | None = None, *, after: int = 0,
               limit: int = 100) -> List[dict]:
        limit, after = _integer(limit, "limit", 1, 1000), _integer(after, "after")
        if call_id is not None:
            call_id = _integer(call_id, "call ID", 1)
        with self._workspace._transaction() as db:
            scope, arguments = self._scope(db)
            parameters: List = [after, *arguments]
            if call_id is not None:
                self._call(db, call_id)
                scope += " AND c.id=?"
                parameters.append(call_id)
            return [dict(row) for row in db.execute(
                "SELECT e.id,e.call_id,e.kind,e.at FROM tool_events e "
                "JOIN tool_calls c ON c.id=e.call_id WHERE e.id>?" + scope
                + " ORDER BY e.id LIMIT ?", (*parameters, limit))]

    @contextmanager
    def track(self, name: str, parameters: object = None) -> Iterator[ToolCall]:
        """Record entry and exit. Tool exceptions are recorded and re-raised."""
        identifier = self.start(name, parameters)
        handle = ToolCall(identifier)
        try:
            yield handle
        except BaseException as exc:
            try:
                self.error(identifier, repr(exc))
            except Exception as audit_error:
                # Preserve the actual tool failure even if storage also failed.
                raise exc from audit_error
            raise
        else:
            self.success(identifier, None if handle._result is None else json.loads(handle._result))

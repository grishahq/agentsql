"""Versioned JSON key-value state, accepted atomically with branch files."""

from __future__ import annotations

import json
import math
import sqlite3

from .core import Branch, Snapshot, WorkspaceError, _branch, _sha


def _key(value: str, *, prefix: bool = False) -> str:
    if not isinstance(value, str) or (not value and not prefix) or len(value) > 1024:
        raise ValueError("Keys must contain 1–1024 characters; listing prefixes may be empty")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("Keys cannot contain control characters")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise ValueError("Keys must be valid UTF-8") from exc
    return value


def _encode(value: object) -> str:
    def validate(item: object) -> None:
        if item is None or type(item) in (bool, int):
            return
        if type(item) is float and math.isfinite(item):
            return
        if type(item) is str:
            item.encode("utf-8")
            return
        if type(item) is list:
            for child in item:
                validate(child)
            return
        if type(item) is dict:
            for key, child in item.items():
                if not isinstance(key, str):
                    raise ValueError("JSON object keys must be strings")
                key.encode("utf-8")
                validate(child)
            return
        raise ValueError("Values must contain only finite JSON types")
    try:
        validate(value)
        return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":"))
    except (UnicodeError, TypeError, RecursionError, OverflowError) as exc:
        raise ValueError("Value must be valid finite JSON") from exc


def _kv_entry(db: sqlite3.Connection, revision: int, key: str) -> tuple[int, str | None]:
    row = db.execute("SELECT version,value FROM kv_entries WHERE revision=? AND key=?", (revision, key)).fetchone()
    return (row[0], row[1]) if row else (0, None)


def _kv_values(db: sqlite3.Connection, revision: int) -> dict[str, tuple[int | None, str]]:
    return {row[0]: (row[1], row[2]) for row in db.execute(
        "SELECT key,version,value FROM kv_entries WHERE revision=? AND value IS NOT NULL", (revision,))}


def _kv_listing(db: sqlite3.Connection, revision: int, prefix: str) -> str:
    # Python startswith treats %, _ and backslashes literally, unlike SQL LIKE.
    keys = sorted(key for key in _kv_values(db, revision) if key.startswith(prefix))
    return _sha(json.dumps(keys, ensure_ascii=True, separators=(",", ":")).encode("ascii"))


class KeyValue:
    """JSON state on a fixed snapshot or tracked mutable branch.

    SQL NULL is a deletion tombstone; the JSON value null is stored as 'null'
    and remains distinguishable with exists(). Reads return independent copies.
    """

    def __init__(self, owner: Branch | Snapshot):
        self._owner = owner
        self._workspace = owner._workspace

    def _get(self, key: str) -> str | None:
        key = _key(key)
        if isinstance(self._owner, Snapshot):
            with self._workspace._transaction() as db:
                return _kv_entry(db, self._owner.revision, key)[1]
        with self._workspace._transaction(write=True) as db:
            branch = _branch(db, self._owner.name, require_open=True)
            pending = db.execute("SELECT value FROM kv_changes WHERE branch=? AND key=?", (self._owner.name, key)).fetchone()
            if pending is not None:
                return pending[0]
            version, value = _kv_entry(db, branch["base_revision"], key)
            db.execute("INSERT OR IGNORE INTO kv_reads VALUES (?,?,?)", (self._owner.name, key, version))
            return value

    def get(self, key: str, default: object = None):
        value = self._get(key)
        return default if value is None else json.loads(value)

    def exists(self, key: str) -> bool:
        return self._get(key) is not None

    def version(self, key: str) -> int | None:
        key = _key(key)
        if isinstance(self._owner, Snapshot):
            with self._workspace._transaction() as db:
                return _kv_entry(db, self._owner.revision, key)[0]
        with self._workspace._transaction(write=True) as db:
            branch = _branch(db, self._owner.name, require_open=True)
            if db.execute("SELECT 1 FROM kv_changes WHERE branch=? AND key=?", (self._owner.name, key)).fetchone():
                return None
            version, _ = _kv_entry(db, branch["base_revision"], key)
            db.execute("INSERT OR IGNORE INTO kv_reads VALUES (?,?,?)",
                       (self._owner.name, key, version))
            return version

    def set(self, key: str, value: object) -> None:
        key, encoded = _key(key), _encode(value)
        if not isinstance(self._owner, Branch):
            raise WorkspaceError("Snapshots are immutable; create a branch to edit state")
        with self._workspace._transaction(write=True) as db:
            _branch(db, self._owner.name, require_open=True)
            db.execute("INSERT INTO kv_changes VALUES (?,?,?) ON CONFLICT(branch,key) DO UPDATE SET value=excluded.value",
                       (self._owner.name, key, encoded))

    def delete(self, key: str) -> None:
        key = _key(key)
        if not isinstance(self._owner, Branch):
            raise WorkspaceError("Snapshots are immutable; create a branch to edit state")
        missing = False
        with self._workspace._transaction(write=True) as db:
            branch = _branch(db, self._owner.name, require_open=True)
            pending = db.execute("SELECT value FROM kv_changes WHERE branch=? AND key=?", (self._owner.name, key)).fetchone()
            version, base_value = _kv_entry(db, branch["base_revision"], key)
            value = pending[0] if pending is not None else base_value
            if value is None:
                missing = True
                if pending is None:
                    db.execute("INSERT OR IGNORE INTO kv_reads VALUES (?,?,?)",
                               (self._owner.name, key, version))
            else:
                db.execute("INSERT INTO kv_changes VALUES (?,?,NULL) ON CONFLICT(branch,key) DO UPDATE SET value=NULL", (self._owner.name, key))
        # Missing deletions are observations too. Commit absence before raising,
        # so callers that catch KeyError still retain the dependency they used.
        if missing:
            raise KeyError(key)

    def list(self, prefix: str = "") -> list[dict]:
        prefix = _key(prefix, prefix=True)
        with self._workspace._transaction(write=isinstance(self._owner, Branch)) as db:
            if isinstance(self._owner, Snapshot):
                values = _kv_values(db, self._owner.revision)
            else:
                branch = _branch(db, self._owner.name, require_open=True)
                base = branch["base_revision"]
                values = _kv_values(db, base)
                db.execute("INSERT OR IGNORE INTO kv_list_reads VALUES (?,?,?)",
                           (self._owner.name, prefix, _kv_listing(db, base, prefix)))
                for row in db.execute("SELECT key,value FROM kv_changes WHERE branch=?", (self._owner.name,)):
                    if row[1] is None:
                        values.pop(row[0], None)
                    else:
                        values[row[0]] = (None, row[1])
                for key, (version, value) in values.items():
                    if key.startswith(prefix) and version is not None:
                        db.execute("INSERT OR IGNORE INTO kv_reads VALUES (?,?,?)", (self._owner.name, key, version))
            return [{"key": key, "value": json.loads(value), "version": version}
                    for key, (version, value) in sorted(values.items()) if key.startswith(prefix)]

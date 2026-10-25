"""Local versioned file workspaces with atomic dependency validation.

SQLite owns both metadata and bytes. No operation publishes into an external
directory: a revision pointer is the single shared-state publication point.
"""

from __future__ import annotations

import difflib
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import time
from contextlib import contextmanager
from typing import Iterator


class WorkspaceError(Exception):
    """An invalid workspace operation."""

    def as_dict(self) -> dict:
        return {"error": type(self).__name__, "message": str(self)}


class InvalidPathError(WorkspaceError):
    """A path is unsafe or cannot belong to a portable file tree."""


class BranchClosedError(WorkspaceError):
    """An accepted branch cannot be modified or used for new tracked reads."""


class WorkspaceBusyError(WorkspaceError):
    """Another connection held a database lock past the configured timeout."""

    def as_dict(self) -> dict:
        return {"error": "workspace_busy", "message": str(self), "retryable": True}


class ProposalMismatchError(WorkspaceError):
    """Acceptance did not identify the branch's sealed proposal."""

    def __init__(self, branch: str, expected: str, actual: str | None):
        self.branch, self.expected, self.actual = branch, expected, actual
        super().__init__(f"Reviewed proposal does not match branch {branch!r}")

    def as_dict(self) -> dict:
        return {"error": "proposal_mismatch", "branch": self.branch,
                "expected": self.expected, "actual": self.actual}


class ConflictError(WorkspaceError):
    """Publication requires rechecking; no shared state was changed."""

    def __init__(self, branch: str, base: int, current: int, conflicts: list[dict]):
        self.branch = branch
        self.base_revision = base
        self.current_revision = current
        self.conflicts = conflicts
        super().__init__(f"Branch {branch!r} requires rechecking ({len(conflicts)} conflicts)")

    def as_dict(self) -> dict:
        return {
            "error": "requires_recheck",
            "branch": self.branch,
            "base_revision": self.base_revision,
            "current_revision": self.current_revision,
            "conflicts": self.conflicts,
        }


def _path(path: str) -> str:
    if not isinstance(path, str) or not path or "\\" in path or ":" in path:
        raise InvalidPathError("Use a nonempty relative POSIX file path")
    if any(ord(c) < 32 or ord(c) == 127 for c in path):
        raise InvalidPathError("Control characters are not allowed in file paths")
    if any(part in ("", ".", "..") for part in path.split("/")):
        raise InvalidPathError("Absolute paths, empty components, '.' and '..' are not allowed")
    try:
        path.encode("utf-8")
    except UnicodeError as exc:
        raise InvalidPathError("Paths must be valid UTF-8") from exc
    return path


def _name(name: str) -> str:
    if not isinstance(name, str) or not name.strip() or len(name) > 200:
        raise WorkspaceError("Branch names must contain 1–200 characters")
    if any(ord(c) < 32 or ord(c) == 127 for c in name):
        raise WorkspaceError("Branch names cannot contain control characters")
    try:
        name.encode("utf-8")
    except UnicodeError as exc:
        raise WorkspaceError("Branch names must be valid UTF-8") from exc
    return name


def _timeout(value: float) -> float:
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or value < 0 or value > 2_147_483.647 or not math.isfinite(value)):
        raise ValueError("timeout must be finite and between 0 and 2147483.647 seconds")
    return float(value)


def _storage_error(exc: sqlite3.Error) -> WorkspaceError:
    code = getattr(exc, "sqlite_errorcode", 0) or 0
    if code & 0xFF in (5, 6) or (isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc).lower()):
        return WorkspaceBusyError(f"Database lock timeout: {exc}")
    return WorkspaceError(f"SQLite operation failed: {exc}")


def _publish_database(temporary: Path, destination: Path) -> None:
    """Publish a closed, checkpointed database without replacing any existing path."""
    with temporary.open("rb") as stream:
        os.fsync(stream.fileno())
    _refuse_sidecars(destination)
    os.link(temporary, destination)
    descriptor = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _refuse_sidecars(destination: Path) -> None:
    # An orphaned WAL can belong to an entirely different database. SQLite may
    # replay it against a new main file; never reuse or delete unknown sidecars.
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(str(destination) + suffix)
        if sidecar.exists() or sidecar.is_symlink():
            raise WorkspaceError(f"Database destination has an existing SQLite sidecar: {sidecar}")


def _remove_database(path: Path) -> None:
    for suffix in ("", "-wal", "-shm", "-journal"):
        Path(str(path) + suffix).unlink(missing_ok=True)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _listing_hash(paths: list[str] | set[str]) -> str:
    return _sha(json.dumps(sorted(paths), ensure_ascii=True).encode("ascii"))


def _collisions(paths: set[str]) -> list[tuple[str, str]]:
    result = []
    for path in sorted(paths):
        parts = path.split("/")
        for i in range(1, len(parts)):
            parent = "/".join(parts[:i])
            if parent in paths:
                result.append((parent, path))
    return result


def _signature(info: os.stat_result) -> tuple:
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def _capture(source: Path, directories: set[str] | None = None) -> dict[str, bytes]:
    """Capture a quiescent tree and reject observed changes; not an OS snapshot."""
    if not hasattr(os, "O_NOFOLLOW") or os.open not in os.supports_dir_fd:
        raise WorkspaceError("Safe directory capture currently requires macOS or Linux")
    flags = os.O_RDONLY | os.O_NOFOLLOW
    directory_flags = flags | os.O_DIRECTORY
    files: dict[str, bytes] = {}

    def walk(fd: int, prefix: str, signatures: dict, read: bool) -> None:
        signatures[prefix] = _signature(os.fstat(fd))
        with os.scandir(fd) as scan:
            names = sorted(entry.name for entry in scan)
        for name in names:
            path = _path(f"{prefix}/{name}" if prefix else name)
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                child = os.open(name, directory_flags, dir_fd=fd)
                try:
                    if _signature(os.fstat(child)) != _signature(info):
                        raise WorkspaceError(f"Source changed during capture: {path}")
                    walk(child, path, signatures, read)
                finally:
                    os.close(child)
            elif stat.S_ISREG(info.st_mode):
                # O_NONBLOCK avoids hanging if a regular file is raced into a FIFO.
                child = os.open(name, flags | os.O_NONBLOCK, dir_fd=fd)
                with os.fdopen(child, "rb") as stream:
                    before = os.fstat(stream.fileno())
                    if not stat.S_ISREG(before.st_mode) or _signature(before) != _signature(info):
                        raise WorkspaceError(f"Source changed during capture: {path}")
                    if read:
                        files[path] = stream.read()
                    after = os.fstat(stream.fileno())
                    if _signature(before) != _signature(after):
                        raise WorkspaceError(f"Source changed during capture: {path}")
                    signatures[path] = _signature(after)
            else:
                raise InvalidPathError(f"Only regular files and directories can be captured: {path}")
        if signatures[prefix] != _signature(os.fstat(fd)):
            raise WorkspaceError(f"Source directory changed during capture: {prefix or '.'}")

    root = os.open(source, directory_flags)
    try:
        first: dict = {}
        second: dict = {}
        walk(root, "", first, True)
        walk(root, "", second, False)
        if first != second or _signature(os.stat(source, follow_symlinks=False)) != first[""]:
            raise WorkspaceError("Source changed during capture; quiesce it and retry")
        if directories is not None:
            directories.update(path for path, signature in first.items()
                               if path and stat.S_ISDIR(signature[2]))
    finally:
        os.close(root)
    return files


_PROPOSALS_SCHEMA = """CREATE TABLE proposals (
    branch TEXT PRIMARY KEY REFERENCES branches(name),
    proposal_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);"""

_V3_STATEMENTS = (
    "CREATE TABLE dirs (revision INTEGER NOT NULL REFERENCES revisions(id), path TEXT NOT NULL, "
    "version INTEGER NOT NULL REFERENCES revisions(id), present INTEGER NOT NULL CHECK(present IN(0,1)), PRIMARY KEY(revision,path))",
    "CREATE TABLE dir_changes (branch TEXT NOT NULL REFERENCES branches(name), path TEXT NOT NULL, "
    "present INTEGER NOT NULL CHECK(present IN(0,1)), PRIMARY KEY(branch,path))",
    "CREATE TABLE dir_reads (branch TEXT NOT NULL REFERENCES branches(name), path TEXT NOT NULL, version INTEGER NOT NULL, PRIMARY KEY(branch,path))",
    "CREATE TABLE kv_entries (revision INTEGER NOT NULL REFERENCES revisions(id), key TEXT NOT NULL, "
    "version INTEGER NOT NULL REFERENCES revisions(id), value TEXT, PRIMARY KEY(revision,key))",
    "CREATE TABLE kv_changes (branch TEXT NOT NULL REFERENCES branches(name), key TEXT NOT NULL, value TEXT, PRIMARY KEY(branch,key))",
    "CREATE TABLE kv_reads (branch TEXT NOT NULL REFERENCES branches(name), key TEXT NOT NULL, version INTEGER NOT NULL, PRIMARY KEY(branch,key))",
    "CREATE TABLE kv_list_reads (branch TEXT NOT NULL REFERENCES branches(name), prefix TEXT NOT NULL, signature TEXT NOT NULL, PRIMARY KEY(branch,prefix))",
    "CREATE TABLE tool_calls (id INTEGER PRIMARY KEY AUTOINCREMENT, branch TEXT REFERENCES branches(name), "
    "name TEXT NOT NULL, parameters TEXT, status TEXT NOT NULL CHECK(status IN('pending','succeeded','failed')), "
    "result TEXT, error TEXT, started_at REAL NOT NULL, completed_at REAL, duration_ms REAL)",
    "CREATE TABLE tool_events (id INTEGER PRIMARY KEY AUTOINCREMENT, call_id INTEGER NOT NULL REFERENCES tool_calls(id), "
    "kind TEXT NOT NULL CHECK(kind IN('started','succeeded','failed')), at REAL NOT NULL, UNIQUE(call_id,kind))",
    "CREATE INDEX tool_calls_branch ON tool_calls(branch,id)",
    "CREATE INDEX tool_calls_name ON tool_calls(name,id)",
    "CREATE INDEX tool_events_call ON tool_events(call_id,id)",
    "ALTER TABLE branches ADD COLUMN listing_format INTEGER NOT NULL DEFAULT 2",
    "ALTER TABLE proposals ADD COLUMN format INTEGER NOT NULL DEFAULT 2",
)
_V3_TABLES = {"dirs", "dir_changes", "dir_reads", "kv_entries", "kv_changes", "kv_reads", "kv_list_reads", "tool_calls", "tool_events"}

_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE blobs (hash TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER REFERENCES revisions(id),
    branch TEXT,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE entries (
    revision INTEGER NOT NULL REFERENCES revisions(id),
    path TEXT NOT NULL,
    version INTEGER NOT NULL REFERENCES revisions(id),
    hash TEXT REFERENCES blobs(hash),
    PRIMARY KEY(revision, path)
);
CREATE TABLE branches (
    name TEXT PRIMARY KEY,
    base_revision INTEGER NOT NULL REFERENCES revisions(id),
    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','accepted')),
    accepted_revision INTEGER REFERENCES revisions(id),
    listing_hash TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE reads (
    branch TEXT NOT NULL REFERENCES branches(name),
    path TEXT NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY(branch, path)
);
CREATE TABLE changes (
    branch TEXT NOT NULL REFERENCES branches(name),
    path TEXT NOT NULL,
    hash TEXT REFERENCES blobs(hash),
    PRIMARY KEY(branch, path)
);
""" + _PROPOSALS_SCHEMA + ";\n" + ";\n".join(_V3_STATEMENTS) + ";"


class Workspace:
    """A local SQLite workspace. Objects hold no persistent database connection."""

    def __init__(self, db_path: str | Path, *, timeout: float = 30.0):
        self.db_path = Path(db_path).expanduser().resolve()
        self.timeout = _timeout(timeout)
        with self._transaction() as db:
            try:
                version = db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
            except sqlite3.DatabaseError as exc:
                raise WorkspaceError("Not an AgentSQL database") from exc
            if version is None or version[0] not in ("1", "2", "3"):
                raise WorkspaceError("Unsupported workspace schema version")
            old_schema = version[0] != "3"
            required = {"meta", "blobs", "revisions", "entries", "branches", "reads", "changes"}
            if version[0] != "1":
                required.add("proposals")
            if not old_schema:
                required.update(_V3_TABLES)
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not required.issubset(tables):
                raise WorkspaceError("Incomplete workspace schema")
            head = _head(db)
            if not db.execute("SELECT 1 FROM revisions WHERE id=?", (head,)).fetchone():
                raise WorkspaceError("Workspace head references a missing revision")
            if version[0] == "3":
                boundary = db.execute("SELECT value FROM meta WHERE key='v3_start_revision'").fetchone()
                try:
                    if boundary is None or not 1 <= int(boundary[0]) <= head:
                        raise ValueError()
                except (ValueError, TypeError) as exc:
                    raise WorkspaceError("Invalid schema migration boundary") from exc
        if old_schema:
            # Concurrent first opens recheck the version under the same write lock.
            with self._transaction(write=True) as db:
                if db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0] == "1":
                    db.execute(_PROPOSALS_SCHEMA)
                    db.execute("UPDATE meta SET value='2' WHERE key='schema_version'")
                if db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0] == "2":
                    for statement in _V3_STATEMENTS:
                        db.execute(statement)
                    known: dict[str, tuple[int, bool]] = {}
                    for row in db.execute("SELECT id FROM revisions ORDER BY id"):
                        revision = row[0]
                        inferred = {parent for path in _paths(db, revision) for parent in _parents(path)}
                        for directory in known.keys() | inferred:
                            present = directory in inferred
                            if directory not in known or known[directory][1] != present:
                                known[directory] = (revision, present)
                        db.executemany("INSERT INTO dirs VALUES (?,?,?,?)",
                                       [(revision, path, version, int(present)) for path, (version, present) in sorted(known.items())])
                    for branch in db.execute("SELECT * FROM branches WHERE status='open'"):
                        base_dirs = _dir_paths(db, branch["base_revision"])
                        needed = {parent for path in _overlay_paths(db, branch["name"], branch["base_revision"])
                                  for parent in _parents(path)}
                        db.executemany("INSERT INTO dir_changes VALUES (?,?,1)",
                                       [(branch["name"], parent) for parent in sorted(needed - base_dirs)])
                        db.executemany("INSERT INTO dir_changes VALUES (?,?,0)",
                                       [(branch["name"], parent) for parent in sorted(base_dirs - needed)])
                    db.execute("INSERT INTO meta VALUES ('v3_start_revision',?)", (str(_head(db)),))
                    db.execute("UPDATE meta SET value='3' WHERE key='schema_version'")
        with self._transaction() as db:
            required = {"meta", "blobs", "revisions", "entries", "branches", "reads", "changes", "proposals"}
            required.update(_V3_TABLES)
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not required.issubset(tables):
                raise WorkspaceError("Incomplete workspace schema")
            head = _head(db)
            if not db.execute("SELECT 1 FROM revisions WHERE id=?", (head,)).fetchone():
                raise WorkspaceError("Workspace head references a missing revision")

    @classmethod
    def create(cls, db_path: str | Path, source_dir: str | Path | None = None, *, timeout: float = 30.0) -> Workspace:
        """Import regular files. Source must be quiescent and outside the database."""
        timeout = _timeout(timeout)
        source = None
        if source_dir is not None:
            source = Path(source_dir).expanduser().absolute()
            if source.is_symlink():
                raise InvalidPathError("Source directory cannot be a symlink")
            source = source.resolve(strict=True)
            if not source.is_dir():
                raise WorkspaceError("Source must be a directory")
        target = Path(db_path).expanduser().absolute()
        if target.is_symlink():
            raise InvalidPathError("Database path cannot be a symlink")
        target = target.resolve()
        if source is not None and target.is_relative_to(source):
            raise WorkspaceError("Store the database outside the source directory")
        if target.exists():
            raise WorkspaceError(f"Database already exists: {target}")
        _refuse_sidecars(target)
        directories: set[str] = set()
        contents = _capture(source, directories) if source is not None else {}
        # Initialize privately, then publish one complete file with no-replace semantics.
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}-", dir=target.parent)
        temporary = Path(temporary_name)
        os.close(descriptor)
        db = None
        try:
            db = sqlite3.connect(str(temporary), timeout=timeout, isolation_level=None)
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.executescript("BEGIN IMMEDIATE;\n" + _SCHEMA)
            db.execute("INSERT INTO meta VALUES ('schema_version','3')")
            db.execute("INSERT INTO revisions(id,parent_id,branch,message) VALUES (1,NULL,NULL,?)",
                       ("Initial capture",))
            db.execute("INSERT INTO meta VALUES ('head_revision','1')")
            db.execute("INSERT INTO meta VALUES ('v3_start_revision','1')")
            for path, data in sorted(contents.items()):
                digest = _put_blob(db, data)
                db.execute("INSERT INTO entries VALUES (1,?,1,?)", (path, digest))
            db.executemany("INSERT INTO dirs VALUES (1,?,1,1)", [(path,) for path in sorted(directories)])
            db.commit()
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            db.close()
            db = None
            _publish_database(temporary, target)
        except sqlite3.Error as exc:
            raise _storage_error(exc) from exc
        finally:
            if db is not None:
                db.close()
            _remove_database(temporary)
        return cls(target, timeout=timeout)

    @classmethod
    def open(cls, db_path: str | Path, *, timeout: float = 30.0) -> Workspace:
        return cls(db_path, timeout=timeout)

    @contextmanager
    def _transaction(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        # mode=rw prevents a typo in open() from creating an empty database.
        db = None
        try:
            db = sqlite3.connect(self.db_path.as_uri() + "?mode=rw", uri=True,
                                 timeout=self.timeout, isolation_level=None)
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield db
            db.commit()
        except sqlite3.Error as exc:
            raise _storage_error(exc) from exc
        finally:
            if db is not None:
                # close() rolls back uncommitted writes after every exception.
                db.close()

    def branch(self, name: str, *, revision: int | None = None) -> Branch:
        name = _name(name)
        with self._transaction(write=True) as db:
            if db.execute("SELECT 1 FROM branches WHERE name=?", (name,)).fetchone():
                raise WorkspaceError(f"Branch already exists: {name}")
            base = _head(db) if revision is None else revision
            if isinstance(base, bool) or not isinstance(base, int) or not db.execute("SELECT 1 FROM revisions WHERE id=?", (base,)).fetchone():
                raise WorkspaceError(f"Unknown revision: {base}")
            db.execute("INSERT INTO branches(name,base_revision,listing_format) VALUES (?,?,3)", (name, base))
        return Branch(self, name)

    def session(self, name: str) -> Branch:
        """Create or reopen one named cooperative branch, atomically."""
        name = _name(name)
        with self._transaction(write=True) as db:
            db.execute("INSERT OR IGNORE INTO branches(name,base_revision,listing_format) VALUES (?,?,3)", (name, _head(db)))
        return Branch(self, name)

    def fork(self, source: str, name: str) -> Branch:
        """Fork staged state with its provenance; audit effects remain on the source."""
        source, name = _name(source), _name(name)
        with self._transaction(write=True) as db:
            original = _branch(db, source)
            if db.execute("SELECT 1 FROM branches WHERE name=?", (name,)).fetchone():
                raise WorkspaceError(f"Branch already exists: {name}")
            accepted = original["status"] == "accepted"
            base = original["accepted_revision"] if accepted else original["base_revision"]
            db.execute("INSERT INTO branches(name,base_revision,listing_hash,listing_format) VALUES (?,?,?,?)",
                       (name, base, None if accepted else original["listing_hash"], 3 if accepted else original["listing_format"]))
            if not accepted:
                for table, fields in (("reads", "path,version"), ("changes", "path,hash"),
                                      ("dir_reads", "path,version"), ("dir_changes", "path,present"),
                                      ("kv_reads", "key,version"), ("kv_changes", "key,value"),
                                      ("kv_list_reads", "prefix,signature")):
                    db.execute(f"INSERT INTO {table}(branch,{fields}) SELECT ?,{fields} FROM {table} WHERE branch=?", (name, source))
        return Branch(self, name)

    @property
    def tools(self):
        from .audit import ToolLog
        return ToolLog(self)

    def get_branch(self, name: str) -> Branch:
        name = _name(name)
        with self._transaction() as db:
            _branch(db, name)
        return Branch(self, name)

    def branches(self) -> list[dict]:
        with self._transaction() as db:
            return [dict(row) for row in db.execute(
                "SELECT name,base_revision,status,accepted_revision,created_at FROM branches ORDER BY name")]

    def snapshot(self, revision: int | None = None) -> Snapshot:
        with self._transaction() as db:
            if revision is None:
                revision = _head(db)
            if isinstance(revision, bool) or not isinstance(revision, int) or not db.execute(
                    "SELECT 1 FROM revisions WHERE id=?", (revision,)).fetchone():
                raise WorkspaceError(f"Unknown revision: {revision}")
        return Snapshot(self, revision)

    def history(self) -> list[dict]:
        with self._transaction() as db:
            result = []
            for row in db.execute("SELECT * FROM revisions ORDER BY id"):
                result.append({
                    "revision": row["id"], "parent_revision": row["parent_id"],
                    "branch": row["branch"], "message": row["message"],
                    "created_at": row["created_at"],
                    "paths": [entry[0] for entry in db.execute(
                        "SELECT path FROM entries WHERE revision=? AND version=? ORDER BY path",
                        (row["id"], row["id"]))],
                    "directories": [entry[0] for entry in db.execute("SELECT path FROM dirs WHERE revision=? AND version=? ORDER BY path", (row["id"], row["id"]))],
                    "keys": [entry[0] for entry in db.execute("SELECT key FROM kv_entries WHERE revision=? AND version=? ORDER BY key", (row["id"], row["id"]))],
                })
            return result

    def backup(self, destination: str | Path) -> Path:
        """Create a consistent private single-file backup, including staged work.

        SQLite's online backup API pins a read snapshot while other processes
        may publish. Existing paths are never replaced, including symlinks.
        """
        target = Path(destination).expanduser().absolute()
        if target.is_symlink() or target.exists():
            raise WorkspaceError(f"Backup destination already exists: {target}")
        target = target.resolve()
        _refuse_sidecars(target)
        descriptor, name = tempfile.mkstemp(prefix=f".{target.name}-", dir=target.parent)
        temporary = Path(name)
        os.close(descriptor)
        output = None
        deadline = time.monotonic() + self.timeout

        def progress(status: int, remaining: int, total: int) -> None:
            if status in (5, 6) and time.monotonic() >= deadline:
                raise WorkspaceBusyError("Database lock timeout during backup")

        try:
            output = sqlite3.connect(str(temporary), timeout=self.timeout, isolation_level=None)
            with self._transaction() as db:
                _head(db)  # Pin one source read snapshot before starting the copy.
                db.backup(output, pages=128, progress=progress, sleep=0.05)
            output.execute("PRAGMA journal_mode=DELETE")
            output.close()
            output = None
            _publish_database(temporary, target)
        except sqlite3.Error as exc:
            raise _storage_error(exc) from exc
        finally:
            if output is not None:
                output.close()
            _remove_database(temporary)
        return target

    def check_integrity(self) -> dict:
        """Audit storage and provenance in one read snapshot, without repairing it."""
        with self._transaction() as db:
            issues = [f"SQLite: {row[0]}" for row in db.execute("PRAGMA integrity_check") if row[0] != "ok"]
            issues.extend(f"Foreign key: {tuple(row)}" for row in db.execute("PRAGMA foreign_key_check"))
            head = _head(db)
            revisions = list(db.execute("SELECT id,parent_id,branch FROM revisions ORDER BY id"))
            previous = None
            for row in revisions:
                if row["parent_id"] != previous:
                    issues.append(f"Broken revision chain at {row['id']}")
                previous = row["id"]
                collisions = _collisions(_paths(db, row["id"]))
                if collisions:
                    issues.append(f"Path collision in revision {row['id']}: {collisions[0]}")
                if row["parent_id"] is not None:
                    accepted_branch = db.execute(
                        "SELECT accepted_revision,status FROM branches WHERE name=?", (row["branch"],)
                    ).fetchone()
                    if accepted_branch is None or accepted_branch[0] != row["id"] or accepted_branch[1] != "accepted":
                        issues.append(f"Revision branch provenance mismatch: {row['id']}")
                    # Compare the entire transition, including inherited files and
                    # tombstones, rather than only the entries still present.
                    for inherited in db.execute(
                        "SELECT p.path FROM entries p LEFT JOIN changes c ON c.branch=? AND c.path=p.path "
                        "LEFT JOIN entries e ON e.revision=? AND e.path=p.path "
                        "WHERE p.revision=? AND c.path IS NULL AND "
                        "(e.path IS NULL OR e.version<>p.version OR e.hash IS NOT p.hash)",
                        (row["branch"], row["id"], row["parent_id"]),
                    ):
                        issues.append(f"Inherited file mismatch: revision {row['id']}, path {inherited[0]}")
                    for changed in db.execute(
                        "SELECT c.path FROM changes c LEFT JOIN entries e ON e.revision=? AND e.path=c.path "
                        "WHERE c.branch=? AND (e.path IS NULL OR e.version<>? OR e.hash IS NOT c.hash)",
                        (row["id"], row["branch"], row["id"]),
                    ):
                        issues.append(f"Accepted change mismatch: revision {row['id']}, path {changed[0]}")
                    for extra in db.execute(
                        "SELECT e.path FROM entries e LEFT JOIN entries p ON p.revision=? AND p.path=e.path "
                        "LEFT JOIN changes c ON c.branch=? AND c.path=e.path "
                        "WHERE e.revision=? AND p.path IS NULL AND c.path IS NULL",
                        (row["parent_id"], row["branch"], row["id"]),
                    ):
                        issues.append(f"Unexpected file: revision {row['id']}, path {extra[0]}")
            if previous != head:
                issues.append("Head is not the latest revision")
            for row in db.execute("SELECT hash,data FROM blobs"):
                if _sha(bytes(row["data"])) != row["hash"]:
                    issues.append(f"Blob digest mismatch: {row['hash']}")
            from .files import validate_tree
            from .state import _encode, _key, _kv_entry, _kv_listing
            boundary = db.execute("SELECT value FROM meta WHERE key='v3_start_revision'").fetchone()
            try:
                v3_start = int(boundary[0]) if boundary is not None else 0
                if not 1 <= v3_start <= head:
                    raise ValueError()
            except (ValueError, TypeError) as exc:
                raise WorkspaceError("Invalid schema migration boundary") from exc
            for revision in revisions:
                number = revision["id"]
                try:
                    validate_tree(_paths(db, number), _dir_paths(db, number))
                except WorkspaceError as error:
                    issues.append(f"Invalid directory tree at revision {number}: {error}")
                if revision["parent_id"] is not None and number > v3_start:
                    for table, changes, key, value in (("dirs", "dir_changes", "path", "present"),
                                                       ("kv_entries", "kv_changes", "key", "value")):
                        parent_values = {entry[key]: (entry["version"], entry[value]) for entry in db.execute(f"SELECT * FROM {table} WHERE revision=?", (revision["parent_id"],))}
                        expected_values = dict(parent_values)
                        for change in db.execute(f"SELECT * FROM {changes} WHERE branch=?", (revision["branch"],)):
                            if table == "dirs" and change[value] and parent_values.get(change[key], (0, 0))[1]:
                                continue
                            expected_values[change[key]] = (number, change[value])
                        actual_values = {entry[key]: (entry["version"], entry[value]) for entry in db.execute(f"SELECT * FROM {table} WHERE revision=?", (number,))}
                        if expected_values != actual_values:
                            issues.append(f"Invalid {table} transition at revision {number}")
            for row in db.execute("SELECT key,value FROM kv_entries UNION SELECT key,value FROM kv_changes"):
                try:
                    _key(row[0])
                    if row[1] is not None:
                        _encode(json.loads(row[1]))
                except (ValueError, TypeError):
                    issues.append(f"Invalid JSON state: {row[0]!r}")
            for table, key, value in (("dirs", "path", "present"), ("kv_entries", "key", "value")):
                for row in db.execute(f"SELECT e.revision,e.{key} FROM {table} e LEFT JOIN {table} v ON v.revision=e.version AND v.{key}=e.{key} WHERE e.version>e.revision OR v.{key} IS NULL OR e.{value} IS NOT v.{value}"):
                    issues.append(f"Invalid {table} version: revision {row[0]}, key {row[1]}")
            for row in db.execute("SELECT e.revision,e.path FROM entries e LEFT JOIN entries v "
                                  "ON v.revision=e.version AND v.path=e.path "
                                  "WHERE e.version>e.revision OR v.path IS NULL OR e.hash IS NOT v.hash"):
                issues.append(f"Invalid file version: revision {row[0]}, path {row[1]}")
            for table in ("entries", "reads", "changes", "dirs", "dir_reads", "dir_changes"):
                for row in db.execute(f"SELECT DISTINCT path FROM {table}"):
                    try:
                        _path(row[0])
                    except InvalidPathError:
                        issues.append(f"Invalid path in {table}: {row[0]!r}")
            branches = list(db.execute("SELECT * FROM branches"))
            for branch in branches:
                name, base = branch["name"], branch["base_revision"]
                expected_listing = _tree_hash(db, base) if branch["listing_format"] >= 3 else _listing_hash(_paths(db, base))
                if branch["listing_hash"] is not None and branch["listing_hash"] != expected_listing:
                    issues.append(f"Listing provenance mismatch: {name}")
                for read in db.execute("SELECT path,version FROM reads WHERE branch=?", (name,)):
                    if _entry(db, base, read["path"])[0] != read["version"]:
                        issues.append(f"Read provenance mismatch: {name}, {read['path']}")
                for read in db.execute("SELECT path,version FROM dir_reads WHERE branch=?", (name,)):
                    if _dir_entry(db, base, read["path"])[0] != read["version"]:
                        issues.append(f"Directory provenance mismatch: {name}, {read['path']}")
                for read in db.execute("SELECT key,version FROM kv_reads WHERE branch=?", (name,)):
                    if _kv_entry(db, base, read["key"])[0] != read["version"]:
                        issues.append(f"State provenance mismatch: {name}, {read['key']}")
                for listing in db.execute("SELECT prefix,signature FROM kv_list_reads WHERE branch=?", (name,)):
                    if _kv_listing(db, base, listing["prefix"]) != listing["signature"]:
                        issues.append(f"State listing mismatch: {name}, {listing['prefix']}")
                proposal = db.execute("SELECT proposal_id FROM proposals WHERE branch=?", (name,)).fetchone()
                if proposal and proposal[0] != _proposal_id(db, branch):
                    issues.append(f"Sealed proposal mismatch: {name}")
                if branch["status"] == "accepted":
                    accepted = db.execute("SELECT branch FROM revisions WHERE id=?", (branch["accepted_revision"],)).fetchone()
                    if accepted is None or accepted[0] != name:
                        issues.append(f"Accepted branch provenance mismatch: {name}")
            for call in db.execute("SELECT * FROM tool_calls"):
                events = list(db.execute("SELECT kind,at FROM tool_events WHERE call_id=? ORDER BY id", (call["id"],)))
                if not events or events[0][0] != "started" or events[0][1] != call["started_at"]:
                    issues.append(f"Missing tool start event: {call['id']}")
                if call["status"] == "pending":
                    if len(events) != 1 or call["completed_at"] is not None:
                        issues.append(f"Invalid pending tool call: {call['id']}")
                else:
                    try:
                        completed, started, duration = float(call["completed_at"]), float(call["started_at"]), float(call["duration_ms"])
                        valid = (len(events) == 2 and events[-1][0] == call["status"] and events[-1][1] == call["completed_at"]
                                 and all(math.isfinite(value) for value in (completed, started, duration))
                                 and completed >= started and abs(duration - (completed - started) * 1000) <= .01)
                    except (ValueError, TypeError, OverflowError):
                        valid = False
                    if not valid:
                        issues.append(f"Invalid tool completion: {call['id']}")
                for field in ("parameters", "result"):
                    if call[field] is not None:
                        try:
                            _encode(json.loads(call[field]))
                        except (ValueError, TypeError):
                            issues.append(f"Invalid tool {field}: {call['id']}")
            return {"ok": not issues, "schema_version": "3", "head_revision": head,
                    "revisions": len(revisions), "branches": len(branches),
                    "blobs": db.execute("SELECT COUNT(*) FROM blobs").fetchone()[0], "issues": issues}


def _put_blob(db: sqlite3.Connection, data: bytes) -> str:
    digest = _sha(data)
    db.execute("INSERT OR IGNORE INTO blobs(hash,data) VALUES (?,?)", (digest, data))
    return digest


def _head(db: sqlite3.Connection) -> int:
    row = db.execute("SELECT value FROM meta WHERE key='head_revision'").fetchone()
    try:
        if row is None:
            raise ValueError()
        return int(row[0])
    except (ValueError, TypeError) as exc:
        raise WorkspaceError("Invalid workspace head metadata") from exc


def _branch(db: sqlite3.Connection, name: str, *, require_open: bool = False, allow_sealed: bool = False) -> sqlite3.Row:
    row = db.execute("SELECT * FROM branches WHERE name=?", (name,)).fetchone()
    if row is None:
        raise WorkspaceError(f"Unknown branch: {name}")
    if require_open and row["status"] != "open":
        raise BranchClosedError(f"Branch {name!r} is already accepted; create a new branch")
    if require_open and not allow_sealed and db.execute("SELECT 1 FROM proposals WHERE branch=?", (name,)).fetchone():
        raise BranchClosedError(f"Branch {name!r} is sealed; create a new branch for further work")
    return row


def _proposal_id(db: sqlite3.Connection, branch: sqlite3.Row) -> str:
    name = branch["name"]
    payload = {"branch": name, "base_revision": branch["base_revision"],
               "listing_hash": branch["listing_hash"],
               "reads": [tuple(row) for row in db.execute("SELECT path,version FROM reads WHERE branch=? ORDER BY path", (name,))],
               "changes": [tuple(row) for row in db.execute("SELECT path,hash FROM changes WHERE branch=? ORDER BY path", (name,))]}
    stored = db.execute("SELECT format FROM proposals WHERE branch=?", (name,)).fetchone()
    if stored is None or stored[0] >= 3:
        payload["listing_format"] = branch["listing_format"]
        for table, fields in (("dir_reads", "path,version"), ("dir_changes", "path,present"),
                              ("kv_reads", "key,version"), ("kv_changes", "key,value"),
                              ("kv_list_reads", "prefix,signature")):
            payload[table] = [tuple(row) for row in db.execute(f"SELECT {fields} FROM {table} WHERE branch=? ORDER BY 1", (name,))]
    return _sha(json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("ascii"))


def _entry(db: sqlite3.Connection, revision: int, path: str) -> tuple[int, str | None]:
    row = db.execute("SELECT version,hash FROM entries WHERE revision=? AND path=?", (revision, path)).fetchone()
    return (row["version"], row["hash"]) if row else (0, None)


def _paths(db: sqlite3.Connection, revision: int) -> set[str]:
    return {row[0] for row in db.execute(
        "SELECT path FROM entries WHERE revision=? AND hash IS NOT NULL", (revision,))}


def _parents(path: str) -> list[str]:
    parts = path.split("/")
    return ["/".join(parts[:index]) for index in range(1, len(parts))]


def _dir_entry(db: sqlite3.Connection, revision: int, path: str) -> tuple[int, bool]:
    row = db.execute("SELECT version,present FROM dirs WHERE revision=? AND path=?", (revision, path)).fetchone()
    return (row[0], bool(row[1])) if row else (0, False)


def _dir_paths(db: sqlite3.Connection, revision: int) -> set[str]:
    return {row[0] for row in db.execute("SELECT path FROM dirs WHERE revision=? AND present=1", (revision,))}


def _overlay_dirs(db: sqlite3.Connection, branch: str, revision: int) -> set[str]:
    paths = _dir_paths(db, revision)
    for row in db.execute("SELECT path,present FROM dir_changes WHERE branch=?", (branch,)):
        if row["present"]:
            paths.add(row["path"])
        else:
            paths.discard(row["path"])
    return paths


def _tree_hash(db: sqlite3.Connection, revision: int) -> str:
    return _sha(json.dumps({"files": sorted(_paths(db, revision)), "dirs": sorted(_dir_paths(db, revision))},
                           sort_keys=True, separators=(",", ":")).encode("utf-8"))


def _track_listing(db: sqlite3.Connection, branch: sqlite3.Row) -> None:
    db.execute("UPDATE branches SET listing_hash=?,listing_format=3 WHERE name=?",
               (_tree_hash(db, branch["base_revision"]), branch["name"]))


def _ensure_parents(db: sqlite3.Connection, branch: sqlite3.Row, path: str) -> None:
    base, name = branch["base_revision"], branch["name"]
    files, directories = _overlay_paths(db, name, base), _overlay_dirs(db, name, base)
    for parent in _parents(path):
        if parent in files:
            raise InvalidPathError(f"Parent path is a file: {parent}")
        if parent not in directories:
            db.execute("INSERT INTO dir_changes VALUES (?,?,1) ON CONFLICT(branch,path) DO UPDATE SET present=1", (name, parent))


def _overlay_paths(db: sqlite3.Connection, branch: str, revision: int) -> set[str]:
    paths = _paths(db, revision)
    for row in db.execute("SELECT path,hash FROM changes WHERE branch=?", (branch,)):
        if row["hash"] is None:
            paths.discard(row["path"])
        else:
            paths.add(row["path"])
    return paths


class Snapshot:
    """Immutable revision for inspection. Reads here do not track branch inputs."""

    def __init__(self, workspace: Workspace, revision: int):
        self._workspace = workspace
        self.revision = revision

    @property
    def fs(self):
        from .files import FileSystem
        return FileSystem(self)

    @property
    def kv(self):
        from .state import KeyValue
        return KeyValue(self)

    def read_bytes(self, path: str) -> bytes:
        path = _path(path)
        with self._workspace._transaction() as db:
            _, digest = _entry(db, self.revision, path)
            if digest is None:
                raise FileNotFoundError(path)
            return bytes(db.execute("SELECT data FROM blobs WHERE hash=?", (digest,)).fetchone()[0])

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def exists(self, path: str) -> bool:
        with self._workspace._transaction() as db:
            return _entry(db, self.revision, _path(path))[1] is not None

    def version(self, path: str) -> int:
        """Last write revision, including tombstones; never-seen paths return 0."""
        with self._workspace._transaction() as db:
            return _entry(db, self.revision, _path(path))[0]

    def list_files(self) -> list[str]:
        with self._workspace._transaction() as db:
            return sorted(_paths(db, self.revision))


class Branch:
    """A fixed input snapshot plus pending writes and durable read dependencies."""

    def __init__(self, workspace: Workspace, name: str):
        self._workspace = workspace
        self.name = name

    @property
    def fs(self):
        from .files import FileSystem
        return FileSystem(self)

    @property
    def kv(self):
        from .state import KeyValue
        return KeyValue(self)

    @property
    def tools(self):
        from .audit import ToolLog
        return ToolLog(self._workspace, self.name)

    @property
    def base_revision(self) -> int:
        with self._workspace._transaction() as db:
            return int(_branch(db, self.name)["base_revision"])

    def _tracked_read(self, path: str) -> bytes | None:
        path = _path(path)
        with self._workspace._transaction(write=True) as db:
            branch = _branch(db, self.name, require_open=True)
            pending = db.execute("SELECT hash FROM changes WHERE branch=? AND path=?", (self.name, path)).fetchone()
            if pending is not None:
                digest = pending["hash"]
            else:
                version, digest = _entry(db, branch["base_revision"], path)
                db.execute("INSERT OR IGNORE INTO reads(branch,path,version) VALUES (?,?,?)",
                           (self.name, path, version))
            data = None if digest is None else bytes(db.execute(
                "SELECT data FROM blobs WHERE hash=?", (digest,)).fetchone()[0])
        return data

    def read_bytes(self, path: str) -> bytes:
        data = self._tracked_read(path)
        # Raise after committing the missing-file dependency.
        if data is None:
            raise FileNotFoundError(path)
        return data

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def exists(self, path: str) -> bool:
        return self._tracked_read(path) is not None

    def list_files(self) -> list[str]:
        with self._workspace._transaction(write=True) as db:
            branch = _branch(db, self.name, require_open=True)
            base = branch["base_revision"]
            _track_listing(db, branch)
            return sorted(_overlay_paths(db, self.name, base))

    def write_bytes(self, path: str, data: bytes) -> None:
        path = _path(path)
        if not isinstance(data, bytes):
            raise TypeError("write_bytes expects bytes")
        with self._workspace._transaction(write=True) as db:
            branch = _branch(db, self.name, require_open=True)
            if path in _overlay_dirs(db, self.name, branch["base_revision"]):
                raise InvalidPathError(f"File path is an existing directory: {path}")
            _ensure_parents(db, branch, path)
            paths = _overlay_paths(db, self.name, branch["base_revision"])
            paths.add(path)
            collisions = _collisions(paths)
            if collisions:
                raise InvalidPathError(f"File/directory path collision: {collisions[0]}")
            digest = _put_blob(db, data)
            db.execute("INSERT INTO changes(branch,path,hash) VALUES (?,?,?) "
                       "ON CONFLICT(branch,path) DO UPDATE SET hash=excluded.hash", (self.name, path, digest))

    def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        self.write_bytes(path, text.encode(encoding))

    def delete(self, path: str) -> None:
        path = _path(path)
        missing = False
        with self._workspace._transaction(write=True) as db:
            branch = _branch(db, self.name, require_open=True)
            if path not in _overlay_paths(db, self.name, branch["base_revision"]):
                missing = True
                if not db.execute("SELECT 1 FROM changes WHERE branch=? AND path=?", (self.name, path)).fetchone():
                    db.execute("INSERT OR IGNORE INTO reads VALUES (?,?,?)", (self.name, path, _entry(db, branch["base_revision"], path)[0]))
            else:
                db.execute("INSERT INTO changes(branch,path,hash) VALUES (?,?,NULL) "
                           "ON CONFLICT(branch,path) DO UPDATE SET hash=NULL", (self.name, path))
        if missing:
            raise FileNotFoundError(path)

    def inspect(self) -> dict:
        """Inspect recorded provenance without adding dependencies."""
        with self._workspace._transaction() as db:
            branch = _branch(db, self.name)
            proposal = db.execute("SELECT proposal_id FROM proposals WHERE branch=?", (self.name,)).fetchone()
            changes = []
            for row in db.execute("SELECT path,hash FROM changes WHERE branch=? ORDER BY path", (self.name,)):
                version, before = _entry(db, branch["base_revision"], row["path"])
                changes.append({
                    "path": row["path"],
                    "kind": "delete" if row["hash"] is None else ("add" if before is None else "modify"),
                    "base_version": version, "base_sha256": before, "proposed_sha256": row["hash"],
                })
            return {
                "name": self.name, "status": branch["status"],
                "sealed": proposal is not None, "proposal_id": proposal[0] if proposal else None,
                "base_revision": branch["base_revision"], "accepted_revision": branch["accepted_revision"],
                "reads": [dict(row) for row in db.execute(
                    "SELECT path,version FROM reads WHERE branch=? ORDER BY path", (self.name,))],
                "tracks_listing": branch["listing_hash"] is not None,
                "listing_hash": branch["listing_hash"], "changes": changes,
                "directories": [dict(row) for row in db.execute("SELECT path,present FROM dir_changes WHERE branch=? ORDER BY path", (self.name,))],
                "directory_reads": [dict(row) for row in db.execute("SELECT path,version FROM dir_reads WHERE branch=? ORDER BY path", (self.name,))],
                "kv_reads": [dict(row) for row in db.execute("SELECT key,version FROM kv_reads WHERE branch=? ORDER BY key", (self.name,))],
                "kv_changes": [{"key": row["key"], "value": json.loads(row["value"]) if row["value"] is not None else None,
                                "deleted": row["value"] is None} for row in db.execute("SELECT key,value FROM kv_changes WHERE branch=? ORDER BY key", (self.name,))],
                "kv_list_reads": [dict(row) for row in db.execute("SELECT prefix,signature FROM kv_list_reads WHERE branch=? ORDER BY prefix", (self.name,))],
            }

    def diff(self) -> str:
        """Text diff against the branch's fixed base, or hashes for binary files."""
        with self._workspace._transaction() as db:
            branch = _branch(db, self.name)
            output = []
            for row in db.execute("SELECT path,hash FROM changes WHERE branch=? ORDER BY path", (self.name,)):
                path, after = row["path"], row["hash"]
                _, before = _entry(db, branch["base_revision"], path)
                def content(digest: str | None) -> bytes:
                    return b"" if digest is None else bytes(db.execute(
                        "SELECT data FROM blobs WHERE hash=?", (digest,)).fetchone()[0])
                left, right = content(before), content(after)
                header = f"diff --agentsql a/{path} b/{path}\n"
                output.append(header)
                try:
                    if b"\0" in left or b"\0" in right:
                        raise UnicodeError()
                    a, b = left.decode("utf-8"), right.decode("utf-8")
                except UnicodeError:
                    output.append(f"Binary content: {before or '(absent)'} -> {after or '(absent)'}\n")
                    continue
                lines = list(difflib.unified_diff(a.splitlines(keepends=True), b.splitlines(keepends=True),
                    fromfile=f"a/{path}" if before else "/dev/null",
                    tofile=f"b/{path}" if after else "/dev/null"))
                if not lines:
                    output.append(f"Recorded write: {before or '(absent)'} -> {after or '(absent)'}\n")
                for line in lines:
                    output.append(line if line.endswith("\n") else line + "\n\\ No newline at end of file\n")
            for row in db.execute("SELECT path,present FROM dir_changes WHERE branch=? ORDER BY path", (self.name,)):
                output.append(f"{'mkdir' if row['present'] else 'rmdir'} {row['path']}\n")
            for row in db.execute("SELECT key,value FROM kv_changes WHERE branch=? ORDER BY key", (self.name,)):
                output.append(f"kv {'delete' if row['value'] is None else 'set'} {json.dumps(row['key'])}: {row['value']}\n")
            return "".join(output)

    def seal(self) -> str:
        """Freeze this proposal's writes and read set before review; return its ID."""
        with self._workspace._transaction(write=True) as db:
            branch = _branch(db, self.name, require_open=True, allow_sealed=True)
            proposal = db.execute("SELECT proposal_id FROM proposals WHERE branch=?", (self.name,)).fetchone()
            if proposal:
                return str(proposal[0])
            if not any(db.execute(f"SELECT 1 FROM {table} WHERE branch=?", (self.name,)).fetchone()
                       for table in ("changes", "dir_changes", "kv_changes")):
                raise WorkspaceError("Cannot seal an empty change set")
            proposal_id = _proposal_id(db, branch)
            db.execute("INSERT INTO proposals(branch,proposal_id,format) VALUES (?,?,3)", (self.name, proposal_id))
            return proposal_id

    def accept(self, message: str = "", *, expected_proposal: str | None = None) -> dict:
        """Atomically validate every dependency and publish the entire change set.

        No selected-path API is offered: reads are branch-wide, and outputs may
        depend on other pending outputs. A failed check leaves the branch open.
        """
        if not isinstance(message, str):
            raise TypeError("message must be a string")
        if expected_proposal is not None and not isinstance(expected_proposal, str):
            raise TypeError("expected_proposal must be a string")
        with self._workspace._transaction(write=True) as db:
            branch = _branch(db, self.name, require_open=True, allow_sealed=True)
            if expected_proposal is not None:
                proposal = db.execute("SELECT proposal_id FROM proposals WHERE branch=?", (self.name,)).fetchone()
                actual_proposal = str(proposal[0]) if proposal else None
                if actual_proposal != expected_proposal:
                    raise ProposalMismatchError(self.name, expected_proposal, actual_proposal)
            base, current = branch["base_revision"], _head(db)
            changes = list(db.execute("SELECT path,hash FROM changes WHERE branch=? ORDER BY path", (self.name,)))
            directories = list(db.execute("SELECT path,present FROM dir_changes WHERE branch=? ORDER BY path", (self.name,)))
            states = list(db.execute("SELECT key,value FROM kv_changes WHERE branch=? ORDER BY key", (self.name,)))
            if not changes and not directories and not states:
                raise WorkspaceError("Cannot accept an empty change set")
            conflicts = []
            for read in db.execute("SELECT path,version FROM reads WHERE branch=? ORDER BY path", (self.name,)):
                actual, _ = _entry(db, current, read["path"])
                if actual != read["version"]:
                    conflicts.append({"kind": "stale_read", "path": read["path"],
                                      "expected": read["version"], "actual": actual})
            for change in changes:
                expected, _ = _entry(db, base, change["path"])
                actual, _ = _entry(db, current, change["path"])
                if expected != actual:
                    conflicts.append({"kind": "conflicting_write", "path": change["path"],
                                      "expected": expected, "actual": actual})
            if branch["listing_hash"] is not None:
                actual_listing = _tree_hash(db, current) if branch["listing_format"] >= 3 else _listing_hash(_paths(db, current))
                if branch["listing_hash"] != actual_listing:
                    conflicts.append({"kind": "stale_listing", "path": "*",
                                      "expected": branch["listing_hash"], "actual": actual_listing})
            for read in db.execute("SELECT path,version FROM dir_reads WHERE branch=? ORDER BY path", (self.name,)):
                actual, _ = _dir_entry(db, current, read["path"])
                if actual != read["version"]:
                    conflicts.append({"kind": "stale_read", "resource": "directory", "path": read["path"], "expected": read["version"], "actual": actual})
            for directory in directories:
                expected, was_present = _dir_entry(db, base, directory["path"])
                actual, present = _dir_entry(db, current, directory["path"])
                if expected != actual and not (directory["present"] and present and not was_present):
                    conflicts.append({"kind": "conflicting_write", "resource": "directory", "path": directory["path"], "expected": expected, "actual": actual})
            from .state import _kv_entry, _kv_listing
            for read in db.execute("SELECT key,version FROM kv_reads WHERE branch=? ORDER BY key", (self.name,)):
                actual, _ = _kv_entry(db, current, read["key"])
                if actual != read["version"]:
                    conflicts.append({"kind": "stale_read", "resource": "kv", "path": read["key"], "expected": read["version"], "actual": actual})
            for state in states:
                expected, _ = _kv_entry(db, base, state["key"])
                actual, _ = _kv_entry(db, current, state["key"])
                if expected != actual:
                    conflicts.append({"kind": "conflicting_write", "resource": "kv", "path": state["key"], "expected": expected, "actual": actual})
            for listing in db.execute("SELECT prefix,signature FROM kv_list_reads WHERE branch=?", (self.name,)):
                actual_keys = _kv_listing(db, current, listing["prefix"])
                if actual_keys != listing["signature"]:
                    conflicts.append({"kind": "stale_listing", "resource": "kv", "path": listing["prefix"], "expected": listing["signature"], "actual": actual_keys})
            merged = _overlay_paths(db, self.name, current)
            for parent, child in _collisions(merged):
                conflicts.append({"kind": "path_collision", "path": parent,
                                  "expected": "no file/directory collision", "actual": child})
            from .files import validate_tree
            try:
                validate_tree(merged, _overlay_dirs(db, self.name, current))
            except WorkspaceError as error:
                conflicts.append({"kind": "path_collision", "path": "*", "expected": "valid directory tree", "actual": str(error)})
            if conflicts:
                raise ConflictError(self.name, base, current, conflicts)
            cursor = db.execute("INSERT INTO revisions(parent_id,branch,message) VALUES (?,?,?)",
                                (current, self.name, message))
            revision = cursor.lastrowid
            db.execute("INSERT INTO entries(revision,path,version,hash) "
                       "SELECT ?,path,version,hash FROM entries WHERE revision=?", (revision, current))
            for change in changes:
                db.execute("INSERT INTO entries(revision,path,version,hash) VALUES (?,?,?,?) "
                           "ON CONFLICT(revision,path) DO UPDATE SET version=excluded.version,hash=excluded.hash",
                           (revision, change["path"], revision, change["hash"]))
            db.execute("INSERT INTO dirs SELECT ?,path,version,present FROM dirs WHERE revision=?", (revision, current))
            for directory in directories:
                if directory["present"] and _dir_entry(db, current, directory["path"])[1]:
                    continue
                db.execute("INSERT INTO dirs VALUES (?,?,?,?) ON CONFLICT(revision,path) DO UPDATE SET version=excluded.version,present=excluded.present",
                           (revision, directory["path"], revision, directory["present"]))
            db.execute("INSERT INTO kv_entries SELECT ?,key,version,value FROM kv_entries WHERE revision=?", (revision, current))
            for state in states:
                db.execute("INSERT INTO kv_entries VALUES (?,?,?,?) ON CONFLICT(revision,key) DO UPDATE SET version=excluded.version,value=excluded.value",
                           (revision, state["key"], revision, state["value"]))
            db.execute("UPDATE meta SET value=? WHERE key='head_revision'", (str(revision),))
            db.execute("UPDATE branches SET status='accepted',accepted_revision=? WHERE name=?", (revision, self.name))
            return {"revision": revision, "branch": self.name, "paths": [row["path"] for row in changes],
                    "directories": [row["path"] for row in directories], "keys": [row["key"] for row in states]}

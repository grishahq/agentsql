"""A versioned file-tree API, including empty directories and atomic tree edits.

These are workspace paths, not paths on the host filesystem. There are no
permissions, links, devices, or implicit interception of external file access.
"""

from __future__ import annotations

import errno
import json
import sqlite3

from .core import (
    Branch,
    InvalidPathError,
    Snapshot,
    WorkspaceError,
    _branch,
    _dir_entry,
    _dir_paths,
    _entry,
    _overlay_dirs,
    _overlay_paths,
    _path,
    _paths,
    _put_blob,
    _sha,
    _track_listing,
)


def _fs_path(path: str) -> str:
    """Accept the two root spellings; otherwise require a portable relative path."""
    return "" if path in ("", "/") else _path(path)


def _parents(path: str) -> list[str]:
    parts = path.split("/")
    return ["/".join(parts[:index]) for index in range(1, len(parts))]


def validate_tree(files: set[str], directories: set[str]) -> None:
    """Reject file/directory collisions and directories without their parents.

    The root is implicit and must not appear in either set. All nonroot parent
    directories are explicit, so an empty directory is a first-class versioned
    object and can be preserved by capture, publication, and subprocess import.
    """
    for path in sorted(files | directories):
        _path(path)
        if path in files and path in directories:
            raise InvalidPathError(f"Path is both a file and a directory: {path}")
        for parent in _parents(path):
            if parent in files:
                raise InvalidPathError(f"File/directory path collision: {parent}, {path}")
            if parent not in directories:
                raise InvalidPathError(f"Missing parent directory: {parent}")


def _visible_digest(db: sqlite3.Connection, branch: sqlite3.Row | None,
                    revision: int, path: str) -> str | None:
    if branch is not None:
        pending = db.execute("SELECT hash FROM changes WHERE branch=? AND path=?",
                             (branch["name"], path)).fetchone()
        if pending is not None:
            return pending[0]
    return _entry(db, revision, path)[1]


def _track_file(db: sqlite3.Connection, branch: sqlite3.Row | None, path: str) -> None:
    if branch is not None and not db.execute(
        "SELECT 1 FROM changes WHERE branch=? AND path=?", (branch["name"], path)
    ).fetchone():
        db.execute("INSERT OR IGNORE INTO reads(branch,path,version) VALUES (?,?,?)",
                   (branch["name"], path, _entry(db, branch["base_revision"], path)[0]))


def _track_dir(db: sqlite3.Connection, branch: sqlite3.Row | None, path: str) -> None:
    if path and branch is not None and not db.execute(
        "SELECT 1 FROM dir_changes WHERE branch=? AND path=?", (branch["name"], path)
    ).fetchone():
        db.execute("INSERT OR IGNORE INTO dir_reads(branch,path,version) VALUES (?,?,?)",
                   (branch["name"], path,
                    _dir_entry(db, branch["base_revision"], path)[0]))


def _track_path(db: sqlite3.Connection, branch: sqlite3.Row | None, path: str) -> None:
    if path:
        _track_file(db, branch, path)
        _track_dir(db, branch, path)


def _file_change(db: sqlite3.Connection, branch: str, path: str, digest: str | None) -> None:
    db.execute("INSERT INTO changes(branch,path,hash) VALUES (?,?,?) "
               "ON CONFLICT(branch,path) DO UPDATE SET hash=excluded.hash",
               (branch, path, digest))


def _dir_change(db: sqlite3.Connection, branch: str, path: str, present: bool) -> None:
    db.execute("INSERT INTO dir_changes(branch,path,present) VALUES (?,?,?) "
               "ON CONFLICT(branch,path) DO UPDATE SET present=excluded.present",
               (branch, path, int(present)))


def _token(files: dict[str, str], directories: set[str]) -> str:
    payload = {"files": sorted(files.items()), "directories": sorted(directories)}
    return _sha(json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("ascii"))


def _stage_tree(db: sqlite3.Connection, branch: sqlite3.Row, before: dict[str, str],
                before_dirs: set[str], after: dict[str, str], after_dirs: set[str]) -> None:
    """Record the difference between two visible trees without publishing either."""
    validate_tree(set(after), after_dirs)
    for path in sorted(before.keys() | after.keys()):
        if before.get(path) != after.get(path):
            _track_path(db, branch, path)
            _file_change(db, branch["name"], path, after.get(path))
    for path in sorted(before_dirs ^ after_dirs):
        _track_path(db, branch, path)
        _dir_change(db, branch["name"], path, path in after_dirs)


class FileSystem:
    """File-tree facade for a branch or an immutable snapshot.

    Branch reads record the versions actually observed. A directory listing
    tracks the entire base tree's name/type membership conservatively; it does
    not claim to know which returned names influenced an agent's answer.
    Snapshot reads do not create dependencies. Mutators require an open,
    unsealed branch and each complete in one SQLite transaction.
    """

    def __init__(self, owner: Snapshot | Branch):
        if not isinstance(owner, (Snapshot, Branch)):
            raise TypeError("FileSystem requires a Snapshot or Branch")
        self._owner = owner
        self._workspace = owner._workspace

    def _state(self, db: sqlite3.Connection, *, mutate: bool = False
               ) -> tuple[sqlite3.Row | None, int, set[str], set[str]]:
        if isinstance(self._owner, Branch):
            branch = _branch(db, self._owner.name, require_open=True)
            revision = int(branch["base_revision"])
            return (branch, revision, _overlay_paths(db, branch["name"], revision),
                    _overlay_dirs(db, branch["name"], revision))
        if mutate:
            raise WorkspaceError("Snapshots are immutable; create a branch to edit files")
        revision = self._owner.revision
        return None, revision, _paths(db, revision), _dir_paths(db, revision)

    def _write_transaction(self) -> bool:
        return isinstance(self._owner, Branch)

    def read_file(self, path: str, encoding: str | None = "utf-8") -> str | bytes:
        """Read text, or bytes with encoding=None, while tracking a branch input."""
        path = _fs_path(path)
        data = None
        with self._workspace._transaction(write=self._write_transaction()) as db:
            branch, revision, files, directories = self._state(db)
            _track_path(db, branch, path)
            is_directory = not path or path in directories
            if path in files:
                digest = _visible_digest(db, branch, revision, path)
                data = bytes(db.execute("SELECT data FROM blobs WHERE hash=?", (digest,)).fetchone()[0])
        # Commit absence and type dependencies even when a read raises.
        if is_directory:
            raise IsADirectoryError(path or "/")
        if data is None:
            raise FileNotFoundError(path)
        return data if encoding is None else data.decode(encoding)

    def write_file(self, path: str, data: str | bytes, encoding: str = "utf-8") -> None:
        """Stage one file, creating parent directories as needed."""
        if not isinstance(self._owner, Branch):
            raise WorkspaceError("Snapshots are immutable; create a branch to edit files")
        path = _fs_path(path)
        if not path:
            raise IsADirectoryError("/")
        if isinstance(data, str):
            data = data.encode(encoding)
        if not isinstance(data, bytes):
            raise TypeError("write_file expects str or bytes")
        self._owner.write_bytes(path, data)

    def stat(self, path: str) -> dict:
        """Return type, byte size, version, and a file's SHA256 digest.

        Pending objects use version=None and staged=True. Directory sizes are
        zero. The root is implicit, always exists, and has version 1.
        """
        path = _fs_path(path)
        result = None
        with self._workspace._transaction(write=self._write_transaction()) as db:
            branch, revision, files, directories = self._state(db)
            _track_path(db, branch, path)
            if not path:
                result = {"path": "", "type": "dir", "size": 0, "version": 1, "staged": False}
            elif path in directories:
                staged = branch is not None and bool(db.execute(
                    "SELECT 1 FROM dir_changes WHERE branch=? AND path=?", (branch["name"], path)
                ).fetchone())
                result = {"path": path, "type": "dir", "size": 0,
                          "version": None if staged else _dir_entry(db, revision, path)[0],
                          "staged": staged}
            elif path in files:
                digest = _visible_digest(db, branch, revision, path)
                staged = branch is not None and bool(db.execute(
                    "SELECT 1 FROM changes WHERE branch=? AND path=?", (branch["name"], path)
                ).fetchone())
                result = {"path": path, "type": "file",
                          "size": db.execute("SELECT length(data) FROM blobs WHERE hash=?",
                                             (digest,)).fetchone()[0],
                          "version": None if staged else _entry(db, revision, path)[0],
                          "sha256": digest, "staged": staged}
        if result is None:
            raise FileNotFoundError(path)
        return result

    def exists(self, path: str) -> bool:
        path = _fs_path(path)
        with self._workspace._transaction(write=self._write_transaction()) as db:
            branch, _, files, directories = self._state(db)
            _track_path(db, branch, path)
            return not path or path in files or path in directories

    def readdir(self, path: str = "") -> list[dict]:
        """List immediate children; branch listings track tree membership."""
        path = _fs_path(path)
        result = []
        with self._workspace._transaction(write=self._write_transaction()) as db:
            branch, _, files, directories = self._state(db)
            _track_path(db, branch, path)
            exists = not path or path in files or path in directories
            is_directory = not path or path in directories
            if is_directory:
                if branch is not None:
                    _track_listing(db, branch)
                for candidate in sorted(files | directories):
                    parent, _, name = candidate.rpartition("/")
                    if parent == path:
                        result.append({"name": name, "path": candidate,
                                       "type": "file" if candidate in files else "dir"})
        if not exists:
            raise FileNotFoundError(path)
        if not is_directory:
            raise NotADirectoryError(path)
        return result

    def mkdir(self, path: str, *, parents: bool = False, exist_ok: bool = False) -> None:
        path = _fs_path(path)
        error: OSError | None = None
        with self._workspace._transaction(write=True) as db:
            branch, _, files, directories = self._state(db, mutate=True)
            assert branch is not None
            _track_path(db, branch, path)
            if not path or path in directories:
                if not exist_ok:
                    error = FileExistsError(path or "/")
            elif path in files:
                error = FileExistsError(path)
            else:
                required = _parents(path)
                for parent in required:
                    _track_path(db, branch, parent)
                    if parent in files:
                        error = NotADirectoryError(parent)
                        break
                    if parent not in directories and not parents:
                        error = FileNotFoundError(parent)
                        break
                # All fallible filesystem preflight checks precede staged edits.
                if error is None:
                    for directory in required + [path]:
                        if directory not in directories:
                            _dir_change(db, branch["name"], directory, True)
        if error is not None:
            raise error

    def remove(self, path: str, *, recursive: bool = False) -> None:
        """Stage file removal or an atomic directory-subtree removal."""
        path = _fs_path(path)
        if not path:
            raise InvalidPathError("Cannot remove the workspace root")
        error: OSError | None = None
        with self._workspace._transaction(write=True) as db:
            branch, _, files, directories = self._state(db, mutate=True)
            assert branch is not None
            _track_path(db, branch, path)
            if path not in files and path not in directories:
                error = FileNotFoundError(path)
            elif path in files:
                _file_change(db, branch["name"], path, None)
            else:
                descendants = {item for item in files | directories if item.startswith(path + "/")}
                _track_listing(db, branch)
                if descendants and not recursive:
                    error = OSError(errno.ENOTEMPTY, "Directory not empty", path)
                else:
                    for item in sorted(descendants | {path}):
                        _track_path(db, branch, item)
                        if item in files:
                            _file_change(db, branch["name"], item, None)
                        else:
                            _dir_change(db, branch["name"], item, False)
        if error is not None:
            raise error

    def rename(self, source: str, destination: str, *, replace: bool = False) -> None:
        """Stage an atomic file or directory move, tracking its source inputs."""
        source, destination = _fs_path(source), _fs_path(destination)
        if not source or not destination:
            raise InvalidPathError("Cannot rename the workspace root")
        error: OSError | None = None
        with self._workspace._transaction(write=True) as db:
            branch, revision, files, directories = self._state(db, mutate=True)
            assert branch is not None
            _track_path(db, branch, source)
            if source not in files and source not in directories:
                error = FileNotFoundError(source)
            elif source != destination:
                _track_path(db, branch, destination)
                if source in directories and destination.startswith(source + "/"):
                    raise InvalidPathError("Cannot move a directory into its own subtree")
                for parent in _parents(destination):
                    _track_path(db, branch, parent)
                    if parent in files:
                        error = NotADirectoryError(parent)
                        break
                    if parent not in directories:
                        error = FileNotFoundError(parent)
                        break
                destination_exists = destination in files or destination in directories
                if error is None and destination_exists:
                    if not replace:
                        error = FileExistsError(destination)
                    elif source in files and destination in directories:
                        error = IsADirectoryError(destination)
                    elif source in directories and destination in files:
                        error = NotADirectoryError(destination)
                    elif destination in directories:
                        _track_listing(db, branch)
                        if any(item.startswith(destination + "/") for item in files | directories):
                            error = OSError(errno.ENOTEMPTY, "Directory not empty", destination)
                if error is None:
                    affected = {item for item in files | directories
                                if item == source or item.startswith(source + "/")}
                    if source in directories:
                        _track_listing(db, branch)
                    for item in sorted(affected):
                        _track_path(db, branch, item)
                    before = {item: _visible_digest(db, branch, revision, item) for item in files}
                    after = {item: digest for item, digest in before.items() if item not in affected}
                    after_dirs = directories - affected
                    for item in affected:
                        target = destination + item[len(source):]
                        if item in files:
                            after[target] = before[item]
                        else:
                            after_dirs.add(target)
                    # Every visible file has a digest, by the tree invariant.
                    _stage_tree(db, branch, {k: v for k, v in before.items() if v is not None}, directories,
                                {k: v for k, v in after.items() if v is not None}, after_dirs)
        if error is not None:
            raise error

    def capture_files(self) -> dict:
        """Atomically read the entire visible tree and return a content token.

        The token can be supplied to apply_files after a subprocess finishes so
        importing its outputs cannot overwrite newer staged work in this branch.
        For branch owners this also records every observed base file dependency,
        directory version, and the base tree's name/type membership.
        """
        with self._workspace._transaction(write=self._write_transaction()) as db:
            branch, revision, files, directories = self._state(db)
            if branch is not None:
                _track_listing(db, branch)
            hashes = {}
            contents = {}
            for path in sorted(files):
                _track_path(db, branch, path)
                digest = _visible_digest(db, branch, revision, path)
                assert digest is not None
                hashes[path] = digest
                contents[path] = bytes(db.execute(
                    "SELECT data FROM blobs WHERE hash=?", (digest,)).fetchone()[0])
            for directory in sorted(directories):
                _track_path(db, branch, directory)
            return {"files": contents, "directories": sorted(directories),
                    "token": _token(hashes, directories)}

    def apply_files(self, files: dict[str, bytes], directories: list[str] | None = None,
                    *, expected_token: str | None = None) -> None:
        """Atomically replace the branch's visible tree with these files/dirs.

        Omitted directories are inferred from file paths; include explicit empty
        directories to preserve them. Parent directories are added automatically.
        Identical file bytes are not recorded as writes. Shared state remains
        unchanged until Branch.accept validates and publishes the whole proposal.
        """
        if not isinstance(files, dict) or any(not isinstance(data, bytes) for data in files.values()):
            raise TypeError("apply_files expects a dict of file paths to bytes")
        if directories is not None and not isinstance(directories, list):
            raise TypeError("directories must be a list of relative paths or None")
        if expected_token is not None and not isinstance(expected_token, str):
            raise TypeError("expected_token must be a string or None")
        contents = {_path(path): data for path, data in files.items()}
        after_dirs = {_path(path) for path in directories or []}
        for path in contents.keys() | after_dirs.copy():
            after_dirs.update(_parents(path))
        validate_tree(set(contents), after_dirs)
        with self._workspace._transaction(write=True) as db:
            branch, revision, before_files, before_dirs = self._state(db, mutate=True)
            assert branch is not None
            before = {}
            for path in before_files:
                digest = _visible_digest(db, branch, revision, path)
                assert digest is not None
                before[path] = digest
            if expected_token is not None and _token(before, before_dirs) != expected_token:
                raise WorkspaceError("Branch files changed after capture; rerun before importing outputs")
            _track_listing(db, branch)
            after = {path: _put_blob(db, data) for path, data in contents.items()}
            _stage_tree(db, branch, before, before_dirs, after, after_dirs)

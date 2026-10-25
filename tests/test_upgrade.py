"""Legacy schema fixtures exercise file/directory migration without an old wheel.

The SQL fixture describes schema 2 independently of the current implementation.
In that schema directories were implied by live file paths and were not stored.
"""

from __future__ import annotations

from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from agentsql import ConflictError, Workspace


LEGACY_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE blobs (hash TEXT PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT, parent_id INTEGER REFERENCES revisions(id),
    branch TEXT, message TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE entries (
    revision INTEGER NOT NULL REFERENCES revisions(id), path TEXT NOT NULL,
    version INTEGER NOT NULL REFERENCES revisions(id), hash TEXT REFERENCES blobs(hash),
    PRIMARY KEY(revision,path)
);
CREATE TABLE branches (
    name TEXT PRIMARY KEY, base_revision INTEGER NOT NULL REFERENCES revisions(id),
    status TEXT NOT NULL DEFAULT 'open' CHECK(status IN('open','accepted')),
    accepted_revision INTEGER REFERENCES revisions(id), listing_hash TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE reads (branch TEXT NOT NULL REFERENCES branches(name), path TEXT NOT NULL,
    version INTEGER NOT NULL, PRIMARY KEY(branch,path));
CREATE TABLE changes (branch TEXT NOT NULL REFERENCES branches(name), path TEXT NOT NULL,
    hash TEXT REFERENCES blobs(hash), PRIMARY KEY(branch,path));
CREATE TABLE proposals (branch TEXT PRIMARY KEY REFERENCES branches(name), proposal_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
"""


def _legacy_database(path, initial, accepted=(), pending=None, *, sealed=False, listing=False):
    """Build complete old revisions, retained file tombstones, and optional work."""
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript(LEGACY_SCHEMA)
        state = {}

        def blob(data):
            if data is None:
                return None
            digest = hashlib.sha256(data).hexdigest()
            db.execute("INSERT OR IGNORE INTO blobs VALUES (?,?)", (digest, data))
            return digest

        for revision, delta in enumerate([initial, *accepted], 1):
            branch = None if revision == 1 else f"old-accepted-{revision}"
            db.execute("INSERT INTO revisions(id,parent_id,branch,message) VALUES (?,?,?,?)",
                       (revision, None if revision == 1 else revision - 1, branch, "old revision"))
            if branch is not None:
                db.execute("INSERT INTO branches(name,base_revision,status,accepted_revision) "
                           "VALUES (?,?,'accepted',?)", (branch, revision - 1, revision))
            for name, data in sorted(delta.items()):
                digest = blob(data)
                state[name] = revision, digest
                if branch is not None:
                    db.execute("INSERT INTO changes VALUES (?,?,?)", (branch, name, digest))
            db.executemany("INSERT INTO entries VALUES (?,?,?,?)",
                           [(revision, name, version, digest) for name, (version, digest) in state.items()])
        db.execute("INSERT INTO meta VALUES ('head_revision',?)", (str(revision),))
        db.execute("INSERT INTO meta VALUES ('schema_version','2')")
        proposal = None
        if pending is not None:
            membership = None
            if listing:
                paths = sorted(name for name, (_, digest) in state.items() if digest is not None)
                membership = hashlib.sha256(json.dumps(paths, ensure_ascii=True).encode("ascii")).hexdigest()
            db.execute("INSERT INTO branches(name,base_revision,listing_hash) VALUES ('old-pending',?,?)",
                       (revision, membership))
            writes = [(name, blob(data)) for name, data in sorted(pending.items())]
            db.executemany("INSERT INTO changes VALUES ('old-pending',?,?)", writes)
            if sealed:
                payload = {"branch": "old-pending", "base_revision": revision,
                           "listing_hash": membership, "reads": [], "changes": writes}
                proposal = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=True,
                    separators=(",", ":")).encode("ascii")).hexdigest()
                db.execute("INSERT INTO proposals(branch,proposal_id) VALUES ('old-pending',?)", (proposal,))
    return proposal


def _implied_directories(files):
    directories = set()
    for name in files:
        components = name.split("/")
        for length in range(1, len(components)):
            directories.add("/".join(components[:length]))
    return sorted(directories)


class SchemaUpgradeContracts(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.database = Path(temporary.name) / "old.db"

    def assert_tree(self, snapshot, expected):
        capture = snapshot.fs.capture_files()
        self.assertEqual(capture["files"], expected)
        self.assertEqual(capture["directories"], _implied_directories(expected))

    def test_committed_directory_to_file_conversion_migrates_without_collisions(self):
        _legacy_database(self.database, {"parent/child": b"nested"},
                         [{"parent/child": None, "parent": b"flat"}])
        workspace = Workspace.open(self.database)
        self.assert_tree(workspace.snapshot(1), {"parent/child": b"nested"})
        self.assert_tree(workspace.snapshot(), {"parent": b"flat"})
        self.assertTrue(workspace.check_integrity()["ok"])
        new = workspace.branch("after-upgrade")
        new.fs.write_file("unrelated", "works")
        new.accept()
        self.assertEqual(workspace.snapshot().fs.read_file("parent"), "flat")

    def test_committed_file_to_directory_conversion_has_new_directory_version(self):
        _legacy_database(self.database, {"parent": b"flat"},
                         [{"parent": None, "parent/child": b"nested"}])
        workspace = Workspace.open(self.database)
        self.assert_tree(workspace.snapshot(1), {"parent": b"flat"})
        self.assert_tree(workspace.snapshot(), {"parent/child": b"nested"})
        self.assertEqual(workspace.snapshot().fs.stat("parent")["version"], 2)
        self.assertTrue(workspace.check_integrity()["ok"])

    def test_multiple_removal_recreation_and_nested_type_conversions_preserve_snapshots(self):
        initial = {"a/b/child": b"first"}
        deltas = [
            {"a/b/child": None},
            {"a/b/child": b"third"},
            {"a/b/child": None, "a": b"flat"},
            {"a": None, "a/b/child": b"fifth"},
        ]
        _legacy_database(self.database, initial, deltas)
        workspace = Workspace.open(self.database)
        expected = dict(initial)
        for revision, delta in enumerate([initial, *deltas], 1):
            for name, data in delta.items():
                if data is None:
                    expected.pop(name, None)
                else:
                    expected[name] = data
            self.assert_tree(workspace.snapshot(revision), expected)
        self.assertEqual(workspace.snapshot(3).fs.stat("a")["version"], 3)
        self.assertEqual(workspace.snapshot(3).fs.stat("a/b")["version"], 3)
        self.assertEqual(workspace.snapshot().fs.stat("a")["version"], 5)
        self.assertEqual(workspace.snapshot().fs.stat("a/b")["version"], 5)
        with closing(sqlite3.connect(self.database)) as db:
            self.assertEqual(db.execute("SELECT version,present FROM dirs WHERE revision=2 AND path='a'").fetchone(), (2, 0))
            self.assertEqual(db.execute("SELECT version,present FROM dirs WHERE revision=4 AND path='a/b'").fetchone(), (4, 0))
        self.assertTrue(workspace.check_integrity()["ok"])

    def test_pending_sealed_directory_to_file_conversion_keeps_exact_old_reviewed_id(self):
        token = _legacy_database(self.database, {"a/b/child": b"nested"},
                                 pending={"a/b/child": None, "a": b"flat"},
                                 sealed=True, listing=True)
        workspace = Workspace.open(self.database)
        branch = workspace.get_branch("old-pending")
        self.assertEqual(branch.inspect()["proposal_id"], token)
        self.assertEqual(branch.inspect()["directories"], [
            {"path": "a", "present": 0}, {"path": "a/b", "present": 0},
        ])
        self.assertTrue(workspace.check_integrity()["ok"])
        branch.accept(expected_proposal=token)
        self.assert_tree(workspace.snapshot(), {"a": b"flat"})
        self.assertTrue(workspace.check_integrity()["ok"])

    def test_pending_sealed_file_to_directory_conversion_keeps_exact_old_reviewed_id(self):
        token = _legacy_database(self.database, {"a": b"flat"},
                                 pending={"a": None, "a/b/child": b"nested"},
                                 sealed=True, listing=True)
        workspace = Workspace.open(self.database)
        branch = workspace.get_branch("old-pending")
        self.assertEqual(branch.inspect()["proposal_id"], token)
        branch.accept(expected_proposal=token)
        self.assert_tree(workspace.snapshot(), {"a/b/child": b"nested"})
        self.assertEqual(workspace.snapshot().fs.stat("a/b")["version"], 2)
        self.assertTrue(workspace.check_integrity()["ok"])

    def test_pending_recreation_after_old_directory_tombstone_gets_new_version(self):
        token = _legacy_database(self.database, {"a/child": b"old"},
                                 [{"a/child": None}], pending={"a/child": b"new"}, sealed=True)
        workspace = Workspace.open(self.database)
        branch = workspace.get_branch("old-pending")
        self.assertEqual(branch.inspect()["directories"], [{"path": "a", "present": 1}])
        branch.accept(expected_proposal=token)
        self.assertEqual(workspace.snapshot().fs.stat("a")["version"], 3)
        self.assert_tree(workspace.snapshot(), {"a/child": b"new"})
        self.assertTrue(workspace.check_integrity()["ok"])

    def test_old_file_only_listing_remains_valid_after_new_empty_directory(self):
        token = _legacy_database(self.database, {"input": b"old"}, pending={"output": b"derived"},
                                 sealed=True, listing=True)
        workspace = Workspace.open(self.database)
        newer = workspace.branch("new-dir")
        newer.fs.mkdir("empty")
        newer.accept()
        old = workspace.get_branch("old-pending")
        old.accept(expected_proposal=token)
        self.assertEqual(workspace.snapshot().fs.read_file("output"), "derived")
        self.assertTrue(workspace.snapshot().fs.exists("empty"))
        self.assertTrue(workspace.check_integrity()["ok"])

    def test_old_file_only_listing_still_rejects_new_file(self):
        token = _legacy_database(self.database, {"input": b"old"}, pending={"output": b"derived"},
                                 sealed=True, listing=True)
        workspace = Workspace.open(self.database)
        newer = workspace.branch("new-file")
        newer.fs.write_file("additional", "new")
        newer.accept()
        old = workspace.get_branch("old-pending")
        with self.assertRaises(ConflictError):
            old.accept(expected_proposal=token)
        self.assertFalse(workspace.snapshot().fs.exists("output"))


if __name__ == "__main__":
    unittest.main()

"""Seeded state-machine checks against a small in-memory contract oracle.

This oracle stores complete byte dictionaries and integer versions. It does
not call the implementation's hashing, SQLite, merge, or path helpers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
from pathlib import Path
import random
import tempfile
import unittest

from agentsql import BranchClosedError, ConflictError, Workspace, WorkspaceError


@dataclass
class ModelBranch:
    base_revision: int
    base: dict
    base_dirs: dict
    reads: dict = field(default_factory=dict)
    changes: dict = field(default_factory=dict)
    dir_reads: dict = field(default_factory=dict)
    dir_changes: dict = field(default_factory=dict)
    listing: tuple[set, set] | None = None
    accepted_revision: int | None = None

    def visible(self, path):
        if path in self.changes:
            return self.changes[path]
        return self.base.get(path, (0, None))[1]

    def paths(self):
        paths = {path for path, (_, data) in self.base.items() if data is not None}
        for path, data in self.changes.items():
            if data is None:
                paths.discard(path)
            else:
                paths.add(path)
        return paths

    def directories(self):
        directories = {path for path, (_, present) in self.base_dirs.items() if present}
        for path, present in self.dir_changes.items():
            if present:
                directories.add(path)
            else:
                directories.discard(path)
        return directories

    def observe_path(self, path):
        if path not in self.changes:
            self.reads.setdefault(path, self.base.get(path, (0, None))[0])
        if path not in self.dir_changes:
            self.dir_reads.setdefault(path, self.base_dirs.get(path, (0, False))[0])

    def list_base(self):
        self.listing = ({path for path, (_, data) in self.base.items() if data is not None},
                        {path for path, (_, present) in self.base_dirs.items() if present})

    def write(self, path, data):
        parts = path.split("/")
        for index in range(1, len(parts)):
            parent = "/".join(parts[:index])
            if parent not in self.directories():
                self.dir_changes[parent] = True
        self.changes[path] = data


class SeededModelTests(unittest.TestCase):
    PATHS = ("input.txt", "counter.txt", "optional.txt", "results/a.txt", "results/b.txt",
             "binary.bin", "café.txt", "never-seen.txt")
    DIRECTORIES = ("empty-a", "empty-b", "seed-empty/child")

    def _assert_snapshot(self, snapshot, state, directories, revision):
        self.assertEqual(snapshot.revision, revision)
        self.assertEqual(snapshot.list_files(), sorted(
            path for path, (_, data) in state.items() if data is not None))
        for path in self.PATHS:
            version, data = state.get(path, (0, None))
            self.assertEqual(snapshot.version(path), version, path)
            self.assertEqual(snapshot.exists(path), data is not None, path)
            if data is None:
                with self.assertRaises(FileNotFoundError):
                    snapshot.read_bytes(path)
            else:
                self.assertEqual(snapshot.read_bytes(path), data, path)
        capture = snapshot.fs.capture_files()
        self.assertEqual(capture["files"], {path: data for path, (_, data) in state.items()
                                            if data is not None})
        self.assertEqual(capture["directories"], sorted(
            path for path, (_, present) in directories.items() if present))
        for path, (version, present) in directories.items():
            self.assertEqual(snapshot.fs.exists(path), present)
            if present:
                self.assertEqual(snapshot.fs.stat(path)["version"], version)
            else:
                with self.assertRaises(FileNotFoundError):
                    snapshot.fs.stat(path)

    def _assert_provenance(self, branch, model):
        observed = branch.inspect()
        self.assertEqual(observed["base_revision"], model.base_revision)
        self.assertEqual(observed["status"], "open" if model.accepted_revision is None else "accepted")
        self.assertEqual(observed["accepted_revision"], model.accepted_revision)
        self.assertEqual({item["path"]: item["version"] for item in observed["reads"]}, model.reads)
        self.assertEqual(observed["tracks_listing"], model.listing is not None)
        self.assertEqual({item["path"]: item["version"] for item in observed["directory_reads"]},
                         model.dir_reads)
        self.assertEqual({item["path"]: bool(item["present"]) for item in observed["directories"]},
                         model.dir_changes)
        actual_changes = {item["path"]: item for item in observed["changes"]}
        self.assertEqual(set(actual_changes), set(model.changes))
        for path, data in model.changes.items():
            old_version, old_data = model.base.get(path, (0, None))
            actual = actual_changes[path]
            self.assertEqual(actual["base_version"], old_version)
            self.assertEqual(actual["kind"], "delete" if data is None else (
                "add" if old_data is None else "modify"))
            self.assertEqual(actual["base_sha256"], None if old_data is None else hashlib.sha256(old_data).hexdigest())
            self.assertEqual(actual["proposed_sha256"], None if data is None else hashlib.sha256(data).hexdigest())

    def _exercise_seed(self, seed):
        randomizer = random.Random(seed)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "inputs"
            source.mkdir()
            initial = {"input.txt": b"source\n", "counter.txt": b"0", "binary.bin": bytes(range(32))}
            for path, data in initial.items():
                (source / path).write_bytes(data)
            (source / "seed-empty" / "child").mkdir(parents=True)
            database = root / "workspace.sqlite3"
            workspace = Workspace.create(database, source)
            state = {path: (1, data) for path, data in initial.items()}
            directories = {"seed-empty": (1, True), "seed-empty/child": (1, True)}
            revision = 1
            models = {}
            snapshots = [(workspace.snapshot(), dict(state), dict(directories), revision)]
            expected_history = [{"revision": 1, "parent_revision": None, "branch": None,
                                 "message": "Initial capture", "paths": sorted(initial),
                                 "directories": sorted(directories), "keys": []}]
            outcomes = {"accepted": 0, "conflict": 0, "read": 0, "write": 0,
                        "delete": 0, "listing": 0, "reopen": 0, "mkdir": 0, "rmdir": 0}

            def new_branch():
                name = f"seed-{seed}-branch-{len(models)}"
                workspace.branch(name)
                models[name] = ModelBranch(revision, dict(state), dict(directories))
                return name

            for _ in range(6):
                new_branch()

            # Prepare a reader with a disjoint output before randomized writes.
            # The fixed seeds and outcome assertions below make the exercised
            # acceptance, rejection, listing, deletion, and reopen cases explicit.
            stale = models[next(iter(models))]
            stale_name = next(iter(models))
            stale.reads["input.txt"] = 1
            workspace.get_branch(stale_name).read_bytes("input.txt")
            stale.write("results/a.txt", b"derived from old input")
            workspace.get_branch(stale_name).write_bytes("results/a.txt", b"derived from old input")

            for step in range(220):
                if step % 19 == 0:
                    workspace = Workspace.open(database)
                    outcomes["reopen"] += 1
                open_names = [name for name, branch in models.items() if branch.accepted_revision is None]
                if len(open_names) < 4 or randomizer.random() < 0.13:
                    open_names.append(new_branch())
                name = randomizer.choice(open_names)
                model = models[name]
                branch = workspace.get_branch(name)
                path = randomizer.choice(self.PATHS)
                operation = randomizer.choices(
                    ("read", "exists", "write", "delete", "listing", "accept", "snapshot",
                     "mkdir", "rmdir"),
                    weights=(13, 8, 25, 12, 9, 26, 7, 6, 6), k=1,
                )[0]
                if operation in ("mkdir", "rmdir"):
                    path = randomizer.choice(self.DIRECTORIES)
                with self.subTest(seed=seed, step=step, branch=name, operation=operation, path=path):
                    if operation in ("read", "exists"):
                        expected = model.visible(path)
                        if path not in model.changes:
                            model.reads.setdefault(path, model.base.get(path, (0, None))[0])
                        if operation == "exists":
                            self.assertEqual(branch.exists(path), expected is not None)
                        elif expected is None:
                            with self.assertRaises(FileNotFoundError):
                                branch.read_bytes(path)
                        else:
                            self.assertEqual(branch.read_bytes(path), expected)
                        outcomes["read"] += 1
                    elif operation == "write":
                        # Include empty and binary files, repeated identical
                        # writes, and overwriting an already-staged deletion.
                        value = randomizer.choice((b"", b"\x00\xff\x10", b"same bytes", f"seed={seed};step={step}".encode()))
                        branch.write_bytes(path, value)
                        model.write(path, value)
                        outcomes["write"] += 1
                    elif operation == "delete":
                        if model.visible(path) is None:
                            with self.assertRaises(FileNotFoundError):
                                branch.delete(path)
                            if path not in model.changes:
                                model.reads.setdefault(path, model.base.get(path, (0, None))[0])
                        else:
                            branch.delete(path)
                            model.changes[path] = None
                            outcomes["delete"] += 1
                    elif operation == "listing":
                        self.assertEqual(branch.list_files(), sorted(model.paths()))
                        model.list_base()
                        outcomes["listing"] += 1
                    elif operation == "mkdir":
                        model.observe_path(path)
                        if path in model.directories():
                            with self.assertRaises(FileExistsError):
                                branch.fs.mkdir(path)
                        else:
                            parts = path.split("/")
                            for index in range(1, len(parts)):
                                model.observe_path("/".join(parts[:index]))
                            branch.fs.mkdir(path)
                            model.dir_changes[path] = True
                            outcomes["mkdir"] += 1
                    elif operation == "rmdir":
                        model.observe_path(path)
                        if path not in model.directories():
                            with self.assertRaises(FileNotFoundError):
                                branch.fs.remove(path)
                        else:
                            branch.fs.remove(path)
                            model.list_base()
                            model.dir_changes[path] = False
                            outcomes["rmdir"] += 1
                    elif operation == "snapshot":
                        snapshot = workspace.snapshot()
                        snapshots.append((snapshot, dict(state), dict(directories), revision))
                        self._assert_snapshot(snapshot, state, directories, revision)
                    else:
                        conflicts = {}
                        for dependency, expected_version in model.reads.items():
                            actual_version = state.get(dependency, (0, None))[0]
                            if expected_version != actual_version:
                                conflicts[("stale_read", "file", dependency)] = (expected_version, actual_version)
                        for changed in model.changes:
                            expected_version = model.base.get(changed, (0, None))[0]
                            actual_version = state.get(changed, (0, None))[0]
                            if expected_version != actual_version:
                                conflicts[("conflicting_write", "file", changed)] = (expected_version, actual_version)
                        for dependency, expected_version in model.dir_reads.items():
                            actual_version = directories.get(dependency, (0, False))[0]
                            if expected_version != actual_version:
                                conflicts[("stale_read", "directory", dependency)] = (expected_version, actual_version)
                        for changed, present in model.dir_changes.items():
                            expected_version, was_present = model.base_dirs.get(changed, (0, False))
                            actual_version, current_present = directories.get(changed, (0, False))
                            # Independent additions may share an implicit parent
                            # directory; creating an existing directory is idempotent.
                            if expected_version != actual_version and not (
                                present and current_present and not was_present
                            ):
                                conflicts[("conflicting_write", "directory", changed)] = (expected_version, actual_version)
                        listing_changed = model.listing is not None and model.listing != (
                            {path for path, (_, data) in state.items() if data is not None},
                            {path for path, (_, present) in directories.items() if present})
                        old_history = workspace.history()
                        old_proposal = branch.inspect()
                        if not model.changes and not model.dir_changes:
                            with self.assertRaises(WorkspaceError):
                                branch.accept()
                            self.assertEqual(workspace.history(), old_history)
                        elif conflicts or listing_changed:
                            with self.assertRaises(ConflictError) as raised:
                                branch.accept(f"model step {step}")
                            evidence = raised.exception.as_dict()
                            self.assertEqual(evidence["base_revision"], model.base_revision)
                            self.assertEqual(evidence["current_revision"], revision)
                            actual = {(item["kind"], item.get("resource", "tree" if item["kind"] == "stale_listing"
                                                               else "file"), item["path"]): item
                                      for item in evidence["conflicts"]}
                            expected_keys = set(conflicts)
                            if listing_changed:
                                expected_keys.add(("stale_listing", "tree", "*"))
                            self.assertEqual(set(actual), expected_keys)
                            for key, (expected_version, actual_version) in conflicts.items():
                                self.assertEqual((actual[key]["expected"], actual[key]["actual"]),
                                                 (expected_version, actual_version))
                            self.assertEqual(workspace.history(), old_history)
                            self.assertEqual(branch.inspect(), old_proposal)
                            self._assert_snapshot(workspace.snapshot(), state, directories, revision)
                            outcomes["conflict"] += 1
                        else:
                            result = branch.accept(f"model step {step}")
                            parent = revision
                            revision += 1
                            self.assertEqual(result, {"revision": revision, "branch": name,
                                                      "paths": sorted(model.changes),
                                                      "directories": sorted(model.dir_changes), "keys": []})
                            for changed, value in model.changes.items():
                                state[changed] = (revision, value)
                            changed_directories = []
                            for changed, present in model.dir_changes.items():
                                if present and directories.get(changed, (0, False))[1]:
                                    continue
                                directories[changed] = (revision, present)
                                changed_directories.append(changed)
                            model.accepted_revision = revision
                            expected_history.append({
                                "revision": revision, "parent_revision": parent, "branch": name,
                                "message": f"model step {step}", "paths": sorted(model.changes),
                                "directories": sorted(changed_directories), "keys": [],
                            })
                            outcomes["accepted"] += 1
                            self._assert_snapshot(workspace.snapshot(), state, directories, revision)
                            with self.assertRaises(BranchClosedError):
                                branch.write_bytes("counter.txt", b"after acceptance")
                            with self.assertRaises(BranchClosedError):
                                branch.accept()
                    self._assert_provenance(branch, model)
                if step % 31 == 0:
                    # Validate an old snapshot after intervening publications.
                    frozen, old_state, old_dirs, old_revision = randomizer.choice(snapshots)
                    self._assert_snapshot(frozen, old_state, old_dirs, old_revision)

            workspace = Workspace.open(database)
            self._assert_snapshot(workspace.snapshot(), state, directories, revision)
            for name, model in models.items():
                self._assert_provenance(workspace.get_branch(name), model)
            history = workspace.history()
            for item in history:
                item.pop("created_at")
            self.assertEqual(history, expected_history)
            for frozen, old_state, old_dirs, old_revision in snapshots:
                self._assert_snapshot(frozen, old_state, old_dirs, old_revision)
            for kind in outcomes:
                self.assertGreater(outcomes[kind], 0, f"Seed {seed} did not exercise {kind}: {outcomes}")

    def test_seeded_random_histories_match_independent_oracle(self):
        for seed in (7, 31, 127, 509):
            with self.subTest(seed=seed):
                self._exercise_seed(seed)


if __name__ == "__main__":
    unittest.main()

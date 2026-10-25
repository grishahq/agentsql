"""JSON state shares file publication, snapshot, and dependency guarantees."""

from contextlib import closing
import multiprocessing
from pathlib import Path
import sqlite3
import tempfile
import unittest

from agentsql import (
    BranchClosedError, ConflictError, ProposalMismatchError, Workspace, WorkspaceError,
)


def _state_writer(database, name, ready, release, results):
    try:
        branch = Workspace.open(database).get_branch(name)
        branch.kv.set("same-key", {"winner": name})
        branch.write_text(f"{name}.txt", "atomic output")
        ready.put(True)
        if not release.wait(15):
            raise RuntimeError("Parent did not release writers")
        try:
            branch.accept()
            results.put((name, "accepted"))
        except ConflictError:
            results.put((name, "conflict"))
    except BaseException as exc:
        results.put((name, repr(exc)))


class StateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        source = self.root / "source"
        source.mkdir()
        (source / "input.txt").write_text("source", encoding="utf-8")
        self.database = self.root / "workspace.sqlite3"
        self.workspace = Workspace.create(self.database, source)

    def seed(self, values):
        branch = self.workspace.branch("seed")
        for key, value in values.items():
            branch.kv.set(key, value)
        branch.accept()
        return self.workspace.snapshot()

    def assert_conflict(self, branch, *, kind, resource="kv", path=None):
        before = self.workspace.history()
        with self.assertRaises(ConflictError) as caught:
            branch.accept()
        self.assertEqual(self.workspace.history(), before)
        matching = [item for item in caught.exception.conflicts
                    if item["kind"] == kind and item.get("resource") == resource
                    and (path is None or item["path"] == path)]
        self.assertTrue(matching, caught.exception.as_dict())
        return matching[0]

    def test_json_null_is_present_and_distinct_from_absent_or_deleted(self):
        branch = self.workspace.branch("null")
        marker = object()
        self.assertIs(branch.kv.get("absent", marker), marker)
        self.assertFalse(branch.kv.exists("absent"))
        self.assertEqual(branch.kv.version("absent"), 0)
        branch.kv.set("nullable", None)
        self.assertIsNone(branch.kv.get("nullable", marker))
        self.assertTrue(branch.kv.exists("nullable"))
        self.assertIsNone(branch.kv.version("nullable"))
        self.assertEqual(branch.kv.list(), [{"key": "nullable", "value": None, "version": None}])
        accepted = branch.accept()
        snapshot = self.workspace.snapshot()
        self.assertTrue(snapshot.kv.exists("nullable"))
        self.assertIsNone(snapshot.kv.get("nullable", marker))
        self.assertEqual(snapshot.kv.version("nullable"), accepted["revision"])
        remove = self.workspace.branch("remove-null")
        remove.kv.delete("nullable")
        self.assertFalse(remove.kv.exists("nullable"))
        self.assertIsNone(remove.kv.version("nullable"))
        deleted = remove.accept()
        final = self.workspace.snapshot()
        self.assertFalse(final.kv.exists("nullable"))
        self.assertIs(final.kv.get("nullable", marker), marker)
        self.assertEqual(final.kv.version("nullable"), deleted["revision"])
        self.assertEqual(final.kv.list(), [])

    def test_values_are_copied_at_set_and_on_each_read_or_listing(self):
        branch = self.workspace.branch("copies")
        original = {"items": [{"number": 1}], "unicode": "دبي"}
        branch.kv.set("config", original)
        original["items"][0]["number"] = 999
        read = branch.kv.get("config")
        self.assertEqual(read["items"][0]["number"], 1)
        read["items"].clear()
        listed = branch.kv.list()
        listed[0]["value"]["items"].clear()
        self.assertEqual(branch.kv.get("config")["items"], [{"number": 1}])
        branch.accept()
        pinned = self.workspace.snapshot()
        snapshot_value = pinned.kv.get("config")
        snapshot_value["items"].append("external")
        self.assertEqual(pinned.kv.get("config")["items"], [{"number": 1}])

    def test_strict_json_validation_never_stages_partial_values(self):
        branch = self.workspace.branch("invalid-json")
        cycle = []
        cycle.append(cycle)
        for value in (float("nan"), float("inf"), -float("inf"), {1: "key"},
                      {"bad": object()}, (1, 2), b"bytes", {"set"}, "\ud800", cycle):
            with self.subTest(value=type(value)), self.assertRaises(ValueError):
                branch.kv.set("bad", value)
            self.assertEqual(branch.inspect()["kv_changes"], [])
        valid = [None, True, False, 42, -99, 1.25, "text", [1, {"two": 2}], {"z": 0, "a": 1}]
        for index, value in enumerate(valid):
            branch.kv.set(f"valid/{index}", value)
            self.assertEqual(branch.kv.get(f"valid/{index}"), value)
        branch.accept()

    def test_key_validation_and_literal_prefix_metacharacters(self):
        branch = self.workspace.branch("keys")
        for key in (None, 10, "", "\n", "x" * 1025, "\ud800"):
            for operation in (lambda: branch.kv.set(key, 1), lambda: branch.kv.get(key),
                              lambda: branch.kv.exists(key), lambda: branch.kv.version(key),
                              lambda: branch.kv.delete(key)):
                with self.subTest(key=key), self.assertRaises(ValueError):
                    operation()
        values = {"a%one": 1, "a_one": 2, "a\\one": 3, "axone": 4,
                  "a%two": 5, "quote' OR 1=1 --": 6}
        for key, value in values.items():
            branch.kv.set(key, value)
        for prefix, keys in (("a%", ["a%one", "a%two"]), ("a_", ["a_one"]),
                             ("a\\", ["a\\one"]), ("quote' OR", ["quote' OR 1=1 --"])):
            self.assertEqual([row["key"] for row in branch.kv.list(prefix)], keys)
        self.assertEqual(len(branch.kv.list("")), len(values))
        branch.accept()
        self.assertEqual([row["key"] for row in self.workspace.snapshot().kv.list("a%")],
                         ["a%one", "a%two"])

    def test_state_only_acceptance_has_history_and_preserves_file_snapshot(self):
        initial = self.workspace.snapshot()
        branch = self.workspace.branch("only-state")
        branch.kv.set("workflow/status", "ready")
        branch.kv.set("workflow/count", 0)
        accepted = branch.accept("State initialized")
        self.assertEqual(accepted["paths"], [])
        self.assertEqual(accepted["keys"], ["workflow/count", "workflow/status"])
        self.assertEqual(self.workspace.history()[-1]["keys"], accepted["keys"])
        self.assertEqual(self.workspace.snapshot().read_text("input.txt"), "source")
        self.assertEqual(initial.kv.list(), [])
        self.assertEqual(self.workspace.snapshot().kv.get("workflow/status"), "ready")
        self.assertTrue(self.workspace.check_integrity()["ok"])

    def test_snapshot_state_is_immutable_and_pinned_across_publications(self):
        old = self.seed({"left": 0, "right": 0})
        reader = self.workspace.branch("reader")
        updater = self.workspace.branch("updater")
        updater.kv.set("left", 1)
        updater.kv.set("right", 1)
        updater.accept()
        self.assertEqual(old.kv.get("left"), 0)
        self.assertEqual(old.kv.get("right"), 0)
        self.assertEqual(reader.kv.get("left"), 0)
        self.assertEqual(reader.kv.get("right"), 0)
        self.assertEqual([row["value"] for row in self.workspace.snapshot().kv.list()], [1, 1])
        with self.assertRaisesRegex(WorkspaceError, "immutable"):
            old.kv.set("left", 2)
        with self.assertRaisesRegex(WorkspaceError, "immutable"):
            old.kv.delete("left")

    def test_stale_state_read_rejects_output_on_different_file_path(self):
        self.seed({"price": 100})
        reader = self.workspace.branch("report")
        reader.write_text("report.md", f"Total: {reader.kv.get('price') * 3}")
        updater = self.workspace.branch("prices")
        updater.kv.set("price", 120)
        new_revision = updater.accept()["revision"]
        evidence = self.assert_conflict(reader, kind="stale_read", path="price")
        self.assertEqual(evidence["actual"], new_revision)
        self.assertFalse(self.workspace.snapshot().exists("report.md"))
        fresh = self.workspace.branch("fresh-report")
        fresh.write_text("report.md", f"Total: {fresh.kv.get('price') * 3}")
        fresh.accept()
        self.assertEqual(self.workspace.snapshot().read_text("report.md"), "Total: 360")

    def test_each_tracked_read_operation_detects_changed_value(self):
        self.seed({"setting": "old"})
        readers = []
        for name, operation in (("get", lambda state: state.get("setting")),
                                ("exists", lambda state: state.exists("setting")),
                                ("version", lambda state: state.version("setting"))):
            reader = self.workspace.branch(f"reader-{name}")
            operation(reader.kv)
            reader.kv.set(f"output/{name}", True)
            readers.append(reader)
        update = self.workspace.branch("update")
        update.kv.set("setting", "new")
        update.accept()
        for reader in readers:
            self.assert_conflict(reader, kind="stale_read", path="setting")

    def test_mixed_changes_reject_atomically_when_state_write_conflicts(self):
        self.seed({"shared": 0})
        loser = self.workspace.branch("loser")
        loser.write_text("loser.txt", "must not escape")
        loser.kv.set("shared", 1)
        loser.kv.set("unique", "must not escape")
        winner = self.workspace.branch("winner")
        winner.kv.set("shared", 2)
        winner.accept()
        self.assert_conflict(loser, kind="conflicting_write", path="shared")
        snapshot = self.workspace.snapshot()
        self.assertFalse(snapshot.exists("loser.txt"))
        self.assertFalse(snapshot.kv.exists("unique"))
        self.assertEqual(snapshot.kv.get("shared"), 2)

    def test_mixed_changes_reject_atomically_when_file_write_conflicts(self):
        loser = self.workspace.branch("loser")
        loser.write_text("input.txt", "loser")
        loser.kv.set("unique", "must not escape")
        winner = self.workspace.branch("winner")
        winner.write_text("input.txt", "winner")
        winner.accept()
        self.assert_conflict(loser, kind="conflicting_write", resource=None, path="input.txt")
        self.assertFalse(self.workspace.snapshot().kv.exists("unique"))
        self.assertEqual(self.workspace.snapshot().read_text("input.txt"), "winner")

    def test_sqlite_failure_after_file_writes_rolls_back_entire_publication(self):
        branch = self.workspace.branch("mixed")
        branch.write_text("result.txt", "not published until all writes commit")
        branch.kv.set("failure", {"fail": True})
        before = self.workspace.history()
        with closing(sqlite3.connect(self.database)) as db:
            db.execute("CREATE TRIGGER fail_state BEFORE INSERT ON kv_entries "
                       "WHEN NEW.key='failure' BEGIN SELECT RAISE(ABORT, 'injected KV failure'); END")
            db.commit()
        with self.assertRaisesRegex(WorkspaceError, "injected KV failure"):
            branch.accept()
        self.assertEqual(self.workspace.history(), before)
        self.assertFalse(self.workspace.snapshot().exists("result.txt"))
        self.assertFalse(self.workspace.snapshot().kv.exists("failure"))
        self.assertEqual(branch.inspect()["status"], "open")
        with closing(sqlite3.connect(self.database)) as db:
            db.execute("DROP TRIGGER fail_state")
            db.commit()
        branch.accept()
        self.assertTrue(self.workspace.snapshot().exists("result.txt"))
        self.assertEqual(self.workspace.snapshot().kv.get("failure"), {"fail": True})

    def test_missing_read_detects_create_then_delete_aba(self):
        reader = self.workspace.branch("absent-reader")
        self.assertFalse(reader.kv.exists("optional"))
        reader.kv.set("report", "computed without optional")
        create = self.workspace.branch("create")
        create.kv.set("optional", 1)
        create.accept()
        delete = self.workspace.branch("delete")
        delete.kv.delete("optional")
        deleted = delete.accept()["revision"]
        self.assertFalse(self.workspace.snapshot().kv.exists("optional"))
        evidence = self.assert_conflict(reader, kind="stale_read", path="optional")
        self.assertEqual(evidence["expected"], 0)
        self.assertEqual(evidence["actual"], deleted)

    def test_caught_missing_delete_tracks_absence_before_producing_another_output(self):
        reader = self.workspace.branch("missing-delete")
        try:
            reader.kv.delete("optional")
        except KeyError:
            reader.write_text("without-optional.md", "Nothing to remove")
        self.assertEqual(reader.inspect()["kv_reads"], [{"key": "optional", "version": 0}])
        creator = self.workspace.branch("creator")
        creator.kv.set("optional", 1)
        creator.accept()
        self.assert_conflict(reader, kind="stale_read", path="optional")
        self.assertFalse(self.workspace.snapshot().exists("without-optional.md"))

    def test_repeated_delete_of_own_pending_tombstone_does_not_invent_base_read(self):
        self.seed({"existing": 1})
        reader = self.workspace.branch("pending-delete")
        reader.kv.delete("existing")
        with self.assertRaises(KeyError):
            reader.kv.delete("existing")
        self.assertEqual(reader.inspect()["kv_reads"], [])
        reader.accept()
        self.assertFalse(self.workspace.snapshot().kv.exists("existing"))

    def test_same_value_rewrite_and_delete_recreate_are_new_versions(self):
        original = self.seed({"setting": {"same": True}})
        reader = self.workspace.branch("old-version")
        reader.kv.get("setting")
        reader.kv.set("output", 1)
        rewrite = self.workspace.branch("same-value")
        rewrite.kv.set("setting", {"same": True})
        rewritten = rewrite.accept()["revision"]
        self.assertEqual(original.kv.get("setting"), self.workspace.snapshot().kv.get("setting"))
        self.assertEqual(self.workspace.snapshot().kv.version("setting"), rewritten)
        self.assert_conflict(reader, kind="stale_read", path="setting")
        reader2 = self.workspace.branch("before-recreate")
        reader2.kv.get("setting")
        reader2.kv.set("output2", 1)
        delete = self.workspace.branch("delete")
        delete.kv.delete("setting")
        delete.accept()
        recreate = self.workspace.branch("recreate")
        recreate.kv.set("setting", {"same": True})
        recreate.accept()
        self.assert_conflict(reader2, kind="stale_read", path="setting")

    def test_prefix_listing_detects_phantom_and_value_change(self):
        self.seed({"team/alice": {"score": 1}, "other/bob": 2})
        phantom = self.workspace.branch("phantom-reader")
        content = self.workspace.branch("content-reader")
        self.assertEqual(len(phantom.kv.list("team/")), 1)
        content.kv.list("team/")
        phantom.kv.set("summary/phantom", True)
        content.kv.set("summary/content", True)
        add = self.workspace.branch("add-member")
        add.kv.set("team/eve", 3)
        add.accept()
        self.assert_conflict(phantom, kind="stale_listing", path="team/")
        update = self.workspace.branch("score")
        update.kv.set("team/alice", {"score": 9})
        update.accept()
        self.assert_conflict(content, kind="stale_read", path="team/alice")

    def test_listing_alone_tracks_existing_value_changes_without_name_changes(self):
        self.seed({"team/alice": 1})
        reader = self.workspace.branch("summary")
        self.assertEqual(reader.kv.list("team/")[0]["value"], 1)
        reader.write_text("summary.md", "score=1")
        update = self.workspace.branch("update")
        update.kv.set("team/alice", 2)
        update.accept()
        conflict = self.assert_conflict(reader, kind="stale_read", path="team/alice")
        self.assertGreater(conflict["actual"], conflict["expected"])

    def test_empty_prefix_query_detects_first_matching_key_but_unrelated_prefix_accepts(self):
        reader = self.workspace.branch("empty-reader")
        self.assertEqual(reader.kv.list("config/%_"), [])
        reader.kv.set("summary", "no matching config")
        unrelated = self.workspace.branch("unrelated")
        unrelated.kv.set("config/anything", True)
        unrelated.accept()
        reader.accept()
        phantom = self.workspace.branch("prefix-reader")
        phantom.kv.list("config/%_")
        phantom.kv.set("summary", "still empty")
        matching = self.workspace.branch("literal-match")
        matching.kv.set("config/%_literal", True)
        matching.accept()
        self.assert_conflict(phantom, kind="stale_listing", path="config/%_")

    def test_pending_values_and_deletions_are_overlaid_on_listing(self):
        self.seed({"a": 1, "b": 2, "outside": 3})
        branch = self.workspace.branch("overlay")
        branch.kv.delete("a")
        branch.kv.set("b", 20)
        branch.kv.set("c", None)
        self.assertEqual(branch.kv.list(), [
            {"key": "b", "value": 20, "version": None},
            {"key": "c", "value": None, "version": None},
            {"key": "outside", "value": 3, "version": 2},
        ])
        self.assertEqual(branch.inspect()["kv_reads"], [{"key": "outside", "version": 2}])
        with self.assertRaises(KeyError):
            branch.kv.delete("missing")
        with self.assertRaises(KeyError):
            branch.kv.delete("a")
        branch.accept()
        self.assertEqual([row["key"] for row in self.workspace.snapshot().kv.list()],
                         ["b", "c", "outside"])

    def test_sealing_freezes_all_state_operations_and_checks_reviewed_token(self):
        self.seed({"input": 1})
        branch = self.workspace.branch("sealed")
        branch.kv.get("input")
        branch.kv.set("output", 2)
        token = branch.seal()
        before = branch.inspect()
        for operation in (lambda: branch.kv.get("input"), lambda: branch.kv.exists("input"),
                          lambda: branch.kv.version("input"), lambda: branch.kv.set("output", 3),
                          lambda: branch.kv.delete("input"), branch.kv.list):
            with self.assertRaises(BranchClosedError):
                operation()
            self.assertEqual(branch.inspect(), before)
        self.assertEqual(branch.seal(), token)
        with self.assertRaises(ProposalMismatchError):
            branch.accept(expected_proposal=token + "wrong")
        branch.accept(expected_proposal=token)
        self.assertEqual(self.workspace.snapshot().kv.get("output"), 2)

    def test_pending_fork_copies_state_and_read_provenance_independently(self):
        self.seed({"input": {"price": 100}})
        source = self.workspace.branch("source")
        source.kv.get("input")
        source.kv.list("input")
        source.kv.set("output", {"attempt": 1})
        source.write_text("output.md", "first")
        source.tools.record("actual-effect", result=True)
        source.seal()
        fork = self.workspace.fork("source", "alternative")
        self.assertEqual(fork.inspect()["kv_reads"], source.inspect()["kv_reads"])
        self.assertEqual(fork.inspect()["kv_list_reads"], source.inspect()["kv_list_reads"])
        self.assertEqual(fork.kv.get("output"), {"attempt": 1})
        self.assertFalse(fork.inspect()["sealed"])
        self.assertEqual(fork.tools.list(), [])
        fork.kv.set("output", {"attempt": 2})
        fork.write_text("output.md", "second")
        self.assertEqual(source.inspect()["kv_changes"][0]["value"], {"attempt": 1})
        fork.accept()
        self.assertEqual(self.workspace.snapshot().kv.get("output"), {"attempt": 2})
        self.assert_conflict(source, kind="conflicting_write", path="output")
        resumed = self.workspace.fork("alternative", "accepted-fork")
        self.assertEqual(resumed.base_revision, self.workspace.snapshot().revision)
        self.assertEqual(resumed.kv.get("output"), {"attempt": 2})
        self.assertEqual(resumed.inspect()["kv_changes"], [])

    def test_persistence_backup_preserve_staged_state_and_dependencies(self):
        self.seed({"input": 1})
        branch = self.workspace.branch("worker")
        branch.kv.get("input")
        branch.kv.list("input")
        branch.kv.set("output", {"nested": [1, 2]})
        token = branch.seal()
        before = branch.inspect()
        reopened = Workspace.open(self.database).get_branch("worker")
        self.assertEqual(reopened.inspect(), before)
        backup = Workspace.open(self.workspace.backup(self.root / "backup.sqlite3"))
        restored = backup.get_branch("worker")
        self.assertEqual(restored.inspect(), before)
        restored.accept(expected_proposal=token)
        self.assertEqual(backup.snapshot().kv.get("output"), {"nested": [1, 2]})
        self.assertFalse(self.workspace.snapshot().kv.exists("output"))
        update = self.workspace.branch("update")
        update.kv.set("input", 2)
        update.accept()
        self.assert_conflict(reopened, kind="stale_read", path="input")

    def test_competing_process_writers_publish_exactly_one_complete_change_set(self):
        self.seed({"same-key": {"winner": "initial"}})
        names = ["writer-a", "writer-b"]
        for name in names:
            self.workspace.branch(name)
        context = multiprocessing.get_context("spawn")
        ready, results, release = context.Queue(), context.Queue(), context.Event()
        workers = [context.Process(target=_state_writer,
                   args=(str(self.database), name, ready, release, results)) for name in names]
        try:
            for worker in workers:
                worker.start()
            for _ in workers:
                self.assertTrue(ready.get(timeout=15))
            release.set()
            outcomes = [results.get(timeout=15) for _ in workers]
            self.assertEqual(sorted(status for _, status in outcomes), ["accepted", "conflict"])
            winner = next(name for name, status in outcomes if status == "accepted")
            loser = next(name for name, status in outcomes if status == "conflict")
            for worker in workers:
                worker.join(10)
                self.assertEqual(worker.exitcode, 0)
            snapshot = self.workspace.snapshot()
            self.assertEqual(snapshot.kv.get("same-key"), {"winner": winner})
            self.assertTrue(snapshot.exists(f"{winner}.txt"))
            self.assertFalse(snapshot.exists(f"{loser}.txt"))
            self.assertTrue(self.workspace.check_integrity()["ok"])
        finally:
            for worker in workers:
                if worker.is_alive():
                    worker.terminate()
                    worker.join(5)
            for queue in (ready, results):
                queue.close()
                queue.join_thread()


if __name__ == "__main__":
    unittest.main()

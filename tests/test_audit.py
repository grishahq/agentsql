"""Tool audit is durable evidence, never transactional rollback of external effects."""

from contextlib import closing
import math
import multiprocessing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from agentsql import ConflictError, Workspace, WorkspaceError
from agentsql.audit import ToolLog


def _finish_worker(database, identifier, ready, release, results, success):
    try:
        log = ToolLog(Workspace.open(database))
        ready.put(True)
        if not release.wait(15):
            raise RuntimeError("Parent did not release competing workers")
        if success:
            log.success(identifier, {"worker": "success"})
        else:
            log.error(identifier, "competing failure")
        results.put("completed")
    except WorkspaceError:
        results.put("already completed")
    except BaseException as exc:
        results.put(repr(exc))


def _records_worker(database, worker, results):
    try:
        log = ToolLog(Workspace.open(database), worker)
        identifiers = [log.record("parallel", result=index) for index in range(10)]
        results.put(identifiers)
    except BaseException as exc:
        results.put(repr(exc))


def _interrupted_worker(database, external, results, hold):
    with ToolLog(Workspace.open(database), "worker").track("external-write") as call:
        Path(external).write_text("effect happened", encoding="utf-8")
        results.put(call.id)
        hold.wait(30)


class AuditTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        source = self.root / "source"
        source.mkdir()
        (source / "prices.csv").write_text("100", encoding="utf-8")
        self.database = self.root / "workspace.sqlite3"
        self.workspace = Workspace.create(self.database, source)
        self.worker = self.workspace.branch("worker")
        self.log = ToolLog(self.workspace, "worker")

    def test_start_finish_persists_json_events_and_statistics(self):
        with patch("agentsql.audit.time.time", side_effect=[10.0, 10.25]):
            identifier = self.log.start("lookup", {"city": "Dubai", "options": [1, True, None]})
            pending = self.log.get(identifier)
            self.assertEqual(pending["status"], "pending")
            self.assertIsNone(pending["duration_ms"])
            self.assertIsNone(self.log.stats()[0]["avg_duration_ms"])
            result = self.log.success(identifier, {"price": 120})
        self.assertEqual(result["branch"], "worker")
        self.assertEqual(result["duration_ms"], 250)
        reopened = ToolLog(Workspace.open(self.database), "worker")
        self.assertEqual(reopened.get(identifier), result)
        self.assertEqual(result["parameters"]["options"], [1, True, None])
        self.assertEqual(result["result"], {"price": 120})
        self.assertEqual([row["kind"] for row in reopened.events(identifier)],
                         ["started", "succeeded"])
        self.assertEqual(reopened.stats(), [{"name": "lookup", "total": 1, "pending": 0,
                                            "succeeded": 1, "failed": 0, "avg_duration_ms": 250}])

    def test_terminal_states_cannot_be_overwritten_even_by_same_outcome(self):
        for failed in (False, True):
            with self.subTest(failed=failed):
                identifier = self.log.start("call")
                if failed:
                    self.log.error(identifier, "problem")
                else:
                    self.log.success(identifier, "answer")
                before = self.log.get(identifier)
                for operation in (lambda: self.log.success(identifier, "replacement"),
                                  lambda: self.log.error(identifier, "replacement")):
                    with self.assertRaises(WorkspaceError):
                        operation()
                    self.assertEqual(self.log.get(identifier), before)
                self.assertEqual(len(self.log.events(identifier)), 2)

    def test_branch_scope_filters_all_reads_and_prevents_other_branch_completion(self):
        self.workspace.branch("other")
        other = ToolLog(self.workspace, "other")
        own = self.log.start("shared-name")
        foreign = other.start("shared-name")
        global_log = ToolLog(self.workspace)
        unbound = global_log.record("global")
        self.assertEqual([row["id"] for row in self.log.list()], [own])
        self.assertEqual([row["call_id"] for row in self.log.events()], [own])
        self.assertEqual(self.log.stats()[0]["total"], 1)
        for operation in (lambda: self.log.get(foreign),
                          lambda: self.log.success(foreign),
                          lambda: self.log.error(foreign, "no"),
                          lambda: self.log.events(foreign),
                          lambda: other.get(unbound)):
            with self.assertRaisesRegex(WorkspaceError, "Unknown tool call"):
                operation()
        self.assertEqual(len(global_log.list()), 3)
        self.assertEqual(global_log.success(foreign)["status"], "succeeded")
        with self.assertRaises(WorkspaceError):
            ToolLog(self.workspace, "missing")

    def test_record_success_failure_filtering_and_id_pagination(self):
        first = self.log.record("fetch", result={"ok": True}, started_at=10, completed_at=11)
        second = self.log.record("fetch", error="", started_at=10, completed_at=13)
        third = self.log.start("other")
        self.assertEqual([row["id"] for row in self.log.list(name="fetch")], [first, second])
        self.assertEqual([row["id"] for row in self.log.list(status="failed")], [second])
        self.assertEqual([row["id"] for row in self.log.list(after=first, limit=1)], [second])
        self.assertEqual([row["id"] for row in self.log.list(after=third)], [])
        first_event = self.log.events(limit=1)[0]
        remaining = self.log.events(after=first_event["id"])
        self.assertEqual(len(remaining), 4)
        self.assertTrue(all(row["id"] > first_event["id"] for row in remaining))
        summary = self.log.stats()[0]
        self.assertEqual(summary, {"name": "fetch", "total": 2, "pending": 0,
                                   "succeeded": 1, "failed": 1, "avg_duration_ms": 2000})

    def test_track_records_both_outcomes_and_does_not_hold_database_lock(self):
        payload = {"answer": [42]}
        with self.log.track("generate", {"model": "simulated"}) as call:
            self.assertEqual(ToolLog(Workspace.open(self.database)).get(call.id)["status"], "pending")
            competing = Workspace.open(self.database).branch("during-tool")
            competing.write_text("independent.md", "progress")
            competing.accept()
            call.set_result(payload)
            payload["answer"].append(99)
        self.assertEqual(self.log.get(call.id)["result"], {"answer": [42]})
        error = RuntimeError("external tool failed")
        with self.assertRaises(RuntimeError) as caught:
            with self.log.track("generate") as failed:
                failed.set_result("partial")
                raise error
        self.assertIs(caught.exception, error)
        failed_record = self.log.get(failed.id)
        self.assertEqual(failed_record["error"], repr(error))
        self.assertIsNone(failed_record["result"])
        self.assertEqual(failed_record["status"], "failed")

    def test_audit_storage_failure_preserves_actual_tool_exception(self):
        original = RuntimeError("tool failure")
        with patch.object(self.log, "error", side_effect=WorkspaceError("storage failure")):
            with self.assertRaises(RuntimeError) as caught:
                with self.log.track("call") as call:
                    raise original
        self.assertIs(caught.exception, original)
        self.assertIsInstance(caught.exception.__cause__, WorkspaceError)
        self.assertEqual(self.log.get(call.id)["status"], "pending")

    def test_track_completion_failure_is_visible_with_pending_evidence(self):
        with patch.object(self.log, "success", side_effect=WorkspaceError("storage failure")):
            with self.assertRaisesRegex(WorkspaceError, "storage failure"):
                with self.log.track("call") as call:
                    call.set_result(True)
        self.assertEqual(self.log.get(call.id)["status"], "pending")

    def test_start_and_finish_event_failures_rollback_summary_as_well(self):
        with closing(sqlite3.connect(self.database)) as db:
            db.execute("CREATE TRIGGER reject_start BEFORE INSERT ON tool_events "
                       "WHEN NEW.kind='started' BEGIN SELECT RAISE(ABORT, 'injected event failure'); END")
            db.commit()
        with self.assertRaisesRegex(WorkspaceError, "injected event failure"):
            self.log.start("never committed")
        self.assertEqual(self.log.list(), [])
        with closing(sqlite3.connect(self.database)) as db:
            db.execute("DROP TRIGGER reject_start")
            db.execute("CREATE TRIGGER reject_completion BEFORE INSERT ON tool_events "
                       "WHEN NEW.kind!='started' BEGIN SELECT RAISE(ABORT, 'injected event failure'); END")
            db.commit()
        identifier = self.log.start("pending")
        with self.assertRaisesRegex(WorkspaceError, "injected event failure"):
            self.log.success(identifier, "not committed")
        self.assertEqual(self.log.get(identifier)["status"], "pending")
        self.assertEqual(len(self.log.events(identifier)), 1)
        with self.assertRaisesRegex(WorkspaceError, "injected event failure"):
            self.log.record("never committed", result=True)
        self.assertEqual(len(self.log.list()), 1)

    def test_audit_is_not_part_of_seal_or_acceptance_and_survives_rejection(self):
        self.worker.write_text("proposal.md", self.worker.read_text("prices.csv"))
        token = self.worker.seal()
        identifier = self.log.record("email", result={"sent": True})
        self.assertEqual(self.worker.inspect()["proposal_id"], token)
        updater = self.workspace.branch("update-prices")
        updater.write_text("prices.csv", "120")
        updater.accept()
        with self.assertRaises(ConflictError):
            self.worker.accept(expected_proposal=token)
        self.assertEqual(self.log.get(identifier)["status"], "succeeded")
        accepted_log = ToolLog(self.workspace, "update-prices")
        accepted_id = accepted_log.record("cleanup")
        self.assertEqual(accepted_log.get(accepted_id)["status"], "succeeded")
        backup = Workspace.open(self.workspace.backup(self.root / "backup.sqlite3"))
        self.assertEqual(ToolLog(backup).list(), ToolLog(self.workspace).list())
        self.assertEqual(ToolLog(backup).events(), ToolLog(self.workspace).events())

    def test_invalid_json_values_and_timestamps_leave_no_partial_records(self):
        circular = []
        circular.append(circular)
        invalid_values = [float("nan"), float("inf"), {1: "bad key"}, object(),
                          (1, 2), {"bad": {1, 2}}, "\ud800", circular]
        for value in invalid_values:
            for operation in (lambda: self.log.start("call", value),
                              lambda: self.log.record("call", result=value)):
                with self.subTest(value=type(value)), self.assertRaises(ValueError):
                    operation()
        for value in (True, -1, math.inf, math.nan, "10", 10**1000):
            with self.subTest(timestamp=value), self.assertRaises(ValueError):
                self.log.record("call", started_at=value)
        for values in ({"started_at": 20, "completed_at": 10},
                       {"started_at": 0, "completed_at": 1e308},
                       {"result": True, "error": "failed"}):
            with self.assertRaises(ValueError):
                self.log.record("call", **values)
        self.assertEqual(self.log.list(), [])
        self.assertEqual(self.log.events(), [])

    def test_invalid_query_and_lifecycle_arguments(self):
        for name in (None, "", " ", "bad\nname", "x" * 201, "\ud800"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.log.start(name)
        for limit in (0, -1, True, 1001, "10"):
            for operation in (lambda: self.log.list(limit=limit),
                              lambda: self.log.events(limit=limit)):
                with self.assertRaises(ValueError):
                    operation()
        for identifier in (0, -1, True, "1", 10**100):
            for operation in (lambda: self.log.get(identifier),
                              lambda: self.log.success(identifier),
                              lambda: self.log.events(identifier)):
                with self.assertRaises(ValueError):
                    operation()
        for identifier in (-1, True, "1"):
            with self.assertRaises(ValueError):
                self.log.list(after=identifier)
        with self.assertRaises(ValueError):
            self.log.list(status="done")
        with self.assertRaises(ValueError):
            self.log.error(1, None)
        with self.assertRaises(WorkspaceError):
            self.log.get(999)

    def test_clock_adjustment_never_creates_negative_duration(self):
        with patch("agentsql.audit.time.time", side_effect=[10, 9]):
            identifier = self.log.start("call")
            completed = self.log.success(identifier)
        self.assertEqual(completed["duration_ms"], 0)
        self.assertEqual(completed["completed_at"], 10)

    def test_concurrent_terminal_transitions_have_exactly_one_winner(self):
        identifier = self.log.start("race")
        context = multiprocessing.get_context("spawn")
        ready, results, release = context.Queue(), context.Queue(), context.Event()
        workers = [context.Process(target=_finish_worker, args=(str(self.database), identifier,
                    ready, release, results, success)) for success in (True, False)]
        try:
            for worker in workers:
                worker.start()
            for _ in workers:
                self.assertTrue(ready.get(timeout=15))
            release.set()
            self.assertEqual(sorted(results.get(timeout=15) for _ in workers),
                             ["already completed", "completed"])
            for worker in workers:
                worker.join(10)
                self.assertEqual(worker.exitcode, 0)
            events = self.log.events(identifier)
            self.assertEqual(len(events), 2)
            self.assertEqual(events[-1]["kind"], self.log.get(identifier)["status"])
        finally:
            for worker in workers:
                if worker.is_alive():
                    worker.terminate()
                    worker.join(5)
            for queue in (ready, results):
                queue.close()
                queue.join_thread()

    def test_concurrent_appends_do_not_lose_calls_or_events(self):
        context = multiprocessing.get_context("spawn")
        results = context.Queue()
        names = [f"parallel-{index}" for index in range(4)]
        for name in names:
            self.workspace.branch(name)
        workers = [context.Process(target=_records_worker,
                   args=(str(self.database), name, results)) for name in names]
        try:
            for worker in workers:
                worker.start()
            recorded = [results.get(timeout=20) for _ in workers]
            self.assertTrue(all(isinstance(item, list) for item in recorded), recorded)
            identifiers = [identifier for group in recorded for identifier in group]
            self.assertEqual(len(set(identifiers)), 40)
            for worker in workers:
                worker.join(10)
                self.assertEqual(worker.exitcode, 0)
            global_log = ToolLog(self.workspace)
            self.assertEqual(len(global_log.list()), 40)
            self.assertEqual(len(global_log.events()), 80)
            self.assertEqual(global_log.stats()[0]["succeeded"], 40)
        finally:
            for worker in workers:
                if worker.is_alive():
                    worker.terminate()
                    worker.join(5)
            results.close()
            results.join_thread()

    def test_interrupted_worker_leaves_pending_evidence_even_if_external_effect_happened(self):
        context = multiprocessing.get_context("spawn")
        results, hold = context.Queue(), context.Event()
        external = self.root / "external.txt"
        worker = context.Process(target=_interrupted_worker,
                   args=(str(self.database), str(external), results, hold))
        try:
            worker.start()
            identifier = results.get(timeout=15)
            worker.terminate()
            worker.join(10)
            self.assertEqual(external.read_text(encoding="utf-8"), "effect happened")
            reopened = ToolLog(Workspace.open(self.database), "worker")
            self.assertEqual(reopened.get(identifier)["status"], "pending")
            self.assertEqual([event["kind"] for event in reopened.events(identifier)], ["started"])
        finally:
            if worker.is_alive():
                worker.terminate()
                worker.join(5)
            results.close()
            results.join_thread()


if __name__ == "__main__":
    unittest.main()

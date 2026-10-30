"""Reproduce on-call write skew with two real processes and controlled schedules.

Run with an installed AgentSQL SDK:
    python examples/on_call_race.py --rounds 10 --output on-call-results.json

Rounds alternate which prepared worker accepts first. These forced schedules
demonstrate dependency validation, not reliability, throughput, or LLM quality.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing
from pathlib import Path
import sys
import tempfile
from threading import BrokenBarrierError

from agentsql import ConflictError, Workspace


CASES = ("snapshot_reads_with_tool_log", "tracked_branch_reads",
         "tracked_plus_recompute", "independent_teams")
TIMEOUT = 20
METRICS = ("requests", "accepted_proposals", "accepted_approvals", "accepted_denials",
           "rechecks", "recomputations", "coverage_violations", "incomplete_publications")


class LabError(RuntimeError):
    """A worker, invariant oracle, or expected experiment outcome failed."""


def require(condition, message):
    if not condition:
        raise LabError(message)


def _seed(directory, independent=False):
    workspace = Workspace.create(directory / "state.sqlite3")
    seed = workspace.branch("seed")
    seed.fs.mkdir("decisions")
    specs = []
    for actor, team, peer in (("alice", "alpha", "backup"), ("bob", "beta", "backup")) if independent else (
            ("alice", "primary", "bob"), ("bob", "primary", "alice")):
        own_key, peer_key = f"roster/{team}/{actor}", f"roster/{team}/{peer}"
        seed.kv.set(own_key, True)
        seed.kv.set(peer_key, True)
        specs.append({"actor": actor, "team": team, "own_key": own_key, "peer_key": peer_key})
    seed.accept(expected_proposal=seed.seal())
    return workspace, specs


def _prepare(workspace, branch, spec, tracked):
    source = branch.kv if tracked else workspace.snapshot(branch.base_revision).kv
    call = branch.tools.start("read_on_call_roster", {
        "keys": [spec["own_key"], spec["peer_key"]],
        "read_mode": "branch" if tracked else "snapshot"})
    own, peer = source.get(spec["own_key"]), source.get(spec["peer_key"])
    require(type(own) is bool and type(peer) is bool, "Roster values must be booleans")
    branch.tools.success(call, {"requester_on_call": own, "peer_on_call": peer})
    decision = {"actor": spec["actor"], "team": spec["team"],
                "decision": "approved" if own and peer else "denied",
                "observed_requester_on_call": own, "observed_peer_on_call": peer}
    if decision["decision"] == "approved":
        branch.kv.set(spec["own_key"], False)
    branch.kv.set(f"decisions/{spec['actor']}", decision)
    branch.fs.write_file(f"decisions/{spec['actor']}.json", json.dumps(decision, sort_keys=True) + "\n")
    proposal = branch.seal()
    return {"actor": spec["actor"], "branch": branch.name,
            "base_revision": branch.base_revision, "proposal_id": proposal,
            "decision": decision["decision"], "tool_call_id": call,
            "kv_reads": branch.inspect()["kv_reads"]}


def _accept(branch, prepared):
    try:
        accepted = branch.accept(expected_proposal=prepared["proposal_id"])
        return dict(prepared, publication="accepted", revision=accepted["revision"])
    except ConflictError as error:
        return dict(prepared, publication="requires_recheck", conflicts=error.as_dict()["conflicts"])


def _worker(database, branch_name, spec, tracked, recompute, barrier, permit, sender):
    try:
        workspace = Workspace.open(database)
        branch = workspace.get_branch(branch_name)
        prepared = _prepare(workspace, branch, spec, tracked)
        barrier.wait(timeout=TIMEOUT)
        require(permit.wait(TIMEOUT), "Parent acceptance permit timed out")
        outcome = _accept(branch, prepared)
        sender.send(outcome)
        if recompute and outcome["publication"] == "requires_recheck":
            fresh = workspace.branch(f"{spec['actor']}-attempt-2")
            retry = _prepare(workspace, fresh, spec, True)
            sender.send(_accept(fresh, retry))
    except Exception as error:
        try:
            sender.send({"publication": "error", "error": type(error).__name__,
                         "message": str(error)})
        finally:
            barrier.abort()
        raise SystemExit(1) from None
    finally:
        sender.close()


def _receive(connection):
    require(connection.poll(TIMEOUT), "Worker response timed out")
    try:
        result = connection.recv()
    except EOFError as error:
        raise LabError("Worker closed its channel without an outcome") from error
    require(result.get("publication") != "error", "Worker failed: " + result.get("error", "unknown"))
    return result


def _publish_parallel(workspace, specs, case, first):
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(3)
    processes, receivers, senders, permits = {}, {}, [], {}
    outcomes, retry = [], None
    order = [first, "bob" if first == "alice" else "alice"]
    try:
        for spec in specs:
            actor = spec["actor"]
            branch = workspace.branch(f"{actor}-attempt-1")
            receiver, sender = context.Pipe(duplex=False)
            receivers[actor], permits[actor] = receiver, context.Event()
            senders.append(sender)
            process = context.Process(target=_worker, args=(str(workspace.db_path), branch.name,
                spec, case != CASES[0], case == CASES[2], barrier, permits[actor], sender))
            processes[actor] = process
            process.start()
            sender.close()
        try:
            barrier.wait(timeout=TIMEOUT)
        except BrokenBarrierError as error:
            raise LabError("Worker preparation failed before both proposals were ready") from error
        for actor in order:
            permits[actor].set()
            outcomes.append(_receive(receivers[actor]))
        if case == CASES[2]:
            rejected = [item for item in outcomes if item["publication"] == "requires_recheck"]
            require(len(rejected) == 1, "Recompute case must first reject exactly one proposal")
            retry = _receive(receivers[rejected[0]["actor"]])
        for process in processes.values():
            process.join(timeout=TIMEOUT)
            require(process.exitcode == 0, "Worker did not exit successfully")
        return outcomes, retry
    finally:
        for process in processes.values():
            if process.pid is not None and process.is_alive():
                process.terminate()
                process.join(timeout=3)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=3)
            if process.pid is not None:
                require(not process.is_alive(), "Worker could not be cleaned up")
        for connection in [*receivers.values(), *senders]:
            connection.close()


def _observe_revision(snapshot, specs):
    roster_keys = sorted({spec[key] for spec in specs for key in ("own_key", "peer_key")})
    roster = {key: snapshot.kv.get(key) for key in roster_keys}
    require(all(type(value) is bool for value in roster.values()), "Oracle found an invalid roster")
    expected = dict.fromkeys(roster_keys, True)
    decisions = {}
    for spec in specs:
        actor = spec["actor"]
        key, path = f"decisions/{actor}", f"decisions/{actor}.json"
        has_state, has_file = snapshot.kv.exists(key), snapshot.fs.exists(path)
        require(has_state == has_file, "Decision file and JSON state were only partially published")
        if has_state:
            decision = snapshot.kv.get(key)
            require(json.loads(snapshot.fs.read_file(path)) == decision, "Decision file and state disagree")
            require(decision.get("actor") == actor and decision.get("team") == spec["team"],
                    "Decision belongs to the wrong requester")
            verdict = decision.get("decision")
            require(verdict in ("approved", "denied"), "Unknown business decision")
            if verdict == "approved":
                require(decision["observed_requester_on_call"] is True and
                        decision["observed_peer_on_call"] is True, "Approval violates its observed policy")
                expected[spec["own_key"]] = False
            else:
                require(not (decision["observed_requester_on_call"] and decision["observed_peer_on_call"]),
                        "Denial contradicts its observed policy")
            decisions[actor] = verdict
    require(roster == expected, "Canonical roster disagrees with published decisions")
    coverage = {team: sum(value for key, value in roster.items() if key.startswith(f"roster/{team}/"))
                for team in sorted({spec["team"] for spec in specs})}
    return {"revision": snapshot.revision, "roster": roster, "decisions": decisions,
            "on_call_per_team": coverage, "coverage_valid": all(count >= 1 for count in coverage.values())}


def run_trial(directory, case, round_number):
    require(case in CASES, "Unknown experiment case")
    directory.mkdir()
    workspace, specs = _seed(directory, independent=case == CASES[3])
    seed_revision = workspace.snapshot().revision
    seed_state = _observe_revision(workspace.snapshot(seed_revision), specs)
    require(seed_state["coverage_valid"], "Initial roster must have coverage")
    first = "alice" if round_number % 2 else "bob"
    outcomes, retry = _publish_parallel(workspace, specs, case, first)
    accepted = [item for item in outcomes if item["publication"] == "accepted"]
    rejected = [item for item in outcomes if item["publication"] == "requires_recheck"]
    expected_accepted = 2 if case in (CASES[0], CASES[3]) else 1
    require(len(accepted) == expected_accepted and len(rejected) == 2 - expected_accepted,
            "Acceptance outcomes do not match the experiment case")
    require(all(item["decision"] == "approved" for item in outcomes), "Initial requests must both approve")
    require(all(bool(item["kv_reads"]) == (case != CASES[0]) for item in outcomes),
            "Read dependencies do not match the selected instrumentation")
    for item in rejected:
        require(any(conflict["kind"] == "stale_read" and conflict.get("resource") == "kv"
                    for conflict in item["conflicts"]), "Rejection was not a stale roster read")
    if retry is not None:
        require(retry["publication"] == "accepted" and retry["decision"] == "denied",
                "Fresh tracked recomputation must accept a denial")
        require(retry["base_revision"] > seed_revision, "Retry reused the original base")
    revisions = [_observe_revision(workspace.snapshot(entry["revision"]), specs)
                 for entry in workspace.history() if entry["revision"] > seed_revision]
    require(len(revisions) == expected_accepted + (retry is not None), "Unexpected accepted revision count")
    for item in rejected:
        before_retry = workspace.snapshot(max(result["revision"] for result in accepted))
        actor = item["actor"]
        require(not before_retry.kv.exists(f"decisions/{actor}") and
                not before_retry.fs.exists(f"decisions/{actor}.json"), "Rejected proposal leaked an artifact")
        spec = next(spec for spec in specs if spec["actor"] == actor)
        require(before_retry.kv.get(spec["own_key"]) is True, "Rejected proposal changed its roster key")
    final = revisions[-1]
    violation = any(not item["coverage_valid"] for item in revisions)
    require(violation == (case == CASES[0]), "Coverage oracle disagrees with the expected case")
    calls = workspace.tools.list()
    require(len(calls) == 2 + (retry is not None) and all(call["status"] == "succeeded" for call in calls),
            "Every policy read must have completed audit evidence")
    require(workspace.check_integrity()["ok"], "Workspace integrity check failed")
    metrics = {"requests": 2, "accepted_proposals": len(revisions),
               "accepted_approvals": sum(value == "approved" for value in final["decisions"].values()),
               "accepted_denials": sum(value == "denied" for value in final["decisions"].values()),
               "rechecks": len(rejected), "recomputations": int(retry is not None),
               "coverage_violations": int(violation), "incomplete_publications": 0}
    return {"case": case, "round": round_number, "acceptance_order": [first, "bob" if first == "alice" else "alice"],
            "seed_revision": seed_revision, "workers": outcomes, "recompute": retry,
            "metrics": metrics, "oracle": {"passed": True, "accepted_revisions": revisions,
                "seed_state": seed_state,
                "final_coverage_valid": final["coverage_valid"],
                "rejected_proposals_absent_after_race": True, "completed_tool_calls": len(calls)}}


def run_lab(rounds=10):
    if type(rounds) is not int or not 1 <= rounds <= 100:
        raise ValueError("rounds must be an integer between 1 and 100")
    trials, summary = [], {}
    with tempfile.TemporaryDirectory(prefix="agentsql-on-call-") as temporary:
        root = Path(temporary)
        for case in CASES:
            summary[case] = dict.fromkeys(METRICS, 0)
            summary[case].update(rounds=rounds, acceptance_orders={"alice_first": 0, "bob_first": 0})
            for round_number in range(1, rounds + 1):
                trial = run_trial(root / f"{case}-{round_number}", case, round_number)
                trials.append(trial)
                for metric in METRICS:
                    summary[case][metric] += trial["metrics"][metric]
                summary[case]["acceptance_orders"][trial["acceptance_order"][0] + "_first"] += 1
    return {"schema_version": 1, "experiment": "on_call_race", "status": "PASS",
            "actors": "Deterministic policy workers; no LLM calls", "rounds_per_case": rounds,
            "schedule": "Both workers prepare before parent-gated acceptance; first worker alternates each round",
            "interpretation": "Expected controlled outcomes, not reliability, throughput, or LLM-quality measurements",
            "case_order": list(CASES), "summary": summary, "trials": trials}


def _rounds(value):
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("rounds must be an integer between 1 and 100") from error
    if not 1 <= number <= 100:
        raise argparse.ArgumentTypeError("rounds must be an integer between 1 and 100")
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=_rounds, default=10, help="Trials per case, 1..100 (default: 10)")
    parser.add_argument("--output", type=Path, help="Write JSON to a new file; existing paths are refused")
    args = parser.parse_args(argv)
    try:
        if args.output is not None and (args.output.exists() or args.output.is_symlink()):
            raise ValueError("Output must be a new file; existing paths are never overwritten")
        report = run_lab(args.rounds)
        encoded = json.dumps(report, indent=2, allow_nan=False) + "\n"
        if args.output is not None:
            with args.output.open("x", encoding="utf-8") as stream:
                stream.write(encoded)
        print(encoded, end="")
        return 0
    except Exception as error:
        print(json.dumps({"status": "FAIL", "error": type(error).__name__, "message": str(error)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

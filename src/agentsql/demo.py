"""A deterministic conflict-and-recompute example using simulated agent workers."""

from __future__ import annotations

import csv
import io
import json
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Callable

from .core import Branch, ConflictError, Workspace


def _prepare_proposal(branch: Branch) -> str:
    """Simulated worker: derive an output from a tracked workspace read."""
    prices = list(csv.DictReader(io.StringIO(branch.read_text("prices.csv"))))
    unit_price = Decimal(prices[0]["unit_price"])
    quantity = 3
    proposal = (
        "# Purchase proposal\n\n"
        f"Buy {quantity} {prices[0]['item']} units at ${unit_price:.2f} each.\n"
        f"Total: ${unit_price * quantity:.2f}.\n"
    )
    branch.write_text("proposal.md", proposal)
    return proposal


def _run(directory: Path, emit: Callable[[str], None]) -> dict:
    source = directory / "initial-files"
    source.mkdir()
    (source / "prices.csv").write_text("item,unit_price\nwidget,100.00\n", encoding="utf-8")
    workspace = Workspace.create(directory / "workspace.sqlite3", source)
    emit("Simulated agent workers: deterministic local code; no model or API key.")

    agent_a = workspace.branch("agent-a")
    initial_proposal = _prepare_proposal(agent_a)
    emit("Agent A reads prices.csv at version 1 and stages proposal.md ($300.00 total).")

    agent_b = workspace.branch("agent-b")
    agent_b.write_text("prices.csv", "item,unit_price\nwidget,120.00\n")
    accepted_b = agent_b.accept(message="Simulated Agent B updates the unit price")
    emit(f"Agent B updates prices.csv; accepted at revision {accepted_b['revision']}.")

    try:
        agent_a.accept(message="Simulated Agent A submits its initial proposal")
    except ConflictError as error:
        conflict = error.as_dict()
    else:
        raise AssertionError("The stale proposal was accepted")
    assert any(
        item["kind"] == "stale_read" and item["path"] == "prices.csv"
        for item in conflict["conflicts"]
    ), "The rejection must identify the tracked prices.csv input"
    assert not workspace.snapshot().exists("proposal.md"), "Rejection changed shared state"
    emit("Agent A requires rechecking: its tracked input changed, despite a different output path.")
    emit(json.dumps(conflict, indent=2, sort_keys=True))
    emit("Shared state still contains no proposal.md. An input change alone does not prove an answer wrong.")

    refreshed_a = workspace.branch("agent-a-refreshed")
    updated_proposal = _prepare_proposal(refreshed_a)
    accepted_a = refreshed_a.accept(message="Simulated Agent A recomputes against current prices")
    assert workspace.snapshot().read_text("proposal.md") == updated_proposal
    assert "$360.00" in updated_proposal and "$300.00" in initial_proposal
    emit(f"Agent A refreshes and recomputes ($360.00 total); accepted at revision {accepted_a['revision']}.")
    emit("Demo passed: stale output rejected atomically, refreshed output accepted.")
    return {"conflict": conflict, "accepted_b": accepted_b, "accepted_a": accepted_a}


def run_demo(
    directory: str | Path | None = None, *, emit: Callable[[str], None] = print
) -> dict:
    """Run the example, optionally keeping all files in a new or empty directory."""
    if directory is None:
        with tempfile.TemporaryDirectory(prefix="agentsql-demo-") as temporary:
            return _run(Path(temporary), emit)
    destination = Path(directory)
    if destination.exists() and (not destination.is_dir() or any(destination.iterdir())):
        raise ValueError("The demo directory must be new or empty")
    destination.mkdir(parents=True, exist_ok=True)
    result = _run(destination, emit)
    emit(f"Demo workspace retained at {destination.resolve() / 'workspace.sqlite3'}")
    return result


if __name__ == "__main__":
    run_demo()

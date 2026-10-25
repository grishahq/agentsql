"""An application integration with deterministic simulated agent workers.

Run after installing agentsql:
    python examples/project_integration.py
    python examples/project_integration.py --directory ./integration-output

The preserved directory must be new or empty. No model or network is used.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Callable

from agentsql import ConflictError, Workspace


def simulated_model(prices_text: str) -> str:
    """Replace this callable with your own model or ordinary application code."""
    row = next(csv.DictReader(io.StringIO(prices_text)))
    quantity = 3
    unit_price = Decimal(row["unit_price"])
    return (
        "# Purchase proposal\n\n"
        f"Buy {quantity} {row['item']} units at ${unit_price:.2f} each.\n"
        f"Total: ${quantity * unit_price:.2f}.\n"
    )


def prepare_attempt(
    database: Path, branch_name: str, generate: Callable[[str], str]
) -> str:
    """One worker owns this branch and seals it before returning its name."""
    workspace = Workspace.open(database)
    branch = workspace.branch(branch_name)
    prices = branch.read_text("prices.csv")
    # The SDK read has finished its transaction before the model is called.
    # This callable gets the tracked bytes, not a raw source-directory path.
    proposal = generate(prices)
    branch.write_text("proposal.md", proposal)
    branch.seal()  # Freeze outputs and tracked inputs before coordinator review.
    return branch.name


def publish_with_recompute(
    database: Path,
    task_id: str,
    first_branch_name: str,
    generate: Callable[[str], str],
    *,
    max_attempts: int = 3,
    emit: Callable[[str], None] = print,
) -> dict:
    """Coordinator reviews each attempt, accepting only a current whole result.

    A production coordinator can put a human or application review policy here.
    The first branch is already prepared. A conflict triggers a new computation;
    exhausting the attempt budget raises the last ConflictError to the caller.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")
    workspace = Workspace.open(database)
    branch_name = first_branch_name
    for attempt in range(1, max_attempts + 1):
        branch = workspace.get_branch(branch_name)
        proposal_id = branch.seal()  # Idempotent: returns the worker's frozen token.
        emit(f"Coordinator reviews sealed proposal {proposal_id} from {branch_name}.")
        emit(json.dumps(branch.inspect(), indent=2, sort_keys=True))
        emit(branch.diff())
        try:
            accepted = branch.accept(
                message=f"Publish purchase task {task_id}, attempt {attempt}",
                expected_proposal=proposal_id,
            )
        except ConflictError as error:
            emit(json.dumps(error.as_dict(), indent=2, sort_keys=True))
            if attempt == max_attempts:
                raise
            emit("Recompute from fresh tracked inputs on a new branch.")
            branch_name = prepare_attempt(
                database, f"{task_id}-attempt-{attempt + 1}", generate
            )
        else:
            return accepted
    raise AssertionError("Unreachable: every attempt accepts, retries, or raises")


def _run(directory: Path, emit: Callable[[str], None]) -> dict:
    inputs = directory / "inputs"
    inputs.mkdir()
    (inputs / "prices.csv").write_text("item,unit_price\nwidget,100.00\n", encoding="utf-8")
    database = directory / "workspace.sqlite3"
    # The coordinator imports once. All subsequent workers reopen the same DB.
    Workspace.create(database, inputs)
    emit("Simulated agent workers: local application code, no model or API key.")

    task_id = "purchase-001"  # Real applications allocate a unique ID for each task.
    first_branch = prepare_attempt(database, f"{task_id}-attempt-1", simulated_model)
    emit("Worker prepared a $300.00 proposal using the captured prices.")

    # An explicit ingestion branch represents new data from the application.
    updater = Workspace.open(database).branch("price-import-001")
    updater.write_text("prices.csv", "item,unit_price\nwidget,120.00\n")
    price_update = updater.accept(message="Import updated prices")
    emit(f"Price updater accepted revision {price_update['revision']} before proposal review.")

    accepted = publish_with_recompute(database, task_id, first_branch, simulated_model, emit=emit)
    workspace = Workspace.open(database)
    pinned = workspace.snapshot(accepted["revision"])
    proposal = pinned.read_text("proposal.md")
    assert pinned.revision == 3
    assert "$360.00" in proposal
    assert workspace.snapshot(1).read_text("prices.csv") == "item,unit_price\nwidget,100.00\n"
    assert (inputs / "prices.csv").read_text(encoding="utf-8") == "item,unit_price\nwidget,100.00\n"
    old_attempt = workspace.get_branch(first_branch).inspect()
    assert old_attempt["status"] == "open"  # Failed attempts remain inspectable.
    assert old_attempt["sealed"] is True  # Its reviewed content cannot change.
    assert old_attempt["reads"] == [{"path": "prices.csv", "version": 1}]

    # A single explicit export for another application. This is an ordinary
    # filesystem write, separate from SQLite acceptance; not a multi-file commit.
    export = directory / "accepted-proposal.md"
    export.write_bytes(pinned.read_bytes("proposal.md"))
    assert export.read_text(encoding="utf-8") == proposal
    emit(f"Accepted revision {pinned.revision}; proposal total is $360.00.")
    emit("Original inputs stay unchanged; accepted bytes were explicitly exported.")
    emit("Integration passed: reopen, tracked reads, review, bounded recompute, and pinned export.")
    return {"accepted": accepted, "database": str(database), "export": str(export)}


def run_example(
    directory: str | Path | None = None, *, emit: Callable[[str], None] = print
) -> dict:
    if directory is None:
        with tempfile.TemporaryDirectory(prefix="agentsql-integration-") as temporary:
            return _run(Path(temporary), emit)
    destination = Path(directory).expanduser()
    if destination.is_symlink() or (
        destination.exists() and (not destination.is_dir() or any(destination.iterdir()))
    ):
        raise ValueError("The integration directory must be new or empty, and not a symlink")
    destination.mkdir(parents=True, exist_ok=True)
    result = _run(destination.resolve(), emit)
    emit(f"Workspace retained at {result['database']}")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, help="Keep files in a new or empty directory")
    args = parser.parse_args()
    try:
        run_example(args.directory)
    except (OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

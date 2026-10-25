# Use AgentSQL in your project

Keep your existing agent runner and model client. Route the files used by a task
through a workspace branch, pass the returned content to your application, and
publish its proposed files only after review and version checks. The MVP is a
Python SDK with a CLI; it does not automatically intercept an agent's tools.

## Install into another project

Install the alpha release into your application's own virtual environment,
using Python 3.10 or later on macOS/Linux:

```sh
mkdir -p ~/workspace-integration-app
cd ~/workspace-integration-app
python3 -m venv .venv
. .venv/bin/activate
python -m pip install agentsql==0.3.0a1
python -c 'from agentsql import Workspace; print("SDK installed")'
agentsql --help
```

To install a source checkout, use
`python -m pip install /absolute/path/to/agentsql`. Replace the absolute
checkout path when installing on another computer. Use
`python -m pip install -e /absolute/path/to/agentsql` only when you want
the application's environment to follow your local source edits.

To hand a versioned installable artifact to a teammate, build a wheel locally:

```sh
python -m pip wheel --no-deps /absolute/path/to/agentsql --wheel-dir ./dist
```

Share `dist/agentsql-0.3.0a1-py3-none-any.whl`. In the teammate's application
environment, install their downloaded file:

```sh
python -m pip install /absolute/path/to/agentsql-0.3.0a1-py3-none-any.whl
```

Building may download standard Python build tooling. The installed runtime has
no third-party dependencies and requires no service, API key, or account.
Share a specific wheel or source commit with the application team so everyone
uses the same SDK version. This does not create or publish a remote repository.

## Run the complete application example

After installation, run this checkout's example from any working directory:

```sh
python /absolute/path/to/agentsql/examples/project_integration.py
python /absolute/path/to/agentsql/examples/project_integration.py --directory ./integration-output
```

The second command keeps files in a **new or empty** directory. The example:

1. Imports `inputs/prices.csv` once into a database outside the input directory.
2. Reopens that database in a worker, reads prices through its own branch, calls
   a deterministic simulated model, and stages `proposal.md` with a $300 total.
3. Accepts a price update from a separate ingestion branch.
4. Seals the first worker's proposal, inspects its tracked reads and diff, then receives structured
   `requires_recheck` evidence when trying to accept its stale proposal.
5. Recomputes on a new branch, accepts the $360 proposal, opens the exact accepted
   revision, and exports one file. The original input directory stays unchanged.

The [example source](../examples/project_integration.py) contains reusable
`prepare_attempt` and `publish_with_recompute` functions. Its simulated model is
an ordinary callable, with no framework dependency. The attempt budget is three;
if inputs keep changing, the last `ConflictError` reaches the caller. The
coordinator must then report or reschedule the task instead of retrying forever.

## Connect a Python worker

Bootstrap once, while writers to your small input directory are paused. Do this
in the coordinator before starting workers; do not race several `create` calls.
Choose a dedicated `inputs` directory rather than your whole project root:
capture imports every file and does not honor `.gitignore` or provide exclusion
patterns. Avoid importing your virtual environment, repository metadata, or
unrelated files.

```python
from pathlib import Path
from agentsql import Workspace

inputs = Path("inputs").resolve()  # Existing small directory of task inputs.
database = Path("workspace.sqlite3").resolve()  # Outside inputs.
workspace = Workspace.create(database, inputs)
```

On subsequent runs use `Workspace.open(database)`. Give every worker the same
absolute database path and a unique task/attempt branch name. Each worker can
run in its own local process; branch state and recorded dependencies persist
between SDK calls and process restarts.

```python
from uuid import uuid4
from agentsql import ConflictError, Workspace

workspace = Workspace.open(database)
task_id = f"proposal-{uuid4().hex}"

def your_existing_model_call(prices_text: str) -> str:
    # Replace this deterministic stand-in with your model client's invocation.
    return "# Draft proposal\n\nInput prices:\n" + prices_text

for attempt in range(1, 4):
    branch = workspace.branch(f"{task_id}-attempt-{attempt}")
    prices = branch.read_text("prices.csv")  # This records the input version.
    proposal = your_existing_model_call(prices)  # No DB transaction remains open.
    branch.write_text("proposal.md", proposal)

    proposal_id = branch.seal()  # Freeze writes and dependencies before review.
    print(branch.inspect())
    print(branch.diff())
    # Apply your application's review policy here before accepting.
    try:
        result = branch.accept(message=f"Publish {task_id}", expected_proposal=proposal_id)
    except ConflictError as error:
        print(error.as_dict())
        if attempt == 3:
            raise
        # The next iteration reads fresh inputs and calls the model again.
    else:
        break

accepted = workspace.snapshot(result["revision"])
print(accepted.read_text("proposal.md"))
```

Acceptance checks tracked dependencies and write conflicts atomically. It does
not judge the quality of the output. A stale input means rechecking is required,
not that the result is necessarily wrong. Creating a fresh branch and copying
the old output into it is **not** a recheck; recompute from fresh tracked reads.

Use `branch.read_text`, `read_bytes`, `exists`, and `list_files` inside every
relevant model tool. Stage outputs with `branch.write_text`, `write_bytes`, or
`delete`. For a tool-calling agent, expose wrappers bound to its branch in that
framework's tool registry. Listing names tracks directory membership; read each
file whose contents influence the result. Snapshot reads are for inspection and
do not record dependencies for a worker.

If a model sees data from raw `Path.read_text`, shell commands, web APIs, another
database, or a framework's default filesystem tool, this SDK cannot validate
those external dependencies. Import external data deliberately into the
workspace and read it through the branch when its version must be checked.
The [MCP stdio server](platform.md) exposes branch-bound file/state tools for
clients that support MCP. It does not observe tools that bypass that server.

Keep one writer per branch. Call `seal()` before review and pass its returned
proposal ID as `expected_proposal` when accepting. Sealing is persistent and
idempotent; it permanently blocks new tracked reads, listings, writes, and
deletions. `inspect` and `diff` remain available, including after a conflict.
`inspect`/`diff` alone do not seal a branch. Legacy acceptance without a token
uses the whole current change set, so use the sealed-token workflow when review
and acceptance happen in separate steps. Compute independent outputs that need
independent approval in separate branches. A branch is cooperative isolation of file state,
not a security sandbox for untrusted code. Your application owns consistency
between generated intermediate files and their downstream outputs within one
branch; changing an intermediate does not automatically regenerate anything
that already consumed it.

## Ingest updates and retrieve results

The database is authoritative. Source files are imported once; neither editing
the source directory nor accepting a branch synchronizes the other location.
The application explicitly ingests later updates:

```python
from uuid import uuid4

update = workspace.branch(f"price-ingest-{uuid4().hex}")
update.write_text("prices.csv", "item,unit_price\nwidget,120.00\n")
proposal_id = update.seal()
update.accept(message="Import refreshed prices", expected_proposal=proposal_id)
```

The upstream collection itself is outside this SDK's tracking. If ingestion
conflicts with another writer, reconcile or re-fetch it according to the source
system's rules; do not repeatedly overwrite by blindly copying an older value.

Pin the returned revision when a downstream application needs the exact accepted
result. Calling `workspace.snapshot()` instead pins whichever revision is latest
at that call. Both snapshots remain stable while other workers publish.

```python
from pathlib import Path

pinned = workspace.snapshot(result["revision"])
proposal_bytes = pinned.read_bytes("proposal.md")
Path("accepted-proposal.md").write_bytes(proposal_bytes)
print(pinned.revision, pinned.version("proposal.md"))
print(workspace.history())
```

This is an explicit single-file export that replaces that destination file if it
exists. It is an ordinary filesystem write outside the acceptance transaction;
it is not an atomic multi-file filesystem export. Database history does not undo
model API charges, sent messages, or other external effects. Keep retryable model
work separate from irreversible external actions in your application.

## Use the CLI from another language

A JavaScript, Go, or shell coordinator can invoke the CLI as a child process.
Use the executable from the Python environment where you installed this package.
For a new database and an existing `./inputs` directory:

```sh
agentsql -w workspace.sqlite3 create ./inputs
agentsql -w workspace.sqlite3 branch task-001-attempt-1
agentsql -w workspace.sqlite3 read task-001-attempt-1 prices.csv > tracked-prices.csv
```

Pass the returned bytes to your normal model client and save the model's result
as `draft.md`. Then stage and review it:

```sh
agentsql -w workspace.sqlite3 write task-001-attempt-1 proposal.md --from-file draft.md
proposal_id=$(agentsql -w workspace.sqlite3 seal task-001-attempt-1 | python -c 'import json,sys; print(json.load(sys.stdin)["proposal_id"])')
agentsql -w workspace.sqlite3 inspect task-001-attempt-1
agentsql -w workspace.sqlite3 diff task-001-attempt-1
agentsql -w workspace.sqlite3 accept task-001-attempt-1 --proposal "$proposal_id" -m 'Publish proposal'
agentsql -w workspace.sqlite3 history
agentsql -w workspace.sqlite3 show proposal.md
```

`read` records its dependency persistently even though each invocation is a new
process. `show` is an untracked snapshot read. For an exact accepted version, use
`show proposal.md --revision N`, substituting the `revision` returned by `accept`.
Metadata is JSON on stdout. A conflict is JSON on stderr with exit status **2**;
other operation errors use **1**, success uses **0**. Invalid arguments also exit
**2** with an `error` of `UsageError`; an interrupt exits **130**. A mismatched
review token emits `proposal_mismatch` and changes nothing. `check` emits a
structured integrity report on stdout and exits **1** if it finds corruption. Check the exit status before
using a result. On a conflict create a unique fresh branch, repeat tracked reads
and computation, and bound retries. Never treat exit status 2 as successful
publication. Parallel workers should also keep their temporary input and output
files separate.

## Deployment boundary

Use small workspaces on one machine with a local SQLite database: import loads
all input bytes into memory, file reads return complete byte strings, and
retained history grows. Keep the database outside the imported directory. Do
not put the live database on a network drive or in an asynchronous sync folder.
Several processes may share that local database, but this release does not
provide distributed or remote-worker consistency, access control, shared-memory
search, automatic semantic merges, or deterministic model replay. Framework
adapters beyond MCP, cross-agent handoffs, and remote collaboration are later work.

For lock timeouts, safe backup/restore, schema migration, and recovery after a
lost acceptance response, follow the [operations guide](operations.md).

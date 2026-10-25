# Operate a local workspace

The supported deployment is a small workspace used by cooperating processes on
one machine, backed by a local SQLite database. The SDK includes transactional
publication, immutable review tokens, backup, and integrity checks. Qualify it
against your workload and storage before deployment. It does not provide an
availability SLA, distributed coordination, storage quotas, garbage collection,
or access control.

## Review exactly what will be published

Give each worker its own branch. Let it complete all tracked reads and stage all
outputs, then seal before review:

```python
from agentsql import ConflictError, Workspace

workspace = Workspace.open("workspace.sqlite3", timeout=5.0)
branch = workspace.get_branch("task-001")
proposal_id = branch.seal()
print(branch.inspect())
print(branch.diff())
# Apply your application's review policy before the next call.
try:
    accepted = branch.accept(message="Approved task-001", expected_proposal=proposal_id)
except ConflictError as error:
    print(error.as_dict())
    raise  # Schedule fresh tracked reads and recomputation on a new branch.
```

`seal()` returns a persisted deterministic ID for the complete proposal; calling
it again returns the same ID. Sealing blocks new tracked reads, existence checks,
listings, writes, and deletions. Inspect/diff remain available. A failed stale
publication leaves the sealed branch available as evidence. There is no unseal;
a changed computation needs a fresh branch. The ID is not a signature, user
identity, or review authorization.

`accept(expected_proposal=...)` verifies that ID, dependencies, and conflicting
writes in the publication transaction. A missing seal or wrong ID raises
`ProposalMismatchError`, publishes nothing, and signals a review workflow error.
A stale input raises `ConflictError` with `requires_recheck` evidence. Rechecking
is required even if the output path differs from the changed input. An input
change alone does not establish that the answer is wrong.

Legacy `accept()` without a token still accepts the current whole branch. It
does not bind acceptance to an earlier inspection. Do not use it when your
application requires review of a specific frozen proposal. Partial acceptance is
unsupported: the SDK tracks dependencies for the whole computation, not a proven
dependency graph for each output.

## Contention and retry policy

```sh
agentsql -w workspace.sqlite3 --timeout 5 check
```

Place `--timeout` before the command. `Workspace.open(..., timeout=5.0)` and
`Workspace.create(..., timeout=5.0)` configure the SDK's lock wait in seconds.
Values must be finite and between 0 and 2147483.647 seconds (SQLite's signed
millisecond limit); zero means fail immediately on contention. The default is
30 seconds. This is a SQLite lock timeout, not an end-to-end task
deadline. A long-running model call holds no SDK database transaction.

The SDK raises `WorkspaceBusyError` when lock contention exhausts the timeout.
Its structured evidence contains `error: "workspace_busy"` and `retryable: true`.
The CLI emits that evidence on stderr and exits 1. Bound application retries with
backoff; a database lock timeout is separate from a stale-input conflict, which
needs fresh computation. Writers serialize on the local database. Do not keep
raw SQL transactions open across model calls or external interactions.

## Backup and verify

```sh
agentsql -w workspace.sqlite3 backup ./backup-2026-10-23.sqlite3
agentsql -w ./backup-2026-10-23.sqlite3 check
```

Or use `workspace.backup(destination)` and `workspace.check_integrity()`. Backup
uses SQLite's online backup API to copy a coherent committed database while
other clients are running. Its destination must be new: existing files,
directories, symlinks, and paths with existing SQLite sidecars are rejected
instead of overwritten. Unknown sidecars are not automatically deleted. It includes
accepted history, pending branches, dependencies, and sealed proposals. A backup
has its own independent history after creation; it is not live replication.

Do not copy only the live `.sqlite3` file with ordinary filesystem tools.
SQLite journal/WAL sidecars can hold committed state while clients are running.
Use the SDK backup API or a proper external filesystem snapshot that preserves
SQLite consistency. Preserve enough free disk space for a complete backup and
for growing retained history. Schedule backups and retention in your deployment;
the SDK does not run a background scheduler.

`check_integrity()` checks SQLite structure, foreign key references, workspace
pointers, and stored content hashes. Its report includes `ok` and `issues`.
`check` exits 0 for an `ok` report and 1 for a failed report. If the database is
too damaged or has an invalid header/head pointer to open, it emits an operation
error on stderr and exits 1. An integrity check does not repair corruption,
authenticate an untrusted database, or establish correctness of agent outputs.
Keep backups on storage with the access permissions appropriate for their file
contents; they include historical versions, including deleted data.

## Restore and recover

To restore a backup made by this release, stop the workers that will use the
restored database. Back up the saved database to a **new** database path, verify it, then
configure the coordinator and all workers to open that same path. For example:

```python
from pathlib import Path
from agentsql import Workspace

backup = Path("backup-2026-10-23.sqlite3")
restored = Path("restored-workspace.sqlite3")
Workspace.open(backup).backup(restored)  # Refuses existing paths and sidecars.
workspace = Workspace.open(restored)
report = workspace.check_integrity()
if not report["ok"]:
    raise RuntimeError(report)
```

Preserve pre-upgrade version 1 backups with older SQLite tooling: opening one
with this release migrates that saved file too. Do not replace a database under
running clients, mix its sidecars with another
copy, or edit revision pointers manually. A restored database represents the
backup's state. Accepted results after that backup need application-level
reconciliation. Restoring files does not undo external API charges, messages,
side effects, or the state of an OS process.

SQLite rolls back interrupted uncommitted transactions. Publication validation
and all accepted file changes share one transaction, so a failed publication
cannot leave a subset accepted. File capture creates a private database before
publishing its final database path; interrupted capture may leave a private
temporary artifact, but must not expose a usable half-created workspace at the
requested path. Inspect unexpected leftovers only after stopping the failed
creator, then remove them using ordinary filesystem administration.

A process may commit successfully and then lose its output response, receive an
interrupt, or encounter a broken pipe. Exit status is not proof of rollback in
that case. Reopen the database and inspect the branch's `status`,
`accepted_revision`, and `history()` before deciding whether to retry. An
accepted branch cannot publish a second time; use its accepted revision as the
application's durable result reference. Keep irreversible external effects out
of automatic recomputation retries or make them independently idempotent.

## Schema upgrades

This release uses schema version 3. Opening a version 1 or 2 workspace upgrades
it in one transaction. Version 3 adds directories, versioned JSON state, and
tool history. Existing accepted history, tracked inputs, pending branches, and
version 2 sealed proposal IDs are retained. Parent directories are inferred from
legacy files and pending outputs; old captures cannot recover empty directories
that were never stored. State and tool history initially start empty. Coordinate
the upgrade: stop older clients, take a backup using
SQLite's online backup API or a fully closed and checkpointed database copy
made with tooling supported by the older release, install the same SDK version
for all clients, then open and
check the database with the new release before restarting workers.

There is no reverse migration. Older SDKs cannot open the newer schema. For a
rollback, use the saved pre-upgrade backup at a new path with the old SDK and
reconcile later work explicitly. Unknown schema versions are rejected instead
of being guessed or rewritten.

## Input and scale boundaries

Capture requires macOS/Linux filesystem APIs and a quiescent source directory.
It checks for observed changes, rejects symlinks and special files, and imports
all files. It does not apply `.gitignore` or provide exclusion patterns. Use a
dedicated input directory, keep the database outside it, and budget memory for
all captured file bytes. Reads return complete files; revisions retain a full
path index and history grows without automatic compaction.

Only SDK/CLI branch operations are tracked. Files read with an ordinary shell,
an agent's default filesystem tool, external databases, or remote APIs are
outside that dependency contract. Import data deliberately when its version
must be checked. One worker owns the internal consistency of intermediate and
final outputs on its branch; the SDK does not regenerate downstream outputs when
an intermediate changes.

Use a local filesystem with reliable SQLite locks and durability. Network drives
and asynchronously synchronized live databases are unsupported. Test with your
actual disk and process topology, monitor disk space and lock contention, and
protect database/backup files with OS permissions. This cooperative API does not
sandbox untrusted agents or database writers. Hardware/power-loss durability is
bounded by SQLite, the OS, and storage behavior; process crash tests alone do not
verify every filesystem or hardware failure mode.

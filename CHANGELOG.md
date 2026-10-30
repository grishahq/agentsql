# Changelog

## Unreleased

- Added an on-call race experiment using the published `0.3.0a1` SDK. It compares
  untracked snapshot reads plus tool history with tracked branch reads, fresh
  recomputation, and independent teams using real local worker processes.
- Added checks of the application invariant after every accepted worker proposal,
  rejected-proposal atomicity, and both acceptance orders, with a JSON report.
- Recorded 400 controlled trials, 100 per case with balanced acceptance orders.
  The snapshot-read baseline lost coverage in all 100 trials; the tracked,
  recomputed, and independent-team cases had zero coverage violations.
- Documented the difference between audit history and tracked dependencies,
  plus proposed experiments for integration coverage, conflict cost, and
  capacity and recovery. No runtime API or package version changed.

## 0.3.0a1 - 2026-10-25

First public alpha release as AgentSQL. Earlier versions were local development builds.
The Python import, CLI command, and distribution name are all `agentsql`.

- Added a versioned file-tree interface with empty directories, metadata,
  directory listings, recursive removal, atomic rename, and checked tree import.
- Added JSON key-value state with tracked reads, absence/tombstones, prefix
  dependencies, and atomic acceptance together with files and directories.
- Added persistent tool-call lifecycle, event history, statistics, and explicit
  branch-scoped audit records independent of acceptance and external effects.
- Added named sessions, provenance-preserving forks, and historical bases.
- Added branch-bound MCP stdio tools and command execution in a temporary
  materialized tree with bounded output, timeouts, and atomic checked import.
- Added CLI file/state/tool commands, MCP serving, and command execution.
- Added transactional schema 1/2 → 3 migration, preserving old sealed IDs.
- Corrected macOS timeout cleanup to signal a command's process group once,
  reap its leader, and preserve execution failures during resource cleanup.

This covers the common AgentFS-style files/state/tool-history workflow. It is
not AgentFS format/API compatibility or full POSIX parity. Native mounting,
OS process sandboxing, links/permissions, encryption, and cloud sync are not
implemented. The command runner runs with the caller's host privileges.

## 0.2.0 - 2026-10-23

- Added immutable proposal sealing and acceptance bound to a reviewed proposal
  ID. Legacy unsealed acceptance remains available.
- Added SQLite online backups, deep storage/provenance checks, structured lock
  timeouts, and transactional schema 1 → 2 migration.
- Initialize databases privately and publish without overwriting an existing
  destination. Reject orphaned SQLite sidecars during creation and backup.
- Hardened CLI errors, interrupted commands, closed pipes, and binary I/O.
- Added process-death, eight-publisher contention, SQLite-full rollback,
  migration, backup, review, and fixed-seed model regression tests.
- Added Linux/macOS CI, static checks, subprocess coverage, and an offline
  wheel/source-distribution verification command with checksums and evidence.
- Added operations guidance and a reproducible local workload benchmark.

Schema 2 is not readable by the original 0.1.0 SDK. Preserve a consistent
pre-upgrade backup and coordinate client upgrades; no reverse migration is
provided. This release supports small cooperative workspaces on one local
macOS/Linux machine. Storage growth, quotas, garbage collection, remote
consistency, access control, and external-side-effect rollback remain outside
the implementation.

## 0.1.0

Initial local MVP: file capture, immutable snapshots, isolated branches, tracked
inputs, diffs, atomic whole-branch publication, conflict evidence, accepted
history, CLI, and deterministic examples. No public release was made.

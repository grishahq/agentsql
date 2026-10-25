# Contributing

Write code, comments, docstrings, CLI messages, examples, and documentation in
English. Non-English text is appropriate as intentional test data, for example
when checking Unicode filenames and content.
Use a colon, parentheses, or a normal hyphen instead of an em dash in code,
documentation, release notes, and public posts.

Use Python 3.10 or later on macOS or Linux. From the checkout root:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
python -m ruff check .
python -m mypy
python -m coverage erase
python -m coverage run -m unittest discover -s tests -v
python -m coverage combine
python -m coverage report
python tools/verify_release.py
```

Coverage uses the checkout root from the shell's `PWD` so spawned workers and
CLI subprocesses executing in temporary directories measure the same source.
Run these commands from the checkout root with an editable development install.
Release verification separately installs a built wheel offline in a clean
environment and tests the source archive from outside the checkout.

Keep runtime dependencies in the Python standard library. Add regression tests
for changes to publication, capture, paths, migration, locking, or provenance.
Tests must use bounded synchronization for concurrent workers and clean up
processes and database connections. Fixed-seed model tests should compare with
an independent state model. Process termination tests exercise crash recovery;
they do not simulate hardware power failure.

Preserve these contracts:

- Validate every tracked base read and every write version in the publication
  transaction. Publish the complete change set or none of it.
- Preserve immutable revisions, tombstones, and accepted provenance.
- Freeze both writes and dependencies when sealing; validate the supplied
  proposal ID in the same transaction as acceptance.
- Never silently replace a database, reuse unknown SQLite sidecars, weaken path
  checks, or claim that SDK-bypassing file operations were tracked.
- State capture, local consistency, and external-effect boundaries explicitly.

Changes to the schema need a transactional migration and failure/reopen tests.
Changes to public SDK or CLI behavior need updated examples and documentation.
No public upload is part of the release verification command. Review its JSON
evidence and checksums before distributing a wheel or source archive.

## Publish a release

The manually triggered `release.yml` workflow verifies the SDK on Linux and
macOS, builds and checks both distributions, and publishes through PyPI Trusted
Publishing. The PyPI publisher must trust owner `grishahq`, repository
`agentsql`, workflow `release.yml`, and GitHub environment `pypi`.
For the first upload, create a pending publisher for PyPI project `agentsql`.
Configure that publisher before running the workflow. No long-lived PyPI token
is required.

Update the package version, `__version__`, changelog, and documented install
commands together. The workflow's exact distribution filenames must match that
version. Run it from the reviewed `main` commit using the Actions tab. A normal
push or pull request does not publish a package. Only the final isolated upload
job has permission to request a short-lived publishing identity.

"""Run a cooperating command against a materialized, conservatively tracked tree.

This is not a security sandbox or syscall interceptor. Commands have the host
user's permissions, environment, network access and possible external effects.
Only successful, stable regular-file/directory output trees are staged back.
"""

from __future__ import annotations

import base64
import math
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import tempfile
import time
from typing import IO

from .core import InvalidPathError, Workspace, WorkspaceError, _path, _signature

MAX_OUTPUT_BYTES = 128 * 1024  # Per stream; drain the rest without retaining it.
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_TREE_BYTES = 256 * 1024 * 1024
MAX_TREE_ENTRIES = 10_000
MAX_COMMAND_TIMEOUT = 2_147_483.647


def _failure_details(error: Exception) -> dict:
    return (error.as_dict() if isinstance(error, WorkspaceError)
            else {"error": type(error).__name__, "message": str(error)})


def _kill_group(process: subprocess.Popen) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _stop_process(process: subprocess.Popen) -> None:
    # Signal once, then reap. On
    # Darwin, signaling an already-killed group containing only zombies can
    # return EPERM, so duplicate cleanup signals are not harmless.
    try:
        _kill_group(process)
    except BaseException:
        # A genuine group permission failure must stay visible and cannot make
        # cleanup wait forever. Try the owned leader separately, then wait only
        # briefly if it too cannot be stopped. Descendants may remain alive.
        try:
            process.kill()
        except OSError:
            pass
        try:
            process.wait(timeout=1.0)
        except subprocess.TimeoutExpired:
            pass
        raise
    process.wait()


def _run_process(command: list[str], directory: Path, timeout: float,
                 environment: dict[str, str] | None) -> dict:
    started = time.monotonic()
    streams: dict[str, bytearray] = {"stdout": bytearray(), "stderr": bytearray()}
    totals = {"stdout": 0, "stderr": 0}
    timed_out = False
    deadline = started + timeout
    # Allocate the selector before spawning a child. Once Popen returns, every
    # setup step is covered by child cleanup, including nonblocking mode and
    # partial selector registration failures.
    with selectors.DefaultSelector() as selector:
        process = subprocess.Popen(command, cwd=directory, env=environment,
                                   stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, start_new_session=True)
        try:
            assert process.stdout is not None and process.stderr is not None
            pipes: tuple[tuple[str, IO[bytes]], ...] = (("stdout", process.stdout),
                                                      ("stderr", process.stderr))
            for name, pipe in pipes:
                os.set_blocking(pipe.fileno(), False)
                selector.register(pipe, selectors.EVENT_READ, name)
            while selector.get_map() or process.poll() is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    break
                for key, _ in selector.select(min(remaining, 0.05)):
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    name = key.data
                    totals[name] += len(chunk)
                    streams[name].extend(chunk[:max(0, MAX_OUTPUT_BYTES - len(streams[name]))])
        except BaseException as error:
            try:
                _stop_process(process)
            except BaseException as cleanup_error:
                # Keep the execution/setup error primary while exposing the
                # cleanup failure as its cause.
                raise error from cleanup_error
            raise
        else:
            # Background children are not allowed to keep changing the exported
            # tree after the foreground command exits. Timeouts use this same
            # single signal. A detached process can escape its process group;
            # this manages lifecycle, not containment.
            _stop_process(process)
        finally:
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()
    result: dict = {"returncode": process.returncode, "timed_out": timed_out,
                    "duration_seconds": round(time.monotonic() - started, 6)}
    for name, data in streams.items():
        result[name] = bytes(data).decode("utf-8", errors="replace")
        result[f"{name}_base64"] = base64.b64encode(data).decode("ascii")
        result[f"{name}_truncated"] = totals[name] > len(data)
    return result


def _scan_tree(root: Path) -> tuple[dict[str, bytes], list[str]]:
    """Capture a stable regular tree without following links, bounded in memory."""
    flags = os.O_RDONLY | os.O_NOFOLLOW
    directory_flags = flags | os.O_DIRECTORY
    files: dict[str, bytes] = {}
    directories: list[str] = []
    total = 0
    entries = 0

    def walk(fd: int, prefix: str, signatures: dict, *, read: bool) -> None:
        nonlocal total, entries
        signatures[prefix] = _signature(os.fstat(fd))
        with os.scandir(fd) as scan:
            names = sorted(entry.name for entry in scan)
        for name in names:
            path = _path(f"{prefix}/{name}" if prefix else name)
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            entries += 1
            if entries > MAX_TREE_ENTRIES:
                raise WorkspaceError("Command tree exceeds the 10000 entry limit")
            if stat.S_ISDIR(info.st_mode):
                child = os.open(name, directory_flags, dir_fd=fd)
                try:
                    if _signature(os.fstat(child)) != _signature(info):
                        raise WorkspaceError(f"Command tree changed during capture: {path}")
                    walk(child, path, signatures, read=read)
                    if read:
                        directories.append(path)
                finally:
                    os.close(child)
            elif stat.S_ISREG(info.st_mode):
                if info.st_size > MAX_FILE_BYTES:
                    raise WorkspaceError(f"Command output file exceeds the 64 MiB limit: {path}")
                child = os.open(name, flags | os.O_NONBLOCK, dir_fd=fd)
                with os.fdopen(child, "rb") as stream:
                    before = os.fstat(stream.fileno())
                    if not stat.S_ISREG(before.st_mode) or _signature(before) != _signature(info):
                        raise WorkspaceError(f"Command tree changed during capture: {path}")
                    if read:
                        data = stream.read(MAX_FILE_BYTES + 1)
                        total += len(data)
                        if len(data) > MAX_FILE_BYTES or total > MAX_TREE_BYTES:
                            raise WorkspaceError("Command output exceeds file/tree size limits")
                        files[path] = data
                    after = os.fstat(stream.fileno())
                    if _signature(before) != _signature(after):
                        raise WorkspaceError(f"Command file changed during capture: {path}")
                    signatures[path] = _signature(after)
            else:
                raise InvalidPathError(f"Command produced a symlink or special file: {path}")
        if signatures[prefix] != _signature(os.fstat(fd)):
            raise WorkspaceError(f"Command directory changed during capture: {prefix or '.'}")

    descriptor = os.open(root, directory_flags)
    try:
        first: dict = {}
        second: dict = {}
        walk(descriptor, "", first, read=True)
        entries = 0
        walk(descriptor, "", second, read=False)
        if first != second or _signature(os.stat(root, follow_symlinks=False)) != first[""]:
            raise WorkspaceError("Command tree changed during capture")
    finally:
        os.close(descriptor)
    return files, sorted(directories)


def run_command(workspace: Workspace, branch_name: str, command: list[str], *,
                timeout: float = 60.0, env: dict[str, str] | None = None) -> dict:
    """Run argv in a private materialized branch and stage successful file changes.

    Every exported file and the full directory listing are tracked, including
    files the process never actually reads. Publication remains a separate step.
    Nonzero exits, timeouts and invalid output trees leave pending file writes
    unchanged. Concurrent changes to this same branch invalidate the import.

    ``env`` replaces the child environment when supplied. The default inherits the
    caller's environment. A shell is only used if explicitly present in ``command``.
    Binary stdout/stderr are also returned as base64; each stream retains 128 KiB.
    Timeout must be positive and at most 2147483.647 seconds. A returned
    ``audit_error`` indicates the call's final audit record could not be saved;
    ``imported`` still reports whether file changes were already staged.
    """
    if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
        raise WorkspaceError("The command runner currently requires macOS or Linux")
    if (not isinstance(command, list) or not command
            or any(not isinstance(part, str) or "\0" in part for part in command)
            or not command[0]):
        raise ValueError("command must be a nonempty argv list of strings without NULs")
    if (isinstance(timeout, bool) or not isinstance(timeout, (float, int))
            or timeout <= 0 or timeout > MAX_COMMAND_TIMEOUT or not math.isfinite(timeout)):
        raise ValueError("timeout must be finite, positive and at most 2147483.647 seconds")
    if env is not None and (not isinstance(env, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) or not key
        or "=" in key or "\0" in key or "\0" in value for key, value in env.items()
    )):
        raise ValueError("env must contain valid string environment names and values")
    branch = workspace.get_branch(branch_name)
    call_id = branch.tools.start("run_command", {"argv": command, "timeout": timeout,
                                               "tracking": "complete_exported_tree"})
    result: dict = {"branch": branch_name, "argv": command, "tool_call_id": call_id,
                    "tracking": "complete_exported_tree", "imported": False}

    def fail_audit(message: str) -> None:
        try:
            branch.tools.error(call_id, message)
        except Exception as audit_error:
            # An unavailable audit store must not replace the actual command,
            # filesystem or input-validation failure with a secondary failure.
            result["audit_error"] = _failure_details(audit_error)

    try:
        captured = branch.fs.capture_files()
        files, directories = captured["files"], captured["directories"]
        if (len(files) + len(directories) > MAX_TREE_ENTRIES
                or any(len(content) > MAX_FILE_BYTES for content in files.values())
                or sum(map(len, files.values())) > MAX_TREE_BYTES):
            raise WorkspaceError("Exported tree exceeds command runner size limits")
        with tempfile.TemporaryDirectory(prefix="agentsql-run-") as temporary:
            directory = Path(temporary)
            for path in directories:
                directory.joinpath(path).mkdir(parents=True, exist_ok=True)
            for path, data in files.items():
                target = directory / path
                target.parent.mkdir(parents=True, exist_ok=True)
                # A valid logical tree can contain names that alias on the
                # host (case folding or Unicode normalization). Never overwrite
                # a previously materialized file through such an alias.
                try:
                    with target.open("xb") as stream:
                        stream.write(data)
                except FileExistsError as error:
                    raise WorkspaceError(f"Workspace path aliases an existing host path: {path}; "
                                         "command was not executed") from error
            initial_files, initial_directories = _scan_tree(directory)
            if initial_files != files or initial_directories != sorted(directories):
                # Exclusive file creation does not detect merged empty directory
                # aliases or renamed Unicode spellings. Verify exact round-trip
                # names and bytes before any command can observe a distorted tree.
                raise WorkspaceError("Workspace tree cannot be represented exactly on this "
                                     "host filesystem; command was not executed")
            result.update(_run_process(command, directory, timeout, env))
            if result["timed_out"] or result["returncode"] != 0:
                result["error"] = "command_timeout" if result["timed_out"] else "command_failed"
                fail_audit(result["error"])
                return result
            after_files, after_directories = _scan_tree(directory)
            changed = sorted(path for path in set(files) | set(after_files)
                             if files.get(path) != after_files.get(path))
            branch.fs.apply_files(after_files, after_directories, expected_token=captured["token"])
            result.update({"imported": True, "changed_files": changed,
                           "directories_added": sorted(set(after_directories) - set(directories)),
                           "directories_removed": sorted(set(directories) - set(after_directories))})
    except (WorkspaceError, OSError, ValueError) as exc:
        result["error"] = _failure_details(exc)
        fail_audit(str(exc))
        return result
    except BaseException:
        try:
            fail_audit("Command runner interrupted")
        except BaseException:
            pass  # Preserve the original interruption even if cleanup is interrupted.
        raise
    # File import has committed. Audit completion is separate and cannot roll
    # those writes back. Keep this outside the execution-failure handler so it
    # cannot falsely record an execution failure or mask the successful import.
    try:
        branch.tools.success(call_id, result)
    except Exception as audit_error:
        result["audit_error"] = _failure_details(audit_error)
    return result

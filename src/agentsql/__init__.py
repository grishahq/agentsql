"""AgentSQL: persistent files, JSON state, and tool history for AI agents."""

from .core import (
    Branch,
    BranchClosedError,
    ConflictError,
    InvalidPathError,
    ProposalMismatchError,
    Snapshot,
    Workspace,
    WorkspaceError,
    WorkspaceBusyError,
)
from .audit import ToolCall, ToolLog
from .files import FileSystem
from .runner import run_command
from .state import KeyValue

__all__ = [
    "Workspace", "Snapshot", "Branch", "WorkspaceError", "ConflictError",
    "InvalidPathError", "BranchClosedError",
    "ProposalMismatchError", "WorkspaceBusyError",
    "FileSystem", "KeyValue", "ToolLog", "ToolCall", "run_command",
]
__version__ = "0.3.0a1"

import sys
from collections import UserList
from pathlib import Path

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from pydantic import BaseModel, Field

from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning, WarningList

MODULE_ISSUE_CODE = "MOD"


class Issue(BaseModel):
    """Base class for all issues"""

    code: str
    message: str | None = Field(default=None)
    fix: str | None = Field(default=None)


# temporary adapter to manage existing warnings
class IssueList(UserList[Issue]):
    """List of build issues."""

    @classmethod
    def from_warning_list(cls, warning_list: WarningList[ToolkitWarning]) -> Self:
        """Create a IssueList from a WarningList."""
        return cls([Issue(name=type(warning).__name__, message=warning.get_message()) for warning in warning_list])  # type: ignore[call-arg]


class ModuleLoadingIssue(Issue):
    """Issue with the loading of a module folder.
    Args:
        path: The path to the module folder.
        message: The message of the issue.
        fix: The fix for the issue.
    """

    code: str = "MOD_001"
    path: Path
    message: str | None = Field(default=None)

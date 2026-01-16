import sys
from collections import UserList

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


# temporary adapter to manage existing warnings
class IssueList(UserList[Issue]):
    """List of build issues."""

    @classmethod
    def from_warning_list(cls, warning_list: WarningList[ToolkitWarning]) -> Self:
        """Create a IssueList from a WarningList."""
        return cls([Issue(code="WARN", message=warning.get_message()) for warning in warning_list])


class ModuleLoadingIssue(Issue):
    """Issue with the loading of a module folder."""

    code: str = "MOD_001"

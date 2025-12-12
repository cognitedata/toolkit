import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from collections import UserList

from pydantic import BaseModel

from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning, WarningList


class BuildIssue(BaseModel):
    """Issue with the build. Can have a recommendation for the user to improve the build."""

    description: str


class BuildIssueList(UserList[BuildIssue]):
    """List of build issues."""

    @classmethod
    def from_warning_list(cls, warning_list: WarningList[ToolkitWarning]) -> Self:
        """Create a BuildIssueList from a WarningList."""
        return cls([BuildIssue(description=warning.get_message()) for warning in warning_list])

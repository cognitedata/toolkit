import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from pydantic import BaseModel, RootModel

from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning, WarningList


class BuildIssue(BaseModel):
    """Issue with the build. Can have a recommendation for the user to improve the build."""

    description: str


class BuildIssueList(RootModel[list[BuildIssue]]):
    """List of build issues."""

    def __init__(self, root: list[BuildIssue] | None = None) -> None:
        """Initialize BuildIssueList with an optional root list."""
        super().__init__(root=root or [])

    @classmethod
    def from_warning_list(cls, warning_list: WarningList[ToolkitWarning]) -> Self:
        """Create a BuildIssueList from a WarningList."""
        return cls(root=[BuildIssue(description=warning.get_message()) for warning in warning_list])

    def append(self, issue: BuildIssue) -> None:
        """Append an issue to the list."""
        self.root.append(issue)

    def extend(self, other: Self) -> None:
        """Extend this list with another BuildIssueList."""
        self.root.extend(other.root)

    def __len__(self) -> int:
        """Return the number of issues in the list."""
        return len(self.root)

    def __contains__(self, item: BuildIssue) -> bool:
        """Check if an issue is in the list."""
        return item in self.root

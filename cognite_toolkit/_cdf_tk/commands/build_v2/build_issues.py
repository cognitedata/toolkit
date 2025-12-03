from typing import Self

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

    def extend(self, other: Self) -> None:
        """Extend this list with another BuildIssueList."""
        self.root.extend(other.root)

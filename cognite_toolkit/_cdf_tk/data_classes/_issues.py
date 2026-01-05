import sys
from collections import UserList
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from pydantic import BaseModel

from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning, WarningList

MODULE_ISSUE_CODE = "MOD"


class Issue(BaseModel):
    """Base class for all issues"""

    name: str | None = None
    message: str | None = None
    code: str | None = None
    fix: str | None = None

    @classmethod
    def issue_type(cls) -> str:
        return cls.__name__


# temporary adapter to manage existing warnings
class IssueList(UserList[Issue]):
    """List of build issues."""

    @classmethod
    def from_warning_list(cls, warning_list: WarningList[ToolkitWarning]) -> Self:
        """Create a IssueList from a WarningList."""
        return cls([Issue(name=type(warning).__name__, message=warning.get_message()) for warning in warning_list])


class ModuleDirectoryIssue(Issue):
    """Issue related to module directory integrity checks."""

    code: str = "DIR"


class ModuleLoadingIssue(Issue):
    """Issue with the loading of the module root folder

    ## What it does
    Validates that the module root folder exists, contains modules and that the selected modules match the modules in the root folder.

    ## Why is this bad?
    If the module root folder does not exist or contains no modules, the build will fail. If the selected modules do not exist, the build will fail.
    """

    code: str = f"{MODULE_ISSUE_CODE}_001"
    path: Path
    config: Any

    def get_message(self, verbose: bool = False) -> str:
        if self.message:
            return self.message
        default_message = f"Module root folder {self.path.as_posix()!r} does not exist or is not a directory, or "
        if not verbose:
            default_message += "does not contain the selected modules"
            return default_message
        default_message += f"does not contain the selected modules: {self.config.environment.selected}"
        default_message += "Please check that the selected modules exist in the module root folder."
        default_message += f"The Toolkit expects the following structure: {self.path.as_posix()!r}/modules/{self.config.environment.selected}."
        return default_message


class ModuleSkippedIssue(Issue):
    """Issue related to skipped modules."""

    code: str = f"{MODULE_ISSUE_CODE}_002"
    path: Path

    def get_message(self, verbose: bool = False) -> str:
        if self.message:
            return self.message
        default_message = (
            f"Module {self.path.as_posix()!r} was ignored by the Toolkit. It may be excluded by .toolkitignore."
        )
        return default_message

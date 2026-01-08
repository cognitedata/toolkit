import sys
from collections import UserList
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from pydantic import BaseModel, model_validator

from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning, WarningList

MODULE_ISSUE_CODE = "MOD"


class Issue(BaseModel):
    """Base class for all issues"""

    code: str
    _custom_message: str | None = None

    @model_validator(mode="before")
    @classmethod
    def handle_message_param(cls, data: dict | Any) -> dict | Any:
        if isinstance(data, dict):
            if "message" in data:
                data["_custom_message"] = data.pop("message")
            if "name" in data:
                # Use name as code if code is not provided
                if "code" not in data:
                    data["code"] = str(data.pop("name"))
                else:
                    _ = data.pop("name")
        return data

    @classmethod
    def issue_type(cls) -> str:
        return cls.__name__

    @property
    def message(self) -> str:
        if self._custom_message:
            return self._custom_message
        fix_msg = self.fix()
        return f"{self.issue_type()} {self.code}: {fix_msg if fix_msg else 'Unknown issue'}"

    def fix(self) -> str | None:
        return None


# temporary adapter to manage existing warnings
class IssueList(UserList[Issue]):
    """List of build issues."""

    @classmethod
    def from_warning_list(cls, warning_list: WarningList[ToolkitWarning]) -> Self:
        """Create a IssueList from a WarningList."""
        return cls([Issue(name=type(warning).__name__, message=warning.get_message()) for warning in warning_list])  # type: ignore[call-arg]


class ModuleLoadingIssue(Issue):
    """Issue with the loading of a module folder

    ## What it does
    Validates that the module folder exists and contains resources.

    ## Why is this bad?
    If the module folder does not exist or contains no resources, the build will skip it.
    """

    code: str = f"{MODULE_ISSUE_CODE}_001"
    path: Path
    _custom_message: str | None = None

    @model_validator(mode="before")
    @classmethod
    def handle_message_param(cls, data: dict | Any) -> dict | Any:
        if isinstance(data, dict) and "message" in data:
            data["_custom_message"] = data.pop("message")
        return data

    @property
    def message(self) -> str:
        if self._custom_message:
            return self._custom_message
        return f"Module {self.path.as_posix()!r} does not exist or is not a directory, or does not contain the selected modules"


class ModuleLoadingUnrecognizedResourceIssue(Issue):
    """Module contains a resource type that is not recognized

    ## What it does
    Validates that the resource type is supported by the Toolkit.

    ## What is the problem?
    If the resource type is not supported, the build will skip it.

    ## How to fix it?
    Check spelling or negate the selection with wildcard to exclude the resource type from the loading.
    """

    code: str = f"{MODULE_ISSUE_CODE}_002"
    path: Path
    unrecognized_resource_folders: list[str]

    @property
    def message(self) -> str:
        return f"unrecognized resource folders: {', '.join(self.unrecognized_resource_folders)}"

    @property
    def verbose(self) -> str:
        return (
            self.message
            + f"\nThe Toolkit supports the following resource folders: {', '.join(CRUDS_BY_FOLDER_NAME.keys())}"
        )


class ModuleLoadingDisabledResourceIssue(Issue):
    """Module contains a resource type that hasn't been enabled

    ## What it does
    Validates that the resource type is enabled.

    ## What is the problem?
    If the resource type is disabled, the build will skip it.

    ## How to fix it?
    Enable the resource type in the cdf.toml file.
    """

    code: str = f"{MODULE_ISSUE_CODE}_003"
    path: Path
    disabled_resource_folders: list[str]

    @property
    def message(self) -> str:
        return f"Contains resource folders that require enabling a flag in your cdf.toml: {', '.join(self.disabled_resource_folders)}"

    @property
    def verbose(self) -> str:
        # TODO: show which flags are required to enable the resource folders
        return (
            self.message
            + f"\nThe Toolkit supports the following resource folders: {', '.join(CRUDS_BY_FOLDER_NAME.keys())}"
        )


class ModuleLoadingNoResourcesIssue(Issue):
    """Module contains no resources

    ## What it does
    Validates that the module contains resources.

    ## What is the problem?
    If the module contains no resources, the build will skip it.
    """

    code: str = f"{MODULE_ISSUE_CODE}_004"
    path: Path

    @property
    def message(self) -> str:
        return f"No resources found in module {self.path.as_posix()!r}"


class ModuleLoadingNestedModulesIssue(Issue):
    """Module contains nested modules

    ## What it does
    Validates that the module is a deepest module.

    ## What is the problem?
    If the module contains nested modules, it is not a deepest module and is discarded.
    """

    code: str = f"{MODULE_ISSUE_CODE}_005"
    path: Path

    @property
    def message(self) -> str:
        return f"Module {self.path.as_posix()!r} contains nested modules and was discarded. Only the deepest modules are loaded."

import builtins
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra, ResourceIO
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import relative_to_if_possible

from ._insights import ConsistencyError, FileReadError, IgnoredFileWarning, Insight, InsightList, ModelSyntaxWarning
from ._module import BuildVariable, FailedReadYAMLFile, IgnoredFile, ModuleId, ResourceType
from ._types import AbsoluteDirPath, AbsoluteFilePath, RelativeDirPath, RelativeFilePath, ValidationType


class BuildParameters(BaseModel):
    organization_dir: Path
    build_dir: Path = Field(default_factory=lambda: Path.cwd() / "build")
    config_yaml: Path | None = Field(
        None,
        description="Path to the configuration YAML file (typically config.<env>.yaml under the organization directory).",
    )
    user_selected_modules: list[str] | None = Field(
        None,
        description="List of module names or paths to build. If not provided, Toolkit will first attempt to find a config YAML "
        "and the modules specified there. If no config YAML is found, Toolkit will prompt the user to choose the modules"
        "to build",
    )
    verbose: bool = False
    insight_format: Literal["csv", "json"] = Field(
        default="csv",
        description="Format for the insights file written to the build directory.",
    )

    @property
    def modules_directory(self) -> Path:
        return self.organization_dir / MODULES

    @property
    def insight_path(self) -> Path:
        return self.build_dir / f"insights.{self.insight_format}"

    @property
    def config_file_name(self) -> str:
        return self.config_yaml.name if self.config_yaml else ""


class BuildSourceFiles(BaseModel):
    """The output of reading the source system.

    All yaml files found in the modules/ directory.
    If available, the config.<name>.yaml file, which specifies which modules to build, variables available,
    CDF Project to build for.
    """

    yaml_files: list[RelativeFilePath] = Field(
        description="List of all YAML files that are part of the build, with paths relative to the organization directory."
    )
    selected_modules: set[RelativeDirPath | str] | None = Field(
        description="Set of modules to build. Either module names (folder names) or relative paths to the organization directory."
        "If None, no modules are selected."
    )
    variables: dict[str, JsonValue] = Field(
        default_factory=dict, description="Variables to be used during the build process."
    )
    validation_type: ValidationType = "prod"
    cdf_project: str
    organization_dir: AbsoluteDirPath

    @property
    def module_dir(self) -> Path:
        return self.organization_dir / MODULES


class BuiltResource(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    identifier: Identifier
    source_hash: str
    type: ResourceType
    source_path: AbsoluteFilePath
    build_path: AbsoluteFilePath
    crud_cls: builtins.type[ResourceIO]
    dependencies: set[tuple[builtins.type[ResourceIO], Identifier]] = Field(default_factory=set)

    failed_extra: list[FailedReadExtra] = Field(default_factory=list)
    has_syntax_error: bool

    @property
    def can_verify(self) -> bool:
        """If a resource has a syntax error or failed extra files
        We assume these must be fixed before further validation can be done.
        """
        return not self.has_syntax_error and not self.failed_extra


class BuiltModule(BaseModel):
    module_id: ModuleId
    resources: list[BuiltResource] = Field(default_factory=list)
    insights: list[Insight] = Field(default_factory=list)
    syntax_warnings_by_source: dict[Path, ModelSyntaxWarning] = Field(default_factory=dict)
    unresolved_variables_by_source: dict[Path, list[str]] = Field(default_factory=dict)
    failed_files: list[FailedReadYAMLFile] = Field(default_factory=list)
    ignored_files: list[IgnoredFile] = Field(default_factory=list)
    yaml_line_count: int

    @property
    def files_built(self) -> bool:
        """Indicates whether any files were built for this module."""
        return len(self.resources) > 0

    @property
    def is_success(self) -> bool:
        """Determines if the module build was successful based on the presence of built file and validation errors."""
        return self.files_built

    @property
    def all_insights(self) -> InsightList:
        """The list of all insights for this module."""
        insights = InsightList(self.insights)
        for resource in self.resources:
            for failed_extra in resource.failed_extra:
                display_path = relative_to_if_possible(resource.source_path)
                insights.append(
                    FileReadError(
                        message=f"In {display_path.as_posix()!r}: {failed_extra.error}", code=failed_extra.code
                    )
                )
        insights.extend(self.syntax_warnings_by_source.values())
        for path, variables in self.unresolved_variables_by_source.items():
            display_path = relative_to_if_possible(path)
            insights.append(
                ConsistencyError(
                    code="UNRESOLVED-VARIABLES",
                    message=f"Unresolved variables in {display_path.as_posix()!r}: {humanize_collection(variables)}",
                    fix="Make sure to define the variables in the 'config YAML' file and that they are "
                    "correctly placed in the variables section matching the file path",
                )
            )
        for failed_file in self.failed_files:
            display_path = relative_to_if_possible(failed_file.source_path)
            insights.append(
                FileReadError(code=failed_file.code, message=f"In {display_path.as_posix()!r}: {failed_file.error}")
            )
        for ignored_file in self.ignored_files:
            display_path = relative_to_if_possible(ignored_file.filepath)
            insights.append(
                IgnoredFileWarning(
                    code=ignored_file.code, message=f"In {display_path.as_posix()!r}: {ignored_file.reason}"
                )
            )

        return insights

    def __hash__(self) -> int:
        return hash(self.module_id.path)


class ValidationResult(BaseModel):
    name: str
    insights: list[Insight]


class BuildFolder(BaseModel):
    """Built folder acts as a container holding all built modules and insights from the build process."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    organization_dir: AbsoluteDirPath
    build_dir: AbsoluteDirPath
    started_at: datetime
    finished_at: datetime

    built_modules: list[BuiltModule] = Field(default_factory=list)
    validation_results: list[ValidationResult] = Field(default_factory=list)
    all_variables: list[BuildVariable] = Field(default_factory=list)

    @property
    def all_insights(self) -> InsightList:
        """Insights from all built modules."""
        insights = InsightList()
        for module in self.built_modules:
            insights.extend(module.all_insights)
        for result in self.validation_results:
            insights.extend(result.insights)

        return insights

    @property
    def build_duration_seconds(self) -> float:
        """Duration of the build in seconds."""
        return (self.finished_at - self.started_at).total_seconds()

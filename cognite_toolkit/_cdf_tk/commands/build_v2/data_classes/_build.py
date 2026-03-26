import builtins
from collections import defaultdict
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD

from ._insights import Insight, InsightList, ModelSyntaxWarning
from ._module import ModuleId, ResourceType
from ._types import AbsoluteDirPath, AbsoluteFilePath, RelativeDirPath, RelativeFilePath, ValidationType


class BuildParameters(BaseModel):
    organization_dir: Path
    build_dir: Path = Field(default_factory=lambda: Path.cwd() / "build")
    config_yaml_name: str | None = Field(
        None,
        description="The name of the configuration YAML file to use. It expected to be"
        "named config.[name].yaml and be located in the organization directory.",
    )
    user_selected_modules: list[str] | None = Field(
        None,
        description="List of module names or paths to build. If not provided, Toolkit will first attempt to find a config YAML "
        "and the modules specified there. If no config YAML is found, Toolkit will build all modules in the organization directory.",
    )
    verbose: bool = False

    @property
    def modules_directory(self) -> Path:
        return self.organization_dir / MODULES


class BuildSourceFiles(BaseModel):
    """The output of reading the source system.

    All yaml files found in the modules/ directory.
    If available, the config.<name>.yaml file, which specifies which modules to build, variables available,
    CDF Project to build for.
    """

    yaml_files: list[RelativeFilePath] = Field(
        description="List of all YAML files that are part of the build, with paths relative to the organization directory."
    )
    selected_modules: set[RelativeDirPath | str] = Field(
        description="Set of modules to build. Either module names (folder names) or relative paths to the organization directory."
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
    crud_cls: builtins.type[ResourceCRUD]
    dependencies: set[tuple[builtins.type[ResourceCRUD], Identifier]] = Field(default_factory=set)

    # Todo: remove
    syntax_warning: ModelSyntaxWarning | None = None


class BuiltModule(BaseModel):
    module_id: ModuleId
    resources: list[BuiltResource] = Field(default_factory=list)
    insights: list[Insight] = Field(default_factory=list)
    syntax_warnings_by_source: dict[Path, ModelSyntaxWarning] = Field(default_factory=dict)

    @property
    def resource_by_type_by_kind(self) -> dict[ResourceType, list[Path]]:
        """Organizes built files by their resource type and kind."""
        resource_by_type: dict[ResourceType, list[Path]] = defaultdict(list)
        for resource in self.resources:
            resource_by_type[resource.type].append(resource.build_path)

        return dict(resource_by_type)

    @property
    def files_built(self) -> bool:
        """Indicates whether any files were built for this module."""
        return len(self.resources) > 0

    @property
    def is_success(self) -> bool:
        """Determines if the module build was successful based on the presence of built file and validation errors."""
        return self.files_built

    def __hash__(self) -> int:
        return hash(self.module_id.path)


class BuildFolder(BaseModel):
    """Built folder acts as a container holding all built modules and insights from the build process."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    path: Path
    built_modules: list[BuiltModule] = Field(default_factory=list)

    syntax_warnings: dict[Path, ModelSyntaxWarning] = Field(default_factory=dict)
    insights_by_validation_type: dict[str, list[Insight]] = Field(default_factory=dict)

    # Todo: Remove
    dependency_insights: InsightList = Field(default_factory=InsightList)
    global_insights: InsightList = Field(default_factory=InsightList)

    @property
    def all_insights(self) -> InsightList:
        """Insights from all built modules."""
        insights = InsightList(self.dependency_insights + self.global_insights)
        for module in self.built_modules:
            insights.extend(module.insights)
            for resource in module.resources:
                if resource.syntax_warning:
                    insights.append(resource.syntax_warning)
        return insights

    @property
    def built_modules_by_success(self) -> dict[bool, list[str]]:
        """Organizes built modules by their success status."""
        modules_by_success: dict[bool, list[str]] = {True: [], False: []}
        for built_module in self.built_modules:
            modules_by_success[built_module.is_success].append(built_module.module_id.name)

        return modules_by_success

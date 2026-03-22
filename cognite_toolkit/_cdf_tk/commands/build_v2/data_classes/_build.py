import builtins
from collections import defaultdict
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD

from ._insights import InsightList
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

    @property
    def modules_directory(self) -> Path:
        return self.organization_dir / MODULES


class BuildSourceFiles(BaseModel):
    """Intermediate format used when parsing modules"""

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


class BuiltResource(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    identifier: Identifier
    source_hash: str
    type: ResourceType
    source_path: AbsoluteFilePath
    build_path: AbsoluteFilePath
    dependencies: set[tuple[builtins.type[ResourceCRUD], Identifier]] = Field(default_factory=set)
    insights: InsightList = Field(default_factory=InsightList)


class BuiltModule(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    module_id: ModuleId
    resources: list[BuiltResource] = Field(default_factory=list)

    # Replace with above
    dependencies: dict[AbsoluteFilePath, set[tuple[type[ResourceCRUD], Identifier]]] = Field(default_factory=dict)
    insights: InsightList = Field(default_factory=InsightList)

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
        return not self.insights.has_errors and self.files_built

    def __hash__(self) -> int:
        return hash(self.module_id.path)


class BuildFolder(BaseModel):
    """Built folder acts as a container holding all built modules and insights from the build process."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    path: Path
    built_modules: list[BuiltModule] = Field(default_factory=list)

    @property
    def insights(self) -> InsightList:
        """Insights from all built modules."""
        insights = InsightList()
        for module in self.built_modules:
            insights.extend(module.insights)
        return insights

    @property
    def built_modules_by_success(self) -> dict[bool, list[str]]:
        """Organizes built modules by their success status."""
        modules_by_success: dict[bool, list[str]] = {True: [], False: []}
        for built_module in self.built_modules:
            modules_by_success[built_module.is_success].append(built_module.module_id.name)

        return modules_by_success

    @property
    def built_resources_identifiers(self) -> set[Identifier]:
        """Set of all built resources across all modules."""
        resources: set[Identifier] = set()
        for built_module in self.built_modules:
            for resource in built_module.resources:
                resources.add(resource.identifier)
        return resources

    @property
    def cdf_dependencies_by_built_module(
        self,
    ) -> dict[BuiltModule, dict[AbsoluteFilePath, dict[type[ResourceCRUD], set[Identifier]]]]:
        """Get CDF dependencies for all built modules.
        CDF dependencies are dependencies that are not part of the build which require validation against CDF.

        If CDF dependency is present in multiple modules, it will be returned only to a single module
        (the first one that it is encountered in) to avoid duplicate validations insights.
        """
        dependencies_by_built_module: dict[
            BuiltModule, dict[AbsoluteFilePath, dict[type[ResourceCRUD], set[Identifier]]]
        ] = {}
        seen: set[Identifier] = set()

        for built_module in self.built_modules:
            for file, dependencies_by_resource_type in built_module.dependencies.items():
                for resource_type, dependency in dependencies_by_resource_type:
                    if dependency in self.built_resources_identifiers:
                        continue
                    if dependency in seen:
                        continue
                    seen.add(dependency)
                    dependencies_by_built_module.setdefault(built_module, {}).setdefault(file, {}).setdefault(
                        resource_type, set()
                    ).add(dependency)

        return dependencies_by_built_module

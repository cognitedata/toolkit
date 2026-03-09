from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD

from ._insights import InsightList
from ._lineage import BuildConfigLineage, BuildLineage
from ._module import ModuleSource
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


class BuiltModule(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    source: ModuleSource
    built_files: list[Path] = Field(default_factory=list)
    built_resources_identifiers: list[Identifier] = Field(default_factory=list)
    dependencies: dict[AbsoluteFilePath, set[tuple[type[ResourceCRUD], Identifier]]] = Field(default_factory=dict)
    insights: InsightList = Field(default_factory=InsightList)

    @property
    def resource_by_type(self) -> dict[str, dict[str, list[Path]]]:
        """Organizes built files by their resource type."""
        resource_by_type: dict[str, dict[str, list[Path]]] = {}
        for file in self.built_files:
            resource_type = file.stem.split(".")[-1]
            resource_type_folder = file.parent.name
            resource_by_type.setdefault(resource_type_folder, {}).setdefault(resource_type, []).append(file)

        return resource_by_type

    @property
    def is_success(self) -> bool:
        return True if self.built_files else False

    def __hash__(self) -> int:
        return hash(self.source.path)


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
    def lineage(self) -> BuildLineage:
        """Generate BuildLineage from the built modules and folder data."""
        # Count statistics
        total_syntax_errors = 0
        total_consistency_errors = 0
        total_warnings = 0
        total_recommendations = 0

        for built_module in self.built_modules:
            for insight_type, insights in built_module.insights.by_type().items():
                if insight_type.__name__ == "ModelSyntaxError":
                    total_syntax_errors += len(insights)
                elif insight_type.__name__ == "ConsistencyError":
                    total_consistency_errors += len(insights)
                elif insight_type.__name__ == "ConsistencyWarning":
                    total_warnings += len(insights)
                elif insight_type.__name__ == "Recommendation":
                    total_recommendations += len(insights)

        total_modules = len(self.built_modules)
        total_resources_discovered = 0
        for module in self.built_modules:
            total_resources_discovered += len([f for f in module.built_files if f])

        return BuildLineage(
            build_duration_seconds=None,
            config=BuildConfigLineage(
                organization_dir=self.path,  # This is the build path, lineage will need context update
                build_dir=self.path,
                config_name=None,
                cdf_project="UNKNOWN",  # Will need to be updated from BuildParameters
                validation_type="prod",  # Will need to be updated from BuildParameters
                selected_modules=set(),  # Will need to be updated from BuildParameters
            ),
            total_modules=total_modules,
            total_modules_processed=len(self.built_modules),
            total_resources_discovered=total_resources_discovered,
            total_resources_built=len(self.built_resources_identifiers),
            total_syntax_errors=total_syntax_errors,
            total_consistency_errors=total_consistency_errors,
            total_warnings=total_warnings,
            total_recommendations=total_recommendations,
            build_successful=len([m for m in self.built_modules if not m.is_success]) == 0,
        )

    @property
    def built_modules_by_success(self) -> dict[bool, list[str]]:
        """Organizes built modules by their success status."""
        modules_by_success: dict[bool, list[str]] = {True: [], False: []}
        for built_module in self.built_modules:
            modules_by_success[built_module.is_success].append(built_module.source.name)

        return modules_by_success

    @property
    def built_resources_identifiers(self) -> set[Identifier]:
        """Set of all built resources across all modules."""
        resources: set[Identifier] = set()
        for built_module in self.built_modules:
            resources.update(built_module.built_resources_identifiers)
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

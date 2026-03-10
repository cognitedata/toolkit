from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD

from ._insights import ConsistencyError, InsightList, ModelSyntaxError, Recommendation
from ._lineage import (
    BuildConfigLineage,
    BuildLineage,
    ModuleLineageItem,
    ModulesSummary,
    ResourceLineageItem,
    ResourcesSummary,
)
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
    build_duration_seconds: float | None = Field(None, description="Total build duration in seconds")

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
        total_recommendations = 0

        # Create module lineage items
        module_lineage_items: dict[RelativeDirPath, list[ModuleLineageItem]] = {}
        for built_module in self.built_modules:
            for insight_type, insights in built_module.insights.by_type().items():
                if insight_type is ModelSyntaxError:
                    total_syntax_errors += len(insights)
                elif insight_type is ConsistencyError:
                    total_consistency_errors += len(insights)
                elif insight_type is Recommendation:
                    total_recommendations += len(insights)

            # Create resource lineage items for each built file
            resource_lineage: dict[AbsoluteFilePath, ResourceLineageItem] = {}

            # Collect all source files from the module
            source_files = []
            for resource_type, files in built_module.source.resource_files_by_folder.items():
                source_files.extend(files)

            for idx, built_file in enumerate(built_module.built_files):
                # Get corresponding source file (if available)
                source_file = source_files[idx] if idx < len(source_files) else built_file

                resource_lineage_item = ResourceLineageItem(
                    source_file=source_file,
                    source_hash="",  # Would need to track this during build
                    resource_type=built_file.parent.name,
                    kind=built_file.stem.rsplit(".", 1)[-1] if "." in built_file.stem else "",
                    built_file=built_file,
                )
                resource_lineage[built_file] = resource_lineage_item
            # Create module lineage item
            module_path: RelativeDirPath = (
                built_module.source.path.relative_to(self.path.parent)
                if self.path.parent in built_module.source.path.parents
                else built_module.source.path
            )

            # Calculate resource summaries
            resources_discovered = len(resource_lineage)
            resources_processed = sum(
                1 for res in resource_lineage.values() if res.overall_status in ("SUCCESS", "BUILT_WITH_WARNINGS")
            )
            resources_failed = resources_discovered - resources_processed

            module_lineage = ModuleLineageItem.model_construct(
                module_id=str(built_module.source.id),
                module_path=built_module.source.path,
                iteration=0,
                resource_lineage=resource_lineage,
                resources=ResourcesSummary(
                    discovered=resources_discovered,
                    processed=resources_processed,
                    failed=resources_failed,
                ),
                insights=built_module.insights.summary,
            )
            module_lineage_items.setdefault(module_path, []).append(module_lineage)

        # Calculate module summaries - a module fails if it has syntax or consistency errors
        modules_discovered = len(self.built_modules)
        modules_failed = sum(
            1
            for module in self.built_modules
            if module.insights.summary.get("syntax_errors", 0) > 0
            or module.insights.summary.get("consistency_errors", 0) > 0
        )
        modules_processed = modules_discovered - modules_failed

        # Check if any module failed based on insights
        has_failures = modules_failed > 0

        # Round timestamp to 2 decimal places (centiseconds)
        timestamp = datetime.utcnow()
        timestamp = timestamp.replace(microsecond=round(timestamp.microsecond / 10000) * 10000)

        return BuildLineage.model_construct(
            build_timestamp=timestamp,
            build_duration_seconds=self.build_duration_seconds,
            config=BuildConfigLineage.model_construct(
                organization_dir=self.path,  # This is the build path, lineage will need context update
                build_dir=self.path,
                cdf_project="UNKNOWN",  # Will need to be updated from BuildParameters
                validation_type="prod",  # Will need to be updated from BuildParameters
                selected_modules=set(),  # Will need to be updated from BuildParameters
                variables_provided={},
            ),
            modules=ModulesSummary(
                discovered=modules_discovered,
                processed=modules_processed,
                failed=modules_failed,
            ),
            insights={
                "syntax_errors": total_syntax_errors,
                "consistency_errors": total_consistency_errors,
                "recommendations": total_recommendations,
            },
            build_successful=not has_failures and total_consistency_errors == 0,
            module_lineage=module_lineage_items,
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

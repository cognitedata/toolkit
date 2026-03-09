"""Build lineage tracking classes for comprehensive build process traceability."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD

from ._insights import InsightList
from ._module import ModuleSource
from ._types import AbsoluteDirPath, AbsoluteFilePath, RelativeDirPath, ValidationType


class BuildConfigLineage(BaseModel):
    """Tracks build configuration and environment."""

    organization_dir: AbsoluteDirPath = Field(description="Organization root directory")
    build_dir: AbsoluteDirPath = Field(description="Build output directory")
    config_name: str | None = Field(None, description="Config YAML name if provided")
    cdf_project: str = Field(description="Target CDF project")
    validation_type: ValidationType = Field(description="Validation type (prod/dev)")
    selected_modules: set[RelativeDirPath | str] = Field(description="Selected modules for build")
    variables_provided: dict[str, JsonValue] = Field(
        default_factory=dict, description="Variables provided via config or command line"
    )


class ResourceLineageItem(BaseModel):
    """Tracks a single resource through the build process."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_file: AbsoluteFilePath = Field(description="Absolute path to source YAML file")
    source_hash: str = Field(description="Hash of source file content (before variable substitution)")
    resource_id: Identifier = Field(description="Resource identifier (e.g., space/externalId)")
    resource_type: str = Field(description="Resource type folder (e.g., 'spaces', 'containers', 'views')")
    resource_kind: str = Field(description="Resource kind (e.g., 'space', 'container', 'view')")

    # Variable substitution tracking
    variables_applied: list[str] = Field(
        default_factory=list, description="Variables that were substituted in this resource"
    )

    # Parsing phase
    parsing_successful: bool = Field(description="Whether resource parsed without syntax errors")
    parsing_errors: InsightList = Field(default_factory=InsightList, description="Syntax errors during parsing")
    parsing_insights: InsightList = Field(
        default_factory=InsightList, description="Recommendations from parsing (e.g., unknown fields)"
    )

    # Validation phase
    local_validation_insights: InsightList = Field(
        default_factory=InsightList, description="Rule validation insights (local validation)"
    )
    cdf_validation_insights: InsightList = Field(
        default_factory=InsightList, description="CDF dependency validation insights"
    )
    global_validation_insights: InsightList = Field(
        default_factory=InsightList, description="Cross-resource validation insights (e.g., from NEAT)"
    )

    # Dependencies
    internal_dependencies: set[tuple[type[ResourceCRUD], Identifier]] = Field(
        default_factory=set, description="Dependencies on other resources in this build"
    )
    external_dependencies: set[tuple[type[ResourceCRUD], Identifier]] = Field(
        default_factory=set, description="Dependencies on resources that must exist in CDF"
    )
    missing_dependencies: set[tuple[type[ResourceCRUD], Identifier]] = Field(
        default_factory=set, description="External dependencies that don't exist in build or CDF"
    )

    # Output
    built_file: AbsoluteFilePath | None = Field(None, description="Path to output YAML file in build directory")

    @property
    def overall_status(self) -> str:
        """Determines overall build status for this resource."""
        if not self.parsing_successful:
            return "FAILED"
        if self.parsing_errors:
            return "FAILED"
        if self.missing_dependencies:
            return "FAILED"
        # Check for consistency errors in CDF validation
        if any(insight.__class__.__name__ == "ConsistencyError" for insight in self.cdf_validation_insights):
            return "FAILED"
        if (self.parsing_insights or self.local_validation_insights) or any(
            insight.__class__.__name__ == "ConsistencyWarning" for insight in self.cdf_validation_insights
        ):
            return "BUILT_WITH_WARNINGS"
        return "SUCCESS"

    @property
    def all_insights(self) -> InsightList:
        """Aggregates all insights across all phases."""
        combined = InsightList()
        combined.extend(self.parsing_errors)
        combined.extend(self.parsing_insights)
        combined.extend(self.local_validation_insights)
        combined.extend(self.cdf_validation_insights)
        combined.extend(self.global_validation_insights)
        return combined


class ModuleLineageItem(BaseModel):
    """Tracks a module through the build process."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    module_source: ModuleSource = Field(description="Module source configuration")
    iteration: int = Field(
        default=0, description="Iteration number if multi-value variables were used (0 if no iteration)"
    )

    # Parsing phase
    discovered_resources: int = Field(description="Number of resources discovered in source files")
    parsing_successful: bool = Field(description="Whether all resources in module parsed without syntax errors")
    parsing_errors_count: int = Field(description="Count of parsing errors across all resources")

    # Resource tracking
    resource_lineage_items: dict[AbsoluteFilePath, ResourceLineageItem] = Field(
        default_factory=dict, description="Mapping of source file to resource lineage"
    )

    # Build output
    built_files: list[AbsoluteFilePath] = Field(
        default_factory=list, description="Output YAML files generated for this module"
    )

    # Statistics
    built_resources_count: int = Field(description="Number of successfully built resources")
    total_insights_count: int = Field(description="Total insights across all resources in this module")

    # Insights breakdown at module level
    insights: dict[str, int] = Field(description="Breakdown of insights by type for this module")

    @property
    def overall_status(self) -> str:
        """Determines overall build status for this module."""
        if not self.parsing_successful:
            return "FAILED"
        if self.parsing_errors_count > 0:
            return "FAILED"

        # Check if any resource failed
        if any(item.overall_status == "FAILED" for item in self.resource_lineage_items.values()):
            return "FAILED"

        if any(item.overall_status == "BUILT_WITH_WARNINGS" for item in self.resource_lineage_items.values()):
            return "BUILT_WITH_WARNINGS"

        return "SUCCESS"

    @property
    def all_insights(self) -> InsightList:
        """Aggregates all insights from all resources."""
        combined = InsightList()
        for item in self.resource_lineage_items.values():
            combined.extend(item.all_insights)
        return combined


class DependencyLineageItem(BaseModel):
    """Tracks dependency resolution through the build process."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    dependent_resource_id: Identifier = Field(description="Resource that has a dependency")
    dependency_resource_id: Identifier = Field(description="Resource being depended on")
    dependency_type: type[ResourceCRUD] = Field(description="CRUD class for dependency")

    # Dependency classification
    is_internal: bool = Field(description="True if dependency is within the build, False if external to CDF")
    resolved_in_build: bool = Field(
        description="True if dependency is found in the build (for internal) or in CDF (for external)"
    )
    built_in_same_module: bool = Field(
        default=False, description="True if dependency is in same module as dependent (internal only)"
    )

    # Resolution details
    satisfied_by: Identifier | None = Field(
        None, description="Actual identifier of the resource that satisfies this dependency (if found)"
    )


class BuildLineage(BaseModel):
    """Comprehensive lineage tracking for the entire build process.

    Provides full traceability of:
    - Source files and modules
    - Resource parsing and validation
    - Dependencies and their resolution
    - Validation insights by phase
    - Build output and statistics
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Build metadata
    build_timestamp: datetime = Field(default_factory=datetime.utcnow, description="When build started")
    build_duration_seconds: float | None = Field(None, description="Total build duration in seconds")

    # Configuration
    config: BuildConfigLineage = Field(description="Build configuration and environment")

    # Module tracking
    module_lineage_items: dict[RelativeDirPath, list[ModuleLineageItem]] = Field(
        default_factory=dict,
        description="Lineage for each module, indexed by module path. List because of iterations.",
    )

    # Dependency tracking
    dependencies: list[DependencyLineageItem] = Field(
        default_factory=list, description="All dependencies discovered across all resources"
    )

    # Summary statistics
    total_modules: int = Field(description="Total modules discovered")
    total_modules_processed: int = Field(description="Total module iterations processed")
    total_resources_discovered: int = Field(description="Total resources found in source")
    total_resources_built: int = Field(description="Total resources successfully built")

    # Insights summary
    insights: dict[str, int] = Field(description="Summary of all insights found during build")

    # Overall status
    build_successful: bool = Field(description="True if build completed without errors")

    # ==================== Properties for Analysis ====================

    @property
    def failed_modules(self) -> list[tuple[RelativeDirPath, ModuleLineageItem]]:
        """All modules that failed during build."""
        failed = []
        for module_path, lineages in self.module_lineage_items.items():
            for lineage in lineages:
                if lineage.overall_status == "FAILED":
                    failed.append((module_path, lineage))
        return failed

    @property
    def built_modules(self) -> list[tuple[RelativeDirPath, ModuleLineageItem]]:
        """All modules that were successfully built."""
        built = []
        for module_path, lineages in self.module_lineage_items.items():
            for lineage in lineages:
                if lineage.overall_status in ("SUCCESS", "BUILT_WITH_WARNINGS"):
                    built.append((module_path, lineage))
        return built

    @property
    def failed_resources(self) -> list[tuple[RelativeDirPath, ResourceLineageItem]]:
        """All resources that failed during build."""
        failed = []
        for module_path, lineages in self.module_lineage_items.items():
            for lineage in lineages:
                for resource in lineage.resource_lineage_items.values():
                    if resource.overall_status == "FAILED":
                        failed.append((module_path, resource))
        return failed

    @property
    def unresolved_dependencies(self) -> list[DependencyLineageItem]:
        """All dependencies that could not be resolved."""
        return [dep for dep in self.dependencies if not dep.resolved_in_build]

    @property
    def dependency_graph(self) -> dict[Identifier, list[Identifier]]:
        """Returns dependency graph: dependent → [dependencies]."""
        graph: dict[Identifier, list[Identifier]] = {}
        for dep in self.dependencies:
            if dep.dependent_resource_id not in graph:
                graph[dep.dependent_resource_id] = []
            graph[dep.dependent_resource_id].append(dep.dependency_resource_id)
        return graph

    @property
    def dependents_graph(self) -> dict[Identifier, list[Identifier]]:
        """Reverse dependency graph: resource → [resources that depend on it]."""
        graph: dict[Identifier, list[Identifier]] = {}
        for dep in self.dependencies:
            if dep.dependency_resource_id not in graph:
                graph[dep.dependency_resource_id] = []
            graph[dep.dependency_resource_id].append(dep.dependent_resource_id)
        return graph

    @property
    def internal_dependencies_only(self) -> list[DependencyLineageItem]:
        """Dependencies within the build."""
        return [dep for dep in self.dependencies if dep.is_internal]

    @property
    def external_dependencies_only(self) -> list[DependencyLineageItem]:
        """Dependencies on resources external to the build."""
        return [dep for dep in self.dependencies if not dep.is_internal]

    @property
    def all_insights(self) -> InsightList:
        """Aggregates all insights across entire build."""
        combined = InsightList()
        for lineages in self.module_lineage_items.values():
            for lineage in lineages:
                combined.extend(lineage.all_insights)
        return combined

    @property
    def build_report(self) -> dict[str, object]:
        """Generates a comprehensive build report."""
        return {
            "timestamp": self.build_timestamp.isoformat(),
            "duration_seconds": self.build_duration_seconds,
            "organization": str(self.config.organization_dir),
            "cdf_project": self.config.cdf_project,
            "validation_type": self.config.validation_type,
            "modules": {
                "total": self.total_modules,
                "processed": self.total_modules_processed,
                "successful": len(self.built_modules),
                "failed": len(self.failed_modules),
            },
            "resources": {
                "total_discovered": self.total_resources_discovered,
                "total_built": self.total_resources_built,
                "failed": len(self.failed_resources),
            },
            "dependencies": {
                "total": len(self.dependencies),
                "internal": len(self.internal_dependencies_only),
                "external": len(self.external_dependencies_only),
                "unresolved": len(self.unresolved_dependencies),
            },
            "insights": {
                "syntax_errors": self.insights["syntax_errors"],
                "consistency_errors": self.insights["consistency_errors"],
                "warnings": self.insights["warnings"],
                "recommendations": self.insights["recommendations"],
                "total": sum(self.insights.values()),
            },
            "overall_status": "SUCCESS" if self.build_successful else "FAILED",
        }

    # ==================== Methods for Analysis ====================

    def get_module_lineage(self, module_id: RelativeDirPath, iteration: int = 0) -> ModuleLineageItem | None:
        """Get lineage for a specific module and iteration."""
        lineages = self.module_lineage_items.get(module_id, [])
        for lineage in lineages:
            if lineage.iteration == iteration:
                return lineage
        return None

    def get_resource_lineage(self, resource_id: Identifier) -> ResourceLineageItem | None:
        """Get lineage for a specific resource."""
        for lineages in self.module_lineage_items.values():
            for lineage in lineages:
                for resource in lineage.resource_lineage_items.values():
                    if resource.resource_id == resource_id:
                        return resource
        return None

    def get_dependency_chain(self, resource_id: Identifier) -> list[list[Identifier]]:
        """Get all dependency chains for a resource (depth-first paths)."""
        chains: list[list[Identifier]] = []

        def dfs(current: Identifier, path: list[Identifier]) -> None:
            path.append(current)
            dependencies = self.dependency_graph.get(current, [])
            if not dependencies:
                chains.append(path[:])
            else:
                for dep in dependencies:
                    dfs(dep, path)
            path.pop()

        dfs(resource_id, [])
        return chains

    def get_dependents_chain(self, resource_id: Identifier) -> list[list[Identifier]]:
        """Get all dependent chains (reverse dependency paths)."""
        chains: list[list[Identifier]] = []

        def dfs(current: Identifier, path: list[Identifier]) -> None:
            path.append(current)
            dependents = self.dependents_graph.get(current, [])
            if not dependents:
                chains.append(path[:])
            else:
                for dependent in dependents:
                    dfs(dependent, path)
            path.pop()

        dfs(resource_id, [])
        return chains

"""Build lineage tracking classes for comprehensive build process traceability."""

from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuildFolder, BuildParameters, BuiltModule
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    ConsistencyError,
    Insight,
    ModelSyntaxError,
    Recommendation,
)

from ._types import AbsoluteDirPath, AbsoluteFilePath, RelativeDirPath, ValidationType


class BuildConfigLineage(BaseModel):
    """Tracks build configuration and environment."""

    organization_dir: AbsoluteDirPath = Field(description="Organization root directory")
    build_dir: AbsoluteDirPath = Field(description="Build output directory")
    cdf_project: str = Field(description="Target CDF project")
    validation_type: ValidationType = Field(description="Validation type (prod/dev)")
    selected_modules: set[RelativeDirPath | str] = Field(description="Selected modules for build")
    variables_provided: dict[str, JsonValue] = Field(
        default_factory=dict, description="Variables provided via config or command line"
    )


class ResourceLineageItem(BaseModel):
    """Tracks a single resource through the build process."""

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    source_file: AbsoluteFilePath
    source_hash: str = Field(description="Hash of source file content (before variable substitution)")
    type_: str = Field(alias="type", description="Resource type folder (e.g., 'spaces', 'containers', 'views')")
    kind: str = Field(description="Resource kind (e.g., 'space', 'container', 'view')")

    built_file: AbsoluteFilePath


class ModuleLineageItem(BaseModel):
    """Tracks a module through the build process."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    module_id: str = Field(description="Module identifier (e.g., modules/my_module)")
    module_path: AbsoluteDirPath = Field(description="Absolute path to module source directory")

    # Resource tracking
    resource_lineage: list[ResourceLineageItem] = Field(
        default_factory=list, description="List of resource lineage items for this module"
    )

    # Insights breakdown at module level
    insights_summary: dict[type[Insight], int] = Field(description="Breakdown of insights by type for this module")

    @property
    def is_success(self) -> bool:
        """Determine if module build was successful based on insights summary."""
        return (
            self.insights_summary.get(ModelSyntaxError, 0) == 0
            and self.insights_summary.get(ConsistencyError, 0) == 0
            and bool(self.resource_lineage)
        )

    @classmethod
    def from_built_module(cls, module: BuiltModule) -> "ModuleLineageItem":
        """Construct lineage item from built module."""

        resource_lineage = []

        for type_, file_by_kind in module.resource_by_type_by_kind.items():
            for kind, files in file_by_kind.items():
                for built_file in files:
                    source_file = module.built_files_by_source.get(built_file)
                    if source_file is None:
                        raise RuntimeError("This is a bug - built file does not have a corresponding source file.")

                    resource_lineage.append(
                        ResourceLineageItem(
                            source_file=source_file,
                            source_hash="",  # Todo: Store source hash in built module for accurate lineage.
                            type_=type_,
                            kind=kind,
                            built_file=built_file,
                        )
                    )

        return cls(
            module_id=module.source.id.as_posix(),
            module_path=module.source.path,
            resource_lineage=resource_lineage,
            insights_summary={type_: len(insights) for type_, insights in module.insights.by_type().items()},
        )

    def to_dict(self, organization_dir: Path | None = None) -> dict[str, Any]:
        """Generate a dictionary representation of ModuleLineageItem containing only string values."""
        simple_dict = {
            "moduleId": self.module_id,
            "modulePath": str(self.module_path.relative_to(organization_dir))
            if organization_dir
            else str(self.module_path),
            "resources": [
                {
                    "sourceFile": str(item.source_file.relative_to(organization_dir))
                    if organization_dir
                    else str(item.source_file),
                    "type": item.type_,
                    "kind": item.kind,
                    "builtFile": str(item.built_file.relative_to(organization_dir))
                    if item.built_file and organization_dir
                    else (str(item.built_file) if item.built_file else None),
                }
                for item in self.resource_lineage
            ],
            "insightsSummary": {insight_type.__name__: count for insight_type, count in self.insights_summary.items()},
            "status": "SUCCESS" if self.is_success else "FAILED",
        }

        if simple_dict["status"] == "FAILED":
            if self.insights_summary.get(ModelSyntaxError, 0) > 0:
                simple_dict["failureReasons"] = "ModelSyntaxError"
            elif self.insights_summary.get(ConsistencyError, 0) > 0:
                simple_dict["failureReasons"] = "ConsistencyError"

        return simple_dict


class BuildLineage(BaseModel):
    """Minimal linage"""

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    # Build metadata
    timestamp: datetime = Field(default_factory=datetime.now, description="When build started", alias="timestamp")
    duration: float | None = Field(None, description="Total build duration in seconds", alias="duration")

    organization_dir: Path = Field(alias="organizationDirectory")
    build_dir: Path = Field(alias="buildDirectory")

    # Module tracking
    module_lineage: list[ModuleLineageItem] = Field(
        default_factory=list,
        alias="moduleLineage",
        description="Lineage for each module, indexed by module path. List because of iterations.",
    )

    @property
    def modules_summary(self) -> dict[str, int]:
        return {
            "processed": len(self.module_lineage),
            "succeeded": sum(1 for module in self.module_lineage if module.is_success),
            "failed": sum(1 for module in self.module_lineage if not module.is_success),
        }

    @property
    def insights_summary(self) -> dict[type[Insight], int]:

        summary: dict[type[Insight], int] = {ModelSyntaxError: 0, ConsistencyError: 0, Recommendation: 0}

        for module in self.module_lineage:
            for insight_type, count in module.insights_summary.items():
                summary[insight_type] += count

        return summary

    @classmethod
    def from_build_parameters_and_results(
        cls,
        parameters: BuildParameters,
        folder: BuildFolder,
        timestamp: datetime | None = None,
        duration: float | None = None,
    ) -> "BuildLineage":
        """Construct lineage from build output folder."""

        return cls(
            timestamp=timestamp or datetime.now(),
            duration=duration,
            organization_dir=parameters.organization_dir,
            build_dir=parameters.build_dir,
            module_lineage=[ModuleLineageItem.from_built_module(module) for module in folder.built_modules],
        )

    def to_dict(self) -> dict[str, Any]:
        """Generate a dictionary representation of BuildLineage containing only string values."""

        is_relative = self.build_dir.is_relative_to(self.organization_dir)

        return {
            "timestamp": self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "duration": round(self.duration, 2) if self.duration is not None else None,
            "organizationDirectory": str(self.organization_dir),
            "buildDirectory": str(self.build_dir.relative_to(self.organization_dir))
            if is_relative
            else str(self.build_dir),
            "modulesSummary": self.modules_summary,
            "insightsSummary": {insight_type.__name__: count for insight_type, count in self.insights_summary.items()},
            "moduleLineage": [
                module.to_dict(self.organization_dir if is_relative else None) for module in self.module_lineage
            ],
        }

    def to_yaml(self) -> str:
        """Convert BuildLineage to YAML string representation."""
        data = self.to_dict()
        return yaml.dump(data, default_flow_style=False, sort_keys=False)

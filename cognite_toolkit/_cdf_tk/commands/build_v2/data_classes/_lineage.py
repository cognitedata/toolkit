from collections import defaultdict
from datetime import datetime
from pathlib import Path

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializationInfo,
    computed_field,
    field_serializer,
)

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuildFolder, BuildParameters, BuiltModule
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    ConsistencyError,
    ModelSyntaxError,
)
from tests.data.complete_org_alpha_flags.modules.my_example_module.functions.fn_multi_file_function.other_module import (
    to_camel,
)

from ._types import AbsoluteDirPath, AbsoluteFilePath


class _BaseLineageModel(BaseModel):
    """Base model for lineage tracking with common configuration."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)


class ResourceLineageItem(_BaseLineageModel):
    """Tracks a single resource through the build process."""

    source_file: AbsoluteFilePath
    source_hash: str
    type_: str = Field(alias="type", description="Resource type folder (e.g., 'spaces', 'containers', 'views')")
    kind: str = Field(description="Resource kind (e.g., 'space', 'container', 'view')")
    built_file: AbsoluteFilePath

    @field_serializer("source_file", "built_file", when_used="json")
    def serialize_paths(self, value: Path, info: SerializationInfo) -> str:
        """Serialize absolute paths to strings."""
        organization_dir = info.context.get("organization_dir") if info.context else None
        if organization_dir and value.is_relative_to(organization_dir):
            return value.relative_to(organization_dir).as_posix()
        return value.as_posix()


class ModuleLineageItem(_BaseLineageModel):
    """Tracks a module through the build process."""

    module_id: str = Field(description="Module identifier (e.g., modules/my_module)")
    module_path: AbsoluteDirPath = Field(description="Absolute path to module source directory")
    insights_summary: dict[str, int] = Field(description="Breakdown of insights by type for this module")
    resource_lineage: list[ResourceLineageItem] = Field(
        default_factory=list, description="List of resource lineage items for this module"
    )

    @property
    def is_success(self) -> bool:
        """Determine if module build was successful based on insights summary."""
        return (
            self.insights_summary.get(ModelSyntaxError.__name__, 0) == 0
            and self.insights_summary.get(ConsistencyError.__name__, 0) == 0
            and bool(self.resource_lineage)
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def status(self) -> str:
        """Overall status of the module build based on insights summary."""
        if self.is_success:
            return "SUCCESS"
        elif self.insights_summary.get(ModelSyntaxError.__name__, 0) > 0:
            return "FAILED: ModelSyntaxError"
        elif self.insights_summary.get(ConsistencyError.__name__, 0) > 0:
            return "FAILED: ConsistencyError"
        else:
            return "FAILED: Unknown reason"

    @field_serializer("module_path", when_used="json")
    def serialize_module_path(self, value: Path, info: SerializationInfo) -> str:
        """Serialize module path to relative path."""
        organization_dir = info.context.get("organization_dir") if info.context else None
        if organization_dir and value.is_relative_to(organization_dir):
            return str(value.relative_to(organization_dir))
        return str(value)

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
                            source_hash="",
                            type_=type_,
                            kind=kind,
                            built_file=built_file,
                        )
                    )

        return cls(
            module_id=module.source.id.as_posix(),
            module_path=module.source.path,
            resource_lineage=resource_lineage,
            insights_summary=module.insights.summary,
        )


class BuildLineage(_BaseLineageModel):
    """Minimal lineage"""

    timestamp: datetime = Field(default_factory=datetime.now, description="When build started")
    duration: float | None = Field(None, description="Total build duration in seconds")
    organization_dir: Path
    build_dir: Path
    modules_summary: dict[str, int] = Field(description="Summary of modules by build status")
    insights_summary: dict[str, int] = Field(description="Summary of insights by type across all modules")

    module_lineage: list[ModuleLineageItem] = Field(
        default_factory=list,
        description="Lineage for each module, indexed by module path. List because of iterations.",
    )

    @field_serializer("timestamp", when_used="json")
    def serialize_timestamp(self, value: datetime, info: SerializationInfo) -> str:
        """Serialize timestamp to string."""
        return value.replace(microsecond=0).isoformat()

    @field_serializer("organization_dir", "build_dir", when_used="json")
    def serialize_paths(self, value: Path, info: SerializationInfo) -> str:
        """Serialize build_dir to relative path if possible."""
        if info.field_name == "build_dir":  # type: ignore
            organization_dir = info.context.get("organization_dir") if info.context else None
            if organization_dir and value.is_relative_to(organization_dir):
                return value.relative_to(organization_dir).as_posix()
        return value.as_posix()

    @field_serializer("duration", when_used="json")
    def serialize_duration(self, value: float | None, info: SerializationInfo) -> float | None:
        """Serialize duration with 2 decimal places."""
        return round(value, 2) if value is not None else None

    @classmethod
    def from_build_parameters_and_results(
        cls,
        parameters: BuildParameters,
        folder: BuildFolder,
        timestamp: datetime | None = None,
        duration: float | None = None,
    ) -> "BuildLineage":
        """Construct lineage from build output folder."""

        module_lineage = [ModuleLineageItem.from_built_module(module) for module in folder.built_modules]
        modules_summary = {
            "processed": len(module_lineage),
            "succeeded": sum(1 for module in module_lineage if module.is_success),
            "failed": sum(1 for module in module_lineage if not module.is_success),
        }
        insights_summary: dict[str, int] = defaultdict(int)
        for module in module_lineage:
            for insight_type, count in module.insights_summary.items():
                insights_summary[insight_type] += count

        return cls(
            timestamp=timestamp or datetime.now(),
            duration=duration,
            organization_dir=parameters.organization_dir,
            build_dir=parameters.build_dir,
            module_lineage=module_lineage,
            modules_summary=modules_summary,
            insights_summary=insights_summary,
        )

    def to_yaml(self) -> str:
        """Convert BuildLineage to YAML string representation."""
        data = self.model_dump(
            by_alias=True,
            exclude_none=False,
            mode="json",
            context={"organization_dir": self.organization_dir},
        )
        return yaml.dump(data, default_flow_style=False, sort_keys=False)

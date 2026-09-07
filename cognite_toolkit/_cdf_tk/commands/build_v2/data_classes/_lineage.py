from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar, cast

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializationInfo,
    ValidationError,
    computed_field,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel
from pydantic_core.core_schema import ValidationInfo

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuildFolder, BuiltModule
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    ConsistencyError,
    ModelSyntaxError,
)
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitMissingResourceError,
    ToolkitValidationError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.resource_ios import ResourceIO, get_crud
from cognite_toolkit._cdf_tk.utils import (
    calculate_directory_hash,
    calculate_hash,
    load_yaml_inject_variables,
    read_yaml_content,
    safe_read,
)
from cognite_toolkit._cdf_tk.validation import humanize_validation_error

from ._module import BuildVariable, ResourceType
from ._types import AbsoluteDirPath, AbsoluteFilePath


class _BaseLineageModel(BaseModel):
    """Base model for lineage tracking with common configuration."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)


class ResourceLineageItem(_BaseLineageModel):
    """Tracks a single resource through the build process."""

    source_file: AbsoluteFilePath
    source_hash: str
    type: ResourceType
    built_file: AbsoluteFilePath
    identifier: Identifier
    variables: list[BuildVariable] = Field(default_factory=list, exclude=True)

    @field_validator("identifier", mode="plain")
    @classmethod
    def load_identifier(cls, value: Any, info: ValidationInfo) -> Any:
        """Load identifier from dict using ResourceType."""
        if not isinstance(value, dict):
            return value
        if "type" not in info.data:
            raise ValueError("Resource type must be provided to load identifier")
        resource_type: ResourceType = info.data["type"]
        return resource_type.load_identifier(value)

    @field_serializer("identifier", when_used="always")
    def serialize_identifier(self, value: Identifier) -> dict[str, Any]:
        return value.dump()

    def load_resource_dict(
        self, environment_variables: dict[str, str | None], validate: bool = False
    ) -> dict[str, Any]:
        content = BuildVariable.substitute(safe_read(self.source_file), self.variables, self.source_file.suffix)
        resource_io = cast(type[ResourceIO], get_crud(self.type.resource_folder, self.type.kind))
        raw = load_yaml_inject_variables(
            content,
            environment_variables,
            validate=validate,
            original_filepath=self.source_file,
        )
        if isinstance(raw, dict):
            return raw
        elif isinstance(raw, list):
            for item in raw:
                if resource_io.get_id(item) == self.identifier:
                    return item
        raise ToolkitMissingResourceError(f"Resource {self.identifier} not found in {self.source_file}")


class ModuleLineageItem(_BaseLineageModel):
    """Tracks a module through the build process."""

    module_id: str = Field(description="Module identifier (e.g., modules/my_module)")
    module_path: AbsoluteDirPath = Field(description="Absolute path to module source directory")
    module_hash: str = Field(
        default="",
        description="Hash of the module source directory at build time, used for incremental rebuilds.",
    )
    insights_summary: dict[str, int] = Field(description="Breakdown of insights by type for this module")
    variables: list[BuildVariable] = Field(
        default_factory=list,
        description="Build variables for this module. Stored so source files can be reloaded from the tmp_build cache.",
    )
    resource_lineage: list[ResourceLineageItem] = Field(
        default_factory=list, description="List of resource lineage items for this module"
    )

    @model_validator(mode="after")
    def _propagate_variables_to_resources(self) -> "ModuleLineageItem":
        if not self.variables:
            return self
        for resource in self.resource_lineage:
            if not resource.variables:
                resource.variables = self.variables
        return self

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

    @classmethod
    def from_built_module(cls, module: BuiltModule) -> "ModuleLineageItem":
        """Construct lineage item from built module."""
        resource_lineage = []
        for resource in module.resources:
            resource_lineage.append(
                ResourceLineageItem(
                    source_file=resource.source_path.resolve(),
                    source_hash=resource.source_hash,
                    built_file=resource.build_path.resolve(),
                    type=resource.type,
                    identifier=resource.identifier,
                    variables=module.variables,
                )
            )
        module_path = module.module_id.path.resolve()
        return cls(
            module_id=module.module_id.id.as_posix(),
            module_path=module_path,
            module_hash=calculate_directory_hash(module_path, shorten=True),
            resource_lineage=resource_lineage,
            insights_summary=module.all_insights.summary,
            variables=module.variables,
        )


class BuildLineage(_BaseLineageModel):
    """Minimal lineage"""

    filename: ClassVar[str] = "lineage.yaml"
    timestamp: datetime = Field(default_factory=datetime.now, description="When build started")
    duration: float | None = Field(None, description="Total build duration in seconds")
    organization_dir: Path
    build_dir: Path
    cdf_project: str | None = None
    config_hash: str | None = Field(
        default=None,
        description="Hash of the config YAML file at build time. Used to invalidate the incremental rebuild cache when the config changes.",
    )
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
    def from_build(
        cls, build: BuildFolder, cdf_project: str | None = None, config_yaml: Path | None = None
    ) -> "BuildLineage":
        """Construct lineage from build output folder."""

        module_lineage = [ModuleLineageItem.from_built_module(module) for module in build.built_modules]
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
            timestamp=build.started_at,
            duration=build.build_duration_seconds,
            organization_dir=build.organization_dir,
            build_dir=build.build_dir,
            module_lineage=module_lineage,
            modules_summary=modules_summary,
            insights_summary=insights_summary,
            cdf_project=cdf_project,
            config_hash=calculate_hash(config_yaml, shorten=True) if config_yaml is not None else None,
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

    @classmethod
    def from_yaml_file(cls, yaml_file: Path) -> "BuildLineage":
        """Load BuildLineage from a YAML file."""
        try:
            # The lineage file should always be located in a build folder.
            content = read_yaml_content(yaml_file.read_text(encoding=BUILD_FOLDER_ENCODING))
        except yaml.YAMLError as e:
            raise ToolkitYAMLFormatError(f"Invalid YAML in {yaml_file.as_posix()}: {e}") from e
        try:
            return cls.model_validate(content, extra="ignore")
        except ValidationError as e:
            errors = " - ".join(humanize_validation_error(e))
            raise ToolkitValidationError(f"Invalid lineage format in {yaml_file.as_posix()}:\n{errors}") from e

    def validate_source_files_unchanged(self) -> None:
        """Validate that source files have not changed since the build.

        Reads each source file tracked in the lineage and compares its current hash
        with the stored hash from build time. Uses the same hashing method as
        BuildV2Command (calculate_hash with shorten=True on the raw file Path).

        Raises:
            ToolkitValidationError: If any source file has changed or is missing.
        """
        changed_files: list[str] = []
        missing_files: list[str] = []
        for module in self.module_lineage:
            for resource in module.resource_lineage:
                source_file = resource.source_file

                if not source_file.exists():
                    missing_files.append(source_file.as_posix())
                    continue

                try:
                    current_hash = calculate_hash(source_file, shorten=True)
                except Exception:
                    missing_files.append(source_file.as_posix())
                    continue

                if current_hash != resource.source_hash:
                    changed_files.append(source_file.as_posix())

        errors: list[str] = []
        if missing_files:
            errors.append(f"Missing source files: {', '.join(missing_files)}")
        if changed_files:
            errors.append(f"Changed source files: {', '.join(changed_files)}")

        if errors:
            raise ToolkitValidationError(
                "Source files have changed since the build. Please rebuild before deploying.\n" + "\n".join(errors)
            )

    def get_resource_of_type(self, resource_type: ResourceType) -> list[ResourceLineageItem]:
        """Get all resources of a specific type from the lineage."""
        resources: list[ResourceLineageItem] = []
        for module in self.module_lineage:
            for resource in module.resource_lineage:
                if resource.type == resource_type:
                    resources.append(resource)
        return resources

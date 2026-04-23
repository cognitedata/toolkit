"""Data class for command tracking information."""

from collections import Counter
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ResourceType
from cognite_toolkit._cdf_tk.dataio.logger import ItemsResult

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuildFolder
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import InsightList


def _to_tracking_key(display_name: str) -> str:
    """Convert a resource label to a camelCase tracking key prefix (matches deploy_v2).

    Examples:
        "Data Sets" -> "dataSets"
        "Space data modeling" -> "spaceDataModeling"
    """
    words = display_name.replace("-", " ").split()
    if not words:
        return display_name.lower()
    return words[0].lower() + "".join(word.capitalize() for word in words[1:])


def _tracking_label_for_resource_type(resource_type: ResourceType) -> str:
    """Build a spaced label suitable for `_to_tracking_key` from a build ResourceType."""
    parts: list[str] = []
    for segment in (resource_type.kind, resource_type.resource_folder):
        parts.extend(segment.replace("-", " ").replace("_", " ").split())
    return " ".join(parts)


class TrackingEvent(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    event_name: str = Field(exclude=True)

    def to_dict(self) -> dict[str, Any]:
        """Convert the tracking info to a dictionary for Mixpanel.

        Returns:
            A dictionary with camelCase keys matching Mixpanel's expected format.
            Default values are excluded.
        """
        return self.model_dump(mode="json", by_alias=True, exclude_defaults=True)


class CommandTracking(TrackingEvent):
    """Structured tracking information for CLI commands.

    This model provides type-safe tracking information that can be collected
    during command execution and sent to Mixpanel for analytics.

    Attributes:
        module_ids: List of module IDs that were deployed or built.
        package_ids: List of package IDs that were deployed or built.
        installed_module_ids: List of module IDs that were installed.
        installed_package_ids: List of package IDs that were installed.
        downloaded_library_ids: List of library IDs that were downloaded.
        downloaded_package_ids: List of package IDs that were downloaded.
        downloaded_module_ids: List of module IDs that were downloaded.
    """

    module_ids: set[str] = Field(default_factory=set)
    package_ids: set[str] = Field(default_factory=set)
    installed_module_ids: set[str] = Field(default_factory=set)
    installed_package_ids: set[str] = Field(default_factory=set)
    downloaded_library_ids: set[str] = Field(default_factory=set)
    downloaded_package_ids: set[str] = Field(default_factory=set)
    downloaded_module_ids: set[str] = Field(default_factory=set)
    function_validation_count: int = Field(default=0)
    function_validation_failures: int = Field(default=0)
    function_validation_credential_errors: int = Field(default=0)
    function_validation_time_ms: int = Field(default=0)
    warning_total_count: int = Field(default=0)
    result: str = Field(default="")
    error: str | None = Field(default=None)
    subcommands: list[str] = Field(default_factory=list)
    alpha_flags: list[str] = Field(default_factory=list)
    plugins: list[str] = Field(default_factory=list)


class DataTracking(TrackingEvent):
    """Structured tracking information for CLI commands."""

    model_config = ConfigDict(extra="allow")
    event_name: Literal["DownloadResult", "UploadResult", "MigrationResult", "PurgeResult"] = Field(exclude=True)
    data_type: str
    total: int

    @classmethod
    def from_item_results(
        cls,
        event_name: Literal["DownloadResult", "UploadResult", "MigrationResult", "PurgeResult"],
        data_type: str,
        item_results: list[ItemsResult],
    ) -> "DataTracking":
        total = sum(result.count for result in item_results)
        tracking_data = {"eventName": event_name, "dataType": data_type, "total": total}
        for result in item_results:
            tracking_data[result.status] = result.count
        return cls.model_validate(tracking_data)


class DeploymentTracking(TrackingEvent):
    """Structured tracking information for deployment commands.

    This model uses a flattened structure for Mixpanel compatibility.
    Per-resource stats are stored as dynamic fields like "dataSets_created", "spaces_updated", etc.

    Attributes:
        is_dry_run: Whether this was a dry run.
        operation: The operation performed (deploy or clean).
        resource_types: List of resource type names that were deployed.
        total_created: Total resources created across all types.
        total_updated: Total resources updated across all types.
        total_deleted: Total resources deleted across all types.
        total_unchanged: Total unchanged resources across all types.
        total_skipped: Total skipped resources across all types.
        total_resources: Total resources across all types.
        resource_type_count: Number of different resource types deployed.
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="allow")

    event_name: Literal["DeploymentResult"] = Field("DeploymentResult", exclude=True)
    is_dry_run: bool = False
    operation: str = "deploy"
    resource_types: list[str] = Field(default_factory=list)
    total_created: int = 0
    total_updated: int = 0
    total_deleted: int = 0
    total_unchanged: int = 0
    total_skipped: int = 0
    total_resources: int = 0
    resource_type_count: int = 0


class BuildTracking(TrackingEvent):
    """Structured tracking information for build v2 (`cdf build`).

    Per-resource-type built counts use flattened Mixpanel fields such as ``spaceDataModelingBuilt``,
    matching the ``DeploymentTracking`` pattern (``extra="allow"``).
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="allow")

    event_name: Literal["BuildResult"] = Field("BuildResult", exclude=True)
    build_duration_ms: int = 0
    resource_types: list[str] = Field(default_factory=list)
    insight_codes: list[str] = Field(default_factory=list)
    dependency_total: int = 0
    dependency_average: float = 0.0
    built_resource_total: int = 0
    module_count: int = 0
    insight_total_count: int = 0

    @classmethod
    def from_build_folder(
        cls,
        build_folder: "BuildFolder",
        insights: "InsightList",
    ) -> "BuildTracking":
        built_resources = [resource for module in build_folder.built_modules for resource in module.resources]
        duration_ms = int(build_folder.build_duration_seconds * 1000)

        label_counts: Counter[str] = Counter()
        for resource in built_resources:
            label_counts[_tracking_label_for_resource_type(resource.type)] += 1

        per_type_built: dict[str, int] = {}
        for label, count in label_counts.items():
            prefix = _to_tracking_key(label)
            per_type_built[f"{prefix}Built"] = count

        dependency_total = sum(len(resource.dependencies) for resource in built_resources)
        built_count = len(built_resources)
        dependency_average = round((dependency_total / built_count), 6) if built_count else 0.0

        insight_codes_set = {ins.code if ins.code is not None else "UNDEFINED" for ins in insights}

        payload: dict[str, Any] = {
            "build_duration_ms": duration_ms,
            "resource_types": sorted(label_counts.keys()),
            "insight_codes": sorted(insight_codes_set),
            "dependency_total": dependency_total,
            "dependency_average": dependency_average,
            "built_resource_total": built_count,
            "module_count": len(build_folder.built_modules),
            "insight_total_count": len(insights),
            "event_name": "BuildResult",
        }
        payload.update(per_type_built)
        return cls.model_validate(payload)

"""Data class for command tracking information."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.dataio.logger import ItemsResult


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


class ResourceDeploymentStats(BaseModel):
    """Statistics for a single resource type deployment."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    resource_name: str
    created: int = 0
    updated: int = 0
    deleted: int = 0
    unchanged: int = 0
    skipped: int = 0
    total: int = 0


class DeploymentTracking(TrackingEvent):
    """Structured tracking information for deployment commands.

    Attributes:
        is_dry_run: Whether this was a dry run.
        operation: The operation performed (deploy or clean).
        resource_stats: List of statistics per resource type.
        total_created: Total resources created across all types.
        total_updated: Total resources updated across all types.
        total_deleted: Total resources deleted across all types.
        total_unchanged: Total unchanged resources across all types.
        total_skipped: Total skipped resources across all types.
        total_resources: Total resources across all types.
        resource_type_count: Number of different resource types deployed.
    """

    event_name: Literal["DeploymentResult"] = Field("DeploymentResult", exclude=True)
    is_dry_run: bool = False
    operation: str = "deploy"
    resource_stats: list[ResourceDeploymentStats] = Field(default_factory=list)
    total_created: int = 0
    total_updated: int = 0
    total_deleted: int = 0
    total_unchanged: int = 0
    total_skipped: int = 0
    total_resources: int = 0
    resource_type_count: int = 0

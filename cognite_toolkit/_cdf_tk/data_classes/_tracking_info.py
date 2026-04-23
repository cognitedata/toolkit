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

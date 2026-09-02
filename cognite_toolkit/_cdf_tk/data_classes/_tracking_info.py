"""Data class for command tracking information."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.dataio.logger import ItemsResult
from cognite_toolkit._cdf_tk.utils.coding_agent import InvocationSource

# --- Mixpanel ingestion safety limits ---------------------------------------
# Documented Mixpanel limits we defensively guard against:
#   * String property values are truncated at 255 chars (silently).
#   * Nested objects/arrays deeper than 3 levels are rejected/flattened.
#   * 255 properties per event.
# Our events sit well within these; the caps below are belt-and-suspenders so
# a future change (e.g. a new free-form field or an explosion in resource
# kinds) cannot silently corrupt analytics data.
MP_STRING_LIMIT: int = 255
MP_MAX_LIST_ITEMS: int = 250


def _mp_safe_str(value: str, limit: int = MP_STRING_LIMIT) -> str:
    """Truncate a string so it will not be silently cut off by Mixpanel."""
    if len(value) <= limit:
        return value
    # Reserve one char for the ellipsis so total length stays <= limit.
    return value[: limit - 1] + "…"


def _mp_safe_str_list(values: list[str]) -> list[str]:
    """Truncate each element and cap the list length."""
    return [_mp_safe_str(v) for v in values[:MP_MAX_LIST_ITEMS]]


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
        installed_module_ids: List of module IDs that were installed.
        installed_package_ids: List of package IDs that were installed.
        downloaded_library_ids: List of library IDs that were downloaded.
        downloaded_package_ids: List of package IDs that were downloaded.
        downloaded_module_ids: List of module IDs that were downloaded.
    """

    installed_module_ids: set[str] = Field(default_factory=set)
    installed_package_ids: set[str] = Field(default_factory=set)
    downloaded_library_ids: set[str] = Field(default_factory=set)
    downloaded_package_ids: set[str] = Field(default_factory=set)
    downloaded_module_ids: set[str] = Field(default_factory=set)

    warning_total_count: int = Field(default=0)
    result: str = Field(default="")
    error: str | None = Field(default=None)
    subcommands: list[str] = Field(default_factory=list)
    alpha_flags: list[str] = Field(default_factory=list)
    plugins: list[str] = Field(default_factory=list)
    invocation_source: InvocationSource | None = Field(default=None)
    coding_agent: str | None = Field(default=None)

    @field_validator("result", mode="after")
    @classmethod
    def _truncate_result(cls, v: str) -> str:
        return _mp_safe_str(v)

    @field_validator("error", mode="after")
    @classmethod
    def _truncate_error(cls, v: str | None) -> str | None:
        return _mp_safe_str(v) if v is not None else None


class DataTracking(TrackingEvent):
    """Structured tracking information for CLI commands."""

    event_name: Literal["DownloadResult", "UploadResult", "MigrationResult", "PurgeResult"] = Field(exclude=True)
    data_type: str
    total: int
    success: int = 0
    failure: int = 0
    pending: int = 0
    skipped: int = 0
    success_with_warning: int = Field(default=0, alias="success-with-warning")
    pending_with_warning: int = Field(default=0, alias="pending-with-warning")

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


class ResourceDeploymentStat(BaseModel):
    """Per-resource-type deployment counts, nested inside ``DeploymentTracking``.

    Kept as a flat set of scalars — do NOT add nested lists/dicts here, as
    Mixpanel only supports 3 levels of nesting and this model already sits at
    level 3 (event → resource_stats[] → this).
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    resource_name: str
    created: int = 0
    updated: int = 0
    deleted: int = 0
    unchanged: int = 0
    skipped: int = 0
    total: int = 0

    @field_validator("resource_name", mode="after")
    @classmethod
    def _truncate_resource_name(cls, v: str) -> str:
        return _mp_safe_str(v)


class DeploymentTracking(TrackingEvent):
    """Structured tracking information for deployment commands.

    Per-resource stats are carried in the ``resource_stats`` list rather than
    being flattened into top-level dynamic properties. This keeps the Mixpanel
    schema fixed regardless of how many resource kinds Toolkit adds and avoids
    silent property-count sprawl.
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    event_name: Literal["DeploymentResult"] = Field("DeploymentResult", exclude=True)
    is_dry_run: bool = False
    operation: str = "deploy"
    resource_types: list[str] = Field(default_factory=list)
    resource_stats: list[ResourceDeploymentStat] = Field(default_factory=list)
    total_created: int = 0
    total_updated: int = 0
    total_deleted: int = 0
    total_unchanged: int = 0
    total_skipped: int = 0
    total_resources: int = 0
    resource_type_count: int = 0

    @field_validator("resource_types", mode="after")
    @classmethod
    def _cap_resource_types(cls, v: list[str]) -> list[str]:
        return _mp_safe_str_list(v)

    @field_validator("resource_stats", mode="after")
    @classmethod
    def _cap_resource_stats(cls, v: list[ResourceDeploymentStat]) -> list[ResourceDeploymentStat]:
        return v[:MP_MAX_LIST_ITEMS]


class ResourceBuildStat(BaseModel):
    """Per-resource-type build counts, nested inside ``BuildTracking``.

    Kept as a flat set of scalars — do NOT add nested lists/dicts here, as
    Mixpanel only supports 3 levels of nesting and this model already sits at
    level 3 (event → resource_stats[] → this).
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    resource_folder: str
    kind: str
    built_count: int = 0
    dependency_total: int = 0
    syntax_error_count: int = 0

    @field_validator("resource_folder", "kind", mode="after")
    @classmethod
    def _truncate_names(cls, v: str) -> str:
        return _mp_safe_str(v)


class BuildTracking(TrackingEvent):
    """Structured tracking information for build v2 (`cdf build`).

    Per-resource-type built counts are carried in the ``resource_stats`` list
    rather than being flattened into top-level dynamic properties. This keeps
    the Mixpanel schema fixed regardless of how many resource kinds Toolkit
    adds and avoids silent property-count sprawl.
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    event_name: Literal["BuildResult"] = Field("BuildResult", exclude=True)
    build_duration_ms: int = 0
    resource_types: list[str] = Field(default_factory=list)
    resource_stats: list[ResourceBuildStat] = Field(default_factory=list)
    insight_codes: list[str] = Field(default_factory=list)
    dependency_total: int = 0
    dependency_average: float = 0.0
    built_resource_total: int = 0
    module_count: int = 0
    insight_total_count: int = 0
    yaml_line_count: int = 0

    @field_validator("resource_types", "insight_codes", mode="after")
    @classmethod
    def _cap_str_lists(cls, v: list[str]) -> list[str]:
        return _mp_safe_str_list(v)

    @field_validator("resource_stats", mode="after")
    @classmethod
    def _cap_resource_stats(cls, v: list[ResourceBuildStat]) -> list[ResourceBuildStat]:
        return v[:MP_MAX_LIST_ITEMS]

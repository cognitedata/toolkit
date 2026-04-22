"""Data class for command tracking information."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class TrackingEvent(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)
    event_name: str = Field(exclude=True)
    project: str | None = Field(default=None)
    cluster: str | None = Field(default=None)
    organization: str | None = Field(default=None)

    def to_dict(self) -> dict[str, Any]:
        """Convert the tracking info to a dictionary for Mixpanel.

        Returns:
            A dictionary with camelCase keys matching Mixpanel's expected format.
            Default values are excluded.
        """
        return self.model_dump(mode="json", by_alias=True, exclude_defaults=True)


class CommandTrackingInfo(TrackingEvent):
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

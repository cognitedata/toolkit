"""Data class for command tracking information."""

from typing import Any

from pydantic import BaseModel, Field


class CommandTrackingInfo(BaseModel):
    """Structured tracking information for CLI commands.

    This model provides type-safe tracking information that can be collected
    during command execution and sent to Mixpanel for analytics.

    Attributes:
        project: The CDF project name.
        cluster: The CDF cluster name.
        module_ids: List of module IDs that were deployed or built.
        package_ids: List of package IDs that were deployed or built.
        installed_module_ids: List of module IDs that were installed.
        installed_package_ids: List of package IDs that were installed.
        downloaded_library_ids: List of library IDs that were downloaded.
        downloaded_package_ids: List of package IDs that were downloaded.
        downloaded_module_ids: List of module IDs that were downloaded.
    """

    project: str | None = Field(default=None)
    cluster: str | None = Field(default=None)
    module_ids: set[str] = Field(default_factory=set, alias="moduleIds")
    package_ids: set[str] = Field(default_factory=set, alias="packageIds")
    installed_module_ids: set[str] = Field(default_factory=set, alias="installedModuleIds")
    installed_package_ids: set[str] = Field(default_factory=set, alias="installedPackageIds")
    downloaded_library_ids: set[str] = Field(default_factory=set, alias="downloadedLibraryIds")
    downloaded_package_ids: set[str] = Field(default_factory=set, alias="downloadedPackageIds")
    downloaded_module_ids: set[str] = Field(default_factory=set, alias="downloadedModuleIds")

    def to_dict(self) -> dict[str, Any]:
        """Convert the tracking info to a dictionary for Mixpanel.

        Returns:
            A dictionary with camelCase keys matching Mixpanel's expected format.
            Default values are excluded.
        """
        return self.model_dump(mode="json", by_alias=True, exclude_defaults=True)

"""Data class for command tracking information."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class CommandTrackingInfo:
    """Structured tracking information for CLI commands.

    This dataclass provides type-safe tracking information that can be collected
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

    project: str | None = None
    cluster: str | None = None
    module_ids: list[str] = field(default_factory=list)
    package_ids: list[str] = field(default_factory=list)
    installed_module_ids: list[str] = field(default_factory=list)
    installed_package_ids: list[str] = field(default_factory=list)
    downloaded_library_ids: list[str] = field(default_factory=list)
    downloaded_package_ids: list[str] = field(default_factory=list)
    downloaded_module_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert the tracking info to a dictionary for Mixpanel.

        Returns:
            A dictionary with camelCase keys matching Mixpanel's expected format.
        """
     self.model_dump(mode="json", by_alias=True)

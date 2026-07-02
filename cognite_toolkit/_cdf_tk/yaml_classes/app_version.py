from typing import Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import AppVersionId

from .base import ToolkitResource


class AppVersionYAML(ToolkitResource):
    """A version of a custom app deployed via the CDF App Hosting API."""

    app_external_id: str = Field(
        description="External ID of the parent app.",
        max_length=255,
    )
    version: str = Field(description="Version sent to App Hosting on upload.", max_length=64)
    lifecycle_state: Literal["DRAFT", "PUBLISHED", "DEPRECATED", "ARCHIVED"] = Field(
        default="PUBLISHED",
        description="Lifecycle state of the version. Transitions are forward-only: DRAFT → PUBLISHED → DEPRECATED → ARCHIVED.",
    )
    alias: Literal["ACTIVE", "PREVIEW"] | None = Field(
        default=None,
        description=(
            "Alias assigned to the version. ACTIVE is unique per app (set automatically clears the previous holder). "
            "PREVIEW allows multiple. Only PUBLISHED versions can hold an alias."
        ),
    )
    entrypoint: str = Field(
        default="index.html",
        description="Path to the entry HTML inside the version zip.",
    )
    source_path: str | None = Field(
        default=None,
        description=(
            "Path to the app source, relative to this YAML file. "
            "Can point at the build output directory directly, or at the app source root whose "
            "dist/ subdirectory contains the build output (dist/ is preferred when it contains "
            "the entrypoint). The entrypoint file must exist at the resolved location. "
            "Defaults to a sibling directory named after appExternalId."
        ),
    )

    def as_id(self) -> AppVersionId:
        return AppVersionId(app_external_id=self.app_external_id, version=self.version)

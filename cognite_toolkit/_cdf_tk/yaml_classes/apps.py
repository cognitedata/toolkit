from typing import Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import AppVersionId

from .base import ToolkitResource


class AppsYAML(ToolkitResource):
    """Dune app deployed via the CDF App Hosting API."""

    external_id: str = Field(
        description="Stable app identifier; must match the sibling directory name containing app sources.",
        max_length=255,
    )
    version: str = Field(description="Version sent to App Hosting on upload.", max_length=64)
    name: str = Field(description="Display name for the app.", max_length=140)
    description: str | None = Field(default=None, description="App description.", max_length=500)
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
            "Path to the app source root, relative to this YAML file. "
            "If the directory contains a dist/ subdirectory, that is used instead. "
            "Defaults to a sibling directory named after externalId."
        ),
    )

    def as_id(self) -> AppVersionId:
        return AppVersionId(external_id=self.external_id, version=self.version)

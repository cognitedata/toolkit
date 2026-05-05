from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import ToolkitResource


class AppsYAML(ToolkitResource):
    """Dune app deployed via the CDF App Hosting API."""

    app_external_id: str = Field(
        description="Stable app identifier; must match the sibling directory name containing app sources.",
        max_length=255,
    )
    version_tag: str = Field(description="Version tag sent to App Hosting on upload.", max_length=255)
    name: str = Field(description="Display name for the app.", max_length=140)
    description: str | None = Field(default=None, description="App description.", max_length=500)
    published: bool = Field(
        default=False,
        description="If True, the uploaded version is transitioned to PUBLISHED with alias ACTIVE.",
    )
    entry_path: str = Field(
        default="index.html",
        description="Relative path to the entry HTML inside the zip.",
    )
    source_path: str | None = Field(
        default=None,
        description=(
            "Path to the app source root, relative to this YAML file. "
            "If the directory contains a dist/ subdirectory, that is used instead. "
            "Defaults to a sibling directory named after appExternalId."
        ),
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.app_external_id)

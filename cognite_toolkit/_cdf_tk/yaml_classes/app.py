from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import ToolkitResource


class AppYAML(ToolkitResource):
    """Custom app deployed via the CDF App Hosting API."""

    external_id: str = Field(
        description="Stable app identifier.",
        max_length=255,
    )
    name: str = Field(description="Display name for the app.", max_length=140)
    description: str | None = Field(default=None, description="App description.", max_length=500)

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

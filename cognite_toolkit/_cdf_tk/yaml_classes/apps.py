from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import ToolkitResource


class AppsYAML(ToolkitResource):
    """Dune app: zipped folder uploaded as a classic file under ``/dune-apps/``."""

    app_external_id: str = Field(
        description="Logical app id; must match the sibling directory name containing app sources.",
        max_length=255,
    )
    version: str = Field(description="Version tag; combined with app id for the file external id.", max_length=255)
    name: str = Field(description="Display name stored in file metadata.", max_length=140)
    description: str | None = Field(
        default=None,
        description="Description stored in file metadata.",
        max_length=500,
    )
    published: bool = Field(default=True, description="Published flag stored in file metadata.")
    data_set_external_id: str | None = Field(
        default=None,
        description="Dataset for the uploaded zip file.",
        max_length=255,
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=f"{self.app_external_id}-{self.version}")

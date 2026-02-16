from pydantic import Field

from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource


class RobotLocationYAML(ToolkitResource):
    name: str = Field(description="Location name.")
    external_id: str = Field(description="Location external id. Must be unique for the resource type.", max_length=256)
    description: str | None = Field(
        default=None,
        description="Description of Location. Textual description of the Location.",
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

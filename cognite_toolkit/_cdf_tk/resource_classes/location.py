from pydantic import Field

from .base import ToolkitResource


class LocationYAML(ToolkitResource):
    external_id: str = Field(description="The external ID provided by the client.")
    name: str = Field(description="The name of the data set.")
    description: str | None = Field(default=None, description="The description of the data set.")

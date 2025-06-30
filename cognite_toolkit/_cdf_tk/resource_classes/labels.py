from pydantic import Field

from .base import ToolkitResource


class LabelsYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client.",
        max_length=255,
    )
    name: str = Field(
        description="The name of the label.",
        min_length=1,
        max_length=140,
    )
    description: str | None = Field(
        default=None,
        description="The description of the label.",
        min_length=1,
        max_length=500,
    )
    data_set_external_id: str | None = Field(
        default=None,
        description="The id of the dataset this label belongs to.",
    )

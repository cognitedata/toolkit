from pydantic import Field

from .base import ToolkitResource


class DataSetYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client.",
        max_length=255,
    )
    name: str | None = Field(
        default=None,
        description="The name of the data set.",
        min_length=1,
        max_length=50,
    )
    description: str | None = Field(
        default=None,
        description="The description of the data set.",
        min_length=1,
        max_length=500,
    )
    metadata: dict[str, str] | None = Field(
        default=None,
        description="Custom, application-specific metadata.",
    )
    write_protected: bool | None = Field(
        default=False,
        description="To write data to a write-protected data set.",
    )

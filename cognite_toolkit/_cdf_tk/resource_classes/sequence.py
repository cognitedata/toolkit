from typing import Literal

from pydantic import Field

from .base import BaseModelResource, ToolkitResource


class SequenceColumnDTO(BaseModelResource):
    external_id: str = Field(
        description="The external ID of the column.",
        max_length=255,
    )
    name: str | None = Field(
        default=None,
        description="The name of the column.",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="The description of the column.",
        max_length=1000,
    )
    metadata: dict[str, str] | None = Field(
        default=None,
        description="Custom, application-specific metadata.",
        max_length=256,
    )
    value_type: Literal["STRING", "string", "DOUBLE", "double", "LONG", "long"] | None = Field(
        default=None,
        description="What type the datapoints in a column will have.",
    )


class SequenceYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client.",
        max_length=255,
    )
    name: str | None = Field(
        default=None,
        description="The name of the sequence.",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="The description of the sequence.",
        max_length=1000,
    )
    data_set_external_id: str | None = Field(
        default=None,
        description="The external ID of the data set that the sequence associated with.",
    )
    asset_external_id: str | None = Field(
        default=None,
        description="The external ID of the asset that the sequence associated with.",
    )
    metadata: dict[str, str] | None = Field(
        default=None,
        description="Custom, application-specific metadata.",
        max_length=256,
    )
    columns: list[SequenceColumnDTO] = Field(
        description="List of column definitions.",
        min_length=1,
        max_length=400,
    )

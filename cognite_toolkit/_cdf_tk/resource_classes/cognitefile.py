from datetime import datetime

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.constants import (
    INSTANCE_EXTERNAL_ID_PATTERN,
    SPACE_FORMAT_PATTERN,
)

from .base import ToolkitResource
from .view_field_definitions import DirectRelationReference, ViewReference


class CogniteFileYAML(ToolkitResource):
    space: str = Field(description="The space where the file is located.", max_length=43, pattern=SPACE_FORMAT_PATTERN)
    external_id: str = Field(
        description="External-id of the file.", max_length=255, pattern=INSTANCE_EXTERNAL_ID_PATTERN
    )
    name: str | None = Field(default=None, description="name of the file.")
    description: str | None = Field(default=None, description="Description of the file.")
    tags: list[str] | None = Field(default=None, description="Text based labels for generic use.", max_length=1000)
    aliases: list[str] | None = Field(default=None, description="Alternative names for the file.")
    source_id: str | None = Field(default=None, description="Identifier from the source system.")
    source_context: str | None = Field(default=None, description="Context of the source system.")
    source: DirectRelationReference | None = Field(default=None, description="Direct relation to the source system.")
    source_created_time: datetime | None = Field(
        default=None, description="When the file was created in the source system."
    )
    source_updated_time: datetime | None = Field(
        default=None, description="When the file was last updated in the source system."
    )
    source_created_user: str | None = Field(
        default=None, description="User identifier from the source system who created the file."
    )
    source_updated_user: str | None = Field(
        default=None, description="User identifier from the source system who last updated the file."
    )
    assets: list[DirectRelationReference] | None = Field(
        default=None, description="A list of assets this file is related to."
    )
    mime_type: str | None = Field(default=None, description="MIME type of the file.")
    directory: str | None = Field(default=None, description="Directory path for the file.")
    category: DirectRelationReference | None = Field(
        default=None, description="Direct relation to the category of the file."
    )
    existing_version: int | None = Field(default=None, description="Existing version of the file.")
    type: DirectRelationReference | None = Field(default=None, description="Direct relation to the type of the file.")
    node_source: ViewReference | None = Field(default=None, description="The source view for this file.")
    extra_properties: dict[str, JsonValue] | None = Field(
        default=None, description="Additional custom properties for the file."
    )

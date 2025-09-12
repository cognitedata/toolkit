from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.constants import SPACE_FORMAT_PATTERN

from .base import BaseModelResource, ToolkitResource
from .container_field_definitions import ContainerReference
from .view_field_definitions import ViewReference


class NodeType(BaseModelResource):
    space: str = Field(
        description="The space where the instance is located.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="External-id of the instance.",
        min_length=1,
        max_length=256,
        pattern=r"^[^\\x00]{1,256}$",
    )


class InstanceSource(BaseModelResource):
    source: ViewReference | ContainerReference = Field(
        description="Reference to the view or container from where this source is inherited.",
    )
    properties: dict[str, JsonValue] | None = Field(
        default=None,
        description="Properties included in the source.",
    )


class NodeYAML(ToolkitResource):
    space: str = Field(
        description="The space where the node is located.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="External-id of the node.",
        min_length=1,
        max_length=256,
        pattern=r"^[^\\x00]{1,256}$",
    )
    type: NodeType | None = Field(
        default=None,
        description="The type of the node, defined by a direct relation to another node.",
    )
    sources: list[InstanceSource] | None = Field(
        default=None,
        description="List of sources (views) and their properties for this node.",
    )
    existing_version: int | None = Field(
        default=None,
        description="Fail the ingestion request if the node version is greater than this value.",
        ge=0,
    )

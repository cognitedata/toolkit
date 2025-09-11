from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.constants import SPACE_FORMAT_PATTERN

from .base import BaseModelResource, ToolkitResource
from .container_field_definitions import ContainerReference
from .node import NodeType
from .view_field_definitions import ViewReference


class EdgeType(BaseModelResource):
    space: str = Field(
        description="Id of the space that the edge belongs to.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="External-id of the edge.",
        min_length=1,
        max_length=256,
        pattern=r"^[^\\x00]{1,256}$",
    )


class EdgeSource(BaseModelResource):
    source: ViewReference | ContainerReference = Field(
        description="Reference to the view or container from where this source is inherited.",
    )
    properties: dict[str, JsonValue] | None = Field(
        default=None,
        description="Properties included in the source.",
    )


class EdgeYAML(ToolkitResource):
    space: str = Field(
        description="Id of the space that the edge belongs to.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="External-id of the edge.",
        min_length=1,
        max_length=256,
        pattern=r"^[^\\x00]{1,256}$",
    )
    type: EdgeType = Field(
        description="The type of the edge, defined by a direct relation to another edge.",
    )
    sources: list[EdgeSource] | None = Field(
        default=None,
        description="List of sources (views) and their properties for this edge.",
    )
    existing_version: int | None = Field(
        default=None,
        description="Version of the edge.",
        ge=0,
    )
    start_node: NodeType = Field(
        description="Reference to the node at the start of the edge.",
    )
    end_node: NodeType = Field(
        description="Reference to the node at the end of the edge.",
    )

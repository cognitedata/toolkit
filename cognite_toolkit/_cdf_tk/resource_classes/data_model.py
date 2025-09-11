from pydantic import Field

from cognite_toolkit._cdf_tk.constants import (
    DM_EXTERNAL_ID_PATTERN,
    DM_VERSION_PATTERN,
    SPACE_FORMAT_PATTERN,
)

from .base import ToolkitResource
from .view_field_definitions import ViewReference


class DataModelYAML(ToolkitResource):
    """Cognite Data Model resource.

    Data models group and structure views into reusable collections.
    A data model contains a set of views where the node types can
    refer to each other with direct relations and edges.
    """

    space: str = Field(
        description="The workspace for the data model, a unique identifier for the space.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="External-id of the data model.",
        min_length=1,
        max_length=255,
        pattern=DM_EXTERNAL_ID_PATTERN,
    )
    version: str = Field(
        description="Version of the data model.",
        max_length=43,
        pattern=DM_VERSION_PATTERN,
    )
    name: str | None = Field(
        default=None,
        description="Human readable name for the data model.",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="Description of the data model.",
        max_length=1024,
    )
    views: list[ViewReference] | None = Field(
        description="List of views included in this data model.",
        default=None,
    )

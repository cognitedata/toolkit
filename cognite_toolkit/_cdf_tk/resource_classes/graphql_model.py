from pydantic import Field

from cognite_toolkit._cdf_tk.constants import (
    DM_EXTERNAL_ID_PATTERN,
    DM_VERSION_PATTERN,
    SPACE_FORMAT_PATTERN,
)

from .base import ToolkitResource


class GraphQLDataModelYAML(ToolkitResource):
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
    dml: str | None = Field(
        default=None,
        description="The path to the graphql file containing the DML schema. If not provided, the file"
        "is assumed to be named <config_file>.graphql in the same directory as the config file.",
    )
    previous_version: str | None = Field(
        default=None,
        max_length=43,
        pattern=DM_VERSION_PATTERN,
    )
    preserve_dml: bool | None = None

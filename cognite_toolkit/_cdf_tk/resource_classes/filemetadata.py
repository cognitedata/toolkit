from typing import Any, Literal

from pydantic import Field

from .base import ToolkitResource


class NodeId(ToolkitResource):
    external_id: str
    space: str


class FileMetadataYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client.",
        max_length=255,
    )
    name: str = Field(
        description="The name of the file.",
        max_length=256,
    )
    directory: str | None = Field(
        default=None,
        description="Directory associated with the file.",
        max_length=512,
    )
    instance_id: NodeId | None = Field(
        default=None,
        description="The Instance ID for the file when created in DMS.",
    )
    source: str | None = Field(
        default=None,
        description="The source of the file.",
        max_length=128,
    )
    mime_type: str | None = Field(
        default=None,
        description="File type. E.g., text/plain, application/pdf, etc.",
        max_length=256,
    )
    metadata: dict[str, str] | None = Field(
        default=None, description="Custom, application-specific metadata.", max_length=256
    )
    asset_external_ids: list[str] | None = Field(default=None, description="Asset IDs.", min_length=1, max_length=1000)
    data_set_external_id: str | None = Field(default=None, description="The dataSet Id for the item.")
    labels: list[dict[Literal["externalId"], str]] | None = Field(
        default=None,
        description="A list of labels associated with this resource.",
        max_length=10,
    )
    geo_location: dict[str, Any] | None = Field(
        default=None,
        description="The geographic metadata of the file.",
    )
    source_created_time: int | None = Field(
        default=None, description="The timestamp for when the file was originally created in the source system.", ge=0
    )
    source_modified_time: int | None = Field(
        default=None, description="The timestamp for when the file was last modified in the source system.", ge=0
    )
    security_categories: list[str] | None = Field(
        default=None, description="The security category IDs required to access this file.", max_length=100
    )

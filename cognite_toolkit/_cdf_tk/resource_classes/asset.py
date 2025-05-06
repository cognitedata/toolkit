from typing import Any, Literal

from pydantic import Field

from .base import ToolkitResource


class AssetYAML(ToolkitResource):
    external_id: str = Field(description="External ID provided by the client.", max_length=255)
    name: str | None = Field(None, description="The name of the asset.", min_length=1, max_length=140)
    parent_external_id: str | None = Field(
        None, description="The external ID of the parent of the node.", max_length=255
    )
    description: str | None = Field(None, description="The description of the asset.", max_length=500)
    metadata: dict[str, str] | None = Field(None, description="Custom, application-specific metadata.", max_length=256)
    data_set_external_id: str | None = Field(None, description="The external id of the dataset this asset belongs to.")
    source: str | None = Field(None, description="The source of the asset.", max_length=128)
    labels: list[str | dict[Literal["externalId"], str]] | None = Field(
        None, description="A list of labels associated with this resource.", max_length=10
    )
    geo_location: dict[str, Any] | None = Field(None, description="The geographic metadata of the asset")

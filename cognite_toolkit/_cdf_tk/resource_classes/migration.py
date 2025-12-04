from typing import Literal

from pydantic import Field

from .base import ToolkitResource
from .view_field_definitions import ViewReference


class ResourceViewMappingYAML(ToolkitResource):
    resource_type: Literal["asset", "event", "file", "timeSeries", "sequence", "assetAnnotation", "fileAnnotation"] = (
        Field(
            description="The type of the resource to map to the view.",
        )
    )
    view_id: ViewReference = Field(
        description="The view to map the resource to.",
    )
    property_mapping: dict[str, str] = Field(
        description="A dictionary mapping from resource property to view property.",
    )

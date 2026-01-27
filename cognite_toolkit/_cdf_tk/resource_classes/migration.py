from typing import Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.constants import INSTANCE_EXTERNAL_ID_PATTERN

from .base import ToolkitResource
from .view_field_definitions import ViewReference


class ResourceViewMappingYAML(ToolkitResource):
    external_id: str = Field(
        description="External-id of the mapping.",
        min_length=1,
        max_length=256,
        pattern=INSTANCE_EXTERNAL_ID_PATTERN,
    )
    resource_type: Literal["asset", "event", "file", "timeseries", "sequence", "assetAnnotation", "fileAnnotation"] = (
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

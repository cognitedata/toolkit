import re
from typing import Any, Literal

from pydantic import Field, field_validator

from cognite_toolkit._cdf_tk.utils import humanize_collection

from .base import ToolkitResource
from .view_field_definitions import ViewReference

JSON_PATH_PATTERN = re.compile(r"^\$(\.[a-zA-Z_][a-zA-Z0-9_]*|\[\d+\]|\[\'[^\']*\'\]|\[\"[^\"]*\"\])*$")


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

    @field_validator("property_mapping")
    @classmethod
    def validate_json_paths(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        not_matching_keys = [k for k in value.keys() if not re.match(JSON_PATH_PATTERN, k)]
        if not_matching_keys:
            raise ValueError(f"Invalid JSON paths: {humanize_collection(not_matching_keys)}")
        return value

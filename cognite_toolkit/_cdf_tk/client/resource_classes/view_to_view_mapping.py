from typing import Any

from pydantic import Field, field_serializer

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

from .data_modeling import ViewReference


class ViewToViewMapping(BaseModelObject):
    source_view: ViewReference
    destination_view: ViewReference
    map_identical_id_properties: bool = Field(
        default=False,
        description="Whether to automatically map properties with identical ID. Note this is a shorthand, "
        "you can achieve the same by including the properties in the property_mapping with identical "
        " and destination IDs.",
    )
    property_mapping: dict[str, str]

    @field_serializer("source_view", "destination_view", mode="plain")
    def serialize_view_id(self, view_id: ViewReference) -> dict[str, Any]:
        return {**view_id.dump(), "type": "view"}

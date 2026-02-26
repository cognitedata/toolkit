from pydantic import Field, field_serializer

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

from .data_modeling import ViewReference


class ViewToViewMapping(BaseModelObject):
    source_view: ViewReference
    destination_view: ViewReference
    map_equal_named_properties: bool = Field(
        default=False,
        description="Whether to automatically map properties with the same name. Note this is a shorthand for"
        " mapping all properties with the same name to each other.",
    )
    property_mapping: dict[str, str]

    @field_serializer("source_view", "destination_view", mode="plain")
    def serialize_view_id(self, view_id: ViewReference) -> dict:
        return {**view_id.dump(), "type": "view"}

from pydantic import field_serializer

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

from .data_modeling import ViewReference


class ViewToViewMapping(BaseModelObject):
    source_view: ViewReference
    destination_view: ViewReference
    property_mapping: dict[str, str]

    @field_serializer("source_view", "destination_view", mode="plain")
    def serialize_view_id(self, view_id: ViewReference) -> dict:
        return {**view_id.dump(), "type": "view"}

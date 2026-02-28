from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import ViewReference


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

    def get_destination_property(self, source_property: str) -> str | None:
        dest_prop_id = self.property_mapping.get(source_property)
        if not dest_prop_id and self.map_identical_id_properties:
            return source_property
        return dest_prop_id

from pydantic import Field, field_serializer

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import EdgeTypeId, ViewId


class ViewToViewMapping(BaseModelObject):
    external_id: str
    source_view: ViewId
    destination_view: ViewId
    map_identical_id_properties: bool = Field(
        default=False,
        description="Whether to automatically map properties with identical ID. Note this is a shorthand, "
        "you can achieve the same by including the properties in the property_mapping with identical "
        " and destination IDs.",
    )
    container_mapping: dict[str, str] = Field(
        description="Mapping from property Ids in the source view to property Ids in the destination view."
    )
    edge_mapping: dict[EdgeTypeId, str] | None = Field(
        None, description="Mapping from edge types to destination property Ids. "
    )

    def get_destination_property(self, source_property: str) -> str | None:
        dest_prop_id = self.container_mapping.get(source_property)
        if not dest_prop_id and self.map_identical_id_properties:
            return source_property
        return dest_prop_id

    @field_serializer("edge_mapping", mode="plain")
    def serialize_edge_mapping(self, edge_mapping: dict[EdgeTypeId, str] | None) -> dict[str, str] | None:
        if isinstance(edge_mapping, dict):
            return {str(k): v for k, v in edge_mapping.items()}
        return edge_mapping

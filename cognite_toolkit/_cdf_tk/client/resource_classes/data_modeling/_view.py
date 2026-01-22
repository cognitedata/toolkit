from abc import ABC
from typing import Any, Literal

from pydantic import Field, JsonValue, field_serializer, model_validator
from pydantic_core.core_schema import FieldSerializationInfo

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource

from ._data_types import DirectNodeRelation
from ._references import ContainerReference, ViewReference
from ._view_property import (
    MultiReverseDirectRelationPropertyResponse,
    SingleReverseDirectRelationPropertyResponse,
    ViewCorePropertyRequest,
    ViewCorePropertyResponse,
    ViewRequestProperty,
    ViewResponseProperty,
)


class View(BaseModelObject, ABC):
    space: str
    external_id: str
    version: str
    name: str | None = None
    description: str | None = None
    filter: JsonValue | None
    implements: list[ViewReference] | None = None

    def as_id(self) -> ViewReference:
        return ViewReference(space=self.space, external_id=self.external_id, version=self.version)

    @model_validator(mode="before")
    def set_connection_type_on_primary_properties(cls, data: dict[str, Any]) -> dict[str, Any]:
        if "properties" not in data:
            return data
        properties = data["properties"]
        if not isinstance(properties, dict):
            return data
        # We assume all properties without connectionType are core properties.
        # The reason we set connectionType is to make it easy for pydantic to discriminate the union.
        # This also leads to better error messages, as if there is a union and pydantic does not know which
        # type to pick it will give errors from all type in the union.
        new_properties: dict[str, Any] = {}
        for prop_id, prop in properties.items():
            if isinstance(prop, dict) and "connectionType" not in prop:
                prop_copy = prop.copy()
                prop_copy["connectionType"] = "primary_property"
                new_properties[prop_id] = prop_copy
            else:
                new_properties[prop_id] = prop
        if new_properties:
            new_data = data.copy()
            new_data["properties"] = new_properties
            return new_data

        return data

    @field_serializer("implements", mode="plain")
    @classmethod
    def serialize_implements(
        cls, implements: list[ViewReference] | None, info: FieldSerializationInfo
    ) -> list[dict[str, Any]] | None:
        if implements is None:
            return None
        output: list[dict[str, Any]] = []
        for view in implements:
            dumped = view.model_dump(**vars(info))
            dumped["type"] = "view"
            output.append(dumped)
        return output


class ViewRequest(View, RequestResource):
    properties: dict[str, ViewRequestProperty] = Field(
        description="View with included properties and expected edges, indexed by a unique space-local identifier."
    )

    @property
    def used_containers(self) -> set[ContainerReference]:
        """Get all containers referenced by this view."""
        return {prop.container for prop in self.properties.values() if isinstance(prop, ViewCorePropertyRequest)}


class ViewResponse(View, ResponseResource[ViewRequest]):
    properties: dict[str, ViewResponseProperty]

    created_time: int
    last_updated_time: int
    writable: bool
    queryable: bool
    used_for: Literal["node", "edge", "all"]
    is_global: bool
    mapped_containers: list[ContainerReference]

    def as_request_resource(self) -> ViewRequest:
        dumped = self.model_dump(by_alias=True, exclude={"properties"})
        properties: dict[str, Any] = {}
        for key, value in self.properties.items():
            if isinstance(value, ViewCorePropertyResponse) and isinstance(value.type, DirectNodeRelation):
                # Special case. In the request the source of DirectNodeRelation is set on the Property object,
                # while in the response it is set on the DirectNodeRelation object.
                request_object = value.as_request().model_dump(by_alias=True)
                request_object["source"] = value.type.source.model_dump(by_alias=True) if value.type.source else None
                properties[key] = request_object
            elif isinstance(
                value,
                ViewCorePropertyResponse
                | SingleReverseDirectRelationPropertyResponse
                | MultiReverseDirectRelationPropertyResponse,
            ):
                properties[key] = value.as_request().model_dump(by_alias=True)
            else:
                properties[key] = value.model_dump(by_alias=True)

        dumped["properties"] = properties
        return ViewRequest.model_validate(dumped, extra="ignore")

    @field_serializer("mapped_containers", mode="plain")
    @classmethod
    def serialize_mapped_containers(
        cls, mapped_containers: list[ContainerReference], info: FieldSerializationInfo
    ) -> list[dict[str, Any]]:
        return [container.model_dump(**vars(info)) | {"type": "container"} for container in mapped_containers]

    @field_serializer("properties", mode="plain")
    @classmethod
    def serialize_properties_special_handling_direct_relation_with_source(
        cls, properties: dict[str, ViewResponseProperty], info: FieldSerializationInfo
    ) -> dict[str, dict[str, Any]]:
        output: dict[str, dict[str, Any]] = {}
        for prop_id, prop in properties.items():
            output[prop_id] = prop.model_dump(**vars(info))
            if (
                isinstance(prop, ViewCorePropertyResponse)
                and isinstance(prop.type, DirectNodeRelation)
                and prop.type.source
            ):
                # We manually include the source as this is excluded by default. The reason why this is excluded
                # is that the DirectNodeRelation is used for both request and response, and in the request the source
                # does not exist on the DirectNodeRelation, but on the Property object.
                output[prop_id]["type"]["source"] = prop.type.source.model_dump(**vars(info)) | {"type": "view"}
        return output

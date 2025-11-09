from typing import Any, ClassVar, Literal, TypeAlias

from pydantic import ConfigDict, JsonValue, model_serializer

from .base import BaseModelObject, Identifier, RequestResource

InstanceType: TypeAlias = Literal["node", "edge"]


class InstanceIdentifier(Identifier):
    """Identifier for an Instance instance."""

    instance_type: InstanceType
    space: str
    external_id: str


class NodeIdentifier(InstanceIdentifier):
    instance_type: Literal["node"] = "node"


class EdgeIdentifier(InstanceIdentifier):
    instance_type: Literal["edge"] = "edge"


class InstanceResult(BaseModelObject):
    instance_type: InstanceType
    version: int
    was_modified: bool
    space: str
    external_id: str
    created_time: int
    last_updated_time: int

    def as_id(self) -> InstanceIdentifier:
        return InstanceIdentifier(
            instance_type=self.instance_type,
            space=self.space,
            external_id=self.external_id,
        )


class ViewReference(Identifier):
    type: Literal["view"] = "view"
    space: str
    external_id: str
    version: str


######################################################
# The classes below are helper classes for making instances request/responses.
# By using these, we can avoid having to include the instances specific classes in the DTO classes
# that are instance. Instead, these classes can now only have the properties they need to define.
#######################################################


class InstanceRequestResource(RequestResource):
    """This is a base class for resources that are Instances."""

    VIEW_ID: ClassVar[ViewReference]
    instance_type: InstanceType
    space: str
    external_id: str

    def as_id(self) -> InstanceIdentifier:
        return InstanceIdentifier(
            instance_type=self.instance_type,
            space=self.space,
            external_id=self.external_id,
        )

    def as_request_item(self) -> "InstanceRequestItem":
        return InstanceRequestItem(
            instance_type=self.instance_type,
            space=self.space,
            external_id=self.external_id,
            sources=[InstanceSource(source=self.VIEW_ID, resource=self)],
        )


class InstanceSource(BaseModelObject):
    source: ViewReference
    resource: InstanceRequestResource

    @model_serializer(mode="plain")
    def serialize_resource(self) -> dict[str, Any]:
        properties: dict[str, JsonValue] = {}
        for field_id, field in type(self.resource).model_fields.items():
            if field_id in InstanceRequestResource.model_fields:
                # Skip space, external_id, instance_type
                continue
            key = field.alias or field_id
            properties[key] = self._serialize_property(getattr(self.resource, field_id))

        return {
            "source": self.source.model_dump(by_alias=True),
            "properties": properties,
        }

    @classmethod
    def _serialize_property(cls, value: Any) -> JsonValue:
        """Handles serialization of direct relations."""
        if isinstance(value, InstanceRequestResource):
            return {"space": value.space, "externalId": value.external_id}
        elif isinstance(value, list):
            return [cls._serialize_property(v) for v in value]
        return value


class InstanceRequestItem(BaseModelObject):
    model_config = ConfigDict(populate_by_name=True)
    instance_type: InstanceType
    space: str
    external_id: str
    existing_version: int | None = None
    sources: list[InstanceSource] | None = None

    def as_id(self) -> InstanceIdentifier:
        return InstanceIdentifier(
            instance_type=self.instance_type,
            space=self.space,
            external_id=self.external_id,
        )


class InstanceResponseItem(BaseModelObject):
    instance_type: InstanceType
    space: str
    external_id: str
    version: int
    type: InstanceIdentifier | None = None
    created_time: int
    last_updated_time: int
    deleted_time: int | None = None
    properties: dict[str, dict[str, dict[str, JsonValue]]] | None = None

    def get_properties_for_source(
        self, source: ViewReference, include_identifier: bool = False
    ) -> dict[str, JsonValue]:
        output: dict[str, JsonValue] = (
            {"space": self.space, "externalId": self.external_id} if include_identifier else {}
        )
        if not self.properties:
            return output
        if source.space not in self.properties:
            return output
        space_properties = self.properties[source.space]
        view_version = f"{source.external_id}/{source.version}"
        output.update(space_properties.get(view_version, {}))
        return output

    def as_id(self) -> InstanceIdentifier:
        return InstanceIdentifier(
            instance_type=self.instance_type,
            space=self.space,
            external_id=self.external_id,
        )

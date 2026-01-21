from abc import ABC
from typing import Annotated, Any, Literal

from pydantic import Field, JsonValue, TypeAdapter, field_serializer
from pydantic_core.core_schema import FieldSerializationInfo

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

from ._data_types import DataType
from ._references import ContainerDirectReference, ContainerReference, NodeReference, ViewDirectReference, ViewReference


class ViewPropertyDefinition(BaseModelObject, ABC):
    connection_type: str


class ViewCoreProperty(ViewPropertyDefinition, ABC):
    # Core properties do not have connection type in the API, but we add it here such that
    # we can use it as a discriminator in unions. The exclude=True ensures that it is not
    # sent to the API.
    connection_type: Literal["primary_property"] = Field(default="primary_property", exclude=True)
    name: str | None = None
    description: str | None = None
    container: ContainerReference
    container_property_identifier: str

    @field_serializer("container", mode="plain")
    @classmethod
    def serialize_container(cls, container: ContainerReference, info: FieldSerializationInfo) -> dict[str, Any]:
        return {**container.model_dump(**vars(info)), "type": "container"}


class ViewCorePropertyRequest(ViewCoreProperty):
    source: ViewReference | None = None

    @field_serializer("source", mode="plain")
    @classmethod
    def serialize_source(cls, source: ViewReference | None, info: FieldSerializationInfo) -> dict[str, Any] | None:
        if source is None:
            return None
        return {**source.model_dump(**vars(info)), "type": "view"}


class ConstraintOrIndexState(BaseModelObject):
    nullability: Literal["current", "pending", "failed"] | None = None
    max_list_size: Literal["current", "pending", "failed"] | None = None
    max_text_size: Literal["current", "pending", "failed"] | None = None


class ViewCorePropertyResponse(ViewCoreProperty):
    immutable: bool | None = None
    nullable: bool | None = None
    auto_increment: bool | None = None
    default_value: str | int | bool | dict[str, JsonValue] | None = None
    constraint_state: ConstraintOrIndexState
    type: DataType

    def as_request(self) -> ViewCorePropertyRequest:
        return ViewCorePropertyRequest.model_validate(self.model_dump(by_alias=True))


class ConnectionPropertyDefinition(ViewPropertyDefinition, ABC):
    name: str | None = None
    description: str | None = None


class EdgeProperty(ConnectionPropertyDefinition, ABC):
    source: ViewReference
    type: NodeReference
    edge_source: ViewReference | None = None
    direction: Literal["outwards", "inwards"] = "outwards"

    @field_serializer("source", "edge_source", mode="plain")
    @classmethod
    def serialize_source(cls, source: ViewReference | None, info: FieldSerializationInfo) -> dict[str, Any] | None:
        if source is None:
            return None
        return {**source.model_dump(**vars(info)), "type": "view"}


class SingleEdgeProperty(EdgeProperty):
    connection_type: Literal["single_edge_connection"] = "single_edge_connection"


class MultiEdgeProperty(EdgeProperty):
    connection_type: Literal["multi_edge_connection"] = "multi_edge_connection"


class ReverseDirectRelationProperty(ConnectionPropertyDefinition, ABC):
    source: ViewReference
    through: ContainerDirectReference | ViewDirectReference

    @field_serializer("source", mode="plain")
    @classmethod
    def serialize_source(cls, source: ViewReference, info: FieldSerializationInfo) -> dict[str, Any]:
        return {**source.model_dump(**vars(info)), "type": "view"}

    @field_serializer("through", mode="plain")
    @classmethod
    def serialize_through(
        cls, through: ContainerDirectReference | ViewDirectReference, info: FieldSerializationInfo
    ) -> dict[str, Any]:
        output = through.model_dump(**vars(info))
        if isinstance(through, ContainerDirectReference):
            output["source"]["type"] = "container"
        else:
            output["source"]["type"] = "view"
        return output


class SingleReverseDirectRelationPropertyRequest(ReverseDirectRelationProperty):
    connection_type: Literal["single_reverse_direct_relation"] = "single_reverse_direct_relation"


class MultiReverseDirectRelationPropertyRequest(ReverseDirectRelationProperty):
    connection_type: Literal["multi_reverse_direct_relation"] = "multi_reverse_direct_relation"


class SingleReverseDirectRelationPropertyResponse(ReverseDirectRelationProperty):
    connection_type: Literal["single_reverse_direct_relation"] = "single_reverse_direct_relation"
    targets_list: bool

    def as_request(self) -> SingleReverseDirectRelationPropertyRequest:
        return SingleReverseDirectRelationPropertyRequest.model_validate(self.model_dump(by_alias=True))


class MultiReverseDirectRelationPropertyResponse(ReverseDirectRelationProperty):
    connection_type: Literal["multi_reverse_direct_relation"] = "multi_reverse_direct_relation"
    targets_list: bool

    def as_request(self) -> MultiReverseDirectRelationPropertyRequest:
        return MultiReverseDirectRelationPropertyRequest.model_validate(self.model_dump(by_alias=True))


ViewRequestProperty = Annotated[
    SingleEdgeProperty
    | MultiEdgeProperty
    | SingleReverseDirectRelationPropertyRequest
    | MultiReverseDirectRelationPropertyRequest
    | ViewCorePropertyRequest,
    Field(discriminator="connection_type"),
]
ViewResponseProperty = Annotated[
    SingleEdgeProperty
    | MultiEdgeProperty
    | SingleReverseDirectRelationPropertyResponse
    | MultiReverseDirectRelationPropertyResponse
    | ViewCorePropertyResponse,
    Field(discriminator="connection_type"),
]

ViewRequestPropertyAdapter: TypeAdapter[ViewRequestProperty] = TypeAdapter(ViewRequestProperty)

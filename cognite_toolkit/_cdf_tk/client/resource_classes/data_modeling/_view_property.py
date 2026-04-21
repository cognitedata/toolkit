from abc import ABC
from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, ConfigDict, Field, JsonValue, TypeAdapter, field_serializer
from pydantic_core.core_schema import FieldSerializationInfo

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import (
    ContainerDirectId,
    ContainerId,
    EdgeTypeId,
    NodeUntypedId,
    ViewDirectId,
    ViewId,
)
from cognite_toolkit._cdf_tk.utils._auxiliary import registry_from_model_classes

from ._data_types import DataType


class ViewPropertyDefinition(BaseModelObject, ABC):
    connection_type: str


class ViewCoreProperty(ViewPropertyDefinition, ABC):
    # Core properties do not have connection type in the API, but we add it here such that
    # we can use it as a discriminator in unions. The exclude=True ensures that it is not
    # sent to the API.
    connection_type: Literal["primary_property"] = Field(default="primary_property", exclude=True)
    name: str | None = None
    description: str | None = None
    container: ContainerId
    container_property_identifier: str

    @field_serializer("container", mode="plain")
    @classmethod
    def serialize_container(cls, container: ContainerId, info: FieldSerializationInfo) -> dict[str, Any]:
        return {**container.model_dump(**vars(info)), "type": "container"}


class ViewCorePropertyRequest(ViewCoreProperty):
    source: ViewId | None = None

    @field_serializer("source", mode="plain")
    @classmethod
    def serialize_source(cls, source: ViewId | None, info: FieldSerializationInfo) -> dict[str, Any] | None:
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
        return ViewCorePropertyRequest.model_validate(self.model_dump(by_alias=True), extra="ignore")


class ConnectionPropertyDefinition(ViewPropertyDefinition, ABC):
    name: str | None = None
    description: str | None = None


class EdgeProperty(ConnectionPropertyDefinition, ABC):
    source: ViewId
    type: NodeUntypedId
    edge_source: ViewId | None = None
    direction: Literal["outwards", "inwards"] = "outwards"

    def as_edge_type_id(self) -> EdgeTypeId:
        return EdgeTypeId(type=self.type, direction=self.direction)


class SingleEdgeProperty(EdgeProperty):
    connection_type: Literal["single_edge_connection"] = "single_edge_connection"


class MultiEdgeProperty(EdgeProperty):
    connection_type: Literal["multi_edge_connection"] = "multi_edge_connection"


class ReverseDirectRelationProperty(ConnectionPropertyDefinition, ABC):
    source: ViewId
    through: ContainerDirectId | ViewDirectId

    @field_serializer("source", mode="plain")
    @classmethod
    def serialize_source(cls, source: ViewId, info: FieldSerializationInfo) -> dict[str, Any]:
        return {**source.model_dump(**vars(info)), "type": "view"}

    @field_serializer("through", mode="plain")
    @classmethod
    def serialize_through(
        cls, through: ContainerDirectId | ViewDirectId, info: FieldSerializationInfo
    ) -> dict[str, Any]:
        output = through.model_dump(**vars(info))
        if isinstance(through, ContainerDirectId):
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


class UnknownViewPropertyRequest(ViewPropertyDefinition):
    model_config = ConfigDict(extra="allow")
    connection_type: str


class UnknownViewPropertyResponse(ViewPropertyDefinition):
    connection_type: str


def _handle_view_request_property(value: Any) -> Any:
    if isinstance(value, dict):
        connection_type = value.get("connectionType") or "primary_property"
        if connection_type not in _VIEW_REQUEST_PROPERTY_BY_CT:
            return UnknownViewPropertyRequest.model_validate(value)
        return _VIEW_REQUEST_PROPERTY_BY_CT[connection_type].model_validate(value)
    return value


def _handle_view_response_property(value: Any) -> Any:
    if isinstance(value, dict):
        connection_type = value.get("connectionType") or "primary_property"
        if connection_type not in _VIEW_RESPONSE_PROPERTY_BY_CT:
            return UnknownViewPropertyResponse.model_validate(value)
        return _VIEW_RESPONSE_PROPERTY_BY_CT[connection_type].model_validate(value)
    return value


_VIEW_REQUEST_PROPERTY_BY_CT = registry_from_model_classes(
    (
        SingleEdgeProperty,
        MultiEdgeProperty,
        SingleReverseDirectRelationPropertyRequest,
        MultiReverseDirectRelationPropertyRequest,
        ViewCorePropertyRequest,
    ),
    type_field="connection_type",
)
_VIEW_RESPONSE_PROPERTY_BY_CT = registry_from_model_classes(
    (
        SingleEdgeProperty,
        MultiEdgeProperty,
        SingleReverseDirectRelationPropertyResponse,
        MultiReverseDirectRelationPropertyResponse,
        ViewCorePropertyResponse,
    ),
    type_field="connection_type",
)


ViewRequestProperty = Annotated[
    SingleEdgeProperty
    | MultiEdgeProperty
    | SingleReverseDirectRelationPropertyRequest
    | MultiReverseDirectRelationPropertyRequest
    | ViewCorePropertyRequest
    | UnknownViewPropertyRequest,
    BeforeValidator(_handle_view_request_property),
]
ViewResponseProperty = Annotated[
    SingleEdgeProperty
    | MultiEdgeProperty
    | SingleReverseDirectRelationPropertyResponse
    | MultiReverseDirectRelationPropertyResponse
    | ViewCorePropertyResponse
    | UnknownViewPropertyResponse,
    BeforeValidator(_handle_view_response_property),
]

ViewRequestPropertyAdapter: TypeAdapter[ViewRequestProperty] = TypeAdapter(ViewRequestProperty)

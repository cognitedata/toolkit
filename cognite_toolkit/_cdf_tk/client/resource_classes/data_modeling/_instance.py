import builtins
from abc import ABC
from typing import Annotated, Any, Generic, Literal, TypeAlias

from pydantic import BeforeValidator, Field, JsonValue, TypeAdapter, field_serializer, field_validator

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
    T_RequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import (
    ContainerId,
    EdgeId,
    InstanceId,
    NodeId,
    NodeUntypedId,
    ViewId,
)
from cognite_toolkit._cdf_tk.utils._auxiliary import registry_from_subclasses_with_type_field


class InstanceDefinition(BaseModelObject, ABC):
    """Base class for node and edge instances."""

    instance_type: str  # "node" | "edge"
    space: str
    external_id: str


class InstanceSource(BaseModelObject):
    source: ViewId | ContainerId
    properties: dict[str, JsonValue | NodeUntypedId | list[NodeUntypedId]] | None = None


class InstanceRequestDefinition(InstanceDefinition, RequestResource, ABC):
    existing_version: int | None = None
    sources: list[InstanceSource] | None = None


class InstanceResponseDefinition(InstanceDefinition, ResponseResource, Generic[T_RequestResource], ABC):
    version: int
    created_time: int
    last_updated_time: int
    deleted_time: int | None = None
    properties: dict[ViewId | ContainerId, dict[str, JsonValue | NodeUntypedId | list[NodeUntypedId]]] | None = None

    @field_validator("properties", mode="before")
    def parse_reference(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        parsed: dict[ViewId | ContainerId, dict[str, Any]] = {}
        for space, inner_dict in value.items():
            if isinstance(space, ViewId | ContainerId):
                parsed[space] = inner_dict
                continue
            if not isinstance(inner_dict, dict) or not isinstance(space, str):
                raise ValueError(
                    f"Invalid properties format expected dict[str, dict[...]], got: dict[{type(space).__name__}, {type(inner_dict).__name__}]"
                )
            for view_or_container_identifier, prop in inner_dict.items():
                if not isinstance(view_or_container_identifier, str):
                    raise ValueError(
                        "Invalid properties format expected dict[str, dict[str, ...]]], "
                        f"got: dict[{type(space).__name__}, "
                        f"dict[{type(view_or_container_identifier).__name__}, ...]]"
                    )
                source_ref: ViewId | ContainerId
                if "/" in view_or_container_identifier:
                    external_id, version = view_or_container_identifier.split("/", 1)
                    source_ref = ViewId(space=space, external_id=external_id, version=version)
                else:
                    source_ref = ContainerId(space=space, external_id=view_or_container_identifier)
                parsed[source_ref] = prop
        return parsed

    @field_serializer("properties", mode="plain")
    def serialize_properties(self, value: dict[ViewId | ContainerId, dict[str, Any]] | None) -> Any:
        if value is None:
            return None
        serialized: dict[str, dict[str, Any]] = {}
        for source_ref, props in value.items():
            space = source_ref.space
            if space not in serialized:
                serialized[space] = {}
            if isinstance(source_ref, ViewId):
                identifier = f"{source_ref.external_id}/{source_ref.version}"
            else:
                identifier = source_ref.external_id
            serialized[space][identifier] = props
        return serialized


class NodeRequest(InstanceRequestDefinition):
    """A node request resource."""

    instance_type: Literal["node"] = "node"
    type: NodeUntypedId | None = None

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)


class EdgeRequest(InstanceRequestDefinition):
    """An edge request resource."""

    instance_type: Literal["edge"] = "edge"
    type: NodeUntypedId
    start_node: NodeUntypedId
    end_node: NodeUntypedId

    def as_id(self) -> EdgeId:
        return EdgeId(space=self.space, external_id=self.external_id)


class NodeResponse(InstanceResponseDefinition[NodeRequest]):
    """A node response from the API."""

    instance_type: Literal["node"] = "node"
    type: NodeUntypedId | None = None

    def as_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)

    @classmethod
    def request_cls(cls) -> builtins.type[NodeRequest]:
        return NodeRequest

    def as_request_resource(self) -> NodeRequest:
        dumped = self.dump()
        if self.properties:
            dumped["sources"] = [
                InstanceSource(source=source_ref, properties=props) for source_ref, props in self.properties.items()
            ]
        dumped["existingVersion"] = dumped.pop("version", None)
        return NodeRequest.model_validate(dumped, extra="ignore")


class EdgeResponse(InstanceResponseDefinition[EdgeRequest]):
    """An edge response from the API."""

    instance_type: Literal["edge"] = "edge"
    type: NodeUntypedId
    start_node: NodeUntypedId
    end_node: NodeUntypedId

    def as_id(self) -> EdgeId:
        return EdgeId(space=self.space, external_id=self.external_id)

    @classmethod
    def request_cls(cls) -> builtins.type[EdgeRequest]:
        return EdgeRequest

    def as_request_resource(self) -> EdgeRequest:
        dumped = self.dump()
        if self.properties:
            dumped["sources"] = [
                InstanceSource(source=source_ref, properties=props) for source_ref, props in self.properties.items()
            ]
        dumped["existingVersion"] = dumped.pop("version", None)
        return EdgeRequest.model_validate(dumped, extra="ignore")


class InstanceSlimDefinition(BaseModelObject):
    """Slim version of instance definition for listing instances."""

    instance_type: Literal["node", "edge"]
    version: int
    was_modified: bool
    space: str
    external_id: str
    created_time: int
    last_updated_time: int

    def as_id(self) -> NodeId | EdgeId:
        if self.instance_type == "node":
            return NodeId(space=self.space, external_id=self.external_id)
        else:
            return EdgeId(space=self.space, external_id=self.external_id)

    def as_instance_id(self) -> InstanceId:
        node_id = self.as_id()
        if not isinstance(node_id, NodeId):
            raise ValueError(f"Cannot create instance ID from an {self.instance_type}.")
        return InstanceId(instance_id=node_id)


class UnknownInstanceRequest(InstanceRequestDefinition):
    instance_type: str

    def as_id(self) -> NodeId:
        """Address unknown kinds by node external id until a typed instance model exists."""
        return NodeId(space=self.space, external_id=self.external_id)


class UnknownInstanceResponse(InstanceResponseDefinition[UnknownInstanceRequest]):
    instance_type: str

    def as_id(self) -> NodeId:
        """Address unknown kinds by node external id until a typed instance model exists."""
        return NodeId(space=self.space, external_id=self.external_id)

    @classmethod
    def request_cls(cls) -> builtins.type[UnknownInstanceRequest]:
        return UnknownInstanceRequest

    def as_request_resource(self) -> UnknownInstanceRequest:
        dumped = self.dump()
        if self.properties:
            dumped["sources"] = [
                InstanceSource(source=source_ref, properties=props) for source_ref, props in self.properties.items()
            ]
        dumped["existingVersion"] = dumped.pop("version", None)
        return UnknownInstanceRequest.model_validate(dumped, extra="ignore")


def _handle_unknown_instance_request(value: Any) -> Any:
    if isinstance(value, dict):
        instance_type = value.get("instanceType")
        if instance_type not in _INSTANCE_REQUEST_BY_TYPE:
            return UnknownInstanceRequest.model_validate(value)
        return _INSTANCE_REQUEST_BY_TYPE[instance_type].model_validate(value)
    return value


def _handle_unknown_instance_response(value: Any) -> Any:
    if isinstance(value, dict):
        instance_type = value.get("instanceType")
        if instance_type not in _INSTANCE_RESPONSE_BY_TYPE:
            return UnknownInstanceResponse.model_validate(value)
        return _INSTANCE_RESPONSE_BY_TYPE[instance_type].model_validate(value)
    return value


_INSTANCE_REQUEST_BY_TYPE = registry_from_subclasses_with_type_field(
    InstanceRequestDefinition,
    type_field="instance_type",
    exclude=(UnknownInstanceRequest,),
)
_INSTANCE_RESPONSE_BY_TYPE = registry_from_subclasses_with_type_field(
    InstanceResponseDefinition,
    type_field="instance_type",
    exclude=(UnknownInstanceResponse,),
)


InstanceRequest: TypeAlias = Annotated[
    NodeRequest | EdgeRequest | UnknownInstanceRequest,
    BeforeValidator(_handle_unknown_instance_request),
]
InstanceResponse: TypeAlias = Annotated[
    NodeResponse | EdgeResponse | UnknownInstanceResponse,
    BeforeValidator(_handle_unknown_instance_response),
]

# We are not using discriminator in general for request/response classes,
# however, this is an exception for the cases that we want exactly a Node or Edge.
NodeOrEdgeRequest: TypeAlias = Annotated[NodeRequest | EdgeRequest, Field(discriminator="instance_type")]
NodeOrEdgeResponse: TypeAlias = Annotated[NodeResponse | EdgeResponse, Field(discriminator="instance_type")]

NodeOrEdgeRequestAdapter: TypeAdapter[NodeOrEdgeRequest] = TypeAdapter(NodeOrEdgeRequest)

InstanceRequestAdapter: TypeAdapter[InstanceRequest] = TypeAdapter(InstanceRequest)

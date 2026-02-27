import builtins
from abc import ABC
from typing import Annotated, Any, Generic, Literal, TypeAlias

from pydantic import Field, JsonValue, TypeAdapter, field_serializer, field_validator

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
    T_RequestResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import (
    ContainerReference,
    EdgeReference,
    NodeReference,
    NodeReferenceUntyped,
    ViewReference,
)


class InstanceDefinition(BaseModelObject, ABC):
    """Base class for node and edge instances."""

    instance_type: str  # "node" | "edge"
    space: str
    external_id: str


class InstanceSource(BaseModelObject):
    source: ViewReference | ContainerReference
    properties: dict[str, JsonValue] | None = None

    @field_serializer("source", mode="plain")
    def serialize_source(self, value: ViewReference | ContainerReference) -> Any:
        return {**value.dump(), "type": value.type}


class InstanceRequestDefinition(InstanceDefinition, RequestResource, ABC):
    existing_version: int | None = None
    sources: list[InstanceSource] | None = None


class InstanceResponseDefinition(InstanceDefinition, ResponseResource, Generic[T_RequestResource], ABC):
    version: int
    created_time: int
    last_updated_time: int
    deleted_time: int | None = None
    properties: dict[ViewReference | ContainerReference, dict[str, JsonValue]] | None = None

    @field_validator("properties", mode="before")
    def parse_reference(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        parsed: dict[ViewReference | ContainerReference, dict[str, Any]] = {}
        for space, inner_dict in value.items():
            if isinstance(space, ViewReference | ContainerReference):
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
                source_ref: ViewReference | ContainerReference
                if "/" in view_or_container_identifier:
                    external_id, version = view_or_container_identifier.split("/", 1)
                    source_ref = ViewReference(space=space, external_id=external_id, version=version)
                else:
                    source_ref = ContainerReference(space=space, external_id=view_or_container_identifier)
                parsed[source_ref] = prop
        return parsed

    @field_serializer("properties", mode="plain")
    def serialize_properties(self, value: dict[ViewReference | ContainerReference, dict[str, Any]] | None) -> Any:
        if value is None:
            return None
        serialized: dict[str, dict[str, Any]] = {}
        for source_ref, props in value.items():
            space = source_ref.space
            if space not in serialized:
                serialized[space] = {}
            if isinstance(source_ref, ViewReference):
                identifier = f"{source_ref.external_id}/{source_ref.version}"
            else:
                identifier = source_ref.external_id
            serialized[space][identifier] = props
        return serialized


class NodeRequest(InstanceRequestDefinition):
    """A node request resource."""

    instance_type: Literal["node"] = "node"
    type: NodeReferenceUntyped | None = None

    def as_id(self) -> NodeReference:
        return NodeReference(space=self.space, external_id=self.external_id)


class EdgeRequest(InstanceRequestDefinition):
    """An edge request resource."""

    instance_type: Literal["edge"] = "edge"
    type: NodeReferenceUntyped
    start_node: NodeReferenceUntyped
    end_node: NodeReferenceUntyped

    def as_id(self) -> EdgeReference:
        return EdgeReference(space=self.space, external_id=self.external_id)


class NodeResponse(InstanceResponseDefinition[NodeRequest]):
    """A node response from the API."""

    instance_type: Literal["node"] = "node"
    type: NodeReferenceUntyped | None = None

    def as_id(self) -> NodeReference:
        return NodeReference(space=self.space, external_id=self.external_id)

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
    type: NodeReferenceUntyped
    start_node: NodeReferenceUntyped
    end_node: NodeReferenceUntyped

    def as_id(self) -> EdgeReference:
        return EdgeReference(space=self.space, external_id=self.external_id)

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

    def as_id(self) -> NodeReference | EdgeReference:
        if self.instance_type == "node":
            return NodeReference(space=self.space, external_id=self.external_id)
        else:
            return EdgeReference(space=self.space, external_id=self.external_id)


InstanceRequest: TypeAlias = Annotated[
    NodeRequest | EdgeRequest,
    Field(discriminator="instance_type"),
]
InstanceResponse: TypeAlias = Annotated[
    NodeResponse | EdgeResponse,
    Field(discriminator="instance_type"),
]

InstanceRequestAdapter: TypeAdapter[InstanceRequest] = TypeAdapter(InstanceRequest)

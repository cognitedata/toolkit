from abc import ABC
from typing import Generic, Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
    T_RequestResource,
)

from ._references import ContainerReference, NodeReference, ViewReference


class InstanceDefinition(BaseModelObject, ABC):
    """Base class for node and edge instances."""

    instance_type: str  # "node" | "edge"
    space: str
    external_id: str


class InstanceSource(BaseModelObject):
    source: ViewReference | ContainerReference
    properties: dict[str, JsonValue] | None = None


class InstanceRequestDefinition(InstanceDefinition, RequestResource, ABC):
    existing_version: int | None = None
    sources: list[InstanceSource]


class InstanceResponseDefinition(
    InstanceDefinition, Generic[T_RequestResource], ResponseResource[T_RequestResource], ABC
):
    version: int
    created_time: int
    last_updated_time: int
    deleted_time: int | None = None
    properties: dict[ViewReference | ContainerReference, dict[str, JsonValue]] | None = None


class NodeRequest(InstanceRequestDefinition):
    """A node request resource."""

    instance_type: Literal["node"] = "node"
    type: NodeReference | None = None

    def as_id(self) -> NodeReference:
        return NodeReference(space=self.space, external_id=self.external_id)


class EdgeRequest(InstanceRequestDefinition):
    """An edge request resource."""

    instance_type: Literal["edge"] = "edge"
    type: NodeReference
    start_node: NodeReference
    end_node: NodeReference

    def as_id(self) -> NodeReference:
        return NodeReference(space=self.space, external_id=self.external_id)


class NodeResponse(InstanceResponseDefinition[NodeRequest]):
    """A node response from the API."""

    instance_type: Literal["node"] = "node"
    type: NodeReference | None = None

    def as_id(self) -> NodeReference:
        return NodeReference(space=self.space, external_id=self.external_id)

    def as_request_resource(self) -> NodeRequest:
        dumped = self.dump()
        if properties := dumped.pop("properties", None):
            dumped["sources"] = [
                InstanceSource(source=source_ref, properties=props) for source_ref, props in properties.items()
            ]
        dumped["existingVersion"] = dumped.pop("version", None)
        return NodeRequest.model_validate(dumped, extra="ignore")


class EdgeResponse(InstanceResponseDefinition[EdgeRequest]):
    """An edge response from the API."""

    instance_type: Literal["edge"] = "edge"
    type: NodeReference
    start_node: NodeReference
    end_node: NodeReference

    def as_id(self) -> NodeReference:
        return NodeReference(space=self.space, external_id=self.external_id)

    def as_request_resource(self) -> EdgeRequest:
        dumped = self.dump()
        if properties := dumped.pop("properties", None):
            dumped["sources"] = [
                InstanceSource(source=source_ref, properties=props) for source_ref, props in properties.items()
            ]
        dumped["existingVersion"] = dumped.pop("version", None)
        return EdgeRequest.model_validate(dumped, extra="ignore")

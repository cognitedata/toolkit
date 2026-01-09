from abc import ABC
from typing import Literal

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject, RequestResource, ResponseResource

from ._references import NodeReference, ViewReference


class InstanceSource(BaseModelObject):
    """A source for an instance, referencing a view and the properties to set."""

    source: ViewReference
    properties: dict[str, JsonValue] | None = None


class Instance(BaseModelObject, ABC):
    """Base class for node and edge instances."""

    space: str
    external_id: str


class NodeWrite(Instance):
    """A node write request."""

    instance_type: Literal["node"] = "node"
    existing_version: int | None = None
    type: NodeReference | None = None
    sources: list[InstanceSource] | None = None

    def as_id(self) -> NodeReference:
        return NodeReference(space=self.space, external_id=self.external_id)


class EdgeWrite(Instance):
    """An edge write request."""

    instance_type: Literal["edge"] = "edge"
    existing_version: int | None = None
    type: NodeReference | None = None
    start_node: NodeReference
    end_node: NodeReference
    sources: list[InstanceSource] | None = None

    def as_id(self) -> NodeReference:
        return NodeReference(space=self.space, external_id=self.external_id)


class NodeRequest(NodeWrite, RequestResource):
    """A node request resource."""

    ...


class EdgeRequest(EdgeWrite, RequestResource):
    """An edge request resource."""

    ...


class InstanceDeleteItem(BaseModelObject):
    """An item to delete from the instance store."""

    instance_type: Literal["node", "edge"]
    space: str
    external_id: str
    existing_version: int | None = None


class InstanceApplyRequest(BaseModelObject):
    """Request body for applying (upserting) nodes and edges."""

    items: list[NodeRequest | EdgeRequest] = Field(max_length=1000)
    delete: list[InstanceDeleteItem] | None = Field(default=None, max_length=1000)
    auto_create_direct_relations: bool = True
    auto_create_start_nodes: bool = False
    auto_create_end_nodes: bool = False
    skip_on_version_conflict: bool = False
    replace: bool = False


class NodeResponse(Instance, ResponseResource["NodeRequest"]):
    """A node response from the API."""

    instance_type: Literal["node"] = "node"
    version: int
    was_modified: bool
    created_time: int
    last_updated_time: int

    def as_id(self) -> NodeReference:
        return NodeReference(space=self.space, external_id=self.external_id)

    def as_request_resource(self) -> NodeRequest:
        return NodeRequest.model_validate({"space": self.space, "externalId": self.external_id, "instanceType": "node"})


class EdgeResponse(Instance, ResponseResource["EdgeRequest"]):
    """An edge response from the API."""

    instance_type: Literal["edge"] = "edge"
    version: int
    was_modified: bool
    created_time: int
    last_updated_time: int

    def as_id(self) -> NodeReference:
        return NodeReference(space=self.space, external_id=self.external_id)

    def as_request_resource(self) -> EdgeRequest:
        # Note: Edge responses don't include start_node/end_node, so we can't fully reconstruct
        raise NotImplementedError(
            "Cannot convert EdgeResponse to EdgeRequest as start_node and end_node are not available in the response"
        )


class InstanceDeletedItem(BaseModelObject):
    """An item that was deleted from the instance store."""

    instance_type: Literal["node", "edge"]
    space: str
    external_id: str


class InstanceApplyResponse(BaseModelObject):
    """Response body from applying (upserting) nodes and edges."""

    items: list[NodeResponse | EdgeResponse]
    deleted: list[InstanceDeletedItem] | None = None

    def nodes(self) -> list[NodeResponse]:
        """Get all node responses."""
        return [item for item in self.items if isinstance(item, NodeResponse)]

    def edges(self) -> list[EdgeResponse]:
        """Get all edge responses."""
        return [item for item in self.items if isinstance(item, EdgeResponse)]

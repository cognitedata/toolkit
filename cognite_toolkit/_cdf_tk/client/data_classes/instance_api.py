from typing import Any, Generic, Literal

from pydantic import ConfigDict, model_serializer

from .base import BaseModelObject, Identifier, T_RequestResource


class InstanceIdentifier(Identifier):
    """Identifier for an Instance instance."""

    instance_type: str
    space: str
    external_id: str


class NodeIdentifier(InstanceIdentifier):
    """Identifier for a NodeId instance."""

    instance_type: Literal["node"] = "node"


class EdgeIdentifier(InstanceIdentifier):
    """Identifier for an EdgeId instance."""

    instance_type: Literal["edge"] = "edge"


class InstanceResult(BaseModelObject):
    instance_type: str
    version: int
    was_modified: bool
    space: str
    external_id: str
    created_time: int
    last_updated_time: int


class NodeResult(InstanceResult):
    instance_type: Literal["node"] = "node"

    def as_id(self) -> NodeIdentifier:
        return NodeIdentifier(
            space=self.space,
            external_id=self.external_id,
        )


class EdgeResult(InstanceResult):
    instance_type: Literal["edge"] = "edge"

    def as_id(self) -> EdgeIdentifier:
        return EdgeIdentifier(
            space=self.space,
            external_id=self.external_id,
        )


class ViewReference(Identifier):
    type: Literal["view"] = "view"
    space: str
    external_id: str
    version: str


class InstanceSource(BaseModelObject, Generic[T_RequestResource]):
    source: ViewReference
    resource: T_RequestResource

    @model_serializer(mode="plain")
    def serialize_resource(self) -> dict[str, Any]:
        return {
            "source": self.source.model_dump(by_alias=True),
            "properties": self.resource.model_dump(exclude={"space", "external_id"}, by_alias=True),
        }


class InstanceRequestItem(BaseModelObject, Generic[T_RequestResource]):
    model_config = ConfigDict(populate_by_name=True)
    instance_type: str
    space: str
    external_id: str
    existing_version: int | None = None
    sources: list[InstanceSource[T_RequestResource]] | None = None


class NodeRequestItem(InstanceRequestItem[T_RequestResource]):
    instance_type: Literal["node"] = "node"

    def as_id(self) -> NodeIdentifier:
        return NodeIdentifier(
            space=self.space,
            external_id=self.external_id,
        )


class EdgeRequestItem(InstanceRequestItem[T_RequestResource]):
    instance_type: Literal["edge"] = "edge"

    def as_id(self) -> EdgeIdentifier:
        return EdgeIdentifier(
            space=self.space,
            external_id=self.external_id,
        )

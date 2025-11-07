from collections.abc import Iterable
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteResourceList, WriteableCogniteResourceList
from cognite.client.data_classes.data_modeling import EdgeId, NodeId
from cognite.client.data_classes.data_modeling.instances import (
    EdgeApplyResult,
    Instance,
    InstanceApply,
    InstanceApplyResult,
    NodeApplyResult,
)


class InstanceApplyResultAdapter(InstanceApplyResult):
    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> InstanceApplyResult:  # type: ignore[override]
        """Load an instance of the class from a resource dictionary."""
        if "instanceType" not in resource:
            raise ValueError("Resource must contain 'instanceType' key.")
        instance_type = resource.get("instanceType")
        if instance_type == "node":
            return NodeApplyResult._load(resource, cognite_client)
        elif instance_type == "edge":
            return EdgeApplyResult._load(resource, cognite_client)
        raise TypeError(f"Resource must be a NodeApplyResult or EdgeApplyResult, not {instance_type}.")


class InstancesApplyResultList(CogniteResourceList[InstanceApplyResult]):
    def __init__(
        self, items: Iterable[InstanceApplyResult] | None = None, cognite_client: CogniteClient | None = None
    ) -> None:
        # Trick to avoid validation in the CogniteResourceList constructor
        super().__init__([], cognite_client=cognite_client)
        self.data.extend(items or [])
        self._build_id_mappings()

    _RESOURCE = InstanceApplyResultAdapter

    def as_ids(self) -> list[NodeId | EdgeId]:
        """Return a list of IDs for the instances in the list."""
        return [
            NodeId(item.space, item.external_id)
            if item.instance_type == "node"
            else EdgeId(item.space, item.external_id)
            for item in self
        ]


class InstanceApplyList(CogniteResourceList[InstanceApply]):
    """A list of instances to be applied (created or updated)."""

    _RESOURCE = InstanceApply


class InstanceList(WriteableCogniteResourceList[InstanceApply, Instance]):
    """A list of instances that can be written to CDF."""

    _RESOURCE = Instance

    def as_write(self) -> InstanceApplyList:
        """Converts the instance list to a list of writeable instances."""
        return InstanceApplyList([item.as_write() for item in self])

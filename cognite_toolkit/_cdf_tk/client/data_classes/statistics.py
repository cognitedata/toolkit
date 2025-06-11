import sys
from dataclasses import dataclass
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import CogniteObject, CogniteResource, CogniteResourceList

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@dataclass
class InstanceCounts(CogniteResource):
    nodes: int
    edges: int
    soft_deleted_nodes: int
    soft_deleted_edges: int

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            nodes=resource["nodes"],
            edges=resource["edges"],
            soft_deleted_nodes=resource["softDeletedNodes"],
            soft_deleted_edges=resource["softDeletedEdges"],
        )


@dataclass
class SpaceInstanceCounts(InstanceCounts):
    space: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            space=resource["space"],
            nodes=resource["nodes"],
            edges=resource["edges"],
            soft_deleted_nodes=resource["softDeletedNodes"],
            soft_deleted_edges=resource["softDeletedEdges"],
        )


class SpaceInstanceCountsList(CogniteResourceList):
    _RESOURCE = SpaceInstanceCounts


@dataclass
class CountLimitPair(CogniteObject):
    count: int
    limit: int

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            count=resource["count"],
            limit=resource["limit"],
        )


@dataclass
class InstanceCountsLimits(InstanceCounts):
    instances_limit: int
    soft_deleted_instances_limit: int

    @property
    def instances(self) -> int:
        return self.nodes + self.edges

    @property
    def soft_deleted_instances(self) -> int:
        return self.soft_deleted_nodes + self.soft_deleted_edges

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            nodes=resource["nodes"],
            edges=resource["edges"],
            soft_deleted_nodes=resource["softDeletedNodes"],
            soft_deleted_edges=resource["softDeletedEdges"],
            instances_limit=resource["instancesLimit"],
            soft_deleted_instances_limit=resource["softDeletedInstancesLimit"],
        )


@dataclass
class ProjectStatsAndLimits(CogniteResource):
    project: str
    spaces: CountLimitPair
    containers: CountLimitPair
    views: CountLimitPair
    data_models: CountLimitPair
    container_properties: CountLimitPair
    instances: InstanceCountsLimits
    concurrent_read_limit: int
    concurrent_write_limit: int
    concurrent_delete_limit: int

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            project=data["project"],
            spaces=CountLimitPair._load(data["spaces"]),
            containers=CountLimitPair._load(data["containers"]),
            views=CountLimitPair._load(data["views"]),
            data_models=CountLimitPair._load(data["dataModels"]),
            container_properties=CountLimitPair._load(data["containerProperties"]),
            instances=InstanceCountsLimits._load(data["instances"]),
            concurrent_read_limit=data["concurrentReadLimit"],
            concurrent_write_limit=data["concurrentWriteLimit"],
            concurrent_delete_limit=data["concurrentDeleteLimit"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "project": self.project,
            "spaces": self.spaces.dump(camel_case=camel_case),
            "containers": self.containers.dump(camel_case=camel_case),
            "views": self.views.dump(camel_case=camel_case),
            "dataModels" if camel_case else "data_models": self.data_models.dump(camel_case=camel_case),
            "containerProperties" if camel_case else "container_properties": self.container_properties.dump(
                camel_case=camel_case
            ),
            "instances": self.instances.dump(camel_case=camel_case),
            "concurrentReadLimit" if camel_case else "concurrent_read_limit": self.concurrent_read_limit,
            "concurrentWriteLimit" if camel_case else "concurrent_write_limit": self.concurrent_write_limit,
            "concurrentDeleteLimit" if camel_case else "concurrent_delete_limit": self.concurrent_delete_limit,
        }

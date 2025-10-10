from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from typing import Literal

from cognite.client.data_classes.data_modeling import EdgeId, NodeId
from cognite.client.utils._identifier import InstanceId

from cognite_toolkit._cdf_tk.resource_classes.views import ViewReference
from cognite_toolkit._cdf_tk.storageio._data_classes import InstanceIdCSVList

from ._base import DataSelector


class InstanceSelector(DataSelector, ABC):
    @abstractmethod
    def get_schema_spaces(self) -> list[str] | None:
        raise NotImplementedError()

    @abstractmethod
    def get_instance_spaces(self) -> list[str] | None:
        raise NotImplementedError()


class InstanceViewSelector(InstanceSelector):
    type: Literal["instanceView"] = "instanceView"
    view: ViewReference
    instance_type: Literal["node", "edge"] = "node"
    instance_spaces: tuple[str, ...] | None = None

    def get_schema_spaces(self) -> list[str] | None:
        return [self.view.space]

    def get_instance_spaces(self) -> list[str] | None:
        return list(self.instance_spaces) if self.instance_spaces else None

    @property
    def group(self) -> str:
        return self.view.space

    def __str__(self) -> str:
        return f"{self.view.external_id}_{self.view.version}_{self.instance_type}"


class InstanceFileSelector(InstanceSelector):
    type: Literal["instanceFile"] = "instanceFile"

    datafile: Path
    validate_instance: bool = True

    @property
    def group(self) -> str:
        return "Instances"

    def __str__(self) -> str:
        return f"file_{self.datafile.as_posix()}"

    @cached_property
    def items(self) -> InstanceIdCSVList:
        return InstanceIdCSVList.read_csv_file(self.datafile)

    @cached_property
    def _ids_by_type(self) -> tuple[list[NodeId], list[EdgeId]]:
        node_ids: list[NodeId] = []
        edge_ids: list[EdgeId] = []
        for instance in self.items:
            if instance.instance_type == "node":
                node_ids.append(NodeId(instance.space, instance.external_id))
            else:
                edge_ids.append(EdgeId(instance.space, instance.external_id))
        return node_ids, edge_ids

    @property
    def instance_ids(self) -> list[InstanceId]:
        node_ids, edge_ids = self._ids_by_type
        return [*node_ids, *edge_ids]

    @property
    def node_ids(self) -> list[NodeId]:
        return self._ids_by_type[0]

    @property
    def edge_ids(self) -> list[EdgeId]:
        return self._ids_by_type[1]

    def get_schema_spaces(self) -> list[str] | None:
        return None

    def get_instance_spaces(self) -> list[str] | None:
        return sorted({instance.space for instance in self.items})

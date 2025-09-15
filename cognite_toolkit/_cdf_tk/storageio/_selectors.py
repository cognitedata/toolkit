from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Generic, Literal

from cognite.client.data_classes.data_modeling import EdgeId, NodeId, ViewId
from cognite.client.utils._identifier import InstanceId

from cognite_toolkit._cdf_tk.storageio._data_classes import InstanceIdCSVList, T_ModelList
from cognite_toolkit._cdf_tk.utils.file import to_directory_compatible


@dataclass(frozen=True)
class CSVFileSelector(Generic[T_ModelList], ABC):
    """Data class for file-based data selection from CSV files."""

    datafile: Path

    @abstractmethod
    def list_cls(self) -> type[T_ModelList]:
        raise NotImplementedError()

    @cached_property
    def items(self) -> T_ModelList:
        return self.list_cls().read_csv_file(self.datafile)

    def __str__(self) -> str:
        return f"{type(self).__name__}={self.datafile.name}"


@dataclass(frozen=True)
class AssetCentricSelector:
    """Data class for asset-centric data selection."""


@dataclass(frozen=True)
class DataSetSelector(AssetCentricSelector):
    """Select data associated with a specific data set."""

    data_set_external_id: str

    def __str__(self) -> str:
        return f"DataSet={to_directory_compatible(self.data_set_external_id)}"


@dataclass(frozen=True)
class AssetSubtreeSelector(AssetCentricSelector):
    """Select data associated with an asset and its subtree."""

    hierarchy: str

    def __str__(self) -> str:
        return f"AssetSubtree={to_directory_compatible(self.hierarchy)}"


@dataclass(frozen=True)
class AssetCentricFileSelector(AssetCentricSelector):
    """Select data from a specific file."""

    datafile: Path

    def __str__(self) -> str:
        return f"File={self.datafile.name}"


@dataclass(frozen=True)
class InstanceSelector:
    @abstractmethod
    def get_schema_spaces(self) -> list[str] | None:
        raise NotImplementedError()

    @abstractmethod
    def get_instance_spaces(self) -> list[str] | None:
        raise NotImplementedError()


@dataclass(frozen=True)
class InstanceFileSelector(CSVFileSelector[InstanceIdCSVList], InstanceSelector):
    datafile: Path
    validate: bool = True

    def list_cls(self) -> type[InstanceIdCSVList]:
        return InstanceIdCSVList

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

    def __str__(self) -> str:
        return f"file {self.datafile.as_posix()}"


@dataclass(frozen=True)
class InstanceViewSelector(InstanceSelector):
    view: ViewId
    instance_type: Literal["node", "edge"] = "node"
    instance_spaces: tuple[str, ...] | None = None

    def get_schema_spaces(self) -> list[str] | None:
        return [self.view.space]

    def get_instance_spaces(self) -> list[str] | None:
        return list(self.instance_spaces) if self.instance_spaces else None

    def __str__(self) -> str:
        return f"view {self.view!r}"


@dataclass(frozen=True)
class ChartSelector: ...


@dataclass(frozen=True)
class ChartOwnerSelector(ChartSelector):
    owner_id: str


@dataclass(frozen=True)
class AllChartSelector(ChartSelector): ...


@dataclass(frozen=True)
class ChartFileSelector(ChartSelector):
    filepath: Path

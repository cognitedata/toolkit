from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Generic, Literal

from cognite.client.data_classes.data_modeling import ViewId

from cognite_toolkit._cdf_tk.storageio._data_classes import InstanceCSVList, T_ModelList
from cognite_toolkit._cdf_tk.utils.file import to_directory_compatible


@dataclass(frozen=True)
class FileSelector(Generic[T_ModelList], ABC):
    """Data class for file-based data selection."""

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
class InstanceSelector: ...


@dataclass(frozen=True)
class InstanceFileSelector(FileSelector[InstanceCSVList], InstanceSelector):
    def list_cls(self) -> type[InstanceCSVList]:
        return InstanceCSVList


@dataclass(frozen=True)
class InstanceViewSelector(InstanceSelector):
    view: ViewId
    instance_type: Literal["node", "edge"] = "node"
    instance_spaces: tuple[str, ...] | None = None


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

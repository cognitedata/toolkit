from dataclasses import dataclass
from pathlib import Path

from cognite_toolkit._cdf_tk.utils.file import to_directory_compatible


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
class ChartSelector: ...


@dataclass(frozen=True)
class ChartOwnerSelector(ChartSelector):
    owner_id: str


@dataclass(frozen=True)
class AllChartSelector(ChartSelector): ...


@dataclass(frozen=True)
class ChartFileSelector(ChartSelector):
    filepath: Path

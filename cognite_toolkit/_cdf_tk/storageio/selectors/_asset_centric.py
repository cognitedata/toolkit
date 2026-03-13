from abc import ABC
from pathlib import Path
from typing import Literal

from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentricKind

from ._base import DataSelector


class AssetCentricSelector(DataSelector, ABC):
    kind: AssetCentricKind


class DataSetSelector(AssetCentricSelector):
    """Select data associated with a specific data set."""

    type: Literal["dataSet"] = "dataSet"

    data_set_external_id: str

    def __str__(self) -> str:
        return self.kind

    @property
    def display_name(self) -> str:
        return f"{self.kind} in dataset {self.data_set_external_id}"


class AssetSubtreeSelector(AssetCentricSelector):
    """Select data associated with an asset and its subtree."""

    type: Literal["assetSubtree"] = "assetSubtree"
    hierarchy: str

    def __str__(self) -> str:
        return self.kind

    @property
    def display_name(self) -> str:
        return f"{self.kind} in asset hierarchy {self.hierarchy}"


class AssetCentricFileSelector(AssetCentricSelector):
    """Select data from a specific file."""

    type: Literal["assetFile"] = "assetFile"
    datafile: Path

    def __str__(self) -> str:
        return f"file_{self.datafile.name}"

    @property
    def display_name(self) -> str:
        return f"{self.kind} from file {self.datafile.name}"

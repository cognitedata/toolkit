from abc import ABC
from pathlib import Path
from typing import Literal

from ._base import DataSelector


class AssetCentricSelector(DataSelector, ABC): ...


class DataSetSelector(AssetCentricSelector):
    """Select data associated with a specific data set."""

    type: Literal["dataSet"] = "dataSet"

    data_set_external_id: str
    resource_type: str

    @property
    def group(self) -> str:
        return f"DataSet_{self.data_set_external_id}"

    def __str__(self) -> str:
        return self.resource_type


class AssetSubtreeSelector(AssetCentricSelector):
    """Select data associated with an asset and its subtree."""

    type: Literal["assetSubtree"] = "assetSubtree"
    hierarchy: str
    resource_type: str

    @property
    def group(self) -> str:
        return f"Hierarchy_{self.hierarchy}"

    def __str__(self) -> str:
        return self.resource_type


class AssetCentricFileSelector(AssetCentricSelector):
    """Select data from a specific file."""

    type: Literal["assetFile"] = "assetFile"
    datafile: Path

    @property
    def group(self) -> str:
        return "AssetCentric"

    def __str__(self) -> str:
        return f"file_{self.datafile.name}"

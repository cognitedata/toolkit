from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, Literal

from cognite.client.data_classes.data_modeling import ViewId

from cognite_toolkit._cdf_tk.storageio._data_classes import InstanceIdList


@dataclass(frozen=True)
class AssetCentricData:
    """Data class for asset-centric data selection.

    This class is used to filter asset-centric data based on a data set external ID and a hierarchy.

    Args:
        data_set_external_id (str | None): The external ID of the data set to filter by. (Used for download)
        hierarchy (str | None): The hierarchy to filter by, typically an asset subtree external ID. (Used for download)
        datafile (Path | None): The path to the data file associated with this selection. (Used for upload)

    """

    data_set_external_id: str | None = None
    hierarchy: str | None = None
    datafile: Path | None = None

    def as_filter(self) -> dict[str, Any]:
        """Convert the AssetCentricData to a filter dictionary."""
        return dict(
            data_set_external_ids=[self.data_set_external_id] if self.data_set_external_id else None,
            asset_subtree_external_ids=[self.hierarchy] if self.hierarchy else None,
        )


@dataclass(frozen=True)
class InstanceSelector(ABC):
    @abstractmethod
    def get_schema_spaces(self) -> list[str] | None:
        raise NotImplementedError()

    @abstractmethod
    def get_instance_spaces(self) -> list[str] | None:
        raise NotImplementedError()


@dataclass(frozen=True)
class InstanceFileSelector(InstanceSelector):
    datafile: Path
    validate: bool = True

    @cached_property
    def instance_ids(self) -> InstanceIdList:
        return InstanceIdList.read_csv_file(self.datafile)

    def get_schema_spaces(self) -> list[str] | None:
        return None

    def get_instance_spaces(self) -> list[str] | None:
        return sorted({instance.space for instance in self.instance_ids})

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

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Generic

from cognite.client.data_classes import Asset, Event, FileMetadata, TimeSeries
from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling import InstanceApply
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitNotImplementedError,
)
from cognite_toolkit._cdf_tk.storageio import AssetCentricSelector, BaseAssetCentricIO, InstanceIO, TableStorageIO
from cognite_toolkit._cdf_tk.storageio._base import T_ID, StorageIOConfig, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from .data_classes import MigrationMapping
from .data_model import INSTANCE_SOURCE_VIEW_ID


@dataclass(frozen=True)
class MigrationSelector(AssetCentricSelector): ...


@dataclass
class AssetCentricMapping(Generic[T_WritableCogniteResource], WriteableCogniteResource[InstanceApply]):
    mapping: MigrationMapping
    resource: T_WritableCogniteResource

    def as_write(self) -> InstanceApply:
        raise NotImplementedError()


class AssetCentricMappingList(
    WriteableCogniteResourceList[AssetCentricMapping[T_WritableCogniteResource], InstanceApply]
):
    _RESOURCE: type = AssetCentricMapping

    def as_write(self) -> InstanceApplyList:  # type: ignore[override]
        return InstanceApplyList([item.as_write() for item in self])


class AssetCentricMigrationIOAdapter(
    Generic[T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList],
    TableStorageIO[int, MigrationSelector, InstanceApplyList, AssetCentricMappingList],
):
    folder_name = "migration"
    kind = "AssetCentricMigration"
    display_name = "Asset-Centric Migration"
    supported_download_formats = frozenset({".parquet", ".csv", ".ndjson"})
    supported_compressions = frozenset({".gz"})
    supported_read_formats = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    chunk_size = 1000

    def __init__(
        self,
        client: ToolkitClient,
        base: BaseAssetCentricIO[
            T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
        ],
        instance: InstanceIO,
    ) -> None:
        super().__init__(client)
        self.base = base
        self.instance = instance

    def get_schema(self, selector: MigrationSelector) -> list[SchemaColumn]:
        raise ToolkitNotImplementedError("get_schema is not implemented for AssetCentricMigrationIOAdapter")

    def as_id(self, item: dict[str, JsonVal] | object) -> int:
        if isinstance(item, InstanceApply):
            sources = item.sources

            mapping = next((source for source in sources if source.source == INSTANCE_SOURCE_VIEW_ID), None)
            if mapping is None:
                raise TypeError(f"Cannot ID from item of type {type(item).__name__!r}")
            id_ = mapping.properties.get("id")
            if not isinstance(id_, int):
                raise TypeError(f"Cannot extract ID from item of type {type(item).__name__!r}")
            return id_
        elif isinstance(item, Event | Asset | TimeSeries | FileMetadata):
            return item.id
        elif isinstance(item, dict) and isinstance(item.get("id"), int):
            # MyPy checked above.
            return item["id"]  # type: ignore[arg-type, return-value]
        raise TypeError(f"Cannot extract ID from item of type {type(item).__name__!r}")

    def download_iterable(
        self, selector: MigrationSelector, limit: int | None = None
    ) -> Iterable[AssetCentricMappingList]:
        raise NotImplementedError()

    def count(self, selector: AssetCentricSelector) -> int | None:
        return self.base.count(selector)

    def upload_items(self, data_chunk: InstanceApplyList, selector: MigrationSelector) -> None:
        raise NotImplementedError()

    def data_to_json_chunk(self, data_chunk: AssetCentricMappingList) -> list[dict[str, JsonVal]]:
        return data_chunk.dump()

    def load_selector(self, datafile: Path) -> MigrationSelector:
        raise ToolkitNotImplementedError("load_selector is not implemented for AssetCentricMigrationIOAdapter")

    def configurations(self, selector: MigrationSelector) -> Iterable[StorageIOConfig]:
        raise ToolkitNotImplementedError("configurations is not implemented for AssetCentricMigrationIOAdapter")

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> InstanceApplyList:
        return self.instance.json_chunk_to_data(data_chunk)

    def ensure_configurations(self, selector: MigrationSelector, console: Console | None = None) -> None:
        raise ToolkitNotImplementedError("ensure_configurations is not implemented for AssetCentricMappingList")

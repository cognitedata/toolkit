from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import cached_property
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
from cognite.client.data_classes.data_modeling import EdgeId, InstanceApply, NodeId
from cognite.client.utils._identifier import InstanceId
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList
from cognite_toolkit._cdf_tk.cruds._base_cruds import T_ID
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.storageio import (
    AssetCentricSelector,
    BaseAssetCentricIO,
    InstanceIO,
    InstanceSelector,
    TableStorageIO,
)
from cognite_toolkit._cdf_tk.storageio._base import StorageIOConfig, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from .data_classes import MigrationMapping, MigrationMappingList


@dataclass(frozen=True)
class MigrationSelector(AssetCentricSelector, InstanceSelector, ABC):
    @abstractmethod
    def get_ingestion_views(self) -> list[str]:
        raise NotImplementedError()


@dataclass(frozen=True)
class MigrationCSVFileSelector(MigrationSelector):
    datafile: Path
    resource_type: str

    def get_schema_spaces(self) -> list[str] | None:
        return None

    def get_instance_spaces(self) -> list[str] | None:
        return sorted({item.instance_id.space for item in self.items})

    def get_ingestion_views(self) -> list[str]:
        views = {item.get_ingestion_view() for item in self.items}
        return sorted(views)

    @cached_property
    def items(self) -> MigrationMappingList:
        return MigrationMappingList.read_csv_file(self.datafile, resource_type=self.resource_type)


@dataclass
class AssetCentricMapping(Generic[T_WritableCogniteResource], WriteableCogniteResource[InstanceApply]):
    mapping: MigrationMapping
    resource: T_WritableCogniteResource

    def as_write(self) -> InstanceApply:
        raise NotImplementedError()

    def dump(self, camel_case: bool = True) -> dict[str, JsonVal]:
        mapping = self.mapping.model_dump(exclude_unset=True, by_alias=camel_case)
        # Ensure that resource type is always included, even if unset.
        mapping["resourceType" if camel_case else "resource_type"] = self.mapping.resource_type
        return {
            "mapping": mapping,
            "resource": self.resource.dump(camel_case=camel_case),
        }


class AssetCentricMappingList(
    WriteableCogniteResourceList[InstanceApply, AssetCentricMapping[T_WritableCogniteResource]]
):
    _RESOURCE: type = AssetCentricMapping

    def as_write(self) -> InstanceApplyList:
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
    UPLOAD_ENDPOINT = InstanceIO.UPLOAD_ENDPOINT

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
        # Used to cache the mapping between instance IDs and asset-centric IDs.
        # This is used in the as_id method to track the same object as it is converted to an instance.
        self._id_by_instance_id: dict[InstanceId, int] = {}

    def get_schema(self, selector: MigrationSelector) -> list[SchemaColumn]:
        raise ToolkitNotImplementedError("get_schema is not implemented for AssetCentricMigrationIOAdapter")

    def as_id(self, item: dict[str, JsonVal] | object) -> int:
        if isinstance(item, AssetCentricMapping):
            instance_id = item.mapping.instance_id
            id_ = item.mapping.id
            if instance_id not in self._id_by_instance_id:
                self._id_by_instance_id[instance_id] = id_
            return id_
        elif isinstance(item, Event | Asset | TimeSeries | FileMetadata):
            if item.id is None:
                raise TypeError(f"Resource of type {type(item).__name__!r} is missing an 'id'.")
            return item.id
        elif isinstance(item, InstanceApply):
            instance_id_ = InstanceId(item.space, item.external_id)
            if instance_id_ not in self._id_by_instance_id:
                raise ValueError(f"Missing mapping for instance {instance_id_!r}")
            return self._id_by_instance_id[instance_id_]
        elif isinstance(item, dict) and isinstance(item.get("id"), int):
            # MyPy checked above.
            return item["id"]  # type: ignore[return-value]
        elif (
            isinstance(item, dict)
            and isinstance(item.get("space"), str)
            and isinstance(item.get("externalId"), str)
            and isinstance(item.get("instanceType"), str)
        ):
            instance: InstanceId
            if item["instanceType"] == "node":
                # MyPy checked above.
                instance = NodeId.load(item)  # type: ignore[arg-type]
            elif item["instanceType"] == "edge":
                instance = EdgeId.load(item)  # type: ignore[arg-type]
            else:
                raise ValueError(f"Unknown instance type {item['instanceType']!r}")
            if instance not in self._id_by_instance_id:
                raise ValueError(f"Missing mapping for instance {instance!r}")
            return self._id_by_instance_id[instance]
        raise TypeError(f"Cannot extract ID from item of type {type(item).__name__!r}")

    def download_iterable(
        self, selector: MigrationSelector, limit: int | None = None
    ) -> Iterator[AssetCentricMappingList]:
        if not isinstance(selector, MigrationCSVFileSelector):
            raise ToolkitNotImplementedError(f"Selector {type(selector)} is not supported for download_iterable")
        items = selector.items
        if limit is not None:
            items = MigrationMappingList(items[:limit])
        chunk: list[AssetCentricMapping[T_WritableCogniteResource]] = []
        for current_batch in chunker_sequence(items, self.chunk_size):
            resources = self.base.retrieve(current_batch.get_ids())
            for mapping, resource in zip(current_batch, resources, strict=True):
                chunk.append(AssetCentricMapping(mapping=mapping, resource=resource))
            if chunk:
                yield AssetCentricMappingList(chunk)
                chunk = []

    def count(self, selector: AssetCentricSelector) -> int | None:
        return self.base.count(selector)

    def upload_items(self, data_chunk: InstanceApplyList, selector: MigrationSelector) -> None:
        self.instance.upload_items(data_chunk, selector)

    def data_to_json_chunk(self, data_chunk: AssetCentricMappingList) -> list[dict[str, JsonVal]]:
        return data_chunk.dump()

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> InstanceApplyList:
        return self.instance.json_chunk_to_data(data_chunk)

    def load_selector(self, datafile: Path) -> MigrationSelector:
        raise ToolkitNotImplementedError("load_selector is not implemented for AssetCentricMigrationIOAdapter")

    def configurations(self, selector: MigrationSelector) -> Iterable[StorageIOConfig]:
        raise ToolkitNotImplementedError("configurations is not implemented for AssetCentricMigrationIOAdapter")

    def ensure_configurations(self, selector: MigrationSelector, console: Console | None = None) -> None:
        raise ToolkitNotImplementedError("ensure_configurations is not implemented for AssetCentricMappingList")

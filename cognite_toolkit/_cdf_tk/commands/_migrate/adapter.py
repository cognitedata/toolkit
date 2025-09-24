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
from cognite.client.data_classes.data_modeling import EdgeApply, EdgeId, InstanceApply, NodeApply, NodeId
from cognite.client.utils._identifier import InstanceId
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList
from cognite_toolkit._cdf_tk.client.data_classes.pending_instances_ids import PendingInstanceId
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
from cognite_toolkit._cdf_tk.utils.thread_safe_dict import ThreadSafeDict
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
        return MigrationMappingList.read_mapping_file(self.datafile, resource_type=self.resource_type)


@dataclass
class AssetCentricMapping(Generic[T_WritableCogniteResource], WriteableCogniteResource[InstanceApply]):
    mapping: MigrationMapping
    resource: T_WritableCogniteResource

    def as_write(self) -> InstanceApply:
        raise NotImplementedError()

    def dump(self, camel_case: bool = True) -> dict[str, JsonVal]:
        return {
            "mapping": self.mapping.model_dump(exclude_unset=True, by_alias=camel_case),
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
    FOLDER_NAME = "migration"
    KIND = "AssetCentricMigration"
    DISPLAY_NAME = "Asset-Centric Migration"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    CHUNK_SIZE = 1000
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
        self._id_by_instance_id = ThreadSafeDict[InstanceId, int]()

    def get_schema(self, selector: MigrationSelector) -> list[SchemaColumn]:
        raise ToolkitNotImplementedError("get_schema is not implemented for AssetCentricMigrationIOAdapter")

    @staticmethod
    def _get_id(item: dict[str, JsonVal]) -> int | None:
        if "id" in item and isinstance(item["id"], int):
            return item["id"]
        return None

    @staticmethod
    def _get_instance_id(item: dict[str, JsonVal]) -> InstanceId | None:
        space, external_id, instance_type = (
            item.get("space"),
            item.get("externalId"),
            item.get("instanceType"),
        )
        if isinstance(space, str) and isinstance(external_id, str) and isinstance(instance_type, str):
            if instance_type == "node":
                return NodeId(space=space, external_id=external_id)
            elif instance_type == "edge":
                return EdgeId(space=space, external_id=external_id)
        return None

    def as_id(self, item: dict[str, JsonVal] | object) -> int:
        # When multiple threads are accessing this class, they will always operate on different ids
        if isinstance(item, AssetCentricMapping):
            instance_id = item.mapping.instance_id
            self._id_by_instance_id.setdefault(instance_id, item.mapping.id)
            return self._id_by_instance_id[instance_id]
        elif isinstance(item, Event | Asset | TimeSeries | FileMetadata | PendingInstanceId):
            if item.id is None:
                raise TypeError(f"Resource of type {type(item).__name__!r} is missing an 'id'.")
            return item.id
        elif isinstance(item, NodeApply | EdgeApply):
            instance_id_ = item.as_id()
            if instance_id_ not in self._id_by_instance_id:
                raise ValueError(f"Missing mapping for instance {instance_id_!r}")
            return self._id_by_instance_id[instance_id_]
        elif isinstance(item, dict) and (id_int := self._get_id(item)):
            return id_int
        elif isinstance(item, dict) and (parsed_instance_id := self._get_instance_id(item)):
            if parsed_instance_id not in self._id_by_instance_id:
                raise ValueError(f"Missing mapping for instance {parsed_instance_id!r}")
            return self._id_by_instance_id[parsed_instance_id]
        raise TypeError(f"Cannot extract ID from item of type {type(item).__name__!r}")

    def stream_data(self, selector: MigrationSelector, limit: int | None = None) -> Iterator[AssetCentricMappingList]:
        if not isinstance(selector, MigrationCSVFileSelector):
            raise ToolkitNotImplementedError(f"Selector {type(selector)} is not supported for stream_data")
        items = selector.items
        if limit is not None:
            items = MigrationMappingList(items[:limit])
        chunk: list[AssetCentricMapping[T_WritableCogniteResource]] = []
        for current_batch in chunker_sequence(items, self.CHUNK_SIZE):
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

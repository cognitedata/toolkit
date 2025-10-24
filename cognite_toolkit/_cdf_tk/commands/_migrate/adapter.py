from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Generic, Literal

from cognite.client.data_classes import (
    Asset,
    Event,
    FileMetadata,
    FileMetadataList,
    FileMetadataWrite,
    FileMetadataWriteList,
    TimeSeries,
)
from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling import EdgeApply, EdgeId, InstanceApply, NodeApply, NodeId
from cognite.client.utils._identifier import InstanceId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList
from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId
from cognite_toolkit._cdf_tk.client.data_classes.pending_instances_ids import PendingInstanceId
from cognite_toolkit._cdf_tk.cruds._base_cruds import T_ID
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.storageio import (
    BaseAssetCentricIO,
    FileMetadataIO,
    InstanceIO,
    UploadableStorageIO,
)
from cognite_toolkit._cdf_tk.storageio._base import T_WritableCogniteResourceList, TmpUploadItem
from cognite_toolkit._cdf_tk.storageio.selectors import (
    DataSelector,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage, ItemsRequest, SuccessResponseItems
from cognite_toolkit._cdf_tk.utils.thread_safe_dict import ThreadSafeDict
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from .data_classes import MigrationMapping, MigrationMappingList
from .data_model import INSTANCE_SOURCE_VIEW_ID


class MigrationSelector(DataSelector, ABC):
    @abstractmethod
    def get_ingestion_views(self) -> list[str]:
        raise NotImplementedError()


class MigrationCSVFileSelector(MigrationSelector):
    type: Literal["migrationCSVFile"] = "migrationCSVFile"
    datafile: Path

    @property
    def group(self) -> str:
        return f"Migration_{self.kind}"

    def __str__(self) -> str:
        return f"file_{self.datafile.name}"

    def get_ingestion_views(self) -> list[str]:
        views = {item.get_ingestion_view() for item in self.items}
        return sorted(views)

    @cached_property
    def items(self) -> MigrationMappingList:
        return MigrationMappingList.read_csv_file(self.datafile, resource_type=self.kind)


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
    UploadableStorageIO[AssetCentricId, MigrationSelector, InstanceApplyList, AssetCentricMappingList],
):
    KIND = "AssetCentricMigration"
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
    ) -> None:
        super().__init__(client)
        self.base = base
        # Used to cache the mapping between instance IDs and asset-centric IDs.
        # This is used in the as_id method to track the same object as it is converted to an instance.
        self._id_by_instance_id = ThreadSafeDict[InstanceId, int]()

    def _get_id(self, item: dict[str, JsonVal]) -> AssetCentricId | None:
        if "id" in item and isinstance(item["id"], int):
            return AssetCentricId(self.base.RESOURCE_TYPE, item["id"])
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

    def as_id(self, item: dict[str, JsonVal] | object) -> AssetCentricId:
        # When multiple threads are accessing this class, they will always operate on different ids
        if isinstance(item, AssetCentricMapping):
            instance_id = item.mapping.instance_id
            self._id_by_instance_id.setdefault(instance_id, item.mapping.id)
            return AssetCentricId(self.base.RESOURCE_TYPE, self._id_by_instance_id[instance_id])
        elif isinstance(item, Event | Asset | TimeSeries | FileMetadata | PendingInstanceId):
            if item.id is None:
                raise TypeError(f"Resource of type {type(item).__name__!r} is missing an 'id'.")
            return AssetCentricId(self.base.RESOURCE_TYPE, item.id)
        elif isinstance(item, NodeApply | EdgeApply):
            instance_id_ = item.as_id()
            if instance_id_ not in self._id_by_instance_id:
                raise ValueError(f"Missing mapping for instance {instance_id_!r}")
            return AssetCentricId(self.base.RESOURCE_TYPE, self._id_by_instance_id[instance_id_])
        elif isinstance(item, dict) and (id_int := self._get_id(item)):
            return id_int
        elif isinstance(item, dict) and (parsed_instance_id := self._get_instance_id(item)):
            if parsed_instance_id not in self._id_by_instance_id:
                raise ValueError(f"Missing mapping for instance {parsed_instance_id!r}")
            return AssetCentricId(self.base.RESOURCE_TYPE, self._id_by_instance_id[parsed_instance_id])
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

    def count(self, selector: MigrationSelector) -> int | None:
        if not isinstance(selector, MigrationCSVFileSelector):
            raise ToolkitNotImplementedError(f"Selector {type(selector)} is not supported for count")
        return len(selector.items)

    def data_to_json_chunk(
        self, data_chunk: AssetCentricMappingList, selector: MigrationSelector
    ) -> list[dict[str, JsonVal]]:
        return data_chunk.dump()

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> InstanceApplyList:
        raise NotImplementedError()


class FileMetaAdapter(
    AssetCentricMigrationIOAdapter[
        str,
        FileMetadataWrite,
        FileMetadata,
        FileMetadataWriteList,
        FileMetadataList,
    ]
):
    """Adapter for migrating file metadata to data model instances.

    This is necessary to link asset-centric FileMetadata to their new CogniteFile instances using the
    files/set-pending-instance-ids.
    """

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client, FileMetadataIO(client))

    @staticmethod
    def as_pending_instance_id(item: InstanceApply) -> PendingInstanceId:
        """Convert an InstanceApply to a PendingInstanceId for linking."""
        source = next((source for source in item.sources if source.source == INSTANCE_SOURCE_VIEW_ID), None)
        if source is None:
            raise ValueError(f"Cannot extract ID from item of type {type(item).__name__!r}")
        if not isinstance(source.properties["id"], int):
            raise ValueError(f"Unexpected ID type: {type(source.properties['id']).__name__!r}")
        id_ = source.properties["id"]
        return PendingInstanceId(
            pending_instance_id=NodeId(item.space, item.external_id),
            id=id_,
        )

    def upload_items(
        self, data_chunk: InstanceApplyList, http_client: HTTPClient, selector: MigrationSelector | None = None
    ) -> Sequence[HTTPMessage]:
        """Upload items by first linking them using files/set-pending-instance-ids and then uploading the instances."""
        config = http_client.config
        results: list[HTTPMessage] = []
        successful_linked: set[int] = set()
        for batch in chunker_sequence(data_chunk, self.CHUNK_SIZE):
            batch_results = http_client.request_with_retries(
                message=ItemsRequest(
                    endpoint_url=config.create_api_url("files/set-pending-instance-ids"),
                    method="POST",
                    api_version="alpha",
                    items=[TmpUploadItem(self.as_pending_instance_id(item), as_id_fun=self.as_id) for item in batch],
                )
            )
            for res in batch_results:
                if isinstance(res, SuccessResponseItems):
                    successful_linked.update(res.ids)
            results.extend(batch_results)
        to_upload = [item for item in data_chunk if self.as_id(item) in successful_linked]
        if to_upload:
            results.extend(list(super().upload_items(InstanceApplyList(to_upload), http_client, selector)))
        return results

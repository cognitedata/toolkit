from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Generic, Literal

from cognite.client.data_classes import (
    FileMetadata,
    FileMetadataList,
    FileMetadataWrite,
    FileMetadataWriteList,
)
from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.data_modeling import InstanceApply, NodeId, ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList
from cognite_toolkit._cdf_tk.client.data_classes.pending_instances_ids import PendingInstanceId
from cognite_toolkit._cdf_tk.cruds._base_cruds import T_ID
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.storageio import (
    BaseAssetCentricIO,
    FileMetadataIO,
    InstanceIO,
    UploadableStorageIO,
)
from cognite_toolkit._cdf_tk.storageio._base import Page, UploadItem
from cognite_toolkit._cdf_tk.storageio.selectors import (
    DataSelector,
    DataSetSelector,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage, ItemsRequest, SuccessResponseItems
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal, T_WritableCogniteResourceList

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


class MigrateDataSetSelector(MigrationSelector):
    type: Literal["migrateDataSet"] = "migrateDataSet"
    kind: Literal["Assets", "Events", "TimeSeries", "FileMetadata"]
    data_set_external_id: str
    ingestion_view: str | None = None
    preferred_consumer_view: ViewId | None = None

    @property
    def group(self) -> str:
        return f"DataSet_{self.data_set_external_id}"

    def __str__(self) -> str:
        return self.kind

    def get_schema_spaces(self) -> list[str] | None:
        return None

    def get_instance_spaces(self) -> list[str] | None:
        return None

    def get_ingestion_views(self) -> list[str]:
        return []


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
    UploadableStorageIO[MigrationSelector, AssetCentricMapping[T_WritableCogniteResource], InstanceApply],
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

    def as_id(self, item: AssetCentricMapping) -> str:
        return f"{item.mapping.resource_type}_{item.mapping.id}"

    def stream_data(self, selector: MigrationSelector, limit: int | None = None) -> Iterator[Page]:
        if isinstance(selector, MigrationCSVFileSelector):
            iterator = self._stream_from_csv(selector, limit)
        elif isinstance(selector, MigrateDataSetSelector):
            iterator = self._stream_given_dataset(selector, limit)
        else:
            raise ToolkitNotImplementedError(f"Selector {type(selector)} is not supported for stream_data")
        yield from (Page(worker_id="main", items=items) for items in iterator)

    def _stream_from_csv(
        self, selector: MigrationCSVFileSelector, limit: int | None = None
    ) -> Iterator[Sequence[AssetCentricMapping[T_WritableCogniteResource]]]:
        items = selector.items
        if limit is not None:
            items = MigrationMappingList(items[:limit])
        chunk: list[AssetCentricMapping[T_WritableCogniteResource]] = []
        for current_batch in chunker_sequence(items, self.CHUNK_SIZE):
            resources = self.base.retrieve(current_batch.get_ids())
            for mapping, resource in zip(current_batch, resources, strict=True):
                chunk.append(AssetCentricMapping(mapping=mapping, resource=resource))
            if chunk:
                yield chunk
                chunk = []

    def count(self, selector: MigrationSelector) -> int | None:
        if isinstance(selector, MigrationCSVFileSelector):
            return len(selector.items)
        elif isinstance(selector, MigrateDataSetSelector):
            asset_centric_selector = self._get_asset_centric_selector(selector)
            return self.base.count(asset_centric_selector)
        else:
            raise ToolkitNotImplementedError(f"Selector {type(selector)} is not supported for count")

    @staticmethod
    def _get_asset_centric_selector(selector: MigrateDataSetSelector) -> DataSetSelector:
        return DataSetSelector(
            data_set_external_id=selector.data_set_external_id,
            kind=selector.kind,
        )

    def _stream_given_dataset(
        self, selector: MigrateDataSetSelector, limit: int | None = None
    ) -> Iterator[Sequence[AssetCentricMapping[T_WritableCogniteResource]]]:
        asset_centric_selector = self._get_asset_centric_selector(selector)
        for data_chunk in self.base.stream_data(asset_centric_selector, limit):
            mapping_list = AssetCentricMappingList[T_WritableCogniteResource]([])
            for resource in data_chunk.items:
                instance_space = self.client.migration.space_source.retrieve(data_set_id=resource.data_set_id)
                if instance_space:
                    instance_space = "<InstanceSpaceMissing>"
                mapping = MigrationMapping(
                    resource_type=selector.kind.lower(),
                    instance_id=NodeId(
                        space=instance_space,
                        external_id=resource.external_id,
                    ),
                    id=resource.id,
                    data_set_id=resource.data_set_id,
                    ingestion_view=selector.ingestion_view,
                    preferred_consumer_view=selector.preferred_consumer_view,
                )
                mapping_list.append(
                    AssetCentricMapping(
                        mapping=mapping,
                        resource=resource,
                    )
                )
            yield mapping_list

    def data_to_json_chunk(
        self, data_chunk: Sequence[AssetCentricMapping[T_WritableCogniteResource]]
    ) -> list[dict[str, JsonVal]]:
        return [item.dump() for item in data_chunk]

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> InstanceApply:
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
        self,
        data_chunk: Sequence[UploadItem[InstanceApply]],
        http_client: HTTPClient,
        selector: MigrationSelector | None = None,
    ) -> Sequence[HTTPMessage]:
        """Upload items by first linking them using files/set-pending-instance-ids and then uploading the instances."""
        config = http_client.config
        results: list[HTTPMessage] = []
        successful_linked: set[str] = set()
        for batch in chunker_sequence(data_chunk, self.CHUNK_SIZE):
            batch_results = http_client.request_with_retries(
                message=ItemsRequest(
                    endpoint_url=config.create_api_url("files/set-pending-instance-ids"),
                    method="POST",
                    api_version="alpha",
                    items=[
                        UploadItem(source_id=item.source_id, item=self.as_pending_instance_id(item.item))
                        for item in batch
                    ],
                )
            )
            for res in batch_results:
                if isinstance(res, SuccessResponseItems):
                    successful_linked.update(res.ids)
            results.extend(batch_results)
        to_upload = [item for item in data_chunk if item.source_id in successful_linked]
        if to_upload:
            results.extend(list(super().upload_items(to_upload, http_client, selector)))
        return results

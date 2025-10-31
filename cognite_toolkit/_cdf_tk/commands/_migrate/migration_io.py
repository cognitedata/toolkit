from collections.abc import Iterator, Mapping, Sequence
from typing import ClassVar, cast

from cognite.client.data_classes._base import (
    T_WritableCogniteResource,
)
from cognite.client.data_classes.data_modeling import InstanceApply, NodeId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.pending_instances_ids import PendingInstanceId
from cognite_toolkit._cdf_tk.constants import MISSING_EXTERNAL_ID, MISSING_INSTANCE_SPACE
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.storageio import (
    HierarchyIO,
    InstanceIO,
    UploadableStorageIO,
)
from cognite_toolkit._cdf_tk.storageio._base import Page, UploadItem
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage, ItemsRequest, SuccessResponseItems
from cognite_toolkit._cdf_tk.utils.useful_types import (
    AssetCentricKind,
    AssetCentricType,
    JsonVal,
)

from .data_classes import AssetCentricMapping, AssetCentricMappingList, MigrationMapping, MigrationMappingList
from .data_model import INSTANCE_SOURCE_VIEW_ID
from .selectors import AssetCentricMigrationSelector, MigrateDataSetSelector, MigrationCSVFileSelector


class AssetCentricMigrationIO(
    UploadableStorageIO[AssetCentricMigrationSelector, AssetCentricMapping[T_WritableCogniteResource], InstanceApply]
):
    KIND = "AssetCentricMigration"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    CHUNK_SIZE = 1000
    UPLOAD_ENDPOINT = InstanceIO.UPLOAD_ENDPOINT

    PENDING_INSTANCE_ID_ENDPOINT_BY_KIND: ClassVar[Mapping[AssetCentricKind, str]] = {
        "TimeSeries": "/timeseries/set-pending-instance-ids",
        "FileMetadata": "/files/set-pending-instance-ids",
    }

    def __init__(self, client: ToolkitClient, skip_linking: bool = True) -> None:
        super().__init__(client)
        self.hierarchy = HierarchyIO(client)
        self.skip_linking = skip_linking

    def as_id(self, item: AssetCentricMapping) -> str:
        return f"{item.mapping.resource_type}_{item.mapping.id}"

    def stream_data(self, selector: AssetCentricMigrationSelector, limit: int | None = None) -> Iterator[Page]:
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
            resources = self.hierarchy.get_resource_io(selector.kind).retrieve(current_batch.get_ids())
            for mapping, resource in zip(current_batch, resources, strict=True):
                chunk.append(AssetCentricMapping(mapping=mapping, resource=resource))
            if chunk:
                yield chunk
                chunk = []

    def count(self, selector: AssetCentricMigrationSelector) -> int | None:
        if isinstance(selector, MigrationCSVFileSelector):
            return len(selector.items)
        elif isinstance(selector, MigrateDataSetSelector):
            return self.hierarchy.count(selector.as_asset_centric_selector())
        else:
            raise ToolkitNotImplementedError(f"Selector {type(selector)} is not supported for count")

    def _stream_given_dataset(
        self, selector: MigrateDataSetSelector, limit: int | None = None
    ) -> Iterator[Sequence[AssetCentricMapping[T_WritableCogniteResource]]]:
        asset_centric_selector = selector.as_asset_centric_selector()
        for data_chunk in self.hierarchy.stream_data(asset_centric_selector, limit):
            mapping_list = AssetCentricMappingList[T_WritableCogniteResource]([])
            for resource in data_chunk.items:
                # We know data_set_id is here as we are using a DataSetSelector
                data_set_id = cast(int, resource.data_set_id)
                space_source = self.client.migration.space_source.retrieve(data_set_id=data_set_id)
                instance_space = space_source.instance_space if space_source else None
                if instance_space is None:
                    instance_space = MISSING_INSTANCE_SPACE
                external_id = resource.external_id
                if external_id is None:
                    external_id = MISSING_EXTERNAL_ID.format(project=self.client.config.project, id=resource.id)
                mapping = MigrationMapping(
                    resource_type=self._kind_to_resource_type(selector.kind),
                    instance_id=NodeId(
                        space=instance_space,
                        external_id=external_id,
                    ),
                    id=resource.id,
                    data_set_id=resource.data_set_id,
                    ingestion_view=selector.ingestion_mapping,
                    preferred_consumer_view=selector.preferred_consumer_view,
                )
                mapping_list.append(AssetCentricMapping(mapping=mapping, resource=resource))
            yield mapping_list

    @staticmethod
    def _kind_to_resource_type(kind: AssetCentricKind) -> AssetCentricType:
        mapping: dict[AssetCentricKind, AssetCentricType] = {
            "Assets": "asset",
            "Events": "event",
            "TimeSeries": "timeseries",
            "FileMetadata": "file",
        }
        try:
            return mapping[kind]
        except KeyError as e:
            raise ToolkitNotImplementedError(f"Kind '{kind}' is not supported") from e

    def data_to_json_chunk(
        self,
        data_chunk: Sequence[AssetCentricMapping[T_WritableCogniteResource]],
        selector: AssetCentricMigrationSelector | None = None,
    ) -> list[dict[str, JsonVal]]:
        return [item.dump() for item in data_chunk]

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> InstanceApply:
        raise NotImplementedError()

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[InstanceApply]],
        http_client: HTTPClient,
        selector: AssetCentricMigrationSelector | None = None,
    ) -> Sequence[HTTPMessage]:
        """Upload items by first linking them using files/set-pending-instance-ids and then uploading the instances."""
        if self.skip_linking:
            return list(super().upload_items(data_chunk, http_client, None))
        elif selector is None:
            raise ToolkitNotImplementedError(f"Selector must be provided for uploading {self.KIND} items.")
        elif selector.kind not in self.PENDING_INSTANCE_ID_ENDPOINT_BY_KIND:
            return list(super().upload_items(data_chunk, http_client, None))

        pending_instance_id_endpoint = self.PENDING_INSTANCE_ID_ENDPOINT_BY_KIND[selector.kind]
        results: list[HTTPMessage] = []
        to_upload = self.link_asset_centric(data_chunk, http_client, results, pending_instance_id_endpoint)
        if to_upload:
            results.extend(list(super().upload_items(to_upload, http_client, None)))
        return results

    @classmethod
    def link_asset_centric(
        cls,
        data_chunk: Sequence[UploadItem[InstanceApply]],
        http_client: HTTPClient,
        results: list[HTTPMessage],
        pending_instance_id_endpoint: str,
    ) -> Sequence[UploadItem[InstanceApply]]:
        """Links asset-centric resources to their (uncreated) instances using the pending-instance-ids endpoint."""
        config = http_client.config
        successful_linked: set[str] = set()
        for batch in chunker_sequence(data_chunk, cls.CHUNK_SIZE):
            batch_results = http_client.request_with_retries(
                message=ItemsRequest(
                    endpoint_url=config.create_api_url(pending_instance_id_endpoint),
                    method="POST",
                    api_version="alpha",
                    items=[
                        UploadItem(source_id=item.source_id, item=cls.as_pending_instance_id(item.item))
                        for item in batch
                    ],
                )
            )
            for res in batch_results:
                if isinstance(res, SuccessResponseItems):
                    successful_linked.update(res.ids)
            results.extend(batch_results)
        to_upload = [item for item in data_chunk if item.source_id in successful_linked]
        return to_upload

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

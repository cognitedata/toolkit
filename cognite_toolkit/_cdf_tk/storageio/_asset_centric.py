from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, MutableSequence, Sequence
from typing import ClassVar, Generic

from cognite.client.data_classes import (
    Asset,
    AssetList,
    AssetWrite,
    AssetWriteList,
    Event,
    EventList,
    EventWrite,
    EventWriteList,
    FileMetadata,
    FileMetadataList,
    FileMetadataWrite,
    FileMetadataWriteList,
    Label,
    LabelDefinition,
    TimeSeries,
    TimeSeriesList,
    TimeSeriesWrite,
    TimeSeriesWriteList,
)
from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
    WriteableCogniteResourceList,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.cruds import (
    AssetCRUD,
    DataSetsCRUD,
    EventCRUD,
    FileMetadataCRUD,
    LabelCRUD,
    ResourceCRUD,
    TimeSeriesCRUD,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.utils.aggregators import (
    AssetAggregator,
    AssetCentricAggregator,
    EventAggregator,
    FileAggregator,
    TimeSeriesAggregator,
)
from cognite_toolkit._cdf_tk.utils.cdf import metadata_key_counts
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage, SimpleBodyRequest
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID, AssetCentricType, JsonVal, T_WritableCogniteResourceList

from ._base import ConfigurableStorageIO, Page, StorageIOConfig, TableStorageIO, UploadableStorageIO, UploadItem
from .selectors import AssetCentricSelector, AssetSubtreeSelector, DataSetSelector


class BaseAssetCentricIO(
    Generic[T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList],
    TableStorageIO[AssetCentricSelector, T_WritableCogniteResource],
    ConfigurableStorageIO[AssetCentricSelector, T_WritableCogniteResource],
    UploadableStorageIO[AssetCentricSelector, T_WritableCogniteResource, T_WriteClass],
    ABC,
):
    RESOURCE_TYPE: ClassVar[AssetCentricType]
    CHUNK_SIZE = 1000
    BASE_SELECTOR = AssetCentricSelector

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._loader = self._get_loader()
        self._aggregator = self._get_aggregator()
        self._downloaded_data_sets_by_selector: dict[AssetCentricSelector, set[int]] = defaultdict(set)
        self._downloaded_labels_by_selector: dict[AssetCentricSelector, set[str]] = defaultdict(set)

    @abstractmethod
    def _get_loader(
        self,
    ) -> ResourceCRUD[
        T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
    ]:
        raise NotImplementedError()

    @abstractmethod
    def _get_aggregator(self) -> AssetCentricAggregator:
        raise NotImplementedError()

    @abstractmethod
    def retrieve(self, ids: Sequence[int]) -> T_WritableCogniteResourceList:
        raise NotImplementedError()

    def count(self, selector: AssetCentricSelector) -> int | None:
        if isinstance(selector, DataSetSelector):
            return self._aggregator.count(data_set_external_id=selector.data_set_external_id)
        elif isinstance(selector, AssetSubtreeSelector):
            return self._aggregator.count(hierarchy=selector.hierarchy)
        return None

    def data_to_json_chunk(self, data_chunk: Sequence[T_WritableCogniteResource]) -> list[dict[str, JsonVal]]:
        return [self._loader.dump_resource(item) for item in data_chunk]

    def configurations(self, selector: AssetCentricSelector) -> Iterable[StorageIOConfig]:
        data_set_ids = self._downloaded_data_sets_by_selector[selector]
        if data_set_ids:
            data_set_external_ids = self.client.lookup.data_sets.external_id(list(data_set_ids))
            yield from self._configurations(data_set_external_ids, DataSetsCRUD.create_loader(self.client))

        yield from self._configurations(
            list(self._downloaded_labels_by_selector[selector]), LabelCRUD.create_loader(self.client)
        )

    def _get_hierarchy_dataset_pair(self, selector: AssetCentricSelector) -> tuple[list[str] | None, list[str] | None]:
        asset_subtree_external_ids: list[str] | None = None
        data_set_external_ids: list[str] | None = None
        if isinstance(selector, DataSetSelector):
            data_set_external_ids = [selector.data_set_external_id]
        elif isinstance(selector, AssetSubtreeSelector):
            asset_subtree_external_ids = [selector.hierarchy]
        else:
            # This selector is for uploads, not for downloading from CDF.
            raise ToolkitNotImplementedError(f"Selector type {type(selector)} not supported for {type(self).__name__}.")
        return asset_subtree_external_ids, data_set_external_ids

    def _collect_dependencies(
        self, resources: AssetList | FileMetadataList | TimeSeriesList | EventList, selector: AssetCentricSelector
    ) -> None:
        for resource in resources:
            if resource.data_set_id:
                self._downloaded_data_sets_by_selector[selector].add(resource.data_set_id)
            if isinstance(resource, Asset | FileMetadata):
                for label in resource.labels or []:
                    if isinstance(label, str):
                        self._downloaded_labels_by_selector[selector].add(label)
                    elif isinstance(label, Label | LabelDefinition) and label.external_id:
                        self._downloaded_labels_by_selector[selector].add(label.external_id)
                    elif isinstance(label, dict) and "externalId" in label:
                        self._downloaded_labels_by_selector[selector].add(label["externalId"])

    @classmethod
    def _configurations(
        cls,
        ids: list[str],
        loader: DataSetsCRUD | LabelCRUD,
    ) -> Iterable[StorageIOConfig]:
        if not ids:
            return
        items = loader.retrieve(list(ids))
        yield StorageIOConfig(
            kind=loader.kind,
            folder_name=loader.folder_name,
            # We know that the items will be labels for LabelLoader and data sets for DataSetsLoader
            value=[loader.dump_resource(item) for item in items],  # type: ignore[arg-type]
        )

    def _create_identifier(self, internal_id: int) -> str:
        return f"INTERNAL_ID_project_{self.client.config.project}_{internal_id!s}"


class AssetIO(BaseAssetCentricIO[str, AssetWrite, Asset, AssetWriteList, AssetList]):
    KIND = "Assets"
    RESOURCE_TYPE = "asset"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    UPLOAD_ENDPOINT = "/assets"

    def as_id(self, item: Asset) -> str:
        return item.external_id if item.external_id is not None else self._create_identifier(item.id)

    def _get_loader(self) -> AssetCRUD:
        return AssetCRUD.create_loader(self.client)

    def _get_aggregator(self) -> AssetCentricAggregator:
        return AssetAggregator(self.client)

    def get_schema(self, selector: AssetCentricSelector) -> list[SchemaColumn]:
        data_set_ids: list[int] = []
        if isinstance(selector, DataSetSelector):
            data_set_ids.append(self.client.lookup.data_sets.id(selector.data_set_external_id))
        hierarchy: list[int] = []
        if isinstance(selector, AssetSubtreeSelector):
            hierarchy.append(self.client.lookup.assets.id(selector.hierarchy))

        if hierarchy or data_set_ids:
            metadata_keys = metadata_key_counts(
                self.client, "assets", data_sets=data_set_ids or None, hierarchies=hierarchy or None
            )
        else:
            metadata_keys = []
        metadata_schema: list[SchemaColumn] = []
        if metadata_keys:
            metadata_schema.extend(
                [SchemaColumn(name=f"metadata.{key}", type="string", is_array=False) for key, _ in metadata_keys]
            )
        asset_schema = [
            SchemaColumn(name="externalId", type="string"),
            SchemaColumn(name="name", type="string"),
            SchemaColumn(name="parentExternalId", type="string"),
            SchemaColumn(name="description", type="string"),
            SchemaColumn(name="dataSetExternalId", type="string"),
            SchemaColumn(name="source", type="string"),
            SchemaColumn(name="labels", type="string", is_array=True),
            SchemaColumn(name="geoLocation", type="json"),
        ]
        return asset_schema + metadata_schema

    def stream_data(self, selector: AssetCentricSelector, limit: int | None = None) -> Iterable[Page]:
        asset_subtree_external_ids, data_set_external_ids = self._get_hierarchy_dataset_pair(selector)
        for asset_list in self.client.assets(
            chunk_size=self.CHUNK_SIZE,
            limit=limit,
            asset_subtree_external_ids=asset_subtree_external_ids,
            data_set_external_ids=data_set_external_ids,
        ):
            self._collect_dependencies(asset_list, selector)
            yield Page(worker_id="main", items=asset_list)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> AssetWrite:
        return self._loader.load_resource(item_json)

    def retrieve(self, ids: Sequence[int]) -> AssetList:
        return self.client.assets.retrieve_multiple(ids)


class FileMetadataIO(BaseAssetCentricIO[str, FileMetadataWrite, FileMetadata, FileMetadataWriteList, FileMetadataList]):
    KIND = "FileMetadata"
    RESOURCE_TYPE = "file"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    UPLOAD_ENDPOINT = "/files"

    def as_id(self, item: FileMetadata) -> str:
        return item.external_id if item.external_id is not None else self._create_identifier(item.id)

    def _get_loader(self) -> FileMetadataCRUD:
        return FileMetadataCRUD.create_loader(self.client)

    def _get_aggregator(self) -> AssetCentricAggregator:
        return FileAggregator(self.client)

    def get_schema(self, selector: AssetCentricSelector) -> list[SchemaColumn]:
        data_set_ids: list[int] = []
        if isinstance(selector, DataSetSelector):
            data_set_ids.append(self.client.lookup.data_sets.id(selector.data_set_external_id))
        if isinstance(selector, AssetSubtreeSelector):
            raise ToolkitNotImplementedError(f"Selector type {type(selector)} not supported for FileIO.")

        if data_set_ids:
            metadata_keys = metadata_key_counts(self.client, "files", data_sets=data_set_ids or None, hierarchies=None)
        else:
            metadata_keys = []
        metadata_schema: list[SchemaColumn] = []
        if metadata_keys:
            metadata_schema.extend(
                [SchemaColumn(name=f"metadata.{key}", type="string", is_array=False) for key, _ in metadata_keys]
            )
        file_schema = [
            SchemaColumn(name="externalId", type="string"),
            SchemaColumn(name="name", type="string"),
            SchemaColumn(name="directory", type="string"),
            SchemaColumn(name="mimeType", type="string"),
            SchemaColumn(name="dataSetExternalId", type="string"),
            SchemaColumn(name="assetExternalIds", type="string", is_array=True),
            SchemaColumn(name="source", type="string"),
            SchemaColumn(name="sourceCreatedTime", type="integer"),
            SchemaColumn(name="sourceModifiedTime", type="integer"),
            SchemaColumn(name="securityCategories", type="string", is_array=True),
            SchemaColumn(name="labels", type="string", is_array=True),
            SchemaColumn(name="geoLocation", type="json"),
        ]
        return file_schema + metadata_schema

    def stream_data(self, selector: AssetCentricSelector, limit: int | None = None) -> Iterable[Page]:
        asset_subtree_external_ids, data_set_external_ids = self._get_hierarchy_dataset_pair(selector)
        for file_list in self.client.files(
            chunk_size=self.CHUNK_SIZE,
            limit=limit,
            asset_subtree_external_ids=asset_subtree_external_ids,
            data_set_external_ids=data_set_external_ids,
        ):
            self._collect_dependencies(file_list, selector)
            yield Page(worker_id="main", items=file_list)

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[FileMetadataWrite]],
        http_client: HTTPClient,
        selector: AssetCentricSelector | None = None,
    ) -> Sequence[HTTPMessage]:
        # The /files endpoint only supports creating one file at a time, so we override the default chunked
        # upload behavior to upload one by one.
        config = http_client.config
        results: MutableSequence[HTTPMessage] = []
        for item in data_chunk:
            file_result = http_client.request_with_retries(
                message=SimpleBodyRequest(
                    endpoint_url=config.create_api_url(self.UPLOAD_ENDPOINT),
                    method="POST",
                    # MyPy does not understand that .dump is valid json
                    body_content=item.dump(),  # type: ignore[arg-type]
                )
            )
            results.extend(file_result)
        return results

    def retrieve(self, ids: Sequence[int]) -> FileMetadataList:
        return self.client.files.retrieve_multiple(ids)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> FileMetadataWrite:
        return self._loader.load_resource(item_json)


class TimeSeriesIO(BaseAssetCentricIO[str, TimeSeriesWrite, TimeSeries, TimeSeriesWriteList, TimeSeriesList]):
    KIND = "TimeSeries"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    UPLOAD_ENDPOINT = "/timeseries"
    RESOURCE_TYPE = "timeseries"

    def as_id(self, item: TimeSeries) -> str:
        return item.external_id if item.external_id is not None else self._create_identifier(item.id)

    def _get_loader(self) -> TimeSeriesCRUD:
        return TimeSeriesCRUD.create_loader(self.client)

    def _get_aggregator(self) -> AssetCentricAggregator:
        return TimeSeriesAggregator(self.client)

    def retrieve(self, ids: Sequence[int]) -> TimeSeriesList:
        return self.client.time_series.retrieve_multiple(ids=ids)

    def stream_data(self, selector: AssetCentricSelector, limit: int | None = None) -> Iterable[Page]:
        asset_subtree_external_ids, data_set_external_ids = self._get_hierarchy_dataset_pair(selector)
        for ts_list in self.client.time_series(
            chunk_size=self.CHUNK_SIZE,
            limit=limit,
            asset_subtree_external_ids=asset_subtree_external_ids,
            data_set_external_ids=data_set_external_ids,
        ):
            self._collect_dependencies(ts_list, selector)
            yield Page(worker_id="main", items=ts_list)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> TimeSeriesWrite:
        return self._loader.load_resource(item_json)

    def get_schema(self, selector: AssetCentricSelector) -> list[SchemaColumn]:
        data_set_ids: list[int] = []
        if isinstance(selector, DataSetSelector):
            data_set_ids.append(self.client.lookup.data_sets.id(selector.data_set_external_id))
        elif isinstance(selector, AssetSubtreeSelector):
            raise ToolkitNotImplementedError(f"Selector type {type(selector)} not supported for {type(self).__name__}.")

        if data_set_ids:
            metadata_keys = metadata_key_counts(
                self.client, "timeseries", data_sets=data_set_ids or None, hierarchies=None
            )
        else:
            metadata_keys = []
        metadata_schema: list[SchemaColumn] = []
        if metadata_keys:
            metadata_schema.extend(
                [SchemaColumn(name=f"metadata.{key}", type="string", is_array=False) for key, _ in metadata_keys]
            )
        ts_schema = [
            SchemaColumn(name="externalId", type="string"),
            SchemaColumn(name="name", type="string"),
            SchemaColumn(name="isString", type="boolean"),
            SchemaColumn(name="unit", type="string"),
            SchemaColumn(name="unitExternalId", type="string"),
            SchemaColumn(name="assetExternalId", type="string"),
            SchemaColumn(name="isStep", type="boolean"),
            SchemaColumn(name="description", type="string"),
            SchemaColumn(name="securityCategories", type="string", is_array=True),
            SchemaColumn(name="dataSetExternalId", type="string"),
        ]
        return ts_schema + metadata_schema


class EventIO(BaseAssetCentricIO[str, EventWrite, Event, EventWriteList, EventList]):
    KIND = "Events"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    UPLOAD_ENDPOINT = "/events"
    RESOURCE_TYPE = "event"

    def as_id(self, item: Event) -> str:
        return item.external_id if item.external_id is not None else self._create_identifier(item.id)

    def _get_loader(self) -> EventCRUD:
        return EventCRUD.create_loader(self.client)

    def _get_aggregator(self) -> AssetCentricAggregator:
        return EventAggregator(self.client)

    def get_schema(self, selector: AssetCentricSelector) -> list[SchemaColumn]:
        data_set_ids: list[int] = []
        if isinstance(selector, DataSetSelector):
            data_set_ids.append(self.client.lookup.data_sets.id(selector.data_set_external_id))
        hierarchy: list[int] = []
        if isinstance(selector, AssetSubtreeSelector):
            raise ToolkitNotImplementedError(f"Selector type {type(selector)} not supported for {type(self).__name__}.")

        if hierarchy or data_set_ids:
            metadata_keys = metadata_key_counts(
                self.client, "events", data_sets=data_set_ids or None, hierarchies=hierarchy or None
            )
        else:
            metadata_keys = []
        metadata_schema: list[SchemaColumn] = []
        if metadata_keys:
            metadata_schema.extend(
                [SchemaColumn(name=f"metadata.{key}", type="string", is_array=False) for key, _ in metadata_keys]
            )
        event_schema = [
            SchemaColumn(name="externalId", type="string"),
            SchemaColumn(name="dataSetExternalId", type="string"),
            SchemaColumn(name="startTime", type="integer"),
            SchemaColumn(name="endTime", type="integer"),
            SchemaColumn(name="type", type="string"),
            SchemaColumn(name="subtype", type="string"),
            SchemaColumn(name="description", type="string"),
            SchemaColumn(name="assetExternalIds", type="string", is_array=True),
            SchemaColumn(name="source", type="string"),
        ]
        return event_schema + metadata_schema

    def stream_data(self, selector: AssetCentricSelector, limit: int | None = None) -> Iterable[Page]:
        asset_subtree_external_ids, data_set_external_ids = self._get_hierarchy_dataset_pair(selector)
        for event_list in self.client.events(
            chunk_size=self.CHUNK_SIZE,
            limit=limit,
            asset_subtree_external_ids=asset_subtree_external_ids,
            data_set_external_ids=data_set_external_ids,
        ):
            self._collect_dependencies(event_list, selector)
            yield Page(worker_id="main", items=event_list)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> EventWrite:
        return self._loader.load_resource(item_json)

    def retrieve(self, ids: Sequence[int]) -> EventList:
        return self.client.events.retrieve_multiple(ids)


class HierarchyIO(ConfigurableStorageIO[AssetCentricSelector, CogniteResource]):
    CHUNK_SIZE = 1000
    BASE_SELECTOR = AssetCentricSelector
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._asset_io = AssetIO(client)
        self._file_io = FileMetadataIO(client)
        self._timeseries_io = TimeSeriesIO(client)
        self._event_io = EventIO(client)
        self._io_by_kind: dict[str, BaseAssetCentricIO] = {
            self._asset_io.KIND: self._asset_io,
            self._file_io.KIND: self._file_io,
            self._timeseries_io.KIND: self._timeseries_io,
            self._event_io.KIND: self._event_io,
        }

    def as_id(self, item: CogniteResource) -> int:
        if hasattr(item, "id") and isinstance(item.id, int):
            return item.id
        if isinstance(item, dict) and isinstance(item.get("id"), int):
            return item["id"]  # type: ignore[return-value]
        raise TypeError(f"Cannot extract ID from item of type {type(item).__name__!r}")

    def stream_data(
        self, selector: AssetCentricSelector, limit: int | None = None
    ) -> Iterable[WriteableCogniteResourceList]:
        io = self._get_io(selector)
        yield from io.stream_data(selector, limit)

    def count(self, selector: AssetCentricSelector) -> int | None:
        io = self._get_io(selector)
        return io.count(selector)

    def data_to_json_chunk(
        self, data_chunk: CogniteResourceList, selector: AssetCentricSelector
    ) -> list[dict[str, JsonVal]]:
        io = self._get_io(selector)
        return io.data_to_json_chunk(data_chunk, selector)

    def configurations(self, selector: AssetCentricSelector) -> Iterable[StorageIOConfig]:
        io = self._get_io(selector)
        yield from io.configurations(selector)

    def _get_io(self, selector: AssetCentricSelector) -> BaseAssetCentricIO:
        if not isinstance(selector, AssetSubtreeSelector | DataSetSelector):
            raise ToolkitNotImplementedError(f"Selector type {type(selector)} not supported for {type(self).__name__}.")
        return self._io_by_kind[selector.kind]

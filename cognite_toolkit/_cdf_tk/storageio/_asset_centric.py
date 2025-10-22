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
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
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
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID, AssetCentric, JsonVal, T_WritableCogniteResourceList

from ._base import StorageIOConfig, TableStorageIO
from .selectors import AssetCentricSelector, AssetSubtreeSelector, DataSetSelector


class BaseAssetCentricIO(
    Generic[T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList],
    TableStorageIO[int, AssetCentricSelector, T_CogniteResourceList, T_WritableCogniteResourceList],
    ABC,
):
    RESOURCE_TYPE: ClassVar[AssetCentric]
    CHUNK_SIZE = 1000
    BASE_SELECTOR = AssetCentricSelector

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._loader = self._get_loader()
        self._aggregator = self._get_aggregator()
        self._downloaded_data_sets_by_selector: dict[AssetCentricSelector, set[int]] = defaultdict(set)
        self._downloaded_labels_by_selector: dict[AssetCentricSelector, set[str]] = defaultdict(set)

    def as_id(self, item: dict[str, JsonVal] | object) -> int:
        if isinstance(item, dict) and isinstance(item.get("id"), int):
            # MyPy checked above.
            return item["id"]  # type: ignore[return-value]
        raise TypeError(f"Cannot extract ID from item of type {type(item).__name__!r}")

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

    def data_to_json_chunk(
        self, data_chunk: T_WritableCogniteResourceList, selector: AssetCentricSelector
    ) -> list[dict[str, JsonVal]]:
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


class AssetIO(BaseAssetCentricIO[str, AssetWrite, Asset, AssetWriteList, AssetList]):
    KIND = "Assets"
    RESOURCE_TYPE = "asset"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    UPLOAD_ENDPOINT = "/assets"

    def as_id(self, item: dict[str, JsonVal] | object) -> int:
        if isinstance(item, Asset | AssetWrite) and item.id is not None:  # type: ignore[union-attr]
            return item.id  # type: ignore[union-attr]
        return super().as_id(item)

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

    def stream_data(self, selector: AssetCentricSelector, limit: int | None = None) -> Iterable[AssetList]:
        asset_subtree_external_ids, data_set_external_ids = self._get_hierarchy_dataset_pair(selector)
        for asset_list in self.client.assets(
            chunk_size=self.CHUNK_SIZE,
            limit=limit,
            asset_subtree_external_ids=asset_subtree_external_ids,
            data_set_external_ids=data_set_external_ids,
        ):
            self._collect_dependencies(asset_list, selector)
            yield asset_list

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> AssetWriteList:
        return AssetWriteList([self._loader.load_resource(item) for item in data_chunk])

    def retrieve(self, ids: Sequence[int]) -> AssetList:
        return self.client.assets.retrieve_multiple(ids)


class FileMetadataIO(BaseAssetCentricIO[str, FileMetadataWrite, FileMetadata, FileMetadataWriteList, FileMetadataList]):
    KIND = "FileMetadata"
    RESOURCE_TYPE = "file"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    UPLOAD_ENDPOINT = "/files"

    def as_id(self, item: dict[str, JsonVal] | object) -> int:
        if isinstance(item, FileMetadata) and item.id is not None:
            return item.id
        return super().as_id(item)

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

    def stream_data(self, selector: AssetCentricSelector, limit: int | None = None) -> Iterable[FileMetadataList]:
        asset_subtree_external_ids, data_set_external_ids = self._get_hierarchy_dataset_pair(selector)
        for file_list in self.client.files(
            chunk_size=self.CHUNK_SIZE,
            limit=limit,
            asset_subtree_external_ids=asset_subtree_external_ids,
            data_set_external_ids=data_set_external_ids,
        ):
            self._collect_dependencies(file_list, selector)
            yield file_list

    def upload_items(
        self, data_chunk: FileMetadataWriteList, http_client: HTTPClient, selector: AssetCentricSelector | None = None
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
                    body_content=item.dump(camel_case=True),
                )
            )
            results.extend(file_result)
        return results

    def retrieve(self, ids: Sequence[int]) -> FileMetadataList:
        return self.client.files.retrieve_multiple(ids)

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> FileMetadataWriteList:
        return FileMetadataWriteList([self._loader.load_resource(item) for item in data_chunk])


class TimeSeriesIO(BaseAssetCentricIO[str, TimeSeriesWrite, TimeSeries, TimeSeriesWriteList, TimeSeriesList]):
    KIND = "TimeSeries"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    UPLOAD_ENDPOINT = "/timeseries"

    def as_id(self, item: dict[str, JsonVal] | object) -> int:
        if isinstance(item, TimeSeries) and item.id is not None:
            return item.id
        return super().as_id(item)

    def _get_loader(self) -> TimeSeriesCRUD:
        return TimeSeriesCRUD.create_loader(self.client)

    def _get_aggregator(self) -> AssetCentricAggregator:
        return TimeSeriesAggregator(self.client)

    def retrieve(self, ids: Sequence[int]) -> TimeSeriesList:
        return self.client.time_series.retrieve_multiple(ids=ids)

    def stream_data(self, selector: AssetCentricSelector, limit: int | None = None) -> Iterable[TimeSeriesList]:
        asset_subtree_external_ids, data_set_external_ids = self._get_hierarchy_dataset_pair(selector)
        for ts_list in self.client.time_series(
            chunk_size=self.CHUNK_SIZE,
            limit=limit,
            asset_subtree_external_ids=asset_subtree_external_ids,
            data_set_external_ids=data_set_external_ids,
        ):
            self._collect_dependencies(ts_list, selector)
            yield ts_list

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> TimeSeriesWriteList:
        return self._loader.list_write_cls([self._loader.load_resource(item) for item in data_chunk])

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

    def as_id(self, item: dict[str, JsonVal] | object) -> int:
        if isinstance(item, Event) and item.id is not None:
            return item.id
        return super().as_id(item)

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

    def stream_data(self, selector: AssetCentricSelector, limit: int | None = None) -> Iterable[EventList]:
        asset_subtree_external_ids, data_set_external_ids = self._get_hierarchy_dataset_pair(selector)
        for event_list in self.client.events(
            chunk_size=self.CHUNK_SIZE,
            limit=limit,
            asset_subtree_external_ids=asset_subtree_external_ids,
            data_set_external_ids=data_set_external_ids,
        ):
            self._collect_dependencies(event_list, selector)
            yield event_list

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> EventWriteList:
        return EventWriteList([self._loader.load_resource(item) for item in data_chunk])

    def retrieve(self, ids: Sequence[int]) -> EventList:
        return self.client.events.retrieve_multiple(ids)

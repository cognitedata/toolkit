from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Any, ClassVar, Generic

from cognite.client.data_classes import Label, LabelDefinition

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetAggregateItem, AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventRequest, EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse
from cognite_toolkit._cdf_tk.cruds import (
    AssetCRUD,
    DataSetsCRUD,
    EventCRUD,
    FileMetadataCRUD,
    LabelCRUD,
    TimeSeriesCRUD,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingResourceError, ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.protocols import T_ResourceRequest, T_ResourceResponse
from cognite_toolkit._cdf_tk.utils.aggregators import (
    AssetAggregator,
    AssetCentricAggregator,
    EventAggregator,
    FileAggregator,
    TimeSeriesAggregator,
)
from cognite_toolkit._cdf_tk.utils.cdf import metadata_key_counts
from cognite_toolkit._cdf_tk.utils.fileio import FileReader, SchemaColumn
from cognite_toolkit._cdf_tk.utils.fileio._readers import TableReader
from cognite_toolkit._cdf_tk.utils.useful_types import (
    AssetCentricType,
    JsonVal,
)
from cognite_toolkit._cdf_tk.utils.useful_types2 import AssetCentricResource

from ._base import (
    ConfigurableStorageIO,
    Page,
    StorageIOConfig,
    TableStorageIO,
    TableUploadableStorageIO,
    UploadItem,
)
from .selectors import AssetCentricSelector, AssetSubtreeSelector, DataSetSelector


class AssetCentricIO(
    Generic[T_ResourceResponse],
    TableStorageIO[AssetCentricSelector, T_ResourceResponse],
    ConfigurableStorageIO[AssetCentricSelector, T_ResourceResponse],
    ABC,
):
    RESOURCE_TYPE: ClassVar[AssetCentricType]
    CHUNK_SIZE = 1000
    BASE_SELECTOR = AssetCentricSelector

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._aggregator = self._get_aggregator()
        self._downloaded_data_sets_by_selector: dict[AssetCentricSelector, set[int]] = defaultdict(set)
        self._downloaded_labels_by_selector: dict[AssetCentricSelector, set[str]] = defaultdict(set)

    @abstractmethod
    def _get_aggregator(self) -> AssetCentricAggregator:
        raise NotImplementedError()

    @abstractmethod
    def retrieve(self, ids: Sequence[int]) -> Sequence[T_ResourceResponse]:
        raise NotImplementedError()

    def count(self, selector: AssetCentricSelector) -> int | None:
        if isinstance(selector, DataSetSelector):
            return self._aggregator.count(data_set_external_id=selector.data_set_external_id)
        elif isinstance(selector, AssetSubtreeSelector):
            return self._aggregator.count(hierarchy=selector.hierarchy)
        return None

    def configurations(self, selector: AssetCentricSelector) -> Iterable[StorageIOConfig]:
        data_set_ids = self._downloaded_data_sets_by_selector[selector]
        if data_set_ids:
            data_set_external_ids = self.client.lookup.data_sets.external_id(list(data_set_ids))
            yield from self._configurations(data_set_external_ids, DataSetsCRUD.create_loader(self.client))

        yield from self._configurations(
            list(self._downloaded_labels_by_selector[selector]), LabelCRUD.create_loader(self.client)
        )

    def _get_classic_filter(self, selector: AssetCentricSelector) -> ClassicFilter:
        if isinstance(selector, DataSetSelector):
            return ClassicFilter.from_asset_subtree_and_data_sets(data_set_id=selector.data_set_external_id)
        elif isinstance(selector, AssetSubtreeSelector):
            return ClassicFilter.from_asset_subtree_and_data_sets(asset_subtree_id=selector.hierarchy)
        else:
            # This selector is for uploads, not for downloading from CDF.
            raise ToolkitNotImplementedError(f"Selector type {type(selector)} not supported for {type(self).__name__}.")

    def _collect_dependencies(
        self,
        resources: Sequence[AssetResponse]
        | Sequence[FileMetadataResponse]
        | Sequence[TimeSeriesResponse]
        | Sequence[EventResponse],
        selector: AssetCentricSelector,
    ) -> None:
        for resource in resources:
            if resource.data_set_id:
                self._downloaded_data_sets_by_selector[selector].add(resource.data_set_id)
            if isinstance(resource, AssetResponse | FileMetadataResponse):
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
        return self.create_internal_identifier(internal_id, self.client.config.project)

    @classmethod
    def create_internal_identifier(cls, internal_id: int, project: str) -> str:
        return f"INTERNAL_ID_project_{project}_{internal_id!s}"

    def _populate_data_set_id_cache(
        self, chunk: Sequence[AssetResponse | FileMetadataResponse | TimeSeriesResponse | EventResponse]
    ) -> None:
        data_set_ids = {item.data_set_id for item in chunk if item.data_set_id is not None}
        self.client.lookup.data_sets.external_id(list(data_set_ids))

    def _populate_security_category_cache(self, chunk: Sequence[FileMetadataResponse | TimeSeriesResponse]) -> None:
        security_category_ids: set[int] = set()
        for item in chunk:
            security_category_ids.update(item.security_categories or [])
        self.client.lookup.security_categories.external_id(list(security_category_ids))

    def _populate_asset_id_cache(self, chunk: Sequence[FileMetadataResponse | EventResponse]) -> None:
        asset_ids: set[int] = set()
        for item in chunk:
            asset_ids.update(item.asset_ids or [])
        self.client.lookup.assets.external_id(list(asset_ids))

    def data_to_row(
        self, data_chunk: Sequence[T_ResourceResponse], selector: AssetCentricSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        rows: list[dict[str, JsonVal]] = []
        for chunk in self.data_to_json_chunk(data_chunk, selector):
            if "metadata" in chunk and isinstance(chunk["metadata"], dict):
                metadata = chunk.pop("metadata")
                # MyPy does understand that metadata is a dict here due to the check above.
                for key, value in metadata.items():  # type: ignore[union-attr]
                    chunk[f"metadata.{key}"] = value
            rows.append(chunk)
        return rows


class UploadableAssetCentricIO(
    Generic[T_ResourceResponse, T_ResourceRequest],
    AssetCentricIO[T_ResourceResponse],
    TableUploadableStorageIO[AssetCentricSelector, T_ResourceResponse, T_ResourceRequest],
    ABC,
):
    def _populate_data_set_external_id_cache(self, chunk: Sequence[dict[str, Any]]) -> None:
        data_set_external_ids: set[str] = set()
        for item in chunk:
            data_set_external_id = item.get("dataSetExternalId")
            if isinstance(data_set_external_id, str):
                data_set_external_ids.add(data_set_external_id)
        self.client.lookup.data_sets.id(list(data_set_external_ids))

    def _populate_asset_external_ids_cache(self, chunk: Sequence[dict[str, Any]]) -> None:
        asset_external_id_set: set[str] = set()
        for item in chunk:
            asset_external_ids = item.get("assetExternalIds")
            if isinstance(asset_external_ids, list):
                for asset_external_id_item in asset_external_ids:
                    if isinstance(asset_external_id_item, str):
                        asset_external_id_set.add(asset_external_id_item)
            asset_external_id = item.get("assetExternalId")
            if isinstance(asset_external_id, str):
                asset_external_id_set.add(asset_external_id)
        self.client.lookup.assets.id(list(asset_external_id_set))

    def _populate_security_category_name_cache(self, chunk: Sequence[dict[str, Any]]) -> None:
        security_category_names: set[str] = set()
        for item in chunk:
            security_category_external_ids_list = item.get("securityCategoryNames")
            if isinstance(security_category_external_ids_list, list):
                for security_category_external_id_item in security_category_external_ids_list:
                    if isinstance(security_category_external_id_item, str):
                        security_category_names.add(security_category_external_id_item)
            security_category_external_id = item.get("securityCategoryNames")
            if isinstance(security_category_external_id, str):
                security_category_names.add(security_category_external_id)
        self.client.lookup.security_categories.id(list(security_category_names))

    def rows_to_data(
        self, rows: list[tuple[str, dict[str, JsonVal]]], selector: AssetCentricSelector | None = None
    ) -> Sequence[UploadItem[T_ResourceRequest]]:
        # We need to populate caches for any external IDs used in the rows before converting to resources.
        # Thus, we override this method instead of row_to_resource, and reuse the json_to_resource method that
        # does the cache population.
        return self.json_chunk_to_data([(source_id, self.row_to_json(row)) for source_id, row in rows])

    @classmethod
    def row_to_json(cls, row: dict[str, JsonVal]) -> dict[str, JsonVal]:
        metadata: dict[str, JsonVal] = {}
        cleaned_row: dict[str, JsonVal] = {}
        for key, value in row.items():
            if key.startswith("metadata."):
                metadata_key = key[len("metadata.") :]
                metadata[metadata_key] = value
            else:
                cleaned_row[key] = value
        if metadata:
            cleaned_row["metadata"] = metadata
        return cleaned_row

    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: AssetCentricSelector | None = None
    ) -> T_ResourceRequest:
        raise NotImplementedError(
            f"This method should not be called. {type(self).__name__} overrides rows_to_data instead."
        )


class AssetIO(UploadableAssetCentricIO[AssetResponse, AssetRequest]):
    KIND = "Assets"
    RESOURCE_TYPE = "asset"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    UPLOAD_ENDPOINT = "/assets"

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._crud = AssetCRUD.create_loader(self.client)

    def as_id(self, item: AssetResponse) -> str:
        return item.external_id if item.external_id is not None else self._create_identifier(item.id)

    def _get_aggregator(self) -> AssetCentricAggregator:
        return AssetAggregator(self.client)

    def get_schema(self, selector: AssetCentricSelector) -> list[SchemaColumn]:
        data_set_ids: list[int] = []
        if isinstance(selector, DataSetSelector):
            data_set_id = self.client.lookup.data_sets.id(selector.data_set_external_id)
            if data_set_id is None:
                raise ToolkitMissingResourceError(
                    f"Data set with external ID {selector.data_set_external_id} not found."
                )
            data_set_ids.append(data_set_id)
        hierarchy: list[int] = []
        if isinstance(selector, AssetSubtreeSelector):
            asset_id = self.client.lookup.assets.id(selector.hierarchy)
            if asset_id is None:
                raise ToolkitMissingResourceError(f"Asset with external ID {selector.hierarchy} not found.")
            hierarchy.append(asset_id)

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
            SchemaColumn(name="childCount", type="integer"),
            SchemaColumn(name="depth", type="integer"),
            SchemaColumn(name="path", type="string", is_array=True),
        ]
        return asset_schema + metadata_schema

    def stream_data(self, selector: AssetCentricSelector, limit: int | None = None) -> Iterable[Page]:
        filter_ = self._get_classic_filter(selector)
        cursor: str | None = None
        total_count = 0
        while True:
            page = self.client.tool.assets.paginate(
                aggregated_properties=True,
                filter=filter_,
                limit=self.CHUNK_SIZE,
                cursor=cursor,
            )
            self._collect_dependencies(page.items, selector)
            yield Page(worker_id="main", items=page.items)
            total_count += len(page.items)
            if page.next_cursor is None or (limit is not None and total_count >= limit):
                break
            cursor = page.next_cursor

    def data_to_json_chunk(
        self, data_chunk: Sequence[AssetResponse], selector: AssetCentricSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        # Ensure data sets are looked up to populate cache.
        # This is to avoid looking up each data set id individually in the .dump_resource call.
        self._populate_data_set_id_cache(data_chunk)
        asset_ids = {
            segment["id"]
            for item in data_chunk
            if isinstance(item.aggregates, AssetAggregateItem)
            for segment in item.aggregates.path
        }
        self.client.lookup.assets.external_id(list(asset_ids))

        return [self._crud.dump_resource(item) for item in data_chunk]

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> AssetRequest:
        return self._crud.load_resource(item_json)

    def retrieve(self, ids: Sequence[int]) -> list[AssetResponse]:
        return self.client.tool.assets.retrieve(InternalId.from_ids(ids))

    @classmethod
    def read_chunks(
        cls, reader: FileReader, selector: AssetCentricSelector
    ) -> Iterable[list[tuple[str, dict[str, JsonVal]]]]:
        """Assets require special handling when reading data to ensure parent assets are created first."""
        current_depth = max_depth = 0
        data_name = "row" if isinstance(reader, TableReader) else "line"
        batch: list[tuple[str, dict[str, JsonVal]]] = []
        # We read the file multiple times, once for each depth level, to ensure parents are created before children.
        while current_depth <= max_depth:
            for line_number, item in reader.read_chunks_with_line_numbers():
                try:
                    depth = int(item["depth"])  # type: ignore[arg-type]
                except (TypeError, ValueError, KeyError):
                    if current_depth == 0:
                        # If depth is not set, we yield it at depth 0
                        batch.append((f"{data_name} {line_number}", item))
                else:
                    if depth == current_depth:
                        batch.append((f"{data_name} {line_number}", item))
                    elif current_depth == 0:
                        max_depth = max(max_depth, depth)
                if len(batch) >= cls.CHUNK_SIZE:
                    yield batch
                    batch = []
            if batch:
                yield batch
                batch = []
            current_depth += 1


class FileMetadataIO(AssetCentricIO[FileMetadataResponse]):
    KIND = "FileMetadata"
    RESOURCE_TYPE = "file"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    UPLOAD_ENDPOINT = "/files"

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._crud = FileMetadataCRUD.create_loader(self.client)

    def as_id(self, item: FileMetadataResponse) -> str:
        return item.external_id if item.external_id is not None else self._create_identifier(item.id)

    def _get_aggregator(self) -> AssetCentricAggregator:
        return FileAggregator(self.client)

    def get_schema(self, selector: AssetCentricSelector) -> list[SchemaColumn]:
        data_set_ids: list[int] = []
        if isinstance(selector, DataSetSelector):
            data_set_id = self.client.lookup.data_sets.id(selector.data_set_external_id)
            if data_set_id is None:
                raise ToolkitMissingResourceError(
                    f"Data set with external ID {selector.data_set_external_id} not found."
                )
            data_set_ids.append(data_set_id)
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

    def stream_data(
        self, selector: AssetCentricSelector, limit: int | None = None
    ) -> Iterable[Page[FileMetadataResponse]]:
        filter_ = self._get_classic_filter(selector)
        cursor: str | None = None
        total_count = 0
        while True:
            page = self.client.tool.filemetadata.paginate(
                filter=filter_,
                limit=self.CHUNK_SIZE,
                cursor=cursor,
            )
            self._collect_dependencies(page.items, selector)
            yield Page(worker_id="main", items=page.items)
            total_count += len(page.items)
            if page.next_cursor is None or (limit is not None and total_count >= limit):
                break
            cursor = page.next_cursor

    def retrieve(self, ids: Sequence[int]) -> list[FileMetadataResponse]:
        return self.client.tool.filemetadata.retrieve(InternalId.from_ids(ids))

    def data_to_json_chunk(
        self, data_chunk: Sequence[FileMetadataResponse], selector: AssetCentricSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        # Ensure data sets/assets/security-categories are looked up to populate cache.
        # This is to avoid looking up each data set id individually in the .dump_resource call
        self._populate_data_set_id_cache(data_chunk)
        self._populate_asset_id_cache(data_chunk)
        self._populate_security_category_cache(data_chunk)

        return [self._crud.dump_resource(item) for item in data_chunk]


class TimeSeriesIO(UploadableAssetCentricIO[TimeSeriesResponse, TimeSeriesRequest]):
    KIND = "TimeSeries"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    UPLOAD_ENDPOINT = "/timeseries"
    RESOURCE_TYPE = "timeseries"

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._crud = TimeSeriesCRUD.create_loader(self.client)

    def as_id(self, item: TimeSeriesResponse) -> str:
        return item.external_id if item.external_id is not None else self._create_identifier(item.id)

    def _get_aggregator(self) -> AssetCentricAggregator:
        return TimeSeriesAggregator(self.client)

    def retrieve(self, ids: Sequence[int]) -> list[TimeSeriesResponse]:
        return self.client.tool.timeseries.retrieve(InternalId.from_ids(ids))

    def stream_data(self, selector: AssetCentricSelector, limit: int | None = None) -> Iterable[Page]:
        filter_ = self._get_classic_filter(selector)
        cursor: str | None = None
        total_count = 0
        while True:
            page = self.client.tool.timeseries.paginate(
                filter=filter_,
                limit=self.CHUNK_SIZE,
                cursor=cursor,
            )
            self._collect_dependencies(page.items, selector)
            yield Page(worker_id="main", items=page.items)
            total_count += len(page.items)
            if page.next_cursor is None or (limit is not None and total_count >= limit):
                break
            cursor = page.next_cursor

    def data_to_json_chunk(
        self, data_chunk: Sequence[TimeSeriesResponse], selector: AssetCentricSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        # Ensure data sets/assets/security categories are looked up to populate cache.
        self._populate_data_set_id_cache(data_chunk)
        self._populate_security_category_cache(data_chunk)
        asset_ids = {item.asset_id for item in data_chunk if item.asset_id is not None}
        self.client.lookup.assets.external_id(list(asset_ids))

        return [self._crud.dump_resource(item) for item in data_chunk]

    def json_chunk_to_data(
        self, data_chunk: list[tuple[str, dict[str, JsonVal]]]
    ) -> Sequence[UploadItem[TimeSeriesRequest]]:
        chunks = [item_json for _, item_json in data_chunk]
        self._populate_asset_external_ids_cache(chunks)
        self._populate_data_set_external_id_cache(chunks)
        self._populate_security_category_name_cache(chunks)
        return super().json_chunk_to_data(data_chunk)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> TimeSeriesRequest:
        return self._crud.load_resource(item_json)

    def get_schema(self, selector: AssetCentricSelector) -> list[SchemaColumn]:
        data_set_ids: list[int] = []
        if isinstance(selector, DataSetSelector):
            data_set_id = self.client.lookup.data_sets.id(selector.data_set_external_id)
            if data_set_id is None:
                raise ToolkitMissingResourceError(
                    f"Data set with external ID {selector.data_set_external_id} not found."
                )
            data_set_ids.append(data_set_id)
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


class EventIO(UploadableAssetCentricIO[EventResponse, EventRequest]):
    KIND = "Events"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    UPLOAD_ENDPOINT = "/events"
    RESOURCE_TYPE = "event"

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._crud = EventCRUD.create_loader(self.client)

    def as_id(self, item: EventResponse) -> str:
        return item.external_id if item.external_id is not None else self._create_identifier(item.id)

    def _get_aggregator(self) -> AssetCentricAggregator:
        return EventAggregator(self.client)

    def get_schema(self, selector: AssetCentricSelector) -> list[SchemaColumn]:
        data_set_ids: list[int] = []
        if isinstance(selector, DataSetSelector):
            data_set_id = self.client.lookup.data_sets.id(selector.data_set_external_id)
            if data_set_id is None:
                raise ToolkitMissingResourceError(
                    f"Data set with external ID {selector.data_set_external_id} not found."
                )
            data_set_ids.append(data_set_id)
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
        filter_ = self._get_classic_filter(selector)
        cursor: str | None = None
        total_count = 0
        while True:
            page = self.client.tool.events.paginate(
                filter=filter_,
                limit=self.CHUNK_SIZE,
                cursor=cursor,
            )
            self._collect_dependencies(page.items, selector)
            yield Page(worker_id="main", items=page.items)
            total_count += len(page.items)
            if page.next_cursor is None or (limit is not None and total_count >= limit):
                break
            cursor = page.next_cursor

    def data_to_json_chunk(
        self, data_chunk: Sequence[EventResponse], selector: AssetCentricSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        # Ensure data sets/assets are looked up to populate cache.
        self._populate_data_set_id_cache(data_chunk)
        self._populate_asset_id_cache(data_chunk)

        return [self._crud.dump_resource(item) for item in data_chunk]

    def json_chunk_to_data(
        self, data_chunk: list[tuple[str, dict[str, JsonVal]]]
    ) -> Sequence[UploadItem[EventRequest]]:
        chunks = [item_json for _, item_json in data_chunk]
        self._populate_asset_external_ids_cache(chunks)
        self._populate_data_set_external_id_cache(chunks)
        return super().json_chunk_to_data(data_chunk)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> EventRequest:
        return self._crud.load_resource(item_json)

    def retrieve(self, ids: Sequence[int]) -> list[EventResponse]:
        return self.client.tool.events.retrieve(InternalId.from_ids(ids))


class HierarchyIO(ConfigurableStorageIO[AssetCentricSelector, AssetCentricResource]):
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
        self._io_by_kind: dict[str, AssetCentricIO] = {
            self._asset_io.KIND: self._asset_io,
            self._file_io.KIND: self._file_io,
            self._timeseries_io.KIND: self._timeseries_io,
            self._event_io.KIND: self._event_io,
        }

    def as_id(self, item: AssetCentricResource) -> str:
        return item.external_id or AssetCentricIO.create_internal_identifier(item.id, self.client.config.project)

    def stream_data(
        self, selector: AssetCentricSelector, limit: int | None = None
    ) -> Iterable[Page[AssetCentricResource]]:
        yield from self.get_resource_io(selector.kind).stream_data(selector, limit)

    def count(self, selector: AssetCentricSelector) -> int | None:
        return self.get_resource_io(selector.kind).count(selector)

    def data_to_json_chunk(
        self, data_chunk: Sequence[AssetCentricResource], selector: AssetCentricSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        if selector is None:
            raise ValueError(f"Selector must be provided to convert data to JSON chunk for {type(self).__name__}.)")
        return self.get_resource_io(selector.kind).data_to_json_chunk(data_chunk, selector)

    def configurations(self, selector: AssetCentricSelector) -> Iterable[StorageIOConfig]:
        yield from self.get_resource_io(selector.kind).configurations(selector)

    def get_resource_io(self, kind: str) -> AssetCentricIO:
        return self._io_by_kind[kind]

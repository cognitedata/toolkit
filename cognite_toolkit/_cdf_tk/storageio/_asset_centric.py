from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Generic

from cognite.client.data_classes import (
    Asset,
    AssetList,
    AssetWrite,
    AssetWriteList,
    DataSetWriteList,
    FileMetadata,
    FileMetadataList,
    FileMetadataWrite,
    FileMetadataWriteList,
    Label,
    LabelDefinition,
)
from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
)
from cognite.client.data_classes.labels import LabelDefinitionWriteList
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.cruds import AssetCRUD, DataSetsCRUD, FileMetadataCRUD, LabelCRUD, ResourceCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.utils.aggregators import AssetAggregator, AssetCentricAggregator, FileAggregator
from cognite_toolkit._cdf_tk.utils.cdf import metadata_key_counts
from cognite_toolkit._cdf_tk.utils.file import find_files_with_suffix_and_prefix
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID, JsonVal, T_WritableCogniteResourceList

from ._base import StorageIOConfig, TableStorageIO
from ._selectors import AssetCentricFileSelector, AssetCentricSelector, AssetSubtreeSelector, DataSetSelector


class BaseAssetCentricIO(
    Generic[T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList],
    TableStorageIO[int, AssetCentricSelector, T_CogniteResourceList, T_WritableCogniteResourceList],
    ABC,
):
    CHUNK_SIZE = 1000

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

    def data_to_json_chunk(self, data_chunk: T_WritableCogniteResourceList) -> list[dict[str, JsonVal]]:
        return [self._loader.dump_resource(item) for item in data_chunk]

    def configurations(self, selector: AssetCentricSelector) -> Iterable[StorageIOConfig]:
        data_set_ids = self._downloaded_data_sets_by_selector[selector]
        if data_set_ids:
            data_set_external_ids = self.client.lookup.data_sets.external_id(list(data_set_ids))
            yield from self._configurations(data_set_external_ids, DataSetsCRUD.create_loader(self.client))

        yield from self._configurations(
            list(self._downloaded_labels_by_selector[selector]), LabelCRUD.create_loader(self.client)
        )

    def _collect_dependencies(self, resources: AssetList | FileMetadataList, selector: AssetCentricSelector) -> None:
        for resource in resources:
            if resource.data_set_id:
                self._downloaded_data_sets_by_selector[selector].add(resource.data_set_id)
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

    def load_selector(self, datafile: Path) -> AssetCentricSelector:
        return AssetCentricFileSelector(datafile=datafile)

    def ensure_configurations(self, selector: AssetCentricSelector, console: Console | None = None) -> None:
        """Ensures that all data sets and labels referenced by the asset selection exist in CDF."""
        if not isinstance(selector, AssetCentricFileSelector):
            return None
        datafile = selector.datafile
        filepaths = find_files_with_suffix_and_prefix(
            datafile.parent.parent / DataSetsCRUD.folder_name, datafile.name, suffix=f".{DataSetsCRUD.kind}.yaml"
        )
        self._create_if_not_exists(filepaths, DataSetsCRUD.create_loader(self.client), console)

        filepaths = find_files_with_suffix_and_prefix(
            datafile.parent.parent / LabelCRUD.folder_name, datafile.name, suffix=f".{LabelCRUD.kind}.yaml"
        )
        self._create_if_not_exists(filepaths, LabelCRUD.create_loader(self.client), console)
        return None

    @classmethod
    def _create_if_not_exists(
        cls,
        filepaths: list[Path],
        loader: DataSetsCRUD | LabelCRUD,
        console: Console | None = None,
    ) -> None:
        items: LabelDefinitionWriteList | DataSetWriteList = loader.list_write_cls([])
        for filepath in filepaths:
            if not filepath.exists():
                continue
            for loaded in loader.load_resource_file(filepath):
                items.append(loader.load_resource(loaded))
        # MyPy fails to understand that existing, existing_ids and missing will be consistent for given loader.
        existing = loader.retrieve(loader.get_ids(items))
        existing_ids = set(loader.get_ids(existing))
        if missing := [item for item in items if loader.get_id(item) not in existing_ids]:  # type: ignore[arg-type]
            loader.create(loader.list_write_cls(missing))  # type: ignore[arg-type]
            if console:
                console.print(
                    f"Created {loader.kind} for {len(missing)} items: {', '.join(str(item) for item in loader.get_ids(missing))}"  # type: ignore[arg-type]
                )


class AssetIO(BaseAssetCentricIO[str, AssetWrite, Asset, AssetWriteList, AssetList]):
    FOLDER_NAME = "classic"
    KIND = "Assets"
    DISPLAY_NAME = "Assets"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})

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
        asset_subtree_external_ids: list[str] | None = None
        data_set_external_ids: list[str] | None = None
        if isinstance(selector, DataSetSelector):
            data_set_external_ids = [selector.data_set_external_id]
        elif isinstance(selector, AssetSubtreeSelector):
            asset_subtree_external_ids = [selector.hierarchy]
        else:
            # This selector is for uploads, not for downloading from CDF.
            raise ToolkitNotImplementedError(f"Selector type {type(selector)} not supported for AssetIO.")
        for asset_list in self.client.assets(
            chunk_size=self.CHUNK_SIZE,
            limit=limit,
            asset_subtree_external_ids=asset_subtree_external_ids,
            data_set_external_ids=data_set_external_ids,
        ):
            self._collect_dependencies(asset_list, selector)
            yield asset_list

    def upload_items(self, data_chunk: AssetWriteList, selector: AssetCentricSelector) -> None:
        if not data_chunk:
            return
        self.client.assets.upsert(data_chunk, mode="patch")

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> AssetWriteList:
        return AssetWriteList([self._loader.load_resource(item) for item in data_chunk])

    def retrieve(self, ids: Sequence[int]) -> AssetList:
        return self.client.assets.retrieve_multiple(ids)


class FileMetadataIO(BaseAssetCentricIO[str, FileMetadataWrite, FileMetadata, FileMetadataWriteList, FileMetadataList]):
    FOLDER_NAME = FileMetadataCRUD.folder_name
    KIND = "FileMetadata"
    DISPLAY_NAME = "file metadata"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})

    def as_id(self, item: dict[str, JsonVal] | object) -> int:
        if isinstance(item, FileMetadata | FileMetadataWrite) and item.id is not None:  # type: ignore[union-attr]
            return item.id  # type: ignore[union-attr]
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
        asset_subtree_external_ids: list[str] | None = None
        data_set_external_ids: list[str] | None = None
        if isinstance(selector, DataSetSelector):
            data_set_external_ids = [selector.data_set_external_id]
        elif isinstance(selector, AssetSubtreeSelector):
            asset_subtree_external_ids = [selector.hierarchy]
        else:
            # This selector is for uploads, not for downloading from CDF.
            raise ToolkitNotImplementedError(f"Selector type {type(selector)} not supported for FileMetadataIO.")
        for file_list in self.client.files(
            chunk_size=self.CHUNK_SIZE,
            limit=limit,
            asset_subtree_external_ids=asset_subtree_external_ids,
            data_set_external_ids=data_set_external_ids,
        ):
            self._collect_dependencies(file_list, selector)
            yield file_list

    def retrieve(self, ids: Sequence[int]) -> FileMetadataList:
        return self.client.files.retrieve_multiple(ids)

    def upload_items(self, data_chunk: FileMetadataWriteList, selector: AssetCentricSelector) -> None:
        if not data_chunk:
            return
        self._loader.create(data_chunk)

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> FileMetadataWriteList:
        return FileMetadataWriteList([self._loader.load_resource(item) for item in data_chunk])

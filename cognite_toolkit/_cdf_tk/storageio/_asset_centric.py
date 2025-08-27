from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

from cognite.client.data_classes import AssetList, AssetWriteList, Label, LabelDefinition
from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
)
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.loaders import AssetLoader, DataSetsLoader, LabelLoader, ResourceLoader
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils.aggregators import AssetAggregator
from cognite_toolkit._cdf_tk.utils.cdf import metadata_key_counts
from cognite_toolkit._cdf_tk.utils.file import find_files_with_suffix_and_prefix
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import StorageIOConfig, TableStorageIO
from ._selectors import AssetCentricData


class AssetIO(TableStorageIO[AssetCentricData, AssetWriteList, AssetList]):
    folder_name = "classic"
    kind = "Assets"
    display_name = "Assets"
    supported_download_formats = frozenset({".parquet", ".csv", ".ndjson"})
    supported_compressions = frozenset({".gz"})
    supported_read_formats = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    chunk_size = 1000

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._loader = AssetLoader.create_loader(client)
        self._downloaded_data_sets_by_selector: dict[AssetCentricData, set[int]] = defaultdict(set)
        self._downloaded_labels_by_selector: dict[AssetCentricData, set[str]] = defaultdict(set)

    def get_schema(self, selector: AssetCentricData) -> list[SchemaColumn]:
        data_set_ids: list[int] = []
        if selector.data_set_external_id:
            data_set_ids.append(self.client.lookup.data_sets.id(selector.data_set_external_id))
        hierarchy: list[int] = []
        if selector.hierarchy:
            hierarchy.append(self.client.lookup.assets.id(selector.hierarchy))

        metadata_keys = metadata_key_counts(
            self.client, "assets", data_sets=data_set_ids or None, hierarchies=hierarchy or None
        )
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

    def count(self, selector: AssetCentricData) -> int:
        return AssetAggregator(self.client).count(
            hierarchy=selector.hierarchy, data_set_external_id=selector.data_set_external_id
        )

    def download_iterable(self, selector: AssetCentricData, limit: int | None = None) -> Iterable[AssetList]:
        for asset_list in self.client.assets(chunk_size=self.chunk_size, limit=limit, **selector.as_filter()):
            for asset in asset_list:
                if asset.data_set_id:
                    self._downloaded_data_sets_by_selector[selector].add(asset.data_set_id)
                for label in asset.labels or []:
                    if isinstance(label, str):
                        self._downloaded_labels_by_selector[selector].add(label)
                    elif isinstance(label, Label | LabelDefinition) and label.external_id:
                        self._downloaded_labels_by_selector[selector].add(label.external_id)
                    elif isinstance(label, dict) and "externalId" in label:
                        self._downloaded_labels_by_selector[selector].add(label["externalId"])

            yield asset_list

    def upload_items(self, data_chunk: AssetWriteList, selector: AssetCentricData) -> None:
        if not data_chunk:
            return
        self.client.assets.create(data_chunk)

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> AssetWriteList:
        return AssetWriteList([self._loader.load_resource(item) for item in data_chunk])

    def data_to_json_chunk(self, data_chunk: AssetList) -> list[dict[str, JsonVal]]:
        return [self._loader.dump_resource(item) for item in data_chunk]

    def configurations(self, selector: AssetCentricData) -> Iterable[StorageIOConfig]:
        data_set_ids = self._downloaded_data_sets_by_selector[selector]
        if data_set_ids:
            data_set_external_ids = self.client.lookup.data_sets.external_id(list(data_set_ids))
            yield from self._configurations(data_set_external_ids, DataSetsLoader.create_loader(self.client))

        yield from self._configurations(
            list(self._downloaded_labels_by_selector[selector]), LabelLoader.create_loader(self.client)
        )

    @classmethod
    def _configurations(
        cls,
        ids: list[T_ID],
        loader: ResourceLoader[
            T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
        ],
    ) -> Iterable[StorageIOConfig]:
        if not ids:
            return
        items = loader.retrieve(list(ids))
        yield StorageIOConfig(
            kind=loader.kind,
            folder_name=loader.folder_name,
            value=[loader.dump_resource(item) for item in items],
        )

    def load_selector(self, datafile: Path) -> AssetCentricData:
        return AssetCentricData(datafile=datafile)

    def ensure_configurations(self, selector: AssetCentricData, console: Console | None = None) -> None:
        """Ensures that all data sets and labels referenced by the asset selection exist in CDF."""
        datafile = selector.datafile
        if not datafile:
            return None
        filepaths = find_files_with_suffix_and_prefix(
            datafile.parent.parent / DataSetsLoader.folder_name, datafile.name, suffix=f".{DataSetsLoader.kind}.yaml"
        )
        self._create_if_not_exists(filepaths, DataSetsLoader.create_loader(self.client), console)

        filepaths = find_files_with_suffix_and_prefix(
            datafile.parent.parent / LabelLoader.folder_name, datafile.name, suffix=f".{LabelLoader.kind}.yaml"
        )
        self._create_if_not_exists(filepaths, LabelLoader.create_loader(self.client), console)
        return None

    @classmethod
    def _create_if_not_exists(
        cls,
        filepaths: list[Path],
        loader: ResourceLoader[
            T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
        ],
        console: Console | None = None,
    ) -> None:
        items: T_CogniteResourceList = loader.list_write_cls([])
        for filepath in filepaths:
            if not filepath.exists():
                continue
            for loaded in loader.load_resource_file(filepath):
                items.append(loader.load_resource(loaded))
        existing = loader.retrieve(loader.get_ids(items))
        existing_ids = set(loader.get_ids(existing))
        if missing := [item for item in items if loader.get_id(item) not in existing_ids]:
            loader.create(loader.list_write_cls(missing))
            if console:
                console.print(
                    f"Created {loader.kind} for {len(missing)} items: {', '.join(str(item) for item in loader.get_ids(missing))}"
                )

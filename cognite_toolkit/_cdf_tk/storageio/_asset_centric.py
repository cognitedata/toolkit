from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

from cognite.client.data_classes import AssetList, AssetWriteList
from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
)
from rich import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.loaders import AssetLoader, DataSetsLoader, LabelLoader, ResourceLoader
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils.aggregators import AssetAggregator
from cognite_toolkit._cdf_tk.utils.cdf import metadata_key_counts
from cognite_toolkit._cdf_tk.utils.table_writers import SchemaColumn
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
        self._loaded_data_sets_by_selector: dict[AssetCentricData, set[int]] = defaultdict(set)
        self._loaded_labels_by_selector: dict[AssetCentricData, set[str]] = defaultdict(set)

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
        if metadata_schema:
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
        yield from self.client.assets(chunk_size=self.chunk_size, limit=limit, **selector.as_filter())

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
        raise NotImplementedError()

    def ensure_configurations(self, selector: AssetCentricData, console: Console | None = None) -> None:
        raise NotImplementedError()

from collections.abc import Iterable

from cognite.client.data_classes import AssetList, AssetWriteList

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.loaders import AssetLoader
from cognite_toolkit._cdf_tk.utils.aggregators import AssetAggregator
from cognite_toolkit._cdf_tk.utils.table_writers import SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import TableStorageIO
from ._identifiers import AssetCentricData


class AssetIO(TableStorageIO[AssetCentricData, AssetWriteList, AssetList]):
    folder_name = "classic"
    kind = "Assets"
    display_name = "Assets"
    supported_download_formats = frozenset({".parquet", ".csv", ".ndjson"})
    supported_compressions = frozenset({".gz"})
    supported_read_formats = frozenset({".parquet", ".csv", ".ndjson"})
    chunk_size = 1000

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._loader = AssetLoader.create_loader(client)

    def get_schema(self, identifier: AssetCentricData) -> list[SchemaColumn]:
        return [
            SchemaColumn(name="externalId", type="string"),
            SchemaColumn(name="name", type="string"),
            SchemaColumn(name="parentExternalId", type="string"),
            SchemaColumn(name="description", type="string"),
            SchemaColumn(name="dataSetExternalId", type="string"),
            SchemaColumn(name="source", type="string"),
            SchemaColumn(name="labels", type="string", is_array=True),
            SchemaColumn(name="geoLocation", type="json"),
        ]

    def count(self, identifier: AssetCentricData) -> int:
        return AssetAggregator(self.client).count(
            hierarchy=identifier.hierarchy, data_set_external_id=identifier.data_set_id
        )

    def download_iterable(self, identifier: AssetCentricData, limit: int | None = None) -> Iterable[AssetList]:
        yield from self.client.assets(chunk_size=self.chunk_size, limit=limit, **identifier.as_filter())

    def upload_items(self, data_chunk: AssetWriteList, identifier: AssetCentricData) -> None:
        if not data_chunk:
            return
        self.client.assets.create(data_chunk)

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> AssetWriteList:
        output = AssetWriteList([])
        for item in data_chunk:
            output.append(self._loader.load_resource(item))
        return output

    def data_to_json_chunk(self, data_chunk: AssetList) -> list[dict[str, JsonVal]]:
        output: list[dict[str, JsonVal]] = []
        for item in data_chunk:
            output.append(self._loader.dump_resource(item))
        return output

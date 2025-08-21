from collections.abc import Iterable

from cognite.client.data_classes import RowList, RowWrite, RowWriteList

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawTable
from cognite_toolkit._cdf_tk.loaders import RawTableLoader
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import StorageIO, StorageIOConfig


class RawIO(StorageIO[RawTable, RowWriteList, RowList]):
    folder_name = "raw"
    kind = "RawRows"
    display_name = "Raw Rows"
    supported_download_formats = frozenset({".yaml", ".ndjson"})
    supported_compressions = frozenset({".gz"})
    supported_read_formats = frozenset({".parquet", ".csv", ".ndjson", ".yaml"})
    chunk_size = 10_000

    def count(self, identifier: RawTable) -> int | None:
        # Raw tables do not support aggregation queries, so we do not know the count
        # up front.
        return None

    def download_iterable(self, identifier: RawTable, limit: int | None = None) -> Iterable[RowList]:
        yield from self.client.raw.rows(
            db_name=identifier.db_name,
            table_name=identifier.table_name,
            limit=limit,
            partitions=8,
            chunk_size=self.chunk_size,
        )

    def upload_items(self, data_chunk: RowWriteList, identifier: RawTable) -> None:
        self.client.raw.rows.insert(db_name=identifier.db_name, table_name=identifier.table_name, row=data_chunk)

    def data_to_json_chunk(self, data_chunk: RowList) -> list[dict[str, JsonVal]]:
        return [row.as_write().dump() for row in data_chunk]

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> RowWriteList:
        return RowWriteList([RowWrite._load(row) for row in data_chunk])

    def configurations(self, identifier: RawTable) -> Iterable[StorageIOConfig]:
        yield StorageIOConfig(kind=RawTableLoader.kind, folder_name=RawTableLoader.folder_name, value=identifier.dump())

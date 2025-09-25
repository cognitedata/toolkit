from collections.abc import Iterable
from pathlib import Path

from cognite.client.data_classes import RowList, RowWrite, RowWriteList
from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawDatabase, RawDatabaseList, RawTable, RawTableList
from cognite_toolkit._cdf_tk.cruds import RawDatabaseCRUD, RawTableCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils.file import find_adjacent_files, read_yaml_file
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import StorageIO, StorageIOConfig


class RawIO(StorageIO[RawTable, RawTable, RowWriteList, RowList]):
    FOLDER_NAME = "raw"
    KIND = "RawRows"
    DISPLAY_NAME = "Raw Rows"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".yaml", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml"})
    CHUNK_SIZE = 10_000

    def as_id(self, item: dict[str, JsonVal] | object) -> RawTable:
        raise ValueError("You cannot extract an ID from a Raw Table row. Use a RawTable selector instead.")

    def count(self, selector: RawTable) -> int | None:
        # Raw tables do not support aggregation queries, so we do not know the count
        # up front.
        return None

    def stream_data(self, selector: RawTable, limit: int | None = None) -> Iterable[RowList]:
        yield from self.client.raw.rows(
            db_name=selector.db_name,
            table_name=selector.table_name,
            limit=limit,
            # We cannot use partitions here as it is not thread safe. This spawn multiple threads
            # that are not shut down until all data is downloaded. We need to be able to abort.
            partitions=None,
            chunk_size=self.CHUNK_SIZE,
        )

    def upload_items(self, data_chunk: RowWriteList, selector: RawTable) -> None:
        self.client.raw.rows.insert(db_name=selector.db_name, table_name=selector.table_name, row=data_chunk)

    def data_to_json_chunk(self, data_chunk: RowList) -> list[dict[str, JsonVal]]:
        return [row.as_write().dump() for row in data_chunk]

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> RowWriteList:
        return RowWriteList([RowWrite._load(row) for row in data_chunk])

    def configurations(self, selector: RawTable) -> Iterable[StorageIOConfig]:
        yield StorageIOConfig(kind=RawTableCRUD.kind, folder_name=RawTableCRUD.folder_name, value=selector.dump())

    def load_selector(self, datafile: Path) -> RawTable:
        config_files = find_adjacent_files(datafile, suffix=f".{RawTableCRUD.kind}.yaml")
        if not config_files:
            raise ToolkitValueError(f"No configuration file found for {datafile.as_posix()!r}")
        if len(config_files) > 1:
            raise ToolkitValueError(f"Multiple configuration files found for {datafile.as_posix()!r}: {config_files}")
        config_file = config_files[0]
        loader = RawTableCRUD.create_loader(self.client)
        return loader.load_resource(read_yaml_file(config_file, expected_output="dict"))

    def ensure_configurations(self, selector: RawTable, console: Console | None = None) -> None:
        """Ensure that the Raw table exists in CDF."""
        db_loader = RawDatabaseCRUD.create_loader(self.client, console=console)
        db = RawDatabase(db_name=selector.db_name)
        if not db_loader.retrieve([db]):
            db_loader.create(RawDatabaseList([db]))
            if console:
                console.print(f"Created raw database: [bold]{db.db_name}[/bold]")

        table_loader = RawTableCRUD.create_loader(self.client, console=console)
        if not table_loader.retrieve([selector]):
            table_loader.create(RawTableList([selector]))
            if console:
                console.print(f"Created raw table: [bold]{selector.db_name}.{selector.table_name}[/bold]")

from collections.abc import Iterable, Sequence
from itertools import chain
from uuid import uuid4

from cognite.client.data_classes import Row, RowWrite

from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsRequest, ItemsResultList
from cognite_toolkit._cdf_tk.cruds import RawDatabaseCRUD, RawTableCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import (
    ConfigurableStorageIO,
    Page,
    StorageIOConfig,
    TableUploadableStorageIO,
    UploadItem,
)
from .selectors import RawTableSelector


class RawIO(
    ConfigurableStorageIO[RawTableSelector, Row],
    TableUploadableStorageIO[RawTableSelector, Row, RowWrite],
):
    KIND = "RawRows"
    DISPLAY_NAME = "Raw Rows"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".yaml", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml"})
    CHUNK_SIZE = 10_000
    UPLOAD_ENDPOINT = "/raw/dbs/{dbName}/tables/{tableName}/rows"
    BASE_SELECTOR = RawTableSelector

    def as_id(self, item: Row) -> str:
        return str(item.key)

    def count(self, selector: RawTableSelector) -> int | None:
        # Raw tables do not support aggregation queries, so we do not know the count
        # up front.
        return None

    def stream_data(self, selector: RawTableSelector, limit: int | None = None) -> Iterable[Page]:
        for chunk in self.client.raw.rows(
            db_name=selector.table.db_name,
            table_name=selector.table.table_name,
            limit=limit,
            # We cannot use partitions here as it is not thread safe. This spawn multiple threads
            # that are not shut down until all data is downloaded. We need to be able to abort.
            partitions=None,
            chunk_size=self.CHUNK_SIZE,
        ):
            yield Page(worker_id="main", items=chunk)

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[RowWrite]],
        http_client: HTTPClient,
        selector: RawTableSelector | None = None,
    ) -> ItemsResultList:
        if selector is None:
            raise ToolkitValueError("Selector must be provided for RawIO upload_items")
        url = self.UPLOAD_ENDPOINT.format(dbName=selector.table.db_name, tableName=selector.table.table_name)
        config = http_client.config
        return http_client.request_items_retries(
            message=ItemsRequest(
                endpoint_url=config.create_api_url(url),
                method="POST",
                items=data_chunk,
            )
        )

    def data_to_json_chunk(
        self, data_chunk: Sequence[Row], selector: RawTableSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        return [row.as_write().dump() for row in data_chunk]

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> RowWrite:
        return RowWrite._load(item_json)

    def configurations(self, selector: RawTableSelector) -> Iterable[StorageIOConfig]:
        yield StorageIOConfig(
            kind=RawDatabaseCRUD.kind,
            folder_name=RawDatabaseCRUD.folder_name,
            value={"dbName": selector.table.db_name},
            filename=sanitize_filename(selector.table.db_name),
        )
        yield StorageIOConfig(
            kind=RawTableCRUD.kind, folder_name=RawTableCRUD.folder_name, value=selector.table.model_dump(by_alias=True)
        )

    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: RawTableSelector | None = None
    ) -> RowWrite:
        key = str(uuid4())
        if selector is not None and selector.key is not None and selector.key in row:
            key = str(row.pop(selector.key))
        return RowWrite(key=key, columns=row)

    @classmethod
    def read_chunks(
        cls, reader: MultiFileReader, selector: RawTableSelector
    ) -> Iterable[list[tuple[str, dict[str, JsonVal]]]]:
        if not reader.is_table or selector.key is None:
            yield from super().read_chunks(reader, selector)
            return
        data_name = "row" if reader.is_table else "line"
        # Validate that the key exists in all files
        for input_file in sorted(reader.input_files, key=reader._part_no):
            iterable = reader.reader_class(input_file).read_chunks()
            try:
                first = next(iterable)
            except StopIteration:
                continue
            if selector.key not in first:
                raise ToolkitValueError(
                    f"Column '{selector.key}' not found in file {input_file.as_posix()!r}. Please ensure the specified column exists."
                )
            full_iterator = chain([first], iterable)
            line_numbered_iterator = ((f"{data_name} {i}", row) for i, row in enumerate(full_iterator, start=1))
            yield from chunker(line_numbered_iterator, cls.CHUNK_SIZE)

from collections.abc import Iterable, Sequence

from cognite.client.data_classes import Row, RowList, RowWrite, RowWriteList

from cognite_toolkit._cdf_tk.cruds import RawDatabaseCRUD, RawTableCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage, ItemsRequest
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import ConfigurableStorageIO, StorageIOConfig
from .selectors import RawTableSelector


class RawIO(ConfigurableStorageIO[str, RawTableSelector, RowWriteList, RowList]):
    KIND = "RawRows"
    DISPLAY_NAME = "Raw Rows"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".yaml", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml"})
    CHUNK_SIZE = 10_000
    UPLOAD_ENDPOINT = "/raw/dbs/{dbName}/tables/{tableName}/rows"
    BASE_SELECTOR = RawTableSelector

    def as_id(self, item: dict[str, JsonVal] | object) -> str:
        if isinstance(item, RowWrite | Row) and item.key is not None:
            return item.key
        elif isinstance(item, dict) and "key" in item and isinstance(item["key"], str):
            return item["key"]
        raise TypeError(f"Cannot extract ID from item of type {type(item).__name__!r}")

    def count(self, selector: RawTableSelector) -> int | None:
        # Raw tables do not support aggregation queries, so we do not know the count
        # up front.
        return None

    def stream_data(self, selector: RawTableSelector, limit: int | None = None) -> Iterable[RowList]:
        yield from self.client.raw.rows(
            db_name=selector.table.db_name,
            table_name=selector.table.table_name,
            limit=limit,
            # We cannot use partitions here as it is not thread safe. This spawn multiple threads
            # that are not shut down until all data is downloaded. We need to be able to abort.
            partitions=None,
            chunk_size=self.CHUNK_SIZE,
        )

    def upload_items(
        self, data_chunk: RowWriteList, http_client: HTTPClient, selector: RawTableSelector | None = None
    ) -> Sequence[HTTPMessage]:
        if selector is None:
            raise ToolkitValueError("Selector must be provided for RawIO upload_items")
        url = self.UPLOAD_ENDPOINT.format(dbName=selector.table.db_name, tableName=selector.table.table_name)
        config = http_client.config
        return http_client.request_with_retries(
            message=ItemsRequest(
                endpoint_url=config.create_api_url(url),
                method="POST",
                # The dump method from the PySDK always returns JsonVal, but mypy cannot infer that
                items=data_chunk.dump(camel_case=True),  # type: ignore[arg-type]
                as_id=self.as_id,
            )
        )

    def data_to_json_chunk(self, data_chunk: RowList, selector: RawTableSelector) -> list[dict[str, JsonVal]]:
        return [row.as_write().dump() for row in data_chunk]

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> RowWriteList:
        return RowWriteList([RowWrite._load(row) for row in data_chunk])

    def configurations(self, selector: RawTableSelector) -> Iterable[StorageIOConfig]:
        yield StorageIOConfig(
            kind=RawDatabaseCRUD.kind,
            folder_name=RawDatabaseCRUD.folder_name,
            value={"db_name": selector.table.db_name},
            filename=sanitize_filename(selector.table.db_name),
        )
        yield StorageIOConfig(
            kind=RawTableCRUD.kind, folder_name=RawTableCRUD.folder_name, value=selector.table.model_dump(by_alias=True)
        )

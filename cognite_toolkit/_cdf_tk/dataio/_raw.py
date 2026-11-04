from collections.abc import Iterable
from dataclasses import dataclass
from itertools import chain
from typing import Literal
from uuid import uuid4

from cognite.client.data_classes import Row, RowWrite

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.http_client._item_classes import ItemsRequest, ItemsResultList
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.resource_ios import RawDatabaseIO, RawTableIO
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.dtype_conversion import (
    convert_str_to_data_type,
    infer_data_type_from_json_value,
    merge_data_types,
)
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader, SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import DataType, JsonVal

from ._base import (
    Bookmark,
    ConfigurableDataIO,
    DataItem,
    Page,
    StorageIOConfig,
    TableDataIO,
    TableUploadableDataIO,
)
from .selectors import RawTableSelector

_KEY_COLUMN = "key"
_UPDATED_COLUMN = "lastUpdatedTime"


@dataclass
class _ObservedType:
    """Column type seen while reading RAW rows.

    Empty lists do not decide the element type. A later non-empty list can still
    specialize the column to an array. Conflicting types fall back to JSON.
    """

    data_type: DataType | None = None
    is_array: bool = False
    only_empty_lists: bool = False

    def observe(self, value: JsonVal) -> None:
        if value is None:
            return
        if isinstance(value, list) and not value:
            if self.data_type is None:
                self.only_empty_lists = True
                self.is_array = True
            return

        data_type, is_array = infer_data_type_from_json_value(value)
        if self.data_type is None:
            if self.only_empty_lists and not is_array:
                self.data_type = "json"
                self.is_array = False
                self.only_empty_lists = False
                return
            self.data_type = data_type
            self.is_array = is_array
            self.only_empty_lists = False
            return

        self.data_type, self.is_array = merge_data_types(self.data_type, self.is_array, data_type, is_array)
        self.only_empty_lists = False

    def as_column(self, name: str) -> SchemaColumn:
        if self.only_empty_lists or self.data_type == "json":
            return SchemaColumn(name=name, type="json")
        if self.data_type is None:
            return SchemaColumn(name=name, type="string")
        return SchemaColumn(name=name, type=self.data_type, is_array=self.is_array)


def _as_float(value: JsonVal) -> JsonVal:
    converted = convert_str_to_data_type(value, "float")
    if converted is None or isinstance(converted, float):
        return converted
    return value


class RawIO(
    ConfigurableDataIO[RawTableSelector, Row],
    TableDataIO[RawTableSelector, Row],
    TableUploadableDataIO[RawTableSelector, Row, RowWrite],
):
    KIND = "RawRows"
    DISPLAY_NAME = "Raw Rows"
    CHUNK_SIZE = 10_000
    UPLOAD_ENDPOINT = "/raw/dbs/{dbName}/tables/{tableName}/rows"
    BASE_SELECTOR = RawTableSelector

    def __init__(self, client: ToolkitClient, api_format: Literal["request", "response"] = "request") -> None:
        super().__init__(client, api_format=api_format)
        self._columns_by_selector: dict[RawTableSelector, dict[str, _ObservedType]] = {}
        self._schema_by_column: dict[str, SchemaColumn] = {}

    def count(self, selector: RawTableSelector) -> int | None:
        # Raw tables do not support aggregation queries, so we do not know the count
        # up front.
        return None

    def stream_data(
        self,
        selector: RawTableSelector,
        limit: int | None = None,
        bookmark: Bookmark | None = None,
    ) -> Iterable[Page]:
        for page in self.client.raw.tables.rows.iterate(
            db_name=selector.table.db_name,
            table_name=selector.table.table_name,
            limit=limit,
        ):
            chunk = [Row(key=item.key, columns=item.columns, last_updated_time=item.last_updated_time) for item in page]
            if not chunk:
                continue
            yield self.emit_registered_page(
                Page(
                    worker_id="main",
                    items=[DataItem(tracking_id=str(item.key), item=item) for item in chunk],
                )
            )

    def upload_items(
        self,
        data_chunk: Page[RowWrite],
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
                items=data_chunk.items,
            )
        )

    def data_to_json_chunk(
        self, data_chunk: Page[Row], selector: RawTableSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        if self.api_format == "response":
            result = [DataItem(tracking_id=item.tracking_id, item=item.item.dump()) for item in data_chunk.items]
        else:
            result = [
                DataItem(tracking_id=item.tracking_id, item=item.item.as_write().dump()) for item in data_chunk.items
            ]
        page = data_chunk.create_from(result)
        if selector is not None and selector in self._columns_by_selector:
            for item in page.items:
                self._observe_record(selector, item.item)
        return page

    def get_schema(self, selector: RawTableSelector) -> list[SchemaColumn] | None:
        """Return the table schema for a RAW table.

        The first call returns None so the download writes JSON first. Column names and
        types are collected while the rows are read, and the next call returns the inferred schema.
        """
        if selector not in self._columns_by_selector:
            self._columns_by_selector[selector] = {}
            return None

        schema = [SchemaColumn(name=_KEY_COLUMN, type="string")]
        if self.api_format == "response":
            schema.append(SchemaColumn(name=_UPDATED_COLUMN, type="integer"))
        for name in sorted(self._columns_by_selector[selector]):
            schema.append(self._columns_by_selector[selector][name].as_column(name))
        self._schema_by_column = {column.name: column for column in schema}
        return schema

    def json_to_row(
        self, item_json: dict[str, JsonVal], selector: RawTableSelector | None = None
    ) -> dict[str, JsonVal]:
        row: dict[str, JsonVal] = {}
        columns = item_json.get("columns")
        if isinstance(columns, dict):
            for name, value in columns.items():
                row[self._data_column_name(str(name))] = value
        if _KEY_COLUMN in item_json:
            row[_KEY_COLUMN] = item_json[_KEY_COLUMN]
        if self.api_format == "response" and _UPDATED_COLUMN in item_json:
            row[_UPDATED_COLUMN] = item_json[_UPDATED_COLUMN]
        return self._coerce_row(row)

    def _data_column_name(self, name: str) -> str:
        if name in self._reserved_column_names():
            return f"columns.{name}"
        return name

    def _reserved_column_names(self) -> set[str]:
        reserved = {_KEY_COLUMN}
        if self.api_format == "response":
            reserved.add(_UPDATED_COLUMN)
        return reserved

    def _observe_record(self, selector: RawTableSelector, item: dict[str, JsonVal]) -> None:
        observed = self._columns_by_selector[selector]
        columns = item.get("columns")
        if not isinstance(columns, dict):
            return
        for name, value in columns.items():
            output_name = self._data_column_name(str(name))
            column = observed.get(output_name)
            if column is None:
                column = _ObservedType()
                observed[output_name] = column
            column.observe(value)

    def _coerce_row(self, row: dict[str, JsonVal]) -> dict[str, JsonVal]:
        if not self._schema_by_column:
            return row
        coerced: dict[str, JsonVal] = {}
        for key, value in row.items():
            column = self._schema_by_column.get(key)
            if column is None or column.type != "float" or value is None:
                coerced[key] = value
                continue
            if column.is_array and isinstance(value, list):
                coerced[key] = [_as_float(item) for item in value]
                continue
            coerced[key] = _as_float(value)
        return coerced

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> RowWrite:
        return RowWrite._load(item_json)

    def configurations(self, selector: RawTableSelector) -> Iterable[StorageIOConfig]:
        yield StorageIOConfig(
            kind=RawDatabaseIO.kind,
            folder_name=RawDatabaseIO.folder_name,
            value={"dbName": selector.table.db_name},
            filename=sanitize_filename(selector.table.db_name),
        )
        yield StorageIOConfig(
            kind=RawTableIO.kind, folder_name=RawTableIO.folder_name, value=selector.table.model_dump(by_alias=True)
        )

    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: RawTableSelector | None = None
    ) -> RowWrite:
        key = str(uuid4())
        if selector is not None and selector.key is not None and selector.key in row:
            key = str(row.pop(selector.key))
        return RowWrite(key=key, columns=row)

    @classmethod
    def read_chunks(cls, reader: MultiFileReader, selector: RawTableSelector) -> Iterable[Page[dict[str, JsonVal]]]:
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
            for chunk in chunker(line_numbered_iterator, cls.CHUNK_SIZE):
                yield Page(
                    worker_id="main",
                    items=[DataItem(tracking_id=tracking_id, item=row) for tracking_id, row in chunk],
                )

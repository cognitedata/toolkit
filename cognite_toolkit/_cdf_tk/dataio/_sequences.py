from collections.abc import Iterable, Sequence
from math import ceil
from pathlib import Path
from typing import Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import SequenceRowFilter
from cognite_toolkit._cdf_tk.client.resource_classes.sequence import SequenceResponse
from cognite_toolkit._cdf_tk.client.resource_classes.sequence_rows import (
    SequenceRow,
    SequenceRowsRequest,
    SequenceRowsResponse,
)
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitMissingResourceError,
    ToolkitNotImplementedError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.resource_ios import SequenceIO
from cognite_toolkit._cdf_tk.utils import humanize_collection, sanitize_filename
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
    file_line_tracking_id,
)
from .logger import LogEntryV2, Severity
from .progress import FileBookmark
from .selectors import SequenceRowSelector

_ROW_NUMBER_COLUMN = "rowNumber"
# The API uses DOUBLE when a column is created without a value type.
_DATA_TYPE_BY_VALUE_TYPE: dict[str, DataType] = {"STRING": "string", "DOUBLE": "float", "LONG": "integer"}


class SequenceRowIO(
    ConfigurableDataIO[SequenceRowSelector, SequenceRowsResponse],
    TableDataIO[SequenceRowSelector, SequenceRowsResponse],
    TableUploadableDataIO[SequenceRowSelector, SequenceRowsResponse, SequenceRowsRequest],
):
    """Downloads and uploads the rows of a sequence.

    Each item is a batch of rows from a single sequence, small enough to be inserted in one request.
    In table formats, each row becomes one line with a 'rowNumber' column followed by the sequence columns.
    """

    KIND = "SequenceRows"
    # Each item is a full insert request, so one item is uploaded per request.
    CHUNK_SIZE = 1
    UPLOAD_ENDPOINT = "/sequences/data"
    BASE_SELECTOR = SequenceRowSelector
    MAX_ROWS_PER_REQUEST = 10_000
    MAX_VALUES_PER_REQUEST = 100_000
    MAX_ROWS_PER_DOWNLOAD = 1_000

    def __init__(self, client: ToolkitClient, api_format: Literal["request", "response"] = "request") -> None:
        super().__init__(client, api_format=api_format)
        self._sequence_by_external_id: dict[str, SequenceResponse | None] = {}

    def _get_sequence(self, external_id: str) -> SequenceResponse | None:
        if external_id not in self._sequence_by_external_id:
            retrieved = self.client.sequences.retrieve([ExternalId(external_id=external_id)], ignore_unknown_ids=True)
            self._sequence_by_external_id[external_id] = retrieved[0] if retrieved else None
        return self._sequence_by_external_id[external_id]

    @classmethod
    def _rows_per_request(cls, column_count: int, max_rows: int) -> int:
        return max(1, min(max_rows, cls.MAX_VALUES_PER_REQUEST // max(column_count, 1)))

    def count(self, selector: SequenceRowSelector) -> int | None:
        # The API does not support counting the rows of a sequence.
        return None

    def stream_data(
        self,
        selector: SequenceRowSelector,
        limit: int | None = None,
        bookmark: Bookmark | None = None,
    ) -> Iterable[Page[SequenceRowsResponse]]:
        sequence = self._get_sequence(selector.external_id)
        if sequence is None:
            raise ToolkitMissingResourceError(f"Sequence {selector.external_id!r} does not exist.")
        page_size = self._rows_per_request(len(sequence.columns), self.MAX_ROWS_PER_DOWNLOAD)
        row_filter = SequenceRowFilter(external_id=selector.external_id)
        cursor: str | None = None
        total = 0
        while limit is None or total < limit:
            page_limit = page_size if limit is None else min(page_size, limit - total)
            response = self.client.sequences.rows.paginate(row_filter, limit=page_limit, cursor=cursor)
            for item in response.items:
                if not item.rows:
                    continue
                total += len(item.rows)
                yield self.emit_registered_page(
                    Page(
                        worker_id="main",
                        items=[DataItem(tracking_id=self._tracking_id(selector.external_id, item.rows), item=item)],
                    )
                )
            if response.next_cursor is None:
                break
            cursor = response.next_cursor

    @staticmethod
    def _tracking_id(external_id: str, rows: Sequence[SequenceRow]) -> str:
        return f"{external_id} rows {rows[0].row_number}-{rows[-1].row_number}"

    def data_to_json_chunk(
        self, data_chunk: Page[SequenceRowsResponse], selector: SequenceRowSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        result: list[DataItem[dict[str, JsonVal]]] = []
        for data_item in data_chunk.items:
            if self.api_format == "response":
                dumped = data_item.item.dump()
                dumped.pop("nextCursor", None)
            else:
                dumped = data_item.item.as_request_resource().dump()
            result.append(DataItem(tracking_id=data_item.tracking_id, item=dumped))
        return data_chunk.create_from(result)

    def get_schema(self, selector: SequenceRowSelector) -> list[SchemaColumn] | None:
        sequence = self._get_sequence(selector.external_id)
        if sequence is None:
            return None
        return [
            SchemaColumn(name=_ROW_NUMBER_COLUMN, type="integer"),
            *(
                SchemaColumn(name=column.external_id, type=_DATA_TYPE_BY_VALUE_TYPE[column.value_type or "DOUBLE"])
                for column in sequence.columns
            ),
        ]

    def data_to_row(
        self, data_chunk: Page[SequenceRowsResponse], selector: SequenceRowSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        result: list[DataItem[dict[str, JsonVal]]] = []
        for data_item in data_chunk.items:
            columns = [column.external_id for column in data_item.item.columns]
            mismatched_rows: list[str] = []
            for row in data_item.item.rows:
                if len(row.values) != len(columns):
                    mismatched_rows.append(f"{row.row_number} ({len(row.values)} values)")
                values: dict[str, JsonVal] = {_ROW_NUMBER_COLUMN: row.row_number}
                # Missing values are filled with None, and values without a matching column are dropped.
                values.update(
                    (column, row.values[no] if no < len(row.values) else None) for no, column in enumerate(columns)
                )
                result.append(DataItem(tracking_id=data_item.tracking_id, item=values))
            if mismatched_rows:
                self.logger.log(
                    LogEntryV2(
                        id=data_item.tracking_id,
                        label="Column and value count mismatch",
                        severity=Severity.warning,
                        message=(
                            f"Expected {len(columns)} values per row to match the columns, but "
                            f"{len(mismatched_rows)} row(s) did not: {humanize_collection(mismatched_rows)}. "
                            "Missing values are written as empty and extra values are dropped."
                        ),
                    )
                )
        return data_chunk.create_from(result)

    def json_to_row(
        self, item_json: dict[str, JsonVal], selector: SequenceRowSelector | None = None
    ) -> dict[str, JsonVal]:
        raise ToolkitNotImplementedError(
            f"{type(self).__name__} converts batches of rows in data_to_row, not single JSON items."
        )

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> SequenceRowsRequest:
        if "id" in item_json:
            return SequenceRowsResponse._load(item_json).as_request_resource()
        return SequenceRowsRequest._load(item_json)

    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: SequenceRowSelector | None = None
    ) -> SequenceRowsRequest:
        # read_chunks has already grouped the table rows into a request item.
        return SequenceRowsRequest._load(row)

    def configurations(self, selector: SequenceRowSelector) -> Iterable[StorageIOConfig]:
        sequence = self._get_sequence(selector.external_id)
        if sequence is None:
            return
        yield StorageIOConfig(
            kind=SequenceIO.kind,
            folder_name=SequenceIO.folder_name,
            value=SequenceIO.create_io(self.client).dump_resource(sequence),
            filename=sanitize_filename(selector.external_id),
        )

    @classmethod
    def read_chunks(cls, reader: MultiFileReader, selector: SequenceRowSelector) -> Iterable[Page[dict[str, JsonVal]]]:
        """Reads table rows into batches, where each batch is one request item for the selected sequence."""
        if not reader.is_table:
            yield from super().read_chunks(reader, selector)
            return
        columns: list[str] = []
        rows: list[JsonVal] = []
        batch_size = cls.MAX_ROWS_PER_REQUEST
        current_file = reader.current_file
        first_line = last_line = 0
        for line_no, row in reader.read_chunks_with_line_numbers():
            if rows and (reader.current_file != current_file or len(rows) >= batch_size):
                yield cls._create_batch_page(selector, columns, rows, current_file, first_line, last_line)
                rows = []
            if not rows:
                columns = [name for name in row if name != _ROW_NUMBER_COLUMN]
                batch_size = cls._rows_per_request(len(columns), cls.MAX_ROWS_PER_REQUEST)
                current_file, first_line = reader.current_file, line_no
            if _ROW_NUMBER_COLUMN not in row:
                raise ToolkitValueError(
                    f"Missing column {_ROW_NUMBER_COLUMN!r} in "
                    f"{file_line_tracking_id(reader.current_file, line_no, 'row')}."
                )
            rows.append({"rowNumber": row[_ROW_NUMBER_COLUMN], "values": [row.get(column) for column in columns]})
            last_line = line_no
        if rows:
            yield cls._create_batch_page(selector, columns, rows, current_file, first_line, last_line)

    @staticmethod
    def _create_batch_page(
        selector: SequenceRowSelector,
        columns: list[str],
        rows: list[JsonVal],
        filepath: Path,
        first_line: int,
        last_line: int,
    ) -> Page[dict[str, JsonVal]]:
        item: dict[str, JsonVal] = {
            "externalId": selector.external_id,
            "columns": [column for column in columns],
            "rows": rows,
        }
        return Page(
            worker_id="main",
            items=[DataItem(tracking_id=f"{filepath.name}:rows-{first_line}-{last_line}", item=item)],
            bookmark=FileBookmark(lineno=last_line, filepath=filepath),
        )

    @classmethod
    def count_items(cls, reader: MultiFileReader, selector: SequenceRowSelector | None = None) -> int:
        if not reader.is_table or reader.schema is None:
            return reader.count()
        column_count = len([column for column in reader.schema if column.name != _ROW_NUMBER_COLUMN])
        return ceil(reader.count() / cls._rows_per_request(column_count, cls.MAX_ROWS_PER_REQUEST))

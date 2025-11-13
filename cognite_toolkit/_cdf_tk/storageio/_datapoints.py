from collections.abc import Iterable, Mapping, Sequence
from typing import Any, ClassVar, cast

from cognite.client._proto.data_point_insertion_request_pb2 import DataPointInsertionItem, DataPointInsertionRequest
from cognite.client._proto.data_point_list_response_pb2 import DataPointListResponse
from cognite.client._proto.data_points_pb2 import (
    NumericDatapoint,
    NumericDatapoints,
    StringDatapoint,
    StringDatapoints,
)

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils.dtype_conversion import (
    _EpochConverter,
    _Float64Converter,
    _TextConverter,
    _ValueConverter,
)
from cognite_toolkit._cdf_tk.utils.fileio import FileReader
from cognite_toolkit._cdf_tk.utils.fileio._readers import TableReader
from cognite_toolkit._cdf_tk.utils.http_client import DataBodyRequest, HTTPClient, HTTPMessage
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import Page, TableUploadableStorageIO, UploadItem
from .selectors import DataPointsFileSelector


class DatapointsIO(TableUploadableStorageIO[DataPointsFileSelector, DataPointListResponse, DataPointInsertionRequest]):
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".csv"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    CHUNK_SIZE = 10_000
    BASE_SELECTOR = DataPointsFileSelector
    KIND = "Datapoints"
    SUPPORTED_READ_FORMATS = frozenset({".csv"})
    UPLOAD_ENDPOINT = "/timeseries/data"
    UPLOAD_EXTRA_ARGS: ClassVar[Mapping[str, JsonVal] | None] = None

    def __init__(self, client: ToolkitClient) -> None:
        super().__init__(client)
        self._warned_columns: set[str] = set()
        self._epoc_converter = _EpochConverter(nullable=True)
        self._numeric_converter = _Float64Converter(nullable=True)
        self._string_converter = _TextConverter(nullable=True)

    def as_id(self, item: DataPointListResponse) -> str:
        raise NotImplementedError()

    def stream_data(self, selector: DataPointsFileSelector, limit: int | None = None) -> Iterable[Page]:
        raise NotImplementedError(f"Download of {type(DatapointsIO).__name__.removesuffix('IO')} is not yet supported")

    def count(self, selector: DataPointsFileSelector) -> int | None:
        raise NotImplementedError(f"Download of {type(DatapointsIO).__name__.removesuffix('IO')} is not yet supported")

    def data_to_json_chunk(
        self, data_chunk: Sequence[DataPointListResponse], selector: DataPointsFileSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        raise ToolkitNotImplementedError(
            f"Download of {type(DatapointsIO).__name__.removesuffix('IO')} does not support json format."
        )

    def upload_items(
        self,
        data_chunk: Sequence[UploadItem[DataPointInsertionRequest]],
        http_client: HTTPClient,
        selector: DataPointsFileSelector | None = None,
    ) -> Sequence[HTTPMessage]:
        results: list[HTTPMessage] = []
        for item in data_chunk:
            response = http_client.request_with_retries(
                DataBodyRequest(
                    endpoint_url=http_client.config.create_api_url(self.UPLOAD_ENDPOINT),
                    method="POST",
                    content_type="application/protobuf",
                    data_content=item.item.SerializeToString(),
                )
            )
            results.extend(response)
        return results

    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: DataPointsFileSelector | None = None
    ) -> DataPointInsertionRequest:
        if selector is None:
            raise ValueError("Selector must be provided to convert row to DataPointInsertionItem.")
        # We assume that the row was read using the read_chunks method.
        rows = cast(dict[str, list[Any]], row)
        if selector.timestamp_column not in rows:
            raise RuntimeError(f"Timestamp column '{selector.timestamp_column}' not found.")

        timestamps = list(
            self._convert_values(
                rows[selector.timestamp_column],
                self._epoc_converter,
                f"timestamps (columns {selector.timestamp_column!r})",
                source_id,
            )
        )

        datapoints_items: list[DataPointInsertionItem] = []
        for col, values in rows.items():
            if col == selector.timestamp_column:
                continue
            column = selector.id_by_column.get(col)
            if column is None:
                self._warn_missing_columns(col)
                continue
            args: dict[str, Any] = column.as_wrapped_id()
            if column.dtype == "numeric":
                number_values = self._convert_values(
                    values, self._numeric_converter, f"numeric datapoints (column {col!r})", source_id
                )
                args["numericDatapoints"] = NumericDatapoints(
                    datapoints=[
                        NumericDatapoint(timestamp=timestamp, value=value)
                        for timestamp, value in zip(timestamps, number_values)
                        if timestamp is not None
                    ]
                )
            elif column.dtype == "string":
                string_values = self._convert_values(
                    values, self._string_converter, f"string datapoints (column {col!r})", source_id
                )
                args["stringDatapoints"] = StringDatapoints(
                    datapoints=[
                        StringDatapoint(timestamp=timestamp, value=value)
                        for timestamp, value in zip(timestamps, string_values)
                        if timestamp is not None
                    ]
                )
            else:
                raise RuntimeError(f"Unsupported dtype {column.dtype} for column {col}.")

            datapoints_items.append(DataPointInsertionItem(**args))
        return DataPointInsertionRequest(items=datapoints_items)

    def _convert_values(
        self, values: list[Any], converter: _ValueConverter, name: str, source_id: str
    ) -> Iterable[Any]:
        failed_count = 0
        for value in values:
            try:
                converted = converter.convert(value)
            except ValueError:
                failed_count += 1
                continue
            yield converted
        if failed_count > 0:
            HighSeverityWarning(
                f"In {source_id}' {failed_count:,} {name} could not be converted and will be skipped."
            ).print_warning(console=self.client.console)

    def _warn_missing_columns(self, column: str) -> None:
        if column not in self._warned_columns:
            HighSeverityWarning(
                f"Column '{column}' not found in selector columns. Skipping this column."
            ).print_warning(console=self.client.console)
            self._warned_columns.add(column)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> DataPointInsertionRequest:
        raise ToolkitNotImplementedError(
            f"Upload of {type(DatapointsIO).__name__.removesuffix('IO')} does not support json format."
        )

    @classmethod
    def read_chunks(cls, reader: FileReader) -> Iterable[list[tuple[str, dict[str, JsonVal]]]]:
        if not isinstance(reader, TableReader):
            raise RuntimeError("DatapointsIO can only read from TableReader instances.")
        iterator = iter(reader.read_chunks_with_line_numbers())
        try:
            start_row, first = next(iterator)
        except StopIteration:
            # Empty file
            return
        column_names = list(first.keys())
        batch: dict[str, list[Any]] = {col: [value] for col, value in first.items()}
        last_row = start_row
        for row_no, chunk in iterator:
            for col, value in chunk.items():
                batch[col].append(value)

            # The number of datapoints is the number of rows times the number of value columns.
            if ((len(column_names) - 1) * len(batch[column_names[0]])) >= cls.CHUNK_SIZE:
                # We cannot guarantee JsonVal here, but that is handled later in the processing pipeline.
                yield [(f"rows {start_row} to {row_no}", batch)]  # type: ignore[list-item]
                start_row = row_no + 1
                batch = {col: [] for col in column_names}
            last_row = row_no
        if any(batch.values()):
            yield [(f"rows {start_row} to{last_row}", batch)]  # type: ignore[list-item]

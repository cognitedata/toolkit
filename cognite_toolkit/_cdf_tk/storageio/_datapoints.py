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

from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.utils.dtype_conversion import _EpochConverter, _Float64Converter, _TextConverter
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
        self, row: dict[str, JsonVal], selector: DataPointsFileSelector | None = None
    ) -> DataPointInsertionRequest:
        if selector is None:
            raise ValueError("Selector must be provided to convert row to DataPointInsertionItem.")
        epoc_converter = _EpochConverter(nullable=True, errors="catch")
        numeric_converter = _Float64Converter(nullable=True, errors="catch")
        string_converter = _TextConverter(nullable=True, errors="catch")

        # We assume that the row was read using the read_chunks method.
        rows = cast(dict[str, list[Any]], row)
        timestamps = [epoc_converter.convert(cell) for cell in rows[selector.timestamp_column]]

        datapoints_items: list[DataPointInsertionItem] = []
        for col, values in rows.items():
            if col == selector.timestamp_column:
                continue
            column = selector.id_by_column.get(col)
            if column is None:
                # Todo: Log warning about unknown column?
                continue
            args: dict[str, Any] = column.as_wrapped_id()
            if column.dtype == "numeric":
                # Todo Handle failed conversions?
                ready_values = (numeric_converter.convert(value) for value in values)
                args["numericDatapoints"] = NumericDatapoints(
                    datapoints=[
                        NumericDatapoint(timestamp=timestamp, value=value)
                        for timestamp, value in zip(timestamps, ready_values)
                        if timestamp is not None
                    ]
                )
            elif column.dtype == "string":
                # Todo Handle failed conversions?
                ready_values = (string_converter.convert(value) for value in values)
                args["stringDatapoints"] = StringDatapoints(
                    datapoints=[
                        StringDatapoint(timestamp=timestamp, value=value)
                        for timestamp, value in zip(timestamps, ready_values)
                        if timestamp is not None
                    ]
                )
            else:
                raise RuntimeError(f"Unsupported dtype {column.dtype} for column {col}.")

            datapoints_items.append(DataPointInsertionItem(**args))
        return DataPointInsertionRequest(items=datapoints_items)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> DataPointInsertionRequest:
        raise ToolkitNotImplementedError(
            f"Upload of {type(DatapointsIO).__name__.removesuffix('IO')} does not support json format."
        )

    @classmethod
    def read_chunks(cls, reader: FileReader) -> Iterable[list[tuple[str, dict[str, JsonVal]]]]:
        if not isinstance(reader, TableReader):
            raise RuntimeError("DatapointsIO can only read from TableReader instances.")
        iterator = iter(reader.read_chunks_with_line_numbers())
        start_row, first = next(iterator)
        column_count = len(first)
        batch: dict[str, list[Any]] = {col: [value] for col, value in first.items()}
        last_row = start_row
        for row_no, chunk in iterator:
            for col, value in chunk.items():
                batch[col].append(value)
            if ((len(batch) - 1) * column_count) >= cls.CHUNK_SIZE:
                yield [(f"rows {start_row} to {row_no}", batch)]
                start_row = row_no + 1
                batch = {k: [] for k in batch.keys()}
            last_row = row_no
        if batch:
            yield [(f"rows {start_row} to{last_row}", batch)]

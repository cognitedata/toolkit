from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from cognite.client._proto.data_point_insertion_request_pb2 import DataPointInsertionItem, DataPointInsertionRequest
from cognite.client._proto.data_point_list_response_pb2 import DataPointListItem, DataPointListResponse
from cognite.client._proto.data_points_pb2 import (
    AggregateDatapoint,
    AggregateDatapoints,
    InstanceId,
    NumericDatapoint,
    NumericDatapoints,
    StateDatapoint,
    StateDatapoints,
    Status,
    StringDatapoint,
    StringDatapoints,
)

from cognite_toolkit._cdf_tk.client.http_client import ErrorDetails, FailedResponse, SuccessResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.dataio import DatapointsIO
from cognite_toolkit._cdf_tk.dataio._base import DataItem, Page
from cognite_toolkit._cdf_tk.dataio._datapoints import MAX_TIMESTAMP_MS, MIN_TIMESTAMP_MS, DatapointsRequestAdapter
from cognite_toolkit._cdf_tk.dataio.logger import DataLogger, LogEntryV2, Severity
from cognite_toolkit._cdf_tk.dataio.selectors import (
    DataPointsDataSetSelector,
    DataPointsFileSelector,
    DataPointsInstanceSpaceSelector,
    ExternalIdColumn,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotImplementedError
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

TIME_RANGE: dict[str, int | str] = {"start": 0, "end": 10}
STATUS_ARGS = {"ignoreBadDataPoints": False, "treatUncertainAsBad": False}
BAD = 2147483648


class RecordingLogger(DataLogger):
    def __init__(self) -> None:
        self.registered: list[str] = []
        self.entries: list[LogEntryV2] = []

    def register(self, ids: list[str]) -> None:
        self.registered.extend(ids)

    def log(self, entry: LogEntryV2 | Sequence[LogEntryV2]) -> None:
        self.entries.extend([entry] if isinstance(entry, LogEntryV2) else entry)


def success(response: DataPointListResponse | None = None) -> SuccessResponse:
    content = response.SerializeToString() if response is not None else b""
    return SuccessResponse(status_code=200, body="", content=content)


def count_response(counts: dict[int, int]) -> DataPointListResponse:
    return DataPointListResponse(
        items=[
            DataPointListItem(
                id=ts_id, aggregateDatapoints=AggregateDatapoints(datapoints=[AggregateDatapoint(count=count)])
            )
            for ts_id, count in counts.items()
        ]
    )


def numeric_item(ts_id: int, n_datapoints: int, next_cursor: str = "") -> DataPointListItem:
    return DataPointListItem(
        id=ts_id,
        externalId=f"ts_{ts_id}",
        nextCursor=next_cursor,
        numericDatapoints=NumericDatapoints(
            datapoints=[NumericDatapoint(timestamp=no, value=float(no)) for no in range(n_datapoints)]
        ),
    )


def create_io(client: Any, responses: list[Any]) -> tuple[DatapointsIO, RecordingLogger]:
    client.config = MagicMock(create_api_url=MagicMock(return_value="https://example.com/timeseries/data/list"))
    client.http_client = MagicMock()
    client.http_client.request_single_retries.side_effect = responses
    io = DatapointsIO(client)
    logger = RecordingLogger()
    io.logger = logger
    return io, logger


def request_bodies(client: Any) -> list[Any]:
    return [call.args[0].body_content for call in client.http_client.request_single_retries.call_args_list]


def log_summary(logger: RecordingLogger) -> list[tuple[str, str, Severity]]:
    return [(entry.id, entry.label, entry.severity) for entry in logger.entries]


class TestDataPointsIO:
    def test_iterate_chunks(self, tmp_path: Path):
        n_rows = 2_000
        n_columns = 10
        expected_iterations = ((n_rows * n_columns) // DatapointsIO.CHUNK_SIZE) + (
            1 if (n_rows * n_columns) % DatapointsIO.CHUNK_SIZE > 0 else 0
        )
        data = {
            "timestamp": pa.array(range(n_rows), type=pa.timestamp("ms")),
            **{f"col_{i}": pa.array(range(n_rows), type=pa.float64()) for i in range(n_columns)},
        }
        data_file = tmp_path / "data.Datapoints.parquet"
        table = pa.Table.from_pydict(data)
        pq.write_table(table, data_file)

        selector = DataPointsFileSelector(
            timestamp_column="timestamp",
            columns=tuple(
                [
                    ExternalIdColumn(dtype="numeric", column=f"col_{i}", external_id=f"my_timeseries_{i}")
                    for i in range(n_columns)
                ]
            ),
        )
        with monkeypatch_toolkit_client() as client:
            io = DatapointsIO(client)

            reader = MultiFileReader([data_file])
            # The upload progress bar advances one step per chunk, so the count must match the number of chunks.
            item_count = io.count_items(reader, selector)
            actual_chunks = list(io.read_chunks(reader, selector))

        assert (item_count, len(actual_chunks)) == (expected_iterations, expected_iterations)

    @pytest.mark.parametrize(
        "data_type, values, expected_items",
        [
            pytest.param(
                "numeric",
                ["1.5", "2.5", "3.5"],
                [
                    DataPointInsertionItem(
                        instanceId=InstanceId(space="my_space", externalId="ts_a"),
                        numericDatapoints=NumericDatapoints(
                            datapoints=[
                                NumericDatapoint(timestamp=1, value=1.5),
                                NumericDatapoint(timestamp=3, value=3.5),
                            ]
                        ),
                    ),
                    DataPointInsertionItem(
                        instanceId=InstanceId(space="my_space", externalId="ts_b"),
                        numericDatapoints=NumericDatapoints(datapoints=[NumericDatapoint(timestamp=2, value=2.5)]),
                    ),
                ],
                id="numeric",
            ),
            pytest.param(
                "string",
                ["on", "off", "on"],
                [
                    DataPointInsertionItem(
                        instanceId=InstanceId(space="my_space", externalId="ts_a"),
                        stringDatapoints=StringDatapoints(
                            datapoints=[
                                StringDatapoint(timestamp=1, value="on"),
                                StringDatapoint(timestamp=3, value="on"),
                            ]
                        ),
                    ),
                    DataPointInsertionItem(
                        instanceId=InstanceId(space="my_space", externalId="ts_b"),
                        stringDatapoints=StringDatapoints(datapoints=[StringDatapoint(timestamp=2, value="off")]),
                    ),
                ],
                id="string",
            ),
        ],
    )
    def test_row_to_resource_instance_space(
        self,
        data_type: Literal["numeric", "string"],
        values: list[JsonVal],
        expected_items: list[DataPointInsertionItem],
    ) -> None:
        selector = DataPointsInstanceSpaceSelector(instance_space="my_space", data_type=data_type)
        rows: dict[str, JsonVal] = {
            "space": ["my_space"] * 3,
            "externalId": ["ts_a", "ts_b", "ts_a"],
            "timestamp": [1, 2, 3],
            "value": values,
        }
        with monkeypatch_toolkit_client() as client:
            resource = DatapointsIO(client).row_to_resource("rows", rows, selector)

        assert resource.datapoints == DataPointInsertionRequest(items=expected_items)

    def test_row_to_resource_instance_space_state(self) -> None:
        selector = DataPointsInstanceSpaceSelector(instance_space="my_space", data_type="state")
        rows: dict[str, JsonVal] = {
            "space": ["my_space"] * 3,
            "externalId": ["ts_a", "ts_b", "ts_a"],
            "timestamp": [1, 2, 3],
            "numericValue": [1, None, 0],
            "stringValue": ["on", "unknown", None],
        }
        with monkeypatch_toolkit_client() as client:
            resource = DatapointsIO(client).row_to_resource("rows", rows, selector)

        assert resource.datapoints == DataPointInsertionRequest(
            items=[
                DataPointInsertionItem(
                    instanceId=InstanceId(space="my_space", externalId="ts_a"),
                    stateDatapoints=StateDatapoints(
                        datapoints=[
                            StateDatapoint(timestamp=1, numericValue=1, stringValue="on"),
                            StateDatapoint(timestamp=3, numericValue=0),
                        ]
                    ),
                ),
                DataPointInsertionItem(
                    instanceId=InstanceId(space="my_space", externalId="ts_b"),
                    stateDatapoints=StateDatapoints(datapoints=[StateDatapoint(timestamp=2, stringValue="unknown")]),
                ),
            ]
        )


class TestDatapointsUploadConversion:
    def test_invalid_value_does_not_shift_following_values(self) -> None:
        selector = DataPointsDataSetSelector(data_set_external_id="my_data_set")
        rows: dict[str, JsonVal] = {
            "externalId": ["ts_a"] * 3,
            "timestamp": [1, 2, 3],
            "value": ["1.0", "not a number", "3.0"],
        }
        with monkeypatch_toolkit_client() as client:
            io, logger = create_io(client, [])
            resource = io.row_to_resource("file.csv rows 1 to 3", rows, selector)

        assert {"datapoints": resource.datapoints, "log": log_summary(logger)} == {
            "datapoints": DataPointInsertionRequest(
                items=[
                    DataPointInsertionItem(
                        externalId="ts_a",
                        numericDatapoints=NumericDatapoints(
                            datapoints=[
                                NumericDatapoint(timestamp=1, value=1.0),
                                NumericDatapoint(timestamp=3, value=3.0),
                            ]
                        ),
                    )
                ]
            ),
            "log": [("file.csv rows 1 to 3", "Skipped rows that could not be converted", Severity.warning)],
        }

    def test_empty_cells_are_skipped_and_status_codes_are_uploaded(self) -> None:
        selector = DataPointsDataSetSelector(data_set_external_id="my_data_set")
        rows: dict[str, JsonVal] = {
            "externalId": ["ts_a", "ts_a", "ts_a", None, "ts_a"],
            "timestamp": [1, None, 3, 4, 5],
            "value": [None, 2.0, None, 4.0, 5.0],
            "statusCode": [None, None, BAD, None, BAD],
        }
        with monkeypatch_toolkit_client() as client:
            io, logger = create_io(client, [])
            resource = io.row_to_resource("rows", rows, selector)

        assert {"datapoints": resource.datapoints, "log": sorted(entry.label for entry in logger.entries)} == {
            "datapoints": DataPointInsertionRequest(
                items=[
                    DataPointInsertionItem(
                        externalId="ts_a",
                        numericDatapoints=NumericDatapoints(
                            datapoints=[
                                NumericDatapoint(timestamp=3, status=Status(code=BAD), nullValue=True),
                                NumericDatapoint(timestamp=5, value=5.0, status=Status(code=BAD)),
                            ]
                        ),
                    )
                ]
            ),
            "log": [
                "Skipped rows without a time series identifier",
                "Skipped rows without a timestamp",
                "Skipped rows without a value",
            ],
        }

    def test_state_rows_without_value_or_identifier_are_skipped(self) -> None:
        selector = DataPointsInstanceSpaceSelector(instance_space="my_space", data_type="state")
        rows: dict[str, JsonVal] = {
            "space": ["my_space", "my_space", None, "my_space"],
            "externalId": ["ts_a", "ts_a", "ts_a", "ts_a"],
            "timestamp": [1, 2, 3, 4],
            "numericValue": [None, 1, 1, None],
            "stringValue": [None, None, None, None],
            "statusCode": [None, None, None, BAD],
        }
        with monkeypatch_toolkit_client() as client:
            io, logger = create_io(client, [])
            resource = io.row_to_resource("rows", rows, selector)

        assert {"datapoints": resource.datapoints, "log": sorted(entry.label for entry in logger.entries)} == {
            "datapoints": DataPointInsertionRequest(
                items=[
                    DataPointInsertionItem(
                        instanceId=InstanceId(space="my_space", externalId="ts_a"),
                        stateDatapoints=StateDatapoints(
                            datapoints=[
                                StateDatapoint(timestamp=2, numericValue=1),
                                StateDatapoint(timestamp=4, status=Status(code=BAD)),
                            ]
                        ),
                    )
                ]
            ),
            "log": ["Skipped rows without a time series identifier", "Skipped rows without a value"],
        }

    def test_file_selector_keeps_strings_and_reads_naive_timestamps_as_utc(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "wide.Datapoints.csv"
        csv_file.write_text("time,code,temperature,unknown\n2024-01-01T00:00:00,007,,1\n2024-01-01T01:00:00,,21.5,2\n")
        selector = DataPointsFileSelector(
            timestamp_column="time",
            columns=(
                ExternalIdColumn(dtype="string", column="code", external_id="ts_code"),
                ExternalIdColumn(dtype="numeric", column="temperature", external_id="ts_temperature"),
            ),
        )
        with monkeypatch_toolkit_client() as client:
            io, logger = create_io(client, [])
            reader = MultiFileReader([csv_file], schema=io.get_schema(selector))
            pages = list(io.read_chunks(reader, selector))
            resources = [
                io.row_to_resource(item.tracking_id, item.item, selector) for page in pages for item in page.items
            ]

        assert {
            "datapoints": [resource.datapoints for resource in resources],
            "log": [entry.message for entry in logger.entries],
        } == {
            "datapoints": [
                DataPointInsertionRequest(
                    items=[
                        DataPointInsertionItem(
                            externalId="ts_code",
                            stringDatapoints=StringDatapoints(
                                datapoints=[StringDatapoint(timestamp=1704067200000, value="007")]
                            ),
                        ),
                        DataPointInsertionItem(
                            externalId="ts_temperature",
                            numericDatapoints=NumericDatapoints(
                                datapoints=[NumericDatapoint(timestamp=1704070800000, value=21.5)]
                            ),
                        ),
                    ]
                )
            ],
            "log": ["Skipped 2 values in column 'unknown', which is not in the selector."],
        }

    def test_parquet_string_values_are_not_parsed_as_json(self, tmp_path: Path) -> None:
        parquet_file = tmp_path / "long.Datapoints.parquet"
        pq.write_table(
            pa.Table.from_pydict({"externalId": ["ts_a", "ts_a"], "timestamp": [1, 2], "value": ["007", "[1, 2]"]}),
            parquet_file,
        )
        selector = DataPointsDataSetSelector(data_set_external_id="my_data_set", data_type="string")
        with monkeypatch_toolkit_client() as client:
            io = DatapointsIO(client)
            reader = MultiFileReader([parquet_file], schema=io.get_schema(selector))
            rows = [item.item for page in io.read_chunks(reader, selector) for item in page.items]

        assert rows == [{"externalId": ["ts_a", "ts_a"], "timestamp": [1, 2], "value": ["007", "[1, 2]"]}]


class TestDatapointsReadChunks:
    def test_long_format_chunks_by_rows_and_never_span_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        first = tmp_path / "data-part-0001.Datapoints.csv"
        second = tmp_path / "data-part-0002.Datapoints.csv"
        rows = "".join(f"ts_a,{no},{no}.0\n" for no in range(3))
        first.write_text(f"externalId,timestamp,value\n{rows}")
        second.write_text(f"externalId,timestamp,value,statusCode\n{rows}")
        selector = DataPointsDataSetSelector(data_set_external_id="my_data_set")
        monkeypatch.setattr(DatapointsIO, "CHUNK_SIZE", 2)

        reader = MultiFileReader([first, second])
        tracking_ids = [item.tracking_id for page in DatapointsIO.read_chunks(reader, selector) for item in page.items]
        item_count = DatapointsIO.count_items(reader, selector)

        # A chunk holds CHUNK_SIZE datapoints, which is CHUNK_SIZE rows as each row is a single datapoint.
        assert {"tracking_ids": tracking_ids, "item_count": item_count} == {
            "tracking_ids": [
                "data-part-0001.Datapoints.csv rows 1 to 2",
                "data-part-0001.Datapoints.csv rows 3 to 3",
                "data-part-0002.Datapoints.csv rows 1 to 2",
                "data-part-0002.Datapoints.csv rows 3 to 3",
            ],
            "item_count": 4,
        }


class TestDatapointsDownload:
    def test_stream_timeseries_datapoints_continues_with_cursor(self) -> None:
        responses = [
            count_response({1: 80_000, 2: 50_000}),
            DataPointListResponse(items=[DataPointListItem(id=1), DataPointListItem(id=2, nextCursor="cursor-2")]),
            DataPointListResponse(items=[DataPointListItem(id=2)]),
        ]
        with monkeypatch_toolkit_client() as client:
            io, _ = create_io(client, [success(response) for response in responses])
            pages = list(
                io._stream_timeseries_datapoints(
                    [
                        {"instanceId": {"space": "my_space", "externalId": "a"}},
                        {"instanceId": {"space": "my_space", "externalId": "b"}},
                    ],
                    TIME_RANGE,
                    limit_per_timeseries=100_000,
                )
            )
            data_requests = request_bodies(client)[1:]

        data_args = {**STATUS_ARGS, "includeStatus": True}
        assert {
            "tracking_ids": [item.tracking_id for page in pages for item in page.items],
            "data_requests": data_requests,
        } == {
            "tracking_ids": ["datapoints batch 1", "datapoints batch 2"],
            "data_requests": [
                {
                    "items": [
                        {"id": 1, **TIME_RANGE, "limit": 80_000, **data_args},
                        {"id": 2, **TIME_RANGE, "limit": 20_000, **data_args},
                    ],
                    "ignoreUnknownIds": True,
                },
                {
                    "items": [{"id": 2, **TIME_RANGE, "limit": 30_000, "cursor": "cursor-2", **data_args}],
                    "ignoreUnknownIds": True,
                },
            ],
        }

    def test_count_request_includes_bad_datapoints_and_ignores_unknown_ids(self) -> None:
        with monkeypatch_toolkit_client() as client:
            io, _ = create_io(client, [success(DataPointListResponse())])
            list(io._stream_timeseries_datapoints([{"id": 1}], TIME_RANGE, limit_per_timeseries=10))

        assert request_bodies(client) == [
            {
                "items": [
                    {
                        "id": 1,
                        **TIME_RANGE,
                        **STATUS_ARGS,
                        "limit": DatapointsIO.MAX_PER_REQUEST_DATAPOINTS_AGGREGATION,
                        "aggregates": ["count"],
                        "granularity": "1200mo",
                    }
                ],
                "ignoreUnknownIds": True,
            }
        ]

    def test_failed_requests_are_logged(self) -> None:
        failed = FailedResponse(status_code=500, body="", error=ErrorDetails(code=500, message="Internal error"))
        with monkeypatch_toolkit_client() as client:
            io, logger = create_io(client, [failed, success(count_response({1: 5})), failed])
            first = list(io._stream_timeseries_datapoints([{"id": 1}], TIME_RANGE, 10))
            second = list(io._stream_timeseries_datapoints([{"id": 1}], TIME_RANGE, 10))

        assert {"pages": first + second, "registered": logger.registered, "log": log_summary(logger)} == {
            "pages": [],
            "registered": ["datapoints batch 1", "datapoints batch 2"],
            "log": [
                ("datapoints batch 1", "Failed to count datapoints", Severity.failure),
                ("datapoints batch 2", "Failed to download datapoints", Severity.failure),
            ],
        }

    def test_truncation_at_limit_per_timeseries_is_logged(self) -> None:
        responses = [count_response({1: 50}), DataPointListResponse(items=[numeric_item(1, 10, next_cursor="more")])]
        with monkeypatch_toolkit_client() as client:
            io, logger = create_io(client, [success(response) for response in responses])
            pages = list(io._stream_timeseries_datapoints([{"id": 1}], TIME_RANGE, 10))

        assert {"pages": len(pages), "requests": len(request_bodies(client)), "log": log_summary(logger)} == {
            "pages": 1,
            "requests": 2,
            "log": [("datapoints batch 1", "Datapoints limit per time series reached", Severity.warning)],
        }

    def test_datapoints_missing_from_count_are_still_downloaded(self) -> None:
        responses = [
            # The count aggregate is eventually consistent, and can lag behind the datapoints.
            count_response({1: 0}),
            DataPointListResponse(items=[numeric_item(1, 1, next_cursor="more")]),
            DataPointListResponse(items=[numeric_item(1, 3)]),
        ]
        with monkeypatch_toolkit_client() as client:
            io, _ = create_io(client, [success(response) for response in responses])
            pages = list(io._stream_timeseries_datapoints([{"id": 1}], TIME_RANGE, 100))
            limits = [body["items"][0]["limit"] for body in request_bodies(client)[1:]]

        assert {"pages": len(pages), "limits": limits} == {"pages": 2, "limits": [1, 99]}

    def test_timeseries_missing_from_response_are_not_requested_again(self) -> None:
        responses = [
            count_response({1: 5, 2: 5}),
            # Time series 2 was deleted after the count request.
            DataPointListResponse(items=[numeric_item(1, 5)]),
        ]
        with monkeypatch_toolkit_client() as client:
            io, _ = create_io(client, [success(response) for response in responses])
            pages = list(io._stream_timeseries_datapoints([{"id": 1}, {"id": 2}], TIME_RANGE, 10))

        assert {"pages": len(pages), "requests": len(request_bodies(client))} == {
            "pages": 1,
            "requests": 2,
        }

    @pytest.mark.parametrize(
        "value, default, expected",
        [
            pytest.param(None, MIN_TIMESTAMP_MS, MIN_TIMESTAMP_MS, id="default start"),
            pytest.param(None, MAX_TIMESTAMP_MS, MAX_TIMESTAMP_MS, id="default end includes future datapoints"),
            pytest.param(1704067200000, 0, 1704067200000, id="epoch ms"),
            pytest.param("1704067200000", 0, 1704067200000, id="epoch ms string"),
            pytest.param("2024-01-01T00:00:00Z", 0, 1704067200000, id="ISO 8601"),
            pytest.param("2024-01-01T00:00:00", 0, 1704067200000, id="naive ISO 8601 is UTC"),
            pytest.param("2d-ago", 0, "2d-ago", id="relative"),
            pytest.param("now", 0, "now", id="now"),
        ],
    )
    def test_query_time(self, value: int | str | None, default: int, expected: int | str) -> None:
        with monkeypatch_toolkit_client() as client:
            assert DatapointsIO(client)._query_time(value, default) == expected

    def test_data_to_row_includes_status_codes_and_null_values(self) -> None:
        selector = DataPointsDataSetSelector(data_set_external_id="my_data_set")
        response = DataPointListResponse(
            items=[
                DataPointListItem(
                    id=1,
                    externalId="ts_a",
                    numericDatapoints=NumericDatapoints(
                        datapoints=[
                            NumericDatapoint(timestamp=1, value=1.0),
                            NumericDatapoint(timestamp=2, status=Status(code=BAD), nullValue=True),
                        ]
                    ),
                )
            ]
        )
        with monkeypatch_toolkit_client() as client:
            page = DatapointsIO(client).data_to_row(
                Page("main", [DataItem(tracking_id="batch", item=response)]), selector
            )

        assert [item.item for item in page.items] == [
            {"externalId": "ts_a", "timestamp": 1, "value": 1.0, "statusCode": None},
            {"externalId": "ts_a", "timestamp": 2, "value": None, "statusCode": BAD},
        ]

    def test_unsupported_json_format_names_the_io(self) -> None:
        with monkeypatch_toolkit_client() as client:
            with pytest.raises(ToolkitNotImplementedError, match="Download of Datapoints does not support json format"):
                DatapointsIO(client).data_to_json_chunk(Page("main", []))

    def test_selector_names_include_data_type(self) -> None:
        names = {
            str(DataPointsDataSetSelector(data_set_external_id="ds", data_type=data_type))
            for data_type in ("numeric", "string")
        } | {
            str(DataPointsInstanceSpaceSelector(instance_space="sp", data_type=data_type))
            for data_type in ("numeric", "string", "state")
        }

        assert len(names) == 5


class TestDatapointsUpload:
    def test_upload_retries_without_missing_timeseries(self) -> None:
        request = DataPointInsertionRequest(
            items=[
                DataPointInsertionItem(
                    externalId=external_id,
                    numericDatapoints=NumericDatapoints(datapoints=[NumericDatapoint(timestamp=1, value=1.0)]),
                )
                for external_id in ["exists", "missing"]
            ]
        )
        missing = FailedResponse(
            status_code=400,
            body="",
            error=ErrorDetails(code=400, message="Time series not found", missing=[{"externalId": "missing"}]),
        )
        http_client = MagicMock()
        http_client.config.create_api_url.return_value = "https://example.com/timeseries/data"
        http_client.request_single_retries.side_effect = [missing, success()]
        with monkeypatch_toolkit_client() as client:
            io, logger = create_io(client, [])
            results = io.upload_items(
                Page("main", [DataItem(tracking_id="rows 1 to 2", item=DatapointsRequestAdapter(datapoints=request))]),
                http_client,
            )
        retried = DataPointInsertionRequest.FromString(
            http_client.request_single_retries.call_args.args[0].data_content
        )

        assert {
            "results": [type(result).__name__ for result in results],
            "retried": [item.externalId for item in retried.items],
            "log": log_summary(logger),
        } == {
            "results": ["ItemsSuccessResponse"],
            "retried": ["exists"],
            "log": [("rows 1 to 2", "Missing time series", Severity.warning)],
        }


class TestDatapointsCount:
    @pytest.mark.parametrize("has_timeseries, expected", [(True, None), (False, 0)])
    def test_zero_count_is_unknown_when_timeseries_exist(self, has_timeseries: bool, expected: int | None) -> None:
        selector = DataPointsInstanceSpaceSelector(instance_space="my_space")
        with monkeypatch_toolkit_client() as client:
            client.instances.aggregate.return_value = MagicMock(items=[])
            client.instances.iterate.return_value = [[MagicMock()]] if has_timeseries else []

            assert DatapointsIO(client).count(selector) == expected

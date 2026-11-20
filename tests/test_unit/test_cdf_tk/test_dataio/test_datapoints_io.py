from pathlib import Path
from typing import Literal
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
    StringDatapoint,
    StringDatapoints,
)

from cognite_toolkit._cdf_tk.client.http_client import SuccessResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.dataio import DatapointsIO
from cognite_toolkit._cdf_tk.dataio.selectors import (
    DataPointsFileSelector,
    DataPointsInstanceSpaceSelector,
    ExternalIdColumn,
)
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


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
            item_count = io.count_items(reader, selector)
            assert item_count == n_rows * n_columns

            actual_chunks = list(io.read_chunks(reader, selector))
            assert len(actual_chunks) == expected_iterations, (
                f"Expected {expected_iterations} chunk, got {len(actual_chunks)}"
            )

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

    def test_stream_timeseries_datapoints_continues_with_cursor(self) -> None:
        selector = DataPointsInstanceSpaceSelector(instance_space="my_space", start=0, end=10)
        responses = [
            DataPointListResponse(
                items=[
                    DataPointListItem(
                        id=ts_id, aggregateDatapoints=AggregateDatapoints(datapoints=[AggregateDatapoint(count=count)])
                    )
                    for ts_id, count in [(1, 80_000), (2, 50_000)]
                ]
            ),
            DataPointListResponse(items=[DataPointListItem(id=1), DataPointListItem(id=2, nextCursor="cursor-2")]),
            DataPointListResponse(items=[DataPointListItem(id=2)]),
        ]
        with monkeypatch_toolkit_client() as client:
            client.config = MagicMock(create_api_url=MagicMock(return_value="https://example.com/timeseries/data/list"))
            client.http_client = MagicMock()
            client.http_client.request_single_retries.side_effect = [
                SuccessResponse(status_code=200, body="", content=response.SerializeToString())
                for response in responses
            ]
            pages = list(
                DatapointsIO(client)._stream_timeseries_datapoints(
                    [
                        {"instanceId": {"space": "my_space", "externalId": "a"}},
                        {"instanceId": {"space": "my_space", "externalId": "b"}},
                    ],
                    selector,
                    limit_per_timeseries=100_000,
                )
            )
            data_requests = [
                call.args[0].body_content for call in client.http_client.request_single_retries.call_args_list[1:]
            ]

        assert {
            "tracking_ids": [item.tracking_id for page in pages for item in page.items],
            "data_requests": data_requests,
        } == {
            "tracking_ids": ["datapoints batch 1", "datapoints batch 2"],
            "data_requests": [
                {
                    "items": [
                        {"id": 1, "start": 0, "end": 10, "limit": 80_000},
                        {"id": 2, "start": 0, "end": 10, "limit": 20_000},
                    ]
                },
                {"items": [{"id": 2, "start": 0, "end": 10, "limit": 30_000, "cursor": "cursor-2"}]},
            ],
        }

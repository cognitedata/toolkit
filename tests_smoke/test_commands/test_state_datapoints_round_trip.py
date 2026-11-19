import time
from pathlib import Path

import pytest
from cognite.client._proto.data_point_insertion_request_pb2 import DataPointInsertionItem, DataPointInsertionRequest
from cognite.client._proto.data_point_list_response_pb2 import DataPointListResponse
from cognite.client._proto.data_points_pb2 import InstanceId, StateDatapoint, StateDatapoints

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import RequestMessage
from cognite_toolkit._cdf_tk.client.identifiers import SpaceId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceSource,
    NodeId,
    NodeRequest,
    SpaceRequest,
    ViewId,
)
from cognite_toolkit._cdf_tk.client.resource_classes.datapoints import DatapointsDeleteRequest
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.dataio import DatapointsIO
from cognite_toolkit._cdf_tk.dataio.selectors import DataPointsInstanceSpaceSelector

STATE_SPACE = "toolkit_test_state_round_trip"
STATE_SET_VIEW = ViewId(space="cdf_cdm", external_id="CogniteStateSet", version="v1")
TIMESERIES_VIEW = ViewId(space="cdf_cdm", external_id="CogniteTimeSeries", version="v1")
STATE_SET_ID = NodeId(space=STATE_SPACE, external_id="state_round_trip_states")
TIMESERIES_IDS = [NodeId(space=STATE_SPACE, external_id=f"state_round_trip_ts_{no}") for no in range(2)]
STATES = [(0, "OFF"), (1, "ON"), (2, "ERROR")]
START = 1_700_000_000_000

StateRow = tuple[int, int | None, str | None]


@pytest.fixture
def state_timeseries(toolkit_client: ToolkitClient) -> list[NodeId]:
    if not toolkit_client.spaces.retrieve([SpaceId(space=STATE_SPACE)]):
        toolkit_client.spaces.create([SpaceRequest(space=STATE_SPACE, name="Toolkit state round trip")])
    state_set = NodeRequest(
        space=STATE_SET_ID.space,
        external_id=STATE_SET_ID.external_id,
        sources=[
            InstanceSource(
                source=STATE_SET_VIEW,
                properties={
                    "name": "Round trip states",
                    "states": [{"numericValue": numeric, "stringValue": string} for numeric, string in STATES],
                },
            )
        ],
    )
    timeseries = [
        NodeRequest(
            space=ts_id.space,
            external_id=ts_id.external_id,
            sources=[
                InstanceSource(
                    source=TIMESERIES_VIEW,
                    properties={
                        "name": ts_id.external_id,
                        "type": "state",
                        "isStep": True,
                        "stateSet": {"space": STATE_SET_ID.space, "externalId": STATE_SET_ID.external_id},
                    },
                )
            ],
        )
        for ts_id in TIMESERIES_IDS
    ]
    toolkit_client.instances.create([state_set, *timeseries])
    return TIMESERIES_IDS


def insert_state_datapoints(client: ToolkitClient, datapoints_by_id: dict[NodeId, list[StateRow]]) -> None:
    request = DataPointInsertionRequest(
        items=[
            DataPointInsertionItem(
                instanceId=InstanceId(space=ts_id.space, externalId=ts_id.external_id),
                stateDatapoints=StateDatapoints(
                    datapoints=[
                        StateDatapoint(timestamp=timestamp, numericValue=numeric, stringValue=string)
                        for timestamp, numeric, string in datapoints
                    ]
                ),
            )
            for ts_id, datapoints in datapoints_by_id.items()
        ]
    )
    message = RequestMessage(
        endpoint_url=client.config.create_api_url("/timeseries/data"),
        method="POST",
        content_type="application/protobuf",
        data_content=request.SerializeToString(),
    )
    client.http_client.request_single_retries(message).get_success_or_raise(message)


def retrieve_state_datapoints(client: ToolkitClient, timeseries_ids: list[NodeId]) -> dict[NodeId, list[StateRow]]:
    message = RequestMessage(
        endpoint_url=client.config.create_api_url("/timeseries/data/list"),
        method="POST",
        accept="application/protobuf",
        body_content={
            "items": [
                {"instanceId": {"space": ts_id.space, "externalId": ts_id.external_id}, "start": 0, "limit": 1000}
                for ts_id in timeseries_ids
            ]
        },
    )
    response = client.http_client.request_single_retries(message).get_success_or_raise(message)
    return {
        NodeId(space=item.instanceId.space, external_id=item.instanceId.externalId): [
            (
                dp.timestamp,
                dp.numericValue if dp.HasField("numericValue") else None,
                dp.stringValue if dp.HasField("stringValue") else None,
            )
            for dp in item.stateDatapoints.datapoints
        ]
        for item in DataPointListResponse.FromString(response.content).items
    }


def delete_state_datapoints(client: ToolkitClient, timeseries_ids: list[NodeId]) -> None:
    client.timeseries.datapoints.delete(
        [
            DatapointsDeleteRequest(
                instance_id=ts_id,
                inclusive_begin=0,
                exclusive_end=START * 2,
            )
            for ts_id in timeseries_ids
        ]
    )


class TestStateDatapointsRoundTrip:
    @pytest.mark.usefixtures("enable_extend_download")
    @pytest.mark.parametrize("file_format", [".csv", ".parquet", ".sqlite"])
    def test_state_datapoints_round_trip(
        self, toolkit_client: ToolkitClient, state_timeseries: list[NodeId], tmp_path: Path, file_format: str
    ) -> None:
        expected: dict[NodeId, list[StateRow]] = {
            ts_id: [(START + i * 1000, *STATES[(i + no) % len(STATES)]) for i in range(5)]
            for no, ts_id in enumerate(state_timeseries)
        }
        delete_state_datapoints(toolkit_client, state_timeseries)
        insert_state_datapoints(toolkit_client, expected)
        selector = DataPointsInstanceSpaceSelector(
            instance_space=STATE_SPACE, data_type="state", download_dir_name="state"
        )
        io = DatapointsIO(toolkit_client)
        # The count comes from an eventually consistent aggregate, and the download is skipped when it is zero.
        for _ in range(120):
            if io.count(selector) == len(state_timeseries):
                break
            time.sleep(1)

        DownloadCommand(silent=True, skip_tracking=True).download(
            selectors=[selector],
            io=io,
            output_dir=tmp_path,
            verbose=False,
            file_format=file_format,
            compression="none",
            limit=None,
        )
        delete_state_datapoints(toolkit_client, state_timeseries)
        upload_dir = tmp_path if file_format == ".sqlite" else tmp_path / "state"
        UploadCommand(silent=True, skip_tracking=True).upload(
            upload_dir, toolkit_client, deploy_resources=False, dry_run=False, verbose=False
        )

        assert retrieve_state_datapoints(toolkit_client, state_timeseries) == expected

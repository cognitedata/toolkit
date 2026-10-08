import json
from typing import Any

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.datapoints import DatapointsAPI
from cognite_toolkit._cdf_tk.client.api.timeseries import TimeSeriesAPI
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.datapoints import (
    Datapoint,
    DatapointsDeleteRequest,
    DatapointsQueryRequest,
    DatapointsRequest,
    DatapointStatus,
    LatestDatapointRequest,
)


def _request_payload(call: respx.models.Call) -> dict[str, Any]:
    return {
        "url": str(call.request.url),
        "content_type": call.request.headers["content-type"],
        "accept": call.request.headers["accept"],
        "body": json.loads(call.request.content),
    }


class TestDatapointsAPI:
    def test_timeseries_api_exposes_datapoints(self, toolkit_config: ToolkitClientConfig) -> None:
        api = TimeSeriesAPI(HTTPClient(toolkit_config))

        assert isinstance(api.datapoints, DatapointsAPI)

    def test_create_posts_json(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter, disable_gzip: None
    ) -> None:
        api = DatapointsAPI(HTTPClient(toolkit_config))
        respx_mock.post(api._make_url("/timeseries/data")).mock(return_value=httpx2.Response(status_code=200, json={}))
        api.create(
            [
                DatapointsRequest(external_id="ts_001", datapoints=[Datapoint(timestamp=1, value=1.5)]),
                DatapointsRequest(
                    instance_id=NodeId(space="space", external_id="ts_node"),
                    datapoints=[Datapoint(timestamp=5, value="hot", status=DatapointStatus(code=192, symbol="Good"))],
                ),
            ]
        )

        assert _request_payload(respx_mock.calls[0]) == {
            "url": api._make_url("/timeseries/data"),
            "content_type": "application/json",
            "accept": "application/json",
            "body": {
                "items": [
                    {"externalId": "ts_001", "datapoints": [{"timestamp": 1, "value": 1.5}]},
                    {
                        "instanceId": {"space": "space", "externalId": "ts_node"},
                        "datapoints": [
                            {"timestamp": 5, "value": "hot", "status": {"code": 192, "symbol": "Good"}},
                        ],
                    },
                ]
            },
        }

    def test_create_splits_on_series_and_datapoint_limits(
        self,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
        disable_gzip: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        api = DatapointsAPI(HTTPClient(toolkit_config))
        api._method_endpoint_map["create"] = Endpoint(method="POST", path="/timeseries/data", item_limit=10)
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.client.api.datapoints._INSERT_DATAPOINT_LIMIT",
            2,
        )
        respx_mock.post(api._make_url("/timeseries/data")).mock(return_value=httpx2.Response(status_code=200, json={}))
        api.create(
            [
                DatapointsRequest(
                    external_id="a",
                    datapoints=[
                        Datapoint(timestamp=1, value=1),
                        Datapoint(timestamp=2, value=2),
                        Datapoint(timestamp=3, value=3),
                    ],
                ),
                DatapointsRequest(external_id="b", datapoints=[Datapoint(timestamp=4, value=4)]),
            ]
        )

        assert [json.loads(call.request.content) for call in respx_mock.calls] == [
            {
                "items": [
                    {
                        "externalId": "a",
                        "datapoints": [{"timestamp": 1, "value": 1}, {"timestamp": 2, "value": 2}],
                    }
                ]
            },
            {
                "items": [
                    {"externalId": "a", "datapoints": [{"timestamp": 3, "value": 3}]},
                    {"externalId": "b", "datapoints": [{"timestamp": 4, "value": 4}]},
                ]
            },
        ]

    def test_retrieve_posts_json_query(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter, disable_gzip: None
    ) -> None:
        api = DatapointsAPI(HTTPClient(toolkit_config))
        response = {
            "items": [
                {
                    "id": 1,
                    "externalId": "ts_001",
                    "isString": False,
                    "isStep": False,
                    "nextCursor": "cursor-2",
                    "datapoints": [
                        {
                            "timestamp": 10,
                            "average": 1.5,
                            "max": 3,
                            "count": 2,
                            "maxDatapoint": {
                                "timestamp": 9,
                                "value": 3,
                                "status": {"code": 0, "symbol": "Good"},
                            },
                        }
                    ],
                }
            ]
        }
        respx_mock.post(api._make_url("/timeseries/data/list")).mock(
            return_value=httpx2.Response(status_code=200, json=response)
        )
        retrieved = api.retrieve(
            [DatapointsQueryRequest(external_id="ts_001", cursor="cursor-1")],
            start="1d-ago",
            aggregates=["average", "max", "maxDatapoint", "count"],
            granularity="1h",
            time_zone="Europe/Oslo",
            ignore_unknown_ids=True,
        )

        assert {
            "request": _request_payload(respx_mock.calls[0]),
            "datapoint": retrieved[0].datapoints[0].dump(),
            "next_cursor": retrieved[0].next_cursor,
        } == {
            "request": {
                "url": api._make_url("/timeseries/data/list"),
                "content_type": "application/json",
                "accept": "application/json",
                "body": {
                    "items": [{"externalId": "ts_001", "cursor": "cursor-1"}],
                    "start": "1d-ago",
                    "aggregates": ["average", "max", "maxDatapoint", "count"],
                    "granularity": "1h",
                    "timeZone": "Europe/Oslo",
                    "ignoreUnknownIds": True,
                },
            },
            "datapoint": {
                "timestamp": 10,
                "average": 1.5,
                "max": 3,
                "count": 2,
                "maxDatapoint": {"timestamp": 9, "value": 3, "status": {"code": 0, "symbol": "Good"}},
            },
            "next_cursor": "cursor-2",
        }

    def test_latest_posts_json_query(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter, disable_gzip: None
    ) -> None:
        api = DatapointsAPI(HTTPClient(toolkit_config))
        respx_mock.post(api._make_url("/timeseries/data/latest")).mock(
            return_value=httpx2.Response(
                status_code=200,
                json={
                    "items": [
                        {
                            "id": 7,
                            "externalId": "ts_001",
                            "isString": False,
                            "isStep": True,
                            "datapoints": [{"timestamp": 50, "value": 4}],
                        }
                    ]
                },
            )
        )
        retrieved = api.latest(
            [LatestDatapointRequest(id=7, before="now")],
            ignore_unknown_ids=True,
        )

        assert {
            "request": _request_payload(respx_mock.calls[0]),
            "value": retrieved[0].datapoints[0].value,
        } == {
            "request": {
                "url": api._make_url("/timeseries/data/latest"),
                "content_type": "application/json",
                "accept": "application/json",
                "body": {
                    "items": [{"id": 7, "before": "now"}],
                    "ignoreUnknownIds": True,
                },
            },
            "value": 4,
        }

    def test_delete_posts_json_ranges(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter, disable_gzip: None
    ) -> None:
        api = DatapointsAPI(HTTPClient(toolkit_config))
        respx_mock.post(api._make_url("/timeseries/data/delete")).mock(
            return_value=httpx2.Response(status_code=200, json={})
        )
        api.delete([DatapointsDeleteRequest(external_id="ts_001", inclusive_begin=10, exclusive_end=20)])

        assert _request_payload(respx_mock.calls[0]) == {
            "url": api._make_url("/timeseries/data/delete"),
            "content_type": "application/json",
            "accept": "application/json",
            "body": {"items": [{"externalId": "ts_001", "inclusiveBegin": 10, "exclusiveEnd": 20}]},
        }

    @pytest.mark.parametrize(
        "retrieve_kwargs, item_kwargs",
        [
            ({"aggregates": ["average"]}, {}),
            ({}, {"aggregates": ["average"]}),
        ],
    )
    def test_retrieve_requires_granularity_for_aggregates(
        self,
        toolkit_config: ToolkitClientConfig,
        retrieve_kwargs: dict[str, Any],
        item_kwargs: dict[str, Any],
    ) -> None:
        api = DatapointsAPI(HTTPClient(toolkit_config))
        with pytest.raises(ValueError, match="granularity is required when aggregates are set"):
            api.retrieve([DatapointsQueryRequest(external_id="ts_001", **item_kwargs)], **retrieve_kwargs)

    def test_query_rejects_two_unit_targets(self) -> None:
        with pytest.raises(ValueError, match="Specify only one of target_unit and target_unit_system"):
            DatapointsQueryRequest(
                external_id="ts_001",
                target_unit="temperature:deg_c",
                target_unit_system="SI",
            )

    def test_request_requires_one_timeseries_identifier(self) -> None:
        with pytest.raises(ValueError, match="Exactly one of id, external_id, or instance_id must be set"):
            DatapointsRequest(
                id=1,
                external_id="ts_001",
                datapoints=[Datapoint(timestamp=1, value=1)],
            )

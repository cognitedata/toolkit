import json

import pytest
import respx
from httpx2 import Response

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.request_classes.filters import EpochTimestampRange, FunctionCallFilter


def _call_payload(call_id: int = 1) -> dict[str, object]:
    return {
        "id": call_id,
        "status": "Running",
        "startTime": 1730204346000,
        "endTime": 1730204347000,
        "functionId": 10,
        "scheduleId": 5,
        "scheduledTime": 1730204346000,
    }


class TestFunctionCallsAPI:
    @pytest.mark.usefixtures("disable_gzip")
    def test_call_function(self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        url = toolkit_config.create_api_url("/functions/10/call")
        respx_mock.post(url).mock(return_value=Response(status_code=201, json=_call_payload()))

        created = client.tool.functions.calls.call(
            function_id=10,
            nonce="session-nonce",
            data={"maxValue": 4},
        )

        assert created.dump() == _call_payload()
        assert len(respx_mock.calls) == 1
        request = respx_mock.calls[0].request
        assert request.url == url
        assert json.loads(request.content) == {"nonce": "session-nonce", "data": {"maxValue": 4}}

    @pytest.mark.usefixtures("disable_gzip")
    def test_retrieve_calls(self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        url = toolkit_config.create_api_url("/functions/10/calls/byids")
        respx_mock.post(url).mock(return_value=Response(status_code=200, json={"items": [_call_payload()]}))

        retrieved = client.tool.functions.calls.retrieve(
            10,
            [InternalId(id=1)],
            ignore_unknown_ids=True,
        )

        assert [item.dump() for item in retrieved] == [_call_payload()]
        assert json.loads(respx_mock.calls[0].request.content) == {
            "items": [{"id": 1}],
            "ignoreUnknownIds": True,
        }

    @pytest.mark.usefixtures("disable_gzip")
    def test_list_calls_follows_cursor(self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        url = toolkit_config.create_api_url("/functions/10/calls/list")
        first = _call_payload(1)
        second = _call_payload(2)
        respx_mock.post(url).mock(
            side_effect=[
                Response(status_code=200, json={"items": [first], "nextCursor": "next"}),
                Response(status_code=200, json={"items": [second]}),
            ]
        )

        listed = client.tool.functions.calls.list(
            function_id=10,
            filter=FunctionCallFilter(
                status="Running",
                schedule_id=5,
                start_time=EpochTimestampRange(min_=1234, max_=5678),
            ),
            limit=2,
        )

        assert [item.dump() for item in listed] == [first, second]
        bodies = [json.loads(call.request.content) for call in respx_mock.calls]
        assert bodies == [
            {
                "filter": {"status": "Running", "scheduleId": 5, "startTime": {"min": 1234, "max": 5678}},
                "limit": 2,
            },
            {
                "filter": {"status": "Running", "scheduleId": 5, "startTime": {"min": 1234, "max": 5678}},
                "limit": 1,
                "cursor": "next",
            },
        ]

    def test_get_response_and_logs(self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        response_url = toolkit_config.create_api_url("/functions/10/calls/2/response")
        logs_url = toolkit_config.create_api_url("/functions/10/calls/2/logs")
        respx_mock.get(response_url).mock(
            return_value=Response(
                status_code=200,
                json={"functionId": 10, "callId": 2, "response": {"numAssets": 1234}},
            )
        )
        respx_mock.get(logs_url).mock(
            return_value=Response(
                status_code=200,
                json={"items": [{"timestamp": 1585350274000, "message": "Did do fancy thing"}]},
            )
        )

        response = client.tool.functions.calls.get_response(function_id=10, call_id=2)
        logs = client.tool.functions.calls.get_logs(function_id=10, call_id=2)

        assert response.dump() == {"functionId": 10, "callId": 2, "response": {"numAssets": 1234}}
        assert [entry.dump() for entry in logs] == [{"timestamp": 1585350274000, "message": "Did do fancy thing"}]

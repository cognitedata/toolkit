import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.function_app import FunctionAppCallPayload
from cognite_toolkit._cdf_tk.client.resource_classes.function_call import FunctionCallLogEntry, FunctionCallResult

_FUNCTION_APP = {
    "id": 1,
    "externalId": "my/app",
    "name": "My app",
    "fileId": 2,
    "createdTime": 0,
    "status": "Ready",
    "functionType": "functionApp",
}
_FUNCTION_CALL = {"id": 10, "status": "Completed", "startTime": 0, "functionId": 1}
_ENCODED_PATH = "/function-apps/my%2Fapp"


@pytest.mark.usefixtures("disable_gzip")
class TestFunctionAppsAPI:
    def test_list_uses_beta_and_limit(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        respx_mock.get(toolkit_config.create_api_url("/function-apps")).mock(
            return_value=httpx2.Response(200, json={"items": [_FUNCTION_APP]})
        )

        toolkit_client.functions.apps.list(limit=5)

        request = respx_mock.calls[0].request
        assert (request.url.params["limit"], request.headers["cdf-version"]) == ("5", "beta")

    def test_retrieve_ignore_unknown_ids(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        respx_mock.get(toolkit_config.create_api_url(_ENCODED_PATH)).mock(
            return_value=httpx2.Response(200, json=_FUNCTION_APP)
        )
        respx_mock.get(toolkit_config.create_api_url("/function-apps/missing")).mock(
            return_value=httpx2.Response(404, json={"error": {"code": 404, "message": "Not found"}})
        )

        retrieved = toolkit_client.functions.apps.retrieve(
            [ExternalId(external_id="my/app"), ExternalId(external_id="missing")], ignore_unknown_ids=True
        )

        assert [item.external_id for item in retrieved] == ["my/app"]


@pytest.mark.usefixtures("disable_gzip")
class TestFunctionAppCallsAPI:
    def test_create_sends_payload(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url(f"{_ENCODED_PATH}/calls")).mock(
            return_value=httpx2.Response(
                200, json={"callId": 10, "functionId": 1, "runtimeStatus": "Completed", "response": {"ok": True}}
            )
        )

        toolkit_client.functions.apps.calls.create(
            "my/app", FunctionAppCallPayload(path="/items/1", method="POST", body={"a": 1}), nonce="abc"
        )

        assert json.loads(route.calls[0].request.content) == {
            "data": {"path": "/items/1", "method": "POST", "body": {"a": 1}},
            "nonce": "abc",
        }

    def test_list_applies_limit_client_side(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        respx_mock.get(toolkit_config.create_api_url(f"{_ENCODED_PATH}/calls")).mock(
            return_value=httpx2.Response(200, json={"items": [_FUNCTION_CALL, {**_FUNCTION_CALL, "id": 11}]})
        )

        calls = toolkit_client.functions.apps.calls.list("my/app", limit=1)

        assert ([call.id for call in calls], dict(respx_mock.calls[0].request.url.params)) == ([10], {})

    def test_logs(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        respx_mock.get(toolkit_config.create_api_url(f"{_ENCODED_PATH}/calls/10/logs")).mock(
            return_value=httpx2.Response(200, json={"items": [{"timestamp": 1, "message": "hello"}]})
        )

        logs = toolkit_client.functions.apps.calls.logs("my/app", 10)

        assert logs == [FunctionCallLogEntry(timestamp=1, message="hello")]

    def test_response(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        respx_mock.get(toolkit_config.create_api_url(f"{_ENCODED_PATH}/calls/10/response")).mock(
            return_value=httpx2.Response(200, json={"functionId": 1, "callId": 10, "response": {"value": 42}})
        )

        result = toolkit_client.functions.apps.calls.response("my/app", 10)

        assert result == FunctionCallResult(function_id=1, call_id=10, response={"value": 42})

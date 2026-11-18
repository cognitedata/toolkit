import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import CurrentUserSinkRef

_SIGNAL = {
    "externalId": "s1",
    "topic": "cognite_workflows",
    "sink": {"type": "current_user"},
    "severity": "info",
    "resourceId": "my_workflow",
    "category": "execution_failed",
    "metadata": {},
    "message": "The workflow failed.",
}


@pytest.mark.usefixtures("disable_gzip")
class TestSignalsAPI:
    def test_sync_sends_sink_and_cursor(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/processes/signals/sync")).mock(
            return_value=httpx2.Response(200, json={"items": [], "nextCursor": "def", "moreData": False})
        )

        toolkit_client.signals.sync(CurrentUserSinkRef(), cursor="abc")

        assert json.loads(route.calls[0].request.content) == {
            "sink": {"type": "current_user"},
            "limit": 100,
            "cursor": "abc",
        }

    def test_list_follows_cursor(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/processes/signals/list")).mock(
            side_effect=[
                httpx2.Response(200, json={"items": [_SIGNAL], "nextCursor": "c1"}),
                httpx2.Response(200, json={"items": [{**_SIGNAL, "externalId": "s2"}]}),
            ]
        )

        result = toolkit_client.signals.list(CurrentUserSinkRef(), limit=None)

        assert ([item.external_id for item in result], json.loads(route.calls[1].request.content)) == (
            ["s1", "s2"],
            {"sink": {"type": "current_user"}, "limit": 1000, "cursor": "c1"},
        )

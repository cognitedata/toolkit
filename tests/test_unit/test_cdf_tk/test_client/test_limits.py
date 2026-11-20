import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.limits import LimitsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import LimitId
from cognite_toolkit._cdf_tk.client.resource_classes.limits import LimitValueResponse


@pytest.mark.usefixtures("disable_gzip")
class TestLimitsAPI:
    def test_retrieve_multiple_ignore_unknown(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        respx_mock.get(toolkit_config.create_api_url("/limits/values/atlas.monthly_ai_tokens")).mock(
            return_value=httpx2.Response(200, json={"limitId": "atlas.monthly_ai_tokens", "value": 1000})
        )
        respx_mock.get(toolkit_config.create_api_url("/limits/values/files.storage_bytes")).mock(
            return_value=httpx2.Response(200, json={"limitId": "files.storage_bytes", "value": 500})
        )
        missing = respx_mock.get(toolkit_config.create_api_url("/limits/values/does.not_exist")).mock(
            return_value=httpx2.Response(404, json={"error": {"code": 404, "message": "Not found"}})
        )

        result = LimitsAPI(HTTPClient(toolkit_config)).retrieve(
            [
                LimitId(limit_id="atlas.monthly_ai_tokens"),
                LimitId(limit_id="does.not_exist"),
                LimitId(limit_id="files.storage_bytes"),
            ],
            ignore_unknown_ids=True,
        )

        assert result == [
            LimitValueResponse(limit_id="atlas.monthly_ai_tokens", value=1000),
            LimitValueResponse(limit_id="files.storage_bytes", value=500),
        ]
        assert missing.calls[0].request.headers["cdf-version"] == "20230101-alpha"

    def test_list_with_prefix(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/limits/values/list")).mock(
            side_effect=[
                httpx2.Response(200, json={"items": [{"limitId": "atlas.a", "value": 1}], "nextCursor": "c1"}),
                httpx2.Response(200, json={"items": [{"limitId": "atlas.b", "value": 2}]}),
            ]
        )

        result = LimitsAPI(HTTPClient(toolkit_config)).list(prefix="atlas.")

        assert [item.limit_id for item in result] == ["atlas.a", "atlas.b"]
        first, second = (json.loads(call.request.content) for call in route.calls)
        assert first == {"filter": {"prefix": {"property": ["limitId"], "value": "atlas."}}, "limit": 1000}
        assert second["cursor"] == "c1"
        assert route.calls[0].request.headers["cdf-version"] == "20230101-alpha"

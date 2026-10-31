import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.limits import LimitsAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.resource_classes.limits import LimitValue


@pytest.mark.usefixtures("disable_gzip")
class TestLimitsAPI:
    def test_retrieve_limit_by_id(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        url = toolkit_config.create_api_url("/limits/values/atlas.ai_requests")
        route = respx_mock.get(url).mock(
            return_value=httpx2.Response(status_code=200, json={"limitId": "atlas.ai_requests", "value": 1000.0})
        )

        limit = LimitsAPI(HTTPClient(toolkit_config)).retrieve("atlas.ai_requests")

        assert limit == LimitValue(limit_id="atlas.ai_requests", value=1000.0)
        assert route.calls[0].request.headers["cdf-version"] == "20230101-alpha"

    def test_retrieve_url_encodes_limit_id(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        url = toolkit_config.create_api_url("/limits/values/a%2Fb")
        respx_mock.get(url).mock(return_value=httpx2.Response(status_code=200, json={"limitId": "a/b", "value": 1.0}))

        LimitsAPI(HTTPClient(toolkit_config)).retrieve("a/b")

    def test_list_limits_follows_cursor(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        url = toolkit_config.create_api_url("/limits/values")
        respx_mock.get(url, params={"limit": "1000"}).mock(
            return_value=httpx2.Response(
                status_code=200,
                json={"items": [{"limitId": "a", "value": 1}], "nextCursor": "c1"},
            )
        )
        respx_mock.get(url, params={"limit": "1000", "cursor": "c1"}).mock(
            return_value=httpx2.Response(
                status_code=200,
                json={"items": [{"limitId": "b", "value": 2}], "nextCursor": None},
            )
        )

        limits = LimitsAPI(HTTPClient(toolkit_config)).list()

        assert limits == [LimitValue(limit_id="a", value=1.0), LimitValue(limit_id="b", value=2.0)]

    def test_search_by_prefix_sends_filter_and_cursor(
        self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        url = toolkit_config.create_api_url("/limits/values/list")
        route = respx_mock.post(url).mock(
            side_effect=[
                httpx2.Response(
                    status_code=200,
                    json={"items": [{"limitId": "atlas.a", "value": 5}], "nextCursor": "c1"},
                ),
                httpx2.Response(
                    status_code=200,
                    json={"items": [{"limitId": "atlas.b", "value": 6}], "nextCursor": None},
                ),
            ]
        )

        limits = LimitsAPI(HTTPClient(toolkit_config)).search("atlas.")

        assert limits == [LimitValue(limit_id="atlas.a", value=5.0), LimitValue(limit_id="atlas.b", value=6.0)]
        first_body = json.loads(route.calls[0].request.content)
        assert first_body["filter"] == {"prefix": {"property": ["limitId"], "value": "atlas."}}
        assert "cursor" not in first_body
        assert json.loads(route.calls[1].request.content)["cursor"] == "c1"
        assert route.calls[0].request.headers["cdf-version"] == "20230101-alpha"
